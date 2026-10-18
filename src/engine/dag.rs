use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::error::{Error, Result};
use crate::schema::ast::Schema;
use crate::storage::StorageBackend;
use crate::storage::{PendingWrites, StorageWriteBatch};
use crate::types::{
    BlockCursor, BlockNumber, ChangeBatch, ChangeRecord, ColumnRegistry, ColumnType, PerfNode,
    PerfNodeKind, Row, RowMap,
};

use super::mv::MVEngine;
use super::perf::PerfTimer;
use super::raw_table::RawTableEngine;
use super::reducer::ReducerEngine;

/// Processing order node — topologically sorted.
#[derive(Debug)]
enum PipelineNode {
    RawTable(String),
    Reducer(String),
    MV(String),
}

/// A reducer or MV. Each finalizes only its own state and writes disjoint
/// storage keys, so the branches finalize in parallel on the rayon pool.
trait FinalizeNode {
    fn advance_finality(&mut self, block: BlockNumber);
    fn write_checkpoint(&mut self, finalized: BlockNumber, batch: &mut StorageWriteBatch);
}

impl FinalizeNode for ReducerEngine {
    fn advance_finality(&mut self, block: BlockNumber) {
        ReducerEngine::advance_finality(self, block);
    }

    fn write_checkpoint(&mut self, finalized: BlockNumber, batch: &mut StorageWriteBatch) {
        ReducerEngine::write_checkpoint(self, finalized, batch);
    }
}

impl FinalizeNode for MVEngine {
    fn advance_finality(&mut self, block: BlockNumber) {
        MVEngine::advance_finality(self, block);
    }

    fn write_checkpoint(&mut self, _finalized: BlockNumber, batch: &mut StorageWriteBatch) {
        MVEngine::write_checkpoint(self, batch);
    }
}

#[cfg(feature = "rayon")]
type NodeMut<'a> = &'a mut (dyn FinalizeNode + Send);
#[cfg(not(feature = "rayon"))]
type NodeMut<'a> = &'a mut dyn FinalizeNode;

/// Replay reads the raw rows of each table this many blocks at a time, so that
/// a long unfinalized tail is not held in memory at once. Tests read two at a
/// time, so that every replay in them crosses the boundaries of a read.
const REPLAY_READ_BLOCKS: usize = if cfg!(test) { 2 } else { 64 };

/// Default of `Config::raw_retention_blocks`.
pub const DEFAULT_RAW_RETENTION_BLOCKS: u64 = 1_000;

/// The raw rows of one table during replay, read a part at a time.
struct ReplayCursor {
    table: String,
    /// The first block of the next read; None once the range is read.
    next_from: Option<BlockNumber>,
    rows: VecDeque<(BlockNumber, Vec<Row>)>,
}

impl ReplayCursor {
    fn new(table: String, from_block: BlockNumber) -> Self {
        Self {
            table,
            next_from: Some(from_block),
            rows: VecDeque::new(),
        }
    }

    /// Read the next part when the rows of the last one are used up.
    fn refill(&mut self, raw: &RawTableEngine, to_block: BlockNumber) -> Result<()> {
        let Some(from) = self.next_from.filter(|_| self.rows.is_empty()) else {
            return Ok(());
        };

        // More blocks than asked for are the whole range: the backend reads no
        // less (see `get_raw_rows_limited`).
        let part = raw.get_rows(from, to_block, REPLAY_READ_BLOCKS)?;
        self.next_from = match part.last() {
            Some(&(last, _)) if part.len() == REPLAY_READ_BLOCKS && last < to_block => {
                Some(last + 1)
            }
            _ => None,
        };
        self.rows.extend(part);
        Ok(())
    }

    fn front_block(&self) -> Option<BlockNumber> {
        self.rows.front().map(|(block, _)| *block)
    }
}

/// An independent reducer→MV chain that can be processed in parallel.
/// Engines are stored inline to avoid HashMap extraction/reinsertion per batch.
struct PipelineBranch {
    reducer_name: String,
    reducer: ReducerEngine,
    mv_entries: Vec<(String, MVEngine)>,
}

impl PipelineBranch {
    /// The reducer, then its MVs.
    fn nodes(&mut self) -> impl Iterator<Item = NodeMut<'_>> {
        let mvs = self.mv_entries.iter_mut().map(|(_, mv)| mv as NodeMut<'_>);
        std::iter::once(&mut self.reducer as NodeMut<'_>).chain(mvs)
    }

    /// Run the reducer on its source rows and feed its output to the branch's
    /// MVs.
    #[cfg(feature = "rayon")]
    fn process_block(
        &mut self,
        block: BlockNumber,
        output_rows: &HashMap<String, Vec<Row>>,
        perf: bool,
    ) -> Result<Option<PerfNode>> {
        let timer = PerfTimer::start(perf);
        let mut mv_nodes = Vec::new();

        if let Some(rows) = output_rows.get(self.reducer.source()) {
            let enriched = self.reducer.process_block(block, rows)?;
            if !enriched.is_empty() {
                for (mv_name, mv) in &mut self.mv_entries {
                    let mv_timer = PerfTimer::start(perf);
                    mv.apply_rows(block, &enriched);
                    mv_nodes.extend(mv_timer.stop(PerfNodeKind::MV, mv_name, vec![]));
                }
            }
        }

        Ok(timer.stop(PerfNodeKind::Reducer, &self.reducer_name, mv_nodes))
    }
}

/// Top-level engine that wires the computation DAG:
/// Raw Tables → Reducers → Materialized Views
pub struct SettleEngine {
    raw_tables: HashMap<String, RawTableEngine>,
    reducers: HashMap<String, ReducerEngine>,
    mvs: HashMap<String, MVEngine>,
    /// Tables marked as VIRTUAL — stored but no changes emitted.
    virtual_tables: HashSet<String>,
    /// Topologically sorted processing order.
    pipeline: Vec<PipelineNode>,
    /// MVs that source directly from raw tables (processed before branches).
    direct_mvs: Vec<String>,
    /// Independent reducer→MV branches. When len() >= 2, processed in parallel.
    branches: Vec<PipelineBranch>,
    /// Reducer name → index in `branches` for O(1) lookup.
    branch_index: HashMap<String, usize>,
    /// MV name → (branch_index, mv_index within branch) for O(1) lookup.
    mv_branch_index: HashMap<String, (usize, usize)>,
    /// Sequence number for change batches.
    sequence: u64,
    /// Latest processed block number (for ordering/rollback logic).
    latest_block: Option<BlockNumber>,
    /// Finalized block number (for finalization logic). In-memory finality
    /// watermark; bounds rollback. May run AHEAD of `durable_block` during
    /// backfill deferral (derived state for `(durable_block, finalized_block]`
    /// is then only in memory, rebuilt from raw rows on recovery).
    finalized_block: BlockNumber,
    /// Highest block whose derived reducer/MV state is actually persisted to
    /// disk. Invariant `durable_block <= min(finalized_block, latest)`. Written
    /// to disk as `META_FINALIZED_BLOCK` so recovery replays from
    /// here. Equals `finalized_block` unless backfill deferral is active.
    /// None until a checkpoint covers data: `Some(0)` means block 0
    /// is durable, which an empty database must not claim.
    durable_block: Option<BlockNumber>,
    /// How many blocks below the durable block keep their raw rows. See
    /// `purge_raw_rows`.
    raw_retention_blocks: u64,
    /// The highest block whose raw rows a write batch deletes; None until the
    /// first purge after open.
    raw_purged_through: Option<BlockNumber>,
    /// True when the pipeline contains no sliding-window MV and no external
    /// reducer, so backfill persist-deferral is safe. Computed at construction
    /// (and after `add_reducer`). When false, every ingest writes a
    /// checkpoint (durable == finalized).
    defer_allowed: bool,
    /// True once finality advanced at least once (or state was restored from
    /// disk). Lets `advance_finality(N)` skip work when N has not moved:
    /// heartbeat ingests with a stale `finalized_head` are common at the tip.
    has_finalized: bool,
    /// Whether ingest builds the `PerfNode` timing tree.
    collect_perf: bool,
    /// Block number → hash for all known blocks.
    /// Populated by set_rollback_chain(). Used for fork resolution and cursors.
    block_hashes: BTreeMap<BlockNumber, String>,
    /// Blocks whose hash changed since the last `take_hash_changes`.
    hash_changes: BTreeSet<BlockNumber>,
}

impl SettleEngine {
    /// Build the engine from a parsed schema and storage backend, with the
    /// stored state of every MV loaded.
    pub fn new(schema: &Schema, storage: Arc<dyn StorageBackend>) -> Result<Self> {
        let mut raw_tables = HashMap::new();
        let mut reducers = HashMap::new();
        let mut mvs = HashMap::new();

        for table_def in &schema.tables {
            raw_tables.insert(
                table_def.name.clone(),
                RawTableEngine::new(table_def.clone(), storage.clone()),
            );
        }

        let modules: Vec<(String, String)> = schema
            .modules
            .iter()
            .map(|m| (m.name.clone(), m.script.clone()))
            .collect();

        for reducer_def in &schema.reducers {
            if let Some(raw) = raw_tables.get(&reducer_def.source) {
                // Source is a raw table — use its registry
                reducers.insert(
                    reducer_def.name.clone(),
                    ReducerEngine::new(
                        reducer_def.clone(),
                        storage.clone(),
                        raw.registry(),
                        &modules,
                    ),
                );
            } else {
                // Source is another reducer — build registry dynamically per batch
                reducers.insert(
                    reducer_def.name.clone(),
                    ReducerEngine::new_chained(reducer_def.clone(), storage.clone(), &modules),
                );
            }
        }

        // Build column type maps for MV source resolution
        let table_column_types: HashMap<String, HashMap<String, ColumnType>> = schema
            .tables
            .iter()
            .map(|t| {
                let cols: HashMap<String, ColumnType> = t
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.column_type.clone()))
                    .collect();
                (t.name.clone(), cols)
            })
            .collect();

        // Build column type maps for reducer output (state fields + group_by from source table)
        let reducer_column_types: HashMap<String, HashMap<String, ColumnType>> = schema
            .reducers
            .iter()
            .map(|r| {
                let mut cols: HashMap<String, ColumnType> = r
                    .state
                    .iter()
                    .map(|s| (s.name.clone(), s.column_type.clone()))
                    .collect();
                if let Some(table_cols) = table_column_types.get(&r.source) {
                    for col in &r.group_by {
                        if let Some(ct) = table_cols.get(col) {
                            cols.insert(col.clone(), ct.clone());
                        }
                    }
                }
                (r.name.clone(), cols)
            })
            .collect();

        for mv_def in &schema.materialized_views {
            // Resolve source column types: from table or reducer output
            let source_col_types = table_column_types
                .get(&mv_def.source)
                .or_else(|| reducer_column_types.get(&mv_def.source))
                .cloned()
                .unwrap_or_default();
            mvs.insert(
                mv_def.name.clone(),
                MVEngine::new(mv_def.clone(), storage.clone(), &source_col_types)?,
            );
        }

        let virtual_tables: HashSet<String> = schema
            .tables
            .iter()
            .filter(|t| t.virtual_table)
            .map(|t| t.name.clone())
            .collect();

        let pipeline = build_pipeline(schema);
        let (direct_mvs, branch_specs) = compute_branches(&pipeline, &reducers, &mvs);

        // Move branch engines out of HashMaps into PipelineBranch structs
        // to avoid per-batch HashMap extraction/reinsertion in parallel path.
        let branches: Vec<PipelineBranch> = branch_specs
            .into_iter()
            .map(|(reducer_name, mv_names)| {
                let reducer = reducers.remove(&reducer_name).unwrap();
                let mv_entries: Vec<_> = mv_names
                    .into_iter()
                    .map(|name| {
                        let mv = mvs.remove(&name).unwrap();
                        (name, mv)
                    })
                    .collect();
                PipelineBranch {
                    reducer_name,
                    reducer,
                    mv_entries,
                }
            })
            .collect();

        let branch_index: HashMap<String, usize> = branches
            .iter()
            .enumerate()
            .map(|(i, b)| (b.reducer_name.clone(), i))
            .collect();

        let mut mv_branch_index: HashMap<String, (usize, usize)> = HashMap::new();
        for (bi, branch) in branches.iter().enumerate() {
            for (mi, (mv_name, _)) in branch.mv_entries.iter().enumerate() {
                mv_branch_index.insert(mv_name.clone(), (bi, mi));
            }
        }

        let defer_allowed = compute_defer_allowed(&branches, &reducers, &mvs);

        Ok(Self {
            raw_tables,
            reducers,
            mvs,
            virtual_tables,
            pipeline,
            direct_mvs,
            branches,
            branch_index,
            mv_branch_index,
            sequence: 0,
            latest_block: None,
            finalized_block: 0,
            durable_block: None,
            raw_retention_blocks: DEFAULT_RAW_RETENTION_BLOCKS,
            raw_purged_through: None,
            defer_allowed,
            has_finalized: false,
            collect_perf: true,
            block_hashes: BTreeMap::new(),
            hash_changes: BTreeSet::new(),
        })
    }

    /// Add an external reducer to the pipeline.
    /// Creates a ReducerEngine and adds it as a new branch (sequential only).
    /// Must be called before any data processing.
    pub fn add_reducer(
        &mut self,
        def: crate::schema::ast::ReducerDef,
        storage: std::sync::Arc<dyn crate::storage::StorageBackend>,
    ) -> crate::error::Result<()> {
        let name = def.name.clone();

        // Build engine — source must be a raw table (chained external not yet supported)
        let engine = if let Some(raw) = self.raw_tables.get(&def.source) {
            ReducerEngine::new(def, storage, raw.registry(), &[])
        } else {
            return Err(crate::error::Error::InvalidOperation(format!(
                "external reducer '{}' source '{}' must be a raw table",
                name, def.source
            )));
        };

        // Find downstream MVs that source from this reducer
        let mv_names: Vec<String> = self
            .mvs
            .keys()
            .filter(|mv_name| {
                self.mvs
                    .get(*mv_name)
                    .map(|mv| mv.source() == name)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        let mv_entries: Vec<_> = mv_names
            .into_iter()
            .filter_map(|n| self.mvs.remove(&n).map(|mv| (n, mv)))
            .collect();

        // Add as a new branch
        let idx = self.branches.len();
        self.branches.push(PipelineBranch {
            reducer_name: name.clone(),
            reducer: engine,
            mv_entries,
        });
        self.branch_index.insert(name.clone(), idx);
        for (mi, (mv_name, _)) in self.branches[idx].mv_entries.iter().enumerate() {
            self.mv_branch_index.insert(mv_name.clone(), (idx, mi));
        }

        // Add to pipeline
        self.pipeline.push(PipelineNode::Reducer(name));

        // Recompute: an added external reducer disables backfill deferral.
        self.defer_allowed = compute_defer_allowed(&self.branches, &self.reducers, &self.mvs);

        Ok(())
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.raw_tables.contains_key(name)
    }

    /// The columns of a raw table, in the order of its positional rows.
    pub fn table_registry(&self, name: &str) -> Option<&Arc<ColumnRegistry>> {
        self.raw_tables.get(name).map(|t| t.registry())
    }

    /// Check if a reducer with the given name exists in the engine.
    pub fn has_reducer(&self, name: &str) -> bool {
        self.reducers.contains_key(name) || self.branch_index.contains_key(name)
    }

    /// Whether the named reducer's `ReducerDef::body` is `External` —
    /// i.e. callable from a host-language callback. Returns false for
    /// `Lua` / `EventRules` reducers (which have their own embedded
    /// runtime and ignore any host-side callback), and for unknown names.
    pub fn reducer_is_external(&self, name: &str) -> bool {
        if let Some(idx) = self.branch_index.get(name) {
            self.branches[*idx].reducer.is_external()
        } else if let Some(r) = self.reducers.get(name) {
            r.is_external()
        } else {
            false
        }
    }

    /// Replace the runtime for a named reducer (used for External/FnReducer injection).
    /// Searches both branches and the reducers HashMap.
    pub fn set_reducer_runtime(
        &mut self,
        name: &str,
        runtime: Box<dyn crate::reducer_runtime::ReducerRuntime>,
    ) {
        for branch in &mut self.branches {
            if branch.reducer_name == name {
                branch.reducer.set_runtime(runtime);
                return;
            }
        }
        if let Some(reducer) = self.reducers.get_mut(name) {
            reducer.set_runtime(runtime);
        }
    }

    /// Process a batch of rows for a raw table at the given block.
    /// Cascades through reducers and MVs, returning all change records.
    ///
    /// When multiple independent reducer branches exist (e.g. two reducers
    /// both sourcing from the same raw table), they are executed in parallel
    /// using rayon's thread pool.
    pub fn process_batch(
        &mut self,
        table: &str,
        block: BlockNumber,
        row_maps: Vec<RowMap>,
    ) -> Result<(Vec<ChangeRecord>, Option<PerfNode>)> {
        let Some(registry) = self.table_registry(table) else {
            return Err(Error::InvalidOperation(format!("unknown table: {table}")));
        };
        let rows = row_maps
            .into_iter()
            .map(|m| Row::from_owned_map(registry.clone(), m))
            .collect();
        let (mut changes, perf_node) = self.process_batch_inner(table, block, rows, None)?;
        changes.extend(self.take_changes());
        Ok((changes, perf_node))
    }

    /// Process a batch, deferring raw row storage writes to the given WriteBatch.
    /// `rows` are in the column order of the table (`table_registry`).
    /// Returns the raw-table changes only: the MV changes of all the blocks of
    /// an ingest wait for `take_changes`.
    pub fn process_batch_deferred(
        &mut self,
        table: &str,
        block: BlockNumber,
        rows: Vec<Row>,
        write_batch: &mut StorageWriteBatch,
    ) -> Result<(Vec<ChangeRecord>, Option<PerfNode>)> {
        self.process_batch_inner(table, block, rows, Some(write_batch))
    }

    /// The MV changes of the blocks processed since the last call, one per
    /// group, in pipeline order.
    pub fn take_changes(&mut self) -> Vec<ChangeRecord> {
        let mut changes = Vec::new();
        self.for_each_mv(|mv| changes.extend(mv.take_changes()));
        changes
    }

    /// Forget the MV changes of a failed ingest.
    fn discard_changes(&mut self) {
        self.for_each_mv(MVEngine::discard_changes);
    }

    fn for_each_mv(&mut self, mut f: impl FnMut(&mut MVEngine)) {
        for node in &self.pipeline {
            let PipelineNode::MV(name) = node else {
                continue;
            };
            let mv = match self.mv_branch_index.get(name) {
                Some(&(bi, mi)) => &mut self.branches[bi].mv_entries[mi].1,
                None => self.mvs.get_mut(name).expect("MV in the pipeline"),
            };
            f(mv);
        }
    }

    fn process_batch_inner(
        &mut self,
        table: &str,
        block: BlockNumber,
        rows: Vec<Row>,
        write_batch: Option<&mut StorageWriteBatch>,
    ) -> Result<(Vec<ChangeRecord>, Option<PerfNode>)> {
        let perf = self.collect_perf;
        let pipeline_timer = PerfTimer::start(perf);
        let mut perf_children: Vec<PerfNode> = Vec::new();

        if !self.raw_tables.contains_key(table) {
            return Err(Error::InvalidOperation(format!("unknown table: {table}")));
        }

        let mut all_changes = Vec::new();

        // Phase 1: Raw table ingest
        let raw_timer = PerfTimer::start(perf);
        let raw_eng = self.raw_tables.get(table).unwrap();
        let is_virtual = self.virtual_tables.contains(table);
        if let Some(batch) = write_batch {
            let changes = raw_eng.ingest_to_batch(block, &rows, batch, is_virtual)?;
            all_changes.extend(changes);
        } else if is_virtual {
            raw_eng.ingest_no_changes(block, &rows)?;
        } else {
            let changes = raw_eng.ingest(block, &rows)?;
            all_changes.extend(changes);
        }
        perf_children.extend(raw_timer.stop(PerfNodeKind::RawTable, table, vec![]));

        // Output rows for downstream consumption (reducers + MVs)
        let mut output_rows: HashMap<String, Vec<Row>> = HashMap::new();
        output_rows.insert(table.to_string(), rows);

        #[cfg(feature = "rayon")]
        let can_parallelize = self.parallel_branches();
        #[cfg(not(feature = "rayon"))]
        let can_parallelize = false;

        if can_parallelize {
            #[cfg(feature = "rayon")]
            {
                // Phase 2a: Process MVs that source directly from raw tables
                for mv_name in &self.direct_mvs {
                    let mv_timer = PerfTimer::start(perf);
                    let mv = self.mvs.get_mut(mv_name).unwrap();
                    if let Some(source_rows) = output_rows.get(mv.source()) {
                        mv.apply_rows(block, source_rows);
                    }
                    perf_children.extend(mv_timer.stop(PerfNodeKind::MV, mv_name, vec![]));
                }

                // Phase 2b: Process reducer branches in parallel.
                let parallel_timer = PerfTimer::start(perf);
                let mut branch_nodes = Vec::new();
                let mut take = |result: Result<Option<PerfNode>>| {
                    branch_nodes.extend(result?);
                    Ok::<(), Error>(())
                };

                if self.branches.len() == 2 {
                    let (first, second) = self.branches.split_at_mut(1);
                    let (result_0, result_1) = rayon::join(
                        || first[0].process_block(block, &output_rows, perf),
                        || second[0].process_block(block, &output_rows, perf),
                    );
                    take(result_0)?;
                    take(result_1)?;
                } else {
                    let results: Vec<_> = self
                        .branches
                        .par_iter_mut()
                        .map(|branch| branch.process_block(block, &output_rows, perf))
                        .collect();
                    for result in results {
                        take(result)?;
                    }
                }

                perf_children.extend(parallel_timer.stop(
                    PerfNodeKind::Parallel,
                    "parallel",
                    branch_nodes,
                ));
            } // #[cfg(feature = "rayon")]
        } else {
            // Sequential execution: process branches + remaining engines.
            // Track reducer name → perf_children index for nesting MV nodes.
            let mut reducer_perf_idx: HashMap<String, usize> = HashMap::new();

            for node in &self.pipeline {
                match node {
                    PipelineNode::RawTable(_) => {} // Already processed in Phase 1
                    PipelineNode::Reducer(name) => {
                        let timer = PerfTimer::start(perf);
                        let reducer = match self.branch_index.get(name) {
                            Some(&i) => &mut self.branches[i].reducer,
                            None => self.reducers.get_mut(name).unwrap(),
                        };
                        let enriched = match output_rows.get(reducer.source()) {
                            Some(source_rows) => reducer.process_block(block, source_rows)?,
                            None => Vec::new(),
                        };
                        if let Some(node) = timer.stop(PerfNodeKind::Reducer, name, vec![]) {
                            reducer_perf_idx.insert(name.clone(), perf_children.len());
                            perf_children.push(node);
                        }
                        if !enriched.is_empty() {
                            output_rows.insert(name.clone(), enriched);
                        }
                    }
                    PipelineNode::MV(name) => {
                        let timer = PerfTimer::start(perf);
                        let mv = match self.mv_branch_index.get(name) {
                            Some(&(bi, mi)) => &mut self.branches[bi].mv_entries[mi].1,
                            None => self.mvs.get_mut(name).unwrap(),
                        };
                        if let Some(source_rows) = output_rows.get(mv.source()) {
                            mv.apply_rows(block, source_rows);
                        }
                        let mv_source = mv.source();
                        // Nest under parent reducer if exists, otherwise add as sibling
                        if let Some(node) = timer.stop(PerfNodeKind::MV, name, vec![]) {
                            match reducer_perf_idx.get(mv_source) {
                                Some(&idx) => perf_children[idx].children.push(node),
                                None => perf_children.push(node),
                            }
                        }
                    }
                }
            }
        }

        if self.latest_block.is_none_or(|b| block > b) {
            self.latest_block = Some(block);
        }

        // A durable watermark can sit on a block that held no data: ingest
        // admits block 0 while latest is 0, and older databases stored a
        // finality above their latest block. This block's state is not on
        // disk, so the next checkpoint must write it.
        if self.durable_block.is_some_and(|d| block <= d) {
            self.durable_block = block.checked_sub(1);
        }

        let perf_node = pipeline_timer.stop(PerfNodeKind::Pipeline, table, perf_children);
        Ok((all_changes, perf_node))
    }

    /// Replay unfinalized blocks from raw rows in storage.
    /// Used on startup to rebuild reducer/MV in-memory state after a crash.
    /// Reads raw rows for each table in [from_block, to_block] and feeds them
    /// through reducers and MVs (without re-ingesting into raw storage).
    pub fn replay_unfinalized(
        &mut self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> Result<()> {
        self.replay_unfinalized_inner(from_block, to_block, None)
    }

    /// Rebuild the unfinalized state of `reducer_name` from the raw rows of
    /// the blocks above `finalized` up to `latest`, after its runtime
    /// changed. The replay runs the reducers upstream of it, which produce
    /// its input, and the reducers and MVs downstream, which consume its
    /// output. Each of them first goes back to its state at `finalized`, so
    /// that no block counts twice.
    ///
    /// An external reducer without a callback produces nothing: the replay
    /// that its callback starts rebuilds what depends on it.
    pub fn replay_reducer_chain(
        &mut self,
        reducer_name: &str,
        finalized: BlockNumber,
        latest: BlockNumber,
    ) -> Result<()> {
        if !self.has_reducer(reducer_name) {
            return Err(Error::InvalidOperation(format!(
                "replay: unknown reducer '{reducer_name}'"
            )));
        }
        if latest <= finalized {
            return Ok(());
        }

        let mut upstream = Vec::new();
        let mut source = self.reducer_source(reducer_name);
        while let Some(name) = source.filter(|s| self.has_reducer(s)) {
            upstream.push(name.to_string());
            source = self.reducer_source(name);
        }

        // The pipeline lists each reducer after its source.
        let mut downstream: Vec<String> = vec![reducer_name.to_string()];
        for node in &self.pipeline {
            if let PipelineNode::Reducer(name) = node
                && self
                    .reducer_source(name)
                    .is_some_and(|s| downstream.iter().any(|d| d == s))
            {
                downstream.push(name.clone());
            }
        }

        let views: Vec<String> = self
            .pipeline
            .iter()
            .filter_map(|node| match node {
                PipelineNode::MV(name) => Some(name),
                _ => None,
            })
            .filter(|name| {
                self.mv_source(name)
                    .is_some_and(|s| downstream.iter().any(|d| d == s))
            })
            .cloned()
            .collect();

        let none_pending = PendingWrites::default();
        for name in upstream.iter().chain(&downstream) {
            if let Some(&i) = self.branch_index.get(name) {
                self.branches[i].reducer.rollback(finalized, &none_pending)?;
            } else if let Some(reducer) = self.reducers.get_mut(name) {
                reducer.rollback(finalized, &none_pending)?;
            }
        }
        // The compensating records of these rollbacks go nowhere: the
        // target already holds the state that the replay rebuilds.
        for name in &views {
            if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                self.branches[bi].mv_entries[mi].1.rollback(finalized);
            } else if let Some(mv) = self.mvs.get_mut(name) {
                mv.rollback(finalized);
            }
        }

        let nodes: HashSet<String> = upstream.into_iter().chain(downstream).chain(views).collect();
        self.replay_unfinalized_inner(finalized + 1, latest, Some(&nodes))
    }

    fn reducer_source(&self, name: &str) -> Option<&str> {
        if let Some(&i) = self.branch_index.get(name) {
            Some(self.branches[i].reducer.source())
        } else {
            self.reducers.get(name).map(|r| r.source())
        }
    }

    fn mv_source(&self, name: &str) -> Option<&str> {
        if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
            Some(self.branches[bi].mv_entries[mi].1.source())
        } else {
            self.mvs.get(name).map(|mv| mv.source())
        }
    }

    fn replay_unfinalized_inner(
        &mut self,
        from_block: BlockNumber,
        to_block: BlockNumber,
        only_nodes: Option<&HashSet<String>>,
    ) -> Result<()> {
        if from_block > to_block {
            return Ok(());
        }

        let mut cursors: Vec<ReplayCursor> = self
            .raw_tables
            .keys()
            .map(|table| ReplayCursor::new(table.clone(), from_block))
            .collect();

        // Each block in order, with the rows of every table that has data in it.
        loop {
            for cursor in &mut cursors {
                cursor.refill(&self.raw_tables[&cursor.table], to_block)?;
            }

            let Some(block) = cursors.iter().filter_map(ReplayCursor::front_block).min() else {
                break;
            };

            for cursor in &mut cursors {
                if cursor.front_block() == Some(block) {
                    let (_, rows) = cursor.rows.pop_front().expect("front block");
                    self.replay_table_block(block, cursor.table.clone(), rows, only_nodes)?;
                }
            }
        }

        Ok(())
    }

    /// Feed the rows of one table at `block` through the reducers and MVs,
    /// with no raw-table ingest and no change records.
    fn replay_table_block(
        &mut self,
        block: BlockNumber,
        table_name: String,
        rows: Vec<Row>,
        only_nodes: Option<&HashSet<String>>,
    ) -> Result<()> {
        let mut output_rows: HashMap<String, Vec<Row>> = HashMap::new();
        output_rows.insert(table_name, rows);

        for node in &self.pipeline {
            match node {
                PipelineNode::RawTable(_) => {
                    // Skip — rows already in storage
                }
                PipelineNode::Reducer(name) => {
                    if only_nodes.is_some_and(|filter| !filter.contains(name.as_str())) {
                        continue;
                    }

                    let reducer = match self.branch_index.get(name) {
                        Some(&i) => &mut self.branches[i].reducer,
                        None => self.reducers.get_mut(name).unwrap(),
                    };
                    // An external reducer with no JS context produces nothing:
                    // the replay that its callback starts rebuilds it.
                    if reducer.needs_host_callback() {
                        continue;
                    }
                    let enriched = match output_rows.get(reducer.source()) {
                        Some(source_rows) => reducer.process_block(block, source_rows)?,
                        None => Vec::new(),
                    };
                    if !enriched.is_empty() {
                        output_rows.insert(name.clone(), enriched);
                    }
                }
                PipelineNode::MV(name) => {
                    if only_nodes.is_some_and(|filter| !filter.contains(name.as_str())) {
                        continue;
                    }

                    let mv = match self.mv_branch_index.get(name) {
                        Some(&(bi, mi)) => &mut self.branches[bi].mv_entries[mi].1,
                        None => self.mvs.get_mut(name).unwrap(),
                    };
                    if let Some(source_rows) = output_rows.get(mv.source()) {
                        mv.replay_block(block, source_rows);
                    }
                }
            }
        }

        Ok(())
    }

    /// Roll back all state after fork_point.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Result<Vec<ChangeRecord>> {
        self.rollback_inner(fork_point, None, &PendingWrites::default())
    }

    /// Roll back all state after fork_point, deferring raw-row deletions
    /// to the provided write batch for atomic commit with metadata. Storage
    /// is read through `pending`, the write batches not committed yet.
    pub fn rollback_to_batch(
        &mut self,
        fork_point: BlockNumber,
        batch: &mut StorageWriteBatch,
        pending: &PendingWrites,
    ) -> Result<Vec<ChangeRecord>> {
        self.rollback_inner(fork_point, Some(batch), pending)
    }

    /// Err if a rollback to `fork_point` is not allowed. Changes nothing, so
    /// callers can tell a rejected target from a rollback that failed part
    /// way.
    pub fn check_rollback_target(&self, fork_point: BlockNumber) -> Result<()> {
        // Above latest the rollback is a no-op, see `rollback_inner`.
        let current_latest = self.latest_block.unwrap_or(0);
        if fork_point > current_latest {
            return Ok(());
        }

        // Finality floor: a fork/rollback must never cross below the finalized
        // watermark — finalized state is irreversible. Pruning keeps the
        // hash of the durable block, which lags finality under backfill
        // deferral, so a fork signal could otherwise resolve below finality —
        // rolling derived state back past the durable watermark, corrupting the
        // recovery anchor and wedging the instance (durable > latest). Every
        // rollback path funnels through `rollback_inner`, which checks this.
        // With deferral off (`durable == finalized`) this can never fire for
        // an in-range fork.
        if fork_point < self.finalized_block {
            return Err(Error::InvalidOperation(format!(
                "rollback target block {fork_point} is below finalized block {} — \
                 finalized state is irreversible",
                self.finalized_block
            )));
        }
        Ok(())
    }

    fn rollback_inner(
        &mut self,
        fork_point: BlockNumber,
        write_batch: Option<&mut StorageWriteBatch>,
        pending: &PendingWrites,
    ) -> Result<Vec<ChangeRecord>> {
        self.check_rollback_target(fork_point)?;

        // Guard: fork_point cannot raise latest_block. If the caller passes
        // a fork point above current latest (e.g. via stale future hashes that
        // somehow leaked into block_hashes), do nothing — no rollback work to
        // perform and latest_block must not advance without underlying data.
        let current_latest = self.latest_block.unwrap_or(0);
        if fork_point > current_latest {
            return Ok(Vec::new());
        }

        let all_changes = self.unwind_to(fork_point, write_batch, pending)?;

        // Remove hashes for rolled-back blocks
        let after = self.block_hashes.split_off(&(fork_point + 1));
        self.hash_changes.extend(after.into_keys());

        Ok(all_changes)
    }

    /// Undo the in-memory effects of a failed ingest, back to `block`: the
    /// latest block before the ingest processed anything, None if there was
    /// none.
    ///
    /// This is not a chain reorg, so the finality bound of `rollback` does
    /// not apply: the undone blocks were never finalized, and on gappy chains
    /// `finalized_block` can be above `block`. Block hashes stay as they are,
    /// because a failed ingest stores none, and the hashes above `block` can
    /// include the finalized head. Raw-row deletions go to a throwaway batch:
    /// the ingest's raw rows were never committed. `pending` holds the write
    /// batches of earlier ingests that are not committed yet.
    pub fn undo_ingest(
        &mut self,
        block: Option<BlockNumber>,
        pending: &PendingWrites,
    ) -> Result<()> {
        self.discard_changes();

        // A rollback keeps its target block, so it cannot undo block 0. With
        // no block before the ingest, storage holds no derived state and no
        // batch waits: the stored state is the state to go back to.
        let Some(block) = block else {
            for branch in &mut self.branches {
                branch.reducer.reload();
                for (_, mv) in &mut branch.mv_entries {
                    mv.reload()?;
                }
            }
            for reducer in self.reducers.values_mut() {
                reducer.reload();
            }
            for mv in self.mvs.values_mut() {
                mv.reload()?;
            }
            self.latest_block = None;
            return Ok(());
        };

        if block > self.latest_block() {
            return Ok(());
        }

        let mut throwaway = StorageWriteBatch::new();
        self.unwind_to(block, Some(&mut throwaway), pending)?;
        Ok(())
    }

    /// Roll every pipeline node back to `fork_point` and make it the latest
    /// block. The callers check that the target is valid.
    fn unwind_to(
        &mut self,
        fork_point: BlockNumber,
        mut write_batch: Option<&mut StorageWriteBatch>,
        pending: &PendingWrites,
    ) -> Result<Vec<ChangeRecord>> {
        let mut all_changes = Vec::new();

        // Roll back in reverse pipeline order
        for node in self.pipeline.iter().rev() {
            match node {
                PipelineNode::MV(name) => {
                    if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                        all_changes.extend(self.branches[bi].mv_entries[mi].1.rollback(fork_point));
                    } else {
                        let mv = self.mvs.get_mut(name).unwrap();
                        all_changes.extend(mv.rollback(fork_point));
                    }
                }
                PipelineNode::Reducer(name) => {
                    // Check branches first, then HashMap
                    if let Some(branch) =
                        self.branch_index.get(name).map(|&i| &mut self.branches[i])
                    {
                        branch.reducer.rollback(fork_point, pending)?;
                    } else {
                        let reducer = self.reducers.get_mut(name).unwrap();
                        reducer.rollback(fork_point, pending)?;
                    }
                }
                PipelineNode::RawTable(name) => {
                    let raw_engine = self.raw_tables.get(name).unwrap();
                    let changes = if let Some(ref mut batch) = write_batch {
                        raw_engine.rollback_to_batch(fork_point, batch, pending)?
                    } else {
                        raw_engine.rollback(fork_point)?
                    };
                    if !self.virtual_tables.contains(name) {
                        all_changes.extend(changes);
                    }
                }
            }
        }

        self.latest_block = Some(fork_point);

        Ok(all_changes)
    }

    /// Merge every node's blocks up to `block` into finalized state; rollback
    /// stops at `block` from now on. Nothing reaches disk before
    /// `write_checkpoint`.
    pub fn advance_finality(&mut self, block: BlockNumber) {
        // With the durable state at this finality, every block up to it is
        // merged: new data lands above the latest block, which is at or above
        // the durable one.
        let merged = self.has_finalized
            && block == self.finalized_block
            && self.durable_block.is_some_and(|d| d >= block);
        if merged {
            return;
        }

        // Finality writes nothing to storage.
        let mut unused = StorageWriteBatch::new();
        self.finalize_nodes(&mut unused, |node, _| node.advance_finality(block));

        self.finalized_block = block;
        self.has_finalized = true;
        self.prune_block_hashes();
    }

    /// Write the derived state of every block up to finality into `batch`,
    /// and move the durable watermark there. The caller commits `batch` with
    /// the raw rows and metadata of the same ingest, so the state and the
    /// watermark reach disk together or not at all.
    pub fn write_checkpoint(&mut self, batch: &mut StorageWriteBatch) {
        if self.durable_block.is_some_and(|d| d >= self.finalized_block) {
            return;
        }

        let finalized = self.finalized_block;
        self.finalize_nodes(batch, |node, batch| node.write_checkpoint(finalized, batch));

        // On gappy chains finality can run ahead of the data. Without data
        // nothing is durable.
        if let Some(latest) = self.latest_block {
            self.durable_block = Some(finalized.min(latest));
        }
        self.purge_raw_rows(batch);
        self.prune_block_hashes();
    }

    /// Delete the raw rows that nothing reads any more, in the batch that
    /// moves the durable block. A reopen replays only the blocks above the
    /// durable block, and a rollback or a reducer replay only those above
    /// finality, which is not below it. The rows of `raw_retention_blocks`
    /// blocks below the durable block stay as a margin. The purge runs in
    /// steps of a tenth of the margin, so that a checkpoint per block does not
    /// write a range tombstone per block.
    fn purge_raw_rows(&mut self, batch: &mut StorageWriteBatch) {
        let Some(through) = self
            .durable_block
            .and_then(|durable| durable.checked_sub(self.raw_retention_blocks))
        else {
            return;
        };

        let step = (self.raw_retention_blocks / 10).max(1);
        // The first purge after open also deletes what earlier runs left.
        let from = match self.raw_purged_through {
            Some(purged) if through < purged.saturating_add(step) => return,
            Some(purged) => purged + 1,
            None => 0,
        };

        let mut tables: Vec<&String> = self.raw_tables.keys().collect();
        tables.sort();
        for table in tables {
            batch.delete_raw_rows(table, from, through);
        }
        self.raw_purged_through = Some(through);
    }

    /// See `Config::raw_retention_blocks`.
    pub fn set_raw_retention_blocks(&mut self, blocks: u64) {
        self.raw_retention_blocks = blocks;
    }

    /// Whether the branches process in parallel: 2+ branches, all reducers
    /// source from raw tables (not from each other), and no external
    /// reducers (which require main-thread JS callbacks).
    #[cfg(feature = "rayon")]
    fn parallel_branches(&self) -> bool {
        self.branches.len() >= 2
            && self.branches.iter().all(|b| {
                self.raw_tables.contains_key(b.reducer.source()) && !b.reducer.is_external()
            })
    }

    /// Run `step` on every reducer and MV, on the thread that processed its
    /// blocks: the branches that process in parallel get a task each, with a
    /// write batch of its own, and the other nodes run on this thread. On
    /// another core the state of a node is not in cache, and that costs more
    /// than a parallel run saves.
    fn finalize_nodes<F>(&mut self, batch: &mut StorageWriteBatch, step: F)
    where
        F: Fn(NodeMut<'_>, &mut StorageWriteBatch) + Sync,
    {
        #[cfg(feature = "rayon")]
        let parallel = self.parallel_branches();
        #[cfg(not(feature = "rayon"))]
        let parallel = false;

        if parallel {
            #[cfg(feature = "rayon")]
            {
                let sub_batches: Vec<StorageWriteBatch> = self
                    .branches
                    .par_iter_mut()
                    .map(|branch| {
                        let mut sub = StorageWriteBatch::new();
                        for node in branch.nodes() {
                            step(node, &mut sub);
                        }
                        sub
                    })
                    .collect();
                for sub in sub_batches {
                    batch.ops.extend(sub.ops);
                }
            }
        } else {
            for branch in &mut self.branches {
                for node in branch.nodes() {
                    step(node, batch);
                }
            }
        }

        for reducer in self.reducers.values_mut() {
            step(reducer, batch);
        }
        for mv in self.mvs.values_mut() {
            step(mv, batch);
        }
    }

    /// Fork resolution stops at finality, so hashes below it only take
    /// memory and storage. Two of them stay: a reopen restarts
    /// finality at the durable block and reads its hash, and on gappy chains
    /// `latest_cursor` reads the hash of a latest block below finality.
    fn prune_block_hashes(&mut self) {
        let finalized = self.finalized_block;
        let anchors = [self.durable_block, self.latest_block];

        let kept = self.block_hashes.split_off(&finalized);
        let below = std::mem::replace(&mut self.block_hashes, kept);
        for (block, hash) in below {
            if anchors.contains(&Some(block)) {
                self.block_hashes.insert(block, hash);
            } else {
                self.hash_changes.insert(block);
            }
        }
    }

    /// Create a ChangeBatch from a set of change records.
    pub fn make_batch(&mut self, records: Vec<ChangeRecord>) -> ChangeBatch {
        self.sequence += 1;
        // Group records by table name
        let mut tables: HashMap<String, Vec<ChangeRecord>> = HashMap::new();
        for record in records {
            if let Some(vec) = tables.get_mut(record.table()) {
                vec.push(record);
            } else {
                tables.insert(record.table().to_string(), vec![record]);
            }
        }
        ChangeBatch {
            sequence: self.sequence,
            finalized_head: self.finalized_cursor(),
            latest_head: self.latest_cursor(),
            tables,
            perf: vec![],
        }
    }

    pub fn latest_block(&self) -> BlockNumber {
        self.latest_block.unwrap_or(0)
    }

    /// The latest block, or None before any block: `latest_block` reads
    /// that as 0.
    pub fn latest_block_if_any(&self) -> Option<BlockNumber> {
        self.latest_block
    }

    pub fn latest_cursor(&self) -> Option<BlockCursor> {
        let block = self.latest_block?;
        let hash = self.block_hashes.get(&block).cloned().unwrap_or_default();
        Some(BlockCursor {
            number: block,
            hash,
        })
    }

    pub fn finalized_block(&self) -> BlockNumber {
        self.finalized_block
    }

    /// Highest block whose derived state is durably persisted, or None if no
    /// data is. Written to disk as `META_FINALIZED_BLOCK` so recovery replays
    /// raw rows from the block after it, or from block 0 without it.
    pub fn durable_block(&self) -> Option<BlockNumber> {
        self.durable_block
    }

    /// Turn the per-ingest `PerfNode` timing tree on or off.
    pub fn set_collect_perf(&mut self, on: bool) {
        self.collect_perf = on;
    }

    /// Whether backfill persist-deferral is structurally safe for this pipeline
    /// (no sliding-window MV, no external reducer).
    pub fn defer_allowed(&self) -> bool {
        self.defer_allowed
    }

    /// True once finality advanced at least once (or state was restored).
    pub fn has_finalized(&self) -> bool {
        self.has_finalized
    }

    pub fn finalized_cursor(&self) -> Option<BlockCursor> {
        self.block_hashes
            .get(&self.finalized_block)
            .map(|hash| BlockCursor {
                number: self.finalized_block,
                hash: hash.clone(),
            })
    }

    pub fn set_latest_block(&mut self, block: BlockNumber) {
        self.latest_block = Some(block);
    }

    pub fn set_finalized_block(&mut self, block: BlockNumber) {
        self.finalized_block = block;
        // On-disk `META_FINALIZED_BLOCK` is the DURABLE watermark, so restoring
        // it sets durable == finalized: derived state on disk is exactly
        // as-of-`block`, and recovery replays raw rows from block+1.
        self.durable_block = Some(block);
        // Restored state is on disk already: advancing to the same block
        // again has nothing to do.
        self.has_finalized = true;
    }

    pub fn restore_block_hashes(&mut self, hashes: BTreeMap<BlockNumber, String>) {
        self.block_hashes = hashes;
    }

    pub fn block_hashes(&self) -> &BTreeMap<BlockNumber, String> {
        &self.block_hashes
    }

    /// Store block hashes from the rollback chain (unfinalized blocks)
    /// and the finalized head. Used for fork resolution.
    pub fn set_rollback_chain(&mut self, chain: &[(BlockNumber, String)]) {
        for (number, hash) in chain {
            if self.block_hashes.get(number) != Some(hash) {
                self.block_hashes.insert(*number, hash.clone());
                self.hash_changes.insert(*number);
            }
        }
    }

    /// The blocks whose hash changed since the last call, each with its hash
    /// now: None if it was removed.
    pub fn take_hash_changes(&mut self) -> Vec<(BlockNumber, Option<&str>)> {
        let changes = std::mem::take(&mut self.hash_changes);
        changes
            .into_iter()
            .map(|block| (block, self.block_hashes.get(&block).map(String::as_str)))
            .collect()
    }

    /// The lowest block up to `up_to` whose stored hash differs from the
    /// supplied one: a block that is no longer canonical.
    pub fn lowest_hash_conflict(
        &self,
        supplied: &[(BlockNumber, &str)],
        up_to: BlockNumber,
    ) -> Option<BlockNumber> {
        supplied
            .iter()
            .filter(|&&(number, hash)| {
                number <= up_to
                    && self
                        .block_hashes
                        .get(&number)
                        .is_some_and(|stored| stored != hash)
            })
            .map(|&(number, _)| number)
            .min()
    }

    /// Find the highest block in `previous_blocks` whose hash matches
    /// our stored hash. Returns the common ancestor as a BlockCursor.
    ///
    /// May return a cursor with `number > latest_block` when the caller has
    /// stored future hashes via `set_rollback_chain` (e.g. Solana-style
    /// chains where finality is known out-of-band for blocks we haven't
    /// ingested data for). For rollback purposes use
    /// `resolve_fork_cursor_bounded` which clamps to `latest_block`.
    pub fn resolve_fork_cursor(
        &self,
        previous_blocks: &[(BlockNumber, &str)],
    ) -> Option<BlockCursor> {
        for &(number, hash) in previous_blocks {
            if self.block_hashes.get(&number).map(|h| h.as_str()) == Some(hash) {
                return Some(BlockCursor {
                    number,
                    hash: hash.to_string(),
                });
            }
        }
        None
    }

    /// Like `resolve_fork_cursor` but skips matches with `number > latest_block`.
    /// Used by `handle_fork` so a stale future hash (e.g. one left behind by
    /// a prior rollback that then heartbeat-committed the rollback_chain)
    /// can't silently advance the cursor past blocks we have no data for.
    pub fn resolve_fork_cursor_bounded(
        &self,
        previous_blocks: &[(BlockNumber, &str)],
    ) -> Option<BlockCursor> {
        let latest = self.latest_block.unwrap_or(0);
        let finalized = self.finalized_block;
        for &(number, hash) in previous_blocks {
            // Skip matches above latest (no data) and below finalized (finality
            // is irreversible). Under backfill deferral `block_hashes` keeps
            // the hash of the durable block below finality; resolving a fork
            // onto it would roll state back below finality. Resolving
            // *to* `finalized` itself stays allowed (it only matches when the
            // hash agrees, i.e. finality is intact — it rolls back the
            // unfinalized tail). See the finality floor in `rollback_inner`.
            if number > latest || number < finalized {
                continue;
            }
            if self.block_hashes.get(&number).map(|h| h.as_str()) == Some(hash) {
                return Some(BlockCursor {
                    number,
                    hash: hash.to_string(),
                });
            }
        }
        None
    }
}

/// Backfill persist-deferral is only allowed when the pipeline has no
/// sliding-window MV (not tested with deferral) and no external reducer
/// (replay skips them, so deferred state can't be rebuilt).
/// See docs/backfill-mode-design.md.
fn compute_defer_allowed(
    branches: &[PipelineBranch],
    reducers: &HashMap<String, ReducerEngine>,
    mvs: &HashMap<String, MVEngine>,
) -> bool {
    let no_external = branches.iter().all(|b| !b.reducer.is_external())
        && reducers.values().all(|r| !r.is_external());
    let no_sliding = branches
        .iter()
        .all(|b| b.mv_entries.iter().all(|(_, mv)| !mv.is_sliding()))
        && mvs.values().all(|mv| !mv.is_sliding());
    no_external && no_sliding
}

/// Identify independent branches and direct MVs from the pipeline.
///
/// A branch is a reducer + its downstream MVs. Branches that all source
/// from raw tables (not from each other) can be executed in parallel.
/// Direct MVs source from raw tables and are processed before branches.
///
/// Returns (direct_mv_names, Vec<(reducer_name, mv_names)>).
fn compute_branches(
    pipeline: &[PipelineNode],
    reducers: &HashMap<String, ReducerEngine>,
    mvs: &HashMap<String, MVEngine>,
) -> (Vec<String>, Vec<(String, Vec<String>)>) {
    let reducer_names: HashSet<&str> = reducers.keys().map(|s| s.as_str()).collect();

    let mut branches = Vec::new();
    let mut direct_mvs = Vec::new();

    // Build branches: each reducer + its downstream MVs
    for node in pipeline {
        if let PipelineNode::Reducer(name) = node {
            let downstream: Vec<String> = pipeline
                .iter()
                .filter_map(|n| {
                    if let PipelineNode::MV(mv_name) = n {
                        let mv = mvs.get(mv_name).unwrap();
                        if mv.source() == name.as_str() {
                            return Some(mv_name.clone());
                        }
                    }
                    None
                })
                .collect();

            branches.push((name.clone(), downstream));
        }
    }

    // Find MVs sourcing from raw tables (not from reducers)
    for node in pipeline {
        if let PipelineNode::MV(name) = node {
            let mv = mvs.get(name).unwrap();
            if !reducer_names.contains(mv.source()) {
                direct_mvs.push(name.clone());
            }
        }
    }

    (direct_mvs, branches)
}

/// Build the topologically sorted pipeline from the schema.
fn build_pipeline(schema: &Schema) -> Vec<PipelineNode> {
    let mut pipeline = Vec::new();

    // Build dependency map: name -> sources
    let mut reducer_sources: HashMap<&str, &str> = HashMap::new();
    let mut mv_sources: HashMap<&str, &str> = HashMap::new();
    let table_names: Vec<&str> = schema.tables.iter().map(|t| t.name.as_str()).collect();

    for r in &schema.reducers {
        reducer_sources.insert(&r.name, &r.source);
    }
    for mv in &schema.materialized_views {
        mv_sources.insert(&mv.name, &mv.source);
    }

    // Phase 1: Raw tables (roots)
    for name in &table_names {
        pipeline.push(PipelineNode::RawTable(name.to_string()));
    }

    // Phase 2: Reducers — topologically sorted so upstream reducers come first.
    // Reducers sourcing from raw tables are roots; chained reducers follow.
    let mut emitted: HashSet<&str> = HashSet::new();
    let mut remaining: Vec<&str> = schema.reducers.iter().map(|r| r.name.as_str()).collect();

    while !remaining.is_empty() {
        let before = remaining.len();
        remaining.retain(|name| {
            let r = schema.reducers.iter().find(|r| r.name == *name).unwrap();
            // Ready if source is a table or an already-emitted reducer
            let source_is_table = table_names.contains(&r.source.as_str());
            let source_emitted = emitted.contains(r.source.as_str());
            if source_is_table || source_emitted {
                pipeline.push(PipelineNode::Reducer(r.name.clone()));
                emitted.insert(name);
                false // remove from remaining
            } else {
                true // keep in remaining
            }
        });
        if remaining.len() == before {
            // No progress — shouldn't happen if validation caught cycles
            break;
        }
    }

    // Phase 3: MVs — sort by dependency
    // MVs sourcing from raw tables come before MVs sourcing from reducers
    let mut mv_from_tables = Vec::new();
    let mut mv_from_reducers = Vec::new();

    for mv in &schema.materialized_views {
        if table_names.contains(&mv.source.as_str()) {
            mv_from_tables.push(PipelineNode::MV(mv.name.clone()));
        } else {
            mv_from_reducers.push(PipelineNode::MV(mv.name.clone()));
        }
    }

    pipeline.extend(mv_from_tables);
    pipeline.extend(mv_from_reducers);

    pipeline
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::*;
    use crate::storage::memory::MemoryBackend;
    use crate::types::{ColumnType, ChangeOp, RowMap, Value};

    fn dex_schema() -> Schema {
        Schema {
            modules: vec![],
            tables: vec![TableDef {
                name: "trades".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "block_number".to_string(),
                        column_type: ColumnType::UInt64,
                    },
                    ColumnDef {
                        name: "user".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "side".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "amount".to_string(),
                        column_type: ColumnType::Float64,
                    },
                    ColumnDef {
                        name: "price".to_string(),
                        column_type: ColumnType::Float64,
                    },
                ],
                virtual_table: false,
            }],
            reducers: vec![ReducerDef {
                name: "pnl".to_string(),
                source: "trades".to_string(),
                group_by: vec!["user".to_string()],
                state: vec![
                    StateField {
                        name: "quantity".to_string(),
                        column_type: ColumnType::Float64,
                        default: "0".to_string(),
                    },
                    StateField {
                        name: "cost_basis".to_string(),
                        column_type: ColumnType::Float64,
                        default: "0".to_string(),
                    },
                ],
                requires: vec![],
                body: ReducerBody::EventRules {
                    when_blocks: vec![
                        WhenBlock {
                            condition: Expr::BinaryOp {
                                left: Box::new(Expr::RowRef("side".into())),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Literal("buy".into())),
                            },
                            lets: vec![],
                            sets: vec![
                                (
                                    "quantity".into(),
                                    Expr::BinaryOp {
                                        left: Box::new(Expr::StateRef("quantity".into())),
                                        op: BinaryOp::Add,
                                        right: Box::new(Expr::RowRef("amount".into())),
                                    },
                                ),
                                (
                                    "cost_basis".into(),
                                    Expr::BinaryOp {
                                        left: Box::new(Expr::StateRef("cost_basis".into())),
                                        op: BinaryOp::Add,
                                        right: Box::new(Expr::BinaryOp {
                                            left: Box::new(Expr::RowRef("amount".into())),
                                            op: BinaryOp::Mul,
                                            right: Box::new(Expr::RowRef("price".into())),
                                        }),
                                    },
                                ),
                            ],
                            emits: vec![("trade_pnl".into(), Expr::Int(0))],
                        },
                        WhenBlock {
                            condition: Expr::BinaryOp {
                                left: Box::new(Expr::RowRef("side".into())),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Literal("sell".into())),
                            },
                            lets: vec![(
                                "avg_cost".into(),
                                Expr::BinaryOp {
                                    left: Box::new(Expr::StateRef("cost_basis".into())),
                                    op: BinaryOp::Div,
                                    right: Box::new(Expr::StateRef("quantity".into())),
                                },
                            )],
                            sets: vec![
                                (
                                    "quantity".into(),
                                    Expr::BinaryOp {
                                        left: Box::new(Expr::StateRef("quantity".into())),
                                        op: BinaryOp::Sub,
                                        right: Box::new(Expr::RowRef("amount".into())),
                                    },
                                ),
                                (
                                    "cost_basis".into(),
                                    Expr::BinaryOp {
                                        left: Box::new(Expr::StateRef("cost_basis".into())),
                                        op: BinaryOp::Sub,
                                        right: Box::new(Expr::BinaryOp {
                                            left: Box::new(Expr::RowRef("amount".into())),
                                            op: BinaryOp::Mul,
                                            right: Box::new(Expr::ColumnRef("avg_cost".into())),
                                        }),
                                    },
                                ),
                            ],
                            emits: vec![(
                                "trade_pnl".into(),
                                Expr::BinaryOp {
                                    left: Box::new(Expr::RowRef("amount".into())),
                                    op: BinaryOp::Mul,
                                    right: Box::new(Expr::BinaryOp {
                                        left: Box::new(Expr::RowRef("price".into())),
                                        op: BinaryOp::Sub,
                                        right: Box::new(Expr::ColumnRef("avg_cost".into())),
                                    }),
                                },
                            )],
                        },
                    ],
                    always_emit: Some(AlwaysEmit {
                        emits: vec![("position_size".into(), Expr::StateRef("quantity".into()))],
                    }),
                },
            }],
            materialized_views: vec![MVDef {
                name: "position_summary".to_string(),
                source: "pnl".to_string(),
                select: vec![
                    SelectItem {
                        expr: SelectExpr::Column("user".into()),
                        alias: None,
                    },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Sum, Some("trade_pnl".into())),
                        alias: Some("total_pnl".into()),
                    },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Last, Some("position_size".into())),
                        alias: Some("current_position".into()),
                    },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Count, None),
                        alias: Some("trade_count".into()),
                    },
                ],
                group_by: vec!["user".into()],
                sliding_window: None,
            }],
        }
    }

    fn simple_mv_only_schema() -> Schema {
        Schema {
            modules: vec![],
            tables: vec![TableDef {
                name: "swaps".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "pool".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "amount".to_string(),
                        column_type: ColumnType::Float64,
                    },
                ],
                virtual_table: false,
            }],
            reducers: vec![],
            materialized_views: vec![MVDef {
                name: "pool_volume".to_string(),
                source: "swaps".to_string(),
                select: vec![
                    SelectItem {
                        expr: SelectExpr::Column("pool".into()),
                        alias: None,
                    },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Sum, Some("amount".into())),
                        alias: Some("total".into()),
                    },
                ],
                group_by: vec!["pool".into()],
                sliding_window: None,
            }],
        }
    }

    fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> RowMap {
        HashMap::from([
            ("user".to_string(), Value::String(user.to_string())),
            ("side".to_string(), Value::String(side.to_string())),
            ("amount".to_string(), Value::Float64(amount)),
            ("price".to_string(), Value::Float64(price)),
        ])
    }

    #[test]
    fn raw_table_to_mv_direct() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage).unwrap();

        let rows = vec![
            HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(100.0)),
            ]),
            HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(200.0)),
            ]),
        ];

        let (changes, _) = engine.process_batch("swaps", 1000, rows).unwrap();

        // Should have: 2 raw inserts + 1 MV insert
        let raw_changes: Vec<_> = changes.iter().filter(|d| d.table() == "swaps").collect();
        let mv_changes: Vec<_> = changes
            .iter()
            .filter(|d| d.table() == "pool_volume")
            .collect();

        assert_eq!(raw_changes.len(), 2);
        assert_eq!(mv_changes.len(), 1);
        assert_eq!(mv_changes[0].operation, ChangeOp::Insert);
        assert_eq!(mv_changes[0].get("total"), Some(&Value::Float64(300.0)));
    }

    #[test]
    fn full_pipeline_raw_reducer_mv() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&dex_schema(), storage).unwrap();

        // Block 1000: alice buys 10 @ 2000
        let (changes, _) = engine
            .process_batch(
                "trades",
                1000,
                vec![make_trade("alice", "buy", 10.0, 2000.0)],
            )
            .unwrap();

        // Raw insert + MV insert (position_summary)
        let mv_changes: Vec<_> = changes
            .iter()
            .filter(|d| d.table() == "position_summary")
            .collect();
        assert_eq!(mv_changes.len(), 1);
        assert_eq!(mv_changes[0].operation, ChangeOp::Insert);
        assert_eq!(mv_changes[0].get("trade_count"), Some(&Value::UInt64(1)));
    }

    #[test]
    fn pipeline_rollback() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage).unwrap();

        // Block 1000
        let _ = engine
            .process_batch(
                "swaps",
                1000,
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(100.0)),
                ])],
            )
            .unwrap();

        // Block 1001
        let _ = engine
            .process_batch(
                "swaps",
                1001,
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(200.0)),
                ])],
            )
            .unwrap();

        // Rollback block 1001
        let changes = engine.rollback(1000).unwrap();

        // MV should update back to 100
        let mv_changes: Vec<_> = changes
            .iter()
            .filter(|d| d.table() == "pool_volume")
            .collect();
        assert_eq!(mv_changes.len(), 1);
        assert_eq!(mv_changes[0].operation, ChangeOp::Update);
        assert_eq!(mv_changes[0].get("total"), Some(&Value::Float64(100.0)));

        // Raw table should emit delete change
        let raw_changes: Vec<_> = changes.iter().filter(|d| d.table() == "swaps").collect();
        assert_eq!(raw_changes.len(), 1);
        assert_eq!(raw_changes[0].operation, ChangeOp::Delete);
    }

    #[test]
    fn pipeline_finalize() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage).unwrap();

        let _ = engine
            .process_batch(
                "swaps",
                1000,
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(100.0)),
                ])],
            )
            .unwrap();

        let _ = engine
            .process_batch(
                "swaps",
                1001,
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(200.0)),
                ])],
            )
            .unwrap();

        let mut batch = StorageWriteBatch::new();
        engine.advance_finality(1000);
        engine.write_checkpoint(&mut batch);
        assert_eq!(engine.finalized_block(), 1000);

        // Rollback to 1000 should only remove block 1001
        let changes = engine.rollback(1000).unwrap();
        let mv_changes: Vec<_> = changes
            .iter()
            .filter(|d| d.table() == "pool_volume")
            .collect();
        assert_eq!(mv_changes.len(), 1);
        // After finalize(1000) + rollback(1001→1000): total should be 100
        assert_eq!(mv_changes[0].get("total"), Some(&Value::Float64(100.0)));
    }

    #[test]
    fn rollback_below_finalized_is_rejected() {
        // Finality floor (Cluster B): rollback_inner must refuse any fork point
        // below finalized_block — finalized state is irreversible. Rolling back
        // to == finalized is allowed (removes the unfinalized tail); below is an
        // error, regardless of which blocks still have retained hashes.
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage).unwrap();
        for (b, amt) in [(1000u64, 100.0), (1001, 200.0), (1002, 300.0)] {
            engine
                .process_batch(
                    "swaps",
                    b,
                    vec![HashMap::from([
                        ("pool".to_string(), Value::String("ETH/USDC".into())),
                        ("amount".to_string(), Value::Float64(amt)),
                    ])],
                )
                .unwrap();
        }
        let mut batch = StorageWriteBatch::new();
        engine.advance_finality(1001);
        engine.write_checkpoint(&mut batch);
        assert_eq!(engine.finalized_block(), 1001);

        // Below finalized → rejected, no mutation.
        assert!(engine.rollback(1000).is_err(), "rollback below finalized must error");
        assert_eq!(engine.latest_block(), 1002, "rejected rollback must not move latest");

        // At finalized → allowed (removes unfinalized block 1002 only).
        engine.rollback(1001).expect("rollback to finalized is allowed");
        assert_eq!(engine.latest_block(), 1001);
    }

    #[test]
    fn undo_ingest_is_not_bounded_by_finality() {
        // Gappy chain: finality (100) is ahead of the latest data block (3).
        // A failed ingest that processed block 4 is undone back to 3, which is
        // below finality, and the finalized head hash survives the undo.
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage.clone()).unwrap();
        let registry = engine.table_registry("swaps").unwrap().clone();
        let swap = |amount: f64| {
            let map = HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(amount)),
            ]);
            vec![Row::from_owned_map(registry.clone(), map)]
        };

        let mut batch = StorageWriteBatch::new();
        engine
            .process_batch_deferred("swaps", 3, swap(100.0), &mut batch)
            .unwrap();
        engine.take_changes();
        engine.set_rollback_chain(&[(100, "0x64".to_string())]);
        engine.advance_finality(100);
        engine.write_checkpoint(&mut batch);
        storage.commit(&batch).unwrap();

        // The failed ingest's write batch is never committed.
        engine
            .process_batch_deferred("swaps", 4, swap(200.0), &mut StorageWriteBatch::new())
            .unwrap();

        assert!(
            engine.rollback(3).is_err(),
            "a chain rollback below finality stays rejected"
        );
        engine
            .undo_ingest(Some(3), &PendingWrites::default())
            .expect("undoing a failed ingest is not bounded by finality");
        assert_eq!(engine.latest_block(), 3);
        assert_eq!(engine.finalized_cursor().map(|c| c.number), Some(100));

        // Block 4 again counts once.
        engine
            .process_batch_deferred("swaps", 4, swap(200.0), &mut StorageWriteBatch::new())
            .unwrap();
        let changes = engine.take_changes();
        let mv = changes.iter().find(|c| c.table() == "pool_volume").unwrap();
        assert_eq!(mv.get("total"), Some(&Value::Float64(300.0)));
    }

    #[test]
    fn full_pipeline_rollback_and_reingest() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&dex_schema(), storage).unwrap();

        // Block 1000: alice buys 10 @ 2000
        let _ = engine
            .process_batch(
                "trades",
                1000,
                vec![make_trade("alice", "buy", 10.0, 2000.0)],
            )
            .unwrap();

        // Block 1001: alice buys 5 @ 2100
        let _ = engine
            .process_batch(
                "trades",
                1001,
                vec![make_trade("alice", "buy", 5.0, 2100.0)],
            )
            .unwrap();

        // Block 1002: alice sells 8 @ 2200 (will be rolled back)
        let _ = engine
            .process_batch(
                "trades",
                1002,
                vec![make_trade("alice", "sell", 8.0, 2200.0)],
            )
            .unwrap();

        // Rollback block 1002
        engine.rollback(1001).unwrap();

        // Re-ingest block 1002 with different trade
        let (changes, _) = engine
            .process_batch(
                "trades",
                1002,
                vec![make_trade("alice", "sell", 3.0, 2300.0)],
            )
            .unwrap();

        // MV should get updated with new trade data
        let mv_changes: Vec<_> = changes
            .iter()
            .filter(|d| d.table() == "position_summary")
            .collect();
        assert_eq!(mv_changes.len(), 1);
        assert_eq!(mv_changes[0].operation, ChangeOp::Update);
        assert_eq!(mv_changes[0].get("trade_count"), Some(&Value::UInt64(3)));
    }

    #[test]
    fn make_batch_increments_sequence() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage).unwrap();

        let batch1 = engine.make_batch(vec![]);
        assert_eq!(batch1.sequence, 1);

        let batch2 = engine.make_batch(vec![]);
        assert_eq!(batch2.sequence, 2);
    }

    #[test]
    fn unknown_table_returns_error() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage).unwrap();

        let result = engine.process_batch("nonexistent", 1000, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn reducer_chaining() {
        // Table → reducer_a (accumulates total) → reducer_b (detects doubles) → MV
        let schema = Schema {
            modules: vec![],
            tables: vec![TableDef {
                name: "events".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "user".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "amount".to_string(),
                        column_type: ColumnType::Float64,
                    },
                ],
                virtual_table: false,
            }],
            reducers: vec![
                ReducerDef {
                    name: "totals".to_string(),
                    source: "events".to_string(),
                    group_by: vec!["user".to_string()],
                    state: vec![StateField {
                        name: "total".to_string(),
                        column_type: ColumnType::Float64,
                        default: "0".to_string(),
                    }],
                    requires: vec![],
                    body: ReducerBody::Lua {
                        script: r#"
                            state.total = state.total + row.amount
                            emit({user = row.user, total = state.total})
                        "#
                        .to_string(),
                    },
                },
                ReducerDef {
                    name: "doubled".to_string(),
                    source: "totals".to_string(), // chained!
                    group_by: vec!["user".to_string()],
                    state: vec![],
                    requires: vec![],
                    body: ReducerBody::Lua {
                        script: r#"
                            emit({user = row.user, doubled = row.total * 2})
                        "#
                        .to_string(),
                    },
                },
            ],
            materialized_views: vec![MVDef {
                name: "summary".to_string(),
                source: "doubled".to_string(),
                select: vec![
                    SelectItem {
                        expr: SelectExpr::Column("user".into()),
                        alias: None,
                    },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Last, Some("doubled".into())),
                        alias: Some("latest_doubled".into()),
                    },
                ],
                group_by: vec!["user".into()],
                sliding_window: None,
            }],
        };

        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&schema, storage).unwrap();

        // Block 1: alice deposits 10
        let (changes, _) = engine
            .process_batch(
                "events",
                1000,
                vec![HashMap::from([
                    ("user".to_string(), Value::String("alice".into())),
                    ("amount".to_string(), Value::Float64(10.0)),
                ])],
            )
            .unwrap();

        // Should have: events insert + summary insert (doubled=20)
        let summary_changes: Vec<_> = changes.iter().filter(|d| d.table() == "summary").collect();
        assert_eq!(summary_changes.len(), 1);
        assert_eq!(
            summary_changes[0].get("latest_doubled"),
            Some(&Value::Float64(20.0))
        );

        // Block 2: alice deposits 5 more (total=15, doubled=30)
        let (changes2, _) = engine
            .process_batch(
                "events",
                1001,
                vec![HashMap::from([
                    ("user".to_string(), Value::String("alice".into())),
                    ("amount".to_string(), Value::Float64(5.0)),
                ])],
            )
            .unwrap();

        let summary2: Vec<_> = changes2.iter().filter(|d| d.table() == "summary").collect();
        assert_eq!(summary2.len(), 1);
        assert_eq!(
            summary2[0].get("latest_doubled"),
            Some(&Value::Float64(30.0))
        );

        // Rollback block 1001
        let rollback_changes = engine.rollback(1000).unwrap();
        let summary_rb: Vec<_> = rollback_changes
            .iter()
            .filter(|d| d.table() == "summary")
            .collect();
        assert!(
            !summary_rb.is_empty(),
            "rollback should produce summary changes"
        );

        // Re-ingest block 1001: alice deposits 20 (total=30, doubled=60)
        let (changes3, _) = engine
            .process_batch(
                "events",
                1001,
                vec![HashMap::from([
                    ("user".to_string(), Value::String("alice".into())),
                    ("amount".to_string(), Value::Float64(20.0)),
                ])],
            )
            .unwrap();

        let summary3: Vec<_> = changes3.iter().filter(|d| d.table() == "summary").collect();
        assert_eq!(summary3.len(), 1);
        assert_eq!(
            summary3[0].get("latest_doubled"),
            Some(&Value::Float64(60.0))
        );
    }

    /// Direct raw-table→MV (no reducer) must work during replay_unfinalized.
    #[test]
    fn replay_unfinalized_direct_mv() {
        let schema = Schema {
            modules: vec![],
            tables: vec![TableDef {
                name: "events".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "block_number".to_string(),
                        column_type: ColumnType::UInt64,
                    },
                    ColumnDef {
                        name: "pool".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "amount".to_string(),
                        column_type: ColumnType::Float64,
                    },
                ],
                virtual_table: false,
            }],
            reducers: vec![],
            materialized_views: vec![MVDef {
                name: "pool_totals".to_string(),
                source: "events".to_string(),
                select: vec![
                    SelectItem {
                        expr: SelectExpr::Column("pool".to_string()),
                        alias: None,
                    },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Sum, Some("amount".to_string())),
                        alias: Some("total".to_string()),
                    },
                ],
                group_by: vec!["pool".to_string()],
                sliding_window: None,
            }],
        };

        // Process two blocks
        let rows1 = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(100.0)),
        ])];
        let rows2 = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(200.0)),
        ])];
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&schema, storage.clone()).unwrap();

        // Process blocks — raw rows go to storage, MV gets data
        let _ = engine.process_batch("events", 1000, rows1).unwrap();
        let _ = engine.process_batch("events", 1001, rows2).unwrap();

        // Simulate crash recovery: create a fresh engine with the same storage
        // that has raw rows but no in-memory MV state.
        let mut engine2 = SettleEngine::new(&schema, storage).unwrap();

        // Replay unfinalized blocks — MV sources directly from raw table
        engine2.replay_unfinalized(1000, 1001).unwrap();

        // After replay, MV should have accumulated 100 + 200 = 300
        // Process another block to get changes that confirm the state
        let rows3 = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(50.0)),
        ])];
        let (changes, _) = engine2.process_batch("events", 1002, rows3).unwrap();
        let mv_changes: Vec<_> = changes
            .iter()
            .filter(|d| d.table() == "pool_totals")
            .collect();
        assert!(
            !mv_changes.is_empty(),
            "MV should produce changes after replay"
        );
        // Total should be 100 + 200 + 50 = 350
        assert_eq!(
            mv_changes.last().unwrap().get("total"),
            Some(&Value::Float64(350.0))
        );
    }

    /// Replay reads each table a few blocks at a time. Two tables whose parts
    /// end at different blocks must still replay every block of each, in
    /// block order: `first`, `last` and `sum` show a lost or reordered block.
    #[test]
    fn replay_merges_tables_read_in_parts() {
        let view = |table: &str| {
            format!(
                "CREATE MATERIALIZED VIEW {table}_view AS
                 SELECT k, first(v) AS first_v, last(v) AS last_v, sum(v) AS total, count() AS n
                 FROM {table} GROUP BY k;"
            )
        };
        let schema = crate::schema::parser::parse_schema(&format!(
            "CREATE TABLE a (block_number UInt64, k String, v Float64);
             CREATE TABLE b (block_number UInt64, k String, v Float64);
             {}
             {}",
            view("a"),
            view("b"),
        ))
        .unwrap();
        let row = |v: f64| {
            vec![RowMap::from([
                ("k".to_string(), Value::String("x".into())),
                ("v".to_string(), Value::Float64(v)),
            ])]
        };

        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let mut live = SettleEngine::new(&schema, storage.clone()).unwrap();
        for block in 1..=40 {
            live.process_batch("a", block, row(block as f64)).unwrap();
            if block % 7 == 0 {
                live.process_batch("b", block, row(block as f64 * 10.0))
                    .unwrap();
            }
        }

        let mut replayed = SettleEngine::new(&schema, storage).unwrap();
        replayed.replay_unfinalized(1, 40).unwrap();

        // The changes of one more block carry each view's state before it.
        let next_block = |engine: &mut SettleEngine| {
            let (mut changes, _) = engine.process_batch("a", 41, row(0.5)).unwrap();
            changes.extend(engine.process_batch("b", 41, row(0.5)).unwrap().0);
            changes
                .into_iter()
                .filter(|c| c.table().ends_with("_view"))
                .map(|c| {
                    let prev: Option<BTreeMap<_, _>> =
                        c.prev_map().map(|p| p.into_iter().collect());
                    format!("{} {prev:?}", c.table())
                })
                .collect::<Vec<_>>()
        };
        let expected = next_block(&mut live);
        assert_eq!(expected.len(), 2);
        assert_eq!(next_block(&mut replayed), expected);
    }

    /// add_reducer must update mv_branch_index so process_batch can find branch MVs.
    #[test]
    fn add_reducer_updates_mv_branch_index() {
        // Schema with a table and MV but no reducer
        let schema = Schema {
            modules: vec![],
            tables: vec![TableDef {
                name: "events".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "block_number".to_string(),
                        column_type: ColumnType::UInt64,
                    },
                    ColumnDef {
                        name: "user".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "amount".to_string(),
                        column_type: ColumnType::Float64,
                    },
                ],
                virtual_table: false,
            }],
            reducers: vec![],
            materialized_views: vec![MVDef {
                name: "user_totals".to_string(),
                source: "counter".to_string(), // will come from the dynamically-added reducer
                select: vec![
                    SelectItem {
                        expr: SelectExpr::Column("user".to_string()),
                        alias: None,
                    },
                    SelectItem {
                        expr: SelectExpr::Agg(AggFunc::Sum, Some("count".to_string())),
                        alias: Some("total".to_string()),
                    },
                ],
                group_by: vec!["user".to_string()],
                sliding_window: None,
            }],
        };

        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&schema, storage.clone()).unwrap();

        // Dynamically add a reducer that the MV sources from
        let reducer_def = ReducerDef {
            name: "counter".to_string(),
            source: "events".to_string(),
            group_by: vec!["user".to_string()],
            state: vec![StateField {
                name: "count".to_string(),
                column_type: ColumnType::Float64,
                default: "0".to_string(),
            }],
            requires: vec![],
            body: ReducerBody::EventRules {
                when_blocks: vec![WhenBlock {
                    condition: Expr::Int(1), // always true
                    lets: vec![],
                    sets: vec![(
                        "count".to_string(),
                        Expr::BinaryOp {
                            left: Box::new(Expr::StateRef("count".into())),
                            op: BinaryOp::Add,
                            right: Box::new(Expr::Int(1)),
                        },
                    )],
                    emits: vec![("count".to_string(), Expr::StateRef("count".into()))],
                }],
                always_emit: None,
            },
        };
        engine.add_reducer(reducer_def, storage).unwrap();

        // Process a batch — should not panic (the MV is now in a branch)
        let (changes, _) = engine
            .process_batch(
                "events",
                1000,
                vec![RowMap::from([
                    ("user".to_string(), Value::String("alice".into())),
                    ("amount".to_string(), Value::Float64(1.0)),
                ])],
            )
            .unwrap();

        // The key assertion: process_batch did not panic.
        // Before the fix, mvs.get_mut(name).unwrap() would panic because
        // the MV was moved into a branch but mv_branch_index wasn't updated.
        assert!(
            changes.len() > 0,
            "should produce some changes without panicking"
        );
    }

    /// replay_reducer_chain must replay ONLY the target reducer,
    /// not double-process other reducers that were already replayed.
    #[test]
    fn replay_reducer_chain_does_not_double_replay() {
        // Schema with two reducers: counter_a and counter_b, each with an MV
        let schema = Schema {
            modules: vec![],
            tables: vec![TableDef {
                name: "events".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "block_number".to_string(),
                        column_type: ColumnType::UInt64,
                    },
                    ColumnDef {
                        name: "pool".to_string(),
                        column_type: ColumnType::String,
                    },
                    ColumnDef {
                        name: "amount".to_string(),
                        column_type: ColumnType::Float64,
                    },
                ],
                virtual_table: false,
            }],
            reducers: vec![
                ReducerDef {
                    name: "counter_a".to_string(),
                    source: "events".to_string(),
                    group_by: vec!["pool".to_string()],
                    state: vec![StateField {
                        name: "total".to_string(),
                        column_type: ColumnType::Float64,
                        default: "0".to_string(),
                    }],
                    requires: vec![],
                    body: ReducerBody::EventRules {
                        when_blocks: vec![WhenBlock {
                            condition: Expr::Int(1),
                            lets: vec![],
                            sets: vec![(
                                "total".to_string(),
                                Expr::BinaryOp {
                                    left: Box::new(Expr::StateRef("total".into())),
                                    op: BinaryOp::Add,
                                    right: Box::new(Expr::RowRef("amount".into())),
                                },
                            )],
                            emits: vec![("total".to_string(), Expr::StateRef("total".into()))],
                        }],
                        always_emit: None,
                    },
                },
                ReducerDef {
                    name: "counter_b".to_string(),
                    source: "events".to_string(),
                    group_by: vec!["pool".to_string()],
                    state: vec![StateField {
                        name: "total".to_string(),
                        column_type: ColumnType::Float64,
                        default: "0".to_string(),
                    }],
                    requires: vec![],
                    body: ReducerBody::EventRules {
                        when_blocks: vec![WhenBlock {
                            condition: Expr::Int(1),
                            lets: vec![],
                            sets: vec![(
                                "total".to_string(),
                                Expr::BinaryOp {
                                    left: Box::new(Expr::StateRef("total".into())),
                                    op: BinaryOp::Add,
                                    right: Box::new(Expr::RowRef("amount".into())),
                                },
                            )],
                            emits: vec![("total".to_string(), Expr::StateRef("total".into()))],
                        }],
                        always_emit: None,
                    },
                },
            ],
            materialized_views: vec![
                MVDef {
                    name: "mv_a".to_string(),
                    source: "counter_a".to_string(),
                    select: vec![
                        SelectItem {
                            expr: SelectExpr::Column("pool".into()),
                            alias: None,
                        },
                        SelectItem {
                            expr: SelectExpr::Agg(AggFunc::Sum, Some("total".into())),
                            alias: Some("sum_a".into()),
                        },
                    ],
                    group_by: vec!["pool".into()],
                    sliding_window: None,
                },
                MVDef {
                    name: "mv_b".to_string(),
                    source: "counter_b".to_string(),
                    select: vec![
                        SelectItem {
                            expr: SelectExpr::Column("pool".into()),
                            alias: None,
                        },
                        SelectItem {
                            expr: SelectExpr::Agg(AggFunc::Sum, Some("total".into())),
                            alias: Some("sum_b".into()),
                        },
                    ],
                    group_by: vec!["pool".into()],
                    sliding_window: None,
                },
            ],
        };

        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&schema, storage.clone()).unwrap();

        // Process block 1000
        let rows = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(100.0)),
        ])];
        engine.process_batch("events", 1000, rows).unwrap();

        // Full replay (simulates open() recovery) — both reducers see block 1000
        let mut engine2 = SettleEngine::new(&schema, storage.clone()).unwrap();
        engine2.replay_unfinalized(1000, 1000).unwrap();

        // Now replay counter_a ONLY — neither reducer may count block 1000 twice
        engine2.replay_reducer_chain("counter_a", 999, 1000).unwrap();

        // Process block 1001 to get changes
        let rows2 = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(50.0)),
        ])];
        let (changes, _) = engine2.process_batch("events", 1001, rows2).unwrap();

        // counter_b was replayed once (full replay). State after block 1000 = 100.
        // Block 1001 adds 50 → state = 150, emits total=150.
        // mv_b SUM(total) = 100 (block 1000) + 150 (block 1001) = 250.
        // If counter_b were double-replayed, block 1000 state = 200,
        // block 1001 state = 250, SUM = 200 + 250 = 450.
        let mv_b: Vec<_> = changes.iter().filter(|d| d.table() == "mv_b").collect();
        assert!(!mv_b.is_empty(), "mv_b should have changes");
        let sum_b = mv_b
            .last()
            .unwrap()
            .get("sum_b")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        assert!(
            (sum_b - 250.0).abs() < 0.01,
            "counter_b NOT double-replayed: expected 250, got {sum_b}"
        );

        let mv_a: Vec<_> = changes.iter().filter(|d| d.table() == "mv_a").collect();
        let sum_a = mv_a.last().unwrap().get("sum_a").and_then(|v| v.as_f64()).unwrap();
        assert!(
            (sum_a - 250.0).abs() < 0.01,
            "counter_a NOT double-replayed: expected 250, got {sum_a}"
        );
    }
}
