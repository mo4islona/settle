use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::error::{Error, Result};
use crate::schema::ast::Schema;
use crate::storage::StorageBackend;
use crate::storage::StorageWriteBatch;
use crate::types::{
    BlockCursor, BlockNumber, ColumnType, ChangeBatch, ChangeRecord, PerfNode, PerfNodeKind, Row,
    RowMap,
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
/// storage keys, so the nodes run in parallel on the rayon pool.
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

/// An independent reducer→MV chain that can be processed in parallel.
/// Engines are stored inline to avoid HashMap extraction/reinsertion per batch.
struct PipelineBranch {
    reducer_name: String,
    reducer: ReducerEngine,
    mv_entries: Vec<(String, MVEngine)>,
}

impl PipelineBranch {
    /// Run the reducer on its source rows and feed its output to the branch's
    /// MVs.
    #[cfg(feature = "rayon")]
    fn process_block(
        &mut self,
        block: BlockNumber,
        output_rows: &HashMap<String, Vec<RowMap>>,
        perf: bool,
    ) -> Result<(Vec<ChangeRecord>, Option<PerfNode>)> {
        let timer = PerfTimer::start(perf);
        let mut changes = Vec::new();
        let mut mv_nodes = Vec::new();

        if let Some(rows) = output_rows.get(self.reducer.source()) {
            let enriched = self.reducer.process_block_maps(block, rows)?;
            if !enriched.is_empty() {
                for (mv_name, mv) in &mut self.mv_entries {
                    let mv_timer = PerfTimer::start(perf);
                    changes.extend(mv.process_block(block, &enriched));
                    mv_nodes.extend(mv_timer.stop(PerfNodeKind::MV, mv_name, vec![]));
                }
            }
        }

        let node = timer.stop(PerfNodeKind::Reducer, &self.reducer_name, mv_nodes);
        Ok((changes, node))
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
}

impl SettleEngine {
    /// Build the engine from a parsed schema and storage backend.
    pub fn new(schema: &Schema, storage: Arc<dyn StorageBackend>) -> Self {
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
                MVEngine::new(mv_def.clone(), storage.clone(), &source_col_types),
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

        Self {
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
            defer_allowed,
            has_finalized: false,
            collect_perf: true,
            block_hashes: BTreeMap::new(),
        }
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
        self.process_batch_inner(table, block, row_maps, None)
    }

    /// Process a batch, deferring raw row storage writes to the given WriteBatch.
    pub fn process_batch_deferred(
        &mut self,
        table: &str,
        block: BlockNumber,
        row_maps: Vec<RowMap>,
        write_batch: &mut StorageWriteBatch,
    ) -> Result<(Vec<ChangeRecord>, Option<PerfNode>)> {
        self.process_batch_inner(table, block, row_maps, Some(write_batch))
    }

    fn process_batch_inner(
        &mut self,
        table: &str,
        block: BlockNumber,
        row_maps: Vec<RowMap>,
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
            let changes = raw_eng.ingest_to_batch(block, &row_maps, batch, is_virtual)?;
            all_changes.extend(changes);
        } else if is_virtual {
            raw_eng.ingest_no_changes(block, &row_maps)?;
        } else {
            let changes = raw_eng.ingest(block, &row_maps)?;
            all_changes.extend(changes);
        }
        perf_children.extend(raw_timer.stop(PerfNodeKind::RawTable, table, vec![]));

        // Output rows for downstream consumption (reducers + MVs)
        let mut output_rows: HashMap<String, Vec<RowMap>> = HashMap::new();
        output_rows.insert(table.to_string(), row_maps);

        // Check if parallel branch execution is possible:
        // - 2+ branches, all reducers source from raw tables (not from each other),
        //   and no external reducers (which require main-thread JS callbacks).
        #[cfg(feature = "rayon")]
        let can_parallelize = self.branches.len() >= 2
            && self.branches.iter().all(|b| {
                self.raw_tables.contains_key(b.reducer.source()) && !b.reducer.is_external()
            });
        #[cfg(not(feature = "rayon"))]
        let can_parallelize = false;

        if can_parallelize {
            #[cfg(feature = "rayon")]
            {
                // Phase 2a: Process MVs that source directly from raw tables
                for mv_name in &self.direct_mvs {
                    let mv_timer = PerfTimer::start(perf);
                    let mv = self.mvs.get_mut(mv_name).unwrap();
                    let source = mv.source().to_string();
                    if let Some(source_rows) = output_rows.get(&source) {
                        let changes = mv.process_block(block, source_rows);
                        all_changes.extend(changes);
                    }
                    perf_children.extend(mv_timer.stop(PerfNodeKind::MV, mv_name, vec![]));
                }

                // Phase 2b: Process reducer branches in parallel.
                let parallel_timer = PerfTimer::start(perf);
                let mut branch_nodes = Vec::new();
                let mut take = |result: Result<(Vec<ChangeRecord>, Option<PerfNode>)>| {
                    let (changes, node) = result?;
                    all_changes.extend(changes);
                    branch_nodes.extend(node);
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
                        let enriched = if let Some(branch) =
                            self.branch_index.get(name).map(|&i| &mut self.branches[i])
                        {
                            let source = branch.reducer.source().to_string();
                            if let Some(source_rows) = output_rows.get(&source) {
                                branch.reducer.process_block_maps(block, source_rows)?
                            } else {
                                Vec::new()
                            }
                        } else {
                            let reducer = self.reducers.get_mut(name).unwrap();
                            let source = reducer.source().to_string();
                            if let Some(source_rows) = output_rows.get(&source) {
                                reducer.process_block_maps(block, source_rows)?
                            } else {
                                Vec::new()
                            }
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
                        let mv_source;
                        if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                            let mv = &mut self.branches[bi].mv_entries[mi].1;
                            mv_source = mv.source().to_string();
                            if let Some(source_rows) = output_rows.get(&mv_source) {
                                let changes = mv.process_block(block, source_rows);
                                all_changes.extend(changes);
                            }
                        } else {
                            let mv = self.mvs.get_mut(name).unwrap();
                            mv_source = mv.source().to_string();
                            if let Some(source_rows) = output_rows.get(&mv_source) {
                                let changes = mv.process_block(block, source_rows);
                                all_changes.extend(changes);
                            }
                        }
                        // Nest under parent reducer if exists, otherwise add as sibling
                        if let Some(node) = timer.stop(PerfNodeKind::MV, name, vec![]) {
                            match reducer_perf_idx.get(&mv_source) {
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

    /// Drop restored sliding-window data of blocks above `durable`. A window
    /// is persisted with the blocks it holds, final or not, and a fork can
    /// replace those blocks without another persist; replay rebuilds them
    /// from the raw rows instead.
    pub fn discard_unfinalized_window_state(&mut self, durable: BlockNumber) {
        let branch_mvs = self
            .branches
            .iter_mut()
            .flat_map(|b| b.mv_entries.iter_mut().map(|(_, mv)| mv));

        for mv in branch_mvs.chain(self.mvs.values_mut()) {
            if mv.is_sliding() {
                mv.rollback(durable);
            }
        }
    }

    /// Replay only a specific reducer and its direct downstream MVs.
    /// Note: only 1 level of MV depth (MV→MV chaining is not supported).
    /// Reset the named reducer and its downstream MVs to their in-memory
    /// state at `fork_point` (typically `finalized_block`). Called before
    /// `replay_unfinalized_for` so the replay rebuilds state from a clean
    /// baseline instead of accumulating on top of already-processed blocks.
    ///
    /// Without this reset, calling `replay_reducer` (or `set_reducer_runtime`)
    /// after the reducer has already processed unfinalized blocks would
    /// double-count: every emit fires twice, every aggregation tracks two
    /// contributions per block.
    ///
    /// Does NOT touch raw-table storage — this is a pure in-memory reset of
    /// the reducer + downstream MV state.
    pub fn reset_reducer_branch_for_replay(
        &mut self,
        reducer_name: &str,
        fork_point: BlockNumber,
    ) -> Result<()> {
        // Reset the reducer itself (covers both branch-hosted and HashMap-hosted).
        if let Some(idx) = self.branch_index.get(reducer_name).copied() {
            self.branches[idx].reducer.rollback(fork_point)?;
        } else if let Some(reducer) = self.reducers.get_mut(reducer_name) {
            reducer.rollback(fork_point)?;
        } else {
            return Err(Error::InvalidOperation(format!(
                "reset_reducer_branch_for_replay: unknown reducer '{reducer_name}'"
            )));
        }

        // Reset MVs that source from this reducer. `mv.rollback` returns
        // compensating change records, which we discard — replay will
        // re-emit them via `process_block`.
        let mv_names: Vec<String> = self
            .pipeline
            .iter()
            .filter_map(|node| {
                if let PipelineNode::MV(name) = node {
                    let source = if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                        self.branches[bi].mv_entries[mi].1.source().to_string()
                    } else if let Some(mv) = self.mvs.get(name) {
                        mv.source().to_string()
                    } else {
                        return None;
                    };
                    if source == reducer_name {
                        Some(name.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        for name in &mv_names {
            if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                let _ = self.branches[bi].mv_entries[mi].1.rollback(fork_point);
            } else if let Some(mv) = self.mvs.get_mut(name) {
                let _ = mv.rollback(fork_point);
            }
        }

        Ok(())
    }

    pub fn replay_unfinalized_for(
        &mut self,
        from_block: BlockNumber,
        to_block: BlockNumber,
        reducer_name: &str,
    ) -> Result<()> {
        // Find MV names that source from this reducer
        let mv_names: HashSet<String> = self
            .pipeline
            .iter()
            .filter_map(|node| {
                if let PipelineNode::MV(name) = node {
                    let source = if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                        self.branches[bi].mv_entries[mi].1.source().to_string()
                    } else if let Some(mv) = self.mvs.get(name) {
                        mv.source().to_string()
                    } else {
                        return None;
                    };
                    if source == reducer_name {
                        Some(name.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        let mut filter = HashSet::new();
        filter.insert(reducer_name.to_string());
        filter.extend(mv_names);
        self.replay_unfinalized_inner(from_block, to_block, Some(&filter))
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

        // Collect all (table_name, block, rows) across all raw tables.
        // Rows are stored as Vec<Row> — no to_map() conversion needed!
        let mut all_blocks: BTreeMap<BlockNumber, HashMap<String, Vec<Row>>> = BTreeMap::new();

        for (table_name, raw_eng) in &self.raw_tables {
            let rows_by_block = raw_eng.get_rows(from_block, to_block)?;
            for (block, rows) in rows_by_block {
                all_blocks
                    .entry(block)
                    .or_default()
                    .insert(table_name.clone(), rows);
            }
        }

        // Replay each block in order through reducers and MVs only
        for (block, tables) in all_blocks {
            for (table_name, rows) in tables {
                // Row cache for reducer input (indexed access)
                let mut row_cache: HashMap<String, Vec<Row>> = HashMap::new();
                row_cache.insert(table_name, rows);

                // Output from reducers (RowMaps for MV consumption)
                let mut output_rows: HashMap<String, Vec<RowMap>> = HashMap::new();

                for node in &self.pipeline {
                    match node {
                        PipelineNode::RawTable(_) => {
                            // Skip — rows already in storage
                        }
                        PipelineNode::Reducer(name) => {
                            // Skip if filtered out
                            if let Some(filter) = &only_nodes {
                                if !filter.contains(name.as_str()) {
                                    continue;
                                }
                            }

                            // Skip external reducers when no JS context is installed
                            let is_ext = self
                                .branch_index
                                .get(name)
                                .map(|&i| self.branches[i].reducer.needs_host_callback())
                                .unwrap_or_else(|| {
                                    self.reducers
                                        .get(name)
                                        .map(|r| r.needs_host_callback())
                                        .unwrap_or(false)
                                });

                            let enriched = if is_ext {
                                Vec::new()
                            } else if let Some(branch) =
                                self.branch_index.get(name).map(|&i| &mut self.branches[i])
                            {
                                let source = branch.reducer.source().to_string();
                                if let Some(source_rows) = row_cache.get(&source) {
                                    branch.reducer.process_block(block, source_rows)?
                                } else if let Some(source_maps) = output_rows.get(&source) {
                                    branch.reducer.process_block_maps(block, source_maps)?
                                } else {
                                    Vec::new()
                                }
                            } else {
                                let reducer = self.reducers.get_mut(name).unwrap();
                                let source = reducer.source().to_string();
                                if let Some(source_rows) = row_cache.get(&source) {
                                    reducer.process_block(block, source_rows)?
                                } else if let Some(source_maps) = output_rows.get(&source) {
                                    reducer.process_block_maps(block, source_maps)?
                                } else {
                                    Vec::new()
                                }
                            };
                            if !enriched.is_empty() {
                                output_rows.insert(name.clone(), enriched);
                            }
                        }
                        PipelineNode::MV(name) => {
                            // Skip if filtered out
                            if let Some(filter) = &only_nodes {
                                if !filter.contains(name.as_str()) {
                                    continue;
                                }
                            }

                            if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                                let mv = &mut self.branches[bi].mv_entries[mi].1;
                                let source = mv.source().to_string();
                                if let Some(source_rows) = output_rows.get(&source) {
                                    mv.replay_block(block, source_rows);
                                } else if let Some(raw_rows) = row_cache.get(&source) {
                                    let maps: Vec<RowMap> =
                                        raw_rows.iter().map(|r| r.to_map()).collect();
                                    mv.replay_block(block, &maps);
                                }
                            } else {
                                let mv = self.mvs.get_mut(name).unwrap();
                                let source = mv.source().to_string();
                                if let Some(source_rows) = output_rows.get(&source) {
                                    mv.replay_block(block, source_rows);
                                } else if let Some(raw_rows) = row_cache.get(&source) {
                                    let maps: Vec<RowMap> =
                                        raw_rows.iter().map(|r| r.to_map()).collect();
                                    mv.replay_block(block, &maps);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Roll back all state after fork_point.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Result<Vec<ChangeRecord>> {
        self.rollback_inner(fork_point, None)
    }

    /// Roll back all state after fork_point, deferring raw-row deletions
    /// to the provided write batch for atomic commit with metadata.
    pub fn rollback_to_batch(
        &mut self,
        fork_point: BlockNumber,
        batch: &mut StorageWriteBatch,
    ) -> Result<Vec<ChangeRecord>> {
        self.rollback_inner(fork_point, Some(batch))
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

        let all_changes = self.unwind_to(fork_point, write_batch)?;

        // Remove hashes for rolled-back blocks
        let after = self.block_hashes.split_off(&(fork_point + 1));
        drop(after);

        Ok(all_changes)
    }

    /// Undo the in-memory effects of a failed ingest, back to `block`: the
    /// latest block before the ingest processed anything.
    ///
    /// This is not a chain reorg, so the finality bound of `rollback` does
    /// not apply: the undone blocks were never finalized, and on gappy chains
    /// `finalized_block` can be above `block`. Block hashes stay as they are,
    /// because a failed ingest stores none, and the hashes above `block` can
    /// include the finalized head. Raw-row deletions go to a throwaway batch:
    /// the ingest's raw rows were never committed.
    pub fn undo_ingest(&mut self, block: BlockNumber) -> Result<()> {
        if block > self.latest_block() {
            return Ok(());
        }

        let mut throwaway = StorageWriteBatch::new();
        self.unwind_to(block, Some(&mut throwaway))?;
        Ok(())
    }

    /// Roll every pipeline node back to `fork_point` and make it the latest
    /// block. The callers check that the target is valid.
    fn unwind_to(
        &mut self,
        fork_point: BlockNumber,
        mut write_batch: Option<&mut StorageWriteBatch>,
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
                        branch.reducer.rollback(fork_point)?;
                    } else {
                        let reducer = self.reducers.get_mut(name).unwrap();
                        reducer.rollback(fork_point)?;
                    }
                }
                PipelineNode::RawTable(name) => {
                    let raw_engine = self.raw_tables.get(name).unwrap();
                    let changes = if let Some(ref mut batch) = write_batch {
                        raw_engine.rollback_to_batch(fork_point, batch)?
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

        let mut nodes = self.finalize_nodes();
        #[cfg(feature = "rayon")]
        nodes
            .par_iter_mut()
            .with_max_len(1)
            .for_each(|node| node.advance_finality(block));
        #[cfg(not(feature = "rayon"))]
        for node in &mut nodes {
            node.advance_finality(block);
        }

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
        let mut nodes = self.finalize_nodes();
        #[cfg(feature = "rayon")]
        if let [node] = nodes.as_mut_slice() {
            node.write_checkpoint(finalized, batch);
        } else {
            // One task per node: the default split could put the two heaviest
            // nodes, a reducer and its MV, in one task on one worker.
            let sub_batches: Vec<StorageWriteBatch> = nodes
                .par_iter_mut()
                .with_max_len(1)
                .map(|node| {
                    let mut sub = StorageWriteBatch::new();
                    node.write_checkpoint(finalized, &mut sub);
                    sub
                })
                .collect();
            for sub in sub_batches {
                batch.ops.extend(sub.ops);
            }
        }
        #[cfg(not(feature = "rayon"))]
        for node in &mut nodes {
            node.write_checkpoint(finalized, batch);
        }

        // On gappy chains finality can run ahead of the data. Without data
        // nothing is durable.
        if let Some(latest) = self.latest_block {
            self.durable_block = Some(finalized.min(latest));
        }
        self.prune_block_hashes();
    }

    /// Every reducer and MV.
    fn finalize_nodes(&mut self) -> Vec<NodeMut<'_>> {
        let mut nodes: Vec<NodeMut<'_>> = Vec::new();
        for branch in &mut self.branches {
            nodes.push(&mut branch.reducer);
            for (_, mv) in &mut branch.mv_entries {
                nodes.push(mv);
            }
        }
        for reducer in self.reducers.values_mut() {
            nodes.push(reducer);
        }
        for mv in self.mvs.values_mut() {
            nodes.push(mv);
        }
        nodes
    }

    /// Fork resolution stops at finality, so hashes below it only grow the
    /// metadata that every ingest writes. Two of them stay: a reopen restarts
    /// finality at the durable block and reads its hash, and on gappy chains
    /// `latest_cursor` reads the hash of a latest block below finality.
    fn prune_block_hashes(&mut self) {
        let finalized = self.finalized_block;
        let anchors = [self.durable_block, self.latest_block];
        self.block_hashes
            .retain(|&block, _| block >= finalized || anchors.contains(&Some(block)));
    }

    /// Create a ChangeBatch from a set of change records.
    pub fn make_batch(&mut self, records: Vec<ChangeRecord>) -> ChangeBatch {
        self.sequence += 1;
        // Group records by table name
        let mut tables: HashMap<String, Vec<ChangeRecord>> = HashMap::new();
        for record in records {
            tables.entry(record.table.clone()).or_default().push(record);
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
            self.block_hashes.insert(*number, hash.clone());
        }
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

/// Backfill persist-deferral is only safe when the pipeline has no
/// sliding-window MV (block_times + per-block aggs must persist atomically) and
/// no external reducer (replay skips them, so deferred state can't be rebuilt).
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
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage);

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
        let raw_changes: Vec<_> = changes.iter().filter(|d| d.table == "swaps").collect();
        let mv_changes: Vec<_> = changes.iter().filter(|d| d.table == "pool_volume").collect();

        assert_eq!(raw_changes.len(), 2);
        assert_eq!(mv_changes.len(), 1);
        assert_eq!(mv_changes[0].operation, ChangeOp::Insert);
        assert_eq!(
            mv_changes[0].values.get("total"),
            Some(&Value::Float64(300.0))
        );
    }

    #[test]
    fn full_pipeline_raw_reducer_mv() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&dex_schema(), storage);

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
            .filter(|d| d.table == "position_summary")
            .collect();
        assert_eq!(mv_changes.len(), 1);
        assert_eq!(mv_changes[0].operation, ChangeOp::Insert);
        assert_eq!(
            mv_changes[0].values.get("trade_count"),
            Some(&Value::UInt64(1))
        );
    }

    #[test]
    fn pipeline_rollback() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage);

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
        let mv_changes: Vec<_> = changes.iter().filter(|d| d.table == "pool_volume").collect();
        assert_eq!(mv_changes.len(), 1);
        assert_eq!(mv_changes[0].operation, ChangeOp::Update);
        assert_eq!(
            mv_changes[0].values.get("total"),
            Some(&Value::Float64(100.0))
        );

        // Raw table should emit delete change
        let raw_changes: Vec<_> = changes.iter().filter(|d| d.table == "swaps").collect();
        assert_eq!(raw_changes.len(), 1);
        assert_eq!(raw_changes[0].operation, ChangeOp::Delete);
    }

    #[test]
    fn pipeline_finalize() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage);

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
        let mv_changes: Vec<_> = changes.iter().filter(|d| d.table == "pool_volume").collect();
        assert_eq!(mv_changes.len(), 1);
        // After finalize(1000) + rollback(1001→1000): total should be 100
        assert_eq!(
            mv_changes[0].values.get("total"),
            Some(&Value::Float64(100.0))
        );
    }

    #[test]
    fn rollback_below_finalized_is_rejected() {
        // Finality floor (Cluster B): rollback_inner must refuse any fork point
        // below finalized_block — finalized state is irreversible. Rolling back
        // to == finalized is allowed (removes the unfinalized tail); below is an
        // error, regardless of which blocks still have retained hashes.
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage);
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
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage.clone());
        let swap = |amount: f64| {
            vec![HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(amount)),
            ])]
        };

        let mut batch = StorageWriteBatch::new();
        engine
            .process_batch_deferred("swaps", 3, swap(100.0), &mut batch)
            .unwrap();
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
            .undo_ingest(3)
            .expect("undoing a failed ingest is not bounded by finality");
        assert_eq!(engine.latest_block(), 3);
        assert_eq!(engine.finalized_cursor().map(|c| c.number), Some(100));

        // Block 4 again counts once.
        let (changes, _) = engine
            .process_batch_deferred("swaps", 4, swap(200.0), &mut StorageWriteBatch::new())
            .unwrap();
        let mv = changes.iter().find(|c| c.table == "pool_volume").unwrap();
        assert_eq!(mv.values.get("total"), Some(&Value::Float64(300.0)));
    }

    #[test]
    fn full_pipeline_rollback_and_reingest() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&dex_schema(), storage);

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
            .filter(|d| d.table == "position_summary")
            .collect();
        assert_eq!(mv_changes.len(), 1);
        assert_eq!(mv_changes[0].operation, ChangeOp::Update);
        assert_eq!(
            mv_changes[0].values.get("trade_count"),
            Some(&Value::UInt64(3))
        );
    }

    #[test]
    fn make_batch_increments_sequence() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage);

        let batch1 = engine.make_batch(vec![]);
        assert_eq!(batch1.sequence, 1);

        let batch2 = engine.make_batch(vec![]);
        assert_eq!(batch2.sequence, 2);
    }

    #[test]
    fn unknown_table_returns_error() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = SettleEngine::new(&simple_mv_only_schema(), storage);

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
        let mut engine = SettleEngine::new(&schema, storage);

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
        let summary_changes: Vec<_> = changes.iter().filter(|d| d.table == "summary").collect();
        assert_eq!(summary_changes.len(), 1);
        assert_eq!(
            summary_changes[0].values.get("latest_doubled"),
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

        let summary2: Vec<_> = changes2.iter().filter(|d| d.table == "summary").collect();
        assert_eq!(summary2.len(), 1);
        assert_eq!(
            summary2[0].values.get("latest_doubled"),
            Some(&Value::Float64(30.0))
        );

        // Rollback block 1001
        let rollback_changes = engine.rollback(1000).unwrap();
        let summary_rb: Vec<_> = rollback_changes
            .iter()
            .filter(|d| d.table == "summary")
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

        let summary3: Vec<_> = changes3.iter().filter(|d| d.table == "summary").collect();
        assert_eq!(summary3.len(), 1);
        assert_eq!(
            summary3[0].values.get("latest_doubled"),
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
        let mut engine = SettleEngine::new(&schema, storage.clone());

        // Process blocks — raw rows go to storage, MV gets data
        let _ = engine.process_batch("events", 1000, rows1).unwrap();
        let _ = engine.process_batch("events", 1001, rows2).unwrap();

        // Simulate crash recovery: create a fresh engine with the same storage
        // that has raw rows but no in-memory MV state.
        let mut engine2 = SettleEngine::new(&schema, storage);

        // Replay unfinalized blocks — MV sources directly from raw table
        engine2.replay_unfinalized(1000, 1001).unwrap();

        // After replay, MV should have accumulated 100 + 200 = 300
        // Process another block to get changes that confirm the state
        let rows3 = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(50.0)),
        ])];
        let (changes, _) = engine2.process_batch("events", 1002, rows3).unwrap();
        let mv_changes: Vec<_> = changes.iter().filter(|d| d.table == "pool_totals").collect();
        assert!(
            !mv_changes.is_empty(),
            "MV should produce changes after replay"
        );
        // Total should be 100 + 200 + 50 = 350
        assert_eq!(
            mv_changes.last().unwrap().values.get("total"),
            Some(&Value::Float64(350.0))
        );
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
        let mut engine = SettleEngine::new(&schema, storage.clone());

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

    /// replay_unfinalized_for must replay ONLY the target reducer,
    /// not double-process other reducers that were already replayed.
    #[test]
    fn replay_unfinalized_for_does_not_double_replay() {
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
        let mut engine = SettleEngine::new(&schema, storage.clone());

        // Process block 1000
        let rows = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(100.0)),
        ])];
        engine.process_batch("events", 1000, rows).unwrap();

        // Full replay (simulates open() recovery) — both reducers see block 1000
        let mut engine2 = SettleEngine::new(&schema, storage.clone());
        engine2.replay_unfinalized(1000, 1000).unwrap();

        // Now replay_for counter_a ONLY — counter_b must NOT be double-replayed
        engine2
            .replay_unfinalized_for(1000, 1000, "counter_a")
            .unwrap();

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
        let mv_b: Vec<_> = changes.iter().filter(|d| d.table == "mv_b").collect();
        assert!(!mv_b.is_empty(), "mv_b should have changes");
        let sum_b = mv_b
            .last()
            .unwrap()
            .values
            .get("sum_b")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        assert!(
            (sum_b - 250.0).abs() < 0.01,
            "counter_b NOT double-replayed: expected 250, got {sum_b}"
        );
    }
}
