use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

#[cfg(feature = "rayon")]
use rayon::prelude::*;

// wasm32 does not support std::time::Instant. Use a zero-cost stub instead.
#[cfg(not(target_arch = "wasm32"))]
fn start_timer() -> std::time::Instant {
    std::time::Instant::now()
}
#[cfg(not(target_arch = "wasm32"))]
fn elapsed_ms(t: std::time::Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}
#[cfg(target_arch = "wasm32")]
fn start_timer() -> () {}
#[cfg(target_arch = "wasm32")]
fn elapsed_ms(_: ()) -> f64 {
    0.0
}

use crate::error::{Error, Result};
use crate::schema::ast::Schema;
use crate::storage::StorageBackend;
use crate::storage::StorageWriteBatch;
use crate::types::{
    BlockCursor, BlockNumber, ColumnType, ChangeBatch, ChangeRecord, PerfNode, PerfNodeKind, Row,
    RowMap,
};

use super::mv::MVEngine;
use super::raw_table::RawTableEngine;
use super::reducer::ReducerEngine;

/// Processing order node — topologically sorted.
#[derive(Debug)]
enum PipelineNode {
    RawTable(String),
    Reducer(String),
    MV(String),
}

/// A node whose finalized state can be persisted independently of all others.
/// Reducers and MVs write disjoint storage keys, so finalize work fans out
/// across the rayon pool (each into its own write batch, merged afterward).
#[cfg(feature = "rayon")]
trait FinalizeNode: Send {
    fn finalize_into(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch, persist: bool);
}

#[cfg(feature = "rayon")]
impl FinalizeNode for ReducerEngine {
    fn finalize_into(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch, persist: bool) {
        self.finalize(block, batch, persist);
    }
}

#[cfg(feature = "rayon")]
impl FinalizeNode for MVEngine {
    fn finalize_into(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch, persist: bool) {
        self.finalize(block, batch, persist);
    }
}

/// An independent reducer→MV chain that can be processed in parallel.
/// Engines are stored inline to avoid HashMap extraction/reinsertion per batch.
struct PipelineBranch {
    reducer_name: String,
    reducer: ReducerEngine,
    mv_entries: Vec<(String, MVEngine)>,
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
    /// to disk as `META_FINALIZED_BLOCK` (Option A) so recovery replays from
    /// here. Equals `finalized_block` unless backfill deferral is active.
    durable_block: BlockNumber,
    /// True when the pipeline contains no sliding-window MV and no external
    /// reducer, so backfill persist-deferral is safe. Computed at construction
    /// (and after `add_reducer`). When false, every finalize persists
    /// (durable == finalized), exactly as before this feature.
    defer_allowed: bool,
    /// True once `finalize()` has run at least once (or state restored from
    /// disk). Lets `finalize(N)` skip work when N hasn't advanced past the
    /// last finalized block — heartbeat ingests with stale `finalized_head`
    /// otherwise re-serialize every MV group state on every call.
    has_finalized: bool,
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
            durable_block: 0,
            defer_allowed,
            has_finalized: false,
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
    ) -> Result<(Vec<ChangeRecord>, PerfNode)> {
        self.process_batch_inner(table, block, row_maps, None)
    }

    /// Process a batch, deferring raw row storage writes to the given WriteBatch.
    pub fn process_batch_deferred(
        &mut self,
        table: &str,
        block: BlockNumber,
        row_maps: Vec<RowMap>,
        write_batch: &mut StorageWriteBatch,
    ) -> Result<(Vec<ChangeRecord>, PerfNode)> {
        self.process_batch_inner(table, block, row_maps, Some(write_batch))
    }

    fn process_batch_inner(
        &mut self,
        table: &str,
        block: BlockNumber,
        row_maps: Vec<RowMap>,
        write_batch: Option<&mut StorageWriteBatch>,
    ) -> Result<(Vec<ChangeRecord>, PerfNode)> {
        let pipeline_start = start_timer();
        let mut perf_children: Vec<PerfNode> = Vec::new();

        if !self.raw_tables.contains_key(table) {
            return Err(Error::InvalidOperation(format!("unknown table: {table}")));
        }

        let mut all_changes = Vec::new();

        // Phase 1: Raw table ingest
        let raw_start = start_timer();
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

        perf_children.push(PerfNode {
            kind: PerfNodeKind::RawTable,
            name: table.to_string(),
            duration_ms: elapsed_ms(raw_start),
            children: vec![],
        });

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
                    let mv_start = start_timer();
                    let mv = self.mvs.get_mut(mv_name).unwrap();
                    let source = mv.source().to_string();
                    if let Some(source_rows) = output_rows.get(&source) {
                        let changes = mv.process_block(block, source_rows);
                        all_changes.extend(changes);
                    }
                    perf_children.push(PerfNode {
                        kind: PerfNodeKind::MV,
                        name: mv_name.clone(),
                        duration_ms: elapsed_ms(mv_start),
                        children: vec![],
                    });
                }

                // Phase 2b: Process reducer branches in parallel.
                let parallel_start = start_timer();
                if self.branches.len() == 2 {
                    let (first, second) = self.branches.split_at_mut(1);
                    let branch_0 = &mut first[0];
                    let branch_1 = &mut second[0];

                    let (result_0, result_1) = rayon::join(
                        || -> Result<(Vec<ChangeRecord>, PerfNode)> {
                            let r_start = start_timer();
                            let source = branch_0.reducer.source();
                            let mut changes = Vec::new();
                            let mut mv_nodes = Vec::new();
                            if let Some(rows) = output_rows.get(source) {
                                let enriched = branch_0.reducer.process_block_maps(block, rows)?;
                                if !enriched.is_empty() {
                                    for (mv_name, mv) in branch_0.mv_entries.iter_mut() {
                                        let mv_start = start_timer();
                                        changes.extend(mv.process_block(block, &enriched));
                                        mv_nodes.push(PerfNode {
                                            kind: PerfNodeKind::MV,
                                            name: mv_name.clone(),
                                            duration_ms: elapsed_ms(mv_start),
                                            children: vec![],
                                        });
                                    }
                                }
                            }
                            Ok((
                                changes,
                                PerfNode {
                                    kind: PerfNodeKind::Reducer,
                                    name: branch_0.reducer_name.clone(),
                                    duration_ms: elapsed_ms(r_start),
                                    children: mv_nodes,
                                },
                            ))
                        },
                        || -> Result<(Vec<ChangeRecord>, PerfNode)> {
                            let r_start = start_timer();
                            let source = branch_1.reducer.source();
                            let mut changes = Vec::new();
                            let mut mv_nodes = Vec::new();
                            if let Some(rows) = output_rows.get(source) {
                                let enriched = branch_1.reducer.process_block_maps(block, rows)?;
                                if !enriched.is_empty() {
                                    for (mv_name, mv) in branch_1.mv_entries.iter_mut() {
                                        let mv_start = start_timer();
                                        changes.extend(mv.process_block(block, &enriched));
                                        mv_nodes.push(PerfNode {
                                            kind: PerfNodeKind::MV,
                                            name: mv_name.clone(),
                                            duration_ms: elapsed_ms(mv_start),
                                            children: vec![],
                                        });
                                    }
                                }
                            }
                            Ok((
                                changes,
                                PerfNode {
                                    kind: PerfNodeKind::Reducer,
                                    name: branch_1.reducer_name.clone(),
                                    duration_ms: elapsed_ms(r_start),
                                    children: mv_nodes,
                                },
                            ))
                        },
                    );

                    let (d0, p0) = result_0?;
                    let (d1, p1) = result_1?;
                    all_changes.extend(d0);
                    all_changes.extend(d1);
                    perf_children.push(PerfNode {
                        kind: PerfNodeKind::Parallel,
                        name: "parallel".to_string(),
                        duration_ms: elapsed_ms(parallel_start),
                        children: vec![p0, p1],
                    });
                } else {
                    // General N-branch parallel using par_iter_mut
                    let results: Vec<Result<(Vec<ChangeRecord>, PerfNode)>> = self
                        .branches
                        .par_iter_mut()
                        .map(|branch| {
                            let r_start = start_timer();
                            let source = branch.reducer.source();
                            let mut changes = Vec::new();
                            let mut mv_nodes = Vec::new();
                            if let Some(rows) = output_rows.get(source) {
                                let enriched = branch.reducer.process_block_maps(block, rows)?;
                                if !enriched.is_empty() {
                                    for (mv_name, mv) in branch.mv_entries.iter_mut() {
                                        let mv_start = start_timer();
                                        changes.extend(mv.process_block(block, &enriched));
                                        mv_nodes.push(PerfNode {
                                            kind: PerfNodeKind::MV,
                                            name: mv_name.clone(),
                                            duration_ms: elapsed_ms(mv_start),
                                            children: vec![],
                                        });
                                    }
                                }
                            }
                            Ok((
                                changes,
                                PerfNode {
                                    kind: PerfNodeKind::Reducer,
                                    name: branch.reducer_name.clone(),
                                    duration_ms: elapsed_ms(r_start),
                                    children: mv_nodes,
                                },
                            ))
                        })
                        .collect();

                    let mut branch_nodes = Vec::new();
                    for result in results {
                        let (d, p) = result?;
                        all_changes.extend(d);
                        branch_nodes.push(p);
                    }
                    perf_children.push(PerfNode {
                        kind: PerfNodeKind::Parallel,
                        name: "parallel".to_string(),
                        duration_ms: elapsed_ms(parallel_start),
                        children: branch_nodes,
                    });
                }
            } // #[cfg(feature = "rayon")]
        } else {
            // Sequential execution: process branches + remaining engines.
            // Track reducer name → perf_children index for nesting MV nodes.
            let mut reducer_perf_idx: HashMap<String, usize> = HashMap::new();

            for node in &self.pipeline {
                match node {
                    PipelineNode::RawTable(_) => {} // Already processed in Phase 1
                    PipelineNode::Reducer(name) => {
                        let r_start = start_timer();
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
                        let idx = perf_children.len();
                        reducer_perf_idx.insert(name.clone(), idx);
                        perf_children.push(PerfNode {
                            kind: PerfNodeKind::Reducer,
                            name: name.clone(),
                            duration_ms: elapsed_ms(r_start),
                            children: vec![],
                        });
                        if !enriched.is_empty() {
                            output_rows.insert(name.clone(), enriched);
                        }
                    }
                    PipelineNode::MV(name) => {
                        let mv_start = start_timer();
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
                        let mv_node = PerfNode {
                            kind: PerfNodeKind::MV,
                            name: name.clone(),
                            duration_ms: elapsed_ms(mv_start),
                            children: vec![],
                        };
                        // Nest under parent reducer if exists, otherwise add as sibling
                        if let Some(&idx) = reducer_perf_idx.get(&mv_source) {
                            perf_children[idx].children.push(mv_node);
                        } else {
                            perf_children.push(mv_node);
                        }
                    }
                }
            }
        }

        if self.latest_block.is_none_or(|b| block > b) {
            self.latest_block = Some(block);
        }

        let perf_node = PerfNode {
            kind: PerfNodeKind::Pipeline,
            name: table.to_string(),
            duration_ms: elapsed_ms(pipeline_start),
            children: perf_children,
        };

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
                                    mv.process_block(block, source_rows);
                                } else if let Some(raw_rows) = row_cache.get(&source) {
                                    let maps: Vec<RowMap> =
                                        raw_rows.iter().map(|r| r.to_map()).collect();
                                    mv.process_block(block, &maps);
                                }
                            } else {
                                let mv = self.mvs.get_mut(name).unwrap();
                                let source = mv.source().to_string();
                                if let Some(source_rows) = output_rows.get(&source) {
                                    mv.process_block(block, source_rows);
                                } else if let Some(raw_rows) = row_cache.get(&source) {
                                    let maps: Vec<RowMap> =
                                        raw_rows.iter().map(|r| r.to_map()).collect();
                                    mv.process_block(block, &maps);
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

    fn rollback_inner(
        &mut self,
        fork_point: BlockNumber,
        mut write_batch: Option<&mut StorageWriteBatch>,
    ) -> Result<Vec<ChangeRecord>> {
        // Guard: fork_point cannot raise latest_block. If the caller passes
        // a fork point above current latest (e.g. via stale future hashes that
        // somehow leaked into block_hashes), do nothing — no rollback work to
        // perform and latest_block must not advance without underlying data.
        let current_latest = self.latest_block.unwrap_or(0);
        if fork_point > current_latest {
            return Ok(Vec::new());
        }

        // Finality floor: a fork/rollback must never cross below the finalized
        // watermark — finalized state is irreversible. Pre-deferral this was
        // enforced implicitly because `finalize` pruned `block_hashes` to
        // `finalized_block`, so the resolver could not find a sub-finalized
        // ancestor. Backfill deferral now prunes only to `durable_block` (which
        // lags finality), so hashes for `(durable_block, finalized_block]`
        // survive and a fork signal could otherwise resolve below finality —
        // rolling derived state back past the durable watermark, corrupting the
        // recovery anchor and wedging the instance (durable > latest). Reject it
        // here at the single chokepoint all rollback paths funnel through. With
        // deferral off (`durable == finalized`) this can never fire for an
        // in-range fork, so behavior is identical to before the feature.
        if fork_point < self.finalized_block {
            return Err(Error::InvalidOperation(format!(
                "rollback target block {fork_point} is below finalized block {} — \
                 finalized state is irreversible",
                self.finalized_block
            )));
        }

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
        // Remove hashes for rolled-back blocks
        let after = self.block_hashes.split_off(&(fork_point + 1));
        drop(after);

        Ok(all_changes)
    }

    /// Finalize all state up to and including the given block.
    /// Reducer finalized state writes are collected into the provided batch.
    ///
    /// `persist`: when true (checkpoint / non-backfill), derived state is
    /// serialized into the batch and `durable_block` advances. When false
    /// (backfill deferral), the in-memory merge/prune still runs but disk
    /// persistence is deferred to the next checkpoint. db.rs decides `persist`
    /// (see backfill_checkpoint_interval) and must only pass false when
    /// `defer_allowed()` and the ingest is no-lag. See design v2.
    pub fn finalize(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch, persist: bool) {
        // Idempotent skip: nothing to do when finality hasn't advanced AND there
        // is nothing deferred to flush. The first call after open (or restore)
        // still runs so initial state lands on disk. A forced checkpoint
        // (`persist` with `durable_block < block`) must NOT be skipped even when
        // `block == finalized_block` — that is how deferred state catches up.
        let nothing_to_flush = !persist || self.durable_block >= block;
        if block == self.finalized_block && self.has_finalized && nothing_to_flush {
            return;
        }

        // Finalize is pure persistence: each reducer/MV serializes its own
        // group state into the batch with disjoint storage keys. These tasks
        // are independent, so fan them out across the rayon pool — each writes
        // into its own sub-batch which is merged afterward. This was the single
        // largest serial section on the main thread (~38% inclusive).
        #[cfg(feature = "rayon")]
        {
            let mut nodes: Vec<&mut dyn FinalizeNode> = Vec::new();
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

            if nodes.len() == 1 {
                nodes[0].finalize_into(block, batch, persist);
            } else if nodes.len() > 1 {
                // Force one rayon task per node (`with_max_len(1)`): the default
                // range split would put the two heavy nodes (heavy reducer +
                // its heavy MV, adjacent in `nodes`) in the same half and run
                // them on one worker — no parallelism. One task per node lets
                // work-stealing spread the heavy finalizes across workers.
                let sub_batches: Vec<StorageWriteBatch> = nodes
                    .par_iter_mut()
                    .with_max_len(1)
                    .map(|node| {
                        let mut sub = StorageWriteBatch::new();
                        node.finalize_into(block, &mut sub, persist);
                        sub
                    })
                    .collect();
                for sub in sub_batches {
                    batch.ops.extend(sub.ops);
                }
            }
        }

        #[cfg(not(feature = "rayon"))]
        for node in &self.pipeline {
            match node {
                PipelineNode::Reducer(name) => {
                    if let Some(branch) =
                        self.branch_index.get(name).map(|&i| &mut self.branches[i])
                    {
                        branch.reducer.finalize(block, batch, persist);
                    } else {
                        let reducer = self.reducers.get_mut(name).unwrap();
                        reducer.finalize(block, batch, persist);
                    }
                }
                PipelineNode::MV(name) => {
                    if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                        self.branches[bi].mv_entries[mi].1.finalize(block, batch, persist);
                    } else {
                        let mv = self.mvs.get_mut(name).unwrap();
                        mv.finalize(block, batch, persist);
                    }
                }
                PipelineNode::RawTable(_) => {
                    // Raw table finalization = eviction eligibility (not implemented yet)
                }
            }
        }

        self.finalized_block = block;
        self.has_finalized = true;
        if persist {
            // Durable watermark advances only when derived state was actually
            // written into this batch. Clamp to latest so it never exceeds the
            // highest block with persisted raw rows (gappy chains: F may exceed
            // latest — design v2 B2).
            let latest = self.latest_block.unwrap_or(block);
            self.durable_block = block.min(latest);
        }

        // Remove hashes for blocks below the DURABLE watermark (not finality).
        // Recovery restores `finalized == durable` and calls
        // `finalized_cursor()`, which needs `durable`'s hash — so we must keep
        // hashes >= durable_block, not >= finalized_block. With deferral off
        // (`durable == finalized`) this is identical to before. On gappy chains
        // (Solana-style) `latest_block` may be BELOW the prune point — the
        // caller knows finality out-of-band for a block whose data they haven't
        // ingested; the plain split_off would drop `latest_block`'s hash too,
        // leaving `latest_cursor()` empty. Preserve it explicitly.
        let prune_to = self.durable_block;
        let preserve_latest = self
            .latest_block
            .filter(|&latest| latest < prune_to)
            .and_then(|latest| self.block_hashes.get(&latest).map(|h| (latest, h.clone())));

        let old_hashes = self.block_hashes.split_off(&prune_to);
        self.block_hashes = old_hashes;

        if let Some((latest, hash)) = preserve_latest {
            self.block_hashes.insert(latest, hash);
        }
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

    /// Highest block whose derived state is durably persisted. Written to disk
    /// as `META_FINALIZED_BLOCK` so recovery replays raw rows from here.
    pub fn durable_block(&self) -> BlockNumber {
        self.durable_block
    }

    /// Whether backfill persist-deferral is structurally safe for this pipeline
    /// (no sliding-window MV, no external reducer).
    pub fn defer_allowed(&self) -> bool {
        self.defer_allowed
    }

    /// True once finalize has run at least once (or state was restored).
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
        // On-disk `META_FINALIZED_BLOCK` is the DURABLE watermark (Option A),
        // so restoring it sets durable == finalized: derived state on disk is
        // exactly as-of-`block`, and recovery replays raw rows from block+1.
        self.durable_block = block;
        // Restored state is on disk already; treat as finalized so the next
        // `finalize(same_block)` short-circuits like a normal idempotent call.
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
            // is irreversible). Under backfill deferral `block_hashes` retains
            // hashes for finalized-but-not-yet-durable blocks; resolving a fork
            // onto one of those would roll state back below finality. Resolving
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
/// See design v2 B4/B5.
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
        engine.finalize(1000, &mut batch, true);
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
        engine.finalize(1001, &mut batch, true);
        assert_eq!(engine.finalized_block(), 1001);

        // Below finalized → rejected, no mutation.
        assert!(engine.rollback(1000).is_err(), "rollback below finalized must error");
        assert_eq!(engine.latest_block(), 1002, "rejected rollback must not move latest");

        // At finalized → allowed (removes unfinalized block 1002 only).
        engine.rollback(1001).expect("rollback to finalized is allowed");
        assert_eq!(engine.latest_block(), 1001);
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
