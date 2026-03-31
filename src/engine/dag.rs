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
    BlockCursor, BlockNumber, ColumnType, DeltaBatch, DeltaRecord, PerfNode, PerfNodeKind, Row,
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

/// An independent reducer→MV chain that can be processed in parallel.
/// Engines are stored inline to avoid HashMap extraction/reinsertion per batch.
struct PipelineBranch {
    reducer_name: String,
    reducer: ReducerEngine,
    mv_entries: Vec<(String, MVEngine)>,
}

/// Top-level engine that wires the computation DAG:
/// Raw Tables → Reducers → Materialized Views
pub struct DeltaEngine {
    raw_tables: HashMap<String, RawTableEngine>,
    reducers: HashMap<String, ReducerEngine>,
    mvs: HashMap<String, MVEngine>,
    /// Tables marked as VIRTUAL — stored but no deltas emitted.
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
    /// Sequence number for delta batches.
    sequence: u64,
    /// Latest processed block number (for ordering/rollback logic).
    latest_block: Option<BlockNumber>,
    /// Finalized block number (for finalization logic).
    finalized_block: BlockNumber,
    /// Block number → hash for all known blocks.
    /// Populated by set_rollback_chain(). Used for fork resolution and cursors.
    block_hashes: BTreeMap<BlockNumber, String>,
}

impl DeltaEngine {
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

        Ok(())
    }

    /// Check if a reducer with the given name exists in the engine.
    pub fn has_reducer(&self, name: &str) -> bool {
        self.reducers.contains_key(name) || self.branch_index.contains_key(name)
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
    /// Cascades through reducers and MVs, returning all delta records.
    ///
    /// When multiple independent reducer branches exist (e.g. two reducers
    /// both sourcing from the same raw table), they are executed in parallel
    /// using rayon's thread pool.
    pub fn process_batch(
        &mut self,
        table: &str,
        block: BlockNumber,
        row_maps: Vec<RowMap>,
    ) -> Result<(Vec<DeltaRecord>, PerfNode)> {
        self.process_batch_inner(table, block, row_maps, None)
    }

    /// Process a batch, deferring raw row storage writes to the given WriteBatch.
    pub fn process_batch_deferred(
        &mut self,
        table: &str,
        block: BlockNumber,
        row_maps: Vec<RowMap>,
        write_batch: &mut StorageWriteBatch,
    ) -> Result<(Vec<DeltaRecord>, PerfNode)> {
        self.process_batch_inner(table, block, row_maps, Some(write_batch))
    }

    fn process_batch_inner(
        &mut self,
        table: &str,
        block: BlockNumber,
        row_maps: Vec<RowMap>,
        write_batch: Option<&mut StorageWriteBatch>,
    ) -> Result<(Vec<DeltaRecord>, PerfNode)> {
        let pipeline_start = start_timer();
        let mut perf_children: Vec<PerfNode> = Vec::new();

        if !self.raw_tables.contains_key(table) {
            return Err(Error::InvalidOperation(format!("unknown table: {table}")));
        }

        let mut all_deltas = Vec::new();

        // Phase 1: Raw table ingest
        let raw_start = start_timer();
        let raw_eng = self.raw_tables.get(table).unwrap();
        let is_virtual = self.virtual_tables.contains(table);
        if let Some(batch) = write_batch {
            let deltas = raw_eng.ingest_to_batch(block, &row_maps, batch, is_virtual)?;
            all_deltas.extend(deltas);
        } else if is_virtual {
            raw_eng.ingest_no_deltas(block, &row_maps)?;
        } else {
            let deltas = raw_eng.ingest(block, &row_maps)?;
            all_deltas.extend(deltas);
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
                        let deltas = mv.process_block(block, source_rows);
                        all_deltas.extend(deltas);
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
                        || -> Result<(Vec<DeltaRecord>, PerfNode)> {
                            let r_start = start_timer();
                            let source = branch_0.reducer.source();
                            let mut deltas = Vec::new();
                            let mut mv_nodes = Vec::new();
                            if let Some(rows) = output_rows.get(source) {
                                let enriched = branch_0.reducer.process_block_maps(block, rows)?;
                                if !enriched.is_empty() {
                                    for (mv_name, mv) in branch_0.mv_entries.iter_mut() {
                                        let mv_start = start_timer();
                                        deltas.extend(mv.process_block(block, &enriched));
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
                                deltas,
                                PerfNode {
                                    kind: PerfNodeKind::Reducer,
                                    name: branch_0.reducer_name.clone(),
                                    duration_ms: elapsed_ms(r_start),
                                    children: mv_nodes,
                                },
                            ))
                        },
                        || -> Result<(Vec<DeltaRecord>, PerfNode)> {
                            let r_start = start_timer();
                            let source = branch_1.reducer.source();
                            let mut deltas = Vec::new();
                            let mut mv_nodes = Vec::new();
                            if let Some(rows) = output_rows.get(source) {
                                let enriched = branch_1.reducer.process_block_maps(block, rows)?;
                                if !enriched.is_empty() {
                                    for (mv_name, mv) in branch_1.mv_entries.iter_mut() {
                                        let mv_start = start_timer();
                                        deltas.extend(mv.process_block(block, &enriched));
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
                                deltas,
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
                    all_deltas.extend(d0);
                    all_deltas.extend(d1);
                    perf_children.push(PerfNode {
                        kind: PerfNodeKind::Parallel,
                        name: "parallel".to_string(),
                        duration_ms: elapsed_ms(parallel_start),
                        children: vec![p0, p1],
                    });
                } else {
                    // General N-branch parallel using par_iter_mut
                    let results: Vec<Result<(Vec<DeltaRecord>, PerfNode)>> = self
                        .branches
                        .par_iter_mut()
                        .map(|branch| {
                            let r_start = start_timer();
                            let source = branch.reducer.source();
                            let mut deltas = Vec::new();
                            let mut mv_nodes = Vec::new();
                            if let Some(rows) = output_rows.get(source) {
                                let enriched = branch.reducer.process_block_maps(block, rows)?;
                                if !enriched.is_empty() {
                                    for (mv_name, mv) in branch.mv_entries.iter_mut() {
                                        let mv_start = start_timer();
                                        deltas.extend(mv.process_block(block, &enriched));
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
                                deltas,
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
                        all_deltas.extend(d);
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
                                let deltas = mv.process_block(block, source_rows);
                                all_deltas.extend(deltas);
                            }
                        } else {
                            let mv = self.mvs.get_mut(name).unwrap();
                            mv_source = mv.source().to_string();
                            if let Some(source_rows) = output_rows.get(&mv_source) {
                                let deltas = mv.process_block(block, source_rows);
                                all_deltas.extend(deltas);
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

        Ok((all_deltas, perf_node))
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
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Result<Vec<DeltaRecord>> {
        self.rollback_inner(fork_point, None)
    }

    /// Roll back all state after fork_point, deferring raw-row deletions
    /// to the provided write batch for atomic commit with metadata.
    pub fn rollback_to_batch(
        &mut self,
        fork_point: BlockNumber,
        batch: &mut StorageWriteBatch,
    ) -> Result<Vec<DeltaRecord>> {
        self.rollback_inner(fork_point, Some(batch))
    }

    fn rollback_inner(
        &mut self,
        fork_point: BlockNumber,
        mut write_batch: Option<&mut StorageWriteBatch>,
    ) -> Result<Vec<DeltaRecord>> {
        let mut all_deltas = Vec::new();

        // Roll back in reverse pipeline order
        for node in self.pipeline.iter().rev() {
            match node {
                PipelineNode::MV(name) => {
                    if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                        all_deltas.extend(self.branches[bi].mv_entries[mi].1.rollback(fork_point));
                    } else {
                        let mv = self.mvs.get_mut(name).unwrap();
                        all_deltas.extend(mv.rollback(fork_point));
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
                    let deltas = if let Some(ref mut batch) = write_batch {
                        raw_engine.rollback_to_batch(fork_point, batch)?
                    } else {
                        raw_engine.rollback(fork_point)?
                    };
                    if !self.virtual_tables.contains(name) {
                        all_deltas.extend(deltas);
                    }
                }
            }
        }

        self.latest_block = Some(fork_point);
        // Remove hashes for rolled-back blocks
        let after = self.block_hashes.split_off(&(fork_point + 1));
        drop(after);

        Ok(all_deltas)
    }

    /// Finalize all state up to and including the given block.
    /// Reducer finalized state writes are collected into the provided batch.
    pub fn finalize(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch) {
        for node in &self.pipeline {
            match node {
                PipelineNode::Reducer(name) => {
                    if let Some(branch) =
                        self.branch_index.get(name).map(|&i| &mut self.branches[i])
                    {
                        branch.reducer.finalize(block, batch);
                    } else {
                        let reducer = self.reducers.get_mut(name).unwrap();
                        reducer.finalize(block, batch);
                    }
                }
                PipelineNode::MV(name) => {
                    if let Some(&(bi, mi)) = self.mv_branch_index.get(name) {
                        self.branches[bi].mv_entries[mi].1.finalize(block, batch);
                    } else {
                        let mv = self.mvs.get_mut(name).unwrap();
                        mv.finalize(block, batch);
                    }
                }
                PipelineNode::RawTable(_) => {
                    // Raw table finalization = eviction eligibility (not implemented yet)
                }
            }
        }

        self.finalized_block = block;

        // Remove hashes for blocks below finalized (keep finalized itself as anchor)
        let old_hashes = self.block_hashes.split_off(&block);
        self.block_hashes = old_hashes;
    }

    /// Create a DeltaBatch from a set of delta records.
    pub fn make_batch(&mut self, records: Vec<DeltaRecord>) -> DeltaBatch {
        self.sequence += 1;
        // Group records by table name
        let mut tables: HashMap<String, Vec<DeltaRecord>> = HashMap::new();
        for record in records {
            tables.entry(record.table.clone()).or_default().push(record);
        }
        DeltaBatch {
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
    use crate::types::{ColumnType, DeltaOperation, RowMap, Value};

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
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

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

        let (deltas, _) = engine.process_batch("swaps", 1000, rows).unwrap();

        // Should have: 2 raw inserts + 1 MV insert
        let raw_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "swaps").collect();
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "pool_volume").collect();

        assert_eq!(raw_deltas.len(), 2);
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Insert);
        assert_eq!(
            mv_deltas[0].values.get("total"),
            Some(&Value::Float64(300.0))
        );
    }

    #[test]
    fn full_pipeline_raw_reducer_mv() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&dex_schema(), storage);

        // Block 1000: alice buys 10 @ 2000
        let (deltas, _) = engine
            .process_batch(
                "trades",
                1000,
                vec![make_trade("alice", "buy", 10.0, 2000.0)],
            )
            .unwrap();

        // Raw insert + MV insert (position_summary)
        let mv_deltas: Vec<_> = deltas
            .iter()
            .filter(|d| d.table == "position_summary")
            .collect();
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Insert);
        assert_eq!(
            mv_deltas[0].values.get("trade_count"),
            Some(&Value::UInt64(1))
        );
    }

    #[test]
    fn pipeline_rollback() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

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
        let deltas = engine.rollback(1000).unwrap();

        // MV should update back to 100
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "pool_volume").collect();
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Update);
        assert_eq!(
            mv_deltas[0].values.get("total"),
            Some(&Value::Float64(100.0))
        );

        // Raw table should emit delete delta
        let raw_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "swaps").collect();
        assert_eq!(raw_deltas.len(), 1);
        assert_eq!(raw_deltas[0].operation, DeltaOperation::Delete);
    }

    #[test]
    fn pipeline_finalize() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

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
        engine.finalize(1000, &mut batch);
        assert_eq!(engine.finalized_block(), 1000);

        // Rollback to 1000 should only remove block 1001
        let deltas = engine.rollback(1000).unwrap();
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "pool_volume").collect();
        assert_eq!(mv_deltas.len(), 1);
        // After finalize(1000) + rollback(1001→1000): total should be 100
        assert_eq!(
            mv_deltas[0].values.get("total"),
            Some(&Value::Float64(100.0))
        );
    }

    #[test]
    fn full_pipeline_rollback_and_reingest() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&dex_schema(), storage);

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
        let (deltas, _) = engine
            .process_batch(
                "trades",
                1002,
                vec![make_trade("alice", "sell", 3.0, 2300.0)],
            )
            .unwrap();

        // MV should get updated with new trade data
        let mv_deltas: Vec<_> = deltas
            .iter()
            .filter(|d| d.table == "position_summary")
            .collect();
        assert_eq!(mv_deltas.len(), 1);
        assert_eq!(mv_deltas[0].operation, DeltaOperation::Update);
        assert_eq!(
            mv_deltas[0].values.get("trade_count"),
            Some(&Value::UInt64(3))
        );
    }

    #[test]
    fn make_batch_increments_sequence() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

        let batch1 = engine.make_batch(vec![]);
        assert_eq!(batch1.sequence, 1);

        let batch2 = engine.make_batch(vec![]);
        assert_eq!(batch2.sequence, 2);
    }

    #[test]
    fn unknown_table_returns_error() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = DeltaEngine::new(&simple_mv_only_schema(), storage);

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
        let mut engine = DeltaEngine::new(&schema, storage);

        // Block 1: alice deposits 10
        let (deltas, _) = engine
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
        let summary_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "summary").collect();
        assert_eq!(summary_deltas.len(), 1);
        assert_eq!(
            summary_deltas[0].values.get("latest_doubled"),
            Some(&Value::Float64(20.0))
        );

        // Block 2: alice deposits 5 more (total=15, doubled=30)
        let (deltas2, _) = engine
            .process_batch(
                "events",
                1001,
                vec![HashMap::from([
                    ("user".to_string(), Value::String("alice".into())),
                    ("amount".to_string(), Value::Float64(5.0)),
                ])],
            )
            .unwrap();

        let summary2: Vec<_> = deltas2.iter().filter(|d| d.table == "summary").collect();
        assert_eq!(summary2.len(), 1);
        assert_eq!(
            summary2[0].values.get("latest_doubled"),
            Some(&Value::Float64(30.0))
        );

        // Rollback block 1001
        let rollback_deltas = engine.rollback(1000).unwrap();
        let summary_rb: Vec<_> = rollback_deltas
            .iter()
            .filter(|d| d.table == "summary")
            .collect();
        assert!(
            !summary_rb.is_empty(),
            "rollback should produce summary deltas"
        );

        // Re-ingest block 1001: alice deposits 20 (total=30, doubled=60)
        let (deltas3, _) = engine
            .process_batch(
                "events",
                1001,
                vec![HashMap::from([
                    ("user".to_string(), Value::String("alice".into())),
                    ("amount".to_string(), Value::Float64(20.0)),
                ])],
            )
            .unwrap();

        let summary3: Vec<_> = deltas3.iter().filter(|d| d.table == "summary").collect();
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
        let mut engine = DeltaEngine::new(&schema, storage.clone());

        // Process blocks — raw rows go to storage, MV gets data
        let _ = engine.process_batch("events", 1000, rows1).unwrap();
        let _ = engine.process_batch("events", 1001, rows2).unwrap();

        // Simulate crash recovery: create a fresh engine with the same storage
        // that has raw rows but no in-memory MV state.
        let mut engine2 = DeltaEngine::new(&schema, storage);

        // Replay unfinalized blocks — MV sources directly from raw table
        engine2.replay_unfinalized(1000, 1001).unwrap();

        // After replay, MV should have accumulated 100 + 200 = 300
        // Process another block to get deltas that confirm the state
        let rows3 = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(50.0)),
        ])];
        let (deltas, _) = engine2.process_batch("events", 1002, rows3).unwrap();
        let mv_deltas: Vec<_> = deltas.iter().filter(|d| d.table == "pool_totals").collect();
        assert!(
            !mv_deltas.is_empty(),
            "MV should produce deltas after replay"
        );
        // Total should be 100 + 200 + 50 = 350
        assert_eq!(
            mv_deltas.last().unwrap().values.get("total"),
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
        let mut engine = DeltaEngine::new(&schema, storage.clone());

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
        let (deltas, _) = engine
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
            deltas.len() > 0,
            "should produce some deltas without panicking"
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
        let mut engine = DeltaEngine::new(&schema, storage.clone());

        // Process block 1000
        let rows = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(100.0)),
        ])];
        engine.process_batch("events", 1000, rows).unwrap();

        // Full replay (simulates open() recovery) — both reducers see block 1000
        let mut engine2 = DeltaEngine::new(&schema, storage.clone());
        engine2.replay_unfinalized(1000, 1000).unwrap();

        // Now replay_for counter_a ONLY — counter_b must NOT be double-replayed
        engine2
            .replay_unfinalized_for(1000, 1000, "counter_a")
            .unwrap();

        // Process block 1001 to get deltas
        let rows2 = vec![RowMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::Float64(50.0)),
        ])];
        let (deltas, _) = engine2.process_batch("events", 1001, rows2).unwrap();

        // counter_b was replayed once (full replay). State after block 1000 = 100.
        // Block 1001 adds 50 → state = 150, emits total=150.
        // mv_b SUM(total) = 100 (block 1000) + 150 (block 1001) = 250.
        // If counter_b were double-replayed, block 1000 state = 200,
        // block 1001 state = 250, SUM = 200 + 250 = 450.
        let mv_b: Vec<_> = deltas.iter().filter(|d| d.table == "mv_b").collect();
        assert!(!mv_b.is_empty(), "mv_b should have deltas");
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
