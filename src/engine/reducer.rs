use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::error::Result;
use crate::reducer_runtime::event_rules::EventRulesRuntime;
use crate::reducer_runtime::lua::LuaRuntime;
use crate::reducer_runtime::{GroupBatch, ReducerRuntime};
use crate::schema::ast::{ReducerBody, ReducerDef};
use crate::storage::{self, StorageBackend, StorageWriteBatch};
use crate::types::{BlockNumber, ColumnId, ColumnRegistry, GroupKey, Row, RowMap, Value};

type State = HashMap<String, Value>;

/// Orchestrates a single reducer: state management, snapshots, rollback, and output.
///
/// State is kept entirely in memory during normal processing. Storage is only
/// used for persisting finalized state. Block-level snapshots are held in an
/// in-memory `BTreeMap` so that rollback can restore to any unfinalized block
/// without serialization overhead.
pub struct ReducerEngine {
    def: ReducerDef,
    runtime: Box<dyn ReducerRuntime>,
    storage: Arc<dyn StorageBackend>,
    /// Cached default state (computed once from def.state).
    default_state: State,
    /// Current hot state per group key.
    state_cache: FxHashMap<Vec<u8>, State>,
    /// In-memory state snapshots: group_key -> (block -> state).
    /// Only contains unfinalized blocks. Used for rollback.
    block_snapshots: FxHashMap<Vec<u8>, BTreeMap<BlockNumber, State>>,
    /// Tracks which blocks have been processed and which group keys were touched.
    /// BTreeMap for O(log N) range queries during rollback/finalize.
    block_groups: BTreeMap<BlockNumber, HashSet<Vec<u8>>>,
    /// Pre-computed column IDs for group-by columns (resolved against source registry).
    /// Enables direct Vec indexing instead of HashMap lookups.
    group_by_ids: Vec<Option<ColumnId>>,
    /// Source table's column registry, used for inline RowMap→Row conversion.
    source_registry: Arc<ColumnRegistry>,
    /// When true, this reducer sources from another reducer (not a raw table).
    /// The source_registry and group_by_ids are rebuilt per-batch from actual input data.
    chained: bool,
}

impl ReducerEngine {
    pub fn new(
        def: ReducerDef,
        storage: Arc<dyn StorageBackend>,
        source_registry: &crate::types::ColumnRegistry,
        modules: &[(String, String)],
    ) -> Self {
        let runtime: Box<dyn ReducerRuntime> = Self::make_runtime(&def, source_registry, modules);

        // Pre-compute column IDs for group-by columns
        let group_by_ids: Vec<Option<ColumnId>> = def
            .group_by
            .iter()
            .map(|col| source_registry.get_id(col))
            .collect();

        let source_registry = Arc::new(source_registry.clone());
        let default_state = compute_default_state(&def);

        Self {
            def,
            runtime,
            storage,
            default_state,
            state_cache: FxHashMap::default(),
            block_snapshots: FxHashMap::default(),
            block_groups: BTreeMap::new(),
            group_by_ids,
            source_registry,
            chained: false,
        }
    }

    /// Build a runtime for the given reducer definition.
    fn make_runtime(
        def: &ReducerDef,
        source_registry: &ColumnRegistry,
        modules: &[(String, String)],
    ) -> Box<dyn ReducerRuntime> {
        match &def.body {
            ReducerBody::EventRules { .. } => Box::new(EventRulesRuntime::new(&def.body)),
            ReducerBody::Lua { script } => {
                let required_modules: Vec<(String, String)> = def
                    .requires
                    .iter()
                    .filter_map(|name| modules.iter().find(|(n, _)| n == name).cloned())
                    .collect();
                let state_fields: Vec<String> = def.state.iter().map(|f| f.name.clone()).collect();
                let state_types: Vec<(String, crate::types::ColumnType)> = def
                    .state
                    .iter()
                    .map(|f| (f.name.clone(), f.column_type.clone()))
                    .collect();
                Box::new(LuaRuntime::with_state_fields(
                    script,
                    &state_fields,
                    &state_types,
                    source_registry.names(),
                    &required_modules,
                ))
            }
            ReducerBody::External { id } => Box::new(
                crate::reducer_runtime::external::ExternalRuntime::new(id.clone()),
            ),
        }
    }

    /// Create a ReducerEngine that sources from another reducer's output.
    /// The source registry and group_by_ids are built dynamically per-batch
    /// since the upstream reducer's output columns are not known at construction time.
    /// Create a ReducerEngine that sources from another reducer's output.
    /// The source registry and group_by_ids are built dynamically per-batch
    /// since the upstream reducer's output columns are not known at construction time.
    pub fn new_chained(
        def: ReducerDef,
        storage: Arc<dyn StorageBackend>,
        modules: &[(String, String)],
    ) -> Self {
        let empty_registry = ColumnRegistry::new(vec![]);
        let runtime = Self::make_runtime(&def, &empty_registry, modules);

        let default_state = compute_default_state(&def);

        Self {
            def,
            runtime,
            storage,
            default_state,
            state_cache: FxHashMap::default(),
            block_snapshots: FxHashMap::default(),
            block_groups: BTreeMap::new(),
            group_by_ids: vec![],
            source_registry: Arc::new(empty_registry),
            chained: true,
        }
    }

    /// Create a ReducerEngine with a custom runtime (for FnReducerRuntime / external).
    pub fn with_runtime(
        def: ReducerDef,
        storage: Arc<dyn StorageBackend>,
        source_registry: &ColumnRegistry,
        runtime: Box<dyn ReducerRuntime>,
    ) -> Self {
        let group_by_ids: Vec<Option<ColumnId>> = def
            .group_by
            .iter()
            .map(|col| source_registry.get_id(col))
            .collect();
        let source_registry = Arc::new(source_registry.clone());
        let default_state = compute_default_state(&def);

        Self {
            def,
            runtime,
            storage,
            default_state,
            state_cache: FxHashMap::default(),
            block_snapshots: FxHashMap::default(),
            block_groups: BTreeMap::new(),
            group_by_ids,
            source_registry,
            chained: false,
        }
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn source(&self) -> &str {
        &self.def.source
    }

    /// Whether this reducer uses an external (host-language) runtime.
    pub fn is_external(&self) -> bool {
        matches!(self.def.body, ReducerBody::External { .. })
    }

    /// True when calling process would panic: the reducer uses LANGUAGE EXTERNAL,
    /// the runtime hasn't been replaced (still ExternalRuntime), and no JS
    /// context is installed on the current thread.
    pub fn needs_host_callback(&self) -> bool {
        self.is_external()
            && self.runtime.use_batched_processing() // ExternalRuntime returns true, FnReducer returns false
            && !crate::reducer_runtime::external::context_installed()
    }

    /// Replace the runtime (used to inject external/fn runtimes after construction).
    pub fn set_runtime(&mut self, runtime: Box<dyn ReducerRuntime>) {
        self.runtime = runtime;
    }

    /// Process a batch of rows for a given block.
    /// Returns enriched output rows (one per input row that produced emit output).
    ///
    /// State is updated in memory only. A snapshot of each touched group key's
    /// state is saved in `block_snapshots` at the end of the block — no storage
    /// I/O or serialization happens here.
    ///
    /// Two code paths:
    /// - **Per-row** (default): iterates rows in order, calls `process()` per row.
    ///   No extra allocations. Used by Lua and EventRules.
    /// - **Batched**: groups rows by key, calls `process_grouped()` once per block.
    ///   Used by external (host-language) reducers to minimize FFI overhead.
    pub fn process_block(&mut self, block: BlockNumber, rows: &[Row]) -> Result<Vec<RowMap>> {
        if self.runtime.use_batched_processing() {
            self.process_block_batched(block, rows)
        } else {
            self.process_block_per_row(block, rows)
        }
    }

    /// Fast per-row path: no grouping overhead, no row cloning.
    fn process_block_per_row(&mut self, block: BlockNumber, rows: &[Row]) -> Result<Vec<RowMap>> {
        let mut output_maps: Vec<RowMap> = Vec::new();
        let mut touched_keys: HashSet<Vec<u8>> = HashSet::new();

        for row in rows {
            let group_key_bytes = self.compute_group_key_bytes(row);

            // Hot path: borrowed lookup, no clone. Cold path: clone + insert.
            let state = if let Some(s) = self.state_cache.get_mut(&group_key_bytes) {
                s
            } else {
                let loaded = load_state_from(
                    self.storage.as_ref(),
                    &self.def.name,
                    &group_key_bytes,
                    &self.default_state,
                )?;
                self.state_cache
                    .entry(group_key_bytes.clone())
                    .or_insert(loaded)
            };

            // Call the runtime
            let emits = self.runtime.process(state, row)?;

            // Track touched key for deferred snapshot
            touched_keys.insert(group_key_bytes);

            for mut emit_row in emits {
                // Add group-by columns to the output row for downstream MVs
                for col in &self.def.group_by {
                    if let Some(v) = row.get(col.as_str()) {
                        emit_row.entry(col.clone()).or_insert_with(|| v.clone());
                    }
                }
                output_maps.push(emit_row);
            }
        }

        // Save in-memory snapshot for each touched group key (one clone per key per block)
        let block_keys = self.block_groups.entry(block).or_default();
        for group_key_bytes in touched_keys {
            let state = self.state_cache.get(&group_key_bytes).unwrap();
            self.block_snapshots
                .entry(group_key_bytes.clone())
                .or_default()
                .insert(block, state.clone());
            block_keys.insert(group_key_bytes);
        }

        Ok(output_maps)
    }

    /// Batched path: groups rows by key, calls process_grouped() once.
    /// Avoids per-row FFI overhead for external reducers.
    fn process_block_batched(&mut self, block: BlockNumber, rows: &[Row]) -> Result<Vec<RowMap>> {
        // Phase 1: Group rows by key, load states
        let mut key_order: Vec<Vec<u8>> = Vec::new();
        let mut group_map: HashMap<Vec<u8>, (usize, Vec<usize>)> = HashMap::new();

        for (row_idx, row) in rows.iter().enumerate() {
            let key = self.compute_group_key_bytes(row);

            // Single lookup: entry API with load on miss
            if let std::collections::hash_map::Entry::Vacant(e) =
                self.state_cache.entry(key.clone())
            {
                let loaded = load_state_from(
                    self.storage.as_ref(),
                    &self.def.name,
                    e.key(),
                    &self.default_state,
                )?;
                e.insert(loaded);
            }

            group_map
                .entry(key.clone())
                .or_insert_with(|| {
                    let idx = key_order.len();
                    key_order.push(key);
                    (idx, Vec::new())
                })
                .1
                .push(row_idx);
        }

        // Build GroupBatch array — take states out of cache for the batch call
        let mut batches: Vec<GroupBatch> = key_order
            .iter()
            .map(|key| {
                let (_, row_indices) = &group_map[key];
                let state = self.state_cache.remove(key).unwrap();
                GroupBatch {
                    state,
                    rows: row_indices.iter().map(|&i| rows[i].clone()).collect(),
                    emits: Vec::new(),
                }
            })
            .collect();

        // Phase 2: Single batch call
        self.runtime.process_grouped(&mut batches)?;

        // Phase 3: Collect results — restore states, save snapshots, enrich emits.
        // Emit in original row order (not grouped order) so first()/last() MVs
        // produce the same results as the per-row path.
        let mut indexed_emits: Vec<(usize, RowMap)> = Vec::new();
        let block_keys = self.block_groups.entry(block).or_default();

        for (i, batch) in batches.into_iter().enumerate() {
            let key = &key_order[i];
            let (_, row_indices) = &group_map[key];

            // Snapshot state before inserting into cache (avoids double lookup)
            self.block_snapshots
                .entry(key.clone())
                .or_default()
                .insert(block, batch.state.clone());
            self.state_cache.insert(key.clone(), batch.state);
            block_keys.insert(key.clone());

            let first_row = &rows[row_indices[0]];
            // Batched path assumes 1 emit per input row for correct ordering.
            // Multi-emit per row would break the row_indices mapping.
            debug_assert!(
                batch.emits.len() <= row_indices.len(),
                "batched path: got {} emits for {} rows in group — multi-emit not supported",
                batch.emits.len(),
                row_indices.len()
            );
            for (emit_idx, mut emit_row) in batch.emits.into_iter().enumerate() {
                for col in &self.def.group_by {
                    if let Some(v) = first_row.get(col.as_str()) {
                        emit_row.entry(col.clone()).or_insert_with(|| v.clone());
                    }
                }
                // Map back to original row index for ordering
                let orig_idx = if emit_idx < row_indices.len() {
                    row_indices[emit_idx]
                } else {
                    // Multi-emit: append after last row
                    usize::MAX
                };
                indexed_emits.push((orig_idx, emit_row));
            }
        }

        // Sort by original row index to preserve input order
        indexed_emits.sort_by_key(|(idx, _)| *idx);
        let output_maps: Vec<RowMap> = indexed_emits.into_iter().map(|(_, row)| row).collect();

        Ok(output_maps)
    }

    /// Process a batch of RowMaps for a given block (normal ingestion path).
    /// Converts each RowMap to Row inline using the stored source registry.
    /// For chained reducers, builds the registry dynamically from input data.
    pub fn process_block_maps(
        &mut self,
        block: BlockNumber,
        rows: &[RowMap],
    ) -> Result<Vec<RowMap>> {
        if self.chained && !rows.is_empty() {
            // Build registry from actual column names in this batch
            let mut col_set = std::collections::HashSet::new();
            for m in rows {
                for k in m.keys() {
                    col_set.insert(k.clone());
                }
            }
            let columns: Vec<String> = col_set.into_iter().collect();
            let registry = Arc::new(ColumnRegistry::new(columns));
            // Re-compute group_by_ids against the actual columns
            self.group_by_ids = self
                .def
                .group_by
                .iter()
                .map(|col| registry.get_id(col))
                .collect();
            self.source_registry = registry;
        }
        let typed_rows: Vec<Row> = rows
            .iter()
            .map(|m| Row::from_map(self.source_registry.clone(), m))
            .collect();
        self.process_block(block, &typed_rows)
    }

    /// Roll back all blocks after fork_point.
    /// Restores state from in-memory block snapshots.
    /// Returns the number of groups affected.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Result<usize> {
        // Guard: fork_point + 1 would overflow u64::MAX to 0, causing split_off(&0)
        // to remove the entire map. MAX is a valid no-op: nothing exists after it.
        if fork_point == BlockNumber::MAX {
            return Ok(0);
        }
        // Use BTreeMap split_off for O(log N) range extraction
        let rolled_back = self.block_groups.split_off(&(fork_point + 1));

        if rolled_back.is_empty() {
            return Ok(0);
        }

        // Collect all affected group keys (consume by value to avoid cloning)
        let mut affected_keys: HashSet<Vec<u8>> = HashSet::new();
        for (_block, keys) in rolled_back {
            for key in keys {
                affected_keys.insert(key);
            }
        }

        // Restore state for each affected group key
        for group_key_bytes in &affected_keys {
            // Remove snapshots after fork_point from the in-memory map.
            // split_off(fork_point+1) leaves entries <= fork_point in the original
            // and returns entries > fork_point (which we discard).
            if let Some(snapshots) = self.block_snapshots.get_mut(group_key_bytes) {
                let _discarded = snapshots.split_off(&(fork_point + 1));
            }

            // Find the state at fork_point (or the most recent before it)
            let state = self.find_state_at_or_before(group_key_bytes, fork_point)?;
            self.state_cache.insert(group_key_bytes.clone(), state);

            // Clean up empty snapshot maps
            if let Some(snapshots) = self.block_snapshots.get(group_key_bytes) {
                if snapshots.is_empty() {
                    self.block_snapshots.remove(group_key_bytes);
                }
            }
        }

        Ok(affected_keys.len())
    }

    /// Finalize state up to the given block.
    /// Collects finalized state writes into the provided batch for atomic commit.
    /// Drops in-memory snapshots for finalized blocks.
    pub fn finalize(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch) {
        // Split off blocks > block, keeping blocks <= block for finalization
        let remaining = self.block_groups.split_off(&(block + 1));
        let finalized_block_groups = std::mem::replace(&mut self.block_groups, remaining);

        let mut finalized_keys: HashSet<Vec<u8>> = HashSet::new();
        for keys in finalized_block_groups.values() {
            finalized_keys.extend(keys.iter().cloned());
        }

        // For each group key, add finalized state to batch and drop old snapshots
        for group_key_bytes in &finalized_keys {
            // Find the most recent state at or before the finalization block
            if let Some(state) = self.find_snapshot_at_or_before(group_key_bytes, block) {
                let state_bytes = storage::encode_state(&state);
                batch.set_reducer_finalized(&self.def.name, group_key_bytes, &state_bytes);
            }

            // Remove in-memory snapshots for blocks <= finalization point
            if let Some(snapshots) = self.block_snapshots.get_mut(group_key_bytes) {
                let remaining = snapshots.split_off(&(block + 1));
                *snapshots = remaining;
                if snapshots.is_empty() {
                    self.block_snapshots.remove(group_key_bytes);
                }
            }
        }
    }

    /// Find state at or before the given block from in-memory snapshots,
    /// falling back to storage (finalized state) or defaults.
    fn find_state_at_or_before(&self, group_key_bytes: &[u8], block: BlockNumber) -> Result<State> {
        if let Some(state) = self.find_snapshot_at_or_before(group_key_bytes, block) {
            return Ok(state);
        }
        // Fall back to finalized state in storage
        load_state_from(
            self.storage.as_ref(),
            &self.def.name,
            group_key_bytes,
            &self.default_state,
        )
    }

    /// Look up the most recent in-memory snapshot at or before the given block.
    fn find_snapshot_at_or_before(
        &self,
        group_key_bytes: &[u8],
        block: BlockNumber,
    ) -> Option<State> {
        self.block_snapshots
            .get(group_key_bytes)
            .and_then(|snapshots| {
                snapshots
                    .range(..=block)
                    .next_back()
                    .map(|(_, state)| state.clone())
            })
    }

    fn compute_group_key_bytes(&self, row: &Row) -> Vec<u8> {
        if self.group_by_ids.is_empty() {
            return Vec::new();
        }
        let values = row.values();
        // Fast path: single string group key — use raw bytes instead of MessagePack
        if self.group_by_ids.len() == 1 {
            if let Some(id) = self.group_by_ids[0] {
                if let Value::String(s) = &values[id as usize] {
                    return s.as_bytes().to_vec();
                }
            }
        }
        // General path: direct Vec indexing with pre-computed column IDs
        let key: GroupKey = self
            .group_by_ids
            .iter()
            .map(|id| {
                id.map(|i| {
                    let v = &values[i as usize];
                    if v.is_null() { Value::Null } else { v.clone() }
                })
                .unwrap_or(Value::Null)
            })
            .collect();
        storage::encode_group_key(&key)
    }
}

fn compute_default_state(def: &ReducerDef) -> State {
    let mut state = HashMap::new();
    for field in &def.state {
        let default_val = parse_default(&field.default, &field.column_type);
        state.insert(field.name.clone(), default_val);
    }
    state
}

/// Load reducer state from storage, avoiding borrowing the entire ReducerEngine.
fn load_state_from(
    storage: &dyn StorageBackend,
    reducer_name: &str,
    group_key_bytes: &[u8],
    default_state: &State,
) -> Result<State> {
    if let Some(bytes) = storage.get_reducer_finalized(reducer_name, group_key_bytes)? {
        return Ok(storage::decode_state(&bytes));
    }
    Ok(default_state.clone())
}

fn parse_default(default_str: &str, column_type: &crate::types::ColumnType) -> Value {
    use crate::types::ColumnType;
    match column_type {
        ColumnType::Float64 => Value::Float64(default_str.parse::<f64>().unwrap_or(0.0)),
        ColumnType::UInt64 => Value::UInt64(default_str.parse::<u64>().unwrap_or(0)),
        ColumnType::Int64 => Value::Int64(default_str.parse::<i64>().unwrap_or(0)),
        ColumnType::String => {
            // Strip surrounding quotes
            let s = default_str.trim_matches('\'').trim_matches('"');
            Value::String(s.to_string())
        }
        ColumnType::Boolean => Value::Boolean(default_str == "true" || default_str == "1"),
        ColumnType::JSON => {
            let s = default_str.trim_matches('\'').trim_matches('"');
            let json_val = serde_json::from_str(s).unwrap_or(serde_json::Value::Null);
            Value::JSON(json_val)
        }
        ColumnType::DateTime => Value::DateTime(default_str.parse::<i64>().unwrap_or(0)),
        ColumnType::Uint256 | ColumnType::Bytes | ColumnType::Base58 => column_type.default_value(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::*;
    use crate::storage::memory::MemoryBackend;
    use crate::types::ColumnRegistry;
    use crate::types::ColumnType;

    fn pnl_reducer_def() -> ReducerDef {
        ReducerDef {
            name: "pnl_tracker".to_string(),
            source: "trades".to_string(),
            group_by: vec!["user".to_string()],
            state: vec![
                StateField {
                    name: "quantity".to_string(),
                    column_type: crate::types::ColumnType::Float64,
                    default: "0".to_string(),
                },
                StateField {
                    name: "cost_basis".to_string(),
                    column_type: crate::types::ColumnType::Float64,
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
        }
    }

    fn trade_registry() -> Arc<ColumnRegistry> {
        Arc::new(ColumnRegistry::new(vec![
            "amount".to_string(),
            "price".to_string(),
            "side".to_string(),
            "user".to_string(),
        ]))
    }

    fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> Row {
        Row::from_map(
            trade_registry(),
            &HashMap::from([
                ("user".to_string(), Value::String(user.to_string())),
                ("side".to_string(), Value::String(side.to_string())),
                ("amount".to_string(), Value::Float64(amount)),
                ("price".to_string(), Value::Float64(price)),
            ]),
        )
    }

    #[test]
    fn reducer_processes_rows_and_emits_output() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(pnl_reducer_def(), storage, &trade_registry(), &[]);

        let rows = vec![
            make_trade("alice", "buy", 10.0, 2000.0),
            make_trade("alice", "buy", 5.0, 2100.0),
        ];

        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(output.len(), 2);

        // Both emits should have trade_pnl = 0 (buys)
        assert_eq!(output[0].get("trade_pnl"), Some(&Value::UInt64(0)));
        assert_eq!(output[0].get("position_size"), Some(&Value::Float64(10.0)));
        // user group-by column should be forwarded
        assert_eq!(output[0].get("user"), Some(&Value::String("alice".into())));

        assert_eq!(output[1].get("position_size"), Some(&Value::Float64(15.0)));
    }

    #[test]
    fn reducer_state_persists_across_blocks() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(pnl_reducer_def(), storage, &trade_registry(), &[]);

        // Block 1: buy
        engine
            .process_block(1000, &[make_trade("alice", "buy", 10.0, 2000.0)])
            .unwrap();

        // Block 2: sell
        let output = engine
            .process_block(1001, &[make_trade("alice", "sell", 5.0, 2200.0)])
            .unwrap();
        assert_eq!(output.len(), 1);
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        // 5 * (2200 - 2000) = 1000
        assert!((pnl - 1000.0).abs() < 0.01);
        assert_eq!(output[0].get("position_size"), Some(&Value::Float64(5.0)));
    }

    #[test]
    fn reducer_rollback_restores_state() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(pnl_reducer_def(), storage, &trade_registry(), &[]);

        // Block 1: buy 10 @ 2000
        engine
            .process_block(1000, &[make_trade("alice", "buy", 10.0, 2000.0)])
            .unwrap();

        // Block 2: buy 5 @ 2100 (will be rolled back)
        engine
            .process_block(1001, &[make_trade("alice", "buy", 5.0, 2100.0)])
            .unwrap();

        // Rollback block 2
        let affected = engine.rollback(1000).unwrap();
        assert_eq!(affected, 1);

        // Process block 2 again with different data
        let output = engine
            .process_block(1001, &[make_trade("alice", "sell", 3.0, 2200.0)])
            .unwrap();
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        // After rollback, state is: qty=10, cost=20000, avg=2000
        // sell 3 @ 2200: pnl = 3 * (2200 - 2000) = 600
        assert!((pnl - 600.0).abs() < 0.01);
        assert_eq!(output[0].get("position_size"), Some(&Value::Float64(7.0)));
    }

    #[test]
    fn reducer_multiple_groups() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::new(pnl_reducer_def(), storage, &trade_registry(), &[]);

        let rows = vec![
            make_trade("alice", "buy", 10.0, 2000.0),
            make_trade("bob", "buy", 5.0, 3000.0),
        ];

        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(output.len(), 2);

        // Alice: position 10
        let alice_out = output
            .iter()
            .find(|r| r.get("user") == Some(&Value::String("alice".into())))
            .unwrap();
        assert_eq!(alice_out.get("position_size"), Some(&Value::Float64(10.0)));

        // Bob: position 5
        let bob_out = output
            .iter()
            .find(|r| r.get("user") == Some(&Value::String("bob".into())))
            .unwrap();
        assert_eq!(bob_out.get("position_size"), Some(&Value::Float64(5.0)));
    }

    #[test]
    fn reducer_finalize_then_rollback() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine =
            ReducerEngine::new(pnl_reducer_def(), storage.clone(), &trade_registry(), &[]);

        // Block 1000: buy 10
        engine
            .process_block(1000, &[make_trade("alice", "buy", 10.0, 2000.0)])
            .unwrap();
        // Block 1001: buy 5
        engine
            .process_block(1001, &[make_trade("alice", "buy", 5.0, 2100.0)])
            .unwrap();

        // Finalize up to 1000
        let mut batch = StorageWriteBatch::new();
        engine.finalize(1000, &mut batch);
        storage.commit(&batch).unwrap();

        // Block 1002: buy 3 (will be rolled back)
        engine
            .process_block(1002, &[make_trade("alice", "buy", 3.0, 2200.0)])
            .unwrap();

        // Rollback to 1001
        engine.rollback(1001).unwrap();

        // After rollback: state should be at block 1001 (qty=15, cost=30500)
        let output = engine
            .process_block(1002, &[make_trade("alice", "sell", 15.0, 2100.0)])
            .unwrap();
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        // avg cost = 30500/15 = 2033.33, sell 15 @ 2100: pnl = 15 * (2100 - 2033.33) = 1000
        assert!((pnl - 1000.0).abs() < 0.01);
    }

    #[test]
    fn reducer_lua_runtime() {
        let def = ReducerDef {
            name: "counter".to_string(),
            source: "events".to_string(),
            group_by: vec![],
            state: vec![StateField {
                name: "count".to_string(),
                column_type: crate::types::ColumnType::Float64,
                default: "0".to_string(),
            }],
            requires: vec![],
            body: ReducerBody::Lua {
                script: r#"
                    state.count = state.count + row.value
                    emit.total = state.count
                "#
                .to_string(),
            },
        };

        let storage = Arc::new(MemoryBackend::new());
        let events_registry = ColumnRegistry::new(vec!["value".to_string()]);
        let mut engine = ReducerEngine::new(def, storage, &events_registry, &[]);

        let reg = Arc::new(events_registry);
        let rows: Vec<Row> = vec![
            Row::from_map(
                reg.clone(),
                &HashMap::from([("value".to_string(), Value::Float64(10.0))]),
            ),
            Row::from_map(
                reg.clone(),
                &HashMap::from([("value".to_string(), Value::Float64(20.0))]),
            ),
        ];
        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0].get("total"), Some(&Value::Float64(10.0)));
        assert_eq!(output[1].get("total"), Some(&Value::Float64(30.0)));
    }

    // ─── FnReducerRuntime tests ─────────────────────────────────

    fn pnl_fn_runtime() -> crate::reducer_runtime::fn_reducer::FnReducerRuntime {
        crate::reducer_runtime::fn_reducer::FnReducerRuntime::new(|state, row| {
            let side = row.get("side").and_then(|v| v.as_str()).unwrap_or("");
            let amount = row.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let price = row.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let qty = state
                .get("quantity")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let cost = state
                .get("cost_basis")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            let mut emit = HashMap::new();
            if side == "buy" {
                state.insert("quantity".into(), Value::Float64(qty + amount));
                state.insert("cost_basis".into(), Value::Float64(cost + amount * price));
                emit.insert("trade_pnl".into(), Value::Float64(0.0));
            } else {
                let avg_cost = if qty > 0.0 { cost / qty } else { 0.0 };
                emit.insert(
                    "trade_pnl".into(),
                    Value::Float64(amount * (price - avg_cost)),
                );
                state.insert("quantity".into(), Value::Float64(qty - amount));
                state.insert(
                    "cost_basis".into(),
                    Value::Float64(cost - amount * avg_cost),
                );
            }
            let new_qty = state
                .get("quantity")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            emit.insert("position_size".into(), Value::Float64(new_qty));
            vec![emit]
        })
    }

    fn pnl_external_def() -> ReducerDef {
        ReducerDef {
            name: "pnl".to_string(),
            source: "trades".to_string(),
            group_by: vec!["user".to_string()],
            state: vec![
                StateField {
                    name: "quantity".to_string(),
                    column_type: crate::types::ColumnType::Float64,
                    default: "0".to_string(),
                },
                StateField {
                    name: "cost_basis".to_string(),
                    column_type: crate::types::ColumnType::Float64,
                    default: "0".to_string(),
                },
            ],
            requires: vec![],
            body: ReducerBody::External {
                id: "pnl".to_string(),
            },
        }
    }

    #[test]
    fn fn_reducer_produces_same_output_as_event_rules() {
        let storage = Arc::new(MemoryBackend::new());
        let mut fn_engine = ReducerEngine::with_runtime(
            pnl_external_def(),
            storage.clone(),
            &trade_registry(),
            Box::new(pnl_fn_runtime()),
        );

        let storage2 = Arc::new(MemoryBackend::new());
        let mut er_engine = ReducerEngine::new(pnl_reducer_def(), storage2, &trade_registry(), &[]);

        let rows = vec![
            make_trade("alice", "buy", 10.0, 2000.0),
            make_trade("alice", "buy", 5.0, 2100.0),
        ];

        let fn_out = fn_engine.process_block(1000, &rows).unwrap();
        let er_out = er_engine.process_block(1000, &rows).unwrap();

        assert_eq!(fn_out.len(), er_out.len());
        for (f, e) in fn_out.iter().zip(er_out.iter()) {
            let f_pos = f.get("position_size").unwrap().as_f64().unwrap();
            let e_pos = e.get("position_size").unwrap().as_f64().unwrap();
            assert!(
                (f_pos - e_pos).abs() < 0.001,
                "position_size mismatch: {} vs {}",
                f_pos,
                e_pos
            );
        }
    }

    #[test]
    fn fn_reducer_state_persists_across_blocks() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::with_runtime(
            pnl_external_def(),
            storage,
            &trade_registry(),
            Box::new(pnl_fn_runtime()),
        );

        engine
            .process_block(1000, &[make_trade("alice", "buy", 10.0, 2000.0)])
            .unwrap();
        let output = engine
            .process_block(1001, &[make_trade("alice", "sell", 5.0, 2200.0)])
            .unwrap();

        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        assert!((pnl - 1000.0).abs() < 0.01); // 5 * (2200 - 2000)
        assert_eq!(output[0].get("position_size"), Some(&Value::Float64(5.0)));
    }

    #[test]
    fn fn_reducer_rollback_restores_state() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::with_runtime(
            pnl_external_def(),
            storage,
            &trade_registry(),
            Box::new(pnl_fn_runtime()),
        );

        engine
            .process_block(1000, &[make_trade("alice", "buy", 10.0, 2000.0)])
            .unwrap();
        engine
            .process_block(1001, &[make_trade("alice", "buy", 5.0, 2100.0)])
            .unwrap();

        engine.rollback(1000).unwrap();

        let output = engine
            .process_block(1001, &[make_trade("alice", "sell", 3.0, 2200.0)])
            .unwrap();
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        // After rollback: qty=10, cost=20000, avg=2000. sell 3@2200: pnl = 3*(2200-2000) = 600
        assert!((pnl - 600.0).abs() < 0.01);
        assert_eq!(output[0].get("position_size"), Some(&Value::Float64(7.0)));
    }

    #[test]
    fn fn_reducer_multiple_groups() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = ReducerEngine::with_runtime(
            pnl_external_def(),
            storage,
            &trade_registry(),
            Box::new(pnl_fn_runtime()),
        );

        let rows = vec![
            make_trade("alice", "buy", 10.0, 2000.0),
            make_trade("bob", "buy", 5.0, 3000.0),
            make_trade("alice", "buy", 3.0, 2100.0),
        ];

        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(output.len(), 3);

        let alice_positions: Vec<f64> = output
            .iter()
            .filter(|r| r.get("user") == Some(&Value::String("alice".into())))
            .map(|r| r.get("position_size").unwrap().as_f64().unwrap())
            .collect();
        assert_eq!(alice_positions, vec![10.0, 13.0]);

        let bob_out: Vec<_> = output
            .iter()
            .filter(|r| r.get("user") == Some(&Value::String("bob".into())))
            .collect();
        assert_eq!(bob_out[0].get("position_size"), Some(&Value::Float64(5.0)));
    }

    /// needs_host_callback() must be true for ExternalRuntime without JS context,
    /// and false after set_runtime replaces it with FnReducer.
    #[test]
    fn needs_host_callback_tracks_runtime_and_context() {
        let def = ReducerDef {
            name: "ext".to_string(),
            source: "t".to_string(),
            group_by: vec![],
            state: vec![],
            requires: vec![],
            body: ReducerBody::External {
                id: "ext".to_string(),
            },
        };
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let reg = ColumnRegistry::new(vec![]);
        let mut engine = ReducerEngine::new(def, storage, &reg, &[]);

        // ExternalRuntime + no JS context → needs callback (would panic)
        assert!(engine.needs_host_callback());
        assert!(engine.is_external());

        // Replace with FnReducer → no longer needs callback
        engine.set_runtime(Box::new(
            crate::reducer_runtime::fn_reducer::FnReducerRuntime::new(|_state, _row| vec![]),
        ));
        assert!(!engine.needs_host_callback());
        assert!(engine.is_external()); // def.body unchanged
    }

    /// parse_default must handle DateTime explicitly (not fall through to wildcard).
    #[test]
    fn parse_default_datetime() {
        let val = parse_default("1700000000", &ColumnType::DateTime);
        assert_eq!(val, Value::DateTime(1700000000));
    }

    /// parse_default for Uint256 returns zero (no hex parsing, just type default).
    #[test]
    fn parse_default_uint256() {
        let val = parse_default("0", &ColumnType::Uint256);
        assert_eq!(val, Value::Uint256([0u8; 32]));
    }

    /// Batched path must preserve original input row order in emits.
    #[test]
    fn batched_path_preserves_emit_order() {
        use crate::reducer_runtime::external::{ExternalRuntime, install_test_context};

        let def = ReducerDef {
            name: "counter".to_string(),
            source: "events".to_string(),
            group_by: vec!["user".to_string()],
            state: vec![StateField {
                name: "count".to_string(),
                column_type: ColumnType::Float64,
                default: "0".to_string(),
            }],
            requires: vec![],
            body: ReducerBody::External {
                id: "counter".to_string(),
            },
        };
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let reg = ColumnRegistry::new(vec!["user".to_string(), "amount".to_string()]);
        let mut engine = ReducerEngine::new(def, storage, &reg, &[]);

        // Install test context that increments count and emits it
        let _guard = install_test_context(|groups| {
            for group in groups.iter_mut() {
                for row in &group.rows {
                    let amount = row.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let count = group
                        .state
                        .get("count")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    let new_count = count + amount;
                    group
                        .state
                        .insert("count".into(), Value::Float64(new_count));
                    group
                        .emits
                        .push(HashMap::from([("count".into(), Value::Float64(new_count))]));
                }
            }
        });

        // Input: alice, bob, alice — interleaved
        let rows = vec![
            Row::from(HashMap::from([
                ("user".into(), Value::String("alice".into())),
                ("amount".into(), Value::Float64(1.0)),
            ])),
            Row::from(HashMap::from([
                ("user".into(), Value::String("bob".into())),
                ("amount".into(), Value::Float64(10.0)),
            ])),
            Row::from(HashMap::from([
                ("user".into(), Value::String("alice".into())),
                ("amount".into(), Value::Float64(2.0)),
            ])),
        ];

        let output = engine.process_block(1000, &rows).unwrap();

        // The key assertion: output follows original input order (alice, bob, alice)
        // not grouped order (alice, alice, bob)
        assert_eq!(output.len(), 3);
        assert_eq!(output[0].get("user"), Some(&Value::String("alice".into())));
        assert_eq!(output[1].get("user"), Some(&Value::String("bob".into())));
        assert_eq!(output[2].get("user"), Some(&Value::String("alice".into())));
    }
}
