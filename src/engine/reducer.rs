use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::error::{Error, Result};
use crate::reducer_runtime::event_rules::EventRulesRuntime;
#[cfg(feature = "lua")]
use crate::reducer_runtime::lua::LuaRuntime;
use crate::reducer_runtime::{Emits, GroupBatch, GroupState, ReducerRuntime};
use crate::schema::ast::{ReducerBody, ReducerDef};
use crate::storage::{self, PendingWrites, StorageBackend, StorageWriteBatch};
use crate::types::{BlockNumber, ColumnId, ColumnRegistry, GroupKey, Row, Value};

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
    /// The STATE fields in schema order: the layout of a group's state in
    /// memory, in snapshots and in storage.
    state_fields: ColumnRegistry,
    /// Cached default state (computed once from def.state).
    default_state: Vec<Value>,
    /// Current hot state per group key. Never evicted: a miss loads the
    /// stored state, which lacks the checkpoints of unacked batches.
    state_cache: FxHashMap<Vec<u8>, Vec<Value>>,
    /// In-memory state snapshots: group_key -> (block -> state).
    /// Holds unfinalized blocks for rollback, plus, for each key in
    /// `pending_durable`, its newest finalized snapshot.
    block_snapshots: FxHashMap<Vec<u8>, BTreeMap<BlockNumber, Vec<Value>>>,
    /// Tracks which blocks have been processed and which group keys were touched.
    /// BTreeMap for O(log N) range queries during rollback/finalize. FxHash on
    /// the per-block key sets: keys are trusted internal `Vec<u8>` group keys
    /// inserted once per touched group per block (hot path) — matches the
    /// FxHash already used for `state_cache` / `block_snapshots`.
    block_groups: BTreeMap<BlockNumber, FxHashSet<Vec<u8>>>,
    /// Group keys finalized since the last checkpoint. Each keeps its newest
    /// finalized snapshot, which `write_checkpoint` persists. Under backfill
    /// deferral keys wait here for several finalizations.
    pending_durable: FxHashSet<Vec<u8>>,
    /// Positions of the group-by columns in `source_registry`.
    group_by_ids: Vec<Option<ColumnId>>,
    /// The registry of the source rows that `group_by_ids` refers to. The
    /// rows of a chained reducer have the emit registry of the reducer
    /// before it, which grows with new columns and shrinks on rollback.
    source_registry: Arc<ColumnRegistry>,
    /// The output of the current block. Its first columns are the group-by
    /// columns, which a row takes from its input row if its emit left them
    /// Null.
    emits: Emits,
    /// The emit width before each block that added emit columns, so that a
    /// rollback drops them. At most `MAX_EMIT_COLUMNS` entries: each one
    /// added a column.
    emit_growth: BTreeMap<BlockNumber, usize>,
}

impl ReducerEngine {
    pub fn new(
        def: ReducerDef,
        storage: Arc<dyn StorageBackend>,
        source_registry: &ColumnRegistry,
        modules: &[(String, String)],
    ) -> Self {
        let runtime = Self::make_runtime(&def, source_registry, modules);
        Self::build(def, storage, source_registry, runtime)
    }

    /// Create a ReducerEngine that sources from another reducer's output,
    /// whose columns are known only from its rows.
    pub fn new_chained(
        def: ReducerDef,
        storage: Arc<dyn StorageBackend>,
        modules: &[(String, String)],
    ) -> Self {
        let empty_registry = ColumnRegistry::default();
        let runtime = Self::make_runtime(&def, &empty_registry, modules);
        Self::build(def, storage, &empty_registry, runtime)
    }

    /// Create a ReducerEngine with a custom runtime (for FnReducerRuntime / external).
    pub fn with_runtime(
        def: ReducerDef,
        storage: Arc<dyn StorageBackend>,
        source_registry: &ColumnRegistry,
        runtime: Box<dyn ReducerRuntime>,
    ) -> Self {
        Self::build(def, storage, source_registry, runtime)
    }

    fn build(
        def: ReducerDef,
        storage: Arc<dyn StorageBackend>,
        source_registry: &ColumnRegistry,
        runtime: Box<dyn ReducerRuntime>,
    ) -> Self {
        let state_fields = ColumnRegistry::new(def.state.iter().map(|f| f.name.clone()).collect());
        let default_state = def
            .state
            .iter()
            .map(|f| parse_default(&f.default, &f.column_type))
            .collect();
        let group_by_ids = def
            .group_by
            .iter()
            .map(|col| source_registry.get_id(col))
            .collect();
        let emits = Emits::with_columns(&def.group_by);

        Self {
            def,
            runtime,
            storage,
            state_fields,
            default_state,
            state_cache: FxHashMap::default(),
            block_snapshots: FxHashMap::default(),
            block_groups: BTreeMap::new(),
            pending_durable: FxHashSet::default(),
            group_by_ids,
            source_registry: Arc::new(source_registry.clone()),
            emits,
            emit_growth: BTreeMap::new(),
        }
    }

    /// Build a runtime for the given reducer definition.
    #[allow(unused_variables)]
    fn make_runtime(
        def: &ReducerDef,
        source_registry: &ColumnRegistry,
        modules: &[(String, String)],
    ) -> Box<dyn ReducerRuntime> {
        match &def.body {
            ReducerBody::EventRules { .. } => Box::new(EventRulesRuntime::new(&def.body)),
            #[cfg(feature = "lua")]
            ReducerBody::Lua { script } => {
                let requiredmodules: Vec<(String, String)> = def
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
                    &requiredmodules,
                ))
            }
            #[cfg(not(feature = "lua"))]
            ReducerBody::Lua { .. } => {
                // The schema parser already rejects Lua bodies when the lua feature is
                // disabled, so this branch is unreachable in normal usage.
                unreachable!("Lua reducer body reached without lua feature — schema parser should have rejected it");
            }
            ReducerBody::External { id } => Box::new(
                crate::reducer_runtime::external::ExternalRuntime::new(id.clone()),
            ),
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

    /// True when processing would fail: the runtime is still the
    /// ExternalRuntime of a LANGUAGE EXTERNAL reducer, and the callback
    /// context on the current thread holds no callback for it.
    pub fn needs_host_callback(&self) -> bool {
        self.runtime.missing_host_callback()
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
    pub fn process_block(&mut self, block: BlockNumber, rows: &[Row]) -> Result<Vec<Row>> {
        if let Some(first) = rows.first() {
            self.resolve_source(first.registry());
        }

        let width = self.emits.columns().len();
        let processed = if self.runtime.use_batched_processing() {
            self.process_block_batched(block, rows)
        } else {
            self.process_block_per_row(block, rows)
        };
        if let Err(e) = processed {
            self.emits.clear();
            self.emits.truncate_columns(width);
            return Err(e);
        }

        if self.emits.columns().len() > width {
            self.emit_growth.entry(block).or_insert(width);
        }
        Ok(self.take_output(rows))
    }

    /// Point `group_by_ids` at the columns of `registry`.
    fn resolve_source(&mut self, registry: &Arc<ColumnRegistry>) {
        if Arc::ptr_eq(&self.source_registry, registry) {
            return;
        }
        self.group_by_ids = self
            .def
            .group_by
            .iter()
            .map(|col| registry.get_id(col))
            .collect();
        self.source_registry = registry.clone();
    }

    /// The emits of the block in input order, as rows.
    fn take_output(&mut self, rows: &[Row]) -> Vec<Row> {
        let mut emitted = self.emits.take();
        if emitted.is_empty() {
            return Vec::new();
        }
        // The batched path emits group by group. Stable: the emits of one
        // row keep their order.
        emitted.sort_by_key(|(source, _)| *source);

        let columns = self.emits.columns().clone();
        emitted
            .into_iter()
            .map(|(source, mut values)| {
                let input = rows[source].values();
                for (value, id) in values.iter_mut().zip(&self.group_by_ids) {
                    if let (true, Some(id)) = (value.is_null(), id) {
                        *value = input[*id as usize].clone();
                    }
                }
                Row::from_values(columns.clone(), values)
            })
            .collect()
    }

    /// Fast per-row path: no grouping overhead, no row cloning.
    fn process_block_per_row(&mut self, block: BlockNumber, rows: &[Row]) -> Result<()> {
        let mut touched_keys: FxHashSet<Vec<u8>> = FxHashSet::default();

        let processed = self.process_rows(rows, &mut touched_keys);

        // Registered on failure too: the undo of the failed ingest then
        // restores the keys that the rows before the error changed.
        let block_keys = self.block_groups.entry(block).or_default();
        if let Err(e) = processed {
            block_keys.extend(touched_keys);
            return Err(e);
        }

        // One snapshot of the state of each touched group per block.
        for group_key_bytes in touched_keys {
            let snapshot = self.state_cache[&group_key_bytes].clone();
            self.block_snapshots
                .entry(group_key_bytes.clone())
                .or_default()
                .insert(block, snapshot);
            block_keys.insert(group_key_bytes);
        }

        Ok(())
    }

    /// The row loop of `process_block_per_row`. A key goes into
    /// `touched_keys` before its row can fail.
    fn process_rows(&mut self, rows: &[Row], touched_keys: &mut FxHashSet<Vec<u8>>) -> Result<()> {
        for (index, row) in rows.iter().enumerate() {
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

            self.emits.set_source(index);
            let state = GroupState {
                values: state,
                fields: &self.state_fields,
            };
            let processed = self
                .runtime
                .process(state, row, &mut self.emits)
                .and_then(|()| self.emits.check());
            touched_keys.insert(group_key_bytes);
            processed?;
        }
        Ok(())
    }

    /// Batched path: groups rows by key, calls process_grouped() once.
    /// Avoids per-row FFI overhead for external reducers.
    fn process_block_batched(&mut self, block: BlockNumber, rows: &[Row]) -> Result<()> {
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
        let processed = self
            .runtime
            .process_grouped(&mut batches, &self.state_fields)
            .and_then(|()| self.check_emit_rows(&batches));
        if let Err(e) = processed {
            // The undo of the failed ingest restores these keys from the
            // blocks before, also the states taken out of `state_cache`.
            self.block_groups.entry(block).or_default().extend(key_order);
            return Err(e);
        }

        // Phase 3: restore states, save snapshots, collect emits by input row.
        let block_keys = self.block_groups.entry(block).or_default();
        for (i, batch) in batches.into_iter().enumerate() {
            let key = &key_order[i];
            let (_, row_indices) = &group_map[key];

            self.block_snapshots
                .entry(key.clone())
                .or_default()
                .insert(block, batch.state.clone());
            self.state_cache.insert(key.clone(), batch.state);
            block_keys.insert(key.clone());

            for (group_row, emit) in batch.emits {
                self.emits.set_source(row_indices[group_row]);
                self.emits.push_named(emit);
            }
        }

        // The keys are in `block_groups`: the undo of the failed ingest
        // restores their states.
        self.emits.check()
    }

    /// Err if an emit names a row that its group does not have.
    fn check_emit_rows(&self, batches: &[GroupBatch]) -> Result<()> {
        for batch in batches {
            if let Some((row, _)) = batch.emits.iter().find(|(row, _)| *row >= batch.rows.len()) {
                return Err(Error::InvalidOperation(format!(
                    "reducer '{}': an emit names row {row} of a group with {} rows",
                    self.def.name,
                    batch.rows.len()
                )));
            }
        }
        Ok(())
    }

    /// Roll back all blocks after fork_point.
    /// Restores state from in-memory block snapshots, else from the newest
    /// finalized state in `pending` or storage.
    /// Returns the number of groups affected.
    pub fn rollback(&mut self, fork_point: BlockNumber, pending: &PendingWrites) -> Result<usize> {
        // Guard: fork_point + 1 would overflow u64::MAX to 0, causing split_off(&0)
        // to remove the entire map. MAX is a valid no-op: nothing exists after it.
        if fork_point == BlockNumber::MAX {
            return Ok(0);
        }

        let grown = self.emit_growth.split_off(&(fork_point + 1));
        if let Some(&width) = grown.values().next() {
            self.emits.truncate_columns(width);
        }

        // Use BTreeMap split_off for O(log N) range extraction
        let rolled_back = self.block_groups.split_off(&(fork_point + 1));

        if rolled_back.is_empty() {
            return Ok(0);
        }

        // Collect all affected group keys (consume by value to avoid cloning)
        let mut affected_keys: FxHashSet<Vec<u8>> = FxHashSet::default();
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
            let state = self.find_state_at_or_before(group_key_bytes, fork_point, pending)?;
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

    /// Drop all in-memory state: each group loads its stored state again on
    /// its next row.
    pub fn reload(&mut self) {
        self.state_cache.clear();
        self.block_snapshots.clear();
        self.block_groups.clear();
        self.pending_durable.clear();
        self.emits.truncate_columns(self.def.group_by.len());
        self.emit_growth.clear();
    }

    /// Merge the blocks up to `block` into finalized state: they can no longer
    /// roll back. Their groups wait in `pending_durable` for `write_checkpoint`,
    /// which reads each group's newest snapshot at or before finality, so the
    /// older snapshots go now.
    pub fn advance_finality(&mut self, block: BlockNumber) {
        let remaining = self.block_groups.split_off(&(block + 1));
        let finalized = std::mem::replace(&mut self.block_groups, remaining);

        for key in finalized.into_values().flatten() {
            if let Some(snapshots) = self.block_snapshots.get_mut(&key) {
                let newest = snapshots.range(..=block).next_back().map(|(b, _)| *b);
                if let Some(newest) = newest {
                    while let Some(entry) = snapshots.first_entry() {
                        if *entry.key() >= newest {
                            break;
                        }
                        entry.remove();
                    }
                }
            }
            self.pending_durable.insert(key);
        }
    }

    /// Write the state as of `finalized` of every group in `pending_durable`.
    /// Storage then holds that state, so the snapshots up to `finalized` go.
    pub fn write_checkpoint(&mut self, finalized: BlockNumber, batch: &mut StorageWriteBatch) {
        for key in std::mem::take(&mut self.pending_durable) {
            let state_bytes = self
                .find_snapshot_at_or_before(&key, finalized)
                .map(storage::encode_values);

            if let Some(snapshots) = self.block_snapshots.get_mut(&key) {
                // Not `split_off`: it allocates a new root even when no block is
                // above `finalized`.
                while let Some(entry) = snapshots.first_entry() {
                    if *entry.key() > finalized {
                        break;
                    }
                    entry.remove();
                }
                if snapshots.is_empty() {
                    self.block_snapshots.remove(&key);
                }
            }

            if let Some(state_bytes) = state_bytes {
                batch.set_reducer_finalized(&self.def.name, key, state_bytes);
            }
        }
    }

    /// Find state at or before the given block from in-memory snapshots,
    /// falling back to finalized state (pending, then stored) or defaults.
    fn find_state_at_or_before(
        &self,
        group_key_bytes: &[u8],
        block: BlockNumber,
        pending: &PendingWrites,
    ) -> Result<Vec<Value>> {
        if let Some(values) = self.find_snapshot_at_or_before(group_key_bytes, block) {
            return Ok(values.to_vec());
        }

        // `write_checkpoint` drops the snapshots it persists, so a checkpoint
        // that waits for an ack is the newest state of this group.
        if let Some(bytes) = pending.reducer_finalized(&self.def.name, group_key_bytes) {
            return decode_state(bytes, &self.def.name, self.default_state.len());
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
    /// Returns the positional state values (schema field order).
    fn find_snapshot_at_or_before(
        &self,
        group_key_bytes: &[u8],
        block: BlockNumber,
    ) -> Option<&[Value]> {
        self.block_snapshots
            .get(group_key_bytes)
            .and_then(|snapshots| {
                snapshots
                    .range(..=block)
                    .next_back()
                    .map(|(_, values)| values.as_slice())
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

/// Load reducer state from storage, avoiding borrowing the entire ReducerEngine.
fn load_state_from(
    storage: &dyn StorageBackend,
    reducer_name: &str,
    group_key_bytes: &[u8],
    default_state: &[Value],
) -> Result<Vec<Value>> {
    if let Some(bytes) = storage.get_reducer_finalized(reducer_name, group_key_bytes)? {
        return decode_state(&bytes, reducer_name, default_state.len());
    }
    Ok(default_state.to_vec())
}

/// Decode a finalized state that `write_checkpoint` encoded.
fn decode_state(bytes: &[u8], reducer_name: &str, fields: usize) -> Result<Vec<Value>> {
    let unreadable =
        |reason: String| Error::Storage(format!("unreadable state of reducer '{reducer_name}': {reason}"));

    let values = storage::decode_values(bytes).map_err(|e| unreadable(e.to_string()))?;
    if values.len() != fields {
        return Err(unreadable(format!(
            "{} values for {fields} state fields",
            values.len()
        )));
    }
    Ok(values)
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
    use crate::types::RowMap;
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
        let affected = engine.rollback(1000, &PendingWrites::default()).unwrap();
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
        engine.advance_finality(1000);
        engine.write_checkpoint(1000, &mut batch);
        storage.commit(&batch).unwrap();

        // Block 1002: buy 3 (will be rolled back)
        engine
            .process_block(1002, &[make_trade("alice", "buy", 3.0, 2200.0)])
            .unwrap();

        // Rollback to 1001
        engine.rollback(1001, &PendingWrites::default()).unwrap();

        // After rollback: state should be at block 1001 (qty=15, cost=30500)
        let output = engine
            .process_block(1002, &[make_trade("alice", "sell", 15.0, 2100.0)])
            .unwrap();
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        // avg cost = 30500/15 = 2033.33, sell 15 @ 2100: pnl = 15 * (2100 - 2033.33) = 1000
        assert!((pnl - 1000.0).abs() < 0.01);
    }

    /// Under deferral a finalize keeps only the newest finalized snapshot of
    /// a group, and the next checkpoint persists that state.
    #[test]
    fn deferred_finalize_keeps_newest_finalized_snapshot() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine =
            ReducerEngine::new(pnl_reducer_def(), storage.clone(), &trade_registry(), &[]);

        for block in 1..=10 {
            engine
                .process_block(block, &[make_trade("alice", "buy", 1.0, 100.0)])
                .unwrap();
            if block > 2 {
                engine.advance_finality(block - 2);
            }
        }
        let blocks: Vec<BlockNumber> = engine
            .block_snapshots
            .values()
            .flat_map(|snapshots| snapshots.keys().copied())
            .collect();
        assert_eq!(blocks, vec![8, 9, 10]);

        let mut batch = StorageWriteBatch::new();
        engine.advance_finality(9);
        engine.write_checkpoint(9, &mut batch);
        storage.commit(&batch).unwrap();

        // Block 10 rolls back onto the persisted state: 9 bought at 100.
        engine.rollback(9, &PendingWrites::default()).unwrap();
        let output = engine
            .process_block(10, &[make_trade("alice", "sell", 9.0, 200.0)])
            .unwrap();
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        assert!((pnl - 900.0).abs() < 0.01);
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
                    emit({total = state.count})
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
                state.set("quantity", Value::Float64(qty + amount));
                state.set("cost_basis", Value::Float64(cost + amount * price));
                emit.insert("trade_pnl".into(), Value::Float64(0.0));
            } else {
                let avg_cost = if qty > 0.0 { cost / qty } else { 0.0 };
                emit.insert(
                    "trade_pnl".into(),
                    Value::Float64(amount * (price - avg_cost)),
                );
                state.set("quantity", Value::Float64(qty - amount));
                state.set("cost_basis", Value::Float64(cost - amount * avg_cost));
            }
            let new_qty = state
                .get("quantity")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            emit.insert("position_size".into(), Value::Float64(new_qty));
            state.emit(emit)
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

        engine.rollback(1000, &PendingWrites::default()).unwrap();

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
            crate::reducer_runtime::fn_reducer::FnReducerRuntime::new(|_ctx, _row| {}),
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

    /// An external reducer `counter` over `events`, grouped by user.
    fn external_counter() -> ReducerEngine {
        let def = external_counter_def();
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let reg = ColumnRegistry::new(vec!["user".to_string(), "amount".to_string()]);
        ReducerEngine::new(def, storage, &reg, &[])
    }

    fn external_counter_def() -> ReducerDef {
        ReducerDef {
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
        }
    }

    fn event(user: &str, amount: f64) -> Row {
        let registry = Arc::new(ColumnRegistry::new(vec![
            "user".to_string(),
            "amount".to_string(),
        ]));
        let map = HashMap::from([
            ("user".into(), Value::String(user.into())),
            ("amount".into(), Value::Float64(amount)),
        ]);
        Row::from_owned_map(registry, map)
    }

    /// Batched path must preserve original input row order in emits.
    #[test]
    fn batched_path_preserves_emit_order() {
        use crate::reducer_runtime::external::install_test_context;

        let mut engine = external_counter();

        // Install test context that increments count and emits it
        let _guard = install_test_context(|groups| {
            for group in groups.iter_mut() {
                for (index, row) in group.rows.iter().enumerate() {
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
                        .push((index, HashMap::from([("count".into(), Value::Float64(new_count))])));
                }
            }
        });

        // Input: alice, bob, alice — interleaved
        let rows = vec![event("alice", 1.0), event("bob", 10.0), event("alice", 2.0)];

        let output = engine.process_block(1000, &rows).unwrap();

        // The key assertion: output follows original input order (alice, bob, alice)
        // not grouped order (alice, alice, bob)
        assert_eq!(output.len(), 3);
        assert_eq!(output[0].get("user"), Some(&Value::String("alice".into())));
        assert_eq!(output[1].get("user"), Some(&Value::String("bob".into())));
        assert_eq!(output[2].get("user"), Some(&Value::String("alice".into())));
    }

    /// A row that emits nothing does not shift the emits of its group: each
    /// emit keeps the place of the row that emitted it.
    #[test]
    fn batched_path_keeps_order_when_a_row_emits_nothing() {
        use crate::reducer_runtime::external::install_test_context;

        let mut engine = external_counter();
        let _guard = install_test_context(|groups| {
            for group in groups.iter_mut() {
                for (index, row) in group.rows.iter().enumerate() {
                    let amount = row.get("amount").cloned().unwrap();
                    if amount != Value::Float64(0.0) {
                        group.emits.push((index, HashMap::from([("amount".into(), amount)])));
                    }
                }
            }
        });

        let rows = vec![event("alice", 0.0), event("bob", 1.0), event("alice", 2.0)];
        let output = engine.process_block(1000, &rows).unwrap();

        let amounts: Vec<_> = output.iter().map(|r| r.get("amount").cloned().unwrap()).collect();
        assert_eq!(amounts, [Value::Float64(1.0), Value::Float64(2.0)]);
    }

    /// A host callback sees each STATE field, and sets only those: a field
    /// that it leaves out is Null, one that STATE lacks is gone.
    #[test]
    fn batched_path_keeps_the_state_fields() {
        use crate::reducer_runtime::external::install_test_context;
        use std::cell::RefCell;
        use std::rc::Rc;

        let mut engine = external_counter();
        let seen: Rc<RefCell<Vec<RowMap>>> = Rc::default();
        let seen_in_callback = seen.clone();
        let _guard = install_test_context(move |groups| {
            for group in groups.iter_mut() {
                seen_in_callback.borrow_mut().push(group.state.clone());
                group.state.clear();
                group.state.insert("extra".into(), Value::UInt64(7));
                if seen_in_callback.borrow().len() == 1 {
                    group.state.insert("count".into(), Value::Float64(5.0));
                }
            }
        });

        for block in 1..=3 {
            engine.process_block(block, &[event("alice", 1.0)]).unwrap();
        }

        let count = |state: &RowMap| (state.len(), state.get("count").cloned());
        let seen: Vec<_> = seen.borrow().iter().map(count).collect();
        assert_eq!(
            seen,
            [
                (1, Some(Value::Float64(0.0))),
                (1, Some(Value::Float64(5.0))),
                (1, Some(Value::Null)),
            ]
        );
    }

    /// A reducer whose emits use more distinct columns than a position holds
    /// fails the block.
    #[test]
    fn emits_past_the_column_limit_fail_the_block() {
        use crate::reducer_runtime::MAX_EMIT_COLUMNS;
        use crate::reducer_runtime::fn_reducer::FnReducerRuntime;

        let runtime = FnReducerRuntime::new(|ctx, _row| {
            for i in 0..MAX_EMIT_COLUMNS {
                ctx.column(&format!("c{i}"));
            }
            ctx.emit(HashMap::from([("amount".to_string(), Value::Float64(1.0))]));
        });
        let def = ReducerDef {
            body: ReducerBody::External {
                id: "wide".to_string(),
            },
            ..external_counter_def()
        };
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let reg = ColumnRegistry::new(vec!["user".to_string(), "amount".to_string()]);
        let mut engine = ReducerEngine::with_runtime(def, storage, &reg, Box::new(runtime));

        let err = engine.process_block(1, &[event("alice", 1.0)]).unwrap_err();
        assert!(err.to_string().contains("distinct emit columns"), "{err}");
    }

    /// An emit that names a row its group lacks fails the block.
    #[test]
    fn batched_path_rejects_an_emit_of_a_missing_row() {
        use crate::reducer_runtime::external::install_test_context;

        let mut engine = external_counter();
        let _guard = install_test_context(|groups| {
            groups[0].emits.push((groups[0].rows.len(), HashMap::new()));
        });

        let rows = vec![event("alice", 1.0)];
        assert!(engine.process_block(1000, &rows).is_err());
    }
}
