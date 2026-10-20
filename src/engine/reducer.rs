use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::error::{Error, Result};
#[cfg(feature = "lua")]
use crate::reducer_runtime::lua::LuaRuntime;
use crate::reducer_runtime::map::StateMaps;
use crate::reducer_runtime::{Emits, GroupBatch, GroupState, GroupedRows, ReducerRuntime};
use crate::schema::ast::{ReducerBody, ReducerDef};
use crate::storage::{self, StorageBackend, StorageWriteBatch};
use crate::types::{BlockNumber, ColumnId, ColumnRegistry, Row, Value};

/// Orchestrates a single reducer: state management, rollback, and output.
///
/// State is kept entirely in memory during normal processing. Storage is only
/// used for persisting finalized state. Each group keeps its state from
/// before each block that changed it and is not final yet, so that rollback
/// can restore to any unfinalized block without serialization overhead.
pub struct ReducerEngine {
    def: ReducerDef,
    runtime: Box<dyn ReducerRuntime>,
    storage: Arc<dyn StorageBackend>,
    /// The STATE fields but the MAP fields, in schema order: the layout of
    /// a group's state in memory and in storage.
    state_fields: ColumnRegistry,
    /// The MAP fields in schema order; empty for most reducers.
    map_fields: Arc<ColumnRegistry>,
    /// Cached default state (computed once from def.state).
    default_state: Vec<Value>,
    /// Every group since open or `reload`.
    groups: Groups,
    /// Whether a new group reads its state from storage. False when storage
    /// held no state of the reducer at open or `reload`, as on a fresh
    /// database: groups are never evicted, so storage then holds no group
    /// that `groups` lacks.
    reads_storage: bool,
    /// The groups that each block not final yet changed, by id. Each of them
    /// holds an undo entry for the block.
    block_groups: BTreeMap<BlockNumber, Vec<usize>>,
    /// Groups finalized since the last checkpoint, which `write_checkpoint`
    /// persists. Under backfill deferral they wait here for several
    /// finalizations.
    pending_durable: Vec<usize>,
    /// The group key of the current row.
    key: Vec<u8>,
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

/// One group of a reducer.
struct Group {
    /// The key of the group's finalized state in storage.
    key: Box<[u8]>,
    /// The current state, in the order of the STATE fields.
    state: Vec<Value>,
    /// The state before each block that changed it and is not final yet,
    /// oldest first. A rollback restores the first one above its fork
    /// point. The state as of finality is the first one above finality,
    /// else `state`.
    undo: VecDeque<(BlockNumber, Vec<Value>)>,
    /// Whether `pending_durable` holds the group.
    pending_durable: bool,
    /// The MAP fields, with their own undo; None for a reducer without
    /// them.
    maps: Option<Box<StateMaps>>,
}

/// The groups of a reducer by id, their index in `all`. Never evicted: a
/// group that is not here loads its stored state, which lacks the
/// checkpoints of unacked batches.
#[derive(Default)]
struct Groups {
    all: Vec<Group>,
    ids: FxHashMap<Box<[u8]>, usize>,
}

impl Groups {
    /// The id of the group `key`. On its first row, `load` gives its state.
    fn id(&mut self, key: &[u8], load: impl FnOnce() -> Result<LoadedState>) -> Result<usize> {
        if let Some(&id) = self.ids.get(key) {
            return Ok(id);
        }

        let (state, maps) = load()?;
        let id = self.all.len();
        self.all.push(Group {
            key: key.into(),
            state,
            undo: VecDeque::new(),
            pending_durable: false,
            maps,
        });
        self.ids.insert(key.into(), id);
        Ok(id)
    }

    /// The group `id`, about to change in `block`. Its first change in the
    /// block keeps the state before it, and adds the group to `changed`.
    fn change(&mut self, id: usize, block: BlockNumber, changed: &mut Vec<usize>) -> &mut Group {
        let group = &mut self.all[id];
        let first = group.undo.back().is_none_or(|(b, _)| *b != block);
        if first {
            group.undo.push_back((block, group.state.clone()));
            changed.push(id);
            if let Some(maps) = &mut group.maps {
                maps.begin(block);
            }
        }
        group
    }

    fn clear(&mut self) {
        self.all.clear();
        self.ids.clear();
    }
}

impl ReducerEngine {
    pub fn new(
        def: ReducerDef,
        storage: Arc<dyn StorageBackend>,
        source_registry: &ColumnRegistry,
        modules: &[(String, String)],
    ) -> Result<Self> {
        let runtime = Self::make_runtime(&def, source_registry, modules)?;
        Ok(Self::build(def, storage, source_registry, runtime))
    }

    /// Create a ReducerEngine that sources from another reducer's output,
    /// whose columns are known only from its rows.
    pub fn new_chained(
        def: ReducerDef,
        storage: Arc<dyn StorageBackend>,
        modules: &[(String, String)],
    ) -> Result<Self> {
        let empty_registry = ColumnRegistry::default();
        let runtime = Self::make_runtime(&def, &empty_registry, modules)?;
        Ok(Self::build(def, storage, &empty_registry, runtime))
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
        let field_names = |map: bool| {
            let names = def.state.iter().filter(|f| f.map == map);
            ColumnRegistry::new(names.map(|f| f.name.clone()).collect())
        };
        let state_fields = field_names(false);
        let map_fields = Arc::new(field_names(true));
        let default_state = def
            .state
            .iter()
            .filter(|f| !f.map)
            .map(|f| parse_default(&f.default, &f.column_type))
            .collect();
        let group_by_ids = def
            .group_by
            .iter()
            .map(|col| source_registry.get_id(col))
            .collect();
        let emits = Emits::with_columns(&def.group_by);
        let reads_storage = stores_state(storage.as_ref(), &def.name);

        Self {
            def,
            runtime,
            storage,
            state_fields,
            map_fields,
            default_state,
            groups: Groups::default(),
            reads_storage,
            block_groups: BTreeMap::new(),
            pending_durable: Vec::new(),
            key: Vec::new(),
            group_by_ids,
            source_registry: Arc::new(source_registry.clone()),
            emits,
            emit_growth: BTreeMap::new(),
        }
    }

    /// Build a runtime for the given reducer definition. Err(Schema) if its
    /// body or a module does not load.
    #[allow(unused_variables)]
    fn make_runtime(
        def: &ReducerDef,
        source_registry: &ColumnRegistry,
        modules: &[(String, String)],
    ) -> Result<Box<dyn ReducerRuntime>> {
        match &def.body {
            #[cfg(feature = "lua")]
            ReducerBody::Lua { script } => {
                let requiredmodules: Vec<(String, String)> = def
                    .requires
                    .iter()
                    .filter_map(|name| modules.iter().find(|(n, _)| n == name).cloned())
                    .collect();
                let typed = |map: bool| -> Vec<(String, crate::types::ColumnType)> {
                    let fields = def.state.iter().filter(|f| f.map == map);
                    fields
                        .map(|f| (f.name.clone(), f.column_type.clone()))
                        .collect()
                };
                let state_types = typed(false);
                let state_fields: Vec<String> =
                    state_types.iter().map(|(name, _)| name.clone()).collect();
                let runtime = LuaRuntime::with_fields(
                    script,
                    &state_fields,
                    &state_types,
                    &typed(true),
                    source_registry.names(),
                    &requiredmodules,
                )
                .map_err(|e| match e {
                    Error::Schema(m) => Error::Schema(format!("reducer '{}': {m}", def.name)),
                    other => other,
                })?;
                Ok(Box::new(runtime))
            }
            #[cfg(not(feature = "lua"))]
            ReducerBody::Lua { .. } => {
                // The schema parser already rejects Lua bodies when the lua feature is
                // disabled, so this branch is unreachable in normal usage.
                unreachable!("Lua reducer body reached without lua feature — schema parser should have rejected it");
            }
            ReducerBody::External { id } => Ok(Box::new(
                crate::reducer_runtime::external::ExternalRuntime::new(id.clone()),
            )),
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
    /// State is updated in memory only. Each group keeps its state from
    /// before its first change in the block — no storage I/O or
    /// serialization happens here, except a group's first read.
    ///
    /// Two code paths:
    /// - **Per-row** (default): iterates rows in order, calls `process()` per row.
    ///   No extra allocations. Used by Lua.
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
    ///
    /// A group is in `block_groups` before its row can fail: the undo of the
    /// failed ingest then restores the groups that the rows before the
    /// error changed.
    fn process_block_per_row(&mut self, block: BlockNumber, rows: &[Row]) -> Result<()> {
        let changed = self.block_groups.entry(block).or_default();
        let storage = self.reads_storage.then_some(self.storage.as_ref());

        for (index, row) in rows.iter().enumerate() {
            write_group_key(&mut self.key, &self.group_by_ids, row);
            let id = self.groups.id(&self.key, || {
                load_state_from(
                    storage,
                    &self.def.name,
                    &self.key,
                    &self.default_state,
                    &self.map_fields,
                )
            })?;
            let group = self.groups.change(id, block, changed);

            self.emits.set_source(index);
            let state = GroupState {
                values: &mut group.state,
                fields: &self.state_fields,
                maps: group.maps.as_deref_mut(),
            };
            self.runtime
                .process(state, row, &mut self.emits)
                .and_then(|()| self.emits.check())?;
        }
        Ok(())
    }

    /// Batched path: groups rows by key, calls process_grouped() once.
    /// Avoids per-row FFI overhead for external reducers.
    fn process_block_batched(&mut self, block: BlockNumber, rows: &[Row]) -> Result<()> {
        // The group of each row as an index in `ids`; the groups in order of
        // their first row. Each group loads before any state leaves
        // `groups`: a failed load then loses none.
        let mut ids: Vec<usize> = Vec::new();
        let mut sizes: Vec<usize> = Vec::new();
        let mut row_groups: Vec<usize> = Vec::with_capacity(rows.len());
        let mut slots: FxHashMap<usize, usize> = FxHashMap::default();
        let storage = self.reads_storage.then_some(self.storage.as_ref());
        for row in rows {
            write_group_key(&mut self.key, &self.group_by_ids, row);
            let id = self.groups.id(&self.key, || {
                load_state_from(
                    storage,
                    &self.def.name,
                    &self.key,
                    &self.default_state,
                    &self.map_fields,
                )
            })?;
            let slot = *slots.entry(id).or_insert_with(|| {
                ids.push(id);
                sizes.push(0);
                ids.len() - 1
            });
            sizes[slot] += 1;
            row_groups.push(slot);
        }

        let changed = self.block_groups.entry(block).or_default();
        let mut groups = Vec::with_capacity(ids.len());
        let mut start = 0;
        for (&id, &size) in ids.iter().zip(&sizes) {
            let group = self.groups.change(id, block, changed);
            groups.push(GroupBatch {
                state: std::mem::take(&mut group.state),
                maps: group.maps.take(),
                rows: start..start + size,
            });
            start += size;
        }

        let mut next: Vec<usize> = groups.iter().map(|group| group.rows.start).collect();
        let mut order = vec![0; rows.len()];
        for (index, &slot) in row_groups.iter().enumerate() {
            order[next[slot]] = index;
            next[slot] += 1;
        }

        let mut grouped = GroupedRows {
            rows,
            order,
            groups,
        };
        let processed =
            self.runtime
                .process_grouped(&mut grouped, &self.state_fields, &mut self.emits);

        // Also on failure, as on the per-row path: the undo of the failed
        // ingest restores the states from before the block.
        for (&id, group) in ids.iter().zip(grouped.groups) {
            let stored = &mut self.groups.all[id];
            stored.state = group.state;
            stored.maps = group.maps;
        }
        processed?;
        self.emits.check()
    }

    /// Roll back all blocks after fork_point: each group they changed goes
    /// back to its state before the first of them.
    /// Returns the number of groups affected.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> usize {
        // Guard: fork_point + 1 would overflow u64::MAX to 0, causing split_off(&0)
        // to remove the entire map. MAX is a valid no-op: nothing exists after it.
        if fork_point == BlockNumber::MAX {
            return 0;
        }

        let grown = self.emit_growth.split_off(&(fork_point + 1));
        if let Some(&width) = grown.values().next() {
            self.emits.truncate_columns(width);
        }

        let rolled_back = self.block_groups.split_off(&(fork_point + 1));
        let mut affected = 0;
        for id in rolled_back.into_values().flatten() {
            let group = &mut self.groups.all[id];
            let kept = group.undo.partition_point(|(block, _)| *block <= fork_point);
            // Only the first block of the group restores: it drains the rest.
            if let Some((_, state)) = group.undo.drain(kept..).next() {
                group.state = state;
                affected += 1;
            }
            if let Some(maps) = &mut group.maps {
                maps.rollback(fork_point);
            }
        }
        affected
    }

    /// Drop all in-memory state: each group loads its stored state again on
    /// its next row.
    pub fn reload(&mut self) {
        self.groups.clear();
        self.reads_storage = stores_state(self.storage.as_ref(), &self.def.name);
        self.block_groups.clear();
        self.pending_durable.clear();
        self.emits.truncate_columns(self.def.group_by.len());
        self.emit_growth.clear();
    }

    #[cfg(test)]
    pub fn first_unfinal_block(&self) -> Option<BlockNumber> {
        self.block_groups.keys().next().copied()
    }

    /// Merge the blocks up to `block` into finalized state: they can no longer
    /// roll back, so their undo entries go. Their groups wait in
    /// `pending_durable` for `write_checkpoint`.
    pub fn advance_finality(&mut self, block: BlockNumber) {
        let remaining = self.block_groups.split_off(&(block + 1));
        let finalized = std::mem::replace(&mut self.block_groups, remaining);

        for id in finalized.into_values().flatten() {
            let group = &mut self.groups.all[id];
            let final_entries = group.undo.partition_point(|(b, _)| *b <= block);
            group.undo.drain(..final_entries);
            if let Some(maps) = &mut group.maps {
                maps.finalize(block);
            }

            if !group.pending_durable {
                group.pending_durable = true;
                self.pending_durable.push(id);
            }
        }
    }

    /// Write the state as of `finalized` of every group in `pending_durable`.
    pub fn write_checkpoint(&mut self, finalized: BlockNumber, batch: &mut StorageWriteBatch) {
        for id in self.pending_durable.drain(..) {
            let group = &mut self.groups.all[id];
            group.pending_durable = false;

            let above = group.undo.partition_point(|(b, _)| *b <= finalized);
            let state = group.undo.get(above).map_or(&group.state, |(_, state)| state);
            batch.set_reducer_finalized(&self.def.name, group.key.to_vec(), storage::encode_values(state));
            if let Some(maps) = &mut group.maps {
                maps.write_checkpoint(finalized, &self.def.name, &group.key, batch);
            }
        }
    }
}

/// Write the group key of `row` into `key`: the raw bytes of a single
/// String group-by column, else the encoded group-by values, Null for a
/// column that the row lacks.
fn write_group_key(key: &mut Vec<u8>, group_by_ids: &[Option<ColumnId>], row: &Row) {
    key.clear();
    if group_by_ids.is_empty() {
        return;
    }

    let values = row.values();
    if let [Some(id)] = group_by_ids
        && let Value::String(s) = &values[*id as usize]
    {
        key.extend_from_slice(s.as_bytes());
        return;
    }

    let group_values = group_by_ids
        .iter()
        .map(|id| id.map_or(&Value::Null, |i| &values[i as usize]));
    storage::write_group_key(key, group_values);
}

/// Whether storage may hold a finalized state of `reducer`. A failed probe
/// counts as yes: the reads of the groups then report the error.
fn stores_state(storage: &dyn StorageBackend, reducer: &str) -> bool {
    storage.has_reducer_finalized(reducer).unwrap_or(true)
}

/// The state of a group as it loads: its values, and its MAP fields if the
/// reducer has any.
type LoadedState = (Vec<Value>, Option<Box<StateMaps>>);

/// The state of a new group: its finalized state in `storage`, else the
/// default. No `storage` when it holds no state of the reducer. A group has
/// stored MAP entries only if it has a stored state.
fn load_state_from(
    storage: Option<&dyn StorageBackend>,
    reducer_name: &str,
    group_key_bytes: &[u8],
    default_state: &[Value],
    map_fields: &Arc<ColumnRegistry>,
) -> Result<LoadedState> {
    let mut maps = (!map_fields.is_empty()).then(|| Box::new(StateMaps::new(map_fields.clone())));

    if let Some(storage) = storage
        && let Some(bytes) = storage.get_reducer_finalized(reducer_name, group_key_bytes)?
    {
        let state = decode_state(&bytes, reducer_name, default_state.len())?;
        if let Some(maps) = &mut maps {
            load_map_entries(storage, reducer_name, group_key_bytes, maps)?;
        }
        return Ok((state, maps));
    }
    Ok((default_state.to_vec(), maps))
}

/// Read the stored MAP entries of a group into `maps`.
fn load_map_entries(
    storage: &dyn StorageBackend,
    reducer_name: &str,
    group_key_bytes: &[u8],
    maps: &mut StateMaps,
) -> Result<()> {
    let unreadable = |reason: String| {
        Error::Storage(format!(
            "unreadable Map entry of reducer '{reducer_name}': {reason}"
        ))
    };

    for (map, key, bytes) in storage.get_reducer_map_entries(reducer_name, group_key_bytes)? {
        let map = map as usize;
        if map >= maps.names().len() {
            return Err(unreadable(format!("Map field {map} of {}", maps.names().len())));
        }
        let value = storage::decode_value_bytes(&bytes).map_err(|e| unreadable(e.to_string()))?;
        maps.insert_stored(map, &key, value);
    }
    Ok(())
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
    use std::collections::HashMap;

    use super::*;
    use crate::schema::ast::*;
    use crate::storage::memory::MemoryBackend;
    use crate::types::ColumnRegistry;
    use crate::types::RowMap;
    use crate::types::ColumnType;

    /// The PnL of a trader; a sell before any buy has an average cost of 0.
    #[cfg(feature = "lua")]
    const PNL_LUA: &str = r#"
        local trade_pnl = 0
        if row.side == "buy" then
            state.quantity = state.quantity + row.amount
            state.cost_basis = state.cost_basis + row.amount * row.price
        elseif row.side == "sell" then
            local avg_cost = 0
            if state.quantity ~= 0 then avg_cost = state.cost_basis / state.quantity end
            state.quantity = state.quantity - row.amount
            state.cost_basis = state.cost_basis - row.amount * avg_cost
            trade_pnl = row.amount * (row.price - avg_cost)
        end
        emit { trade_pnl = trade_pnl, position_size = state.quantity }
    "#;

    #[cfg(feature = "lua")]
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
                    map: false,
                },
                StateField {
                    name: "cost_basis".to_string(),
                    column_type: crate::types::ColumnType::Float64,
                    default: "0".to_string(),
                    map: false,
                },
            ],
            requires: vec![],
            body: ReducerBody::Lua {
                script: PNL_LUA.to_string(),
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

    #[cfg(feature = "lua")]
    #[test]
    fn reducer_processes_rows_and_emits_output() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine =
            ReducerEngine::new(pnl_reducer_def(), storage, &trade_registry(), &[]).unwrap();

        let rows = vec![
            make_trade("alice", "buy", 10.0, 2000.0),
            make_trade("alice", "buy", 5.0, 2100.0),
        ];

        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(output.len(), 2);

        // Both emits should have trade_pnl = 0 (buys)
        assert_eq!(output[0].get("trade_pnl"), Some(&Value::Int64(0)));
        assert_eq!(output[0].get("position_size"), Some(&Value::Float64(10.0)));
        // user group-by column should be forwarded
        assert_eq!(output[0].get("user"), Some(&Value::String("alice".into())));

        assert_eq!(output[1].get("position_size"), Some(&Value::Float64(15.0)));
    }

    #[cfg(feature = "lua")]
    #[test]
    fn reducer_state_persists_across_blocks() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine =
            ReducerEngine::new(pnl_reducer_def(), storage, &trade_registry(), &[]).unwrap();

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

    #[cfg(feature = "lua")]
    #[test]
    fn reducer_rollback_restores_state() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine =
            ReducerEngine::new(pnl_reducer_def(), storage, &trade_registry(), &[]).unwrap();

        // Block 1: buy 10 @ 2000
        engine
            .process_block(1000, &[make_trade("alice", "buy", 10.0, 2000.0)])
            .unwrap();

        // Block 2: buy 5 @ 2100 (will be rolled back)
        engine
            .process_block(1001, &[make_trade("alice", "buy", 5.0, 2100.0)])
            .unwrap();

        // Rollback block 2
        let affected = engine.rollback(1000);
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

    #[cfg(feature = "lua")]
    #[test]
    fn reducer_multiple_groups() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine =
            ReducerEngine::new(pnl_reducer_def(), storage, &trade_registry(), &[]).unwrap();

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

    #[cfg(feature = "lua")]
    #[test]
    fn reducer_finalize_then_rollback() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine =
            ReducerEngine::new(pnl_reducer_def(), storage.clone(), &trade_registry(), &[]).unwrap();

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
        engine.rollback(1001);

        // After rollback: state should be at block 1001 (qty=15, cost=30500)
        let output = engine
            .process_block(1002, &[make_trade("alice", "sell", 15.0, 2100.0)])
            .unwrap();
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        // avg cost = 30500/15 = 2033.33, sell 15 @ 2100: pnl = 15 * (2100 - 2033.33) = 1000
        assert!((pnl - 1000.0).abs() < 0.01);
    }

    /// Under deferral a finalize drops the undo entries up to finality, and
    /// the next checkpoint persists the state as of finality, not the state
    /// of a later block.
    #[cfg(feature = "lua")]
    #[test]
    fn deferred_finalize_persists_the_state_as_of_finality() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine =
            ReducerEngine::new(pnl_reducer_def(), storage.clone(), &trade_registry(), &[]).unwrap();

        for block in 1..=10 {
            engine
                .process_block(block, &[make_trade("alice", "buy", 1.0, 100.0)])
                .unwrap();
            if block > 2 {
                engine.advance_finality(block - 2);
            }
        }
        let blocks: Vec<BlockNumber> = engine.groups.all[0].undo.iter().map(|(b, _)| *b).collect();
        assert_eq!(blocks, vec![9, 10]);

        let mut batch = StorageWriteBatch::new();
        engine.advance_finality(9);
        engine.write_checkpoint(9, &mut batch);
        storage.commit(&batch).unwrap();

        let stored = storage.get_reducer_finalized("pnl_tracker", b"alice").unwrap().unwrap();
        let stored = storage::decode_values(&stored).unwrap();
        assert_eq!(stored, [Value::Float64(9.0), Value::Float64(900.0)]);

        // Block 10 rolls back onto the persisted state: 9 bought at 100.
        engine.rollback(9);
        let output = engine
            .process_block(10, &[make_trade("alice", "sell", 9.0, 200.0)])
            .unwrap();
        let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
        assert!((pnl - 900.0).abs() < 0.01);
    }

    /// A new group reads storage only when storage holds a state of the
    /// reducer: not on a fresh database, but after a reopen or a `reload`
    /// once a checkpoint is stored.
    #[cfg(feature = "lua")]
    #[test]
    fn new_groups_read_storage_once_it_holds_state() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine =
            ReducerEngine::new(pnl_reducer_def(), storage.clone(), &trade_registry(), &[]).unwrap();
        assert!(!engine.reads_storage);

        engine
            .process_block(1, &[make_trade("alice", "buy", 10.0, 100.0)])
            .unwrap();
        let mut batch = StorageWriteBatch::new();
        engine.advance_finality(1);
        engine.write_checkpoint(1, &mut batch);
        storage.commit(&batch).unwrap();
        assert!(!engine.reads_storage);

        engine.reload();
        let mut reopened =
            ReducerEngine::new(pnl_reducer_def(), storage.clone(), &trade_registry(), &[]).unwrap();

        // Both continue from the stored 10 bought at 100.
        for engine in [&mut engine, &mut reopened] {
            assert!(engine.reads_storage);
            let output = engine
                .process_block(2, &[make_trade("alice", "sell", 10.0, 200.0)])
                .unwrap();
            let pnl = output[0].get("trade_pnl").unwrap().as_f64().unwrap();
            assert!((pnl - 1000.0).abs() < 0.01);
        }
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
                map: false,
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
        let mut engine = ReducerEngine::new(def, storage, &events_registry, &[]).unwrap();

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

    /// `external_counter` in Lua, which fails on an amount of 13.
    #[cfg(feature = "lua")]
    fn lua_counter() -> ReducerEngine {
        let def = ReducerDef {
            body: ReducerBody::Lua {
                script: r#"
                    if row.amount == 13 then error("unlucky amount") end
                    state.count = state.count + row.amount
                    emit({count = state.count})
                "#
                .to_string(),
            },
            ..external_counter_def()
        };
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let reg = ColumnRegistry::new(vec!["user".to_string(), "amount".to_string()]);
        ReducerEngine::new(def, storage, &reg, &[]).unwrap()
    }

    #[cfg(feature = "lua")]
    fn user_values(output: &[Row], column: &str) -> Vec<(String, Value)> {
        output
            .iter()
            .map(|row| {
                let user = row.get("user").and_then(|v| v.as_str()).unwrap();
                (user.to_string(), row.get(column).cloned().unwrap())
            })
            .collect()
    }

    #[cfg(feature = "lua")]
    fn expected(rows: &[(&str, f64)]) -> Vec<(String, Value)> {
        rows.iter()
            .map(|&(user, value)| (user.to_string(), Value::Float64(value)))
            .collect()
    }

    /// Lua runs a block group by group: each emit keeps the place of its
    /// row, and each group keeps its state across rows, blocks and rollback.
    #[cfg(feature = "lua")]
    #[test]
    fn lua_groups_keep_input_order_and_state() {
        let mut engine = lua_counter();

        let rows = [event("alice", 1.0), event("bob", 10.0), event("alice", 2.0)];
        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(
            user_values(&output, "count"),
            expected(&[("alice", 1.0), ("bob", 10.0), ("alice", 3.0)])
        );

        let output = engine.process_block(1001, &[event("alice", 5.0)]).unwrap();
        assert_eq!(user_values(&output, "count"), expected(&[("alice", 8.0)]));

        engine.rollback(1000);
        let rows = [event("bob", 1.0), event("alice", 1.0)];
        let output = engine.process_block(1001, &rows).unwrap();
        assert_eq!(
            user_values(&output, "count"),
            expected(&[("bob", 11.0), ("alice", 4.0)])
        );
    }

    /// A block that fails in its second group rolls back both groups: also
    /// the first, which had finished.
    #[cfg(feature = "lua")]
    #[test]
    fn lua_failed_block_rolls_back_every_group() {
        let mut engine = lua_counter();
        engine
            .process_block(1000, &[event("alice", 1.0), event("bob", 10.0)])
            .unwrap();

        let rows = [event("bob", 2.0), event("alice", 5.0), event("alice", 13.0)];
        assert!(engine.process_block(1001, &rows).is_err());
        engine.rollback(1000);

        let rows = [event("alice", 5.0), event("bob", 2.0)];
        let output = engine.process_block(1001, &rows).unwrap();
        assert_eq!(
            user_values(&output, "count"),
            expected(&[("alice", 6.0), ("bob", 12.0)])
        );
    }

    /// A JSON state carries from row to row of its group in a block, and
    /// from the engine's JSON into the next block.
    #[cfg(feature = "lua")]
    #[test]
    fn lua_json_state_carries_across_rows_and_blocks() {
        let def = ReducerDef {
            name: "holdings".to_string(),
            source: "trades".to_string(),
            group_by: vec!["user".to_string()],
            state: vec![StateField {
                name: "book".to_string(),
                column_type: ColumnType::JSON,
                default: "'{}'".to_string(),
                map: false,
            }],
            requires: vec![],
            body: ReducerBody::Lua {
                script: r#"
                    local held = (state.book[row.token] or 0) + row.amount
                    state.book[row.token] = held
                    emit({held = held})
                "#
                .to_string(),
            },
        };
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let registry = Arc::new(ColumnRegistry::new(
            ["user", "token", "amount"].map(String::from).to_vec(),
        ));
        let mut engine = ReducerEngine::new(def, storage, &registry, &[]).unwrap();
        let trade = |user: &str, token: &str, amount: f64| {
            let values = vec![
                Value::String(user.into()),
                Value::String(token.into()),
                Value::Float64(amount),
            ];
            Row::from_values(registry.clone(), values)
        };

        let rows = [
            trade("alice", "A", 1.0),
            trade("bob", "A", 7.0),
            trade("alice", "B", 2.0),
            trade("alice", "A", 3.0),
        ];
        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(
            user_values(&output, "held"),
            expected(&[("alice", 1.0), ("bob", 7.0), ("alice", 2.0), ("alice", 4.0)])
        );

        let rows = [trade("alice", "A", 1.0), trade("alice", "B", 1.0)];
        let output = engine.process_block(1001, &rows).unwrap();
        assert_eq!(
            user_values(&output, "held"),
            expected(&[("alice", 5.0), ("alice", 3.0)])
        );
    }

    /// A row that the state keeps stays as it was when the next row of the
    /// block comes, as it did when the state converted after each row.
    #[cfg(feature = "lua")]
    #[test]
    fn lua_state_keeps_a_row_of_the_block() {
        let def = ReducerDef {
            state: vec![StateField {
                name: "last".to_string(),
                column_type: ColumnType::JSON,
                default: "'{}'".to_string(),
                map: false,
            }],
            body: ReducerBody::Lua {
                script: r#"
                    emit({previous = state.last.amount or 0.0})
                    state.last = row
                "#
                .to_string(),
            },
            ..external_counter_def()
        };
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let reg = ColumnRegistry::new(vec!["user".to_string(), "amount".to_string()]);
        let mut engine = ReducerEngine::new(def, storage, &reg, &[]).unwrap();

        let rows = [event("alice", 1.0), event("bob", 5.0), event("alice", 2.0)];
        let output = engine.process_block(1000, &rows).unwrap();
        assert_eq!(
            user_values(&output, "previous"),
            expected(&[("alice", 0.0), ("bob", 0.0), ("alice", 1.0)])
        );

        let output = engine.process_block(1001, &[event("alice", 3.0)]).unwrap();
        assert_eq!(
            user_values(&output, "previous"),
            expected(&[("alice", 2.0)])
        );
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
                    map: false,
                },
                StateField {
                    name: "cost_basis".to_string(),
                    column_type: crate::types::ColumnType::Float64,
                    default: "0".to_string(),
                    map: false,
                },
            ],
            requires: vec![],
            body: ReducerBody::External {
                id: "pnl".to_string(),
            },
        }
    }

    #[cfg(feature = "lua")]
    #[test]
    fn fn_reducer_produces_same_output_as_lua() {
        let storage = Arc::new(MemoryBackend::new());
        let mut fn_engine = ReducerEngine::with_runtime(
            pnl_external_def(),
            storage.clone(),
            &trade_registry(),
            Box::new(pnl_fn_runtime()),
        );

        let storage2 = Arc::new(MemoryBackend::new());
        let mut lua_engine =
            ReducerEngine::new(pnl_reducer_def(), storage2, &trade_registry(), &[]).unwrap();

        let rows = vec![
            make_trade("alice", "buy", 10.0, 2000.0),
            make_trade("alice", "buy", 5.0, 2100.0),
        ];

        let fn_out = fn_engine.process_block(1000, &rows).unwrap();
        let lua_out = lua_engine.process_block(1000, &rows).unwrap();

        assert_eq!(fn_out.len(), lua_out.len());
        for (f, e) in fn_out.iter().zip(lua_out.iter()) {
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

        engine.rollback(1000);

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
        let mut engine = ReducerEngine::new(def, storage, &reg, &[]).unwrap();

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
        ReducerEngine::new(def, storage, &reg, &[]).unwrap()
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
                map: false,
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

    /// `book Map(String, value_type)` for each user, with the Lua `script`.
    #[cfg(feature = "lua")]
    fn lua_book(script: &str, value_type: ColumnType, storage: Arc<dyn StorageBackend>) -> ReducerEngine {
        let def = ReducerDef {
            state: vec![StateField {
                name: "book".to_string(),
                column_type: value_type,
                default: String::new(),
                map: true,
            }],
            body: ReducerBody::Lua {
                script: script.to_string(),
            },
            ..external_counter_def()
        };
        let reg = ColumnRegistry::new(vec!["user".to_string(), "amount".to_string()]);
        ReducerEngine::new(def, storage, &reg, &[]).unwrap()
    }

    /// The `before` emits of `output`, in order; None for a Null.
    fn befores(output: &[Row]) -> Vec<Option<f64>> {
        output
            .iter()
            .map(|row| row.get("before").and_then(Value::as_f64))
            .collect()
    }

    /// Emits the entry `x` as it was, then sets it to the amount; 0 deletes
    /// it, 13 fails. The amount keeps an emit of a nil `before` from being
    /// empty, which Lua drops.
    #[cfg(feature = "lua")]
    const SET_X: &str = r#"
        if row.amount == 13 then error("unlucky amount") end
        local before = state.book:get("x")
        if row.amount == 0 then state.book:delete("x") else state.book:set("x", row.amount) end
        emit({ before = before, amount = row.amount })
    "#;

    /// Map entries roll back, and a checkpoint stores them as of finality: a
    /// reload reads the entry that a final block deleted as absent, and not
    /// the value that a later block set.
    #[cfg(feature = "lua")]
    #[test]
    fn lua_map_entries_roll_back_and_reload_as_of_finality() {
        let storage = Arc::new(MemoryBackend::new());
        let mut engine = lua_book(SET_X, ColumnType::Float64, storage.clone());

        let output = engine.process_block(1, &[event("alice", 1.0), event("bob", 5.0)]).unwrap();
        assert_eq!(befores(&output), [None, None]);
        let output = engine.process_block(2, &[event("alice", 2.0)]).unwrap();
        assert_eq!(befores(&output), [Some(1.0)]);

        engine.rollback(1);
        let output = engine.process_block(2, &[event("alice", 0.0)]).unwrap();
        assert_eq!(befores(&output), [Some(1.0)]);
        let output = engine.process_block(3, &[event("alice", 7.0), event("bob", 6.0)]).unwrap();
        assert_eq!(befores(&output), [None, Some(5.0)]);

        let mut batch = StorageWriteBatch::new();
        engine.advance_finality(2);
        engine.write_checkpoint(2, &mut batch);
        storage.commit(&batch).unwrap();

        engine.reload();
        let output = engine.process_block(3, &[event("alice", 4.0), event("bob", 8.0)]).unwrap();
        assert_eq!(befores(&output), [None, Some(5.0)]);
    }

    /// A block that fails in its second group undoes the map changes of
    /// both groups.
    #[cfg(feature = "lua")]
    #[test]
    fn lua_failed_block_rolls_back_map_entries() {
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let mut engine = lua_book(SET_X, ColumnType::Float64, storage);
        engine.process_block(1, &[event("alice", 1.0), event("bob", 5.0)]).unwrap();

        let rows = [event("bob", 6.0), event("alice", 2.0), event("alice", 13.0)];
        assert!(engine.process_block(2, &rows).is_err());
        engine.rollback(1);

        let output = engine.process_block(2, &[event("alice", 3.0), event("bob", 7.0)]).unwrap();
        assert_eq!(befores(&output), [Some(1.0), Some(5.0)]);
    }

    /// A Map field takes no index, and a Map of numbers takes no table.
    #[cfg(feature = "lua")]
    #[test]
    fn lua_map_misuse_fails_the_block() {
        let cases = [
            ("local p = state.book[row.user]", "takes no index"),
            ("state.book[row.user] = 1", "takes no index"),
            ("state.book:set(row.user, {})", "holds Float64 values, not tables"),
        ];
        for (script, error) in cases {
            let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
            let mut engine = lua_book(script, ColumnType::Float64, storage);
            let err = engine.process_block(1, &[event("alice", 1.0)]).unwrap_err();
            assert!(err.to_string().contains(error), "{script}: {err}");
        }
    }

    /// `get` on a Json map returns a copy: a change of the table stays only
    /// after a `set`.
    #[cfg(feature = "lua")]
    #[test]
    fn lua_json_map_get_returns_a_copy() {
        let script = r#"
            local p = state.book:get("p") or { n = 0 }
            p.n = p.n + 1
            if row.amount == 1 then state.book:set("p", p) end
            emit({ before = p.n })
        "#;
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let mut engine = lua_book(script, ColumnType::JSON, storage);

        let rows = [event("alice", 1.0), event("alice", 2.0), event("alice", 1.0)];
        let output = engine.process_block(1, &rows).unwrap();
        assert_eq!(befores(&output), [Some(1.0), Some(2.0), Some(2.0)]);
    }

    /// A Rust closure reads and writes a Map field through `StateCtx::map`.
    #[test]
    fn fn_reducer_uses_a_map_field() {
        use crate::reducer_runtime::fn_reducer::FnReducerRuntime;

        let runtime = FnReducerRuntime::new(|ctx, row| {
            assert!(ctx.map("missing").is_none());
            let amount = row.get("amount").cloned().unwrap_or(Value::Null);
            let mut book = ctx.map("book").expect("a Map field");
            let before = book.get("x").cloned().unwrap_or(Value::Null);
            book.set("x", amount);
            ctx.emit(HashMap::from([("before".to_string(), before)]));
        });
        let def = ReducerDef {
            state: vec![StateField {
                name: "book".to_string(),
                column_type: ColumnType::Float64,
                default: String::new(),
                map: true,
            }],
            ..external_counter_def()
        };
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let reg = ColumnRegistry::new(vec!["user".to_string(), "amount".to_string()]);
        let mut engine = ReducerEngine::with_runtime(def, storage, &reg, Box::new(runtime));

        engine.process_block(1, &[event("alice", 1.0)]).unwrap();
        engine.process_block(2, &[event("alice", 2.0)]).unwrap();
        engine.rollback(1);
        let output = engine.process_block(2, &[event("alice", 3.0)]).unwrap();
        assert_eq!(befores(&output), [Some(1.0)]);
    }

    /// SET_X as a host callback: the Map field of group `g` through
    /// `with_host_map`. On 13 it emits for a row that the group lacks, which
    /// fails the block after the set.
    #[cfg(feature = "lua")]
    fn host_set_x(groups: &mut [crate::reducer_runtime::external::HostGroup]) {
        use crate::reducer_runtime::external::with_host_map;

        for (g, group) in groups.iter_mut().enumerate() {
            for (index, row) in group.rows.iter().enumerate() {
                let amount = row.get("amount").cloned().unwrap();
                let before = with_host_map(g as u32, 0, |mut book| {
                    let before = book.get("x").cloned();
                    if amount == Value::Float64(0.0) {
                        book.remove("x");
                    } else {
                        book.set("x", amount.clone());
                    }
                    before
                })
                .unwrap();

                let mut emit = HashMap::from([("amount".to_string(), amount.clone())]);
                if let Some(before) = before {
                    emit.insert("before".to_string(), before);
                }
                let row = if amount == Value::Float64(13.0) { usize::MAX } else { index };
                group.emits.push((row, emit));
            }
        }
    }

    /// The `before` emits of each block of a run with a fork, a failed block
    /// and a reload from a checkpoint.
    #[cfg(feature = "lua")]
    fn book_run(engine: &mut ReducerEngine, storage: &MemoryBackend) -> Vec<Vec<Option<f64>>> {
        let mut out = Vec::new();
        let mut block = |engine: &mut ReducerEngine, number, rows: &[Row]| {
            out.push(befores(&engine.process_block(number, rows).unwrap()));
        };

        block(engine, 1, &[event("alice", 1.0), event("bob", 5.0)]);
        block(engine, 2, &[event("alice", 2.0)]);
        engine.rollback(1);
        assert!(engine.process_block(2, &[event("bob", 6.0), event("alice", 13.0)]).is_err());
        engine.rollback(1);
        block(engine, 2, &[event("alice", 0.0)]);
        block(engine, 3, &[event("alice", 7.0), event("bob", 6.0)]);

        let mut batch = StorageWriteBatch::new();
        engine.advance_finality(2);
        engine.write_checkpoint(2, &mut batch);
        storage.commit(&batch).unwrap();
        engine.reload();
        block(engine, 3, &[event("alice", 4.0), event("bob", 8.0)]);
        out
    }

    /// A host body reads and writes the Map field of its group and gets the
    /// batches of the same body in Lua: across a fork, a block that fails
    /// after its sets, and a reload.
    #[cfg(feature = "lua")]
    #[test]
    fn external_map_entries_match_lua() {
        use crate::reducer_runtime::external::install_test_context;

        let _guard = install_test_context(host_set_x);
        let def = ReducerDef {
            state: vec![StateField {
                name: "book".to_string(),
                column_type: ColumnType::Float64,
                default: String::new(),
                map: true,
            }],
            ..external_counter_def()
        };
        let reg = ColumnRegistry::new(vec!["user".to_string(), "amount".to_string()]);
        let storage = Arc::new(MemoryBackend::new());
        let mut external = ReducerEngine::new(def, storage.clone(), &reg, &[]).unwrap();
        let host = book_run(&mut external, &storage);

        let storage = Arc::new(MemoryBackend::new());
        let mut lua = lua_book(SET_X, ColumnType::Float64, storage.clone());
        let lua = book_run(&mut lua, &storage);

        let expected = [
            vec![None, None],
            vec![Some(1.0)],
            vec![Some(1.0)],
            vec![None, Some(5.0)],
            vec![None, Some(5.0)],
        ];
        assert_eq!(lua, expected);
        assert_eq!(host, expected);
    }

    /// A Map call outside a host callback fails, and does not panic.
    #[test]
    fn host_map_needs_a_callback() {
        use crate::reducer_runtime::external::with_host_map;

        let err = with_host_map(0, 0, |book| book.get("x").cloned()).unwrap_err();
        assert!(err.contains("outside a reducer callback"), "{err}");
    }
}
