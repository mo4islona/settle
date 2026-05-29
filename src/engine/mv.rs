use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::schema::ast::{AggFunc, MVDef, SelectExpr, SelectItem, SlidingWindowDef};
use crate::storage::{self, StorageBackend, StorageWriteBatch};
use crate::types::{BlockNumber, ColumnType, ChangeOp, ChangeRecord, GroupKey, RowMap, Value};

use super::aggregation::{AggregationFunc, create_agg, restore_agg, to_start_of_interval};

/// Describes one output column of the MV.
#[derive(Debug)]
enum OutputColumn {
    /// A pass-through GROUP BY column (value comes from the group key).
    GroupBy {
        source_col: String,
        output_name: String,
    },
    /// A time-window GROUP BY column.
    Window {
        source_col: String,
        interval_seconds: u64,
        output_name: String,
    },
    /// An aggregation column.
    Agg {
        source_col: Option<String>,
        agg_index: usize,
        output_name: String,
    },
}

/// Pre-computed info for feeding a single aggregation from a source row.
struct AggFeedInfo {
    source_col: Option<String>,
    agg_index: usize,
    column_type: ColumnType,
}

/// Pre-computed group key extraction — avoids per-row pattern matching on OutputColumn.
enum GroupKeyExtractor {
    Column(String),
    Window(String, u64),
}

/// Manages a single materialized view: GROUP BY routing, aggregation, rollback, changes.
pub struct MVEngine {
    def: MVDef,
    /// The output column descriptors (in SELECT order).
    output_columns: Vec<OutputColumn>,
    /// The AggFunc types in SELECT order (for deserialization).
    agg_funcs: Vec<AggFunc>,
    /// Number of aggregation functions per group.
    agg_count: usize,
    /// Pre-computed list of agg columns for fast row feeding (avoids per-row pattern matching).
    agg_feeds: Vec<AggFeedInfo>,
    /// Pre-computed group key extractors (avoids per-row pattern matching on OutputColumn).
    group_key_extractors: Vec<GroupKeyExtractor>,
    /// group_key -> aggregation state (one AggregationFunc per agg column).
    groups: FxHashMap<GroupKey, Vec<Box<dyn AggregationFunc>>>,
    /// Tracks which blocks have been ingested (for rollback).
    /// block -> set of group keys touched. BTreeMap for O(log N) range queries.
    block_groups: BTreeMap<BlockNumber, FxHashSet<GroupKey>>,
    /// Snapshot of previous output values per group key, for change computation.
    prev_output: FxHashMap<GroupKey, HashMap<String, Value>>,
    /// Storage backend for persisting finalized MV state.
    storage: Arc<dyn StorageBackend>,
    /// Sliding window configuration (None for tumbling/non-windowed MVs).
    sliding_window: Option<SlidingWindowDef>,
    /// Block number → max timestamp (ms) seen in that block.
    /// Only populated when sliding_window is Some.
    block_times: BTreeMap<BlockNumber, i64>,
    /// The maximum timestamp seen across all blocks (the "watermark").
    current_watermark: i64,
    /// Group keys removed since last finalize (for storage cleanup).
    removed_groups: Vec<GroupKey>,
    /// Groups touched since the last successful `finalize`. Unchanged groups
    /// already have their state on disk from the previous finalize; re-
    /// persisting them is wasted work that dominated the bench (see
    /// BENCHMARKS.md "Optimization Roadmap Baseline 2026-05-29"). Populated
    /// in `process_block`, `rollback`, drained by `finalize`.
    dirty_groups: FxHashSet<GroupKey>,
}

impl MVEngine {
    pub fn new(
        def: MVDef,
        storage: Arc<dyn StorageBackend>,
        source_column_types: &HashMap<String, ColumnType>,
    ) -> Self {
        let mut output_columns = Vec::new();
        let mut agg_funcs = Vec::new();
        let mut agg_index = 0usize;

        for item in &def.select {
            let output_name = resolve_output_name(item);
            match &item.expr {
                SelectExpr::Column(col) => {
                    output_columns.push(OutputColumn::GroupBy {
                        source_col: col.clone(),
                        output_name,
                    });
                }
                SelectExpr::WindowFunc {
                    column,
                    interval_seconds,
                } => {
                    output_columns.push(OutputColumn::Window {
                        source_col: column.clone(),
                        interval_seconds: *interval_seconds,
                        output_name,
                    });
                }
                SelectExpr::Agg(func, source_col) => {
                    output_columns.push(OutputColumn::Agg {
                        source_col: source_col.clone(),
                        agg_index,
                        output_name,
                    });
                    agg_funcs.push(func.clone());
                    agg_index += 1;
                }
            }
        }

        let agg_count = agg_index;

        // Pre-compute agg feed info for fast row processing
        let agg_feeds: Vec<AggFeedInfo> = output_columns
            .iter()
            .filter_map(|col| {
                if let OutputColumn::Agg {
                    source_col,
                    agg_index,
                    ..
                } = col
                {
                    let ct = source_col
                        .as_ref()
                        .and_then(|c| source_column_types.get(c))
                        .cloned()
                        .unwrap_or(ColumnType::Float64);
                    Some(AggFeedInfo {
                        source_col: source_col.clone(),
                        agg_index: *agg_index,
                        column_type: ct,
                    })
                } else {
                    None
                }
            })
            .collect();

        // Pre-compute group key extractors for fast group key computation
        let group_key_extractors: Vec<GroupKeyExtractor> = output_columns
            .iter()
            .filter_map(|col| match col {
                OutputColumn::GroupBy { source_col, .. } => {
                    Some(GroupKeyExtractor::Column(source_col.clone()))
                }
                OutputColumn::Window {
                    source_col,
                    interval_seconds,
                    ..
                } => Some(GroupKeyExtractor::Window(
                    source_col.clone(),
                    *interval_seconds,
                )),
                OutputColumn::Agg { .. } => None,
            })
            .collect();

        // Restore finalized MV state from storage
        let mut groups: FxHashMap<GroupKey, Vec<Box<dyn AggregationFunc>>> = FxHashMap::default();
        let mut prev_output: FxHashMap<GroupKey, HashMap<String, Value>> = FxHashMap::default();

        if let Ok(group_keys) = storage.list_mv_group_keys(&def.name) {
            for gk_bytes in group_keys {
                if let Ok(Some(state_bytes)) = storage.get_mv_state(&def.name, &gk_bytes) {
                    if let Some((aggs, prev)) = deserialize_mv_group(&state_bytes, &agg_funcs) {
                        let group_key = storage::decode_group_key(&gk_bytes);
                        if let Some(prev) = prev {
                            prev_output.insert(group_key.clone(), prev);
                        }
                        groups.insert(group_key, aggs);
                    }
                }
            }
        }

        let sliding_window = def.sliding_window.clone();
        let mut block_times: BTreeMap<BlockNumber, i64> = BTreeMap::new();
        let mut block_groups: BTreeMap<BlockNumber, FxHashSet<GroupKey>> = BTreeMap::new();

        // Restore sliding window metadata from storage
        if sliding_window.is_some() {
            let meta_key = format!("mv_block_times:{}", def.name);
            if let Ok(Some(bt_bytes)) = storage.get_meta(&meta_key) {
                if let Ok(bt) = rmp_serde::from_slice::<BTreeMap<BlockNumber, i64>>(&bt_bytes) {
                    block_times = bt;
                }
            }

            // Rebuild block_groups from restored agg state (union of all aggs'
            // block numbers, since different agg types may track different blocks)
            for (group_key, aggs) in &groups {
                for agg in aggs {
                    for block in agg.block_numbers() {
                        block_groups
                            .entry(block)
                            .or_default()
                            .insert(group_key.clone());
                    }
                }
            }
        }

        let current_watermark = block_times.values().copied().max().unwrap_or(0);

        MVEngine {
            def,
            output_columns,
            agg_funcs,
            agg_count,
            agg_feeds,
            group_key_extractors,
            groups,
            block_groups,
            prev_output,
            storage,
            sliding_window,
            block_times,
            current_watermark,
            removed_groups: Vec::new(),
            dirty_groups: FxHashSet::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn source(&self) -> &str {
        &self.def.source
    }

    /// Process a batch of rows from a single block.
    /// Returns change records for new/updated groups.
    pub fn process_block(&mut self, block: BlockNumber, rows: &[RowMap]) -> Vec<ChangeRecord> {
        // Sliding window replay protection: skip blocks already in restored state
        if self.sliding_window.is_some() && self.block_times.contains_key(&block) {
            return Vec::new();
        }

        // Track block timestamp for sliding windows
        if let Some(ref sw) = self.sliding_window {
            let block_max_ts = rows
                .iter()
                .filter_map(|r| r.get(&sw.time_column).and_then(|v| v.as_i64()))
                .max()
                // Fallback: if no rows have a valid timestamp, use current watermark
                // so the block still participates in expiry rather than leaking.
                .unwrap_or(self.current_watermark);
            self.block_times.insert(block, block_max_ts);
            if block_max_ts > self.current_watermark {
                self.current_watermark = block_max_ts;
            }
        }

        // Snapshot current output for touched groups before mutation
        let mut touched_keys: FxHashSet<GroupKey> = FxHashSet::default();

        for row in rows {
            let group_key = self.compute_group_key(row);

            // Snapshot prev output before first mutation of this group
            if !touched_keys.contains(&group_key) {
                let prev = self.compute_output(&group_key);
                if let Some(prev) = prev {
                    self.prev_output.insert(group_key.clone(), prev);
                }
                touched_keys.insert(group_key.clone());
            }

            // Ensure group exists
            if !self.groups.contains_key(&group_key) {
                self.groups.insert(group_key.clone(), self.create_agg_vec());
            }
            let aggs = self.groups.get_mut(&group_key).unwrap();

            // Feed values to each aggregation using pre-computed agg info
            for feed in &self.agg_feeds {
                let value = match feed.source_col.as_deref() {
                    Some(col) => row.get(col).cloned().unwrap_or(Value::Null),
                    None => Value::UInt64(1),
                };
                aggs[feed.agg_index].add_block(block, std::slice::from_ref(&value));
            }

            // Track block -> group key mapping for rollback
            self.block_groups
                .entry(block)
                .or_default()
                .insert(group_key);
        }

        // Expire old blocks for sliding windows
        if self.sliding_window.is_some() {
            let expired_keys = self.expire_old_blocks();
            touched_keys.extend(expired_keys);
        }

        // Emit changes for all touched groups
        let changes = self.emit_changes(&touched_keys);
        // Mark touched groups as dirty so the next finalize knows what to
        // persist. Consume `touched_keys` here — its only other use was the
        // borrow passed to `emit_changes` above.
        self.dirty_groups.extend(touched_keys);
        changes
    }

    /// Roll back all blocks after fork_point.
    /// Returns compensating change records.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Vec<ChangeRecord> {
        // Guard: fork_point + 1 would overflow u64::MAX to 0, causing split_off(&0)
        // to remove the entire map. MAX is a valid no-op: nothing exists after it.
        if fork_point == BlockNumber::MAX {
            return Vec::new();
        }
        // Use BTreeMap range to efficiently find blocks > fork_point
        let rolled_back = self.block_groups.split_off(&(fork_point + 1));

        if rolled_back.is_empty() {
            return Vec::new();
        }

        // Clean up sliding window state for rolled-back blocks
        if self.sliding_window.is_some() {
            drop(self.block_times.split_off(&(fork_point + 1)));
            self.current_watermark = self.block_times.values().copied().max().unwrap_or(0);
        }

        // Collect all group keys affected by rolled-back blocks (consume by value)
        let mut touched_keys: FxHashSet<GroupKey> = FxHashSet::default();
        for (_block, keys) in rolled_back {
            for key in keys {
                touched_keys.insert(key);
            }
        }

        // Snapshot prev output before mutation
        for key in &touched_keys {
            let prev = self.compute_output(key);
            if let Some(prev) = prev {
                self.prev_output.insert(key.clone(), prev);
            }
        }

        // Batch-remove blocks from aggregations: one split_off per group key
        for key in &touched_keys {
            if let Some(aggs) = self.groups.get_mut(key) {
                for agg in aggs.iter_mut() {
                    agg.remove_blocks_after(fork_point);
                }
            }
        }

        // Emit changes (updates or deletes)
        let changes = self.emit_changes(&touched_keys);
        self.dirty_groups.extend(touched_keys);
        changes
    }

    /// Finalize all blocks up to and including the given block.
    /// Persists finalized aggregation state to the batch for atomic commit.
    pub fn finalize(&mut self, block: BlockNumber, batch: &mut StorageWriteBatch) {
        let is_sliding = self.sliding_window.is_some();

        // Take dirty_groups so we can borrow self.groups mutably below. After
        // finalize completes, dirty_groups is empty (default) — exactly what we
        // want; the take is the "clear".
        let dirty = std::mem::take(&mut self.dirty_groups);

        if !is_sliding {
            // Standard path: merge per-block data into finalized state. Only
            // dirty groups can have un-merged per-block data — untouched groups
            // were already finalized last time.
            for key in &dirty {
                if let Some(aggs) = self.groups.get_mut(key) {
                    for agg in aggs.iter_mut() {
                        agg.finalize_up_to(block);
                    }
                }
            }
        }
        // For sliding windows: do NOT call finalize_up_to — keep per-block data

        // Persist state only for dirty groups. Unchanged group state is already
        // on disk from the previous finalize.
        for group_key in &dirty {
            let aggs = match self.groups.get(group_key) {
                Some(a) => a,
                None => continue, // removed since being marked dirty
            };
            let gk_bytes = storage::encode_group_key(group_key);
            let prev = self.prev_output.get(group_key);
            let state_bytes = if is_sliding {
                serialize_mv_group_full(aggs, prev)
            } else {
                serialize_mv_group(aggs, prev)
            };
            batch.put_mv_state(&self.def.name, &gk_bytes, &state_bytes);
        }

        // Persist block_times for sliding windows
        if is_sliding {
            let bt_bytes = rmp_serde::to_vec(&self.block_times)
                .expect("block_times serialization should not fail");
            batch.put_meta(&format!("mv_block_times:{}", self.def.name), &bt_bytes);
        }

        // Delete stale groups from storage (expired/removed since last finalize)
        for key in self.removed_groups.drain(..) {
            let gk_bytes = storage::encode_group_key(&key);
            batch.delete_mv_state(&self.def.name, &gk_bytes);
        }

        if !is_sliding {
            // Remove finalized blocks from tracking using split_off
            let remaining = self.block_groups.split_off(&(block + 1));
            self.block_groups = remaining;
        }
        // For sliding windows: block_groups pruning is handled by expire_old_blocks
    }

    /// Remove blocks whose timestamps have fallen outside the sliding window.
    /// Returns the set of group keys affected by expiry.
    fn expire_old_blocks(&mut self) -> FxHashSet<GroupKey> {
        let sw = self.sliding_window.as_ref().unwrap();
        let window_ms = (sw.interval_seconds as i64).saturating_mul(1000);
        let cutoff = self.current_watermark - window_ms;

        // Find all blocks with timestamp < cutoff (strict less-than: boundary is inclusive).
        // Scan is bounded by window size since expired blocks are removed each round.
        let expired_blocks: Vec<BlockNumber> = self
            .block_times
            .iter()
            .filter(|(_, ts)| **ts < cutoff)
            .map(|(block, _)| *block)
            .collect();

        if expired_blocks.is_empty() {
            return FxHashSet::default();
        }

        let mut expired_keys: FxHashSet<GroupKey> = FxHashSet::default();

        for &block in &expired_blocks {
            if let Some(keys) = self.block_groups.remove(&block) {
                for key in keys {
                    // Snapshot prev output before first mutation of this group by expiry.
                    // If already in prev_output (touched by row processing), skip.
                    if !self.prev_output.contains_key(&key) {
                        if let Some(prev) = self.compute_output(&key) {
                            self.prev_output.insert(key.clone(), prev);
                        }
                    }

                    // Remove block's contribution from all aggs for this group
                    if let Some(aggs) = self.groups.get_mut(&key) {
                        for agg in aggs.iter_mut() {
                            agg.remove_block(block);
                        }
                    }

                    expired_keys.insert(key);
                }
            }

            self.block_times.remove(&block);
        }

        expired_keys
    }

    fn compute_group_key(&self, row: &RowMap) -> GroupKey {
        let mut key = GroupKey::new();
        for ext in &self.group_key_extractors {
            match ext {
                GroupKeyExtractor::Column(source_col) => {
                    let v = row.get(source_col.as_str()).cloned().unwrap_or(Value::Null);
                    key.push(v);
                }
                GroupKeyExtractor::Window(source_col, interval_seconds) => {
                    let ts = row
                        .get(source_col.as_str())
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let window_start = to_start_of_interval(ts, *interval_seconds);
                    key.push(Value::DateTime(window_start));
                }
            }
        }
        key
    }

    fn compute_output(&self, group_key: &GroupKey) -> Option<HashMap<String, Value>> {
        let aggs = self.groups.get(group_key)?;
        let mut output = HashMap::with_capacity(self.output_columns.len());

        let mut key_idx = 0;
        for col in &self.output_columns {
            match col {
                OutputColumn::GroupBy { output_name, .. }
                | OutputColumn::Window { output_name, .. } => {
                    output.insert(output_name.clone(), group_key[key_idx].clone());
                    key_idx += 1;
                }
                OutputColumn::Agg {
                    agg_index,
                    output_name,
                    ..
                } => {
                    output.insert(output_name.clone(), aggs[*agg_index].current_value());
                }
            }
        }

        Some(output)
    }

    fn emit_changes(&mut self, touched_keys: &FxHashSet<GroupKey>) -> Vec<ChangeRecord> {
        let mut changes = Vec::new();

        for key in touched_keys {
            let prev = self.prev_output.remove(key);
            let current = self.compute_output(key);

            // Check if group is now empty (all aggs have no data)
            let is_empty = self
                .groups
                .get(key)
                .map(|aggs| aggs.iter().all(|a| !a.has_data()))
                .unwrap_or(true);

            let change_key = self.build_change_key(key);

            match (prev, is_empty) {
                (None, false) => {
                    // New group -> Insert
                    if let Some(values) = current {
                        changes.push(ChangeRecord {
                            table: self.def.name.clone(),
                            operation: ChangeOp::Insert,
                            key: change_key,
                            values,
                            prev_values: None,
                        });
                    }
                }
                (Some(prev_vals), false) => {
                    // Existing group updated -> Update
                    if let Some(values) = current {
                        if values != prev_vals {
                            changes.push(ChangeRecord {
                                table: self.def.name.clone(),
                                operation: ChangeOp::Update,
                                key: change_key,
                                values,
                                prev_values: Some(prev_vals),
                            });
                        }
                    }
                }
                (Some(prev_vals), true) => {
                    // Group became empty after rollback/expiry -> Delete
                    changes.push(ChangeRecord {
                        table: self.def.name.clone(),
                        operation: ChangeOp::Delete,
                        key: change_key,
                        values: prev_vals.clone(),
                        prev_values: Some(prev_vals),
                    });
                    // Clean up empty group and track for storage deletion
                    self.groups.remove(key);
                    self.removed_groups.push(key.clone());
                }
                (None, true) => {
                    // Was never emitted and is empty — no change needed
                }
            }
        }

        changes
    }

    fn build_change_key(&self, group_key: &GroupKey) -> HashMap<String, Value> {
        let mut change_key = HashMap::new();
        let mut key_idx = 0;
        for col in &self.output_columns {
            match col {
                OutputColumn::GroupBy { output_name, .. }
                | OutputColumn::Window { output_name, .. } => {
                    change_key.insert(output_name.clone(), group_key[key_idx].clone());
                    key_idx += 1;
                }
                OutputColumn::Agg { .. } => {}
            }
        }
        change_key
    }

    fn create_agg_vec(&self) -> Vec<Box<dyn AggregationFunc>> {
        let mut aggs = Vec::with_capacity(self.agg_count);
        for (func, feed) in self.agg_funcs.iter().zip(self.agg_feeds.iter()) {
            aggs.push(create_agg(func, &feed.column_type));
        }
        aggs
    }
}

fn resolve_output_name(item: &SelectItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    match &item.expr {
        SelectExpr::Column(col) => col.clone(),
        SelectExpr::Agg(func, col) => {
            let func_name = match func {
                crate::schema::ast::AggFunc::Sum => "sum",
                crate::schema::ast::AggFunc::Count => "count",
                crate::schema::ast::AggFunc::Min => "min",
                crate::schema::ast::AggFunc::Max => "max",
                crate::schema::ast::AggFunc::Avg => "avg",
                crate::schema::ast::AggFunc::First => "first",
                crate::schema::ast::AggFunc::Last => "last",
            };
            match col {
                Some(c) => format!("{func_name}_{c}"),
                None => func_name.to_string(),
            }
        }
        SelectExpr::WindowFunc { column, .. } => column.clone(),
    }
}

/// Serialize an MV group's full state (finalized + per-block) for sliding window persistence.
fn serialize_mv_group_full(
    aggs: &[Box<dyn AggregationFunc>],
    prev_output: Option<&HashMap<String, Value>>,
) -> Vec<u8> {
    let agg_bytes: Vec<Vec<u8>> = aggs.iter().map(|a| a.to_bytes()).collect();
    rmp_serde::to_vec(&(agg_bytes, prev_output))
        .expect("MV group state serialization should not fail")
}

/// Serialize an MV group's aggregation state + prev_output for persistence.
/// Format: MessagePack-encoded (Vec<Vec<u8>>, Option<HashMap<String, Value>>)
fn serialize_mv_group(
    aggs: &[Box<dyn AggregationFunc>],
    prev_output: Option<&HashMap<String, Value>>,
) -> Vec<u8> {
    let agg_bytes: Vec<Vec<u8>> = aggs.iter().map(|a| a.to_finalized_bytes()).collect();
    rmp_serde::to_vec(&(agg_bytes, prev_output))
        .expect("MV group state serialization should not fail")
}

/// Deserialize an MV group's state from bytes.
fn deserialize_mv_group(
    bytes: &[u8],
    agg_funcs: &[AggFunc],
) -> Option<(
    Vec<Box<dyn AggregationFunc>>,
    Option<HashMap<String, Value>>,
)> {
    let (agg_bytes, prev_output): (Vec<Vec<u8>>, Option<HashMap<String, Value>>) =
        rmp_serde::from_slice(bytes).ok()?;
    if agg_bytes.len() != agg_funcs.len() {
        return None;
    }
    let aggs: Vec<Box<dyn AggregationFunc>> = agg_funcs
        .iter()
        .zip(agg_bytes.iter())
        .map(|(func, bytes)| restore_agg(func, bytes))
        .collect();
    Some((aggs, prev_output))
}

#[cfg(test)]
#[path = "mv_core_tests.rs"]
mod core_tests;

#[cfg(test)]
#[path = "mv_sliding_tests.rs"]
mod sliding_tests;
