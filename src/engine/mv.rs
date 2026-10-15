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

/// The groups that one block (or rollback) touched, each with its aggregation
/// values from before the change. The values of all groups share one Vec,
/// `agg_count` per group in `agg_index` order, so a snapshot allocates nothing.
#[derive(Default)]
struct Touched {
    /// Offset of the group's values in `prev`; None if the group did not exist.
    groups: FxHashMap<GroupKey, Option<usize>>,
    prev: Vec<Value>,
    /// Record only whether each group existed, for a caller with no use for
    /// change records. A value reads all unfinalized blocks of its group.
    existence_only: bool,
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
    /// Storage backend for persisting finalized MV state.
    storage: Arc<dyn StorageBackend>,
    /// Sliding window configuration (None for tumbling/non-windowed MVs).
    sliding_window: Option<SlidingWindowDef>,
    /// Block number → max timestamp (ms) seen in that block.
    /// Only populated when sliding_window is Some.
    block_times: BTreeMap<BlockNumber, i64>,
    /// The maximum timestamp seen across all blocks (the "watermark").
    current_watermark: i64,
    /// Group keys removed since the last checkpoint (for storage cleanup).
    removed_groups: Vec<GroupKey>,
    /// Sliding windows only: groups touched since the last
    /// `advance_finality`, whose full per-block state must be re-persisted.
    /// Unchanged groups already have their state on disk. Non-sliding MVs
    /// persist only finalized state, which changes when a group's blocks are
    /// finalized, not when it is touched: see `finalize_blocks_up_to`.
    dirty_groups: FxHashSet<GroupKey>,
    /// Group keys whose stored state changed since the last checkpoint, which
    /// `write_checkpoint` persists. Under backfill deferral keys wait here for
    /// several finalizations. See docs/backfill-mode-design.md.
    pending_durable: FxHashSet<GroupKey>,
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

        if let Ok(group_keys) = storage.list_mv_group_keys(&def.name) {
            for gk_bytes in group_keys {
                if let Ok(Some(state_bytes)) = storage.get_mv_state(&def.name, &gk_bytes) {
                    if let Some(aggs) = deserialize_mv_group(&state_bytes, &agg_funcs) {
                        let group_key = storage::decode_group_key(&gk_bytes);
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
            storage,
            sliding_window,
            block_times,
            current_watermark,
            removed_groups: Vec::new(),
            dirty_groups: FxHashSet::default(),
            pending_durable: FxHashSet::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn source(&self) -> &str {
        &self.def.source
    }

    /// Whether this MV uses a sliding window. Sliding-window MVs are excluded
    /// from backfill persist-deferral (block_times + per-block agg data form an
    /// atomic unit; deferring them would corrupt window replay — see
    /// docs/backfill-mode-design.md).
    pub fn is_sliding(&self) -> bool {
        self.sliding_window.is_some()
    }

    /// Process a batch of rows from a single block.
    /// Returns change records for new/updated groups.
    pub fn process_block(&mut self, block: BlockNumber, rows: &[RowMap]) -> Vec<ChangeRecord> {
        let mut touched = Touched::default();
        self.apply_block(block, rows, &mut touched);
        self.emit_changes(touched)
    }

    /// `process_block` with no change records, for replay, which discards
    /// them. Replay after a deferred tail leaves every replayed block
    /// unfinalized, so reading the values of a group touched in each block
    /// would cost quadratic time in the tail length.
    pub fn replay_block(&mut self, block: BlockNumber, rows: &[RowMap]) {
        let mut touched = Touched {
            existence_only: true,
            ..Touched::default()
        };
        self.apply_block(block, rows, &mut touched);
        self.drop_empty_groups(touched);
    }

    #[inline]
    fn apply_block(&mut self, block: BlockNumber, rows: &[RowMap], touched: &mut Touched) {
        // Sliding window replay protection: skip blocks already in restored state
        if self.sliding_window.is_some() && self.block_times.contains_key(&block) {
            return;
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

        for row in rows {
            let group_key = self.compute_group_key(row);

            // Snapshot prev output before first mutation of this group
            self.snapshot(touched, &group_key);

            // Ensure group exists
            if !self.groups.contains_key(&group_key) {
                self.groups.insert(group_key.clone(), self.create_agg_vec());
            }
            let aggs = self.groups.get_mut(&group_key).unwrap();

            // Feed values to each aggregation using pre-computed agg info
            for feed in &self.agg_feeds {
                let value = match feed.source_col.as_deref() {
                    Some(col) => row.get(col).unwrap_or(&Value::Null),
                    None => &Value::UInt64(1),
                };
                aggs[feed.agg_index].add_block(block, std::slice::from_ref(value));
            }

            // Track block -> group key mapping for rollback
            self.block_groups
                .entry(block)
                .or_default()
                .insert(group_key);
        }

        // Expire old blocks for sliding windows
        if self.sliding_window.is_some() {
            self.expire_old_blocks(touched);
        }
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

        // Collect all group keys affected by rolled-back blocks (consume by
        // value), with their output before the rollback.
        let mut touched = Touched::default();
        for (_block, keys) in rolled_back {
            for key in keys {
                self.snapshot(&mut touched, &key);
            }
        }

        // Batch-remove blocks from aggregations: one split_off per group key
        for key in touched.groups.keys() {
            if let Some(aggs) = self.groups.get_mut(key) {
                for agg in aggs.iter_mut() {
                    agg.remove_blocks_after(fork_point);
                }
            }
        }

        // Emit changes (updates or deletes)
        self.emit_changes(touched)
    }

    /// Merge the blocks up to `block` into finalized state. The groups whose
    /// stored state changed wait in `pending_durable` for `write_checkpoint`.
    /// A sliding window stores its blocks final or not, so it queues the groups
    /// touched since the last call instead.
    pub fn advance_finality(&mut self, block: BlockNumber) {
        let changed = if self.sliding_window.is_some() {
            std::mem::take(&mut self.dirty_groups)
        } else {
            self.finalize_blocks_up_to(block)
        };
        self.pending_durable.extend(changed);
    }

    /// Write the stored state of every group in `pending_durable`, and delete
    /// the groups that emptied.
    pub fn write_checkpoint(&mut self, batch: &mut StorageWriteBatch) {
        let is_sliding = self.sliding_window.is_some();

        for group_key in std::mem::take(&mut self.pending_durable) {
            let aggs = match self.groups.get(&group_key) {
                Some(a) => a,
                None => continue, // removed meanwhile (handled below)
            };
            let gk_bytes = storage::encode_group_key(&group_key);
            let state_bytes = if is_sliding {
                serialize_mv_group_full(aggs)
            } else {
                serialize_mv_group(aggs)
            };
            batch.put_mv_state(&self.def.name, gk_bytes, state_bytes);
        }

        // Persist block_times for sliding windows
        if is_sliding {
            let bt_bytes = rmp_serde::to_vec(&self.block_times)
                .expect("block_times serialization should not fail");
            batch.put_meta(&format!("mv_block_times:{}", self.def.name), &bt_bytes);
        }

        // Delete stale groups from storage. Membership-wins reconcile: only
        // delete keys NOT currently present — a key removed then re-added
        // between checkpoints was written above and must NOT also be deleted.
        for key in std::mem::take(&mut self.removed_groups) {
            if !self.groups.contains_key(&key) {
                let gk_bytes = storage::encode_group_key(&key);
                batch.delete_mv_state(&self.def.name, &gk_bytes);
            }
        }
    }

    /// Merge the per-block data of blocks up to `block` into finalized state
    /// and stop tracking those blocks for rollback. Returns the groups whose
    /// finalized state changed. With finality lag, a group gets here several
    /// finalizes after it was last touched, so the touched set is not enough.
    fn finalize_blocks_up_to(&mut self, block: BlockNumber) -> FxHashSet<GroupKey> {
        let remaining = self.block_groups.split_off(&(block + 1));
        let finalized = std::mem::replace(&mut self.block_groups, remaining);

        let mut changed = FxHashSet::default();
        for keys in finalized.into_values() {
            changed.extend(keys);
        }

        for key in &changed {
            if let Some(aggs) = self.groups.get_mut(key) {
                for agg in aggs.iter_mut() {
                    agg.finalize_up_to(block);
                }
            }
        }

        changed
    }

    /// Remove blocks whose timestamps have fallen outside the sliding window,
    /// and add the groups they held to `touched`.
    fn expire_old_blocks(&mut self, touched: &mut Touched) {
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

        for &block in &expired_blocks {
            if let Some(keys) = self.block_groups.remove(&block) {
                for key in keys {
                    // Snapshot prev output before first mutation of this group by expiry.
                    // If already touched by row processing, keep that snapshot.
                    self.snapshot(touched, &key);

                    // Remove block's contribution from all aggs for this group
                    if let Some(aggs) = self.groups.get_mut(&key) {
                        for agg in aggs.iter_mut() {
                            agg.remove_block(block);
                        }
                    }
                }
            }

            self.block_times.remove(&block);
        }
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

    /// Record the aggregation values of a group before its first change.
    fn snapshot(&self, touched: &mut Touched, group_key: &GroupKey) {
        if touched.groups.contains_key(group_key) {
            return;
        }

        let at = self.groups.get(group_key).map(|aggs| {
            let at = touched.prev.len();
            if !touched.existence_only {
                touched.prev.extend(aggs.iter().map(|a| a.current_value()));
            }
            at
        });
        touched.groups.insert(group_key.clone(), at);
    }

    /// The output row of a group: its key columns plus `agg_values`, which
    /// are moved out.
    fn build_output(
        &self,
        group_key: &GroupKey,
        agg_values: &mut [Value],
    ) -> HashMap<String, Value> {
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
                    let value = std::mem::replace(&mut agg_values[*agg_index], Value::Null);
                    output.insert(output_name.clone(), value);
                }
            }
        }

        output
    }

    /// Emit one change per touched group against its snapshot, and mark a
    /// sliding-window group dirty for the next checkpoint. Output rows are
    /// built only for the groups whose values changed.
    fn emit_changes(&mut self, touched: Touched) -> Vec<ChangeRecord> {
        debug_assert!(!touched.existence_only);
        let Touched {
            groups: touched,
            mut prev,
            ..
        } = touched;
        let mut changes = Vec::with_capacity(touched.len());
        let mut current: Vec<Value> = Vec::with_capacity(self.agg_count);

        for (key, at) in touched {
            current.clear();
            // False once the group has no data left (all aggs empty) or is gone.
            let has_data = match self.groups.get(&key) {
                Some(aggs) if aggs.iter().any(|a| a.has_data()) => {
                    current.extend(aggs.iter().map(|a| a.current_value()));
                    true
                }
                _ => false,
            };
            let prev_values = at.map(|at| &mut prev[at..at + self.agg_count]);

            match (prev_values, has_data) {
                (None, true) => {
                    // New group -> Insert
                    let values = self.build_output(&key, &mut current);
                    changes.push(self.change_record(ChangeOp::Insert, &key, values, None));
                }
                (Some(prev_values), true) => {
                    // Existing group updated -> Update
                    if current[..] != prev_values[..] {
                        let values = self.build_output(&key, &mut current);
                        let prev_values = self.build_output(&key, prev_values);
                        changes.push(self.change_record(
                            ChangeOp::Update,
                            &key,
                            values,
                            Some(prev_values),
                        ));
                    }
                }
                (Some(prev_values), false) => {
                    // Group became empty after rollback/expiry -> Delete
                    let prev_values = self.build_output(&key, prev_values);
                    changes.push(self.change_record(
                        ChangeOp::Delete,
                        &key,
                        prev_values.clone(),
                        Some(prev_values),
                    ));

                    // Clean up empty group and track for storage deletion
                    self.groups.remove(&key);
                    self.removed_groups.push(key.clone());
                }
                (None, false) => {
                    // Never emitted and already empty, e.g. its rows expired on
                    // arrival: nothing to emit, but a lingering empty group would
                    // turn the next row's Insert into an Update and get persisted.
                    // It was never stored, so there is nothing to delete.
                    self.groups.remove(&key);
                }
            }

            if self.sliding_window.is_some() {
                self.dirty_groups.insert(key);
            }
        }

        changes
    }

    /// The state changes of `emit_changes`, without its change records.
    fn drop_empty_groups(&mut self, touched: Touched) {
        for (key, at) in touched.groups {
            let has_data = self
                .groups
                .get(&key)
                .is_some_and(|aggs| aggs.iter().any(|a| a.has_data()));

            if !has_data {
                self.groups.remove(&key);
                if at.is_some() {
                    self.removed_groups.push(key.clone());
                }
            }

            if self.sliding_window.is_some() {
                self.dirty_groups.insert(key);
            }
        }
    }

    fn change_record(
        &self,
        operation: ChangeOp,
        group_key: &GroupKey,
        values: HashMap<String, Value>,
        prev_values: Option<HashMap<String, Value>>,
    ) -> ChangeRecord {
        ChangeRecord {
            table: self.def.name.clone(),
            operation,
            key: self.build_change_key(group_key),
            values,
            prev_values,
        }
    }

    fn build_change_key(&self, group_key: &GroupKey) -> HashMap<String, Value> {
        let mut change_key = HashMap::with_capacity(group_key.len());
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

// MV group state binary layout:
//   num_aggs: u8 (hence MAX_MV_AGGREGATES)
//   per agg: len: u32 LE, then `len` bytes (per-agg rmp blob)
//   prev flag: u8, written as 0. Older builds could write 1, then
//     len: u32 LE and an rmp map of the last output; the reader checks and drops it.
// Per-agg blobs use the aggregates' rmp codecs (to_bytes / write_finalized /
// from_bytes).

fn write_mv_group(
    buf: &mut Vec<u8>,
    write_agg: impl Fn(&dyn AggregationFunc, &mut Vec<u8>),
    aggs: &[Box<dyn AggregationFunc>],
) {
    let count = u8::try_from(aggs.len()).expect("schema validation bounds the aggregate count");
    buf.push(count);
    for a in aggs {
        // The blob goes straight into `buf`; its length is patched in after.
        let len_at = buf.len();
        buf.extend_from_slice(&[0; 4]);
        write_agg(a.as_ref(), buf);

        let len = (buf.len() - len_at - 4) as u32;
        buf[len_at..len_at + 4].copy_from_slice(&len.to_le_bytes());
    }
    buf.push(0);
}

/// Serialize an MV group's full state (finalized + per-block) for sliding window persistence.
fn serialize_mv_group_full(aggs: &[Box<dyn AggregationFunc>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + aggs.len() * 24);
    write_mv_group(
        &mut buf,
        |a, out| out.extend_from_slice(&a.to_bytes()),
        aggs,
    );
    buf
}

/// Serialize an MV group's finalized aggregation state for persistence.
fn serialize_mv_group(aggs: &[Box<dyn AggregationFunc>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + aggs.len() * 24);
    write_mv_group(&mut buf, |a, out| a.write_finalized(out), aggs);
    buf
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Option<usize> {
    let end = pos.checked_add(4)?;
    let v = u32::from_le_bytes(bytes.get(*pos..end)?.try_into().ok()?) as usize;
    *pos = end;
    Some(v)
}

/// Deserialize an MV group's state from bytes (see `write_mv_group` layout).
fn deserialize_mv_group(
    bytes: &[u8],
    agg_funcs: &[AggFunc],
) -> Option<Vec<Box<dyn AggregationFunc>>> {
    let mut pos = 0usize;
    let n = *bytes.get(pos)? as usize;
    pos += 1;
    if n != agg_funcs.len() {
        return None;
    }
    let mut aggs: Vec<Box<dyn AggregationFunc>> = Vec::with_capacity(n);
    for func in agg_funcs {
        let len = read_u32(bytes, &mut pos)?;
        let end = pos.checked_add(len)?;
        let blob = bytes.get(pos..end)?;
        pos = end;
        aggs.push(restore_agg(func, blob));
    }
    match *bytes.get(pos)? {
        0 => {}
        1 => {
            pos += 1;
            let len = read_u32(bytes, &mut pos)?;
            let end = pos.checked_add(len)?;
            let pb = bytes.get(pos..end)?;
            rmp_serde::from_slice::<HashMap<String, Value>>(pb).ok()?;
        }
        _ => return None,
    }
    Some(aggs)
}

#[cfg(test)]
#[path = "mv_core_tests.rs"]
mod core_tests;

#[cfg(test)]
#[path = "mv_sliding_tests.rs"]
mod sliding_tests;
