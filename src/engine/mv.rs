use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::error::{Error, Result};
use crate::schema::ast::{AggFunc, MVDef, SelectExpr, SelectItem, SlidingWindowDef};
use crate::storage::{self, StorageBackend, StorageWriteBatch};
use crate::types::{
    BlockNumber, ChangeOp, ChangeRecord, ChangeSchema, ColumnId, ColumnRegistry, ColumnType,
    GroupKey, Row, RowMap, Value,
};

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
    source_col: Option<SourceColumn>,
    agg_index: usize,
    column_type: ColumnType,
}

/// Pre-computed group key extraction — avoids per-row pattern matching on OutputColumn.
enum GroupKeyExtractor {
    Column(SourceColumn),
    Window(SourceColumn, u64),
}

/// A column that the view reads from its source rows.
pub struct SourceColumn {
    name: String,
    /// Its position in the registry of the last positional rows applied.
    id: Option<ColumnId>,
}

impl SourceColumn {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            id: None,
        }
    }
}

static NULL: Value = Value::Null;

/// A source row of a view: the positional rows of a raw table, or the
/// emits of a reducer. A column that the row lacks reads as Null.
pub trait SourceRow {
    fn column<'a>(&'a self, column: &SourceColumn) -> &'a Value;

    /// The registry of positional rows, which `SourceColumn::id` refers to.
    fn registry(&self) -> Option<&Arc<ColumnRegistry>> {
        None
    }
}

impl SourceRow for Row {
    fn column<'a>(&'a self, column: &SourceColumn) -> &'a Value {
        column.id.map_or(&NULL, |id| &self.values()[id as usize])
    }

    fn registry(&self) -> Option<&Arc<ColumnRegistry>> {
        Some(Row::registry(self))
    }
}

impl SourceRow for RowMap {
    fn column<'a>(&'a self, column: &SourceColumn) -> &'a Value {
        self.get(&column.name).unwrap_or(&NULL)
    }
}

const EMIT_ORDER_KEEP: usize = 8192;

/// The groups that the blocks of an ingest (or a rollback) touched, each with
/// its aggregation values from before the change. The values of all groups
/// share one Vec, `agg_count` per group in `agg_index` order, so a snapshot
/// allocates nothing.
#[derive(Default)]
struct Touched {
    /// Per group: the order of its first touch, and the offset of its values
    /// in `prev` (None if the group did not exist).
    groups: FxHashMap<GroupKey, (usize, Option<usize>)>,
    prev: Vec<Value>,
    /// Record only whether each group existed, for a caller with no use for
    /// change records. A value reads all unfinalized blocks of its group.
    existence_only: bool,
}

/// A block that left a sliding window. Its data stays until the block that
/// expired it is final: a fork below that block brings it back.
struct ExpiredBlock {
    block: BlockNumber,
    ts: i64,
    /// Per group, aggregates that hold only this block.
    groups: Vec<(GroupKey, Vec<Box<dyn AggregationFunc>>)>,
}

/// The max timestamp (ms) of each block in a sliding window, indexed by block
/// and by timestamp, so that expiry finds the blocks below the cutoff without
/// a scan of the window. Block timestamps need not grow with the block number.
#[derive(Default)]
struct BlockTimes {
    by_block: BTreeMap<BlockNumber, i64>,
    by_ts: BTreeSet<(i64, BlockNumber)>,
}

impl BlockTimes {
    fn insert(&mut self, block: BlockNumber, ts: i64) {
        if let Some(old) = self.by_block.insert(block, ts) {
            self.by_ts.remove(&(old, block));
        }
        self.by_ts.insert((ts, block));
    }

    /// Remove and return the blocks from `block` up.
    fn split_off(&mut self, block: BlockNumber) -> BTreeMap<BlockNumber, i64> {
        let removed = self.by_block.split_off(&block);
        for (&block, &ts) in &removed {
            self.by_ts.remove(&(ts, block));
        }
        removed
    }

    /// Remove and return the blocks with a timestamp below `cutoff`, in block
    /// order.
    fn take_older_than(&mut self, cutoff: i64) -> Vec<(BlockNumber, i64)> {
        let mut taken = Vec::new();
        while let Some(&(ts, block)) = self.by_ts.first()
            && ts < cutoff
        {
            self.by_ts.pop_first();
            self.by_block.remove(&block);
            taken.push((block, ts));
        }

        taken.sort_unstable();
        taken
    }

    fn max_ts(&self) -> Option<i64> {
        self.by_ts.last().map(|&(ts, _)| ts)
    }
}

impl Deref for BlockTimes {
    type Target = BTreeMap<BlockNumber, i64>;

    fn deref(&self) -> &Self::Target {
        &self.by_block
    }
}

/// Manages a single materialized view: GROUP BY routing, aggregation, rollback, changes.
pub struct MVEngine {
    def: MVDef,
    change_schema: Arc<ChangeSchema>,
    /// The AggFunc types in SELECT order (for deserialization).
    agg_funcs: Vec<AggFunc>,
    /// Number of aggregation functions per group.
    agg_count: usize,
    /// Pre-computed list of agg columns for fast row feeding (avoids per-row pattern matching).
    agg_feeds: Vec<AggFeedInfo>,
    /// Pre-computed group key extractors (avoids per-row pattern matching on OutputColumn).
    group_key_extractors: Vec<GroupKeyExtractor>,
    /// Sliding windows only: the time column.
    time_column: Option<SourceColumn>,
    /// The registry that the `SourceColumn` ids refer to.
    resolved_for: Option<Arc<ColumnRegistry>>,
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
    block_times: BlockTimes,
    /// The maximum timestamp seen across all blocks (the "watermark").
    current_watermark: i64,
    /// Sliding windows only: the expired blocks, by the block that expired
    /// them.
    expired: BTreeMap<BlockNumber, Vec<ExpiredBlock>>,
    /// Sliding windows only: the blocks that storage lacks. Storage holds
    /// the window as of the last finality advance, so these are the blocks
    /// applied since, mostly above it.
    unstored: BTreeSet<BlockNumber>,
    /// Sliding windows only: the storage writes of the finality advances
    /// since the last checkpoint, in order.
    window_writes: Vec<WindowWrite>,
    /// Group keys removed since the last checkpoint (for storage cleanup).
    removed_groups: Vec<GroupKey>,
    /// Group keys whose stored state changed since the last checkpoint, which
    /// `write_checkpoint` persists. Under backfill deferral keys wait here for
    /// several finalizations. See spec/decisions/ADR-2-finality-and-durable-watermark.md.
    pending_durable: FxHashSet<GroupKey>,
    /// The groups that the blocks of the current ingest touched, with their
    /// values from before it. `take_changes` emits one record per group: the
    /// change buffer would merge the records of each block into that anyway.
    ingest_touched: Touched,
    /// The touched groups of an emit in first-touch order, kept between
    /// emits up to `EMIT_ORDER_KEEP` entries so that a usual emit allocates
    /// nothing for its sort; a larger buffer would hold its memory.
    emit_order: Vec<(GroupKey, (usize, Option<usize>))>,
}

impl MVEngine {
    /// Build the view and load its stored state: Err if storage fails or
    /// holds state that this view cannot read.
    pub fn new(
        def: MVDef,
        storage: Arc<dyn StorageBackend>,
        source_column_types: &HashMap<String, ColumnType>,
    ) -> Result<Self> {
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
                        source_col: source_col.as_deref().map(SourceColumn::new),
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
                    Some(GroupKeyExtractor::Column(SourceColumn::new(source_col)))
                }
                OutputColumn::Window {
                    source_col,
                    interval_seconds,
                    ..
                } => Some(GroupKeyExtractor::Window(
                    SourceColumn::new(source_col),
                    *interval_seconds,
                )),
                OutputColumn::Agg { .. } => None,
            })
            .collect();

        let change_schema = Arc::new(ChangeSchema::mv(
            &def.name,
            output_columns.iter().map(|col| match col {
                OutputColumn::GroupBy { output_name, .. }
                | OutputColumn::Window { output_name, .. } => (output_name.clone(), true),
                OutputColumn::Agg { output_name, .. } => (output_name.clone(), false),
            }),
        ));

        let time_column = def
            .sliding_window
            .as_ref()
            .map(|sw| SourceColumn::new(&sw.time_column));

        let mut mv = MVEngine {
            sliding_window: def.sliding_window.clone(),
            def,
            change_schema,
            agg_funcs,
            agg_count,
            agg_feeds,
            group_key_extractors,
            time_column,
            resolved_for: None,
            groups: FxHashMap::default(),
            block_groups: BTreeMap::new(),
            storage,
            block_times: BlockTimes::default(),
            current_watermark: 0,
            expired: BTreeMap::new(),
            unstored: BTreeSet::new(),
            window_writes: Vec::new(),
            removed_groups: Vec::new(),
            pending_durable: FxHashSet::default(),
            ingest_touched: Touched::default(),
            emit_order: Vec::new(),
        };
        if mv.sliding_window.is_some() {
            mv.restore_window()?;
        } else {
            mv.restore_groups()?;
        }
        Ok(mv)
    }

    /// Load the finalized state of each group.
    fn restore_groups(&mut self) -> Result<()> {
        for key in self.storage.list_mv_group_keys(&self.def.name)? {
            let group_key = storage::decode_group_key(&key)
                .map_err(|e| self.unreadable(format!("group key: {e}")))?;
            let aggs = self
                .storage
                .get_mv_state(&self.def.name, &key)?
                .and_then(|bytes| deserialize_mv_group(&bytes, &self.agg_funcs))
                .ok_or_else(|| self.unreadable(format!("state of group {group_key:?}")))?;
            self.groups.insert(group_key, aggs);
        }
        Ok(())
    }

    /// Load the window as of the durable block, which `write_checkpoint`
    /// stores block by block.
    fn restore_window(&mut self) -> Result<()> {
        for key in self.storage.list_mv_group_keys(&self.def.name)? {
            let block = <[u8; 8]>::try_from(key.as_slice())
                .map(BlockNumber::from_be_bytes)
                .map_err(|_| self.unreadable(format!("window key {key:?}")))?;
            let record = self.storage.get_mv_state(&self.def.name, &key)?;
            record
                .and_then(|record| self.read_window_record(block, &record))
                .ok_or_else(|| self.unreadable(format!("window record of block {block}")))?;
        }
        self.current_watermark = self.block_times.max_ts().unwrap_or(0);
        Ok(())
    }

    /// Drop all in-memory state and load the stored state again.
    pub fn reload(&mut self) -> Result<()> {
        self.groups.clear();
        self.block_groups.clear();
        self.block_times = BlockTimes::default();
        self.current_watermark = 0;
        self.expired.clear();
        self.unstored.clear();
        self.window_writes.clear();
        self.removed_groups.clear();
        self.pending_durable.clear();
        self.ingest_touched = Touched::default();

        if self.sliding_window.is_some() {
            self.restore_window()
        } else {
            self.restore_groups()
        }
    }

    fn unreadable(&self, what: String) -> Error {
        Error::Storage(format!("unreadable state of MV '{}': {what}", self.def.name))
    }

    /// Add the block of a record that `window_record` wrote.
    fn read_window_record(&mut self, block: BlockNumber, record: &[u8]) -> Option<()> {
        let mut pos = 0;
        let ts = read_u64(record, &mut pos)? as i64;
        let group_count = read_u32(record, &mut pos)?;
        self.block_times.insert(block, ts);

        for _ in 0..group_count {
            let group_key = storage::decode_group_key(read_slice(record, &mut pos)?).ok()?;
            if !self.groups.contains_key(&group_key) {
                let aggs = self.create_agg_vec();
                self.groups.insert(group_key.clone(), aggs);
            }

            let aggs = self.groups.get_mut(&group_key).unwrap();
            for agg in aggs {
                let data = read_slice(record, &mut pos)?;
                if !data.is_empty() {
                    agg.read_block(block, data)?;
                }
            }
            self.block_groups.entry(block).or_default().insert(group_key);
        }
        (pos == record.len()).then_some(())
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn source(&self) -> &str {
        &self.def.source
    }

    /// Whether this MV uses a sliding window. Sliding-window MVs are excluded
    /// from backfill persist-deferral: see spec/decisions/ADR-2-finality-and-durable-watermark.md.
    pub fn is_sliding(&self) -> bool {
        self.sliding_window.is_some()
    }

    /// Process a batch of rows from a single block.
    /// Returns change records for new/updated groups.
    pub fn process_block<R: SourceRow>(
        &mut self,
        block: BlockNumber,
        rows: &[R],
    ) -> Vec<ChangeRecord> {
        self.apply_rows(block, rows);
        self.take_changes()
    }

    /// Apply the rows of one block of the current ingest; `take_changes`
    /// emits their changes.
    pub fn apply_rows<R: SourceRow>(&mut self, block: BlockNumber, rows: &[R]) {
        let mut touched = std::mem::take(&mut self.ingest_touched);
        self.apply_block(block, rows, &mut touched);
        self.ingest_touched = touched;
    }

    /// The changes of the blocks applied since the last call, one per group.
    pub fn take_changes(&mut self) -> Vec<ChangeRecord> {
        let touched = std::mem::take(&mut self.ingest_touched);
        if touched.groups.is_empty() {
            return Vec::new();
        }
        self.emit_changes(touched)
    }

    /// Forget the changes of a failed ingest, which its undo rolls back.
    pub fn discard_changes(&mut self) {
        self.ingest_touched = Touched::default();
    }

    /// `process_block` with no change records, for replay, which discards
    /// them. Replay after a deferred tail leaves every replayed block
    /// unfinalized, so reading the values of a group touched in each block
    /// would cost quadratic time in the tail length.
    pub fn replay_block<R: SourceRow>(&mut self, block: BlockNumber, rows: &[R]) {
        let mut touched = Touched {
            existence_only: true,
            ..Touched::default()
        };
        self.apply_block(block, rows, &mut touched);
        self.drop_empty_groups(touched);
    }

    #[inline]
    fn apply_block<R: SourceRow>(&mut self, block: BlockNumber, rows: &[R], touched: &mut Touched) {
        // Sliding window replay protection: skip blocks already in restored state
        if self.sliding_window.is_some() && self.block_times.contains_key(&block) {
            return;
        }

        if let Some(registry) = rows.first().and_then(R::registry) {
            self.resolve_columns(registry);
        }
        debug_assert!(rows.iter().all(|r| {
            let resolved = self.resolved_for.as_ref();
            r.registry()
                .is_none_or(|reg| resolved.is_some_and(|res| reg.names() == res.names()))
        }));

        // Track block timestamp for sliding windows
        if let Some(time_column) = &self.time_column {
            let block_max_ts = rows
                .iter()
                .filter_map(|r| r.column(time_column).as_i64())
                .max()
                // Fallback: if no rows have a valid timestamp, use current watermark
                // so the block still participates in expiry rather than leaking.
                .unwrap_or(self.current_watermark);
            self.block_times.insert(block, block_max_ts);
            self.unstored.insert(block);
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
                let value = match &feed.source_col {
                    Some(col) => row.column(col),
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
            self.expire_old_blocks(block, touched);
        }
    }

    /// Roll back all blocks after fork_point.
    /// Returns compensating change records.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Vec<ChangeRecord> {
        debug_assert!(
            self.ingest_touched.groups.is_empty(),
            "rollback with the changes of an ingest not taken"
        );

        // Guard: fork_point + 1 would overflow u64::MAX to 0, causing split_off(&0)
        // to remove the entire map. MAX is a valid no-op: nothing exists after it.
        if fork_point == BlockNumber::MAX {
            return Vec::new();
        }
        // Use BTreeMap range to efficiently find blocks > fork_point
        let rolled_back = self.block_groups.split_off(&(fork_point + 1));
        // The blocks that the rolled-back blocks expired.
        let unexpired = self.expired.split_off(&(fork_point + 1));
        // Not covered by `rolled_back`: a reopen rebuilds the groups of a
        // block from the aggregates, so a block that fed no aggregate has a
        // time and no groups.
        let rolled_back_times = self.block_times.split_off(fork_point + 1);
        self.unstored.split_off(&(fork_point + 1));

        if rolled_back.is_empty() && unexpired.is_empty() && rolled_back_times.is_empty() {
            return Vec::new();
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

        if self.sliding_window.is_some() {
            self.restore_expired(unexpired, fork_point, &mut touched);
            self.current_watermark = self.block_times.max_ts().unwrap_or(0);
        }

        // Emit changes (updates or deletes)
        self.emit_changes(touched)
    }

    /// Put back the blocks of `unexpired` at or below `fork_point`.
    fn restore_expired(
        &mut self,
        unexpired: BTreeMap<BlockNumber, Vec<ExpiredBlock>>,
        fork_point: BlockNumber,
        touched: &mut Touched,
    ) {
        for expired in unexpired.into_values().flatten() {
            // Rolled back itself: it expired on arrival.
            if expired.block > fork_point {
                continue;
            }

            self.block_times.insert(expired.block, expired.ts);
            for (key, mut data) in expired.groups {
                self.snapshot(touched, &key);

                match self.groups.get_mut(&key) {
                    Some(aggs) => {
                        for (from, agg) in data.iter_mut().zip(aggs.iter_mut()) {
                            from.move_block(expired.block, agg.as_mut());
                        }
                    }
                    // Expiry emptied the group and removed it.
                    None => {
                        self.groups.insert(key.clone(), data);
                    }
                }

                self.block_groups
                    .entry(expired.block)
                    .or_default()
                    .insert(key);
            }
        }
    }

    /// Merge the blocks up to `block` into finalized state. The groups whose
    /// stored state changed wait in `pending_durable` for `write_checkpoint`.
    /// A sliding window keeps its blocks apart until they expire, final or
    /// not; it queues the storage writes that move the stored window to
    /// `block` instead.
    pub fn advance_finality(&mut self, block: BlockNumber) {
        debug_assert!(
            self.ingest_touched.groups.is_empty(),
            "finality advanced with the changes of an ingest not taken"
        );

        if self.sliding_window.is_some() {
            self.advance_window(block);
        } else {
            let changed = self.finalize_blocks_up_to(block);
            self.pending_durable.extend(changed);
        }
    }

    /// Queue the writes that turn the stored window as of the previous
    /// advance into the window as of `block`: the blocks that became final,
    /// unless a final block expired them, and the deletion of the stored
    /// blocks that a block now final expired. A block expired by a block
    /// above `block` stays stored: a fork can bring it back.
    fn advance_window(&mut self, block: BlockNumber) {
        let (newly_final, expired_final) = match block.checked_add(1) {
            Some(after) => {
                let unstored = self.unstored.split_off(&after);
                let expired = self.expired.split_off(&after);
                (
                    std::mem::replace(&mut self.unstored, unstored),
                    std::mem::replace(&mut self.expired, expired),
                )
            }
            None => (
                std::mem::take(&mut self.unstored),
                std::mem::take(&mut self.expired),
            ),
        };

        for &b in &newly_final {
            let Some(&ts) = self.block_times.get(&b) else {
                continue; // Expired: in `self.expired` or `expired_final`.
            };
            let keys = self.block_groups.get(&b).into_iter().flatten();
            let groups = keys.filter_map(|key| Some((key, self.groups.get(key)?.as_slice())));
            let record = window_record(b, ts, groups);
            self.window_writes.push(WindowWrite::Put(b, record));
        }

        // The window as of `block` still holds these.
        for expired in self.expired.values().flatten() {
            if newly_final.contains(&expired.block) {
                let groups = expired.groups.iter().map(|(key, aggs)| (key, aggs.as_slice()));
                let record = window_record(expired.block, expired.ts, groups);
                self.window_writes.push(WindowWrite::Put(expired.block, record));
            }
        }

        // No fork can go below `block` now, so these blocks stay expired.
        for expired in expired_final.into_values().flatten() {
            if !newly_final.contains(&expired.block) {
                self.window_writes.push(WindowWrite::Delete(expired.block));
            }
        }
    }

    /// Write the stored state of every group in `pending_durable`, and delete
    /// the groups that emptied.
    pub fn write_checkpoint(&mut self, batch: &mut StorageWriteBatch) {
        if self.sliding_window.is_some() {
            for write in self.window_writes.drain(..) {
                match write {
                    WindowWrite::Put(block, record) => {
                        batch.put_mv_state(&self.def.name, block.to_be_bytes().to_vec(), record);
                    }
                    WindowWrite::Delete(block) => {
                        batch.delete_mv_state(&self.def.name, &block.to_be_bytes());
                    }
                }
            }
            // A window stores blocks, not groups.
            self.removed_groups.clear();
            return;
        }

        for group_key in std::mem::take(&mut self.pending_durable) {
            let aggs = match self.groups.get(&group_key) {
                Some(a) => a,
                None => continue, // removed meanwhile (handled below)
            };
            let gk_bytes = storage::encode_group_key(&group_key);
            batch.put_mv_state(&self.def.name, gk_bytes, serialize_mv_group(aggs));
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

    /// Move the blocks whose timestamps have fallen outside the sliding window
    /// to `expired`, and add the groups they held to `touched`.
    fn expire_old_blocks(&mut self, expired_by: BlockNumber, touched: &mut Touched) {
        let sw = self.sliding_window.as_ref().unwrap();
        let window_ms = (sw.interval_seconds as i64).saturating_mul(1000);
        let cutoff = self.current_watermark - window_ms;

        // Strict less-than: a block at the cutoff stays in the window.
        for (block, ts) in self.block_times.take_older_than(cutoff) {
            let keys = self.block_groups.remove(&block).unwrap_or_default();

            let mut groups = Vec::with_capacity(keys.len());
            for key in keys {
                // Snapshot prev output before first mutation of this group by expiry.
                // If already touched by row processing, keep that snapshot.
                self.snapshot(touched, &key);

                let mut data = self.create_agg_vec();
                let Some(aggs) = self.groups.get_mut(&key) else {
                    continue;
                };
                for (agg, into) in aggs.iter_mut().zip(data.iter_mut()) {
                    agg.move_block(block, into.as_mut());
                }
                groups.push((key, data));
            }

            self.expired
                .entry(expired_by)
                .or_default()
                .push(ExpiredBlock { block, ts, groups });
        }
    }

    /// Point the source columns at their positions in `registry`.
    fn resolve_columns(&mut self, registry: &Arc<ColumnRegistry>) {
        if self
            .resolved_for
            .as_ref()
            .is_some_and(|r| Arc::ptr_eq(r, registry))
        {
            return;
        }

        let feeds = self
            .agg_feeds
            .iter_mut()
            .filter_map(|f| f.source_col.as_mut());
        let keys = self.group_key_extractors.iter_mut().map(|e| match e {
            GroupKeyExtractor::Column(c) | GroupKeyExtractor::Window(c, _) => c,
        });
        for column in feeds.chain(keys).chain(self.time_column.as_mut()) {
            column.id = registry.get_id(&column.name);
        }
        self.resolved_for = Some(registry.clone());
    }

    fn compute_group_key<R: SourceRow>(&self, row: &R) -> GroupKey {
        let mut key = GroupKey::new();
        for ext in &self.group_key_extractors {
            match ext {
                GroupKeyExtractor::Column(source_col) => {
                    key.push(row.column(source_col).clone());
                }
                GroupKeyExtractor::Window(source_col, interval_seconds) => {
                    let ts = row.column(source_col).as_i64().unwrap_or(0);
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
        let order = touched.groups.len();
        touched.groups.insert(group_key.clone(), (order, at));
    }

    /// Emit one change per touched group against its snapshot. Output rows
    /// are built only for the groups whose values changed.
    fn emit_changes(&mut self, touched: Touched) -> Vec<ChangeRecord> {
        debug_assert!(!touched.existence_only);
        let Touched {
            groups, mut prev, ..
        } = touched;

        // In first-touch order, not hash order: keys that arrive together stay
        // together, and a target's index applies them faster that way.
        let mut touched = std::mem::take(&mut self.emit_order);
        touched.extend(groups);
        touched.sort_unstable_by_key(|(_, (order, _))| *order);

        let mut changes = Vec::with_capacity(touched.len());
        let mut current: Vec<Value> = Vec::with_capacity(self.agg_count);

        for (key, (_, at)) in touched.drain(..) {
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
                    let values = current.drain(..).collect();
                    changes.push(self.change_record(ChangeOp::Insert, key, values, None));
                }
                (Some(prev_values), true) => {
                    // Existing group updated -> Update
                    if current[..] != prev_values[..] {
                        let values = current.drain(..).collect();
                        let prev_values = take_values(prev_values);
                        changes.push(self.change_record(
                            ChangeOp::Update,
                            key,
                            values,
                            Some(prev_values),
                        ));
                    }
                }
                (Some(prev_values), false) => {
                    // Group became empty after rollback/expiry -> Delete
                    let prev_values = take_values(prev_values);

                    // Clean up empty group and track for storage deletion
                    self.groups.remove(&key);
                    self.removed_groups.push(key.clone());

                    changes.push(self.change_record(
                        ChangeOp::Delete,
                        key,
                        prev_values.clone(),
                        Some(prev_values),
                    ));
                }
                (None, false) => {
                    // Never emitted and already empty, e.g. its rows expired on
                    // arrival: nothing to emit, but a lingering empty group would
                    // turn the next row's Insert into an Update and get persisted.
                    // It was never stored, so there is nothing to delete.
                    self.groups.remove(&key);
                }
            }
        }

        if touched.capacity() <= EMIT_ORDER_KEEP {
            self.emit_order = touched;
        }
        changes
    }

    /// The state changes of `emit_changes`, without its change records.
    fn drop_empty_groups(&mut self, touched: Touched) {
        for (key, (_, at)) in touched.groups {
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
        }
    }

    fn change_record(
        &self,
        operation: ChangeOp,
        group_key: GroupKey,
        values: Box<[Value]>,
        prev_values: Option<Box<[Value]>>,
    ) -> ChangeRecord {
        ChangeRecord::new(
            operation,
            self.change_schema.clone(),
            group_key,
            values,
            prev_values,
        )
    }

    fn create_agg_vec(&self) -> Vec<Box<dyn AggregationFunc>> {
        let mut aggs = Vec::with_capacity(self.agg_count);
        for (func, feed) in self.agg_funcs.iter().zip(self.agg_feeds.iter()) {
            aggs.push(create_agg(func, &feed.column_type));
        }
        aggs
    }
}

/// Moves the values out of a snapshot, which is dropped after the emit.
fn take_values(values: &mut [Value]) -> Box<[Value]> {
    values
        .iter_mut()
        .map(|v| std::mem::replace(v, Value::Null))
        .collect()
}

fn resolve_output_name(item: &SelectItem) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    match &item.expr {
        SelectExpr::Column(col) => col.clone(),
        SelectExpr::Agg(func, col) => {
            let func_name = func.name();
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
        aggs.push(restore_agg(func, blob)?);
    }
    let prev_flag = *bytes.get(pos)?;
    pos += 1;
    match prev_flag {
        0 => {}
        1 => {
            let len = read_u32(bytes, &mut pos)?;
            let end = pos.checked_add(len)?;
            let pb = bytes.get(pos..end)?;
            rmp_serde::from_slice::<HashMap<String, Value>>(pb).ok()?;
            pos = end;
        }
        _ => return None,
    }
    (pos == bytes.len()).then_some(aggs)
}

// A sliding window is stored as of the last finality advance, one record per
// block under the view's name. All groups of a block expire together, and a
// final block changes only by expiry, so a record is written once and
// deleted once.
//   key:    block: u64 BE
//   record: ts: i64 LE, groups: u32 LE, per group: len: u32 LE +
//           `encode_group_key`, per agg: len: u32 LE + `write_block` bytes
//           (len 0: no data)

enum WindowWrite {
    Put(BlockNumber, Vec<u8>),
    Delete(BlockNumber),
}

/// The record of `block`: its time and the data that each group holds of it.
fn window_record<'a>(
    block: BlockNumber,
    ts: i64,
    groups: impl Iterator<Item = (&'a GroupKey, &'a [Box<dyn AggregationFunc>])>,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&ts.to_le_bytes());
    let count_at = buf.len();
    buf.extend_from_slice(&0u32.to_le_bytes());

    // A group with no data in the block, e.g. one fed only NULLs, is left
    // out: a reopen would restore it as a group with no data, which a group
    // in memory never is, and its next row would update instead of insert.
    let mut count = 0u32;
    for (key, aggs) in groups {
        let group_at = buf.len();
        write_len_prefixed(&mut buf, |buf| storage::write_values(buf, key));

        let mut has_data = false;
        for agg in aggs {
            write_len_prefixed(&mut buf, |buf| {
                has_data |= agg.write_block(block, buf);
            });
        }

        if has_data {
            count += 1;
        } else {
            buf.truncate(group_at);
        }
    }

    buf[count_at..count_at + 4].copy_from_slice(&count.to_le_bytes());
    buf
}

/// Append a u32 LE length, then what `write` appends.
fn write_len_prefixed(buf: &mut Vec<u8>, write: impl FnOnce(&mut Vec<u8>)) {
    let len_at = buf.len();
    buf.extend_from_slice(&0u32.to_le_bytes());
    write(buf);
    let len = (buf.len() - len_at - 4) as u32;
    buf[len_at..len_at + 4].copy_from_slice(&len.to_le_bytes());
}

fn read_slice<'a>(bytes: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let len = read_u32(bytes, pos)?;
    let end = pos.checked_add(len)?;
    let slice = bytes.get(*pos..end)?;
    *pos = end;
    Some(slice)
}

fn read_u64(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let end = pos.checked_add(8)?;
    let v = u64::from_le_bytes(bytes.get(*pos..end)?.try_into().ok()?);
    *pos = end;
    Some(v)
}

#[cfg(test)]
#[path = "mv_core_tests.rs"]
mod core_tests;

#[cfg(test)]
#[path = "mv_sliding_tests.rs"]
mod sliding_tests;
