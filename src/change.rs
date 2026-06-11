use std::collections::HashMap;

use rustc_hash::FxHashMap;

use crate::types::{BlockCursor, ChangeBatch, ChangeOp, ChangeRecord, PerfNode, Value};

/// Buffers change batches while downstream hasn't acknowledged.
/// Records are appended on push; merging is deferred to flush time.
pub struct ChangeBuffer {
    /// Pending (unacked) records, appended in order.
    pending: Vec<ChangeRecord>,
    /// Next sequence number.
    next_sequence: u64,
    /// Finalized cursor as of last push.
    finalized_head: Option<BlockCursor>,
    /// Latest cursor as of last push.
    latest_head: Option<BlockCursor>,
    /// Pending perf nodes, accumulated per push.
    pending_perf: Vec<PerfNode>,
    /// Max pending records before backpressure is applied.
    max_buffer_size: usize,
}

impl ChangeBuffer {
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            pending: Vec::new(),
            next_sequence: 1,
            finalized_head: None,
            latest_head: None,
            pending_perf: Vec::new(),
            max_buffer_size,
        }
    }

    /// Returns true if backpressure should be applied (buffer is full).
    pub fn is_full(&self) -> bool {
        self.pending.len() >= self.max_buffer_size
    }

    /// Number of pending records in the buffer.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Update the finalized and latest head cursors without pushing records.
    pub fn set_heads(
        &mut self,
        finalized_head: Option<BlockCursor>,
        latest_head: Option<BlockCursor>,
    ) {
        self.finalized_head = finalized_head;
        self.latest_head = latest_head;
    }

    /// Push new change records into the buffer (append-only, no merge on push).
    /// Merging is deferred to flush() time.
    pub fn push(
        &mut self,
        records: Vec<ChangeRecord>,
        finalized_head: Option<BlockCursor>,
        latest_head: Option<BlockCursor>,
        perf: Vec<PerfNode>,
    ) {
        self.finalized_head = finalized_head;
        self.latest_head = latest_head;
        self.pending.extend(records);
        self.pending_perf.extend(perf);
    }

    /// Flush: merge and drain all pending records into a ChangeBatch.
    /// Returns None if there are no pending records (or all cancel out).
    pub fn flush(&mut self) -> Option<ChangeBatch> {
        if self.pending.is_empty() {
            self.pending_perf.clear();
            return None;
        }

        // Merge records by (table, key) identity in a single pass.
        // Hash is used for fast lookup; key equality is verified to prevent collisions.
        // Vec<usize> handles the rare case of 3+ records with the same hash but different keys.
        // FxHashMap: the key is our own already-mixed u64 (from `hash_change_key`),
        // so SipHash over it is pure overhead — FxHash on the flush hot path.
        let mut index: FxHashMap<u64, Vec<usize>> =
            FxHashMap::with_capacity_and_hasher(self.pending.len(), Default::default());
        let mut merged: Vec<ChangeRecord> = Vec::with_capacity(self.pending.len());

        for record in self.pending.drain(..) {
            let key_hash = hash_change_key(&record.table, &record.key);

            let match_idx = index.get(&key_hash).and_then(|bucket| {
                bucket
                    .iter()
                    .copied()
                    .find(|&idx| merged[idx].table == record.table && merged[idx].key == record.key)
            });

            if let Some(idx) = match_idx {
                if !merge_in_place(&mut merged[idx], record) {
                    // Records cancel out — mark as cancelled and remove from bucket
                    merged[idx].operation = ChangeOp::Delete;
                    merged[idx].prev_values = None;
                    merged[idx].values.clear();
                    if let Some(bucket) = index.get_mut(&key_hash) {
                        bucket.retain(|&i| i != idx);
                        if bucket.is_empty() {
                            index.remove(&key_hash);
                        }
                    }
                }
            } else {
                let idx = merged.len();
                index.entry(key_hash).or_default().push(idx);
                merged.push(record);
            }
        }

        // Filter out cancelled records and group by table
        let mut tables: HashMap<String, Vec<ChangeRecord>> = HashMap::new();
        for record in merged.into_iter().filter(|r| !is_cancelled(r)) {
            if let Some(vec) = tables.get_mut(&record.table) {
                vec.push(record);
            } else {
                let table = record.table.clone();
                tables.insert(table, vec![record]);
            }
        }

        if tables.is_empty() {
            self.pending_perf.clear();
            return None;
        }

        let seq = self.next_sequence;
        self.next_sequence += 1;

        Some(ChangeBatch {
            sequence: seq,
            finalized_head: self.finalized_head.clone(),
            latest_head: self.latest_head.clone(),
            tables,
            perf: self.pending_perf.drain(..).collect(),
        })
    }

    /// Acknowledge a batch (no-op at the buffer level; durability is handled by Settle).
    pub fn ack(&mut self, _sequence: u64) {}

    /// Next sequence number that will be assigned to the next produced ChangeBatch.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Restore the next sequence number (used on open() to maintain monotonic
    /// sequence across restart from persisted META_NEXT_SEQUENCE).
    pub fn set_next_sequence(&mut self, v: u64) {
        self.next_sequence = v;
    }
}

/// Merge `incoming` into `existing` in place, moving fields instead of cloning.
/// Returns `false` if the records cancel out (insert + delete = no-op).
fn merge_in_place(existing: &mut ChangeRecord, incoming: ChangeRecord) -> bool {
    match (&existing.operation, &incoming.operation) {
        // Insert then Update: net Insert with latest values
        (ChangeOp::Insert, ChangeOp::Update) => {
            existing.values = incoming.values;
            // operation stays Insert, prev_values stays None
            true
        }

        // Insert then Delete: cancel out
        (ChangeOp::Insert, ChangeOp::Delete) => false,

        // Update then Update: keep original prev_values, latest values
        (ChangeOp::Update, ChangeOp::Update) => {
            existing.values = incoming.values;
            // operation stays Update, prev_values stays from first update
            true
        }

        // Update then Delete: net Delete with original prev_values
        (ChangeOp::Update, ChangeOp::Delete) => {
            existing.operation = ChangeOp::Delete;
            existing.values = incoming.values;
            // prev_values stays from first update
            true
        }

        // Delete then Insert: net Update (prev_values already set from the Delete)
        (ChangeOp::Delete, ChangeOp::Insert) => {
            existing.operation = ChangeOp::Update;
            existing.values = incoming.values;
            true
        }

        // Same operation following same: just replace in place
        _ => {
            *existing = incoming;
            true
        }
    }
}

/// Check if a record has been cancelled (insert + delete = no net effect).
fn is_cancelled(record: &ChangeRecord) -> bool {
    record.operation == ChangeOp::Delete
        && record.prev_values.is_none()
        && record.values.is_empty()
}

fn hash_change_key(table: &str, key: &HashMap<String, Value>) -> u64 {
    use core::hash::{Hash, Hasher};
    use rustc_hash::FxHasher;
    // FxHash, not SipHash: this runs per key-field per emit on the flush hot
    // path (it was ~6.9% `Sip13::write` in the flamegraph). The hash only feeds
    // an internal dedup index whose buckets re-check key equality in `flush()`,
    // so collision resistance is irrelevant here — speed is what matters.
    let mut hasher = FxHasher::default();
    table.hash(&mut hasher);
    // Order-independent hash: XOR + rotate for better mixing than wrapping_add.
    // Still commutative — collisions are handled by key equality check in flush().
    let mut combined: u64 = 0;
    for (k, v) in key {
        let mut field_hasher = FxHasher::default();
        k.hash(&mut field_hasher);
        v.hash(&mut field_hasher);
        combined ^= field_hasher.finish().rotate_left(5);
    }
    hasher.write_u64(combined);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ChangeOp;

    fn make_insert(table: &str, key_val: &str, data: &str) -> ChangeRecord {
        ChangeRecord {
            table: table.to_string(),
            operation: ChangeOp::Insert,
            key: HashMap::from([("id".to_string(), Value::String(key_val.to_string()))]),
            values: HashMap::from([("data".to_string(), Value::String(data.to_string()))]),
            prev_values: None,
        }
    }

    fn make_update(table: &str, key_val: &str, data: &str, prev: &str) -> ChangeRecord {
        ChangeRecord {
            table: table.to_string(),
            operation: ChangeOp::Update,
            key: HashMap::from([("id".to_string(), Value::String(key_val.to_string()))]),
            values: HashMap::from([("data".to_string(), Value::String(data.to_string()))]),
            prev_values: Some(HashMap::from([(
                "data".to_string(),
                Value::String(prev.to_string()),
            )])),
        }
    }

    fn make_delete(table: &str, key_val: &str) -> ChangeRecord {
        ChangeRecord {
            table: table.to_string(),
            operation: ChangeOp::Delete,
            key: HashMap::from([("id".to_string(), Value::String(key_val.to_string()))]),
            values: HashMap::new(),
            prev_values: Some(HashMap::from([(
                "data".to_string(),
                Value::String("old".to_string()),
            )])),
        }
    }

    #[test]
    fn empty_buffer_flush_returns_none() {
        let mut buffer = ChangeBuffer::new(100);
        assert!(buffer.flush().is_none());
    }

    fn cursor(n: u64) -> Option<BlockCursor> {
        if n == 0 {
            None
        } else {
            Some(BlockCursor {
                number: n,
                hash: format!("0x{n:x}"),
            })
        }
    }

    #[test]
    fn flush_returns_batch_and_clears() {
        let mut buffer = ChangeBuffer::new(100);
        buffer.push(
            vec![make_insert("t", "1", "a")],
            cursor(0),
            cursor(1000),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        assert_eq!(batch.sequence, 1);
        assert_eq!(batch.record_count(), 1);
        assert_eq!(batch.latest_head.as_ref().unwrap().number, 1000);

        // Second flush should be empty
        assert!(buffer.flush().is_none());
    }

    #[test]
    fn sequence_numbers_increment() {
        let mut buffer = ChangeBuffer::new(100);

        buffer.push(
            vec![make_insert("t", "1", "a")],
            cursor(0),
            cursor(1000),
            vec![],
        );
        let b1 = buffer.flush().unwrap();

        buffer.push(
            vec![make_insert("t", "2", "b")],
            cursor(0),
            cursor(1001),
            vec![],
        );
        let b2 = buffer.flush().unwrap();

        assert_eq!(b1.sequence, 1);
        assert_eq!(b2.sequence, 2);
    }

    #[test]
    fn merge_insert_then_update() {
        let mut buffer = ChangeBuffer::new(100);
        buffer.push(
            vec![make_insert("t", "1", "a")],
            cursor(0),
            cursor(1000),
            vec![],
        );
        buffer.push(
            vec![make_update("t", "1", "b", "a")],
            cursor(0),
            cursor(1001),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        let records = batch.records_for("t");
        assert_eq!(records.len(), 1);
        // Net effect: Insert with latest values
        assert_eq!(records[0].operation, ChangeOp::Insert);
        assert_eq!(
            records[0].values.get("data"),
            Some(&Value::String("b".into()))
        );
        assert!(records[0].prev_values.is_none());
    }

    #[test]
    fn merge_insert_then_delete_cancels() {
        let mut buffer = ChangeBuffer::new(100);
        buffer.push(
            vec![make_insert("t", "1", "a")],
            cursor(0),
            cursor(1000),
            vec![],
        );
        buffer.push(
            vec![ChangeRecord {
                table: "t".to_string(),
                operation: ChangeOp::Delete,
                key: HashMap::from([("id".to_string(), Value::String("1".to_string()))]),
                values: HashMap::new(),
                prev_values: Some(HashMap::from([(
                    "data".to_string(),
                    Value::String("a".to_string()),
                )])),
            }],
            cursor(0),
            cursor(1001),
            vec![],
        );

        // The merged result should be None (cancelled), so flush returns None
        let batch = buffer.flush();
        assert!(batch.is_none());
    }

    #[test]
    fn merge_update_then_update() {
        let mut buffer = ChangeBuffer::new(100);
        buffer.push(
            vec![make_update("t", "1", "b", "a")],
            cursor(0),
            cursor(1000),
            vec![],
        );
        buffer.push(
            vec![make_update("t", "1", "c", "b")],
            cursor(0),
            cursor(1001),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        let records = batch.records_for("t");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].operation, ChangeOp::Update);
        assert_eq!(
            records[0].values.get("data"),
            Some(&Value::String("c".into()))
        );
        // prev_values should be from the first update
        assert_eq!(
            records[0].prev_values.as_ref().unwrap().get("data"),
            Some(&Value::String("a".into()))
        );
    }

    #[test]
    fn merge_delete_then_insert() {
        let mut buffer = ChangeBuffer::new(100);
        buffer.push(vec![make_delete("t", "1")], cursor(0), cursor(1000), vec![]);
        buffer.push(
            vec![make_insert("t", "1", "new")],
            cursor(0),
            cursor(1001),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        let records = batch.records_for("t");
        assert_eq!(records.len(), 1);
        // Delete then Insert = Update
        assert_eq!(records[0].operation, ChangeOp::Update);
        assert_eq!(
            records[0].values.get("data"),
            Some(&Value::String("new".into()))
        );
    }

    /// Issue #1: Delete→Insert merge must preserve the original prev_values from the Delete,
    /// not overwrite them with the Delete's empty `values` map.
    #[test]
    fn delete_insert_merge_preserves_prev_values() {
        let mut buffer = ChangeBuffer::new(100);
        // Delete record has prev_values = {"data": "old"} and values = {}
        buffer.push(vec![make_delete("t", "1")], cursor(0), cursor(1000), vec![]);
        // Insert record brings new values
        buffer.push(
            vec![make_insert("t", "1", "new")],
            cursor(0),
            cursor(1001),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        let records = batch.records_for("t");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].operation, ChangeOp::Update);
        assert_eq!(
            records[0].values.get("data"),
            Some(&Value::String("new".into()))
        );
        // prev_values must be the original deleted row's data, NOT empty
        let prev = records[0]
            .prev_values
            .as_ref()
            .expect("prev_values must be Some");
        assert_eq!(
            prev.get("data"),
            Some(&Value::String("old".into())),
            "prev_values should contain the original deleted row data, not an empty map"
        );
    }

    /// Issue #2: Insert→Delete→Insert for the same key should produce a net Insert,
    /// not an Update (the row never existed before this batch).
    #[test]
    fn insert_delete_insert_produces_insert_not_update() {
        let mut buffer = ChangeBuffer::new(100);
        buffer.push(
            vec![make_insert("t", "1", "a")],
            cursor(0),
            cursor(1000),
            vec![],
        );
        buffer.push(
            vec![ChangeRecord {
                table: "t".to_string(),
                operation: ChangeOp::Delete,
                key: HashMap::from([("id".to_string(), Value::String("1".to_string()))]),
                values: HashMap::new(),
                prev_values: Some(HashMap::from([(
                    "data".to_string(),
                    Value::String("a".to_string()),
                )])),
            }],
            cursor(0),
            cursor(1001),
            vec![],
        );
        // Re-insert same key with new data
        buffer.push(
            vec![make_insert("t", "1", "b")],
            cursor(0),
            cursor(1002),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        let records = batch.records_for("t");
        assert_eq!(records.len(), 1);
        // The row never existed before this batch, so net effect should be Insert
        assert_eq!(
            records[0].operation,
            ChangeOp::Insert,
            "Insert→Delete→Insert should produce a net Insert, not Update"
        );
        assert_eq!(
            records[0].values.get("data"),
            Some(&Value::String("b".into()))
        );
        assert!(
            records[0].prev_values.is_none(),
            "A net Insert should have no prev_values"
        );
    }

    /// Three records for the same key must all merge correctly (A+B+C).
    /// Tests the Vec<usize> bucket handles repeated same-key merges.
    #[test]
    fn three_way_merge_same_key() {
        let mut buffer = ChangeBuffer::new(100);
        buffer.push(
            vec![make_insert("t", "1", "a")],
            cursor(0),
            cursor(1000),
            vec![],
        );
        buffer.push(
            vec![make_update("t", "1", "b", "a")],
            cursor(0),
            cursor(1001),
            vec![],
        );
        buffer.push(
            vec![make_update("t", "1", "c", "b")],
            cursor(0),
            cursor(1002),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        let records = batch.records_for("t");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].operation, ChangeOp::Insert);
        assert_eq!(
            records[0].values.get("data"),
            Some(&Value::String("c".into()))
        );
    }

    #[test]
    fn different_keys_not_merged() {
        let mut buffer = ChangeBuffer::new(100);
        buffer.push(
            vec![make_insert("t", "1", "a"), make_insert("t", "2", "b")],
            cursor(0),
            cursor(1000),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        assert_eq!(batch.record_count(), 2);
    }

    #[test]
    fn different_tables_not_merged() {
        let mut buffer = ChangeBuffer::new(100);
        buffer.push(
            vec![make_insert("t1", "1", "a"), make_insert("t2", "1", "b")],
            cursor(0),
            cursor(1000),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        assert_eq!(batch.record_count(), 2);
        assert_eq!(batch.records_for("t1").len(), 1);
        assert_eq!(batch.records_for("t2").len(), 1);
    }

    #[test]
    fn backpressure_when_full() {
        let mut buffer = ChangeBuffer::new(2);
        assert!(!buffer.is_full());

        buffer.push(
            vec![make_insert("t", "1", "a"), make_insert("t", "2", "b")],
            cursor(0),
            cursor(1000),
            vec![],
        );
        assert!(buffer.is_full());

        buffer.flush();
        assert!(!buffer.is_full());
    }

    #[test]
    fn finalized_and_latest_cursor_tracking() {
        let mut buffer = ChangeBuffer::new(100);

        buffer.push(
            vec![make_insert("t", "1", "a")],
            cursor(500),
            cursor(1000),
            vec![],
        );
        buffer.push(
            vec![make_insert("t", "2", "b")],
            cursor(600),
            cursor(1100),
            vec![],
        );

        let batch = buffer.flush().unwrap();
        assert_eq!(batch.finalized_head.as_ref().unwrap().number, 600);
        assert_eq!(batch.latest_head.as_ref().unwrap().number, 1100);
    }

    /// pending_perf must not leak into future batches when flush returns None.
    #[test]
    fn perf_cleared_on_empty_flush() {
        use crate::types::{PerfNode, PerfNodeKind};

        let mut buffer = ChangeBuffer::new(100);
        let node = PerfNode {
            kind: PerfNodeKind::Pipeline,
            name: "test".into(),
            duration_ms: 1.0,
            children: vec![],
        };

        // Push perf with no records — flush returns None
        buffer.push(vec![], cursor(0), cursor(1000), vec![node.clone()]);
        assert!(buffer.flush().is_none());

        // Next batch should not contain stale perf
        buffer.push(
            vec![make_insert("t", "1", "a")],
            cursor(0),
            cursor(1001),
            vec![],
        );
        let batch = buffer.flush().unwrap();
        assert!(batch.perf.is_empty(), "stale perf should not leak");
    }

    /// PerfNodeKind serde uses snake_case.
    #[test]
    fn perf_node_kind_serde_snake_case() {
        use crate::types::PerfNodeKind;

        let json = serde_json::to_string(&PerfNodeKind::RawTable).unwrap();
        assert_eq!(json, "\"raw_table\"");
        let json = serde_json::to_string(&PerfNodeKind::MV).unwrap();
        assert_eq!(json, "\"mv\"");
        let json = serde_json::to_string(&PerfNodeKind::Pipeline).unwrap();
        assert_eq!(json, "\"pipeline\"");
    }
}
