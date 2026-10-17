pub mod format;
pub mod memory;
#[cfg(feature = "rocksdb")]
pub mod rocks;

use crate::error::{Error, Result};
use crate::types::{BlockNumber, ColumnRegistry, GroupKey, Row, RowMap, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// An operation to be committed atomically as part of a WriteBatch.
pub enum BatchOp {
    PutRawRows {
        table: String,
        block: BlockNumber,
        data: Vec<u8>,
    },
    SetReducerFinalized {
        reducer: String,
        group_key: Vec<u8>,
        state: Vec<u8>,
    },
    PutMvState {
        view: String,
        group_key: Vec<u8>,
        state: Vec<u8>,
    },
    PutMeta {
        key: String,
        value: Vec<u8>,
    },
    DeleteMvState {
        view: String,
        group_key: Vec<u8>,
    },
    DeleteRawRowsAfter {
        table: String,
        after_block: BlockNumber,
    },
    PutBlockHash {
        block: BlockNumber,
        hash: String,
    },
    DeleteBlockHash {
        block: BlockNumber,
    },
}

/// A collection of operations to be committed atomically.
/// Used for crash-safe finalization: all reducer state + metadata
/// is written in a single atomic operation.
pub struct StorageWriteBatch {
    pub ops: Vec<BatchOp>,
}

impl StorageWriteBatch {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn put_raw_rows(&mut self, table: &str, block: BlockNumber, data: Vec<u8>) {
        self.ops.push(BatchOp::PutRawRows {
            table: table.to_string(),
            block,
            data,
        });
    }

    pub fn set_reducer_finalized(&mut self, reducer: &str, group_key: Vec<u8>, state: Vec<u8>) {
        self.ops.push(BatchOp::SetReducerFinalized {
            reducer: reducer.to_string(),
            group_key,
            state,
        });
    }

    pub fn put_mv_state(&mut self, view: &str, group_key: Vec<u8>, state: Vec<u8>) {
        self.ops.push(BatchOp::PutMvState {
            view: view.to_string(),
            group_key,
            state,
        });
    }

    pub fn put_meta(&mut self, key: &str, value: &[u8]) {
        self.ops.push(BatchOp::PutMeta {
            key: key.to_string(),
            value: value.to_vec(),
        });
    }

    pub fn delete_mv_state(&mut self, view: &str, group_key: &[u8]) {
        self.ops.push(BatchOp::DeleteMvState {
            view: view.to_string(),
            group_key: group_key.to_vec(),
        });
    }

    pub fn delete_raw_rows_after(&mut self, table: &str, after_block: BlockNumber) {
        self.ops.push(BatchOp::DeleteRawRowsAfter {
            table: table.to_string(),
            after_block,
        });
    }

    pub fn put_block_hash(&mut self, block: BlockNumber, hash: &str) {
        self.ops.push(BatchOp::PutBlockHash {
            block,
            hash: hash.to_string(),
        });
    }

    pub fn delete_block_hash(&mut self, block: BlockNumber) {
        self.ops.push(BatchOp::DeleteBlockHash { block });
    }
}

/// Write batches that are made but not committed yet, oldest first. Disk
/// lacks their raw rows and reducer checkpoints, so a rollback reads storage
/// through them: it then sees what disk holds once they all commit.
#[derive(Default)]
pub struct PendingWrites<'a> {
    batches: Vec<&'a StorageWriteBatch>,
    /// Reducer → group key → newest pending finalized state.
    reducer_finalized: HashMap<&'a str, HashMap<&'a [u8], &'a [u8]>>,
}

impl<'a> PendingWrites<'a> {
    pub fn new(batches: impl IntoIterator<Item = &'a StorageWriteBatch>) -> Self {
        let batches: Vec<&StorageWriteBatch> = batches.into_iter().collect();

        let mut reducer_finalized: HashMap<&str, HashMap<&[u8], &[u8]>> = HashMap::new();
        for op in batches.iter().copied().flat_map(|b| &b.ops) {
            if let BatchOp::SetReducerFinalized {
                reducer,
                group_key,
                state,
            } = op
            {
                reducer_finalized
                    .entry(reducer.as_str())
                    .or_default()
                    .insert(group_key.as_slice(), state.as_slice());
            }
        }

        Self {
            batches,
            reducer_finalized,
        }
    }

    /// The raw rows of `table` above `after_block` once every pending batch
    /// commits, from the `stored` rows above it.
    pub fn raw_rows_after(
        &self,
        table: &str,
        after_block: BlockNumber,
        stored: Vec<(BlockNumber, Vec<u8>)>,
    ) -> Vec<(BlockNumber, Vec<u8>)> {
        if self.batches.is_empty() {
            return stored;
        }

        // Ops apply in batch order, as `commit` applies them.
        let mut rows: BTreeMap<BlockNumber, Vec<u8>> = stored.into_iter().collect();
        for op in self.batches.iter().copied().flat_map(|b| &b.ops) {
            match op {
                BatchOp::PutRawRows {
                    table: t,
                    block,
                    data,
                } if t == table && *block > after_block => {
                    rows.insert(*block, data.clone());
                }
                BatchOp::DeleteRawRowsAfter {
                    table: t,
                    after_block: deleted_after,
                } if t == table => {
                    if let Some(first_deleted) = deleted_after.checked_add(1) {
                        rows.split_off(&first_deleted);
                    }
                }
                _ => {}
            }
        }
        rows.into_iter().collect()
    }

    /// The newest finalized state of the group that a pending batch writes.
    pub fn reducer_finalized(&self, reducer: &str, group_key: &[u8]) -> Option<&'a [u8]> {
        self.reducer_finalized.get(reducer)?.get(group_key).copied()
    }
}

/// Serialized state blob (MessagePack-encoded).
pub type StateBytes = Vec<u8>;

/// A composite key for reducer/MV group lookups, serialized for storage.
pub type GroupKeyBytes = Vec<u8>;

/// Encode a group key (Vec<Value>) into deterministic bytes for storage keying.
/// Uses the fast tag-based binary codec (no msgpack string/marker overhead).
pub fn encode_group_key(key: &[Value]) -> GroupKeyBytes {
    encode_values(key)
}

/// Decode a group key from storage bytes.
pub fn decode_group_key(bytes: &[u8]) -> Result<GroupKey> {
    Ok(decode_values(bytes)?.into_iter().collect())
}

/// Encode reducer state (RowMap) to bytes.
pub fn encode_state(state: &RowMap) -> StateBytes {
    rmp_serde::to_vec(state).expect("state serialization should not fail")
}

/// Decode reducer state from bytes.
pub fn decode_state(bytes: &[u8]) -> RowMap {
    rmp_serde::from_slice(bytes).expect("state deserialization should not fail")
}

/// Encode a positional list of `Value`s (reducer state in schema field order)
/// using the fast tag-based binary codec — no string keys, no msgpack overhead.
/// Format: `count: u32 LE` then one `encode_value` per value.
pub fn encode_values(values: &[Value]) -> StateBytes {
    let capacity = 4 + values.iter().map(encoded_len_hint).sum::<usize>();
    let mut buf = Vec::with_capacity(capacity);
    write_values(&mut buf, values);
    buf
}

/// [`encode_values`] appended to `buf`.
pub fn write_values(buf: &mut Vec<u8>, values: &[Value]) {
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for v in values {
        encode_value(buf, v);
    }
}

/// Decode a positional list of `Value`s encoded by [`encode_values`].
pub fn decode_values(bytes: &[u8]) -> Result<Vec<Value>> {
    let mut pos = 0usize;
    let n = u32::from_le_bytes(read_bytes(bytes, &mut pos, 4)?.try_into().unwrap()) as usize;

    // Each value takes at least one byte: a corrupt count must not allocate.
    let mut out = Vec::with_capacity(n.min(bytes.len()));
    for _ in 0..n {
        out.push(decode_value(bytes, &mut pos)?);
    }

    if pos != bytes.len() {
        return Err(Error::Storage(format!(
            "{} trailing bytes after {n} encoded values",
            bytes.len() - pos
        )));
    }
    Ok(out)
}

// --- Custom binary row encoding ---
//
// Format:
//   num_rows: u32 LE
//   num_cols: u16 LE
//   For each row × column:
//     type_tag: u8
//     data (varies by type — see encode_value/decode_value)
//
// This avoids serde/msgpack overhead for the inner Value serialization loop.

const TAG_NULL: u8 = 0;
const TAG_UINT64: u8 = 1;
const TAG_INT64: u8 = 2;
const TAG_FLOAT64: u8 = 3;
const TAG_STRING: u8 = 4;
const TAG_DATETIME: u8 = 5;
const TAG_BOOLEAN: u8 = 6;
const TAG_BYTES: u8 = 7;
const TAG_UINT256: u8 = 8;
const TAG_BASE58: u8 = 9;
const TAG_JSON: u8 = 10;

/// An upper bound of what `encode_value` writes for `val`, JSON aside.
fn encoded_len_hint(val: &Value) -> usize {
    // The tag and any fixed-size value but a U256; length-prefixed values
    // need their payload on top.
    let extra = match val {
        Value::String(s) => s.len(),
        Value::Bytes(b) | Value::Base58(b) => b.len(),
        Value::Uint256(_) => 24,
        _ => 0,
    };
    9 + extra
}

fn encode_value(buf: &mut Vec<u8>, val: &Value) {
    match val {
        Value::Null => buf.push(TAG_NULL),
        Value::UInt64(v) => {
            buf.push(TAG_UINT64);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Value::Int64(v) => {
            buf.push(TAG_INT64);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Value::Float64(v) => {
            buf.push(TAG_FLOAT64);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Value::String(s) => {
            buf.push(TAG_STRING);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::DateTime(v) => {
            buf.push(TAG_DATETIME);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Value::Boolean(v) => {
            buf.push(TAG_BOOLEAN);
            buf.push(if *v { 1 } else { 0 });
        }
        Value::Bytes(v) => {
            buf.push(TAG_BYTES);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        Value::Uint256(v) => {
            buf.push(TAG_UINT256);
            buf.extend_from_slice(v);
        }
        Value::Base58(v) => {
            buf.push(TAG_BASE58);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        Value::JSON(v) => {
            buf.push(TAG_JSON);
            let s = serde_json::to_string(v).unwrap_or_default();
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

fn read_bytes<'a>(bytes: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    if *pos + n > bytes.len() {
        return Err(Error::Storage(format!(
            "truncated data: need {} bytes at offset {}, have {} remaining",
            n,
            *pos,
            bytes.len() - *pos
        )));
    }
    let slice = &bytes[*pos..*pos + n];
    *pos += n;
    Ok(slice)
}

fn decode_value(bytes: &[u8], pos: &mut usize) -> Result<Value> {
    let tag = *read_bytes(bytes, pos, 1)?.first().unwrap();
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_UINT64 => {
            let v = u64::from_le_bytes(read_bytes(bytes, pos, 8)?.try_into().unwrap());
            Ok(Value::UInt64(v))
        }
        TAG_INT64 => {
            let v = i64::from_le_bytes(read_bytes(bytes, pos, 8)?.try_into().unwrap());
            Ok(Value::Int64(v))
        }
        TAG_FLOAT64 => {
            let v = f64::from_le_bytes(read_bytes(bytes, pos, 8)?.try_into().unwrap());
            Ok(Value::Float64(v))
        }
        TAG_STRING => {
            let len = u32::from_le_bytes(read_bytes(bytes, pos, 4)?.try_into().unwrap()) as usize;
            let s = std::str::from_utf8(read_bytes(bytes, pos, len)?)
                .map_err(|e| Error::Storage(format!("invalid utf8 in stored string: {e}")))?
                .to_string();
            Ok(Value::String(s))
        }
        TAG_DATETIME => {
            let v = i64::from_le_bytes(read_bytes(bytes, pos, 8)?.try_into().unwrap());
            Ok(Value::DateTime(v))
        }
        TAG_BOOLEAN => {
            let v = read_bytes(bytes, pos, 1)?[0] != 0;
            Ok(Value::Boolean(v))
        }
        TAG_BYTES => {
            let len = u32::from_le_bytes(read_bytes(bytes, pos, 4)?.try_into().unwrap()) as usize;
            let v = read_bytes(bytes, pos, len)?.to_vec();
            Ok(Value::Bytes(v))
        }
        TAG_UINT256 => {
            let v: [u8; 32] = read_bytes(bytes, pos, 32)?.try_into().unwrap();
            Ok(Value::Uint256(v))
        }
        TAG_BASE58 => {
            let len = u32::from_le_bytes(read_bytes(bytes, pos, 4)?.try_into().unwrap()) as usize;
            let v = read_bytes(bytes, pos, len)?.to_vec();
            Ok(Value::Base58(v))
        }
        TAG_JSON => {
            let len = u32::from_le_bytes(read_bytes(bytes, pos, 4)?.try_into().unwrap()) as usize;
            let s = std::str::from_utf8(read_bytes(bytes, pos, len)?)
                .map_err(|e| Error::Storage(format!("invalid utf8 in stored json: {e}")))?;
            let v = serde_json::from_str(s)
                .map_err(|e| Error::Storage(format!("invalid json in stored data: {e}")))?;
            Ok(Value::JSON(v))
        }
        _ => Err(Error::Storage(format!("unknown value type tag: {tag}"))),
    }
}

/// Encode rows directly from RowMaps using the registry's column order.
/// Avoids creating intermediate Row objects.
pub fn encode_rows_from_maps(maps: &[RowMap], registry: &ColumnRegistry) -> Vec<u8> {
    let num_rows = maps.len();
    let num_cols = registry.len();
    let mut buf = Vec::with_capacity(6 + num_rows * num_cols * 10);

    buf.extend_from_slice(&(num_rows as u32).to_le_bytes());
    buf.extend_from_slice(&(num_cols as u16).to_le_bytes());

    for map in maps {
        for name in registry.names() {
            match map.get(name) {
                None => buf.push(TAG_NULL),
                Some(val) => encode_value(&mut buf, val),
            }
        }
    }

    buf
}

/// Encode Row objects into bytes for storage.
pub fn encode_rows(rows: &[Row]) -> Vec<u8> {
    if rows.is_empty() {
        let mut buf = Vec::with_capacity(6);
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        return buf;
    }
    let num_rows = rows.len();
    let num_cols = rows[0].registry().len();
    let mut buf = Vec::with_capacity(6 + num_rows * num_cols * 10);

    buf.extend_from_slice(&(num_rows as u32).to_le_bytes());
    buf.extend_from_slice(&(num_cols as u16).to_le_bytes());

    for row in rows {
        for val in row.values() {
            encode_value(&mut buf, val);
        }
    }

    buf
}

/// Decode rows from bytes, wrapping each with the given registry.
pub fn decode_rows(bytes: &[u8], registry: &Arc<ColumnRegistry>) -> Result<Vec<Row>> {
    let mut pos = 0;
    let num_rows = u32::from_le_bytes(read_bytes(bytes, &mut pos, 4)?.try_into().unwrap()) as usize;
    if num_rows == 0 {
        return Ok(Vec::new());
    }
    let num_cols = u16::from_le_bytes(read_bytes(bytes, &mut pos, 2)?.try_into().unwrap()) as usize;

    if num_cols != registry.len() {
        return Err(Error::Storage(format!(
            "decode_rows: header column count {} does not match registry length {}",
            num_cols,
            registry.len()
        )));
    }

    let mut rows = Vec::new();
    rows.try_reserve(num_rows)
        .map_err(|_| Error::Storage(format!("decode_rows: cannot allocate {num_rows} rows")))?;
    for _ in 0..num_rows {
        let mut values = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            values.push(decode_value(bytes, &mut pos)?);
        }
        rows.push(Row::from_values(registry.clone(), values));
    }
    Ok(rows)
}

/// Storage backend trait for Settle persistence.
///
/// All methods use `&self` with interior mutability (the implementation
/// is expected to use internal locking).
///
/// Raw row methods operate on pre-encoded bytes. The engine layer handles
/// encoding/decoding with the appropriate ColumnRegistry.
pub trait StorageBackend: Send + Sync {
    // --- Raw table rows (encoded bytes) ---

    /// Store encoded rows for a given table and block number.
    fn put_raw_rows(&self, table: &str, block: BlockNumber, data: &[u8]) -> Result<()>;

    /// Get encoded rows for a table in the given block range (inclusive).
    /// Returns (block_number, encoded_bytes) pairs ordered by block number.
    fn get_raw_rows(
        &self,
        table: &str,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> Result<Vec<(BlockNumber, Vec<u8>)>>;

    /// `get_raw_rows` of the first `max_blocks` blocks in the range, for a
    /// caller that reads a long range a part at a time. A backend that cannot
    /// stop early keeps this default, which returns the whole range: the
    /// caller accepts more blocks than it asked for, whereas a cut to
    /// `max_blocks` would read the rest of the range again for each part.
    fn get_raw_rows_limited(
        &self,
        table: &str,
        from_block: BlockNumber,
        to_block: BlockNumber,
        _max_blocks: usize,
    ) -> Result<Vec<(BlockNumber, Vec<u8>)>> {
        self.get_raw_rows(table, from_block, to_block)
    }

    /// Delete all rows for a table where block_number > after_block.
    fn delete_raw_rows_after(&self, table: &str, after_block: BlockNumber) -> Result<()>;

    /// Atomically remove and return encoded rows where block_number > after_block.
    /// Must be implemented as a single atomic operation — a non-atomic read+delete
    /// can lose data on crash.
    fn take_raw_rows_after(
        &self,
        table: &str,
        after_block: BlockNumber,
    ) -> Result<Vec<(BlockNumber, Vec<u8>)>>;

    // --- Reducer state snapshots ---

    fn put_reducer_state(
        &self,
        reducer: &str,
        group_key: &[u8],
        block: BlockNumber,
        state: &[u8],
    ) -> Result<()>;

    fn get_reducer_state(
        &self,
        reducer: &str,
        group_key: &[u8],
        block: BlockNumber,
    ) -> Result<Option<Vec<u8>>>;

    fn get_reducer_state_at_or_before(
        &self,
        reducer: &str,
        group_key: &[u8],
        block: BlockNumber,
    ) -> Result<Option<(BlockNumber, Vec<u8>)>>;

    fn delete_reducer_states_after(
        &self,
        reducer: &str,
        group_key: &[u8],
        after_block: BlockNumber,
    ) -> Result<()>;

    // --- Reducer finalized state ---

    fn get_reducer_finalized(&self, reducer: &str, group_key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn set_reducer_finalized(&self, reducer: &str, group_key: &[u8], state: &[u8]) -> Result<()>;

    fn delete_reducer_states_up_to(
        &self,
        reducer: &str,
        group_key: &[u8],
        up_to_block: BlockNumber,
    ) -> Result<()>;

    // --- MV state ---

    fn put_mv_state(&self, view: &str, group_key: &[u8], state: &[u8]) -> Result<()>;

    fn get_mv_state(&self, view: &str, group_key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn delete_mv_state(&self, view: &str, group_key: &[u8]) -> Result<()>;

    fn list_mv_group_keys(&self, view: &str) -> Result<Vec<Vec<u8>>>;

    // --- Metadata ---

    fn put_meta(&self, key: &str, value: &[u8]) -> Result<()>;
    fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>>;

    // --- Block hashes (written only through `commit`) ---

    /// Every stored block hash, in block order.
    fn get_block_hashes(&self) -> Result<Vec<(BlockNumber, String)>>;

    // --- Bulk operations ---

    fn list_reducer_group_keys(&self, reducer: &str) -> Result<Vec<Vec<u8>>>;

    // --- Atomic batch commit ---

    /// Atomically commit a batch of operations.
    /// All operations in the batch either succeed together or fail together.
    /// Used for crash-safe finalization.
    fn commit(&self, batch: &StorageWriteBatch) -> Result<()>;

    /// Make every commit so far survive a power loss, not only a crash of
    /// the process. A backend whose commits are that durable on return keeps
    /// this default.
    fn sync(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_rows_truncated_returns_err() {
        let reg = Arc::new(ColumnRegistry::new(vec!["x".to_string()]));
        assert!(decode_rows(&[], &reg).is_err());
        assert!(decode_rows(&[0, 0], &reg).is_err()); // too short for header
    }

    #[test]
    fn decode_rows_unknown_tag_returns_err() {
        let reg = Arc::new(ColumnRegistry::new(vec!["x".to_string()]));
        // Header: 1 row, 1 col, then an invalid tag byte (0xFF)
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // num_rows
        bytes.extend_from_slice(&1u16.to_le_bytes()); // num_cols
        bytes.push(0xFF); // unknown tag
        assert!(decode_rows(&bytes, &reg).is_err());
    }

    #[test]
    fn decode_rows_column_count_mismatch_returns_err() {
        let reg = Arc::new(ColumnRegistry::new(vec!["x".to_string(), "y".to_string()]));
        // Header says 1 col but registry has 2
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // num_rows
        bytes.extend_from_slice(&1u16.to_le_bytes()); // num_cols = 1, registry = 2
        let err = decode_rows(&bytes, &reg).unwrap_err();
        assert!(err.to_string().contains("column count"));
    }
}
