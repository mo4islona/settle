//! The journal: the writes of each change batch that waits for its ack,
//! stored before the caller gets the batch. The target can apply a batch
//! and the process can stop before the ack; a reopen then commits the
//! journaled batches up to the one that the target applied, so that storage
//! and target agree, and a later fork undoes those batches in the target too.

use crate::error::{Error, Result};

use super::{BatchOp, StorageWriteBatch};

/// One stored part of a journaled batch. Part 0 holds the writes of the
/// batch; each later part, the writes of a call without a batch that
/// joined it.
pub struct JournalPart {
    pub sequence: u64,
    pub part: u64,
    pub entry: Vec<u8>,
}

const PUT_RAW_ROWS: u8 = 0;
const SET_REDUCER_FINALIZED: u8 = 1;
const PUT_MV_STATE: u8 = 2;
const PUT_META: u8 = 3;
const DELETE_MV_STATE: u8 = 4;
const DELETE_RAW_ROWS_AFTER: u8 = 5;
const PUT_BLOCK_HASH: u8 = 6;
const DELETE_BLOCK_HASH: u8 = 7;
const PUT_JOURNAL: u8 = 8;
const DELETE_JOURNAL: u8 = 9;
const DELETE_RAW_ROWS: u8 = 10;

pub fn encode(batch: &StorageWriteBatch) -> Vec<u8> {
    let len = batch.ops.iter().map(encoded_len).sum();
    let mut out = Vec::with_capacity(len);

    for op in &batch.ops {
        match op {
            BatchOp::PutRawRows { table, block, data } => {
                out.push(PUT_RAW_ROWS);
                put_bytes(&mut out, table.as_bytes());
                put_u64(&mut out, *block);
                put_bytes(&mut out, data);
            }
            BatchOp::SetReducerFinalized {
                reducer,
                group_key,
                state,
            } => {
                out.push(SET_REDUCER_FINALIZED);
                put_bytes(&mut out, reducer.as_bytes());
                put_bytes(&mut out, group_key);
                put_bytes(&mut out, state);
            }
            BatchOp::PutMvState {
                view,
                group_key,
                state,
            } => {
                out.push(PUT_MV_STATE);
                put_bytes(&mut out, view.as_bytes());
                put_bytes(&mut out, group_key);
                put_bytes(&mut out, state);
            }
            BatchOp::PutMeta { key, value } => {
                out.push(PUT_META);
                put_bytes(&mut out, key.as_bytes());
                put_bytes(&mut out, value);
            }
            BatchOp::DeleteMvState { view, group_key } => {
                out.push(DELETE_MV_STATE);
                put_bytes(&mut out, view.as_bytes());
                put_bytes(&mut out, group_key);
            }
            BatchOp::DeleteRawRowsAfter { table, after_block } => {
                out.push(DELETE_RAW_ROWS_AFTER);
                put_bytes(&mut out, table.as_bytes());
                put_u64(&mut out, *after_block);
            }
            BatchOp::DeleteRawRows {
                table,
                from_block,
                to_block,
            } => {
                out.push(DELETE_RAW_ROWS);
                put_bytes(&mut out, table.as_bytes());
                put_u64(&mut out, *from_block);
                put_u64(&mut out, *to_block);
            }
            BatchOp::PutBlockHash { block, hash } => {
                out.push(PUT_BLOCK_HASH);
                put_u64(&mut out, *block);
                put_bytes(&mut out, hash.as_bytes());
            }
            BatchOp::DeleteBlockHash { block } => {
                out.push(DELETE_BLOCK_HASH);
                put_u64(&mut out, *block);
            }
            BatchOp::PutJournal {
                sequence,
                part,
                entry,
            } => {
                out.push(PUT_JOURNAL);
                put_u64(&mut out, *sequence);
                put_u64(&mut out, *part);
                put_bytes(&mut out, entry);
            }
            BatchOp::DeleteJournal { sequence, part } => {
                out.push(DELETE_JOURNAL);
                put_u64(&mut out, *sequence);
                put_u64(&mut out, *part);
            }
        }
    }

    out
}

pub fn decode(bytes: &[u8]) -> Result<StorageWriteBatch> {
    let mut reader = Reader(bytes);
    let mut batch = StorageWriteBatch::new();

    while !reader.0.is_empty() {
        let op = match reader.u8()? {
            PUT_RAW_ROWS => BatchOp::PutRawRows {
                table: reader.string()?,
                block: reader.u64()?,
                data: reader.bytes()?,
            },
            SET_REDUCER_FINALIZED => BatchOp::SetReducerFinalized {
                reducer: reader.string()?,
                group_key: reader.bytes()?,
                state: reader.bytes()?,
            },
            PUT_MV_STATE => BatchOp::PutMvState {
                view: reader.string()?,
                group_key: reader.bytes()?,
                state: reader.bytes()?,
            },
            PUT_META => BatchOp::PutMeta {
                key: reader.string()?,
                value: reader.bytes()?,
            },
            DELETE_MV_STATE => BatchOp::DeleteMvState {
                view: reader.string()?,
                group_key: reader.bytes()?,
            },
            DELETE_RAW_ROWS_AFTER => BatchOp::DeleteRawRowsAfter {
                table: reader.string()?,
                after_block: reader.u64()?,
            },
            DELETE_RAW_ROWS => BatchOp::DeleteRawRows {
                table: reader.string()?,
                from_block: reader.u64()?,
                to_block: reader.u64()?,
            },
            PUT_BLOCK_HASH => BatchOp::PutBlockHash {
                block: reader.u64()?,
                hash: reader.string()?,
            },
            DELETE_BLOCK_HASH => BatchOp::DeleteBlockHash {
                block: reader.u64()?,
            },
            PUT_JOURNAL => BatchOp::PutJournal {
                sequence: reader.u64()?,
                part: reader.u64()?,
                entry: reader.bytes()?,
            },
            DELETE_JOURNAL => BatchOp::DeleteJournal {
                sequence: reader.u64()?,
                part: reader.u64()?,
            },
            tag => return Err(corrupt(&format!("unknown operation {tag}"))),
        };
        batch.ops.push(op);
    }

    Ok(batch)
}

fn encoded_len(op: &BatchOp) -> usize {
    const N: usize = 8;
    let fields = match op {
        BatchOp::PutRawRows { table, data, .. } => N + table.len() + N + N + data.len(),
        BatchOp::SetReducerFinalized {
            reducer,
            group_key,
            state,
        } => 3 * N + reducer.len() + group_key.len() + state.len(),
        BatchOp::PutMvState {
            view,
            group_key,
            state,
        } => 3 * N + view.len() + group_key.len() + state.len(),
        BatchOp::PutMeta { key, value } => 2 * N + key.len() + value.len(),
        BatchOp::DeleteMvState { view, group_key } => 2 * N + view.len() + group_key.len(),
        BatchOp::DeleteRawRowsAfter { table, .. } => 2 * N + table.len(),
        BatchOp::DeleteRawRows { table, .. } => 3 * N + table.len(),
        BatchOp::PutBlockHash { hash, .. } => 2 * N + hash.len(),
        BatchOp::DeleteBlockHash { .. } => N,
        BatchOp::PutJournal { entry, .. } => 3 * N + entry.len(),
        BatchOp::DeleteJournal { .. } => 2 * N,
    };
    1 + fields
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn corrupt(what: &str) -> Error {
    Error::Storage(format!("corrupt journal entry: {what}"))
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.0.len() {
            return Err(corrupt("truncated"));
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = usize::try_from(self.u64()?).map_err(|_| corrupt("length"))?;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?).map_err(|_| corrupt("name is not UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_op() -> StorageWriteBatch {
        let mut batch = StorageWriteBatch::new();
        batch.delete_raw_rows_after("swaps", 9);
        batch.put_raw_rows("swaps", 10, vec![1, 2, 3]);
        batch.delete_raw_rows("swaps", 0, 4);
        batch.set_reducer_finalized("running", b"ETH".to_vec(), vec![4]);
        batch.put_mv_state("volume", vec![5, 6], vec![]);
        batch.delete_mv_state("volume", &[7]);
        batch.put_meta("latest_block", &10u64.to_be_bytes());
        batch.put_block_hash(10, "0xabc");
        batch.delete_block_hash(11);
        batch.put_journal(3, 1, vec![8, 9]);
        batch.delete_journal(3, 0);
        batch
    }

    fn debug(batch: &StorageWriteBatch) -> Vec<String> {
        batch.ops.iter().map(|op| format!("{op:?}")).collect()
    }

    #[test]
    fn every_op_reads_back() {
        let batch = every_op();
        let encoded = encode(&batch);

        assert_eq!(
            encoded.len(),
            batch.ops.iter().map(encoded_len).sum::<usize>()
        );
        assert_eq!(debug(&decode(&encoded).unwrap()), debug(&batch));
    }

    #[test]
    fn damaged_entry_is_an_error() {
        let batch = every_op();
        let encoded = encode(&batch);
        // A cut between two operations leaves a valid shorter entry.
        let op_ends: Vec<usize> = batch
            .ops
            .iter()
            .scan(0, |end, op| {
                *end += encoded_len(op);
                Some(*end)
            })
            .collect();

        for len in (1..encoded.len()).filter(|len| !op_ends.contains(len)) {
            assert!(decode(&encoded[..len]).is_err(), "truncated to {len} bytes");
        }
        assert!(decode(&[0xff]).is_err());
    }
}
