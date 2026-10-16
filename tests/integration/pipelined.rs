//! Pipelined acks on the RocksDB backend: several batches wait for their
//! ack, commit in order, and a reopen goes back to the last acked one.

use std::path::Path;

use settle::db::{Config, Settle};
use settle::storage::rocks::RocksDbBackend;
use settle::storage::StorageBackend;
use settle::test_helpers::{cursor, ingest_with_finalized_no_ack};
use settle::types::{BlockNumber, ChangeBatch, RowMap, Value};
use tempfile::TempDir;

/// `logs` is virtual and feeds nothing: a fork that only rolls back its
/// rows has no change to return.
const SCHEMA: &str = r#"
CREATE VIRTUAL TABLE logs (
    block_number UInt64,
    x            UInt64
);

CREATE TABLE swaps (
    block_number UInt64,
    pool         String,
    amount       UInt64
);

CREATE MATERIALIZED VIEW volume AS
SELECT pool, sum(amount) AS total
FROM swaps
GROUP BY pool;
"#;

fn open(dir: &Path, depth: usize) -> Settle {
    let config = Config::with_data_dir(SCHEMA, dir.to_str().unwrap()).max_pending_batches(depth);
    Settle::open(config).unwrap()
}

fn swap(amount: u64) -> RowMap {
    RowMap::from([
        ("pool".to_string(), Value::String("A".into())),
        ("amount".to_string(), Value::UInt64(amount)),
    ])
}

fn swap_block(db: &mut Settle, block: BlockNumber) -> ChangeBatch {
    let rows = vec![("swaps".into(), block, vec![swap(block)])];
    ingest_with_finalized_no_ack(db, rows, 0).unwrap().unwrap()
}

fn total(batch: &ChangeBatch) -> Value {
    batch.records_for("volume")[0].values["total"].clone()
}

#[test]
fn reopen_goes_back_to_the_last_acked_batch() {
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open(tmp.path(), 3);
        swap_block(&mut db, 1);
        let b2 = swap_block(&mut db, 2);
        swap_block(&mut db, 3);
        db.ack(b2.sequence).unwrap();
    }

    let mut db = open(tmp.path(), 3);
    assert_eq!(db.latest_block(), 2);
    let again = swap_block(&mut db, 3);
    assert_eq!(total(&again), Value::UInt64(6));
}

/// The fork's raw-row delete joins the unacked batch that put block 2 of
/// `logs`, after that put. The commit must apply them in that order.
#[test]
fn fork_without_batch_deletes_rows_of_the_unacked_batch() {
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open(tmp.path(), 2);
        let log = RowMap::from([("x".to_string(), Value::UInt64(7))]);
        let rows = vec![
            ("swaps".into(), 1, vec![swap(1)]),
            ("logs".into(), 2, vec![log]),
        ];
        let batch = ingest_with_finalized_no_ack(&mut db, rows, 0).unwrap().unwrap();

        let fork = db.handle_fork(vec![cursor(1)]).unwrap();
        assert!(fork.batch.is_none());
        db.ack(batch.sequence).unwrap();
    }

    let storage = RocksDbBackend::open(tmp.path().to_str().unwrap()).unwrap();
    assert!(storage.get_raw_rows("logs", 0, BlockNumber::MAX).unwrap().is_empty());
    assert_eq!(storage.get_raw_rows("swaps", 0, BlockNumber::MAX).unwrap().len(), 1);
    drop(storage);

    let db = open(tmp.path(), 2);
    assert_eq!(db.latest_block(), 1);
}
