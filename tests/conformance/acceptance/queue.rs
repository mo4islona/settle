//! Pipelined acks: several change batches wait for their ack while the
//! caller ingests the next blocks. Random histories with crashes and forks
//! are in `ct02_reopen/recovery.rs`; these tests pin the contract.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use settle::db::{Config, Settle};
use settle::error::{Error, Result};
use settle::storage::StorageBackend;
use settle::storage::memory::MemoryBackend;
use settle::test_helpers::{cursor, ingest_with_finalized_no_ack};
use settle::types::{BlockNumber, ChangeBatch, ChangeOp, RowMap, Value};

use crate::support::*;

fn open(schema: &str, storage: Arc<dyn StorageBackend>, depth: usize) -> Settle {
    let config = Config::new(schema).storage(storage).max_pending_batches(depth);
    Settle::open(config).unwrap()
}

fn swap_block(db: &mut Settle, block: BlockNumber, finalized: BlockNumber) -> Result<ChangeBatch> {
    let rows = vec![("swaps".into(), block, vec![make_swap("ETH", block as f64)])];
    let batch = ingest_with_finalized_no_ack(db, rows, finalized)?;
    Ok(batch.expect("a data block makes a batch"))
}

#[test]
fn ack_is_cumulative() {
    let mut db = open(SIMPLE_SCHEMA, Arc::new(MemoryBackend::new()), 3);
    let b1 = swap_block(&mut db, 1, 0).unwrap();
    let b2 = swap_block(&mut db, 2, 0).unwrap();
    let b3 = swap_block(&mut db, 3, 0).unwrap();
    assert_eq!(db.unacked_batches(), 3);

    db.ack(b2.sequence).unwrap();
    assert_eq!(db.unacked_batches(), 1);

    db.ack(b1.sequence).unwrap();
    assert_eq!(db.unacked_batches(), 1, "an ack below the oldest unacked batch does nothing");

    match db.ack(b3.sequence + 1) {
        Err(Error::WrongAckSequence { expected, got }) => {
            assert_eq!((expected, got), (b3.sequence, b3.sequence + 1));
        }
        other => panic!("expected WrongAckSequence, got {other:?}"),
    }

    db.ack(b3.sequence).unwrap();
    assert!(!db.is_awaiting_ack());
}

#[test]
fn full_queue_blocks_ingest_and_fork() {
    let mut db = open(SIMPLE_SCHEMA, Arc::new(MemoryBackend::new()), 2);
    let b1 = swap_block(&mut db, 1, 0).unwrap();
    swap_block(&mut db, 2, 0).unwrap();

    match swap_block(&mut db, 3, 0) {
        Err(Error::PendingAck { sequence, .. }) => assert_eq!(sequence, b1.sequence),
        other => panic!("expected PendingAck, got {other:?}"),
    }
    assert!(matches!(
        db.handle_fork(vec![cursor(1)]),
        Err(Error::PendingAck { .. })
    ));

    db.ack(b1.sequence).unwrap();
    swap_block(&mut db, 3, 0).unwrap();
}

/// Replay reads raw rows and reducer state from disk only.
#[test]
fn replay_needs_every_batch_acked() {
    let mut db = open(DEX_SCHEMA, Arc::new(MemoryBackend::new()), 4);
    let rows = vec![("trades".into(), 1, vec![make_trade("alice", "buy", 1.0, 1.0)])];
    ingest_with_finalized_no_ack(&mut db, rows, 0).unwrap();

    assert!(matches!(
        db.replay_reducer("pnl"),
        Err(Error::PendingAck { .. })
    ));
}

/// A heartbeat behind an unacked batch must not reach disk first: its
/// latest block and durable watermark would point past raw rows that disk
/// does not have.
#[test]
fn writes_without_batch_commit_with_the_newest_batch() {
    let storage = Arc::new(MemoryBackend::new());
    let mut db = open(SIMPLE_SCHEMA, storage.clone(), 2);
    let b1 = swap_block(&mut db, 1, 0).unwrap();

    let heartbeat = ingest_with_finalized_no_ack(&mut db, vec![], 1).unwrap();
    assert!(heartbeat.is_none());
    assert_eq!(stored_u64(&storage, META_LATEST_BLOCK), None);

    db.ack(b1.sequence).unwrap();
    assert_eq!(stored_u64(&storage, META_LATEST_BLOCK), Some(1));
    assert_eq!(stored_u64(&storage, META_DURABLE_BLOCK), Some(1));
}

/// The writes of a call without a batch wait in memory with the newest
/// batch, so the call takes a place in the queue: a slow target cannot make
/// them grow without a bound.
#[test]
fn a_call_without_batch_takes_a_place() {
    let mut db = open(SIMPLE_SCHEMA, Arc::new(MemoryBackend::new()), 2);
    let b1 = swap_block(&mut db, 1, 0).unwrap();

    let heartbeat = ingest_with_finalized_no_ack(&mut db, vec![], 1).unwrap();
    assert!(heartbeat.is_none());
    assert_eq!(db.unacked_batches(), 2);

    match ingest_with_finalized_no_ack(&mut db, vec![], 1) {
        Err(Error::PendingAck { sequence, .. }) => assert_eq!(sequence, b1.sequence),
        other => panic!("expected PendingAck, got {other:?}"),
    }

    db.ack(b1.sequence).unwrap();
    assert_eq!(db.unacked_batches(), 0);
    swap_block(&mut db, 2, 1).unwrap();
}

#[test]
fn failed_ack_commit_keeps_that_batch_and_later_ones() {
    let storage = Arc::new(FailingStorage::default());
    let mut db = open(SIMPLE_SCHEMA, storage.clone(), 3);
    swap_block(&mut db, 1, 0).unwrap();
    swap_block(&mut db, 2, 0).unwrap();
    let b3 = swap_block(&mut db, 3, 0).unwrap();

    storage.commits_before_failure.store(1, Ordering::SeqCst);
    assert!(db.ack(b3.sequence).is_err());
    assert_eq!(db.unacked_batches(), 2);

    storage.commits_before_failure.store(u64::MAX, Ordering::SeqCst);
    db.ack(b3.sequence).unwrap();
    assert!(!db.is_awaiting_ack());

    let reopened = open(SIMPLE_SCHEMA, storage, 3);
    assert_eq!(reopened.latest_block(), 3);
}

/// An ack syncs storage once, after all the batches it commits: the sync
/// covers every commit before it. A heartbeat commits at once and syncs
/// nothing, as no target waits for it.
#[test]
fn ack_syncs_once_per_cumulative_ack() {
    let storage = Arc::new(FailingStorage::default());
    let mut db = open(SIMPLE_SCHEMA, storage.clone(), 3);
    swap_block(&mut db, 1, 0).unwrap();
    let b2 = swap_block(&mut db, 2, 0).unwrap();

    db.ack(b2.sequence).unwrap();
    assert_eq!(storage.syncs.load(Ordering::SeqCst), 1);

    db.ack(b2.sequence).unwrap();
    assert!(ingest_with_finalized_no_ack(&mut db, vec![], 2).unwrap().is_none());
    assert_eq!(storage.syncs.load(Ordering::SeqCst), 1);
}

/// The batches are committed but maybe not on disk: a second sync could
/// report them synced after a failure lost them. So the instance poisons, and
/// no later ack succeeds: not a retry, which has nothing to commit, nor the
/// ack of a later batch, whose sync cannot vouch for the earlier commits.
#[test]
fn failed_sync_refuses_every_later_ack() {
    let storage = Arc::new(FailingStorage::default());
    let mut db = open(SIMPLE_SCHEMA, storage.clone(), 3);
    let b1 = swap_block(&mut db, 1, 0).unwrap();
    let b2 = swap_block(&mut db, 2, 0).unwrap();

    storage.fail_syncs.store(true, Ordering::SeqCst);
    assert!(matches!(db.ack(b1.sequence), Err(Error::SyncFailed(_))));
    assert!(db.is_poisoned());
    assert_eq!(db.unacked_batches(), 1);

    storage.fail_syncs.store(false, Ordering::SeqCst);
    assert!(matches!(db.ack(b1.sequence), Err(Error::SyncFailed(_))));
    assert!(matches!(db.ack(b2.sequence), Err(Error::SyncFailed(_))));
    assert!(matches!(swap_block(&mut db, 3, 0), Err(Error::SyncFailed(_))));
    assert_eq!(db.unacked_batches(), 1);
    assert_eq!(storage.syncs.load(Ordering::SeqCst), 0);

    // Block 1 was committed before the failed sync; block 2 never was.
    let reopened = open(SIMPLE_SCHEMA, storage, 3);
    assert_eq!(reopened.latest_block(), 1);
}

/// The fork rolls back batches that wait for their ack: the raw row of
/// block 3 is only in an unacked batch, and so is the reducer checkpoint of
/// blocks 1 and 2.
#[test]
fn fork_rolls_back_unacked_batches() {
    let storage = Arc::new(MemoryBackend::new());
    let mut db = open(DEX_SCHEMA, storage.clone(), 4);

    let finalized_blocks = vec![
        ("trades".into(), 1, vec![make_trade("alice", "buy", 10.0, 2000.0)]),
        ("trades".into(), 2, vec![make_trade("alice", "buy", 10.0, 2100.0)]),
    ];
    ingest_with_finalized_no_ack(&mut db, finalized_blocks, 2).unwrap();
    let old_block = vec![("trades".into(), 3, vec![make_trade("alice", "sell", 5.0, 2200.0)])];
    ingest_with_finalized_no_ack(&mut db, old_block, 2).unwrap();

    let fork = db.handle_fork(vec![cursor(2)]).unwrap().batch.unwrap();
    let deleted: Vec<_> = fork
        .records_for("trades")
        .iter()
        .map(|r| (r.operation.clone(), r.key("block_number").cloned()))
        .collect();
    assert_eq!(deleted, vec![(ChangeOp::Delete, Some(Value::UInt64(3)))]);

    let new_block = vec![("trades".into(), 3, vec![make_trade("alice", "sell", 4.0, 2300.0)])];
    let batch = ingest_with_finalized_no_ack(&mut db, new_block, 2).unwrap().unwrap();
    let summary = &batch.records_for("position_summary")[0];
    assert_eq!(summary.get("current_position"), Some(&Value::Float64(16.0)));
}

/// The reducer fails on an amount of 13.
#[cfg(feature = "lua")]
const FAILING_REDUCER_SCHEMA: &str = r#"
    CREATE TABLE swaps (
        block_number UInt64,
        pool String,
        amount UInt64
    );

    CREATE REDUCER running
    SOURCE swaps
    GROUP BY pool
    STATE (
        total UInt64 DEFAULT 0
    )
    LANGUAGE lua
    PROCESS $$
        if row.amount == 13 then error("unlucky amount") end
        state.total = state.total + row.amount
        emit({ pool = row.pool, total = state.total })
    $$;

    CREATE MATERIALIZED VIEW pool_total AS
    SELECT pool, last(total) AS total
    FROM running
    GROUP BY pool;
"#;

#[cfg(feature = "lua")]
fn pool_rows(amounts: &[(BlockNumber, u64)]) -> Vec<(String, BlockNumber, Vec<RowMap>)> {
    amounts
        .iter()
        .map(|&(block, amount)| {
            let row = RowMap::from([
                ("pool".to_string(), Value::String("A".into())),
                ("amount".to_string(), Value::UInt64(amount)),
            ]);
            ("swaps".to_string(), block, vec![row])
        })
        .collect()
}

/// A failed ingest undoes block 4 back to the state at block 3, which only
/// the checkpoint of the unacked first batch holds.
#[cfg(feature = "lua")]
#[test]
fn failed_ingest_restores_state_from_unacked_checkpoint() {
    let mut db = open(FAILING_REDUCER_SCHEMA, Arc::new(MemoryBackend::new()), 2);
    ingest_with_finalized_no_ack(&mut db, pool_rows(&[(1, 1), (2, 2), (3, 3)]), 3).unwrap();

    assert!(ingest_with_finalized_no_ack(&mut db, pool_rows(&[(4, 5), (5, 13)]), 3).is_err());

    let batch = ingest_with_finalized_no_ack(&mut db, pool_rows(&[(4, 5), (5, 7)]), 3)
        .unwrap()
        .unwrap();
    let total = batch.records_for("pool_total")[0].get("total").cloned();
    assert_eq!(total, Some(Value::Int64(18)));
}
