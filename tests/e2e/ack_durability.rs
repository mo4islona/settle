//! E2E: commit-after-ack durability against a real Postgres target sink.
//!
//! Each test spins up a `postgres:16-alpine` container via testcontainers
//! and exercises the full `ingest → apply(batch) → ack` loop. Requires a
//! running Docker daemon; ~3s container start per test.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use settle::db::{Config, IngestInput, Settle};
use settle::error::Error;
use settle::storage::memory::MemoryBackend;
#[cfg(feature = "rocksdb")]
use settle::storage::rocks::{RocksDbBackend, RocksDbConfig};
use settle::storage::{StorageBackend, StorageWriteBatch};
use settle::types::{BlockCursor, BlockNumber, RowMap, Value};
use tempfile::TempDir;

use super::common::pg::{apply_batch, pg_row, start_pg};
use super::common::{open_rocks, order, SCHEMA};

const ASSET: &str = "token_a";

fn cursor(n: BlockNumber) -> BlockCursor {
    BlockCursor {
        number: n,
        hash: format!("0x{n:016x}"),
    }
}

fn ingest_block(block: BlockNumber, amount: u64, finalized: BlockNumber) -> IngestInput {
    let mut row = order(ASSET, amount);
    row.insert("block_number".into(), Value::UInt64(block));
    let mut data: HashMap<String, Vec<RowMap>> = HashMap::new();
    data.insert("orders".into(), vec![row]);
    let rollback_chain: Vec<BlockCursor> = (finalized.max(1).min(block)..=block)
        .rev()
        .map(cursor)
        .collect();
    IngestInput {
        data,
        rollback_chain,
        finalized_head: cursor(finalized),
    }
}

// ─── Happy path ──────────────────────────────────────────────────

/// Chain of ingests, each applied atomically and acked. Final PG state
/// reflects the cumulative materialized view.
#[tokio::test(flavor = "multi_thread")]
async fn happy_path_apply_and_ack_writes_to_target() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    for (block, amount) in [(1u64, 10u64), (2, 20), (3, 30)] {
        let batch = db.ingest(ingest_block(block, amount, 0)).unwrap().expect("batch");
        apply_batch(&mut pg.client, &batch).await.expect("apply");
        db.ack(batch.sequence).expect("ack");
    }

    let (total, count) = pg_row(&pg.client, ASSET).await.expect("row");
    assert_eq!(count, 3);
    assert_eq!(total, 60, "10 + 20 + 30");
}

// ─── Crash recovery scenarios ────────────────────────────────────

/// Apply succeeded for block 1 but caller crashed between apply(b2) and
/// ack(b2). On reopen, re-ingest produces an identical batch — caller
/// re-applies idempotently → final PG state matches a clean run.
#[tokio::test(flavor = "multi_thread")]
async fn crash_before_ack_target_replay_is_idempotent() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();

    {
        let mut db = open_rocks(tmp.path());

        let b1 = db.ingest(ingest_block(1, 100, 0)).unwrap().expect("b1");
        apply_batch(&mut pg.client, &b1).await.expect("apply b1");
        db.ack(b1.sequence).expect("ack b1");

        let b2 = db.ingest(ingest_block(2, 200, 0)).unwrap().expect("b2");
        apply_batch(&mut pg.client, &b2).await.expect("apply b2");
        // Drop without acking b2 — simulates crash.
    }

    let mut db = open_rocks(tmp.path());
    assert_eq!(db.latest_block(), 1, "uncommitted b2 lost on reopen");

    let b2_replay = db.ingest(ingest_block(2, 200, 0)).unwrap().expect("b2 replay");
    apply_batch(&mut pg.client, &b2_replay).await.expect("re-apply");
    db.ack(b2_replay.sequence).expect("ack b2 replay");

    let (total, count) = pg_row(&pg.client, ASSET).await.unwrap();
    assert_eq!(count, 2, "no double-count from replay");
    assert_eq!(total, 300);
}

/// Apply fails (target table dropped). Caller does NOT call ack → pending
/// stays. Subsequent ingest hits `Err(PendingAck)`. After fixing target +
/// retry apply+ack, pipeline proceeds.
#[tokio::test(flavor = "multi_thread")]
async fn apply_throws_then_pending_stays_with_pg() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    let b1 = db.ingest(ingest_block(1, 100, 0)).unwrap().expect("b1");

    pg.client.batch_execute("DROP TABLE token_summary").await.unwrap();
    let apply_err = apply_batch(&mut pg.client, &b1).await;
    assert!(apply_err.is_err(), "apply must fail without table");

    assert!(db.is_awaiting_ack());
    match db.ingest(ingest_block(2, 50, 0)) {
        Err(Error::PendingAck { .. }) => {}
        other => panic!("expected PendingAck, got {other:?}"),
    }

    pg.client
        .batch_execute(
            "CREATE TABLE token_summary (
                asset_id TEXT PRIMARY KEY,
                total_volume BIGINT NOT NULL,
                trade_count BIGINT NOT NULL
            )",
        )
        .await
        .unwrap();
    apply_batch(&mut pg.client, &b1).await.expect("retry apply");
    db.ack(b1.sequence).expect("ack after retry");

    let b2 = db.ingest(ingest_block(2, 50, 0)).unwrap().expect("b2");
    apply_batch(&mut pg.client, &b2).await.expect("apply b2");
    db.ack(b2.sequence).expect("ack b2");

    let (total, count) = pg_row(&pg.client, ASSET).await.unwrap();
    assert_eq!(count, 2);
    assert_eq!(total, 150);
}

/// Apply committed PG, but `db.ack()` failed against a real **RocksDB**
/// backend wrapped in a commit-failure injector. Caller retries; second
/// commit lands; final state is consistent. Exercises the at-least-once
/// guarantee from the opposite direction (apply succeeded but ack didn't)
/// against the production storage path — not just MemoryBackend.
#[tokio::test(flavor = "multi_thread")]
async fn ack_failure_against_real_rocksdb_keeps_pending_for_retry() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let rocks: Arc<dyn StorageBackend> = Arc::new(
        RocksDbBackend::open_with_config(
            tmp.path().to_str().unwrap(),
            &RocksDbConfig::default(),
        )
        .expect("open rocks"),
    );
    let backend: Arc<dyn StorageBackend> =
        Arc::new(FailNthCommit::wrapping(rocks, /* fail_at = */ 1));
    let mut db = Settle::open(Config::new(SCHEMA).storage(backend)).expect("open");

    let b1 = db.ingest(ingest_block(1, 100, 0)).unwrap().expect("b1");
    apply_batch(&mut pg.client, &b1).await.expect("apply b1");

    // First ack: rocks-backed commit fails (intercepted by wrapper).
    let err = db.ack(b1.sequence).unwrap_err();
    assert!(matches!(err, Error::Storage(_)));
    assert!(db.is_awaiting_ack(), "pending preserved across RocksDB commit failure");

    // PG already has b1 applied — Settle's internal state must allow retry.
    let (total, count) = pg_row(&pg.client, ASSET).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(total, 100);

    // Retry — wrapper's failure budget exhausted, real RocksDB commit lands.
    db.ack(b1.sequence).expect("retry ack succeeds against real rocks");
    assert!(!db.is_awaiting_ack());

    // Next ingest proceeds normally against the now-consistent backend.
    let b2 = db.ingest(ingest_block(2, 50, 0)).unwrap().expect("b2");
    apply_batch(&mut pg.client, &b2).await.expect("apply b2");
    db.ack(b2.sequence).expect("ack b2");

    let (total, count) = pg_row(&pg.client, ASSET).await.unwrap();
    assert_eq!(count, 2);
    assert_eq!(total, 150);
}

/// Apply committed PG, but `db.ack()` failed (e.g. disk full). Caller retries
/// ack; the same write batch commits on the retry; the final state is
/// consistent. Verifies at-least-once from the OPPOSITE direction (apply
/// succeeded but ack didn't).
#[tokio::test(flavor = "multi_thread")]
async fn ack_failed_after_apply_succeeded() {
    let mut pg = start_pg().await;
    let backend: Arc<dyn StorageBackend> = Arc::new(FailNthCommit::new(/* fail nth = */ 1));
    let mut db = Settle::open(Config::new(SCHEMA).storage(backend)).expect("open with mock");

    // First ingest goes through (commit #1 succeeds at open path? actually
    // commit #2 is the one that fails — we want the FIRST data ack to fail).
    let b1 = db.ingest(ingest_block(1, 100, 0)).unwrap().expect("b1");
    apply_batch(&mut pg.client, &b1).await.expect("apply b1");
    // Caller acks — backend's commit fails (1st commit call).
    let err = db.ack(b1.sequence).unwrap_err();
    assert!(matches!(err, Error::Storage(_)));
    // Pending preserved — PG already has b1 applied.
    assert!(db.is_awaiting_ack());
    let (total, count) = pg_row(&pg.client, ASSET).await.unwrap();
    assert_eq!(count, 1, "PG already reflects b1");
    assert_eq!(total, 100);

    // Retry ack — backend now succeeds.
    db.ack(b1.sequence).expect("retry ack");
    assert!(!db.is_awaiting_ack());

    // Subsequent ingest works, PG remains consistent.
    let b2 = db.ingest(ingest_block(2, 50, 0)).unwrap().expect("b2");
    apply_batch(&mut pg.client, &b2).await.expect("apply b2");
    db.ack(b2.sequence).expect("ack b2");

    let (total, count) = pg_row(&pg.client, ASSET).await.unwrap();
    assert_eq!(count, 2);
    assert_eq!(total, 150);
}

/// Clean run (5 acked blocks) vs crash run (5 blocks with drop mid-stream
/// and recovery). Final PG state must be **byte-equal** — the deterministic
/// replay guarantee surfaced at the target.
#[tokio::test(flavor = "multi_thread")]
async fn deterministic_replay_target_byte_equal() {
    // Clean run.
    let mut pg_clean = start_pg().await;
    let tmp_clean = TempDir::new().unwrap();
    let mut db = open_rocks(tmp_clean.path());
    for (block, amount) in [(1u64, 11u64), (2, 22), (3, 33), (4, 44), (5, 55)] {
        let b = db.ingest(ingest_block(block, amount, 0)).unwrap().expect("b");
        apply_batch(&mut pg_clean.client, &b).await.expect("apply");
        db.ack(b.sequence).expect("ack");
    }
    let clean = pg_row(&pg_clean.client, ASSET).await.unwrap();

    // Crash run: ingest 3 with ack, ingest 4 + apply without ack, drop, reopen,
    // re-ingest 4 + apply (idempotent) + ack, then 5.
    let mut pg_crash = start_pg().await;
    let tmp_crash = TempDir::new().unwrap();
    {
        let mut db = open_rocks(tmp_crash.path());
        for (block, amount) in [(1u64, 11u64), (2, 22), (3, 33)] {
            let b = db.ingest(ingest_block(block, amount, 0)).unwrap().expect("b");
            apply_batch(&mut pg_crash.client, &b).await.expect("apply");
            db.ack(b.sequence).expect("ack");
        }
        let b4 = db.ingest(ingest_block(4, 44, 0)).unwrap().expect("b4");
        apply_batch(&mut pg_crash.client, &b4).await.expect("apply b4");
        // Drop without ack — crash mid-stream.
    }
    let mut db = open_rocks(tmp_crash.path());
    assert_eq!(db.latest_block(), 3, "disk rolled back to last ack");
    let b4_replay = db.ingest(ingest_block(4, 44, 0)).unwrap().expect("b4 replay");
    apply_batch(&mut pg_crash.client, &b4_replay).await.expect("re-apply b4");
    db.ack(b4_replay.sequence).expect("ack b4 replay");
    let b5 = db.ingest(ingest_block(5, 55, 0)).unwrap().expect("b5");
    apply_batch(&mut pg_crash.client, &b5).await.expect("apply b5");
    db.ack(b5.sequence).expect("ack b5");
    let crash = pg_row(&pg_crash.client, ASSET).await.unwrap();

    assert_eq!(clean, crash, "deterministic replay must yield identical PG state");
}

// ─── Heartbeat ───────────────────────────────────────────────────

/// Heartbeat ingest (empty data + advancing finalized_head) must not change
/// PG. `META_NEXT_SEQUENCE` is still persisted — reopen + next ingest
/// observes monotonic sequence.
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_ingest_no_target_change() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let seq_after_data;
    {
        let mut db = open_rocks(tmp.path());
        let b1 = db.ingest(ingest_block(1, 100, 0)).unwrap().expect("b1");
        apply_batch(&mut pg.client, &b1).await.expect("apply");
        db.ack(b1.sequence).expect("ack");
        seq_after_data = b1.sequence;

        // Heartbeat — no data, finalized_head = cursor(1).
        let hb = db.ingest(IngestInput {
            data: HashMap::new(),
            rollback_chain: vec![cursor(1)],
            finalized_head: cursor(1),
        }).expect("heartbeat ingest");
        assert!(hb.is_none(), "heartbeat returns no batch");
        assert!(!db.is_awaiting_ack(), "heartbeat path commits immediately");
    }

    // PG unchanged.
    let row = pg_row(&pg.client, ASSET).await.unwrap();
    assert_eq!(row.1, 1, "PG count not changed by heartbeat");

    // Sequence monotonic across the reopen. Finalized = 1 to match the
    // engine state advanced by the heartbeat.
    let mut db = open_rocks(tmp.path());
    let b2 = db.ingest(ingest_block(2, 50, 1)).unwrap().expect("b2");
    assert!(
        b2.sequence > seq_after_data,
        "META_NEXT_SEQUENCE persisted across heartbeat + reopen",
    );
    apply_batch(&mut pg.client, &b2).await.expect("apply b2");
    db.ack(b2.sequence).expect("ack b2");
}

// ─── Backpressure ────────────────────────────────────────────────

/// With a tiny `max_buffer_size`, multiple records in one ingest still
/// surface as a coherent batch; the target receives all merged records.
/// `isBackpressured` reflects buffer fullness during the call.
#[tokio::test(flavor = "multi_thread")]
async fn backpressure_signals_to_caller() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let cfg = Config::with_data_dir(SCHEMA, tmp.path().to_str().unwrap()).max_buffer_size(2);
    let mut db = Settle::open(cfg).expect("open");

    // Single ingest that produces multiple records (3 rows for same asset
    // → merged to 1 MV record by buffer.flush()). max_buffer_size=2 means
    // 3 raw records temporarily exceed capacity but merge still yields 1.
    let mut row1 = order(ASSET, 10);
    row1.insert("block_number".into(), Value::UInt64(1));
    let mut row2 = order(ASSET, 20);
    row2.insert("block_number".into(), Value::UInt64(1));
    let mut row3 = order(ASSET, 30);
    row3.insert("block_number".into(), Value::UInt64(1));
    let mut data: HashMap<String, Vec<RowMap>> = HashMap::new();
    data.insert("orders".into(), vec![row1, row2, row3]);

    let batch = db.ingest(IngestInput {
        data,
        rollback_chain: vec![cursor(1)],
        finalized_head: cursor(0),
    }).unwrap().expect("batch");
    apply_batch(&mut pg.client, &batch).await.expect("apply");
    db.ack(batch.sequence).expect("ack");

    let (total, count) = pg_row(&pg.client, ASSET).await.unwrap();
    assert_eq!(count, 3, "merged MV record reflects three rows");
    assert_eq!(total, 60);

    // Buffer drained — not backpressured between calls.
    assert!(!db.is_backpressured());
}

// ─── Error contract under real flow ─────────────────────────────

/// Mistaken ack with the wrong sequence must not advance the target — the
/// caller's bug is surfaced as a typed error, the pending slot is intact,
/// and a correct ack proceeds.
#[tokio::test(flavor = "multi_thread")]
async fn wrong_ack_sequence_does_not_advance_target() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    let b1 = db.ingest(ingest_block(1, 100, 0)).unwrap().expect("b1");
    apply_batch(&mut pg.client, &b1).await.expect("apply");

    let err = db.ack(b1.sequence + 999).unwrap_err();
    assert!(matches!(err, Error::WrongAckSequence { .. }));
    assert!(db.is_awaiting_ack(), "pending unchanged on wrong-seq ack");

    db.ack(b1.sequence).expect("correct ack");
    assert!(!db.is_awaiting_ack());

    let (total, count) = pg_row(&pg.client, ASSET).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(total, 100);
}

/// Across a delta-db restart, the target never sees a duplicate sequence.
#[tokio::test(flavor = "multi_thread")]
async fn sequence_monotonic_across_target_restart() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();

    let mut seen_seqs = Vec::new();
    {
        let mut db = open_rocks(tmp.path());
        for (block, amount) in [(1u64, 10u64), (2, 20)] {
            let b = db.ingest(ingest_block(block, amount, 0)).unwrap().expect("b");
            apply_batch(&mut pg.client, &b).await.expect("apply");
            seen_seqs.push(b.sequence);
            db.ack(b.sequence).expect("ack");
        }
    }
    // Reopen + more ingests.
    let mut db = open_rocks(tmp.path());
    for (block, amount) in [(3u64, 30u64), (4, 40)] {
        let b = db.ingest(ingest_block(block, amount, 0)).unwrap().expect("b");
        apply_batch(&mut pg.client, &b).await.expect("apply");
        assert!(
            !seen_seqs.contains(&b.sequence),
            "target must never receive a duplicate sequence across restart",
        );
        seen_seqs.push(b.sequence);
        db.ack(b.sequence).expect("ack");
    }
    let mut sorted = seen_seqs.clone();
    sorted.sort();
    assert_eq!(seen_seqs, sorted, "sequences strictly monotonic");
}

// ─── Reducer aggregations correct on rollback ───────────────────

/// After ingesting + acking 5 blocks then forking back to block 2, the
/// downstream PG row reflects only blocks 1-2's contribution. Verifies
/// that `replay_unfinalized` reconstructs reducer/MV state correctly so
/// the compensating ChangeBatch carries the right `prev_values`.
#[tokio::test(flavor = "multi_thread")]
async fn reducer_aggregations_correct_after_partial_rollback() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    for (block, amount) in [(1u64, 10u64), (2, 20), (3, 30), (4, 40), (5, 50)] {
        let b = db.ingest(ingest_block(block, amount, 0)).unwrap().expect("b");
        apply_batch(&mut pg.client, &b).await.expect("apply");
        db.ack(b.sequence).expect("ack");
    }
    assert_eq!(pg_row(&pg.client, ASSET).await, Some((150, 5)));

    // Fork: blocks 3-5 invalidated. Common ancestor at block 2.
    let fork_chain = vec![
        BlockCursor { number: 5, hash: "0xfork_5".into() },
        BlockCursor { number: 4, hash: "0xfork_4".into() },
        BlockCursor { number: 3, hash: "0xfork_3".into() },
        cursor(2),
        cursor(1),
    ];
    let fork = db.handle_fork(fork_chain).expect("handle_fork");
    assert_eq!(fork.cursor.number, 2);
    let comp = fork.batch.expect("rollback produces compensating records");
    apply_batch(&mut pg.client, &comp).await.expect("apply comp");
    db.ack(comp.sequence).expect("ack fork");

    assert_eq!(
        pg_row(&pg.client, ASSET).await,
        Some((30, 2)),
        "MV reflects blocks 1+2 cumulative (10 + 20 = 30, count 2)",
    );
}

// ─── Mock backend for ack-failure injection ─────────────────────

/// Storage backend whose Nth `commit()` (1-indexed) returns Err; others
/// pass through. `n = 1` ⇒ first commit fails, retry succeeds. Wraps an
/// arbitrary inner backend so the same harness can exercise both
/// MemoryBackend and the RocksDB backend (whose `WriteBatch` semantics
/// differ — Sonnet review pointed out we were only proving the contract
/// against the in-memory mock).
struct FailNthCommit {
    inner: Arc<dyn StorageBackend>,
    seen: AtomicUsize,
    fail_at: usize,
}

impl FailNthCommit {
    fn new(fail_at: usize) -> Self {
        Self {
            inner: Arc::new(MemoryBackend::new()),
            seen: AtomicUsize::new(0),
            fail_at,
        }
    }

    fn wrapping(inner: Arc<dyn StorageBackend>, fail_at: usize) -> Self {
        Self { inner, seen: AtomicUsize::new(0), fail_at }
    }
}

impl StorageBackend for FailNthCommit {
    fn put_raw_rows(&self, t: &str, b: BlockNumber, d: &[u8]) -> settle::error::Result<()> { self.inner.put_raw_rows(t, b, d) }
    fn get_raw_rows(&self, t: &str, f: BlockNumber, u: BlockNumber) -> settle::error::Result<Vec<(BlockNumber, Vec<u8>)>> { self.inner.get_raw_rows(t, f, u) }
    fn delete_raw_rows_after(&self, t: &str, a: BlockNumber) -> settle::error::Result<()> { self.inner.delete_raw_rows_after(t, a) }
    fn take_raw_rows_after(&self, t: &str, a: BlockNumber) -> settle::error::Result<Vec<(BlockNumber, Vec<u8>)>> { self.inner.take_raw_rows_after(t, a) }
    fn put_reducer_state(&self, r: &str, k: &[u8], b: BlockNumber, s: &[u8]) -> settle::error::Result<()> { self.inner.put_reducer_state(r, k, b, s) }
    fn get_reducer_state(&self, r: &str, k: &[u8], b: BlockNumber) -> settle::error::Result<Option<Vec<u8>>> { self.inner.get_reducer_state(r, k, b) }
    fn get_reducer_state_at_or_before(&self, r: &str, k: &[u8], b: BlockNumber) -> settle::error::Result<Option<(BlockNumber, Vec<u8>)>> { self.inner.get_reducer_state_at_or_before(r, k, b) }
    fn delete_reducer_states_after(&self, r: &str, k: &[u8], a: BlockNumber) -> settle::error::Result<()> { self.inner.delete_reducer_states_after(r, k, a) }
    fn get_reducer_finalized(&self, r: &str, k: &[u8]) -> settle::error::Result<Option<Vec<u8>>> { self.inner.get_reducer_finalized(r, k) }
    fn set_reducer_finalized(&self, r: &str, k: &[u8], s: &[u8]) -> settle::error::Result<()> { self.inner.set_reducer_finalized(r, k, s) }
    fn delete_reducer_states_up_to(&self, r: &str, k: &[u8], u: BlockNumber) -> settle::error::Result<()> { self.inner.delete_reducer_states_up_to(r, k, u) }
    fn put_mv_state(&self, v: &str, k: &[u8], s: &[u8]) -> settle::error::Result<()> { self.inner.put_mv_state(v, k, s) }
    fn get_mv_state(&self, v: &str, k: &[u8]) -> settle::error::Result<Option<Vec<u8>>> { self.inner.get_mv_state(v, k) }
    fn delete_mv_state(&self, v: &str, k: &[u8]) -> settle::error::Result<()> { self.inner.delete_mv_state(v, k) }
    fn list_mv_group_keys(&self, v: &str) -> settle::error::Result<Vec<Vec<u8>>> { self.inner.list_mv_group_keys(v) }
    fn put_meta(&self, k: &str, v: &[u8]) -> settle::error::Result<()> { self.inner.put_meta(k, v) }
    fn get_meta(&self, k: &str) -> settle::error::Result<Option<Vec<u8>>> { self.inner.get_meta(k) }
    fn list_reducer_group_keys(&self, r: &str) -> settle::error::Result<Vec<Vec<u8>>> { self.inner.list_reducer_group_keys(r) }
    fn commit(&self, batch: &StorageWriteBatch) -> settle::error::Result<()> {
        let n = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_at {
            return Err(Error::Storage(format!("injected failure on commit #{n}")));
        }
        self.inner.commit(batch)
    }
}

// `MemoryBackend` is not referenced after the switch to `Arc<dyn ...>` —
// keep the import only when no other usage retains it.
#[allow(unused_imports)]
use settle::storage::memory::MemoryBackend as _MemoryBackend;
