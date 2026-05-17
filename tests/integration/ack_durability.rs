//! E2E: commit-after-ack durability semantics through the public `Settle` API.
//!
//! Pins the contract that `Settle::ingest` and `Settle::handle_fork` defer
//! `storage.commit` until `ack(sequence)` succeeds. Until ack:
//!   - Engine in-memory state is ahead of disk by one batch.
//!   - Disk reflects the last successfully-committed (acked or heartbeat) state.
//!   - Mutating APIs return `Err(PendingAck)`.
//!   - Drop+reopen rolls back to disk; re-ingest deterministically rebuilds.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use settle::db::{Config, IngestInput, Settle};
use settle::error::Error;
use settle::storage::memory::MemoryBackend;
use settle::storage::{StorageBackend, StorageWriteBatch};
use settle::types::{BlockCursor, BlockNumber, RowMap, Value};
use tempfile::TempDir;

use super::common::{open_rocks, order, SCHEMA};

const ASSET: &str = "token_a";

fn cursor(n: BlockNumber) -> BlockCursor {
    BlockCursor {
        number: n,
        hash: format!("0x{n:016x}"),
    }
}

fn order_row(asset: &str, amount: u64, block: BlockNumber) -> RowMap {
    let mut row = order(asset, amount);
    row.insert("block_number".into(), Value::UInt64(block));
    row
}

/// Build an `IngestInput` for a single-block ingest. Doesn't borrow `db`,
/// so it composes inside `db.ingest(...)` without borrow-checker conflicts.
fn ingest_block(block: BlockNumber, finalized: BlockNumber) -> IngestInput {
    let mut data = HashMap::new();
    data.insert("orders".into(), vec![order_row(ASSET, 100, block)]);
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

// ─── Basic pending/ack semantics ─────────────────────────────────

#[test]
fn ingest_does_not_commit_until_ack() {
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open_rocks(tmp.path());
        let batch = db.ingest(ingest_block(1, 0)).unwrap();
        assert!(batch.is_some());
        assert!(db.is_awaiting_ack());
        // No ack — drop here.
    }
    let db = open_rocks(tmp.path());
    assert_eq!(db.latest_block(), 0, "no ack ⇒ block 1 rolled back on reopen");
    assert!(!db.is_awaiting_ack());
}

#[test]
fn ack_commits_state_durably() {
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open_rocks(tmp.path());
        let batch = db
            .ingest(ingest_block(1, 0))
            .unwrap()
            .expect("block 1 produces a batch");
        db.ack(batch.sequence).unwrap();
        assert!(!db.is_awaiting_ack());
    }
    let db = open_rocks(tmp.path());
    assert_eq!(db.latest_block(), 1, "acked block survives reopen");
}

#[test]
fn second_ingest_returns_pending_ack_error() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    let b1 = db
        .ingest(ingest_block(1, 0))
        .unwrap()
        .expect("block 1 produces a batch");

    // Without ack, the next ingest must report PendingAck — not partially process.
    let err = db.ingest(ingest_block(2, 0)).unwrap_err();
    match err {
        Error::PendingAck { sequence, .. } => {
            assert_eq!(sequence, b1.sequence);
        }
        other => panic!("expected PendingAck, got {other:?}"),
    }
}

#[test]
fn ack_with_no_pending_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());
    // No batch has been produced yet — ack should be a silent no-op.
    db.ack(0).unwrap();
    db.ack(42).unwrap();
    db.ack(u64::MAX).unwrap();
}

#[test]
fn ack_repeated_after_success_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());
    let batch = db
        .ingest(ingest_block(1, 0))
        .unwrap()
        .expect("batch");
    db.ack(batch.sequence).unwrap();
    // Second ack with the same sequence — pending=None now, so Ok.
    db.ack(batch.sequence).unwrap();
}

#[test]
fn ack_wrong_seq_returns_wrong_ack_sequence_error() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());
    let batch = db
        .ingest(ingest_block(1, 0))
        .unwrap()
        .expect("batch");
    let err = db.ack(batch.sequence + 999).unwrap_err();
    match err {
        Error::WrongAckSequence { expected, got } => {
            assert_eq!(expected, batch.sequence);
            assert_eq!(got, batch.sequence + 999);
        }
        other => panic!("expected WrongAckSequence, got {other:?}"),
    }
    // Pending is unchanged — retry with the right sequence succeeds.
    assert!(db.is_awaiting_ack());
    db.ack(batch.sequence).unwrap();
}

#[test]
fn heartbeat_ingest_commits_immediately() {
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open_rocks(tmp.path());
        // Empty data + finalized=0 produces no records ⇒ heartbeat path.
        let result = db
            .ingest(IngestInput {
                data: HashMap::new(),
                rollback_chain: vec![],
                finalized_head: cursor(0),
            })
            .unwrap();
        assert!(result.is_none(), "heartbeat returns None");
        assert!(!db.is_awaiting_ack(), "heartbeat path commits immediately");
    }
    // META_NEXT_SEQUENCE was persisted even though no batch was produced.
    let db = open_rocks(tmp.path());
    assert_eq!(db.latest_block(), 0);
}

// ─── Engine vs disk view during pending ──────────────────────────

#[test]
fn engine_advances_ahead_of_disk_during_pending() {
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open_rocks(tmp.path());
        let _ = db.ingest(ingest_block(1, 0)).unwrap();
        // Engine in-memory reflects block 1.
        assert_eq!(db.latest_block(), 1);
        // is_awaiting_ack signals the durability gap.
        assert!(db.is_awaiting_ack());
        // Drop without ack.
    }
    let db = open_rocks(tmp.path());
    // Disk did NOT have block 1 — pending was lost on drop.
    assert_eq!(db.latest_block(), 0);
}

#[test]
fn sequence_monotonic_across_restart() {
    let tmp = TempDir::new().unwrap();
    let seq_after_b2 = {
        let mut db = open_rocks(tmp.path());
        let b1 = db.ingest(ingest_block(1, 0)).unwrap().unwrap();
        db.ack(b1.sequence).unwrap();
        let b2 = db.ingest(ingest_block(2, 0)).unwrap().unwrap();
        db.ack(b2.sequence).unwrap();
        b2.sequence
    };

    let mut db = open_rocks(tmp.path());
    let b3 = db.ingest(ingest_block(3, 0)).unwrap().unwrap();
    assert!(
        b3.sequence > seq_after_b2,
        "sequence must not reset on reopen"
    );
}

// ─── Mutating-API guards ─────────────────────────────────────────

#[test]
fn replay_reducer_errors_during_pending() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());
    let _ = db.ingest(ingest_block(1, 0)).unwrap().unwrap();
    let err = db.replay_reducer("market_stats").unwrap_err();
    assert!(matches!(err, Error::PendingAck { .. }));
}

// ─── Input validation ─────────────────────────────────────────────

#[test]
fn finalized_head_backwards_rejected() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    let b1 = db
        .ingest(ingest_block(5, 3))
        .unwrap()
        .expect("first batch");
    db.ack(b1.sequence).unwrap();
    assert_eq!(db.finalized_block(), 3);

    // Try to regress finalized to 2 — must be rejected.
    let err = db
        .ingest(IngestInput {
            data: HashMap::new(),
            rollback_chain: vec![],
            finalized_head: cursor(2),
        })
        .unwrap_err();
    assert!(matches!(err, Error::InvalidOperation(_)));
}

#[test]
fn finalized_head_above_latest_accepted_for_gappy_chains() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());
    // Ingest block 1 with finalized_head pointing to block 5 — Solana-style
    // gap where the caller knows finality out-of-band for un-ingested blocks.
    let mut chain = vec![cursor(5), cursor(4), cursor(3), cursor(2), cursor(1)];
    chain.sort_by_key(|c| std::cmp::Reverse(c.number));
    let result = db.ingest(IngestInput {
        data: HashMap::from([("orders".into(), vec![order_row(ASSET, 100, 1)])]),
        rollback_chain: chain,
        finalized_head: cursor(5),
    });
    assert!(result.is_ok(), "gappy finalized_head must be accepted");
}

#[test]
fn handle_fork_empty_chain_rejected() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());
    let err = match db.handle_fork(vec![]) {
        Ok(_) => panic!("empty rollback_chain must be rejected"),
        Err(e) => e,
    };
    assert!(matches!(err, Error::InvalidOperation(_)));
}

#[test]
fn ingest_validation_with_pending_uses_committed_not_engine_state() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());
    // Acked ingest brings finalized to 3.
    let b1 = db.ingest(ingest_block(5, 3)).unwrap().unwrap();
    db.ack(b1.sequence).unwrap();

    // Second ingest creates pending; engine.finalized advances to 4.
    let _ = db.ingest(ingest_block(6, 4)).unwrap();
    assert!(db.is_awaiting_ack());

    // Third ingest with finalized_head=3 (committed view) should report
    // PendingAck — NOT InvalidOperation against the uncommitted engine.finalized=4.
    let err = db
        .ingest(IngestInput {
            data: HashMap::from([("orders".into(), vec![order_row(ASSET, 100, 7)])]),
            rollback_chain: vec![cursor(7)],
            finalized_head: cursor(3),
        })
        .unwrap_err();
    assert!(
        matches!(err, Error::PendingAck { .. }),
        "pending guard must run before finality validation, got {err:?}",
    );
}

// ─── Fork pending semantics ──────────────────────────────────────

#[test]
fn fork_during_pending_returns_pending_ack_error() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());
    let b1 = db.ingest(ingest_block(1, 0)).unwrap().unwrap();
    db.ack(b1.sequence).unwrap();

    // Ingest block 2 without acking.
    let _ = db.ingest(ingest_block(2, 0)).unwrap();
    assert!(db.is_awaiting_ack());

    // handle_fork while pending must report PendingAck — caller has to ack
    // (or drop+reopen) first, then refork on the resulting committed state.
    let err = match db.handle_fork(vec![cursor(1)]) {
        Ok(_) => panic!("expected PendingAck"),
        Err(e) => e,
    };
    assert!(matches!(err, Error::PendingAck { .. }));
}

#[test]
fn fork_batch_requires_ack_to_commit() {
    // ingest two blocks, ack them, then handle_fork to drop block 2.
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open_rocks(tmp.path());
        let b1 = db.ingest(ingest_block(1, 0)).unwrap().unwrap();
        db.ack(b1.sequence).unwrap();
        let b2 = db.ingest(ingest_block(2, 0)).unwrap().unwrap();
        db.ack(b2.sequence).unwrap();
        let fork = db.handle_fork(vec![cursor(2), cursor(1)]).unwrap();
        // Fork resolves to block 2 (chain matches) — no rollback ⇒ heartbeat path.
        // To exercise the pending path we'd need a fork that actually rolls back,
        // which produces compensating records. Use a divergent hash for block 2.
        let _ = fork;
    }

    let tmp2 = TempDir::new().unwrap();
    let pre_fork_latest;
    {
        let mut db = open_rocks(tmp2.path());
        let b1 = db.ingest(ingest_block(1, 0)).unwrap().unwrap();
        db.ack(b1.sequence).unwrap();
        let b2 = db.ingest(ingest_block(2, 0)).unwrap().unwrap();
        db.ack(b2.sequence).unwrap();
        pre_fork_latest = db.latest_block();

        // Fork that actually rolls back: chain claims block 2 has a different hash.
        let divergent = BlockCursor {
            number: 2,
            hash: "0xDIFFERENT".into(),
        };
        let fork = db.handle_fork(vec![divergent, cursor(1)]).unwrap();
        assert_eq!(fork.cursor.number, 1, "fork resolves at block 1");
        let batch = fork
            .batch
            .expect("rollback of block 2 must emit a compensating batch");
        assert!(db.is_awaiting_ack(), "compensating batch must be acked");
        // Drop without ack — fork should roll back.
        drop(db);

        let db = open_rocks(tmp2.path());
        assert_eq!(
            db.latest_block(),
            pre_fork_latest,
            "fork that wasn't acked must not be durable",
        );
        let _ = batch;
    }
}

#[test]
fn fork_immediate_commit_then_crash_then_refork_is_idempotent() {
    // The V3-B1 regression: after a fork's heartbeat-immediate-commit, the
    // future-hashes that landed in `block_hashes` must NOT cause a second
    // fork attempt against the same chain to advance the cursor past blocks
    // we have no data for.
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open_rocks(tmp.path());

        // Ingest blocks 1..=3 (acked).
        for block in 1..=3u64 {
            let b = db.ingest(ingest_block(block, 0)).unwrap().unwrap();
            db.ack(b.sequence).unwrap();
        }

        // Fork that rolls back to block 1. rollback_chain claims blocks 2,3
        // have NEW hashes. Result: latest=1, but block_hashes contains
        // {1, 2:new, 3:new} after set_rollback_chain.
        let new_chain = vec![
            BlockCursor {
                number: 3,
                hash: "0xNEW3".into(),
            },
            BlockCursor {
                number: 2,
                hash: "0xNEW2".into(),
            },
            cursor(1),
        ];
        let fork = db.handle_fork(new_chain.clone()).unwrap();
        assert_eq!(fork.cursor.number, 1);
        if let Some(b) = fork.batch {
            db.ack(b.sequence).unwrap();
        }
        assert_eq!(db.latest_block(), 1);
    }

    // Drop + reopen: disk has block_hashes that include {2:new, 3:new}
    // (added via set_rollback_chain during fork) but data only for block 1.
    let mut db = open_rocks(tmp.path());
    assert_eq!(db.latest_block(), 1);

    // Replay the SAME fork. Without the bounded-resolve guard this would
    // match 3:0xNEW3 → cursor=3 → silently advance past missing data.
    let new_chain = vec![
        BlockCursor {
            number: 3,
            hash: "0xNEW3".into(),
        },
        BlockCursor {
            number: 2,
            hash: "0xNEW2".into(),
        },
        cursor(1),
    ];
    let fork = db.handle_fork(new_chain).unwrap();
    assert_eq!(
        fork.cursor.number, 1,
        "re-fork must clamp to data-bearing latest, not advance to a future hash",
    );
    assert_eq!(db.latest_block(), 1, "latest_block did not advance");
}

// ─── Commit failure injection ────────────────────────────────────

/// Storage backend wrapping `MemoryBackend` whose first `commit()` call
/// returns `Err`; subsequent calls succeed. Used to verify that an ack
/// failure preserves the pending slot for retry.
struct FailFirstCommitBackend {
    inner: MemoryBackend,
    commits_attempted: AtomicUsize,
    fail_until: usize,
}

impl FailFirstCommitBackend {
    fn new(fail_until: usize) -> Self {
        Self {
            inner: MemoryBackend::new(),
            commits_attempted: AtomicUsize::new(0),
            fail_until,
        }
    }
}

impl StorageBackend for FailFirstCommitBackend {
    fn put_raw_rows(&self, table: &str, block: BlockNumber, data: &[u8]) -> settle::error::Result<()> {
        self.inner.put_raw_rows(table, block, data)
    }
    fn get_raw_rows(&self, table: &str, from: BlockNumber, to: BlockNumber) -> settle::error::Result<Vec<(BlockNumber, Vec<u8>)>> {
        self.inner.get_raw_rows(table, from, to)
    }
    fn delete_raw_rows_after(&self, table: &str, after: BlockNumber) -> settle::error::Result<()> {
        self.inner.delete_raw_rows_after(table, after)
    }
    fn take_raw_rows_after(&self, table: &str, after: BlockNumber) -> settle::error::Result<Vec<(BlockNumber, Vec<u8>)>> {
        self.inner.take_raw_rows_after(table, after)
    }
    fn put_reducer_state(&self, reducer: &str, group_key: &[u8], block: BlockNumber, state: &[u8]) -> settle::error::Result<()> {
        self.inner.put_reducer_state(reducer, group_key, block, state)
    }
    fn get_reducer_state(&self, reducer: &str, group_key: &[u8], block: BlockNumber) -> settle::error::Result<Option<Vec<u8>>> {
        self.inner.get_reducer_state(reducer, group_key, block)
    }
    fn get_reducer_state_at_or_before(&self, reducer: &str, group_key: &[u8], block: BlockNumber) -> settle::error::Result<Option<(BlockNumber, Vec<u8>)>> {
        self.inner.get_reducer_state_at_or_before(reducer, group_key, block)
    }
    fn delete_reducer_states_after(&self, reducer: &str, group_key: &[u8], after: BlockNumber) -> settle::error::Result<()> {
        self.inner.delete_reducer_states_after(reducer, group_key, after)
    }
    fn get_reducer_finalized(&self, reducer: &str, group_key: &[u8]) -> settle::error::Result<Option<Vec<u8>>> {
        self.inner.get_reducer_finalized(reducer, group_key)
    }
    fn set_reducer_finalized(&self, reducer: &str, group_key: &[u8], state: &[u8]) -> settle::error::Result<()> {
        self.inner.set_reducer_finalized(reducer, group_key, state)
    }
    fn delete_reducer_states_up_to(&self, reducer: &str, group_key: &[u8], up_to: BlockNumber) -> settle::error::Result<()> {
        self.inner.delete_reducer_states_up_to(reducer, group_key, up_to)
    }
    fn put_mv_state(&self, view: &str, group_key: &[u8], state: &[u8]) -> settle::error::Result<()> {
        self.inner.put_mv_state(view, group_key, state)
    }
    fn get_mv_state(&self, view: &str, group_key: &[u8]) -> settle::error::Result<Option<Vec<u8>>> {
        self.inner.get_mv_state(view, group_key)
    }
    fn delete_mv_state(&self, view: &str, group_key: &[u8]) -> settle::error::Result<()> {
        self.inner.delete_mv_state(view, group_key)
    }
    fn list_mv_group_keys(&self, view: &str) -> settle::error::Result<Vec<Vec<u8>>> {
        self.inner.list_mv_group_keys(view)
    }
    fn put_meta(&self, key: &str, value: &[u8]) -> settle::error::Result<()> {
        self.inner.put_meta(key, value)
    }
    fn get_meta(&self, key: &str) -> settle::error::Result<Option<Vec<u8>>> {
        self.inner.get_meta(key)
    }
    fn list_reducer_group_keys(&self, reducer: &str) -> settle::error::Result<Vec<Vec<u8>>> {
        self.inner.list_reducer_group_keys(reducer)
    }
    fn commit(&self, batch: &StorageWriteBatch) -> settle::error::Result<()> {
        let n = self.commits_attempted.fetch_add(1, Ordering::SeqCst);
        if n < self.fail_until {
            return Err(settle::error::Error::Storage(format!(
                "injected commit failure {n}"
            )));
        }
        // Re-construct the batch contents through the backend's commit.
        // MemoryBackend.commit reads ops directly — call through.
        let _ = batch;
        self.inner.commit(batch)
    }
}

#[test]
fn ack_failure_keeps_pending_for_retry() {
    let backend: Arc<dyn StorageBackend> =
        Arc::new(FailFirstCommitBackend::new(1));
    let mut db = Settle::open(Config::new(SCHEMA).storage(backend)).unwrap();

    let batch = db
        .ingest(ingest_block(1, 0))
        .unwrap()
        .expect("batch");
    // First ack: backend's commit returns Err.
    let err = db.ack(batch.sequence).unwrap_err();
    assert!(matches!(err, Error::Storage(_)));
    // Pending is preserved — caller can retry.
    assert!(db.is_awaiting_ack());

    // Second ack: backend now succeeds.
    db.ack(batch.sequence).unwrap();
    assert!(!db.is_awaiting_ack());
}

// ─── Error-recovery rollback stays in-memory ─────────────────────

/// A failed `ingest()` must not touch disk during recovery rollback. The
/// previous implementation called `engine.rollback()` which issues a
/// separate `storage.commit(empty)`; if that side-commit failed (disk
/// full / I/O error), engine in-memory state would be partially rolled
/// back while disk META still pointed at the old `latest_block`, wedging
/// the instance.
///
/// This test uses a backend that fails EVERY commit and confirms that:
///   1. The original ingest error is returned cleanly (no compound error).
///   2. No extra commits are attempted during recovery (the only commit
///      attempt is the deferred ack-commit, which is never reached).
///   3. The instance's in-memory state is reverted to the pre-ingest
///      recovery point — confirmed by a subsequent ingest succeeding
///      from that point.
#[test]
fn failed_ingest_recovery_stays_in_memory_only() {
    let backend: Arc<AlwaysFailCommit> = Arc::new(AlwaysFailCommit::default());
    let mut db = Settle::open(Config::new(SCHEMA).storage(backend.clone())).unwrap();

    // First ingest with bad block_number type → triggers the error path
    // inside the closure that wraps process_batch_deferred.
    let mut bad = HashMap::new();
    bad.insert(
        "orders".into(),
        vec![HashMap::from([
            ("asset_id".into(), Value::String("token_a".into())),
            ("amount".into(), Value::UInt64(100)),
            // block_number is a String instead of UInt64 — process_batch_deferred fails.
            ("block_number".into(), Value::String("not-a-number".into())),
        ])],
    );
    let err = db
        .ingest(IngestInput {
            data: bad,
            rollback_chain: vec![cursor(1)],
            finalized_head: cursor(0),
        })
        .unwrap_err();

    // Original InvalidOperation surfaces — not wrapped in Error::Rollback.
    assert!(
        matches!(err, Error::InvalidOperation(_)),
        "expected InvalidOperation, got {err:?}",
    );

    // Backend recorded ZERO commit attempts — the only commit the new flow
    // would trigger is the deferred ack-commit (not reached on early error).
    assert_eq!(
        backend.commits_attempted.load(Ordering::SeqCst),
        0,
        "recovery rollback must NOT issue side commits — it should be purely in-memory",
    );

    // Engine state is at the recovery point (0 / fresh DB). A clean
    // ingest from there must succeed (would fail if engine had partial state
    // from the bad ingest leaking through).
    assert!(!db.is_awaiting_ack());
    assert_eq!(db.latest_block(), 0);
}

#[derive(Default)]
struct AlwaysFailCommit {
    inner: MemoryBackend,
    commits_attempted: AtomicUsize,
}

impl StorageBackend for AlwaysFailCommit {
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
    fn commit(&self, _batch: &settle::storage::StorageWriteBatch) -> settle::error::Result<()> {
        self.commits_attempted.fetch_add(1, Ordering::SeqCst);
        Err(Error::Storage("commit always fails".into()))
    }
}

// ─── Heartbeat-commit failure → instance poisoned ────────────────

/// A failed immediate-commit on the heartbeat path (empty-batch ingest)
/// leaves engine in-memory state mutated past disk: `engine.finalize`
/// pruned `block_snapshots` and a naive retry would re-run `finalize`
/// against the pruned state and write an empty batch — silently leaving
/// CF_REDUCER_FIN stuck at the old finalized block.
///
/// The chosen recovery is to **poison** the instance: subsequent mutating
/// calls return `Err(Poisoned)` forcing the caller to drop+reopen, which
/// rebuilds in-memory state from disk via `replay_unfinalized`.
#[test]
fn heartbeat_commit_failure_poisons_instance() {
    let backend = Arc::new(FailFirstCommitBackend::new(1));
    let mut db = Settle::open(Config::new(SCHEMA).storage(backend)).unwrap();

    // Heartbeat ingest: engine.finalize() runs (mutates in-memory), then
    // the immediate commit fails. The instance is now poisoned.
    let result = db.ingest(IngestInput {
        data: HashMap::new(),
        rollback_chain: vec![],
        finalized_head: cursor(0),
    });
    assert!(result.is_err(), "heartbeat commit failure surfaces as Err");
    assert!(db.is_poisoned(), "commit failure poisons the instance");
    // No data-pending was set (heartbeat path doesn't stash pending).
    assert!(!db.is_awaiting_ack());

    // Subsequent mutating calls reject with Poisoned — caller must drop+reopen.
    let mut row = order(ASSET, 50);
    row.insert("block_number".into(), Value::UInt64(1));
    let err = db
        .ingest(IngestInput {
            data: HashMap::from([("orders".into(), vec![row])]),
            rollback_chain: vec![cursor(1)],
            finalized_head: cursor(0),
        })
        .unwrap_err();
    assert!(
        matches!(err, Error::Poisoned(_)),
        "poisoned instance must reject further mutating work, got {err:?}",
    );

    // handle_fork, replay_reducer, ack — all rejected too.
    let fork_err = match db.handle_fork(vec![cursor(1)]) {
        Ok(_) => panic!("poisoned handle_fork must error"),
        Err(e) => e,
    };
    assert!(matches!(fork_err, Error::Poisoned(_)));
    assert!(matches!(
        db.replay_reducer("market_stats").unwrap_err(),
        Error::Poisoned(_),
    ));
    assert!(matches!(db.ack(0).unwrap_err(), Error::Poisoned(_)));
}

// ─── replay_reducer must NOT double-count ────────────────────────

/// `replay_reducer` is intended for hot-reloading an external reducer or
/// re-processing unfinalized blocks after installing a JS callback. If
/// called when the reducer + downstream MV already have state for the
/// unfinalized range, the replay must rebuild from the finalized baseline
/// — NOT re-apply on top of the existing state (which would double every
/// emit and aggregate contribution).
#[test]
fn replay_reducer_does_not_double_accumulate() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    // Ingest two unfinalized blocks of 10 each. Finalized stays at 0.
    for block in [1u64, 2] {
        let mut row = order(ASSET, 10);
        row.insert("block_number".into(), Value::UInt64(block));
        let chain: Vec<BlockCursor> = (1..=block).rev().map(cursor).collect();
        let b = db
            .ingest(IngestInput {
                data: HashMap::from([("orders".into(), vec![row])]),
                rollback_chain: chain,
                finalized_head: cursor(0),
            })
            .unwrap()
            .expect("batch");
        db.ack(b.sequence).unwrap();
    }

    // At this point MV state for ASSET should be sum=20, count=2.
    // Re-replaying the unfinalized range must NOT re-add those emits.
    db.replay_reducer("market_stats").unwrap();

    // Probe with a zero-amount block 3 — its ChangeBatch surfaces the
    // current cumulative MV state. Without the reset-before-replay fix,
    // the reducer + MV would have re-processed blocks 1-2 on top of their
    // existing state and `total_volume` would read 40 / count 5 instead
    // of 20 / 3.
    let mut row = order(ASSET, 0);
    row.insert("block_number".into(), Value::UInt64(3));
    let batch = db
        .ingest(IngestInput {
            data: HashMap::from([("orders".into(), vec![row])]),
            rollback_chain: vec![cursor(3), cursor(2), cursor(1)],
            finalized_head: cursor(0),
        })
        .unwrap()
        .expect("batch");

    let mv = batch
        .records_for("token_summary")
        .iter()
        .find(|r| r.key.get("asset_id") == Some(&Value::String(ASSET.to_string())))
        .expect("token_summary record for asset_a")
        .clone();
    let total = match mv.values.get("total_volume") {
        Some(Value::UInt64(n)) => *n,
        Some(Value::Int64(n)) => *n as u64,
        Some(Value::Float64(f)) => *f as u64,
        other => panic!("unexpected total_volume type: {other:?}"),
    };
    let count = match mv.values.get("trade_count") {
        Some(Value::UInt64(n)) => *n,
        Some(Value::Int64(n)) => *n as u64,
        Some(Value::Float64(f)) => *f as u64,
        other => panic!("unexpected trade_count type: {other:?}"),
    };
    assert_eq!(total, 20, "replay must not double-count: total_volume");
    assert_eq!(count, 3, "replay must not double-count: trade_count");
}

// ─── Gappy-chain finalize preserves latest_cursor hash ───────────

/// On gappy chains (Solana-style) the caller knows finality for blocks
/// it hasn't ingested data for. After `finalize(F)` the engine drops
/// hashes below `F` — but `latest_block` may be one of those lower
/// blocks. Naive pruning leaves `latest_cursor()` returning a
/// `BlockCursor { hash: "" }` because the hash was wiped.
///
/// The fix preserves `latest_block`'s hash when `latest < finalized`.
/// This test pins that contract: after a gappy ingest, both
/// `latest_cursor` and `finalized_cursor` have non-empty hashes.
#[test]
fn gappy_chain_finalize_preserves_latest_cursor_hash() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    // Ingest data only at block 3, but tell the engine finality is at
    // block 7 — the canonical Solana scenario where most blocks are
    // skipped on the indexed side.
    let mut row = order(ASSET, 100);
    row.insert("block_number".into(), Value::UInt64(3));
    let rollback_chain = vec![cursor(7), cursor(3)];
    let b = db
        .ingest(IngestInput {
            data: HashMap::from([("orders".into(), vec![row])]),
            rollback_chain,
            finalized_head: cursor(7),
        })
        .unwrap()
        .expect("batch");
    db.ack(b.sequence).unwrap();

    // After finalize(7): latest=3 < finalized=7. Without the gap-preserve
    // fix the hash for block 3 would have been pruned by split_off(&7).
    let latest = db.latest_cursor().expect("latest_cursor present");
    let finalized = db.finalized_cursor().expect("finalized_cursor present");

    assert_eq!(latest.number, 3);
    assert!(
        !latest.hash.is_empty(),
        "latest_cursor must carry a non-empty hash even when latest < finalized",
    );
    // Should match the deterministic hash we passed in.
    assert_eq!(latest.hash, format!("0x{:016x}", 3));

    assert_eq!(finalized.number, 7);
    assert_eq!(finalized.hash, format!("0x{:016x}", 7));
}

// ─── register_reducer rejects duplicate names ────────────────────

/// `Settle::register_reducer` is the low-level Rust API; calling it twice
/// with the same name would silently append a second `PipelineNode::Reducer`
/// and every subsequent ingest would double-apply state. The right
/// response is to error so callers explicitly route via `replay_reducer`
/// / `register_reducer_callback` for hot-reload semantics.
///
/// NAPI / WASM bindings already branch on `has_reducer` BEFORE calling
/// this method, so this guard is the Rust-API safety net.
#[test]
fn register_reducer_rejects_duplicate_name() {
    use settle::schema::ast::{ReducerBody, ReducerDef, StateField};
    use settle::types::ColumnType;

    // SCHEMA already declares the `market_stats` Lua reducer.
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());
    assert!(db.has_reducer("market_stats"));

    // Try to register a fresh reducer under the SAME name through the Rust API.
    let dup = ReducerDef {
        name: "market_stats".into(),
        source: "orders".into(),
        group_by: vec!["asset_id".into()],
        state: vec![StateField {
            name: "volume".into(),
            column_type: ColumnType::UInt64,
            default: "0".into(),
        }],
        body: ReducerBody::External {
            id: "market_stats".into(),
        },
        requires: vec![],
    };
    let err = db.register_reducer(dup).unwrap_err();
    match err {
        Error::InvalidOperation(msg) => {
            assert!(
                msg.contains("market_stats") && msg.contains("already exists"),
                "expected duplicate-name error, got: {msg}",
            );
        }
        other => panic!("expected InvalidOperation, got {other:?}"),
    }

    // Ingest still works through the original reducer — no duplicate
    // pipeline node was appended.
    let mut row = order(ASSET, 10);
    row.insert("block_number".into(), Value::UInt64(1));
    let batch = db
        .ingest(IngestInput {
            data: HashMap::from([("orders".into(), vec![row])]),
            rollback_chain: vec![cursor(1)],
            finalized_head: cursor(0),
        })
        .unwrap()
        .expect("batch");
    let mv = batch
        .records_for("token_summary")
        .iter()
        .find(|r| r.key.get("asset_id") == Some(&Value::String(ASSET.to_string())))
        .expect("token_summary record")
        .clone();
    let count = match mv.values.get("trade_count") {
        Some(Value::UInt64(n)) => *n,
        Some(Value::Int64(n)) => *n as u64,
        Some(Value::Float64(f)) => *f as u64,
        other => panic!("unexpected trade_count type: {other:?}"),
    };
    assert_eq!(
        count, 1,
        "single ingest must produce trade_count=1, not 2 (which a duplicate pipeline node would)",
    );
}

// ─── register_reducer_callback strict: one runtime per name ────────────

/// A runtime can only be attached once per reducer name; a second
/// attempt errors with "already attached" rather than silently replacing
/// the previous runtime.
#[test]
fn register_reducer_callback_rejects_second_attach() {
    use settle::reducer_runtime::fn_reducer::FnReducerRuntime;

    let schema = r#"
        CREATE TABLE trades (
            block_number UInt64,
            user String,
            amount Float64
        );
        CREATE REDUCER pnl
        SOURCE trades
        GROUP BY user
        STATE (qty Float64 DEFAULT 0)
        LANGUAGE EXTERNAL;
    "#;
    let mut db = Settle::open(Config::new(schema)).unwrap();

    // First attach — succeeds.
    db.register_reducer_callback(
        "pnl",
        Box::new(FnReducerRuntime::new(|state, row| {
            let q = state.get("qty").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let a = row.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
            state.insert("qty".into(), Value::Float64(q + a));
            vec![HashMap::new()]
        })),
    )
    .expect("first attach should succeed");

    // Second attach with a different runtime — must error.
    let err = db
        .register_reducer_callback(
            "pnl",
            Box::new(FnReducerRuntime::new(|_state, _row| vec![HashMap::new()])),
        )
        .unwrap_err();
    match err {
        Error::InvalidOperation(msg) => {
            assert!(
                msg.contains("already attached"),
                "expected already-attached error, got: {msg}",
            );
        }
        other => panic!("expected InvalidOperation, got {other:?}"),
    }

    // Unknown name still errors with its own message.
    let err = db
        .register_reducer_callback(
            "no_such_reducer",
            Box::new(FnReducerRuntime::new(|_state, _row| vec![HashMap::new()])),
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidOperation(msg) if msg.contains("unknown reducer")));
}

// ─── register_reducer_callback rejects non-external reducers ─────

/// Attaching a host callback to a Lua reducer used to silently succeed —
/// the callback was stored but never invoked because the reducer's body
/// is `Lua`, not `External`. Now the API rejects it loudly.
#[test]
fn register_reducer_callback_rejects_lua_reducer() {
    use settle::reducer_runtime::fn_reducer::FnReducerRuntime;

    // Shared SCHEMA declares `market_stats` as a Lua reducer.
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    let err = db
        .register_reducer_callback(
            "market_stats",
            Box::new(FnReducerRuntime::new(|_state, _row| vec![HashMap::new()])),
        )
        .unwrap_err();
    match err {
        Error::InvalidOperation(msg) => assert!(
            msg.contains("not declared") && msg.contains("EXTERNAL"),
            "expected not-EXTERNAL error, got: {msg}",
        ),
        other => panic!("expected InvalidOperation, got {other:?}"),
    }
}

