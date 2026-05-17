//! E2E: buffer + ack-sequence semantics through the public `Settle` API.
//!
//! `Settle::ingest()` drains the buffer at the end of every call: returned
//! `ChangeBatch` is the only observable view of buffered records. These tests
//! pin that contract plus the durable monotonic sequence counter.

use tempfile::TempDir;

use super::common::{ingest_orders, mv_record_for, open_rocks, order, total_volume, trade_count};

const ASSET: &str = "token_a";

/// Multiple rows in a single ingest for the same MV group must surface as a
/// single merged record in the resulting `ChangeBatch`. This is the only
/// place where buffer merging is observable at the public API (across
/// ingests the buffer is auto-drained).
#[test]
fn single_ingest_merges_multiple_rows_for_same_key() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    let batch = ingest_orders(
        &mut db,
        1,
        vec![order(ASSET, 10), order(ASSET, 20), order(ASSET, 30)],
    );
    let records = batch.records_for("token_summary");
    assert_eq!(records.len(), 1, "three rows merge into one MV record");
    assert_eq!(total_volume(&records[0]), 60);
    assert_eq!(trade_count(&records[0]), 3);
}

/// `ingest()` always drains the buffer before returning. After acking the
/// produced batch, `pending_count` is zero and no pending-ack slot remains.
#[test]
fn ingest_drains_buffer() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    ingest_orders(&mut db, 1, vec![order(ASSET, 10)]);
    assert_eq!(db.pending_count(), 0);
    assert!(!db.is_awaiting_ack(), "ack via helper clears pending");
}

/// `ack()` durably commits the pending batch and is the gate for the next
/// `ingest()`. Without the helper-driven ack, subsequent ingest returns
/// `Err(PendingAck)`. After ack, sequences strictly increase.
#[test]
fn ack_commits_pending_state_and_sequence_increments() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    let b1 = ingest_orders(&mut db, 1, vec![order(ASSET, 10)]);
    assert_eq!(b1.sequence, 1);

    let b2 = ingest_orders(&mut db, 2, vec![order(ASSET, 10)]);
    assert_eq!(b2.sequence, 2, "sequence advances per acked batch");

    // ack with a stale or unknown sequence at pending=None is a no-op,
    // not an error — covers double-ack and ack-on-startup idempotence.
    assert!(db.ack(99).is_ok());

    let b3 = ingest_orders(&mut db, 3, vec![order(ASSET, 10)]);
    assert_eq!(b3.sequence, 3);
}

/// `META_NEXT_SEQUENCE` is persisted alongside raw rows + finalized state,
/// so sequences stay monotonic across `drop` + reopen. (Previously the
/// in-memory counter reset to 1 on reopen.)
#[test]
fn sequence_monotonic_across_reopen() {
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open_rocks(tmp.path());
        let b1 = ingest_orders(&mut db, 1, vec![order(ASSET, 10)]);
        let b2 = ingest_orders(&mut db, 2, vec![order(ASSET, 10)]);
        assert_eq!(b1.sequence, 1);
        assert_eq!(b2.sequence, 2);
    }

    let mut db = open_rocks(tmp.path());
    let b_after_restart = ingest_orders(&mut db, 3, vec![order(ASSET, 10)]);
    assert_eq!(
        b_after_restart.sequence, 3,
        "META_NEXT_SEQUENCE persists across reopen — sequence does not reset"
    );
    let mv = mv_record_for(&b_after_restart, ASSET).unwrap();
    assert_eq!(total_volume(mv), 30);
    assert_eq!(trade_count(mv), 3);
}
