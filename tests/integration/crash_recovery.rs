//! E2E: drop-and-reopen crash recovery against a real RocksDB backend.
//!
//! `Settle` uses commit-after-ack semantics: storage writes are deferred
//! into a pending slot until the caller calls `ack(batch.sequence)`. The
//! shared `ingest_orders` / `ingest_with_finalized` test helpers auto-ack,
//! so dropping a `Settle` instance after these helpers return is equivalent
//! to a process kill that happens after the last successful ack.

use std::collections::HashMap;

use settle::db::IngestInput;
use settle::test_helpers::{cursor, ingest_one, ingest_with_finalized};
use tempfile::TempDir;

use super::common::{
    ingest_orders, mv_record_for, open_rocks, order, prev_total_volume, prev_trade_count,
    total_volume, trade_count,
};

const ASSET: &str = "token_a";

/// Crash with all blocks still unfinalized — the engine must replay them
/// from per-block snapshots in `CF_REDUCER_SNAP` on reopen.
#[test]
fn crash_between_ingest_and_next_finalize() {
    let tmp = TempDir::new().unwrap();

    {
        let mut db = open_rocks(tmp.path());
        for block in 1..=10 {
            ingest_orders(&mut db, block, vec![order(ASSET, 10)]);
        }
        assert_eq!(db.latest_block(), 10);
        assert_eq!(db.finalized_block(), 0);
        // drop here — simulates process death after ingest returned
    }

    let mut db = open_rocks(tmp.path());
    assert_eq!(db.latest_block(), 10, "latest_block must persist");
    assert_eq!(db.finalized_block(), 0, "finalized stays where it was");

    // A follow-up ingest at block 11 reveals the restored MV state via
    // prev_values: 10 blocks × 10 amount = 100.
    let batch = ingest_one(&mut db, "orders", 11, vec![order(ASSET, 7)])
        .unwrap()
        .unwrap();
    let mv = mv_record_for(&batch, ASSET).expect("MV row emitted");
    assert_eq!(prev_total_volume(mv), 100);
    assert_eq!(prev_trade_count(mv), 10);
    assert_eq!(total_volume(mv), 107);
    assert_eq!(trade_count(mv), 11);
}

/// Crash with a mix of finalized and unfinalized blocks — finalized state
/// must come from `CF_REDUCER_FIN`, unfinalized blocks must be replayed.
#[test]
fn crash_after_partial_finalization() {
    let tmp = TempDir::new().unwrap();

    {
        let mut db = open_rocks(tmp.path());
        // Blocks 1..=5 finalized, 6..=10 unfinalized.
        ingest_with_finalized(
            &mut db,
            (1..=10)
                .map(|b| ("orders".into(), b, vec![order(ASSET, 10)]))
                .collect(),
            5,
        )
        .unwrap();
        assert_eq!(db.finalized_block(), 5);
        assert_eq!(db.latest_block(), 10);
    }

    let mut db = open_rocks(tmp.path());
    assert_eq!(db.finalized_block(), 5, "finalized_block must persist");
    assert_eq!(db.latest_block(), 10, "latest_block must persist");

    let batch = ingest_one(&mut db, "orders", 11, vec![order(ASSET, 7)])
        .unwrap()
        .unwrap();
    let mv = mv_record_for(&batch, ASSET).expect("MV row emitted");
    assert_eq!(prev_total_volume(mv), 100);
    assert_eq!(prev_trade_count(mv), 10);
}

/// Crash before ack: a `ChangeBatch` was returned but never acked. Disk
/// state stays at the previous committed point, the in-memory write batch
/// is dropped, and on reopen the same input deterministically reproduces
/// the same batch.
#[test]
fn crash_before_ack_rolls_back_to_last_committed() {
    use settle::test_helpers::ingest_with_finalized_no_ack;
    use settle::types::ChangeBatch;

    let tmp = TempDir::new().unwrap();

    // First run: ingest block 1 + ack (helper does ack), then ingest block 2
    // WITHOUT ack and drop. Disk should remain at block 1.
    let block1_batch: ChangeBatch;
    {
        let mut db = open_rocks(tmp.path());
        block1_batch = ingest_orders(&mut db, 1, vec![order(ASSET, 100)]);
        // Ingest block 2 without acking.
        let _ = ingest_with_finalized_no_ack(
            &mut db,
            vec![("orders".into(), 2, vec![order(ASSET, 200)])],
            0,
        )
        .unwrap()
        .expect("block 2 produces a batch");
        assert!(db.is_awaiting_ack());
        // Engine view advances on ingest; durability signal is is_awaiting_ack.
        assert_eq!(db.latest_block(), 2, "engine in-memory advances");
    }

    // Reopen: disk is at block 1, block 2's writes were dropped with pending.
    let mut db = open_rocks(tmp.path());
    assert_eq!(db.latest_block(), 1, "uncommitted block 2 rolled back on reopen");
    assert!(!db.is_awaiting_ack());

    // Re-ingest block 2 with identical input: same sequence (META_NEXT_SEQUENCE
    // was rolled back too because it sat inside the same pending write_batch),
    // same content.
    let replay = ingest_orders(&mut db, 2, vec![order(ASSET, 200)]);
    assert_eq!(
        replay.sequence,
        block1_batch.sequence + 1,
        "post-crash re-ingest deterministically replays the same sequence"
    );

    let mv = mv_record_for(&replay, ASSET).unwrap();
    assert_eq!(total_volume(mv), 300, "MV reflects blocks 1+2 cumulative");
    assert_eq!(trade_count(mv), 2);
}

/// Bit-for-bit equivalence: ingesting N blocks straight through and
/// ingesting (N/2) → drop+reopen → (N/2) must produce identical MV records
/// for the final block.
#[test]
fn replay_after_crash_matches_clean_run() {
    // --- Clean run: one process, blocks 1..=20 ---
    let clean_dir = TempDir::new().unwrap();
    let clean_final_batch;
    {
        let mut db = open_rocks(clean_dir.path());
        for block in 1..=19 {
            ingest_orders(&mut db, block, vec![order(ASSET, 13)]);
        }
        clean_final_batch = ingest_one(&mut db, "orders", 20, vec![order(ASSET, 7)])
            .unwrap()
            .unwrap();
    }

    // --- Crash run: blocks 1..=10, drop+reopen, blocks 11..=20 ---
    let crash_dir = TempDir::new().unwrap();
    let crash_final_batch;
    {
        let mut db = open_rocks(crash_dir.path());
        for block in 1..=10 {
            ingest_orders(&mut db, block, vec![order(ASSET, 13)]);
        }
    }
    {
        let mut db = open_rocks(crash_dir.path());
        for block in 11..=19 {
            ingest_orders(&mut db, block, vec![order(ASSET, 13)]);
        }
        crash_final_batch = ingest_one(&mut db, "orders", 20, vec![order(ASSET, 7)])
            .unwrap()
            .unwrap();
    }

    // `META_NEXT_SEQUENCE` is now persisted across reopen, so sequence
    // numbers stay monotonic in the crash run too. We still compare MV
    // record content rather than full batches because the clean and crash
    // runs reach the same final state via different intermediate batches.
    let clean_mv = mv_record_for(&clean_final_batch, ASSET).unwrap();
    let crash_mv = mv_record_for(&crash_final_batch, ASSET).unwrap();
    assert_eq!(
        clean_mv.values, crash_mv.values,
        "final MV values must match bit-for-bit"
    );
    assert_eq!(
        clean_mv.prev_values, crash_mv.prev_values,
        "prev_values must match bit-for-bit"
    );
    assert_eq!(clean_mv.operation, crash_mv.operation);
    assert_eq!(clean_mv.key, crash_mv.key);
}

/// Sanity: explicit no-data IngestInput with the same finalized head as
/// stored on disk must be a stable heartbeat — no MV emissions, no state
/// drift across drop+reopen.
#[test]
fn heartbeat_ingest_stable_across_restart() {
    let tmp = TempDir::new().unwrap();
    {
        let mut db = open_rocks(tmp.path());
        ingest_with_finalized(
            &mut db,
            vec![("orders".into(), 5, vec![order(ASSET, 50)])],
            5,
        )
        .unwrap();
    }
    let mut db = open_rocks(tmp.path());
    let batch = db
        .ingest(IngestInput {
            data: HashMap::new(),
            rollback_chain: vec![],
            finalized_head: cursor(5),
        })
        .unwrap();
    assert!(batch.is_none(), "heartbeat after reopen emits nothing");
    assert_eq!(db.latest_block(), 5);
    assert_eq!(db.finalized_block(), 5);
}
