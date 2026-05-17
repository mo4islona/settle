//! E2E: no data duplication on common Portal re-ingest patterns.
//!
//! Each test exercises a real RocksDB-backed `Settle` and inspects the MV
//! state through the emitted `ChangeBatch`. The first test currently
//! documents Issue #1 (FORK_ISSUES.md) — re-ingest of an identical block
//! doubles aggregates because `ingest()` has no dedup guard.

use std::collections::HashMap;

use settle::db::IngestInput;
use settle::test_helpers::{cursor, ingest_input, ingest_one, ingest_with_finalized, rollback_to};
use settle::types::{BlockCursor, Value};
use tempfile::TempDir;

use super::common::{
    ingest_orders, mv_record_for, open_rocks, order, prev_total_volume, prev_trade_count,
    total_volume, trade_count,
};

const ASSET: &str = "token_a";

/// Block hash for tests that need to fork by hash (different content at the
/// same height). The deterministic `block_hash` from `test_helpers` keys
/// off block number only.
fn cursor_with_hash(n: u64, hash: &str) -> BlockCursor {
    BlockCursor {
        number: n,
        hash: hash.to_string(),
    }
}

/// Issue #1 (FORK_ISSUES.md): re-ingesting a block at or below
/// `latest_block` would double reducer/MV aggregates. The dedup guard in
/// `Settle::ingest()` returns `Err` instead, so caller bugs / Portal retry
/// glitches surface immediately.
#[test]
fn re_ingest_same_block_returns_error() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    let rows = vec![order(ASSET, 100), order(ASSET, 50)];

    let first = ingest_orders(&mut db, 5, rows.clone());
    let first_mv = mv_record_for(&first, ASSET).expect("first ingest emits MV row");
    assert_eq!(total_volume(first_mv), 150);
    assert_eq!(trade_count(first_mv), 2);
    assert_eq!(db.latest_block(), 5);

    // Second ingest with identical block — must error, not silently double.
    let err = ingest_one(&mut db, "orders", 5, rows).err();
    let msg = err.expect("duplicate ingest must error").to_string();
    assert!(
        msg.contains("duplicate"),
        "error message must explain the cause, got: {msg}"
    );

    // State unchanged after the rejection — confirmed via a probe at block 6.
    assert_eq!(db.latest_block(), 5);
    let follow_up = ingest_one(&mut db, "orders", 6, vec![order(ASSET, 1)])
        .unwrap()
        .unwrap();
    let mv = mv_record_for(&follow_up, ASSET).expect("follow-up emits MV row");
    assert_eq!(
        prev_total_volume(mv),
        150,
        "MV state must not be perturbed by the failed re-ingest"
    );
    assert_eq!(prev_trade_count(mv), 2);
}

/// A batch mixing new and already-processed blocks must reject the whole
/// batch — partial application would leave aggregates in an inconsistent
/// state.
#[test]
fn batch_with_one_duplicate_block_rejects_whole_batch() {
    use settle::test_helpers::ingest_blocks;

    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    ingest_orders(&mut db, 5, vec![order(ASSET, 100)]);

    // New block 6 + duplicate of block 5 in the same batch.
    let err = ingest_blocks(
        &mut db,
        vec![
            ("orders".into(), 5, vec![order(ASSET, 999)]),
            ("orders".into(), 6, vec![order(ASSET, 10)]),
        ],
    )
    .err();
    assert!(
        err.is_some(),
        "batch containing a duplicate block must error as a whole"
    );

    // Neither block 5 nor block 6 was processed: state matches the
    // original block-5 ingest.
    assert_eq!(db.latest_block(), 5);
    let follow_up = ingest_one(&mut db, "orders", 7, vec![order(ASSET, 1)])
        .unwrap()
        .unwrap();
    let mv = mv_record_for(&follow_up, ASSET).expect("follow-up emits MV row");
    assert_eq!(prev_total_volume(mv), 100);
    assert_eq!(prev_trade_count(mv), 1);
}

/// Real fork-by-hash: block N arrives with content A, the chain reorgs, and
/// block N is re-delivered with content B. After handle_fork → re-ingest,
/// the MV must reflect ONLY B (the canonical chain).
#[test]
fn fork_rollback_then_reingest_same_height() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    // Block 4 acts as the common ancestor; ingested with the deterministic hash.
    ingest_orders(&mut db, 4, vec![order(ASSET, 10)]);

    // Block 5 first arrives with content A (volume 100, 1 trade).
    let mut data_a: HashMap<String, Vec<settle::types::RowMap>> = HashMap::new();
    let mut row_a = order(ASSET, 100);
    row_a.insert("block_number".into(), Value::UInt64(5));
    data_a.insert("orders".into(), vec![row_a]);
    ingest_input(&mut db, IngestInput {
        data: data_a,
        rollback_chain: vec![cursor_with_hash(5, "0xA5"), cursor(4)],
        finalized_head: cursor(0),
    })
    .unwrap();

    // Reorg: roll back to block 4 (the common ancestor).
    rollback_to(&mut db, 4).unwrap();
    assert_eq!(db.latest_block(), 4);

    // Block 5 re-arrives with content B (volume 200, 2 trades).
    let mut data_b: HashMap<String, Vec<settle::types::RowMap>> = HashMap::new();
    let mut row_b1 = order(ASSET, 80);
    row_b1.insert("block_number".into(), Value::UInt64(5));
    let mut row_b2 = order(ASSET, 120);
    row_b2.insert("block_number".into(), Value::UInt64(5));
    data_b.insert("orders".into(), vec![row_b1, row_b2]);
    let batch = db
        .ingest(IngestInput {
            data: data_b,
            rollback_chain: vec![cursor_with_hash(5, "0xB5"), cursor(4)],
            finalized_head: cursor(0),
        })
        .unwrap()
        .expect("re-ingest produces a batch");

    let mv = mv_record_for(&batch, ASSET).expect("re-ingest emits MV row");
    // Block 4 contributed 10. Block 5 B contributes 80 + 120 = 200. Total 210.
    assert_eq!(total_volume(mv), 210, "MV must reflect only the B fork");
    assert_eq!(trade_count(mv), 3);
}

/// A "heartbeat" ingest — empty data, same finalized head as current — must
/// not perturb existing aggregates. This guards against spurious re-emission
/// or accidental state mutation on idle polls.
#[test]
fn idempotent_no_op_ingest_preserves_state() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    ingest_with_finalized(
        &mut db,
        vec![
            ("orders".into(), 4, vec![order(ASSET, 10)]),
            ("orders".into(), 5, vec![order(ASSET, 20)]),
        ],
        5,
    )
    .unwrap();
    assert_eq!(db.finalized_block(), 5);

    // Heartbeat: same finalized head, no new data.
    let batch = db
        .ingest(IngestInput {
            data: HashMap::new(),
            rollback_chain: vec![],
            finalized_head: cursor(5),
        })
        .unwrap();
    assert!(batch.is_none(), "heartbeat ingest must not emit any change");

    // State remains correct: a real follow-up ingest at block 6 sees the
    // expected prev_values (volume = 30 from blocks 4 + 5).
    let follow_up = ingest_one(&mut db, "orders", 6, vec![order(ASSET, 5)])
        .unwrap()
        .unwrap();
    let mv = mv_record_for(&follow_up, ASSET).expect("follow-up emits MV row");
    assert_eq!(total_volume(mv), 35);
    assert_eq!(trade_count(mv), 3);
    assert_eq!(prev_total_volume(mv), 30);
    assert_eq!(prev_trade_count(mv), 2);
}
