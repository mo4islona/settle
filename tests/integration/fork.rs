//! E2E: `handle_fork()` against a real RocksDB backend.
//!
//! Exercises the Portal-409 recovery path: when Portal signals a chain
//! reorg, `handle_fork(rollback_chain)` must resolve the highest common
//! ancestor in our stored hashes, emit compensating changes for everything
//! above it, and atomically commit the rolled-back state.

use settle::db::IngestInput;
use settle::test_helpers::{cursor, handle_fork, ingest_input};
use settle::types::{BlockCursor, ChangeOp, Value};
use std::collections::HashMap;
use tempfile::TempDir;

use super::common::{mv_record_for, open_rocks, order, prev_total_volume, total_volume, trade_count};

const ASSET: &str = "token_a";

fn cursor_with_hash(n: u64, hash: &str) -> BlockCursor {
    BlockCursor {
        number: n,
        hash: hash.to_string(),
    }
}

/// Ingest a single unfinalized block carrying one order for `ASSET`, using a
/// caller-chosen hash so the test can fork by content at the same height.
fn ingest_block_with_hash(
    db: &mut settle::db::Settle,
    block: u64,
    hash: &str,
    amount: u64,
    rollback_chain: Vec<BlockCursor>,
) {
    let mut row = order(ASSET, amount);
    row.insert("block_number".into(), Value::UInt64(block));
    let mut data: HashMap<String, Vec<settle::types::RowMap>> = HashMap::new();
    data.insert("orders".into(), vec![row]);

    let mut chain = rollback_chain;
    chain.push(cursor_with_hash(block, hash));
    ingest_input(db, IngestInput {
        data,
        rollback_chain: chain,
        finalized_head: cursor(0),
    })
    .unwrap();
}

/// Realistic Portal-409: blocks A3..A5 ingested, then Portal sends a new
/// `rollback_chain` showing B3..B5 is canonical. `handle_fork` must
/// (a) resolve cursor 2 as the highest common ancestor, (b) emit a
/// compensating MV update reflecting the rollback, (c) leave the engine
/// ready to ingest B3..B5 with no traces of the A fork.
#[test]
fn fork_at_unfinalized_block_rolls_back_to_common_ancestor() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    // Linear A-chain: blocks 1..=5. Each block carries a distinct hash so
    // we can construct a divergent B-chain later.
    ingest_block_with_hash(&mut db, 1, "0xA1", 10, vec![]);
    ingest_block_with_hash(&mut db, 2, "0xA2", 20, vec![cursor_with_hash(1, "0xA1")]);
    ingest_block_with_hash(
        &mut db,
        3,
        "0xA3",
        30,
        vec![cursor_with_hash(1, "0xA1"), cursor_with_hash(2, "0xA2")],
    );
    ingest_block_with_hash(
        &mut db,
        4,
        "0xA4",
        40,
        vec![
            cursor_with_hash(1, "0xA1"),
            cursor_with_hash(2, "0xA2"),
            cursor_with_hash(3, "0xA3"),
        ],
    );
    ingest_block_with_hash(
        &mut db,
        5,
        "0xA5",
        50,
        vec![
            cursor_with_hash(1, "0xA1"),
            cursor_with_hash(2, "0xA2"),
            cursor_with_hash(3, "0xA3"),
            cursor_with_hash(4, "0xA4"),
        ],
    );
    assert_eq!(db.latest_block(), 5);

    // Portal 409 — new chain diverges at block 3 (B3 != A3).
    let new_chain = vec![
        cursor_with_hash(1, "0xA1"),
        cursor_with_hash(2, "0xA2"),
        cursor_with_hash(3, "0xB3"),
        cursor_with_hash(4, "0xB4"),
        cursor_with_hash(5, "0xB5"),
    ];
    let fork = handle_fork(&mut db, new_chain).unwrap();
    assert_eq!(
        fork.cursor.number, 2,
        "handle_fork resolves the highest common ancestor"
    );
    assert_eq!(db.latest_block(), 2);

    // Compensating MV update reflects rollback of blocks 3,4,5 (30+40+50=120).
    // Remaining state after rollback: blocks 1+2 = 30.
    let batch = fork.batch.expect("fork emits a compensating batch");
    let mv = mv_record_for(&batch, ASSET).expect("MV update emitted");
    assert_eq!(mv.operation, ChangeOp::Update);
    assert_eq!(total_volume(mv), 30, "post-rollback state = blocks 1+2");
    assert_eq!(trade_count(mv), 2);
    assert_eq!(prev_total_volume(mv), 150, "prev = full A-chain state");

    // Engine is now ready for B3..B5. Re-ingest B3.
    ingest_block_with_hash(
        &mut db,
        3,
        "0xB3",
        7,
        vec![cursor_with_hash(1, "0xA1"), cursor_with_hash(2, "0xA2")],
    );
    assert_eq!(db.latest_block(), 3);
}

/// `handle_fork` must error when no common ancestor exists in our stored
/// hashes (e.g. an entirely foreign chain). This guards against silently
/// dropping all state on a malformed Portal response.
#[test]
fn fork_with_no_common_ancestor_returns_error() {
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    // Ingest two blocks on our chain.
    ingest_block_with_hash(&mut db, 1, "0xA1", 10, vec![]);
    ingest_block_with_hash(&mut db, 2, "0xA2", 20, vec![cursor_with_hash(1, "0xA1")]);

    // Foreign chain with no overlapping hashes.
    let foreign = vec![
        cursor_with_hash(1, "0xZ1"),
        cursor_with_hash(2, "0xZ2"),
    ];
    let err = handle_fork(&mut db, foreign).err();
    assert!(
        err.is_some(),
        "fork with no common ancestor must error, got Ok"
    );

    // State unchanged after the failed call.
    assert_eq!(db.latest_block(), 2);
}

