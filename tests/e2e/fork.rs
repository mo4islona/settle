//! E2E: chain reorg (`handle_fork`) against a real Postgres target sink.

use std::collections::HashMap;

use settle::db::IngestInput;
use settle::test_helpers::cursor;
use settle::types::{BlockCursor, Value};
use tempfile::TempDir;

use super::common::pg::{apply_batch, pg_row, start_pg};
use super::common::{open_rocks, order};

const ASSET_A: &str = "token_a";
const ASSET_B: &str = "token_b";

fn cursor_with_hash(n: u64, hash: &str) -> BlockCursor {
    BlockCursor {
        number: n,
        hash: hash.to_string(),
    }
}

fn ingest_block_for_pg(
    db: &mut settle::db::Settle,
    block: u64,
    asset: &str,
    amount: u64,
    chain: Vec<BlockCursor>,
) -> settle::types::ChangeBatch {
    let mut row = order(asset, amount);
    row.insert("block_number".into(), Value::UInt64(block));
    let mut data: HashMap<String, Vec<settle::types::RowMap>> = HashMap::new();
    data.insert("orders".into(), vec![row]);
    let mut chain = chain;
    chain.push(cursor_with_hash(block, &format!("0x{block:016x}")));
    db.ingest(IngestInput {
        data,
        rollback_chain: chain,
        finalized_head: cursor(0),
    })
    .unwrap()
    .expect("batch")
}

/// After ingesting two assets across three blocks (all acked into Postgres),
/// `handle_fork` rolls back to block 1. The compensating `ChangeBatch` —
/// when applied to Postgres — must delete `asset_b`'s row entirely and
/// restore `asset_a` to its block-1 state.
#[tokio::test(flavor = "multi_thread")]
async fn fork_compensating_ops_roll_back_target() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    let b1 = ingest_block_for_pg(&mut db, 1, ASSET_A, 10, vec![]);
    apply_batch(&mut pg.client, &b1).await.unwrap();
    db.ack(b1.sequence).unwrap();

    let b2 = ingest_block_for_pg(
        &mut db,
        2,
        ASSET_B,
        20,
        vec![cursor_with_hash(1, "0x0000000000000001")],
    );
    apply_batch(&mut pg.client, &b2).await.unwrap();
    db.ack(b2.sequence).unwrap();

    let b3 = ingest_block_for_pg(
        &mut db,
        3,
        ASSET_A,
        30,
        vec![
            cursor_with_hash(2, "0x0000000000000002"),
            cursor_with_hash(1, "0x0000000000000001"),
        ],
    );
    apply_batch(&mut pg.client, &b3).await.unwrap();
    db.ack(b3.sequence).unwrap();

    assert_eq!(pg_row(&pg.client, ASSET_A).await, Some((40, 2)));
    assert_eq!(pg_row(&pg.client, ASSET_B).await, Some((20, 1)));

    let fork_chain = vec![
        cursor_with_hash(3, "0xfork_3"),
        cursor_with_hash(2, "0xfork_2"),
        cursor_with_hash(1, "0x0000000000000001"),
    ];
    let fork = db.handle_fork(fork_chain).expect("handle_fork");
    assert_eq!(fork.cursor.number, 1);

    if let Some(batch) = fork.batch {
        apply_batch(&mut pg.client, &batch).await.expect("apply comp");
        db.ack(batch.sequence).expect("ack fork");
    }

    assert_eq!(pg_row(&pg.client, ASSET_A).await, Some((10, 1)));
    assert_eq!(
        pg_row(&pg.client, ASSET_B).await,
        None,
        "asset_b had no records below block 2 — compensating batch must delete it",
    );
}

/// Caller has an unacked batch in-flight and already applied it to Postgres
/// when a fork notification arrives. `handle_fork` rolls back from the state
/// after that batch; the caller applies the compensating batch after it, and
/// one ack commits both.
#[tokio::test(flavor = "multi_thread")]
async fn fork_during_pending_full_workflow() {
    let mut pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let mut db = open_rocks(tmp.path());

    // Establish block 1 (acked).
    let b1 = ingest_block_for_pg(&mut db, 1, ASSET_A, 10, vec![]);
    apply_batch(&mut pg.client, &b1).await.unwrap();
    db.ack(b1.sequence).unwrap();

    // Ingest block 2; apply to PG; do NOT ack yet.
    let b2 = ingest_block_for_pg(
        &mut db,
        2,
        ASSET_A,
        20,
        vec![cursor_with_hash(1, "0x0000000000000001")],
    );
    apply_batch(&mut pg.client, &b2).await.unwrap();
    assert!(db.is_awaiting_ack());

    // Fork notification arrives mid-flight.
    let fork_chain = vec![
        cursor_with_hash(2, "0xfork_2"),
        cursor_with_hash(1, "0x0000000000000001"),
    ];
    let fork = db.handle_fork(fork_chain).expect("handle_fork with b2 unacked");
    assert_eq!(fork.cursor.number, 1);
    let comp = fork.batch.expect("rollback of block 2 produces compensation");
    apply_batch(&mut pg.client, &comp).await.expect("apply comp");
    db.ack(comp.sequence).expect("ack covers b2 and the fork");
    assert!(!db.is_awaiting_ack());

    // PG reflects block-1-only state.
    assert_eq!(pg_row(&pg.client, ASSET_A).await, Some((10, 1)));
}
