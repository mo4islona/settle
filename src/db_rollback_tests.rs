use super::test_helpers::*;
use super::*;
use crate::types::{BlockCursor, DeltaOperation, Value};
use std::collections::HashMap;

#[test]
fn rollback_produces_compensating_deltas() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    db.process_batch("swaps", 1000, vec![make_swap("ETH/USDC", 100.0)])
        .unwrap();
    db.process_batch("swaps", 1001, vec![make_swap("ETH/USDC", 200.0)])
        .unwrap();

    // Flush and clear buffer
    db.flush();

    // Rollback block 1001
    db.rollback(1000).unwrap();

    let batch = db.flush().unwrap();

    // Should have MV update (back to 100) and raw delete
    let mv_records: Vec<_> = batch.records_for("pool_volume").iter().collect();
    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].operation, DeltaOperation::Update);
    assert_eq!(
        mv_records[0].values.get("total_volume"),
        Some(&Value::Float64(100.0))
    );

    assert_eq!(db.latest_block(), 1000);
}

#[test]
fn finalize_and_rollback() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    db.process_batch("swaps", 1000, vec![make_swap("ETH/USDC", 100.0)])
        .unwrap();
    db.process_batch("swaps", 1001, vec![make_swap("ETH/USDC", 200.0)])
        .unwrap();
    db.process_batch("swaps", 1002, vec![make_swap("ETH/USDC", 300.0)])
        .unwrap();
    db.flush();

    // Finalize up to 1001
    db.finalize(1001).unwrap();
    assert_eq!(db.finalized_block(), 1001);

    // Rollback block 1002
    db.rollback(1001).unwrap();

    let batch = db.flush().unwrap();
    let mv_records: Vec<_> = batch.records_for("pool_volume").iter().collect();
    assert_eq!(mv_records.len(), 1);
    // total should be 100 + 200 = 300
    assert_eq!(
        mv_records[0].values.get("total_volume"),
        Some(&Value::Float64(300.0))
    );
}

#[test]
fn full_pipeline_rollback_and_reingest() {
    let mut db = DeltaDb::open(Config::new(DEX_SCHEMA)).unwrap();

    db.process_batch(
        "trades",
        1000,
        vec![make_trade("alice", "buy", 10.0, 2000.0)],
    )
    .unwrap();
    db.process_batch(
        "trades",
        1001,
        vec![make_trade("alice", "buy", 5.0, 2100.0)],
    )
    .unwrap();
    db.process_batch(
        "trades",
        1002,
        vec![make_trade("alice", "sell", 8.0, 2200.0)],
    )
    .unwrap();
    db.flush();

    // Rollback block 1002 (the sell)
    db.rollback(1001).unwrap();
    db.flush();

    // Re-ingest with different sell
    db.process_batch(
        "trades",
        1002,
        vec![make_trade("alice", "sell", 3.0, 2300.0)],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let mv_records: Vec<_> = batch.records_for("position_summary").iter().collect();
    assert_eq!(mv_records.len(), 1);
    assert_eq!(
        mv_records[0].values.get("trade_count"),
        Some(&Value::UInt64(3))
    );

    // position_size after: 10 + 5 - 3 = 12
    assert_eq!(
        mv_records[0].values.get("current_position"),
        Some(&Value::Float64(12.0))
    );
}

#[test]
fn full_rollback_emits_delete_for_mv_group() {
    // Schema: aggregate volume per wallet. A wallet that only appeared in
    // rolled-back blocks should produce a Delete delta for its MV group.
    let schema = r#"
        CREATE TABLE transfers (
            wallet String,
            amount Float64
        );

        CREATE MATERIALIZED VIEW wallet_volume AS
        SELECT
            wallet,
            sum(amount) AS total_volume,
            count() AS tx_count
        FROM transfers
        GROUP BY wallet;
    "#;

    let mut db = DeltaDb::open(Config::new(schema)).unwrap();

    // Block 1000: alice appears for the first time
    db.process_batch(
        "transfers",
        1000,
        vec![HashMap::from([
            ("wallet".to_string(), Value::String("alice".to_string())),
            ("amount".to_string(), Value::Float64(500.0)),
        ])],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    // Verify Insert was emitted for alice's MV group
    let mv_inserts: Vec<_> = batch
        .records_for("wallet_volume")
        .iter()
        .filter(|r| r.operation == DeltaOperation::Insert)
        .collect();
    assert_eq!(mv_inserts.len(), 1);
    assert_eq!(
        mv_inserts[0].values.get("total_volume"),
        Some(&Value::Float64(500.0))
    );

    // Rollback block 1000 — alice's only block
    db.rollback(999).unwrap();

    let batch = db.flush().unwrap();

    // The MV group for alice should be deleted since she has no data left
    let mv_deletes: Vec<_> = batch
        .records_for("wallet_volume")
        .iter()
        .filter(|r| r.operation == DeltaOperation::Delete)
        .collect();
    assert_eq!(
        mv_deletes.len(),
        1,
        "expected Delete delta for fully rolled-back MV group"
    );
    assert_eq!(
        mv_deletes[0].key.get("wallet"),
        Some(&Value::String("alice".to_string()))
    );
}

#[test]
fn partial_rollback_emits_update_not_delete() {
    // When a wallet has data across multiple blocks and only some are
    // rolled back, the MV group should emit Update (not Delete).
    let schema = r#"
        CREATE TABLE transfers (
            wallet String,
            amount Float64
        );

        CREATE MATERIALIZED VIEW wallet_volume AS
        SELECT
            wallet,
            sum(amount) AS total_volume,
            count() AS tx_count
        FROM transfers
        GROUP BY wallet;
    "#;

    let mut db = DeltaDb::open(Config::new(schema)).unwrap();

    db.process_batch(
        "transfers",
        1000,
        vec![HashMap::from([
            ("wallet".to_string(), Value::String("alice".to_string())),
            ("amount".to_string(), Value::Float64(100.0)),
        ])],
    )
    .unwrap();
    db.process_batch(
        "transfers",
        1001,
        vec![HashMap::from([
            ("wallet".to_string(), Value::String("alice".to_string())),
            ("amount".to_string(), Value::Float64(200.0)),
        ])],
    )
    .unwrap();
    db.flush();

    // Rollback only block 1001
    db.rollback(1000).unwrap();

    let batch = db.flush().unwrap();

    let mv_records: Vec<_> = batch.records_for("wallet_volume").iter().collect();
    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].operation, DeltaOperation::Update);
    assert_eq!(
        mv_records[0].values.get("total_volume"),
        Some(&Value::Float64(100.0))
    );
    assert_eq!(
        mv_records[0].values.get("tx_count"),
        Some(&Value::UInt64(1))
    );
}

#[test]
fn rollback_persists_metadata_atomically() {
    use crate::storage::memory::MemoryBackend;

    let storage = Arc::new(MemoryBackend::new());
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA).storage(storage.clone())).unwrap();

    // Process blocks 1-3
    db.process_batch("swaps", 1, vec![make_swap("ETH", 10.0)])
        .unwrap();
    db.process_batch("swaps", 2, vec![make_swap("ETH", 20.0)])
        .unwrap();
    db.process_batch("swaps", 3, vec![make_swap("ETH", 30.0)])
        .unwrap();
    db.finalize(1).unwrap();

    // Rollback to block 1
    db.rollback(1).unwrap();

    // Verify metadata was persisted — latest_block should be 1
    let latest_bytes = storage.get_meta("latest_block").unwrap().unwrap();
    let latest = u64::from_be_bytes(latest_bytes.try_into().unwrap());
    assert_eq!(latest, 1, "latest_block should be persisted after rollback");

    // Verify block_hashes only has block 1
    let hashes_bytes = storage.get_meta("block_hashes").unwrap().unwrap();
    let hashes: BTreeMap<BlockNumber, String> = serde_json::from_slice(&hashes_bytes).unwrap();
    assert!(!hashes.contains_key(&2), "block 2 hash should be removed");
    assert!(!hashes.contains_key(&3), "block 3 hash should be removed");

    // Verify raw rows for blocks 2,3 are deleted
    let rows_after = storage.get_raw_rows("swaps", 2, 3).unwrap();
    assert!(
        rows_after.is_empty(),
        "raw rows for rolled-back blocks should be deleted"
    );
}

#[test]
fn rollback_survives_simulated_restart() {
    use crate::storage::memory::MemoryBackend;

    let storage = Arc::new(MemoryBackend::new());

    // Phase 1: process and finalize
    {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA).storage(storage.clone())).unwrap();
        db.process_batch("swaps", 1, vec![make_swap("ETH", 10.0)])
            .unwrap();
        db.process_batch("swaps", 2, vec![make_swap("ETH", 20.0)])
            .unwrap();
        db.process_batch("swaps", 3, vec![make_swap("ETH", 30.0)])
            .unwrap();
        db.finalize(1).unwrap();

        // Rollback to block 1
        db.rollback(1).unwrap();
        db.flush();
    }

    // Phase 2: "restart" — open from same storage
    {
        let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA).storage(storage.clone())).unwrap();

        // latest_block should be 1 (not 3 — the ghost head)
        assert_eq!(db.latest_block(), 1);

        // Process block 2 with new data — should work correctly
        db.process_batch("swaps", 2, vec![make_swap("BTC", 50.0)])
            .unwrap();
        let batch = db.flush().unwrap();

        // Should have MV update with the new data
        let pool_vol = batch.tables.get("pool_volume").unwrap();
        assert!(!pool_vol.is_empty());
    }
}

#[test]
fn crash_recovery_replays_unfinalized_blocks() {
    // Full pipeline with reducer: ingest blocks, finalize some,
    // reopen (simulating crash), verify reducer/MV state is rebuilt
    // from raw rows and can continue processing correctly.
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: ingest blocks 1000-1002, finalize up to 1000
    {
        let mut db = DeltaDb::open(Config::with_data_dir(
            DEX_SCHEMA,
            dir.path().to_str().unwrap(),
        ))
        .unwrap();

        db.ingest(IngestInput {
            data: std::collections::HashMap::from([(
                "trades".to_string(),
                vec![
                    // Block 1000: alice buys 10 @ 2000
                    {
                        let mut r = make_trade("alice", "buy", 10.0, 2000.0);
                        r.insert("block_number".into(), Value::UInt64(1000));
                        r
                    },
                    // Block 1001: alice buys 5 @ 2100
                    {
                        let mut r = make_trade("alice", "buy", 5.0, 2100.0);
                        r.insert("block_number".into(), Value::UInt64(1001));
                        r
                    },
                    // Block 1002: alice buys 3 @ 2200
                    {
                        let mut r = make_trade("alice", "buy", 3.0, 2200.0);
                        r.insert("block_number".into(), Value::UInt64(1002));
                        r
                    },
                ],
            )]),
            rollback_chain: vec![
                BlockCursor {
                    number: 1000,
                    hash: "0xa".into(),
                },
                BlockCursor {
                    number: 1001,
                    hash: "0xb".into(),
                },
                BlockCursor {
                    number: 1002,
                    hash: "0xc".into(),
                },
            ],
            finalized_head: BlockCursor {
                number: 1000,
                hash: "0xa".into(),
            },
        })
        .unwrap();

        assert_eq!(db.latest_block(), 1002);
        assert_eq!(db.finalized_block(), 1000);
    }
    // db dropped — simulates crash

    // Phase 2: reopen and verify state was rebuilt
    {
        let mut db = DeltaDb::open(Config::with_data_dir(
            DEX_SCHEMA,
            dir.path().to_str().unwrap(),
        ))
        .unwrap();

        assert_eq!(db.latest_block(), 1002);
        assert_eq!(db.finalized_block(), 1000);

        // Process block 1003: alice sells 5 @ 2300
        // This requires correct reducer state from blocks 1000-1002:
        //   qty = 10 + 5 + 3 = 18, cost = 20000 + 10500 + 6600 = 37100
        //   avg_cost = 37100/18 ≈ 2061.11
        //   pnl = 5 * (2300 - 2061.11) = 1194.44
        db.process_batch(
            "trades",
            1003,
            vec![make_trade("alice", "sell", 5.0, 2300.0)],
        )
        .unwrap();

        let batch = db.flush().unwrap();

        let mv_records: Vec<_> = batch.records_for("position_summary").iter().collect();
        assert_eq!(mv_records.len(), 1);

        // trade_count: 3 replayed + 1 new = 4
        assert_eq!(
            mv_records[0].values.get("trade_count"),
            Some(&Value::UInt64(4))
        );

        // current_position: 18 - 5 = 13
        assert_eq!(
            mv_records[0].values.get("current_position"),
            Some(&Value::Float64(13.0))
        );

        // total_pnl: 0 + 0 + 0 + 5*(2300 - 37100/18) ≈ 1194.44
        let total_pnl = mv_records[0]
            .values
            .get("total_pnl")
            .unwrap()
            .as_f64()
            .unwrap();
        assert!((total_pnl - 1194.44).abs() < 1.0);
    }
}

#[test]
fn resolve_fork_cursor_finds_common_ancestor() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    db.ingest(IngestInput {
        data: std::collections::HashMap::from([(
            "swaps".to_string(),
            vec![
                HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(100.0)),
                    ("block_number".to_string(), Value::UInt64(100)),
                ]),
                HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(200.0)),
                    ("block_number".to_string(), Value::UInt64(101)),
                ]),
            ],
        )]),
        rollback_chain: vec![
            BlockCursor {
                number: 100,
                hash: "0xa".into(),
            },
            BlockCursor {
                number: 101,
                hash: "0xb".into(),
            },
        ],
        finalized_head: BlockCursor {
            number: 99,
            hash: "0xf".into(),
        },
    })
    .unwrap();

    // Portal says block 101 has different hash, but 100 matches
    let previous_blocks = vec![(101, "0xdifferent"), (100, "0xa")];
    let fork_cursor = db.resolve_fork_cursor(&previous_blocks).unwrap();
    assert_eq!(fork_cursor.number, 100);
    assert_eq!(fork_cursor.hash, "0xa");

    // No match at all
    let previous_blocks = vec![(101, "0xnope"), (100, "0xnope")];
    assert!(db.resolve_fork_cursor(&previous_blocks).is_none());

    // Finalized head acts as fallback anchor
    let previous_blocks = vec![(101, "0xnope"), (99, "0xf")];
    let fork_cursor = db.resolve_fork_cursor(&previous_blocks).unwrap();
    assert_eq!(fork_cursor.number, 99);
}
