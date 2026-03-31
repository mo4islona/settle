use super::test_helpers::*;
use super::*;
use crate::types::{BlockCursor, DeltaOperation, Value};
use std::collections::HashMap;

#[test]
fn open_with_valid_schema() {
    let db = DeltaDb::open(Config::new(SIMPLE_SCHEMA));
    assert!(db.is_ok());
}

#[test]
fn open_with_invalid_schema() {
    let db = DeltaDb::open(Config::new("INVALID SQL GARBAGE"));
    assert!(db.is_err());
}

#[test]
fn simple_ingest_and_flush() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    db.process_batch(
        "swaps",
        1000,
        vec![make_swap("ETH/USDC", 100.0), make_swap("ETH/USDC", 200.0)],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    assert_eq!(batch.sequence, 1);
    assert_eq!(batch.latest_head.as_ref().map(|c| c.number), Some(1000));

    // 2 raw inserts + 1 MV insert = 3 records
    assert_eq!(batch.record_count(), 3);

    let mv_records: Vec<_> = batch.records_for("pool_volume").iter().collect();
    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].operation, DeltaOperation::Insert);
    assert_eq!(
        mv_records[0].values.get("total_volume"),
        Some(&Value::Float64(300.0))
    );
}

#[test]
fn multiple_blocks_merge_in_buffer() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    db.process_batch("swaps", 1000, vec![make_swap("ETH/USDC", 100.0)])
        .unwrap();
    db.process_batch("swaps", 1001, vec![make_swap("ETH/USDC", 200.0)])
        .unwrap();

    let batch = db.flush().unwrap();

    // MV records should be merged: Insert + Update -> Insert with latest values
    let mv_records: Vec<_> = batch.records_for("pool_volume").iter().collect();
    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].operation, DeltaOperation::Insert);
    assert_eq!(
        mv_records[0].values.get("total_volume"),
        Some(&Value::Float64(300.0))
    );
}

#[test]
fn full_pipeline_with_reducer() {
    let mut db = DeltaDb::open(Config::new(DEX_SCHEMA)).unwrap();

    // Block 1000: alice buys 10 @ 2000
    db.process_batch(
        "trades",
        1000,
        vec![make_trade("alice", "buy", 10.0, 2000.0)],
    )
    .unwrap();

    // Block 1001: alice sells 5 @ 2200
    db.process_batch(
        "trades",
        1001,
        vec![make_trade("alice", "sell", 5.0, 2200.0)],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    let mv_records: Vec<_> = batch.records_for("position_summary").iter().collect();
    assert_eq!(mv_records.len(), 1);

    // trade_count should be 2
    assert_eq!(
        mv_records[0].values.get("trade_count"),
        Some(&Value::UInt64(2))
    );
    // current_position = last(position_size) = 5.0
    assert_eq!(
        mv_records[0].values.get("current_position"),
        Some(&Value::Float64(5.0))
    );

    // total_pnl: trade 1 = 0 (buy), trade 2 = 5*(2200-2000) = 1000
    let total_pnl = mv_records[0]
        .values
        .get("total_pnl")
        .unwrap()
        .as_f64()
        .unwrap();
    assert!((total_pnl - 1000.0).abs() < 0.01);
}

#[test]
fn backpressure_signal() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA).max_buffer_size(3)).unwrap();

    // First batch: 2 raw + 1 MV = 3 records → buffer full
    let full = db
        .process_batch(
            "swaps",
            1000,
            vec![make_swap("ETH/USDC", 100.0), make_swap("ETH/USDC", 200.0)],
        )
        .unwrap();

    assert!(full);
    assert!(db.is_backpressured());

    // Flush clears buffer
    db.flush();
    assert!(!db.is_backpressured());
}

#[test]
fn unknown_table_returns_error() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();
    let result = db.process_batch("nonexistent", 1000, vec![]);
    assert!(result.is_err());
}

#[test]
fn empty_flush_returns_none() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();
    assert!(db.flush().is_none());
}

#[test]
fn sequence_numbers_increment() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    db.process_batch("swaps", 1000, vec![make_swap("ETH/USDC", 100.0)])
        .unwrap();
    let b1 = db.flush().unwrap();

    db.process_batch("swaps", 1001, vec![make_swap("ETH/USDC", 200.0)])
        .unwrap();
    let b2 = db.flush().unwrap();

    assert_eq!(b1.sequence, 1);
    assert_eq!(b2.sequence, 2);
}

#[test]
fn ingest_groups_rows_by_block_number() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    let batch = db
        .ingest(IngestInput {
            data: std::collections::HashMap::from([(
                "swaps".to_string(),
                vec![
                    HashMap::from([
                        ("pool".to_string(), Value::String("ETH/USDC".into())),
                        ("amount".to_string(), Value::Float64(100.0)),
                        ("block_number".to_string(), Value::UInt64(1001)),
                    ]),
                    HashMap::from([
                        ("pool".to_string(), Value::String("ETH/USDC".into())),
                        ("amount".to_string(), Value::Float64(200.0)),
                        ("block_number".to_string(), Value::UInt64(1000)),
                    ]),
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
            ],
            finalized_head: BlockCursor {
                number: 999,
                hash: "0xf".into(),
            },
        })
        .unwrap();

    let batch = batch.unwrap();
    assert_eq!(batch.record_count(), 3); // 2 raw inserts + 1 MV insert
    assert_eq!(db.latest_block(), 1001);
    assert_eq!(db.finalized_block(), 999);
}

#[test]
fn ingest_stores_block_hashes_and_cursor() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    db.ingest(IngestInput {
        data: std::collections::HashMap::from([(
            "swaps".to_string(),
            vec![HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(100.0)),
                ("block_number".to_string(), Value::UInt64(1000)),
            ])],
        )]),
        rollback_chain: vec![BlockCursor {
            number: 1000,
            hash: "0xabc".into(),
        }],
        finalized_head: BlockCursor {
            number: 999,
            hash: "0xfin".into(),
        },
    })
    .unwrap();

    // Cursor should have the latest block's hash
    let cursor = db.latest_cursor().unwrap();
    assert_eq!(cursor.number, 1000);
    assert_eq!(cursor.hash, "0xabc");
}

#[test]
fn ingest_errors_on_missing_block_number() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    let result = db.ingest(IngestInput {
        data: std::collections::HashMap::from([(
            "swaps".to_string(),
            vec![HashMap::from([
                ("pool".to_string(), Value::String("ETH/USDC".into())),
                ("amount".to_string(), Value::Float64(100.0)),
                // no block_number!
            ])],
        )]),
        rollback_chain: vec![],
        finalized_head: BlockCursor {
            number: 0,
            hash: "0x0".into(),
        },
    });

    assert!(result.is_err());
}

#[test]
fn ingest_persists_and_restores_state() {
    let dir = tempfile::tempdir().unwrap();
    let schema = SIMPLE_SCHEMA;

    // Ingest some data
    {
        let mut db =
            DeltaDb::open(Config::with_data_dir(schema, dir.path().to_str().unwrap())).unwrap();

        db.ingest(IngestInput {
            data: std::collections::HashMap::from([(
                "swaps".to_string(),
                vec![HashMap::from([
                    ("pool".to_string(), Value::String("ETH/USDC".into())),
                    ("amount".to_string(), Value::Float64(100.0)),
                    ("block_number".to_string(), Value::UInt64(1000)),
                ])],
            )]),
            rollback_chain: vec![BlockCursor {
                number: 1000,
                hash: "0xabc".into(),
            }],
            finalized_head: BlockCursor {
                number: 999,
                hash: "0xfin".into(),
            },
        })
        .unwrap();
    }

    // Reopen and verify state was restored
    {
        let db =
            DeltaDb::open(Config::with_data_dir(schema, dir.path().to_str().unwrap())).unwrap();

        assert_eq!(db.latest_block(), 1000);
        assert_eq!(db.finalized_block(), 999);

        let cursor = db.latest_cursor().unwrap();
        assert_eq!(cursor.number, 1000);
        assert_eq!(cursor.hash, "0xabc");
    }
}

/// ingest() with multiple blocks must batch perf nodes into a flat array,
/// not create nested per-push arrays.
#[test]
fn ingest_batches_perf_into_single_array() {
    let mut db = DeltaDb::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    let batch = db
        .ingest(IngestInput {
            data: HashMap::from([(
                "swaps".to_string(),
                vec![
                    {
                        let mut r = make_swap("ETH", 100.0);
                        r.insert("block_number".to_string(), Value::UInt64(1000));
                        r
                    },
                    {
                        let mut r = make_swap("ETH", 200.0);
                        r.insert("block_number".to_string(), Value::UInt64(1001));
                        r
                    },
                    {
                        let mut r = make_swap("ETH", 300.0);
                        r.insert("block_number".to_string(), Value::UInt64(1002));
                        r
                    },
                ],
            )]),
            rollback_chain: vec![
                BlockCursor { number: 1002, hash: "0x2".into() },
                BlockCursor { number: 1001, hash: "0x1".into() },
                BlockCursor { number: 1000, hash: "0x0".into() },
            ],
            finalized_head: BlockCursor { number: 1000, hash: "0x0".into() },
        })
        .unwrap()
        .expect("should produce batch");

    // Single "ingest" root with aggregated children (not per-block)
    assert_eq!(batch.perf.len(), 1, "expected 1 perf node, got {}", batch.perf.len());
    let root = &batch.perf[0];
    assert_eq!(root.name, "ingest");
    assert!(root.duration_ms > 0.0);
    // "swaps" table aggregated into one child (sum of 3 blocks)
    let swaps = root.children.iter().find(|c| c.name == "swaps");
    assert!(swaps.is_some(), "expected 'swaps' child node");
    assert!(swaps.unwrap().duration_ms > 0.0);
}
