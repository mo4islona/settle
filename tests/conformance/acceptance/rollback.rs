use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use settle::db::{Config, IngestInput, Settle};
use settle::error::Error;
use settle::storage::StorageBackend;
use settle::storage::memory::MemoryBackend;
use settle::test_helpers::cursor;
use settle::types::{BlockCursor, BlockNumber, ChangeBatch, ChangeOp, ChangeRecord, Value};

use crate::support::*;

#[test]
fn rollback_produces_compensating_changes() {
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    ingest_blocks(
        &mut db,
        vec![
            ("swaps".into(), 1000, vec![make_swap("ETH/USDC", 100.0)]),
            ("swaps".into(), 1001, vec![make_swap("ETH/USDC", 200.0)]),
        ],
    )
    .unwrap();

    // Rollback block 1001
    let batch = rollback_to(&mut db, 1000).unwrap().batch.unwrap();

    // Should have MV update (back to 100) and raw delete
    let mv_records: Vec<_> = batch.records_for("pool_volume").iter().collect();
    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].operation, ChangeOp::Update);
    assert_eq!(
        mv_records[0].get("total_volume"),
        Some(&Value::Float64(100.0))
    );

    assert_eq!(db.latest_block(), 1000);
}

#[test]
fn finalize_and_rollback() {
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    ingest_with_finalized(
        &mut db,
        vec![
            ("swaps".into(), 1000, vec![make_swap("ETH/USDC", 100.0)]),
            ("swaps".into(), 1001, vec![make_swap("ETH/USDC", 200.0)]),
            ("swaps".into(), 1002, vec![make_swap("ETH/USDC", 300.0)]),
        ],
        1001,
    )
    .unwrap();
    assert_eq!(db.finalized_block(), 1001);

    // Rollback block 1002
    let batch = rollback_to(&mut db, 1001).unwrap().batch.unwrap();
    let mv_records: Vec<_> = batch.records_for("pool_volume").iter().collect();
    assert_eq!(mv_records.len(), 1);
    // total should be 100 + 200 = 300
    assert_eq!(
        mv_records[0].get("total_volume"),
        Some(&Value::Float64(300.0))
    );
}

#[test]
fn full_pipeline_rollback_and_reingest() {
    let mut db = Settle::open(Config::new(DEX_SCHEMA)).unwrap();

    // Keep all blocks unfinalized so rollback to 1001 is allowed.
    ingest_with_finalized(
        &mut db,
        vec![
            (
                "trades".into(),
                1000,
                vec![make_trade("alice", "buy", 10.0, 2000.0)],
            ),
            (
                "trades".into(),
                1001,
                vec![make_trade("alice", "buy", 5.0, 2100.0)],
            ),
            (
                "trades".into(),
                1002,
                vec![make_trade("alice", "sell", 8.0, 2200.0)],
            ),
        ],
        999,
    )
    .unwrap();

    // Rollback block 1002 (the sell)
    rollback_to(&mut db, 1001).unwrap();

    // Re-ingest with different sell
    let batch = ingest_one(
        &mut db,
        "trades",
        1002,
        vec![make_trade("alice", "sell", 3.0, 2300.0)],
    )
    .unwrap()
    .unwrap();
    let mv_records: Vec<_> = batch.records_for("position_summary").iter().collect();
    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].get("trade_count"), Some(&Value::UInt64(3)));

    // position_size after: 10 + 5 - 3 = 12
    assert_eq!(
        mv_records[0].get("current_position"),
        Some(&Value::Float64(12.0))
    );
}

#[test]
fn full_rollback_emits_delete_for_mv_group() {
    // Schema: aggregate volume per wallet. A wallet that only appeared in
    // rolled-back blocks should produce a Delete change for its MV group.
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

    let mut db = Settle::open(Config::new(schema)).unwrap();

    // Marker block 999 (no alice data) so the rollback target has a stored hash.
    ingest_one(
        &mut db,
        "transfers",
        999,
        vec![HashMap::from([
            ("wallet".to_string(), Value::String("setup".to_string())),
            ("amount".to_string(), Value::Float64(0.0)),
        ])],
    )
    .unwrap();

    // Block 1000: alice appears for the first time
    let batch = ingest_one(
        &mut db,
        "transfers",
        1000,
        vec![HashMap::from([
            ("wallet".to_string(), Value::String("alice".to_string())),
            ("amount".to_string(), Value::Float64(500.0)),
        ])],
    )
    .unwrap()
    .unwrap();

    // Verify Insert was emitted for alice's MV group
    let mv_inserts: Vec<_> = batch
        .records_for("wallet_volume")
        .iter()
        .filter(|r| r.operation == ChangeOp::Insert)
        .collect();
    assert_eq!(mv_inserts.len(), 1);
    assert_eq!(
        mv_inserts[0].get("total_volume"),
        Some(&Value::Float64(500.0))
    );

    // Rollback block 1000 — alice's only block
    let batch = rollback_to(&mut db, 999).unwrap().batch.unwrap();

    // The MV group for alice should be deleted since she has no data left
    let mv_deletes: Vec<_> = batch
        .records_for("wallet_volume")
        .iter()
        .filter(|r| r.operation == ChangeOp::Delete)
        .collect();
    assert_eq!(
        mv_deletes.len(),
        1,
        "expected Delete change for fully rolled-back MV group"
    );
    assert_eq!(
        mv_deletes[0].key("wallet"),
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

    let mut db = Settle::open(Config::new(schema)).unwrap();

    ingest_blocks(
        &mut db,
        vec![
            (
                "transfers".into(),
                1000,
                vec![HashMap::from([
                    ("wallet".to_string(), Value::String("alice".to_string())),
                    ("amount".to_string(), Value::Float64(100.0)),
                ])],
            ),
            (
                "transfers".into(),
                1001,
                vec![HashMap::from([
                    ("wallet".to_string(), Value::String("alice".to_string())),
                    ("amount".to_string(), Value::Float64(200.0)),
                ])],
            ),
        ],
    )
    .unwrap();

    // Rollback only block 1001
    let batch = rollback_to(&mut db, 1000).unwrap().batch.unwrap();

    let mv_records: Vec<_> = batch.records_for("wallet_volume").iter().collect();
    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].operation, ChangeOp::Update);
    assert_eq!(
        mv_records[0].get("total_volume"),
        Some(&Value::Float64(100.0))
    );
    assert_eq!(mv_records[0].get("tx_count"), Some(&Value::UInt64(1)));
}

#[test]
fn rollback_persists_metadata_atomically() {
    use settle::storage::memory::MemoryBackend;

    let storage = Arc::new(MemoryBackend::new());
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA).storage(storage.clone())).unwrap();

    // Process blocks 1-3 with finalized head at 1.
    ingest_with_finalized(
        &mut db,
        vec![
            ("swaps".into(), 1, vec![make_swap("ETH", 10.0)]),
            ("swaps".into(), 2, vec![make_swap("ETH", 20.0)]),
            ("swaps".into(), 3, vec![make_swap("ETH", 30.0)]),
        ],
        1,
    )
    .unwrap();

    // Rollback to block 1
    rollback_to(&mut db, 1).unwrap();

    // Verify metadata was persisted — latest_block should be 1
    let latest_bytes = storage.get_meta("latest_block").unwrap().unwrap();
    let latest = u64::from_be_bytes(latest_bytes.try_into().unwrap());
    assert_eq!(latest, 1, "latest_block should be persisted after rollback");

    // Verify block_hashes only has block 1
    let hashes: BTreeMap<BlockNumber, String> =
        storage.get_block_hashes().unwrap().into_iter().collect();
    assert!(hashes.contains_key(&1), "block 1 hash should stay");
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
    use settle::storage::memory::MemoryBackend;

    let storage = Arc::new(MemoryBackend::new());

    // Phase 1: process and finalize
    {
        let mut db = Settle::open(Config::new(SIMPLE_SCHEMA).storage(storage.clone())).unwrap();
        ingest_with_finalized(
            &mut db,
            vec![
                ("swaps".into(), 1, vec![make_swap("ETH", 10.0)]),
                ("swaps".into(), 2, vec![make_swap("ETH", 20.0)]),
                ("swaps".into(), 3, vec![make_swap("ETH", 30.0)]),
            ],
            1,
        )
        .unwrap();

        // Rollback to block 1
        rollback_to(&mut db, 1).unwrap();
    }

    // Phase 2: "restart" — open from same storage
    {
        let mut db = Settle::open(Config::new(SIMPLE_SCHEMA).storage(storage.clone())).unwrap();

        // latest_block should be 1 (not 3 — the ghost head)
        assert_eq!(db.latest_block(), 1);

        // Process block 2 with new data — should work correctly
        let batch = ingest_one(&mut db, "swaps", 2, vec![make_swap("BTC", 50.0)])
            .unwrap()
            .unwrap();

        // Should have MV update with the new data
        let pool_vol = batch.tables.get("pool_volume").unwrap();
        assert!(!pool_vol.is_empty());
    }
}

#[test]
#[cfg(feature = "rocksdb")]
fn crash_recovery_replays_unfinalized_blocks() {
    // Full pipeline with reducer: ingest blocks, finalize some,
    // reopen (simulating crash), verify reducer/MV state is rebuilt
    // from raw rows and can continue processing correctly.
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: ingest blocks 1000-1002, finalize up to 1000
    {
        let mut db = Settle::open(Config::with_data_dir(
            DEX_SCHEMA,
            dir.path().to_str().unwrap(),
        ))
        .unwrap();

        ingest_input(&mut db, IngestInput {
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
                    hash: settle::test_helpers::block_hash(1000),
                },
                BlockCursor {
                    number: 1001,
                    hash: settle::test_helpers::block_hash(1001),
                },
                BlockCursor {
                    number: 1002,
                    hash: settle::test_helpers::block_hash(1002),
                },
            ],
            finalized_head: BlockCursor {
                number: 1000,
                hash: settle::test_helpers::block_hash(1000),
            },
        })
        .unwrap();

        assert_eq!(db.latest_block(), 1002);
        assert_eq!(db.finalized_block(), 1000);
    }
    // db dropped — simulates crash

    // Phase 2: reopen and verify state was rebuilt
    {
        let mut db = Settle::open(Config::with_data_dir(
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
        let batch = ingest_one(
            &mut db,
            "trades",
            1003,
            vec![make_trade("alice", "sell", 5.0, 2300.0)],
        )
        .unwrap()
        .unwrap();

        let mv_records: Vec<_> = batch.records_for("position_summary").iter().collect();
        assert_eq!(mv_records.len(), 1);

        // trade_count: 3 replayed + 1 new = 4
        assert_eq!(mv_records[0].get("trade_count"), Some(&Value::UInt64(4)));

        // current_position: 18 - 5 = 13
        assert_eq!(
            mv_records[0].get("current_position"),
            Some(&Value::Float64(13.0))
        );

        // total_pnl: 0 + 0 + 0 + 5*(2300 - 37100/18) ≈ 1194.44
        let total_pnl = mv_records[0].get("total_pnl").unwrap().as_f64().unwrap();
        assert!((total_pnl - 1194.44).abs() < 1.0);
    }
}

#[test]
fn resolve_fork_cursor_finds_common_ancestor() {
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    ingest_input(&mut db, IngestInput {
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

#[test]
fn ingest_auto_rollback_on_fork() {
    // Verify that ingest() automatically rolls back when the rollback chain
    // holds a new hash for an ingested block.
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    // Ingest blocks 1 and 2. Block 2 is unfinalized (finalizedHead = 1).
    ingest_input(&mut db, IngestInput {
        data: std::collections::HashMap::from([(
            "swaps".to_string(),
            vec![
                {
                    let mut r = make_swap("ETH", 100.0);
                    r.insert("block_number".into(), Value::UInt64(1));
                    r
                },
                {
                    let mut r = make_swap("ETH", 200.0);
                    r.insert("block_number".into(), Value::UInt64(2));
                    r
                },
            ],
        )]),
        rollback_chain: vec![
            BlockCursor {
                number: 2,
                hash: "0x2".into(),
            },
            BlockCursor {
                number: 1,
                hash: "0x1".into(),
            },
        ],
        finalized_head: BlockCursor {
            number: 1,
            hash: "0x1".into(),
        },
    })
    .unwrap();

    assert_eq!(db.latest_block(), 2);

    // Rollback: send ingest with a new hash for block 2.
    // ingest() must detect the fork and roll back to block 1.
    let batch = ingest_input(&mut db, IngestInput {
            data: std::collections::HashMap::new(),
            rollback_chain: vec![
                BlockCursor {
                    number: 2,
                    hash: "0x2b".into(),
                },
                BlockCursor {
                    number: 1,
                    hash: "0x1".into(),
                },
            ],
            finalized_head: BlockCursor {
                number: 1,
                hash: "0x1".into(),
            },
        })
        .unwrap();

    assert_eq!(
        db.latest_block(),
        1,
        "latest_block must revert to fork point"
    );

    // The batch must contain compensating changes for block 2's data
    let batch = batch.expect("rollback ingest must return a batch with compensating changes");
    let mv_records: Vec<_> = batch.records_for("pool_volume").iter().collect();
    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].operation, ChangeOp::Update);
    // After rolling back block 2's 200.0, total should be back to 100.0
    assert_eq!(
        mv_records[0].get("total_volume"),
        Some(&Value::Float64(100.0))
    );
}

#[test]
fn deferral_fork_below_finality_rejected_cleanly() {
    // Cluster B fork-safety regression (was `repro_deferral_fork_wedge`).
    // Deferral active (interval=100). Ingest blocks 1..50, each no-lag
    // (finalized == block) so finalize advances finalized_block to 50 but
    // durable_block stays at the first block (deferred, interval never
    // reached); block_hashes for 1..50 are retained (pruned only to durable=1).
    // Before the fix, a fork resolving
    // onto a sub-finalized block (30) rolled derived state back below finality,
    // left finalized_block stale at 50 > latest 30, and wedged the instance.
    // The fix refuses any rollback below finalized at the chokepoint, so the
    // sub-finality reorg is rejected cleanly and the instance stays consistent
    // and usable.
    let mut db = Settle::open(
        Config::new(SIMPLE_SCHEMA).backfill_checkpoint_interval(100),
    )
    .unwrap();

    for b in 1..=50u64 {
        let input = IngestInput {
            data: std::collections::HashMap::from([(
                "swaps".to_string(),
                vec![{
                    let mut r = make_swap("ETH", 1.0);
                    r.insert("block_number".into(), Value::UInt64(b));
                    r
                }],
            )]),
            rollback_chain: vec![BlockCursor {
                number: b,
                hash: settle::test_helpers::block_hash(b),
            }],
            finalized_head: BlockCursor {
                number: b,
                hash: settle::test_helpers::block_hash(b),
            },
        };
        ingest_input(&mut db, input).unwrap();
    }

    assert_eq!(db.finalized_block(), 50, "finalized advanced");
    assert_eq!(db.durable_block(), 1, "durable deferred after the first block");

    // Fork: a new canonical chain that shares blocks 1..30 but diverges at 31,
    // i.e. a reorg resolving to common ancestor 30 — BELOW finalized (50).
    // resolve_fork_cursor_bounded iterates DESC; 31 hash differs, 30 matches.
    let fork_chain = vec![
        BlockCursor { number: 31, hash: "0xforked31".into() },
        BlockCursor { number: 30, hash: settle::test_helpers::block_hash(30) },
    ];
    assert!(
        handle_fork(&mut db, fork_chain).is_err(),
        "fork resolving below finalized must be rejected — finality is irreversible"
    );

    // No partial rollback: watermarks untouched, finalized <= latest preserved.
    assert_eq!(db.finalized_block(), 50, "rejected fork must not mutate finalized");
    assert_eq!(db.durable_block(), 1, "rejected fork must not mutate durable");
    assert_eq!(db.latest_block(), 50, "rejected fork must not roll back latest");

    // Instance is NOT wedged: a valid no-lag continuation still ingests and
    // advances both watermarks.
    let next = IngestInput {
        data: std::collections::HashMap::from([(
            "swaps".to_string(),
            vec![{
                let mut r = make_swap("ETH", 1.0);
                r.insert("block_number".into(), Value::UInt64(51));
                r
            }],
        )]),
        rollback_chain: vec![BlockCursor {
            number: 51,
            hash: settle::test_helpers::block_hash(51),
        }],
        finalized_head: BlockCursor {
            number: 51,
            hash: settle::test_helpers::block_hash(51),
        },
    };
    ingest_input(&mut db, next).expect("instance must remain usable after a rejected fork");
    assert_eq!(db.finalized_block(), 51);
    assert_eq!(db.latest_block(), 51);
}

#[test]
fn ingest_auto_rollback_full_when_no_common_ancestor() {
    // When no common ancestor exists in the new chain, ingest() does a full
    // rollback to block 0 and processes fresh data.
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    ingest_input(&mut db, IngestInput {
        data: std::collections::HashMap::from([(
            "swaps".to_string(),
            vec![{
                let mut r = make_swap("ETH", 100.0);
                r.insert("block_number".into(), Value::UInt64(1));
                r
            }],
        )]),
        rollback_chain: vec![BlockCursor {
            number: 1,
            hash: "0xold1".into(),
        }],
        finalized_head: BlockCursor {
            number: 0,
            hash: "0x0".into(),
        },
    })
    .unwrap();

    assert_eq!(db.latest_block(), 1);

    // New chain has completely different hashes — no common ancestor
    let batch = ingest_input(&mut db, IngestInput {
            data: std::collections::HashMap::from([(
                "swaps".to_string(),
                vec![{
                    let mut r = make_swap("BTC", 500.0);
                    r.insert("block_number".into(), Value::UInt64(1));
                    r
                }],
            )]),
            rollback_chain: vec![BlockCursor {
                number: 1,
                hash: "0xnew1".into(),
            }],
            finalized_head: BlockCursor {
                number: 0,
                hash: "0x0".into(),
            },
        })
        .unwrap()
        .expect("must return a batch");

    assert_eq!(db.latest_block(), 1);

    // Only BTC data should be present (ETH was rolled back)
    let mv_records: Vec<_> = batch.records_for("pool_volume").iter().collect();
    let btc = mv_records
        .iter()
        .find(|r| r.key("pool") == Some(&Value::String("BTC".into())));
    assert!(btc.is_some(), "BTC must appear after re-ingest");
}

#[test]
fn ingest_fork_detection_robust_against_asc_rollback_chain() {
    // If the portal sends rollbackChain in ascending order (oldest first),
    // ingest() must still detect fork correctly (no false rollback, no deep rollback).
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    // Ingest blocks 1, 2, 3. Block 3 is unfinalized.
    ingest_input(&mut db, IngestInput {
        data: std::collections::HashMap::from([(
            "swaps".to_string(),
            vec![
                {
                    let mut r = make_swap("ETH", 10.0);
                    r.insert("block_number".into(), Value::UInt64(1));
                    r
                },
                {
                    let mut r = make_swap("ETH", 20.0);
                    r.insert("block_number".into(), Value::UInt64(2));
                    r
                },
                {
                    let mut r = make_swap("ETH", 30.0);
                    r.insert("block_number".into(), Value::UInt64(3));
                    r
                },
            ],
        )]),
        rollback_chain: vec![
            BlockCursor {
                number: 3,
                hash: "0x3".into(),
            },
            BlockCursor {
                number: 2,
                hash: "0x2".into(),
            },
            BlockCursor {
                number: 1,
                hash: "0x1".into(),
            },
        ],
        finalized_head: BlockCursor {
            number: 1,
            hash: "0x1".into(),
        },
    })
    .unwrap();

    assert_eq!(db.latest_block(), 3);

    // Second ingest: SAME chain but rollbackChain sent in ASCENDING order.
    // No fork has occurred — ingest() must NOT roll back anything.
    let batch = ingest_input(&mut db, IngestInput {
            data: std::collections::HashMap::new(),
            rollback_chain: vec![
                // ASC order — oldest first (wrong but must be tolerated)
                BlockCursor {
                    number: 2,
                    hash: "0x2".into(),
                },
                BlockCursor {
                    number: 3,
                    hash: "0x3".into(),
                },
            ],
            finalized_head: BlockCursor {
                number: 1,
                hash: "0x1".into(),
            },
        })
        .unwrap();

    // No rollback should have happened — latest_block must remain at 3
    assert_eq!(
        db.latest_block(),
        3,
        "ASC rollbackChain must not trigger spurious rollback"
    );
    // Empty data + no rollback → None batch
    assert!(batch.is_none(), "no change with empty data and no fork");
}

#[test]
fn ingest_no_spurious_rollback_on_multi_batch_advance() {
    // ingest() must NOT roll back when called in multiple batches
    // where each batch only contains NEW blocks (not yet in block_hashes).
    // Previously, resolve_fork_cursor would find an old finalized anchor and
    // trigger a rollback on every batch after the first.
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    // Batch 1: blocks 1-3 (finalized=1, unfinalized=2,3)
    ingest_input(&mut db, IngestInput {
        data: std::collections::HashMap::from([(
            "swaps".to_string(),
            vec![
                {
                    let mut r = make_swap("ETH", 10.0);
                    r.insert("block_number".into(), Value::UInt64(1));
                    r
                },
                {
                    let mut r = make_swap("ETH", 20.0);
                    r.insert("block_number".into(), Value::UInt64(2));
                    r
                },
                {
                    let mut r = make_swap("ETH", 30.0);
                    r.insert("block_number".into(), Value::UInt64(3));
                    r
                },
            ],
        )]),
        rollback_chain: vec![
            BlockCursor {
                number: 3,
                hash: "0x3".into(),
            },
            BlockCursor {
                number: 2,
                hash: "0x2".into(),
            },
        ],
        finalized_head: BlockCursor {
            number: 1,
            hash: "0x1".into(),
        },
    })
    .unwrap();

    assert_eq!(db.latest_block(), 3);

    // Batch 2: only NEW blocks 4-5, rollbackChain contains only batch blocks.
    // block_hashes already has {1,2,3} from batch 1.
    // Without the "advancing" guard, resolve_fork_cursor would find block 1
    // (finalized anchor) and falsely rollback to 1.
    let batch = ingest_input(&mut db, IngestInput {
            data: std::collections::HashMap::from([(
                "swaps".to_string(),
                vec![
                    {
                        let mut r = make_swap("ETH", 40.0);
                        r.insert("block_number".into(), Value::UInt64(4));
                        r
                    },
                    {
                        let mut r = make_swap("ETH", 50.0);
                        r.insert("block_number".into(), Value::UInt64(5));
                        r
                    },
                ],
            )]),
            // Only current batch blocks (as pipes-sdk's extractRollbackChain would produce)
            rollback_chain: vec![
                BlockCursor {
                    number: 5,
                    hash: "0x5".into(),
                },
                BlockCursor {
                    number: 4,
                    hash: "0x4".into(),
                },
            ],
            finalized_head: BlockCursor {
                number: 1,
                hash: "0x1".into(),
            },
        })
        .unwrap()
        .expect("must return a batch with new data");

    // No rollback — latest must advance, not regress
    assert_eq!(
        db.latest_block(),
        5,
        "multi-batch advance must not rollback"
    );

    // Batch 2 data should be there (40 + 50 = 90, plus 10+20+30=60 from batch 1, total 150)
    let mv_records: Vec<_> = batch.records_for("pool_volume").iter().collect();
    assert!(!mv_records.is_empty(), "batch 2 must produce MV updates");
}

fn record_for<'a>(batch: &'a ChangeBatch, table: &str, key: &str, value: &str) -> &'a ChangeRecord {
    batch
        .records_for(table)
        .iter()
        .find(|r| r.key(key) == Some(&Value::String(value.into())))
        .unwrap()
}

/// Finality can reach an MV group's block in a later ingest that has no rows
/// for the group. The group must be persisted then: recovery replays only the
/// blocks above finality.
#[test]
fn finality_only_ingest_persists_mv_group() {
    let storage = Arc::new(MemoryBackend::new());

    {
        let mut db = Settle::open(Config::new(SIMPLE_SCHEMA).storage(storage.clone())).unwrap();
        ingest_with_finalized(&mut db, vec![("swaps".into(), 1, vec![make_swap("ETH", 10.0)])], 0)
            .unwrap();
        ingest_with_finalized(&mut db, vec![], 1).unwrap();
        assert_eq!(db.finalized_block(), 1);
    }

    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA).storage(storage)).unwrap();
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 2, vec![make_swap("ETH", 10.0)])], 1)
        .unwrap()
        .unwrap();
    let eth = record_for(&batch, "pool_volume", "pool", "ETH");
    assert_eq!(eth.get("total_volume"), Some(&Value::Float64(20.0)));
    assert_eq!(eth.get("swap_count"), Some(&Value::UInt64(2)));
}

/// Same with a reducer in front of the MV.
#[test]
fn finality_only_ingest_persists_reducer_and_mv_state() {
    let storage = Arc::new(MemoryBackend::new());

    {
        let mut db = Settle::open(Config::new(DEX_SCHEMA).storage(storage.clone())).unwrap();
        ingest_with_finalized(
            &mut db,
            vec![("trades".into(), 1, vec![make_trade("alice", "buy", 10.0, 2000.0)])],
            0,
        )
        .unwrap();
        ingest_with_finalized(&mut db, vec![], 1).unwrap();
    }

    let mut db = Settle::open(Config::new(DEX_SCHEMA).storage(storage)).unwrap();
    let batch = ingest_with_finalized(
        &mut db,
        vec![("trades".into(), 2, vec![make_trade("alice", "sell", 5.0, 2100.0)])],
        1,
    )
    .unwrap()
    .unwrap();
    let alice = record_for(&batch, "position_summary", "user", "alice");
    assert_eq!(alice.get("trade_count"), Some(&Value::UInt64(2)));
    assert_eq!(alice.get("current_position"), Some(&Value::Float64(5.0)));
    // 5 * (2100 - 2000)
    let pnl = alice.get("total_pnl").and_then(|v| v.as_f64()).unwrap();
    assert!((pnl - 500.0).abs() < 1e-9, "total_pnl = {pnl}");
}

/// Same through backfill deferral: the finalize that reaches the block is
/// deferred, and the next checkpoint must write the group.
#[test]
fn checkpoint_persists_mv_group_finalized_during_deferral() {
    let storage = Arc::new(MemoryBackend::new());
    let config = || {
        Config::new(SIMPLE_SCHEMA)
            .storage(storage.clone())
            .backfill_checkpoint_interval(2)
    };

    {
        let mut db = Settle::open(config()).unwrap();
        // Finality lags: this finalize persists.
        ingest_with_finalized(&mut db, vec![("swaps".into(), 1, vec![make_swap("ETH", 10.0)])], 0)
            .unwrap();
        // No lag, interval not reached: deferred.
        ingest_with_finalized(&mut db, vec![], 1).unwrap();
        assert_eq!(db.durable_block(), 0);
        // The interval counts blocks of data, not finality.
        ingest_with_finalized(&mut db, vec![], 2).unwrap();
        assert_eq!(db.durable_block(), 0);
        // Interval reached by another pool: the checkpoint writes ETH too.
        ingest_with_finalized(&mut db, vec![("swaps".into(), 2, vec![make_swap("BTC", 1.0)])], 2)
            .unwrap();
        assert_eq!(db.durable_block(), 2);
    }

    let mut db = Settle::open(config()).unwrap();
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 3, vec![make_swap("ETH", 10.0)])], 3)
        .unwrap()
        .unwrap();
    let eth = record_for(&batch, "pool_volume", "pool", "ETH");
    assert_eq!(eth.get("total_volume"), Some(&Value::Float64(20.0)));
}

/// Swaps of pool ETH, one per block, with block hashes from `cursor` unless
/// the chain says otherwise.
fn eth_swaps(
    blocks: &[(BlockNumber, f64)],
    rollback_chain: Vec<BlockCursor>,
    finalized_head: BlockCursor,
) -> IngestInput {
    let rows = blocks
        .iter()
        .map(|&(block, amount)| {
            let mut row = make_swap("ETH", amount);
            row.insert("block_number".into(), Value::UInt64(block));
            row
        })
        .collect();
    IngestInput {
        data: HashMap::from([("swaps".to_string(), rows)]),
        rollback_chain,
        finalized_head,
    }
}

fn eth_volume(batch: &ChangeBatch) -> Option<Value> {
    batch
        .records_for("pool_volume")
        .iter()
        .find(|r| r.key("pool") == Some(&Value::String("ETH".into())))
        .and_then(|r| r.get("total_volume").cloned())
}

fn new_hash(number: BlockNumber) -> BlockCursor {
    BlockCursor {
        number,
        hash: format!("0xnew{number}"),
    }
}

/// A chain that leaves out ingested blocks shows no fork: blocks sent with
/// no rollback chain all count, also after a heartbeat whose finality is
/// below the latest block.
#[test]
fn ingest_without_rollback_chain_keeps_earlier_blocks() {
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    for block in [1, 2] {
        ingest_input(&mut db, eth_swaps(&[(block, 10.0)], vec![], cursor(0))).unwrap();
    }
    let heartbeat = ingest_input(&mut db, eth_swaps(&[], vec![], cursor(0))).unwrap();
    assert!(heartbeat.is_none());
    assert_eq!(db.latest_block(), 2);

    let batch = ingest_input(&mut db, eth_swaps(&[(3, 10.0)], vec![], cursor(0)))
        .unwrap()
        .unwrap();
    assert_eq!(eth_volume(&batch), Some(Value::Float64(30.0)));
}

/// A chain that extends past the latest block but holds a new hash for an
/// ingested block shows a fork there. The ingest rolls back the old block,
/// so its data does not stay under the new hash.
#[test]
fn advancing_chain_with_a_new_hash_rolls_back_the_old_block() {
    for new_blocks in [&[(2, 20.0), (3, 30.0)][..], &[(3, 30.0)]] {
        let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();
        let old = eth_swaps(&[(1, 10.0), (2, 100.0)], vec![cursor(1), cursor(2)], cursor(0));
        ingest_input(&mut db, old).unwrap();

        let chain = vec![cursor(1), new_hash(2), new_hash(3)];
        let batch = ingest_input(&mut db, eth_swaps(new_blocks, chain, cursor(0)))
            .unwrap()
            .unwrap();
        let expected = 10.0 + new_blocks.iter().map(|&(_, amount)| amount).sum::<f64>();
        assert_eq!(eth_volume(&batch), Some(Value::Float64(expected)), "{new_blocks:?}");

        // The old hash of block 2 is no common ancestor.
        let fork = handle_fork(&mut db, vec![cursor(2), cursor(1)]).unwrap();
        assert_eq!(fork.cursor, cursor(1), "{new_blocks:?}");
    }
}

/// A new hash for a finalized block is a fork that no rollback may follow.
/// The ingest fails and changes nothing.
#[test]
fn new_hash_for_a_finalized_block_is_rejected() {
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();
    ingest_input(&mut db, eth_swaps(&[(1, 10.0), (2, 10.0)], vec![cursor(2)], cursor(1))).unwrap();

    let err = db
        .ingest(eth_swaps(&[(3, 10.0)], vec![cursor(3)], new_hash(1)))
        .unwrap_err();
    assert!(matches!(err, Error::InvalidOperation(_)), "{err}");
    assert_eq!(db.latest_cursor(), Some(cursor(2)));

    let batch = ingest_input(&mut db, eth_swaps(&[(3, 10.0)], vec![cursor(3)], cursor(1)))
        .unwrap()
        .unwrap();
    assert_eq!(eth_volume(&batch), Some(Value::Float64(30.0)));
}
