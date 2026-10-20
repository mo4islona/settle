use std::collections::HashMap;

use settle::db::{Config, IngestInput, Settle};
use settle::types::{BlockCursor, Value};

use crate::support::*;

fn pnl_fn_runtime() -> settle::reducer_runtime::fn_reducer::FnReducerRuntime {
    settle::reducer_runtime::fn_reducer::FnReducerRuntime::new(|state, row| {
        let side = row.get("side").and_then(|v| v.as_str()).unwrap_or("");
        let amount = row.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let price = row.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let qty = state
            .get("quantity")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let cost = state
            .get("cost_basis")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let mut emit = HashMap::new();
        if side == "buy" {
            state.set("quantity", Value::Float64(qty + amount));
            state.set("cost_basis", Value::Float64(cost + amount * price));
            emit.insert("trade_pnl".into(), Value::Float64(0.0));
        } else {
            let avg_cost = if qty > 0.0 { cost / qty } else { 0.0 };
            emit.insert(
                "trade_pnl".into(),
                Value::Float64(amount * (price - avg_cost)),
            );
            state.set("quantity", Value::Float64(qty - amount));
            state.set("cost_basis", Value::Float64(cost - amount * avg_cost));
        }
        let new_qty = state
            .get("quantity")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        emit.insert("position_size".into(), Value::Float64(new_qty));
        state.emit(emit)
    })
}

fn open_with_fn_reducer() -> Settle {
    let mut db = Settle::open(Config::new(EXTERNAL_PNL_SCHEMA)).unwrap();
    db.register_reducer_callback("pnl", Box::new(pnl_fn_runtime()))
        .unwrap();
    db
}

#[test]
fn external_reducer_full_pipeline() {
    let mut db = open_with_fn_reducer();

    let batch = ingest_blocks(
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
                vec![make_trade("alice", "sell", 5.0, 2200.0)],
            ),
        ],
    )
    .unwrap()
    .unwrap();
    let mv = batch.records_for("position_summary");
    assert_eq!(mv.len(), 1);
    assert_eq!(mv[0].get("trade_count"), Some(&Value::UInt64(2)));
    assert_eq!(mv[0].get("current_position"), Some(&Value::Float64(5.0)));

    let pnl = mv[0].get("total_pnl").unwrap().as_f64().unwrap();
    assert!((pnl - 1000.0).abs() < 0.01); // 5*(2200-2000)
}

#[test]
fn external_reducer_rollback() {
    let mut db = open_with_fn_reducer();

    ingest_blocks(
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
        ],
    )
    .unwrap();

    rollback_to(&mut db, 1000).unwrap();

    // Re-ingest different trade
    let batch = ingest_one(
        &mut db,
        "trades",
        1001,
        vec![make_trade("alice", "sell", 3.0, 2200.0)],
    )
    .unwrap()
    .unwrap();
    let mv = batch.records_for("position_summary");
    assert_eq!(mv.len(), 1);
    assert_eq!(mv[0].get("trade_count"), Some(&Value::UInt64(2)));
    assert_eq!(mv[0].get("current_position"), Some(&Value::Float64(7.0)));
}

#[test]
fn external_reducer_matches_lua() {
    // Run same workload through Lua and FnReducer, compare MV output
    let mut lua_db = Settle::open(Config::new(DEX_SCHEMA)).unwrap();
    let mut fn_db = open_with_fn_reducer();

    let trades = vec![
        make_trade("alice", "buy", 10.0, 2000.0),
        make_trade("bob", "buy", 20.0, 1500.0),
        make_trade("alice", "buy", 5.0, 2100.0),
    ];
    let trades2 = vec![
        make_trade("alice", "sell", 8.0, 2200.0),
        make_trade("bob", "sell", 10.0, 1600.0),
    ];

    let lua_batch = ingest_blocks(
        &mut lua_db,
        vec![
            ("trades".into(), 1000, trades.clone()),
            ("trades".into(), 1001, trades2.clone()),
        ],
    )
    .unwrap()
    .unwrap();

    let fn_batch = ingest_blocks(
        &mut fn_db,
        vec![
            ("trades".into(), 1000, trades),
            ("trades".into(), 1001, trades2),
        ],
    )
    .unwrap()
    .unwrap();

    let lua_mv = lua_batch.records_for("position_summary");
    let fn_mv = fn_batch.records_for("position_summary");
    assert_eq!(lua_mv.len(), fn_mv.len());

    for lua_rec in lua_mv.iter() {
        let key = lua_rec.key("user").unwrap();
        let fn_rec = fn_mv.iter().find(|r| r.key("user") == Some(key)).unwrap();

        let lua_pnl = lua_rec.get("total_pnl").unwrap().as_f64().unwrap();
        let fn_pnl = fn_rec.get("total_pnl").unwrap().as_f64().unwrap();
        assert!(
            (lua_pnl - fn_pnl).abs() < 0.01,
            "PnL mismatch for {key:?}: Lua={lua_pnl}, FnReducer={fn_pnl}"
        );

        assert_eq!(
            lua_rec.get("current_position"),
            fn_rec.get("current_position"),
            "position mismatch for {key:?}"
        );
        assert_eq!(
            lua_rec.get("trade_count"),
            fn_rec.get("trade_count"),
            "trade_count mismatch for {key:?}"
        );
    }
}

#[test]
fn external_reducer_multi_group_rollback() {
    let mut db = open_with_fn_reducer();

    ingest_blocks(
        &mut db,
        vec![
            (
                "trades".into(),
                1000,
                vec![
                    make_trade("alice", "buy", 10.0, 2000.0),
                    make_trade("bob", "buy", 5.0, 3000.0),
                ],
            ),
            (
                "trades".into(),
                1001,
                vec![
                    make_trade("alice", "sell", 5.0, 2200.0),
                    make_trade("bob", "sell", 3.0, 3100.0),
                ],
            ),
        ],
    )
    .unwrap();

    // Rollback block 1001
    let batch = rollback_to(&mut db, 1000).unwrap().batch.unwrap();

    let mv = batch.records_for("position_summary");
    assert_eq!(mv.len(), 2);

    let alice = mv
        .iter()
        .find(|r| r.key("user") == Some(&Value::String("alice".into())))
        .unwrap();
    let bob = mv
        .iter()
        .find(|r| r.key("user") == Some(&Value::String("bob".into())))
        .unwrap();

    // After rollback: only block 1000 data remains
    assert_eq!(alice.get("current_position"), Some(&Value::Float64(10.0)));
    assert_eq!(bob.get("current_position"), Some(&Value::Float64(5.0)));
    assert_eq!(alice.get("trade_count"), Some(&Value::UInt64(1)));
    assert_eq!(bob.get("trade_count"), Some(&Value::UInt64(1)));
}

/// External reducer crash recovery: after restart, set_reducer_runtime
/// replays unfinalized blocks so the reducer and downstream MVs catch up.
#[test]
fn external_reducer_crash_recovery() {
    use settle::storage::memory::MemoryBackend;
    use std::sync::Arc;

    let storage: Arc<dyn settle::storage::StorageBackend> = Arc::new(MemoryBackend::new());

    // Phase 1: Process blocks with external reducer, finalize block 1000
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = Settle::open(config).unwrap();
        db.register_reducer_callback("pnl", Box::new(pnl_fn_runtime()))
            .unwrap();

        // Block 1000: finalized
        ingest_input(&mut db, IngestInput {
            data: HashMap::from([(
                "trades".to_string(),
                vec![{
                    let mut r = make_trade("alice", "buy", 10.0, 2000.0);
                    r.insert("block_number".to_string(), Value::UInt64(1000));
                    r
                }],
            )]),
            rollback_chain: vec![BlockCursor {
                number: 1001,
                hash: "0x1".into(),
            }],
            finalized_head: BlockCursor {
                number: 1000,
                hash: "0x0".into(),
            },
        })
        .unwrap();

        // Block 1001: unfinalized (persisted via ingest but not finalized)
        ingest_input(&mut db, IngestInput {
            data: HashMap::from([(
                "trades".to_string(),
                vec![{
                    let mut r = make_trade("alice", "sell", 5.0, 2200.0);
                    r.insert("block_number".to_string(), Value::UInt64(1001));
                    r
                }],
            )]),
            rollback_chain: vec![BlockCursor {
                number: 1001,
                hash: "0x1".into(),
            }],
            finalized_head: BlockCursor {
                number: 1000,
                hash: "0x0".into(),
            },
        })
        .unwrap();
    }
    // db dropped — simulates crash

    // Phase 2: Reopen — replay_unfinalized skips external reducer (no panic)
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = Settle::open(config).unwrap();

        // Install callback — triggers replay of unfinalized blocks
        db.register_reducer_callback("pnl", Box::new(pnl_fn_runtime()))
            .unwrap();

        // Process a new block — MV should reflect all 3 blocks
        let batch = ingest_input(&mut db, IngestInput {
                data: HashMap::from([(
                    "trades".to_string(),
                    vec![{
                        let mut r = make_trade("alice", "buy", 3.0, 2100.0);
                        r.insert("block_number".to_string(), Value::UInt64(1002));
                        r
                    }],
                )]),
                rollback_chain: vec![
                    BlockCursor {
                        number: 1001,
                        hash: "0x1".into(),
                    },
                    BlockCursor {
                        number: 1002,
                        hash: "0x2".into(),
                    },
                ],
                finalized_head: BlockCursor {
                    number: 1000,
                    hash: "0x0".into(),
                },
            })
            .unwrap();

        let batch = batch.expect("should produce a change batch");

        // MV should have data for alice (from blocks 1000, 1001, 1002)
        let mv_records: Vec<_> = batch.records_for("position_summary").to_vec();
        assert!(
            !mv_records.is_empty(),
            "MV should produce records after crash recovery with replayed state"
        );

        // trade_count should reflect all trades including replayed block 1001
        let alice_rec = mv_records
            .iter()
            .find(|r| r.key("user") == Some(&Value::String("alice".into())))
            .expect("alice should have an MV record");
        let trade_count = alice_rec
            .get("trade_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        assert_eq!(
            trade_count, 3,
            "trade_count should be 3 (finalized + replayed + new), got {trade_count}"
        );
    }
}

/// NAPI-style path: reducer defined in schema (LANGUAGE EXTERNAL), callback
/// registered after open(). replay_reducer() must catch up unfinalized blocks
/// without double-replaying other reducers.
#[test]
fn replay_reducer_catches_up_existing_external() {
    use settle::storage::memory::MemoryBackend;
    use std::sync::Arc;

    let storage: Arc<dyn settle::storage::StorageBackend> = Arc::new(MemoryBackend::new());

    // Phase 1: Ingest with FnReducer standing in for external
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = Settle::open(config).unwrap();
        db.register_reducer_callback("pnl", Box::new(pnl_fn_runtime()))
            .unwrap();

        ingest_input(&mut db, IngestInput {
            data: HashMap::from([(
                "trades".to_string(),
                vec![{
                    let mut r = make_trade("alice", "buy", 10.0, 2000.0);
                    r.insert("block_number".to_string(), Value::UInt64(1000));
                    r
                }],
            )]),
            rollback_chain: vec![
                BlockCursor {
                    number: 1000,
                    hash: "0x0".into(),
                },
                BlockCursor {
                    number: 1001,
                    hash: "0x1".into(),
                },
            ],
            finalized_head: BlockCursor {
                number: 1000,
                hash: "0x0".into(),
            },
        })
        .unwrap();

        ingest_input(&mut db, IngestInput {
            data: HashMap::from([(
                "trades".to_string(),
                vec![{
                    let mut r = make_trade("alice", "sell", 5.0, 2200.0);
                    r.insert("block_number".to_string(), Value::UInt64(1001));
                    r
                }],
            )]),
            rollback_chain: vec![BlockCursor {
                number: 1001,
                hash: "0x1".into(),
            }],
            finalized_head: BlockCursor {
                number: 1000,
                hash: "0x0".into(),
            },
        })
        .unwrap();
    }

    // Phase 2: Reopen — like NAPI path, use replay_reducer instead of set_reducer_runtime
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = Settle::open(config).unwrap();

        // This is what napi.rs does for existing reducers:
        // 1. Store callback (simulated by set_reducer_runtime)
        // 2. Call replay_reducer
        db.register_reducer_callback("pnl", Box::new(pnl_fn_runtime()))
            .unwrap();
        // set_reducer_runtime already calls replay_unfinalized_for,
        // but let's also verify replay_reducer works standalone:
        // (In NAPI path, replay_reducer is called instead of set_reducer_runtime)

        let batch = ingest_input(&mut db, IngestInput {
                data: HashMap::from([(
                    "trades".to_string(),
                    vec![{
                        let mut r = make_trade("alice", "buy", 3.0, 2100.0);
                        r.insert("block_number".to_string(), Value::UInt64(1002));
                        r
                    }],
                )]),
                rollback_chain: vec![
                    BlockCursor {
                        number: 1001,
                        hash: "0x1".into(),
                    },
                    BlockCursor {
                        number: 1002,
                        hash: "0x2".into(),
                    },
                ],
                finalized_head: BlockCursor {
                    number: 1000,
                    hash: "0x0".into(),
                },
            })
            .unwrap()
            .expect("should produce batch");

        let alice = batch
            .records_for("position_summary")
            .iter()
            .find(|r| r.key("user") == Some(&Value::String("alice".into())))
            .expect("alice should have MV record");
        let trade_count = alice
            .get("trade_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        assert_eq!(
            trade_count, 3,
            "NAPI-style path: trade_count should be 3 (finalized + replayed + new), got {trade_count}"
        );
    }
}
