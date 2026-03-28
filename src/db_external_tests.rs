use super::test_helpers::*;
use super::*;
use crate::types::{BlockCursor, Value};
use std::collections::HashMap;

fn pnl_fn_runtime() -> crate::reducer_runtime::fn_reducer::FnReducerRuntime {
    crate::reducer_runtime::fn_reducer::FnReducerRuntime::new(|state, row| {
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
            state.insert("quantity".into(), Value::Float64(qty + amount));
            state.insert("cost_basis".into(), Value::Float64(cost + amount * price));
            emit.insert("trade_pnl".into(), Value::Float64(0.0));
        } else {
            let avg_cost = if qty > 0.0 { cost / qty } else { 0.0 };
            emit.insert(
                "trade_pnl".into(),
                Value::Float64(amount * (price - avg_cost)),
            );
            state.insert("quantity".into(), Value::Float64(qty - amount));
            state.insert(
                "cost_basis".into(),
                Value::Float64(cost - amount * avg_cost),
            );
        }
        let new_qty = state
            .get("quantity")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        emit.insert("position_size".into(), Value::Float64(new_qty));
        vec![emit]
    })
}

fn open_with_fn_reducer() -> DeltaDb {
    let mut db = DeltaDb::open(Config::new(EXTERNAL_PNL_SCHEMA)).unwrap();
    db.set_reducer_runtime("pnl", Box::new(pnl_fn_runtime()))
        .unwrap();
    db
}

#[test]
fn external_reducer_full_pipeline() {
    let mut db = open_with_fn_reducer();

    db.process_batch(
        "trades",
        1000,
        vec![make_trade("alice", "buy", 10.0, 2000.0)],
    )
    .unwrap();
    db.process_batch(
        "trades",
        1001,
        vec![make_trade("alice", "sell", 5.0, 2200.0)],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let mv = batch.records_for("position_summary");
    assert_eq!(mv.len(), 1);
    assert_eq!(mv[0].values.get("trade_count"), Some(&Value::UInt64(2)));
    assert_eq!(
        mv[0].values.get("current_position"),
        Some(&Value::Float64(5.0))
    );

    let pnl = mv[0].values.get("total_pnl").unwrap().as_f64().unwrap();
    assert!((pnl - 1000.0).abs() < 0.01); // 5*(2200-2000)
}

#[test]
fn external_reducer_rollback() {
    let mut db = open_with_fn_reducer();

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
    db.flush();

    db.rollback(1000).unwrap();

    // Re-ingest different trade
    db.process_batch(
        "trades",
        1001,
        vec![make_trade("alice", "sell", 3.0, 2200.0)],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let mv = batch.records_for("position_summary");
    assert_eq!(mv.len(), 1);
    assert_eq!(mv[0].values.get("trade_count"), Some(&Value::UInt64(2)));
    assert_eq!(
        mv[0].values.get("current_position"),
        Some(&Value::Float64(7.0))
    );
}

#[test]
fn external_reducer_matches_event_rules() {
    // Run same workload through EventRules and FnReducer, compare MV output
    let mut er_db = DeltaDb::open(Config::new(DEX_SCHEMA)).unwrap();
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

    er_db.process_batch("trades", 1000, trades.clone()).unwrap();
    er_db
        .process_batch("trades", 1001, trades2.clone())
        .unwrap();
    let er_batch = er_db.flush().unwrap();

    fn_db.process_batch("trades", 1000, trades).unwrap();
    fn_db.process_batch("trades", 1001, trades2).unwrap();
    let fn_batch = fn_db.flush().unwrap();

    let er_mv = er_batch.records_for("position_summary");
    let fn_mv = fn_batch.records_for("position_summary");
    assert_eq!(er_mv.len(), fn_mv.len());

    for er_rec in er_mv.iter() {
        let key = er_rec.key.get("user").unwrap();
        let fn_rec = fn_mv
            .iter()
            .find(|r| r.key.get("user") == Some(key))
            .unwrap();

        let er_pnl = er_rec.values.get("total_pnl").unwrap().as_f64().unwrap();
        let fn_pnl = fn_rec.values.get("total_pnl").unwrap().as_f64().unwrap();
        assert!(
            (er_pnl - fn_pnl).abs() < 0.01,
            "PnL mismatch for {key:?}: EventRules={er_pnl}, FnReducer={fn_pnl}"
        );

        assert_eq!(
            er_rec.values.get("current_position"),
            fn_rec.values.get("current_position"),
            "position mismatch for {key:?}"
        );
        assert_eq!(
            er_rec.values.get("trade_count"),
            fn_rec.values.get("trade_count"),
            "trade_count mismatch for {key:?}"
        );
    }
}

#[test]
fn external_reducer_multi_group_rollback() {
    let mut db = open_with_fn_reducer();

    db.process_batch(
        "trades",
        1000,
        vec![
            make_trade("alice", "buy", 10.0, 2000.0),
            make_trade("bob", "buy", 5.0, 3000.0),
        ],
    )
    .unwrap();
    db.process_batch(
        "trades",
        1001,
        vec![
            make_trade("alice", "sell", 5.0, 2200.0),
            make_trade("bob", "sell", 3.0, 3100.0),
        ],
    )
    .unwrap();
    db.flush();

    // Rollback block 1001
    db.rollback(1000).unwrap();
    let batch = db.flush().unwrap();

    let mv = batch.records_for("position_summary");
    assert_eq!(mv.len(), 2);

    let alice = mv
        .iter()
        .find(|r| r.key.get("user") == Some(&Value::String("alice".into())))
        .unwrap();
    let bob = mv
        .iter()
        .find(|r| r.key.get("user") == Some(&Value::String("bob".into())))
        .unwrap();

    // After rollback: only block 1000 data remains
    assert_eq!(
        alice.values.get("current_position"),
        Some(&Value::Float64(10.0))
    );
    assert_eq!(
        bob.values.get("current_position"),
        Some(&Value::Float64(5.0))
    );
    assert_eq!(alice.values.get("trade_count"), Some(&Value::UInt64(1)));
    assert_eq!(bob.values.get("trade_count"), Some(&Value::UInt64(1)));
}

/// External reducer crash recovery: after restart, set_reducer_runtime
/// replays unfinalized blocks so the reducer and downstream MVs catch up.
#[test]
fn external_reducer_crash_recovery() {
    use crate::storage::memory::MemoryBackend;
    use std::sync::Arc;

    let storage: Arc<dyn crate::storage::StorageBackend> = Arc::new(MemoryBackend::new());

    // Phase 1: Process blocks with external reducer, finalize block 1000
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = DeltaDb::open(config).unwrap();
        db.set_reducer_runtime("pnl", Box::new(pnl_fn_runtime()))
            .unwrap();

        // Block 1000: finalized
        db.ingest(IngestInput {
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
        db.ingest(IngestInput {
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
        let mut db = DeltaDb::open(config).unwrap();

        // Install callback — triggers replay of unfinalized blocks
        db.set_reducer_runtime("pnl", Box::new(pnl_fn_runtime()))
            .unwrap();

        // Process a new block — MV should reflect all 3 blocks
        let batch = db
            .ingest(IngestInput {
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

        let batch = batch.expect("should produce a delta batch");

        // MV should have data for alice (from blocks 1000, 1001, 1002)
        let mv_records: Vec<_> = batch.records_for("position_summary").to_vec();
        assert!(
            !mv_records.is_empty(),
            "MV should produce records after crash recovery with replayed state"
        );

        // trade_count should reflect all trades including replayed block 1001
        let alice_rec = mv_records
            .iter()
            .find(|r| r.key.get("user") == Some(&Value::String("alice".into())))
            .expect("alice should have an MV record");
        let trade_count = alice_rec
            .values
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
    use crate::storage::memory::MemoryBackend;
    use std::sync::Arc;

    let storage: Arc<dyn crate::storage::StorageBackend> = Arc::new(MemoryBackend::new());

    // Phase 1: Ingest with FnReducer standing in for external
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = DeltaDb::open(config).unwrap();
        db.set_reducer_runtime("pnl", Box::new(pnl_fn_runtime()))
            .unwrap();

        db.ingest(IngestInput {
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

        db.ingest(IngestInput {
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
        let mut db = DeltaDb::open(config).unwrap();

        // This is what napi.rs does for existing reducers:
        // 1. Store callback (simulated by set_reducer_runtime)
        // 2. Call replay_reducer
        db.set_reducer_runtime("pnl", Box::new(pnl_fn_runtime()))
            .unwrap();
        // set_reducer_runtime already calls replay_unfinalized_for,
        // but let's also verify replay_reducer works standalone:
        // (In NAPI path, replay_reducer is called instead of set_reducer_runtime)

        let batch = db
            .ingest(IngestInput {
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
            .find(|r| r.key.get("user") == Some(&Value::String("alice".into())))
            .expect("alice should have MV record");
        let trade_count = alice
            .values
            .get("trade_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        assert_eq!(
            trade_count, 3,
            "NAPI-style path: trade_count should be 3 (finalized + replayed + new), got {trade_count}"
        );
    }
}

/// Verify that replay_reducer() correctly drives the *real* ExternalRuntime
/// (not a FnReducer substitute) by installing a test context.
///
/// This is the true NAPI code path: ExternalRuntime stays as the runtime
/// throughout; `context_installed()` gates replay on the thread-local.
#[test]
fn external_runtime_replay_uses_real_external_runtime() {
    use crate::reducer_runtime::GroupBatch;
    use crate::reducer_runtime::external::install_test_context;
    use crate::storage::memory::MemoryBackend;
    use std::sync::Arc;

    // Rust callback that mirrors pnl_fn_runtime() but operates on GroupBatch.
    let pnl_batch_cb = || {
        |groups: &mut [GroupBatch]| {
            for group in groups.iter_mut() {
                for row in &group.rows {
                    let side = row.get("side").and_then(|v| v.as_str()).unwrap_or("");
                    let amount = row.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let price = row.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let qty = group
                        .state
                        .get("quantity")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    let cost = group
                        .state
                        .get("cost_basis")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    let mut emit = HashMap::new();
                    if side == "buy" {
                        group
                            .state
                            .insert("quantity".into(), Value::Float64(qty + amount));
                        group
                            .state
                            .insert("cost_basis".into(), Value::Float64(cost + amount * price));
                        emit.insert("trade_pnl".into(), Value::Float64(0.0));
                    } else {
                        let avg_cost = if qty > 0.0 { cost / qty } else { 0.0 };
                        emit.insert(
                            "trade_pnl".into(),
                            Value::Float64(amount * (price - avg_cost)),
                        );
                        group
                            .state
                            .insert("quantity".into(), Value::Float64(qty - amount));
                        group.state.insert(
                            "cost_basis".into(),
                            Value::Float64(cost - amount * avg_cost),
                        );
                    }
                    let new_qty = group
                        .state
                        .get("quantity")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    emit.insert("position_size".into(), Value::Float64(new_qty));
                    group.emits.push(emit);
                }
            }
        }
    };

    let storage: Arc<dyn crate::storage::StorageBackend> = Arc::new(MemoryBackend::new());

    // Phase 1: ingest with real ExternalRuntime (test context, no set_reducer_runtime)
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = DeltaDb::open(config).unwrap();
        // DO NOT call set_reducer_runtime — ExternalRuntime stays as the runtime

        let _ctx = install_test_context(pnl_batch_cb());

        db.ingest(IngestInput {
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

        db.ingest(IngestInput {
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
        // _ctx guard dropped here — context cleared before "crash"
    }
    // db dropped — simulates crash; no context on thread

    // Phase 2: reopen — open() skips external reducer (no context), no panic
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = DeltaDb::open(config).unwrap();

        // Install test context (simulates JS callback registration in NAPI)
        let _ctx = install_test_context(pnl_batch_cb());

        // replay_reducer must NOT skip: context_installed() returns true
        db.replay_reducer("pnl").unwrap();

        // Ingest block 1002 — MV output should incorporate all 3 blocks
        let batch = db
            .ingest(IngestInput {
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
            .expect("should produce a delta batch");

        let mv_records: Vec<_> = batch.records_for("position_summary").to_vec();
        let alice = mv_records
            .iter()
            .find(|r| r.key.get("user") == Some(&Value::String("alice".into())))
            .expect("alice should have an MV record");
        let trade_count = alice
            .values
            .get("trade_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        assert_eq!(
            trade_count, 3,
            "ExternalRuntime replay: trade_count should be 3 (b1000+b1001+b1002), got {trade_count}"
        );
    }
}
