//! Tests that run the real external runtime through the test bridge of
//! `reducer_runtime::external`. The bridge exists only in unit tests, so
//! these tests cannot move to the black-box suite in `tests/conformance`.

use super::*;
use crate::test_helpers::{ingest_input, ingest_with_finalized};
use crate::types::{BlockCursor, Value};
use std::collections::HashMap;

const EXTERNAL_PNL_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user String,
        side String,
        amount Float64,
        price Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
    LANGUAGE EXTERNAL;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl) AS total_pnl,
        last(position_size) AS current_position,
        count() AS trade_count
    FROM pnl
    GROUP BY user;
"#;

fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> RowMap {
    HashMap::from([
        ("user".to_string(), Value::String(user.to_string())),
        ("side".to_string(), Value::String(side.to_string())),
        ("amount".to_string(), Value::Float64(amount)),
        ("price".to_string(), Value::Float64(price)),
    ])
}

/// Verify that replay_reducer() correctly drives the *real* ExternalRuntime
/// (not a FnReducer substitute) by installing a test context.
///
/// This is the true NAPI code path: ExternalRuntime stays as the runtime
/// throughout; `callback_installed()` gates replay on the thread-local.
#[test]
fn external_runtime_replay_uses_real_external_runtime() {
    use crate::reducer_runtime::external::{HostGroup, install_test_context};
    use crate::storage::memory::MemoryBackend;
    use std::sync::Arc;

    // Rust callback that mirrors pnl_fn_runtime() but operates on GroupBatch.
    let pnl_batch_cb = || {
        |groups: &mut [HostGroup]| {
            for group in groups.iter_mut() {
                for (index, row) in group.rows.iter().enumerate() {
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
                    group.emits.push((index, emit));
                }
            }
        }
    };

    let storage: Arc<dyn crate::storage::StorageBackend> = Arc::new(MemoryBackend::new());

    // Phase 1: ingest with real ExternalRuntime (test context, no set_reducer_runtime)
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = Settle::open(config).unwrap();
        // DO NOT call set_reducer_runtime — ExternalRuntime stays as the runtime

        let _ctx = install_test_context(pnl_batch_cb());

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
        // _ctx guard dropped here — context cleared before "crash"
    }
    // db dropped — simulates crash; no context on thread

    // Phase 2: reopen — open() skips external reducer (no context), no panic
    {
        let mut config = Config::new(EXTERNAL_PNL_SCHEMA);
        config.storage = Some(storage.clone());
        let mut db = Settle::open(config).unwrap();

        // Install test context (simulates JS callback registration in NAPI)
        let _ctx = install_test_context(pnl_batch_cb());

        // replay_reducer must NOT skip: callback_installed() returns true
        db.replay_reducer("pnl").unwrap();

        // Ingest block 1002 — MV output should incorporate all 3 blocks
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
            .expect("should produce a change batch");

        let mv_records: Vec<_> = batch.records_for("position_summary").to_vec();
        let alice = mv_records
            .iter()
            .find(|r| r.key("user") == Some(&Value::String("alice".into())))
            .expect("alice should have an MV record");
        let trade_count = alice
            .get("trade_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        assert_eq!(
            trade_count, 3,
            "ExternalRuntime replay: trade_count should be 3 (b1000+b1001+b1002), got {trade_count}"
        );
    }
}

/// raw → `upstream` → external `counter` → view. The external reducer counts
/// the rows of each pool; `upstream` passes the amount through.
fn chained_schema(upstream_body: &str) -> String {
    format!(
        r#"
        CREATE TABLE swaps (block_number UInt64, pool String, amount UInt64);

        CREATE REDUCER upstream
        SOURCE swaps
        GROUP BY pool
        STATE (n UInt64 DEFAULT 0)
        {upstream_body}

        CREATE REDUCER counter
        SOURCE upstream
        GROUP BY pool
        STATE (n UInt64 DEFAULT 0)
        LANGUAGE EXTERNAL;

        CREATE MATERIALIZED VIEW pool_counts AS
        SELECT pool, count() AS events, last(seen) AS seen
        FROM counter
        GROUP BY pool;
        "#
    )
}

/// The callback of every external reducer: count the rows of the group,
/// and emit the count with the amount of the row.
fn count_rows(groups: &mut [crate::reducer_runtime::external::HostGroup]) {
    for group in groups.iter_mut() {
        for (index, row) in group.rows.iter().enumerate() {
            let n = group.state.get("n").and_then(Value::as_u64).unwrap_or(0) + 1;
            group.state.insert("n".into(), Value::UInt64(n));

            let mut emit = HashMap::from([("seen".to_string(), Value::UInt64(n))]);
            if let Some(amount) = row.get("amount") {
                emit.insert("amount".into(), amount.clone());
            }
            group.emits.push((index, emit));
        }
    }
}

/// After a reopen, the callbacks replay the unfinalized blocks. The replay
/// of `counter` must also run `upstream`, which produces its input, or the
/// count restarts from the finalized state.
#[test]
fn chained_reducers_survive_reopen() {
    use crate::reducer_runtime::external::install_test_context;
    use crate::storage::memory::MemoryBackend;

    let lua = "LANGUAGE lua PROCESS $$ if row.amount > 0 then emit { amount = row.amount } end $$;";
    let external = "LANGUAGE EXTERNAL;";
    let cases = [
        (lua, &["counter"][..]),
        (external, &["upstream", "counter"]),
        (external, &["counter", "upstream"]),
    ];

    for (upstream_body, callbacks) in cases {
        let schema = chained_schema(upstream_body);
        let storage = Arc::new(MemoryBackend::new());
        let block = |b: BlockNumber| {
            let swap = HashMap::from([
                ("pool".to_string(), Value::String("A".into())),
                ("amount".to_string(), Value::UInt64(b)),
            ]);
            vec![("swaps".to_string(), b, vec![swap])]
        };

        {
            let mut db = Settle::open(Config::new(schema.as_str()).storage(storage.clone())).unwrap();
            let _ctx = install_test_context(count_rows);
            ingest_with_finalized(&mut db, block(1), 0).unwrap();
        }

        let mut db = Settle::open(Config::new(schema.as_str()).storage(storage.clone())).unwrap();
        let _ctx = install_test_context(count_rows);
        for name in callbacks {
            db.replay_reducer(name).unwrap();
        }
        let batch = ingest_with_finalized(&mut db, block(2), 0).unwrap().unwrap();

        let pool = &batch.records_for("pool_counts")[0];
        let case = format!("{upstream_body} {callbacks:?}");
        assert_eq!(pool.get("events"), Some(&Value::UInt64(2)), "{case}");
        assert_eq!(pool.get("seen"), Some(&Value::UInt64(2)), "{case}");
    }
}
