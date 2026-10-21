// Checks the Rust Pipeline builder: the generated schema, the state types of
// initial_state, update(), Map fields, and the options that BuildOptions lacks.
use std::collections::HashMap;

use settle::db::{Config, IngestInput, Settle};
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::types::{BlockCursor, RowMap, Value};
use settle::{
    BuildOptions, Pipeline, ReducerOptions, SlidingWindowOptions, ViewOptions, datetime, float64,
    interval, string, uint64,
};

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

fn main() -> settle::error::Result<()> {
    // [!region compile]
    let mut p = Pipeline::new();

    let trades = p.table(
        "trades",
        [
            ("block_number", uint64()),
            ("timestamp", datetime()),
            ("trader", string()),
            ("amount", float64()),
        ],
    );

    let stats = trades.create_reducer(
        "stats",
        ReducerOptions {
            group_by: vec!["trader".into()],
            initial_state: RowMap::from([
                ("volume".into(), Value::Float64(0.0)),
                ("trades".into(), Value::UInt64(0)),
                ("last_side".into(), Value::String("none".into())),
                ("active".into(), Value::Boolean(false)),
            ]),
            reduce: Box::new(|state, row| {
                let amount = row.get("amount").and_then(Value::as_f64).unwrap_or(0.0);
                let volume = state.get_f64("volume") + amount;

                state.set("volume", Value::Float64(volume));
                state.emit(RowMap::from([
                    ("volume".into(), Value::Float64(volume)),
                    ("timestamp".into(), row.get("timestamp").cloned().unwrap_or(Value::Null)),
                ]));
            }),
        },
    );

    stats.create_view(
        "trader_volume",
        ViewOptions {
            group_by: vec!["trader".into(), interval("timestamp", "1 hour").r#as("hour").into()],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("trader".into(), agg.key("trader").into()),
                    ("hour".into(), agg.key("hour").into()),
                    ("volume".into(), agg.last("volume").into()),
                ]
            }),
        },
    );

    trades.create_view(
        "volume_24h",
        ViewOptions {
            group_by: vec!["trader".into()],
            sliding_window: Some(SlidingWindowOptions {
                interval: "24 hours".into(),
                time_column: "timestamp".into(),
            }),
            select: Box::new(|agg| {
                vec![
                    ("trader".into(), agg.key("trader").into()),
                    ("volume".into(), agg.sum("amount").into()),
                ]
            }),
        },
    );

    println!("{}", p.to_ddl()?.replace(';', ";\n"));
    // [!endregion compile]

    println!("--- update() sets the absent fields to Null");
    let mut q = Pipeline::new();
    q.table("t", [("block_number", uint64()), ("k", string()), ("n", float64())])
        .create_reducer(
            "r",
            ReducerOptions {
                group_by: vec!["k".into()],
                initial_state: RowMap::from([
                    ("a".into(), Value::Float64(0.0)),
                    ("b".into(), Value::Float64(0.0)),
                ]),
                reduce: Box::new(|state, row| {
                    let n = row.get("n").and_then(Value::as_f64).unwrap_or(0.0);
                    println!("  row {n} sees a = {:?}, b = {:?}", state.get("a"), state.get("b"));
                    if n == 1.0 {
                        state.update(RowMap::from([
                            ("a".into(), Value::Float64(1.0)),
                            ("b".into(), Value::Float64(1.0)),
                        ]));
                    } else {
                        state.update(RowMap::from([("a".into(), Value::Float64(2.0))]));
                    }
                }),
            },
        );
    let mut db = q.build(BuildOptions::new())?;
    for n in 1..=3u64 {
        let batch = db.ingest(IngestInput {
            data: HashMap::from([(
                "t".to_string(),
                vec![RowMap::from([
                    ("block_number".into(), Value::UInt64(n)),
                    ("k".into(), Value::String("x".into())),
                    ("n".into(), Value::Float64(n as f64)),
                ])],
            )]),
            rollback_chain: vec![cursor(n, &format!("0x{n}a"))],
            finalized_head: cursor(0, "0x00"),
        })?;
        if let Some(batch) = batch {
            db.ack(batch.sequence)?;
        }
    }

    println!("--- more options than BuildOptions has");
    // [!region options]
    let mut p = Pipeline::new();
    p.table("swaps", [("block_number", uint64()), ("pool", string()), ("amount", float64())])
        .create_reducer(
            "totals",
            ReducerOptions {
                group_by: vec!["pool".into()],
                initial_state: RowMap::from([("volume".into(), Value::Float64(0.0))]),
                reduce: Box::new(|_, _| {}), // not used: the closure below runs
            },
        );

    let config = Config::with_data_dir(p.to_ddl()?, "./settle-data")
        .max_pending_batches(1)
        .applied_sequence(0);
    let mut db = Settle::open(config)?;

    db.register_reducer_callback(
        "totals",
        Box::new(FnReducerRuntime::new(|state, row| {
            let amount = row.get("amount").and_then(Value::as_f64).unwrap_or(0.0);
            state.set("volume", Value::Float64(state.get_f64("volume") + amount));
        })),
    )?;
    // [!endregion options]
    println!("opened, unacked: {}", db.unacked_batches());
    drop(db);
    let _ = std::fs::remove_dir_all("./settle-data");

    println!("--- a Map field in a closure");
    // [!region map]
    let schema = "
        CREATE TABLE moves (block_number UInt64, user String, token String, amount Float64);
        CREATE REDUCER books SOURCE moves GROUP BY user
        STATE (book Map(String, Float64))
        LANGUAGE EXTERNAL;
        CREATE MATERIALIZED VIEW holdings AS
        SELECT user, token, last(qty) AS qty FROM books GROUP BY user, token;
    ";
    let mut db = Settle::open(Config::new(schema))?;

    db.register_reducer_callback(
        "books",
        Box::new(FnReducerRuntime::new(|state, row| {
            let token = row.get("token").and_then(Value::as_str).unwrap_or("").to_string();
            let amount = row.get("amount").and_then(Value::as_f64).unwrap_or(0.0);

            let mut book = state.map("book").expect("a Map field");
            let qty = book.get(&token).and_then(Value::as_f64).unwrap_or(0.0) + amount;
            if qty == 0.0 {
                book.remove(&token);
            } else {
                book.set(&token, Value::Float64(qty));
            }

            state.emit(RowMap::from([
                ("token".into(), Value::String(token)),
                ("qty".into(), Value::Float64(qty)),
            ]));
        })),
    )?;
    // [!endregion map]

    let mv = |block: u64, amount: f64| {
        RowMap::from([
            ("block_number".into(), Value::UInt64(block)),
            ("user".into(), Value::String("a".into())),
            ("token".into(), Value::String("X".into())),
            ("amount".into(), Value::Float64(amount)),
        ])
    };
    let batch = db
        .ingest(IngestInput {
            data: HashMap::from([("moves".to_string(), vec![mv(1, 5.0), mv(1, 2.0), mv(2, -7.0), mv(2, 1.0)])]),
            rollback_chain: vec![cursor(1, "0x1a"), cursor(2, "0x2a")],
            finalized_head: cursor(0, "0x00"),
        })?
        .expect("a batch");
    for record in &batch.tables["holdings"] {
        println!("{:?} qty={}", record.operation, record.get("qty").unwrap());
    }

    println!("--- interval errors");
    let mut bad = Pipeline::new();
    bad.table("t", [("block_number", uint64()), ("ts", datetime()), ("n", float64())])
        .create_view(
            "v",
            ViewOptions {
                group_by: vec![interval("ts", "1 week").into()],
                sliding_window: None,
                select: Box::new(|agg| vec![("n".into(), agg.sum("n").into())]),
            },
        );
    match bad.to_ddl() {
        Err(e) => println!("{e}"),
        Ok(ddl) => println!("ok: {ddl}"),
    }
    Ok(())
}
