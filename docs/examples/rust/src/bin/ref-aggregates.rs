// Checks the aggregate functions in Rust, with the schema of the TS example.
use std::collections::HashMap;

use settle::db::{Config, IngestInput, Settle};
use settle::types::{BlockCursor, RowMap, Value};

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

fn main() -> settle::error::Result<()> {
    // [!region schema]
    let mut db = Settle::open(Config::new(
        "
        CREATE VIRTUAL TABLE transfers (
            block_number UInt64,
            token        String,
            amount       UInt64,
            price        Float64,
            memo         String
        );

        CREATE MATERIALIZED VIEW token_stats AS
        SELECT
            token,
            count()       AS rows,
            count(memo)   AS memos,
            sum(amount)   AS total,
            avg(amount)   AS mean,
            min(amount)   AS smallest,
            max(price)    AS top_price,
            first(memo)   AS first_memo,
            last(memo)    AS last_memo
        FROM transfers
        GROUP BY token;
        ",
    ))?;
    // [!endregion schema]

    let transfer = |block: u64, amount: u64, price: f64, memo: Option<&str>| {
        RowMap::from([
            ("block_number".into(), Value::UInt64(block)),
            ("token".into(), Value::String("USDC".into())),
            ("amount".into(), Value::UInt64(amount)),
            ("price".into(), Value::Float64(price)),
            ("memo".into(), memo.map_or(Value::Null, |m| Value::String(m.into()))),
        ])
    };

    let batch = db
        .ingest(IngestInput {
            data: HashMap::from([(
                "transfers".to_string(),
                vec![
                    transfer(1, 10, 1.0, None),
                    transfer(1, 25, 1.01, Some("rent")),
                    transfer(2, 5, 0.99, Some("tip")),
                    transfer(2, 7, 1.0, None),
                ],
            )]),
            rollback_chain: vec![cursor(1, "0x1a"), cursor(2, "0x2a")],
            finalized_head: cursor(0, "0x00"),
        })?
        .expect("a batch");

    // [!region output]
    let record = &batch.tables["token_stats"][0];
    for (name, value) in record.values() {
        println!("{name:<10} {value:?}");
    }
    // [!endregion output]

    println!("--- a Float64 emit into an integer state name");
    for (label, value) in [("Float64", Value::Float64(1.0)), ("UInt64", Value::UInt64(1))] {
        let mut db = Settle::open(Config::new(
            "CREATE TABLE t (block_number UInt64, k String);
             CREATE REDUCER r SOURCE t GROUP BY k STATE (n UInt64 DEFAULT 0) LANGUAGE EXTERNAL;
             CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS s FROM r GROUP BY k;",
        ))?;
        db.register_reducer_callback(
            "r",
            Box::new(settle::reducer_runtime::fn_reducer::FnReducerRuntime::new(move |state, _row| {
                state.emit(RowMap::from([("n".into(), value.clone())]));
            })),
        )?;
        let batch = db.ingest(IngestInput {
            data: HashMap::from([(
                "t".to_string(),
                vec![RowMap::from([
                    ("block_number".into(), Value::UInt64(1)),
                    ("k".into(), Value::String("a".into())),
                ])],
            )]),
            rollback_chain: vec![cursor(1, "0x1a")],
            finalized_head: cursor(0, "0x00"),
        })?;
        let sum = batch.as_ref().and_then(|b| b.tables.get("v")).map(|v| v[0].get("s").cloned());
        println!("{label}: {sum:?}");
    }
    Ok(())
}
