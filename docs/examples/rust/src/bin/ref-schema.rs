// Runs the schema of the schema reference page in Rust.
use std::collections::HashMap;

use settle::db::{Config, IngestInput, Settle};
use settle::types::{BlockCursor, RowMap, Value};

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

fn main() -> settle::error::Result<()> {
    // [!region schema]
    let schema = r#"
CREATE VIRTUAL TABLE trades (
    block_number UInt64,
    pool         String,
    price        Float64,
    amount       Float64
);

CREATE MODULE fees LANGUAGE lua AS $$
    return { rate = 0.003 }
$$;

CREATE REDUCER pool_fees
SOURCE trades
GROUP BY pool
STATE (
    total Float64 DEFAULT 0,
    seen  Map(String, Float64)
)
REQUIRE fees
LANGUAGE lua
PROCESS $$
    if row.amount <= 0 then return end

    local fee = row.amount * row.price * fees.rate
    state.total = state.total + fee
    state.seen:set(tostring(row.price), fee)

    emit({ fee = fee, total = state.total })
$$;

CREATE MATERIALIZED VIEW pool_summary AS
SELECT
    pool,
    sum(fee),
    count(),
    max(total) AS max_total
FROM pool_fees
GROUP BY pool;
"#;
    // [!endregion schema]

    let mut db = Settle::open(Config::new(schema))?;
    let trade = |block: u64, price: f64, amount: f64| {
        RowMap::from([
            ("block_number".into(), Value::UInt64(block)),
            ("pool".into(), Value::String("ETH/USDC".into())),
            ("price".into(), Value::Float64(price)),
            ("amount".into(), Value::Float64(amount)),
        ])
    };
    let batch = db
        .ingest(IngestInput {
            data: HashMap::from([(
                "trades".to_string(),
                vec![trade(1, 2000.0, 1.0), trade(1, 2000.0, 0.0), trade(2, 1000.0, 2.0)],
            )]),
            rollback_chain: vec![cursor(1, "0x1a"), cursor(2, "0x2a")],
            finalized_head: cursor(0, "0x00"),
        })?
        .expect("a batch");

    // [!region output]
    for record in &batch.tables["pool_summary"] {
        let values: Vec<String> = record.values().map(|(k, v)| format!("{k}: {v}")).collect();
        println!("{:?} {{ {} }}", record.operation, values.join(", "));
    }
    // [!endregion output]
    Ok(())
}
