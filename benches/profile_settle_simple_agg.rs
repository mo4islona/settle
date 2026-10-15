//! Standalone Settle-only run of the simple_agg pipeline for profiling.
//! No Postgres, no Docker. Run with:
//!   samply record cargo bench --bench profile_settle_simple_agg

use std::collections::HashMap;

use settle::db::{Config, Settle};
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::test_helpers::ingest_blocks;
use settle::types::{RowMap, Value};

#[path = "common/mod.rs"]
mod common;

use common::{gen_transfer, split_blocks, BATCH_SIZE, BLOCKS_PER_BATCH};

const SETTLE_SCHEMA: &str = r#"
CREATE TABLE transfers (
    block_number UInt64,
    from_addr    String,
    to_addr      String,
    value        Float64
);

CREATE REDUCER user_balance
SOURCE transfers
GROUP BY from_addr
STATE (
    balance Float64 DEFAULT 0
)
LANGUAGE EXTERNAL;

CREATE MATERIALIZED VIEW balances AS
SELECT
    from_addr,
    last(balance) AS balance
FROM user_balance
GROUP BY from_addr;
"#;

const TOTAL_ROWS: usize = 500_000;
const NUM_USERS: usize = 10_000;

fn balance_reducer() -> FnReducerRuntime {
    FnReducerRuntime::new(|state, row| {
        let from = row.get("from_addr").and_then(|v| v.as_str()).unwrap_or("");
        let delta = row.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let current = state.get("balance").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let next = current + delta;
        state.insert("balance".into(), Value::Float64(next));

        let mut emit = HashMap::new();
        emit.insert("from_addr".into(), Value::String(from.to_string()));
        emit.insert("balance".into(), Value::Float64(next));
        vec![emit]
    })
}

fn open_settle_db() -> anyhow::Result<(Settle, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let cfg = Config::with_data_dir(SETTLE_SCHEMA, dir.path().to_str().unwrap());
    let mut db = Settle::open(cfg)?;
    db.register_reducer_callback("user_balance", Box::new(balance_reducer()))?;
    Ok((db, dir))
}

fn main() -> anyhow::Result<()> {
    let rows: Vec<RowMap> = (0..TOTAL_ROWS)
        .map(|i| gen_transfer(i, NUM_USERS))
        .collect();

    let (mut db, _dir) = open_settle_db()?;

    let t = std::time::Instant::now();
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        let items: Vec<(String, u64, Vec<RowMap>)> = split_blocks(chunk, block_no)
            .into_iter()
            .map(|(b, c)| ("transfers".to_string(), b, c.to_vec()))
            .collect();
        ingest_blocks(&mut db, items)?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    let elapsed = t.elapsed();
    eprintln!(
        "settle simple_agg: {TOTAL_ROWS} rows in {:.3}s = {:.0} rows/s",
        elapsed.as_secs_f64(),
        TOTAL_ROWS as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}
