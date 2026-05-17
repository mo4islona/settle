//! Shared helpers for e2e tests against a real RocksDB backend (and, for
//! Postgres-target tests, the fixture in `pg`).
//!
//! The schema is integer-only (deterministic across crash boundaries), keeps
//! a reducer + MV so Settle's value-proposition is exercised, and is small
//! enough for assertions to be readable.

#![allow(dead_code)]


use std::collections::HashMap;
use std::path::Path;

use settle::db::{Config, Settle};
use settle::test_helpers::ingest_one;
use settle::types::{BlockNumber, ChangeBatch, ChangeRecord, RowMap, Value};

pub const SCHEMA: &str = r#"
CREATE VIRTUAL TABLE orders (
    block_number UInt64,
    asset_id     String,
    amount       UInt64
);

CREATE REDUCER market_stats
SOURCE orders
GROUP BY asset_id
STATE (
    volume UInt64 DEFAULT 0,
    trades UInt64 DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    state.volume = state.volume + row.amount
    state.trades = state.trades + 1
    emit({ asset_id = row.asset_id, vol = row.amount })
$$;

CREATE MATERIALIZED VIEW token_summary AS
SELECT
    asset_id,
    sum(vol) AS total_volume,
    count()  AS trade_count
FROM market_stats
GROUP BY asset_id;
"#;

pub fn open_rocks(dir: &Path) -> Settle {
    let cfg = Config::with_data_dir(SCHEMA, dir.to_str().expect("utf-8 path"));
    Settle::open(cfg).expect("open settle with rocksdb backend")
}

pub fn order(asset: &str, amount: u64) -> RowMap {
    HashMap::from([
        ("asset_id".to_string(), Value::String(asset.to_string())),
        ("amount".to_string(), Value::UInt64(amount)),
    ])
}

pub fn ingest_orders(db: &mut Settle, block: BlockNumber, rows: Vec<RowMap>) -> ChangeBatch {
    ingest_one(db, "orders", block, rows)
        .expect("ingest must not error")
        .expect("ingest must produce a batch for non-empty input")
}

pub fn mv_record_for<'a>(batch: &'a ChangeBatch, asset: &str) -> Option<&'a ChangeRecord> {
    batch
        .records_for("token_summary")
        .iter()
        .find(|r| r.key.get("asset_id") == Some(&Value::String(asset.to_string())))
}

pub fn total_volume(r: &ChangeRecord) -> u64 {
    numeric(r.values.get("total_volume"), "total_volume")
}

pub fn trade_count(r: &ChangeRecord) -> u64 {
    numeric(r.values.get("trade_count"), "trade_count")
}

pub fn prev_total_volume(r: &ChangeRecord) -> u64 {
    let prev = r
        .prev_values
        .as_ref()
        .expect("record has no prev_values");
    numeric(prev.get("total_volume"), "prev.total_volume")
}

pub fn prev_trade_count(r: &ChangeRecord) -> u64 {
    let prev = r
        .prev_values
        .as_ref()
        .expect("record has no prev_values");
    numeric(prev.get("trade_count"), "prev.trade_count")
}

fn numeric(v: Option<&Value>, label: &str) -> u64 {
    match v {
        Some(Value::UInt64(n)) => *n,
        Some(Value::Int64(n)) => *n as u64,
        Some(Value::Float64(f)) => *f as u64,
        Some(other) => panic!("{label} has unexpected type: {other:?}"),
        None => panic!("missing {label}"),
    }
}
