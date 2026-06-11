//! Phase-by-phase microbench for the simple_agg workload.
//!
//! Drills into the MV-only and FnReducer pipelines to attribute wall time
//! to specific phases:
//!  - raw ingest (storage write)
//!  - reducer process (state lookup + emit allocations)
//!  - MV group key extraction
//!  - MV agg feed (BTreeMap entry + NumAccum::add)
//!  - MV change emission (prev/current diff + ChangeRecord build)
//!
//! Plus a synthetic "pure ideal" baseline that does the same arithmetic with
//! plain Vec<f64> + FxHashMap<&str, f64> — the absolute floor for this hardware
//! and workload before any engine abstractions kick in.
//!
//! Run: cargo bench --bench profile_phases

use std::collections::HashMap;
use std::time::Instant;

use rustc_hash::FxHashMap;
use settle::db::{Config, Settle};
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::test_helpers::ingest_blocks;
use settle::types::{RowMap, Value};

#[path = "common/mod.rs"]
mod common;

use common::{gen_transfer, split_blocks, BATCH_SIZE, BLOCKS_PER_BATCH, ROWS_PER_BLOCK};

const TOTAL_ROWS: usize = 200_000;
const NUM_USERS: usize = 10_000;

// ─── Schemas ─────────────────────────────────────────────────────────────────

const SCHEMA_MV_ONLY: &str = r#"
CREATE TABLE transfers (
    block_number UInt64,
    from_addr    String,
    to_addr      String,
    value        Float64
);

CREATE MATERIALIZED VIEW balances AS
SELECT from_addr, sum(value) AS balance
FROM transfers GROUP BY from_addr;
"#;

const SCHEMA_FN_REDUCER: &str = r#"
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
SELECT from_addr, last(balance) AS balance
FROM user_balance GROUP BY from_addr;
"#;

const SCHEMA_RAW_ONLY: &str = r#"
CREATE TABLE transfers (
    block_number UInt64,
    from_addr    String,
    to_addr      String,
    value        Float64
);
"#;

// COUNT(*) has no source_col and no Value coercion — isolates the
// group-key / BTreeMap / change-emit cost from the value-extraction cost.
const SCHEMA_MV_COUNT: &str = r#"
CREATE TABLE transfers (
    block_number UInt64,
    from_addr    String,
    to_addr      String,
    value        Float64
);

CREATE MATERIALIZED VIEW balances AS
SELECT from_addr, count() AS n
FROM transfers GROUP BY from_addr;
"#;

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

// ─── Settle pipelines ────────────────────────────────────────────────────────

fn run_settle_pipeline(schema: &str, rows: &[RowMap], reducer_cb: Option<FnReducerRuntime>) -> std::time::Duration {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config::with_data_dir(schema, dir.path().to_str().unwrap());
    let mut db = Settle::open(cfg).unwrap();
    if let Some(rt) = reducer_cb {
        db.register_reducer_callback("user_balance", Box::new(rt)).unwrap();
    }

    let t = Instant::now();
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        let items: Vec<(String, u64, Vec<RowMap>)> = split_blocks(chunk, block_no)
            .into_iter()
            .map(|(b, c)| ("transfers".to_string(), b, c.to_vec()))
            .collect();
        ingest_blocks(&mut db, items).unwrap();
        block_no += BLOCKS_PER_BATCH as u64;
    }
    t.elapsed()
}

fn run_settle_memory(schema: &str, rows: &[RowMap], reducer_cb: Option<FnReducerRuntime>) -> std::time::Duration {
    let cfg = Config::new(schema);
    let mut db = Settle::open(cfg).unwrap();
    if let Some(rt) = reducer_cb {
        db.register_reducer_callback("user_balance", Box::new(rt)).unwrap();
    }

    let t = Instant::now();
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        let items: Vec<(String, u64, Vec<RowMap>)> = split_blocks(chunk, block_no)
            .into_iter()
            .map(|(b, c)| ("transfers".to_string(), b, c.to_vec()))
            .collect();
        ingest_blocks(&mut db, items).unwrap();
        block_no += BLOCKS_PER_BATCH as u64;
    }
    t.elapsed()
}

// ─── Synthetic floor: the same workload by hand ─────────────────────────────

/// What a hand-written, allocation-tight Rust loop would cost — the floor.
/// FxHashMap<String, f64>, hash once per row, add. No Value, no RowMap, no MV.
fn run_synthetic_floor(rows: &[RowMap]) -> std::time::Duration {
    let mut acc: FxHashMap<String, f64> = FxHashMap::default();
    let t = Instant::now();
    for row in rows {
        let from = row.get("from_addr").and_then(|v| v.as_str()).unwrap_or("");
        let v = row.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
        *acc.entry(from.to_string()).or_insert(0.0) += v;
    }
    let elapsed = t.elapsed();
    std::hint::black_box(&acc);
    elapsed
}

/// Same as floor but with pre-extracted columns so the per-row HashMap lookup
/// is gone too — what a "columnar batch" Settle would pay.
fn run_synthetic_columnar(rows: &[RowMap]) -> std::time::Duration {
    // Pre-extract (the cost of doing this once per batch is amortized; we count
    // only the inner aggregation loop, since this is the workload Settle could
    // do if MV had direct column access via ColumnId).
    let from_col: Vec<&str> = rows
        .iter()
        .map(|r| r.get("from_addr").and_then(|v| v.as_str()).unwrap_or(""))
        .collect();
    let val_col: Vec<f64> = rows
        .iter()
        .map(|r| r.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0))
        .collect();

    let mut acc: FxHashMap<&str, f64> = FxHashMap::default();
    let t = Instant::now();
    for i in 0..rows.len() {
        *acc.entry(from_col[i]).or_insert(0.0) += val_col[i];
    }
    let elapsed = t.elapsed();
    std::hint::black_box(&acc);
    elapsed
}

/// Even tighter: pre-aggregate per BATCH first (10K unique users, 5000 rows per
/// batch means ~5 dup rows per user per batch → BTreeMap touches reduce 5×).
/// This is what PG's `unnest+GROUP BY ON CONFLICT` does internally.
fn run_synthetic_preagg(rows: &[RowMap]) -> std::time::Duration {
    let mut global: FxHashMap<String, f64> = FxHashMap::default();
    let t = Instant::now();
    for chunk in rows.chunks(BATCH_SIZE) {
        // per-batch pre-aggregation (the PG batch trick)
        let mut batch_acc: FxHashMap<&str, f64> = FxHashMap::default();
        for row in chunk {
            let from = row.get("from_addr").and_then(|v| v.as_str()).unwrap_or("");
            let v = row.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
            *batch_acc.entry(from).or_insert(0.0) += v;
        }
        for (k, v) in batch_acc {
            *global.entry(k.to_string()).or_insert(0.0) += v;
        }
    }
    let elapsed = t.elapsed();
    std::hint::black_box(&global);
    elapsed
}

// ─── Output formatting ───────────────────────────────────────────────────────

fn print(label: &str, total_rows: usize, dur: std::time::Duration) {
    let secs = dur.as_secs_f64();
    let rps = total_rows as f64 / secs;
    let us_per_row = secs * 1e6 / total_rows as f64;
    eprintln!(
        "  {label:<38}  {secs:>7.3}s  {rps:>10.0} rows/s  {us_per_row:>6.2} us/row",
    );
}

fn main() -> anyhow::Result<()> {
    let rows: Vec<RowMap> = (0..TOTAL_ROWS).map(|i| gen_transfer(i, NUM_USERS)).collect();

    eprintln!(
        "workload: simple_agg-style, {TOTAL_ROWS} rows, {NUM_USERS} users, \
         {ROWS_PER_BLOCK} rows/block, {BLOCKS_PER_BATCH} blocks/batch"
    );
    eprintln!();
    eprintln!("─── absolute floor (no Settle) ───");
    print("synthetic, FxHashMap<String,f64>", TOTAL_ROWS, run_synthetic_floor(&rows));
    print("synthetic, columnar (&str keys)", TOTAL_ROWS, run_synthetic_columnar(&rows));
    print("synthetic, batch-preagg+merge", TOTAL_ROWS, run_synthetic_preagg(&rows));

    eprintln!();
    eprintln!("─── Settle pipelines [Memory backend] ───");
    print("raw_only (storage write only)", TOTAL_ROWS, run_settle_memory(SCHEMA_RAW_ONLY, &rows, None));
    print("mv_only count(*) (no value extract)", TOTAL_ROWS, run_settle_memory(SCHEMA_MV_COUNT, &rows, None));
    print("mv_only sum(value)", TOTAL_ROWS, run_settle_memory(SCHEMA_MV_ONLY, &rows, None));
    print("fn_reducer + last MV", TOTAL_ROWS, run_settle_memory(SCHEMA_FN_REDUCER, &rows, Some(balance_reducer())));

    eprintln!();
    eprintln!("─── Settle pipelines [RocksDB backend] ───");
    print("raw_only (storage write only)", TOTAL_ROWS, run_settle_pipeline(SCHEMA_RAW_ONLY, &rows, None));
    print("mv_only (raw + MV sum)", TOTAL_ROWS, run_settle_pipeline(SCHEMA_MV_ONLY, &rows, None));
    print("fn_reducer + last MV", TOTAL_ROWS, run_settle_pipeline(SCHEMA_FN_REDUCER, &rows, Some(balance_reducer())));

    Ok(())
}
