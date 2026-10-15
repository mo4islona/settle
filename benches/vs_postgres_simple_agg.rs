//! Workload 2: Simple aggregation (per-user balance).
//!   - **postgres_only**: PG does it all — INSERT raw + pre-aggregate batch
//!     in Rust then multi-row UPSERT into `balances` (incremental update).
//!   - **settle→postgres**: Settle FnReducer maintains balance per user,
//!     MV emits absolute values; PG receives INSERT raw + UPSERT balances
//!     (replace, not increment) from ChangeBatch records.
//!
//! Both pipelines leave PG with the same final state.
//!
//! Run with: `cargo bench --bench vs_postgres_simple_agg` (Docker required)

use std::collections::HashMap;
use std::time::Instant;

use settle::db::{Config, Settle};
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::test_helpers::{ingest_blocks, ingest_with_finalized};
use settle::types::{ChangeBatch, ChangeRecord, RowMap, Value};
use tokio_postgres::types::ToSql;

#[path = "common/mod.rs"]
mod common;

use common::{
    build_multi_insert, gen_transfer, print_result_split, row_f64, row_str,
    split_blocks, PgRuntime, BATCH_SIZE, BLOCKS_PER_BATCH, ROWS_PER_BLOCK,
};

const SETTLE_SCHEMA_FN: &str = r#"
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

const SETTLE_SCHEMA_MV: &str = r#"
CREATE TABLE transfers (
    block_number UInt64,
    from_addr    String,
    to_addr      String,
    value        Float64
);

CREATE MATERIALIZED VIEW balances AS
SELECT
    from_addr,
    sum(value) AS balance
FROM transfers
GROUP BY from_addr;
"#;

const SETTLE_SCHEMA_ER: &str = r#"
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
    WHEN 1 = 1 THEN
        SET state.balance = state.balance + row.value
        EMIT _dummy = 0
    ALWAYS EMIT
        row.from_addr AS from_addr,
        state.balance AS balance
END;

CREATE MATERIALIZED VIEW balances AS
SELECT
    from_addr,
    last(balance) AS balance
FROM user_balance
GROUP BY from_addr;
"#;

const PG_SCHEMA: &str = "
CREATE TABLE transfers (
    block_number BIGINT NOT NULL,
    from_addr    TEXT   NOT NULL,
    to_addr      TEXT   NOT NULL,
    value        DOUBLE PRECISION NOT NULL
);

CREATE TABLE balances (
    from_addr TEXT PRIMARY KEY,
    balance   DOUBLE PRECISION NOT NULL
);
";

const TOTAL_ROWS: usize = 100_000;
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

#[derive(Clone, Copy)]
enum Storage {
    Memory,
    Rocks,
}

impl Storage {
    fn label(self) -> &'static str {
        match self {
            Storage::Memory => "mem",
            Storage::Rocks => "rocks",
        }
    }
}

/// Build a config. `checkpoint_interval > 1` enables backfill persist-deferral
/// (only effective on no-lag ingest with a deferral-safe pipeline — no external
/// reducer, no sliding-window MV).
fn make_cfg(
    schema: &str,
    storage: Storage,
    checkpoint_interval: u64,
) -> (Config, Option<tempfile::TempDir>) {
    match storage {
        Storage::Memory => (
            Config::new(schema).backfill_checkpoint_interval(checkpoint_interval),
            None,
        ),
        Storage::Rocks => {
            let dir = tempfile::tempdir().unwrap();
            let cfg = Config::with_data_dir(schema, dir.path().to_str().unwrap())
                .backfill_checkpoint_interval(checkpoint_interval);
            (cfg, Some(dir))
        }
    }
}

fn open_settle_db_fn(
    storage: Storage,
    checkpoint_interval: u64,
) -> anyhow::Result<(Settle, Option<tempfile::TempDir>)> {
    let (cfg, dir) = make_cfg(SETTLE_SCHEMA_FN, storage, checkpoint_interval);
    let mut db = Settle::open(cfg)?;
    db.register_reducer_callback("user_balance", Box::new(balance_reducer()))?;
    Ok((db, dir))
}

fn open_settle_db_eventrules(
    storage: Storage,
    checkpoint_interval: u64,
) -> anyhow::Result<(Settle, Option<tempfile::TempDir>)> {
    let (cfg, dir) = make_cfg(SETTLE_SCHEMA_ER, storage, checkpoint_interval);
    let db = Settle::open(cfg)?;
    Ok((db, dir))
}

fn open_settle_db_mv_only(
    storage: Storage,
    checkpoint_interval: u64,
) -> anyhow::Result<(Settle, Option<tempfile::TempDir>)> {
    let (cfg, dir) = make_cfg(SETTLE_SCHEMA_MV, storage, checkpoint_interval);
    let db = Settle::open(cfg)?;
    Ok((db, dir))
}

fn pg_insert_transfers(pg: &PgRuntime, first_block: u64, rows: &[RowMap]) -> anyhow::Result<()> {
    let n = rows.len();
    if n == 0 {
        return Ok(());
    }
    let sql = build_multi_insert(
        "transfers",
        &["block_number", "from_addr", "to_addr", "value"],
        n,
    );
    let block_buf: Vec<i64> = (0..n)
        .map(|i| (first_block + (i / ROWS_PER_BLOCK) as u64) as i64)
        .collect();
    let from_buf: Vec<String> = rows
        .iter()
        .map(|r| row_str(r, "from_addr").to_string())
        .collect();
    let to_buf: Vec<String> = rows
        .iter()
        .map(|r| row_str(r, "to_addr").to_string())
        .collect();
    let val_buf: Vec<f64> = rows.iter().map(|r| row_f64(r, "value")).collect();

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n * 4);
    for i in 0..n {
        params.push(&block_buf[i]);
        params.push(&from_buf[i]);
        params.push(&to_buf[i]);
        params.push(&val_buf[i]);
    }
    pg.execute(&sql, &params)?;
    Ok(())
}

/// Per-row UPSERT — one INSERT ... ON CONFLICT per input row, one
/// round-trip each. Mirrors naive application code / unbatched ORM saves.
fn pg_upsert_balances_per_row(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    let sql = "INSERT INTO balances (from_addr, balance) VALUES ($1, $2) \
               ON CONFLICT (from_addr) DO UPDATE \
               SET balance = balances.balance + EXCLUDED.balance";
    for r in rows {
        let from = row_str(r, "from_addr").to_string();
        let v = row_f64(r, "value");
        pg.execute(sql, &[&from, &v])?;
    }
    Ok(())
}

/// Batched UPSERT — aggregation happens inside Postgres via CTE + GROUP BY
/// over `unnest` of the input arrays. One round-trip per batch, no Rust-side
/// pre-aggregation. PG handles `ON CONFLICT`'s "row-twice" constraint by
/// collapsing duplicates in `GROUP BY` before the INSERT.
fn pg_upsert_balances_batch(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let from_buf: Vec<String> = rows
        .iter()
        .map(|r| row_str(r, "from_addr").to_string())
        .collect();
    let val_buf: Vec<f64> = rows.iter().map(|r| row_f64(r, "value")).collect();
    let sql = "INSERT INTO balances (from_addr, balance) \
               SELECT from_addr, SUM(value) \
               FROM unnest($1::text[], $2::float8[]) AS x(from_addr, value) \
               GROUP BY from_addr \
               ON CONFLICT (from_addr) DO UPDATE \
               SET balance = balances.balance + EXCLUDED.balance";
    pg.execute(sql, &[&from_buf, &val_buf])?;
    Ok(())
}

fn pg_upsert_balances_from_changes(
    pg: &PgRuntime,
    records: &[ChangeRecord],
) -> anyhow::Result<()> {
    let n = records.len();
    if n == 0 {
        return Ok(());
    }
    let sql = format!(
        "{} ON CONFLICT (from_addr) DO UPDATE SET balance = EXCLUDED.balance",
        build_multi_insert("balances", &["from_addr", "balance"], n),
    );
    let from_buf: Vec<String> = records
        .iter()
        .map(|r| {
            r.values
                .get("from_addr")
                .or_else(|| r.key.get("from_addr"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    let val_buf: Vec<f64> = records
        .iter()
        .map(|r| {
            r.values
                .get("balance")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
        })
        .collect();
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n * 2);
    for i in 0..n {
        params.push(&from_buf[i]);
        params.push(&val_buf[i]);
    }
    pg.execute(&sql, &params)?;
    Ok(())
}

fn run_pg_only_per_row(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        pg_insert_transfers(pg, block_no, chunk)?;
        pg_upsert_balances_per_row(pg, chunk)?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(())
}

fn run_pg_only_batch(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        pg_insert_transfers(pg, block_no, chunk)?;
        pg_upsert_balances_batch(pg, chunk)?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(())
}

/// Settle ingest (with reducer/MV) → forward to PG. Returns time spent
/// inside Settle.ingest_blocks (separate from PG forwarding cost).
///
/// `pre_finalized=true` uses ingest_with_finalized with finalized_head set
/// far ahead, so ingested blocks land directly as finalized state.
/// Read final state from PG and return (row_count, sum_of_balances).
/// Used to verify all variants land the same data.
fn read_pg_state(pg: &PgRuntime) -> anyhow::Result<(i64, f64)> {
    let rows = pg
        .rt
        .block_on(async {
            pg.client
                .query("SELECT COUNT(*), COALESCE(SUM(balance), 0) FROM balances", &[])
                .await
        })?;
    let r = &rows[0];
    Ok((r.get::<_, i64>(0), r.get::<_, f64>(1)))
}

fn expected_state() -> (i64, f64) {
    // For our deterministic generator (gen_transfer with NUM_USERS=10_000,
    // TOTAL_ROWS=100_000), every user gets exactly 10 transfers. The sum of
    // all values = SUM(1.0 + i*0.01 for i in 0..100000) = 50099500.
    let mut sum = 0.0_f64;
    for i in 0..TOTAL_ROWS {
        sum += 1.0 + i as f64 * 0.01;
    }
    (NUM_USERS as i64, sum)
}

fn check_pg_state(pg: &PgRuntime, label: &str) -> anyhow::Result<()> {
    let (actual_count, actual_sum) = read_pg_state(pg)?;
    let (exp_count, exp_sum) = expected_state();
    // Allow tiny float drift from order-of-summation.
    let sum_ok = (actual_sum - exp_sum).abs() < 0.01;
    let ok = actual_count == exp_count && sum_ok;
    eprintln!(
        "  [check] {label:<28}  rows={:>5} (exp {:>5})  sum={:>14.2} (exp {:>14.2})  {}",
        actual_count,
        exp_count,
        actual_sum,
        exp_sum,
        if ok { "OK" } else { "*** MISMATCH ***" }
    );
    if !ok {
        anyhow::bail!("{label}: state mismatch");
    }
    Ok(())
}

/// `no_lag = false` → tip operation (finality stays put, `ingest_blocks`).
/// `no_lag = true` → historical backfill: each batch reports finality at its
/// own tip block (`finalized_head = max block in batch`), the realistic
/// no-confirmation-lag case. Combined with `backfill_checkpoint_interval > 1`
/// on the Config, this is what activates persist-deferral.
fn run_settle_pg_timed(
    db: &mut Settle,
    pg: &PgRuntime,
    rows: &[RowMap],
    no_lag: bool,
) -> anyhow::Result<std::time::Duration> {
    let mut block_no = 1u64;
    let mut settle_total = std::time::Duration::ZERO;
    for chunk in rows.chunks(BATCH_SIZE) {
        let items: Vec<(String, u64, Vec<RowMap>)> = split_blocks(chunk, block_no)
            .into_iter()
            .map(|(b, c)| ("transfers".to_string(), b, c.to_vec()))
            .collect();
        // Highest block in this batch — the realistic no-lag finalized head.
        let batch_tip = items.iter().map(|(_, b, _)| *b).max().unwrap_or(block_no);
        let t = Instant::now();
        let batch: ChangeBatch = if no_lag {
            ingest_with_finalized(db, items, batch_tip)?
        } else {
            ingest_blocks(db, items)?
        }
        .expect("non-empty batch");
        settle_total += t.elapsed();

        pg_insert_transfers(pg, block_no, chunk)?;
        pg_upsert_balances_from_changes(pg, batch.records_for("balances"))?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(settle_total)
}

fn main() -> anyhow::Result<()> {
    let rows: Vec<RowMap> = (0..TOTAL_ROWS)
        .map(|i| gen_transfer(i, NUM_USERS))
        .collect();

    eprintln!("workload: vs_postgres_simple_agg");
    eprintln!(
        "  config: {TOTAL_ROWS} rows, {NUM_USERS} users, {ROWS_PER_BLOCK} rows/block, \
         {BLOCKS_PER_BATCH} blocks/batch, {} batches",
        TOTAL_ROWS / BATCH_SIZE
    );

    // postgres_only (no Settle) — PG handles everything, settle portion = 0
    let pg = PgRuntime::start()?;
    pg.batch_execute(PG_SCHEMA)?;
    pg.take_stats();
    let t = Instant::now();
    run_pg_only_per_row(&pg, &rows)?;
    let elapsed = t.elapsed();
    print_result_split(
        "pg_only_per_row",
        TOTAL_ROWS,
        elapsed,
        std::time::Duration::ZERO,
        pg.take_stats(),
    );
    check_pg_state(&pg, "pg_only_per_row")?;
    drop(pg);

    let pg = PgRuntime::start()?;
    pg.batch_execute(PG_SCHEMA)?;
    pg.take_stats();
    let t = Instant::now();
    run_pg_only_batch(&pg, &rows)?;
    let elapsed = t.elapsed();
    print_result_split(
        "pg_only_batch",
        TOTAL_ROWS,
        elapsed,
        std::time::Duration::ZERO,
        pg.take_stats(),
    );
    check_pg_state(&pg, "pg_only_batch")?;
    drop(pg);

    // Settle: reducer style × {Memory, Rocks} × {mode}.
    //
    // Modes (tag, pre_finalized, checkpoint_interval):
    //  - ""         tip operation: finality lags, persist every finalize.
    //  - ",fin"     no-lag backfill, persist every finalize (durable == finality).
    //  - ",backfill" no-lag backfill + checkpoint_interval=100 → derived-state
    //               persistence DEFERRED (the historical-load fast path). Only
    //               effective for deferral-safe pipelines (er, mv); the fn
    //               pipeline is LANGUAGE EXTERNAL so deferral stays off by design,
    //               so its ",backfill" row should ≈ its ",fin" row.
    // interval=1000 > BLOCKS_PER_BATCH (100): a checkpoint fires roughly every
    // 10 batches, so 9/10 batches defer their derived-state persist.
    let modes: [(&str, bool, u64); 3] = [
        ("", false, 1),
        (",fin", true, 1),
        (",backfill", true, 1000),
    ];
    for (tag, pre_finalized, interval) in modes {
        for storage in [Storage::Memory, Storage::Rocks] {
            // FnReducer
            {
                let (mut db, _dir) = open_settle_db_fn(storage, interval)?;
                let pg = PgRuntime::start()?;
                pg.batch_execute(PG_SCHEMA)?;
                pg.take_stats();
                let t = Instant::now();
                let settle_t = run_settle_pg_timed(&mut db, &pg, &rows, pre_finalized)?;
                let total = t.elapsed();
                let label = format!("settle_fn[{}{tag}]_then_pg", storage.label());
                print_result_split(&label, TOTAL_ROWS, total, settle_t, pg.take_stats());
                check_pg_state(&pg, &label)?;
            }
            // EventRules
            {
                let (mut db, _dir) = open_settle_db_eventrules(storage, interval)?;
                let pg = PgRuntime::start()?;
                pg.batch_execute(PG_SCHEMA)?;
                pg.take_stats();
                let t = Instant::now();
                let settle_t = run_settle_pg_timed(&mut db, &pg, &rows, pre_finalized)?;
                let total = t.elapsed();
                let label = format!("settle_er[{}{tag}]_then_pg", storage.label());
                print_result_split(&label, TOTAL_ROWS, total, settle_t, pg.take_stats());
                check_pg_state(&pg, &label)?;
            }
            // MV-only (no reducer)
            {
                let (mut db, _dir) = open_settle_db_mv_only(storage, interval)?;
                let pg = PgRuntime::start()?;
                pg.batch_execute(PG_SCHEMA)?;
                pg.take_stats();
                let t = Instant::now();
                let settle_t = run_settle_pg_timed(&mut db, &pg, &rows, pre_finalized)?;
                let total = t.elapsed();
                let label = format!("settle_mv[{}{tag}]_then_pg", storage.label());
                print_result_split(&label, TOTAL_ROWS, total, settle_t, pg.take_stats());
                check_pg_state(&pg, &label)?;
            }
        }
    }

    Ok(())
}
