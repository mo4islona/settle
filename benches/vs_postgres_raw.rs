//! Workload 1: Raw passthrough.
//!   - **postgres_only**: rows → Postgres via multi-row `INSERT VALUES`.
//!   - **settle→postgres**: Settle ingests the same multi-block batch
//!     (RocksDB WAL, ack, fork-tracking), then rows are forwarded to PG.
//!
//! Each batch: BLOCKS_PER_BATCH (20) × ROWS_PER_BLOCK (50) = 1000 rows.
//!
//! Run with: `cargo bench --bench vs_postgres_raw` (Docker required)

use std::time::Instant;

use settle::db::{Config, Settle};
use settle::test_helpers::ingest_blocks;
use settle::types::RowMap;
use tokio_postgres::types::ToSql;

#[path = "common/mod.rs"]
mod common;

use common::{
    build_multi_insert, gen_transfer, print_result, print_result_no_pg, row_f64, row_str,
    split_blocks, PgRuntime, BATCH_SIZE, BLOCKS_PER_BATCH, ROWS_PER_BLOCK,
};

const SETTLE_SCHEMA: &str = r#"
CREATE TABLE transfers (
    block_number UInt64,
    from_addr    String,
    to_addr      String,
    value        Float64
);
"#;

const PG_SCHEMA: &str = "
CREATE TABLE transfers (
    block_number BIGINT NOT NULL,
    from_addr    TEXT   NOT NULL,
    to_addr      TEXT   NOT NULL,
    value        DOUBLE PRECISION NOT NULL
);
";

const TOTAL_ROWS: usize = 100_000;

fn open_settle_db() -> anyhow::Result<(Settle, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let cfg = Config::with_data_dir(SETTLE_SCHEMA, dir.path().to_str().unwrap());
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

fn run_pg_only(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        pg_insert_transfers(pg, block_no, chunk)?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(())
}

fn run_settle_only(db: &mut Settle, rows: &[RowMap]) -> anyhow::Result<()> {
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        let items: Vec<(String, u64, Vec<RowMap>)> = split_blocks(chunk, block_no)
            .into_iter()
            .map(|(b, c)| ("transfers".to_string(), b, c.to_vec()))
            .collect();
        ingest_blocks(db, items)?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(())
}

fn run_settle_pg(db: &mut Settle, pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        let items: Vec<(String, u64, Vec<RowMap>)> = split_blocks(chunk, block_no)
            .into_iter()
            .map(|(b, c)| ("transfers".to_string(), b, c.to_vec()))
            .collect();
        ingest_blocks(db, items)?;
        pg_insert_transfers(pg, block_no, chunk)?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let rows: Vec<RowMap> = (0..TOTAL_ROWS)
        .map(|i| gen_transfer(i, 10_000))
        .collect();

    eprintln!("workload: vs_postgres_raw");
    eprintln!(
        "  config: {TOTAL_ROWS} rows, {ROWS_PER_BLOCK} rows/block, \
         {BLOCKS_PER_BATCH} blocks/batch, {} batches",
        TOTAL_ROWS / BATCH_SIZE
    );

    // postgres_only
    let pg = PgRuntime::start()?;
    pg.batch_execute(PG_SCHEMA)?;
    pg.take_stats();
    let t = Instant::now();
    run_pg_only(&pg, &rows)?;
    let elapsed = t.elapsed();
    print_result("postgres_only", TOTAL_ROWS, elapsed, pg.take_stats());
    drop(pg);

    // settle_only — Settle ingest, no PG
    {
        let (mut db, _dir) = open_settle_db()?;
        let t = Instant::now();
        run_settle_only(&mut db, &rows)?;
        let elapsed = t.elapsed();
        print_result_no_pg("settle_only", TOTAL_ROWS, elapsed);
    }

    // settle_then_postgres
    let (mut db, _dir) = open_settle_db()?;
    let pg = PgRuntime::start()?;
    pg.batch_execute(PG_SCHEMA)?;
    pg.take_stats();
    let t = Instant::now();
    run_settle_pg(&mut db, &pg, &rows)?;
    let elapsed = t.elapsed();
    print_result("settle_then_postgres", TOTAL_ROWS, elapsed, pg.take_stats());

    Ok(())
}
