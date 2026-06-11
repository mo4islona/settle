//! Shared infrastructure for `vs_postgres_*` benches.
//!
//! Provides `BenchBackend` — a unified ingest interface used by both
//! Settle and Postgres adapters so workload code stays identical across
//! engines. Extendable later for ClickHouse, DuckDB, etc.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use settle::types::{RowMap, Value};
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres as PgImage;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};

// ─── Batch shape ───────────────────────────────────────────────────────────

/// One ingest call carries multiple consecutive blocks. Realistic for EVM
/// indexing where the source feeds N blocks per chunk and each block has a
/// modest number of events.
pub const ROWS_PER_BLOCK: usize = 50;
pub const BLOCKS_PER_BATCH: usize = 100;
pub const BATCH_SIZE: usize = ROWS_PER_BLOCK * BLOCKS_PER_BATCH; // 5000

/// Slice a flat batch of rows into per-block groups for a given starting
/// block number. Returns `(block_number, slice)` pairs.
pub fn split_blocks<'a>(
    rows: &'a [RowMap],
    first_block: u64,
) -> Vec<(u64, &'a [RowMap])> {
    rows.chunks(ROWS_PER_BLOCK)
        .enumerate()
        .map(|(i, chunk)| (first_block + i as u64, chunk))
        .collect()
}

// ─── Postgres runtime (shared infra for all PG-backed benches) ─────────────

/// Snapshot of Postgres operation counters. Print after each pipeline run
/// to compare op-count between `postgres-only` and `settle→postgres`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgStats {
    pub write_statements: u64,
    pub read_statements: u64,
    pub rows_written: u64,
    pub rows_read: u64,
}

impl PgStats {
    pub fn print(&self, label: &str) {
        eprintln!(
            "  [pg ops]  {label:<36}  writes={:>6}  rows_w={:>9}  reads={:>4}  rows_r={:>6}",
            self.write_statements, self.rows_written, self.read_statements, self.rows_read,
        );
    }
}

/// Print a single pipeline result row — wall time, rows/sec, PG op counts.
pub fn print_result(label: &str, rows: usize, elapsed: Duration, stats: PgStats) {
    let secs = elapsed.as_secs_f64();
    let rps = rows as f64 / secs;
    eprintln!(
        "  {label:<24}  {:>8.3}s  {:>10.0} rows/s   \
         pg_writes={:>6}  pg_rows_w={:>8}  pg_reads={:>3}  pg_rows_r={:>5}",
        secs,
        rps,
        stats.write_statements,
        stats.rows_written,
        stats.read_statements,
        stats.rows_read,
    );
}

/// Print a result row that touches no Postgres (e.g. settle-only).
pub fn print_result_no_pg(label: &str, rows: usize, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let rps = rows as f64 / secs;
    eprintln!(
        "  {label:<28}  {:>7.3}s  {:>9.0} rows/s   (no postgres)",
        secs, rps,
    );
}

/// Print result split between Settle internal and Postgres forwarding portions.
pub fn print_result_split(
    label: &str,
    rows: usize,
    total: Duration,
    settle: Duration,
    stats: PgStats,
) {
    let total_s = total.as_secs_f64();
    let settle_s = settle.as_secs_f64();
    let pg_s = (total - settle).as_secs_f64();
    let rps = rows as f64 / total_s;
    eprintln!(
        "  {label:<28}  total={:>6.3}s  settle={:>6.3}s  pg={:>6.3}s  {:>8.0} rows/s   \
         pg_w={:>5} pg_rows_w={:>8} pg_r={:>4} pg_rows_r={:>8}",
        total_s,
        settle_s,
        pg_s,
        rps,
        stats.write_statements,
        stats.rows_written,
        stats.read_statements,
        stats.rows_read,
    );
}

/// Wraps a `testcontainers` Postgres instance and a `tokio` runtime so
/// the sync `BenchBackend::ingest_batch` can drive async `tokio_postgres`.
///
/// Tracks write/read statement counts and rows affected so benches can
/// report op-count alongside wall-clock time.
pub struct PgRuntime {
    pub rt: tokio::runtime::Runtime,
    pub client: Client,
    // Option so Drop can take it out and drop it inside a runtime
    // context (testcontainers' ContainerAsync::drop is async).
    container: Option<ContainerAsync<PgImage>>,
    write_statements: AtomicU64,
    read_statements: AtomicU64,
    rows_written: AtomicU64,
    rows_read: AtomicU64,
}

impl Drop for PgRuntime {
    fn drop(&mut self) {
        if let Some(c) = self.container.take() {
            // Drop the container inside a tokio runtime context so its
            // async cleanup hooks can call `current()`.
            let _guard = self.rt.enter();
            drop(c);
        }
    }
}

impl PgRuntime {
    pub fn start() -> anyhow::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;

        let (container, client) = rt.block_on(async {
            let container = PgImage::default().start().await?;
            let host = container.get_host().await?;
            let port = container.get_host_port_ipv4(5432).await?;
            let conn_str = format!(
                "host={host} port={port} user=postgres password=postgres dbname=postgres"
            );
            let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    eprintln!("postgres connection error: {e}");
                }
            });
            Ok::<_, anyhow::Error>((container, client))
        })?;

        Ok(Self {
            rt,
            client,
            container: Some(container),
            write_statements: AtomicU64::new(0),
            read_statements: AtomicU64::new(0),
            rows_written: AtomicU64::new(0),
            rows_read: AtomicU64::new(0),
        })
    }

    /// Run a multi-statement script. NOT counted in stats — meant for
    /// schema setup and `TRUNCATE` between iterations.
    pub fn batch_execute(&self, sql: &str) -> anyhow::Result<()> {
        self.rt.block_on(self.client.batch_execute(sql))?;
        Ok(())
    }

    /// Start a transaction on the connection. All subsequent `execute` /
    /// `query` calls land in this transaction until `commit` is called.
    /// Reduces fsync cost: 1 commit per batch instead of one per statement.
    pub fn begin(&self) -> anyhow::Result<()> {
        self.rt.block_on(self.client.batch_execute("BEGIN"))?;
        Ok(())
    }

    pub fn commit(&self) -> anyhow::Result<()> {
        self.rt.block_on(self.client.batch_execute("COMMIT"))?;
        Ok(())
    }

    /// Run a single write statement (INSERT/UPDATE/UPSERT/DELETE). Counted.
    pub fn execute(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> anyhow::Result<u64> {
        let n = self.rt.block_on(self.client.execute(sql, params))?;
        self.write_statements.fetch_add(1, Ordering::Relaxed);
        self.rows_written.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }

    /// Run a single read statement (SELECT). Counted.
    pub fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> anyhow::Result<Vec<tokio_postgres::Row>> {
        let rows = self.rt.block_on(self.client.query(sql, params))?;
        self.read_statements.fetch_add(1, Ordering::Relaxed);
        self.rows_read.fetch_add(rows.len() as u64, Ordering::Relaxed);
        Ok(rows)
    }

    /// Snapshot all counters and reset them to zero. Use at the start and
    /// end of a measured section to get per-section totals.
    pub fn take_stats(&self) -> PgStats {
        PgStats {
            write_statements: self.write_statements.swap(0, Ordering::Relaxed),
            read_statements: self.read_statements.swap(0, Ordering::Relaxed),
            rows_written: self.rows_written.swap(0, Ordering::Relaxed),
            rows_read: self.rows_read.swap(0, Ordering::Relaxed),
        }
    }
}

// ─── Multi-row INSERT/UPSERT helpers ───────────────────────────────────────

/// Build `INSERT INTO <table> (<cols>) VALUES ($1, $2, ...), ($N+1, ...)` for
/// `n_rows` rows. No trailing newline; caller appends `ON CONFLICT ...` if
/// needed. Placeholders are 1-indexed (tokio-postgres convention).
pub fn build_multi_insert(table: &str, cols: &[&str], n_rows: usize) -> String {
    debug_assert!(n_rows > 0 && !cols.is_empty());
    let n_cols = cols.len();
    let mut sql = String::with_capacity(64 + n_rows * n_cols * 6);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" (");
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(c);
    }
    sql.push_str(") VALUES ");
    for r in 0..n_rows {
        if r > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for c in 0..n_cols {
            if c > 0 {
                sql.push_str(", ");
            }
            sql.push('$');
            sql.push_str(&(r * n_cols + c + 1).to_string());
        }
        sql.push(')');
    }
    sql
}

// ─── Data generators ───────────────────────────────────────────────────────

pub fn gen_transfer(i: usize, num_users: usize) -> RowMap {
    let from_idx = i % num_users;
    let to_idx = (i + 1) % num_users;
    HashMap::from([
        (
            "from_addr".to_string(),
            Value::String(format!("0xfrom{from_idx:06x}")),
        ),
        (
            "to_addr".to_string(),
            Value::String(format!("0xto{to_idx:06x}")),
        ),
        // Small positive delta so balances diverge but don't overflow.
        ("value".to_string(), Value::Float64(1.0 + (i as f64 * 0.01))),
    ])
}

pub fn gen_order(i: usize, num_assets: usize, num_traders: usize) -> RowMap {
    let trader_idx = i % num_traders;
    let asset_idx = i % num_assets;
    let side: u64 = if i % 5 < 3 { 0 } else { 1 };
    let amount = 1_000u64 + (i as u64 % 10_000);
    let price = 100.0 + ((i % 1000) as f64) * 0.1;
    let timestamp = 1_700_000_000u64 + (i as u64 / 100);
    // day is a computed column emitted at data-gen time so MVs can
    // `GROUP BY (asset_id, day)` without a derivation reducer (the schema
    // parser only accepts column names in GROUP BY, not expressions).
    let day = (timestamp / 86_400) as i64;
    HashMap::from([
        (
            "trader".to_string(),
            Value::String(format!("0xt{trader_idx:06x}")),
        ),
        (
            "asset_id".to_string(),
            Value::String(format!("asset_{asset_idx:04}")),
        ),
        ("side".to_string(), Value::UInt64(side)),
        ("amount".to_string(), Value::UInt64(amount)),
        ("price".to_string(), Value::Float64(price)),
        ("timestamp".to_string(), Value::UInt64(timestamp)),
        ("day".to_string(), Value::Int64(day)),
    ])
}

// ─── RowMap → typed PG params helpers ──────────────────────────────────────

/// Trade generator for stateful PnL workload. 75% buys / 25% sells, gently
/// rising price walk. Adds `token` and `day` columns so workload covers
/// per-token positions and per-day PnL aggregations.
///
/// Token: 10 distinct ("tk0".."tk9"), cycled to give each user multiple tokens.
/// Day: bucketed every 5000 trades (20 days over 100K trades).
pub fn gen_trade(i: usize, num_users: usize) -> RowMap {
    let user = i % num_users;
    // Slow token rotation: 4 trades on same token before moving to next.
    // Pattern per user: buy, buy, buy, sell on token X → buy, buy, buy, sell on token X+1.
    // Guarantees sells hit positions that have prior buys (non-zero PnL).
    let token = (user + (i / num_users) / 4) % 10;
    let day = (i / 5000) as i64;
    let side: u64 = if (i / num_users) % 4 < 3 { 0 } else { 1 };
    let amount = 1.0 + (i % 10) as f64 * 0.1;
    let price = 100.0 + ((i / 100) as f64) * 0.5;
    HashMap::from([
        ("seq".to_string(), Value::UInt64(i as u64)),
        (
            "user_addr".to_string(),
            Value::String(format!("0xu{user:06x}")),
        ),
        ("token".to_string(), Value::String(format!("tk{token}"))),
        ("day".to_string(), Value::Int64(day)),
        ("side".to_string(), Value::UInt64(side)),
        ("amount".to_string(), Value::Float64(amount)),
        ("price".to_string(), Value::Float64(price)),
    ])
}

pub fn row_str<'a>(row: &'a RowMap, col: &str) -> &'a str {
    row.get(col).and_then(|v| v.as_str()).unwrap_or_default()
}

pub fn row_i64(row: &RowMap, col: &str) -> i64 {
    row.get(col).and_then(|v| v.as_i64()).unwrap_or(0)
}

pub fn row_f64(row: &RowMap, col: &str) -> f64 {
    row.get(col).and_then(|v| v.as_f64()).unwrap_or(0.0)
}
