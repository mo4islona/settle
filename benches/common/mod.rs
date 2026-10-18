//! Shared infrastructure for `vs_postgres_*` benches.
//!
//! Provides `BenchBackend` — a unified ingest interface used by both
//! Settle and Postgres adapters so workload code stays identical across
//! engines. Extendable later for ClickHouse, DuckDB, etc.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
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
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
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

// ─── Pure-Postgres baseline ────────────────────────────────────────────────

/// One measured run of a pure-Postgres case. `output` is what the bench
/// checks later (a final state, a counter), so a cached case can still
/// take part in the correctness checks.
pub struct PgCase<T> {
    pub elapsed: Duration,
    pub stats: PgStats,
    pub output: T,
}

#[derive(Serialize, Deserialize)]
struct CachedCase {
    elapsed_secs: f64,
    runs: usize,
    recorded_at: u64,
    stats: PgStats,
    output: serde_json::Value,
}

#[derive(Serialize, Deserialize, Default)]
struct BaselineFile {
    key: String,
    cases: BTreeMap<String, CachedCase>,
}

#[derive(Clone, Copy, PartialEq)]
enum BaselineMode {
    Reuse,
    Refresh,
    Off,
}

/// Cache for the cases that run no Settle code. A Settle change cannot move
/// them, so an A/B run measured the same Postgres work on both sides.
///
/// A missing case runs `PG_BASELINE_RUNS` times (default 3) and its median
/// goes to `target/tmp/pg_baseline/<bench>.json`; later runs reuse it.
/// `PG_BASELINE=refresh` measures every case again, `PG_BASELINE=off` runs
/// each case once and keeps nothing.
///
/// The key is the bench's config and `version`, not its source: the two
/// sides of an A/B can differ in their Settle code and still share one
/// baseline. Bump `version` when the SQL or the workload of a case changes.
pub struct PgBaseline {
    path: PathBuf,
    mode: BaselineMode,
    runs: usize,
    file: BaselineFile,
}

impl PgBaseline {
    pub fn open(bench: &str, version: u32, config: &str) -> anyhow::Result<Self> {
        let mode = match std::env::var("PG_BASELINE").as_deref() {
            Err(_) | Ok("") | Ok("reuse") => BaselineMode::Reuse,
            Ok("refresh") => BaselineMode::Refresh,
            Ok("off") => BaselineMode::Off,
            Ok(other) => anyhow::bail!("PG_BASELINE={other}: expected reuse, refresh or off"),
        };
        let runs = match std::env::var("PG_BASELINE_RUNS") {
            Ok(n) => n.parse()?,
            Err(_) => 3,
        };
        anyhow::ensure!(runs > 0, "PG_BASELINE_RUNS must be at least 1");

        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join("pg_baseline")
            .join(format!("{bench}.json"));
        let key = format!("v{version} {config}");

        let stored = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<BaselineFile>(&bytes).ok());
        let file = match stored {
            Some(file) if file.key == key && mode == BaselineMode::Reuse => file,
            _ => BaselineFile {
                key,
                cases: BTreeMap::new(),
            },
        };

        Ok(Self {
            path,
            mode,
            runs,
            file,
        })
    }

    /// The case `label`: from the cache, or measured by `run`.
    pub fn case<T: Serialize + DeserializeOwned>(
        &mut self,
        label: &str,
        mut run: impl FnMut() -> anyhow::Result<PgCase<T>>,
    ) -> anyhow::Result<PgCase<T>> {
        if self.mode == BaselineMode::Reuse {
            if let Some(cached) = self.file.cases.get(label) {
                let age_hours = now_secs().saturating_sub(cached.recorded_at) / 3600;
                eprintln!(
                    "  [pg baseline] {label}: cached, median of {} runs, recorded {age_hours} h ago",
                    cached.runs
                );
                return Ok(PgCase {
                    elapsed: Duration::from_secs_f64(cached.elapsed_secs),
                    stats: cached.stats,
                    output: serde_json::from_value(cached.output.clone())?,
                });
            }
        }

        let runs = if self.mode == BaselineMode::Off {
            1
        } else {
            self.runs
        };
        let mut measured = Vec::with_capacity(runs);
        for _ in 0..runs {
            measured.push(run()?);
        }
        measured.sort_by_key(|case| case.elapsed);
        let all: Vec<String> = measured
            .iter()
            .map(|case| format!("{:.3}s", case.elapsed.as_secs_f64()))
            .collect();
        let median = measured.swap_remove(runs / 2);

        if self.mode != BaselineMode::Off {
            eprintln!(
                "  [pg baseline] {label}: measured {}, median kept",
                all.join(" ")
            );
            self.file.cases.insert(
                label.to_string(),
                CachedCase {
                    elapsed_secs: median.elapsed.as_secs_f64(),
                    runs,
                    recorded_at: now_secs(),
                    stats: median.stats,
                    output: serde_json::to_value(&median.output)?,
                },
            );
            // Written after each case, so a failed run keeps what it measured.
            std::fs::create_dir_all(self.path.parent().expect("baseline dir"))?;
            std::fs::write(&self.path, serde_json::to_vec_pretty(&self.file)?)?;
        }

        Ok(median)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ─── Fork bookkeeping in Postgres ──────────────────────────────────────────

/// rindexer prunes its block hashes once per this many blocks.
const PRUNE_INTERVAL: u64 = 50;

const NETWORK: &str = "bench";

const LAST_SYNCED_SCHEMA: &str = "
CREATE TABLE last_synced_block (
    network           TEXT PRIMARY KEY,
    last_synced_block NUMERIC
);
INSERT INTO last_synced_block VALUES ('bench', 0);
";

const BLOCK_HASHES_SCHEMA: &str = "
CREATE TABLE reorg_block_hashes (
    network      TEXT     NOT NULL,
    block_number BIGINT   NOT NULL,
    block_hash   CHAR(66) NOT NULL,
    parent_hash  CHAR(66) NOT NULL,
    PRIMARY KEY (network, block_number)
);
";

const PONDER_SCHEMA: &str = "
CREATE SEQUENCE _reorg_operation_id;
CREATE TABLE _ponder_checkpoint (
    chain_name           TEXT PRIMARY KEY,
    chain_id             BIGINT NOT NULL,
    safe_checkpoint      VARCHAR(75) NOT NULL,
    latest_checkpoint    VARCHAR(75) NOT NULL,
    finalized_checkpoint VARCHAR(75) NOT NULL
);
";

/// What a Postgres-only indexer at the chain tip keeps so that it can undo a
/// reorg. Settle keeps this itself, so without it a pure Postgres case skips
/// work that its Settle counterpart does. All of it is written in the
/// transaction of the block it belongs to. A backfill keeps none of it:
/// Ponder creates its triggers after the backfill, and rindexer stores the
/// hashes of live blocks only.
///
/// `PG_FORKS` selects the variants a bench runs: a comma list of `off`,
/// `blocks` and `undo`, or `all`. The default is `off`.
#[derive(Clone, Copy, PartialEq)]
pub enum PgForks {
    Off,
    /// rindexer: a row per unfinalized block in `reorg_block_hashes`, pruned
    /// below finality once per `PRUNE_INTERVAL` blocks, and the last synced
    /// block. Undoing the data of a reorg is left to the indexer.
    Blocks,
    /// Ponder: a trigger copies each row that an INSERT, UPDATE or DELETE
    /// touches into `_reorg__<table>`, and the copies get the checkpoint of
    /// their block when it ends. A reorg restores the rows from the copies.
    /// Ponder keeps block hashes in memory only.
    Undo,
}

impl PgForks {
    pub fn from_env() -> anyhow::Result<Vec<PgForks>> {
        let value = std::env::var("PG_FORKS").unwrap_or_default();
        let mut variants = Vec::new();
        for name in value.split(',').map(str::trim).filter(|name| !name.is_empty()) {
            let named: &[PgForks] = match name {
                "off" => &[PgForks::Off],
                "blocks" => &[PgForks::Blocks],
                "undo" => &[PgForks::Undo],
                "all" => &[PgForks::Off, PgForks::Blocks, PgForks::Undo],
                other => anyhow::bail!("PG_FORKS={other}: expected off, blocks, undo or all"),
            };
            for &variant in named {
                if !variants.contains(&variant) {
                    variants.push(variant);
                }
            }
        }
        if variants.is_empty() {
            variants.push(PgForks::Off);
        }
        Ok(variants)
    }

    /// The suffix of a case label.
    pub fn label(self) -> &'static str {
        match self {
            PgForks::Off => "",
            PgForks::Blocks => "+blocks",
            PgForks::Undo => "+undo",
        }
    }

    /// Creates the bookkeeping tables. `tables` are the tables the indexer
    /// writes; the undo log covers all of them, as Ponder's does.
    pub fn setup(self, pg: &PgRuntime, tables: &[&str]) -> anyhow::Result<ForkLog> {
        let mut log = ForkLog {
            forks: self,
            tables: Vec::new(),
        };
        match self {
            PgForks::Off => {}
            PgForks::Blocks => {
                pg.batch_execute(BLOCK_HASHES_SCHEMA)?;
                pg.batch_execute(LAST_SYNCED_SCHEMA)?;
            }
            PgForks::Undo => {
                pg.batch_execute(PONDER_SCHEMA)?;
                let zero = checkpoint(0);
                pg.batch_execute(&format!(
                    "INSERT INTO _ponder_checkpoint VALUES ('{NETWORK}', 1, '{zero}', '{zero}', '{zero}')"
                ))?;
                for &name in tables {
                    let table = UndoTable::read(pg, name)?;
                    pg.batch_execute(&table.create_sql())?;
                    pg.batch_execute(&table.create_trigger_sql())?;
                    log.tables.push(table);
                }
            }
        }
        Ok(log)
    }
}

/// The fork bookkeeping of one Postgres run.
pub struct ForkLog {
    forks: PgForks,
    tables: Vec<UndoTable>,
}

impl ForkLog {
    /// Whether `rollback` undoes the data too, so the indexer has nothing
    /// left to undo.
    pub fn undoes_data(&self) -> bool {
        self.forks == PgForks::Undo
    }

    /// The indexer wrote the blocks `first..=last`; `finalized` is the
    /// finalized head after them.
    pub fn record(
        &self,
        pg: &PgRuntime,
        first: u64,
        last: u64,
        finalized: u64,
    ) -> anyhow::Result<()> {
        match self.forks {
            PgForks::Off => Ok(()),
            PgForks::Blocks => record_block_hashes(pg, first, last, finalized),
            PgForks::Undo => self.commit_undo(pg, last, finalized),
        }
    }

    /// A reorg dropped the blocks above `fork_point`.
    pub fn rollback(&self, pg: &PgRuntime, fork_point: u64) -> anyhow::Result<()> {
        match self.forks {
            PgForks::Off => Ok(()),
            PgForks::Blocks => {
                let fork_point = fork_point as i64;
                pg.execute(
                    "DELETE FROM reorg_block_hashes WHERE network = $1 AND block_number > $2",
                    &[&NETWORK, &fork_point],
                )?;
                pg.execute(
                    "UPDATE last_synced_block SET last_synced_block = $1::bigint WHERE network = $2",
                    &[&fork_point, &NETWORK],
                )?;
                Ok(())
            }
            PgForks::Undo => self.revert_undo(pg, fork_point),
        }
    }

    /// Ponder's `commitBlock` and `finalize`, once per batch instead of once
    /// per block: the benches write a batch with one statement per table.
    fn commit_undo(&self, pg: &PgRuntime, last: u64, finalized: u64) -> anyhow::Result<()> {
        let latest = checkpoint(last);
        let finalized = checkpoint(finalized);
        let pending = checkpoint(u64::MAX);

        for table in &self.tables {
            let sql = format!(
                "UPDATE \"{}\" SET checkpoint = $1 WHERE checkpoint = $2",
                table.undo_name()
            );
            pg.execute(&sql, &[&latest, &pending])?;
        }
        for table in &self.tables {
            let sql = format!("DELETE FROM \"{}\" WHERE checkpoint <= $1", table.undo_name());
            pg.execute(&sql, &[&finalized])?;
        }
        pg.execute(
            "UPDATE _ponder_checkpoint SET latest_checkpoint = $1, \
             safe_checkpoint = $2, finalized_checkpoint = $2 WHERE chain_name = $3",
            &[&latest, &finalized, &NETWORK],
        )?;
        Ok(())
    }

    /// Ponder drops the triggers, so that the revert itself is not logged,
    /// and creates them again, in the same transaction.
    fn revert_undo(&self, pg: &PgRuntime, fork_point: u64) -> anyhow::Result<()> {
        for table in &self.tables {
            pg.batch_execute(&format!("DROP TRIGGER _reorg_trigger ON \"{}\"", table.name))?;
        }

        let fork_checkpoint = checkpoint(fork_point);
        for table in &self.tables {
            if table.primary_key.is_empty() {
                // Raw rows without a key: they go by block number.
                let sql = format!("DELETE FROM \"{}\" WHERE checkpoint > $1", table.undo_name());
                pg.execute(&sql, &[&fork_checkpoint])?;
                let sql = format!("DELETE FROM \"{}\" WHERE block_number > $1", table.name);
                pg.execute(&sql, &[&(fork_point as i64)])?;
            } else {
                pg.execute(&table.revert_sql(), &[&fork_checkpoint])?;
            }
        }

        for table in &self.tables {
            pg.batch_execute(&table.create_trigger_sql())?;
        }
        Ok(())
    }
}

fn record_block_hashes(
    pg: &PgRuntime,
    first: u64,
    last: u64,
    finalized: u64,
) -> anyhow::Result<()> {
    // rindexer writes one row per block; a batch goes in one statement.
    let hot_from = first.max(finalized.saturating_add(1));
    if hot_from <= last {
        let numbers: Vec<i64> = (hot_from..=last).map(|b| b as i64).collect();
        let hashes: Vec<String> = (hot_from..=last).map(block_hash).collect();
        let parents: Vec<String> = (hot_from..=last).map(|b| block_hash(b - 1)).collect();
        pg.execute(
            "INSERT INTO reorg_block_hashes (network, block_number, block_hash, parent_hash) \
             SELECT $1, * FROM unnest($2::bigint[], $3::text[], $4::text[]) \
             ON CONFLICT (network, block_number) DO UPDATE \
             SET block_hash = EXCLUDED.block_hash, parent_hash = EXCLUDED.parent_hash",
            &[&NETWORK, &numbers, &hashes, &parents],
        )?;
    }

    let prune_due = last / PRUNE_INTERVAL > first.saturating_sub(1) / PRUNE_INTERVAL;
    if prune_due {
        pg.execute(
            "DELETE FROM reorg_block_hashes WHERE network = $1 AND block_number <= $2",
            &[&NETWORK, &(finalized as i64)],
        )?;
    }

    pg.execute(
        "UPDATE last_synced_block SET last_synced_block = $1::bigint \
         WHERE network = $2 AND $1::bigint > last_synced_block",
        &[&(last as i64), &NETWORK],
    )?;
    Ok(())
}

/// A table under Ponder's undo log.
struct UndoTable {
    name: String,
    columns: Vec<String>,
    primary_key: Vec<String>,
}

impl UndoTable {
    fn read(pg: &PgRuntime, name: &str) -> anyhow::Result<Self> {
        let columns = pg.rt.block_on(pg.client.query(
            "SELECT column_name::text FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = $1::text ORDER BY ordinal_position",
            &[&name],
        ))?;
        let primary_key = pg.rt.block_on(pg.client.query(
            "SELECT a.attname::text FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = $1::text::regclass AND i.indisprimary",
            &[&name],
        ))?;
        Ok(Self {
            name: name.to_string(),
            columns: columns.iter().map(|row| row.get(0)).collect(),
            primary_key: primary_key.iter().map(|row| row.get(0)).collect(),
        })
    }

    fn undo_name(&self) -> String {
        format!("_reorg__{}", self.name)
    }

    fn create_sql(&self) -> String {
        let undo = self.undo_name();
        format!(
            "CREATE TABLE \"{undo}\" ( \
                 LIKE \"{}\", \
                 operation_id INTEGER NOT NULL PRIMARY KEY DEFAULT nextval('_reorg_operation_id'), \
                 operation INTEGER NOT NULL, \
                 checkpoint VARCHAR(75) NOT NULL \
             ); \
             CREATE INDEX ON \"{undo}\" (checkpoint);",
            self.name
        )
    }

    fn create_trigger_sql(&self) -> String {
        let undo = self.undo_name();
        let columns = self.quoted_columns("");
        let new = self.quoted_columns("NEW.");
        let old = self.quoted_columns("OLD.");
        let pending = checkpoint(u64::MAX);
        format!(
            "CREATE OR REPLACE FUNCTION \"{undo}_fn\"() RETURNS TRIGGER AS $$ \
             BEGIN \
             IF TG_OP = 'INSERT' THEN \
             INSERT INTO \"{undo}\" ({columns}, operation, checkpoint) VALUES ({new}, 0, '{pending}'); \
             ELSIF TG_OP = 'UPDATE' THEN \
             INSERT INTO \"{undo}\" ({columns}, operation, checkpoint) VALUES ({old}, 1, '{pending}'); \
             ELSIF TG_OP = 'DELETE' THEN \
             INSERT INTO \"{undo}\" ({columns}, operation, checkpoint) VALUES ({old}, 2, '{pending}'); \
             END IF; \
             RETURN NULL; \
             END; \
             $$ LANGUAGE plpgsql; \
             CREATE TRIGGER _reorg_trigger AFTER INSERT OR UPDATE OR DELETE ON \"{}\" \
             FOR EACH ROW EXECUTE PROCEDURE \"{undo}_fn\"();",
            self.name
        )
    }

    /// Ponder's revert: the first logged operation on each key after the
    /// fork point holds the row as it was there.
    fn revert_sql(&self) -> String {
        let undo = self.undo_name();
        let key = self.quoted_key("");
        let key_join = |left: &str, right: &str| {
            self.primary_key
                .iter()
                .map(|c| format!("{left}.\"{c}\" = {right}.\"{c}\""))
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        let columns = self.quoted_columns("");
        let logged_columns = self.quoted_columns("reverted1.");
        let set = self
            .columns
            .iter()
            .map(|c| format!("\"{c}\" = EXCLUDED.\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "WITH reverted1 AS ( \
                 DELETE FROM \"{undo}\" WHERE checkpoint > $1 RETURNING * \
             ), reverted2 AS ( \
                 SELECT {key}, MIN(operation_id) AS operation_id FROM reverted1 GROUP BY {key} \
             ), reverted3 AS ( \
                 SELECT {logged_columns}, reverted1.operation FROM reverted2 \
                 INNER JOIN reverted1 ON {} AND reverted2.operation_id = reverted1.operation_id \
             ), inserted AS ( \
                 DELETE FROM \"{name}\" AS t WHERE EXISTS ( \
                     SELECT * FROM reverted3 WHERE {} AND operation = 0 \
                 ) RETURNING * \
             ), updated_or_deleted AS ( \
                 INSERT INTO \"{name}\" SELECT {columns} FROM reverted3 \
                 WHERE operation = 1 OR operation = 2 \
                 ON CONFLICT ({key}) DO UPDATE SET {set} RETURNING * \
             ) SELECT COUNT(*) FROM reverted1",
            key_join("reverted2", "reverted1"),
            key_join("t", "reverted3"),
            name = self.name,
        )
    }

    fn quoted_columns(&self, prefix: &str) -> String {
        self.columns
            .iter()
            .map(|c| format!("{prefix}\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn quoted_key(&self, prefix: &str) -> String {
        self.primary_key
            .iter()
            .map(|c| format!("{prefix}\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Ponder's checkpoints are 75-character strings that sort by block.
fn checkpoint(block: u64) -> String {
    format!("{block:075}")
}

/// A 32-byte hash as `0x` hex. Its value does not change the cost; its
/// length does.
fn block_hash(height: u64) -> String {
    let h = height.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    format!(
        "0x{h:016x}{:016x}{:016x}{:016x}",
        h.rotate_left(16),
        h.rotate_left(32),
        h.rotate_left(48)
    )
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
