//! Runs the phases of a scenario on both sides and reports them.
//!
//! The parent starts a Postgres container for each case and runs the case in
//! a child process of this binary, so that the memory peak and the allocator
//! of one case do not carry into the next, and so that a case can crash.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader};
use std::ops::Range;
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use settle::db::{BlockRows, Config, IngestRows, Settle};
use settle::types::{BlockCursor, ChangeBatch};

use crate::common::{PgForks, PgRuntime};
use crate::fixture::{Block, Finality, Fixture};
use crate::metrics;

/// One app, written twice: on Postgres alone, and on Settle with Postgres as
/// its target. Both sides write the same Postgres tables.
pub trait Scenario {
    const NAME: &'static str;
    const SETTLE_SCHEMA: &'static str;
    const PG_SCHEMA: &'static str;
    /// The tables that the app writes. At the tip, the undo log of the
    /// Postgres-only side covers them.
    const PG_TABLES: &'static [&'static str];

    /// What the app has after decoding, so that neither side pays for it.
    type Data;

    fn decode(fx: &Fixture) -> Self::Data;

    /// Postgres only: write `blocks` inside the open transaction.
    fn pg_only_write(
        pg: &PgRuntime,
        fx: &Fixture,
        data: &Self::Data,
        blocks: Range<usize>,
    ) -> anyhow::Result<()>;

    /// Settle: the input rows of `blocks`.
    fn settle_rows(
        db: &Settle,
        fx: &Fixture,
        data: &Self::Data,
        blocks: Range<usize>,
    ) -> HashMap<String, BlockRows>;

    /// Settle: apply a change batch inside the open transaction.
    fn pg_apply(pg: &PgRuntime, batch: &ChangeBatch) -> anyhow::Result<()>;

    /// Fails unless Postgres holds the state of the blocks `..end`.
    fn check(pg: &PgRuntime, fx: &Fixture, data: &Self::Data, end: usize) -> anyhow::Result<()>;
}

/// `maxBytes` of the pipes SDK Portal client.
const PIPES_MAX_BYTES: u64 = 10 * 1024 * 1024;
const TIP_BLOCKS: usize = 1_000;
/// The default of `Config::max_pending_batches`.
const PENDING_BATCHES: usize = 2;
/// Where in the backfill run `r` crashes, as a share of its batches.
const CRASH_POINTS: [f64; 5] = [0.5, 0.37, 0.63, 0.44, 0.56];

/// The cursor of the Postgres-only app and the applied sequence of the
/// Settle app (journal mode, as in the docs).
const APP_TABLES: &str = "
CREATE TABLE app_cursor (id INT PRIMARY KEY, number BIGINT NOT NULL, hash TEXT NOT NULL);
INSERT INTO app_cursor VALUES (1, 0, '');
CREATE TABLE settle_sequence (id INT PRIMARY KEY, sequence BIGINT NOT NULL);
INSERT INTO settle_sequence VALUES (1, 0);
";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Pg,
    Settle,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Side::Pg => "Postgres only",
            Side::Settle => "Settle → Postgres",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Backfill,
    Tip,
    Restart,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    /// The backfill, then the tip if `Case::tip`.
    Full,
    /// The backfill until the batch at `share` of the batches is in flight;
    /// then the process aborts.
    Crash { share: f64 },
    /// After `Crash`: reopen and catch up to the crash, then finish the
    /// backfill.
    Recover { share: f64 },
}

#[derive(Serialize, Deserialize)]
struct Case {
    side: Side,
    /// `Config::backfill_checkpoint_interval`; 0 on the Postgres side.
    interval: u64,
    run: usize,
    pg: String,
    batch_bytes: u64,
    tip_blocks: usize,
    tip: bool,
    mode: Mode,
    /// The Settle store of a crash and its recovery.
    data_dir: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PhaseResult {
    pub phase: Phase,
    pub side: Side,
    pub interval: u64,
    pub run: usize,
    pub blocks: usize,
    pub rows: usize,
    pub calls: usize,
    /// For a restart: from the start of the process to the crashed batch in
    /// Postgres again.
    pub wall_s: f64,
    /// CPU time of the app process: Settle and the Postgres client.
    pub cpu_s: Option<f64>,
    /// Time in the calls to Settle: ingest and ack.
    pub settle_s: Option<f64>,
    /// Time in the Postgres writer, client side.
    pub pg_busy_s: f64,
    /// The memory peak of the phase above the memory of the loaded fixture.
    pub mem_peak_mb: Option<f64>,
    /// Bytes that the app process wrote to disk: the Settle store.
    pub disk_write_mb: Option<f64>,
    pub pg_wal_mb: f64,
    /// Per call, from the input to the Postgres commit: p50, p99, max.
    pub latency_ms: Option<[f64; 3]>,
    /// Restart: the time to open the store (Settle replays in it).
    pub reopen_s: Option<f64>,
}

// ─── Parent ─────────────────────────────────────────────────────────────────

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> anyhow::Result<T> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v
            .parse()
            .map_err(|_| anyhow::anyhow!("{name}={v}: cannot parse")),
        _ => Ok(default),
    }
}

fn env_list(name: &str, default: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Runs a child for `case` and returns the results that it prints.
fn run_child(case: &Case, expect_crash: bool) -> anyhow::Result<Vec<PhaseResult>> {
    let mut child = Command::new(std::env::current_exe()?)
        .env("SCENARIO_CASE", serde_json::to_string(case)?)
        .stdout(Stdio::piped())
        .spawn()?;

    let mut results = Vec::new();
    for line in BufReader::new(child.stdout.take().expect("stdout")).lines() {
        if let Some(json) = line?.strip_prefix("RESULT ") {
            let result: PhaseResult = serde_json::from_str(json)?;
            eprintln!(
                "  run {} {:<18} {:<8} interval {:>4}: {:>7.2} s",
                case.run,
                case.side.label(),
                format!("{:?}", result.phase).to_lowercase(),
                case.interval,
                result.wall_s
            );
            results.push(result);
        }
    }

    let status = child.wait()?;
    let what = format!(
        "{:?} interval {} run {}",
        case.side, case.interval, case.run
    );
    if expect_crash {
        const SIGABRT: i32 = 6;
        anyhow::ensure!(
            status.signal() == Some(SIGABRT),
            "{what}: did not crash ({status})"
        );
    } else {
        anyhow::ensure!(status.success(), "{what}: failed");
    }
    Ok(results)
}

pub fn main<S: Scenario>() -> anyhow::Result<()> {
    if let Ok(case) = std::env::var("SCENARIO_CASE") {
        return child::<S>(&serde_json::from_str(&case)?);
    }

    let runs: usize = env_or("RUNS", 5)?;
    let batch_bytes: u64 = env_or("BATCH_BYTES", PIPES_MAX_BYTES)?;
    let tip_blocks: usize = env_or("TIP_BLOCKS", TIP_BLOCKS)?;
    let phases = env_list("PHASES", "backfill,tip,restart");
    for phase in &phases {
        anyhow::ensure!(
            ["backfill", "tip", "restart"].contains(&phase.as_str()),
            "PHASES: unknown phase {phase}"
        );
    }
    let has = |phase: &str| phases.iter().any(|p| p == phase);
    let intervals: Vec<u64> = env_list("INTERVALS", "1")
        .iter()
        .map(|v| v.parse())
        .collect::<Result<_, _>>()?;
    let mut sides = Vec::new();
    for side in env_list("SIDES", "pg,settle") {
        sides.push(match side.as_str() {
            "pg" => Side::Pg,
            "settle" => Side::Settle,
            other => anyhow::bail!("SIDES: unknown side {other}"),
        });
    }

    // The parent checks the state that each case leaves in Postgres, so that
    // the check does not count in the memory of the case.
    let fx = Fixture::load(S::NAME)?;
    let data = S::decode(&fx);
    anyhow::ensure!(
        tip_blocks < fx.blocks.len(),
        "TIP_BLOCKS is above the fixture"
    );
    let tip_start = fx.blocks.len() - tip_blocks;
    let check = |pg: &PgRuntime, end: usize, what: &str| -> anyhow::Result<()> {
        S::check(pg, &fx, &data, end).map_err(|e| anyhow::anyhow!("state check after {what}: {e}"))
    };

    let manifest = &fx.manifest;
    let (lag_min, lag_max) = Finality::new(&manifest.finality_samples).lag_range();
    eprintln!("scenario {}", S::NAME);
    eprintln!(
        "  fixture: {} blocks {}..={}, {} blocks with rows, {} rows, recorded {}",
        manifest.dataset,
        manifest.from_block,
        manifest.to_block,
        manifest.blocks,
        manifest.rows,
        manifest.recorded_at
    );
    eprintln!(
        "  finality lag {lag_min}..{lag_max} blocks ({} samples); batches of {} MiB of JSON; tip: last {tip_blocks} blocks",
        manifest.finality_samples.len(),
        batch_bytes / (1024 * 1024)
    );
    if let Ok(load) = std::fs::read_to_string("/proc/loadavg") {
        eprintln!("  load average: {}", load.trim());
    }

    let mut results = Vec::new();
    for run in 1..=runs {
        for &side in &sides {
            let side_intervals = if side == Side::Pg {
                vec![0]
            } else {
                intervals.clone()
            };
            for interval in side_intervals {
                let case = |pg: &PgRuntime, mode, data_dir| Case {
                    side,
                    interval,
                    run,
                    pg: pg.conn_str().to_string(),
                    batch_bytes,
                    tip_blocks,
                    tip: has("tip"),
                    mode,
                    data_dir,
                };

                if has("backfill") || has("tip") {
                    let pg = PgRuntime::start()?;
                    results.extend(run_child(&case(&pg, Mode::Full, None), false)?);
                    let end = if has("tip") {
                        fx.blocks.len()
                    } else {
                        tip_start
                    };
                    check(&pg, end, "the backfill and the tip")?;
                }

                if has("restart") {
                    let pg = PgRuntime::start()?;
                    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR"))?;
                    let dir_path = Some(dir.path().to_str().unwrap().to_string());
                    let share = CRASH_POINTS[(run - 1) % CRASH_POINTS.len()];
                    run_child(&case(&pg, Mode::Crash { share }, dir_path.clone()), true)?;
                    results.extend(run_child(
                        &case(&pg, Mode::Recover { share }, dir_path),
                        false,
                    )?);
                    check(&pg, tip_start, "the recovery")?;
                }
            }
        }
    }

    let report = serde_json::json!({
        "scenario": S::NAME,
        "fixture": {
            "dataset": &manifest.dataset,
            "from_block": manifest.from_block,
            "to_block": manifest.to_block,
            "blocks": manifest.blocks,
            "rows": manifest.rows,
            "rows_sha256": &manifest.rows_sha256,
        },
        "batch_bytes": batch_bytes,
        "tip_blocks": tip_blocks,
        "results": results,
    });
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("scenarios");
    std::fs::create_dir_all(&dir)?;
    let secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let path = dir.join(format!("{}-{secs}.json", S::NAME));
    std::fs::write(&path, serde_json::to_vec_pretty(&report)?)?;
    eprintln!("  report: {}", path.display());

    print_summary(&results);
    Ok(())
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn median_of(results: &[&PhaseResult], f: impl Fn(&PhaseResult) -> Option<f64>) -> String {
    let mut values: Vec<f64> = results.iter().filter_map(|r| f(r)).collect();
    if values.is_empty() {
        return "–".to_string();
    }
    let m = median(&mut values);
    if m >= 100.0 {
        format!("{m:.0}")
    } else {
        format!("{m:.2}")
    }
}

fn print_summary(results: &[PhaseResult]) {
    let mut groups: BTreeMap<(Phase, Side, u64), Vec<&PhaseResult>> = BTreeMap::new();
    for r in results {
        groups
            .entry((r.phase, r.side, r.interval))
            .or_default()
            .push(r);
    }

    println!();
    println!(
        "| Phase | Side | Interval | Wall s (min–max) | Blocks/s | Rows/s | CPU s | Settle s | PG busy s | Memory MB | Disk MB | PG WAL MB | p50 ms | p99 ms | Reopen s |"
    );
    println!("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|");
    for ((phase, side, interval), runs) in groups {
        let mut walls: Vec<f64> = runs.iter().map(|r| r.wall_s).collect();
        let wall = median(&mut walls);
        let (min, max) = (walls[0], walls[walls.len() - 1]);
        // A restart catches up a different number of blocks in each run.
        let rate = |count: fn(&PhaseResult) -> usize| match phase {
            Phase::Restart => "–".to_string(),
            _ => format!("{:.0}", count(runs[0]) as f64 / wall),
        };
        let interval = if side == Side::Pg {
            "–".to_string()
        } else {
            interval.to_string()
        };
        println!(
            "| {} | {} | {interval} | {wall:.2} ({min:.2}–{max:.2}) | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            format!("{phase:?}").to_lowercase(),
            side.label(),
            rate(|r| r.blocks),
            rate(|r| r.rows),
            median_of(&runs, |r| r.cpu_s),
            median_of(&runs, |r| r.settle_s),
            median_of(&runs, |r| Some(r.pg_busy_s)),
            median_of(&runs, |r| r.mem_peak_mb),
            median_of(&runs, |r| r.disk_write_mb),
            median_of(&runs, |r| Some(r.pg_wal_mb)),
            median_of(&runs, |r| r.latency_ms.map(|l| l[0])),
            median_of(&runs, |r| r.latency_ms.map(|l| l[1])),
            median_of(&runs, |r| r.reopen_s),
        );
    }
}

// ─── Child: one case ────────────────────────────────────────────────────────

struct Ctx<'a, S: Scenario> {
    pg: &'a PgRuntime,
    fx: &'a Fixture,
    data: &'a S::Data,
    case: &'a Case,
    finality: Finality,
    /// The head when the tip phase starts: the last block of the backfill.
    tip_head: u64,
    base_memory: Option<u64>,
}

impl<S: Scenario> Ctx<'_, S> {
    /// The finalized head, as a fixture block, when the chain head is `head`.
    fn finalized(&self, head: u64, floor: u64) -> &Block {
        let number = self.finality.at(self.tip_head, head).max(floor);
        &self.fx.blocks[self.fx.block_at_or_below(number)]
    }

    fn rows(&self, blocks: &Range<usize>) -> usize {
        self.fx.blocks[blocks.clone()]
            .iter()
            .map(|b| b.rows.len())
            .sum()
    }

    fn batches(&self, blocks: Range<usize>) -> Vec<Range<usize>> {
        self.fx.batches(blocks, self.case.batch_bytes)
    }

    /// The index of the first block above `number`.
    fn after(&self, number: Option<u64>) -> usize {
        number.map_or(0, |n| self.fx.blocks.partition_point(|b| b.number <= n))
    }

    fn settle_config(&self, dir: &str, applied_sequence: u64) -> Config {
        Config::with_data_dir(S::SETTLE_SCHEMA, dir)
            .backfill_checkpoint_interval(self.case.interval)
            .max_pending_batches(PENDING_BATCHES)
            .applied_sequence(applied_sequence)
    }
}

fn cursor(block: &Block) -> BlockCursor {
    BlockCursor {
        number: block.number,
        hash: block.hash.clone(),
    }
}

fn wal_lsn(pg: &PgRuntime) -> anyhow::Result<String> {
    let row = pg.rt.block_on(
        pg.client
            .query_one("SELECT pg_current_wal_lsn()::text", &[]),
    )?;
    Ok(row.get(0))
}

fn wal_since(pg: &PgRuntime, lsn: &str) -> anyhow::Result<f64> {
    let row = pg.rt.block_on(pg.client.query_one(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::text::pg_lsn)::float8",
        &[&lsn],
    ))?;
    Ok(row.get::<_, f64>(0) / 1e6)
}

fn read_i64(pg: &PgRuntime, sql: &str) -> anyhow::Result<i64> {
    Ok(pg.rt.block_on(pg.client.query_one(sql, &[]))?.get(0))
}

/// The counters of a phase.
struct Meter {
    started: Instant,
    cpu: Option<Duration>,
    written: Option<u64>,
    lsn: String,
}

/// What a phase did, besides the counters of its `Meter`.
struct Work {
    blocks: Range<usize>,
    calls: usize,
    settle: Option<Duration>,
    pg_busy: Duration,
    latencies: Option<Vec<f64>>,
    reopen: Option<Duration>,
}

impl Meter {
    fn start(pg: &PgRuntime) -> anyhow::Result<Self> {
        metrics::reset_peak_memory();
        Ok(Self {
            lsn: wal_lsn(pg)?,
            cpu: metrics::cpu_time(),
            written: metrics::write_bytes(),
            started: Instant::now(),
        })
    }

    fn finish<S: Scenario>(
        self,
        ctx: &Ctx<S>,
        phase: Phase,
        work: Work,
    ) -> anyhow::Result<PhaseResult> {
        let wall = self.started.elapsed();
        let cpu = metrics::cpu_time()
            .zip(self.cpu)
            .map(|(end, start)| (end - start).as_secs_f64());
        let written = metrics::write_bytes()
            .zip(self.written)
            .map(|(end, start)| (end - start) as f64 / 1e6);
        let memory = metrics::memory("VmHWM")
            .zip(ctx.base_memory)
            .map(|(peak, base)| peak.saturating_sub(base) as f64 / 1e6);
        let latency_ms = work.latencies.map(|mut l| {
            l.sort_by(f64::total_cmp);
            [
                metrics::percentile(&l, 50.0),
                metrics::percentile(&l, 99.0),
                l[l.len() - 1],
            ]
        });
        Ok(PhaseResult {
            phase,
            side: ctx.case.side,
            interval: ctx.case.interval,
            run: ctx.case.run,
            blocks: work.blocks.len(),
            rows: ctx.rows(&work.blocks),
            calls: work.calls,
            wall_s: wall.as_secs_f64(),
            cpu_s: cpu,
            settle_s: work.settle.map(|d| d.as_secs_f64()),
            pg_busy_s: work.pg_busy.as_secs_f64(),
            mem_peak_mb: memory,
            disk_write_mb: written,
            pg_wal_mb: wal_since(ctx.pg, &self.lsn)?,
            latency_ms,
            reopen_s: work.reopen.map(|d| d.as_secs_f64()),
        })
    }
}

fn child<S: Scenario>(case: &Case) -> anyhow::Result<()> {
    let fx = Fixture::load(S::NAME)?;
    let data = S::decode(&fx);
    let pg = PgRuntime::connect(&case.pg)?;
    if !matches!(case.mode, Mode::Recover { .. }) {
        pg.batch_execute(S::PG_SCHEMA)?;
        pg.batch_execute(APP_TABLES)?;
    }

    anyhow::ensure!(
        case.tip_blocks < fx.blocks.len(),
        "TIP_BLOCKS is above the fixture"
    );
    let tip_start = fx.blocks.len() - case.tip_blocks;
    let ctx = Ctx::<S> {
        pg: &pg,
        fx: &fx,
        data: &data,
        case,
        finality: Finality::new(&fx.manifest.finality_samples),
        tip_head: fx.blocks[tip_start - 1].number,
        base_memory: metrics::memory("VmRSS"),
    };
    let backfill = 0..tip_start;
    let tip = tip_start..fx.blocks.len();

    let results = match case.mode {
        Mode::Full => full(&ctx, backfill, tip)?,
        Mode::Crash { share } => {
            let at = crash_batch(&ctx, &backfill, share);
            match case.side {
                Side::Pg => {
                    write_pg(&ctx, backfill, Some(at))?;
                }
                Side::Settle => {
                    let dir = case.data_dir.as_deref().expect("data dir");
                    let mut db = Settle::open(ctx.settle_config(dir, 0))?;
                    write_settle(&ctx, &mut db, backfill, Some(at))?;
                }
            }
            anyhow::bail!("the backfill ended before the crash point");
        }
        Mode::Recover { share } => {
            let crashed = ctx.batches(backfill.clone())[crash_batch(&ctx, &backfill, share)].end;
            vec![recover(&ctx, crashed, backfill.end)?]
        }
    };

    for result in results {
        println!("RESULT {}", serde_json::to_string(&result)?);
    }
    Ok(())
}

fn crash_batch<S: Scenario>(ctx: &Ctx<S>, blocks: &Range<usize>, share: f64) -> usize {
    let batches = ctx.batches(blocks.clone()).len();
    ((share * batches as f64) as usize).min(batches - 1)
}

fn full<S: Scenario>(
    ctx: &Ctx<S>,
    backfill: Range<usize>,
    tip: Range<usize>,
) -> anyhow::Result<Vec<PhaseResult>> {
    let pg = ctx.pg;
    let mut results = Vec::new();
    match ctx.case.side {
        Side::Pg => {
            let meter = Meter::start(pg)?;
            let calls = write_pg(ctx, backfill.clone(), None)?;
            let pg_busy = meter.started.elapsed();
            let work = Work {
                blocks: backfill.clone(),
                calls,
                settle: None,
                pg_busy,
                latencies: None,
                reopen: None,
            };
            results.push(meter.finish(ctx, Phase::Backfill, work)?);
            if ctx.case.tip {
                results.push(tip_pg(ctx, tip)?);
            }
        }
        Side::Settle => {
            let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR"))?;
            let mut db = Settle::open(ctx.settle_config(dir.path().to_str().unwrap(), 0))?;

            let meter = Meter::start(pg)?;
            let (calls, settle, pg_busy) = write_settle(ctx, &mut db, backfill.clone(), None)?;
            let work = Work {
                blocks: backfill.clone(),
                calls,
                settle: Some(settle),
                pg_busy,
                latencies: None,
                reopen: None,
            };
            results.push(meter.finish(ctx, Phase::Backfill, work)?);
            if ctx.case.tip {
                results.push(tip_settle(ctx, &mut db, tip)?);
            }
        }
    }
    Ok(results)
}

/// Reopens after a crash in the batch that ends before block `crashed`,
/// catches up to it (the measured part), then finishes the backfill up to
/// `end`.
fn recover<S: Scenario>(ctx: &Ctx<S>, crashed: usize, end: usize) -> anyhow::Result<PhaseResult> {
    let pg = ctx.pg;
    let meter = Meter::start(pg)?;

    let result = match ctx.case.side {
        Side::Pg => {
            let number = read_i64(pg, "SELECT number FROM app_cursor WHERE id = 1")?;
            let resume = ctx.after((number > 0).then_some(number as u64));
            let reopen = meter.started.elapsed();

            let calls = write_pg(ctx, resume..crashed, None)?;
            let pg_busy = meter.started.elapsed() - reopen;
            let work = Work {
                blocks: resume..crashed,
                calls,
                settle: None,
                pg_busy,
                latencies: None,
                reopen: Some(reopen),
            };
            let result = meter.finish(ctx, Phase::Restart, work)?;
            write_pg(ctx, crashed..end, None)?;
            result
        }
        Side::Settle => {
            let applied = read_i64(pg, "SELECT sequence FROM settle_sequence WHERE id = 1")?;
            let dir = ctx.case.data_dir.as_deref().expect("data dir");
            let mut db = Settle::open(ctx.settle_config(dir, applied as u64))?;
            let resume = ctx.after(db.latest_cursor().map(|c| c.number));
            let reopen = meter.started.elapsed();

            let (calls, settle, pg_busy) = write_settle(ctx, &mut db, resume..crashed, None)?;
            let work = Work {
                blocks: resume..crashed,
                calls,
                settle: Some(reopen + settle),
                pg_busy,
                latencies: None,
                reopen: Some(reopen),
            };
            let result = meter.finish(ctx, Phase::Restart, work)?;
            write_settle(ctx, &mut db, crashed..end, None)?;
            result
        }
    };

    Ok(result)
}

fn save_cursor(pg: &PgRuntime, block: &Block) -> anyhow::Result<()> {
    pg.execute(
        "UPDATE app_cursor SET number = $1, hash = $2 WHERE id = 1",
        &[&(block.number as i64), &block.hash],
    )?;
    Ok(())
}

/// The Postgres-only backfill: one transaction per batch. With `crash_at`,
/// the process aborts before the commit of that batch.
fn write_pg<S: Scenario>(
    ctx: &Ctx<S>,
    blocks: Range<usize>,
    crash_at: Option<usize>,
) -> anyhow::Result<usize> {
    let (pg, fx) = (ctx.pg, ctx.fx);
    let batches = ctx.batches(blocks);
    for (i, batch) in batches.iter().enumerate() {
        pg.begin()?;
        S::pg_only_write(pg, fx, ctx.data, batch.clone())?;
        save_cursor(pg, &fx.blocks[batch.end - 1])?;
        if crash_at == Some(i) {
            std::process::abort();
        }
        pg.commit()?;
    }
    Ok(batches.len())
}

/// At the tip the app keeps an undo log (Ponder's), set up after the
/// backfill, as Ponder does. Each block is one call.
fn tip_pg<S: Scenario>(ctx: &Ctx<S>, blocks: Range<usize>) -> anyhow::Result<PhaseResult> {
    let (pg, fx) = (ctx.pg, ctx.fx);
    let forks = PgForks::Undo.setup(pg, S::PG_TABLES)?;

    let mut finalized = ctx.finalized(ctx.tip_head, 0).number;
    let mut latencies = Vec::with_capacity(blocks.len());
    let meter = Meter::start(pg)?;
    for i in blocks.clone() {
        let block = &fx.blocks[i];
        finalized = ctx.finalized(block.number, finalized).number;

        let started = Instant::now();
        pg.begin()?;
        S::pg_only_write(pg, fx, ctx.data, i..i + 1)?;
        save_cursor(pg, block)?;
        forks.record(pg, block.number, block.number, finalized)?;
        pg.commit()?;
        latencies.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    let work = Work {
        calls: blocks.len(),
        settle: None,
        pg_busy: meter.started.elapsed(),
        blocks,
        latencies: Some(latencies),
        reopen: None,
    };
    meter.finish(ctx, Phase::Tip, work)
}

fn apply<S: Scenario>(pg: &PgRuntime, batch: &ChangeBatch) -> anyhow::Result<()> {
    pg.begin()?;
    S::pg_apply(pg, batch)?;
    pg.execute(
        "UPDATE settle_sequence SET sequence = $1 WHERE id = 1",
        &[&(batch.sequence as i64)],
    )?;
    pg.commit()?;
    Ok(())
}

/// Also returns the time of the Settle call.
fn ingest<S: Scenario>(
    ctx: &Ctx<S>,
    db: &mut Settle,
    blocks: Range<usize>,
    finalized: &Block,
) -> anyhow::Result<(Option<ChangeBatch>, Duration)> {
    let rollback_chain = ctx.fx.blocks[blocks.clone()]
        .iter()
        .filter(|b| b.number > finalized.number)
        .map(cursor)
        .collect();
    let input = IngestRows {
        data: S::settle_rows(db, ctx.fx, ctx.data, blocks),
        rollback_chain,
        finalized_head: cursor(finalized),
    };
    let started = Instant::now();
    let batch = db.ingest_rows(input)?;
    Ok((batch, started.elapsed()))
}

fn ack(db: &mut Settle, sequence: u64, settle: &mut Duration) -> anyhow::Result<()> {
    let started = Instant::now();
    db.ack(sequence)?;
    *settle += started.elapsed();
    Ok(())
}

/// The Settle backfill: Settle ingests the next batch while another thread
/// applies the batch before to Postgres (`max_pending_batches`). With
/// `crash_at`, the process aborts once that batch is handed to the writer.
/// Returns the calls, the time in Settle and the busy time of the writer.
fn write_settle<S: Scenario>(
    ctx: &Ctx<S>,
    db: &mut Settle,
    blocks: Range<usize>,
    crash_at: Option<usize>,
) -> anyhow::Result<(usize, Duration, Duration)> {
    let pg = ctx.pg;
    let batches = ctx.batches(blocks);
    let finalized = ctx.finalized(ctx.tip_head, 0);

    let mut settle = Duration::ZERO;
    let pg_busy = std::thread::scope(|scope| -> anyhow::Result<Duration> {
        let (batch_tx, batch_rx) = mpsc::channel::<ChangeBatch>();
        let (applied_tx, applied_rx) = mpsc::channel::<anyhow::Result<u64>>();
        let writer = scope.spawn(move || {
            let mut busy = Duration::ZERO;
            for batch in batch_rx {
                let started = Instant::now();
                let applied = apply::<S>(pg, &batch).map(|()| batch.sequence);
                busy += started.elapsed();
                if applied_tx.send(applied).is_err() {
                    break;
                }
            }
            busy
        });

        for (i, batch) in batches.iter().enumerate() {
            while let Ok(applied) = applied_rx.try_recv() {
                ack(db, applied?, &mut settle)?;
            }
            while db.unacked_batches() >= PENDING_BATCHES {
                ack(db, applied_rx.recv()??, &mut settle)?;
            }
            let (changes, took) = ingest(ctx, db, batch.clone(), finalized)?;
            settle += took;
            if let Some(changes) = changes {
                batch_tx
                    .send(changes)
                    .map_err(|_| anyhow::anyhow!("the Postgres writer stopped"))?;
            }
            if crash_at == Some(i) {
                std::process::abort();
            }
        }
        drop(batch_tx);
        while db.is_awaiting_ack() {
            ack(db, applied_rx.recv()??, &mut settle)?;
        }
        Ok(writer.join().expect("Postgres writer"))
    })?;
    Ok((batches.len(), settle, pg_busy))
}

/// Each block is one call, applied and acked before the next block.
fn tip_settle<S: Scenario>(
    ctx: &Ctx<S>,
    db: &mut Settle,
    blocks: Range<usize>,
) -> anyhow::Result<PhaseResult> {
    let (pg, fx) = (ctx.pg, ctx.fx);

    let mut finalized = ctx.finalized(ctx.tip_head, 0).number;
    let mut latencies = Vec::with_capacity(blocks.len());
    let mut pg_busy = Duration::ZERO;
    let mut settle = Duration::ZERO;
    let meter = Meter::start(pg)?;
    for i in blocks.clone() {
        let final_block = ctx.finalized(fx.blocks[i].number, finalized);
        finalized = final_block.number;

        let started = Instant::now();
        let (batch, took) = ingest(ctx, db, i..i + 1, final_block)?;
        settle += took;
        if let Some(batch) = batch {
            let applying = Instant::now();
            apply::<S>(pg, &batch)?;
            pg_busy += applying.elapsed();
            ack(db, batch.sequence, &mut settle)?;
        }
        latencies.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    let work = Work {
        calls: blocks.len(),
        blocks,
        settle: Some(settle),
        pg_busy,
        latencies: Some(latencies),
        reopen: None,
    };
    meter.finish(ctx, Phase::Tip, work)
}
