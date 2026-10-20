//! Solana DEX PnL: the swaps of ten DEX programs through three reducers and
//! five views, once with each reducer runtime that a Rust host has. The
//! target is a store that applies every change batch at once, so the
//! numbers are the cost of Settle alone.
//!
//! - `prices` (one group): the USD price of each swap, where USD means
//!   USDC, through a chain of pools from the swaps themselves (X/BONK,
//!   BONK/SOL, SOL/USDC). The last price of each token is an entry of a Map
//!   field. The other reducers take their rows from it.
//! - `pool_prices` (GROUP BY pool): feeds 1-minute OHLC candles of each
//!   token, hourly volume of each DEX, pool stats.
//! - `positions` (GROUP BY trader, token): average-cost PnL, feeds the PnL
//!   of each wallet and its open positions, about a million groups.
//!   `SDEX_POSITIONS=json`: GROUP BY trader, the positions of the wallet in
//!   a JSON map, as tests/conformance/ct10_golden/uniswap.sql kept them. `map`: the same in a
//!   Map field.
//!
//! Runtimes: `fn` (Rust closures, LANGUAGE EXTERNAL), `lua`. Modes:
//! - `backfill`: 100 slots in each ingest, finality at the batch tip.
//! - `tip`: the first 80% of the slots as a backfill (not timed), then one
//!   slot in each ingest with finality 32 blocks back.
//!
//! Run: `cargo bench --bench solana_dex`
//! Env: `SDEX_SLOTS` (12000, ~3M swaps), `SDEX_RUNTIMES` (fn,lua),
//! `SDEX_MODES` (backfill,tip), `SDEX_STORAGE` (rocks | memory),
//! `SDEX_BATCH_SLOTS` (100), `SDEX_POSITIONS` (pair | json | map),
//! `SDEX_CHECKPOINT` (1: a list of backfill checkpoint intervals, in blocks),
//! `RAYON_NUM_THREADS`.
//! `SDEX_WRITE=<path>` writes the swaps for the Node bench and exits.

#[path = "solana_dex/dataset.rs"]
mod dataset;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dataset::{Dataset, USDC};
use settle::db::{Config, IngestRows, Settle};
use settle::reducer_runtime::fn_reducer::{FnReducerRuntime, StateCtx};
use settle::test_helpers::cursor;
use settle::types::{ChangeBatch, ChangeOp, ColumnRegistry, PerfNode, PerfNodeKind, Row, Value};

const TABLES: &str = include_str!("solana_dex/tables.sql");
const VIEWS: &str = include_str!("solana_dex/views.sql");
/// `prices`, `pool_prices`, then `positions` in the order of `Layout`.
const LUA: [&str; 5] = [
    include_str!("solana_dex/lua/prices.sql"),
    include_str!("solana_dex/lua/pool_prices.sql"),
    include_str!("solana_dex/lua/positions.sql"),
    include_str!("solana_dex/lua/positions_json.sql"),
    include_str!("solana_dex/lua/positions_map.sql"),
];
const EXTERNAL: [&str; 5] = [
    include_str!("solana_dex/external/prices.sql"),
    include_str!("solana_dex/external/pool_prices.sql"),
    include_str!("solana_dex/external/positions.sql"),
    include_str!("solana_dex/external/positions_json.sql"),
    include_str!("solana_dex/external/positions_map.sql"),
];

const SEED: u64 = 0x50_1A4A;
const SLOT_SECONDS: f64 = 0.4;
const CONFIRMATION_DEPTH: usize = 32;
const TIP_SHARE: f64 = 0.2;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_list(name: &str, default: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

// ─── Rust closures: the logic of the Lua reducers ─────────────────────────

fn f64_of(row: &Row, column: &str) -> f64 {
    row.get(column).and_then(Value::as_f64).unwrap_or(0.0)
}

fn str_of<'a>(row: &'a Row, column: &str) -> &'a str {
    row.get(column).and_then(Value::as_str).unwrap_or("")
}

/// Emit `fields` by output position, as a Rust reducer that cares about
/// speed does: no map with a string key for each column.
fn emit<const N: usize>(state: &mut StateCtx<'_>, fields: [(&str, Value); N]) {
    let mut values = Vec::with_capacity(N + 2);
    for (name, value) in fields {
        let id = state.column(name).expect("emit columns within the limit") as usize;
        if values.len() <= id {
            values.resize(id + 1, Value::Null);
        }
        values[id] = value;
    }
    state.emit_values(values);
}

/// `lua/prices.sql`.
fn prices_fn() -> FnReducerRuntime {
    FnReducerRuntime::new(|state, row| {
        let quote = str_of(row, "quote");
        let token = str_of(row, "token");
        let base = f64_of(row, "base_amount");
        let quote_amount = f64_of(row, "quote_amount");

        let mut usdc = state.map("usdc").expect("the usdc Map field");
        let quote_usd = match quote {
            USDC => Some(1.0),
            _ => usdc.get(quote).and_then(Value::as_f64),
        };
        let price = match quote_usd {
            Some(quote_usd) => {
                let price = quote_amount * quote_usd / base;
                usdc.set(token, Value::Float64(price));
                price
            }
            None => {
                let Some(price) = usdc.get(token).and_then(Value::as_f64) else {
                    return;
                };
                usdc.set(quote, Value::Float64(base * price / quote_amount));
                price
            }
        };

        let column = |name: &str| row.get(name).cloned().unwrap_or(Value::Null);
        emit(
            state,
            [
                ("dex", column("dex")),
                ("pool", column("pool")),
                ("trader", column("trader")),
                ("token", column("token")),
                ("side", column("side")),
                ("base_amount", column("base_amount")),
                ("timestamp", column("timestamp")),
                ("price_usd", Value::Float64(price)),
                ("usd", Value::Float64(base * price)),
            ],
        );
    })
}

fn pool_prices_fn() -> FnReducerRuntime {
    FnReducerRuntime::new(|state, row| {
        let price = f64_of(row, "price_usd");
        let usd = f64_of(row, "usd");

        let last = state.get_f64("price_usd");
        let change = if last > 0.0 { price / last - 1.0 } else { 0.0 };

        state.set("price_usd", Value::Float64(price));
        state.set(
            "volume_usd",
            Value::Float64(state.get_f64("volume_usd") + usd),
        );
        state.set("swaps", Value::UInt64(state.get_u64("swaps") + 1));

        let column = |name: &str| row.get(name).cloned().unwrap_or(Value::Null);
        emit(
            state,
            [
                ("token", column("token")),
                ("dex", column("dex")),
                ("timestamp", column("timestamp")),
                ("price_usd", Value::Float64(price)),
                ("usd", Value::Float64(usd)),
                ("price_change", Value::Float64(change)),
            ],
        );
    })
}

/// The position of a wallet in a token: the state of `positions`, or an
/// entry of its book.
#[derive(Default)]
struct Position {
    qty: f64,
    cost_usd: f64,
    realized_usd: f64,
    buys: u64,
    sells: u64,
}

impl Position {
    fn from_json(entry: Option<&serde_json::Value>) -> Self {
        let Some(entry) = entry else {
            return Self::default();
        };
        let field = |name: &str| entry[name].as_f64().unwrap_or(0.0);
        Self {
            qty: field("qty"),
            cost_usd: field("cost_usd"),
            realized_usd: field("realized_usd"),
            buys: field("buys") as u64,
            sells: field("sells") as u64,
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "qty": self.qty,
            "cost_usd": self.cost_usd,
            "realized_usd": self.realized_usd,
            "buys": self.buys,
            "sells": self.sells,
        })
    }

    /// Apply the swap `row`; returns the PnL that it realized.
    fn trade(&mut self, row: &Row) -> f64 {
        let base = f64_of(row, "base_amount");
        if str_of(row, "side") == "buy" {
            self.qty += base;
            self.cost_usd += f64_of(row, "usd");
            self.buys += 1;
            return 0.0;
        }

        let avg = if self.qty > 0.0 { self.cost_usd / self.qty } else { 0.0 };
        let matched = base.min(self.qty);
        let realized = matched * f64_of(row, "price_usd") - matched * avg;
        self.qty -= matched;
        self.cost_usd -= matched * avg;
        self.realized_usd += realized;
        self.sells += 1;
        realized
    }

    /// The emit of `positions` for the swap `row`.
    fn emit(&self, state: &mut StateCtx<'_>, row: &Row, realized: f64) {
        let fields = [
            ("token", Value::String(str_of(row, "token").to_string())),
            ("realized", Value::Float64(realized)),
            ("usd", Value::Float64(f64_of(row, "usd"))),
            ("price_usd", Value::Float64(f64_of(row, "price_usd"))),
            ("position", Value::Float64(self.qty)),
            ("cost_usd", Value::Float64(self.cost_usd)),
            ("win", Value::Int64((realized > 0.0) as i64)),
            ("loss", Value::Int64((realized < 0.0) as i64)),
        ];
        emit(state, fields);
    }
}

fn positions_fn() -> FnReducerRuntime {
    FnReducerRuntime::new(|state, row| {
        let mut position = Position {
            qty: state.get_f64("qty"),
            cost_usd: state.get_f64("cost_usd"),
            realized_usd: state.get_f64("realized_usd"),
            buys: state.get_u64("buys"),
            sells: state.get_u64("sells"),
        };
        let realized = position.trade(row);

        state.set("qty", Value::Float64(position.qty));
        state.set("cost_usd", Value::Float64(position.cost_usd));
        state.set("realized_usd", Value::Float64(position.realized_usd));
        state.set("buys", Value::UInt64(position.buys));
        state.set("sells", Value::UInt64(position.sells));
        position.emit(state, row, realized);
    })
}

/// `lua/positions_json.sql`: the positions of a wallet in one JSON map,
/// changed in place.
fn positions_json_fn() -> FnReducerRuntime {
    FnReducerRuntime::new(|state, row| {
        let token = str_of(row, "token");

        let book = &mut state.values()[0];
        if !matches!(book, Value::JSON(serde_json::Value::Object(_))) {
            *book = Value::JSON(serde_json::Value::Object(Default::default()));
        }
        let Value::JSON(serde_json::Value::Object(book)) = book else {
            unreachable!()
        };
        let mut position = Position::from_json(book.get(token));
        let realized = position.trade(row);
        book.insert(token.to_string(), position.to_json());

        position.emit(state, row, realized);
    })
}

/// `lua/positions_map.sql`: the positions of a wallet in a Map field.
fn positions_map_fn() -> FnReducerRuntime {
    FnReducerRuntime::new(|state, row| {
        let token = str_of(row, "token");

        let mut book = state.map("book").expect("the book Map field");
        let entry = match book.get(token) {
            Some(Value::JSON(entry)) => Some(entry),
            _ => None,
        };
        let mut position = Position::from_json(entry);
        let realized = position.trade(row);
        book.set(token, Value::JSON(position.to_json()));

        position.emit(state, row, realized);
    })
}

/// How `positions` keeps the positions of a wallet.
#[derive(Clone, Copy, PartialEq)]
enum Layout {
    /// A group for each wallet and token.
    Pair,
    /// A group for each wallet, its positions in a JSON map.
    Json,
    /// A group for each wallet, its positions in a Map field.
    Map,
}

impl Layout {
    fn parse(name: &str) -> Self {
        match name {
            "pair" => Self::Pair,
            "json" => Self::Json,
            "map" => Self::Map,
            other => panic!("unknown positions layout {other}"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Pair => "pair",
            Self::Json => "json",
            Self::Map => "map",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

fn schema(runtime: &str, layout: Layout) -> String {
    let sql = match runtime {
        "fn" => &EXTERNAL,
        "lua" => &LUA,
        other => panic!("no {other} reducers"),
    };
    let [prices, pool_prices, positions @ ..] = sql;
    [TABLES, prices, pool_prices, positions[layout.index()], VIEWS].join("\n")
}

// ─── Input rows ────────────────────────────────────────────────────────────

/// The rows of each block, in the column order of `registry`.
fn build_rows(data: &Dataset, registry: &Arc<ColumnRegistry>) -> Vec<(u64, Vec<Row>)> {
    let names = registry.names().to_vec();
    let string = |id: u32| Value::String(data.strings[id as usize].clone());

    data.blocks
        .iter()
        .map(|block| {
            let rows = data.swaps[block.swaps.clone()]
                .iter()
                .map(|swap| {
                    let values = names
                        .iter()
                        .map(|name| match name.as_str() {
                            "block_number" => Value::UInt64(block.slot),
                            "timestamp" => Value::UInt64(block.timestamp_ms),
                            "dex" => string(swap.dex),
                            "pool" => string(swap.pool),
                            "trader" => string(swap.trader),
                            "token" => string(swap.token),
                            "quote" => string(swap.quote),
                            "side" => Value::String(if swap.buy { "buy" } else { "sell" }.into()),
                            "base_amount" => Value::Float64(swap.base_amount),
                            "quote_amount" => Value::Float64(swap.quote_amount),
                            other => panic!("unexpected column {other}"),
                        })
                        .collect();
                    Row::from_values(registry.clone(), values)
                })
                .collect();
            (block.slot, rows)
        })
        .collect()
}

// ─── Target and measurements ───────────────────────────────────────────────

/// A target that applies each batch at once. It keeps three totals that do
/// not depend on the runtime, to check that each runtime did the same work.
#[derive(Default)]
struct Target {
    records: u64,
    realized_pnl: f64,
    volume_usd: f64,
    candle_trades: f64,
}

impl Target {
    fn apply(&mut self, batch: &ChangeBatch) {
        for (table, records) in &batch.tables {
            self.records += records.len() as u64;
            let (column, total) = match table.as_str() {
                "wallet_pnl" => ("realized_pnl", &mut self.realized_pnl),
                "dex_volume_1h" => ("volume_usd", &mut self.volume_usd),
                "token_candles_1m" => ("trades", &mut self.candle_trades),
                _ => continue,
            };
            for record in records {
                let value = |v: Option<&Value>| v.and_then(Value::as_f64).unwrap_or(0.0);
                *total += match record.operation {
                    ChangeOp::Insert => value(record.get(column)),
                    ChangeOp::Update => value(record.get(column)) - value(record.prev(column)),
                    ChangeOp::Delete => -value(record.get(column)),
                };
            }
        }
    }
}

/// Where the time of the ingests went, from the perf tree of each batch.
#[derive(Default)]
struct Split {
    dag_ms: f64,
    raw_ms: f64,
    /// Reducer name → (its own time, the time of its views).
    reducers_ms: BTreeMap<String, (f64, f64)>,
    parallel: bool,
}

impl Split {
    fn add(&mut self, nodes: &[PerfNode]) {
        for node in nodes {
            if node.kind == PerfNodeKind::Pipeline && node.name == "ingest" {
                self.dag_ms += node.duration_ms;
                self.walk(&node.children, false);
            }
        }
    }

    /// A reducer node holds its views as children; in a parallel branch its
    /// time covers them too.
    fn walk(&mut self, nodes: &[PerfNode], in_parallel: bool) {
        for node in nodes {
            match node.kind {
                PerfNodeKind::RawTable => self.raw_ms += node.duration_ms,
                PerfNodeKind::Reducer => {
                    let views: f64 = node.children.iter().map(|c| c.duration_ms).sum();
                    let own = if in_parallel {
                        node.duration_ms - views
                    } else {
                        node.duration_ms
                    };
                    let total = self.reducers_ms.entry(node.name.clone()).or_default();
                    total.0 += own;
                    total.1 += views;
                }
                PerfNodeKind::Parallel => {
                    self.parallel = true;
                    self.walk(&node.children, true);
                }
                PerfNodeKind::Pipeline => self.walk(&node.children, in_parallel),
                PerfNodeKind::MV => {}
            }
        }
    }
}

/// CPU time of this process (user + system), from /proc; None elsewhere.
fn cpu_time() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let ticks: u64 = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    Some(Duration::from_millis(ticks * 10))
}

/// Resident and peak resident memory in MB, from /proc; None elsewhere.
fn memory_mb() -> Option<(f64, f64)> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let field = |name: &str| -> Option<f64> {
        let line = status.lines().find(|l| l.starts_with(name))?;
        let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
        Some(kb / 1024.0)
    };
    Some((field("VmRSS:")?, field("VmHWM:")?))
}

/// Start a new peak of resident memory (Linux 4.0+).
fn reset_peak_memory() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

struct Outcome {
    label: String,
    swaps: usize,
    slots: f64,
    wall: Duration,
    cpu: Option<Duration>,
    ingest_ms: Vec<f64>,
    ack_ms: f64,
    split: Split,
    target: Target,
    /// Change records of the timed ingests.
    records: u64,
    rss_before: Option<f64>,
    rss_peak: Option<f64>,
    /// Swaps per second in each quarter of the timed ingests.
    quarters: Vec<f64>,
}

fn open(
    runtime: &str,
    layout: Layout,
    storage: &str,
    checkpoint: u64,
) -> (Settle, Option<tempfile::TempDir>) {
    let schema = schema(runtime, layout);
    let (config, dir) = match storage {
        "memory" => (Config::new(schema), None),
        "rocks" => {
            let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
            let config = Config::with_data_dir(schema, dir.path().to_str().unwrap());
            (config, Some(dir))
        }
        other => panic!("unknown storage {other}"),
    };
    let config = config.backfill_checkpoint_interval(checkpoint);
    let mut db = Settle::open(config).unwrap();
    if runtime == "fn" {
        let positions = match layout {
            Layout::Pair => positions_fn(),
            Layout::Json => positions_json_fn(),
            Layout::Map => positions_map_fn(),
        };
        let runtimes = [
            ("prices", prices_fn()),
            ("pool_prices", pool_prices_fn()),
            ("positions", positions),
        ];
        for (name, runtime) in runtimes {
            db.register_reducer_callback(name, Box::new(runtime)).unwrap();
        }
    }
    (db, dir)
}

/// One ingest of `blocks`, with finality at `finalized`, applied and acked.
/// Returns the time of the ingest and of the ack, in ms.
fn ingest(
    db: &mut Settle,
    blocks: Vec<(u64, Vec<Row>)>,
    finalized: u64,
    target: &mut Target,
    split: &mut Split,
) -> (f64, f64) {
    let rollback_chain = blocks
        .iter()
        .filter(|(slot, _)| *slot > finalized)
        .map(|(slot, _)| cursor(*slot))
        .collect();
    let by_block: BTreeMap<u64, Vec<Row>> = blocks.into_iter().collect();
    let input = IngestRows {
        data: HashMap::from([("swaps".to_string(), by_block)]),
        rollback_chain,
        finalized_head: cursor(finalized),
    };

    let started = Instant::now();
    let batch = db.ingest_rows(input).unwrap();
    let ingest_ms = started.elapsed().as_secs_f64() * 1000.0;

    let Some(batch) = batch else {
        return (ingest_ms, 0.0);
    };
    split.add(&batch.perf);
    target.apply(&batch);

    let started = Instant::now();
    db.ack(batch.sequence).unwrap();
    (ingest_ms, started.elapsed().as_secs_f64() * 1000.0)
}

fn run(
    data: &Dataset,
    runtime: &str,
    mode: &str,
    layout: Layout,
    storage: &str,
    batch_slots: usize,
    checkpoint: u64,
) -> Outcome {
    let (mut db, _dir) = open(runtime, layout, storage, checkpoint);
    let registry = db.table_registry("swaps").unwrap().clone();
    let mut blocks = build_rows(data, &registry);

    let mut target = Target::default();
    let mut split = Split::default();

    // Tip: the untimed backfill keeps finality 32 blocks back too, so the
    // timed part can go on from it.
    let timed_from = match mode {
        "backfill" => 0,
        "tip" => {
            let split_at = ((1.0 - TIP_SHARE) * blocks.len() as f64) as usize;
            let warm: Vec<_> = blocks.drain(..split_at).collect();
            let mut warm = warm.into_iter().peekable();
            let mut done = 0;
            let mut warm_split = Split::default();
            while warm.peek().is_some() {
                let chunk: Vec<_> = warm.by_ref().take(batch_slots).collect();
                done += chunk.len();
                let finalized = data.blocks[done.saturating_sub(CONFIRMATION_DEPTH + 1)].slot;
                ingest(&mut db, chunk, finalized, &mut target, &mut warm_split);
            }
            split_at
        }
        other => panic!("unknown mode {other}"),
    };

    let timed_swaps: usize = blocks.iter().map(|(_, rows)| rows.len()).sum();
    let swaps_per_ingest: Vec<usize>;
    let mut ingest_ms = Vec::new();
    let mut ack_ms = 0.0;

    let records_before = target.records;
    reset_peak_memory();
    let rss_before = memory_mb().map(|(rss, _)| rss);
    let cpu_before = cpu_time();
    let started = Instant::now();

    if mode == "backfill" {
        let mut sizes = Vec::new();
        let mut rest = blocks.into_iter().peekable();
        while rest.peek().is_some() {
            let chunk: Vec<_> = rest.by_ref().take(batch_slots).collect();
            let finalized = chunk.last().unwrap().0;
            sizes.push(chunk.iter().map(|(_, rows)| rows.len()).sum());
            let (i, a) = ingest(&mut db, chunk, finalized, &mut target, &mut split);
            ingest_ms.push(i + a);
            ack_ms += a;
        }
        swaps_per_ingest = sizes;
    } else {
        let mut sizes = Vec::new();
        for (offset, block) in blocks.into_iter().enumerate() {
            let index = timed_from + offset;
            let finalized = data.blocks[index - CONFIRMATION_DEPTH].slot;
            sizes.push(block.1.len());
            let (i, a) = ingest(&mut db, vec![block], finalized, &mut target, &mut split);
            ingest_ms.push(i + a);
            ack_ms += a;
        }
        swaps_per_ingest = sizes;
    }

    let wall = started.elapsed();
    let cpu = cpu_time()
        .zip(cpu_before)
        .map(|(after, before)| after - before);
    let rss_peak = memory_mb().map(|(_, peak)| peak);

    let quarter = ingest_ms.len().div_ceil(4);
    let quarters = ingest_ms
        .chunks(quarter)
        .zip(swaps_per_ingest.chunks(quarter))
        .map(|(ms, swaps)| swaps.iter().sum::<usize>() as f64 / (ms.iter().sum::<f64>() / 1000.0))
        .collect();

    let timed_blocks = &data.blocks[timed_from..];
    let slots = (timed_blocks.last().unwrap().slot - timed_blocks[0].slot + 1) as f64;

    Outcome {
        label: match checkpoint {
            1 => format!("{runtime},{mode},{}", layout.name()),
            n => format!("{runtime},{mode},{},ckpt{n}", layout.name()),
        },
        swaps: timed_swaps,
        slots,
        wall,
        cpu,
        ingest_ms,
        ack_ms,
        split,
        records: target.records - records_before,
        target,
        rss_before,
        rss_peak,
        quarters,
    }
}

fn report(o: &Outcome) {
    let secs = o.wall.as_secs_f64();
    let mut sorted = o.ingest_ms.clone();
    sorted.sort_by(f64::total_cmp);
    let total_ms: f64 = o.ingest_ms.iter().sum();

    let cores = o
        .cpu
        .map(|cpu| format!("{:.2}", cpu.as_secs_f64() / secs))
        .unwrap_or_else(|| "-".into());
    let memory = match (o.rss_before, o.rss_peak) {
        (Some(before), Some(peak)) => format!("{:.0} MB (+{:.0})", peak, peak - before),
        _ => "-".into(),
    };

    eprintln!(
        "{:<26} {:>9.0} swaps/s  {:>5.1}x real time  {:>7.2} s  cores {:>5}  \
         ingest p50 {:>7.2} p99 {:>7.2} max {:>7.2} ms  peak RSS {}",
        o.label,
        o.swaps as f64 / secs,
        o.slots * SLOT_SECONDS / secs,
        secs,
        cores,
        percentile(&sorted, 0.5),
        percentile(&sorted, 0.99),
        sorted.last().copied().unwrap_or(0.0),
        memory,
    );

    let share = |ms: f64| 100.0 * ms / total_ms;
    let reducers: Vec<String> = o
        .split
        .reducers_ms
        .iter()
        .map(|(name, (own, views))| {
            format!("{name} {:.0}% + views {:.0}%", share(*own), share(*views))
        })
        .collect();
    let rest = total_ms - o.split.dag_ms - o.ack_ms;
    eprintln!(
        "{:<19} time: DAG {:.0}% (raw {:.0}%; {}{}), finality+batch {:.0}%, commit {:.0}%",
        "",
        share(o.split.dag_ms),
        share(o.split.raw_ms),
        reducers.join("; "),
        if o.split.parallel {
            ", in parallel"
        } else {
            ""
        },
        share(rest),
        share(o.ack_ms),
    );
    let quarters: Vec<String> = o.quarters.iter().map(|q| format!("{q:.0}")).collect();
    eprintln!(
        "{:<19} swaps/s by quarter: {}   change records {:.0}/s   \
         check: realized {:.6e} volume {:.6e} trades {:.0}",
        "",
        quarters.join(" / "),
        o.records as f64 / secs,
        o.target.realized_pnl,
        o.target.volume_usd,
        o.target.candle_trades,
    );
}

fn main() {
    let slots = env_usize("SDEX_SLOTS", 12_000);
    let started = Instant::now();
    let data = dataset::generate(slots, SEED);
    eprintln!(
        "dataset: {} slots, {} blocks, {} swaps, {} wallets, {} tokens, {} pools, \
         {} wallet-token pairs ({:.1} s)",
        slots,
        data.blocks.len(),
        data.swaps.len(),
        data.traders,
        data.tokens,
        data.pools,
        data.positions,
        started.elapsed().as_secs_f64(),
    );

    if let Ok(path) = std::env::var("SDEX_WRITE") {
        data.write(&path).unwrap();
        eprintln!("wrote {path}");
        return;
    }

    let layout = Layout::parse(&std::env::var("SDEX_POSITIONS").unwrap_or_else(|_| "pair".into()));
    let runtimes = env_list("SDEX_RUNTIMES", "fn,lua");
    let modes = env_list("SDEX_MODES", "backfill,tip");
    let storage = std::env::var("SDEX_STORAGE").unwrap_or_else(|_| "rocks".into());
    let batch_slots = env_usize("SDEX_BATCH_SLOTS", 100);
    let checkpoints: Vec<u64> = env_list("SDEX_CHECKPOINT", "1")
        .iter()
        .map(|n| n.parse().expect("SDEX_CHECKPOINT: a list of block counts"))
        .collect();
    eprintln!(
        "storage {storage}, {batch_slots} slots in each backfill ingest, \
         rayon threads {}",
        std::env::var("RAYON_NUM_THREADS").unwrap_or_else(|_| "all".into()),
    );

    for mode in &modes {
        eprintln!();
        for runtime in &runtimes {
            for &checkpoint in &checkpoints {
                let outcome = run(&data, runtime, mode, layout, &storage, batch_slots, checkpoint);
                report(&outcome);
            }
        }
    }
}
