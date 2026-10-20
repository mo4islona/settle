//! Instructions and allocations of `Settle::ingest`, counted under Valgrind by
//! Gungraun: Callgrind for instructions, DHAT for bytes and blocks allocated.
//! Between runs of one binary, allocation counts repeat exactly (Lua cases to a
//! few blocks) and instruction counts move by less than 0.4%, so one run of each
//! build compares two builds. The wall-clock benches cannot resolve a change that
//! small.
//!
//! Linux only. Needs `valgrind` and the runner of the same version as the
//! `gungraun` dev-dependency. DHAT cannot see allocations made through
//! mimalloc, so build without the default features:
//!
//!   cargo install gungraun-runner --version 0.20.0
//!   git switch BASE && cargo bench --bench instructions --no-default-features \
//!       --features lua,rayon -- --save-baseline=base
//!   git switch NEW  && cargo bench --bench instructions --no-default-features \
//!       --features lua,rayon -- --baseline=base
//!
//! Memory storage only: RocksDB flushes and compacts on its own threads, which
//! neither tool counts. Rayon runs on the bench thread for the same reason.
//! Each case ingests warm-up batches in setup, outside the count, so the counted
//! batches see a steady state. Postgres comparisons stay in `vs_postgres_*`.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use gungraun::prelude::*;
use gungraun::{Callgrind, Dhat, DhatMetric, EventKind};
use settle::db::{Config, IngestInput, IngestRows, Settle};
use settle::js_codec::{self, Decoder, Strings};
use settle::storage::memory::MemoryBackend;
use settle::test_helpers::cursor;
use settle::types::{BlockCursor, ChangeBatch, RowMap, Value};

const ROWS_PER_BLOCK: u64 = 50;
/// Finality lag at the tip, in blocks: about two Ethereum epochs.
const FINALITY_LAG: u64 = 64;

#[derive(Clone, Copy)]
enum Workload {
    /// `sum` MV straight off a virtual table: the engine floor.
    MvSum,
    /// `MvSum` on a stored table, which also emits one change record per raw row.
    MvSumStored,
    /// `min`/`max`/`last` MV: aggregates with no reverse delta.
    MvMinMax,
    /// A Lua reducer feeding a `last` MV.
    Lua,
    /// The Polymarket schema: two Lua reducers and their MVs.
    Polymarket,
}

impl Workload {
    fn table(self) -> &'static str {
        match self {
            Workload::MvSum | Workload::MvSumStored | Workload::Lua => "transfers",
            Workload::MvMinMax => "trades",
            Workload::Polymarket => "orders",
        }
    }

    fn schema(self) -> String {
        let transfers = "transfers (
            block_number UInt64,
            from_addr    String,
            to_addr      String,
            value        Float64
        );";

        match self {
            Workload::MvSum | Workload::MvSumStored => {
                let create = match self {
                    Workload::MvSum => "CREATE VIRTUAL TABLE",
                    _ => "CREATE TABLE",
                };
                format!(
                    "{create} {transfers}
                    CREATE MATERIALIZED VIEW balances AS
                    SELECT from_addr, sum(value) AS balance
                    FROM transfers GROUP BY from_addr;"
                )
            }
            Workload::MvMinMax => "CREATE VIRTUAL TABLE trades (
                    block_number UInt64,
                    asset_id     String,
                    price        Float64
                );
                CREATE MATERIALIZED VIEW asset_price AS
                SELECT asset_id, max(price) AS hi, min(price) AS lo, last(price) AS last_price
                FROM trades GROUP BY asset_id;"
                .to_string(),
            Workload::Lua => format!(
                "CREATE VIRTUAL TABLE {transfers}
                CREATE REDUCER user_balance
                SOURCE transfers
                GROUP BY from_addr
                STATE (balance Float64 DEFAULT 0)
                LANGUAGE lua
                PROCESS $$
                    state.balance = state.balance + row.value
                    emit {{ balance = state.balance }}
                $$;
                CREATE MATERIALIZED VIEW balances AS
                SELECT from_addr, last(balance) AS balance
                FROM user_balance GROUP BY from_addr;"
            ),
            Workload::Polymarket => include_str!("../tests/conformance/ct10_golden/polymarket.sql").to_string(),
        }
    }

    fn row(self, i: u64) -> RowMap {
        match self {
            Workload::MvSum | Workload::MvSumStored | Workload::Lua => HashMap::from([
                (
                    "from_addr".into(),
                    Value::String(format!("0xfrom{:06x}", i % 10_000)),
                ),
                (
                    "to_addr".into(),
                    Value::String(format!("0xto{:06x}", (i + 1) % 10_000)),
                ),
                ("value".into(), Value::Float64(1.0 + i as f64 * 0.01)),
            ]),
            Workload::MvMinMax => HashMap::from([
                (
                    "asset_id".into(),
                    Value::String(format!("asset_{:04}", i * 7 % 1000)),
                ),
                (
                    "price".into(),
                    Value::Float64(100.0 + (i * 13 % 997) as f64 * 0.1),
                ),
            ]),
            Workload::Polymarket => polymarket_order(i),
        }
    }
}

/// Same distribution as `profile_polymarket`: 100K traders, the top 10% place
/// 80% of the orders.
fn polymarket_order(i: u64) -> RowMap {
    let traders = 100_000;
    let top = traders / 10;
    let trader = if i % 5 < 4 {
        i % top
    } else {
        top + i % (traders - top)
    };

    let price_bps = if i % 10 < 7 {
        9600 + i % 400
    } else {
        3000 + i % 6000
    };
    let shares = 1_000_000_000u64;

    HashMap::from([
        (
            "trader".into(),
            Value::String(format!("0xtrader{trader:06x}")),
        ),
        (
            "asset_id".into(),
            Value::String(format!("token_{:04}", i % 10_000)),
        ),
        ("usdc".into(), Value::UInt64(shares * price_bps / 10_000)),
        ("shares".into(), Value::UInt64(shares)),
        ("side".into(), Value::UInt64(u64::from(i % 5 >= 3))),
        ("timestamp".into(), Value::UInt64(1_000_000 + i / 500)),
    ])
}

#[derive(Clone, Copy)]
enum Mode {
    /// 100 blocks per ingest, finality at the batch tip.
    Backfill,
    /// One block per ingest, finality `FINALITY_LAG` blocks behind.
    Tip,
}

impl Mode {
    fn blocks_per_ingest(self) -> u64 {
        match self {
            Mode::Backfill => 100,
            Mode::Tip => 1,
        }
    }

    /// Ingests in warm-up, and again in the counted run.
    fn ingests(self) -> usize {
        match self {
            Mode::Backfill => 4,
            Mode::Tip => 200,
        }
    }

    /// Built one at a time, so that a setup that ingests them as they come
    /// never holds them all.
    fn inputs(self, workload: Workload) -> impl Iterator<Item = IngestInput> {
        let per_ingest = self.blocks_per_ingest();
        let mut row_index = 0;

        (0..2 * self.ingests() as u64).map(move |n| {
            let first = 1 + n * per_ingest;
            let last = first + per_ingest - 1;

            let mut rows = Vec::with_capacity((per_ingest * ROWS_PER_BLOCK) as usize);
            for block in first..=last {
                for _ in 0..ROWS_PER_BLOCK {
                    let mut row = workload.row(row_index);
                    row.insert("block_number".into(), Value::UInt64(block));
                    rows.push(row);
                    row_index += 1;
                }
            }

            let finalized = match self {
                Mode::Backfill => last,
                Mode::Tip => last.saturating_sub(FINALITY_LAG),
            };

            IngestInput {
                data: HashMap::from([(workload.table().to_string(), rows)]),
                rollback_chain: (first..=last)
                    .filter(|b| *b > finalized)
                    .map(cursor)
                    .collect(),
                finalized_head: cursor(finalized),
            }
        })
    }
}

struct Prepared {
    db: Settle,
    inputs: Vec<IngestInput>,
}

fn init_bench_process() {
    // A runtime check: a compile error would break `cargo build --all-targets`.
    if cfg!(feature = "mimalloc") {
        panic!(
            "DHAT cannot see mimalloc allocations: build with --no-default-features --features lua,rayon"
        );
    }

    #[cfg(feature = "rayon")]
    rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .use_current_thread()
        .build_global()
        .unwrap();
}

/// Leaked, so that dropping the engine is not counted.
fn prepare(workload: Workload, mode: Mode) -> &'static mut Prepared {
    let (db, inputs) = warm_up(workload, mode);
    Box::leak(Box::new(Prepared { db, inputs }))
}

/// The engine after the warm-up ingests, and the inputs to count.
fn warm_up(workload: Workload, mode: Mode) -> (Settle, Vec<IngestInput>) {
    init_bench_process();

    let mut db = Settle::open(Config::new(workload.schema())).unwrap();
    let mut inputs: Vec<IngestInput> = mode.inputs(workload).collect();
    let counted = inputs.split_off(mode.ingests());
    for input in inputs {
        ingest_and_ack(&mut db, input);
    }
    (db, counted)
}

fn ingest_and_ack(db: &mut Settle, input: IngestInput) -> usize {
    let Some(batch) = db.ingest(input).unwrap() else {
        return 0;
    };
    db.ack(batch.sequence).unwrap();
    batch.record_count()
}

fn prepare_backfill(workload: Workload) -> &'static mut Prepared {
    prepare(workload, Mode::Backfill)
}

fn prepare_tip(workload: Workload) -> &'static mut Prepared {
    prepare(workload, Mode::Tip)
}

fn ingest_all(case: &'static mut Prepared) -> usize {
    let inputs = std::mem::take(&mut case.inputs);
    let records = inputs
        .into_iter()
        .map(|input| ingest_and_ack(&mut case.db, input))
        .sum();
    black_box(records)
}

#[library_benchmark]
#[bench::mv_sum(args = (Workload::MvSum), setup = prepare_backfill)]
#[bench::mv_sum_stored(args = (Workload::MvSumStored), setup = prepare_backfill)]
#[bench::mv_minmax(args = (Workload::MvMinMax), setup = prepare_backfill)]
#[bench::lua(args = (Workload::Lua), setup = prepare_backfill)]
#[bench::polymarket(args = (Workload::Polymarket), setup = prepare_backfill)]
fn backfill(case: &'static mut Prepared) -> usize {
    ingest_all(case)
}

#[library_benchmark]
#[bench::mv_sum(args = (Workload::MvSum), setup = prepare_tip)]
#[bench::mv_sum_stored(args = (Workload::MvSumStored), setup = prepare_tip)]
#[bench::mv_minmax(args = (Workload::MvMinMax), setup = prepare_tip)]
#[bench::lua(args = (Workload::Lua), setup = prepare_tip)]
#[bench::polymarket(args = (Workload::Polymarket), setup = prepare_tip)]
fn tip(case: &'static mut Prepared) -> usize {
    ingest_all(case)
}

/// One ingest of many blocks with one row each, all in one group. A linear
/// engine doubles the count when the blocks double; a path that reads all
/// unfinalized blocks of a group on each block quadruples it.
#[derive(Clone, Copy)]
enum Scale {
    Sum,
    /// Aggregates with no reverse delta.
    MinMax,
    /// A window wider than the batch, so no block expires.
    SlidingLast,
    /// `sum` in a window wider than the batch, then one block whose timestamp
    /// expires every other block of the group at once.
    SlidingJump,
}

impl Scale {
    fn schema(self) -> String {
        let aggregates = match self {
            Scale::Sum => "sum(value) AS total",
            Scale::MinMax => "min(value) AS lo, max(value) AS hi",
            Scale::SlidingLast => "last(value) AS latest",
            Scale::SlidingJump => "sum(value) AS total",
        };
        let window = match self {
            Scale::SlidingLast | Scale::SlidingJump => "WINDOW SLIDING INTERVAL 1 DAY BY ts",
            Scale::Sum | Scale::MinMax => "",
        };

        format!(
            "CREATE VIRTUAL TABLE ticks (
                block_number UInt64,
                ts           DateTime,
                pair         String,
                value        Float64
            );
            CREATE MATERIALIZED VIEW hot AS
            SELECT pair, {aggregates}
            FROM ticks GROUP BY pair {window};"
        )
    }
}

fn tick(block: u64, seconds: u64) -> RowMap {
    HashMap::from([
        ("block_number".into(), Value::UInt64(block)),
        (
            "ts".into(),
            Value::DateTime(1_700_000_000_000 + seconds as i64 * 1000),
        ),
        ("pair".into(), Value::String("hot".into())),
        ("value".into(), Value::Float64((block * 7919 % 1000) as f64)),
    ])
}

/// One block per second, finality at the batch tip.
fn prepare_scale(scale: Scale, blocks: u64) -> &'static mut Prepared {
    init_bench_process();

    let mut rows: Vec<RowMap> = (1..=blocks).map(|block| tick(block, block)).collect();
    let mut last = blocks;
    if let Scale::SlidingJump = scale {
        last += 1;
        rows.push(tick(last, blocks + 2 * 86_400));
    }

    let input = IngestInput {
        data: HashMap::from([("ticks".to_string(), rows)]),
        rollback_chain: Vec::new(),
        finalized_head: cursor(last),
    };

    Box::leak(Box::new(Prepared {
        db: Settle::open(Config::new(scale.schema())).unwrap(),
        inputs: vec![input],
    }))
}

#[library_benchmark]
#[bench::sum_2k(args = (Scale::Sum, 2_000), setup = prepare_scale)]
#[bench::sum_4k(args = (Scale::Sum, 4_000), setup = prepare_scale)]
#[bench::sum_8k(args = (Scale::Sum, 8_000), setup = prepare_scale)]
#[bench::minmax_2k(args = (Scale::MinMax, 2_000), setup = prepare_scale)]
#[bench::minmax_4k(args = (Scale::MinMax, 4_000), setup = prepare_scale)]
#[bench::minmax_8k(args = (Scale::MinMax, 8_000), setup = prepare_scale)]
#[bench::sliding_last_2k(args = (Scale::SlidingLast, 2_000), setup = prepare_scale)]
#[bench::sliding_last_4k(args = (Scale::SlidingLast, 4_000), setup = prepare_scale)]
#[bench::sliding_last_8k(args = (Scale::SlidingLast, 8_000), setup = prepare_scale)]
#[bench::sliding_jump_2k(args = (Scale::SlidingJump, 2_000), setup = prepare_scale)]
#[bench::sliding_jump_4k(args = (Scale::SlidingJump, 4_000), setup = prepare_scale)]
#[bench::sliding_jump_8k(args = (Scale::SlidingJump, 8_000), setup = prepare_scale)]
fn scale(case: &'static mut Prepared) -> usize {
    ingest_all(case)
}

/// A sliding window of one group that holds `window` blocks, one block per
/// second, so each block expires the oldest. Each ingest advances finality
/// and so writes a checkpoint; a checkpoint of the whole window grows the
/// count with it.
fn prepare_window(window: u64, mode: Mode) -> &'static mut Prepared {
    init_bench_process();

    let schema = format!(
        "CREATE VIRTUAL TABLE ticks (
            block_number UInt64,
            ts           DateTime,
            pair         String,
            value        Float64
        );
        CREATE MATERIALIZED VIEW hot AS
        SELECT pair, sum(value) AS total
        FROM ticks GROUP BY pair
        WINDOW SLIDING INTERVAL {window} SECONDS BY ts;"
    );
    let input = |first: u64, last: u64| {
        let finalized = match mode {
            Mode::Backfill => last,
            Mode::Tip => last.saturating_sub(FINALITY_LAG),
        };
        IngestInput {
            data: HashMap::from([(
                "ticks".to_string(),
                (first..=last).map(|block| tick(block, block)).collect(),
            )]),
            rollback_chain: (first..=last)
                .filter(|b| *b > finalized)
                .map(cursor)
                .collect(),
            finalized_head: cursor(finalized),
        }
    };

    let mut db = Settle::open(Config::new(schema)).unwrap();
    ingest_and_ack(&mut db, input(1, window));

    let per_ingest = mode.blocks_per_ingest();
    let inputs = (0..mode.ingests() as u64)
        .map(|n| {
            let first = window + 1 + n * per_ingest;
            input(first, first + per_ingest - 1)
        })
        .collect();
    Box::leak(Box::new(Prepared { db, inputs }))
}

fn prepare_window_tip(window: u64) -> &'static mut Prepared {
    prepare_window(window, Mode::Tip)
}

fn prepare_window_backfill(window: u64) -> &'static mut Prepared {
    prepare_window(window, Mode::Backfill)
}

#[library_benchmark]
#[bench::window_1k(args = (1_000), setup = prepare_window_tip)]
#[bench::window_2k(args = (2_000), setup = prepare_window_tip)]
#[bench::window_4k(args = (4_000), setup = prepare_window_tip)]
#[bench::backfill_window_1k(args = (1_000), setup = prepare_window_backfill)]
#[bench::backfill_window_4k(args = (4_000), setup = prepare_window_backfill)]
fn checkpoint(case: &'static mut Prepared) -> usize {
    ingest_all(case)
}

/// A config that reopens a database whose derived state is durable only up to
/// its first ingest: setup ingests the `backfill` inputs of `mv_sum_stored`
/// with persist deferral, so reopen replays a tail of 700 blocks, 35K rows.
fn prepare_reopen() -> Config {
    init_bench_process();

    let workload = Workload::MvSumStored;
    let storage = Arc::new(MemoryBackend::new());
    let config = || {
        Config::new(workload.schema())
            .storage(storage.clone())
            .backfill_checkpoint_interval(u64::MAX)
    };

    let mut db = Settle::open(config()).unwrap();
    for input in Mode::Backfill.inputs(workload) {
        ingest_and_ack(&mut db, input);
    }
    drop(db);

    config()
}

#[library_benchmark]
#[bench::mv_sum_stored(setup = prepare_reopen)]
fn reopen(config: Config) {
    // Leaked, so that dropping the engine is not counted.
    std::mem::forget(black_box(Settle::open(config).unwrap()));
}

/// An ingest as the napi binding receives it: the arrays that `TableWriter`
/// in `codec.ts` fills.
struct EncodedInput {
    ints: Vec<u32>,
    tags: Vec<u8>,
    nums: Vec<f64>,
    strings: String,
    rollback_chain: Vec<BlockCursor>,
    finalized_head: BlockCursor,
}

impl EncodedInput {
    fn new(db: &Settle, input: IngestInput) -> Self {
        let mut encoded = Self {
            ints: vec![0, 0],
            tags: Vec::new(),
            nums: Vec::new(),
            strings: String::new(),
            rollback_chain: input.rollback_chain,
            finalized_head: input.finalized_head,
        };

        let tables: Vec<_> = input.data.iter().collect();
        encoded.ints.push(tables.len() as u32);
        for (table, rows) in &tables {
            let width = db.table_registry(table).unwrap().len() + 1;
            encoded.ints.extend([table.len() as u32, rows.len() as u32, width as u32]);
            encoded.strings.push_str(table);
        }
        for (table, rows) in &tables {
            let columns = db.table_registry(table).unwrap().names().to_vec();
            for row in rows.iter() {
                encoded.put(row.get("block_number"));
                for column in &columns {
                    encoded.put(row.get(column));
                }
            }
        }

        encoded.ints[0] = encoded.ints.len() as u32;
        encoded.ints[1] = encoded.tags.len() as u32;
        assert!(encoded.strings.is_ascii());
        encoded
    }

    fn put(&mut self, value: Option<&Value>) {
        let (tag, num) = match value {
            None | Some(Value::Null) => (js_codec::NULL, 0.0),
            Some(Value::Float64(f)) => (js_codec::NUMBER, *f),
            Some(Value::UInt64(n)) => (js_codec::NUMBER, *n as f64),
            Some(Value::String(s)) => {
                self.strings.push_str(s);
                (js_codec::STRING, s.len() as f64)
            }
            Some(other) => panic!("no JS value for {other:?}"),
        };
        self.tags.push(tag);
        self.nums.push(num);
    }
}

/// What the napi binding does with an ingest in Rust: decode the rows,
/// ingest them, encode the batch.
fn ingest_napi(db: &mut Settle, input: EncodedInput) -> Option<ChangeBatch> {
    let strings = Strings::Ascii(&input.strings);
    let mut dec = Decoder::new(&input.ints[2..], &input.tags, &input.nums, strings, true).unwrap();
    let mut no_others = |i: usize| -> Result<Value, String> { panic!("host value {i}") };
    let data = js_codec::decode_ingest(&mut dec, |t| db.table_registry(t), &mut no_others).unwrap();

    let batch = db
        .ingest_rows(IngestRows {
            data,
            rollback_chain: input.rollback_chain,
            finalized_head: input.finalized_head,
        })
        .unwrap();
    if let Some(batch) = &batch {
        let enc = js_codec::encode_batch(batch);
        let mut buffer = vec![0u8; enc.byte_len()];
        enc.write(&mut buffer);
        black_box((buffer, enc.strings()));
    }
    batch
}

struct PreparedNapi {
    db: Settle,
    inputs: Vec<EncodedInput>,
}

/// Leaked, so that dropping the engine is not counted.
fn prepare_napi(workload: Workload, mode: Mode) -> &'static mut PreparedNapi {
    let (db, inputs) = warm_up(workload, mode);
    let inputs = inputs.into_iter().map(|input| EncodedInput::new(&db, input)).collect();
    Box::leak(Box::new(PreparedNapi { db, inputs }))
}

fn prepare_napi_backfill(workload: Workload) -> &'static mut PreparedNapi {
    prepare_napi(workload, Mode::Backfill)
}

fn prepare_napi_tip(workload: Workload) -> &'static mut PreparedNapi {
    prepare_napi(workload, Mode::Tip)
}

fn ingest_all_napi(case: &'static mut PreparedNapi) -> usize {
    let inputs = std::mem::take(&mut case.inputs);
    let mut records = 0;
    for input in inputs {
        if let Some(batch) = ingest_napi(&mut case.db, input) {
            case.db.ack(batch.sequence).unwrap();
            records += batch.record_count();
        }
    }
    black_box(records)
}

#[library_benchmark]
#[bench::mv_sum(args = (Workload::MvSum), setup = prepare_napi_backfill)]
#[bench::lua(args = (Workload::Lua), setup = prepare_napi_backfill)]
#[bench::polymarket(args = (Workload::Polymarket), setup = prepare_napi_backfill)]
fn napi_backfill(case: &'static mut PreparedNapi) -> usize {
    ingest_all_napi(case)
}

#[library_benchmark]
#[bench::mv_sum(args = (Workload::MvSum), setup = prepare_napi_tip)]
#[bench::polymarket(args = (Workload::Polymarket), setup = prepare_napi_tip)]
fn napi_tip(case: &'static mut PreparedNapi) -> usize {
    ingest_all_napi(case)
}

library_benchmark_group!(name = ingest, benchmarks = [backfill, tip]);
library_benchmark_group!(name = napi, benchmarks = [napi_backfill, napi_tip]);
library_benchmark_group!(name = scaling, benchmarks = [scale, checkpoint]);
library_benchmark_group!(name = recovery, benchmarks = [reopen]);

// No cache simulation: it doubles the run time, and the limits read instructions.
main!(
    config = LibraryBenchmarkConfig::default()
        .tool(
            Callgrind::with_args(["--cache-sim=no"])
                .format([EventKind::Ir])
                .soft_limits([(EventKind::Ir, 1.0)])
        )
        // Only allocations whose stack reaches the bench function are counted,
        // and Valgrind keeps 12 frames by default: a deeper one would drop out.
        .tool(
            Dhat::with_args(["--num-callers=500"])
                .format([DhatMetric::TotalBytes, DhatMetric::TotalBlocks])
                .soft_limits([
                    (DhatMetric::TotalBytes, 1.0),
                    (DhatMetric::TotalBlocks, 1.0),
                ])
        ),
    library_benchmark_groups = [ingest, napi, scaling, recovery]
);
