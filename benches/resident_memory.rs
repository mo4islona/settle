//! Resident memory per group (SLI-6) and the cost of `max_resident_groups`.
//! Each case runs in a process of its own on RocksDB: blocks of new keys,
//! final at once, each batch acked, then a second pass over the same keys.
//! It prints the RSS and the peak RSS, and the bytes per group from the
//! growth of the peak between two group counts, so that the constant memory
//! of RocksDB and of the process drops out. The peak, because mimalloc gives
//! freed pages back late, and the RSS at one moment varies from run to run.
//!
//! Run with: cargo bench --bench resident_memory (Linux: it reads /proc).
//!
//! The database goes to `target/tmp`, on the disk of the target directory.

use std::collections::HashMap;
use std::process::Command;
use std::time::Instant;

use settle::db::{Config, IngestInput, Settle};
use settle::test_helpers::cursor;
use settle::types::{RowMap, Value};

const ROWS_PER_BLOCK: u64 = 10_000;
const GROUP_COUNTS: [u64; 2] = [500_000, 1_000_000];
const CAP: usize = 100_000;

const SHAPES: [(&str, &str); 4] = [
    (
        "reducer",
        "CREATE TABLE trades (block_number UInt64, trader String, amount UInt64);
        CREATE REDUCER positions SOURCE trades GROUP BY trader
        STATE (total UInt64 DEFAULT 0, trades UInt64 DEFAULT 0)
        LANGUAGE lua
        PROCESS $$
            state.total = state.total + row.amount
            state.trades = state.trades + 1
        $$;",
    ),
    (
        "map",
        "CREATE TABLE trades (block_number UInt64, trader String, amount UInt64);
        CREATE REDUCER positions SOURCE trades GROUP BY trader
        STATE (book Map(String, UInt64))
        LANGUAGE lua
        PROCESS $$
            state.book:set('total', (state.book:get('total') or 0) + row.amount)
        $$;",
    ),
    (
        "view",
        "CREATE TABLE trades (block_number UInt64, trader String, amount UInt64);
        CREATE MATERIALIZED VIEW volume AS
        SELECT trader, sum(amount) AS total, count() AS trades
        FROM trades GROUP BY trader;",
    ),
    (
        "both",
        "CREATE TABLE trades (block_number UInt64, trader String, amount UInt64);
        CREATE REDUCER positions SOURCE trades GROUP BY trader
        STATE (total UInt64 DEFAULT 0)
        LANGUAGE lua
        PROCESS $$
            state.total = state.total + row.amount
            emit { total = state.total }
        $$;
        CREATE MATERIALIZED VIEW volume AS
        SELECT trader, last(total) AS total, count() AS trades
        FROM positions GROUP BY trader;",
    ),
];

/// The kB of a `/proc/self/status` line.
fn status_kb(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.trim().trim_end_matches(" kB").trim().parse().ok())
        .unwrap_or(0)
}

fn block_input(block: u64, groups: u64) -> IngestInput {
    let rows: Vec<RowMap> = (0..ROWS_PER_BLOCK)
        .map(|i| {
            let key = (block * ROWS_PER_BLOCK + i) % groups;
            HashMap::from([
                ("block_number".into(), Value::UInt64(block)),
                ("trader".into(), Value::String(format!("trader-{key:08}"))),
                ("amount".into(), Value::UInt64(1 + i % 7)),
            ])
        })
        .collect();
    IngestInput {
        data: HashMap::from([("trades".to_string(), rows)]),
        rollback_chain: Vec::new(),
        finalized_head: cursor(block),
    }
}

/// One case in this process: prints `rss_kb hwm_kb first_pass_s second_pass_s`.
fn run_case(shape: &str, groups: u64, cap: usize) {
    let schema = SHAPES.iter().find(|(name, _)| *name == shape).unwrap().1;
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let config = Config::with_data_dir(schema, dir.path().to_str().unwrap())
        .collect_perf(false)
        .max_resident_groups(cap);
    let mut db = Settle::open(config).unwrap();

    let blocks = groups / ROWS_PER_BLOCK;
    let mut pass = |from: u64| {
        let start = Instant::now();
        for block in from..from + blocks {
            if let Some(batch) = db.ingest(block_input(block, groups)).unwrap() {
                db.ack(batch.sequence).unwrap();
            }
        }
        start.elapsed().as_secs_f64()
    };
    let first = pass(1);
    let rss = status_kb("VmRSS:");
    let second = pass(1 + blocks);

    println!("{rss} {} {first:.3} {second:.3}", status_kb("VmHWM:"));
}

fn main() {
    if let Ok(case) = std::env::var("RESIDENT_CASE") {
        let parts: Vec<&str> = case.split(':').collect();
        run_case(
            parts[0],
            parts[1].parse().unwrap(),
            parts[2].parse().unwrap(),
        );
        return;
    }

    let exe = std::env::current_exe().unwrap();
    println!(
        "{:<8} {:>10} {:>9} {:>10} {:>10} {:>9} {:>9}",
        "shape", "cap", "groups", "RSS MB", "peak MB", "pass 1 s", "pass 2 s"
    );
    for (shape, _) in SHAPES {
        for cap in [usize::MAX, CAP] {
            let mut peak = Vec::new();
            for groups in GROUP_COUNTS {
                let output = Command::new(&exe)
                    .env("RESIDENT_CASE", format!("{shape}:{groups}:{cap}"))
                    .output()
                    .unwrap();
                let line = String::from_utf8_lossy(&output.stdout);
                let fields: Vec<f64> = line
                    .split_whitespace()
                    .map(|f| f.parse().unwrap())
                    .collect();
                let cap_label = if cap == usize::MAX {
                    "none".to_string()
                } else {
                    cap.to_string()
                };
                println!(
                    "{shape:<8} {cap_label:>10} {groups:>9} {:>10.0} {:>9.0} {:>10.2} {:>9.2}",
                    fields[0] / 1024.0,
                    fields[1] / 1024.0,
                    fields[2],
                    fields[3]
                );
                peak.push(fields[1]);
            }
            let added = (GROUP_COUNTS[1] - GROUP_COUNTS[0]) as f64;
            println!(
                "{shape:<8} bytes per added group: {:.0}",
                (peak[1] - peak[0]) * 1024.0 / added
            );
        }
    }
}
