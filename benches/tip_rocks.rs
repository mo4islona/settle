//! Wall-clock cost of an ingest and its ack at the chain tip on RocksDB: one
//! block per ingest, finality `FINALITY_LAG` blocks behind, so each ingest
//! advances finality and writes a checkpoint. A sliding window of `window`
//! blocks shows what a checkpoint writes per block.
//!
//! Run with: cargo bench --bench tip_rocks
//!
//! The database goes to `target/tmp`, on the disk of the target directory.

use std::collections::HashMap;
use std::time::Instant;

use settle::db::{Config, IngestInput, Settle};
use settle::test_helpers::cursor;
use settle::types::{RowMap, Value};

const FINALITY_LAG: u64 = 64;
const ROWS_PER_BLOCK: u64 = 20;
const PAIRS: u64 = 100;
const MEASURED_INGESTS: u64 = 2_000;
const RUNS: usize = 3;

fn schema(window_seconds: u64) -> String {
    format!(
        "CREATE VIRTUAL TABLE ticks (
            block_number UInt64,
            ts           DateTime,
            pair         String,
            value        Float64
        );
        CREATE MATERIALIZED VIEW pair_window AS
        SELECT pair, sum(value) AS volume, last(value) AS price
        FROM ticks GROUP BY pair
        WINDOW SLIDING INTERVAL {window_seconds} SECONDS BY ts;"
    )
}

/// One block per second.
fn input(first: u64, last: u64, finalized: u64) -> IngestInput {
    let mut rows: Vec<RowMap> = Vec::new();
    for block in first..=last {
        for i in 0..ROWS_PER_BLOCK {
            let n = block * ROWS_PER_BLOCK + i;
            rows.push(HashMap::from([
                ("block_number".into(), Value::UInt64(block)),
                ("ts".into(), Value::DateTime(1_700_000_000_000 + block as i64 * 1000)),
                ("pair".into(), Value::String(format!("pair{}", n * 7919 % PAIRS))),
                ("value".into(), Value::Float64((n % 1000) as f64)),
            ]));
        }
    }

    IngestInput {
        data: HashMap::from([("ticks".to_string(), rows)]),
        rollback_chain: (first..=last).filter(|b| *b > finalized).map(cursor).collect(),
        finalized_head: cursor(finalized),
    }
}

fn ingest_and_ack(db: &mut Settle, input: IngestInput) {
    if let Some(batch) = db.ingest(input).unwrap() {
        db.ack(batch.sequence).unwrap();
    }
}

/// Microseconds per tip ingest, after a backfill that fills the window.
fn run(window: u64) -> f64 {
    // In target/, on the disk: /tmp can be tmpfs, where fsync costs nothing.
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let config = Config::with_data_dir(schema(window), dir.path().to_str().unwrap());
    let mut db = Settle::open(config).unwrap();

    let mut block = 0;
    while block < window {
        let last = (block + 100).min(window);
        ingest_and_ack(&mut db, input(block + 1, last, last.saturating_sub(FINALITY_LAG)));
        block = last;
    }

    let start = Instant::now();
    for _ in 0..MEASURED_INGESTS {
        block += 1;
        ingest_and_ack(&mut db, input(block, block, block - FINALITY_LAG));
    }
    start.elapsed().as_secs_f64() * 1e6 / MEASURED_INGESTS as f64
}

fn main() {
    println!("tip ingest + ack on RocksDB, sliding window, {ROWS_PER_BLOCK} rows per block");
    println!("{:>10} {:>14}", "window", "us/ingest");

    for window in [600, 3_600, 14_400] {
        let mut times: Vec<f64> = (0..RUNS).map(|_| run(window)).collect();
        times.sort_by(f64::total_cmp);
        println!("{:>10} {:>14.1}", format!("{window} s"), times[RUNS / 2]);
    }
}
