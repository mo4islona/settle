//! Backfill-load probe: large batches, RocksDB backend, finality AT TIP
//! (each ingested block is immediately final — the historical-backfill case).
//!
//! Purpose: measure the upper bound of skipping derived-state persistence.
//! Run twice and compare:
//!   normal:  cargo bench --bench profile_backfill
//!   skip:    SETTLE_SKIP_FINALIZE_PERSIST=1 cargo bench --bench profile_backfill
//! The delta = cost of reducer+MV finalize serialize + RocksDB commit of
//! derived state on this load. (SKIP mode yields an unrecoverable DB — probe
//! only, see storage::skip_finalize_persist.)

use std::collections::HashMap;
use std::time::Instant;

use settle::db::{Config, IngestInput, Settle};
use settle::test_helpers::{cursor, ingest_input};
use settle::types::{RowMap, Value};

const SCHEMA: &str = include_str!("../tests/polymarket/schema.sql");

fn make_polymarket_order(i: usize, num_traders: usize) -> RowMap {
    let top_traders = num_traders / 10;
    let remaining = num_traders - top_traders;
    let trader_idx = if i % 5 < 4 {
        i % top_traders
    } else {
        top_traders + (i % remaining)
    };
    let trader = format!("0xtrader{trader_idx:06x}");
    let asset_id = format!("token_{:04}", i % 10_000);
    let side: u64 = if i % 5 < 3 { 0 } else { 1 };
    let (usdc, shares) = if i % 10 < 7 {
        let price_bps = 9600 + (i % 400);
        let shares_val = 1_000_000_000u64;
        let usdc_val = shares_val * price_bps as u64 / 10_000;
        (usdc_val, shares_val)
    } else {
        let price_bps = 3000 + (i % 6000);
        let shares_val = 1_000_000_000u64;
        let usdc_val = shares_val * price_bps as u64 / 10_000;
        (usdc_val, shares_val)
    };
    let timestamp = 1_000_000 + (i as u64 / 500);

    HashMap::from([
        ("trader".to_string(), Value::String(trader)),
        ("asset_id".to_string(), Value::String(asset_id)),
        ("usdc".to_string(), Value::UInt64(usdc)),
        ("shares".to_string(), Value::UInt64(shares)),
        ("side".to_string(), Value::UInt64(side)),
        ("timestamp".to_string(), Value::UInt64(timestamp)),
    ])
}

/// Backfill ingest: the block is immediately final (finalized_head == block).
fn ingest_backfill(db: &mut Settle, table: &str, block: u64, rows: Vec<RowMap>) {
    let mut rows = rows;
    for row in &mut rows {
        row.insert("block_number".to_string(), Value::UInt64(block));
    }
    let mut data: HashMap<String, Vec<RowMap>> = HashMap::new();
    data.insert(table.to_string(), rows);
    let input = IngestInput {
        data,
        rollback_chain: vec![cursor(block)],
        finalized_head: cursor(block), // <-- finality AT TIP (backfill)
    };
    ingest_input(db, input).unwrap();
}

fn main() {
    let total_rows = 500_000;
    let batch_size: usize = std::env::var("SETTLE_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000); // large batches (backfill)
    let num_traders = 100_000;
    let interval: u64 = std::env::var("SETTLE_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let dir = tempfile::tempdir().unwrap();
    let cfg = Config::with_data_dir(SCHEMA, dir.path().to_str().unwrap())
        .backfill_checkpoint_interval(interval);
    let mut db = Settle::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| make_polymarket_order(i, num_traders))
        .collect();

    // Warm up (not timed).
    let warmup = 10_000usize;
    let mut block = 1u64;
    for chunk in rows[..warmup].chunks(batch_size) {
        ingest_backfill(&mut db, "orders", block, chunk.to_vec());
        block += 1;
    }

    let profile_rows = total_rows - warmup;
    let t = Instant::now();
    for chunk in rows[warmup..].chunks(batch_size) {
        ingest_backfill(&mut db, "orders", block, chunk.to_vec());
        block += 1;
    }
    let elapsed = t.elapsed();
    eprintln!(
        "backfill rocks batch={} interval={} durable={}: {} rows in {:.3}s = {:.0} rows/s",
        batch_size,
        interval,
        db.durable_block(),
        profile_rows,
        elapsed.as_secs_f64(),
        profile_rows as f64 / elapsed.as_secs_f64()
    );
}
