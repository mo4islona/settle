//! Realistic-finality variant of profile_polymarket: ingest each block, but
//! `finalized_head` lags by `CONFIRMATION_DEPTH` blocks to mimic real
//! blockchain consumer behaviour (Ethereum confirms in ~12-32 blocks, Polygon
//! / Polymarket in more).
//!
//! Compared to `profile_polymarket` which calls `ingest_one` (always passes
//! current `finalized_block()` = 0, so `engine.finalize(0)` runs as no-op
//! every batch yet still serializes all group state).
//!
//! Run with: samply record cargo bench --bench profile_polymarket_realistic

use std::collections::HashMap;
use std::time::Instant;

use settle::db::{Config, IngestInput, Settle};
use settle::test_helpers::{cursor, ingest_input};
use settle::types::{RowMap, Value};

const SCHEMA: &str = include_str!("../tests/polymarket/schema.sql");
const CONFIRMATION_DEPTH: u64 = 32;

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

fn ingest_realistic(db: &mut Settle, table: &str, block: u64, rows: Vec<RowMap>) {
    let finalized = block.saturating_sub(CONFIRMATION_DEPTH);
    let mut rows = rows;
    for row in &mut rows {
        row.insert("block_number".to_string(), Value::UInt64(block));
    }
    let mut data: HashMap<String, Vec<RowMap>> = HashMap::new();
    data.insert(table.to_string(), rows);
    let input = IngestInput {
        data,
        rollback_chain: vec![cursor(block)],
        finalized_head: cursor(finalized),
    };
    ingest_input(db, input).unwrap();
}

fn main() {
    let total_rows = 500_000;
    let batch_size = 500;
    let num_traders = 100_000;

    let cfg = Config::new(SCHEMA);
    let mut db = Settle::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| make_polymarket_order(i, num_traders))
        .collect();

    let warmup_chunks = rows[..5000].chunks(batch_size).count() as u64;
    for (block, chunk) in rows[..5000].chunks(batch_size).enumerate() {
        ingest_realistic(&mut db, "orders", block as u64 + 1, chunk.to_vec());
    }

    let start_block = warmup_chunks + 1;
    let profile_rows = total_rows - 5000;
    let t = Instant::now();
    for (i, chunk) in rows[5000..].chunks(batch_size).enumerate() {
        ingest_realistic(&mut db, "orders", start_block + i as u64, chunk.to_vec());
    }
    let elapsed = t.elapsed();
    eprintln!(
        "polymarket REALISTIC (confirmation_depth={}): {} rows in {:.3}s = {:.0} rows/s",
        CONFIRMATION_DEPTH,
        profile_rows,
        elapsed.as_secs_f64(),
        profile_rows as f64 / elapsed.as_secs_f64()
    );
}
