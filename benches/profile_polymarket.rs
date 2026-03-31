//! Focused profiling benchmark for Polymarket full pipeline.
//! Run with: samply record cargo bench --bench profile_polymarket

use std::collections::HashMap;

use delta_db::db::{Config, DeltaDb};
use delta_db::types::{RowMap, Value};

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

fn main() {
    let total_rows = 500_000;
    let batch_size = 500;
    let num_traders = 100_000;

    let cfg = Config::new(SCHEMA);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| make_polymarket_order(i, num_traders))
        .collect();

    // Warm up
    for (block, chunk) in rows[..5000].chunks(batch_size).enumerate() {
        db.process_batch("orders", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();

    // Profiled section
    let start_block = 10;
    for (i, chunk) in rows[5000..].chunks(batch_size).enumerate() {
        db.process_batch("orders", (start_block + i) as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();

    eprintln!("Done: {} rows processed", total_rows);
}
