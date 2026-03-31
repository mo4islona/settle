//! Breakdown profiling: measures time spent in each phase of the pipeline.
//! Run with: cargo bench --bench profile_breakdown

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use delta_db::db::{Config, DeltaDb};
use delta_db::engine::reducer::ReducerEngine;
use delta_db::schema::parser::parse_schema;
use delta_db::storage::memory::MemoryBackend;
use delta_db::types::{ColumnRegistry, Row, RowMap, Value};

const FULL_SCHEMA: &str = include_str!("../tests/polymarket/schema.sql");

const MARKET_STATS_ONLY: &str = r#"
CREATE VIRTUAL TABLE orders (
    block_number UInt64, timestamp UInt64, trader String,
    asset_id String, usdc UInt64, shares UInt64, side UInt64
);
CREATE REDUCER market_stats SOURCE orders GROUP BY asset_id
STATE (
    volume Float64 DEFAULT 0, trades UInt64 DEFAULT 0,
    sum_price Float64 DEFAULT 0, sum_price_sq Float64 DEFAULT 0,
    first_seen UInt64 DEFAULT 0, last_seen UInt64 DEFAULT 0
)
LANGUAGE lua PROCESS $$
    if row.shares == 0 then return end
    local price = row.usdc / row.shares
    local vol = row.usdc / 1000000
    state.volume = state.volume + vol
    state.trades = state.trades + 1
    state.sum_price = state.sum_price + price
    state.sum_price_sq = state.sum_price_sq + price * price
    if state.first_seen == 0 then state.first_seen = row.timestamp end
    state.last_seen = row.timestamp
    emit.asset_id = row.asset_id
    emit.volume = vol
    emit.price = price
    emit.price_sq = price * price
$$;
CREATE MATERIALIZED VIEW token_summary AS
SELECT asset_id, sum(volume) AS total_volume, count() AS trade_count,
    last(price) AS last_price, sum(price) AS sum_price, sum(price_sq) AS sum_price_sq
FROM market_stats GROUP BY asset_id;
"#;

fn make_order(i: usize, num_traders: usize) -> RowMap {
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
        (
            1_000_000_000u64 * price_bps as u64 / 10_000,
            1_000_000_000u64,
        )
    } else {
        let price_bps = 3000 + (i % 6000);
        (
            1_000_000_000u64 * price_bps as u64 / 10_000,
            1_000_000_000u64,
        )
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
    let n = 200_000;
    let batch = 500;
    let traders = 100_000;
    let rows: Vec<RowMap> = (0..n).map(|i| make_order(i, traders)).collect();

    println!(
        "=== Pipeline Breakdown ({}K rows, batch={}) ===\n",
        n / 1000,
        batch
    );

    // 1. Measure RowMap creation overhead (already done above, but for reference)
    let start = Instant::now();
    let _rows2: Vec<RowMap> = (0..n).map(|i| make_order(i, traders)).collect();
    let data_gen = start.elapsed();
    println!(
        "  Data generation:     {:>7.1}ms  ({:.1}us/row)",
        data_gen.as_secs_f64() * 1000.0,
        data_gen.as_secs_f64() * 1_000_000.0 / n as f64
    );

    // 2. Isolated reducer: market_stats only (no raw table, no MV)
    {
        let schema = parse_schema(MARKET_STATS_ONLY).unwrap();
        let storage = Arc::new(MemoryBackend::new());
        let reducer_def = schema.reducers[0].clone();
        let source_registry = ColumnRegistry::new(
            schema.tables[0]
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect(),
        );
        let mut engine = ReducerEngine::new(reducer_def, storage, &source_registry, &[]);

        // Convert RowMaps to Rows for reducer input
        let registry = Arc::new(source_registry);
        let typed_rows: Vec<Row> = rows
            .iter()
            .map(|m| Row::from_map(registry.clone(), m))
            .collect();

        let start = Instant::now();
        for (block, chunk) in typed_rows.chunks(batch).enumerate() {
            engine.process_block(block as u64, chunk).unwrap();
        }
        let elapsed = start.elapsed();
        println!(
            "  Reducer only (market_stats): {:>7.1}ms  ({:.1}us/row, {:.0}K/s)",
            elapsed.as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1_000_000.0 / n as f64,
            n as f64 / elapsed.as_secs_f64() / 1000.0
        );
    }

    // 3. Full pipeline: market_stats only (raw + reducer + MV)
    {
        let cfg = Config::new(MARKET_STATS_ONLY);
        let mut db = DeltaDb::open(cfg).unwrap();

        let start = Instant::now();
        for (block, chunk) in rows.chunks(batch).enumerate() {
            db.process_batch("orders", block as u64, chunk.to_vec())
                .unwrap();
        }
        db.flush();
        let elapsed = start.elapsed();
        println!(
            "  Pipeline (market_stats+MV): {:>7.1}ms  ({:.1}us/row, {:.0}K/s)",
            elapsed.as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1_000_000.0 / n as f64,
            n as f64 / elapsed.as_secs_f64() / 1000.0
        );
    }

    // 4. Full pipeline: both reducers + both MVs
    {
        let cfg = Config::new(FULL_SCHEMA);
        let mut db = DeltaDb::open(cfg).unwrap();

        let start = Instant::now();
        for (block, chunk) in rows.chunks(batch).enumerate() {
            db.process_batch("orders", block as u64, chunk.to_vec())
                .unwrap();
        }
        db.flush();
        let elapsed = start.elapsed();
        println!(
            "  Pipeline (full):     {:>7.1}ms  ({:.1}us/row, {:.0}K/s)",
            elapsed.as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1_000_000.0 / n as f64,
            n as f64 / elapsed.as_secs_f64() / 1000.0
        );
    }

    // 5. Measure process_batch overhead: Vec clone + Row conversion
    {
        let cfg = Config::new(FULL_SCHEMA);
        let mut db = DeltaDb::open(cfg).unwrap();

        // Measure just the chunk.to_vec() overhead
        let start = Instant::now();
        for chunk in rows.chunks(batch) {
            let _ = chunk.to_vec();
        }
        let elapsed = start.elapsed();
        println!(
            "\n  Vec<RowMap> clone:    {:>7.1}ms  ({:.1}us/row)",
            elapsed.as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1_000_000.0 / n as f64
        );

        // Measure the full process_batch with a raw-only schema (no reducers, no MVs)
        let raw_schema = r#"
            CREATE VIRTUAL TABLE orders (
                block_number UInt64, timestamp UInt64, trader String,
                asset_id String, usdc UInt64, shares UInt64, side UInt64
            );
        "#;
        let cfg = Config::new(raw_schema);
        let mut db = DeltaDb::open(cfg).unwrap();

        let start = Instant::now();
        for (block, chunk) in rows.chunks(batch).enumerate() {
            db.process_batch("orders", block as u64, chunk.to_vec())
                .unwrap();
        }
        db.flush();
        let elapsed = start.elapsed();
        println!(
            "  Raw table only (virtual): {:>7.1}ms  ({:.1}us/row, {:.0}K/s)",
            elapsed.as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1_000_000.0 / n as f64,
            n as f64 / elapsed.as_secs_f64() / 1000.0
        );

        // Non-virtual raw table
        let raw_schema2 = r#"
            CREATE TABLE orders (
                block_number UInt64, timestamp UInt64, trader String,
                asset_id String, usdc UInt64, shares UInt64, side UInt64
            );
        "#;
        let cfg = Config::new(raw_schema2);
        let mut db = DeltaDb::open(cfg).unwrap();

        let start = Instant::now();
        for (block, chunk) in rows.chunks(batch).enumerate() {
            db.process_batch("orders", block as u64, chunk.to_vec())
                .unwrap();
        }
        db.flush();
        let elapsed = start.elapsed();
        println!(
            "  Raw table only (stored):  {:>7.1}ms  ({:.1}us/row, {:.0}K/s)",
            elapsed.as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1_000_000.0 / n as f64,
            n as f64 / elapsed.as_secs_f64() / 1000.0
        );
    }

    // 6. String formatting overhead (trader/asset_id generation dominates data creation)
    {
        let start = Instant::now();
        for i in 0..n {
            let _ = format!("0xtrader{:06x}", i % 10_000);
            let _ = format!("token_{:04}", i % 10_000);
        }
        let elapsed = start.elapsed();
        println!(
            "\n  String formatting:   {:>7.1}ms  ({:.1}us/row)",
            elapsed.as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1_000_000.0 / n as f64
        );
    }

    println!("\n=== Done ===");
}
