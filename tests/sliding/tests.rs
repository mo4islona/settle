//! Integration tests for sliding window materialized views.
//!
//! Tests the full pipeline through DeltaDb: schema parsing, raw table ingest,
//! sliding window MV processing with expiry, rollback, and delta emission.

use std::collections::HashMap;

use delta_db::db::{Config, DeltaDb};
use delta_db::types::{DeltaBatch, DeltaOperation, DeltaRecord, RowMap, Value};

const SCHEMA: &str = include_str!("schema.sql");

fn make_trade(pair: &str, volume: f64, price: f64, block_time_ms: i64) -> RowMap {
    HashMap::from([
        ("pair".to_string(), Value::String(pair.to_string())),
        ("volume".to_string(), Value::Float64(volume)),
        ("price".to_string(), Value::Float64(price)),
        ("block_time".to_string(), Value::DateTime(block_time_ms)),
    ])
}

fn records_for_table<'a>(batch: &'a DeltaBatch, table: &str) -> Vec<&'a DeltaRecord> {
    batch
        .tables
        .get(table)
        .map(|v| v.iter().collect())
        .unwrap_or_default()
}

#[test]
fn schema_parses() {
    DeltaDb::open(Config::new(SCHEMA)).unwrap();
}

#[test]
fn sliding_window_basic_volume_tracking() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Block 1: ETH trade at t=0
    db.process_batch("trades", 1, vec![make_trade("ETH", 100.0, 2000.0, 0)])
        .unwrap();

    let batch = db.flush().unwrap();
    let vol_recs = records_for_table(&batch, "volume_1h");
    let eth_insert = vol_recs
        .iter()
        .find(|r| {
            r.operation == DeltaOperation::Insert
                && r.key.get("pair") == Some(&Value::String("ETH".into()))
        })
        .expect("should have ETH insert in volume_1h");
    assert_eq!(
        eth_insert.values.get("total_volume"),
        Some(&Value::Float64(100.0))
    );
    assert_eq!(
        eth_insert.values.get("trade_count"),
        Some(&Value::UInt64(1))
    );
}

#[test]
fn sliding_window_expiry_reduces_volume() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Block 1: ETH trade at t=0, volume=100
    db.process_batch("trades", 1, vec![make_trade("ETH", 100.0, 2000.0, 0)])
        .unwrap();
    db.flush().unwrap();

    // Block 2: ETH trade at t=30min, volume=200
    db.process_batch(
        "trades",
        2,
        vec![make_trade("ETH", 200.0, 2100.0, 1_800_000)],
    )
    .unwrap();
    db.flush().unwrap();

    // Block 3: ETH trade at t=1hr+1s, volume=50
    // This should expire block 1 (ts=0 < cutoff=1000)
    db.process_batch(
        "trades",
        3,
        vec![make_trade("ETH", 50.0, 2200.0, 3_601_000)],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let vol_recs = records_for_table(&batch, "volume_1h");

    // Should have an Update for ETH with volume=200+50=250 (block 1 expired)
    let eth_update = vol_recs
        .iter()
        .find(|r| r.key.get("pair") == Some(&Value::String("ETH".into())))
        .expect("should have ETH delta in volume_1h");
    assert_eq!(eth_update.operation, DeltaOperation::Update);
    assert_eq!(
        eth_update.values.get("total_volume"),
        Some(&Value::Float64(250.0))
    );
    assert_eq!(
        eth_update.values.get("trade_count"),
        Some(&Value::UInt64(2))
    );

    // Meanwhile, the unbounded "totals" MV should still have all data
    let total_recs = records_for_table(&batch, "totals");
    let eth_total = total_recs
        .iter()
        .find(|r| r.key.get("pair") == Some(&Value::String("ETH".into())))
        .expect("should have ETH delta in totals");
    assert_eq!(
        eth_total.values.get("total_volume"),
        Some(&Value::Float64(350.0))
    ); // 100+200+50
}

#[test]
fn sliding_window_rollback_and_reprocess() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Block 1-3: normal processing
    db.process_batch("trades", 1, vec![make_trade("ETH", 100.0, 2000.0, 0)])
        .unwrap();
    db.process_batch(
        "trades",
        2,
        vec![make_trade("ETH", 200.0, 2100.0, 1_000_000)],
    )
    .unwrap();
    db.process_batch(
        "trades",
        3,
        vec![make_trade("ETH", 300.0, 2200.0, 2_000_000)],
    )
    .unwrap();
    db.flush().unwrap();

    // Rollback to block 1
    db.rollback(1).unwrap();
    let rb_batch = db.flush().unwrap();

    let vol_recs = records_for_table(&rb_batch, "volume_1h");
    let eth_rb = vol_recs
        .iter()
        .find(|r| r.key.get("pair") == Some(&Value::String("ETH".into())))
        .expect("should have ETH rollback delta");
    assert_eq!(eth_rb.operation, DeltaOperation::Update);
    assert_eq!(
        eth_rb.values.get("total_volume"),
        Some(&Value::Float64(100.0))
    );

    // Re-ingest block 2 with different data
    db.process_batch(
        "trades",
        2,
        vec![make_trade("ETH", 50.0, 1900.0, 1_500_000)],
    )
    .unwrap();
    let new_batch = db.flush().unwrap();

    let vol_recs = records_for_table(&new_batch, "volume_1h");
    let eth_new = vol_recs
        .iter()
        .find(|r| r.key.get("pair") == Some(&Value::String("ETH".into())))
        .expect("should have ETH update after re-ingest");
    assert_eq!(
        eth_new.values.get("total_volume"),
        Some(&Value::Float64(150.0))
    );
}

#[test]
fn sliding_window_multiple_groups_and_windows() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Block 1: trades for ETH and BTC at t=0
    db.process_batch(
        "trades",
        1,
        vec![
            make_trade("ETH", 100.0, 2000.0, 0),
            make_trade("BTC", 1.0, 50000.0, 0),
        ],
    )
    .unwrap();

    // Block 2: more trades at t=20min
    db.process_batch(
        "trades",
        2,
        vec![
            make_trade("ETH", 200.0, 2100.0, 1_200_000),
            make_trade("BTC", 2.0, 51000.0, 1_200_000),
        ],
    )
    .unwrap();

    // Block 3: at t=31min → 30-min window (stats_30m) expires block 1,
    //          but 1-hour window (volume_1h) keeps everything
    db.process_batch(
        "trades",
        3,
        vec![
            make_trade("ETH", 50.0, 2200.0, 1_860_001), // 31 min + 1ms
        ],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    // volume_1h: nothing expired (all within 1 hour)
    let vol_recs = records_for_table(&batch, "volume_1h");
    let eth_vol = vol_recs
        .iter()
        .find(|r| r.key.get("pair") == Some(&Value::String("ETH".into())))
        .expect("ETH volume_1h");
    assert_eq!(
        eth_vol.values.get("total_volume"),
        Some(&Value::Float64(350.0))
    );

    // stats_30m: block 1 expired for ETH and BTC
    let stats_recs = records_for_table(&batch, "stats_30m");
    let eth_stats = stats_recs
        .iter()
        .find(|r| r.key.get("pair") == Some(&Value::String("ETH".into())))
        .expect("ETH stats_30m");
    // ETH: blocks 2+3 remain (prices: 2100, 2200; volumes: 200, 50)
    assert_eq!(
        eth_stats.values.get("vol_sum"),
        Some(&Value::Float64(250.0))
    );
    assert_eq!(
        eth_stats.values.get("price_min"),
        Some(&Value::Float64(2100.0))
    );
    assert_eq!(
        eth_stats.values.get("price_max"),
        Some(&Value::Float64(2200.0))
    );
    assert_eq!(
        eth_stats.values.get("price_first"),
        Some(&Value::Float64(2100.0))
    );
    assert_eq!(
        eth_stats.values.get("price_last"),
        Some(&Value::Float64(2200.0))
    );
}

#[test]
fn sliding_window_complete_expiry_deletes_group() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Block 1: single trade for DOGE at t=0
    db.process_batch("trades", 1, vec![make_trade("DOGE", 1000.0, 0.1, 0)])
        .unwrap();
    db.flush().unwrap();

    // Block 2: trade for ETH at t=1hr+1s → DOGE group fully expires in volume_1h
    db.process_batch(
        "trades",
        2,
        vec![make_trade("ETH", 100.0, 2000.0, 3_601_000)],
    )
    .unwrap();
    let batch = db.flush().unwrap();

    let vol_recs = records_for_table(&batch, "volume_1h");

    // DOGE should be deleted
    let doge_delete = vol_recs
        .iter()
        .find(|r| r.key.get("pair") == Some(&Value::String("DOGE".into())));
    assert!(
        doge_delete
            .map(|d| d.operation == DeltaOperation::Delete)
            .unwrap_or(false),
        "DOGE should be deleted from volume_1h"
    );

    // ETH should be inserted
    let eth_insert = vol_recs
        .iter()
        .find(|r| r.key.get("pair") == Some(&Value::String("ETH".into())));
    assert!(eth_insert.is_some());
    assert_eq!(eth_insert.unwrap().operation, DeltaOperation::Insert);
}

#[test]
fn sliding_window_with_finalization() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Process several blocks
    for i in 0..5u64 {
        db.process_batch(
            "trades",
            i + 1,
            vec![make_trade(
                "ETH",
                (i + 1) as f64 * 10.0,
                2000.0,
                (i * 600_000) as i64,
            )],
        )
        .unwrap();
    }

    // Finalize block 3
    db.finalize(3).unwrap();
    db.flush().unwrap();

    // Process more blocks that trigger expiry
    // Block 6 at t=1hr+1s
    db.process_batch(
        "trades",
        6,
        vec![make_trade("ETH", 100.0, 2500.0, 3_601_000)],
    )
    .unwrap();
    let batch = db.flush().unwrap();

    let vol_recs = records_for_table(&batch, "volume_1h");
    let eth = vol_recs
        .iter()
        .find(|r| r.key.get("pair") == Some(&Value::String("ETH".into())))
        .expect("ETH should have delta");
    assert_eq!(eth.operation, DeltaOperation::Update);

    // Block 1 (ts=0) should be expired. Volume = blocks 2-6.
    // Block 2: vol=20 (ts=600k), Block 3: vol=30 (ts=1200k), Block 4: vol=40 (ts=1800k),
    // Block 5: vol=50 (ts=2400k), Block 6: vol=100 (ts=3601k)
    // cutoff = 3601000 - 3600000 = 1000. Block 1 (ts=0) < 1000 → expired.
    assert_eq!(eth.values.get("total_volume"), Some(&Value::Float64(240.0)));
}
