//! Polymarket insider detection pipeline — integration tests.
//!
//! Tests the full schema from tests/polymarket/schema.sql:
//! - orders (raw table)
//! - market_stats (Lua reducer, GROUP BY asset_id) → token_summary (MV)
//! - insider_classifier (Lua reducer, GROUP BY trader) → insider_positions (MV)

use std::collections::HashMap;

use delta_db::db::{Config, DeltaDb};
use delta_db::types::{DeltaOperation, RowMap, Value};

const SCHEMA: &str = include_str!("schema.sql");

fn make_order(
    trader: &str,
    asset_id: &str,
    usdc: u64,
    shares: u64,
    side: u64,
    timestamp: u64,
) -> RowMap {
    HashMap::from([
        ("trader".to_string(), Value::String(trader.to_string())),
        ("asset_id".to_string(), Value::String(asset_id.to_string())),
        ("usdc".to_string(), Value::UInt64(usdc)),
        ("shares".to_string(), Value::UInt64(shares)),
        ("side".to_string(), Value::UInt64(side)),
        ("timestamp".to_string(), Value::UInt64(timestamp)),
    ])
}

#[test]
fn schema_parses() {
    DeltaDb::open(Config::new(SCHEMA)).unwrap();
}

#[test]
fn market_stats_tracks_volume_and_price_stats() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Two trades on the same token, both price = 0.5
    db.process_batch(
        "orders",
        1000,
        vec![
            // 100 USDC for 200 shares → price = 0.5, vol = 100
            make_order("alice", "token_a", 100_000_000, 200_000_000, 0, 1000),
            // 50 USDC for 100 shares → price = 0.5, vol = 50
            make_order("bob", "token_a", 50_000_000, 100_000_000, 1, 1001),
        ],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    let mv_records: Vec<_> = batch.records_for("token_summary").iter().collect();

    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].operation, DeltaOperation::Insert);
    assert_eq!(
        mv_records[0].values.get("trade_count"),
        Some(&Value::UInt64(2))
    );

    // last_price should be the second trade's price (0.5)
    let last_price = mv_records[0]
        .values
        .get("last_price")
        .unwrap()
        .as_f64()
        .unwrap();
    assert!((last_price - 0.5).abs() < 0.001);

    // sum_price = 0.5 + 0.5 = 1.0
    let sum_price = mv_records[0]
        .values
        .get("sum_price")
        .unwrap()
        .as_f64()
        .unwrap();
    assert!((sum_price - 1.0).abs() < 0.001);

    // sum_price_sq = 0.25 + 0.25 = 0.5
    let sum_price_sq = mv_records[0]
        .values
        .get("sum_price_sq")
        .unwrap()
        .as_f64()
        .unwrap();
    assert!((sum_price_sq - 0.5).abs() < 0.001);
}

#[test]
fn market_stats_different_prices() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    db.process_batch(
        "orders",
        1000,
        vec![
            // price = 0.4
            make_order("alice", "token_a", 400_000_000, 1_000_000_000, 0, 1000),
            // price = 0.6
            make_order("bob", "token_a", 600_000_000, 1_000_000_000, 1, 1001),
        ],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let mv = batch.records_for("token_summary").first().unwrap();

    let last_price = mv.values.get("last_price").unwrap().as_f64().unwrap();
    assert!((last_price - 0.6).abs() < 0.001);

    // sum_price = 0.4 + 0.6 = 1.0, mean = 0.5
    let sum_price = mv.values.get("sum_price").unwrap().as_f64().unwrap();
    assert!((sum_price - 1.0).abs() < 0.001);

    // sum_price_sq = 0.16 + 0.36 = 0.52
    // variance = 0.52/2 - 0.5^2 = 0.26 - 0.25 = 0.01, stddev = 0.1
    let sum_price_sq = mv.values.get("sum_price_sq").unwrap().as_f64().unwrap();
    assert!((sum_price_sq - 0.52).abs() < 0.001);
}

#[test]
fn market_stats_skips_zero_shares() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    db.process_batch(
        "orders",
        1000,
        vec![
            make_order("alice", "token_a", 100, 0, 0, 1000), // zero shares → skipped
        ],
    )
    .unwrap();

    // Virtual table produces no raw deltas, and zero-shares means no reducer
    // output either, so flush returns None (empty buffer).
    assert!(db.flush().is_none());
}

#[test]
fn insider_classifier_ignores_sell_side() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // side=1 (SELL) — should be ignored by insider_classifier
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "alice",
            "token_a",
            5_000_000_000, // $5000 — above threshold
            10_000_000_000,
            1, // SELL
            1000,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    assert!(insider_records.is_empty());
}

#[test]
fn insider_classifier_ignores_high_price() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Price = usdc/shares = 0.96 (above 0.95 threshold)
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "alice",
            "token_a",
            9_600_000_000, // $9600
            10_000_000_000,
            0, // BUY
            1000,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    assert!(insider_records.is_empty());
}

#[test]
fn insider_detected_with_full_fields() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Trader buys $5000 in a single order (above $4000 threshold)
    // price = 0.5 (well below 0.95), BUY side
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "whale",
            "token_a",
            5_000_000_000, // $5000 USDC (6 decimals)
            10_000_000_000,
            0,    // BUY
            1000, // timestamp
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    assert_eq!(insider_records.len(), 1);
    let rec = &insider_records[0];
    assert_eq!(rec.values.get("trade_count"), Some(&Value::UInt64(1)));

    // sum_price should be the price (0.5)
    let sum_price = rec.values.get("sum_price").unwrap().as_f64().unwrap();
    assert!((sum_price - 0.5).abs() < 0.001);

    // sum_price_sq = 0.25
    let sum_price_sq = rec.values.get("sum_price_sq").unwrap().as_f64().unwrap();
    assert!((sum_price_sq - 0.25).abs() < 0.001);

    // first_seen = last_seen = 1000
    assert_eq!(rec.values.get("first_seen"), Some(&Value::Int64(1000)));
    assert_eq!(rec.values.get("last_seen"), Some(&Value::Int64(1000)));

    // detected_at = 1000
    assert_eq!(rec.values.get("detected_at"), Some(&Value::Int64(1000)));
}

#[test]
fn insider_accumulates_across_orders_in_window() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Two orders from same trader within 15 min, total > $4000
    db.process_batch(
        "orders",
        1000,
        vec![
            // $2500 @ price=0.5
            make_order("trader_x", "token_a", 2_500_000_000, 5_000_000_000, 0, 1000),
            // $2000 @ price=0.4 → total $4500 > $4000 threshold
            make_order("trader_x", "token_b", 2_000_000_000, 5_000_000_000, 0, 1100),
        ],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    // Multi-emit: should emit positions for both token_a and token_b
    assert_eq!(insider_records.len(), 2);

    let total_count: u64 = insider_records
        .iter()
        .map(|r| r.values.get("trade_count").unwrap().as_u64().unwrap())
        .sum();
    assert_eq!(total_count, 2); // one emit per token

    // Both should have detected_at = 1100 (timestamp when threshold was crossed)
    for rec in &insider_records {
        let detected_at = rec.values.get("detected_at").unwrap().as_f64().unwrap();
        assert!((detected_at - 1100.0).abs() < 0.001);
    }
}

#[test]
fn insider_subsequent_orders_emit_with_timestamps() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // First order: triggers insider classification ($5000 > $4000) at t=1000
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "whale",
            "token_a",
            5_000_000_000,
            10_000_000_000,
            0,
            1000,
        )],
    )
    .unwrap();
    db.flush();

    // Second order: known insider buys on a different token at t=2000
    db.process_batch(
        "orders",
        1001,
        vec![make_order(
            "whale",
            "token_b",
            1_000_000_000,
            2_000_000_000,
            0,
            2000,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    assert!(!insider_records.is_empty());

    // Find the token_b record
    let token_b_rec = insider_records
        .iter()
        .find(|r| {
            r.key
                .get("asset_id")
                .map_or(false, |v| v == &Value::String("token_b".into()))
        })
        .expect("should have token_b record");

    // first_seen = last_seen = 2000 (only one order on this token)
    let first_seen = token_b_rec
        .values
        .get("first_seen")
        .unwrap()
        .as_f64()
        .unwrap();
    assert!((first_seen - 2000.0).abs() < 0.001);

    // detected_at = 2000 (path 2: known insider, detected_at = order timestamp)
    let detected_at = token_b_rec
        .values
        .get("detected_at")
        .unwrap()
        .as_f64()
        .unwrap();
    assert!((detected_at - 2000.0).abs() < 0.001);
}

#[test]
fn window_expiration_marks_clean() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // First order: $2000 (below threshold), starts window
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "slow_trader",
            "token_a",
            2_000_000_000,
            4_000_000_000,
            0,
            1000,
        )],
    )
    .unwrap();
    db.flush();

    // Second order: 20 minutes later (> 15 min window) → should classify as clean
    db.process_batch(
        "orders",
        1001,
        vec![make_order(
            "slow_trader",
            "token_a",
            3_000_000_000,
            6_000_000_000,
            0,
            2200, // 1000 + 1200s = 20 minutes
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    // Clean trader → no insider positions emitted
    assert!(insider_records.is_empty());

    // Third order: even more volume — should still be clean (no re-classification)
    db.process_batch(
        "orders",
        1002,
        vec![make_order(
            "slow_trader",
            "token_a",
            10_000_000_000,
            20_000_000_000,
            0,
            2300,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    assert!(insider_records.is_empty());
}

#[test]
fn rollback_undoes_insider_classification() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Block 1000: small order (below threshold)
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "maybe_insider",
            "token_a",
            1_000_000_000,
            2_000_000_000,
            0,
            1000,
        )],
    )
    .unwrap();
    db.flush();

    // Block 1001: big order pushes over threshold → insider detected
    db.process_batch(
        "orders",
        1001,
        vec![make_order(
            "maybe_insider",
            "token_a",
            4_000_000_000,
            8_000_000_000,
            0,
            1100,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let insider_count = batch.records_for("insider_positions").len();
    assert!(insider_count > 0, "insider should be detected");

    // Rollback block 1001 — classification should be undone
    db.rollback(1000).unwrap();
    db.flush();

    // Re-process block 1001 with a small order (stays below threshold)
    db.process_batch(
        "orders",
        1001,
        vec![make_order(
            "maybe_insider",
            "token_a",
            500_000_000, // only $500
            1_000_000_000,
            0,
            1100,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    // No insider classification with the smaller order
    let insider_records: Vec<_> = batch
        .records_for("insider_positions")
        .iter()
        .filter(|r| r.operation == DeltaOperation::Insert)
        .collect();
    assert!(
        insider_records.is_empty(),
        "no insider positions after rollback + small order"
    );
}

#[test]
fn market_stats_rollback() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "alice",
            "token_a",
            100_000_000,
            200_000_000,
            0,
            1000,
        )],
    )
    .unwrap();

    db.process_batch(
        "orders",
        1001,
        vec![make_order(
            "bob",
            "token_a",
            200_000_000,
            400_000_000,
            0,
            1001,
        )],
    )
    .unwrap();
    db.flush();

    // Rollback block 1001
    db.rollback(1000).unwrap();

    let batch = db.flush().unwrap();

    let mv_records: Vec<_> = batch.records_for("token_summary").iter().collect();

    assert_eq!(mv_records.len(), 1);
    assert_eq!(mv_records[0].operation, DeltaOperation::Update);
    // Only 1 trade left
    assert_eq!(
        mv_records[0].values.get("trade_count"),
        Some(&Value::UInt64(1))
    );
    // last_price should be the remaining trade's price (0.5)
    let last_price = mv_records[0]
        .values
        .get("last_price")
        .unwrap()
        .as_f64()
        .unwrap();
    assert!((last_price - 0.5).abs() < 0.001);
}

#[test]
fn full_pipeline_both_reducers() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Multiple orders in one block from different traders
    db.process_batch(
        "orders",
        1000,
        vec![
            // Insider: $5000 BUY at price=0.5
            make_order(
                "insider_1",
                "token_a",
                5_000_000_000,
                10_000_000_000,
                0,
                1000,
            ),
            // Normal: $100 BUY at price=0.5
            make_order("normal_1", "token_a", 100_000_000, 200_000_000, 0, 1000),
            // SELL: goes to market_stats but not insider_classifier
            make_order("seller_1", "token_a", 300_000_000, 600_000_000, 1, 1000),
        ],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    // token_summary should have 3 trades for token_a
    let token_records: Vec<_> = batch.records_for("token_summary").iter().collect();
    assert_eq!(token_records.len(), 1);
    assert_eq!(
        token_records[0].values.get("trade_count"),
        Some(&Value::UInt64(3))
    );
    // All three trades have price=0.5, so sum_price=1.5, sum_price_sq=0.75
    let sum_price = token_records[0]
        .values
        .get("sum_price")
        .unwrap()
        .as_f64()
        .unwrap();
    assert!((sum_price - 1.5).abs() < 0.001);

    // insider_positions should have 1 entry (only insider_1 exceeded threshold)
    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();
    assert_eq!(insider_records.len(), 1);
    // Verify it has all required fields
    assert!(insider_records[0].values.contains_key("sum_price"));
    assert!(insider_records[0].values.contains_key("sum_price_sq"));
    assert!(insider_records[0].values.contains_key("first_seen"));
    assert!(insider_records[0].values.contains_key("last_seen"));
    assert!(insider_records[0].values.contains_key("detected_at"));
}

// ── Tests ported from polygains-main reference implementation ───────

/// Ported from pipe.test.ts: "marks sub-4k trader as non-insider when only
/// post-window trades push total above threshold"
///
/// Trader has $3k in the first 15-min window, then $2k after the window expires.
/// Total exceeds $4k but the window expired → classified as clean, not insider.
#[test]
fn post_window_trades_do_not_trigger_insider() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Batch 1: $3000 at t=1000 (starts window)
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "0x1111",
            "token_a",
            3_000_000_000, // $3k
            3_200_000_000, // price = 0.9375 (< 0.95)
            0,             // BUY
            1000,
        )],
    )
    .unwrap();
    db.flush();

    // Batch 2: $2000 at t=2000 (1000s > 900s window) → window expired
    db.process_batch(
        "orders",
        1001,
        vec![make_order(
            "0x1111",
            "token_a",
            2_000_000_000, // $2k
            2_200_000_000, // price ~= 0.909 (< 0.95)
            0,             // BUY
            2000,          // 1000s later
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    // Window expired → clean → no insider emission
    assert!(
        insider_records.is_empty(),
        "post-window trades must not trigger insider classification"
    );
}

/// Ported from pipe.test.ts: "only tracks BUY orders priced below 0.95"
///
/// Two traders in the same batch: one with price=1.0 (filtered), one with
/// price=0.9 (tracked). Both above $4k threshold on BUY side.
/// Only the low-price trader should be classified as insider.
#[test]
fn price_filter_high_vs_low_in_same_batch() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    db.process_batch(
        "orders",
        1000,
        vec![
            // High price trader: price = 1.0 → filtered out
            make_order(
                "high_price_trader",
                "token_a",
                5_000_000_000, // $5k
                5_000_000_000, // price = 1.0
                0,             // BUY
                1000,
            ),
            // Low price trader: price = 0.5 → tracked
            make_order(
                "low_price_trader",
                "token_a",
                5_000_000_000,  // $5k
                10_000_000_000, // price = 0.5
                0,              // BUY
                1000,
            ),
        ],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    // Only low_price_trader should appear
    assert_eq!(insider_records.len(), 1);
    assert_eq!(
        insider_records[0].key.get("trader"),
        Some(&Value::String("low_price_trader".into()))
    );
}

/// Ported from market-stats.integration.test.ts: "persists stats for both
/// outcomes with multiple matched fills per side"
///
/// SELL-side fills must contribute to market stats. Both yes/no tokens get
/// independent stats. Verifies mean and stddev can be derived from deltas.
#[test]
fn market_stats_both_sides_multiple_tokens_with_derivation() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    db.process_batch(
        "orders",
        1000,
        vec![
            // YES token: two SELL fills
            // price = 350000/1000000 = 0.35
            make_order("seller_1", "yes_token", 350_000, 1_000_000, 1, 1000),
            // price = 420000/1000000 = 0.42
            make_order("seller_2", "yes_token", 420_000, 1_000_000, 1, 1001),
            // NO token: two BUY fills
            // price = 960000/1000000 = 0.96
            make_order("buyer_1", "no_token", 960_000, 1_000_000, 0, 1002),
            // price = 910000/1000000 = 0.91
            make_order("buyer_2", "no_token", 910_000, 1_000_000, 0, 1003),
        ],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let mv_records: Vec<_> = batch.records_for("token_summary").iter().collect();

    // Two tokens → two MV records
    assert_eq!(mv_records.len(), 2);

    let yes = mv_records
        .iter()
        .find(|r| r.key.get("asset_id") == Some(&Value::String("yes_token".into())))
        .expect("should have yes_token record");
    let no = mv_records
        .iter()
        .find(|r| r.key.get("asset_id") == Some(&Value::String("no_token".into())))
        .expect("should have no_token record");

    // YES token: 2 trades
    assert_eq!(yes.values.get("trade_count"), Some(&Value::UInt64(2)));
    // NO token: 2 trades
    assert_eq!(no.values.get("trade_count"), Some(&Value::UInt64(2)));

    // YES token: mean = (0.35 + 0.42) / 2 = 0.385
    let yes_sum_price = yes.values.get("sum_price").unwrap().as_f64().unwrap();
    let yes_count = yes.values.get("trade_count").unwrap().as_u64().unwrap() as f64;
    let yes_mean = yes_sum_price / yes_count;
    assert!(
        (yes_mean - 0.385).abs() < 0.001,
        "yes_token mean should be ~0.385, got {}",
        yes_mean
    );

    // YES token: stddev = sqrt(sum_price_sq/n - mean^2)
    let yes_sum_price_sq = yes.values.get("sum_price_sq").unwrap().as_f64().unwrap();
    let yes_variance = yes_sum_price_sq / yes_count - yes_mean * yes_mean;
    let yes_stddev = yes_variance.sqrt();
    assert!(
        yes_stddev > 0.0,
        "yes_token stddev should be positive, got {}",
        yes_stddev
    );
    // stddev = sqrt((0.35^2 + 0.42^2)/2 - 0.385^2) = sqrt(0.14945 - 0.148225) = sqrt(0.001225) ≈ 0.035
    assert!(
        (yes_stddev - 0.035).abs() < 0.001,
        "yes_token stddev should be ~0.035, got {}",
        yes_stddev
    );

    // NO token: mean = (0.96 + 0.91) / 2 = 0.935
    let no_sum_price = no.values.get("sum_price").unwrap().as_f64().unwrap();
    let no_count = no.values.get("trade_count").unwrap().as_u64().unwrap() as f64;
    let no_mean = no_sum_price / no_count;
    assert!(
        (no_mean - 0.935).abs() < 0.001,
        "no_token mean should be ~0.935, got {}",
        no_mean
    );

    // NO token: stddev
    let no_sum_price_sq = no.values.get("sum_price_sq").unwrap().as_f64().unwrap();
    let no_variance = no_sum_price_sq / no_count - no_mean * no_mean;
    let no_stddev = no_variance.sqrt();
    assert!(
        no_stddev > 0.0,
        "no_token stddev should be positive, got {}",
        no_stddev
    );
}

/// Virtual table test: orders should NOT appear in delta output.
#[test]
fn virtual_table_suppresses_order_deltas() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "alice",
            "token_a",
            100_000_000,
            200_000_000,
            0,
            1000,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();

    // No raw "orders" records should appear in deltas
    let order_records: Vec<_> = batch.records_for("orders").iter().collect();
    assert!(
        order_records.is_empty(),
        "virtual table should not emit delta records"
    );

    // But market_stats → token_summary should still work
    let mv_records: Vec<_> = batch.records_for("token_summary").iter().collect();
    assert_eq!(mv_records.len(), 1);
}

/// Multiple insiders detected in the same block.
#[test]
fn multiple_insiders_same_block() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    db.process_batch(
        "orders",
        1000,
        vec![
            // Insider 1: $5000
            make_order("whale_a", "token_a", 5_000_000_000, 10_000_000_000, 0, 1000),
            // Insider 2: $6000
            make_order("whale_b", "token_b", 6_000_000_000, 12_000_000_000, 0, 1000),
            // Normal trader: $100 (below threshold)
            make_order("minnow", "token_a", 100_000_000, 200_000_000, 0, 1000),
        ],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    // Two insiders detected
    assert_eq!(insider_records.len(), 2);

    let traders: Vec<&str> = insider_records
        .iter()
        .map(|r| r.key.get("trader").unwrap().as_str().unwrap())
        .collect();
    assert!(traders.contains(&"whale_a"));
    assert!(traders.contains(&"whale_b"));
}

/// Exact threshold boundary: $4000 exactly should trigger insider.
#[test]
fn exact_threshold_triggers_insider() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Exactly $4000 (4_000_000_000 raw USDC with 6 decimals)
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "borderline",
            "token_a",
            4_000_000_000,
            8_000_000_000, // price = 0.5
            0,
            1000,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    assert_eq!(
        insider_records.len(),
        1,
        "exactly $4000 should trigger insider classification"
    );
}

/// Just below threshold: $3999.99 should NOT trigger.
#[test]
fn just_below_threshold_no_insider() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // $3999.99 = 3_999_990_000 raw USDC
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "almost",
            "token_a",
            3_999_990_000,
            7_999_980_000, // price = 0.5
            0,
            1000,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    assert!(
        insider_records.is_empty(),
        "$3999.99 should not trigger insider"
    );
}

/// Exact price boundary: price = 0.95 should be filtered out.
/// The condition is: usdc * BPS_SCALE >= shares * MIN_PRICE_BPS
/// At price=0.95: usdc/shares = 0.95, so usdc*10000 = shares*9500 → filtered
#[test]
fn exact_price_boundary_filtered() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // price = 9500/10000 = 0.95 exactly
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "boundary",
            "token_a",
            9_500_000_000, // price = 0.95
            10_000_000_000,
            0,
            1000,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    assert!(
        insider_records.is_empty(),
        "price=0.95 exactly should be filtered out of insider detection"
    );
}

/// Rollback across multiple blocks restores correct state.
/// Block 1000: trade A. Block 1001: trade B. Block 1002: trade C.
/// Rollback to 1000 → only trade A remains.
#[test]
fn multi_block_rollback() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "alice",
            "token_a",
            100_000_000,
            200_000_000,
            0,
            1000,
        )],
    )
    .unwrap();
    db.flush();

    db.process_batch(
        "orders",
        1001,
        vec![make_order(
            "bob",
            "token_a",
            200_000_000,
            400_000_000,
            0,
            1001,
        )],
    )
    .unwrap();
    db.flush();

    db.process_batch(
        "orders",
        1002,
        vec![make_order(
            "charlie",
            "token_a",
            300_000_000,
            600_000_000,
            0,
            1002,
        )],
    )
    .unwrap();
    db.flush();

    // Rollback blocks 1001 and 1002
    db.rollback(1000).unwrap();
    let batch = db.flush().unwrap();

    let mv = batch
        .records_for("token_summary")
        .first()
        .expect("should have token_summary after rollback");

    // Only 1 trade remains (from block 1000)
    assert_eq!(mv.values.get("trade_count"), Some(&Value::UInt64(1)));

    // Volume: 100_000_000 / 1_000_000 = 100.0
    let total_vol = mv.values.get("total_volume").unwrap().as_f64().unwrap();
    assert!((total_vol - 100.0).abs() < 0.001);
}

/// Known insider's subsequent orders across multiple blocks emit correctly.
#[test]
fn insider_multi_block_subsequent_orders() {
    let mut db = DeltaDb::open(Config::new(SCHEMA)).unwrap();

    // Block 1000: triggers insider ($5000)
    db.process_batch(
        "orders",
        1000,
        vec![make_order(
            "whale",
            "token_a",
            5_000_000_000,
            10_000_000_000,
            0,
            1000,
        )],
    )
    .unwrap();
    db.flush();

    // Block 1001: subsequent order on token_b
    db.process_batch(
        "orders",
        1001,
        vec![make_order(
            "whale",
            "token_b",
            500_000_000,
            1_000_000_000,
            0,
            2000,
        )],
    )
    .unwrap();
    db.flush();

    // Block 1002: another order on token_a
    db.process_batch(
        "orders",
        1002,
        vec![make_order(
            "whale",
            "token_a",
            300_000_000,
            600_000_000,
            0,
            3000,
        )],
    )
    .unwrap();

    let batch = db.flush().unwrap();
    let insider_records: Vec<_> = batch.records_for("insider_positions").iter().collect();

    // token_a should get an Update (trade_count goes from 1 to 2)
    let token_a = insider_records
        .iter()
        .find(|r| r.key.get("asset_id") == Some(&Value::String("token_a".into())));
    assert!(token_a.is_some(), "token_a should have an updated position");
    assert_eq!(
        token_a.unwrap().values.get("trade_count"),
        Some(&Value::UInt64(2))
    );
}
