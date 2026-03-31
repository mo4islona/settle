//! End-to-end integration test: full DEX dashboard pipeline.
//!
//! Based on PLAN.md Step 15 and RFC Section 11:
//! 1. Parse DEX schema (trades -> pnl reducer -> position_summary MV + volume MV)
//! 2. Ingest synthetic trade data across 100 blocks
//! 3. Trigger rollback at block 75
//! 4. Re-process blocks 75-100 with different data
//! 5. Verify all deltas are correct
//! 6. Flush to mock target, verify records

use std::collections::HashMap;

use delta_db::db::{Config, DeltaDb};
use delta_db::types::{DeltaBatch, DeltaOperation, DeltaRecord, RowMap, Value};

const DEX_SCHEMA: &str = include_str!("schema.sql");

fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> RowMap {
    HashMap::from([
        ("user".to_string(), Value::String(user.to_string())),
        ("side".to_string(), Value::String(side.to_string())),
        ("amount".to_string(), Value::Float64(amount)),
        ("price".to_string(), Value::Float64(price)),
    ])
}

fn make_swap(pool: &str, amount: f64) -> RowMap {
    HashMap::from([
        ("pool".to_string(), Value::String(pool.to_string())),
        ("amount".to_string(), Value::Float64(amount)),
    ])
}

fn find_records<'a>(batch: &'a DeltaBatch, table: &str) -> Vec<&'a DeltaRecord> {
    batch.records_for(table).iter().collect()
}

fn find_record_by_key<'a>(
    batch: &'a DeltaBatch,
    table: &str,
    key_col: &str,
    key_val: &Value,
) -> Option<&'a DeltaRecord> {
    batch
        .records_for(table)
        .iter()
        .find(|r| r.key.get(key_col) == Some(key_val))
}

/// Collect all flushed batches, applying each to a mock target.
struct MockTarget {
    batches: Vec<DeltaBatch>,
}

impl MockTarget {
    fn new() -> Self {
        Self { batches: vec![] }
    }

    fn apply(&mut self, batch: DeltaBatch) {
        self.batches.push(batch);
    }

    fn total_records(&self) -> usize {
        self.batches.iter().map(|b| b.record_count()).sum()
    }
}

#[test]
fn full_dex_pipeline_100_blocks_with_rollback() {
    let mut db = DeltaDb::open(Config::new(DEX_SCHEMA)).unwrap();
    let mut target = MockTarget::new();

    let users = ["alice", "bob", "charlie"];
    let pools = ["ETH/USDC", "BTC/USDC"];

    // Phase 1: Ingest blocks 1-100
    for block in 1..=100u64 {
        let mut trades = Vec::new();
        let mut swaps = Vec::new();

        // Each block: each user makes a trade
        for (i, user) in users.iter().enumerate() {
            let side = if block % 3 == (i as u64 % 3) {
                "sell"
            } else {
                "buy"
            };
            let amount = 1.0 + (block as f64 * 0.1);
            let price = 2000.0 + (block as f64);
            trades.push(make_trade(user, side, amount, price));
        }

        // Each block: swaps for both pools
        for pool in &pools {
            let amount = 100.0 + (block as f64);
            swaps.push(make_swap(pool, amount));
        }

        db.process_batch("trades", block, trades).unwrap();
        db.process_batch("swaps", block, swaps).unwrap();

        // Finalize periodically, but stay below our rollback target of 75
        if block % 20 == 0 && block >= 20 && block <= 60 {
            db.finalize(block - 10).unwrap();
        }
    }

    // Flush phase 1
    let batch1 = db.flush().unwrap();
    assert!(batch1.record_count() > 0);
    assert!(batch1.latest_head.is_some());
    target.apply(batch1);

    // Verify MV state after 100 blocks
    // Each user had 100 trades
    let batch = &target.batches[0];
    for user in &users {
        let user_val = Value::String(user.to_string());
        let rec = find_record_by_key(&batch, "position_summary", "user", &user_val);
        assert!(rec.is_some(), "missing position_summary for {user}");
        let rec = rec.unwrap();
        assert_eq!(
            rec.values.get("trade_count"),
            Some(&Value::UInt64(100)),
            "wrong trade_count for {user}"
        );
    }

    // Both pools should have volume
    for pool in &pools {
        let pool_val = Value::String(pool.to_string());
        let rec = find_record_by_key(&batch, "volume_by_pool", "pool", &pool_val);
        assert!(rec.is_some(), "missing volume_by_pool for {pool}");
        let rec = rec.unwrap();
        assert_eq!(
            rec.values.get("swap_count"),
            Some(&Value::UInt64(100)),
            "wrong swap_count for {pool}"
        );
        // sum of 101+102+...+200 = 100*150.5 = 15050
        let total_volume = rec.values.get("total_volume").unwrap().as_f64().unwrap();
        assert!(
            (total_volume - 15050.0).abs() < 0.01,
            "wrong total_volume for {pool}: {total_volume}"
        );
    }

    // Phase 2: Rollback to block 75
    db.rollback(75).unwrap();
    assert_eq!(db.latest_block(), 75);

    let rollback_batch = db.flush().unwrap();
    assert!(rollback_batch.record_count() > 0);
    target.apply(rollback_batch);

    // After rollback, position_summary should reflect 75 trades per user
    let rb = &target.batches[1];
    for user in &users {
        let user_val = Value::String(user.to_string());
        let rec = find_record_by_key(&rb, "position_summary", "user", &user_val);
        assert!(
            rec.is_some(),
            "missing rollback position_summary for {user}"
        );
        let rec = rec.unwrap();
        assert_eq!(
            rec.values.get("trade_count"),
            Some(&Value::UInt64(75)),
            "wrong rollback trade_count for {user}"
        );
    }

    // Volume MV should also be rolled back
    for pool in &pools {
        let pool_val = Value::String(pool.to_string());
        let rec = find_record_by_key(&rb, "volume_by_pool", "pool", &pool_val);
        assert!(rec.is_some(), "missing rollback volume for {pool}");
        let rec = rec.unwrap();
        assert_eq!(
            rec.values.get("swap_count"),
            Some(&Value::UInt64(75)),
            "wrong rollback swap_count for {pool}"
        );
        // sum of 101+102+...+175 = 75*138 = 10350
        let total_volume = rec.values.get("total_volume").unwrap().as_f64().unwrap();
        assert!(
            (total_volume - 10350.0).abs() < 0.01,
            "wrong rollback total_volume for {pool}: {total_volume}"
        );
    }

    // Phase 3: Re-ingest blocks 76-100 with different data (doubled amounts)
    for block in 76..=100u64 {
        let mut trades = Vec::new();
        let mut swaps = Vec::new();

        for (i, user) in users.iter().enumerate() {
            // All buys in the re-ingested phase (different from original)
            let side = if block % 5 == (i as u64 % 5) {
                "sell"
            } else {
                "buy"
            };
            let amount = 2.0 + (block as f64 * 0.2); // doubled amounts
            let price = 2500.0 + (block as f64); // different prices
            trades.push(make_trade(user, side, amount, price));
        }

        for pool in &pools {
            let amount = 200.0 + (block as f64 * 2.0); // doubled
            swaps.push(make_swap(pool, amount));
        }

        db.process_batch("trades", block, trades).unwrap();
        db.process_batch("swaps", block, swaps).unwrap();
    }

    assert_eq!(db.latest_block(), 100);

    let reingest_batch = db.flush().unwrap();
    assert!(reingest_batch.record_count() > 0);
    target.apply(reingest_batch);

    // Phase 4: Verify final state
    let final_batch = &target.batches[2];

    // Each user should have 100 total trades (75 original + 25 re-ingested)
    for user in &users {
        let user_val = Value::String(user.to_string());
        let rec = find_record_by_key(&final_batch, "position_summary", "user", &user_val);
        assert!(rec.is_some(), "missing final position_summary for {user}");
        let rec = rec.unwrap();
        assert_eq!(
            rec.values.get("trade_count"),
            Some(&Value::UInt64(100)),
            "wrong final trade_count for {user}"
        );
    }

    // Volume should be sum of blocks 1-75 (original) + 76-100 (re-ingested with doubled amounts)
    for pool in &pools {
        let pool_val = Value::String(pool.to_string());
        let rec = find_record_by_key(&final_batch, "volume_by_pool", "pool", &pool_val);
        assert!(rec.is_some(), "missing final volume for {pool}");
        let rec = rec.unwrap();
        assert_eq!(
            rec.values.get("swap_count"),
            Some(&Value::UInt64(100)),
            "wrong final swap_count for {pool}"
        );

        // Original blocks 1-75: sum(101..175) = 75*138 = 10350
        // Re-ingested blocks 76-100: sum(200+76*2 .. 200+100*2) = sum(352,354,...,400) = 25*376 = 9400
        let expected_volume = 10350.0 + 9400.0;
        let total_volume = rec.values.get("total_volume").unwrap().as_f64().unwrap();
        assert!(
            (total_volume - expected_volume).abs() < 0.01,
            "wrong final total_volume for {pool}: {total_volume}, expected {expected_volume}"
        );
    }

    // Verify sequence numbers are monotonically increasing
    for (i, batch) in target.batches.iter().enumerate() {
        assert_eq!(batch.sequence, (i + 1) as u64);
    }

    // Verify total record count is reasonable
    assert!(target.total_records() > 0);
    println!(
        "E2E test passed: {} batches, {} total records",
        target.batches.len(),
        target.total_records()
    );
}

#[test]
fn rollback_to_finalized_boundary() {
    let mut db = DeltaDb::open(Config::new(DEX_SCHEMA)).unwrap();

    // Ingest 50 blocks
    for block in 1..=50u64 {
        db.process_batch(
            "trades",
            block,
            vec![make_trade("alice", "buy", 1.0, 2000.0)],
        )
        .unwrap();
    }
    db.flush();

    // Finalize up to block 30
    db.finalize(30).unwrap();
    assert_eq!(db.finalized_block(), 30);

    // Rollback to block 30 (the finalized boundary)
    db.rollback(30).unwrap();
    assert_eq!(db.latest_block(), 30);

    let batch = db.flush().unwrap();
    let pos = find_records(&batch, "position_summary");
    assert_eq!(pos.len(), 1);
    assert_eq!(pos[0].values.get("trade_count"), Some(&Value::UInt64(30)));
}

#[test]
fn multi_user_pnl_correctness() {
    let mut db = DeltaDb::open(Config::new(DEX_SCHEMA)).unwrap();

    // Alice buys 10 @ 2000
    db.process_batch("trades", 1, vec![make_trade("alice", "buy", 10.0, 2000.0)])
        .unwrap();
    // Bob buys 5 @ 3000
    db.process_batch("trades", 2, vec![make_trade("bob", "buy", 5.0, 3000.0)])
        .unwrap();
    // Alice sells 5 @ 2500 (PnL = 5 * (2500 - 2000) = 2500)
    db.process_batch("trades", 3, vec![make_trade("alice", "sell", 5.0, 2500.0)])
        .unwrap();
    // Bob sells 3 @ 2800 (PnL = 3 * (2800 - 3000) = -600)
    db.process_batch("trades", 4, vec![make_trade("bob", "sell", 3.0, 2800.0)])
        .unwrap();

    let batch = db.flush().unwrap();

    let alice_rec = find_record_by_key(
        &batch,
        "position_summary",
        "user",
        &Value::String("alice".into()),
    )
    .unwrap();

    let alice_pnl = alice_rec.values.get("total_pnl").unwrap().as_f64().unwrap();
    assert!((alice_pnl - 2500.0).abs() < 0.01, "alice PnL: {alice_pnl}");
    assert_eq!(
        alice_rec.values.get("current_position"),
        Some(&Value::Float64(5.0))
    );
    assert_eq!(alice_rec.values.get("trade_count"), Some(&Value::UInt64(2)));

    let bob_rec = find_record_by_key(
        &batch,
        "position_summary",
        "user",
        &Value::String("bob".into()),
    )
    .unwrap();

    let bob_pnl = bob_rec.values.get("total_pnl").unwrap().as_f64().unwrap();
    assert!((bob_pnl - (-600.0)).abs() < 0.01, "bob PnL: {bob_pnl}");
    assert_eq!(
        bob_rec.values.get("current_position"),
        Some(&Value::Float64(2.0))
    );
}

#[test]
fn delta_operations_are_correct() {
    let mut db = DeltaDb::open(Config::new(DEX_SCHEMA)).unwrap();

    // Block 1: first insert for alice
    db.process_batch("trades", 1, vec![make_trade("alice", "buy", 10.0, 2000.0)])
        .unwrap();
    let b1 = db.flush().unwrap();
    let pos1 = find_records(&b1, "position_summary");
    assert_eq!(pos1.len(), 1);
    assert_eq!(pos1[0].operation, DeltaOperation::Insert);

    // Block 2: update for alice
    db.process_batch("trades", 2, vec![make_trade("alice", "buy", 5.0, 2100.0)])
        .unwrap();
    let b2 = db.flush().unwrap();
    let pos2 = find_records(&b2, "position_summary");
    assert_eq!(pos2.len(), 1);
    assert_eq!(pos2[0].operation, DeltaOperation::Update);

    // Rollback block 2 + block 1
    db.rollback(0).unwrap();
    let rb = db.flush().unwrap();
    let pos_rb = find_records(&rb, "position_summary");
    assert_eq!(pos_rb.len(), 1);
    assert_eq!(pos_rb[0].operation, DeltaOperation::Delete);
}
