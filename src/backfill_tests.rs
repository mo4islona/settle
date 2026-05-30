//! Backfill-mode (durable checkpoint ≠ finality) crash-safety tests.
//!
//! These exercise the deferral path added in `Config::backfill_checkpoint_interval`
//! and the design v2 invariants verified by the adversarial review
//! (docs/backfill-mode-design.md): recovery anchored on the durable watermark,
//! gappy-chain clamping, and the sliding/external deferral exclusions.
//!
//! Gated on `feature = "rocksdb"` (drop+reopen needs real persistence).

use super::*;
use crate::reducer_runtime::fn_reducer::FnReducerRuntime;
use crate::test_helpers::cursor;
use crate::types::{ChangeBatch, RowMap, Value};
use std::collections::HashMap;

const MV_SCHEMA: &str = r#"
CREATE TABLE orders (
    block_number UInt64,
    asset_id     String,
    usdc         UInt64
);

CREATE MATERIALIZED VIEW summary AS
SELECT asset_id, sum(usdc) AS total
FROM orders
GROUP BY asset_id;
"#;

const SLIDING_SCHEMA: &str = r#"
CREATE TABLE swaps (
    block_number UInt64,
    pool         String,
    volume       Float64,
    ts           DateTime
);

CREATE MATERIALIZED VIEW pool_1h AS
  SELECT
    pool,
    SUM(volume) AS vol
  FROM swaps
  GROUP BY pool
  WINDOW SLIDING INTERVAL 1 HOUR BY ts;
"#;

fn order(asset: &str, usdc: u64) -> RowMap {
    HashMap::from([
        ("asset_id".to_string(), Value::String(asset.to_string())),
        ("usdc".to_string(), Value::UInt64(usdc)),
    ])
}

/// Ingest one block. `finalized` is the finality watermark this ingest reports;
/// for no-lag backfill pass `finalized == block`.
fn ingest_block(
    db: &mut Settle,
    table: &str,
    block: u64,
    finalized: u64,
    mut rows: Vec<RowMap>,
) -> Option<ChangeBatch> {
    for r in &mut rows {
        r.insert("block_number".to_string(), Value::UInt64(block));
    }
    let input = IngestInput {
        data: HashMap::from([(table.to_string(), rows)]),
        rollback_chain: vec![cursor(block)],
        finalized_head: cursor(finalized),
    };
    let batch = db.ingest(input).unwrap();
    if let Some(ref b) = batch {
        db.ack(b.sequence).unwrap();
    }
    batch
}

/// Pull an MV group's aggregate value out of a ChangeBatch (the MV emits
/// absolute current values, not deltas).
fn mv_value(batch: &ChangeBatch, view: &str, group_col: &str, group_val: &str, out: &str) -> Option<f64> {
    batch
        .records_for(view)
        .iter()
        .find(|r| r.values.get(group_col) == Some(&Value::String(group_val.to_string())))
        .and_then(|r| r.values.get(out))
        .and_then(|v| v.as_f64())
}

/// CRITICAL (review hole #1): no-lag backfill that NEVER reaches a checkpoint,
/// then crash (drop) + reopen, must rebuild the full MV via replay from the
/// durable watermark (= 0 here). If recovery anchored on finality instead of
/// durability, the (0, 79] derived state would be silently lost.
#[test]
fn backfill_recovery_without_checkpoint_preserves_mv() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(100);
        let mut db = Settle::open(cfg).unwrap();
        for b in 1..=79u64 {
            ingest_block(&mut db, "orders", b, b, vec![order("A", 1000)]);
        }
        // interval=100 never reached → nothing persisted durably yet.
        assert_eq!(db.durable_block(), 0, "no checkpoint should have fired at interval=100");
        assert_eq!(db.finalized_block(), 79, "in-memory finality still advances");
        // db dropped here: committed batches (raw rows + META_FINALIZED=0) are on
        // disk; the deferred derived MV state lived only in memory and is lost.
    }

    let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(100);
    let mut db = Settle::open(cfg).unwrap();
    // Recovery: durable=0 on disk → replay raw rows 1..79 rebuilds the MV.
    let batch = ingest_block(&mut db, "orders", 80, 80, vec![order("A", 1000)]).unwrap();
    let total = mv_value(&batch, "summary", "asset_id", "A", "total").unwrap();
    assert_eq!(
        total, 80_000.0,
        "MV must reflect all 80 blocks (1..79 rebuilt via replay + block 80)"
    );
}

/// Deferral must not change computed results: the same ingest sequence with
/// deferral on (interval=100) and off (interval=1) must emit identical MV
/// values at every step (deferral only affects disk persistence, not the
/// in-memory aggregate).
#[test]
fn backfill_deferred_matches_always_persist() {
    let run = |interval: u64| -> Vec<f64> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(interval);
        let mut db = Settle::open(cfg).unwrap();
        let mut totals = Vec::new();
        for b in 1..=50u64 {
            let batch = ingest_block(&mut db, "orders", b, b, vec![order("A", 100)]).unwrap();
            totals.push(mv_value(&batch, "summary", "asset_id", "A", "total").unwrap());
        }
        totals
    };
    let deferred = run(100);
    let always = run(1);
    assert_eq!(deferred, always, "deferral must not alter emitted MV values");
    assert_eq!(*deferred.last().unwrap(), 5_000.0);
}

/// A checkpoint mid-backfill advances the durable watermark; after it, a crash
/// replays only the post-checkpoint tail and still yields the full MV.
#[test]
fn backfill_checkpoint_advances_durable_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(10);
        let mut db = Settle::open(cfg).unwrap();
        for b in 1..=25u64 {
            ingest_block(&mut db, "orders", b, b, vec![order("A", 1000)]);
        }
        // Checkpoints fire at b=10 and b=20 (interval=10). Durable clamps to the
        // last checkpoint (≤ finalized).
        assert!(db.durable_block() >= 20, "durable should have checkpointed (got {})", db.durable_block());
        assert_eq!(db.finalized_block(), 25);
    }
    let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(10);
    let mut db = Settle::open(cfg).unwrap();
    let batch = ingest_block(&mut db, "orders", 26, 26, vec![order("A", 1000)]).unwrap();
    let total = mv_value(&batch, "summary", "asset_id", "A", "total").unwrap();
    assert_eq!(total, 26_000.0, "checkpoint + replay-of-tail must yield full MV");
}

/// Gappy chain (review hole #6): finalized head ahead of the latest data block.
/// The durable watermark must clamp to the latest persisted raw block, so a
/// later sub-finality data block's contribution survives a crash.
#[test]
fn backfill_gappy_chain_clamps_durable_to_latest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(5);
        let mut db = Settle::open(cfg).unwrap();
        // Data block 3, but finality reported far ahead (block 100) — gappy.
        // A checkpoint fires (100-0 >= 5) but durable must clamp to latest=3,
        // never to 100 (no raw rows for 4..100 to replay).
        ingest_block(&mut db, "orders", 3, 100, vec![order("A", 1000)]);
        assert!(
            db.durable_block() <= 3,
            "durable must clamp to latest data block, got {}",
            db.durable_block()
        );
    }
    let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(5);
    let mut db = Settle::open(cfg).unwrap();
    let batch = ingest_block(&mut db, "orders", 101, 101, vec![order("A", 1000)]).unwrap();
    let total = mv_value(&batch, "summary", "asset_id", "A", "total").unwrap();
    assert_eq!(total, 2_000.0, "block-3 contribution must survive (no data lost to clamp)");
}

const EXTERNAL_SCHEMA: &str = r#"
CREATE TABLE events (
    block_number UInt64,
    user         String,
    amount       Float64
);

CREATE REDUCER agg
SOURCE events
GROUP BY user
STATE (
    total Float64 DEFAULT 0
)
LANGUAGE EXTERNAL;

CREATE MATERIALIZED VIEW user_totals AS
SELECT user, last(total) AS total
FROM agg
GROUP BY user;
"#;

fn ext_runtime() -> FnReducerRuntime {
    FnReducerRuntime::new(|state, row| {
        let amount = row.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let total = state.get("total").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let new_total = total + amount;
        state.insert("total".to_string(), Value::Float64(new_total));
        vec![HashMap::from([("total".to_string(), Value::Float64(new_total))])]
    })
}

/// Gating (review hole #5): a pipeline with an external reducer must NOT defer —
/// replay skips external reducers, so their deferred state couldn't be rebuilt.
#[test]
fn defer_disabled_for_external_reducer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let cfg = Config::with_data_dir(EXTERNAL_SCHEMA, &path).backfill_checkpoint_interval(100);
    let mut db = Settle::open(cfg).unwrap();
    db.register_reducer_callback("agg", Box::new(ext_runtime())).unwrap();
    let ev = HashMap::from([
        ("user".to_string(), Value::String("u1".to_string())),
        ("amount".to_string(), Value::Float64(5.0)),
    ]);
    ingest_block(&mut db, "events", 5, 5, vec![ev]);
    assert_eq!(
        db.durable_block(),
        5,
        "external-reducer pipeline must persist every finalize (no deferral)"
    );
}

/// Reducer deferral path (review hole #1, reducer side): a Lua-reducer pipeline
/// backfilled without ever checkpointing, then crash+reopen, must rebuild BOTH
/// the reducer state and its downstream MV via replay from the durable
/// watermark. Exercises reducer `pending_durable` + snapshot retention.
#[cfg(feature = "lua")]
#[test]
fn backfill_reducer_recovery_without_checkpoint() {
    const REDUCER_SCHEMA: &str = r#"
CREATE VIRTUAL TABLE orders (
    block_number UInt64,
    asset_id     String,
    usdc         UInt64,
    shares       UInt64
);

CREATE REDUCER stats
SOURCE orders
GROUP BY asset_id
STATE (
    volume Float64 DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    local vol = row.usdc / 1000000
    state.volume = state.volume + vol
    emit({asset_id = row.asset_id, volume = vol})
$$;

CREATE MATERIALIZED VIEW summary AS
SELECT asset_id, sum(volume) AS total
FROM stats
GROUP BY asset_id;
"#;
    let order_rs = |asset: &str, usdc: u64| -> RowMap {
        HashMap::from([
            ("asset_id".to_string(), Value::String(asset.to_string())),
            ("usdc".to_string(), Value::UInt64(usdc)),
            ("shares".to_string(), Value::UInt64(1)),
        ])
    };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let cfg = Config::with_data_dir(REDUCER_SCHEMA, &path).backfill_checkpoint_interval(100);
        let mut db = Settle::open(cfg).unwrap();
        for b in 1..=79u64 {
            ingest_block(&mut db, "orders", b, b, vec![order_rs("A", 1_000_000)]);
        }
        assert_eq!(db.durable_block(), 0, "no checkpoint at interval=100");
    }
    let cfg = Config::with_data_dir(REDUCER_SCHEMA, &path).backfill_checkpoint_interval(100);
    let mut db = Settle::open(cfg).unwrap();
    let batch = ingest_block(&mut db, "orders", 80, 80, vec![order_rs("A", 1_000_000)]).unwrap();
    // Each order contributes usdc/1e6 = 1.0 to volume; 80 blocks → 80.0.
    let total = mv_value(&batch, "summary", "asset_id", "A", "total").unwrap();
    assert_eq!(
        total, 80.0,
        "reducer + MV must be rebuilt from replay (1..79) + block 80"
    );
}

/// Gating (review hole #4): a sliding-window MV pipeline must NOT defer — every
/// finalize persists, so the durable watermark tracks finality even at a large
/// interval.
#[test]
fn defer_disabled_for_sliding_window() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let cfg = Config::with_data_dir(SLIDING_SCHEMA, &path).backfill_checkpoint_interval(100);
    let mut db = Settle::open(cfg).unwrap();
    let swap = HashMap::from([
        ("pool".to_string(), Value::String("ETH".to_string())),
        ("volume".to_string(), Value::Float64(10.0)),
        ("ts".to_string(), Value::DateTime(1_000_000)),
    ]);
    ingest_block(&mut db, "swaps", 5, 5, vec![swap]);
    assert_eq!(
        db.durable_block(),
        5,
        "sliding-window pipeline must persist every finalize (no deferral)"
    );
}
