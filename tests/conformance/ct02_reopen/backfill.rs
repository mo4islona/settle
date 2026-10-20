//! Backfill-mode (durable checkpoint ≠ finality) crash-safety tests.
//!
//! These exercise the deferral path of `Config::backfill_checkpoint_interval`
//! and the invariants of spec/decisions/ADR-2-finality-and-durable-watermark.md:
//! recovery anchored on the durable watermark, gappy-chain clamping, the
//! sliding deferral exclusion, and external reducers after a reopen.
//!
//! Gated on `feature = "rocksdb"` (drop+reopen needs real persistence).

use std::collections::HashMap;

use settle::db::{Config, IngestInput, Settle};
use settle::schema::parser::parse_schema;
use settle::error::{Error, Result};
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::reducer_runtime::{Emits, GroupState, ReducerRuntime};
use settle::test_helpers::cursor;
use settle::types::{ChangeBatch, Row, RowMap, Value};

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
        .find(|r| r.get(group_col) == Some(&Value::String(group_val.to_string())))
        .and_then(|r| r.get(out))
        .and_then(|v| v.as_f64())
}

/// CRITICAL (review hole #1): no-lag backfill that NEVER reaches a checkpoint,
/// then crash (drop) + reopen, must rebuild the full MV via replay from the
/// durable watermark (= 1 here: the first data block is always a checkpoint).
/// If recovery anchored on finality instead of durability, the (1, 79]
/// derived state would be silently lost.
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
        // interval=100 never reached → only the first block is durable.
        assert_eq!(db.durable_block(), 1, "only the first block is a checkpoint at interval=100");
        assert_eq!(db.finalized_block(), 79, "in-memory finality still advances");
        // db dropped here: committed batches (raw rows + the durable block 1) are on
        // disk; the deferred derived MV state lived only in memory and is lost.
    }

    let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(100);
    let mut db = Settle::open(cfg).unwrap();
    // Recovery: durable=1 on disk → replay raw rows 2..79 rebuilds the MV.
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
        state.set("total", Value::Float64(new_total));
        state.emit(HashMap::from([(
            "total".to_string(),
            Value::Float64(new_total),
        )]))
    })
}

/// An external reducer defers too. After a reopen, the open replay skipped
/// it, so an ingest waits for its callback, whose replay rebuilds the
/// deferred blocks from the durable one.
#[test]
fn external_reducer_defers_and_replays_from_the_durable_block() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let open = || {
        let cfg = Config::with_data_dir(EXTERNAL_SCHEMA, &path).backfill_checkpoint_interval(100);
        Settle::open(cfg).unwrap()
    };
    let event = || {
        vec![HashMap::from([
            ("user".to_string(), Value::String("u1".to_string())),
            ("amount".to_string(), Value::Float64(1.0)),
        ])]
    };

    {
        let mut db = open();
        db.register_reducer_callback("agg", Box::new(ext_runtime())).unwrap();
        for b in 1..=50u64 {
            ingest_block(&mut db, "events", b, b, event());
        }
        assert_eq!(db.durable_block(), 1, "the first data block is the only checkpoint");
        assert_eq!(db.finalized_block(), 50);
    }

    let mut db = open();
    assert_eq!(db.finalized_block(), 50);
    // No row reaches the reducer, but the finality of this call would make
    // blocks 2..50 final without it.
    let heartbeat = db.ingest(IngestInput {
        data: HashMap::new(),
        rollback_chain: vec![cursor(50)],
        finalized_head: cursor(50),
    });
    let error = heartbeat.unwrap_err().to_string();
    assert!(error.contains("'agg' has no callback"), "{error}");

    db.register_reducer_callback("agg", Box::new(ext_runtime())).unwrap();
    let batch = ingest_block(&mut db, "events", 51, 51, event()).unwrap();
    let total = mv_value(&batch, "user_totals", "user", "u1", "total").unwrap();
    assert_eq!(total, 51.0, "blocks 2..50 come back through the callback replay");

    // Once final, the blocks stay out of a later replay.
    db.replay_reducer("agg").unwrap();
    let batch = ingest_block(&mut db, "events", 52, 52, event()).unwrap();
    let total = mv_value(&batch, "user_totals", "user", "u1", "total").unwrap();
    assert_eq!(total, 52.0);
}

/// `ext_runtime`, but it fails on an amount of 3.
struct FailsOnThree;

impl ReducerRuntime for FailsOnThree {
    fn process(&self, mut state: GroupState<'_>, row: &Row, emits: &mut Emits) -> Result<()> {
        let amount = row.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
        if amount == 3.0 {
            return Err(Error::Reducer("unlucky amount".into()));
        }

        let total = state.get("total").and_then(|v| v.as_f64()).unwrap_or(0.0) + amount;
        state.set("total", Value::Float64(total));
        emits.push_named([("total", Value::Float64(total))]);
        Ok(())
    }
}

/// A callback whose replay fails leaves its chain rolled back and in part
/// replayed, so the instance is poisoned until a reopen. Under deferral the
/// replay covers final blocks too: an ingest would make the gap final.
#[test]
fn failed_callback_replay_poisons_the_instance() {
    // (interval, finality of block b): deferral with finality at each block,
    // and the tip with finality at block 1.
    for (interval, finality) in [(100, None), (1, Some(1))] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        let open = || {
            let cfg = Config::with_data_dir(EXTERNAL_SCHEMA, &path)
                .backfill_checkpoint_interval(interval);
            Settle::open(cfg).unwrap()
        };
        let event = |amount: f64| {
            vec![HashMap::from([
                ("user".to_string(), Value::String("u1".to_string())),
                ("amount".to_string(), Value::Float64(amount)),
            ])]
        };

        {
            let mut db = open();
            db.register_reducer_callback("agg", Box::new(ext_runtime())).unwrap();
            for b in 1..=4u64 {
                ingest_block(&mut db, "events", b, finality.unwrap_or(b), event(b as f64));
            }
            assert_eq!(db.durable_block(), 1, "interval {interval}");
        }

        let mut db = open();
        // A replay that never starts changes nothing.
        assert!(db.replay_reducer("missing").is_err());
        assert!(!db.is_poisoned(), "interval {interval}");
        let failed = db.register_reducer_callback("agg", Box::new(FailsOnThree));
        assert!(failed.is_err(), "interval {interval}: the replay reaches block 3");
        assert!(db.is_poisoned(), "interval {interval}");
        let mut rows = event(5.0);
        rows[0].insert("block_number".to_string(), Value::UInt64(5));
        let next = db.ingest(IngestInput {
            data: HashMap::from([("events".to_string(), rows)]),
            rollback_chain: vec![cursor(5)],
            finalized_head: cursor(5),
        });
        assert!(next.is_err(), "interval {interval}: ingest after a failed replay");
        drop(db);

        let mut db = open();
        db.register_reducer_callback("agg", Box::new(ext_runtime())).unwrap();
        let batch = ingest_block(&mut db, "events", 5, 5, event(5.0)).unwrap();
        let total = mv_value(&batch, "user_totals", "user", "u1", "total").unwrap();
        assert_eq!(total, 15.0, "interval {interval}: blocks 1..5");
    }
}

/// A reducer registered at run time is not in the schema, so an open cannot
/// wait for it: its pipeline writes a checkpoint at each finality.
#[test]
fn run_time_reducer_keeps_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(100);
    let mut db = Settle::open(cfg).unwrap();

    let sql = format!(
        "{MV_SCHEMA}
        CREATE REDUCER late SOURCE orders GROUP BY asset_id
        STATE (total UInt64 DEFAULT 0) LANGUAGE EXTERNAL;"
    );
    let def = parse_schema(&sql).unwrap().reducers.remove(0);
    db.register_reducer(def).unwrap();
    let count = FnReducerRuntime::new(|state, _row| {
        let total = state.get_u64("total") + 1;
        state.set("total", Value::UInt64(total));
    });
    db.register_reducer_callback("late", Box::new(count)).unwrap();

    for b in 1..=5u64 {
        ingest_block(&mut db, "orders", b, b, vec![order("A", 1000)]);
    }
    assert_eq!(db.durable_block(), 5);
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
        assert_eq!(db.durable_block(), 1, "only the first block is a checkpoint at interval=100");
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

/// Cluster B fork-safety regression: under active backfill deferral the
/// in-memory finality watermark runs ahead of the durable watermark, and
/// `block_hashes` retain entries for the finalized-but-not-durable range. A
/// fork signal resolving onto one of those sub-finalized blocks MUST be
/// rejected — rolling derived state back below finality would corrupt the
/// durable recovery anchor and wedge the instance (durable > latest). The
/// rollback must error cleanly and leave engine state untouched.
#[test]
fn fork_below_finality_during_deferral_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let cfg = Config::with_data_dir(MV_SCHEMA, &path).backfill_checkpoint_interval(100);
    let mut db = Settle::open(cfg).unwrap();

    // No-lag backfill 1..50 (finalized == block). After the first block, the
    // interval=100 never fires, so durable stays 1 while finality reaches 50
    // and hashes for 1..50 survive.
    for b in 1..=50u64 {
        ingest_block(&mut db, "orders", b, b, vec![order("A", 1000)]);
    }
    assert_eq!(db.durable_block(), 1, "no checkpoint after the first block");
    assert_eq!(db.finalized_block(), 50);
    assert_eq!(db.latest_block(), 50);

    // Fork onto block 30 (< finalized 50). The hash matches a retained entry,
    // so pre-fix the bounded resolver would have found it and rolled back below
    // finality. It must now be refused.
    assert!(
        db.handle_fork(vec![cursor(30)]).is_err(),
        "handle_fork below finalized must be rejected"
    );

    // State untouched: no partial rollback, watermarks intact.
    assert_eq!(db.durable_block(), 1, "rejected fork must not mutate durable");
    assert_eq!(db.finalized_block(), 50, "rejected fork must not mutate finalized");
    assert_eq!(db.latest_block(), 50, "rejected fork must not mutate latest");

    // And the pipeline is still correct: the next no-lag block aggregates on top
    // of the intact 50-block state (proving no corruption / no lost contributions).
    let batch = ingest_block(&mut db, "orders", 51, 51, vec![order("A", 1000)]).unwrap();
    let total = mv_value(&batch, "summary", "asset_id", "A", "total").unwrap();
    assert_eq!(total, 51_000.0, "MV must still reflect all 51 blocks");
}
