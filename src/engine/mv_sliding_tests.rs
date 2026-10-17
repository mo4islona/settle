use super::*;
use crate::schema::ast::{AggFunc, MVDef, SelectExpr, SelectItem, SlidingWindowDef};
use crate::storage::BatchOp;
use crate::storage::memory::MemoryBackend;

fn test_storage() -> Arc<dyn StorageBackend> {
    Arc::new(MemoryBackend::new())
}

fn test_column_types() -> HashMap<String, ColumnType> {
    HashMap::from([
        ("pool".to_string(), ColumnType::String),
        ("user".to_string(), ColumnType::String),
        ("pair".to_string(), ColumnType::String),
        ("amount".to_string(), ColumnType::Float64),
        ("price".to_string(), ColumnType::Float64),
        ("volume".to_string(), ColumnType::Float64),
        ("ts".to_string(), ColumnType::DateTime),
        ("block_time".to_string(), ColumnType::DateTime),
    ])
}

fn make_row(pairs: &[(&str, Value)]) -> RowMap {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// -----------------------------------------------------------------------
// Sliding window tests
// -----------------------------------------------------------------------

/// Helper: create a sliding window MV def with SUM(volume), COUNT().
/// Window = `window_secs` seconds, grouped by `pair`, time column = `ts`.
fn sliding_sum_mv_def(window_secs: u64) -> MVDef {
    MVDef {
        name: "volume_sliding".to_string(),
        source: "trades".to_string(),
        select: vec![
            SelectItem {
                expr: SelectExpr::Column("pair".into()),
                alias: None,
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Sum, Some("volume".into())),
                alias: Some("total_volume".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Count, None),
                alias: Some("trade_count".into()),
            },
        ],
        group_by: vec!["pair".into()],
        sliding_window: Some(SlidingWindowDef {
            interval_seconds: window_secs,
            time_column: "ts".into(),
        }),
    }
}

/// Helper: create a sliding window MV def with all 7 aggregation types.
fn sliding_all_aggs_mv_def(window_secs: u64) -> MVDef {
    MVDef {
        name: "all_aggs_sliding".to_string(),
        source: "data".to_string(),
        select: vec![
            SelectItem {
                expr: SelectExpr::Column("grp".into()),
                alias: None,
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Sum, Some("val".into())),
                alias: Some("s".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Count, None),
                alias: Some("c".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Min, Some("val".into())),
                alias: Some("mn".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Max, Some("val".into())),
                alias: Some("mx".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Avg, Some("val".into())),
                alias: Some("av".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::First, Some("val".into())),
                alias: Some("fi".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Last, Some("val".into())),
                alias: Some("la".into()),
            },
        ],
        group_by: vec!["grp".into()],
        sliding_window: Some(SlidingWindowDef {
            interval_seconds: window_secs,
            time_column: "ts".into(),
        }),
    }
}

fn make_ts_row(pairs: &[(&str, Value)], ts_ms: i64) -> RowMap {
    let mut row = make_row(pairs);
    row.insert("ts".to_string(), Value::DateTime(ts_ms));
    row
}

#[test]
fn sliding_window_no_expiry_within_window() {
    let mut mv = MVEngine::new(
        sliding_sum_mv_def(3600),
        test_storage(),
        &test_column_types(),
    );

    // Three blocks all within 1 hour
    let d1 = mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(100.0)),
            ],
            0,
        )],
    );
    assert_eq!(d1.len(), 1);
    assert_eq!(d1[0].operation, ChangeOp::Insert);
    assert_eq!(d1[0].get("total_volume"), Some(&Value::Float64(100.0)));

    let d2 = mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(200.0)),
            ],
            1_800_000,
        )],
    );
    assert_eq!(d2.len(), 1);
    assert_eq!(d2[0].operation, ChangeOp::Update);
    assert_eq!(d2[0].get("total_volume"), Some(&Value::Float64(300.0)));

    let d3 = mv.process_block(
        3,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(50.0)),
            ],
            3_500_000,
        )],
    );
    assert_eq!(d3.len(), 1);
    assert_eq!(d3[0].get("total_volume"), Some(&Value::Float64(350.0)));
    assert_eq!(d3[0].get("trade_count"), Some(&Value::UInt64(3)));
}

#[test]
fn sliding_window_basic_expiry() {
    let mut mv = MVEngine::new(
        sliding_sum_mv_def(3600),
        test_storage(),
        &test_column_types(),
    ); // 1 hour

    // Block 1: ts=0, volume=100
    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(100.0)),
            ],
            0,
        )],
    );

    // Block 2: ts=30min, volume=200
    mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(200.0)),
            ],
            1_800_000,
        )],
    );

    // Block 3: ts=1hr+1s → block 1 (ts=0) should expire
    // cutoff = 3_601_000 - 3_600_000 = 1_000. Block 1 ts=0 < 1_000 → expired
    let d3 = mv.process_block(
        3,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(300.0)),
            ],
            3_601_000,
        )],
    );
    assert_eq!(d3.len(), 1);
    assert_eq!(d3[0].operation, ChangeOp::Update);
    // After expiry: 200 + 300 = 500 (block 1's 100 expired)
    assert_eq!(d3[0].get("total_volume"), Some(&Value::Float64(500.0)));
    assert_eq!(d3[0].get("trade_count"), Some(&Value::UInt64(2)));
}

#[test]
fn sliding_window_full_expiry_delete() {
    let mut mv = MVEngine::new(
        sliding_sum_mv_def(3600),
        test_storage(),
        &test_column_types(),
    );

    // Block 1: group A at ts=0
    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("A".into())),
                ("volume", Value::Float64(10.0)),
            ],
            0,
        )],
    );

    // Block 2: group B at ts=1hr+1s → group A fully expires
    let d2 = mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("B".into())),
                ("volume", Value::Float64(20.0)),
            ],
            3_601_000,
        )],
    );

    // Should have Insert for B and Delete for A
    assert_eq!(d2.len(), 2);
    let insert = d2
        .iter()
        .find(|d| d.operation == ChangeOp::Insert)
        .unwrap();
    let delete = d2
        .iter()
        .find(|d| d.operation == ChangeOp::Delete)
        .unwrap();
    assert_eq!(insert.key("pair"), Some(&Value::String("B".into())));
    assert_eq!(delete.key("pair"), Some(&Value::String("A".into())));
}

#[test]
fn sliding_window_sum_correctness_across_expiry() {
    let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage(), &test_column_types()); // 10 second window

    // 5 blocks, each 3 seconds apart
    for i in 0..5u64 {
        mv.process_block(
            i + 1,
            &[make_ts_row(
                &[
                    ("pair", Value::String("X".into())),
                    ("volume", Value::Float64((i + 1) as f64 * 10.0)),
                ],
                (i * 3_000) as i64,
            )],
        );
    }

    // At block 5: ts=12_000, window=10_000, cutoff=2_000
    // Block 1 (ts=0) expired. Blocks 2-5 remain.
    // Sum = 20 + 30 + 40 + 50 = 140, count = 4
    // (Note: we need to check current state, which is reflected in the last change)
    // Actually, let me trace: after block 5, the last emit_changes for "X" captures all changes
    // including expiry. Let me just check the accumulated state.
    // Re-check: block 5 ts=12000, cutoff = 12000-10000 = 2000
    // Block 1 ts=0 < 2000 → expired. Blocks 2(ts=3000),3(ts=6000),4(ts=9000),5(ts=12000) remain
    // Sum = 20+30+40+50 = 140

    // Process one more block that doesn't expire anything, to get current state
    let d = mv.process_block(
        6,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(1.0)),
            ],
            12_500, // still within window of block 2
        )],
    );
    assert_eq!(d.len(), 1);
    // Sum: 20+30+40+50+1 = 141
    assert_eq!(d[0].get("total_volume"), Some(&Value::Float64(141.0)));
}

#[test]
fn sliding_window_all_agg_types() {
    let mut mv = MVEngine::new(
        sliding_all_aggs_mv_def(3600),
        test_storage(),
        &test_column_types(),
    );

    // Block 1: val=10 at ts=0
    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("grp", Value::String("G".into())),
                ("val", Value::Float64(10.0)),
            ],
            0,
        )],
    );

    // Block 2: val=20 at ts=30min
    mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("grp", Value::String("G".into())),
                ("val", Value::Float64(20.0)),
            ],
            1_800_000,
        )],
    );

    // Block 3: val=15 at ts=1hr+1s → block 1 expires
    let d3 = mv.process_block(
        3,
        &[make_ts_row(
            &[
                ("grp", Value::String("G".into())),
                ("val", Value::Float64(15.0)),
            ],
            3_601_000,
        )],
    );
    assert_eq!(d3.len(), 1);
    let v = &d3[0];
    // After expiry of block 1 (val=10):
    // Remaining: block 2 (val=20), block 3 (val=15)
    assert_eq!(v.get("s"), Some(&Value::Float64(35.0))); // sum: 20+15
    assert_eq!(v.get("c"), Some(&Value::UInt64(2))); // count: 2
    assert_eq!(v.get("mn"), Some(&Value::Float64(15.0))); // min: 15
    assert_eq!(v.get("mx"), Some(&Value::Float64(20.0))); // max: 20
    assert_eq!(v.get("av"), Some(&Value::Float64(17.5))); // avg: 35/2
    assert_eq!(v.get("fi"), Some(&Value::Float64(20.0))); // first: earliest remaining = block 2
    assert_eq!(v.get("la"), Some(&Value::Float64(15.0))); // last: latest = block 3
}

#[test]
fn sliding_window_rollback() {
    let mut mv = MVEngine::new(
        sliding_sum_mv_def(3600),
        test_storage(),
        &test_column_types(),
    );

    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(100.0)),
            ],
            0,
        )],
    );
    mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(200.0)),
            ],
            1_000_000,
        )],
    );
    mv.process_block(
        3,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(300.0)),
            ],
            2_000_000,
        )],
    );

    // Rollback to block 1
    let rollback_changes = mv.rollback(1);
    assert_eq!(rollback_changes.len(), 1);
    assert_eq!(rollback_changes[0].operation, ChangeOp::Update);
    assert_eq!(
        rollback_changes[0].get("total_volume"),
        Some(&Value::Float64(100.0))
    );

    // Re-ingest block 2 with different data
    let d = mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(50.0)),
            ],
            1_500_000,
        )],
    );
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].get("total_volume"), Some(&Value::Float64(150.0)));
}

#[test]
fn sliding_window_rollback_plus_expiry() {
    let mut mv = MVEngine::new(
        sliding_sum_mv_def(3600),
        test_storage(),
        &test_column_types(),
    );

    // Block 1: ts=0
    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(100.0)),
            ],
            0,
        )],
    );
    // Block 2: ts=1800s
    mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(200.0)),
            ],
            1_800_000,
        )],
    );
    // Block 3: ts=3601s → block 1 expired
    mv.process_block(
        3,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(300.0)),
            ],
            3_601_000,
        )],
    );

    // Rollback to block 2 removes block 3, so block 1 is in the window again:
    // the watermark goes back to 1_800_000.
    let rb = mv.rollback(2);
    assert_eq!(rb.len(), 1);
    assert_eq!(rb[0].get("total_volume"), Some(&Value::Float64(300.0)));
}

fn trade(pair: &str, volume: f64, ts_ms: i64) -> RowMap {
    make_ts_row(
        &[
            ("pair", Value::String(pair.into())),
            ("volume", Value::Float64(volume)),
        ],
        ts_ms,
    )
}

/// Expiry removes the BTC group; a fork below the block that expired it
/// brings the group back as an Insert.
#[test]
fn sliding_window_rollback_restores_group_removed_by_expiry() {
    let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage(), &test_column_types());

    mv.process_block(1, &[trade("BTC", 5.0, 0)]);
    mv.process_block(2, &[trade("ETH", 10.0, 5_000)]);
    let expiry = mv.process_block(3, &[trade("ETH", 20.0, 11_000)]);
    assert!(
        expiry.iter().any(|c| c.operation == ChangeOp::Delete
            && c.key("pair") == Some(&Value::String("BTC".into())))
    );

    let rb = mv.rollback(2);
    let btc = rb
        .iter()
        .find(|c| c.key("pair") == Some(&Value::String("BTC".into())))
        .unwrap();
    assert_eq!(btc.operation, ChangeOp::Insert);
    assert_eq!(btc.get("total_volume"), Some(&Value::Float64(5.0)));

    // Block 1 is a live block again: the next expiry removes it once more.
    let again = mv.process_block(3, &[trade("ETH", 20.0, 11_000)]);
    assert!(
        again.iter().any(|c| c.operation == ChangeOp::Delete
            && c.key("pair") == Some(&Value::String("BTC".into())))
    );
}

/// A block that expires on arrival is rolled back with itself, not restored.
#[test]
fn sliding_window_rollback_drops_block_expired_on_arrival() {
    let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage(), &test_column_types());

    mv.process_block(1, &[trade("ETH", 10.0, 20_000)]);
    mv.process_block(2, &[trade("BTC", 5.0, 1_000)]);

    assert!(mv.rollback(1).is_empty());
    assert!(mv.groups.keys().all(|k| k[0] != Value::String("BTC".into())));
    assert!(!mv.block_times.contains_key(&2));
}

/// Block 1 is final, but block 3 that expired it is not: storage holds the
/// window as of block 2, which keeps block 1, and replay of block 3 expires
/// it again.
#[test]
fn sliding_window_expired_block_survives_reopen() {
    let storage = test_storage();
    {
        let mut mv = MVEngine::new(sliding_sum_mv_def(3600), storage.clone(), &test_column_types());
        mv.process_block(1, &[trade("ETH", 100.0, 0)]);
        mv.process_block(2, &[trade("ETH", 200.0, 1_800_000)]);
        mv.process_block(3, &[trade("ETH", 300.0, 3_601_000)]);

        let mut batch = StorageWriteBatch::new();
        mv.advance_finality(2);
        mv.write_checkpoint(&mut batch);
        storage.commit(&batch).unwrap();
    }

    let mut mv = MVEngine::new(sliding_sum_mv_def(3600), storage.clone(), &test_column_types());
    assert_eq!(mv.block_times.keys().copied().collect::<Vec<_>>(), vec![1, 2]);

    let changes = mv.process_block(3, &[trade("ETH", 300.0, 3_601_000)]);
    assert_eq!(
        changes[0].prev("total_volume"),
        Some(&Value::Float64(300.0))
    );
    assert_eq!(changes[0].get("total_volume"), Some(&Value::Float64(500.0)));
}

/// A checkpoint stores the block that became final and deletes the stored
/// block that a final block expired, whatever the size of the window.
#[test]
fn sliding_checkpoint_does_not_grow_with_the_window() {
    for window in [10u64, 1_000] {
        let storage = test_storage();
        let mut mv = MVEngine::new(sliding_sum_mv_def(window), storage.clone(), &test_column_types());

        // One block per second; finality 5 blocks behind.
        let mut last = StorageWriteBatch::new();
        for block in 1..=2 * window {
            mv.process_block(block, &[trade("ETH", 1.0, block as i64 * 1000)]);
            last = StorageWriteBatch::new();
            mv.advance_finality(block - 5.min(block));
            mv.write_checkpoint(&mut last);
            storage.commit(&last).unwrap();
        }

        let ops: Vec<&str> = last
            .ops
            .iter()
            .map(|op| match op {
                BatchOp::PutMvState { .. } => "put",
                BatchOp::DeleteMvState { .. } => "delete",
                _ => "other",
            })
            .collect();
        assert_eq!(ops, ["put", "delete"], "window of {window} s");
    }
}

/// Once the block that expired a block is final, the expired data goes.
#[test]
fn sliding_window_final_expiry_drops_expired_data() {
    let mut mv = MVEngine::new(sliding_sum_mv_def(3600), test_storage(), &test_column_types());
    mv.process_block(1, &[trade("ETH", 100.0, 0)]);
    mv.process_block(2, &[trade("ETH", 200.0, 3_601_000)]);

    mv.advance_finality(1);
    assert_eq!(mv.expired.len(), 1);

    mv.advance_finality(2);
    assert!(mv.expired.is_empty());
}

#[test]
fn sliding_window_rapid_expiry() {
    // 1-second window: every new block expires the previous one
    let mut mv = MVEngine::new(sliding_sum_mv_def(1), test_storage(), &test_column_types());

    let d1 = mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(10.0)),
            ],
            0,
        )],
    );
    assert_eq!(d1[0].get("total_volume"), Some(&Value::Float64(10.0)));

    let d2 = mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(20.0)),
            ],
            2_000,
        )],
    );
    // Block 1 (ts=0) expired (cutoff = 2000 - 1000 = 1000, 0 < 1000)
    assert_eq!(d2[0].get("total_volume"), Some(&Value::Float64(20.0)));
    assert_eq!(d2[0].get("trade_count"), Some(&Value::UInt64(1)));

    let d3 = mv.process_block(
        3,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(30.0)),
            ],
            4_000,
        )],
    );
    // Block 2 (ts=2000) expired (cutoff = 4000 - 1000 = 3000, 2000 < 3000)
    assert_eq!(d3[0].get("total_volume"), Some(&Value::Float64(30.0)));
}

#[test]
fn sliding_window_multiple_groups_independent_expiry() {
    let mut mv = MVEngine::new(
        sliding_sum_mv_def(3600),
        test_storage(),
        &test_column_types(),
    );

    // Group A at ts=0
    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("A".into())),
                ("volume", Value::Float64(100.0)),
            ],
            0,
        )],
    );
    // Group B at ts=3000s (within window)
    mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("B".into())),
                ("volume", Value::Float64(200.0)),
            ],
            3_000_000,
        )],
    );
    // Group A new data at ts=3601s → block 1 (group A) expires
    let d3 = mv.process_block(
        3,
        &[make_ts_row(
            &[
                ("pair", Value::String("A".into())),
                ("volume", Value::Float64(50.0)),
            ],
            3_601_000,
        )],
    );

    // Group A: block 1 expired, block 3 added → volume=50
    // Group B: block 2 (ts=3000s) still within window (cutoff=3_601_000-3_600_000=1_000)
    let a_change = d3
        .iter()
        .find(|d| d.key("pair") == Some(&Value::String("A".into())))
        .unwrap();
    assert_eq!(a_change.get("total_volume"), Some(&Value::Float64(50.0)));

    // Group B should NOT appear in changes (not touched and not expired)
    assert!(
        d3.iter()
            .all(|d| d.key("pair") != Some(&Value::String("B".into())))
    );
}

#[test]
fn sliding_window_persistence_and_restore() {
    let storage = test_storage();

    // Create MV, process blocks, finalize
    {
        let mut mv = MVEngine::new(
            sliding_sum_mv_def(3600),
            storage.clone(),
            &test_column_types(),
        );

        mv.process_block(
            1,
            &[make_ts_row(
                &[
                    ("pair", Value::String("ETH".into())),
                    ("volume", Value::Float64(100.0)),
                ],
                0,
            )],
        );
        mv.process_block(
            2,
            &[make_ts_row(
                &[
                    ("pair", Value::String("ETH".into())),
                    ("volume", Value::Float64(200.0)),
                ],
                1_000_000,
            )],
        );

        let mut batch = StorageWriteBatch::new();
        mv.advance_finality(2);
        mv.write_checkpoint(&mut batch);
        storage.commit(&batch).unwrap();
    }

    // Restore from storage
    {
        let mv = MVEngine::new(
            sliding_sum_mv_def(3600),
            storage.clone(),
            &test_column_types(),
        );

        // block_times should be restored
        assert_eq!(mv.block_times.len(), 2);
        assert!(mv.block_times.contains_key(&1));
        assert!(mv.block_times.contains_key(&2));

        // block_groups should be rebuilt
        assert!(mv.block_groups.contains_key(&1));
        assert!(mv.block_groups.contains_key(&2));

        // Aggregation state should be restored with per-block data
        assert_eq!(mv.groups.len(), 1);
    }
}

#[test]
fn sliding_window_replay_skip() {
    let storage = test_storage();

    {
        let mut mv = MVEngine::new(
            sliding_sum_mv_def(3600),
            storage.clone(),
            &test_column_types(),
        );
        mv.process_block(
            1,
            &[make_ts_row(
                &[
                    ("pair", Value::String("ETH".into())),
                    ("volume", Value::Float64(100.0)),
                ],
                0,
            )],
        );
        let mut batch = StorageWriteBatch::new();
        mv.advance_finality(1);
        mv.write_checkpoint(&mut batch);
        storage.commit(&batch).unwrap();
    }

    // Simulate restart + replay
    let mut mv = MVEngine::new(
        sliding_sum_mv_def(3600),
        storage.clone(),
        &test_column_types(),
    );

    // Replay block 1 — should be skipped (already in block_times)
    let d = mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(100.0)),
            ],
            0,
        )],
    );
    assert!(d.is_empty(), "replay of persisted block should be skipped");

    // New block 2 should work normally
    let d2 = mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("ETH".into())),
                ("volume", Value::Float64(50.0)),
            ],
            500_000,
        )],
    );
    assert_eq!(d2.len(), 1);
    assert_eq!(d2[0].get("total_volume"), Some(&Value::Float64(150.0)));
}

#[test]
fn sliding_window_out_of_order_timestamps() {
    let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage(), &test_column_types()); // 10 second window

    // Block 1 at ts=5000
    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(10.0)),
            ],
            5_000,
        )],
    );
    // Block 2 at ts=2000 (earlier than block 1!)
    mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(20.0)),
            ],
            2_000,
        )],
    );
    // Block 3 at ts=13000 → watermark=13000, cutoff=3000
    // Block 2 (ts=2000) < 3000 → expired. Block 1 (ts=5000) stays.
    let d3 = mv.process_block(
        3,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(30.0)),
            ],
            13_000,
        )],
    );
    assert_eq!(d3.len(), 1);
    // Remaining: block 1 (10) + block 3 (30) = 40
    assert_eq!(d3[0].get("total_volume"), Some(&Value::Float64(40.0)));
}

#[test]
fn sliding_window_boundary_inclusive() {
    // Test that the window boundary is inclusive (data AT cutoff is NOT expired)
    let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage(), &test_column_types()); // 10 second window

    // Block 1 at ts=0
    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(10.0)),
            ],
            0,
        )],
    );

    // Block 2 at ts=10000 → cutoff = 10000 - 10000 = 0
    // Block 1 ts=0, cutoff=0. Since we use strict less-than (ts < cutoff),
    // ts=0 is NOT less than 0, so block 1 is NOT expired.
    let d2 = mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(20.0)),
            ],
            10_000,
        )],
    );
    assert_eq!(d2.len(), 1);
    // Both blocks remain: 10 + 20 = 30
    assert_eq!(d2[0].get("total_volume"), Some(&Value::Float64(30.0)));
}

#[test]
fn sliding_window_empty_group_cleanup_on_finalize() {
    let storage = test_storage();
    let mut mv = MVEngine::new(sliding_sum_mv_def(1), storage.clone(), &test_column_types()); // 1 second window

    // Block 1 at ts=0
    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(10.0)),
            ],
            0,
        )],
    );
    // Block 2 at ts=2000 → block 1 expired, group X is now empty
    mv.process_block(
        2,
        &[make_ts_row(
            &[
                ("pair", Value::String("Y".into())),
                ("volume", Value::Float64(20.0)),
            ],
            2_000,
        )],
    );

    // Group X should be gone from self.groups (Delete change cleans it up)
    assert!(
        !mv.groups
            .keys()
            .any(|k| k.iter().any(|v| v == &Value::String("X".into())))
    );

    // Finalize
    let mut batch = StorageWriteBatch::new();
    mv.advance_finality(2);
    mv.write_checkpoint(&mut batch);
    storage.commit(&batch).unwrap();

    // Restore — group X should not exist
    let mv2 = MVEngine::new(sliding_sum_mv_def(1), storage.clone(), &test_column_types());
    assert_eq!(mv2.groups.len(), 1); // only Y
}

/// A group whose rows expire in the block that creates it must not linger
/// empty: the next live row for it is an Insert, and nothing is persisted.
#[test]
fn sliding_window_group_expired_on_arrival_is_removed() {
    let storage = test_storage();
    let mut mv = MVEngine::new(
        sliding_sum_mv_def(10),
        storage.clone(),
        &test_column_types(),
    );
    let trade = |pair: &str, volume: f64, ts_ms: i64| {
        make_ts_row(
            &[
                ("pair", Value::String(pair.into())),
                ("volume", Value::Float64(volume)),
            ],
            ts_ms,
        )
    };

    // Watermark at 20 s.
    mv.process_block(1, &[trade("ETH", 10.0, 20_000)]);

    // 1 s is 19 s behind the watermark: the block expires as it arrives.
    let late = mv.process_block(2, &[trade("BTC", 5.0, 1_000)]);
    assert!(late.is_empty());
    assert!(
        !mv.groups
            .keys()
            .any(|k| k[0] == Value::String("BTC".into()))
    );

    let mut batch = StorageWriteBatch::new();
    mv.advance_finality(2);
    mv.write_checkpoint(&mut batch);
    storage.commit(&batch).unwrap();
    let restored = MVEngine::new(
        sliding_sum_mv_def(10),
        storage.clone(),
        &test_column_types(),
    );
    assert_eq!(restored.groups.len(), 1); // only ETH

    // A zero volume leaves the sum unchanged: only an Insert carries the row.
    let live = mv.process_block(3, &[trade("BTC", 0.0, 21_000)]);
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].operation, ChangeOp::Insert);
    assert_eq!(live[0].prev_values, None);
    assert_eq!(live[0].get("total_volume"), Some(&Value::Float64(0.0)));
    assert_eq!(live[0].get("trade_count"), Some(&Value::UInt64(1)));
}

#[test]
fn sliding_window_missing_timestamp_uses_watermark() {
    let mut mv = MVEngine::new(sliding_sum_mv_def(10), test_storage(), &test_column_types()); // 10s window

    // Block 1: has timestamp
    mv.process_block(
        1,
        &[make_ts_row(
            &[
                ("pair", Value::String("X".into())),
                ("volume", Value::Float64(10.0)),
            ],
            5_000,
        )],
    );

    // Block 2: missing "ts" column — should still get a block_times entry
    mv.process_block(
        2,
        &[make_row(&[
            ("pair", Value::String("X".into())),
            ("volume", Value::Float64(20.0)),
        ])],
    );

    // block_times should have entries for both blocks
    assert!(mv.block_times.contains_key(&1));
    assert!(mv.block_times.contains_key(&2));
    // Block 2 should use watermark (5000) as fallback
    assert_eq!(mv.block_times[&2], 5_000);
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

/// The rows a consumer of the change records holds, by group.
type View = BTreeMap<String, HashMap<String, Value>>;

fn apply_changes(view: &mut View, changes: Vec<ChangeRecord>) {
    for change in changes {
        let key = format!("{:?}", change.key("grp"));
        match change.operation {
            ChangeOp::Delete => {
                view.remove(&key);
            }
            _ => {
                view.insert(key, change.values_map());
            }
        }
    }
}

/// Without count(), a block whose values are all NULL feeds no aggregate.
fn sliding_sum_only_mv_def(window_secs: u64) -> MVDef {
    let mut def = sliding_all_aggs_mv_def(window_secs);
    def.select.truncate(2);
    def
}

fn random_forks_mv(def: fn(u64) -> MVDef, storage: &Arc<dyn StorageBackend>) -> MVEngine {
    MVEngine::new(def(10), storage.clone(), &test_column_types())
}

/// The view of a fresh MV fed only `chain`.
fn replay_view(def: fn(u64) -> MVDef, chain: &[(BlockNumber, Vec<RowMap>)]) -> View {
    let mut mv = random_forks_mv(def, &test_storage());
    let mut view = View::new();
    for (block, rows) in chain {
        apply_changes(&mut view, mv.process_block(*block, rows));
    }
    view
}

/// Random blocks, forks, finalizations and reopens. After each step the view
/// must equal the view of a fresh MV fed only the blocks that remain.
fn check_random_forks(def: fn(u64) -> MVDef, seed: u64, steps: usize) {
    let storage = test_storage();
    let mut mv = random_forks_mv(def, &storage);
    let mut view = View::new();
    let mut rng = Rng::new(seed);
    let mut chain: Vec<(BlockNumber, Vec<RowMap>)> = Vec::new();
    let mut finalized: BlockNumber = 0;
    let mut durable: Option<BlockNumber> = None;
    let mut clock: i64 = 0;
    let mut log = Vec::new();

    for _ in 0..steps {
        let top = chain.last().map_or(0, |(b, _)| *b).max(finalized);

        match rng.below(100) {
            0..55 => {
                let block = top + 1 + rng.below(2);
                clock += rng.below(4_000) as i64;
                // Sometimes far behind the watermark: it expires on arrival.
                let ts = match rng.below(10) {
                    0 => clock - rng.below(20_000) as i64,
                    _ => clock,
                };
                let rows: Vec<RowMap> = (0..1 + rng.below(3))
                    .map(|_| {
                        let grp = Value::String(["A", "B", "C", "D"][rng.below(4) as usize].into());
                        let val = match rng.below(5) {
                            0 => Value::Null,
                            n => Value::Float64(n as f64),
                        };
                        match rng.below(20) {
                            0 => make_row(&[("grp", grp), ("val", val)]),
                            _ => make_ts_row(&[("grp", grp), ("val", val)], ts),
                        }
                    })
                    .collect();

                log.push(format!("block {block}: {rows:?}"));
                apply_changes(&mut view, mv.process_block(block, &rows));
                chain.push((block, rows));
            }
            55..75 => {
                let at = finalized + rng.below(top - finalized + 1);
                log.push(format!("fork {at}"));
                apply_changes(&mut view, mv.rollback(at));
                chain.retain(|(b, _)| *b <= at);
            }
            75..90 => {
                let to = finalized + rng.below(top - finalized + 1);
                log.push(format!("finalize {to}"));
                let mut batch = StorageWriteBatch::new();
                mv.advance_finality(to);
                mv.write_checkpoint(&mut batch);
                storage.commit(&batch).unwrap();
                finalized = to;
                durable = Some(to);
            }
            _ => {
                // What Settle::open does: roll the stored window back to the
                // durable block, then replay the raw rows above it.
                let Some(durable) = durable else { continue };
                log.push("reopen".into());
                mv = random_forks_mv(def, &storage);
                mv.rollback(durable);
                for (block, rows) in chain.iter().filter(|(b, _)| *b > durable) {
                    mv.replay_block(*block, rows);
                }
            }
        }

        assert_eq!(
            view,
            replay_view(def, &chain),
            "seed {seed}, after\n{}",
            log.join("\n")
        );
    }
}

#[test]
fn sliding_window_random_forks_match_replay() {
    for seed in 0..1000 {
        check_random_forks(sliding_all_aggs_mv_def, seed, 60);
        check_random_forks(sliding_sum_only_mv_def, seed, 60);
    }
}
