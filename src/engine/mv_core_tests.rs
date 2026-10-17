use super::*;
use crate::schema::ast::{AggFunc, MVDef, SelectExpr, SelectItem};
use crate::storage::memory::MemoryBackend;

fn test_storage() -> Arc<dyn StorageBackend> {
    Arc::new(MemoryBackend::new())
}

fn test_column_types() -> HashMap<String, ColumnType> {
    HashMap::from([
        ("pool".to_string(), ColumnType::String),
        ("user".to_string(), ColumnType::String),
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

fn ohlcv_mv_def() -> MVDef {
    MVDef {
        name: "candles_5m".to_string(),
        source: "trades".to_string(),
        select: vec![
            SelectItem {
                expr: SelectExpr::Column("pair".into()),
                alias: None,
            },
            SelectItem {
                expr: SelectExpr::WindowFunc {
                    column: "block_time".into(),
                    interval_seconds: 300,
                },
                alias: Some("window_start".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::First, Some("price".into())),
                alias: Some("open".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Max, Some("price".into())),
                alias: Some("high".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Min, Some("price".into())),
                alias: Some("low".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Last, Some("price".into())),
                alias: Some("close".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Sum, Some("amount".into())),
                alias: Some("volume".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Count, None),
                alias: Some("trade_count".into()),
            },
        ],
        group_by: vec!["pair".into(), "window_start".into()],
        sliding_window: None,
    }
}

fn simple_sum_mv_def() -> MVDef {
    MVDef {
        name: "volume_by_pool".to_string(),
        source: "swaps".to_string(),
        select: vec![
            SelectItem {
                expr: SelectExpr::Column("pool".into()),
                alias: None,
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Sum, Some("amount".into())),
                alias: Some("total_volume".into()),
            },
            SelectItem {
                expr: SelectExpr::Agg(AggFunc::Count, None),
                alias: Some("swap_count".into()),
            },
        ],
        group_by: vec!["pool".into()],
        sliding_window: None,
    }
}

#[test]
fn simple_mv_insert_changes() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types()).unwrap();

    let rows = vec![
        make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(10.0)),
        ]),
        make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(20.0)),
        ]),
        make_row(&[
            ("pool", Value::String("BTC/USDC".into())),
            ("amount", Value::Float64(5.0)),
        ]),
    ];

    let changes = mv.process_block(1000, &rows);

    // Two new groups -> two Insert changes
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|d| d.operation == ChangeOp::Insert));

    let eth = changes
        .iter()
        .find(|d| d.key("pool") == Some(&Value::String("ETH/USDC".into())))
        .unwrap();
    assert_eq!(eth.get("total_volume"), Some(&Value::Float64(30.0)));
    assert_eq!(eth.get("swap_count"), Some(&Value::UInt64(2)));

    let btc = changes
        .iter()
        .find(|d| d.key("pool") == Some(&Value::String("BTC/USDC".into())))
        .unwrap();
    assert_eq!(btc.get("total_volume"), Some(&Value::Float64(5.0)));
    assert_eq!(btc.get("swap_count"), Some(&Value::UInt64(1)));
}

#[test]
fn mv_update_changes_on_second_block() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types()).unwrap();

    let rows1 = vec![make_row(&[
        ("pool", Value::String("ETH/USDC".into())),
        ("amount", Value::Float64(10.0)),
    ])];
    let changes1 = mv.process_block(1000, &rows1);
    assert_eq!(changes1.len(), 1);
    assert_eq!(changes1[0].operation, ChangeOp::Insert);

    let rows2 = vec![make_row(&[
        ("pool", Value::String("ETH/USDC".into())),
        ("amount", Value::Float64(20.0)),
    ])];
    let changes2 = mv.process_block(1001, &rows2);
    assert_eq!(changes2.len(), 1);
    assert_eq!(changes2[0].operation, ChangeOp::Update);
    assert_eq!(changes2[0].get("total_volume"), Some(&Value::Float64(30.0)));
    assert_eq!(
        changes2[0].prev("total_volume"),
        Some(&Value::Float64(10.0))
    );
}

#[test]
fn mv_rollback_produces_update_change() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types()).unwrap();

    mv.process_block(
        1000,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(10.0)),
        ])],
    );
    mv.process_block(
        1001,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(20.0)),
        ])],
    );

    let changes = mv.rollback(1000);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].operation, ChangeOp::Update);
    assert_eq!(changes[0].get("total_volume"), Some(&Value::Float64(10.0)));
    assert_eq!(changes[0].prev("total_volume"), Some(&Value::Float64(30.0)));
}

#[test]
fn mv_rollback_produces_delete_when_empty() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types()).unwrap();

    mv.process_block(
        1000,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(10.0)),
        ])],
    );

    let changes = mv.rollback(999); // rollback everything
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].operation, ChangeOp::Delete);
}

#[test]
fn mv_rollback_noop_when_nothing_to_rollback() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types()).unwrap();

    mv.process_block(
        1000,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(10.0)),
        ])],
    );

    let changes = mv.rollback(1000);
    assert!(changes.is_empty());
}

#[test]
fn mv_finalize_then_rollback() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types()).unwrap();

    mv.process_block(
        1000,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(10.0)),
        ])],
    );
    mv.process_block(
        1001,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(20.0)),
        ])],
    );
    mv.process_block(
        1002,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(30.0)),
        ])],
    );

    let mut batch = StorageWriteBatch::new();
    mv.advance_finality(1001);
    mv.write_checkpoint(&mut batch);

    // Rollback block 1002
    let changes = mv.rollback(1001);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].operation, ChangeOp::Update);
    // Finalized sum: 10+20=30, block 1002 removed
    assert_eq!(changes[0].get("total_volume"), Some(&Value::Float64(30.0)));
}

#[test]
fn ohlcv_candle_end_to_end() {
    let mut mv = MVEngine::new(ohlcv_mv_def(), test_storage(), &test_column_types()).unwrap();

    // All trades in same 5-min window (block_time within same 300s interval)
    let window_base = 1_700_000_000_000i64; // some ms timestamp

    // Block 1000: ETH/USDC price=100, amount=1
    mv.process_block(
        1000,
        &[make_row(&[
            ("pair", Value::String("ETH/USDC".into())),
            ("block_time", Value::DateTime(window_base + 10_000)),
            ("price", Value::Float64(100.0)),
            ("amount", Value::Float64(1.0)),
        ])],
    );

    // Block 1001: price=110, amount=2
    mv.process_block(
        1001,
        &[make_row(&[
            ("pair", Value::String("ETH/USDC".into())),
            ("block_time", Value::DateTime(window_base + 20_000)),
            ("price", Value::Float64(110.0)),
            ("amount", Value::Float64(2.0)),
        ])],
    );

    // Block 1002: price=90, amount=3
    mv.process_block(
        1002,
        &[make_row(&[
            ("pair", Value::String("ETH/USDC".into())),
            ("block_time", Value::DateTime(window_base + 30_000)),
            ("price", Value::Float64(90.0)),
            ("amount", Value::Float64(3.0)),
        ])],
    );

    // Block 1003: price=200, amount=10 (will be rolled back)
    mv.process_block(
        1003,
        &[make_row(&[
            ("pair", Value::String("ETH/USDC".into())),
            ("block_time", Value::DateTime(window_base + 40_000)),
            ("price", Value::Float64(200.0)),
            ("amount", Value::Float64(10.0)),
        ])],
    );

    // Rollback block 1003
    let changes = mv.rollback(1002);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].operation, ChangeOp::Update);

    let vals = &changes[0];
    assert_eq!(vals.get("open"), Some(&Value::Float64(100.0)));
    assert_eq!(vals.get("high"), Some(&Value::Float64(110.0))); // was 200, now 110
    assert_eq!(vals.get("low"), Some(&Value::Float64(90.0)));
    assert_eq!(vals.get("close"), Some(&Value::Float64(90.0))); // was 200, now 90
    assert_eq!(vals.get("volume"), Some(&Value::Float64(6.0))); // was 16, now 6
    assert_eq!(vals.get("trade_count"), Some(&Value::UInt64(3))); // was 4, now 3
}

#[test]
fn ohlcv_multiple_pairs_isolated() {
    let mut mv = MVEngine::new(ohlcv_mv_def(), test_storage(), &test_column_types()).unwrap();
    let ts = 1_700_000_000_000i64;

    mv.process_block(
        1000,
        &[
            make_row(&[
                ("pair", Value::String("ETH/USDC".into())),
                ("block_time", Value::DateTime(ts)),
                ("price", Value::Float64(100.0)),
                ("amount", Value::Float64(1.0)),
            ]),
            make_row(&[
                ("pair", Value::String("BTC/USDC".into())),
                ("block_time", Value::DateTime(ts)),
                ("price", Value::Float64(50000.0)),
                ("amount", Value::Float64(0.1)),
            ]),
        ],
    );

    // Rollback block 1000 — both groups should be deleted
    let changes = mv.rollback(999);
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|d| d.operation == ChangeOp::Delete));
}

#[test]
fn mv_different_time_windows() {
    let mut mv = MVEngine::new(ohlcv_mv_def(), test_storage(), &test_column_types()).unwrap();

    // Two trades in different 5-min windows
    let window1 = 1_700_000_000_000i64;
    let window2 = window1 + 300_000; // +5 minutes

    mv.process_block(
        1000,
        &[
            make_row(&[
                ("pair", Value::String("ETH/USDC".into())),
                ("block_time", Value::DateTime(window1 + 1000)),
                ("price", Value::Float64(100.0)),
                ("amount", Value::Float64(1.0)),
            ]),
            make_row(&[
                ("pair", Value::String("ETH/USDC".into())),
                ("block_time", Value::DateTime(window2 + 1000)),
                ("price", Value::Float64(200.0)),
                ("amount", Value::Float64(2.0)),
            ]),
        ],
    );

    // Should produce 2 Insert changes (different windows = different groups)
    // Already consumed by process_block, let's check via rollback
    let changes = mv.rollback(999);
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|d| d.operation == ChangeOp::Delete));
}

#[test]
fn full_cycle_ingest_rollback_reingest() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types()).unwrap();

    // Block 1000
    mv.process_block(
        1000,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(10.0)),
        ])],
    );
    // Block 1001
    mv.process_block(
        1001,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(20.0)),
        ])],
    );
    // Block 1002 (will be rolled back)
    mv.process_block(
        1002,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(100.0)),
        ])],
    );

    // Rollback block 1002
    let rollback_changes = mv.rollback(1001);
    assert_eq!(rollback_changes.len(), 1);
    assert_eq!(
        rollback_changes[0].get("total_volume"),
        Some(&Value::Float64(30.0))
    );

    // Re-ingest block 1002 with different data (reorg)
    let new_changes = mv.process_block(
        1002,
        &[make_row(&[
            ("pool", Value::String("ETH/USDC".into())),
            ("amount", Value::Float64(5.0)),
        ])],
    );
    assert_eq!(new_changes.len(), 1);
    assert_eq!(new_changes[0].operation, ChangeOp::Update);
    assert_eq!(
        new_changes[0].get("total_volume"),
        Some(&Value::Float64(35.0))
    );
}

#[test]
fn non_sliding_mv_finalize_deletes_empty_group_from_storage() {
    let storage = test_storage();

    // Phase 1: Two groups persisted — ETH/USDC and BTC/USDC
    {
        let mut mv = MVEngine::new(simple_sum_mv_def(), storage.clone(), &test_column_types()).unwrap();
        mv.process_block(
            1,
            &[make_row(&[
                ("pool", Value::String("ETH/USDC".into())),
                ("amount", Value::Float64(10.0)),
            ])],
        );
        mv.process_block(
            2,
            &[make_row(&[
                ("pool", Value::String("BTC/USDC".into())),
                ("amount", Value::Float64(20.0)),
            ])],
        );
        let mut batch = StorageWriteBatch::new();
        mv.advance_finality(1);
        mv.write_checkpoint(&mut batch);
        storage.commit(&batch).unwrap();
    }

    // Phase 2: Rollback block 2 (above finalized=1) — BTC/USDC group empties
    {
        let mut mv = MVEngine::new(simple_sum_mv_def(), storage.clone(), &test_column_types()).unwrap();
        // Replay unfinalized block 2
        mv.process_block(
            2,
            &[make_row(&[
                ("pool", Value::String("BTC/USDC".into())),
                ("amount", Value::Float64(20.0)),
            ])],
        );
        // Rollback to block 1, removing block 2
        let changes = mv.rollback(1);
        // BTC/USDC should get a Delete change (it only had unfinalized data)
        assert!(changes.iter().any(|d| d.operation == ChangeOp::Delete));

        let mut batch = StorageWriteBatch::new();
        mv.advance_finality(1);
        mv.write_checkpoint(&mut batch);
        storage.commit(&batch).unwrap();
    }

    // Phase 3: Restore — BTC/USDC should not be resurrected
    {
        let mv = MVEngine::new(simple_sum_mv_def(), storage.clone(), &test_column_types()).unwrap();
        assert_eq!(mv.groups.len(), 1, "only ETH/USDC should survive");
    }
}

fn pool_row(pool: &str, amount: f64) -> RowMap {
    make_row(&[
        ("pool", Value::String(pool.into())),
        ("amount", Value::Float64(amount)),
    ])
}

/// The pools whose group state a finalize wrote into `batch`.
fn persisted_pools(batch: &StorageWriteBatch) -> Vec<Value> {
    let mut pools: Vec<Value> = batch
        .ops
        .iter()
        .filter_map(|op| match op {
            storage::BatchOp::PutMvState { group_key, .. } => {
                Some(storage::decode_group_key(group_key).unwrap()[0].clone())
            }
            _ => None,
        })
        .collect();
    pools.sort_by_key(|v| format!("{v:?}"));
    pools
}

fn finalize_and_commit(
    mv: &mut MVEngine,
    storage: &Arc<dyn StorageBackend>,
    block: BlockNumber,
    checkpoint: bool,
) -> StorageWriteBatch {
    let mut batch = StorageWriteBatch::new();
    mv.advance_finality(block);
    if checkpoint {
        mv.write_checkpoint(&mut batch);
    }
    storage.commit(&batch).unwrap();
    batch
}

/// Finality can reach a group's block in a later finalize that has no rows
/// for the group. Its finalized state must still be persisted, because
/// recovery replays only the blocks above finality.
#[test]
fn finalize_persists_group_when_finality_catches_up() {
    let storage = test_storage();

    {
        let mut mv = MVEngine::new(simple_sum_mv_def(), storage.clone(), &test_column_types()).unwrap();
        mv.process_block(1, &[pool_row("ETH/USDC", 10.0)]);
        finalize_and_commit(&mut mv, &storage, 0, true);
        finalize_and_commit(&mut mv, &storage, 1, true);
    }

    let mut mv = MVEngine::new(simple_sum_mv_def(), storage, &test_column_types()).unwrap();
    let changes = mv.process_block(2, &[pool_row("ETH/USDC", 10.0)]);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].operation, ChangeOp::Update);
    assert_eq!(changes[0].get("total_volume"), Some(&Value::Float64(20.0)));
    assert_eq!(changes[0].get("swap_count"), Some(&Value::UInt64(2)));
}

/// Same as above, but the finalize that reaches the block is deferred
/// (backfill) and a later checkpoint must write the group.
#[test]
fn checkpoint_persists_group_finalized_by_deferred_finalize() {
    let storage = test_storage();

    {
        let mut mv = MVEngine::new(simple_sum_mv_def(), storage.clone(), &test_column_types()).unwrap();
        mv.process_block(1, &[pool_row("ETH/USDC", 10.0)]);
        finalize_and_commit(&mut mv, &storage, 0, true);

        let deferred = finalize_and_commit(&mut mv, &storage, 1, false);
        assert!(deferred.ops.is_empty(), "a deferred finalize writes nothing");

        let checkpoint = finalize_and_commit(&mut mv, &storage, 1, true);
        assert_eq!(
            persisted_pools(&checkpoint),
            vec![Value::String("ETH/USDC".into())]
        );
    }

    let mut mv = MVEngine::new(simple_sum_mv_def(), storage, &test_column_types()).unwrap();
    let changes = mv.process_block(2, &[pool_row("ETH/USDC", 10.0)]);
    assert_eq!(changes[0].get("total_volume"), Some(&Value::Float64(20.0)));
}

/// A finalize writes exactly the groups of the blocks it finalizes: a group
/// touched only above finality has no new finalized state yet.
#[test]
fn finalize_persists_groups_of_finalized_blocks_only() {
    let storage = test_storage();
    let mut mv = MVEngine::new(simple_sum_mv_def(), storage.clone(), &test_column_types()).unwrap();

    mv.process_block(1, &[pool_row("A", 1.0)]);
    mv.process_block(2, &[pool_row("B", 2.0)]);
    mv.process_block(3, &[pool_row("A", 3.0), pool_row("C", 3.0)]);

    let batch = finalize_and_commit(&mut mv, &storage, 1, true);
    assert_eq!(persisted_pools(&batch), vec![Value::String("A".into())]);

    let batch = finalize_and_commit(&mut mv, &storage, 2, true);
    assert_eq!(persisted_pools(&batch), vec![Value::String("B".into())]);

    let batch = finalize_and_commit(&mut mv, &storage, 2, true);
    assert!(persisted_pools(&batch).is_empty(), "nothing new to finalize");

    let batch = finalize_and_commit(&mut mv, &storage, 3, true);
    assert_eq!(
        persisted_pools(&batch),
        vec![Value::String("A".into()), Value::String("C".into())]
    );

    // Every group is on disk with all its finalized data.
    let restored = MVEngine::new(simple_sum_mv_def(), storage, &test_column_types()).unwrap();
    let total = |pool: &str| {
        let key: GroupKey = std::iter::once(Value::String(pool.into())).collect();
        restored.groups[&key][0].current_value()
    };
    assert_eq!(total("A"), Value::Float64(4.0));
    assert_eq!(total("B"), Value::Float64(2.0));
    assert_eq!(total("C"), Value::Float64(3.0));
}

/// Replay drops a group that holds no data, as `process_block` does: if it
/// stayed, the group's next row would read as an Update.
#[test]
fn replay_block_drops_group_without_data() {
    let mut def = simple_sum_mv_def();
    def.select.truncate(2);
    let mut mv = MVEngine::new(def, test_storage(), &test_column_types()).unwrap();

    let pool = ("pool", Value::String("ETH/USDC".into()));
    mv.replay_block(1000, &[make_row(&[pool.clone(), ("amount", Value::Null)])]);

    let changes = mv.process_block(1001, &[make_row(&[pool, ("amount", Value::Float64(5.0))])]);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].operation, ChangeOp::Insert);
}
