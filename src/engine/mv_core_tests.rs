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
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types());

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
        .find(|d| d.key.get("pool") == Some(&Value::String("ETH/USDC".into())))
        .unwrap();
    assert_eq!(eth.values.get("total_volume"), Some(&Value::Float64(30.0)));
    assert_eq!(eth.values.get("swap_count"), Some(&Value::UInt64(2)));

    let btc = changes
        .iter()
        .find(|d| d.key.get("pool") == Some(&Value::String("BTC/USDC".into())))
        .unwrap();
    assert_eq!(btc.values.get("total_volume"), Some(&Value::Float64(5.0)));
    assert_eq!(btc.values.get("swap_count"), Some(&Value::UInt64(1)));
}

#[test]
fn mv_update_changes_on_second_block() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types());

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
    assert_eq!(
        changes2[0].values.get("total_volume"),
        Some(&Value::Float64(30.0))
    );
    assert_eq!(
        changes2[0].prev_values.as_ref().unwrap().get("total_volume"),
        Some(&Value::Float64(10.0))
    );
}

#[test]
fn mv_rollback_produces_update_change() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types());

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
    assert_eq!(
        changes[0].values.get("total_volume"),
        Some(&Value::Float64(10.0))
    );
    assert_eq!(
        changes[0].prev_values.as_ref().unwrap().get("total_volume"),
        Some(&Value::Float64(30.0))
    );
}

#[test]
fn mv_rollback_produces_delete_when_empty() {
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types());

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
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types());

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
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types());

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
    mv.finalize(1001, &mut batch, true);

    // Rollback block 1002
    let changes = mv.rollback(1001);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].operation, ChangeOp::Update);
    // Finalized sum: 10+20=30, block 1002 removed
    assert_eq!(
        changes[0].values.get("total_volume"),
        Some(&Value::Float64(30.0))
    );
}

#[test]
fn ohlcv_candle_end_to_end() {
    let mut mv = MVEngine::new(ohlcv_mv_def(), test_storage(), &test_column_types());

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

    let vals = &changes[0].values;
    assert_eq!(vals.get("open"), Some(&Value::Float64(100.0)));
    assert_eq!(vals.get("high"), Some(&Value::Float64(110.0))); // was 200, now 110
    assert_eq!(vals.get("low"), Some(&Value::Float64(90.0)));
    assert_eq!(vals.get("close"), Some(&Value::Float64(90.0))); // was 200, now 90
    assert_eq!(vals.get("volume"), Some(&Value::Float64(6.0))); // was 16, now 6
    assert_eq!(vals.get("trade_count"), Some(&Value::UInt64(3))); // was 4, now 3
}

#[test]
fn ohlcv_multiple_pairs_isolated() {
    let mut mv = MVEngine::new(ohlcv_mv_def(), test_storage(), &test_column_types());
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
    let mut mv = MVEngine::new(ohlcv_mv_def(), test_storage(), &test_column_types());

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
    let mut mv = MVEngine::new(simple_sum_mv_def(), test_storage(), &test_column_types());

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
        rollback_changes[0].values.get("total_volume"),
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
        new_changes[0].values.get("total_volume"),
        Some(&Value::Float64(35.0))
    );
}

#[test]
fn non_sliding_mv_finalize_deletes_empty_group_from_storage() {
    let storage = test_storage();

    // Phase 1: Two groups persisted — ETH/USDC and BTC/USDC
    {
        let mut mv = MVEngine::new(simple_sum_mv_def(), storage.clone(), &test_column_types());
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
        mv.finalize(1, &mut batch, true);
        storage.commit(&batch).unwrap();
    }

    // Phase 2: Rollback block 2 (above finalized=1) — BTC/USDC group empties
    {
        let mut mv = MVEngine::new(simple_sum_mv_def(), storage.clone(), &test_column_types());
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
        mv.finalize(1, &mut batch, true);
        storage.commit(&batch).unwrap();
    }

    // Phase 3: Restore — BTC/USDC should not be resurrected
    {
        let mv = MVEngine::new(simple_sum_mv_def(), storage.clone(), &test_column_types());
        assert_eq!(mv.groups.len(), 1, "only ETH/USDC should survive");
    }
}
