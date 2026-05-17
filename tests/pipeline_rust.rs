//! End-to-end tests for the Rust `Pipeline` builder.

use std::collections::HashMap;

use settle::db::IngestInput;
use settle::test_helpers::ingest_input;
use settle::types::{BlockCursor, RowMap, Value};
use settle::{
    BuildOptions, Pipeline, ReducerOptions, ViewOptions, datetime, interval, string, uint64,
};

fn cursor(n: u64) -> BlockCursor {
    BlockCursor {
        number: n,
        hash: format!("0x{n:x}"),
    }
}

fn row(pairs: &[(&str, Value)]) -> RowMap {
    pairs.iter().cloned().map(|(k, v)| (k.to_string(), v)).collect()
}

#[test]
fn market_stats_reducer_and_token_summary_view() {
    let mut p = Pipeline::new();

    let orders = p.table(
        "orders",
        [
            ("block_number", uint64()),
            ("trader", string()),
            ("asset_id", string()),
            ("usdc", uint64()),
            ("ts", datetime()),
        ],
    );

    let stats = orders.create_reducer(
        "market_stats",
        ReducerOptions {
            group_by: vec!["asset_id".into()],
            initial_state: row(&[
                ("volume", Value::Float64(0.0)),
                ("trades", Value::UInt64(0)),
            ]),
            reduce: Box::new(|state, row| {
                let usdc = row.get("usdc").and_then(Value::as_f64).unwrap_or(0.0);
                let vol = usdc / 1_000_000.0;
                let new_volume = state.get_f64("volume") + vol;
                let new_trades = state.get_u64("trades") + 1;
                state.update(HashMap::from([
                    ("volume".into(), Value::Float64(new_volume)),
                    ("trades".into(), Value::UInt64(new_trades)),
                ]));
                state.emit(HashMap::from([
                    (
                        "asset_id".into(),
                        row.get("asset_id").cloned().unwrap_or(Value::Null),
                    ),
                    ("volume_running".into(), Value::Float64(new_volume)),
                ]));
            }),
        },
    );

    stats.create_view(
        "token_summary",
        ViewOptions {
            group_by: vec!["asset_id".into()],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("asset_id".into(), agg.key("asset_id").into()),
                    ("total_volume".into(), agg.sum("volume_running").into()),
                    ("last_volume".into(), agg.last("volume_running").into()),
                    ("event_count".into(), agg.count().into()),
                ]
            }),
        },
    );

    let mut db = p.build(BuildOptions::new()).expect("build");

    // Two orders for asset A, one for asset B in block 1.
    let mut data = HashMap::new();
    data.insert(
        "orders".to_string(),
        vec![
            row(&[
                ("block_number", Value::UInt64(1)),
                ("trader", Value::String("alice".into())),
                ("asset_id", Value::String("A".into())),
                ("usdc", Value::UInt64(2_000_000)),
                ("ts", Value::DateTime(1_700_000_000)),
            ]),
            row(&[
                ("block_number", Value::UInt64(1)),
                ("trader", Value::String("bob".into())),
                ("asset_id", Value::String("A".into())),
                ("usdc", Value::UInt64(3_000_000)),
                ("ts", Value::DateTime(1_700_000_001)),
            ]),
            row(&[
                ("block_number", Value::UInt64(1)),
                ("trader", Value::String("carol".into())),
                ("asset_id", Value::String("B".into())),
                ("usdc", Value::UInt64(7_000_000)),
                ("ts", Value::DateTime(1_700_000_002)),
            ]),
        ],
    );

    let batch = ingest_input(&mut db, IngestInput {
            data,
            rollback_chain: vec![],
            finalized_head: cursor(1),
        })
        .expect("ingest")
        .expect("batch");

    // Reducer emits feed downstream MVs but don't surface as ChangeRecords —
    // only MV output does. token_summary should have one row per asset.
    let view_records = batch
        .tables
        .get("token_summary")
        .expect("token_summary records present");
    assert!(
        view_records.iter().any(|r| matches!(r.values.get("asset_id"), Some(Value::String(s)) if s == "A")),
        "view must contain asset A"
    );
    assert!(
        view_records.iter().any(|r| matches!(r.values.get("asset_id"), Some(Value::String(s)) if s == "B")),
        "view must contain asset B"
    );

    // event_count for asset A == 2; for asset B == 1.
    for r in view_records {
        let asset = match r.values.get("asset_id") {
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };
        let count = r.values.get("event_count").and_then(Value::as_u64).unwrap_or(0);
        match asset.as_str() {
            "A" => assert_eq!(count, 2, "asset A should have count=2"),
            "B" => assert_eq!(count, 1, "asset B should have count=1"),
            _ => {}
        }
    }
}

#[test]
fn sliding_window_view_via_pipeline() {
    use settle::SlidingWindowOptions;

    let mut p = Pipeline::new();

    let orders = p.table(
        "orders",
        [
            ("block_number", uint64()),
            ("asset_id", string()),
            ("usdc", uint64()),
            ("ts", datetime()),
        ],
    );

    let stats = orders.create_reducer(
        "market_stats",
        ReducerOptions {
            group_by: vec!["asset_id".into()],
            initial_state: row(&[("volume".into(), Value::Float64(0.0))]),
            reduce: Box::new(|state, row| {
                let usdc = row.get("usdc").and_then(Value::as_f64).unwrap_or(0.0);
                let vol = usdc / 1_000_000.0;
                let new_volume = state.get_f64("volume") + vol;
                state.update(HashMap::from([(
                    "volume".into(),
                    Value::Float64(new_volume),
                )]));
                state.emit(HashMap::from([
                    (
                        "asset_id".into(),
                        row.get("asset_id").cloned().unwrap_or(Value::Null),
                    ),
                    ("ts".into(), row.get("ts").cloned().unwrap_or(Value::Null)),
                    ("volume_running".into(), Value::Float64(new_volume)),
                ]));
            }),
        },
    );

    // Sliding 5-minute window per asset, plus a per-bucket view using interval().
    stats.create_view(
        "rolling_5m",
        ViewOptions {
            group_by: vec!["asset_id".into()],
            sliding_window: Some(SlidingWindowOptions {
                interval: "5 minutes".into(),
                time_column: "ts".into(),
            }),
            select: Box::new(|agg| {
                vec![
                    ("asset_id".into(), agg.key("asset_id").into()),
                    ("rolling_vol".into(), agg.sum("volume_running").into()),
                ]
            }),
        },
    );

    stats.create_view(
        "buckets_5m",
        ViewOptions {
            group_by: vec![
                "asset_id".into(),
                interval("ts", "5 minutes").r#as("window_start").into(),
            ],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("asset_id".into(), agg.key("asset_id").into()),
                    ("window_start".into(), agg.key("window_start").into()),
                    ("bucket_vol".into(), agg.sum("volume_running").into()),
                ]
            }),
        },
    );

    let ddl = p.to_ddl().expect("ddl");
    assert!(
        ddl.contains("WINDOW SLIDING INTERVAL 300 SECOND BY ts"),
        "expected sliding-window clause in DDL, got:\n{ddl}"
    );
    assert!(
        ddl.contains("toStartOfInterval(ts, INTERVAL 300 SECOND) AS window_start"),
        "expected interval bucket projection, got:\n{ddl}"
    );

    // Build must succeed (i.e. parser accepts our DDL).
    let mut db = p.build(BuildOptions::new()).expect("build");

    let mut data = HashMap::new();
    data.insert(
        "orders".to_string(),
        vec![
            row(&[
                ("block_number", Value::UInt64(1)),
                ("asset_id", Value::String("A".into())),
                ("usdc", Value::UInt64(1_000_000)),
                ("ts", Value::DateTime(1_700_000_000_000)),
            ]),
            row(&[
                ("block_number", Value::UInt64(1)),
                ("asset_id", Value::String("A".into())),
                ("usdc", Value::UInt64(2_000_000)),
                ("ts", Value::DateTime(1_700_000_010_000)),
            ]),
        ],
    );

    let batch = ingest_input(&mut db, IngestInput {
            data,
            rollback_chain: vec![],
            finalized_head: cursor(1),
        })
        .expect("ingest")
        .expect("batch");

    assert!(batch.tables.contains_key("rolling_5m"));
    assert!(batch.tables.contains_key("buckets_5m"));
}

#[test]
fn virtual_table_emits_no_changes() {
    let mut p = Pipeline::new();
    p.virtual_table(
        "prices",
        [
            ("block_number", uint64()),
            ("asset_id", string()),
            ("price", uint64()),
        ],
    );
    p.table(
        "transfers",
        [
            ("block_number", uint64()),
            ("from", string()),
            ("to", string()),
            ("value", uint64()),
        ],
    );

    let ddl = p.to_ddl().unwrap();
    assert!(ddl.contains("CREATE VIRTUAL TABLE prices"));
    assert!(ddl.contains("CREATE TABLE transfers"));

    let mut db = p.build(BuildOptions::new()).expect("build");

    let mut data = HashMap::new();
    data.insert(
        "transfers".to_string(),
        vec![row(&[
            ("block_number", Value::UInt64(1)),
            ("from", Value::String("0xA".into())),
            ("to", Value::String("0xB".into())),
            ("value", Value::UInt64(100)),
        ])],
    );
    data.insert(
        "prices".to_string(),
        vec![row(&[
            ("block_number", Value::UInt64(1)),
            ("asset_id", Value::String("A".into())),
            ("price", Value::UInt64(42)),
        ])],
    );

    let batch = ingest_input(&mut db, IngestInput {
            data,
            rollback_chain: vec![],
            finalized_head: cursor(1),
        })
        .expect("ingest")
        .expect("batch");

    assert!(batch.tables.contains_key("transfers"));
    assert!(
        !batch.tables.contains_key("prices"),
        "virtual table must not emit change records"
    );
}

// ─── Coverage filler tests ──────────────────────────────────────────────

#[test]
fn all_column_factories_compile_and_emit_correct_sql() {
    use settle::{
        base58, boolean, bytes, datetime, float64, int64, json, string, uint256, uint64,
    };
    let mut p = Pipeline::new();
    p.table(
        "all_types",
        [
            ("a", uint64()),
            ("b", int64()),
            ("c", float64()),
            ("d", uint256()),
            ("e", string()),
            ("f", datetime()),
            ("g", boolean()),
            ("h", bytes()),
            ("i", base58()),
            ("j", json()),
        ],
    );
    let ddl = p.to_ddl().unwrap();
    for piece in [
        "a UInt64",
        "b Int64",
        "c Float64",
        "d Uint256",
        "e String",
        "f DateTime",
        "g Boolean",
        "h Bytes",
        "i Base58",
        "j Json",
    ] {
        assert!(ddl.contains(piece), "DDL missing '{piece}': {ddl}");
    }
}

#[test]
fn agg_proxy_exposes_every_aggregation() {
    let mut p = Pipeline::new();
    let raw = p.table(
        "raw",
        [("block_number", uint64()), ("v", uint64()), ("g", string())],
    );
    let r = raw.create_reducer(
        "r",
        ReducerOptions {
            group_by: vec!["g".into()],
            initial_state: row(&[("acc", Value::Float64(0.0))]),
            reduce: Box::new(|s, _row| {
                s.update(HashMap::from([("acc".into(), Value::Float64(s.get_f64("acc")))]));
                s.emit(HashMap::from([("v".into(), Value::Float64(0.0))]));
            }),
        },
    );
    r.create_view(
        "summary",
        ViewOptions {
            group_by: vec!["g".into()],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("g".into(), agg.key("g").into()),
                    ("s".into(), agg.sum("v").into()),
                    ("n".into(), agg.count().into()),
                    ("a".into(), agg.avg("v").into()),
                    ("mn".into(), agg.min("v").into()),
                    ("mx".into(), agg.max("v").into()),
                    ("f".into(), agg.first("v").into()),
                    ("l".into(), agg.last("v").into()),
                ]
            }),
        },
    );
    let ddl = p.to_ddl().unwrap();
    for func in ["sum(v)", "count()", "avg(v)", "min(v)", "max(v)", "first(v)", "last(v)"] {
        assert!(ddl.contains(func), "DDL missing {func}: {ddl}");
    }
    // Build must succeed (parser accepts every aggregation).
    let _db = p.build(BuildOptions::new()).expect("build");
}

#[test]
fn parse_duration_units_and_errors() {
    use settle::parse_duration;
    assert_eq!(parse_duration("30 seconds").unwrap(), 30);
    assert_eq!(parse_duration("1m").unwrap(), 60);
    assert_eq!(parse_duration("2 hours").unwrap(), 7200);
    assert_eq!(parse_duration("1 day").unwrap(), 86400);
    assert!(parse_duration("nonsense").is_err());
    assert!(parse_duration("10 fortnights").is_err());
}

#[test]
fn interval_as_alias_appears_in_ddl() {
    let mut p = Pipeline::new();
    let raw = p.table(
        "events",
        [
            ("block_number", uint64()),
            ("ts", datetime()),
            ("user", string()),
        ],
    );
    let r = raw.create_reducer(
        "by_user",
        ReducerOptions {
            group_by: vec!["user".into()],
            initial_state: row(&[("c", Value::UInt64(0))]),
            reduce: Box::new(|s, row| {
                let c = s.get_u64("c") + 1;
                s.update(HashMap::from([("c".into(), Value::UInt64(c))]));
                s.emit(HashMap::from([
                    ("user".into(), row.get("user").cloned().unwrap_or(Value::Null)),
                    ("ts".into(), row.get("ts").cloned().unwrap_or(Value::Null)),
                ]));
            }),
        },
    );
    r.create_view(
        "hourly",
        ViewOptions {
            group_by: vec![
                "user".into(),
                interval("ts", "1 hour").r#as("hour_start").into(),
            ],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("user".into(), agg.key("user").into()),
                    ("hour_start".into(), agg.key("hour_start").into()),
                    ("n".into(), agg.count().into()),
                ]
            }),
        },
    );
    let ddl = p.to_ddl().unwrap();
    assert!(
        ddl.contains("toStartOfInterval(ts, INTERVAL 3600 SECOND) AS hour_start"),
        "{ddl}"
    );
    assert!(ddl.contains("GROUP BY user, hour_start"));
}

#[test]
fn state_ctx_typed_accessors_and_set() {
    use std::sync::{Arc, Mutex};
    let captured: Arc<Mutex<Vec<(i64, bool, String, f64)>>> = Arc::new(Mutex::new(vec![]));
    let cap = captured.clone();

    let mut p = Pipeline::new();
    let raw = p.table(
        "in",
        [("block_number", uint64()), ("v", uint64()), ("k", string())],
    );
    raw.create_reducer(
        "r",
        ReducerOptions {
            group_by: vec!["k".into()],
            initial_state: row(&[
                ("i", Value::Int64(1)),
                ("b", Value::Boolean(true)),
                ("s", Value::String("hello".into())),
                ("f", Value::Float64(2.5)),
            ]),
            reduce: Box::new(move |state, _row| {
                cap.lock().unwrap().push((
                    state.get_i64("i"),
                    state.get_bool("b"),
                    state.get_str("s").to_string(),
                    state.get_f64("f"),
                ));
                state.set("f", Value::Float64(state.get_f64("f") + 1.0));
                state.emit(HashMap::new());
            }),
        },
    );
    let mut db = p.build(BuildOptions::new()).expect("build");

    let mut data = HashMap::new();
    data.insert(
        "in".into(),
        vec![
            row(&[
                ("block_number", Value::UInt64(1)),
                ("v", Value::UInt64(10)),
                ("k", Value::String("g".into())),
            ]),
            row(&[
                ("block_number", Value::UInt64(1)),
                ("v", Value::UInt64(20)),
                ("k", Value::String("g".into())),
            ]),
        ],
    );
    ingest_input(&mut db, IngestInput {
        data,
        rollback_chain: vec![],
        finalized_head: cursor(1),
    })
    .unwrap();

    let calls = captured.lock().unwrap();
    assert_eq!(calls.len(), 2);
    // First row sees defaults, second sees set("f", 2.5+1.0)
    assert_eq!(calls[0], (1, true, "hello".into(), 2.5));
    assert_eq!(calls[1], (1, true, "hello".into(), 3.5));
}

#[test]
fn handle_accessors_expose_names() {
    let mut p = Pipeline::new();
    let raw = p.table("orders", [("block_number", uint64()), ("g", string())]);
    assert_eq!(raw.name(), "orders");

    let r = raw.create_reducer(
        "by_g",
        ReducerOptions {
            group_by: vec!["g".into()],
            initial_state: row(&[("n", Value::UInt64(0))]),
            reduce: Box::new(|s, _row| {
                s.update(HashMap::from([(
                    "n".into(),
                    Value::UInt64(s.get_u64("n") + 1),
                )]));
                s.emit(HashMap::new());
            }),
        },
    );
    assert_eq!(r.name(), "by_g");

    // ReducerHandle::create_reducer (chained) and create_view
    let chained = r.create_reducer(
        "chained",
        ReducerOptions {
            group_by: vec!["g".into()],
            initial_state: row(&[("c", Value::UInt64(0))]),
            reduce: Box::new(|s, _row| {
                s.update(HashMap::from([(
                    "c".into(),
                    Value::UInt64(s.get_u64("c") + 1),
                )]));
                s.emit(HashMap::new());
            }),
        },
    );
    assert_eq!(chained.name(), "chained");

    let view = r.create_view(
        "view",
        ViewOptions {
            group_by: vec!["g".into()],
            sliding_window: None,
            select: Box::new(|agg| vec![("n".into(), agg.count().into())]),
        },
    );
    assert_eq!(view.name(), "view");
}

#[test]
fn pipeline_default_constructs_empty_builder() {
    let p: Pipeline = Pipeline::default();
    assert_eq!(p.to_ddl().unwrap(), "");
}

#[test]
fn build_fails_when_reducer_groups_by_unknown_column() {
    let mut p = Pipeline::new();
    let t = p.table("orders", [("block_number", uint64()), ("g", string())]);
    t.create_reducer(
        "bad",
        ReducerOptions {
            group_by: vec!["nope".into()],
            initial_state: row(&[("n", Value::UInt64(0))]),
            reduce: Box::new(|_s, _row| {}),
        },
    );
    // The schema parser/validator must reject an unknown GROUP BY column.
    assert!(p.build(BuildOptions::new()).is_err());
}
