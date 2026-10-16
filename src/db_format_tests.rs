//! The format version and the stored layouts: open must refuse stored data
//! that the schema would read wrong.

use super::*;
use crate::schema::ast::ReducerDef;
use crate::storage::format::FORMAT_VERSION;
use crate::test_helpers::ingest_with_finalized;
use crate::types::Value;
use std::collections::HashMap;

const SCHEMA: &str = r#"
    CREATE TABLE transfers (
        block_number UInt64,
        user String,
        a UInt64,
        b UInt64
    );

    CREATE REDUCER balances
    SOURCE transfers
    GROUP BY user
    STATE (
        sum_a UInt64 DEFAULT 0,
        sum_b UInt64 DEFAULT 0
    )
        WHEN row.a >= 0 THEN
            SET state.sum_a = state.sum_a + row.a
            SET state.sum_b = state.sum_b + row.b
            EMIT balance_a = state.sum_a
    END;

    CREATE MATERIALIZED VIEW totals AS
    SELECT user, sum(a) AS total_a
    FROM transfers
    GROUP BY user;
"#;

fn open(schema: &str, storage: &Arc<MemoryBackend>) -> Result<Settle> {
    Settle::open(Config::new(schema).storage(storage.clone()))
}

fn open_err(schema: &str, storage: &Arc<MemoryBackend>) -> String {
    match open(schema, storage) {
        Ok(_) => panic!("open succeeded"),
        Err(Error::IncompatibleStorage(msg)) => msg,
        Err(other) => panic!("unexpected error: {other}"),
    }
}

fn transfer(a: u64, b: u64) -> RowMap {
    HashMap::from([
        ("user".to_string(), Value::String("alice".into())),
        ("a".to_string(), Value::UInt64(a)),
        ("b".to_string(), Value::UInt64(b)),
    ])
}

fn ingest(db: &mut Settle, block: BlockNumber, a: u64, b: u64, finalized: BlockNumber) -> ChangeBatch {
    let rows = vec![("transfers".to_string(), block, vec![transfer(a, b)])];
    ingest_with_finalized(db, rows, finalized).unwrap().unwrap()
}

fn total_a(batch: &ChangeBatch) -> Value {
    batch.records_for("totals")[0].values["total_a"].clone()
}

/// Block 1 stays unfinalized, so a reopen replays its raw row.
fn storage_with_data() -> Arc<MemoryBackend> {
    let storage = Arc::new(MemoryBackend::new());
    let mut db = open(SCHEMA, &storage).unwrap();
    ingest(&mut db, 1, 1, 100, 0);
    storage
}

#[test]
fn reopen_with_the_same_schema_reads_the_stored_data() {
    let storage = storage_with_data();

    let mut db = open(SCHEMA, &storage).unwrap();
    let batch = ingest(&mut db, 2, 10, 0, 0);
    assert_eq!(total_a(&batch), Value::UInt64(11));
}

/// Rows are stored by column position: with `a` and `b` swapped, the
/// replayed row of block 1 would read as a = 100, and the total as 110.
#[test]
fn swapped_table_columns_are_refused() {
    let storage = storage_with_data();

    let swapped = SCHEMA.replace("a UInt64,\n        b UInt64", "b UInt64,\n        a UInt64");
    assert_ne!(swapped, SCHEMA);

    let msg = open_err(&swapped, &storage);
    assert!(msg.contains("table 'transfers'"), "{msg}");
    assert!(msg.contains("columns (block_number UInt64, user String, a UInt64, b UInt64)"), "{msg}");
}

#[test]
fn changed_column_type_is_refused() {
    let storage = storage_with_data();

    let changed = SCHEMA.replace("b UInt64\n    );", "b Int64\n    );");
    assert_ne!(changed, SCHEMA);

    let msg = open_err(&changed, &storage);
    assert!(msg.contains("table 'transfers'"), "{msg}");
}

#[test]
fn swapped_reducer_state_is_refused() {
    let storage = storage_with_data();

    let swapped = SCHEMA.replace(
        "sum_a UInt64 DEFAULT 0,\n        sum_b UInt64 DEFAULT 0",
        "sum_b UInt64 DEFAULT 0,\n        sum_a UInt64 DEFAULT 0",
    );
    assert_ne!(swapped, SCHEMA);

    let msg = open_err(&swapped, &storage);
    assert!(msg.contains("reducer 'balances'"), "{msg}");
}

#[test]
fn changed_mv_aggregates_are_refused() {
    let changes = [
        ("sum(a) AS total_a", "sum(b) AS total_a"),
        ("sum(a) AS total_a", "count() AS total_a"),
        ("sum(a) AS total_a", "sum(a) AS total_a, max(a) AS high"),
        ("GROUP BY user;", "GROUP BY user\n    WINDOW SLIDING INTERVAL 1 HOUR BY b;"),
    ];

    for (from, to) in changes {
        let storage = storage_with_data();
        let changed = SCHEMA.replace(from, to);
        assert_ne!(changed, SCHEMA);

        let msg = open_err(&changed, &storage);
        assert!(msg.contains("materialized view 'totals'"), "{to}: {msg}");
    }
}

/// Reducer logic, defaults and output aliases change only new results, and a
/// new object has no stored data.
#[test]
fn logic_aliases_and_new_objects_may_change() {
    let storage = storage_with_data();

    let changed = SCHEMA
        .replace("WHEN row.a >= 0", "WHEN row.a > 5")
        .replace("sum_b UInt64 DEFAULT 0", "sum_b UInt64 DEFAULT 7")
        .replace("sum(a) AS total_a", "sum(a) AS total_a_renamed")
        + r#"
    CREATE MATERIALIZED VIEW highs AS
    SELECT user, max(b) AS high
    FROM transfers
    GROUP BY user;
"#;

    let mut db = open(&changed, &storage).unwrap();
    let batch = ingest(&mut db, 2, 10, 0, 0);
    let totals = &batch.records_for("totals")[0];
    assert_eq!(totals.values["total_a_renamed"], Value::UInt64(11));
}

#[test]
fn database_without_data_may_change_its_layout() {
    let storage = Arc::new(MemoryBackend::new());
    drop(open(SCHEMA, &storage).unwrap());

    let swapped = SCHEMA.replace("a UInt64,\n        b UInt64", "b UInt64,\n        a UInt64");
    let mut db = open(&swapped, &storage).unwrap();
    ingest(&mut db, 1, 1, 100, 0);
    drop(db);

    // The new layout is the stored one now.
    open(&swapped, &storage).unwrap();
    open_err(SCHEMA, &storage);
}

/// The stamp goes with the first commit: a batch that is never acked leaves
/// the database empty, free to take any layout.
#[test]
fn stamp_is_written_with_the_first_commit() {
    let storage = Arc::new(MemoryBackend::new());
    {
        let mut db = open(SCHEMA, &storage).unwrap();
        let rows = vec![("transfers".to_string(), 1, vec![transfer(1, 100)])];
        crate::test_helpers::ingest_with_finalized_no_ack(&mut db, rows, 0).unwrap().unwrap();
    }
    assert_eq!(storage.get_meta("format_version").unwrap(), None);

    let mut db = open(SCHEMA, &storage).unwrap();
    ingest(&mut db, 1, 1, 100, 0);
    assert!(storage.get_meta("format_version").unwrap().is_some());
    assert!(storage.get_meta("layout:mv:totals").unwrap().is_some());
}

/// A call that returns no batch commits at once, with the stamp.
#[test]
fn stamp_is_written_with_a_heartbeat() {
    let storage = Arc::new(MemoryBackend::new());
    let mut db = open(SCHEMA, &storage).unwrap();
    assert!(ingest_with_finalized(&mut db, vec![], 0).unwrap().is_none());
    drop(db);

    assert!(storage.get_meta("format_version").unwrap().is_some());
    let swapped = SCHEMA.replace("a UInt64,\n        b UInt64", "b UInt64,\n        a UInt64");
    open_err(&swapped, &storage);
}

/// A database that a version before format versions wrote.
#[test]
fn database_without_a_format_version_is_refused() {
    let storage = Arc::new(MemoryBackend::new());
    let mut old = StorageWriteBatch::new();
    old.put_meta(META_LATEST_BLOCK, &5u64.to_be_bytes());
    storage.commit(&old).unwrap();

    let msg = open_err(SCHEMA, &storage);
    assert!(msg.contains("no format version"), "{msg}");
}

#[test]
fn other_format_versions_are_refused() {
    for (version, writer) in [(FORMAT_VERSION - 1, "an older"), (FORMAT_VERSION + 1, "a newer")] {
        let storage = Arc::new(MemoryBackend::new());
        let mut stamp = StorageWriteBatch::new();
        stamp.put_meta("format_version", &version.to_be_bytes());
        storage.commit(&stamp).unwrap();

        let msg = open_err(SCHEMA, &storage);
        assert!(msg.contains(&format!("format version {version}, {writer}")), "{msg}");
    }
}

fn reducer_def(state: &str) -> ReducerDef {
    let sql = format!(
        r#"
        CREATE TABLE transfers (block_number UInt64, user String, a UInt64, b UInt64);
        CREATE REDUCER late
        SOURCE transfers
        GROUP BY user
        STATE ({state})
            WHEN row.a >= 0 THEN
                SET state.x = state.x + row.a
                EMIT x = state.x
        END;
        "#
    );
    parse_schema(&sql).unwrap().reducers.remove(0)
}

/// A reducer added after open is not in the schema, so `register_reducer`
/// checks its layout.
#[test]
fn registered_reducer_layout_is_checked() {
    let storage = Arc::new(MemoryBackend::new());
    let state = "x UInt64 DEFAULT 0, y UInt64 DEFAULT 0";
    {
        let mut db = open(SCHEMA, &storage).unwrap();
        db.register_reducer(reducer_def(state)).unwrap();
        ingest(&mut db, 1, 1, 100, 1);
    }

    let mut db = open(SCHEMA, &storage).unwrap();
    let swapped = "y UInt64 DEFAULT 0, x UInt64 DEFAULT 0";
    match db.register_reducer(reducer_def(swapped)) {
        Err(Error::IncompatibleStorage(msg)) => assert!(msg.contains("reducer 'late'"), "{msg}"),
        other => panic!("unexpected result: {:?}", other.err()),
    }
    db.register_reducer(reducer_def(state)).unwrap();
}
