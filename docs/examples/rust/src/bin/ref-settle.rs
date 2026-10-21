// Checks the Settle calls in Rust: open with a schema text, a closure
// reducer, the batch fields, the status reads and the fork helpers.
use std::collections::{BTreeMap, HashMap};

use settle::db::{Config, IngestInput, IngestRows, Settle};
use settle::error::Error;
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::schema::ast::{ReducerBody, ReducerDef, StateField};
use settle::types::{BlockCursor, ChangeRecord, ColumnType, Row, RowMap, Value};

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

fn show(label: &str, record: &ChangeRecord) {
    let fields = |it: &mut dyn Iterator<Item = (&str, &Value)>| {
        it.map(|(k, v)| format!("{k}: {v}")).collect::<Vec<_>>().join(", ")
    };
    let prev = record.prev_values().map(|mut p| fields(&mut p));
    println!(
        "{label}: {} {:?} key {{ {} }} values {{ {} }} prev {}",
        record.table(),
        record.operation,
        fields(&mut record.key_values()),
        fields(&mut record.values()),
        prev.map_or("None".to_string(), |p| format!("{{ {p} }}")),
    );
}

fn main() -> settle::error::Result<()> {
    // [!region open]
    let schema = "
        CREATE TABLE swaps (block_number UInt64, pool String, amount Float64);

        CREATE REDUCER totals
        SOURCE swaps
        GROUP BY pool
        STATE (volume Float64 DEFAULT 0)
        LANGUAGE EXTERNAL;

        CREATE MATERIALIZED VIEW pool_volume AS
        SELECT pool, last(volume) AS volume
        FROM totals
        GROUP BY pool;
    ";

    let mut db = Settle::open(Config::new(schema))?;

    let totals = FnReducerRuntime::new(|state, row| {
        let amount = row.get("amount").and_then(Value::as_f64).unwrap_or(0.0);
        let volume = state.get_f64("volume") + amount;

        state.set("volume", Value::Float64(volume));
        state.emit(RowMap::from([("volume".into(), Value::Float64(volume))]));
    });
    db.register_reducer_callback("totals", Box::new(totals))?;
    // [!endregion open]

    println!("cursor before: {:?}", db.latest_cursor());

    let row = |block: u64, amount: f64| -> RowMap {
        RowMap::from([
            ("block_number".into(), Value::UInt64(block)),
            ("pool".into(), Value::String("ETH/USDC".into())),
            ("amount".into(), Value::Float64(amount)),
        ])
    };

    let batch = db
        .ingest(IngestInput {
            data: HashMap::from([("swaps".to_string(), vec![row(10, 100.0), row(11, 50.0)])]),
            rollback_chain: vec![cursor(10, "0x10a"), cursor(11, "0x11a")],
            finalized_head: cursor(9, "0x09a"),
        })?
        .expect("a batch");

    // [!region batch]
    println!("sequence: {}", batch.sequence);
    println!("latest_head: {:?}", batch.latest_head);
    println!("finalized_head: {:?}", batch.finalized_head);
    let mut tables: Vec<&String> = batch.tables.keys().collect();
    tables.sort();
    println!("tables: {tables:?}");
    show("raw record", &batch.tables["swaps"][0]);
    show("view record", &batch.tables["pool_volume"][0]);
    // [!endregion batch]

    println!("unacked_batches: {}, is_awaiting_ack: {}", db.unacked_batches(), db.is_awaiting_ack());
    db.ack(batch.sequence)?;
    println!("after ack: {}, {}", db.unacked_batches(), db.is_awaiting_ack());

    // [!region status]
    println!("latest_cursor: {:?}", db.latest_cursor());
    println!("finalized_cursor: {:?}", db.finalized_cursor());
    println!("durable_block: {}", db.durable_block());
    // [!endregion status]

    // [!region rows]
    let registry = db.table_registry("swaps").expect("a table").clone();
    println!("columns: {:?}", registry.names());

    let block_12 = vec![Row::from_values(
        registry.clone(),
        vec![Value::UInt64(12), Value::String("ETH/USDC".into()), Value::Float64(25.0)],
    )];

    let batch = db
        .ingest_rows(IngestRows {
            data: HashMap::from([("swaps".to_string(), BTreeMap::from([(12, block_12)]))]),
            rollback_chain: vec![cursor(12, "0x12a")],
            finalized_head: cursor(10, "0x10a"),
        })?
        .expect("a batch");
    show("update", &batch.tables["pool_volume"][0]);
    // [!endregion rows]
    db.ack(batch.sequence)?;

    // [!region resolve]
    let point = db.resolve_fork_cursor(&[(12, "0x12b"), (11, "0x11a")]);
    println!("{point:?}");
    // [!endregion resolve]

    let fork = db.handle_fork(vec![cursor(12, "0x12b"), cursor(11, "0x11a")])?;
    println!("fork point: {:?}", fork.cursor);
    let fork_batch = fork.batch.expect("a batch");
    for (table, records) in &fork_batch.tables {
        for record in records {
            show(table, record);
        }
    }
    db.ack(fork_batch.sequence)?;

    println!("--- register_reducer");
    let mut other = Settle::open(Config::new(
        "CREATE TABLE swaps (block_number UInt64, pool String, amount Float64);",
    ))?;
    // [!region register]
    other.register_reducer(ReducerDef {
        name: "counts".into(),
        source: "swaps".into(),
        group_by: vec!["pool".into()],
        state: vec![StateField {
            name: "count".into(),
            column_type: ColumnType::UInt64,
            default: "0".into(),
            map: false,
        }],
        body: ReducerBody::External { id: "counts".into() },
        requires: vec![],
    })?;

    other.register_reducer_callback(
        "counts",
        Box::new(FnReducerRuntime::new(|state, _row| {
            let count = state.get_u64("count") + 1;
            state.set("count", Value::UInt64(count));
        })),
    )?;
    // [!endregion register]
    println!("registered: {}", other.has_reducer("counts"));

    match db.register_reducer_callback("totals", Box::new(FnReducerRuntime::new(|_, _| {}))) {
        Err(e) => println!("callback twice: {e}"),
        Ok(()) => println!("callback twice: ok"),
    }

    println!("--- pending acks");
    let mut q = Settle::open(
        Config::new("CREATE TABLE t (block_number UInt64, k String);").max_pending_batches(1),
    )?;
    let t_row = |block: u64| RowMap::from([
        ("block_number".into(), Value::UInt64(block)),
        ("k".into(), Value::String("a".into())),
    ]);
    let first = q
        .ingest(IngestInput {
            data: HashMap::from([("t".to_string(), vec![t_row(1)])]),
            rollback_chain: vec![cursor(1, "0x1a")],
            finalized_head: cursor(0, "0x00"),
        })?
        .expect("a batch");
    match q.ingest(IngestInput {
        data: HashMap::from([("t".to_string(), vec![t_row(2)])]),
        rollback_chain: vec![cursor(2, "0x2a")],
        finalized_head: cursor(0, "0x00"),
    }) {
        Err(Error::PendingAck { sequence, .. }) => println!("PendingAck {{ sequence: {sequence} }}"),
        other => println!("unexpected: {:?}", other.map(|b| b.map(|b| b.sequence))),
    }
    match q.ack(5) {
        Err(Error::WrongAckSequence { expected, got }) => {
            println!("WrongAckSequence {{ expected: {expected}, got: {got} }}")
        }
        other => println!("unexpected: {other:?}"),
    }
    q.ack(first.sequence)?;
    q.ack(first.sequence)?;
    println!("double ack: ok");
    Ok(())
}
