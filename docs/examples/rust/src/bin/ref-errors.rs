// Checks the Rust Error variants of each failure.
use std::collections::HashMap;

use settle::db::{Config, IngestInput, Settle};
use settle::error::Error;
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::types::{BlockCursor, RowMap, Value};

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

fn variant(e: &Error) -> &'static str {
    match e {
        Error::Schema(_) => "Schema",
        Error::Storage(_) => "Storage",
        Error::IncompatibleStorage(_) => "IncompatibleStorage",
        Error::Reducer(_) => "Reducer",
        Error::Rollback(_) => "Rollback",
        Error::Serialization(_) => "Serialization",
        Error::Deserialization(_) => "Deserialization",
        Error::InvalidOperation(_) => "InvalidOperation",
        Error::PendingAck { .. } => "PendingAck",
        Error::WrongAckSequence { .. } => "WrongAckSequence",
        Error::Poisoned(_) => "Poisoned",
        Error::SyncFailed(_) => "SyncFailed",
    }
}

fn report<T>(label: &str, result: settle::error::Result<T>) {
    match result {
        Ok(_) => println!("{label}: ok"),
        Err(e) => println!("{label}: {} — {}", variant(&e), e.to_string().lines().next().unwrap_or("")),
    }
}

fn rows(table: &str, blocks: &[u64]) -> HashMap<String, Vec<RowMap>> {
    let rows = blocks
        .iter()
        .map(|b| RowMap::from([
            ("block_number".into(), Value::UInt64(*b)),
            ("k".into(), Value::String("a".into())),
            ("n".into(), Value::Float64(1.0)),
        ]))
        .collect();
    HashMap::from([(table.to_string(), rows)])
}

fn main() -> settle::error::Result<()> {
    let schema = "
        CREATE TABLE t (block_number UInt64, k String, n Float64);
        CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS total FROM t GROUP BY k;
    ";
    let mut db = Settle::open(Config::new(schema))?;
    let first = db
        .ingest(IngestInput {
            data: rows("t", &[5]),
            rollback_chain: vec![cursor(4, "0x4a"), cursor(5, "0x5a")],
            finalized_head: cursor(2, "0x2a"),
        })?
        .expect("a batch");
    db.ack(first.sequence)?;

    report("block not above latest", db.ingest(IngestInput {
        data: rows("t", &[5]),
        rollback_chain: vec![cursor(5, "0x5a")],
        finalized_head: cursor(2, "0x2a"),
    }));
    report("finality goes back", db.ingest(IngestInput {
        data: rows("t", &[6]),
        rollback_chain: vec![cursor(6, "0x6a")],
        finalized_head: cursor(1, "0x1a"),
    }));
    report("unknown table with rows", db.ingest(IngestInput {
        data: rows("nope", &[6]),
        rollback_chain: vec![cursor(6, "0x6a")],
        finalized_head: cursor(2, "0x2a"),
    }));
    report("no common ancestor", db.handle_fork(vec![cursor(6, "0x6x"), cursor(5, "0x5x")]));
    report("empty fork chain", db.handle_fork(vec![]));
    report("fork below finality", db.ingest(IngestInput {
        data: rows("t", &[7]),
        rollback_chain: vec![cursor(2, "0x2x"), cursor(7, "0x7a")],
        finalized_head: cursor(2, "0x2a"),
    }));
    report("wrong ack", db.ack(99));
    report("schema", Settle::open(Config::new("CREATE TABLE t (block_number UInt64, c Nope);")));
    report(
        "config",
        Settle::open(Config::with_data_dir(schema, "./settle-bad").compression("gzip")),
    );
    let _ = std::fs::remove_dir_all("./settle-bad");

    let mut r = Settle::open(Config::new(
        "CREATE TABLE t (block_number UInt64, k String, n Float64);
         CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) LANGUAGE EXTERNAL;",
    ))?;
    report("external reducer without code", r.ingest(IngestInput {
        data: rows("t", &[1]),
        rollback_chain: vec![cursor(1, "0x1a")],
        finalized_head: cursor(0, "0x00"),
    }));
    r.register_reducer_callback(
        "r",
        Box::new(FnReducerRuntime::new(|_state, _row| panic!("negative amount"))),
    )?;
    report("closure panics", r.ingest(IngestInput {
        data: rows("t", &[1]),
        rollback_chain: vec![cursor(1, "0x1a")],
        finalized_head: cursor(0, "0x00"),
    }));
    println!("is_poisoned: {}", r.is_poisoned());
    Ok(())
}
