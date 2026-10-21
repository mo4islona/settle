// Checks the column types in Rust: one Value of each type in, the Value
// that comes out.
use std::collections::HashMap;

use settle::db::{Config, IngestInput, Settle};
use settle::types::{BlockCursor, RowMap, Value};

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

fn main() -> settle::error::Result<()> {
    // [!region builder]
    let schema = "CREATE TABLE values (
        block_number UInt64, u64 UInt64, i64 Int64, f64 Float64, u256 UInt256,
        text String, time DateTime, flag Boolean, raw BYTEA, b58 Base58
    );";
    let mut db = Settle::open(Config::new(schema))?;

    let mut u256 = [0u8; 32];
    u256[6] = 1; // 2^200

    let row = RowMap::from([
        ("block_number".into(), Value::UInt64(1)),
        ("u64".into(), Value::UInt64(1 << 60)),
        ("i64".into(), Value::Int64(-5)),
        ("f64".into(), Value::Float64(1.5)),
        ("u256".into(), Value::Uint256(u256)),
        ("text".into(), Value::String("hello".into())),
        ("time".into(), Value::DateTime(1_767_225_600_000)),
        ("flag".into(), Value::Boolean(true)),
        ("raw".into(), Value::Bytes(vec![1, 2, 255])),
        ("b58".into(), Value::Base58(vec![1, 2, 3])),
    ]);

    let batch = db
        .ingest(IngestInput {
            data: HashMap::from([("values".to_string(), vec![row])]),
            rollback_chain: vec![cursor(1, "0x1a")],
            finalized_head: cursor(0, "0x00"),
        })?
        .expect("a batch");

    let record = &batch.tables["values"][0];
    for (name, value) in record.values() {
        println!("{name:<12} {value:?}");
    }
    // [!endregion builder]

    println!("--- a value of another type");
    let mut db = Settle::open(Config::new("CREATE TABLE t (block_number UInt64, n UInt64);"))?;
    let result = db.ingest(IngestInput {
        data: HashMap::from([(
            "t".to_string(),
            vec![RowMap::from([
                ("block_number".into(), Value::UInt64(1)),
                ("n".into(), Value::String("12".into())),
            ])],
        )]),
        rollback_chain: vec![cursor(1, "0x1a")],
        finalized_head: cursor(0, "0x00"),
    });
    match result {
        Ok(Some(batch)) => println!("accepted: {:?}", batch.tables["t"][0].get("n")),
        Ok(None) => println!("no batch"),
        Err(e) => println!("refused: {e}"),
    }
    Ok(())
}
