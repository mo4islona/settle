use settle::db::{IngestInput, Settle};
use settle::error::Result;
use settle::types::{BlockCursor, ChangeBatch, RowMap, Value};
use settle::{BuildOptions, Pipeline, ViewOptions, float64, string, uint64};

fn open() -> Result<Settle> {
    let mut p = Pipeline::new();

    let swaps = p.virtual_table(
        "swaps",
        [("block_number", uint64()), ("pool", string()), ("amount", float64())],
    );

    swaps.create_view(
        "pool_volume",
        ViewOptions {
            group_by: vec!["pool".into()],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("pool".into(), agg.key("pool").into()),
                    ("volume".into(), agg.sum("amount").into()),
                ]
            }),
        },
    );

    p.build(BuildOptions::new())
}

fn swap(block: u64, amount: f64) -> RowMap {
    RowMap::from([
        ("block_number".into(), Value::UInt64(block)),
        ("pool".into(), Value::String("ETH/USDC".into())),
        ("amount".into(), Value::Float64(amount)),
    ])
}

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

fn apply(db: &mut Settle, batch: Option<ChangeBatch>) -> Result<()> {
    let Some(batch) = batch else { return Ok(()) };

    for record in batch.tables.get("pool_volume").into_iter().flatten() {
        let values: Vec<String> = record.values().map(|(n, v)| format!("{n}: {v}")).collect();
        let operation = format!("{:?}", record.operation).to_lowercase();

        println!("{operation} {{ {} }}", values.join(", "));
    }

    db.ack(batch.sequence)
}

// [!region inline]
// Block 2 and block 3 have new hashes. Block 1 does not change.
fn send_new_chain(db: &mut Settle) -> Result<()> {
    let batch = db.ingest(IngestInput {
        data: [("swaps".to_string(), vec![swap(2, 10.0), swap(3, 20.0)])].into(),
        rollback_chain: vec![cursor(1, "0x1a"), cursor(2, "0x2b"), cursor(3, "0x3b")],
        finalized_head: cursor(1, "0x1a"),
    })?;

    apply(db, batch)
}
// [!endregion inline]

// [!region no-ancestor]
fn fork_without_ancestor(db: &mut Settle) {
    if let Err(error) = db.handle_fork(vec![cursor(3, "0x3c")]) {
        println!("{error}");
    }
}
// [!endregion no-ancestor]

fn main() -> Result<()> {
    let mut db = open()?;

    let batch = db.ingest(IngestInput {
        data: [("swaps".to_string(), vec![swap(1, 100.0), swap(2, 250.0), swap(3, 50.0)])].into(),
        rollback_chain: vec![cursor(1, "0x1a"), cursor(2, "0x2a"), cursor(3, "0x3a")],
        finalized_head: cursor(1, "0x1a"),
    })?;
    apply(&mut db, batch)?;

    println!("--- in-line fork");
    send_new_chain(&mut db)?;

    println!("--- no common ancestor");
    fork_without_ancestor(&mut db);
    Ok(())
}
