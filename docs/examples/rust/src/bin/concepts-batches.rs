use settle::db::{IngestInput, Settle};
use settle::error::Result;
use settle::types::{BlockCursor, RowMap, Value};
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
                    ("swaps".into(), agg.count().into()),
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

// [!region update]
fn send_two_swaps(db: &mut Settle) -> Result<()> {
    let batch = db.ingest(IngestInput {
        data: [("swaps".to_string(), vec![swap(2, 250.0), swap(3, 50.0)])].into(),
        rollback_chain: vec![cursor(2, "0x2a"), cursor(3, "0x3a")],
        finalized_head: cursor(1, "0x1a"),
    })?;

    let Some(batch) = batch else { return Ok(()) };

    println!("sequence: {}", batch.sequence);
    for record in &batch.tables["pool_volume"] {
        println!("table: {}", record.table());
        println!("operation: {:?}", record.operation);
        println!("key: {:?}", record.key_values().collect::<Vec<_>>());
        println!("values: {:?}", record.values().collect::<Vec<_>>());
        println!("prev_values: {:?}", record.prev_values().map(|v| v.collect::<Vec<_>>()));
    }

    db.ack(batch.sequence)
}
// [!endregion update]

// [!region heartbeat]
fn move_finality(db: &mut Settle) -> Result<()> {
    let batch = db.ingest(IngestInput {
        data: Default::default(),
        rollback_chain: vec![],
        finalized_head: cursor(3, "0x3a"),
    })?;

    println!("{batch:?}"); // None: no output table changed
    Ok(())
}
// [!endregion heartbeat]

fn main() -> Result<()> {
    let mut db = open()?;

    let first = db.ingest(IngestInput {
        data: [("swaps".to_string(), vec![swap(1, 100.0)])].into(),
        rollback_chain: vec![cursor(1, "0x1a")],
        finalized_head: cursor(0, "0x00"),
    })?;
    if let Some(batch) = first {
        db.ack(batch.sequence)?;
    }

    send_two_swaps(&mut db)?;
    move_finality(&mut db)
}
