// [!region pipeline]
use settle::db::{IngestInput, Settle};
use settle::error::Result;
use settle::types::{BlockCursor, ChangeBatch, RowMap, Value};
use settle::{BuildOptions, Pipeline, ViewOptions, float64, string, uint64};

fn open() -> Result<Settle> {
    let mut p = Pipeline::new();

    let swaps = p.virtual_table(
        "swaps",
        [
            ("block_number", uint64()),
            ("pool", string()),
            ("amount", float64()),
        ],
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
// [!endregion pipeline]

// [!region apply]
fn apply(db: &mut Settle, batch: Option<ChangeBatch>) -> Result<()> {
    let Some(batch) = batch else { return Ok(()) };

    for record in batch.tables.get("pool_volume").into_iter().flatten() {
        let values: Vec<String> = record
            .values()
            .map(|(name, value)| format!("{name}: {value}"))
            .collect();

        let operation = format!("{:?}", record.operation).to_lowercase();

        println!("{operation} {{ {} }}", values.join(", "));
    }

    db.ack(batch.sequence)
}
// [!endregion apply]

// [!region ingest]
fn swap(block: u64, pool: &str, amount: f64) -> RowMap {
    RowMap::from([
        ("block_number".into(), Value::UInt64(block)),
        ("pool".into(), Value::String(pool.into())),
        ("amount".into(), Value::Float64(amount)),
    ])
}

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

fn send_two_blocks(db: &mut Settle) -> Result<()> {
    let batch = db.ingest(IngestInput {
        data: [(
            "swaps".to_string(),
            vec![
                swap(1, "ETH/USDC", 100.0),
                swap(2, "ETH/USDC", 250.0),
                swap(2, "BTC/USDC", 50.0),
            ],
        )]
        .into(),
        rollback_chain: vec![cursor(1, "0x1a"), cursor(2, "0x2a")],
        finalized_head: cursor(0, "0x00"),
    })?;

    apply(db, batch)
}
// [!endregion ingest]

// [!region fork]
fn replace_block(db: &mut Settle) -> Result<()> {
    let fork = db.handle_fork(vec![cursor(2, "0x2b"), cursor(1, "0x1a")])?;

    println!("fork point: {}", fork.cursor.number);
    apply(db, fork.batch)
}
// [!endregion fork]

// [!region new-block]
fn send_new_block(db: &mut Settle) -> Result<()> {
    let batch = db.ingest(IngestInput {
        data: [("swaps".to_string(), vec![swap(2, "ETH/USDC", 300.0)])].into(),
        rollback_chain: vec![cursor(2, "0x2b")],
        finalized_head: cursor(1, "0x1a"),
    })?;

    apply(db, batch)
}
// [!endregion new-block]

// [!region main]
fn main() -> Result<()> {
    let mut db = open()?;

    send_two_blocks(&mut db)?;
    replace_block(&mut db)?;
    send_new_block(&mut db)
}
// [!endregion main]
