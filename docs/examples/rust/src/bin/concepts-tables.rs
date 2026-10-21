use settle::db::{IngestInput, Settle};
use settle::error::Result;
use settle::types::{BlockCursor, ChangeRecord, RowMap, Value};
use settle::{BuildOptions, Pipeline, TableHandle, ViewOptions, float64, string, uint64};

// [!region table]
fn declare_swaps(p: &mut Pipeline) -> TableHandle {
    p.table(
        "swaps",
        [
            ("block_number", uint64()),
            ("pool", string()),
            ("amount", float64()),
        ],
    )
}
// [!endregion table]

fn open() -> Result<Settle> {
    let mut p = Pipeline::new();
    let swaps = declare_swaps(&mut p);

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

fn show(record: &ChangeRecord) {
    let key: Vec<String> = record.key_values().map(|(n, v)| format!("{n}: {v}")).collect();
    let values: Vec<String> = record.values().map(|(n, v)| format!("{n}: {v}")).collect();

    let operation = format!("{:?}", record.operation).to_lowercase();

    println!("{operation} {{ {} }} {{ {} }}", key.join(", "), values.join(", "));
}

// [!region ingest]
fn send_rows(db: &mut Settle) -> Result<()> {
    let batch = db.ingest(IngestInput {
        data: [(
            "swaps".to_string(),
            vec![swap(7, "ETH/USDC", 100.0), swap(7, "BTC/USDC", 50.0)],
        )]
        .into(),
        rollback_chain: vec![cursor(7, "0x7a")],
        finalized_head: cursor(0, "0x00"),
    })?;

    let Some(batch) = batch else { return Ok(()) };

    for record in &batch.tables["swaps"] {
        show(record);
    }

    db.ack(batch.sequence)
}
// [!endregion ingest]

fn main() -> Result<()> {
    let mut db = open()?;
    send_rows(&mut db)?;

    // Not shown on a page: a fork removes the rows of block 7.
    let fork = db.handle_fork(vec![cursor(7, "0x7b"), cursor(0, "0x00")])?;
    for record in fork.batch.iter().flat_map(|b| &b.tables["swaps"]) {
        show(record);
        println!("  prev_values: {}", record.prev_values().is_some());
    }

    Ok(())
}
