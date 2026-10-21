use settle::db::{IngestInput, Settle};
use settle::types::{BlockCursor, ChangeBatch, RowMap, Value};
use settle::{BuildOptions, Pipeline, ViewOptions, datetime, float64, interval, string, uint64};

const T0: i64 = 1_767_268_800_000; // 2026-01-01 12:00 UTC

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

fn trade(block: u64, timestamp: i64, price: f64) -> RowMap {
    RowMap::from([
        ("block_number".into(), Value::UInt64(block)),
        ("timestamp".into(), Value::DateTime(timestamp)),
        ("pool".into(), Value::String("ETH/USDC".into())),
        ("price".into(), Value::Float64(price)),
    ])
}

fn apply(db: &mut Settle, batch: Option<ChangeBatch>) -> settle::error::Result<()> {
    let Some(batch) = batch else { return Ok(()) };

    for record in batch.tables.get("candles_1m").into_iter().flatten() {
        let operation = format!("{:?}", record.operation).to_lowercase();
        let field = |name: &str| record.get(name).cloned().unwrap_or(Value::Null);

        println!(
            "{operation} {{ open: {}, high: {}, low: {}, close: {} }}",
            field("open"),
            field("high"),
            field("low"),
            field("close"),
        );
    }

    db.ack(batch.sequence)
}

fn main() -> settle::error::Result<()> {
    let mut p = Pipeline::new();

    p.virtual_table(
        "trades",
        [
            ("block_number", uint64()),
            ("timestamp", datetime()),
            ("pool", string()),
            ("price", float64()),
        ],
    )
    .create_view(
        "candles_1m",
        ViewOptions {
            group_by: vec!["pool".into(), interval("timestamp", "1 minute").r#as("minute").into()],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("pool".into(), agg.key("pool").into()),
                    ("minute".into(), agg.key("minute").into()),
                    ("open".into(), agg.first("price").into()),
                    ("high".into(), agg.max("price").into()),
                    ("low".into(), agg.min("price").into()),
                    ("close".into(), agg.last("price").into()),
                ]
            }),
        },
    );

    let mut db = p.build(BuildOptions::new())?;

    // [!region blocks]
    let batch = db.ingest(IngestInput {
        data: [(
            "trades".to_string(),
            vec![
                trade(1, T0 + 10_000, 2000.0),
                trade(2, T0 + 20_000, 2050.0),
                trade(3, T0 + 30_000, 2010.0),
            ],
        )]
        .into(),
        rollback_chain: vec![cursor(1, "0x1a"), cursor(2, "0x2a"), cursor(3, "0x3a")],
        finalized_head: cursor(0, "0x00"),
    })?;
    apply(&mut db, batch)?;
    // [!endregion blocks]

    println!("---");

    // [!region fork]
    // The chain replaces blocks 2 and 3. Block 2 had the highest price.
    let fork = db.handle_fork(vec![cursor(3, "0x3b"), cursor(2, "0x2b"), cursor(1, "0x1a")])?;
    apply(&mut db, fork.batch)?;

    let batch = db.ingest(IngestInput {
        data: [("trades".to_string(), vec![trade(2, T0 + 20_000, 1995.0)])].into(),
        rollback_chain: vec![cursor(2, "0x2b")],
        finalized_head: cursor(1, "0x1a"),
    })?;
    apply(&mut db, batch)?;
    // [!endregion fork]

    Ok(())
}
