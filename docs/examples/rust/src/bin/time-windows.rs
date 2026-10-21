use settle::db::IngestInput;
use settle::types::{BlockCursor, ChangeRecord, RowMap, Value};
use settle::{
    BuildOptions, Pipeline, SlidingWindowOptions, ViewOptions, datetime, float64, interval, string,
    uint64,
};

const MINUTE: i64 = 60_000;
const T0: i64 = 1_767_268_800_000; // 2026-01-01 12:00 UTC

fn cursor(number: u64) -> BlockCursor {
    BlockCursor { number, hash: format!("0x{number}a") }
}

fn trade(block: u64, timestamp: i64, price: f64, amount: f64) -> RowMap {
    RowMap::from([
        ("block_number".into(), Value::UInt64(block)),
        ("timestamp".into(), Value::DateTime(timestamp)),
        ("pool".into(), Value::String("ETH/USDC".into())),
        ("price".into(), Value::Float64(price)),
        ("amount".into(), Value::Float64(amount)),
    ])
}

fn show(record: &ChangeRecord) {
    let operation = format!("{:?}", record.operation).to_lowercase();
    let fields: Vec<String> = record.values().map(|(name, value)| format!("{name}: {value}")).collect();

    println!("{operation} {{ {} }}", fields.join(", "));
}

fn main() -> settle::error::Result<()> {
    let mut p = Pipeline::new();

    // [!region table]
    let trades = p.virtual_table(
        "trades",
        [
            ("block_number", uint64()),
            ("timestamp", datetime()), // milliseconds since 1970-01-01
            ("pool", string()),
            ("price", float64()),
            ("amount", float64()),
        ],
    );
    // [!endregion table]

    // [!region candles]
    trades.create_view(
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
                    ("volume".into(), agg.sum("amount").into()),
                ]
            }),
        },
    );
    // [!endregion candles]

    // [!region sliding]
    trades.create_view(
        "volume_1h",
        ViewOptions {
            group_by: vec!["pool".into()],
            sliding_window: Some(SlidingWindowOptions {
                interval: "1 hour".into(),
                time_column: "timestamp".into(),
            }),
            select: Box::new(|agg| {
                vec![
                    ("pool".into(), agg.key("pool").into()),
                    ("volume".into(), agg.sum("amount").into()),
                    ("trades".into(), agg.count().into()),
                ]
            }),
        },
    );
    // [!endregion sliding]

    let mut db = p.build(BuildOptions::new())?;

    let batch = db.ingest(IngestInput {
        data: [(
            "trades".to_string(),
            vec![
                trade(1, T0 + 5_000, 2000.0, 1.0),
                trade(2, T0 + 30_000, 2010.0, 2.0),
                trade(3, T0 + 50_000, 1990.0, 1.0),
                trade(4, T0 + MINUTE + 10_000, 2005.0, 3.0),
            ],
        )]
        .into(),
        rollback_chain: (1..=4).map(cursor).collect(),
        finalized_head: BlockCursor { number: 0, hash: "0x00".into() },
    })?;

    if let Some(batch) = batch {
        for table in ["candles_1m", "volume_1h"] {
            batch.tables.get(table).into_iter().flatten().for_each(show);
        }
        db.ack(batch.sequence)?;
    }

    // Two hours later: the trades of block 1 to 4 leave the 1-hour window.
    let later = db.ingest(IngestInput {
        data: [("trades".to_string(), vec![trade(5, T0 + 120 * MINUTE, 2100.0, 4.0)])].into(),
        rollback_chain: vec![cursor(5)],
        finalized_head: cursor(4),
    })?;

    println!("---");
    if let Some(later) = later {
        later.tables.get("volume_1h").into_iter().flatten().for_each(show);
        db.ack(later.sequence)?;
    }

    Ok(())
}
