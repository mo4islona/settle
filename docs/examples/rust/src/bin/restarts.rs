use settle::db::IngestInput;
use settle::types::{BlockCursor, RowMap, Value};
use settle::{BuildOptions, Pipeline, ViewOptions, float64, string, uint64};

fn main() -> settle::error::Result<()> {
    // [!region open]
    let mut p = Pipeline::new();

    p.virtual_table(
        "swaps",
        [("block_number", uint64()), ("pool", string()), ("amount", float64())],
    )
    .create_view(
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

    let mut db = p.build(BuildOptions::new().data_dir("./settle-data"))?;

    println!("continue after: {:?}", db.latest_cursor());
    // [!endregion open]

    // [!region next-block]
    let next = db.latest_cursor().map_or(0, |c| c.number) + 1;

    let row = RowMap::from([
        ("block_number".into(), Value::UInt64(next)),
        ("pool".into(), Value::String("ETH/USDC".into())),
        ("amount".into(), Value::Float64(100.0)),
    ]);

    let batch = db.ingest(IngestInput {
        data: [("swaps".to_string(), vec![row])].into(),
        rollback_chain: vec![BlockCursor { number: next, hash: format!("0x{next}") }],
        finalized_head: BlockCursor { number: next - 1, hash: format!("0x{}", next - 1) },
    })?;
    // [!endregion next-block]

    // [!region apply]
    if let Some(batch) = batch {
        for record in batch.tables.get("pool_volume").into_iter().flatten() {
            let fields: Vec<String> = record.values().map(|(n, v)| format!("{n}: {v}")).collect();
            let operation = format!("{:?}", record.operation).to_lowercase();
            println!("batch {}: {operation} {{ {} }}", batch.sequence, fields.join(", "));
        }

        // A crash before the ack loses the batch in Settle, but not in your database.
        if !std::env::args().any(|arg| arg == "--crash") {
            db.ack(batch.sequence)?;
        }
    }
    // [!endregion apply]

    Ok(())
}
