use settle::db::{Config, IngestInput, Settle};
use settle::error::Error;
use settle::types::{BlockCursor, RowMap, Value};
use settle::{Pipeline, ViewOptions, float64, string, uint64};

fn block(n: u64) -> IngestInput {
    let row = RowMap::from([
        ("block_number".into(), Value::UInt64(n)),
        ("pool".into(), Value::String("ETH/USDC".into())),
        ("amount".into(), Value::Float64(100.0)),
    ]);

    IngestInput {
        data: [("swaps".to_string(), vec![row])].into(),
        rollback_chain: vec![BlockCursor { number: n, hash: format!("0x{n}") }],
        finalized_head: BlockCursor { number: 0, hash: "0x0".into() },
    }
}

fn main() -> settle::error::Result<()> {
    let mut p = Pipeline::new();

    p.table(
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
                ]
            }),
        },
    );

    // The pipeline has no reducer, so its schema text is sufficient to open it.
    let mut db = Settle::open(Config::new(p.to_ddl()?).max_pending_batches(1))?;

    let first = db.ingest(block(1))?;

    // [!region pending]
    match db.ingest(block(2)) {
        Err(Error::PendingAck { sequence, .. }) => {
            // Apply and ack the oldest batch, then send the same blocks again.
            println!("wait for the ack of batch {sequence}");
            db.ack(sequence)?;
            db.ingest(block(2))?;
        }
        other => {
            other?;
        }
    }
    // [!endregion pending]

    println!(
        "first batch: {:?} unacked batches: {}",
        first.map(|b| b.sequence),
        db.unacked_batches(),
    );

    Ok(())
}
