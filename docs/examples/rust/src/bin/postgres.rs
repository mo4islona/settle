use std::error::Error;

use postgres::{Client, NoTls};
use settle::db::{Config, IngestInput, Settle};
use settle::types::{BlockCursor, ChangeBatch, ChangeOp, RowMap, Value};
use settle::{Pipeline, ViewOptions, float64, string, uint64};

// [!region apply]
fn apply(pg: &mut Client, db: &mut Settle, batch: Option<ChangeBatch>) -> Result<(), Box<dyn Error>> {
    let Some(batch) = batch else { return Ok(()) };

    let mut tx = pg.transaction()?;

    for record in batch.tables.get("pool_volume").into_iter().flatten() {
        let pool = record.key("pool").and_then(Value::as_str).unwrap_or_default();

        if record.operation == ChangeOp::Delete {
            tx.execute("DELETE FROM pool_volume WHERE pool = $1", &[&pool])?;
            continue;
        }

        let volume = record.get("volume").and_then(Value::as_f64).unwrap_or_default();
        let swaps = record.get("swaps").and_then(Value::as_u64).unwrap_or_default() as i64;
        tx.execute(
            "INSERT INTO pool_volume (pool, volume, swaps) VALUES ($1, $2, $3)
             ON CONFLICT (pool) DO UPDATE SET volume = $2, swaps = $3",
            &[&pool, &volume, &swaps],
        )?;
    }

    let sequence = batch.sequence as i64;
    tx.execute("UPDATE settle_sequence SET sequence = $1 WHERE id = 1", &[&sequence])?;
    tx.commit()?;

    if std::env::args().any(|arg| arg == "--crash") {
        println!("batch {} is in Postgres, stop before the ack", batch.sequence);
        std::process::exit(1);
    }

    db.ack(batch.sequence)?;
    Ok(())
}
// [!endregion apply]

// A source of three blocks for each run. A real app reads them from the chain.
fn swaps_of(block: u64) -> Vec<RowMap> {
    let pool = if block % 2 == 1 { "BTC/USDC" } else { "ETH/USDC" };

    vec![RowMap::from([
        ("block_number".into(), Value::UInt64(block)),
        ("pool".into(), Value::String(pool.into())),
        ("amount".into(), Value::Float64(100.0)),
    ])]
}

fn main() -> Result<(), Box<dyn Error>> {
    // [!region tables]
    let mut pg = Client::connect(&std::env::var("DATABASE_URL")?, NoTls)?;

    pg.batch_execute(
        "CREATE TABLE IF NOT EXISTS pool_volume (
           pool   text PRIMARY KEY,
           volume double precision NOT NULL,
           swaps  bigint NOT NULL
         );

         CREATE TABLE IF NOT EXISTS settle_sequence (
           id       int PRIMARY KEY,
           sequence bigint NOT NULL
         );

         INSERT INTO settle_sequence VALUES (1, 0) ON CONFLICT DO NOTHING;",
    )?;
    // [!endregion tables]

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

    let applied: i64 = pg.query_one("SELECT sequence FROM settle_sequence WHERE id = 1", &[])?.get(0);

    // The pipeline has no reducer, so its schema text is sufficient to open it.
    let config = Config::with_data_dir(p.to_ddl()?, "./settle-data").applied_sequence(applied as u64);
    let mut db = Settle::open(config)?;
    // [!endregion open]

    // [!region loop]
    println!("continue after: {:?}", db.latest_cursor());

    let from = db.latest_cursor().map_or(0, |c| c.number) + 1;

    for block in from..from + 3 {
        let batch = db.ingest(IngestInput {
            data: [("swaps".to_string(), swaps_of(block))].into(),
            rollback_chain: vec![BlockCursor { number: block, hash: format!("0x{block}") }],
            finalized_head: BlockCursor { number: block - 1, hash: format!("0x{}", block - 1) },
        })?;

        apply(&mut pg, &mut db, batch)?;
    }
    // [!endregion loop]

    let sequence: i64 = pg.query_one("SELECT sequence FROM settle_sequence", &[])?.get(0);
    println!("sequence in Postgres: {sequence}");

    for row in pg.query("SELECT pool, volume, swaps FROM pool_volume ORDER BY pool", &[])? {
        let (pool, volume, swaps): (String, f64, i64) = (row.get(0), row.get(1), row.get(2));
        println!("{{ pool: {pool}, volume: {volume}, swaps: {swaps} }}");
    }

    Ok(())
}
