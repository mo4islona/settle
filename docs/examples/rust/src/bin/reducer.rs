// [!region pipeline]
use settle::db::{IngestInput, Settle};
use settle::error::Result;
use settle::types::{BlockCursor, RowMap, Value};
use settle::{BuildOptions, Pipeline, ReducerOptions, ViewOptions, float64, string, uint64};

fn open() -> Result<Settle> {
    let mut p = Pipeline::new();

    let trades = p.virtual_table(
        "trades",
        [
            ("block_number", uint64()),
            ("trader", string()),
            ("side", string()),
            ("amount", float64()),
            ("price", float64()),
        ],
    );

    let pnl = trades.create_reducer(
        "pnl",
        ReducerOptions {
            group_by: vec!["trader".into()],
            initial_state: RowMap::from([
                ("position".into(), Value::Float64(0.0)),
                ("cost".into(), Value::Float64(0.0)),
            ]),
            reduce: Box::new(|state, row| {
                let amount = row.get("amount").and_then(Value::as_f64).unwrap_or(0.0);
                let price = row.get("price").and_then(Value::as_f64).unwrap_or(0.0);
                let is_buy = row.get("side").and_then(Value::as_str) == Some("buy");

                let (position, cost, profit) = if is_buy {
                    let position = state.get_f64("position") + amount;
                    let cost = state.get_f64("cost") + amount * price;
                    (position, cost, 0.0)
                } else {
                    let average_price = state.get_f64("cost") / state.get_f64("position");
                    let position = state.get_f64("position") - amount;
                    let cost = state.get_f64("cost") - amount * average_price;
                    (position, cost, amount * (price - average_price))
                };

                state.update(RowMap::from([
                    ("position".into(), Value::Float64(position)),
                    ("cost".into(), Value::Float64(cost)),
                ]));
                state.emit(RowMap::from([
                    ("profit".into(), Value::Float64(profit)),
                    ("position".into(), Value::Float64(position)),
                ]));
            }),
        },
    );

    pnl.create_view(
        "trader_pnl",
        ViewOptions {
            group_by: vec!["trader".into()],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("trader".into(), agg.key("trader").into()),
                    ("profit".into(), agg.sum("profit").into()),
                    ("position".into(), agg.last("position").into()),
                    ("trades".into(), agg.count().into()),
                ]
            }),
        },
    );

    p.build(BuildOptions::new())
}
// [!endregion pipeline]

fn trade(block: u64, side: &str, amount: f64, price: f64) -> RowMap {
    RowMap::from([
        ("block_number".into(), Value::UInt64(block)),
        ("trader".into(), Value::String("alice".into())),
        ("side".into(), Value::String(side.into())),
        ("amount".into(), Value::Float64(amount)),
        ("price".into(), Value::Float64(price)),
    ])
}

fn cursor(number: u64, hash: &str) -> BlockCursor {
    BlockCursor { number, hash: hash.into() }
}

// [!region ingest]
fn send_trades(db: &mut Settle) -> Result<()> {
    let batch = db.ingest(IngestInput {
        data: [(
            "trades".to_string(),
            vec![
                trade(1, "buy", 10.0, 2000.0),
                trade(2, "buy", 10.0, 2400.0),
                trade(3, "sell", 5.0, 2600.0),
            ],
        )]
        .into(),
        rollback_chain: vec![cursor(1, "0x1a"), cursor(2, "0x2a"), cursor(3, "0x3a")],
        finalized_head: cursor(0, "0x00"),
    })?;

    let Some(batch) = batch else { return Ok(()) };

    for record in &batch.tables["trader_pnl"] {
        let values: Vec<String> = record
            .values()
            .map(|(name, value)| format!("{name}: {value}"))
            .collect();

        let operation = format!("{:?}", record.operation).to_lowercase();

        println!("{operation} {{ {} }}", values.join(", "));
    }

    db.ack(batch.sequence)
}
// [!endregion ingest]

fn main() -> Result<()> {
    let mut db = open()?;
    send_trades(&mut db)
}
