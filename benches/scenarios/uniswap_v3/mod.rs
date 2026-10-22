//! Scenario 2: the pools of Uniswap V3 on Ethereum. Per pool: the swap
//! stats and the last price, 1-minute and 1-hour candles, and the tokens in
//! the pool (TVL) from its swaps, mints and burns.
//!
//! Amounts stay exact on both sides: Uint256 in Settle, NUMERIC in Postgres.

use std::collections::HashMap;
use std::ops::Range;

use ethnum::{I256, U256};
use rustc_hash::FxHashMap;
use settle::db::{BlockRows, Settle};
use settle::types::{ChangeBatch, ChangeOp, ChangeRecord, Row, Value};

use crate::common::PgRuntime;
use crate::fixture::{Fixture, hex};
use crate::harness::Scenario;

pub struct UniswapV3;

const CANDLES: [(&str, i64); 2] = [("candles_1m", 60_000), ("candles_1h", 3_600_000)];

struct Swap {
    /// Token1 per token0 in raw units, after the swap.
    price: f64,
    tick: i64,
}

pub struct Event {
    pool: u32,
    /// The tokens that went into the pool; negative when they left it.
    delta0: I256,
    delta1: I256,
    /// None for a mint or a burn.
    swap: Option<Swap>,
}

/// The decoder output: each pool address once, and the events by fixture row.
pub struct Data {
    pools: Vec<String>,
    events: Vec<Event>,
}

#[derive(Debug, PartialEq)]
struct PoolStats {
    swaps: i64,
    volume0: U256,
    volume1: U256,
    price: f64,
    tick: i64,
}

#[derive(Debug, PartialEq)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume0: U256,
    volume1: U256,
    swaps: i64,
}

/// The app state of some blocks: the whole chain from the first block for
/// the check, one batch for the Postgres-only side.
#[derive(Default)]
struct State {
    pools: FxHashMap<u32, PoolStats>,
    /// One map for each of `CANDLES`, by (pool, start in Unix ms).
    candles: [FxHashMap<(u32, i64), Candle>; 2],
    tvl: FxHashMap<u32, (I256, I256)>,
}

impl State {
    fn of(fx: &Fixture, data: &Data, blocks: Range<usize>) -> Self {
        let mut state = Self::default();
        for block in &fx.blocks[blocks] {
            let ts = block.timestamp as i64 * 1000;
            for event in &data.events[block.rows.clone()] {
                state.add(event, ts);
            }
        }
        state
    }

    fn add(&mut self, event: &Event, ts: i64) {
        let tvl = self.tvl.entry(event.pool).or_default();
        tvl.0 += event.delta0;
        tvl.1 += event.delta1;

        let Some(swap) = &event.swap else {
            return;
        };
        let volume0 = event.delta0.unsigned_abs();
        let volume1 = event.delta1.unsigned_abs();

        let pool = self.pools.entry(event.pool).or_insert(PoolStats {
            swaps: 0,
            volume0: U256::ZERO,
            volume1: U256::ZERO,
            price: 0.0,
            tick: 0,
        });
        pool.swaps += 1;
        pool.volume0 += volume0;
        pool.volume1 += volume1;
        pool.price = swap.price;
        pool.tick = swap.tick;

        for (candles, (_, interval)) in self.candles.iter_mut().zip(CANDLES) {
            let start = ts - ts % interval;
            let candle = candles.entry((event.pool, start)).or_insert(Candle {
                open: swap.price,
                high: swap.price,
                low: swap.price,
                close: swap.price,
                volume0: U256::ZERO,
                volume1: U256::ZERO,
                swaps: 0,
            });
            candle.high = candle.high.max(swap.price);
            candle.low = candle.low.min(swap.price);
            candle.close = swap.price;
            candle.volume0 += volume0;
            candle.volume1 += volume1;
            candle.swaps += 1;
        }
    }
}

/// How an upsert meets the row that Postgres has.
#[derive(Clone, Copy, PartialEq)]
enum Upsert {
    /// Postgres only: the row holds the blocks before the batch, the input
    /// the blocks of the batch.
    Merge,
    /// Settle: the input is the whole row.
    Replace,
}

fn write_pools<'a>(
    pg: &PgRuntime,
    upsert: Upsert,
    rows: impl Iterator<Item = (&'a str, &'a PoolStats)>,
) -> anyhow::Result<()> {
    let (mut pools, mut swaps, mut volume0, mut volume1, mut price, mut tick) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (pool, stats) in rows {
        pools.push(pool);
        swaps.push(stats.swaps);
        volume0.push(stats.volume0.to_string());
        volume1.push(stats.volume1.to_string());
        price.push(stats.price);
        tick.push(stats.tick as i32);
    }
    if pools.is_empty() {
        return Ok(());
    }

    let set = match upsert {
        Upsert::Merge => {
            "swaps = t.swaps + EXCLUDED.swaps, volume0 = t.volume0 + EXCLUDED.volume0, \
             volume1 = t.volume1 + EXCLUDED.volume1"
        }
        Upsert::Replace => {
            "swaps = EXCLUDED.swaps, volume0 = EXCLUDED.volume0, volume1 = EXCLUDED.volume1"
        }
    };
    let sql = format!(
        "INSERT INTO pools AS t (pool, swaps, volume0, volume1, price, tick) \
         SELECT * FROM unnest($1::text[], $2::int8[], $3::text[]::numeric[], \
             $4::text[]::numeric[], $5::float8[], $6::int4[]) \
         ON CONFLICT (pool) DO UPDATE SET {set}, price = EXCLUDED.price, tick = EXCLUDED.tick"
    );
    pg.execute(&sql, &[&pools, &swaps, &volume0, &volume1, &price, &tick])?;
    Ok(())
}

fn write_candles<'a>(
    pg: &PgRuntime,
    table: &str,
    upsert: Upsert,
    rows: impl Iterator<Item = ((&'a str, i64), &'a Candle)>,
) -> anyhow::Result<()> {
    let (mut pools, mut starts, mut open, mut high, mut low, mut close) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    let (mut volume0, mut volume1, mut swaps) = (vec![], vec![], vec![]);
    for ((pool, start), candle) in rows {
        pools.push(pool);
        starts.push(start);
        open.push(candle.open);
        high.push(candle.high);
        low.push(candle.low);
        close.push(candle.close);
        volume0.push(candle.volume0.to_string());
        volume1.push(candle.volume1.to_string());
        swaps.push(candle.swaps);
    }
    if pools.is_empty() {
        return Ok(());
    }

    // The open of a merged candle is the open of its first batch.
    let set = match upsert {
        Upsert::Merge => {
            "high = GREATEST(t.high, EXCLUDED.high), low = LEAST(t.low, EXCLUDED.low), \
             close = EXCLUDED.close, volume0 = t.volume0 + EXCLUDED.volume0, \
             volume1 = t.volume1 + EXCLUDED.volume1, swaps = t.swaps + EXCLUDED.swaps"
        }
        Upsert::Replace => {
            "open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, \
             close = EXCLUDED.close, volume0 = EXCLUDED.volume0, \
             volume1 = EXCLUDED.volume1, swaps = EXCLUDED.swaps"
        }
    };
    let sql = format!(
        "INSERT INTO {table} AS t (pool, start, open, high, low, close, volume0, volume1, swaps) \
         SELECT * FROM unnest($1::text[], $2::int8[], $3::float8[], $4::float8[], \
             $5::float8[], $6::float8[], $7::text[]::numeric[], $8::text[]::numeric[], \
             $9::int8[]) \
         ON CONFLICT (pool, start) DO UPDATE SET {set}"
    );
    pg.execute(
        &sql,
        &[
            &pools, &starts, &open, &high, &low, &close, &volume0, &volume1, &swaps,
        ],
    )?;
    Ok(())
}

fn write_tvl<'a>(
    pg: &PgRuntime,
    upsert: Upsert,
    rows: impl Iterator<Item = (&'a str, (I256, I256))>,
) -> anyhow::Result<()> {
    let (mut pools, mut amount0, mut amount1) = (vec![], vec![], vec![]);
    for (pool, (delta0, delta1)) in rows {
        pools.push(pool);
        amount0.push(delta0.to_string());
        amount1.push(delta1.to_string());
    }
    if pools.is_empty() {
        return Ok(());
    }

    let set = match upsert {
        Upsert::Merge => {
            "amount0 = t.amount0 + EXCLUDED.amount0, amount1 = t.amount1 + EXCLUDED.amount1"
        }
        Upsert::Replace => "amount0 = EXCLUDED.amount0, amount1 = EXCLUDED.amount1",
    };
    let sql = format!(
        "INSERT INTO tvl AS t (pool, amount0, amount1) \
         SELECT * FROM unnest($1::text[], $2::text[]::numeric[], $3::text[]::numeric[]) \
         ON CONFLICT (pool) DO UPDATE SET {set}"
    );
    pg.execute(&sql, &[&pools, &amount0, &amount1])?;
    Ok(())
}

fn uint256(value: U256) -> Value {
    Value::Uint256(value.to_be_bytes())
}

/// (in, out) of a flow into a pool.
fn split(delta: I256) -> (Value, Value) {
    let amount = uint256(delta.unsigned_abs());
    let zero = uint256(U256::ZERO);
    if delta.is_negative() {
        (zero, amount)
    } else {
        (amount, zero)
    }
}

fn value_of<'a>(record: &'a ChangeRecord, column: &str) -> &'a Value {
    record
        .get(column)
        .unwrap_or_else(|| panic!("{}: no column {column}", record.table()))
}

fn read_text<'a>(record: &'a ChangeRecord, column: &str) -> &'a str {
    value_of(record, column).as_str().expect("a String")
}

fn read_int(record: &ChangeRecord, column: &str) -> i64 {
    value_of(record, column).as_i64().expect("an integer")
}

fn read_float(record: &ChangeRecord, column: &str) -> f64 {
    value_of(record, column).as_f64().expect("a Float64")
}

fn read_u256(record: &ChangeRecord, column: &str) -> U256 {
    match value_of(record, column) {
        Value::Uint256(bytes) => U256::from_be_bytes(*bytes),
        other => panic!("{column}: {other:?}, a Uint256 expected"),
    }
}

impl Scenario for UniswapV3 {
    const NAME: &'static str = "uniswap_v3";
    const SETTLE_SCHEMA: &'static str = include_str!("settle.sql");
    const PG_SCHEMA: &'static str = include_str!("pg.sql");
    const PG_TABLES: &'static [&'static str] = &["pools", "candles_1m", "candles_1h", "tvl"];

    type Data = Data;

    fn decode(fx: &Fixture) -> Data {
        let kind_at = fx.column("kind");
        let pool_at = fx.column("pool");
        let amount0_at = fx.column("amount0");
        let amount1_at = fx.column("amount1");
        let sqrt_price_at = fx.column("sqrt_price_x96");
        let tick_at = fx.column("tick");
        let word =
            |row: &[u8], at: &Range<usize>| -> [u8; 32] { row[at.clone()].try_into().unwrap() };

        let mut ids: FxHashMap<[u8; 20], u32> = FxHashMap::default();
        let mut pools = Vec::new();
        let mut events = Vec::with_capacity(fx.row_count());
        for i in 0..fx.row_count() {
            let row = fx.row(i);
            let address: [u8; 20] = row[pool_at.clone()].try_into().unwrap();
            let pool = *ids.entry(address).or_insert_with(|| {
                pools.push(format!("0x{}", hex(&address)));
                (pools.len() - 1) as u32
            });
            let amount0 = I256::from_be_bytes(word(row, &amount0_at));
            let amount1 = I256::from_be_bytes(word(row, &amount1_at));

            let event = match row[kind_at.start] {
                0 => {
                    let sqrt_price =
                        U256::from_be_bytes(word(row, &sqrt_price_at)).as_f64() / 2f64.powi(96);
                    let swap = Swap {
                        price: sqrt_price * sqrt_price,
                        tick: I256::from_be_bytes(word(row, &tick_at)).as_i64(),
                    };
                    Event {
                        pool,
                        delta0: amount0,
                        delta1: amount1,
                        swap: Some(swap),
                    }
                }
                1 => Event {
                    pool,
                    delta0: amount0,
                    delta1: amount1,
                    swap: None,
                },
                2 => Event {
                    pool,
                    delta0: -amount0,
                    delta1: -amount1,
                    swap: None,
                },
                other => panic!("unknown event kind {other}"),
            };
            events.push(event);
        }
        Data { pools, events }
    }

    fn pg_only_write(
        pg: &PgRuntime,
        fx: &Fixture,
        data: &Data,
        blocks: Range<usize>,
    ) -> anyhow::Result<()> {
        let batch = State::of(fx, data, blocks);
        let name = |id: u32| data.pools[id as usize].as_str();

        write_pools(
            pg,
            Upsert::Merge,
            batch.pools.iter().map(|(&id, stats)| (name(id), stats)),
        )?;
        for (candles, (table, _)) in batch.candles.iter().zip(CANDLES) {
            let rows = candles
                .iter()
                .map(|(&(id, start), candle)| ((name(id), start), candle));
            write_candles(pg, table, Upsert::Merge, rows)?;
        }
        write_tvl(
            pg,
            Upsert::Merge,
            batch.tvl.iter().map(|(&id, &tvl)| (name(id), tvl)),
        )
    }

    fn settle_rows(
        db: &Settle,
        fx: &Fixture,
        data: &Data,
        blocks: Range<usize>,
    ) -> HashMap<String, BlockRows> {
        let swaps_registry = db.table_registry("swaps").expect("table");
        let flows_registry = db.table_registry("flows").expect("table");

        let mut swaps = BlockRows::new();
        let mut flows = BlockRows::new();
        for block in &fx.blocks[blocks] {
            let number = Value::UInt64(block.number);
            let ts = Value::DateTime(block.timestamp as i64 * 1000);
            let mut block_swaps = Vec::new();
            let mut block_flows = Vec::new();

            for event in &data.events[block.rows.clone()] {
                let pool = Value::String(data.pools[event.pool as usize].clone());
                if let Some(swap) = &event.swap {
                    let values = vec![
                        number.clone(),
                        ts.clone(),
                        pool.clone(),
                        Value::Float64(swap.price),
                        Value::Int64(swap.tick),
                        uint256(event.delta0.unsigned_abs()),
                        uint256(event.delta1.unsigned_abs()),
                    ];
                    block_swaps.push(Row::from_values(swaps_registry.clone(), values));
                }

                let (in0, out0) = split(event.delta0);
                let (in1, out1) = split(event.delta1);
                let values = vec![number.clone(), pool, in0, out0, in1, out1];
                block_flows.push(Row::from_values(flows_registry.clone(), values));
            }

            if !block_swaps.is_empty() {
                swaps.insert(block.number, block_swaps);
            }
            if !block_flows.is_empty() {
                flows.insert(block.number, block_flows);
            }
        }
        HashMap::from([("swaps".to_string(), swaps), ("flows".to_string(), flows)])
    }

    fn pg_apply(pg: &PgRuntime, batch: &ChangeBatch) -> anyhow::Result<()> {
        let mut pools = Vec::new();
        let mut gone_pools = Vec::new();
        for record in batch.records_for("pools") {
            let pool = read_text(record, "pool");
            if record.operation == ChangeOp::Delete {
                gone_pools.push(pool);
                continue;
            }
            let stats = PoolStats {
                swaps: read_int(record, "swaps"),
                volume0: read_u256(record, "volume0"),
                volume1: read_u256(record, "volume1"),
                price: read_float(record, "price"),
                tick: read_int(record, "tick"),
            };
            pools.push((pool, stats));
        }
        write_pools(pg, Upsert::Replace, pools.iter().map(|(p, s)| (*p, s)))?;
        if !gone_pools.is_empty() {
            pg.execute("DELETE FROM pools WHERE pool = ANY($1)", &[&gone_pools])?;
        }

        for (table, _) in CANDLES {
            let mut candles = Vec::new();
            let (mut gone_pools, mut gone_starts) = (Vec::new(), Vec::new());
            for record in batch.records_for(table) {
                let key = (read_text(record, "pool"), read_int(record, "start"));
                if record.operation == ChangeOp::Delete {
                    gone_pools.push(key.0);
                    gone_starts.push(key.1);
                    continue;
                }
                let candle = Candle {
                    open: read_float(record, "open"),
                    high: read_float(record, "high"),
                    low: read_float(record, "low"),
                    close: read_float(record, "close"),
                    volume0: read_u256(record, "volume0"),
                    volume1: read_u256(record, "volume1"),
                    swaps: read_int(record, "swaps"),
                };
                candles.push((key, candle));
            }
            write_candles(
                pg,
                table,
                Upsert::Replace,
                candles.iter().map(|(k, c)| (*k, c)),
            )?;
            if !gone_pools.is_empty() {
                let sql = format!(
                    "DELETE FROM {table} WHERE (pool, start) IN \
                     (SELECT * FROM unnest($1::text[], $2::int8[]))"
                );
                pg.execute(&sql, &[&gone_pools, &gone_starts])?;
            }
        }

        let mut tvl = Vec::new();
        let mut gone_pools = Vec::new();
        for record in batch.records_for("tvl") {
            let pool = read_text(record, "pool");
            if record.operation == ChangeOp::Delete {
                gone_pools.push(pool);
                continue;
            }
            let net = |flow_in: &str, flow_out: &str| {
                read_u256(record, flow_in).as_i256() - read_u256(record, flow_out).as_i256()
            };
            tvl.push((pool, (net("in0", "out0"), net("in1", "out1"))));
        }
        write_tvl(pg, Upsert::Replace, tvl.into_iter())?;
        if !gone_pools.is_empty() {
            pg.execute("DELETE FROM tvl WHERE pool = ANY($1)", &[&gone_pools])?;
        }
        Ok(())
    }

    fn check(pg: &PgRuntime, fx: &Fixture, data: &Data, end: usize) -> anyhow::Result<()> {
        let expected = State::of(fx, data, 0..end);
        let ids: FxHashMap<&str, u32> = data
            .pools
            .iter()
            .enumerate()
            .map(|(id, pool)| (pool.as_str(), id as u32))
            .collect();
        let id = |pool: &str| ids.get(pool).copied().unwrap_or(u32::MAX);
        let numeric = |text: &str| -> anyhow::Result<U256> { Ok(text.parse()?) };

        let rows = pg.query(
            "SELECT pool, swaps, volume0::text, volume1::text, price, tick FROM pools",
            &[],
        )?;
        anyhow::ensure!(
            rows.len() == expected.pools.len(),
            "{} pools in Postgres, {} expected",
            rows.len(),
            expected.pools.len()
        );
        for row in &rows {
            let pool: &str = row.get(0);
            let actual = PoolStats {
                swaps: row.get(1),
                volume0: numeric(row.get(2))?,
                volume1: numeric(row.get(3))?,
                price: row.get(4),
                tick: row.get::<_, i32>(5) as i64,
            };
            let want = expected.pools.get(&id(pool));
            anyhow::ensure!(
                want == Some(&actual),
                "pool {pool}: {actual:?} in Postgres, {want:?} expected"
            );
        }

        for (candles, (table, _)) in expected.candles.iter().zip(CANDLES) {
            let sql = format!(
                "SELECT pool, start, open, high, low, close, volume0::text, volume1::text, swaps \
                 FROM {table}"
            );
            let rows = pg.query(&sql, &[])?;
            anyhow::ensure!(
                rows.len() == candles.len(),
                "{} rows in {table}, {} expected",
                rows.len(),
                candles.len()
            );
            for row in &rows {
                let (pool, start): (&str, i64) = (row.get(0), row.get(1));
                let actual = Candle {
                    open: row.get(2),
                    high: row.get(3),
                    low: row.get(4),
                    close: row.get(5),
                    volume0: numeric(row.get(6))?,
                    volume1: numeric(row.get(7))?,
                    swaps: row.get(8),
                };
                let want = candles.get(&(id(pool), start));
                anyhow::ensure!(
                    want == Some(&actual),
                    "{table} {pool} {start}: {actual:?} in Postgres, {want:?} expected"
                );
            }
        }

        let rows = pg.query("SELECT pool, amount0::text, amount1::text FROM tvl", &[])?;
        anyhow::ensure!(
            rows.len() == expected.tvl.len(),
            "{} rows in tvl, {} expected",
            rows.len(),
            expected.tvl.len()
        );
        for row in &rows {
            let pool: &str = row.get(0);
            let actual: (I256, I256) = (
                row.get::<_, &str>(1).parse()?,
                row.get::<_, &str>(2).parse()?,
            );
            let want = expected.tvl.get(&id(pool));
            anyhow::ensure!(
                want == Some(&actual),
                "tvl {pool}: {actual:?} in Postgres, {want:?} expected"
            );
        }
        Ok(())
    }
}
