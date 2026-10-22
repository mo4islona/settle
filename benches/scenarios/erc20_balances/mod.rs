//! Scenario 1: the balance of each holder of USDC on Ethereum.
//!
//! USDC amounts fit in 63 bits, so both sides keep them in BIGINT.

use std::collections::HashMap;
use std::ops::Range;

use rustc_hash::FxHashMap;
use settle::db::{BlockRows, Settle};
use settle::types::{ChangeBatch, ChangeOp, Row, Value};

use crate::common::PgRuntime;
use crate::fixture::{Fixture, hex};
use crate::harness::Scenario;

pub struct Erc20Balances;

pub struct Transfer {
    from: u32,
    to: u32,
    value: i64,
}

/// The decoder output: each address once, and the transfers by fixture row.
pub struct Data {
    addresses: Vec<String>,
    transfers: Vec<Transfer>,
}

impl Data {
    fn deltas(&self, rows: Range<usize>) -> impl Iterator<Item = (u32, i64)> + '_ {
        self.transfers[rows]
            .iter()
            .flat_map(|t| [(t.from, -t.value), (t.to, t.value)])
    }
}

const UPSERT_DELTAS: &str = "\
    INSERT INTO balances (address, balance) \
    SELECT * FROM unnest($1::text[], $2::int8[]) \
    ON CONFLICT (address) DO UPDATE SET balance = balances.balance + EXCLUDED.balance";

const UPSERT_BALANCES: &str = "\
    INSERT INTO balances (address, balance) \
    SELECT * FROM unnest($1::text[], $2::int8[]) \
    ON CONFLICT (address) DO UPDATE SET balance = EXCLUDED.balance";

impl Scenario for Erc20Balances {
    const NAME: &'static str = "erc20_balances";
    const SETTLE_SCHEMA: &'static str = include_str!("settle.sql");
    const PG_SCHEMA: &'static str = include_str!("pg.sql");
    const PG_TABLES: &'static [&'static str] = &["balances"];

    type Data = Data;

    fn decode(fx: &Fixture) -> Data {
        let from = fx.column("from");
        let to = fx.column("to");
        let value = fx.column("value");

        let mut ids: FxHashMap<[u8; 20], u32> = FxHashMap::default();
        let mut addresses = Vec::new();
        let mut id = |raw: &[u8]| -> u32 {
            *ids.entry(raw.try_into().unwrap()).or_insert_with(|| {
                addresses.push(format!("0x{}", hex(raw)));
                (addresses.len() - 1) as u32
            })
        };

        let mut transfers = Vec::with_capacity(fx.row_count());
        for i in 0..fx.row_count() {
            let row = fx.row(i);
            let amount = &row[value.clone()];
            let low = u64::from_be_bytes(amount[24..].try_into().unwrap());
            assert!(
                amount[..24].iter().all(|&b| b == 0) && low <= i64::MAX as u64,
                "a transfer of more than 2^63 - 1"
            );
            transfers.push(Transfer {
                from: id(&row[from.clone()]),
                to: id(&row[to.clone()]),
                value: low as i64,
            });
        }
        Data {
            addresses,
            transfers,
        }
    }

    fn pg_only_write(
        pg: &PgRuntime,
        fx: &Fixture,
        data: &Data,
        blocks: Range<usize>,
    ) -> anyhow::Result<()> {
        let rows = fx.blocks[blocks.start].rows.start..fx.blocks[blocks.end - 1].rows.end;

        let mut sums: FxHashMap<u32, i64> = FxHashMap::default();
        for (address, delta) in data.deltas(rows) {
            *sums.entry(address).or_default() += delta;
        }
        let (addresses, deltas): (Vec<&str>, Vec<i64>) = sums
            .into_iter()
            .map(|(address, delta)| (data.addresses[address as usize].as_str(), delta))
            .unzip();

        pg.execute(UPSERT_DELTAS, &[&addresses, &deltas])?;
        Ok(())
    }

    fn settle_rows(
        db: &Settle,
        fx: &Fixture,
        data: &Data,
        blocks: Range<usize>,
    ) -> HashMap<String, BlockRows> {
        let registry = db.table_registry("balance_deltas").expect("table");
        let by_block: BlockRows = fx.blocks[blocks]
            .iter()
            .map(|block| {
                let rows = data
                    .deltas(block.rows.clone())
                    .map(|(address, delta)| {
                        let values = vec![
                            Value::UInt64(block.number),
                            Value::String(data.addresses[address as usize].clone()),
                            Value::Int64(delta),
                        ];
                        Row::from_values(registry.clone(), values)
                    })
                    .collect();
                (block.number, rows)
            })
            .collect();
        HashMap::from([("balance_deltas".to_string(), by_block)])
    }

    fn pg_apply(pg: &PgRuntime, batch: &ChangeBatch) -> anyhow::Result<()> {
        let mut addresses = Vec::new();
        let mut balances = Vec::new();
        let mut deleted = Vec::new();
        for record in batch.records_for("balances") {
            let address = record
                .get("address")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if record.operation == ChangeOp::Delete {
                deleted.push(address);
                continue;
            }
            addresses.push(address);
            balances.push(
                record
                    .get("balance")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
            );
        }

        if !addresses.is_empty() {
            pg.execute(UPSERT_BALANCES, &[&addresses, &balances])?;
        }
        if !deleted.is_empty() {
            pg.execute("DELETE FROM balances WHERE address = ANY($1)", &[&deleted])?;
        }
        Ok(())
    }

    fn check(pg: &PgRuntime, fx: &Fixture, data: &Data, end: usize) -> anyhow::Result<()> {
        let rows = 0..fx.blocks[end - 1].rows.end;
        let mut expected: FxHashMap<&str, i64> = FxHashMap::default();
        for (address, delta) in data.deltas(rows) {
            *expected
                .entry(&data.addresses[address as usize])
                .or_default() += delta;
        }

        let actual = pg.rt.block_on(
            pg.client
                .query("SELECT address, balance FROM balances", &[]),
        )?;
        anyhow::ensure!(
            actual.len() == expected.len(),
            "{} balances in Postgres, {} expected",
            actual.len(),
            expected.len()
        );
        for row in &actual {
            let address: &str = row.get(0);
            let balance: i64 = row.get(1);
            let want = expected.get(address).copied();
            anyhow::ensure!(
                want == Some(balance),
                "balance of {address}: {balance} in Postgres, {want:?} expected"
            );
        }
        Ok(())
    }
}
