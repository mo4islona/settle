use super::*;
use crate::msgpack_conv::{decode_data_from_msgpack, decode_table_rows_from_msgpack};
use crate::test_helpers::cursor;
use std::collections::HashMap;

/// A raw table without a `block_number` column, one with it, and a VIRTUAL
/// one, each read by a view directly and through a reducer.
const SCHEMA: &str = r#"
    CREATE TABLE swaps (
        pool String,
        amount UInt64,
        ts DateTime
    );

    CREATE TABLE trades (
        block_number UInt64,
        user String,
        side String,
        amount Float64,
        price Float64
    );

    CREATE VIRTUAL TABLE transfers (
        block_number UInt64,
        wallet String,
        value UInt64
    );

    CREATE MATERIALIZED VIEW pool_stats AS
    SELECT
        pool,
        sum(amount) AS volume,
        min(amount) AS low,
        max(amount) AS high,
        count() AS swaps,
        last(ts) AS last_ts
    FROM swaps
    GROUP BY pool;

    CREATE MATERIALIZED VIEW pool_hour AS
    SELECT pool, sum(amount) AS volume
    FROM swaps
    GROUP BY pool
    WINDOW SLIDING INTERVAL 1 HOUR BY ts;

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
        WHEN row.side = 'buy' THEN
            SET state.quantity = state.quantity + row.amount
            SET state.cost_basis = state.cost_basis + row.amount * row.price
            EMIT trade_pnl = 0
        WHEN row.side = 'sell' THEN
            SET state.quantity = state.quantity - row.amount
            EMIT trade_pnl = row.amount * row.price
        ALWAYS EMIT
            state.quantity AS position_size
    END;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl) AS total_pnl,
        last(position_size) AS current_position,
        count() AS trade_count
    FROM pnl
    GROUP BY user;

    CREATE REDUCER balance
    SOURCE transfers
    GROUP BY wallet
    STATE (
        total UInt64 DEFAULT 0
    )
    LANGUAGE lua
    PROCESS $$
        state.total = state.total + row.value
        emit({ wallet = row.wallet, total = state.total })
    $$;

    CREATE MATERIALIZED VIEW wallet_total AS
    SELECT wallet, last(total) AS total, count() AS transfers
    FROM balance
    GROUP BY wallet;

    CREATE MATERIALIZED VIEW trade_users AS
    SELECT user, sum(amount) AS amount, max(price) AS high
    FROM trades
    GROUP BY user;
"#;

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

/// A random chain with finality behind the tip and forks above finality.
struct Chain {
    rng: Lcg,
    hashes: BTreeMap<BlockNumber, String>,
    epoch: u64,
    finalized: BlockNumber,
}

impl Chain {
    fn new(seed: u64) -> Self {
        Self {
            rng: Lcg(seed),
            hashes: BTreeMap::new(),
            epoch: 0,
            finalized: 0,
        }
    }

    fn cursor(&self, block: BlockNumber) -> BlockCursor {
        match self.hashes.get(&block) {
            Some(hash) => BlockCursor {
                number: block,
                hash: hash.clone(),
            },
            None => cursor(block),
        }
    }

    /// The next ingest: the msgpack data, the rollback chain and the
    /// finalized head.
    fn next_ingest(&mut self) -> (Vec<u8>, Vec<BlockCursor>, BlockCursor) {
        let latest = self.hashes.keys().next_back().copied().unwrap_or(0);

        // A fork replaces the blocks above a block that is not final.
        let mut first = latest + 1;
        if latest > self.finalized + 1 && self.rng.below(5) == 0 {
            let fork_point = self.finalized + self.rng.below(latest - self.finalized);
            self.epoch += 1;
            self.hashes.split_off(&(fork_point + 1));
            first = fork_point + 1;
        }

        let forked = first <= latest;
        let last = first + self.rng.below(4);
        let mut tables: HashMap<&str, Vec<serde_json::Value>> = HashMap::new();
        for block in first..=last {
            self.hashes
                .insert(block, format!("0x{block:x}-{}", self.epoch));
            for _ in 0..self.rng.below(5) {
                tables.entry("swaps").or_default().push(self.swap(block));
            }
            for _ in 0..self.rng.below(4) {
                tables.entry("trades").or_default().push(self.trade(block));
            }
            for _ in 0..self.rng.below(3) {
                tables
                    .entry("transfers")
                    .or_default()
                    .push(self.transfer(block));
            }
        }
        let data = rmp_serde::to_vec(&tables).unwrap();

        // The chain must hold a common ancestor at or above finality.
        if !forked {
            self.finalized = self.finalized.max(last.saturating_sub(self.rng.below(4)));
        }
        let chain = (self.finalized + 1..=last)
            .map(|b| self.cursor(b))
            .collect();
        (data, chain, self.cursor(self.finalized))
    }

    fn swap(&mut self, block: BlockNumber) -> serde_json::Value {
        let pool = ["ETH/USDC", "BTC/USDC", "SOL/USDC"][self.rng.below(3) as usize];
        // A column that the table does not have.
        serde_json::json!({
            "block_number": block,
            "pool": pool,
            "amount": self.rng.below(1000),
            "ts": 1_700_000_000_000u64 + block * 600_000,
            "note": "dropped",
        })
    }

    fn trade(&mut self, block: BlockNumber) -> serde_json::Value {
        let user = ["alice", "bob", "carol", "dave"][self.rng.below(4) as usize];
        let side = ["buy", "sell"][self.rng.below(2) as usize];
        serde_json::json!({
            "block_number": block,
            "user": user,
            "side": side,
            "amount": (1 + self.rng.below(50)) as f64,
            "price": (100 + self.rng.below(20)) as f64 / 4.0,
        })
    }

    fn transfer(&mut self, block: BlockNumber) -> serde_json::Value {
        let wallet = ["w1", "w2", "w3"][self.rng.below(3) as usize];
        serde_json::json!({
            "block_number": block,
            "wallet": wallet,
            "value": self.rng.below(100),
        })
    }
}

/// A batch with its tables in name order: `tables` is a HashMap.
fn canonical(batch: &ChangeBatch) -> String {
    let tables: BTreeMap<_, _> = batch.tables.iter().collect();
    format!(
        "{} {:?} {:?} {tables:?}",
        batch.sequence, batch.finalized_head, batch.latest_head
    )
}

/// Ingest by name and by position, and ack.
fn ingest_both(
    by_name: &mut Settle,
    by_position: &mut Settle,
    data: &[u8],
    rollback_chain: Vec<BlockCursor>,
    finalized_head: BlockCursor,
) -> (Option<String>, Option<String>) {
    let maps = decode_data_from_msgpack(data).unwrap();
    let rows = decode_table_rows_from_msgpack(data, |t| by_position.table_registry(t)).unwrap();

    let a = by_name
        .ingest(IngestInput {
            data: maps,
            rollback_chain: rollback_chain.clone(),
            finalized_head: finalized_head.clone(),
        })
        .unwrap();
    let b = by_position
        .ingest_rows(IngestRows {
            data: rows,
            rollback_chain,
            finalized_head,
        })
        .unwrap();

    for (db, batch) in [(&mut *by_name, &a), (&mut *by_position, &b)] {
        if let Some(batch) = batch {
            db.ack(batch.sequence).unwrap();
        }
    }
    (a.as_ref().map(canonical), b.as_ref().map(canonical))
}

fn open(dir: &std::path::Path) -> Settle {
    Settle::open(Config::with_data_dir(SCHEMA, dir.to_str().unwrap()).collect_perf(false)).unwrap()
}

/// The positional path of the bindings (msgpack → `ingest_rows`) produces
/// the same change batches as rows by name, through forks, sliding expiry
/// and a reopen that replays the unfinalized blocks.
#[test]
fn rows_by_position_match_rows_by_name() {
    for seed in 1..=4 {
        let name_dir = tempfile::tempdir().unwrap();
        let position_dir = tempfile::tempdir().unwrap();
        let mut chain = Chain::new(seed);
        let mut batches = 0;

        for round in 0..2 {
            let mut by_name = open(name_dir.path());
            let mut by_position = open(position_dir.path());

            for step in 0..60 {
                let (data, rollback_chain, finalized) = chain.next_ingest();
                let (a, b) = ingest_both(
                    &mut by_name,
                    &mut by_position,
                    &data,
                    rollback_chain,
                    finalized,
                );
                assert_eq!(a, b, "seed {seed}, round {round}, step {step}");
                batches += a.is_some() as usize;
            }
        }
        assert!(batches > 100, "seed {seed}: {batches} batches");
    }
}

fn simple_db() -> Settle {
    Settle::open(Config::new(SCHEMA)).unwrap()
}

fn rows_input(table: &str, block: BlockNumber, rows: Vec<Row>) -> IngestRows {
    IngestRows {
        data: HashMap::from([(table.to_string(), BTreeMap::from([(block, rows)]))]),
        rollback_chain: vec![cursor(block)],
        finalized_head: cursor(0),
    }
}

fn trade(registry: &Arc<ColumnRegistry>, block: BlockNumber) -> Row {
    Row::from_owned_map(
        registry.clone(),
        HashMap::from([
            ("block_number".to_string(), Value::UInt64(block)),
            ("user".to_string(), Value::String("alice".into())),
            ("side".to_string(), Value::String("buy".into())),
            ("amount".to_string(), Value::Float64(1.0)),
            ("price".to_string(), Value::Float64(2.0)),
        ]),
    )
}

#[test]
fn ingest_rows_accepts_a_registry_with_the_same_columns() {
    let mut db = simple_db();
    let registry = Arc::new(db.table_registry("trades").unwrap().as_ref().clone());

    let batch = db
        .ingest_rows(rows_input("trades", 1, vec![trade(&registry, 1)]))
        .unwrap()
        .unwrap();
    let users = &batch.tables["trade_users"];
    assert_eq!(users[0].get("amount"), Some(&Value::Float64(1.0)));
}

#[test]
fn ingest_rows_rejects_rows_with_other_columns() {
    let mut db = simple_db();
    let registry = db.table_registry("transfers").unwrap().clone();
    let row = Row::from_owned_map(
        registry,
        HashMap::from([("block_number".to_string(), Value::UInt64(1))]),
    );

    let err = db
        .ingest_rows(rows_input("trades", 1, vec![row]))
        .err()
        .unwrap();
    assert!(err.to_string().contains("must have its columns"), "{err}");
    assert_eq!(db.latest_cursor(), None);
}

#[test]
fn ingest_rows_rejects_a_block_number_column_of_another_block() {
    let mut db = simple_db();
    let registry = db.table_registry("trades").unwrap().clone();

    let err = db
        .ingest_rows(rows_input("trades", 2, vec![trade(&registry, 1)]))
        .err()
        .unwrap();
    assert!(
        err.to_string().contains("at block 2 has block_number"),
        "{err}"
    );
}

#[test]
fn ingest_rows_rejects_an_unknown_table_with_rows() {
    let mut db = simple_db();
    let registry = db.table_registry("trades").unwrap().clone();

    let err = db
        .ingest_rows(rows_input("orders", 1, vec![trade(&registry, 1)]))
        .err()
        .unwrap();
    assert!(err.to_string().contains("unknown table: orders"), "{err}");

    // With no rows the table is not looked up, as in `ingest`.
    db.ingest_rows(rows_input("orders", 1, vec![])).unwrap();
}

#[test]
fn ingest_rows_rejects_a_block_at_or_below_latest() {
    let mut db = simple_db();
    let registry = db.table_registry("trades").unwrap().clone();
    let batch = db
        .ingest_rows(rows_input("trades", 5, vec![trade(&registry, 5)]))
        .unwrap()
        .unwrap();
    db.ack(batch.sequence).unwrap();

    let mut input = rows_input("trades", 5, vec![trade(&registry, 5)]);
    // An empty block below does not count.
    input.data.get_mut("trades").unwrap().insert(3, Vec::new());
    let err = db.ingest_rows(input).err().unwrap();
    assert!(
        err.to_string().contains("duplicate ingest: block 5"),
        "{err}"
    );
}
