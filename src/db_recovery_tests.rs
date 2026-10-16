//! Crash-recovery tests. A Settle instance that is dropped and reopened must
//! go on exactly as one that never stopped: the derived state it persisted,
//! plus the raw rows it replays, must rebuild the same reducers and MVs.

use super::test_helpers::*;
use super::*;
use crate::test_helpers::cursor;
use crate::types::{ChangeOp, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const SUM_SCHEMA: &str = r#"
    CREATE TABLE swaps (
        block_number UInt64,
        pool String,
        amount UInt64,
        ts DateTime
    );

    CREATE MATERIALIZED VIEW pool_volume AS
    SELECT
        pool,
        sum(amount) AS total,
        count() AS swaps,
        min(amount) AS low,
        max(amount) AS high
    FROM swaps
    GROUP BY pool;
"#;

const SLIDING_SCHEMA: &str = r#"
    CREATE TABLE swaps (
        block_number UInt64,
        pool String,
        amount UInt64,
        ts DateTime
    );

    CREATE MATERIALIZED VIEW pool_1h AS
    SELECT
        pool,
        sum(amount) AS total,
        count() AS swaps
    FROM swaps
    GROUP BY pool
    WINDOW SLIDING INTERVAL 1 HOUR BY ts;
"#;

const REDUCER_SCHEMA: &str = r#"
    CREATE TABLE swaps (
        block_number UInt64,
        pool String,
        amount UInt64,
        ts DateTime
    );

    CREATE REDUCER running
    SOURCE swaps
    GROUP BY pool
    STATE (
        balance Float64 DEFAULT 0
    )
        WHEN row.amount > 0 THEN
            SET state.balance = state.balance + row.amount
            EMIT seen = row.amount
        ALWAYS EMIT
            state.balance AS balance
    END;

    CREATE MATERIALIZED VIEW pool_summary AS
    SELECT
        pool,
        last(balance) AS balance,
        sum(seen) AS total,
        max(seen) AS high,
        count() AS events
    FROM running
    GROUP BY pool;
"#;

/// No count(): a row with a NULL amount feeds no aggregate.
const SLIDING_SUM_SCHEMA: &str = r#"
    CREATE TABLE swaps (
        block_number UInt64,
        pool String,
        amount UInt64,
        ts DateTime
    );

    CREATE MATERIALIZED VIEW pool_1h AS
    SELECT
        pool,
        sum(amount) AS total
    FROM swaps
    GROUP BY pool
    WINDOW SLIDING INTERVAL 1 HOUR BY ts;
"#;

/// A window that nothing in a test history outlives: forks without expiry.
const SLIDING_NO_EXPIRY_SCHEMA: &str = r#"
    CREATE TABLE swaps (
        block_number UInt64,
        pool String,
        amount UInt64,
        ts DateTime
    );

    CREATE MATERIALIZED VIEW pool_30d AS
    SELECT
        pool,
        sum(amount) AS total,
        count() AS swaps
    FROM swaps
    GROUP BY pool
    WINDOW SLIDING INTERVAL 30 DAY BY ts;
"#;

/// Ten minutes per block, so a one-hour window holds six blocks.
fn swap(block: BlockNumber, pool: &str, amount: u64) -> RowMap {
    HashMap::from([
        ("pool".to_string(), Value::String(pool.to_string())),
        ("amount".to_string(), Value::UInt64(amount)),
        ("ts".to_string(), Value::DateTime(block as i64 * 600_000)),
    ])
}

fn reopen(schema: &str, storage: &Arc<MemoryBackend>, interval: u64) -> Settle {
    let config = Config::new(schema)
        .storage(storage.clone())
        .backfill_checkpoint_interval(interval);
    Settle::open(config).unwrap()
}

fn total(batch: &ChangeBatch, view: &str, pool: &str) -> Value {
    batch
        .records_for(view)
        .iter()
        .find(|r| r.key.get("pool") == Some(&Value::String(pool.into())))
        .and_then(|r| r.values.get("total").cloned())
        .unwrap()
}

/// A fork replaces a block of a sliding window while finality stays put, so
/// no finalize persists the window. On reopen the stored window must not
/// bring back the replaced block.
#[test]
fn reopen_after_fork_does_not_restore_replaced_sliding_block() {
    let storage = Arc::new(MemoryBackend::new());

    {
        let mut db = reopen(SLIDING_SCHEMA, &storage, 1);
        ingest_with_finalized(
            &mut db,
            vec![
                ("swaps".into(), 1, vec![swap(1, "ETH", 10)]),
                ("swaps".into(), 2, vec![swap(2, "ETH", 20)]),
            ],
            0,
        )
        .unwrap();
        handle_fork(&mut db, vec![cursor(1)]).unwrap();
        ingest_with_finalized(&mut db, vec![("swaps".into(), 2, vec![swap(2, "ETH", 30)])], 0)
            .unwrap();
    }

    let mut db = reopen(SLIDING_SCHEMA, &storage, 1);
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 3, vec![swap(3, "ETH", 10)])], 0)
        .unwrap()
        .unwrap();
    assert_eq!(total(&batch, "pool_1h", "ETH"), Value::UInt64(50));
}

/// Block 8 expires block 1 of the one-hour window. Block 1 is final and
/// block 8 is not, so after a reopen a fork to block 7 must still bring
/// block 1 back.
#[test]
fn fork_after_reopen_restores_block_expired_by_rolled_back_block() {
    let storage = Arc::new(MemoryBackend::new());
    {
        let mut db = reopen(SLIDING_SCHEMA, &storage, 1);
        let blocks = (1..=8)
            .map(|b| ("swaps".into(), b, vec![swap(b, "ETH", b)]))
            .collect();
        ingest_with_finalized(&mut db, blocks, 2).unwrap();
    }

    let mut db = reopen(SLIDING_SCHEMA, &storage, 1);
    let batch = handle_fork(&mut db, vec![cursor(7)]).unwrap().batch.unwrap();
    assert_eq!(total(&batch, "pool_1h", "ETH"), Value::UInt64(28));
}

/// Block 2 feeds no aggregate, so after a reopen it has a window time and no
/// groups. A fork to block 1 must still remove that time, or the new block 2
/// counts as replayed and is skipped.
#[test]
fn fork_after_reopen_replaces_block_without_aggregate_values() {
    let storage = Arc::new(MemoryBackend::new());
    {
        let mut db = reopen(SLIDING_SUM_SCHEMA, &storage, 1);
        let mut no_amount = swap(2, "ETH", 0);
        no_amount.insert("amount".into(), Value::Null);
        ingest_with_finalized(
            &mut db,
            vec![
                ("swaps".into(), 1, vec![swap(1, "ETH", 10)]),
                ("swaps".into(), 2, vec![no_amount]),
            ],
            1,
        )
        .unwrap();
    }

    let mut db = reopen(SLIDING_SUM_SCHEMA, &storage, 1);
    handle_fork(&mut db, vec![cursor(1)]).unwrap();
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 2, vec![swap(2, "ETH", 30)])], 1)
        .unwrap()
        .unwrap();
    assert_eq!(total(&batch, "pool_1h", "ETH"), Value::UInt64(40));
}

/// A heartbeat on an empty database reports finality far ahead. No data is
/// durable then, so a later block below that finality must still be
/// persisted or replayed.
#[test]
fn empty_heartbeat_does_not_make_later_blocks_durable() {
    let storage = Arc::new(MemoryBackend::new());

    {
        let mut db = reopen(SUM_SCHEMA, &storage, 1);
        ingest_with_finalized(&mut db, vec![], 100).unwrap();
        ingest_with_finalized(&mut db, vec![("swaps".into(), 3, vec![swap(3, "ETH", 10)])], 100)
            .unwrap();
    }

    let mut db = reopen(SUM_SCHEMA, &storage, 1);
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 4, vec![swap(4, "ETH", 10)])], 100)
        .unwrap()
        .unwrap();
    assert_eq!(total(&batch, "pool_volume", "ETH"), Value::UInt64(20));
}

/// Backfill deferral from the first block: block 0 must survive a reopen
/// before the first checkpoint.
#[test]
fn deferred_block_zero_survives_reopen() {
    let storage = Arc::new(MemoryBackend::new());

    {
        let mut db = reopen(SUM_SCHEMA, &storage, 100);
        ingest_with_finalized(&mut db, vec![("swaps".into(), 0, vec![swap(0, "ETH", 10)])], 0)
            .unwrap();
        ingest_with_finalized(&mut db, vec![("swaps".into(), 1, vec![swap(1, "ETH", 10)])], 1)
            .unwrap();
    }

    let mut db = reopen(SUM_SCHEMA, &storage, 100);
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 2, vec![swap(2, "ETH", 10)])], 2)
        .unwrap()
        .unwrap();
    assert_eq!(total(&batch, "pool_volume", "ETH"), Value::UInt64(30));
}

/// Deferral starts only after a checkpoint exists. Before that, recovery
/// would have to replay the chain from block 0 on every reopen.
#[test]
fn first_data_block_is_a_checkpoint_under_deferral() {
    let storage = Arc::new(MemoryBackend::new());
    let mut db = reopen(SUM_SCHEMA, &storage, 100);

    ingest_with_finalized(&mut db, vec![], 7).unwrap();
    assert_eq!(db.engine.durable_block(), None, "no data, so nothing is durable");
    assert!(storage.get_meta(META_FINALIZED_BLOCK).unwrap().is_none());

    ingest_with_finalized(&mut db, vec![("swaps".into(), 5, vec![swap(5, "ETH", 10)])], 7)
        .unwrap();
    assert_eq!(db.engine.durable_block(), Some(5), "the first data block persists");

    ingest_with_finalized(&mut db, vec![("swaps".into(), 8, vec![swap(8, "ETH", 10)])], 8)
        .unwrap();
    assert_eq!(db.engine.durable_block(), Some(5), "later blocks defer");
}

/// A heartbeat on an empty database stores no latest block. If it stored 0,
/// a reopen would read back block 0 as ingested, the next heartbeat would make
/// block 0 durable, and block 0's data would then never be persisted.
#[test]
fn reopen_of_empty_database_keeps_block_zero() {
    let storage = Arc::new(MemoryBackend::new());

    {
        let mut db = reopen(SUM_SCHEMA, &storage, 1);
        ingest_with_finalized(&mut db, vec![], 0).unwrap();
    }
    {
        let mut db = reopen(SUM_SCHEMA, &storage, 1);
        assert_eq!(db.latest_cursor(), None);
        ingest_with_finalized(&mut db, vec![], 0).unwrap();
        ingest_with_finalized(&mut db, vec![("swaps".into(), 0, vec![swap(0, "ETH", 10)])], 0)
            .unwrap();
    }

    let mut db = reopen(SUM_SCHEMA, &storage, 1);
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 1, vec![swap(1, "ETH", 10)])], 1)
        .unwrap()
        .unwrap();
    assert_eq!(total(&batch, "pool_volume", "ETH"), Value::UInt64(20));
}

/// Databases written before the durable watermark existed store latest 0
/// when empty, and their finality even when it is above the latest block. A
/// block at or below that watermark must still be persisted.
#[test]
fn legacy_watermark_without_data_does_not_hide_new_blocks() {
    for (stored_finalized, first) in [(0u64, 0), (100, 0), (100, 7)] {
        let storage = Arc::new(MemoryBackend::new());
        let mut legacy = StorageWriteBatch::new();
        legacy.put_meta(META_LATEST_BLOCK, &0u64.to_be_bytes());
        legacy.put_meta(META_FINALIZED_BLOCK, &stored_finalized.to_be_bytes());
        storage.commit(&legacy).unwrap();

        {
            let mut db = reopen(SUM_SCHEMA, &storage, 1);
            let rows = vec![("swaps".into(), first, vec![swap(first, "ETH", 10)])];
            ingest_with_finalized(&mut db, rows, stored_finalized).unwrap();
        }

        let mut db = reopen(SUM_SCHEMA, &storage, 1);
        let next = stored_finalized + 1;
        let batch =
            ingest_with_finalized(&mut db, vec![("swaps".into(), next, vec![swap(next, "ETH", 10)])], next)
                .unwrap()
                .unwrap();
        assert_eq!(
            total(&batch, "pool_volume", "ETH"),
            Value::UInt64(20),
            "stored finality {stored_finalized}, first block {first}"
        );
    }
}

/// The same for a legacy database with data: finality stored above its
/// latest block, as on a gappy chain.
#[test]
fn legacy_watermark_above_latest_does_not_hide_new_blocks() {
    let storage = Arc::new(MemoryBackend::new());

    {
        let mut db = reopen(SUM_SCHEMA, &storage, 1);
        ingest_with_finalized(&mut db, vec![("swaps".into(), 50, vec![swap(50, "ETH", 10)])], 50)
            .unwrap();
    }
    let mut legacy = StorageWriteBatch::new();
    legacy.put_meta(META_FINALIZED_BLOCK, &100u64.to_be_bytes());
    storage.commit(&legacy).unwrap();

    {
        let mut db = reopen(SUM_SCHEMA, &storage, 1);
        ingest_with_finalized(&mut db, vec![("swaps".into(), 60, vec![swap(60, "ETH", 10)])], 100)
            .unwrap();
    }

    let mut db = reopen(SUM_SCHEMA, &storage, 1);
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 101, vec![swap(101, "ETH", 10)])], 101)
        .unwrap()
        .unwrap();
    assert_eq!(total(&batch, "pool_volume", "ETH"), Value::UInt64(30));
}

/// Every ingest writes the block hashes. Under deferral the hashes between
/// the durable block and finality must not pile up there: only the durable
/// block's hash, which a reopen needs, stays below finality.
#[test]
fn deferral_keeps_only_the_durable_hash_below_finality() {
    let storage = Arc::new(MemoryBackend::new());

    {
        let mut db = reopen(SUM_SCHEMA, &storage, 1000);
        for block in 0..50 {
            ingest_with_finalized(&mut db, vec![("swaps".into(), block, vec![swap(block, "ETH", 1)])], block)
                .unwrap();
        }
        assert_eq!(db.engine.durable_block(), Some(0));
        let hashes: Vec<BlockNumber> = db.engine.block_hashes().keys().copied().collect();
        assert_eq!(hashes, vec![0, 49]);
    }

    let mut db = reopen(SUM_SCHEMA, &storage, 1000);
    assert_eq!(db.finalized_cursor(), Some(cursor(0)));
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 50, vec![swap(50, "ETH", 1)])], 50)
        .unwrap()
        .unwrap();
    assert_eq!(total(&batch, "pool_volume", "ETH"), Value::UInt64(51));
}

// ---------------------------------------------------------------------------
// Differential test: random histories, one instance reopened at random points,
// one never reopened. A downstream target fed by each must stay identical.
// ---------------------------------------------------------------------------

/// xorshift64*: deterministic, so a failing seed replays exactly.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// What a downstream database holds after it applies every change batch.
#[derive(Default)]
struct Target(BTreeMap<(String, String), String>);

impl Target {
    fn apply(&mut self, batch: &ChangeBatch) {
        for (table, records) in &batch.tables {
            for record in records {
                let key = (table.clone(), sorted_debug(&record.key));
                match record.operation {
                    ChangeOp::Insert | ChangeOp::Update => {
                        self.0.insert(key, sorted_debug(&record.values));
                    }
                    ChangeOp::Delete => {
                        self.0.remove(&key);
                    }
                }
            }
        }
    }

    /// One line per row that differs from `reference`.
    fn diff(&self, reference: &Target) -> Vec<String> {
        let keys: BTreeSet<_> = self.0.keys().chain(reference.0.keys()).collect();
        keys.into_iter()
            .filter_map(|key| {
                let (ours, theirs) = (self.0.get(key), reference.0.get(key));
                (ours != theirs).then(|| format!("{key:?}\n  reopened:  {ours:?}\n  reference: {theirs:?}"))
            })
            .collect()
    }
}

fn sorted_debug(map: &HashMap<String, Value>) -> String {
    format!("{:?}", map.iter().collect::<BTreeMap<_, _>>())
}

#[derive(Debug)]
enum Op {
    /// Data blocks (none for a heartbeat). The rollback chain holds them and
    /// the current tip, as a client sends it, so an ingest never forks.
    Ingest {
        blocks: Vec<(BlockNumber, Vec<RowMap>)>,
        tip: Option<BlockNumber>,
        finalized: BlockNumber,
    },
    /// Back to `at`, through `handle_fork` or through an ingest whose
    /// rollback chain ends there.
    Fork {
        at: BlockNumber,
        by_ingest: bool,
        finalized: BlockNumber,
    },
    Reopen,
}

impl Op {
    fn input(op: &Op) -> IngestInput {
        let mut rows = Vec::new();
        let mut chain = Vec::new();

        let finalized = match op {
            Op::Ingest {
                blocks,
                tip,
                finalized,
            } => {
                chain.extend(tip.map(cursor));
                for (block, block_rows) in blocks {
                    chain.push(cursor(*block));
                    for row in block_rows {
                        let mut row = row.clone();
                        row.insert("block_number".to_string(), Value::UInt64(*block));
                        rows.push(row);
                    }
                }
                *finalized
            }
            Op::Fork { at, finalized, .. } => {
                chain.push(cursor(*at));
                *finalized
            }
            Op::Reopen => unreachable!(),
        };

        let data = if rows.is_empty() {
            HashMap::new()
        } else {
            HashMap::from([("swaps".to_string(), rows)])
        };
        IngestInput {
            data,
            rollback_chain: chain,
            finalized_head: cursor(finalized),
        }
    }
}

/// Generates a valid history: block numbers grow (with gaps), finality never
/// goes back, and forks stay at or above finality.
struct History {
    rng: Rng,
    forks: bool,
    latest: Option<BlockNumber>,
    finalized: BlockNumber,
    data_blocks: Vec<BlockNumber>,
}

impl History {
    fn next_op(&mut self) -> Op {
        let roll = self.rng.below(100);

        if roll < 15 {
            return Op::Reopen;
        }

        if self.forks && roll < 27 {
            let candidates: Vec<BlockNumber> = self
                .data_blocks
                .iter()
                .copied()
                .filter(|&b| b >= self.finalized && Some(b) < self.latest)
                .collect();
            if !candidates.is_empty() {
                let at = candidates[self.rng.below(candidates.len() as u64) as usize];
                return Op::Fork {
                    at,
                    by_ingest: self.rng.below(2) == 0,
                    finalized: self.finalized,
                };
            }
        }

        let heartbeat = roll < 40;
        let mut blocks = Vec::new();
        if !heartbeat {
            let mut block = match self.latest {
                None => self.rng.below(2),
                Some(latest) => latest + 1 + self.rng.below(2),
            };
            for _ in 0..1 + self.rng.below(3) {
                let rows = (0..1 + self.rng.below(2))
                    .map(|_| {
                        let pool = ["A", "B", "C"][self.rng.below(3) as usize];
                        swap(block, pool, 1 + self.rng.below(9))
                    })
                    .collect();
                blocks.push((block, rows));
                block += 1 + self.rng.below(2);
            }
        }

        let tip = blocks.last().map(|(b, _)| *b).or(self.latest).unwrap_or(0);
        let reported = match self.rng.below(100) {
            0..30 => self.finalized,
            30..60 => tip,
            60..85 => tip.saturating_sub(self.rng.below(4)),
            _ => tip + 20 + self.rng.below(50),
        };
        Op::Ingest {
            blocks,
            tip: self.latest,
            finalized: reported.max(self.finalized),
        }
    }

    fn applied(&mut self, op: &Op) {
        match op {
            Op::Ingest {
                blocks, finalized, ..
            } => {
                if let Some((last, _)) = blocks.last() {
                    self.latest = Some(*last);
                }
                self.data_blocks.extend(blocks.iter().map(|(b, _)| *b));
                self.finalized = *finalized;
            }
            Op::Fork { at, .. } => {
                self.latest = Some(*at);
                self.data_blocks.retain(|b| b <= at);
            }
            Op::Reopen => {}
        }
    }
}

fn run(db: &mut Settle, target: &mut Target, op: &Op) -> std::result::Result<(), String> {
    let batch = match op {
        Op::Fork {
            at,
            by_ingest: false,
            ..
        } => db.handle_fork(vec![cursor(*at)]).map(|r| r.batch),
        _ => db.ingest(Op::input(op)),
    }
    .map_err(|e| e.to_string())?;

    if let Some(batch) = batch {
        target.apply(&batch);
        db.ack(batch.sequence).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn check_history(schema: &str, interval: u64, forks: bool, seed: u64, steps: usize) {
    let storage = Arc::new(MemoryBackend::new());
    let mut reopened = reopen(schema, &storage, interval);
    let mut reference = reopen(schema, &Arc::new(MemoryBackend::new()), interval);
    let mut reopened_target = Target::default();
    let mut reference_target = Target::default();

    let mut history = History {
        rng: Rng::new(seed),
        forks,
        latest: None,
        finalized: 0,
        data_blocks: Vec::new(),
    };
    let mut log = Vec::new();

    for _ in 0..steps {
        let op = history.next_op();
        log.push(format!("{op:?}"));

        if let Op::Reopen = op {
            drop(reopened);
            reopened = reopen(schema, &storage, interval);
            continue;
        }

        let expected = run(&mut reference, &mut reference_target, &op);
        let actual = run(&mut reopened, &mut reopened_target, &op);
        assert_eq!(
            actual.is_ok(),
            expected.is_ok(),
            "seed {seed}, interval {interval}: reopened {actual:?} vs reference {expected:?}\n{}",
            log.join("\n")
        );
        if expected.is_ok() {
            history.applied(&op);
        }

        let diff = reopened_target.diff(&reference_target);
        assert!(
            diff.is_empty(),
            "seed {seed}, interval {interval}: outputs differ after\n{}\n\n{}",
            log.join("\n"),
            diff.join("\n")
        );
    }
}

#[test]
fn random_histories_survive_reopen_sum() {
    for seed in 0..300 {
        check_history(SUM_SCHEMA, 1, true, seed, 40);
    }
}

#[test]
fn random_histories_survive_reopen_sum_deferred() {
    for seed in 0..300 {
        check_history(SUM_SCHEMA, 3, true, seed, 40);
    }
}

#[test]
fn random_histories_survive_reopen_reducer() {
    for seed in 0..300 {
        check_history(REDUCER_SCHEMA, 1, true, seed, 40);
    }
}

#[test]
fn random_histories_survive_reopen_reducer_deferred() {
    for seed in 0..300 {
        check_history(REDUCER_SCHEMA, 3, true, seed, 40);
    }
}

/// Short histories with many seeds reach the states near an empty database
/// and block 0, where the watermarks are None or 0, far more often.
#[test]
fn random_short_histories_survive_reopen() {
    for seed in 0..5000 {
        check_history(SUM_SCHEMA, 1, true, seed, 8);
        check_history(SUM_SCHEMA, 3, true, seed, 8);
    }
}

#[test]
fn random_histories_survive_reopen_sliding_forks() {
    for seed in 0..300 {
        check_history(SLIDING_NO_EXPIRY_SCHEMA, 1, true, seed, 40);
    }
}

#[test]
fn random_histories_survive_reopen_sliding_expiry() {
    for seed in 0..300 {
        check_history(SLIDING_SCHEMA, 1, true, seed, 40);
    }
}

#[test]
fn random_short_histories_survive_reopen_sliding() {
    for seed in 0..5000 {
        check_history(SLIDING_SCHEMA, 1, true, seed, 8);
    }
}
