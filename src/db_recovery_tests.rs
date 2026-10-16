//! Crash-recovery tests. A Settle instance that is dropped and reopened must
//! go on exactly as one that never stopped: the derived state it persisted,
//! plus the raw rows it replays, must rebuild the same reducers and MVs.

use super::test_helpers::*;
use super::*;
use crate::test_helpers::{cursor, ingest_with_finalized_no_ack};
use crate::types::{ChangeOp, Value};
use std::sync::atomic::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

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

/// Two external reducers (`FailingSum`): one takes its rows one by one and
/// fails on 13, the other takes a block of groups at once and fails on 14.
/// External reducers run one after the other, so each needs its own amount.
const FAILING_SCHEMA: &str = r#"
    CREATE TABLE swaps (
        block_number UInt64,
        pool String,
        amount UInt64,
        ts DateTime
    );

    CREATE REDUCER per_row
    SOURCE swaps
    GROUP BY pool
    STATE (
        total UInt64 DEFAULT 0
    )
    LANGUAGE EXTERNAL;

    CREATE REDUCER batched
    SOURCE swaps
    GROUP BY pool
    STATE (
        total UInt64 DEFAULT 0
    )
    LANGUAGE EXTERNAL;

    CREATE MATERIALIZED VIEW per_row_total AS
    SELECT pool, last(total) AS total, count() AS events
    FROM per_row
    GROUP BY pool;

    CREATE MATERIALIZED VIEW batched_total AS
    SELECT pool, last(total) AS total, count() AS events
    FROM batched
    GROUP BY pool;
"#;

const PER_ROW_POISON: u64 = 13;
const BATCHED_POISON: u64 = 14;

/// Sums `amount` per group and fails on `poison`, after the rows before it,
/// also in other groups, changed their states.
struct FailingSum {
    batched: bool,
    poison: u64,
}

impl crate::reducer_runtime::ReducerRuntime for FailingSum {
    fn process(&self, state: &mut HashMap<String, Value>, row: &crate::types::Row) -> Result<Vec<RowMap>> {
        let amount = row.get("amount").and_then(|v| v.as_u64()).unwrap_or(0);
        if amount == self.poison {
            return Err(Error::Reducer("unlucky amount".into()));
        }

        let total = state.get("total").and_then(|v| v.as_u64()).unwrap_or(0) + amount;
        state.insert("total".into(), Value::UInt64(total));
        Ok(vec![HashMap::from([("total".to_string(), Value::UInt64(total))])])
    }

    fn use_batched_processing(&self) -> bool {
        self.batched
    }
}

fn attach_runtimes(db: &mut Settle, schema: &str) {
    if schema == FAILING_SCHEMA {
        let per_row = FailingSum {
            batched: false,
            poison: PER_ROW_POISON,
        };
        let batched = FailingSum {
            batched: true,
            poison: BATCHED_POISON,
        };
        db.register_reducer_callback("per_row", Box::new(per_row)).unwrap();
        db.register_reducer_callback("batched", Box::new(batched)).unwrap();
    }
}

/// Ten minutes per block, so a one-hour window holds six blocks.
/// A backend with no bounded raw-row read of its own returns the whole replay
/// range at once; replay must not read the rest of it again for each part.
#[test]
fn replay_reads_each_block_once_without_bounded_reads() {
    let storage = Arc::new(FailingStorage::default());
    let open = || {
        let config = Config::new(SUM_SCHEMA)
            .storage(storage.clone())
            .backfill_checkpoint_interval(u64::MAX);
        Settle::open(config).unwrap()
    };

    let mut db = open();
    for block in 1..=40 {
        let mut row = swap(block, "A", 1);
        row.insert("block_number".to_string(), Value::UInt64(block));
        let input = IngestInput {
            data: HashMap::from([("swaps".to_string(), vec![row])]),
            rollback_chain: Vec::new(),
            finalized_head: cursor(block),
        };
        let batch = db.ingest(input).unwrap().unwrap();
        db.ack(batch.sequence).unwrap();
    }
    drop(db);

    storage.blocks_read.store(0, Ordering::SeqCst);
    drop(open());

    // Block 1 is durable; the replay reads the 39 blocks after it.
    assert_eq!(storage.blocks_read.load(Ordering::SeqCst), 39);
}

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
    let mut db = Settle::open(config).unwrap();
    attach_runtimes(&mut db, schema);
    db
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

/// The reducer fails on an amount of 13.
#[cfg(feature = "lua")]
const FAILING_REDUCER_SCHEMA: &str = r#"
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
        total UInt64 DEFAULT 0
    )
    LANGUAGE lua
    PROCESS $$
        if row.amount == 13 then error("unlucky amount") end
        state.total = state.total + row.amount
        emit({ pool = row.pool, total = state.total })
    $$;

    CREATE MATERIALIZED VIEW pool_total AS
    SELECT pool, last(total) AS total, count() AS events
    FROM running
    GROUP BY pool;
"#;

/// One block of a new chain that forks off after block 1.
fn new_chain_block(block: BlockNumber, amount: u64) -> IngestInput {
    let mut row = swap(block, "A", amount);
    row.insert("block_number".into(), Value::UInt64(block));
    IngestInput {
        data: HashMap::from([("swaps".to_string(), vec![row])]),
        rollback_chain: vec![
            cursor(1),
            BlockCursor {
                number: block,
                hash: format!("0xnew{block}"),
            },
        ],
        finalized_head: cursor(0),
    }
}

fn apply_and_ack(db: &mut Settle, target: &mut Target, batch: Option<ChangeBatch>) {
    if let Some(batch) = batch {
        target.apply(&batch);
        db.ack(batch.sequence).unwrap();
    }
}

/// Block 1, then blocks 2 and 3 in a batch that the target applies and has
/// not acked yet, then an ingest that detects a fork to block 1 and fails.
/// Pool B is only on the old fork. A caller that acks the applied batch,
/// goes on or reopens a poisoned instance, and sends the new chain again
/// must get the same downstream rows as a run without the failure.
fn check_failed_fork_ingest(schema: &str, failing: IngestInput) {
    let storage = Arc::new(MemoryBackend::new());
    let mut db = reopen(schema, &storage, 1);
    let mut target = Target::default();

    let block_1 = vec![("swaps".into(), 1, vec![swap(1, "A", 1)])];
    let batch = ingest_with_finalized_no_ack(&mut db, block_1, 0).unwrap();
    apply_and_ack(&mut db, &mut target, batch);

    let old_fork = (2..=3).map(|b| ("swaps".into(), b, vec![swap(b, "B", b)])).collect();
    let applied = ingest_with_finalized_no_ack(&mut db, old_fork, 0).unwrap().unwrap();
    target.apply(&applied);

    assert!(db.ingest(failing).is_err());
    db.ack(applied.sequence).unwrap();

    let batch = match db.ingest(new_chain_block(2, 5)) {
        Err(Error::Poisoned(_)) => {
            drop(db);
            db = reopen(schema, &storage, 1);
            db.ingest(new_chain_block(2, 5)).unwrap()
        }
        other => other.unwrap(),
    };
    apply_and_ack(&mut db, &mut target, batch);

    let mut reference_db = reopen(schema, &Arc::new(MemoryBackend::new()), 1);
    let mut reference = Target::default();
    let block_1 = vec![("swaps".into(), 1, vec![swap(1, "A", 1)])];
    reference.apply(&ingest_with_finalized(&mut reference_db, block_1, 0).unwrap().unwrap());
    let batch = reference_db.ingest(new_chain_block(2, 5)).unwrap();
    apply_and_ack(&mut reference_db, &mut reference, batch);

    let diff = target.diff(&reference);
    assert!(diff.is_empty(), "{}", diff.join("\n"));
}

#[cfg(feature = "lua")]
#[test]
fn reducer_error_after_ingest_fork_loses_no_compensation() {
    check_failed_fork_ingest(FAILING_REDUCER_SCHEMA, new_chain_block(2, 13));
}

/// The input error is found before the fork rollback, so the instance
/// stays as it was.
#[test]
fn input_error_after_ingest_fork_loses_no_compensation() {
    let mut at_fork_point = new_chain_block(2, 5);
    let rows = at_fork_point.data.get_mut("swaps").unwrap();
    rows[0].insert("block_number".into(), Value::UInt64(1));
    check_failed_fork_ingest(SUM_SCHEMA, at_fork_point);

    let mut bad_type = new_chain_block(2, 5);
    let rows = bad_type.data.get_mut("swaps").unwrap();
    rows[0].insert("block_number".into(), Value::String("2".into()));
    check_failed_fork_ingest(SUM_SCHEMA, bad_type);
}

/// The raw tables roll back last, after the reducers and MVs. A read
/// failure there leaves the engine half rolled back.
#[test]
fn rollback_failure_poisons_instance() {
    let storage = Arc::new(FailingStorage::default());
    let config = Config::new(REDUCER_SCHEMA).storage(storage.clone());
    let mut db = Settle::open(config).unwrap();

    let blocks = (1..=3).map(|b| ("swaps".into(), b, vec![swap(b, "A", b)])).collect();
    ingest_with_finalized(&mut db, blocks, 0).unwrap();

    storage.fail_reads.store(true, Ordering::SeqCst);
    assert!(db.handle_fork(vec![cursor(1)]).is_err());
    assert!(db.is_poisoned());
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
#[derive(Default, Clone)]
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

#[derive(Debug, Clone)]
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
    /// An ingest that `FailingSum` rejects.
    fn poisoned(&self) -> bool {
        let Op::Ingest { blocks, .. } = self else {
            return false;
        };
        let poison = [PER_ROW_POISON, BATCHED_POISON].map(Value::UInt64);
        blocks
            .iter()
            .flat_map(|(_, rows)| rows)
            .filter_map(|row| row.get("amount"))
            .any(|amount| poison.contains(amount))
    }

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
    /// Some data ingests get a row that a `FailingSum` rejects.
    poison: bool,
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
                let rows: Vec<RowMap> = (0..1 + self.rng.below(2))
                    .map(|_| {
                        let pool = ["A", "B", "C"][self.rng.below(3) as usize];
                        swap(block, pool, 1 + self.rng.below(9))
                    })
                    .collect();
                blocks.push((block, rows));
                block += 1 + self.rng.below(2);
            }

            // An undo keeps block 0, which counts as no block (#31).
            let above_block_0 = blocks[0].0 > 0;
            if self.poison && above_block_0 && self.rng.below(4) == 0 {
                let at = self.rng.below(blocks.len() as u64) as usize;
                let (block, rows) = &mut blocks[at];
                let pool = ["A", "B", "C"][self.rng.below(3) as usize];
                let position = self.rng.below(rows.len() as u64 + 1) as usize;
                let amount = [PER_ROW_POISON, BATCHED_POISON][self.rng.below(2) as usize];
                rows.insert(position, swap(*block, pool, amount));
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
        poison: schema == FAILING_SCHEMA,
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

        // The reference never sees a rejected ingest.
        let expected = if op.poisoned() {
            Err("poisoned".to_string())
        } else {
            run(&mut reference, &mut reference_target, &op)
        };
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

/// A reducer error in the middle of an ingest leaves no trace, also with
/// the rows before it in the same block and in other groups.
#[test]
fn random_histories_with_reducer_errors() {
    for seed in 0..300 {
        check_history(FAILING_SCHEMA, 1, true, seed, 40);
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

// ---------------------------------------------------------------------------
// Pipelined acks: the same random histories, with batches that wait for
// their ack while the next ones are ingested.
// ---------------------------------------------------------------------------

/// A client that lets up to `depth` batches wait for their ack. Its target
/// applies the batches late, in order, and acks some of the applied ones. A
/// crash loses the unacked batches, so the client sends again every op whose
/// writes were not acked.
struct PipelinedClient {
    schema: &'static str,
    interval: u64,
    depth: usize,
    storage: Arc<MemoryBackend>,
    db: Settle,
    rng: Rng,
    target: Target,
    /// Returned and not applied yet, in sequence order.
    unapplied: VecDeque<ChangeBatch>,
    /// Applied and not acked yet.
    applied_unacked: VecDeque<u64>,
    /// Ops whose writes are not on disk, with the sequence whose ack commits
    /// them.
    not_durable: VecDeque<(Op, u64)>,
}

fn open_pipelined(schema: &str, storage: &Arc<MemoryBackend>, interval: u64, depth: usize) -> Settle {
    let config = Config::new(schema)
        .storage(storage.clone())
        .backfill_checkpoint_interval(interval)
        .max_pending_batches(depth);
    let mut db = Settle::open(config).unwrap();
    attach_runtimes(&mut db, schema);
    db
}

impl PipelinedClient {

    fn apply_next(&mut self) {
        let batch = self.unapplied.pop_front().unwrap();
        self.target.apply(&batch);
        self.applied_unacked.push_back(batch.sequence);
    }

    fn ack(&mut self, sequence: u64) {
        self.db.ack(sequence).unwrap();
        self.applied_unacked.retain(|&s| s > sequence);
        self.not_durable.retain(|&(_, s)| s > sequence);
    }

    fn make_room(&mut self) {
        while self.db.unacked_batches() >= self.depth {
            if self.applied_unacked.is_empty() {
                self.apply_next();
            }
            let oldest = self.applied_unacked[0];
            self.ack(oldest);
        }
    }

    fn run(&mut self, op: &Op) -> std::result::Result<(), String> {
        self.make_room();

        let batch = match op {
            Op::Fork {
                at,
                by_ingest: false,
                ..
            } => self.db.handle_fork(vec![cursor(*at)]).map(|r| r.batch),
            _ => self.db.ingest(Op::input(op)),
        }
        .map_err(|e| e.to_string())?;

        // An op without a batch commits at once, or with the newest batch.
        let commit_sequence = match &batch {
            Some(batch) => Some(batch.sequence),
            None => self.newest_unacked(),
        };
        if let Some(sequence) = commit_sequence {
            self.not_durable.push_back((op.clone(), sequence));
        }
        self.unapplied.extend(batch);
        Ok(())
    }

    fn newest_unacked(&self) -> Option<u64> {
        let unapplied = self.unapplied.back().map(|b| b.sequence);
        unapplied.or(self.applied_unacked.back().copied())
    }

    /// The target applies some batches and acks up to one of them.
    fn consume(&mut self) {
        let apply = self.rng.below(self.unapplied.len() as u64 + 1);
        for _ in 0..apply {
            self.apply_next();
        }

        if !self.applied_unacked.is_empty() && self.rng.below(2) == 0 {
            let at = self.rng.below(self.applied_unacked.len() as u64) as usize;
            let sequence = self.applied_unacked[at];
            self.ack(sequence);
        }
    }

    /// The process dies: unacked batches are lost, applied rows stay.
    fn crash(&mut self) {
        self.db = open_pipelined(self.schema, &self.storage, self.interval, self.depth);
        self.unapplied.clear();
        self.applied_unacked.clear();

        for (op, _) in std::mem::take(&mut self.not_durable) {
            self.run(&op).expect("an op that succeeded before succeeds again");
        }
    }

    /// The target once every returned batch is applied.
    fn drained_target(&self) -> Target {
        let mut target = self.target.clone();
        for batch in &self.unapplied {
            target.apply(batch);
        }
        target
    }
}

fn check_pipelined_history(schema: &'static str, interval: u64, depth: usize, seed: u64, steps: usize) {
    let storage = Arc::new(MemoryBackend::new());
    let mut client = PipelinedClient {
        schema,
        interval,
        depth,
        db: open_pipelined(schema, &storage, interval, depth),
        storage,
        rng: Rng::new(seed ^ 0x5EED),
        target: Target::default(),
        unapplied: VecDeque::new(),
        applied_unacked: VecDeque::new(),
        not_durable: VecDeque::new(),
    };

    let mut reference = reopen(schema, &Arc::new(MemoryBackend::new()), interval);
    let mut reference_target = Target::default();

    let mut history = History {
        rng: Rng::new(seed),
        forks: true,
        poison: schema == FAILING_SCHEMA,
        latest: None,
        finalized: 0,
        data_blocks: Vec::new(),
    };
    let mut log = Vec::new();

    for _ in 0..steps {
        let op = history.next_op();
        log.push(format!("{op:?}"));

        if let Op::Reopen = op {
            client.crash();
        } else {
            let expected = if op.poisoned() {
                Err("poisoned".to_string())
            } else {
                run(&mut reference, &mut reference_target, &op)
            };
            let actual = client.run(&op);
            assert_eq!(
                actual.is_ok(),
                expected.is_ok(),
                "seed {seed}, depth {depth}, interval {interval}: pipelined {actual:?} vs reference {expected:?}\n{}",
                log.join("\n")
            );
            if expected.is_ok() {
                history.applied(&op);
            }
        }
        client.consume();

        let diff = client.drained_target().diff(&reference_target);
        assert!(
            diff.is_empty(),
            "seed {seed}, depth {depth}, interval {interval}: outputs differ after\n{}\n\n{}",
            log.join("\n"),
            diff.join("\n")
        );
    }
}

#[test]
fn random_pipelined_histories_sum() {
    for seed in 0..300 {
        for depth in [2, 4] {
            check_pipelined_history(SUM_SCHEMA, 1, depth, seed, 40);
            check_pipelined_history(SUM_SCHEMA, 3, depth, seed, 40);
        }
    }
}

#[test]
fn random_pipelined_histories_reducer() {
    for seed in 0..300 {
        for depth in [2, 4] {
            check_pipelined_history(REDUCER_SCHEMA, 1, depth, seed, 40);
            check_pipelined_history(REDUCER_SCHEMA, 3, depth, seed, 40);
        }
    }
}

#[test]
fn random_pipelined_histories_sliding() {
    for seed in 0..300 {
        for depth in [2, 4] {
            check_pipelined_history(SLIDING_SCHEMA, 1, depth, seed, 40);
            check_pipelined_history(SLIDING_NO_EXPIRY_SCHEMA, 1, depth, seed, 40);
        }
    }
}

#[test]
fn random_pipelined_histories_with_reducer_errors() {
    for seed in 0..300 {
        for depth in [2, 4] {
            check_pipelined_history(FAILING_SCHEMA, 1, depth, seed, 40);
        }
    }
}

#[test]
fn random_short_pipelined_histories() {
    for seed in 0..3000 {
        check_pipelined_history(SUM_SCHEMA, 1, 2, seed, 10);
        check_pipelined_history(REDUCER_SCHEMA, 3, 3, seed, 10);
    }
}
