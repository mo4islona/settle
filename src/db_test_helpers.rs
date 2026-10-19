// The ingest/rollback helpers live in `crate::test_helpers` so they're also
// reachable from integration tests under `tests/` and benches under `benches/`.
// Re-export them here so the unit tests under `db::` keep their existing
// `use super::test_helpers::*;` import without further plumbing.
pub use crate::test_helpers::{
    handle_fork, ingest_blocks, ingest_input, ingest_one, ingest_with_finalized, rollback_to,
};

use crate::error::{Error, Result};
use crate::storage::memory::MemoryBackend;
use crate::storage::{StorageBackend, StorageWriteBatch};
use crate::types::{BlockNumber, RowMap, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub const DEX_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user String,
        side String,
        amount Float64,
        price Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
    LANGUAGE lua
    PROCESS $$
        local trade_pnl = 0
        if row.side == "buy" then
            state.quantity = state.quantity + row.amount
            state.cost_basis = state.cost_basis + row.amount * row.price
        elseif row.side == "sell" then
            local avg_cost = 0
            if state.quantity ~= 0 then avg_cost = state.cost_basis / state.quantity end
            state.quantity = state.quantity - row.amount
            state.cost_basis = state.cost_basis - row.amount * avg_cost
            trade_pnl = row.amount * (row.price - avg_cost)
        end
        emit { trade_pnl = trade_pnl, position_size = state.quantity }
    $$;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl) AS total_pnl,
        last(position_size) AS current_position,
        count() AS trade_count
    FROM pnl
    GROUP BY user;
"#;

pub const SIMPLE_SCHEMA: &str = r#"
    CREATE TABLE swaps (
        pool String,
        amount Float64
    );

    CREATE MATERIALIZED VIEW pool_volume AS
    SELECT
        pool,
        sum(amount) AS total_volume,
        count() AS swap_count
    FROM swaps
    GROUP BY pool;
"#;

pub const EXTERNAL_PNL_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user String,
        side String,
        amount Float64,
        price Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
    LANGUAGE EXTERNAL;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl) AS total_pnl,
        last(position_size) AS current_position,
        count() AS trade_count
    FROM pnl
    GROUP BY user;
"#;

pub fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> RowMap {
    HashMap::from([
        ("user".to_string(), Value::String(user.to_string())),
        ("side".to_string(), Value::String(side.to_string())),
        ("amount".to_string(), Value::Float64(amount)),
        ("price".to_string(), Value::Float64(price)),
    ])
}

pub fn make_swap(pool: &str, amount: f64) -> RowMap {
    HashMap::from([
        ("pool".to_string(), Value::String(pool.to_string())),
        ("amount".to_string(), Value::Float64(amount)),
    ])
}

/// `MemoryBackend` with injected failures: raw-row reads fail while
/// `fail_reads` is set, syncs while `fail_syncs` is set, and commits fail once
/// `commits_before_failure` commits have passed. It keeps the default bounded
/// raw-row read; `blocks_read` counts the blocks of raw rows that it returned,
/// and `syncs` the syncs that succeeded.
pub struct FailingStorage {
    inner: MemoryBackend,
    pub fail_reads: AtomicBool,
    pub fail_syncs: AtomicBool,
    pub commits_before_failure: AtomicU64,
    pub blocks_read: AtomicU64,
    pub syncs: AtomicU64,
}

impl Default for FailingStorage {
    fn default() -> Self {
        Self {
            inner: MemoryBackend::new(),
            fail_reads: AtomicBool::new(false),
            fail_syncs: AtomicBool::new(false),
            commits_before_failure: AtomicU64::new(u64::MAX),
            blocks_read: AtomicU64::new(0),
            syncs: AtomicU64::new(0),
        }
    }
}

impl StorageBackend for FailingStorage {
    fn put_raw_rows(&self, t: &str, b: BlockNumber, d: &[u8]) -> Result<()> { self.inner.put_raw_rows(t, b, d) }
    fn get_raw_rows(&self, t: &str, f: BlockNumber, u: BlockNumber) -> Result<Vec<(BlockNumber, Vec<u8>)>> {
        if self.fail_reads.load(Ordering::SeqCst) {
            return Err(Error::Storage("injected read failure".into()));
        }
        let rows = self.inner.get_raw_rows(t, f, u)?;
        self.blocks_read.fetch_add(rows.len() as u64, Ordering::SeqCst);
        Ok(rows)
    }
    fn delete_raw_rows_after(&self, t: &str, a: BlockNumber) -> Result<()> { self.inner.delete_raw_rows_after(t, a) }
    fn take_raw_rows_after(&self, t: &str, a: BlockNumber) -> Result<Vec<(BlockNumber, Vec<u8>)>> { self.inner.take_raw_rows_after(t, a) }
    fn put_reducer_state(&self, r: &str, k: &[u8], b: BlockNumber, s: &[u8]) -> Result<()> { self.inner.put_reducer_state(r, k, b, s) }
    fn get_reducer_state(&self, r: &str, k: &[u8], b: BlockNumber) -> Result<Option<Vec<u8>>> { self.inner.get_reducer_state(r, k, b) }
    fn get_reducer_state_at_or_before(&self, r: &str, k: &[u8], b: BlockNumber) -> Result<Option<(BlockNumber, Vec<u8>)>> { self.inner.get_reducer_state_at_or_before(r, k, b) }
    fn delete_reducer_states_after(&self, r: &str, k: &[u8], a: BlockNumber) -> Result<()> { self.inner.delete_reducer_states_after(r, k, a) }
    fn get_reducer_finalized(&self, r: &str, k: &[u8]) -> Result<Option<Vec<u8>>> { self.inner.get_reducer_finalized(r, k) }
    fn set_reducer_finalized(&self, r: &str, k: &[u8], s: &[u8]) -> Result<()> { self.inner.set_reducer_finalized(r, k, s) }
    fn delete_reducer_states_up_to(&self, r: &str, k: &[u8], u: BlockNumber) -> Result<()> { self.inner.delete_reducer_states_up_to(r, k, u) }
    fn put_mv_state(&self, v: &str, k: &[u8], s: &[u8]) -> Result<()> { self.inner.put_mv_state(v, k, s) }
    fn get_mv_state(&self, v: &str, k: &[u8]) -> Result<Option<Vec<u8>>> { self.inner.get_mv_state(v, k) }
    fn delete_mv_state(&self, v: &str, k: &[u8]) -> Result<()> { self.inner.delete_mv_state(v, k) }
    fn list_mv_group_keys(&self, v: &str) -> Result<Vec<Vec<u8>>> { self.inner.list_mv_group_keys(v) }
    fn put_meta(&self, k: &str, v: &[u8]) -> Result<()> { self.inner.put_meta(k, v) }
    fn get_meta(&self, k: &str) -> Result<Option<Vec<u8>>> { self.inner.get_meta(k) }
    fn get_block_hashes(&self) -> Result<Vec<(BlockNumber, String)>> { self.inner.get_block_hashes() }
    fn journal(&self) -> Result<Vec<crate::storage::journal::JournalPart>> { self.inner.journal() }
    fn list_reducer_group_keys(&self, r: &str) -> Result<Vec<Vec<u8>>> { self.inner.list_reducer_group_keys(r) }
    fn commit(&self, batch: &StorageWriteBatch) -> Result<()> {
        let left = self.commits_before_failure.load(Ordering::SeqCst);
        if left == 0 {
            return Err(Error::Storage("injected commit failure".into()));
        }
        if left != u64::MAX {
            self.commits_before_failure.store(left - 1, Ordering::SeqCst);
        }
        self.inner.commit(batch)
    }
    fn sync(&self) -> Result<()> {
        if self.fail_syncs.load(Ordering::SeqCst) {
            return Err(Error::Storage("injected sync failure".into()));
        }
        self.syncs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
