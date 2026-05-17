//! Test helpers for driving `Settle` from tests and benchmarks. Wrap
//! `ingest()` and `handle_fork()` so call sites don't have to construct
//! `IngestInput`/`BlockCursor` by hand or invent block hashes.
//!
//! These are public so integration tests under `tests/` and benchmarks
//! under `benches/` (each compiled as its own crate) can reuse them.

use crate::db::{ForkResult, IngestInput, Settle};
use crate::error::Result;
use crate::types::{BlockCursor, BlockNumber, ChangeBatch, RowMap, Value};
use std::collections::{BTreeSet, HashMap};

/// Deterministic block hash. Tests that drive `Settle` through `ingest()`
/// and `handle_fork()` need a stable hash for every block they touch.
pub fn block_hash(n: BlockNumber) -> String {
    format!("0x{n:016x}")
}

/// Forwards to `Settle::ingest` and auto-acks the resulting batch (if any).
/// Tests that intentionally exercise pending-ack semantics should call
/// `Settle::ingest` + `Settle::ack` manually instead.
pub fn ingest_input(
    db: &mut Settle,
    input: IngestInput,
) -> Result<Option<ChangeBatch>> {
    let batch = db.ingest(input)?;
    if let Some(ref b) = batch {
        db.ack(b.sequence)?;
    }
    Ok(batch)
}

/// Forwards to `Settle::handle_fork` and auto-acks the resulting batch (if any).
pub fn handle_fork(db: &mut Settle, rollback_chain: Vec<BlockCursor>) -> Result<crate::db::ForkResult> {
    let result = db.handle_fork(rollback_chain)?;
    if let Some(ref b) = result.batch {
        db.ack(b.sequence)?;
    }
    Ok(result)
}

pub fn cursor(n: BlockNumber) -> BlockCursor {
    BlockCursor {
        number: n,
        hash: block_hash(n),
    }
}

/// Single-table, single-block ingest. The block stays unfinalized — its hash
/// is stored so a later `rollback_to(block)` can target it.
///
/// Auto-acks the produced batch so subsequent reads of `latest_block` etc.
/// reflect the just-ingested state. Tests that need to inspect uncommitted
/// state should call `db.ingest` directly.
pub fn ingest_one(
    db: &mut Settle,
    table: &str,
    block: BlockNumber,
    rows: Vec<RowMap>,
) -> Result<Option<ChangeBatch>> {
    let finalized = db.finalized_block();
    ingest_with_finalized(db, vec![(table.to_string(), block, rows)], finalized)
}

/// Multi-block, multi-table ingest. All ingested blocks stay unfinalized
/// relative to the engine's current finalized head, so `rollback_to` can
/// target any of them. Auto-acks.
pub fn ingest_blocks(
    db: &mut Settle,
    items: Vec<(String, BlockNumber, Vec<RowMap>)>,
) -> Result<Option<ChangeBatch>> {
    let finalized = db.finalized_block();
    ingest_with_finalized(db, items, finalized)
}

/// Multi-block ingest that lets the caller pick the finalized head. Auto-acks.
pub fn ingest_with_finalized(
    db: &mut Settle,
    items: Vec<(String, BlockNumber, Vec<RowMap>)>,
    finalized: BlockNumber,
) -> Result<Option<ChangeBatch>> {
    let batch = ingest_with_finalized_no_ack(db, items, finalized)?;
    if let Some(ref b) = batch {
        db.ack(b.sequence)?;
    }
    Ok(batch)
}

/// Same as `ingest_with_finalized` but does NOT call `ack`. Use when the test
/// needs to inspect/exercise pending-ack semantics (drop without ack,
/// duplicate-ingest-while-pending, etc.).
pub fn ingest_with_finalized_no_ack(
    db: &mut Settle,
    items: Vec<(String, BlockNumber, Vec<RowMap>)>,
    finalized: BlockNumber,
) -> Result<Option<ChangeBatch>> {
    let mut data: HashMap<String, Vec<RowMap>> = HashMap::new();
    let mut blocks: BTreeSet<BlockNumber> = BTreeSet::new();
    for (table, block, mut rows) in items {
        for row in &mut rows {
            row.insert("block_number".to_string(), Value::UInt64(block));
        }
        blocks.insert(block);
        data.entry(table).or_default().extend(rows);
    }
    let rollback_chain: Vec<BlockCursor> = blocks
        .into_iter()
        .filter(|b| *b > finalized)
        .map(cursor)
        .collect();
    db.ingest(IngestInput {
        data,
        rollback_chain,
        finalized_head: cursor(finalized),
    })
}

/// Roll the engine back to `fork_point`. Auto-acks any produced batch so
/// subsequent reads see the rolled-back state.
///
/// For `fork_point > 0`, calls `handle_fork(vec![cursor(fork_point)])` —
/// the fork point must be a block whose hash was previously stored by one
/// of the `ingest_*` helpers above (deterministic `block_hash(n)`).
///
/// For `fork_point == 0`, "drop everything" semantics — handled via an
/// `ingest()` call with no data and `finalized_head = cursor(0)`. The
/// engine's fork detection then sees no common ancestor and falls back
/// to a full rollback. `handle_fork` itself errors on this case.
pub fn rollback_to(db: &mut Settle, fork_point: BlockNumber) -> Result<ForkResult> {
    if fork_point == 0 {
        let batch = db.ingest(IngestInput {
            data: HashMap::new(),
            rollback_chain: vec![],
            finalized_head: cursor(0),
        })?;
        if let Some(ref b) = batch {
            db.ack(b.sequence)?;
        }
        return Ok(ForkResult {
            cursor: cursor(0),
            batch,
        });
    }
    let result = db.handle_fork(vec![cursor(fork_point)])?;
    if let Some(ref b) = result.batch {
        db.ack(b.sequence)?;
    }
    Ok(result)
}
