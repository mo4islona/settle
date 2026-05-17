use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
// `web_time::Instant` is `std::time::Instant` on native and a JS-Date-backed
// shim on `wasm32-unknown-unknown` (where the real `Instant::now()` panics).
use web_time::Instant;

use crate::change::ChangeBuffer;
use crate::engine::dag::SettleEngine;
use crate::error::{Error, Result};
use crate::schema::parser::parse_schema;
use crate::storage::memory::MemoryBackend;
#[cfg(feature = "rocksdb")]
use crate::storage::rocks::{RocksDbBackend, RocksDbConfig};
use crate::storage::{StorageBackend, StorageWriteBatch};
use crate::types::{BlockCursor, BlockNumber, ChangeBatch, ChangeRecord, PerfNode, PerfNodeKind, RowMap, Value};

/// Configuration for opening a Settle instance.
#[non_exhaustive]
pub struct Config {
    /// SQL schema definition string.
    pub schema: String,
    /// Maximum number of pending change records before backpressure.
    pub max_buffer_size: usize,
    /// Path to RocksDB data directory. When set, data is persisted to disk.
    /// When None, uses in-memory storage (data lost on drop).
    pub data_dir: Option<String>,
    /// Explicit storage backend override. Takes precedence over data_dir.
    pub storage: Option<Arc<dyn StorageBackend>>,
    /// Compression algorithm for RocksDB: "none", "snappy" (default), "zstd", "lz4".
    pub compression: Option<String>,
    /// Disable RocksDB automatic background compactions.
    pub disable_compaction: bool,
    /// Block cache size in bytes. None = RocksDB default, 0 = disable.
    pub cache_size: Option<usize>,
    /// How long an unacked pending batch may live before `ingest()` logs a
    /// warning when blocked by it. The block itself happens immediately —
    /// the threshold only controls log noise. `handle_fork()` does not log
    /// (it rejects with `PendingAck` straight away).
    pub ack_warning_threshold: Duration,
}

impl Config {
    /// Create a config with in-memory storage (no persistence).
    /// Suitable for tests and benchmarks.
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            max_buffer_size: 10_000,
            data_dir: None,
            storage: None,
            compression: None,
            disable_compaction: false,
            cache_size: None,
            ack_warning_threshold: Duration::from_secs(10),
        }
    }

    /// Create a config with RocksDB persistence at the given path.
    pub fn with_data_dir(schema: impl Into<String>, data_dir: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            max_buffer_size: 10_000,
            data_dir: Some(data_dir.into()),
            storage: None,
            compression: None,
            disable_compaction: false,
            cache_size: None,
            ack_warning_threshold: Duration::from_secs(10),
        }
    }

    /// Threshold after which a still-pending unacked batch causes a warning
    /// log on each subsequent `ingest()` / `handle_fork()` call (which all
    /// return `Err(PendingAck)` immediately regardless).
    pub fn ack_warning_threshold(mut self, threshold: Duration) -> Self {
        self.ack_warning_threshold = threshold;
        self
    }

    pub fn max_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    pub fn storage(mut self, storage: Arc<dyn StorageBackend>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set the on-disk RocksDB directory. Pass [`MEMORY`] (`":memory:"`) — or
    /// just call `Config::new(schema)` without this — to keep everything in
    /// memory. Same convention as SQLite.
    pub fn data_dir(mut self, path: impl Into<String>) -> Self {
        let p = path.into();
        self.data_dir = if p == MEMORY { None } else { Some(p) };
        self
    }

    /// RocksDB compression algorithm. Accepts
    /// `"none" | "snappy" | "zstd" | "lz4"`.
    pub fn compression(mut self, algo: impl Into<String>) -> Self {
        self.compression = Some(algo.into());
        self
    }

    /// Disable RocksDB background compaction.
    pub fn disable_compaction(mut self, value: bool) -> Self {
        self.disable_compaction = value;
        self
    }

    /// RocksDB block-cache size in bytes.
    pub fn cache_size(mut self, bytes: usize) -> Self {
        self.cache_size = Some(bytes);
        self
    }
}

/// Sentinel value (SQLite convention) — passing this as `data_dir` is
/// equivalent to leaving it unset, i.e. open the database in memory.
pub const MEMORY: &str = ":memory:";

/// Input for the atomic `ingest()` method.
pub struct IngestInput {
    /// Table name → rows. Each row must contain `block_number`.
    pub data: std::collections::HashMap<String, Vec<RowMap>>,
    /// Unfinalized blocks with hashes (from ctx.state.rollbackChain).
    pub rollback_chain: Vec<BlockCursor>,
    /// Finalized head cursor (from ctx.head.finalized). Required.
    pub finalized_head: BlockCursor,
}

/// Result of `handle_fork()`.
pub struct ForkResult {
    /// The block to resume ingestion from (highest common ancestor).
    pub cursor: BlockCursor,
    /// Compensating change batch (rollback changes), if any state was rolled back.
    pub batch: Option<ChangeBatch>,
}

// Metadata keys for persistence
const META_LATEST_BLOCK: &str = "latest_block";
const META_FINALIZED_BLOCK: &str = "finalized_block";
const META_BLOCK_HASHES: &str = "block_hashes";
const META_NEXT_SEQUENCE: &str = "next_sequence";

/// State stashed between a successful `ingest()` / `handle_fork()` and the
/// caller's `ack()`. The on-disk view is one batch behind the engine
/// in-memory view; this struct holds the write batch that advances disk to
/// in-memory when `ack()` succeeds.
///
/// `pending` is only set for the data path (where the caller received a
/// `ChangeBatch` and owns the apply→ack handshake). On the heartbeat path
/// (empty-batch immediate commit) a commit failure poisons the instance
/// instead — see [`Settle::poisoned`].
struct PendingAck {
    sequence: u64,
    write_batch: StorageWriteBatch,
    since: Instant,
}

/// Top-level Settle API.
///
/// Provides a simple interface for ingesting blockchain data,
/// handling rollbacks, and producing change batches for downstream targets.
pub struct Settle {
    engine: SettleEngine,
    buffer: ChangeBuffer,
    storage: Arc<dyn StorageBackend>,
    pending: Option<PendingAck>,
    ack_warning_threshold: Duration,
    /// Set when a non-recoverable commit (heartbeat immediate-commit) fails.
    /// In that case the engine's in-memory state has already mutated past
    /// disk (e.g. `engine.finalize` pruned `block_snapshots`) and naively
    /// retrying would re-run `finalize` against the pruned state, writing
    /// an *empty* batch and silently leaving CF_REDUCER_FIN stale. Every
    /// public mutating method short-circuits with `Error::Poisoned` once
    /// this is set; the only recovery is `drop` + reopen, which rebuilds
    /// in-memory state from disk via `replay_unfinalized`.
    poisoned: Option<String>,
    /// Names of reducers whose runtime has been explicitly installed via
    /// `register_reducer_callback`. Enforces the strict "one callback per
    /// name, drop+reopen to change" contract.
    runtimes_attached: std::collections::HashSet<String>,
}

impl Settle {
    /// Open a Settle instance with the given configuration.
    /// Parses and validates the schema at open time.
    pub fn open(config: Config) -> Result<Self> {
        let schema = parse_schema(&config.schema)?;

        let storage: Arc<dyn StorageBackend> = if let Some(s) = config.storage {
            s
        } else if let Some(ref _dir) = config.data_dir {
            #[cfg(feature = "rocksdb")]
            {
                let rocks_config = RocksDbConfig {
                    compression: config.compression.clone(),
                    disable_compaction: config.disable_compaction,
                    cache_size: config.cache_size,
                };
                Arc::new(RocksDbBackend::open_with_config(_dir, &rocks_config)?)
            }
            #[cfg(not(feature = "rocksdb"))]
            {
                return Err(Error::InvalidOperation(
                    "RocksDB requires the 'rocksdb' feature".into(),
                ));
            }
        } else {
            Arc::new(MemoryBackend::new())
        };

        let mut engine = SettleEngine::new(&schema, storage.clone());

        // Restore persisted state
        if let Some(bytes) = storage.get_meta(META_LATEST_BLOCK)? {
            let block = u64::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| Error::Storage("corrupt latest_block metadata".into()))?,
            );
            engine.set_latest_block(block);
        }
        if let Some(bytes) = storage.get_meta(META_FINALIZED_BLOCK)? {
            let block = u64::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| Error::Storage("corrupt finalized_block metadata".into()))?,
            );
            engine.set_finalized_block(block);
        }
        if let Some(bytes) = storage.get_meta(META_BLOCK_HASHES)? {
            let hashes: BTreeMap<BlockNumber, String> = serde_json::from_slice(&bytes)
                .map_err(|e| Error::Storage(format!("corrupt block_hashes metadata: {e}")))?;
            engine.restore_block_hashes(hashes);
        }

        // Replay unfinalized blocks to rebuild reducer/MV in-memory state
        let finalized = engine.finalized_block();
        let latest = engine.latest_block();
        if latest > finalized {
            engine.replay_unfinalized(finalized + 1, latest)?;
        }

        let mut buffer = ChangeBuffer::new(config.max_buffer_size);
        // Restore monotonic sequence across restart. Default 1 on fresh DB.
        if let Some(bytes) = storage.get_meta(META_NEXT_SEQUENCE)? {
            let seq = u64::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| Error::Storage("corrupt next_sequence metadata".into()))?,
            );
            buffer.set_next_sequence(seq);
        }

        Ok(Self {
            engine,
            buffer,
            storage,
            pending: None,
            ack_warning_threshold: config.ack_warning_threshold,
            poisoned: None,
            runtimes_attached: std::collections::HashSet::new(),
        })
    }

    /// Returns true if a previously-produced `ChangeBatch` is still awaiting
    /// `ack()`. While true, mutating APIs (`ingest`, `handle_fork`,
    /// `register_reducer`, `register_reducer_callback`, `replay_reducer`) return
    /// `Err(PendingAck)`. Use to surface state in dashboards / readiness probes.
    pub fn is_awaiting_ack(&self) -> bool {
        self.pending.is_some()
    }

    /// True if a previous immediate-commit failure has poisoned this
    /// instance. The only recovery is `drop` + reopen.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    /// Early gate for every mutating API: if a prior immediate-commit
    /// failure mutated in-memory state past disk, refuse further work
    /// instead of silently producing stale writes.
    fn guard_not_poisoned(&self) -> Result<()> {
        if let Some(reason) = &self.poisoned {
            return Err(Error::Poisoned(reason.clone()));
        }
        Ok(())
    }

    /// Replace the runtime for a named reducer (for External/FnReducer injection).
    /// Replays unfinalized blocks so the reducer catches up with current state.
    ///
    /// Returns `Err(PendingAck)` if an unacked batch is in flight — the engine
    /// in-memory state is ahead of disk, so a replay against the on-disk
    /// finalized state would rebuild inconsistent reducer state.
    /// Attach a reducer runtime to an existing reducer declared in SQL
    /// with `LANGUAGE EXTERNAL`. Replays unfinalized blocks so the runtime
    /// catches up with current state.
    ///
    /// **Strict**: errors if no reducer named `name` exists, AND errors if
    /// a runtime is already attached for that name. To change a runtime,
    /// drop and reopen the instance.
    pub fn register_reducer_callback(
        &mut self,
        name: &str,
        runtime: Box<dyn crate::reducer_runtime::ReducerRuntime>,
    ) -> Result<()> {
        self.guard_not_poisoned()?;
        self.guard_no_pending()?;
        if !self.engine.has_reducer(name) {
            return Err(Error::InvalidOperation(format!(
                "register_reducer_callback: unknown reducer '{name}' — \
                 use register_reducer to create a brand-new reducer"
            )));
        }
        // The reducer must be declared `LANGUAGE EXTERNAL`. Attaching a
        // callback to a Lua/EventRules reducer silently never invokes it
        // (those bodies have their own embedded runtime) — fail loud
        // rather than let the caller think a callback is wired up.
        if !self.engine.reducer_is_external(name) {
            return Err(Error::InvalidOperation(format!(
                "register_reducer_callback: reducer '{name}' is not declared \
                 LANGUAGE EXTERNAL — callbacks can only be attached to external reducers"
            )));
        }
        // Strict: a runtime can only be attached once per name. To change
        // a runtime, drop and reopen the Settle instance.
        if self.runtimes_attached.contains(name) {
            return Err(Error::InvalidOperation(format!(
                "register_reducer_callback: runtime for '{name}' is already attached — \
                 drop and reopen the instance to change it"
            )));
        }
        self.engine.set_reducer_runtime(name, runtime);
        self.runtimes_attached.insert(name.to_string());

        // Replay only this reducer and its downstream MVs. Reset their
        // in-memory state to the finalized baseline first — otherwise the
        // replay applies on top of state already populated by prior ingests,
        // doubling every emit / aggregate contribution.
        let finalized = self.engine.finalized_block();
        let latest = self.engine.latest_block();
        if latest > finalized {
            self.engine.reset_reducer_branch_for_replay(name, finalized)?;
            self.engine
                .replay_unfinalized_for(finalized + 1, latest, name)?;
        }
        Ok(())
    }

    /// Register a *new* external reducer definition. The reducer is added
    /// to the engine's pipeline and unfinalized blocks are replayed so its
    /// in-memory state catches up with disk.
    ///
    /// Returns `Err(InvalidOperation)` if a reducer with this name already
    /// exists — call `replay_reducer(name)` or `register_reducer_callback(name, …)`
    /// instead to re-attach / re-replay an existing slot. Silently appending
    /// a duplicate `PipelineNode::Reducer` would double-apply state on every
    /// subsequent ingest.
    ///
    /// Returns `Err(PendingAck)` if an unacked batch is in flight; returns
    /// `Err(Poisoned)` if a previous commit failure poisoned the instance.
    pub fn register_reducer(&mut self, def: crate::schema::ast::ReducerDef) -> Result<()> {
        self.guard_not_poisoned()?;
        self.guard_no_pending()?;
        let name = def.name.clone();
        if self.engine.has_reducer(&name) {
            return Err(Error::InvalidOperation(format!(
                "register_reducer: reducer '{name}' already exists — \
                 use `replay_reducer` or `register_reducer_callback` to re-attach"
            )));
        }
        self.engine.add_reducer(def, self.storage.clone())?;

        // Newly-added reducer starts with empty state — replay rebuilds it
        // from the unfinalized range without any double-counting risk.
        let finalized = self.engine.finalized_block();
        let latest = self.engine.latest_block();
        if latest > finalized {
            self.engine
                .replay_unfinalized_for(finalized + 1, latest, &name)?;
        }
        Ok(())
    }

    /// Replay unfinalized blocks for a specific reducer and its downstream MVs.
    /// Used after installing an external reducer's JS callback.
    ///
    /// Returns `Err(PendingAck)` if an unacked batch is in flight.
    pub fn replay_reducer(&mut self, name: &str) -> Result<()> {
        self.guard_not_poisoned()?;
        self.guard_no_pending()?;
        let finalized = self.engine.finalized_block();
        let latest = self.engine.latest_block();
        if latest > finalized {
            // Reset reducer + downstream MVs to finalized baseline BEFORE
            // replaying. Without this, a replay called after the reducer
            // already processed the unfinalized range would double every
            // emit and aggregate contribution.
            self.engine.reset_reducer_branch_for_replay(name, finalized)?;
            self.engine
                .replay_unfinalized_for(finalized + 1, latest, name)?;
        }
        Ok(())
    }

    /// Check if a reducer with the given name already exists in the engine.
    pub fn has_reducer(&self, name: &str) -> bool {
        self.engine.has_reducer(name)
    }

    /// Whether the named reducer is declared `LANGUAGE EXTERNAL` — only
    /// these can have a host-language callback attached via
    /// `register_reducer_callback`. Returns false for Lua / EventRules
    /// reducers and for unknown names.
    pub fn reducer_is_external(&self, name: &str) -> bool {
        self.engine.reducer_is_external(name)
    }

    /// Acknowledge a previously returned `ChangeBatch` and durably commit the
    /// pending state.
    ///
    /// Behaviour:
    /// - `pending == None` → `Ok(())` (idempotent — covers double-ack after
    ///   success, stale ack on startup before any new ingest).
    /// - `pending == Some(p)`, `sequence != p.sequence` →
    ///   `Err(WrongAckSequence)`. Pending is unchanged; this signals a
    ///   protocol bug on the caller side.
    /// - `pending == Some(p)`, matches: `storage.commit(&p.write_batch)`. On
    ///   `Err`, pending is preserved — caller MUST retry by calling
    ///   `ack(sequence)` again with the SAME sequence (NOT `ingest()`,
    ///   which would return `Err(PendingAck)`). On `Ok`, `last_committed_meta`
    ///   is refreshed and pending is cleared.
    pub fn ack(&mut self, sequence: u64) -> Result<()> {
        self.guard_not_poisoned()?;
        let Some(p) = self.pending.as_ref() else {
            return Ok(());
        };
        if p.sequence != sequence {
            return Err(Error::WrongAckSequence {
                expected: p.sequence,
                got: sequence,
            });
        }
        // CRITICAL: only `take()` after a successful commit. Otherwise a
        // commit failure would silently drop the write_batch and leave engine
        // state ahead of disk with no path to recover.
        self.storage.commit(&p.write_batch)?;
        self.pending = None;
        Ok(())
    }

    /// Number of pending (unflushed) change records inside the buffer.
    ///
    /// Note: this is the buffer's record count; it does NOT reflect the
    /// `pending` ack slot. Use `is_awaiting_ack()` for that.
    pub fn pending_count(&self) -> usize {
        self.buffer.pending_count()
    }

    /// Whether buffer backpressure should be applied.
    pub fn is_backpressured(&self) -> bool {
        self.buffer.is_full()
    }

    /// Current logical latest processed block (engine in-memory view). If a
    /// `ChangeBatch` is awaiting `ack()`, this reflects the not-yet-durable
    /// state — use `is_awaiting_ack()` to distinguish.
    pub fn latest_block(&self) -> BlockNumber {
        self.engine.latest_block()
    }

    pub fn latest_cursor(&self) -> Option<BlockCursor> {
        self.engine.latest_cursor()
    }

    pub fn finalized_block(&self) -> BlockNumber {
        self.engine.finalized_block()
    }

    pub fn finalized_cursor(&self) -> Option<BlockCursor> {
        self.engine.finalized_cursor()
    }

    /// Find the highest block in `previous_blocks` whose hash matches our
    /// stored hash — including hashes added via `set_rollback_chain` for
    /// blocks we haven't ingested data for (Solana-style gappy chains).
    ///
    /// May return a cursor with `number > latest_block`. This is informational
    /// only: `handle_fork` clamps the resolved cursor to `latest_block`
    /// internally to avoid advancing past blocks with no data. Callers that
    /// want the "what would handle_fork pick" answer should consume the
    /// result of `handle_fork` directly rather than pre-flighting through
    /// this method.
    pub fn resolve_fork_cursor(
        &self,
        previous_blocks: &[(BlockNumber, &str)],
    ) -> Option<BlockCursor> {
        self.engine.resolve_fork_cursor(previous_blocks)
    }

    /// Atomically handle a fork (409 from Portal).
    ///
    /// Finds the highest common ancestor in `rollback_chain`, rolls back all
    /// state after that point, and (a) if no compensating records resulted —
    /// commits metadata immediately and returns `batch: None`, or (b)
    /// otherwise stashes the write batch as `pending` and returns the
    /// compensating `ChangeBatch` to the caller. Caller MUST call
    /// `ack(batch.sequence)` after applying the batch to durably commit.
    ///
    /// Returns `Err(PendingAck)` if an unacked batch is already in flight —
    /// caller must ack it first, then retry. Returns `Err(InvalidOperation)`
    /// if `rollback_chain` is empty or no common ancestor exists.
    pub fn handle_fork(&mut self, mut rollback_chain: Vec<BlockCursor>) -> Result<ForkResult> {
        // 0. Poison gate (see `ingest` for rationale).
        self.guard_not_poisoned()?;
        // 1. Pending guard — before validation, so caller gets a clean
        //    "ack first" signal instead of "bad input" derived from
        //    uncommitted engine state.
        self.guard_no_pending()?;

        // 2. Validation: empty chain has no resolvable common ancestor.
        if rollback_chain.is_empty() {
            return Err(Error::InvalidOperation(
                "rollback_chain must not be empty".into(),
            ));
        }

        // Sort DESC to ensure we find the HIGHEST common ancestor first,
        // matching the same contract as ingest()'s rollback_chain handling.
        rollback_chain.sort_unstable_by_key(|c| std::cmp::Reverse(c.number));

        let previous_blocks: Vec<(BlockNumber, &str)> = rollback_chain
            .iter()
            .map(|c| (c.number, c.hash.as_str()))
            .collect();

        // `resolve_fork_cursor_bounded` ignores matches with number > latest_block.
        // The unbounded `resolve_fork_cursor` is fine for the read API (caller
        // may have stored future hashes intentionally via rollback_chain), but
        // for the rollback path it would silently advance the cursor onto a
        // block we have no data for and lose blocks in between.
        let cursor = self
            .engine
            .resolve_fork_cursor_bounded(&previous_blocks)
            .ok_or_else(|| {
                Error::InvalidOperation(
                    "Fork too deep: no common ancestor found in block hashes".into(),
                )
            })?;

        let mut write_batch = StorageWriteBatch::new();
        let changes = self
            .engine
            .rollback_to_batch(cursor.number, &mut write_batch)?;

        // Store the new rollback chain and persist atomically
        let chain: Vec<(BlockNumber, String)> = rollback_chain
            .iter()
            .map(|c| (c.number, c.hash.clone()))
            .collect();
        self.engine.set_rollback_chain(&chain);

        self.append_meta_to_batch(&mut write_batch)?;

        self.buffer.push(
            changes,
            self.engine.finalized_cursor(),
            self.engine.latest_cursor(),
            vec![],
        );
        self.buffer
            .set_heads(self.engine.finalized_cursor(), self.engine.latest_cursor());

        let batch_opt = self.buffer.flush();
        // META_NEXT_SEQUENCE is written AFTER flush so the persisted value
        // reflects what `buffer.next_sequence` will be on the next call.
        write_batch.put_meta(
            META_NEXT_SEQUENCE,
            &self.buffer.next_sequence().to_be_bytes(),
        );

        match batch_opt {
            None => {
                // Heartbeat-style fork. Commit immediately; on failure
                // poison the instance (engine state already rolled-back
                // past disk — retry would re-encode against the mutated
                // state and could silently land inconsistent writes).
                if let Err(e) = self.storage.commit(&write_batch) {
                    self.poisoned =
                        Some(format!("heartbeat handle_fork commit failed: {e}"));
                    return Err(e);
                }
                Ok(ForkResult { cursor, batch: None })
            }
            Some(batch) => {
                self.pending = Some(PendingAck {
                    sequence: batch.sequence,
                    write_batch,
                    since: Instant::now(),
                });
                Ok(ForkResult {
                    cursor,
                    batch: Some(batch),
                })
            }
        }
    }

    /// Atomic ingest: process all tables, store rollback chain, finalize.
    ///
    /// Returns `Ok(Some(batch))` and stashes the underlying write batch in
    /// `pending` — caller MUST call `ack(batch.sequence)` after successfully
    /// applying the batch to durably commit. Returns `Ok(None)` for heartbeat
    /// ingests (no records produced after merge) where the disk write is
    /// committed immediately and nothing requires ack.
    ///
    /// Returns `Err(PendingAck)` if a previous batch is still awaiting ack —
    /// caller must `ack(seq)` it first (or `drop` and reopen). Returns
    /// `Err(InvalidOperation)` if `finalized_head` regresses below the
    /// currently committed finalized block (finality is monotonic; going
    /// above `latest_block` is allowed for gappy chains like Solana).
    ///
    /// Each row in `data` must contain a `block_number` field (UInt64).
    pub fn ingest(&mut self, input: IngestInput) -> Result<Option<ChangeBatch>> {
        // 0. Poison gate. Any prior immediate-commit failure made in-memory
        //    state diverge from disk; reject all further work so the caller
        //    drops and reopens rather than silently producing stale writes.
        self.guard_not_poisoned()?;

        // 1. Pending guard — before any read of engine state, since engine
        //    may be ahead of disk when a pending exists. Reading
        //    `engine.finalized_block()` for validation would compare against
        //    the uncommitted finalize and reject valid inputs.
        if let Some(p) = &self.pending {
            let since = p.since.elapsed();
            if since > self.ack_warning_threshold {
                tracing::warn!(
                    sequence = p.sequence,
                    since = ?since,
                    "ack pending, ingest blocked"
                );
            }
            return Err(Error::PendingAck {
                sequence: p.sequence,
                since,
            });
        }

        // 2. Validation: finality must not regress. Going above current
        //    latest is allowed — some chains (Solana) skip block numbers and
        //    finality may come from an out-of-band source.
        let current_finalized = self.engine.finalized_block();
        if input.finalized_head.number < current_finalized {
            return Err(Error::InvalidOperation(format!(
                "finalized_head.number ({}) < current finalized_block ({}); finality must be monotonic",
                input.finalized_head.number, current_finalized
            )));
        }

        // Single WriteBatch for all storage writes (raw rows + finalize + meta)
        let mut write_batch = StorageWriteBatch::new();

        // Collect changes locally — only push to buffer on success to avoid
        // partial changes leaking into downstream output on failure.
        let mut pending_changes: Vec<(Vec<ChangeRecord>, Vec<PerfNode>)> = Vec::new();

        // 0. Detect fork: compare new chain against stored block_hashes and rollback
        //    if our latest block is no longer on the canonical chain.
        //    Sort rollback_chain DESC (newest first) so resolve_fork_cursor returns
        //    the HIGHEST common ancestor — preventing catastrophic deep rollbacks
        //    when the caller sends blocks in ascending order.
        //
        //    IMPORTANT: skip fork detection when the chain is advancing (rollbackChain
        //    or finalizedHead contains blocks above current_latest). In that case, the
        //    caller is providing new unseen blocks, not a rollback signal. External
        //    fork handling (e.g. 409 in pipes-sdk) runs before ingest() is called.
        let recovery_block = {
            let current_latest = self.engine.latest_block();
            if current_latest > 0 {
                let mut sorted_chain: Vec<_> = input.rollback_chain.iter().collect();
                sorted_chain.sort_unstable_by_key(|c| std::cmp::Reverse(c.number));

                // If any block in rollbackChain or finalizedHead is ABOVE current_latest,
                // the caller is advancing (normal progress). Resolve-fork-cursor would
                // fall through to an old finalized anchor and trigger a spurious rollback.
                let advancing = sorted_chain
                    .first()
                    .map(|c| c.number > current_latest)
                    .unwrap_or(false)
                    || input.finalized_head.number > current_latest;

                if !advancing {
                    let new_chain: Vec<(BlockNumber, &str)> = sorted_chain
                        .iter()
                        .map(|c| (c.number, c.hash.as_str()))
                        .chain(std::iter::once((
                            input.finalized_head.number,
                            input.finalized_head.hash.as_str(),
                        )))
                        .collect();

                    // Use `_bounded` so a stale future hash can't push the
                    // resolved fork point above latest and cause silent skips.
                    let fork_point = match self.engine.resolve_fork_cursor_bounded(&new_chain) {
                        Some(ref c) if c.number < current_latest => c.number,
                        Some(_) => current_latest, // no divergence
                        None => 0,                 // no common ancestor: full rollback
                    };

                    if fork_point < current_latest {
                        let changes = self
                            .engine
                            .rollback_to_batch(fork_point, &mut write_batch)?;
                        pending_changes.push((changes, vec![]));
                    }
                    fork_point
                } else {
                    current_latest // advancing — no fork detection needed
                }
            } else {
                0
            }
        };

        // 1. For each table, group rows by block_number and process in order.
        //    Tables are sorted by name for deterministic processing order across
        //    multiple tables (HashMap iteration order is non-deterministic).
        let result = (|| -> Result<()> {
            // Dedup guard (FORK_ISSUES.md issue #1): reject any block at or
            // below the current (post-fork-detection) latest. Re-ingesting an
            // already-processed block would double reducer/MV aggregates.
            // For real chain reorgs the caller must use handle_fork() to roll
            // state back BEFORE feeding new-chain data — relying on ingest()'s
            // in-line fork detection to also delete duplicate content at the
            // fork point is not supported.
            let post_fork_latest = self.engine.latest_block();
            if post_fork_latest > 0 {
                let min_input = input
                    .data
                    .values()
                    .flat_map(|rows| {
                        rows.iter().filter_map(|r| match r.get("block_number") {
                            Some(Value::UInt64(n)) => Some(*n),
                            _ => None,
                        })
                    })
                    .min();
                if let Some(min) = min_input
                    && min <= post_fork_latest
                {
                    return Err(Error::InvalidOperation(format!(
                        "duplicate ingest: block {min} is at or below latest_block {post_fork_latest} — \
                         use handle_fork() for chain reorgs"
                    )));
                }
            }

            let mut tables: Vec<(&String, &Vec<RowMap>)> = input.data.iter().collect();
            tables.sort_by_key(|(name, _)| name.as_str());

            for (table, rows) in tables {
                let mut by_block: BTreeMap<BlockNumber, Vec<RowMap>> = BTreeMap::new();
                for row in rows {
                    let block = match row.get("block_number") {
                        Some(Value::UInt64(n)) => *n,
                        Some(other) => {
                            return Err(Error::InvalidOperation(format!(
                                "row in table '{table}' has invalid block_number type: expected UInt64, got {}",
                                other.type_name()
                            )));
                        }
                        None => {
                            return Err(Error::InvalidOperation(format!(
                                "row in table '{table}' missing block_number"
                            )));
                        }
                    };
                    by_block.entry(block).or_default().push(row.clone());
                }

                for (block, block_rows) in by_block {
                    let (changes, perf_node) = self.engine.process_batch_deferred(
                        table,
                        block,
                        block_rows,
                        &mut write_batch,
                    )?;
                    pending_changes.push((changes, vec![perf_node]));
                }
            }
            Ok(())
        })();

        if let Err(e) = result {
            // Rollback in-memory state to recovery_block (fork point, or 0 for
            // fresh DB). Use the *_to_batch variant with a throwaway batch so
            // the rollback stays in-memory only: raw rows from this ingest
            // were never committed (they live in `write_batch`, dropped below),
            // so storage has no `> recovery_block` rows to delete on disk.
            // Pre-deferred-commit code path called `engine.rollback()` which
            // does a separate `storage.commit(empty)` — if that side-commit
            // fails (disk full / I/O error), engine state gets stuck partial
            // while disk META still points at the old `latest_block`.
            //
            // Propagate the original error after attempting in-memory rollback;
            // if the in-memory rollback itself errors (extremely rare — it
            // mutates Rust collections), surface that as a separate
            // `Error::Rollback` so the caller knows the instance is in a
            // questionable state and should be dropped + reopened.
            let mut throwaway = StorageWriteBatch::new();
            if let Err(rollback_err) =
                self.engine.rollback_to_batch(recovery_block, &mut throwaway)
            {
                return Err(Error::Rollback(format!(
                    "ingest failed ({e}) and recovery rollback also failed ({rollback_err}); \
                     instance state is inconsistent — drop and reopen",
                )));
            }
            return Err(e);
        }

        // Success — push all changes with aggregated perf.
        // Sum per-block durations into one "ingest" node. Children are
        // accumulated in-place using the first block's structure as template.
        let mut all_changes = Vec::new();
        let mut total_ms = 0.0f64;
        let mut merged: Option<PerfNode> = None;
        for (changes, perf) in pending_changes {
            all_changes.extend(changes);
            for node in perf {
                total_ms += node.duration_ms;
                match &mut merged {
                    None => merged = Some(node),
                    Some(m) => {
                        m.duration_ms += node.duration_ms;
                        // Sum children by index (same pipeline structure every block)
                        for (i, child) in node.children.into_iter().enumerate() {
                            if i < m.children.len() {
                                m.children[i].duration_ms += child.duration_ms;
                                for (j, gc) in child.children.into_iter().enumerate() {
                                    if j < m.children[i].children.len() {
                                        m.children[i].children[j].duration_ms += gc.duration_ms;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let batch_perf = match merged {
            Some(m) => vec![PerfNode {
                kind: PerfNodeKind::Pipeline,
                name: "ingest".to_string(),
                duration_ms: total_ms,
                children: vec![m],
            }],
            None => vec![],
        };
        self.buffer.push(
            all_changes,
            self.engine.finalized_cursor(),
            self.engine.latest_cursor(),
            batch_perf,
        );

        // 2. Store rollback chain hashes (including finalized head)
        let mut chain: Vec<(BlockNumber, String)> = input
            .rollback_chain
            .iter()
            .map(|c| (c.number, c.hash.clone()))
            .collect();
        chain.push((
            input.finalized_head.number,
            input.finalized_head.hash.clone(),
        ));
        self.engine.set_rollback_chain(&chain);

        // 3. Finalize: populate write_batch with finalized state + meta.
        //    Note: engine.finalize() destructively prunes in-memory snapshots
        //    even though storage.commit() hasn't run yet. This is intentional:
        //    on crash before ack, drop() loses in-memory state, and on reopen
        //    `replay_unfinalized` rebuilds it from on-disk CF_REDUCER_FIN.
        //    On commit success (in ack or heartbeat path), in-memory and disk
        //    agree.
        self.engine
            .finalize(input.finalized_head.number, &mut write_batch);
        self.append_meta_to_batch(&mut write_batch)?;

        // 4. Update buffer heads with correct cursors (hashes now stored)
        self.buffer
            .set_heads(self.engine.finalized_cursor(), self.engine.latest_cursor());

        // 5. Flush. After this, buffer.next_sequence has been incremented
        //    (if a batch was produced) — persist the post-increment value so
        //    sequences stay monotonic across restart.
        let batch_opt = self.buffer.flush();
        write_batch.put_meta(
            META_NEXT_SEQUENCE,
            &self.buffer.next_sequence().to_be_bytes(),
        );

        match batch_opt {
            None => {
                // Heartbeat: nothing to apply downstream, so nothing to ack.
                // Commit immediately to avoid orphaning the finalize writes
                // (engine.finalize() already pruned in-memory snapshots).
                // On failure, poison the instance — `engine.finalize()`'s
                // in-memory pruning is past disk and a naive retry would
                // re-`finalize` against the pruned state and write an empty
                // batch, silently leaving CF_REDUCER_FIN stale.
                if let Err(e) = self.storage.commit(&write_batch) {
                    self.poisoned = Some(format!("heartbeat ingest commit failed: {e}"));
                    return Err(e);
                }
                Ok(None)
            }
            Some(batch) => {
                // Stash for ack. storage.commit is deferred.
                self.pending = Some(PendingAck {
                    sequence: batch.sequence,
                    write_batch,
                    since: Instant::now(),
                });
                Ok(Some(batch))
            }
        }
    }

    /// Returns `Err(PendingAck)` when an unacked batch is in flight, with the
    /// pending sequence and elapsed time included. Used to gate mutating APIs.
    fn guard_no_pending(&self) -> Result<()> {
        if let Some(p) = &self.pending {
            return Err(Error::PendingAck {
                sequence: p.sequence,
                since: p.since.elapsed(),
            });
        }
        Ok(())
    }

    /// Append engine metadata (latest_block, finalized_block, block_hashes)
    /// to a write batch for atomic commit.
    fn append_meta_to_batch(&self, batch: &mut StorageWriteBatch) -> Result<()> {
        batch.put_meta(META_LATEST_BLOCK, &self.engine.latest_block().to_be_bytes());
        batch.put_meta(
            META_FINALIZED_BLOCK,
            &self.engine.finalized_block().to_be_bytes(),
        );
        let hashes_json = serde_json::to_vec(self.engine.block_hashes())
            .map_err(|e| Error::Storage(format!("failed to serialize block_hashes: {e}")))?;
        batch.put_meta(META_BLOCK_HASHES, &hashes_json);
        Ok(())
    }
}

#[cfg(test)]
#[path = "db_test_helpers.rs"]
mod test_helpers;

#[cfg(test)]
#[path = "db_core_tests.rs"]
mod core_tests;

#[cfg(test)]
#[path = "db_rollback_tests.rs"]
mod rollback_tests;

#[cfg(test)]
#[path = "db_external_tests.rs"]
mod external_tests;

#[cfg(test)]
#[path = "db_validation_tests.rs"]
mod validation_tests;
