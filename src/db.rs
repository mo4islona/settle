use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
// `web_time::Instant` is `std::time::Instant` on native and a JS-Date-backed
// shim on `wasm32-unknown-unknown` (where the real `Instant::now()` panics).
use web_time::Instant;

use crate::change::ChangeBuffer;
use crate::engine::dag::SettleEngine;
use crate::engine::perf;
use crate::error::{Error, Result};
use crate::schema::parser::parse_schema;
use crate::storage::memory::MemoryBackend;
#[cfg(feature = "rocksdb")]
use crate::storage::rocks::{RocksDbBackend, RocksDbConfig};
use crate::storage::{PendingWrites, StorageBackend, StorageWriteBatch, format};
use crate::types::{BlockCursor, BlockNumber, ChangeBatch, ChangeRecord, PerfNode, RowMap, Value};

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
    /// Most change batches that may wait for `ack()` at a time. Default 2:
    /// the caller can ingest the next blocks while the target still applies
    /// the batch before, which already keeps a single target busy. 1 makes
    /// `ingest()` wait for the ack of the batch before it. A call that
    /// returns no batch while batches wait takes a place too: its writes wait
    /// with the newest batch. Each batch reaches disk only when it is acked,
    /// and in sequence order, so a reopen goes back to the last acked batch.
    /// The target must apply every batch, in sequence order, also across a
    /// fork: a fork's compensating batch undoes all the batches before it.
    pub max_pending_batches: usize,
    /// Backfill durability checkpoint interval, in blocks. Default `1` =
    /// persist derived reducer/MV state on every finalize. When `> 1` AND the ingest is no-lag (finalized head at the
    /// batch tip, i.e. historical backfill) AND the pipeline is deferral-safe
    /// (no sliding-window MV, no external reducer), derived-state persistence
    /// is deferred and only flushed every `interval` blocks. Recovery replays
    /// raw rows from the last durable checkpoint, so a crash mid-interval costs
    /// extra replay but never loses data. See docs/backfill-mode-design.md.
    pub backfill_checkpoint_interval: u64,
    /// Build the `ChangeBatch::perf` timing tree on ingest. Default `true`.
    pub collect_perf: bool,
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
            max_pending_batches: 2,
            backfill_checkpoint_interval: 1,
            collect_perf: true,
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
            max_pending_batches: 2,
            backfill_checkpoint_interval: 1,
            collect_perf: true,
        }
    }

    /// Threshold after which the oldest unacked batch causes a warning log on
    /// each `ingest()` that it blocks (which returns `Err(PendingAck)`
    /// immediately regardless).
    pub fn ack_warning_threshold(mut self, threshold: Duration) -> Self {
        self.ack_warning_threshold = threshold;
        self
    }

    pub fn max_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    /// Most change batches that may wait for `ack()` at a time (default 2).
    /// See [`Config::max_pending_batches`].
    pub fn max_pending_batches(mut self, n: usize) -> Self {
        self.max_pending_batches = n.max(1);
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

    /// Backfill durability checkpoint interval, in blocks (default 1 = persist
    /// every finalize). Set `> 1` to defer derived-state persistence on no-lag
    /// historical backfill, flushing every `n` blocks. Safe: recovery replays
    /// raw rows from the last checkpoint. Has no effect at chain tip (where
    /// finality lags), nor for pipelines with sliding-window MVs or external
    /// reducers (those always persist every finalize).
    pub fn backfill_checkpoint_interval(mut self, n: u64) -> Self {
        self.backfill_checkpoint_interval = n.max(1);
        self
    }

    /// Build the per-ingest `ChangeBatch::perf` timing tree (default true).
    /// Off, ingest reads no clock and `perf` stays empty.
    pub fn collect_perf(mut self, on: bool) -> Self {
        self.collect_perf = on;
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

/// Whether the database holds data: every commit of an ingest or a fork
/// writes one of these keys.
fn has_data(storage: &dyn StorageBackend) -> Result<bool> {
    for key in [
        META_LATEST_BLOCK,
        META_FINALIZED_BLOCK,
        META_BLOCK_HASHES,
        META_NEXT_SEQUENCE,
    ] {
        if storage.get_meta(key)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// State stashed between a successful `ingest()` / `handle_fork()` and the
/// caller's `ack()`: the write batch that advances disk to the state after
/// this change batch.
///
/// Only the data path makes one (where the caller received a `ChangeBatch`
/// and owns the apply→ack handshake). A call without a batch commits at
/// once, or behind unacked batches joins the newest one. A failed immediate
/// commit poisons the instance — see [`Settle::poisoned`].
struct PendingAck {
    sequence: u64,
    write_batch: StorageWriteBatch,
    /// Calls without a batch whose writes joined this one. Each takes a
    /// place in the queue, so their writes cannot grow without a bound.
    joined: usize,
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
    /// Unacked batches, oldest first. They commit in this order, so disk
    /// always holds the state after some returned batch, and the engine reads
    /// storage through them (`PendingWrites`) to see the state after the
    /// newest.
    pending: VecDeque<PendingAck>,
    max_pending_batches: usize,
    ack_warning_threshold: Duration,
    /// Set when a non-recoverable commit (heartbeat immediate-commit) fails.
    /// In that case the engine's in-memory state has already mutated past
    /// disk (e.g. `advance_finality` pruned `block_snapshots`) and naively
    /// retrying would write a checkpoint from the pruned state, an *empty*
    /// batch, silently leaving CF_REDUCER_FIN stale. Also set when a rollback
    /// fails part way, or an ingest fails after its fork rollback: memory then
    /// holds a state that no returned batch describes. Every
    /// public mutating method but `ack` short-circuits with `Error::Poisoned`
    /// once this is set; the only recovery is `drop` + reopen, which rebuilds
    /// in-memory state from disk via `replay_unfinalized`. The unacked
    /// batches were all returned before the failure, so `ack` may still
    /// commit them.
    poisoned: Option<String>,
    /// Names of reducers whose runtime has been explicitly installed via
    /// `register_reducer_callback`. Enforces the strict "one callback per
    /// name, drop+reopen to change" contract.
    runtimes_attached: std::collections::HashSet<String>,
    /// Backfill durability checkpoint interval (blocks). 1 = persist every
    /// finalize. See `Config::backfill_checkpoint_interval`.
    backfill_checkpoint_interval: u64,
    /// The format version and layouts that storage lacks. Every write batch
    /// carries them until one commits; each unacked batch holds all of them,
    /// as they grow only while no batch waits.
    stamp: format::Stamp,
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

        let stamp = format::check(storage.as_ref(), &schema, has_data(storage.as_ref())?)?;

        let mut engine = SettleEngine::new(&schema, storage.clone());
        engine.set_collect_perf(config.collect_perf);

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

        // Replay the blocks above the durable checkpoint to rebuild
        // reducer/MV in-memory state; without a checkpoint, every block.
        let durable = engine.durable_block();
        let replay_from = durable.map_or(0, |d| d + 1);
        let latest = engine.latest_block();
        if latest >= replay_from {
            engine.replay_unfinalized(replay_from, latest)?;
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
            pending: VecDeque::new(),
            max_pending_batches: config.max_pending_batches.max(1),
            ack_warning_threshold: config.ack_warning_threshold,
            poisoned: None,
            runtimes_attached: std::collections::HashSet::new(),
            backfill_checkpoint_interval: config.backfill_checkpoint_interval,
            stamp,
        })
    }

    /// Returns true if a previously-produced `ChangeBatch` is still awaiting
    /// `ack()`. While true, `register_reducer`, `register_reducer_callback`
    /// and `replay_reducer` return `Err(PendingAck)`; `ingest` and
    /// `handle_fork` do so when the ack queue is full. Use to
    /// surface state in dashboards / readiness probes.
    pub fn is_awaiting_ack(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Places in use in the ack queue: each change batch that waits for
    /// `ack()`, and each call without a batch whose writes wait with one.
    /// `ingest()` and `handle_fork()` return `PendingAck` once it reaches
    /// `max_pending_batches`.
    pub fn unacked_batches(&self) -> usize {
        self.pending.iter().map(|p| 1 + p.joined).sum()
    }

    /// True if a previous failure has poisoned this instance. Every call but
    /// `ack()` then returns `Poisoned`. Ack the batches that the target
    /// applied, then drop and reopen: the reopened instance can undo only
    /// what is on disk.
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
            self.engine
                .reset_reducer_branch_for_replay(name, finalized)?;
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
        let stamp = format::check_reducer(self.storage.as_ref(), &def)?;
        self.engine.add_reducer(def, self.storage.clone())?;
        self.stamp.extend(stamp);

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
            self.engine
                .reset_reducer_branch_for_replay(name, finalized)?;
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

    /// Acknowledge every returned `ChangeBatch` up to `sequence` and durably
    /// commit their state, oldest first. Acks are cumulative: the target
    /// applies the batches in sequence order, so an ack covers all batches
    /// before it.
    ///
    /// Behaviour:
    /// - No batch with a sequence `<= sequence` waits → `Ok(())`
    ///   (idempotent — covers double-ack after success, stale ack on startup
    ///   before any new ingest).
    /// - `sequence` above the newest unacked batch →
    ///   `Err(WrongAckSequence)` with that batch's sequence as `expected`.
    ///   Nothing is committed; this signals a protocol bug on the caller side.
    /// - Otherwise each batch up to `sequence` commits in turn. On `Err`,
    ///   that batch and all later ones stay unacked — caller MUST retry by
    ///   calling `ack(sequence)` again (NOT `ingest()`, which can return
    ///   `Err(PendingAck)`).
    ///
    /// Also works on a poisoned instance: a failure leaves the unacked write
    /// batches as they were, and each holds the state after a batch that was
    /// returned before it. A target that applied such a batch must ack it
    /// before the reopen, else a later fork cannot undo it in the target.
    pub fn ack(&mut self, sequence: u64) -> Result<()> {
        let Some(newest) = self.pending.back().map(|p| p.sequence) else {
            return Ok(());
        };
        if sequence > newest {
            return Err(Error::WrongAckSequence {
                expected: newest,
                got: sequence,
            });
        }

        // CRITICAL: pop only after a successful commit. Otherwise a commit
        // failure would silently drop the write_batch and leave engine state
        // ahead of disk with no path to recover.
        while let Some(oldest) = self.pending.front() {
            if oldest.sequence > sequence {
                break;
            }
            self.storage.commit(&oldest.write_batch)?;
            self.pending.pop_front();
            self.stamp.clear();
        }
        Ok(())
    }

    /// Number of pending (unflushed) change records inside the buffer.
    ///
    /// Note: this is the buffer's record count; it does NOT reflect the
    /// unacked batches. Use `unacked_batches()` for that.
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

    /// Highest block whose derived (reducer/MV) state is durably persisted to
    /// disk. Equals `finalized_block()` in normal operation; under backfill
    /// deferral it lags finality (derived state for `(durable, finalized]` is
    /// in memory only, rebuilt from raw rows on recovery). Persisted as the
    /// on-disk finalized watermark so recovery replays from `durable + 1`.
    /// 0 also when no data is durable yet; recovery then replays from 0.
    pub fn durable_block(&self) -> BlockNumber {
        self.engine.durable_block().unwrap_or(0)
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
    /// commits metadata (behind unacked batches, with the newest of them) and
    /// returns `batch: None`, or (b) otherwise stashes the write batch as
    /// pending and returns the compensating `ChangeBatch` to the caller.
    /// Caller MUST call `ack(batch.sequence)` after applying the batch to
    /// durably commit.
    ///
    /// The rollback starts from the state after every returned batch, also
    /// the unacked ones, so the target must apply those before this one.
    ///
    /// Returns `Err(PendingAck)` if the ack queue is full (see
    /// `unacked_batches`) — caller must ack first, then retry. Returns
    /// `Err(InvalidOperation)` if `rollback_chain` is empty or no common
    /// ancestor exists.
    pub fn handle_fork(&mut self, mut rollback_chain: Vec<BlockCursor>) -> Result<ForkResult> {
        // 0. Poison gate (see `ingest` for rationale).
        self.guard_not_poisoned()?;
        // 1. Queue guard — before validation, so caller gets a clean
        //    "ack first" signal instead of "bad input".
        self.guard_pending_room()?;

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
        let changes = self.rollback_engine(cursor.number, &mut write_batch)?;

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
                // Heartbeat-style fork. The engine state is already rolled
                // back past disk, so a failed commit poisons the instance.
                self.commit_without_batch(write_batch, "heartbeat handle_fork")?;
                Ok(ForkResult {
                    cursor,
                    batch: None,
                })
            }
            Some(batch) => {
                self.push_pending(batch.sequence, write_batch);
                Ok(ForkResult {
                    cursor,
                    batch: Some(batch),
                })
            }
        }
    }

    /// Atomic ingest: process all tables, store rollback chain, finalize.
    ///
    /// Returns `Ok(Some(batch))` and stashes the underlying write batch as
    /// pending — caller MUST call `ack(batch.sequence)` after successfully
    /// applying the batch to durably commit. Returns `Ok(None)` for heartbeat
    /// ingests (no records produced after merge): nothing requires ack, and
    /// the disk write commits at once, or behind unacked batches with the
    /// newest of them, where it takes a place in the queue.
    ///
    /// Returns `Err(PendingAck)` if the ack queue is full (see
    /// `unacked_batches`) — caller must `ack(seq)` first (or `drop` and
    /// reopen). Returns `Err(InvalidOperation)` if `finalized_head` regresses
    /// below the currently committed finalized block (finality is monotonic;
    /// going above `latest_block` is allowed for gappy chains like Solana).
    ///
    /// If the rollback chain shows a fork, the ingest rolls it back first and
    /// the batch carries the compensating changes. An input error is found
    /// before that rollback. A reducer error after it poisons the instance:
    /// ack the batches that the target applied, drop and reopen, and the next
    /// ingest detects the fork again.
    ///
    /// Each row in `data` must contain a `block_number` field (UInt64).
    pub fn ingest(&mut self, mut input: IngestInput) -> Result<Option<ChangeBatch>> {
        // 0. Poison gate. Any prior immediate-commit failure made in-memory
        //    state diverge from disk; reject all further work so the caller
        //    drops and reopens rather than silently producing stale writes.
        self.guard_not_poisoned()?;

        // 1. Queue guard — before validation, so a caller that retries the
        //    input of an unacked batch gets a clean "ack first" signal, not a
        //    duplicate-block error.
        if let Err(e) = self.guard_pending_room() {
            if let Error::PendingAck { sequence, since } = &e
                && *since > self.ack_warning_threshold
            {
                tracing::warn!(sequence, since = ?since, "ack pending, ingest blocked");
            }
            return Err(e);
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
        let mut pending_changes: Vec<ChangeRecord> = Vec::new();
        let mut block_perf: Vec<PerfNode> = Vec::new();

        let fork_point = self.detect_fork(&input);
        let recovery_block = fork_point.unwrap_or_else(|| self.engine.latest_block());

        // Checked before the fork rollback, which cannot be undone: a bad
        // input then leaves the instance as it was.
        let tables = self.group_input_rows(std::mem::take(&mut input.data), recovery_block)?;

        if let Some(fork_point) = fork_point {
            let changes = self.rollback_engine(fork_point, &mut write_batch)?;
            pending_changes.extend(changes);
        }

        let result = (|| -> Result<()> {
            for (table, by_block) in tables {
                for (block, block_rows) in by_block {
                    let (changes, perf_node) = self.engine.process_batch_deferred(
                        &table,
                        block,
                        block_rows,
                        &mut write_batch,
                    )?;
                    pending_changes.extend(changes);
                    block_perf.extend(perf_node);
                }
            }
            Ok(())
        })();

        if let Err(e) = result {
            // The fork rollback is done and its compensating changes would be
            // lost with this error, so the caller must drop and reopen. The
            // reopened instance has the pre-fork state of the acked batches
            // and detects the fork again.
            if fork_point.is_some() {
                self.poisoned = Some(format!("ingest failed after a fork rollback: {e}"));
                return Err(e);
            }

            // Undo in-memory state to recovery_block (0 for a fresh DB). The
            // undo stays in-memory only: raw rows from this ingest were never
            // committed (they live in `write_batch`, dropped below), so
            // storage has no `> recovery_block` rows to delete on disk, and no
            // separate commit can fail half-way. Not a chain rollback: on
            // gappy chains recovery_block can be below finality.
            let pending = PendingWrites::new(self.pending.iter().map(|p| &p.write_batch));
            if let Err(rollback_err) = self.engine.undo_ingest(recovery_block, &pending) {
                let reason = format!(
                    "ingest failed ({e}) and recovery rollback also failed ({rollback_err}); \
                     instance state is inconsistent — drop and reopen",
                );
                self.poisoned = Some(reason.clone());
                return Err(Error::Rollback(reason));
            }
            return Err(e);
        }

        // Before `advance_finality`: the emit marks the groups that the
        // checkpoint writes (sliding windows) or deletes (emptied groups).
        pending_changes.extend(self.engine.take_changes());

        self.buffer.push(
            pending_changes,
            self.engine.finalized_cursor(),
            self.engine.latest_cursor(),
            perf::merge_blocks(block_perf).into_iter().collect(),
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

        // 3. Finalize in memory, then write a checkpoint into write_batch.
        //    advance_finality() prunes in-memory snapshots before
        //    storage.commit() runs. On a crash before ack, reopen rebuilds that
        //    state from disk and the replayed raw rows; on commit, in-memory and
        //    disk agree.
        //
        //    Backfill deferral: when the ingest is no-lag (finalized head at the
        //    batch tip — historical backfill), the pipeline is deferral-safe,
        //    and the checkpoint interval hasn't elapsed, no checkpoint is
        //    written. The durable watermark (META_FINALIZED_BLOCK) then stays
        //    put and recovery replays raw rows from it. Any lag (tip
        //    operation), interval elapse, or a deferral-unsafe pipeline writes
        //    one, catching the durable watermark up to finality.
        let target_finalized = input.finalized_head.number;
        let checkpoint = {
            let latest = self.engine.latest_block();
            let durable = self.engine.durable_block();
            let no_lag = target_finalized >= latest;
            let defer_active =
                self.backfill_checkpoint_interval > 1 && self.engine.defer_allowed() && no_lag;
            // Until data is durable, recovery has no checkpoint to replay from.
            let reached_interval = durable.is_none_or(|d| {
                target_finalized.saturating_sub(d) >= self.backfill_checkpoint_interval
            });
            !defer_active || reached_interval
        };
        self.engine.advance_finality(target_finalized);
        if checkpoint {
            self.engine.write_checkpoint(&mut write_batch);
        }
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
                // The checkpoint writes must not be orphaned
                // (advance_finality() already pruned in-memory snapshots).
                // A failed commit poisons the instance: the in-memory pruning
                // is past disk, and a naive retry would write a checkpoint
                // from the pruned state, silently leaving CF_REDUCER_FIN stale.
                self.commit_without_batch(write_batch, "heartbeat ingest")?;
                Ok(None)
            }
            Some(batch) => {
                // Stash for ack. storage.commit is deferred.
                self.push_pending(batch.sequence, write_batch);
                Ok(Some(batch))
            }
        }
    }

    /// The fork point if the input's rollback chain shows that blocks above
    /// it are no longer canonical, else None.
    ///
    /// The chain is sorted DESC (newest first) so the HIGHEST common ancestor
    /// wins — preventing catastrophic deep rollbacks when the caller sends
    /// blocks in ascending order.
    ///
    /// IMPORTANT: no fork when the chain is advancing (rollbackChain or
    /// finalizedHead contains blocks above current_latest). In that case, the
    /// caller is providing new unseen blocks, not a rollback signal. External
    /// fork handling (e.g. 409 in pipes-sdk) runs before ingest() is called.
    fn detect_fork(&self, input: &IngestInput) -> Option<BlockNumber> {
        let current_latest = self.engine.latest_block();
        if current_latest == 0 {
            return None;
        }

        let mut sorted_chain: Vec<_> = input.rollback_chain.iter().collect();
        sorted_chain.sort_unstable_by_key(|c| std::cmp::Reverse(c.number));

        // Resolve-fork-cursor would fall through to an old finalized anchor
        // and trigger a spurious rollback.
        let advancing = sorted_chain
            .first()
            .is_some_and(|c| c.number > current_latest)
            || input.finalized_head.number > current_latest;
        if advancing {
            return None;
        }

        let new_chain: Vec<(BlockNumber, &str)> = sorted_chain
            .iter()
            .map(|c| (c.number, c.hash.as_str()))
            .chain(std::iter::once((
                input.finalized_head.number,
                input.finalized_head.hash.as_str(),
            )))
            .collect();

        // Use `_bounded` so a stale future hash can't push the resolved fork
        // point above latest and cause silent skips. No common ancestor means
        // a full rollback.
        let fork_point = self
            .engine
            .resolve_fork_cursor_bounded(&new_chain)
            .map_or(0, |c| c.number);
        (fork_point < current_latest).then_some(fork_point)
    }

    /// Check the input rows and group them by table (in name order, for a
    /// deterministic processing order) and by block. `latest` is the latest
    /// block once a detected fork is rolled back.
    fn group_input_rows(
        &self,
        data: std::collections::HashMap<String, Vec<RowMap>>,
        latest: BlockNumber,
    ) -> Result<Vec<(String, BTreeMap<BlockNumber, Vec<RowMap>>)>> {
        // Dedup guard: reject any block at or below the current
        // (post-fork-detection) latest. Re-ingesting an already-processed
        // block would double reducer/MV aggregates.
        // For real chain reorgs the caller must use handle_fork() to roll
        // state back BEFORE feeding new-chain data — relying on ingest()'s
        // in-line fork detection to also delete duplicate content at the
        // fork point is not supported.
        if latest > 0 {
            let min_input = data
                .values()
                .flat_map(|rows| {
                    rows.iter().filter_map(|r| match r.get("block_number") {
                        Some(Value::UInt64(n)) => Some(*n),
                        _ => None,
                    })
                })
                .min();
            if let Some(min) = min_input
                && min <= latest
            {
                return Err(Error::InvalidOperation(format!(
                    "duplicate ingest: block {min} is at or below latest_block {latest} — \
                     use handle_fork() for chain reorgs"
                )));
            }
        }

        // Rows move into per-block buckets by value, without a RowMap clone.
        let mut tables: Vec<(String, Vec<RowMap>)> = data.into_iter().collect();
        tables.sort_by(|a, b| a.0.cmp(&b.0));

        let mut grouped = Vec::with_capacity(tables.len());
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
                by_block.entry(block).or_default().push(row);
            }

            if !by_block.is_empty() && !self.engine.has_table(&table) {
                return Err(Error::InvalidOperation(format!("unknown table: {table}")));
            }
            grouped.push((table, by_block));
        }
        Ok(grouped)
    }

    /// Roll the engine back to `fork_point`, from the state after the newest
    /// unacked batch. A failure part way leaves some nodes rolled back and
    /// others not, so it poisons the instance.
    fn rollback_engine(
        &mut self,
        fork_point: BlockNumber,
        write_batch: &mut StorageWriteBatch,
    ) -> Result<Vec<ChangeRecord>> {
        self.engine.check_rollback_target(fork_point)?;

        let pending = PendingWrites::new(self.pending.iter().map(|p| &p.write_batch));
        self.engine
            .rollback_to_batch(fork_point, write_batch, &pending)
            .inspect_err(|e| {
                self.poisoned = Some(format!("rollback to block {fork_point} failed: {e}"));
            })
    }

    fn push_pending(&mut self, sequence: u64, write_batch: StorageWriteBatch) {
        self.pending.push_back(PendingAck {
            sequence,
            write_batch,
            joined: 0,
            since: Instant::now(),
        });
    }

    /// Commit the writes of a call that returned no batch. Behind unacked
    /// batches they must not reach disk first, so they join the newest one,
    /// take a place in the queue, and commit with its ack. A failed commit
    /// poisons the instance: the engine state is already past disk.
    fn commit_without_batch(&mut self, write_batch: StorageWriteBatch, call: &str) -> Result<()> {
        if let Some(newest) = self.pending.back_mut() {
            newest.write_batch.ops.extend(write_batch.ops);
            newest.joined += 1;
            return Ok(());
        }

        self.storage.commit(&write_batch).inspect_err(|e| {
            self.poisoned = Some(format!("{call} commit failed: {e}"));
        })?;
        self.stamp.clear();
        Ok(())
    }

    /// Returns `Err(PendingAck)` when an unacked batch is in flight, with the
    /// oldest pending sequence and elapsed time included. Used to gate APIs
    /// that read storage without the unacked batches.
    fn guard_no_pending(&self) -> Result<()> {
        if let Some(p) = self.pending.front() {
            return Err(Error::PendingAck {
                sequence: p.sequence,
                since: p.since.elapsed(),
            });
        }
        Ok(())
    }

    /// Returns `Err(PendingAck)`, as `guard_no_pending`, when the queue has
    /// no free place (see `unacked_batches`).
    fn guard_pending_room(&self) -> Result<()> {
        if self.unacked_batches() < self.max_pending_batches {
            return Ok(());
        }
        self.guard_no_pending()
    }

    /// Append engine metadata (latest_block, durable watermark, block_hashes)
    /// and the stamp to a write batch for atomic commit.
    ///
    /// `META_FINALIZED_BLOCK` stores the engine's DURABLE watermark, not the
    /// in-memory finality watermark. Under backfill deferral the two diverge:
    /// derived state on disk is only as-of `durable_block`, so recovery must
    /// replay raw rows from `durable_block + 1`. On disk the field's meaning is
    /// exactly "state is durable as of this block" — for non-deferred operation
    /// `durable_block == finalized_block`, so this is back-compatible with DBs
    /// written before backfill mode existed. The client still sees the true
    /// finality watermark via `finalized_cursor()` in the ChangeBatch. The key
    /// stays absent until data is durable, and recovery then replays from 0.
    /// `META_LATEST_BLOCK` stays absent until a block exists: a stored 0 reads
    /// back as block 0.
    fn append_meta_to_batch(&self, batch: &mut StorageWriteBatch) -> Result<()> {
        if let Some(latest) = self.engine.latest_block_if_any() {
            batch.put_meta(META_LATEST_BLOCK, &latest.to_be_bytes());
        }
        if let Some(durable) = self.engine.durable_block() {
            batch.put_meta(META_FINALIZED_BLOCK, &durable.to_be_bytes());
        }
        let hashes_json = serde_json::to_vec(self.engine.block_hashes())
            .map_err(|e| Error::Storage(format!("failed to serialize block_hashes: {e}")))?;
        batch.put_meta(META_BLOCK_HASHES, &hashes_json);
        format::write_stamp(&self.stamp, batch);
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

#[cfg(test)]
#[path = "db_recovery_tests.rs"]
mod recovery_tests;

#[cfg(test)]
#[path = "db_pipeline_tests.rs"]
mod pipeline_tests;

#[cfg(test)]
#[path = "db_format_tests.rs"]
mod format_tests;

#[cfg(all(test, feature = "rocksdb"))]
#[path = "backfill_tests.rs"]
mod backfill_tests;
