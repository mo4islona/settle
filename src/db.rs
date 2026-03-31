use std::collections::BTreeMap;
use std::sync::Arc;

use crate::delta::DeltaBuffer;
use crate::engine::dag::DeltaEngine;
use crate::error::{Error, Result};
use crate::schema::parser::parse_schema;
use crate::storage::memory::MemoryBackend;
#[cfg(feature = "rocksdb")]
use crate::storage::rocks::RocksDbBackend;
use crate::storage::{StorageBackend, StorageWriteBatch};
use crate::types::{BlockCursor, BlockNumber, DeltaBatch, DeltaRecord, PerfNode, PerfNodeKind, RowMap, Value};

/// Configuration for opening a DeltaDb instance.
pub struct Config {
    /// SQL schema definition string.
    pub schema: String,
    /// Maximum number of pending delta records before backpressure.
    pub max_buffer_size: usize,
    /// Path to RocksDB data directory. When set, data is persisted to disk.
    /// When None, uses in-memory storage (data lost on drop).
    pub data_dir: Option<String>,
    /// Explicit storage backend override. Takes precedence over data_dir.
    pub storage: Option<Arc<dyn StorageBackend>>,
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
        }
    }

    /// Create a config with RocksDB persistence at the given path.
    pub fn with_data_dir(schema: impl Into<String>, data_dir: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            max_buffer_size: 10_000,
            data_dir: Some(data_dir.into()),
            storage: None,
        }
    }

    pub fn max_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    pub fn storage(mut self, storage: Arc<dyn StorageBackend>) -> Self {
        self.storage = Some(storage);
        self
    }
}

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
    /// Compensating delta batch (rollback deltas), if any state was rolled back.
    pub batch: Option<DeltaBatch>,
}

// Metadata keys for persistence
const META_LATEST_BLOCK: &str = "latest_block";
const META_FINALIZED_BLOCK: &str = "finalized_block";
const META_BLOCK_HASHES: &str = "block_hashes";

/// Top-level Delta DB API.
///
/// Provides a simple interface for ingesting blockchain data,
/// handling rollbacks, and producing delta batches for downstream targets.
pub struct DeltaDb {
    engine: DeltaEngine,
    buffer: DeltaBuffer,
    storage: Arc<dyn StorageBackend>,
}

impl DeltaDb {
    /// Open a DeltaDb instance with the given configuration.
    /// Parses and validates the schema at open time.
    pub fn open(config: Config) -> Result<Self> {
        let schema = parse_schema(&config.schema)?;

        let storage: Arc<dyn StorageBackend> = if let Some(s) = config.storage {
            s
        } else if let Some(ref _dir) = config.data_dir {
            #[cfg(feature = "rocksdb")]
            {
                Arc::new(RocksDbBackend::open(_dir)?)
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

        let mut engine = DeltaEngine::new(&schema, storage.clone());

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

        let buffer = DeltaBuffer::new(config.max_buffer_size);

        Ok(Self {
            engine,
            buffer,
            storage,
        })
    }

    /// Replace the runtime for a named reducer (for External/FnReducer injection).
    /// Replays unfinalized blocks so the reducer catches up with current state.
    pub fn set_reducer_runtime(
        &mut self,
        name: &str,
        runtime: Box<dyn crate::reducer_runtime::ReducerRuntime>,
    ) -> Result<()> {
        if !self.engine.has_reducer(name) {
            return Err(Error::InvalidOperation(format!(
                "set_reducer_runtime: unknown reducer '{name}'"
            )));
        }
        self.engine.set_reducer_runtime(name, runtime);

        // Replay only this reducer and its downstream MVs — a full replay
        // would double-process non-external reducers that were already replayed
        // during open().
        let finalized = self.engine.finalized_block();
        let latest = self.engine.latest_block();
        if latest > finalized {
            self.engine
                .replay_unfinalized_for(finalized + 1, latest, name)?;
        }
        Ok(())
    }

    /// Register an external reducer definition.
    /// The reducer is added to the engine's pipeline and unfinalized blocks
    /// are replayed so it catches up with the current state.
    pub fn register_reducer(&mut self, def: crate::schema::ast::ReducerDef) -> Result<()> {
        let name = def.name.clone();
        self.engine.add_reducer(def, self.storage.clone())?;

        // Replay only this reducer and its downstream MVs
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
    pub fn replay_reducer(&mut self, name: &str) -> Result<()> {
        let finalized = self.engine.finalized_block();
        let latest = self.engine.latest_block();
        if latest > finalized {
            self.engine
                .replay_unfinalized_for(finalized + 1, latest, name)?;
        }
        Ok(())
    }

    /// Check if a reducer with the given name already exists in the engine.
    pub fn has_reducer(&self, name: &str) -> bool {
        self.engine.has_reducer(name)
    }

    /// Process a batch of rows for a raw table at the given block number.
    /// Delta records are buffered internally.
    /// Returns true if backpressure should be applied (buffer is full).
    ///
    /// **Warning:** This method writes raw rows to storage immediately but does
    /// not persist `latest_block` metadata until the next `finalize()`. A crash
    /// between these two operations leaves orphaned raw rows in storage that are
    /// never replayed into reducer/MV state on recovery. For crash-safe ingestion,
    /// use `ingest()` which commits all writes atomically.
    /// **Deprecated**: Not crash-safe. Use `ingest()` instead.
    /// Kept public for benchmarks and tests only.
    #[doc(hidden)]
    pub fn process_batch(
        &mut self,
        table: &str,
        block: BlockNumber,
        rows: Vec<RowMap>,
    ) -> Result<bool> {
        let (deltas, perf_node) = self.engine.process_batch(table, block, rows)?;

        self.buffer.push(
            deltas,
            self.engine.finalized_cursor(),
            self.engine.latest_cursor(),
            vec![perf_node],
        );

        Ok(self.buffer.is_full())
    }

    /// Roll back all state after fork_point.
    /// Compensating delta records are buffered.
    /// Raw-row deletions + metadata updates are committed atomically.
    pub fn rollback(&mut self, fork_point: BlockNumber) -> Result<()> {
        let mut batch = StorageWriteBatch::new();
        let deltas = self.engine.rollback_to_batch(fork_point, &mut batch)?;

        // Persist updated latest_block + block_hashes atomically with raw-row deletions
        self.append_meta_to_batch(&mut batch)?;
        self.storage.commit(&batch)?;

        self.buffer.push(
            deltas,
            self.engine.finalized_cursor(),
            self.engine.latest_cursor(),
            vec![],
        );

        Ok(())
    }

    /// Finalize all state up to and including the given block.
    /// Finalized data cannot be rolled back.
    /// All finalized state + metadata is committed atomically.
    pub fn finalize(&mut self, block: BlockNumber) -> Result<()> {
        let mut batch = StorageWriteBatch::new();
        self.engine.finalize(block, &mut batch);
        self.append_meta_to_batch(&mut batch)?;
        self.storage.commit(&batch)
    }

    /// Flush all buffered delta records into a DeltaBatch.
    /// Returns None if there are no pending records.
    pub fn flush(&mut self) -> Option<DeltaBatch> {
        self.buffer.flush()
    }

    /// Acknowledge a previously flushed batch by sequence number.
    pub fn ack(&mut self, sequence: u64) {
        self.buffer.ack(sequence);
    }

    /// Number of pending (unflushed) delta records.
    pub fn pending_count(&self) -> usize {
        self.buffer.pending_count()
    }

    /// Whether backpressure should be applied.
    pub fn is_backpressured(&self) -> bool {
        self.buffer.is_full()
    }

    /// Current latest processed block number.
    pub fn latest_block(&self) -> BlockNumber {
        self.engine.latest_block()
    }

    /// Current latest processed block as a cursor (number + hash).
    pub fn latest_cursor(&self) -> Option<BlockCursor> {
        self.engine.latest_cursor()
    }

    /// Current finalized block number.
    pub fn finalized_block(&self) -> BlockNumber {
        self.engine.finalized_block()
    }

    /// Current finalized block as a cursor (number + hash).
    pub fn finalized_cursor(&self) -> Option<BlockCursor> {
        self.engine.finalized_cursor()
    }

    /// Store block hashes from the rollback chain and finalized head.
    pub fn set_rollback_chain(&mut self, chain: &[(BlockNumber, String)]) {
        self.engine.set_rollback_chain(chain);
    }

    /// Find the common ancestor between our state and the Portal's chain.
    pub fn resolve_fork_cursor(
        &self,
        previous_blocks: &[(BlockNumber, &str)],
    ) -> Option<BlockCursor> {
        self.engine.resolve_fork_cursor(previous_blocks)
    }

    /// Atomically handle a fork (409 from Portal).
    ///
    /// Finds the highest common ancestor in `rollback_chain`, rolls back all
    /// state after that point, commits compensating deltas and updated metadata
    /// atomically, and returns the cursor to resume from plus any delta batch.
    ///
    /// Uses the current internal finalized block — no need to pass it in.
    ///
    /// Returns `Err` if no common ancestor is found (fork too deep / unrecoverable).
    pub fn handle_fork(&mut self, mut rollback_chain: Vec<BlockCursor>) -> Result<ForkResult> {
        // Sort DESC to ensure we find the HIGHEST common ancestor first,
        // matching the same contract as ingest()'s rollback_chain handling.
        rollback_chain.sort_unstable_by_key(|c| std::cmp::Reverse(c.number));

        let previous_blocks: Vec<(BlockNumber, &str)> = rollback_chain
            .iter()
            .map(|c| (c.number, c.hash.as_str()))
            .collect();

        let cursor = self
            .engine
            .resolve_fork_cursor(&previous_blocks)
            .ok_or_else(|| {
                Error::InvalidOperation(
                    "Fork too deep: no common ancestor found in block hashes".into(),
                )
            })?;

        let mut write_batch = StorageWriteBatch::new();
        let deltas = self
            .engine
            .rollback_to_batch(cursor.number, &mut write_batch)?;

        // Store the new rollback chain and persist atomically
        let chain: Vec<(BlockNumber, String)> = rollback_chain
            .iter()
            .map(|c| (c.number, c.hash.clone()))
            .collect();
        self.engine.set_rollback_chain(&chain);

        self.append_meta_to_batch(&mut write_batch)?;
        self.storage.commit(&write_batch)?;

        self.buffer.push(
            deltas,
            self.engine.finalized_cursor(),
            self.engine.latest_cursor(),
            vec![],
        );
        self.buffer
            .set_heads(self.engine.finalized_cursor(), self.engine.latest_cursor());

        let batch = self.buffer.flush();
        Ok(ForkResult { cursor, batch })
    }

    /// Atomic ingest: process all tables, store rollback chain, finalize, flush.
    ///
    /// Replaces separate `process_batch` + `set_rollback_chain` + `finalize` + `flush`.
    /// Each row must contain a `block_number` field (UInt64).
    pub fn ingest(&mut self, input: IngestInput) -> Result<Option<DeltaBatch>> {
        // Single WriteBatch for all storage writes (raw rows + finalize + meta)
        let mut write_batch = StorageWriteBatch::new();

        // Collect deltas locally — only push to buffer on success to avoid
        // partial deltas leaking into downstream output on failure.
        let mut pending_deltas: Vec<(Vec<DeltaRecord>, Vec<PerfNode>)> = Vec::new();

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

                    let fork_point = match self.engine.resolve_fork_cursor(&new_chain) {
                        Some(ref c) if c.number < current_latest => c.number,
                        Some(_) => current_latest, // no divergence
                        None => 0,                 // no common ancestor: full rollback
                    };

                    if fork_point < current_latest {
                        let deltas = self
                            .engine
                            .rollback_to_batch(fork_point, &mut write_batch)?;
                        pending_deltas.push((deltas, vec![]));
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
                    let (deltas, perf_node) = self.engine.process_batch_deferred(
                        table,
                        block,
                        block_rows,
                        &mut write_batch,
                    )?;
                    pending_deltas.push((deltas, vec![perf_node]));
                }
            }
            Ok(())
        })();

        if let Err(e) = result {
            // Rollback in-memory state to recovery_block (the fork point, or 0 for fresh DB).
            // pending_deltas is dropped — buffer stays clean.
            let _ = self.engine.rollback(recovery_block);
            return Err(e);
        }

        // Success — push all deltas with aggregated perf.
        // Sum per-block durations into one "ingest" node. Children are
        // accumulated in-place using the first block's structure as template.
        let mut all_deltas = Vec::new();
        let mut total_ms = 0.0f64;
        let mut merged: Option<PerfNode> = None;
        for (deltas, perf) in pending_deltas {
            all_deltas.extend(deltas);
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
            all_deltas,
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

        // 3. Finalize atomically
        self.engine
            .finalize(input.finalized_head.number, &mut write_batch);
        self.append_meta_to_batch(&mut write_batch)?;
        self.storage.commit(&write_batch)?;

        // 4. Update buffer heads with correct cursors (hashes now stored)
        self.buffer
            .set_heads(self.engine.finalized_cursor(), self.engine.latest_cursor());

        // 5. Flush
        let batch = self.buffer.flush();

        Ok(batch)
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
