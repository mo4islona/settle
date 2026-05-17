use std::collections::HashMap;

use napi::NapiRaw;
use napi::bindgen_prelude::*;
use napi::sys;
use napi_derive::napi;

use crate::db::{Config, IngestInput as IngestInputInner, Settle as Inner};
use crate::error::Error as SettleError;
use crate::msgpack_conv::{decode_data_from_msgpack, encode_batch_to_msgpack};
use crate::reducer_runtime::external::install_context;
use crate::schema::ast::{ReducerBody, ReducerDef, StateField};
use crate::types::{BlockCursor, ColumnType};

/// Convert a `SettleError` to a NAPI error. Typed variants (`PendingAck`,
/// `WrongAckSequence`) are surfaced with a structured prefix that the TS
/// wrapper recognizes and rethrows as a typed JS class. Other variants stay
/// as plain `GenericFailure` with the error display string.
fn settle_err_to_napi(e: SettleError) -> napi::Error {
    match e {
        SettleError::PendingAck { sequence, since } => napi::Error::from_reason(format!(
            "__SETTLE_PENDING_ACK__ sequence={sequence} since_ms={}",
            since.as_millis()
        )),
        SettleError::WrongAckSequence { expected, got } => napi::Error::from_reason(format!(
            "__SETTLE_WRONG_ACK_SEQUENCE__ expected={expected} got={got}"
        )),
        other => napi::Error::new(Status::GenericFailure, format!("{other}")),
    }
}

/// Configuration for opening a Settle instance.
#[napi(object)]
pub struct SettleConfig {
    /// SQL schema definition string.
    pub schema: String,
    /// Path to RocksDB data directory for persistence.
    /// When omitted, uses in-memory storage (data lost on restart).
    pub data_dir: Option<String>,
    /// Maximum buffer size before backpressure (default: 10000).
    pub max_buffer_size: Option<u32>,
    /// Compression algorithm for RocksDB: "none", "snappy" (default), "zstd", "lz4".
    pub compression: Option<String>,
    /// Disable RocksDB automatic background compactions.
    pub disable_compaction: Option<bool>,
    /// Block cache size in bytes. Omit for RocksDB default (~8MB per CF), 0 to disable.
    /// Capped at the target's `usize::MAX` (≈18 EB on 64-bit, ≈4 GB on 32-bit);
    /// values above that are rejected. JS `number` precision is exact only up to
    /// `2^53` (≈9 PB), so larger values cannot be passed reliably from JS anyway.
    /// Negative values are rejected.
    pub cache_size: Option<i64>,
}

/// Block cursor: number + hash.
#[napi(object)]
pub struct SettleCursor {
    pub number: u32,
    pub hash: String,
}

impl From<BlockCursor> for SettleCursor {
    fn from(c: BlockCursor) -> Self {
        SettleCursor {
            number: c.number as u32,
            hash: c.hash,
        }
    }
}

/// Result of `handleFork()`.
#[napi(object)]
pub struct ForkResultJs {
    /// The block to resume ingestion from (highest common ancestor).
    pub cursor: SettleCursor,
    /// Compensating change batch (msgpack-encoded), or null if nothing was rolled back.
    pub batch: Option<Buffer>,
}

/// Input for the atomic `ingest()` method.
#[napi(object)]
pub struct IngestInput {
    /// Table name → rows, msgpack-encoded as `{tableName: [{col: val}, ...], ...}`.
    pub data: Buffer,
    /// Unfinalized blocks with hashes for fork resolution.
    pub rollback_chain: Option<Vec<SettleCursor>>,
    /// Finalized head cursor — both number and hash stored.
    pub finalized_head: SettleCursor,
}

/// State field definition for external reducers.
#[napi(object)]
pub struct ExternalStateField {
    pub name: String,
    /// Column type: "Float64", "UInt64", "Int64", "String", "Boolean", "Json"
    pub column_type: String,
    /// Default value as a string literal (e.g., "0", "'hello'", "{}")
    pub default_value: String,
}

/// Configuration for registering an external reducer.
#[napi(object)]
pub struct ExternalReducerConfig {
    pub name: String,
    pub source: String,
    pub group_by: Vec<String>,
    pub state: Vec<ExternalStateField>,
}

/// Settle N-API wrapper.
#[napi]
pub struct Settle {
    inner: Inner,
    /// Stored raw napi_ref handles for external reducer callbacks (prevent GC).
    external_callbacks: HashMap<String, sys::napi_ref>,
    /// Raw napi_env, captured at open() for callback-ref cleanup on Drop.
    raw_env: sys::napi_env,
}

impl Drop for Settle {
    fn drop(&mut self) {
        // Release the strong references we created in `register_reducer` so
        // the underlying JS callbacks become eligible for GC. Without this
        // every registered reducer permanently roots its callback —
        // a leak that compounds for long-lived processes that open
        // multiple `Settle` instances over their lifetime.
        for (_name, raw_ref) in self.external_callbacks.drain() {
            unsafe {
                sys::napi_delete_reference(self.raw_env, raw_ref);
            }
        }
    }
}

#[napi]
impl Settle {
    /// Open a new Settle instance.
    #[napi(factory)]
    pub fn open(env: Env, config: SettleConfig) -> Result<Self> {
        let mut cfg = if let Some(dir) = config.data_dir {
            Config::with_data_dir(config.schema, dir)
        } else {
            Config::new(config.schema)
        };
        if let Some(max) = config.max_buffer_size {
            cfg = cfg.max_buffer_size(max as usize);
        }
        cfg.compression = config.compression;
        cfg.disable_compaction = config.disable_compaction.unwrap_or(false);
        cfg.cache_size = match config.cache_size {
            Some(s) if s < 0 => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "cache_size must be non-negative",
                ))
            }
            Some(s) if (s as u128) > usize::MAX as u128 => {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!(
                        "cache_size ({s}) exceeds usize::MAX ({}) on this target",
                        usize::MAX
                    ),
                ))
            }
            Some(s) => Some(s as usize),
            None => None,
        };

        let inner = Inner::open(cfg)
            .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;

        Ok(Self {
            inner,
            external_callbacks: HashMap::new(),
            raw_env: env.raw(),
        })
    }

    /// Register a brand-new external reducer + JS batch callback.
    ///
    /// **Strict semantics**: errors if a reducer with this name already
    /// exists (whether declared in SQL via `LANGUAGE EXTERNAL` or registered
    /// via a prior call). To attach a callback to a reducer that was
    /// declared in SQL, use `registerReducerCallback(name, callback)`.
    /// To change a callback after it's registered, drop and reopen the
    /// instance — silent hot-reload is not supported.
    ///
    /// The callback receives an array of `{ state, rows }` groups and must
    /// return an array of `{ state, emits }` results (same length, same order).
    #[napi]
    pub fn register_reducer(
        &mut self,
        env: Env,
        config: ExternalReducerConfig,
        callback: JsFunction,
    ) -> Result<()> {
        // Strict: a reducer with this name must NOT already exist (in SQL
        // or registered previously). If it does, the caller probably meant
        // `registerReducerCallback` (attach callback to existing SQL slot).
        if self.inner.has_reducer(&config.name) {
            return Err(Error::new(
                Status::InvalidArg,
                format!(
                    "registerReducer: reducer '{}' already exists — \
                     use registerReducerCallback to attach a callback to a \
                     SQL-declared external reducer, or drop and reopen the \
                     instance to change a previously-registered callback",
                    config.name,
                ),
            ));
        }
        if self.external_callbacks.contains_key(&config.name) {
            return Err(Error::new(
                Status::InvalidArg,
                format!(
                    "registerReducer: callback for '{}' is already registered",
                    config.name,
                ),
            ));
        }

        // Create the raw napi_ref protecting the JS callback from GC.
        let mut raw_ref: sys::napi_ref = std::ptr::null_mut();
        let status =
            unsafe { sys::napi_create_reference(env.raw(), callback.raw(), 1, &mut raw_ref) };
        if status != sys::Status::napi_ok {
            return Err(Error::new(
                Status::GenericFailure,
                "failed to create callback reference",
            ));
        }
        self.external_callbacks.insert(config.name.clone(), raw_ref);

        let state_fields: Vec<StateField> = config
            .state
            .into_iter()
            .map(|f| {
                let column_type = parse_column_type(&f.column_type);
                StateField {
                    name: f.name,
                    column_type,
                    default: f.default_value,
                }
            })
            .collect();

        let def = ReducerDef {
            name: config.name.clone(),
            source: config.source,
            group_by: config.group_by,
            state: state_fields,
            body: ReducerBody::External {
                id: config.name.clone(),
            },
            requires: vec![],
        };

        let _guard = install_context(env, &self.external_callbacks);
        if let Err(e) = self.inner.register_reducer(def).map_err(settle_err_to_napi) {
            // Strict roll-back: there was no previous callback (we checked
            // above), so we just remove what we just inserted.
            self.external_callbacks.remove(&config.name);
            unsafe {
                sys::napi_delete_reference(env.raw(), raw_ref);
            }
            return Err(e);
        }

        Ok(())
    }

    /// Attach a JS callback to an existing reducer that was declared in
    /// SQL with `LANGUAGE EXTERNAL`, and re-replay unfinalized blocks
    /// through it.
    ///
    /// **Strict semantics**: errors if no reducer named `name` exists,
    /// AND errors if a callback is already registered for that name. To
    /// change a registered callback, drop and reopen the instance.
    #[napi]
    pub fn register_reducer_callback(
        &mut self,
        env: Env,
        name: String,
        callback: JsFunction,
    ) -> Result<()> {
        if !self.inner.has_reducer(&name) {
            return Err(Error::new(
                Status::InvalidArg,
                format!(
                    "registerReducerCallback: no reducer named '{name}' — \
                     use registerReducer to create a brand-new reducer",
                ),
            ));
        }
        if !self.inner.reducer_is_external(&name) {
            return Err(Error::new(
                Status::InvalidArg,
                format!(
                    "registerReducerCallback: reducer '{name}' is not declared \
                     LANGUAGE EXTERNAL — Lua and EventRules reducers have their \
                     own embedded runtime and ignore host callbacks",
                ),
            ));
        }
        if self.external_callbacks.contains_key(&name) {
            return Err(Error::new(
                Status::InvalidArg,
                format!(
                    "registerReducerCallback: callback for '{name}' is already \
                     registered — drop and reopen the instance to change it",
                ),
            ));
        }

        let mut raw_ref: sys::napi_ref = std::ptr::null_mut();
        let status =
            unsafe { sys::napi_create_reference(env.raw(), callback.raw(), 1, &mut raw_ref) };
        if status != sys::Status::napi_ok {
            return Err(Error::new(
                Status::GenericFailure,
                "failed to create callback reference",
            ));
        }
        self.external_callbacks.insert(name.clone(), raw_ref);

        let _guard = install_context(env, &self.external_callbacks);
        if let Err(e) = self.inner.replay_reducer(&name).map_err(settle_err_to_napi) {
            self.external_callbacks.remove(&name);
            unsafe {
                sys::napi_delete_reference(env.raw(), raw_ref);
            }
            return Err(e);
        }

        Ok(())
    }

    /// Process a batch of rows for a raw table.
    // process_batch, rollback, finalize removed from public API:
    // not crash-safe individually. Use ingest() which handles all three atomically.

    /// Atomic ingest: process all tables, store rollback chain, finalize, flush.
    /// Returns a msgpack-encoded ChangeBatch buffer, or null if no records produced.
    #[napi]
    pub fn ingest(&mut self, env: Env, input: IngestInput) -> Result<Option<Buffer>> {
        let data =
            decode_data_from_msgpack(&input.data).map_err(|e| Error::new(Status::InvalidArg, e))?;

        let rollback_chain = input
            .rollback_chain
            .unwrap_or_default()
            .into_iter()
            .map(|c| BlockCursor {
                number: c.number as u64,
                hash: c.hash,
            })
            .collect();

        let ingest_input = IngestInputInner {
            data,
            rollback_chain,
            finalized_head: BlockCursor {
                number: input.finalized_head.number as u64,
                hash: input.finalized_head.hash,
            },
        };

        // Install external callback context for the duration of this call
        let _guard = if !self.external_callbacks.is_empty() {
            Some(install_context(env, &self.external_callbacks))
        } else {
            None
        };

        let batch = self.inner.ingest(ingest_input).map_err(settle_err_to_napi)?;

        Ok(batch.map(|b| Buffer::from(encode_batch_to_msgpack(&b))))
    }

    /// Find the common ancestor between our state and the Portal's chain.
    /// Returns the matching block cursor, or null if no common ancestor found.
    #[napi]
    pub fn resolve_fork_cursor(
        &self,
        previous_blocks: Vec<SettleCursor>,
    ) -> Option<SettleCursor> {
        // Sort DESC so resolve_fork_cursor returns the HIGHEST common ancestor
        // regardless of the order the portal sends previousBlocks (typically ASC).
        let mut blocks: Vec<(u64, String)> = previous_blocks
            .into_iter()
            .map(|c| (c.number as u64, c.hash))
            .collect();
        blocks.sort_unstable_by_key(|(n, _)| std::cmp::Reverse(*n));
        let refs: Vec<(u64, &str)> = blocks.iter().map(|(n, h)| (*n, h.as_str())).collect();
        self.inner.resolve_fork_cursor(&refs).map(|c| c.into())
    }

    /// Atomically handle a fork (409 from Portal).
    ///
    /// Finds the common ancestor in `previousBlocks`, rolls back all state after
    /// that point, and returns the cursor to resume from plus any compensating
    /// change batch (msgpack-encoded). Uses the internal finalized block — no need
    /// to pass it in.
    ///
    /// Throws if no common ancestor is found (fork too deep / unrecoverable).
    #[napi]
    pub fn handle_fork(
        &mut self,
        _env: Env,
        previous_blocks: Vec<SettleCursor>,
    ) -> napi::Result<ForkResultJs> {
        let chain: Vec<crate::types::BlockCursor> = previous_blocks
            .into_iter()
            .map(|c| crate::types::BlockCursor {
                number: c.number as u64,
                hash: c.hash,
            })
            .collect();

        let result = self.inner.handle_fork(chain).map_err(settle_err_to_napi)?;

        let batch = result
            .batch
            .map(|b| Buffer::from(encode_batch_to_msgpack(&b)));

        Ok(ForkResultJs {
            cursor: result.cursor.into(),
            batch,
        })
    }

    /// Acknowledge the pending batch by sequence number and durably commit
    /// its writes. `sequence` is passed as `i64` — the JS-side accepts a
    /// non-negative integer up to `Number.MAX_SAFE_INTEGER` (2^53), well
    /// below `i64::MAX`; the TS wrapper rejects fractional / out-of-range
    /// values before they reach this boundary.
    ///
    /// Returns the typed errors `SettlePendingAckError` / `SettleWrongAckSequenceError`
    /// via the structured-reason prefix protocol; the TS wrapper rethrows them
    /// as typed classes.
    #[napi]
    pub fn ack(&mut self, sequence: i64) -> napi::Result<()> {
        if sequence < 0 {
            return Err(napi::Error::new(
                Status::InvalidArg,
                "ack sequence must be non-negative",
            ));
        }
        self.inner
            .ack(sequence as u64)
            .map_err(settle_err_to_napi)
    }

    /// Number of pending (unflushed) change records in the buffer. Does NOT
    /// reflect the pending-ack slot — see `isAwaitingAck` for that.
    #[napi(getter)]
    pub fn pending_count(&self) -> u32 {
        self.inner.pending_count() as u32
    }

    /// Whether backpressure should be applied.
    #[napi(getter)]
    pub fn is_backpressured(&self) -> bool {
        self.inner.is_backpressured()
    }

    /// Whether a previously-returned `ChangeBatch` is still awaiting `ack()`.
    /// While true, mutating APIs (`ingest`, `handleFork`, `registerReducer`)
    /// throw `SettlePendingAckError`.
    #[napi(getter)]
    pub fn is_awaiting_ack(&self) -> bool {
        self.inner.is_awaiting_ack()
    }

    /// Whether an unrecoverable commit failure has poisoned this instance.
    /// Once true the only recovery is to drop the instance and reopen — all
    /// mutating calls reject with `Error("instance poisoned ...")` so the
    /// caller does not silently produce stale writes.
    #[napi(getter)]
    pub fn is_poisoned(&self) -> bool {
        self.inner.is_poisoned()
    }

    /// Current cursor: latest processed block + hash. Null if no blocks processed.
    #[napi(getter)]
    pub fn cursor(&self) -> Option<SettleCursor> {
        self.inner.latest_cursor().map(|c| c.into())
    }
}

fn parse_column_type(s: &str) -> ColumnType {
    match s.to_lowercase().as_str() {
        "float64" => ColumnType::Float64,
        "uint64" => ColumnType::UInt64,
        "int64" => ColumnType::Int64,
        "string" => ColumnType::String,
        "boolean" => ColumnType::Boolean,
        "json" => ColumnType::JSON,
        "datetime" => ColumnType::DateTime,
        "uint256" => ColumnType::Uint256,
        "bytes" => ColumnType::Bytes,
        "base58" => ColumnType::Base58,
        _ => ColumnType::String,
    }
}
