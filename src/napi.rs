use std::collections::HashMap;

use napi::NapiRaw;
use napi::bindgen_prelude::*;
use napi::sys;
use napi_derive::napi;

use crate::db::{Config, DeltaDb as DeltaDbInner, IngestInput as IngestInputInner};
use crate::msgpack_conv::{decode_data_from_msgpack, encode_batch_to_msgpack};
use crate::reducer_runtime::external::install_context;
use crate::schema::ast::{ReducerBody, ReducerDef, StateField};
use crate::types::{BlockCursor, ColumnType};

/// Configuration for opening a DeltaDb instance.
#[napi(object)]
pub struct DeltaDbConfig {
    /// SQL schema definition string.
    pub schema: String,
    /// Path to RocksDB data directory for persistence.
    /// When omitted, uses in-memory storage (data lost on restart).
    pub data_dir: Option<String>,
    /// Maximum buffer size before backpressure (default: 10000).
    pub max_buffer_size: Option<u32>,
}

/// Block cursor: number + hash.
#[napi(object)]
pub struct DeltaDbCursor {
    pub number: u32,
    pub hash: String,
}

impl From<BlockCursor> for DeltaDbCursor {
    fn from(c: BlockCursor) -> Self {
        DeltaDbCursor {
            number: c.number as u32,
            hash: c.hash,
        }
    }
}

/// Input for the atomic `ingest()` method.
#[napi(object)]
pub struct IngestInput {
    /// Table name → rows, msgpack-encoded as `{tableName: [{col: val}, ...], ...}`.
    pub data: Buffer,
    /// Unfinalized blocks with hashes for fork resolution.
    pub rollback_chain: Option<Vec<DeltaDbCursor>>,
    /// Finalized head cursor — both number and hash stored.
    pub finalized_head: DeltaDbCursor,
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

/// Delta DB N-API wrapper.
#[napi]
pub struct DeltaDb {
    inner: DeltaDbInner,
    /// Stored raw napi_ref handles for external reducer callbacks (prevent GC).
    external_callbacks: HashMap<String, sys::napi_ref>,
    /// Raw napi_env for cleanup on drop.
    #[allow(dead_code)]
    raw_env: sys::napi_env,
}

#[napi]
impl DeltaDb {
    /// Open a new DeltaDb instance.
    #[napi(factory)]
    pub fn open(env: Env, config: DeltaDbConfig) -> Result<Self> {
        let mut cfg = if let Some(dir) = config.data_dir {
            Config::with_data_dir(config.schema, dir)
        } else {
            Config::new(config.schema)
        };
        if let Some(max) = config.max_buffer_size {
            cfg = cfg.max_buffer_size(max as usize);
        }

        let inner = DeltaDbInner::open(cfg)
            .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;

        Ok(Self {
            inner,
            external_callbacks: HashMap::new(),
            raw_env: env.raw(),
        })
    }

    /// Register an external reducer with a JS batch callback.
    ///
    /// The callback receives an array of `{ state, rows }` groups and must
    /// return an array of `{ state, emits }` results (same length, same order).
    ///
    /// Must be called before any `processBatch` or `ingest` calls.
    #[napi]
    pub fn register_reducer(
        &mut self,
        env: Env,
        config: ExternalReducerConfig,
        callback: JsFunction,
    ) -> Result<()> {
        // Create a raw napi_ref to prevent GC of the callback
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

        // If the reducer already exists in the engine (defined via SQL with
        // LANGUAGE EXTERNAL), we only need to store the callback — the
        // ExternalRuntime will pick it up from the thread-local context.
        // Then replay unfinalized blocks (skipped during open() because
        // no JS context existed at that point).
        if self.inner.has_reducer(&config.name) {
            // Install JS context for the replay call
            let _guard = install_context(env, &self.external_callbacks);
            self.inner
                .replay_reducer(&config.name)
                .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;
        } else {
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
                body: ReducerBody::External { id: config.name },
                requires: vec![],
            };

            // Install JS context for the replay inside register_reducer
            let _guard = install_context(env, &self.external_callbacks);
            self.inner
                .register_reducer(def)
                .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;
        }

        Ok(())
    }

    /// Process a batch of rows for a raw table.
    // process_batch, rollback, finalize removed from public API:
    // not crash-safe individually. Use ingest() which handles all three atomically.

    /// Atomic ingest: process all tables, store rollback chain, finalize, flush.
    /// Returns a msgpack-encoded DeltaBatch buffer, or null if no records produced.
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

        let batch = self
            .inner
            .ingest(ingest_input)
            .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;

        Ok(batch.map(|b| Buffer::from(encode_batch_to_msgpack(&b))))
    }

    /// Find the common ancestor between our state and the Portal's chain.
    /// Returns the matching block cursor, or null if no common ancestor found.
    #[napi]
    pub fn resolve_fork_cursor(
        &self,
        previous_blocks: Vec<DeltaDbCursor>,
    ) -> Option<DeltaDbCursor> {
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

    /// Flush buffered deltas into a msgpack-encoded batch.
    /// Returns null if no pending records.
    #[napi]
    pub fn flush(&mut self) -> Option<Buffer> {
        self.inner
            .flush()
            .map(|b| Buffer::from(encode_batch_to_msgpack(&b)))
    }

    /// Acknowledge a flushed batch by sequence number.
    #[napi]
    pub fn ack(&mut self, sequence: u32) {
        self.inner.ack(sequence as u64);
    }

    /// Number of pending (unflushed) delta records.
    #[napi(getter)]
    pub fn pending_count(&self) -> u32 {
        self.inner.pending_count() as u32
    }

    /// Whether backpressure should be applied.
    #[napi(getter)]
    pub fn is_backpressured(&self) -> bool {
        self.inner.is_backpressured()
    }

    /// Current cursor: latest processed block + hash. Null if no blocks processed.
    #[napi(getter)]
    pub fn cursor(&self) -> Option<DeltaDbCursor> {
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
        _ => ColumnType::String,
    }
}
