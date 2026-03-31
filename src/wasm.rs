use std::collections::HashMap;

use js_sys::Function;
use serde::Serialize;
use wasm_bindgen::prelude::*;

use crate::db::{Config, DeltaDb as DeltaDbInner, IngestInput as IngestInputInner};
use crate::json_conv::json_object_to_row;
use crate::msgpack_conv::encode_batch_to_json_value;
use crate::reducer_runtime::external::{WasmContextGuard, install_wasm_context};
use crate::schema::ast::{ReducerBody, ReducerDef, StateField};
use crate::types::{BlockCursor, ColumnType, RowMap};

/// Serialize a value with maps-as-objects so HashMap serializes as a plain JS
/// object (not a JS Map). Used for cursor serialization.
fn to_js<T: Serialize>(v: &T) -> Result<JsValue, serde_wasm_bindgen::Error> {
    v.serialize(&serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true))
}

/// WASM binding for DeltaDb.
#[wasm_bindgen]
pub struct DeltaDb {
    inner: DeltaDbInner,
    /// JS callbacks for external reducers, keyed by reducer name.
    external_callbacks: HashMap<String, Function>,
}

#[wasm_bindgen]
impl DeltaDb {
    /// Create a new DeltaDb with in-memory storage.
    #[wasm_bindgen(constructor)]
    pub fn new(schema: &str) -> Result<DeltaDb, JsError> {
        let config = Config::new(schema);
        let inner = DeltaDbInner::open(config).map_err(to_js_err)?;
        Ok(DeltaDb {
            inner,
            external_callbacks: HashMap::new(),
        })
    }

    /// Register an external reducer with a JS batch callback.
    ///
    /// The callback receives an array of `{ state, rows }` groups and must
    /// return an array of `{ state, emits }` results (same length, same order).
    ///
    /// Must be called before any `ingest` calls that use this reducer.
    pub fn register_reducer(
        &mut self,
        name: &str,
        source: &str,
        group_by: JsValue,
        state: JsValue,
        callback: Function,
    ) -> Result<(), JsError> {
        let group_by: Vec<String> = serde_wasm_bindgen::from_value(group_by).map_err(to_js_err)?;
        let state_fields: Vec<WasmStateField> =
            serde_wasm_bindgen::from_value(state).map_err(to_js_err)?;

        self.external_callbacks.insert(name.to_string(), callback);

        let state = state_fields
            .into_iter()
            .map(|f| StateField {
                name: f.name,
                column_type: parse_column_type(&f.column_type),
                default: f.default_value,
            })
            .collect();

        let _guard = self.install_context();

        if self.inner.has_reducer(name) {
            self.inner.replay_reducer(name).map_err(to_js_err)?;
        } else {
            let def = ReducerDef {
                name: name.to_string(),
                source: source.to_string(),
                group_by,
                state,
                body: ReducerBody::External {
                    id: name.to_string(),
                },
                requires: vec![],
            };
            self.inner.register_reducer(def).map_err(to_js_err)?;
        }

        Ok(())
    }

    /// Atomic ingest: process all tables, finalize, and return delta batch.
    /// Input and output are plain JS objects — no msgpack encoding needed.
    pub fn ingest(&mut self, input: JsValue) -> Result<JsValue, JsError> {
        let input: WasmIngestInput = serde_wasm_bindgen::from_value(input).map_err(to_js_err)?;

        // Convert plain JSON rows to typed RowMap — serde_wasm_bindgen gives us
        // serde_json::Value objects which json_object_to_row maps to our Value enum.
        let data: HashMap<String, Vec<RowMap>> = input
            .data
            .into_iter()
            .map(|(table, rows)| {
                let typed_rows: Result<Vec<RowMap>, _> = rows
                    .iter()
                    .enumerate()
                    .map(|(i, row)| {
                        json_object_to_row(row).ok_or_else(|| {
                            to_js_err(format!(
                                "table '{table}': row {i} is not a JSON object"
                            ))
                        })
                    })
                    .collect();
                typed_rows.map(|rows| (table, rows))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        let ingest_input = IngestInputInner {
            data,
            rollback_chain: input
                .rollback_chain
                .unwrap_or_default()
                .into_iter()
                .map(|c| BlockCursor {
                    number: c.number as u64,
                    hash: c.hash,
                })
                .collect(),
            finalized_head: BlockCursor {
                number: input.finalized_head.number as u64,
                hash: input.finalized_head.hash,
            },
        };

        let _guard = self.install_context();
        let batch = self.inner.ingest(ingest_input).map_err(to_js_err)?;
        match batch {
            Some(b) => to_js(&encode_batch_to_json_value(&b)).map_err(to_js_err),
            None => Ok(JsValue::NULL),
        }
    }

    /// Flush buffered deltas. Returns a delta batch object, or null if empty.
    pub fn flush(&mut self) -> Result<JsValue, JsError> {
        let _guard = self.install_context();
        match self.inner.flush() {
            Some(b) => to_js(&encode_batch_to_json_value(&b)).map_err(to_js_err),
            None => Ok(JsValue::NULL),
        }
    }

    /// Acknowledge a flushed batch by sequence number.
    pub fn ack(&mut self, sequence: u32) {
        self.inner.ack(sequence as u64);
    }

    /// Number of pending (unflushed) delta records.
    #[wasm_bindgen(getter, js_name = pendingCount)]
    pub fn pending_count(&self) -> u32 {
        self.inner.pending_count() as u32
    }

    /// Whether backpressure should be applied.
    #[wasm_bindgen(getter, js_name = isBackpressured)]
    pub fn is_backpressured(&self) -> bool {
        self.inner.is_backpressured()
    }

    /// Current cursor: latest processed block + hash. Null if no blocks processed.
    #[wasm_bindgen(getter)]
    pub fn cursor(&self) -> JsValue {
        match self.inner.latest_cursor() {
            Some(c) => {
                let cursor = WasmCursor {
                    number: c.number as u32,
                    hash: c.hash,
                };
                to_js(&cursor).unwrap_or(JsValue::NULL)
            }
            None => JsValue::NULL,
        }
    }

    /// Find the common ancestor between our state and the portal's chain.
    /// Returns the matching block cursor, or null if no common ancestor found.
    pub fn resolve_fork_cursor(&self, previous_blocks: JsValue) -> Result<JsValue, JsError> {
        let mut blocks: Vec<WasmCursor> =
            serde_wasm_bindgen::from_value(previous_blocks).map_err(to_js_err)?;
        // Sort DESC so we return the highest common ancestor
        blocks.sort_unstable_by(|a, b| b.number.cmp(&a.number));
        let refs: Vec<(u64, &str)> = blocks
            .iter()
            .map(|c| (c.number as u64, c.hash.as_str()))
            .collect();
        match self.inner.resolve_fork_cursor(&refs) {
            Some(c) => {
                let cursor = WasmCursor {
                    number: c.number as u32,
                    hash: c.hash,
                };
                to_js(&cursor).map_err(to_js_err)
            }
            None => Ok(JsValue::NULL),
        }
    }

    /// Atomically handle a fork (409 from Portal).
    ///
    /// Finds the common ancestor in `previousBlocks`, rolls back all state after
    /// that point, and returns `{ cursor, batch }`. Uses the internal finalized
    /// block — no need to pass it in.
    ///
    /// Throws if no common ancestor is found (fork too deep / unrecoverable).
    pub fn handle_fork(&mut self, previous_blocks: JsValue) -> Result<JsValue, JsError> {
        let blocks: Vec<WasmCursor> =
            serde_wasm_bindgen::from_value(previous_blocks).map_err(to_js_err)?;

        let chain: Vec<crate::types::BlockCursor> = blocks
            .into_iter()
            .map(|c| crate::types::BlockCursor {
                number: c.number as u64,
                hash: c.hash,
            })
            .collect();

        let result = self.inner.handle_fork(chain).map_err(to_js_err)?;

        let cursor = to_js(&WasmCursor {
            number: result.cursor.number as u32,
            hash: result.cursor.hash,
        })
        .map_err(to_js_err)?;

        let batch = match result.batch {
            Some(b) => to_js(&encode_batch_to_json_value(&b)).map_err(to_js_err)?,
            None => JsValue::NULL,
        };

        let obj = js_sys::Object::new();
        js_sys::Reflect::set(&obj, &"cursor".into(), &cursor)
            .unwrap_throw();
        js_sys::Reflect::set(&obj, &"batch".into(), &batch)
            .unwrap_throw();
        Ok(obj.into())
    }
}

impl DeltaDb {
    /// Install external callbacks as a thread-local context guard.
    fn install_context(&self) -> Option<WasmContextGuard> {
        if self.external_callbacks.is_empty() {
            return None;
        }
        Some(install_wasm_context(self.external_callbacks.clone()))
    }
}

// ─── Internal serde types ────────────────────────────────────────

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct WasmIngestInput {
    /// Table rows as plain JSON objects — converted to RowMap via json_object_to_row.
    data: std::collections::HashMap<String, Vec<serde_json::Value>>,
    rollback_chain: Option<Vec<WasmCursor>>,
    finalized_head: WasmCursor,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct WasmCursor {
    number: u32,
    hash: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct WasmStateField {
    name: String,
    column_type: String,
    default_value: String,
}

fn parse_column_type(s: &str) -> ColumnType {
    match s.to_lowercase().as_str() {
        "uint64" => ColumnType::UInt64,
        "int64" => ColumnType::Int64,
        "float64" => ColumnType::Float64,
        "string" => ColumnType::String,
        "boolean" => ColumnType::Boolean,
        "datetime" => ColumnType::DateTime,
        "uint256" => ColumnType::Uint256,
        "bytes" => ColumnType::Bytes,
        "json" => ColumnType::JSON,
        _ => ColumnType::String,
    }
}

fn to_js_err(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}
