//! Fluent Pipeline builder — Rust 1-to-1 mirror of `@settle/stream`'s
//! `Pipeline` API.
//!
//! Compose tables, reducers and materialized views with chained handles, then
//! call [`Pipeline::build`] to generate DDL, open [`crate::db::Settle`], and
//! auto-register every reducer's callback in one call. The generated DDL is
//! the same shape produced by the TypeScript builder, so the existing
//! `src/schema/parser.rs` accepts it without changes.
//!
//! # Example
//!
//! ```no_run
//! use settle::pipeline::{
//!     Pipeline, BuildOptions, ReducerOptions, ViewOptions,
//!     uint64, string, datetime, interval,
//! };
//! use settle::types::{RowMap, Value};
//!
//! let mut p = Pipeline::new();
//!
//! let orders = p.table("orders", [
//!     ("block_number", uint64()),
//!     ("trader",       string()),
//!     ("asset_id",     string()),
//!     ("usdc",         uint64()),
//!     ("ts",           datetime()),
//! ]);
//!
//! let stats = orders.create_reducer("market_stats", ReducerOptions {
//!     group_by: vec!["asset_id".into()],
//!     initial_state: RowMap::from([
//!         ("volume".into(), Value::Float64(0.0)),
//!         ("trades".into(), Value::UInt64(0)),
//!     ]),
//!     reduce: Box::new(|state, row| {
//!         let usdc = row.get("usdc").and_then(Value::as_f64).unwrap_or(0.0);
//!         let vol  = usdc / 1_000_000.0;
//!         let new_volume = state.get_f64("volume") + vol;
//!         let new_trades = state.get_u64("trades") + 1;
//!         state.update(RowMap::from([
//!             ("volume".into(), Value::Float64(new_volume)),
//!             ("trades".into(), Value::UInt64(new_trades)),
//!         ]));
//!         state.emit(RowMap::from([
//!             ("asset_id".into(),
//!                 row.get("asset_id").cloned().unwrap_or(Value::Null)),
//!             ("volume_running".into(), Value::Float64(new_volume)),
//!         ]));
//!     }),
//! });
//!
//! stats.create_view("token_summary", ViewOptions {
//!     group_by: vec!["asset_id".into()],
//!     sliding_window: None,
//!     select: Box::new(|agg| vec![
//!         ("asset_id".into(),     agg.key("asset_id").into()),
//!         ("total_volume".into(), agg.sum("volume_running").into()),
//!         ("last_volume".into(),  agg.last("volume_running").into()),
//!         ("event_count".into(),  agg.count().into()),
//!     ]),
//! });
//!
//! let _db = p.build(BuildOptions::new()).unwrap();
//! ```

pub mod codegen;
pub mod column;
pub mod ddl;
pub mod handles;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::db::{Config, Settle};
use crate::error::{Error, Result};
use crate::reducer_runtime::fn_reducer::FnReducerRuntime;
use crate::types::{Row, Value};

pub use column::{
    ColumnType, base58, boolean, bytes, datetime, float64, int64, json, string, uint256, uint64,
};
pub use ddl::{
    AggExpr, AggFn, AggProxy, GroupByItem, IntervalExpr, KeyRef, Projection, ReduceFn,
    ReducerOptions, SelectFn, SlidingWindowOptions, StateCtx, ViewOptions, interval,
    parse_duration,
};
pub use handles::{ReducerHandle, TableHandle, ViewHandle};

use codegen::{ColumnSpec, ReducerSpec, TableSpec, ViewSpec};

/// Options consumed by [`Pipeline::build`]. Mirrors TS
/// `Pipeline.build({ dataDir, maxBufferSize, compression, ... })`.
///
/// Use the chainable setters for the common case:
///
/// ```no_run
/// # use settle::BuildOptions;
/// let _ = BuildOptions::new()
///     .data_dir("/var/lib/settle")    // or ":memory:" for in-memory
///     .compression("zstd")
///     .cache_size(256 * 1024 * 1024);
/// ```
#[derive(Debug, Clone, Default)]
pub struct BuildOptions {
    pub data_dir: Option<String>,
    pub max_buffer_size: Option<usize>,
    /// `"none"` | `"snappy"` | `"zstd"` | `"lz4"`.
    pub compression: Option<String>,
    pub disable_compaction: bool,
    pub cache_size: Option<usize>,
}

pub use crate::db::MEMORY;

impl BuildOptions {
    /// Empty options — equivalent to `BuildOptions::new()`. Use for
    /// symmetry with `Config::new(...)` so both APIs read the same:
    ///
    /// ```no_run
    /// # use settle::{BuildOptions, Pipeline};
    /// # let mut p = Pipeline::new();
    /// let _db = p.build(BuildOptions::new().data_dir("/tmp/settle"));
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the on-disk RocksDB directory. Pass [`MEMORY`] (`":memory:"`) to
    /// keep everything in memory — same convention as SQLite.
    pub fn data_dir(mut self, path: impl Into<String>) -> Self {
        let p = path.into();
        self.data_dir = if p == MEMORY { None } else { Some(p) };
        self
    }

    /// Buffer size before backpressure / forced flush kicks in.
    pub fn max_buffer_size(mut self, n: usize) -> Self {
        self.max_buffer_size = Some(n);
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

/// Top-level builder. Hold tables/reducers/views and produce a [`Settle`].
pub struct Pipeline {
    inner: Rc<RefCell<PipelineInner>>,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(PipelineInner::default())),
        }
    }

    /// Declare a raw table. `columns` accepts any iterable of
    /// `(name, ColumnType)` pairs (arrays, slices, vectors).
    pub fn table<I, S>(&mut self, name: impl Into<String>, columns: I) -> TableHandle
    where
        I: IntoIterator<Item = (S, ColumnType)>,
        S: Into<String>,
    {
        let table_name = name.into();
        let cols: Vec<ColumnSpec> = columns
            .into_iter()
            .map(|(n, ct)| ColumnSpec {
                name: n.into(),
                sql_type: ct.sql,
            })
            .collect();
        self.inner.borrow_mut().tables.push(TableSpec {
            name: table_name.clone(),
            columns: cols,
            virtual_table: false,
        });
        TableHandle::new(self.inner.clone(), table_name)
    }

    /// Declare a VIRTUAL table — fed to reducers/MVs but no change records.
    pub fn virtual_table<I, S>(&mut self, name: impl Into<String>, columns: I) -> TableHandle
    where
        I: IntoIterator<Item = (S, ColumnType)>,
        S: Into<String>,
    {
        let h = self.table(name, columns);
        h.set_virtual(true)
    }

    /// Generate DDL for the current pipeline (for inspection / debugging).
    /// Identical to the string passed into `Settle::open` by [`Self::build`].
    pub fn to_ddl(&self) -> Result<String> {
        self.inner.borrow().to_ddl()
    }

    /// Open Settle, register every reducer's callback, return the database.
    pub fn build(self, opts: BuildOptions) -> Result<Settle> {
        // Drop the outer Rc so we hold the only reference (handles dropped by
        // user already, or kept around — borrow_mut still works either way).
        let mut inner = self.inner.borrow_mut();

        let schema = inner.to_ddl()?;
        let mut config = Config::new(schema);
        if let Some(s) = opts.max_buffer_size {
            config = config.max_buffer_size(s);
        }
        config.data_dir = opts.data_dir;
        config.compression = opts.compression;
        config.disable_compaction = opts.disable_compaction;
        config.cache_size = opts.cache_size;

        let mut db = Settle::open(config)?;

        // Drain reducer closures and install each as a runtime override.
        let reducers = std::mem::take(&mut inner.reducers);
        let mut closures = std::mem::take(&mut inner.reduce_fns);
        for spec in reducers {
            let reduce = closures.remove(&spec.name).ok_or_else(|| {
                Error::InvalidOperation(format!(
                    "Pipeline::build: missing reduce closure for '{}'",
                    spec.name
                ))
            })?;
            let runtime =
                FnReducerRuntime::new(move |state: &mut HashMap<String, Value>, row: &Row| {
                    let mut ctx = StateCtx::new(state);
                    reduce(&mut ctx, row);
                    ctx.into_emits()
                });
            db.register_reducer_callback(&spec.name, Box::new(runtime))?;
        }

        Ok(db)
    }
}

/// Shared mutable state for [`Pipeline`] and its handles.
#[derive(Default)]
pub struct PipelineInner {
    tables: Vec<TableSpec>,
    reducers: Vec<ReducerSpec>,
    views: Vec<ViewSpec>,
    /// Reducer name → reduce callback (drained into runtimes by `build()`).
    reduce_fns: HashMap<String, ReduceFn>,
}

impl PipelineInner {
    pub(crate) fn mark_virtual(&mut self, name: &str, value: bool) {
        if let Some(t) = self.tables.iter_mut().find(|t| t.name == name) {
            t.virtual_table = value;
        }
    }

    pub(crate) fn add_reducer(
        &mut self,
        name: String,
        source: String,
        opts: ReducerOptions,
    ) {
        let state_fields = match codegen::infer_state_fields(&opts.initial_state) {
            Ok(f) => f,
            Err(e) => panic!("Pipeline::create_reducer('{name}'): {e}"),
        };
        self.reducers.push(ReducerSpec {
            name: name.clone(),
            source,
            group_by: opts.group_by,
            state_fields,
        });
        self.reduce_fns.insert(name, opts.reduce);
    }

    pub(crate) fn add_view(&mut self, name: String, source: String, opts: ViewOptions) {
        // Run the select callback eagerly (matches TS, which calls viewToSql at
        // create time). The user's closure is FnOnce — invoke it now.
        let proxy = AggProxy;
        let projections = (opts.select)(&proxy);
        self.views.push(ViewSpec {
            name,
            source,
            group_by: opts.group_by,
            sliding_window: opts.sliding_window,
            projections,
        });
    }

    pub(crate) fn to_ddl(&self) -> Result<String> {
        let mut parts: Vec<String> = Vec::with_capacity(
            self.tables.len() + self.reducers.len() + self.views.len(),
        );
        for t in &self.tables {
            parts.push(codegen::table_to_sql(t));
        }
        for r in &self.reducers {
            parts.push(codegen::reducer_to_sql(r));
        }
        for v in &self.views {
            parts.push(codegen::view_to_sql(v)?);
        }
        Ok(parts.join("\n"))
    }
}

