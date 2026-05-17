pub mod db;
pub mod change;
pub mod engine;
pub mod error;
pub mod json_conv;
pub mod msgpack_conv;
#[cfg(feature = "napi")]
mod napi;
pub mod pipeline;
pub mod reducer_runtime;
pub mod schema;
pub mod storage;
pub mod test_helpers;
pub mod types;
#[cfg(feature = "wasm")]
mod wasm;

// Flat re-exports of the canonical Pipeline builder API so callers can
// `use settle::{Pipeline, uint64, ...};` without nested paths.
pub use pipeline::{
    AggExpr, AggFn, AggProxy, BuildOptions, ColumnType, GroupByItem, IntervalExpr, KeyRef, MEMORY,
    Pipeline, Projection, ReduceFn, ReducerHandle, ReducerOptions, SelectFn, SlidingWindowOptions,
    StateCtx, TableHandle, ViewHandle, ViewOptions, base58, boolean, bytes, datetime, float64,
    int64, interval, json, parse_duration, string, uint256, uint64,
};
