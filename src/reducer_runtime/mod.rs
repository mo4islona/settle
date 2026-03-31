pub mod event_rules;
pub mod external;
pub mod fn_reducer;
#[cfg(feature = "lua")]
pub mod lua;

use std::collections::HashMap;

use crate::error::Result;
use crate::types::{Row, RowMap, Value};

/// A batch of rows for a single group key, used by `process_grouped`.
///
/// The runtime processes rows in order, updating state, and appending emits.
pub struct GroupBatch {
    pub state: HashMap<String, Value>,
    pub rows: Vec<Row>,
    /// Output rows produced by the runtime. Filled during `process_grouped`.
    pub emits: Vec<RowMap>,
}

/// Trait for reducer process logic runtimes.
///
/// A runtime evaluates the reducer's process function for a single row,
/// reading and mutating state, and producing output (emit) rows.
///
/// Returns Vec<RowMap> since Lua reducers can emit multiple rows per input.
pub trait ReducerRuntime: Send {
    /// Process one input row against the current state.
    ///
    /// - `state`: mutable reducer state (read + write)
    /// - `row`: the input row from the source table (indexed access via ColumnRegistry)
    ///
    /// Returns zero or more output rows (the emitted columns).
    fn process(&self, state: &mut HashMap<String, Value>, row: &Row) -> Result<Vec<RowMap>>;

    /// Whether this runtime benefits from grouped/batched processing.
    ///
    /// When true, `ReducerEngine::process_block` groups all rows by key and
    /// calls `process_grouped` once per block. This avoids per-row FFI overhead
    /// for external (host-language) reducers.
    ///
    /// When false (default), the engine uses the faster per-row loop that avoids
    /// cloning rows into GroupBatch.
    fn use_batched_processing(&self) -> bool {
        false
    }

    /// Process a batch of groups. Each group has its state and ordered rows.
    ///
    /// Only called when `use_batched_processing()` returns true.
    /// External (host-language) reducers override this for batch callbacks.
    fn process_grouped(&self, groups: &mut [GroupBatch]) -> Result<()> {
        for group in groups.iter_mut() {
            for row in &group.rows {
                let emits = self.process(&mut group.state, row)?;
                group.emits.extend(emits);
            }
        }
        Ok(())
    }
}
