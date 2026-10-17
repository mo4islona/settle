use crate::error::Result;
use crate::types::{ColumnId, Row, RowMap, Value};

use super::{Emits, GroupState, ReducerRuntime};

/// Mutable context handed to `reduce(state, row)` closures.
///
/// Wraps the state of the row's group and the reducer's output. Mirrors TS
/// `ReducerCtx<TState, TEmit>` (`state.update()`, `state.emit()`, plus typed
/// read accessors). Access by name looks the field up; `values` and
/// `emit_values` work by position.
pub struct StateCtx<'a> {
    state: GroupState<'a>,
    emits: &'a mut Emits,
}

impl<'a> StateCtx<'a> {
    pub(crate) fn new(state: GroupState<'a>, emits: &'a mut Emits) -> Self {
        Self { state, emits }
    }

    /// Replace the entire state: each STATE field takes its value in
    /// `new_state`, or Null. Names that STATE lacks are ignored.
    pub fn update(&mut self, mut new_state: RowMap) {
        let names = self.state.fields.names();
        for (value, name) in self.state.values.iter_mut().zip(names) {
            *value = new_state.remove(name).unwrap_or(Value::Null);
        }
    }

    /// Set a single state field without touching the others. A name that
    /// STATE lacks is ignored.
    pub fn set(&mut self, name: &str, value: Value) {
        self.state.set(name, value);
    }

    /// Emit a row into the reducer's output stream.
    pub fn emit(&mut self, row: RowMap) {
        self.emits.push_named(row);
    }

    /// The output position of column `name`, for `emit_values`.
    pub fn column(&mut self, name: &str) -> ColumnId {
        self.emits.column(name)
    }

    /// Emit a row by output position (`column`); it may be shorter than the
    /// columns.
    pub fn emit_values(&mut self, values: Vec<Value>) {
        self.emits.push(values);
    }

    /// Read a state field by name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.state.get(name)
    }

    pub fn get_f64(&self, name: &str) -> f64 {
        self.get(name).and_then(Value::as_f64).unwrap_or(0.0)
    }
    pub fn get_i64(&self, name: &str) -> i64 {
        self.get(name).and_then(Value::as_i64).unwrap_or(0)
    }
    pub fn get_u64(&self, name: &str) -> u64 {
        self.get(name).and_then(Value::as_u64).unwrap_or(0)
    }
    pub fn get_bool(&self, name: &str) -> bool {
        self.get(name).and_then(Value::as_bool).unwrap_or(false)
    }
    pub fn get_str(&self, name: &str) -> &str {
        self.get(name).and_then(Value::as_str).unwrap_or("")
    }

    /// The state values, in the order of the STATE fields.
    pub fn values(&mut self) -> &mut [Value] {
        self.state.values
    }
}

type ReduceFn = dyn Fn(&mut StateCtx<'_>, &Row) + Send + Sync;

/// A reducer runtime backed by a Rust closure.
pub struct FnReducerRuntime {
    process_fn: Box<ReduceFn>,
}

impl FnReducerRuntime {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&mut StateCtx<'_>, &Row) + Send + Sync + 'static,
    {
        Self {
            process_fn: Box::new(f),
        }
    }
}

impl ReducerRuntime for FnReducerRuntime {
    fn process(&self, state: GroupState<'_>, row: &Row, emits: &mut Emits) -> Result<()> {
        let mut ctx = StateCtx::new(state, emits);
        (self.process_fn)(&mut ctx, row);
        Ok(())
    }
}
