pub mod event_rules;
pub mod external;
pub mod fn_reducer;
#[cfg(feature = "lua")]
pub mod lua;

use std::sync::Arc;

use crate::error::Result;
use crate::types::{ColumnId, ColumnRegistry, Row, RowMap, Value};

/// The state of one group: a value per STATE field of the reducer, in the
/// order of `fields`.
pub struct GroupState<'a> {
    pub values: &'a mut [Value],
    pub fields: &'a ColumnRegistry,
}

impl GroupState<'_> {
    /// A state field by name; None if the reducer has no such field.
    pub fn get(&self, name: &str) -> Option<&Value> {
        let id = self.fields.get_id(name)?;
        Some(&self.values[id as usize])
    }

    /// Set a state field by name. False, with no change, if the reducer has
    /// no such field.
    pub fn set(&mut self, name: &str, value: Value) -> bool {
        let Some(id) = self.fields.get_id(name) else {
            return false;
        };
        self.values[id as usize] = value;
        true
    }
}

/// The rows that a reducer emits, by column position. The columns are the
/// names that its emits used, in the order of first use; a column that a
/// row did not set is Null.
#[derive(Default)]
pub struct Emits {
    columns: Arc<ColumnRegistry>,
    rows: Vec<(usize, Vec<Value>)>,
    /// The input row that the next emit comes from.
    source: usize,
}

impl Emits {
    /// Emits whose first columns are `names`.
    pub fn with_columns(names: &[String]) -> Self {
        Self {
            columns: Arc::new(ColumnRegistry::new(names.to_vec())),
            ..Self::default()
        }
    }

    /// The position of column `name`, added on first use.
    pub fn column(&mut self, name: &str) -> ColumnId {
        match self.columns.get_id(name) {
            Some(id) => id,
            // Rows taken before keep the old registry, which has fewer columns.
            None => Arc::make_mut(&mut self.columns).get_or_push(name),
        }
    }

    pub fn columns(&self) -> &Arc<ColumnRegistry> {
        &self.columns
    }

    /// Emit a row by column position; it may be shorter than the columns.
    pub fn push(&mut self, values: Vec<Value>) {
        self.rows.push((self.source, values));
    }

    /// Emit a row by column name.
    pub fn push_named<K: AsRef<str>>(&mut self, fields: impl IntoIterator<Item = (K, Value)>) {
        let mut values = Vec::with_capacity(self.columns.len());
        for (name, value) in fields {
            set_at(&mut values, self.column(name.as_ref()), value);
        }
        self.push(values);
    }

    pub(crate) fn set_source(&mut self, source: usize) {
        self.source = source;
    }

    /// The emitted rows, each with the input row that it came from, padded
    /// to all the columns.
    pub(crate) fn take(&mut self) -> Vec<(usize, Vec<Value>)> {
        let width = self.columns.len();
        let mut rows = std::mem::take(&mut self.rows);
        for (_, values) in &mut rows {
            values.resize(width, Value::Null);
        }
        rows
    }

    pub(crate) fn clear(&mut self) {
        self.rows.clear();
    }
}

/// `values[id] = value`, with Nulls up to `id`.
pub fn set_at(values: &mut Vec<Value>, id: ColumnId, value: Value) {
    let id = id as usize;
    if values.len() <= id {
        values.resize(id + 1, Value::Null);
    }
    values[id] = value;
}

/// `process` with the state and the emits by name, for tests. The state
/// fields are `fields`, else the keys of `state` in name order.
#[cfg(test)]
pub(crate) fn run_by_name(
    runtime: &dyn ReducerRuntime,
    fields: Option<&[String]>,
    state: &mut RowMap,
    row: &Row,
) -> Result<Vec<RowMap>> {
    let names = match fields {
        Some(fields) => fields.to_vec(),
        None => {
            let mut names: Vec<String> = state.keys().cloned().collect();
            names.sort();
            names
        }
    };
    let fields = ColumnRegistry::new(names);
    let mut values: Vec<Value> = fields
        .names()
        .iter()
        .map(|n| state.get(n).cloned().unwrap_or(Value::Null))
        .collect();

    let mut emits = Emits::default();
    let group = GroupState {
        values: &mut values,
        fields: &fields,
    };
    runtime.process(group, row, &mut emits)?;

    for (name, value) in fields.names().iter().zip(values) {
        state.insert(name.clone(), value);
    }
    let columns = emits.columns().clone();
    let rows = emits.take().into_iter().map(|(_, values)| {
        columns
            .names()
            .iter()
            .cloned()
            .zip(values)
            .filter(|(_, v)| !v.is_null())
            .collect()
    });
    Ok(rows.collect())
}

/// A batch of rows for a single group key, used by `process_grouped`.
///
/// The runtime processes rows in order, updating state, and appending emits.
pub struct GroupBatch {
    /// The group's state, in the order of the reducer's STATE fields.
    pub state: Vec<Value>,
    pub rows: Vec<Row>,
    /// Output rows produced by the runtime, each with the index in `rows` of
    /// the row that emitted it. Filled during `process_grouped`. By name: a
    /// host builds them from its own objects.
    pub emits: Vec<(usize, RowMap)>,
}

/// Trait for reducer process logic runtimes.
///
/// A runtime evaluates the reducer's process function for a single row,
/// reading and mutating state, and producing output (emit) rows.
pub trait ReducerRuntime: Send {
    /// Process one input row against the current state of its group, and
    /// push zero or more output rows into `emits`.
    fn process(&self, state: GroupState<'_>, row: &Row, emits: &mut Emits) -> Result<()>;

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

    /// Whether `process_grouped` needs a host (JS) callback that the
    /// callback context on the calling thread does not hold.
    fn missing_host_callback(&self) -> bool {
        false
    }

    /// Process a batch of groups. Each group has its state and ordered rows.
    /// `fields` names the state values.
    ///
    /// Only called when `use_batched_processing()` returns true.
    /// External (host-language) reducers override this for batch callbacks.
    fn process_grouped(&self, groups: &mut [GroupBatch], fields: &ColumnRegistry) -> Result<()> {
        let mut emits = Emits::default();
        for group in groups.iter_mut() {
            for (index, row) in group.rows.iter().enumerate() {
                let state = GroupState {
                    values: &mut group.state,
                    fields,
                };
                self.process(state, row, &mut emits)?;

                let columns = emits.columns().clone();
                for (_, values) in emits.take() {
                    let emit = columns
                        .names()
                        .iter()
                        .cloned()
                        .zip(values)
                        .filter(|(_, v)| !v.is_null())
                        .collect();
                    group.emits.push((index, emit));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A column added in a later block extends the rows from then on; rows
    /// taken before keep the registry that they had.
    #[test]
    fn emit_columns_grow_by_first_use() {
        let mut emits = Emits::with_columns(&["user".to_string()]);
        emits.set_source(4);
        emits.push_named([("amount", Value::UInt64(1))]);
        let before = emits.columns().clone();
        let first = emits.take();
        assert_eq!(first, [(4, vec![Value::Null, Value::UInt64(1)])]);

        emits.set_source(0);
        emits.push(vec![Value::String("a".into())]);
        emits.push_named([
            ("late", Value::UInt64(2)),
            ("user", Value::String("b".into())),
        ]);
        let rows = emits.take();

        assert_eq!(before.names(), ["user", "amount"]);
        assert_eq!(emits.columns().names(), ["user", "amount", "late"]);
        assert_eq!(
            rows,
            [
                (0, vec![Value::String("a".into()), Value::Null, Value::Null]),
                (
                    0,
                    vec![Value::String("b".into()), Value::Null, Value::UInt64(2)]
                ),
            ]
        );
    }
}
