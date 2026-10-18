pub mod event_rules;
pub mod external;
pub mod fn_reducer;
#[cfg(feature = "lua")]
pub mod lua;

use std::ops::Range;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::types::{ColumnId, ColumnRegistry, Row, Value};
#[cfg(test)]
use crate::types::RowMap;

/// The most distinct column names that the emits of a reducer use. Each
/// emitted row has a value for every one of them, so emit keys taken from
/// data would make every row as wide as the data has distinct keys.
pub const MAX_EMIT_COLUMNS: usize = 1024;

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
    /// The first column that found no room, which fails the block.
    overflow: Option<String>,
}

impl Emits {
    /// Emits whose first columns are `names`.
    pub fn with_columns(names: &[String]) -> Self {
        Self {
            columns: Arc::new(ColumnRegistry::new(names.to_vec())),
            ..Self::default()
        }
    }

    /// The position of column `name`, added on first use. None once the
    /// emits use `MAX_EMIT_COLUMNS` names: the engine then fails the block.
    pub fn column(&mut self, name: &str) -> Option<ColumnId> {
        if let Some(id) = self.columns.get_id(name) {
            return Some(id);
        }
        // Rows taken before keep the old registry, which has fewer columns.
        let id = (self.columns.len() < MAX_EMIT_COLUMNS)
            .then(|| Arc::make_mut(&mut self.columns).get_or_push(name))
            .flatten();
        if id.is_none() && self.overflow.is_none() {
            self.overflow = Some(name.to_string());
        }
        id
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
            if let Some(id) = self.column(name.as_ref()) {
                set_at(&mut values, id, value);
            }
        }
        self.push(values);
    }

    /// Err if an emit asked for a column past `MAX_EMIT_COLUMNS` since the
    /// last call.
    pub(crate) fn check(&mut self) -> Result<()> {
        match self.overflow.take() {
            None => Ok(()),
            Some(name) => Err(Error::Reducer(format!(
                "emit column '{name}' is past the limit of {MAX_EMIT_COLUMNS} distinct emit \
                 columns: the keys of an emit must be column names, not data"
            ))),
        }
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

    /// Drop the rows and the overflow of a failed block.
    pub(crate) fn clear(&mut self) {
        self.rows.clear();
        self.overflow = None;
    }

    /// Keep only the first `width` columns. Rows taken before keep their
    /// registry.
    pub(crate) fn truncate_columns(&mut self, width: usize) {
        if self.columns.len() > width {
            Arc::make_mut(&mut self.columns).truncate(width);
        }
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

/// The rows of one block by group key, the input of `process_grouped`.
pub struct GroupedRows<'a> {
    /// The rows of the block, in input order.
    pub rows: &'a [Row],
    /// Indices in `rows`, group by group; each group keeps input order.
    pub order: Vec<usize>,
    pub groups: Vec<GroupBatch>,
}

impl GroupedRows<'_> {
    /// The indices in `rows` of the rows of `group`.
    pub fn row_indices(&self, group: &GroupBatch) -> &[usize] {
        &self.order[group.rows.clone()]
    }
}

/// One group of a block, used by `process_grouped`.
pub struct GroupBatch {
    /// The group's state, in the order of the reducer's STATE fields.
    pub state: Vec<Value>,
    /// The group's range in `GroupedRows::order`.
    pub rows: Range<usize>,
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

    /// Process the groups of a block, each with its state and its rows in
    /// order. `fields` names the state values. Each emit goes into `emits`
    /// with the index in `block.rows` of the row that emitted it as its
    /// source.
    ///
    /// Only called when `use_batched_processing()` returns true.
    /// External (host-language) reducers override this for batch callbacks.
    fn process_grouped(
        &self,
        block: &mut GroupedRows<'_>,
        fields: &ColumnRegistry,
        emits: &mut Emits,
    ) -> Result<()> {
        let GroupedRows {
            rows,
            order,
            groups,
        } = block;
        for group in groups.iter_mut() {
            for &index in &order[group.rows.clone()] {
                emits.set_source(index);
                let state = GroupState {
                    values: &mut group.state,
                    fields,
                };
                self.process(state, &rows[index], emits)?;
                emits.check()?;
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

    /// Past `MAX_EMIT_COLUMNS` names a column gets no position; the block
    /// fails.
    #[test]
    fn emit_columns_stop_at_the_limit() {
        let mut emits = Emits::with_columns(&["user".to_string()]);
        for i in 1..MAX_EMIT_COLUMNS {
            assert_eq!(emits.column(&format!("c{i}")), Some(i as ColumnId));
        }
        emits.check().unwrap();

        assert_eq!(emits.column("one_more"), None);
        emits.push_named([
            ("user", Value::String("alice".into())),
            ("late", Value::UInt64(1)),
        ]);

        let err = emits.check().unwrap_err();
        assert!(err.to_string().contains("emit column 'one_more'"), "{err}");
        let rows = emits.take();
        assert_eq!(rows[0].1[0], Value::String("alice".into()));
        assert_eq!(emits.columns().len(), MAX_EMIT_COLUMNS);
    }
}
