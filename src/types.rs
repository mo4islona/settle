use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub type BlockNumber = u64;

/// Column index within a ColumnRegistry.
pub type ColumnId = u16;

/// Maps column names to integer indices and vice versa.
/// Created once per table/pipeline stage; shared via Arc across all Rows.
#[derive(Debug, Clone)]
pub struct ColumnRegistry {
    names: Vec<String>,
    indices: HashMap<String, ColumnId>,
}

impl ColumnRegistry {
    pub fn new(names: Vec<String>) -> Self {
        let indices = names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as ColumnId))
            .collect();
        Self { names, indices }
    }

    pub fn get_id(&self, name: &str) -> Option<ColumnId> {
        self.indices.get(name).copied()
    }

    pub fn get_name(&self, id: ColumnId) -> Option<&str> {
        self.names.get(id as usize).map(|s| s.as_str())
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }
}

/// Optimized row representation using column indices instead of string keys.
/// Each Row carries an Arc to its ColumnRegistry so it is self-describing.
#[derive(Debug, Clone)]
pub struct Row {
    registry: Arc<ColumnRegistry>,
    values: Vec<Value>,
}

impl Row {
    /// Create a new row with all values initialized to Null.
    pub fn new(registry: Arc<ColumnRegistry>) -> Self {
        let len = registry.len();
        Self {
            registry,
            values: vec![Value::Null; len],
        }
    }

    /// Create a Row from a pre-built values vector (must match registry length).
    pub fn from_values(registry: Arc<ColumnRegistry>, values: Vec<Value>) -> Self {
        assert_eq!(
            values.len(),
            registry.len(),
            "Row::from_values: values length {} != registry length {}",
            values.len(),
            registry.len()
        );
        Self { registry, values }
    }

    /// Create a Row from a HashMap, setting only columns present in the registry.
    pub fn from_map(registry: Arc<ColumnRegistry>, map: &HashMap<String, Value>) -> Self {
        let mut row = Self::new(registry);
        for (k, v) in map {
            row.set(k, v.clone());
        }
        row
    }

    /// `from_map` that moves the values out of the map.
    pub fn from_owned_map(registry: Arc<ColumnRegistry>, map: HashMap<String, Value>) -> Self {
        let mut row = Self::new(registry);
        for (k, v) in map {
            if let Some(id) = row.registry.get_id(&k) {
                row.values[id as usize] = v;
            }
        }
        row
    }

    /// Look up a column value by name. Returns None for unknown columns or Null values.
    pub fn get(&self, name: &str) -> Option<&Value> {
        let id = self.registry.get_id(name)?;
        let val = &self.values[id as usize];
        if val.is_null() { None } else { Some(val) }
    }

    /// Set a column value by name. No-op if the column is not in the registry.
    pub fn set(&mut self, name: &str, value: Value) {
        if let Some(id) = self.registry.get_id(name) {
            self.values[id as usize] = value;
        }
    }

    /// Convert to a HashMap (for ChangeRecord construction). Skips Null values.
    pub fn to_map(&self) -> HashMap<String, Value> {
        self.registry
            .names
            .iter()
            .zip(self.values.iter())
            .filter(|(_, v)| !v.is_null())
            .map(|(n, v)| (n.clone(), v.clone()))
            .collect()
    }

    /// Access the raw values slice (for serialization).
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    /// Access the column registry.
    pub fn registry(&self) -> &Arc<ColumnRegistry> {
        &self.registry
    }

    /// Iterate over (name, value) pairs, skipping Nulls.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.registry
            .names
            .iter()
            .zip(self.values.iter())
            .filter(|(_, v)| !v.is_null())
            .map(|(n, v)| (n.as_str(), v))
    }

    /// Iterate over all (name, value) pairs, including Nulls.
    pub fn iter_all(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.registry
            .names
            .iter()
            .zip(self.values.iter())
            .map(|(n, v)| (n.as_str(), v))
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.registry, &other.registry) {
            // Same registry: direct value comparison
            return self.values == other.values;
        }
        // Different registries: compare non-null fields by name
        let self_count = self.values.iter().filter(|v| !v.is_null()).count();
        let other_count = other.values.iter().filter(|v| !v.is_null()).count();
        if self_count != other_count {
            return false;
        }
        for (name, val) in self.iter() {
            match other.get(name) {
                Some(other_val) if val == other_val => {}
                _ => return false,
            }
        }
        true
    }
}

impl Eq for Row {}

impl From<RowMap> for Row {
    fn from(map: RowMap) -> Self {
        let mut names: Vec<String> = map.keys().cloned().collect();
        names.sort();
        let registry = Arc::new(ColumnRegistry::new(names));
        Self::from_map(registry, &map)
    }
}

/// A HashMap-based row representation used for ChangeRecord fields,
/// reducer state, and API boundaries where column names are dynamic.
pub type RowMap = HashMap<String, Value>;

/// A block reference: number + hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockCursor {
    pub number: BlockNumber,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnType {
    UInt64,
    Int64,
    Float64,
    Uint256,
    String,
    DateTime,
    Boolean,
    Bytes,
    Base58,
    JSON,
}

impl ColumnType {
    pub fn default_value(&self) -> Value {
        match self {
            ColumnType::UInt64 => Value::UInt64(0),
            ColumnType::Int64 => Value::Int64(0),
            ColumnType::Float64 => Value::Float64(0.0),
            ColumnType::Uint256 => Value::Uint256([0u8; 32]),
            ColumnType::String => Value::String(String::new()),
            ColumnType::DateTime => Value::DateTime(0),
            ColumnType::Boolean => Value::Boolean(false),
            ColumnType::Bytes => Value::Bytes(Vec::new()),
            ColumnType::Base58 => Value::Base58(Vec::new()),
            ColumnType::JSON => Value::JSON(serde_json::Value::Null),
        }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnType::UInt64 => write!(f, "UInt64"),
            ColumnType::Int64 => write!(f, "Int64"),
            ColumnType::Float64 => write!(f, "Float64"),
            ColumnType::Uint256 => write!(f, "Uint256"),
            ColumnType::String => write!(f, "String"),
            ColumnType::DateTime => write!(f, "DateTime"),
            ColumnType::Boolean => write!(f, "Boolean"),
            ColumnType::Bytes => write!(f, "Bytes"),
            ColumnType::Base58 => write!(f, "Base58"),
            ColumnType::JSON => write!(f, "JSON"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    UInt64(u64),
    Int64(i64),
    Float64(f64),
    /// 256-bit unsigned integer, stored as 32 bytes big-endian.
    Uint256([u8; 32]),
    String(String),
    DateTime(i64),
    Boolean(bool),
    Bytes(Vec<u8>),
    /// Base58-encoded byte string (Solana addresses, Bitcoin addresses, IPFS CIDs).
    Base58(Vec<u8>),
    /// Structured JSON data, pushed to Lua as native tables (no json.decode/encode needed).
    JSON(serde_json::Value),
    Null,
}

impl Value {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::UInt64(v) => Some(*v as f64),
            Value::Int64(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            Value::DateTime(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::UInt64(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            Value::Float64(v) => Some(*v as i64),
            Value::DateTime(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::UInt64(v) => Some(*v),
            Value::Int64(v) => Some(*v as u64),
            Value::Float64(v) => Some(*v as u64),
            Value::DateTime(v) => Some(*v as u64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::UInt64(_) => "UInt64",
            Value::Int64(_) => "Int64",
            Value::Float64(_) => "Float64",
            Value::String(_) => "String",
            Value::DateTime(_) => "DateTime",
            Value::Boolean(_) => "Boolean",
            Value::Null => "Null",
            Value::Bytes(_) => "Bytes",
            Value::Uint256(_) => "Uint256",
            Value::Base58(_) => "Base58",
            Value::JSON(_) => "JSON",
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Boolean(v) => *v,
            Value::UInt64(v) => *v != 0,
            Value::Int64(v) => *v != 0,
            Value::Float64(v) => *v != 0.0,
            Value::String(v) => !v.is_empty(),
            Value::JSON(v) => !v.is_null(),
            Value::Null => false,
            _ => true,
        }
    }

    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Value::UInt64(_) => Some(ColumnType::UInt64),
            Value::Int64(_) => Some(ColumnType::Int64),
            Value::Float64(_) => Some(ColumnType::Float64),
            Value::Uint256(_) => Some(ColumnType::Uint256),
            Value::String(_) => Some(ColumnType::String),
            Value::DateTime(_) => Some(ColumnType::DateTime),
            Value::Boolean(_) => Some(ColumnType::Boolean),
            Value::Bytes(_) => Some(ColumnType::Bytes),
            Value::Base58(_) => Some(ColumnType::Base58),
            Value::JSON(_) => Some(ColumnType::JSON),
            Value::Null => None,
        }
    }
}

// We need Eq + Hash for group-by keys. Float64 uses bit-level equality.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::UInt64(a), Value::UInt64(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Uint256(a), Value::Uint256(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::DateTime(a), Value::DateTime(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Base58(a), Value::Base58(b)) => a == b,
            (Value::JSON(a), Value::JSON(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::UInt64(v) => v.hash(state),
            Value::Int64(v) => v.hash(state),
            Value::Float64(v) => v.to_bits().hash(state),
            Value::Uint256(v) => v.hash(state),
            Value::String(v) => v.hash(state),
            Value::DateTime(v) => v.hash(state),
            Value::Boolean(v) => v.hash(state),
            Value::Bytes(v) => v.hash(state),
            Value::Base58(v) => v.hash(state),
            Value::JSON(v) => hash_json(v, state),
            Value::Null => {}
        }
    }
}

/// Hash a serde_json::Value without allocating a String.
fn hash_json<H: Hasher>(v: &serde_json::Value, state: &mut H) {
    match v {
        serde_json::Value::Null => 0u8.hash(state),
        serde_json::Value::Bool(b) => {
            1u8.hash(state);
            b.hash(state);
        }
        serde_json::Value::Number(n) => {
            2u8.hash(state);
            n.hash(state);
        }
        serde_json::Value::String(s) => {
            3u8.hash(state);
            s.hash(state);
        }
        serde_json::Value::Array(arr) => {
            4u8.hash(state);
            arr.len().hash(state);
            for item in arr {
                hash_json(item, state);
            }
        }
        serde_json::Value::Object(obj) => {
            5u8.hash(state);
            obj.len().hash(state);
            for (k, v) in obj {
                k.hash(state);
                hash_json(v, state);
            }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::UInt64(a), Value::UInt64(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::Uint256(a), Value::Uint256(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.partial_cmp(b),
            (Value::Base58(a), Value::Base58(b)) => a.partial_cmp(b),
            (Value::JSON(_), Value::JSON(_)) => None,
            (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::UInt64(v) => write!(f, "{v}"),
            Value::Int64(v) => write!(f, "{v}"),
            Value::Float64(v) => write!(f, "{v}"),
            Value::Uint256(v) => {
                write!(f, "0x")?;
                for byte in v {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            Value::String(v) => write!(f, "{v}"),
            Value::DateTime(v) => write!(f, "{v}"),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::Bytes(v) => write!(f, "{v:?}"),
            Value::Base58(v) => {
                // Display raw bytes as hex; actual base58 encoding is done at the boundary
                write!(f, "base58:{v:?}")
            }
            Value::JSON(v) => write!(f, "{v}"),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// A composite key used for GROUP BY lookups.
/// SmallVec avoids heap allocation for the common case of 1-2 group-by columns.
/// N=2 not 4: Value is ~32 bytes, so [Value; 4] = ~144 bytes per FxHashMap key,
/// which hurts cache locality. [Value; 2] = ~80 bytes covers most schemas while
/// keeping hash maps compact.
pub type GroupKey = SmallVec<[Value; 2]>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

/// Where a column of a change record lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    Key(usize),
    Value(usize),
}

static NULL: Value = Value::Null;

/// The column layout of the change records of one table. All records of a
/// table share one schema, so a record holds only its values, by position.
#[derive(Debug, PartialEq, Eq)]
pub struct ChangeSchema {
    table: String,
    key_columns: Box<[String]>,
    columns: Box<[(String, Slot)]>,
}

impl ChangeSchema {
    /// A raw table: the row is the values, and its block and its index in
    /// the block are the key.
    pub fn raw_table(table: &str, columns: &[String]) -> Self {
        Self {
            table: table.to_string(),
            key_columns: ["block_number".to_string(), "_row_index".to_string()].into(),
            columns: columns
                .iter()
                .enumerate()
                .map(|(i, name)| (name.clone(), Slot::Value(i)))
                .collect(),
        }
    }

    /// An MV: `columns` in SELECT order, each with whether it is a GROUP BY
    /// column. GROUP BY columns are the key, the others the values.
    pub fn mv(table: &str, columns: impl IntoIterator<Item = (String, bool)>) -> Self {
        let mut key_columns = Vec::new();
        let mut value_count = 0;

        let columns = columns
            .into_iter()
            .map(|(name, is_key)| {
                let slot = if is_key {
                    key_columns.push(name.clone());
                    Slot::Key(key_columns.len() - 1)
                } else {
                    value_count += 1;
                    Slot::Value(value_count - 1)
                };
                (name, slot)
            })
            .collect();

        Self {
            table: table.to_string(),
            key_columns: key_columns.into(),
            columns,
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
    }

    /// The column names, in the order of `ChangeRecord::values`.
    pub fn columns(&self) -> impl ExactSizeIterator<Item = &str> {
        self.columns.iter().map(|(name, _)| name.as_str())
    }

    /// The position of `column`, for `ChangeRecord::get` in a hot loop.
    pub fn index_of(&self, column: &str) -> Option<usize> {
        self.columns.iter().position(|(name, _)| name == column)
    }
}

/// A column of a change record: its name, or its position in the schema.
pub trait ColumnIndex {
    fn position(&self, schema: &ChangeSchema) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn position(&self, schema: &ChangeSchema) -> Option<usize> {
        (*self < schema.columns.len()).then_some(*self)
    }
}

impl ColumnIndex for str {
    fn position(&self, schema: &ChangeSchema) -> Option<usize> {
        schema.index_of(self)
    }
}

impl ColumnIndex for String {
    fn position(&self, schema: &ChangeSchema) -> Option<usize> {
        schema.index_of(self)
    }
}

impl<T: ColumnIndex + ?Sized> ColumnIndex for &T {
    fn position(&self, schema: &ChangeSchema) -> Option<usize> {
        (**self).position(schema)
    }
}

/// One change of one row of an output table. The values are positional;
/// `schema` names them.
#[derive(Debug, Clone)]
pub struct ChangeRecord {
    pub operation: ChangeOp,
    pub(crate) schema: Arc<ChangeSchema>,
    pub(crate) key: GroupKey,
    /// The non-key columns: an MV keeps its GROUP BY columns only in `key`.
    pub(crate) values: Box<[Value]>,
    pub(crate) prev_values: Option<Box<[Value]>>,
}

impl ChangeRecord {
    pub fn new(
        operation: ChangeOp,
        schema: Arc<ChangeSchema>,
        key: GroupKey,
        values: Box<[Value]>,
        prev_values: Option<Box<[Value]>>,
    ) -> Self {
        debug_assert_eq!(key.len(), schema.key_columns.len());
        Self {
            operation,
            schema,
            key,
            values,
            prev_values,
        }
    }

    pub fn table(&self) -> &str {
        &self.schema.table
    }

    pub fn schema(&self) -> &Arc<ChangeSchema> {
        &self.schema
    }

    /// The value of a column, by name or position: `record.get("balance")`.
    pub fn get(&self, column: impl ColumnIndex) -> Option<&Value> {
        let (_, slot) = &self.schema.columns[column.position(&self.schema)?];
        Some(self.resolve(*slot, &self.values))
    }

    /// The value of a column before the change; None without `prev_values`.
    pub fn prev(&self, column: impl ColumnIndex) -> Option<&Value> {
        let prev = self.prev_values.as_deref()?;
        let (_, slot) = &self.schema.columns[column.position(&self.schema)?];
        Some(self.resolve(*slot, prev))
    }

    /// The value of a key column.
    pub fn key(&self, column: &str) -> Option<&Value> {
        let i = self.schema.key_columns.iter().position(|c| c == column)?;
        self.key.get(i)
    }

    pub fn has_prev(&self) -> bool {
        self.prev_values.is_some()
    }

    /// All columns in schema order, Nulls included.
    pub fn values(&self) -> impl ExactSizeIterator<Item = (&str, &Value)> + Clone + '_ {
        self.columns(&self.values)
    }

    pub fn prev_values(
        &self,
    ) -> Option<impl ExactSizeIterator<Item = (&str, &Value)> + Clone + '_> {
        self.prev_values.as_deref().map(|prev| self.columns(prev))
    }

    pub fn key_values(&self) -> impl ExactSizeIterator<Item = (&str, &Value)> + Clone + '_ {
        self.schema
            .key_columns
            .iter()
            .zip(self.key.iter())
            .map(|(name, value)| (name.as_str(), value))
    }

    pub fn values_map(&self) -> HashMap<String, Value> {
        to_map(self.values())
    }

    pub fn prev_map(&self) -> Option<HashMap<String, Value>> {
        self.prev_values().map(to_map)
    }

    pub fn key_map(&self) -> HashMap<String, Value> {
        to_map(self.key_values())
    }

    fn columns<'a>(
        &'a self,
        values: &'a [Value],
    ) -> impl ExactSizeIterator<Item = (&'a str, &'a Value)> + Clone + 'a {
        self.schema
            .columns
            .iter()
            .map(move |(name, slot)| (name.as_str(), self.resolve(*slot, values)))
    }

    /// A cancelled record in `ChangeBuffer::flush` has no values: Null then.
    fn resolve<'a>(&'a self, slot: Slot, values: &'a [Value]) -> &'a Value {
        match slot {
            Slot::Key(i) => &self.key[i],
            Slot::Value(i) => values.get(i).unwrap_or(&NULL),
        }
    }
}

fn to_map<'a>(columns: impl Iterator<Item = (&'a str, &'a Value)>) -> HashMap<String, Value> {
    columns
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

/// Serializes `(name, value)` pairs as a map.
struct Columns<I>(I);

impl<'a, I> Serialize for Columns<I>
where
    I: ExactSizeIterator<Item = (&'a str, &'a Value)> + Clone,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in self.0.clone() {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

impl Serialize for ChangeRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("table", self.table())?;
        map.serialize_entry("operation", &self.operation)?;
        map.serialize_entry("key", &Columns(self.key_values()))?;
        map.serialize_entry("values", &Columns(self.values()))?;
        map.serialize_entry("prev_values", &self.prev_values().map(Columns))?;
        map.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerfNodeKind {
    Pipeline,
    RawTable,
    Reducer,
    #[serde(rename = "mv")]
    MV,
    Parallel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfNode {
    pub kind: PerfNodeKind,
    pub name: String,
    pub duration_ms: f64,
    pub children: Vec<PerfNode>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChangeBatch {
    pub sequence: u64,
    pub finalized_head: Option<BlockCursor>,
    pub latest_head: Option<BlockCursor>,
    pub tables: HashMap<String, Vec<ChangeRecord>>,
    #[serde(default)]
    pub perf: Vec<PerfNode>,
}

impl ChangeBatch {
    /// Iterate all records across all tables.
    pub fn all_records(&self) -> impl Iterator<Item = &ChangeRecord> {
        self.tables.values().flat_map(|v| v.iter())
    }

    /// Get records for a specific table.
    pub fn records_for(&self, table: &str) -> &[ChangeRecord] {
        self.tables.get(table).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Total number of records across all tables.
    pub fn record_count(&self) -> usize {
        self.tables.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_numeric_conversions() {
        assert_eq!(Value::UInt64(42).as_f64(), Some(42.0));
        assert_eq!(Value::Int64(-5).as_f64(), Some(-5.0));
        assert_eq!(Value::Float64(3.14).as_f64(), Some(3.14));
        assert_eq!(Value::String("hi".into()).as_f64(), None);

        assert_eq!(Value::UInt64(10).as_i64(), Some(10));
        assert_eq!(Value::Int64(-10).as_u64(), Some(-10i64 as u64));
        assert_eq!(Value::Float64(1.0).as_str(), None);
        assert_eq!(Value::String("hi".into()).as_str(), Some("hi"));
    }

    #[test]
    fn value_truthiness() {
        assert!(Value::Boolean(true).is_truthy());
        assert!(!Value::Boolean(false).is_truthy());
        assert!(Value::UInt64(1).is_truthy());
        assert!(!Value::UInt64(0).is_truthy());
        assert!(Value::Float64(0.1).is_truthy());
        assert!(!Value::Float64(0.0).is_truthy());
        assert!(Value::String("x".into()).is_truthy());
        assert!(!Value::String(String::new()).is_truthy());
        assert!(!Value::Null.is_truthy());
    }

    #[test]
    fn value_eq_and_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Value::UInt64(1));
        set.insert(Value::UInt64(1));
        assert_eq!(set.len(), 1);

        // NaN bit-equality: two NaNs with same bits are equal
        set.insert(Value::Float64(f64::NAN));
        set.insert(Value::Float64(f64::NAN));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn value_ordering() {
        assert!(Value::UInt64(1) < Value::UInt64(2));
        assert!(Value::Int64(-1) < Value::Int64(0));
        assert!(Value::Float64(1.0) < Value::Float64(2.0));
        assert!(Value::String("a".into()) < Value::String("b".into()));
        // Cross-type comparison returns None
        assert_eq!(
            Value::UInt64(1).partial_cmp(&Value::String("1".into())),
            None
        );
    }

    #[test]
    fn group_key_as_map_key() {
        let mut map = HashMap::new();
        let key = vec![Value::String("alice".into()), Value::String("ETH".into())];
        map.insert(key.clone(), 42);
        assert_eq!(map.get(&key), Some(&42));
    }

    #[test]
    fn column_type_defaults() {
        assert_eq!(ColumnType::UInt64.default_value(), Value::UInt64(0));
        assert_eq!(ColumnType::Float64.default_value(), Value::Float64(0.0));
        assert_eq!(
            ColumnType::Uint256.default_value(),
            Value::Uint256([0u8; 32])
        );
        assert_eq!(
            ColumnType::String.default_value(),
            Value::String(String::new())
        );
        assert_eq!(ColumnType::Boolean.default_value(), Value::Boolean(false));
        assert_eq!(
            ColumnType::Base58.default_value(),
            Value::Base58(Vec::new())
        );
    }

    #[test]
    fn uint256_basics() {
        // Big-endian: value "1" is 31 zero bytes then 0x01
        let mut one = [0u8; 32];
        one[31] = 1;
        let mut two = [0u8; 32];
        two[31] = 2;

        let v1 = Value::Uint256(one);
        let v2 = Value::Uint256(two);

        assert_eq!(v1, v1.clone());
        assert_ne!(v1, v2);
        assert!(v1 < v2); // big-endian byte comparison
        assert_eq!(v1.column_type(), Some(ColumnType::Uint256));

        // Display as hex
        let display = format!("{v1}");
        assert!(display.starts_with("0x"));
        assert!(display.ends_with("01"));
    }

    #[test]
    fn uint256_serde_roundtrip() {
        let mut val = [0u8; 32];
        val[0] = 0xff;
        val[31] = 0x01;
        let v = Value::Uint256(val);

        let bytes = rmp_serde::to_vec(&v).unwrap();
        let decoded: Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    #[test]
    fn base58_basics() {
        // Solana address bytes (32 bytes typically)
        let addr_bytes = vec![1u8, 2, 3, 4, 5];
        let v = Value::Base58(addr_bytes.clone());

        assert_eq!(v.column_type(), Some(ColumnType::Base58));
        assert_eq!(v, Value::Base58(addr_bytes));
        assert_ne!(v, Value::Base58(vec![9, 8, 7]));

        // Usable as hash key
        let mut set = std::collections::HashSet::new();
        set.insert(v.clone());
        set.insert(v);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn base58_serde_roundtrip() {
        let v = Value::Base58(vec![11, 22, 33, 44, 55]);
        let bytes = rmp_serde::to_vec(&v).unwrap();
        let decoded: Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded, v);
    }

    fn balance_update() -> ChangeRecord {
        let schema = ChangeSchema::mv(
            "balances",
            [
                ("amount".to_string(), false),
                ("user".to_string(), true),
                ("count".to_string(), false),
            ],
        );
        ChangeRecord::new(
            ChangeOp::Update,
            Arc::new(schema),
            smallvec::smallvec![Value::String("alice".into())],
            Box::from([Value::Float64(10.0), Value::UInt64(2)]),
            Some(Box::from([Value::Float64(4.0), Value::UInt64(1)])),
        )
    }

    #[test]
    fn change_record_reads_columns_by_name_and_position() {
        let record = balance_update();

        assert_eq!(record.table(), "balances");
        assert_eq!(record.get("amount"), Some(&Value::Float64(10.0)));
        assert_eq!(record.get("user"), Some(&Value::String("alice".into())));
        assert_eq!(record.get(2), Some(&Value::UInt64(2)));
        assert_eq!(record.get("count".to_string()), Some(&Value::UInt64(2)));
        assert_eq!(record.get("missing"), None);
        assert_eq!(record.get(3), None);

        assert_eq!(record.key("user"), Some(&Value::String("alice".into())));
        assert_eq!(record.key("amount"), None);
        assert_eq!(record.prev("amount"), Some(&Value::Float64(4.0)));
        assert_eq!(record.prev("user"), Some(&Value::String("alice".into())));

        let names: Vec<&str> = record.values().map(|(name, _)| name).collect();
        assert_eq!(names, ["amount", "user", "count"]);
        assert_eq!(record.schema().index_of("count"), Some(2));
    }

    #[test]
    fn change_record_serializes_by_name() {
        let json = serde_json::to_value(balance_update()).unwrap();
        let value = |v: Value| serde_json::to_value(v).unwrap();

        assert_eq!(json["table"], "balances");
        assert_eq!(json["operation"], "update");
        assert_eq!(json["key"]["user"], value(Value::String("alice".into())));
        assert_eq!(json["values"]["user"], value(Value::String("alice".into())));
        assert_eq!(json["values"]["amount"], value(Value::Float64(10.0)));
        assert_eq!(json["prev_values"]["count"], value(Value::UInt64(1)));
    }

    #[test]
    fn change_batch_serializes() {
        let batch = ChangeBatch {
            sequence: 1,
            finalized_head: Some(BlockCursor {
                number: 900,
                hash: "0xabc".into(),
            }),
            latest_head: Some(BlockCursor {
                number: 1000,
                hash: "0xdef".into(),
            }),
            tables: HashMap::from([("balances".to_string(), vec![balance_update()])]),
            perf: vec![],
        };

        let json = serde_json::to_value(&batch).unwrap();
        assert_eq!(json["sequence"], 1);
        assert_eq!(json["finalized_head"]["number"], 900);
        assert_eq!(json["latest_head"]["hash"], "0xdef");
        assert_eq!(json["tables"]["balances"][0]["operation"], "update");
    }
}
