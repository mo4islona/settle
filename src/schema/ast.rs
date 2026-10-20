use ethnum::U256;

use crate::types::{ColumnType, Value};

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// When true, the table is storage-only: rows are persisted and fed to
    /// reducers/MVs, but no change records are emitted to the output stream.
    /// Set via `CREATE VIRTUAL TABLE ...` syntax.
    pub virtual_table: bool,
}

#[derive(Debug, Clone)]
pub enum AggFunc {
    Sum,
    Count,
    Min,
    Max,
    Avg,
    First,
    Last,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Sum => "sum",
            AggFunc::Count => "count",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Avg => "avg",
            AggFunc::First => "first",
            AggFunc::Last => "last",
        }
    }
}

#[derive(Debug, Clone)]
pub enum SelectExpr {
    Column(String),
    Agg(AggFunc, Option<String>),
    WindowFunc {
        column: String,
        interval_seconds: u64,
    },
}

#[derive(Debug, Clone)]
pub struct SelectItem {
    pub expr: SelectExpr,
    pub alias: Option<String>,
}

impl SelectItem {
    /// The column of the view that this item gives.
    pub fn output_name(&self) -> String {
        if let Some(alias) = &self.alias {
            return alias.clone();
        }
        match &self.expr {
            SelectExpr::Column(col) => col.clone(),
            SelectExpr::Agg(func, col) => {
                let func_name = func.name();
                match col {
                    Some(c) => format!("{func_name}_{c}"),
                    None => func_name.to_string(),
                }
            }
            SelectExpr::WindowFunc { column, .. } => column.clone(),
        }
    }
}

/// Configuration for a sliding (rolling) time window on a materialized view.
/// When present, the MV aggregations cover only the last `interval_seconds`
/// of data, with old blocks expiring as new data arrives.
#[derive(Debug, Clone)]
pub struct SlidingWindowDef {
    /// Duration of the sliding window in seconds.
    pub interval_seconds: u64,
    /// The column containing row timestamps (milliseconds) used for expiry.
    pub time_column: String,
}

#[derive(Debug, Clone)]
pub struct MVDef {
    pub name: String,
    pub source: String,
    pub select: Vec<SelectItem>,
    pub group_by: Vec<String>,
    pub sliding_window: Option<SlidingWindowDef>,
}

/// The most aggregate columns an MV can have: the persisted group state
/// stores the aggregate count in one byte.
pub const MAX_MV_AGGREGATES: usize = u8::MAX as usize;

#[derive(Debug, Clone)]
pub struct StateField {
    pub name: String,
    /// The type of the field; of the value of each entry for a MAP field.
    pub column_type: ColumnType,
    /// Empty for a MAP field, which starts with no entries.
    pub default: String,
    /// A MAP field, `Map(String, column_type)`: entries by String key, each
    /// changed on its own.
    pub map: bool,
}

impl StateField {
    /// The value of DEFAULT, or why the text is not a value of the type.
    pub fn default_value(&self) -> Result<Value, String> {
        let text = self.default.as_str();
        let quoted = unquote(text);
        let value = match self.column_type {
            ColumnType::Float64 => text.parse().ok().map(Value::Float64),
            ColumnType::UInt64 => text.parse().ok().map(Value::UInt64),
            ColumnType::Int64 => text.parse().ok().map(Value::Int64),
            ColumnType::DateTime => text.parse().ok().map(Value::DateTime),
            ColumnType::Boolean => match text.to_ascii_lowercase().as_str() {
                "true" | "1" => Some(Value::Boolean(true)),
                "false" | "0" => Some(Value::Boolean(false)),
                _ => None,
            },
            ColumnType::Uint256 => U256::from_str_prefixed(text)
                .ok()
                .map(|n| Value::Uint256(n.to_be_bytes())),
            ColumnType::String => quoted.map(Value::String),
            ColumnType::JSON => quoted
                .and_then(|json| serde_json::from_str(&json).ok())
                .map(Value::JSON),
            // Settle reads no text of these types: only the empty value.
            ColumnType::Bytes | ColumnType::Base58 => quoted
                .filter(String::is_empty)
                .map(|_| self.column_type.default_value()),
        };
        value.ok_or_else(|| format!("DEFAULT {text} is not a {} value", self.column_type))
    }

    /// The type as the schema writes it.
    pub fn type_name(&self) -> String {
        if self.map {
            format!("Map(String, {})", self.column_type)
        } else {
            self.column_type.to_string()
        }
    }
}

/// The text of a quoted literal, with a doubled quote read as one.
fn unquote(text: &str) -> Option<String> {
    let quote = text.chars().next().filter(|c| *c == '\'' || *c == '"')?;
    let inner = text[1..].strip_suffix(quote)?;
    Some(inner.replace(&format!("{quote}{quote}"), &quote.to_string()))
}

#[derive(Debug, Clone)]
pub enum ReducerBody {
    Lua {
        script: String,
    },
    /// Reducer logic provided by the host language (e.g., TypeScript via napi).
    /// The `id` ties this definition to a registered callback.
    External {
        id: String,
    },
}

#[derive(Debug, Clone)]
pub struct ReducerDef {
    pub name: String,
    pub source: String,
    pub group_by: Vec<String>,
    pub state: Vec<StateField>,
    pub body: ReducerBody,
    pub requires: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ModuleDef {
    pub name: String,
    pub script: String,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub tables: Vec<TableDef>,
    pub modules: Vec<ModuleDef>,
    pub reducers: Vec<ReducerDef>,
    pub materialized_views: Vec<MVDef>,
}
