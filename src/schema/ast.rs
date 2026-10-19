use crate::types::ColumnType;

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
    pub column_type: ColumnType,
    pub default: String,
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
