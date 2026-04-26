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

#[derive(Debug, Clone)]
pub struct StateField {
    pub name: String,
    pub column_type: ColumnType,
    pub default: String,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(String),
    Float(f64),
    Int(i64),
    ColumnRef(String),
    StateRef(String),
    RowRef(String),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    If {
        condition: Box<Expr>,
        then_expr: Box<Expr>,
        else_expr: Box<Expr>,
    },
}

#[derive(Debug, Clone)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
}

#[derive(Debug, Clone)]
pub struct WhenBlock {
    pub condition: Expr,
    pub lets: Vec<(String, Expr)>,
    pub sets: Vec<(String, Expr)>,
    pub emits: Vec<(String, Expr)>,
}

#[derive(Debug, Clone)]
pub struct AlwaysEmit {
    pub emits: Vec<(String, Expr)>,
}

#[derive(Debug, Clone)]
pub enum ReducerBody {
    EventRules {
        when_blocks: Vec<WhenBlock>,
        always_emit: Option<AlwaysEmit>,
    },
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
