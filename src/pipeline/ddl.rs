//! Builder option types + helpers — Rust mirror of `@settle/stream`'s `ddl.ts`.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::types::{Row, RowMap, Value};

// ─── Duration parsing ───────────────────────────────────────────

/// Parse a duration string like `"5 minutes"`, `"1 hour"`, `"86400 seconds"`.
/// Supported units: `s/sec/second(s)`, `m/min/minute(s)`, `h/hr/hour(s)`,
/// `d/day(s)` (case-insensitive).
pub fn parse_duration(s: &str) -> Result<u64> {
    let trimmed = s.trim();
    let split = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| Error::Schema(format!("invalid duration: '{s}'")))?;
    let (num_part, rest) = trimmed.split_at(split);
    let n: u64 = num_part
        .parse()
        .map_err(|_| Error::Schema(format!("invalid duration: '{s}'")))?;
    let unit = rest.trim().to_ascii_lowercase();
    let mult: u64 = match unit.as_str() {
        "s" | "sec" | "second" | "seconds" => 1,
        "m" | "min" | "minute" | "minutes" => 60,
        "h" | "hr" | "hour" | "hours" => 3600,
        "d" | "day" | "days" => 86400,
        _ => return Err(Error::Schema(format!("unknown duration unit: '{unit}'"))),
    };
    Ok(n * mult)
}

// ─── Interval helper ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IntervalExpr {
    pub column: String,
    pub seconds: u64,
    pub alias: Option<String>,
}

impl IntervalExpr {
    pub fn r#as(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }
}

/// `interval("block_time", "5 minutes")` — bucket a DateTime column by duration.
pub fn interval(column: impl Into<String>, duration: &str) -> IntervalExpr {
    let seconds = parse_duration(duration).unwrap_or_else(|e| panic!("interval(): {e}"));
    IntervalExpr {
        column: column.into(),
        seconds,
        alias: None,
    }
}

// ─── Aggregation expressions ────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AggExpr {
    pub func: AggFn,
    pub column: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum AggFn {
    Sum,
    Count,
    First,
    Last,
    Min,
    Max,
    Avg,
}

impl AggFn {
    pub fn name(self) -> &'static str {
        match self {
            AggFn::Sum => "sum",
            AggFn::Count => "count",
            AggFn::First => "first",
            AggFn::Last => "last",
            AggFn::Min => "min",
            AggFn::Max => "max",
            AggFn::Avg => "avg",
        }
    }
}

#[derive(Debug, Clone)]
pub struct KeyRef {
    pub column: String,
}

#[derive(Debug, Clone)]
pub enum Projection {
    Agg(AggExpr),
    Key(KeyRef),
}

impl From<AggExpr> for Projection {
    fn from(a: AggExpr) -> Self {
        Projection::Agg(a)
    }
}
impl From<KeyRef> for Projection {
    fn from(k: KeyRef) -> Self {
        Projection::Key(k)
    }
}

/// Proxy passed to `select: |agg| ...` view callbacks.
pub struct AggProxy;

impl AggProxy {
    pub fn key(&self, column: impl Into<String>) -> KeyRef {
        KeyRef {
            column: column.into(),
        }
    }
    pub fn sum(&self, column: impl Into<String>) -> AggExpr {
        AggExpr {
            func: AggFn::Sum,
            column: Some(column.into()),
        }
    }
    pub fn count(&self) -> AggExpr {
        AggExpr {
            func: AggFn::Count,
            column: None,
        }
    }
    pub fn first(&self, column: impl Into<String>) -> AggExpr {
        AggExpr {
            func: AggFn::First,
            column: Some(column.into()),
        }
    }
    pub fn last(&self, column: impl Into<String>) -> AggExpr {
        AggExpr {
            func: AggFn::Last,
            column: Some(column.into()),
        }
    }
    pub fn min(&self, column: impl Into<String>) -> AggExpr {
        AggExpr {
            func: AggFn::Min,
            column: Some(column.into()),
        }
    }
    pub fn max(&self, column: impl Into<String>) -> AggExpr {
        AggExpr {
            func: AggFn::Max,
            column: Some(column.into()),
        }
    }
    pub fn avg(&self, column: impl Into<String>) -> AggExpr {
        AggExpr {
            func: AggFn::Avg,
            column: Some(column.into()),
        }
    }
}

// ─── GROUP BY items ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum GroupByItem {
    Column(String),
    Interval(IntervalExpr),
}

impl From<&str> for GroupByItem {
    fn from(s: &str) -> Self {
        GroupByItem::Column(s.to_string())
    }
}
impl From<String> for GroupByItem {
    fn from(s: String) -> Self {
        GroupByItem::Column(s)
    }
}
impl From<IntervalExpr> for GroupByItem {
    fn from(i: IntervalExpr) -> Self {
        GroupByItem::Interval(i)
    }
}

// ─── Reducer / view options ─────────────────────────────────────

pub type ReduceFn = Box<dyn Fn(&mut StateCtx, &Row) + Send + Sync>;
pub type SelectFn = Box<dyn FnOnce(&AggProxy) -> Vec<(String, Projection)>>;

pub struct ReducerOptions {
    pub group_by: Vec<String>,
    pub initial_state: RowMap,
    pub reduce: ReduceFn,
}

#[derive(Debug, Clone)]
pub struct SlidingWindowOptions {
    /// Window duration string (`"1 hour"`, `"30 minutes"`, ...).
    pub interval: String,
    /// DateTime/numeric column to window on.
    pub time_column: String,
}

pub struct ViewOptions {
    pub group_by: Vec<GroupByItem>,
    pub sliding_window: Option<SlidingWindowOptions>,
    pub select: SelectFn,
}

// ─── State context (passed to reduce closures) ──────────────────

/// Mutable context handed to `reduce(state, row)` closures.
///
/// Wraps the reducer's running state and accumulates emitted rows for the
/// current input row. Mirrors TS `ReducerCtx<TState, TEmit>` (`state.update()`,
/// `state.emit()`, plus typed read accessors).
pub struct StateCtx<'a> {
    state: &'a mut HashMap<String, Value>,
    emits: Vec<RowMap>,
}

impl<'a> StateCtx<'a> {
    pub(crate) fn new(state: &'a mut HashMap<String, Value>) -> Self {
        Self {
            state,
            emits: Vec::new(),
        }
    }

    pub(crate) fn into_emits(self) -> Vec<RowMap> {
        self.emits
    }

    /// Replace the entire state with the given map.
    pub fn update(&mut self, new_state: RowMap) {
        *self.state = new_state;
    }

    /// Set a single state field without touching the others.
    pub fn set(&mut self, name: impl Into<String>, value: Value) {
        self.state.insert(name.into(), value);
    }

    /// Emit a row into the reducer's output stream.
    pub fn emit(&mut self, row: RowMap) {
        self.emits.push(row);
    }

    /// Read a state field by name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.state.get(name)
    }

    pub fn get_f64(&self, name: &str) -> f64 {
        self.state.get(name).and_then(Value::as_f64).unwrap_or(0.0)
    }
    pub fn get_i64(&self, name: &str) -> i64 {
        self.state.get(name).and_then(Value::as_i64).unwrap_or(0)
    }
    pub fn get_u64(&self, name: &str) -> u64 {
        self.state.get(name).and_then(Value::as_u64).unwrap_or(0)
    }
    pub fn get_bool(&self, name: &str) -> bool {
        self.state.get(name).and_then(Value::as_bool).unwrap_or(false)
    }
    pub fn get_str(&self, name: &str) -> &str {
        self.state.get(name).and_then(Value::as_str).unwrap_or("")
    }

    /// Borrow the underlying state map.
    pub fn state(&self) -> &HashMap<String, Value> {
        self.state
    }
}
