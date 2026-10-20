//! A pipeline as the harness knows it. One description renders the schema
//! text for Settle and drives the reference model, so the two cannot drift.

use std::collections::BTreeMap;
use std::fmt::Write;

use settle::types::{RowMap, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    UInt64,
    Int64,
    Float64,
    String,
    DateTime,
}

impl Type {
    fn ddl(self) -> &'static str {
        match self {
            Type::UInt64 => "UInt64",
            Type::Int64 => "Int64",
            Type::Float64 => "Float64",
            Type::String => "String",
            Type::DateTime => "DateTime",
        }
    }

    /// Whether `value` is a value of this type. Every column accepts Null (DEF-4).
    pub fn admits(self, value: &Value) -> bool {
        matches!(
            (self, value),
            (_, Value::Null)
                | (Type::UInt64, Value::UInt64(_))
                | (Type::Int64, Value::Int64(_))
                | (Type::Float64, Value::Float64(_))
                | (Type::String, Value::String(_))
                | (Type::DateTime, Value::DateTime(_))
        )
    }
}

pub struct Table {
    pub name: String,
    /// `block_number` first, as in the schema text.
    pub columns: Vec<(String, Type)>,
    pub is_virtual: bool,
}

pub enum Field {
    Scalar {
        name: String,
        ty: Type,
        default: Value,
    },
    Map {
        name: String,
        ty: Type,
    },
}

/// The state of one reducer group, as a body sees it.
#[derive(Clone, Debug, Default)]
pub struct GroupState {
    pub scalars: BTreeMap<String, Value>,
    pub maps: BTreeMap<String, BTreeMap<String, Value>>,
}

/// The model of a Lua body: the same logic in Rust, with the value types
/// that Lua gives back (an integer comes back as Int64). Err is a body error.
pub type Body = fn(&mut GroupState, &RowMap) -> Result<Vec<RowMap>, String>;

pub struct Reducer {
    pub name: String,
    pub source: String,
    pub group_by: Vec<String>,
    pub state: Vec<Field>,
    pub lua: String,
    pub body: Body,
}

impl Reducer {
    pub fn defaults(&self) -> GroupState {
        let mut state = GroupState::default();
        for field in &self.state {
            match field {
                Field::Scalar { name, default, .. } => {
                    state.scalars.insert(name.clone(), default.clone());
                }
                Field::Map { name, .. } => {
                    state.maps.insert(name.clone(), BTreeMap::new());
                }
            }
        }
        state
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Func {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    First,
    Last,
}

impl Func {
    fn ddl(self) -> &'static str {
        match self {
            Func::Count => "count",
            Func::Sum => "sum",
            Func::Min => "min",
            Func::Max => "max",
            Func::Avg => "avg",
            Func::First => "first",
            Func::Last => "last",
        }
    }
}

pub enum Item {
    Key(String),
    /// `toStartOfInterval(column, INTERVAL seconds SECOND) AS name`.
    Tumbling {
        name: String,
        column: String,
        seconds: u64,
    },
    Agg {
        name: String,
        func: Func,
        column: Option<String>,
    },
}

impl Item {
    pub fn name(&self) -> &str {
        match self {
            Item::Key(name) | Item::Tumbling { name, .. } | Item::Agg { name, .. } => name,
        }
    }

    pub fn is_key(&self) -> bool {
        !matches!(self, Item::Agg { .. })
    }
}

pub struct Sliding {
    pub seconds: u64,
    pub column: String,
}

pub struct View {
    pub name: String,
    pub source: String,
    pub items: Vec<Item>,
    pub sliding: Option<Sliding>,
}

pub struct Schema {
    pub name: &'static str,
    pub tables: Vec<Table>,
    pub reducers: Vec<Reducer>,
    pub views: Vec<View>,
    /// Whether the simulator may send Null values. A body that does
    /// arithmetic on a column cannot take them.
    pub nulls: bool,
    /// An `amount` that a body rejects with an error.
    pub poison: Option<u64>,
}

impl Schema {
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }

    pub fn reducer(&self, name: &str) -> Option<&Reducer> {
        self.reducers.iter().find(|r| r.name == name)
    }

    pub fn view(&self, name: &str) -> Option<&View> {
        self.views.iter().find(|v| v.name == name)
    }

    /// DEF-31: the non-virtual raw tables and the views.
    pub fn is_output(&self, table: &str) -> bool {
        self.table(table).is_some_and(|t| !t.is_virtual) || self.view(table).is_some()
    }

    /// DEF-84: deferral-safe means no sliding view and no reducer registered
    /// at run time; the harness registers none.
    pub fn deferral_safe(&self) -> bool {
        self.views.iter().all(|v| v.sliding.is_none())
    }

    /// The declared type of `column` of a source, for DEF-43. A reducer
    /// source types its state fields, and its group-by columns when it reads
    /// a raw table. Every other emit column is untyped.
    pub fn column_type(&self, source: &str, column: &str) -> Option<Type> {
        if let Some(table) = self.table(source) {
            return table
                .columns
                .iter()
                .find(|(c, _)| c == column)
                .map(|(_, ty)| *ty);
        }

        let reducer = self.reducer(source)?;
        let field = reducer.state.iter().find_map(|field| match field {
            Field::Scalar { name, ty, .. } if name == column => Some(*ty),
            _ => None,
        });
        let group_column = || {
            let reads_raw = self.table(&reducer.source).is_some();
            let groups_by = reducer.group_by.iter().any(|c| c == column);
            (reads_raw && groups_by)
                .then(|| self.column_type(&reducer.source, column))
                .flatten()
        };
        field.or_else(group_column)
    }

    /// The column names of the records of an output table, in record order (RP-2).
    pub fn columns(&self, table: &str) -> Vec<String> {
        if let Some(table) = self.table(table) {
            return table.columns.iter().map(|(c, _)| c.clone()).collect();
        }
        let view = self.view(table).expect("an output table");
        view.items
            .iter()
            .map(|item| item.name().to_string())
            .collect()
    }

    pub fn ddl(&self) -> String {
        let mut out = String::new();

        for table in &self.tables {
            let kind = if table.is_virtual {
                "VIRTUAL TABLE"
            } else {
                "TABLE"
            };
            let columns: Vec<String> = table
                .columns
                .iter()
                .map(|(c, ty)| format!("{c} {}", ty.ddl()))
                .collect();
            writeln!(
                out,
                "CREATE {kind} {} ({});\n",
                table.name,
                columns.join(", ")
            )
            .unwrap();
        }

        for reducer in &self.reducers {
            writeln!(
                out,
                "CREATE REDUCER {}\nSOURCE {}",
                reducer.name, reducer.source
            )
            .unwrap();
            if !reducer.group_by.is_empty() {
                writeln!(out, "GROUP BY {}", reducer.group_by.join(", ")).unwrap();
            }
            let fields: Vec<String> = reducer
                .state
                .iter()
                .map(|field| match field {
                    Field::Scalar { name, ty, default } => {
                        format!("{name} {} DEFAULT {}", ty.ddl(), literal(default))
                    }
                    Field::Map { name, ty } => format!("{name} Map(String, {})", ty.ddl()),
                })
                .collect();
            writeln!(out, "STATE ({})", fields.join(", ")).unwrap();
            writeln!(out, "LANGUAGE lua\nPROCESS $$\n{}\n$$;\n", reducer.lua).unwrap();
        }

        for view in &self.views {
            let items: Vec<String> = view
                .items
                .iter()
                .map(|item| match item {
                    Item::Key(column) => column.clone(),
                    Item::Tumbling {
                        name,
                        column,
                        seconds,
                    } => {
                        format!("toStartOfInterval({column}, INTERVAL {seconds} SECOND) AS {name}")
                    }
                    Item::Agg { name, func, column } => {
                        format!(
                            "{}({}) AS {name}",
                            func.ddl(),
                            column.as_deref().unwrap_or("")
                        )
                    }
                })
                .collect();
            let keys: Vec<&str> = view
                .items
                .iter()
                .filter(|i| i.is_key())
                .map(Item::name)
                .collect();
            write!(
                out,
                "CREATE MATERIALIZED VIEW {} AS\nSELECT {}\nFROM {}\nGROUP BY {}",
                view.name,
                items.join(", "),
                view.source,
                keys.join(", ")
            )
            .unwrap();
            if let Some(sliding) = &view.sliding {
                write!(
                    out,
                    "\nWINDOW SLIDING INTERVAL {} SECOND BY {}",
                    sliding.seconds, sliding.column
                )
                .unwrap();
            }
            writeln!(out, ";\n").unwrap();
        }

        out
    }
}

fn literal(value: &Value) -> String {
    match value {
        Value::UInt64(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::String(v) => format!("'{v}'"),
        other => panic!("no literal for {other:?}"),
    }
}
