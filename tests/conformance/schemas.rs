//! The schemas of CT-1: the shapes of the current tests, each with the
//! model of its Lua bodies.

use std::collections::BTreeMap;

use settle::types::{RowMap, Value};

use crate::harness::schema::{
    Field, Func, GroupState, Item, Reducer, Schema, Sliding, Table, Type, View,
};

const HOUR: u64 = 3600;
const DAY: u64 = 24 * HOUR;

fn swaps() -> Table {
    Table {
        name: "swaps".into(),
        columns: vec![
            ("block_number".into(), Type::UInt64),
            ("pool".into(), Type::String),
            ("amount".into(), Type::UInt64),
            ("ts".into(), Type::DateTime),
        ],
        is_virtual: false,
    }
}

fn key(column: &str) -> Item {
    Item::Key(column.into())
}

fn agg(name: &str, func: Func, column: Option<&str>) -> Item {
    Item::Agg {
        name: name.into(),
        func,
        column: column.map(Into::into),
    }
}

fn view(name: &str, source: &str, items: Vec<Item>) -> View {
    View {
        name: name.into(),
        source: source.into(),
        items,
        sliding: None,
    }
}

/// sum, both counts, min and max per pool, straight from a raw table.
pub fn sum() -> Schema {
    Schema {
        name: "sum",
        tables: vec![swaps()],
        reducers: vec![],
        views: vec![view(
            "pool_volume",
            "swaps",
            vec![
                key("pool"),
                agg("total", Func::Sum, Some("amount")),
                agg("swaps", Func::Count, None),
                agg("priced", Func::Count, Some("amount")),
                agg("low", Func::Min, Some("amount")),
                agg("high", Func::Max, Some("amount")),
            ],
        )],
        nulls: true,
        poison: None,
    }
}

/// Two raw tables, one of them virtual, and the Int64 and Float64 modes.
pub fn two_tables() -> Schema {
    let transfers = Table {
        name: "transfers".into(),
        columns: vec![
            ("block_number".into(), Type::UInt64),
            ("pool".into(), Type::String),
            ("delta".into(), Type::Int64),
            ("price".into(), Type::Float64),
        ],
        is_virtual: true,
    };
    Schema {
        name: "two_tables",
        tables: vec![swaps(), transfers],
        reducers: vec![],
        views: vec![
            view(
                "pool_volume",
                "swaps",
                vec![
                    key("pool"),
                    agg("total", Func::Sum, Some("amount")),
                    agg("swaps", Func::Count, None),
                ],
            ),
            view(
                "pool_flows",
                "transfers",
                vec![
                    key("pool"),
                    agg("net", Func::Sum, Some("delta")),
                    agg("low", Func::Min, Some("delta")),
                    agg("high", Func::Max, Some("delta")),
                    agg("price_sum", Func::Sum, Some("price")),
                    agg("price_avg", Func::Avg, Some("price")),
                    agg("price_high", Func::Max, Some("price")),
                ],
            ),
        ],
        nulls: true,
        poison: None,
    }
}

/// A tumbling window: the hour is part of the group key.
pub fn tumbling() -> Schema {
    Schema {
        name: "tumbling",
        tables: vec![swaps()],
        reducers: vec![],
        views: vec![view(
            "pool_hourly",
            "swaps",
            vec![
                key("pool"),
                Item::Tumbling {
                    name: "hour".into(),
                    column: "ts".into(),
                    seconds: HOUR,
                },
                agg("total", Func::Sum, Some("amount")),
                agg("swaps", Func::Count, None),
                agg("mean", Func::Avg, Some("amount")),
                agg("opening", Func::First, Some("amount")),
                agg("closing", Func::Last, Some("amount")),
            ],
        )],
        nulls: true,
        poison: None,
    }
}

fn sliding_view(name: &str, seconds: u64, items: Vec<Item>) -> View {
    View {
        sliding: Some(Sliding {
            seconds,
            column: "ts".into(),
        }),
        ..view(name, "swaps", items)
    }
}

/// Every aggregate in a window of an hour, which six blocks fill.
pub fn sliding() -> Schema {
    Schema {
        name: "sliding",
        tables: vec![swaps()],
        reducers: vec![],
        views: vec![sliding_view(
            "pool_1h",
            HOUR,
            vec![
                key("pool"),
                agg("total", Func::Sum, Some("amount")),
                agg("swaps", Func::Count, None),
                agg("low", Func::Min, Some("amount")),
                agg("high", Func::Max, Some("amount")),
                agg("mean", Func::Avg, Some("amount")),
                agg("opening", Func::First, Some("amount")),
                agg("closing", Func::Last, Some("amount")),
            ],
        )],
        nulls: true,
        poison: None,
    }
}

/// A window that no history outlives: forks without expiry.
pub fn sliding_no_expiry() -> Schema {
    Schema {
        name: "sliding_no_expiry",
        tables: vec![swaps()],
        reducers: vec![],
        views: vec![sliding_view(
            "pool_30d",
            30 * DAY,
            vec![
                key("pool"),
                agg("total", Func::Sum, Some("amount")),
                agg("swaps", Func::Count, None),
            ],
        )],
        nulls: true,
        poison: None,
    }
}

fn u64_of(row: &RowMap, column: &str) -> u64 {
    match row.get(column) {
        Some(Value::UInt64(v)) => *v,
        other => panic!("{column} is a UInt64, got {other:?}"),
    }
}

fn str_of(row: &RowMap, column: &str) -> String {
    match row.get(column) {
        Some(Value::String(v)) => v.clone(),
        other => panic!("{column} is a String, got {other:?}"),
    }
}

fn emit(pairs: Vec<(&str, Value)>) -> RowMap {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

const POISON: u64 = 13;

const RUNNING_LUA: &str = r#"
    if row.amount == 13 then
        error("poison")
    end
    local seen = nil
    if row.amount > 0 then
        state.balance = state.balance + row.amount
        seen = row.amount
    end
    emit { seen = seen, balance = state.balance }
"#;

fn running(state: &mut GroupState, row: &RowMap) -> Result<Vec<RowMap>, String> {
    let amount = u64_of(row, "amount");
    if amount == POISON {
        return Err("poison".into());
    }

    let mut out = RowMap::new();
    if amount > 0 {
        let Some(Value::Float64(balance)) = state.scalars.get("balance") else {
            panic!("balance is a Float64");
        };
        let balance = balance + amount as f64;
        state
            .scalars
            .insert("balance".into(), Value::Float64(balance));
        out.insert("seen".into(), Value::Int64(amount as i64));
    }
    out.insert("balance".into(), state.scalars["balance"].clone());
    Ok(vec![out])
}

/// A Lua reducer that a view reads, with a row that the body rejects.
pub fn reducer() -> Schema {
    Schema {
        name: "reducer",
        tables: vec![swaps()],
        reducers: vec![Reducer {
            name: "running".into(),
            source: "swaps".into(),
            group_by: vec!["pool".into()],
            state: vec![Field::Scalar {
                name: "balance".into(),
                ty: Type::Float64,
                default: Value::Float64(0.0),
            }],
            lua: RUNNING_LUA.into(),
            body: running,
        }],
        views: vec![view(
            "pool_summary",
            "running",
            vec![
                key("pool"),
                agg("balance", Func::Last, Some("balance")),
                agg("total", Func::Sum, Some("seen")),
                agg("high", Func::Max, Some("seen")),
                agg("events", Func::Count, None),
            ],
        )],
        nulls: false,
        poison: Some(POISON),
    }
}

const TALLIES_LUA: &str = r#"
    state.rows = state.rows + 1
    local before = state.by_pool:get(row.pool)
    if row.amount % 3 == 0 then
        state.by_pool:delete(row.pool)
    else
        state.by_pool:set(row.pool, (before or 0) + row.amount)
    end
    emit { pool = row.pool, before = before, rows = state.rows }
"#;

fn tallies(state: &mut GroupState, row: &RowMap) -> Result<Vec<RowMap>, String> {
    let Some(Value::UInt64(rows)) = state.scalars.get("rows") else {
        panic!("rows is a UInt64");
    };
    let rows = rows + 1;
    state.scalars.insert("rows".into(), Value::UInt64(rows));

    let (pool, amount) = (str_of(row, "pool"), u64_of(row, "amount"));
    let by_pool = state.maps.get_mut("by_pool").expect("a map field");
    let before = by_pool.get(&pool).cloned();
    update_entry(by_pool, pool.clone(), amount, amount);

    let mut out = emit(vec![
        ("pool", Value::String(pool)),
        ("rows", Value::Int64(rows as i64)),
    ]);
    if let Some(before) = before {
        out.insert("before".into(), lua_integer(&before));
    }
    Ok(vec![out])
}

const CLASSES_LUA: &str = r#"
    local class = tostring(row.amount % 4)
    local before = state.by_class:get(class)
    if row.amount % 3 == 0 then
        state.by_class:delete(class)
    else
        state.by_class:set(class, (before or 0) + 1)
    end
    emit { before = before }
"#;

fn classes(state: &mut GroupState, row: &RowMap) -> Result<Vec<RowMap>, String> {
    let amount = u64_of(row, "amount");
    let class = (amount % 4).to_string();
    let by_class = state.maps.get_mut("by_class").expect("a map field");
    let before = by_class.get(&class).cloned();
    update_entry(by_class, class, amount, 1);

    // `{ before = nil }` is an empty Lua table, and an empty emit is no row.
    let Some(before) = before else {
        return Ok(vec![]);
    };
    Ok(vec![emit(vec![("before", lua_integer(&before))])])
}

/// A multiple of 3 deletes the entry, anything else adds `add` to it.
fn update_entry(map: &mut BTreeMap<String, Value>, key: String, amount: u64, add: u64) {
    if amount % 3 == 0 {
        map.remove(&key);
        return;
    }
    let before = match map.get(&key) {
        Some(Value::UInt64(v)) => *v,
        _ => 0,
    };
    map.insert(key, Value::UInt64(before + add));
}

/// A UInt64 as a Lua body gives it back: a Lua integer, so Int64.
fn lua_integer(value: &Value) -> Value {
    match value {
        Value::UInt64(v) => Value::Int64(*v as i64),
        other => other.clone(),
    }
}

/// Map state: one group with an entry per pool, and a group per pool with
/// an entry per amount class. Entries come, go and come back.
pub fn map() -> Schema {
    Schema {
        name: "map",
        tables: vec![swaps()],
        reducers: vec![
            Reducer {
                name: "tallies".into(),
                source: "swaps".into(),
                group_by: vec![],
                state: vec![
                    Field::Scalar {
                        name: "rows".into(),
                        ty: Type::UInt64,
                        default: Value::UInt64(0),
                    },
                    Field::Map {
                        name: "by_pool".into(),
                        ty: Type::UInt64,
                    },
                ],
                lua: TALLIES_LUA.into(),
                body: tallies,
            },
            Reducer {
                name: "classes".into(),
                source: "swaps".into(),
                group_by: vec!["pool".into()],
                state: vec![Field::Map {
                    name: "by_class".into(),
                    ty: Type::UInt64,
                }],
                lua: CLASSES_LUA.into(),
                body: classes,
            },
        ],
        views: vec![
            view(
                "pool_tallies",
                "tallies",
                vec![
                    key("pool"),
                    agg("before", Func::Sum, Some("before")),
                    agg("rows", Func::Max, Some("rows")),
                    agg("events", Func::Count, None),
                ],
            ),
            view(
                "pool_classes",
                "classes",
                vec![
                    key("pool"),
                    agg("before", Func::Sum, Some("before")),
                    agg("events", Func::Count, None),
                ],
            ),
        ],
        nulls: false,
        poison: None,
    }
}
