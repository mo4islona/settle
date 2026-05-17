//! DDL string generation for [`crate::pipeline::Pipeline`].
//!
//! Mirrors the SQL emitted by `bindings/typescript/settle/src/ddl.ts`
//! (`tableToSql`, `reducerToSql`, `viewToSql`) so the existing schema parser
//! accepts whatever the builder produces.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::types::{RowMap, Value};

use super::ddl::{
    AggExpr, GroupByItem, IntervalExpr, Projection, SlidingWindowOptions, parse_duration,
};

/// One column definition for a `CREATE TABLE` builder call.
#[derive(Clone)]
pub(crate) struct ColumnSpec {
    pub name: String,
    pub sql_type: &'static str,
}

#[derive(Clone)]
pub(crate) struct TableSpec {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub virtual_table: bool,
}

pub(crate) struct ReducerSpec {
    pub name: String,
    pub source: String,
    pub group_by: Vec<String>,
    pub state_fields: Vec<StateField>,
}

pub(crate) struct ViewSpec {
    pub name: String,
    pub source: String,
    pub group_by: Vec<GroupByItem>,
    pub sliding_window: Option<SlidingWindowOptions>,
    pub projections: Vec<(String, Projection)>,
}

#[derive(Debug)]
pub(crate) struct StateField {
    pub name: String,
    pub sql_type: &'static str,
    pub default: String,
}

// ─── Public DDL emitters ────────────────────────────────────────

pub(crate) fn table_to_sql(t: &TableSpec) -> String {
    let prefix = if t.virtual_table {
        "CREATE VIRTUAL TABLE"
    } else {
        "CREATE TABLE"
    };
    let cols = t
        .columns
        .iter()
        .map(|c| format!("{} {}", c.name, c.sql_type))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{prefix} {} ({cols});", t.name)
}

pub(crate) fn reducer_to_sql(r: &ReducerSpec) -> String {
    let gb = r.group_by.join(", ");
    let state = r
        .state_fields
        .iter()
        .map(|f| format!("{} {} DEFAULT {}", f.name, f.sql_type, f.default))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE REDUCER {} SOURCE {} GROUP BY {gb} STATE ({state}) LANGUAGE EXTERNAL;",
        r.name, r.source
    )
}

pub(crate) fn view_to_sql(v: &ViewSpec) -> Result<String> {
    let mut group_by_cols: Vec<String> = Vec::new();
    let mut interval_defs: HashMap<String, IntervalExpr> = HashMap::new();

    for item in &v.group_by {
        match item {
            GroupByItem::Column(c) => group_by_cols.push(c.clone()),
            GroupByItem::Interval(i) => {
                let alias = i
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{}_interval", i.column));
                interval_defs.insert(alias.clone(), i.clone());
                group_by_cols.push(alias);
            }
        }
    }

    let mut select_items: Vec<String> = Vec::with_capacity(v.projections.len());
    for (alias, proj) in &v.projections {
        match proj {
            Projection::Key(k) => {
                if let Some(intv) = interval_defs.get(&k.column) {
                    select_items.push(format!(
                        "toStartOfInterval({}, INTERVAL {} SECOND) AS {alias}",
                        intv.column, intv.seconds
                    ));
                } else if alias == &k.column {
                    select_items.push(alias.clone());
                } else {
                    select_items.push(format!("{} AS {alias}", k.column));
                }
            }
            Projection::Agg(AggExpr { func, column }) => {
                let arg = match column {
                    Some(c) => format!("({c})"),
                    None => "()".to_string(),
                };
                select_items.push(format!("{}{arg} AS {alias}", func.name()));
            }
        }
    }

    let mut sql = format!(
        "CREATE MATERIALIZED VIEW {} AS SELECT {} FROM {} GROUP BY {}",
        v.name,
        select_items.join(", "),
        v.source,
        group_by_cols.join(", ")
    );

    if let Some(sw) = &v.sliding_window {
        let seconds = parse_duration(&sw.interval)?;
        sql.push_str(&format!(
            " WINDOW SLIDING INTERVAL {seconds} SECOND BY {}",
            sw.time_column
        ));
    }

    sql.push(';');
    Ok(sql)
}

// ─── State inference (initial_state → SQL state columns) ────────

/// Mirrors `inferStateFields` in `bindings/typescript/settle/src/ddl.ts:128`:
/// derives the SQL state-column shape (type + DEFAULT) from the runtime
/// `Value` variant of each entry in `initial_state`.
pub(crate) fn infer_state_fields(initial: &RowMap) -> Result<Vec<StateField>> {
    // Sort by name for deterministic DDL across runs.
    let mut keys: Vec<&String> = initial.keys().collect();
    keys.sort();

    let mut out = Vec::with_capacity(keys.len());
    for k in keys {
        let v = &initial[k];
        let (sql_type, default) = match v {
            Value::Float64(n) => ("Float64", format_float(*n)),
            Value::UInt64(n) => ("UInt64", n.to_string()),
            Value::Int64(n) => ("Int64", n.to_string()),
            Value::String(s) => ("String", format!("'{}'", sql_escape(s))),
            Value::Boolean(b) => ("Boolean", if *b { "true".into() } else { "false".into() }),
            Value::DateTime(n) => ("DateTime", n.to_string()),
            Value::JSON(j) => ("Json", format!("'{}'", sql_escape(&j.to_string()))),
            Value::Null => {
                return Err(Error::Schema(format!(
                    "initial_state['{k}'] is Null — cannot infer column type"
                )));
            }
            other => {
                return Err(Error::Schema(format!(
                    "initial_state['{k}']: unsupported Value variant {} for state inference",
                    other.type_name()
                )));
            }
        };
        out.push(StateField {
            name: k.clone(),
            sql_type,
            default,
        });
    }
    Ok(out)
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn format_float(n: f64) -> String {
    if n == n.trunc() && n.is_finite() {
        format!("{n:.1}")
    } else {
        format!("{n}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::ddl::{AggExpr, AggFn, AggProxy, GroupByItem, KeyRef, interval};

    fn agg(func: AggFn, col: Option<&str>) -> AggExpr {
        AggExpr {
            func,
            column: col.map(str::to_string),
        }
    }

    #[test]
    fn table_to_sql_virtual_vs_persisted() {
        let cols = vec![
            ColumnSpec {
                name: "block_number".into(),
                sql_type: "UInt64",
            },
            ColumnSpec {
                name: "asset".into(),
                sql_type: "String",
            },
        ];
        let persisted = table_to_sql(&TableSpec {
            name: "t".into(),
            columns: cols.clone(),
            virtual_table: false,
        });
        assert_eq!(
            persisted,
            "CREATE TABLE t (block_number UInt64, asset String);"
        );

        let virt = table_to_sql(&TableSpec {
            name: "t".into(),
            columns: cols,
            virtual_table: true,
        });
        assert!(virt.starts_with("CREATE VIRTUAL TABLE t ("));
    }

    #[test]
    fn reducer_to_sql_emits_external_language() {
        let sql = reducer_to_sql(&ReducerSpec {
            name: "r".into(),
            source: "t".into(),
            group_by: vec!["a".into(), "b".into()],
            state_fields: vec![StateField {
                name: "v".into(),
                sql_type: "Float64",
                default: "0.0".into(),
            }],
        });
        assert_eq!(
            sql,
            "CREATE REDUCER r SOURCE t GROUP BY a, b STATE (v Float64 DEFAULT 0.0) LANGUAGE EXTERNAL;"
        );
    }

    #[test]
    fn view_to_sql_unaliased_key_and_count() {
        // alias == column → column emitted bare; count() → ()
        let sql = view_to_sql(&ViewSpec {
            name: "v".into(),
            source: "src".into(),
            group_by: vec![GroupByItem::Column("k".into())],
            sliding_window: None,
            projections: vec![
                (
                    "k".into(),
                    Projection::Key(KeyRef { column: "k".into() }),
                ),
                ("n".into(), Projection::Agg(agg(AggFn::Count, None))),
            ],
        })
        .unwrap();
        assert!(sql.contains("SELECT k, count() AS n"), "{sql}");
        assert!(sql.contains("GROUP BY k"));
    }

    #[test]
    fn view_to_sql_aliased_key() {
        let sql = view_to_sql(&ViewSpec {
            name: "v".into(),
            source: "src".into(),
            group_by: vec![GroupByItem::Column("user_id".into())],
            sliding_window: None,
            projections: vec![(
                "user".into(),
                Projection::Key(KeyRef {
                    column: "user_id".into(),
                }),
            )],
        })
        .unwrap();
        assert!(sql.contains("user_id AS user"), "{sql}");
    }

    #[test]
    fn view_to_sql_interval_default_alias() {
        // No .as() → alias defaults to "<col>_interval"
        let sql = view_to_sql(&ViewSpec {
            name: "v".into(),
            source: "src".into(),
            group_by: vec![GroupByItem::Interval(interval("ts", "1 hour"))],
            sliding_window: None,
            projections: vec![(
                "bucket".into(),
                Projection::Key(KeyRef {
                    column: "ts_interval".into(),
                }),
            )],
        })
        .unwrap();
        assert!(
            sql.contains("toStartOfInterval(ts, INTERVAL 3600 SECOND) AS bucket"),
            "{sql}"
        );
        assert!(sql.contains("GROUP BY ts_interval"));
    }

    #[test]
    fn infer_state_fields_covers_all_value_kinds() {
        let initial: RowMap = HashMap::from([
            ("f".into(), Value::Float64(1.5)),
            ("u".into(), Value::UInt64(7)),
            ("i".into(), Value::Int64(-3)),
            ("s".into(), Value::String("hi 'there'".into())),
            ("b".into(), Value::Boolean(true)),
            ("d".into(), Value::DateTime(123456)),
            (
                "j".into(),
                Value::JSON(serde_json::json!({"k":"v"})),
            ),
        ]);
        let fields = infer_state_fields(&initial).unwrap();
        let by_name: HashMap<_, _> = fields
            .into_iter()
            .map(|f| (f.name.clone(), f))
            .collect();
        assert_eq!(by_name["f"].sql_type, "Float64");
        assert_eq!(by_name["f"].default, "1.5");
        assert_eq!(by_name["u"].sql_type, "UInt64");
        assert_eq!(by_name["i"].sql_type, "Int64");
        assert_eq!(by_name["s"].sql_type, "String");
        assert_eq!(by_name["s"].default, "'hi ''there'''");
        assert_eq!(by_name["b"].sql_type, "Boolean");
        assert_eq!(by_name["b"].default, "true");
        assert_eq!(by_name["d"].sql_type, "DateTime");
        assert_eq!(by_name["j"].sql_type, "Json");
    }

    #[test]
    fn infer_state_fields_rejects_null() {
        let initial: RowMap = HashMap::from([("x".into(), Value::Null)]);
        let err = infer_state_fields(&initial).unwrap_err();
        assert!(err.to_string().contains("cannot infer column type"));
    }

    #[test]
    fn infer_state_fields_rejects_unsupported_variant() {
        let initial: RowMap = HashMap::from([("x".into(), Value::Bytes(vec![1, 2, 3]))]);
        let err = infer_state_fields(&initial).unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn format_float_truncates_integers() {
        assert_eq!(format_float(0.0), "0.0");
        assert_eq!(format_float(42.0), "42.0");
        assert_eq!(format_float(1.5), "1.5");
    }

    // Re-export to silence unused-import lint from the agg/AggProxy import
    #[allow(dead_code)]
    fn _unused(_p: &AggProxy) {}
}
