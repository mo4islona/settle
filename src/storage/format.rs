//! The on-disk format version, and the stored layout of each table, reducer
//! and materialized view. Rows, reducer state and MV state are encoded by
//! position, so a schema whose layout differs from the one that wrote them
//! would read them wrong without an error.

use serde::Serialize;

use crate::error::{Error, Result};
use crate::schema::ast::{MVDef, ReducerDef, Schema, SelectExpr, TableDef};

use super::{StorageBackend, StorageWriteBatch};

/// The version of what is stored and how. Bump it on any change to either:
/// open refuses a database of another version.
pub const FORMAT_VERSION: u32 = 1;

const META_FORMAT_VERSION: &str = "format_version";

/// Meta writes of what storage lacks: the format version, layouts of new
/// objects. Each write batch carries them until one commits, so a database
/// holds a version exactly when it holds data.
pub type Stamp = Vec<(String, Vec<u8>)>;

/// Check the stored format version and the stored layout of each object in
/// `schema`. `has_data` tells a database that holds data from a new one;
/// only the latter may lack the version.
pub fn check(storage: &dyn StorageBackend, schema: &Schema, has_data: bool) -> Result<Stamp> {
    let mut stamp = Stamp::new();

    match storage.get_meta(META_FORMAT_VERSION)? {
        Some(bytes) => check_version(&bytes)?,
        None if has_data => {
            return Err(Error::IncompatibleStorage(
                "the database has no format version: a Settle version before format \
                 versions wrote it. Rebuild the database from the source data"
                    .into(),
            ));
        }
        None => stamp.push((META_FORMAT_VERSION.into(), FORMAT_VERSION.to_be_bytes().into())),
    }

    let layouts = schema
        .tables
        .iter()
        .map(table_layout)
        .chain(schema.reducers.iter().map(reducer_layout))
        .chain(schema.materialized_views.iter().map(mv_layout));
    for layout in layouts {
        stamp.extend(layout.check(storage)?);
    }

    Ok(stamp)
}

/// Check the layout of a reducer added after open.
pub fn check_reducer(storage: &dyn StorageBackend, def: &ReducerDef) -> Result<Stamp> {
    Ok(reducer_layout(def).check(storage)?.into_iter().collect())
}

pub fn write_stamp(stamp: &Stamp, batch: &mut StorageWriteBatch) {
    for (key, value) in stamp {
        batch.put_meta(key, value);
    }
}

fn check_version(bytes: &[u8]) -> Result<()> {
    let version = bytes
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| Error::Storage("corrupt format_version metadata".into()))?;

    if version == FORMAT_VERSION {
        return Ok(());
    }

    let writer = if version > FORMAT_VERSION { "a newer" } else { "an older" };
    Err(Error::IncompatibleStorage(format!(
        "the database has format version {version}, {writer} Settle version wrote it; \
         this version reads only format version {FORMAT_VERSION}. Rebuild the database \
         from the source data"
    )))
}

/// What decides how the stored data of one object reads back. Logic that
/// only changes new results (reducer bodies, defaults, output aliases) is
/// not part of it.
struct Layout {
    /// Tables, reducers and MVs have separate key spaces in storage.
    key_space: &'static str,
    kind: &'static str,
    name: String,
    text: String,
}

impl Layout {
    fn meta_key(&self) -> String {
        format!("layout:{}:{}", self.key_space, self.name)
    }

    /// The meta write that stores this layout, if storage has none yet.
    fn check(&self, storage: &dyn StorageBackend) -> Result<Option<(String, Vec<u8>)>> {
        let key = self.meta_key();
        let Some(stored) = storage.get_meta(&key)? else {
            return Ok(Some((key, self.text.clone().into_bytes())));
        };
        if stored == self.text.as_bytes() {
            return Ok(None);
        }

        let stored = String::from_utf8_lossy(&stored);
        Err(Error::IncompatibleStorage(format!(
            "{} '{}' does not match the layout of its stored data, which it would read \
             wrong. Stored: {stored}. Schema: {}. Keep the stored layout, give the {} a \
             new name, or rebuild the database from the source data",
            self.kind, self.name, self.text, self.kind
        )))
    }
}

/// Layouts are stored as JSON of these structs: identifiers are escaped and
/// each select expression is tagged, so two different layouts never share a
/// text. Field order is the declaration order.
#[derive(Serialize)]
struct TableLayout<'a> {
    columns: Vec<TypedName<'a>>,
}

#[derive(Serialize)]
struct ReducerLayout<'a> {
    source: &'a str,
    group_by: &'a [String],
    state: Vec<TypedName<'a>>,
}

#[derive(Serialize)]
struct MvLayout<'a> {
    source: &'a str,
    select: Vec<SelectLayout<'a>>,
    group_by: &'a [String],
    sliding_window: Option<SlidingLayout<'a>>,
}

#[derive(Serialize)]
struct TypedName<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    column_type: String,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum SelectLayout<'a> {
    Column(&'a str),
    Agg {
        func: &'static str,
        column: Option<&'a str>,
    },
    Window {
        column: &'a str,
        seconds: u64,
    },
}

#[derive(Serialize)]
struct SlidingLayout<'a> {
    seconds: u64,
    time_column: &'a str,
}

fn to_text(layout: &impl Serialize) -> String {
    serde_json::to_string(layout).expect("a layout serializes")
}

fn table_layout(def: &TableDef) -> Layout {
    let layout = TableLayout {
        columns: def
            .columns
            .iter()
            .map(|c| TypedName {
                name: &c.name,
                column_type: c.column_type.to_string(),
            })
            .collect(),
    };

    Layout {
        key_space: "table",
        kind: "table",
        name: def.name.clone(),
        text: to_text(&layout),
    }
}

fn reducer_layout(def: &ReducerDef) -> Layout {
    let layout = ReducerLayout {
        source: &def.source,
        group_by: &def.group_by,
        state: def
            .state
            .iter()
            .map(|f| TypedName {
                name: &f.name,
                column_type: f.type_name(),
            })
            .collect(),
    };

    Layout {
        key_space: "reducer",
        kind: "reducer",
        name: def.name.clone(),
        text: to_text(&layout),
    }
}

fn mv_layout(def: &MVDef) -> Layout {
    let layout = MvLayout {
        source: &def.source,
        select: def.select.iter().map(|item| select_layout(&item.expr)).collect(),
        group_by: &def.group_by,
        sliding_window: def.sliding_window.as_ref().map(|w| SlidingLayout {
            seconds: w.interval_seconds,
            time_column: &w.time_column,
        }),
    };

    Layout {
        key_space: "mv",
        kind: "materialized view",
        name: def.name.clone(),
        text: to_text(&layout),
    }
}

fn select_layout(expr: &SelectExpr) -> SelectLayout<'_> {
    match expr {
        SelectExpr::Column(column) => SelectLayout::Column(column),
        SelectExpr::Agg(func, column) => SelectLayout::Agg {
            func: func.name(),
            column: column.as_deref(),
        },
        SelectExpr::WindowFunc {
            column,
            interval_seconds,
        } => SelectLayout::Window {
            column,
            seconds: *interval_seconds,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::{AggFunc, ColumnDef, SelectItem};
    use crate::types::ColumnType;

    fn mv(select: Vec<SelectExpr>) -> MVDef {
        MVDef {
            name: "v".into(),
            source: "t".into(),
            select: select
                .into_iter()
                .map(|expr| SelectItem { expr, alias: None })
                .collect(),
            group_by: vec![],
            sliding_window: None,
        }
    }

    fn table(columns: &[(&str, ColumnType)]) -> TableDef {
        TableDef {
            name: "t".into(),
            columns: columns
                .iter()
                .map(|(name, column_type)| ColumnDef {
                    name: name.to_string(),
                    column_type: column_type.clone(),
                })
                .collect(),
            virtual_table: false,
        }
    }

    /// A quoted column may be named like an aggregate.
    #[test]
    fn column_named_like_an_aggregate_has_its_own_layout() {
        let column = mv(vec![SelectExpr::Column("sum(a)".into())]);
        let agg = mv(vec![SelectExpr::Agg(AggFunc::Sum, Some("a".into()))]);
        assert_ne!(mv_layout(&column).text, mv_layout(&agg).text);

        let window = mv(vec![SelectExpr::WindowFunc {
            column: "a".into(),
            interval_seconds: 60,
        }]);
        let count = mv(vec![SelectExpr::Agg(AggFunc::Count, None)]);
        let column = mv(vec![SelectExpr::Column("count()".into())]);
        assert_ne!(mv_layout(&window).text, mv_layout(&column).text);
        assert_ne!(mv_layout(&count).text, mv_layout(&column).text);
    }

    #[test]
    fn column_names_with_separators_have_their_own_layout() {
        let one = table(&[("a UInt64, b", ColumnType::String)]);
        let two = table(&[("a", ColumnType::UInt64), ("b", ColumnType::String)]);
        assert_ne!(table_layout(&one).text, table_layout(&two).text);

        let quoted = table(&[(r#"a","type":"UInt64"},{"name":"b"#, ColumnType::String)]);
        assert_ne!(table_layout(&quoted).text, table_layout(&two).text);
    }
}
