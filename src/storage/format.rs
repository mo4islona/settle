//! The on-disk format version, and the stored layout of each table, reducer
//! and materialized view. Rows, reducer state and MV state are encoded by
//! position, so a schema whose layout differs from the one that wrote them
//! would read them wrong without an error.

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

fn table_layout(def: &TableDef) -> Layout {
    let columns: Vec<String> = def
        .columns
        .iter()
        .map(|c| format!("{} {}", c.name, c.column_type))
        .collect();

    Layout {
        key_space: "table",
        kind: "table",
        name: def.name.clone(),
        text: format!("columns ({})", columns.join(", ")),
    }
}

fn reducer_layout(def: &ReducerDef) -> Layout {
    let state: Vec<String> = def
        .state
        .iter()
        .map(|f| format!("{} {}", f.name, f.column_type))
        .collect();

    Layout {
        key_space: "reducer",
        kind: "reducer",
        name: def.name.clone(),
        text: format!(
            "source {}; group by ({}); state ({})",
            def.source,
            def.group_by.join(", "),
            state.join(", ")
        ),
    }
}

fn mv_layout(def: &MVDef) -> Layout {
    let select: Vec<String> = def.select.iter().map(|item| select_layout(&item.expr)).collect();

    let mut text = format!(
        "source {}; select ({}); group by ({})",
        def.source,
        select.join(", "),
        def.group_by.join(", ")
    );
    if let Some(window) = &def.sliding_window {
        text.push_str(&format!(
            "; sliding window {} s on {}",
            window.interval_seconds, window.time_column
        ));
    }

    Layout {
        key_space: "mv",
        kind: "materialized view",
        name: def.name.clone(),
        text,
    }
}

fn select_layout(expr: &SelectExpr) -> String {
    match expr {
        SelectExpr::Column(column) => column.clone(),
        SelectExpr::Agg(func, column) => {
            format!("{}({})", func.name(), column.as_deref().unwrap_or(""))
        }
        SelectExpr::WindowFunc {
            column,
            interval_seconds,
        } => format!("window({column}, {interval_seconds} s)"),
    }
}
