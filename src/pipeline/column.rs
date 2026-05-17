//! Column-type factories — Rust mirror of `@settle/stream` column functions.
//!
//! Each factory returns a [`ColumnType`] carrying the SQL fragment used by the
//! DDL generator (e.g. `uint64()` → `ColumnType { sql: "UInt64" }`). The SQL
//! names match what `src/schema/parser.rs` accepts (case-insensitive).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnType {
    pub sql: &'static str,
}

impl ColumnType {
    const fn new(sql: &'static str) -> Self {
        Self { sql }
    }
}

pub fn uint64() -> ColumnType {
    ColumnType::new("UInt64")
}
pub fn int64() -> ColumnType {
    ColumnType::new("Int64")
}
pub fn float64() -> ColumnType {
    ColumnType::new("Float64")
}
pub fn uint256() -> ColumnType {
    ColumnType::new("Uint256")
}
pub fn string() -> ColumnType {
    ColumnType::new("String")
}
pub fn datetime() -> ColumnType {
    ColumnType::new("DateTime")
}
pub fn boolean() -> ColumnType {
    ColumnType::new("Boolean")
}
pub fn bytes() -> ColumnType {
    ColumnType::new("Bytes")
}
pub fn base58() -> ColumnType {
    ColumnType::new("Base58")
}
pub fn json() -> ColumnType {
    ColumnType::new("Json")
}
