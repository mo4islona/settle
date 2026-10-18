//! The layout in which the napi binding moves the values of a whole batch
//! across the JS boundary in one call, with no napi call per value. The JS
//! side is `bindings/typescript/settle/src/codec.ts`.
//!
//! Each value has a slot: a tag in `tags` and a number in `nums` at the same
//! index. Counts and column ids go in `ints`. Both are read in order.
//! - `NUMBER`: the number.
//! - `STRING`: Rust → JS, the index of the string in a table of distinct
//!   strings; JS → Rust, the length of the string in UTF-16 units, the
//!   strings being joined in slot order.
//! - `TRUE`, `FALSE`, `NULL`: no number.
//! - `OTHER`: the index in an array of host values: a BigInt, an object,
//!   bytes. Only the host can build or read them.
//!
//! Rust → JS, the slots, the ints and the end of each string (in UTF-16
//! units) go in one `ArrayBuffer`, and the strings in one JS string.

#![cfg_attr(not(feature = "napi"), allow(dead_code))]

#[cfg(feature = "napi")]
pub(crate) mod napi;

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::db::BlockRows;
use crate::reducer_runtime::{Emits, GroupedRows, set_at};
use crate::types::{ChangeBatch, ChangeOp, ColumnRegistry, PerfNode, PerfNodeKind, Value};

pub const NULL: u8 = 0;
pub const NUMBER: u8 = 1;
pub const STRING: u8 = 2;
pub const TRUE: u8 = 3;
pub const FALSE: u8 = 4;
pub const OTHER: u8 = 5;

/// The largest integer that a JS number holds exactly: 2^53 - 1. Larger
/// integers cross as BigInt.
pub const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// u32 words before the sections of an encoded buffer: the slot, int and
/// string counts, and room for more.
const HEADER_WORDS: usize = 8;

/// The value of a JS BigInt from its sign and its 64-bit words, least
/// significant first: UInt64, Int64 or Uint256. None if none of them holds it.
pub fn bigint_to_value(negative: bool, words: &[u64]) -> Option<Value> {
    let len = words.iter().rposition(|&w| w != 0).map_or(0, |i| i + 1);
    let words = &words[..len];

    match (negative, words) {
        (_, []) => Some(Value::UInt64(0)),
        (false, &[w]) => Some(Value::UInt64(w)),
        (true, &[w]) if w <= 1 << 63 => Some(Value::Int64((w as i64).wrapping_neg())),
        (false, _) if words.len() <= 4 => {
            let mut be = [0u8; 32];
            for (i, w) in words.iter().enumerate() {
                be[32 - 8 * (i + 1)..32 - 8 * i].copy_from_slice(&w.to_be_bytes());
            }
            Some(Value::Uint256(be))
        }
        _ => None,
    }
}

/// A JS number in JSON, as the msgpack input made it: a safe integer is an
/// integer; NaN and ±Infinity are null.
pub fn json_number(n: f64) -> serde_json::Value {
    let safe = n.fract() == 0.0 && n.abs() <= MAX_SAFE_INTEGER as f64;
    match (safe, n >= 0.0) {
        (true, true) => serde_json::Value::from(n as u64),
        (true, false) => serde_json::Value::from(n as i64),
        (false, _) => serde_json::Number::from_f64(n).map_or(serde_json::Value::Null, Into::into),
    }
}

/// A JSON number as JS gets it: an integer past 2^53 as a BigInt, as the
/// msgpack output did.
#[derive(Debug, PartialEq)]
pub enum JsNumber {
    Number(f64),
    BigUint(u64),
    BigInt(i64),
}

pub fn js_number(n: &serde_json::Number) -> JsNumber {
    if let Some(u) = n.as_u64() {
        return match u <= MAX_SAFE_INTEGER {
            true => JsNumber::Number(u as f64),
            false => JsNumber::BigUint(u),
        };
    }
    if let Some(i) = n.as_i64() {
        return match i.unsigned_abs() <= MAX_SAFE_INTEGER {
            true => JsNumber::Number(i as f64),
            false => JsNumber::BigInt(i),
        };
    }
    JsNumber::Number(n.as_f64().unwrap_or(f64::NAN))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xf) as usize] as char);
    }
    out
}

fn count(n: usize) -> u32 {
    u32::try_from(n).expect("a count of a batch fits in u32")
}

// ─── Rust → JS ─────────────────────────────────────────────────────

/// Values for JS. Each distinct borrowed string is sent once.
pub struct Encoder<'a> {
    tags: Vec<u8>,
    nums: Vec<f64>,
    ints: Vec<u32>,
    strings: String,
    /// The end of each string in `strings`, in UTF-16 units.
    string_ends: Vec<u32>,
    utf16_len: usize,
    ascii: bool,
    ids: FxHashMap<&'a str, u32>,
    others: Vec<&'a Value>,
    /// Bytes as hex text, as a reducer callback sees them; else as a host
    /// value.
    bytes_as_hex: bool,
}

impl<'a> Encoder<'a> {
    pub fn new(bytes_as_hex: bool) -> Self {
        Self {
            tags: Vec::new(),
            nums: Vec::new(),
            ints: Vec::new(),
            strings: String::new(),
            string_ends: Vec::new(),
            utf16_len: 0,
            ascii: true,
            ids: FxHashMap::default(),
            others: Vec::new(),
            bytes_as_hex,
        }
    }

    pub fn int(&mut self, n: u32) {
        self.ints.push(n);
    }

    pub fn count(&mut self, n: usize) {
        self.ints.push(count(n));
    }

    /// The id of `s` in the string table.
    pub fn string_id(&mut self, s: &'a str) -> u32 {
        if let Some(&id) = self.ids.get(s) {
            return id;
        }
        let id = self.push_string(s);
        self.ids.insert(s, id);
        id
    }

    fn push_string(&mut self, s: &str) -> u32 {
        self.strings.push_str(s);
        let units = if s.is_ascii() {
            s.len()
        } else {
            self.ascii = false;
            s.encode_utf16().count()
        };
        self.utf16_len += units;
        self.string_ends.push(count(self.utf16_len));
        count(self.string_ends.len() - 1)
    }

    fn slot(&mut self, tag: u8, num: f64) {
        self.tags.push(tag);
        self.nums.push(num);
    }

    pub fn number(&mut self, n: f64) {
        self.slot(NUMBER, n);
    }

    pub fn string(&mut self, s: &'a str) {
        let id = self.string_id(s);
        self.slot(STRING, id as f64);
    }

    fn text(&mut self, s: &str) {
        let id = self.push_string(s);
        self.slot(STRING, id as f64);
    }

    pub fn value(&mut self, value: &'a Value) {
        match value {
            Value::Null => self.slot(NULL, 0.0),
            Value::Boolean(true) => self.slot(TRUE, 0.0),
            Value::Boolean(false) => self.slot(FALSE, 0.0),
            Value::Float64(f) => self.number(*f),
            Value::UInt64(n) if *n <= MAX_SAFE_INTEGER => self.number(*n as f64),
            Value::Int64(n) | Value::DateTime(n) if n.unsigned_abs() <= MAX_SAFE_INTEGER => {
                self.number(*n as f64)
            }
            Value::String(s) => self.string(s),
            Value::Uint256(b) => self.text(&format!("0x{}", hex(b))),
            Value::Base58(b) => self.text(&hex(b)),
            Value::Bytes(b) if self.bytes_as_hex => self.text(&hex(b)),
            // Integers past 2^53, bytes, JSON.
            _ => {
                self.slot(OTHER, self.others.len() as f64);
                self.others.push(value);
            }
        }
    }

    /// The bytes of the buffer that `write` fills.
    pub fn byte_len(&self) -> usize {
        4 * HEADER_WORDS
            + 8 * self.nums.len()
            + 4 * self.ints.len()
            + 4 * self.string_ends.len()
            + self.tags.len()
    }

    /// Fill `out` (`byte_len` bytes, 8-aligned) with the header, the
    /// numbers, the ints, the string ends and the tags, in native byte
    /// order.
    pub fn write(&self, out: &mut [u8]) {
        let mut header = [0u32; HEADER_WORDS];
        header[0] = count(self.nums.len());
        header[1] = count(self.ints.len());
        header[2] = count(self.string_ends.len());

        let mut at = 0;
        let mut put = |bytes: &[u8]| {
            out[at..at + bytes.len()].copy_from_slice(bytes);
            at += bytes.len();
        };
        for word in header {
            put(&word.to_ne_bytes());
        }
        for n in &self.nums {
            put(&n.to_ne_bytes());
        }
        for n in self.ints.iter().chain(&self.string_ends) {
            put(&n.to_ne_bytes());
        }
        put(&self.tags);
    }

    /// The strings, joined, and whether they are all ASCII.
    pub fn strings(&self) -> (&str, bool) {
        (&self.strings, self.ascii)
    }

    /// The values of the `OTHER` slots, in order of their index.
    pub fn others(&self) -> &[&'a Value] {
        &self.others
    }
}

/// The groups of a block for a reducer callback (`nativeBatchCallback` in
/// `reducer-callback.ts`).
///
/// ints: group count, state field count, column count, the string id of
/// each state field and each column, the row count of each group. Slots:
/// the state of each group, then the rows group by group, a value per
/// column.
pub fn encode_groups<'a>(block: &'a GroupedRows<'_>, fields: &'a ColumnRegistry) -> Encoder<'a> {
    let mut enc = Encoder::new(true);
    let uniform = match block.rows.first() {
        Some(first) => block
            .rows
            .iter()
            .all(|row| Arc::ptr_eq(row.registry(), first.registry())),
        None => true,
    };
    // Rows of a chained reducer can carry registries of different widths.
    let union = (!uniform).then(|| {
        let mut union = ColumnRegistry::default();
        for row in block.rows {
            for name in row.registry().names() {
                union.get_or_push(name);
            }
        }
        union
    });
    let names: &[String] = match (&union, block.rows.first()) {
        (Some(union), _) => union.names(),
        (None, Some(first)) => first.registry().names(),
        (None, None) => &[],
    };

    enc.count(block.groups.len());
    enc.count(fields.len());
    enc.count(names.len());
    for name in fields.names() {
        let id = enc.string_id(name);
        enc.int(id);
    }
    for name in names {
        let id = enc.push_string(name);
        enc.int(id);
    }
    for group in &block.groups {
        enc.count(group.rows.len());
    }

    for group in &block.groups {
        for value in &group.state {
            enc.value(value);
        }
    }
    for &index in &block.order {
        let row = &block.rows[index];
        if union.is_some() {
            for name in names {
                match row.registry().get_id(name) {
                    Some(id) => enc.value(&row.values()[id as usize]),
                    None => enc.slot(NULL, 0.0),
                }
            }
            continue;
        }
        let values = row.values();
        for value in values.iter().take(names.len()) {
            enc.value(value);
        }
        for _ in values.len()..names.len() {
            enc.slot(NULL, 0.0);
        }
    }
    enc
}

/// The kind name of a perf node, as `codec.ts` lists them.
fn perf_kind(kind: &PerfNodeKind) -> u32 {
    match kind {
        PerfNodeKind::Pipeline => 0,
        PerfNodeKind::RawTable => 1,
        PerfNodeKind::Reducer => 2,
        PerfNodeKind::MV => 3,
        PerfNodeKind::Parallel => 4,
    }
}

/// A change batch for JS (`readBatch` in `codec.ts`).
///
/// Slot: the sequence. Then each head: int 1 and slots number, hash; or
/// int 0. Int: table count; per table: slot name, int record count; per
/// record: int flags (operation | has prev << 2 | new schema << 3); with a
/// new schema: ints key column count, column count, slots table, key
/// column names, column names; then slots key, values, prev values. Last
/// the perf nodes in preorder: int count; per node: ints kind, child count,
/// slots name, duration.
pub fn encode_batch(batch: &ChangeBatch) -> Encoder<'_> {
    let mut enc = Encoder::new(false);
    enc.number(batch.sequence as f64);
    for head in [&batch.finalized_head, &batch.latest_head] {
        match head {
            Some(cursor) => {
                enc.int(1);
                enc.number(cursor.number as f64);
                enc.string(&cursor.hash);
            }
            None => enc.int(0),
        }
    }

    enc.count(batch.tables.len());
    for (name, records) in &batch.tables {
        enc.string(name);
        enc.count(records.len());

        let mut schema = None;
        for record in records {
            let new_schema = !schema.is_some_and(|s| Arc::ptr_eq(s, record.schema()));
            let operation = match record.operation {
                ChangeOp::Insert => 0,
                ChangeOp::Update => 1,
                ChangeOp::Delete => 2,
            };
            enc.int(operation | (record.has_prev() as u32) << 2 | (new_schema as u32) << 3);

            if new_schema {
                let current = record.schema();
                schema = Some(current);
                enc.count(current.key_columns().len());
                enc.count(current.columns().len());
                enc.string(current.table());
                for column in current.key_columns() {
                    enc.string(column);
                }
                for column in current.columns() {
                    enc.string(column);
                }
            }

            for (_, value) in record.key_values() {
                enc.value(value);
            }
            for (_, value) in record.values() {
                enc.value(value);
            }
            if let Some(prev) = record.prev_values() {
                for (_, value) in prev {
                    enc.value(value);
                }
            }
        }
    }

    enc.count(batch.perf.len());
    for node in &batch.perf {
        encode_perf(&mut enc, node);
    }
    enc
}

fn encode_perf<'a>(enc: &mut Encoder<'a>, node: &'a PerfNode) {
    enc.int(perf_kind(&node.kind));
    enc.count(node.children.len());
    enc.string(&node.name);
    enc.number(node.duration_ms);
    for child in &node.children {
        encode_perf(enc, child);
    }
}

// ─── JS → Rust ─────────────────────────────────────────────────────

/// The strings that JS joined, in slot order.
pub enum Strings<'a> {
    /// All ASCII: a UTF-16 unit is a byte.
    Ascii(&'a str),
    Utf16(&'a [u16]),
}

/// A host value of an `OTHER` slot, by its index.
pub type Others<'o> = dyn FnMut(usize) -> Result<Value, String> + 'o;

/// Values from JS, read in order.
pub struct Decoder<'a> {
    tags: &'a [u8],
    nums: &'a [f64],
    ints: &'a [u32],
    strings: Strings<'a>,
    next_int: usize,
    next_slot: usize,
    next_unit: usize,
    /// A number that is a safe integer reads as UInt64 or Int64, as the
    /// msgpack input did; else every number reads as Float64.
    integers: bool,
}

impl<'a> Decoder<'a> {
    /// `tags` and `nums` hold the used slots; `ints` the used ints.
    pub fn new(
        ints: &'a [u32],
        tags: &'a [u8],
        nums: &'a [f64],
        strings: Strings<'a>,
        integers: bool,
    ) -> Result<Self, String> {
        if tags.len() != nums.len() {
            return Err(format!("{} tags for {} numbers", tags.len(), nums.len()));
        }
        Ok(Self {
            tags,
            nums,
            ints,
            strings,
            next_int: 0,
            next_slot: 0,
            next_unit: 0,
            integers,
        })
    }

    pub fn int(&mut self) -> Result<u32, String> {
        let n = *self
            .ints
            .get(self.next_int)
            .ok_or("the ints end before the data")?;
        self.next_int += 1;
        Ok(n)
    }

    /// The next int, as a count or an index.
    pub fn count(&mut self) -> Result<usize, String> {
        self.int().map(|n| n as usize)
    }

    /// The next `units` UTF-16 units of the strings.
    pub fn string(&mut self, units: usize) -> Result<Cow<'a, str>, String> {
        let start = self.next_unit;
        let end = start
            .checked_add(units)
            .ok_or("a string length overflows")?;
        let piece = match self.strings {
            Strings::Ascii(s) => s.get(start..end).map(Cow::Borrowed),
            Strings::Utf16(u) => u.get(start..end).map(|u| Cow::Owned(String::from_utf16_lossy(u))),
        };
        self.next_unit = end;
        piece.ok_or_else(|| "the strings end before the data".to_string())
    }

    pub fn value(&mut self, others: &mut Others<'_>) -> Result<Value, String> {
        let i = self.next_slot;
        let tag = *self.tags.get(i).ok_or("the values end before the data")?;
        let num = self.nums[i];
        self.next_slot += 1;

        match tag {
            NULL => Ok(Value::Null),
            TRUE => Ok(Value::Boolean(true)),
            FALSE => Ok(Value::Boolean(false)),
            NUMBER => Ok(self.number(num)),
            STRING => {
                let units = index(num)?;
                Ok(Value::String(self.string(units)?.into_owned()))
            }
            OTHER => others(index(num)?),
            _ => Err(format!("unknown value tag {tag}")),
        }
    }

    fn number(&self, n: f64) -> Value {
        let safe = n.fract() == 0.0 && n.abs() <= MAX_SAFE_INTEGER as f64;
        match (self.integers && safe, n >= 0.0) {
            (true, true) => Value::UInt64(n as u64),
            (true, false) => Value::Int64(n as i64),
            (false, _) => Value::Float64(n),
        }
    }

    /// Err unless every int, slot and string unit was read.
    pub fn finish(&self) -> Result<(), String> {
        let units = match self.strings {
            Strings::Ascii(s) => s.len(),
            Strings::Utf16(u) => u.len(),
        };
        let complete = self.next_int == self.ints.len()
            && self.next_slot == self.tags.len()
            && self.next_unit == units;
        if complete {
            return Ok(());
        }
        Err(format!(
            "data left after decode: ints {}/{}, values {}/{}, string units {}/{units}",
            self.next_int,
            self.ints.len(),
            self.next_slot,
            self.tags.len(),
            self.next_unit,
        ))
    }
}

fn index(n: f64) -> Result<usize, String> {
    if n >= 0.0 && n.fract() == 0.0 && n <= u32::MAX as f64 {
        Ok(n as usize)
    } else {
        Err(format!("{n} is not an index"))
    }
}

/// The names joined in `strings`, each `lengths[i]` UTF-16 units long.
pub fn split_names(strings: Strings<'_>, lengths: &[u32]) -> Result<Vec<String>, String> {
    let mut dec = Decoder::new(&[], &[], &[], strings, false)?;
    let names = lengths
        .iter()
        .map(|&len| dec.string(len as usize).map(Cow::into_owned))
        .collect::<Result<Vec<_>, _>>()?;
    dec.finish()?;
    Ok(names)
}

/// The output of a reducer callback (`GroupWriter` in `codec.ts`): the new
/// state of each changed group goes into `block`, each emit into `emits`.
/// `columns` names the emit columns by their JS id.
///
/// ints, per group: changed (0 or 1), emit count; per emit: its row in the
/// group, field count, the column id of each field. Slots, per group: the
/// field values of its emits, then its state if it changed.
pub fn decode_group_results(
    dec: &mut Decoder<'_>,
    columns: &[String],
    block: &mut GroupedRows<'_>,
    emits: &mut Emits,
    others: &mut Others<'_>,
) -> Result<(), String> {
    let ids: Vec<_> = columns.iter().map(|name| emits.column(name)).collect();
    let GroupedRows { order, groups, .. } = block;

    for group in groups.iter_mut() {
        let changed = dec.int()?;
        let emit_count = dec.count()?;
        let rows = &order[group.rows.clone()];

        for _ in 0..emit_count {
            let row = dec.count()?;
            let &source = rows.get(row).ok_or_else(|| {
                format!("an emit names row {row} of a group with {} rows", rows.len())
            })?;
            let fields = dec.count()?;

            let mut values = Vec::with_capacity(emits.columns().len());
            for _ in 0..fields {
                let column = dec.count()?;
                let id = *ids
                    .get(column)
                    .ok_or_else(|| format!("emit column {column} of {}", ids.len()))?;
                let value = dec.value(others)?;
                // None past MAX_EMIT_COLUMNS: `Emits::check` fails the block.
                if let Some(id) = id {
                    set_at(&mut values, id, value);
                }
            }
            emits.set_source(source);
            emits.push(values);
        }

        if changed != 0 {
            for value in group.state.iter_mut() {
                *value = dec.value(others)?;
            }
        }
    }
    dec.finish()
}

/// The rows of an ingest (`TableWriter` in `codec.ts`) by table and
/// block, in the column order that `registry` gives.
///
/// ints: table count; per table: name length, row count, row width. The
/// strings start with the table names. Slots, per row: `block_number`, then
/// a value per column of the table.
pub fn decode_ingest<'r>(
    dec: &mut Decoder<'_>,
    registry: impl Fn(&str) -> Option<&'r Arc<ColumnRegistry>>,
    others: &mut Others<'_>,
) -> Result<HashMap<String, BlockRows>, String> {
    let table_count = dec.count()?;
    let mut tables = Vec::with_capacity(table_count.min(1024));
    for _ in 0..table_count {
        let name_len = dec.count()?;
        let rows = dec.count()?;
        let width = dec.count()?;
        tables.push((name_len, rows, width));
    }
    let names = tables
        .iter()
        .map(|&(name_len, ..)| dec.string(name_len).map(Cow::into_owned))
        .collect::<Result<Vec<_>, _>>()?;

    let mut data = HashMap::with_capacity(tables.len());
    for ((_, rows, width), table) in tables.into_iter().zip(names) {
        let Some(registry) = registry(&table) else {
            if rows == 0 {
                continue;
            }
            return Err(format!("unknown table: {table}"));
        };
        if width != registry.len() + 1 {
            return Err(format!(
                "rows of table '{table}' have {width} values, the table has {} columns",
                registry.len()
            ));
        }

        let mut by_block = BlockRows::new();
        for _ in 0..rows {
            let block = match dec.value(others)? {
                Value::UInt64(n) => n,
                Value::Null => return Err(format!("row in table '{table}' missing block_number")),
                other => {
                    return Err(format!(
                        "row in table '{table}' has invalid block_number type: expected UInt64, got {}",
                        other.type_name()
                    ));
                }
            };
            let values = (0..registry.len())
                .map(|_| dec.value(others))
                .collect::<Result<Vec<_>, _>>()?;
            by_block
                .entry(block)
                .or_default()
                .push(crate::types::Row::from_values(registry.clone(), values));
        }
        data.insert(table, by_block);
    }
    dec.finish()?;
    Ok(data)
}

#[cfg(test)]
mod tests;
