use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, Serializer};

use crate::db::BlockRows;
use crate::types::{
    ChangeBatch, ChangeOp, ChangeRecord, ColumnId, ColumnRegistry, Row, RowMap, Value,
};

/// Wrapper for Value that deserializes from plain (untagged) msgpack values.
///
/// When data arrives from JavaScript via MessagePack, values are plain:
/// - integers → UInt64/Int64
/// - floats → Float64
/// - strings → String
/// - bools → Boolean
/// - null → Null
/// - binary → Bytes
/// - arrays/maps → String (JSON-stringified, matching json_conv behavior)
struct PlainValue(Value);

impl<'de> Deserialize<'de> for PlainValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(PlainValueVisitor)
    }
}

struct PlainValueVisitor;

impl<'de> Visitor<'de> for PlainValueVisitor {
    type Value = PlainValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a msgpack value")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::Boolean(v)))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<PlainValue, E> {
        if v >= 0 {
            Ok(PlainValue(Value::UInt64(v as u64)))
        } else {
            Ok(PlainValue(Value::Int64(v)))
        }
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::UInt64(v)))
    }

    fn visit_f32<E: de::Error>(self, v: f32) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::Float64(v as f64)))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::Float64(v)))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::String(v.to_owned())))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::String(v)))
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::Bytes(v.to_vec())))
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::Bytes(v)))
    }

    fn visit_none<E: de::Error>(self) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::Null))
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<PlainValue, D::Error> {
        PlainValue::deserialize(deserializer)
    }

    fn visit_unit<E: de::Error>(self) -> Result<PlainValue, E> {
        Ok(PlainValue(Value::Null))
    }

    fn visit_seq<A>(self, seq: A) -> Result<PlainValue, A::Error>
    where
        A: SeqAccess<'de>,
    {
        // Nested arrays → JSON string (matching json_conv behavior)
        let json = serde_json::Value::deserialize(de::value::SeqAccessDeserializer::new(seq))
            .map_err(de::Error::custom)?;
        Ok(PlainValue(Value::String(json.to_string())))
    }

    fn visit_map<A>(self, map: A) -> Result<PlainValue, A::Error>
    where
        A: MapAccess<'de>,
    {
        // Nested maps → JSON string (matching json_conv behavior)
        let json = serde_json::Value::deserialize(de::value::MapAccessDeserializer::new(map))
            .map_err(de::Error::custom)?;
        Ok(PlainValue(Value::String(json.to_string())))
    }
}

fn plain_row_to_row_map(row: HashMap<String, PlainValue>) -> RowMap {
    row.into_iter().map(|(k, PlainValue(v))| (k, v)).collect()
}

/// Decode a msgpack buffer into a table→rows data map.
/// Expected format: msgpack map `{tableName: [{col: val, ...}, ...], ...}`
pub fn decode_data_from_msgpack(buf: &[u8]) -> Result<HashMap<String, Vec<RowMap>>, String> {
    let raw: HashMap<String, Vec<HashMap<String, PlainValue>>> =
        rmp_serde::from_slice(buf).map_err(|e| format!("invalid msgpack: {e}"))?;

    let data = raw
        .into_iter()
        .map(|(table, rows)| {
            let rows = rows.into_iter().map(plain_row_to_row_map).collect();
            (table, rows)
        })
        .collect();

    Ok(data)
}

/// Decode a msgpack buffer `{tableName: [{col: val, ...}, ...], ...}` into
/// rows by block in the column order of each table, the input of
/// [`Settle::ingest_rows`](crate::db::Settle::ingest_rows). `registry` gives
/// the columns of a table; a row column that its table lacks is skipped.
pub fn decode_table_rows_from_msgpack<'r>(
    buf: &[u8],
    registry: impl Fn(&str) -> Option<&'r Arc<ColumnRegistry>>,
) -> Result<HashMap<String, BlockRows>, String> {
    let mut de = rmp_serde::Deserializer::from_read_ref(buf);
    let tables = TablesSeed {
        registry: &registry,
    }
    .deserialize(&mut de)
    .map_err(|e| format!("invalid msgpack: {e}"))?;

    let mut data = HashMap::with_capacity(tables.len());
    for (table, decoded) in tables {
        let rows = match decoded {
            DecodedTable::Unknown { rows: 0 } => continue,
            DecodedTable::Unknown { .. } => return Err(format!("unknown table: {table}")),
            DecodedTable::Rows(rows) => rows,
        };

        let mut by_block = BlockRows::new();
        for (block, row) in rows {
            let block = match block {
                Some(Value::UInt64(n)) => n,
                Some(other) => {
                    return Err(format!(
                        "row in table '{table}' has invalid block_number type: expected UInt64, got {}",
                        other.type_name()
                    ));
                }
                None => return Err(format!("row in table '{table}' missing block_number")),
            };
            by_block.entry(block).or_default().push(row);
        }
        data.insert(table, by_block);
    }
    Ok(data)
}

enum DecodedTable {
    /// A table that `registry` does not know, with its row count.
    Unknown { rows: usize },
    /// Each row with its `block_number` field, if it has one.
    Rows(Vec<(Option<Value>, Row)>),
}

struct TablesSeed<'a, F> {
    registry: &'a F,
}

impl<'de, 'r, F> DeserializeSeed<'de> for TablesSeed<'_, F>
where
    F: Fn(&str) -> Option<&'r Arc<ColumnRegistry>>,
{
    type Value = HashMap<String, DecodedTable>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de, 'r, F> Visitor<'de> for TablesSeed<'_, F>
where
    F: Fn(&str) -> Option<&'r Arc<ColumnRegistry>>,
{
    type Value = HashMap<String, DecodedTable>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a map of table names to rows")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut tables = HashMap::new();
        while let Some(table) = map.next_key::<String>()? {
            let decoded = map.next_value_seed(TableSeed {
                registry: (self.registry)(&table),
            })?;
            tables.insert(table, decoded);
        }
        Ok(tables)
    }
}

struct TableSeed<'r> {
    registry: Option<&'r Arc<ColumnRegistry>>,
}

impl<'de> DeserializeSeed<'de> for TableSeed<'_> {
    type Value = DecodedTable;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for TableSeed<'_> {
    type Value = DecodedTable;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an array of rows")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let Some(registry) = self.registry else {
            let mut rows = 0;
            while seq.next_element::<IgnoredAny>()?.is_some() {
                rows += 1;
            }
            return Ok(DecodedTable::Unknown { rows });
        };

        let mut rows = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(row) = seq.next_element_seed(RowSeed { registry })? {
            rows.push(row);
        }
        Ok(DecodedTable::Rows(rows))
    }
}

struct RowSeed<'r> {
    registry: &'r Arc<ColumnRegistry>,
}

impl<'de> DeserializeSeed<'de> for RowSeed<'_> {
    type Value = (Option<Value>, Row);

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for RowSeed<'_> {
    type Value = (Option<Value>, Row);

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a row object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut values = vec![Value::Null; self.registry.len()];
        let mut block = None;

        while let Some(key) = map.next_key_seed(KeySeed(self.registry))? {
            match key {
                RowKey::Column(id) => values[id as usize] = map.next_value::<PlainValue>()?.0,
                RowKey::Block(id) => {
                    let PlainValue(value) = map.next_value()?;
                    if let Some(id) = id {
                        values[id as usize] = value.clone();
                    }
                    block = Some(value);
                }
                RowKey::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok((block, Row::from_values(self.registry.clone(), values)))
    }
}

enum RowKey {
    Column(ColumnId),
    /// `block_number`, with its position if the table has it.
    Block(Option<ColumnId>),
    Other,
}

struct KeySeed<'r>(&'r ColumnRegistry);

impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = RowKey;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for KeySeed<'_> {
    type Value = RowKey;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a column name")
    }

    fn visit_str<E: de::Error>(self, name: &str) -> Result<RowKey, E> {
        let id = self.0.get_id(name);
        Ok(match id {
            _ if name == "block_number" => RowKey::Block(id),
            Some(id) => RowKey::Column(id),
            None => RowKey::Other,
        })
    }
}

/// Decode a msgpack buffer into a Vec of rows.
/// Expected format: msgpack array `[{col: val, ...}, ...]`
pub fn decode_rows_from_msgpack(buf: &[u8]) -> Result<Vec<RowMap>, String> {
    let raw: Vec<HashMap<String, PlainValue>> =
        rmp_serde::from_slice(buf).map_err(|e| format!("invalid msgpack: {e}"))?;

    Ok(raw.into_iter().map(plain_row_to_row_map).collect())
}

// ── Serialization (Rust → msgpack Buffer) ──────────────────────────

/// Wrapper for serializing Value as plain (untagged) msgpack.
/// Maps Value variants to native msgpack types instead of tagged enums.
struct PlainValueRef<'a>(&'a Value);

impl Serialize for PlainValueRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            Value::UInt64(v) => serializer.serialize_u64(*v),
            Value::Int64(v) => serializer.serialize_i64(*v),
            Value::Float64(v) => serializer.serialize_f64(*v),
            Value::String(v) => serializer.serialize_str(v),
            Value::Boolean(v) => serializer.serialize_bool(*v),
            Value::DateTime(v) => serializer.serialize_i64(*v),
            Value::Null => serializer.serialize_none(),
            Value::Bytes(v) => serializer.serialize_bytes(v),
            Value::Uint256(v) => {
                let mut hex = std::string::String::with_capacity(66);
                hex.push_str("0x");
                for b in v {
                    hex.push_str(&format!("{b:02x}"));
                }
                serializer.serialize_str(&hex)
            }
            Value::Base58(v) => {
                let hex: std::string::String = v.iter().map(|b| format!("{b:02x}")).collect();
                serializer.serialize_str(&hex)
            }
            Value::JSON(v) => v.serialize(serializer),
        }
    }
}

struct BatchRef<'a>(&'a ChangeBatch);

impl Serialize for BatchRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("sequence", &self.0.sequence)?;
        map.serialize_entry(
            "finalizedHead",
            &self.0.finalized_head.as_ref().map(|c| CursorRef(c)),
        )?;
        map.serialize_entry(
            "latestHead",
            &self.0.latest_head.as_ref().map(|c| CursorRef(c)),
        )?;
        map.serialize_entry("tables", &TablesRef(&self.0.tables))?;
        let perf_refs: Vec<PerfNodeRef> = self.0.perf.iter().map(PerfNodeRef).collect();
        map.serialize_entry("perf", &perf_refs)?;
        map.end()
    }
}

struct PerfNodeRef<'a>(&'a crate::types::PerfNode);

impl Serialize for PerfNodeRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry(
            "kind",
            match self.0.kind {
                crate::types::PerfNodeKind::Pipeline => "pipeline",
                crate::types::PerfNodeKind::RawTable => "raw_table",
                crate::types::PerfNodeKind::Reducer => "reducer",
                crate::types::PerfNodeKind::MV => "mv",
                crate::types::PerfNodeKind::Parallel => "parallel",
            },
        )?;
        map.serialize_entry("name", &self.0.name)?;
        map.serialize_entry("durationMs", &self.0.duration_ms)?;
        let children: Vec<PerfNodeRef> = self.0.children.iter().map(PerfNodeRef).collect();
        map.serialize_entry("children", &children)?;
        map.end()
    }
}

/// Wrapper for serializing HashMap<String, Vec<ChangeRecord>> as a map of table_name → records array.
struct TablesRef<'a>(&'a HashMap<String, Vec<ChangeRecord>>);

impl Serialize for TablesRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (table_name, records) in self.0 {
            let refs: Vec<RecordRef> = records.iter().map(RecordRef).collect();
            map.serialize_entry(table_name, &refs)?;
        }
        map.end()
    }
}

struct CursorRef<'a>(&'a crate::types::BlockCursor);

impl Serialize for CursorRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("number", &self.0.number)?;
        map.serialize_entry("hash", &self.0.hash)?;
        map.end()
    }
}

struct RecordRef<'a>(&'a ChangeRecord);

impl Serialize for RecordRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("table", self.0.table())?;
        map.serialize_entry(
            "operation",
            match self.0.operation {
                ChangeOp::Insert => "insert",
                ChangeOp::Update => "update",
                ChangeOp::Delete => "delete",
            },
        )?;
        map.serialize_entry("key", &PlainColumns(self.0.key_values()))?;
        map.serialize_entry("values", &PlainColumns(self.0.values()))?;
        map.serialize_entry("prevValues", &self.0.prev_values().map(PlainColumns))?;
        map.end()
    }
}

/// `(name, value)` pairs as a map of plain values.
struct PlainColumns<I>(I);

impl<'a, I> Serialize for PlainColumns<I>
where
    I: ExactSizeIterator<Item = (&'a str, &'a Value)> + Clone,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in self.0.clone() {
            map.serialize_entry(name, &PlainValueRef(value))?;
        }
        map.end()
    }
}

/// Encode a ChangeBatch into a msgpack buffer with plain (untagged) values.
pub fn encode_batch_to_msgpack(batch: &ChangeBatch) -> Vec<u8> {
    rmp_serde::to_vec(&BatchRef(batch)).expect("ChangeBatch serialization should never fail")
}

/// Encode a ChangeBatch into a `serde_json::Value` with plain (untagged) values.
///
/// Used by the WASM binding to produce a JS-friendly object (via
/// `serde_wasm_bindgen::to_value`) with the same field names and value
/// representation as the msgpack path.
#[cfg(feature = "wasm")]
pub fn encode_batch_to_json_value(batch: &ChangeBatch) -> serde_json::Value {
    serde_json::to_value(BatchRef(batch)).expect("ChangeBatch serialization should never fail")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use smallvec::smallvec;

    use super::*;
    use crate::types::ChangeSchema;

    /// Encode a value as msgpack bytes (using serde_json::Value as proxy).
    fn encode_json_as_msgpack(val: &serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec(val).unwrap()
    }

    #[test]
    fn decode_rows_basic_types() {
        let json = serde_json::json!([
            {
                "user": "alice",
                "amount": 10.5,
                "count": 42,
                "active": true,
                "extra": null
            }
        ]);
        let buf = encode_json_as_msgpack(&json);
        let rows = decode_rows_from_msgpack(&buf).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("user"), Some(&Value::String("alice".into())));
        assert_eq!(rows[0].get("amount"), Some(&Value::Float64(10.5)));
        assert_eq!(rows[0].get("count"), Some(&Value::UInt64(42)));
        assert_eq!(rows[0].get("active"), Some(&Value::Boolean(true)));
        assert_eq!(rows[0].get("extra"), Some(&Value::Null));
    }

    #[test]
    fn decode_rows_negative_int() {
        let json = serde_json::json!([{"value": -5}]);
        let buf = encode_json_as_msgpack(&json);
        let rows = decode_rows_from_msgpack(&buf).unwrap();

        assert_eq!(rows[0].get("value"), Some(&Value::Int64(-5)));
    }

    #[test]
    fn decode_rows_nested_array() {
        let json = serde_json::json!([{"tags": [1, 2, 3]}]);
        let buf = encode_json_as_msgpack(&json);
        let rows = decode_rows_from_msgpack(&buf).unwrap();

        assert_eq!(rows[0].get("tags"), Some(&Value::String("[1,2,3]".into())));
    }

    #[test]
    fn decode_rows_nested_object() {
        let json = serde_json::json!([{"meta": {"key": "val"}}]);
        let buf = encode_json_as_msgpack(&json);
        let rows = decode_rows_from_msgpack(&buf).unwrap();

        let meta = rows[0].get("meta").unwrap().as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(meta).unwrap();
        assert_eq!(parsed, serde_json::json!({"key": "val"}));
    }

    #[test]
    fn decode_data_multi_table() {
        let json = serde_json::json!({
            "swaps": [
                {"pool": "ETH/USDC", "amount": 100.0, "block_number": 1000},
                {"pool": "ETH/USDC", "amount": 200.0, "block_number": 1001}
            ],
            "transfers": [
                {"wallet": "alice", "amount": 50.0, "block_number": 1000}
            ]
        });
        let buf = encode_json_as_msgpack(&json);
        let data = decode_data_from_msgpack(&buf).unwrap();

        assert_eq!(data.len(), 2);
        assert_eq!(data["swaps"].len(), 2);
        assert_eq!(data["transfers"].len(), 1);
        assert_eq!(
            data["swaps"][0].get("pool"),
            Some(&Value::String("ETH/USDC".into()))
        );
        assert_eq!(
            data["swaps"][0].get("block_number"),
            Some(&Value::UInt64(1000))
        );
    }

    #[test]
    fn decode_empty_buffer_fails() {
        assert!(decode_rows_from_msgpack(&[]).is_err());
        assert!(decode_data_from_msgpack(&[]).is_err());
    }

    #[test]
    fn decode_empty_array() {
        let json = serde_json::json!([]);
        let buf = encode_json_as_msgpack(&json);
        let rows = decode_rows_from_msgpack(&buf).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn decode_matches_json_conv() {
        // Verify that msgpack decode produces the same result as json_conv
        use crate::json_conv::json_object_to_row;

        let json = serde_json::json!({
            "user": "alice",
            "amount": 100.0,
            "count": 42,
            "active": true,
            "empty": null
        });

        // json_conv path
        let json_row = json_object_to_row(&json).unwrap();

        // msgpack path
        let buf = encode_json_as_msgpack(&serde_json::json!([json]));
        let msgpack_rows = decode_rows_from_msgpack(&buf).unwrap();

        assert_eq!(json_row, msgpack_rows[0]);
    }

    #[test]
    fn encode_batch_roundtrip() {
        use crate::types::{BlockCursor, ChangeOp};

        let batch = ChangeBatch {
            sequence: 1,
            finalized_head: Some(BlockCursor {
                number: 900,
                hash: "0xabc".into(),
            }),
            latest_head: Some(BlockCursor {
                number: 1000,
                hash: "0xdef".into(),
            }),
            tables: HashMap::from([(
                "swaps".into(),
                vec![ChangeRecord::new(
                    ChangeOp::Insert,
                    Arc::new(ChangeSchema::raw_table(
                        "swaps",
                        &["block_number".into(), "pool".into(), "amount".into()],
                    )),
                    smallvec![Value::UInt64(1000), Value::UInt64(0)],
                    Box::from([
                        Value::UInt64(1000),
                        Value::String("ETH/USDC".into()),
                        Value::Float64(100.5),
                    ]),
                    None,
                )],
            )]),
            perf: vec![],
        };

        let buf = encode_batch_to_msgpack(&batch);

        // Decode back as generic msgpack and verify structure
        let decoded: serde_json::Value = rmp_serde::from_slice(&buf).unwrap();
        assert_eq!(decoded["sequence"], 1);
        assert_eq!(decoded["finalizedHead"]["number"], 900);
        assert_eq!(decoded["finalizedHead"]["hash"], "0xabc");
        assert_eq!(decoded["latestHead"]["number"], 1000);
        assert_eq!(decoded["tables"]["swaps"][0]["table"], "swaps");
        assert_eq!(decoded["tables"]["swaps"][0]["operation"], "insert");
        // Values are plain (not tagged enums)
        assert_eq!(decoded["tables"]["swaps"][0]["values"]["amount"], 100.5);
        assert_eq!(decoded["tables"]["swaps"][0]["values"]["pool"], "ETH/USDC");
        assert_eq!(decoded["tables"]["swaps"][0]["key"]["block_number"], 1000);
        assert!(decoded["tables"]["swaps"][0]["prevValues"].is_null());
    }

    #[test]
    fn encode_batch_with_prev_values() {
        use crate::types::ChangeOp;

        let batch = ChangeBatch {
            sequence: 2,
            finalized_head: None,
            latest_head: None,
            tables: HashMap::from([(
                "volume".into(),
                vec![ChangeRecord::new(
                    ChangeOp::Update,
                    Arc::new(ChangeSchema::mv(
                        "volume",
                        [("pool".into(), true), ("total".into(), false)],
                    )),
                    smallvec![Value::String("ETH".into())],
                    Box::from([Value::Float64(300.0)]),
                    Some(Box::from([Value::Float64(200.0)])),
                )],
            )]),
            perf: vec![],
        };

        let buf = encode_batch_to_msgpack(&batch);
        let decoded: serde_json::Value = rmp_serde::from_slice(&buf).unwrap();

        assert_eq!(decoded["tables"]["volume"][0]["operation"], "update");
        assert_eq!(decoded["tables"]["volume"][0]["prevValues"]["total"], 200.0);
        // An MV row carries its GROUP BY columns in both key and values.
        assert_eq!(decoded["tables"]["volume"][0]["key"]["pool"], "ETH");
        assert_eq!(decoded["tables"]["volume"][0]["values"]["pool"], "ETH");
        assert_eq!(decoded["tables"]["volume"][0]["prevValues"]["pool"], "ETH");
        assert!(decoded["finalizedHead"].is_null());
    }

    #[test]
    fn encode_empty_batch() {
        let batch = ChangeBatch {
            sequence: 0,
            finalized_head: None,
            latest_head: None,
            tables: HashMap::new(),
            perf: vec![],
        };

        let buf = encode_batch_to_msgpack(&batch);
        let decoded: serde_json::Value = rmp_serde::from_slice(&buf).unwrap();
        assert_eq!(decoded["tables"].as_object().unwrap().len(), 0);
    }

    fn decode_positional(json: serde_json::Value) -> Result<HashMap<String, BlockRows>, String> {
        let swaps = Arc::new(ColumnRegistry::new(vec!["pool".into(), "amount".into()]));
        let trades = Arc::new(ColumnRegistry::new(vec![
            "block_number".into(),
            "user".into(),
        ]));
        decode_table_rows_from_msgpack(&encode_json_as_msgpack(&json), |table| match table {
            "swaps" => Some(&swaps),
            "trades" => Some(&trades),
            _ => None,
        })
    }

    #[test]
    fn decode_table_rows_by_position() {
        let data = decode_positional(serde_json::json!({
            "swaps": [
                {"amount": 7, "block_number": 11, "extra": [1, 2], "pool": "ETH"},
                {"pool": "BTC", "block_number": 10},
                {"block_number": 11, "pool": "SOL", "amount": -3},
            ],
            "trades": [{"user": "alice", "block_number": 10}],
            "orders": [],
        }))
        .unwrap();

        assert_eq!(data.len(), 2);
        let values = |table: &str, block| -> Vec<Vec<Value>> {
            data[table][&block]
                .iter()
                .map(|r| r.values().to_vec())
                .collect()
        };
        assert_eq!(
            values("swaps", 10),
            vec![vec![Value::String("BTC".into()), Value::Null]]
        );
        assert_eq!(
            values("swaps", 11),
            vec![
                vec![Value::String("ETH".into()), Value::UInt64(7)],
                vec![Value::String("SOL".into()), Value::Int64(-3)],
            ]
        );
        assert_eq!(
            values("trades", 10),
            vec![vec![Value::UInt64(10), Value::String("alice".into())]]
        );
    }

    #[test]
    fn decode_table_rows_errors() {
        let err = |json| decode_positional(json).unwrap_err();

        assert_eq!(
            err(serde_json::json!({"orders": [{"block_number": 1}]})),
            "unknown table: orders"
        );
        assert_eq!(
            err(serde_json::json!({"swaps": [{"pool": "ETH"}]})),
            "row in table 'swaps' missing block_number"
        );
        assert_eq!(
            err(serde_json::json!({"swaps": [{"block_number": 1.5}]})),
            "row in table 'swaps' has invalid block_number type: expected UInt64, got Float64"
        );
        assert!(err(serde_json::json!({"swaps": [1]})).starts_with("invalid msgpack"));
        assert!(err(serde_json::json!([])).starts_with("invalid msgpack"));
    }
}
