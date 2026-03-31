use std::collections::HashMap;

use crate::types::Value;

/// Convert a serde_json::Value to our internal Value type.
pub fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= 0 {
                    Value::UInt64(i as u64)
                } else {
                    Value::Int64(i)
                }
            } else if let Some(u) = n.as_u64() {
                Value::UInt64(u)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        // Arrays and nested objects stored as JSON strings
        _ => Value::String(v.to_string()),
    }
}

/// Convert our internal Value to a serde_json::Value.
pub fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::UInt64(n) => serde_json::Value::Number((*n).into()),
        Value::Int64(n) => serde_json::Value::Number((*n).into()),
        Value::Float64(n) => serde_json::json!(*n),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::DateTime(n) => serde_json::Value::Number((*n).into()),
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Null => serde_json::Value::Null,
        Value::Bytes(b) => serde_json::Value::String(bytes_to_hex(b)),
        Value::Uint256(b) => serde_json::Value::String(format!("0x{}", bytes_to_hex(b))),
        Value::Base58(b) => serde_json::Value::String(bytes_to_hex(b)),
        Value::JSON(v) => v.clone(),
    }
}

/// Convert a JSON object to a Row (HashMap<String, Value>).
pub fn json_object_to_row(val: &serde_json::Value) -> Option<HashMap<String, Value>> {
    let obj = val.as_object()?;
    let mut row = HashMap::new();
    for (k, v) in obj {
        row.insert(k.clone(), json_to_value(v));
    }
    Some(row)
}

/// Convert a HashMap<String, Value> to a JSON object.
pub fn value_map_to_json(map: &HashMap<String, Value>) -> serde_json::Value {
    let obj: serde_json::Map<String, serde_json::Value> = map
        .iter()
        .map(|(k, v)| (k.clone(), value_to_json(v)))
        .collect();
    serde_json::Value::Object(obj)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_null_to_value() {
        assert_eq!(json_to_value(&serde_json::Value::Null), Value::Null);
    }

    #[test]
    fn json_bool_to_value() {
        assert_eq!(
            json_to_value(&serde_json::json!(true)),
            Value::Boolean(true)
        );
        assert_eq!(
            json_to_value(&serde_json::json!(false)),
            Value::Boolean(false)
        );
    }

    #[test]
    fn json_int_to_value() {
        assert_eq!(json_to_value(&serde_json::json!(42)), Value::UInt64(42));
        assert_eq!(json_to_value(&serde_json::json!(-5)), Value::Int64(-5));
        assert_eq!(json_to_value(&serde_json::json!(0)), Value::UInt64(0));
    }

    /// Issue #9: u64 values > i64::MAX must not be demoted to lossy f64 or Null.
    #[test]
    fn json_large_u64_to_value() {
        let large: u64 = u64::MAX; // 18446744073709551615
        let json = serde_json::json!(large);
        let val = json_to_value(&json);
        assert_eq!(val, Value::UInt64(large));

        // Also test a value just above i64::MAX
        let above_i64: u64 = (i64::MAX as u64) + 1;
        let json = serde_json::json!(above_i64);
        let val = json_to_value(&json);
        assert_eq!(val, Value::UInt64(above_i64));
    }

    #[test]
    fn json_float_to_value() {
        assert_eq!(
            json_to_value(&serde_json::json!(3.14)),
            Value::Float64(3.14)
        );
    }

    #[test]
    fn json_string_to_value() {
        assert_eq!(
            json_to_value(&serde_json::json!("hello")),
            Value::String("hello".into())
        );
    }

    #[test]
    fn json_array_to_value_string() {
        let v = json_to_value(&serde_json::json!([1, 2, 3]));
        assert_eq!(v, Value::String("[1,2,3]".into()));
    }

    #[test]
    fn json_object_to_row_basic() {
        let json = serde_json::json!({
            "user": "alice",
            "amount": 10.5,
            "active": true
        });

        let row = json_object_to_row(&json).unwrap();
        assert_eq!(row.get("user"), Some(&Value::String("alice".into())));
        assert_eq!(row.get("amount"), Some(&Value::Float64(10.5)));
        assert_eq!(row.get("active"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn json_object_to_row_rejects_non_object() {
        assert!(json_object_to_row(&serde_json::json!("not an object")).is_none());
        assert!(json_object_to_row(&serde_json::json!(42)).is_none());
    }

    #[test]
    fn value_to_json_roundtrip() {
        let original = HashMap::from([
            ("name".to_string(), Value::String("alice".into())),
            ("amount".to_string(), Value::Float64(10.5)),
            ("count".to_string(), Value::UInt64(42)),
            ("negative".to_string(), Value::Int64(-7)),
            ("active".to_string(), Value::Boolean(true)),
            ("ts".to_string(), Value::DateTime(1700000000)),
            ("empty".to_string(), Value::Null),
        ]);

        let json = value_map_to_json(&original);
        let row = json_object_to_row(&json).unwrap();

        assert_eq!(row.get("name"), Some(&Value::String("alice".into())));
        assert_eq!(row.get("count"), Some(&Value::UInt64(42)));
        assert_eq!(row.get("active"), Some(&Value::Boolean(true)));
        assert_eq!(row.get("empty"), Some(&Value::Null));
    }

    #[test]
    fn value_uint256_to_json() {
        let mut val = [0u8; 32];
        val[31] = 0xff;
        let json = value_to_json(&Value::Uint256(val));
        let s = json.as_str().unwrap();
        assert!(s.starts_with("0x"));
        assert!(s.ends_with("ff"));
        assert_eq!(s.len(), 66); // "0x" + 64 hex chars
    }

    #[test]
    fn value_bytes_to_json() {
        let json = value_to_json(&Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(json.as_str().unwrap(), "deadbeef");
    }

    #[test]
    fn full_pipeline_json_roundtrip() {
        // Simulate what happens in napi: JSON rows → internal → process → JSON output
        let input_json = serde_json::json!({
            "pool": "ETH/USDC",
            "amount": 100.0,
            "block_number": 1000
        });

        let row = json_object_to_row(&input_json).unwrap();
        assert_eq!(row.get("pool"), Some(&Value::String("ETH/USDC".into())));
        assert_eq!(row.get("amount"), Some(&Value::Float64(100.0)));
        assert_eq!(row.get("block_number"), Some(&Value::UInt64(1000)));

        // Convert back
        let output_json = value_map_to_json(&row);
        let obj = output_json.as_object().unwrap();
        assert_eq!(obj.get("pool").unwrap(), "ETH/USDC");
        assert_eq!(obj.get("amount").unwrap(), 100.0);
        assert_eq!(obj.get("block_number").unwrap(), 1000);
    }
}
