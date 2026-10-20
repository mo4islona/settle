use std::collections::HashMap;
use std::sync::Arc;

use smallvec::smallvec;

use super::*;
use crate::reducer_runtime::GroupBatch;
use crate::types::{BlockCursor, ChangeRecord, ChangeSchema, Row};

/// An encoder's buffer, read back the way `codec.ts` reads it.
struct Parsed {
    nums: Vec<f64>,
    ints: Vec<u32>,
    strings: Vec<String>,
    tags: Vec<u8>,
}

impl Parsed {
    fn new(enc: &Encoder<'_>) -> Self {
        let mut buf = vec![0u8; enc.byte_len()];
        enc.write(&mut buf);
        let word = |at: usize| u32::from_ne_bytes(buf[at..at + 4].try_into().unwrap());
        let (slots, ints, strings) = (word(0) as usize, word(4) as usize, word(8) as usize);

        let mut at = 4 * HEADER_WORDS;
        let nums = (0..slots)
            .map(|i| f64::from_ne_bytes(buf[at + 8 * i..at + 8 * i + 8].try_into().unwrap()))
            .collect();
        at += 8 * slots;
        let ints_vec = (0..ints).map(|i| word(at + 4 * i)).collect();
        at += 4 * ints;
        let ends: Vec<usize> = (0..strings).map(|i| word(at + 4 * i) as usize).collect();
        at += 4 * strings;
        let tags = buf[at..at + slots].to_vec();
        assert_eq!(at + slots, buf.len());

        let (joined, ascii) = enc.strings();
        assert_eq!(ascii, joined.is_ascii());
        let units: Vec<u16> = joined.encode_utf16().collect();
        let mut start = 0;
        let strings = ends
            .iter()
            .map(|&end| {
                let s = String::from_utf16(&units[start..end]).unwrap();
                start = end;
                s
            })
            .collect();

        Self {
            nums,
            ints: ints_vec,
            strings,
            tags,
        }
    }

    /// Slot `i` as text: `n:1.5`, `s:alice`, `null`, `true`, `false`, `other:0`.
    fn slot(&self, i: usize) -> String {
        let n = self.nums[i];
        match self.tags[i] {
            NULL => "null".into(),
            TRUE => "true".into(),
            FALSE => "false".into(),
            NUMBER => format!("n:{n}"),
            STRING => format!("s:{}", self.strings[n as usize]),
            OTHER => format!("other:{n}"),
            tag => panic!("tag {tag}"),
        }
    }

    fn slots(&self) -> Vec<String> {
        (0..self.tags.len()).map(|i| self.slot(i)).collect()
    }
}

fn registry(names: &[&str]) -> Arc<ColumnRegistry> {
    Arc::new(ColumnRegistry::new(names.iter().map(|n| n.to_string()).collect()))
}

fn row(registry: &Arc<ColumnRegistry>, values: Vec<Value>) -> Row {
    Row::from_values(registry.clone(), values)
}

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn no_others(i: usize) -> Result<Value, String> {
    Err(format!("no host value {i}"))
}

#[test]
fn bigint_to_value_keeps_every_digit() {
    assert_eq!(bigint_to_value(false, &[]), Some(Value::UInt64(0)));
    assert_eq!(bigint_to_value(true, &[0, 0]), Some(Value::UInt64(0)));
    assert_eq!(bigint_to_value(false, &[u64::MAX, 0]), Some(Value::UInt64(u64::MAX)));
    assert_eq!(bigint_to_value(true, &[5]), Some(Value::Int64(-5)));
    assert_eq!(bigint_to_value(true, &[1 << 63]), Some(Value::Int64(i64::MIN)));
    assert_eq!(bigint_to_value(true, &[(1 << 63) + 1]), None);
    assert_eq!(bigint_to_value(true, &[0, 1]), None);

    let mut two_pow_64 = [0u8; 32];
    two_pow_64[23] = 1;
    assert_eq!(bigint_to_value(false, &[0, 1]), Some(Value::Uint256(two_pow_64)));
    assert_eq!(
        bigint_to_value(false, &[u64::MAX; 4]),
        Some(Value::Uint256([0xff; 32]))
    );
    assert_eq!(bigint_to_value(false, &[0, 0, 0, 0, 1]), None);
}

/// Groups go as their states, then their rows in group order; each
/// distinct string goes once; integers past 2^53 and JSON go to the host.
#[test]
fn groups_carry_states_rows_and_names() {
    let reg = registry(&["block_number", "user", "amount"]);
    let rows = vec![
        row(&reg, vec![Value::UInt64(7), s("alice"), Value::Float64(1.5)]),
        row(&reg, vec![Value::UInt64(7), s("bob"), Value::UInt64(1 << 60)]),
        row(&reg, vec![Value::UInt64(7), s("alice"), Value::Null]),
    ];
    let fields = ColumnRegistry::new(vec!["count".into(), "user".into()]);
    let block = GroupedRows {
        rows: &rows,
        order: vec![0, 2, 1],
        groups: vec![
            GroupBatch {
                state: vec![Value::Int64(-3), s("alice")],
                maps: None,
                rows: 0..2,
            },
            GroupBatch {
                state: vec![Value::JSON(serde_json::json!({"a": 1})), Value::Boolean(true)],
                maps: None,
                rows: 2..3,
            },
        ],
    };

    let enc = encode_groups(&block, &fields);
    let parsed = Parsed::new(&enc);
    let ints = &parsed.ints;
    assert_eq!(ints[..3], [2, 2, 3]);
    let names: Vec<&str> = ints[3..8].iter().map(|&i| parsed.strings[i as usize].as_str()).collect();
    assert_eq!(names, ["count", "user", "block_number", "user", "amount"]);
    assert_eq!(ints[8..], [2, 1]);

    assert_eq!(
        parsed.slots(),
        [
            "n:-3", "s:alice", "other:0", "true", // states
            "n:7", "s:alice", "n:1.5", // row 0
            "n:7", "s:alice", "null", // row 2
            "n:7", "s:bob", "other:1", // row 1
        ]
    );
    assert_eq!(parsed.strings.iter().filter(|s| *s == "alice").count(), 1);
    assert_eq!(
        enc.others(),
        [&Value::JSON(serde_json::json!({"a": 1})), &Value::UInt64(1 << 60)]
    );
}

/// Rows with registries of different widths go by the union of their
/// columns; a column that a row lacks is Null.
#[test]
fn groups_of_rows_with_different_registries() {
    let narrow = registry(&["user"]);
    let wide = registry(&["user", "amount"]);
    let rows = vec![
        row(&narrow, vec![s("a")]),
        row(&wide, vec![s("b"), Value::Float64(2.0)]),
    ];
    let fields = ColumnRegistry::default();
    let block = GroupedRows {
        rows: &rows,
        order: vec![0, 1],
        groups: vec![GroupBatch {
            state: vec![],
            maps: None,
            rows: 0..2,
        }],
    };

    let parsed = Parsed::new(&encode_groups(&block, &fields));
    assert_eq!(parsed.ints[..3], [1, 0, 2]);
    assert_eq!(parsed.slots(), ["s:a", "null", "s:b", "n:2"]);
}

/// String ends count UTF-16 units, so that JS can slice the joined string.
#[test]
fn string_ends_count_utf16_units() {
    let values = [s("é"), s("😀"), s("x")];
    let mut enc = Encoder::new(false);
    for value in &values {
        enc.value(value);
    }

    let parsed = Parsed::new(&enc);
    assert_eq!(parsed.strings, ["é", "😀", "x"]);
    assert!(!enc.strings().1);
}

/// Uint256 goes as 0x hex; bytes as hex text to a reducer, else to the host.
#[test]
fn bytes_go_as_hex_or_to_the_host() {
    let bytes = Value::Bytes(vec![0xab, 0x01]);
    let big = Value::Uint256([1; 32]);

    let mut reducer = Encoder::new(true);
    reducer.value(&bytes);
    reducer.value(&big);
    let parsed = Parsed::new(&reducer);
    assert_eq!(parsed.slots()[0], "s:ab01");
    assert_eq!(parsed.slots()[1], format!("s:0x{}", "01".repeat(32)));

    let mut batch = Encoder::new(false);
    batch.value(&bytes);
    assert_eq!(Parsed::new(&batch).slots(), ["other:0"]);
}

/// A callback output as `GroupWriter` in `codec.ts` writes it.
struct Output {
    ints: Vec<u32>,
    tags: Vec<u8>,
    nums: Vec<f64>,
    strings: String,
}

impl Output {
    fn new() -> Self {
        Self {
            ints: Vec::new(),
            tags: Vec::new(),
            nums: Vec::new(),
            strings: String::new(),
        }
    }

    fn number(&mut self, n: f64) -> &mut Self {
        self.tags.push(NUMBER);
        self.nums.push(n);
        self
    }

    fn string(&mut self, text: &str) -> &mut Self {
        self.tags.push(STRING);
        self.nums.push(text.encode_utf16().count() as f64);
        self.strings.push_str(text);
        self
    }

    fn null(&mut self) -> &mut Self {
        self.tags.push(NULL);
        self.nums.push(0.0);
        self
    }

    fn ints(&mut self, ints: &[u32]) -> &mut Self {
        self.ints.extend_from_slice(ints);
        self
    }

    fn decoder(&self, integers: bool) -> Decoder<'_> {
        assert!(self.strings.is_ascii());
        let strings = Strings::Ascii(&self.strings);
        Decoder::new(&self.ints, &self.tags, &self.nums, strings, integers).unwrap()
    }
}

fn two_groups(rows: &[Row]) -> GroupedRows<'_> {
    GroupedRows {
        rows,
        order: vec![0, 2, 1],
        groups: vec![
            GroupBatch {
                state: vec![Value::Float64(1.0)],
                maps: None,
                rows: 0..2,
            },
            GroupBatch {
                state: vec![Value::Float64(9.0)],
                maps: None,
                rows: 2..3,
            },
        ],
    }
}

/// Emits go to the engine by their row in the block; a changed group takes
/// its new state, an unchanged one keeps it.
#[test]
fn group_results_set_states_and_emits() {
    let reg = registry(&["user"]);
    let rows = vec![row(&reg, vec![s("a")]), row(&reg, vec![s("b")]), row(&reg, vec![s("a")])];
    let mut block = two_groups(&rows);

    let mut out = Output::new();
    // Group 0: changed, two emits.
    out.ints(&[1, 2]);
    out.ints(&[1, 2, 0, 1]).number(5.0).string("late");
    out.ints(&[0, 1, 1]).string("early");
    out.number(2.0);
    // Group 1: unchanged, no emits.
    out.ints(&[0, 0]);

    let columns = ["amount".to_string(), "note".to_string()];
    let mut emits = Emits::with_columns(&["user".to_string()]);
    let mut dec = out.decoder(false);
    decode_group_results(&mut dec, &columns, &mut block, &mut emits, &mut no_others).unwrap();

    assert_eq!(block.groups[0].state, [Value::Float64(2.0)]);
    assert_eq!(block.groups[1].state, [Value::Float64(9.0)]);
    assert_eq!(emits.columns().names(), ["user", "amount", "note"]);
    assert_eq!(
        emits.take(),
        [
            (2, vec![Value::Null, Value::Float64(5.0), s("late")]),
            (0, vec![Value::Null, Value::Null, s("early")]),
        ]
    );
}

#[test]
fn group_results_reject_a_row_that_the_group_lacks() {
    let reg = registry(&["user"]);
    let rows = vec![row(&reg, vec![s("a")]), row(&reg, vec![s("b")]), row(&reg, vec![s("a")])];
    let mut block = two_groups(&rows);

    let mut out = Output::new();
    out.ints(&[0, 1, 2, 0]);
    out.ints(&[0, 0]);

    let mut emits = Emits::default();
    let mut dec = out.decoder(false);
    let err = decode_group_results(&mut dec, &[], &mut block, &mut emits, &mut no_others)
        .unwrap_err();
    assert!(err.contains("names row 2 of a group with 2 rows"), "{err}");
}

#[test]
fn decoder_rejects_data_left_over() {
    let reg = registry(&["user"]);
    let rows = vec![row(&reg, vec![s("a")]), row(&reg, vec![s("b")]), row(&reg, vec![s("a")])];
    let mut block = two_groups(&rows);

    let mut out = Output::new();
    out.ints(&[0, 0, 0, 0]).number(1.0);

    let mut emits = Emits::default();
    let mut dec = out.decoder(false);
    let err = decode_group_results(&mut dec, &[], &mut block, &mut emits, &mut no_others)
        .unwrap_err();
    assert!(err.contains("data left"), "{err}");
}

#[test]
fn decoder_reads_utf16_strings() {
    let units: Vec<u16> = "é😀x".encode_utf16().collect();
    let tags = [STRING, STRING, STRING];
    let nums = [1.0, 2.0, 1.0];
    let mut dec = Decoder::new(&[], &tags, &nums, Strings::Utf16(&units), false).unwrap();

    let values: Vec<Value> = (0..3).map(|_| dec.value(&mut no_others).unwrap()).collect();
    assert_eq!(values, [s("é"), s("😀"), s("x")]);
    dec.finish().unwrap();
}

/// On ingest, a number that is a safe integer is UInt64 or Int64, as with
/// msgpack; else Float64.
#[test]
fn ingest_numbers_follow_msgpack() {
    let tags = [NUMBER; 6];
    let nums = [2.0, -0.0, -3.0, 1.5, 9007199254740992.0, f64::NAN];
    let mut dec = Decoder::new(&[], &tags, &nums, Strings::Ascii(""), true).unwrap();

    let values: Vec<Value> = (0..5).map(|_| dec.value(&mut no_others).unwrap()).collect();
    assert_eq!(
        values,
        [
            Value::UInt64(2),
            Value::UInt64(0),
            Value::Int64(-3),
            Value::Float64(1.5),
            Value::Float64(9007199254740992.0),
        ]
    );
    assert!(matches!(dec.value(&mut no_others).unwrap(), Value::Float64(n) if n.is_nan()));
}

fn trades() -> Arc<ColumnRegistry> {
    registry(&["block_number", "user", "amount"])
}

fn decode_tables(out: &Output) -> Result<HashMap<String, BlockRows>, String> {
    let reg = trades();
    let mut dec = out.decoder(true);
    decode_ingest(
        &mut dec,
        |table| (table == "trades").then_some(&reg),
        &mut no_others,
    )
}

/// The rows of each table go by block, in the column order of the table.
#[test]
fn ingest_rows_by_table_and_block() {
    let mut out = Output::new();
    out.ints(&[1, 6, 2, 4]);
    out.strings.push_str("trades");
    out.number(5.0).number(5.0).string("a").number(1.5);
    out.number(6.0).number(6.0).string("b").number(2.0);

    let data = decode_tables(&out).unwrap();
    let blocks = &data["trades"];
    let values = |block: u64| blocks[&block][0].values().to_vec();
    assert_eq!(values(5), [Value::UInt64(5), s("a"), Value::Float64(1.5)]);
    assert_eq!(values(6), [Value::UInt64(6), s("b"), Value::UInt64(2)]);
}

#[test]
fn ingest_rejects_bad_tables_and_blocks() {
    let error = |build: &dyn Fn(&mut Output)| {
        let mut out = Output::new();
        build(&mut out);
        decode_tables(&out).unwrap_err()
    };

    let unknown = error(&|out| {
        out.ints(&[1, 5, 1, 1]);
        out.strings.push_str("other");
        out.number(1.0);
    });
    assert_eq!(unknown, "unknown table: other");

    let width = error(&|out| {
        out.ints(&[1, 6, 1, 3]);
        out.strings.push_str("trades");
    });
    assert!(width.contains("have 3 values, the table has 3 columns"), "{width}");

    let missing = error(&|out| {
        out.ints(&[1, 6, 1, 4]);
        out.strings.push_str("trades");
        out.null().null().string("a").number(1.0);
    });
    assert_eq!(missing, "row in table 'trades' missing block_number");

    let float = error(&|out| {
        out.ints(&[1, 6, 1, 4]);
        out.strings.push_str("trades");
        out.number(1.5).number(1.5).string("a").number(1.0);
    });
    assert!(float.contains("expected UInt64, got Float64"), "{float}");
}

/// A table that the schema lacks is skipped when it has no rows.
#[test]
fn ingest_skips_an_unknown_table_without_rows() {
    let mut out = Output::new();
    out.ints(&[1, 5, 0, 1]);
    out.strings.push_str("other");

    assert!(decode_tables(&out).unwrap().is_empty());
}

#[test]
fn batch_layout() {
    let schema = Arc::new(ChangeSchema::mv(
        "totals",
        [("user".to_string(), true), ("total".to_string(), false)],
    ));
    let insert = ChangeRecord::new(
        ChangeOp::Insert,
        schema.clone(),
        smallvec![s("alice")],
        vec![Value::UInt64(3)].into(),
        None,
    );
    let update = ChangeRecord::new(
        ChangeOp::Update,
        schema.clone(),
        smallvec![s("bob")],
        vec![Value::UInt64(1 << 60)].into(),
        Some(vec![Value::Float64(0.5)].into()),
    );
    let batch = ChangeBatch {
        sequence: 4,
        finalized_head: None,
        latest_head: Some(BlockCursor {
            number: 10,
            hash: "0xa".into(),
        }),
        tables: HashMap::from([("totals".to_string(), vec![insert, update])]),
        perf: vec![PerfNode {
            kind: PerfNodeKind::MV,
            name: "totals".into(),
            duration_ms: 0.25,
            children: vec![PerfNode {
                kind: PerfNodeKind::Reducer,
                name: "r".into(),
                duration_ms: 0.125,
                children: vec![],
            }],
        }],
    };

    let enc = encode_batch(&batch);
    let parsed = Parsed::new(&enc);
    assert_eq!(
        parsed.ints,
        [
            0, 1, // heads
            1, 2, // one table, two records
            8, 1, 2, // insert, new schema: 1 key column, 2 columns
            1 | 4, // update with prev
            1, 3, 1, // perf: one node, mv, one child
            2, 0, // reducer, no children
        ]
    );
    assert_eq!(
        parsed.slots(),
        [
            "n:4",
            "n:10",
            "s:0xa",
            "s:totals",
            // schema: table, key columns, columns
            "s:totals",
            "s:user",
            "s:user",
            "s:total",
            // insert: key, values
            "s:alice",
            "s:alice",
            "n:3",
            // update: key, values, prev
            "s:bob",
            "s:bob",
            "other:0",
            "s:bob",
            "n:0.5",
            // perf
            "s:totals",
            "n:0.25",
            "s:r",
            "n:0.125",
        ]
    );
    assert_eq!(enc.others(), [&Value::UInt64(1 << 60)]);
}

/// Counts come from JS: a row count that the slots do not back is an
/// error, not a reservation of that many rows.
#[test]
fn ingest_rejects_a_row_count_without_rows() {
    let mut out = Output::new();
    out.ints(&[1, 6, u32::MAX, 4]);
    out.strings.push_str("trades");

    let err = decode_tables(&out).unwrap_err();
    assert!(err.contains("the values end"), "{err}");
}

/// Numbers in JSON follow the msgpack paths: a safe integer is an integer
/// in, a number out; past 2^53 an integer is a BigInt out.
#[test]
fn json_numbers_follow_msgpack() {
    assert_eq!(json_number(5.0), serde_json::json!(5));
    assert_eq!(json_number(-0.0), serde_json::json!(0));
    assert_eq!(json_number(-7.0), serde_json::json!(-7));
    assert_eq!(json_number(1.5), serde_json::json!(1.5));
    assert_eq!(json_number(1e20), serde_json::json!(1e20));
    assert_eq!(json_number(f64::NAN), serde_json::Value::Null);
    assert_eq!(serde_json::json!({ "n": json_number(1e20) }).to_string(), r#"{"n":1e+20}"#);

    let number = |json: serde_json::Value| match json {
        serde_json::Value::Number(n) => js_number(&n),
        other => panic!("{other}"),
    };
    assert_eq!(number(serde_json::json!(5_000_000_000u64)), JsNumber::Number(5e9));
    assert_eq!(number(serde_json::json!(-3)), JsNumber::Number(-3.0));
    assert_eq!(number(serde_json::json!(1u64 << 60)), JsNumber::BigUint(1 << 60));
    assert_eq!(number(serde_json::json!(-(1i64 << 60))), JsNumber::BigInt(-(1 << 60)));
    assert_eq!(number(serde_json::json!(0.5)), JsNumber::Number(0.5));
}
