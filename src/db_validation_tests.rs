use super::test_helpers::*;
use super::*;
use crate::schema::ast::MAX_MV_AGGREGATES;
use crate::types::{BlockCursor, Value};
use std::collections::HashMap;

/// ingest() must reject non-UInt64 block_number values.
#[test]
fn ingest_rejects_negative_block_number() {
    let schema = r#"
        CREATE TABLE t (block_number UInt64, x Float64);
    "#;
    let mut db = Settle::open(Config::new(schema)).unwrap();
    let result = ingest_input(&mut db, IngestInput {
        data: HashMap::from([(
            "t".to_string(),
            vec![HashMap::from([
                ("block_number".to_string(), Value::Int64(-1)),
                ("x".to_string(), Value::Float64(1.0)),
            ])],
        )]),
        rollback_chain: vec![],
        finalized_head: BlockCursor {
            number: 0,
            hash: "0x0".into(),
        },
    });
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid block_number type")
    );
}

#[test]
fn ingest_rejects_float_block_number() {
    let schema = r#"
        CREATE TABLE t (block_number UInt64, x Float64);
    "#;
    let mut db = Settle::open(Config::new(schema)).unwrap();
    let result = ingest_input(&mut db, IngestInput {
        data: HashMap::from([(
            "t".to_string(),
            vec![HashMap::from([
                ("block_number".to_string(), Value::Float64(1.5)),
                ("x".to_string(), Value::Float64(1.0)),
            ])],
        )]),
        rollback_chain: vec![],
        finalized_head: BlockCursor {
            number: 0,
            hash: "0x0".into(),
        },
    });
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid block_number type")
    );
}

/// Partial ingest failure must rollback in-memory state so retries don't
/// double-count previously processed blocks.
#[test]
fn partial_ingest_failure_rolls_back() {
    let mut db = Settle::open(Config::new(SIMPLE_SCHEMA)).unwrap();

    // Successful ingest — block 1000
    ingest_input(&mut db, IngestInput {
        data: HashMap::from([(
            "swaps".to_string(),
            vec![{
                let mut r = make_swap("ETH/USDC", 100.0);
                r.insert("block_number".to_string(), Value::UInt64(1000));
                r
            }],
        )]),
        rollback_chain: vec![BlockCursor {
            number: 1000,
            hash: "0x0".into(),
        }],
        finalized_head: BlockCursor {
            number: 1000,
            hash: "0x0".into(),
        },
    })
    .unwrap();

    // Failed ingest — block 1001 has invalid block_number type
    let err = ingest_input(&mut db, IngestInput {
        data: HashMap::from([(
            "swaps".to_string(),
            vec![{
                let mut r = make_swap("ETH/USDC", 200.0);
                r.insert("block_number".to_string(), Value::Int64(-1)); // invalid!
                r
            }],
        )]),
        rollback_chain: vec![],
        finalized_head: BlockCursor {
            number: 1000,
            hash: "0x0".into(),
        },
    });
    assert!(err.is_err());

    // Retry with valid data — should NOT double-count block 1000
    let batch = ingest_input(&mut db, IngestInput {
            data: HashMap::from([(
                "swaps".to_string(),
                vec![{
                    let mut r = make_swap("ETH/USDC", 200.0);
                    r.insert("block_number".to_string(), Value::UInt64(1001));
                    r
                }],
            )]),
            rollback_chain: vec![BlockCursor {
                number: 1001,
                hash: "0x1".into(),
            }],
            finalized_head: BlockCursor {
                number: 1000,
                hash: "0x0".into(),
            },
        })
        .unwrap()
        .expect("retry should produce a batch");
    let mv = batch.records_for("pool_volume");
    let eth_rec = mv
        .iter()
        .find(|r| r.key.get("pool") == Some(&Value::String("ETH/USDC".into())));
    assert!(eth_rec.is_some());
    let total = eth_rec
        .unwrap()
        .values
        .get("total_volume")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    // Should be 100 + 200 = 300 (not 100 + 100 + 200 = 400 from double-count)
    assert!(
        (total - 300.0).abs() < 0.01,
        "expected 300 after rollback+retry, got {total}"
    );
}

/// Failed ingest must not leak partial changes into the buffer.
/// On retry, only the retry's changes should appear in the output.
#[test]
fn failed_ingest_does_not_leak_changes_to_buffer() {
    let schema = r#"
        CREATE TABLE t (block_number UInt64, x Float64);
        CREATE MATERIALIZED VIEW mv AS
          SELECT SUM(x) AS total FROM t GROUP BY x;
    "#;
    let mut db = Settle::open(Config::new(schema)).unwrap();

    // First ingest succeeds — consumed by flush inside ingest()
    let batch1 = ingest_input(&mut db, IngestInput {
            data: HashMap::from([(
                "t".to_string(),
                vec![HashMap::from([
                    ("block_number".to_string(), Value::UInt64(1000)),
                    ("x".to_string(), Value::Float64(1.0)),
                ])],
            )]),
            rollback_chain: vec![BlockCursor {
                number: 1000,
                hash: "0x0".into(),
            }],
            finalized_head: BlockCursor {
                number: 1000,
                hash: "0x0".into(),
            },
        })
        .unwrap();
    assert!(batch1.is_some());

    // Second ingest fails — bad block_number
    let err = ingest_input(&mut db, IngestInput {
        data: HashMap::from([(
            "t".to_string(),
            vec![HashMap::from([
                ("block_number".to_string(), Value::String("bad".into())),
                ("x".to_string(), Value::Float64(999.0)),
            ])],
        )]),
        rollback_chain: vec![],
        finalized_head: BlockCursor {
            number: 1000,
            hash: "0x0".into(),
        },
    });
    assert!(err.is_err());

    // Third ingest succeeds
    let batch3 = ingest_input(&mut db, IngestInput {
            data: HashMap::from([(
                "t".to_string(),
                vec![HashMap::from([
                    ("block_number".to_string(), Value::UInt64(1001)),
                    ("x".to_string(), Value::Float64(2.0)),
                ])],
            )]),
            rollback_chain: vec![BlockCursor {
                number: 1001,
                hash: "0x1".into(),
            }],
            finalized_head: BlockCursor {
                number: 1000,
                hash: "0x0".into(),
            },
        })
        .unwrap();

    // batch3 should contain ONLY changes from ingest 3, not leaked from ingest 2
    let batch3 = batch3.expect("third ingest should produce batch");
    let raw_records = batch3.records_for("t");
    // Should have 1 raw insert (block 1001), not 2
    assert_eq!(
        raw_records.len(),
        1,
        "failed ingest should not leak changes: got {} raw records",
        raw_records.len()
    );
}

fn wide_mv_schema(aggregates: usize) -> String {
    let columns: Vec<String> = (0..aggregates)
        .map(|i| format!("sum(amount) AS s{i}"))
        .collect();
    format!(
        r#"
        CREATE TABLE swaps (pool String, amount Float64);

        CREATE MATERIALIZED VIEW wide AS
        SELECT pool, {}
        FROM swaps
        GROUP BY pool;
        "#,
        columns.join(", ")
    )
}

/// The persisted MV group state stores the aggregate count in one byte.
#[test]
fn mv_at_aggregate_limit_survives_reopen() {
    let schema = wide_mv_schema(MAX_MV_AGGREGATES);
    let storage = Arc::new(MemoryBackend::new());

    {
        let mut db = Settle::open(Config::new(schema.clone()).storage(storage.clone())).unwrap();
        ingest_with_finalized(&mut db, vec![("swaps".into(), 1, vec![make_swap("ETH", 10.0)])], 1)
            .unwrap();
    }

    let mut db = Settle::open(Config::new(schema).storage(storage)).unwrap();
    let batch = ingest_with_finalized(&mut db, vec![("swaps".into(), 2, vec![make_swap("ETH", 10.0)])], 2)
        .unwrap()
        .unwrap();
    let eth = &batch.records_for("wide")[0];
    let last = format!("s{}", MAX_MV_AGGREGATES - 1);
    assert_eq!(eth.values.get("s0"), Some(&Value::Float64(20.0)));
    assert_eq!(eth.values.get(&last), Some(&Value::Float64(20.0)));
}
