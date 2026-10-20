use std::collections::HashMap;

use settle::db::{Config, Settle};
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::types::{ChangeBatch, ChangeOp, RowMap, Value};

use crate::support::*;

/// The latest values of the rows of `view`, by the value of `key`.
#[derive(Default)]
struct View(HashMap<String, RowMap>);

impl View {
    fn apply(&mut self, batch: Option<ChangeBatch>, view: &str, key: &str) {
        let Some(batch) = batch else { return };
        for record in batch.tables.get(view).into_iter().flatten() {
            let id = record.get(key).and_then(Value::as_str).unwrap().to_string();
            match record.operation {
                ChangeOp::Delete => {
                    self.0.remove(&id);
                }
                _ => {
                    let values = record.values().map(|(n, v)| (n.to_string(), v.clone()));
                    self.0.insert(id, values.collect());
                }
            }
        }
    }

    fn u64(&self, id: &str, column: &str) -> Option<u64> {
        self.0.get(id)?.get(column)?.as_u64()
    }
}

fn event(user: &str, amount: u64) -> RowMap {
    HashMap::from([
        ("user".to_string(), Value::String(user.into())),
        ("amount".to_string(), Value::UInt64(amount)),
    ])
}

/// `first` emits `early` for the first two rows of a user and `late` from
/// the third on, so `late` joins its output columns in a later block. The view
/// and the chained reducer `second` read it from then on, also after a fork
/// rolls the blocks with it back.
const GROWING_SCHEMA: &str = r#"
    CREATE TABLE events (block_number UInt64, user String, amount UInt64);

    CREATE REDUCER first
    SOURCE events
    GROUP BY user
    STATE (n UInt64 DEFAULT 0)
    LANGUAGE lua
    PROCESS $$
        state.n = state.n + 1
        if state.n >= 3 then
            emit({ user = row.user, late = row.amount })
        else
            emit({ user = row.user, early = row.amount })
        end
    $$;

    CREATE MATERIALIZED VIEW first_sums AS
    SELECT user, sum(early) AS early, sum(late) AS late
    FROM first
    GROUP BY user;

    CREATE REDUCER second
    SOURCE first
    GROUP BY user
    STATE (total UInt64 DEFAULT 0)
    LANGUAGE lua
    PROCESS $$
        if row.late then state.total = state.total + row.late end
        emit({ user = row.user, total = state.total })
    $$;

    CREATE MATERIALIZED VIEW second_totals AS
    SELECT user, last(total) AS total
    FROM second
    GROUP BY user;
"#;

#[cfg(feature = "lua")]
#[test]
fn emit_columns_that_appear_later_reach_views_and_chained_reducers() {
    let mut db = Settle::open(Config::new(GROWING_SCHEMA)).unwrap();
    let mut sums = View::default();
    let mut totals = View::default();

    for block in 1..=5 {
        let batch = ingest_one(&mut db, "events", block, vec![event("alice", block)]).unwrap();
        sums.apply(batch.clone(), "first_sums", "user");
        totals.apply(batch, "second_totals", "user");
    }
    assert_eq!(sums.u64("alice", "early"), Some(3));
    assert_eq!(sums.u64("alice", "late"), Some(12));
    assert_eq!(totals.u64("alice", "total"), Some(12));

    // Back to block 2, before `late` was first emitted, and a new chain.
    let batch = rollback_to(&mut db, 2).unwrap().batch;
    sums.apply(batch.clone(), "first_sums", "user");
    totals.apply(batch, "second_totals", "user");
    assert_eq!(totals.u64("alice", "total"), Some(0));

    let batch = ingest_one(&mut db, "events", 3, vec![event("alice", 10)]).unwrap();
    sums.apply(batch.clone(), "first_sums", "user");
    totals.apply(batch, "second_totals", "user");
    assert_eq!(sums.u64("alice", "early"), Some(3));
    assert_eq!(sums.u64("alice", "late"), Some(10));
    assert_eq!(totals.u64("alice", "total"), Some(10));
}

/// An emit that leaves a group-by column Null gets the value of its input
/// row, as one that leaves the column out does.
#[test]
fn group_by_column_left_null_by_an_emit_comes_from_the_input_row() {
    let schema = r#"
        CREATE TABLE events (block_number UInt64, user String, amount UInt64);

        CREATE REDUCER passthrough
        SOURCE events
        GROUP BY user
        STATE ()
        LANGUAGE EXTERNAL;

        CREATE MATERIALIZED VIEW amounts AS
        SELECT user, sum(amount) AS amount
        FROM passthrough
        GROUP BY user;
    "#;
    let mut db = Settle::open(Config::new(schema)).unwrap();
    // A Lua emit cannot hold a Null: nil leaves the key out.
    let passthrough = FnReducerRuntime::new(|state, row| {
        state.emit(HashMap::from([
            ("user".to_string(), Value::Null),
            ("amount".to_string(), row.get("amount").cloned().unwrap()),
        ]));
    });
    db.register_reducer_callback("passthrough", Box::new(passthrough))
        .unwrap();

    let rows = vec![event("alice", 1), event("bob", 2), event("alice", 3)];
    let mut amounts = View::default();
    amounts.apply(
        ingest_one(&mut db, "events", 1, rows).unwrap(),
        "amounts",
        "user",
    );

    assert_eq!(amounts.u64("alice", "amount"), Some(4));
    assert_eq!(amounts.u64("bob", "amount"), Some(2));
}

/// Each row emits the columns `prefix1` to `prefix{n}`, then fails if `fail`.
const WIDE_SCHEMA: &str = r#"
    CREATE TABLE events (block_number UInt64, user String, prefix String, n UInt64, fail Boolean);

    CREATE REDUCER wide
    SOURCE events
    GROUP BY user
    STATE (rows UInt64 DEFAULT 0)
    LANGUAGE lua
    PROCESS $$
        state.rows = state.rows + 1
        local out = { user = row.user }
        for i = 1, row.n do out[row.prefix .. i] = i end
        emit(out)
        if row.fail then error('bad input') end
    $$;

    CREATE MATERIALIZED VIEW wide_sums AS
    SELECT user, sum(b1) AS b1, sum(d40) AS d40, sum(f50) AS f50
    FROM wide
    GROUP BY user;
"#;

fn wide(prefix: &str, n: usize, fail: bool) -> RowMap {
    HashMap::from([
        ("user".to_string(), Value::String("alice".into())),
        ("prefix".to_string(), Value::String(prefix.into())),
        ("n".to_string(), Value::UInt64(n as u64)),
        ("fail".to_string(), Value::Boolean(fail)),
    ])
}

/// The emit columns that a failed or undone block added go with it, so the
/// blocks after it have the room that a fresh instance has. Each step leaves
/// too little room for the next one if the columns of the step stay.
#[cfg(feature = "lua")]
#[test]
fn undone_blocks_free_their_emit_columns() {
    use settle::reducer_runtime::MAX_EMIT_COLUMNS;

    let mut db = Settle::open(Config::new(WIDE_SCHEMA)).unwrap();
    let mut sums = View::default();

    // The first ingest fails, so its undo starts from no block.
    let failing = vec![
        ("events".to_string(), 1, vec![wide("z", 1000, false)]),
        ("events".to_string(), 2, vec![wide("z", 0, true)]),
    ];
    assert!(ingest_blocks(&mut db, failing).is_err());
    let batch = ingest_one(&mut db, "events", 1, vec![wide("b", 900, false)]).unwrap();
    sums.apply(batch, "wide_sums", "user");

    // Past the limit, then a Lua error, which skips the check of the limit.
    let over = vec![wide("a", MAX_EMIT_COLUMNS, true)];
    assert!(ingest_one(&mut db, "events", 2, over).is_err());
    let batch = ingest_one(&mut db, "events", 2, vec![wide("d", 40, false)]).unwrap();
    sums.apply(batch, "wide_sums", "user");

    // Block 3 adds 50 columns and the ingest fails at block 4.
    let failing = vec![
        ("events".to_string(), 3, vec![wide("c", 50, false)]),
        ("events".to_string(), 4, vec![wide("c", 0, true)]),
    ];
    assert!(ingest_blocks(&mut db, failing).is_err());
    let batch = ingest_one(&mut db, "events", 3, vec![wide("e", 50, false)]).unwrap();
    sums.apply(batch, "wide_sums", "user");

    let batch = rollback_to(&mut db, 2).unwrap().batch;
    sums.apply(batch, "wide_sums", "user");
    let batch = ingest_one(&mut db, "events", 3, vec![wide("f", 50, false)]).unwrap();
    sums.apply(batch, "wide_sums", "user");

    assert_eq!(sums.u64("alice", "b1"), Some(1));
    assert_eq!(sums.u64("alice", "d40"), Some(40));
    assert_eq!(sums.u64("alice", "f50"), Some(50));
}
