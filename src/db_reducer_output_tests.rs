use super::test_helpers::*;
use super::*;
use crate::types::{ChangeOp, RowMap};
use std::collections::HashMap;

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
        STATE (n UInt64 DEFAULT 0)
            WHEN 1 = 1 THEN
                SET state.n = state.n + 1
                EMIT seen = state.n
            ALWAYS EMIT
                row.missing AS user,
                row.amount AS amount
        END;

        CREATE MATERIALIZED VIEW amounts AS
        SELECT user, sum(amount) AS amount
        FROM passthrough
        GROUP BY user;
    "#;
    let mut db = Settle::open(Config::new(schema)).unwrap();

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
