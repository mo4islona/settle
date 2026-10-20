//! FM-1: no schema text and no reducer result terminates the process.

use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};

use settle::db::{Config, Settle};
use settle::error::Error;
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::types::Value;
use settle::{
    BuildOptions, Pipeline, ReducerOptions, ViewOptions, datetime, interval, string, uint64,
};

use crate::support::*;

/// Every pre-pass of the parser: a module, a reducer with REQUIRE, GROUP BY
/// and a Map field, a virtual table, a tumbling and a sliding view.
const EVERY_CONSTRUCT: &str = r#"
    CREATE MODULE util LANGUAGE lua AS $$ return { one = 1 } $$;

    CREATE VIRTUAL TABLE swaps (block_number UInt64, pool String, amount UInt64, ts DateTime);

    CREATE REDUCER running
    SOURCE swaps
    GROUP BY pool
    STATE (
        total UInt64 DEFAULT 0,
        seen Map(String, UInt64)
    )
    REQUIRE util
    LANGUAGE lua
    PROCESS $$
        state.total = state.total + row.amount * util.one
        emit { total = state.total }
    $$;

    CREATE MATERIALIZED VIEW hourly AS
    SELECT pool, toStartOfInterval(ts, INTERVAL 1 HOUR) AS hour, sum(amount) AS volume
    FROM swaps
    GROUP BY pool, hour;

    CREATE MATERIALIZED VIEW recent AS
    SELECT pool, max(total) AS high
    FROM running
    GROUP BY pool
    WINDOW SLIDING INTERVAL 1 HOUR BY ts;
"#;

const TABLE: &str = "CREATE TABLE swaps (block_number UInt64, pool String, amount UInt64);";

fn open_error(schema: &str) -> Error {
    match Settle::open(Config::new(schema)) {
        Ok(_) => panic!("the schema opened:\n{schema}"),
        Err(e) => e,
    }
}

fn reducer(body: &str, require: &str) -> String {
    format!(
        "CREATE REDUCER r SOURCE swaps GROUP BY pool STATE (n UInt64 DEFAULT 0) {require} \
         LANGUAGE lua PROCESS $$ {body} $$;"
    )
}

#[test]
fn a_lua_syntax_error_fails_the_open() {
    let schema = format!("{TABLE} {}", reducer("state.n = = 1", ""));
    let error = open_error(&schema);
    assert!(
        matches!(&error, Error::Schema(m) if m.contains("'r'")),
        "{error}"
    );
}

#[test]
fn a_module_that_does_not_load_fails_the_open() {
    for module in ["return {", "error('no')"] {
        let schema = format!(
            "{TABLE} CREATE MODULE util LANGUAGE lua AS $${module}$$; {}",
            reducer("state.n = 1", "REQUIRE util")
        );
        let error = open_error(&schema);
        assert!(
            matches!(&error, Error::Schema(m) if m.contains("'util'")),
            "{error}"
        );
    }
}

/// A module can set a metatable on the globals: the setup of the runtime after
/// the module, and the next module, do not go through it.
#[test]
fn a_module_that_guards_the_globals_does_not_break_the_open() {
    for name in ["state", "row", "emit", "util"] {
        let schema = format!(
            "CREATE TABLE swaps (block_number UInt64, pool String, amount UInt64); \
             CREATE MODULE guard LANGUAGE lua AS $$ \
                 setmetatable(_G, {{ __newindex = function(_, key) \
                     if key == '{name}' then error('blocked') end \
                 end }}) \
                 return {{}} \
             $$; \
             CREATE MODULE util LANGUAGE lua AS $$ return {{ one = 1 }} $$; \
             CREATE REDUCER r SOURCE swaps GROUP BY pool STATE (n UInt64 DEFAULT 0) \
             REQUIRE guard, util LANGUAGE lua PROCESS $$ \
                 state.n = state.n + row.amount * util.one \
                 emit {{ n = state.n }} \
             $$; \
             CREATE MATERIALIZED VIEW v AS SELECT pool, last(n) AS total FROM r GROUP BY pool;"
        );
        let mut db = Settle::open(Config::new(&schema)).unwrap();

        let row = HashMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::UInt64(5)),
        ]);
        let batch = ingest_one(&mut db, "swaps", 1, vec![row]).unwrap().unwrap();
        let totals: Vec<_> = batch
            .records_for("v")
            .iter()
            .filter_map(|r| r.get("total").and_then(Value::as_u64))
            .collect();
        assert_eq!(totals, [5], "{name}");
    }
}

/// Each change of the case mapping: a character whose uppercase has another
/// length in UTF-8, inserted at each position, and the characters whose
/// uppercase is an ASCII letter in place of that letter.
#[test]
fn an_open_never_panics_on_non_ascii_text() {
    let mut texts = Vec::new();
    for (at, _) in EVERY_CONSTRUCT.char_indices() {
        for c in ['ı', 'ǰ', 'ﬀ', '😀'] {
            let mut text = EVERY_CONSTRUCT.to_string();
            text.insert(at, c);
            texts.push(text);
        }
    }
    for (from, to) in [
        ('s', "ſ"),
        ('S', "ſ"),
        ('i', "ı"),
        ('I', "ı"),
        ('k', "\u{212a}"),
    ] {
        for (at, _) in EVERY_CONSTRUCT.match_indices(from) {
            let mut text = EVERY_CONSTRUCT.to_string();
            text.replace_range(at..at + 1, to);
            texts.push(text);
        }
    }

    let hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let panicked: Vec<&String> = texts
        .iter()
        .filter(|text| panic::catch_unwind(|| Settle::open(Config::new(text.as_str()))).is_err())
        .collect();
    panic::set_hook(hook);

    assert!(Settle::open(Config::new(EVERY_CONSTRUCT)).is_ok());
    assert!(
        panicked.is_empty(),
        "{} texts panic, the first:\n{}",
        panicked.len(),
        panicked[0]
    );
}

#[test]
fn a_bad_interval_fails_the_build() {
    let mut pipeline = Pipeline::new();
    let swaps = pipeline.table("swaps", [("block_number", uint64()), ("ts", datetime())]);
    swaps.create_view(
        "v",
        ViewOptions {
            group_by: vec![interval("ts", "5 fortnights").r#as("bucket").into()],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("bucket".into(), agg.key("bucket").into()),
                    ("n".into(), agg.count().into()),
                ]
            }),
        },
    );

    let error = pipeline
        .build(BuildOptions::default())
        .err()
        .expect("the build fails");
    assert!(
        matches!(&error, Error::Schema(m) if m.contains("fortnights")),
        "{error}"
    );
}

#[test]
fn a_bad_initial_state_fails_the_build() {
    let mut pipeline = Pipeline::new();
    let swaps = pipeline.table("swaps", [("block_number", uint64()), ("pool", string())]);
    swaps.create_reducer(
        "r",
        ReducerOptions {
            group_by: vec!["pool".into()],
            initial_state: HashMap::from([("n".to_string(), Value::Null)]),
            reduce: Box::new(|_, _| {}),
        },
    );

    let error = pipeline
        .build(BuildOptions::default())
        .err()
        .expect("the build fails");
    assert!(
        matches!(&error, Error::Schema(m) if m.contains("'r'")),
        "{error}"
    );
}

/// FM-47: a panic in a Rust reducer fails the call, and the call has no effect.
#[test]
fn a_panic_in_a_rust_reducer_fails_the_call() {
    let schema = r#"
        CREATE TABLE swaps (block_number UInt64, pool String, amount UInt64);
        CREATE REDUCER r SOURCE swaps GROUP BY pool STATE (n UInt64 DEFAULT 0) LANGUAGE EXTERNAL;
        CREATE MATERIALIZED VIEW v AS SELECT pool, last(n) AS total FROM r GROUP BY pool;
    "#;
    let mut db = Settle::open(Config::new(schema)).unwrap();
    let runtime = FnReducerRuntime::new(|ctx, row| {
        let amount = row.get("amount").and_then(Value::as_u64).unwrap_or(0);
        assert!(amount != 13, "unlucky amount");
        let n = ctx.get_u64("n");
        ctx.set("n", Value::UInt64(n + amount));
        ctx.emit(HashMap::from([(
            "n".to_string(),
            Value::UInt64(n + amount),
        )]));
    });
    db.register_reducer_callback("r", Box::new(runtime))
        .unwrap();

    let row = |amount: u64| {
        HashMap::from([
            ("pool".to_string(), Value::String("ETH".into())),
            ("amount".to_string(), Value::UInt64(amount)),
        ])
    };
    ingest_one(&mut db, "swaps", 1, vec![row(1)]).unwrap();

    let hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        ingest_one(&mut db, "swaps", 2, vec![row(13)])
    }));
    panic::set_hook(hook);

    let error = result
        .expect("the panic stays inside the call")
        .err()
        .expect("the call fails");
    assert!(
        matches!(&error, Error::Reducer(m) if m.contains("unlucky")),
        "{error}"
    );
    assert!(!db.is_poisoned());
    assert_eq!(db.latest_block(), 1);

    let batch = ingest_one(&mut db, "swaps", 2, vec![row(2)])
        .unwrap()
        .unwrap();
    let totals: Vec<_> = batch
        .records_for("v")
        .iter()
        .filter_map(|r| r.get("total").cloned())
        .collect();
    assert_eq!(totals, [Value::UInt64(3)]);
}
