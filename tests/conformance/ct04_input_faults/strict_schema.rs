//! IB-68: an open refuses each construct that Settle does not execute.

use settle::db::{Config, Settle};
use settle::error::Error;

const TABLES: &str = "
    CREATE TABLE swaps (block_number UInt64, pool String, amount UInt64, ok Boolean);
    CREATE TABLE pools (block_number UInt64, pool String, fee UInt64);
";

fn view(body: &str) -> String {
    format!("{TABLES} CREATE MATERIALIZED VIEW v AS {body};")
}

fn reducer(state: &str) -> String {
    format!(
        "{TABLES} CREATE REDUCER r SOURCE swaps GROUP BY pool STATE ({state}) \
         LANGUAGE EXTERNAL;"
    )
}

/// The error of an open of `schema`, which must fail with INVALID_SCHEMA and
/// name `what`.
#[track_caller]
fn refused(schema: &str, what: &str) {
    match Settle::open(Config::new(schema)) {
        Ok(_) => panic!("the schema opened, expected a refusal of {what}:\n{schema}"),
        Err(Error::Schema(message)) => {
            assert!(message.contains(what), "'{message}' does not name {what}")
        }
        Err(other) => panic!("{other}, expected a schema error that names {what}"),
    }
}

#[test]
fn a_view_clause_that_settle_ignores_fails_the_open() {
    let cases = [
        (
            "SELECT pool, sum(amount) AS s FROM swaps WHERE amount > 0 GROUP BY pool",
            "WHERE",
        ),
        (
            "SELECT pool, sum(amount) AS s FROM swaps GROUP BY pool HAVING sum(amount) > 0",
            "HAVING",
        ),
        (
            "SELECT DISTINCT pool, sum(amount) AS s FROM swaps GROUP BY pool",
            "DISTINCT",
        ),
        (
            "SELECT pool, sum(amount) AS s FROM swaps GROUP BY pool ORDER BY pool",
            "ORDER BY",
        ),
        (
            "SELECT pool, sum(amount) AS s FROM swaps GROUP BY pool LIMIT 10",
            "LIMIT",
        ),
        (
            "SELECT pool, sum(amount) AS s FROM swaps GROUP BY pool LIMIT 10 OFFSET 5",
            "LIMIT",
        ),
        (
            "SELECT swaps.pool, sum(amount) AS s FROM swaps JOIN pools ON swaps.pool = pools.pool \
             GROUP BY swaps.pool",
            "JOIN",
        ),
        (
            "SELECT pool, sum(amount) AS s FROM swaps, pools GROUP BY pool",
            "JOIN",
        ),
        (
            "WITH big AS (SELECT * FROM swaps) SELECT pool, sum(amount) AS s FROM swaps GROUP BY pool",
            "WITH",
        ),
        (
            "SELECT pool, sum(amount) AS s FROM swaps GROUP BY ROLLUP (pool)",
            "GROUP BY",
        ),
    ];
    for (body, what) in cases {
        refused(&view(body), what);
    }
}

#[test]
fn an_option_of_create_materialized_view_fails_the_open() {
    let body = "AS SELECT pool, sum(amount) AS s FROM swaps GROUP BY pool";
    let cases = [
        ("CREATE MATERIALIZED VIEW v (p, s)", "column list"),
        ("CREATE MATERIALIZED VIEW v TO totals", "TO table"),
        ("CREATE OR REPLACE MATERIALIZED VIEW v", "OR REPLACE"),
        ("CREATE TEMPORARY MATERIALIZED VIEW v", "TEMPORARY"),
        ("CREATE MATERIALIZED VIEW IF NOT EXISTS v", "IF NOT EXISTS"),
        (
            "CREATE MATERIALIZED VIEW v OPTIONS (enable_refresh = false)",
            "OPTIONS",
        ),
        ("CREATE MATERIALIZED VIEW v CLUSTER BY (pool)", "CLUSTER BY"),
        ("CREATE MATERIALIZED VIEW v COMMENT = 'totals'", "COMMENT"),
    ];
    for (head, what) in cases {
        refused(&format!("{TABLES} {head} {body};"), what);
    }
}

#[test]
fn a_modifier_of_an_aggregate_or_of_the_source_fails_the_open() {
    let cases = [
        (
            "SELECT pool, sum(DISTINCT amount) AS s FROM swaps GROUP BY pool",
            "DISTINCT",
        ),
        (
            "SELECT pool, sum(amount) OVER (PARTITION BY pool) AS s FROM swaps GROUP BY pool",
            "OVER",
        ),
        (
            "SELECT toStartOfInterval(block_number, INTERVAL 1 HOUR) OVER () AS h, count() AS n \
             FROM swaps GROUP BY h",
            "OVER",
        ),
        (
            "SELECT pool, first(amount ORDER BY block_number) AS s FROM swaps GROUP BY pool",
            "ORDER BY",
        ),
        (
            "SELECT pool, first(amount) WITHIN GROUP (ORDER BY block_number) AS s FROM swaps \
             GROUP BY pool",
            "WITHIN GROUP",
        ),
        (
            "SELECT pool, sum(amount) AS s FROM swaps TABLESAMPLE BERNOULLI (0) GROUP BY pool",
            "TABLESAMPLE",
        ),
        (
            "SELECT pool, sum(amount) AS s FROM swaps AS w GROUP BY pool",
            "alias",
        ),
    ];
    for (body, what) in cases {
        refused(&view(body), what);
    }
}

#[test]
fn a_group_by_that_differs_from_the_key_items_fails_the_open() {
    let cases = [
        "SELECT pool, sum(amount) AS s FROM swaps GROUP BY amount",
        "SELECT pool, block_number, sum(amount) AS s FROM swaps GROUP BY pool",
        "SELECT pool, sum(amount) AS s FROM swaps GROUP BY pool, block_number",
        "SELECT pool, sum(amount) AS s FROM swaps",
        "SELECT pool, sum(amount) AS s FROM swaps GROUP BY pool, pool",
        "SELECT pool AS x, pool AS y, count() AS n FROM swaps GROUP BY pool, x",
    ];
    for body in cases {
        refused(&view(body), "GROUP BY");
    }
}

#[test]
fn a_group_by_names_a_key_by_its_column_or_its_alias() {
    let cases = [
        "SELECT pool AS p, sum(amount) AS s FROM swaps GROUP BY pool",
        "SELECT pool AS p, sum(amount) AS s FROM swaps GROUP BY p",
        "SELECT sum(amount) AS s FROM swaps",
        "SELECT pool AS amount, amount AS a, sum(amount) AS s FROM swaps GROUP BY pool, amount",
        "SELECT pool AS x, pool AS y, count() AS n FROM swaps GROUP BY pool",
    ];
    for body in cases {
        let schema = view(body);
        assert!(Settle::open(Config::new(&schema)).is_ok(), "{schema}");
    }
}

#[test]
fn a_view_with_no_aggregate_fails_the_open() {
    refused(&view("SELECT pool FROM swaps GROUP BY pool"), "aggregate");
}

#[test]
fn a_numeric_aggregate_over_a_column_with_no_numeric_mode_fails_the_open() {
    for func in ["sum", "min", "max", "avg"] {
        refused(
            &view(&format!(
                "SELECT pool, {func}(ok) AS x FROM swaps GROUP BY pool"
            )),
            "'ok'",
        );
    }

    let over_reducer = format!(
        "{} CREATE MATERIALIZED VIEW v AS SELECT pool, sum(label) AS x FROM r GROUP BY pool;",
        reducer("label String DEFAULT ''")
    );
    refused(&over_reducer, "'label'");
}

#[test]
fn two_objects_or_two_columns_with_one_name_fail_the_open() {
    let cases = [
        (
            format!("{TABLES} CREATE TABLE swaps (block_number UInt64);"),
            "'swaps'",
        ),
        (
            format!(
                "{} CREATE MATERIALIZED VIEW r AS SELECT sum(amount) AS s FROM swaps;",
                reducer("n UInt64 DEFAULT 0")
            ),
            "'r'",
        ),
        (
            "CREATE TABLE t (block_number UInt64, x UInt64, x Float64);".to_string(),
            "'x'",
        ),
        (reducer("n UInt64 DEFAULT 0, n Float64 DEFAULT 0"), "'n'"),
        (
            view("SELECT pool, sum(amount) AS s, max(amount) AS s FROM swaps GROUP BY pool"),
            "'s'",
        ),
    ];
    for (schema, what) in cases {
        refused(&schema, what);
    }
}

#[test]
fn a_default_that_does_not_parse_fails_the_open() {
    let cases = [
        "n UInt64 DEFAULT abc",
        "n UInt64 DEFAULT -1",
        "n Int64 DEFAULT 1.5",
        "n Float64 DEFAULT x",
        "n DateTime DEFAULT soon",
        "n Boolean DEFAULT yes",
        "n String DEFAULT abc",
        "n JSON DEFAULT '{'",
        "n UInt256 DEFAULT 0xzz",
        "n Bytes DEFAULT 1",
    ];
    for state in cases {
        refused(&reducer(state), "'n'");
    }
}

#[test]
fn a_default_of_each_type_opens() {
    let state = "a UInt64 DEFAULT 7, b Int64 DEFAULT -7, c Float64 DEFAULT 0.5, d DateTime DEFAULT 0, \
                 e Boolean DEFAULT true, f String DEFAULT 'it''s', g JSON DEFAULT '{\"k\": 1}', \
                 h UInt256 DEFAULT 0x10, i Bytes DEFAULT '', j Base58 DEFAULT ''";
    let schema = reducer(state);
    assert!(Settle::open(Config::new(&schema)).is_ok(), "{schema}");
}
