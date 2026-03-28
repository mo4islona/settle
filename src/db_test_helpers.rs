use crate::types::{RowMap, Value};
use std::collections::HashMap;

pub const DEX_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user String,
        side String,
        amount Float64,
        price Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
        WHEN row.side = 'buy' THEN
            SET state.quantity = state.quantity + row.amount
            SET state.cost_basis = state.cost_basis + row.amount * row.price
            EMIT trade_pnl = 0
        WHEN row.side = 'sell' THEN
            LET avg_cost = state.cost_basis / state.quantity
            SET state.quantity = state.quantity - row.amount
            SET state.cost_basis = state.cost_basis - row.amount * avg_cost
            EMIT trade_pnl = row.amount * (row.price - avg_cost)
        ALWAYS EMIT
            state.quantity AS position_size
    END;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl) AS total_pnl,
        last(position_size) AS current_position,
        count() AS trade_count
    FROM pnl
    GROUP BY user;
"#;

pub const SIMPLE_SCHEMA: &str = r#"
    CREATE TABLE swaps (
        pool String,
        amount Float64
    );

    CREATE MATERIALIZED VIEW pool_volume AS
    SELECT
        pool,
        sum(amount) AS total_volume,
        count() AS swap_count
    FROM swaps
    GROUP BY pool;
"#;

pub const EXTERNAL_PNL_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user String,
        side String,
        amount Float64,
        price Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
    LANGUAGE EXTERNAL;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl) AS total_pnl,
        last(position_size) AS current_position,
        count() AS trade_count
    FROM pnl
    GROUP BY user;
"#;

pub fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> RowMap {
    HashMap::from([
        ("user".to_string(), Value::String(user.to_string())),
        ("side".to_string(), Value::String(side.to_string())),
        ("amount".to_string(), Value::Float64(amount)),
        ("price".to_string(), Value::Float64(price)),
    ])
}

pub fn make_swap(pool: &str, amount: f64) -> RowMap {
    HashMap::from([
        ("pool".to_string(), Value::String(pool.to_string())),
        ("amount".to_string(), Value::Float64(amount)),
    ])
}
