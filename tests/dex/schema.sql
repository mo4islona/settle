CREATE TABLE trades (
    block_number UInt64,
    user         String,
    side         String,
    amount       Float64,
    price        Float64
);

CREATE REDUCER pnl
SOURCE trades
GROUP BY user
STATE (
    quantity   Float64 DEFAULT 0,
    cost_basis Float64 DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    local trade_pnl = 0
    if row.side == "buy" then
        state.quantity = state.quantity + row.amount
        state.cost_basis = state.cost_basis + row.amount * row.price
    elseif row.side == "sell" then
        local avg_cost = 0
        if state.quantity ~= 0 then avg_cost = state.cost_basis / state.quantity end
        state.quantity = state.quantity - row.amount
        state.cost_basis = state.cost_basis - row.amount * avg_cost
        trade_pnl = row.amount * (row.price - avg_cost)
    end
    emit { trade_pnl = trade_pnl, position_size = state.quantity }
$$;

CREATE MATERIALIZED VIEW position_summary AS
SELECT
    user,
    sum(trade_pnl)       AS total_pnl,
    last(position_size)  AS current_position,
    count()              AS trade_count
FROM pnl
GROUP BY user;

CREATE TABLE swaps (
    block_number UInt64,
    pool         String,
    amount       Float64
);

CREATE MATERIALIZED VIEW volume_by_pool AS
SELECT
    pool,
    sum(amount) AS total_volume,
    count()     AS swap_count
FROM swaps
GROUP BY pool;
