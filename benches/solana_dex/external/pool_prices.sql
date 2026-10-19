-- The host gives the body: a Rust closure or a JS callback with the logic
-- of lua/pool_prices.sql.
CREATE REDUCER pool_prices
SOURCE swaps
GROUP BY pool
STATE (
    price_usd  Float64 DEFAULT 0,
    volume_usd Float64 DEFAULT 0,
    swaps      UInt64  DEFAULT 0
)
LANGUAGE EXTERNAL;
