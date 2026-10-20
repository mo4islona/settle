-- The host gives the body: a Rust closure or a JS callback with the logic
-- of lua/positions.sql.
CREATE REDUCER positions
SOURCE prices
GROUP BY trader, token
STATE (
    qty          Float64 DEFAULT 0,
    cost_usd     Float64 DEFAULT 0,
    realized_usd Float64 DEFAULT 0,
    buys         UInt64  DEFAULT 0,
    sells        UInt64  DEFAULT 0
)
LANGUAGE EXTERNAL;
