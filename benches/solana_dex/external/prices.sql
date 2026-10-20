-- The host gives the body: a Rust closure or a JS callback with the logic
-- of lua/prices.sql.
CREATE REDUCER prices
SOURCE swaps
STATE (
    usdc Map(String, Float64)
)
LANGUAGE EXTERNAL;
