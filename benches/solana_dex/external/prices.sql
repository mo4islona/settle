-- The host gives the body: a Rust closure with the logic of
-- lua/prices.sql. JS reducers do not support Map fields yet.
CREATE REDUCER prices
SOURCE swaps
STATE (
    usdc Map(String, Float64)
)
LANGUAGE EXTERNAL;
