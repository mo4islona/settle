-- The host gives the body: a Rust closure with the logic of
-- lua/positions_map.sql.
CREATE REDUCER positions
SOURCE prices
GROUP BY trader
STATE (
    book Map(String, Json)
)
LANGUAGE EXTERNAL;
