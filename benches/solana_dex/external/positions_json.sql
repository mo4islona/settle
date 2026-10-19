-- The host gives the body: a Rust closure or a JS callback with the logic
-- of lua/positions_json.sql.
CREATE REDUCER positions
SOURCE swaps
GROUP BY trader
STATE (
    book Json DEFAULT '{}'
)
LANGUAGE EXTERNAL;
