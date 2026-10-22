-- Each swap. Price: token1 per token0 in raw units, after the swap.
CREATE VIRTUAL TABLE swaps (
    block_number UInt64,
    ts DateTime,
    pool String,
    price Float64,
    tick Int64,
    volume0 Uint256,
    volume1 Uint256
);

-- The tokens that each swap, mint and burn moves into or out of a pool.
-- A sum of Uint256 has no sign, so the two directions are apart.
CREATE VIRTUAL TABLE flows (
    block_number UInt64,
    pool String,
    in0 Uint256,
    out0 Uint256,
    in1 Uint256,
    out1 Uint256
);

CREATE MATERIALIZED VIEW pools AS
SELECT
    pool,
    count() AS swaps,
    sum(volume0) AS volume0,
    sum(volume1) AS volume1,
    last(price) AS price,
    last(tick) AS tick
FROM swaps
GROUP BY pool;

CREATE MATERIALIZED VIEW candles_1m AS
SELECT
    pool,
    toStartOfInterval(ts, INTERVAL 1 MINUTE) AS start,
    first(price) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price) AS close,
    sum(volume0) AS volume0,
    sum(volume1) AS volume1,
    count() AS swaps
FROM swaps
GROUP BY pool, start;

CREATE MATERIALIZED VIEW candles_1h AS
SELECT
    pool,
    toStartOfInterval(ts, INTERVAL 1 HOUR) AS start,
    first(price) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price) AS close,
    sum(volume0) AS volume0,
    sum(volume1) AS volume1,
    count() AS swaps
FROM swaps
GROUP BY pool, start;

CREATE MATERIALIZED VIEW tvl AS
SELECT
    pool,
    sum(in0) AS in0,
    sum(out0) AS out0,
    sum(in1) AS in1,
    sum(out1) AS out1
FROM flows
GROUP BY pool;
