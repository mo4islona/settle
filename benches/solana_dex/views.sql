-- OHLC candles of each token, in USD.
CREATE MATERIALIZED VIEW token_candles_1m AS
SELECT
    token,
    toStartOfInterval(timestamp, INTERVAL 1 MINUTE) AS minute,
    first(price_usd) AS open,
    max(price_usd)   AS high,
    min(price_usd)   AS low,
    last(price_usd)  AS close,
    sum(usd)         AS volume_usd,
    count()          AS trades
FROM pool_prices
GROUP BY token, minute;

CREATE MATERIALIZED VIEW dex_volume_1h AS
SELECT
    dex,
    toStartOfInterval(timestamp, INTERVAL 1 HOUR) AS hour,
    sum(usd) AS volume_usd,
    count()  AS swaps
FROM pool_prices
GROUP BY dex, hour;

CREATE MATERIALIZED VIEW pool_stats AS
SELECT
    pool,
    last(price_usd) AS price_usd,
    sum(usd)        AS volume_usd,
    count()         AS swaps
FROM pool_prices
GROUP BY pool;

CREATE MATERIALIZED VIEW wallet_pnl AS
SELECT
    trader,
    sum(realized) AS realized_pnl,
    sum(usd)      AS volume_usd,
    count()       AS trades,
    sum(win)      AS wins,
    sum(loss)     AS losses
FROM positions
GROUP BY trader;

-- The reader values each open position at the last candle close.
CREATE MATERIALIZED VIEW open_positions AS
SELECT
    trader,
    token,
    last(position)  AS qty,
    last(cost_usd)  AS cost_usd,
    last(price_usd) AS last_price
FROM positions
GROUP BY trader, token;
