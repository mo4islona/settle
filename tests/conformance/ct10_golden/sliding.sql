CREATE TABLE trades (
    block_number UInt64,
    block_time   DateTime,
    pair         String,
    volume       Float64,
    price        Float64
);

-- Sliding window: volume over last 1 hour
CREATE MATERIALIZED VIEW volume_1h AS
  SELECT
    pair,
    SUM(volume) AS total_volume,
    COUNT()     AS trade_count
  FROM trades
  GROUP BY pair
  WINDOW SLIDING INTERVAL 1 HOUR BY block_time;

-- Sliding window: all agg types over last 30 minutes
CREATE MATERIALIZED VIEW stats_30m AS
  SELECT
    pair,
    SUM(volume)  AS vol_sum,
    COUNT()      AS vol_count,
    MIN(price)   AS price_min,
    MAX(price)   AS price_max,
    AVG(price)   AS price_avg,
    FIRST(price) AS price_first,
    LAST(price)  AS price_last
  FROM trades
  GROUP BY pair
  WINDOW SLIDING INTERVAL 30 MINUTE BY block_time;

-- Non-sliding MV for comparison (unbounded aggregation)
CREATE MATERIALIZED VIEW totals AS
  SELECT
    pair,
    SUM(volume) AS total_volume,
    COUNT()     AS trade_count
  FROM trades
  GROUP BY pair;
