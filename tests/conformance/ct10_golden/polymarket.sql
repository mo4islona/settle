CREATE VIRTUAL TABLE orders (
    block_number UInt64,
    timestamp    UInt64,
    trader       String,
    asset_id     String,
    usdc         UInt64,
    shares       UInt64,
    side         UInt64
);

-- Per-token market stats (every order, both sides).
-- Emits per qualifying order: { asset_id, volume, price, price_sq }
-- Skips orders with shares == 0.
CREATE REDUCER market_stats
SOURCE orders
GROUP BY asset_id
STATE (
    volume      Float64 DEFAULT 0,
    trades      UInt64  DEFAULT 0,
    sum_price   Float64 DEFAULT 0,
    sum_price_sq Float64 DEFAULT 0,
    first_seen  UInt64  DEFAULT 0,
    last_seen   UInt64  DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    if row.shares == 0 then return end

    local price = row.usdc / row.shares
    local vol = row.usdc / 1000000

    state.volume = state.volume + vol
    state.trades = state.trades + 1
    state.sum_price = state.sum_price + price
    state.sum_price_sq = state.sum_price_sq + price * price
    if state.first_seen == 0 then state.first_seen = row.timestamp end
    state.last_seen = row.timestamp

    emit({asset_id = row.asset_id, volume = vol, price = price, price_sq = price * price})
$$;

-- Aggregates market_stats emits into per-token totals.
-- Emits to end user: { asset_id, total_volume, trade_count, last_price, sum_price, sum_price_sq }
-- Downstream can derive: mean = sum_price / trade_count,
--   std_dev = sqrt(sum_price_sq / trade_count - mean^2)
CREATE MATERIALIZED VIEW token_summary AS
SELECT
    asset_id,
    sum(volume)    AS total_volume,
    count()        AS trade_count,
    last(price)    AS last_price,
    sum(price)     AS sum_price,
    sum(price_sq)  AS sum_price_sq
FROM market_stats
GROUP BY asset_id;

-- Insider classification: 15-min window, $4000 threshold, buy-side only, price < 0.95.
-- Emits per insider order: { trader, asset_id, volume, price, price_sq, timestamp, detected_at }
-- Two emit paths:
--   1. Threshold crossed: multi-emit all accumulated positions from the window.
--   2. Known insider: single emit for each subsequent qualifying order.
-- No emit for: sell-side, high-price, clean traders, or sub-threshold unknown traders.
CREATE REDUCER insider_classifier
SOURCE orders
GROUP BY trader
STATE (
    status       String  DEFAULT 'unknown',
    window_start UInt64  DEFAULT 0,
    window_vol   UInt64  DEFAULT 0,
    window_trades UInt64 DEFAULT 0,
    positions    Json    DEFAULT '{}'
)
LANGUAGE lua
PROCESS $$
    if row.shares == 0 then return end

    local FIFTEEN_MIN = 900
    local VOLUME_THRESHOLD = 4000000000
    local MIN_PRICE_BPS = 9500
    local BPS_SCALE = 10000

    -- Only BUY side for insider detection
    if row.side ~= 0 then return end

    -- Only low-priced outcomes (price < 0.95)
    if row.usdc * BPS_SCALE >= row.shares * MIN_PRICE_BPS then return end

    -- Already classified
    if state.status ~= "unknown" then
        if state.status == "insider" then
            -- Path 2: known insider, emit each new qualifying order
            local price = row.usdc / row.shares
            emit {
                trader = row.trader,
                asset_id = row.asset_id,
                volume = row.usdc / 1000000,
                price = price,
                price_sq = price * price,
                timestamp = row.timestamp,
                detected_at = row.timestamp
            }
        end
        return
    end

    -- Window logic: reset if expired
    if state.window_start == 0 then
        state.window_start = row.timestamp
    elseif row.timestamp - state.window_start > FIFTEEN_MIN then
        state.status = "clean"
        return
    end

    state.window_vol = state.window_vol + row.usdc
    state.window_trades = state.window_trades + 1

    -- Track per-token positions in window (native table, no json.decode needed)
    local token = row.asset_id
    local price = row.usdc / row.shares
    local vol = row.usdc / 1000000
    local pos = state.positions[token]
    if not pos then
        pos = { volume = 0, trades = 0, sum_price = 0, sum_price_sq = 0,
                first_seen = row.timestamp, last_seen = row.timestamp }
    end
    pos.volume = pos.volume + vol
    pos.trades = pos.trades + 1
    pos.sum_price = pos.sum_price + price
    pos.sum_price_sq = pos.sum_price_sq + price * price
    if row.timestamp < pos.first_seen then pos.first_seen = row.timestamp end
    if row.timestamp > pos.last_seen then pos.last_seen = row.timestamp end
    state.positions[token] = pos

    -- Check threshold
    if state.window_vol >= VOLUME_THRESHOLD then
        state.status = "insider"
        -- Path 1: threshold crossed, multi-emit all accumulated positions
        for tid, p in pairs(state.positions) do
            emit {
                trader = row.trader,
                asset_id = tid,
                volume = p.volume,
                price = p.sum_price / p.trades,
                price_sq = p.sum_price_sq / p.trades,
                timestamp = p.first_seen,
                detected_at = row.timestamp
            }
        end
    end
$$;

-- Aggregates insider_classifier emits into per-(trader, asset_id) totals.
-- Emits to end user: { trader, asset_id, total_volume, trade_count,
--   sum_price, sum_price_sq, first_seen, last_seen, detected_at }
-- Downstream can derive: avg_price = sum_price / trade_count
CREATE MATERIALIZED VIEW insider_positions AS
SELECT
    trader,
    asset_id,
    sum(volume)      AS total_volume,
    count()          AS trade_count,
    sum(price)       AS sum_price,
    sum(price_sq)    AS sum_price_sq,
    first(timestamp) AS first_seen,
    last(timestamp)  AS last_seen,
    first(detected_at) AS detected_at
FROM insider_classifier
GROUP BY trader, asset_id;
