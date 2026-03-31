-- Raw Uniswap swap events with token addresses
CREATE TABLE swaps (
    block_number UInt64,
    block_time   DateTime,
    tx_hash      String,
    network      String,
    pool         String,
    token0       String,
    token1       String,
    sender       String,
    amount0      Float64,
    amount1      Float64
);

-- Price oracle: computes USD price for each swap.
-- Groups by network (constant) to maintain a global ETH/USD reference.
-- Cross-prices tokens via ETH when no direct stablecoin pair exists.
-- Passes through sender and base_delta for downstream wallet_pnl reducer.
CREATE REDUCER swap_prices
SOURCE swaps
GROUP BY network
STATE (
    eth_usd Float64 DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    local USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    local USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    local WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    if row.amount0 == 0 then return end

    local ratio = math.abs(row.amount1 / row.amount0)
    local t0 = string.lower(row.token0)
    local t1 = string.lower(row.token1)
    local price_usd = 0
    local target = ""
    local vol_usd = 0
    local base_delta = 0

    -- Case 1: token1 is stablecoin -> direct pricing
    if t1 == USDT or t1 == USDC then
        price_usd = ratio
        target = t0
        base_delta = row.amount0
        if t0 == WETH then state.eth_usd = price_usd end
        vol_usd = math.abs(row.amount1)

    -- Case 2: token0 is stablecoin -> inverse pricing
    elseif t0 == USDT or t0 == USDC then
        if ratio > 0 then price_usd = 1 / ratio end
        target = t1
        base_delta = row.amount1
        if t1 == WETH then state.eth_usd = price_usd end
        vol_usd = math.abs(row.amount0)

    -- Case 3: token1 is WETH -> cross via ETH
    elseif t1 == WETH then
        price_usd = ratio * state.eth_usd
        target = t0
        base_delta = row.amount0
        vol_usd = math.abs(row.amount1) * state.eth_usd

    -- Case 4: token0 is WETH -> cross via ETH (inverted)
    elseif t0 == WETH then
        if ratio > 0 then price_usd = state.eth_usd / ratio end
        target = t1
        base_delta = row.amount1
        vol_usd = math.abs(row.amount0) * state.eth_usd
    end

    if price_usd > 0 then
        emit({pool = row.pool, token = target, block_time = row.block_time, price_usd = price_usd, volume_usd = vol_usd, sender = row.sender, base_delta = base_delta})
    end
$$;

-- OHLC 5-minute candles per pool, in USD
CREATE MATERIALIZED VIEW candles_5m AS
SELECT
    pool,
    toStartOfInterval(block_time, INTERVAL 5 MINUTE) AS window_start,
    first(price_usd)  AS open,
    max(price_usd)    AS high,
    min(price_usd)    AS low,
    last(price_usd)   AS close,
    sum(volume_usd)   AS volume,
    count()            AS trade_count
FROM swap_prices
GROUP BY pool, window_start;

-- Wallet PnL tracker: sources from swap_prices (chained reducer).
-- Price resolution is already done; this reducer only tracks positions.
-- Groups by sender so each wallet has its own small positions state.
CREATE REDUCER wallet_pnl
SOURCE swap_prices
GROUP BY sender
STATE (
    positions JSON DEFAULT '{}'
)
LANGUAGE lua
PROCESS $$
    local pos_key = row.token
    local pos = state.positions[pos_key]
    if not pos then pos = { balance = 0, cost_usd = 0 } end

    local pnl = 0
    if row.base_delta > 0 then
        -- Buy: accumulate cost basis
        pos.balance = pos.balance + row.base_delta
        pos.cost_usd = pos.cost_usd + row.base_delta * row.price_usd
    elseif row.base_delta < 0 and pos.balance > 0 then
        -- Sell: realize PnL vs average cost
        local sold = math.abs(row.base_delta)
        local avg_cost = pos.cost_usd / pos.balance
        pnl = sold * (row.price_usd - avg_cost)
        pos.balance = pos.balance - sold
        pos.cost_usd = pos.cost_usd - sold * avg_cost
    end

    state.positions[pos_key] = pos

    emit({sender = row.sender, pool = row.pool, realized_pnl = pnl, position = pos.balance})
$$;

-- Aggregate PnL per wallet per pool
CREATE MATERIALIZED VIEW wallet_summary AS
SELECT
    sender,
    pool,
    sum(realized_pnl) AS total_pnl,
    last(position)    AS current_position,
    count()           AS trade_count
FROM wallet_pnl
GROUP BY sender, pool;
