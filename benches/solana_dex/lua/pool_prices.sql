CREATE REDUCER pool_prices
SOURCE swaps
GROUP BY pool
STATE (
    price_usd  Float64 DEFAULT 0,
    volume_usd Float64 DEFAULT 0,
    swaps      UInt64  DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    local usd = row.quote_amount
    if row.quote == "So11111111111111111111111111111111111111112" then
        usd = usd * row.sol_usd
    end
    local price = usd / row.base_amount

    local change = 0
    if state.price_usd > 0 then change = price / state.price_usd - 1 end

    state.price_usd = price
    state.volume_usd = state.volume_usd + usd
    state.swaps = state.swaps + 1

    emit({token = row.token, dex = row.dex, timestamp = row.timestamp,
          price_usd = price, usd = usd, price_change = change})
$$;
