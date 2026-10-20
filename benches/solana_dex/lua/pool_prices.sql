CREATE REDUCER pool_prices
SOURCE prices
GROUP BY pool
STATE (
    price_usd  Float64 DEFAULT 0,
    volume_usd Float64 DEFAULT 0,
    swaps      UInt64  DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    local change = 0
    if state.price_usd > 0 then change = row.price_usd / state.price_usd - 1 end

    state.price_usd = row.price_usd
    state.volume_usd = state.volume_usd + row.usd
    state.swaps = state.swaps + 1

    emit({token = row.token, dex = row.dex, timestamp = row.timestamp,
          price_usd = row.price_usd, usd = row.usd, price_change = change})
$$;
