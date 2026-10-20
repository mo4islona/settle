-- The USD price of each swap, where USD means USDC. Each price comes from
-- the swaps, through a chain of pools to USDC: X/BONK, BONK/SOL, SOL/USDC.
-- `usdc` holds the last price of each token, so the price is exact at the
-- place of the swap in the chain and rolls back with a fork. A swap whose
-- quote has no price yet prices the quote from the base instead; a swap of
-- two tokens without a price is not emitted.
CREATE REDUCER prices
SOURCE swaps
STATE (
    usdc Map(String, Float64)
)
LANGUAGE lua
PROCESS $$
    local quote_usd = 1
    if row.quote ~= "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" then
        quote_usd = state.usdc:get(row.quote)
    end

    local price
    if quote_usd then
        price = row.quote_amount * quote_usd / row.base_amount
        state.usdc:set(row.token, price)
    else
        price = state.usdc:get(row.token)
        if not price then return end
        state.usdc:set(row.quote, row.base_amount * price / row.quote_amount)
    end

    emit({dex = row.dex, pool = row.pool, trader = row.trader, token = row.token,
          side = row.side, base_amount = row.base_amount, timestamp = row.timestamp,
          price_usd = price, usd = row.base_amount * price})
$$;
