CREATE REDUCER pool_prices
SOURCE swaps
GROUP BY pool
STATE (
    price_usd  Float64 DEFAULT 0,
    volume_usd Float64 DEFAULT 0,
    swaps      UInt64  DEFAULT 0
)
    WHEN row.base_amount > 0 THEN
        LET usd = IF(row.quote = 'So11111111111111111111111111111111111111112', row.quote_amount * row.sol_usd, row.quote_amount)
        LET price = usd / row.base_amount
        LET change = IF(state.price_usd > 0, price / state.price_usd - 1, 0)
        SET state.price_usd = price
        SET state.volume_usd = state.volume_usd + usd
        SET state.swaps = state.swaps + 1
        EMIT token = row.token, dex = row.dex, timestamp = row.timestamp,
             price_usd = price, usd = usd, price_change = change
END;
