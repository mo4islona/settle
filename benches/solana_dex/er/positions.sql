CREATE REDUCER positions
SOURCE swaps
GROUP BY trader, token
STATE (
    qty          Float64 DEFAULT 0,
    cost_usd     Float64 DEFAULT 0,
    realized_usd Float64 DEFAULT 0,
    buys         UInt64  DEFAULT 0,
    sells        UInt64  DEFAULT 0
)
    WHEN row.side = 'buy' THEN
        LET usd = IF(row.quote = 'So11111111111111111111111111111111111111112', row.quote_amount * row.sol_usd, row.quote_amount)
        SET state.qty = state.qty + row.base_amount
        SET state.cost_usd = state.cost_usd + usd
        SET state.buys = state.buys + 1
        EMIT realized = 0, usd = usd, price_usd = usd / row.base_amount, win = 0, loss = 0
    WHEN row.side = 'sell' THEN
        LET usd = IF(row.quote = 'So11111111111111111111111111111111111111112', row.quote_amount * row.sol_usd, row.quote_amount)
        LET price = usd / row.base_amount
        LET avg = IF(state.qty > 0, state.cost_usd / state.qty, 0)
        LET matched = IF(row.base_amount < state.qty, row.base_amount, state.qty)
        LET realized = matched * price - matched * avg
        SET state.qty = state.qty - matched
        SET state.cost_usd = state.cost_usd - matched * avg
        SET state.realized_usd = state.realized_usd + realized
        SET state.sells = state.sells + 1
        EMIT realized = realized, usd = usd, price_usd = price,
             win = IF(realized > 0, 1, 0), loss = IF(realized < 0, 1, 0)
    ALWAYS EMIT
        state.qty AS position,
        state.cost_usd AS cost_usd
END;
