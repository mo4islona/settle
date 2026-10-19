-- lua/positions.sql with one group for each wallet: its positions in a JSON
-- map by token, as tests/uniswap/schema.sql keeps them. A bot that trades
-- thousands of tokens has thousands of entries in its map.
CREATE REDUCER positions
SOURCE swaps
GROUP BY trader
STATE (
    book Json DEFAULT '{}'
)
LANGUAGE lua
PROCESS $$
    local usd = row.quote_amount
    if row.quote == "So11111111111111111111111111111111111111112" then
        usd = usd * row.sol_usd
    end
    local price = usd / row.base_amount

    local p = state.book[row.token]
    if not p then
        p = {qty = 0, cost_usd = 0, realized_usd = 0, buys = 0, sells = 0}
    end

    local realized = 0
    if row.side == "buy" then
        p.qty = p.qty + row.base_amount
        p.cost_usd = p.cost_usd + usd
        p.buys = p.buys + 1
    else
        local avg = 0
        if p.qty > 0 then avg = p.cost_usd / p.qty end
        local matched = math.min(row.base_amount, p.qty)
        realized = matched * price - matched * avg
        p.qty = p.qty - matched
        p.cost_usd = p.cost_usd - matched * avg
        p.realized_usd = p.realized_usd + realized
        p.sells = p.sells + 1
    end
    state.book[row.token] = p

    local win, loss = 0, 0
    if realized > 0 then win = 1 elseif realized < 0 then loss = 1 end

    emit({token = row.token, realized = realized, usd = usd, price_usd = price,
          position = p.qty, cost_usd = p.cost_usd, win = win, loss = loss})
$$;
