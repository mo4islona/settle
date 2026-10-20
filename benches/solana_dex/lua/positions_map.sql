-- lua/positions_json.sql with a Map field: a swap changes one entry of the
-- book, whatever the number of tokens of the wallet.
CREATE REDUCER positions
SOURCE prices
GROUP BY trader
STATE (
    book Map(String, Json)
)
LANGUAGE lua
PROCESS $$
    local p = state.book:get(row.token)
    if not p then
        p = {qty = 0, cost_usd = 0, realized_usd = 0, buys = 0, sells = 0}
    end

    local realized = 0
    if row.side == "buy" then
        p.qty = p.qty + row.base_amount
        p.cost_usd = p.cost_usd + row.usd
        p.buys = p.buys + 1
    else
        local avg = 0
        if p.qty > 0 then avg = p.cost_usd / p.qty end
        local matched = math.min(row.base_amount, p.qty)
        realized = matched * row.price_usd - matched * avg
        p.qty = p.qty - matched
        p.cost_usd = p.cost_usd - matched * avg
        p.realized_usd = p.realized_usd + realized
        p.sells = p.sells + 1
    end
    state.book:set(row.token, p)

    local win, loss = 0, 0
    if realized > 0 then win = 1 elseif realized < 0 then loss = 1 end

    emit({token = row.token, realized = realized, usd = row.usd, price_usd = row.price_usd,
          position = p.qty, cost_usd = p.cost_usd, win = win, loss = loss})
$$;
