-- Average-cost PnL of each wallet in each token. A sell of more than the
-- wallet bought here (tokens that came by transfer) realizes only the
-- bought part.
CREATE REDUCER positions
SOURCE prices
GROUP BY trader, token
STATE (
    qty          Float64 DEFAULT 0,
    cost_usd     Float64 DEFAULT 0,
    realized_usd Float64 DEFAULT 0,
    buys         UInt64  DEFAULT 0,
    sells        UInt64  DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    local realized = 0
    if row.side == "buy" then
        state.qty = state.qty + row.base_amount
        state.cost_usd = state.cost_usd + row.usd
        state.buys = state.buys + 1
    else
        local avg = 0
        if state.qty > 0 then avg = state.cost_usd / state.qty end
        local matched = math.min(row.base_amount, state.qty)
        realized = matched * row.price_usd - matched * avg
        state.qty = state.qty - matched
        state.cost_usd = state.cost_usd - matched * avg
        state.realized_usd = state.realized_usd + realized
        state.sells = state.sells + 1
    end

    local win, loss = 0, 0
    if realized > 0 then win = 1 elseif realized < 0 then loss = 1 end

    emit({realized = realized, usd = row.usd, price_usd = row.price_usd, position = state.qty,
          cost_usd = state.cost_usd, win = win, loss = loss})
$$;
