-- One DEX swap on Solana, as the decoder hands it over: it has picked the
-- base token (the side that is not SOL/USDC/USDT or a hub token, or SOL
-- against a stablecoin) and the quote token. No oracle price: the `prices`
-- reducer prices each token from the swaps. `timestamp` is in milliseconds.
CREATE VIRTUAL TABLE swaps (
    block_number UInt64,
    timestamp    UInt64,
    dex          String,
    pool         String,
    trader       String,
    token        String,
    quote        String,
    side         String,
    base_amount  Float64,
    quote_amount Float64
);
