-- Everything starts at zero at the first block of the fixture. Candle starts
-- are Unix milliseconds.
CREATE TABLE pools (
    pool TEXT PRIMARY KEY,
    swaps BIGINT NOT NULL,
    volume0 NUMERIC NOT NULL,
    volume1 NUMERIC NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    tick INTEGER NOT NULL
);

CREATE TABLE candles_1m (
    pool TEXT NOT NULL,
    start BIGINT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume0 NUMERIC NOT NULL,
    volume1 NUMERIC NOT NULL,
    swaps BIGINT NOT NULL,
    PRIMARY KEY (pool, start)
);

CREATE TABLE candles_1h (LIKE candles_1m INCLUDING ALL);

-- The net tokens that went into each pool.
CREATE TABLE tvl (
    pool TEXT PRIMARY KEY,
    amount0 NUMERIC NOT NULL,
    amount1 NUMERIC NOT NULL
);
