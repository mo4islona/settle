-- Balances start at zero at the first block of the fixture.
CREATE TABLE balances (
    address TEXT PRIMARY KEY,
    balance BIGINT NOT NULL
);
