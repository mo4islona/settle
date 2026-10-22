-- The app writes each transfer as two rows: the debit of the sender and the
-- credit of the recipient.
CREATE VIRTUAL TABLE balance_deltas (
    block_number UInt64,
    address      String,
    delta        Int64
);

CREATE MATERIALIZED VIEW balances AS
SELECT
    address,
    sum(delta) AS balance
FROM balance_deltas
GROUP BY address;
