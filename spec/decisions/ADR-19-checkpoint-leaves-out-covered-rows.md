# ADR-19 — A checkpoint leaves out the raw rows that it covers

Status: Proposed

**Context.** ADR-3 purges the raw rows of the blocks at or below `D − P-RAW-RETENTION`, because nothing reads them. A reopen replays only the blocks above `D`, and a rollback only the blocks above `F`. In a backfill call, the blocks are final when they arrive, so the checkpoint of the call sets `D` to its last block. The call still stored the raw rows of each of its blocks, and a later purge deleted them. With the journal, the call wrote these rows twice: in the journal and in the commit at the ack.

On a Uniswap V3 backfill of 96,566 blocks, the raw rows were 647 MB of the 1,024 MB that the commits wrote. The backfill had 1.93M events, 4 views and a checkpoint at each call.

**Decision.** A write set with a checkpoint holds no raw rows of the blocks at or below the `D` that the checkpoint sets (RS-6). The largest value of P-RAW-RETENTION keeps them, because that value keeps every raw row.

**Consequences.**

- A backfill call stores raw rows only for its blocks above `min(F, L)`. Under deferral, it stores them for its blocks above `D`, as before.
- At the tip, a block is above `F` when it arrives, so Settle stores its raw rows and purges them later, as before.
- The retention margin applies only to rows that Settle stored.
- A write set that joins an unacked batch keeps the raw rows of that batch.
- On the Uniswap V3 backfill, Settle wrote 962 MB to disk in place of 2,340 MB. Its time fell from 13.5 s to 11.7 s.
- On a USDC balance backfill, Settle wrote 285 MB in place of 845 MB. Its time fell from 5.2 s to 4.3 s.
- Settle still encodes the rows that it then leaves out.
- Shapes: RS-1, RS-6, WP-30.
