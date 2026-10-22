# ADR-18 — The checkpoint interval counts to the latest block

Status: Proposed

**Context.** ADR-2 defers the checkpoint of a backfill call until `F − D` reaches P-CHECKPOINT-INTERVAL. A checkpoint sets `D` to `min(F, L)` (WP-42). A source can report the finalized head of the chain, far above the blocks of a backfill call. Then `F − D` reaches the interval at each call, and each call writes a checkpoint.

**Decision.** Settle defers the checkpoint of a backfill call until `min(F, L) − D` reaches P-CHECKPOINT-INTERVAL. In a backfill call `L` is not above `F`, so the interval counts the blocks of data after the last checkpoint.

**Consequences.**

- Deferral also applies when the source reports finality above the data.
- On the stateful Postgres bench, the Settle time falls from 0.84 s to 0.49 s. The bench runs in serial mode on the persistent backend, with finality far above the data and an interval of 1,000 blocks.
- The batches do not change (INV-44). A crash replays at most P-CHECKPOINT-INTERVAL more blocks, as before.
- A heartbeat that only moves `F` writes no checkpoint under deferral.
- Shapes: REQ-16, DEF-84, WP-40.
