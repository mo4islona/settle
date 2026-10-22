# ADR-20 — Views keep no undo data for blocks final at their call

Status: Proposed

**Context.** In a backfill call, almost every block is at or below the finalized head of the call. A view still kept a contribution for each block and each group, and an index of the groups of each block. The finality advance at the end of the call then merged all of them. No rollback can reach these blocks, so this work gives nothing.

**Decision.** A view folds a block that is final at its call into the final state of each group. The fold happens when the next block of the group arrives, or when the call ends. The view does not index the groups of such a block. Until the call ends, it keeps a copy of each group that these blocks change, as the group was before the call. A failed call puts the copies back (RS-17).

**Consequences.**

- Each block contribution still folds into the final state in chain order, so each aggregate stays the same bit for bit (DEF-42, INV-24).
- A call copies each group that existed before it and that its final blocks change, once.
- On a Uniswap V3 backfill with 4 views, the Settle time falls from 13.6 s to 8.9 s. On an ERC-20 balance backfill, it falls from 5.1 s to 4.6 s. The tip time does not change.
- A sliding view and a reducer keep their undo data for these blocks.
- Shapes: RS-11, RS-17, WP-70, INV-24.
