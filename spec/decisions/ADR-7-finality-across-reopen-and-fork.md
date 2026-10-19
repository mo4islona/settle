# ADR-7 — Finality across a reopen and a fork

Status: Proposed

This record holds two related proposals. Both keep the finality floor of ADR-2 and give one response to one fault. The maintainers can accept each part on its own.

## 1. Persist the finality watermark next to the durable watermark

**Context.** ADR-2 stores the durable watermark `D` where recovery reads the finality, so an open sets `F` to `D`. Under deferral, and on gappy chains where `D = min(F, L) < F`, the finality goes back at a reopen. A smaller finalized head then passes, and a fork can undo blocks that Settle already reported final (GAP-3). The replay also finalizes nothing, so the replayed tail stays as unfinal data until the next call (GAP-22).

**Decision.** The write set of each call stores `F` and `H(F)` in addition to `D`. An open restores `F` from the store, and replays the tail above `D` with the finality advances that the stored `F` allows. ADR-3 applies, so the format change needs no migration.

**Consequences.**

- The finality of the last durable commit survives a reopen (INV-10, INV-47), and the floor of ADR-2 holds after a crash.
- The memory of an open falls, because the replayed blocks at or below `F` become final at once.
- Each write set holds one more value.
- Shapes: REQ-5, WP-24, CN-20, INV-47, GAP-3, GAP-22.

## 2. A fork with no common ancestor fails on both paths

**Context.** A fork call with no supplied block that matches in `[F, L]` fails ("fork too deep"). An ingest call with a hash conflict and no block that matches rolls back to block 0 when `F` is 0 (GAP-8). When `F` is above 0, it fails with a message about final state, also when the conflict is above `F`. One fault thus gets three different responses.

The alternatives are to fail on both paths, or to roll back to `F` on both paths. A rollback to `F` relies on the finality promise of the source and is correct when that promise holds. But a chain with no common ancestor in `[F, L]` more often comes from a short chain or from a wrong source. Also, a rollback to `F` can undo many blocks that the target then writes again.

**Decision.** Both paths use the fork point of DEF-13. When no supplied block in `[F, L]` matches, the call fails with NO_COMMON_ANCESTOR and changes nothing. The host can retry with a longer chain.

**Consequences.**

- One fault gets one response, and the error class tells the host what to do.
- A host that sends a short chain gets an error in place of a deep rollback.
- Shapes: WP-12, FM-22, EC-5, GAP-8.
