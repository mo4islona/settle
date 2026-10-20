# ADR-13 — A fork with no common ancestor fails on both paths

Status: Proposed

This record was part 2 of the first draft of ADR-7. It keeps the finality floor of ADR-2 and gives one response to one fault.

**Context.** A fork call with no supplied block that matches in `[F, L]` fails ("fork too deep"). An ingest call with a hash conflict and no block that matches rolls back to block 0 when `F` is 0 (GAP-8). When `F` is above 0, it fails with a message about final state, also when the conflict is above `F`. One fault thus gets three different responses.

The alternatives are to fail on both paths, or to roll back to `F` on both paths. A rollback to `F` relies on the finality promise of the source and is correct when that promise holds. But a chain with no common ancestor in `[F, L]` more often comes from a short chain or from a wrong source. Also, a rollback to `F` can undo many blocks that the target then writes again.

**Decision.** Both paths use the fork point of DEF-13. When no supplied block in `[F, L]` matches, the call fails with NO_COMMON_ANCESTOR and changes nothing. The host can retry with a longer chain.

**Consequences.**

- One fault gets one response, and the error class tells the host what to do.
- A host that sends a short chain gets an error in place of a deep rollback.
- Shapes: WP-12, FM-22, EC-5, GAP-8.
