# ADR-7 — Persist the finality watermark next to the durable watermark

Status: Accepted (2026-10-20)

**Context.** ADR-2 stores the durable watermark `D` where recovery reads the finality, so an open set `F` to `D`. Under deferral, and on gappy chains where `D = min(F, L) < F`, the finality went back at a reopen. A smaller finalized head then passed, and a fork could undo blocks that Settle already reported final. The replay also finalized nothing, so the replayed tail stayed as unfinal data until the next call.

**Decision.** The write set of each call stores `F` in addition to `D`. The hash `H(F)` is already in the write set, because the stored hashes keep every block at or above `F`. An open restores `D`, replays the tail above `D`, and then advances finality to the stored `F`. A store without the stored `F` restarts finality at `D`. ADR-3 applies, so the format change needs no migration.

**Consequences.**

- The finality of the last durable commit survives a reopen (INV-10, INV-47), and the floor of ADR-2 holds after a crash.
- The memory of an open falls, because the replayed blocks at or below `F` become final at once.
- Each write set holds one more value.
- The fork part of the first draft of this record is now ADR-13.
- Shapes: REQ-5, WP-24, CN-20, INV-47, RS-12.
