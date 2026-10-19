# ADR-2 — Finality and the durable watermark

Status: Accepted (historical)

This record holds three related decisions about the finality watermark `F` and the durable watermark `D`. Each part gives its context, its decision and its consequences.

## 1. Finality bounds every rollback

**Context.** A rollback must restore the derived state of the fork point. That needs undo data for each block above the fork point. Undo data for all history costs memory and disk without a bound. The source also reports finality: it promises that final blocks do not change.

Several defects came from fork detection. An omitted chain counted as a fork and deleted data, and a chain that advanced skipped the overlap check (e9f8613). During backfill deferral, a review found that a fork could reach below finality, because hashes below finality stayed in memory. The fix put a floor at the single rollback entry point.

**Decision.** Settle keeps undo data only above the finality watermark. A fork is evidence only when a supplied hash differs from a stored hash. The fork point is the highest supplied block in `[F, L]` that matches. A conflict at or below `F` is an error, and Settle never rolls back below `F`.

**Consequences.**

- The finality lag, not the history, bounds the memory for undo data.
- A source that contradicts its finality stops the host with FORK_BELOW_FINALITY. The host cannot repair it without a new database.
- A host that supplies no hashes gets no fork detection (FM-24).
- The floor must also hold across a reopen. Today it does not (GAP-3, ADR-7).
- Shapes: REQ-4, REQ-5, WP-10, WP-11, WP-13, INV-11, NG-7.

## 2. Finality can run ahead of the latest block

**Context.** Some chains skip block numbers, for example Solana slots. A source can report a finalized block for which the host sends no rows. If Settle clamped the finality watermark to the latest block, the finality of such a chain would lag without a reason. The undo data would then grow.

**Decision.** Settle accepts a finalized head above the latest block. The finality watermark stays monotonic. A later block between the latest block and the finality watermark is still accepted, and the next finality advance makes it final. The durable watermark is clamped to `min(F, L)`.

**Consequences.**

- Gappy chains finalize at the speed of the source.
- The durable watermark needs the clamp, or a recovery can skip blocks (WP-42).
- A finalized head near the largest block number reaches integer limits (GAP-17).
- Shapes: REQ-6, WP-7, WP-20, DEF-14.

## 3. Backfill deferral stores the durable watermark

**Context.** In a backfill, the source marks each block final at the tip of each call. Settle then wrote a checkpoint of all changed derived state on every call. That cost 22% to 33% of the wall time. The live aggregates were already correct in memory, so the disk writes were pure durability cost.

A first design stored the finality watermark and wrote derived state only at the checkpoint. A recovery from it replays from `F + 1` and skips `(D, F]` silently. An adversarial review of eight scenarios rejected it.

**Decision.** Settle separates two watermarks: `F` bounds rollback, and `D` marks the derived state on disk. The stored watermark that recovery reads holds `D`, so recovery replays from `D + 1` with no change to the recovery code. Settle defers the checkpoint of a backfill call until `F − D` reaches P-CHECKPOINT-INTERVAL. Deferral is off for a pipeline with a sliding view or an external reducer.

**Consequences.**

- Backfill throughput rose by 35% to 45% on the persistent backend.
- A crash replays up to P-CHECKPOINT-INTERVAL extra blocks. The raw rows above `D` must stay (RS-1).
- External reducers cannot defer: a replay at open cannot run them before the host attaches their code (ADR-5).
- Sliding views do not defer: they were not tested with deferral, and they would gain little (OQ-5).
- A reopen restores `F` from the stored `D`, so finality goes back at a reopen (GAP-3). ADR-7 proposes the fix.
- Shapes: REQ-16, WP-40, WP-41, WP-42, INV-44, DEF-84.
