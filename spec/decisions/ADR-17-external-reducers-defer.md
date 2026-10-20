# ADR-17 — External reducers defer their checkpoints

Status: Proposed

**Context.** ADR-2 turns deferral off for a pipeline with an external reducer: an open cannot run external code before the host attaches it (ADR-5). The attach then replayed the chain of the reducer from `F + 1`. Before ADR-7, an open set `F` to `D`, so this replay covered every block above the checkpoint. Since ADR-7, an open restores `F` above `D`. A reopen differential with the gate removed lost the blocks in `(D, F]` for each external chain. Thus the gate was necessary.

The bodies of the Rust and JS bindings are external reducers. In a backfill on the Solana DEX bench, finality, checkpoint and commit take approximately 60% of the ingest time of such a pipeline.

**Decision.** An external reducer does not turn deferral off. If `D < min(F, L)` after the replay of an open, and an external reducer has no code, the open waits for code. During the wait, the reducers and views stay final at `D`, and `F` keeps its stored value. Each attach replays its chain from `D + 1`. Each ingest call fails until code attaches to every external reducer. The first ingest call after the last attach makes the replayed blocks final.

A reducer that the host registers at run time is not in the schema. Thus an open cannot wait for it, and its registration still turns deferral off.

**Consequences.**

- A pipeline with external reducers defers like a Lua pipeline. The gain on the Solana DEX bench (4,000 slots, backfill, persistent backend) is:
  - 100 slots in each call, an interval of 1,000 blocks: Rust closures +13%, JS +14%, Lua +10%.
  - 10 slots in each call, an interval of 100 blocks: Rust closures +23%, JS +17%, Lua +14%.
- An interval that is not larger than the blocks of one call gives no gain, because a checkpoint then comes at each call.
- The call at the end of an interval writes all the deferred state. For 100 slots in each call at 1,000 blocks, p99 ingest goes from 440 ms to 980 ms.
- The finality work stays: deferral removes the encode and the write of the checkpoint.
- After a crash during deferral, each attach replays up to P-CHECKPOINT-INTERVAL more blocks of its chain.
- A host must attach the code of every external reducer before the first ingest call after such an open, also a heartbeat.
- A replay that fails poisons the instance (WP-64, WP-71). Before, the reducer stayed attached after a failed replay, and the next ingest call made the gap final.
- The fork floor stays at the stored `F` during the wait (INV-47).
- The Node binding gets the interval setting (IB-22).
- Shapes: DEF-84, WP-41, WP-60, WP-62, CN-25, RS-16, LIV-13, IB-22.
