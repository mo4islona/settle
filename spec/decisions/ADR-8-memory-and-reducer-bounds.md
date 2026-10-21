# ADR-8 — Bounds on memory and on reducer execution

Status: Proposed

This record holds two related proposals. Both give a bound to a resource that a pipeline could take without a limit. The maintainers can accept each part on its own.

## 1. Bound resident memory with group eviction

**Context.** Reducer groups never left memory, and an open loaded every stored view group. Memory grew with the number of distinct keys, without a bound, and a pipeline with many keys ran out of memory. The bound of the ack queue does not limit this memory. A group with an unfinal block or a write that is not durable yet cannot leave memory, because its stored state lacks the unacked checkpoints. The fresh-database probe that skips the state read of a new group was correct only because no group left memory.

**Decision.** Each reducer and each view that is not sliding keeps about P-RESIDENT-GROUPS groups in memory (RS-24):

- A group can leave only when the committed store holds its state. It has no unfinal block and no final change that waits for a checkpoint. Also, the batch that holds its last checkpoint write is committed. Each checkpoint has a number, and each group keeps the number of the checkpoint that last wrote it. Each commit gives Settle the newest number on disk.
- After each ingest call, a node with more than P-RESIDENT-GROUPS groups drops the groups that can leave. Those written longest ago go first, down to three quarters of P-RESIDENT-GROUPS. So the scan of the groups comes once in a quarter of the cap of new groups.
- The groups that must stay can keep a node above three quarters of the cap. Then the next scan waits for a later commit, or for a quarter of the cap more groups.
- A group that is not in memory loads its stored state on its next row. An open loads no group of a view that is not sliding. It probes whether the store holds any group of the view, as it does for a reducer.
- The persistent store keeps a Bloom filter of the group keys in each file and in each memtable, at P-KEY-FILTER for each key. So a new group above the cap reads no block of the store.
- The first eviction of a node turns on its reads of the store. Before it, every group that the store holds since the open is in memory, so the fresh-database probe stays correct.
- A sliding view keeps its groups: its window bounds them, and it stores blocks, not groups.
- `count(c)` keeps its data once its block is final, also when every value was Null (DEF-42). So a group that the store holds never empties, and a miss never reads a group whose delete waits in an unacked batch.

**Consequences.**

- REQ-41 holds for the persistent backend. The memory backend holds the store in memory, so it has no ceiling.
- P-SLO-RSS-PER-GROUP gets the values of the `resident_memory` bench, at 1M groups on the persistent backend:

  | Node | Peak RSS per group | Peak RSS at 1M groups, cap 100K | Time of new keys above the cap | Time of a pass that misses on each row |
  |---|---:|---:|---:|---:|
  | Reducer | 358 bytes | 243 MB, not 475 MB | +8% | 1.8 times |
  | Reducer with a MAP field | 751 bytes | 315 MB, not 876 MB | +15% | 2.5 times |
  | View | 649 bytes | 284 MB, not 781 MB | +23% | 1.6 times |

- A miss of a stored group costs a read of the store, so above the cap the throughput falls. The worst case is a cyclic access to more keys than the cap. A new group costs only a check of the filters. At 5M stored groups, the filters cut the time of a pass of new keys above the cap by 7% to 25%.
- The filters take P-KEY-FILTER of memory for each stored group. A flush or a compaction that builds them raises the peak memory, by 40 to 60 MB at 5M stored groups.
- The groups of unfinal blocks, of a deferred checkpoint and of unacked batches stay. A finality stall, a long deferral interval or a long ack queue can keep a node above the cap.
- A view group that does not decode fails the ingest that loads it, not the open (INV-46).
- Before, a fork could delete the group of a `count(c)` that only Null values reached, after its block was final.
- Shapes: REQ-41, PF-1, RS-20, RS-24, INV-46, INV-48, CN-40, IB-2, IB-22, P-RESIDENT-GROUPS, P-KEY-FILTER.

## 2. Bound reducer execution

**Context.** A Lua body has no step limit and no memory limit. An endless loop blocks an ingest call forever, and a large allocation can take all the memory of the host (GAP-5). The schema author is trusted for logic, but not for the life of the host process.

**Decision.** Settle counts the steps of each Lua body call with an instruction hook and stops the call after P-REDUCER-STEPS steps. It limits the memory of each Lua runtime to P-REDUCER-MEMORY. A breach fails the call with REDUCER_FAILED, and Settle undoes the call. External reducers and Rust closures are host code and stay out of scope.

**Consequences.**

- LIV-10 holds for Lua reducers.
- The hook costs some time for each step. The budget must be large enough for real bodies.
- Shapes: LIV-10, FM-42, P-REDUCER-STEPS, P-REDUCER-MEMORY, GAP-5.
