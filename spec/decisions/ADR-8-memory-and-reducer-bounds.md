# ADR-8 — Bounds on memory and on reducer execution

Status: Proposed

This record holds two related proposals. Both give a bound to a resource that a pipeline can take without a limit today. The maintainers can accept each part on its own.

## 1. Bound resident memory with group eviction

**Context.** Reducer groups never leave memory, and an open loads every stored view group. Memory grows with the number of distinct keys, without a bound, and a pipeline with many keys runs out of memory (GAP-6). The bound of the ack queue does not limit this memory. A group with an unfinal block or a write that is not durable yet cannot leave memory, because its stored state lacks the unacked checkpoints. The fresh-database probe that skips the state read of a new group is correct only because no group leaves memory.

**Decision.** The first step measures the bytes of each reducer group and each view group at 1M groups. It also measures the peak memory of a long run. Then Settle evicts each group that has no unfinal block and no durable write that waits. It loads a group on a miss, and it loads view groups on demand at open. The ceiling P-MEMORY-CEILING becomes a formula of the configuration and the group count, and the host can set a limit.

**Consequences.**

- REQ-41 can hold, and P-SLO-RSS-PER-GROUP gets a value.
- A miss costs a read of the store, so throughput at high cardinality can fall.
- The fresh-database probe needs a new argument for correctness.
- Shapes: REQ-41, PF-1, RS-20, GAP-6.

## 2. Bound reducer execution

**Context.** A Lua body has no step limit and no memory limit. An endless loop blocks an ingest call forever, and a large allocation can take all the memory of the host (GAP-5). The schema author is trusted for logic, but not for the life of the host process.

**Decision.** Settle counts the steps of each Lua body call with an instruction hook and stops the call after P-REDUCER-STEPS steps. It limits the memory of each Lua runtime to P-REDUCER-MEMORY. A breach fails the call with REDUCER_FAILED, and Settle undoes the call. External reducers and Rust closures are host code and stay out of scope.

**Consequences.**

- LIV-10 holds for Lua reducers.
- The hook costs some time for each step. The budget must be large enough for real bodies.
- Shapes: LIV-10, FM-42, P-REDUCER-STEPS, P-REDUCER-MEMORY, GAP-5.
