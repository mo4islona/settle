# 13 — Conformance and TDD

**As of 2026-10-20.** This is a mutable status document. The statuses come from the test inventory of the repository at that date: about 580 Rust tests and 76 binding tests. The reference model of this document is normative. The matrices, the registers and the build order are status.

## Harness architecture

```
             ┌──────────────────────────── fault injectors ───────────────────────────┐
             │  store faults (read / write / sync / corrupt)   kill points   power loss │
             └──────────┬──────────────────────────────────────────────┬──────────────┘
                        ▼                                              ▼
 ┌───────────────┐   calls   ┌──────────────────────────┐  writes  ┌────────────────┐
 │ input         │──────────►│ Settle (black box)       │─────────►│ store          │
 │ simulator     │           │ Rust · Node · browser    │          │ memory or disk │
 │ + ledger      │◄──────────│                          │          └────────────────┘
 └──────┬────────┘  results  └────────────┬─────────────┘
        │ same calls                      │ batches, errors, status reads
        ▼                                 ▼
 ┌───────────────┐            ┌──────────────────────────┐
 │ reference     │──expected─►│ comparator + structural  │──► pass / fail + seed
 │ model         │            │ validators               │
 └───────────────┘            └──────────────────────────┘
                                          ▲
                                          │ signals OB-1..OB-16
                                  observability scraper
```

- The input simulator makes random call histories from a seed. A history holds ingest calls with gaps, forks by both paths, finality above the latest block and heartbeat calls. It also holds acks that come late, crashes and reopens. Its ledger records every row that it sends, for INV-29.
- The model target applies each batch idempotently (RP-21). The comparator checks each record against the model output before its call.
- The comparator works at each observation point. Quiescence for a final comparison means an empty ack queue and no call in progress.

## Reference model

The model is a pure state machine over the instance state of DEF-86. `out(A)` recomputes the output image from the accepted chain alone, with its own aggregate code. It never reads the state of Settle. The functions `body`, `agg` and `window` follow DEF-41 to DEF-45, with saturating integer arithmetic. `body` folds the rows in chain order. `agg` folds in the two levels of DEF-42: the rows of each block first, then the block contributions.

The model folds with the current bodies over the whole chain. So CT-2 changes a body only in a way that CN-32 allows, for example with a switch at a block above `L`.

Outside journal mode, a crash can lose batches that the target already applied (RP-21). Such a batch can insert rows above `L`, and a compensation can delete rows at or below it. The model keeps `K`, the keys that these batches changed. It compares the target with `out(A)` only outside `K` (INV-20). A later record of a key removes the key from `K`, because the target then holds the values of that record. A key that gets no later record stays in `K`, which is the degrade of FM-51.

```
state: Π, A = {}, H = {}, L = ⊥, F = 0, D = ⊥, Q = [], s = 1, health = healthy
       C = copy of the durable part of the state;  T = {} (the model target)
       K = {}                 (the keys that can differ after a crash, RP-21)

def out(A):                                      # DEF-46
    res = {}
    for t in raw tables of Π, not virtual:
        res[t] = {(b, i): row for b in A for i, row in enumerate(A[b][t])}
    emits = {}
    for r in reducers of Π, in topological order:
        st = {}; emits[r] = []
        for (b, i, row) in chain_order(source(r)):            # DEF-40
            g = key(row, r.group_by)
            st[g], rows = body(r, st.get(g, defaults(r)), row) # sees only st[g] and row
            emits[r] += [(b, i, j, e) for j, e in enumerate(rows)]
    for v in views of Π:
        rows = chain_order(source(v))
        if v.sliding: rows = [x for x in rows if in_window(A, v, x.block)]   # DEF-45
        groups = group_by_key_items(v, rows)                                 # DEF-30, DEF-32
        res[v] = {g: key_items(g) + [agg(f, rs) for f in v.aggs]
                  for g, rs in groups if has_data(v, rs)}                    # DEF-42..DEF-44
    return res

def ingest(rows, chain, fin):                    # TR-1
    require health == healthy                    else POISONED or SYNC_FAILED
    require places(Q) < P_MAX_UNACKED            else PENDING_ACK
    require fin.number >= F                      else INVALID_INPUT
    sup = chain ∪ {fin}
    c = min({b for (b, h) in sup if L ≠ ⊥ and b <= L and b in H and H[b] ≠ h}, default=⊥)
    if c ≠ ⊥:
        require c > F                            else FORK_BELOW_FINALITY    # WP-13
        p = max({b for (b, h) in sup if b < c and F <= b <= L and H.get(b) == h}, default=⊥)
        require p ≠ ⊥                            else NO_COMMON_ANCESTOR     # WP-12
    base = p if c ≠ ⊥ else L
    require rows_valid(rows)                     else INVALID_INPUT          # WP-4
    require rows == [] or base == ⊥ or min_block(rows) > base                # WP-5
                                                 else INVALID_INPUT
    before = out(A)
    assert image(T) == before at each key outside K                         # INV-20
    if c ≠ ⊥: rollback(p)
    add rows to A; L = max block with rows, else base; F = fin.number
    for (b, h) in sup: H[b] = h                                              # WP-16
    if checkpoint_due(): D = min(F, L)                                       # WP-40, WP-42
    return emit(diff(before, out(A)), before)

def fork(chain):                                 # TR-2
    require health == healthy and places(Q) < P_MAX_UNACKED
    require chain ≠ {}                           else INVALID_INPUT
    p = max({b for (b, h) in chain if F <= b <= L and H.get(b) == h}, default=⊥)
    require p ≠ ⊥                                else NO_COMMON_ANCESTOR
    before = out(A); rollback(p)
    for (b, h) in chain: H[b] = h
    return ((p, H[p]), emit(diff(before, out(A)), before))

def rollback(p): A = A↾p; L = p; H = H↾p                                     # WP-14

def diff(before, after):                         # RP-3, RP-4, RP-6
    for each table and key in before ∪ after:
        absent → present:            insert(values = after[key])
        present → absent:            delete(values = before[key], prev = before[key])
        present in both, not equal:  update(values = after[key], prev = before[key])

def emit(records, before):
    if records == []: join_or_commit(); return ⊥                             # WP-33
    validate(records, before)                    # the structural validators below
    batch = (s, heads(), records); s += 1; Q.append(batch)
    K = K − the keys of records                  # the target takes their values (RP-21)
    return batch                                 # the model host applies it to T idempotently

def ack(n):                                      # TR-3
    require health ≠ sync_failed                 else SYNC_FAILED
    if Q == [] or n < Q[0].seq: return ok
    require n <= Q[-1].seq                       else WRONG_ACK_SEQUENCE
    commit every entry of Q up to n, oldest first; C = durable part of the state

def crash():                                     # TR-6
    if not journal: K = K ∪ the keys of the batches in Q that T applied      # RP-21
    lose everything except C and, in journal mode, the journal

def open(applied):                               # TR-4
    state = C; reconcile the journal with applied (WP-52); Q = []
    # T and K stay: they belong to the model host, not to Settle
    # the derived state follows from A; F is the committed F (INV-47)

after every transition: assert well_formed()     # INV-1..INV-9
```

A new object at an open covers only the blocks above `D` (CN-33). The model starts its `out` for that object at `D + 1`.

### Free variables

The model and Settle can differ only in these points:

| ID | Free variable |
|---|---|
| FV-1 | The order of the tables in a batch. |
| FV-2 | The order of the records inside one table of a batch. |
| FV-3 | The content and the durations of the performance tree. |
| FV-4 | The text of an error message. |
| FV-5 | The age value of a PENDING_ACK error. |

Everything else is deterministic against the model, also the bits of each Float64 aggregate.

## Test classes

| ID | Class | Primary properties | Needs |
|---|---|---|---|
| CT-1 | Model-based property tests: random histories against the reference model, with the structural validators on every batch. | INV-1 to INV-9, INV-10 to INV-14, INV-20 to INV-31, INV-36, LIV-4, LIV-5 | HC-2, HC-4, HC-9, HC-10, HC-20 |
| CT-2 | Reopen differential: one history with reopens at random points against the same history with no reopen. | INV-5, INV-10, INV-40, INV-41, INV-43, INV-44, INV-47, INV-48 | HC-2, HC-3, HC-8, HC-20 |
| CT-3 | Crash points and power loss: a kill at each commit point, and a store that drops the writes after the last sync. | INV-15, INV-40, INV-41, INV-42, INV-49, LIV-9 | HC-7, HC-8 |
| CT-4 | Input-fault corpus: one case for each input fault and each error class. | INV-12, INV-14, INV-17, INV-26, INV-27, LIV-2, LIV-10, LIV-15 | HC-1 |
| CT-5 | Interface conformance: the same calls in Rust, Node and the browser, with the value and error mapping. | INV-26, INV-30, INV-33, LIV-12, IB-40..IB-51 | HC-16, HC-17 |
| CT-6 | Performance benchmarks: throughput, latency, instruction counts and scale cases. | SLI-1 to SLI-9, LIV-1, LIV-3, LIV-13 | HC-13, HC-14 |
| CT-7 | Soak: long runs that measure memory, disk, files and open time. | INV-7, INV-18, LIV-6, LIV-7, LIV-8, LIV-9, SLI-6 | HC-15 |
| CT-8 | Store faults: failed reads, writes and syncs at each step, and corrupt stored values. | INV-12, INV-15, INV-16, INV-46, LIV-11, LIV-14 | HC-5, HC-6 |
| CT-9 | Fuzz: the schema text, the row codec of each binding, the stored-data and journal decoders, and call sequences. | FM-1, INV-26 | HC-11 |
| CT-10 | Golden scenarios: domain pipelines with values that a person computed. | INV-20, INV-24 | HC-1, HC-21 |
| CT-11 | Schema-evolution matrix: each element of each layout, changed one at a time, and each allowed change. | INV-45, CN-32, CN-34 | HC-1 |
| CT-12 | Concurrency and isolation: parallel against sequential stages, two instances in one process, two opens of one directory, re-entrant callbacks. | INV-25, INV-35, CN-43, FM-46, FM-57 | HC-17, HC-22 |

## Structural validators

These checks need no domain knowledge, so CT-1, CT-2 and CT-5 run them on every batch:

- The batch decodes, and its sequence follows the previous sequence (INV-32).
- Each table of the batch is an output table and not a virtual table (INV-23).
- Each key occurs once in its table (INV-4).
- Each operation fits `out(A)` before the call, and no record is an empty change (INV-21).
- The previous values equal the row of `out(A)` before the call (INV-22).
- Each value has the type of its column (INV-27).
- The latest head is `L`, and the finalized head is `F` with its hash or ⊥ (INV-31).
- After the batch, `D ≤ min(F, L)` and the places do not exceed P-MAX-UNACKED (INV-1, INV-2).

## Acceptance matrix

| Requirement | Status | Note |
|---|---|---|
| REQ-1 | P | Parser tests cover cycles, absent sources and unknown types. No test opens the examples of IB-60. |
| REQ-2 | C | Validation tests compare the state after a failed ingest with the state before it. |
| REQ-3 | P | The differential compares the final targets. No test checks each record against a model target. |
| REQ-4 | P | The differential uses both fork paths, but its reference is Settle itself. |
| REQ-5 | P | known-violated: an open restores `F` as `D`, below the `F` of the last durable commit (GAP-3). |
| REQ-6 | C | Gappy chains are in the differential and in the ack tests. |
| REQ-7 | P | Many tests use virtual tables. No test checks the absence of their records after a fork. |
| REQ-8 | U | known-violated (GAP-16). |
| REQ-10 | C | Tests of the cumulative ack, and an end-to-end crash before an ack. |
| REQ-11 | C | Tests of a full queue, of joined heartbeat calls and of pipelined clients. |
| REQ-12 | C | The reopen differential on the memory backend, and an end-to-end replay with a byte-equal target. |
| REQ-13 | C | The journal variant of the differential, and an end-to-end fork after a crash. |
| REQ-14 | P | Tests count one sync for each ack and refuse acks after a failed sync. No power-loss simulation exists. |
| REQ-15 | P | Memory mode is the default of most tests. The Node sentinel creates a directory (GAP-12). |
| REQ-16 | C | Deferred output equals output with no deferral, and deferred recovery tests pass. |
| REQ-20 | C | Aggregate unit tests and the random fold oracle. |
| REQ-21 | C | Unit tests and golden scenarios. |
| REQ-22 | C | The sliding-window oracle with random forks. |
| REQ-23 | P | Pairwise differentials exist (closure against Lua, JS against Lua). No test compares all runtimes. A WHEN clause fails with INVALID_SCHEMA. |
| REQ-24 | P | A cycle test and one chained reopen test. Chained reducers are absent from the random histories. |
| REQ-25 | C | Format tests for each refused and each allowed change. |
| REQ-26 | U | known-violated (GAP-2). |
| REQ-27 | U | known-violated (GAP-1). |
| REQ-30 | P | known-violated: the engine module is public (GAP-39). |
| REQ-31 | P | Binding tests exist. No cross-binding comparison exists, and the release workflow is broken (GAP-24). |
| REQ-32 | P | known-violated: the browser entry does not ship its module (GAP-23), and its output differs (GAP-29). |
| REQ-33 | P | BigInt tests exist. known-violated for cursors (GAP-9). |
| REQ-34 | P | Only PENDING_ACK and WRONG_ACK_SEQUENCE have types (GAP-14). |
| REQ-35 | P | Builder tests check the generated text and run pipelines. No test compares a builder pipeline with its text. |
| REQ-40 | P | The throughput bench prints each target. No gate runs it (GAP-37). |
| REQ-41 | U | known-violated (GAP-6). |
| REQ-42 | U | known-violated (GAP-4). No fuzzer exists (GAP-34). |
| REQ-43 | P | An instruction case measures one reopen. No time bound is checked. |
| REQ-44 | P | Purge tests check the margin. No test checks the disk bound. |
| REQ-45 | P | Seeds are deterministic. No test compares the batches of two instances. |

## Properties matrix

| Property | Classes | Status | Note |
|---|---|---|---|
| INV-1 | CT-1, CT-2 | P | Backfill tests read `D`. The differential does not read it after each step. |
| INV-2 | CT-1, CT-4 | C | Tests of the full queue. |
| INV-3 | CT-1, CT-2 | P | Pipelined tests check the sequences. No check after each reopen. |
| INV-4 | CT-1 | P | Unit tests of the merge. No validator runs on each batch. |
| INV-5 | CT-1, CT-2 | C | The differential compares stored and memory hashes at each reopen. |
| INV-6 | CT-1 | P | Unit tests of empty groups. |
| INV-7 | CT-2, CT-7 | P | Tests of the purge margin. |
| INV-8 | CT-1, CT-4 | C | Tests of the emit limit and of columns that shrink. |
| INV-9 | CT-1 | P | The fold and window oracles are independent. The pipeline oracle is Settle itself. |
| INV-10 | CT-1, CT-2 | P | known-violated (GAP-3). |
| INV-11 | CT-1, CT-2 | P | In a running process: C. known-violated after a reopen (GAP-3). |
| INV-12 | CT-4, CT-8 | P | known-violated for attach (GAP-31). |
| INV-13 | CT-1 | U | No test reads a status twice. |
| INV-14 | CT-4 | P | Dedup tests, also for block 0. No test for a second table of an accepted block. |
| INV-15 | CT-8, CT-3 | P | CT-8 is C, with a store that records each commit. CT-3 is U: no kill points (HC-7). |
| INV-16 | CT-8 | C | Tests of each poison trigger and of a failed sync. |
| INV-17 | CT-4 | C | A test sends a conflict with a bad row. |
| INV-18 | CT-1, CT-7 | U | No test classifies deletes. |
| INV-20 | CT-1, CT-2, CT-10 | P | The differential and the golden scenarios. No independent model (GAP-32). |
| INV-21 | CT-1 | U | known-suspect (GAP-41). |
| INV-22 | CT-1 | U | known-violated (GAP-10). |
| INV-23 | CT-1 | P | No validator on compensation batches. |
| INV-24 | CT-1 | P | The fold oracle (300 seeds of 300 operations) compares the running fold with a fresh fold of the same block contributions. It does not fold `A` from scratch. The window oracle compares Settle with its own replay (GAP-32). |
| INV-25 | CT-1, CT-12 | P | No parallel against sequential comparison. |
| INV-26 | CT-4, CT-5 | P | Error tests in Rust. JS tests only for two classes. |
| INV-27 | CT-1, CT-4 | U | known-violated (GAP-1). |
| INV-28 | CT-1 | U | known-violated (GAP-1, GAP-26). |
| INV-29 | CT-1 | U | No ledger. |
| INV-30 | CT-1, CT-5 | P | The cursors are checked in tests, not after each step. |
| INV-31 | CT-1 | P | Some tests check the heads. |
| INV-32 | CT-1 | P | Pipelined tests. |
| INV-33 | CT-5 | U | known-violated (GAP-11). |
| INV-35 | CT-12 | U | No test. |
| INV-36 | CT-1 | U | known-violated (GAP-7). |
| INV-40 | CT-2, CT-3 | P | Memory backend only. No kill points. |
| INV-41 | CT-2, CT-3 | P | The journal differential on the memory backend. |
| INV-42 | CT-3 | U | No kill during an open. |
| INV-43 | CT-2 | C | The reopen differential, 300 seeds of 40 steps and 5,000 seeds of 8 steps. |
| INV-44 | CT-2 | C | Deferred schemas in the differential and in the backfill tests. |
| INV-45 | CT-11 | C | Format tests. |
| INV-46 | CT-8 | P | Corrupt view and reducer state. Raw rows only in unit tests. |
| INV-47 | CT-2 | U | known-violated (GAP-3). |
| INV-48 | CT-2 | P | Purge tests. No metamorphic run over the margins. |
| INV-49 | CT-3 | U | No power-loss store (GAP-33). |
| LIV-1 | CT-6 | P | Scale cases double the input, by hand on the host. |
| LIV-2 | CT-4 | C | A full queue fails at once. |
| LIV-3 | CT-6, CT-7 | P | One reopen case. |
| LIV-4 | CT-1 | C | Pipelined tests. |
| LIV-5 | CT-1 | P | The differential with late acks. |
| LIV-6 | CT-7 | U | No memory probe. |
| LIV-7 | CT-7 | P | Purge tests. |
| LIV-8 | CT-7 | P | Window record tests. |
| LIV-9 | CT-3, CT-7 | P | Journal tests. |
| LIV-10 | CT-4 | U | known-violated (GAP-5). |
| LIV-11 | CT-8 | P | Reopen after a failed fork ingest. |
| LIV-12 | CT-5 | U | known-violated (GAP-13). |
| LIV-13 | CT-6 | U | No timed attach. |
| LIV-14 | CT-8 | C | The failing store returns errors. |
| LIV-15 | CT-4 | U | No test reads the log event. |
| FM-1 | CT-9 | U | known-violated (GAP-4). |
| FM-2, FM-3 | CT-8, CT-4 | P | The poison and undo tests. |
| FM-4 | CT-1 | U | known-violated (GAP-1). |
| FM-10..FM-17 | CT-4 | C | Validation and fork tests. |
| FM-18, FM-19 | CT-4 | U | OQ-1 and GAP-1. |
| FM-20..FM-25 | CT-4, CT-1 | P | Fork tests. FM-22 is known-violated on the in-line path (GAP-8). |
| FM-26..FM-28 | CT-4, CT-9 | U | GAP-9 and GAP-17. |
| FM-29 | CT-2 | C | Replay gives the same batch. |
| FM-30..FM-35 | CT-8 | C | The failing store and the corruption tests. |
| FM-36..FM-38 | CT-3, CT-11 | P | Format refusal: C. Torn log tail: U. |
| FM-40, FM-43..FM-45 | CT-4 | C | Reducer error, emit limit and external bridge tests. |
| FM-41, FM-42, FM-46..FM-48 | CT-4, CT-9 | U | GAP-4, GAP-5, GAP-16 and GAP-1. |
| FM-50..FM-56 | CT-2, CT-3 | P | Drop and reopen only. No kill points. |
| FM-57 | CT-12 | U | No test of two opens. |
| FM-60..FM-68 | CT-4, CT-2 | P | Ack and journal tests. |
| FM-70..FM-76 | CT-11, CT-7 | P | Format tests. FM-71 and FM-73 are known-violated (GAP-18, GAP-12). |
| SLI-1 | CT-6 | P | Manual throughput bench. |
| SLI-2 | CT-6 | P | Manual tip bench. |
| SLI-3 | CT-6 | P | Measured once. |
| SLI-4 | CT-6 | P | Manual rollback bench. |
| SLI-5 | CT-6 | P | One instruction case. |
| SLI-6 | CT-7 | U | Not measured. |
| SLI-7 | CT-6 | C | Instruction counts with a saved baseline and a limit. |
| SLI-8 | CT-6 | P | Manual, with a cached Postgres baseline. |
| SLI-9 | CT-6 | P | Manual reorg bench. |

## Gap register

**As of 2026-10-20.** Priorities: P0 is an active production risk, P1 a correctness hole with a plausible trigger, P2 bounded or rare, P3 polish. No entry is P0.

| ID | Statement | Violated | Priority | First test |
|---|---|---|---|---|
| GAP-1 | Settle checks no value type at ingest. An aggregate skips each value outside its numeric mode without an error. Lua floats and every number from a JS callback are Float64, so an integer view column drops them. | REQ-27, INV-27, INV-28, FM-4, FM-19, FM-48 | P1 | An external reducer emits a UInt64 state field `n`, and a view sums `n`. Expect the sum of `n`. The code shows that the sum stays empty today, but no test confirms it yet. |
| GAP-2 | The schema accepts constructs that Settle ignores: WHERE, HAVING, joins, ORDER BY, LIMIT and DISTINCT. It does not match GROUP BY to the key items. It accepts duplicate names, a view with no aggregate, numeric aggregates over columns with no numeric mode, and defaults that do not parse. | REQ-26 | P1 | Open a view with a WHERE clause. Expect INVALID_SCHEMA. |
| GAP-3 | An open sets `F` to `D`. The reported finality goes back, a smaller finalized head passes, and a fork can undo blocks that were final. | REQ-5, INV-10, INV-11, INV-47, WP-24, CN-20 | P1 | Use a checkpoint interval of 3, reach `F = 49` with `D < 49`, reopen. Expect the finalized cursor at 49. |
| GAP-4 | Input can terminate the process. A Lua compile error or a failed module load panics at open. The parser can panic on some non-ASCII text. The builders panic on some arguments. The JS boundary has no unwind guard. | REQ-42, FM-1, FM-41, FM-47, WP-73 | P1 | Open a schema with a Lua syntax error. Expect INVALID_SCHEMA, not a panic. |
| GAP-5 | A reducer body has no CPU bound and no memory bound. An endless Lua loop blocks an ingest call forever. | LIV-10, FM-42 | P2 | A Lua body `while true do end`. Expect REDUCER_FAILED within P-REDUCER-STEPS. |
| GAP-6 | Resident memory has no bound. Reducer groups never leave memory, and an open loads every view group. | REQ-41, PF-1, RS-20 | P1 | Measure the bytes for each group at 1M groups. Then expect a run at P-W-GROUPS below P-MEMORY-CEILING. |
| GAP-7 | The Lua state table and Lua globals keep values across groups and calls. These values are not rolled back and do not survive a reopen. | INV-36, INV-43 | P2 | Group A sets an undeclared state key. Group B reads it. Expect nil. |
| GAP-8 | An in-line fork with no common ancestor rolls back to block 0 when `F` is 0. A fork call fails in the same case. | WP-12, FM-22 | P2 | Ingest blocks 0 to 5 with hashes, then a conflict at 3 with no block that matches. Expect NO_COMMON_ANCESTOR. |
| GAP-9 | The Node and browser bindings keep cursor block numbers as 32-bit numbers and cut larger values. | REQ-33, IB-41, FM-28 | P2 | Ingest block 2^32 + 5 in Node. Expect a cursor at 2^32 + 5. |
| GAP-10 | A raw delete record has no previous values. A rollback and a new row of the same raw key in one call merge into an update with no previous values. | RP-4, INV-22 | P2 | An in-line fork replaces the rows of one block. Expect an update with the old row as previous values. |
| GAP-11 | The buffer setting and the buffer gauges have no effect. The buffer is empty after each call. | INV-33, OB-16 | P3 | Remove them (ADR-6), or test that each gauge reaches its full range. |
| GAP-12 | The Node binding creates a directory named by the memory sentinel. | REQ-15, IB-23, FM-73 | P2 | Open in Node with the sentinel. Expect no directory. |
| GAP-13 | The Node and browser bindings have no close. Only garbage collection releases the lock of the store. | LIV-12 | P2 | Open, close and open again one directory in one process. Expect success. |
| GAP-14 | JS errors have types only for PENDING_ACK and WRONG_ACK_SEQUENCE. In Rust, several classes share one variant. | REQ-34, IB-50 | P3 | Trigger POISONED in Node. Expect a class field. |
| GAP-15 | The JS bindings give no finalized cursor, no durable block and no last acked sequence. No read tells sync-failed from poisoned. | OB-2, OB-3, OB-5, OB-6 | P3 | Read each value in Node and compare it with Rust. |
| GAP-16 | Settle does not refuse a call from a reducer callback into the instance that runs it. | REQ-8, WP-1, FM-46 | P2 | A JS callback calls ack on its instance. Expect INVALID_OPERATION. |
| GAP-17 | Some additions wrap at the integer limits in a release build: the next block after 2^64 − 1, the interval products and some counts. | FM-1, FM-27 | P3 | A finalized head at 2^64 − 1 on an empty instance. Expect INVALID_INPUT or exact behavior. |
| GAP-18 | The stored data and layout of a removed object stay forever. When the object comes back with the same layout, its old state comes back. | CN-34, RS-22, FM-71 | P2 | Ingest with view V, reopen without V, ingest, reopen with V. Expect the behavior that OQ-3 decides. |
| GAP-19 | An open commits the journal before its second format check, so a failed open can change the store. | CN-30 | P3 | A journal with a layout that the schema refuses. Expect the store unchanged after the failed open. |
| GAP-20 | A reducer that the host registers at run time feeds no view, so its output is not visible. After a crash during deferral, its replay covers blocks that the running instance never gave it. | WP-61, INV-43 | P3 | Register a reducer during deferral, crash, reopen. Expect the same state as with no crash. |
| GAP-21 | Each expiry in a sliding view refolds the window, so the cost of a tip block grows with the window. | PF-4 | P2 | SC-6 at three widths. Expect a cost for each block that does not grow with the width. |
| GAP-22 | The replayed tail stays as unfinal data until the next ingest call. | RS-12 | P3 | Measure the memory after an open of a long tail. Expect only final state for blocks at or below `F`. |
| GAP-23 | The browser entry imports a module that the package does not ship. | REQ-32 | P2 | Pack the package, install it, and load the browser entry. |
| GAP-24 | The release workflow points at a package directory that does not exist. | REQ-31 | P2 | Run the release build on a test tag. |
| GAP-25 | The public documents show calls that the API removed. | REQ-30, REQ-31 | P3 | Compile each example of the public documents. |
| GAP-26 | The Lua bridge loses data. A row read with the index form gives nil, a nested emit table becomes Null, and a text that is not UTF-8 becomes empty. | INV-28 | P2 | A Lua body reads `row["x"]`. Expect the value of `x`. |
| GAP-28 | A Base58 value leaves the JS bindings as hex. The TS types say BigInt for Uint256, but the value is hex text. | IB-44 | P3 | Ingest a Base58 value and read it back in Node. Expect the form that OQ-4 decides. |
| GAP-29 | The browser output differs from Node: NaN and infinities become null, and bytes become arrays of numbers. | REQ-32, IB-48 | P2 | The same calls in Node and in the browser. Expect equal batches. |
| GAP-30 | A String group key whose bytes equal the encoding of a Null key shares one reducer group with it. | DEF-25 | P3 | Rows with the key Null and the key `"\x01\x00\x00\x00\x00"`. Expect two groups. |
| GAP-31 | A failed attach, registration or replay leaves the chain rolled back and in part replayed. The instance is not poisoned. | WP-64, INV-12 | P2 | An attach whose replay fails. Expect the state before the attach. |
| GAP-32 | No independent reference model exists. The differential tests compare Settle with itself, on the memory backend, with one table. | INV-9, INV-20 | P1 | Build HC-4, and run CT-1 on the schemas of the current tests. |
| GAP-33 | No kill-point or power-loss harness exists, and the differential never runs on the persistent backend. | INV-40, INV-42, INV-49 | P1 | Build HC-7 and HC-8. Kill after the journal write and before the return. |
| GAP-34 | No fuzzer exists. | REQ-42, FM-1 | P2 | A fuzzer for the schema parser with case-mapping text. |
| GAP-35 | No line coverage is measured. | MG-4 | P3 | Build HC-12 and record the first value. |
| GAP-36 | CI runs no formatter check and no linter. | MG-10 | P3 | Add both to CI. |
| GAP-37 | No performance gate runs in CI or before a release. | REQ-40, MG-7, MG-8 | P3 | A release checklist step that runs CT-6 on the reference host. |
| GAP-38 | The configuration differs across bindings. The Node binding has no deferral, retention, timing or warning setting, and the Rust builder has no queue or journal setting. | REQ-31, IB-22 | P3 | Set each configuration field in each binding. |
| GAP-39 | The engine module is public in Rust, so a host can commit, roll back or finalize outside an atomic call. | REQ-30 | P3 | A compile test that the engine module is not public. |
| GAP-40 | A `count(c)` group with only Null values has data before finality and none after it, so its presence changes at finality. | DEF-42, INV-6 | P3 | A group with only Null values of `c`, then a finality advance and a reopen. Expect no change in presence. |
| GAP-41 | After an in-line fork, a view update can have values equal to its previous values. | RP-6, INV-21 | P3 | An in-line fork that restores the same aggregate. Expect no record. |
| GAP-42 | The layout check lets the virtual flag and the runtime kind change. | CN-32 | P3 | Make a stored table virtual and reopen. Expect INCOMPATIBLE_STORAGE. |
| GAP-43 | No memory gauge, no open-phase report and no store gauge exists. | OB-10, OB-11, OB-12 | P3 | Read each gauge after a run and compare it with a probe. |
| GAP-44 | The type names `Bytes`, `JSON` and `JSONB` probably fail in a CREATE TABLE statement, so raw tables cannot have these types. This is known-suspect. | REQ-1, IB-62 | P2 | Open `CREATE TABLE t (block_number UInt64, doc JSON)`. Expect success. |
| GAP-46 | The JS and WASM bridges carry no Map fields (ADR-12). A reducer with a Map field fails each block in a JS or WASM body. | REQ-23, REQ-31 | P2 | A JS body that sets and reads a Map entry across a fork. Expect the batches of the same body in Lua. |

## Build order

| ID | Phase | Content | Done when |
|---|---|---|---|
| BO-1 | Harness skeleton | HC-4, HC-9, HC-10 and HC-17. Run CT-1 on the schemas of the current tests. | CT-1 runs in CI, and the matrix shows its first results. |
| BO-2 | P1 correctness | GAP-1, GAP-2, GAP-3 and GAP-4. Measure for GAP-6. | Each closure has its failing test first, and the matrix rows move to C. |
| BO-3 | Crash harness | HC-7 and HC-8 (GAP-33). CT-3 at each kill point. | INV-40, INV-41, INV-42 and INV-49 are C. |
| BO-4 | Robustness | GAP-5, GAP-7, GAP-8, GAP-16, GAP-17 and GAP-31. HC-11 (GAP-34). | FM-1 is C, and CT-9 runs nightly. |
| BO-5 | Binding parity | GAP-9, GAP-12, GAP-13, GAP-14, GAP-15, GAP-23, GAP-24, GAP-28, GAP-29 and GAP-38. | CT-5 compares all bindings in CI. |
| BO-6 | Performance regime | GAP-6, GAP-21 and GAP-22. HC-15 and the gates of GAP-37. Ratify ADR-9. | Each SLO row has a gate. |

Each phase ends with an update of the matrices and the gap register in the same change.

## Merge gates

| ID | Gate | Threshold | When | Enforced by | Blocking? |
|---|---|---|---|---|---|
| MG-1 | The Rust tests pass: library, integration and end-to-end. | P-GATE-TEST-FAILURES | per-PR | HC-1, HC-2, HC-3, HC-5, HC-21 | Yes |
| MG-2 | The binding tests pass for Node and the browser. | P-GATE-TEST-FAILURES | per-PR | HC-16 | Yes |
| MG-3 | Property coverage: the number of `INV`, `LIV` and `REQ` at C does not decrease. A change that adds a requirement or an invariant adds its matrix row and its test class. | P-GATE-PROPERTY-COV | per-PR | HC-18 | Yes. The checker compares the count with the stored floor, and a change that raises the count raises the floor. The floor goes down only in a change that corrects a wrong C, and that change names the row. |
| MG-4 | Line coverage of the changed lines, and of the whole crate. | P-GATE-DIFF-COV, P-GATE-TOTAL-COV | per-PR | HC-12 | Advisory (GAP-35) |
| MG-5 | Failing test first: each gap closure and each fix comes with a test that fails without it, named in the gap register. | P-GATE-TEST-FAILURES | per-PR | HC-1 | Yes, as a review item |
| MG-6 | A change to finalize, durability or recovery code runs the reopen differential with a red check and a mutation check. | P-GATE-TEST-FAILURES | per-PR | HC-2, HC-19 | Yes, as a review item |
| MG-7 | No instruction-count case grows by more than the noise band, unless the change notes it. | P-GATE-INSTR-NOISE | pre-release, and per engine change | HC-13 | Advisory (GAP-37) |
| MG-8 | Each SLO row of 11 holds, and a soak run (CT-7) shows no growth of memory, disk or open time. | P-GATE-WALL-NOISE | pre-release | HC-14, HC-15 | Advisory (GAP-37) |
| MG-9 | The checker of this suite reports no error. | P-GATE-SPEC-ERRORS | per-PR | HC-18 | Yes |
| MG-10 | The formatter reports no change, and the linter reports no warning at its configured level. | P-GATE-LINT-WARNINGS | per-PR | HC-23 | Advisory (GAP-36) |
| MG-11 | The nightly runs pass at their budgets: the oracle (CT-1), the crash points (CT-3), fuzz (CT-9) and concurrency (CT-12). | P-ORACLE-SEEDS, P-FUZZ-BUDGET | nightly | HC-4, HC-7, HC-11, HC-22 | Advisory (GAP-32, GAP-33, GAP-34) |
| MG-12 | Flake policy: no automatic retry. A test that fails twice with no code change goes to quarantine, with an owner and an expiry of P-QUARANTINE-DAYS. A silent skip is not allowed. | P-QUARANTINE-DAYS | per-PR | HC-1, HC-20 | Yes |
| MG-13 | The release build packs and installs each binding package. | P-GATE-TEST-FAILURES | pre-release | HC-16 | Advisory (GAP-23, GAP-24) |

A per-PR gate stays within P-GATE-PR-BUDGET of CI time. CT-6, CT-7 and the long budgets of CT-1 and CT-9 run outside per-PR CI.

## Harness capability register

| ID | Capability | Needed by | Status | Note |
|---|---|---|---|---|
| HC-1 | A Rust test runner for library, integration and end-to-end tests. | CT-4, CT-10, CT-11, MG-1, MG-5, MG-12 | C | Runs in CI in debug mode. |
| HC-2 | A random history generator with a model client and a target map: the reopen differential. | CT-1, CT-2, MG-1, MG-6 | C | Memory backend, one table, forks only above finality. |
| HC-3 | Pipelined and journal client models for the differential. | CT-2, MG-1 | C | Depths 1, 2 and 4. |
| HC-4 | An independent reference model as an executable oracle. | CT-1, MG-11 | U | GAP-32. |
| HC-5 | A store that fails reads, syncs and commits on request. | CT-8, MG-1 | C | |
| HC-6 | A tool that corrupts stored values of each kind. | CT-8 | P | View and reducer state only. |
| HC-7 | A kill-point injector, and a store that drops the writes after the last sync. | CT-3, MG-11 | U | GAP-33. |
| HC-8 | A run of the differential on the persistent backend. | CT-2, CT-3 | U | GAP-33. |
| HC-9 | Structural validators on every batch. | CT-1 | U | Today only the final target is compared. |
| HC-10 | An input ledger for provenance checks. | CT-1 | U | |
| HC-11 | Fuzzers for the schema text, the row codecs, the decoders and call sequences. | CT-9, MG-11 | U | GAP-34. |
| HC-12 | Line coverage instrumentation. | MG-4 | U | GAP-35. |
| HC-13 | An instruction-count runner with saved baselines and a limit. | CT-6, MG-7 | C | Manual, on the reference host only. |
| HC-14 | A wall-clock benchmark runner with a cached Postgres baseline and a state check. | CT-6, MG-8 | C | Manual, on the reference host only. |
| HC-15 | Memory, disk and file probes for long runs. | CT-7, MG-8 | U | |
| HC-16 | A binding test runner for Node and the browser. | CT-5, MG-2, MG-13 | C | Runs in CI. |
| HC-17 | A comparator that runs the same calls in Rust, Node and the browser. | CT-5, CT-12 | U | |
| HC-18 | The checker of this suite, with the coverage floor in `tools/coverage-floor`. | MG-3, MG-9 | C | [tools/check_spec.py](tools/check_spec.py). |
| HC-19 | Mutation checks of a fix. | MG-6 | P | Manual scripts. |
| HC-20 | Deterministic seeds, with the failing seed in the report. | CT-1, CT-2, MG-12 | C | |
| HC-21 | A Postgres container for end-to-end tests. | CT-10, MG-1 | C | The image has no pinned version. |
| HC-22 | A switch between parallel and sequential stages. | CT-12, MG-11 | U | |
| HC-23 | A formatter check and a linter in CI. | MG-10 | U | GAP-36. |

## Closed

| ID | Closed on | Resolution |
|---|---|---|
| GAP-27 | 2026-10-19 | Event-rules arithmetic was Float64 only. ADR-10 removes the runtime, and GAP-45 holds the rest of the deviation. |
| GAP-45 | 2026-10-20 | The event-rules runtime and its grammar are removed. A reducer with a WHEN clause fails with INVALID_SCHEMA, and the message names Lua. The tests and benches that used event rules use Lua. |
