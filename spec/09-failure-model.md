# 09 — Failure model

## Response verbs

Each fault has one of four required responses:

- **Mask:** Settle hides the fault and the result is correct.
- **Degrade:** the result is correct, but Settle or the host does more work, or a weaker tier applies.
- **Fail-safe:** the call fails with an error class and changes nothing (INV-12).
- **Alarm:** the call fails and the instance needs an action of the host or the operator: a poison, a sync failure or a refused open.

Bands: 1–9 global · 10–29 input · 30–39 storage · 40–49 reducer runtime · 50–59 process · 60–69 host and target · 70–79 operator.

## Global requirements

**FM-1 — No termination.** No input, schema text, reducer result, call order or stored data terminates the host process. Settle returns an error class. The current code can panic on some schema texts, on a Lua compile error and on some numeric limits (GAP-4, GAP-17).

**FM-2 — Fault classes.** Settle classifies each fault as transient or integrity:

- A transient fault fails one call. A retry of the same call is safe. Examples: PENDING_ACK, and STORAGE_FAILED at an ack commit.
- An integrity fault means that memory and the store can disagree, or that the input contradicts itself. Settle raises an alarm and never retries by itself. Examples: the poison triggers of WP-71, SYNC_FAILED, INCOMPATIBLE_STORAGE and TARGET_MISMATCH.

**FM-3 — Blast radius.** A fault affects only the instance that sees it (CN-44). A reducer failure affects only the call that runs it, except after a rollback in the same call (WP-71).

**FM-4 — No silent fault.** Each fault gives an error or a signal. No fault gives a wrong batch without an error. The current code skips mistyped values silently (GAP-1).

## Input faults

| ID | Fault | Required response |
|---|---|---|
| FM-10 | The finalized head is below `F` (a source that goes back). | Fail-safe: INVALID_INPUT. |
| FM-11 | The finalized head is above `L` (a gappy chain). | Mask: Settle accepts it (WP-20). |
| FM-12 | A block at or below `L` (a duplicate), also for another table of an accepted block. | Fail-safe: INVALID_INPUT (WP-5). |
| FM-13 | Gaps in the block numbers. | Mask (WP-7). |
| FM-14 | The blocks of one call arrive out of order. | Mask: Settle processes them in ascending order (WP-6). |
| FM-15 | A row without a block number, or with a block number that is not UInt64. | Fail-safe: INVALID_INPUT. |
| FM-16 | A positional row whose block number differs from its block, or whose width differs from its table. | Fail-safe: INVALID_INPUT. |
| FM-17 | Rows for an undeclared table. | Fail-safe: INVALID_INPUT. |
| FM-18 | An undeclared column name in a row map. | Open (OQ-1). Today Settle ignores the name. |
| FM-19 | A value whose type differs from its column. | Fail-safe: INVALID_INPUT, unless the value converts exactly (ADR-6). Today Settle stores it, and aggregates skip it (GAP-1). |
| FM-20 | A hash conflict above `F` with a common ancestor. | Mask: Settle rolls back (WP-14). |
| FM-21 | A hash conflict at or below `F` (a source that contradicts its finality). | Fail-safe: FORK_BELOW_FINALITY. |
| FM-22 | A conflict or a fork chain with no common ancestor in `[F, L]`. | Fail-safe: NO_COMMON_ANCESTOR (WP-12). The in-line path rolls back to block 0 today (GAP-8). |
| FM-23 | An empty fork chain. | Fail-safe: INVALID_INPUT. |
| FM-24 | A host that supplies no hashes. | Mask: no conflict, no rollback (WP-10). Settle cannot see a fork without hashes, so the host MUST supply them at the tip. |
| FM-25 | Stale hashes above `L` from an earlier chain. | Mask: they are not evidence, and the next call overwrites them (WP-16). |
| FM-26 | A very large call. | Degrade: the batch and its write set grow with the call (RP-14). |
| FM-27 | A block number or a finalized head near 2^64 − 1. | Fail-safe or exact behavior. Today some additions wrap in a release build (GAP-17). |
| FM-28 | A JS block number above 2^32 − 1. | Exact behavior up to P-JS-SAFE-INT. Today the JS cursors truncate it (GAP-9). |
| FM-29 | The host sends the same blocks again after a reopen. | Mask: the same batch (RP-21). |

## Storage faults

| ID | Fault | Required response |
|---|---|---|
| FM-30 | A write fails at an ack commit, for example on a full disk. | Fail-safe: STORAGE_FAILED. The batch stays unacked, and the host can retry (WP-36). |
| FM-31 | A sync fails. | Alarm: SYNC_FAILED, which stays (WP-37). A later sync cannot vouch for the lost writes. |
| FM-32 | A write fails at a heartbeat commit or a journal write. | Alarm: poison (WP-71). |
| FM-33 | A read fails during a rollback. | Alarm: poison (WP-71). |
| FM-34 | A read fails during an ingest call with no rollback. | Fail-safe: STORAGE_FAILED, and Settle undoes the call (WP-70). |
| FM-35 | A stored value does not decode. | Alarm: STORAGE_FAILED. View state fails the open. Reducer state fails each ingest call that reaches the group (INV-46). |
| FM-36 | A stored value is corrupt but decodes. | Out of scope: Settle keeps no checksums of its own. |
| FM-37 | A torn log tail after a power loss. | Mask: the committed state is a prefix of the commits (CN-12). |
| FM-38 | A format version or a layout that does not match, or data with no version. | Fail-safe: INCOMPATIBLE_STORAGE at open (WP-51). |

## Reducer runtime faults

| ID | Fault | Required response |
|---|---|---|
| FM-40 | A body fails at run time, or a JS function throws. | Fail-safe: REDUCER_FAILED with the cause of the body, and Settle undoes the call. After a rollback in the same call: alarm, poison (WP-71). |
| FM-41 | A body or a module has a syntax error, or a module does not load. | Fail-safe: INVALID_SCHEMA at open. Today the open panics (GAP-4). |
| FM-42 | A body loops without an end, or allocates without a bound. | Fail-safe: REDUCER_FAILED within P-REDUCER-STEPS and P-REDUCER-MEMORY (LIV-10). Today no bound exists (GAP-5). |
| FM-43 | A reducer emits more than P-MAX-EMIT-COLUMNS columns. | Fail-safe: REDUCER_FAILED. |
| FM-44 | Rows reach an external reducer with no attached code. | Fail-safe: INVALID_OPERATION (WP-62). |
| FM-45 | External code returns a bad shape or an emit row index that no row has. | Fail-safe: REDUCER_FAILED. |
| FM-46 | External code calls into the instance that runs it. | Fail-safe: INVALID_OPERATION (WP-1). Today the call is not refused (GAP-16). |
| FM-47 | A Rust closure panics. | Fail-safe: REDUCER_FAILED, and Settle undoes the call. Today the panic passes to the host and the instance state is not defined (GAP-4). |
| FM-48 | A body writes a value of the wrong type into a typed state field or emit column. | Fail-safe: exact conversion or REDUCER_FAILED (ADR-6). Today Settle keeps the value (GAP-1). |

## Process faults

| ID | Fault | Required response |
|---|---|---|
| FM-50 | A crash before an ack, and the target did not apply the batch. | Mask: the open continues from the last acked batch, and the same blocks give the same batch. |
| FM-51 | A crash after the target applied a batch and before its ack, outside journal mode. | Degrade: the batch comes again and the target applies it idempotently (RP-21). If those blocks fork during the downtime, nothing undoes them in the target. Only journal mode masks this (ADR-1). |
| FM-52 | The same crash in journal mode. | Mask: the open reconciles with the applied sequence (WP-52). |
| FM-53 | A crash during a cumulative ack. | Mask: the committed state is a prefix of the acked batches (CN-2). |
| FM-54 | A crash during an open. | Mask: the next open gives the same state (CN-22). |
| FM-55 | A power loss after a successful ack. | Mask: the tier of CN-10. |
| FM-56 | A power loss after a heartbeat commit. | Degrade: the heartbeat writes can be lost. The next calls give the hashes and the finality again. |
| FM-57 | A second instance opens a data directory that is open (a dual instance). | Fail-safe: STORAGE_FAILED for the second open (CN-43). |

## Host and target faults

| ID | Fault | Required response |
|---|---|---|
| FM-60 | The host acks a sequence that it did not get. | Fail-safe: WRONG_ACK_SEQUENCE. |
| FM-61 | The host acks before the target applies the batch. | Not detectable. The host duty RP-24 applies. |
| FM-62 | The host never acks. | Fail-safe: PENDING_ACK, and the log event of OB-8 after P-ACK-WARN (LIV-15). |
| FM-63 | The host drops an instance with unacked batches. | Mask outside journal mode: the batches are lost and come again. In journal mode, the open reconciles. |
| FM-64 | The target applies batches out of order or skips one. | Not detectable outside journal mode. The host duty RP-20 applies. |
| FM-65 | The target loses batches that it applied, for example after a restore from a backup. | Alarm in journal mode: TARGET_MISMATCH. Not detectable outside journal mode. |
| FM-66 | The target stores a sequence above the newest returned one. | Alarm: TARGET_MISMATCH. |
| FM-67 | The host makes a fork call before the target applied the earlier batches. | Not detectable. The host duty WP-15 applies. |
| FM-68 | The host continues to call a poisoned instance. | Fail-safe: POISONED (WP-72). |

## Operator faults

| ID | Fault | Required response |
|---|---|---|
| FM-70 | A schema change that changes a stored layout. | Fail-safe: INCOMPATIBLE_STORAGE at open (WP-51). |
| FM-71 | A schema change that removes an object. | Its stored data stays (CN-34). A later return of the object is open (OQ-3, GAP-18). |
| FM-72 | A data directory that another schema wrote. | Fail-safe: INCOMPATIBLE_STORAGE for each common name with another layout. |
| FM-73 | The memory sentinel as the data directory in the Node binding. | Mask: memory mode (IB-23). Today the binding creates a directory with that name (GAP-12). |
| FM-74 | An unknown compression name or a negative count. | Fail-safe: INVALID_CONFIG. |
| FM-75 | A database of another format version. | Fail-safe: INCOMPATIBLE_STORAGE. No migration exists (ADR-3). |
| FM-76 | A retention margin of 0, or the largest value. | Mask: 0 purges up to `D`, and the replay source stays (INV-7). The largest value never purges, so the disk grows (RS-21). |

## Fault cross-reference

| Faults | Properties | Test classes |
|---|---|---|
| FM-10 to FM-29 | INV-12, INV-14, INV-17, INV-26, INV-27, INV-28 | CT-4, CT-9 |
| FM-30 to FM-38 | INV-12, INV-15, INV-16, INV-46, INV-49, LIV-14 | CT-8, CT-3 |
| FM-40 to FM-48 | INV-12, INV-26, INV-36, LIV-10, FM-1 | CT-4, CT-9 |
| FM-50 to FM-57 | INV-40, INV-41, INV-42, INV-49, CN-43 | CT-3, CT-2, CT-12 |
| FM-60 to FM-68 | INV-15, INV-41, RP-20, RP-24 | CT-4, CT-2 |
| FM-70 to FM-76 | INV-45, INV-7, CN-34 | CT-11, CT-7 |
