# ADR-4 — Exact aggregates, windows and view records

Status: Accepted (historical)

This record holds four related decisions about the values of a view and the records that it emits. Each part gives its context, its decision and its consequences.

## 1. Exact aggregates by block contributions and a fold that never subtracts

**Context.** Min, max, first and last have no inverse, so a rollback cannot subtract a removed value. A float sum depends on the order of addition, and a saturating integer sum has no inverse near the limit. A hot group that a call changes in many blocks cost quadratic work when each read folded all unfinal blocks (97023ca). A running fold fixed that, but a bulk expiry then refolded once for each moved block (3c8c000).

**Decision.** Each aggregate keeps one contribution for each unfinal block, and a finalized accumulator. A rollback removes the contributions above the fork point. A running fold is kept for a group with many unfinal blocks. It is bit-identical to a left fold in chain order and never subtracts. A removal drops the running fold, and the next read folds again.

**Consequences.**

- Every aggregate equals its value from scratch, bit for bit (INV-24), and the reference model can compare exactly.
- A sliding view refolds its window after each expiry, so its tip cost grows with the window (GAP-21). A fix must keep the exact fold order.
- Shapes: REQ-20, REQ-22, DEF-42, INV-24, PF-4.

## 2. Integer aggregates in 128-bit and 256-bit saturating arithmetic

**Context.** Token amounts in wei and lamports often exceed 2^53, so a Float64 sum loses precision. A sum of many UInt64 values can exceed 2^64 (88766cf, 2dd4353).

**Decision.** The numeric mode of a column follows its declared type. Integer columns sum in a signed 128-bit accumulator, and Uint256 columns in an unsigned 256-bit accumulator, both with saturation. Other columns use Float64. The integer result type is the smallest of UInt64, Int64 and Float64 that holds the value.

**Consequences.**

- Integer sums are exact in the range of the accumulator.
- A value outside the mode of its column needs a rule. Today the aggregate skips it silently (GAP-1). ADR-6 proposes the rule.
- Shapes: REQ-20, DEF-43.

## 3. Expired window data stays until its expirer is final

**Context.** A sliding view deleted the data of a block when the block left the window. A fork below the block that pushed it out then left the window short, and a replay after a reopen gave another window. The defect existed on master and a review of the deferral work found it (9eee235).

**Decision.** When a block leaves the window, Settle moves its data to a stash keyed by the block that pushed it out. A rollback restores the stash entries above the fork point. A finality advance drops the entries whose expiring block is final. The checkpoint stores the stash, and the window is stored block by block (2f917c9).

**Consequences.**

- A rollback restores the window exactly, and a replay agrees with the running instance.
- The stash grows while finality stalls (RS-23).
- Shapes: REQ-22, RS-13, DEF-89, LIV-8, WP-14.

## 4. One view record per group per call, and merged batches

**Context.** A view group that one call changes in many blocks produced one record for each block, and the change buffer merged them. That cost a build and a merge for each block (ea50ec3). The order of the records was first the hash order. The random key order made the Postgres writes of a target 3% to 5% slower (31e4fad).

**Decision.** A view keeps one snapshot of each group for the whole call. It emits one record for each changed group, in the order of first touch. The change buffer merges all records of one key in a call by the merge table of RP-11. A call whose records all cancel returns no batch.

**Consequences.**

- A batch has one record for each key (RP-5), so the order of apply does not matter.
- The record order is a free variable of the contract (FV-2), even though the code keeps first-touch order.
- An in-line fork can leave an update with values equal to its previous values (GAP-41).
- Shapes: REQ-3, RP-5, RP-11, RP-13.
