# ADR-11 — A body change keeps the results of accepted rows

Status: Proposed

## Context

The schema can change a reducer body, a default or external code between two runs (CN-32). Settle stores the derived state at the durable watermark `D`, and it stores no bodies. An open replays the blocks above `D` with the bodies of the new run. An attach replays the blocks above `F` with the attached code (ADR-5). Neither returns a batch.

So after a change, the blocks at or below the replay start keep the results of the old body. The blocks above it get the results of the new body, but the target keeps their old output. The replay start is an internal watermark that the schema author does not see. For example, the body `n += x` gives `n = 1` at `D`. After a change to `n += 2 · x`, a next row with `x = 1` gives 3. A fold of the whole chain with the new body gives 4.

The alternatives:

- Define the result as a fold with the old bodies up to the replay start and the new bodies above it. This exposes `D` to the author. The target also keeps the old output of the replayed blocks, which breaks INV-20.
- Store the bodies with each checkpoint, and replay the blocks above `D` with the stored bodies. A change then applies only to the rows that Settle accepts after the open. This needs a new stored record and two runtimes at an open. It cannot cover external code, because the host holds that code.
- Refuse a body change. A pipeline then cannot fix its logic without a new object.

## Decision

The result of a reducer is the fold of the current bodies over the whole chain (DEF-41). A change of a body, a default or external code MUST keep the state and the emit rows of each row that Settle accepted earlier. The schema author makes sure of this. To change the logic, the new body switches on the block number, at a block above `L` of the last run. Settle does not check this rule. After a change that breaks it, the output of the reducer and of the objects after it is unspecified.

## Consequences

- DEF-41, INV-20 and the reference model stay simple: one fold with the current bodies.
- A fix of the logic for rows that Settle already accepted needs a new object or a new database.
- A changed default breaks the rule for each group that had rows before the change. So a default can change only for a reducer that has no groups yet.
- CT-2 changes bodies only in the ways that this rule allows.
- Shapes: DEF-41, CN-32.
