# ADR-15 — Check and convert value types at each boundary

Status: Proposed

This record was part 2 of the first draft of ADR-6, "Loud failure at every input boundary".

**Context.** Settle checks no value type at ingest, except the block number. A reducer can write any value into a state field. An aggregate reads only the values of its numeric mode and skips the others without an error. Event-rules arithmetic, Lua floats and every number that a JS reducer returns are Float64. So a sum over a UInt64 state field can lose all of them (GAP-1). The JS bindings turn objects into JSON text, also for a JSON column.

The alternatives:

- Refuse each value whose type differs from its column.
- Convert each value with a loose rule, for example a truncation of a float to an integer.
- Convert a value when the conversion is exact, and refuse it otherwise.

**Decision.** Settle converts a value to the declared type of its column or field when the conversion is exact:

- an integral Float64 into an integer type, in range;
- an integer into Float64, up to 2^53;
- a UInt64 into Uint256;
- a JS object into a JSON value, for a JSON column.

Otherwise an ingest call fails with INVALID_INPUT, and a reducer result fails the call with REDUCER_FAILED. An emit column that is not a state field or a group-by column takes the numeric mode of its first value. A later value outside that mode fails the call.

**Consequences.**

- No aggregate skips a value silently (INV-28), and the target gets the declared types (INV-27).
- Some pipelines that run today will fail with an error. Each of them loses values today.
- The conversion costs a type check for each value at the boundary.
- The mode rule for untyped emit columns needs a test against replay, because the first value after a reopen can differ.
- Shapes: REQ-27, WP-4, IB-40, IB-43, FM-19, FM-48, GAP-1.
