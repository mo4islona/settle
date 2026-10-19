# ADR-6 — Loud failure at every input boundary

Status: Proposed

This record holds four related proposals. Each one replaces a silent effect of an input with an error, or removes an input that has no effect. The maintainers can accept each part on its own.

## 1. Refuse schema constructs that Settle does not execute

**Context.** The view parser reads only the first FROM item, the select list and the GROUP BY list. It ignores WHERE, HAVING, joins, ORDER BY, LIMIT and DISTINCT without an error.

It groups by the key items of the select list, not by GROUP BY. So a GROUP BY that differs from the key items has no effect. It accepts two objects with one name and a view with no aggregate. It also accepts numeric aggregates over a column with no numeric mode, and defaults that do not parse. A schema author then gets a pipeline that does something other than the text says (GAP-2).

The alternatives are to implement the constructs, to warn, or to refuse them. An implementation of WHERE is a larger feature with its own rollback questions. A warning in a log is easy to miss.

**Decision.** An open refuses each construct of IB-68 with INVALID_SCHEMA and names the construct. A later decision can add a construct, for example a WHERE filter, together with its semantics in DEF-30.

**Consequences.**

- A schema that opens means what it says (REQ-26).
- Schemas that used an ignored clause stop to open. Their output was wrong for the author's intent, so this is the purpose.
- The builders must not generate a refused construct.
- Shapes: REQ-26, IB-61, IB-68, GAP-2.

## 2. Check and convert value types at each boundary

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

## 3. No input terminates the host process

**Context.** Settle panics in some places where input decides the outcome. Examples are a Lua compile error or a failed module load at open, some non-ASCII schema texts, some builder arguments and some numeric limits. Nothing catches an unwind at the JS boundary, so a panic inside a native call can abort the Node process (GAP-4). A Rust closure that panics passes through an ingest call and leaves the instance state undefined.

**Decision.** Each boundary that reads input returns an error class in place of a panic. Settle compiles Lua bodies and modules during the schema validation, and returns INVALID_SCHEMA for an error. The native JS entry points catch an unwind and turn it into an error. A panic in a Rust closure fails the call with REDUCER_FAILED, and Settle undoes the call. Fuzzers guard the boundaries (CT-9).

**Consequences.**

- FM-1 and REQ-42 can hold.
- An internal invariant violation still stops the process. Such a stop is a defect, not an input error.
- Shapes: REQ-42, FM-1, FM-41, FM-47, WP-73, GAP-4.

## 4. Remove the inert buffer setting and gauges

**Context.** The configuration has a buffer size, and the bindings give a buffer count and a backpressure flag. Each ingest call and each fork call empties the buffer before it returns, so between calls the count is 0 and the flag is false. The setting has no effect (GAP-11). The real backpressure is the ack queue (ADR-1). A gauge that never moves tells a host that all is well.

The alternative is to give the setting a meaning, for example a bound on the records of one batch. That would split a batch, and a split batch breaks the rule of one record per key and the atomic apply of a call.

**Decision.** Settle removes the buffer setting, the buffer count and the backpressure flag from each binding. The places read (RP-34) is the backpressure signal.

**Consequences.**

- No inert gauge remains (INV-33, OB-16).
- Hosts that read the removed fields need a change. No released host depends on them for behavior.
- Shapes: INV-33, OB-16, IB-2, IB-21, P-BUFFER-SIZE, GAP-11.
