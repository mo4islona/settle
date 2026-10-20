# ADR-10 — Remove the event-rules runtime

Status: Accepted (2026-10-19)

## Context

Settle runs reducer bodies in four runtimes: Lua, event rules, external host code and Rust closures. Event rules are a small declarative language in the schema, with WHEN, LET, SET and EMIT clauses. The first design made them the primary body language, with Lua as the escape hatch.

The facts at the time of this decision:

- No builder of the bindings and no code generator produces event rules. They produce only external reducers.
- Each event-rules construct has a Lua equivalent, and Lua has more. Event rules have no loops and emit at most one row for each input row. Their arithmetic is Float64 only, and a division by zero gives 0 without an error.
- The parser and the runtime of event rules have about 1,000 lines, more than the Lua runtime.
- The code and IB-65 disagree on the order of the LET, SET and EMIT clauses. The EMIT parser splits an item at its first `=`, also inside an `IF` condition.
- On native builds, event rules are a little faster than Lua. They take 0.71 against 1.39 µs/row at 100 groups, and 2.10 against 2.89 µs/row at 100K groups. These numbers come from a runtime spike on a loaded host. The same spike found that the body language is not the main cost of a pipeline.
- The browser build has no Lua. There, event rules are the only body language inside the schema. An external JS reducer there costs 10 µs/row at 100 groups and 458 µs/row at 100K groups. The maintainers do not need a fast reducer inside the schema in the browser (NG-9).

The alternatives:

- Keep event rules and repair them: exact integer arithmetic, a loud division by zero, the clause order and the EMIT parser. This keeps a second body language to specify, test and maintain.
- Keep event rules for the browser build only. This keeps the full cost for one build that has no performance target.
- Add Lua to the browser build. A later decision can do this. It does not need event rules.

## Decision

Settle removes the event-rules runtime and its grammar. A body inside the schema is Lua. A body in host code is an external reducer or a Rust closure. An open refuses a reducer with a WHEN clause with INVALID_SCHEMA, and the message names Lua as the replacement.

## Consequences

- The schema language has one body language, and OQ-2 closes.
- In the browser, each reducer is an external JS reducer. At many groups it costs much more for each row than event rules did. This stays so until the browser bridge moves a block in one call, as the Node bridge does (ADR-5).
- Schemas with event-rules bodies stop to open. No release stores data (ADR-3), and the stored layouts do not hold the body kind, so stored data needs no migration.
- The tests and benches that use event rules move to Lua. Lua 5.4 keeps integers, so some expected values change from Float64 to an integer type.
- The SLO row of the full pipeline with event rules goes away. The row of the full pipeline with Lua stays (SLI-1).
- Shapes: REQ-23, DEF-28, IB-63, IB-65, SLI-1, OQ-2.
