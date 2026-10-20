# ADR-14 — Refuse schema constructs that Settle does not execute

Status: Proposed

This record was part 1 of the first draft of ADR-6, "Loud failure at every input boundary".

**Context.** The view parser reads only the first FROM item, the select list and the GROUP BY list. It ignores WHERE, HAVING, joins, ORDER BY, LIMIT and DISTINCT without an error.

It groups by the key items of the select list, not by GROUP BY. So a GROUP BY that differs from the key items has no effect. It accepts two objects with one name and a view with no aggregate. It also accepts numeric aggregates over a column with no numeric mode, and defaults that do not parse. A schema author then gets a pipeline that does something other than the text says (GAP-2).

The alternatives are to implement the constructs, to warn, or to refuse them. An implementation of WHERE is a larger feature with its own rollback questions. A warning in a log is easy to miss.

**Decision.** An open refuses each construct of IB-68 with INVALID_SCHEMA and names the construct. A later decision can add a construct, for example a WHERE filter, together with its semantics in DEF-30.

**Consequences.**

- A schema that opens means what it says (REQ-26).
- Schemas that used an ignored clause stop to open. Their output was wrong for the author's intent, so this is the purpose.
- The builders must not generate a refused construct.
- Shapes: REQ-26, IB-61, IB-68, GAP-2.
