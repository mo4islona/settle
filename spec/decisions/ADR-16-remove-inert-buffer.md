# ADR-16 — Remove the inert buffer setting and gauges

Status: Proposed

This record was part 4 of the first draft of ADR-6, "Loud failure at every input boundary".

**Context.** The configuration has a buffer size, and the bindings give a buffer count and a backpressure flag. Each ingest call and each fork call empties the buffer before it returns, so between calls the count is 0 and the flag is false. The setting has no effect (GAP-11). The real backpressure is the ack queue (ADR-1). A gauge that never moves tells a host that all is well.

The alternative is to give the setting a meaning, for example a bound on the records of one batch. That would split a batch, and a split batch breaks the rule of one record per key and the atomic apply of a call.

**Decision.** Settle removes the buffer setting, the buffer count and the backpressure flag from each binding. The places read (RP-34) is the backpressure signal.

**Consequences.**

- No inert gauge remains (INV-33, OB-16).
- Hosts that read the removed fields need a change. No released host depends on them for behavior.
- Shapes: INV-33, OB-16, IB-2, IB-21, P-BUFFER-SIZE, GAP-11.
