# ADR-5 — External reducers run once per block on the calling thread

Status: Accepted (historical)

## Context

A reducer body in host code, for example a JS function, crosses the language boundary. A JS object for each row and field made marshal more than 60% of the ingest time (3890a40). A JS function can run only on the thread of its runtime. An open happens before the host attaches the code, so the open cannot replay an external reducer. A review found that a late callback did not replay its upstream reducer.

## Decision

Settle calls an external reducer once for each block, with every group of the block, through typed arrays and one joined string in each direction. External reducers always run on the calling thread, so a pipeline with one runs its stages in sequence. An open skips external reducers. The attach of their code rolls their chain back to finality and replays it. An attach needs an empty ack queue, applies only to a declared external reducer, and only once.

## Consequences

- An external JS reducer costs about what an event-rules reducer costs.
- A pipeline with an external reducer cannot defer its checkpoints (ADR-2) and gets no parallel stages.
- A host must attach the code before the first ingest call that reaches it (WP-62).
- Shapes: REQ-23, WP-60, WP-62, CN-25, RS-16, DEF-28.
