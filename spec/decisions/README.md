# Decision log

Each record has one file, `ADR-<n>-<slug>.md`, numbered in sequence. A record holds one decision, or a group of related decisions in numbered parts. All parts of a record have the status of the record. The log lists the records in number order. Proposed records wait for ratification.

The files only gain new records. An accepted record changes only when a later record supersedes it, and then only its status line changes. A proposed record can still change. The maintainers can accept it without some of its parts. Each part that they remove then goes into a new record of its own.

| ADR | Title | Status | Date |
|---|---|---|---|
| [ADR-1](ADR-1-host-protocol-and-crash-safety.md) | The host protocol and crash safety | Accepted (historical) | — |
| [ADR-2](ADR-2-finality-and-durable-watermark.md) | Finality and the durable watermark | Accepted (historical) | — |
| [ADR-3](ADR-3-storage-format-and-disk-bound.md) | The storage format and the disk bound | Accepted (historical) | — |
| [ADR-4](ADR-4-exact-aggregates-and-view-records.md) | Exact aggregates, windows and view records | Accepted (historical) | — |
| [ADR-5](ADR-5-external-reducers.md) | External reducers run once per block on the calling thread | Accepted (historical) | — |
| [ADR-6](ADR-6-loud-failure-at-input-boundaries.md) | Loud failure at every input boundary | Proposed | — |
| [ADR-7](ADR-7-finality-across-reopen.md) | Persist the finality watermark next to the durable watermark | Accepted (2026-10-20) | 2026-10-20 |
| [ADR-8](ADR-8-memory-and-reducer-bounds.md) | Bounds on memory and on reducer execution | Proposed | — |
| [ADR-9](ADR-9-ratify-targets.md) | Ratify the service level targets and the gate thresholds | Proposed | — |
| [ADR-10](ADR-10-remove-event-rules.md) | Remove the event-rules runtime | Accepted (2026-10-19) | 2026-10-19 |
| [ADR-11](ADR-11-body-change-keeps-results.md) | A body change keeps the results of accepted rows | Proposed | — |
| [ADR-12](ADR-12-map-state-fields.md) | Map state fields | Accepted (2026-10-20) | 2026-10-20 |
| [ADR-13](ADR-13-no-common-ancestor-fails.md) | A fork with no common ancestor fails on both paths | Proposed | — |
