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
| [ADR-6](ADR-6-no-termination-by-input.md) | No input terminates the host process | Accepted (2026-10-20) | 2026-10-20 |
| [ADR-7](ADR-7-finality-across-reopen.md) | Persist the finality watermark next to the durable watermark | Accepted (2026-10-20) | 2026-10-20 |
| [ADR-8](ADR-8-memory-and-reducer-bounds.md) | Bounds on memory and on reducer execution | Proposed | — |
| [ADR-9](ADR-9-ratify-targets.md) | Ratify the service level targets and the gate thresholds | Proposed | — |
| [ADR-10](ADR-10-remove-event-rules.md) | Remove the event-rules runtime | Accepted (2026-10-19) | 2026-10-19 |
| [ADR-11](ADR-11-body-change-keeps-results.md) | A body change keeps the results of accepted rows | Proposed | — |
| [ADR-12](ADR-12-map-state-fields.md) | Map state fields | Accepted (2026-10-20) | 2026-10-20 |
| [ADR-13](ADR-13-no-common-ancestor-fails.md) | A fork with no common ancestor fails on both paths | Proposed | — |
| [ADR-14](ADR-14-refuse-unexecuted-constructs.md) | Refuse schema constructs that Settle does not execute | Accepted (2026-10-20) | 2026-10-20 |
| [ADR-15](ADR-15-exact-value-conversion.md) | Check and convert value types at each boundary | Proposed | — |
| [ADR-16](ADR-16-remove-inert-buffer.md) | Remove the inert buffer setting and gauges | Proposed | — |
| [ADR-17](ADR-17-external-reducers-defer.md) | External reducers defer their checkpoints | Proposed | — |
| [ADR-18](ADR-18-interval-counts-to-the-data.md) | The checkpoint interval counts to the latest block | Proposed | — |
| [ADR-19](ADR-19-checkpoint-leaves-out-covered-rows.md) | A checkpoint leaves out the raw rows that it covers | Proposed | — |
