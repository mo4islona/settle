# ADR-9 — Ratify the service level targets and the gate thresholds

Status: Proposed

## Context

The suite needs targets for the service level indicators of 11-performance.md and thresholds for the merge gates of 13-conformance-tdd.md. Some targets exist as floors in the throughput bench. The others are new proposals with a ⚠ mark in 15-parameters.md.

## Decision

The maintainers accept or change each ⚠ target of 15-parameters.md. These are the targets:

- P-SLO-DEFER-GAIN, P-SLO-TIP-LATENCY, P-SLO-ACK-SYNC, P-SLO-OPEN, P-SLO-RSS-PER-GROUP, P-SLO-PG-RATIO and P-SLO-REORG-RATIO.
- P-DISK-AMP, P-DISK-CONST and P-MAINT-CPU.
- P-GATE-PROPERTY-COV, P-GATE-DIFF-COV, P-GATE-TOTAL-COV, P-GATE-WALL-NOISE, P-GATE-LINT-WARNINGS and P-GATE-PR-BUDGET.
- P-QUARANTINE-DAYS, P-ORACLE-SEEDS and P-FUZZ-BUDGET.

When this ADR is accepted, the registry drops the ⚠ marks of the accepted values.

## Consequences

- REQ-40 and the gates MG-3 to MG-11 get fixed bars.
- A later change of a target needs a new ADR.
- Shapes: REQ-40, SLI-1, SLI-8, MG-3, MG-4, MG-7, MG-8, MG-10, MG-11, MG-12.
