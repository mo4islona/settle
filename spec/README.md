# Settle specification

## What Settle is

Settle is an embedded computation engine for blockchain data. A host program gives it blocks of rows. Settle sends the rows through a pipeline of reducers and materialized views, and returns change batches that the host applies to a target store.

Settle keeps the target exact when the chain forks. It also keeps its own state across a restart, in step with the acknowledgements of the host.

## Purpose of this suite

This suite states what Settle MUST do, records why it does it that way, and gives the base for a test-first hardening program.

- **Tier:** conformance, the full depth. The suite is the regression baseline for a system that exists. So it includes the reference model, the test-class taxonomy, the traceability matrix and the gap register.
- **Shape:** a stateful embedded library that works as a data pipeline. Module 04 specifies the transitions and the checkpoints. Module 05 specifies the output contract (the change batches). Module 06 specifies consistency and durability. Module 10 specifies retention and replay.
- **Boundary:** the public surface of the library in Rust, in Node and in a browser, plus the schema language. Internal modules are out of scope. The block source and the target store are external actors.

The suite specifies the intended behavior. Where the current code does something else, the suite states the intent and registers the deviation as a `GAP-n` entry in [13-conformance-tdd.md](13-conformance-tdd.md).

## Document map

| Doc | Content | Normative? |
|---|---|---|
| [01-overview.md](01-overview.md) | Context, actors, goals, non-goals, trust model, lifecycle | Yes |
| [02-requirements.md](02-requirements.md) | Product requirements `REQ-n`, open questions `OQ-n` | Yes |
| [03-data-model.md](03-data-model.md) | Definitions `DEF-n`, the state tuple, the transition summary, the terminology table | Yes |
| [04-mutations.md](04-mutations.md) | Write rules `WP-n` and the transition catalog `TR-n` | Yes |
| [05-output-contract.md](05-output-contract.md) | Change batches, status reads `RP-n`, error classes `EC-n` | Yes |
| [06-consistency-durability.md](06-consistency-durability.md) | Commit model, durability tiers, recovery `CN-n` | Yes |
| [07-invariants.md](07-invariants.md) | Safety properties `INV-n` | Yes |
| [08-liveness.md](08-liveness.md) | Progress properties `LIV-n` | Yes |
| [09-failure-model.md](09-failure-model.md) | Faults and the required responses `FM-n` | Yes |
| [10-retention-replay.md](10-retention-replay.md) | Retention, purge, replay, space and memory `RS-n` | Yes |
| [11-performance.md](11-performance.md) | SLIs, SLOs, resource bounds, workload, hazards `PF-n`, `SLI-n`, `SC-n`, `HZ-n` | Yes |
| [12-observability.md](12-observability.md) | Required signals `OB-n` | Yes |
| [13-conformance-tdd.md](13-conformance-tdd.md) | Harness, reference model, test classes, matrix, gaps, merge gates `CT-n`, `FV-n`, `HC-n`, `MG-n`, `GAP-n`, `BO-n` | Mutable status doc; the reference model is normative |
| [14-interface-binding.md](14-interface-binding.md) | The concrete API, the schema grammar, the value and error mapping `IB-n` | Yes |
| [15-parameters.md](15-parameters.md) | The registry of every `P-*` symbol | Mutable status doc |
| [decisions/](decisions/README.md) | Architecture decision records `ADR-n` and their log | Append-only |
| [tools/check_spec.py](tools/check_spec.py) | The quality gate of this suite | Tool |

## Conventions

### Requirement keywords

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT and MAY have the meaning of RFC 2119, and only in uppercase. A lowercase "must", "should" or "may" has no normative force (RFC 8174), so this suite does not use them.

### IDs

Every requirement, definition, property, decision and test class has a stable ID. The suite never reuses or renumbers an ID. A retired ID stays in a "Closed" section. Each prefix has one home document, and the other documents refer to it by ID.

| Prefix | Meaning | Home | Bands |
|---|---|---|---|
| `G-n`, `NG-n` | Design goal, non-goal | 01 | — |
| `REQ-n` | Product requirement | 02 | 1–9 core flow · 10–19 delivery and durability · 20–29 schema and computation · 30–39 bindings · 40–49 quality |
| `OQ-n` | Open question | 02 | — |
| `DEF-n` | Definition | 03 | 1–19 chain and input · 20–39 pipeline · 40–59 derived values · 60–79 output and delivery · 80–99 instance state |
| `TR-n` | Transition | 04 | — |
| `WP-n` | Write rule | 04 | 1–9 calls and input · 10–19 forks · 20–29 finality · 30–39 commit and ack · 40–49 checkpoints · 50–59 open · 60–69 reducer attach · 70–79 errors |
| `RP-n` | Output rule | 05 | 1–9 batch content · 10–19 sequence and order · 20–29 target duties · 30–39 status reads |
| `EC-n` | Error class | 05 | — |
| `CN-n` | Consistency or durability rule | 06 | 1–9 commit model · 10–19 durability · 20–29 recovery · 30–39 format · 40–49 interference |
| `INV-n` | Invariant | 07 | 1–9 structural · 10–19 transition legality · 20–29 output semantics · 30–34 reports · 35–39 isolation · 40–49 durability and recovery |
| `LIV-n` | Liveness property | 08 | 1–9 progress · 10–19 recovery and release |
| `FM-n` | Fault and response | 09 | 1–9 global · 10–29 input · 30–39 storage · 40–49 reducer runtime · 50–59 process · 60–69 host and target · 70–79 operator |
| `RS-n` | Retention or replay rule | 10 | 1–9 raw rows · 10–19 derived state · 20–29 space and memory |
| `PF-n`, `SLI-n`, `SC-n`, `HZ-n` | Resource bound, indicator, scenario, hazard | 11 | — |
| `OB-n` | Observable signal | 12 | — |
| `CT-n`, `FV-n`, `HC-n`, `MG-n`, `GAP-n`, `BO-n` | Test class, free variable, harness capability, merge gate, gap, build-order phase | 13 | — |
| `IB-n` | Binding rule | 14 | 1–19 Rust · 20–29 Node · 30–39 browser · 40–49 values · 50–59 errors · 60–79 schema language |
| `P-*` | Parameter | 15 | — |
| `ADR-n` | Decision | [decisions/](decisions/README.md) | sequential, no bands |

A band leaves room for new IDs, so a hole inside a band is normal.

### Notation

- ℕ is the set of natural numbers, with 0.
- ⊥ means "no value". For example, the latest block of an empty database is ⊥.
- `⟨a, b, c⟩` is a sequence, and `|s|` is its length.
- `A↾b` is the accepted chain `A` cut after block `b`: the blocks of `A` with a number at or below `b`.
- `min` and `max` over an empty set give ⊥.

### Parameters

Every constant appears in normative text only as a `P-*` symbol. The values live in [15-parameters.md](15-parameters.md), with an "observed" and a "target" column. A ⚠ marks a proposed value that waits for an ADR.

### Status markers

- **C / P / U** in the matrices mean covered, partial and unchecked.
- **known-violated** marks a property that the current code breaks. A `GAP-n` entry names the deviation.
- **known-suspect** marks a property that the code probably breaks, without a failing test yet.

### Language

All prose in this suite follows ASD-STE100 Simplified Technical English. Code, formulas, IDs, `P-*` symbols, pseudocode and the concrete surface in [14-interface-binding.md](14-interface-binding.md) are not prose.

The suite uses these technical words outside the STE dictionary:

- The `DEF` terms of [03-data-model.md](03-data-model.md) and the rows of its terminology table. The suite uses one term for each concept.
- The domain verbs: ingest, emit, ack, commit, persist, replay, roll back, finalize, purge, expire, journal, poison, sync, checkpoint, reopen, attach, fold.
- The technical names of the domain: backfill, tip (the newest end of the chain), heartbeat call, change batch and change record.
- The technical names of the pipeline: materialized view, sliding window, tumbling window, reducer, host and target.

### Mutable documents

Only two documents carry dates, statuses and current values: [13-conformance-tdd.md](13-conformance-tdd.md) and [15-parameters.md](15-parameters.md). The [decisions/](decisions/README.md) folder only gains files. An accepted ADR changes only when a later ADR supersedes it. The other documents change only when the intended behavior changes.

## How to use this suite

1. Read [01-overview.md](01-overview.md), then the definitions in [03-data-model.md](03-data-model.md).
2. Ratify or reject each `Proposed` ADR in [decisions/](decisions/README.md).
3. Build the harness capabilities that [13-conformance-tdd.md](13-conformance-tdd.md) marks `U`, in the build order BO-1 to BO-6.
4. Make the reference model executable and use it as the test oracle.
5. Close the gaps in priority order, each with its failing test first.
6. Update the matrix and the gap register in the same change as the code.

## Quality gate

Run the checker from the repository root:

```sh
python3 spec/tools/check_spec.py
```

The checker validates references, coverage, the decision log and the STE rules that a script can see. `python3 spec/tools/check_spec.py --list-checks` lists every check. A change that leaves an error-severity finding does not merge (`MG-9` in [13-conformance-tdd.md](13-conformance-tdd.md)).
