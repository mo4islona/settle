# 12 — Observability

Settle is a library, so its signals are status reads, fields of a batch, error details and log events. The host exports them to its own monitor. The status of each signal is in [13-conformance-tdd.md](13-conformance-tdd.md).

## Required signals

**OB-1 — Latest cursor.** Each binding gives the latest cursor (RP-31).

**OB-2 — Finalized cursor.** Each binding gives the finalized cursor (RP-32). Today the JS bindings show it only in the heads of a batch (GAP-15).

**OB-3 — Durable block.** Each binding gives `D` (RP-33). Today only the Rust API gives it (GAP-15).

**OB-4 — Places.** Each binding gives the places in use and whether a batch waits for an ack (RP-34).

**OB-5 — Health.** Each binding gives the health (RP-35) and the reason of the first failure. Today the reason is only in the text of the POISONED error, and no read tells sync-failed from poisoned (GAP-15).

**OB-6 — Last acked sequence.** Each binding gives the sequence of the last committed batch. Today no binding gives it (GAP-15).

**OB-7 — Performance tree.** Each batch carries the durations of the stages that produced it: the pipeline, each raw table, each reducer, each view and each parallel section (RP-8).

**OB-8 — Ack-wait event.** When a call fails with PENDING_ACK, and the oldest unacked batch is older than P-ACK-WARN, Settle emits a warning log event. The event gives the oldest sequence and its age.

**OB-9 — Error details.** PENDING_ACK carries the oldest unacked sequence and its age. WRONG_ACK_SEQUENCE carries the expected and the given sequence. Each other error carries its class and a text.

**OB-10 — Memory gauges.** Settle gives, for each reducer and each view, the resident groups, the unfinal blocks and the size of the expired-block stash. Today no gauge exists (GAP-43).

**OB-11 — Open phases.** An open reports each phase. The phases are the format check, the journal reconcile with its committed and dropped entries, and the replay with its blocks, rows and duration. Today no report exists (GAP-43).

**OB-12 — Store gauges.** Settle gives the range of the stored raw blocks, the number of journal entries and the size of the store. Today no gauge exists (GAP-43).

**OB-13 — Poison event.** When an instance becomes poisoned or sync-failed, Settle emits an error log event with the reason.

**OB-14 — Idle against stalled.** With OB-1 and OB-4, the host can tell an idle source from a stalled target. An idle source keeps the latest cursor and has no places in use. A stalled target keeps every place in use and gets PENDING_ACK.

**OB-15 — Bounded cardinality.** The names in log events and in the performance tree come only from the schema object names. No signal has a label for each row, block or group.

**OB-16 — No inert gauge.** A binding gives no gauge that does not track a real quantity (INV-33). Today the buffer gauges always report an empty buffer (GAP-11).

## Property to observable

| Property | Signals that decide it |
|---|---|
| LIV-1 | OB-7 |
| LIV-2 | OB-9 |
| LIV-3 | OB-11 |
| LIV-4 | OB-4 |
| LIV-5 | OB-1, OB-14 |
| LIV-6 | OB-10 |
| LIV-7 | OB-12 |
| LIV-8 | OB-10 |
| LIV-9 | OB-12 |
| LIV-10 | OB-9 |
| LIV-11 | OB-5, OB-13 |
| LIV-12 | The return of the second open |
| LIV-13 | OB-7 |
| LIV-14 | OB-9, OB-13 |
| LIV-15 | OB-8 |

## The harness rule

A signal that does not agree with the state is a failure, as a wrong batch is. At each observation point, the harness compares each signal with the reference model. A mismatch fails the test.
