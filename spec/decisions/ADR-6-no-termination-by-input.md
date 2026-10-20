# ADR-6 — No input terminates the host process

Status: Accepted (2026-10-20)

The first draft of this record held four proposals. This part is accepted. Parts 1, 2 and 4 are now ADR-14, ADR-15 and ADR-16.

**Context.** Settle panics in some places where input decides the outcome. Examples are a Lua compile error or a failed module load at open, some non-ASCII schema texts, some builder arguments and some numeric limits. Nothing catches an unwind at the JS boundary, so a panic inside a native call can abort the Node process. A Rust closure that panics passes through an ingest call and leaves the instance state undefined.

**Decision.** Each boundary that reads input returns an error class in place of a panic. An open compiles each Lua body and module, and returns INVALID_SCHEMA for an error. A builder keeps the error of a bad argument, and its build fails with it.

The native JS entry points catch an unwind and turn it into an error. The state after such a panic is not known, so the panic also poisons the instance. A panic in a Rust closure fails the call with REDUCER_FAILED, and Settle undoes the call. Fuzzers guard the boundaries (CT-9).

**Consequences.**

- FM-1 and REQ-42 can hold.
- An internal invariant violation still stops the process. Such a stop is a defect, not an input error.
- In the browser a panic traps, and the call throws. It does not stop the page, so the browser binding needs no guard.
- Shapes: REQ-42, FM-1, FM-41, FM-47, WP-73.
