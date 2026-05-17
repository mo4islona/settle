/**
 * Typed errors thrown by `Settle.ingest`, `Settle.ack`, `Settle.handleFork`,
 * and `Settle.registerReducer`.
 *
 * The Rust core surfaces these via structured prefixes in `Error.message`:
 *   `__SETTLE_PENDING_ACK__ sequence=<u64> since_ms=<u128>`
 *   `__SETTLE_WRONG_ACK_SEQUENCE__ expected=<u64> got=<u64>`
 *
 * `rethrowSettleError(caught)` parses those and rethrows as typed classes.
 * Anything else is rethrown unchanged.
 */

/** Thrown when a previously-returned `ChangeBatch` is still awaiting `ack()`. */
export class SettlePendingAckError extends Error {
  readonly sequence: number
  readonly sinceMs: number

  constructor(sequence: number, sinceMs: number) {
    super(`ack pending: sequence ${sequence}, pending for ${sinceMs}ms`)
    this.name = 'SettlePendingAckError'
    this.sequence = sequence
    this.sinceMs = sinceMs
    Object.setPrototypeOf(this, SettlePendingAckError.prototype)
  }
}

/** Thrown when `ack(seq)` is called with a sequence that doesn't match the pending slot. */
export class SettleWrongAckSequenceError extends Error {
  readonly expected: number
  readonly got: number

  constructor(expected: number, got: number) {
    super(`wrong ack sequence: expected ${expected}, got ${got}`)
    this.name = 'SettleWrongAckSequenceError'
    this.expected = expected
    this.got = got
    Object.setPrototypeOf(this, SettleWrongAckSequenceError.prototype)
  }
}

const PENDING_RE = /__SETTLE_PENDING_ACK__ sequence=(\d+) since_ms=(\d+)/
const WRONG_ACK_RE = /__SETTLE_WRONG_ACK_SEQUENCE__ expected=(\d+) got=(\d+)/

/**
 * If `caught` is one of the structured-prefix errors thrown by the native
 * layer, rethrow it as a typed class. Otherwise rethrow as-is. Always throws —
 * never returns. Marked `never` so callers can `return rethrowSettleError(e)`
 * inside `catch` without unreachable-code lints.
 */
export function rethrowSettleError(caught: unknown): never {
  const msg = (caught as { message?: string } | null | undefined)?.message
  if (typeof msg === 'string') {
    const pending = msg.match(PENDING_RE)
    if (pending) {
      throw new SettlePendingAckError(Number(pending[1]), Number(pending[2]))
    }
    const wrong = msg.match(WRONG_ACK_RE)
    if (wrong) {
      throw new SettleWrongAckSequenceError(Number(wrong[1]), Number(wrong[2]))
    }
  }
  throw caught
}
