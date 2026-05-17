/* tslint:disable */
/* eslint-disable */

/**
 * WASM binding for Settle.
 */
export class Settle {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Acknowledge the pending batch by sequence number and durably commit
     * its writes. `sequence` is passed as f64 (JS number); values up to 2^53
     * preserve exact precision. Throws typed errors via the structured-
     * reason prefix protocol.
     */
    ack(sequence: number): void;
    /**
     * Atomically handle a fork (409 from Portal).
     *
     * Finds the common ancestor in `previousBlocks`, rolls back all state after
     * that point, and returns `{ cursor, batch }`. Uses the internal finalized
     * block — no need to pass it in.
     *
     * Throws if no common ancestor is found (fork too deep / unrecoverable).
     */
    handleFork(previous_blocks: any): any;
    /**
     * Atomic ingest: process all tables, finalize, and return change batch.
     * Input and output are plain JS objects — no msgpack encoding needed.
     */
    ingest(input: any): any;
    /**
     * Create a new Settle with in-memory storage.
     */
    constructor(schema: string);
    /**
     * Register an external reducer with a JS batch callback.
     *
     * The callback receives an array of `{ state, rows }` groups and must
     * return an array of `{ state, emits }` results (same length, same order).
     *
     * Must be called before any `ingest` calls that use this reducer.
     */
    registerReducer(name: string, source: string, group_by: any, state: any, callback: Function): void;
    /**
     * Attach a JS callback to an existing reducer that was declared in
     * SQL with `LANGUAGE EXTERNAL`, and replay unfinalized blocks. Errors
     * if no such reducer exists OR if a callback is already registered
     * for that name.
     */
    registerReducerCallback(name: string, callback: Function): void;
    /**
     * Find the common ancestor between our state and the portal's chain.
     * Returns the matching block cursor, or null if no common ancestor found.
     */
    resolveForkCursor(previous_blocks: any): any;
    /**
     * Current cursor: latest processed block + hash. Null if no blocks processed.
     */
    readonly cursor: any;
    /**
     * Whether a previously-returned ChangeBatch is still awaiting `ack()`.
     */
    readonly isAwaitingAck: boolean;
    /**
     * Whether backpressure should be applied.
     */
    readonly isBackpressured: boolean;
    /**
     * Whether an unrecoverable commit failure has poisoned this instance.
     * Once true the only recovery is to drop the instance and reopen.
     */
    readonly isPoisoned: boolean;
    /**
     * Number of pending (unflushed) change records.
     */
    readonly pendingCount: number;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_settle_free: (a: number, b: number) => void;
    readonly settle_ack: (a: number, b: number) => [number, number];
    readonly settle_cursor: (a: number) => any;
    readonly settle_handleFork: (a: number, b: any) => [number, number, number];
    readonly settle_ingest: (a: number, b: any) => [number, number, number];
    readonly settle_isAwaitingAck: (a: number) => number;
    readonly settle_isBackpressured: (a: number) => number;
    readonly settle_isPoisoned: (a: number) => number;
    readonly settle_new: (a: number, b: number) => [number, number, number];
    readonly settle_pendingCount: (a: number) => number;
    readonly settle_registerReducer: (a: number, b: number, c: number, d: number, e: number, f: any, g: any, h: any) => [number, number];
    readonly settle_registerReducerCallback: (a: number, b: number, c: number, d: any) => [number, number];
    readonly settle_resolveForkCursor: (a: number, b: any) => [number, number, number];
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
