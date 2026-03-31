/* tslint:disable */
/* eslint-disable */

/**
 * WASM binding for DeltaDb.
 */
export class DeltaDb {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Acknowledge a flushed batch by sequence number.
     */
    ack(sequence: number): void;
    /**
     * Flush buffered deltas. Returns a delta batch object, or null if empty.
     */
    flush(): any;
    /**
     * Atomically handle a fork (409 from Portal).
     *
     * Finds the common ancestor in `previousBlocks`, rolls back all state after
     * that point, and returns `{ cursor, batch }`. Uses the internal finalized
     * block — no need to pass it in.
     *
     * Throws if no common ancestor is found (fork too deep / unrecoverable).
     */
    handle_fork(previous_blocks: any): any;
    /**
     * Atomic ingest: process all tables, finalize, and return delta batch.
     * Input and output are plain JS objects — no msgpack encoding needed.
     */
    ingest(input: any): any;
    /**
     * Create a new DeltaDb with in-memory storage.
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
    register_reducer(name: string, source: string, group_by: any, state: any, callback: Function): void;
    /**
     * Find the common ancestor between our state and the portal's chain.
     * Returns the matching block cursor, or null if no common ancestor found.
     */
    resolve_fork_cursor(previous_blocks: any): any;
    /**
     * Current cursor: latest processed block + hash. Null if no blocks processed.
     */
    readonly cursor: any;
    /**
     * Whether backpressure should be applied.
     */
    readonly isBackpressured: boolean;
    /**
     * Number of pending (unflushed) delta records.
     */
    readonly pendingCount: number;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_deltadb_free: (a: number, b: number) => void;
    readonly deltadb_ack: (a: number, b: number) => void;
    readonly deltadb_cursor: (a: number) => any;
    readonly deltadb_flush: (a: number) => [number, number, number];
    readonly deltadb_handle_fork: (a: number, b: any) => [number, number, number];
    readonly deltadb_ingest: (a: number, b: any) => [number, number, number];
    readonly deltadb_isBackpressured: (a: number) => number;
    readonly deltadb_new: (a: number, b: number) => [number, number, number];
    readonly deltadb_pendingCount: (a: number) => number;
    readonly deltadb_register_reducer: (a: number, b: number, c: number, d: number, e: number, f: any, g: any, h: any) => [number, number];
    readonly deltadb_resolve_fork_cursor: (a: number, b: any) => [number, number, number];
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
