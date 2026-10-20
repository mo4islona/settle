import type { WidenBigint } from './column'
import type { StateMap } from './ddl'

/**
 * The engine functions that reach the Map fields of the groups of the block
 * in a reducer callback (`mapGet` and the others in `src/napi.rs` and
 * `src/wasm.rs`). `group` and `map` are indices in the groups and the Map
 * fields that the callback got.
 */
export interface MapAccess {
  mapGet(group: number, map: number, key: string): unknown
  mapSet(group: number, map: number, key: string, value: unknown): void
  mapDelete(group: number, map: number, key: string): boolean
}

/**
 * Each group runs its rows in an epoch of its own. A handle works only in
 * the epoch that made it: else it would reach the group at its index in a
 * later block, or in the block of another reducer.
 */
let currentEpoch = 0

/** Start the rows of the next group. */
export function nextEpoch(): void {
  currentEpoch++
}

/** A Map state field of the group whose rows run. */
export class StateMapHandle<V = unknown> implements StateMap<V> {
  private readonly epoch = currentEpoch

  constructor(
    private readonly access: MapAccess,
    private readonly group: number,
    private readonly map: number,
    private readonly name: string,
  ) {}

  get(key: string): WidenBigint<V> | undefined {
    this.check()
    return this.access.mapGet(this.group, this.map, key) as WidenBigint<V> | undefined
  }

  set(key: string, value: V | null | undefined): void {
    this.check()
    this.access.mapSet(this.group, this.map, key, value)
  }

  delete(key: string): boolean {
    this.check()
    return this.access.mapDelete(this.group, this.map, key)
  }

  private check(): void {
    if (this.epoch !== currentEpoch) {
      throw new Error(`Map field '${this.name}' is used outside the rows of its group`)
    }
  }
}
