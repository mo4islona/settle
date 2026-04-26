export interface ColumnType<T = any> {
  readonly _sql: string
  /** Phantom field for type inference — not present at runtime. */
  readonly _type?: T
}

function col<T>(sql: string): () => ColumnType<T> {
  return () => ({ _sql: sql })
}

export const uint64 = col<number>('UInt64')
export const int64 = col<number>('Int64')
export const float64 = col<number>('Float64')
export const uint256 = col<bigint>('Uint256')
export const string = col<string>('String')
export const datetime = col<number>('DateTime')
export const boolean = col<boolean>('Boolean')
export const bytes = col<Uint8Array>('Bytes')
export const base58 = col<string>('Base58')
export function json<T = any>(): ColumnType<T> {
  return { _sql: 'Json' }
}

/** Infer the row type from a column definition record. */
export type InferRow<T extends Record<string, ColumnType>> = {
  [K in keyof T]: T[K] extends ColumnType<infer V> ? V : unknown
}
