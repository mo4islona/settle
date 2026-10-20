export interface ColumnType<T = any> {
  readonly _sql: string
  /** Phantom field for type inference — not present at runtime. */
  readonly _type?: T
}

function col<T>(sql: string): () => ColumnType<T> {
  return () => ({ _sql: sql })
}

/**
 * A value of a 64-bit integer column. The engine gives a number while the
 * value fits in 2^53, and a bigint above, so that no digit is lost.
 */
export type Int64Value = number | bigint

export const uint64 = col<Int64Value>('UInt64')
export const int64 = col<Int64Value>('Int64')
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

/**
 * A record as the engine gives it back. A `bigint` field holds a 64-bit
 * integer, so it comes back as a number while its value fits in 2^53.
 */
export type FromEngine<T> = { [K in keyof T]: WidenBigint<T[K]> }

export type WidenBigint<V> = V extends bigint ? Int64Value : V

/** Infer the row type from a column definition record. */
export type InferRow<T extends Record<string, ColumnType>> = {
  [K in keyof T]: T[K] extends ColumnType<infer V> ? V : unknown
}
