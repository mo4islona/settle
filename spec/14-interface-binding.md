# 14 — Interface binding

This document maps the abstract operations, values and error classes of the suite to the concrete surface of each binding and to the schema language. It names the surface only. It does not name internals. Where the surface falls short of the intent, the entry cites its `GAP-n`.

Bands: 1–19 Rust · 20–29 Node · 30–39 browser · 40–49 values · 50–59 errors · 60–79 schema language.

## General rules

- One crate, `settle`, gives the Rust API. One package, `@settle/stream`, gives the Node entry and, as `@settle/stream/web`, the browser entry.
- The three entries give the semantics of [04-mutations.md](04-mutations.md) and [05-output-contract.md](05-output-contract.md). A difference between them is a defect (G-6).
- Each call is synchronous. The Node `ingest` returns a promise, but it does its work before it returns.
- IB-71 and IB-72 state the rules for changes to this document.

## Rust

**IB-1 — Operations.** The type `Settle` gives these calls, with the shapes of IB-3 and IB-4:

| Operation | Call |
|---|---|
| Open (TR-4) | `Settle::open(Config) -> Result<Settle>` |
| Ingest (TR-1) | `ingest(IngestInput) -> Result<Option<ChangeBatch>>` and `ingest_rows(IngestRows) -> Result<Option<ChangeBatch>>` |
| Fork (TR-2) | `handle_fork(Vec<BlockCursor>) -> Result<ForkResult>` |
| Ack (TR-3) | `ack(u64) -> Result<()>` |
| Attach (TR-5) | `register_reducer_callback(&str, Box<dyn ReducerRuntime>)`, `register_reducer(ReducerDef)`, `replay_reducer(&str)`. A Rust closure follows IB-5. |
| Latest cursor | `latest_cursor() -> Option<BlockCursor>`, `latest_block()` |
| Finalized cursor | `finalized_cursor()`, `finalized_block()` |
| Durable block | `durable_block()` |
| Places | `unacked_batches()`, `is_awaiting_ack()` |
| Health | `is_poisoned()` |
| Object reads | `has_reducer(&str)`, `reducer_is_external(&str)`, `table_registry(&str)` |
| Fork helper | `resolve_fork_cursor(&[(BlockNumber, &str)]) -> Option<BlockCursor>`: a read-only search for the highest supplied block whose hash matches. It is not bound to `[F, L]`. |
| Inert gauges | `pending_count()`, `is_backpressured()` (GAP-11) |

The drop of a `Settle` value closes the instance. The unacked batches of a dropped instance are lost outside journal mode. The crate also makes its engine module public, which this suite does not specify (GAP-39).

**IB-2 — Configuration.** `Config` is not exhaustive, so a host builds it with `Config::new(schema)` or `Config::with_data_dir(schema, dir)` and builder methods:

| Field | Default | Meaning |
|---|---|---|
| `schema` | — | The schema text (IB-60). |
| `data_dir` | none: memory | The directory of the persistent backend. The builder `data_dir(":memory:")` selects memory. |
| `storage` | none | An explicit backend. It comes before `data_dir` (WP-55). |
| `max_pending_batches` | P-MAX-UNACKED | The places of the ack queue. A value below 1 counts as 1. |
| `applied_sequence` | none | The applied sequence. A value turns journal mode on. |
| `backfill_checkpoint_interval` | P-CHECKPOINT-INTERVAL | The deferral interval. A value below 1 counts as 1. |
| `raw_retention_blocks` | P-RAW-RETENTION | The retention margin. |
| `max_resident_groups` | P-RESIDENT-GROUPS | The eviction bound of RS-24. `usize::MAX` keeps every group in memory. |
| `ack_warning_threshold` | P-ACK-WARN | The age for the event of OB-8. |
| `collect_perf` | true | Whether batches carry the performance tree. |
| `compression` | `snappy` | One of `none`, `snappy`, `zstd`, `lz4`. Another value fails with INVALID_CONFIG. |
| `cache_size` | backend default | The block cache in bytes. 0 turns it off. |
| `max_buffer_size` | P-BUFFER-SIZE | No effect (GAP-11). |

**IB-3 — Input.** `IngestInput { data: map table → rows by name, rollback_chain: Vec<BlockCursor>, finalized_head: BlockCursor }`. Each row by name carries `block_number` as UInt64. `IngestRows` carries positional rows, built on `table_registry(table)`, by table and block. `BlockCursor { number, hash }`.

**IB-4 — Output.** `ChangeBatch { sequence, finalized_head: Option<BlockCursor>, latest_head: Option<BlockCursor>, tables: map table → Vec<ChangeRecord>, perf }`. A `ChangeRecord` gives `operation` and the accessors `key`, `values()`, `prev_values()`, `get(column)` and `prev(column)`. `ForkResult { cursor: BlockCursor, batch: Option<ChangeBatch> }`.

**IB-5 — Rust closures.** A Rust closure gets a state context for each row. The context reads the state with `get`, `get_u64`, `get_i64`, `get_f64`, `get_bool` and `get_str`. It replaces the state with `update`, which sets an absent field to Null. It sets one field with `set`, and emits rows with `emit` and `emit_values`.

## Node

**IB-20 — Package.** `@settle/stream` ships prebuilt native modules for macOS on x64 and arm64, and for Linux with glibc on x64 and arm64. The release build of the package is broken today (GAP-24).

**IB-21 — Operations.** The class `Settle`, with the shapes of IB-24 and IB-25:

| Operation | Call |
|---|---|
| Open | `Settle.open(config): Settle` |
| Ingest | `ingest(input): Promise<ChangeBatch \| null>` |
| Fork | `handleFork(previousBlocks: SettleCursor[]): { cursor, batch: ChangeBatch \| null }` |
| Ack | `ack(sequence: number): void`. A value that is not a non-negative safe integer throws a `RangeError`. |
| Attach | `registerReducerCallback(name, reduce)` for a declared external reducer, and `registerReducer(options)` for a new one. |
| Latest cursor | `cursor: SettleCursor \| null` |
| Places | `unackedBatches`, `isAwaitingAck` |
| Health | `isPoisoned` |
| Fork helper | `resolveForkCursor(previousBlocks)` |
| Inert gauges | `pendingCount`, `isBackpressured` (GAP-11) |

No call gives the finalized cursor, the durable block or the last acked sequence (GAP-15), and no call closes the instance (GAP-13).

**IB-22 — Configuration.** `SettleConfig { schema, dataDir?, maxBufferSize?, maxPendingBatches?, compression?, cacheSize?, appliedSequence?, backfillCheckpointInterval?, maxResidentGroups? }`. A negative `cacheSize` or `appliedSequence` fails with INVALID_CONFIG. `backfillCheckpointInterval` is P-CHECKPOINT-INTERVAL, and `maxResidentGroups` is P-RESIDENT-GROUPS. The retention margin, the warning age and the performance switch are absent (GAP-38).

**IB-23 — Memory mode.** With no `dataDir`, the instance is in memory. A `dataDir` of `":memory:"` also selects memory. Today `Settle.open` creates a directory with that name, and only the pipeline builder maps it (GAP-12).

**IB-24 — Input.** `IngestInput { data: Record<table, Record<column, value>[]>, rollbackChain?: SettleCursor[], finalizedHead: SettleCursor }`. Each row carries `block_number`. `SettleCursor { number, hash }`.

**IB-25 — Output.** `ChangeBatch { sequence, finalizedHead, latestHead, tables: Record<table, ChangeRecord[]>, perf }`. `ChangeRecord { table, operation: 'insert' | 'update' | 'delete', key, values, prevValues }`. `perf` is a list of `{ kind, name, durationMs, children }` with the kinds `pipeline`, `raw_table`, `reducer`, `mv` and `parallel`.

**IB-26 — JS reducers.** A JS reducer is a function `reduce(ctx, row)`. `ctx` gives the state fields of the group, `ctx.update(next)`, which replaces the whole state and sets an absent field to Null, and `ctx.emit(row)`. Settle calls the binding once for each block, and the binding calls `reduce` for each row.

A Map field is `ctx.<field>`, with the methods `get(key)`, `set(key, value)` and `delete(key)` of IB-64. `get` returns a copy or `undefined`, and a `set` of `null` or `undefined` deletes the entry. `update` does not change a Map field. A Map field works only while the rows of its group run, and a call at another time throws.

**IB-27 — Builder.** `Pipeline().table(...).createReducer(...).createView(...).build(options)` generates the schema text, opens the instance and attaches each reducer. Its options are the fields of IB-22.

## Browser

**IB-30 — Entry.** `@settle/stream/web`. The host MUST await `init(wasmUrl?)` before the first call. `Settle.open(config)` opens a memory instance (NG-9). It throws when the config sets `dataDir`, `compression`, `cacheSize` or `maxBufferSize`. It ignores `appliedSequence` and `backfillCheckpointInterval` (GAP-38), and `maxResidentGroups`, which bounds nothing in memory (RS-20). The packed package does not ship the module of this entry today (GAP-23).

**IB-31 — Operations.** The operations of IB-21, with the same names. The ack throws for a value that is not finite, negative, fractional or above P-JS-SAFE-INT.

**IB-32 — JS reducers.** The JS reducer contract of IB-26.

## Values

**IB-40 — Ingest values, Node.** The Node binding maps each JS value to a value of DEF-4:

| JS value | Value |
|---|---|
| A safe integer `number`, 0 or more | UInt64 |
| A safe integer `number` below 0 | Int64 |
| Another `number`: a fraction, an unsafe integer, NaN or an infinity | Float64 |
| `bigint` from 0 to 2^64 − 1 | UInt64 |
| `bigint` from −2^63 to −1 | Int64 |
| `bigint` from 2^64 to 2^256 − 1 | Uint256 |
| Another `bigint` | String with the decimal digits |
| `string` | String. A lone surrogate becomes U+FFFD. |
| `boolean` | Boolean |
| `null`, `undefined`, a function, a symbol | Null |
| A typed array or a `DataView` | Bytes |
| Another object or array | String with the JSON text, also for a JSON column. The depth is at most P-MAX-JSON-DEPTH. |

By REQ-27, Settle then converts each value to the declared type of its column, or refuses it. Today no conversion occurs (GAP-1).

**IB-41 — Block numbers.** Each binding carries a block number without loss up to P-JS-SAFE-INT, in a row and in a cursor. Today the Node and browser cursors cut a number to 32 bits (GAP-9).

**IB-42 — Output values, Node.**

| Value | JS value |
|---|---|
| Float64 | `number` |
| UInt64, Int64, DateTime with a magnitude up to P-JS-SAFE-INT | `number` |
| UInt64, Int64, DateTime above that | `bigint` |
| Uint256 | `string`: `0x` and 64 hex digits |
| Base58 | `string` of hex digits (GAP-28) |
| Bytes | `Uint8Array` |
| JSON | The JS value, with an integer above P-JS-SAFE-INT as `bigint` |
| Null | `null` |

**IB-43 — Reducer callback values, Node.** A JS reducer gets Bytes as hex text, an absent row field as `undefined` and a Null state field as `null`. Settle reads each number that the reducer returns as Float64, and each object as JSON. By ADR-15, Settle converts each returned value to the declared type, or fails with REDUCER_FAILED (GAP-1).

**IB-44 — Wide values.** Uint256 and Base58 values leave the JS bindings as text. The TS types MUST describe the runtime form. Today the TS type of a Uint256 column says `bigint` (GAP-28).

**IB-48 — Browser values.** The browser binding maps values as IB-40 and IB-42 do. Today it gives `null` for NaN and the infinities, and an array of numbers for Bytes (GAP-29).

## Errors

**IB-50 — Error mapping.**

| Class | Rust variant | Node and browser |
|---|---|---|
| EC-1 PENDING_ACK | `PendingAck { sequence, since }` | `SettlePendingAckError { sequence, sinceMs }` |
| EC-2 WRONG_ACK_SEQUENCE | `WrongAckSequence { expected, got }` | `SettleWrongAckSequenceError { expected, got }` |
| EC-3 INVALID_INPUT | `InvalidOperation` | `Error`; a decode failure in the binding has the status `InvalidArg` |
| EC-4 FORK_BELOW_FINALITY | `InvalidOperation` | `Error` |
| EC-5 NO_COMMON_ANCESTOR | `InvalidOperation` | `Error` |
| EC-6 INVALID_SCHEMA | `Schema` | `Error` |
| EC-7 INCOMPATIBLE_STORAGE | `IncompatibleStorage` | `Error` |
| EC-8 TARGET_MISMATCH | `InvalidOperation` | `Error` |
| EC-9 INVALID_OPERATION | `InvalidOperation` | `Error`; the binding checks of attach have the status `InvalidArg` |
| EC-10 REDUCER_FAILED | `Reducer` | `Error`, or the error that the JS reducer threw |
| EC-11 STORAGE_FAILED | `Storage`, `Rollback`, `Serialization`, `Deserialization` | `Error` |
| EC-12 SYNC_FAILED | `SyncFailed` | `Error` |
| EC-13 POISONED | `Poisoned` | `Error` |
| EC-14 INVALID_CONFIG | `InvalidOperation`, `Storage` | `Error` with the status `InvalidArg` |

By REQ-34, each class needs its own type or a class field in each binding. Today the classes from EC-3 to EC-14 differ only in their text, and some share one Rust variant (GAP-14).

**IB-51 — Typed errors on the wire.** The native Node and browser modules give PENDING_ACK and WRONG_ACK_SEQUENCE as messages that start with `__SETTLE_PENDING_ACK__ sequence=N since_ms=M` and `__SETTLE_WRONG_ACK_SEQUENCE__ expected=E got=G`. The TS layer turns them into the two error classes of IB-50.

## Schema language

**IB-60 — Statements.** A schema is a list of these statements, each ended by `;`:

- `CREATE TABLE name (column Type, ...)` and `CREATE VIRTUAL TABLE name (...)`.
- `CREATE MODULE name LANGUAGE lua AS $$ ... $$`.
- `CREATE REDUCER name ...` (IB-63).
- `CREATE MATERIALIZED VIEW name AS SELECT ... FROM source GROUP BY ... [WINDOW SLIDING ...]` (IB-66).

Each other statement fails with INVALID_SCHEMA. This example is valid:

```sql
CREATE VIRTUAL TABLE orders (
    block_number UInt64, trader String, asset_id String, usdc UInt64, side UInt64
);

CREATE REDUCER market_stats
SOURCE orders
GROUP BY asset_id
STATE (volume Float64 DEFAULT 0, trades UInt64 DEFAULT 0)
LANGUAGE lua
PROCESS $$
    local vol = row.usdc / 1000000
    state.volume = state.volume + vol
    state.trades = state.trades + 1
    emit({ asset_id = row.asset_id, volume = vol })
$$;

CREATE MATERIALIZED VIEW token_summary AS
SELECT asset_id, sum(volume) AS total_volume, count() AS trade_count
FROM market_stats
GROUP BY asset_id;
```

**IB-61 — Validation.** An open fails with INVALID_SCHEMA when any of these conditions is true:

- A source of a reducer or a view does not exist, or a view reads a view.
- A reducer reads itself, or the reducers form a cycle.
- A required module does not exist, or two modules have one name.
- A SET names an undeclared state field.
- A group-by column of a reducer over a raw table is not a column of the table.
- A column of a view over a raw table is not a column of the table.
- The time column of a sliding view over a raw table is not DateTime, UInt64 or Int64.
- A limit of IB-67 is exceeded.

By ADR-14, an open also fails for each construct of IB-68. A CREATE TABLE statement still accepts column options and constraints that Settle ignores (GAP-49).

**IB-62 — Column types.**

| Schema name | Type |
|---|---|
| `UInt64` | UInt64 |
| `Int64`, `BIGINT` | Int64 |
| `Float64`, `DOUBLE` | Float64 |
| `UInt256` | Uint256 |
| `String`, `VARCHAR`, `TEXT` | String |
| `DateTime`, `TIMESTAMP` | DateTime |
| `Boolean` | Boolean |
| `BYTEA`, `VARBINARY`, `BLOB`, `Bytes` | Bytes |
| `Base58` | Base58 |
| `JSON`, `JSONB` | JSON |

The names are not case-sensitive. An unknown name fails with INVALID_SCHEMA. A state field uses the same names. In a CREATE TABLE statement, `Bytes`, `JSON` and `JSONB` probably fail today, because the SQL grammar reads them as its own types (GAP-44).

**IB-63 — Reducers.**

```
CREATE REDUCER name
SOURCE source
[GROUP BY column [, column ...]]
STATE ( field_def [, ...] )
[REQUIRE module [, module ...]]
( LANGUAGE lua PROCESS $$ lua code $$
| LANGUAGE EXTERNAL ) ;

field_def := field Type DEFAULT literal
           | field Map(String, Type)
```

A reducer without GROUP BY has one group. A Map field has no DEFAULT (ADR-12).

IB-64 gives the semantics of a Lua body. Any other body, for example a WHEN clause, fails with INVALID_SCHEMA (ADR-10).

**IB-64 — Lua bodies.** A Lua body reads `state.<field>` and `row.<column>`, and assigns `state.<field>`. It calls `emit({...})` for each emit row. It can use `json.encode` and `json.decode`. The sandbox removes `os`, `io`, `debug`, `loadfile`, `dofile`, `load`, `rawget`, `rawset`, `require` and `package`. A module (DEF-29) that `REQUIRE` names is loaded before the body.

A Map field is `state.<field>` with the methods `get(key)`, `set(key, value)` and `delete(key)`. `get` returns a copy or nil, and a `set` of nil deletes the entry. A Map field takes no index, and a Map of a type other than JSON takes no table. A JS body has the same methods (IB-26).

**IB-66 — Views.**

```
CREATE MATERIALIZED VIEW name AS
SELECT item [, item ...]
FROM source
GROUP BY column [, column ...]
[WINDOW SLIDING INTERVAL n unit BY time_column] ;

item := column [AS name]
      | agg ( [column] ) [AS name]
      | toStartOfInterval ( column, INTERVAL n unit ) [AS name]
agg  := sum | count | min | max | avg | first | last
unit := SECOND | MINUTE | HOUR | DAY        (a plural form is valid in WINDOW SLIDING)
```

`count()` counts rows and `count(column)` counts non-Null values. The default output name of an aggregate is `func_column`, or `count` for `count()`. A timestamp column holds milliseconds. A composite like OHLCV is a select list of `first`, `max`, `min`, `last` and `sum` with a tumbling window.

**IB-67 — Limits.** A table has at most P-MAX-COLUMNS columns, and a reducer at most P-MAX-COLUMNS state fields. A view has at most P-MAX-AGGREGATES aggregates. A reducer has at most P-MAX-EMIT-COLUMNS emit columns.

**IB-68 — Refused constructs.** By ADR-14, an open refuses these constructs with INVALID_SCHEMA:

- WHERE, HAVING, joins, ORDER BY, LIMIT, OFFSET, DISTINCT, WITH, a GROUP BY modifier and each other SELECT clause in a view.
- Each option of CREATE MATERIALIZED VIEW, for example a column list, a TO table, OR REPLACE, TEMPORARY, OPTIONS or CLUSTER BY.
- A modifier of a function call in a view: DISTINCT, OVER, WITHIN GROUP, an ORDER BY in the arguments and each other. A modifier of the FROM table: an alias, TABLESAMPLE, a hint and each other.
- A view GROUP BY that does not name each key item of its select list once. A GROUP BY entry names each column item that reads the column with that name. If no column item reads it, the entry names the key item with that output name, as in SQL.
- A view with no aggregate.
- `sum`, `min`, `max` or `avg` over a column with no numeric mode. The check needs a declared type. A table column, a state field and a group-by column of a reducer over a table have one.
- Two tables, reducers or views with one name. Two columns of a table, two state fields of a reducer or two output columns of a view with one name.
- A default that does not parse as the type of its field:
  - A String or JSON default is a quoted literal, and a doubled quote in it is one quote.
  - A Boolean default is `true`, `false`, `1` or `0`.
  - A UInt256 default is a decimal or a `0x` hexadecimal number.
  - A Bytes or Base58 default is `''`.

**IB-70 — Builders.** The Rust `Pipeline` and the TS `Pipeline` generate a schema text of IB-60 from calls. A builder pipeline and its generated text give the same batches (REQ-35).

## Closed

**IB-65 — Event rules.** Removed by ADR-10. A WHEN clause in a reducer fails with INVALID_SCHEMA (IB-63).

## Rules for this document

**IB-71 — Versioning.** A change of a binding surface updates this document and the CT-5 tests in the same change.

**IB-72 — Unspecified surface.** A behavior of a binding that this document does not state is unspecified. Tests MUST NOT pin it.
