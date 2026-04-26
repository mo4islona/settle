# Settle — Public API Surface

> Internal engineering reference (not user-facing docs).
> Captures the **complete** public API of both bindings as of branch
> `refactor/rename-to-settle` (commit `f1fd90d`) and flags every drift
> between Rust and TypeScript that should be resolved before publishing
> the user-facing documentation.

---

## 1. Overview

Two distributions ship from this repo:

| Distribution | Crate / package | Entry | Storage backends |
|---|---|---|---|
| **Rust crate** | `settle` (single crate) | `src/lib.rs` | `MemoryBackend`, `RocksDbBackend`, custom (`Arc<dyn StorageBackend>`) |
| **NPM (Node)** | `@settle/stream` | `dist/index.js` (NAPI-RS) | Memory, RocksDB |
| **NPM (Web)** | `@settle/stream/web` | `dist/web.js` (WASM) | Memory only |

### Cargo features
- `rocksdb` — enables `RocksDbBackend` + RocksDB tuning fields on `Config`.
- `lua` — enables `LuaRuntime` (and `Lua` reducer body in schema).
- `napi` — Node.js native binding (consumed by `@settle/stream`).
- `wasm` — browser WASM target (consumed by `@settle/stream/web`).

### NPM exports (`bindings/typescript/settle/package.json`)
```jsonc
{
  "name": "@settle/stream",
  "version": "0.0.1-alpha.20",
  "exports": {
    ".":      { "types": "./dist/index.d.ts", "default": "./dist/index.js" },
    "./web":  { "types": "./dist/web.d.ts",   "default": "./dist/web.js"   }
  },
  "napi": { "name": "settle" }
}
```

---

## 2. Side-by-side Comparison Table

| Concept | Rust | TS (Node) | TS (WASM) | Notes |
|---|---|---|---|---|
| Construct | `Settle::open(Config) -> Result<Self>` | `Settle.open(config)` (static) | `new Settle({ schema })` | WASM does not accept `dataDir` etc. |
| Async init | n/a | n/a | `await init(wasmUrl?)` | WASM-only loader; throws if not called |
| Schema (raw) | `Config { schema: String, … }` | `SettleConfig.schema: string` | `{ schema: string }` | DDL string format identical |
| Schema (builder) | — | `Pipeline().table().createReducer().createView().build()` | same | Pure-TS; produces SQL → calls `Settle.open` |
| Atomic ingest | `settle.ingest(IngestInput) -> Result<Option<ChangeBatch>>` | `await db.ingest(input)` → `ChangeBatch \| null` | same | TS encodes `data` to msgpack at FFI boundary |
| Resolve fork cursor | `settle.resolve_fork_cursor(&[(BlockNumber, &str)]) -> Option<BlockCursor>` | `db.resolveForkCursor(SettleCursor[]) -> SettleCursor \| null` | same | Same semantics |
| Handle fork (atomic) | `settle.handle_fork(Vec<BlockCursor>) -> Result<ForkResult>` | `db.handleFork(SettleCursor[]) -> { cursor, batch }` | same | Same semantics |
| Flush buffer | `settle.flush() -> Option<ChangeBatch>` | `db.flush() -> ChangeBatch \| null` | same | |
| Ack batch | `settle.ack(seq: u64)` | `db.ack(seq: number)` | same | |
| Pending count | `settle.pending_count() -> usize` | `get pendingCount: number` | same | |
| Backpressure | `settle.is_backpressured() -> bool` | `get isBackpressured: boolean` | same | |
| Latest cursor | `settle.latest_cursor() -> Option<BlockCursor>` + `latest_block()` | `get cursor: SettleCursor \| null` | same | TS exposes only one cursor; semantics = latest |
| Finalized cursor | `settle.finalized_cursor() -> Option<BlockCursor>` + `finalized_block()` | — (visible only via `ChangeBatch.finalizedHead`) | same | No standalone getter |
| Register reducer (def) | `settle.register_reducer(ReducerDef) -> Result<()>` | — | — | TS `registerReducer` covers a different case (External callback) |
| Replace reducer runtime | `settle.set_reducer_runtime(name, Box<dyn ReducerRuntime>) -> Result<()>` | `db.registerReducer(opts)` (External only) | same (snake_case calls under the hood) | TS path only allows External JS callbacks |
| Replay reducer | `settle.replay_reducer(name) -> Result<()>` | — | — | Not exposed |
| Has reducer | `settle.has_reducer(name) -> bool` | — | — | Not exposed |
| Reducer kinds available | EventRules, Lua, External | External only (JS callbacks) | External only (JS callbacks) | Lua/EventRules can still be declared via SQL DDL |
| Custom storage | `Config::storage(Arc<dyn StorageBackend>)` | — | — | Pluggable trait Rust-only |
| RocksDB tuning | `Config { compression, disable_compaction, cache_size }` + `RocksDbConfig` | `SettleConfig { compression, disableCompaction, cacheSize }` | n/a | Same fields; **`Pipeline.build()` does not forward them** |
| Errors | `enum Error` (7 variants) + `Result<T>` | `throw new Error(...)` (untyped) | same | No `SettleError` class on TS |
| Perf tree | `Vec<PerfNode>` on `ChangeBatch` | `PerfNode[]` on `ChangeBatch` (camelCase fields) | same | TS prepends/appends `msgpack_encode`/`msgpack_decode` nodes |
| `onChange` callback | — (manual `flush()` + `ack()`) | `IngestInput.onChange(batch)` (auto-ack on resolve) | same | TS sugar; auto-acks even on callback throw (try/finally) |

---

## 3. Rust Public Surface

### 3.1 Module re-exports (`src/lib.rs`)
```rust
pub mod db;
pub mod change;
pub mod engine;
pub mod error;
pub mod json_conv;
pub mod msgpack_conv;
pub mod reducer_runtime;
pub mod schema;
pub mod storage;
pub mod test_helpers;
pub mod types;
```
There is no top-level `pub use`; consumers access via the modules
(e.g. `settle::db::Settle`, `settle::types::Value`). Internal `engine`
is exposed as a module but has no documented public surface and is
considered implementation detail. `test_helpers` is shared across the
crate's own tests/benches and the integration tests under `tests/` —
see §3.10 for its public surface.

### 3.2 `db::Settle` (`src/db.rs`)
```rust
pub struct Settle { /* fields private */ }

impl Settle {
    pub fn open(config: Config) -> Result<Self>;

    // Reducer management
    pub fn set_reducer_runtime(
        &mut self,
        name: &str,
        runtime: Box<dyn ReducerRuntime>,
    ) -> Result<()>;
    pub fn register_reducer(
        &mut self,
        def: schema::ast::ReducerDef,
    ) -> Result<()>;
    pub fn replay_reducer(&mut self, name: &str) -> Result<()>;
    pub fn has_reducer(&self, name: &str) -> bool;

    // Ingestion (atomic — process all tables, store rollback chain,
    // finalize, and flush in a single RocksDB WriteBatch)
    pub fn ingest(
        &mut self,
        input: IngestInput,
    ) -> Result<Option<ChangeBatch>>;

    // Buffering
    pub fn flush(&mut self) -> Option<ChangeBatch>;
    pub fn ack(&mut self, sequence: u64);
    pub fn pending_count(&self) -> usize;
    pub fn is_backpressured(&self) -> bool;

    // Cursors
    pub fn latest_block(&self) -> BlockNumber;
    pub fn latest_cursor(&self) -> Option<BlockCursor>;
    pub fn finalized_block(&self) -> BlockNumber;
    pub fn finalized_cursor(&self) -> Option<BlockCursor>;

    // Forks
    pub fn resolve_fork_cursor(
        &self,
        previous_blocks: &[(BlockNumber, &str)],
    ) -> Option<BlockCursor>;
    pub fn handle_fork(
        &mut self,
        rollback_chain: Vec<BlockCursor>,
    ) -> Result<ForkResult>;
}
```

> Removed in this branch (in favour of the atomic `ingest()` /
> `handle_fork()` pair): `process_batch`, `rollback`, `finalize`,
> `set_rollback_chain`. The semantics they provided are now covered
> by `ingest()` (per-block processing + finalization + chain tracking)
> and `handle_fork()` (rollback to a known good cursor). Tests and
> benchmarks use the helpers in `crate::test_helpers` (see §3.10).

### 3.3 `db::Config` + RocksDB
```rust
#[non_exhaustive]
pub struct Config {
    pub schema: String,
    pub max_buffer_size: usize,           // default 10_000
    pub data_dir: Option<String>,
    pub storage: Option<Arc<dyn StorageBackend>>,
    pub compression: Option<String>,      // "none"|"snappy"|"zstd"|"lz4"
    pub disable_compaction: bool,
    pub cache_size: Option<usize>,        // bytes; 0 = disable
}

impl Config {
    pub fn new(schema: impl Into<String>) -> Self;
    pub fn with_data_dir(schema: impl Into<String>, data_dir: impl Into<String>) -> Self;
    pub fn max_buffer_size(self, size: usize) -> Self;
    pub fn storage(self, storage: Arc<dyn StorageBackend>) -> Self;
}

pub struct IngestInput {
    pub data: HashMap<String, Vec<RowMap>>,  // each row must include `block_number: UInt64`
    pub rollback_chain: Vec<BlockCursor>,
    pub finalized_head: BlockCursor,
}

pub struct ForkResult {
    pub cursor: BlockCursor,
    pub batch: Option<ChangeBatch>,
}
```

`storage::rocks` (gated by `feature = "rocksdb"`):
```rust
pub struct RocksDbConfig {
    pub compression: Option<String>,
    pub disable_compaction: bool,
    pub cache_size: Option<usize>,
}

pub struct RocksDbBackend { /* … */ }
impl RocksDbBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self>;
    pub fn open_with_config(path: impl AsRef<Path>, config: &RocksDbConfig) -> Result<Self>;
    pub fn destroy(path: impl AsRef<Path>) -> Result<()>;
}
```

### 3.4 `storage::StorageBackend` trait
```rust
pub trait StorageBackend: Send + Sync {
    // Raw rows (block-keyed)
    fn put_raw_rows(&self, table: &str, block: BlockNumber, data: &[u8]) -> Result<()>;
    fn get_raw_rows(&self, table: &str, from: BlockNumber, to: BlockNumber)
        -> Result<Vec<(BlockNumber, Vec<u8>)>>;
    fn delete_raw_rows_after(&self, table: &str, after: BlockNumber) -> Result<()>;
    fn take_raw_rows_after(&self, table: &str, after: BlockNumber)
        -> Result<Vec<(BlockNumber, Vec<u8>)>>;

    // Reducer per-block snapshots
    fn put_reducer_state(&self, reducer: &str, group_key: &[u8], block: BlockNumber, state: &[u8]) -> Result<()>;
    fn get_reducer_state(&self, reducer: &str, group_key: &[u8], block: BlockNumber) -> Result<Option<Vec<u8>>>;
    fn get_reducer_state_at_or_before(&self, reducer: &str, group_key: &[u8], block: BlockNumber)
        -> Result<Option<(BlockNumber, Vec<u8>)>>;
    fn delete_reducer_states_after(&self, reducer: &str, group_key: &[u8], after: BlockNumber) -> Result<()>;

    // Reducer finalized state
    fn get_reducer_finalized(&self, reducer: &str, group_key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn set_reducer_finalized(&self, reducer: &str, group_key: &[u8], state: &[u8]) -> Result<()>;
    fn delete_reducer_states_up_to(&self, reducer: &str, group_key: &[u8], up_to: BlockNumber) -> Result<()>;

    // MV state
    fn put_mv_state(&self, view: &str, group_key: &[u8], state: &[u8]) -> Result<()>;
    fn get_mv_state(&self, view: &str, group_key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn delete_mv_state(&self, view: &str, group_key: &[u8]) -> Result<()>;
    fn list_mv_group_keys(&self, view: &str) -> Result<Vec<Vec<u8>>>;

    // Meta
    fn put_meta(&self, key: &str, value: &[u8]) -> Result<()>;
    fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn list_reducer_group_keys(&self, reducer: &str) -> Result<Vec<Vec<u8>>>;

    // Atomic batch
    fn commit(&self, batch: &StorageWriteBatch) -> Result<()>;
}

pub struct StorageWriteBatch { pub ops: Vec<BatchOp> }
pub enum BatchOp { /* PutRawRows | SetReducerFinalized | PutMvState | PutMeta | DeleteMvState | DeleteRawRowsAfter */ }

// Encoding helpers
pub fn encode_rows_from_maps(maps: &[RowMap], registry: &ColumnRegistry) -> Vec<u8>;
pub fn encode_rows(rows: &[Row]) -> Vec<u8>;
pub fn decode_rows(bytes: &[u8], registry: &Arc<ColumnRegistry>) -> Result<Vec<Row>>;
pub fn encode_group_key(key: &[Value]) -> Vec<u8>;
pub fn decode_group_key(bytes: &[u8]) -> GroupKey;
pub fn encode_state(state: &RowMap) -> Vec<u8>;
pub fn decode_state(bytes: &[u8]) -> RowMap;
```

### 3.5 Schema AST (`src/schema/ast.rs`, parser in `src/schema/parser.rs`)
```rust
pub fn parse_schema(input: &str) -> Result<Schema, Error>;

pub struct Schema {
    pub tables: Vec<TableDef>,
    pub modules: Vec<ModuleDef>,
    pub reducers: Vec<ReducerDef>,
    pub materialized_views: Vec<MVDef>,
}

pub struct TableDef     { pub name: String, pub columns: Vec<ColumnDef>, pub virtual_table: bool }
pub struct ColumnDef    { pub name: String, pub column_type: ColumnType }

pub enum ColumnType {
    UInt64, Int64, Float64, Uint256, String, DateTime,
    Boolean, Bytes, Base58, JSON,
}

pub struct MVDef {
    pub name: String, pub source: String,
    pub select: Vec<SelectItem>,
    pub group_by: Vec<String>,
    pub sliding_window: Option<SlidingWindowDef>,
}
pub struct SlidingWindowDef { pub interval_seconds: u64, pub time_column: String }
pub struct SelectItem       { pub expr: SelectExpr, pub alias: Option<String> }
pub enum   SelectExpr       { Column(String), Agg(AggFunc, Option<String>),
                              WindowFunc { column: String, interval_seconds: u64 } }
pub enum   AggFunc          { Sum, Count, Min, Max, Avg, First, Last }

pub struct ReducerDef {
    pub name: String, pub source: String,
    pub group_by: Vec<String>,
    pub state: Vec<StateField>,
    pub body: ReducerBody,
    pub requires: Vec<String>,
}
pub struct StateField { pub name: String, pub column_type: ColumnType, pub default: String }
pub enum ReducerBody {
    EventRules { when_blocks: Vec<WhenBlock>, always_emit: Option<AlwaysEmit> },
    Lua        { script: String },
    External   { id: String },
}
pub struct WhenBlock { pub condition: Expr, pub lets: Vec<(String,Expr)>, pub sets: Vec<(String,Expr)>, pub emits: Vec<(String,Expr)> }
pub struct AlwaysEmit { pub emits: Vec<(String, Expr)> }
pub enum Expr { Literal(String), Float(f64), Int(i64),
                ColumnRef(String), StateRef(String), RowRef(String),
                BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr> },
                If { condition: Box<Expr>, then_expr: Box<Expr>, else_expr: Box<Expr> } }
pub enum BinaryOp { Add, Sub, Mul, Div, Eq, Neq, Gt, Lt, Gte, Lte, And, Or }

pub struct ModuleDef { pub name: String, pub script: String }
```

### 3.6 Core types (`src/types.rs`)
```rust
pub type BlockNumber = u64;
pub type ColumnId = u16;
pub type RowMap = HashMap<String, Value>;
pub type GroupKey = SmallVec<[Value; 2]>;

pub struct ColumnRegistry { /* names ↔ ColumnId */ }
impl ColumnRegistry {
    pub fn new(names: Vec<String>) -> Self;
    pub fn get_id(&self, name: &str) -> Option<ColumnId>;
    pub fn get_name(&self, id: ColumnId) -> Option<&str>;
    pub fn len(&self) -> usize;
    pub fn names(&self) -> &[String];
}

pub struct Row { /* Arc<ColumnRegistry> + Vec<Value> */ }
impl Row {
    pub fn new(registry: Arc<ColumnRegistry>) -> Self;
    pub fn from_values(registry: Arc<ColumnRegistry>, values: Vec<Value>) -> Self;
    pub fn from_map(registry: Arc<ColumnRegistry>, map: &HashMap<String, Value>) -> Self;
    pub fn get(&self, name: &str) -> Option<&Value>;
    pub fn set(&mut self, name: &str, value: Value);
    pub fn to_map(&self) -> HashMap<String, Value>;
    pub fn values(&self) -> &[Value];
    pub fn registry(&self) -> &Arc<ColumnRegistry>;
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)>;       // skips Null
    pub fn iter_all(&self) -> impl Iterator<Item = (&str, &Value)>;   // includes Null
}

#[derive(…)]
pub enum Value {
    UInt64(u64), Int64(i64), Float64(f64),
    Uint256([u8; 32]),
    String(String), DateTime(i64),
    Boolean(bool), Bytes(Vec<u8>), Base58(Vec<u8>),
    JSON(serde_json::Value),
    Null,
}
impl Value {
    pub fn as_f64(&self)  -> Option<f64>;
    pub fn as_i64(&self)  -> Option<i64>;
    pub fn as_u64(&self)  -> Option<u64>;
    pub fn as_str(&self)  -> Option<&str>;
    pub fn as_bool(&self) -> Option<bool>;
    pub fn is_null(&self) -> bool;
    pub fn is_truthy(&self) -> bool;
    pub fn type_name(&self) -> &'static str;
    pub fn column_type(&self) -> Option<ColumnType>;
}

pub struct BlockCursor { pub number: BlockNumber, pub hash: String }
```

### 3.7 Change records (`src/types.rs`, `src/change.rs`)
```rust
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOp { Insert, Update, Delete }   // wire form: "insert"|"update"|"delete"

pub struct ChangeRecord {
    pub table: String,
    pub operation: ChangeOp,
    pub key: HashMap<String, Value>,
    pub values: HashMap<String, Value>,
    pub prev_values: Option<HashMap<String, Value>>,
}

pub struct ChangeBatch {
    pub sequence: u64,
    pub finalized_head: Option<BlockCursor>,
    pub latest_head: Option<BlockCursor>,
    pub tables: HashMap<String, Vec<ChangeRecord>>,
    pub perf: Vec<PerfNode>,
}
impl ChangeBatch {
    pub fn all_records(&self) -> impl Iterator<Item = &ChangeRecord>;
    pub fn records_for(&self, table: &str) -> &[ChangeRecord];
    pub fn record_count(&self) -> usize;
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerfNodeKind {
    Pipeline, RawTable, Reducer,
    #[serde(rename = "mv")] MV,
    Parallel,
}
pub struct PerfNode {
    pub kind: PerfNodeKind,
    pub name: String,
    pub duration_ms: f64,
    pub children: Vec<PerfNode>,
}

// Buffer (mostly internal but exported)
pub struct ChangeBuffer { /* … */ }
impl ChangeBuffer {
    pub fn new(max_buffer_size: usize) -> Self;
    pub fn is_full(&self) -> bool;
    pub fn pending_count(&self) -> usize;
    pub fn set_heads(&mut self, finalized_head: Option<BlockCursor>, latest_head: Option<BlockCursor>);
    pub fn push(&mut self, records: Vec<ChangeRecord>,
                finalized_head: Option<BlockCursor>, latest_head: Option<BlockCursor>,
                perf: Vec<PerfNode>);
    pub fn flush(&mut self) -> Option<ChangeBatch>;
    pub fn ack(&mut self, sequence: u64);
}
```

### 3.8 Reducer runtimes (`src/reducer_runtime/`)
```rust
pub struct GroupBatch {
    pub state: HashMap<String, Value>,
    pub rows:  Vec<Row>,
    pub emits: Vec<RowMap>,
}

pub trait ReducerRuntime: Send {
    fn process(&self, state: &mut HashMap<String, Value>, row: &Row) -> Result<Vec<RowMap>>;
    fn use_batched_processing(&self) -> bool { false }
    fn process_grouped(&self, groups: &mut [GroupBatch]) -> Result<()> { /* default impl loops */ }
}

pub struct EventRulesRuntime;
impl EventRulesRuntime { pub fn new(body: &ReducerBody) -> Self; }

pub struct LuaRuntime;     // gated by `feature = "lua"`
impl LuaRuntime {
    pub fn new(script: &str) -> Self;
    pub fn with_state_fields(
        script: &str,
        state_fields: &[String],
        state_types: &[(String, ColumnType)],
        source_columns: &[String],
        modules: &[(String, String)],
    ) -> Self;
}

pub struct ExternalRuntime;
impl ExternalRuntime {
    pub fn new(id: String) -> Self;
    // fn use_batched_processing(&self) -> bool { true }
}
```

### 3.9 Errors (`src/error.rs`)
```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("schema error: {0}")]          Schema(String),
    #[error("storage error: {0}")]         Storage(String),
    #[error("reducer error: {0}")]         Reducer(String),
    #[error("rollback error: {0}")]        Rollback(String),
    #[error("serialization error: {0}")]   Serialization(#[from] rmp_serde::encode::Error),
    #[error("deserialization error: {0}")] Deserialization(#[from] rmp_serde::decode::Error),
    #[error("invalid operation: {0}")]     InvalidOperation(String),
}

pub type Result<T> = std::result::Result<T, Error>;
```

### 3.10 `test_helpers` (`src/test_helpers.rs`)
Public so integration tests under `tests/` and benchmarks under
`benches/` (each its own crate) can drive `Settle` without rebuilding
`IngestInput`/`BlockCursor` payloads at every call site. Re-exported
from `db_test_helpers` for unit tests under `db::*_tests`.
```rust
pub fn block_hash(n: BlockNumber) -> String;        // "0x{n:016x}"
pub fn cursor(n: BlockNumber) -> BlockCursor;

pub fn ingest_one(
    db: &mut Settle,
    table: &str,
    block: BlockNumber,
    rows: Vec<RowMap>,
) -> Result<Option<ChangeBatch>>;

pub fn ingest_blocks(
    db: &mut Settle,
    items: Vec<(String, BlockNumber, Vec<RowMap>)>,
) -> Result<Option<ChangeBatch>>;

pub fn ingest_with_finalized(
    db: &mut Settle,
    items: Vec<(String, BlockNumber, Vec<RowMap>)>,
    finalized: BlockNumber,
) -> Result<Option<ChangeBatch>>;

pub fn rollback_to(
    db: &mut Settle,
    fork_point: BlockNumber,
) -> Result<ForkResult>;
```
- `ingest_one` / `ingest_blocks` keep all ingested blocks unfinalized
  relative to `db.finalized_block()`, so subsequent `rollback_to`
  calls can target any of them.
- `ingest_with_finalized` lets the caller pin the finalized head
  explicitly — every block above it lands in `rollback_chain`.
- `rollback_to(0)` falls through to `ingest()` with empty data and
  no chain, exercising the "no common ancestor → full rollback" path
  (the only way to drop everything when no block hash matches the
  requested fork point — `handle_fork` itself errors in that case).

---

## 4. TypeScript Public Surface

### 4.1 Package exports
```ts
// dist/index.d.ts (Node entry)
export * from './column'
export {
  type AggExpr, type AggProxy, type GroupByItem, type IntervalExpr,
  interval, type KeyRef,
  type ReducerCtx, type ReducerOptions,
  type SlidingWindowOptions, type ViewOptions,
} from './ddl'
export {
  type ChangeBatch, Settle,
  type SettleConfig, type SettleCursor,
  type ChangeOp, type ChangeRecord,
  type ExternalReducerOptions, type IngestInput, type StateFieldDef,
} from './settle'
export { Pipeline, ReducerHandle, TableHandle, ViewHandle } from './pipeline'
```

```ts
// dist/web.d.ts (Web entry) — same builder/column/ddl re-exports plus:
export async function init(wasmUrl?: URL | string): Promise<void>
export class Settle { /* WASM-backed; see 4.3 */ }
```

> `bindings/typescript/settle/src/builder.ts` exists but is **not** referenced
> by `index.ts` — it is a superseded version of `pipeline.ts` + `ddl.ts`
> and should be considered dead code.

### 4.2 `Settle` class — Node (`src/settle.ts`)
```ts
export class Settle {
  static open(config: SettleConfig): Settle

  async ingest(input: IngestInput): Promise<ChangeBatch | null>
  resolveForkCursor(previousBlocks: SettleCursor[]): SettleCursor | null
  handleFork(previousBlocks: SettleCursor[]): { cursor: SettleCursor; batch: ChangeBatch | null }
  flush(): ChangeBatch | null
  ack(sequence: number): void

  get pendingCount(): number
  get isBackpressured(): boolean
  get cursor(): SettleCursor | null

  registerReducer<TState, TRow, TEmit>(opts: ExternalReducerOptions<TState, TRow, TEmit>): void
}
```
- `ingest()` msgpack-encodes `input.data` to a `Buffer` before FFI;
  decoded `ChangeBatch` is augmented with `msgpack_encode` (front) and
  `msgpack_decode` (back) `PerfNode` entries.
- If `IngestInput.onChange` is set, the wrapper calls it then auto-acks
  in a `try/finally`.

### 4.3 `Settle` class — Web (`src/web.ts`)
```ts
export async function init(wasmUrl?: URL | string): Promise<void>
// Must be called once; throws on construct otherwise.

export class Settle {
  constructor(config: { schema: string })

  registerReducer<TState, TRow, TEmit>(opts: ExternalReducerOptions<TState, TRow, TEmit>): void

  async ingest(input: IngestInput): Promise<ChangeBatch | null>
  flush(): ChangeBatch | null
  ack(sequence: number): void

  get pendingCount(): number
  get isBackpressured(): boolean
  get cursor(): SettleCursor | null

  resolveForkCursor(previousBlocks: SettleCursor[]): SettleCursor | null
  handleFork(previousBlocks: SettleCursor[]): { cursor: SettleCursor; batch: ChangeBatch | null }
}
```
**Differences vs Node:**
- Constructor is `new Settle({ schema })` instead of `Settle.open(config)`.
- No `dataDir` / `compression` / `disableCompaction` / `cacheSize` /
  `maxBufferSize` (memory-only WASM).
- Native methods are still **snake_case** under the hood
  (`register_reducer`, `resolve_fork_cursor`, `handle_fork`); the Node
  NAPI layer has already been camelCased.

### 4.4 `SettleConfig`
```ts
export interface SettleConfig {
  schema: string
  dataDir?: string
  maxBufferSize?: number                                  // default 10_000 (Rust-side)
  compression?: 'none' | 'snappy' | 'zstd' | 'lz4'
  disableCompaction?: boolean
  cacheSize?: number                                      // bytes; 0 disables
}

export interface SettleCursor { number: number; hash: string }

export interface IngestInput {
  data: Record<string, Record<string, any>[]>
  rollbackChain?: SettleCursor[]
  finalizedHead: SettleCursor
  onChange?: (batch: ChangeBatch) => void | Promise<void>
}
```

### 4.5 Builder API (`src/pipeline.ts`)
```ts
export class Pipeline {
  table<TCols extends Record<string, ColumnType>>(
    name: string, columns: TCols, opts?: { virtual?: boolean },
  ): TableHandle<InferRow<TCols>>

  build(opts?: { dataDir?: string; maxBufferSize?: number }): Settle
}

export class TableHandle<TRow = any> {
  get name(): string
  createReducer<TState, TEmit>(name: string, opts: ReducerOptions<TState, TRow, TEmit>): ReducerHandle<TEmit>
  createView(name: string, opts: ViewOptions<TRow>): ViewHandle
}

export class ReducerHandle<TOutput = any> {
  get name(): string
  createReducer<TState, TEmit>(name: string, opts: ReducerOptions<TState, TOutput, TEmit>): ReducerHandle<TEmit>
  createView(name: string, opts: ViewOptions<TOutput>): ViewHandle
}

export class ViewHandle { get name(): string }
```
Builder generates SQL DDL via `tableToSql` / `reducerToSql` / `viewToSql`
(see 4.7), then calls `Settle.open({ schema, dataDir, maxBufferSize })`
and registers JS reducers via `registerReducer`. `Pipeline.build()`
**only forwards `dataDir` and `maxBufferSize`** — RocksDB tuning fields
are ignored.

### 4.6 Column types (`src/column.ts`)
```ts
export interface ColumnType<T = any> {
  readonly _sql: string
  readonly _type?: T   // phantom
}

export const uint64:  () => ColumnType<number>
export const int64:   () => ColumnType<number>
export const float64: () => ColumnType<number>
export const uint256: () => ColumnType<bigint>
export const string:  () => ColumnType<string>
export const datetime:() => ColumnType<number>           // ms since epoch
export const boolean: () => ColumnType<boolean>
export const bytes:   () => ColumnType<Uint8Array>
export const base58:  () => ColumnType<string>
export function json<T = any>(): ColumnType<T>

export type InferRow<T extends Record<string, ColumnType>> = {
  [K in keyof T]: T[K] extends ColumnType<infer V> ? V : unknown
}
```

### 4.7 DDL helpers (`src/ddl.ts`)
```ts
export function parseDuration(s: string): number          // seconds
export function interval(column: string, duration: string): IntervalExpr

export interface IntervalExpr {
  _type: 'interval'
  column: string
  seconds: number
  alias?: string
  as(alias: string): IntervalExpr
}

export interface AggExpr { _type: 'agg'; func: string; column: string | null }
export interface KeyRef  { _type: 'key'; column: string }
export interface AggProxy<TSource = any> {
  key: Record<string, KeyRef>
  sum(c: string & keyof TSource): AggExpr
  count(): AggExpr
  first(c: string & keyof TSource): AggExpr
  last (c: string & keyof TSource): AggExpr
  min  (c: string & keyof TSource): AggExpr
  max  (c: string & keyof TSource): AggExpr
  avg  (c: string & keyof TSource): AggExpr
}

export type GroupByItem = string | IntervalExpr

export type ReducerCtx<TState, TEmit> = Readonly<TState> & {
  update(newState: TState): void
  emit(row: TEmit): void
}

export interface ReducerOptions<TState, TRow, TEmit> {
  groupBy: (string & keyof TRow) | (string & keyof TRow)[]
  initialState: TState
  reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void
}

export interface SlidingWindowOptions<TSource = any> {
  interval: string                     // "1 hour", "30 min", …
  timeColumn: string & keyof TSource
}

export interface ViewOptions<TSource = any> {
  groupBy: GroupByItem | GroupByItem[]
  select: (agg: AggProxy<TSource>) => Record<string, AggExpr | KeyRef>
  slidingWindow?: SlidingWindowOptions<TSource>
}

// Inference / DDL emit helpers (exported because Pipeline uses them; not commonly user-facing)
export function inferStateFields(initialState: Record<string, unknown>): StateFieldDef[]
export function tableToSql(name: string, columns: Record<string, ColumnType>, virtual: boolean): string
export function reducerToSql(name: string, source: string, groupBy: string[], stateFields: StateFieldDef[]): string
export function viewToSql(name: string, source: string, groupByItems: GroupByItem[],
                          selectFn: (agg: AggProxy<any>) => Record<string, AggExpr | KeyRef>,
                          slidingWindow?: SlidingWindowOptions): string
```

### 4.8 Output types (`src/settle.ts`)
```ts
export type ChangeOp = 'insert' | 'update' | 'delete'

export interface ChangeRecord {
  table: string
  operation: ChangeOp
  key:        Record<string, any>
  values:     Record<string, any>
  prevValues: Record<string, any> | null
}

export type PerfNodeKind = 'pipeline' | 'raw_table' | 'reducer' | 'mv' | 'parallel'
export interface PerfNode {
  kind: PerfNodeKind
  name: string
  durationMs: number
  children: PerfNode[]
}

export interface ChangeBatch {
  sequence: number
  finalizedHead: SettleCursor | null
  latestHead:    SettleCursor | null
  tables: Record<string, ChangeRecord[]>
  perf: PerfNode[]
}
```

### 4.9 External reducer registration
```ts
export interface StateFieldDef {
  name: string
  columnType: string             // "Float64" | "UInt64" | "Int64" | "String" | "Boolean" | "Json"
  defaultValue: string           // SQL literal
}

export interface ExternalReducerOptions<TState = any, TRow = any, TEmit = any> {
  name: string
  source: string
  groupBy: string[]
  state: StateFieldDef[]
  reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void
}

class Settle {
  registerReducer<TState, TRow, TEmit>(opts: ExternalReducerOptions<TState, TRow, TEmit>): void
}
```
The wrapper builds a per-call `ctx` with `update()`, `emit()`, and a
shallow snapshot of `state` properties; iterates `rows`; returns
`{ state, emits }[]` to the native side.

### 4.10 Native binding (`src/native/native.d.ts`, generated)
```ts
export declare class Settle {
  static open(config: SettleConfig): Settle
  ingest(input: IngestInput): Buffer | null            // msgpack-encoded ChangeBatch
  resolveForkCursor(previousBlocks: SettleCursor[]): SettleCursor | null
  handleFork(previousBlocks: SettleCursor[]): ForkResultJs
  flush(): Buffer | null
  ack(sequence: number): void
  registerReducer(config: ExternalReducerConfig, callback: (groups: any[]) => any[]): void
  get pendingCount(): number
  get isBackpressured(): boolean
  get cursor(): SettleCursor | null
}

interface ForkResultJs { cursor: SettleCursor; batch?: Buffer }
interface IngestInput  { data: Buffer; rollbackChain?: SettleCursor[]; finalizedHead: SettleCursor }
interface ExternalReducerConfig { name: string; source: string; groupBy: string[]; state: ExternalStateField[] }
interface ExternalStateField    { name: string; columnType: string; defaultValue: string }
```

---

## 5. Inconsistencies & Drift

Each item is a divergence between bindings, **not** a recommendation —
remediation will be decided in a follow-up. Severity columns:

- **B** = breaking change to fix
- **A** = additive (new TS surface, no break)
- **D** = docs/cosmetic only

### A. Schema definition

| # | Drift | Severity |
|---|---|---|
| 1 | TS exposes a fluent `Pipeline` builder; Rust has SQL-only. Builder reduces typo surface and gives row-type inference but doesn't cover the full SQL grammar. | A |
| 2 | TS builder cannot define `CREATE MODULE` (Lua modules) or `CREATE REDUCER … USING Lua` / `USING EventRules`. Only `LANGUAGE EXTERNAL` is emitted (`reducerToSql` in `ddl.ts:177-187`). | A |
| 3 | Builder writes `SOURCE`/`STATE`/`LANGUAGE EXTERNAL` keywords — verify these match the parser grammar in `src/schema/parser.rs` (the AST shows `source: String`, `body: ReducerBody::External { id }`). If parser still expects `ON <table> USING External(<id>)`, builder output may be parser-version-specific. | B |

### B. Reducer surface

| # | Drift | Severity |
|---|---|---|
| 4 | Rust publicly exposes `LuaRuntime`, `EventRulesRuntime`, `ExternalRuntime`; TS exposes only the External path through `registerReducer`. | A (Lua/EventRules already work via DDL, just not configurable from TS) |
| 5 | Rust has two distinct methods: `register_reducer(ReducerDef)` adds new definitions; `set_reducer_runtime(name, Box<dyn ReducerRuntime>)` swaps the runtime of an existing one. TS `registerReducer` conflates both — it expects the reducer to already be in the schema (via builder) and only attaches the JS callback. | D |
| 6 | `replay_reducer(name)` and `has_reducer(name)` are public in Rust, not exposed in TS. | A |

### C. Lifecycle / atomicity

| # | Drift | Severity |
|---|---|---|
| 7 | ~~Rust exposes manual `rollback()`, `finalize()`, `set_rollback_chain()` separately from `ingest()`.~~ **Resolved on this branch:** removed from `Settle`. The atomic `ingest()` + `handleFork()` pair is now the only public path, matching TS. Tests and benches use the helpers in `crate::test_helpers` (§3.10). | — |
| 8 | ~~`process_batch` is `#[doc(hidden)]` and marked deprecated.~~ **Resolved on this branch:** removed. ~100 call sites in tests/benches were migrated to `ingest_one` / `ingest_blocks`. | — |

### D. Cursor / state queries

| # | Drift | Severity |
|---|---|---|
| 9 | Rust exposes 4 getters: `latest_block`, `latest_cursor`, `finalized_block`, `finalized_cursor`. TS exposes only `cursor` (latest). To read finalized state from TS you must inspect `ChangeBatch.finalizedHead` returned by `ingest()`. | A |

### E. Storage

| # | Drift | Severity |
|---|---|---|
| 10 | `Config::storage(Arc<dyn StorageBackend>)` lets Rust callers plug a custom backend. There is no equivalent escape hatch in TS. (Probably intentional; document the constraint.) | D |
| 11 | `Pipeline.build()` (TS) accepts only `dataDir` and `maxBufferSize` — it does not forward `compression`, `disableCompaction`, `cacheSize` even though `SettleConfig` supports them. Builder users cannot tune RocksDB. | B |

### F. WASM vs Node TS

| # | Drift | Severity |
|---|---|---|
| 12 | Different constructors: WASM uses `new Settle({ schema })`, Node uses static `Settle.open(config)`. Builder code that calls `Settle.open(...)` will not work against WASM. | B |
| 13 | WASM has no persistence options at all (memory-only). Document the constraint clearly. | D |
| 14 | WASM's underlying binding still uses `snake_case` method names (`register_reducer`, `resolve_fork_cursor`, `handle_fork`); the Node NAPI binding has been camelCased. The TS wrapper hides this from end users but it complicates the layered code. | D |
| 15 | WASM `Settle` and Node `Settle` are two distinct exported classes; consider unifying behind a single interface or factory. | B |

### G. Errors

| # | Drift | Severity |
|---|---|---|
| 16 | Rust has a typed `Error` enum (Schema / Storage / Reducer / Rollback / (De)Serialization / InvalidOperation). TS has no `SettleError` class — every failure is a plain `Error` with a string message. Consumers cannot programmatically distinguish error kinds. | A (introduce typed error class) |

### H. Output / wire format

| # | Drift | Severity |
|---|---|---|
| 17 | `ChangeOp` wire format **matches** (Rust `#[serde(rename_all = "lowercase")]` produces `"insert"`/`"update"`/`"delete"`, equal to TS string union). No drift; document this guarantee so future enum additions follow the same casing. | D |
| 18 | `PerfNodeKind` wire format **matches** (snake_case + `mv` rename). Same guarantee should be documented. | D |
| 19 | Field naming: Rust serializes `duration_ms`/`finalized_head`/`prev_values`; TS expects `durationMs`/`finalizedHead`/`prevValues`. Verify msgpack-side renames (likely via `#[serde(rename)]`) cover every public struct — easy regression vector. | B if missing |
| 20 | TS Node wrapper injects synthetic `msgpack_encode`/`msgpack_decode` `PerfNode`s into `batch.perf`. The WASM wrapper does **not** (no msgpack hop). Perf-tree shape is therefore platform-dependent. | D |

### I. Ergonomics

| # | Drift | Severity |
|---|---|---|
| 21 | TS `IngestInput.onChange` callback auto-acks on resolve (in `try/finally`). Rust users must manually call `flush()` and `ack()`. | A |
| 22 | TS `ingest()` returns `null` for "no records produced"; Rust returns `Ok(None)`. Equivalent semantics, document so. | D |
| 23 | TS `bindings/typescript/settle/src/builder.ts` (377 lines) is **dead code** — superseded by `pipeline.ts` + `ddl.ts`, not exported from `index.ts`, kept around in source. Candidate for deletion. | D |
| 24 | TS `bindings/typescript/polygains-main/` is an out-of-tree consumer fork. Out of scope for the API doc but worth noting as a downstream that may pin pre-rename APIs. | D |
| 25 | TS `bindings/typescript/settle/examples/settle-target.test.ts` (818 lines) references `db.processBatch` / `db.rollback` / `db.finalize` — methods that have not existed on the TS `Settle` since the rename commit (`src/napi.rs:205` confirms NAPI deliberately drops them). The file is excluded from `tsconfig.build.json` and from the `vitest` include glob, so it does not break the build, but it is broken code on disk. Candidate for deletion or rewrite to `db.ingest()` / `db.handleFork()`. | D |

---

## 6. Open Questions for Documentation Phase

Surface these before starting user-facing docs:

1. Is the SQL DDL grammar formally specified anywhere, or is the parser
   the only source of truth? Doc effort scales with this answer.
2. Should the user-facing TS docs surface the builder API as the
   canonical entry point and treat raw SQL as escape hatch — or both
   equally?
3. Is there an intended "minimum API" that Rust and TS must both
   support? (If yes, this drives which of the items in §5 are
   blockers.)
4. WASM target: is it production-grade or experimental? Determines
   whether §F items 12–15 are blockers or footnotes.
5. Public surface of `engine` module in Rust: keep `pub` or downgrade
   to `pub(crate)`? (Currently `pub mod engine;` in `lib.rs` but
   `SettleEngine` is not documented as user-facing.)
