//! ExternalRuntime — reducer logic provided by the host language via napi callback.
//!
//! Uses a thread-local "callback slot" to bridge the gap between the
//! `ReducerRuntime` trait (which requires Send + Sync) and napi's `JsFunction`
//! (which is not Send). The slot is installed by the napi layer before each
//! `process_batch`/`ingest` call and cleared after via an RAII guard.
//!
//! The batched path sends all groups for a block in a single JS call,
//! avoiding per-row FFI overhead. A body reaches the MAP fields of its group
//! with host calls into `with_host_map` during that call.

use std::cell::RefCell;

use crate::types::{ColumnRegistry, Row};

use super::map::{MapMut, StateMaps};
use super::{Emits, GroupState, GroupedRows, ReducerRuntime};

// ─── MAP fields in a host callback ─────────────────────────────────

thread_local! {
    /// The MAP fields of each group of the block in the host callback that
    /// runs, by group index; None outside a callback.
    static HOST_MAPS: RefCell<Option<Vec<Option<Box<StateMaps>>>>> = const { RefCell::new(None) };
}

/// Run `call`, the host callback of `block`, with the MAP fields of its
/// groups in reach of `with_host_map`. They go back into the groups, journal
/// and all, also when the callback fails.
#[cfg(any(feature = "napi", feature = "wasm", test))]
fn with_host_maps<T>(block: &mut GroupedRows<'_>, call: impl FnOnce() -> T) -> T {
    if block.map_fields().is_empty() {
        return call();
    }

    let maps = block.groups.iter_mut().map(|group| group.maps.take()).collect();
    let outer = HOST_MAPS.with(|cell| cell.replace(Some(maps)));
    let result = call();
    let maps = HOST_MAPS
        .with(|cell| cell.replace(outer))
        .expect("the maps of this callback");

    for (group, maps) in block.groups.iter_mut().zip(maps) {
        group.maps = maps;
    }
    result
}

/// Run `f` on MAP field `map` of group `group` of the block in the host
/// callback that runs.
pub fn with_host_map<T>(group: u32, map: u32, f: impl FnOnce(MapMut<'_>) -> T) -> Result<T, String> {
    HOST_MAPS.with(|cell| {
        let mut groups = cell
            .try_borrow_mut()
            .map_err(|_| "a Map field is used inside a call on a Map field")?;
        let groups = groups
            .as_mut()
            .ok_or("a Map field is used outside a reducer callback")?;
        let maps = groups
            .get_mut(group as usize)
            .and_then(Option::as_deref_mut)
            .ok_or_else(|| format!("the block has no group {group} with Map fields"))?;
        let field = maps
            .map_at(map as usize)
            .ok_or_else(|| format!("the reducer has no Map field {map}"))?;
        Ok(f(field))
    })
}

/// The row of emit `index` from a callback that names no rows: emit n
/// belongs to row n, and each emit past the rows to the last row.
#[cfg(any(feature = "wasm", test))]
fn legacy_emit_row(index: usize, rows: usize) -> usize {
    index.min(rows.saturating_sub(1))
}

/// The emit of host callback row `row` of a group whose rows are `rows`
/// (indices in the block), by name.
#[cfg(any(feature = "wasm", test))]
fn push_host_emit(
    emits: &mut Emits,
    rows: &[usize],
    row: usize,
    emit: crate::types::RowMap,
) -> Result<(), String> {
    let &source = rows
        .get(row)
        .ok_or_else(|| format!("an emit names row {row} of a group with {} rows", rows.len()))?;
    emits.set_source(source);
    emits.push_named(emit);
    Ok(())
}

// ─── Thread-local callback context (napi only) ────────────────────

#[cfg(feature = "napi")]
mod napi_bridge {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::ptr;

    use napi::sys;

    use crate::js_codec::napi::{
        Arrays, HandleScope, element, other_value, read_strings, to_js,
    };
    use crate::js_codec::{self, Decoder};
    use crate::reducer_runtime::{Emits, GroupedRows};
    use crate::types::ColumnRegistry;

    use super::with_host_maps;

    /// Raw napi env + callback pointers. Stored in thread-local.
    /// We store raw pointers because napi::Ref<()> is not Clone.
    pub(crate) struct ExternalContext {
        env: napi::Env,
        /// reducer_id → raw napi_ref (prevent GC)
        callbacks: HashMap<String, sys::napi_ref>,
    }

    impl ExternalContext {
        pub(crate) fn has_callback(&self, reducer_id: &str) -> bool {
            self.callbacks.contains_key(reducer_id)
        }
    }

    thread_local! {
        pub(crate) static EXTERNAL_CTX: RefCell<Option<ExternalContext>> = const { RefCell::new(None) };
    }

    /// Install the external callback context for the current thread.
    /// Returns an RAII guard that clears it on drop.
    pub fn install_context(
        env: napi::Env,
        callbacks: &HashMap<String, sys::napi_ref>,
    ) -> ContextGuard {
        EXTERNAL_CTX.with(|cell| {
            *cell.borrow_mut() = Some(ExternalContext {
                env,
                callbacks: callbacks.clone(),
            });
        });
        ContextGuard
    }

    /// RAII guard — clears the thread-local on drop (even on panic).
    pub struct ContextGuard;

    impl Drop for ContextGuard {
        fn drop(&mut self) {
            EXTERNAL_CTX.with(|cell| {
                *cell.borrow_mut() = None;
            });
        }
    }

    /// Run the JS callback of `reducer_id` on the groups of a block, in one
    /// call each way (`nativeBatchCallback` in `reducer-callback.ts`).
    ///
    /// The callback gets `(buffer, strings, others)` from
    /// `js_codec::encode_groups` and returns `[ints, tags, nums, strings,
    /// columns, others]`: ints `[int count, slot count, column count, ...
    /// js_codec::decode_group_results ..., the UTF-16 length of each
    /// column name]`; `columns` is the emit column names, joined.
    ///
    /// If the callback throws, the JS exception stays pending: the ingest
    /// that fails with this error rethrows it.
    pub fn call_js_batch(
        reducer_id: &str,
        block: &mut GroupedRows<'_>,
        fields: &ColumnRegistry,
        emits: &mut Emits,
    ) -> Result<(), String> {
        EXTERNAL_CTX.with(|cell| {
            let ctx = cell.borrow();
            let ctx = ctx
                .as_ref()
                .ok_or("external reducer called outside a napi call")?;
            let raw_ref = ctx
                .callbacks
                .get(reducer_id)
                .ok_or_else(|| format!("no callback for reducer '{reducer_id}'"))?;
            let env = &ctx.env;
            let raw = env.raw();
            let _scope = HandleScope::open(env)?;

            let mut callback = ptr::null_mut();
            let status = unsafe { sys::napi_get_reference_value(raw, *raw_ref, &mut callback) };
            if status != sys::Status::napi_ok || callback.is_null() {
                return Err("the callback is gone".to_string());
            }

            let args = to_js(env, &js_codec::encode_groups(block, fields))?;
            let mut this = ptr::null_mut();
            unsafe { sys::napi_get_undefined(raw, &mut this) };
            let mut result = ptr::null_mut();
            let status = with_host_maps(block, || unsafe {
                sys::napi_call_function(raw, this, callback, args.len(), args.as_ptr(), &mut result)
            });
            if status != sys::Status::napi_ok {
                return Err("the callback threw".to_string());
            }

            let part = |i| element(env, result, i);
            let arrays = Arrays::read(env, part(0)?, part(1)?, part(2)?)?;
            let strings = read_strings(env, part(3)?)?;
            let column_names = read_strings(env, part(4)?)?;
            let others_array = part(5)?;

            let ints = &arrays.ints;
            let column_count = *ints.get(2).ok_or("the ints have no column count")? as usize;
            if 3 + column_count > ints.len() {
                return Err("the output header is out of range".to_string());
            }

            let columns_at = ints.len() - column_count;
            let columns = js_codec::split_names(column_names.as_strings(), &ints[columns_at..])?;
            let mut dec = Decoder::new(
                &ints[3..columns_at],
                &arrays.tags,
                &arrays.nums,
                strings.as_strings(),
                false,
            )?;
            let mut others = |i| other_value(env, others_array, i, false);
            js_codec::decode_group_results(&mut dec, &columns, block, emits, &mut others)
        })
    }
}

#[cfg(feature = "napi")]
pub use napi_bridge::{ContextGuard, install_context};

// ─── WASM callback context ─────────────────────────────────────────

#[cfg(feature = "wasm")]
mod wasm_bridge {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use js_sys::Function;
    use wasm_bindgen::prelude::*;

    use crate::js_codec::{MAX_JSON_DEPTH, MAX_SAFE_INTEGER, bigint_to_value, json_number};
    use crate::json_conv::value_to_json;
    use crate::wasm::json_to_js;
    use crate::reducer_runtime::{Emits, GroupedRows};
    use crate::types::{ColumnRegistry, Row, RowMap, Value};

    use super::{legacy_emit_row, push_host_emit, with_host_maps};

    thread_local! {
        pub(super) static WASM_CTX: RefCell<Option<HashMap<String, Function>>> = RefCell::new(None);
    }

    pub fn install_context(callbacks: HashMap<String, Function>) -> ContextGuard {
        WASM_CTX.with(|cell| {
            *cell.borrow_mut() = Some(callbacks);
        });
        ContextGuard
    }

    pub struct ContextGuard;

    impl Drop for ContextGuard {
        fn drop(&mut self) {
            WASM_CTX.with(|cell| {
                *cell.borrow_mut() = None;
            });
        }
    }

    pub fn call_js_batch(
        reducer_id: &str,
        block: &mut GroupedRows<'_>,
        fields: &ColumnRegistry,
        emits: &mut Emits,
    ) -> Result<(), String> {
        WASM_CTX.with(|cell| {
            let ctx = cell.borrow();
            let ctx = ctx
                .as_ref()
                .ok_or("external reducer called outside a wasm call")?;

            let func = ctx
                .get(reducer_id)
                .ok_or_else(|| format!("no callback for reducer '{reducer_id}'"))?;

            let js_input = groups_to_js(block, fields);
            let map_names: js_sys::Array =
                block.map_fields().iter().map(|name| JsValue::from_str(name)).collect();

            let result = with_host_maps(block, || func.call2(&JsValue::NULL, &js_input, &map_names))
                .map_err(|e| format!("the callback threw: {e:?}"))?;

            js_result_to_groups(result, block, fields, emits)
        })
    }

    fn groups_to_js(block: &GroupedRows<'_>, fields: &ColumnRegistry) -> JsValue {
        let arr = js_sys::Array::new();
        for group in &block.groups {
            let obj = js_sys::Object::new();

            js_sys::Reflect::set(
                &obj,
                &JsValue::from_str("state"),
                &state_to_js(fields, &group.state),
            )
            .unwrap();

            let rows_arr = js_sys::Array::new();
            for &index in block.row_indices(group) {
                rows_arr.push(&row_to_js(&block.rows[index]));
            }
            js_sys::Reflect::set(&obj, &JsValue::from_str("rows"), &rows_arr).unwrap();

            arr.push(&obj);
        }
        arr.into()
    }

    fn state_to_js(fields: &ColumnRegistry, state: &[Value]) -> JsValue {
        let obj = js_sys::Object::new();
        for (name, v) in fields.names().iter().zip(state) {
            js_sys::Reflect::set(&obj, &JsValue::from_str(name), &value_to_js(v)).unwrap();
        }
        obj.into()
    }

    fn row_to_js(row: &Row) -> JsValue {
        let obj = js_sys::Object::new();
        for name in row.registry().names() {
            if let Some(val) = row.get(name) {
                js_sys::Reflect::set(&obj, &JsValue::from_str(name), &value_to_js(val)).unwrap();
            }
        }
        obj.into()
    }

    pub(crate) fn value_to_js(val: &Value) -> JsValue {
        match val {
            Value::Float64(f) => JsValue::from_f64(*f),
            Value::UInt64(n) if *n <= MAX_SAFE_INTEGER => JsValue::from_f64(*n as f64),
            Value::UInt64(n) => js_sys::BigInt::from(*n).into(),
            Value::Int64(n) if n.unsigned_abs() <= MAX_SAFE_INTEGER => JsValue::from_f64(*n as f64),
            Value::Int64(n) => js_sys::BigInt::from(*n).into(),
            Value::String(s) => JsValue::from_str(s),
            Value::Boolean(b) => JsValue::from_bool(*b),
            Value::Null => JsValue::NULL,
            // The default serializer of `serde_wasm_bindgen` makes a JS Map of an object.
            Value::JSON(v) => json_to_js(v),
            _ => json_to_js(&value_to_json(val)),
        }
    }

    fn js_result_to_groups(
        result: JsValue,
        block: &mut GroupedRows<'_>,
        fields: &ColumnRegistry,
        emits: &mut Emits,
    ) -> Result<(), String> {
        let arr: js_sys::Array = result
            .dyn_into()
            .map_err(|_| "external reducer result must be an array")?;
        let GroupedRows { order, groups, .. } = block;
        for (i, group) in groups.iter_mut().enumerate() {
            let item = arr.get(i as u32);
            let item_obj: js_sys::Object = item
                .dyn_into()
                .map_err(|_| "result item must be an object")?;

            let js_state = js_sys::Reflect::get(&item_obj, &JsValue::from_str("state"))
                .map_err(|_| "the 'state' getter of a result item threw")?;
            for (value, name) in group.state.iter_mut().zip(fields.names()) {
                let js_value = js_sys::Reflect::get(&js_state, &JsValue::from_str(name))
                    .unwrap_or(JsValue::NULL);
                *value = js_to_value(js_value)?;
            }

            let rows = &order[group.rows.clone()];
            let js_emits = js_sys::Reflect::get(&item_obj, &JsValue::from_str("emits"))
                .map_err(|_| "the 'emits' getter of a result item threw")?;
            let emits_arr: js_sys::Array = js_emits
                .dyn_into()
                .map_err(|_| "emits must be an array")?;
            let js_emit_rows = js_sys::Reflect::get(&item_obj, &JsValue::from_str("emitRows"))
                .map_err(|_| "the 'emitRows' getter of a result item threw")?;
            let emit_rows: Option<js_sys::Array> = js_emit_rows.dyn_into().ok();
            for j in 0..emits_arr.length() {
                let row = match &emit_rows {
                    Some(emit_rows) => emit_rows
                        .get(j)
                        .as_f64()
                        .ok_or("'emitRows' must hold a row index per emit")?
                        as usize,
                    None => legacy_emit_row(j as usize, rows.len()),
                };
                push_host_emit(emits, rows, row, js_to_value_map(emits_arr.get(j))?)?;
            }
        }
        Ok(())
    }

    fn decimal(big: &js_sys::BigInt) -> String {
        String::from(big.to_string(10).unwrap())
    }

    /// A BigInt as UInt64, Int64 or Uint256, else as its decimal text.
    fn bigint_value(big: &js_sys::BigInt) -> Value {
        let Some(hex) = big.to_string(16).ok().map(String::from) else {
            return Value::String(decimal(big));
        };

        let (negative, digits) = match hex.strip_prefix('-') {
            Some(digits) => (true, digits),
            None => (false, hex.as_str()),
        };
        // 64-bit words, least significant first.
        let words: Option<Vec<u64>> = digits
            .as_bytes()
            .rchunks(16)
            .map(|chunk| u64::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok())
            .collect();

        words
            .and_then(|words| bigint_to_value(negative, &words))
            .unwrap_or_else(|| Value::String(decimal(big)))
    }

    /// Field `name` of `object`; Err if a getter throws.
    fn field(object: &JsValue, name: &JsValue) -> Result<JsValue, String> {
        js_sys::Reflect::get(object, name).map_err(|e| format!("a getter threw: {e:?}"))
    }

    fn js_to_value_map(val: JsValue) -> Result<RowMap, String> {
        let mut map = RowMap::new();
        if val.is_null() || val.is_undefined() {
            return Ok(map);
        }
        let obj: js_sys::Object = match val.dyn_into() {
            Ok(o) => o,
            Err(_) => return Ok(map),
        };
        let keys = js_sys::Object::keys(&obj);
        for i in 0..keys.length() {
            let key_js = keys.get(i);
            let key_str = key_js.as_string().unwrap_or_default();
            map.insert(key_str, js_to_value(field(&obj, &key_js)?)?);
        }
        Ok(map)
    }

    /// A JS value as `js_codec::napi::js_to_value` reads it. Never a Null in
    /// place of an error: a Null deletes a Map entry.
    pub(crate) fn js_to_value(val: JsValue) -> Result<Value, String> {
        if val.is_null() || val.is_undefined() {
            return Ok(Value::Null);
        }
        if let Some(b) = val.as_bool() {
            return Ok(Value::Boolean(b));
        }
        if let Some(n) = val.as_f64() {
            return Ok(Value::Float64(n));
        }
        if let Some(s) = val.as_string() {
            return Ok(Value::String(s));
        }
        if let Some(big) = val.dyn_ref::<js_sys::BigInt>() {
            return Ok(bigint_value(big));
        }
        if val.is_object() {
            return js_to_json(&val, 0).map(Value::JSON);
        }
        Ok(Value::Null)
    }

    /// A JS value as JSON, as `js_codec::napi::js_to_json` reads it.
    fn js_to_json(val: &JsValue, depth: usize) -> Result<serde_json::Value, String> {
        if depth > MAX_JSON_DEPTH {
            return Err(format!("an object nests deeper than {MAX_JSON_DEPTH} levels"));
        }
        if let Some(b) = val.as_bool() {
            return Ok(serde_json::Value::Bool(b));
        }
        if let Some(n) = val.as_f64() {
            return Ok(json_number(n));
        }
        if let Some(s) = val.as_string() {
            return Ok(serde_json::Value::String(s));
        }
        if let Some(big) = val.dyn_ref::<js_sys::BigInt>() {
            return Ok(match bigint_value(big) {
                Value::UInt64(n) => n.into(),
                Value::Int64(n) => n.into(),
                _ => serde_json::Value::String(decimal(big)),
            });
        }
        if !val.is_object() {
            return Ok(serde_json::Value::Null);
        }

        if js_sys::Array::is_array(val) {
            let items: &js_sys::Array = val.unchecked_ref();
            return items
                .iter()
                .map(|item| js_to_json(&item, depth + 1))
                .collect::<Result<_, _>>()
                .map(serde_json::Value::Array);
        }
        if let Some(bytes) = view_bytes(val)? {
            return Ok(serde_json::Value::Array(bytes.into_iter().map(Into::into).collect()));
        }

        let keys = js_sys::Object::keys(val.unchecked_ref::<js_sys::Object>());
        let mut fields = serde_json::Map::new();
        for key in keys.iter() {
            let value = js_to_json(&field(val, &key)?, depth + 1)?;
            fields.insert(key.as_string().unwrap_or_default(), value);
        }
        Ok(serde_json::Value::Object(fields))
    }

    /// The bytes of a typed array or a DataView, as `js_codec::napi` reads them.
    fn view_bytes(val: &JsValue) -> Result<Option<Vec<u8>>, String> {
        if !js_sys::ArrayBuffer::is_view(val) {
            return Ok(None);
        }

        let number = |name: &str| -> Result<u32, String> {
            let value = field(val, &JsValue::from_str(name))?;
            value.as_f64().map(|n| n as u32).ok_or_else(|| format!("'{name}' is not a number"))
        };
        let buffer = field(val, &JsValue::from_str("buffer"))?;
        let (offset, len) = (number("byteOffset")?, number("byteLength")?);
        let bytes = js_sys::Uint8Array::new_with_byte_offset_and_length(&buffer, offset, len);
        Ok(Some(bytes.to_vec()))
    }
}

#[cfg(feature = "wasm")]
pub use wasm_bridge::{ContextGuard as WasmContextGuard, install_context as install_wasm_context};
#[cfg(feature = "wasm")]
pub(crate) use wasm_bridge::{js_to_value as wasm_to_value, value_to_js as wasm_value};

// ─── Test-only context (non-napi builds) ─────────────────────────

/// A test-only callback context that lets unit tests drive `ExternalRuntime`
/// without a real JS/napi environment.
///
/// Install via `install_test_context(callback)` before calling `ingest()` or
/// `replay_reducer()`. The returned guard clears the context on drop.
#[cfg(test)]
pub mod test_bridge {
    use std::cell::RefCell;

    use super::{push_host_emit, with_host_maps};
    use crate::reducer_runtime::{Emits, GroupedRows};
    use crate::types::{ColumnRegistry, Row, RowMap, Value};

    /// A group as a host callback sees it: the state by field name, as JS
    /// does.
    pub struct HostGroup {
        pub state: RowMap,
        pub rows: Vec<Row>,
        pub emits: Vec<(usize, RowMap)>,
    }

    type TestCallback = Box<dyn Fn(&mut [HostGroup])>;

    thread_local! {
        pub(super) static TEST_CTX: RefCell<Option<TestCallback>> = RefCell::new(None);
    }

    /// RAII guard — clears the test context on drop.
    pub struct TestContextGuard;

    impl Drop for TestContextGuard {
        fn drop(&mut self) {
            TEST_CTX.with(|cell| *cell.borrow_mut() = None);
        }
    }

    /// Install a Rust callback as the `ExternalRuntime` context for the current
    /// thread. The callback receives the groups of each block, processes
    /// rows, and fills `group.emits` — identical to the JS contract. It
    /// reaches the MAP fields of group `i` through `with_host_map(i, ..)`.
    pub fn install_test_context(f: impl Fn(&mut [HostGroup]) + 'static) -> TestContextGuard {
        TEST_CTX.with(|cell| {
            *cell.borrow_mut() = Some(Box::new(f));
        });
        TestContextGuard
    }

    /// Run the test callback on the groups of `block`, with each state by
    /// name.
    pub(super) fn call(
        block: &mut GroupedRows<'_>,
        fields: &ColumnRegistry,
        emits: &mut Emits,
    ) -> Result<(), String> {
        let names = fields.names();
        let mut host: Vec<HostGroup> = block
            .groups
            .iter()
            .map(|group| HostGroup {
                state: names
                    .iter()
                    .cloned()
                    .zip(group.state.iter().cloned())
                    .collect(),
                rows: block
                    .row_indices(group)
                    .iter()
                    .map(|&i| block.rows[i].clone())
                    .collect(),
                emits: Vec::new(),
            })
            .collect();

        with_host_maps(block, || {
            TEST_CTX.with(|cell| {
                let borrow = cell.borrow();
                let cb = borrow
                    .as_ref()
                    .expect("ExternalRuntime::process_grouped() called without install_test_context");
                cb(&mut host);
            })
        });

        let GroupedRows { order, groups, .. } = block;
        for (group, mut host) in groups.iter_mut().zip(host) {
            for (value, name) in group.state.iter_mut().zip(names) {
                *value = host.state.remove(name).unwrap_or(Value::Null);
            }
            let rows = &order[group.rows.clone()];
            for (row, emit) in host.emits {
                push_host_emit(emits, rows, row, emit)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub use test_bridge::{HostGroup, TestContextGuard, install_test_context};

/// Whether the callback context on the current thread holds a callback for
/// `reducer_id`. Replay skips an external reducer without one.
pub fn callback_installed(reducer_id: &str) -> bool {
    #[cfg(feature = "napi")]
    if napi_bridge::EXTERNAL_CTX.with(|cell| {
        cell.borrow()
            .as_ref()
            .is_some_and(|ctx| ctx.has_callback(reducer_id))
    }) {
        return true;
    }
    #[cfg(feature = "wasm")]
    if wasm_bridge::WASM_CTX.with(|cell| {
        cell.borrow()
            .as_ref()
            .is_some_and(|callbacks| callbacks.contains_key(reducer_id))
    }) {
        return true;
    }
    // The test context is one callback for every reducer.
    #[cfg(test)]
    if test_bridge::TEST_CTX.with(|cell| cell.borrow().is_some()) {
        return true;
    }
    let _ = reducer_id;
    false
}

// ─── ExternalRuntime ───────────────────────────────────────────────

/// A reducer runtime whose logic lives in the host language (e.g., TypeScript).
///
/// Overrides `process_grouped` to send all groups in a single JS callback call
/// per block. The JS side iterates groups and rows, calling the user's
/// `reduce(state, row)` function, and returns updated states + emits.
pub struct ExternalRuntime {
    reducer_id: String,
}

// Safety: same reasoning as LuaRuntime — each ExternalRuntime is owned by
// exactly one ReducerEngine. process_grouped() is only called from the main
// thread during synchronous napi methods (process_batch / ingest).
unsafe impl Send for ExternalRuntime {}
unsafe impl Sync for ExternalRuntime {}

impl ExternalRuntime {
    pub fn new(reducer_id: String) -> Self {
        Self { reducer_id }
    }
}

impl ReducerRuntime for ExternalRuntime {
    fn process(
        &self,
        _state: GroupState<'_>,
        _row: &Row,
        _emits: &mut Emits,
    ) -> crate::error::Result<()> {
        panic!(
            "ExternalRuntime::process() should not be called directly; \
             use_batched_processing() returns true"
        )
    }

    fn use_batched_processing(&self) -> bool {
        true
    }

    fn missing_host_callback(&self) -> bool {
        !callback_installed(&self.reducer_id)
    }

    fn process_grouped(
        &self,
        block: &mut GroupedRows<'_>,
        fields: &ColumnRegistry,
        emits: &mut Emits,
    ) -> crate::error::Result<()> {
        if self.missing_host_callback() {
            return Err(crate::error::Error::InvalidOperation(format!(
                "external reducer '{}' has no callback: register it before the first ingest",
                self.reducer_id
            )));
        }

        #[cfg(feature = "napi")]
        let called = napi_bridge::call_js_batch(&self.reducer_id, block, fields, emits);
        #[cfg(all(feature = "wasm", not(feature = "napi")))]
        let called = wasm_bridge::call_js_batch(&self.reducer_id, block, fields, emits);
        #[cfg(all(test, not(any(feature = "napi", feature = "wasm"))))]
        let called = test_bridge::call(block, fields, emits);
        #[cfg(not(any(test, feature = "napi", feature = "wasm")))]
        let called: Result<(), String> = {
            let _ = (block, fields, emits);
            Err("ExternalRuntime requires the 'napi' or 'wasm' feature".to_string())
        };

        called.map_err(|e| {
            crate::error::Error::Reducer(format!("external reducer '{}': {e}", self.reducer_id))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_emit_rows_follow_the_rows() {
        let rows: Vec<usize> = (0..4).map(|i| legacy_emit_row(i, 2)).collect();
        assert_eq!(rows, [0, 1, 1, 1]);
    }
}
