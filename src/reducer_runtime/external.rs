//! ExternalRuntime — reducer logic provided by the host language via napi callback.
//!
//! Uses a thread-local "callback slot" to bridge the gap between the
//! `ReducerRuntime` trait (which requires Send + Sync) and napi's `JsFunction`
//! (which is not Send). The slot is installed by the napi layer before each
//! `process_batch`/`ingest` call and cleared after via an RAII guard.
//!
//! The batched path sends all groups for a block in a single JS call,
//! avoiding per-row FFI overhead.

use crate::types::{ColumnRegistry, Row};

use super::{Emits, GroupState, GroupedRows, ReducerRuntime};

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
            let status = unsafe {
                sys::napi_call_function(raw, this, callback, args.len(), args.as_ptr(), &mut result)
            };
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

    use crate::js_codec::{MAX_SAFE_INTEGER, bigint_to_value};
    use crate::json_conv::value_to_json;
    use crate::reducer_runtime::{Emits, GroupedRows};
    use crate::types::{ColumnRegistry, Row, RowMap, Value};

    use super::{legacy_emit_row, push_host_emit};

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

            let result = func
                .call1(&JsValue::NULL, &js_input)
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

    fn value_to_js(val: &Value) -> JsValue {
        match val {
            Value::Float64(f) => JsValue::from_f64(*f),
            Value::UInt64(n) if *n <= MAX_SAFE_INTEGER => JsValue::from_f64(*n as f64),
            Value::UInt64(n) => js_sys::BigInt::from(*n).into(),
            Value::Int64(n) if n.unsigned_abs() <= MAX_SAFE_INTEGER => JsValue::from_f64(*n as f64),
            Value::Int64(n) => js_sys::BigInt::from(*n).into(),
            Value::String(s) => JsValue::from_str(s),
            Value::Boolean(b) => JsValue::from_bool(*b),
            Value::Null => JsValue::NULL,
            Value::JSON(v) => serde_wasm_bindgen::to_value(v).unwrap_or(JsValue::NULL),
            _ => {
                let json = value_to_json(val);
                serde_wasm_bindgen::to_value(&json).unwrap_or(JsValue::NULL)
            }
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

            let js_state = js_sys::Reflect::get(&item_obj, &JsValue::from_str("state")).unwrap();
            for (value, name) in group.state.iter_mut().zip(fields.names()) {
                let js_value = js_sys::Reflect::get(&js_state, &JsValue::from_str(name))
                    .unwrap_or(JsValue::NULL);
                *value = js_to_value(js_value);
            }

            let rows = &order[group.rows.clone()];
            let js_emits = js_sys::Reflect::get(&item_obj, &JsValue::from_str("emits")).unwrap();
            let emits_arr: js_sys::Array = js_emits
                .dyn_into()
                .map_err(|_| "emits must be an array")?;
            let js_emit_rows =
                js_sys::Reflect::get(&item_obj, &JsValue::from_str("emitRows")).unwrap();
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
                push_host_emit(emits, rows, row, js_to_value_map(emits_arr.get(j)))?;
            }
        }
        Ok(())
    }

    /// A BigInt as UInt64, Int64 or Uint256, else as its decimal text.
    fn bigint_value(big: &js_sys::BigInt) -> Value {
        let decimal = || String::from(big.to_string(10).unwrap());
        let Some(hex) = big.to_string(16).ok().map(String::from) else {
            return Value::String(decimal());
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
            .unwrap_or_else(|| Value::String(decimal()))
    }

    fn js_to_value_map(val: JsValue) -> RowMap {
        let mut map = RowMap::new();
        if val.is_null() || val.is_undefined() {
            return map;
        }
        let obj: js_sys::Object = match val.dyn_into() {
            Ok(o) => o,
            Err(_) => return map,
        };
        let keys = js_sys::Object::keys(&obj);
        for i in 0..keys.length() {
            let key_js = keys.get(i);
            let key_str = key_js.as_string().unwrap_or_default();
            let v = js_sys::Reflect::get(&obj, &key_js).unwrap_or(JsValue::NULL);
            map.insert(key_str, js_to_value(v));
        }
        map
    }

    fn js_to_value(val: JsValue) -> Value {
        if val.is_null() || val.is_undefined() {
            return Value::Null;
        }
        if let Some(b) = val.as_bool() {
            return Value::Boolean(b);
        }
        if let Some(n) = val.as_f64() {
            return Value::Float64(n);
        }
        if let Some(s) = val.as_string() {
            return Value::String(s);
        }
        if let Some(big) = val.dyn_ref::<js_sys::BigInt>() {
            return bigint_value(big);
        }
        match serde_wasm_bindgen::from_value::<serde_json::Value>(val) {
            Ok(json) => Value::JSON(json),
            Err(_) => Value::Null,
        }
    }
}

#[cfg(feature = "wasm")]
pub use wasm_bridge::{ContextGuard as WasmContextGuard, install_context as install_wasm_context};

// ─── Test-only context (non-napi builds) ─────────────────────────

/// A test-only callback context that lets unit tests drive `ExternalRuntime`
/// without a real JS/napi environment.
///
/// Install via `install_test_context(callback)` before calling `ingest()` or
/// `replay_reducer()`. The returned guard clears the context on drop.
#[cfg(test)]
pub mod test_bridge {
    use std::cell::RefCell;

    use super::push_host_emit;
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
    /// rows, and fills `group.emits` — identical to the JS contract.
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

        TEST_CTX.with(|cell| {
            let borrow = cell.borrow();
            let cb = borrow
                .as_ref()
                .expect("ExternalRuntime::process_grouped() called without install_test_context");
            cb(&mut host);
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

    /// The JS and WASM bridges carry no MAP fields yet.
    fn supports_maps(&self) -> bool {
        false
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
