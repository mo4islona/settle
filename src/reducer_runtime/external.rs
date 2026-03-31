//! ExternalRuntime — reducer logic provided by the host language via napi callback.
//!
//! Uses a thread-local "callback slot" to bridge the gap between the
//! `ReducerRuntime` trait (which requires Send + Sync) and napi's `JsFunction`
//! (which is not Send). The slot is installed by the napi layer before each
//! `process_batch`/`ingest` call and cleared after via an RAII guard.
//!
//! The batched path sends all groups for a block in a single JS call,
//! avoiding per-row FFI overhead.

use std::collections::HashMap;

use crate::types::{Row, RowMap, Value};

use super::{GroupBatch, ReducerRuntime};

// ─── Thread-local callback context (napi only) ────────────────────

#[cfg(feature = "napi")]
mod napi_bridge {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use napi::NapiValue;
    use napi::sys;

    use crate::json_conv::value_to_json;
    use crate::types::{Row, Value};

    use super::GroupBatch;

    /// Raw napi env + callback pointers. Stored in thread-local.
    /// We store raw pointers because napi::Ref<()> is not Clone.
    pub(crate) struct ExternalContext {
        env: napi::Env,
        /// reducer_id → raw napi_ref (prevent GC)
        callbacks: HashMap<String, sys::napi_ref>,
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

    /// Called by ExternalRuntime::process_grouped to invoke the JS callback.
    pub fn call_js_batch(reducer_id: &str, groups: &mut [GroupBatch]) {
        EXTERNAL_CTX.with(|cell| {
            let ctx = cell.borrow();
            let ctx = ctx
                .as_ref()
                .expect("ExternalRuntime called outside napi context");

            let raw_ref = ctx
                .callbacks
                .get(reducer_id)
                .unwrap_or_else(|| panic!("no callback for reducer '{}'", reducer_id));

            let env = ctx.env;

            // Get JsFunction from raw ref
            let mut raw_value: sys::napi_value = std::ptr::null_mut();
            unsafe {
                let status = sys::napi_get_reference_value(env.raw(), *raw_ref, &mut raw_value);
                assert_eq!(status, sys::Status::napi_ok, "failed to get callback ref");
            }
            let js_fn = unsafe { napi::JsFunction::from_raw_unchecked(env.raw(), raw_value) };

            // Marshal groups → JS
            let js_input = groups_to_js(&env, groups);

            // Single JS call
            let js_result = js_fn
                .call(None, &[js_input])
                .expect("external reducer callback threw an exception");

            // Unmarshal result
            js_result_to_groups(&env, js_result, groups);
        });
    }

    // ── Marshalling: Rust → JS ─────────────────────────────────────

    fn groups_to_js(env: &napi::Env, groups: &[GroupBatch]) -> napi::JsObject {
        let mut js_array = env.create_array_with_length(groups.len()).unwrap();
        for (i, group) in groups.iter().enumerate() {
            let mut group_obj = env.create_object().unwrap();

            let state_obj = value_map_to_js(env, &group.state);
            group_obj.set_named_property("state", state_obj).unwrap();

            let mut rows_array = env.create_array_with_length(group.rows.len()).unwrap();
            for (j, row) in group.rows.iter().enumerate() {
                let row_obj = row_to_js(env, row);
                rows_array.set_element(j as u32, row_obj).unwrap();
            }
            group_obj.set_named_property("rows", rows_array).unwrap();

            js_array.set_element(i as u32, group_obj).unwrap();
        }
        js_array
    }

    fn value_map_to_js(env: &napi::Env, map: &HashMap<String, Value>) -> napi::JsObject {
        let mut obj = env.create_object().unwrap();
        for (key, val) in map {
            let js_val = value_to_napi(env, val);
            obj.set_named_property(key, js_val).unwrap();
        }
        obj
    }

    fn row_to_js(env: &napi::Env, row: &Row) -> napi::JsObject {
        let mut obj = env.create_object().unwrap();
        let registry = row.registry();
        for name in registry.names() {
            if let Some(val) = row.get(name) {
                let js_val = value_to_napi(env, val);
                obj.set_named_property(name, js_val).unwrap();
            }
        }
        obj
    }

    fn value_to_napi(env: &napi::Env, val: &Value) -> napi::JsUnknown {
        match val {
            Value::Float64(f) => env.create_double(*f).unwrap().into_unknown(),
            Value::UInt64(n) => env.create_double(*n as f64).unwrap().into_unknown(),
            Value::Int64(n) => env.create_double(*n as f64).unwrap().into_unknown(),
            Value::String(s) => env.create_string(s).unwrap().into_unknown(),
            Value::Boolean(b) => env.get_boolean(*b).unwrap().into_unknown(),
            Value::Null => env.get_null().unwrap().into_unknown(),
            Value::JSON(v) => env.to_js_value(v).unwrap(),
            _ => {
                let json = value_to_json(val);
                env.to_js_value(&json).unwrap()
            }
        }
    }

    // ── Marshalling: JS → Rust ─────────────────────────────────────

    fn js_result_to_groups(env: &napi::Env, result: napi::JsUnknown, groups: &mut [GroupBatch]) {
        let result_obj: napi::JsObject = result
            .coerce_to_object()
            .expect("external reducer must return an array");

        for (i, group) in groups.iter_mut().enumerate() {
            let group_result: napi::JsObject = result_obj
                .get_element::<napi::JsObject>(i as u32)
                .expect("result array too short");

            // Parse state
            let js_state: napi::JsObject = group_result
                .get_named_property("state")
                .expect("result group missing 'state'");
            group.state = js_object_to_value_map(env, &js_state);

            // Parse emits
            let js_emits: napi::JsObject = group_result
                .get_named_property("emits")
                .expect("result group missing 'emits'");
            let emits_len = js_emits.get_array_length().expect("emits must be an array");

            for j in 0..emits_len {
                let js_emit: napi::JsObject =
                    js_emits.get_element(j).expect("emit element missing");
                let emit_map = js_object_to_value_map(env, &js_emit);
                group.emits.push(emit_map);
            }
        }
    }

    fn js_object_to_value_map(env: &napi::Env, obj: &napi::JsObject) -> HashMap<String, Value> {
        let mut map = HashMap::new();

        let keys = obj.get_property_names().expect("failed to get object keys");
        let len = keys.get_array_length().expect("keys not an array");

        for i in 0..len {
            let key: napi::JsString = keys.get_element(i).expect("key not a string");
            let key_str = key
                .into_utf8()
                .expect("key not valid utf8")
                .as_str()
                .expect("key str failed")
                .to_string();

            let val: napi::JsUnknown = obj
                .get_named_property(&key_str)
                .expect("failed to get property");

            let value = napi_to_value(env, val);
            map.insert(key_str, value);
        }

        map
    }

    fn napi_to_value(env: &napi::Env, val: napi::JsUnknown) -> Value {
        use napi::ValueType;

        match val.get_type().unwrap() {
            ValueType::Number => {
                let n = val.coerce_to_number().unwrap().get_double().unwrap();
                Value::Float64(n)
            }
            ValueType::String => {
                let s = val
                    .coerce_to_string()
                    .unwrap()
                    .into_utf8()
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string();
                Value::String(s)
            }
            ValueType::Boolean => {
                let b = val.coerce_to_bool().unwrap().get_value().unwrap();
                Value::Boolean(b)
            }
            ValueType::Null | ValueType::Undefined => Value::Null,
            ValueType::Object => {
                // Preserve nested objects/arrays as Value::JSON (not Value::String)
                let json: serde_json::Value =
                    env.from_js_value(val).unwrap_or(serde_json::Value::Null);
                Value::JSON(json)
            }
            _ => Value::Null,
        }
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

    use crate::json_conv::value_to_json;
    use crate::types::{Row, Value};

    use super::GroupBatch;

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

    pub fn call_js_batch(reducer_id: &str, groups: &mut [GroupBatch]) {
        WASM_CTX.with(|cell| {
            let ctx = cell.borrow();
            let ctx = ctx
                .as_ref()
                .expect("ExternalRuntime called outside wasm context");

            let func = ctx
                .get(reducer_id)
                .unwrap_or_else(|| panic!("no callback for reducer '{reducer_id}'"));

            let js_input = groups_to_js(groups);

            let result = func
                .call1(&JsValue::NULL, &js_input)
                .expect("external reducer callback threw an exception");

            js_result_to_groups(result, groups);
        });
    }

    fn groups_to_js(groups: &[GroupBatch]) -> JsValue {
        let arr = js_sys::Array::new();
        for group in groups {
            let obj = js_sys::Object::new();

            js_sys::Reflect::set(
                &obj,
                &JsValue::from_str("state"),
                &value_map_to_js(&group.state),
            )
            .unwrap();

            let rows_arr = js_sys::Array::new();
            for row in &group.rows {
                rows_arr.push(&row_to_js(row));
            }
            js_sys::Reflect::set(&obj, &JsValue::from_str("rows"), &rows_arr).unwrap();

            arr.push(&obj);
        }
        arr.into()
    }

    fn value_map_to_js(map: &HashMap<String, Value>) -> JsValue {
        let obj = js_sys::Object::new();
        for (k, v) in map {
            js_sys::Reflect::set(&obj, &JsValue::from_str(k), &value_to_js(v)).unwrap();
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
            Value::UInt64(n) => JsValue::from_f64(*n as f64),
            Value::Int64(n) => JsValue::from_f64(*n as f64),
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

    fn js_result_to_groups(result: JsValue, groups: &mut [GroupBatch]) {
        let arr: js_sys::Array = result
            .dyn_into()
            .expect("external reducer result must be an array");
        for (i, group) in groups.iter_mut().enumerate() {
            let item = arr.get(i as u32);
            let item_obj: js_sys::Object = item.dyn_into().expect("result item must be an object");

            let js_state = js_sys::Reflect::get(&item_obj, &JsValue::from_str("state")).unwrap();
            group.state = js_to_value_map(js_state);

            let js_emits = js_sys::Reflect::get(&item_obj, &JsValue::from_str("emits")).unwrap();
            let emits_arr: js_sys::Array = js_emits.dyn_into().expect("emits must be an array");
            for j in 0..emits_arr.length() {
                group.emits.push(js_to_value_map(emits_arr.get(j)));
            }
        }
    }

    fn js_to_value_map(val: JsValue) -> HashMap<String, Value> {
        let mut map = HashMap::new();
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

    use super::GroupBatch;

    type TestCallback = Box<dyn Fn(&mut [GroupBatch])>;

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
    /// thread. The callback receives the `GroupBatch` slice for each block,
    /// processes rows, and fills `group.emits` — identical to the JS contract.
    pub fn install_test_context(f: impl Fn(&mut [GroupBatch]) + 'static) -> TestContextGuard {
        TEST_CTX.with(|cell| {
            *cell.borrow_mut() = Some(Box::new(f));
        });
        TestContextGuard
    }
}

#[cfg(test)]
pub use test_bridge::{TestContextGuard, install_test_context};

/// Returns true when a JS (or test) callback context is installed on the
/// current thread. Used by `replay_unfinalized` to decide whether to skip
/// external reducers — false means "no host yet, skip to avoid panic".
pub fn context_installed() -> bool {
    #[cfg(feature = "napi")]
    if napi_bridge::EXTERNAL_CTX.with(|cell| cell.borrow().is_some()) {
        return true;
    }
    #[cfg(feature = "wasm")]
    if wasm_bridge::WASM_CTX.with(|cell| cell.borrow().is_some()) {
        return true;
    }
    #[cfg(test)]
    if test_bridge::TEST_CTX.with(|cell| cell.borrow().is_some()) {
        return true;
    }
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
        _state: &mut HashMap<String, Value>,
        _row: &Row,
    ) -> crate::error::Result<Vec<RowMap>> {
        panic!(
            "ExternalRuntime::process() should not be called directly; \
             use_batched_processing() returns true"
        )
    }

    fn use_batched_processing(&self) -> bool {
        true
    }

    fn process_grouped(&self, groups: &mut [GroupBatch]) -> crate::error::Result<()> {
        #[cfg(feature = "napi")]
        {
            napi_bridge::call_js_batch(&self.reducer_id, groups);
            return Ok(());
        }
        #[cfg(all(feature = "wasm", not(feature = "napi")))]
        {
            wasm_bridge::call_js_batch(&self.reducer_id, groups);
            return Ok(());
        }
        #[cfg(test)]
        {
            test_bridge::TEST_CTX.with(|cell| {
                let borrow = cell.borrow();
                let cb = borrow.as_ref().expect(
                    "ExternalRuntime::process_grouped() called without install_test_context",
                );
                cb(groups);
            });
            return Ok(());
        }
        #[cfg(not(any(feature = "napi", feature = "wasm")))]
        {
            let _ = groups;
            panic!("ExternalRuntime requires the 'napi' or 'wasm' feature");
        }
        #[allow(unreachable_code)]
        Ok(())
    }
}
