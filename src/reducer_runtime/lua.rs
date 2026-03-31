use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use mlua::{Lua, MultiValue, RegistryKey, Result as LuaResult, Value as LuaValue};

use crate::types::{ColumnType, Row, RowMap, Value};

use super::ReducerRuntime;

/// Lua runtime for reducer process functions.
///
/// The Lua VM is created once and reused across all `process()` calls.
/// Key performance optimizations:
/// - State/row tables are persistent globals, updated in-place (no alloc per row)
/// - Emit metatable created once at init, fields cleared per row via pre-compiled fn
/// - User script wrapped in a closure, called via pre-compiled wrapper
/// - When state_fields are known, state is passed/returned as positional function
///   args/returns — eliminates per-field C API table.set/table.get calls (~1.3us/row savings)
/// - Only row fields actually referenced in the Lua script are marshalled (skip unused columns)
/// - Pre-computed column IDs enable direct Vec indexing into Row.values()
pub struct LuaRuntime {
    lua: Lua,
    /// Pre-compiled wrapper: calls user function, returns state fields
    call_key: RegistryKey,
    /// Registry handle to the persistent `state` global table
    state_key: RegistryKey,
    /// Registry handle to the persistent `row` global table
    row_key: RegistryKey,
    /// Ordered state field names for positional arg passing (empty = use generic path)
    state_fields: Vec<String>,
    /// State field types — used to know which fields should be read back as JSON
    state_types: HashMap<String, ColumnType>,
    /// Row field names actually referenced by the Lua script.
    /// Used to filter row.iter() — only matching fields are marshalled to Lua.
    /// Empty = no filtering (all non-null fields are set).
    accessed_fields: HashSet<String>,
    /// Rust-side emit buffer. Each `emit({...})` call from Lua is serialized
    /// immediately into this Vec, avoiding any Lua-side table accumulation.
    emits: Rc<RefCell<Vec<RowMap>>>,
}

// Safety: Each LuaRuntime is owned by exactly one ReducerEngine.
// ReducerEngine::process_block takes &mut self, guaranteeing that
// The Lua VM is a self-contained C state that is safe to move between
// threads as long as it is not accessed concurrently.
unsafe impl Send for LuaRuntime {}

impl LuaRuntime {
    /// Create a new LuaRuntime without state field or source column optimization.
    /// State is synced via per-field table.get/set calls. All row fields are marshalled.
    pub fn new(script: &str) -> Self {
        Self::with_state_fields(script, &[], &[], &[], &[])
    }

    /// Create a LuaRuntime with known state field names, types, source columns, and modules.
    /// State values are passed as function arguments and returned as return values,
    /// eliminating per-field C API calls (~1.3us/row savings for 6-field state).
    /// Source columns enable selective row field marshalling — only fields referenced
    /// in the Lua script are set on the row table, skipping unused columns.
    /// Modules are loaded before the user script, each executed once with its return
    /// value stored as a global with the module name.
    pub fn with_state_fields(
        script: &str,
        state_fields: &[String],
        state_types: &[(String, ColumnType)],
        source_columns: &[String],
        modules: &[(String, String)],
    ) -> Self {
        let lua = Lua::new();

        // Sandbox: remove dangerous globals
        sandbox(&lua).expect("failed to sandbox Lua");

        // Provide json module
        register_json_module(&lua).expect("failed to register json module");

        // Load required modules: execute each script once, store return value as global
        for (name, code) in modules {
            let result: LuaValue = lua
                .load(code.as_str())
                .eval()
                .unwrap_or_else(|e| panic!("failed to load module '{name}': {e}"));
            lua.globals()
                .set(name.as_str(), result)
                .unwrap_or_else(|e| panic!("failed to set module global '{name}': {e}"));
        }

        // Create persistent state and row tables as globals
        let state_table = lua.create_table().expect("failed to create state table");
        lua.globals()
            .set("state", state_table.clone())
            .expect("failed to set state global");
        let state_key = lua
            .create_registry_value(state_table)
            .expect("failed to store state in registry");

        let row_table = lua.create_table().expect("failed to create row table");
        lua.globals()
            .set("row", row_table.clone())
            .expect("failed to set row global");
        let row_key = lua
            .create_registry_value(row_table)
            .expect("failed to store row in registry");

        // Register `emit` as a Rust callback. Each emit({...}) call is serialized
        // immediately into the Rust-side buffer — no Lua table accumulation.
        let emits: Rc<RefCell<Vec<RowMap>>> = Rc::new(RefCell::new(Vec::new()));
        let emits_for_lua = emits.clone();
        let emit_fn = lua
            .create_function(move |_lua, tbl: mlua::Table| {
                let map = lua_table_to_value_map(&tbl)?;
                if !map.is_empty() {
                    emits_for_lua.borrow_mut().push(map);
                }
                Ok(())
            })
            .expect("failed to create emit function");
        lua.globals()
            .set("emit", emit_fn)
            .expect("failed to set emit global");

        // Pre-compile wrapper: calls user function and returns state fields.
        // Using string concat (not format!) to avoid issues with {} in Lua scripts.
        let call_src = if state_fields.is_empty() {
            // Generic path: no state field optimization.
            [
                "local _user = function()\n",
                script,
                "\nend\n",
                "return function()\n",
                "    _user()\n",
                "end\n",
            ]
            .concat()
        } else {
            // Specialized path: state passed as positional args, returned as extra values.
            let n = state_fields.len();
            let params: Vec<String> = (0..n).map(|i| format!("_s{i}")).collect();
            let param_list = params.join(", ");

            let mut set_lines = String::new();
            for (i, field) in state_fields.iter().enumerate() {
                set_lines.push_str(&format!("    state[\"{field}\"] = _s{i}\n"));
            }

            let return_fields: Vec<String> = state_fields
                .iter()
                .map(|f| format!("state[\"{f}\"]"))
                .collect();
            let return_list = return_fields.join(", ");

            let mut parts = Vec::new();
            parts.push("local _user = function()\n".to_string());
            parts.push(script.to_string());
            parts.push("\nend\n".to_string());
            parts.push(format!("return function({param_list})\n"));
            parts.push(set_lines);
            parts.push("    _user()\n".to_string());
            parts.push(format!("    return {return_list}\n"));
            parts.push("end\n".to_string());
            parts.concat()
        };

        let call_func: mlua::Function = lua
            .load(&call_src)
            .eval()
            .expect("failed to compile Lua wrapper");
        let call_key = lua
            .create_registry_value(call_func)
            .expect("failed to store call function in registry");

        // Determine which row fields the script actually accesses.
        // Only those fields will be marshalled to Lua per row.
        let accessed_fields = compute_accessed_fields(script, source_columns);

        Self {
            lua,
            call_key,
            state_key,
            row_key,
            state_fields: state_fields.to_vec(),
            state_types: state_types.iter().cloned().collect(),
            accessed_fields,
            emits,
        }
    }
}

impl ReducerRuntime for LuaRuntime {
    fn process(
        &self,
        state: &mut HashMap<String, Value>,
        row: &Row,
    ) -> crate::error::Result<Vec<RowMap>> {
        let lua_err = |e: mlua::Error| crate::error::Error::Reducer(e.to_string());

        let row_table: mlua::Table = self.lua.registry_value(&self.row_key).map_err(lua_err)?;
        marshal_row_to_lua(&self.lua, &row_table, row, &self.accessed_fields)
            .map_err(lua_err)?;

        let call: mlua::Function = self.lua.registry_value(&self.call_key).map_err(lua_err)?;

        // Clear the emit buffer before calling user script
        self.emits.borrow_mut().clear();

        if self.state_fields.is_empty() {
            let state_table: mlua::Table =
                self.lua.registry_value(&self.state_key).map_err(lua_err)?;
            serialize_state_generic(&self.lua, &state_table, state).map_err(lua_err)?;

            lua_call_generic(&call).map_err(lua_err)?;

            deserialize_state_generic(&state_table, state, &self.state_types)
                .map_err(lua_err)?;
        } else {
            let args =
                serialize_state_hoisted(&self.lua, &self.state_fields, state).map_err(lua_err)?;

            let result =
                lua_call_hoisted(&call, args).map_err(lua_err)?;

            deserialize_state_hoisted(result, &self.state_fields, state, &self.state_types);
        }

        Ok(std::mem::take(&mut *self.emits.borrow_mut()))
    }
}

// --- Perf-visible phases: each #[inline(never)] fn is a distinct frame in perf/flamegraph ---

/// Phase 1: Marshal Row (Rust) → Lua row table
#[inline(never)]
fn marshal_row_to_lua(
    lua: &Lua,
    row_table: &mlua::Table,
    row: &Row,
    accessed_fields: &HashSet<String>,
) -> LuaResult<()> {
    row_table.clear()?;
    if accessed_fields.is_empty() {
        for (k, v) in row.iter() {
            row_table.raw_set(k, value_to_lua(lua, v)?)?;
        }
    } else {
        for (k, v) in row.iter() {
            if accessed_fields.contains(k) {
                row_table.raw_set(k, value_to_lua(lua, v)?)?;
            }
        }
    }
    Ok(())
}

/// Phase 2a: Serialize state → Lua table (generic path)
#[inline(never)]
fn serialize_state_generic(
    lua: &Lua,
    state_table: &mlua::Table,
    state: &HashMap<String, Value>,
) -> LuaResult<()> {
    for (k, v) in state.iter() {
        state_table.raw_set(k.as_str(), value_to_lua(lua, v)?)?;
    }
    Ok(())
}

/// Phase 2b: Serialize state → MultiValue args (hoisted path)
#[inline(never)]
fn serialize_state_hoisted(
    lua: &Lua,
    state_fields: &[String],
    state: &HashMap<String, Value>,
) -> LuaResult<MultiValue> {
    let mut args = MultiValue::new();
    for field in state_fields {
        let v = state.get(field).unwrap_or(&Value::Null);
        args.push_back(value_to_lua(lua, v)?);
    }
    Ok(args)
}

/// Phase 3a: Call Lua function (generic path — no args)
#[inline(never)]
fn lua_call_generic(call: &mlua::Function) -> LuaResult<()> {
    call.call::<()>(())
}

/// Phase 3b: Call Lua function (hoisted path — state as args, returns state)
#[inline(never)]
fn lua_call_hoisted(call: &mlua::Function, args: MultiValue) -> LuaResult<MultiValue> {
    call.call(args)
}

/// Phase 4a: Deserialize state ← Lua table (generic path)
#[inline(never)]
fn deserialize_state_generic(
    state_table: &mlua::Table,
    state: &mut HashMap<String, Value>,
    state_types: &HashMap<String, ColumnType>,
) -> LuaResult<()> {
    for (k, v) in state.iter_mut() {
        let lua_val: LuaValue = state_table.raw_get(k.as_str())?;
        *v = lua_to_value_typed(&lua_val, state_types.get(k));
    }
    Ok(())
}

/// Phase 4b: Deserialize state ← MultiValue returns (hoisted path)
#[inline(never)]
fn deserialize_state_hoisted(
    result: MultiValue,
    state_fields: &[String],
    state: &mut HashMap<String, Value>,
    state_types: &HashMap<String, ColumnType>,
) {
    let mut iter = result.into_vec().into_iter();
    for field in state_fields {
        if let Some(lua_val) = iter.next() {
            if let Some(v) = state.get_mut(field) {
                *v = lua_to_value_typed(&lua_val, state_types.get(field));
            }
        }
    }
}

/// Determine which row fields the Lua script accesses.
/// Returns empty HashSet if source_columns is empty or all fields are accessed
/// (no filtering needed). Otherwise returns just the accessed field names.
fn compute_accessed_fields(script: &str, source_columns: &[String]) -> HashSet<String> {
    if source_columns.is_empty() {
        return HashSet::new();
    }

    match extract_row_field_refs(script) {
        Some(accessed) if !accessed.is_empty() && accessed.len() + 1 < source_columns.len() => {
            // Worthwhile to filter: script skips 2+ fields.
            // Skipping only 1 field doesn't offset the HashSet lookup overhead.
            accessed
        }
        _ => {
            // Script uses dynamic access, no refs detected, or accesses all fields
            HashSet::new()
        }
    }
}

/// Extract row field names referenced in a Lua script via `row.field` patterns.
/// Returns None if the script uses dynamic row iteration (pairs/next/ipairs).
fn extract_row_field_refs(script: &str) -> Option<HashSet<String>> {
    // Dynamic iteration over row — can't determine fields statically
    if script.contains("pairs(row)")
        || script.contains("next(row")
        || script.contains("ipairs(row)")
    {
        return None;
    }

    let mut fields = HashSet::new();
    let bytes = script.as_bytes();
    let mut i = 0;

    while i + 4 < bytes.len() {
        // Match "row." where preceding char is not alphanumeric/_
        if bytes[i] == b'r'
            && bytes[i + 1] == b'o'
            && bytes[i + 2] == b'w'
            && bytes[i + 3] == b'.'
            && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_'))
        {
            i += 4;
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            if i > start {
                fields.insert(String::from_utf8_lossy(&bytes[start..i]).to_string());
            }
            continue;
        }
        i += 1;
    }

    Some(fields)
}

fn sandbox(lua: &Lua) -> LuaResult<()> {
    let globals = lua.globals();
    // Remove dangerous modules/functions
    for name in &[
        "os", "io", "debug", "loadfile", "dofile", "load", "rawget", "rawset", "require", "package",
    ] {
        globals.set(*name, mlua::Value::Nil)?;
    }
    Ok(())
}

fn register_json_module(lua: &Lua) -> LuaResult<()> {
    let json_table = lua.create_table()?;

    // json.encode(value) -> string
    let encode = lua.create_function(|lua, val: LuaValue| {
        let s = lua_value_to_json_string(lua, &val);
        Ok(s)
    })?;
    json_table.set("encode", encode)?;

    // json.decode(string) -> value
    let decode = lua.create_function(|lua, s: String| {
        let val: serde_json::Value =
            serde_json::from_str(&s).map_err(|e| mlua::Error::external(e))?;
        json_value_to_lua(lua, &val)
    })?;
    json_table.set("decode", decode)?;

    lua.globals().set("json", json_table)?;
    Ok(())
}

fn lua_table_to_value_map(table: &mlua::Table) -> LuaResult<HashMap<String, Value>> {
    let mut map = HashMap::new();
    table.for_each(|k: String, v: LuaValue| {
        map.insert(k, lua_to_value(&v));
        Ok(())
    })?;
    Ok(map)
}

fn value_to_lua(lua: &Lua, val: &Value) -> LuaResult<LuaValue> {
    match val {
        Value::UInt64(v) => {
            if *v <= i64::MAX as u64 {
                Ok(LuaValue::Integer(*v as i64))
            } else {
                // Values > i64::MAX cannot be represented as Lua integer; use f64 (lossy for > 2^53)
                Ok(LuaValue::Number(*v as f64))
            }
        }
        Value::Int64(v) => Ok(LuaValue::Integer(*v)),
        Value::Float64(v) => Ok(LuaValue::Number(*v)),
        Value::String(v) => Ok(LuaValue::String(lua.create_string(v)?)),
        Value::DateTime(v) => Ok(LuaValue::Number(*v as f64)),
        Value::Boolean(v) => Ok(LuaValue::Boolean(*v)),
        Value::Null => Ok(LuaValue::Nil),
        Value::Bytes(v) => Ok(LuaValue::String(lua.create_string(v)?)),
        Value::Uint256(v) => Ok(LuaValue::String(lua.create_string(v.as_slice())?)),
        Value::Base58(v) => Ok(LuaValue::String(lua.create_string(v)?)),
        Value::JSON(v) => json_value_to_lua(lua, v),
    }
}

fn lua_to_value(val: &LuaValue) -> Value {
    match val {
        LuaValue::Number(n) => {
            // If it's a whole number, store as Float64 for consistency
            Value::Float64(*n)
        }
        LuaValue::Integer(n) => Value::Int64(*n),
        LuaValue::String(s) => Value::String(lua_string_to_rust(s)),
        LuaValue::Boolean(b) => Value::Boolean(*b),
        LuaValue::Nil => Value::Null,
        _ => Value::Null,
    }
}

/// Read back a Lua value with type hint. When the column type is JSON,
/// Lua tables are converted directly to `Value::JSON(serde_json::Value)`
/// instead of being dropped as Null.
fn lua_to_value_typed(val: &LuaValue, col_type: Option<&ColumnType>) -> Value {
    match val {
        LuaValue::Table(t) if col_type == Some(&ColumnType::JSON) => {
            Value::JSON(lua_table_to_json(t))
        }
        other => lua_to_value(other),
    }
}

/// Convert a Lua table to serde_json::Value (recursive).
fn lua_table_to_json(table: &mlua::Table) -> serde_json::Value {
    if is_lua_array(table) {
        let mut arr = Vec::new();
        let _ = table.for_each_value::<LuaValue>(|v| {
            arr.push(lua_value_to_json(&v));
            Ok(())
        });
        serde_json::Value::Array(arr)
    } else {
        let mut map = serde_json::Map::new();
        let _ = table.for_each(|k: String, v: LuaValue| {
            map.insert(k, lua_value_to_json(&v));
            Ok(())
        });
        serde_json::Value::Object(map)
    }
}

/// Convert a single Lua value to serde_json::Value (recursive for tables).
fn lua_value_to_json(val: &LuaValue) -> serde_json::Value {
    match val {
        LuaValue::Nil => serde_json::Value::Null,
        LuaValue::Boolean(b) => serde_json::Value::Bool(*b),
        LuaValue::Integer(n) => serde_json::json!(*n),
        LuaValue::Number(n) => serde_json::json!(*n),
        LuaValue::String(s) => {
            serde_json::Value::String(s.to_str().map(|s| s.to_string()).unwrap_or_default())
        }
        LuaValue::Table(t) => lua_table_to_json(t),
        _ => serde_json::Value::Null,
    }
}

fn lua_string_to_rust(s: &mlua::String) -> String {
    s.to_str().map(|s| s.to_string()).unwrap_or_default()
}

fn lua_value_to_json_string(_lua: &Lua, val: &LuaValue) -> String {
    match val {
        LuaValue::Number(n) => serde_json::to_string(n).unwrap(),
        LuaValue::Integer(n) => serde_json::to_string(n).unwrap(),
        LuaValue::String(s) => serde_json::to_string(&lua_string_to_rust(s)).unwrap(),
        LuaValue::Boolean(b) => serde_json::to_string(b).unwrap(),
        LuaValue::Nil => "null".to_string(),
        LuaValue::Table(t) => {
            // Check if it's an array or object
            if is_lua_array(t) {
                let mut arr = Vec::new();
                let _ = t.for_each_value::<LuaValue>(|v| {
                    arr.push(lua_value_to_json_string(_lua, &v));
                    Ok(())
                });
                format!("[{}]", arr.join(","))
            } else {
                let mut obj = Vec::new();
                let _ = t.for_each(|k: String, v: LuaValue| {
                    obj.push(format!(
                        "{}:{}",
                        serde_json::to_string(&k).unwrap(),
                        lua_value_to_json_string(_lua, &v)
                    ));
                    Ok(())
                });
                format!("{{{}}}", obj.join(","))
            }
        }
        _ => "null".to_string(),
    }
}

fn json_value_to_lua(lua: &Lua, val: &serde_json::Value) -> LuaResult<LuaValue> {
    match val {
        serde_json::Value::Null => Ok(LuaValue::Nil),
        serde_json::Value::Bool(b) => Ok(LuaValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(LuaValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(LuaValue::Number(f))
            } else {
                // u64 > i64::MAX: lossy but better than silent 0.0
                Ok(LuaValue::Number(n.as_u64().unwrap_or(0) as f64))
            }
        }
        serde_json::Value::String(s) => Ok(LuaValue::String(lua.create_string(s)?)),
        serde_json::Value::Array(arr) => {
            let table = lua.create_table()?;
            for (i, v) in arr.iter().enumerate() {
                table.set(i + 1, json_value_to_lua(lua, v)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        serde_json::Value::Object(obj) => {
            let table = lua.create_table()?;
            for (k, v) in obj {
                table.set(k.as_str(), json_value_to_lua(lua, v)?)?;
            }
            Ok(LuaValue::Table(table))
        }
    }
}

fn is_lua_array(table: &mlua::Table) -> bool {
    let mut has_int_keys = false;
    let mut has_str_keys = false;
    let _ = table.for_each(|k: LuaValue, _: LuaValue| {
        match k {
            LuaValue::Integer(_) | LuaValue::Number(_) => has_int_keys = true,
            _ => has_str_keys = true,
        }
        Ok(())
    });
    has_int_keys && !has_str_keys
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trade(side: &str, amount: f64, price: f64) -> Row {
        Row::from(HashMap::from([
            ("side".to_string(), Value::String(side.to_string())),
            ("amount".to_string(), Value::Float64(amount)),
            ("price".to_string(), Value::Float64(price)),
        ]))
    }

    #[test]
    fn simple_lua_reducer() {
        let runtime = LuaRuntime::new(
            r#"
            state.count = state.count + 1
            emit({total = state.count})
            "#,
        );

        let mut state = HashMap::from([("count".to_string(), Value::Float64(0.0))]);
        let row = Row::from(RowMap::new());

        let out1 = runtime
            .process(&mut state, &row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out1.get("total"), Some(&Value::Float64(1.0)));

        let out2 = runtime
            .process(&mut state, &row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out2.get("total"), Some(&Value::Float64(2.0)));
    }

    #[test]
    fn lua_reads_row_values() {
        let runtime = LuaRuntime::new(
            r#"
            emit({doubled = row.amount * 2})
            "#,
        );

        let mut state = HashMap::new();
        let row = Row::from(HashMap::from([("amount".to_string(), Value::Float64(5.0))]));

        let out = runtime
            .process(&mut state, &row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out.get("doubled"), Some(&Value::Float64(10.0)));
    }

    #[test]
    fn lua_pnl_tracker() {
        // Simplified PnL: buy increases position, sell computes pnl
        let runtime = LuaRuntime::new(
            r#"
            if row.side == "buy" then
                state.quantity = state.quantity + row.amount
                state.cost_basis = state.cost_basis + row.amount * row.price
                emit({trade_pnl = 0, position_size = state.quantity})
            else
                local avg_cost = state.cost_basis / state.quantity
                local pnl = row.amount * (row.price - avg_cost)
                state.quantity = state.quantity - row.amount
                state.cost_basis = state.cost_basis - row.amount * avg_cost
                emit({trade_pnl = pnl, position_size = state.quantity})
            end
            "#,
        );

        let mut state = HashMap::from([
            ("quantity".to_string(), Value::Float64(0.0)),
            ("cost_basis".to_string(), Value::Float64(0.0)),
        ]);

        // BUY 10 @ 2000
        let out1 = runtime
            .process(&mut state, &make_trade("buy", 10.0, 2000.0))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out1.get("trade_pnl"), Some(&Value::Int64(0)));
        assert_eq!(out1.get("position_size"), Some(&Value::Float64(10.0)));

        // BUY 5 @ 2100
        let out2 = runtime
            .process(&mut state, &make_trade("buy", 5.0, 2100.0))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out2.get("position_size"), Some(&Value::Float64(15.0)));

        // SELL 8 @ 2200
        let out3 = runtime
            .process(&mut state, &make_trade("sell", 8.0, 2200.0))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let pnl = out3.get("trade_pnl").unwrap().as_f64().unwrap();
        // 8 * (2200 - 2033.33) ≈ 1333.33
        assert!((pnl - 1333.33).abs() < 0.01);
        assert_eq!(out3.get("position_size"), Some(&Value::Float64(7.0)));
    }

    #[test]
    fn lua_json_roundtrip() {
        // Test the json module with FIFO-like lot tracking
        let runtime = LuaRuntime::new(
            r#"
            local lots = json.decode(state.lots)
            local pnl = 0
            if row.side == "buy" then
                table.insert(lots, { qty = row.amount, price = row.price })
            else
                local remaining = row.amount
                while remaining > 0 and #lots > 0 do
                    local lot = lots[1]
                    local used = math.min(remaining, lot.qty)
                    pnl = pnl + used * (row.price - lot.price)
                    lot.qty = lot.qty - used
                    remaining = remaining - used
                    if lot.qty <= 0 then table.remove(lots, 1) end
                end
            end
            state.lots = json.encode(lots)

            local total_qty = 0
            for _, lot in ipairs(lots) do
                total_qty = total_qty + lot.qty
            end
            emit({trade_pnl = pnl, position_size = total_qty})
            "#,
        );

        let mut state = HashMap::from([("lots".to_string(), Value::String("[]".to_string()))]);

        // BUY 10 @ 100
        let out1 = runtime
            .process(&mut state, &make_trade("buy", 10.0, 100.0))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out1.get("trade_pnl"), Some(&Value::Int64(0)));
        assert_eq!(out1.get("position_size"), Some(&Value::Float64(10.0)));

        // BUY 5 @ 200
        let out2 = runtime
            .process(&mut state, &make_trade("buy", 5.0, 200.0))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out2.get("position_size"), Some(&Value::Float64(15.0)));

        // SELL 12 @ 150 (FIFO: 10 @ cost=100, 2 @ cost=200)
        let out3 = runtime
            .process(&mut state, &make_trade("sell", 12.0, 150.0))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let pnl = out3.get("trade_pnl").unwrap().as_f64().unwrap();
        // 10*(150-100) + 2*(150-200) = 500 + (-100) = 400
        assert!((pnl - 400.0).abs() < 0.01);
        // Remaining: 3 lots @ 200
        assert_eq!(out3.get("position_size"), Some(&Value::Float64(3.0)));
    }

    #[test]
    fn lua_empty_emit_returns_none() {
        let runtime = LuaRuntime::new("-- do nothing");
        let mut state = HashMap::new();
        let row = Row::from(RowMap::new());
        assert!(runtime.process(&mut state, &row).unwrap().is_empty());
    }

    #[test]
    fn lua_sandbox_blocks_os() {
        let runtime = LuaRuntime::new(
            r#"
            if os == nil then
                emit({sandboxed = 1})
            end
            "#,
        );
        let mut state = HashMap::new();
        let row = Row::from(RowMap::new());
        let out = runtime
            .process(&mut state, &row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out.get("sandboxed"), Some(&Value::Int64(1)));
    }

    /// Issue #17: Sandbox must block require and package to prevent escaping.
    #[test]
    fn lua_sandbox_blocks_require_and_package() {
        let runtime = LuaRuntime::new(
            r#"
            if require == nil and package == nil then
                emit({sandboxed = 1})
            end
            "#,
        );
        let mut state = HashMap::new();
        let row = Row::from(RowMap::new());
        let out = runtime
            .process(&mut state, &row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out.get("sandboxed"), Some(&Value::Int64(1)));
    }

    #[test]
    fn lua_string_comparison() {
        let runtime = LuaRuntime::new(
            r#"
            if row.side == "buy" then
                emit({matched = 1})
            else
                emit({matched = 0})
            end
            "#,
        );
        let mut state = HashMap::new();
        let row = Row::from(HashMap::from([(
            "side".to_string(),
            Value::String("buy".to_string()),
        )]));
        let out = runtime
            .process(&mut state, &row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out.get("matched"), Some(&Value::Int64(1)));
    }

    #[test]
    fn lua_multi_emit() {
        let runtime = LuaRuntime::new(
            r#"
            local items = json.decode(row.items)
            for _, item in ipairs(items) do
                emit { name = item.name, value = item.value }
            end
            "#,
        );

        let mut state = HashMap::new();
        let row = Row::from(HashMap::from([(
            "items".to_string(),
            Value::String(
                r#"[{"name":"a","value":1},{"name":"b","value":2},{"name":"c","value":3}]"#
                    .to_string(),
            ),
        )]));

        let results = runtime.process(&mut state, &row).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].get("name"), Some(&Value::String("a".into())));
        assert_eq!(results[0].get("value"), Some(&Value::Int64(1)));
        assert_eq!(results[1].get("name"), Some(&Value::String("b".into())));
        assert_eq!(results[1].get("value"), Some(&Value::Int64(2)));
        assert_eq!(results[2].get("name"), Some(&Value::String("c".into())));
        assert_eq!(results[2].get("value"), Some(&Value::Int64(3)));
    }

    #[test]
    fn lua_multi_emit_mixed_with_single() {
        // If emit() is called, single-emit fields on the emit table are ignored
        let runtime = LuaRuntime::new(
            r#"
            emit { x = 1 }
            emit { x = 2 }
            "#,
        );

        let mut state = HashMap::new();
        let row = Row::from(RowMap::new());

        let results = runtime.process(&mut state, &row).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("x"), Some(&Value::Int64(1)));
        assert_eq!(results[1].get("x"), Some(&Value::Int64(2)));
    }

    #[test]
    fn lua_multi_emit_empty() {
        // emit() called with no args or not called → empty
        let runtime = LuaRuntime::new("-- no emit calls");

        let mut state = HashMap::new();
        let row = Row::from(RowMap::new());

        let results = runtime.process(&mut state, &row).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn lua_json_state_native_tables() {
        // Test Value::JSON as native Lua table — no json.decode/encode needed
        let state_fields = vec!["positions".to_string(), "count".to_string()];
        let state_types = vec![
            ("positions".to_string(), ColumnType::JSON),
            ("count".to_string(), ColumnType::Float64),
        ];
        let runtime = LuaRuntime::with_state_fields(
            r#"
            -- positions is already a native Lua table (no json.decode needed)
            local token = row.token
            local vol = row.volume

            if not state.positions[token] then
                state.positions[token] = { volume = 0, trades = 0 }
            end

            local pos = state.positions[token]
            pos.volume = pos.volume + vol
            pos.trades = pos.trades + 1
            state.count = state.count + 1

            emit({token = token, total_volume = pos.volume, total_trades = pos.trades})
            "#,
            &state_fields,
            &state_types,
            &["token".to_string(), "volume".to_string()],
            &[],
        );

        // Initialize state with JSON default (empty object)
        let mut state: HashMap<String, Value> = HashMap::from([
            ("positions".to_string(), Value::JSON(serde_json::json!({}))),
            ("count".to_string(), Value::Float64(0.0)),
        ]);

        // Row 1: token A, volume 100
        let row1 = Row::from(HashMap::from([
            ("token".to_string(), Value::String("A".into())),
            ("volume".to_string(), Value::Float64(100.0)),
        ]));
        let out1 = runtime.process(&mut state, &row1).unwrap();
        assert_eq!(out1.len(), 1);
        assert_eq!(out1[0].get("token"), Some(&Value::String("A".into())));
        assert_eq!(out1[0].get("total_volume"), Some(&Value::Float64(100.0)));
        assert_eq!(out1[0].get("total_trades").unwrap().as_f64(), Some(1.0));

        // Verify state was read back as Json
        match state.get("positions").unwrap() {
            Value::JSON(v) => {
                assert_eq!(v["A"]["volume"], 100.0);
                assert!(v["A"]["trades"] == 1 || v["A"]["trades"] == 1.0);
            }
            other => panic!("expected Value::JSON, got {:?}", other),
        }

        // Row 2: token A again
        let row2 = Row::from(HashMap::from([
            ("token".to_string(), Value::String("A".into())),
            ("volume".to_string(), Value::Float64(50.0)),
        ]));
        let out2 = runtime.process(&mut state, &row2).unwrap();
        assert_eq!(out2[0].get("total_volume"), Some(&Value::Float64(150.0)));
        assert_eq!(out2[0].get("total_trades").unwrap().as_f64(), Some(2.0));

        // Row 3: different token B
        let row3 = Row::from(HashMap::from([
            ("token".to_string(), Value::String("B".into())),
            ("volume".to_string(), Value::Float64(200.0)),
        ]));
        let out3 = runtime.process(&mut state, &row3).unwrap();
        assert_eq!(out3[0].get("total_volume"), Some(&Value::Float64(200.0)));

        // State should have both tokens
        match state.get("positions").unwrap() {
            Value::JSON(v) => {
                assert_eq!(v["A"]["volume"], 150.0);
                assert_eq!(v["B"]["volume"], 200.0);
            }
            other => panic!("expected Value::JSON, got {:?}", other),
        }
        assert_eq!(state.get("count"), Some(&Value::Float64(3.0)));
    }

    #[test]
    fn lua_json_state_array() {
        // Test JSON state with array values (FIFO lots without json.decode/encode)
        let state_fields = vec!["lots".to_string()];
        let state_types = vec![("lots".to_string(), ColumnType::JSON)];
        let runtime = LuaRuntime::with_state_fields(
            r#"
            -- lots is already a native Lua table (array)
            if row.side == "buy" then
                state.lots[#state.lots + 1] = { qty = row.amount, price = row.price }
                emit({pnl = 0})
            else
                local remaining = row.amount
                local pnl = 0
                while remaining > 0 and #state.lots > 0 do
                    local lot = state.lots[1]
                    local used = math.min(remaining, lot.qty)
                    pnl = pnl + used * (row.price - lot.price)
                    lot.qty = lot.qty - used
                    remaining = remaining - used
                    if lot.qty <= 0 then table.remove(state.lots, 1) end
                end
                emit({pnl = pnl})
            end
            "#,
            &state_fields,
            &state_types,
            &[
                "side".to_string(),
                "amount".to_string(),
                "price".to_string(),
            ],
            &[],
        );

        let mut state: HashMap<String, Value> =
            HashMap::from([("lots".to_string(), Value::JSON(serde_json::json!([])))]);

        // BUY 10 @ 100
        let out1 = runtime
            .process(&mut state, &make_trade("buy", 10.0, 100.0))
            .unwrap();
        assert_eq!(out1[0].get("pnl"), Some(&Value::Int64(0)));

        // BUY 5 @ 200
        runtime
            .process(&mut state, &make_trade("buy", 5.0, 200.0))
            .unwrap();

        // SELL 12 @ 150 (FIFO: 10@100, 2@200)
        let out3 = runtime
            .process(&mut state, &make_trade("sell", 12.0, 150.0))
            .unwrap();
        let pnl = out3[0].get("pnl").unwrap().as_f64().unwrap();
        // 10*(150-100) + 2*(150-200) = 500 + (-100) = 400
        assert!((pnl - 400.0).abs() < 0.01);

        // Verify state: 3 lots remaining at price 200
        match state.get("lots").unwrap() {
            Value::JSON(v) => {
                let arr = v.as_array().unwrap();
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0]["qty"], 3.0);
                assert_eq!(arr[0]["price"], 200.0);
            }
            other => panic!("expected Value::JSON, got {:?}", other),
        }
    }

    #[test]
    fn extract_row_field_refs_basic() {
        let refs = extract_row_field_refs("row.side == 0 and row.usdc > 100").unwrap();
        assert!(refs.contains("side"));
        assert!(refs.contains("usdc"));
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn extract_row_field_refs_skips_non_row() {
        let refs = extract_row_field_refs("local myrow = 1\nrow.amount + state.quantity").unwrap();
        assert!(refs.contains("amount"));
        assert!(!refs.contains("quantity")); // state.quantity, not row.quantity
        assert_eq!(refs.len(), 1);
    }

    #[test]
    fn extract_row_field_refs_dynamic_access_returns_none() {
        assert!(extract_row_field_refs("for k, v in pairs(row) do end").is_none());
    }

    #[test]
    fn compute_accessed_fields_filters_correctly() {
        let cols = vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()];
        let script = "row.a + row.c";
        let accessed = compute_accessed_fields(script, &cols);
        // Skips 3 fields (b, d, e) — filtering is active
        assert!(accessed.contains("a"));
        assert!(accessed.contains("c"));
        assert_eq!(accessed.len(), 2);
    }

    #[test]
    fn compute_accessed_fields_no_filter_when_skip_one() {
        let cols = vec!["a".into(), "b".into(), "c".into()];
        let script = "row.a + row.b"; // accesses 2 of 3 → skips only 1
        let accessed = compute_accessed_fields(script, &cols);
        // Only skipping 1 field — not worth filtering
        assert!(accessed.is_empty());
    }

    #[test]
    fn lua_module_loading() {
        let modules = vec![(
            "utils".to_string(),
            r#"
            local M = {}
            function M.double(x) return x * 2 end
            function M.add(a, b) return a + b end
            return M
            "#
            .to_string(),
        )];

        let runtime = LuaRuntime::with_state_fields(
            r#"
            local doubled = utils.double(row.value)
            state.total = utils.add(state.total, doubled)
            emit({result = state.total})
            "#,
            &["total".to_string()],
            &[("total".to_string(), ColumnType::Float64)],
            &["value".to_string()],
            &modules,
        );

        let mut state = HashMap::from([("total".to_string(), Value::Float64(0.0))]);
        let row = Row::from(HashMap::from([("value".to_string(), Value::Float64(5.0))]));

        let out = runtime.process(&mut state, &row).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("result"), Some(&Value::Float64(10.0)));

        let row2 = Row::from(HashMap::from([("value".to_string(), Value::Float64(3.0))]));
        let out2 = runtime.process(&mut state, &row2).unwrap();
        assert_eq!(out2[0].get("result"), Some(&Value::Float64(16.0)));
    }

    /// Issue #16: Int64 values must be passed as Lua integers, not lossy f64.
    #[test]
    fn lua_int64_precision() {
        let runtime = LuaRuntime::new(
            r#"
            emit({result = row.big})
            "#,
        );
        let mut state = HashMap::new();
        // 2^53 + 1 = 9007199254740993, loses precision as f64
        let big: i64 = (1i64 << 53) + 1;
        let row = Row::from(HashMap::from([("big".to_string(), Value::Int64(big))]));
        let out = runtime
            .process(&mut state, &row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            out.get("result"),
            Some(&Value::Int64(big)),
            "Int64 value should roundtrip through Lua without precision loss"
        );
    }

    /// Issue #10: Null fields in a row must not retain stale values from previous rows.
    #[test]
    fn lua_null_row_fields_do_not_leak() {
        let runtime = LuaRuntime::new(
            r#"
            if row.optional ~= nil then
                emit({saw = row.optional})
            else
                emit({saw = "nil"})
            end
            "#,
        );

        let mut state = HashMap::new();

        // Row 1: has "optional" field
        let row1 = Row::from(HashMap::from([(
            "optional".to_string(),
            Value::String("present".into()),
        )]));
        let out1 = runtime
            .process(&mut state, &row1)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(out1.get("saw"), Some(&Value::String("present".into())));

        // Row 2: does NOT have "optional" field — it must be nil, not "present"
        let row2 = Row::from(HashMap::new());
        let out2 = runtime
            .process(&mut state, &row2)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            out2.get("saw"),
            Some(&Value::String("nil".into())),
            "Null field should be nil in Lua, not stale value from previous row"
        );
    }

    /// Numeric keys set by Lua scripts on the row table must be cleared between rows.
    #[test]
    fn lua_numeric_row_keys_do_not_leak() {
        let runtime = LuaRuntime::new(
            r#"
            if row.do_set then
                row[1] = "injected"
            end
            if row[1] ~= nil then
                emit({saw = row[1]})
            else
                emit({saw = "nil"})
            end
            "#,
        );
        let mut state = HashMap::new();

        // Row 1: has do_set flag, script sets row[1]
        let row1 = Row::from(HashMap::from([(
            "do_set".to_string(),
            Value::Boolean(true),
        )]));
        let out1 = runtime.process(&mut state, &row1).unwrap();
        assert_eq!(out1[0].get("saw"), Some(&Value::String("injected".into())));

        // Row 2: no do_set flag — row[1] must be cleared from previous call
        let row2 = Row::from(RowMap::new());
        let out2 = runtime.process(&mut state, &row2).unwrap();
        assert_eq!(
            out2[0].get("saw"),
            Some(&Value::String("nil".into())),
            "Numeric key row[1] should be cleared between rows"
        );
    }

    /// Reproduce insider_classifier-style: JSON state grows with many unique keys,
    /// flush loop calls emit() many times, then continues processing.
    #[test]
    fn lua_json_state_many_rows_no_overflow() {
        let state_fields = vec![
            "status".to_string(),
            "window_vol".to_string(),
            "positions".to_string(),
        ];
        let state_types = vec![("positions".to_string(), ColumnType::JSON)];
        let source_cols = vec![
            "shares".to_string(),
            "side".to_string(),
            "usdc".to_string(),
            "trader".to_string(),
            "asset_id".to_string(),
        ];
        let runtime = LuaRuntime::with_state_fields(
            r#"
            if row.shares == 0 then return end
            if row.side ~= 0 then return end
            if state.status ~= "unknown" then
                if state.status == "insider" then
                    local price = row.usdc / row.shares
                    emit({ trader = row.trader, asset_id = row.asset_id, vol = row.usdc / 1000000, price = price })
                end
                return
            end
            state.window_vol = state.window_vol + row.usdc
            local token = row.asset_id
            local price = row.usdc / row.shares
            local pos = state.positions[token]
            if not pos then
                pos = { volume = 0, trades = 0 }
            end
            pos.volume = pos.volume + row.usdc / 1000000
            pos.trades = pos.trades + 1
            state.positions[token] = pos
            if state.window_vol >= 4000000000 then
                state.status = "insider"
                for tid, p in pairs(state.positions) do
                    emit({ trader = row.trader, asset_id = tid, volume = p.volume })
                end
            end
            "#,
            &state_fields,
            &state_types,
            &source_cols,
            &[],
        );

        let mut state: HashMap<String, Value> = HashMap::from([
            ("status".to_string(), Value::String("unknown".into())),
            ("window_vol".to_string(), Value::UInt64(0)),
            ("positions".to_string(), Value::JSON(serde_json::json!({}))),
        ]);

        // 5000 rows across 100 unique asset_ids; window_vol crosses threshold ~row 4
        for i in 0u64..5000 {
            let asset = format!("asset_{}", i % 100);
            let row = Row::from(HashMap::from([
                ("shares".to_string(), Value::UInt64(1000)),
                ("side".to_string(), Value::UInt64(0)),
                ("usdc".to_string(), Value::UInt64(1_000_000_000)), // $1000
                ("trader".to_string(), Value::String("trader1".into())),
                ("asset_id".to_string(), Value::String(asset)),
            ]));
            runtime.process(&mut state, &row).unwrap();
        }
        assert_eq!(
            state.get("status"),
            Some(&Value::String("insider".into())),
            "Trader should be classified as insider"
        );
    }

    /// Emit buffer must be cleared between process() calls — no bleed from one row to the next.
    #[test]
    fn emit_buffer_cleared_between_rows() {
        let runtime = LuaRuntime::new(
            r#"
            for i = 1, row.n do
                emit({ i = i })
            end
            "#,
        );
        let mut state = HashMap::new();

        // Row 1 emits 5
        let row1 = Row::from(HashMap::from([("n".to_string(), Value::Int64(5))]));
        let out1 = runtime.process(&mut state, &row1).unwrap();
        assert_eq!(out1.len(), 5);

        // Row 2 emits 1 — must NOT include the 5 from row 1
        let row2 = Row::from(HashMap::from([("n".to_string(), Value::Int64(1))]));
        let out2 = runtime.process(&mut state, &row2).unwrap();
        assert_eq!(out2.len(), 1, "emit buffer must be cleared between rows");
        assert_eq!(out2[0].get("i"), Some(&Value::Int64(1)));

        // Row 3 emits 0
        let row3 = Row::from(HashMap::from([("n".to_string(), Value::Int64(0))]));
        let out3 = runtime.process(&mut state, &row3).unwrap();
        assert!(out3.is_empty(), "emit buffer must be empty when no emit is called");
    }

    /// A single row that emits 1000 rows must return all 1000 with correct values.
    /// Tests the Rust-side buffer under load (replacing the old _emits Lua table).
    #[test]
    fn emit_large_flush_preserves_all_values() {
        let runtime = LuaRuntime::new(
            r#"
            for i = 1, row.n do
                emit({ idx = i, double = i * 2 })
            end
            "#,
        );
        let mut state = HashMap::new();
        let n = 1000i64;
        let row = Row::from(HashMap::from([("n".to_string(), Value::Int64(n))]));

        let results = runtime.process(&mut state, &row).unwrap();
        assert_eq!(results.len(), n as usize);
        for (i, r) in results.iter().enumerate() {
            let expected_idx = (i + 1) as i64;
            assert_eq!(
                r.get("idx"),
                Some(&Value::Int64(expected_idx)),
                "row {i}: wrong idx"
            );
            assert_eq!(
                r.get("double"),
                Some(&Value::Int64(expected_idx * 2)),
                "row {i}: wrong double"
            );
        }
    }

    /// After a large flush, subsequent process() calls must still emit correctly.
    #[test]
    fn emit_after_large_flush_continues_correctly() {
        let runtime = LuaRuntime::new(
            r#"
            for i = 1, row.n do
                emit({ val = i })
            end
            "#,
        );
        let mut state = HashMap::new();

        // Large flush
        let row_many = Row::from(HashMap::from([("n".to_string(), Value::Int64(500))]));
        let out_many = runtime.process(&mut state, &row_many).unwrap();
        assert_eq!(out_many.len(), 500);

        // Single emit after flush
        let row_one = Row::from(HashMap::from([("n".to_string(), Value::Int64(1))]));
        let out_one = runtime.process(&mut state, &row_one).unwrap();
        assert_eq!(out_one.len(), 1);
        assert_eq!(out_one[0].get("val"), Some(&Value::Int64(1)));

        // Zero emits after flush
        let row_zero = Row::from(HashMap::from([("n".to_string(), Value::Int64(0))]));
        let out_zero = runtime.process(&mut state, &row_zero).unwrap();
        assert!(out_zero.is_empty());
    }

    /// emit() called inside pairs() loop with a large JSON table.
    /// Pre-populate 500 keys, then emit once per key inside pairs().
    /// This caused Lua stack overflow when emit was a Rust callback.
    #[test]
    fn emit_inside_pairs_no_stack_overflow() {
        let state_fields = vec!["positions".to_string()];
        let state_types = vec![("positions".to_string(), ColumnType::JSON)];
        let source_cols = vec!["x".to_string()];
        let runtime = LuaRuntime::with_state_fields(
            r#"
            for k, v in pairs(state.positions) do
                emit({ key = k, val = v })
            end
            "#,
            &state_fields,
            &state_types,
            &source_cols,
            &[],
        );

        // Pre-populate 500 keys
        let mut positions = serde_json::Map::new();
        for i in 0..500 {
            positions.insert(format!("asset_{i}"), serde_json::json!(i));
        }
        let mut state: HashMap<String, Value> = HashMap::from([(
            "positions".to_string(),
            Value::JSON(serde_json::Value::Object(positions)),
        )]);

        // Single row triggers 500 emits inside pairs()
        let row = Row::from(HashMap::from([("x".to_string(), Value::UInt64(1))]));
        let out = runtime.process(&mut state, &row).unwrap();
        assert_eq!(out.len(), 500);
    }

    /// Multi-emit in the specialized path (with state_fields / positional args).
    #[test]
    fn emit_specialized_path_multi_emit() {
        let state_fields = vec!["count".to_string(), "positions".to_string()];
        let state_types = vec![
            ("count".to_string(), ColumnType::Float64),
            ("positions".to_string(), ColumnType::JSON),
        ];
        let runtime = LuaRuntime::with_state_fields(
            r#"
            state.count = state.count + 1
            local token = row.token
            if not state.positions[token] then
                state.positions[token] = 0
            end
            state.positions[token] = state.positions[token] + row.vol

            -- flush: emit one row per position tracked so far
            if state.count % 3 == 0 then
                for k, v in pairs(state.positions) do
                    emit({ token = k, total_vol = v, flush_at = state.count })
                end
            end
            "#,
            &state_fields,
            &state_types,
            &["token".to_string(), "vol".to_string()],
            &[],
        );

        let mut state = HashMap::from([
            ("count".to_string(), Value::Float64(0.0)),
            ("positions".to_string(), Value::JSON(serde_json::json!({}))),
        ]);

        let make_row = |token: &str, vol: f64| {
            Row::from(HashMap::from([
                ("token".to_string(), Value::String(token.into())),
                ("vol".to_string(), Value::Float64(vol)),
            ]))
        };

        // Rows 1 & 2: no flush yet
        assert!(runtime.process(&mut state, &make_row("A", 10.0)).unwrap().is_empty());
        assert!(runtime.process(&mut state, &make_row("B", 20.0)).unwrap().is_empty());

        // Row 3: count == 3, triggers flush (2 tokens: A and B)
        let out3 = runtime.process(&mut state, &make_row("A", 5.0)).unwrap();
        assert_eq!(out3.len(), 2, "flush should emit one row per tracked token");
        let mut totals: HashMap<String, f64> = HashMap::new();
        for r in &out3 {
            if let (Some(Value::String(k)), Some(v)) = (r.get("token"), r.get("total_vol")) {
                totals.insert(k.clone(), v.as_f64().unwrap());
            }
        }
        assert_eq!(totals["A"], 15.0); // 10 + 5
        assert_eq!(totals["B"], 20.0);

        // State must still be updated correctly
        assert_eq!(state.get("count"), Some(&Value::Float64(3.0)));

        // Rows 4 & 5: no flush
        assert!(runtime.process(&mut state, &make_row("C", 30.0)).unwrap().is_empty());
        assert!(runtime.process(&mut state, &make_row("C", 10.0)).unwrap().is_empty());

        // Row 6: flush again (3 tokens: A, B, C)
        let out6 = runtime.process(&mut state, &make_row("B", 5.0)).unwrap();
        assert_eq!(out6.len(), 3, "second flush should emit 3 tokens");
    }

    /// Emitting an empty table must be silently dropped.
    #[test]
    fn emit_empty_table_is_skipped() {
        let runtime = LuaRuntime::new(
            r#"
            emit({})
            emit({ x = 1 })
            emit({})
            "#,
        );
        let mut state = HashMap::new();
        let row = Row::from(RowMap::new());
        let results = runtime.process(&mut state, &row).unwrap();
        assert_eq!(results.len(), 1, "empty table emits must be skipped");
        assert_eq!(results[0].get("x"), Some(&Value::Int64(1)));
    }

    /// Lua script runtime errors must return Err, not panic.
    #[test]
    fn lua_script_error_returns_err() {
        let runtime = LuaRuntime::new(
            r#"
            error("intentional failure")
            "#,
        );
        let mut state = HashMap::new();
        let row = Row::from(RowMap::new());
        let result = runtime.process(&mut state, &row);
        assert!(result.is_err(), "Lua error() should return Err, not panic");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("intentional failure"),
            "Error should contain the Lua message, got: {msg}"
        );
    }
}
