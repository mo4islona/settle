//! The napi side of the codec: an `Encoder` to JS values, and typed arrays,
//! strings and host values from JS.

use std::ffi::c_void;
use std::ptr;

use napi::{Env, JsUnknown, NapiRaw, NapiValue, sys};

use super::{Encoder, Strings, bigint_to_value};
use crate::types::Value;

fn check(status: sys::napi_status, what: &str) -> Result<(), String> {
    if status == sys::Status::napi_ok {
        Ok(())
    } else {
        Err(format!("napi: {what} failed with status {status}"))
    }
}

/// `enc` as the JS values `[buffer, strings, others]`: an `ArrayBuffer`,
/// a string, and an array of host values or null.
pub fn to_js(env: &Env, enc: &Encoder<'_>) -> Result<[sys::napi_value; 3], String> {
    let raw = env.raw();

    let len = enc.byte_len();
    let mut data: *mut c_void = ptr::null_mut();
    let mut buffer = ptr::null_mut();
    check(
        unsafe { sys::napi_create_arraybuffer(raw, len, &mut data, &mut buffer) },
        "create ArrayBuffer",
    )?;
    // SAFETY: V8 just allocated `len` bytes at `data`, and nothing else holds them.
    let out = unsafe { std::slice::from_raw_parts_mut(data as *mut u8, len) };
    enc.write(out);

    let (text, ascii) = enc.strings();
    let mut strings = ptr::null_mut();
    let created = if ascii {
        unsafe { sys::napi_create_string_latin1(raw, text.as_ptr().cast(), text.len(), &mut strings) }
    } else {
        unsafe { sys::napi_create_string_utf8(raw, text.as_ptr().cast(), text.len(), &mut strings) }
    };
    check(created, "create string")?;

    let mut others = ptr::null_mut();
    if enc.others().is_empty() {
        check(unsafe { sys::napi_get_null(raw, &mut others) }, "get null")?;
    } else {
        check(
            unsafe { sys::napi_create_array_with_length(raw, enc.others().len(), &mut others) },
            "create array",
        )?;
        for (i, value) in enc.others().iter().enumerate() {
            let js = host_value(env, value)?;
            check(
                unsafe { sys::napi_set_element(raw, others, i as u32, js) },
                "set element",
            )?;
        }
    }

    Ok([buffer, strings, others])
}

/// A value that the encoder left to the host.
fn host_value(env: &Env, value: &Value) -> Result<sys::napi_value, String> {
    let raw = env.raw();
    let mut out = ptr::null_mut();
    match value {
        Value::UInt64(n) => check(
            unsafe { sys::napi_create_bigint_uint64(raw, *n, &mut out) },
            "create BigInt",
        )?,
        Value::Int64(n) | Value::DateTime(n) => check(
            unsafe { sys::napi_create_bigint_int64(raw, *n, &mut out) },
            "create BigInt",
        )?,
        Value::Bytes(bytes) => {
            let mut data: *mut c_void = ptr::null_mut();
            let mut buffer = ptr::null_mut();
            check(
                unsafe { sys::napi_create_arraybuffer(raw, bytes.len(), &mut data, &mut buffer) },
                "create ArrayBuffer",
            )?;
            if !bytes.is_empty() {
                // SAFETY: V8 just allocated `bytes.len()` bytes at `data`.
                unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), data as *mut u8, bytes.len()) };
            }
            check(
                unsafe {
                    sys::napi_create_typedarray(
                        raw,
                        sys::TypedarrayType::uint8_array,
                        bytes.len(),
                        buffer,
                        0,
                        &mut out,
                    )
                },
                "create Uint8Array",
            )?;
        }
        Value::JSON(json) => {
            let js = env.to_js_value(json).map_err(|e| e.to_string())?;
            out = unsafe { js.raw() };
        }
        _ => check(unsafe { sys::napi_get_null(raw, &mut out) }, "get null")?,
    }
    Ok(out)
}

/// Element `index` of the JS array `array`.
pub fn element(env: &Env, array: sys::napi_value, index: u32) -> Result<sys::napi_value, String> {
    let mut out = ptr::null_mut();
    check(
        unsafe { sys::napi_get_element(env.raw(), array, index, &mut out) },
        "get element",
    )?;
    Ok(out)
}

/// The elements of a JS typed array.
///
/// # Safety
///
/// `value` must stay alive, and its buffer must not be detached or resized,
/// while the slice is in use.
pub unsafe fn typed_array<'a, T>(
    env: &Env,
    value: sys::napi_value,
    expected: sys::napi_typedarray_type,
) -> Result<&'a [T], String> {
    let mut kind = 0;
    let mut len = 0;
    let mut data: *mut c_void = ptr::null_mut();
    let mut buffer = ptr::null_mut();
    let mut offset = 0;
    check(
        unsafe {
            sys::napi_get_typedarray_info(
                env.raw(),
                value,
                &mut kind,
                &mut len,
                &mut data,
                &mut buffer,
                &mut offset,
            )
        },
        "read typed array",
    )?;
    if kind != expected {
        return Err(format!("expected typed array kind {expected}, got {kind}"));
    }
    if len == 0 {
        return Ok(&[]);
    }
    // SAFETY: napi points `data` at `len` elements of the array's kind,
    // which the check above matched to `T`.
    Ok(unsafe { std::slice::from_raw_parts(data as *const T, len) })
}

/// A JS string, copied out: as bytes if it is all ASCII, else as UTF-16.
pub enum OwnedStrings {
    Ascii(String),
    Utf16(Vec<u16>),
}

impl OwnedStrings {
    pub fn as_strings(&self) -> Strings<'_> {
        match self {
            OwnedStrings::Ascii(s) => Strings::Ascii(s),
            OwnedStrings::Utf16(u) => Strings::Utf16(u),
        }
    }
}

pub fn read_strings(env: &Env, value: sys::napi_value) -> Result<OwnedStrings, String> {
    let raw = env.raw();
    let mut units = 0;
    check(
        unsafe { sys::napi_get_value_string_utf16(raw, value, ptr::null_mut(), 0, &mut units) },
        "read string length",
    )?;
    let mut bytes = 0;
    check(
        unsafe { sys::napi_get_value_string_utf8(raw, value, ptr::null_mut(), 0, &mut bytes) },
        "read string length",
    )?;

    // Each char past ASCII takes more UTF-8 bytes than UTF-16 units.
    if bytes == units {
        let mut buf = vec![0u8; bytes + 1];
        let mut written = 0;
        check(
            unsafe {
                sys::napi_get_value_string_utf8(raw, value, buf.as_mut_ptr().cast(), buf.len(), &mut written)
            },
            "read string",
        )?;
        buf.truncate(written);
        let text = String::from_utf8(buf).map_err(|e| e.to_string())?;
        return Ok(OwnedStrings::Ascii(text));
    }

    let mut buf = vec![0u16; units + 1];
    let mut written = 0;
    check(
        unsafe { sys::napi_get_value_string_utf16(raw, value, buf.as_mut_ptr(), buf.len(), &mut written) },
        "read string",
    )?;
    buf.truncate(written);
    Ok(OwnedStrings::Utf16(buf))
}

/// The value of element `index` of the host value array `others`. A BigInt
/// becomes UInt64, Int64 or Uint256, else its decimal text. An object
/// becomes JSON; on ingest, as the msgpack input did, its JSON text, and a
/// `Uint8Array` its bytes.
pub fn other_value(
    env: &Env,
    others: sys::napi_value,
    index: usize,
    ingest: bool,
) -> Result<Value, String> {
    let index = u32::try_from(index).map_err(|_| format!("host value {index} out of range"))?;
    let raw_value = element(env, others, index)?;
    let value = unsafe { JsUnknown::from_raw_unchecked(env.raw(), raw_value) };

    match value.get_type().map_err(|e| e.to_string())? {
        napi::ValueType::BigInt => {
            let mut big: napi::JsBigInt = unsafe { value.cast() };
            let (negative, words) = big.get_words().map_err(|e| e.to_string())?;
            match bigint_to_value(negative, &words) {
                Some(value) => Ok(value),
                None => {
                    let text = value
                        .coerce_to_string()
                        .and_then(|s| s.into_utf8())
                        .and_then(|s| s.into_owned())
                        .map_err(|e| e.to_string())?;
                    Ok(Value::String(text))
                }
            }
        }
        napi::ValueType::Object if ingest => {
            if let Some(bytes) = uint8_array(env, raw_value)? {
                return Ok(Value::Bytes(bytes));
            }
            let json: serde_json::Value = env.from_js_value(value).map_err(|e| e.to_string())?;
            Ok(Value::String(json.to_string()))
        }
        napi::ValueType::Object => {
            let json: serde_json::Value = env.from_js_value(value).map_err(|e| e.to_string())?;
            Ok(Value::JSON(json))
        }
        _ => Ok(Value::Null),
    }
}

/// The bytes of `value` if it is a `Uint8Array` (a `Buffer` is one).
fn uint8_array(env: &Env, value: sys::napi_value) -> Result<Option<Vec<u8>>, String> {
    let mut is_typed = false;
    check(
        unsafe { sys::napi_is_typedarray(env.raw(), value, &mut is_typed) },
        "check typed array",
    )?;
    if !is_typed {
        return Ok(None);
    }
    // SAFETY: the bytes are copied out before any JS runs.
    match unsafe { typed_array::<u8>(env, value, sys::TypedarrayType::uint8_array) } {
        Ok(bytes) => Ok(Some(bytes.to_vec())),
        Err(_) => Ok(None),
    }
}

/// A handle scope for the napi values of one callback: without it, they
/// would live until the whole ingest returns.
pub struct HandleScope {
    env: sys::napi_env,
    scope: sys::napi_handle_scope,
}

impl HandleScope {
    pub fn open(env: &Env) -> Result<Self, String> {
        let mut scope = ptr::null_mut();
        check(
            unsafe { sys::napi_open_handle_scope(env.raw(), &mut scope) },
            "open handle scope",
        )?;
        Ok(Self {
            env: env.raw(),
            scope,
        })
    }
}

impl Drop for HandleScope {
    fn drop(&mut self) {
        unsafe { sys::napi_close_handle_scope(self.env, self.scope) };
    }
}
