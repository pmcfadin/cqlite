//! Value conversion from cqlite_core to JavaScript types.
//!
//! This module handles conversion of all CQL data types to their JavaScript equivalents.
//! The mapping follows Issue #302 specification for type fidelity.
//!
//! ## Type Mapping
//!
//! | CQL Type | JavaScript Type |
//! |----------|-----------------|
//! | Null | `null` |
//! | Boolean | `boolean` |
//! | TinyInt/SmallInt/Int | `number` |
//! | BigInt/Counter | `bigint` |
//! | Float/Float32 | `number` |
//! | Text | `string` |
//! | Blob | `Buffer` |
//! | Timestamp | `Date` |
//! | Date | `Date` |
//! | Time | `bigint` (nanoseconds) |
//! | Uuid | `string` (formatted) |
//! | Varint | `bigint` |
//! | Decimal | `string` (preserves precision) |
//! | Duration | `{ months, days, nanos }` |
//! | Inet | `string` (IP format) |
//! | List | `Array` |
//! | Set | `Set` |
//! | Map | `Map` |
//! | Tuple | `Array` |
//! | Udt | `object` with `_type`, `_keyspace`, and field properties |

use crate::error::to_napi_error;
use cqlite_core::types::Value;
use napi::{Env, JsFunction, JsObject, JsString, JsUnknown, Result};
use std::cell::OnceCell;

/// Per-result-conversion context that caches the global `Set` and `Map`
/// constructors (Issue #1448).
///
/// Before this, every CQL `set`/`map` cell re-fetched its JS constructor from
/// scratch (`env.get_global()` + `get_named_property`) PER CELL. A result set
/// with many collection cells paid that lookup once per cell. `ConvCtx` fetches
/// each constructor at most once per result conversion, lazily: a result with no
/// `set`/`map` cells performs zero constructor lookups.
///
/// INVARIANT: at most one `get_global()` + named-property lookup for `Set` and
/// one for `Map` per `ConvCtx`, regardless of row/cell count; zero when the
/// result has no set/map cells. The [`ctor_lookups`](self::testing::ctor_lookups)
/// work counter proves this in tests.
///
/// One `ConvCtx` is constructed per result (batch: once in `resolve`; streaming:
/// once per yielded row, since napi handles are scoped to each `resolve` `Env`)
/// and threaded by shared reference through `row_to_object` → `value_to_napi` →
/// `set_to_js_set`/`map_to_js_map`.
pub struct ConvCtx<'a> {
    env: &'a Env,
    set_ctor: OnceCell<JsFunction>,
    map_ctor: OnceCell<JsFunction>,
}

impl<'a> ConvCtx<'a> {
    /// Build a fresh conversion context over `env`. No constructor lookups happen
    /// here — both caches start empty and fill lazily on first use.
    pub fn new(env: &'a Env) -> Self {
        Self {
            env,
            set_ctor: OnceCell::new(),
            map_ctor: OnceCell::new(),
        }
    }

    /// The napi environment this context converts values into.
    fn env(&self) -> &Env {
        self.env
    }

    /// The cached global `Set` constructor, fetched at most once per context.
    fn set_constructor(&self) -> Result<&JsFunction> {
        cache_get_or_try_init(&self.set_ctor, || {
            let global = self.env.get_global()?;
            global.get_named_property::<JsFunction>("Set")
        })
    }

    /// The cached global `Map` constructor, fetched at most once per context.
    fn map_constructor(&self) -> Result<&JsFunction> {
        cache_get_or_try_init(&self.map_ctor, || {
            let global = self.env.get_global()?;
            global.get_named_property::<JsFunction>("Map")
        })
    }
}

/// Stable-Rust equivalent of the unstable `OnceCell::get_or_try_init`: return the
/// cached value if present, otherwise run the fallible `fetch` exactly once,
/// cache it, and return the cached reference.
///
/// This is the single fetch-vs-cached decision point for the `ConvCtx`
/// constructor caches, so the [`ctor_lookups`](self::testing::ctor_lookups) work
/// counter is bumped here — and ONLY here, on the fetch path. Because `fetch` is
/// generic, the caching decision is unit-testable in Rust without a live JS
/// `Env` (see the tests below): the counter must read 1 after two calls on one
/// cell, and 0 when `fetch` is never invoked.
fn cache_get_or_try_init<T, F>(cell: &OnceCell<T>, fetch: F) -> Result<&T>
where
    F: FnOnce() -> Result<T>,
{
    if cell.get().is_none() {
        let value = fetch()?;
        #[cfg(test)]
        testing::bump_ctor_lookups();
        // The conversion path is single-threaded per `Env`, so no other caller
        // can have populated the cell between the `get` above and here; `set`
        // therefore succeeds. If it ever raced, we simply keep whichever value
        // won and fall through to the shared `get` below (no `unwrap`).
        let _ = cell.set(value);
    }
    cell.get()
        .ok_or_else(|| napi::Error::from_reason("constructor cache not initialized"))
}

/// Test-only instrumentation proving the constructor-caching invariant of
/// [`ConvCtx`] (Issue #1448): a process-global counter bumped exactly once per
/// real constructor fetch in [`cache_get_or_try_init`], mirroring the read-work
/// counter pattern in `cqlite-core`'s `work_counters`.
#[cfg(test)]
pub(crate) mod testing {
    use std::sync::atomic::{AtomicU64, Ordering};

    static CTOR_LOOKUPS: AtomicU64 = AtomicU64::new(0);

    /// Record one real constructor fetch (called only on the cache-miss path).
    pub(crate) fn bump_ctor_lookups() {
        CTOR_LOOKUPS.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of constructor fetches since the last [`reset_ctor_lookups`].
    pub(crate) fn ctor_lookups() -> u64 {
        CTOR_LOOKUPS.load(Ordering::Relaxed)
    }

    /// Clear the counter so a test starts from a known zero.
    pub(crate) fn reset_ctor_lookups() {
        CTOR_LOOKUPS.store(0, Ordering::Relaxed);
    }
}

/// Convert a CQL Value to a JavaScript value with native types.
///
/// This function creates proper JavaScript native types:
/// - BigInt for i64 values (preserves precision)
/// - Buffer for blob data
/// - Date for timestamps
/// - Set for CQL sets
/// - Map for CQL maps
///
/// # Arguments
///
/// * `ctx` - The per-result conversion context (holds the napi `Env` and the
///   lazily-cached `Set`/`Map` constructors, Issue #1448)
/// * `value` - The CQL value to convert
///
/// # Returns
///
/// A `JsUnknown` representing the JavaScript value
pub fn value_to_napi(ctx: &ConvCtx, value: &Value) -> Result<JsUnknown> {
    let env = ctx.env();
    match value {
        // Null
        Value::Null => env.get_null().map(|v| v.into_unknown()),

        // Boolean
        Value::Boolean(b) => env.get_boolean(*b).map(|v| v.into_unknown()),

        // Integer types that fit in JavaScript number without precision loss
        Value::TinyInt(i) => env.create_int32(*i as i32).map(|n| n.into_unknown()),
        Value::SmallInt(i) => env.create_int32(*i as i32).map(|n| n.into_unknown()),
        Value::Integer(i) => env.create_int32(*i).map(|n| n.into_unknown()),

        // 64-bit integers use BigInt to preserve precision
        Value::BigInt(i) => env.create_bigint_from_i64(*i)?.into_unknown(),
        Value::Counter(i) => env.create_bigint_from_i64(*i)?.into_unknown(),

        // Floating point
        Value::Float32(f) => env.create_double(*f as f64).map(|n| n.into_unknown()),
        Value::Float(f) => env.create_double(*f).map(|n| n.into_unknown()),

        // Text
        Value::Text(s) => env
            .create_string(std::str::from_utf8(s).unwrap_or_default())
            .map(|s| s.into_unknown()),

        // Blob -> Buffer
        Value::Blob(bytes) => env.create_buffer_copy(bytes).map(|b| b.into_unknown()),

        // Timestamp -> Date (milliseconds since epoch)
        Value::Timestamp(millis) => env.create_date(*millis as f64).map(|d| d.into_unknown()),

        // Date -> Date (days since epoch converted to milliseconds at midnight UTC)
        Value::Date(days) => {
            // CQL date is days since epoch (1970-01-01)
            // Convert to milliseconds: days * 24 * 60 * 60 * 1000 = days * 86400000
            // Note: JavaScript Date has no date-only type; time component will be midnight UTC
            let millis = (*days as i64)
                .checked_mul(86_400_000)
                .ok_or_else(|| napi::Error::from_reason("Date value overflow"))?;
            env.create_date(millis as f64).map(|d| d.into_unknown())
        }

        // Time -> BigInt (nanoseconds since midnight)
        Value::Time(nanos) => env.create_bigint_from_i64(*nanos)?.into_unknown(),

        // UUID -> formatted string
        Value::Uuid(bytes) => {
            let uuid = uuid::Uuid::from_bytes(*bytes);
            env.create_string(&uuid.to_string())
                .map(|s| s.into_unknown())
        }

        // Varint -> BigInt
        Value::Varint(bytes) => varint_to_bigint(env, bytes),

        // Decimal -> string (preserves arbitrary precision)
        Value::Decimal { scale, unscaled } => {
            let decimal_str = decimal_to_string(*scale, unscaled)?;
            env.create_string(&decimal_str).map(|s| s.into_unknown())
        }

        // Duration -> object { months, days, nanos }
        Value::Duration {
            months,
            days,
            nanos,
        } => duration_to_object(env, *months, *days, *nanos),

        // Inet -> IP address string
        Value::Inet(bytes) => inet_to_string_js(env, bytes),

        // JSON -> recursive conversion
        Value::Json(json) => json_to_napi(ctx, json),

        // List -> Array
        Value::List(items) => list_to_array(ctx, items),

        // Set -> JavaScript Set
        Value::Set(items) => set_to_js_set(ctx, items),

        // Map -> JavaScript Map
        Value::Map(pairs) => map_to_js_map(ctx, pairs),

        // Tuple -> Array
        Value::Tuple(items) => list_to_array(ctx, items),

        // UDT -> object with fields
        Value::Udt(udt) => udt_to_object(ctx, udt),

        // Frozen -> unwrap inner value
        Value::Frozen(inner) => value_to_napi(ctx, inner),

        // Tombstone -> null (deleted data)
        Value::Tombstone(_) => env.get_null().map(|v| v.into_unknown()),
    }
}

/// Convert variable-length integer bytes to a JavaScript `BigInt`.
///
/// A thin adapter: the CQL `varint` semantic (big-endian two's complement, empty
/// payload meaning zero, sign extension at any width) and its projection into
/// napi's sign-magnitude little-endian `u64` word form are decided ONCE in
/// [`cqlite_ffi_common::varint`] (issue #1452). The hand-rolled `<= 8` byte
/// special case, the padding, the word assembly and the carry-propagating
/// two's-complement negate loop that used to live here are all gone — the word
/// form is now derived from the shared `BigInt`, so the two can never disagree.
fn varint_to_bigint(env: &Env, bytes: &[u8]) -> Result<JsUnknown> {
    let (is_negative, words) = cqlite_ffi_common::varint::varint_to_sign_and_le_words(bytes);
    env.create_bigint_from_words(is_negative, words)?
        .into_unknown()
}

/// Render a CQL DECIMAL to its exact string through the ONE shared
/// implementation.
///
/// The single Node-specific step is mapping the shared
/// [`cqlite_ffi_common::decimal::DecimalError`] onto
/// [`cqlite_core::Error::corruption`] and thence through this binding's existing
/// production [`to_napi_error`] path, so a refused cell's `error.code` still
/// comes from the one FFI error contract and its message has one spelling in the
/// repository (issue #1452). The rendering policy — the refusal ceiling and the
/// exponent-form thresholds — is now stated once, in the shared crate, and is
/// identical in both bindings.
fn decimal_to_string(scale: i32, unscaled: &[u8]) -> Result<String> {
    cqlite_ffi_common::decimal::decimal_to_string(scale, unscaled)
        .map_err(|err| to_napi_error(cqlite_core::Error::corruption(err.to_string())))
}

/// Convert duration to JavaScript object { months, days, nanos }.
fn duration_to_object(env: &Env, months: i32, days: i32, nanos: i64) -> Result<JsUnknown> {
    let mut obj = env.create_object()?;
    obj.set_named_property("months", env.create_int32(months)?)?;
    obj.set_named_property("days", env.create_int32(days)?)?;
    let nanos_bigint = env.create_bigint_from_i64(nanos)?;
    obj.set_named_property("nanos", nanos_bigint)?;
    Ok(obj.into_unknown())
}

/// Convert inet bytes to an IP-address string.
///
/// A thin adapter over [`cqlite_ffi_common::inet::inet_bytes_to_string`]: the
/// 4/16 length dispatch and the malformed-length message are decided ONCE in the
/// shared crate, so this module holds no literal copy of that message (issue
/// #1453 had aligned the two bindings by hand-copying the string into both
/// files; issue #1452 removed the copy). Per the no-heuristics mandate (issue
/// #28) there is no passthrough branch: the only outcomes are IPv4, IPv6 and a
/// typed error.
///
/// The refusal is mapped through [`to_napi_error`] — exactly as the sibling
/// DECIMAL adapter does — so it carries the ONE FFI error contract's identity
/// for a data fault (`code: 'PARSE'`, `category: 'Data'`, issue #1451). A bare
/// `napi::Error::from_reason` carries no `\0code=` metadata, so
/// `lib/error-wrapper.js` fell back to its INTERNAL/Internal defaults and a
/// corrupt SSTable cell claimed an internal-bug identity.
fn inet_to_string_js(env: &Env, bytes: &[u8]) -> Result<JsUnknown> {
    let ip_str = cqlite_ffi_common::inet::inet_bytes_to_string(bytes)
        .map_err(|err| to_napi_error(cqlite_core::Error::corruption(err.to_string())))?;
    env.create_string(&ip_str).map(|s| s.into_unknown())
}

/// Convert a JSON number to the JavaScript value that represents it EXACTLY.
///
/// A thin adapter over the ONE shared classifier
/// [`cqlite_ffi_common::json_number::classify_json_number`] — the same one the
/// Python binding uses, so the two can no longer disagree about the boundary
/// (before issue #3505 they disagreed twice over: Python fell through to a
/// lossy `f64` then a `str`, Node to a lossy `f64` then a fabricated `null`).
///
/// **The JS answer is genuinely different from Python's, and this is the
/// statement of it rather than an assumption (#3505 AC5).** A JS `number` is an
/// `f64`, so it cannot carry an integer above `2^53`; `BigInt` can. `BigInt` is
/// the ESTABLISHED lossless integer type in this binding — not a novel choice —
/// because the `i64`-outside-`i32`-range arm below already used it before this
/// change. So:
///
/// * `I64` inside `i32` range → `number` (unchanged: exact, and idiomatic JS)
/// * `I64` outside `i32` range → `BigInt` (unchanged)
/// * `U64` → `BigInt`. **The fix.** Previously `as_i64()` returned `None` and
///   `as_f64()` succeeded lossily, so `18446744073709551615` reached JS as
///   `1.8446744073709552e19`.
/// * `F64` → `number` (a JSON float literal; exact by construction)
/// * `Beyond` → an exact `BigInt` if the text is an integer literal, else a
///   REFUSAL. The old arm returned `env.get_null()`, i.e. it delivered a
///   **fabricated `null`** for an unrepresentable number — a silent
///   data-loss bug in its own right, and strictly worse than Python's string
///   fallback, since `null` is indistinguishable from a genuine JSON `null`.
fn json_number_to_napi(env: &Env, n: &serde_json::Number) -> Result<JsUnknown> {
    use cqlite_ffi_common::json_number::JsonNumberClass;
    match cqlite_ffi_common::json_number::classify_json_number(n) {
        JsonNumberClass::I64(i) => {
            if (i32::MIN as i64..=i32::MAX as i64).contains(&i) {
                env.create_int32(i as i32).map(|v| v.into_unknown())
            } else {
                env.create_bigint_from_i64(i)?.into_unknown()
            }
        }
        JsonNumberClass::U64(u) => env.create_bigint_from_u64(u)?.into_unknown(),
        JsonNumberClass::F64(f) => env.create_double(f).map(|v| v.into_unknown()),
        JsonNumberClass::Beyond(text) => {
            match cqlite_ffi_common::json_number::beyond_text_to_sign_and_le_words(&text) {
                Some((is_negative, words)) => env
                    .create_bigint_from_words(is_negative, words)?
                    .into_unknown(),
                // Fail closed through the ONE FFI error contract, exactly as the
                // DECIMAL and INET adapters do.
                None => Err(to_napi_error(cqlite_core::Error::unsupported_format(
                    cqlite_ffi_common::json_number::beyond_range_message(&text),
                ))),
            }
        }
    }
}

/// Convert serde_json::Value to JavaScript value.
fn json_to_napi(ctx: &ConvCtx, json: &serde_json::Value) -> Result<JsUnknown> {
    let env = ctx.env();
    match json {
        serde_json::Value::Null => env.get_null().map(|v| v.into_unknown()),
        serde_json::Value::Bool(b) => env.get_boolean(*b).map(|v| v.into_unknown()),
        serde_json::Value::Number(n) => json_number_to_napi(env, n),
        serde_json::Value::String(s) => env.create_string(s).map(|v| v.into_unknown()),
        serde_json::Value::Array(arr) => {
            let mut js_arr = env.create_array_with_length(arr.len())?;
            for (i, item) in arr.iter().enumerate() {
                js_arr.set_element(i as u32, json_to_napi(ctx, item)?)?;
            }
            Ok(js_arr.into_unknown())
        }
        serde_json::Value::Object(obj) => {
            let mut js_obj = env.create_object()?;
            for (k, v) in obj {
                js_obj.set_named_property(k, json_to_napi(ctx, v)?)?;
            }
            Ok(js_obj.into_unknown())
        }
    }
}

/// Convert CQL List to JavaScript Array.
fn list_to_array(ctx: &ConvCtx, items: &[Value]) -> Result<JsUnknown> {
    let env = ctx.env();
    let mut arr = env.create_array_with_length(items.len())?;
    for (i, item) in items.iter().enumerate() {
        let js_value = value_to_napi(ctx, item)?;
        arr.set_element(i as u32, js_value)?;
    }
    Ok(arr.into_unknown())
}

/// Convert CQL Set to JavaScript Set.
///
/// Uses the `Set` constructor cached on `ctx` (Issue #1448): fetched from global
/// at most once per result conversion, not once per set cell.
fn set_to_js_set(ctx: &ConvCtx, items: &[Value]) -> Result<JsUnknown> {
    let env = ctx.env();

    // Create an array of items first
    let mut arr = env.create_array_with_length(items.len())?;
    for (i, item) in items.iter().enumerate() {
        let js_value = value_to_napi(ctx, item)?;
        arr.set_element(i as u32, js_value)?;
    }

    // Create new Set from array: new Set(array)
    let set_instance = ctx.set_constructor()?.new_instance(&[arr])?;
    Ok(set_instance.into_unknown())
}

/// Convert CQL Map to JavaScript Map.
///
/// Uses the `Map` constructor cached on `ctx` (Issue #1448): fetched from global
/// at most once per result conversion, not once per map cell.
fn map_to_js_map(ctx: &ConvCtx, pairs: &[(Value, Value)]) -> Result<JsUnknown> {
    let env = ctx.env();

    // Create an array of [key, value] pairs
    let mut entries = env.create_array_with_length(pairs.len())?;
    for (i, (key, value)) in pairs.iter().enumerate() {
        let mut pair = env.create_array_with_length(2)?;
        pair.set_element(0, value_to_napi(ctx, key)?)?;
        pair.set_element(1, value_to_napi(ctx, value)?)?;
        entries.set_element(i as u32, pair)?;
    }

    // Create new Map from entries: new Map([[k1, v1], [k2, v2], ...])
    let map_instance = ctx.map_constructor()?.new_instance(&[entries])?;
    Ok(map_instance.into_unknown())
}

/// Convert UDT to JavaScript object.
///
/// Creates an object with:
/// - `_type`: The UDT type name
/// - `_keyspace`: The keyspace containing the UDT
/// - All field names as properties
fn udt_to_object(ctx: &ConvCtx, udt: &cqlite_core::UdtValue) -> Result<JsUnknown> {
    let env = ctx.env();
    let mut obj = env.create_object()?;

    // Add type metadata
    obj.set_named_property("_type", env.create_string(&udt.type_name)?)?;
    obj.set_named_property("_keyspace", env.create_string(&udt.keyspace)?)?;

    // Add fields
    for field in &udt.fields {
        let value = match &field.value {
            Some(v) => value_to_napi(ctx, v)?,
            None => env.get_null()?.into_unknown(),
        };
        obj.set_named_property(&field.name, value)?;
    }

    Ok(obj.into_unknown())
}

/// Reusable, once-per-result column-key structure for row construction.
///
/// Issue #1446: both the interned `JsString` handles and the membership set are
/// built a single time per result set (they depend only on the column list,
/// which is constant across every row) so a wide-table scan pays neither the
/// `O(rows × columns)` string re-interning nor a per-row `HashSet` rebuild.
pub struct ColumnKeys {
    /// `(lookup_name, pre-interned JS key)` in authoritative SELECT order.
    /// `JsString` is a `Copy` handle valid for the enclosing `Env` scope.
    ordered: Vec<(String, JsString)>,
    /// Membership set of the ordered names, for O(1) "is this value covered by
    /// the authoritative column list?" checks in [`row_to_object`].
    known: std::collections::HashSet<String>,
}

/// Intern the SELECT-order column names into a reusable [`ColumnKeys`].
///
/// Called once per result set; the returned structure is borrowed for every row.
pub fn intern_column_keys(env: &Env, names: &[String]) -> Result<ColumnKeys> {
    let ordered = names
        .iter()
        .map(|name| Ok((name.clone(), env.create_string(name)?)))
        .collect::<Result<Vec<_>>>()?;
    let known = names.iter().cloned().collect();
    Ok(ColumnKeys { ordered, known })
}

/// Convert row values to a JavaScript object in authoritative SELECT order.
///
/// Issue #1446: property insertion order equals `columns` order (V8 preserves
/// string-key insertion order), so `Object.keys(row)` matches
/// `columns.map(c => c.name)` — not `HashMap` hash order. `keys` are the
/// once-per-result handles from [`intern_column_keys`], reused across every row.
///
/// Issue #1448: `ctx` carries the napi `Env` plus the per-result-cached
/// `Set`/`Map` constructors, threaded into [`value_to_napi`] so a scan with many
/// collection cells fetches each constructor once per result, not once per cell.
pub fn row_to_object(
    ctx: &ConvCtx,
    keys: &ColumnKeys,
    values: &std::collections::HashMap<String, Value>,
) -> Result<JsObject> {
    let env = ctx.env();
    let mut obj = env.create_object()?;
    // Emit the selected columns that are present in this row's values, in
    // authoritative SELECT order (#1446). For the normal case where metadata
    // names match the value keys, this is every column, so `Object.keys(row)`
    // equals `columns.map(c => c.name)`. A metadata column with no matching value
    // is skipped (not null-filled): for aggregate queries core's metadata uses a
    // fallback name like `col_0` while the value is keyed by the expression name
    // like `Count(*)`, and null-filling would emit a phantom `col_0: null`
    // alongside the real cell.
    for (col_name, js_key) in &keys.ordered {
        if let Some(value) = values.get(col_name) {
            let js_value = value_to_napi(ctx, value)?;
            obj.set_property(*js_key, js_value)?;
        }
    }
    // Never drop cells (#1446 roborev): emit any values the authoritative column
    // list does not cover — an aggregate value keyed differently from its
    // metadata name, or a streaming `SELECT *` whose schema lookup failed leaves
    // `metadata.columns` empty while rows are still yielded — in a deterministic
    // (name-sorted) order rather than dropping them or using nondeterministic
    // hash order. Extras are detected by membership against the precomputed
    // `known` set (built once per result), so the common path where metadata
    // covers every cell allocates no set and does no sort.
    let mut extra: Vec<&String> = values
        .keys()
        .filter(|name| !keys.known.contains(name.as_str()))
        .collect();
    if !extra.is_empty() {
        extra.sort();
        for name in extra {
            if let Some(value) = values.get(name) {
                let js_value = value_to_napi(ctx, value)?;
                obj.set_named_property(name, js_value)?;
            }
        }
    }
    Ok(obj)
}

// Unit tests live in a sibling file to keep this file under the campsite
// size limit (#1116); `super::*` there resolves to this `value` module.
#[cfg(test)]
#[path = "value_tests.rs"]
mod tests;
