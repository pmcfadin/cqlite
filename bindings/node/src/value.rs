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
        Value::Text(s) => env.create_string(s).map(|s| s.into_unknown()),

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
            let decimal_str = decimal_to_string(*scale, unscaled);
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

/// Convert variable-length integer bytes to JavaScript BigInt.
///
/// Varint is stored as big-endian two's complement bytes.
fn varint_to_bigint(env: &Env, bytes: &[u8]) -> Result<JsUnknown> {
    if bytes.is_empty() {
        return env.create_bigint_from_i64(0)?.into_unknown();
    }

    // Determine sign from high bit
    let is_negative = (bytes[0] & 0x80) != 0;

    // For small varints that fit in i64, use the direct method
    if bytes.len() <= 8 {
        let mut value: i64 = 0;
        for &byte in bytes {
            value = (value << 8) | (byte as i64);
        }
        // Sign extend if negative
        if is_negative && bytes.len() < 8 {
            let sign_bits = !0i64 << (bytes.len() * 8);
            value |= sign_bits;
        }
        return env.create_bigint_from_i64(value)?.into_unknown();
    }

    // For larger varints, convert to u64 words for BigInt creation
    // napi's create_bigint_from_words expects little-endian u64 words
    let mut words: Vec<u64> = Vec::new();

    // Pad bytes to multiple of 8 for processing
    let padded_len = bytes.len().div_ceil(8) * 8;
    let mut padded = vec![if is_negative { 0xFF } else { 0x00 }; padded_len];
    padded[padded_len - bytes.len()..].copy_from_slice(bytes);

    // Convert to little-endian u64 words
    for chunk in padded.chunks(8).rev() {
        let word = u64::from_be_bytes(
            chunk
                .try_into()
                .map_err(|_| napi::Error::from_reason("Invalid varint chunk size"))?,
        );
        words.push(word);
    }

    // For negative numbers in two's complement, napi expects the magnitude
    // with a sign flag, not raw two's complement
    if is_negative {
        // Negate: invert all bits and add 1
        let mut carry = 1u64;
        for word in &mut words {
            *word = !*word;
            let (new_val, new_carry) = word.overflowing_add(carry);
            *word = new_val;
            carry = if new_carry { 1 } else { 0 };
        }
    }

    env.create_bigint_from_words(is_negative, words)?
        .into_unknown()
}

/// Convert decimal to string representation for arbitrary precision.
///
/// Format: Represents the decimal as an exact string.
/// For example: scale=2, unscaled=[1, 23] (123) -> "1.23"
fn decimal_to_string(scale: i32, unscaled: &[u8]) -> String {
    if unscaled.is_empty() {
        return "0".to_string();
    }

    // Determine sign from high bit (two's complement)
    let is_negative = (unscaled[0] & 0x80) != 0;

    // Convert bytes to absolute magnitude
    let mut magnitude = unscaled.to_vec();
    if is_negative {
        // Two's complement negation
        let mut carry = true;
        for byte in magnitude.iter_mut().rev() {
            *byte = !*byte;
            if carry {
                let (new_val, new_carry) = byte.overflowing_add(1);
                *byte = new_val;
                carry = new_carry;
            }
        }
    }

    // Convert bytes to decimal string using repeated division
    let mut digits = String::new();
    while !magnitude.is_empty() && magnitude.iter().any(|&b| b != 0) {
        let mut remainder = 0u32;
        for byte in &mut magnitude {
            let dividend = remainder * 256 + (*byte as u32);
            *byte = (dividend / 10) as u8;
            remainder = dividend % 10;
        }
        digits.push(char::from_digit(remainder, 10).unwrap());
        // Remove leading zeros from magnitude
        while magnitude.first() == Some(&0) {
            magnitude.remove(0);
        }
    }

    if digits.is_empty() {
        digits = "0".to_string();
    } else {
        // Reverse since we built it backwards
        digits = digits.chars().rev().collect();
    }

    // Apply scale
    let result = if scale == 0 {
        digits
    } else if scale > 0 {
        // Positive scale means decimal point moves left
        let scale_usize = scale as usize;
        if digits.len() <= scale_usize {
            // Need leading zeros: 123 with scale 5 -> 0.00123
            format!("0.{digits:0>scale_usize$}")
        } else {
            // Insert decimal point
            let split_point = digits.len() - scale_usize;
            let int_part = &digits[..split_point];
            let frac_part = &digits[split_point..];
            format!("{int_part}.{frac_part}")
        }
    } else {
        // Negative scale means multiply by power of 10
        let neg_scale = -scale;
        format!("{digits}e{neg_scale}")
    };

    if is_negative {
        format!("-{result}")
    } else {
        result
    }
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

/// Convert inet bytes to IP address string.
fn inet_to_string_js(env: &Env, bytes: &[u8]) -> Result<JsUnknown> {
    let ip_str = match bytes.len() {
        4 => {
            let ip = std::net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
            ip.to_string()
        }
        16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            std::net::Ipv6Addr::from(arr).to_string()
        }
        _ => {
            return Err(napi::Error::from_reason(format!(
                "Invalid inet address length: {} (expected 4 or 16)",
                bytes.len()
            )))
        }
    };
    env.create_string(&ip_str).map(|s| s.into_unknown())
}

/// Convert serde_json::Value to JavaScript value.
fn json_to_napi(ctx: &ConvCtx, json: &serde_json::Value) -> Result<JsUnknown> {
    let env = ctx.env();
    match json {
        serde_json::Value::Null => env.get_null().map(|v| v.into_unknown()),
        serde_json::Value::Bool(b) => env.get_boolean(*b).map(|v| v.into_unknown()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                // Check if it fits in i32 for JavaScript number
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    env.create_int32(i as i32).map(|v| v.into_unknown())
                } else {
                    // Use BigInt for large integers
                    env.create_bigint_from_i64(i)?.into_unknown()
                }
            } else if let Some(f) = n.as_f64() {
                env.create_double(f).map(|v| v.into_unknown())
            } else {
                env.get_null().map(|v| v.into_unknown())
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes every test that resets/reads the process-global
    /// `ctor_lookups` counter. The increment site lives in library code (a true
    /// process-global, unlike the local-instance trick in `cqlite-core`'s
    /// `work_counters`), so two `reset`-then-assert tests running under Rust's
    /// default parallel runner would race. Both counter tests take this guard.
    static CTOR_COUNTER_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_decimal_to_string_positive() {
        // 123 with scale 2 = 1.23
        let unscaled = vec![123];
        assert_eq!(decimal_to_string(2, &unscaled), "1.23");
    }

    #[test]
    fn test_decimal_to_string_no_scale() {
        // 123 with scale 0 = 123
        let unscaled = vec![123];
        assert_eq!(decimal_to_string(0, &unscaled), "123");
    }

    #[test]
    fn test_decimal_to_string_negative_scale() {
        // 123 with scale -2 = 12300 (123e2)
        let unscaled = vec![123];
        assert_eq!(decimal_to_string(-2, &unscaled), "123e2");
    }

    #[test]
    fn test_decimal_to_string_large_scale() {
        // 123 with scale 5 = 0.00123
        let unscaled = vec![123];
        assert_eq!(decimal_to_string(5, &unscaled), "0.00123");
    }

    #[test]
    fn test_decimal_to_string_empty() {
        assert_eq!(decimal_to_string(0, &[]), "0");
    }

    #[test]
    fn test_decimal_to_string_negative() {
        // -123 in two's complement (single byte) = 0x85 = 133, but need proper encoding
        // For -123: 256 - 123 = 133 = 0x85
        let unscaled = vec![0x85]; // -123 as two's complement byte
        assert_eq!(decimal_to_string(2, &unscaled), "-1.23");
    }

    // Issue #1448: prove the constructor-caching invariant without a live JS
    // `Env`. `cache_get_or_try_init` is the single fetch-vs-cached decision point
    // both `set_constructor` and `map_constructor` delegate to; exercising it with
    // a plain `OnceCell<T>` and a counting `fetch` reproduces exactly the caching
    // logic those methods use, so the work counter (bumped only on the fetch path)
    // proves the "at most one lookup per cache, zero when unused" invariant.
    //
    // Both the zero-lookups case and the once-per-cache case live in a SINGLE
    // test on purpose: the counter is a process-global (the increment site is in
    // library code, unlike the local-instance trick in `cqlite-core`'s
    // `work_counters`), so splitting them into two `reset`-then-assert tests would
    // race under Rust's default parallel test runner. Tests that touch the counter
    // serialize on `CTOR_COUNTER_GUARD`.
    #[test]
    fn ctor_cache_fetches_at_most_once_per_cache() {
        let _guard = CTOR_COUNTER_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        testing::reset_ctor_lookups();

        // Zero lookups when no set/map cell is ever converted (a `ConvCtx` never
        // reaches `cache_get_or_try_init` unless a collection cell needs a ctor).
        assert_eq!(testing::ctor_lookups(), 0);

        let cell: OnceCell<u32> = OnceCell::new();

        // First access: cache miss -> exactly one fetch (counter goes 0 -> 1).
        let first = cache_get_or_try_init(&cell, || Ok(7u32)).expect("first init");
        assert_eq!(*first, 7);
        assert_eq!(testing::ctor_lookups(), 1);

        // Second access on the SAME cell: cache hit -> NO further fetch. This is
        // the per-cell repeat that used to re-`get_global()` before #1448.
        let second = cache_get_or_try_init(&cell, || {
            panic!("must not fetch again once cached");
        })
        .expect("second hit");
        assert_eq!(*second, 7);
        assert_eq!(testing::ctor_lookups(), 1);
    }

    // Issue #1449: FFI-call BUDGET ratchet for the #1448 constructor-caching win.
    //
    // The `ctor_lookups` counter is Rust-`#[cfg(test)]` only and NOT exposed to
    // JS, so per the issue this FFI-call budget is asserted here (Rust) while the
    // JS test owns the per-row heap-delta budget.
    //
    // A `ConvCtx` lives for a WHOLE result conversion; its two `OnceCell`s back
    // the `Set` and `Map` constructor caches. `row_to_object` -> `value_to_napi`
    // routes every set/map cell through `set_constructor`/`map_constructor`, both
    // of which delegate to the single `cache_get_or_try_init` fetch-vs-cached
    // decision point. So converting a wide result of ROWS rows, each with several
    // set AND map cells, must still fetch each global constructor at most once for
    // the entire result — total lookups <= 2 (one Set cache + one Map cache),
    // regardless of row/cell count. A regression to per-cell `get_global()` would
    // make this O(rows x collection-cells).
    //
    // This exercises `cache_get_or_try_init` directly on two shared cells (exactly
    // what `set_constructor`/`map_constructor` delegate to) because instantiating a
    // real `ConvCtx` needs a live napi `Env`, which is unavailable in a unit test.
    #[test]
    fn set_map_ctor_lookups_bounded_per_result() {
        let _guard = CTOR_COUNTER_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        testing::reset_ctor_lookups();

        // One pair of caches shared across the whole simulated result, as a real
        // per-result `ConvCtx` holds.
        let set_cell: OnceCell<u32> = OnceCell::new();
        let map_cell: OnceCell<u32> = OnceCell::new();

        const ROWS: usize = 200;
        const COLLECTION_CELLS_PER_ROW: usize = 5;
        for _ in 0..ROWS {
            for _ in 0..COLLECTION_CELLS_PER_ROW {
                let _ = cache_get_or_try_init(&set_cell, || Ok(1u32)).expect("set ctor");
                let _ = cache_get_or_try_init(&map_cell, || Ok(2u32)).expect("map ctor");
            }
        }

        // 2 caches, each accessed ROWS * COLLECTION_CELLS_PER_ROW = 1000 times,
        // but each fetched exactly once -> total 2. Budget is 2 (<=1 per cache).
        let lookups = testing::ctor_lookups();
        assert!(
            lookups <= 2,
            "constructor lookups {lookups} exceeded FFI-call budget of 2 \
             (<=1 per Set/Map cache per result); a regression to per-cell \
             get_global() would make this O(rows x collection-cells) — see #1449"
        );
    }
}
