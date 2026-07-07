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

/// Sanity ceiling on the unscaled-magnitude byte length this converter will
/// render (issue #1754). A Cassandra `decimal` unscaled value is a Java
/// `BigInteger` — legitimately arbitrary-precision — so a merely-large value is
/// NOT corrupt and must render faithfully. The only hard cost is the single
/// `BigInt` → decimal-string base conversion (superlinear in digit count); a
/// 32 KB magnitude (~79k digits) still converts in tens of milliseconds even in
/// a debug build. Only a genuinely pathological magnitude beyond that could
/// stall the JS event-loop resolve() thread (`row_to_object`, database.rs, NOT
/// inside the catch_unwind-firewalled worker), so we fail closed with a typed
/// corruption error ONLY above this ceiling.
const DECIMAL_MAX_UNSCALED_BYTES: usize = 32 * 1024;

/// Byte-length threshold above which a well-formed magnitude is rendered in
/// precision-preserving exponent form rather than an O(digits)-wide positional
/// expansion (issue #1754). At/under 1024 bytes (~2466 digits) the positional
/// render is cheap and byte-identical to the historical output; beyond it we
/// emit `<digits>e<-scale>` (exact, bounded) to avoid superlinear padding work.
const DECIMAL_POSITIONAL_MAX_BYTES: usize = 1024;

/// Threshold on `scale.abs()` above which the value is rendered in exponent form
/// instead of positional (issue #1754). `scale` would otherwise drive a
/// `format!` padding width / leading-zero `repeat`; a huge scale (e.g.
/// `i32::MAX`) would panic ("Formatting argument out of range") or allocate an
/// unbounded string. Exponent form is exact and bounded, so no scale value is
/// rejected — a well-formed decimal always renders.
const DECIMAL_MAX_SCALE_DIGITS: usize = 1_000_000;

/// Convert decimal to string representation for arbitrary precision.
///
/// Format: Represents the decimal as an exact string.
/// For example: scale=2, unscaled=[1, 23] (123) -> "1.23"
///
/// # Errors
///
/// Returns a typed corruption error (never panics/aborts) ONLY when the unscaled
/// magnitude exceeds the sanity ceiling — a size that could not come from a
/// legitimate value and whose single base-10 conversion would stall the event
/// loop (issue #1754). A merely-large-but-well-formed decimal is NOT corrupt: it
/// renders faithfully in precision-preserving exponent form.
fn decimal_to_string(scale: i32, unscaled: &[u8]) -> Result<String> {
    if unscaled.is_empty() {
        return Ok("0".to_string());
    }

    // Sanity ceiling (issue #1754): O(1) length check BEFORE the one superlinear
    // base conversion. Only a genuinely pathological magnitude is rejected; a
    // well-formed arbitrary-precision value renders below.
    if unscaled.len() > DECIMAL_MAX_UNSCALED_BYTES {
        return Err(to_napi_error(cqlite_core::Error::corruption(format!(
            "DECIMAL cell not representable (scale={scale}, unscaled_len={} bytes, \
             max_unscaled={DECIMAL_MAX_UNSCALED_BYTES} bytes): corrupt SSTable — \
             refusing to enter a superlinear render on a pathological magnitude \
             (issue #1754)",
            unscaled.len()
        ))));
    }

    // Cassandra encodes the unscaled value as a two's-complement big-endian Java
    // BigInteger. ONE base-10 conversion (the sole superlinear step) yields the
    // digit string; every branch below is a single O(digits) pass over it — no
    // repeated division, no scale-width padding blowup.
    let bigint = num_bigint::BigInt::from_signed_bytes_be(unscaled);
    let full = bigint.to_string();
    let (is_negative, digits) = match full.strip_prefix('-') {
        Some(rest) => (true, rest.to_string()),
        None => (false, full),
    };

    // Precision-preserving exponent form for over-bound cases (issue #1754): a
    // large magnitude (thousands+ of digits) or a pathological scale (which as a
    // padding width would panic / allocate unbounded, and at `i32::MIN` would
    // overflow `-scale`). `<digits>e<-scale>` preserves every digit exactly.
    let result = if unscaled.len() > DECIMAL_POSITIONAL_MAX_BYTES
        || (scale.unsigned_abs() as usize) > DECIMAL_MAX_SCALE_DIGITS
    {
        if scale == 0 {
            digits
        } else {
            // `i64` avoids the `(-scale)` overflow at `scale == i32::MIN`.
            format!("{digits}e{}", -(scale as i64))
        }
    } else if scale == 0 {
        digits
    } else if scale > 0 {
        // Positive scale means decimal point moves left.
        let scale_usize = scale as usize;
        if digits.len() <= scale_usize {
            // Need leading zeros: 123 with scale 5 -> 0.00123
            format!("0.{digits:0>scale_usize$}")
        } else {
            // Insert decimal point.
            let split_point = digits.len() - scale_usize;
            let int_part = &digits[..split_point];
            let frac_part = &digits[split_point..];
            format!("{int_part}.{frac_part}")
        }
    } else {
        // Negative scale means multiply by power of 10.
        format!("{digits}e{}", -scale)
    };

    if is_negative {
        Ok(format!("-{result}"))
    } else {
        Ok(result)
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

/// Format inet bytes into an IP-address string, or return a typed error message
/// for a malformed length.
///
/// A CQL `inet` value is authoritatively 4 (IPv4) or 16 (IPv6) bytes; any other
/// length is corrupt data. Per the no-heuristics mandate (issue #28) we surface a
/// typed error naming the bad length rather than inventing a passthrough — this
/// is the reference behavior the Python binding was aligned to (issue #1453).
///
/// Pure (no napi `Env`) so it is unit-testable; `inet_to_string_js` wraps it.
fn inet_bytes_to_string(bytes: &[u8]) -> std::result::Result<String, String> {
    match bytes.len() {
        4 => Ok(std::net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]).to_string()),
        16 => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            Ok(std::net::Ipv6Addr::from(arr).to_string())
        }
        n => Err(format!(
            "Invalid inet address length: {n} (expected 4 or 16)"
        )),
    }
}

/// Convert inet bytes to IP address string.
fn inet_to_string_js(env: &Env, bytes: &[u8]) -> Result<JsUnknown> {
    let ip_str = inet_bytes_to_string(bytes).map_err(napi::Error::from_reason)?;
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

// Unit tests live in a sibling file to keep this file under the campsite
// size limit (#1116); `super::*` there resolves to this `value` module.
#[cfg(test)]
#[path = "value_tests.rs"]
mod tests;
