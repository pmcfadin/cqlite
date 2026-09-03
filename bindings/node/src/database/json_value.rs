//! The DEPRECATED legacy JSON shaping used by `Database::execute()`.
//!
//! Isolated in its own file (issue #1464) so issue #1457's next-major removal
//! of `execute()` + `value_to_json` is a clean single-file delete. Pure code
//! motion: the conversion, its deprecation attributes and its doc comments are
//! unchanged.

use crate::error::to_napi_error;

/// Convert a CQL Value to a JSON value.
///
/// This provides basic type conversion for Phase 2.
/// For native JavaScript types, use `executeNative()` instead.
///
/// ⚠️ Lossy legacy encoding — see `execute()` in `lib/index.d.ts` for the full
/// hazard list. blob → base64 string; timestamp → ISO-8601 string; varint →
/// `"0x{hex}"`; decimal → `"decimal:{scale}:0x{hex}"`; date/time → number.
/// (BigInt/Counter serde numbers are converted by napi to an exact JS `BigInt`
/// on this build, so they are not presently rounded.)
///
/// Fallible on purpose (issue #1452): the `inet` arm used to carry a private
/// 4/16 length dispatch whose malformed-length branch produced a JSON `null`,
/// indistinguishable from a genuine NULL — silent data loss on the `execute()`
/// path while `executeNative()` raised on the same cell. The dispatch now comes
/// from `cqlite_ffi_common::inet` and a refusal propagates to the caller.
///
/// TODO(next-major): remove `execute()` + `value_to_json` (breaking change;
/// deprecated since 0.4.0, callers must migrate to `executeNative()`). See
/// issue #1457. Do NOT remove before the next major bump.
#[deprecated(
    since = "0.4.0",
    note = "Use executeNative() for native JavaScript types"
)]
#[allow(deprecated)]
pub(super) fn value_to_json(value: &cqlite_core::types::Value) -> napi::Result<serde_json::Value> {
    use cqlite_core::types::Value;

    let json = match value {
        Value::Null => serde_json::Value::Null,
        // EMPTY-BUFFER SENTINEL (issue #3805) → the EMPTY JSON STRING, matching
        // `sstabledump`'s `"path" : [ "" ]`
        // (`tools/JsonTransformer.java:444-458` →
        // `db/marshal/AbstractType.java:146-156`, at `cassandra-5.0.8`) and
        // `SELECT JSON`'s `{"": v}` (`db/marshal/MapType.java:362-388`). NOT
        // `null` — the entry is present and the key is distinct from null. All
        // three surfaces render it identically (cross-binding parity, #1455).
        Value::Empty(_) => serde_json::Value::String(String::new()),
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i as i64).into()),
        Value::BigInt(i) => serde_json::Value::Number((*i).into()),
        Value::TinyInt(i) => serde_json::Value::Number((*i as i64).into()),
        Value::SmallInt(i) => serde_json::Value::Number((*i as i64).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Float32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(String::from_utf8_lossy(s).into_owned()),
        Value::Blob(b) => {
            // Convert blob to base64 string
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            serde_json::Value::String(encoded)
        }
        Value::Timestamp(ts) => {
            // Use from_timestamp_millis to correctly handle pre-epoch timestamps
            // (Issue #341: truncating division was incorrect for negative values)
            if let Some(dt) = chrono::DateTime::from_timestamp_millis(*ts) {
                serde_json::Value::String(dt.to_rfc3339())
            } else {
                serde_json::Value::Number((*ts).into())
            }
        }
        Value::Date(d) => {
            // Days since epoch as number (Cassandra format)
            serde_json::Value::Number((*d as i64).into())
        }
        Value::Time(t) => {
            // Nanoseconds since midnight as number
            serde_json::Value::Number((*t).into())
        }
        Value::Uuid(bytes) => {
            // Format as UUID string
            let uuid = uuid::Uuid::from_bytes(*bytes);
            serde_json::Value::String(uuid.to_string())
        }
        Value::Varint(bytes) => {
            // Convert to hex string for large integers
            let hex_str = hex::encode(bytes);
            serde_json::Value::String(format!("0x{hex_str}"))
        }
        Value::Decimal { scale, unscaled } => {
            // Represent as string to preserve precision
            let hex_str = hex::encode(unscaled);
            serde_json::Value::String(format!("decimal:{scale}:0x{hex_str}"))
        }
        Value::Duration {
            months,
            days,
            nanos,
        } => {
            serde_json::json!({
                "months": months,
                "days": days,
                "nanos": nanos
            })
        }
        // The 4/16 dispatch and the malformed-length message are decided ONCE,
        // in the shared crate, so this legacy JSON shaping cannot drift from
        // `value_to_napi`'s native shaping (issue #1452). There is no
        // passthrough arm and no silent null: a malformed length is a typed
        // refusal carrying the one FFI error contract's identity.
        Value::Inet(bytes) => serde_json::Value::String(
            cqlite_ffi_common::inet::inet_bytes_to_string(bytes)
                .map_err(|err| to_napi_error(cqlite_core::Error::corruption(err.to_string())))?,
        ),
        Value::List(items) => serde_json::Value::Array(json_array(items)?),
        Value::Set(items) => serde_json::Value::Array(json_array(items)?),
        Value::Map(pairs) => {
            // Convert map to object if keys are strings, otherwise array of pairs
            let all_string_keys = pairs.iter().all(|(k, _)| matches!(k, Value::Text(_)));

            if all_string_keys {
                let mut obj = serde_json::Map::with_capacity(pairs.len());
                for (k, v) in pairs {
                    if let Value::Text(s) = k {
                        obj.insert(String::from_utf8_lossy(s).into_owned(), value_to_json(v)?);
                    }
                }
                serde_json::Value::Object(obj)
            } else {
                let mut entries = Vec::with_capacity(pairs.len());
                for (k, v) in pairs {
                    entries.push(serde_json::json!({
                        "key": value_to_json(k)?,
                        "value": value_to_json(v)?
                    }));
                }
                serde_json::Value::Array(entries)
            }
        }
        Value::Tuple(items) => serde_json::Value::Array(json_array(items)?),
        Value::Udt(udt) => {
            let mut obj = serde_json::Map::with_capacity(udt.fields.len());
            for field in &udt.fields {
                let value = match field.value.as_ref() {
                    Some(inner) => value_to_json(inner)?,
                    None => serde_json::Value::Null,
                };
                obj.insert(field.name.clone(), value);
            }
            serde_json::Value::Object(obj)
        }
        Value::Frozen(inner) => value_to_json(inner)?,
        Value::Json(json_value) => {
            // Value::Json contains a boxed serde_json::Value, return it directly
            (**json_value).clone()
        }
        Value::Tombstone(_) => serde_json::Value::Null,
        Value::Counter(c) => serde_json::Value::Number((*c).into()),
    };

    Ok(json)
}

/// Convert each element of a `list`/`set`/`tuple`, short-circuiting on the first
/// refusal so a nested malformed cell cannot be flattened into a null.
#[deprecated(
    since = "0.4.0",
    note = "Use executeNative() for native JavaScript types"
)]
#[allow(deprecated)]
fn json_array(items: &[cqlite_core::types::Value]) -> napi::Result<Vec<serde_json::Value>> {
    items.iter().map(value_to_json).collect()
}

// The legacy `value_to_json` unit tests live in a sibling file. They were split
// out while this code was still inside the over-threshold `database.rs` (#1116);
// at 179 lines this module is now well under the threshold, so the split is no
// longer forced. It is kept because #1457 deletes this whole deprecated path in
// the next major, and two adjacent files go as cleanly as one.
//
// `#[path]` is required (not decorative): without it a child of a non-`mod.rs`
// file module is looked for in `database/json_value/`, and the test file is a
// sibling of `json_value.rs` in `database/`. With it, the base is this file's
// own directory — verified against rustc, not assumed.
#[cfg(test)]
#[path = "legacy_json_tests.rs"]
mod legacy_json_tests;
