//! One CQL value rendered for the `--format json` egress.
//!
//! # Why this is not just a `serde_json::Value` (issue #3644 item 3)
//!
//! Cassandra renders a `decimal` and a `varint` as UNQUOTED JSON NUMBERS of
//! arbitrary precision. Authority, read at the pinned tag:
//!
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/DecimalType.java:314-317`
//!   — `toJSONString` returns `Objects.toString(getSerializer().deserialize(buffer), "\"\"")`,
//!   i.e. a bare `BigDecimal.toString()`. It deliberately OVERRIDES
//!   `AbstractType.java:186-189`, which is the QUOTING form, so the absence of
//!   quotes here is a decision Cassandra made explicitly.
//! * `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/IntegerType.java:488-491`
//!   — the `varint` `toJSONString` has the identical shape, also unquoted.
//! * `tools/JsonTransformer.java:494` writes a cell VALUE with
//!   `writeRawValue(cellType.toJSONString(...))`, so that text reaches the
//!   document verbatim, unquoted.
//!
//! A `decimal`'s unscaled value is a Java `BigInteger`, so its rendering is
//! legitimately longer than any `f64` — the committed
//! `test_signed_coll.signed_special_collections` fixture carries
//! `-999999999999999999999999999999.999` (33 significant digits). A
//! `serde_json::Number` cannot hold that without the `arbitrary_precision`
//! feature, which this crate deliberately does not enable (see
//! `cqlite-cli/Cargo.toml`), and a `serde_json::Value` cannot hold a raw
//! fragment at all. So the rendered cell is its own small tree whose numeric
//! leaves are `serde_json::value::RawValue` fragments — the already-rendered
//! digits, written through unchanged — and everything else is an ordinary
//! `serde_json::Value`.
//!
//! # Fail-safe: a raw fragment must be VALID JSON
//!
//! `ValueFormatter::format_value` is total and renders an over-bound `decimal`
//! as the marker `<corrupt-decimal:scale=…,unscaled_len=…bytes>`
//! (`cqlite-core/src/util/value_fmt.rs`). Emitting that raw would produce an
//! INVALID JSON document — a far worse failure than a quoted number — so the raw
//! path is taken only when the formatted text is valid JSON that begins like a
//! number, and any other text falls back to a JSON STRING (the pre-#3644
//! rendering). `serde_json` itself is the authority on validity here; nothing
//! re-implements the JSON number grammar.

use cqlite_core::util::udt_json::udt_render_fields;
use cqlite_core::Value;
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::value::RawValue;
use serde_json::Value as JsonValue;

use super::value_fmt::ValueFormatter;

/// A CQL value rendered for JSON egress.
///
/// `Array`/`Object` exist so a `decimal` NESTED in a collection, a tuple, a map
/// value or a UDT field keeps its unquoted rendering — the divergence is a
/// property of the TYPE, not of the position, and the fixture that exposed it
/// (`set<decimal>`) is a nested one.
pub(crate) enum JsonCell {
    /// Anything `serde_json::Value` renders correctly on its own.
    Plain(JsonValue),
    /// A pre-VALIDATED raw JSON fragment, written through verbatim. Only ever
    /// built by [`numeric_cell`], which refuses anything that is not valid JSON
    /// beginning like a number.
    Raw(Box<RawValue>),
    Array(Vec<JsonCell>),
    /// Entries in emission order (a map entry's `key`/`value`, a UDT's declared
    /// fields).
    Object(Vec<(String, JsonCell)>),
}

impl Serialize for JsonCell {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            JsonCell::Plain(v) => v.serialize(serializer),
            JsonCell::Raw(raw) => raw.serialize(serializer),
            JsonCell::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            JsonCell::Object(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

/// Render `value` as an UNQUOTED JSON number when its text is one, else as a
/// JSON string.
///
/// The guard is deliberately two-part and neither part is a JSON parser of our
/// own:
///
/// 1. the text must BEGIN with `-` or a digit — in JSON no value other than a
///    number can, so a valid document starting that way IS a number (this also
///    rejects the `<corrupt-decimal:…>` marker without allocating); and
/// 2. `serde_json` must accept the whole text as one JSON document
///    (`RawValue::from_string`, which is `from_str::<&RawValue>` plus a move and
///    therefore rejects trailing content too).
///
/// Anything else keeps the pre-#3644 quoted rendering: a wrong KIND is a
/// documented divergence, while invalid JSON is unparseable output.
fn numeric_cell(value: &Value) -> JsonCell {
    let text = ValueFormatter::format_value(value);
    if !matches!(text.as_bytes().first(), Some(b'-' | b'0'..=b'9')) {
        return JsonCell::Plain(JsonValue::String(text));
    }
    match RawValue::from_string(text) {
        Ok(raw) => JsonCell::Raw(raw),
        // `from_string` consumes the text on the error path, so the fallback
        // re-renders it. Unreachable for anything `format_varint`/`format_decimal`
        // can produce today; kept because "the formatter's output set" is not a
        // property this module can enforce.
        Err(_) => JsonCell::Plain(JsonValue::String(ValueFormatter::format_value(value))),
    }
}

impl JsonCell {
    /// Convert a CQLite [`Value`] to its JSON egress rendering.
    ///
    /// Uses string representations for complex types to ensure human
    /// readability.
    pub(crate) fn from_value(value: &Value) -> JsonCell {
        match value {
            Value::Null => JsonCell::Plain(JsonValue::Null),
            Value::Boolean(b) => JsonCell::Plain(JsonValue::Bool(*b)),
            Value::Integer(i) => JsonCell::Plain(JsonValue::Number((*i).into())),
            Value::BigInt(i) => JsonCell::Plain(JsonValue::Number((*i).into())),
            Value::Counter(c) => JsonCell::Plain(JsonValue::Number((*c).into())),
            Value::TinyInt(i) => JsonCell::Plain(JsonValue::Number((*i as i64).into())),
            Value::SmallInt(i) => JsonCell::Plain(JsonValue::Number((*i as i64).into())),
            // A non-finite float renders as JSON `null`, matching
            // `cassandra-5.0.8:.../marshal/DoubleType.java:114-123` (and
            // `FloatType.java:115-124`), whose `toJSONString` returns the literal
            // `null` with the in-source comment "JSON does not support NaN,
            // Infinity and -Infinity values. Most of the parser convert them into
            // null." `Number::from_f64` is `None` for exactly those three.
            Value::Float(f) => JsonCell::Plain(
                serde_json::Number::from_f64(*f)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null),
            ),
            Value::Float32(f) => JsonCell::Plain(
                serde_json::Number::from_f64(*f as f64)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null),
            ),
            Value::Text(s) => {
                JsonCell::Plain(JsonValue::String(String::from_utf8_lossy(s).into_owned()))
            }
            // Use ValueFormatter for human-readable Blob formatting (0x... hex)
            Value::Blob(_) => {
                JsonCell::Plain(JsonValue::String(ValueFormatter::format_value(value)))
            }
            // Use ValueFormatter for human-readable Timestamp (YYYY-MM-DD HH:MM:SS.fff+0000)
            Value::Timestamp(_) => {
                JsonCell::Plain(JsonValue::String(ValueFormatter::format_value(value)))
            }
            // Use ValueFormatter for human-readable Date (YYYY-MM-DD)
            Value::Date(_) => {
                JsonCell::Plain(JsonValue::String(ValueFormatter::format_value(value)))
            }
            // Use ValueFormatter for human-readable Time (HH:MM:SS.nnnnnnnnn)
            Value::Time(_) => {
                JsonCell::Plain(JsonValue::String(ValueFormatter::format_value(value)))
            }
            Value::Uuid(uuid) => {
                // Format UUID via the shared hex lookup-table encoder (issue #1499)
                // instead of a 16-arg `format!` per cell.
                let mut uuid_str = String::with_capacity(36);
                ValueFormatter::format_uuid_into(uuid, &mut uuid_str);
                JsonCell::Plain(JsonValue::String(uuid_str))
            }
            // An UNQUOTED JSON number, per `IntegerType.toJSONString:488-491`
            // (module docs). The digits are `ValueFormatter`'s, so the JSON and
            // the table/CSV egress still render the same text.
            Value::Varint(_) => numeric_cell(value),
            // An UNQUOTED JSON number, per `DecimalType.toJSONString:314-317`.
            Value::Decimal { .. } => numeric_cell(value),
            // Use ValueFormatter for human-readable Duration (XmoYdZns format)
            Value::Duration { .. } => {
                JsonCell::Plain(JsonValue::String(ValueFormatter::format_value(value)))
            }
            Value::Json(j) => JsonCell::Plain((**j).clone()),
            Value::List(list) => JsonCell::Array(list.iter().map(Self::from_value).collect()),
            Value::Set(set) => JsonCell::Array(set.iter().map(Self::from_value).collect()),
            Value::Map(map) => {
                // Maps are Vec<(Value, Value)> in CQLite
                // Represent as array of {"key": k, "value": v} objects for clarity
                JsonCell::Array(
                    map.iter()
                        .map(|(k, v)| {
                            JsonCell::Object(vec![
                                ("key".to_string(), Self::from_value(k)),
                                ("value".to_string(), Self::from_value(v)),
                            ])
                        })
                        .collect(),
                )
            }
            Value::Tuple(tuple) => JsonCell::Array(tuple.iter().map(Self::from_value).collect()),
            // Declared fields and NOTHING else — no injected `_type` (issue
            // #3629): type identity must not share the user's field namespace.
            // One shared rule (`udt_render_fields`), each writer keeping its own
            // field-value renderer.
            Value::Udt(udt) => {
                let mut entries: Vec<(String, JsonCell)> = Vec::with_capacity(udt.fields.len());
                udt_render_fields(
                    udt,
                    Self::from_value,
                    || JsonCell::Plain(JsonValue::Null),
                    |name, rendered| entries.push((name.to_string(), rendered)),
                );
                JsonCell::Object(entries)
            }
            Value::Frozen(boxed_value) => Self::from_value(boxed_value),
            // Tombstoned cells represent deleted values. Emit JSON null to match
            // cqlsh and Python binding behaviour (issue #806).
            Value::Tombstone(_) => JsonCell::Plain(JsonValue::Null),
            Value::Inet(bytes) => {
                // Format as IP address string if possible
                if bytes.len() == 4 {
                    JsonCell::Plain(JsonValue::String(format!(
                        "{}.{}.{}.{}",
                        bytes[0], bytes[1], bytes[2], bytes[3]
                    )))
                } else if bytes.len() == 16 {
                    // IPv6 - use std::net::Ipv6Addr for canonical formatting
                    use std::net::Ipv6Addr;
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(bytes);
                    let addr = Ipv6Addr::from(octets);
                    JsonCell::Plain(JsonValue::String(addr.to_string()))
                } else {
                    // Invalid length, encode as base64
                    use base64::Engine;
                    let engine = base64::engine::general_purpose::STANDARD;
                    JsonCell::Plain(JsonValue::String(engine.encode(bytes)))
                }
            }
        }
    }
}

#[cfg(test)]
impl JsonCell {
    /// The cell's own JSON text, as the writer emits it. Tests assert on this
    /// rather than on a re-parsed `serde_json::Value`, because the whole point of
    /// [`JsonCell::Raw`] is a number no `serde_json::Number` can hold: a
    /// round-trip through the parser would destroy exactly the digits under test.
    fn to_json_text(value: &Value) -> String {
        serde_json::to_string(&JsonCell::from_value(value)).expect("cell serializes")
    }
}

#[cfg(test)]
mod tests;
