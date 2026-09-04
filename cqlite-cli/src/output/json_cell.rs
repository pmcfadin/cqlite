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
//!
//! The corruption marker is the only rendering known to reach that fallback. A
//! ZERO magnitude at a NEGATIVE scale used to reach it too — `format_decimal`
//! spelled it `00`, which JSON forbids — and that was fixed AT THE FORMATTER
//! (#3644): it now renders `0e1`, a valid JSON number, in the same bounded
//! exponent form the #1754 branch already used. So guard 1 (the leading
//! character) catches every rendering that is known to be unusable, and guard 2
//! is retained as defence in depth over a formatter that promises no JSON
//! property — see the `Err` arm of [`numeric_cell`].

use cqlite_core::util::udt_json::udt_render_fields;
use cqlite_core::Value;
use indexmap::IndexMap;
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
    ///
    /// A fragment is always SINGLE-LINE (`ValueFormatter`'s digits, with no
    /// embedded newline), which is what keeps the streaming writer's pretty path
    /// correct: it re-indents a row by walking `json_str.lines()`
    /// (`cqlite-cli/src/output/json.rs`), so a multi-line fragment would be
    /// re-indented as if it were separate lines.
    Raw(Box<RawValue>),
    Array(Vec<JsonCell>),
    /// Entries in emission order (a map entry's `key`/`value`, a UDT's declared
    /// fields), keyed so a repeated name collapses to ONE JSON key.
    ///
    /// `IndexMap` is the container BECAUSE of that collapse: `insert` keeps a
    /// key's FIRST position and writes its LAST value, which is exactly what the
    /// `serde_json::Map` this displaced did (this workspace builds serde_json with
    /// `preserve_order`, so that type IS an `IndexMap`). Two JSON keys of one name
    /// is an ambiguous document that parsers resolve differently, and a duplicate
    /// UDT field name IS constructible — see `Value::Udt` in
    /// [`JsonCell::from_value`]. Taking the rule from the container is what keeps
    /// a hand-rolled dedupe out of this file.
    Object(IndexMap<String, JsonCell>),
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
    // `from_string` CONSUMES the text on the error path and `serde_json::Error`
    // does not hand it back, so the fallback needs its own copy. A clone (one
    // memcpy) is paid on the numeric path; the alternative — re-calling
    // `format_value` in the `Err` arm — pays a second BigInt → base-10
    // conversion, which is the superlinear step `format_decimal` documents as its
    // sole hard cost, on the very inputs (tens of thousands of digits) where it
    // hurts most.
    match RawValue::from_string(text.clone()) {
        Ok(raw) => JsonCell::Raw(raw),
        // NO KNOWN INPUT REACHES THIS ARM, and that is stated rather than implied.
        // It HAD one: `Value::Decimal { scale: -1, unscaled: vec![0x00] }` —
        // `BigInteger.ZERO` at a negative scale, Cassandra's `0E+1` — formatted as
        // `00`, which passes guard 1 (a leading digit) and `serde_json` then
        // rejects, JSON forbidding a leading zero followed by another digit. That
        // was fixed at the FORMATTER (#3644): `format_decimal` now renders the
        // zero-magnitude/negative-scale case in its existing bounded exponent form
        // (`0e1`), so the whole class is a valid JSON number before it gets here.
        // The other non-numeric rendering, the `<corrupt-decimal:…>` marker, never
        // reaches guard 2 either — guard 1 rejects its leading `<`.
        //
        // The arm STAYS, for two reasons. `format_decimal` guarantees nothing about
        // JSON: it is a text formatter whose contract is totality, so a future
        // spelling change there must degrade to a quoted string here rather than
        // emit an unparseable document. And removing it would mean `expect()`ing
        // the `Result`, which library code may not do. It is therefore defence in
        // depth over an unreachable state — deliberately NOT covered by a test,
        // because no `Value` can construct the text that would exercise it, and a
        // test seam that could would be a settable hole in the fail-safe.
        Err(_) => JsonCell::Plain(JsonValue::String(text)),
    }
}

/// Serialize a CQL `float` as the shortest decimal that round-trips the **f32**.
///
/// Issue #3777. `Number::from_f64(f as f64)` widens the f32 to its
/// exact-but-imprecise f64 first, so the emitted decimal is the shortest one
/// round-tripping THAT f64 (`1.6699999570846558`) instead of the f32 (`1.67`).
/// The oracle is `sstabledump`, whose `float` cells carry the f32 spelling
/// (Cassandra `FloatSerializer` -> `Float.toString`); the CSV and table writers
/// already agree with it via `ValueFormatter::format_float32`.
///
/// # Why this and not `serde_json`'s `float_roundtrip`
///
/// MEASURED, not assumed (see
/// `issue_3777_json_float_spelling.rs::serde_json_value_from_f32_still_widens_so_the_fix_must_be_local`):
/// `float_roundtrip` is a DESERIALIZATION feature — it appears only in
/// serde_json's `src/de.rs` and `src/value/de.rs`, never in `ser.rs`/`number.rs` —
/// so it cannot reach this arm at all. And `serde_json::Number` stores an `f64`
/// unconditionally (`Number::from_f32` is itself `N::Float(f as f64)`), so no
/// `Number`/`Value` constructor can carry f32 precision. Only the streaming
/// `Serializer::serialize_f32` path preserves it, and a [`JsonCell::Plain`]
/// carries a `JsonValue`. So the conversion is done here, locally, with no new
/// dependency and no feature flag whose absence would silently change release
/// output.
///
/// # How the shortest f32 form is obtained, and why not `f32::to_string`
///
/// Via serde_json's OWN f32 serializer (`serde_json::to_string(&f32)`, which is the
/// only path in the crate that formats an f32 as an f32), then re-parsed as `f64`
/// for the `Number`. Rust's `Display` was tried first and is WRONG against the
/// oracle on an exact tie: for `36.6015625f32` (exactly representable, and a real
/// `test_timeseries.sensor_data` `temperature`) four 8-digit decimals round-trip
/// and two are equidistant, so the tie-break decides. Measured —
///
/// ```text
/// f32 36.6015625:  Display -> 36.601563     serde_json -> 36.601562
/// sstabledump golden (Cassandra Float.toString): 36.601562
/// ```
///
/// — serde_json rounds the tie to an EVEN last digit, which is what `Float.toString`
/// specifies and what the committed dump carries, while `Display` rounds away from
/// zero. This is the same "Rust float formatting is not Java's" family as
/// `total_cmp` vs `Float.compare` (CLAUDE.md self-check list), and the AD2 lane's
/// `test_timeseries.sensor_data` case is what caught it.
///
/// Re-parsing that text as `f64` is lossless: the text carries at most 9
/// significant digits, f64 recovers any decimal of up to 15, so the nearest f64 to
/// it is the only f64 whose own shortest form is that same text. Verified over a
/// spread of values by
/// `issue_3777_json_float_spelling.rs::float32_json_round_trips_through_f32_for_a_spread_of_values`.
///
/// The cost is one short `String` per `float` cell — the same order as this
/// module's blob, timestamp and decimal arms, all of which render through a
/// `String` already — in exchange for not adding a formatting dependency and not
/// depending on a cargo feature whose absence would silently change release output.
fn float32_cell(f: f32) -> JsonCell {
    // Non-finite floats stay JSON `null`: JSON has no literal for NaN or
    // +/-Infinity, matching `cassandra-5.0.8:.../marshal/FloatType.java:115-124`
    // exactly as the `Value::Float` arm below does. That is also a DECLARED
    // cross-binding divergence (CLAUDE.md `bindings/parity` gap 4, AD2's
    // `Divergence::NonFiniteFloatRendersAsJsonNull`), deliberately NOT changed
    // here — pinned by
    // `issue_3777_json_float_spelling.rs::nonfinite_float_renders_as_json_null_unchanged`.
    if !f.is_finite() {
        return JsonCell::Plain(JsonValue::Null);
    }
    // serde_json's f32 serializer, NOT `f32::to_string` — see the tie-break
    // measurement above. `serialize_f32` cannot fail for a finite f32 (it writes
    // into a `Vec<u8>`), but the error is mapped rather than unwrapped: no
    // `unwrap()`/`expect()` in this crate.
    let Ok(shortest) = serde_json::to_string(&f) else {
        return JsonCell::Plain(JsonValue::Null);
    };
    match shortest.parse::<f64>() {
        Ok(widened) => JsonCell::Plain(
            serde_json::Number::from_f64(widened)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
        ),
        Err(_) => JsonCell::Plain(JsonValue::Null),
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
            // The shortest decimal that round-trips the **f32**, NOT the widened
            // f64's (issue #3777) — see [`float32_cell`]. Non-finite handling is
            // the same as the `Value::Float` arm above and lives in that helper.
            Value::Float32(f) => float32_cell(*f),
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
                            // Two FIXED keys, in this order — a map entry is
                            // not user-keyed, so nothing here can collide.
                            let mut entry = IndexMap::with_capacity(2);
                            entry.insert("key".to_string(), Self::from_value(k));
                            entry.insert("value".to_string(), Self::from_value(v));
                            JsonCell::Object(entry)
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
                // FIRST position, LAST value — and NOT enforced here: that is
                // `IndexMap::insert`'s own contract, and it is the collapse the
                // `serde_json::Map` this rendering displaced performed (serde_json
                // is built with `preserve_order`, so that type IS an `IndexMap`).
                // The same rule `dedup_keys_last_wins` applies to row keys
                // (`json.rs`).
                //
                // A duplicate FIELD name is not legal CQL (Cassandra rejects the
                // `CREATE TYPE`) but CQLite's own `CREATE TYPE` parser does not
                // check, and `UdtValue` is public, so it IS constructible — and
                // two JSON keys of one name is an ambiguous document that parsers
                // resolve differently.
                let mut fields = IndexMap::with_capacity(udt.fields.len());
                udt_render_fields(
                    udt,
                    Self::from_value,
                    || JsonCell::Plain(JsonValue::Null),
                    |name, rendered| {
                        fields.insert(name.to_string(), rendered);
                    },
                );
                JsonCell::Object(fields)
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

// No unit-test module here, deliberately. `cqlite-cli`'s lib/bin unit tests
// execute in NO gate component and NO CI job
// (`scripts/tests/workspace-test-disposition.txt` records the crate as
// `PARTIAL / contradicts-doctrine`: the gate's `cli-tests` passes no `--lib`, and
// `.github/workflows/ci.yml` runs only `--test unit_tests`), so a case placed
// here would be maintained and never run. Every case covering this module lives
// in `cqlite-cli/tests/issue_3644_json_decimal_unquoted.rs`, an integration
// target the gate derives from the `cqlite-cli/tests/*.rs` glob, and drives the
// two PUBLIC writers rather than this crate-private type.
