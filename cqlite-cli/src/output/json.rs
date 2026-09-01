//! JSON output writer for QueryResult
//!
//! Emits deterministic JSON with keys in column order (as defined in metadata.columns).
//! This ensures that JSON object keys appear in the same order as columns, NOT in
//! arbitrary HashMap iteration order.

use crate::config::OutputConfig;
use crate::output::{OutputError, StreamingWriter};
use cqlite_core::query::{QueryMetadata, QueryResult, QueryRow};
use cqlite_core::util::udt_json::udt_to_json_object;
use cqlite_core::Value;
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::{json, Value as JsonValue};
use std::error::Error as StdError;
use std::io::Write;

use super::value_fmt::ValueFormatter;

/// A single result row serialized as a JSON object with keys in `metadata.columns`
/// order, borrowing each column name (`&str`) as the object key instead of cloning
/// a `String` per row (issue #1499).
///
/// Because it drives serde_json's own serializer, the produced bytes are identical
/// to building a `serde_json::Map` and calling `to_string`/`to_string_pretty`.
struct RowObj<'a> {
    row: &'a QueryRow,
    keys: &'a [&'a str],
}

/// De-duplicate output column keys keeping the FIRST position and the LAST value,
/// matching the historical `serde_json::Map::insert` (with `preserve_order`)
/// collapse of duplicate keys. A query with duplicate output column names (e.g.
/// `SELECT a, a` or duplicate aliases) must render a SINGLE JSON key, not two.
///
/// Returns `None` when there are no duplicates so the caller keeps using the
/// borrowed key slice allocation-free (the common, unique-key case). The
/// duplicate check itself does not allocate.
///
/// Because the row's values live in a `HashMap` keyed by column name, every
/// occurrence of a duplicate key resolves to the same (last-written) value, so
/// keeping the first position with a single entry is byte-identical to the old
/// `Map::insert` last-wins behaviour.
fn dedup_keys_last_wins<'a>(keys: &[&'a str]) -> Option<Vec<&'a str>> {
    let has_dup = keys.iter().enumerate().any(|(i, k)| keys[..i].contains(k));
    if !has_dup {
        return None;
    }
    let mut unique: Vec<&str> = Vec::with_capacity(keys.len());
    for &k in keys {
        if !unique.contains(&k) {
            unique.push(k);
        }
    }
    Some(unique)
}

impl Serialize for RowObj<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.keys.len()))?;
        for &key in self.keys {
            // Missing column → JSON null, matching the historical Map behaviour.
            let json_value = match self.row.values.get(key) {
                Some(value) => JSONWriter::value_to_json(value),
                None => JsonValue::Null,
            };
            map.serialize_entry(key, &json_value)?;
        }
        map.end()
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
/// `json_tests.rs::serde_json_value_from_f32_still_widens_so_the_fix_must_be_local`):
/// `float_roundtrip` is a DESERIALIZATION feature — it appears only in
/// serde_json's `src/de.rs` and `src/value/de.rs`, never in `ser.rs`/`number.rs` —
/// so it cannot reach this arm at all. And `serde_json::Number` stores an `f64`
/// unconditionally (`Number::from_f32` is itself `N::Float(f as f64)`), so no
/// `Number`/`Value` constructor can carry f32 precision. Only the streaming
/// `Serializer::serialize_f32` path preserves it, and this writer builds a
/// `JsonValue`. So the conversion is done here, locally, with no new dependency
/// and no feature flag whose absence would silently change release output.
///
/// Rust's `f32` `Display` emits the shortest round-tripping decimal for the f32
/// (at most 9 significant digits, never in exponent form). Re-parsing that text
/// as `f64` is lossless — f64 recovers any decimal of up to 15 significant
/// digits, so the nearest f64 to that text is the only f64 whose own shortest
/// form is that same text — and the `Number` therefore serializes the f32
/// spelling. Verified over a spread of values by
/// `json_tests.rs::float32_json_round_trips_through_f32_for_a_spread_of_values`.
fn float32_to_json(f: f32) -> JsonValue {
    // Non-finite floats stay JSON `null`: JSON has no literal for NaN or
    // +/-Infinity. That is a DECLARED divergence (CLAUDE.md `bindings/parity`
    // gap 4, AD2's `Divergence::NonFiniteFloatRendersAsJsonNull`), deliberately
    // NOT changed here — pinned by
    // `json_tests.rs::nonfinite_float_renders_as_json_null_unchanged`.
    if !f.is_finite() {
        return JsonValue::Null;
    }
    let shortest = f.to_string();
    match shortest.parse::<f64>() {
        Ok(widened) => serde_json::Number::from_f64(widened)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Err(_) => JsonValue::Null,
    }
}

/// JSON writer for QueryResult
#[allow(dead_code)]
pub struct JSONWriter;

impl JSONWriter {
    /// Write QueryResult to JSON string with deterministic key ordering
    ///
    /// # Key Ordering Guarantee
    ///
    /// JSON object keys will appear in the SAME order as columns in `metadata.columns`.
    /// This is critical for testing and ensures deterministic output regardless of
    /// HashMap iteration order.
    ///
    /// # Example
    ///
    /// If metadata.columns = [c, b, a], the JSON will be:
    /// ```json
    /// [
    ///   {"c": 1, "b": 2, "a": 3}
    /// ]
    /// ```
    /// NOT {"a": 3, "b": 2, "c": 1}
    ///
    /// # Arguments
    ///
    /// * `result` - The query result to convert to JSON
    /// * `config` - Output configuration for row limits
    ///
    /// # Returns
    ///
    /// Pretty-printed JSON string or error
    #[allow(dead_code)]
    pub fn write(result: &QueryResult, config: &OutputConfig) -> Result<String, Box<dyn StdError>> {
        // Apply row limit if specified in config
        let rows_to_display = if let Some(limit) = config.limit {
            &result.rows[..result.rows.len().min(limit)]
        } else {
            &result.rows
        };

        // Column names are borrowed once (not cloned per row) as object keys.
        let keys: Vec<&str> = result
            .metadata
            .columns
            .iter()
            .map(|col| col.name.as_str())
            .collect();
        // Collapse duplicate output column names to a single (last-wins) key,
        // matching the historical `Map::insert` semantics (issue #1499). Only
        // allocated when a duplicate is actually present.
        let deduped = dedup_keys_last_wins(&keys);
        let keys: &[&str] = deduped.as_deref().unwrap_or(&keys);

        // Serialize directly through serde_json's pretty serializer so the bytes
        // are identical to the previous `to_string_pretty(Vec<Object>)` path while
        // avoiding a per-row key clone (issue #1499).
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut serializer = serde_json::Serializer::pretty(&mut buf);
            let mut seq = serializer.serialize_seq(Some(rows_to_display.len()))?;
            for row in rows_to_display {
                seq.serialize_element(&RowObj { row, keys })?;
            }
            SerializeSeq::end(seq)?;
        }
        String::from_utf8(buf).map_err(|e| e.into())
    }

    /// Convert a CQLite Value to a serde_json::Value
    ///
    /// Uses string representations for complex types to ensure human readability.
    #[allow(dead_code)]
    fn value_to_json(value: &Value) -> JsonValue {
        match value {
            Value::Null => JsonValue::Null,
            Value::Boolean(b) => JsonValue::Bool(*b),
            Value::Integer(i) => JsonValue::Number((*i).into()),
            Value::BigInt(i) => JsonValue::Number((*i).into()),
            Value::Counter(c) => JsonValue::Number((*c).into()),
            Value::TinyInt(i) => JsonValue::Number((*i as i64).into()),
            Value::SmallInt(i) => JsonValue::Number((*i as i64).into()),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Value::Float32(f) => float32_to_json(*f),
            Value::Text(s) => JsonValue::String(String::from_utf8_lossy(s).into_owned()),
            // Use ValueFormatter for human-readable Blob formatting (0x... hex)
            Value::Blob(_) => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Timestamp (YYYY-MM-DD HH:MM:SS.fff+0000)
            Value::Timestamp(_) => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Date (YYYY-MM-DD)
            Value::Date(_) => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Time (HH:MM:SS.nnnnnnnnn)
            Value::Time(_) => JsonValue::String(ValueFormatter::format_value(value)),
            Value::Uuid(uuid) => {
                // Format UUID via the shared hex lookup-table encoder (issue #1499)
                // instead of a 16-arg `format!` per cell.
                let mut uuid_str = String::with_capacity(36);
                ValueFormatter::format_uuid_into(uuid, &mut uuid_str);
                JsonValue::String(uuid_str)
            }
            // Use ValueFormatter for human-readable Varint (decimal string)
            Value::Varint(_) => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Decimal (e.g., "69799.73")
            Value::Decimal { .. } => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Duration (XmoYdZns format)
            Value::Duration { .. } => JsonValue::String(ValueFormatter::format_value(value)),
            Value::Json(j) => (**j).clone(),
            Value::List(list) => {
                let json_list: Vec<JsonValue> = list.iter().map(Self::value_to_json).collect();
                JsonValue::Array(json_list)
            }
            Value::Set(set) => {
                let json_list: Vec<JsonValue> = set.iter().map(Self::value_to_json).collect();
                JsonValue::Array(json_list)
            }
            Value::Map(map) => {
                // Maps are Vec<(Value, Value)> in CQLite
                // Represent as array of {"key": k, "value": v} objects for clarity
                let entries: Vec<JsonValue> = map
                    .iter()
                    .map(|(k, v)| {
                        json!({
                            "key": Self::value_to_json(k),
                            "value": Self::value_to_json(v)
                        })
                    })
                    .collect();
                JsonValue::Array(entries)
            }
            Value::Tuple(tuple) => {
                let json_list: Vec<JsonValue> = tuple.iter().map(Self::value_to_json).collect();
                JsonValue::Array(json_list)
            }
            // Declared fields and NOTHING else — no injected `_type` (issue
            // #3629): type identity must not share the user's field namespace.
            // One shared rule, each writer keeping its own field-value renderer.
            Value::Udt(udt) => udt_to_json_object(udt, Self::value_to_json),
            Value::Frozen(boxed_value) => Self::value_to_json(boxed_value),
            // Tombstoned cells represent deleted values. Emit JSON null to match
            // cqlsh and Python binding behaviour (issue #806).
            Value::Tombstone(_) => JsonValue::Null,
            Value::Inet(bytes) => {
                // Format as IP address string if possible
                if bytes.len() == 4 {
                    JsonValue::String(format!(
                        "{}.{}.{}.{}",
                        bytes[0], bytes[1], bytes[2], bytes[3]
                    ))
                } else if bytes.len() == 16 {
                    // IPv6 - use std::net::Ipv6Addr for canonical formatting
                    use std::net::Ipv6Addr;
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(bytes);
                    let addr = Ipv6Addr::from(octets);
                    JsonValue::String(addr.to_string())
                } else {
                    // Invalid length, encode as base64
                    use base64::Engine;
                    let engine = base64::engine::general_purpose::STANDARD;
                    JsonValue::String(engine.encode(bytes))
                }
            }
        }
    }
}

// ============================================================================
// Streaming JSON Writer (Issue #280)
// ============================================================================

/// Streaming JSON writer for memory-efficient export of large datasets
///
/// Outputs a JSON array with one object per row. Unlike the batch `JSONWriter`,
/// this writer processes data incrementally, allowing export of arbitrarily
/// large result sets within memory constraints.
///
/// # Output Format
///
/// ```json
/// [
///   {"col1": "value1", "col2": 123},
///   {"col1": "value2", "col2": 456}
/// ]
/// ```
///
/// # Example
///
/// ```ignore
/// let file = File::create("output.json")?;
/// let mut writer = StreamingJSONWriter::new(file);
///
/// writer.write_header(&metadata)?;
///
/// for chunk in result_iterator.chunks(10_000) {
///     writer.write_chunk(&chunk)?;
/// }
///
/// writer.finalize()?;
/// ```
pub struct StreamingJSONWriter<W: Write> {
    /// Inner writer
    writer: W,
    /// Column names in order
    columns: Vec<String>,
    /// Count of rows written
    rows_written: u64,
    /// Whether we've written any rows (for comma handling)
    first_row: bool,
    /// Whether to pretty-print
    pretty: bool,
}

impl<W: Write> StreamingJSONWriter<W> {
    /// Create a new streaming JSON writer with pretty-printing
    pub fn new(output: W) -> Self {
        Self {
            writer: output,
            columns: Vec::new(),
            rows_written: 0,
            first_row: true,
            pretty: true,
        }
    }

    /// Create with compact (non-pretty) output
    #[allow(dead_code)]
    pub fn compact(output: W) -> Self {
        Self {
            writer: output,
            columns: Vec::new(),
            rows_written: 0,
            first_row: true,
            pretty: false,
        }
    }

    /// Serialize a single row to a JSON object string with deterministic key
    /// ordering, borrowing column names as keys (no per-row key clone, issue
    /// #1499). Output is byte-identical to serializing a `serde_json::Map`.
    fn row_to_json_string(&self, row: &QueryRow) -> Result<String, serde_json::Error> {
        let keys: Vec<&str> = self.columns.iter().map(|c| c.as_str()).collect();
        // Collapse duplicate column names to a single (last-wins) key so the
        // streaming writer matches the batch writer and the old `Map::insert`
        // semantics (issue #1499). Allocation-free when there are no duplicates.
        let deduped = dedup_keys_last_wins(&keys);
        let keys: &[&str] = deduped.as_deref().unwrap_or(&keys);
        let obj = RowObj { row, keys };
        if self.pretty {
            serde_json::to_string_pretty(&obj)
        } else {
            serde_json::to_string(&obj)
        }
    }
}

impl<W: Write + Send> StreamingWriter for StreamingJSONWriter<W> {
    fn write_header(&mut self, metadata: &QueryMetadata) -> Result<(), OutputError> {
        // Store column names for row writing
        self.columns = metadata.columns.iter().map(|c| c.name.clone()).collect();

        // Write opening bracket
        if self.pretty {
            writeln!(self.writer, "[").map_err(OutputError::Io)?;
        } else {
            write!(self.writer, "[").map_err(OutputError::Io)?;
        }

        Ok(())
    }

    fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, OutputError> {
        for row in rows {
            let json_str = self
                .row_to_json_string(row)
                .map_err(|e| OutputError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

            // Handle comma separator between rows
            if !self.first_row {
                if self.pretty {
                    writeln!(self.writer, ",").map_err(OutputError::Io)?;
                } else {
                    write!(self.writer, ",").map_err(OutputError::Io)?;
                }
            }
            self.first_row = false;

            // Write JSON object
            if self.pretty {
                // Indent each line
                for line in json_str.lines() {
                    write!(self.writer, "  {}", line).map_err(OutputError::Io)?;
                    writeln!(self.writer).map_err(OutputError::Io)?;
                }
            } else {
                write!(self.writer, "{}", json_str).map_err(OutputError::Io)?;
            }

            self.rows_written += 1;
        }

        Ok(rows.len())
    }

    fn finalize(&mut self) -> Result<(), OutputError> {
        // Write closing bracket
        if self.pretty {
            writeln!(self.writer, "]").map_err(OutputError::Io)?;
        } else {
            write!(self.writer, "]").map_err(OutputError::Io)?;
        }

        self.writer.flush().map_err(OutputError::Io)
    }

    fn rows_written(&self) -> u64 {
        self.rows_written
    }
}

#[cfg(test)]
#[path = "json_tests.rs"]
mod json_tests;
