//! Arrow → canonical value decoding for the Parquet↔JSONL parity harness (#1490).
//!
//! # This decode is DECLARED-TYPE-GUIDED, and it is not the entry point
//!
//! Nothing here is public. Every Arrow cell is decoded through
//! `declared::canonicalize_arrow`, which requires the `CqlTypeSpec` declared for
//! that position, and this module's [`decode_declared`] is the structural half
//! of that one door. The decode is mostly structural — an `Int32Array` can only
//! be an integer — but `Decimal128` is genuinely AMBIGUOUS (scale zero is both a
//! `varint` and a whole-valued `decimal`), so it consults the declared type and
//! REFUSES when none is available. See `declared.rs` for the invariant and for
//! the three review rounds that produced it.
//!
//! Reads a Parquet file back through the `arrow`/`parquet` crates and projects
//! every cell into the SAME [`CanonicalValue`] space the sstabledump golden is
//! parsed into (`canonical_jsonl`), so the comparison is per-cell and typed
//! rather than a string diff.
//!
//! # Where each rendering comes from
//!
//! The renderings below are chosen to match what Apache Cassandra's
//! `sstabledump` writes for the same value — the ORACLE — not what CQLite
//! happens to emit:
//!
//! | Arrow type                  | canonical form                       | golden form |
//! |-----------------------------|--------------------------------------|-------------|
//! | `Boolean`                   | `Bool`                               | JSON bool |
//! | `Int8/16/32/64`             | `Int`                                | JSON number |
//! | `Float32`                   | `Float` (f32 widened)                | JSON number, narrowed to f32 by the golden side |
//! | `Float64`                   | `Float`                              | JSON number |
//! | `Decimal128(38, s)` decimal | EXACT decimal text (`decimal.rs`)    | JSON number, recovered to the same exact decimal (`declared::canonicalize_golden`) |
//! | `Decimal128(38, 0)` varint  | `Int`                                | JSON number |
//! | `Utf8`                      | `Text`                               | JSON string |
//! | `Binary`                    | `Text("0x" + lower hex)`             | `"0x…"` string |
//! | `FixedSizeBinary(16)`       | `Text` hyphenated UUID               | UUID string |
//! | `Timestamp(ms, UTC)`        | `Timestamp` (epoch µs)               | `"YYYY-MM-DD HH:MM:SS.mmmZ"` |
//! | `Date32`                    | `Text("YYYY-MM-DD")`                 | `"YYYY-MM-DD"` |
//! | `Time64(ns)`                | `Text("HH:MM:SS.nnnnnnnnn")`         | same shape |
//! | `Interval(MonthDayNano)`    | `Tuple(months, days, nanos)`         | duration text, via `spelling.rs` |
//! | `List`                      | `List`                               | assembled from per-element cells |
//! | `Map`                       | `Map`                                | assembled from `path`+`value` cells |
//! | `Struct`                    | `Tuple`                              | JSON object |
//!
//! DECODING a `Struct` is not the same as being able to COMPARE it: a declared
//! CQL `tuple` lands here as a named `Tuple` while its golden lands as a
//! positional `List`, so the harness REFUSES that column's values rather than
//! comparing two representations (`unsupported.rs`). A UDT's Struct decodes and
//! compares; only its Arrow FIELD TYPES are unmeasurable.
//!
//! An Arrow type NOT in that table is an ERROR. A permissive fallback (render it
//! with `Debug`, say) would let an unexpected mapping compare equal to something
//! and is exactly how a parity harness silently stops testing.
//!
//! # This table and `arrow_expect::ArrowShape::check` MUST stay in sync
//!
//! `check` decides which Arrow types the harness declares VALID for a declared
//! CQL type; this module decides which it can actually DECODE. An accept-list
//! broader than the decoder is a promise the harness cannot keep: the schema
//! check passes and the run then dies during value projection, which is a
//! confusing late failure instead of a clear early one. Add or remove a
//! representation in BOTH places, in one edit.

#![allow(dead_code)]

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalMonthDayNanoArray,
    LargeBinaryArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
    Time64NanosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

use super::canonical_jsonl::{CanonicalValue, NormalizedFloat};
use super::declared::{
    arrow_child, canonicalize_arrow_decimal, map_kv, seq_elem, struct_field, Declared, Position,
};

/// Project cell `(array, row)` into the canonical space, at a DECLARED position.
///
/// `pub(super)` on purpose: the only public door is
/// `declared::canonicalize_arrow`, so no call site can decode an Arrow cell
/// without naming the declared type that governs it.
pub(super) fn decode_declared(
    array: &dyn Array,
    row: usize,
    at: &Declared<'_>,
) -> Result<CanonicalValue, String> {
    let ctx = at.ctx();
    if array.is_null(row) {
        return Ok(CanonicalValue::Absent);
    }
    match array.data_type() {
        DataType::Null => Ok(CanonicalValue::Absent),
        DataType::Boolean => Ok(CanonicalValue::Bool(
            downcast::<BooleanArray>(array, ctx)?.value(row),
        )),
        DataType::Int8 => Ok(CanonicalValue::Int(
            downcast::<Int8Array>(array, ctx)?.value(row) as i128,
        )),
        DataType::Int16 => Ok(CanonicalValue::Int(
            downcast::<Int16Array>(array, ctx)?.value(row) as i128,
        )),
        DataType::Int32 => Ok(CanonicalValue::Int(
            downcast::<Int32Array>(array, ctx)?.value(row) as i128,
        )),
        DataType::Int64 => Ok(CanonicalValue::Int(
            downcast::<Int64Array>(array, ctx)?.value(row) as i128,
        )),
        // A CQL `float` is 32-bit; sstabledump prints Java's `Float.toString`,
        // which the golden side re-narrows to f32 before widening, so both sides
        // hold the SAME double. See `declared::canonicalize_golden`.
        DataType::Float32 => Ok(CanonicalValue::Float(NormalizedFloat(
            downcast::<Float32Array>(array, ctx)?.value(row) as f64,
        ))),
        DataType::Float64 => Ok(CanonicalValue::Float(NormalizedFloat(
            downcast::<Float64Array>(array, ctx)?.value(row),
        ))),
        DataType::Utf8 => Ok(CanonicalValue::Text(
            downcast::<StringArray>(array, ctx)?.value(row).to_string(),
        )),
        DataType::LargeUtf8 => Ok(CanonicalValue::Text(
            downcast::<LargeStringArray>(array, ctx)?
                .value(row)
                .to_string(),
        )),
        DataType::Binary => Ok(CanonicalValue::Text(hex_blob(
            downcast::<arrow::array::BinaryArray>(array, ctx)?.value(row),
        ))),
        DataType::LargeBinary => Ok(CanonicalValue::Text(hex_blob(
            downcast::<LargeBinaryArray>(array, ctx)?.value(row),
        ))),
        DataType::FixedSizeBinary(16) => {
            let bytes = downcast::<FixedSizeBinaryArray>(array, ctx)?.value(row);
            Ok(CanonicalValue::Text(format_uuid(bytes)?))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ms = downcast::<TimestampMillisecondArray>(array, ctx)?.value(row);
            let micros = ms
                .checked_mul(1_000)
                .ok_or_else(|| format!("{ctx}: timestamp {ms}ms overflows microseconds"))?;
            Ok(CanonicalValue::Timestamp {
                micros,
                raw: format!("{ms}ms"),
            })
        }
        DataType::Date32 => {
            let days = downcast::<Date32Array>(array, ctx)?.value(row);
            Ok(CanonicalValue::Text(format_date(days)))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let nanos = downcast::<Time64NanosecondArray>(array, ctx)?.value(row);
            Ok(CanonicalValue::Text(format_time_nanos(nanos)?))
        }
        // The faithful Arrow type for CQL `duration` (see `arrow_expect`), and
        // the ONE case where the canonical form is produced directly rather than
        // as text: the two writers spell a duration differently, so
        // `spelling::normalize_spelling` reconciles both sides onto a
        // (months, days, nanos) triple — and an Arrow interval already IS that
        // triple, so emitting it avoids inventing a text spelling only to parse
        // it back (which would also have to re-guess Cassandra's global-sign
        // convention). Emitted in EXACTLY the shape `normalize_spelling`
        // produces for the golden's duration text, which then leaves it
        // unchanged.
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = downcast::<IntervalMonthDayNanoArray>(array, ctx)?.value(row);
            Ok(CanonicalValue::Tuple(vec![
                ("months".to_string(), CanonicalValue::Int(v.months as i128)),
                ("days".to_string(), CanonicalValue::Int(v.days as i128)),
                (
                    "nanos".to_string(),
                    CanonicalValue::Int(v.nanoseconds as i128),
                ),
            ]))
        }
        // The ONE genuinely ambiguous representation: scale zero is both a
        // `varint` and a whole-valued `decimal`. Settled by the DECLARED type,
        // and refused when none is available (issue #1490 round 7).
        DataType::Decimal128(_, scale) => {
            let unscaled = downcast::<Decimal128Array>(array, ctx)?.value(row);
            canonicalize_arrow_decimal(unscaled, *scale, at)
        }
        // Every container member descends at ITS OWN declared position, built
        // from the parent's declared type — the same derivation the golden side
        // uses, so an element/key/value can never be canonicalized by one side
        // with a declared type and by the other without one.
        DataType::List(_) => {
            let list = downcast::<ListArray>(array, ctx)?;
            let values = list.value(row);
            let elem = seq_elem(at);
            let mut out = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                let child = arrow_child(at, elem, Position::Element, format!("{ctx}[{i}]"));
                out.push(decode_declared(values.as_ref(), i, &child)?);
            }
            Ok(CanonicalValue::List(out))
        }
        DataType::Map(_, _) => {
            let map = downcast::<MapArray>(array, ctx)?;
            let entries = map.value(row);
            let keys = entries.column(0);
            let vals = entries.column(1);
            let (key_spec, value_spec) = map_kv(at);
            let mut out = Vec::with_capacity(entries.len());
            for i in 0..entries.len() {
                let kc = arrow_child(at, key_spec, Position::MapKey, format!("{ctx}.key[{i}]"));
                let vc = arrow_child(
                    at,
                    value_spec,
                    Position::MapValue,
                    format!("{ctx}.value[{i}]"),
                );
                out.push((
                    decode_declared(keys.as_ref(), i, &kc)?,
                    decode_declared(vals.as_ref(), i, &vc)?,
                ));
            }
            Ok(CanonicalValue::Map(out))
        }
        DataType::Struct(fields) => {
            let s = downcast::<StructArray>(array, ctx)?;
            let mut out = Vec::with_capacity(fields.len());
            for (i, f) in fields.iter().enumerate() {
                let child = struct_field(at, i, f.name(), fields.len());
                out.push((
                    f.name().clone(),
                    decode_declared(s.column(i).as_ref(), row, &child)?,
                ));
            }
            Ok(CanonicalValue::Tuple(out))
        }
        other => Err(format!(
            "{ctx}: Arrow type {other:?} has no declared canonical rendering — \
             add it to the table in arrow_rows.rs rather than comparing loosely"
        )),
    }
}

fn downcast<'a, T: 'static>(array: &'a dyn Array, ctx: &str) -> Result<&'a T, String> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("{ctx}: array does not downcast to {}", type_name::<T>()))
}

fn type_name<T>() -> &'static str {
    std::any::type_name::<T>()
}

/// sstabledump renders a blob as `0x` + lowercase hex.
fn hex_blob(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn format_uuid(bytes: &[u8]) -> Result<String, String> {
    if bytes.len() != 16 {
        return Err(format!("uuid must be 16 bytes, got {}", bytes.len()));
    }
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

/// Days since the Unix epoch → `YYYY-MM-DD` (proleptic Gregorian).
///
/// Hinnant's `civil_from_days`, which is exact for the whole `i32` range — the
/// same algorithm `canonical_jsonl::days_since_epoch` inverts.
pub fn format_date(days: i32) -> String {
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// Nanoseconds since midnight → `HH:MM:SS.nnnnnnnnn` (sstabledump's `time`
/// rendering: always 9 fractional digits).
pub fn format_time_nanos(nanos: i64) -> Result<String, String> {
    if !(0..86_400_000_000_000).contains(&nanos) {
        return Err(format!("time {nanos}ns is outside a single day"));
    }
    let secs_total = nanos / 1_000_000_000;
    let frac = nanos % 1_000_000_000;
    Ok(format!(
        "{:02}:{:02}:{:02}.{:09}",
        secs_total / 3_600,
        (secs_total % 3_600) / 60,
        secs_total % 60,
        frac
    ))
}
