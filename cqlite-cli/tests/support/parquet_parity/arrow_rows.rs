//! Arrow → canonical value decoding for the Parquet↔JSONL parity harness (#1490).
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
//! | `Decimal128(38, s>0)`       | `Float` (unscaled / 10^s)            | JSON number, incl. an integer-shaped whole decimal (`golden_rows::normalize_declared_numbers`) |
//! | `Decimal128(38, 0)`         | `Int` (varint)                       | JSON number |
//! | `Utf8`                      | `Text`                               | JSON string |
//! | `Binary`                    | `Text("0x" + lower hex)`             | `"0x…"` string |
//! | `FixedSizeBinary(16)`       | `Text` hyphenated UUID               | UUID string |
//! | `Timestamp(ms, UTC)`        | `Timestamp` (epoch µs)               | `"YYYY-MM-DD HH:MM:SS.mmmZ"` |
//! | `Date32`                    | `Text("YYYY-MM-DD")`                 | `"YYYY-MM-DD"` |
//! | `Time64(ns)`                | `Text("HH:MM:SS.nnnnnnnnn")`         | same shape |
//! | `List`                      | `List`                               | assembled from per-element cells |
//! | `Map`                       | `Map`                                | assembled from `path`+`value` cells |
//! | `Struct`                    | `Tuple`                              | JSON object |
//!
//! An Arrow type NOT in that table is an ERROR. A permissive fallback (render it
//! with `Debug`, say) would let an unexpected mapping compare equal to something
//! and is exactly how a parity harness silently stops testing.

#![allow(dead_code)]

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, ListArray, MapArray, StringArray, StructArray, Time64NanosecondArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use super::canonical_jsonl::{CanonicalValue, NormalizedFloat};

/// The largest `Decimal128` unscaled magnitude the harness will convert to `f64`
/// exactly: `2^53`. Beyond it the conversion is lossy, so a mismatch could be an
/// artifact of the comparison rather than of the export — the harness ERRORS
/// instead of comparing (the golden side would need arbitrary-precision decimal
/// parsing, which sstabledump's JSON number rendering does not preserve anyway).
const MAX_EXACT_F64_INT: i128 = 1i128 << 53;

/// Project cell `(array, row)` into the canonical space.
pub fn canonical_from_arrow(
    array: &dyn Array,
    row: usize,
    ctx: &str,
) -> Result<CanonicalValue, String> {
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
        // hold the SAME double. See `golden_rows::normalize_declared_numbers`.
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
        DataType::Decimal128(_, scale) => {
            let unscaled = downcast::<Decimal128Array>(array, ctx)?.value(row);
            decimal_to_canonical(unscaled, *scale, ctx)
        }
        DataType::List(_) => {
            let list = downcast::<ListArray>(array, ctx)?;
            let values = list.value(row);
            let mut out = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                out.push(canonical_from_arrow(
                    values.as_ref(),
                    i,
                    &format!("{ctx}[{i}]"),
                )?);
            }
            Ok(CanonicalValue::List(out))
        }
        DataType::Map(_, _) => {
            let map = downcast::<MapArray>(array, ctx)?;
            let entries = map.value(row);
            let keys = entries.column(0);
            let vals = entries.column(1);
            let mut out = Vec::with_capacity(entries.len());
            for i in 0..entries.len() {
                out.push((
                    canonical_from_arrow(keys.as_ref(), i, &format!("{ctx}.key[{i}]"))?,
                    canonical_from_arrow(vals.as_ref(), i, &format!("{ctx}.value[{i}]"))?,
                ));
            }
            Ok(CanonicalValue::Map(out))
        }
        DataType::Struct(fields) => {
            let s = downcast::<StructArray>(array, ctx)?;
            let mut out = Vec::with_capacity(fields.len());
            for (i, f) in fields.iter().enumerate() {
                out.push((
                    f.name().clone(),
                    canonical_from_arrow(
                        s.column(i).as_ref(),
                        row,
                        &format!("{ctx}.{}", f.name()),
                    )?,
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

/// `decimal` → `Float`; `varint` (scale 0) → `Int`.
///
/// The division is exact-by-construction for every magnitude the harness
/// accepts: `unscaled` and `10^scale` are both exactly representable, so IEEE
/// division returns the correctly-rounded true quotient — bit-identical to the
/// double `serde_json` produces from the golden's decimal literal.
pub fn decimal_to_canonical(
    unscaled: i128,
    scale: i8,
    ctx: &str,
) -> Result<CanonicalValue, String> {
    if scale == 0 {
        return Ok(CanonicalValue::Int(unscaled));
    }
    if scale < 0 {
        return Err(format!("{ctx}: negative Decimal128 scale {scale}"));
    }
    if unscaled.saturating_abs() >= MAX_EXACT_F64_INT {
        return Err(format!(
            "{ctx}: decimal unscaled value {unscaled} exceeds the exactly-f64-representable \
             range (2^53); the harness refuses a lossy comparison"
        ));
    }
    let divisor = 10f64.powi(scale as i32);
    Ok(CanonicalValue::Float(NormalizedFloat(
        unscaled as f64 / divisor,
    )))
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
