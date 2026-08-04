//! Scalar / primitive Arrow column builders (feature = "arrow").
//!
//! Split out of `arrow_convert` (epic #1116 file-size split, issue #3096 Phase 0a)
//! with no behaviour change. Each builder turns one column's pre-resolved,
//! row-aligned [`Cells`] slice into an Arrow `ArrayRef`:
//!
//! * the flat `DataType` builders (`bool`/`int`/`float`/`text`/`blob`/
//!   `timestamp`/`uuid`), used when `ColumnInfo.cql_type` is `None` or opaque;
//! * the high-fidelity `CqlType` builders (`date`, `time`, `decimal`, `varint`,
//!   `duration`, `uuid`/`timeuuid`, `inet`).
//!
//! Dispatch between them lives in
//! [`convert_column_to_array`](super::arrow_convert::convert_column_to_array);
//! the recursive element-wise builders for collections live in
//! [`super::arrow_builders_nested`].

use super::arrow_convert::ArrowConvertError;
use super::arrow_convert_util::{
    bigint_to_i128, checked_binary_offsets, checked_string_offsets, unwrap_frozen_type,
    unwrap_frozen_value, Cells,
};
use super::arrow_decimal::rescale_decimal;
use super::arrow_schema::{DECIMAL_FIXED_SCALE, DECIMAL_MAX_PRECISION};
use crate::query::ColumnInfo;
use crate::schema::CqlType;
use crate::types::Value;
use crate::util::value_fmt::ValueFormatter;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, Time64NanosecondArray,
    TimestampMillisecondArray,
};
use std::borrow::Cow;
use std::sync::Arc;

// =========================================================================
// Flat (DataType-dispatched) scalar builders
// =========================================================================

pub(super) fn build_boolean_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<bool>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::Boolean(b)) => Ok(Some(*b)),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Boolean value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<bool>>, ArrowConvertError>>()?;
    Ok(Arc::new(BooleanArray::from(values)))
}

pub(super) fn build_int8_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i8>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::TinyInt(i)) => Ok(Some(*i)),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected TinyInt value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<i8>>, ArrowConvertError>>()?;
    Ok(Arc::new(Int8Array::from(values)))
}

pub(super) fn build_int16_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i16>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::SmallInt(i)) => Ok(Some(*i)),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected SmallInt value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<i16>>, ArrowConvertError>>()?;
    Ok(Arc::new(Int16Array::from(values)))
}

pub(super) fn build_int32_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    // The same-width `Date`→i32 acceptance is only valid on the OPAQUE path
    // (`cql_type = None`): an authoritative `date` column routes to
    // `build_date32_array`, so an authoritative `int` column carrying a `Date`
    // is a genuine mismatch that must fail closed.
    let allow_compat = col.cql_type.is_none();
    let values: Vec<Option<i32>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::Integer(i)) => Ok(Some(*i)),
            Some(Value::Date(d)) if allow_compat => Ok(Some(*d)), // Date is stored as i32 days
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Int value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<i32>>, ArrowConvertError>>()?;
    Ok(Arc::new(Int32Array::from(values)))
}

pub(super) fn build_int64_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    // `build_int64_array` backs authoritative `bigint` and `counter` columns
    // plus the opaque (`cql_type = None`) path. `Counter` is legitimate for a
    // `counter` column; the same-width `Time`→i64 acceptance and cross-accepting
    // `Counter` for a `bigint` column are only valid on the opaque path (an
    // authoritative `time` column routes to `build_time64_ns_array`).
    let effective = col.cql_type.as_ref().map(unwrap_frozen_type);
    let allow_counter = matches!(effective, None | Some(CqlType::Counter));
    let allow_compat = effective.is_none();
    let values: Vec<Option<i64>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::BigInt(i)) => Ok(Some(*i)),
            Some(Value::Counter(c)) if allow_counter => Ok(Some(*c)),
            Some(Value::Time(t)) if allow_compat => Ok(Some(*t)), // Time is stored as i64 nanos
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected BigInt value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
    Ok(Arc::new(Int64Array::from(values)))
}

pub(super) fn build_float32_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<f32>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::Float32(f)) => Ok(Some(*f)),
            // A CQL `float` (32-bit) may be carried as the wider `Value::Float`
            // (f64) by the decode path; narrow it back (lossless for genuine
            // f32 values), mirroring build_float64_array which accepts both.
            Some(Value::Float(f)) => Ok(Some(*f as f32)),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Float value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<f32>>, ArrowConvertError>>()?;
    Ok(Arc::new(Float32Array::from(values)))
}

pub(super) fn build_float64_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<f64>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::Float(f)) => Ok(Some(*f)),
            Some(Value::Float32(f)) => Ok(Some(*f as f64)),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Double value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<f64>>, ArrowConvertError>>()?;
    Ok(Arc::new(Float64Array::from(values)))
}

pub(super) fn build_string_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    // When the schema carries an AUTHORITATIVE text type, fail closed on a
    // wrong-variant value (mirroring the other scalar builders) rather than
    // silently string-formatting it. For `cql_type = None` or opaque types
    // (e.g. `Custom`) this stays the permissive Utf8 fallback: those columns
    // have no authoritative type to validate against.
    let strict_text = matches!(
        col.cql_type.as_ref().map(unwrap_frozen_type),
        Some(CqlType::Text | CqlType::Ascii | CqlType::Varchar)
    );
    if strict_text {
        // Authoritative wide-text column: values are raw `Value::Text` payloads
        // (or null/error). Guard on BORROWED &str slices before StringArray::from
        // copies them, so the fail-closed path never clones ~2 GiB of raw text
        // just to reject it (issue #2235).
        let refs: Vec<Option<&str>> = cells
            .iter()
            .map(|cell| match unwrap_frozen_value(*cell) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::Text(s)) => std::str::from_utf8(s).map(Some).map_err(|e| {
                    ArrowConvertError::InvalidValue(format!("invalid UTF-8 in text: {e}"))
                }),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Text value, got {:?}",
                    col.name, other
                ))),
            })
            .collect::<Result<Vec<Option<&str>>, ArrowConvertError>>()?;
        checked_string_offsets(&refs)?;
        return Ok(Arc::new(StringArray::from(refs)));
    }
    // Opaque / untyped fallback. Raw `Value::Text` payloads can be multi-GiB, so
    // represent them as BORROWED `Cow::Borrowed(&str)` and guard on the borrowed
    // lengths before any owned copy is made (issue #2235). Only the small,
    // computed `format_value`/`Json` representations are materialized as owned
    // `Cow::Owned` strings — those are never a raw multi-GiB payload.
    let values: Vec<Option<Cow<str>>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::Null) => Ok(None),
            Some(Value::Text(s)) => std::str::from_utf8(s)
                .map(|st| Some(Cow::Borrowed(st)))
                .map_err(|e| {
                    ArrowConvertError::InvalidValue(format!("invalid UTF-8 in text: {e}"))
                }),
            Some(Value::Json(j)) => Ok(Some(Cow::Owned(j.to_string()))),
            Some(other) => Ok(Some(Cow::Owned(ValueFormatter::format_value(other)))),
        })
        .collect::<Result<Vec<Option<Cow<str>>>, ArrowConvertError>>()?;
    checked_string_offsets(&values)?;
    Ok(Arc::new(StringArray::from_iter(
        values.iter().map(|v| v.as_deref()),
    )))
}

pub(super) fn build_binary_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<&[u8]>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::Blob(b)) => Ok(Some(b.as_ref())),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Blob value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<&[u8]>>, ArrowConvertError>>()?;
    checked_binary_offsets(&values)?;
    Ok(Arc::new(BinaryArray::from(values)))
}

pub(super) fn build_timestamp_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i64>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::Timestamp(ts)) => Ok(Some(*ts)),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Timestamp value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
    Ok(Arc::new(
        TimestampMillisecondArray::from(values).with_timezone("UTC"),
    ))
}

pub(super) fn build_uuid_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    // Append UUID bytes straight into a capacity-hinted builder — no intermediate
    // `Vec<Option<[u8; 16]>>` and no reallocating growth (issue #1496).
    let mut builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(cells.len(), 16);
    for cell in cells {
        match unwrap_frozen_value(*cell) {
            None | Some(Value::Null) => builder.append_null(),
            Some(Value::Uuid(uuid)) => builder.append_value(uuid)?,
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Uuid value, got {:?}",
                    col.name, other
                )));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// High-fidelity CQL type builders
// =========================================================================

/// Build an Arrow `Date32` array from `Value::Date(i32)`.
pub(super) fn build_date32_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i32>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::Date(days)) => Ok(Some(*days)),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Date value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<i32>>, ArrowConvertError>>()?;
    Ok(Arc::new(Date32Array::from(values)))
}

/// Build an Arrow `Time64(Nanosecond)` array from `Value::Time(i64)`.
pub(super) fn build_time64_ns_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i64>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(Value::Time(nanos)) => Ok(Some(*nanos)),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Time value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
    Ok(Arc::new(Time64NanosecondArray::from(values)))
}

/// Build an Arrow `Decimal128(38, DECIMAL_FIXED_SCALE)` array from
/// `Value::Decimal { scale, unscaled }`.
pub(super) fn build_decimal128_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let mut builder = arrow::array::Decimal128Builder::with_capacity(cells.len())
        .with_precision_and_scale(DECIMAL_MAX_PRECISION, DECIMAL_FIXED_SCALE as i8)?;

    for cell in cells {
        match unwrap_frozen_value(*cell) {
            Some(Value::Decimal { scale, unscaled }) => {
                let rescaled = rescale_decimal(*scale, unscaled).map_err(|e| {
                    ArrowConvertError::InvalidValue(format!("Column '{}': {e}", col.name))
                })?;
                builder.append_value(rescaled);
            }
            Some(Value::Null) | None => {
                builder.append_null();
            }
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "Column '{}': expected Decimal value, got {:?}",
                    col.name, other
                )));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build an Arrow `Decimal128(38, 0)` array from `Value::varint(Vec<u8>)`.
pub(super) fn build_varint_as_decimal128_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    use num_bigint::BigInt;

    let mut builder = arrow::array::Decimal128Builder::with_capacity(cells.len())
        .with_precision_and_scale(DECIMAL_MAX_PRECISION, 0)?;

    for cell in cells {
        match unwrap_frozen_value(*cell) {
            Some(Value::Varint(bytes)) => {
                if bytes.is_empty() {
                    builder.append_value(0);
                } else {
                    let bigint = BigInt::from_signed_bytes_be(bytes);

                    // Check it fits in Decimal128 (precision 38).
                    let max_abs = BigInt::from(10i64).pow(38u32) - BigInt::from(1i64);
                    let abs_val = if bigint.sign() == num_bigint::Sign::Minus {
                        -bigint.clone()
                    } else {
                        bigint.clone()
                    };
                    if abs_val > max_abs {
                        return Err(ArrowConvertError::InvalidValue(format!(
                            "Column '{}': varint value exceeds Decimal128(38, 0) range",
                            col.name
                        )));
                    }

                    let i128_val = bigint_to_i128(&bigint).map_err(|e| {
                        ArrowConvertError::InvalidValue(format!("Column '{}': {e}", col.name))
                    })?;
                    builder.append_value(i128_val);
                }
            }
            Some(Value::Null) | None => {
                builder.append_null();
            }
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "Column '{}': expected Varint value, got {:?}",
                    col.name, other
                )));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build an Arrow `Utf8` array from `Value::Duration { months, days, nanos }`.
pub(super) fn build_duration_utf8_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<String>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            Some(v @ Value::Duration { .. }) => Ok(Some(ValueFormatter::format_value(v))),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Duration value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
    checked_string_offsets(&values)?;
    Ok(Arc::new(StringArray::from(values)))
}

/// Build an Arrow `FixedSizeBinary(16)` array from `Value::Uuid([u8; 16])`.
pub(super) fn build_uuid_fixed_binary_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let mut builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(cells.len(), 16);
    for cell in cells {
        match unwrap_frozen_value(*cell) {
            Some(Value::Uuid(bytes)) => builder.append_value(bytes)?,
            Some(Value::Null) | None => builder.append_null(),
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "Column '{}': expected Uuid value, got {:?}",
                    col.name, other
                )));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build an Arrow `Utf8` array from `Value::inet(Vec<u8>)`.
pub(super) fn build_inet_utf8_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<String>> = cells
        .iter()
        .map(|cell| match unwrap_frozen_value(*cell) {
            None => Ok(None),
            // Format the borrowed `&Value::Inet` in place — no per-cell clone of
            // the address bytes (issue #1496).
            Some(inet @ Value::Inet(_)) => Ok(Some(ValueFormatter::format_value(inet))),
            Some(Value::Null) => Ok(None),
            Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                "column '{}': expected Inet value, got {:?}",
                col.name, other
            ))),
        })
        .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
    checked_string_offsets(&values)?;
    Ok(Arc::new(StringArray::from(values)))
}
