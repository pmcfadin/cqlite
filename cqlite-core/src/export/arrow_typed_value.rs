//! The recursive CQL value → Arrow `ArrayRef` builder (feature = "arrow").
//!
//! Split out of `arrow_convert` (epic #1116 file-size split, issue #3096 Phase 0a)
//! with no behaviour change. [`build_typed_value_array`] is the single recursive
//! entry point shared by top-level column dispatch
//! ([`convert_column_to_array`](super::arrow_convert::convert_column_to_array))
//! and by nested element building (list-of-list, map values, tuple/UDT children).
//!
//! The scalar arms are inline here; the `list`/`set`, `map`, `tuple`, UDT and
//! `custom` arms delegate to [`super::arrow_builders_nested`], which recurses
//! back into this dispatcher for its element types.

use super::arrow_builders_nested::{
    build_typed_custom_array, build_typed_list_or_set_array, build_typed_map_array,
    build_typed_tuple_array, build_typed_udt_array,
};
use super::arrow_convert::ArrowConvertError;
use super::arrow_convert_util::{
    bigint_to_i128, checked_binary_offsets, checked_string_offsets, unwrap_frozen_type,
    unwrap_frozen_value,
};
use super::arrow_decimal::rescale_decimal;
use super::arrow_schema::{DECIMAL_FIXED_SCALE, DECIMAL_MAX_PRECISION};
use crate::schema::CqlType;
use crate::types::Value;
use crate::util::value_fmt::ValueFormatter;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, Time64NanosecondArray,
    TimestampMillisecondArray,
};
use std::sync::Arc;

// =========================================================================
// Recursive value → ArrayRef builder
// =========================================================================

/// Recursively build an Arrow `ArrayRef` from a slice of optional `Value`
/// references, guided by a `CqlType` for element dispatch.
///
/// This is the shared recursive entry point used by both top-level column
/// dispatch and nested element building (list-of-list, list-of-set, etc.).
///
/// `CqlType::Frozen(inner)` is transparent: `Value::Frozen(inner)` runtime
/// values are also unwrapped before dispatch.
pub(crate) fn build_typed_value_array(
    cql_type: &CqlType,
    values: &[Option<&Value>],
) -> Result<ArrayRef, ArrowConvertError> {
    // Unwrap Frozen at the type level — transparent for both schema and values.
    let effective_type = unwrap_frozen_type(cql_type);

    match effective_type {
        // ----------------------------------------------------------------
        // Scalar types
        // ----------------------------------------------------------------
        CqlType::Boolean => {
            let arr: Vec<Option<bool>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Boolean(b) => Ok(Some(*b)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Boolean value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<bool>>, ArrowConvertError>>()?;
            Ok(Arc::new(BooleanArray::from(arr)))
        }
        CqlType::TinyInt => {
            let arr: Vec<Option<i8>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::TinyInt(i) => Ok(Some(*i)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected TinyInt value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<i8>>, ArrowConvertError>>()?;
            Ok(Arc::new(Int8Array::from(arr)))
        }
        CqlType::SmallInt => {
            let arr: Vec<Option<i16>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::SmallInt(i) => Ok(Some(*i)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected SmallInt value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<i16>>, ArrowConvertError>>()?;
            Ok(Arc::new(Int16Array::from(arr)))
        }
        CqlType::Int => {
            let arr: Vec<Option<i32>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Integer(i) => Ok(Some(*i)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Int value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<i32>>, ArrowConvertError>>()?;
            Ok(Arc::new(Int32Array::from(arr)))
        }
        CqlType::BigInt => {
            let arr: Vec<Option<i64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::BigInt(i) => Ok(Some(*i)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected BigInt value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
            Ok(Arc::new(Int64Array::from(arr)))
        }
        CqlType::Counter => {
            let arr: Vec<Option<i64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Counter(c) => Ok(Some(*c)),
                        Value::BigInt(i) => Ok(Some(*i)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Counter value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
            Ok(Arc::new(Int64Array::from(arr)))
        }
        CqlType::Float => {
            let arr: Vec<Option<f32>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Float32(f) => Ok(Some(*f)),
                        // A CQL `float` (32-bit) may be carried as the wider
                        // `Value::Float` (f64) by the decode path; narrow it
                        // back (lossless for genuine f32 values), mirroring the
                        // Double arm which accepts both float variants.
                        Value::Float(f) => Ok(Some(*f as f32)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Float value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<f32>>, ArrowConvertError>>()?;
            Ok(Arc::new(Float32Array::from(arr)))
        }
        CqlType::Double => {
            let arr: Vec<Option<f64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Float(f) => Ok(Some(*f)),
                        Value::Float32(f) => Ok(Some(*f as f64)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Double value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<f64>>, ArrowConvertError>>()?;
            Ok(Arc::new(Float64Array::from(arr)))
        }
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => {
            // Guard on BORROWED &str slices of the already-materialized row
            // values — the i32-offset check runs before StringArray::from
            // copies them, so the fail-closed path never clones ~2 GiB just to
            // reject it (issue #2235).
            let refs: Vec<Option<&str>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Text(s) => std::str::from_utf8(s).map(Some).map_err(|e| {
                            ArrowConvertError::InvalidValue(format!("invalid UTF-8 in text: {e}"))
                        }),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Text value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<&str>>, ArrowConvertError>>()?;
            checked_string_offsets(&refs)?;
            Ok(Arc::new(StringArray::from(refs)))
        }
        CqlType::Blob => {
            // Guard on BORROWED &[u8] slices before BinaryArray::from copies
            // them (issue #2235) — no owned Vec<u8> clone precedes the check.
            let refs: Vec<Option<&[u8]>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Blob(b) => Ok(Some(b.as_ref())),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Blob value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<&[u8]>>, ArrowConvertError>>()?;
            checked_binary_offsets(&refs)?;
            Ok(Arc::new(BinaryArray::from(refs)))
        }
        CqlType::Timestamp => {
            let arr: Vec<Option<i64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Timestamp(ts) => Ok(Some(*ts)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Timestamp value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
            Ok(Arc::new(
                TimestampMillisecondArray::from(arr).with_timezone("UTC"),
            ))
        }
        CqlType::Date => {
            let arr: Vec<Option<i32>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Date(d) => Ok(Some(*d)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Date value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<i32>>, ArrowConvertError>>()?;
            Ok(Arc::new(Date32Array::from(arr)))
        }
        CqlType::Time => {
            let arr: Vec<Option<i64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Time(t) => Ok(Some(*t)),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Time value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
            Ok(Arc::new(Time64NanosecondArray::from(arr)))
        }
        CqlType::Decimal => {
            let mut builder = arrow::array::Decimal128Builder::with_capacity(values.len())
                .with_precision_and_scale(DECIMAL_MAX_PRECISION, DECIMAL_FIXED_SCALE as i8)?;
            for opt in values {
                let v = unwrap_frozen_value(*opt);
                match v {
                    Some(Value::Decimal { scale, unscaled }) => {
                        let rescaled = rescale_decimal(*scale, unscaled)?;
                        builder.append_value(rescaled);
                    }
                    Some(Value::Null) | None => builder.append_null(),
                    Some(other) => {
                        return Err(ArrowConvertError::InvalidValue(format!(
                            "expected Decimal value in element, got {:?}",
                            other
                        )));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        CqlType::Varint => {
            use num_bigint::BigInt;
            let mut builder = arrow::array::Decimal128Builder::with_capacity(values.len())
                .with_precision_and_scale(DECIMAL_MAX_PRECISION, 0)?;
            for opt in values {
                let v = unwrap_frozen_value(*opt);
                match v {
                    Some(Value::Varint(bytes)) => {
                        if bytes.is_empty() {
                            builder.append_value(0);
                        } else {
                            let bigint = BigInt::from_signed_bytes_be(bytes);
                            let max_abs = BigInt::from(10i64).pow(38u32) - BigInt::from(1i64);
                            let abs_val = if bigint.sign() == num_bigint::Sign::Minus {
                                -bigint.clone()
                            } else {
                                bigint.clone()
                            };
                            if abs_val > max_abs {
                                return Err(ArrowConvertError::InvalidValue(
                                    "varint element exceeds Decimal128(38, 0) range".to_string(),
                                ));
                            }
                            let i128_val = bigint_to_i128(&bigint)?;
                            builder.append_value(i128_val);
                        }
                    }
                    Some(Value::Null) | None => builder.append_null(),
                    Some(other) => {
                        return Err(ArrowConvertError::InvalidValue(format!(
                            "expected Varint value in element, got {:?}",
                            other
                        )));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        CqlType::Duration => {
            // Serialise as Utf8 text (parquet crate v53 MonthDayNano NYI).
            let arr: Vec<Option<String>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Duration { .. } => Ok(Some(ValueFormatter::format_value(v))),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Duration value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
            checked_string_offsets(&arr)?;
            Ok(Arc::new(StringArray::from(arr)))
        }
        CqlType::Uuid | CqlType::TimeUuid => {
            let mut builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
            for opt in values {
                let v = unwrap_frozen_value(*opt);
                match v {
                    Some(Value::Uuid(bytes)) => builder.append_value(bytes)?,
                    Some(Value::Null) | None => builder.append_null(),
                    Some(other) => {
                        return Err(ArrowConvertError::InvalidValue(format!(
                            "expected Uuid value in element, got {:?}",
                            other
                        )));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        CqlType::Inet => {
            let arr: Vec<Option<String>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        // `v` is already `&Value::Inet(_)`; format it in place —
                        // no per-cell clone of the address bytes (issue #1496).
                        inet @ Value::Inet(_) => Ok(Some(ValueFormatter::format_value(inet))),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Inet value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
            checked_string_offsets(&arr)?;
            Ok(Arc::new(StringArray::from(arr)))
        }
        // ----------------------------------------------------------------
        // List and Set (recursive): element type dispatches back here.
        // Arrow has no dedicated Set type; Set maps to List.
        // See `arrow_builders_nested::build_typed_list_or_set_array`.
        // ----------------------------------------------------------------
        // #4114: a vector's value IS `Value::List`, so the list builder is exact.
        CqlType::List(inner) | CqlType::Set(inner) | CqlType::Vector(inner, _) => {
            build_typed_list_or_set_array(inner, values)
        }
        // Frozen is unwrapped above in `unwrap_frozen_type`; this arm is
        // unreachable but required for exhaustiveness.
        CqlType::Frozen(inner) => build_typed_value_array(inner, values),
        // Map with recursively typed keys and values —
        // see `arrow_builders_nested::build_typed_map_array`.
        CqlType::Map(key_type, val_type) => build_typed_map_array(key_type, val_type, values),
        // Tuple<A, B, …> → Arrow Struct with positional field names —
        // see `arrow_builders_nested::build_typed_tuple_array`.
        CqlType::Tuple(element_types) => build_typed_tuple_array(element_types, values),
        // UDT → Arrow Struct with the UDT's schema field names —
        // see `arrow_builders_nested::build_typed_udt_array`.
        CqlType::Udt(_udt_name, udt_fields) => build_typed_udt_array(udt_fields, values),
        // Custom / unknown types: the Utf8 textual fallback.
        CqlType::Custom(_) => build_typed_custom_array(values),
    }
}
