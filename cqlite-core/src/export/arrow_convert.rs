//! CQL → Arrow RecordBatch conversion (feature = "arrow")
//!
//! This module contains the pure CQL-to-Arrow type mapping and array building
//! logic that was previously embedded in the `parquet` module.  Exposing it
//! behind a standalone `arrow` cargo feature allows consumers (e.g. a separate
//! Arrow IPC writer crate) to reuse the conversion without depending on the
//! `parquet` crate.
//!
//! The `parquet` feature depends on the `arrow` feature, so all of the
//! conversion logic is available to the Parquet writer without code duplication.
//!
//! # CQL → Arrow type mapping
//!
//! When `ColumnInfo.cql_type` is `Some`, the following high-fidelity mappings
//! are used instead of the flat `data_type` fallback:
//!
//! | CQL type          | Arrow type                            | Notes                             |
//! |-------------------|---------------------------------------|-----------------------------------|
//! | date              | `Date32`                              | Signed days since 1970-01-01      |
//! | time              | `Time64(Nanosecond)`                  | Nanos since midnight              |
//! | decimal           | `Decimal128(38, DECIMAL_FIXED_SCALE)` | Rescaled; see strategy below      |
//! | varint            | `Decimal128(38, 0)` or `Utf8`         | `Utf8` fallback on overflow       |
//! | duration          | `Utf8` (CQL text form)                | Parquet crate v53 NYI MonthDayNano|
//! | uuid/timeuuid     | `FixedSizeBinary(16)` + UUID ext      | Arrow UUID extension metadata     |
//! | inet              | `Utf8`                                | Canonical textual form (deliberate)|
//! | counter           | `Int64`                               | Unchanged                         |
//! | list\<X\>         | `List<mapped(X)>`                     | Recursive element mapping         |
//! | set\<X\>          | `List<mapped(X)>`                     | Arrow has no Set type; uses List  |
//! | map\<K,V\>        | `Map<Struct(key:K,value:V)>`          | Typed keys/values; nested OK      |
//! | tuple\<A,B,…\>    | `Struct(field_0:A, field_1:B, …)`     | Positional names; per-position types|
//! | udt\<name\>       | `Struct(f1:T1, f2:T2, …)`             | Field names from schema; null OK  |

use crate::query::{ColumnInfo, QueryRow};
use crate::schema::CqlType;
use crate::types::{DataType, Value};
use crate::util::value_fmt::ValueFormatter;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, ListArray, MapArray, StringArray, StructArray,
    Time64NanosecondArray, TimestampMillisecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

// ============================================================================
// Constants
// ============================================================================

/// Fixed scale used for all `decimal` columns mapped to `Decimal128`.
///
/// We choose 9 (nanosecond-like resolution) as a reasonable default that
/// accommodates most CQL decimal use-cases without requiring per-column
/// inspection of the data.
pub(crate) const DECIMAL_FIXED_SCALE: i32 = 9;

/// Maximum precision for `Decimal128` (Arrow/Parquet limit).
pub(crate) const DECIMAL_MAX_PRECISION: u8 = 38;

/// Arrow UUID extension type name, as specified by the Arrow spec.
/// The field metadata key `ARROW:extension:name` = `arrow.uuid` triggers
/// the Parquet UUID logical type annotation.
pub(crate) const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
pub(crate) const ARROW_UUID_EXTENSION_NAME: &str = "arrow.uuid";

// ============================================================================
// Error type
// ============================================================================

/// Errors produced by the CQL → Arrow conversion.
///
/// A dedicated `thiserror` enum so that callers (e.g. `ParquetExportError`)
/// can wrap or delegate to this error without pulling in Parquet-specific
/// error types.
#[derive(Debug, Error)]
pub enum ArrowConvertError {
    /// Arrow array or schema construction failure.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A value could not be represented in the target Arrow type.
    #[error("{0}")]
    InvalidValue(String),
}

// ============================================================================
// Public API
// ============================================================================

/// Build an Arrow [`Schema`] from CQL column metadata.
///
/// Each column is mapped through the high-fidelity CQL type path when
/// `col.cql_type` is `Some`, falling back to the flat `DataType` mapping
/// otherwise.
///
/// This is the same schema building logic used by [`rows_to_record_batch`].
pub fn build_arrow_schema(columns: &[ColumnInfo]) -> Result<Schema, ArrowConvertError> {
    let fields: Vec<Field> = columns.iter().map(column_to_field).collect();
    Ok(Schema::new(fields))
}

/// Convert a slice of [`QueryRow`] values to an Arrow [`RecordBatch`].
///
/// The schema is derived from `columns` via [`build_arrow_schema`].  Each
/// column is converted to an Arrow array using the same type mapping.
///
/// # Errors
///
/// Returns [`ArrowConvertError`] if any value cannot be represented in the
/// target Arrow type, or if the Arrow schema/array construction fails.
pub fn rows_to_record_batch(
    columns: &[ColumnInfo],
    rows: &[QueryRow],
) -> Result<RecordBatch, ArrowConvertError> {
    let schema = build_arrow_schema(columns)?;
    let arrays = convert_to_arrays(columns, rows)?;
    let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;
    Ok(batch)
}

// ============================================================================
// BigInt → i128 helper
// ============================================================================

/// Convert a `num_bigint::BigInt` to `i128`, sign-extending if necessary.
///
/// Uses the two's-complement big-endian representation via
/// `to_signed_bytes_be()` and sign-extends to 16 bytes before reinterpreting
/// as `i128`.  Returns an error if the value requires more than 16 bytes
/// (i.e. exceeds the i128 range).
pub(crate) fn bigint_to_i128(n: &num_bigint::BigInt) -> Result<i128, ArrowConvertError> {
    let tc_bytes = n.to_signed_bytes_be();
    if tc_bytes.len() > 16 {
        return Err(ArrowConvertError::InvalidValue(
            "BigInt value requires more than 16 bytes; cannot fit in i128".to_string(),
        ));
    }
    // Determine the sign-extension byte: 0x00 for non-negative, 0xFF for negative.
    let pad: u8 = if n.sign() == num_bigint::Sign::Minus {
        0xFF
    } else {
        0x00
    };
    let mut buf = [pad; 16];
    // Copy the two's-complement bytes into the *right* side of the buffer.
    buf[16 - tc_bytes.len()..].copy_from_slice(&tc_bytes);
    Ok(i128::from_be_bytes(buf))
}

// ============================================================================
// Schema building helpers
// ============================================================================

/// Convert a CQL column to an Arrow [`Field`].
///
/// When `col.cql_type` is `Some`, the high-fidelity schema mapping
/// (`cql_type_to_arrow_field`) is used.  For scalar types this produces the
/// correct Arrow logical type (e.g. `Date32`, `Time64`, `Decimal128`).
pub(crate) fn column_to_field(col: &ColumnInfo) -> Field {
    if let Some(cql_type) = &col.cql_type {
        if let Some(field) = cql_type_to_arrow_field(&col.name, cql_type, col.nullable) {
            return field;
        }
    }
    let arrow_type = data_type_to_arrow(&col.data_type);
    Field::new(&col.name, arrow_type, col.nullable)
}

/// Map a scalar `CqlType` to an Arrow `Field`, returning `None` for complex
/// or unknown types so the caller can fall back to `data_type_to_arrow`.
///
/// UUID and TimeUUID columns receive the canonical Arrow UUID extension
/// metadata (`ARROW:extension:name` = `arrow.uuid`) so that Parquet readers
/// emit the Parquet UUID logical type.
///
/// `CqlType::Frozen(inner)` transparently unwraps to `inner`.
pub(crate) fn cql_type_to_arrow_field(
    name: &str,
    cql_type: &CqlType,
    nullable: bool,
) -> Option<Field> {
    match cql_type {
        CqlType::Date => Some(Field::new(name, ArrowDataType::Date32, nullable)),
        CqlType::Time => Some(Field::new(
            name,
            ArrowDataType::Time64(TimeUnit::Nanosecond),
            nullable,
        )),
        CqlType::Decimal => Some(Field::new(
            name,
            ArrowDataType::Decimal128(DECIMAL_MAX_PRECISION, DECIMAL_FIXED_SCALE as i8),
            nullable,
        )),
        CqlType::Varint => {
            // varint → Decimal128(38, 0) — the integer domain.
            // Values that exceed 38 digits will be detected at write time and
            // produce an error (never silently truncated).
            Some(Field::new(
                name,
                ArrowDataType::Decimal128(DECIMAL_MAX_PRECISION, 0),
                nullable,
            ))
        }
        CqlType::Duration => {
            // NOTE: The Parquet format's INTERVAL logical type does not support
            // nanosecond precision (only months + days + milliseconds).  The
            // `parquet` crate v53 therefore refuses to write
            // `Interval(MonthDayNano)` and returns an NYI error at write time.
            //
            // Arrow `Interval(MonthDayNano)` is the correct *Arrow* type for
            // CQL duration (months + days + nanos), but it cannot be persisted
            // to Parquet in this crate version.  We fall back to `Utf8` using
            // the canonical CQL textual representation (e.g. "1mo2d3ns") so
            // that the data is always readable.  Once the parquet crate gains
            // MonthDayNano write support this can be upgraded.
            Some(Field::new(name, ArrowDataType::Utf8, nullable))
        }
        CqlType::Uuid | CqlType::TimeUuid => {
            // FixedSizeBinary(16) with the Arrow UUID extension metadata so that
            // Parquet readers interpret the column as UUID logical type.
            let mut meta = HashMap::new();
            meta.insert(
                ARROW_EXTENSION_NAME_KEY.to_string(),
                ARROW_UUID_EXTENSION_NAME.to_string(),
            );
            Some(
                Field::new(name, ArrowDataType::FixedSizeBinary(16), nullable)
                    .with_metadata(meta),
            )
        }
        CqlType::Inet => Some(Field::new(name, ArrowDataType::Utf8, nullable)),
        CqlType::Counter => Some(Field::new(name, ArrowDataType::Int64, nullable)),
        // List and Set: map to Arrow List with recursively mapped element type.
        // Arrow has no dedicated Set type; Set is represented as List.
        CqlType::List(inner) | CqlType::Set(inner) => {
            let item_type = cql_type_to_arrow_data_type(inner);
            let item_field = Arc::new(Field::new("item", item_type, true));
            Some(Field::new(name, ArrowDataType::List(item_field), nullable))
        }
        // Frozen<T> is transparent: same Arrow type as T.
        CqlType::Frozen(inner) => cql_type_to_arrow_field(name, inner, nullable),
        // Map: emit typed Arrow Map with non-nullable keys and nullable values.
        // The entries struct is conventionally named "entries" with children
        // "key" (non-nullable) and "value" (nullable).
        CqlType::Map(key_type, val_type) => {
            let key_arrow = cql_type_to_arrow_data_type(key_type);
            let val_arrow = cql_type_to_arrow_data_type(val_type);
            let entries_field = Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("key", key_arrow, false),
                    Field::new("value", val_arrow, true),
                ])),
                false,
            ));
            Some(Field::new(
                name,
                ArrowDataType::Map(entries_field, false),
                nullable,
            ))
        }
        // Tuple<A,B,…> → Arrow Struct with positional field names.
        // Zero-field tuples fall back to Utf8 (Arrow Struct requires ≥1 field).
        CqlType::Tuple(element_types) => {
            if element_types.is_empty() {
                return Some(Field::new(name, ArrowDataType::Utf8, nullable));
            }
            let struct_type = cql_type_to_arrow_data_type(cql_type);
            Some(Field::new(name, struct_type, nullable))
        }
        // UDT → Arrow Struct with the UDT's field names.
        // Zero-field UDTs fall back to Utf8 (Arrow Struct requires ≥1 field).
        CqlType::Udt(_udt_name, udt_fields) => {
            if udt_fields.is_empty() {
                return Some(Field::new(name, ArrowDataType::Utf8, nullable));
            }
            let struct_type = cql_type_to_arrow_data_type(cql_type);
            Some(Field::new(name, struct_type, nullable))
        }
        // Remaining scalar types are already handled correctly by the flat
        // DataType mapping; return None to allow that path to run.
        _ => None,
    }
}

// =========================================================================
// Recursive CQL type → Arrow DataType mapping
// =========================================================================

/// Recursively map a `CqlType` to an Arrow `DataType`.
///
/// This function is the single source of truth for element-type mapping used
/// by both the schema-building path (`cql_type_to_arrow_field`) and the
/// value-building path (`build_typed_value_array`).  It handles all scalar
/// types and recursively handles `List`, `Set`, `Frozen`, `Map`, `Tuple`,
/// and `Udt`.
///
/// `CqlType::Frozen(inner)` is transparent: the same Arrow type as `inner`.
///
/// Zero-field `Tuple` and `Udt` fall back to `Utf8` because Arrow `Struct`
/// with zero fields cannot be represented in Parquet.
pub(crate) fn cql_type_to_arrow_data_type(cql_type: &CqlType) -> ArrowDataType {
    match cql_type {
        // Scalar types
        CqlType::Boolean => ArrowDataType::Boolean,
        CqlType::TinyInt => ArrowDataType::Int8,
        CqlType::SmallInt => ArrowDataType::Int16,
        CqlType::Int => ArrowDataType::Int32,
        CqlType::BigInt => ArrowDataType::Int64,
        CqlType::Counter => ArrowDataType::Int64,
        CqlType::Float => ArrowDataType::Float32,
        CqlType::Double => ArrowDataType::Float64,
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => ArrowDataType::Utf8,
        CqlType::Blob => ArrowDataType::Binary,
        CqlType::Timestamp => {
            ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        }
        CqlType::Date => ArrowDataType::Date32,
        CqlType::Time => ArrowDataType::Time64(TimeUnit::Nanosecond),
        CqlType::Decimal => {
            ArrowDataType::Decimal128(DECIMAL_MAX_PRECISION, DECIMAL_FIXED_SCALE as i8)
        }
        CqlType::Varint => ArrowDataType::Decimal128(DECIMAL_MAX_PRECISION, 0),
        // Duration: Utf8 fallback (parquet crate v53 MonthDayNano NYI)
        CqlType::Duration => ArrowDataType::Utf8,
        CqlType::Uuid | CqlType::TimeUuid => ArrowDataType::FixedSizeBinary(16),
        // Inet: canonical text form
        CqlType::Inet => ArrowDataType::Utf8,
        // List/Set → Arrow List with recursively mapped element type.
        // Arrow has no dedicated Set type; both map to List.
        CqlType::List(inner) | CqlType::Set(inner) => {
            let item_type = cql_type_to_arrow_data_type(inner);
            ArrowDataType::List(Arc::new(Field::new("item", item_type, true)))
        }
        // Frozen<T> is transparent in type mapping.
        CqlType::Frozen(inner) => cql_type_to_arrow_data_type(inner),
        // Map: Arrow Map type with typed key (non-nullable) and value (nullable).
        // The entries struct field is named "entries" with children "key" and
        // "value".
        CqlType::Map(key_type, val_type) => {
            let key_arrow = cql_type_to_arrow_data_type(key_type);
            let val_arrow = cql_type_to_arrow_data_type(val_type);
            ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowDataType::Struct(Fields::from(vec![
                        Field::new("key", key_arrow, false),
                        Field::new("value", val_arrow, true),
                    ])),
                    false,
                )),
                false,
            )
        }
        // Tuple<A, B, …> → Struct(field_0: A, field_1: B, …).
        // Zero-field tuples fall back to Utf8 (Arrow Struct requires ≥1 field).
        CqlType::Tuple(element_types) => {
            if element_types.is_empty() {
                return ArrowDataType::Utf8;
            }
            let struct_fields: Vec<Field> = element_types
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    Field::new(
                        format!("field_{i}"),
                        cql_type_to_arrow_data_type(t),
                        true, // tuple positions are always nullable
                    )
                })
                .collect();
            ArrowDataType::Struct(Fields::from(struct_fields))
        }
        // UDT → Struct with the UDT's schema field names and recursively mapped types.
        // Zero-field UDTs fall back to Utf8 (Arrow Struct requires ≥1 field).
        CqlType::Udt(_udt_name, udt_fields) => {
            if udt_fields.is_empty() {
                return ArrowDataType::Utf8;
            }
            let struct_fields: Vec<Field> = udt_fields
                .iter()
                .map(|(field_name, field_type)| {
                    Field::new(
                        field_name.as_str(),
                        cql_type_to_arrow_data_type(field_type),
                        true, // UDT fields are always nullable (can be unset)
                    )
                })
                .collect();
            ArrowDataType::Struct(Fields::from(struct_fields))
        }
        // Custom/unknown types: Utf8
        CqlType::Custom(_) => ArrowDataType::Utf8,
    }
}

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
                        Value::Boolean(b) => Some(*b),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(BooleanArray::from(arr)))
        }
        CqlType::TinyInt => {
            let arr: Vec<Option<i8>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::TinyInt(i) => Some(*i),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(Int8Array::from(arr)))
        }
        CqlType::SmallInt => {
            let arr: Vec<Option<i16>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::SmallInt(i) => Some(*i),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(Int16Array::from(arr)))
        }
        CqlType::Int => {
            let arr: Vec<Option<i32>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Integer(i) => Some(*i),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(Int32Array::from(arr)))
        }
        CqlType::BigInt => {
            let arr: Vec<Option<i64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::BigInt(i) => Some(*i),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(Int64Array::from(arr)))
        }
        CqlType::Counter => {
            let arr: Vec<Option<i64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Counter(c) => Some(*c),
                        Value::BigInt(i) => Some(*i),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(Int64Array::from(arr)))
        }
        CqlType::Float => {
            let arr: Vec<Option<f32>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Float32(f) => Some(*f),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(Float32Array::from(arr)))
        }
        CqlType::Double => {
            let arr: Vec<Option<f64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Float(f) => Some(*f),
                        Value::Float32(f) => Some(*f as f64),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(Float64Array::from(arr)))
        }
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => {
            let arr: Vec<Option<String>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Text(s) => Some(s.clone()),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(arr)))
        }
        CqlType::Blob => {
            let byte_slices: Vec<Option<Vec<u8>>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Blob(b) => Some(b.clone()),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            let refs: Vec<Option<&[u8]>> = byte_slices.iter().map(|o| o.as_deref()).collect();
            Ok(Arc::new(BinaryArray::from(refs)))
        }
        CqlType::Timestamp => {
            let arr: Vec<Option<i64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Timestamp(ts) => Some(*ts),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
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
                        Value::Date(d) => Some(*d),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(Date32Array::from(arr)))
        }
        CqlType::Time => {
            let arr: Vec<Option<i64>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Time(t) => Some(*t),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(Time64NanosecondArray::from(arr)))
        }
        CqlType::Decimal => {
            let mut builder = arrow::array::Decimal128Builder::new()
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
            let mut builder = arrow::array::Decimal128Builder::new()
                .with_precision_and_scale(DECIMAL_MAX_PRECISION, 0)?;
            for opt in values {
                let v = unwrap_frozen_value(*opt);
                match v {
                    Some(Value::Varint(bytes)) => {
                        if bytes.is_empty() {
                            builder.append_value(0);
                        } else {
                            let bigint = BigInt::from_signed_bytes_be(bytes);
                            let max_abs =
                                BigInt::from(10i64).pow(38u32) - BigInt::from(1i64);
                            let abs_val = if bigint.sign() == num_bigint::Sign::Minus {
                                -bigint.clone()
                            } else {
                                bigint.clone()
                            };
                            if abs_val > max_abs {
                                return Err(ArrowConvertError::InvalidValue(
                                    "varint element exceeds Decimal128(38, 0) range"
                                        .to_string(),
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
                        Value::Duration { .. } => Some(ValueFormatter::format_value(v)),
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(arr)))
        }
        CqlType::Uuid | CqlType::TimeUuid => {
            let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
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
                        Value::Inet(bytes) => {
                            Some(ValueFormatter::format_value(&Value::Inet(bytes.clone())))
                        }
                        Value::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(arr)))
        }
        // ----------------------------------------------------------------
        // List and Set (recursive): element type dispatches back here.
        // Arrow has no dedicated Set type; Set maps to List.
        // ----------------------------------------------------------------
        CqlType::List(inner) | CqlType::Set(inner) => {
            let element_type = cql_type_to_arrow_data_type(inner);
            let item_field = Arc::new(Field::new("item", element_type, true));

            // Collect flat elements for all list/set values,
            // recording offsets so we can reconstruct the list structure.
            let mut offsets: Vec<i32> = vec![0];
            let mut flat_elements: Vec<Option<&Value>> = Vec::new();
            let mut null_bitmap: Vec<bool> = Vec::new();

            for opt in values {
                let v = unwrap_frozen_value(*opt);
                match v {
                    Some(Value::List(items)) | Some(Value::Set(items)) => {
                        null_bitmap.push(true);
                        for item in items {
                            flat_elements.push(Some(item));
                        }
                        offsets.push(flat_elements.len() as i32);
                    }
                    Some(Value::Null) | None => {
                        null_bitmap.push(false);
                        offsets.push(flat_elements.len() as i32);
                    }
                    Some(other) => {
                        // Unexpected value type — serialize as empty list to
                        // avoid hard failure (defensive; shouldn't happen with
                        // correct schema).
                        null_bitmap.push(false);
                        offsets.push(flat_elements.len() as i32);
                        let _ = other; // suppress unused warning
                    }
                }
            }

            // Recursively build the flat element array using the inner type.
            let elements_array = build_typed_value_array(inner, &flat_elements)?;

            let offset_buffer = OffsetBuffer::new(offsets.into());
            let null_buffer = NullBuffer::from(null_bitmap);

            Ok(Arc::new(ListArray::new(
                item_field,
                offset_buffer,
                elements_array,
                Some(null_buffer),
            )))
        }
        // Frozen is unwrapped above in `unwrap_frozen_type`; this arm is
        // unreachable but required for exhaustiveness.
        CqlType::Frozen(inner) => build_typed_value_array(inner, values),
        // ----------------------------------------------------------------
        // Map: recursively typed keys and values.
        //
        // Arrow Map is represented as:
        //   Map<Struct("entries") { key: K (non-nullable), value: V (nullable) }>
        //
        // We collect flat (key, value) pairs from all rows, track per-row
        // offsets, recursively build the key and value arrays via the same
        // recursive builder, then assemble a MapArray.
        //
        // Null key policy: a Value::Null in the key position is an error
        // (Arrow MapArray requires non-nullable keys).  We return an error
        // clearly rather than silently skip the entry.
        // ----------------------------------------------------------------
        CqlType::Map(key_type, val_type) => {
            let key_arrow = cql_type_to_arrow_data_type(key_type);
            let val_arrow = cql_type_to_arrow_data_type(val_type);

            let mut offsets: Vec<i32> = vec![0];
            let mut flat_keys: Vec<Option<&Value>> = Vec::new();
            let mut flat_vals: Vec<Option<&Value>> = Vec::new();
            let mut null_bitmap: Vec<bool> = Vec::new();

            for opt in values {
                let v = unwrap_frozen_value(*opt);
                match v {
                    Some(Value::Map(pairs)) => {
                        null_bitmap.push(true);
                        for (k, val) in pairs {
                            // Keys must be non-nullable in Arrow MapArray.
                            if matches!(k, Value::Null) {
                                return Err(ArrowConvertError::InvalidValue(
                                    "null key in map is not allowed in Arrow MapArray"
                                        .to_string(),
                                ));
                            }
                            flat_keys.push(Some(k));
                            flat_vals.push(Some(val));
                        }
                        offsets.push(flat_keys.len() as i32);
                    }
                    Some(Value::Null) | None => {
                        null_bitmap.push(false);
                        offsets.push(flat_keys.len() as i32);
                    }
                    Some(other) => {
                        // Unexpected value type — treat as null map entry
                        // (defensive; shouldn't happen with correct schema).
                        null_bitmap.push(false);
                        offsets.push(flat_keys.len() as i32);
                        let _ = other;
                    }
                }
            }

            // Recursively build the flat key and value arrays.
            let key_array = build_typed_value_array(key_type, &flat_keys)?;
            let val_array = build_typed_value_array(val_type, &flat_vals)?;

            // Build the entries StructArray (no validity buffer: all non-null).
            let struct_fields = Fields::from(vec![
                Field::new("key", key_arrow, false),
                Field::new("value", val_arrow, true),
            ]);
            let entries_array =
                StructArray::new(struct_fields.clone(), vec![key_array, val_array], None);

            let map_field = Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(struct_fields),
                false,
            ));
            let offset_buffer = OffsetBuffer::new(offsets.into());
            let null_buffer = NullBuffer::from(null_bitmap);

            Ok(Arc::new(MapArray::new(
                map_field,
                offset_buffer,
                entries_array,
                Some(null_buffer),
                false,
            )))
        }
        // ----------------------------------------------------------------
        // Tuple<A, B, …>: Arrow Struct with positional field names.
        //
        // For each field position i, we collect per-row child values by
        // indexing into the `Value::Tuple` element vector.  Rows whose
        // tuple is shorter than the schema position, or whose top-level
        // value is Null/absent, contribute None for that child position.
        //
        // Zero-field tuples (degenerate) fall back to Utf8.
        // ----------------------------------------------------------------
        CqlType::Tuple(element_types) => {
            if element_types.is_empty() {
                // Degenerate case: no fields → Utf8 fallback.
                let arr: Vec<Option<String>> = values
                    .iter()
                    .map(|opt| match opt {
                        Some(Value::Null) | None => None,
                        Some(v) => Some(ValueFormatter::format_value(v)),
                    })
                    .collect();
                return Ok(Arc::new(StringArray::from(arr)));
            }

            let n_rows = values.len();
            let n_fields = element_types.len();

            // Unwrap Frozen at the value level before inspecting tuples.
            let unwrapped: Vec<Option<&Value>> = values
                .iter()
                .map(|opt| unwrap_frozen_value(*opt))
                .collect();

            // Build a null bitmap: true = row is non-null (valid struct).
            let null_bitmap: Vec<bool> = unwrapped
                .iter()
                .map(|v| !matches!(v, Some(Value::Null) | None))
                .collect();

            // For each schema field position, build a Vec<Option<&Value>>
            // by pulling out the element at that position.
            //
            // IMPORTANT: absent/null positions must contribute `Some(&Value::Null)`
            // rather than `None`.  The scalar type builders use `?` on
            // `unwrap_frozen_value` which silently drops `None` entries via
            // `flatten()`, producing a shorter child array than the struct
            // expects.  Arrow's StructArray::new() panics on length mismatch.
            // Using `Some(&Value::Null)` keeps every row represented while
            // the builder treats it as a null element.
            let null_sentinel = Value::Null;
            let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(n_fields);
            for (field_idx, element_type) in element_types.iter().enumerate() {
                let child_values: Vec<Option<&Value>> = (0..n_rows)
                    .map(|row_idx| {
                        match unwrapped[row_idx] {
                            Some(Value::Tuple(items)) => {
                                // Missing trailing positions → null sentinel.
                                Some(
                                    items
                                        .get(field_idx)
                                        .map(|v| v as &Value)
                                        .unwrap_or(&null_sentinel),
                                )
                            }
                            // Null row or wrong type → null sentinel.
                            _ => Some(&null_sentinel),
                        }
                    })
                    .collect();
                let child_arr = build_typed_value_array(element_type, &child_values)?;
                child_arrays.push(child_arr);
            }

            let struct_fields: Fields = Fields::from(
                element_types
                    .iter()
                    .enumerate()
                    .map(|(i, t)| {
                        Field::new(
                            format!("field_{i}"),
                            cql_type_to_arrow_data_type(t),
                            true,
                        )
                    })
                    .collect::<Vec<_>>(),
            );

            let null_buffer = NullBuffer::from(null_bitmap);
            Ok(Arc::new(StructArray::new(
                struct_fields,
                child_arrays,
                Some(null_buffer),
            )))
        }
        // ----------------------------------------------------------------
        // Udt: Arrow Struct with the UDT's schema field names.
        //
        // The CQL type carries the schema field order and types.  For each
        // schema field, we look up the matching UdtField by name in each
        // row's Value::Udt.  Missing fields and fields whose value is None
        // (unset) become null in the child array.
        //
        // Zero-field UDTs fall back to Utf8.
        // ----------------------------------------------------------------
        CqlType::Udt(_udt_name, udt_fields) => {
            if udt_fields.is_empty() {
                // Degenerate case: no fields → Utf8 fallback.
                let arr: Vec<Option<String>> = values
                    .iter()
                    .map(|opt| match opt {
                        Some(Value::Null) | None => None,
                        Some(v) => Some(ValueFormatter::format_value(v)),
                    })
                    .collect();
                return Ok(Arc::new(StringArray::from(arr)));
            }

            let n_rows = values.len();

            // Unwrap Frozen at the value level before inspecting UDTs.
            let unwrapped: Vec<Option<&Value>> = values
                .iter()
                .map(|opt| unwrap_frozen_value(*opt))
                .collect();

            // Build a null bitmap: true = row is non-null (valid struct).
            let null_bitmap: Vec<bool> = unwrapped
                .iter()
                .map(|v| !matches!(v, Some(Value::Null) | None))
                .collect();

            // For each schema field, build a child array by looking up the
            // field by name in each row's UdtValue.
            //
            // IMPORTANT: absent/null positions must contribute `Some(&Value::Null)`
            // rather than `None`.  See the Tuple arm above for the explanation.
            let null_sentinel = Value::Null;
            let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(udt_fields.len());
            for (field_name, field_type) in udt_fields.iter() {
                let child_values: Vec<Option<&Value>> = (0..n_rows)
                    .map(|row_idx| match unwrapped[row_idx] {
                        Some(Value::Udt(udt_val)) => {
                            // Look up by field name; null sentinel if absent or unset.
                            Some(
                                udt_val
                                    .fields
                                    .iter()
                                    .find(|f| &f.name == field_name)
                                    .and_then(|f| f.value.as_ref().map(|v| v as &Value))
                                    .unwrap_or(&null_sentinel),
                            )
                        }
                        // Null row or wrong type → null sentinel.
                        _ => Some(&null_sentinel),
                    })
                    .collect();
                let child_arr = build_typed_value_array(field_type, &child_values)?;
                child_arrays.push(child_arr);
            }

            let struct_fields: Fields = Fields::from(
                udt_fields
                    .iter()
                    .map(|(field_name, field_type)| {
                        Field::new(
                            field_name.as_str(),
                            cql_type_to_arrow_data_type(field_type),
                            true,
                        )
                    })
                    .collect::<Vec<_>>(),
            );

            let null_buffer = NullBuffer::from(null_bitmap);
            Ok(Arc::new(StructArray::new(
                struct_fields,
                child_arrays,
                Some(null_buffer),
            )))
        }
        CqlType::Custom(_) => {
            let arr: Vec<Option<String>> = values
                .iter()
                .map(|opt| match opt {
                    Some(Value::Null) | None => None,
                    Some(v) => Some(ValueFormatter::format_value(v)),
                })
                .collect();
            Ok(Arc::new(StringArray::from(arr)))
        }
    }
}

/// Unwrap nested `CqlType::Frozen` wrappers to reach the effective type.
///
/// `Frozen(Frozen(T))` → `T`. This handles the rare but valid case of
/// double-frozen types in schema definitions.
pub(crate) fn unwrap_frozen_type(cql_type: &CqlType) -> &CqlType {
    let mut t = cql_type;
    while let CqlType::Frozen(inner) = t {
        t = inner.as_ref();
    }
    t
}

/// Unwrap a `Value::Frozen(inner)` reference to its inner value.
///
/// Returns the inner value reference if `v` is `Frozen`, or the original
/// reference otherwise.  `None` (absent column value) is passed through.
pub(crate) fn unwrap_frozen_value(v: Option<&Value>) -> Option<&Value> {
    match v {
        Some(Value::Frozen(inner)) => Some(inner.as_ref()),
        other => other,
    }
}

/// Map CQL `DataType` to Arrow `DataType` (flat fallback path).
///
/// This is used when `ColumnInfo.cql_type` is `None`.
pub(crate) fn data_type_to_arrow(data_type: &DataType) -> ArrowDataType {
    match data_type {
        DataType::Null => ArrowDataType::Null,
        DataType::Boolean => ArrowDataType::Boolean,
        DataType::TinyInt => ArrowDataType::Int8,
        DataType::SmallInt => ArrowDataType::Int16,
        DataType::Integer => ArrowDataType::Int32,
        DataType::BigInt => ArrowDataType::Int64,
        DataType::Float32 => ArrowDataType::Float32,
        DataType::Float => ArrowDataType::Float64,
        DataType::Text => ArrowDataType::Utf8,
        DataType::Blob => ArrowDataType::Binary,
        DataType::Timestamp => {
            ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        }
        DataType::Uuid => ArrowDataType::FixedSizeBinary(16),
        DataType::Json => ArrowDataType::Utf8,
        DataType::List => {
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true)))
        }
        DataType::Set => {
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true)))
        }
        DataType::Map => ArrowDataType::Map(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("key", ArrowDataType::Utf8, false),
                    Field::new("value", ArrowDataType::Utf8, true),
                ])),
                false,
            )),
            false,
        ),
        DataType::Tuple => ArrowDataType::Utf8, // Serialize as JSON string
        DataType::Udt => ArrowDataType::Utf8,   // Serialize as JSON string
        DataType::Frozen => ArrowDataType::Utf8,
        DataType::Tombstone => ArrowDataType::Utf8,
    }
}

// =========================================================================
// Column-oriented conversion
// =========================================================================

/// Convert all rows to Arrow arrays (one per column).
pub(crate) fn convert_to_arrays(
    columns: &[ColumnInfo],
    rows: &[QueryRow],
) -> Result<Vec<ArrayRef>, ArrowConvertError> {
    columns
        .iter()
        .map(|col| convert_column_to_array(col, rows))
        .collect()
}

/// Convert a single column across all rows to an Arrow array.
///
/// When `col.cql_type` is `Some` and the type is a high-fidelity scalar
/// (date, time, decimal, varint, duration, uuid/timeuuid, inet, counter),
/// the corresponding typed builder is used.
///
/// For `List`, `Set`, and `Frozen(List|Set)` with a `cql_type`, the
/// recursive `build_typed_value_array` path is used, which maps element
/// types through the same scalar mapping above.
///
/// All other cases fall through to the existing flat `data_type`-based
/// dispatch.
pub(crate) fn convert_column_to_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    // High-fidelity CQL-type dispatch
    if let Some(cql_type) = &col.cql_type {
        // Check if the (possibly Frozen-wrapped) type is a List or Set.
        let effective = unwrap_frozen_type(cql_type);
        match effective {
            CqlType::Date => return build_date32_array(col, rows),
            CqlType::Time => return build_time64_ns_array(col, rows),
            CqlType::Decimal => return build_decimal128_array(col, rows),
            CqlType::Varint => return build_varint_as_decimal128_array(col, rows),
            CqlType::Duration => return build_duration_utf8_array(col, rows),
            CqlType::Uuid | CqlType::TimeUuid => {
                return build_uuid_fixed_binary_array(col, rows)
            }
            CqlType::Inet => return build_inet_utf8_array(col, rows),
            CqlType::Counter => return build_int64_array(col, rows),
            // List, Set, Map, Tuple, and Udt: use the recursive typed builder.
            CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _) => {
                let column_values: Vec<Option<&Value>> =
                    rows.iter().map(|row| row.values.get(&col.name)).collect();
                return build_typed_value_array(cql_type, &column_values);
            }
            // All other complex/collection types fall through to the flat dispatch.
            _ => {}
        }
    }

    // Flat data_type dispatch (legacy path)
    match &col.data_type {
        DataType::Boolean => build_boolean_array(col, rows),
        DataType::TinyInt => build_int8_array(col, rows),
        DataType::SmallInt => build_int16_array(col, rows),
        DataType::Integer => build_int32_array(col, rows),
        DataType::BigInt => build_int64_array(col, rows),
        DataType::Float32 => build_float32_array(col, rows),
        DataType::Float => build_float64_array(col, rows),
        DataType::Text | DataType::Json => build_string_array(col, rows),
        DataType::Blob => build_binary_array(col, rows),
        DataType::Timestamp => build_timestamp_array(col, rows),
        DataType::Uuid => build_uuid_array(col, rows),
        DataType::List | DataType::Set => build_list_array(col, rows),
        DataType::Map => build_map_array(col, rows),
        DataType::Tuple
        | DataType::Udt
        | DataType::Frozen
        | DataType::Tombstone
        | DataType::Null => {
            build_string_array(col, rows) // Fallback to string representation
        }
    }
}

// =========================================================================
// Rescaling helper
// =========================================================================

/// Rescale a CQL decimal value to the fixed column scale (`DECIMAL_FIXED_SCALE`).
///
/// Returns the rescaled `i128` value, or an error if:
/// - The rescaled magnitude exceeds 38 decimal digits (overflow of `Decimal128`).
/// - Checked multiplication overflows `i128` when scaling up.
pub(crate) fn rescale_decimal(scale: i32, unscaled: &[u8]) -> Result<i128, ArrowConvertError> {
    use num_bigint::BigInt;

    if unscaled.is_empty() {
        return Ok(0i128);
    }

    // Decode big-endian two's-complement signed integer.
    let bigint = BigInt::from_signed_bytes_be(unscaled);

    // Compute scale delta: positive means we must multiply (scale up),
    // negative means we must divide (scale down / truncate).
    let delta = DECIMAL_FIXED_SCALE - scale;

    let rescaled = if delta == 0 {
        bigint
    } else if delta > 0 {
        // Scale up: multiply by 10^delta.
        let factor = BigInt::from(10i64).pow(delta as u32);
        bigint * factor
    } else {
        // Scale down: divide by 10^(-delta), truncating toward zero.
        let factor = BigInt::from(10i64).pow((-delta) as u32);
        bigint / factor
    };

    // Verify the result fits in Decimal128(38, …).
    // 10^38 − 1 is the maximum absolute value representable.
    let max_abs = BigInt::from(10i64).pow(38u32) - BigInt::from(1i64);
    let abs_rescaled = if rescaled.sign() == num_bigint::Sign::Minus {
        -rescaled.clone()
    } else {
        rescaled.clone()
    };
    if abs_rescaled > max_abs {
        return Err(ArrowConvertError::InvalidValue(format!(
            "Decimal value exceeds Decimal128(38, {DECIMAL_FIXED_SCALE}) range after rescaling"
        )));
    }

    bigint_to_i128(&rescaled)
}

// =========================================================================
// Type-specific array builders (flat / column-based)
// =========================================================================

fn build_boolean_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<bool>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Boolean(b) => Some(*b),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(BooleanArray::from(values)))
}

fn build_int8_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i8>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::TinyInt(i) => Some(*i),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(Int8Array::from(values)))
}

fn build_int16_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i16>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::SmallInt(i) => Some(*i),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(Int16Array::from(values)))
}

fn build_int32_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i32>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Integer(i) => Some(*i),
                Value::Date(d) => Some(*d), // Date is stored as i32 days
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(Int32Array::from(values)))
}

fn build_int64_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i64>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::BigInt(i) => Some(*i),
                Value::Counter(c) => Some(*c),
                Value::Time(t) => Some(*t), // Time is stored as i64 nanos
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(Int64Array::from(values)))
}

fn build_float32_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<f32>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Float32(f) => Some(*f),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(Float32Array::from(values)))
}

fn build_float64_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<f64>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Float(f) => Some(*f),
                Value::Float32(f) => Some(*f as f64),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(Float64Array::from(values)))
}

fn build_string_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<String>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Null => None,
                Value::Text(s) => Some(s.clone()),
                Value::Json(j) => Some(j.to_string()),
                // Use ValueFormatter for complex types
                other => Some(ValueFormatter::format_value(other)),
            })
        })
        .collect();
    Ok(Arc::new(StringArray::from(values)))
}

fn build_binary_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<&[u8]>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Blob(b) => Some(b.as_slice()),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(BinaryArray::from(values)))
}

fn build_timestamp_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i64>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Timestamp(ts) => Some(*ts),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(
        TimestampMillisecondArray::from(values).with_timezone("UTC"),
    ))
}

fn build_uuid_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<[u8; 16]>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Uuid(uuid) => Some(*uuid),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();

    let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
    for opt in values {
        match opt {
            Some(uuid) => builder.append_value(uuid)?,
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// High-fidelity CQL type builders
// =========================================================================

/// Build an Arrow `Date32` array from `Value::Date(i32)`.
fn build_date32_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i32>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Date(days) => Some(*days),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(Date32Array::from(values)))
}

/// Build an Arrow `Time64(Nanosecond)` array from `Value::Time(i64)`.
fn build_time64_ns_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i64>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Time(nanos) => Some(*nanos),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(Time64NanosecondArray::from(values)))
}

/// Build an Arrow `Decimal128(38, DECIMAL_FIXED_SCALE)` array from
/// `Value::Decimal { scale, unscaled }`.
fn build_decimal128_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let mut builder = arrow::array::Decimal128Builder::new()
        .with_precision_and_scale(DECIMAL_MAX_PRECISION, DECIMAL_FIXED_SCALE as i8)?;

    for row in rows {
        match row.values.get(&col.name) {
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

/// Build an Arrow `Decimal128(38, 0)` array from `Value::Varint(Vec<u8>)`.
fn build_varint_as_decimal128_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    use num_bigint::BigInt;

    let mut builder = arrow::array::Decimal128Builder::new()
        .with_precision_and_scale(DECIMAL_MAX_PRECISION, 0)?;

    for row in rows {
        match row.values.get(&col.name) {
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
fn build_duration_utf8_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<String>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Duration { .. } => Some(ValueFormatter::format_value(v)),
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(StringArray::from(values)))
}

/// Build an Arrow `FixedSizeBinary(16)` array from `Value::Uuid([u8; 16])`.
fn build_uuid_fixed_binary_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
    for row in rows {
        match row.values.get(&col.name) {
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

/// Build an Arrow `Utf8` array from `Value::Inet(Vec<u8>)`.
fn build_inet_utf8_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<String>> = rows
        .iter()
        .map(|row| {
            row.values.get(&col.name).and_then(|v| match v {
                Value::Inet(bytes) => {
                    Some(ValueFormatter::format_value(&Value::Inet(bytes.clone())))
                }
                Value::Null => None,
                _ => None,
            })
        })
        .collect();
    Ok(Arc::new(StringArray::from(values)))
}

fn build_list_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    // For lists/sets, we serialize elements as strings for simplicity
    let mut offsets: Vec<i32> = vec![0];
    let mut values: Vec<Option<String>> = Vec::new();
    let mut null_bitmap: Vec<bool> = Vec::new();

    for row in rows {
        match row.values.get(&col.name) {
            Some(Value::List(items)) | Some(Value::Set(items)) => {
                null_bitmap.push(true);
                for item in items {
                    values.push(Some(ValueFormatter::format_value(item)));
                }
                offsets.push(values.len() as i32);
            }
            Some(Value::Null) | None => {
                null_bitmap.push(false);
                offsets.push(values.len() as i32);
            }
            _ => {
                null_bitmap.push(false);
                offsets.push(values.len() as i32);
            }
        }
    }

    let values_array = Arc::new(StringArray::from(values)) as ArrayRef;
    let field = Arc::new(Field::new("item", ArrowDataType::Utf8, true));
    let offset_buffer = OffsetBuffer::new(offsets.into());
    let null_buffer = NullBuffer::from(null_bitmap);

    Ok(Arc::new(ListArray::new(
        field,
        offset_buffer,
        values_array,
        Some(null_buffer),
    )))
}

fn build_map_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    // For maps, serialize key-value pairs as structs
    let mut offsets: Vec<i32> = vec![0];
    let mut keys: Vec<Option<String>> = Vec::new();
    let mut values: Vec<Option<String>> = Vec::new();
    let mut null_bitmap: Vec<bool> = Vec::new();

    for row in rows {
        match row.values.get(&col.name) {
            Some(Value::Map(pairs)) => {
                null_bitmap.push(true);
                for (k, v) in pairs {
                    keys.push(Some(ValueFormatter::format_value(k)));
                    values.push(Some(ValueFormatter::format_value(v)));
                }
                offsets.push(keys.len() as i32);
            }
            Some(Value::Null) | None => {
                null_bitmap.push(false);
                offsets.push(keys.len() as i32);
            }
            _ => {
                null_bitmap.push(false);
                offsets.push(keys.len() as i32);
            }
        }
    }

    // Build struct array for entries
    let key_array = Arc::new(StringArray::from(keys)) as ArrayRef;
    let value_array = Arc::new(StringArray::from(values)) as ArrayRef;

    let struct_fields = Fields::from(vec![
        Field::new("key", ArrowDataType::Utf8, false),
        Field::new("value", ArrowDataType::Utf8, true),
    ]);

    let entries_array =
        StructArray::new(struct_fields.clone(), vec![key_array, value_array], None);

    let map_field = Arc::new(Field::new(
        "entries",
        ArrowDataType::Struct(struct_fields),
        false,
    ));
    let offset_buffer = OffsetBuffer::new(offsets.into());
    let null_buffer = NullBuffer::from(null_bitmap);

    Ok(Arc::new(MapArray::new(
        map_field,
        offset_buffer,
        entries_array,
        Some(null_buffer),
        false,
    )))
}
