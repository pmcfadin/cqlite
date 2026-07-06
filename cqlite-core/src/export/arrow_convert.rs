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
//! | varint            | `Decimal128(38, 0)`                   | Err on >38-digit overflow (fail-closed, never Utf8) |
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

/// Convert an accumulated collection-element count to an Arrow 32-bit offset,
/// failing closed instead of silently wrapping.
///
/// Arrow `List`/`Map` offset buffers are `i32`-backed. A plain `usize as i32`
/// cast wraps to a **negative** value once the flattened element count of a
/// row group crosses `i32::MAX` (2,147,483,647) — exactly the wide-partition
/// case — producing non-monotonic offsets that either panic
/// `OffsetBuffer::new` (monotonicity assert, on a library data path) or yield
/// a structurally corrupt array. This returns
/// [`ArrowConvertError::InvalidValue`] at that boundary instead. See issue
/// #1486. Normal-size collections take the identical fast path.
#[inline]
fn checked_offset(len: usize) -> Result<i32, ArrowConvertError> {
    i32::try_from(len).map_err(|_| {
        ArrowConvertError::InvalidValue(format!(
            "collection offset {} exceeds i32::MAX ({}); Arrow List/Map offsets \
             are 32-bit — split the row group (fewer rows) or export via LargeList",
            len,
            i32::MAX
        ))
    })
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
            Some(Field::new(name, ArrowDataType::FixedSizeBinary(16), nullable).with_metadata(meta))
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
        CqlType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
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
            let arr: Vec<Option<String>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Text(s) => Ok(Some(s.clone())),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Text value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
            Ok(Arc::new(StringArray::from(arr)))
        }
        CqlType::Blob => {
            let byte_slices: Vec<Option<Vec<u8>>> = values
                .iter()
                .filter_map(|opt| {
                    let v = unwrap_frozen_value(*opt)?;
                    Some(match v {
                        Value::Blob(b) => Ok(Some(b.clone())),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Blob value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<Vec<u8>>>, ArrowConvertError>>()?;
            let refs: Vec<Option<&[u8]>> = byte_slices.iter().map(|o| o.as_deref()).collect();
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
                        Value::Inet(bytes) => Ok(Some(ValueFormatter::format_value(&Value::Inet(
                            bytes.clone(),
                        )))),
                        Value::Null => Ok(None),
                        other => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Inet value in element, got {:?}",
                            other
                        ))),
                    })
                })
                .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
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
                        offsets.push(checked_offset(flat_elements.len())?);
                    }
                    Some(Value::Null) | None => {
                        null_bitmap.push(false);
                        offsets.push(checked_offset(flat_elements.len())?);
                    }
                    Some(other) => {
                        return Err(ArrowConvertError::InvalidValue(format!(
                            "expected List/Set value, got {:?}",
                            other
                        )));
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
                                    "null key in map is not allowed in Arrow MapArray".to_string(),
                                ));
                            }
                            flat_keys.push(Some(k));
                            flat_vals.push(Some(val));
                        }
                        offsets.push(checked_offset(flat_keys.len())?);
                    }
                    Some(Value::Null) | None => {
                        null_bitmap.push(false);
                        offsets.push(checked_offset(flat_keys.len())?);
                    }
                    Some(other) => {
                        return Err(ArrowConvertError::InvalidValue(format!(
                            "expected Map value, got {:?}",
                            other
                        )));
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
                // Degenerate case: no fields → Utf8 fallback. Still fail closed
                // on a wrong top-level variant; only a Tuple (or null) is valid.
                let arr: Vec<Option<String>> = values
                    .iter()
                    .map(|opt| match unwrap_frozen_value(*opt) {
                        Some(Value::Null) | None => Ok(None),
                        Some(v @ Value::Tuple(_)) => Ok(Some(ValueFormatter::format_value(v))),
                        Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Tuple value, got {:?}",
                            other
                        ))),
                    })
                    .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
                return Ok(Arc::new(StringArray::from(arr)));
            }

            let n_rows = values.len();
            let n_fields = element_types.len();

            // Unwrap Frozen at the value level before inspecting tuples.
            let unwrapped: Vec<Option<&Value>> =
                values.iter().map(|opt| unwrap_frozen_value(*opt)).collect();

            // Fail closed: a non-null top-level value that is not a Tuple is a
            // type mismatch, not a null.  Mirror the scalar arms rather than
            // silently coercing the whole struct row's children to null.
            for v in unwrapped.iter() {
                match v {
                    Some(Value::Tuple(_)) | Some(Value::Null) | None => {}
                    Some(other) => {
                        return Err(ArrowConvertError::InvalidValue(format!(
                            "expected Tuple value, got {:?}",
                            other
                        )));
                    }
                }
            }

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
                            // Null/absent row → null sentinel (wrong variants
                            // already failed closed above).
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
                        Field::new(format!("field_{i}"), cql_type_to_arrow_data_type(t), true)
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
                // Degenerate case (incl. unresolved named UDTs, which carry an
                // empty field list): Utf8 fallback. Still fail closed on a wrong
                // top-level variant; only a Udt (or null) is valid.
                let arr: Vec<Option<String>> = values
                    .iter()
                    .map(|opt| match unwrap_frozen_value(*opt) {
                        Some(Value::Null) | None => Ok(None),
                        Some(v @ Value::Udt(_)) => Ok(Some(ValueFormatter::format_value(v))),
                        Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                            "expected Udt value, got {:?}",
                            other
                        ))),
                    })
                    .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
                return Ok(Arc::new(StringArray::from(arr)));
            }

            let n_rows = values.len();

            // Unwrap Frozen at the value level before inspecting UDTs.
            let unwrapped: Vec<Option<&Value>> =
                values.iter().map(|opt| unwrap_frozen_value(*opt)).collect();

            // Fail closed: a non-null top-level value that is not a Udt is a
            // type mismatch, not a null.  Mirror the scalar arms rather than
            // silently coercing the whole struct row's children to null.
            for v in unwrapped.iter() {
                match v {
                    Some(Value::Udt(_)) | Some(Value::Null) | None => {}
                    Some(other) => {
                        return Err(ArrowConvertError::InvalidValue(format!(
                            "expected Udt value, got {:?}",
                            other
                        )));
                    }
                }
            }

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
                        // Null/absent row → null sentinel (wrong variants
                        // already failed closed above).
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
        DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
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
            CqlType::Uuid | CqlType::TimeUuid => return build_uuid_fixed_binary_array(col, rows),
            CqlType::Inet => return build_inet_utf8_array(col, rows),
            CqlType::Counter => return build_int64_array(col, rows),
            // List, Set, Map, Tuple, and Udt: use the recursive typed builder.
            CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _) => {
                let column_values: Vec<Option<&Value>> = rows
                    .iter()
                    .map(|row| row.values.get(col.name.as_str()))
                    .collect();
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
/// - The input scale exceeds `DECIMAL_FIXED_SCALE` (would require truncation /
///   silent precision loss — fail closed instead of divide-and-truncate).
/// - The rescaled magnitude exceeds 38 decimal digits (overflow of `Decimal128`).
/// - Checked multiplication overflows `i128` when scaling up.
///
/// Follow-up option (not implemented here per owner decision 2026-07-01): derive
/// a per-column target scale from schema / `Statistics.db` metadata so that
/// higher-scale decimals can be represented without loss instead of erroring.
pub(crate) fn rescale_decimal(scale: i32, unscaled: &[u8]) -> Result<i128, ArrowConvertError> {
    use num_bigint::BigInt;

    if unscaled.is_empty() {
        return Ok(0i128);
    }

    // Fail closed: a scale greater than the fixed target scale can only be
    // reconciled by dividing (truncating toward zero), which silently drops
    // precision from an authoritative export. Error instead — mirror the
    // over-magnitude guard below rather than truncate.
    if scale > DECIMAL_FIXED_SCALE {
        return Err(ArrowConvertError::InvalidValue(format!(
            "decimal scale {scale} exceeds the fixed export scale {DECIMAL_FIXED_SCALE}; \
             refusing to truncate (would lose precision)"
        )));
    }

    // Decode big-endian two's-complement signed integer.
    let bigint = BigInt::from_signed_bytes_be(unscaled);

    // Compute scale delta: positive means we must multiply (scale up).
    // A negative delta (scale > DECIMAL_FIXED_SCALE) is rejected above.
    let delta = DECIMAL_FIXED_SCALE - scale;

    let rescaled = if delta == 0 {
        bigint
    } else {
        // Scale up: multiply by 10^delta.
        let factor = BigInt::from(10i64).pow(delta as u32);
        bigint * factor
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

fn build_boolean_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<bool>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Boolean(b)) => Ok(Some(*b)),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Boolean value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<bool>>, ArrowConvertError>>()?;
    Ok(Arc::new(BooleanArray::from(values)))
}

fn build_int8_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i8>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::TinyInt(i)) => Ok(Some(*i)),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected TinyInt value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<i8>>, ArrowConvertError>>()?;
    Ok(Arc::new(Int8Array::from(values)))
}

fn build_int16_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i16>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::SmallInt(i)) => Ok(Some(*i)),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected SmallInt value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<i16>>, ArrowConvertError>>()?;
    Ok(Arc::new(Int16Array::from(values)))
}

fn build_int32_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    // The same-width `Date`→i32 acceptance is only valid on the OPAQUE path
    // (`cql_type = None`): an authoritative `date` column routes to
    // `build_date32_array`, so an authoritative `int` column carrying a `Date`
    // is a genuine mismatch that must fail closed.
    let allow_compat = col.cql_type.is_none();
    let values: Vec<Option<i32>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Integer(i)) => Ok(Some(*i)),
                Some(Value::Date(d)) if allow_compat => Ok(Some(*d)), // Date is stored as i32 days
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Int value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<i32>>, ArrowConvertError>>()?;
    Ok(Arc::new(Int32Array::from(values)))
}

fn build_int64_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    // `build_int64_array` backs authoritative `bigint` and `counter` columns
    // plus the opaque (`cql_type = None`) path. `Counter` is legitimate for a
    // `counter` column; the same-width `Time`→i64 acceptance and cross-accepting
    // `Counter` for a `bigint` column are only valid on the opaque path (an
    // authoritative `time` column routes to `build_time64_ns_array`).
    let effective = col.cql_type.as_ref().map(unwrap_frozen_type);
    let allow_counter = matches!(effective, None | Some(CqlType::Counter));
    let allow_compat = effective.is_none();
    let values: Vec<Option<i64>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::BigInt(i)) => Ok(Some(*i)),
                Some(Value::Counter(c)) if allow_counter => Ok(Some(*c)),
                Some(Value::Time(t)) if allow_compat => Ok(Some(*t)), // Time is stored as i64 nanos
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected BigInt value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
    Ok(Arc::new(Int64Array::from(values)))
}

fn build_float32_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<f32>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
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
            },
        )
        .collect::<Result<Vec<Option<f32>>, ArrowConvertError>>()?;
    Ok(Arc::new(Float32Array::from(values)))
}

fn build_float64_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<f64>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Float(f)) => Ok(Some(*f)),
                Some(Value::Float32(f)) => Ok(Some(*f as f64)),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Double value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<f64>>, ArrowConvertError>>()?;
    Ok(Arc::new(Float64Array::from(values)))
}

fn build_string_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    // When the schema carries an AUTHORITATIVE text type, fail closed on a
    // wrong-variant value (mirroring the other scalar builders) rather than
    // silently string-formatting it. For `cql_type = None` or opaque types
    // (e.g. `Custom`) this stays the permissive Utf8 fallback: those columns
    // have no authoritative type to validate against.
    let strict_text = matches!(
        col.cql_type.as_ref().map(unwrap_frozen_type),
        Some(CqlType::Text | CqlType::Ascii | CqlType::Varchar)
    );
    let values: Vec<Option<String>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Null) => Ok(None),
                Some(Value::Text(s)) => Ok(Some(s.clone())),
                // `Json` is only a valid string source on the opaque fallback;
                // an authoritative text column must fail closed on it.
                Some(Value::Json(j)) if !strict_text => Ok(Some(j.to_string())),
                Some(other) if strict_text => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Text value, got {:?}",
                    col.name, other
                ))),
                // Opaque / untyped fallback: format complex types as strings.
                Some(other) => Ok(Some(ValueFormatter::format_value(other))),
            },
        )
        .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
    Ok(Arc::new(StringArray::from(values)))
}

fn build_binary_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<&[u8]>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Blob(b)) => Ok(Some(b.as_slice())),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Blob value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<&[u8]>>, ArrowConvertError>>()?;
    Ok(Arc::new(BinaryArray::from(values)))
}

fn build_timestamp_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i64>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Timestamp(ts)) => Ok(Some(*ts)),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Timestamp value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
    Ok(Arc::new(
        TimestampMillisecondArray::from(values).with_timezone("UTC"),
    ))
}

fn build_uuid_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<[u8; 16]>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Uuid(uuid)) => Ok(Some(*uuid)),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Uuid value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<[u8; 16]>>, ArrowConvertError>>()?;

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
fn build_date32_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i32>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Date(days)) => Ok(Some(*days)),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Date value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<i32>>, ArrowConvertError>>()?;
    Ok(Arc::new(Date32Array::from(values)))
}

/// Build an Arrow `Time64(Nanosecond)` array from `Value::Time(i64)`.
fn build_time64_ns_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let values: Vec<Option<i64>> = rows
        .iter()
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Time(nanos)) => Ok(Some(*nanos)),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Time value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<i64>>, ArrowConvertError>>()?;
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
        match unwrap_frozen_value(row.values.get(col.name.as_str())) {
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
        match unwrap_frozen_value(row.values.get(col.name.as_str())) {
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
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(v @ Value::Duration { .. }) => Ok(Some(ValueFormatter::format_value(v))),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Duration value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
    Ok(Arc::new(StringArray::from(values)))
}

/// Build an Arrow `FixedSizeBinary(16)` array from `Value::Uuid([u8; 16])`.
fn build_uuid_fixed_binary_array(
    col: &ColumnInfo,
    rows: &[QueryRow],
) -> Result<ArrayRef, ArrowConvertError> {
    let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
    for row in rows {
        match unwrap_frozen_value(row.values.get(col.name.as_str())) {
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
        .map(
            |row| match unwrap_frozen_value(row.values.get(col.name.as_str())) {
                None => Ok(None),
                Some(Value::Inet(bytes)) => Ok(Some(ValueFormatter::format_value(&Value::Inet(
                    bytes.clone(),
                )))),
                Some(Value::Null) => Ok(None),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Inet value, got {:?}",
                    col.name, other
                ))),
            },
        )
        .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
    Ok(Arc::new(StringArray::from(values)))
}

fn build_list_array(col: &ColumnInfo, rows: &[QueryRow]) -> Result<ArrayRef, ArrowConvertError> {
    // For lists/sets, we serialize elements as strings for simplicity
    let mut offsets: Vec<i32> = vec![0];
    let mut values: Vec<Option<String>> = Vec::new();
    let mut null_bitmap: Vec<bool> = Vec::new();

    for row in rows {
        match unwrap_frozen_value(row.values.get(col.name.as_str())) {
            Some(Value::List(items)) | Some(Value::Set(items)) => {
                null_bitmap.push(true);
                for item in items {
                    values.push(Some(ValueFormatter::format_value(item)));
                }
                offsets.push(checked_offset(values.len())?);
            }
            Some(Value::Null) | None => {
                null_bitmap.push(false);
                offsets.push(checked_offset(values.len())?);
            }
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected List/Set value, got {:?}",
                    col.name, other
                )));
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
        match unwrap_frozen_value(row.values.get(col.name.as_str())) {
            Some(Value::Map(pairs)) => {
                null_bitmap.push(true);
                for (k, v) in pairs {
                    keys.push(Some(ValueFormatter::format_value(k)));
                    values.push(Some(ValueFormatter::format_value(v)));
                }
                offsets.push(checked_offset(keys.len())?);
            }
            Some(Value::Null) | None => {
                null_bitmap.push(false);
                offsets.push(checked_offset(keys.len())?);
            }
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Map value, got {:?}",
                    col.name, other
                )));
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

    let entries_array = StructArray::new(struct_fields.clone(), vec![key_array, value_array], None);

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

// =========================================================================
// Tests — fail-closed Value→Arrow conversion (Issue #1485)
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{ColumnInfo, QueryRow};
    use crate::schema::CqlType;
    use crate::types::{DataType, Value};
    use crate::RowKey;
    use arrow::array::{Array, Int32Array};

    /// Build a `ColumnInfo` for a single test column.
    fn col(name: &str, data_type: DataType, cql_type: Option<CqlType>) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
            position: 0,
            table_name: None,
            cql_type,
        }
    }

    /// Build a `QueryRow` from a single (column, value) pair.
    fn row_one(name: &str, value: Value) -> QueryRow {
        let mut values: HashMap<Arc<str>, Value> = HashMap::new();
        values.insert(name.into(), value);
        QueryRow {
            values,
            key: RowKey::new(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        }
    }

    /// An empty row: the column is absent entirely.
    fn row_absent() -> QueryRow {
        QueryRow {
            values: HashMap::new(),
            key: RowKey::new(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        }
    }

    fn is_invalid_value(res: Result<arrow::record_batch::RecordBatch, ArrowConvertError>) -> bool {
        matches!(res, Err(ArrowConvertError::InvalidValue(_)))
    }

    /// (1) Typed high-fidelity scalar builder (`build_date32_array`): a
    /// type-mismatched value must FAIL CLOSED rather than silently become NULL.
    #[test]
    fn typed_scalar_type_mismatch_is_error() {
        let columns = vec![col("d", DataType::Timestamp, Some(CqlType::Date))];
        let rows = vec![row_one("d", Value::Text("not-a-date".into()))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (2) Flat `data_type` builder path (`build_int32_array`, `cql_type = None`):
    /// a type-mismatched value must FAIL CLOSED.
    #[test]
    fn flat_builder_type_mismatch_is_error() {
        let columns = vec![col("n", DataType::Integer, None)];
        let rows = vec![row_one("n", Value::Text("nope".into()))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (3a) Collection path (`build_typed_value_array` List arm): a scalar where
    /// a list is expected must FAIL CLOSED.
    #[test]
    fn collection_expected_list_got_scalar_is_error() {
        let columns = vec![col(
            "l",
            DataType::List,
            Some(CqlType::List(Box::new(CqlType::Int))),
        )];
        let rows = vec![row_one("l", Value::Integer(5))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (3b) Collection element dispatch (Pattern A scalar arm reached via list
    /// recursion): a mistyped element inside a well-formed list must FAIL CLOSED.
    #[test]
    fn collection_mistyped_element_is_error() {
        let columns = vec![col(
            "l",
            DataType::List,
            Some(CqlType::List(Box::new(CqlType::Int))),
        )];
        let rows = vec![row_one(
            "l",
            Value::List(vec![Value::Integer(1), Value::Text("bad".into())]),
        )];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (3c) Map path (`build_typed_value_array` Map arm): a scalar where a map is
    /// expected must FAIL CLOSED.
    #[test]
    fn collection_expected_map_got_scalar_is_error() {
        let columns = vec![col(
            "m",
            DataType::Map,
            Some(CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::Int),
            )),
        )];
        let rows = vec![row_one("m", Value::Integer(7))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (4) Regression guard: `Value::Null` and an ABSENT column must STILL map to
    /// proper Arrow nulls — never an error.
    #[test]
    fn null_and_absent_still_build_ok() {
        let columns = vec![col("n", DataType::Integer, None)];
        let rows = vec![row_one("n", Value::Null), row_absent()];
        let batch = rows_to_record_batch(&columns, &rows).expect("null/absent must build");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(0).null_count(), 2);
    }

    /// (5) Happy path: a correctly-typed value converts cleanly.
    #[test]
    fn correctly_typed_value_builds_ok() {
        let columns = vec![col("n", DataType::Integer, None)];
        let rows = vec![row_one("n", Value::Integer(42))];
        let batch = rows_to_record_batch(&columns, &rows).expect("well-typed value must build");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        assert_eq!(arr.value(0), 42);
        assert_eq!(arr.null_count(), 0);
    }

    /// (5b) Fail-closed (issue #1487): a `decimal` with scale > `DECIMAL_FIXED_SCALE`
    /// (here scale 12) must return an error rather than silently truncating toward
    /// zero. On the pre-fix code path this scaled down and succeeded lossily.
    #[test]
    fn decimal_scale_above_fixed_is_error() {
        let columns = vec![col("d", DataType::Blob, Some(CqlType::Decimal))];
        // 123456789012 with scale 12 == 0.123456789012 — 12 fractional digits.
        let unscaled = num_bigint::BigInt::from(123_456_789_012i64).to_signed_bytes_be();
        let rows = vec![row_one(
            "d",
            Value::Decimal {
                scale: 12,
                unscaled,
            },
        )];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (5c) Happy path (issue #1487): an in-range `decimal` (scale <= 9) still
    /// converts exactly as before.
    #[test]
    fn decimal_scale_within_fixed_builds_ok() {
        use arrow::array::Decimal128Array;
        let columns = vec![col("d", DataType::Blob, Some(CqlType::Decimal))];
        // 123456 with scale 3 == 123.456 — rescaled to scale 9 -> 123_456_000_000.
        let unscaled = num_bigint::BigInt::from(123_456i64).to_signed_bytes_be();
        let rows = vec![row_one("d", Value::Decimal { scale: 3, unscaled })];
        let batch = rows_to_record_batch(&columns, &rows).expect("in-range decimal must build");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128Array");
        assert_eq!(arr.value(0), 123_456_000_000i128);
        assert_eq!(arr.null_count(), 0);
    }

    /// (5d) Regression guard (issue #1487): a NULL / absent decimal stays NULL —
    /// the fail-closed scale check must not disturb the null path.
    #[test]
    fn decimal_null_and_absent_still_null() {
        let columns = vec![col("d", DataType::Blob, Some(CqlType::Decimal))];
        let rows = vec![row_one("d", Value::Null), row_absent()];
        let batch = rows_to_record_batch(&columns, &rows).expect("null/absent decimal must build");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(0).null_count(), 2);
    }

    /// (6) Regression: a CQL `float` (32-bit) column whose value is carried as
    /// the wider `Value::Float` (f64) by the decode path must convert (narrowed
    /// to f32), NOT be rejected as a type mismatch. Real data (e.g. a `height`
    /// float column) surfaces `Value::Float`; the old silent `_ => None`
    /// dropped it to NULL. Covers both the typed and flat float32 arms.
    #[test]
    fn float32_column_accepts_wide_float_value() {
        // Flat path (cql_type = None -> build_float32_array).
        let flat = vec![col("h", DataType::Float32, None)];
        let rows = vec![row_one("h", Value::Float(1.84f32 as f64))];
        let batch = rows_to_record_batch(&flat, &rows).expect("wide float must narrow, not error");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32Array");
        assert_eq!(arr.value(0), 1.84f32);
        assert_eq!(arr.null_count(), 0);

        // Typed high-fidelity path (CqlType::Float -> build_typed_value_array).
        let typed = vec![col("h", DataType::Float32, Some(CqlType::Float))];
        let rows = vec![row_one("h", Value::Float(1.84f32 as f64))];
        let batch =
            rows_to_record_batch(&typed, &rows).expect("wide float (typed) must narrow, not error");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32Array");
        assert_eq!(arr.value(0), 1.84f32);
    }

    /// (7a) Tuple arm: a non-`Tuple` top-level value in a tuple column must FAIL
    /// CLOSED, not silently become a struct row of null children.
    #[test]
    fn tuple_expected_tuple_got_scalar_is_error() {
        let columns = vec![col(
            "t",
            DataType::Text,
            Some(CqlType::Tuple(vec![CqlType::Int, CqlType::Text])),
        )];
        let rows = vec![row_one("t", Value::Text("not-a-tuple".into()))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (7b) Tuple arm regression: `Value::Null` and an ABSENT tuple column must
    /// STILL build (as struct nulls), never error.
    #[test]
    fn tuple_null_and_absent_still_build_ok() {
        let columns = vec![col(
            "t",
            DataType::Text,
            Some(CqlType::Tuple(vec![CqlType::Int, CqlType::Text])),
        )];
        let rows = vec![row_one("t", Value::Null), row_absent()];
        let batch = rows_to_record_batch(&columns, &rows).expect("null/absent tuple must build");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(0).null_count(), 2);
    }

    /// (8a) UDT arm: a non-`Udt` top-level value in a UDT column must FAIL
    /// CLOSED, not silently become a struct row of null children.
    #[test]
    fn udt_expected_udt_got_scalar_is_error() {
        let columns = vec![col(
            "u",
            DataType::Text,
            Some(CqlType::Udt(
                "my_type".into(),
                vec![("a".into(), CqlType::Int), ("b".into(), CqlType::Text)],
            )),
        )];
        let rows = vec![row_one("u", Value::Integer(9))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (8b) UDT arm regression: `Value::Null` and an ABSENT UDT column must STILL
    /// build (as struct nulls), never error.
    #[test]
    fn udt_null_and_absent_still_build_ok() {
        let columns = vec![col(
            "u",
            DataType::Text,
            Some(CqlType::Udt(
                "my_type".into(),
                vec![("a".into(), CqlType::Int), ("b".into(), CqlType::Text)],
            )),
        )];
        let rows = vec![row_one("u", Value::Null), row_absent()];
        let batch = rows_to_record_batch(&columns, &rows).expect("null/absent UDT must build");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(0).null_count(), 2);
    }

    /// (8c) UDT degenerate/empty-field arm (also how UNRESOLVED named UDTs are
    /// represented): a non-`Udt` scalar must still FAIL CLOSED, not silently
    /// serialize as UTF-8.
    #[test]
    fn empty_field_udt_expected_udt_got_scalar_is_error() {
        let columns = vec![col(
            "u",
            DataType::Text,
            Some(CqlType::Udt("unresolved".into(), vec![])),
        )];
        let rows = vec![row_one("u", Value::Integer(9))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (7c) Tuple degenerate/empty-field arm: a non-`Tuple` scalar must still
    /// FAIL CLOSED, not silently serialize as UTF-8.
    #[test]
    fn empty_field_tuple_expected_tuple_got_scalar_is_error() {
        let columns = vec![col("t", DataType::Text, Some(CqlType::Tuple(vec![])))];
        let rows = vec![row_one("t", Value::Text("nope".into()))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (9a) Authoritative text column: a non-`Text` value must FAIL CLOSED via
    /// the strict typed builder, not be silently string-formatted by the flat
    /// `build_string_array`.
    #[test]
    fn authoritative_text_column_type_mismatch_is_error() {
        for cql in [CqlType::Text, CqlType::Ascii, CqlType::Varchar] {
            let columns = vec![col("s", DataType::Text, Some(cql))];
            let rows = vec![row_one("s", Value::Integer(1))];
            assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
        }
    }

    /// (9c) Authoritative text column: a `Value::Json` must FAIL CLOSED (the JSON
    /// stringification is only valid on the opaque fallback).
    #[test]
    fn authoritative_text_column_rejects_json() {
        let columns = vec![col("s", DataType::Text, Some(CqlType::Text))];
        let rows = vec![row_one("s", Value::Json(serde_json::json!({"a": 1})))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (9d) Frozen-wrapped valid values must NOT be rejected: `frozen<text>`
    /// with `Value::Frozen(Value::Text(..))` builds, and a high-fidelity
    /// `frozen<date>` with `Value::Frozen(Value::Date(..))` builds.
    #[test]
    fn frozen_wrapped_scalar_values_build_ok() {
        // frozen<text> via the flat string builder.
        let text_cols = vec![col(
            "s",
            DataType::Text,
            Some(CqlType::Frozen(Box::new(CqlType::Text))),
        )];
        let text_rows = vec![row_one(
            "s",
            Value::Frozen(Box::new(Value::Text("hi".into()))),
        )];
        let batch =
            rows_to_record_batch(&text_cols, &text_rows).expect("frozen<text> value must build");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(arr.value(0), "hi");

        // frozen<date> via the high-fidelity date builder.
        let date_cols = vec![col(
            "d",
            DataType::Integer,
            Some(CqlType::Frozen(Box::new(CqlType::Date))),
        )];
        let date_rows = vec![row_one("d", Value::Frozen(Box::new(Value::Date(19_000))))];
        let batch =
            rows_to_record_batch(&date_cols, &date_rows).expect("frozen<date> value must build");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).null_count(), 0);
    }

    /// (9b) Authoritative text column happy path + nulls: a correct `Value::Text`
    /// converts cleanly and null/absent stay null.
    #[test]
    fn authoritative_text_column_builds_ok() {
        let columns = vec![col("s", DataType::Text, Some(CqlType::Text))];
        let rows = vec![
            row_one("s", Value::Text("hi".into())),
            row_one("s", Value::Null),
            row_absent(),
        ];
        let batch = rows_to_record_batch(&columns, &rows).expect("well-typed text must build");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(arr.value(0), "hi");
        assert_eq!(arr.null_count(), 2);
    }

    /// (10a) Authoritative `int` column: a `Value::Date` (same-width i32) must
    /// FAIL CLOSED — the `date`→i32 acceptance is only for the opaque path.
    #[test]
    fn authoritative_int_column_rejects_date() {
        let columns = vec![col("n", DataType::Integer, Some(CqlType::Int))];
        let rows = vec![row_one("n", Value::Date(19_000))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }

    /// (10b) Opaque (`cql_type = None`) int column: the `Date`→i32 same-width
    /// acceptance is preserved (no authoritative type to validate against).
    #[test]
    fn opaque_int_column_accepts_date() {
        let columns = vec![col("n", DataType::Integer, None)];
        let rows = vec![row_one("n", Value::Date(19_000))];
        let batch = rows_to_record_batch(&columns, &rows).expect("opaque int accepts Date");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        assert_eq!(arr.value(0), 19_000);
    }

    /// (10c) Authoritative `bigint` / `counter` columns: a `Value::Time`
    /// (same-width i64) must FAIL CLOSED; `Value::Counter` in a `bigint` column
    /// must also FAIL CLOSED.
    #[test]
    fn authoritative_bigint_counter_reject_mismatch() {
        let bigint_time = vec![col("b", DataType::BigInt, Some(CqlType::BigInt))];
        assert!(is_invalid_value(rows_to_record_batch(
            &bigint_time,
            &[row_one("b", Value::Time(123))]
        )));

        let counter_time = vec![col("c", DataType::BigInt, Some(CqlType::Counter))];
        assert!(is_invalid_value(rows_to_record_batch(
            &counter_time,
            &[row_one("c", Value::Time(123))]
        )));

        let bigint_counter = vec![col("b", DataType::BigInt, Some(CqlType::BigInt))];
        assert!(is_invalid_value(rows_to_record_batch(
            &bigint_counter,
            &[row_one("b", Value::Counter(7))]
        )));
    }

    /// (10d) Authoritative `counter` column happy path: `Value::Counter` builds.
    #[test]
    fn authoritative_counter_column_accepts_counter() {
        let columns = vec![col("c", DataType::BigInt, Some(CqlType::Counter))];
        let rows = vec![row_one("c", Value::Counter(42))];
        let batch = rows_to_record_batch(&columns, &rows).expect("counter accepts Counter");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).null_count(), 0);
    }

    /// (11) Issue #1486: `checked_offset` at the `i32::MAX` boundary fails
    /// closed instead of wrapping negative. Materializing >2^31 real elements
    /// is infeasible, so we drive the offset-building helper directly — the
    /// exact path every List/Map offset push now goes through. On main the
    /// sites used `len() as i32`, which wraps to a negative offset (no `Err`);
    /// this asserts the boundary now returns `Err`.
    #[test]
    fn checked_offset_past_i32_max_is_error() {
        // At the ceiling: i32::MAX still fits.
        assert_eq!(
            super::checked_offset(i32::MAX as usize).ok(),
            Some(i32::MAX)
        );
        // One past the ceiling must fail closed (would wrap to i32::MIN as i32).
        assert!(matches!(
            super::checked_offset(i32::MAX as usize + 1),
            Err(ArrowConvertError::InvalidValue(_))
        ));
    }

    /// (11b) Normal-size collections behave identically: small counts map
    /// straight through to their `i32` value.
    #[test]
    fn checked_offset_normal_sizes_are_identity() {
        assert_eq!(super::checked_offset(0).ok(), Some(0));
        assert_eq!(super::checked_offset(1).ok(), Some(1));
        assert_eq!(super::checked_offset(1_000_000).ok(), Some(1_000_000));
    }

    /// (11c) End-to-end regression guard: a real, normal-size List/Map still
    /// builds unchanged through the checked offset path.
    #[test]
    fn normal_collections_still_build_through_checked_offsets() {
        let list_cols = vec![col(
            "l",
            DataType::List,
            Some(CqlType::List(Box::new(CqlType::Int))),
        )];
        let list_rows = vec![
            row_one("l", Value::List(vec![Value::Integer(1), Value::Integer(2)])),
            row_one("l", Value::Null),
        ];
        let batch = rows_to_record_batch(&list_cols, &list_rows).expect("list must build");
        assert_eq!(batch.num_rows(), 2);

        let map_cols = vec![col(
            "m",
            DataType::Map,
            Some(CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::Int),
            )),
        )];
        let map_rows = vec![row_one(
            "m",
            Value::Map(vec![(Value::Text("k".into()), Value::Integer(9))]),
        )];
        let batch = rows_to_record_batch(&map_cols, &map_rows).expect("map must build");
        assert_eq!(batch.num_rows(), 1);
    }
}
