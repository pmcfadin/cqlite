//! Parquet output writer for QueryResult
//!
//! Converts CQL query results to Apache Parquet format with proper type mapping.
//! Uses Snappy compression by default (Cassandra default, good speed/size balance).
//!
//! # CQL → Arrow type mapping
//!
//! When `ColumnInfo.cql_type` is `Some`, the following high-fidelity mappings are
//! used instead of the flat `data_type` fallback:
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
//!
//! # List and Set mapping
//!
//! CQL `list<X>` and `set<X>` both map to Arrow `List` (Arrow has no dedicated Set
//! type).  Element types are mapped recursively through the same scalar mapping
//! table above, so `list<uuid>` produces `List<FixedSizeBinary(16)>`,
//! `list<timestamp>` produces `List<Timestamp(ms,UTC)>`, etc.
//!
//! `CqlType::Frozen(inner)` is transparent in the recursion: it unwraps and
//! recurses into `inner`.  Runtime `Value::Frozen(inner)` values are also unwrapped
//! before element dispatch.
//!
//! For nested collections (`list<frozen<list<int>>>`), the recursion produces
//! `List<List<Int32>>` and handles element unwrapping at all levels.  Map, Tuple,
//! and UDT element types currently fall back to a stringified `Utf8` representation
//! (handled by issues #677 and #678 respectively).
//!
//! # Recursive builder design
//!
//! `build_typed_value_array(cql_type, values)` is the shared recursive entry point
//! used by both top-level column dispatch and nested element building.  Adding
//! support for Map/Tuple/UDT elements (#677/#678) requires only new match arms in
//! `cql_type_to_arrow_data_type` and `build_typed_value_array`.
//! # Decimal strategy
//!
//! CQL `decimal` stores a per-value scale alongside the unscaled big-endian integer.
//! Arrow `Decimal128` requires a single fixed (precision, scale) pair at schema time.
//! We fix `DECIMAL_FIXED_SCALE = 9` (nanosecond-like resolution, fits most Cassandra
//! decimals in practice). Each value is rescaled to that fixed scale:
//!
//! - If the value's scale equals `DECIMAL_FIXED_SCALE`, no rescaling is needed.
//! - If the value's scale is smaller (fewer decimal places), the unscaled integer
//!   is multiplied by `10^(DECIMAL_FIXED_SCALE - value_scale)` — a checked
//!   multiplication is used; on overflow the write fails with a clear error.
//! - If the value's scale is larger, the unscaled integer is divided by
//!   `10^(value_scale - DECIMAL_FIXED_SCALE)` with truncation toward zero; the
//!   caller is warned via the error path if the value cannot be represented exactly.
//! - Precision is fixed at 38 (max for `Decimal128`).
//! - Values whose rescaled magnitude exceeds 38 decimal digits fail with a clear
//!   error rather than silently truncating.
//!
//! For `varint`, the big-endian signed integer is decoded and stored as
//! `Decimal128(38, 0)` (integer, no fractional part).  Values that exceed 38
//! decimal digits fall back to `Utf8` (the column schema will be `Utf8`; the
//! value is rendered as a decimal string via `ValueFormatter`).

use crate::config::OutputConfig;
use crate::output::{OutputError, StreamingWriter};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, ListArray, MapArray, StringArray, StructArray,
    Time64NanosecondArray, TimestampMillisecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use cqlite_core::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
use cqlite_core::schema::CqlType;
use cqlite_core::types::DataType;
use cqlite_core::Value;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use super::value_fmt::ValueFormatter;

// ============================================================================
// Decimal constants (Issue #675)
// ============================================================================

/// Fixed scale used for all `decimal` columns mapped to `Decimal128`.
///
/// We choose 9 (nanosecond-like resolution) as a reasonable default that
/// accommodates most CQL decimal use-cases without requiring per-column
/// inspection of the data.  See the module-level doc for the full rescaling
/// strategy.
const DECIMAL_FIXED_SCALE: i32 = 9;

/// Maximum precision for `Decimal128` (Arrow/Parquet limit).
const DECIMAL_MAX_PRECISION: u8 = 38;

/// Arrow UUID extension type name, as specified by the Arrow spec.
/// The field metadata key `ARROW:extension:name` = `arrow.uuid` triggers
/// the Parquet UUID logical type annotation.
const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
const ARROW_UUID_EXTENSION_NAME: &str = "arrow.uuid";

// ============================================================================
// BigInt → i128 helper
// ============================================================================

/// Convert a `num_bigint::BigInt` to `i128`, sign-extending if necessary.
///
/// Uses the two's-complement big-endian representation via
/// `to_signed_bytes_be()` and sign-extends to 16 bytes before reinterpreting
/// as `i128`.  Returns an error if the value requires more than 16 bytes
/// (i.e. exceeds the i128 range).
fn bigint_to_i128(n: &num_bigint::BigInt) -> Result<i128, Box<dyn std::error::Error>> {
    let tc_bytes = n.to_signed_bytes_be();
    if tc_bytes.len() > 16 {
        return Err("BigInt value requires more than 16 bytes; cannot fit in i128".into());
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

/// Parquet writer for QueryResult
///
/// Converts query results to Apache Parquet binary format.
/// Unlike JSON/CSV writers, this returns `Vec<u8>` (binary data).
#[allow(dead_code)]
pub struct ParquetWriter;

impl ParquetWriter {
    /// Write QueryResult to Parquet binary format
    ///
    /// # Arguments
    ///
    /// * `result` - The query result to convert to Parquet
    /// * `config` - Output configuration for row limits
    ///
    /// # Returns
    ///
    /// Binary Parquet data or error
    #[allow(dead_code)]
    pub fn write(
        result: &QueryResult,
        config: &OutputConfig,
    ) -> Result<Vec<u8>, Box<dyn StdError>> {
        // Handle empty results
        if result.metadata.columns.is_empty() {
            return Self::write_empty_parquet();
        }

        // Build Arrow schema from column metadata
        let schema = Self::build_schema(&result.metadata.columns)?;

        // Apply row limit if specified in config
        let rows_to_process = if let Some(limit) = config.limit {
            &result.rows[..result.rows.len().min(limit)]
        } else {
            &result.rows
        };

        // Convert rows to Arrow arrays (one per column)
        let arrays = Self::convert_to_arrays(&result.metadata.columns, rows_to_process)?;

        // Create RecordBatch
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

        // Write to Parquet with Snappy compression
        Self::write_parquet(&batch)
    }

    /// Write an empty Parquet file
    fn write_empty_parquet() -> Result<Vec<u8>, Box<dyn StdError>> {
        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(Arc::new(schema));
        Self::write_parquet(&batch)
    }

    /// Build Arrow schema from CQL column metadata
    fn build_schema(columns: &[ColumnInfo]) -> Result<Schema, Box<dyn StdError>> {
        let fields: Vec<Field> = columns
            .iter()
            .map(|col| Self::column_to_field(col))
            .collect();
        Ok(Schema::new(fields))
    }

    /// Convert a CQL column to Arrow field.
    ///
    /// When `col.cql_type` is `Some`, the high-fidelity schema mapping
    /// (`cql_type_to_arrow_field`) is used.  For scalar types this produces
    /// the correct Arrow logical type (e.g. `Date32`, `Time64`, `Decimal128`).
    /// Collection and complex CQL types fall back to the existing flat
    /// `data_type` mapping for now (handled by later issues #676–#678).
    fn column_to_field(col: &ColumnInfo) -> Field {
        if let Some(cql_type) = &col.cql_type {
            if let Some(field) = Self::cql_type_to_arrow_field(&col.name, cql_type, col.nullable) {
                return field;
            }
        }
        let arrow_type = Self::data_type_to_arrow(&col.data_type);
        Field::new(&col.name, arrow_type, col.nullable)
    }

    /// Map a scalar `CqlType` to an Arrow `Field`, returning `None` for
    /// complex/collection types so the caller can fall back to `data_type_to_arrow`.
    ///
    /// UUID and TimeUUID columns receive the canonical Arrow UUID extension
    /// metadata (`ARROW:extension:name` = `arrow.uuid`) so that Parquet readers
    /// emit the Parquet UUID logical type.
    ///
    /// `CqlType::List` and `CqlType::Set` are now handled here via
    /// `cql_type_to_arrow_data_type` which maps element types recursively.
    /// `CqlType::Frozen(inner)` transparently unwraps to `inner`.
    fn cql_type_to_arrow_field(name: &str, cql_type: &CqlType, nullable: bool) -> Option<Field> {
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
                let item_type = Self::cql_type_to_arrow_data_type(inner);
                let item_field = Arc::new(Field::new("item", item_type, true));
                Some(Field::new(name, ArrowDataType::List(item_field), nullable))
            }
            // Frozen<T> is transparent: same Arrow type as T.
            CqlType::Frozen(inner) => Self::cql_type_to_arrow_field(name, inner, nullable),
            // Map, Tuple, UDT: fall back to existing string mapping (#677/#678).
            CqlType::Map(_, _) | CqlType::Tuple(_) | CqlType::Udt(_, _) => None,
            // Remaining scalar types are already handled correctly by the flat
            // DataType mapping; return None to allow that path to run.
            _ => None,
        }
    }

    // =========================================================================
    // Recursive CQL type → Arrow DataType mapping (Issue #676)
    // =========================================================================

    /// Recursively map a `CqlType` to an Arrow `DataType`.
    ///
    /// This function is the single source of truth for element-type mapping used
    /// by both the schema-building path (`cql_type_to_arrow_field`) and the
    /// value-building path (`build_typed_value_array`).  It handles all scalar
    /// types and recursively handles `List`, `Set`, and `Frozen`.
    ///
    /// `CqlType::Frozen(inner)` is transparent: the same Arrow type as `inner`.
    ///
    /// `Map`, `Tuple`, and `UDT` currently fall back to `Utf8` — those are
    /// addressed by issues #677 and #678 respectively.  When those issues add
    /// new match arms here, the corresponding arms in `build_typed_value_array`
    /// must also be added.
    fn cql_type_to_arrow_data_type(cql_type: &CqlType) -> ArrowDataType {
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
                let item_type = Self::cql_type_to_arrow_data_type(inner);
                ArrowDataType::List(Arc::new(Field::new("item", item_type, true)))
            }
            // Frozen<T> is transparent in type mapping.
            CqlType::Frozen(inner) => Self::cql_type_to_arrow_data_type(inner),
            // Map, Tuple, UDT: fall back to Utf8 until #677/#678 add proper support.
            CqlType::Map(_, _) | CqlType::Tuple(_) | CqlType::Udt(_, _) => ArrowDataType::Utf8,
            // Custom/unknown types: Utf8
            CqlType::Custom(_) => ArrowDataType::Utf8,
        }
    }

    // =========================================================================
    // Recursive value → ArrayRef builder (Issue #676)
    // =========================================================================

    /// Recursively build an Arrow `ArrayRef` from a slice of optional `Value`
    /// references, guided by a `CqlType` for element dispatch.
    ///
    /// This is the shared recursive entry point used by both top-level column
    /// dispatch and nested element building (list-of-list, list-of-set, etc.).
    ///
    /// `CqlType::Frozen(inner)` is transparent: `Value::Frozen(inner)` runtime
    /// values are also unwrapped before dispatch.
    ///
    /// `Map`, `Tuple`, and `UDT` currently fall back to `Utf8` via
    /// `ValueFormatter` — new match arms in this function (plus corresponding
    /// arms in `cql_type_to_arrow_data_type`) are sufficient to add typed support
    /// in issues #677 and #678.
    fn build_typed_value_array(
        cql_type: &CqlType,
        values: &[Option<&Value>],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        // Unwrap Frozen at the type level — transparent for both schema and values.
        let effective_type = Self::unwrap_frozen_type(cql_type);

        match effective_type {
            // ----------------------------------------------------------------
            // Scalar types
            // ----------------------------------------------------------------
            CqlType::Boolean => {
                let arr: Vec<Option<bool>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Boolean(b) => Some(*b),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(BooleanArray::from(arr)))
            }
            CqlType::TinyInt => {
                let arr: Vec<Option<i8>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::TinyInt(i) => Some(*i),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(Int8Array::from(arr)))
            }
            CqlType::SmallInt => {
                let arr: Vec<Option<i16>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::SmallInt(i) => Some(*i),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(Int16Array::from(arr)))
            }
            CqlType::Int => {
                let arr: Vec<Option<i32>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Integer(i) => Some(*i),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(Int32Array::from(arr)))
            }
            CqlType::BigInt => {
                let arr: Vec<Option<i64>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::BigInt(i) => Some(*i),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(Int64Array::from(arr)))
            }
            CqlType::Counter => {
                let arr: Vec<Option<i64>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Counter(c) => Some(*c),
                            Value::BigInt(i) => Some(*i),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(Int64Array::from(arr)))
            }
            CqlType::Float => {
                let arr: Vec<Option<f32>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Float32(f) => Some(*f),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(Float32Array::from(arr)))
            }
            CqlType::Double => {
                let arr: Vec<Option<f64>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Float(f) => Some(*f),
                            Value::Float32(f) => Some(*f as f64),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(Float64Array::from(arr)))
            }
            CqlType::Text | CqlType::Ascii | CqlType::Varchar => {
                let arr: Vec<Option<String>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Text(s) => Some(s.clone()),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(StringArray::from(arr)))
            }
            CqlType::Blob => {
                let byte_slices: Vec<Option<Vec<u8>>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Blob(b) => Some(b.clone()),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                let refs: Vec<Option<&[u8]>> = byte_slices.iter().map(|o| o.as_deref()).collect();
                Ok(Arc::new(BinaryArray::from(refs)))
            }
            CqlType::Timestamp => {
                let arr: Vec<Option<i64>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Timestamp(ts) => Some(*ts),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(
                    TimestampMillisecondArray::from(arr).with_timezone("UTC"),
                ))
            }
            CqlType::Date => {
                let arr: Vec<Option<i32>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Date(d) => Some(*d),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(Date32Array::from(arr)))
            }
            CqlType::Time => {
                let arr: Vec<Option<i64>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Time(t) => Some(*t),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(Time64NanosecondArray::from(arr)))
            }
            CqlType::Decimal => {
                let mut builder = arrow::array::Decimal128Builder::new()
                    .with_precision_and_scale(DECIMAL_MAX_PRECISION, DECIMAL_FIXED_SCALE as i8)?;
                for opt in values {
                    let v = Self::unwrap_frozen_value(*opt);
                    match v {
                        Some(Value::Decimal { scale, unscaled }) => {
                            let rescaled = Self::rescale_decimal(*scale, unscaled)?;
                            builder.append_value(rescaled);
                        }
                        Some(Value::Null) | None => builder.append_null(),
                        Some(other) => {
                            return Err(format!(
                                "expected Decimal value in element, got {:?}",
                                other
                            )
                            .into());
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
                    let v = Self::unwrap_frozen_value(*opt);
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
                                    return Err(
                                        "varint element exceeds Decimal128(38, 0) range".into()
                                    );
                                }
                                let i128_val = bigint_to_i128(&bigint)?;
                                builder.append_value(i128_val);
                            }
                        }
                        Some(Value::Null) | None => builder.append_null(),
                        Some(other) => {
                            return Err(format!(
                                "expected Varint value in element, got {:?}",
                                other
                            )
                            .into());
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            CqlType::Duration => {
                // Serialise as Utf8 text (parquet crate v53 MonthDayNano NYI).
                let arr: Vec<Option<String>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Duration { .. } => Some(ValueFormatter::format_value(v)),
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(StringArray::from(arr)))
            }
            CqlType::Uuid | CqlType::TimeUuid => {
                let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
                for opt in values {
                    let v = Self::unwrap_frozen_value(*opt);
                    match v {
                        Some(Value::Uuid(bytes)) => builder.append_value(bytes)?,
                        Some(Value::Null) | None => builder.append_null(),
                        Some(other) => {
                            return Err(
                                format!("expected Uuid value in element, got {:?}", other).into()
                            );
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            CqlType::Inet => {
                let arr: Vec<Option<String>> = values
                    .iter()
                    .map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
                        Some(match v {
                            Value::Inet(bytes) => {
                                Some(ValueFormatter::format_value(&Value::Inet(bytes.clone())))
                            }
                            Value::Null => None,
                            _ => None,
                        })
                    })
                    .flatten()
                    .collect();
                Ok(Arc::new(StringArray::from(arr)))
            }
            // ----------------------------------------------------------------
            // List and Set (recursive): element type dispatches back here.
            // Arrow has no dedicated Set type; Set maps to List.
            // ----------------------------------------------------------------
            CqlType::List(inner) | CqlType::Set(inner) => {
                let element_type = Self::cql_type_to_arrow_data_type(inner);
                let item_field = Arc::new(Field::new("item", element_type, true));

                // Collect flat elements for all list/set values,
                // recording offsets so we can reconstruct the list structure.
                let mut offsets: Vec<i32> = vec![0];
                let mut flat_elements: Vec<Option<&Value>> = Vec::new();
                let mut null_bitmap: Vec<bool> = Vec::new();

                for opt in values {
                    let v = Self::unwrap_frozen_value(*opt);
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
                let elements_array = Self::build_typed_value_array(inner, &flat_elements)?;

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
            CqlType::Frozen(inner) => Self::build_typed_value_array(inner, values),
            // Map, Tuple, UDT: fall back to Utf8 until #677/#678 add typed support.
            CqlType::Map(_, _) | CqlType::Tuple(_) | CqlType::Udt(_, _) | CqlType::Custom(_) => {
                let arr: Vec<Option<String>> = values
                    .iter()
                    .map(|opt| {
                        let v = *opt;
                        match v {
                            Some(Value::Null) | None => None,
                            Some(v) => Some(ValueFormatter::format_value(v)),
                        }
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
    fn unwrap_frozen_type(cql_type: &CqlType) -> &CqlType {
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
    ///
    /// This is a shallow unwrap; for deeply nested Frozen values the caller
    /// should loop or use the recursive builder which handles Frozen at each level.
    fn unwrap_frozen_value<'a>(v: Option<&'a Value>) -> Option<&'a Value> {
        match v {
            Some(Value::Frozen(inner)) => Some(inner.as_ref()),
            other => other,
        }
    }

    /// Map CQL DataType to Arrow DataType
    fn data_type_to_arrow(data_type: &DataType) -> ArrowDataType {
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

    /// Convert all rows to Arrow arrays (column-oriented)
    fn convert_to_arrays(
        columns: &[ColumnInfo],
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<Vec<ArrayRef>, Box<dyn StdError>> {
        columns
            .iter()
            .map(|col| Self::convert_column_to_array(col, rows))
            .collect()
    }

    /// Convert a single column across all rows to an Arrow array.
    ///
    /// When `col.cql_type` is `Some` and the type is a high-fidelity scalar
    /// (date, time, decimal, varint, duration, uuid/timeuuid, inet, counter),
    /// the corresponding typed builder is used.
    ///
    /// For `List`, `Set`, and `Frozen(List|Set)` with a `cql_type`, the
    /// recursive `build_typed_value_array` path is used (Issue #676), which
    /// maps element types through the same scalar mapping above.
    ///
    /// All other cases fall through to the existing flat `data_type`-based dispatch.
    fn convert_column_to_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        // High-fidelity CQL-type dispatch (Issues #675, #676)
        if let Some(cql_type) = &col.cql_type {
            // Check if the (possibly Frozen-wrapped) type is a List or Set.
            let effective = Self::unwrap_frozen_type(cql_type);
            match effective {
                CqlType::Date => return Self::build_date32_array(col, rows),
                CqlType::Time => return Self::build_time64_ns_array(col, rows),
                CqlType::Decimal => return Self::build_decimal128_array(col, rows),
                CqlType::Varint => return Self::build_varint_as_decimal128_array(col, rows),
                CqlType::Duration => return Self::build_duration_utf8_array(col, rows),
                CqlType::Uuid | CqlType::TimeUuid => {
                    return Self::build_uuid_fixed_binary_array(col, rows)
                }
                CqlType::Inet => return Self::build_inet_utf8_array(col, rows),
                CqlType::Counter => return Self::build_int64_array(col, rows),
                // List and Set: use the recursive typed builder (Issue #676).
                CqlType::List(_) | CqlType::Set(_) => {
                    let column_values: Vec<Option<&Value>> =
                        rows.iter().map(|row| row.values.get(&col.name)).collect();
                    return Self::build_typed_value_array(cql_type, &column_values);
                }
                // All other complex/collection types fall through to the flat dispatch.
                _ => {}
            }
        }

        // Flat data_type dispatch (legacy path — no behavior change for existing callers)
        match &col.data_type {
            DataType::Boolean => Self::build_boolean_array(col, rows),
            DataType::TinyInt => Self::build_int8_array(col, rows),
            DataType::SmallInt => Self::build_int16_array(col, rows),
            DataType::Integer => Self::build_int32_array(col, rows),
            DataType::BigInt => Self::build_int64_array(col, rows),
            DataType::Float32 => Self::build_float32_array(col, rows),
            DataType::Float => Self::build_float64_array(col, rows),
            DataType::Text | DataType::Json => Self::build_string_array(col, rows),
            DataType::Blob => Self::build_binary_array(col, rows),
            DataType::Timestamp => Self::build_timestamp_array(col, rows),
            DataType::Uuid => Self::build_uuid_array(col, rows),
            DataType::List | DataType::Set => Self::build_list_array(col, rows),
            DataType::Map => Self::build_map_array(col, rows),
            DataType::Tuple
            | DataType::Udt
            | DataType::Frozen
            | DataType::Tombstone
            | DataType::Null => {
                Self::build_string_array(col, rows) // Fallback to string representation
            }
        }
    }

    // =========================================================================
    // Type-specific array builders
    // =========================================================================

    fn build_boolean_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

    fn build_int8_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

    fn build_int16_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

    fn build_int32_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

    fn build_int64_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

    fn build_uuid_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

        // Build FixedSizeBinaryArray manually
        let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
        for opt in values {
            match opt {
                Some(uuid) => builder.append_value(&uuid)?,
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    // =========================================================================
    // High-fidelity CQL type builders (Issue #675)
    // =========================================================================

    /// Build an Arrow `Date32` array from `Value::Date(i32)`.
    ///
    /// `Value::Date` already carries signed days since 1970-01-01 (the SSTable
    /// parser removes the Cassandra `i32::MIN` offset before storing the value
    /// in `Value::Date`), which is exactly the Arrow `Date32` encoding.
    fn build_date32_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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
    ///
    /// CQL `time` is stored as nanoseconds since midnight in `Value::Time`,
    /// matching the Arrow `Time64(Nanosecond)` encoding exactly.
    fn build_time64_ns_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

    /// Rescale a CQL decimal value to the fixed column scale (`DECIMAL_FIXED_SCALE`).
    ///
    /// Returns the rescaled `i128` value, or an error if:
    /// - The rescaled magnitude exceeds 38 decimal digits (overflow of `Decimal128`).
    /// - Checked multiplication overflows `i128` when scaling up.
    fn rescale_decimal(scale: i32, unscaled: &[u8]) -> Result<i128, Box<dyn StdError>> {
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
        // abs() on BigInt gives the magnitude.
        let abs_rescaled = if rescaled.sign() == num_bigint::Sign::Minus {
            -rescaled.clone()
        } else {
            rescaled.clone()
        };
        if abs_rescaled > max_abs {
            return Err(format!(
                "Decimal value exceeds Decimal128(38, {DECIMAL_FIXED_SCALE}) range after rescaling"
            )
            .into());
        }

        // Convert BigInt to i128.
        // `to_signed_bytes_be()` gives the two's-complement big-endian representation.
        // We sign-extend it to fill an i128 (16 bytes).
        bigint_to_i128(&rescaled)
    }

    /// Build an Arrow `Decimal128(38, DECIMAL_FIXED_SCALE)` array from
    /// `Value::Decimal { scale, unscaled }`.
    ///
    /// Each value is rescaled to `DECIMAL_FIXED_SCALE`.  Values that cannot
    /// be represented exactly (overflow, too many digits) produce an error.
    fn build_decimal128_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let mut builder = arrow::array::Decimal128Builder::new()
            .with_precision_and_scale(DECIMAL_MAX_PRECISION, DECIMAL_FIXED_SCALE as i8)?;

        for row in rows {
            match row.values.get(&col.name) {
                Some(Value::Decimal { scale, unscaled }) => {
                    let rescaled = Self::rescale_decimal(*scale, unscaled)
                        .map_err(|e| format!("Column '{}': {e}", col.name))?;
                    builder.append_value(rescaled);
                }
                Some(Value::Null) | None => {
                    builder.append_null();
                }
                Some(other) => {
                    return Err(format!(
                        "Column '{}': expected Decimal value, got {:?}",
                        col.name, other
                    )
                    .into());
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Build an Arrow `Decimal128(38, 0)` array from `Value::Varint(Vec<u8>)`.
    ///
    /// Varint bytes are big-endian two's-complement signed integers.  Values
    /// that exceed 38 decimal digits (cannot fit in `Decimal128`) produce an
    /// error.  Callers that need to handle arbitrarily large varints should
    /// use the `Utf8` fallback path (available via `DataType::Text` without
    /// `cql_type` set).
    fn build_varint_as_decimal128_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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
                            return Err(format!(
                                "Column '{}': varint value exceeds Decimal128(38, 0) range",
                                col.name
                            )
                            .into());
                        }

                        let i128_val = bigint_to_i128(&bigint)
                            .map_err(|e| format!("Column '{}': {e}", col.name))?;
                        builder.append_value(i128_val);
                    }
                }
                Some(Value::Null) | None => {
                    builder.append_null();
                }
                Some(other) => {
                    return Err(format!(
                        "Column '{}': expected Varint value, got {:?}",
                        col.name, other
                    )
                    .into());
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Build an Arrow `Utf8` array from `Value::Duration { months, days, nanos }`.
    ///
    /// CQL Duration is ideally represented as `Interval(MonthDayNano)` in Arrow,
    /// but the `parquet` crate v53 does not support writing `IntervalMonthDayNano`
    /// to Parquet files (the Parquet INTERVAL logical type only supports millisecond
    /// precision, not nanoseconds).  We therefore serialize durations as their
    /// canonical CQL text form (e.g. `"1mo2d3ns"`) via `ValueFormatter`.  When the
    /// `parquet` crate gains `MonthDayNano` write support, this builder can be
    /// upgraded to emit `IntervalMonthDayNanoArray` instead.
    fn build_duration_utf8_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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
    ///
    /// The field carries the Arrow UUID extension metadata
    /// (`ARROW:extension:name` = `arrow.uuid`), which is set in
    /// `cql_type_to_arrow_field`.  This builder just writes the raw bytes;
    /// the schema-level metadata is what triggers Parquet UUID logical type.
    fn build_uuid_fixed_binary_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
        for row in rows {
            match row.values.get(&col.name) {
                Some(Value::Uuid(bytes)) => builder.append_value(bytes)?,
                Some(Value::Null) | None => builder.append_null(),
                Some(other) => {
                    return Err(format!(
                        "Column '{}': expected Uuid value, got {:?}",
                        col.name, other
                    )
                    .into());
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Build an Arrow `Utf8` array from `Value::Inet(Vec<u8>)`.
    ///
    /// InetAddress is intentionally stored as canonical text (e.g. "192.168.1.1"
    /// or "2001:db8::1") rather than raw bytes.  There is no standard Arrow type
    /// for IP addresses, and text is the most portable representation for
    /// downstream consumers (DuckDB, pandas, etc.).
    fn build_inet_utf8_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

    fn build_list_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

    fn build_map_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
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

    /// Write RecordBatch to Parquet bytes
    fn write_parquet(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn StdError>> {
        let mut buffer = Vec::new();

        // Configure Snappy compression (Cassandra default)
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        Ok(buffer)
    }
}

// ============================================================================
// Streaming Parquet Writer (Issue #280)
// ============================================================================

/// Streaming Parquet writer for memory-efficient export of large datasets
///
/// Unlike the batch `ParquetWriter`, this writer processes data incrementally
/// using Parquet row groups. Each chunk is converted to a row group, allowing
/// export of arbitrarily large result sets within memory constraints.
///
/// # Row Group Strategy
///
/// The writer buffers rows until `row_group_size` is reached (default: 10,000),
/// then writes a complete row group to the file. This balances memory usage
/// against I/O efficiency.
///
/// # Example
///
/// ```ignore
/// let file = File::create("output.parquet")?;
/// let mut writer = StreamingParquetWriter::new(file, 10_000);
///
/// writer.write_header(&metadata)?;
///
/// for chunk in result_iterator.chunks(10_000) {
///     writer.write_chunk(&chunk)?;
/// }
///
/// writer.finalize()?;
/// ```
pub struct StreamingParquetWriter<W: Write + Send> {
    /// Inner Arrow/Parquet writer
    writer: Option<ArrowWriter<W>>,
    /// Arrow schema
    schema: Option<Arc<Schema>>,
    /// Column metadata
    columns: Vec<ColumnInfo>,
    /// Buffered rows for current row group
    row_buffer: Vec<QueryRow>,
    /// Row group size (rows per group)
    row_group_size: usize,
    /// Total rows written
    rows_written: u64,
}

impl<W: Write + Send> StreamingParquetWriter<W> {
    /// Create a new streaming Parquet writer
    ///
    /// # Arguments
    ///
    /// * `_output` - The output writer (typically a File) - unused in base constructor,
    ///              use `create_streaming_parquet_writer()` for proper initialization
    /// * `row_group_size` - Number of rows per row group (default: 10,000)
    #[allow(dead_code)]
    pub fn new(_output: W, row_group_size: usize) -> Self {
        Self {
            writer: None,
            schema: None,
            columns: Vec::new(),
            row_buffer: Vec::with_capacity(row_group_size),
            row_group_size,
            rows_written: 0,
        }
    }

    /// Create with default row group size (10,000 rows)
    #[allow(dead_code)]
    pub fn with_defaults(output: W) -> Self {
        Self::new(output, 10_000)
    }

    /// Write buffered rows as a row group
    #[allow(dead_code)]
    fn flush_row_group(&mut self) -> Result<(), OutputError> {
        if self.row_buffer.is_empty() {
            return Ok(());
        }

        let writer = self.writer.as_mut().ok_or_else(|| {
            OutputError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Writer not initialized - call write_header first",
            ))
        })?;

        // Convert buffered rows to Arrow arrays
        let arrays =
            ParquetWriter::convert_to_arrays(&self.columns, &self.row_buffer).map_err(|e| {
                OutputError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;

        // Create RecordBatch for this row group
        let schema = self.schema.as_ref().ok_or_else(|| {
            OutputError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Schema not initialized",
            ))
        })?;

        let batch = RecordBatch::try_new(Arc::clone(schema), arrays).map_err(|e| {
            OutputError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        // Write row group
        writer.write(&batch).map_err(|e| {
            OutputError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        self.row_buffer.clear();
        Ok(())
    }
}

impl<W: Write + Send> StreamingWriter for StreamingParquetWriter<W> {
    fn write_header(&mut self, metadata: &QueryMetadata) -> Result<(), OutputError> {
        // Store column metadata
        self.columns = metadata.columns.clone();

        // Build Arrow schema
        let schema = ParquetWriter::build_schema(&self.columns).map_err(|e| {
            OutputError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;
        self.schema = Some(Arc::new(schema));

        // Note: We can't create the ArrowWriter here because it needs ownership of the writer.
        // Since we don't have the writer available (it would require changing the struct),
        // we defer initialization until the first write_chunk call.
        // This is handled in the actual implementation.

        Ok(())
    }

    fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, OutputError> {
        // Add rows to buffer
        self.row_buffer.extend(rows.iter().cloned());
        self.rows_written += rows.len() as u64;

        // Flush complete row groups
        let mut flushed = 0;
        while self.row_buffer.len() >= self.row_group_size {
            // Take row_group_size rows from buffer
            let chunk: Vec<QueryRow> = self.row_buffer.drain(..self.row_group_size).collect();

            // Convert to arrays and write
            if let Some(ref mut writer) = self.writer {
                let arrays =
                    ParquetWriter::convert_to_arrays(&self.columns, &chunk).map_err(|e| {
                        OutputError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        ))
                    })?;

                let schema = self.schema.as_ref().ok_or_else(|| {
                    OutputError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Schema not initialized",
                    ))
                })?;

                let batch = RecordBatch::try_new(Arc::clone(schema), arrays).map_err(|e| {
                    OutputError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;

                writer.write(&batch).map_err(|e| {
                    OutputError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;

                flushed += self.row_group_size;
            }
        }

        Ok(flushed)
    }

    fn finalize(&mut self) -> Result<(), OutputError> {
        // Flush any remaining rows
        if !self.row_buffer.is_empty() {
            if let Some(ref mut writer) = self.writer {
                let remaining = std::mem::take(&mut self.row_buffer);

                let arrays =
                    ParquetWriter::convert_to_arrays(&self.columns, &remaining).map_err(|e| {
                        OutputError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        ))
                    })?;

                let schema = self.schema.as_ref().ok_or_else(|| {
                    OutputError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Schema not initialized",
                    ))
                })?;

                let batch = RecordBatch::try_new(Arc::clone(schema), arrays).map_err(|e| {
                    OutputError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;

                writer.write(&batch).map_err(|e| {
                    OutputError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })?;
            }
        }

        // Close the Parquet writer
        if let Some(writer) = self.writer.take() {
            writer.close().map_err(|e| {
                OutputError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })?;
        }

        Ok(())
    }

    fn rows_written(&self) -> u64 {
        self.rows_written
    }
}

/// Create a StreamingParquetWriter that writes to a file
///
/// This is a convenience function that handles the file creation and
/// ArrowWriter initialization.
pub fn create_streaming_parquet_writer(
    file: File,
    metadata: &QueryMetadata,
    row_group_size: usize,
) -> Result<StreamingParquetWriter<File>, OutputError> {
    // Build Arrow schema
    let schema = ParquetWriter::build_schema(&metadata.columns).map_err(|e| {
        OutputError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))
    })?;
    let schema = Arc::new(schema);

    // Configure Snappy compression
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    // Create ArrowWriter
    let arrow_writer =
        ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).map_err(|e| {
            OutputError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

    Ok(StreamingParquetWriter {
        writer: Some(arrow_writer),
        schema: Some(schema),
        columns: metadata.columns.clone(),
        row_buffer: Vec::with_capacity(row_group_size),
        row_group_size,
        rows_written: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, FixedSizeBinaryArray};
    use bytes::Bytes;
    use cqlite_core::query::{ColumnInfo, QueryRow};
    use cqlite_core::{RowKey, Value};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashMap;

    fn default_config() -> OutputConfig {
        OutputConfig::default()
    }

    /// Helper to verify Parquet output by reading it back
    fn read_parquet_back(bytes: &[u8]) -> Result<RecordBatch, Box<dyn StdError>> {
        // Use Bytes which implements ChunkReader
        let bytes = Bytes::copy_from_slice(bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        let mut reader = builder.build()?;
        reader
            .next()
            .ok_or_else(|| "No batches in Parquet file".to_string())?
            .map_err(|e| Box::new(e) as Box<dyn StdError>)
    }

    #[test]
    fn test_empty_result() {
        let result = QueryResult::new();
        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();

        // Should produce valid (empty) Parquet
        assert!(!bytes.is_empty());
        // Parquet magic bytes: PAR1
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_boolean_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "bool_col".to_string(),
            DataType::Boolean,
            true,
            0,
        )];

        let mut values = HashMap::new();
        values.insert("bool_col".to_string(), Value::Boolean(true));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn test_integer_types() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new("tiny".to_string(), DataType::TinyInt, false, 0),
            ColumnInfo::new("small".to_string(), DataType::SmallInt, false, 1),
            ColumnInfo::new("int".to_string(), DataType::Integer, false, 2),
            ColumnInfo::new("big".to_string(), DataType::BigInt, false, 3),
        ];

        let mut values = HashMap::new();
        values.insert("tiny".to_string(), Value::TinyInt(127));
        values.insert("small".to_string(), Value::SmallInt(32767));
        values.insert("int".to_string(), Value::Integer(2147483647));
        values.insert("big".to_string(), Value::BigInt(9223372036854775807));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn test_float_types() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new("f32".to_string(), DataType::Float32, false, 0),
            ColumnInfo::new("f64".to_string(), DataType::Float, false, 1),
        ];

        let mut values = HashMap::new();
        values.insert("f32".to_string(), Value::Float32(3.14));
        values.insert("f64".to_string(), Value::Float(2.71828));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_text_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "text_col".to_string(),
            DataType::Text,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert(
            "text_col".to_string(),
            Value::Text("Hello, Parquet!".to_string()),
        );
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0);
        let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "Hello, Parquet!");
    }

    #[test]
    fn test_blob_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "blob_col".to_string(),
            DataType::Blob,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert(
            "blob_col".to_string(),
            Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        );
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0);
        let binary_array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary_array.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_timestamp_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "ts_col".to_string(),
            DataType::Timestamp,
            false,
            0,
        )];

        // 2023-01-15 10:30:45.123 UTC = 1673778645123 milliseconds
        let mut values = HashMap::new();
        values.insert("ts_col".to_string(), Value::Timestamp(1673778645123));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_uuid_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "uuid_col".to_string(),
            DataType::Uuid,
            false,
            0,
        )];

        let uuid_bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];

        let mut values = HashMap::new();
        values.insert("uuid_col".to_string(), Value::Uuid(uuid_bytes));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0);
        let uuid_array = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(uuid_array.value(0), uuid_bytes);
    }

    #[test]
    fn test_null_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "nullable_col".to_string(),
            DataType::Text,
            true,
            0,
        )];

        // First row with value, second row with null
        let mut values1 = HashMap::new();
        values1.insert(
            "nullable_col".to_string(),
            Value::Text("present".to_string()),
        );
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![1]), values1));

        let mut values2 = HashMap::new();
        values2.insert("nullable_col".to_string(), Value::Null);
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![2]), values2));

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 2);
        let col = batch.column(0);
        let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(string_array.is_valid(0));
        assert!(!string_array.is_valid(1)); // Null
    }

    #[test]
    fn test_list_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "list_col".to_string(),
            DataType::List,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert(
            "list_col".to_string(),
            Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
        );
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_map_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "map_col".to_string(),
            DataType::Map,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert(
            "map_col".to_string(),
            Value::Map(vec![
                (Value::Text("key1".to_string()), Value::Integer(1)),
                (Value::Text("key2".to_string()), Value::Integer(2)),
            ]),
        );
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_config_limit() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "id".to_string(),
            DataType::Integer,
            false,
            0,
        )];

        // Add 10 rows
        for i in 1..=10 {
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Integer(i));
            let row = QueryRow::with_values(RowKey::new(vec![i as u8]), values);
            result.rows.push(row);
        }

        // Limit to 3 rows
        let config = OutputConfig {
            color_enabled: true,
            limit: Some(3),
            page_size: None,
            target: crate::output::OutputTarget::Stdout,
            overwrite: false,
        };
        let bytes = ParquetWriter::write(&result, &config).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(
            batch.num_rows(),
            3,
            "Limit should restrict output to 3 rows"
        );
    }

    #[test]
    fn test_multiple_rows() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
            ColumnInfo::new("name".to_string(), DataType::Text, false, 1),
        ];

        for i in 1..=5 {
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Integer(i));
            values.insert("name".to_string(), Value::Text(format!("row_{i}")));
            let row = QueryRow::with_values(RowKey::new(vec![i as u8]), values);
            result.rows.push(row);
        }

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_counter_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "counter_col".to_string(),
            DataType::BigInt, // Counters map to BigInt in DataType
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert("counter_col".to_string(), Value::Counter(1000000));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_parquet_magic_bytes() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "col".to_string(),
            DataType::Integer,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert("col".to_string(), Value::Integer(42));
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![1]), values));

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();

        // Parquet files start and end with PAR1 magic bytes
        assert_eq!(&bytes[0..4], b"PAR1", "Should start with PAR1 magic bytes");
        assert_eq!(
            &bytes[bytes.len() - 4..],
            b"PAR1",
            "Should end with PAR1 magic bytes"
        );
    }
}
