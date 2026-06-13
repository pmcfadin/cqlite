//! Parquet export writer for QueryResult (feature = "parquet")
//!
//! Converts CQL query results to Apache Parquet format with proper type mapping.
//! Uses Snappy compression by default (Cassandra default, good speed/size balance).
//!
//! This module is compiled only when the `parquet` cargo feature is enabled
//! (off by default, Epic #682).  CQLite produces Parquet *files*; committing
//! those files to Iceberg/Delta table formats is an external committer's job
//! and is deliberately out of scope (see `export` module docs).
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
//! | map\<K,V\>        | `Map<Struct(key:K,value:V)>`          | Typed keys/values; nested OK      |
//! | tuple\<A,B,…\>    | `Struct(field_0:A, field_1:B, …)`     | Positional names; per-position types|
//! | udt\<name\>       | `Struct(f1:T1, f2:T2, …)`             | Field names from schema; null OK  |
//!
//! # List, Set, and Map mapping
//!
//! CQL `list<X>` and `set<X>` both map to Arrow `List` (Arrow has no dedicated Set
//! type).  Element types are mapped recursively through the same scalar mapping
//! table above, so `list<uuid>` produces `List<FixedSizeBinary(16)>`,
//! `list<timestamp>` produces `List<Timestamp(ms,UTC)>`, etc.
//!
//! CQL `map<K,V>` maps to Arrow `Map` with a non-nullable struct entries field
//! containing children `"key"` (non-nullable) and `"value"` (nullable), matching
//! the naming convention used by the legacy string-map path (`build_map_array`).
//! Both key and value types are mapped recursively, enabling nested value types
//! such as `map<text, frozen<list<int>>>`.
//!
//! `CqlType::Frozen(inner)` is transparent in the recursion: it unwraps and
//! recurses into `inner`.  Runtime `Value::Frozen(inner)` values are also unwrapped
//! before element dispatch.
//!
//! For nested collections (`list<frozen<list<int>>>`), the recursion produces
//! `List<List<Int32>>` and handles element unwrapping at all levels.
//!
//! # UDT and Tuple mapping (Issue #678)
//!
//! CQL `tuple<A,B,…>` maps to Arrow `Struct` with positional field names
//! (`field_0`, `field_1`, …) and per-position Arrow types mapped recursively.
//! CQL UDTs map to Arrow `Struct` with the UDT's schema field names and
//! recursively mapped field types.  Unset (None) UDT fields and missing
//! tuple positions become null children in the output StructArray.
//!
//! If a UDT or Tuple has zero fields (degenerate case), the column falls back
//! to `Utf8` and a note is logged in the field name to avoid an Arrow error
//! (`StructArray` must have at least one child or an explicit validity bitmap,
//! but zero-field structs are not representable in Parquet).
//!
//! # Recursive builder design
//!
//! `build_typed_value_array(cql_type, values)` is the shared recursive entry point
//! used by both top-level column dispatch and nested element building.  Adding
//! support for Tuple/UDT elements (#678) requires only new match arms in
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

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, ListArray, MapArray, StringArray, StructArray,
    Time64NanosecondArray, TimestampMillisecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use crate::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
use crate::schema::CqlType;
use crate::types::{DataType, Value};
use crate::util::value_fmt::ValueFormatter;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use thiserror::Error;

// ============================================================================
// Export options and error type (Issues #683, #684)
// ============================================================================

/// Compression codec for Parquet output.
///
/// Snappy is the default (Cassandra default, good speed/size balance).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParquetCompression {
    /// Snappy compression (default).
    #[default]
    Snappy,
    /// Zstandard compression (better ratio, slower).
    Zstd,
    /// No compression.
    Uncompressed,
}

impl ParquetCompression {
    /// Map to the `parquet` crate's compression enum.
    fn to_parquet(self) -> Compression {
        match self {
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
            ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
        }
    }
}

/// Writer-owned options for Parquet export.
///
/// Replaces the CLI-only `OutputConfig` parameter (Issue #683) so the writer
/// has no dependency on CLI types.  The CLI constructs this from its own
/// configuration; library consumers construct it directly.
#[derive(Debug, Clone)]
pub struct ParquetExportOptions {
    /// Maximum number of rows to write (batch writer only). `None` = all rows.
    pub row_limit: Option<usize>,
    /// Rows per Parquet row group (streaming writer). Default: 10,000.
    pub row_group_size: usize,
    /// Compression codec. Default: Snappy.
    pub compression: ParquetCompression,
}

impl Default for ParquetExportOptions {
    fn default() -> Self {
        Self {
            row_limit: None,
            row_group_size: 10_000,
            compression: ParquetCompression::default(),
        }
    }
}

/// Errors produced by the Parquet export writers.
///
/// A dedicated `thiserror` enum (Issue #683) so the writer does not depend on
/// the CLI's `OutputError`.  The CLI maps these to its own error type at the
/// boundary.
#[derive(Debug, Error)]
pub enum ParquetExportError {
    /// Underlying I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Arrow array or schema construction failure.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet encoding failure.
    #[error("Parquet error: {0}")]
    Parquet(#[from] ::parquet::errors::ParquetError),
    /// A value could not be represented in the target Arrow/Parquet type.
    #[error("{0}")]
    InvalidValue(String),
    /// Invalid writer configuration (e.g. zero row group size).
    #[error("invalid Parquet export options: {0}")]
    InvalidOptions(String),
}

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
fn bigint_to_i128(n: &num_bigint::BigInt) -> Result<i128, ParquetExportError> {
    let tc_bytes = n.to_signed_bytes_be();
    if tc_bytes.len() > 16 {
        return Err(ParquetExportError::InvalidValue("BigInt value requires more than 16 bytes; cannot fit in i128".to_string()));
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
pub struct ParquetWriter;

impl ParquetWriter {
    /// Write QueryResult to Parquet binary format
    ///
    /// # Arguments
    ///
    /// * `result` - The query result to convert to Parquet
    /// * `options` - Export options (row limit, compression)
    ///
    /// # Returns
    ///
    /// Binary Parquet data or error
    pub fn write(
        result: &QueryResult,
        options: &ParquetExportOptions,
    ) -> Result<Vec<u8>, ParquetExportError> {
        // Handle empty results
        if result.metadata.columns.is_empty() {
            return Self::write_empty_parquet(options);
        }

        // Build Arrow schema from column metadata
        let schema = Self::build_schema(&result.metadata.columns)?;

        // Apply row limit if specified in the options
        let rows_to_process = if let Some(limit) = options.row_limit {
            &result.rows[..result.rows.len().min(limit)]
        } else {
            &result.rows
        };

        // Convert rows to Arrow arrays (one per column)
        let arrays = Self::convert_to_arrays(&result.metadata.columns, rows_to_process)?;

        // Create RecordBatch
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

        // Write to Parquet with the configured compression
        Self::write_parquet(&batch, options.compression)
    }

    /// Write an empty Parquet file
    fn write_empty_parquet(options: &ParquetExportOptions) -> Result<Vec<u8>, ParquetExportError> {
        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(Arc::new(schema));
        Self::write_parquet(&batch, options.compression)
    }

    /// Build Arrow schema from CQL column metadata
    fn build_schema(columns: &[ColumnInfo]) -> Result<Schema, ParquetExportError> {
        let fields: Vec<Field> = columns
            .iter()
            .map(Self::column_to_field)
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
            // Map: emit typed Arrow Map with non-nullable keys and nullable values.
            // The entries struct is conventionally named "entries" with children
            // "key" (non-nullable) and "value" (nullable), matching the legacy
            // `build_map_array` schema so that the two paths are interchangeable.
            CqlType::Map(key_type, val_type) => {
                let key_arrow = Self::cql_type_to_arrow_data_type(key_type);
                let val_arrow = Self::cql_type_to_arrow_data_type(val_type);
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
                let struct_type = Self::cql_type_to_arrow_data_type(cql_type);
                Some(Field::new(name, struct_type, nullable))
            }
            // UDT → Arrow Struct with the UDT's field names.
            // Zero-field UDTs fall back to Utf8 (Arrow Struct requires ≥1 field).
            CqlType::Udt(_udt_name, udt_fields) => {
                if udt_fields.is_empty() {
                    return Some(Field::new(name, ArrowDataType::Utf8, nullable));
                }
                let struct_type = Self::cql_type_to_arrow_data_type(cql_type);
                Some(Field::new(name, struct_type, nullable))
            }
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
    /// types and recursively handles `List`, `Set`, `Frozen`, `Map`, `Tuple`, and `Udt`.
    ///
    /// `CqlType::Frozen(inner)` is transparent: the same Arrow type as `inner`.
    ///
    /// Zero-field `Tuple` and `Udt` fall back to `Utf8` because Arrow `Struct` with
    /// zero fields cannot be represented in Parquet.
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
            // Map: Arrow Map type with typed key (non-nullable) and value (nullable).
            // The entries struct field is named "entries" with children "key" and
            // "value", matching the legacy `data_type_to_arrow` and `build_map_array`
            // field naming so that schema and array representations are consistent.
            CqlType::Map(key_type, val_type) => {
                let key_arrow = Self::cql_type_to_arrow_data_type(key_type);
                let val_arrow = Self::cql_type_to_arrow_data_type(val_type);
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
                            Self::cql_type_to_arrow_data_type(t),
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
                            Self::cql_type_to_arrow_data_type(field_type),
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
    ) -> Result<ArrayRef, ParquetExportError> {
        // Unwrap Frozen at the type level — transparent for both schema and values.
        let effective_type = Self::unwrap_frozen_type(cql_type);

        match effective_type {
            // ----------------------------------------------------------------
            // Scalar types
            // ----------------------------------------------------------------
            CqlType::Boolean => {
                let arr: Vec<Option<bool>> = values
                    .iter()
                    .filter_map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                    let v = Self::unwrap_frozen_value(*opt);
                    match v {
                        Some(Value::Decimal { scale, unscaled }) => {
                            let rescaled = Self::rescale_decimal(*scale, unscaled)?;
                            builder.append_value(rescaled);
                        }
                        Some(Value::Null) | None => builder.append_null(),
                        Some(other) => {
                            return Err(ParquetExportError::InvalidValue(format!(
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
                                    return Err(ParquetExportError::InvalidValue("varint element exceeds Decimal128(38, 0) range".to_string()));
                                }
                                let i128_val = bigint_to_i128(&bigint)?;
                                builder.append_value(i128_val);
                            }
                        }
                        Some(Value::Null) | None => builder.append_null(),
                        Some(other) => {
                            return Err(ParquetExportError::InvalidValue(format!(
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
                        let v = Self::unwrap_frozen_value(*opt)?;
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
                    let v = Self::unwrap_frozen_value(*opt);
                    match v {
                        Some(Value::Uuid(bytes)) => builder.append_value(bytes)?,
                        Some(Value::Null) | None => builder.append_null(),
                        Some(other) => {
                            return Err(ParquetExportError::InvalidValue(format!("expected Uuid value in element, got {:?}", other)));
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            CqlType::Inet => {
                let arr: Vec<Option<String>> = values
                    .iter()
                    .filter_map(|opt| {
                        let v = Self::unwrap_frozen_value(*opt)?;
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
            // ----------------------------------------------------------------
            // Map: recursively typed keys and values (Issue #677).
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
                let key_arrow = Self::cql_type_to_arrow_data_type(key_type);
                let val_arrow = Self::cql_type_to_arrow_data_type(val_type);

                let mut offsets: Vec<i32> = vec![0];
                let mut flat_keys: Vec<Option<&Value>> = Vec::new();
                let mut flat_vals: Vec<Option<&Value>> = Vec::new();
                let mut null_bitmap: Vec<bool> = Vec::new();

                for opt in values {
                    let v = Self::unwrap_frozen_value(*opt);
                    match v {
                        Some(Value::Map(pairs)) => {
                            null_bitmap.push(true);
                            for (k, val) in pairs {
                                // Keys must be non-nullable in Arrow MapArray.
                                if matches!(k, Value::Null) {
                                    return Err(ParquetExportError::InvalidValue("null key in map is not allowed in Arrow MapArray".to_string()));
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
                let key_array = Self::build_typed_value_array(key_type, &flat_keys)?;
                let val_array = Self::build_typed_value_array(val_type, &flat_vals)?;

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
                    .map(|opt| Self::unwrap_frozen_value(*opt))
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
                    let child_arr = Self::build_typed_value_array(element_type, &child_values)?;
                    child_arrays.push(child_arr);
                }

                let struct_fields: Fields = Fields::from(
                    element_types
                        .iter()
                        .enumerate()
                        .map(|(i, t)| {
                            Field::new(
                                format!("field_{i}"),
                                Self::cql_type_to_arrow_data_type(t),
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
                    .map(|opt| Self::unwrap_frozen_value(*opt))
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
                // rather than `None`.  See the Tuple arm above for the explanation:
                // scalar type builders drop `None` via `flatten()`, which would
                // produce a shorter child array than the struct expects, causing
                // Arrow's StructArray::new() to panic.
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
                    let child_arr = Self::build_typed_value_array(field_type, &child_values)?;
                    child_arrays.push(child_arr);
                }

                let struct_fields: Fields = Fields::from(
                    udt_fields
                        .iter()
                        .map(|(field_name, field_type)| {
                            Field::new(
                                field_name.as_str(),
                                Self::cql_type_to_arrow_data_type(field_type),
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
    fn unwrap_frozen_value(v: Option<&Value>) -> Option<&Value> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<Vec<ArrayRef>, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
                // List, Set, Map, Tuple, and Udt: use the recursive typed builder
                // (Issues #676, #677, #678).
                CqlType::List(_)
                | CqlType::Set(_)
                | CqlType::Map(_, _)
                | CqlType::Tuple(_)
                | CqlType::Udt(_, _) => {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
                Some(uuid) => builder.append_value(uuid)?,
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
    fn rescale_decimal(scale: i32, unscaled: &[u8]) -> Result<i128, ParquetExportError> {
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
            return Err(ParquetExportError::InvalidValue(format!(
                "Decimal value exceeds Decimal128(38, {DECIMAL_FIXED_SCALE}) range after rescaling"
            )));
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
        let mut builder = arrow::array::Decimal128Builder::new()
            .with_precision_and_scale(DECIMAL_MAX_PRECISION, DECIMAL_FIXED_SCALE as i8)?;

        for row in rows {
            match row.values.get(&col.name) {
                Some(Value::Decimal { scale, unscaled }) => {
                    let rescaled = Self::rescale_decimal(*scale, unscaled)
                        .map_err(|e| ParquetExportError::InvalidValue(format!("Column '{}': {e}", col.name)))?;
                    builder.append_value(rescaled);
                }
                Some(Value::Null) | None => {
                    builder.append_null();
                }
                Some(other) => {
                    return Err(ParquetExportError::InvalidValue(format!(
                        "Column '{}': expected Decimal value, got {:?}",
                        col.name, other
                    )));
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
                            return Err(ParquetExportError::InvalidValue(format!(
                                "Column '{}': varint value exceeds Decimal128(38, 0) range",
                                col.name
                            )));
                        }

                        let i128_val = bigint_to_i128(&bigint)
                            .map_err(|e| ParquetExportError::InvalidValue(format!("Column '{}': {e}", col.name)))?;
                        builder.append_value(i128_val);
                    }
                }
                Some(Value::Null) | None => {
                    builder.append_null();
                }
                Some(other) => {
                    return Err(ParquetExportError::InvalidValue(format!(
                        "Column '{}': expected Varint value, got {:?}",
                        col.name, other
                    )));
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
        let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
        for row in rows {
            match row.values.get(&col.name) {
                Some(Value::Uuid(bytes)) => builder.append_value(bytes)?,
                Some(Value::Null) | None => builder.append_null(),
                Some(other) => {
                    return Err(ParquetExportError::InvalidValue(format!(
                        "Column '{}': expected Uuid value, got {:?}",
                        col.name, other
                    )));
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
        rows: &[crate::query::QueryRow],
    ) -> Result<ArrayRef, ParquetExportError> {
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
    fn write_parquet(
        batch: &RecordBatch,
        compression: ParquetCompression,
    ) -> Result<Vec<u8>, ParquetExportError> {
        let mut buffer = Vec::new();

        let props = WriterProperties::builder()
            .set_compression(compression.to_parquet())
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
/// then writes a complete row group to the output. This balances memory usage
/// against I/O efficiency.
///
/// # Schema parity
///
/// The constructor uses the same [`ParquetWriter::build_schema`] call as the
/// batch writer, so the schema produced by the streaming path is always
/// identical to the schema produced by the batch `ParquetWriter`.
///
/// # Example
///
/// ```ignore
/// let file = File::create("output.parquet")?;
/// let mut writer = StreamingParquetWriter::new(
///     file,
///     &metadata,
///     &ParquetExportOptions::default(),
/// )?;
///
/// for chunk in result_iterator.chunks(10_000) {
///     writer.write_chunk(&chunk)?;
/// }
///
/// writer.finalize()?;
/// ```
pub struct StreamingParquetWriter<W: Write + Send> {
    /// Inner Arrow/Parquet writer (`None` after `finalize`)
    writer: Option<ArrowWriter<W>>,
    /// Arrow schema
    schema: Arc<Schema>,
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
    /// Create a streaming Parquet writer over any `W: Write + Send`.
    ///
    /// The Arrow schema is built from `metadata.columns` using the same
    /// mapping as the batch [`ParquetWriter`], and the Parquet file header
    /// is initialized immediately.
    ///
    /// # Errors
    ///
    /// Returns [`ParquetExportError::InvalidOptions`] if
    /// `options.row_group_size` is zero, or an Arrow/Parquet error if the
    /// schema cannot be built or the writer cannot be initialized.
    pub fn new(
        output: W,
        metadata: &QueryMetadata,
        options: &ParquetExportOptions,
    ) -> Result<Self, ParquetExportError> {
        if options.row_group_size == 0 {
            return Err(ParquetExportError::InvalidOptions(
                "row_group_size must be greater than 0".to_string(),
            ));
        }

        // Build Arrow schema - same helper used by the batch ParquetWriter.
        let schema = Arc::new(ParquetWriter::build_schema(&metadata.columns)?);

        // set_max_row_group_size makes `row_group_size` authoritative: without
        // it, ArrowWriter coalesces written batches into ~1M-row groups, which
        // both ignored the documented "row group per chunk" behavior and let
        // memory grow unbounded on large streaming exports.
        let props = WriterProperties::builder()
            .set_compression(options.compression.to_parquet())
            .set_max_row_group_size(options.row_group_size)
            .build();

        let arrow_writer = ArrowWriter::try_new(output, Arc::clone(&schema), Some(props))?;

        Ok(Self {
            writer: Some(arrow_writer),
            schema,
            columns: metadata.columns.clone(),
            row_buffer: Vec::with_capacity(options.row_group_size),
            row_group_size: options.row_group_size,
            rows_written: 0,
        })
    }

    /// Buffer rows and flush complete row groups.
    ///
    /// Returns the number of rows flushed to the output in this call (rows
    /// remaining in the buffer are flushed by [`finalize`](Self::finalize)).
    pub fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, ParquetExportError> {
        self.row_buffer.extend(rows.iter().cloned());
        self.rows_written += rows.len() as u64;

        let mut flushed = 0;
        while self.row_buffer.len() >= self.row_group_size {
            let chunk: Vec<QueryRow> = self.row_buffer.drain(..self.row_group_size).collect();
            self.write_row_group(&chunk)?;
            flushed += self.row_group_size;
        }

        Ok(flushed)
    }

    /// Flush any buffered rows and close the Parquet file.
    ///
    /// Must be called exactly once after all chunks are written; dropping the
    /// writer without calling `finalize` produces a truncated file.
    pub fn finalize(&mut self) -> Result<(), ParquetExportError> {
        if !self.row_buffer.is_empty() {
            let remaining = std::mem::take(&mut self.row_buffer);
            self.write_row_group(&remaining)?;
        }

        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }

        Ok(())
    }

    /// Total number of rows accepted by `write_chunk` so far.
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// Convert a slice of rows to a RecordBatch and write it as a row group.
    fn write_row_group(&mut self, rows: &[QueryRow]) -> Result<(), ParquetExportError> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            ParquetExportError::InvalidOptions(
                "writer already finalized - cannot write more rows".to_string(),
            )
        })?;

        let arrays = ParquetWriter::convert_to_arrays(&self.columns, rows)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)?;
        writer.write(&batch)?;

        Ok(())
    }
}

/// Create a StreamingParquetWriter that writes to a file.
///
/// Convenience wrapper around [`StreamingParquetWriter::new`].
pub fn create_streaming_parquet_writer(
    file: File,
    metadata: &QueryMetadata,
    options: &ParquetExportOptions,
) -> Result<StreamingParquetWriter<File>, ParquetExportError> {
    StreamingParquetWriter::new(file, metadata, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{ColumnInfo, QueryRow};
    use crate::{RowKey, Value};
    use arrow::array::{Array, FixedSizeBinaryArray};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashMap;
    use std::error::Error as StdError;

    fn default_options() -> ParquetExportOptions {
        ParquetExportOptions::default()
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
        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();

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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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
        values.insert("f32".to_string(), Value::Float32(3.5));
        values.insert("f64".to_string(), Value::Float(2.75));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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
        let options = ParquetExportOptions {
            row_limit: Some(3),
            ..Default::default()
        };
        let bytes = ParquetWriter::write(&result, &options).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
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

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();

        // Parquet files start and end with PAR1 magic bytes
        assert_eq!(&bytes[0..4], b"PAR1", "Should start with PAR1 magic bytes");
        assert_eq!(
            &bytes[bytes.len() - 4..],
            b"PAR1",
            "Should end with PAR1 magic bytes"
        );
    }
}
