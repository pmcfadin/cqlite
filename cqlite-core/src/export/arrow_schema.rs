//! CQL → Arrow **schema** mapping (feature = "arrow").
//!
//! Split out of `arrow_convert` (epic #1116 file-size split, issue #3096 Phase 0a)
//! with no behaviour change. This module owns the type side of the conversion —
//! turning `ColumnInfo` / `CqlType` / `DataType` into Arrow `Field`s and
//! `DataType`s — while the value side (building the arrays themselves) lives in
//! the `arrow_builders_*` / `arrow_typed_value` siblings and the entry points in
//! [`super::arrow_convert`].
//!
//! The full CQL → Arrow mapping table (and the rationale for each choice) is
//! documented on [`super::arrow_convert`].

use crate::query::ColumnInfo;
use crate::schema::CqlType;
use crate::types::DataType;
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

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
        // #4114: a `vector<element, n>` decodes to `Value::List` of its elements,
        // so it takes the SAME Arrow List mapping as a list/set. Arrow's
        // `FixedSizeList(element, n)` would additionally carry the dimension, but it
        // needs its own array builder and #4114's scope is the READ path; the value
        // shape here is correct either way.
        CqlType::List(inner) | CqlType::Set(inner) | CqlType::Vector(inner, _) => {
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
        // Same rule as `cql_type_to_arrow_field` above (#4114).
        CqlType::List(inner) | CqlType::Set(inner) | CqlType::Vector(inner, _) => {
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
// Flat DataType fallback mapping
// =========================================================================

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
