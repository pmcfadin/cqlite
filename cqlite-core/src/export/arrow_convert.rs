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

//!
//! # Module layout (epic #1116 file-size split, issue #3096 Phase 0a)
//!
//! This file keeps the public entry points and the top-level column dispatch;
//! the rest of the converter lives in siblings, with no behaviour change:
//!
//! | Module                     | Responsibility                                   |
//! |----------------------------|--------------------------------------------------|
//! | `arrow_schema`             | `CqlType`/`DataType` → Arrow `Field`/`DataType`  |
//! | `arrow_typed_value`        | recursive value → `ArrayRef` dispatcher (scalars)|
//! | `arrow_builders_scalar`    | per-column scalar/primitive array builders       |
//! | `arrow_builders_nested`    | collection / tuple / UDT array builders          |
//! | `arrow_convert_util`       | fail-closed offset guards, `Frozen` unwrapping   |
//! | `arrow_decimal`            | bounded decimal rescaling (issue #1755)          |
//! | `arrow_columnar`           | row → column transpose (issue #1495)             |

use crate::query::{ColumnInfo, QueryRow};
use crate::schema::CqlType;
use crate::types::DataType;
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use thiserror::Error;

use super::arrow_builders_nested::{build_list_array, build_map_array};
use super::arrow_builders_scalar::{
    build_binary_array, build_boolean_array, build_date32_array, build_decimal128_array,
    build_duration_utf8_array, build_float32_array, build_float64_array, build_inet_utf8_array,
    build_int16_array, build_int32_array, build_int64_array, build_int8_array, build_string_array,
    build_time64_ns_array, build_timestamp_array, build_uuid_array, build_uuid_fixed_binary_array,
    build_varint_as_decimal128_array,
};
use super::arrow_convert_util::{unwrap_frozen_type, Cells};
use super::arrow_schema::column_to_field;
// Also re-exported (`pub(crate)`) so `delta_parquet` keeps reaching the recursive
// builder through its existing `export::arrow_convert::…` path — the epic #1116
// split must not grow that already-over-threshold file with a new import line.
pub(crate) use super::arrow_typed_value::build_typed_value_array;

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
    /// A caller-supplied Arrow schema does not describe the column set it was
    /// passed with (issue #3096 review).
    ///
    /// Raised by [`rows_to_record_batch_with_schema`], which is the only entry
    /// point that accepts a schema it did not build. `RecordBatch::try_new`
    /// checks field TYPES and array lengths only, so a schema whose fields are
    /// REORDERED (or renamed) among same-typed columns is accepted by Arrow and
    /// silently mislabels every affected column — this variant is what makes
    /// that a rejection instead.
    #[error("Arrow schema does not match the column set: {0}")]
    SchemaMismatch(String),
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
    let schema = Arc::new(build_arrow_schema(columns)?);
    rows_to_record_batch_with_schema(schema, columns, rows)
}

/// [`rows_to_record_batch`] over a schema the caller already holds.
///
/// Identical output — `schema` MUST be the [`build_arrow_schema`] of the same
/// `columns`, which is exactly what [`rows_to_record_batch`] passes. The point is
/// the `Arc`: a caller that emits many batches over ONE column set (the Flight
/// `do_get` egress does, once per batch for the whole scan) rebuilt the entire
/// `Schema` — a `Vec<Field>`, each `Field` owning a fresh `String` name and, for
/// uuid/timeuuid columns, its own extension-metadata `HashMap` — on every call,
/// then dropped it again. Reusing the `Arc` makes the per-batch cost a refcount
/// bump (issue #3096, lever 6).
///
/// # What is validated, exactly (issue #3096 review)
///
/// This function accepts a schema it did not build, so the mismatch question is
/// real. Two independent checks cover it, and the split matters:
///
/// * **Field arity, NAMES and ORDER** — checked HERE, by
///   [`check_schema_matches_columns`], against `columns[i].name`. `Field`'s name
///   is always `col.name` ([`build_arrow_schema`] → `column_to_field`), so this
///   is an exact equality check, not a heuristic.
/// * **Field TYPES and array lengths** — checked by `RecordBatch::try_new`.
///
/// The name/order half is checked here **because `RecordBatch::try_new` does not
/// check it**: it compares field data types and lengths only, so a schema whose
/// fields are REORDERED among columns of the same Arrow type is accepted and
/// every one of those columns is silently mislabeled on the wire. The doc comment
/// this function shipped with claimed `try_new` covered that; it does not, and
/// this check is what makes the claim true.
///
/// Cost is one `usize` comparison plus one `str` comparison per COLUMN per batch
/// (12 short comparisons for `ws0.events`), against per-batch work proportional
/// to rows x columns — so it does not undo lever 6's `Arc` reuse.
///
/// # Errors
///
/// Returns [`ArrowConvertError::SchemaMismatch`] if `schema`'s field count,
/// names, or order do not match `columns`; [`ArrowConvertError::InvalidValue`] if
/// a value cannot be represented in the target Arrow type; and
/// [`ArrowConvertError::Arrow`] if array construction fails or the built arrays
/// do not match `schema`'s field TYPES or lengths.
pub fn rows_to_record_batch_with_schema(
    schema: Arc<Schema>,
    columns: &[ColumnInfo],
    rows: &[QueryRow],
) -> Result<RecordBatch, ArrowConvertError> {
    check_schema_matches_columns(&schema, columns)?;
    let arrays = convert_to_arrays(columns, rows)?;
    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(batch)
}

/// Reject a caller-supplied `schema` whose fields are not `columns`' fields, in
/// `columns`' order (issue #3096 review).
///
/// Only the arity/name/order half is checked: field TYPES and array lengths are
/// `RecordBatch::try_new`'s job, and duplicating that here would be dead weight.
/// Name equality is exact because [`build_arrow_schema`]'s `column_to_field`
/// names every field `col.name` verbatim — nothing is inferred from the name's
/// shape (no-heuristics, #28).
fn check_schema_matches_columns(
    schema: &Schema,
    columns: &[ColumnInfo],
) -> Result<(), ArrowConvertError> {
    let fields = schema.fields();
    if fields.len() != columns.len() {
        return Err(ArrowConvertError::SchemaMismatch(format!(
            "schema has {} field(s) but {} column(s) were supplied",
            fields.len(),
            columns.len()
        )));
    }
    for (i, (field, col)) in fields.iter().zip(columns.iter()).enumerate() {
        if field.name() != &col.name {
            return Err(ArrowConvertError::SchemaMismatch(format!(
                "field {i} is '{}' but column {i} is '{}' — a reordered or renamed \
                 schema is accepted by RecordBatch::try_new whenever the Arrow types \
                 line up, and would mislabel the batch",
                field.name(),
                col.name
            )));
        }
    }
    Ok(())
}

// =========================================================================
// Column-oriented conversion
// =========================================================================

/// Convert all rows to Arrow arrays (one per column).
///
/// Per-column accessors are resolved ONCE via [`transpose_columns`] (issue #1495
/// / parser epic J1): each builder gets its column's pre-resolved, row-aligned
/// slice instead of re-hashing `col.name` per cell. Output is byte-identical.
///
/// [`transpose_columns`]: super::arrow_columnar::transpose_columns
pub(crate) fn convert_to_arrays(
    columns: &[ColumnInfo],
    rows: &[QueryRow],
) -> Result<Vec<ArrayRef>, ArrowConvertError> {
    let columnar = super::arrow_columnar::transpose_columns(columns, rows);
    columns
        .iter()
        .zip(columnar.iter())
        .map(|(col, cells)| convert_column_to_array(col, cells))
        .collect()
}

/// Convert a single column's pre-resolved, row-aligned value slice to an Arrow
/// array. `cells[i]` is row `i`'s value (`None` when the column is absent),
/// resolved once by the transpose — no per-cell `col.name` hashing (issue #1495).
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
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    // High-fidelity CQL-type dispatch
    if let Some(cql_type) = &col.cql_type {
        // Check if the (possibly Frozen-wrapped) type is a List or Set.
        let effective = unwrap_frozen_type(cql_type);
        match effective {
            CqlType::Date => return build_date32_array(col, cells),
            CqlType::Time => return build_time64_ns_array(col, cells),
            CqlType::Decimal => return build_decimal128_array(col, cells),
            CqlType::Varint => return build_varint_as_decimal128_array(col, cells),
            CqlType::Duration => return build_duration_utf8_array(col, cells),
            CqlType::Uuid | CqlType::TimeUuid => return build_uuid_fixed_binary_array(col, cells),
            CqlType::Inet => return build_inet_utf8_array(col, cells),
            CqlType::Counter => return build_int64_array(col, cells),
            // List, Set, Map, Tuple, and Udt: use the recursive typed builder.
            CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _) => {
                return build_typed_value_array(cql_type, cells);
            }
            // All other complex/collection types fall through to the flat dispatch.
            _ => {}
        }
    }

    // Flat data_type dispatch (legacy path)
    match &col.data_type {
        DataType::Boolean => build_boolean_array(col, cells),
        DataType::TinyInt => build_int8_array(col, cells),
        DataType::SmallInt => build_int16_array(col, cells),
        DataType::Integer => build_int32_array(col, cells),
        DataType::BigInt => build_int64_array(col, cells),
        DataType::Float32 => build_float32_array(col, cells),
        DataType::Float => build_float64_array(col, cells),
        DataType::Text | DataType::Json => build_string_array(col, cells),
        DataType::Blob => build_binary_array(col, cells),
        DataType::Timestamp => build_timestamp_array(col, cells),
        DataType::Uuid => build_uuid_array(col, cells),
        DataType::List | DataType::Set => build_list_array(col, cells),
        DataType::Map => build_map_array(col, cells),
        DataType::Tuple
        | DataType::Udt
        | DataType::Frozen
        | DataType::Tombstone
        | DataType::Null => {
            build_string_array(col, cells) // Fallback to string representation
        }
    }
}

// =========================================================================
// Tests (loaded from `arrow_convert_tests.rs`, epic #1116)
// =========================================================================

#[cfg(test)]
#[path = "arrow_convert_tests.rs"]
mod arrow_convert_tests;
