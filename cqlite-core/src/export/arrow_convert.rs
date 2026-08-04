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
use std::collections::HashMap;
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
    /// checks field DATA TYPES and array lengths (plus a non-nullable field
    /// holding actual nulls), so a schema whose fields are REORDERED, RENAMED,
    /// re-flagged NULLABLE, or stripped of their METADATA is accepted by Arrow and
    /// silently mislabels every affected column — this variant is what makes
    /// that a rejection instead.
    ///
    /// The two in-crate error-conversion boundaries (`ParquetExportError`,
    /// `DeltaParquetError`) carry it as their `InvalidValue` **through `Display`**,
    /// so the prefix above survives the hop. Neither writer supplies its own
    /// schema today, so neither can raise it; they map it rather than drop it so a
    /// future caller that does gets the reason instead of a bare Arrow error.
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
/// # Cost
///
/// The schema is built HERE from `columns` and handed straight to
/// [`rows_to_record_batch_trusted_schema`], so it is NOT revalidated: this entry
/// point pays ONE `column_to_field` per column, not two. Routing it through
/// [`rows_to_record_batch_with_schema`] instead would reconstruct every expected
/// `Field` a second time to compare it against the fields
/// [`build_arrow_schema`] had just produced from the same `columns` — pure
/// duplicate work, since a mismatch is unconstructible on this path (issue
/// #3096, third review).
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
    // Trusted: `schema` IS `build_arrow_schema(columns)`, one line up.
    rows_to_record_batch_trusted_schema(schema, columns, rows)
}

/// Build the arrays for `columns`/`rows` and assemble the batch under a schema
/// the CALLER HAS ALREADY ESTABLISHED is [`build_arrow_schema`]'s output for
/// `columns` (issue #3096, third review).
///
/// The shared tail of both public entry points, and the only place
/// `RecordBatch::try_new` is called. What differs is the precondition:
///
/// * [`rows_to_record_batch`] built `schema` from these same `columns`
///   immediately before calling, so it holds by CONSTRUCTION — checking it would
///   re-derive, per batch, the very fields `build_arrow_schema` had just built.
/// * [`rows_to_record_batch_with_schema`] accepts a schema it did not build, so
///   it must PROVE the precondition first via [`check_schema_matches_columns`];
///   that is a documented public contract and is not weakened by this helper's
///   existence.
///
/// * [`rows_to_record_batch_prevalidated`] takes a [`PrevalidatedSchema`], which
///   OWNS the columns its schema was derived from and can only be constructed by
///   deriving it — so the precondition is discharged by that argument's TYPE and a
///   violating pair is unconstructible.
///
/// This is deliberately NOT public: `pub(super)` reaches the two in-module callers
/// above and stops there, so it is not a "skip the checks" door for external
/// callers, and the schema-mismatch guarantee of the public API is unchanged.
/// `RecordBatch::try_new` still owns array lengths and field data types, so an
/// internal caller that broke its precondition would surface as
/// [`ArrowConvertError::Arrow`] rather than undefined behaviour.
///
/// [`rows_to_record_batch_prevalidated`]: super::arrow_prevalidated::rows_to_record_batch_prevalidated
/// [`PrevalidatedSchema`]: super::arrow_prevalidated::PrevalidatedSchema
pub(super) fn rows_to_record_batch_trusted_schema(
    schema: Arc<Schema>,
    columns: &[ColumnInfo],
    rows: &[QueryRow],
) -> Result<RecordBatch, ArrowConvertError> {
    let arrays = convert_to_arrays(columns, rows)?;
    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(batch)
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
/// # What is validated, exactly (issue #3096, second review)
///
/// This function accepts a schema it did not build, so the mismatch question is
/// real — and what is enforced is that `schema` **IS** [`build_arrow_schema`]'s
/// output for `columns`, not merely that it is compatible with the arrays:
///
/// * **Full `Field` identity, position by position** — checked HERE, by
///   [`check_schema_matches_columns`], against `column_to_field(col)`: the exact
///   function [`build_arrow_schema`] itself uses, so there is ONE source of truth
///   and no re-derived mapping that can drift. Arrow's `Field: PartialEq`
///   compares **name, data type, nullability and metadata** (deliberately
///   excluding the IPC-only `dict_id`/`dict_is_ordered`, which arrow-schema
///   documents as irrelevant to schema equality), so every one of those four axes
///   is covered — including the `ARROW:extension:name` = `arrow.uuid` metadata a
///   uuid/timeuuid column carries. Field ORDER and ARITY follow from comparing
///   position by position.
/// * **Schema-level metadata** — must be empty, because `build_arrow_schema`
///   builds with `Schema::new`, which sets none.
/// * **Array lengths** — left to `RecordBatch::try_new`, which owns them.
///
/// All of the above is checked here because **`RecordBatch::try_new` does not
/// check it**: it compares field DATA TYPES and array lengths, and rejects a
/// non-nullable field that actually holds nulls. It never compares field NAMES,
/// field METADATA, or schema metadata, and a nullability difference the data does
/// not contradict passes it. So a schema whose fields are REORDERED or RENAMED
/// among columns of the same Arrow type, or which drops the uuid extension
/// metadata, or which flips `nullable`, is accepted by Arrow and silently
/// mislabels the batch. The first version of this guard checked arity and names
/// only — leaving nullability and metadata unchecked while documenting the
/// contract as enforced; the full-identity comparison is what makes the
/// documented guarantee TRUE.
///
/// A near-miss worth naming: `cqlite-flight`'s `MergeProducer::arrow_schema()`
/// (the WIRE schema for `GetFlightInfo`/`GetSchema`) augments every field with
/// `cqlite:pushdown` metadata, so it is NOT this function's schema and is
/// correctly rejected here. The egress path passes
/// `build_arrow_schema(&self.columns)` unmodified and is unaffected.
///
/// # Cost — borne by THIS entry point only
///
/// One `column_to_field` construction and one `Field` comparison per COLUMN per
/// BATCH — for a 12-column table, 12 short `String` allocations plus one small
/// metadata `HashMap` per uuid/timeuuid column, against per-batch work
/// proportional to rows x columns.
///
/// [`rows_to_record_batch`] does NOT pay it: it goes through the private trusted
/// tail both entry points share, because its schema is
/// [`build_arrow_schema`]'s output for the same `columns` by construction (issue
/// #3096, third review — before that fix it delegated here and every caller paid
/// the mapping twice).
///
/// So state the schema-reuse tradeoff exactly, without overclaiming: reusing one
/// `Arc<Schema>` across a scan's batches THROUGH THIS ENTRY POINT saves the
/// `Schema`/`Fields` allocation and a fresh `SchemaRef` per batch, and costs a
/// `column_to_field` construction plus a `Field` comparison per column per batch.
/// It is therefore CHEAPER IN ALLOCATIONS but NOT strictly less work per batch than
/// rebuilding the schema through [`rows_to_record_batch`] — the per-field
/// construction is paid either way, once as `build_arrow_schema` or once as this
/// validation.
///
/// **A caller that wants reuse to be genuinely free should not use this function.**
/// That is what [`PrevalidatedSchema`] + [`rows_to_record_batch_prevalidated`] are
/// (issue #3096, fourth review): the schema is bound to the columns it was derived
/// from, in a type whose only constructor derives it, so a mismatch is
/// unconstructible and there is nothing to revalidate — ZERO per-batch schema work.
/// The Flight `do_get` egress (`cqlite-flight`'s `EgressBatchPlan`) goes that way;
/// this entry point remains for callers whose schema arrives from somewhere it did
/// not derive, and its validation is exactly what such a caller needs.
///
/// [`PrevalidatedSchema`]: super::arrow_prevalidated::PrevalidatedSchema
/// [`rows_to_record_batch_prevalidated`]: super::arrow_prevalidated::rows_to_record_batch_prevalidated
///
/// # Errors
///
/// Returns [`ArrowConvertError::SchemaMismatch`] if `schema` is not
/// [`build_arrow_schema`]'s output for `columns` (field count, order, name, data
/// type, nullability, field metadata, or schema-level metadata);
/// [`ArrowConvertError::InvalidValue`] if a value cannot be represented in the
/// target Arrow type; and [`ArrowConvertError::Arrow`] if array construction
/// fails or the built arrays do not match `schema`'s field types or lengths.
pub fn rows_to_record_batch_with_schema(
    schema: Arc<Schema>,
    columns: &[ColumnInfo],
    rows: &[QueryRow],
) -> Result<RecordBatch, ArrowConvertError> {
    check_schema_matches_columns(&schema, columns)?;
    // The precondition is now PROVEN, not assumed, so the shared tail may trust it.
    rows_to_record_batch_trusted_schema(schema, columns, rows)
}

/// Reject a caller-supplied `schema` that is not [`build_arrow_schema`]'s output
/// for `columns` (issue #3096, second review).
///
/// FULL `Field` identity — name, data type, nullability, metadata — position by
/// position, plus empty schema-level metadata. See
/// [`rows_to_record_batch_with_schema`] for which of those axes Arrow itself
/// checks (data types, lengths) and which it silently accepts (everything else),
/// and for the cost.
///
/// The expected field is CONSTRUCTED by `column_to_field`, the same function
/// [`build_arrow_schema`] uses — nothing is inferred from a name's or a type's
/// shape, and no part of the mapping is restated here where it could drift
/// (no-heuristics, #28).
fn check_schema_matches_columns(
    schema: &Schema,
    columns: &[ColumnInfo],
) -> Result<(), ArrowConvertError> {
    // Probe instrumentation (issue #3096, third review): the ONLY way a test can
    // distinguish "the trusted path skipped validation" from "validation ran and
    // happened to pass", since a schema `build_arrow_schema` just produced can
    // never FAIL this check. A no-op in any default/release build.
    record_schema_validation();
    if !schema.metadata().is_empty() {
        return Err(ArrowConvertError::SchemaMismatch(format!(
            "schema carries top-level metadata {:?} but build_arrow_schema sets none \
             (it builds with Schema::new), so this is not the schema of the supplied \
             columns; RecordBatch::try_new never compares schema metadata",
            sorted_pairs(schema.metadata())
        )));
    }
    let fields = schema.fields();
    if fields.len() != columns.len() {
        return Err(ArrowConvertError::SchemaMismatch(format!(
            "schema has {} field(s) but {} column(s) were supplied",
            fields.len(),
            columns.len()
        )));
    }
    for (i, (field, col)) in fields.iter().zip(columns.iter()).enumerate() {
        let expected = column_to_field(col);
        if field.as_ref() != &expected {
            return Err(ArrowConvertError::SchemaMismatch(field_mismatch_reason(
                i, field, &expected,
            )));
        }
    }
    Ok(())
}

/// The FIRST axis on which `field` differs from the `expected` field of column
/// `i`, as an operator-facing reason.
///
/// One named axis rather than two `Debug` dumps of a whole `Field`, and each arm
/// says what Arrow would have done with that difference — which is the question a
/// reader has when a batch is refused. The axes are exactly the four Arrow's
/// `Field: PartialEq` compares, so the last arm is reached only for a metadata
/// difference (all four matching means `==` held and this function was not
/// called).
fn field_mismatch_reason(i: usize, field: &Field, expected: &Field) -> String {
    if field.name() != expected.name() {
        return format!(
            "field {i} is '{}' but column {i} is '{}' — a reordered or renamed \
             schema is accepted by RecordBatch::try_new whenever the Arrow types \
             line up, and would mislabel the batch",
            field.name(),
            expected.name()
        );
    }
    if field.data_type() != expected.data_type() {
        return format!(
            "field {i} '{}' has Arrow type {:?} but column {i} maps to {:?}",
            field.name(),
            field.data_type(),
            expected.data_type()
        );
    }
    if field.is_nullable() != expected.is_nullable() {
        return format!(
            "field {i} '{}' is nullable={} but column {i} maps to nullable={} — \
             RecordBatch::try_new only rejects a non-nullable field that actually \
             holds nulls, so this difference is otherwise accepted silently",
            field.name(),
            field.is_nullable(),
            expected.is_nullable()
        );
    }
    format!(
        "field {i} '{}' carries metadata {:?} but column {i} maps to {:?} — \
         RecordBatch::try_new never compares field metadata, so dropping or altering \
         the Arrow UUID extension key is otherwise accepted silently",
        field.name(),
        sorted_pairs(field.metadata()),
        sorted_pairs(expected.metadata())
    )
}

/// Count one [`check_schema_matches_columns`] run on this thread.
///
/// A no-op — no static, no atomic, nothing referenced — unless the test-only
/// `arrow-validation-probe` feature is on (or this crate is under `cargo test`).
/// Same convention as `storage::sstable::read_work_counters`' `record_*()`: the
/// call site is unconditional, the body is not, so a default or release build links
/// no counter at all.
#[inline]
fn record_schema_validation() {
    #[cfg(any(test, feature = "arrow-validation-probe"))]
    SCHEMA_VALIDATIONS.with(|n| n.set(n.get() + 1));
}

#[cfg(any(test, feature = "arrow-validation-probe"))]
thread_local! {
    /// How many times [`check_schema_matches_columns`] has run on THIS thread.
    ///
    /// Probe-only. Exists so a test can assert the negative —
    /// `rows_to_record_batch` and `rows_to_record_batch_prevalidated` perform ZERO
    /// schema validations while `rows_to_record_batch_with_schema` performs exactly
    /// one — which is not observable from any of their return values (all succeed,
    /// with an identical batch).
    ///
    /// [`rows_to_record_batch_prevalidated`]: super::arrow_prevalidated::rows_to_record_batch_prevalidated
    static SCHEMA_VALIDATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The current thread's [`SCHEMA_VALIDATIONS`] count (issue #3096).
///
/// Test/probe-only surface, compiled under the `arrow-validation-probe` feature,
/// which `cqlite-flight` enables as a DEV-dependency so its egress test can assert
/// that the Flight `do_get` flush path revalidates NOTHING. Pair it with
/// `prevalidated_batch_builds_on_this_thread` — a zero here is also true of a
/// thread that built no batches, so the two together are the property.
#[cfg(any(test, feature = "arrow-validation-probe"))]
pub fn schema_validations_on_this_thread() -> usize {
    SCHEMA_VALIDATIONS.with(|n| n.get())
}

/// Metadata as key-sorted pairs, so a rejection message is deterministic.
///
/// `Field::metadata` is a `HashMap`, whose `Debug` order is not stable across
/// runs; a message a test can assert on (and an operator can diff) must be.
fn sorted_pairs(metadata: &HashMap<String, String>) -> Vec<(&str, &str)> {
    let mut pairs: Vec<(&str, &str)> = metadata
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    pairs.sort_unstable();
    pairs
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
