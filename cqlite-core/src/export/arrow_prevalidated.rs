//! A schema whose PROVENANCE is structural, not documented (issue #3096, lever 6,
//! fourth review).
//!
//! # Why this type exists
//!
//! [`rows_to_record_batch`] pays no schema validation because it builds the schema
//! from `columns` one line before using it — the precondition holds by
//! construction, inside one function, where a reader can see it. A caller that
//! wants the same deal ACROSS CALLS (the Flight `do_get` egress builds one schema
//! per merge and emits many batches under it) could not have it: the only entry
//! point taking a caller-supplied `Arc<Schema>`,
//! [`rows_to_record_batch_with_schema`], must re-prove the precondition on every
//! call, because a bare `(schema, columns)` pair carries no evidence that the two
//! belong together. So the per-batch saving of reusing the `Arc` was cancelled by a
//! per-batch `column_to_field` per column — lever 6 delivered nothing on the only
//! surface that ships.
//!
//! The fix is not a `_trusted` function any caller could hand any schema to. It is
//! this type: [`PrevalidatedSchema`] carries the columns it was built FROM
//! alongside the schema it built, its fields are private, and its only constructor
//! DERIVES the schema itself via [`build_arrow_schema`]. A caller cannot pair a
//! schema with columns it does not describe, so
//! [`rows_to_record_batch_prevalidated`] takes no `columns` argument at all: there
//! is nothing left for it to disagree with, and the mismatch
//! [`check_schema_matches_columns`] exists to catch is UNCONSTRUCTIBLE rather than
//! merely unchecked.
//!
//! This is the follow-up [`rows_to_record_batch_with_schema`]'s own docs named
//! ("a prevalidated-schema newtype owning both the schema and the columns it was
//! built from, making a mismatch unconstructible").
//!
//! # What is NOT changed
//!
//! [`rows_to_record_batch_with_schema`] keeps FULL `Field`-identity validation for
//! every caller-supplied schema. It is a public API with a documented rejection
//! contract; this module adds a second, differently-shaped door rather than
//! widening that one.
//!
//! [`rows_to_record_batch`]: super::arrow_convert::rows_to_record_batch
//! [`rows_to_record_batch_with_schema`]: super::arrow_convert::rows_to_record_batch_with_schema
//! [`check_schema_matches_columns`]: super::arrow_convert

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::query::{ColumnInfo, QueryRow};

use super::arrow_convert::{
    build_arrow_schema, rows_to_record_batch_trusted_schema, ArrowConvertError,
};

/// An Arrow [`Schema`] bound to the CQL columns it was derived from.
///
/// Build it ONCE per output column set — per merge, per scan, per export — and
/// convert every batch under it with [`rows_to_record_batch_prevalidated`]. The
/// per-batch cost is then an `Arc` refcount bump and nothing else: no `Schema`
/// rebuild, and no revalidation either.
///
/// # The invariant, and why it cannot be broken from outside
///
/// `schema == build_arrow_schema(&columns)`. Both fields are private and the only
/// constructor is [`Self::build`], which takes the columns and derives the schema
/// itself — no constructor accepts a schema, so no caller can supply one that
/// disagrees. [`Self::columns`] hands out a shared slice (never `&mut`), so the
/// pair cannot drift after construction either.
///
/// The columns are CLONED (once per column set, not per batch): the alternative,
/// borrowing them, would tie this value's lifetime to the caller's column storage
/// and force the borrow through every drive loop that holds the plan. The clone is
/// column METADATA only — the same order of work `build_arrow_schema` already does
/// once here — and is not repeated per batch.
#[derive(Debug, Clone)]
pub struct PrevalidatedSchema {
    /// The columns `schema` was derived from, and the slice the arrays are built
    /// from — one source, so they cannot describe different column sets.
    columns: Vec<ColumnInfo>,
    /// `build_arrow_schema(&columns)`, built in [`Self::build`] and shared by every
    /// batch.
    schema: Arc<Schema>,
}

impl PrevalidatedSchema {
    /// Derive the Arrow schema for `columns` and bind the two together.
    ///
    /// The ONLY constructor, deliberately: it takes the columns and calls
    /// [`build_arrow_schema`] itself, which is what makes the type's invariant hold
    /// by construction instead of by a caller's promise.
    ///
    /// # Errors
    ///
    /// Returns [`ArrowConvertError`] if a column cannot be mapped to an Arrow
    /// field — exactly the errors [`build_arrow_schema`] returns.
    pub fn build(columns: &[ColumnInfo]) -> Result<Self, ArrowConvertError> {
        Ok(Self {
            schema: Arc::new(build_arrow_schema(columns)?),
            columns: columns.to_vec(),
        })
    }

    /// The shared Arrow schema. Every batch built through
    /// [`rows_to_record_batch_prevalidated`] carries this exact `Arc`.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// The columns [`Self::schema`] was derived from, and the ones the arrays are
    /// built from.
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }
}

/// Convert `rows` to a [`RecordBatch`] under an already-derived
/// [`PrevalidatedSchema`].
///
/// The third and last entry point of the CQL → Arrow converter, and the one the
/// Flight `do_get` egress uses (`cqlite-flight`'s `EgressBatchPlan`):
///
/// | Entry point                          | Schema comes from | Validation per batch |
/// |--------------------------------------|-------------------|----------------------|
/// | [`rows_to_record_batch`]             | built inline      | none (built here)    |
/// | [`rows_to_record_batch_with_schema`] | the caller        | full `Field` identity|
/// | this function                        | a [`PrevalidatedSchema`] | none (unconstructible mismatch) |
///
/// There is no `columns` parameter: the columns come from `prevalidated`, which is
/// also where the schema came from, so the two cannot disagree. That is the whole
/// point of the type — the "trusted" precondition is discharged by the argument's
/// TYPE rather than asserted in prose at the call site.
///
/// `RecordBatch::try_new` still owns array lengths and field data types, as on both
/// other entry points.
///
/// # Errors
///
/// Returns [`ArrowConvertError::InvalidValue`] if a value cannot be represented in
/// its target Arrow type, and [`ArrowConvertError::Arrow`] if array construction
/// fails or an array does not match its field.
/// [`ArrowConvertError::SchemaMismatch`] is unreachable here by construction.
///
/// [`rows_to_record_batch`]: super::arrow_convert::rows_to_record_batch
/// [`rows_to_record_batch_with_schema`]: super::arrow_convert::rows_to_record_batch_with_schema
pub fn rows_to_record_batch_prevalidated(
    prevalidated: &PrevalidatedSchema,
    rows: &[QueryRow],
) -> Result<RecordBatch, ArrowConvertError> {
    record_prevalidated_batch_build();
    // Trusted: `prevalidated.schema` IS `build_arrow_schema(&prevalidated.columns)`
    // — enforced by `PrevalidatedSchema`'s private fields and single constructor,
    // not by this call site's word.
    rows_to_record_batch_trusted_schema(
        Arc::clone(&prevalidated.schema),
        &prevalidated.columns,
        rows,
    )
}

/// Count one [`rows_to_record_batch_prevalidated`] call on this thread.
///
/// A no-op — no static, no atomic, nothing referenced — unless the test-only
/// `arrow-validation-probe` feature is on (or this crate is under `cargo test`),
/// following the `work-counters` convention: the call site is unconditional, the
/// body is not.
#[inline]
fn record_prevalidated_batch_build() {
    #[cfg(any(test, feature = "arrow-validation-probe"))]
    PREVALIDATED_BUILDS.with(|n| n.set(n.get() + 1));
}

#[cfg(any(test, feature = "arrow-validation-probe"))]
thread_local! {
    /// How many batches this thread has built through
    /// [`rows_to_record_batch_prevalidated`].
    ///
    /// The POSITIVE control for the negative assertion in
    /// `super::arrow_convert::schema_validations_on_this_thread`: "zero validations
    /// on this thread" is also true of a thread that built no batches at all, so a
    /// test asserting only the zero could pass vacuously — most plausibly if the
    /// batch build ever moves off the asserting thread. Pairing the two counters
    /// turns that silent vacuity into a failure.
    ///
    /// Thread-local, so concurrently-running tests in one binary cannot perturb
    /// each other's counts.
    static PREVALIDATED_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many batches THIS thread has built through
/// [`rows_to_record_batch_prevalidated`] (issue #3096).
///
/// Test/probe-only surface, compiled under the `arrow-validation-probe` feature,
/// which `cqlite-flight` enables as a DEV-dependency so its egress test can assert
/// the Flight flush path both (a) ran here and (b) revalidated nothing. Absent from
/// any default or release build.
#[cfg(any(test, feature = "arrow-validation-probe"))]
pub fn prevalidated_batch_builds_on_this_thread() -> usize {
    PREVALIDATED_BUILDS.with(|n| n.get())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::export::arrow_convert::{
        rows_to_record_batch, rows_to_record_batch_with_schema, schema_validations_on_this_thread,
    };
    use crate::schema::CqlType;
    use crate::types::{DataType, Value};
    use crate::RowKey;
    use std::collections::HashMap;

    /// A uuid + text column set: uuid carries the Arrow extension metadata, which
    /// is the part of a schema two construction routes could most plausibly
    /// diverge on.
    fn uuid_and_text() -> (Vec<ColumnInfo>, Vec<QueryRow>) {
        let columns = vec![
            col("id", DataType::Uuid, CqlType::Uuid, 0),
            col("label", DataType::Text, CqlType::Text, 1),
        ];
        let mut values: HashMap<Arc<str>, Value> = HashMap::new();
        values.insert("id".into(), Value::Uuid([7u8; 16]));
        values.insert("label".into(), Value::Text("L".into()));
        let rows = vec![QueryRow {
            values,
            key: RowKey::new(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        }];
        (columns, rows)
    }

    fn col(name: &str, data_type: DataType, cql_type: CqlType, position: usize) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
            position,
            table_name: None,
            cql_type: Some(cql_type),
        }
    }

    /// The type's invariant, asserted: the bound schema IS
    /// `build_arrow_schema(columns)`, and the bound columns ARE the ones supplied.
    #[test]
    fn the_bound_schema_is_build_arrow_schema_of_the_bound_columns() {
        let (columns, _) = uuid_and_text();
        let prevalidated = PrevalidatedSchema::build(&columns).expect("build");
        assert_eq!(
            prevalidated.schema().as_ref(),
            &build_arrow_schema(&columns).expect("schema")
        );
        assert_eq!(prevalidated.columns().len(), columns.len());
        for (bound, supplied) in prevalidated.columns().iter().zip(columns.iter()) {
            assert_eq!(bound.name, supplied.name);
            assert_eq!(bound.cql_type, supplied.cql_type);
        }
    }

    /// The prevalidated entry point performs ZERO schema validations, for any
    /// number of batches — and reports each build, so the zero is not vacuous.
    #[test]
    fn the_prevalidated_path_validates_nothing() {
        let (columns, rows) = uuid_and_text();
        let prevalidated = PrevalidatedSchema::build(&columns).expect("build");
        let validations_before = schema_validations_on_this_thread();
        let builds_before = prevalidated_batch_builds_on_this_thread();
        for _ in 0..3 {
            rows_to_record_batch_prevalidated(&prevalidated, &rows).expect("batch");
        }
        assert_eq!(
            prevalidated_batch_builds_on_this_thread() - builds_before,
            3,
            "the probe must see the three batches, or the zero below proves nothing"
        );
        assert_eq!(
            schema_validations_on_this_thread() - validations_before,
            0,
            "a PrevalidatedSchema cannot mismatch its columns, so validating it is \
             pure repeat work"
        );
    }

    /// Every batch shares the ONE `Arc`, and the batches are indistinguishable from
    /// what the two older entry points return.
    #[test]
    fn the_batches_share_one_schema_arc_and_match_the_other_entry_points() {
        let (columns, rows) = uuid_and_text();
        let prevalidated = PrevalidatedSchema::build(&columns).expect("build");
        let first = rows_to_record_batch_prevalidated(&prevalidated, &rows).expect("batch");
        let second = rows_to_record_batch_prevalidated(&prevalidated, &rows).expect("batch");
        assert!(
            Arc::ptr_eq(&first.schema(), &second.schema()),
            "both batches must carry the ONE shared schema Arc — value equality \
             would hold even if each batch rebuilt it"
        );
        assert!(Arc::ptr_eq(&first.schema(), prevalidated.schema()));

        let inline = rows_to_record_batch(&columns, &rows).expect("inline");
        let validated = rows_to_record_batch_with_schema(
            Arc::new(build_arrow_schema(&columns).expect("schema")),
            &columns,
            &rows,
        )
        .expect("validated");
        assert_eq!(first.schema().as_ref(), inline.schema().as_ref());
        assert_eq!(first.schema().as_ref(), validated.schema().as_ref());
        for i in 0..first.num_columns() {
            assert_eq!(
                first.column(i).to_data(),
                inline.column(i).to_data(),
                "column {i} must be byte-identical to the inline-schema path"
            );
            assert_eq!(
                first.column(i).to_data(),
                validated.column(i).to_data(),
                "column {i} must be byte-identical to the validating path"
            );
        }
    }
}
