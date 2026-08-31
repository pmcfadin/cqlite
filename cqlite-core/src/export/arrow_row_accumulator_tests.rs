//! Tests for the fused push-time columnar row accumulator (issue #3552).
//!
//! Loaded via `#[path]` from `arrow_row_accumulator.rs` so the production module
//! stays under the campsite file-size threshold (epic #1116).
//!
//! The load-bearing property is
//! [`fused_width_equals_the_standalone_estimate_over_the_shape_corpus`]: the
//! width the accumulator charges from the cells it resolved for the BUILD must
//! equal the width `estimate_arrow_row_bytes` charges from its own
//! `values.get(name)` resolution, for every shape in the SHARED corpus. The two
//! share one charging core, so what this pins is the RESOLUTION — which is where
//! a fold like this can silently diverge (an absent column the transpose never
//! enumerates, a duplicate output column, a fan-out that fails closed).
//!
//! That the property is DISCRIMINATING rather than a tautology is proven by
//! [`the_equivalence_property_catches_a_fold_that_skips_absent_columns`], which
//! measures the width a fold that enumerated only the row's PRESENT entries would
//! report and shows it differs — so such an implementation fails the assertion
//! above rather than passing it.

use super::*;
use crate::export::arrow_shape_corpus::{col, row, shape_corpus, text};
use crate::export::{estimate_arrow_row_bytes, rows_to_record_batch};
use crate::schema::CqlType;
use crate::types::DataType;

/// Stage + commit every row of `shape`, asserting per row that the fused width
/// equals the standalone estimate. Returns the accumulator's batch.
fn stage_all<'c>(
    columns: &'c [ColumnInfo],
    rows: &[QueryRow],
    name: &str,
) -> ArrowRowAccumulator<'c> {
    let mut acc = ArrowRowAccumulator::with_capacity(columns, rows.len());
    for (i, r) in rows.iter().enumerate() {
        let expected = estimate_arrow_row_bytes(columns, r);
        let fused = acc.stage(r.clone());
        assert_eq!(
            fused, expected,
            "shape '{name}' row {i}: fused width {fused} != standalone estimate {expected}"
        );
        acc.commit();
    }
    assert_eq!(acc.len(), rows.len(), "shape '{name}': committed row count");
    acc
}

/// The fused push-time accounting reports EXACTLY the standalone estimate for
/// every row of every corpus shape — including the shapes with absent cells,
/// all-null rows, empty collections and deep nesting.
///
/// A divergence here is a divergence in the byte-cap's input, which would move
/// batch boundaries and (via issue #2821's reserve-before-materialize) the egress
/// reservation — the two things issue #3552 must leave bit-for-bit identical.
#[test]
fn fused_width_equals_the_standalone_estimate_over_the_shape_corpus() {
    let shapes = shape_corpus();
    assert!(!shapes.is_empty(), "the shared shape corpus is empty");
    for shape in &shapes {
        let acc = stage_all(&shape.columns, &shape.rows, shape.name);
        // Non-vacuity: the shape must have charged real bytes.
        assert!(
            acc.recomputed_payload() > 0,
            "shape '{}' charged zero bytes — vacuous",
            shape.name
        );
    }
}

/// The batch built from the ALREADY-transposed cells is identical to the batch
/// `rows_to_record_batch` builds from the rows (AC4: Arrow output unchanged).
///
/// `RecordBatch: PartialEq` compares the schema and every column's `ArrayData` —
/// buffers, offsets, validity and child data — so this is a byte-level identity
/// check, not a values-level one.
#[test]
fn the_columnar_batch_is_identical_to_the_row_built_batch() {
    for shape in shape_corpus() {
        let acc = stage_all(&shape.columns, &shape.rows, shape.name);
        let fused = acc
            .to_record_batch()
            .unwrap_or_else(|e| panic!("shape '{}' fused build failed: {e}", shape.name));
        let reference = rows_to_record_batch(&shape.columns, &shape.rows)
            .unwrap_or_else(|e| panic!("shape '{}' reference build failed: {e}", shape.name));
        assert_eq!(
            fused, reference,
            "shape '{}': the columnar batch differs from the row-built batch",
            shape.name
        );
    }
}

/// Columns for the absent-cell cases: three columns, one of which no row carries.
fn absent_columns() -> Vec<ColumnInfo> {
    vec![
        col("a", DataType::Integer, Some(CqlType::Int)),
        col("gone", DataType::Map, None),
        col("t", DataType::Text, Some(CqlType::Text)),
    ]
}

/// An ABSENT column is charged exactly as the standalone estimator charges it —
/// its validity byte, its shape's structural overhead and its per-column residual.
///
/// This is the one asymmetry between the estimator's enumeration (every projected
/// column, per row) and the transpose's (only the row's own entries): the
/// transpose never SEES an absent cell. The accumulator keeps the estimator's
/// enumeration, and this pins it — for a column absent from every row, and for a
/// column absent from SOME rows only.
#[test]
fn absent_columns_are_charged_exactly_as_the_standalone_estimate() {
    let columns = absent_columns();
    let rows = vec![
        row(vec![("a", Value::Integer(7)), ("t", text("hello"))]),
        // `t` absent as well as `gone`.
        row(vec![("a", Value::Integer(8))]),
        // explicit null is NOT absence — both must charge the same as the estimator
        row(vec![("a", Value::Null), ("t", Value::Null)]),
    ];
    let acc = stage_all(&columns, &rows, "absent columns");
    let fused = acc.to_record_batch().expect("fused build");
    let reference = rows_to_record_batch(&columns, &rows).expect("reference build");
    assert_eq!(fused, reference, "absent-column batch differs");
}

/// The equivalence property is DISCRIMINATING, not a tautology.
///
/// A fold that resolved cells the way `transpose_columns` does — enumerating only
/// the row's PRESENT entries — would never charge an absent column. That
/// deliberately-wrong width is measured here (as the estimate over just the
/// columns the row carries) and shown to be strictly SMALLER than the correct
/// one, so such an implementation fails
/// [`fused_width_equals_the_standalone_estimate_over_the_shape_corpus`] rather
/// than passing it. Verified by running this assertion.
#[test]
fn the_equivalence_property_catches_a_fold_that_skips_absent_columns() {
    let columns = absent_columns();
    let r = row(vec![("a", Value::Integer(7))]);

    let correct = estimate_arrow_row_bytes(&columns, &r);
    // What a present-cells-only fold would report: the same charge over only the
    // columns this row actually carries.
    let present_only: Vec<ColumnInfo> = columns
        .iter()
        .filter(|c| r.values.contains_key(c.name.as_str()))
        .cloned()
        .collect();
    assert_eq!(present_only.len(), 1, "the fixture must have absent columns");
    let skipping_absent = estimate_arrow_row_bytes(&present_only, &r);

    assert!(
        skipping_absent < correct,
        "a fold that skipped absent columns would report {skipping_absent}, not \
         distinguishable from the correct {correct} — this test could not catch it"
    );
    // And the accumulator reports the CORRECT one.
    let mut acc = ArrowRowAccumulator::new(&columns);
    assert_eq!(acc.stage(r), correct);
}

/// A cell whose fan-out exhausts the estimator's per-column leaf budget fails
/// closed to `usize::MAX` on BOTH accountings — the fused width cannot silently
/// report a finite number where the standalone estimate saturates (that would
/// under-reserve and defeat the byte-cap's fail-closed cut).
#[test]
fn a_saturating_fan_out_fails_closed_on_both_accountings() {
    let columns = vec![col(
        "l",
        DataType::List,
        Some(CqlType::List(Box::new(CqlType::Int))),
    )];
    // One leaf slot per element, one past MAX_ESTIMATE_LEAF_SLOTS.
    let items: Vec<Value> = (0..=(crate::export::MAX_ESTIMATE_LEAF_SLOTS as i32))
        .map(Value::Integer)
        .collect();
    let r = row(vec![("l", Value::List(items))]);

    let expected = estimate_arrow_row_bytes(&columns, &r);
    assert_eq!(
        expected,
        usize::MAX,
        "fixture does not saturate — the case is vacuous"
    );
    let mut acc = ArrowRowAccumulator::new(&columns);
    assert_eq!(acc.stage(r), usize::MAX, "fused width did not fail closed");
}

/// Duplicate output columns for one name (`SELECT a, a`) receive equal cells, and
/// the width matches the standalone estimate — which resolves the same name twice.
#[test]
fn duplicate_output_columns_match_the_standalone_estimate_and_replicate() {
    let columns = vec![
        col("a", DataType::Text, Some(CqlType::Text)),
        col("a", DataType::Text, Some(CqlType::Text)),
    ];
    let rows = vec![
        row(vec![("a", text("dup"))]),
        row(vec![("a", Value::Null)]),
        row(vec![]),
    ];
    let acc = stage_all(&columns, &rows, "duplicate names");
    let fused = acc.to_record_batch().expect("fused build");
    let reference = rows_to_record_batch(&columns, &rows).expect("reference build");
    assert_eq!(fused, reference, "duplicate-name batch differs");
}

/// A STAGED row survives a flush: `clear` empties the committed rows and leaves
/// the staged row to open the next batch — the test-then-push boundary the
/// byte-cap depends on (the crossing row is not lost and not double-counted).
#[test]
fn a_staged_row_survives_a_flush_and_opens_the_next_batch() {
    let columns = vec![col("a", DataType::Integer, Some(CqlType::Int))];
    let first = row(vec![("a", Value::Integer(1))]);
    let crossing = row(vec![("a", Value::Integer(2))]);

    let mut acc = ArrowRowAccumulator::with_capacity(&columns, 4);
    acc.stage(first.clone());
    acc.commit();
    // The crossing row is staged, then the batch is cut BEFORE it joins.
    acc.stage(crossing.clone());
    let batch = acc.to_record_batch().expect("first batch");
    assert_eq!(batch.num_rows(), 1, "the staged row must not be in the cut");
    assert_eq!(
        batch,
        rows_to_record_batch(&columns, &[first]).expect("reference"),
        "the cut batch must hold exactly the committed row"
    );
    acc.clear();
    assert!(acc.is_empty(), "clear must drop the committed rows");
    // The staged row is still there and opens the next batch.
    acc.commit();
    assert_eq!(acc.len(), 1);
    assert_eq!(
        acc.to_record_batch().expect("second batch"),
        rows_to_record_batch(&columns, &[crossing]).expect("reference"),
        "the staged row must open the next batch"
    );
}

/// `recomputed_payload` re-derives the committed rows' total from the STORED
/// cells and matches the sum of the per-row widths — the invariant a consumer
/// asserts its running accumulator against.
#[test]
fn recomputed_payload_matches_the_sum_of_the_row_widths() {
    for shape in shape_corpus() {
        let expected: usize = shape
            .rows
            .iter()
            .map(|r| estimate_arrow_row_bytes(&shape.columns, r))
            .fold(0usize, |a, b| a.saturating_add(b));
        let acc = stage_all(&shape.columns, &shape.rows, shape.name);
        assert_eq!(
            acc.recomputed_payload(),
            expected,
            "shape '{}': recomputed payload != Σ per-row widths",
            shape.name
        );
    }
}

/// A zero-column projection still counts its rows (the row count lives in the
/// accumulator, not in a column's length).
#[test]
fn a_zero_column_projection_still_counts_rows() {
    let columns: Vec<ColumnInfo> = Vec::new();
    let mut acc = ArrowRowAccumulator::new(&columns);
    for _ in 0..3 {
        assert_eq!(acc.stage(row(vec![("a", Value::Integer(1))])), 0);
        acc.commit();
    }
    assert_eq!(acc.len(), 3);
}
