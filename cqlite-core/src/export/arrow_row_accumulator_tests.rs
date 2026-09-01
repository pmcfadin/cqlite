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
use crate::export::{arrow_payload_bytes, estimate_arrow_row_bytes, rows_to_record_batch};
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
    assert_eq!(
        present_only.len(),
        1,
        "the fixture must have absent columns"
    );
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

/// A WIDE collection value projected twice (`SELECT a, a`) — legal CQL, and the
/// case where a per-duplicate deep clone would cost the most.
fn duplicate_name_fixture() -> (Vec<ColumnInfo>, Vec<QueryRow>) {
    let list_of_text = CqlType::List(Box::new(CqlType::Text));
    let columns = vec![
        col("a", DataType::List, Some(list_of_text.clone())),
        col("a", DataType::List, Some(list_of_text)),
    ];
    let wide = Value::List((0..64).map(|i| text(&format!("element-{i}"))).collect());
    let rows = vec![
        row(vec![("a", wide)]),
        row(vec![("a", Value::Null)]),
        row(vec![]), // absent from the row entirely
    ];
    (columns, rows)
}

/// Duplicate output columns for one name produce byte-identical output to the
/// pre-fold path, charge the same width as the standalone estimate (which resolves
/// the name once per column), and store the payload EXACTLY ONCE.
///
/// # This test fails if the deep clone is reintroduced
///
/// The fan-out is by reference: the value is moved into its name's CANONICAL slot
/// and every duplicate column reads that slot through `canonical`. All three ways
/// a clone could come back are caught here, which is the point — equal VALUES in
/// both slots (what this test used to assert) is true of a clone as well, so it
/// could not distinguish them:
///
/// * a clone into the duplicate's STAGING slot — caught by the `staged` assertions
///   between `stage` and `commit` (a clone there is transient, dropped at commit,
///   so no later observation can see it);
/// * a clone into the duplicate's CELL STORE — caught by `cells[1]` being empty;
/// * anything that changes the OUTPUT — caught by the batch equality.
#[test]
fn duplicate_output_columns_store_the_payload_once_and_match_the_pre_fold_batch() {
    let (columns, rows) = duplicate_name_fixture();

    // The canonical map: both output columns resolve to column 0's store.
    let mut acc = ArrowRowAccumulator::with_capacity(&columns, rows.len());
    assert_eq!(
        acc.canonical,
        vec![0, 0],
        "both same-named output columns must resolve to the FIRST one's store"
    );

    for (i, r) in rows.iter().enumerate() {
        let expected = estimate_arrow_row_bytes(&columns, r);
        let fused = acc.stage(r.clone());
        assert_eq!(
            fused, expected,
            "row {i}: a duplicated name must be CHARGED once per output column"
        );
        // The payload exists in ONE staging slot. A `value.clone()` into the
        // duplicate column would show up right here.
        assert!(
            acc.staged[1].is_none(),
            "row {i}: the duplicate output column must hold no copy of its own — \
             a deep clone per duplicate column allocates the payload N times, at \
             stage time, before `cut_before` admits the row (issue #3552 B3)"
        );
        assert_eq!(
            acc.staged[0].is_some(),
            !r.values.is_empty(),
            "row {i}: the canonical slot holds the value exactly when the row \
             carries one"
        );
        acc.commit();
    }

    // Storage: one store populated SPARSELY, the duplicate's never written at all.
    //
    // The fixture's three rows are: a wide list, an explicit `Value::Null`, and
    // the column ABSENT. Only the first two are PRESENT cells, so the store holds
    // TWO entries for three rows — an absent cell costs no slot (issue #3552 B4).
    // A dense store would hold three. The explicit null IS stored, which is what
    // keeps the absent/null distinction the builders depend on.
    assert_eq!(
        acc.cells[0].len(),
        2,
        "the store is SPARSE: 2 present cells over 3 rows, not one slot per row \
         (a dense store would retain n_cols x rows slots whatever the sparsity, \
         which neither cap bounds — issue #3552 B4)"
    );
    assert_eq!(
        acc.cells[0].iter().map(|(r, _)| *r).collect::<Vec<usize>>(),
        vec![0usize, 1],
        "present cells are stored with their row index, in ascending row order"
    );
    assert!(
        acc.cells[1].is_empty(),
        "the duplicate output column must have NO store of its own — a value \
         named twice is stored once and read twice (issue #3552 B3)"
    );

    // Output: byte-identical to the pre-fold path, which fanned out a `&Value`.
    let fused = acc.to_record_batch().expect("fused build");
    let reference = rows_to_record_batch(&columns, &rows).expect("reference build");
    assert_eq!(
        fused, reference,
        "the duplicate-name batch must be byte-identical to the pre-fold one"
    );
    // Non-vacuity: both duplicate columns really carry the wide list.
    assert_eq!(fused.num_columns(), 2);
    assert_eq!(fused.num_rows(), rows.len());
    assert_eq!(
        arrow_payload_bytes(&fused),
        arrow_payload_bytes(&reference),
        "identical batches must carry identical payload bytes"
    );
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

/// A zero-column projection: `len()` reports the staged rows, and the BATCH
/// cannot carry them.
///
/// # What this test asserts, and what it must not be read as proving
///
/// It does NOT prove zero-column projections work — it pins the opposite. With no
/// columns there is no array to carry a length, so `to_record_batch()` cannot
/// report `len()` rows: the row count is tracked in the accumulator and is LOST at
/// the batch boundary. The test asserts the property that actually matters for
/// issue #3552 (AC4, output unchanged): the fused path and the pre-fold
/// `rows_to_record_batch` behave IDENTICALLY on this input — same `Ok`/`Err`, and
/// equal batches when `Ok` — because both end in the same
/// `RecordBatch::try_new(schema, arrays)` over an empty array list. The
/// disagreement arm is the live assertion: it fires if either path is ever
/// "fixed" on its own.
///
/// This behaviour is PRE-EXISTING, not introduced by the fold: `origin/main`'s
/// `rows_to_record_batch` ends in the identical `try_new` with no explicit row
/// count and `convert_to_arrays` returns an empty vec for zero columns. Changing
/// it here would change Arrow output for this case inside a behaviour-preserving
/// refactor, so it is deliberately NOT changed. **Whether a zero-column
/// projection is reachable at all on the `do_get`/streaming path is UNRESOLVED** —
/// this is neither a known-harmless case nor a known-live one. Issue #3742 owns
/// both that question and the behaviour.
///
/// The exact terminal behaviour of `try_new` on an empty array list (a zero-row
/// batch, or an `Err`) is deliberately not hard-coded beyond the `Ok` arm, because
/// it is arrow's, not this crate's, and both outcomes are equally consistent with
/// the property under test.
#[test]
fn a_zero_column_projection_tracks_rows_that_its_batch_cannot_carry() {
    let columns: Vec<ColumnInfo> = Vec::new();
    let rows: Vec<QueryRow> = (0..3)
        .map(|_| row(vec![("a", Value::Integer(1))]))
        .collect();

    let mut acc = ArrowRowAccumulator::new(&columns);
    for r in &rows {
        // Every column is absent from a zero-column projection, so there is
        // nothing to charge: the width is 0, matching the standalone estimator.
        let expected = estimate_arrow_row_bytes(&columns, r);
        let fused = acc.stage(r.clone());
        assert_eq!(fused, expected);
        assert_eq!(fused, 0);
        acc.commit();
    }
    // The accumulator DOES track the rows...
    assert_eq!(acc.len(), 3, "len() reports the committed rows");

    // ...and the batch does NOT, identically on both paths (issue #3742).
    match (acc.to_record_batch(), rows_to_record_batch(&columns, &rows)) {
        (Ok(fused), Ok(reference)) => {
            assert_eq!(
                fused, reference,
                "the fused and pre-fold zero-column batches must be identical"
            );
            assert_eq!(
                fused.num_rows(),
                0,
                "an empty array list carries no length, so the batch reports 0 rows"
            );
            assert_ne!(
                fused.num_rows(),
                acc.len(),
                "the tracked count is LOST at the batch boundary — this test pins \
                 that, it does not endorse it (issue #3742)"
            );
        }
        // Both paths REFUSING is equally consistent: the property under test is
        // that they agree, not which way arrow decides.
        (Err(_), Err(_)) => {}
        (fused, reference) => panic!(
            "the fused and pre-fold zero-column paths DISAGREE — fused ok={}, \
             reference ok={} (issue #3742)",
            fused.is_ok(),
            reference.is_ok()
        ),
    }
}

/// `clear` retains capacity, and it retains it **per column** — so across batches
/// whose dense column MOVES, the resident total converges on the SUM of per-column
/// high-water marks rather than the high-water mark of the sum. Rotating one dense
/// column per batch therefore reached the full dense `n_cols × rows_per_batch`
/// residency that the sparse store exists to avoid, and NEITHER cap bounds it: the
/// byte cap bounds a batch's PAYLOAD and says nothing about what survives between
/// batches (issue #3552, roborev round 6).
///
/// Two comments in `arrow_row_accumulator.rs` asserted the opposite — "the store
/// reaches its steady state after one batch regardless" — which holds only for a
/// STABLE density pattern. This is the case that falsifies it.
#[test]
fn rotating_density_does_not_accumulate_per_column_capacity() {
    const N_COLS: usize = 48;
    const ROWS_PER_BATCH: usize = 96;

    let names: Vec<String> = (0..N_COLS).map(|i| format!("c{i}")).collect();
    let columns: Vec<ColumnInfo> = names
        .iter()
        .map(|n| col(n, DataType::Text, Some(CqlType::Text)))
        .collect();

    let mut acc = ArrowRowAccumulator::new(&columns);
    for (dense, dense_name) in names.iter().enumerate() {
        // Every row of THIS batch carries exactly one present cell, in column
        // `dense` — so this batch's store for `dense` grows to ROWS_PER_BATCH while
        // every other store stays empty. Next batch moves to the next column.
        for r in 0..ROWS_PER_BATCH {
            acc.stage(row(vec![(
                dense_name.as_str(),
                text(&format!("b{dense}r{r}")),
            )]));
            acc.commit();
        }
        assert_eq!(acc.len(), ROWS_PER_BATCH, "batch {dense}: committed rows");
        acc.clear();
        assert_eq!(acc.len(), 0, "batch {dense}: cleared");
    }

    let retained = acc.retained_cell_slots();

    // DISCRIMINATING, not a tautology: without the trim each of the N_COLS stores
    // keeps its own ROWS_PER_BATCH high-water mark, so the retained total is the full
    // dense product. Asserting against that computed value means this test fails if
    // the trim is removed, rather than passing for any implementation.
    let unbounded = N_COLS * ROWS_PER_BATCH;
    assert!(
        retained < unbounded,
        "retained {retained} slots after rotating density across {N_COLS} columns; \
         without the per-column trim this reaches the dense product {unbounded}"
    );

    // And the bound is the DOCUMENTED one, not merely "less than dense": a batch's
    // own peak is ROWS_PER_BATCH present cells, so the allowance is
    // max(peak × SLACK, FLOOR), plus at most one warm slot per column.
    let allowance = (ROWS_PER_BATCH * 2).max(1024) + N_COLS;
    assert!(
        retained <= allowance,
        "retained {retained} slots exceeds the documented allowance {allowance}"
    );
}

/// A STABLY dense column must stay warm across batches — the trim must not shrink it
/// every batch and force a reallocation on the next one.
///
/// The defect this pins (issue #3552, roborev round 12): the first trim divided the
/// allowance into EQUAL per-column shares. That bounds the total correctly, but once
/// the inactive stores hold their shares, a column that is stably dense exceeds its
/// own share on every batch and is shrunk on every batch — permanent allocation churn
/// in the steady state, which is the opposite of what retaining capacity is for.
///
/// Deliberately NOT a timing test: churn is observable structurally as capacity
/// collapsing between batches, and a wall-clock threshold in a correctness path is a
/// mechanized `roborev-lints` failure (#2642).
#[test]
fn a_stably_dense_column_stays_warm_across_batches() {
    const N_COLS: usize = 48;
    const ROWS_PER_BATCH: usize = 96;

    let names: Vec<String> = (0..N_COLS).map(|i| format!("c{i}")).collect();
    let columns: Vec<ColumnInfo> = names
        .iter()
        .map(|n| col(n, DataType::Text, Some(CqlType::Text)))
        .collect();

    let mut acc = ArrowRowAccumulator::new(&columns);

    // First, move density across many columns so the stores collectively hold enough
    // retained capacity to put the total over its allowance — the state in which the
    // equal-share trim began churning.
    for (dense, dense_name) in names.iter().enumerate() {
        for r in 0..ROWS_PER_BATCH {
            acc.stage(row(vec![(
                dense_name.as_str(),
                text(&format!("warm{dense}r{r}")),
            )]));
            acc.commit();
        }
        acc.clear();
    }

    // Now hold ONE column stably dense and watch its retained capacity across batches.
    let stable = names[0].as_str();
    let mut retained_after = Vec::new();
    for batch in 0..4 {
        for r in 0..ROWS_PER_BATCH {
            acc.stage(row(vec![(stable, text(&format!("s{batch}r{r}")))]));
            acc.commit();
        }
        acc.clear();
        // THE COLUMN'S OWN capacity, not the total. The total cannot express this
        // property: an equal-share trim holds the TOTAL at its allowance precisely
        // BY shrinking whichever column is active, so a total-based assertion passes
        // for the churning implementation. Verified by mutant run.
        retained_after.push(acc.retained_cell_slots_for(0));
    }

    // Under the equal-share trim the stable column was shrunk to allowance/N_COLS (21
    // slots) after EVERY batch, so this sequence collapsed to ~21 and stayed there
    // while the column kept needing 96. Usage-proportional keeps it warm.
    let last = *retained_after.last().expect("four batches recorded");
    assert!(
        last >= ROWS_PER_BATCH,
        "a stably dense column was shrunk below its own steady-state need: retained \
         {retained_after:?} across four identical batches, but each needed \
         {ROWS_PER_BATCH} slots — the trim is churning"
    );

    // And the total bound still holds: this must not be a warmth-for-unboundedness trade.
    let allowance = (ROWS_PER_BATCH * 2).max(1024) + N_COLS;
    assert!(
        acc.retained_cell_slots() <= allowance,
        "total retained {} exceeds the documented allowance {allowance} — warmth must \
         not be bought with unboundedness",
        acc.retained_cell_slots()
    );
}
