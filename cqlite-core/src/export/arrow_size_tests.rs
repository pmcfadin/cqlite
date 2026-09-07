//! Tests for the conservative Arrow payload-byte estimator (issue #2825).
//!
//! Loaded via `#[path]` from `arrow_size.rs` so the production module stays
//! under the campsite file-size threshold (epic #1116).
//!
//! The load-bearing test here is [`estimate_is_conservative_across_shape_corpus`]:
//! it asserts `Σ estimate_arrow_row_bytes(..) >= arrow_payload_bytes(batch)` over
//! a corpus of row shapes. A future CQL type that `rows_to_record_batch` learns
//! to convert but the estimator does not model FAILS that test rather than
//! silently under-counting (spec Requirement 3).

use super::*;
// The row-shape corpus and its builders are SHARED with `cqlite-flight`'s
// published-capacity-bound guard (issue #2932) — see
// `crate::export::arrow_shape_corpus`. Keeping one corpus is what stops the two
// contracts (estimator conservatism here, the fail-closed capacity reservation
// there) from being validated against different shape sets.
use crate::export::arrow_shape_corpus::{blob, col, row, shape_corpus, text};
use crate::export::rows_to_record_batch;
use crate::query::{ColumnInfo, QueryRow};
use crate::schema::CqlType;
use crate::types::{DataType, Value};

// ---------------------------------------------------------------------------
// Requirement 3: the estimator is conservative across the shape corpus
// ---------------------------------------------------------------------------

/// `Σ estimate_arrow_row_bytes(..) >= arrow_payload_bytes(rows_to_record_batch(..))`
/// for every shape in the corpus — the conservatism contract.
///
/// A CQL type the converter handles but the estimator does not model shows up
/// here as an under-count and FAILS, so a future type addition cannot silently
/// introduce one (spec Requirement 3).
#[test]
fn estimate_is_conservative_across_shape_corpus() {
    for shape in shape_corpus() {
        let estimated: usize = shape
            .rows
            .iter()
            .map(|r| estimate_arrow_row_bytes(&shape.columns, r))
            .fold(0usize, |a, b| a.saturating_add(b));
        let batch = rows_to_record_batch(&shape.columns, &shape.rows)
            .unwrap_or_else(|e| panic!("shape '{}' failed to convert: {e}", shape.name));
        let realized = arrow_payload_bytes(&batch);
        assert!(
            estimated >= realized,
            "shape '{}': estimate {estimated} UNDER-COUNTS realized payload {realized}",
            shape.name
        );
        // Non-vacuity: the corpus must exercise real bytes, not empty batches.
        assert!(
            realized > 0 && batch.num_rows() == shape.rows.len(),
            "shape '{}' is vacuous: {realized} payload bytes, {} rows",
            shape.name,
            batch.num_rows()
        );
    }
}

/// The estimator must not be so loose that it is useless: it stays within a
/// bounded multiple of the realized payload, so the cap cuts batches near the
/// configured size rather than far short of it.
///
/// Covers the COLLECTION-heavy and MULTI-COLUMN shapes as well as the wide ones
/// (review B3): those are exactly where a slack charged per SLOT instead of per
/// COLUMN inflates the estimate — a `list<int>` cell of 1000 elements or a
/// 30-column fixed-width row is where an over-estimate turns into a real
/// batching regression, so the looseness is measured there, not only where it
/// amortizes away.
#[test]
fn estimate_is_within_a_bounded_multiple_of_the_payload() {
    // (shape, allowed multiple). The multiples are TIGHT — each is the smallest
    // whole number above the shape's measured ratio — so a regression in the
    // charging model shows up here rather than being absorbed.
    let cases: &[(&str, usize)] = &[
        ("text wide", 2),
        ("blob wide", 2),
        ("blob single row", 2),
        // Collection-heavy: the per-element charge is what must stay tight.
        ("list<int>", 2),
        ("set<text>", 2),
        ("map<text,bigint>", 2),
        ("map<text,list<text>> nested", 3),
        ("flat list rendered", 3),
        // Multi-column narrow: these are where a residual charged for a column
        // that has no childless array node shows up (review C1). Both are now
        // pinned at the smallest whole number above their measured ratio
        // (1.18 and 2.55), so re-introducing the residual for a fixed-width or
        // single-node column FAILS here.
        ("fixed-width scalars", 2),
        ("flat text/blob/int", 2),
        ("flat json rendered", 5),
        ("high-fidelity scalars", 3),
    ];
    for (name, multiple) in cases {
        let shape = shape_corpus()
            .into_iter()
            .find(|s| &s.name == name)
            .unwrap_or_else(|| panic!("missing corpus shape '{name}'"));
        let estimated: usize = shape
            .rows
            .iter()
            .map(|r| estimate_arrow_row_bytes(&shape.columns, r))
            .sum();
        let batch = rows_to_record_batch(&shape.columns, &shape.rows).expect("convert");
        let realized = arrow_payload_bytes(&batch);
        assert!(
            estimated <= realized.saturating_mul(*multiple),
            "shape '{name}': estimate {estimated} is more than {multiple}x the \
             realized payload {realized}"
        );
    }
}

/// The per-column residual is charged ONCE PER COLUMN, never per cell (review
/// B3): growing a collection cell's ELEMENT COUNT by 100x must grow the estimate
/// by roughly the elements' own Arrow cost, not by 100 slack charges.
///
/// Pinned as a ratio against the realized payload so it cannot be satisfied by
/// simply shrinking a constant: a per-slot slack of `S` would show up here as
/// `~S/4` extra bytes per `int` element.
#[test]
fn per_column_residual_does_not_scale_with_element_count() {
    let columns = vec![col(
        "l",
        DataType::List,
        Some(CqlType::List(Box::new(CqlType::Int))),
    )];
    let big = row(vec![(
        "l",
        Value::List((0..1000).map(Value::Integer).collect()),
    )]);
    let estimated = estimate_arrow_row_bytes(&columns, &big);
    let batch = rows_to_record_batch(&columns, std::slice::from_ref(&big)).expect("convert");
    let realized = arrow_payload_bytes(&batch);
    assert!(realized >= 4000, "vacuous: {realized} realized bytes");
    assert!(
        estimated >= realized,
        "estimate {estimated} under-counts {realized}"
    );
    // 1000 int elements: ~4 KB realized. A per-SLOT slack of 32 would put this
    // at ~9x.
    assert!(
        estimated <= realized.saturating_mul(3) / 2,
        "estimate {estimated} is more than 1.5x the realized payload {realized} \
         — the residual is scaling with element count, not column count"
    );
}

/// A wide fixed-width schema still lets the ROW-cap bind at the 4 MiB default.
///
/// Placed PAST the predicted break point, not before it (review C1): charging
/// the per-column residual for a fixed-width column cost 13 B/column/row and put
/// the byte-cap's binding point at 40 `int` columns — so a 30-column guard sat
/// just under the cliff and reported green while an ordinary 40–100-column
/// `int` table was already being cut at ~1.3 MB of real payload. A fixed-width
/// column now costs 5 B/row (`1` validity + `4` content), which keeps the
/// row-cap binding through 102 columns; both sizes below are above the OLD
/// cliff, and 100 is the largest ordinary-schema width the issue names.
#[test]
fn a_wide_fixed_width_row_still_fits_a_full_default_batch() {
    const DEFAULT_CAP: usize = 4 * 1024 * 1024;
    const BATCH_ROWS: usize = 8192;
    for n_cols in [64i32, 100i32] {
        let names: Vec<String> = (0..n_cols).map(|i| format!("c{i}")).collect();
        let columns: Vec<ColumnInfo> = names
            .iter()
            .map(|n| col(n, DataType::Integer, Some(CqlType::Int)))
            .collect();
        let r = row(names
            .iter()
            .zip(0..n_cols)
            .map(|(n, i)| (n.as_str(), Value::Integer(i)))
            .collect());
        let per_row = estimate_arrow_row_bytes(&columns, &r);
        assert!(
            per_row.saturating_mul(BATCH_ROWS) <= DEFAULT_CAP,
            "a {n_cols}-column int row estimates {per_row} B, so the byte-cap \
             would cut at {} rows — below the {BATCH_ROWS}-row batch size, a \
             throughput regression on a narrow shape",
            DEFAULT_CAP / per_row.max(1)
        );
        // And the estimate is still an UPPER bound on what such a batch really
        // costs, so the headroom above is real rather than an under-count.
        let rows: Vec<QueryRow> = (0..64).map(|_| r.clone()).collect();
        let batch = rows_to_record_batch(&columns, &rows).expect("convert");
        assert!(
            per_row.saturating_mul(rows.len()) >= arrow_payload_bytes(&batch),
            "{n_cols}-column int row under-counts the realized payload"
        );
    }
}

/// The conservatism property is DISCRIMINATING, not a tautology.
///
/// Spec Requirement 3 asks that a column type the converter handles but the
/// estimator does not model FAIL the property test. Two mechanisms enforce that
/// here. First, `column_shape`/`charge_cql`/`charge_flat` match [`CqlType`] and
/// [`DataType`] EXHAUSTIVELY with no wildcard arm, so a newly added variant is a
/// *compile* error before it can ever be under-counted. Second — proven below —
/// an estimator that models a type's CONTENT but forgets its Arrow structural
/// overhead (the exact failure mode of `Value::size_estimate`,
/// `memory::estimate_value_size` and `Memtable::estimate_value_size`, and the
/// likeliest shape of a careless future arm) UNDER-counts real corpus shapes and
/// so trips the assertion.
#[test]
fn the_conservatism_property_catches_a_content_only_estimator() {
    /// A deliberately unmodelled estimator: raw content bytes, zero Arrow
    /// structural overhead. Recursion is fine here — the corpus is shallow and
    /// this is test-only scaffolding, not the production walk.
    fn content_only(v: &Value) -> usize {
        match v {
            // `Value::Empty` renders as `""` and carries no content bytes
            // (issue #3805) — same as `Null` for this deliberately
            // content-only estimator.
            Value::Null | Value::Tombstone(_) | Value::Empty(_) => 0,
            Value::Boolean(_) | Value::TinyInt(_) => 1,
            Value::SmallInt(_) => 2,
            Value::Integer(_) | Value::Float32(_) | Value::Date(_) => 4,
            Value::BigInt(_)
            | Value::Counter(_)
            | Value::Float(_)
            | Value::Timestamp(_)
            | Value::Time(_) => 8,
            Value::Uuid(_) => 16,
            Value::Duration { .. } => 16,
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
            Value::Varint(b) => b.len(),
            Value::Inet(b) => b.len(),
            Value::Decimal { unscaled, .. } => unscaled.len(),
            Value::Json(j) => j.to_string().len(),
            Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
                items.iter().map(content_only).sum()
            }
            Value::Map(pairs) => pairs
                .iter()
                .map(|(k, v)| content_only(k) + content_only(v))
                .sum(),
            Value::Udt(u) => u
                .fields
                .iter()
                .filter_map(|f| f.value.as_ref())
                .map(content_only)
                .sum(),
            Value::Frozen(inner) => content_only(inner),
        }
    }

    let mut under_counted: Vec<&str> = Vec::new();
    for shape in shape_corpus() {
        let naive: usize = shape
            .rows
            .iter()
            .map(|r| {
                shape
                    .columns
                    .iter()
                    .filter_map(|c| r.values.get(c.name.as_str()))
                    .map(content_only)
                    .sum::<usize>()
            })
            .sum();
        let batch = rows_to_record_batch(&shape.columns, &shape.rows).expect("convert");
        if naive < arrow_payload_bytes(&batch) {
            under_counted.push(shape.name);
        }
    }
    assert!(
        !under_counted.is_empty(),
        "a content-only estimator under-counted NOTHING — the conservatism \
         property test cannot detect an unmodelled type and is a tautology"
    );
    // And the real estimator covers every one of those same shapes (the
    // property test above asserts this for the whole corpus).
    for name in &under_counted {
        let shape = shape_corpus()
            .into_iter()
            .find(|s| &s.name == name)
            .unwrap_or_else(|| panic!("missing shape '{name}'"));
        let estimated: usize = shape
            .rows
            .iter()
            .map(|r| estimate_arrow_row_bytes(&shape.columns, r))
            .sum();
        let batch = rows_to_record_batch(&shape.columns, &shape.rows).expect("convert");
        assert!(
            estimated >= arrow_payload_bytes(&batch),
            "shape '{name}' under-counted by the REAL estimator"
        );
    }
}

// ---------------------------------------------------------------------------
// Requirement 3: width sensitivity
// ---------------------------------------------------------------------------

/// Two rows differing only in one blob cell's length differ in estimate by at
/// least that content-byte difference — the estimator is width-driven, not a
/// per-row constant.
#[test]
fn variable_width_content_drives_the_estimate() {
    let columns = vec![col("b", DataType::Blob, Some(CqlType::Blob))];
    let small = row(vec![("b", blob(16))]);
    let large = row(vec![("b", blob(64 * 1024))]);
    let d = estimate_arrow_row_bytes(&columns, &large) - estimate_arrow_row_bytes(&columns, &small);
    assert!(
        d >= 64 * 1024 - 16,
        "estimate difference {d} is below the content difference"
    );
}

/// A `Value::Text` NESTED in a rendered container is charged for the worst-case
/// `String::from_utf8_lossy` expansion (3 bytes per input byte), while a
/// TOP-LEVEL text cell is charged exactly (review B5).
///
/// Both `build_string_array` branches borrow a top-level `Value::Text` after a
/// non-lossy `str::from_utf8` and hard-error on invalid UTF-8, so `s.len()` is
/// exact there. A nested one reaches `ValueFormatter::format_value`, which uses
/// `from_utf8_lossy` — each invalid byte becomes a 3-byte U+FFFD. The estimator
/// must not depend on the issue-#1644 "text is UTF-8-validated at construction"
/// invariant, which the type does not enforce.
#[test]
fn nested_rendered_text_is_charged_for_lossy_utf8_expansion() {
    const N: usize = 64;
    let top = vec![col("t", DataType::Text, None)];
    let nested = vec![col("l", DataType::List, None)];
    let flat_row = row(vec![("t", text(&"a".repeat(N)))]);
    let nested_row = row(vec![("l", Value::List(vec![text(&"a".repeat(N))]))]);

    let top_estimate = estimate_arrow_row_bytes(&top, &flat_row);
    let nested_estimate = estimate_arrow_row_bytes(&nested, &nested_row);
    // Top-level: exact, so the estimate stays close to the content length.
    assert!(
        top_estimate < N * 2,
        "top-level text over-charged: {top_estimate} for {N} content bytes"
    );
    // Nested: at least 3x the content, covering the U+FFFD expansion.
    assert!(
        nested_estimate >= N * 3,
        "nested rendered text charged {nested_estimate} for {N} content bytes — \
         below the 3x from_utf8_lossy worst case"
    );

    // And the property still holds against the real converter for a value whose
    // bytes are NOT valid UTF-8 — the case the expansion exists for.
    let invalid = Value::List(vec![Value::Text(vec![0xFFu8; N].into())]);
    let invalid_row = row(vec![("l", invalid)]);
    let estimated = estimate_arrow_row_bytes(&nested, &invalid_row);
    let batch = rows_to_record_batch(&nested, std::slice::from_ref(&invalid_row)).expect("convert");
    let realized = arrow_payload_bytes(&batch);
    assert!(
        realized >= N * 3,
        "the converter did not expand the invalid bytes ({realized} for {N}) — \
         the fixture no longer exercises lossy rendering"
    );
    assert!(
        estimated >= realized,
        "estimate {estimated} under-counts the lossy-expanded payload {realized}"
    );
}

/// The same holds for `text`, and through the flat (untyped) dispatch.
#[test]
fn text_width_drives_the_estimate_on_both_dispatch_paths() {
    for cql in [Some(CqlType::Text), None] {
        let columns = vec![col("t", DataType::Text, cql.clone())];
        let small = row(vec![("t", text("ab"))]);
        let large = row(vec![("t", text(&"a".repeat(10_000)))]);
        let d =
            estimate_arrow_row_bytes(&columns, &large) - estimate_arrow_row_bytes(&columns, &small);
        assert!(
            d >= 10_000 - 2,
            "estimate difference {d} too small for {cql:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Requirement 3: pathological values fail closed
// ---------------------------------------------------------------------------

/// `list<frozen<list<int>>>` whose CONTAINER fan-out exceeds the structural node
/// budget saturates instead of spinning — no panic, no unbounded work, and the
/// returned width trips the cap.
///
/// Container elements are what the branching budget counts (review C2): each
/// inner list can itself fan out, so each spends a node and enters the worklist.
fn nested_list_of_lists(n: usize) -> Value {
    Value::List((0..n).map(|_| Value::List(Vec::new())).collect())
}

#[test]
fn oversized_container_fanout_fails_closed_to_a_saturated_width() {
    let columns = vec![col(
        "l",
        DataType::List,
        Some(CqlType::List(Box::new(CqlType::List(Box::new(
            CqlType::Int,
        ))))),
    )];
    let r = row(vec![("l", nested_list_of_lists(MAX_ESTIMATE_NODES + 1))]);
    assert_eq!(estimate_arrow_row_bytes(&columns, &r), usize::MAX);
}

/// A LEAF fan-out past [`MAX_ESTIMATE_LEAF_SLOTS`] also fails closed: leaves cost
/// no worklist entry and no structural node, but the linear-work bound still
/// holds, so a `Value` tree far larger than any decoded Cassandra row terminates.
#[test]
fn oversized_leaf_fanout_fails_closed_to_a_saturated_width() {
    let columns = vec![col(
        "l",
        DataType::List,
        Some(CqlType::List(Box::new(CqlType::Int))),
    )];
    let huge = Value::List(
        (0..(MAX_ESTIMATE_LEAF_SLOTS + 1) as i32)
            .map(Value::Integer)
            .collect(),
    );
    let r = row(vec![("l", huge)]);
    assert_eq!(estimate_arrow_row_bytes(&columns, &r), usize::MAX);
}

/// A value nested far deeper than any schema still terminates, returns a
/// saturated width, and never recurses (the walk is an explicit worklist — a
/// recursive one would blow the stack here instead of returning).
#[test]
fn deeply_nested_value_fails_closed_without_recursion() {
    let mut v = Value::Integer(1);
    for _ in 0..(MAX_ESTIMATE_NODES + 16) {
        v = Value::List(vec![v]);
    }
    let columns = vec![col("l", DataType::List, None)];
    let r = row(vec![("l", v)]);
    assert_eq!(estimate_arrow_row_bytes(&columns, &r), usize::MAX);
    // `Value`'s derived `Drop` IS recursive, so releasing a 65k-deep chain would
    // overflow the stack in the harness (not in the estimator, which already
    // returned). Leak the fixture rather than weaken the depth under test.
    std::mem::forget(r);
}

/// A row that mixes a saturating cell with ordinary ones still reports
/// `usize::MAX` — repeated additions of the fail-closed sentinel stay saturated
/// rather than wrapping to a small (and therefore cap-defeating) number.
#[test]
fn saturating_arithmetic_never_wraps() {
    let columns = vec![
        col("b0", DataType::Blob, Some(CqlType::Blob)),
        col(
            "l1",
            DataType::List,
            Some(CqlType::List(Box::new(CqlType::List(Box::new(
                CqlType::Int,
            ))))),
        ),
        col("b2", DataType::Blob, Some(CqlType::Blob)),
        col("b3", DataType::Blob, Some(CqlType::Blob)),
    ];
    let r = row(vec![
        ("b0", blob(8)),
        ("l1", nested_list_of_lists(MAX_ESTIMATE_NODES + 1)),
        ("b2", blob(8)),
        ("b3", blob(8)),
    ]);
    assert_eq!(estimate_arrow_row_bytes(&columns, &r), usize::MAX);
}

/// Wide-but-LEGAL collections are estimated exactly rather than failing closed
/// (review C2).
///
/// Before the per-column, leaf-exempt budgets, a single non-frozen collection
/// near Cassandra's classic 65,535-element limit — or a few thousand elements
/// across ~20 columns — exhausted one shared 65,536-node row budget and pinned
/// the row's width at `usize::MAX`. `cut_before` then fired for EVERY subsequent
/// row, so the stream degraded to one row per batch indefinitely on an ordinary
/// (if ill-advised) schema. Each shape below exceeded that old budget.
#[test]
fn wide_legal_collections_do_not_fail_closed_and_still_bound_the_payload() {
    const DEFAULT_CAP: usize = 4 * 1024 * 1024;

    // (name, columns, one row) — each over the OLD 65,536-node ROW budget.
    let one_near_limit_map = {
        let columns = vec![col(
            "m",
            DataType::Map,
            Some(CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::Int),
            )),
        )];
        // 40,000 entries = 80,000 slots under the old accounting.
        let r = row(vec![(
            "m",
            Value::Map(
                (0..40_000i32)
                    .map(|j| (text(&format!("k{j}")), Value::Integer(j)))
                    .collect(),
            ),
        )]);
        ("one near-limit map<text,int>", columns, r)
    };
    let twenty_collection_columns = {
        let names: Vec<String> = (0..20).map(|i| format!("l{i}")).collect();
        let columns: Vec<ColumnInfo> = names
            .iter()
            .map(|n| {
                col(
                    n,
                    DataType::List,
                    Some(CqlType::List(Box::new(CqlType::Int))),
                )
            })
            .collect();
        // 20 x 3,500 = 70,000 slots under the old accounting.
        let r = row(names
            .iter()
            .map(|n| {
                (
                    n.as_str(),
                    Value::List((0..3_500i32).map(Value::Integer).collect()),
                )
            })
            .collect());
        ("20 x list<int> of 3,500", columns, r)
    };

    for (name, columns, r) in [one_near_limit_map, twenty_collection_columns] {
        let per_row = estimate_arrow_row_bytes(&columns, &r);
        assert!(
            per_row < usize::MAX,
            "shape '{name}': the estimate failed closed on a LEGAL collection \
             width, which degrades the stream to one row per batch"
        );
        // Real payload allows several rows per 4 MiB batch, so the byte-cap must
        // let several rows in — the throughput property the cliff destroyed.
        let rows_per_batch = DEFAULT_CAP / per_row.max(1);
        assert!(
            rows_per_batch > 1,
            "shape '{name}': {per_row} B/row admits only {rows_per_batch} row(s) \
             per {DEFAULT_CAP}-byte batch"
        );
        // And the estimate is still an UPPER bound on the realized payload: the
        // cliff is fixed by counting correctly, not by under-counting.
        let rows: Vec<QueryRow> = (0..3).map(|_| r.clone()).collect();
        let batch = rows_to_record_batch(&columns, &rows).expect("convert");
        let realized = arrow_payload_bytes(&batch);
        assert!(
            realized > 100_000,
            "shape '{name}' is vacuous: {realized} B"
        );
        assert!(
            per_row.saturating_mul(rows.len()) >= realized,
            "shape '{name}': estimate {per_row}/row UNDER-COUNTS realized \
             payload {realized} over {} rows",
            rows.len()
        );
    }
}

/// One wide column no longer starves the columns after it: the budgets are per
/// COLUMN, so a row whose first column consumes a large share still estimates
/// its remaining columns exactly.
#[test]
fn the_node_budget_is_per_column_not_per_row() {
    let wide = || {
        col(
            "w",
            DataType::List,
            Some(CqlType::List(Box::new(CqlType::Int))),
        )
    };
    let names: Vec<String> = (0..8).map(|i| format!("w{i}")).collect();
    let columns: Vec<ColumnInfo> = names
        .iter()
        .map(|n| ColumnInfo {
            name: n.clone(),
            ..wide()
        })
        .collect();
    // 8 x 30,000 = 240,000 slots — 3.6x the old shared ROW budget.
    let r = row(names
        .iter()
        .map(|n| {
            (
                n.as_str(),
                Value::List((0..30_000i32).map(Value::Integer).collect()),
            )
        })
        .collect());
    let per_row = estimate_arrow_row_bytes(&columns, &r);
    assert!(
        per_row < usize::MAX,
        "a per-ROW budget starved the tail columns"
    );
    let batch = rows_to_record_batch(&columns, std::slice::from_ref(&r)).expect("convert");
    let realized = arrow_payload_bytes(&batch);
    assert!(
        per_row >= realized,
        "estimate {per_row} under-counts realized payload {realized}"
    );
}

/// A `Frozen` chain deeper than the unwrap bound terminates without panicking.
#[test]
fn deep_frozen_chain_terminates() {
    let mut v = Value::Integer(1);
    for _ in 0..64 {
        v = Value::Frozen(Box::new(v));
    }
    let columns = vec![col("f", DataType::Integer, Some(CqlType::Int))];
    let r = row(vec![("f", v)]);
    // Terminates and returns a finite, non-zero width.
    let e = estimate_arrow_row_bytes(&columns, &r);
    assert!(e > 0 && e < usize::MAX);
}

// ---------------------------------------------------------------------------
// Projection / cell-resolution semantics
// ---------------------------------------------------------------------------

/// Only projected columns are charged: a value present in the row but absent
/// from `columns` never reaches the batch, so it must never reach the estimate.
#[test]
fn unprojected_values_are_not_charged() {
    let projected = vec![col("t", DataType::Text, Some(CqlType::Text))];
    let lean = row(vec![("t", text("abc"))]);
    let fat = row(vec![("t", text("abc")), ("other", blob(1_000_000))]);
    assert_eq!(
        estimate_arrow_row_bytes(&projected, &lean),
        estimate_arrow_row_bytes(&projected, &fat)
    );
}

/// An empty projection costs nothing.
#[test]
fn empty_projection_is_zero() {
    assert_eq!(
        estimate_arrow_row_bytes(&[], &row(vec![("t", text("x"))])),
        0
    );
}

// ---------------------------------------------------------------------------
// The payload oracle itself
// ---------------------------------------------------------------------------

/// `arrow_payload_bytes` counts buffer LENGTHS, so it is strictly at or below
/// `get_array_memory_size()` (which counts capacity) — the two currencies the
/// byte-cap keeps separate.
#[test]
fn payload_bytes_never_exceeds_reported_memory_size() {
    for shape in shape_corpus() {
        let batch = rows_to_record_batch(&shape.columns, &shape.rows).expect("convert");
        let payload = arrow_payload_bytes(&batch);
        let capacity = batch.get_array_memory_size();
        assert!(
            payload <= capacity,
            "shape '{}': payload {payload} exceeds reported memory {capacity}",
            shape.name
        );
    }
}
