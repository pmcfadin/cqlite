//! Tests for the dual row-cap / byte-cap Arrow egress batch boundary
//! (issue #2825).
//!
//! Loaded via `#[path]` from `batch_bytes.rs` (the `admission.rs` /
//! `admission_tests.rs` precedent) so the production module stays under the
//! campsite file-size threshold, and so these tests never land in the
//! already-over-threshold `streaming_tests.rs` (epic #1116/#1135).
//!
//! Every wide-row test runs against the **synthetic**
//! [`crate::wide_row_fixture`] shapes built in process, never the fetched
//! `test_wide_rows` corpus — a dataset-backed byte-cap test would pass vacuously
//! on an empty dataset. Each one asserts non-vacuity (rows > 0 AND batches > 1)
//! BEFORE any byte assertion.
//!
//! Assertions here compare byte totals, row counts and batch counts only — never
//! elapsed wall-clock (#2642 / `roborev-lints`). The throughput comparison lives
//! in the `#[ignore]`d `perf-gate-allow` test at the bottom.

use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use cqlite_core::export::arrow_payload_bytes;
use cqlite_core::schema::TableSchema;

use super::*;
use crate::filter::ScanSpec;
use crate::producer::{DirSource, MergeProducer};
use crate::testutil::build_sstables;
use crate::wide_row_fixture as fx;

// ---------------------------------------------------------------------------
// Fixture plumbing
// ---------------------------------------------------------------------------

/// Flush `n_rows` wide rows (each carrying a `payload_len`-byte blob) into a real
/// SSTable and return the temp dir (keep it alive), the table dir and the schema.
fn wide_fixture(n_rows: i32, payload_len: usize) -> (tempfile::TempDir, PathBuf, TableSchema) {
    let schema = fx::wide_row_schema();
    let (temp, data_dir, table_dir) =
        build_sstables(&schema, vec![fx::wide_row_mutations(n_rows, payload_len)]);
    let _ = data_dir;
    (temp, table_dir, schema)
}

/// Flush `n_rows` narrow rows into a real SSTable.
fn narrow_fixture(n_rows: i32) -> (tempfile::TempDir, PathBuf, TableSchema) {
    let schema = fx::narrow_row_schema();
    let (temp, data_dir, table_dir) =
        build_sstables(&schema, vec![fx::narrow_row_mutations(n_rows)]);
    let _ = data_dir;
    (temp, table_dir, schema)
}

fn producer(schema: TableSchema, batch_size: usize, cap: usize) -> MergeProducer {
    MergeProducer::with_spec(schema, batch_size, ScanSpec::default())
        .expect("producer")
        .with_max_batch_bytes(cap)
}

/// Run the `producer.rs` buffered merge path (`drive_merge`) end to end.
fn scan_merge(producer: &MergeProducer, dir: &std::path::Path) -> Vec<RecordBatch> {
    producer
        .produce(&DirSource::new(dir))
        .expect("merge full scan")
}

/// Run the `producer_stream.rs` row-granular path (`drive_merge_streaming`) end
/// to end over the same fixture, through the real `produce_streaming` seam the
/// `do_get` response stream uses.
fn scan_streaming(producer: &MergeProducer, dir: &std::path::Path) -> Vec<RecordBatch> {
    let source = DirSource::new(dir);
    let paths = producer.resolve_paths(&source).expect("resolve paths");
    producer
        .produce_streaming_to_vec(paths, &crate::cancel::CancelFlag::new())
        .expect("streaming full scan")
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Non-vacuity gate: real rows AND a real boundary decision. Runs BEFORE any byte
/// assertion so a fixture that silently produced nothing fails here, loudly.
fn assert_non_vacuous(batches: &[RecordBatch], expected_rows: usize, what: &str) {
    assert_eq!(
        total_rows(batches),
        expected_rows,
        "{what}: expected {expected_rows} rows, got {} — vacuous fixture",
        total_rows(batches)
    );
    assert!(
        batches.len() > 1,
        "{what}: only {} batch(es) emitted — the fixture is degenerate and the \
         byte assertions below would be vacuous",
        batches.len()
    );
}

/// Rows in every batch but the last.
fn non_final_row_counts(batches: &[RecordBatch]) -> Vec<usize> {
    batches
        .iter()
        .take(batches.len().saturating_sub(1))
        .map(|b| b.num_rows())
        .collect()
}

// Shared fixture sizing. 220 rows × 4 KiB payload = ~900 KiB of blob, so a
// 64 KiB cap yields ~15 batches while `batch_size` stays far out of reach.
const WIDE_ROWS: i32 = 220;
const WIDE_PAYLOAD: usize = 4096;
const WIDE_CAP: usize = 64 * 1024;
const BIG_BATCH_SIZE: usize = 8192;

// ---------------------------------------------------------------------------
// Requirement 1: whichever cap trips first finishes the batch
// ---------------------------------------------------------------------------

/// Wide rows finish batches on the BYTE-cap: every non-final batch has strictly
/// fewer than `batch_size` rows, and more than one batch is emitted.
///
/// FAILS on pre-change `main`: with no byte-cap the whole 220-row scan is a
/// single row-cut batch, so `batches.len() > 1` fails at the non-vacuity gate.
#[test]
fn wide_rows_finish_batches_on_the_byte_cap() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    let p = producer(schema, BIG_BATCH_SIZE, WIDE_CAP);
    let batches = scan_merge(&p, &dir);
    assert_non_vacuous(&batches, WIDE_ROWS as usize, "wide byte-cut");
    for (i, rows) in non_final_row_counts(&batches).iter().enumerate() {
        assert!(
            *rows < BIG_BATCH_SIZE,
            "batch {i} has {rows} rows — the ROW-cap cut it, not the byte-cap"
        );
        assert!(*rows > 0, "batch {i} is empty");
    }
}

/// The `producer_stream.rs` path honours the same boundary rule over the same
/// fixture — neither egress path is left unbounded.
///
/// FAILS on pre-change `main` for the same reason.
#[test]
fn streaming_path_finishes_batches_on_the_byte_cap() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    let p = producer(schema, BIG_BATCH_SIZE, WIDE_CAP);
    let batches = scan_streaming(&p, &dir);
    assert_non_vacuous(&batches, WIDE_ROWS as usize, "streaming byte-cut");
    for (i, rows) in non_final_row_counts(&batches).iter().enumerate() {
        assert!(
            *rows < BIG_BATCH_SIZE,
            "streaming batch {i} has {rows} rows — row-cut, not byte-cut"
        );
    }
}

/// Both paths agree on the boundary: the same fixture under the same cap yields
/// the same per-batch row counts through `drive_merge` and
/// `drive_merge_streaming`. A cap wired into only one path would diverge here.
#[test]
fn both_egress_paths_produce_the_same_byte_cut_boundaries() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    let p = producer(schema, BIG_BATCH_SIZE, WIDE_CAP);
    let merged: Vec<usize> = scan_merge(&p, &dir).iter().map(|b| b.num_rows()).collect();
    let streamed: Vec<usize> = scan_streaming(&p, &dir)
        .iter()
        .map(|b| b.num_rows())
        .collect();
    assert!(merged.len() > 1, "degenerate: {} batch(es)", merged.len());
    assert_eq!(
        merged, streamed,
        "the two egress paths cut batches differently"
    );
}

/// Narrow rows still finish on the ROW-cap under the default 4 MiB cap: every
/// non-final batch has EXACTLY `batch_size` rows, and the boundaries are
/// identical to a run with the byte-cap effectively disabled.
#[test]
fn narrow_rows_still_finish_batches_on_the_row_cap() {
    const BATCH: usize = 64;
    const ROWS: i32 = 4 * BATCH as i32; // well past the "2 x batch_size" floor
    let (_temp, dir, schema) = narrow_fixture(ROWS);

    let capped = producer(schema.clone(), BATCH, DEFAULT_MAX_BATCH_BYTES);
    let capped_batches = scan_merge(&capped, &dir);
    assert_non_vacuous(&capped_batches, ROWS as usize, "narrow default cap");
    for (i, rows) in non_final_row_counts(&capped_batches).iter().enumerate() {
        assert_eq!(
            *rows, BATCH,
            "batch {i} has {rows} rows — the byte-cap tripped on a NARROW shape"
        );
    }

    // Byte-cap effectively disabled: identical boundaries prove no behaviour
    // change on the narrow path.
    let unbounded = producer(schema, BATCH, usize::MAX);
    let unbounded_rows: Vec<usize> = scan_merge(&unbounded, &dir)
        .iter()
        .map(|b| b.num_rows())
        .collect();
    let capped_rows: Vec<usize> = capped_batches.iter().map(|b| b.num_rows()).collect();
    assert_eq!(
        capped_rows, unbounded_rows,
        "the default cap changed narrow-path batch boundaries"
    );
}

// ---------------------------------------------------------------------------
// Requirement 2: the decision precedes materialization
// ---------------------------------------------------------------------------

/// No oversized batch is ever allocated: every emitted batch's realized payload
/// respects the cap (up to the last row that crossed it), so the wide batch that
/// the row-cap alone would have built was never constructed at all.
#[test]
fn no_oversized_batch_is_ever_allocated() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    let p = producer(schema, BIG_BATCH_SIZE, WIDE_CAP);
    let batches = scan_merge(&p, &dir);
    assert_non_vacuous(&batches, WIDE_ROWS as usize, "no oversized batch");
    // The row-capped batch this scan would otherwise have produced.
    let uncapped = scan_merge(
        &producer(fx::wide_row_schema(), BIG_BATCH_SIZE, usize::MAX),
        &dir,
    );
    let uncapped_payload: usize = uncapped.iter().map(arrow_payload_bytes).sum();
    assert!(
        uncapped_payload > WIDE_CAP * 4,
        "fixture too small to prove anything: uncapped payload {uncapped_payload}"
    );
    for (i, b) in batches.iter().enumerate() {
        let payload = arrow_payload_bytes(b);
        // A batch is cut on the row that CROSSES the cap, so the last row's
        // width is the allowed excess. Every row here is the same width.
        assert!(
            payload <= WIDE_CAP + per_row_payload_bound(),
            "batch {i} realized {payload} payload bytes, above the cap {WIDE_CAP}"
        );
    }
}

/// Upper bound on one wide fixture row's realized payload contribution — the
/// allowed excess of the row that crosses the cap.
fn per_row_payload_bound() -> usize {
    WIDE_PAYLOAD + 256
}

/// The running estimate is per-row and reset on flush: pushing N rows then
/// resetting leaves the accumulator at zero, and each push advances it by that
/// row's width only.
#[test]
fn accumulator_is_incremental_and_reset_on_flush() {
    let mut cap = BatchByteCap::new(1000);
    assert_eq!(cap.accumulated(), 0);
    assert_eq!(cap.push_width(300), ShouldFlush::No);
    assert_eq!(cap.accumulated(), 300);
    assert_eq!(cap.push_width(300), ShouldFlush::No);
    assert_eq!(cap.accumulated(), 600);
    assert_eq!(cap.push_width(500), ShouldFlush::Yes);
    assert_eq!(cap.accumulated(), 1100);
    cap.reset();
    assert_eq!(cap.accumulated(), 0);
    assert_eq!(cap.push_width(1), ShouldFlush::No);
}

// ---------------------------------------------------------------------------
// Requirement 4: the payload / capacity tolerance, in named constants
// ---------------------------------------------------------------------------

/// Every emitted batch stays inside the PUBLISHED capacity tolerance
/// `BATCH_BYTES_CAPACITY_FACTOR * cap + BATCH_BYTES_PER_COLUMN_SLACK * columns`,
/// and — separately and tightly — every multi-row batch's PAYLOAD bytes are at
/// or below the cap. Both bounds are expressed through the named constants, not
/// inline literals.
#[test]
fn emitted_batches_respect_the_payload_and_capacity_bounds() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    let n_columns = schema.columns.len();
    let p = producer(schema, BIG_BATCH_SIZE, WIDE_CAP);
    let batches = scan_merge(&p, &dir);
    assert_non_vacuous(&batches, WIDE_ROWS as usize, "capacity tolerance");

    let capacity_bound = worst_case_batch_capacity_bytes(WIDE_CAP, n_columns);
    for (i, b) in batches.iter().enumerate() {
        assert!(
            b.get_array_memory_size() <= capacity_bound,
            "batch {i}: get_array_memory_size {} exceeds the published tolerance {capacity_bound}",
            b.get_array_memory_size()
        );
        // Tight payload bound, in the cap's own currency: a batch of two or more
        // rows must sit at or below the cap once its final row is excluded — the
        // cut happens ON the crossing row.
        if b.num_rows() >= 2 {
            let payload = arrow_payload_bytes(b);
            assert!(
                payload <= WIDE_CAP + per_row_payload_bound(),
                "batch {i}: payload {payload} above cap {WIDE_CAP}"
            );
        }
    }
}

/// The published capacity conversion is derivable from the named constants
/// alone, with no hidden fudge factor.
#[test]
fn worst_case_capacity_follows_from_the_named_constants() {
    assert_eq!(
        worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, 3),
        DEFAULT_MAX_BATCH_BYTES * BATCH_BYTES_CAPACITY_FACTOR + BATCH_BYTES_PER_COLUMN_SLACK * 3
    );
    // Saturating: an unbounded cap reports the ceiling, never a wrapped value.
    assert_eq!(worst_case_batch_capacity_bytes(usize::MAX, 8), usize::MAX);
    // The #2821 composition: 6 MiB ceiling + one worst-case 4 MiB-payload batch
    // stays inside B4's 16Mi at concurrency 1.
    let one_batch = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, 16);
    assert!(
        6 * 1024 * 1024 + one_batch <= 16 * 1024 * 1024,
        "6 MiB ceiling + one max batch ({one_batch}) breaks the B4 budget"
    );
}

// ---------------------------------------------------------------------------
// Requirement 5: the one-row floor
// ---------------------------------------------------------------------------

/// A single row wider than the WHOLE cap is still delivered, as a one-row batch.
///
/// FAILS on pre-change `main`: a "flush before the cap is exceeded" boundary
/// would either drop the row or spin on an empty buffer; without any cap the
/// batch count is 1 for all N, so the per-batch assertion below is what pins the
/// behaviour.
#[test]
fn rows_wider_than_the_cap_are_emitted_one_per_batch() {
    const N: i32 = 6;
    let (_temp, dir, schema) = wide_fixture(N, 8192);
    // Cap far below one row's width.
    let p = producer(schema, BIG_BATCH_SIZE, 64);
    let batches = scan_merge(&p, &dir);
    assert_eq!(total_rows(&batches), N as usize, "a row was dropped");
    assert_eq!(
        batches.len(),
        N as usize,
        "expected exactly one batch per over-cap row"
    );
    for (i, b) in batches.iter().enumerate() {
        assert_eq!(b.num_rows(), 1, "batch {i} is not a one-row batch");
    }
}

/// Caps of `0` and `1` degrade to one row per batch rather than hanging or
/// dropping rows — on BOTH egress paths.
#[test]
fn zero_and_one_byte_caps_degrade_to_one_row_per_batch() {
    const N: i32 = 12;
    let (_temp, dir, schema) = narrow_fixture(N);
    for cap in [0usize, 1] {
        let p = producer(schema.clone(), BIG_BATCH_SIZE, cap);
        for (label, batches) in [
            ("merge", scan_merge(&p, &dir)),
            ("streaming", scan_streaming(&p, &dir)),
        ] {
            assert_eq!(
                total_rows(&batches),
                N as usize,
                "cap {cap} on the {label} path dropped rows"
            );
            assert_eq!(
                batches.len(),
                N as usize,
                "cap {cap} on the {label} path did not yield one row per batch"
            );
            assert!(
                batches.iter().all(|b| b.num_rows() == 1),
                "cap {cap} on the {label} path emitted a non-single-row batch"
            );
        }
    }
}

/// The accumulator itself never reports a flush with an empty buffer, for any
/// cap — the push-then-test invariant that makes the one-row floor total.
#[test]
fn accumulator_never_flushes_an_empty_buffer() {
    for cap in [0usize, 1, 64, DEFAULT_MAX_BATCH_BYTES, usize::MAX] {
        let mut acc = BatchByteCap::new(cap);
        // A fresh accumulator has flushed nothing.
        assert_eq!(acc.accumulated(), 0);
        // Even a saturating width only trips AFTER the row is counted.
        let decision = acc.push_width(usize::MAX);
        if cap == usize::MAX {
            assert_eq!(decision, ShouldFlush::Yes, "cap {cap}");
        } else {
            assert_eq!(decision, ShouldFlush::Yes, "cap {cap}");
        }
        assert_eq!(acc.accumulated(), usize::MAX, "cap {cap} wrapped");
    }
}

// ---------------------------------------------------------------------------
// Requirement 6: the knob is functional, not decorative
// ---------------------------------------------------------------------------

/// Two distinct configured caps produce correspondingly different boundaries:
/// the smaller cap yields strictly more batches with strictly fewer rows each.
///
/// FAILS on pre-change `main`: there is no cap, so both runs produce identical
/// (row-cut) boundaries.
#[test]
fn two_distinct_caps_produce_different_batch_boundaries() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    let small = scan_merge(&producer(schema.clone(), BIG_BATCH_SIZE, 32 * 1024), &dir);
    let large = scan_merge(&producer(schema, BIG_BATCH_SIZE, 256 * 1024), &dir);
    assert_non_vacuous(&small, WIDE_ROWS as usize, "small cap");
    assert_non_vacuous(&large, WIDE_ROWS as usize, "large cap");
    assert!(
        small.len() > large.len(),
        "smaller cap produced {} batches, not more than the larger cap's {}",
        small.len(),
        large.len()
    );
    let max_small = small.iter().map(|b| b.num_rows()).max().unwrap_or(0);
    let max_large = large.iter().map(|b| b.num_rows()).max().unwrap_or(0);
    assert!(
        max_small < max_large,
        "smaller cap's widest batch ({max_small} rows) is not smaller than the \
         larger cap's ({max_large} rows)"
    );
}

/// The cap is ON by default on the plain constructors — a library embedder that
/// supplies nothing still gets a bounded batch (design §f).
#[test]
fn the_default_cap_is_in_force_on_plain_constructors() {
    let p = MergeProducer::new(fx::wide_row_schema(), BIG_BATCH_SIZE).expect("producer");
    assert_eq!(p.max_batch_bytes(), DEFAULT_MAX_BATCH_BYTES);
    let p =
        MergeProducer::with_spec(fx::wide_row_schema(), 1, ScanSpec::default()).expect("producer");
    assert_eq!(p.max_batch_bytes(), DEFAULT_MAX_BATCH_BYTES);
    let svc = crate::service::CqliteFlightService::new("/nonexistent", BIG_BATCH_SIZE);
    assert_eq!(svc.max_batch_bytes(), DEFAULT_MAX_BATCH_BYTES);
    let svc = crate::service::CqliteFlightService::with_admission(
        "/nonexistent",
        BIG_BATCH_SIZE,
        crate::admission::Admission::unconstrained(),
    );
    assert_eq!(svc.max_batch_bytes(), DEFAULT_MAX_BATCH_BYTES);
}

/// A default-constructed embedder scan over the wide fixture is genuinely
/// bounded: with a cap small enough to bind, the library path cuts batches
/// without any explicit opt-in beyond the builder.
#[test]
fn library_embedder_scan_is_bounded_by_default() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    // The plain constructor, then only the knob's value changed — proving the
    // mechanism is present before configuration, not installed by it.
    let p = MergeProducer::new(schema, BIG_BATCH_SIZE).expect("producer");
    assert_eq!(p.max_batch_bytes(), DEFAULT_MAX_BATCH_BYTES);
    let bounded = p.with_max_batch_bytes(WIDE_CAP);
    let batches = scan_merge(&bounded, &dir);
    assert_non_vacuous(&batches, WIDE_ROWS as usize, "embedder default");
}

// ---------------------------------------------------------------------------
// Requirement 7: the cap does not alter result content
// ---------------------------------------------------------------------------

/// Capped and effectively-unbounded runs concatenate to identical rows, in
/// identical order, with identical values and an identical Arrow schema — only
/// the batch boundaries differ.
#[test]
fn capped_and_uncapped_runs_concatenate_to_identical_results() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    let capped = scan_merge(&producer(schema.clone(), BIG_BATCH_SIZE, WIDE_CAP), &dir);
    let uncapped = scan_merge(&producer(schema, BIG_BATCH_SIZE, usize::MAX), &dir);
    assert_non_vacuous(&capped, WIDE_ROWS as usize, "capped run");
    assert_eq!(
        total_rows(&uncapped),
        WIDE_ROWS as usize,
        "uncapped run lost rows"
    );
    assert!(
        capped.len() > uncapped.len(),
        "the cap did not change any boundary — the comparison is vacuous"
    );

    let capped_schema = capped.first().map(|b| b.schema());
    let uncapped_schema = uncapped.first().map(|b| b.schema());
    assert_eq!(capped_schema, uncapped_schema, "Arrow schema differs");

    // Concatenate each side into one batch and compare cell for cell.
    let cat = |batches: &[RecordBatch]| -> RecordBatch {
        let schema = batches.first().map(|b| b.schema()).expect("a batch");
        arrow::compute::concat_batches(&schema, batches).expect("concat")
    };
    assert_eq!(cat(&capped), cat(&uncapped), "row content or order differs");
}

/// The total row count is invariant across a descending series of caps.
#[test]
fn lowering_the_cap_does_not_change_the_total_row_count() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    let mut seen_batch_counts = Vec::new();
    for cap in [usize::MAX, 1024 * 1024, 128 * 1024, 32 * 1024, 4096, 1] {
        let batches = scan_merge(&producer(schema.clone(), BIG_BATCH_SIZE, cap), &dir);
        assert_eq!(
            total_rows(&batches),
            WIDE_ROWS as usize,
            "cap {cap} changed the total row count"
        );
        seen_batch_counts.push(batches.len());
    }
    // Monotone non-decreasing batch counts as the cap descends.
    for w in seen_batch_counts.windows(2) {
        assert!(
            w[1] >= w[0],
            "batch count went DOWN as the cap shrank: {seen_batch_counts:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Requirement 8: the guards are real — the pre-change control
// ---------------------------------------------------------------------------

/// The pre-change `main` control, in tree and mechanical.
///
/// `main` has no byte-cap at all, which is behaviourally exactly this run: the
/// cap disabled (`usize::MAX`), leaving the row-cap as the sole boundary. This
/// asserts that under those conditions the wide fixture produces a SINGLE
/// row-cut batch whose payload is many times the cap — so
/// [`wide_rows_finish_batches_on_the_byte_cap`],
/// [`streaming_path_finishes_batches_on_the_byte_cap`],
/// [`rows_wider_than_the_cap_are_emitted_one_per_batch`],
/// [`two_distinct_caps_produce_different_batch_boundaries`] and
/// [`emitted_batches_respect_the_payload_and_capacity_bounds`] all FAIL there.
/// (Those tests additionally fail to COMPILE against `main`, whose
/// `MergeProducer` has no `with_max_batch_bytes`; this control is the stronger,
/// behavioural form of the same evidence.)
#[test]
fn without_the_byte_cap_the_wide_scan_is_one_oversized_row_cut_batch() {
    let (_temp, dir, schema) = wide_fixture(WIDE_ROWS, WIDE_PAYLOAD);
    for batches in [
        scan_merge(&producer(schema.clone(), BIG_BATCH_SIZE, usize::MAX), &dir),
        scan_streaming(&producer(schema.clone(), BIG_BATCH_SIZE, usize::MAX), &dir),
    ] {
        assert_eq!(total_rows(&batches), WIDE_ROWS as usize);
        assert_eq!(
            batches.len(),
            1,
            "pre-change control: the row-cap alone must yield ONE batch"
        );
        let payload: usize = batches.iter().map(arrow_payload_bytes).sum();
        assert!(
            payload > WIDE_CAP * 8,
            "pre-change control: {payload} payload bytes is not meaningfully \
             above the cap {WIDE_CAP} — the byte-cut tests would be vacuous"
        );
        // And it breaks the capacity tolerance the capped run satisfies.
        let bound = worst_case_batch_capacity_bytes(WIDE_CAP, schema.columns.len());
        assert!(
            batches.iter().any(|b| b.get_array_memory_size() > bound),
            "pre-change control: the uncapped batch already fits the tolerance"
        );
    }
}

// ---------------------------------------------------------------------------
// The aggregate route (task 4.3): no route escapes the cap
// ---------------------------------------------------------------------------

/// `split_rows_into_batches` applies the same dual boundary to an
/// already-materialized row slice: never an empty group, never a lost row, and a
/// single over-cap row becomes a one-row group.
#[test]
fn split_rows_into_batches_applies_the_same_dual_boundary() {
    use cqlite_core::query::{ColumnInfo, QueryRow};
    use cqlite_core::types::{DataType, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    let columns = vec![ColumnInfo {
        name: "b".into(),
        data_type: DataType::Blob,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type: Some(cqlite_core::schema::CqlType::Blob),
    }];
    let rows: Vec<QueryRow> = (0..10)
        .map(|_| {
            let mut values: HashMap<Arc<str>, Value> = HashMap::new();
            values.insert(Arc::from("b"), Value::Blob(vec![7u8; 1000].into()));
            QueryRow::with_interned_values(cqlite_core::RowKey::new(Vec::new()), values)
        })
        .collect();

    // Byte-cut: ~3 rows per group at a 3 KiB cap.
    let groups = split_rows_into_batches(&columns, &rows, 8192, 3000);
    assert!(groups.len() > 1, "no byte cut: {} group(s)", groups.len());
    assert!(groups.iter().all(|g| !g.is_empty()), "an empty group");
    assert_eq!(groups.iter().map(|g| g.len()).sum::<usize>(), rows.len());

    // Row-cut still applies when it binds first.
    let groups = split_rows_into_batches(&columns, &rows, 2, usize::MAX);
    assert_eq!(groups.len(), 5);
    assert!(groups.iter().all(|g| g.len() == 2));

    // One over-cap row per group; empty input yields no groups.
    let groups = split_rows_into_batches(&columns, &rows, 8192, 1);
    assert_eq!(groups.len(), rows.len());
    assert!(split_rows_into_batches(&columns, &[], 8192, 1).is_empty());
}

// ---------------------------------------------------------------------------
// Requirement 9: throughput evidence lives OUTSIDE the correctness path
// ---------------------------------------------------------------------------

/// perf-gate-allow — the ~1.0–1.1x throughput expectation from issue #2825.
///
/// `#[ignore]`d and excluded from the gate: this is the ONLY test in the byte-cap
/// suite permitted to read a clock, and it is deliberately outside the
/// correctness path (#2642 / `roborev-lints`). Run explicitly with
/// `cargo test -p cqlite-flight byte_cap_throughput -- --ignored --nocapture`.
#[test]
#[ignore = "perf-gate-allow: throughput comparison, not a correctness assertion"]
fn byte_cap_throughput_is_within_the_expected_band() {
    const ROWS: i32 = 4000;
    let (_temp, dir, schema) = narrow_fixture(ROWS);
    let run = |cap: usize| {
        let p = producer(schema.clone(), BIG_BATCH_SIZE, cap);
        let start = std::time::Instant::now();
        let batches = scan_merge(&p, &dir);
        let elapsed = start.elapsed();
        assert_eq!(total_rows(&batches), ROWS as usize);
        elapsed
    };
    let uncapped = run(usize::MAX);
    let capped = run(DEFAULT_MAX_BATCH_BYTES);
    println!(
        "narrow-path scan: uncapped {uncapped:?}, capped {capped:?}, ratio {:.3}",
        capped.as_secs_f64() / uncapped.as_secs_f64().max(f64::MIN_POSITIVE)
    );
}
