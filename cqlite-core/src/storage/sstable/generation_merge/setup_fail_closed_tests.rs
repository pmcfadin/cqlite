//! Issue #3154 — END-TO-END pins for what a `KWayMerger::new` failure earns.
//!
//! `SSTableManager::scan_stream` is allowed to answer a multi-generation read with the
//! NON-reconciling token-order concat when the reconciling merge cannot be built. That
//! substitution returns a FULL-LENGTH but UNRECONCILED result set — duplicated
//! overwritten rows, resurrected deleted rows — so it is only ever acceptable for an
//! input the reconciling merger genuinely cannot handle. Before this issue it was
//! applied to EVERY reported construction failure, so a transient I/O hiccup or a
//! corrupt file made the query return the wrong rows and report success, behind a
//! `tracing::warn!`.
//!
//! Three arms, one fixture, one oracle:
//!
//! * an I/O failure ⇒ the read FAILS (never the concat);
//! * a corruption failure ⇒ the read FAILS (never the concat);
//! * a merger-INELIGIBLE (unsupported-format) failure ⇒ the read still degrades to the
//!   concat, exactly as before. Over-restricting is its own regression — it would turn
//!   previously-working queries into failures — so that arm is asserted POSITIVELY:
//!   the concat's own answer must come back, by count AND by value.
//!
//! # Why the fixture must OVERLAP, and why the control arm runs first
//!
//! Over the shared `multi_gen_fixture`'s overlapping generations, reconciled and
//! concatenated answers differ by row COUNT and by row VALUE, so "it fell back" and "it
//! did not" are distinguishable observations rather than a coin flip. Every arm below
//! first runs `assert_reconciled_control`, which pins that the healthy read really is
//! the reconciled LWW winner set — without it, an `is_err()`-only assertion (or a
//! fixture that yields nothing) would pass while proving nothing.
//!
//! # Why the failure is INJECTED
//!
//! The classification keys on the error VARIANT `KWayMerger::new` reports, and no
//! on-disk fixture can make that one call site report an I/O error, then a corruption
//! error, then an unsupported-format error, on demand and deterministically. The
//! `storage::producer_fault::construction` seam does exactly that, scoped to this
//! test's own `TempDir` so a concurrently-running test can neither consume nor be hit
//! by the arm (see that module's doc).

use tempfile::TempDir;

use super::multi_gen_fixture::{
    assert_reconciled_control, drain_stream, flush_overlapping_generations, newest_value_prefix,
    open_manager, open_reconciling_stream, reconciled_rows, unreconciled_rows,
};
use crate::storage::producer_fault::{
    arm_merge_construction_error, MergeConstructionFault, INJECTED_CONSTRUCTION_MESSAGE,
};
use crate::Error;

/// An arm whose construction failure must PROPAGATE: the read fails, and it fails with
/// THIS fault's error — never with a full-length unreconciled result set under `Ok`.
///
/// `is_expected_variant` is checked on the returned `Error` rather than on its message:
/// the fix's whole point is that the decision (and therefore the error the caller sees)
/// is type-level, so a test that only matched on text could not tell the classification
/// from a coincidence.
async fn assert_construction_failure_propagates(
    fault: MergeConstructionFault,
    is_expected_variant: fn(&Error) -> bool,
    expected_variant: &str,
) {
    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = flush_overlapping_generations(&temp_dir).await;
    let manager = open_manager(&data_dir).await;

    // Control arm FIRST: the healthy read is the RECONCILED answer, so the fault arm's
    // "this is not the concat's answer" below is a real observation.
    assert_reconciled_control(&manager).await;

    let scope = temp_dir.path().to_string_lossy().to_string();
    let _fault = arm_merge_construction_error(&scope, fault);
    let outcome = open_reconciling_stream(&manager).await;

    let error = match outcome {
        Err(e) => e,
        Ok(stream) => {
            let (drained, values) = drain_stream(stream).await;
            panic!(
                "issue #3154: `KWayMerger::new` reported {fault:?}, which is a RUNTIME \
                 failure and says nothing about whether the merger supports this input, \
                 so the read MUST fail. Succeeding means the non-reconciling concat was \
                 substituted: {} rows (the reconciled answer is {}, the concat's is {}) \
                 of UNRECONCILED data served as a successful reconciling scan — \
                 duplicated overwritten rows and resurrected deleted ones. Values: \
                 {values:?}",
                drained.rows,
                reconciled_rows(),
                unreconciled_rows()
            )
        }
    };

    assert!(
        is_expected_variant(&error),
        "the propagated error must stay the {expected_variant} the merger reported — the \
         caller (and a binding deciding whether a retry could help) needs the real cause, \
         not a re-labelled one. Got: {error:?}"
    );
    assert!(
        error.to_string().contains(INJECTED_CONSTRUCTION_MESSAGE),
        "the error must carry THIS fault's message, proving the injected construction \
         failure (not some unrelated setup problem) is what failed the read. Got: {error}"
    );
}

/// A transient I/O error during construction must FAIL the read.
///
/// This is the likelier of the two propagating triggers — an I/O hiccup needs no bug at
/// all — and pre-fix it silently produced the concat's wrong answer under `Ok`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Issue #1704: a propagating setup failure now RECORDS into the process-global
// `cqlite.errors.total` capture (`MergeStreamSetupError::into_counted_error`), so
// these three must serialize against the tests that ASSERT on that capture — the
// harness is process-wide and DELTA-temporality, so a concurrent emitter lands
// inside someone else's measurement window. Measured, not theorised: without this
// tag the sibling `error_metrics::a_fallback_eligible_setup_failure_counts_nothing`
// read 1.0 where it must read 0.0.
#[serial_test::serial(read_metrics)]
async fn an_io_error_from_merger_construction_fails_the_read_instead_of_serving_the_concat() {
    assert_construction_failure_propagates(
        MergeConstructionFault::Io,
        |e| matches!(e, Error::Io(_)),
        "I/O error",
    )
    .await;
}

/// A corruption / parse error during construction must FAIL the read.
///
/// Answering it with the concat would serve data from a file already known to be bad,
/// under `Ok`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Issue #1704: a propagating setup failure now RECORDS into the process-global
// `cqlite.errors.total` capture (`MergeStreamSetupError::into_counted_error`), so
// these three must serialize against the tests that ASSERT on that capture — the
// harness is process-wide and DELTA-temporality, so a concurrent emitter lands
// inside someone else's measurement window. Measured, not theorised: without this
// tag the sibling `error_metrics::a_fallback_eligible_setup_failure_counts_nothing`
// read 1.0 where it must read 0.0.
#[serial_test::serial(read_metrics)]
async fn a_corruption_error_from_merger_construction_fails_the_read_instead_of_serving_the_concat()
{
    assert_construction_failure_propagates(
        MergeConstructionFault::Corruption,
        |e| matches!(e, Error::Corruption(_)),
        "corruption error",
    )
    .await;
}

/// The one arm that must STILL fall back: a genuinely merger-ineligible input.
///
/// Asserted POSITIVELY — the read succeeds AND returns the concat's own answer, by count
/// and by value — because "it did not error" alone would also pass if the fallback had
/// been narrowed away and replaced by something else entirely. Narrowing too far is its
/// own regression: it turns a query that used to return the documented (Issue #883)
/// unreconciled answer into a hard failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Issue #1704: a propagating setup failure now RECORDS into the process-global
// `cqlite.errors.total` capture (`MergeStreamSetupError::into_counted_error`), so
// these three must serialize against the tests that ASSERT on that capture — the
// harness is process-wide and DELTA-temporality, so a concurrent emitter lands
// inside someone else's measurement window. Measured, not theorised: without this
// tag the sibling `error_metrics::a_fallback_eligible_setup_failure_counts_nothing`
// read 1.0 where it must read 0.0.
#[serial_test::serial(read_metrics)]
async fn a_merger_ineligible_input_still_degrades_to_the_documented_concat() {
    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = flush_overlapping_generations(&temp_dir).await;
    let manager = open_manager(&data_dir).await;

    // Control arm FIRST: prove the healthy read is the RECONCILED answer, so observing
    // the UNRECONCILED one below really does mean "the concat ran".
    assert_reconciled_control(&manager).await;

    let scope = temp_dir.path().to_string_lossy().to_string();
    let _fault = arm_merge_construction_error(&scope, MergeConstructionFault::UnsupportedFormat);
    let stream = open_reconciling_stream(&manager)
        .await
        .expect("an input the reconciling merger cannot handle must still be READABLE via the concat fallback — narrowing that away turns a previously-working query into a failure (issue #3154 AC2)");
    let (drained, values) = drain_stream(stream).await;

    assert_eq!(
        drained.error, None,
        "the concat fallback must complete cleanly, exactly as before"
    );
    assert_eq!(
        drained.rows,
        unreconciled_rows(),
        "the fallback must be the documented NON-reconciling concat: every generation's \
         copy of every partition ({} rows), not the reconciled {}",
        unreconciled_rows(),
        reconciled_rows()
    );
    let newest = newest_value_prefix();
    assert!(
        values.iter().any(|v| !v.starts_with(&newest)),
        "the fallback's answer must actually contain superseded older-generation values \
         — that is what identifies it as the concat rather than as some other path that \
         merely returned the right number of rows. Got: {values:?}"
    );
}

// The METRIC half of this same property (issue #1704): a propagating setup failure
// must also land in `cqlite.errors.total{subsystem="reader"}`, and the concat
// fallback must not. Declared from HERE rather than from `generation_merge.rs`
// because that file is over the ~800-line campsite target (#1116) and adding a
// declaration to it would trip the gate's growth ratchet; this suite is its natural
// parent anyway — same fixture, same injected construction faults.
#[cfg(feature = "observability-testing")]
#[path = "setup_error_metric_tests.rs"]
mod error_metrics;
