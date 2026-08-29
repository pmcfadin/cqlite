//! Scan-stream SETUP failures must land in `cqlite.errors.total` (issue #1704).
//!
//! # The gap this closes
//!
//! `SSTableManager::scan_stream` can fail BEFORE any `JoinedStream` is constructed:
//! the multi-generation reconciling merge reports a construction failure and, when
//! that failure is not merger-ineligible (issue #3154), the call returns `Err`
//! without ever creating a stream. #1704's streaming seam lives in
//! `JoinedStream::recv`, so it cannot see an error that predates the stream — the
//! scan failed, the caller got the error, and the operator's error dashboard stayed
//! clean. The issue names "early return" alongside "stream item `Err`" as a scan
//! exit; this is that early return.
//!
//! # Why the failure is INJECTED, and why the fallback arm is asserted too
//!
//! The classification keys on the error VARIANT `KWayMerger::new` reports, and no
//! on-disk fixture makes that one call site report an I/O error on demand — the same
//! reasoning the sibling `setup_fail_closed_tests` records, whose
//! `producer_fault::construction` seam (scoped to this test's own `TempDir`) is
//! reused here verbatim.
//!
//! The merger-INELIGIBLE arm is the other half of the property and is easy to get
//! wrong in the opposite direction: it returns `Ok` after degrading to the
//! non-reconciling concat, which is a DEGRADED read, not a FAILED one. Counting it
//! would inflate the error rate on a path that served rows successfully, so it is
//! pinned at ZERO.
//!
//! Gated on `observability-testing` for the in-memory capture harness, and
//! `#[serial(read_metrics)]` against every other test that resets that
//! process-global, DELTA-temporality harness.

use tempfile::TempDir;

use super::multi_gen_fixture::{
    assert_reconciled_control, flush_overlapping_generations, open_manager, open_reconciling_stream,
};
use crate::observability::{catalog, testing};
use crate::storage::producer_fault::{arm_merge_construction_error, MergeConstructionFault};

/// This capture window's `cqlite.errors.total{subsystem="reader"}` total.
fn reader_errors(m: &testing::CapturedMetrics) -> f64 {
    m.sum_where(
        catalog::ERRORS_TOTAL,
        &[(catalog::attr::SUBSYSTEM, "reader")],
    )
}

/// Run one armed-fault `scan_stream` and return `(outcome, reader-error count)`.
///
/// The control arm runs BEFORE the capture is reset, so its own (successful) read
/// cannot contribute to the measured window while still proving the fixture yields
/// the reconciled answer when healthy — i.e. that the fault arm below really is the
/// fault and not a fixture that fails for everyone.
async fn scan_stream_under_fault(
    fault: MergeConstructionFault,
) -> (
    crate::Result<crate::storage::sstable::reader::RowScanStream>,
    f64,
) {
    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = flush_overlapping_generations(&temp_dir).await;
    let manager = open_manager(&data_dir).await;
    assert_reconciled_control(&manager).await;

    let mc = testing::metrics_capture();
    mc.reset();

    let scope = temp_dir.path().to_string_lossy().to_string();
    let _fault = arm_merge_construction_error(&scope, fault);
    let outcome = open_reconciling_stream(&manager).await;

    let m = mc.flush_and_collect();
    (outcome, reader_errors(&m))
}

/// A PROPAGATING setup failure (I/O) is a failed scan and must count exactly once.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_propagating_setup_failure_counts_exactly_one_reader_error() {
    let (outcome, counted) = scan_stream_under_fault(MergeConstructionFault::Io).await;

    assert!(
        outcome.is_err(),
        "issue #3154: an I/O construction failure must PROPAGATE, not be answered \
         with the concat — this test's premise"
    );
    assert_eq!(
        counted, 1.0,
        "a scan that failed at SETUP — before any stream existed — must record \
         exactly one cqlite.errors.total{{subsystem=reader}} increment (0 = the \
         #1704 gap: the streaming seam cannot see a pre-stream error)"
    );
}

/// The same, for the corruption variant: the count must not depend on WHICH
/// propagating variant the merger reported.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_propagating_corruption_setup_failure_counts_exactly_one_reader_error() {
    let (outcome, counted) = scan_stream_under_fault(MergeConstructionFault::Corruption).await;

    assert!(
        outcome.is_err(),
        "a corruption setup failure must propagate"
    );
    assert_eq!(
        counted, 1.0,
        "a corruption setup failure is one failed scan, so exactly one increment"
    );
}

/// The merger-INELIGIBLE arm degrades to the concat and returns `Ok`. A degraded
/// read is NOT a failed scan, so it must record NOTHING — the direction a naive
/// "count every construction Err" placement would get wrong.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_fallback_eligible_setup_failure_counts_nothing() {
    let (outcome, counted) =
        scan_stream_under_fault(MergeConstructionFault::UnsupportedFormat).await;

    assert!(
        outcome.is_ok(),
        "issue #3154: a merger-ineligible input still degrades to the concat — this \
         test's premise"
    );
    assert_eq!(
        counted, 0.0,
        "the concat fallback SERVED the read: a degraded read is not a failed scan, \
         and counting it would inflate the error rate on a successful query"
    );
}
