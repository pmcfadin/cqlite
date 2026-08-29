//! Cross-generation scan failures must land in `cqlite.errors.total` EXACTLY ONCE
//! (issue #1704) — the SETUP arm (a merge that never produces a stream) and the
//! MID-STREAM REOPEN arm (a producer thread whose `SSTableReader::open` fails).
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
//!
//! Declared from `setup_fail_closed_tests.rs` (its natural parent — same fixture,
//! same faults) rather than from `generation_merge.rs`, which is over the ~800-line
//! campsite target, so the fixture is reached by its absolute path.

use tempfile::TempDir;

use crate::observability::{catalog, testing};
use crate::storage::producer_fault::{arm_merge_construction_error, MergeConstructionFault};
use crate::storage::sstable::generation_merge::multi_gen_fixture::{
    assert_reconciled_control, flush_overlapping_generations, open_manager, open_reconciling_stream,
};

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

/// Every `*-Data.db` under `dir`, recursively — the write engine nests generations in
/// a per-table subdirectory, so a single `read_dir` of the data root finds none.
#[cfg(unix)]
fn collect_data_dbs(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for path in entries.filter_map(|e| e.ok().map(|e| e.path())) {
        if path.is_dir() {
            collect_data_dbs(&path, out);
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with("-Data.db"))
        {
            out.push(path);
        }
    }
}

/// A MID-STREAM REOPEN failure counts ONCE, not twice (issue #1704, roborev round 3).
///
/// `KWayMerger`'s producer thread reopens each input BY PATH
/// (`merge/producer_iter.rs`), and that call used to be the self-instrumenting
/// `SSTableReader::open`: it recorded the failure, the same error then surfaced
/// mid-stream at `step()`, was forwarded through the MEASURED `JoinedStream`, and was
/// recorded again. One failed cross-generation scan, two increments — the third
/// instance of one root cause, now fixed at the source with
/// `SSTableReader::open_unrecorded` rather than a third special case.
///
/// # How the reopen is made to fail
///
/// One generation's `Data.db` is made UNREADABLE (mode 0) after the manager has
/// already opened its own readers. The manager's readers keep working (they hold open
/// descriptors) and the file is still DISCOVERED, so the multi-generation reconciling
/// route is still chosen — but the producer's fresh `open` gets `EACCES`. Deleting the
/// file instead would not do: discovery would simply not see it, the merge would run
/// over the remaining generations, and the test would pass without ever reopening.
///
/// Unix-only, and skipped for a uid that can read a mode-0 file (i.e. root), which is
/// asserted rather than assumed — a run that cannot arrange the failure must not
/// silently claim to have tested it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_midstream_reopen_failure_counts_exactly_one_reader_error() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = flush_overlapping_generations(&temp_dir).await;
    let manager = open_manager(&data_dir).await;
    // Control FIRST, while every input is still readable: the reconciled answer is
    // what a healthy read returns, so the failure below is a real observation.
    assert_reconciled_control(&manager).await;

    let mut data_dbs = Vec::new();
    collect_data_dbs(&data_dir, &mut data_dbs);
    data_dbs.sort();
    assert!(
        data_dbs.len() > 1,
        "the fixture must hold MULTIPLE generations for the reconciling merge to run; \
         found {data_dbs:?}"
    );
    let victim = &data_dbs[0];
    std::fs::set_permissions(victim, std::fs::Permissions::from_mode(0o000))
        .expect("make one generation's Data.db unreadable");
    if std::fs::File::open(victim).is_ok() {
        eprintln!(
            "SKIP: this uid can read a mode-0 file (root?), so a reopen failure \
                   cannot be arranged; the double-count assertion is not exercised."
        );
        return;
    }

    let mc = testing::metrics_capture();
    mc.reset();

    let mut stream = open_reconciling_stream(&manager)
        .await
        .expect("the merger is constructed by spawning producers, so setup still succeeds");
    let mut delivered = None;
    while let Some(item) = stream.recv().await {
        if let Err(e) = item {
            delivered = Some(e);
            break;
        }
    }
    let err = delivered.expect(
        "an unreadable input must surface as a terminal stream error, not a short \
         successful scan",
    );
    drop(stream);

    let m = mc.flush_and_collect();
    assert_eq!(
        reader_errors(&m),
        1.0,
        "one failed cross-generation scan must record ONE reader error; 2.0 means the \
         producer's reopen recorded it and the measured JoinedStream recorded it again. \
         Delivered: {err}; entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
}
