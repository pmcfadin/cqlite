//! Scan-error counting at this boundary (issue #1704): the exactly-once latch and
//! the nested-stream delegation that keep one failed scan operation worth exactly
//! one `cqlite.errors.total{subsystem="reader"}` increment.
//!
//! Gated on `observability-testing` because the assertions read back the emitted
//! series through the in-memory capture harness, and `#[serial(read_metrics)]`
//! against the sibling read-metric tests because that harness is process-global and
//! uses DELTA temporality.
//!
//! These two properties are UNREACHABLE from an integration test: both need a
//! producer task that DIES (the sticky arm) or a hand-built nested stream, and
//! `JoinedStream`'s constructors are crate-internal. The corpus-level "one
//! increment per failed scan" pins live in
//! `tests/issue_1704_scan_path_error_counts.rs`.
//!
//! Included from [`super`] via `#[path = "joined_scan_stream_error_metric_tests.rs"]`,
//! so `use super::*` reaches `JoinedStream` and its constructors.

use crate::observability::{catalog, testing};
use tokio::sync::mpsc;

use super::*;
use crate::storage::sstable::reader::ScanErrorReporting;

/// This capture window's `cqlite.errors.total{subsystem="reader"}` total.
fn reader_errors(m: &testing::CapturedMetrics) -> f64 {
    m.sum_where(
        catalog::ERRORS_TOTAL,
        &[(catalog::attr::SUBSYSTEM, "reader")],
    )
}

/// A dead producer's verdict is STICKY — re-REPORTED to the consumer on every later
/// `recv` — but must be COUNTED once.
///
/// This is the easiest half of #1704 to get wrong: the naive placement (a
/// `record_error` beside each `Some(Err(..))` return) turns one failed scan into one
/// increment per poll, so a consumer that retries N times inflates the error rate
/// N-fold and the metric stops meaning "failed operations".
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_sticky_dead_task_error_is_counted_once_however_often_it_is_polled() {
    let mc = testing::metrics_capture();
    mc.reset();

    let _silence = crate::storage::producer_fault::silence_injected_panics();
    let (tx, rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(4);
    let task = tokio::spawn(async move {
        // Keep the sender alive until the panic so the channel closes only when
        // the task unwinds — the shape a real producer death has.
        let _tx = tx;
        panic!("{}", crate::storage::producer_fault::INJECTED_PANIC_MESSAGE);
    });
    let mut stream = RowScanStream::new_measured_rows(rx, task, ReadOpMeter::start(None));

    let mut outcomes = Vec::new();
    for _ in 0..4 {
        outcomes.push(stream.recv().await);
    }
    // Dropped BEFORE the assertions: restoring a panic hook from a panicking thread
    // aborts the process, which would turn a legitimate failure below into SIGABRT.
    drop(_silence);

    for (attempt, outcome) in outcomes.iter().enumerate() {
        assert!(
            matches!(outcome, Some(Err(_))),
            "attempt {attempt}: a dead task must keep reporting an error, got {outcome:?}"
        );
    }

    let m = mc.flush_and_collect();
    assert_eq!(
        reader_errors(&m),
        1.0,
        "four polls of ONE dead scan must count ONE reader error, not one per poll; \
         entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
}

/// A NESTED stream must not count: the enclosing measured stream sees the very same
/// error when the fan-out merge (or the per-row → batch re-chunker) forwards it.
///
/// Without this, a cross-generation query whose sub-scan fails would record two or
/// three increments for one failed query — the same double-count `ReadOpMeter`
/// already avoids for `read.rows`, which is why the two decisions share the same
/// pair of constructors.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_nested_stream_delegates_counting_to_the_measured_stream_above_it() {
    let mc = testing::metrics_capture();
    mc.reset();

    let (tx, rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(4);
    tx.send(Err(Error::Corruption("sub-scan failed".to_string())))
        .await
        .expect("send");
    drop(tx);
    // `new` (not `new_measured_rows`) is exactly how a fan-out sub-scan and the
    // re-chunker's source are built.
    let mut nested = RowScanStream::new_nested(rx, tokio::spawn(async {}));

    let item = nested.recv().await.expect("one item");
    assert!(
        matches!(item, Err(Error::Corruption(_))),
        "the nested stream still DELIVERS the error unchanged"
    );

    let m = mc.flush_and_collect();
    assert_eq!(
        reader_errors(&m),
        0.0,
        "a nested (unmeasured) stream must not count — its error is counted once by \
         the measured stream that forwards it; entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
}

/// The control for the test above: the SAME error on a MEASURED stream counts once.
/// Without it, "0 increments" could be passing because nothing counts at all.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_measured_stream_counts_the_same_forwarded_error_once() {
    let mc = testing::metrics_capture();
    mc.reset();

    let (tx, rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(4);
    tx.send(Err(Error::Corruption("scan failed".to_string())))
        .await
        .expect("send");
    drop(tx);
    let mut stream =
        RowScanStream::new_measured_rows(rx, tokio::spawn(async {}), ReadOpMeter::start(None));

    let item = stream.recv().await.expect("one item");
    assert!(matches!(item, Err(Error::Corruption(_))));
    // Poll past the terminal error: the channel is closed and the task finished
    // cleanly, so this is the "consumer keeps reading" shape.
    assert!(stream.recv().await.is_none());

    let m = mc.flush_and_collect();
    assert_eq!(
        reader_errors(&m),
        1.0,
        "the measured stream counts the forwarded error exactly once; entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
    assert_eq!(
        m.sum_where(
            catalog::ERRORS_TOTAL,
            &[
                (catalog::attr::SUBSYSTEM, "reader"),
                (catalog::attr::ERROR_CATEGORY, "corruption"),
            ],
        ),
        1.0,
        "and carries the classifier's category for Error::Corruption; entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
}

/// A TOP-LEVEL but UNMETERED stream whose producer dies must count ONE (issue #1704,
/// roborev F2).
///
/// This is the regression guard for inferring "nested" from "has no `ReadOpMeter`".
/// The two properties are independent: `scan_stream_batched_admitted(.., Exempt)` is
/// unmetered because a fan-out merge owns the admission permit for the whole
/// operation, and `summary_scan::query_rows` drains exactly that stream DIRECTLY as
/// the top-level Flight full-scan producer. Under the inference its failures counted
/// ZERO — the #1704 defect itself, live, on a path this issue exists to fix.
///
/// The dead-task arm is the one that matters here: a forwarded `Err` from such a
/// stream is at least visible to whoever polls it, whereas a producer that UNWINDS
/// produces a verdict this stream synthesises and nothing else can ever observe.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_top_level_unmetered_stream_counts_its_dead_producer() {
    let mc = testing::metrics_capture();
    mc.reset();

    let _silence = crate::storage::producer_fault::silence_injected_panics();
    let (tx, rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(4);
    let task = tokio::spawn(async move {
        let _tx = tx;
        panic!("{}", crate::storage::producer_fault::INJECTED_PANIC_MESSAGE);
    });
    // UNMETERED and TOP-LEVEL — the combination the meter-derived inference could not
    // represent.
    let mut stream = RowScanStream::unmetered_as(rx, task, ScanErrorReporting::TopLevel);
    let first = stream.recv().await;
    drop(_silence);

    assert!(
        matches!(first, Some(Err(_))),
        "a dead producer must be an error, not a clean end of stream; got {first:?}"
    );

    let m = mc.flush_and_collect();
    assert_eq!(
        reader_errors(&m),
        1.0,
        "an unmetered TOP-LEVEL stream is the only observer of its producer's death, \
         so it must count it; 0 means `Nested` was inferred from the absent meter. \
         entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
}

/// The complement: a stream over an ALREADY-COUNTING source counts its OWN death but
/// NOT what it forwards — the re-chunker's shape (issue #1704).
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_stream_over_a_counted_source_counts_only_its_own_death() {
    let mc = testing::metrics_capture();
    mc.reset();

    // Forwarded arm: the source already counted this, so this stream must not.
    let (tx, rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(4);
    tx.send(Err(Error::Corruption(
        "already counted upstream".to_string(),
    )))
    .await
    .expect("send");
    drop(tx);
    let mut forwarding = RowScanStream::unmetered_as(
        rx,
        tokio::spawn(async {}),
        ScanErrorReporting::TopLevelOverCountedSource,
    );
    assert!(matches!(forwarding.recv().await, Some(Err(_))));
    assert!(forwarding.recv().await.is_none());

    let m = mc.flush_and_collect();
    assert_eq!(
        reader_errors(&m),
        0.0,
        "a forwarded error was already counted by the counting source; entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );

    // Own-death arm: nothing upstream can see this, so it MUST be counted.
    mc.reset();
    let _silence = crate::storage::producer_fault::silence_injected_panics();
    let (tx2, rx2) = mpsc::channel::<Result<(RowKey, ScanRow)>>(4);
    let task = tokio::spawn(async move {
        let _tx = tx2;
        panic!("{}", crate::storage::producer_fault::INJECTED_PANIC_MESSAGE);
    });
    let mut dying =
        RowScanStream::unmetered_as(rx2, task, ScanErrorReporting::TopLevelOverCountedSource);
    let outcome = dying.recv().await;
    drop(_silence);
    assert!(matches!(outcome, Some(Err(_))));

    let m = mc.flush_and_collect();
    assert_eq!(
        reader_errors(&m),
        1.0,
        "this stream's OWN death is invisible to its source, so it must count it; \
         entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
}
