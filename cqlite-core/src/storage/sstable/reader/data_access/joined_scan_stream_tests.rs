//! Behavioural unit tests for [`JoinedStream`] (issues #3106, #3124): the
//! producer-join protocol, its sticky dead-task verdict, and cancellation safety.
//!
//! Split out of `joined_scan_stream.rs` per the campsite rule (#1116/#1135) — that
//! source file sat within a handful of lines of the ~800-line target, so any
//! addition to it had to move the tests out first. This is a VERBATIM move: no
//! assertion changed.
//!
//! Included from [`super`] via `#[path = "joined_scan_stream_tests.rs"]`, so
//! `use super::*` reaches `JoinedStream` and its constructors.

use tokio::sync::mpsc;

use super::*;

fn row(byte: u8) -> (RowKey, ScanRow) {
    (RowKey::new(vec![byte]), ScanRow::Row(Vec::new()))
}

/// Issue #3124: the PER-ROW flavour of the #3106 property — a producer task that
/// DIED is reported as an error, and that verdict is sticky. The batched flavour
/// is pinned by the sibling tests below; this proves the generic machinery carries
/// the property to the per-row surface #3124 is about.
#[tokio::test]
async fn a_dead_per_row_task_is_reported_as_an_error_and_stays_one() {
    // Held across every await that can raise the injected panic; the guard
    // filters ONLY the injected message and delegates every other panic to the
    // previous hook, so a genuine failure in a parallel test still prints.
    let _silence = crate::storage::producer_fault::silence_injected_panics();
    let (tx, rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(4);
    let task = tokio::spawn(async move {
        // Deliver one row, then die WITHOUT reporting — the exact shape the
        // discarded `JoinHandle` used to turn into a clean end of stream.
        let _ = tx.send(Ok(row(1))).await;
        panic!("{}", crate::storage::producer_fault::INJECTED_PANIC_MESSAGE);
    });
    let mut stream = RowScanStream::new_nested(rx, task);

    let first = stream.recv().await;
    let mut rest = Vec::new();
    for _ in 0..3 {
        rest.push(stream.recv().await);
    }
    // Dropped BEFORE the assertions: restoring a panic hook from a panicking
    // thread aborts the process, which would turn a legitimate failure below
    // into an unreadable SIGABRT.
    drop(_silence);

    assert!(
        matches!(first, Some(Ok((key, _))) if key.as_bytes() == [1]),
        "the row produced before the task died is still delivered"
    );
    for (attempt, outcome) in rest.into_iter().enumerate() {
        let msg = outcome
            .expect("a dead task must never report a clean end of stream")
            .expect_err("it must be an error")
            .to_string();
        assert!(
            msg.contains("per-row scan stream")
                && msg.contains("DIED without reporting")
                && msg.contains("TRUNCATED"),
            "attempt {attempt}: the error must name the surface, the dead task \
             and the truncation, got: {msg}"
        );
    }
}

/// The control: a per-row task that finishes normally yields the clean end of
/// stream, so the fail-closed join above cannot be passing for a trivial reason.
#[tokio::test]
async fn a_finished_per_row_task_yields_a_clean_end_of_stream() {
    let (tx, rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(4);
    let task = tokio::spawn(async move {
        let _ = tx.send(Ok(row(2))).await;
    });
    let mut stream = RowScanStream::new_nested(rx, task);
    assert!(
        matches!(stream.recv().await, Some(Ok(_))),
        "the row arrives"
    );
    assert!(
        stream.recv().await.is_none(),
        "a task that returned normally is a clean end of stream"
    );
    assert!(
        stream.recv().await.is_none(),
        "and stays one on a later poll"
    );
}

/// Issue #3106: a driver task that DIED is reported as an error, and that
/// verdict is STICKY — a consumer that keeps polling can never coax a
/// "proven-clean end of stream" `None` out of a dead scan.
///
/// The task is killed with a real panic (the same unwind a decode bug would
/// produce); its console noise is silenced by message so a genuine failure in
/// a parallel test is never masked.
#[tokio::test]
async fn a_dead_scan_task_is_reported_as_an_error_and_stays_one() {
    // Held for the whole test: the guard filters ONLY the injected message and
    // delegates every other panic to the previous hook, so an assertion failure
    // below (here or in a parallel test) still prints. The task panics
    // asynchronously, so a narrower scope would race the hook restore.
    let _silence = crate::storage::producer_fault::silence_injected_panics();
    let (tx, rx) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(4);
    let task = tokio::spawn(async move {
        // Deliver one batch, then die WITHOUT reporting — the exact shape the
        // discarded `JoinHandle` used to turn into a clean end of stream.
        let _ = tx
            .send(Ok(vec![(RowKey::new(vec![1]), ScanRow::Row(Vec::new()))]))
            .await;
        panic!("{}", crate::storage::producer_fault::INJECTED_PANIC_MESSAGE);
    });
    let mut stream = BatchedScanStream::new_nested(rx, task);

    let (first, rest) = {
        let first = stream.recv().await;
        let mut rest = Vec::new();
        for _ in 0..3 {
            rest.push(stream.recv().await);
        }
        (first, rest)
    };
    // The silencer (held above across every await that can raise the injected
    // panic) is dropped here, BEFORE the assertions: restoring a panic hook
    // from a panicking thread aborts the process, which would turn a
    // legitimate failure below into an unreadable SIGABRT.
    drop(_silence);
    assert!(
        matches!(first, Some(Ok(batch)) if batch.len() == 1),
        "the batch produced before the task died is still delivered"
    );
    for (attempt, outcome) in rest.into_iter().enumerate() {
        let msg = outcome
            .expect("a dead task must never report a clean end of stream")
            .expect_err("it must be an error")
            .to_string();
        assert!(
            msg.contains("DIED without reporting") && msg.contains("TRUNCATED"),
            "attempt {attempt}: the error must name the dead task and the \
             truncation, got: {msg}"
        );
    }
}

/// Build a stream whose task: sends one batch, CLOSES the channel, signals
/// that it has done so, then parks until released — so a `recv` that runs
/// after the close signal is guaranteed to be suspended in the JOIN, not in
/// the channel. `then_panic` decides how it finally exits.
///
/// The close signal is what makes the cancellation test deterministic: without
/// it, a `recv` could be cancelled while still awaiting the channel, which
/// exercises nothing.
fn stream_parked_after_closing_the_channel(
    then_panic: bool,
) -> (
    BatchedScanStream,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (tx, rx) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(4);
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = tx
            .send(Ok(vec![(RowKey::new(vec![7]), ScanRow::Row(Vec::new()))]))
            .await;
        drop(tx);
        let _ = closed_tx.send(());
        let _ = release_rx.await;
        if then_panic {
            panic!("{}", crate::storage::producer_fault::INJECTED_PANIC_MESSAGE);
        }
    });
    (
        BatchedScanStream::new_nested(rx, task),
        closed_rx,
        release_tx,
    )
}

/// Cancel a `recv` WHILE it is awaiting the join, then poll again.
///
/// `biased` polls the `recv` first, so it runs to the join (the channel is
/// already closed and drained) and returns `Pending` because the task is
/// parked; the ready branch then completes the `select!` and DROPS the `recv`
/// future — a cancellation landing exactly on the join await.
async fn cancel_a_recv_mid_join(stream: &mut BatchedScanStream) {
    tokio::select! {
        biased;
        _ = stream.recv() => panic!(
            "test precondition: the join must still be pending — the driver \
             task is parked, so recv cannot have resolved"
        ),
        _ = std::future::ready(()) => {}
    }
}

/// Issue #3106 (roborev blocker): a `recv` CANCELLED mid-join must not lose the
/// task's verdict. The pre-fix code moved the `JoinHandle` out of `self` before
/// awaiting it, so a cancellation dropped the handle (detaching the task) with
/// no verdict recorded — and the next `recv` short-circuited on the absent
/// handle to a clean end of stream for a task that then PANICKED. That is the
/// #3106 defect, re-entered through the door the fix built.
#[tokio::test]
async fn a_recv_cancelled_mid_join_still_reports_the_dead_task() {
    let (mut stream, closed, release) = stream_parked_after_closing_the_channel(true);

    assert!(
        matches!(stream.recv().await, Some(Ok(batch)) if batch.len() == 1),
        "the batch produced before the task parked is delivered"
    );
    closed
        .await
        .expect("the task signals that it closed the channel");
    cancel_a_recv_mid_join(&mut stream).await;

    // Now let the parked task die, and re-poll. The silencer is dropped BEFORE
    // any assertion: restoring a panic hook from a panicking thread aborts the
    // process, which would turn a legitimate failure below into an unreadable
    // SIGABRT instead of a reported assertion.
    let outcomes = {
        let _silence = crate::storage::producer_fault::silence_injected_panics();
        release.send(()).expect("release the parked task");
        let mut outcomes = Vec::new();
        for _ in 0..3 {
            outcomes.push(stream.recv().await);
        }
        outcomes
    };
    for (attempt, outcome) in outcomes.into_iter().enumerate() {
        let msg = outcome
            .expect(
                "a cancelled join must NOT lose the verdict: reporting a clean \
                 end of stream here is issue #3106 reintroduced",
            )
            .expect_err("the task panicked, so this must be an error")
            .to_string();
        assert!(
            msg.contains("DIED without reporting") && msg.contains("TRUNCATED"),
            "attempt {attempt}: the error must name the dead task and the \
             truncation, got: {msg}"
        );
    }
}

/// The complement: a cancelled join must not be turned into a spurious error
/// either. Same cancellation, but the task then finishes NORMALLY — the stream
/// must report the clean end of stream it genuinely observed.
#[tokio::test]
async fn a_recv_cancelled_mid_join_still_reports_a_clean_finish_as_clean() {
    let (mut stream, closed, release) = stream_parked_after_closing_the_channel(false);

    assert!(
        matches!(stream.recv().await, Some(Ok(_))),
        "the batch arrives"
    );
    closed
        .await
        .expect("the task signals that it closed the channel");
    cancel_a_recv_mid_join(&mut stream).await;

    release.send(()).expect("release the parked task");
    assert!(
        stream.recv().await.is_none(),
        "a task that ran to completion is a clean end of stream, even though an \
         earlier recv was cancelled mid-join"
    );
    assert!(
        stream.recv().await.is_none(),
        "and stays clean on a later poll"
    );
}

/// A CANCELLED (aborted) producer is reported as [`Error::Cancelled`], not as the
/// panic-flavoured internal error — and that cause survives the sticky re-report.
///
/// Nothing in the crate aborts these tasks today; this pins that the documented
/// cancellation coverage is REAL rather than aspirational, so the first caller that
/// does abort a scan gets an honest cause instead of "an internal invariant was
/// violated".
#[tokio::test]
async fn a_cancelled_scan_task_is_reported_as_cancelled_and_stays_cancelled() {
    let (tx, rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(4);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = started_tx.send(());
        // Park forever holding the sender: only the abort below can end this.
        std::future::pending::<()>().await;
        drop(tx);
    });
    started_rx.await.expect("the task started");
    task.abort();
    let mut stream = RowScanStream::new_nested(rx, task);

    for attempt in 0..3 {
        let err = stream
            .recv()
            .await
            .expect("an aborted producer must never report a clean end of stream")
            .expect_err("it must be an error");
        assert!(
            matches!(err, Error::Cancelled),
            "attempt {attempt}: a cancelled task is Error::Cancelled, not an \
             internal dead-task report, got: {err}"
        );
    }
}

/// The control: a task that finishes normally yields the clean end of stream,
/// so the fail-closed join above cannot be passing for a trivial reason.
#[tokio::test]
async fn a_finished_scan_task_yields_a_clean_end_of_stream() {
    let (tx, rx) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(4);
    let task = tokio::spawn(async move {
        let _ = tx
            .send(Ok(vec![(RowKey::new(vec![2]), ScanRow::Row(Vec::new()))]))
            .await;
    });
    let mut stream = BatchedScanStream::new_nested(rx, task);
    assert!(
        matches!(stream.recv().await, Some(Ok(_))),
        "the batch arrives"
    );
    assert!(
        stream.recv().await.is_none(),
        "a task that returned normally is a clean end of stream"
    );
    assert!(
        stream.recv().await.is_none(),
        "and stays one on a later poll"
    );
}
