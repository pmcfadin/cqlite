//! A scan channel whose close is PROVEN, not assumed — for EVERY streaming scan
//! surface, not just the batched one (issues #3106, #3124).
//!
//! # The defect class this type exists to make unrepresentable
//!
//! Every streaming scan in this crate is "a spawned producer that owns the sending
//! half of a bounded channel and reports a failure by sending `Err(..)`". If the
//! producer instead UNWINDS — a decode panic, an abort, a cancelled task — it drops
//! its sender with NO error and NO terminator. A consumer that reads a channel
//! close as "the scan finished" then reports a SHORT result set as a complete,
//! successful scan: fewer rows, no error, no log line.
//!
//! Issue #3106 closed that hole for ONE surface (the batched single-source scan) by
//! pairing the channel with its producer's `JoinHandle`. Issue #3124 found the same
//! hole on the ≠1-generation (query-engine full scan) path, at four more
//! boundaries: the fan-out k-way merge task, each per-reader per-row sub-scan, the
//! per-row → batch re-chunker's source, and the windowed forwarder. Rather than
//! grow a fourth bespoke protocol, the #3106 mechanism is generalised HERE over the
//! channel's item type, so both surfaces share ONE implementation:
//!
//! * [`RowScanStream`] — per-row items, `Result<(RowKey, ScanRow)>`
//!   (`SSTableReader::scan_stream`, `SSTableManager::scan_stream`,
//!   `generation_merge::stream_generations_for_read`).
//! * [`BatchedScanStream`] — batch items, `Result<Vec<(RowKey, ScanRow)>>`
//!   (`SSTableReader::scan_stream_batched`, `SSTableManager::scan_stream_batched`).
//!
//! Consumers are unchanged in shape: [`JoinedStream::recv`] has the same
//! `async fn(&mut self) -> Option<Result<T>>` signature `mpsc::Receiver::recv` had,
//! so every `while let Some(item) = stream.recv().await` loop keeps working — and
//! now fails closed.

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::types::ScanRow;
use crate::{Error, Result, RowKey};

/// An item type a [`JoinedStream`] can carry, plus the human name of the surface
/// that carries it.
///
/// The name exists only so a dead-producer error says WHICH stream died
/// (`per-row scan stream` vs `batched scan stream`) instead of being anonymous. It
/// is an associated const rather than a constructor argument so a new stream
/// surface cannot be wired up with a copy-pasted, wrong label — and so the
/// existing `JoinedStream::new(rx, task)` call sites need no churn.
pub trait ScanStreamItem {
    /// How this stream is named in a failure message.
    const STREAM_KIND: &'static str;
}

impl ScanStreamItem for (RowKey, ScanRow) {
    const STREAM_KIND: &'static str = "per-row scan stream";
}

impl ScanStreamItem for Vec<(RowKey, ScanRow)> {
    const STREAM_KIND: &'static str = "batched scan stream";
}

/// Consumer handle for a streaming scan: the item channel PLUS the producer task
/// that feeds it, so end-of-stream is an OBSERVED fact rather than an assumption.
///
/// Dropping it drops the receiver, which makes the producer's next send fail and
/// unwinds it cooperatively (the pre-existing backpressure/teardown behaviour); the
/// task is deliberately NOT joined on drop, so an abandoned scan never blocks the
/// consumer.
pub struct JoinedStream<T: ScanStreamItem> {
    rx: mpsc::Receiver<Result<T>>,
    /// What is KNOWN about the producer task. See [`TaskState`] — the point of the
    /// enum is that there is no representable "the handle is gone and I never saw a
    /// verdict" state to accidentally read as success.
    task: TaskState,
}

/// What a [`JoinedStream`] knows about its producer task (issue #3106, roborev).
///
/// Modelled as a state machine rather than `Option<JoinHandle>` + a `died` flag
/// because that pair had an UNREPRESENTED third state — handle taken, verdict not
/// yet observed — which a cancelled join produced and which then read as a clean
/// end of stream. Here every state either OWNS the handle (so the join can still be
/// completed later) or carries an observed verdict.
enum TaskState {
    /// Not yet joined; the handle is still owned HERE. A cancelled `recv` leaves the
    /// stream in this state, so the next `recv` can still join.
    Running(JoinHandle<()>),
    /// Joined, and the join returned `Ok(())`: the scan PROVABLY ran to completion.
    /// The only state from which end-of-stream is reported.
    Finished,
    /// Joined, and the join returned a `JoinError` (panic or abort). STICKY: every
    /// later `recv` re-reports the failure, mirroring the sibling boundary
    /// (`QueryRowStream::terminated`), so a retrying consumer — these streams are
    /// public API, returned by `SSTableManager`/`StorageEngine` — can never
    /// downgrade a dead task to a clean end of stream.
    Died,
}

impl<T: ScanStreamItem> JoinedStream<T> {
    /// Pair a scan channel with the task that drives it.
    pub(in crate::storage::sstable) fn new(rx: mpsc::Receiver<Result<T>>, task: JoinHandle<()>) -> Self {
        Self {
            rx,
            task: TaskState::Running(task),
        }
    }

    /// Next item, or `None` at a PROVEN-clean end of stream.
    ///
    /// When the channel closes, the producer task is joined: a task that returned
    /// normally yields `None` (the scan really finished), while a task that DIED —
    /// panicked, or was cancelled/aborted — yields `Some(Err(..))`. That is the
    /// whole point: a dead producer must never be indistinguishable from a finished
    /// one (issues #3106, #3124).
    ///
    /// A dead task is STICKY: every subsequent call re-reports the failure, so
    /// polling again can never downgrade it to a clean end of stream.
    ///
    /// # Cancellation safety (issue #3106, roborev)
    ///
    /// This method is cancel-safe in the way that matters here: cancelling it
    /// (`tokio::select!`, `timeout`, …) while it is awaiting the JOIN must not lose
    /// the task's verdict. The handle is therefore polled IN PLACE — it is never
    /// moved out of `self` before a result is observed — so a cancelled `recv`
    /// leaves the stream in [`TaskState::Running`] and the next `recv` simply joins
    /// again. Moving the handle out first (`self.task.take()?`) is exactly what
    /// reintroduced the #3106 defect: the dropped handle DETACHED the task, no
    /// verdict was ever recorded, and the following `recv` short-circuited to a
    /// clean end of stream for a task that may have panicked.
    pub async fn recv(&mut self) -> Option<Result<T>> {
        if let TaskState::Died = self.task {
            return Some(Err(dead_scan_task_error_after_report::<T>()));
        }
        if let Some(item) = self.rx.recv().await {
            return Some(item);
        }
        // Channel closed: the producer dropped its sender. Join it to learn WHY.
        let outcome = match &mut self.task {
            // `JoinHandle: Future + Unpin`, so `&mut JoinHandle` is itself a future:
            // awaiting it polls the join WITHOUT taking ownership. A cancellation
            // here drops only this borrow.
            TaskState::Running(handle) => handle.await,
            // A proven-clean completion is the ONLY route to end-of-stream.
            TaskState::Finished => return None,
            TaskState::Died => return Some(Err(dead_scan_task_error_after_report::<T>())),
        };
        // No `.await` between observing the outcome and recording it, so the verdict
        // cannot be lost to a cancellation.
        match outcome {
            Ok(()) => {
                self.task = TaskState::Finished;
                None
            }
            Err(join_err) => {
                self.task = TaskState::Died;
                Some(Err(dead_scan_task_error::<T>(&join_err)))
            }
        }
    }
}

/// The fail-closed error for a scan producer task that died without reporting
/// (issues #3106, #3124).
///
/// [`Error::Internal`] (not `Corruption`, not `Storage`): nothing suggests the
/// `Data.db` is bad — an internal invariant was violated — and `Internal` is
/// `is_recoverable() == false`, which is honest for a deterministic failure that a
/// retry would reproduce. The panic message is carried in the payload so the
/// failure is diagnosable rather than anonymous.
fn dead_scan_task_error<T: ScanStreamItem>(join_err: &tokio::task::JoinError) -> Error {
    Error::internal(format!(
        "{}: the scan task DIED without reporting ({join_err}) — the result set is \
         TRUNCATED and cannot be reported as a complete scan (issues #3106/#3124)",
        T::STREAM_KIND
    ))
}

/// Re-report for a consumer that keeps polling after the dead-task error. The
/// `JoinError` is consumed by the join, so this restates the verdict rather than
/// re-deriving it — what matters is that it is still an ERROR, never a clean end of
/// stream.
fn dead_scan_task_error_after_report<T: ScanStreamItem>() -> Error {
    Error::internal(format!(
        "{}: the scan task DIED without reporting (already reported) — the result \
         set is TRUNCATED and cannot be reported as a complete scan (issues \
         #3106/#3124)",
        T::STREAM_KIND
    ))
}

/// Per-row streaming scan handle (issue #3124): `SSTableReader::scan_stream`,
/// `SSTableManager::scan_stream`, `StorageEngine::scan_stream` and the
/// cross-generation `stream_generations_for_read` all return this instead of a bare
/// `mpsc::Receiver`, so a producer that dies mid-scan is an ERROR at every one of
/// those boundaries rather than a short, successful-looking result set.
pub type RowScanStream = JoinedStream<(RowKey, ScanRow)>;

#[cfg(test)]
mod tests {
    use super::*;

    fn row(byte: u8) -> (RowKey, ScanRow) {
        (RowKey::new(vec![byte]), ScanRow::Row(Vec::new()))
    }

    /// Issue #3124: the PER-ROW flavour of the #3106 property — a producer task that
    /// DIED is reported as an error, and that verdict is sticky. The batched flavour
    /// is pinned by the sibling tests in `batched_scan_stream.rs`; this proves the
    /// generic machinery carries the property to the surface #3124 is about.
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
        let mut stream = RowScanStream::new(rx, task);

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
        let mut stream = RowScanStream::new(rx, task);
        assert!(matches!(stream.recv().await, Some(Ok(_))), "the row arrives");
        assert!(
            stream.recv().await.is_none(),
            "a task that returned normally is a clean end of stream"
        );
        assert!(
            stream.recv().await.is_none(),
            "and stays one on a later poll"
        );
    }
}
