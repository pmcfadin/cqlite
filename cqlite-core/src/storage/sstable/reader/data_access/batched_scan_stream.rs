//! The batched streaming scan's consumer handle — a channel whose close is
//! PROVEN, not assumed (issue #3106).
//!
//! # The boundary this closes
//!
//! `scan_stream_batched_admitted` (`sequential.rs`) drives the scan in a spawned
//! `tokio` task that owns the sending half of a bounded channel and reports a
//! failure by sending `Err(..)`. Before this type it returned the bare
//! `mpsc::Receiver` and DISCARDED the task's `JoinHandle`, so a task that
//! UNWOUND — a decode panic in `parse_block_entries_at_now`, which on the
//! non-stitching (uncompressed) branch runs directly inside that task, or any
//! future panic on the walk — dropped its sender with no error and no terminator.
//! Every consumer read that close as "the scan finished":
//!
//! * `query_rows::drive_full_scan_rows` returned `Ok(())`, its own thread then
//!   sent the #3106 `Done` sentinel, and the Flight `do_get` completed
//!   SUCCESSFULLY with a truncated result set — the #3106 defect exactly, one
//!   layer below the channel #3106 first fixed, on the arm a `do_get` with no
//!   token filter takes (i.e. the reported repro);
//! * the query engine's streaming SELECT / aggregate folds
//!   (`select_executor::streaming`, `stream_agg`, `executor::streaming_scan_rows`)
//!   returned short row sets / wrong aggregates the same way.
//!
//! [`BatchedScanStream`] owns the handle instead, so a channel close is
//! DISAMBIGUATED: the task is joined and a `JoinError` (panic or cancellation)
//! surfaces as `Some(Err(..))` rather than `None`. This mirrors what the windowed
//! sub-path already did internally for its own two child tasks
//! (`scan_stream_windowed`'s `parse_task.await` / feed `join_err` arms map a
//! `JoinError` to an error), so the two arms no longer diverge on whether a dead
//! producer is an error.
//!
//! Consumers are unchanged: [`BatchedScanStream::recv`] has the same
//! `async fn(&mut self) -> Option<Result<Vec<(RowKey, ScanRow)>>>` shape
//! `mpsc::Receiver::recv` had, so every `while let Some(batch) = stream.recv().await`
//! loop keeps working — and now fails closed.

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::super::SSTableReader;
use crate::types::{ScanRow, TableId};
use crate::{Error, Result, RowKey};

/// Consumer handle for the batched streaming scan: the batch channel PLUS the
/// driver task that feeds it, so end-of-stream is an observed fact.
///
/// Dropping it drops the receiver, which makes the driver task's next send fail
/// and unwinds it cooperatively (the pre-existing backpressure/teardown
/// behaviour); the task is deliberately NOT joined on drop, so an abandoned scan
/// never blocks the consumer.
pub struct BatchedScanStream {
    rx: mpsc::Receiver<Result<Vec<(RowKey, ScanRow)>>>,
    /// The driver task. `Option` because it is JOINED — and therefore consumed —
    /// the first time the channel reports close.
    task: Option<JoinHandle<()>>,
    /// Whether the join already reported a DEAD task (issue #3106, roborev).
    ///
    /// Makes the failure STICKY, mirroring the sibling boundary
    /// (`QueryRowStream::terminated`): the join can only happen once, so without
    /// this a consumer that polls [`Self::recv`] again after the error would get a
    /// bare `None` — which this type DEFINES as a proven-clean end of stream. That
    /// would hand a retrying external consumer (this is public API, returned by
    /// `SSTableManager`/`StorageEngine::scan_stream_batched`) exactly the silent
    /// truncation the join exists to prevent. No in-tree consumer polls past an
    /// error — all four propagate with `?` — so this is a contract guard, not a
    /// live bug fix.
    task_died: bool,
}

impl BatchedScanStream {
    /// Pair a batch channel with the task that drives it.
    pub(in crate::storage::sstable) fn new(
        rx: mpsc::Receiver<Result<Vec<(RowKey, ScanRow)>>>,
        task: JoinHandle<()>,
    ) -> Self {
        Self {
            rx,
            task: Some(task),
            task_died: false,
        }
    }

    /// Next batch, or `None` at a PROVEN-clean end of stream.
    ///
    /// When the channel closes, the driver task is joined: a task that returned
    /// normally yields `None` (the scan really finished), while a task that DIED
    /// — panicked, or was cancelled/aborted — yields `Some(Err(..))`. That is the
    /// whole point: a dead producer must never be indistinguishable from a
    /// finished one (issue #3106).
    ///
    /// A dead task is STICKY: every subsequent call re-reports the failure, so
    /// polling again can never downgrade it to a clean end of stream.
    pub async fn recv(&mut self) -> Option<Result<Vec<(RowKey, ScanRow)>>> {
        if self.task_died {
            return Some(Err(dead_scan_task_error_after_report()));
        }
        if let Some(item) = self.rx.recv().await {
            return Some(item);
        }
        // Channel closed: the driver dropped its sender. Join it to learn WHY.
        // `take()` makes the join happen exactly once; the `task_died` flag (not
        // the absent handle) is what a later call reads.
        let task = self.task.take()?;
        match task.await {
            Ok(()) => None,
            Err(join_err) => {
                self.task_died = true;
                Some(Err(dead_scan_task_error(&join_err)))
            }
        }
    }
}

/// The fail-closed error for a batched-scan driver task that died without
/// reporting (issue #3106).
///
/// [`Error::Internal`] (not `Corruption`, not `Storage`): nothing suggests the
/// `Data.db` is bad — an internal invariant was violated — and `Internal` is
/// `is_recoverable() == false`, which is honest for a deterministic failure that
/// a retry would reproduce. The panic message is carried in the payload so the
/// failure is diagnosable rather than anonymous.
fn dead_scan_task_error(join_err: &tokio::task::JoinError) -> Error {
    Error::internal(format!(
        "batched scan stream: the scan task DIED without reporting ({join_err}) — \
         the result set is TRUNCATED and cannot be reported as a complete scan \
         (issue #3106)"
    ))
}

/// Re-report for a consumer that keeps polling after the dead-task error. The
/// `JoinError` is consumed by the join, so this restates the verdict rather than
/// re-deriving it — what matters is that it is still an ERROR, never a clean end
/// of stream.
fn dead_scan_task_error_after_report() -> Error {
    Error::internal(
        "batched scan stream: the scan task DIED without reporting (already \
         reported) — the result set is TRUNCATED and cannot be reported as a \
         complete scan (issue #3106)",
    )
}

impl SSTableReader {
    /// Open the batched streaming scan's cursor — the FIRST thing the driver task
    /// does, ABOVE the `requires_chunk_stitching()` branch, i.e. on every format.
    ///
    /// A named seam because it is the SINGLE test-only fault checkpoint for this
    /// task (issue #3106), and it is here precisely because it is
    /// format-branch-independent: a checkpoint inside either branch fires only for
    /// the formats that take that branch, so it can silently not fire and leave a
    /// test passing vacuously. Killing the task here reproduces exactly the
    /// condition the fix is about — the task's sender drops with no error and no
    /// terminator — for any reader, and the join that catches it wraps the whole
    /// task, so the property proven is branch-agnostic.
    pub(super) async fn open_batched_scan_cursor(&self) -> Result<super::ScanCursor> {
        crate::storage::producer_fault::inner_scan_task_checkpoint(|| self.file_path());
        self.new_scan_cursor().await
    }

    /// Decode one `Data.db` block for the batched NON-STITCHING streaming scan,
    /// pinning `read_shadowing = true` for this scan in ONE place.
    ///
    /// Deliberately carries NO fault checkpoint (issue #3106, roborev): it is
    /// reached only by readers whose format takes the non-stitching branch, and no
    /// fixture in the tree does, so a checkpoint here would be an armable-but-never-
    /// armed seam — latent confusion suggesting coverage that does not exist. The
    /// task-level checkpoint in [`Self::open_batched_scan_cursor`] covers both
    /// branches, which is what the join it proves is scoped to anyway.
    pub(super) fn parse_batched_block(
        &self,
        block: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        now_secs: Option<i64>,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        self.parse_block_entries_at_now(block, schema, true, now_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut stream = BatchedScanStream::new(rx, task);

        assert!(
            matches!(stream.recv().await, Some(Ok(batch)) if batch.len() == 1),
            "the batch produced before the task died is still delivered"
        );
        for attempt in 0..3 {
            let err = stream
                .recv()
                .await
                .expect("a dead task must never report a clean end of stream")
                .expect_err("it must be an error");
            let msg = err.to_string();
            assert!(
                msg.contains("DIED without reporting") && msg.contains("TRUNCATED"),
                "attempt {attempt}: the error must name the dead task and the \
                 truncation, got: {msg}"
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
        let mut stream = BatchedScanStream::new(rx, task);
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
}
