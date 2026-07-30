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
    /// the first time the channel reports close; a later `recv` on an
    /// already-joined stream is a plain end of stream.
    task: Option<JoinHandle<()>>,
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
        }
    }

    /// Next batch, or `None` at a PROVEN-clean end of stream.
    ///
    /// When the channel closes, the driver task is joined: a task that returned
    /// normally yields `None` (the scan really finished), while a task that DIED
    /// — panicked, or was cancelled/aborted — yields `Some(Err(..))`. That is the
    /// whole point: a dead producer must never be indistinguishable from a
    /// finished one (issue #3106).
    pub async fn recv(&mut self) -> Option<Result<Vec<(RowKey, ScanRow)>>> {
        if let Some(item) = self.rx.recv().await {
            return Some(item);
        }
        // Channel closed: the driver dropped its sender. Join it to learn WHY.
        // `take()` makes this happen exactly once — a second `recv` after a
        // clean join is an ordinary end of stream.
        let task = self.task.take()?;
        match task.await {
            Ok(()) => None,
            Err(join_err) => Some(Err(dead_scan_task_error(&join_err))),
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

impl SSTableReader {
    /// Decode one `Data.db` block for the batched (non-stitching) streaming scan.
    ///
    /// A named seam rather than an inline call for two reasons: it pins
    /// `read_shadowing = true` for this scan in ONE place, and it is where the
    /// test-only inner-boundary fault checkpoint lives (issue #3106) — arming it
    /// unwinds the scan task exactly as a real decode panic would, which is how
    /// the fail-closed join above is PROVEN rather than asserted by inspection.
    pub(super) fn parse_batched_block(
        &self,
        block: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        now_secs: Option<i64>,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        crate::storage::producer_fault::inner_scan_decode_checkpoint();
        self.parse_block_entries_at_now(block, schema, true, now_secs)
    }
}
