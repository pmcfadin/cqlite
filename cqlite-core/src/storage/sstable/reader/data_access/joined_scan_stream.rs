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
//!
//! # The boundary #3106 closed first (the batched single-source scan)
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
//! # The #3124 boundaries (the multi-generation path)
//!
//! `SSTableManager::scan_stream`'s fan-out k-way merge task, each per-generation
//! `scan_stream_admitted` sub-scan, and the per-row → batch re-chunker's source had
//! the identical shape on the ≠1-generation path, and the windowed forwarder
//! discarded its `JoinError` outright. Their wiring lives in
//! `storage/sstable/scan_stream_fanout.rs` and
//! `storage/sstable/reader/scan_stream_forwarder.rs`; the mechanism they all use is
//! this type.

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::super::SSTableReader;
use crate::observability::read_metrics::ReadOpMeter;
use crate::types::{ScanRow, TableId};
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

/// How a stream item's rows are accounted into the read-operation meter (issue #1701).
///
/// A FUNCTION POINTER stored on the stream, deliberately NOT a method on
/// [`ScanStreamItem`]: that trait is PUBLIC (re-exported through
/// `reader::{RowScanStream, BatchedScanStream}`, whose defining module is not
/// nameable), so adding a required method to it would break every downstream
/// implementation at compile time — and its parameter type [`ReadOpMeter`] is
/// crate-private, so an external implementer could not even NAME the signature it was
/// being asked to write. The metric wiring therefore stays entirely off the public
/// surface: the pointer is a private field, set by the crate-internal constructors.
type ItemAccounting<T> = fn(&T, &mut ReadOpMeter);

/// Per-item accounting for the PER-ROW surface: one row, and a partition boundary when
/// its key differs from the previous row's.
fn account_row_item(item: &(RowKey, ScanRow), meter: &mut ReadOpMeter) {
    meter.record_row(&item.0);
}

/// Per-item accounting for the BATCHED surface: every row of the batch. Per-item
/// BOOKKEEPING, not per-item EMISSION — the meter bumps two `u64`s and the single
/// counter add / duration record happens once, when the operation ends.
///
/// `&Vec<_>` (not `&[_]`) because the signature must match `ItemAccounting<T>` for
/// `T = Vec<(RowKey, ScanRow)>` exactly.
#[allow(clippy::ptr_arg)]
fn account_batch_item(items: &Vec<(RowKey, ScanRow)>, meter: &mut ReadOpMeter) {
    for (key, _) in items {
        meter.record_row(key);
    }
}

/// Accounting for a stream that is NOT a metered read operation. Its meter is inert,
/// so this would be a no-op either way; naming it keeps the field non-optional.
fn account_nothing<T>(_item: &T, _meter: &mut ReadOpMeter) {}

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
    /// This read OPERATION's row/partition/duration accounting (issue #1701).
    /// INERT unless the stream was built by [`JoinedStream::new_measured`], because
    /// only a TOP-LEVEL operation may be measured: a fan-out merge's per-generation
    /// sub-scan and the per-row → batch re-chunker would otherwise count the same
    /// rows a second and third time. Emits once — at the observed end of stream, or
    /// on drop for a scan the consumer abandoned (the `LIMIT` shape).
    meter: ReadOpMeter,
    /// How to account one delivered item into `meter` (see [`ItemAccounting`]). A
    /// private field rather than a public-trait method, so the metric wiring adds
    /// nothing to [`ScanStreamItem`]'s published surface.
    account: ItemAccounting<T>,
    /// Whether a failed scan is reported into `cqlite.errors.total` HERE, and
    /// whether it already has (issue #1704). See [`ScanErrorReporting`].
    errors: ScanErrorReporting,
}

/// Whether THIS stream is the boundary that counts a failed scan into
/// `cqlite.errors.total{category, subsystem="reader"}` (issue #1704).
///
/// # Why the grain is per-OPERATION, not per-stream
///
/// Streams nest: a fan-out k-way merge drains one [`JoinedStream`] per generation
/// and re-sends a sub-scan's `Err` on its own channel, and the per-row → batch
/// re-chunker drains a per-row stream and re-sends its `Err` too. The SAME failure
/// therefore crosses two or three `recv` boundaries, and a per-stream emission
/// would count one failed query two or three times.
///
/// This is the identical grain problem [`ReadOpMeter`] already solved for
/// `read.rows`/`read.partitions`, and it takes the identical answer: exactly the
/// streams built by [`JoinedStream::new_measured`] — the TOP-LEVEL read operations —
/// report, and every nested stream built by [`JoinedStream::new`] delegates to the
/// one above it. Keeping the two decisions in the same two constructors is what
/// stops them drifting apart.
enum ScanErrorReporting {
    /// A nested stream: an enclosing measured stream sees (and counts) the same
    /// error when the merge/re-chunker forwards it.
    Delegated,
    /// A top-level operation that has not yet failed.
    Pending,
    /// A top-level operation whose failure has been counted. LATCHED: the sticky
    /// dead-task verdict is re-REPORTED to the consumer on every later `recv`
    /// (see [`TaskState::Died`]) and must not be re-COUNTED, and neither must a
    /// consumer that keeps polling after a terminal channel error.
    Reported,
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
    /// Joined, and the join returned a `JoinError`. STICKY: every later `recv`
    /// re-reports the failure, mirroring the sibling boundary
    /// (`QueryRowStream::terminated`), so a retrying consumer — these streams are
    /// public API, returned by `SSTableManager`/`StorageEngine` — can never
    /// downgrade a dead task to a clean end of stream.
    ///
    /// `cancelled` remembers WHICH kind of `JoinError` it was, so the sticky
    /// re-report keeps saying the same thing the first report said (a cancellation
    /// re-reported as a panic would be a lie about the cause).
    Died { cancelled: bool },
}

impl<T: ScanStreamItem> JoinedStream<T> {
    /// Pair a scan channel with the task that drives it, WITHOUT read-metric
    /// accounting — for a stream that is not a top-level read operation (a fan-out
    /// sub-scan, a re-chunker over an already-measured source, a test stand-in).
    pub(in crate::storage::sstable) fn new(
        rx: mpsc::Receiver<Result<T>>,
        task: JoinHandle<()>,
    ) -> Self {
        Self {
            rx,
            task: TaskState::Running(task),
            meter: ReadOpMeter::inert(),
            account: account_nothing,
            // Nested stream: the enclosing measured stream counts the failure it
            // forwards (issue #1704).
            errors: ScanErrorReporting::Delegated,
        }
    }

    /// [`new`](Self::new) for a TOP-LEVEL read operation, whose rows, partitions and
    /// duration are reported through the catalog read metrics (issue #1701).
    ///
    /// `format` is the single-SSTable format label (`"big"` / `"bti"`) when the
    /// stream reads ONE SSTable, or `None` for a cross-generation merge, whose
    /// reconciled rows come from possibly mixed-format inputs — the
    /// format-agnostic grain [`crate::observability::catalog::READ_ROWS`] documents.
    pub(in crate::storage::sstable) fn new_measured(
        rx: mpsc::Receiver<Result<T>>,
        task: JoinHandle<()>,
        meter: ReadOpMeter,
        account: ItemAccounting<T>,
    ) -> Self {
        Self {
            rx,
            task: TaskState::Running(task),
            meter,
            account,
            // Top-level read operation: this is the ONE boundary that counts a
            // failed scan (issue #1704).
            errors: ScanErrorReporting::Pending,
        }
    }

    /// Count a terminal scan failure into `cqlite.errors.total` exactly once
    /// (issue #1704).
    ///
    /// A pure side effect: the caller returns the SAME `Err` it would have returned
    /// before, with the same variant and message. The category is derived by the
    /// classifier via [`crate::observability::record_error`] — never chosen here.
    fn count_scan_error(&mut self, err: &Error) {
        if matches!(self.errors, ScanErrorReporting::Pending) {
            self.errors = ScanErrorReporting::Reported;
            crate::observability::record_error(err, "reader");
        }
    }

    /// Next item, or `None` at a PROVEN-clean end of stream.
    ///
    /// When the channel closes, the producer task is joined: a task that returned
    /// normally yields `None` (the scan really finished), while a task that DIED
    /// yields `Some(Err(..))` — a panic as the fail-closed
    /// [`dead_scan_task_error`], a CANCELLED/aborted task as [`Error::Cancelled`],
    /// which is the honest cause for a scan someone stopped. That is the whole
    /// point: a dead producer must never be indistinguishable from a finished one
    /// (issues #3106, #3124).
    ///
    /// A dead task is STICKY: every subsequent call re-reports the failure, so
    /// polling again can never downgrade it to a clean end of stream.
    ///
    /// # Error counting (issue #1704)
    ///
    /// Every `Err` this method returns — a producer-reported terminal item, a dead
    /// task, a cancellation, and the sticky re-reports of the last two — counts ONCE
    /// into `cqlite.errors.total{category, subsystem="reader"}` when this stream is
    /// the top-level operation (see [`ScanErrorReporting`]). The counting is a pure
    /// side effect: the `Err` returned is bit-for-bit the one that would have been
    /// returned before, and the category comes from the classifier, never from here.
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
        if let TaskState::Died { cancelled } = self.task {
            self.meter.finish();
            let err = sticky_dead_task_error::<T>(cancelled);
            // Latched (issue #1704): the join-error arm below already counted this
            // failure, so re-reporting it to a still-polling consumer adds nothing.
            self.count_scan_error(&err);
            return Some(Err(err));
        }
        if let Some(item) = self.rx.recv().await {
            // Read-metric bookkeeping (issue #1701) for the rows this item
            // DELIVERED. An `Err` item carries no rows, and the meter's own totals
            // are emitted at the terminal transition below.
            match &item {
                Ok(delivered) => (self.account)(delivered, &mut self.meter),
                // The producer reported a failure as a terminal stream item — the
                // ordinary mid-scan failure path (issue #1704). Counted here, at the
                // one boundary every streaming surface crosses, and passed through
                // untouched.
                Err(e) => self.count_scan_error(e),
            }
            return Some(item);
        }
        // Terminal: the producer is finished, dead, or about to be joined. Emit this
        // operation's read totals exactly once (idempotent; `Drop` is the backstop
        // for a consumer that stops polling early).
        self.meter.finish();
        // Channel closed: the producer dropped its sender. Join it to learn WHY.
        let outcome = match &mut self.task {
            // `JoinHandle: Future + Unpin`, so `&mut JoinHandle` is itself a future:
            // awaiting it polls the join WITHOUT taking ownership. A cancellation
            // here drops only this borrow.
            TaskState::Running(handle) => handle.await,
            // A proven-clean completion is the ONLY route to end-of-stream.
            TaskState::Finished => return None,
            TaskState::Died { cancelled } => {
                let err = sticky_dead_task_error::<T>(*cancelled);
                self.count_scan_error(&err);
                return Some(Err(err));
            }
        };
        // No `.await` between observing the outcome and recording it, so the verdict
        // cannot be lost to a cancellation.
        match outcome {
            Ok(()) => {
                self.task = TaskState::Finished;
                None
            }
            Err(join_err) => {
                // A CANCELLED task (aborted, or its runtime shut down) is a distinct
                // cause from a panic and gets the error the doc above already promised
                // for it: `Error::Cancelled`. Nothing in the crate aborts these tasks
                // today, so this arm is about the doc being TRUE — and about the next
                // caller that does abort one getting an honest cause rather than an
                // internal-invariant-violated report.
                let cancelled = join_err.is_cancelled();
                self.task = TaskState::Died { cancelled };
                let err = if cancelled {
                    Error::Cancelled
                } else {
                    dead_scan_task_error::<T>(&join_err)
                };
                // A dead producer IS a failed scan (issue #1704) — the case that
                // previously reached the consumer as an error while the operator's
                // error dashboard stayed clean.
                self.count_scan_error(&err);
                Some(Err(err))
            }
        }
    }
}

impl<T: ScanStreamItem> Drop for JoinedStream<T> {
    /// Account the rows the producer had ALREADY MATERIALISED and enqueued but this
    /// consumer never polled, then emit this operation's read totals (issue #1701,
    /// roborev F2).
    ///
    /// # Why draining is the honest accounting
    ///
    /// Rows are accounted as they cross [`recv`](JoinedStream::recv), which is the
    /// only place this type sees them. A `LIMIT` consumer — the COMMON case, and the
    /// one real caller of the per-row streaming surface — stops polling early and
    /// drops the stream while the bounded channel still holds rows the producer
    /// decoded, sent, and can never take back. Emitting only the polled rows
    /// understated `read.rows`/`read.partitions` while `read.bytes` still counted the
    /// chunks those rows were decoded from, so an amplification ratio built from the
    /// two was wrong in the exact case it matters, and the documented
    /// "rows materialised by the read path" was contradicted by its own metric.
    ///
    /// `try_recv` is non-blocking and the receiver is dropped immediately after, so
    /// this neither waits for the producer nor changes the teardown behaviour. Each
    /// item is accounted EXACTLY once: a polled row was accounted in `recv` and is no
    /// longer in the channel; a buffered row is accounted here and was never polled.
    fn drop(&mut self) {
        // R2: CLOSE the channel BEFORE draining. Without it a producer still running
        // can complete a send in the window between the last `try_recv() == Empty` and
        // the receiver's destruction, and that row — already decoded, already
        // enqueued — is discarded AFTER the meter finished, making an abandoned
        // scan's totals race-dependent. `close()` makes every in-flight and later
        // send fail, so the set of enqueued rows is FIXED before the drain reads it;
        // buffered messages stay receivable, which is what the drain then collects.
        self.rx.close();
        while let Ok(item) = self.rx.try_recv() {
            if let Ok(delivered) = &item {
                (self.account)(delivered, &mut self.meter);
            }
        }
        self.meter.finish();
    }
}

impl RowScanStream {
    /// [`new_measured`](JoinedStream::new_measured) for the PER-ROW surface, supplying
    /// its accounting (issue #1701).
    ///
    /// Exists so the measured row-stream constructors OUTSIDE this module (the fan-out
    /// merge and the cross-generation merge) need no access to the private accounting
    /// function — keeping the metric wiring off both the public `ScanStreamItem` trait
    /// and the crate's re-export surface.
    ///
    /// The CALLER starts the meter (issue #1701, roborev round 5): a stream whose
    /// construction does real work — the cross-generation merge opens every generation
    /// and builds a `KWayMerger` before it can signal readiness — must start timing at
    /// its own function entry, or construction latency falls outside `read.duration`
    /// and a construction FAILURE reports none at all (no stream is ever returned).
    /// That is the entry-at-function rule `manager_point_read`'s module doc states.
    pub(in crate::storage::sstable) fn new_measured_rows(
        rx: mpsc::Receiver<Result<(RowKey, ScanRow)>>,
        task: JoinHandle<()>,
        meter: ReadOpMeter,
    ) -> Self {
        Self::new_measured(rx, task, meter, account_row_item)
    }
}

impl BatchedScanStream {
    /// [`new_measured`](JoinedStream::new_measured) for the BATCHED surface, supplying
    /// its accounting (issue #1701) — the sibling of
    /// [`RowScanStream::new_measured_rows`], for the same reason.
    pub(in crate::storage::sstable) fn new_measured_batches(
        rx: mpsc::Receiver<Result<Vec<(RowKey, ScanRow)>>>,
        task: JoinHandle<()>,
        meter: ReadOpMeter,
    ) -> Self {
        Self::new_measured(rx, task, meter, account_batch_item)
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

/// Re-report for a consumer that keeps polling after the dead-task error, preserving
/// the CAUSE the first report gave (a cancellation stays `Error::Cancelled`).
fn sticky_dead_task_error<T: ScanStreamItem>(cancelled: bool) -> Error {
    if cancelled {
        return Error::Cancelled;
    }
    dead_scan_task_error_after_report::<T>()
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

/// The batched streaming scan's consumer handle: the batch channel PLUS the driver
/// task that feeds it, so end-of-stream is an observed fact (issue #3106). Behaviour
/// and API are unchanged from when this was its own `struct` (issue #3106); only the
/// item type is fixed here.
pub type BatchedScanStream = JoinedStream<Vec<(RowKey, ScanRow)>>;

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

// Behavioural unit tests for the producer-join protocol. Split into a sibling
// `*_tests.rs` file per the campsite rule (#1116/#1135): this source file is at the
// ~800-line target and the assertions are self-contained.
#[cfg(test)]
#[path = "joined_scan_stream_tests.rs"]
mod tests;

// Read-metric accounting at this boundary (issue #1701, roborev F2). Split into a
// sibling `*_tests.rs` file per the campsite rule (#1116/#1135): this source file is
// near the ~800-line target and the assertions are self-contained.
#[cfg(all(test, feature = "observability-testing"))]
#[path = "joined_scan_stream_read_metric_tests.rs"]
mod read_metric_tests;

// Scan-error counting at this boundary (issue #1704): the exactly-once latch and the
// nested-stream delegation, neither of which an integration test can reach.
#[cfg(all(test, feature = "observability-testing"))]
#[path = "joined_scan_stream_error_metric_tests.rs"]
mod error_metric_tests;
