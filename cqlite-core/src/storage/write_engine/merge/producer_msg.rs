//! The producer→consumer CHANNEL PROTOCOL for the k-way merge (issue #3120).
//!
//! # Why a protocol, and not just `Result<MergeEntry, _>`
//!
//! Every merge run is fed by a detached producer THREAD over a bounded
//! `sync_channel` ([`producer_iter`](super::producer_iter) for the path-based
//! shape, [`from_readers`](super::from_readers) for the shared-reader shape).
//! Before this issue the channel item was `Result<MergeEntry,
//! MergeProducerError>` and there was NO terminator: a producer that finished
//! and a producer that UNWOUND both simply dropped their `SyncSender`, and the
//! consumer mapped that channel DISCONNECT onto `None` = "this run is
//! exhausted". So a panicking producer made its run look EXHAUSTED:
//!
//! * on the READ path, a silently short result set served as a successful scan;
//! * on the WRITE path, `compact_sstables` rewrote an SSTable **missing rows**
//!   and then deleted (or superseded) its inputs — silent data loss at rest.
//!
//! [`MergeMsg`] makes completion an OBSERVED FACT instead of an inference. The
//! producer's last act on EVERY exit path is exactly one terminator
//! ([`MergeMsg::Done`] or [`MergeMsg::Failed`]), sent with the BLOCKING
//! `SyncSender::send` (a `try_send` that dropped the terminator would recreate
//! the very ambiguity this closes), and the consumer reports end-of-input ONLY
//! on `Done`. This is the same treatment issue #3106 gave the query row stream's
//! two producer boundaries.
//!
//! Deliberately NOT `Err(MergeProducerError::Done)` (a sentinel inside the
//! `Result`): that makes normal completion an *error* every match site has to
//! special-case, still permits a non-terminal `Err` to ride in the DATA slot,
//! and would make the terminator untracked on the egress-depth gauge by
//! ACCIDENT (right answer, wrong reason — see [`MergeMsg::tracked_entries`]).
//!
//! # Both halves are needed, and they cover different builds
//!
//! `catch_unwind` in the producer thread body turns a panic into an INFORMATIVE
//! terminal [`MergeProducerError::Panicked`], but it only fires under `panic =
//! "unwind"`. The `Done` terminator needs no unwinding at all: it makes any
//! *other* way a producer thread can stop without reporting — a future exit path
//! that forgets its terminator included — fail CLOSED in every profile.

use crate::error::Error;

use super::MergeEntry;

/// Channel-safe error payload sent from a producer thread to the merge (issue
/// #2264).
///
/// The producer thread runs the reader's compaction scan under `Result<_,
/// crate::Error>`, but `crate::Error` is not `Clone` and the channel item is
/// consumed once anyway, so this small enum is the minimal payload that
/// SURVIVES the thread boundary while still distinguishing a cooperative
/// cancellation (`Error::Cancelled`) from every other failure. Without this,
/// stringifying every error the same way would make a genuine I/O/corruption
/// error indistinguishable from a cancelled scan at the receiving end — exactly
/// the ambiguity `SSTableRowIterator::next` and `drive_merge` must NOT have.
///
/// Deliberately NOT `Clone` (rust-reviewer, issue #3120): the consumer stores the
/// terminal verdict on the run's state by MOVE, and re-reports it through
/// [`Self::to_error`], which borrows. Nothing ever clones one, so a `Clone` derive
/// would only invite a needless copy of the payload string.
#[derive(Debug)]
pub(super) enum MergeProducerError {
    /// The scan was cooperatively cancelled (`Error::Cancelled`).
    Cancelled,
    /// Any other failure, stringified (matches the pre-#2264 behaviour).
    Other(String),
    /// The producer thread UNWOUND — a `panic!`/assertion anywhere in its
    /// walk/decode — carrying the panic payload (issue #3120).
    ///
    /// A DISTINCT variant, not folded into [`Self::Other`], because it is the
    /// event this issue exists to make visible: the run is TRUNCATED at an
    /// arbitrary point, so it must never be reconciled or written as if it were
    /// complete.
    Panicked(String),
}

impl From<Error> for MergeProducerError {
    fn from(e: Error) -> Self {
        match e {
            Error::Cancelled => MergeProducerError::Cancelled,
            other => MergeProducerError::Other(other.to_string()),
        }
    }
}

impl MergeProducerError {
    /// Rebuild the consumer-side [`Error`] for this terminal verdict.
    ///
    /// Total and side-effect free, so a run whose verdict is STICKY can
    /// re-report the identical error on every subsequent poll rather than
    /// downgrading to `None` once the payload has been handed out (issue #3120).
    ///
    /// [`Self::Panicked`] maps to [`Error::internal`], NOT `Error::Storage`
    /// (roborev, issue #3106): `Storage.is_recoverable() == true`, which would
    /// advertise a deterministic dead-producer failure as RETRYABLE to the
    /// Node/Python error mapping. Nothing suggests the `Data.db` is bad either,
    /// so `Corruption` would send an operator hunting a nonexistent bad file.
    /// `Internal` is `is_recoverable() == false` and is the honest variant for a
    /// violated internal invariant.
    pub(super) fn to_error(&self) -> Error {
        match self {
            // Issue #2264: reconstructed DISTINCTLY so a cancelled scan is never
            // confused with a genuine I/O/corruption error at the
            // merge/producer boundary — `drive_merge` matches on the variant.
            MergeProducerError::Cancelled => Error::Cancelled,
            MergeProducerError::Other(msg) => {
                Error::Storage(format!("streaming merge producer error: {msg}"))
            }
            MergeProducerError::Panicked(message) => Error::internal(format!(
                "streaming merge producer: the producer thread PANICKED mid-walk \
                 ({message}) — this run is TRUNCATED and cannot be merged as if it \
                 were complete (issue #3120)"
            )),
        }
    }
}

/// One message on a merge producer→consumer channel (issue #3120).
///
/// INTERNAL to the merge: the terminator is deliberately not observable by any
/// merge CONSUMER (`SSTableRowIterator::next` keeps its
/// `Option<Result<MergeEntry>>` signature), so no `SSTableRowIterator`
/// implementation — `VecRun`, `SinglePartitionFilterRun`, the synthetic
/// streaming iterators — has to know this protocol exists. Only the two
/// producer-thread shapes and the one adapter that consumes their channels do.
// Issue #2820: the DATA variant carries a `Vec` BATCH of entries, not one entry
// (~288 bytes) — so every variant of this enum is now small and no
// `large_enum_variant` allowance is needed. The batch's ONE heap allocation is
// amortised over up to `BATCH_EMIT_ROWS_MERGE` rows, unlike the per-entry
// allocation issue #1664 removed; what it buys is one cross-thread park/wake per
// BATCH instead of per ROW (measured at 49.9% of single-stream CPU before this
// change). See `egress_batch` for the batching contract.
#[derive(Debug)]
pub(super) enum MergeMsg {
    /// One BATCH of decoded merge entries, in scan order (issue #2820). Carries
    /// DATA rows and NOTHING else BY CONSTRUCTION (issue #3120): a failure is
    /// [`MergeMsg::Failed`], so "this message ends the run" is a structural
    /// property of the variant rather than an unenforced invariant about where an
    /// `Err` was built.
    ///
    /// NON-EMPTY by construction (`EgressBatcher::flush` returns early on an
    /// empty accumulator); the consumer treats an empty batch as "nothing to hand
    /// out" and polls again rather than trusting that, so an empty one could never
    /// be mistaken for end-of-input.
    ///
    /// This is why `forward_row` no longer sends a conversion failure as a
    /// mid-walk channel message: a non-terminal error in the DATA slot, after
    /// which the walk KEEPS GOING, is exactly the shape that would let a later
    /// genuine dead-producer disconnect revert to a clean end-of-input.
    Batch(Vec<MergeEntry>),
    /// The run failed. TERMINAL: the producer sends exactly one of this or
    /// [`MergeMsg::Done`], as its last act, on every exit path.
    Failed(MergeProducerError),
    /// The producer finished its walk and is exiting normally. The ONLY thing
    /// that makes the consuming adapter report a clean end-of-input; a
    /// disconnect without it is a dead producer, i.e. a TRUNCATED run.
    Done,
}

impl MergeMsg {
    /// How many entries of this message occupy TRACKED data slots on the
    /// egress-depth gauge ([`channel_depth`](super::channel_depth)) — the gauge's
    /// unit is ENTRIES (rows), and issue #2820 did not change that: a batch of
    /// `n` rows counts `n`, exactly as `n` per-row messages used to.
    ///
    /// Called at the SEND site (`egress_batch::EgressBatcher::flush`). It is worth
    /// being precise about what this does and does not buy, because overclaiming
    /// here is how the next reader stops looking (rust-reviewer, issue #3120):
    ///
    /// * At that call site it is a **TAUTOLOGY** — the batcher builds
    ///   `MergeMsg::Batch(batch)` unconditionally, so the answer can only be the
    ///   batch length. It is not a runtime filter; it is a **compile-time
    ///   tripwire**. Because the body below is an EXHAUSTIVE match with no
    ///   wildcard, adding a 4th `MergeMsg` variant forces an explicit
    ///   tracked/untracked decision here instead of a silent default.
    /// * The RECEIVE site (`producer_iter`'s `next`) does NOT call this. It is a
    ///   hand-written `MergeMsg::Batch(entries)` match arm, held correct by its own
    ///   two properties: that `match` is exhaustive with no wildcard, and
    ///   `channel_depth::received_n` / `received_count` each appear at exactly ONE
    ///   site in the crate, both inside that arm (issue #2820 moved
    ///   `add_merge_run_entry_decoded` to the per-ENTRY hand-out below it, since
    ///   its unit is entries decoded, not messages received).
    ///
    /// Why it matters at all: before issue #3120 the two sides were two DIFFERENT
    /// expressions of one rule — `msg.is_ok()` on send versus an `Ok(Ok(_))`
    /// pattern on receive — and a message counted on exactly one side drives the
    /// reconcile residual NEGATIVE, which
    /// `channel_depth::reconcile_residual`'s `> 0` guard skips and `record`'s
    /// `max(0)` floor then hides from every observer, permanently.
    pub(super) fn tracked_entries(&self) -> usize {
        match self {
            MergeMsg::Batch(entries) => entries.len(),
            // TERMINATORS are untracked on BOTH sides, so they can never
            // unbalance the level.
            MergeMsg::Failed(_) | MergeMsg::Done => 0,
        }
    }
}

/// The fail-closed error for a producer that disconnected WITHOUT a terminator
/// (issue #3120).
///
/// [`Error::internal`] for the same reason [`MergeProducerError::Panicked`] is
/// (see [`MergeProducerError::to_error`]): a dead producer is a violated
/// internal invariant, not a retryable storage condition and not a bad file.
pub(super) fn dead_producer_error() -> Error {
    Error::internal(
        "streaming merge producer: the channel disconnected WITHOUT a terminal Done \
         message — the producer thread died mid-walk, so this run is TRUNCATED and \
         must not be merged (or written) as if it were exhausted (issue #3120)",
    )
}

/// Turn a caught producer-thread panic payload into a
/// [`MergeProducerError::Panicked`].
///
/// The payload is a `String`/`&str` for every `panic!`/assertion (the only other
/// shapes are hand-rolled `panic_any`), so an unrecognized payload degrades to a
/// named placeholder rather than being dropped — the terminator is still sent
/// either way, which is the property that matters.
pub(super) fn panicked_producer_error(payload: &(dyn std::any::Any + Send)) -> MergeProducerError {
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&'static str>().copied())
        .unwrap_or("<non-string panic payload>");
    MergeProducerError::Panicked(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::write_engine::merge::{CellData, RowData};
    use crate::storage::write_engine::mutation::DecoratedKey;
    use crate::types::Value;

    fn entry(n: i64) -> MergeEntry {
        MergeEntry::new(
            0,
            DecoratedKey::new(n, n.to_be_bytes().to_vec()),
            None,
            100,
            RowData::Live {
                cells: vec![CellData::new("name".to_string(), Value::text("v"), 100)],
            },
        )
    }

    fn batch(rows: usize) -> MergeMsg {
        MergeMsg::Batch((0..rows as i64).map(entry).collect())
    }

    /// The gauge unit is ENTRIES: a DATA batch of `n` rows counts `n` tracked
    /// slots (issue #2820 — batching changed the MESSAGE count, never the gauge
    /// unit), and EVERY terminator counts zero (issue #3120). Asserted on the real
    /// values both the send site and the receive site key on, so an asymmetry
    /// between them cannot hide here.
    #[test]
    fn a_data_batch_tracks_one_slot_per_entry_and_a_terminator_tracks_none() {
        for rows in [1usize, 2, 7, 256] {
            assert_eq!(
                batch(rows).tracked_entries(),
                rows,
                "a batch of {rows} DATA entries occupies {rows} tracked channel \
                 slots — counting it as ONE would make the #2419 gauge read \
                 messages while its unit says entries"
            );
        }
        for terminator in [
            MergeMsg::Done,
            MergeMsg::Failed(MergeProducerError::Cancelled),
            MergeMsg::Failed(MergeProducerError::Other("io".to_string())),
            MergeMsg::Failed(MergeProducerError::Panicked("boom".to_string())),
        ] {
            assert_eq!(
                terminator.tracked_entries(),
                0,
                "a terminator must be UNTRACKED on both send and receive, else the \
                 reconcile residual goes negative and is hidden by the `> 0` guard \
                 + `max(0)` floor forever (issue #3120): {terminator:?}"
            );
        }
    }

    /// A panicked producer must surface as a NON-recoverable `Internal` error
    /// naming the panic — never `Storage`, whose `is_recoverable() == true`
    /// would advertise a deterministic dead-producer failure as RETRYABLE to the
    /// bindings' error mapping (roborev, issue #3106).
    #[test]
    fn a_panicked_producer_maps_to_a_non_recoverable_internal_error() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("boom-from-the-walk".to_string());
        let error = panicked_producer_error(payload.as_ref()).to_error();
        assert!(
            !error.is_recoverable(),
            "a dead producer is deterministic, not retryable: {error}"
        );
        let text = error.to_string();
        assert!(
            text.contains("PANICKED") && text.contains("boom-from-the-walk"),
            "the error must name the panic and carry its payload so the failure is \
             debuggable rather than a generic 'the producer died', got: {text}"
        );
        assert!(
            text.contains("TRUNCATED"),
            "the error must state the run is incomplete, got: {text}"
        );
    }

    /// The bare-disconnect backstop is the same non-recoverable `Internal`
    /// class, and says so.
    #[test]
    fn a_bare_disconnect_maps_to_a_non_recoverable_internal_error() {
        let error = dead_producer_error();
        assert!(!error.is_recoverable(), "not retryable: {error}");
        assert!(error.to_string().contains("TRUNCATED"));
    }

    /// Cancellation stays a DISTINCT `Error::Cancelled` through the terminator
    /// protocol (issue #2264): a cancelled scan must never be reported as a dead
    /// producer, which is the likeliest false positive of this fix.
    #[test]
    fn cancellation_survives_the_terminator_protocol_as_cancelled() {
        assert!(matches!(
            MergeProducerError::from(Error::Cancelled).to_error(),
            Error::Cancelled
        ));
    }
}
