//! Why the streaming cross-generation merge could not start — and whether the
//! caller may answer that by falling back to the NON-reconciling concatenation
//! (issue #3124, roborev).
//!
//! # The defect this type exists to make unrepresentable
//!
//! `generation_merge::stream_generations_for_read` reports setup back over a
//! oneshot from its blocking task, and the caller
//! (`SSTableManager::scan_stream`) is allowed to substitute the lazy per-reader
//! token-order concat when the merge cannot be CONSTRUCTED — e.g. an input
//! format the `KWayMerger` cannot open. That substitution is safe there and only
//! there: nothing has been streamed yet, and the concat's documented Issue #883
//! limitation is accepted for a table the reconciling merge simply cannot read.
//!
//! But the oneshot has a SECOND failure mode: the blocking task can DIE (panic)
//! before signalling anything, dropping its sender. Flattened into one `Error`,
//! that death was indistinguishable from a construction failure, so the caller
//! fell back — and returned a FULL-LENGTH, UNRECONCILED result set (duplicated
//! overwritten rows, resurrected deleted rows) with `Ok` and a `tracing::warn!`.
//! That is strictly worse than the silent truncation issue #3124 is about:
//! silently WRONG data rather than silently SHORT data.
//!
//! Distinguishing the two by inspecting the error's message string would be a
//! guess. It is a TYPE here instead: only [`MergeStreamSetupError::Construction`]
//! is [`fallback_eligible`](MergeStreamSetupError::fallback_eligible), and a
//! caller that ignores the distinction cannot compile past the `match`.

use crate::Error;

/// A streaming cross-generation merge that never produced a live stream.
pub(in crate::storage::sstable) enum MergeStreamSetupError {
    /// `KWayMerger::new` returned `Err` and the task REPORTED it: the merge could
    /// not be built, nothing was streamed, and the producer is alive and
    /// well-behaved. The caller MAY fall back to the non-reconciling concat.
    Construction(Error),
    /// The producer task ended WITHOUT signalling readiness — it panicked (or was
    /// otherwise lost). The reconciling merge never started for an INTERNAL
    /// reason, and no conclusion about the table's readability is available, so
    /// this is NOT eligible for the concat fallback: the read fails closed.
    ProducerDied(Error),
}

impl MergeStreamSetupError {
    /// Whether the caller may substitute the non-reconciling concatenation.
    ///
    /// `true` for exactly one variant, on purpose (see the module doc).
    pub(in crate::storage::sstable) fn fallback_eligible(&self) -> bool {
        matches!(self, Self::Construction(_))
    }

    /// The underlying error, for a caller that is reporting rather than falling back.
    pub(in crate::storage::sstable) fn into_error(self) -> Error {
        match self {
            Self::Construction(e) | Self::ProducerDied(e) => e,
        }
    }
}

/// The fail-closed error for a merge producer that died before signalling readiness.
///
/// Worded like the five sibling dead-producer sites (issues #3106/#3124) — "DIED
/// without reporting", plus the `JoinError`, whose `Display` carries the panic
/// message — so one grep finds every one of them, and so the operator sees WHICH
/// fault ended the read rather than an anonymous internal error.
///
/// [`Error::internal`] for the same reason as those sites: nothing suggests the
/// `Data.db` is bad, and `Internal` is not `is_recoverable()`, which is honest for
/// a deterministic failure a retry reproduces.
pub(in crate::storage::sstable) fn dead_merge_producer_error(
    outcome: std::result::Result<(), tokio::task::JoinError>,
) -> Error {
    // `Ok(())` is unreachable through the current producer (both of its readiness
    // arms send before returning), but it is still a producer that reported
    // NOTHING, so it fails closed identically rather than being unwrapped.
    let cause = match outcome {
        Ok(()) => "the task returned without signalling readiness".to_string(),
        Err(join_err) => join_err.to_string(),
    };
    Error::internal(format!(
        "cross-generation reconciling merge: the scan task DIED without reporting \
         ({cause}) — the merge never started, so this read CANNOT fall back to the \
         non-reconciling concatenation (which would duplicate overwritten rows and \
         resurrect deleted ones) and is failed closed (issues #3106/#3124)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_reported_construction_failure_is_fallback_eligible() {
        assert!(
            MergeStreamSetupError::Construction(Error::unsupported_format("nope"))
                .fallback_eligible(),
            "a REPORTED construction failure is the one case the caller may answer \
             with the non-reconciling concat"
        );
        assert!(
            !MergeStreamSetupError::ProducerDied(Error::internal("died")).fallback_eligible(),
            "a dead producer must NEVER be answered with the concat: that returns a \
             full-length UNRECONCILED result set as a success (issue #3124, roborev)"
        );
    }

    #[tokio::test]
    async fn a_dead_producer_error_names_the_death_and_carries_the_panic_message() {
        let _silence = crate::storage::producer_fault::silence_injected_panics();
        let outcome = tokio::spawn(async {
            panic!("{}", crate::storage::producer_fault::INJECTED_PANIC_MESSAGE);
        })
        .await;
        drop(_silence);

        let message = dead_merge_producer_error(outcome).to_string();
        assert!(
            message.contains("DIED without reporting")
                && message.contains("CANNOT fall back")
                && message.contains(crate::storage::producer_fault::INJECTED_PANIC_MESSAGE),
            "the error must name the dead task, say the fallback is refused, and \
             carry the panic message, got: {message}"
        );
    }

    #[test]
    fn a_producer_that_returned_without_reporting_also_fails_closed() {
        let message = dead_merge_producer_error(Ok(())).to_string();
        assert!(
            message.contains("without signalling readiness")
                && message.contains("CANNOT fall back"),
            "a producer that reported nothing fails closed even without a JoinError, \
             got: {message}"
        );
    }
}
