//! Why the streaming cross-generation merge could not start — and whether the
//! caller may answer that by falling back to the NON-reconciling concatenation
//! (issues #3124 and #3154, both roborev).
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
//! guess. It is a TYPE here instead: exactly one variant is
//! [`fallback_eligible`](MergeStreamSetupError::fallback_eligible), and a caller that
//! ignores the distinction cannot compile past the `match`.
//!
//! # The second, likelier route to the same wrong data (issue #3154)
//!
//! Labelling every REPORTED construction failure fallback-eligible left the wrong-data
//! route wide open, reached by a far more ordinary trigger than a panic: a transient
//! I/O error, or a corrupt input encountered while constructing the merger, made the
//! query return the full-length UNRECONCILED result set under `Ok`.
//!
//! The fallback's justification is narrow. It exists so an input the reconciling merger
//! CANNOT HANDLE still returns something — the documented Issue #883 concat limitation,
//! accepted for a table that would otherwise be unreadable. That justification covers
//! an unsupported-format / merger-ineligible condition and NOTHING else: a runtime
//! failure says nothing about whether the merger supports the input, so answering it
//! with the concat trades an honest error for silently wrong data.
//!
//! So a reported construction failure is CLASSIFIED, by
//! [`MergeStreamSetupError::from_construction_failure`], into
//! [`MergeStreamSetupError::MergerIneligible`] (fallback-eligible, unchanged
//! behaviour) or [`MergeStreamSetupError::ConstructionFailed`] (propagates). The
//! classification keys on the `Error` VARIANT — never on its message — and
//! [`fallback_eligible`](MergeStreamSetupError::fallback_eligible) remains the SINGLE
//! predicate the caller consults.

use crate::Error;

/// A streaming cross-generation merge that never produced a live stream.
pub(in crate::storage::sstable) enum MergeStreamSetupError {
    /// `KWayMerger::new` reported that it cannot handle this INPUT: an unsupported
    /// format, or a version outside the supported floor. Nothing was streamed, the
    /// producer is alive and well-behaved, and the reconciling merge is genuinely
    /// unavailable for this table — so the caller MAY answer with the non-reconciling
    /// concat, accepting its documented Issue #883 limitation rather than failing a
    /// read that CQLite could otherwise serve.
    MergerIneligible(Error),
    /// `KWayMerger::new` reported a RUNTIME failure — an I/O error, a corrupt or
    /// unparseable input, a resource failure, or anything else that is not evidence
    /// about the input's format (issue #3154).
    ///
    /// NOT eligible for the concat fallback. The merger is not known to be unable to
    /// read this table; something went wrong while it tried. Substituting the concat
    /// there returns a FULL-LENGTH, UNRECONCILED result set (duplicated overwritten
    /// rows, resurrected deleted rows) as a success, which is strictly worse than
    /// reporting the failure the caller can act on.
    ConstructionFailed(Error),
    /// The producer task ended WITHOUT signalling readiness — it panicked (or was
    /// otherwise lost). The reconciling merge never started for an INTERNAL
    /// reason, and no conclusion about the table's readability is available, so
    /// this is NOT eligible for the concat fallback: the read fails closed.
    ProducerDied(Error),
}

impl MergeStreamSetupError {
    /// Classify a REPORTED `KWayMerger::new` failure (issue #3154).
    ///
    /// Matched on the `Error` VARIANT, never on its message: a message match would be
    /// exactly the guess this module exists to eliminate, and it would silently
    /// re-classify itself whenever an error string is reworded.
    ///
    /// The catch-all arm deliberately fails CLOSED. A future `Error` variant, or any
    /// error whose meaning is not "the merger cannot handle this input", propagates
    /// rather than earning the concat — the wrong direction there returns wrong data
    /// under `Ok`, while the wrong direction here returns an honest error.
    pub(in crate::storage::sstable) fn from_construction_failure(cause: Error) -> Self {
        match &cause {
            // The merger genuinely cannot read this input, which is the ONE condition
            // the documented concat fallback exists to serve.
            Error::UnsupportedFormat(_) | Error::UnsupportedVersion { .. } => {
                Self::MergerIneligible(cause)
            }
            _ => Self::ConstructionFailed(cause),
        }
    }

    /// Whether the caller may substitute the non-reconciling concatenation.
    ///
    /// `true` for exactly one variant, on purpose (see the module doc). This stays the
    /// SINGLE predicate for that decision: a second mechanism could disagree with it.
    pub(in crate::storage::sstable) fn fallback_eligible(&self) -> bool {
        matches!(self, Self::MergerIneligible(_))
    }

    /// The underlying error, for a caller that is reporting rather than falling back.
    ///
    /// Returned VERBATIM, keeping the variant the merger reported: an I/O failure that
    /// a retry might clear must not reach a binding disguised as an internal error (and
    /// vice versa) — `Error::is_recoverable` is derived from that variant.
    pub(in crate::storage::sstable) fn into_error(self) -> Error {
        match self {
            Self::MergerIneligible(e) | Self::ConstructionFailed(e) | Self::ProducerDied(e) => e,
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
    fn only_a_merger_ineligible_input_is_fallback_eligible() {
        assert!(
            MergeStreamSetupError::MergerIneligible(Error::unsupported_format("nope"))
                .fallback_eligible(),
            "an input the merger cannot handle is the one case the caller may answer \
             with the non-reconciling concat"
        );
        assert!(
            !MergeStreamSetupError::ConstructionFailed(Error::corruption("bad"))
                .fallback_eligible(),
            "a RUNTIME construction failure must NEVER be answered with the concat: \
             that returns a full-length UNRECONCILED result set as a success (issue #3154)"
        );
        assert!(
            !MergeStreamSetupError::ProducerDied(Error::internal("died")).fallback_eligible(),
            "a dead producer must NEVER be answered with the concat: that returns a \
             full-length UNRECONCILED result set as a success (issue #3124, roborev)"
        );
    }

    /// The classification itself (issue #3154): which REPORTED construction errors earn
    /// the concat. Every case is decided from the `Error` VARIANT, so this table is the
    /// whole contract — including the fail-CLOSED default for anything unrecognised.
    #[test]
    fn a_construction_failure_earns_the_concat_only_when_the_input_is_merger_ineligible() {
        let eligible: Vec<Error> = vec![
            Error::unsupported_format("BTI input the k-way merger cannot open"),
            Error::UnsupportedVersion {
                version: "ma".to_string(),
                floor: "na".to_string(),
            },
        ];
        for cause in eligible {
            let label = cause.to_string();
            assert!(
                MergeStreamSetupError::from_construction_failure(cause).fallback_eligible(),
                "an unsupported input must keep degrading to the documented Issue #883 \
                 concat — narrowing that away turns a previously-working read into a \
                 failure: {label}"
            );
        }

        let propagates: Vec<Error> = vec![
            // The two triggers issue #3154 is about: far likelier than a panic, and
            // both used to return a full-length UNRECONCILED result set under `Ok`.
            Error::Io(std::io::Error::other("transient read failure")),
            Error::corruption("truncated partition header"),
            // Neither is evidence about the input FORMAT, so neither earns the concat.
            Error::Serialization {
                message: "cell parse".to_string(),
                source: None,
            },
            Error::invalid_format("unexpected flags byte"),
            Error::schema("dropped column not declared in columns"),
            Error::storage("failed to spawn producer thread"),
            Error::internal("unexpected state"),
        ];
        for cause in propagates {
            let label = cause.to_string();
            assert!(
                !MergeStreamSetupError::from_construction_failure(cause).fallback_eligible(),
                "a runtime construction failure must propagate, never be answered with \
                 the non-reconciling concat: {label}"
            );
        }
    }

    /// The propagated error keeps the VARIANT the merger reported — bindings derive
    /// retryability (`Error::is_recoverable`) from it, so re-labelling an I/O hiccup as
    /// an internal error (or the reverse) would misinform the caller.
    #[test]
    fn a_propagated_construction_failure_keeps_its_variant_and_message() {
        let classified = MergeStreamSetupError::from_construction_failure(Error::Io(
            std::io::Error::other("transient read failure"),
        ));
        assert!(!classified.fallback_eligible());
        let error = classified.into_error();
        assert!(
            matches!(error, Error::Io(_)) && error.to_string().contains("transient read failure"),
            "expected the reported I/O error verbatim, got: {error:?}"
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
