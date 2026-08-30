//! Why the streaming cross-generation merge could not start — and whether the
//! caller may answer that by falling back to the NON-reconciling concatenation
//! (issues #3124 and #3154, both roborev).
//!
//! # The defect this type exists to make unrepresentable
//!
//! `generation_merge::stream_generations_for_read` reports setup back over a
//! oneshot from its blocking task, and the caller
//! (`SSTableManager::scan_stream`) is allowed to substitute the lazy per-reader
//! token-order concat when the merge cannot be CONSTRUCTED *because the merger
//! cannot handle the input at all*. That substitution is safe there and only
//! there: nothing has been streamed yet, and the concat's documented Issue #883
//! limitation is accepted for a table the reconciling merge simply cannot read.
//! (Whether that condition is actually REACHABLE through today's constructor is a
//! separate question, answered — "no" — two sections down. Read it before
//! concluding anything about production behaviour from this type.)
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
//!
//! # What `KWayMerger::new` can ACTUALLY return — and what issue #3154 changed
//!
//! The classification above must be read with the real constructor chain in hand,
//! because it is NOT the chain the original wording of this module implied.
//! `KWayMerger::new` (`storage/write_engine/merge/mod.rs:1627`) delegates through
//! `new_with_gc` (`:1667`) / `new_with_gc_and_registry` (`:1683`) to
//! `new_with_gc_and_registry_cancellable` (`:1710`), which has exactly THREE
//! fallible steps:
//!
//! 1. `Error::InvalidInput` when `input_paths` is empty (`merge/mod.rs:1718-1721`) —
//!    unreachable from the `scan_stream` site, which is guarded by
//!    `readers.len() > 1`.
//! 2. `schema.validate_dropped_columns()?` (`merge/mod.rs:1728`) → `Error::Schema`
//!    (`schema/mod.rs:611`).
//! 3. `SSTableRowIteratorAdapter::open(...)?` (`merge/mod.rs:1746`), whose ONLY error
//!    is `Error::Storage("streaming producer: failed to spawn thread: …")`
//!    (`merge/producer_iter.rs:275`).
//!
//! Step 3 OPENS NOTHING. `SSTableRowIteratorAdapter::open`
//! (`merge/producer_iter.rs:201-294`) creates a channel and `Builder::spawn`s a
//! producer thread; `SSTableReader::open` — and therefore EVERY
//! `UnsupportedFormat` / `UnsupportedVersion` header- and version-gate check — runs
//! INSIDE that thread (`merge/producer_iter.rs:385-388`) and surfaces as a failed
//! producer message observed later at `merger.step()`, i.e. mid-stream on the output
//! channel, never as a construction error. From the `scan_stream` site it is doubly
//! impossible: `readers.len() > 1` means every generation ALREADY opened
//! successfully through `SSTableReader::open`.
//!
//! Two consequences, stated plainly so no reader draws the flattering conclusion:
//!
//! * [`MergerIneligible`](MergeStreamSetupError::MergerIneligible) is **DEAD IN
//!   PRODUCTION**. Only the test-only construction-error injection seam
//!   (`producer_fault::FaultScope::injected_construction_error`) ever constructs it.
//!   An unsupported-format / below-floor generation does NOT degrade to the concat at
//!   this site — it errors mid-stream through the channel. **That was already true
//!   BEFORE issue #3154's narrowing**, so the narrowing removed no real-world
//!   degradation: AC2's "an unsupported input keeps falling back" property is
//!   preserved only because the fallback was already unreachable. The AC2 test
//!   therefore proves the CLASSIFIER, not an end-to-end fallback, and must not be
//!   read as evidence that a BTI / unsupported multi-generation table currently
//!   degrades to the concat. It does not.
//! * The errors that WERE actually reaching the fallback are `Error::Schema`
//!   (dropped-column validation, step 2) and `Error::Storage` (producer thread-spawn
//!   failure, step 3). Both used to be answered with the non-reconciling concat under
//!   `Ok`; both now PROPAGATE as an error. **That is the real behaviour change of
//!   issue #3154** — not a change to unsupported-format handling.
//!
//! The `MergerIneligible` arm is nevertheless KEPT, as a DELIBERATELY DEFENSIVE arm:
//! should a future constructor validate format/version EAGERLY (opening readers on
//! the calling thread instead of inside the producer thread), an unsupported input
//! would start arriving here as a construction error, and the documented Issue #883
//! degradation must keep working the moment it does.

use crate::Error;

/// A streaming cross-generation merge that never produced a live stream.
pub(in crate::storage::sstable) enum MergeStreamSetupError {
    /// `KWayMerger::new` reported that it cannot handle this INPUT: an unsupported
    /// format, or a version outside the supported floor.
    ///
    /// **DEFENSIVE — dead in production.** The current `KWayMerger::new` chain cannot
    /// return either of those variants: format/version gating happens inside the
    /// producer thread's `SSTableReader::open`
    /// (`storage/write_engine/merge/producer_iter.rs:385-388`) and surfaces mid-stream
    /// at `step()`, not at construction (see this module's doc for the full
    /// enumeration). Today only the test-only construction-error injection seam
    /// constructs this variant, so an unsupported multi-generation table does NOT
    /// currently degrade to the concat here. The arm is retained so that a future
    /// eagerly-validating constructor keeps the documented Issue #883 degradation
    /// instead of silently turning a merger-unreadable table into a failed read.
    ///
    /// When it IS produced the fallback is sound: nothing was streamed, the producer
    /// is alive and well-behaved, and the reconciling merge is genuinely unavailable
    /// for this table — so the caller MAY answer with the non-reconciling concat,
    /// accepting its documented Issue #883 limitation rather than failing a read that
    /// CQLite could otherwise serve.
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
    ///
    /// This is the ONLY outcome reachable in production at the `scan_stream` site:
    /// `Error::Schema` (dropped-column validation, `merge/mod.rs:1728`) and
    /// `Error::Storage` (producer thread-spawn failure,
    /// `merge/producer_iter.rs:275`) are the two errors `KWayMerger::new` can actually
    /// report there, and BOTH were previously answered with the concat. Narrowing them
    /// to propagate is the real behaviour change of issue #3154.
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
    /// What the current constructor can actually hand this function is enumerated in
    /// the module doc, and it is worth being blunt about: `Error::Schema`
    /// (dropped-column validation, `merge/mod.rs:1728`) and `Error::Storage`
    /// (thread-spawn failure, `merge/producer_iter.rs:275`) are the reachable cases and
    /// both propagate, while `UnsupportedFormat` / `UnsupportedVersion` are detected in
    /// the producer thread (`merge/producer_iter.rs:385-388`) and surface at `step()`,
    /// never here. The eligible arm below is therefore DEFENSIVE, not live.
    ///
    /// The catch-all arm deliberately fails CLOSED. A future `Error` variant, or any
    /// error whose meaning is not "the merger cannot handle this input", propagates
    /// rather than earning the concat — the wrong direction there returns wrong data
    /// under `Ok`, while the wrong direction here returns an honest error.
    pub(in crate::storage::sstable) fn from_construction_failure(cause: Error) -> Self {
        match &cause {
            // The merger genuinely cannot read this input, which is the ONE condition
            // the documented concat fallback exists to serve. Defensive: unreachable
            // through today's constructor (see this function's doc).
            Error::UnsupportedFormat(_) | Error::UnsupportedVersion { .. } => {
                Self::MergerIneligible(cause)
            }
            // Fail CLOSED. Note the cost of that direction: a future `Error` variant
            // that SHOULD earn the concat degrades SILENTLY into a propagated error
            // here, with nothing to notice it. Whoever adds such a variant must also
            // extend the enumerated classification table in this module's tests —
            // `a_construction_failure_earns_the_concat_only_when_the_input_is_merger_ineligible`,
            // whose `eligible` / `propagates` vectors ARE the contract.
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

    /// [`into_error`](Self::into_error) for the caller that is PROPAGATING, counting
    /// the failure once into `cqlite.errors.total{category, subsystem="reader"}`
    /// (issue #1704).
    ///
    /// A propagating setup failure returns from `SSTableManager::scan_stream` BEFORE
    /// any `JoinedStream` exists, so the streaming error-counting seam
    /// (`JoinedStream::recv`) can never see it: without this the scan failed, the
    /// caller got the error, and the error metric stayed flat. Nothing can
    /// double-count it either — the caller RETURNS here, so no stream is constructed
    /// to count it a second time.
    ///
    /// Deliberately NOT folded into [`into_error`](Self::into_error): that one is also
    /// used to REPORT a `fallback_eligible` failure into a `tracing::warn!` on the arm
    /// that then serves the concat successfully. A degraded read is not a failed scan,
    /// and counting it would inflate the error rate on a query that returned rows. The
    /// split keeps "which outcome is a failure" a decision of the CALL SITE that knows
    /// it, rather than a property of the conversion.
    ///
    /// The category comes from the classifier (`Error::obs_category`) and the error is
    /// returned unchanged; counting is a pure side effect.
    pub(in crate::storage::sstable) fn into_counted_error(self) -> Error {
        let e = self.into_error();
        crate::observability::record_error(&e, "reader");
        e
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
