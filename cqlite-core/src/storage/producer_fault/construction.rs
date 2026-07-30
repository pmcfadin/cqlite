//! TEST-ONLY injection of a `KWayMerger::new` CONSTRUCTION FAILURE on the
//! cross-generation reconciling merge path (issue #3154).
//!
//! # Why this is an ERROR seam, not a panic seam
//!
//! Every other arm in [`super`] injects a PANIC, because the defects those seams
//! prove are about a producer that DIES (issues #3106/#3120/#3124). This one injects
//! a REPORTED `Err`: the defect is that `generation_merge::stream_generations_for_read`
//! labelled EVERY error out of `KWayMerger::new` as fallback-eligible, so a transient
//! I/O hiccup or a corrupt input made `SSTableManager::scan_stream` silently
//! substitute the NON-reconciling token-order concat and return a FULL-LENGTH,
//! UNRECONCILED result set under `Ok` — duplicated overwritten rows and resurrected
//! deleted rows, behind a `tracing::warn!`.
//!
//! Proving the narrowed classification therefore needs `KWayMerger::new` to report a
//! CHOSEN error VARIANT, deterministically: an I/O error and a corruption error must
//! propagate, while the genuinely merger-ineligible unsupported-format error must
//! still degrade to the concat exactly as before (over-restricting is its own
//! regression). No on-disk fixture can produce all three at that one call site on
//! demand, and none of them can be produced at all without racing a real filesystem.
//!
//! # Same structural safety properties as its panic siblings
//!
//! * Its own registry, separate from the outer/task/merge ones, so a construction arm
//!   can neither consume nor be consumed by another seam's arm.
//! * A `Vec` of arms, each SCOPED to a `Data.db` path substring and TAKEN by the first
//!   matching merge, so concurrently armed tests in the shared lib test binary cannot
//!   steal one another's fault (see [`super`]'s module doc).
//! * Nothing here exists in a production build: the module is `cfg`'d out entirely,
//!   and the one production-side consumer
//!   ([`FaultScope::injected_construction_error`](super::FaultScope::injected_construction_error))
//!   compiles to a function that returns `None` without touching any registry. No
//!   environment variable, config field or on-disk byte pattern can arm it, so it
//!   cannot influence a decoding decision (issue #28).

use parking_lot::Mutex;

use crate::Error;

/// Which typed `KWayMerger::new` failure to make the cross-generation merge report.
///
/// The three variants are exactly the three classes issue #3154's acceptance criteria
/// distinguish: two that MUST propagate and one that MUST still fall back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConstructionFault {
    /// A transient I/O failure ([`Error::Io`]) — a runtime failure that says nothing
    /// about whether the reconciling merger supports this input, so it must NOT be
    /// answered with the concat.
    Io,
    /// A corrupt / unparseable input ([`Error::Corruption`]) — likewise a runtime
    /// failure, and answering it with the concat would serve wrong data from a file
    /// that is already known to be bad.
    Corruption,
    /// The genuinely merger-INELIGIBLE condition ([`Error::UnsupportedFormat`]): an
    /// input the reconciling merger cannot handle at all, which is the one case the
    /// documented Issue #883 concat fallback exists to serve.
    UnsupportedFormat,
}

/// The message every injected construction error carries, so a test can prove THIS
/// fault (rather than some unrelated failure) is what the read reported.
pub const INJECTED_CONSTRUCTION_MESSAGE: &str =
    "cqlite test fault injection (issue #3154): merge construction failure";

impl MergeConstructionFault {
    /// The error `KWayMerger::new` is made to return for this fault.
    fn into_error(self) -> Error {
        match self {
            Self::Io => Error::Io(std::io::Error::other(format!(
                "{INJECTED_CONSTRUCTION_MESSAGE} (io)"
            ))),
            Self::Corruption => {
                Error::corruption(format!("{INJECTED_CONSTRUCTION_MESSAGE} (corrupt input)"))
            }
            Self::UnsupportedFormat => Error::unsupported_format(format!(
                "{INJECTED_CONSTRUCTION_MESSAGE} (merger-ineligible input)"
            )),
        }
    }
}

/// One registered CONSTRUCTION arm.
struct ConstructionArm {
    id: u64,
    scope: String,
    fault: MergeConstructionFault,
}

/// Registered arms — a `Vec`, not a slot, for the same reason as every sibling
/// registry: two concurrently armed tests must coexist without clobbering each other.
/// The critical sections below are a few comparisons long and never span an `.await`.
static CONSTRUCTION_ARMS: Mutex<Vec<ConstructionArm>> = Mutex::new(Vec::new());

/// Arm the next cross-generation reconciling merge over an input whose `Data.db` path
/// contains `scope` to have its `KWayMerger::new` report `fault` instead of building.
///
/// Disarmed when the returned guard drops, and TAKEN by the first MATCHING merge — a
/// merge over any other table leaves it registered, so a concurrently-running test can
/// neither consume nor clobber it. Scope to something unique: the test's own `TempDir`
/// path, or `keyspace/table`.
///
/// TEST-ONLY: this symbol does not exist unless (`cfg(test)` or the
/// `producer-fault-injection` feature) AND `write-support` AND not `tombstones` — the
/// seam it injures exists in exactly that configuration (see the `mod construction`
/// declaration in [`super`]).
#[must_use = "the fault stays armed only while the guard is alive"]
pub fn arm_merge_construction_error(
    scope: &str,
    fault: MergeConstructionFault,
) -> ArmedMergeConstructionError {
    super::armed::check_scope(scope);
    let id = super::armed::next_id();
    CONSTRUCTION_ARMS.lock().push(ConstructionArm {
        id,
        scope: scope.to_string(),
        fault,
    });
    ArmedMergeConstructionError { id }
}

/// Guard returned by [`arm_merge_construction_error`]: removes its OWN arm (by id) on
/// drop. Holds no lock, so it is safe to hold across an `.await`.
#[derive(Debug)]
pub struct ArmedMergeConstructionError {
    id: u64,
}

impl Drop for ArmedMergeConstructionError {
    fn drop(&mut self) {
        CONSTRUCTION_ARMS.lock().retain(|arm| arm.id != self.id);
    }
}

/// TAKE the arm scoped to this merge's input path, if any, and build its error. A
/// non-matching path leaves every arm registered, which is what makes a foreign merge
/// unable to consume someone else's arm.
pub(super) fn take(path: &str) -> Option<Error> {
    let fault = {
        let mut arms = CONSTRUCTION_ARMS.lock();
        let index = arms
            .iter()
            .position(|arm| path.contains(arm.scope.as_str()))?;
        arms.remove(index).fault
    };
    Some(fault.into_error())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The injected errors must carry the VARIANTS the classification under test keys
    /// on — if this seam handed back, say, `Error::Storage` for `Io`, every end-to-end
    /// test built on it would be proving the wrong classification.
    #[test]
    fn each_fault_injects_its_own_error_variant_and_names_itself() {
        for (fault, matched) in [
            (
                MergeConstructionFault::Io,
                matches!(MergeConstructionFault::Io.into_error(), Error::Io(_)),
            ),
            (
                MergeConstructionFault::Corruption,
                matches!(
                    MergeConstructionFault::Corruption.into_error(),
                    Error::Corruption(_)
                ),
            ),
            (
                MergeConstructionFault::UnsupportedFormat,
                matches!(
                    MergeConstructionFault::UnsupportedFormat.into_error(),
                    Error::UnsupportedFormat(_)
                ),
            ),
        ] {
            assert!(matched, "{fault:?} injected the wrong Error variant");
            assert!(
                fault
                    .into_error()
                    .to_string()
                    .contains(INJECTED_CONSTRUCTION_MESSAGE),
                "{fault:?}: the injected error must name itself so a test can prove \
                 THIS fault ended the read"
            );
        }
    }

    /// A scan whose path does not match leaves the arm registered (no cross-test
    /// consumption), and a matching one takes it exactly once.
    #[test]
    fn an_arm_is_scoped_and_taken_exactly_once() {
        let scope = "issue-3154-scoped-take-probe";
        let _armed = arm_merge_construction_error(scope, MergeConstructionFault::Io);

        assert!(
            take("/some/other/table/nb-1-big-Data.db").is_none(),
            "a merge over an unrelated input must not consume this arm"
        );
        assert!(
            take(&format!("/tmp/{scope}/data/nb-1-big-Data.db")).is_some(),
            "the matching merge must take the arm"
        );
        assert!(
            take(&format!("/tmp/{scope}/data/nb-2-big-Data.db")).is_none(),
            "the arm must be taken exactly once, so a retry is not hit by it again"
        );
    }

    /// The guard removes its own registration, so an armed-but-never-matched fault
    /// cannot leak into a later test in the shared lib test binary.
    #[test]
    fn dropping_the_guard_disarms() {
        let scope = "issue-3154-disarm-probe";
        let path = format!("/tmp/{scope}/data/nb-1-big-Data.db");
        drop(arm_merge_construction_error(
            scope,
            MergeConstructionFault::Corruption,
        ));
        assert!(
            take(&path).is_none(),
            "a dropped guard must leave nothing armed"
        );
    }
}
