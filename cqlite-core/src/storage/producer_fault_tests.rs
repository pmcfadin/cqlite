//! Unit tests for the producer-fault injection seams (issues #3106 / #3120 /
//! #3124).
//!
//! Lifted VERBATIM out of `producer_fault.rs`: gating the MERGE seam on
//! `write-support` (so the gate's `minimal-build` feature set stops seeing it as
//! dead code) pushed that file over the ~800-line source campsite target, and a
//! `*_tests.rs` sibling is the sanctioned way to make room (epic #1116 / #1135).
//! Bodies are unchanged, and the module is still a CHILD of `producer_fault`
//! (included via `#[path]`), so `super::*` resolves exactly as before.

use super::*;

fn path_of(s: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(s)
}

/// A default-constructed fault (the production shape) NEVER panics, however
/// many batches are handed over.
#[test]
fn an_unarmed_producer_fault_is_inert() {
    let mut fault = ProducerFault::default();
    for _ in 0..1000 {
        fault.before_batch_handoff();
    }
}

/// The OUTER budget semantics, asserted WITHOUT touching the arm registry
/// (`for_test`): the fault survives exactly its budget of handoffs, then
/// panics.
#[test]
fn an_armed_producer_fault_panics_after_exactly_its_budget() {
    let mut fault = ProducerFault::for_test(2);
    fault.before_batch_handoff();
    fault.before_batch_handoff();
    // The panic is EXPECTED, so only THIS message is silenced (a blanket hook
    // would hide a real failure from a parallel test) and the previous hook is
    // restored before the assertion runs.
    let died = {
        let _silence = silence_injected_panics();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fault.before_batch_handoff();
        }))
    };
    assert!(
        died.is_err(),
        "the armed fault must panic on the handoff after its budget"
    );
}

/// SCOPING is what makes the registry safe in a shared test binary (roborev,
/// issue #3106): a reader whose path does not match must NOT take the arm, and
/// must leave it registered for the reader it was meant for.
#[test]
fn an_arm_is_taken_only_by_a_matching_reader_path() {
    let scope = "issue-3106-scoping-unit-test/only-me";
    let guard = arm_query_row_producer_panic(scope, 5);

    // A foreign reader (what a concurrently-running sibling test looks like)
    // captures NOTHING and leaves the arm in place.
    for foreign in [
        "/tmp/other-test/data/ks/tbl/nb-1-big-Data.db",
        "/tmp/issue-3106-scoping-unit-test/someone-else/nb-1-big-Data.db",
    ] {
        let stolen = ProducerFault::capture_for(|| path_of(foreign));
        assert_eq!(
            stolen.panic_after_batches, None,
            "a non-matching reader path must not consume the arm ({foreign})"
        );
    }

    // The intended reader takes it, exactly once.
    let matching = format!("/tmp/{scope}/nb-1-big-Data.db");
    assert_eq!(
        ProducerFault::capture_for(|| path_of(&matching)).panic_after_batches,
        Some(5),
        "the reader the arm was scoped to takes it"
    );
    assert_eq!(
        ProducerFault::capture_for(|| path_of(&matching)).panic_after_batches,
        None,
        "and it is consumed — a second stream over the same reader is clean"
    );
    drop(guard);
}

/// Two arms coexist: arming second does not clobber the first, and each guard
/// removes only its OWN registration. This is the property a single global
/// slot could not provide, and it is why parallel arming tests are safe.
#[test]
fn concurrent_arms_coexist_and_each_guard_removes_only_its_own() {
    let first = arm_query_row_producer_panic("issue-3106-coexist/alpha", 1);
    let second = arm_query_row_producer_panic("issue-3106-coexist/beta", 2);

    drop(first);
    assert_eq!(
        ProducerFault::capture_for(|| path_of("/x/issue-3106-coexist/alpha/d.db"))
            .panic_after_batches,
        None,
        "dropping the first guard removes ONLY its arm"
    );
    assert_eq!(
        ProducerFault::capture_for(|| path_of("/x/issue-3106-coexist/beta/d.db"))
            .panic_after_batches,
        Some(2),
        "the second arm survives its sibling's disarm, un-clobbered"
    );
    drop(second);
}

/// A default-constructed MERGE fault (the production shape) NEVER panics,
/// however many rows are forwarded.
#[cfg(feature = "write-support")]
#[test]
fn an_unarmed_merge_producer_fault_is_inert() {
    let mut fault = MergeProducerFault::default();
    for _ in 0..1000 {
        fault.before_row_forward();
    }
}

/// The MERGE budget semantics, asserted WITHOUT touching the arm registry
/// (`for_test`): the fault survives exactly its budget of row forwards, then
/// panics.
#[cfg(feature = "write-support")]
#[test]
fn an_armed_merge_producer_fault_panics_after_exactly_its_budget() {
    let mut fault = MergeProducerFault::for_test(2);
    fault.before_row_forward();
    fault.before_row_forward();
    let died = {
        let _silence = silence_injected_panics();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fault.before_row_forward();
        }))
    };
    assert!(
        died.is_err(),
        "the armed merge fault must panic on the row forward after its budget"
    );
}

/// The MERGE registry is SEPARATE from the query-row one (issue #3120): a
/// merge run must not consume a query-row arm, and a query row stream must not
/// consume a merge arm — cross-consumption would let either test pass for the
/// wrong reason.
#[cfg(feature = "write-support")]
#[test]
fn merge_and_query_row_registries_never_consume_each_others_arms() {
    let scope = "issue-3120-registry-separation/only-me";
    let path = format!("/tmp/{scope}/nb-1-big-Data.db");

    // A MERGE arm is invisible to the query-row capture...
    let merge_guard = arm_merge_producer_panic(scope, 3);
    assert_eq!(
        ProducerFault::capture_for(|| path_of(&path)).panic_after_batches,
        None,
        "a query row stream must not consume a MERGE arm"
    );
    assert_eq!(
        MergeProducerFault::capture_for(|| path_of(&path)).panic_after_rows,
        Some(3),
        "the merge run the arm was scoped to takes it"
    );
    drop(merge_guard);

    // ...and symmetrically, a query-row arm is invisible to the merge capture.
    let query_guard = arm_query_row_producer_panic(scope, 4);
    assert_eq!(
        MergeProducerFault::capture_for(|| path_of(&path)).panic_after_rows,
        None,
        "a merge run must not consume a QUERY-ROW arm"
    );
    assert_eq!(
        ProducerFault::capture_for(|| path_of(&path)).panic_after_batches,
        Some(4),
        "the query row stream the arm was scoped to takes it"
    );
    drop(query_guard);
}

/// A MERGE arm is taken ONLY by a matching input path — the property that
/// makes a K-input merge's victim deterministic (a `TempDir`-wide scope would
/// kill whichever producer reached its first row first).
#[cfg(feature = "write-support")]
#[test]
fn a_merge_arm_is_taken_only_by_a_matching_input_path() {
    let scope = "issue-3120-merge-scope/nb-2-big-Data.db";
    let guard = arm_merge_producer_panic(scope, 1);

    let sibling =
        MergeProducerFault::capture_for(|| path_of("/tmp/issue-3120-merge-scope/nb-1-big-Data.db"));
    assert_eq!(
        sibling.panic_after_rows, None,
        "the SIBLING input of the same merge must not consume the arm"
    );
    assert_eq!(
        MergeProducerFault::capture_for(|| path_of(&format!("/tmp/{scope}"))).panic_after_rows,
        Some(1),
        "the input the arm was scoped to takes it"
    );
    assert_eq!(
        MergeProducerFault::capture_for(|| path_of(&format!("/tmp/{scope}"))).panic_after_rows,
        None,
        "and it is consumed — a retry over the same input is clean"
    );
    drop(guard);
}

/// The INNER checkpoint obeys the same scoping rule: a foreign scan passes
/// through untouched, the scoped one dies, and the arm is consumed.
#[test]
fn the_inner_checkpoint_fires_only_for_the_scoped_reader() {
    let scope = "issue-3106-inner-scope-unit-test/only-me";
    let guard = arm_inner_scan_task_panic(scope);
    let matching = format!("/tmp/{scope}/nb-1-big-Data.db");

    // A foreign scan must run right through the checkpoint.
    inner_scan_task_checkpoint(|| path_of("/tmp/someone-else/nb-1-big-Data.db"));

    let died = {
        let _silence = silence_injected_panics();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            inner_scan_task_checkpoint(|| path_of(&matching));
        }))
    };
    assert!(died.is_err(), "the scoped scan must hit the injected panic");

    // Consumed: a retry of the same scan is clean.
    inner_scan_task_checkpoint(|| path_of(&matching));
    drop(guard);
}

/// Issue #3124: an arm is keyed by `(site, scope)`. A checkpoint at a DIFFERENT
/// site over the very same reader must pass straight through and LEAVE the arm
/// registered — otherwise a test arming (say) the fan-out merge would have its
/// arm eaten by the per-row sub-scan checkpoint the same scan also runs, and
/// would pass while the boundary under test never fired.
#[test]
fn an_arm_is_taken_only_at_its_own_site() {
    let scope = "issue-3124-site-key-unit-test/only-me";
    let matching = format!("/tmp/{scope}/nb-1-big-Data.db");
    let guard = arm_scan_task_panic(scope, ScanTaskSite::FanoutMerge);

    // Every OTHER site over the same reader path runs right through.
    for other in [
        ScanTaskSite::InnerBatchedScan,
        ScanTaskSite::PerRowScan,
        ScanTaskSite::WindowedForwarder,
    ] {
        scan_task_checkpoint(other, || path_of(&matching));
    }

    // The armed site still fires — the arm was not consumed above.
    let died = {
        let _silence = silence_injected_panics();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            scan_task_checkpoint(ScanTaskSite::FanoutMerge, || path_of(&matching));
        }))
    };
    assert!(
        died.is_err(),
        "the arm must survive checkpoints at other sites and fire at its own"
    );

    // Consumed exactly once.
    scan_task_checkpoint(ScanTaskSite::FanoutMerge, || path_of(&matching));
    drop(guard);
}

/// The owned-scope form ([`FaultScope`], used by the two #3124 sites whose task
/// is spawned away from the reader) matches on the same `(site, scope)` key.
#[test]
fn a_captured_fault_scope_fires_for_its_own_site_and_scope() {
    let scope = "issue-3124-captured-scope-unit-test/only-me";
    let matching = format!("/tmp/{scope}/nb-1-big-Data.db");
    let guard = arm_scan_task_panic(scope, ScanTaskSite::WindowedForwarder);

    let foreign = FaultScope::capture(|| path_of("/tmp/someone-else/nb-1-big-Data.db"));
    foreign.checkpoint(ScanTaskSite::WindowedForwarder);

    let mine = FaultScope::capture(|| path_of(&matching));
    // Wrong site over the right scope: pass through, arm retained.
    mine.checkpoint(ScanTaskSite::FanoutMerge);

    let died = {
        let _silence = silence_injected_panics();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            mine.checkpoint(ScanTaskSite::WindowedForwarder);
        }))
    };
    assert!(
        died.is_err(),
        "a captured scope must fire at its armed site for its own reader"
    );
    drop(guard);
}
