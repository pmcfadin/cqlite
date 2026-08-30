//! Coverage for the WAIT CENSUS CHECK in `budgets.rs`
//! (`assert_census_matches_run`), issue #3515.
//!
//! Split out of that file under the campsite rule (#1135) when round 16 added the
//! unfinished-stage assert to it. The division is by SUBJECT: this file is about
//! the check that verifies a test's declared census against the stages the run
//! actually opened and finished; `budgets.rs` keeps the ONE deadline's arithmetic
//! and the calibration invariants.
//!
//! A child module can see its parent's private items, so these tests reach
//! `panic_text` and the census constants directly.

use super::*;
use std::time::Duration;

/// The census's STAGE SET is verified against the run, so a stage cannot join the
/// deadline without joining the census (roborev job 253, finding 3).
#[test]
fn the_stage_census_check_accepts_a_run_that_matches_it() {
    let deadline = TestDeadline::start(Duration::from_secs(60), Duration::from_secs(60));
    for entry in T1_WAIT_CENSUS {
        deadline.stage(entry.stage).finish();
    }
    assert_census_matches_run("a synthetic T1 run", T1_WAIT_CENSUS, &deadline);
}

/// ...and it REJECTS a run whose stages differ from the census — the property that
/// makes the aggregate floor's base something other than a hand-label.
///
/// Both directions matter, so both are exercised: a stage the census does not
/// declare, and a declared stage the run never opened.
#[test]
fn the_stage_census_check_rejects_a_run_that_does_not_match_it() {
    let extra = std::panic::catch_unwind(|| {
        let deadline = TestDeadline::start(Duration::from_secs(60), Duration::from_secs(60));
        for entry in T1_WAIT_CENSUS {
            deadline.stage(entry.stage).finish();
        }
        deadline.stage("f.undeclared").finish();
        assert_census_matches_run("a synthetic T1 run", T1_WAIT_CENSUS, &deadline);
    })
    .expect_err("a stage the census does not declare must fail the check");
    let extra = panic_text(extra.as_ref());
    assert!(
        extra.contains("f.undeclared"),
        "the failure must NAME the stage the census does not declare: {extra}"
    );

    let missing = std::panic::catch_unwind(|| {
        let deadline = TestDeadline::start(Duration::from_secs(60), Duration::from_secs(60));
        for entry in T1_WAIT_CENSUS.iter().skip(1) {
            deadline.stage(entry.stage).finish();
        }
        assert_census_matches_run("a synthetic T1 run", T1_WAIT_CENSUS, &deadline);
    })
    .expect_err("a declared stage the run never opened must fail the check");
    let missing = panic_text(missing.as_ref());
    assert!(
        missing.contains(T1_WAIT_CENSUS[0].stage),
        "the failure must NAME the declared stage the run did not open: {missing}"
    );
}

/// **AN EXTRA STAGE THAT IS NEVER `finish`ED IS STILL CAUGHT** (roborev job 255,
/// finding 1) — the case the completion-keyed record could not see.
///
/// A stage opened without being finished has already been able to consume
/// deadline-backed waits, so it belongs in the census exactly as a finished one
/// does. Keyed on completion, this run read as a perfect match and the aggregate
/// floor stayed green on a base that did not account for the stage: the guard did
/// not cover the case it is named for. Keyed on OPENING, it fails, names the extra
/// stage, and says it never finished.
#[test]
fn the_stage_census_check_rejects_an_extra_stage_that_never_finished() {
    let deadline = TestDeadline::start(Duration::from_secs(60), Duration::from_secs(60));
    for entry in T1_WAIT_CENSUS {
        deadline.stage(entry.stage).finish();
    }
    // Opened, therefore able to draw on the one deadline; never `finish()`ed,
    // therefore absent from every completion record. `Stage` has no `Drop`, so it
    // is simply left to go out of scope — the record of its OPENING is already in
    // the deadline.
    let _never_finished = deadline.stage("f.opened-never-finished");

    // THE PROPERTY, asserted FIRST so that a regression reds on the guard hole
    // itself rather than on a precondition supporting it: the check REJECTS this
    // run. Keyed on completion, it accepted it, and the aggregate floor stayed
    // green on a base that did not account for the stage.
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_census_matches_run("a synthetic T1 run", T1_WAIT_CENSUS, &deadline);
    }))
    .expect_err(
        "the census check ACCEPTED a run carrying a stage the census does not declare, because \
         that stage was never `finish`ed — the guard not covering the case it is named for \
         (job 255, finding 1)",
    );
    let panicked = panic_text(panicked.as_ref());
    assert!(
        panicked.contains("f.opened-never-finished"),
        "the failure must NAME the extra stage: {panicked}"
    );
    assert!(
        panicked.contains("opened and never finished"),
        "the failure must say the extra stage never finished, so the reader is not left \
         looking for it in the timings: {panicked}"
    );

    // ...and the mechanism that makes it hold: the stage is recorded from its
    // OPENING, and is reported as unfinished rather than silently absent.
    assert!(
        deadline
            .opened_stages()
            .contains(&"f.opened-never-finished"),
        "a stage must be recorded from the point it is OPENED: {:?}",
        deadline.opened_stages()
    );
    assert!(
        deadline
            .unfinished_stages()
            .contains(&"f.opened-never-finished"),
        "an opened-and-dropped stage must be reported as unfinished: {:?}",
        deadline.unfinished_stages()
    );
}

/// **A DECLARED STAGE THAT IS OPENED AND NEVER `finish`ED IS CAUGHT** (roborev job
/// 259, finding 2) — the case the stage-SET check structurally cannot see.
///
/// The sibling test above covers an EXTRA stage that never finishes, which the set
/// check catches on the extra name alone. THIS run declares nothing extra: every
/// stage in the census is opened, in order, and one of them is simply never
/// finished. The opened list therefore EQUALS the declared list, the set assert
/// passes, and before round 16 the whole check passed — losing that stage's timing
/// from the attribution report the stages exist to produce, with
/// `unfinished_stages()` computed and rendered into a message that was never
/// printed.
#[test]
fn the_stage_census_check_rejects_a_declared_stage_that_never_finished() {
    let deadline = TestDeadline::start(Duration::from_secs(60), Duration::from_secs(60));
    let mut open_forever = None;
    for (i, entry) in T1_WAIT_CENSUS.iter().enumerate() {
        let stage = deadline.stage(entry.stage);
        // The LAST declared stage is opened and never finished. `Stage` has no
        // `Drop`, so holding it is all it takes.
        if i + 1 == T1_WAIT_CENSUS.len() {
            open_forever = Some(stage);
        } else {
            stage.finish();
        }
    }
    let never_finished = T1_WAIT_CENSUS
        .last()
        .expect("the census is not empty")
        .stage;

    // THE PRECONDITION THAT MAKES THIS THE UNCOVERED CASE: the stage SET matches,
    // so the assert this test is about is the only thing that can fail. Asserted
    // here rather than argued, because if a future edit made the sets differ this
    // test would pass on the WRONG assert and the hole would reopen unnoticed.
    assert_eq!(
        deadline.opened_stages(),
        T1_WAIT_CENSUS.iter().map(|e| e.stage).collect::<Vec<_>>(),
        "this test is about the case where the stage SET matches"
    );
    assert_eq!(
        deadline.unfinished_stages(),
        vec![never_finished],
        "exactly one declared stage was left unfinished"
    );

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_census_matches_run("a synthetic T1 run", T1_WAIT_CENSUS, &deadline);
    }))
    .expect_err(
        "the census check ACCEPTED a run that never finished a declared stage: `unfinished_stages` \
         only ever reached a failure message, so it guarded nothing (job 259, finding 2)",
    );
    let panicked = panic_text(panicked.as_ref());
    assert!(
        panicked.contains(never_finished),
        "the failure must NAME the stage that never finished: {panicked}"
    );
    assert!(
        panicked.contains("never `finish`ed"),
        "the failure must say what went wrong, so it is not read as a census mismatch: {panicked}"
    );

    // Held to the end deliberately: `Stage` has no `Drop`, so what makes it
    // unfinished is that `finish()` is never called on it — not that it is still
    // in scope. Naming it here keeps that from reading as an oversight.
    let _never_finished_stage = open_forever;
}

/// A caught panic's message, for the two directions above.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "(a panic payload that is not a string)".to_string())
}
