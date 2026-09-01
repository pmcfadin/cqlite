//! **HOW A MEASUREMENT BECOMES A DEADLINE** — coverage for
//! `TestDeadline::calibrate` and the derivation it reports (issues #1693, #3515,
//! #3652).
//!
//! Split out of `budgets.rs` under the campsite rule (#1135), by SUBJECT: the
//! calibration arithmetic and the point at which a measurement may be APPLIED are
//! here, the stage machinery and the wait-census floor stay in the parent. A child
//! module sees its parent's private items, so these tests reach the same internals
//! the parent's do.

use super::*;

/// Calibration takes the LARGEST scale, only ever LOOSENS, and never exceeds the
/// cap.
#[test]
fn calibration_takes_the_largest_scale_and_only_ever_loosens() {
    let base = Duration::from_secs(100);
    let cap = Duration::from_secs(300);

    // Below the baseline: the identity.
    let d = TestDeadline::start(base, cap);
    d.calibrate("t_boot", QUIET_OBSERVATION_BASELINE / 10);
    assert_eq!(d.span(), base, "a quiet observation must not scale");

    // 2x the baseline loosens proportionally...
    d.calibrate("t_ack", QUIET_OBSERVATION_BASELINE * 2);
    assert_eq!(d.span(), Duration::from_secs(200));

    // ...and a SMALLER later observation may not pull it back in: the deadline
    // takes the largest scale seen, so calibration is monotone.
    d.calibrate("t_ack(again)", QUIET_OBSERVATION_BASELINE / 2);
    assert_eq!(
        d.span(),
        Duration::from_secs(200),
        "a smaller later observation must not TIGHTEN the deadline: {}",
        d.describe()
    );

    // A pathological observation is clamped at the cap, never beyond it.
    d.calibrate("t_ack(pathological)", QUIET_OBSERVATION_BASELINE * 600);
    assert_eq!(d.span(), cap, "the cap is the maximum: {}", d.describe());
}

/// The one bound reports its own derivation, so any failure can be audited.
#[test]
fn the_deadline_describes_its_own_derivation() {
    let uncalibrated = TestDeadline::start(T1_DEADLINE_BASE, T1_DEADLINE_CAP).describe();
    assert!(
        uncalibrated.contains("UNCALIBRATED base"),
        "the irreducible base must say so: {uncalibrated}"
    );

    let d = TestDeadline::start(T1_DEADLINE_BASE, T1_DEADLINE_CAP);
    d.calibrate("t_ack", QUIET_OBSERVATION_BASELINE * 2);
    let described = d.describe();
    for needle in [
        "ONE per-test deadline",
        "base",
        "scale",
        "cap",
        "t_ack",
        "quiet baseline",
        "no per-stage budgets",
        "NEVER extends it",
    ] {
        assert!(
            described.contains(needle),
            "the deadline description must report {needle:?}: {described}"
        );
    }
}

/// **A COMPLETED MEASUREMENT IS APPLIED WHILE THE STAGE THAT PRODUCED IT IS STILL
/// OPEN** (#3652, roborev job 271 finding 5).
///
/// The sibling integration test acknowledges FIVE writes inside ONE stage. Every
/// one of those measurements used to reach `calibrate` only after the loop had
/// finished and the stage had been dropped — not by choice but because `calibrate`
/// took `&mut self` while a live [`Stage`] borrows the deadline immutably, so a
/// mid-loop call was not expressible at all. The consequence is the
/// calibration-inertness class this harness treats as its most serious: a slow
/// FIRST acknowledgement could not extend the deadline for the four writes that
/// FOLLOWED it, so the loop ran against a deadline calibrated from `t_boot` alone.
///
/// THERE IS NO WALL-CLOCK THRESHOLD HERE (#2642). Every assertion is on the
/// deadline's SPAN — pure arithmetic over the base and the largest scale — and on
/// the open stage's remaining budget against the BASE, which carries 100s of slack
/// against the microseconds of work between the two reads. Nothing asserts that
/// anything happened quickly.
#[test]
fn a_measurement_is_applied_while_its_stage_is_still_open() {
    let base = Duration::from_secs(100);
    let cap = Duration::from_secs(300);
    let deadline = TestDeadline::start(base, cap);

    // THE STAGE IS OPEN FOR THE REST OF THIS TEST: it borrows the deadline, which
    // is what made every call below unwritable through `&mut self`.
    let stage = deadline.stage("b.write-acks");
    assert_eq!(
        deadline.span(),
        base,
        "no measurement has been folded in yet, so the deadline is its uncalibrated base"
    );

    // Ack 1 completes and is folded in IMMEDIATELY.
    deadline.calibrate("t_ack(write)", QUIET_OBSERVATION_BASELINE * 2);
    assert_eq!(
        deadline.span(),
        Duration::from_secs(200),
        "a measurement completed inside an OPEN stage must move the one deadline as soon as it \
         lands — deferring it to the end of the loop is what left the later writes of the same \
         loop running against an uncalibrated deadline: {}",
        deadline.describe()
    );
    assert!(
        stage.remaining() > base,
        "and the OPEN stage must see the loosened deadline, since that is what the writes after \
         this one are bounded by: {}",
        stage.describe()
    );

    // Ack 2 is slower still, and loosens it again: this is the property that lets
    // acknowledgement N extend the deadline for acknowledgement N+1.
    deadline.calibrate("t_ack(write)", QUIET_OBSERVATION_BASELINE * 3);
    assert_eq!(
        deadline.span(),
        Duration::from_secs(300),
        "each later measurement is applied in turn: {}",
        deadline.describe()
    );

    // AND REPEATED APPLICATION DOES NOT COMPOUND, which is what makes applying
    // every measurement safe. The span is `clamp(base x LARGEST scale, base, cap)`,
    // derived from the BASE and never from the current span, so folding the same
    // value in again is the identity — five one-at-a-time calls and one call with
    // the slowest of the five give the identical deadline.
    deadline.calibrate("t_ack(write)", QUIET_OBSERVATION_BASELINE * 3);
    assert_eq!(
        deadline.span(),
        Duration::from_secs(300),
        "re-folding a measurement must not accumulate: {}",
        deadline.describe()
    );

    // What each call DOES add is one diagnostic observation, which is the honest
    // record: three acknowledgement measurements were folded in above, so three
    // are reported.
    let described = deadline.describe();
    assert_eq!(
        described.matches("t_ack(write)").count(),
        3,
        "every applied measurement must appear in the derivation the failures report: {described}"
    );
    stage.finish();
}
