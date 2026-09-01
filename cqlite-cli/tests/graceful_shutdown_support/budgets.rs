//! The DEADLINE LAYER of the graceful-shutdown oracle (issues #1693, #3515).
//!
//! ONE deadline per test, and nothing else. `mod.rs` keeps the *child harness*
//! (pipes, transcript, the progress-observing poll, the read-side SELECT); this
//! file owns the clock and the unit tests that pin its invariants.
//!
//! # Why there is only one deadline (round-8 DESCOPE, design.md D6a)
//!
//! Rounds 4-7 of this change carried a PER-STAGE calibrated budget layer: a
//! `StageSpec` base/cap pair per stage, a total-budget clock that clipped each
//! stage's deadline to the remaining total, a `starved` flag, a floor invariant
//! stated by composition, and asserts that summed the declared maxima. roborev
//! reviewed it four times and returned **12 findings, all 12 inside that layer**,
//! at a flat 3 per round — while the *oracle* it wrapped (the staged waits, the
//! stderr progress markers, the honest failure messages) produced ZERO findings
//! after round 3. This repository descopes a mechanism whose defect count does
//! not fall rather than patch it a fifth time.
//!
//! The load-bearing realisation is that the acceptance criteria never asked for
//! the calibration. AC1 asks for *liveness confirmation rather than a bare
//! deadline*; that is supplied by stage (c)'s handler-entry marker, which proves
//! the signal was delivered, the handler was entered and the child was scheduled.
//! Per-stage budget arithmetic supplied none of it, and its final finding was
//! that the composition rule itself was wrong: summing per-stage caps does not
//! preserve a SHARED old deadline, so a handler entering at 31s and exiting at
//! 32s — which the old flat 60s allowed — failed a 30s per-stage cap.
//!
//! What is here instead:
//!
//! * **ONE deadline per test**, calibrated ONCE from the LARGEST scale of the
//!   in-band measurements, with a generous base and a cap.
//! * **Stages remain, purely for ATTRIBUTION.** [`Stage`] carries a name and a
//!   start instant and NO BOUND OF ITS OWN; which stage was pending when the
//!   deadline passed is what names the failure.
//! * **Progress observation remains as EVIDENCE IN THE MESSAGE ONLY** (see
//!   `poll_with_progress` in `mod.rs`). It reports what it saw and extends
//!   nothing. That removes the "declared cap is not the actual maximum" family at
//!   the root: there is one bound, no wait is granted more time than it leaves,
//!   and none is started past it. Scoped precisely (roborev job 232 finding 1):
//!   the deadline bounds how long the test WAITS FOR EVIDENCE, not the acceptance
//!   of evidence already observed — `poll_with_progress` in `mod.rs` deliberately
//!   accepts a success it notices as the deadline lapses, and states the bound on
//!   how late that can be. Symmetrically (job 233 finding 1), every expiry site
//!   takes a FINAL NON-BLOCKING look at what already arrived before declaring a
//!   timeout, so an unconsumed-but-delivered signal is never reported as absent —
//!   and that look DECIDES FROM THE STORE THE FAILURE REPORTS FROM (job 236
//!   finding 1), because a reader records into the transcript before it publishes
//!   to the queue, so a queue-only check leaves the message able to print
//!   evidence the decision never saw. Refined again in round 12 (job 243
//!   finding 1): the same store is not the same SNAPSHOT — one read is taken at
//!   the decision and CARRIED into the failure value, and the window opens BEFORE
//!   the operation whose response is awaited, not when the wait starts.
//!
//! The accepted cost, stated plainly: a genuine defect now surfaces at the
//! deadline rather than at a tight per-stage cap. It is paid only on a real
//! failure.

use std::cell::RefCell;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// The deadline constants
// ---------------------------------------------------------------------------
//
// WHAT THE FLOOR CLAIM IS, EXACTLY — CORRECTED IN ROUND 13 (design.md D6c,
// roborev job 247 finding 1). This change replaces bare wall-clock deadlines and
// must not make the reported flake fire SOONER, which would be a regression
// wearing a fix's clothes. Round 8 stated that as "no bound here is tighter than
// the bound it replaced", full stop, and THAT CLAIM IS FALSE: the pre-#3515 code
// gave each wait an INDEPENDENT 60s, so a later wait got a fresh 60s no matter
// what earlier waits had consumed, and ONE ABSOLUTE DEADLINE CANNOT REPRODUCE
// THAT. An early stage may consume nearly all of it and leave a later stage
// seconds. "Any stage may consume the whole deadline" and "every stage is
// guaranteed a fresh 60s" are not jointly satisfiable by a single fixed bound.
//
// It is NOT fixed by restoring per-stage limits: that is the layer D6a descoped
// for producing twelve findings across four rounds. It is fixed by stating the
// trade truthfully, and by naming the assert for the property it actually tests.
//
//  * WHAT HOLDS — any single stage MAY CONSUME THE WHOLE deadline, and the base
//    is at or above the aggregate of the bounds it replaced. So no stage is
//    tighter *in isolation* than its old 60s, and the whole test is not tighter
//    than the old nominal total. That is what
//    `no_stage_in_isolation_is_tighter_than_the_bound_it_replaced` asserts.
//  * WHAT DOES NOT HOLD — a fresh per-wait allowance after earlier consumption.
//    An exhausted deadline leaves a later stage nothing, which
//    `an_exhausted_deadline_leaves_a_later_stage_nothing` pins so the stronger
//    claim cannot quietly come back.
//  * WHAT WAS BOUGHT — a BOUNDED TOTAL. The old design had no total bound at all
//    (`agent-gate.sh`'s `cli-tests` runs plain `cargo test`, with no harness
//    timeout), so the sibling test's seven independent 60s waits could genuinely
//    consume 420s+ before anything cut them off. A bounded total necessarily
//    gives up per-wait freshness; that is the trade, deliberately taken.
//
// The starvation path requires an early stage to burn the whole base (360s /
// 600s) while the product works, at which point the run is failing regardless —
// but that is a MITIGATION, recorded as one, and not the claim.
//
// WHAT COUNTS TOWARDS THE AGGREGATE IS DERIVED, NOT LABELLED (roborev job 253,
// finding 3): see `WaitCensus`. The previous form of the aggregate term added ONE
// hand-written stage to the old waits and was named for "every stage that draws on
// the one deadline", while two more had joined it — so the invariant could stay
// green on an undercounted base.

/// The single wall-clock bound every wait in the pre-#3515 version of this file
/// used: `Duration::from_secs(60)`, seven times over. The floor invariant is
/// stated against this value.
const OLD_BOUND_SECS: u64 = 60;

/// [`OLD_BOUND_SECS`] as a `Duration`. Both spellings exist because the aggregate
/// floor is computed in a `const fn`, where `Duration * u32` is not available.
const OLD_BOUND: Duration = Duration::from_secs(OLD_BOUND_SECS);

/// What the PRE-#3515 code bounded a wait with — the history a census entry
/// records, so the aggregate arithmetic never has to guess it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Replaced {
    /// An INDEPENDENT `OLD_BOUND` wall-clock wait. These are the waits the floor
    /// claim is literally about.
    OldBound,
    /// Nothing of its own: the old code did this work INSIDE another wait's
    /// `OLD_BOUND` (boot was folded into the first `OK` wait), or did not await it
    /// at all (there was no handler-entry wait).
    Folded,
    /// Nothing at all: the old code left this wait UNBOUNDED (`Command::output()`
    /// has no timeout, and nothing runs this target under a harness that would cut
    /// it short — design.md D6).
    Unbounded,
}

/// **ONE STAGE'S DRAW ON THE ONE DEADLINE.** The census these entries form is what
/// the aggregate floor is computed from, replacing the hand-labelled
/// `T1_OLD_WAITS`/`T2_OLD_WAITS`/`NEW_READINESS_WAITS` triple.
///
/// **WHY IT EXISTS (roborev job 253, finding 3).** The floor assert claimed to
/// count "EVERY stage that draws on the one deadline" and added exactly one term
/// to the old waits: readiness. It had been correct when written, and then two
/// more stages joined the deadline without joining the sum — `c.handler-entry`
/// (split out of the post-SIGINT wait, exactly as acknowledgement is separate from
/// readiness) and `e.durability-read` (newly BOUNDED, and absent from both
/// censuses). So the invariant could stay green on an undercounted base. That is
/// the fourth instance of one class in this issue — an assert named for more than
/// it tests, after three anchor instances and two floor ones — and the class is
/// closed the way rounds 6-10 closed the anchors: DERIVE the number, do not label
/// it.
///
/// **THE UNIT IS A WAIT, NOT A STAGE**, because a wait is what can consume the
/// whole deadline: `b.write-acks` is ONE stage containing FIVE waits, each of
/// which replaced an independent 60s bound. A wait counted here is one that is
/// GRANTED `stage.remaining()`; work merely CHARGED to a stage (the process spawn
/// inside `select_rows`) is not a wait and is not counted — see the note on
/// `select_rows` in `mod.rs`.
///
/// **WHAT IS DERIVED AND WHAT IS DECLARED — the boundary, stated because the
/// census is only as good as it.** The per-stage `waits` counts are DECLARED here
/// by hand: nothing in Rust can count the bounded waits inside a function body.
/// What IS verified is the STAGE SET: [`assert_census_matches_run`] compares this
/// census against the stages the test actually opened and finished, so a stage
/// added, removed or renamed without touching the census fails the integration
/// test that runs it. A wait added INSIDE an already-declared stage is the residual
/// — it changes `waits` and nothing detects that it did.
pub struct WaitCensus {
    /// The attribution stage this entry accounts for. Exactly one entry per stage,
    /// in the order the test opens them.
    pub stage: &'static str,
    /// How many waits inside that stage are GRANTED the one deadline.
    pub waits: u32,
    /// What the pre-#3515 code bounded those waits with.
    pub replaced: Replaced,
    /// Why this entry reads the way it does, for a reader auditing the arithmetic.
    pub note: &'static str,
}

/// The waits `sigint_in_writable_session_flushes_before_exit` draws on its one
/// deadline: SIX, of which TWO replaced an independent 60s bound.
pub const T1_WAIT_CENSUS: &[WaitCensus] = &[
    WaitCensus {
        stage: "a.session-up",
        waits: 1,
        replaced: Replaced::Folded,
        note: "the old code never awaited the banner: boot and engine init happened INSIDE the \
               first 60s `OK` wait",
    },
    WaitCensus {
        stage: "b.write-ack",
        waits: 1,
        replaced: Replaced::OldBound,
        note: "the post-spawn `OK` wait",
    },
    WaitCensus {
        stage: "c.handler-entry",
        waits: 1,
        replaced: Replaced::Folded,
        note: "no handler-entry wait existed; the marker was never awaited. It is now a SEPARATE \
               wait from clean exit, exactly as acknowledgement is separate from readiness — and \
               it was the stage the previous form of this census omitted",
    },
    WaitCensus {
        stage: "d.clean-exit",
        waits: 1,
        replaced: Replaced::OldBound,
        note: "the post-SIGINT `wait_timeout`",
    },
    WaitCensus {
        stage: "e.durability-read",
        waits: 2,
        replaced: Replaced::Unbounded,
        note: "`wait_timeout` on the read-side child and the pipe collection, each GRANTED \
               `stage.remaining()`; the spawn is charged but not granted a bound. The old \
               `Command::output()` bounded none of it, so this stage is a new ceiling — and it \
               was missing from both censuses",
    },
];

/// The same for `writable_session_auto_flushes_mid_session_across_threshold`: TEN
/// waits, of which SEVEN replaced an independent 60s bound.
pub const T2_WAIT_CENSUS: &[WaitCensus] = &[
    WaitCensus {
        stage: "a.session-up",
        waits: 1,
        replaced: Replaced::Folded,
        note: "as in the sibling test: boot was folded into the first 60s `OK` wait",
    },
    WaitCensus {
        stage: "b.write-acks",
        waits: 5,
        replaced: Replaced::OldBound,
        note: "FIVE waits in ONE stage — the five per-write `OK` waits, each of which replaced an \
               independent 60s bound. This is why the census unit is a wait and not a stage",
    },
    WaitCensus {
        stage: "c.mid-session-flush",
        waits: 1,
        replaced: Replaced::OldBound,
        note: "the mid-session durable-artifact wait",
    },
    WaitCensus {
        stage: "d.eof-exit",
        waits: 1,
        replaced: Replaced::OldBound,
        note: "the post-EOF exit wait",
    },
    WaitCensus {
        stage: "e.durability-read",
        waits: 2,
        replaced: Replaced::Unbounded,
        note: "the same two bounded waits as in the sibling test, from the same shared helper",
    },
];

/// EVERY wait drawing on the deadline, whatever the old code did about it.
const fn waits_sharing(census: &[WaitCensus]) -> u32 {
    let mut total = 0;
    let mut i = 0;
    while i < census.len() {
        total += census[i].waits;
        i += 1;
    }
    total
}

/// Only the waits that replaced an independent `OLD_BOUND` — the waits the floor
/// claim is literally about.
///
/// It reaches a failure message (the floor assert's text) AND an assert of its own
/// (`each_census_matches_the_totals_its_documentation_states`), which is the point:
/// until round 16 it was interpolated and nothing else, so it could drift with
/// nothing failing (roborev job 259, finding 2).
const fn old_bounded_waits(census: &[WaitCensus]) -> u32 {
    let mut total = 0;
    let mut i = 0;
    while i < census.len() {
        if matches!(census[i].replaced, Replaced::OldBound) {
            total += census[i].waits;
        }
        i += 1;
    }
    total
}

/// THE AGGREGATE FLOOR: an `OLD_BOUND` for EVERY wait that draws on the one
/// deadline.
///
/// **THE RESERVE FOR A WAIT THE OLD CODE DID NOT BOUND IS A CHOICE, NOT A
/// MEASUREMENT**, and it is recorded as one. For a `Replaced::OldBound` wait the
/// term is the bound it literally replaced. For a `Folded` or `Unbounded` wait
/// there is no old bound to preserve — the term is `OLD_BOUND` anyway, so that the
/// base is large enough for EVERY wait sharing the deadline to take a full
/// `OLD_BOUND` without the total being exceeded. That is the strongest form of the
/// floor claim one absolute deadline can support, and it is precisely what roborev
/// job 232's finding 2 asked for when it reported the readiness stage missing: a
/// new consumer of the shared budget leaves the original waits short unless the
/// base covers it too.
const fn aggregate_floor(census: &[WaitCensus]) -> Duration {
    Duration::from_secs(OLD_BOUND_SECS * waits_sharing(census) as u64)
}

/// **VERIFY A TEST'S WAIT CENSUS AGAINST THE STAGES IT ACTUALLY RAN.** Call it as
/// the last statement of each integration test.
///
/// This is what stops [`WaitCensus`] being one more hand-label (roborev job 253,
/// finding 3). The per-stage `waits` counts cannot be derived — nothing in Rust
/// counts the bounded waits inside a function body — but the STAGE SET can be, and
/// it is: the deadline records every stage that was OPENED, and a stage added,
/// removed or renamed without touching the census fails here, in the very test
/// that runs it.
///
/// **IT IS THE OPENED SET, NOT THE COMPLETED ONE (roborev job 255, finding 1).**
/// This check used to read the stages that had `finish`ed, which left it defeatable
/// by the very thing it is named for: a stage that draws deadline-backed waits and
/// is then dropped without finishing counted as though it had never existed, so the
/// aggregate floor could again be asserted against an undercounted base — the fifth
/// instance in this change of a guard that does not cover the case it is named for.
/// A stage is now recorded where it COMES INTO EXISTENCE, which no later omission
/// can undo.
///
/// ORDER IS PART OF IT: the census reads as a walk through the test, and an entry
/// whose position no longer matches the run is a census a reader can no longer
/// audit against the code.
///
/// **IT ALSO ASSERTS THAT EVERY OPENED STAGE FINISHED** (roborev job 259, finding
/// 2), as a SEPARATE assert. `unfinished_stages()` arrived in round 15 and was
/// only ever interpolated into the message above — so a run in which every
/// DECLARED stage is opened and one of them is never `finish`ed matched the
/// declared list, passed, and lost that stage's timing in silence. A value that
/// reaches nothing but a failure message is not a guard.
///
/// WHAT IT DOES NOT CATCH, so nobody reads more into a green run than is there: a
/// wait ADDED INSIDE an already-declared stage. That changes `waits` and this check
/// cannot see it. The stage set is verified; the per-stage counts are declared, and
/// so is every entry's `note`.
pub fn assert_census_matches_run(test: &str, census: &[WaitCensus], deadline: &TestDeadline) {
    let declared: Vec<&str> = census.iter().map(|e| e.stage).collect();
    let ran = deadline.opened_stages();
    let unfinished = deadline.unfinished_stages();
    let unfinished_note = if unfinished.is_empty() {
        "(every stage this run opened also finished)".to_string()
    } else {
        format!("{unfinished:?} — opened and never finished")
    };
    assert_eq!(
        ran,
        declared,
        "{test}: the stages this run OPENED are not the stages its wait census declares.\n\
         declared: {declared:?}\n\
         opened:   {ran:?}\n\
         of those: {unfinished_note}\n\
         the census, as declared (each entry's `note` says why it reads the way it does):\n{}\n\
         The census is what the aggregate floor is computed from \
         (`no_stage_in_isolation_is_tighter_than_the_bound_it_replaced`), so a stage that draws \
         on the one deadline without appearing there means the floor was asserted against an \
         UNDERCOUNTED base — roborev job 253, finding 3. Add or remove the entry, and move the \
         base with it: the floor assert requires the base to EQUAL the derived aggregate.\n\
         (A stage is counted from the moment it is OPENED, so one that was opened and never \
         finished appears above and is named on the `of those:` line — that is a defect in the \
         test rather than in the census, but it can no longer hide the stage from this check \
         (job 255, finding 1). A stage missing from `opened` altogether means the test returned \
         before reaching it.)",
        describe_census(census)
    );

    // **ASSERTED, NOT MERELY INTERPOLATED** (roborev job 259, finding 2). Round 15
    // added `unfinished_stages()` and used it in the message above and nowhere
    // else, which does not GUARD anything: if every DECLARED stage is opened but
    // one is never `finish`ed, the opened list still equals the declared list, the
    // assert above passes, and that stage's timing is silently absent from the
    // attribution report the stages exist for. A value that only ever reaches a
    // failure message is diagnostics wearing a guard's name — the sixth instance of
    // that class in this change — so the property gets its own assert.
    //
    // IT IS A SEPARATE ASSERT AND NOT A STRONGER FORM OF THE ONE ABOVE, because the
    // two catch different defects and a reader has to be told which one fired: a
    // set mismatch means the census is wrong, an unfinished stage means the test
    // returned or panicked mid-stage. Ordered second so a run with BOTH is reported
    // as the census mismatch it primarily is.
    assert!(
        unfinished.is_empty(),
        "{test}: {unfinished:?} — opened and never `finish`ed, though the stage SET matches the \
         census.\n\
         opened: {ran:?}\n\
         A stage is recorded when it is OPENED (job 255, finding 1), so such a stage passes the \
         stage-set check above while contributing NO timing: it is missing from the attribution \
         report, from the `slowest completed stage` figure, and from every failure message that \
         prints them — which is the whole reason stages exist (D6a). The cause is in the test, \
         not in the census: either the stage was dropped without `finish()`, or the test returned \
         before reaching it.\n{}",
        deadline.report()
    );
}

/// The census as a table, for any failure that has to be audited against the code.
///
/// It is what READS each entry's `note`: the arithmetic needs only `waits` and
/// `replaced`, so without a message that renders the reasons, the reasons would be
/// a comment the compiler discards — and this file's own round-3 blocker was a
/// comment that could not fail.
fn describe_census(census: &[WaitCensus]) -> String {
    census
        .iter()
        .map(|e| {
            format!(
                "  {} — {} wait(s), replaced: {:?}; {}",
                e.stage, e.waits, e.replaced, e.note
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Base deadline for `sigint_in_writable_session_flushes_before_exit`.
///
/// `aggregate_floor(T1_WAIT_CENSUS)` = 6 waits x 60s = **360s**. Hand-written
/// rather than computed from the census so the assert comparing the two has teeth
/// in BOTH directions: derive the constant and the invariant becomes a tautology
/// that no undercount can fail (see
/// `no_stage_in_isolation_is_tighter_than_the_bound_it_replaced`).
pub const T1_DEADLINE_BASE: Duration = Duration::from_secs(360);

/// Calibration ceiling for that test: 2x the base, the ratio this change has used
/// since round 8. No measured contention may push the deadline past it.
pub const T1_DEADLINE_CAP: Duration = Duration::from_secs(720);

/// Base deadline for `writable_session_auto_flushes_mid_session_across_threshold`.
///
/// `aggregate_floor(T2_WAIT_CENSUS)` = 10 waits x 60s = **600s**. Larger than its
/// sibling because five of those waits are the five per-write `OK` waits.
pub const T2_DEADLINE_BASE: Duration = Duration::from_secs(600);

/// Calibration ceiling for the sibling test, and THE PLACE
/// [`MAX_TEST_DEADLINE`] NOW BINDS: 1.5x the base rather than the 2x its sibling
/// gets, because 2x would be 1200s and a test may not outlast the gate it runs in.
/// It is deliberately left at exactly the limit rather than under it, so that the
/// next wait added to `T2_WAIT_CENSUS` raises the floor past what the limit
/// permits and FAILS the assert — a conflict between the floor claim and the
/// test-length limit is a decision for a human, not something to absorb silently.
pub const T2_DEADLINE_CAP: Duration = Duration::from_secs(900);

/// The upper bound on any test's deadline, because that deadline is now the ONLY
/// timeout these tests have (verified: `agent-gate.sh`'s `cli-tests` runs plain
/// `cargo test`, and nothing anywhere runs `cqlite-cli` under nextest, so no
/// harness `slow-timeout` applies — design.md D6). A self-termination that
/// outlasts the run it protects protects nothing.
///
/// Anchored on the full agent gate's own wall clock (15-20 minutes, CLAUDE.md):
/// one test able to run longer than the entire gate would dominate the
/// `cli-tests` component it lives in.
const MAX_TEST_DEADLINE: Duration = Duration::from_secs(900);

/// The quiet-host reference every in-band measurement is scaled against:
/// `scale = max(1, observed / QUIET_OBSERVATION_BASELINE)`.
///
/// ONE constant, for both `t_boot` (spawn -> readiness banner) and `t_ack`
/// (INSERT written -> `OK` observed), because both measure the same shape of
/// work: a full round-trip through a freshly-loaded child.
///
/// MEASURED values recorded for this change (warm build, 16-core box,
/// `--test-threads=1`):
///
/// ```text
///                                 quiet        load avg 30   load avg 116
///   t_boot (spawn -> banner)      11.4-29ms    45-66ms       81-132ms
///   t_ack, SIGINT test            1.4-3ms      13ms          76ms
///   t_ack, sibling (slowest of 5) 38-43ms      97ms          133ms
/// ```
///
/// THE VALUE IS DERIVED FROM THAT TABLE, NOT LABELLED AGAINST IT (roborev job 233,
/// finding 2). This comment used to call 81ms "the fastest observation taken under
/// real contention" and set the baseline to 60ms — while the table three lines
/// above it records loaded observations of 13ms, 45ms and 76ms. At the recorded
/// load-average-30 timings the SIGINT test could therefore stay entirely unscaled:
/// the calibration inert at moderate load, which is the ORIGINAL defect this change
/// exists to remove. That was the THIRD hand-labelled "binding" value in this file
/// to decay (round 2: a `MEASURED_QUIET_T_ACK` of 3ms against a recorded 1.4ms;
/// round 6: an 11.4ms `t_boot` anchor that was itself permissive), so the label is
/// GONE rather than corrected — `the_baseline_is_quiet_inert_and_contention_active`
/// encodes the table as DATA and derives both bounds from it, asserting activation
/// per case instead of against one hand-picked observation.
///
/// WHAT AN "INTENDED CONTENTION CASE" IS: one TEST RUN at one recorded load level,
/// not one cell of the table. That is the unit the mechanism operates on —
/// `calibrate` takes the LARGEST scale over every measurement a run makes, so what
/// decides whether a run's deadline scales is the MAXIMUM of that run's
/// observations and never any single one of them. A 13ms `t_ack` does not mean the
/// calibration failed for that run: the same run's `t_boot` measured 45-66ms.
///
/// The table leaves exactly one admissible window — above the slowest recorded
/// QUIET observation (43ms, the sibling's slowest ack: below it, an unloaded host
/// scales and the calibration becomes a flake source of its own) and below the
/// LEAST-scaled contention case (45ms: the SIGINT test at load average 30, whose
/// binding observation is the slowest of its two measurements at their recorded
/// floors). Both numbers are computed from the table by the test, which also
/// asserts the window is non-empty; 44ms is the only whole millisecond inside it.
///
/// THE WINDOW IS NARROW (2ms), AND NARROW IS SAFE IN THE DIRECTION THAT MATTERS.
/// `scale` is floored at 1 and the span clamped at `base`, so calibration can only
/// ever LOOSEN a deadline: over-eager engagement costs a marginally later timeout
/// on a genuine hang, while under-eager engagement is the flake this change exists
/// to remove. A quiet host that happens to measure 45ms gets a deadline 2% longer
/// and nothing else. The hazard being guarded against is the opposite one: the
/// first version of this change used 500ms/200ms baselines and `scale` stayed at
/// EXACTLY 1.000 in every run taken, including load average 116.
pub const QUIET_OBSERVATION_BASELINE: Duration = Duration::from_millis(44);

// ---------------------------------------------------------------------------
// The one deadline
// ---------------------------------------------------------------------------

/// A test's ONE deadline, plus the stage timings that attribute a failure to a
/// stage.
///
/// THE INVARIANT THIS TYPE EXISTS TO MAKE TRUE BY CONSTRUCTION: there is exactly
/// one bound in the test, and no wait may be granted or started past it. Every
/// wait — `wait_for`, `wait_timeout`, `recv_timeout`, the progress-observing poll
/// — takes its timeout from [`Stage::remaining`], which is this deadline and
/// nothing else. No call site subtracts anything, so no call site can forget to;
/// and no call site can be granted anything, so none can double-spend.
///
/// THE CLAIM IS ABOUT THE TIMEOUT ARITHMETIC, NOT ABOUT WALL CLOCK (roborev job
/// 232 finding 1). This deadline bounds how long the test WAITS FOR EVIDENCE; a
/// success OBSERVED while it lapses is still accepted, deliberately, because
/// failing a stage that saw its signal would be a false failure on a working
/// product. `poll_with_progress` in `mod.rs` owns that decision and quantifies
/// how late an accepted success can be. The failure path is the same rule read
/// the other way (job 233 finding 1): no expiry is declared until a final
/// non-blocking check confirms the evidence really is absent — taken from the ONE
/// snapshot of the ONE sequenced store the failure message renders. There is no
/// second store left for that verdict to disagree with: the queue whose divergence
/// produced job 236 finding 1, job 243 finding 1 and job 247 findings 2 and 3 is
/// deleted (design.md D6b).
///
/// It is LIVE from construction: build it as the first statement of the test, so
/// every stage including the first is charged.
/// **THE CALIBRATED STATE, BEHIND ONE `RefCell`** (#3652, roborev job 271
/// finding 5).
///
/// It used to be four plain fields mutated through `&mut self`, and that made a
/// measurement UNAPPLIABLE while any stage was open: a [`Stage`] borrows the
/// deadline immutably for its whole life, so the sibling test's five-write loop
/// could not fold a completed acknowledgement in until the loop had finished and
/// the stage had been dropped — i.e. an ack could not extend the deadline for the
/// LATER writes of the same loop, which is precisely the calibration-inertness
/// class #3515 treated as its most important.
///
/// Interior mutability rather than a restructured loop, because the alternatives
/// each break something load-bearing: re-opening the stage per write would record
/// five stages and fail the wait census (`assert_census_matches_run`), and
/// tracking the stage's own timing outside the stage would leave two clocks for
/// one stage. This type already holds `RefCell` state for exactly this reason
/// (`opened`, `stages`, both written by a live `Stage`), so this is the shape it
/// already had rather than a new mechanism.
///
/// Grouped in ONE cell and not four: every write here is one transition (a new
/// scale implies a new span implies a new deadline instant), so a single borrow is
/// what makes it impossible to publish half of it.
#[derive(Debug)]
struct Calibration {
    /// The instant past which no wait in this test may be STARTED (a wait already
    /// in flight can return its observed success a bounded moment later — see
    /// `poll_with_progress`). Moves LATER on calibration and never earlier.
    deadline: Instant,
    /// `clamp(base x scale, base, cap)`.
    span: Duration,
    /// The LARGEST scale any in-band measurement has yielded so far.
    scale: f64,
    /// Every measurement folded in, with the scale it yielded, so a failure can
    /// report how the one bound was arrived at.
    observations: Vec<(&'static str, Duration, f64)>,
}

pub struct TestDeadline {
    started: Instant,
    /// The deadline instant, its span, the largest scale and every observation —
    /// one cell, so a completed measurement can be folded in WHILE a stage is open
    /// (#3652). See [`Calibration`].
    cal: RefCell<Calibration>,
    base: Duration,
    cap: Duration,
    /// EVERY STAGE OPENED, in the order [`TestDeadline::stage`] created it.
    ///
    /// **RECORDED WHERE A STAGE COMES INTO EXISTENCE, NOT WHERE IT REPORTS**
    /// (roborev job 255, finding 1). This record is what the wait census is
    /// verified against, and it used to be written by `finish()` — so a stage that
    /// was opened, granted deadline-backed waits and then dropped WITHOUT
    /// finishing was invisible to it, and the undercount the census check exists
    /// to catch could be reintroduced by a stage that simply never finishes. A
    /// stage cannot draw on the deadline without being opened, so opening is the
    /// only point at which the record is complete by construction.
    opened: RefCell<Vec<&'static str>>,
    /// Completion TIMINGS, kept SEPARATELY from the record above because a
    /// duration only exists once a stage has ended. `RefCell` because a live
    /// [`Stage`] borrows the deadline immutably and records itself on `finish`.
    stages: RefCell<Vec<(&'static str, Duration)>>,
}

impl TestDeadline {
    /// Start the clock. `base` applies immediately, uncalibrated: the first
    /// measurement it could calibrate from has not been taken yet.
    ///
    /// THE RESIDUAL, stated at the seam (design.md): this base is the one
    /// irreducible bound. Calibrating it would need a measurement taken before
    /// the test began, whose own bound would need a measurement before *that* —
    /// the regress terminates only by accepting one bare wall-clock value. What
    /// the design buys is that the value is generous (above the whole nominal
    /// aggregate of the bounds it replaced) and that it loosens as soon as the
    /// first in-band measurement lands.
    pub fn start(base: Duration, cap: Duration) -> Self {
        debug_assert!(base <= cap, "base must not exceed cap");
        let started = Instant::now();
        Self {
            started,
            cal: RefCell::new(Calibration {
                deadline: started + base,
                span: base,
                scale: 1.0,
                observations: Vec::new(),
            }),
            base,
            cap,
            opened: RefCell::new(Vec::new()),
            stages: RefCell::new(Vec::new()),
        }
    }

    /// Fold one in-band measurement into the ONE scale.
    ///
    /// `scale = max(1, observed / QUIET_OBSERVATION_BASELINE)`, and the deadline
    /// takes the LARGEST scale seen so far — so calibration is monotone: it can
    /// only ever move the deadline LATER. A quiet host measures below the
    /// baseline, yields `scale == 1`, and gets exactly `base`.
    /// **IT TAKES `&self`, SO A MEASUREMENT CAN BE APPLIED THE MOMENT IT COMPLETES
    /// — EVEN WITH THE STAGE THAT PRODUCED IT STILL OPEN** (#3652, roborev job 271
    /// finding 5). Through `&mut self` it could not: a live [`Stage`] borrows this
    /// deadline, so the sibling test's five acknowledgements reached this function
    /// only after all five waits had finished, and a slow FIRST ack could not
    /// extend the deadline for the four writes that followed it.
    ///
    /// **REPEATED CALLS DO NOT COMPOUND, WHICH IS WHAT MAKES PER-MEASUREMENT
    /// APPLICATION SAFE.** The span is `clamp(base x LARGEST scale, base, cap)` —
    /// derived from the base every time and never from the current span — so
    /// folding the same value in twice, or folding five values in one at a time
    /// rather than the slowest of them at the end, yields the identical deadline.
    /// What each call does add is one entry to `observations`, which is the
    /// diagnostic record [`TestDeadline::describe`] renders: five acks now appear
    /// as five observations, which is what actually happened.
    pub fn calibrate(&self, name: &'static str, observed: Duration) {
        let scale = (observed.as_secs_f64() / QUIET_OBSERVATION_BASELINE.as_secs_f64()).max(1.0);
        let mut cal = self.cal.borrow_mut();
        cal.observations.push((name, observed, scale));
        if scale <= cal.scale {
            return;
        }
        cal.scale = scale;
        let scaled = Duration::from_secs_f64(self.base.as_secs_f64() * scale);
        let span = scaled.clamp(self.base, self.cap);
        debug_assert!(
            span >= cal.span,
            "calibration may only ever LOOSEN the deadline"
        );
        cal.span = span;
        cal.deadline = self.started + span;
    }

    /// Open an attribution stage. A [`Stage`] carries a name and a start instant
    /// and NO BOUND: its `remaining()` is this deadline's.
    ///
    /// THE STAGE IS RECORDED HERE, at the point it comes into existence (roborev
    /// job 255, finding 1) — see [`TestDeadline::opened`]. `finish()` adds only the
    /// timing.
    pub fn stage(&self, name: &'static str) -> Stage<'_> {
        self.opened.borrow_mut().push(name);
        Stage {
            deadline: self,
            name,
            started: Instant::now(),
        }
    }

    /// Time left before the one deadline.
    pub fn remaining(&self) -> Duration {
        self.cal
            .borrow()
            .deadline
            .saturating_duration_since(Instant::now())
    }

    /// How much of the test has been consumed. Deliberately NOT named `elapsed`:
    /// the #2642 wall-clock-assert guard keys on that identifier, and this value
    /// is legitimately compared in the unit tests below.
    pub fn spent(&self) -> Duration {
        self.started.elapsed()
    }

    /// The deadline's span — `clamp(base x scale, base, cap)`.
    pub fn span(&self) -> Duration {
        self.cal.borrow().span
    }

    /// How the one bound was arrived at. Reported by every failure.
    pub fn describe(&self) -> String {
        // ONE borrow for every calibrated fact this sentence states, so the span,
        // the scale and the observations it lists cannot come from different
        // moments (#3652 — a stage may now calibrate while this is being built,
        // from another site in the same thread's call stack).
        let (span, scale, observations) = {
            let cal = self.cal.borrow();
            let observations = if cal.observations.is_empty() {
                "none yet — the deadline is still its UNCALIBRATED base (design.md, \"The \
                 residual\": no measurement exists yet to calibrate it against)"
                    .to_string()
            } else {
                cal.observations
                    .iter()
                    .map(|(name, value, scale)| format!("{name} {value:.3?} => scale {scale:.3}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            (cal.span, cal.scale, observations)
        };
        format!(
            "ONE per-test deadline {:.1?} = clamp(base {:.1?} x scale {:.3}, base, cap {:.1?}), \
             where scale is the LARGEST of [{observations}] over quiet baseline {:.0?}. ANY single \
             stage may consume the whole of it: there are no per-stage budgets. Observed progress \
             is reported as evidence and NEVER extends it. Spent {:.2?}, remaining {:.2?}",
            span,
            self.base,
            scale,
            self.cap,
            QUIET_OBSERVATION_BASELINE,
            self.spent(),
            self.remaining()
        )
    }

    /// The stages that were OPENED, in the order they were opened — the run's own
    /// record of which stages drew on this deadline.
    ///
    /// Exists so the wait census can be checked against reality rather than
    /// trusted (see [`assert_census_matches_run`]), and keyed on OPENING rather
    /// than completion so that a stage which never finishes still appears (roborev
    /// job 255, finding 1).
    pub fn opened_stages(&self) -> Vec<&'static str> {
        self.opened.borrow().clone()
    }

    /// Stages that were opened and never finished, for the census failure message:
    /// such a stage IS counted by [`TestDeadline::opened_stages`], and naming it
    /// separates "the census is wrong" from "the test returned mid-stage".
    ///
    /// Positional, not set-based: the Nth opening of a name is matched against the
    /// Nth completion of it, so a stage opened twice and finished once is reported
    /// as one unfinished occurrence rather than as none.
    pub fn unfinished_stages(&self) -> Vec<&'static str> {
        let mut finished: Vec<&'static str> =
            self.stages.borrow().iter().map(|(n, _)| *n).collect();
        let mut unfinished = Vec::new();
        for name in self.opened.borrow().iter() {
            match finished.iter().position(|f| f == name) {
                Some(i) => {
                    finished.remove(i);
                }
                None => unfinished.push(*name),
            }
        }
        unfinished
    }

    /// Per-stage timings + deadline state, for both diagnostics and the
    /// end-of-test record printed with `--nocapture`. This is the ATTRIBUTION the
    /// stages exist for.
    pub fn report(&self) -> String {
        let recorded = self.stages.borrow();
        let stages = if recorded.is_empty() {
            "(none completed)".to_string()
        } else {
            recorded
                .iter()
                .map(|(name, took)| format!("{name} {took:.3?}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let worst = recorded
            .iter()
            .max_by_key(|(_, took)| *took)
            .map(|(name, took)| format!("; slowest completed stage: {name} {took:.3?}"))
            .unwrap_or_default();
        format!(
            "stage timings: {stages}{worst}\ndeadline {:.1?}: spent {:.2?}, remaining {:.2?}",
            self.span(),
            self.spent(),
            self.remaining()
        )
    }
}

/// An ATTRIBUTION stage: a name, a start instant, and a borrow of the test's one
/// deadline. It holds no bound of its own, which is why no stage can be tighter
/// than the deadline and no stage can starve another.
pub struct Stage<'d> {
    deadline: &'d TestDeadline,
    name: &'static str,
    started: Instant,
}

/// The ONE deadline had ALREADY PASSED when an operation was about to be
/// INITIATED, so that operation was refused (roborev job 253, finding 2).
///
/// It reports what was not started and how the bound was derived, and it names NO
/// cause: which stage exhausted the deadline is the attribution report's job, and
/// this measurement cannot select between "an earlier stage was slow" and "the
/// product is wedged".
#[derive(Debug)]
pub struct Expired {
    stage: &'static str,
    what: String,
    spent: Duration,
    deadline: String,
}

impl Expired {
    pub fn describe(&self) -> String {
        format!(
            "stage {}: the test's ONE deadline had ALREADY PASSED, so {} was NOT initiated \
             (roborev job 253, finding 2: an operation issued after expiry can still produce \
             fresh evidence — an `OK`, a handler-entry marker, an exit — that a wait's final \
             look would accept, carrying the test past its sole bound with work that began \
             after it).\n\
             this stage had been running {:.2?} when the operation was refused.\n\
             {}\n\
             WHAT THIS ESTABLISHES: only that the deadline was exhausted before this operation \
             began. It does NOT say which stage consumed it, and nothing here is a statement \
             about the product — read the stage timings below for the attribution.",
            self.stage, self.what, self.spent, self.deadline
        )
    }
}

impl Stage<'_> {
    /// **THE ONE PLACE A PER-WAIT TIMEOUT IS COMPUTED**, and what it returns is
    /// the TEST's remaining time — not a stage allowance, because there is none.
    pub fn remaining(&self) -> Duration {
        self.deadline.remaining()
    }

    /// This stage's own duration so far. DIAGNOSTIC ONLY: nothing is bounded by
    /// it. (Not named `elapsed`; see [`TestDeadline::spent`].)
    pub fn spent(&self) -> Duration {
        self.started.elapsed()
    }

    /// **REFUSE TO INITIATE NEW EVIDENCE-PRODUCING WORK ONCE THE ONE DEADLINE HAS
    /// PASSED** (roborev job 253, finding 2). Call this immediately before every
    /// write, signal, spawn or stdin close.
    ///
    /// THE DISTINCTION THIS PRESERVES, WHICH IS NOT THE SAME THING AS THE ROUND-9
    /// RULING. The deadline bounds how long the test WAITS FOR EVIDENCE, never
    /// whether it ACCEPTS evidence already in hand: every expiry site takes a final
    /// non-blocking look and returns a success it finds there, deliberately,
    /// because failing a stage that observed its signal is a false failure on a
    /// working product. That stays exactly as it is.
    ///
    /// What is NOT sound is *manufacturing* evidence after expiry. An operation
    /// ISSUED past the deadline — the `writeln!`, the `libc::kill`, a child spawn,
    /// the stdin `drop` — can still produce a fresh `OK`, a fresh handler-entry
    /// marker or a fresh exit, which the final look then accepts as though it had
    /// arrived in time. That carries the test past its SOLE bound with work that
    /// began after it, which no amount of care inside `wait_for` can distinguish:
    /// by the time the line exists, it is indistinguishable from one that arrived
    /// late. The check therefore belongs at the point of INITIATION, which is the
    /// only place the two cases are still distinguishable.
    ///
    /// `Err` rather than a panic so the call site can clean up first — a
    /// post-expiry failure must not leak a running child (see
    /// `require_live_or_kill` in `mod.rs`).
    pub fn check_live(&self, what: &str) -> Result<(), Expired> {
        if self.remaining().is_zero() {
            return Err(Expired {
                stage: self.name,
                what: what.to_string(),
                spent: self.spent(),
                deadline: self.deadline.describe(),
            });
        }
        Ok(())
    }

    /// [`Stage::check_live`] for a site with nothing to clean up (a spawn that has
    /// not happened yet), which panics rather than returning.
    pub fn require_live(&self, what: &str) {
        if let Err(expired) = self.check_live(what) {
            panic!("{}\n{}", expired.describe(), self.report());
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The stage's own spend plus the derivation of the one deadline bounding it.
    pub fn describe(&self) -> String {
        format!(
            "stage {} has been running {:.2?}. {}",
            self.name,
            self.spent(),
            self.deadline.describe()
        )
    }

    pub fn report(&self) -> String {
        self.deadline.report()
    }

    /// Record this stage's duration for the attribution report and return it.
    pub fn finish(self) -> Duration {
        let took = self.spent();
        self.deadline.stages.borrow_mut().push((self.name, took));
        took
    }
}

/// The wait census's own coverage, moved out under the campsite rule (#1135):
/// this file was 40 lines from the 1500-line test threshold when round 16 added
/// to it. Split by SUBJECT — everything about [`assert_census_matches_run`] is
/// there, everything about the one deadline's arithmetic stays here.
#[cfg(test)]
mod census_tests;

/// The CALIBRATION's own coverage, moved out under the campsite rule (#1135): this
/// file was 50 lines from the 1500-line test threshold when #3652 added to it.
/// Split by SUBJECT — everything about how a measurement becomes a deadline is
/// there (including the round-8 arithmetic tests moved with it), everything about
/// the stage/deadline machinery around it stays here.
#[cfg(test)]
mod calibration_tests;

// ---------------------------------------------------------------------------
// Unit coverage
//
// These exist because THE ROUND-3 BLOCKER WAS A COMMENT THAT COULD NOT FAIL: the
// budget arithmetic was written in prose above the constants, was wrong, and
// nothing noticed until a RED run's timing was read by hand. Every claim above
// that a test can hold is asserted here.
// ---------------------------------------------------------------------------

/// NO STAGE, IN ISOLATION, IS TIGHTER THAN THE BOUND IT REPLACED — and the base
/// is EXACTLY the census-derived aggregate of every wait that draws on the one
/// deadline.
///
/// **THE NAME IS THE CORRECTION (design.md D6c, roborev job 247 finding 1).** This
/// test used to be called `the_deadline_is_never_tighter_than_the_bounds_it_replaced`,
/// which names a STRONGER property than it tests and than one deadline can
/// deliver: the old code gave every wait an independent 60s, and no single
/// absolute deadline can guarantee a later wait a fresh allowance after earlier
/// consumption. An assert named for a stronger property than it tests is the
/// defect class this change has hit repeatedly, so the name and the messages now
/// say exactly what is asserted:
///
/// * IN ISOLATION — a stage may consume the WHOLE deadline (asserted separately by
///   `any_single_stage_may_consume_the_whole_deadline`), so a stage that runs with
///   the deadline untouched has at least the 60s it replaced iff `base >= 60s`.
/// * IN AGGREGATE — the base EQUALS `aggregate_floor(census)`, an `OLD_BOUND` for
///   every wait sharing the deadline, so the test as a whole is not tighter than
///   the sum of the bounds it replaced and every one of those waits could take a
///   full `OLD_BOUND`.
///
/// What is NOT asserted here, because it is not true: that a later stage still has
/// 60s after earlier stages have consumed the deadline. See
/// `an_exhausted_deadline_leaves_a_later_stage_nothing`.
///
/// **THE AGGREGATE TERM IS DERIVED FROM THE WAIT CENSUS (roborev job 253, finding
/// 3), AND EQUALITY IS WHAT GIVES IT TEETH.** The previous form added ONE
/// hand-written term (readiness) to the old waits while claiming to count every
/// stage sharing the deadline, and two stages had since joined it —
/// `c.handler-entry` and the newly-bounded `e.durability-read` — so the invariant
/// could pass on an undercounted base. Deriving the base from the census instead
/// would make this assert a TAUTOLOGY that no undercount could fail; the base is
/// therefore hand-written and required to EQUAL the derived floor, which fails in
/// both directions: an undercounted census (a wait or stage dropped) and a base
/// that carries margin the census does not explain.
#[test]
fn no_stage_in_isolation_is_tighter_than_the_bound_it_replaced() {
    for (test, base, cap, census) in [
        (
            "sigint_in_writable_session_flushes_before_exit",
            T1_DEADLINE_BASE,
            T1_DEADLINE_CAP,
            T1_WAIT_CENSUS,
        ),
        (
            "writable_session_auto_flushes_mid_session_across_threshold",
            T2_DEADLINE_BASE,
            T2_DEADLINE_CAP,
            T2_WAIT_CENSUS,
        ),
    ] {
        // A census entry per stage, and no stage twice: the aggregate is a sum over
        // entries, so a duplicated stage would double-count it and a missing one
        // would be invisible here (the stage SET itself is verified against the
        // real run by `assert_census_matches_run`).
        for (i, entry) in census.iter().enumerate() {
            assert!(
                entry.waits > 0,
                "{test}: census entry {i} ({}) declares ZERO waits — a stage that draws nothing \
                 from the deadline does not exist",
                entry.stage
            );
            assert!(
                census.iter().filter(|e| e.stage == entry.stage).count() == 1,
                "{test}: stage {} appears more than once in the census; the aggregate is a sum \
                 over entries, so one entry per stage",
                entry.stage
            );
        }

        // PER WAIT, IN ISOLATION: any single stage may consume the whole deadline
        // (there are no per-stage budgets — see
        // `any_single_stage_may_consume_the_whole_deadline`), so a wait that runs
        // with the deadline untouched is not tighter than the 60s it replaced iff
        // the base is at least 60s. It is NOT a claim about a wait that starts
        // after earlier stages have consumed the deadline (D6c).
        assert!(
            base >= OLD_BOUND,
            "{test}: a base of {base:?} would let a single wait — even one running with the \
             deadline untouched — fire SOONER than the {OLD_BOUND:?} bound it replaced"
        );

        // IN AGGREGATE: an `OLD_BOUND` for EVERY wait that draws on this deadline,
        // counted from the census rather than hand-labelled. The old code had no
        // total bound at all, so any total is a new ceiling; it must at least cover
        // the sum of the nominal bounds it replaced AND every wait that now draws
        // on the same deadline. This is the bound that was BOUGHT (D6c): it is what
        // the loss of per-wait freshness paid for.
        let sharing = waits_sharing(census);
        let replaced = old_bounded_waits(census);
        let floor = aggregate_floor(census);
        let stages = census.len();
        assert_eq!(
            base,
            floor,
            "{test}: the base {base:?} is not the {floor:?} aggregate its wait census derives \
             ({sharing} waits across {stages} stages, of which {replaced} replaced an \
             independent {OLD_BOUND:?} wait, at {OLD_BOUND:?} each).\n\
             BELOW the floor: the test as a whole would be tighter than the bounds it replaced, \
             and a wait sharing the deadline could be left under its former allowance — which is \
             what roborev job 232 finding 2 reported and job 253 finding 3 found again, the census \
             having gained `c.handler-entry` and `e.durability-read` without gaining the terms \
             for them.\n\
             ABOVE the floor: the base carries margin the census does not explain, so either the \
             census is incomplete (add the wait) or the derivation has changed (change \
             `aggregate_floor`). Do NOT adjust the base alone — equality is what stops an \
             undercounted census passing this assert.\n\
             NOTE what this does and does not say (D6c): the base covering the aggregate is what \
             keeps the test as a whole from being tighter than the bounds it replaced; it does \
             NOT give a later stage a fresh {OLD_BOUND:?} once earlier stages have consumed the \
             deadline, and no single absolute deadline can.\n\
             the census this floor was derived from:\n{}",
            describe_census(census)
        );

        assert!(base <= cap, "{test}: base {base:?} exceeds cap {cap:?}");

        // The deadline is the ONLY timeout these tests have, so it must still
        // self-terminate inside the run it protects. This is where a census that
        // GROWS surfaces as a conflict rather than as a silently longer test: the
        // sibling's cap already sits at exactly `MAX_TEST_DEADLINE`.
        assert!(
            cap <= MAX_TEST_DEADLINE,
            "{test}: a {cap:?} cap exceeds the {MAX_TEST_DEADLINE:?} limit — it is the only \
             timeout this test has, and one that outlasts the gate it runs in protects nothing. \
             The census derives a {floor:?} base; if that is right, the conflict between the \
             floor claim and the test-length limit is a decision for a human"
        );
    }
}

/// ANY SINGLE STAGE MAY CONSUME THE WHOLE DEADLINE — the property the floor
/// invariant above rests on IN ISOLATION, and the one that kills the
/// "declared cap is not the actual maximum" family: a stage has no allowance to
/// exceed, and an earlier stage cannot starve a later one *by holding an
/// allowance*.
///
/// NOT "unconditional", which is what this comment used to say (roborev job 255,
/// finding 3, propagating design.md D6c): a stage whose predecessors have consumed
/// the deadline gets what is left, and can get nothing —
/// `an_exhausted_deadline_leaves_a_later_stage_nothing`. What holds unconditionally
/// is the ARITHMETIC asserted below: nothing is ever deducted from a stage for
/// another stage's sake.
///
/// A long deadline is used deliberately so the assert has a ~10-minute margin
/// against scheduling delay between two statements: this is an assert about
/// ARITHMETIC (nothing was deducted), not about speed.
#[test]
fn any_single_stage_may_consume_the_whole_deadline() {
    let deadline = TestDeadline::start(Duration::from_secs(3600), Duration::from_secs(3600));

    let first = deadline.stage("first");
    let first_remaining = first.remaining();
    let _ = first.finish();

    // A LATER stage, after an earlier one has completed, still has essentially the
    // entire deadline: nothing was deducted for the first stage, because no stage
    // has an allowance.
    let second = deadline.stage("second");
    assert!(
        second.remaining() > Duration::from_secs(3000),
        "a later stage must still be able to consume the whole deadline, but got {:?}",
        second.remaining()
    );
    assert!(
        first_remaining > Duration::from_secs(3000),
        "the first stage must be able to consume the whole deadline, but got {first_remaining:?}"
    );
}

/// **THE PROPERTY THAT DOES NOT HOLD, pinned so it cannot quietly come back
/// (design.md D6c).** One absolute deadline gives no stage a fresh allowance: once
/// it is exhausted, a later stage gets ZERO, where the pre-#3515 code would have
/// given that wait a full independent 60s.
///
/// This is the honest counterpart to `any_single_stage_may_consume_the_whole_deadline`
/// — the same absence of per-stage budgets read from the other side — and it is
/// asserted from an ALREADY-EXHAUSTED deadline (base and cap of zero), so it is
/// arithmetic and not a wall-clock race: no sleep, no threshold, nothing that a
/// loaded host can change.
#[test]
fn an_exhausted_deadline_leaves_a_later_stage_nothing() {
    let deadline = TestDeadline::start(Duration::ZERO, Duration::ZERO);
    let stage = deadline.stage("later");
    assert!(
        stage.remaining().is_zero(),
        "an exhausted deadline must leave a later stage NOTHING — that is the per-wait freshness \
         a single bounded total gives up (D6c). {:?} of allowance here would mean a per-stage \
         budget had come back",
        stage.remaining()
    );
}

// ---------------------------------------------------------------------------
// THE RECORDED MEASUREMENT TABLE, AS DATA (roborev job 233, finding 2)
// ---------------------------------------------------------------------------
//
// Three times now a "binding" observation has been picked by hand and written
// into prose, and three times the label decayed against the table sitting a few
// lines from it (round 2: `MEASURED_QUIET_T_ACK = 3ms` vs a recorded 1.4ms; round
// 6: a `t_boot` anchor of 11.4ms that was itself permissive; round 10: "the
// fastest loaded observation is 81ms" while the same table records 13ms, 45ms and
// 76ms). The class is closed by DERIVING the binding values instead: the table is
// encoded here once, and every bound the baseline must respect is computed from
// it. A new measurement is added by editing this table and nothing else.

/// One measurement series: its recorded QUIET range and one recorded range per
/// contention level. Microseconds, so 11.4ms is exact in integer arithmetic.
struct Series {
    what: &'static str,
    /// `(fastest, slowest)` recorded on a quiet host.
    quiet: (u64, u64),
    /// `(level, (fastest, slowest))`, one entry per recorded contention level.
    loaded: &'static [(&'static str, (u64, u64))],
}

impl Series {
    /// The recorded FLOOR at `level` — the least favourable value for activation,
    /// because a case must scale even when its measurement lands at the fast end
    /// of what was recorded. `None` if this series was not recorded at `level`.
    fn floor_at(&self, level: &str) -> Option<u64> {
        self.loaded
            .iter()
            .find(|(name, _)| *name == level)
            .map(|(_, (fastest, _))| *fastest)
    }
}

const T_BOOT: Series = Series {
    what: "t_boot (spawn -> banner)",
    quiet: (11_400, 29_000),
    loaded: &[
        ("load avg 30", (45_000, 66_000)),
        ("load avg 116", (81_000, 132_000)),
    ],
};

const T_ACK_SIGINT: Series = Series {
    what: "t_ack, SIGINT test",
    quiet: (1_400, 3_000),
    loaded: &[
        ("load avg 30", (13_000, 13_000)),
        ("load avg 116", (76_000, 76_000)),
    ],
};

const T_ACK_SIBLING: Series = Series {
    what: "t_ack, sibling (slowest of 5)",
    quiet: (38_000, 43_000),
    loaded: &[
        ("load avg 30", (97_000, 97_000)),
        ("load avg 116", (133_000, 133_000)),
    ],
};

/// An INTENDED CONTENTION CASE is one TEST RUN at one recorded load level — the
/// unit the mechanism actually operates on, because `calibrate` takes the LARGEST
/// scale over every measurement a run makes. So a run's binding observation is the
/// MAXIMUM of its series' recorded floors at that level, never any single cell.
struct RecordedRun {
    test: &'static str,
    series: &'static [Series],
}

const RECORDED_RUNS: &[RecordedRun] = &[
    RecordedRun {
        test: "sigint_in_writable_session_flushes_before_exit",
        series: &[T_BOOT, T_ACK_SIGINT],
    },
    RecordedRun {
        test: "writable_session_auto_flushes_mid_session_across_threshold",
        series: &[T_BOOT, T_ACK_SIBLING],
    },
];

/// Every contention level the table records, in the order they were taken.
const RECORDED_LEVELS: &[&str] = &["load avg 30", "load avg 116"];

/// THE BASELINE MUST BE INERT ON A QUIET HOST AND ACTIVE FOR EVERY INTENDED
/// CONTENTION CASE, asserted against the RECORDED MEASUREMENTS rather than against
/// itself or against a hand-picked "binding" observation.
///
/// Two properties, both derived from `RECORDED_RUNS`:
///
/// * **Quiet-inert**, per recorded series: the SLOWEST value ever recorded on a
///   quiet host must still yield `scale == 1` exactly, or an unloaded host scales
///   and the calibration becomes a flake source of its own.
/// * **Contention-active**, per CASE (one run at one level): the case's binding
///   observation — the largest of its series' recorded FLOORS at that level, which
///   is what `calibrate`'s max-of-scales actually consumes — must engage scaling.
///   Asserted per case and named per case: a suite-wide "some case scaled" cannot
///   see one case silently staying inert behind its siblings, which is exactly the
///   defect this replaces.
///
/// A test that derives its synthetic observation FROM the baseline is invariant to
/// the baseline's value: inflating it 1000x — the defect that left the calibration
/// inert through every real run of the first version — leaves such a test GREEN. So
/// every value below comes from the recorded table, never from the constant under
/// examination.
#[test]
fn the_baseline_is_quiet_inert_and_contention_active() {
    // --- Quiet inertness, per recorded series -------------------------------
    let mut quiet_slowest = 0u64;
    let mut quiet_checked = 0usize;
    for run in RECORDED_RUNS {
        for series in run.series {
            let slowest = Duration::from_micros(series.quiet.1);
            quiet_slowest = quiet_slowest.max(series.quiet.1);
            let d = TestDeadline::start(T1_DEADLINE_BASE, T1_DEADLINE_CAP);
            d.calibrate(series.what, slowest);
            assert_eq!(
                d.span(),
                T1_DEADLINE_BASE,
                "{}: the SLOWEST recorded QUIET observation of `{}` ({slowest:.1?}) must leave \
                 the deadline at EXACTLY the base — otherwise an unloaded host scales and the \
                 calibration becomes a flake source of its own: {}",
                run.test,
                series.what,
                d.describe()
            );
            quiet_checked += 1;
        }
    }
    assert!(
        quiet_checked > 0,
        "the recorded table is empty: this test would assert nothing"
    );

    // --- Contention activation, per intended contention case ----------------
    let mut binding_case = None::<(&str, &str, u64)>;
    let mut cases_checked = 0usize;
    for run in RECORDED_RUNS {
        for level in RECORDED_LEVELS {
            // The binding observation for this case: the largest recorded FLOOR
            // across the run's series, because `calibrate` takes the largest scale
            // over everything the run measures.
            let mut binding = 0u64;
            for series in run.series {
                let floor = series.floor_at(level).unwrap_or_else(|| {
                    panic!(
                        "{}: series `{}` records nothing at {level:?} — the table must record \
                         every series at every level it claims, or a case is silently unchecked",
                        run.test, series.what
                    )
                });
                binding = binding.max(floor);
            }

            let observed = Duration::from_micros(binding);
            let d = TestDeadline::start(T1_DEADLINE_BASE, T1_DEADLINE_CAP);
            d.calibrate("recorded contention case", observed);
            assert!(
                d.span() > T1_DEADLINE_BASE,
                "CONTENTION CASE `{}` @ {level}: its binding observation {observed:.1?} (the \
                 largest recorded floor across that run's measurements) leaves the deadline \
                 UNSCALED against a {QUIET_OBSERVATION_BASELINE:?} baseline. The calibration is \
                 inert for this case, which is the original defect of this change: {}",
                run.test,
                d.describe()
            );

            if binding_case.is_none_or(|(_, _, b)| binding < b) {
                binding_case = Some((run.test, level, binding));
            }
            cases_checked += 1;
        }
    }
    let (binding_test, binding_level, contended_floor) =
        binding_case.expect("the recorded table declares no contention case");
    assert!(
        cases_checked >= RECORDED_RUNS.len(),
        "every recorded run must contribute at least one contention case, but only \
         {cases_checked} were checked across {} runs",
        RECORDED_RUNS.len()
    );

    // --- The admissible window, DERIVED and reported ------------------------
    //
    // Not a third property: it is the two above stated as the interval the table
    // leaves, so a failure names the whole window instead of one violated end.
    let quiet_bound = Duration::from_micros(quiet_slowest);
    let contended_bound = Duration::from_micros(contended_floor);
    assert!(
        quiet_bound < contended_bound,
        "the recorded table leaves NO admissible baseline: the slowest quiet observation \
         {quiet_bound:.1?} is not below the least-scaled contention case ({binding_test} @ \
         {binding_level}, {contended_bound:.1?}). No single baseline can be both quiet-inert and \
         contention-active for that data."
    );
    assert!(
        quiet_bound < QUIET_OBSERVATION_BASELINE && QUIET_OBSERVATION_BASELINE < contended_bound,
        "the baseline {QUIET_OBSERVATION_BASELINE:?} is outside the window the recorded table \
         leaves: it must sit strictly above the slowest recorded quiet observation \
         {quiet_bound:.1?} and strictly below the least-scaled contention case ({binding_test} @ \
         {binding_level}, {contended_bound:.1?}). Both ends are DERIVED from the table in this \
         file — do not relabel the constant, adjust it or the recorded data."
    );
}

/// A stage's waits share the ONE deadline, so none of them can double-spend it.
///
/// WHAT THIS ASSERTS, AND WHAT IT DOES NOT (roborev job 232 finding 1): it asserts
/// the TIMEOUT ARITHMETIC — work done inside a stage is charged, and a later wait
/// plus what is already spent is never GRANTED more than the span. It says nothing
/// about wall clock at the moment a verdict is returned: a wait that has already
/// observed its success returns that success even if the deadline lapsed while it
/// was looking, which is deliberate (`poll_with_progress` in `mod.rs` states why,
/// and bounds how late it can be). The deadline bounds waiting for evidence, not
/// the acceptance of evidence in hand — including evidence that arrived in time
/// and had not been consumed when the deadline passed, which every expiry site
/// checks for before declaring a timeout (job 233 finding 1), reading the store
/// its own failure message reports from (job 236 finding 1).
///
/// Under the pre-descope `derived: Duration` this was false at five sites (rounds
/// 2, 4, 6 and roborev job 224 findings 2 and 3): each wait received a stage's
/// full span fresh, and each call site was separately responsible for subtracting
/// what had already been spent. `Stage` now has nothing to hand out.
///
/// NOTE ON THE SLEEP: a `sleep` can only OVERSHOOT, and every assertion here is
/// in the direction overshoot makes MORE true (time was charged; the span did not
/// grow). This is the opposite of the #2642 flake class, which asserts that
/// something completed FAST.
#[test]
fn a_stages_waits_share_the_one_deadline_so_none_can_double_spend() {
    let deadline = TestDeadline::start(Duration::from_secs(2), Duration::from_secs(2));
    let stage = deadline.stage("only");
    let span = deadline.span();
    let first = stage.remaining();

    // Work inside the stage between two waits — a process spawn, in the real
    // `select_rows`, which is exactly what job 224 finding 2 reported going
    // uncharged.
    std::thread::sleep(Duration::from_millis(200));

    let second = stage.remaining();
    let charged = first.saturating_sub(second);
    assert!(
        charged >= Duration::from_millis(150),
        "work done inside a stage must be charged to the deadline: only {charged:?} of \
         {span:?} was charged across a 200ms gap"
    );
    assert!(
        second + charged <= span,
        "a later wait may never be GRANTED more than the one deadline less what has already \
         been spent: {second:?} + {charged:?} against {span:?}"
    );

    // The span is fixed unless something CALIBRATES it, so it cannot move under
    // the stage's feet.
    assert!(
        deadline.span() == span,
        "the deadline's span may not change without calibration: {:?} vs {span:?}",
        deadline.span()
    );
}
