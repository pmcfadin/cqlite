//! Test harness for `graceful_shutdown_tests.rs` (issues #1693, #3515).
//!
//! Split out of that file under the campsite rule (#1135): the staged oracle
//! #3515 required pushed the single file past the 1500-line test threshold. This
//! module holds the INSTRUMENT — child I/O with a shared transcript, calibrated
//! budgets, the stage/total budget clock, the progress-checked poll, and the
//! bounded read-side SELECT — plus the unit tests that pin the instrument's own
//! invariants (the floor invariant, the cap-sum arithmetic and the calibration
//! baselines). The two integration tests that USE it stay in
//! `graceful_shutdown_tests.rs`.
//!
//! Included as a module (not a test target) by exactly one consumer, so the
//! parent's `#![cfg(all(feature = "write-support", unix))]` gates it too.

use serde_json::Value as Json;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use wait_timeout::ChildExt;

// ---------------------------------------------------------------------------
// Product progress markers (see `cqlite-cli/src/main.rs`)
// ---------------------------------------------------------------------------
//
// ACCEPTED RESIDUAL (design.md D5): these couple the test to user-facing text.
// Match a SHORT STABLE SUBSTRING, and make every failure that awaits one NAME
// the substring it awaited and print the child transcript, so text drift is
// distinguishable at a glance from a real defect.

/// `run_writable_interactive`'s readiness banner — the interactive loop is up.
const MARKER_SESSION_READY: &str = "cqlite writable session: enter CQL DML";

/// The `ctrl_c` select branch's handler-entry marker. NOTE: the product text
/// contains an em-dash ("Received Ctrl-C — flushing memtable before exit...");
/// the substring below deliberately stops short of it so the assertion does not
/// depend on that character surviving a copy/paste.
///
/// The product `eprintln!` has a LEADING newline, so `BufRead::lines()` yields
/// TWO lines: an empty one, then one CONTAINING this substring. Measured, not
/// assumed — from a RED run's own transcript (`cat -A`):
///
/// ```text
///   [stderr] $
///   [stderr] Received Ctrl-C M-bM-^@M-^T flushing memtable before exit...$
/// ```
///
/// So the marker is never split across lines, and the empty line is harmless:
/// `wait_for` skips it (the predicate does not match) while the transcript keeps
/// it. The marker also cannot be consumed by an EARLIER stage's wait and
/// discarded, because `SIGINT` is only sent after stage (b) has already
/// returned — no other wait is in flight when the handler prints.
pub const MARKER_HANDLER_ENTERED: &str = "Received Ctrl-C";

// ---------------------------------------------------------------------------
// Stage budgets: the floor invariant, and the total-budget arithmetic
// ---------------------------------------------------------------------------
//
// THE FLOOR INVARIANT (#3515, round-3 blocker). This change replaces bare
// wall-clock deadlines with staged ones, and it MAY NEVER BE TIGHTER THAN THE
// BOUND IT REPLACED — otherwise it makes the reported flake fire SOONER, which
// is a regression wearing a fix's clothes. The first version of this change did
// exactly that: stage (d) had `base 25s` where the old code had a flat 60s, and
// the hung-flush RED run failed at exactly 25.0s, proving it (a silent flush
// produces no progress events, so the stall window is already satisfied and the
// effective bound IS `derived`).
//
// The invariant is BY COMPOSITION, not per stage: a single old bound was often
// split across several new stages, and each new stage can look innocent while
// the group is tighter. So it is stated as a mapping from each OLD bound to the
// GROUP of new stages that replaced it, and the group's BASES must sum to at
// least the old value:
//
//   sigint_in_writable_session_flushes_before_exit
//     OLD wait_for_line(OK, 60s)   [spawn + boot + read + execute + print]
//        -> (a) session-up 40 + (b) write-ack 25            = 65s >= 60s  OK
//     OLD wait_timeout(60s)        [handler entry + flush + exit]
//        -> (c) handler-entry 20 + (d) clean-exit 60        = 80s >= 60s  OK
//        and (d) ALONE is 60s >= 60s, because (d) is the stage #3515 flaked at
//     (e) durability-read 25       [NEW ceiling: `select_rows` was unbounded]
//
//   writable_session_auto_flushes_mid_session_across_threshold
//     OLD per-write wait_for_line(OK, 60s), write id=0  [includes boot]
//        -> (a) 40 + (b0) 25                               = 65s >= 60s  OK
//     OLD per-write wait_for_line(OK, 60s), writes id=1..4  [4 INDEPENDENT bounds]
//        -> (b1..4) 60s base EACH, aggregate bounded by the clock
//     OLD wait_for_sstable(60s)    -> (c) 60s               = 60s >= 60s  OK
//     OLD wait_timeout(60s) on EOF -> (d) 60s               = 60s >= 60s  OK
//     (e) durability-read 20       [NEW ceiling: `select_rows` was unbounded]
//
// THE PER-OPERATION vs AGGREGATE DISTINCTION (roborev job 219, finding 1). An
// earlier version of this file argued a "DECLARED EXCEPTION": the sibling's old
// bounds were SEVEN independent 60s deadlines = 420s nominal against a 240s HARD
// KILL, so they were never simultaneously realizable, and three of its stages
// were therefore floored well under 60s (writes id=1..4 at 10s, the sstable and
// EOF waits at 35s).
//
// That aggregate argument is TRUE AND IRRELEVANT PER OPERATION. Under the old
// code any SINGLE contended write could use the full 60s provided its siblings
// were fast; a 12s cap failed it with ~200s of the envelope unused. That is the
// round-3 blocker — a bound tighter than the one it replaced — relocated into the
// sibling, and the aggregate reasoning papered over it.
//
// THE SIBLING'S STAGES ARE THEREFORE NO LONGER NOMINALLY REDUCED. Each carries the
// full old bound as its base, and the ONLY reduction is the one
// `StageClock::clip` imposes on genuine aggregate exhaustion — the clock IS the
// group deadline, and it subtracts what has actually been consumed rather than
// what might be. A run that hits it fails with an attributed message naming the
// exhaustion (see `Budget::starved`), not with a 240s harness kill. That is a
// STRICTLY STRONGER claim than the exception it replaces, and unlike that wording
// it is true per operation and not merely in aggregate.
//
// The cost, stated because it is real: the sibling's nominal ceilings no longer
// sum under the total, so its guarantee is genuinely WEAKER than the SIGINT
// test's. The SIGINT test can promise "every stage can have its nominal budget";
// the sibling can promise only "every stage can have its nominal budget unless
// earlier stages actually consumed it, in which case the failure says so". See
// `the_nominal_cap_sums_stay_under_the_total_budget`, which asserts the first for
// test 1 and the second for the sibling rather than picking whichever passes.
//
// TOTAL-BUDGET ARITHMETIC (spec: "The test owns a total budget below the harness
// hard-kill"). `.config/nextest.toml` sets
// `slow-timeout = { period = "60s", terminate-after = 4 }` => a **240s hard
// kill**. Each test owns `TEST_TOTAL_BUDGET` and clips every stage budget to what
// REMAINS of it (`StageClock::clip`), so the stages can never sum past it however
// slow the host is. The nominal per-stage CAPS are additionally chosen so their
// worst-case sum is already under the total.
//
// NONE OF THIS ARITHMETIC IS LEFT TO A COMMENT. Every claim above — each group
// floor, each declared exception, and both cap sums — is asserted by the unit
// tests at the bottom of this file (`no_wait_is_tighter_than_the_bound_it_
// replaced`, `the_nominal_cap_sums_stay_under_the_total_budget`), so a future
// edit that tightens a stage reds the suite instead of silently reintroducing
// the round-3 blocker. A comment cannot fail; a test can.
//
// What the remaining ~10s of the 240s covers is only what CANNOT be a stage:
// `TempDir` teardown, and the bounded (5s) collection of the read-side child's
// already-at-EOF pipes after it has exited.
pub const TEST_TOTAL_BUDGET: Duration = Duration::from_secs(230);

/// The single wall-clock bound every wait in the pre-#3515 version of this file
/// used: `Duration::from_secs(60)`, seven times over. The floor invariant above
/// is stated against this value.
const OLD_BOUND: Duration = Duration::from_secs(60);

/// A stage's calibration inputs: `base` is the budget on a quiet host, `cap` the
/// ceiling no amount of measured contention may exceed.
#[derive(Clone, Copy, Debug)]
pub struct StageSpec {
    pub base: Duration,
    pub cap: Duration,
}

const fn spec(base_secs: u64, cap_secs: u64) -> StageSpec {
    StageSpec {
        base: Duration::from_secs(base_secs),
        cap: Duration::from_secs(cap_secs),
    }
}

// sigint_in_writable_session_flushes_before_exit
pub const T1_ACK: StageSpec = spec(25, 30);
pub const T1_HANDLER: StageSpec = spec(20, 30);
pub const T1_EXIT: StageSpec = spec(60, 85);
pub const T1_READ: StageSpec = spec(25, 35);

// writable_session_auto_flushes_mid_session_across_threshold
pub const T2_ACK_FIRST: StageSpec = spec(25, 28);
/// Writes id=1..4: each replaced an INDEPENDENT 60s wait, so each carries the
/// FULL old bound as its base. Their aggregate is bounded by `StageClock::clip`
/// (the group deadline), never by a small per-operation cap.
pub const T2_ACK_LATER: StageSpec = spec(60, 70);
pub const T2_SSTABLE: StageSpec = spec(60, 70);
pub const T2_EOF_EXIT: StageSpec = spec(60, 70);
pub const T2_READ: StageSpec = spec(20, 25);

/// The stall window for the progress-checked polls. Calibrated like any stage,
/// but it is not a stage: it never bounds the test on its own.
pub const STALL_WINDOW: StageSpec = spec(5, 20);

/// Stage (a). **The irreducible bound** (design.md, "The residual").
///
/// This one deadline is NOT calibrated, and cannot be: calibrating it would
/// require a measurement taken before it, whose own bound would need a
/// measurement before *that* — the regress terminates only by accepting one
/// bare wall-clock deadline. It is placed on the cheapest operation in the test
/// (process spawn + dynamic link + engine init, not a flush), and its expiry
/// message states exactly what the expiry means and nothing more. It is exempt
/// from the calibration requirement rather than silently non-compliant with it.
///
/// It is a NEW ceiling: the old code had no readiness wait at all (it wrote the
/// INSERT immediately after spawn), so this bound is floored against nothing —
/// but it is part of the group that replaces the old ack deadline, and the floor
/// invariant above is asserted on that group.
const SESSION_UP_DEADLINE: Duration = Duration::from_secs(40);

// ---------------------------------------------------------------------------
// Calibration baselines
// ---------------------------------------------------------------------------
//
// MEASURED quiet values for this test (warm build, unloaded 16-core box,
// `--test-threads=1`), and under self-generated CPU contention:
//
//                                    quiet        load avg 30    load avg 116
//   t_boot (spawn -> banner)         11.4-29ms    45-66ms        81-132ms
//   t_ack  (write -> `OK`), test 1   1.4-3ms      13ms           76ms
//   t_ack  (slowest of 5), test 2    38-43ms      97ms           133ms
//
// THE BASELINES SIT JUST ABOVE THE QUIET NOISE FLOOR, and that is deliberate:
// `scale = max(1, observed / quiet_baseline)`, so a baseline set far above the
// quiet measurement makes the whole mechanism INERT. Measured: with the first
// version's 500ms/200ms baselines, `scale` stayed at EXACTLY 1.000 in every run
// taken, including load average 116 (~7x oversubscription) — a mechanism with
// zero observed firings.
//
// The asymmetry that makes a small baseline safe: CALIBRATION CAN ONLY LOOSEN A
// BUDGET (`scale` is floored at 1 and `derived` is clamped at `base`). A
// spuriously large `scale` therefore cannot cause a failure — it can only delay
// one. There is no quiet-side risk to protect against, so over-eager engagement
// is harmless and under-eager engagement is the only real hazard.
//
// (design.md D2 asks for baselines "in seconds, not milliseconds" and the spec
// for "large enough that an unloaded host yields scale == 1". The second is
// honoured; the first is the one place this implementation deviates, because
// taken literally it makes the calibration inert on the very host #3515
// measured. Reported with the change.)

// THE ANCHORS ARE THE MEASUREMENTS THAT BIND: the SMALLEST relevant quiet value,
// not the largest. The anchor's job is to form an UPPER bound on the baseline (a
// baseline far above the noise floor makes the calibration inert), and in THAT
// direction the slowest observed value is the PERMISSIVE choice — anchoring
// `t_ack` to the sibling's 43ms would license a baseline up to 430ms, within a
// factor of two of the 500ms that was MEASURED to be inert.
//
// Observed quiet values across every run recorded for this change:
//   t_boot   11.4ms (smallest, BINDING) .. 29ms
//   t_ack     1.4ms (smallest, BINDING) ..  3ms (SIGINT test)
//                                       .. 43ms (sibling, slowest of 5 writes)
//
// If a future host is genuinely slower at the LOW end, update these with the new
// measurement rather than inflating the baselines away from them.
//
// CONSEQUENCE, stated because it is a real trade: anchoring `t_ack` low puts the
// SIBLING's quiet `t_ack` (~42ms) ABOVE the baseline, so that test scales by ~1.7
// even on an unloaded host — loosened when it did not need to be. Harmless by the
// asymmetry the whole mechanism rests on (calibration can only loosen, so an
// over-eager `scale` delays a failure but never causes one), and strictly
// preferable to an anchor that licenses an inert baseline. It does mean the
// "quiet host yields exactly `base`" property holds for the SIGINT test and not
// for the sibling; the property that matters — never TIGHTER than `base` — holds
// for both unconditionally.
const MEASURED_QUIET_T_BOOT: Duration = Duration::from_micros(11_400);

const MEASURED_QUIET_T_ACK: Duration = Duration::from_micros(1_400);

// THE BASELINES ARE DERIVED FROM THE ANCHORS, NOT WRITTEN ALONGSIDE THEM.
//
// This closes a class rather than an instance (roborev job 222, finding 2). Both
// baselines used to be independent literals whose doc comments HAND-STATED their
// multiple ("~6.8x", "~8.3x") — and a hand-written claim about arithmetic decays
// exactly like a stale comment. It did: `MEASURED_QUIET_T_ACK` was set to 3ms
// (the SIGINT test's typical quiet ack) while the recorded BINDING value four
// lines above it was 1.4ms, so `ACK_QUIET_BASELINE = 25ms` was ~18x the binding
// value while its comment claimed ~8.3x, and the `<= 10x` guard permitted 30ms
// where 10x the binding value is 14ms. The guard permitted what it claimed to
// forbid, and the prose was the reason nobody noticed.
//
// Expressing each baseline as `anchor * multiple` makes that disagreement
// UNREPRESENTABLE: there is no second number to drift. The multiples are the only
// tunable, they are bounded by `MAX_BASELINE_MULTIPLE`, and the guard's message
// prints the COMPUTED multiple so a reader never has to trust prose arithmetic.
//
// The anchors themselves are rounded DOWN (11.4ms -> 11_400us, 1.4ms -> 1_400us),
// the STRICT direction: they form an upper bound on the baselines, so rounding up
// would loosen the guard. That asymmetry is the one this issue got wrong twice.

/// How far above the binding measurement each baseline sits. Single-digit by
/// policy — see `MAX_BASELINE_MULTIPLE`.
const BOOT_BASELINE_MULTIPLE: u32 = 7;
const ACK_BASELINE_MULTIPLE: u32 = 8;

/// The most a baseline may exceed its binding measurement before the calibration
/// is effectively inert.
const MAX_BASELINE_MULTIPLE: u32 = 10;

/// Quiet-host reference for `t_boot`, DERIVED from the binding anchor.
pub const BOOT_QUIET_BASELINE: Duration =
    MEASURED_QUIET_T_BOOT.saturating_mul(BOOT_BASELINE_MULTIPLE);

/// Quiet-host reference for `t_ack`, DERIVED from the binding anchor.
pub const ACK_QUIET_BASELINE: Duration = MEASURED_QUIET_T_ACK.saturating_mul(ACK_BASELINE_MULTIPLE);

/// The `cqlite` binary this test crate built with `--features write-support`.
fn cqlite_bin() -> &'static str {
    env!("CARGO_BIN_EXE_cqlite")
}

/// Write the single-table schema used by the round-trip.
pub fn write_schema(dir: &Path) -> PathBuf {
    let path = dir.join("schema.cql");
    std::fs::write(
        &path,
        r#"
CREATE KEYSPACE IF NOT EXISTS test_write WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE test_write;

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name TEXT,
    age INT,
    active BOOLEAN
);
"#,
    )
    .expect("write schema file");
    path
}

// ---------------------------------------------------------------------------
// Child I/O: both pipes drained, every line kept in a shared transcript
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    fn tag(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
        }
    }
}

/// How a [`ChildIo::wait_for`] ended when it did NOT observe the line it awaited.
///
/// Reported by every such failure because it is an OBSERVATION, not a cause: a
/// budget that expired with the pipes still open and a child whose pipes reached
/// EOF are different measurements, and the second is the signature a RED run
/// produces when the child dies instead of handling the signal. Neither variant
/// names WHY.
#[derive(Debug)]
pub enum WaitEnd {
    /// The budget expired; the child's pipes were still open, so more output was
    /// still possible.
    BudgetExpired,
    /// Both reader threads ended and the queue drained: the child's stdout AND
    /// stderr reached EOF after this long, so no further line could ever arrive.
    /// The child had exited, crashed, or closed its pipes -- this does not say
    /// which.
    PipesClosed(Duration),
}

impl WaitEnd {
    pub fn describe(&self) -> String {
        match self {
            WaitEnd::BudgetExpired => "how the wait ended: the budget expired with the child's \
                 pipes still open (more output was still possible)"
                .to_string(),
            WaitEnd::PipesClosed(after) => format!(
                "how the wait ended: the child's stdout AND stderr both reached EOF after \
                 {after:.3?}, so no further line could arrive: the child had exited, crashed, or \
                 closed its pipes (this measurement does not say which)"
            ),
        }
    }
}

/// Drains BOTH of the child's pipes (design.md D7: `stderr` was piped and never
/// read — discarding the evidence this oracle needs, and a latent wedge for any
/// chattier session) and accumulates every line into a shared transcript, so a
/// failure can print what the child actually said. The previous `wait_for_line`
/// discarded every non-matching line, so a failure could report nothing.
pub struct ChildIo {
    rx: Receiver<(Stream, String)>,
    transcript: Arc<Mutex<Vec<String>>>,
}

fn spawn_reader<R: std::io::Read + Send + 'static>(
    stream: Stream,
    reader: R,
    tx: Sender<(Stream, String)>,
    transcript: Arc<Mutex<Vec<String>>>,
) {
    thread::spawn(move || {
        let buf = BufReader::new(reader);
        for line in buf.lines() {
            let Ok(line) = line else { break };
            if let Ok(mut t) = transcript.lock() {
                t.push(format!("[{}] {}", stream.tag(), line));
            }
            if tx.send((stream, line)).is_err() {
                break;
            }
        }
    });
}

impl ChildIo {
    /// Attach readers to a spawned child's stdout + stderr.
    fn attach(child: &mut std::process::Child) -> Self {
        let transcript: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::channel();
        let out = child.stdout.take().expect("child stdout");
        let err = child.stderr.take().expect("child stderr");
        spawn_reader(Stream::Stdout, out, tx.clone(), Arc::clone(&transcript));
        spawn_reader(Stream::Stderr, err, tx, Arc::clone(&transcript));
        Self { rx, transcript }
    }

    /// Block until a line on `want` satisfies `pred`, or `budget` elapses.
    /// Returns the matching line and how long the wait took (so a successful
    /// wait can calibrate a later stage).
    pub fn wait_for(
        &self,
        want: Stream,
        pred: impl Fn(&str) -> bool,
        budget: Duration,
    ) -> Result<(String, Duration), WaitEnd> {
        let started = Instant::now();
        let deadline = started + budget;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(WaitEnd::BudgetExpired);
            }
            match self
                .rx
                .recv_timeout(remaining.min(Duration::from_millis(100)))
            {
                Ok((stream, line)) => {
                    if stream == want && pred(&line) {
                        return Ok((line, started.elapsed()));
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                // Both readers ended: the child's pipes are closed and the
                // buffer is drained, so no further line can ever arrive.
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(WaitEnd::PipesClosed(started.elapsed()))
                }
            }
        }
    }

    /// Non-blocking: consume whatever the readers have queued. Returns how many
    /// lines were newly observed — the "a new line arrived" progress signal.
    fn drain_new(&self) -> usize {
        let mut n = 0;
        while self.rx.try_recv().is_ok() {
            n += 1;
        }
        n
    }

    /// Everything the child has said so far, indented for a panic message.
    pub fn transcript_text(&self) -> String {
        match self.transcript.lock() {
            Ok(t) if t.is_empty() => {
                "  (the child emitted nothing at all on stdout or stderr)".to_string()
            }
            Ok(t) => t
                .iter()
                .map(|l| format!("  {l}"))
                .collect::<Vec<_>>()
                .join("\n"),
            Err(_) => "  (transcript unavailable: a reader thread panicked)".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Calibrated budgets
// ---------------------------------------------------------------------------

/// A wait budget, carrying its own derivation so a failure can report it.
#[derive(Clone, Debug)]
pub struct Budget {
    /// What the wait is actually bounded by.
    pub derived: Duration,
    base: Duration,
    cap: Duration,
    scale: f64,
    /// The measurement `scale` was computed from (`Duration::ZERO` when bare).
    observed: Duration,
    /// Name of that measurement, e.g. `t_ack`, or `None` for a bare deadline.
    observed_name: Option<&'static str>,
    quiet_baseline: Duration,
    /// Set when `StageClock::clip` shortened `derived` to the test's remaining
    /// total budget — i.e. the total budget, not this stage, is the binding
    /// constraint.
    clipped_to_total: bool,
    /// Set by `StageClock::clip` when the remaining total budget is less than
    /// this stage's own `base` — i.e. earlier stages have eaten the headroom and
    /// this stage cannot even get its nominal budget. Reported prominently,
    /// because a stage clipped to near zero fails on its first poll, and that is
    /// otherwise indistinguishable from the property genuinely not holding.
    starved: bool,
}

/// `clamp(base * scale, base, cap)` with `scale = max(1, observed /
/// quiet_baseline)`.
///
/// `scale` is floored at 1 and `derived` is clamped at `base`, so calibration
/// can only ever LOOSEN a budget. A quiet host measures far below
/// `quiet_baseline`, yields `scale == 1`, and gets exactly `base` — calibration
/// can therefore never itself become a source of flakes on an unloaded box.
pub fn calibrated(
    stage: StageSpec,
    observed: Duration,
    observed_name: &'static str,
    quiet_baseline: Duration,
) -> Budget {
    let StageSpec { base, cap } = stage;
    debug_assert!(base <= cap, "base must not exceed cap");
    debug_assert!(!quiet_baseline.is_zero(), "quiet_baseline must be non-zero");
    let scale = (observed.as_secs_f64() / quiet_baseline.as_secs_f64()).max(1.0);
    let scaled = Duration::from_secs_f64(base.as_secs_f64() * scale);
    Budget {
        derived: scaled.clamp(base, cap),
        base,
        cap,
        scale,
        observed,
        observed_name: Some(observed_name),
        quiet_baseline,
        clipped_to_total: false,
        starved: false,
    }
}

/// An uncalibrated deadline — used ONLY for stage (a); see
/// [`SESSION_UP_DEADLINE`].
fn bare(deadline: Duration) -> Budget {
    Budget {
        derived: deadline,
        base: deadline,
        cap: deadline,
        scale: 1.0,
        observed: Duration::ZERO,
        observed_name: None,
        quiet_baseline: Duration::ZERO,
        clipped_to_total: false,
        starved: false,
    }
}

impl Budget {
    /// How this budget was arrived at — reported by every wait failure.
    pub fn describe(&self) -> String {
        let core = match self.observed_name {
            Some(name) => format!(
                "budget {:.2?} = clamp(base {:.2?} x scale {:.3}, base, cap {:.2?}), \
                 scale = max(1, {name} {:.3?} / quiet_baseline {:.2?})",
                self.derived, self.base, self.scale, self.cap, self.observed, self.quiet_baseline
            ),
            None => format!(
                "budget {:.2?} (BARE wall-clock deadline: no prior measurement exists to \
                 calibrate it — the irreducible bound, see design.md \"The residual\")",
                self.derived
            ),
        };
        if self.starved {
            return format!(
                "{core} [TOTAL BUDGET ALREADY EXHAUSTED BY EARLIER STAGES: this stage received \
                 {:.2?} of its {:.2?} base, so it cannot make its own guarantee. A failure here \
                 is about the budget, NOT about the property under test]",
                self.derived, self.base
            );
        }
        if self.clipped_to_total {
            format!(
                "{core} [CLIPPED to {:.2?} by the test's REMAINING TOTAL BUDGET — the total \
                 budget, not this stage, is the binding constraint]",
                self.derived
            )
        } else {
            core
        }
    }
}

/// Tracks a test's elapsed time across stages against its own total budget, so
/// the test always emits its own attributed failure rather than being killed by
/// nextest's 240s hard kill. See the TOTAL-BUDGET ARITHMETIC comment above.
pub struct StageClock {
    started: Instant,
    total: Duration,
    spent: Vec<(&'static str, Duration)>,
}

impl StageClock {
    pub fn new(total: Duration) -> Self {
        Self {
            started: Instant::now(),
            total,
            spent: Vec::new(),
        }
    }

    pub fn remaining(&self) -> Duration {
        self.total.saturating_sub(self.started.elapsed())
    }

    /// Shorten a stage budget to what remains of the total budget.
    ///
    /// THIS IS THE GROUP DEADLINE. Stages that replaced several INDEPENDENT old
    /// bounds each carry the FULL old bound as their base, and the aggregate is
    /// bounded here — by subtracting what has actually been consumed. So a single
    /// contended operation can still reach the full old ceiling when its siblings
    /// ran fast, and a reduction applies only on genuine aggregate exhaustion,
    /// never unconditionally through a small per-operation cap. (An earlier
    /// version of this change added a separate `GroupBudget` type for this; it was
    /// a second mechanism for a job this clock already does.)
    pub fn clip(&self, mut budget: Budget) -> Budget {
        let remaining = self.remaining();
        if budget.derived > remaining {
            budget.derived = remaining;
            budget.clipped_to_total = true;
            // Weaker than "clipped": this stage cannot even reach its own base.
            budget.starved = remaining < budget.base;
        }
        budget
    }

    pub fn record(&mut self, stage: &'static str, took: Duration) {
        self.spent.push((stage, took));
    }

    /// Per-stage timings + total-budget state, for both diagnostics and the
    /// end-of-test record printed with `--nocapture`.
    pub fn report(&self) -> String {
        let stages = if self.spent.is_empty() {
            "(none completed)".to_string()
        } else {
            self.spent
                .iter()
                .map(|(name, took)| format!("{name} {took:.3?}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let worst = self
            .spent
            .iter()
            .max_by_key(|(_, took)| *took)
            .map(|(name, took)| format!("; slowest completed stage: {name} {took:.3?}"))
            .unwrap_or_default();
        format!(
            "stage timings: {stages}{worst}\ntotal budget {:.1?}: elapsed {:.2?}, remaining {:.2?}",
            self.total,
            self.started.elapsed(),
            self.remaining()
        )
    }
}

// ---------------------------------------------------------------------------
// Progress-checked polling
// ---------------------------------------------------------------------------

/// Why a progress-checked poll gave up.
#[derive(Debug)]
pub enum PollGaveUp {
    /// The budget expired AND nothing had happened for the whole stall window.
    Stalled,
    /// Progress kept arriving, but the test's own total budget ran out.
    TotalBudgetExhausted,
}

#[derive(Debug)]
pub struct PollFail {
    why: PollGaveUp,
    elapsed: Duration,
    stall: Duration,
    stall_window: Duration,
    new_lines: usize,
    new_artifacts: usize,
}

impl PollFail {
    /// What the poll observed — never why it happened.
    ///
    /// `TotalBudgetExhausted` used to read "progress was still arriving, but the
    /// test's own TOTAL budget ran out". That asserted a cause the branch cannot
    /// establish: it is reached whenever the envelope expires BEFORE the stall
    /// window elapses, and `last_progress` is initialised at poll entry — so with
    /// a short remaining envelope it can fire having observed ZERO new lines and
    /// ZERO new artifacts. Inside the change whose whole purpose is removing
    /// messages that assert unestablishable causes, that was a defect of the same
    /// class (roborev job 219, finding 3). It now reports only the ordering it
    /// actually measured, and the observed counts speak for themselves.
    pub fn observed(&self) -> String {
        let progress_seen = self.new_lines + self.new_artifacts;
        let why = match self.why {
            PollGaveUp::Stalled => format!(
                "the budget expired and NOTHING was observed for the whole stall window \
                 ({:.2?}, itself calibrated)",
                self.stall_window
            ),
            PollGaveUp::TotalBudgetExhausted => format!(
                "the test's own TOTAL budget ran out BEFORE the stall window ({:.2?}) elapsed, so \
                 this is NOT a stall verdict — it establishes only that ordering. Whether the \
                 child was making progress is reported by the counts below and by nothing else \
                 (this branch does not require any progress to have been observed)",
                self.stall_window
            ),
        };
        let counts = if progress_seen == 0 {
            format!(
                "progress observed while polling: NONE — 0 new output lines and 0 new durable \
                 artifacts in {:.2?}",
                self.elapsed
            )
        } else {
            format!(
                "progress observed while polling: {} new output line(s), {} new durable \
                 artifact(s); last progress was {:.2?} ago",
                self.new_lines, self.new_artifacts, self.stall
            )
        };
        format!("gave up after {:.2?}: {why}\n{counts}", self.elapsed)
    }
}

/// Poll `step` in short slices, treating a new child output line OR a new
/// durable `-Data.db` artifact as progress that resets the stall window.
///
/// This is the AC1 "unbounded-but-progress-checked loop" inside a bounded
/// envelope (design.md D6): a literally unbounded loop under nextest produces a
/// harness KILL, which is a strictly worse message than the one #3515 removed.
/// So the loop gives up only when
///   * `budget.derived` has expired AND nothing has happened for `stall_window`
///     (a genuine stall), or
///   * `envelope` — what remains of the test's own total budget — is exhausted.
pub fn poll_with_progress<T>(
    io: &ChildIo,
    data_dir: &Path,
    budget: &Budget,
    stall_window: Duration,
    envelope: Duration,
    mut step: impl FnMut(Duration) -> Option<T>,
) -> Result<(T, Duration), PollFail> {
    const SLICE: Duration = Duration::from_millis(100);
    let started = Instant::now();
    let mut last_progress = Instant::now();
    let mut artifacts = count_data_db(data_dir);
    let mut new_lines = 0usize;
    let mut new_artifacts = 0usize;

    loop {
        if let Some(done) = step(SLICE) {
            return Ok((done, started.elapsed()));
        }
        let lines = io.drain_new();
        if lines > 0 {
            new_lines += lines;
            last_progress = Instant::now();
        }
        let now_artifacts = count_data_db(data_dir);
        if now_artifacts > artifacts {
            new_artifacts += now_artifacts - artifacts;
            artifacts = now_artifacts;
            last_progress = Instant::now();
        }

        let elapsed = started.elapsed();
        let stall = last_progress.elapsed();
        let why = if elapsed >= budget.derived && stall >= stall_window {
            Some(PollGaveUp::Stalled)
        } else if elapsed >= envelope {
            Some(PollGaveUp::TotalBudgetExhausted)
        } else {
            None
        };
        if let Some(why) = why {
            return Err(PollFail {
                why,
                elapsed,
                stall,
                stall_window,
                new_lines,
                new_artifacts,
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Durable-artifact and read-side helpers
// ---------------------------------------------------------------------------

/// Count Data.db SSTable components under a write engine's data directory.
pub fn count_data_db(data_dir: &Path) -> usize {
    fn walk(dir: &Path, acc: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, acc);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
            {
                *acc += 1;
            }
        }
    }
    let mut n = 0;
    walk(data_dir, &mut n);
    n
}

/// Read a piped handle to EOF on its own thread, so a bounded wait on the child
/// can never deadlock against a full pipe buffer.
fn collect_to_end<R: std::io::Read + Send + 'static>(
    stream: Stream,
    mut reader: R,
    tx: Sender<(Stream, Vec<u8>)>,
) {
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = reader.read_to_end(&mut buf);
        let _ = tx.send((stream, buf));
    });
}

/// Stage (e): reopen an SSTable directory read-only and SELECT, returning the
/// rows as JSON and how long the read took.
///
/// BOUNDED and ATTRIBUTED, for the reason in the TOTAL-BUDGET ARITHMETIC comment
/// above: `Command::output()` has no timeout, so the previous version of this
/// helper was an unbounded wait on a child process, outside the test's budget,
/// on the one host class this issue is about.
pub fn select_rows(
    data_dir: &Path,
    schema: &Path,
    query: &str,
    budget: &Budget,
    clock: &StageClock,
) -> (Vec<Json>, Duration) {
    let started = Instant::now();
    let mut child = Command::new(cqlite_bin())
        .args([
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--schema",
            schema.to_str().unwrap(),
            "--execute",
            query,
            "--out",
            "json",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn read-side cqlite");
    let (tx, rx) = mpsc::channel();
    collect_to_end(
        Stream::Stdout,
        child.stdout.take().expect("read-side stdout"),
        tx.clone(),
    );
    collect_to_end(
        Stream::Stderr,
        child.stderr.take().expect("read-side stderr"),
        tx,
    );

    let status = match child
        .wait_timeout(budget.derived)
        .expect("wait_timeout on read-side cqlite")
    {
        Some(status) => status,
        None => {
            let _ = child.kill();
            panic!(
                "stage (e) durability-read: the read-side `cqlite --execute` child did not exit.\n\
                 query: `{query}`\n\
                 data dir: {}\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: only that the independent read-only reopen did not finish \
                 within the budget. It says NOTHING about whether the row is durable, and nothing \
                 about the write side, which had already exited cleanly. This wait is inside the \
                 test\'s own total budget precisely so that THIS message appears instead of the \
                 harness\'s 240s hard kill.\n{}",
                data_dir.display(),
                budget.describe(),
                clock.report()
            );
        }
    };
    // Named so the collection's failure message can say how much of stage (e)'s
    // budget the child wait had already spent. NOT the value returned: the stage's
    // recorded duration must INCLUDE the collection (finding 1), so it is measured
    // after the loop below.
    let child_wait = started.elapsed();

    // The child has exited, so both pipes are at EOF and the reader threads
    // finish promptly — but "promptly" is a claim about SCHEDULING, and a reader
    // thread on a saturated host can stay descheduled for seconds. This used to
    // be a hardcoded `recv_timeout(5s)`: a NEW, uncalibrated wall-clock bound
    // that could false-fail the harness under exactly the contention #3515 is
    // about (roborev job 219, finding 2). The collection is part of stage (e), so
    // it is bounded by stage (e)'s remaining allowance and, beyond that, by the
    // test's own remaining TOTAL budget — no fixed constant, and still incapable
    // of reaching nextest's hard kill.
    // STAGE (e) MAY NOT SPEND ITS BUDGET TWICE (roborev job 222, finding 1). This
    // used to hand the collection a FRESH `budget.derived` after the child wait
    // had already spent part of that same stage budget, so stage (e) could consume
    // up to 2x its cap. The envelope survived (`clock.remaining()` is real-time,
    // so `clip` still bounded later stages), but the CAP-SUM INVARIANT did not:
    // `the_nominal_cap_sums_stay_under_the_total_budget` computed a worst case the
    // code could exceed, making the assert not a bound on the thing it names — a
    // guard measuring a proxy, in a change whose whole value is that its guards
    // mean what they say.
    //
    // The collection therefore gets what is LEFT of stage (e)'s budget, further
    // clipped to the test's remaining total.
    let collect_allowance = budget
        .derived
        .saturating_sub(started.elapsed())
        .min(clock.remaining());
    let collect_deadline = Instant::now() + collect_allowance;
    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    let mut collected = 0;
    while collected < 2 {
        let left = collect_deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            panic!(
                "stage (e) durability-read: the read-side child exited ({status:?}) but only \
                 {collected}/2 of its output streams could be collected within what remained of \
                 stage (e)'s budget ({collect_allowance:.2?}; the child wait had already spent \
                 {child_wait:.2?} of it).\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: only that a reader thread had not delivered its buffer \
                 in time. It says nothing about durability, and nothing about the child, which \
                 exited successfully.\n{}",
                budget.describe(),
                clock.report()
            );
        }
        match rx.recv_timeout(left.min(Duration::from_millis(250))) {
            Ok((Stream::Stdout, buf)) => {
                stdout_buf = buf;
                collected += 1;
            }
            Ok((Stream::Stderr, buf)) => {
                stderr_buf = buf;
                collected += 1;
            }
            // Not a timeout: both reader threads dropped their senders without
            // sending, which can only be a panic inside the harness.
            Err(RecvTimeoutError::Disconnected) => panic!(
                "stage (e) durability-read: the read-side output channel disconnected with only \
                 {collected}/2 streams collected — a reader thread ended without sending. This \
                 is a defect in this test harness, not a statement about durability.\n{}",
                clock.report()
            ),
            Err(RecvTimeoutError::Timeout) => {}
        }
    }

    // Stage (e)'s elapsed time INCLUDES the collection, so the reported timing and
    // the cap-sum invariant describe the same quantity.
    let took = started.elapsed();

    let stdout = String::from_utf8_lossy(&stdout_buf);
    let stderr = String::from_utf8_lossy(&stderr_buf);
    assert!(
        status.success(),
        "SELECT failed: `{query}`\nstdout: {stdout}\nstderr: {stderr}"
    );
    let rows = match serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("SELECT did not emit JSON: {e}\nstdout: {stdout}"))
    {
        Json::Array(rows) => rows,
        other => panic!("expected a JSON array of rows, got: {other}"),
    };
    (rows, took)
}

/// Spawn the CLI in interactive `--writable` mode with both pipes drained, and
/// run stage (a): wait for the child's own readiness banner.
///
/// Returns the child, its I/O, and `t_boot` (spawn -> banner), which calibrates
/// the write-acknowledgement budget.
pub fn start_writable_session(
    wd: &Path,
    schema: &Path,
    env: &[(&str, &str)],
    clock: &mut StageClock,
) -> (std::process::Child, ChildIo, Duration) {
    let mut cmd = Command::new(cqlite_bin());
    cmd.args([
        "--writable",
        "--write-dir",
        wd.to_str().unwrap(),
        "--schema",
        schema.to_str().unwrap(),
    ]);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let spawned = Instant::now();
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cqlite interactive writable session");
    let io = ChildIo::attach(&mut child);

    // Stage (a) — THE IRREDUCIBLE BOUND. See `SESSION_UP_DEADLINE`.
    let budget = clock.clip(bare(SESSION_UP_DEADLINE));
    let ready = io.wait_for(
        Stream::Stderr,
        |l| l.contains(MARKER_SESSION_READY),
        budget.derived,
    );
    if let Err(end) = &ready {
        let _ = child.kill();
        panic!(
            "stage (a) session-up: the readiness banner was not observed on the child's stderr.\n\
             awaited substring on stderr: {MARKER_SESSION_READY:?}\n\
             {}\n\
             {}\n\
             WHAT THIS ESTABLISHES: only that the banner was not observed within that deadline on \
             THIS host. It does NOT distinguish a child that never reached the interactive loop \
             from one that was never scheduled, nor either of those from drift in the product's \
             banner text.\n\
             child transcript:\n{}\n{}",
            budget.describe(),
            end.describe(),
            io.transcript_text(),
            clock.report()
        );
    }
    // `t_boot` spans the whole spawn -> banner path (fork/exec + dynamic link +
    // engine init), not just the wait, so it is measured from the spawn call.
    let t_boot = spawned.elapsed();
    clock.record("a.session-up", t_boot);
    (child, io, t_boot)
}

/// Wait for a write acknowledgement (`OK` on stdout), calibrated from a prior
/// in-band measurement. Returns how long the round-trip took.
///
/// Shared by both tests. The failure reports what it awaited, how the budget was
/// derived, and what the child actually said. It does NOT conclude that the
/// session dead-ended, nor that no interactive writable session exists (the two
/// causes the retired messages named), neither of which a timeout establishes.
pub fn await_write_ack(
    io: &ChildIo,
    stage: &'static str,
    what: &str,
    budget: &Budget,
    clock: &StageClock,
) -> Duration {
    match io.wait_for(Stream::Stdout, |l| l.trim() == "OK", budget.derived) {
        Ok((_, took)) => took,
        Err(end) => panic!(
            "{stage}: {what} was not acknowledged with `OK` on the child's stdout.\n\
             awaited on stdout: a line whose trimmed text is exactly \"OK\"\n\
             {}\n\
             {}\n\
             WHAT THIS ESTABLISHES: only that no acknowledgement was observed within that budget. \
             It does NOT establish whether the write was rejected, is still in progress, was \
             never read, or whether the child was descheduled — inspect the transcript below \
             (the child prints `Error: ...` on stderr for a rejected statement).\n\
             child transcript:\n{}\n{}",
            budget.describe(),
            end.describe(),
            io.transcript_text(),
            clock.report()
        ),
    }
}
// ---------------------------------------------------------------------------
// Unit coverage: the floor invariant, the total-budget arithmetic, and the
// calibration helper (tasks.md 1.3)
//
// These exist because THE ROUND-3 BLOCKER WAS A COMMENT THAT COULD NOT FAIL.
// The budget arithmetic was written in prose above the constants, was wrong
// (stage (d) 25s replacing a 60s bound), and nothing noticed until a RED run's
// timing was read by hand. Every claim in that comment is now asserted here.
// ---------------------------------------------------------------------------

/// THE FLOOR INVARIANT: this change may never be tighter than the bound it
/// replaced, for any GROUP of stages that replaced one old bound.
#[test]
fn no_wait_is_tighter_than_the_bound_it_replaced() {
    // --- sigint_in_writable_session_flushes_before_exit ---
    //
    // OLD: a single `wait_for_line(OK, 60s)` issued immediately after spawn, so
    // it covered child boot + engine init + read + execute + print. There was NO
    // readiness wait, so stage (a) is a new bound INSIDE this old one and the
    // floor applies to the group, not to either stage alone.
    assert!(
        SESSION_UP_DEADLINE + T1_ACK.base >= OLD_BOUND,
        "stages (a)+(b) replace one {OLD_BOUND:?} ack deadline but sum to only {:?}",
        SESSION_UP_DEADLINE + T1_ACK.base
    );
    // OLD: `wait_timeout(60s)` after SIGINT, covering handler entry + flush + exit.
    assert!(
        T1_HANDLER.base + T1_EXIT.base >= OLD_BOUND,
        "stages (c)+(d) replace one {OLD_BOUND:?} post-SIGINT deadline but sum to only {:?}",
        T1_HANDLER.base + T1_EXIT.base
    );
    // And stage (d) ALONE, because it is the stage #3515 actually flaked at: a
    // silent flush produces no progress events, so the stall window is already
    // satisfied and the effective bound is exactly `derived`. If this drops
    // below the old bound, the "fix" makes the reported flake fire SOONER.
    assert!(
        T1_EXIT.base >= OLD_BOUND,
        "stage (d) is the stage #3515 flaked at and a silent flush makes its \
         effective bound exactly `base` — {:?} would fire SOONER than the {OLD_BOUND:?} \
         it replaces",
        T1_EXIT.base
    );

    // --- writable_session_auto_flushes_mid_session_across_threshold ---
    //
    // OLD: the id=0 write's 60s ack deadline also covered boot.
    assert!(
        SESSION_UP_DEADLINE + T2_ACK_FIRST.base >= OLD_BOUND,
        "stages (a)+(b0) replace one {OLD_BOUND:?} deadline but sum to only {:?}",
        SESSION_UP_DEADLINE + T2_ACK_FIRST.base
    );
    // The four LATER writes, the sstable wait and the EOF exit each replaced an
    // INDEPENDENT 60s wait, so each carries the FULL old bound as its base and the
    // AGGREGATE is bounded by the clock (roborev job 219, finding 1: the aggregate
    // argument that justified `spec(10, 12)` was true in aggregate and irrelevant
    // per operation).
    //
    // The checkable form of "no wait is tighter than the bound it replaced" under
    // a group deadline: WITH A FRESH CLOCK, each of these stages' DERIVED ceiling
    // reaches the old bound. Asserting the spec constants alone would miss a clip
    // that silently reduced them.
    let fresh = StageClock::new(TEST_TOTAL_BUDGET);
    for (name, stage) in [
        ("(b1..4) per-write ack", T2_ACK_LATER),
        ("(c) mid-session flush", T2_SSTABLE),
        ("(d) EOF exit", T2_EOF_EXIT),
    ] {
        let budget = fresh.clip(calibrated(
            stage,
            Duration::ZERO,
            "t_ack",
            ACK_QUIET_BASELINE,
        ));
        assert!(
            budget.derived >= OLD_BOUND,
            "sibling stage {name} derives {:?} from a FRESH clock, tighter than the \
             {OLD_BOUND:?} it replaced",
            budget.derived
        );
        assert!(
            !budget.starved && !budget.clipped_to_total,
            "nothing has been consumed yet, so {name} may not be reduced at all: {budget:?}"
        );
    }

    // Stage (e) is floored against nothing: `select_rows` was an UNBOUNDED
    // `Command::output()` before, so this is a new ceiling. It must still be
    // generous on its own terms, since a bound that can fail replaces a wait
    // that never could.
    for (name, base) in [("test 1", T1_READ.base), ("test 2", T2_READ.base)] {
        assert!(
            base >= Duration::from_secs(20),
            "stage (e) in {name} replaces an unbounded wait with {base:?}, which is not \
             generous enough for a new ceiling"
        );
    }
}

/// THE TOTAL-BUDGET ARITHMETIC: per-stage caps may not sum past the total, or a
/// late stage gets squeezed by `StageClock::clip` and fails for a reason that is
/// about the budget rather than the product.
#[test]
fn the_nominal_cap_sums_stay_under_the_total_budget() {
    // nextest: slow-timeout period 60s x terminate-after 4.
    const NEXTEST_HARD_KILL: Duration = Duration::from_secs(240);
    assert!(
        TEST_TOTAL_BUDGET < NEXTEST_HARD_KILL,
        "the test's own budget {TEST_TOTAL_BUDGET:?} must leave room to emit its own \
         failure before nextest's {NEXTEST_HARD_KILL:?} hard kill"
    );

    let t1 = SESSION_UP_DEADLINE + T1_ACK.cap + T1_HANDLER.cap + T1_EXIT.cap + T1_READ.cap;
    assert!(
        t1 <= TEST_TOTAL_BUDGET,
        "sigint test caps sum to {t1:?}, over the {TEST_TOTAL_BUDGET:?} total"
    );

    // THE SIBLING'S GUARANTEE IS GENUINELY WEAKER, AND THIS ASSERTS THE WEAKER ONE
    // RATHER THAN WHICHEVER PASSES. Its stages now each carry the full old 60s
    // bound (see the floor invariant), so their nominal sum CANNOT fit the total —
    // that is a consequence of the old test having had seven 60s bounds against a
    // 240s kill, not a defect. What holds instead is that the clock enforces the
    // total regardless of nominal caps, and that a stage which loses out says so.
    let sibling_nominal = SESSION_UP_DEADLINE
        + T2_ACK_FIRST.cap
        + T2_ACK_LATER.cap * 4
        + T2_SSTABLE.cap
        + T2_EOF_EXIT.cap
        + T2_READ.cap;
    assert!(
        sibling_nominal > TEST_TOTAL_BUDGET,
        "the sibling's nominal ceilings now FIT the {TEST_TOTAL_BUDGET:?} envelope \
         ({sibling_nominal:?}) — it can therefore make the SAME guarantee as the SIGINT test, so \
         promote it to the plain nominal-sum assert above instead of the weaker property below"
    );

    // The weaker property, in three parts. (1) `clip` never returns more than the
    // remaining total, for any spec — this is what bounds the aggregate.
    let nearly_spent = StageClock::new(Duration::from_millis(500));
    for (name, stage) in [
        ("T2_ACK_LATER", T2_ACK_LATER),
        ("T2_SSTABLE", T2_SSTABLE),
        ("T2_EOF_EXIT", T2_EOF_EXIT),
        ("T2_READ", T2_READ),
    ] {
        let budget = nearly_spent.clip(calibrated(
            stage,
            Duration::ZERO,
            "t_ack",
            ACK_QUIET_BASELINE,
        ));
        assert!(
            budget.derived <= Duration::from_millis(500),
            "{name} drew {:?} from a clock with 500ms left",
            budget.derived
        );
        // (2) A stage that cannot even reach its own base is marked STARVED...
        assert!(
            budget.starved,
            "{name} could not reach its {:?} base and must be marked starved: {budget:?}",
            stage.base
        );
        // (3) ...and says so, so a first-poll failure is distinguishable from the
        // property under test genuinely not holding.
        let described = budget.describe();
        assert!(
            described.contains("TOTAL BUDGET ALREADY EXHAUSTED BY EARLIER STAGES"),
            "{name} must name the exhaustion: {described}"
        );
    }

    // Every spec must be internally coherent.
    for (name, spec) in [
        ("T1_ACK", T1_ACK),
        ("T1_HANDLER", T1_HANDLER),
        ("T1_EXIT", T1_EXIT),
        ("T1_READ", T1_READ),
        ("T2_ACK_FIRST", T2_ACK_FIRST),
        ("T2_ACK_LATER", T2_ACK_LATER),
        ("T2_SSTABLE", T2_SSTABLE),
        ("T2_EOF_EXIT", T2_EOF_EXIT),
        ("T2_READ", T2_READ),
        ("STALL_WINDOW", STALL_WINDOW),
    ] {
        assert!(
            spec.base <= spec.cap,
            "{name}: base {:?} exceeds cap {:?}",
            spec.base,
            spec.cap
        );
    }
}

/// THE BASELINES MUST SIT JUST ABOVE THE MEASURED QUIET NOISE FLOOR, asserted
/// against the MEASUREMENTS rather than against themselves.
///
/// This test exists because the first version of
/// `calibration_engages_on_a_contended_observation` derived its synthetic
/// observation FROM the baseline (`ACK_QUIET_BASELINE * 8`), which makes it
/// invariant to the baseline's value: inflating `ACK_QUIET_BASELINE` 1000x — the
/// exact defect that left the calibration inert through every real run — left it
/// GREEN. A test whose input is scaled by the constant under examination cannot
/// detect a wrong value for that constant.
#[test]
fn the_baselines_sit_just_above_the_measured_quiet_noise_floor() {
    // The multiple a reader sees is COMPUTED here, never hand-written in a doc
    // comment: a prose claim about arithmetic decays exactly like a stale comment,
    // and in this file it did (roborev job 222, finding 2 — a comment claiming
    // "~8.3x" over a constant that was really ~18x the binding value).
    fn multiple_of(baseline: Duration, anchor: Duration) -> f64 {
        baseline.as_secs_f64() / anchor.as_secs_f64()
    }

    for (name, baseline, anchor, anchor_name, declared) in [
        (
            "BOOT_QUIET_BASELINE",
            BOOT_QUIET_BASELINE,
            MEASURED_QUIET_T_BOOT,
            "MEASURED_QUIET_T_BOOT",
            BOOT_BASELINE_MULTIPLE,
        ),
        (
            "ACK_QUIET_BASELINE",
            ACK_QUIET_BASELINE,
            MEASURED_QUIET_T_ACK,
            "MEASURED_QUIET_T_ACK",
            ACK_BASELINE_MULTIPLE,
        ),
    ] {
        let computed = multiple_of(baseline, anchor);

        // At or above the BINDING (smallest) measurement, so the fastest observed
        // quiet host still yields `scale == 1`.
        assert!(
            baseline >= anchor,
            "{name} {baseline:?} is below the BINDING measured quiet value \
             {anchor_name} {anchor:?} (computed multiple {computed:.2}x), so even the fastest \
             observed quiet host would scale"
        );

        // ...and not far above it, or the mechanism is INERT: `scale` is
        // `observed / quiet_baseline`, so a baseline many times the noise floor
        // never moves under real contention (measured: `scale` stayed at exactly
        // 1.000 at load average 116). Calibration can only LOOSEN, so there is no
        // quiet-side risk to trade against this.
        assert!(
            computed <= f64::from(MAX_BASELINE_MULTIPLE),
            "{name} {baseline:?} is {computed:.2}x its binding anchor {anchor_name} {anchor:?}, \
             over the {MAX_BASELINE_MULTIPLE}x limit: the calibration would be inert"
        );

        // The baseline is DERIVED as `anchor * declared`, so the computed multiple
        // must equal the declared one. If this ever fails, someone reintroduced an
        // independent literal — which is exactly the drift this factoring removes.
        assert!(
            (computed - f64::from(declared)).abs() < 1e-9,
            "{name} computes to {computed:.4}x its anchor but declares {declared}x — the baseline \
             is no longer derived from {anchor_name}"
        );
        assert!(
            declared <= MAX_BASELINE_MULTIPLE,
            "{name}'s declared multiple {declared}x exceeds the {MAX_BASELINE_MULTIPLE}x limit"
        );
    }

    // The consequence, asserted from the MEASUREMENT rather than the baseline: a
    // host 10x slower than the binding quiet floor must actually move the budget.
    // This is the assertion a baseline-relative test cannot make.
    let realistic = calibrated(
        T1_EXIT,
        MEASURED_QUIET_T_ACK * 10,
        "t_ack",
        ACK_QUIET_BASELINE,
    );
    assert!(
        realistic.scale > 1.0 && realistic.derived > T1_EXIT.base,
        "a host 10x slower than the binding measured quiet floor must loosen stage (d): {realistic:?}"
    );
}

/// THE CALIBRATION FORMULA ENGAGES: growth, proportionality, cap saturation.
///
/// NOTE the division of labour — this test uses baseline-relative inputs, so it
/// covers the FORMULA and is deliberately blind to the baseline's VALUE. The
/// value is covered by
/// `the_baselines_sit_just_above_the_measured_quiet_noise_floor` above.
#[test]
fn calibration_engages_on_a_contended_observation() {
    // 8x the baseline: the budget must GROW, proportionally, from the real
    // constants a real run uses.
    let contended = calibrated(T1_EXIT, ACK_QUIET_BASELINE * 8, "t_ack", ACK_QUIET_BASELINE);
    assert!(
        (contended.scale - 8.0).abs() < 1e-9,
        "scale must track the observation: {contended:?}"
    );
    assert!(
        contended.derived > T1_EXIT.base,
        "a contended observation must LOOSEN the budget: derived {:?} vs base {:?}",
        contended.derived,
        T1_EXIT.base
    );
    assert_eq!(
        contended.derived, T1_EXIT.cap,
        "8x on this spec saturates the cap"
    );

    // Just over the baseline: growth is proportional, not a step to the cap.
    let mild = calibrated(
        T1_EXIT,
        ACK_QUIET_BASELINE + ACK_QUIET_BASELINE / 4,
        "t_ack",
        ACK_QUIET_BASELINE,
    );
    assert!(
        mild.derived > T1_EXIT.base && mild.derived < T1_EXIT.cap,
        "1.25x must land strictly between base and cap: {mild:?}"
    );

    // And an observation under the baseline is exactly `base` — the quiet-host
    // property, from the same real constants.
    let quiet = calibrated(
        T1_EXIT,
        ACK_QUIET_BASELINE / 10,
        "t_ack",
        ACK_QUIET_BASELINE,
    );
    assert_eq!(quiet.derived, T1_EXIT.base);
    assert_eq!(quiet.scale, 1.0);
}

#[test]
fn calibration_is_the_identity_on_a_quiet_observation() {
    // A quiet host measures below `quiet_baseline`, so `scale == 1` and the
    // derived budget is EXACTLY `base`: calibration can never tighten a budget
    // and can never itself flake on an unloaded box.
    // Baseline-RELATIVE, deliberately: this is a formula test, so it must not
    // break when the baseline moves (an absolute 12ms here started failing the
    // moment `ACK_QUIET_BASELINE` was correctly derived down to 11.2ms). The
    // baseline's VALUE is guarded by
    // `the_baselines_sit_just_above_the_measured_quiet_noise_floor`.
    let b = calibrated(
        spec(15, 30),
        ACK_QUIET_BASELINE / 2,
        "t_ack",
        ACK_QUIET_BASELINE,
    );
    assert_eq!(b.scale, 1.0, "quiet observation must not scale: {b:?}");
    assert_eq!(b.derived, b.base, "quiet host must get exactly `base`");
}

#[test]
fn calibration_only_ever_loosens_and_never_exceeds_the_cap() {
    // Observation at exactly the baseline is still the identity.
    let at_baseline = calibrated(
        spec(10, 40),
        ACK_QUIET_BASELINE,
        "t_ack",
        ACK_QUIET_BASELINE,
    );
    assert_eq!(at_baseline.derived, Duration::from_secs(10));

    // 3x the baseline loosens proportionally.
    let contended = calibrated(
        spec(10, 40),
        ACK_QUIET_BASELINE * 3,
        "t_ack",
        ACK_QUIET_BASELINE,
    );
    assert!((contended.scale - 3.0).abs() < 1e-9, "{contended:?}");
    assert_eq!(contended.derived, Duration::from_secs(30));

    // A pathological observation is clamped at the cap, never beyond it.
    let saturated = calibrated(
        spec(10, 40),
        ACK_QUIET_BASELINE * 600,
        "t_ack",
        ACK_QUIET_BASELINE,
    );
    assert_eq!(saturated.derived, saturated.cap);

    // And the derivation is reported, so a failure can be audited.
    let described = contended.describe();
    for needle in ["budget", "base", "scale", "cap", "t_ack", "quiet_baseline"] {
        assert!(
            described.contains(needle),
            "budget description must report {needle:?}: {described}"
        );
    }
}

#[test]
fn a_bare_budget_names_itself_as_uncalibrated() {
    let described = bare(SESSION_UP_DEADLINE).describe();
    assert!(
        described.contains("BARE"),
        "the irreducible bound must say so: {described}"
    );
}

#[test]
fn the_stage_clock_clips_a_budget_to_the_remaining_total() {
    // CASE 1: clipped but NOT starved. Calibration granted headroom above `base`,
    // the clock took some of it back, and the stage still has its full base — so
    // its own guarantee is intact and the reduction is merely reported.
    let clock = StageClock::new(Duration::from_secs(20));
    let clipped = clock.clip(calibrated(
        spec(10, 40),
        ACK_QUIET_BASELINE * 3, // scale 3 => derived 30s, above the 20s remaining
        "t_ack",
        ACK_QUIET_BASELINE,
    ));
    assert!(clipped.clipped_to_total, "{clipped:?}");
    assert!(
        !clipped.starved,
        "20s remaining exceeds the 10s base, so this stage is not starved: {clipped:?}"
    );
    assert!(
        clipped.derived <= Duration::from_secs(20),
        "a stage may never outlive the test's total budget: {clipped:?}"
    );
    assert!(
        clipped.describe().contains("CLIPPED"),
        "the clip must be reported: {}",
        clipped.describe()
    );

    // CASE 2: STARVED — the remaining total is below the stage's own base, so the
    // stage cannot make its guarantee at all. This must be reported DIFFERENTLY,
    // because such a stage fails on its first poll and that is otherwise
    // indistinguishable from the property under test genuinely not holding.
    let spent = StageClock::new(Duration::from_secs(1));
    let starved = spent.clip(calibrated(
        spec(30, 30),
        Duration::ZERO,
        "t_ack",
        ACK_QUIET_BASELINE,
    ));
    assert!(starved.clipped_to_total && starved.starved, "{starved:?}");
    let described = starved.describe();
    assert!(
        described.contains("TOTAL BUDGET ALREADY EXHAUSTED BY EARLIER STAGES"),
        "a starved stage must name the exhaustion: {described}"
    );
    assert!(
        described.contains("NOT about the property under test"),
        "a starved stage must disclaim the property: {described}"
    );
}
