//! Issue #1693 (AG4) — graceful shutdown integration test.
//! Issue #3515 — the *oracle* used to observe it.
//!
//! Drives the `cqlite` binary as a REAL child process in interactive
//! `--writable` mode, performs a write over stdin, sends it `SIGINT`, and
//! verifies the process:
//!   1. exits cleanly (success), and
//!   2. flushed the memtable to a durable SSTable before exiting — the written
//!      row is present when the write directory is reopened read-only.
//!
//! # The oracle (issue #3515)
//!
//! The subject property above is unchanged. What changed is *how it is
//! observed*. A single bare `wait_timeout(60s)` after `SIGINT` cannot tell a
//! broken shutdown handler from a child that was never scheduled, yet its expiry
//! message named an absent shutdown handler as the cause — a cause the
//! measurement cannot establish — and it expired on a contended gate host while
//! the handler worked (standalone: 0.34s; under six concurrent gates: >60s).
//!
//! The wait is therefore **staged**, and each stage's failure reports only what
//! that stage measured:
//!
//! | stage | awaited signal | what its expiry establishes |
//! |-------|----------------|-----------------------------|
//! | a. session up      | readiness banner (stderr)         | the banner was not observed in time |
//! | b. write ack       | `OK` (stdout), timed -> `t_ack`    | no write was acknowledged in time |
//! | c. handler entered | Ctrl-C handler-entry marker (stderr) | signal undelivered / handler not entered / marker text drifted |
//! | d. clean exit      | process exit, progress-checked     | the shutdown flush did not complete in time |
//!
//! Observing (c) proves three things at once — the signal was delivered, a
//! shutdown handler exists and was entered, and the child was scheduled — which
//! is exactly the conjunction the old message guessed at. So (d) may never claim
//! anything about the *existence* of a handler.
//!
//! Every budget that follows a completed measurement is calibrated from that
//! measurement, taken on this host in this run: `clamp(base * scale, base, cap)`
//! with `scale = max(1, observed / quiet_baseline)`. The baselines are generous
//! (seconds), so a quiet host always yields `scale == 1`: calibration can only
//! loosen a budget, never tighten one.
//!
//! Unix-only: it sends a real `SIGINT` via `libc::kill`.

#![cfg(all(feature = "write-support", unix))]

use serde_json::Value as Json;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
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
const MARKER_HANDLER_ENTERED: &str = "Received Ctrl-C";

// ---------------------------------------------------------------------------
// Stage budgets and the total-budget arithmetic
// ---------------------------------------------------------------------------
//
// TOTAL-BUDGET ARITHMETIC (spec: "The test owns a total budget below the
// harness hard-kill"). `.config/nextest.toml` sets
// `slow-timeout = { period = "60s", terminate-after = 4 }` => a **240s hard
// kill**. Each test owns `TEST_TOTAL_BUDGET = 180s` and clips every stage
// budget to what REMAINS of it (`StageClock::clip`), so the stages can never
// sum past 180s however slow the host is — the test always emits its own
// attributed failure instead of being killed by the harness.
//
// EVERY wait ON A CHILD PROCESS is a stage, INCLUDING the read-side durability
// SELECT. That was not true in the first version of this change: `select_rows`
// used `Command::output()`, which has no timeout at all, so on the saturated
// host this issue is about it was an unbounded wait sitting OUTSIDE the budget —
// and an overrun there lands on nextest's hard kill, producing exactly the
// uninformative failure this change exists to remove. It is now stage (e), with
// its own calibrated budget and its own attributed message.
//
// Independently of the clipping, the nominal per-stage CAPS are chosen so their
// worst-case sum is already under the total:
//
//   sigint_in_writable_session_flushes_before_exit
//     (a) session up 40 + (b) ack 25 + (c) handler 25 + (d) exit 50
//       + (e) durability read 35                                    = 175s <= 180s
//
//   writable_session_auto_flushes_mid_session_across_threshold
//     (a) 40 + (b) 5 writes x 10 = 50 + (c) sstable 25 + (d) EOF exit 25
//       + (e) durability read 35                                    = 175s <= 180s
//
// What the remaining 60s of the 240s now covers is only what CANNOT be a stage:
// `TempDir` teardown, and the bounded (5s) collection of the read-side child's
// already-at-EOF pipes after it has exited.
const TEST_TOTAL_BUDGET: Duration = Duration::from_secs(180);

/// Stage (a). **The irreducible bound** (design.md, "The residual").
///
/// This one deadline is NOT calibrated, and cannot be: calibrating it would
/// require a measurement taken before it, whose own bound would need a
/// measurement before *that* — the regress terminates only by accepting one
/// bare wall-clock deadline. It is placed on the cheapest operation in the test
/// (process spawn + dynamic link + engine init, not a flush), and its expiry
/// message states exactly what the expiry means and nothing more. It is exempt
/// from the calibration requirement rather than silently non-compliant with it.
const SESSION_UP_DEADLINE: Duration = Duration::from_secs(40);

// The quiet-host references below are set from MEASURED quiet values on this
// test (warm build, unloaded 16-core box, `--test-threads=1`):
//
//   t_boot (spawn -> readiness banner)            22-23ms
//   t_ack  (write -> `OK`)                        3ms (SIGINT test)
//                                                 38ms (sibling, slowest of 5)
//
// They are set an order of magnitude ABOVE those, so an unloaded host always
// measures well under the baseline and always gets `scale == 1` — but NOT
// arbitrarily above them, and that upper limit is load-bearing rather than
// cosmetic. `scale` is `observed / quiet_baseline`, so a baseline chosen far
// above the quiet measurement makes the calibration INERT: with a 1s `t_ack`
// baseline, the issue's measured ~175x contended host (t_ack ~0.5s) would still
// yield `scale == 1`, leaving stage (d) bounded by its 25s base — TIGHTER than
// the 60s deadline #3515 is fixing, i.e. a regression dressed as a fix.
// (design.md D2 suggests baselines "in seconds, not milliseconds"; that was
// written before these measurements and is the one place this implementation
// deviates from it, for the reason above. Reported with the change.)
//
// With the values below, that same ~175x host derives `scale ~= 2.6` from
// `t_ack` and lands stages (c)/(d) on their caps (30s/60s) — i.e. at least the
// old ceiling under real contention, while a quiet box still fails a genuine
// hang in `base`.

/// Quiet-host reference for `t_boot` (spawn -> readiness banner): ~22x the
/// measured quiet value.
const BOOT_QUIET_BASELINE: Duration = Duration::from_millis(500);

/// Quiet-host reference for `t_ack` (write -> `OK` round-trip): ~5x the slowest
/// measured quiet value (the sibling's 38ms), ~66x the SIGINT test's 3ms.
const ACK_QUIET_BASELINE: Duration = Duration::from_millis(200);

/// The `cqlite` binary this test crate built with `--features write-support`.
fn cqlite_bin() -> &'static str {
    env!("CARGO_BIN_EXE_cqlite")
}

/// Write the single-table schema used by the round-trip.
fn write_schema(dir: &Path) -> PathBuf {
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
enum Stream {
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
enum WaitEnd {
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
    fn describe(&self) -> String {
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
struct ChildIo {
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
    fn wait_for(
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
    fn transcript_text(&self) -> String {
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
struct Budget {
    /// What the wait is actually bounded by.
    derived: Duration,
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
}

/// `clamp(base * scale, base, cap)` with `scale = max(1, observed /
/// quiet_baseline)`.
///
/// `scale` is floored at 1 and `derived` is clamped at `base`, so calibration
/// can only ever LOOSEN a budget. A quiet host measures far below
/// `quiet_baseline`, yields `scale == 1`, and gets exactly `base` — calibration
/// can therefore never itself become a source of flakes on an unloaded box.
fn calibrated(
    base: Duration,
    cap: Duration,
    observed: Duration,
    observed_name: &'static str,
    quiet_baseline: Duration,
) -> Budget {
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
    }
}

impl Budget {
    /// How this budget was arrived at — reported by every wait failure.
    fn describe(&self) -> String {
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
struct StageClock {
    started: Instant,
    total: Duration,
    spent: Vec<(&'static str, Duration)>,
}

impl StageClock {
    fn new(total: Duration) -> Self {
        Self {
            started: Instant::now(),
            total,
            spent: Vec::new(),
        }
    }

    fn remaining(&self) -> Duration {
        self.total.saturating_sub(self.started.elapsed())
    }

    /// Shorten a stage budget to what remains of the total budget. This is what
    /// makes the per-stage sum bounded by construction, whatever the host does.
    fn clip(&self, mut budget: Budget) -> Budget {
        let remaining = self.remaining();
        if budget.derived > remaining {
            budget.derived = remaining;
            budget.clipped_to_total = true;
        }
        budget
    }

    fn record(&mut self, stage: &'static str, took: Duration) {
        self.spent.push((stage, took));
    }

    /// Per-stage timings + total-budget state, for both diagnostics and the
    /// end-of-test record printed with `--nocapture`.
    fn report(&self) -> String {
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
enum PollGaveUp {
    /// The budget expired AND nothing had happened for the whole stall window.
    Stalled,
    /// Progress kept arriving, but the test's own total budget ran out.
    TotalBudgetExhausted,
}

#[derive(Debug)]
struct PollFail {
    why: PollGaveUp,
    elapsed: Duration,
    stall: Duration,
    stall_window: Duration,
    new_lines: usize,
    new_artifacts: usize,
}

impl PollFail {
    /// What the poll observed — never why it happened.
    fn observed(&self) -> String {
        let why = match self.why {
            PollGaveUp::Stalled => format!(
                "the budget expired and NOTHING was observed for the whole stall window \
                 ({:.2?}, itself calibrated)",
                self.stall_window
            ),
            PollGaveUp::TotalBudgetExhausted => "progress was still arriving, but the test's own \
                 TOTAL budget ran out (it must fail before nextest's 240s hard kill)"
                .to_string(),
        };
        format!(
            "gave up after {:.2?}: {why}\n\
             progress observed while polling: {} new output line(s), {} new durable artifact(s); \
             last progress was {:.2?} ago",
            self.elapsed, self.new_lines, self.new_artifacts, self.stall
        )
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
fn poll_with_progress<T>(
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
fn count_data_db(data_dir: &Path) -> usize {
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
fn select_rows(
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
    let took = started.elapsed();

    // The child has exited, so both pipes are at EOF and the reader threads
    // finish promptly; bound the collection anyway rather than block forever.
    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    let mut collected = 0;
    while collected < 2 {
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok((Stream::Stdout, buf)) => stdout_buf = buf,
            Ok((Stream::Stderr, buf)) => stderr_buf = buf,
            Err(_) => panic!(
                "stage (e) durability-read: the read-side child exited ({status:?}) but its \
                 output could not be collected within 5s (collected {collected}/2 streams). \
                 This is a defect in this test harness, not a statement about durability.\n{}",
                clock.report()
            ),
        }
        collected += 1;
    }

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
fn start_writable_session(
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
fn await_write_ack(
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

/// AC (issue #1693): an interactive `--writable` session that receives SIGINT
/// after a write exits cleanly AND has flushed the row to a durable SSTable.
///
/// Oracle (issue #3515): four staged waits, each reporting only what it measures.
#[test]
fn sigint_in_writable_session_flushes_before_exit() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");
    let data_dir = wd.join("data");
    let mut clock = StageClock::new(TEST_TOTAL_BUDGET);

    // Stage (a): session up (bare deadline — the irreducible bound).
    let (mut child, io, t_boot) = start_writable_session(&wd, &schema, &[], &mut clock);

    // Keep the stdin handle alive for the whole test so the child exits via
    // SIGINT, NOT via stdin EOF (an EOF would also flush and mask the bug).
    let mut stdin = child.stdin.take().expect("child stdin");

    // Stage (b): write ack, timed -> `t_ack`. Budget calibrated from `t_boot`.
    writeln!(
        stdin,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (7, 'Grace', 30, true);"
    )
    .expect("write INSERT to child stdin");
    stdin.flush().expect("flush child stdin");

    let ack_budget = clock.clip(calibrated(
        Duration::from_secs(15),
        Duration::from_secs(25),
        t_boot,
        "t_boot",
        BOOT_QUIET_BASELINE,
    ));
    let t_ack = await_write_ack(
        &io,
        "stage (b) write-ack",
        "the INSERT (id=7)",
        &ack_budget,
        &clock,
    );
    clock.record("b.write-ack", t_ack);

    // The stall window for the progress-checked exit wait is calibrated from the
    // same `t_ack`: on a host where a full write round-trip takes seconds, a
    // few seconds of silence is not evidence of a stall.
    let stall_window = calibrated(
        Duration::from_secs(5),
        Duration::from_secs(20),
        t_ack,
        "t_ack",
        ACK_QUIET_BASELINE,
    );

    // Send a real SIGINT to the child.
    let pid = child.id() as libc::pid_t;
    let rc = unsafe { libc::kill(pid, libc::SIGINT) };
    assert_eq!(rc, 0, "failed to deliver SIGINT to child pid {pid}");

    // Stage (c): handler ENTERED. Observing this marker establishes, together,
    // that the signal was delivered, that a shutdown handler exists and was
    // entered, and that the child was scheduled — so stage (d) below may never
    // claim anything about a handler's existence.
    let handler_budget = clock.clip(calibrated(
        Duration::from_secs(15),
        Duration::from_secs(25),
        t_ack,
        "t_ack",
        ACK_QUIET_BASELINE,
    ));
    let entered = io.wait_for(
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        handler_budget.derived,
    );
    let t_handler = match entered {
        Ok((_, took)) => took,
        Err(end) => {
            let _ = child.kill();
            panic!(
            "stage (c) handler-entry: the shutdown handler's entry marker was not observed on the \
             child's stderr after SIGINT was delivered to pid {pid}.\n\
             awaited substring on stderr: {MARKER_HANDLER_ENTERED:?}\n\
             {}\n\
             {}\n\
             CANDIDATE CAUSES (this measurement does NOT select between them):\n\
             \x20 1. the signal was not delivered to / not received by the child;\n\
             \x20 2. a shutdown handler was not entered (absent, or the interrupt lost a race);\n\
             \x20 3. the product's marker text drifted, so this test awaited a string the child \
             no longer prints — compare the awaited substring against the transcript below.\n\
             child transcript:\n{}\n{}",
            handler_budget.describe(),
            end.describe(),
            io.transcript_text(),
            clock.report()
        );
        }
    };
    clock.record("c.handler-entry", t_handler);

    // Stage (d): clean exit, PROGRESS-CHECKED. A new child output line or a new
    // durable `-Data.db` artifact resets the stall window, so a flush that is
    // landing slowly is never mistaken for a stall.
    let exit_budget = clock.clip(calibrated(
        Duration::from_secs(25),
        Duration::from_secs(50),
        t_ack,
        "t_ack",
        ACK_QUIET_BASELINE,
    ));
    let envelope = clock.remaining();
    let exited = poll_with_progress(
        &io,
        &data_dir,
        &exit_budget,
        stall_window.derived,
        envelope,
        |slice| child.wait_timeout(slice).expect("wait_timeout on child"),
    );
    let (status, t_exit): (ExitStatus, Duration) = match exited {
        Ok(v) => v,
        Err(fail) => {
            let _ = child.kill();
            panic!(
                "stage (d) clean-exit: the shutdown flush did not complete within the budget.\n\
                 {}\n\
                 stall window {}\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: the handler-entry marker {MARKER_HANDLER_ENTERED:?} WAS \
                 observed {:.3?} after SIGINT, so the shutdown handler exists, was entered, and \
                 the child was scheduled. This failure therefore establishes ONLY that the flush \
                 did not complete in time; it says nothing about whether a handler is present.\n\
                 durable -Data.db artifacts under {}: {}\n\
                 child transcript:\n{}\n{}",
                exit_budget.describe(),
                stall_window.describe(),
                fail.observed(),
                t_handler,
                data_dir.display(),
                count_data_db(&data_dir),
                io.transcript_text(),
                clock.report()
            );
        }
    };
    clock.record("d.clean-exit", t_exit);
    // Release stdin only after the process has exited.
    drop(stdin);
    assert!(
        status.success(),
        "child exited uncleanly after SIGINT: {status:?}\nchild transcript:\n{}",
        io.transcript_text()
    );

    // Stage (e): durability. The SIGINT handler must have flushed the memtable to
    // a real SSTable — the row is present on an independent read-only reopen.
    // A fresh CLI process doing a read is the same shape of work as the session
    // boot, so this budget is calibrated from `t_boot`.
    let read_budget = clock.clip(calibrated(
        Duration::from_secs(20),
        Duration::from_secs(35),
        t_boot,
        "t_boot",
        BOOT_QUIET_BASELINE,
    ));
    let (rows, t_read) = select_rows(
        &data_dir,
        &schema,
        "SELECT * FROM test_write.users",
        &read_budget,
        &clock,
    );
    clock.record("e.durability-read", t_read);
    let grace = rows
        .iter()
        .find(|r| r.get("id").and_then(|v| v.as_i64()) == Some(7))
        .unwrap_or_else(|| {
            panic!(
                "row id=7 not durable after SIGINT; rows: {rows:?}\nchild transcript:\n{}",
                io.transcript_text()
            )
        });
    assert_eq!(
        grace["name"].as_str(),
        Some("Grace"),
        "durable row has wrong name: {grace}"
    );

    // Visible with `--nocapture`: the per-stage timings and the budgets they
    // derived, which is what makes a loaded-host run auditable (#3515 AC1).
    eprintln!(
        "[#3515] sigint_in_writable_session_flushes_before_exit\n{}",
        clock.report()
    );
    eprintln!("[#3515]   b.write-ack       {}", ack_budget.describe());
    eprintln!("[#3515]   c.handler-entry   {}", handler_budget.describe());
    eprintln!("[#3515]   d.clean-exit      {}", exit_budget.describe());
    eprintln!("[#3515]   e.durability-read {}", read_budget.describe());
    eprintln!("[#3515]   stall window      {}", stall_window.describe());
}

/// Issue #1693 (roborev): the interactive writable loop must use the async,
/// threshold-flushing path (`execute_flushing`) rather than the sync `execute`
/// (which intentionally skips auto-flush in an async context). Otherwise a long
/// session grows the memtable past the flush threshold up to the hard limit and
/// then FAILS every write until exit.
///
/// This drives a real interactive session with a tiny flush threshold (env
/// override), writes several rows to cross it, and asserts a durable SSTable
/// appears MID-SESSION — before any Ctrl-D/Ctrl-C — and that writes keep being
/// accepted afterwards.
///
/// Oracle (issue #3515 AC4): this test carried the same defective shape in THREE
/// places (per-write ack, mid-session artifact wait, EOF exit), each a bare 60s
/// deadline whose expiry blamed a dead-ended session, or an interactive loop
/// that had bypassed the threshold-flushing path. A timeout establishes neither.
/// All three are now staged, calibrated and (where they poll) progress-checked.
#[test]
fn writable_session_auto_flushes_mid_session_across_threshold() {
    const WRITES: i64 = 5;
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");
    let data_dir = wd.join("data");
    let mut clock = StageClock::new(TEST_TOTAL_BUDGET);

    // Stage (a): session up (bare deadline — the irreducible bound). The tiny
    // threshold makes a handful of small rows cross it, forcing a mid-session
    // flush without writing 64MB over stdin.
    let (mut child, io, t_boot) = start_writable_session(
        &wd,
        &schema,
        &[("CQLITE_MEMTABLE_FLUSH_THRESHOLD", "1")],
        &mut clock,
    );
    let mut stdin = child.stdin.take().expect("child stdin");

    // Stage (b): every write is acknowledged. The first ack is calibrated from
    // `t_boot`; each later one from the slowest ack seen so far, so a session
    // that is merely slow keeps loosening its own budget.
    let mut t_ack = Duration::ZERO;
    for id in 0..WRITES {
        writeln!(
            stdin,
            "INSERT INTO test_write.users (id, name, age, active) VALUES ({id}, 'row{id}', {id}, true);"
        )
        .expect("write INSERT to child stdin");
        stdin.flush().expect("flush child stdin");

        let (observed, name, baseline) = if id == 0 {
            (t_boot, "t_boot", BOOT_QUIET_BASELINE)
        } else {
            (t_ack, "t_ack(slowest so far)", ACK_QUIET_BASELINE)
        };
        let budget = clock.clip(calibrated(
            Duration::from_secs(6),
            Duration::from_secs(10),
            observed,
            name,
            baseline,
        ));
        let took = await_write_ack(
            &io,
            "stage (b) write-ack",
            &format!("write id={id}"),
            &budget,
            &clock,
        );
        t_ack = t_ack.max(took);
    }
    clock.record("b.write-acks", t_ack);

    let stall_window = calibrated(
        Duration::from_secs(5),
        Duration::from_secs(20),
        t_ack,
        "t_ack",
        ACK_QUIET_BASELINE,
    );

    // Stage (c): a durable SSTable must exist BEFORE we close the session.
    // Progress-checked, and calibrated from `t_ack`.
    let sstable_budget = clock.clip(calibrated(
        Duration::from_secs(20),
        Duration::from_secs(25),
        t_ack,
        "t_ack",
        ACK_QUIET_BASELINE,
    ));
    let envelope = clock.remaining();
    let flushed = poll_with_progress(
        &io,
        &data_dir,
        &sstable_budget,
        stall_window.derived,
        envelope,
        |slice| {
            if count_data_db(&data_dir) >= 1 {
                Some(())
            } else {
                thread::sleep(slice);
                None
            }
        },
    );
    let t_sstable = match flushed {
        Ok((_, took)) => took,
        Err(fail) => {
            let _ = child.kill();
            panic!(
                "stage (c) mid-session-flush: no durable `-Data.db` artifact appeared under {} \
                 while the session was still open, after {WRITES} acknowledged writes with \
                 CQLITE_MEMTABLE_FLUSH_THRESHOLD=1.\n\
                 {}\n\
                 stall window {}\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: only that no artifact was observed within that budget. It \
                 does NOT establish that the interactive loop skipped the threshold-flushing path \
                 — a flush still in progress, or a child that was descheduled, produces the same \
                 reading. The writes WERE acknowledged (stage (b) passed), so the session was \
                 accepting statements.\n\
                 child transcript:\n{}\n{}",
                data_dir.display(),
                sstable_budget.describe(),
                stall_window.describe(),
                fail.observed(),
                io.transcript_text(),
                clock.report()
            );
        }
    };
    clock.record("c.mid-session-flush", t_sstable);

    // Stage (d): cleanly end via EOF; progress-checked exit wait.
    drop(stdin);
    let exit_budget = clock.clip(calibrated(
        Duration::from_secs(20),
        Duration::from_secs(25),
        t_ack,
        "t_ack",
        ACK_QUIET_BASELINE,
    ));
    let envelope = clock.remaining();
    let exited = poll_with_progress(
        &io,
        &data_dir,
        &exit_budget,
        stall_window.derived,
        envelope,
        |slice| child.wait_timeout(slice).expect("wait_timeout on child"),
    );
    let (status, t_exit): (ExitStatus, Duration) = match exited {
        Ok(v) => v,
        Err(fail) => {
            let _ = child.kill();
            panic!(
                "stage (d) eof-exit: the child had not exited after its stdin reached EOF.\n\
                 {}\n\
                 stall window {}\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: only that no exit was observed within that budget. The \
                 EOF path flushes and finalizes the engine before returning, so a slow flush and \
                 a wedged one read the same here; the progress check above reports whether \
                 anything was still happening.\n\
                 durable -Data.db artifacts under {}: {}\n\
                 child transcript:\n{}\n{}",
                exit_budget.describe(),
                stall_window.describe(),
                fail.observed(),
                data_dir.display(),
                count_data_db(&data_dir),
                io.transcript_text(),
                clock.report()
            );
        }
    };
    clock.record("d.eof-exit", t_exit);
    assert!(
        status.success(),
        "child exited uncleanly on EOF: {status:?}\nchild transcript:\n{}",
        io.transcript_text()
    );

    // Stage (e): all rows are durable on an independent read-only reopen.
    let read_budget = clock.clip(calibrated(
        Duration::from_secs(20),
        Duration::from_secs(35),
        t_boot,
        "t_boot",
        BOOT_QUIET_BASELINE,
    ));
    let (rows, t_read) = select_rows(
        &data_dir,
        &schema,
        "SELECT * FROM test_write.users",
        &read_budget,
        &clock,
    );
    clock.record("e.durability-read", t_read);
    for id in 0..WRITES {
        assert!(
            rows.iter()
                .any(|r| r.get("id").and_then(|v| v.as_i64()) == Some(id)),
            "row id={id} not durable after mid-session flush; rows: {rows:?}\n\
             child transcript:\n{}",
            io.transcript_text()
        );
    }

    eprintln!(
        "[#3515] writable_session_auto_flushes_mid_session_across_threshold\n{}",
        clock.report()
    );
    eprintln!(
        "[#3515]   c.mid-session-flush {}",
        sstable_budget.describe()
    );
    eprintln!("[#3515]   d.eof-exit          {}", exit_budget.describe());
    eprintln!("[#3515]   e.durability-read   {}", read_budget.describe());
    eprintln!("[#3515]   stall window        {}", stall_window.describe());
}

// ---------------------------------------------------------------------------
// Unit coverage for the calibration helper itself (tasks.md 1.3)
// ---------------------------------------------------------------------------

#[test]
fn calibration_is_the_identity_on_a_quiet_observation() {
    // A quiet host measures far below `quiet_baseline`, so `scale == 1` and the
    // derived budget is EXACTLY `base`: calibration can never tighten a budget
    // and can never itself flake on an unloaded box.
    let b = calibrated(
        Duration::from_secs(15),
        Duration::from_secs(30),
        Duration::from_millis(12),
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
        Duration::from_secs(10),
        Duration::from_secs(40),
        ACK_QUIET_BASELINE,
        "t_ack",
        ACK_QUIET_BASELINE,
    );
    assert_eq!(at_baseline.derived, Duration::from_secs(10));

    // 3x the baseline loosens proportionally.
    let contended = calibrated(
        Duration::from_secs(10),
        Duration::from_secs(40),
        ACK_QUIET_BASELINE * 3,
        "t_ack",
        ACK_QUIET_BASELINE,
    );
    assert!((contended.scale - 3.0).abs() < 1e-9, "{:?}", contended);
    assert_eq!(contended.derived, Duration::from_secs(30));

    // A pathological observation is clamped at the cap, never beyond it.
    let saturated = calibrated(
        Duration::from_secs(10),
        Duration::from_secs(40),
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
    let clock = StageClock::new(Duration::from_secs(1));
    let clipped = clock.clip(calibrated(
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::ZERO,
        "t_ack",
        ACK_QUIET_BASELINE,
    ));
    assert!(clipped.clipped_to_total, "{clipped:?}");
    assert!(
        clipped.derived <= Duration::from_secs(1),
        "a stage may never outlive the test's total budget: {clipped:?}"
    );
    assert!(
        clipped.describe().contains("CLIPPED"),
        "the clip must be reported: {}",
        clipped.describe()
    );
}
