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

mod budgets;

pub use budgets::*;

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

    /// Block until a line on `want` satisfies `pred`, or the STAGE's deadline
    /// passes. Returns the matching line and how much of the stage it took (so a
    /// successful wait can calibrate a later stage).
    ///
    /// Takes the `Budget` itself, never a `Duration`: the timeout comes from
    /// `Budget::remaining()`, the one place a per-wait timeout is computed, so a
    /// second wait inside the same stage cannot be handed the full span again.
    pub fn wait_for(
        &self,
        want: Stream,
        pred: impl Fn(&str) -> bool,
        budget: &Budget,
    ) -> Result<(String, Duration), WaitEnd> {
        loop {
            let remaining = budget.remaining();
            if remaining.is_zero() {
                return Err(WaitEnd::BudgetExpired);
            }
            match self
                .rx
                .recv_timeout(remaining.min(Duration::from_millis(100)))
            {
                Ok((stream, line)) => {
                    if stream == want && pred(&line) {
                        return Ok((line, budget.spent()));
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                // Both readers ended: the child's pipes are closed and the
                // buffer is drained, so no further line can ever arrive.
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(WaitEnd::PipesClosed(budget.spent()))
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
// Progress-checked polling
// ---------------------------------------------------------------------------

/// Why a progress-checked poll gave up.
#[derive(Debug)]
pub enum PollGaveUp {
    /// The NOMINAL budget expired AND nothing had happened for the whole stall
    /// window. The genuine-stall verdict, and the one a silent (progress-free)
    /// hang produces — at exactly the nominal budget, which is why the floor
    /// invariant is stated against `nominal` and not against the declared maximum.
    Stalled,
    /// The stage's DECLARED MAXIMUM (nominal budget + one declared stall window of
    /// progress extension) was reached while progress was still arriving inside
    /// the stall window. The extension is bounded because it is part of the
    /// declared maximum the total-budget arithmetic sums (roborev job 224,
    /// finding 3): an unbounded extension would let this stage consume a later
    /// stage's allowance.
    DeclaredMaximumReached,
    /// The stage's deadline had been CLIPPED to the test's total budget, and that
    /// clipped deadline was reached — so the binding constraint was the total, not
    /// this stage. A backstop: the totals are sized to fit every declared maximum.
    TotalBudgetExhausted,
}

#[derive(Debug)]
pub struct PollFail {
    why: PollGaveUp,
    elapsed: Duration,
    nominal: Duration,
    declared_max: Duration,
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
                "the nominal budget ({:.2?}) expired and NOTHING was observed for the whole \
                 stall window ({:.2?}, itself calibrated)",
                self.nominal, self.stall_window
            ),
            PollGaveUp::DeclaredMaximumReached => format!(
                "this stage reached its DECLARED MAXIMUM ({:.2?} = nominal budget {:.2?} + one \
                 declared stall window {:.2?}) without the stall window ever elapsing, so this \
                 is NOT a stall verdict — it establishes only that ordering. The progress-checked \
                 wait deliberately continues past the nominal budget while the child is still \
                 making progress; that extension is bounded at one stall window because it is \
                 part of this stage's declared maximum, and a later stage is entitled to its own. \
                 Whether the child was making progress is reported by the counts below and by \
                 nothing else (this branch does not require any progress to have been observed)",
                self.declared_max, self.nominal, self.stall_window
            ),
            PollGaveUp::TotalBudgetExhausted => format!(
                "this stage's deadline had been CLIPPED to the test's remaining TOTAL budget, and \
                 that clipped deadline ran out BEFORE the stall window ({:.2?}) elapsed, so this \
                 is NOT a stall verdict — it establishes only that ordering. Whether the child \
                 was making progress is reported by the counts below and by nothing else (this \
                 branch does not require any progress to have been observed)",
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
/// envelope (design.md D6). It takes a [`PollBudget`], not a `Budget`: the
/// progress extension has to be DECLARED to get one, so it cannot be omitted from
/// the declared maximum the total-budget arithmetic sums (roborev job 224, finding
/// 3 — the extension used to be an unaccounted addition on top of a cap that
/// claimed to be a maximum, letting this stage eat a later stage's allowance).
///
/// The stall window comes from the `PollBudget` too, so the extension the deadline
/// grants and the window a stall is judged against are ONE value and cannot
/// disagree.
///
/// The loop gives up only when
///   * the NOMINAL budget has expired AND nothing has happened for the stall
///     window (a genuine stall — and the verdict a silent hang gets, at exactly
///     the nominal budget), or
///   * the stage's DEADLINE is reached: its declared maximum, or the test's total
///     budget if `StageClock::clip` pulled the deadline in.
pub fn poll_with_progress<T>(
    io: &ChildIo,
    data_dir: &Path,
    poll: &PollBudget,
    mut step: impl FnMut(Duration) -> Option<T>,
) -> Result<(T, Duration), PollFail> {
    const SLICE: Duration = Duration::from_millis(100);
    let budget = poll.budget();
    let stall_window = poll.stall_window();
    let mut last_progress = Instant::now();
    let mut artifacts = count_data_db(data_dir);
    let mut new_lines = 0usize;
    let mut new_artifacts = 0usize;

    loop {
        if let Some(done) = step(SLICE) {
            return Ok((done, budget.spent()));
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

        let elapsed = budget.spent();
        let stall = last_progress.elapsed();
        let why = if elapsed >= budget.nominal() && stall >= stall_window {
            Some(PollGaveUp::Stalled)
        } else if budget.remaining().is_zero() {
            // The deadline is the ONE bound here. Which of the two things pulled
            // it in is a property of the budget, not a second piece of
            // arithmetic at this call site.
            Some(if budget.clipped_to_total() {
                PollGaveUp::TotalBudgetExhausted
            } else {
                PollGaveUp::DeclaredMaximumReached
            })
        } else {
            None
        };
        if let Some(why) = why {
            return Err(PollFail {
                why,
                elapsed,
                nominal: budget.nominal(),
                declared_max: budget.span(),
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
/// in `budgets.rs`: `Command::output()` has no timeout, so the original version of
/// this helper was an unbounded wait on a child process, outside the test's budget,
/// on the one host class this issue is about.
///
/// THIS STAGE PERFORMS THREE WAITS (spawn, `wait_timeout`, two pipe collections)
/// AND WAS THE SITE OF THREE SEPARATE FINDINGS, all the same defect: each wait
/// separately received the stage's full `derived` duration, so the stage could
/// consume a multiple of its own cap and the cap-sum arithmetic bounded nothing.
/// Every wait below now takes `budget.remaining()`, so the spawn is charged, the
/// collection gets only what the child wait left, and there is no per-call-site
/// subtraction left to forget.
pub fn select_rows(
    data_dir: &Path,
    schema: &Path,
    query: &str,
    budget: &Budget,
    clock: &StageClock,
) -> (Vec<Json>, Duration) {
    // The budget is LIVE from the moment the caller derived it, so the spawn below
    // is already charged to stage (e) — the fix for roborev job 224, finding 2,
    // which timed the stage from before the spawn but then handed the wait the
    // stage's FULL budget.
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
        .wait_timeout(budget.remaining())
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
                 about the write side, which had already exited cleanly. `Command::output()` has no \
                 timeout and no test harness bounds this target, so without this stage the wait \
                 would be UNBOUNDED and no message would appear at all.\n{}",
                data_dir.display(),
                budget.describe(),
                clock.report()
            );
        }
    };
    // How much of stage (e) the child wait consumed, so the collection's failure
    // message can say so. Diagnostic only: the bound below comes from the stage's
    // deadline, not from this value.
    let child_wait = budget.spent();

    // The child has exited, so both pipes are at EOF and the reader threads
    // finish promptly — but "promptly" is a claim about SCHEDULING, and a reader
    // thread on a saturated host can stay descheduled for seconds. This was once a
    // hardcoded `recv_timeout(5s)`: a NEW, uncalibrated wall-clock bound that could
    // false-fail under exactly the contention #3515 is about (roborev job 219,
    // finding 2). It then became a hand-computed `budget.derived - elapsed`, which
    // is the arithmetic that produced job 222 finding 1 (a fresh full budget, so
    // stage (e) could spend up to 2x its cap) and job 224 finding 2.
    //
    // It is now `budget.remaining()`: the stage's own deadline, already bounded by
    // the total budget through `StageClock::clip`. No constant, no subtraction, and
    // nothing for a future edit here to get wrong.
    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    let mut collected = 0;
    while collected < 2 {
        let left = budget.remaining();
        if left.is_zero() {
            panic!(
                "stage (e) durability-read: the read-side child exited ({status:?}) but only \
                 {collected}/2 of its output streams could be collected before stage (e)'s \
                 deadline (the spawn and the child wait had already spent {child_wait:.2?} of \
                 the stage).\n\
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

    // Stage (e)'s duration INCLUDES the spawn and the collection, so the reported
    // timing and the declared-maximum arithmetic describe the same quantity.
    let took = budget.spent();

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

    // Stage (a) — THE IRREDUCIBLE BOUND. See `SESSION_UP_DEADLINE`.
    //
    // Derived BEFORE the spawn, deliberately: `t_boot` spans the whole
    // spawn -> banner path (fork/exec + dynamic link + engine init), and the stage
    // that is bounded must be the stage that is measured. Deriving it after the
    // spawn would leave the spawn uncharged — the same defect as roborev job 224
    // finding 2, one stage over.
    let budget = clock.clip(bare(SESSION_UP_DEADLINE));
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cqlite interactive writable session");
    let io = ChildIo::attach(&mut child);

    let ready = io.wait_for(
        Stream::Stderr,
        |l| l.contains(MARKER_SESSION_READY),
        &budget,
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
    // `t_boot` is the stage's own spend, which starts before the spawn (above).
    let t_boot = budget.spent();
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
    match io.wait_for(Stream::Stdout, |l| l.trim() == "OK", budget) {
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
