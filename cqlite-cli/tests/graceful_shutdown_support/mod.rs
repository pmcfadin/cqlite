//! Test harness for `graceful_shutdown_tests.rs` (issues #1693, #3515).
//!
//! Split out of that file under the campsite rule (#1135): the staged oracle
//! #3515 required pushed the single file past the 1500-line test threshold. This
//! module holds the INSTRUMENT — child I/O with a shared transcript, the
//! progress-OBSERVING poll, and the bounded read-side SELECT. `budgets.rs` holds
//! the ONE per-test deadline (round-8 descope, design.md D6a) and the unit tests
//! that pin its invariants. The two integration tests that USE it stay in
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
/// deadline that passed with the pipes still open and a child whose pipes reached
/// EOF are different measurements, and the second is the signature a RED run
/// produces when the child dies instead of handling the signal. Neither variant
/// names WHY.
#[derive(Debug)]
pub enum WaitEnd {
    /// The test's one deadline passed; the child's pipes were still open, so more
    /// output was still possible.
    DeadlineReached,
    /// Both reader threads ended and the queue drained: the child's stdout AND
    /// stderr reached EOF after this long, so no further line could ever arrive.
    /// The child had exited, crashed, or closed its pipes -- this does not say
    /// which.
    PipesClosed(Duration),
}

impl WaitEnd {
    pub fn describe(&self) -> String {
        match self {
            WaitEnd::DeadlineReached => "how the wait ended: the test's one deadline passed with \
                 the child's pipes still open (more output was still possible)"
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

    /// Block until a line on `want` satisfies `pred`, or the TEST's one deadline
    /// passes. Returns the matching line and how much of the stage it took (so a
    /// successful wait can calibrate the deadline).
    ///
    /// Takes the `Stage` itself, never a `Duration`: the timeout comes from
    /// `Stage::remaining()`, the one place a per-wait timeout is computed, so no
    /// call site can be handed a fresh allowance and none can double-spend.
    pub fn wait_for(
        &self,
        want: Stream,
        pred: impl Fn(&str) -> bool,
        stage: &Stage,
    ) -> Result<(String, Duration), WaitEnd> {
        loop {
            let remaining = stage.remaining();
            if remaining.is_zero() {
                return Err(WaitEnd::DeadlineReached);
            }
            match self
                .rx
                .recv_timeout(remaining.min(Duration::from_millis(100)))
            {
                Ok((stream, line)) => {
                    if stream == want && pred(&line) {
                        return Ok((line, stage.spent()));
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                // Both readers ended: the child's pipes are closed and the
                // buffer is drained, so no further line can ever arrive.
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(WaitEnd::PipesClosed(stage.spent()))
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
// Progress OBSERVATION — evidence in the message, never an input to the bound
// ---------------------------------------------------------------------------
//
// THE ROUND-8 DESCOPE (design.md D6a) lands here. Progress observation used to
// EXTEND a stage's budget by a calibrated stall window, which is what made a
// declared per-stage cap not the actual maximum — the defect family four review
// rounds could not close. It now reports what it saw and extends NOTHING: the
// test's one deadline is the only bound — no wait is granted more time than it
// leaves, and none is started past it. It bounds WAITING FOR evidence, not the
// acceptance of evidence already observed; see `poll_with_progress` for the
// success path that is deliberately accepted late, and for the bound on the lag.
//
// What is kept is the value: a failure that says `progress observed: NONE - 0 new
// output lines and 0 new durable artifacts` is a materially different diagnosis
// from one that says the flush was still landing when the deadline passed.

/// What a [`poll_with_progress`] gave up with: the observation, never a cause.
#[derive(Debug)]
pub struct PollFail {
    /// How long the STAGE ran. Diagnostic: the bound was the test's deadline.
    stage_spent: Duration,
    /// The one deadline's derivation, captured at the moment of failure.
    deadline: String,
    /// How long since anything at all was observed.
    since_progress: Duration,
    new_lines: usize,
    new_artifacts: usize,
}

impl PollFail {
    /// What the poll observed — never why it happened.
    ///
    /// There is exactly ONE way to give up now (the test's deadline passed), so
    /// this reports that fact and the progress evidence, and nothing else. The
    /// three-variant `PollGaveUp` it replaces existed only to distinguish which
    /// piece of budget arithmetic had bound the stage; one of those variants
    /// (roborev job 219, finding 3) asserted a cause it could not establish, and
    /// another (job 229, finding 2) reported the wrong variant for a starved
    /// stage. Neither is expressible now.
    pub fn observed(&self) -> String {
        let progress_seen = self.new_lines + self.new_artifacts;
        let counts = if progress_seen == 0 {
            format!(
                "progress observed while polling: NONE — 0 new output lines and 0 new durable \
                 artifacts in {:.2?}",
                self.stage_spent
            )
        } else {
            format!(
                "progress observed while polling: {} new output line(s), {} new durable \
                 artifact(s); last progress was {:.2?} ago. NOTE: observed progress is EVIDENCE \
                 ONLY — it does not and may not extend the deadline (design.md D6a)",
                self.new_lines, self.new_artifacts, self.since_progress
            )
        };
        format!(
            "gave up after {:.2?}, when the test's ONE deadline passed while this stage was \
             pending — which is what attributes the failure to this stage and to nothing else.\n\
             {}\n{counts}",
            self.stage_spent, self.deadline
        )
    }
}

/// Poll `step` in short slices until it completes or the TEST's one deadline
/// passes, OBSERVING (never crediting) a new child output line or a new durable
/// `-Data.db` artifact as progress.
///
/// This is AC1's "unbounded-but-progress-checked loop" inside the single bounded
/// envelope: the liveness confirmation AC1 asks for comes from stage (c)'s
/// handler-entry marker and from the progress counts reported here, not from a
/// budget that progress could move.
///
/// WHAT THE DEADLINE BOUNDS, EXACTLY: it bounds how long the test WAITS FOR
/// EVIDENCE. It does NOT bound the acceptance of evidence already in hand
/// (roborev job 232 finding 1, and the OVERRULE recorded in tasks.md round 9).
///
/// The deadline is checked BEFORE `step` is invoked and `step` is handed
/// `min(SLICE, remaining)` (roborev job 229, finding 3), so no wait here is ever
/// STARTED past the deadline and no single wait is granted more than what is
/// left. It is deliberately NOT rechecked on the SUCCESS path: if `step` reports
/// that the child exited — or that the artifact appeared — while the deadline
/// lapses, that success is ACCEPTED.
///
/// THAT IS A DECISION, NOT AN OVERSIGHT. On the success path the property has been
/// OBSERVED. Rejecting an observed success because the loop noticed it a few
/// hundred milliseconds late would be a false failure on a working product —
/// precisely the flake class #3515 exists to remove — and it would make the
/// test's verdict depend on how long a directory scan happened to take, which is
/// the scheduling sensitivity this change exists to eliminate.
///
/// THE OVERRUN IS BOUNDED BUT NOT TINY. The instant this loop decides can lag the
/// deadline by at most one `SLICE.min(remaining)` (<= 100ms) plus one
/// `count_data_db` scan — and that scan is a recursive `read_dir` walk of the
/// data directory, which on a loaded host is not necessarily quick. The same lag
/// applies to the FAILURE path, which is declared at the next loop top rather
/// than the instant the deadline passes; `PollFail` reports the stage's real
/// spend, so the message never understates it.
///
/// So read every "nothing may exceed the deadline" claim in this harness as a
/// statement about the timeout ARITHMETIC — no wait is granted, or started, past
/// the deadline — never as a wall-clock guarantee about the instant a verdict is
/// returned.
pub fn poll_with_progress<T>(
    io: &ChildIo,
    data_dir: &Path,
    stage: &Stage,
    mut step: impl FnMut(Duration) -> Option<T>,
) -> Result<(T, Duration), PollFail> {
    const SLICE: Duration = Duration::from_millis(100);
    let mut last_progress = Instant::now();
    let mut artifacts = count_data_db(data_dir);
    let mut new_lines = 0usize;
    let mut new_artifacts = 0usize;

    loop {
        let remaining = stage.remaining();
        if remaining.is_zero() {
            return Err(PollFail {
                stage_spent: stage.spent(),
                deadline: stage.describe(),
                since_progress: last_progress.elapsed(),
                new_lines,
                new_artifacts,
            });
        }
        if let Some(done) = step(SLICE.min(remaining)) {
            return Ok((done, stage.spent()));
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
/// BOUNDED and ATTRIBUTED: `Command::output()` has no timeout, so the original
/// version of this helper was an unbounded wait on a child process — outside any
/// bound at all, on the one host class this issue is about — and nothing anywhere
/// runs `cqlite-cli`'s tests under a harness that would cut it short (design.md
/// D6).
///
/// THIS STAGE PERFORMS THREE WAITS (spawn, `wait_timeout`, two pipe collections)
/// AND WAS THE SITE OF THREE SEPARATE FINDINGS, all the same defect: each wait
/// separately received the stage's full budget, so the stage could consume a
/// multiple of its own declared cap. Every wait below takes `stage.remaining()`,
/// which is the TEST's one deadline: the spawn is charged, the collection gets
/// only what the child wait left, and there is no per-call-site subtraction left
/// to forget.
pub fn select_rows(
    data_dir: &Path,
    schema: &Path,
    query: &str,
    stage: &Stage,
) -> (Vec<Json>, Duration) {
    // The stage is live from the moment the caller opened it, so the spawn below
    // is already charged to stage (e) — the fix for roborev job 224, finding 2,
    // which timed the stage from before the spawn but then handed the wait a fresh
    // full budget.
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
        .wait_timeout(stage.remaining())
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
                 before the test's deadline. It says NOTHING about whether the row is durable, and \
                 nothing about the write side, which had already exited cleanly. \
                 `Command::output()` has no timeout and no test harness bounds this target, so \
                 without this stage the wait would be UNBOUNDED and no message would appear at \
                 all.\n{}",
                data_dir.display(),
                stage.describe(),
                stage.report()
            );
        }
    };
    // How much of stage (e) the child wait consumed, so the collection's failure
    // message can say so. Diagnostic only: the bound below is the test's deadline.
    let child_wait = stage.spent();

    // The child has exited, so both pipes are at EOF and the reader threads
    // finish promptly — but "promptly" is a claim about SCHEDULING, and a reader
    // thread on a saturated host can stay descheduled for seconds. This was once a
    // hardcoded `recv_timeout(5s)`: a NEW, uncalibrated wall-clock bound that could
    // false-fail under exactly the contention #3515 is about (roborev job 219,
    // finding 2). It then became a hand-computed `budget.derived - elapsed`, which
    // is the arithmetic that produced job 222 finding 1 and job 224 finding 2.
    //
    // It is now `stage.remaining()`: the test's one deadline. No constant, no
    // subtraction, and nothing for a future edit here to get wrong.
    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    let mut collected = 0;
    while collected < 2 {
        let left = stage.remaining();
        if left.is_zero() {
            panic!(
                "stage (e) durability-read: the read-side child exited ({status:?}) but only \
                 {collected}/2 of its output streams could be collected before the test's \
                 deadline (the spawn and the child wait had already taken {child_wait:.2?} of \
                 this stage).\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: only that a reader thread had not delivered its buffer \
                 in time. It says nothing about durability, and nothing about the child, which \
                 exited successfully.\n{}",
                stage.describe(),
                stage.report()
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
                stage.report()
            ),
            Err(RecvTimeoutError::Timeout) => {}
        }
    }

    // Stage (e)'s duration INCLUDES the spawn and the collection, so the reported
    // timing describes the same quantity the stage bounded.
    let took = stage.spent();

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
/// Returns the child, its I/O, and `t_boot` (spawn -> banner), the first in-band
/// measurement the test's one deadline can be calibrated from.
pub fn start_writable_session(
    wd: &Path,
    schema: &Path,
    env: &[(&str, &str)],
    deadline: &TestDeadline,
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

    // Stage (a) is opened BEFORE the spawn, deliberately: `t_boot` spans the whole
    // spawn -> banner path (fork/exec + dynamic link + engine init), and the stage
    // that is measured must be the stage that is timed. Opening it after the spawn
    // would leave the spawn out of `t_boot` — the same defect as roborev job 224
    // finding 2, one stage over.
    //
    // The bound here is the test's deadline, still at its UNCALIBRATED base (no
    // measurement exists yet to calibrate it from — design.md, "The residual").
    let stage = deadline.stage("a.session-up");
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cqlite interactive writable session");
    let io = ChildIo::attach(&mut child);

    let ready = io.wait_for(Stream::Stderr, |l| l.contains(MARKER_SESSION_READY), &stage);
    if let Err(end) = &ready {
        let _ = child.kill();
        panic!(
            "stage (a) session-up: the readiness banner was not observed on the child's stderr.\n\
             awaited substring on stderr: {MARKER_SESSION_READY:?}\n\
             {}\n\
             {}\n\
             WHAT THIS ESTABLISHES: only that the banner was not observed before the deadline on \
             THIS host. It does NOT distinguish a child that never reached the interactive loop \
             from one that was never scheduled, nor either of those from drift in the product's \
             banner text.\n\
             child transcript:\n{}\n{}",
            stage.describe(),
            end.describe(),
            io.transcript_text(),
            stage.report()
        );
    }
    // `t_boot` is the stage's own spend, which starts before the spawn (above).
    let t_boot = stage.finish();
    (child, io, t_boot)
}

/// Wait for a write acknowledgement (`OK` on stdout). Returns how long the
/// round-trip took.
///
/// Shared by both tests. The failure reports what it awaited, how the one deadline
/// was derived, and what the child actually said. It does NOT conclude that the
/// session dead-ended, nor that no interactive writable session exists (the two
/// causes the retired messages named), neither of which a timeout establishes.
pub fn await_write_ack(io: &ChildIo, what: &str, stage: &Stage) -> Duration {
    match io.wait_for(Stream::Stdout, |l| l.trim() == "OK", stage) {
        Ok((_, took)) => took,
        Err(end) => panic!(
            "stage {}: {what} was not acknowledged with `OK` on the child's stdout.\n\
             awaited on stdout: a line whose trimmed text is exactly \"OK\"\n\
             {}\n\
             {}\n\
             WHAT THIS ESTABLISHES: only that no acknowledgement was observed before the test's \
             deadline. It does NOT establish whether the write was rejected, is still in progress, \
             was never read, or whether the child was descheduled — inspect the transcript below \
             (the child prints `Error: ...` on stderr for a rejected statement).\n\
             child transcript:\n{}\n{}",
            stage.name(),
            stage.describe(),
            end.describe(),
            io.transcript_text(),
            stage.report()
        ),
    }
}

// ---------------------------------------------------------------------------
// Unit coverage for the harness (the deadline's own invariants are in
// `budgets.rs`)
// ---------------------------------------------------------------------------

#[cfg(test)]
impl ChildIo {
    /// A `ChildIo` with no child behind it: the returned `Sender` stands in for a
    /// reader thread, so a unit test can make progress arrive on demand.
    fn synthetic() -> (Self, Sender<(Stream, String)>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                rx,
                transcript: Arc::new(Mutex::new(Vec::new())),
            },
            tx,
        )
    }
}

/// OBSERVED PROGRESS MAY NOT EXTEND THE DEADLINE — the property the round-8
/// descope exists to make true (design.md D6a).
///
/// Under the pre-descope design, progress on every slice reset a calibrated stall
/// window and pushed the stage past its declared cap, which is why a declared cap
/// was not the actual maximum. Here progress arrives on EVERY slice and the poll
/// must still give up when the one deadline passes — and must still REPORT what it
/// saw, because the evidence is the part worth keeping.
///
/// There is no timing threshold asserted: the property is that the poll
/// TERMINATES at all under continuous progress. It is run on a worker thread with
/// a 30s collection bound (100x the 300ms deadline under test) so a regression
/// reports a diagnosis instead of hanging the suite.
#[test]
fn observed_progress_never_extends_the_deadline() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let (done_tx, done_rx) = mpsc::channel();

    let worker = thread::spawn(move || {
        let (io, lines) = ChildIo::synthetic();
        let deadline = TestDeadline::start(Duration::from_millis(300), Duration::from_millis(300));
        let stage = deadline.stage("synthetic");
        let outcome = poll_with_progress(&io, &data_dir, &stage, |slice| {
            // Progress on every single slice.
            let _ = lines.send((Stream::Stderr, "still working".to_string()));
            thread::sleep(slice);
            None::<()>
        });
        let report = match outcome {
            Ok(_) => unreachable!("the step never completes"),
            Err(fail) => fail.observed(),
        };
        let _ = done_tx.send(report);
    });

    let observed = done_rx.recv_timeout(Duration::from_secs(30)).expect(
        "the progress-observing poll did not terminate within 30s despite a 300ms deadline: \
             observed progress is extending the bound, which is exactly what the round-8 descope \
             removed",
    );
    worker.join().expect("poll worker thread");

    assert!(
        observed.contains("new output line(s)"),
        "the failure must REPORT the progress it observed: {observed}"
    );
    assert!(
        observed.contains("EVIDENCE ONLY"),
        "the failure must say the progress it observed did not extend the deadline: {observed}"
    );
    assert!(
        observed.contains("ONE deadline passed"),
        "the failure must name the one bound that ended the poll: {observed}"
    );
}
