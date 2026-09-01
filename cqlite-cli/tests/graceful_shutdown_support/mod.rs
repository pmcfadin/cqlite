//! Test harness for `graceful_shutdown_tests.rs` (issues #1693, #3515).
//!
//! Split out of that file under the campsite rule (#1135): the staged oracle
//! #3515 required pushed the single file past the 1500-line test threshold. This
//! module holds the INSTRUMENT — child I/O with a shared transcript, the
//! progress-OBSERVING poll, and the bounded read-side SELECT. `budgets.rs` holds
//! the ONE per-test deadline (round-8 descope, design.md D6a) and the unit tests
//! that pin its invariants; `harness_tests.rs` holds the unit tests for the
//! instrument in THIS file. The two integration tests that USE it stay in
//! `graceful_shutdown_tests.rs`.
//!
//! Included as a module (not a test target) by exactly one consumer, so the
//! parent's `#![cfg(all(feature = "write-support", unix))]` gates it too.

use serde_json::Value as Json;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use wait_timeout::ChildExt;

mod budgets;
mod transcript;

pub use budgets::*;
pub use transcript::*;

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
// Child I/O
// ---------------------------------------------------------------------------
//
// `ChildIo`, `Mark`, `TranscriptSnapshot`, `WaitEnd` and the reader threads live
// in `transcript.rs`: THE ONE STORE and the waits that read it (design.md D6b).
// The channel that used to sit beside that store — and the `Empty`/`Disconnected`
// and stale-re-delivery families it made expressible — is deleted, not patched.

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
// success path that is deliberately accepted late, for the bound on the lag, and
// for the final non-blocking check that keeps the FAILURE path from declaring a
// timeout against evidence that had already arrived.
//
// What is kept is the value: a failure that says `progress observed: NONE - 0 new
// output lines and 0 new durable artifacts` is a materially different diagnosis
// from one that says the flush was still landing when the deadline passed.

/// What [`poll_with_progress`] returns.
///
/// The failure is BOXED because it carries the transcript snapshot its verdict was
/// taken from (job 243, finding 1) and clippy's `result_large_err` is right that a
/// 150-byte `Err` on a hot success path is the wrong shape. It is allocated only on
/// the failure path, which then panics.
pub type PollOutcome<T> = Result<(T, Duration), Box<PollFail>>;

/// What a [`poll_with_progress`] gave up with: the observation, never a cause.
#[derive(Debug)]
pub struct PollFail {
    /// How long the STAGE ran. Diagnostic: the bound was the test's deadline.
    stage_spent: Duration,
    /// The one deadline's derivation, captured at the moment of failure.
    deadline: String,
    /// How long since anything at all was observed.
    ///
    /// DERIVED FROM THE SAME SNAPSHOT as `new_lines` below: the later of the newest
    /// record's own instant and the last artifact increase (design.md D6b). Under
    /// two stores this was a running `last_progress` updated from a DIFFERENT sample
    /// than the count, so a line appended between the two was counted as new output
    /// while "last progress" still pointed at an older moment (roborev job 247,
    /// finding 3). With one store and one snapshot per iteration there is no second
    /// sample for it to disagree with.
    since_progress: Duration,
    /// New records since the poll began, COUNTED FROM THE SNAPSHOT below — the same
    /// records the message renders (job 243, finding 1). Counting channel drains
    /// instead let the message report "0 new output lines" beside a printed
    /// transcript that showed some.
    new_lines: usize,
    new_artifacts: usize,
    /// The artifact count from the iteration that declared the timeout — THE SAME
    /// SAMPLE the final status check was given. Carried here so a call site can
    /// report it without taking another directory scan after the verdict
    /// (roborev job 236, finding 2).
    artifacts_now: usize,
    /// **WHAT THE READERS' STATE ESTABLISHED ABOUT THE CHILD'S PIPES** — the
    /// [`PipeStatus`] derived from the snapshot below. A separate FACT from the
    /// progress counts (roborev job 243, finding 3): a poll that gave up with the
    /// pipes closed is a different diagnosis from one that gave up with output
    /// still possible.
    ///
    /// **A STATE, NOT A BOOL (round 16 — roborev job 259, finding 1).** This was
    /// `pipes_closed: bool`, taken from the reader COUNT alone, and it survived
    /// round 15's fix at the neighbouring site: a reader that ended in an I/O
    /// ERROR was reported here as a clean EOF, a child one of whose two pipes had
    /// ended was reported as having both still open, and an UNREADABLE store —
    /// which establishes nothing at all — was reported the same way. Deriving the
    /// state from [`TranscriptSnapshot::pipe_status`] makes each of those claims
    /// unspellable rather than merely annotated, which is what round 15 did for
    /// `WaitEnd` and what this propagates.
    ///
    /// Read from the SAME snapshot as the records, under one lock, so it can never
    /// be claimed about a state whose lines the verdict did not examine — which is
    /// what retired the `Empty`-vs-`Disconnected` family rather than fixing it at a
    /// fourth site (design.md D6b).
    pipes: PipeStatus,
    data_dir: PathBuf,
    /// The ONE store read taken at the verdict: `new_lines`, `since_progress`,
    /// `pipes` and the rendered transcript all come from it.
    snapshot: TranscriptSnapshot,
}

impl PollFail {
    /// The transcript the VERDICT was taken from, ready for a panic message.
    pub fn transcript(&self) -> String {
        self.snapshot.render()
    }

    /// The pipe state THIS VERDICT was taken from — the same snapshot everything
    /// else in the message comes from.
    ///
    /// Exposed so the harness's own tests can assert the STATE rather than
    /// pattern-match the sentence [`PollFail::observed`] renders it as: a test that
    /// greps prose passes on a message that has drifted away from what was
    /// measured (round 16).
    pub fn pipes(&self) -> &PipeStatus {
        &self.pipes
    }

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
        // ONE derivation, rendered rather than re-decided (round 16): the two
        // branches this replaces asserted EOF from a count that could not establish
        // it, and "still open" from a count that could not establish that either.
        let pipes = format!("\npipe state at the verdict: {}", self.pipes.describe());
        format!(
            "gave up after {:.2?}, when the test's ONE deadline passed while this stage was \
             pending — which is what attributes the failure to this stage and to nothing else.\n\
             {}\n{counts}{pipes}\n\
             durable `-Data.db` artifacts under {} when the verdict was taken: {} (the ONE sample \
             this iteration took, which is also the sample the final status check was given — \
             this path takes no further scan)",
            self.stage_spent,
            self.deadline,
            self.data_dir.display(),
            self.artifacts_now
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
/// (roborev job 232 finding 1, and the OVERRULE recorded in tasks.md round 9) —
/// and that holds in BOTH directions (job 233 finding 1): an expiry is declared
/// only after a FINAL NON-BLOCKING `step(ZERO)` confirms the evidence is still
/// absent, because evidence that arrived in time and was merely not consumed yet
/// is evidence in hand.
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
/// THE OVERRUN IS BOUNDED BUT NOT TINY, AND THE BOUND IS ENFORCED BY STRUCTURE
/// (roborev job 236, finding 2). The instant this loop decides can lag the
/// deadline by at most one `SLICE.min(remaining)` (<= 100ms) plus ONE
/// `count_data_db` scan — and that scan is a recursive `read_dir` walk of the
/// data directory, which on a loaded host is not necessarily quick. The same lag
/// applies to the FAILURE path, which is declared at the next loop top rather
/// than the instant the deadline passes; `PollFail` reports the stage's real
/// spend, so the message never understates it.
///
/// THAT CLAIM WAS FALSE THREE TIMES BEFORE IT WAS MADE TRUE, AND IT IS THE CLAIM
/// THAT GETS FIXED RATHER THAN WEAKENED. It was rescoped in round 9 and was still
/// wrong: the iteration scanned, the artifact `step` scanned again on its own,
/// `step(ZERO)` scanned a third time at expiry, and the failure path a fourth — so
/// a post-deadline overrun of FOUR walks was possible while the comment promised
/// one. Round 11 removed three of those and left the fourth (roborev job 243,
/// finding 2): the BASELINE scan was taken and then the loop immediately scanned
/// AGAIN before its first deadline check, so a poll entered at or past expiry
/// still walked the directory TWICE. The claim was already weakened once, in
/// round 9; it is not weakened again.
///
/// SO THE BASELINE **IS** ITERATION 0'S SAMPLE, and every later iteration's sample
/// is taken at the BOTTOM of the loop — only after that iteration has confirmed
/// the deadline had NOT yet passed. Consequences, which are the whole point:
/// * exactly ONE sample per iteration, asserted in `harness_tests.rs` through the
///   `sample` seam rather than argued from reading this loop;
/// * a poll entered ALREADY EXPIRED takes exactly ONE scan, not two;
/// * at most ONE scan is ever taken after the deadline lapses, which is what
///   makes the bound above true.
///
/// The sample is handed to `step` as its second argument, so a `step` that decides
/// on durable artifacts MUST use the count it is given rather than scanning for
/// itself; the expiry check reuses that same sample, and `PollFail` carries it to
/// the call site so even the failure message takes no further scan.
///
/// So read every "nothing may exceed the deadline" claim in this harness as a
/// statement about the timeout ARITHMETIC — no wait is granted, or started, past
/// the deadline — never as a wall-clock guarantee about the instant a verdict is
/// returned.
pub fn poll_with_progress<T>(
    io: &ChildIo,
    data_dir: &Path,
    stage: &Stage,
    step: impl FnMut(Duration, usize) -> Option<T>,
) -> PollOutcome<T> {
    poll_with_progress_sampled(io, data_dir, || count_data_db(data_dir), stage, || {}, step)
}

/// [`poll_with_progress`], but it PERFORMS the operation whose effects it observes
/// — **AFTER establishing its observation window, never before** (#3652, roborev
/// job 262 finding 2).
///
/// **THE DEFECT THIS EXISTS TO MAKE UNSPELLABLE.** A poll's window is a transcript
/// [`Mark`] plus an artifact baseline, both taken inside the poll. So a caller that
/// performed the triggering operation — closing the child's stdin, delivering a
/// signal — and *then* called the poll had its window open AFTER the operation:
/// every line the child emitted, and every artifact it created, in that gap was
/// outside the window, and a timeout could report `progress observed: NONE — 0 new
/// output lines and 0 new durable artifacts` against evidence the harness had
/// already produced. That is the same shape as roborev job 253 finding 2 (a `t_ack`
/// timer started after the write it measured) and job 243 finding 1 (a `Mark` taken
/// after the `writeln!` whose response it awaited); [`Mark`] states the rule, and
/// this makes the poll's own version of it structural rather than a convention a
/// call site has to remember.
///
/// **THE RESIDUAL, STATED WHERE IT IS PAID.** The caller must still refuse to
/// INITIATE the operation past the one deadline (`require_live_or_kill`, roborev job
/// 253 finding 2), and that check necessarily happens BEFORE this call — so it now
/// precedes the window establishment (one `mark()`, i.e. a lock acquisition, and one
/// artifact scan) rather than the trigger itself. Both of those reads wait for
/// nothing, and the alternative is the defect above.
pub fn poll_with_progress_triggered<T>(
    io: &ChildIo,
    data_dir: &Path,
    stage: &Stage,
    trigger: impl FnOnce(),
    step: impl FnMut(Duration, usize) -> Option<T>,
) -> PollOutcome<T> {
    poll_with_progress_sampled(
        io,
        data_dir,
        || count_data_db(data_dir),
        stage,
        trigger,
        step,
    )
}

/// [`poll_with_progress`] with the artifact sample supplied by the caller.
///
/// The seam exists so the "exactly one scan per iteration, at most one past the
/// deadline" bound documented above can be MEASURED by a test instead of argued
/// from reading the loop (roborev job 243, finding 2: the previous version of that
/// bound was documented, believed and false). Production call sites use
/// [`poll_with_progress`], which supplies `count_data_db`; nothing else may pass
/// its own sampler, which is why this is private.
fn poll_with_progress_sampled<T>(
    io: &ChildIo,
    data_dir: &Path,
    mut sample: impl FnMut() -> usize,
    stage: &Stage,
    trigger: impl FnOnce(),
    mut step: impl FnMut(Duration, usize) -> Option<T>,
) -> PollOutcome<T> {
    const SLICE: Duration = Duration::from_millis(100);
    let started = Instant::now();
    // The poll's own window into the ONE store, taken BEFORE its first sample or
    // step, so "new records" means records sequenced during THIS poll (job 243,
    // finding 1 — the mark precedes the operation, and here the poll IS the
    // operation).
    let mark = io.mark();
    // The baseline, taken before any step runs: `new_artifacts` counts what
    // appeared DURING the poll, so iteration 0 must have something to differ
    // from. It is ALSO iteration 0's sample — taking a second scan at the first
    // loop top is the redundant walk job 243 finding 2 found.
    let mut prev_artifacts = sample();
    let mut artifacts = prev_artifacts;
    let mut new_artifacts = 0usize;
    // When the artifact count last increased. The OTHER half of "last progress"
    // — new output — is derived from the snapshot below rather than tracked here,
    // which is what makes the two halves of the progress report come from one
    // read (design.md D6b; job 247, finding 3).
    let mut last_artifact_at: Option<Instant> = None;
    // THE TRIGGERING OPERATION, AFTER THE WINDOW AND BEFORE THE FIRST STEP
    // (#3652, job 262 finding 2). Everything the operation causes — a line, an
    // artifact — is therefore inside the window this poll decides and reports
    // from. `poll_with_progress` passes a no-op: a poll with no operation of its
    // own observes one issued by an earlier stage, which opened its own window
    // before it.
    trigger();

    loop {
        // THE ITERATION'S ONE SAMPLE OF EACH SIGNAL, reused by everything below
        // it: the progress accounting, `step`, the expiry status check and the
        // failure message. Nothing downstream re-scans, which is what makes the
        // documented overrun bound above true rather than aspirational.
        if artifacts > prev_artifacts {
            new_artifacts += artifacts - prev_artifacts;
            prev_artifacts = artifacts;
            last_artifact_at = Some(Instant::now());
        }
        // NOTHING NEEDS DRAINING ANY MORE. The reader threads append straight into
        // the one store, so a chatty child cannot fill a pipe buffer waiting for
        // this loop to consume a queue — which is the only reason the deleted
        // channel had to be drained here at all. EOF on both pipes is still a FACT
        // the failure reports and never a reason to stop polling: the child may be
        // exiting, and `step` is what observes that (job 243, finding 3).
        let remaining = stage.remaining();
        if remaining.is_zero() {
            // FINAL NON-BLOCKING STATUS CHECK BEFORE DECLARING A TIMEOUT (roborev
            // job 233, finding 1). The child may have EXITED, or the artifact may
            // have APPEARED, before the deadline and simply not have been observed
            // yet: this thread sleeps a slice at a time and can be descheduled
            // arbitrarily long past the end of one. `step(ZERO, ..)` waits for
            // nothing — `wait_timeout(ZERO)` is a `try_wait`, and an artifact
            // predicate reads the sample above rather than scanning — so this
            // cannot extend the deadline; it only consumes evidence that already
            // arrived within it.
            if let Some(done) = step(Duration::ZERO, artifacts) {
                return Ok((done, stage.spent()));
            }
            // ONE read of the one store for the verdict: it supplies the reported
            // record count, the last-progress instant, the end-of-stream fact and
            // the rendered transcript, so none of the four can disagree with
            // another and no later append can contradict the message (job 243
            // finding 1; job 247 finding 3).
            let snapshot = io.snapshot(mark);
            let last_progress = [snapshot.newest_at(), last_artifact_at]
                .into_iter()
                .flatten()
                .max()
                .unwrap_or(started);
            return Err(Box::new(PollFail {
                stage_spent: stage.spent(),
                deadline: stage.describe(),
                since_progress: last_progress.elapsed(),
                new_lines: snapshot.examined(),
                new_artifacts,
                artifacts_now: artifacts,
                pipes: snapshot.pipe_status(),
                data_dir: data_dir.to_path_buf(),
                snapshot,
            }));
        }
        if let Some(done) = step(SLICE.min(remaining), artifacts) {
            return Ok((done, stage.spent()));
        }
        // THE NEXT ITERATION'S SAMPLE, taken at the BOTTOM: this iteration has
        // already established that the deadline had not passed, so this scan is
        // charged to a live poll. Taking it at the loop TOP instead is what let an
        // already-expired poll walk the directory twice (job 243, finding 2).
        artifacts = sample();
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

/// THE READ SIDE'S ONE STORE: stage (e)'s two output buffers plus how many
/// collector threads are still attached.
///
/// The same shape as the transcript log, for the same reason (design.md D6b). This
/// site carried its OWN `mpsc` channel and its own copy of the
/// `Empty`-vs-`Disconnected` defect (roborev job 243, finding 3, found here after
/// round 11 fixed it in `ChildIo`) — the class stops recurring when there is no
/// queue to mis-classify: "both buffers are here" and "every collector has ended"
/// are two fields of one store, read under one lock.
#[derive(Debug, Default)]
struct StreamBufs {
    stdout: Option<Vec<u8>>,
    stderr: Option<Vec<u8>>,
    /// Collector threads still attached; `0` means every one has ended, whether by
    /// delivering or by unwinding.
    collectors_open: usize,
    /// **COLLECTORS WHOSE `read_to_end` RETURNED `Err`, with how much they had read**
    /// (roborev job 255, finding 2). The result used to be discarded — `let _ =
    /// reader.read_to_end(..)` — and the partial buffer was then DELIVERED as
    /// though it were the whole stream, so a truncated read was presented as a
    /// complete one. A collector that fails now records the failure instead of
    /// delivering, and the collection propagates it.
    read_failures: Vec<(Stream, String)>,
}

impl StreamBufs {
    fn collected(&self) -> usize {
        usize::from(self.stdout.is_some()) + usize::from(self.stderr.is_some())
    }
}

/// A collector's handle on that store: the only way a buffer is delivered.
///
/// End-of-collection is signalled by DROP — normal return AND unwind — which is
/// the `Sender`-drop semantics the deleted channel had, kept deliberately: a
/// collector that panics must not leave the wait believing a buffer is still
/// coming.
struct BufHandle {
    bufs: Arc<Mutex<StreamBufs>>,
}

impl BufHandle {
    /// **THIS COLLECTOR COULD NOT READ ITS PIPE TO THE END** (roborev job 255,
    /// finding 2), so it has nothing whole to deliver and delivers NOTHING: a
    /// partial buffer presented as complete is a false statement about the child's
    /// output, where a recorded failure is a true one about this harness.
    fn read_failed(&self, stream: Stream, error: impl std::fmt::Display, read_so_far: usize) {
        if let Ok(mut b) = self.bufs.lock() {
            b.read_failures.push((
                stream,
                format!("{error} (after {read_so_far} byte(s) had been read)"),
            ));
        }
    }

    fn deliver(&self, stream: Stream, buf: Vec<u8>) {
        if let Ok(mut b) = self.bufs.lock() {
            match stream {
                Stream::Stdout => b.stdout = Some(buf),
                Stream::Stderr => b.stderr = Some(buf),
            }
        }
    }
}

impl Drop for BufHandle {
    fn drop(&mut self) {
        if let Ok(mut b) = self.bufs.lock() {
            b.collectors_open = b.collectors_open.saturating_sub(1);
        }
    }
}

/// Read a piped handle to EOF on its own thread, so a bounded wait on the child
/// can never deadlock against a full pipe buffer.
fn collect_to_end<R: std::io::Read + Send + 'static>(
    stream: Stream,
    mut reader: R,
    handle: BufHandle,
) {
    thread::spawn(move || {
        let handle = handle;
        let mut buf = Vec::new();
        // THE TERMINAL RESULT DECIDES WHETHER THERE IS ANYTHING TO DELIVER (roborev
        // job 255, finding 2). Discarding it delivered whatever had been read as
        // the WHOLE stream.
        match reader.read_to_end(&mut buf) {
            Ok(_) => handle.deliver(stream, buf),
            Err(error) => handle.read_failed(stream, error, buf.len()),
        }
    });
}

/// What [`collect_both_streams`] ended with. An enum rather than a panic inside
/// the loop so the collection is unit-testable: the diagnostics stay at the call
/// site, which owns the stage-(e) context they report.
enum CollectEnd {
    /// Both streams delivered, in `(stdout, stderr)` order.
    Both(Vec<u8>, Vec<u8>),
    /// The deadline passed with `collected` of 2 delivered.
    DeadlineReached { collected: usize },
    /// Every collector thread ended with `collected` of 2 delivered, so no further
    /// buffer can arrive. A collector that reached EOF always delivers, so this can
    /// only be a panic inside the harness.
    CollectorsEnded { collected: usize },
    /// A collector's `read_to_end` returned `Err`, so its stream was never read to
    /// the end (roborev job 255, finding 2). DISTINCT from every variant above: the
    /// deadline was not reached, no collector merely ended, and the lock was
    /// readable — this harness could not read the pipe, and the partial buffer is
    /// deliberately NOT presented as the stream's output.
    ReadFailed { failures: String, collected: usize },
    /// The store's lock was poisoned — a collector panicked while holding it. A
    /// harness defect, and a DIFFERENT one from `CollectorsEnded`, so it is not
    /// folded into it.
    Unavailable,
}

/// ONE read of the read-side store: it decides AND extracts, under a single lock,
/// so "both buffers are here" and "every collector has ended" can never come from
/// different moments.
enum Look {
    Both(Vec<u8>, Vec<u8>),
    Partial {
        collected: usize,
        collectors_open: usize,
    },
    ReadFailed {
        failures: String,
        collected: usize,
    },
    Unavailable,
}

fn look(bufs: &Mutex<StreamBufs>) -> Look {
    match bufs.lock() {
        Ok(mut b) => {
            // CHECKED FIRST, under the same lock as everything else (job 255,
            // finding 2): a stream that could not be read to its end must never be
            // reported through a variant that says the collection succeeded, nor
            // blamed on the deadline or on a collector merely ending.
            if !b.read_failures.is_empty() {
                let failures = b
                    .read_failures
                    .iter()
                    .map(|(stream, error)| format!("{stream:?} collector: {error}"))
                    .collect::<Vec<_>>()
                    .join("; ");
                return Look::ReadFailed {
                    failures,
                    collected: b.collected(),
                };
            }
            if b.collected() == 2 {
                let out = b.stdout.take().unwrap_or_default();
                let err = b.stderr.take().unwrap_or_default();
                Look::Both(out, err)
            } else {
                Look::Partial {
                    collected: b.collected(),
                    collectors_open: b.collectors_open,
                }
            }
        }
        Err(_) => Look::Unavailable,
    }
}

/// Collect both of the read-side child's output buffers under the TEST's one
/// deadline (`stage.remaining()`: no constant, no per-call-site subtraction).
///
/// EXACTLY ONE READ OF THE STORE PER ITERATION, and the expiry verdict comes from a
/// read taken AFTER the deadline lapsed (roborev job 233, finding 1). A collector
/// can deliver its buffer before the deadline and this thread be descheduled before
/// looking; declaring a timeout without one last look would be a false failure
/// reported against a child that had already exited successfully. That look waits
/// for nothing, so it cannot extend the deadline.
fn collect_both_streams(bufs: &Arc<Mutex<StreamBufs>>, stage: &Stage) -> CollectEnd {
    loop {
        let left = stage.remaining();
        if left.is_zero() {
            return match look(bufs) {
                Look::Both(out, err) => CollectEnd::Both(out, err),
                Look::ReadFailed {
                    failures,
                    collected,
                } => CollectEnd::ReadFailed {
                    failures,
                    collected,
                },
                Look::Unavailable => CollectEnd::Unavailable,
                // The cause comes from the SAME read as the count, so a collection
                // that ran out of COLLECTORS is never reported as one that ran out
                // of TIME (job 243, finding 3 — the class this store retires).
                Look::Partial {
                    collected,
                    collectors_open: 0,
                } => CollectEnd::CollectorsEnded { collected },
                Look::Partial { collected, .. } => CollectEnd::DeadlineReached { collected },
            };
        }
        match look(bufs) {
            Look::Both(out, err) => return CollectEnd::Both(out, err),
            Look::ReadFailed {
                failures,
                collected,
            } => {
                return CollectEnd::ReadFailed {
                    failures,
                    collected,
                }
            }
            Look::Unavailable => return CollectEnd::Unavailable,
            Look::Partial {
                collected,
                collectors_open: 0,
            } => return CollectEnd::CollectorsEnded { collected },
            Look::Partial { .. } => {}
        }
        thread::sleep(Duration::from_millis(2).min(left));
    }
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
/// THIS STAGE TAKES TWO BOUNDED WAITS — `wait_timeout` on the child, and the pipe
/// collection — PLUS A CHARGED SPAWN, and it was the site of three separate
/// findings, all the same defect: each wait separately received the stage's full
/// budget, so the stage could consume a multiple of its own declared cap. Every
/// wait below takes `stage.remaining()`, which is the TEST's one deadline: the
/// spawn is charged, the collection gets only what the child wait left, and there
/// is no per-call-site subtraction left to forget.
///
/// TWO IS ALSO WHAT THE WAIT CENSUS DECLARES for `e.durability-read`
/// (`budgets.rs`): a wait counted there is one that is GRANTED `stage.remaining()`,
/// and the spawn is charged without being granted a bound of its own. If a third
/// bounded wait is ever added here, the census entry has to change with it.
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
    // Nothing new may be INITIATED once the one deadline has passed (job 253,
    // finding 2): a read-side child spawned after expiry can still exit and
    // deliver its rows, which is fresh evidence produced after the sole bound.
    stage.require_live("the read-side `cqlite --execute` spawn");
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
    let bufs = Arc::new(Mutex::new(StreamBufs {
        collectors_open: 2,
        ..StreamBufs::default()
    }));
    collect_to_end(
        Stream::Stdout,
        child.stdout.take().expect("read-side stdout"),
        BufHandle {
            bufs: Arc::clone(&bufs),
        },
    );
    collect_to_end(
        Stream::Stderr,
        child.stderr.take().expect("read-side stderr"),
        BufHandle {
            bufs: Arc::clone(&bufs),
        },
    );

    let status = match child
        .wait_timeout(stage.remaining())
        .expect("wait_timeout on read-side cqlite")
    {
        Some(status) => status,
        None => {
            // Killed AND REAPED (#3652): the read-side child is the one being
            // diagnosed, and leaving it unreaped leaves its two collector threads
            // blocked on its pipes for the rest of the binary.
            let teardown = kill_and_reap(&mut child);
            panic!(
                "stage (e) durability-read: the read-side `cqlite --execute` child did not exit.\n\
                 query: `{query}`\n\
                 data dir: {}\n\
                 {}\n\
                 {teardown}\n\
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

    // The child has exited, so both pipes are at EOF and the collector threads
    // finish promptly — but "promptly" is a claim about SCHEDULING, and a collector
    // thread on a saturated host can stay descheduled for seconds. This was once a
    // hardcoded `recv_timeout(5s)`: a NEW, uncalibrated wall-clock bound that could
    // false-fail under exactly the contention #3515 is about (roborev job 219,
    // finding 2). It then became a hand-computed `budget.derived - elapsed`, which
    // is the arithmetic that produced job 222 finding 1 and job 224 finding 2.
    //
    // It is now `stage.remaining()`: the test's one deadline. No constant, no
    // subtraction, and nothing for a future edit here to get wrong.
    let (stdout_buf, stderr_buf) = match collect_both_streams(&bufs, stage) {
        CollectEnd::Both(out, err) => (out, err),
        CollectEnd::DeadlineReached { collected } => panic!(
            "stage (e) durability-read: the read-side child exited ({status:?}) but only \
             {collected}/2 of its output streams could be collected before the test's \
             deadline (the spawn and the child wait had already taken {child_wait:.2?} of \
             this stage).\n\
             {}\n\
             WHAT THIS ESTABLISHES: only that a collector thread had not delivered its buffer \
             in time. It says nothing about durability, and nothing about the child, which \
             exited successfully.\n{}",
            stage.describe(),
            stage.report()
        ),
        // Not a timeout: every collector thread ended without delivering both
        // buffers, which can only be a panic inside the harness.
        CollectEnd::CollectorsEnded { collected } => panic!(
            "stage (e) durability-read: every read-side collector thread ended with only \
             {collected}/2 streams delivered — a collector ended without delivering its \
             buffer. This is a defect in this test harness, not a statement about \
             durability.\n{}",
            stage.report()
        ),
        // Not a timeout and not a whole stream: a collector could not read its pipe
        // to the end, so the rows below would have been parsed out of a TRUNCATED
        // buffer had the partial output been delivered as complete (job 255,
        // finding 2).
        CollectEnd::ReadFailed {
            failures,
            collected,
        } => panic!(
            "stage (e) durability-read: a read-side collector could not read its stream to the \
             end ({collected}/2 streams collected whole): {failures}.\n\
             The partial buffer is deliberately NOT used: rows parsed out of a truncated stream \
             would be a statement about this harness's read, not about what the child emitted.\n\
             WHAT THIS ESTABLISHES: only that this harness failed to read a pipe. It says nothing \
             about durability, and nothing about the child, which exited ({status:?}).\n{}",
            stage.report()
        ),
        CollectEnd::Unavailable => panic!(
            "stage (e) durability-read: the read-side buffer store's lock was poisoned — a \
             collector thread panicked while holding it. This is a defect in this test \
             harness, not a statement about durability.\n{}",
            stage.report()
        ),
    };

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
    // Nothing new may be INITIATED once the one deadline has passed (roborev job
    // 253, finding 2): a child spawned after expiry can still print the readiness
    // banner this stage awaits. There is no child to clean up yet, so this site
    // panics directly rather than going through `require_live_or_kill`.
    stage.require_live("the interactive writable session spawn");
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cqlite interactive writable session");
    // The mark comes back from `attach`, taken before either reader existed — the
    // earliest point at which one can be taken (job 243, finding 1).
    let (io, mark) = ChildIo::attach(&mut child);

    let ready = io.wait_for(
        mark,
        Stream::Stderr,
        |l| l.contains(MARKER_SESSION_READY),
        &stage,
    );
    if let Err(end) = &ready {
        // Killed AND REAPED through the one teardown (#3652).
        let teardown = kill_and_reap(&mut child);
        panic!(
            "stage (a) session-up: the readiness banner was not observed on the child's stderr.\n\
             awaited substring on stderr: {MARKER_SESSION_READY:?}\n\
             {}\n\
             {}\n\
             {teardown}\n\
             WHAT THIS ESTABLISHES: only that the banner was not observed before the deadline on \
             THIS host. It does NOT distinguish a child that never reached the interactive loop \
             from one that was never scheduled, nor either of those from drift in the product's \
             banner text.\n\
             child transcript (the snapshot the verdict was taken from):\n{}\n{}",
            stage.describe(),
            end.describe(),
            end.transcript(),
            stage.report()
        );
    }
    // `t_boot` is the stage's own spend, which starts before the spawn (above).
    let t_boot = stage.finish();
    (child, io, t_boot)
}

/// **THE ONE PLACE A CHILD IS TORN DOWN ON A FAILURE PATH: KILL *AND* REAP**
/// (#3652, roborev job 265 finding 4).
///
/// Every failure site in this harness used to spell this `let _ = child.kill();`,
/// which sends the signal and never waits — so the child it was diagnosing was
/// left as a ZOMBIE for the rest of the test binary, and its reader threads stayed
/// blocked on pipes nobody would ever close. `await_write_ack` did not even do
/// that: it panicked with the interactive child — *the stalled process being
/// diagnosed* — still running.
///
/// So the teardown is ONE function, and it returns a SENTENCE rather than
/// printing: the caller owns its failure message, and what happened to the child
/// is part of the evidence that message carries. It is deliberately reported
/// rather than asserted — a kill that fails because the child had already exited
/// is not a defect, and neither is one that fails because the test is already
/// failing for another reason.
///
/// `wait()` is unbounded ON PURPOSE and cannot hang here: `Child::kill` sends
/// `SIGKILL`, which no process can catch, block or ignore, so the only wait is for
/// the kernel to reap. A bounded wait would need a fresh uncalibrated wall-clock
/// constant — exactly what #3515 removed from this harness — to buy nothing.
pub fn kill_and_reap(child: &mut std::process::Child) -> String {
    let pid = child.id();
    let killed = child.kill();
    match child.wait() {
        Ok(status) => format!(
            "child teardown: pid {pid} was killed and REAPED before this failure was reported (it \
             ended {status:?}), so it is not left running and its pipe readers are released{}",
            match killed {
                Ok(()) => String::new(),
                // Not a defect: `kill` reports an error for a child that had
                // already exited, which is the common case on a failure path.
                Err(error) =>
                    format!(" — the kill itself reported {error} (it had already exited)"),
            }
        ),
        Err(error) => format!(
            "child teardown: pid {pid} could NOT be reaped ({error}), so it may still be running. \
             This is a defect in this test harness and NOT a statement about the property under \
             test{}",
            match killed {
                Ok(()) => String::new(),
                Err(kill_error) => format!("; the kill reported {kill_error}"),
            }
        ),
    }
}

/// **REFUSE TO INITIATE NEW EVIDENCE-PRODUCING WORK PAST THE ONE DEADLINE**, and
/// kill the child before failing so a post-expiry failure does not leak it
/// (roborev job 253, finding 2).
///
/// The rule and its scope live on [`Stage::check_live`]: the deadline never
/// stopped the test ACCEPTING evidence already in hand — that is the round-9
/// ruling and it is untouched — but an operation ISSUED after expiry can
/// MANUFACTURE evidence a final look then accepts. Call this immediately before
/// each `writeln!`, `libc::kill`, spawn and stdin `drop`.
pub fn require_live_or_kill(
    stage: &Stage,
    io: &ChildIo,
    child: &mut std::process::Child,
    what: &str,
) {
    if let Err(expired) = stage.check_live(what) {
        // Killed AND REAPED through the one teardown, and what happened to it is
        // part of the message (#3652): `let _ = child.kill()` left a zombie and
        // its reader threads attached.
        let teardown = kill_and_reap(child);
        panic!(
            "{}\n{teardown}\nchild transcript:\n{}\n{}",
            expired.describe(),
            io.transcript_text(),
            stage.report()
        );
    }
}

/// Wait for a write acknowledgement (`OK` on stdout). Returns how long the
/// round-trip took.
///
/// `mark` MUST have been taken before the statement was written to the child's
/// stdin (job 243, finding 1): the ack can be recorded by a reader thread and left
/// unpublished, and a mark taken after the `writeln!` excludes exactly that line
/// from the expiry check's window.
///
/// Shared by both tests. The failure reports what it awaited, how the one deadline
/// was derived, and what the child actually said. It does NOT conclude that the
/// session dead-ended, nor that no interactive writable session exists (the two
/// causes the retired messages named), neither of which a timeout establishes.
///
/// **IT TAKES THE CHILD MUTABLY SO THE FAILURE PATH CAN TEAR IT DOWN** (#3652,
/// roborev job 265 finding 4). A missing acknowledgement is the signature of a
/// STALLED interactive child, and this function used to panic with that child
/// still running — leaking the very process it was diagnosing, plus the two reader
/// threads blocked on its pipes, into the rest of the test binary. Every other
/// failure site in this harness already took the child; this one is now the same
/// shape, through the same [`kill_and_reap`].
pub fn await_write_ack(
    io: &ChildIo,
    mark: Mark,
    what: &str,
    stage: &Stage,
    child: &mut std::process::Child,
) -> Duration {
    match io.wait_for(mark, Stream::Stdout, |l| l.trim() == "OK", stage) {
        Ok((_, took)) => took,
        Err(end) => {
            let teardown = kill_and_reap(child);
            panic!(
                "stage {}: {what} was not acknowledged with `OK` on the child's stdout.\n\
                 awaited on stdout: a line whose trimmed text is exactly \"OK\"\n\
                 {}\n\
                 {}\n\
                 {teardown}\n\
                 WHAT THIS ESTABLISHES: only that no acknowledgement was observed before the \
                 test's deadline. It does NOT establish whether the write was rejected, is still \
                 in progress, was never read, or whether the child was descheduled — inspect the \
                 transcript below (the child prints `Error: ...` on stderr for a rejected \
                 statement).\n\
                 child transcript (the snapshot the verdict was taken from):\n{}\n{}",
                stage.name(),
                stage.describe(),
                end.describe(),
                end.transcript(),
                stage.report()
            )
        }
    }
}

#[cfg(test)]
mod harness_tests;
