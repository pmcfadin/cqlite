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

    /// The exact prefix `spawn_reader` writes into the transcript for this
    /// stream. It exists so the TRANSCRIPT SCAN and the transcript RENDER agree
    /// on attribution by construction: both derive it from here, so a line the
    /// failure message shows tagged `[stderr]` is a line the decision considered
    /// as stderr. Note the tag is the HARNESS's own framing, not the child's.
    fn transcript_prefix(self) -> &'static str {
        match self {
            Stream::Stdout => "[stdout] ",
            Stream::Stderr => "[stderr] ",
        }
    }
}

/// A position in the cumulative transcript, taken BEFORE the operation whose
/// response is awaited.
///
/// WHY THE CALLER OWNS IT (roborev job 243, finding 1). The mark bounds the window
/// a wait's expiry check may consider, so it must be taken before the operation
/// that can PRODUCE the awaited line — the spawn, the `writeln!`, the `kill`. Taken
/// inside the wait instead, a reader that RECORDED a fast response and was then
/// descheduled before publishing it left that line BEFORE the mark and OUTSIDE the
/// channel, so it was excluded from both halves of the expiry check: a false
/// timeout against evidence the harness already held.
///
/// It is a newtype, not a bare `usize`, so a call site cannot pass a length, an
/// index or a count where a mark belongs.
#[derive(Clone, Copy, Debug)]
pub struct Mark(usize);

/// ONE read of the transcript, used for BOTH the decision and the message.
///
/// DECIDE AND RENDER FROM THE SAME SNAPSHOT, NOT MERELY THE SAME STORE (roborev
/// job 243, finding 1). Round 11 made the expiry decision read the transcript
/// rather than the channel, but then RE-READ the transcript to render the failure
/// — two acquisitions of the same lock, so a line appended in between could still
/// appear in a message that had just called it absent. The two reads are now one:
/// the snapshot is copied out under a single lock, the verdict is taken from it,
/// and it is carried into [`WaitEnd`] so the rendered transcript is literally the
/// bytes the decision examined.
#[derive(Debug)]
pub struct TranscriptSnapshot {
    /// Every transcript line as of the decision, tags included.
    lines: Vec<String>,
    /// Where the awaiting wait began; `lines[mark..]` is its window.
    mark: usize,
    /// The lock was readable. `false` means a reader thread panicked, which is
    /// reported rather than rendered as an empty transcript.
    available: bool,
}

impl TranscriptSnapshot {
    /// How many lines the decision's window covered — how much evidence it
    /// actually looked at.
    fn examined(&self) -> usize {
        self.lines.len().saturating_sub(self.mark)
    }

    /// The lines in the window, on `want`, with the harness's `[stream] ` tag
    /// stripped so the caller receives the child's own text.
    ///
    /// The predicate is applied OUTSIDE any lock, by construction: this reads an
    /// owned copy. `pred` is caller code, and applying it while holding the
    /// transcript lock deadlocks any predicate that touches the transcript — a std
    /// `Mutex` is not reentrant. Found the hard way: the first version of a plant
    /// for this did exactly that and wedged the test binary for nine minutes.
    fn window_on(&self, want: Stream) -> impl Iterator<Item = &str> {
        self.lines
            .get(self.mark..)
            .unwrap_or(&[])
            .iter()
            .filter_map(move |l| l.strip_prefix(want.transcript_prefix()))
    }

    /// The snapshot, indented for a panic message. THE SAME BYTES the verdict was
    /// taken from — no fresh read, so no append can contradict the message.
    pub fn render(&self) -> String {
        if !self.available {
            return "  (transcript unavailable: a reader thread panicked)".to_string();
        }
        if self.lines.is_empty() {
            return "  (the child emitted nothing at all on stdout or stderr)".to_string();
        }
        self.lines
            .iter()
            .map(|l| format!("  {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// How a [`ChildIo::wait_for`] ended when it did NOT observe the line it awaited.
///
/// Reported by every such failure because it is an OBSERVATION, not a cause: a
/// deadline that passed with the pipes still open and a child whose pipes reached
/// EOF are different measurements, and the second is the signature a RED run
/// produces when the child dies instead of handling the signal. Neither variant
/// names WHY.
///
/// BOTH variants carry the snapshot the verdict was taken from, and the call site
/// renders THAT rather than re-reading the transcript, so the message and the
/// decision are the same bytes (job 243, finding 1).
#[derive(Debug)]
pub enum WaitEnd {
    /// The test's one deadline passed; the child's pipes were still open, so more
    /// output was still possible.
    DeadlineReached { snapshot: TranscriptSnapshot },
    /// Both reader threads ended and the queue drained: the child's stdout AND
    /// stderr reached EOF after this long, so no further line could ever arrive.
    /// The child had exited, crashed, or closed its pipes -- this does not say
    /// which.
    PipesClosed {
        after: Duration,
        snapshot: TranscriptSnapshot,
    },
}

impl WaitEnd {
    pub fn describe(&self) -> String {
        match self {
            WaitEnd::DeadlineReached { snapshot } => format!(
                "how the wait ended: the test's one deadline passed with the child's pipes still \
                 open (more output was still possible). The verdict was taken from ONE snapshot \
                 of the transcript covering the {} line(s) recorded since this wait began, and \
                 none of them matched. The transcript printed below IS that snapshot — not a \
                 fresh read of a store that may since have grown — so this message cannot be \
                 contradicted by the evidence it prints",
                snapshot.examined()
            ),
            WaitEnd::PipesClosed { after, snapshot } => format!(
                "how the wait ended: the child's stdout AND stderr both reached EOF after \
                 {after:.3?}, so no further line could arrive: the child had exited, crashed, or \
                 closed its pipes (this measurement does not say which). The {} transcript \
                 line(s) recorded since this wait began, printed below, are the snapshot the \
                 verdict was taken from",
                snapshot.examined()
            ),
        }
    }

    /// The transcript the DECISION examined, ready for a panic message.
    pub fn transcript(&self) -> String {
        match self {
            WaitEnd::DeadlineReached { snapshot } => snapshot.render(),
            WaitEnd::PipesClosed { snapshot, .. } => snapshot.render(),
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

/// What a final non-blocking drain of the channel found. Two facts, kept
/// separate because collapsing them is exactly what reported the wrong cause
/// (roborev job 236, finding 3).
struct FinalDrain {
    /// The first queued line that matched, if any.
    matched: Option<String>,
    /// The queue was drained to the end and every sender was gone, so no further
    /// line can ever arrive. Never set when `matched` is `Some` (the drain stops
    /// there and learns nothing about the senders).
    disconnected: bool,
}

/// How a non-blocking [`ChildIo::drain_new`] ended. Kept as two values for the
/// same reason [`FinalDrain`] keeps its two facts separate: "nothing more is
/// queued right now" and "nothing more can ever arrive" are different
/// measurements, and collapsing them reports the first about a child in the
/// second state.
///
/// It deliberately does NOT report how many lines it consumed. The "new output
/// lines" a poll reports are counted from the TRANSCRIPT — the store its failure
/// message renders — so a channel-derived count here would be a second store for
/// the message to disagree with (job 243, finding 1).
#[derive(PartialEq, Eq, Debug)]
enum DrainEnd {
    /// The queue was emptied and the senders are alive: more output is possible.
    Empty,
    /// The queue was emptied AND every sender was gone: both reader threads have
    /// ended, so the child's pipes are at EOF.
    Disconnected,
}

impl ChildIo {
    /// Attach readers to a spawned child's stdout + stderr, returning the harness
    /// AND the [`Mark`] for the first wait.
    ///
    /// THE MARK IS TAKEN BEFORE EITHER READER EXISTS, which is the earliest point
    /// at which a mark can be taken at all: until a reader is spawned, nothing can
    /// record into the transcript. So the first wait's window provably covers every
    /// line the child has ever emitted, and the "recorded a fast response, then got
    /// descheduled before the mark" race (job 243, finding 1) is not expressible
    /// for stage (a). Returning it — rather than letting the call site take one
    /// after `attach` — is what makes that structural instead of a convention.
    fn attach(child: &mut std::process::Child) -> (Self, Mark) {
        let transcript: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::channel();
        let io = Self {
            rx,
            transcript: Arc::clone(&transcript),
        };
        let mark = io.mark();
        let out = child.stdout.take().expect("child stdout");
        let err = child.stderr.take().expect("child stderr");
        spawn_reader(Stream::Stdout, out, tx.clone(), Arc::clone(&transcript));
        spawn_reader(Stream::Stderr, err, tx, transcript);
        (io, mark)
    }

    /// Take a transcript [`Mark`] for a wait that has NOT YET been started.
    ///
    /// CALL THIS BEFORE THE OPERATION WHOSE RESPONSE YOU WILL AWAIT — before the
    /// `writeln!`, before the `kill`. See [`Mark`] for what taking it later costs.
    pub fn mark(&self) -> Mark {
        Mark(self.transcript_len())
    }

    /// Block until a line on `want` satisfies `pred`, or the TEST's one deadline
    /// passes. Returns the matching line and how much of the stage it took (so a
    /// successful wait can calibrate the deadline).
    ///
    /// Takes the `Stage` itself, never a `Duration`: the timeout comes from
    /// `Stage::remaining()`, the one place a per-wait timeout is computed, so no
    /// call site can be handed a fresh allowance and none can double-spend.
    ///
    /// `mark` bounds the window the expiry check may consider. The transcript is
    /// CUMULATIVE across the whole test, so without a window an earlier stage's
    /// already-consumed line would satisfy a later wait — `await_write_ack` awaits
    /// five separate `OK`s in one test, so the first would silently satisfy all
    /// five, and a wedged session would read as green. A false PASS is strictly
    /// worse than a confusing diagnostic.
    ///
    /// THE MARK COMES FROM THE CALLER, TAKEN BEFORE THE OPERATION (job 243,
    /// finding 1). Taking it here made the window start AFTER the `writeln!` /
    /// `kill` that produces the awaited line, so a reader that recorded a fast
    /// response and was then descheduled before publishing it fell outside the
    /// window AND outside the channel — excluded from both halves of the expiry
    /// check.
    pub fn wait_for(
        &self,
        mark: Mark,
        want: Stream,
        pred: impl Fn(&str) -> bool,
        stage: &Stage,
    ) -> Result<(String, Duration), WaitEnd> {
        loop {
            let remaining = stage.remaining();
            if remaining.is_zero() {
                // DECIDE FROM THE STORE YOU REPORT FROM (roborev job 236,
                // finding 1 — the durable principle of this whole change).
                //
                // The awaited line may have ARRIVED before the deadline and not
                // have been consumed yet: this thread can be descheduled between
                // the reader thread's transcript `push` and its `send`, and again
                // between that `send` and this loop's next `recv_timeout`.
                // Declaring expiry without looking would be a false timeout on a
                // working product (the round-9 ruling: the deadline bounds how
                // long we WAIT FOR evidence, never whether we accept evidence we
                // already hold).
                //
                // Job 233's fix looked only at the CHANNEL, which narrowed the
                // window instead of closing it: `spawn_reader` records into the
                // transcript FIRST and publishes to the channel SECOND, so a
                // reader preempted between those two operations left the channel
                // without a line the transcript already had — and the failure
                // message renders the TRANSCRIPT. The message could therefore
                // print the very marker the decision had just called absent.
                //
                // The fix is not to synchronise the two stores; it is to make
                // their divergence IRRELEVANT by deciding from the one we report
                // from. The channel is still drained (it maintains the progress
                // counts and the ordering the blocking path depends on) and a
                // queued match still counts — every channel line is in the
                // transcript too, so that direction can never contradict the
                // message. The verdict of ABSENCE, though, is taken from the
                // transcript itself, so "the message prints evidence the decision
                // did not see" is impossible by construction: they read the same
                // bytes.
                //
                // Nothing here waits: the drain is `try_recv` and the scan is a
                // read of an in-memory `Vec`, so the deadline cannot be extended.
                let drained = self.final_drain(want, &pred);
                if let Some(line) = drained.matched {
                    return Ok((line, stage.spent()));
                }
                // ONE SNAPSHOT, used for the verdict AND for the message (job 243,
                // finding 1). Round 11 scanned the transcript and then re-read it
                // to render, which is two acquisitions of one lock: a line
                // appended in between appeared in a message that had just called
                // it absent. The snapshot below is the only read from here on.
                //
                // The transcript lines carry the harness's own `[stream] ` tag, so
                // the window strips it and the predicate sees the child's own text
                // — `await_write_ack` matches on the WHOLE trimmed line.
                let snapshot = self.snapshot(mark);
                if let Some(line) = snapshot.window_on(want).find(|l| pred(l)) {
                    return Ok((line.to_string(), stage.spent()));
                }
                // Only now can a cause be named, and the drain above is what
                // distinguishes them (roborev job 236, finding 3): a drain that
                // ran out of QUEUE reports a deadline, while one that ran out of
                // SENDERS reports closed pipes. Collapsing the two reported
                // "pipes still open" about a child whose readers had both ended.
                return Err(if drained.disconnected {
                    WaitEnd::PipesClosed {
                        after: stage.spent(),
                        snapshot,
                    }
                } else {
                    WaitEnd::DeadlineReached { snapshot }
                });
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
                    return Err(WaitEnd::PipesClosed {
                        after: stage.spent(),
                        // Taken here for the same reason as at expiry: the message
                        // must render the bytes this verdict was taken against.
                        snapshot: self.snapshot(mark),
                    });
                }
            }
        }
    }

    /// Non-blocking: consume every line the readers have already queued and
    /// return the first that matches, if any. Never waits, so it cannot extend
    /// the deadline; it only inspects evidence that has already arrived.
    ///
    /// Non-matching lines are discarded exactly as the blocking loop discards
    /// them — the transcript keeps every line, so nothing is lost to diagnostics.
    ///
    /// `disconnected` is reported ONLY when the queue was drained to the end
    /// without a match, because `try_recv` yields `Disconnected` exactly when the
    /// queue is empty AND every sender is gone. On a match the drain stops, so
    /// the remaining lines stay queued for the next stage (the blocking path
    /// behaves the same way) and nothing is claimed about the senders.
    fn final_drain(&self, want: Stream, pred: &impl Fn(&str) -> bool) -> FinalDrain {
        loop {
            match self.rx.try_recv() {
                Ok((stream, line)) => {
                    if stream == want && pred(&line) {
                        return FinalDrain {
                            matched: Some(line),
                            disconnected: false,
                        };
                    }
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return FinalDrain {
                        matched: None,
                        disconnected: false,
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return FinalDrain {
                        matched: None,
                        disconnected: true,
                    }
                }
            }
        }
    }

    /// How many lines the transcript holds, for a wait's own window mark.
    ///
    /// A poisoned transcript lock is reported as a length of 0, which makes the
    /// window cover everything a later scan can read: the scan itself would then
    /// find nothing, so this can only widen what is examined, never narrow it.
    fn transcript_len(&self) -> usize {
        self.transcript.lock().map(|t| t.len()).unwrap_or(0)
    }

    /// ONE read of the transcript, under ONE lock acquisition: the store the
    /// failure message renders, windowed at `mark`.
    ///
    /// Every expiry verdict and every message it produces comes from a value of
    /// this type, so "the message prints evidence the decision did not see" is not
    /// expressible (job 243, finding 1). Reads nothing but memory, so it cannot
    /// extend the deadline; and the whole transcript is COPIED, so the lock is
    /// released before any caller predicate runs (a std `Mutex` is not reentrant —
    /// applying a predicate that touches the transcript while holding it wedged the
    /// test binary for nine minutes once).
    fn snapshot(&self, mark: Mark) -> TranscriptSnapshot {
        match self.transcript.lock() {
            Ok(t) => TranscriptSnapshot {
                lines: t.clone(),
                mark: mark.0,
                available: true,
            },
            // A poisoned lock is REPORTED, never rendered as an empty transcript:
            // "the child said nothing" and "a reader thread panicked" are different
            // facts, and only one of them is about the child.
            Err(_) => TranscriptSnapshot {
                lines: Vec::new(),
                mark: 0,
                available: false,
            },
        }
    }

    /// Non-blocking: consume whatever the readers have queued.
    ///
    /// `EMPTY AND DISCONNECTED ARE NEVER COLLAPSED` (roborev job 236 finding 3
    /// and job 243 finding 3, the same class at two more sites). The old
    /// `while try_recv().is_ok()` stopped identically on either, so a poll could
    /// report "0 new output lines" about a child whose pipes had BOTH reached EOF
    /// — a materially different diagnosis, silently discarded. The queue is
    /// checked to the end before the disconnect is reported, which `try_recv`
    /// guarantees: it yields `Disconnected` only once the queue is empty AND every
    /// sender is gone.
    fn drain_new(&self) -> DrainEnd {
        loop {
            match self.rx.try_recv() {
                Ok(_) => {}
                Err(mpsc::TryRecvError::Empty) => return DrainEnd::Empty,
                Err(mpsc::TryRecvError::Disconnected) => return DrainEnd::Disconnected,
            }
        }
    }

    /// Everything the child has said so far, indented for a panic message.
    ///
    /// FOR CLAIMS THAT ARE NOT ABOUT THE TRANSCRIPT (job 243, finding 1). A wait
    /// or poll failure asserts that an awaited line is ABSENT, so its message must
    /// render the snapshot its verdict was taken from — `WaitEnd::transcript` /
    /// `PollFail::transcript` — or a later append could contradict it. An exit
    /// status or a missing row is a different claim, whose evidence is the status
    /// or the rows; the transcript is CONTEXT there, and a fresh read of it can
    /// contradict nothing.
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
    since_progress: Duration,
    /// New transcript lines since the poll began, COUNTED FROM THE SNAPSHOT below
    /// — the same bytes the message renders (job 243, finding 1). Counting the
    /// channel drains instead let the message report "0 new output lines" beside a
    /// printed transcript that showed some.
    new_lines: usize,
    new_artifacts: usize,
    /// The artifact count from the iteration that declared the timeout — THE SAME
    /// SAMPLE the final status check was given. Carried here so a call site can
    /// report it without taking another directory scan after the verdict
    /// (roborev job 236, finding 2).
    artifacts_now: usize,
    /// Both reader threads had ended when the verdict was taken, so the child's
    /// stdout AND stderr were at EOF. A separate FACT from the progress counts
    /// (roborev job 243, finding 3): a poll that gave up with the pipes closed is
    /// a different diagnosis from one that gave up with output still possible, and
    /// the old drain discarded the distinction.
    pipes_closed: bool,
    data_dir: PathBuf,
    /// The ONE transcript read taken at the verdict: both `new_lines` above and
    /// the rendered transcript come from it.
    snapshot: TranscriptSnapshot,
}

impl PollFail {
    /// The transcript the VERDICT was taken from, ready for a panic message.
    pub fn transcript(&self) -> String {
        self.snapshot.render()
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
        let pipes = if self.pipes_closed {
            "\nthe child's stdout AND stderr had BOTH reached EOF when the verdict was taken, so \
             no further output could arrive: the child had exited, crashed, or closed its pipes \
             (this measurement does not say which)"
        } else {
            "\nthe child's pipes were still open when the verdict was taken, so more output was \
             still possible"
        };
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
    poll_with_progress_sampled(io, data_dir, || count_data_db(data_dir), stage, step)
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
    mut step: impl FnMut(Duration, usize) -> Option<T>,
) -> PollOutcome<T> {
    const SLICE: Duration = Duration::from_millis(100);
    let mut last_progress = Instant::now();
    // The poll's own transcript window, taken BEFORE its first sample or step, so
    // "new output lines" means lines recorded during THIS poll (job 243, finding 1
    // — the mark precedes the operation, and here the poll IS the operation).
    let mark = io.mark();
    let mut prev_lines = io.transcript_len();
    // The baseline, taken before any step runs: `new_artifacts` counts what
    // appeared DURING the poll, so iteration 0 must have something to differ
    // from. It is ALSO iteration 0's sample — taking a second scan at the first
    // loop top is the redundant walk job 243 finding 2 found.
    let mut prev_artifacts = sample();
    let mut artifacts = prev_artifacts;
    let mut new_artifacts = 0usize;

    loop {
        // THE ITERATION'S ONE SAMPLE OF EACH SIGNAL, reused by everything below
        // it: the progress accounting, `step`, the expiry status check and the
        // failure message. Nothing downstream re-scans, which is what makes the
        // documented overrun bound above true rather than aspirational.
        if artifacts > prev_artifacts {
            new_artifacts += artifacts - prev_artifacts;
            prev_artifacts = artifacts;
            last_progress = Instant::now();
        }
        // The channel MUST still be drained — it is what keeps a chatty child from
        // filling the pipe buffer — but the progress COUNT is read from the
        // transcript, the store this poll's failure message renders.
        let drained = io.drain_new();
        let lines_now = io.transcript_len();
        if lines_now > prev_lines {
            prev_lines = lines_now;
            last_progress = Instant::now();
        }
        // EOF on both pipes is a FACT the failure reports, never a reason to stop
        // polling: the child may still be exiting, and `step` is what observes
        // that (roborev job 243, finding 3). It is read from THIS iteration's
        // drain — the one whose counts the verdict reports — so it is scoped to
        // the iteration rather than carried across them.

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
            //
            // The sample and the drain above were taken AFTER the deadline lapsed
            // and before this decision, so the progress counts describe the moment
            // the verdict is taken rather than one slice earlier — no separate
            // fold-in is needed, and none is taken.
            if let Some(done) = step(Duration::ZERO, artifacts) {
                return Ok((done, stage.spent()));
            }
            // ONE transcript read for the verdict: it supplies BOTH the reported
            // line count and the rendered transcript, so the two cannot disagree
            // and no later append can contradict the message (job 243, finding 1).
            let snapshot = io.snapshot(mark);
            return Err(Box::new(PollFail {
                stage_spent: stage.spent(),
                deadline: stage.describe(),
                since_progress: last_progress.elapsed(),
                new_lines: snapshot.examined(),
                new_artifacts,
                artifacts_now: artifacts,
                pipes_closed: drained == DrainEnd::Disconnected,
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

/// What [`collect_both_streams`] ended with. An enum rather than a panic inside
/// the loop so the collection is unit-testable: the diagnostics stay at the call
/// site, which owns the stage-(e) context they report.
enum CollectEnd {
    /// Both streams delivered, in `(stdout, stderr)` order.
    Both(Vec<u8>, Vec<u8>),
    /// The deadline passed with `collected` of 2 delivered.
    DeadlineReached { collected: usize },
    /// A reader thread ended without sending, with `collected` of 2 delivered.
    Disconnected { collected: usize },
}

/// Collect both of the read-side child's output buffers under the TEST's one
/// deadline (`stage.remaining()`: no constant, no per-call-site subtraction).
///
/// FINAL NON-BLOCKING DRAIN BEFORE DECLARING A TIMEOUT (roborev job 233,
/// finding 1). A reader thread can deliver its buffer before the deadline and
/// this thread can be descheduled before consuming it. Declaring a timeout
/// without one last `try_recv` would be a false failure on a working product,
/// reported against a child that had already exited successfully.
fn collect_both_streams(rx: &Receiver<(Stream, Vec<u8>)>, stage: &Stage) -> CollectEnd {
    fn store(
        stream: Stream,
        buf: Vec<u8>,
        stdout_buf: &mut Vec<u8>,
        stderr_buf: &mut Vec<u8>,
        collected: &mut usize,
    ) {
        match stream {
            Stream::Stdout => *stdout_buf = buf,
            Stream::Stderr => *stderr_buf = buf,
        }
        *collected += 1;
    }

    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    let mut collected = 0usize;
    loop {
        if collected >= 2 {
            return CollectEnd::Both(stdout_buf, stderr_buf);
        }
        let left = stage.remaining();
        if left.is_zero() {
            // Consume what already arrived; this waits for nothing, so it cannot
            // extend the deadline.
            //
            // EMPTY AND DISCONNECTED ARE NEVER COLLAPSED (roborev job 243,
            // finding 3 — the same class job 236 finding 3 fixed in
            // `ChildIo::final_drain`, still live here). The old `let Ok(..) else
            // break` stopped identically on either, so reader threads that had
            // ENDED without delivering both buffers were reported as
            // `DeadlineReached` — a timeout blamed on the deadline, about a
            // harness defect the `Disconnected` variant already exists to name.
            // The disconnect is recorded and the loop still terminates through
            // the same exit, so ALL QUEUED ITEMS have been checked before any
            // verdict is returned (`try_recv` yields `Disconnected` only once the
            // queue is empty AND every sender is gone).
            let mut disconnected = false;
            while collected < 2 {
                match rx.try_recv() {
                    Ok((stream, buf)) => store(
                        stream,
                        buf,
                        &mut stdout_buf,
                        &mut stderr_buf,
                        &mut collected,
                    ),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }
            return if collected >= 2 {
                CollectEnd::Both(stdout_buf, stderr_buf)
            } else if disconnected {
                CollectEnd::Disconnected { collected }
            } else {
                CollectEnd::DeadlineReached { collected }
            };
        }
        match rx.recv_timeout(left.min(Duration::from_millis(250))) {
            Ok((stream, buf)) => store(
                stream,
                buf,
                &mut stdout_buf,
                &mut stderr_buf,
                &mut collected,
            ),
            Err(RecvTimeoutError::Disconnected) => return CollectEnd::Disconnected { collected },
            Err(RecvTimeoutError::Timeout) => {}
        }
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
    let (stdout_buf, stderr_buf) = match collect_both_streams(&rx, stage) {
        CollectEnd::Both(out, err) => (out, err),
        CollectEnd::DeadlineReached { collected } => panic!(
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
        ),
        // Not a timeout: both reader threads dropped their senders without
        // sending, which can only be a panic inside the harness.
        CollectEnd::Disconnected { collected } => panic!(
            "stage (e) durability-read: the read-side output channel disconnected with only \
             {collected}/2 streams collected — a reader thread ended without sending. This \
             is a defect in this test harness, not a statement about durability.\n{}",
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
pub fn await_write_ack(io: &ChildIo, mark: Mark, what: &str, stage: &Stage) -> Duration {
    match io.wait_for(mark, Stream::Stdout, |l| l.trim() == "OK", stage) {
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
             child transcript (the snapshot the verdict was taken from):\n{}\n{}",
            stage.name(),
            stage.describe(),
            end.describe(),
            end.transcript(),
            stage.report()
        ),
    }
}

#[cfg(test)]
mod harness_tests;
