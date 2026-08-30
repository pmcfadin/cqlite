//! THE SINGLE STORE: one sequenced, mutex-guarded transcript log, and the waits
//! that read it (issues #1693, #3515; design.md D6b).
//!
//! **Why this file exists at all.** Rounds 11, 12 and 13 of review returned the
//! same two defect shapes at new sites every time — *evidence identity*
//! (transcript and channel diverge) and *channel-variant handling* (`Empty`
//! collapsed with `Disconnected`) — and both existed **only because there were
//! TWO stores**: a shared transcript `Vec` that a `Mark` could window, and an
//! `mpsc` queue that carried no sequence and so could not be windowed at all.
//! Each fix closed one site: decide from the same store (round 11), then from the
//! same *snapshot* (round 12), and then roborev job 247 found that a queue can
//! hold a COPY of a line the transcript has already served — so a stale `OK`
//! matched from the transcript could be re-delivered afterwards and accepted as
//! the NEXT write's acknowledgement. That is a vacuous pass in the sibling test's
//! five-write loop, not a diagnostic wart.
//!
//! Per the escalation rule written down before the round that triggered it
//! (design.md D6b), the channel is DELETED rather than patched a fifth time.
//! Reader threads append to ONE log; a [`Mark`] is a SEQUENCE POSITION in it;
//! every wait, every progress count and every rendering reads that one store from
//! the mark onward. What that buys, by construction and not by census:
//!
//! * **no divergence** — there is nothing for the transcript to diverge from;
//! * **no stale re-delivery** — a record is served from its seq, so a record the
//!   window excludes can never be re-presented to a later wait (a re-appearance
//!   of the same TEXT is a new record with a new seq, which is exactly right: it
//!   IS a new acknowledgement);
//! * **no `Empty`/`Disconnected` class** — end-of-stream is a field of the same
//!   store, read under the same lock as the records, so "closed" can never be
//!   reported about a state whose lines the verdict did not see;
//! * **no second sample** — a poll's "new lines" count and its last-progress
//!   instant both come from the one snapshot it decides from, so a line appended
//!   between two samples cannot be counted without moving last-progress (job 247
//!   finding 3): there is no second sample.
//!
//! Stream attribution is a FIELD of a record, never a text prefix parsed back out
//! of a line. That is the same doctrine one directory over (CLAUDE.md, #3312):
//! control and data must not share a channel.

use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use super::Stage;

/// How often a wait re-reads the one store.
///
/// **THE ACCEPTED COST OF D6b, stated where it is paid.** A channel wakes on
/// delivery; a log is polled, so a wait observes its line up to one interval
/// late. At 2ms that inflates a stage measurement by at most 2ms, which is far
/// below the 43ms slowest RECORDED quiet observation the calibration baseline is
/// derived from (`budgets.rs`), so the calibration stays quiet-inert; and it is
/// irrelevant against a 180s+ deadline. The cost in the other direction is
/// wakeups on a HANGING run — at most one lock acquisition and one clone of a
/// short `Vec` per 2ms, i.e. sub-second CPU over a whole 180s expiry, and only
/// ever on a run that is already failing.
const WAIT_POLL: Duration = Duration::from_millis(2);

/// Which of the child's two pipes a record came from.
///
/// A FIELD of every record, never a prefix parsed out of its text: the decision
/// and the render read the same structured value, so a line the failure message
/// shows as stderr is provably a line the decision considered as stderr.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    /// The tag used when RENDERING a record for a panic message. Rendering only —
    /// nothing parses it back, which is what retired the old
    /// `transcript_prefix`/`strip_prefix` round-trip.
    fn tag(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
        }
    }
}

/// One line the child emitted, as recorded in the one store.
///
/// `seq` is assigned under the store's lock and is monotone, so it totally orders
/// the child's output across BOTH pipes; `at` is when the reader recorded it,
/// which is what lets a poll derive its last-progress instant from the same
/// snapshot it decides from.
#[derive(Clone, Debug)]
struct Record {
    seq: usize,
    at: Instant,
    stream: Stream,
    text: String,
}

/// THE ONE STORE. Records plus the end-of-stream fact, under ONE lock.
///
/// Keeping `readers_open` HERE — rather than inferring end-of-stream from a
/// channel's disconnected senders — is what dissolves the `Empty`-vs-
/// `Disconnected` family: a reader that has ended has already appended everything
/// it ever will, and a snapshot reads the records and the count together, so
/// "the pipes are closed" is never claimed about a state whose lines the verdict
/// did not examine.
#[derive(Debug)]
struct Transcript {
    records: Vec<Record>,
    /// The next sequence number to hand out. Equal to `records.len()` today; kept
    /// explicitly because a [`Mark`] is a SEQ, and windowing by seq stays correct
    /// even if this store ever stops being a bare append-only `Vec`.
    next_seq: usize,
    /// Reader threads still attached. `0` means every pipe reached EOF (or its
    /// reader died), so no further record can ever appear.
    readers_open: usize,
}

impl Transcript {
    fn new(readers: usize) -> Self {
        Self {
            records: Vec::new(),
            next_seq: 0,
            readers_open: readers,
        }
    }

    fn append(&mut self, stream: Stream, text: String) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.records.push(Record {
            seq,
            at: Instant::now(),
            stream,
            text,
        });
    }
}

/// A reader's handle on the one store: the ONLY way anything is recorded.
///
/// Real reader threads and the unit tests' stand-ins use the SAME type, so the
/// tests drive the actual recording path instead of a mock of it.
///
/// End-of-stream is signalled by DROP, deliberately mirroring the semantics the
/// deleted `Sender` had: a reader that returns normally, and one that unwinds,
/// both mark their pipe closed, so a panicking reader cannot leave a wait
/// believing more output is possible.
pub struct ReaderHandle {
    log: Arc<Mutex<Transcript>>,
}

impl ReaderHandle {
    /// Append one line the child emitted.
    ///
    /// A poisoned lock loses the line — the same outcome the old transcript had,
    /// and it is REPORTED rather than rendered as silence: a snapshot taken
    /// through a poisoned lock says "a reader thread panicked".
    pub fn record(&self, stream: Stream, line: impl Into<String>) {
        if let Ok(mut log) = self.log.lock() {
            log.append(stream, line.into());
        }
    }
}

impl Drop for ReaderHandle {
    fn drop(&mut self) {
        if let Ok(mut log) = self.log.lock() {
            log.readers_open = log.readers_open.saturating_sub(1);
        }
    }
}

/// A position in the one store's SEQUENCE, taken BEFORE the operation whose
/// response is awaited.
///
/// WHY THE CALLER OWNS IT (roborev job 243, finding 1). The mark bounds the window
/// a wait may consider, so it must be taken before the operation that can PRODUCE
/// the awaited line — the spawn, the `writeln!`, the `kill`. Taken inside the wait
/// instead, a reader that recorded a fast response in the gap left that record
/// BEFORE the mark, outside the window: a false timeout against evidence the
/// harness already held.
///
/// WHY IT IS A SEQUENCE AND NOT A LENGTH (job 247). A length windows a `Vec`; it
/// cannot window a queue, which is why the deleted channel had no window at all
/// and could re-serve a consumed line. With one sequenced store every wait's
/// window is expressible, so "the previous write's `OK` satisfied this write's
/// wait" is not.
///
/// It is a newtype, not a bare `usize`, so a call site cannot pass a length, an
/// index or a count where a mark belongs.
#[derive(Clone, Copy, Debug)]
pub struct Mark(usize);

/// ONE read of the one store, used for BOTH the decision and the message.
///
/// DECIDE AND RENDER FROM THE SAME SNAPSHOT, NOT MERELY THE SAME STORE (roborev
/// job 243, finding 1): two acquisitions of one lock let a line appended in
/// between appear in a message that had just called it absent. Everything a
/// verdict needs — the window, the total render, the end-of-stream fact and the
/// newest record's instant — is copied out under a single lock, so no append can
/// contradict the message, and no two facts in one verdict can come from
/// different moments.
#[derive(Debug)]
pub struct TranscriptSnapshot {
    /// Every record as of the decision, in sequence order.
    records: Vec<Record>,
    /// The awaiting wait's window start: records with `seq >= mark`.
    mark: usize,
    /// Every reader had ended at the moment of this read, so no further record
    /// can ever arrive. Read under the SAME lock as `records`.
    pipes_closed: bool,
    /// The lock was readable. `false` means a reader thread panicked, which is
    /// reported rather than rendered as an empty transcript.
    available: bool,
}

impl TranscriptSnapshot {
    /// The records in the window: everything recorded at or after the mark.
    fn window(&self) -> impl Iterator<Item = &Record> {
        let mark = self.mark;
        self.records.iter().filter(move |r| r.seq >= mark)
    }

    /// How many records the decision's window covered — how much evidence it
    /// actually looked at.
    pub fn examined(&self) -> usize {
        self.window().count()
    }

    /// The window's lines on `want`, as the child's own text.
    ///
    /// The predicate is applied OUTSIDE any lock, by construction: this reads an
    /// owned copy. `pred` is caller code, and applying it while holding the store's
    /// lock deadlocks any predicate that touches the store — a std `Mutex` is not
    /// reentrant. Found the hard way: the first version of a plant for this did
    /// exactly that and wedged the test binary for nine minutes.
    fn window_on(&self, want: Stream) -> impl Iterator<Item = &str> {
        self.window()
            .filter(move |r| r.stream == want)
            .map(|r| r.text.as_str())
    }

    /// Every reader had ended when this snapshot was taken.
    pub fn pipes_closed(&self) -> bool {
        self.pipes_closed
    }

    /// When the newest record in the WHOLE store was recorded, if any.
    ///
    /// A poll derives its last-progress instant from this, so the count of new
    /// lines and the "how long since anything happened" figure come from the same
    /// read — job 247 finding 3 is not expressible.
    pub fn newest_at(&self) -> Option<Instant> {
        self.records.last().map(|r| r.at)
    }

    /// The snapshot, indented for a panic message. THE SAME RECORDS the verdict was
    /// taken from — no fresh read, so no append can contradict the message.
    pub fn render(&self) -> String {
        if !self.available {
            return "  (transcript unavailable: a reader thread panicked)".to_string();
        }
        if self.records.is_empty() {
            return "  (the child emitted nothing at all on stdout or stderr)".to_string();
        }
        self.records
            .iter()
            .map(|r| format!("  [{}] {}", r.stream.tag(), r.text))
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
/// renders THAT rather than re-reading the store, so the message and the decision
/// are the same records (job 243, finding 1).
#[derive(Debug)]
pub enum WaitEnd {
    /// The test's one deadline passed; the child's pipes were still open, so more
    /// output was still possible.
    DeadlineReached { snapshot: TranscriptSnapshot },
    /// Every reader had ended: the child's stdout AND stderr reached EOF after
    /// this long, so no further line could ever arrive. The child had exited,
    /// crashed, or closed its pipes -- this does not say which.
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
                 of the ONE transcript log covering the {} record(s) sequenced since this wait \
                 began, and none of them matched. The transcript printed below IS that snapshot — \
                 not a fresh read of a store that may since have grown — so this message cannot \
                 be contradicted by the evidence it prints",
                snapshot.examined()
            ),
            WaitEnd::PipesClosed { after, snapshot } => format!(
                "how the wait ended: the child's stdout AND stderr both reached EOF after \
                 {after:.3?}, so no further line could arrive: the child had exited, crashed, or \
                 closed its pipes (this measurement does not say which). The {} record(s) \
                 sequenced since this wait began, printed below, are the snapshot the verdict was \
                 taken from — the records and the end-of-stream fact were read under ONE lock",
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
/// chattier session) into the ONE store, so a failure can print what the child
/// actually said. The original `wait_for_line` discarded every non-matching line,
/// so a failure could report nothing.
pub struct ChildIo {
    log: Arc<Mutex<Transcript>>,
}

fn spawn_reader<R: std::io::Read + Send + 'static>(
    stream: Stream,
    reader: R,
    handle: ReaderHandle,
) {
    thread::spawn(move || {
        // Dropped when this thread ends by ANY path — normal return or unwind —
        // which is what marks this pipe closed exactly once.
        let handle = handle;
        let buf = BufReader::new(reader);
        for line in buf.lines() {
            let Ok(line) = line else { break };
            handle.record(stream, line);
        }
    });
}

impl ChildIo {
    /// Attach readers to a spawned child's stdout + stderr, returning the harness
    /// AND the [`Mark`] for the first wait.
    ///
    /// THE MARK IS TAKEN BEFORE EITHER READER EXISTS, which is the earliest point
    /// at which a mark can be taken at all: until a reader is spawned, nothing can
    /// record. So the first wait's window provably covers every line the child has
    /// ever emitted, and the "recorded a fast response, then got descheduled before
    /// the mark" race (job 243, finding 1) is not expressible for stage (a).
    /// Returning it — rather than letting the call site take one after `attach` —
    /// is what makes that structural instead of a convention.
    pub(super) fn attach(child: &mut std::process::Child) -> (Self, Mark) {
        let (io, handles) = Self::with_readers(2);
        let mark = io.mark();
        let out = child.stdout.take().expect("child stdout");
        let err = child.stderr.take().expect("child stderr");
        let mut handles = handles.into_iter();
        spawn_reader(
            Stream::Stdout,
            out,
            handles.next().expect("stdout reader handle"),
        );
        spawn_reader(
            Stream::Stderr,
            err,
            handles.next().expect("stderr reader handle"),
        );
        (io, mark)
    }

    /// A harness over a fresh store with `readers` reader handles outstanding.
    ///
    /// The one constructor: `attach` hands its handles to reader threads, and the
    /// unit tests keep theirs, so a test drives the same recording and
    /// end-of-stream paths a real reader does.
    pub(super) fn with_readers(readers: usize) -> (Self, Vec<ReaderHandle>) {
        let log = Arc::new(Mutex::new(Transcript::new(readers)));
        let handles = (0..readers)
            .map(|_| ReaderHandle {
                log: Arc::clone(&log),
            })
            .collect();
        (Self { log }, handles)
    }

    /// Take a [`Mark`] for a wait that has NOT YET been started.
    ///
    /// CALL THIS BEFORE THE OPERATION WHOSE RESPONSE YOU WILL AWAIT — before the
    /// `writeln!`, before the `kill`. See [`Mark`] for what taking it later costs.
    pub fn mark(&self) -> Mark {
        Mark(self.log.lock().map(|l| l.next_seq).unwrap_or(0))
    }

    /// ONE read of the one store, under ONE lock acquisition: the records, their
    /// sequence, the end-of-stream fact, and the newest record's instant, windowed
    /// at `mark`.
    ///
    /// Every verdict and every message it produces comes from a value of this type,
    /// so "the message prints evidence the decision did not see" is not expressible
    /// (job 243, finding 1). Reads nothing but memory, so it cannot extend a
    /// deadline; and the records are COPIED, so the lock is released before any
    /// caller predicate runs.
    ///
    /// A poisoned lock is REPORTED, never rendered as an empty transcript: "the
    /// child said nothing" and "a reader thread panicked" are different facts, and
    /// only one of them is about the child. `pipes_closed` is `false` there — an
    /// unreadable store has not established that output is over.
    pub fn snapshot(&self, mark: Mark) -> TranscriptSnapshot {
        match self.log.lock() {
            Ok(log) => TranscriptSnapshot {
                records: log.records.clone(),
                mark: mark.0,
                pipes_closed: log.readers_open == 0,
                available: true,
            },
            Err(_) => TranscriptSnapshot {
                records: Vec::new(),
                mark: 0,
                pipes_closed: false,
                available: false,
            },
        }
    }

    /// Block until a record on `want` at or after `mark` satisfies `pred`, or the
    /// TEST's one deadline passes. Returns the matching line and how much of the
    /// stage it took (so a successful wait can calibrate the deadline).
    ///
    /// Takes the `Stage` itself, never a `Duration`: the timeout comes from
    /// `Stage::remaining()`, the one place a per-wait timeout is computed, so no
    /// call site can be handed a fresh allowance and none can double-spend.
    ///
    /// `mark` bounds the window. The store is CUMULATIVE across the whole test, so
    /// without a window an earlier stage's already-consumed record would satisfy a
    /// later wait — `await_write_ack` awaits five separate `OK`s in one test, so
    /// the first would silently satisfy all five and a wedged session would read as
    /// green. A false PASS is strictly worse than a confusing diagnostic. That is
    /// exactly the hole the deleted channel had: it carried no sequence, so it
    /// could re-deliver an `OK` a completed wait had already accepted (job 247).
    ///
    /// EXACTLY ONE SNAPSHOT PER ITERATION, and the expiry verdict comes from a
    /// snapshot taken AFTER the deadline lapsed: the awaited line may have been
    /// recorded before expiry and this thread descheduled, so declaring a timeout
    /// without one last look would be a false timeout on a working product (the
    /// round-9 ruling: the deadline bounds how long we WAIT FOR evidence, never
    /// whether we accept evidence we already hold). Nothing here waits on that
    /// path: it is one lock acquisition and a scan of memory, so the deadline
    /// cannot be extended.
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
                // THE FINAL LOOK. One snapshot: it supplies the match test, the
                // end-of-stream fact and the rendered transcript, so the cause
                // named and the evidence printed cannot disagree.
                let snapshot = self.snapshot(mark);
                if let Some(line) = snapshot.window_on(want).find(|l| pred(l)) {
                    return Ok((line.to_string(), stage.spent()));
                }
                // Only now can a cause be named, and it comes from the same read:
                // a store whose readers have all ended reports closed pipes, one
                // whose readers are live reports the deadline (roborev job 236,
                // finding 3 — collapsing the two reported "pipes still open"
                // about a child whose readers had both ended).
                return Err(if snapshot.pipes_closed() {
                    WaitEnd::PipesClosed {
                        after: stage.spent(),
                        snapshot,
                    }
                } else {
                    WaitEnd::DeadlineReached { snapshot }
                });
            }
            let snapshot = self.snapshot(mark);
            if let Some(line) = snapshot.window_on(want).find(|l| pred(l)) {
                return Ok((line.to_string(), stage.spent()));
            }
            if snapshot.pipes_closed() {
                // Every reader has ended AND this snapshot read their records, so
                // no further line can ever arrive: waiting out the deadline could
                // only delay the same verdict.
                return Err(WaitEnd::PipesClosed {
                    after: stage.spent(),
                    snapshot,
                });
            }
            thread::sleep(WAIT_POLL.min(remaining));
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
        self.snapshot(Mark(0)).render()
    }
}
