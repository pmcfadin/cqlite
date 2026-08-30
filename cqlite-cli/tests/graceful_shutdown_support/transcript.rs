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
/// irrelevant against a 360s+ deadline (the bases the wait census derives in
/// `budgets.rs`: 360s and 600s). The cost in the other direction is wakeups on a
/// HANGING run — at most one lock acquisition and one clone of a short `Vec` per
/// 2ms, i.e. sub-second CPU over a whole 360s expiry, and only ever on a run that
/// is already failing.
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
    /// Reader threads still attached. `0` means every reader has ENDED, so no
    /// further record can ever appear — but NOT that each of them ended at EOF:
    /// see `read_failures`.
    readers_open: usize,
    /// How many readers were EVER attached, so a snapshot can say how many of
    /// them have ended rather than only whether ALL of them have (round 16). A
    /// bare `readers_open` cannot express PARTIAL closure, which is what let a
    /// poll report "the child's pipes were still open" about a child one of whose
    /// pipes had already ended — in an I/O error, at that.
    readers_total: usize,
    /// **THE READERS' TERMINAL RESULTS, for the ones that did not end at EOF**
    /// (roborev job 255, finding 2). A reader's `Err` used to be dropped on the
    /// floor, so an I/O failure on a pipe ended the reader exactly as EOF does and
    /// the wait then reported, in as many words, that "the child's stdout AND
    /// stderr both reached EOF" — a cause the measurement had not established.
    /// Recorded in the SAME store as the records and the count, so a verdict reads
    /// all three under one lock.
    read_failures: Vec<(Stream, String)>,
}

impl Transcript {
    fn new(readers: usize) -> Self {
        Self {
            records: Vec::new(),
            next_seq: 0,
            readers_open: readers,
            readers_total: readers,
            read_failures: Vec::new(),
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

    /// **RECORD THAT THIS READER ENDED IN AN I/O ERROR RATHER THAN AT EOF**
    /// (roborev job 255, finding 2).
    ///
    /// A reader ends by dropping its handle either way, so without this the two
    /// outcomes are the same event and the wait names EOF for both. What is at
    /// stake is the CAUSE a failure reports, not a verdict: the awaited line is
    /// absent in both cases. It is stored, never rendered from the reader thread,
    /// so the message and the decision still come from one snapshot.
    pub fn read_failed(&self, stream: Stream, error: impl std::fmt::Display) {
        if let Ok(mut log) = self.log.lock() {
            log.read_failures.push((stream, error.to_string()));
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

/// **WHAT THE READERS' STATE ESTABLISHES ABOUT THE CHILD'S PIPES** — the ONE
/// derivation every verdict in this harness reads (round 16).
///
/// **WHY IT IS AN ENUM AND NOT THE BOOL IT REPLACES.** `pipes_closed: bool` has
/// exactly two answers for FIVE distinguishable states, so it had to collapse
/// three of them onto a permissive one — and both collapses were reported as
/// facts about the child:
///
/// * a reader that ended in an I/O ERROR counted as closure, so a verdict named
///   EOF for a pipe this harness had simply failed to read (round 15 fixed that in
///   `WaitEnd` and left `PollFail`, which is what this enum propagates);
/// * ONE of two readers having ended left the bool `false`, so a verdict said "the
///   child's pipes were still open, more output was still possible" about a child
///   one of whose pipes could produce nothing further;
/// * an UNREADABLE store (a reader panicked holding the lock) also left it
///   `false`, so a verdict that had established NOTHING about the pipes reported
///   that they were open.
///
/// A permissive default for an unmeasured state is the "positive verdict without
/// an affirmative measurement" class this repository names in doctrine
/// (CLAUDE.md); the fix is the same one round 15 applied one type over — make the
/// unsupported claim UNREACHABLE rather than annotate it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PipeStatus {
    /// The store could not be read — a reader thread panicked while holding its
    /// lock. NOTHING is established about the pipes: not that they are open, and
    /// not that they are closed.
    Unavailable,
    /// Every reader had ended, and every one of them ended AT EOF, so no further
    /// line can arrive. The child had exited, crashed, or closed its pipes — this
    /// does not say which.
    AllEof { readers: usize },
    /// Every reader had ended and at least one ended in an I/O ERROR rather than at
    /// EOF. No further line can arrive either, but the CAUSE is this harness's read
    /// and not the child (roborev job 255, finding 2).
    ReaderFailed { note: String },
    /// Some readers had ended and at least one was still attached: more output is
    /// still possible ON THE SURVIVING PIPE(S) ONLY.
    PartiallyClosed {
        open: usize,
        ended: usize,
        /// Set when one of the ENDED readers ended in an I/O error rather than at
        /// EOF: the deadline is still what bound the wait, but a pipe was lost.
        failure_note: Option<String>,
    },
    /// Every reader was still attached, so output was still possible on both pipes.
    AllOpen { open: usize },
}

impl PipeStatus {
    /// **NO FURTHER RECORD CAN EVER ARRIVE**, so continuing to wait could only
    /// delay the same verdict.
    ///
    /// TRUE ONLY FOR THE TWO STATES THAT ESTABLISH IT. `Unavailable` is deliberately
    /// NOT terminal: an unreadable store has not established that output is over,
    /// and treating it as terminal would abandon a wait on the strength of a
    /// measurement that failed.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PipeStatus::AllEof { .. } | PipeStatus::ReaderFailed { .. }
        )
    }

    /// This state as a sentence for a failure message. Each one states what was
    /// measured and stops there.
    pub fn describe(&self) -> String {
        match self {
            PipeStatus::Unavailable => "the transcript store's lock was POISONED when the \
                 verdict was taken — a reader thread panicked while holding it — so NOTHING was \
                 established about the child's pipes: not that they were open, and not that they \
                 were closed. This is a defect in this test harness, and it is not a statement \
                 about the child"
                .to_string(),
            PipeStatus::AllEof { readers } => format!(
                "all {readers} of the child's pipe readers had ended AT EOF when the verdict was \
                 taken, so no further output could arrive: the child had exited, crashed, or \
                 closed its pipes (this measurement does not say which)"
            ),
            PipeStatus::ReaderFailed { note } => format!(
                "every reader had ended when the verdict was taken, so no further output could \
                 arrive — but at least one ended in an I/O ERROR rather than at EOF, so this is \
                 NOT the \"both pipes reached EOF\" measurement: {note}. WHAT THAT ESTABLISHES: \
                 only that this harness could not read the child's output to its end. It is a \
                 statement about the pipe, NOT about the child"
            ),
            PipeStatus::PartiallyClosed {
                open,
                ended,
                failure_note,
            } => format!(
                "{ended} of the child's pipe readers had already ENDED and {open} was/were still \
                 attached, so further output was possible ONLY on the surviving pipe(s) — NOT on \
                 the ended one(s){}",
                failure_note
                    .as_ref()
                    .map(|note| format!(
                        ", and the reader(s) that ended did NOT all end at EOF: {note}"
                    ))
                    .unwrap_or_default()
            ),
            PipeStatus::AllOpen { open } => format!(
                "all {open} of the child's pipe readers were still attached when the verdict was \
                 taken, so more output was still possible on either pipe"
            ),
        }
    }
}

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
    /// Readers still attached at the moment of this read, and how many were ever
    /// attached — read under the SAME lock as `records`. TWO COUNTS RATHER THAN A
    /// BOOL (round 16): `pipes_closed` collapsed "one of two pipes has ended" onto
    /// "the pipes are still open", so a verdict could report that more output was
    /// possible on a pipe that had already ended.
    readers_open: usize,
    readers_total: usize,
    /// The lock was readable. `false` means a reader thread panicked, which is
    /// reported rather than rendered as an empty transcript.
    available: bool,
    /// Readers that ended in an I/O ERROR rather than at EOF, read under the SAME
    /// lock as `records` and the reader counts — so "every reader ended" and "one
    /// of them ended badly" can never come from different moments.
    read_failures: Vec<(Stream, String)>,
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

    /// **THE ONE PLACE PIPE STATE IS DERIVED** — from the readers' counts AND their
    /// recorded terminal results, read under the one lock this snapshot was taken
    /// under (round 16).
    ///
    /// It replaces a `pipes_closed()` bool that two separate verdicts consulted.
    /// Round 15 recorded the readers' terminal results and taught `WaitEnd` to use
    /// them, and `PollFail` — the other consumer — kept inferring pipe state from
    /// the count alone, so a poll reported EOF for a reader that had FAILED and
    /// "still open" for a child one of whose pipes had ended. Both are statements
    /// the measurement had not established. Returning a state rather than a bool is
    /// what makes each of them unspellable: there is no `true` for a caller to
    /// render as EOF.
    pub fn pipe_status(&self) -> PipeStatus {
        if !self.available {
            return PipeStatus::Unavailable;
        }
        let ended = self.readers_total.saturating_sub(self.readers_open);
        // CHECKED BEFORE ANY EOF CLAIM: a failed reader ends exactly as one at EOF
        // does, so the terminal results — not the count — decide (job 255, finding
        // 2, now applied to every consumer).
        let failure_note = self.read_failure_note();
        if self.readers_open == 0 {
            return match failure_note {
                Some(note) => PipeStatus::ReaderFailed { note },
                None => PipeStatus::AllEof {
                    readers: self.readers_total,
                },
            };
        }
        if ended > 0 {
            return PipeStatus::PartiallyClosed {
                open: self.readers_open,
                ended,
                failure_note,
            };
        }
        PipeStatus::AllOpen {
            open: self.readers_open,
        }
    }

    /// Those terminal results, rendered for a failure message; `None` when every
    /// reader that ended did so at EOF.
    pub fn read_failure_note(&self) -> Option<String> {
        if self.read_failures.is_empty() {
            return None;
        }
        Some(
            self.read_failures
                .iter()
                .map(|(stream, error)| format!("{} reader: {error}", stream.tag()))
                .collect::<Vec<_>>()
                .join("; "),
        )
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
    /// Every reader had ended AT EOF: the child's stdout AND stderr reached EOF
    /// after this long, so no further line could ever arrive. The child had exited,
    /// crashed, or closed its pipes -- this does not say which.
    PipesClosed {
        after: Duration,
        snapshot: TranscriptSnapshot,
    },
    /// Every reader had ended, and at least one of them ended in an I/O ERROR
    /// rather than at EOF (roborev job 255, finding 2).
    ///
    /// A SEPARATE variant rather than a note on `PipesClosed`, because that
    /// variant's message states EOF as a fact about the child: a reader whose
    /// `Err` was discarded ended exactly as one at EOF does, so the wait reported
    /// a cause its own measurement had not established. The awaited line is absent
    /// either way — what this fixes is which cause is named, and a variant is what
    /// stops the EOF claim being reachable from a failed read.
    ReaderFailed {
        after: Duration,
        snapshot: TranscriptSnapshot,
    },
}

impl WaitEnd {
    pub fn describe(&self) -> String {
        match self {
            WaitEnd::DeadlineReached { snapshot } => format!(
                "how the wait ended: the test's one deadline passed, and the readers' state did \
                 NOT establish that output was over. The verdict was taken from ONE snapshot of \
                 the ONE transcript log covering the {} record(s) sequenced since this wait \
                 began, and none of them matched. The transcript printed below IS that snapshot — \
                 not a fresh read of a store that may since have grown — so this message cannot \
                 be contradicted by the evidence it prints.\n\
                 pipe state at the verdict: {}",
                snapshot.examined(),
                // WHAT THE PIPES WERE, FROM THE ONE DERIVATION (round 16). This
                // used to say "with the child's pipes still open" and append a note
                // only when a reader had FAILED — so a CLEAN partial closure (one
                // pipe at EOF, its sibling live) and an UNREADABLE store were both
                // reported as both pipes open. `PipeStatus` states each case.
                snapshot.pipe_status().describe()
            ),
            WaitEnd::PipesClosed { after, snapshot } => format!(
                "how the wait ended: the child's stdout AND stderr both reached EOF after \
                 {after:.3?}, so no further line could arrive: the child had exited, crashed, or \
                 closed its pipes (this measurement does not say which). The {} record(s) \
                 sequenced since this wait began, printed below, are the snapshot the verdict was \
                 taken from — the records and the end-of-stream fact were read under ONE lock",
                snapshot.examined()
            ),
            WaitEnd::ReaderFailed { after, snapshot } => format!(
                "how the wait ended: every reader had ended after {after:.3?}, so no further line \
                 could arrive — but at least one of them ended in an I/O ERROR rather than at \
                 EOF, so this is NOT the \"both pipes reached EOF\" measurement: {}. \
                 The {} record(s) sequenced since this wait began, printed below, are the \
                 snapshot the verdict was taken from — the records, the end-of-stream fact and \
                 the readers' terminal results were read under ONE lock.\n\
                 WHAT THIS ESTABLISHES: only that this harness could not read the child's output \
                 to its end. It is a statement about the pipe, NOT about the child, and NOT about \
                 the property under test",
                snapshot
                    .read_failure_note()
                    .unwrap_or_else(|| "(no terminal result recorded)".to_string()),
                snapshot.examined()
            ),
        }
    }

    /// The transcript the DECISION examined, ready for a panic message.
    pub fn transcript(&self) -> String {
        match self {
            WaitEnd::DeadlineReached { snapshot } => snapshot.render(),
            WaitEnd::PipesClosed { snapshot, .. } => snapshot.render(),
            WaitEnd::ReaderFailed { snapshot, .. } => snapshot.render(),
        }
    }
}

/// **THE ONE PLACE THE END-OF-READERS VERDICT IS NAMED** — `PipesClosed` when
/// every reader ended at EOF, `ReaderFailed` when at least one ended in an I/O
/// error (roborev job 255, finding 2).
///
/// One function, called from both of `wait_for`'s closed-pipe branches, so neither
/// branch can name EOF for a reader whose terminal result says otherwise.
///
/// **IT ANSWERS "IS OUTPUT OVER?" AND NAMES THE VERDICT IN ONE STEP** (round 16),
/// returning the snapshot back on `Err` when the state does NOT establish that
/// output is over — a live pipe, a PARTIAL closure, or an unreadable store. A
/// caller therefore cannot ask the question through one derivation and build its
/// verdict from another, and no arm of this match is unreachable.
fn ended(after: Duration, snapshot: TranscriptSnapshot) -> Result<WaitEnd, TranscriptSnapshot> {
    match snapshot.pipe_status() {
        PipeStatus::ReaderFailed { .. } => Ok(WaitEnd::ReaderFailed { after, snapshot }),
        PipeStatus::AllEof { .. } => Ok(WaitEnd::PipesClosed { after, snapshot }),
        // NOT established: output may still arrive (or the store could not be
        // read), so no "no further line could arrive" variant may be built.
        PipeStatus::PartiallyClosed { .. }
        | PipeStatus::AllOpen { .. }
        | PipeStatus::Unavailable => Err(snapshot),
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

/// `pub(super)` so the harness's own tests can drive THIS function with a reader
/// that fails mid-stream, rather than only the store beneath it (roborev job 255,
/// finding 2 lived in the loop below, so that is where a test has to reach).
pub(super) fn spawn_reader<R: std::io::Read + Send + 'static>(
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
            // THE TERMINAL RESULT IS RECORDED, NOT DISCARDED (roborev job 255,
            // finding 2): `break` alone made an I/O failure indistinguishable from
            // EOF, and the wait then reported EOF as the cause.
            match line {
                Ok(line) => handle.record(stream, line),
                Err(error) => {
                    handle.read_failed(stream, error);
                    break;
                }
            }
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
    /// only one of them is about the child. Its [`PipeStatus`] there is
    /// `Unavailable` — an unreadable store has established NOTHING about the pipes,
    /// which is a third state and not the "still open" a bool had to collapse it
    /// onto (round 16).
    pub fn snapshot(&self, mark: Mark) -> TranscriptSnapshot {
        match self.log.lock() {
            Ok(log) => TranscriptSnapshot {
                records: log.records.clone(),
                mark: mark.0,
                readers_open: log.readers_open,
                readers_total: log.readers_total,
                available: true,
                read_failures: log.read_failures.clone(),
            },
            Err(_) => TranscriptSnapshot {
                records: Vec::new(),
                mark: 0,
                readers_open: 0,
                readers_total: 0,
                available: false,
                read_failures: Vec::new(),
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
                //
                // EOF and a failed read both END a reader, so the variant is chosen
                // from the readers' recorded TERMINAL RESULTS and never from a count
                // (job 255, finding 2). A state that establishes neither — including
                // a PARTIAL closure and an unreadable store — hands the snapshot
                // back, and the deadline is then the cause.
                return Err(match ended(stage.spent(), snapshot) {
                    Ok(end) => end,
                    Err(snapshot) => WaitEnd::DeadlineReached { snapshot },
                });
            }
            let snapshot = self.snapshot(mark);
            if let Some(line) = snapshot.window_on(want).find(|l| pred(l)) {
                return Ok((line.to_string(), stage.spent()));
            }
            // Every reader has ended AND this snapshot read their records, so no
            // further line can ever arrive: waiting out the deadline could only
            // delay the same verdict. `ended` is what decides that — asking it is
            // the same act as naming the verdict — and on `Err` the wait continues
            // because output is still possible (or the store could not be read).
            match ended(stage.spent(), snapshot) {
                Ok(end) => return Err(end),
                Err(_still_possible) => {}
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

// ---------------------------------------------------------------------------
// THE ONE DERIVATION'S OWN COVERAGE (round 16, roborev job 259 finding 1)
// ---------------------------------------------------------------------------
//
// These live HERE rather than in `harness_tests.rs` because the state they are
// about is derived from this file's private fields, and one of the five —
// `Unavailable` — is only reachable by POISONING the store's lock, which needs the
// `Arc` a `ChildIo` holds. A test that cannot reach a state cannot pin it, and an
// unpinnable state is exactly where a permissive default hides.

#[cfg(test)]
mod pipe_status_tests {
    use super::super::TestDeadline;
    use super::*;

    /// Poison the one store's lock the way a reader thread panicking inside
    /// `record` would, so the `Unavailable` state is reached through the real
    /// mechanism rather than by constructing a snapshot by hand.
    fn poison(io: &ChildIo) {
        let log = Arc::clone(&io.log);
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = log.lock().expect("the store's lock before it is poisoned");
            panic!("a reader thread panicked while holding the store's lock");
        }));
        std::panic::set_hook(hook);
        assert!(
            poisoned.is_err(),
            "this helper's whole purpose is the panic it did not take"
        );
    }

    /// **EVERY ONE OF THE FIVE STATES IS DISTINGUISHED, AND `is_terminal` IS TRUE
    /// FOR EXACTLY THE TWO THAT ESTABLISH IT.**
    ///
    /// A bool had two answers for these five, so three of them were reported as
    /// one — and each collapse was reported as a fact about the child (see
    /// [`PipeStatus`]).
    #[test]
    fn each_reader_state_is_derived_distinctly() {
        // ALL OPEN: nothing has ended.
        let (io, handles) = ChildIo::with_readers(2);
        let mark = io.mark();
        assert_eq!(
            io.snapshot(mark).pipe_status(),
            PipeStatus::AllOpen { open: 2 }
        );
        assert!(!io.snapshot(mark).pipe_status().is_terminal());

        // PARTIALLY CLOSED: one reader ended at EOF, its sibling is still attached.
        let mut handles = handles.into_iter();
        let out = handles.next().expect("stdout handle");
        drop(handles.next().expect("stderr handle"));
        assert_eq!(
            io.snapshot(mark).pipe_status(),
            PipeStatus::PartiallyClosed {
                open: 1,
                ended: 1,
                failure_note: None,
            },
            "one of two readers had ended: a bool could only say \"not all of them\", which every \
             consumer then rendered as \"the pipes were still open\""
        );
        assert!(
            !io.snapshot(mark).pipe_status().is_terminal(),
            "a surviving pipe can still produce output, so the wait must not be abandoned"
        );

        // ALL AT EOF: every reader ended, none of them badly.
        drop(out);
        assert_eq!(
            io.snapshot(mark).pipe_status(),
            PipeStatus::AllEof { readers: 2 }
        );
        assert!(io.snapshot(mark).pipe_status().is_terminal());

        // READER FAILED: every reader ended and one recorded an I/O error. Chosen
        // from the TERMINAL RESULTS and never from the count, which is identical in
        // both cases (job 255, finding 2).
        let (io, handles) = ChildIo::with_readers(1);
        let mark = io.mark();
        let handle = handles.into_iter().next().expect("one handle");
        handle.read_failed(Stream::Stdout, std::io::Error::other("simulated failure"));
        drop(handle);
        match io.snapshot(mark).pipe_status() {
            PipeStatus::ReaderFailed { note } => assert!(
                note.contains("simulated failure") && note.contains("stdout"),
                "the state must carry the reader's own terminal result: {note}"
            ),
            other => panic!("a failed read must not be derived as {other:?}"),
        }
        assert!(io.snapshot(mark).pipe_status().is_terminal());

        // UNAVAILABLE: the store could not be read, so NOTHING is established —
        // which a bool had to render as "still open" (round 16).
        let (io, _handles) = ChildIo::with_readers(2);
        let mark = io.mark();
        poison(&io);
        assert_eq!(io.snapshot(mark).pipe_status(), PipeStatus::Unavailable);
        assert!(
            !io.snapshot(mark).pipe_status().is_terminal(),
            "an unreadable store has NOT established that output is over: treating it as \
             terminal would abandon a wait on the strength of a measurement that failed"
        );
    }

    /// **A WAIT AGAINST AN UNREADABLE STORE REPORTS THAT IT COULD NOT MEASURE** —
    /// it does not report that the pipes were open.
    ///
    /// The wait's VERDICT is unchanged (the awaited line is absent and the deadline
    /// is what bound it); what changes is that the message no longer asserts a pipe
    /// state the failed read could not establish.
    #[test]
    fn a_wait_against_an_unreadable_store_does_not_claim_the_pipes_were_open() {
        let (io, _handles) = ChildIo::with_readers(2);
        let mark = io.mark();
        poison(&io);

        let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
        thread::sleep(Duration::from_millis(25));
        let stage = deadline.stage("unreadable-store");
        assert!(
            stage.remaining().is_zero(),
            "the precondition of this test is an already-lapsed deadline"
        );

        match io.wait_for(mark, Stream::Stderr, |_| true, &stage) {
            Err(end @ WaitEnd::DeadlineReached { .. }) => {
                let described = end.describe();
                assert!(
                    described.contains("POISONED"),
                    "the failure must say the store could not be read: {described}"
                );
                assert!(
                    !described.contains("still attached"),
                    "nothing was established about the pipes, so the message may not report \
                     them open: {described}"
                );
            }
            Err(other) => panic!(
                "an unreadable store establishes no end of output, so no \"no further line could \
                 arrive\" verdict may be built from it: {}",
                other.describe()
            ),
            Ok((line, _)) => panic!("the store is unreadable, yet {line:?} matched"),
        }
    }
}
