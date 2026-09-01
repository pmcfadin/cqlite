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

use std::cell::Cell;
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
    /// **THE PER-STREAM READER STATE — one slot per stream, never a pair of
    /// counts** (round 18).
    ///
    /// A wait awaits a line on ONE stream, so "how many readers are left" cannot
    /// answer the question a wait actually has: can the line I am waiting for
    /// still arrive? With counts alone it could not, and the answer it gave was
    /// the permissive one — the reader of the AWAITED stream ending while its
    /// sibling stayed attached counted as "output is still possible", so a wait
    /// whose line had become impossible ran to the full deadline (up to the 360s
    /// and 600s bases in `budgets.rs`) and then reported a DEADLINE as the cause.
    /// That is a wrong cause for an unsatisfiable wait, which is the very defect
    /// class #3515 exists to remove — one level up.
    ///
    /// The two counts `PipeStatus` needs are DERIVED from this, so a count can
    /// never disagree with the per-stream state it summarises.
    readers: ReaderSlots,
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
    fn new(streams: &[Stream]) -> Self {
        Self {
            records: Vec::new(),
            next_seq: 0,
            readers: ReaderSlots::new(streams),
            read_failures: Vec::new(),
        }
    }

    /// **THE ONE TERMINAL TRANSITION: A READER'S END AND ITS RESULT, TOGETHER**
    /// (#3652, roborev job 262 finding 1, re-found independently by job 271).
    ///
    /// The two facts used to be written under SEPARATE lock acquisitions —
    /// `ReaderHandle::read_failed` pushed the result, `impl Drop` set the slot —
    /// so a snapshot taken between them held a failure for a reader it still
    /// counted as `Open`. That pairing is not merely untidy: `pipe_status` reads
    /// the OPEN COUNT first, so a store whose only reader had failed but not yet
    /// dropped derived `AllOpen` and DISCARDED the failure note, and
    /// `stream_status` answered `Open` for a stream whose reader was finished —
    /// which is exactly the "wait out the whole deadline and then blame it"
    /// wrong-cause path round 18 removed, reintroduced through a lock window.
    ///
    /// Both fields belong to this store, so the transition is ONE method on it
    /// and there is no interleaving point left to observe. `failure` is `None`
    /// for a reader that ended at EOF.
    ///
    /// WHAT THIS DOES **NOT** ENFORCE, stated rather than implied: nothing here
    /// stops a handle appending a record AFTER its terminal transition. That a
    /// failed reader appends nothing more is a property of `spawn_reader`, whose
    /// `Err` arm `break`s out of the read loop immediately — it is not enforced by
    /// this type, and a future caller that recorded after failing would produce a
    /// record on a stream this store already calls finished.
    fn reader_ended(&mut self, stream: Stream, failure: Option<String>) {
        if let Some(note) = failure {
            self.read_failures.push((stream, note));
        }
        self.readers.set(stream, ReaderSlot::Ended);
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

/// ONE reader's state, as the store knows it (round 18).
///
/// Three states and not a bool, for the same reason [`PipeStatus`] is not one: a
/// stream nothing was ever attached to and a stream whose reader has ENDED are both
/// finished, but only the first is a defect in this harness — and neither is the
/// "still open" a two-valued field would have to collapse one of them onto.
///
/// A failed read is NOT a fourth state here: which of EOF and an I/O error ended a
/// reader is read from the terminal results this store already holds, so the two
/// cannot disagree about one reader.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ReaderSlot {
    Unattached,
    Open,
    Ended,
}

/// **ONE SLOT PER STREAM** — the store's whole knowledge of its readers.
///
/// A FIXED pair rather than a `Vec<Stream>` of attachments, for two reasons. It
/// makes "two readers on one stream" unrepresentable, so a per-stream verdict
/// cannot be ambiguous; and it is `Copy` and 2 bytes wide, which keeps
/// [`TranscriptSnapshot`] — and therefore `WaitEnd`, which every wait returns by
/// value — small enough that `clippy::result_large_err` is satisfied without
/// boxing a type this file's tests match on by variant.
#[derive(Clone, Copy, Debug)]
struct ReaderSlots {
    stdout: ReaderSlot,
    stderr: ReaderSlot,
}

impl ReaderSlots {
    /// Every named stream `Open`, every other one `Unattached`.
    ///
    /// A stream named TWICE is rejected rather than silently merged: two handles
    /// on one stream would make "that stream's reader has ended" true as soon as
    /// EITHER dropped, which is exactly the misattribution the per-stream verdict
    /// is decided from.
    fn new(streams: &[Stream]) -> Self {
        let mut slots = Self {
            stdout: ReaderSlot::Unattached,
            stderr: ReaderSlot::Unattached,
        };
        for stream in streams {
            assert_eq!(
                slots.get(*stream),
                ReaderSlot::Unattached,
                "one reader per stream: {} was named twice",
                stream.tag()
            );
            slots.set(*stream, ReaderSlot::Open);
        }
        slots
    }

    fn get(&self, stream: Stream) -> ReaderSlot {
        match stream {
            Stream::Stdout => self.stdout,
            Stream::Stderr => self.stderr,
        }
    }

    fn set(&mut self, stream: Stream, slot: ReaderSlot) {
        match stream {
            Stream::Stdout => self.stdout = slot,
            Stream::Stderr => self.stderr = slot,
        }
    }

    fn count(&self, want: ReaderSlot) -> usize {
        [self.stdout, self.stderr]
            .into_iter()
            .filter(|slot| *slot == want)
            .count()
    }

    /// Readers EVER attached, and readers still attached — derived from the slots,
    /// so no count can disagree with the per-stream state.
    fn total(&self) -> usize {
        2 - self.count(ReaderSlot::Unattached)
    }

    fn open(&self) -> usize {
        self.count(ReaderSlot::Open)
    }

    fn ended(&self) -> usize {
        self.count(ReaderSlot::Ended)
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
    /// **WHICH PIPE THIS READER IS ATTACHED TO — a FIELD of the handle** (round
    /// 18), so the stream a line is attributed to and the stream marked ENDED
    /// when this handle drops are ONE fact and cannot disagree.
    ///
    /// It is why `record` and `read_failed` no longer take a stream: an
    /// attribution passed per call is a SECOND channel carrying the same control
    /// information, and a handle whose caller passed the other stream would
    /// record lines as one pipe's while ending the other's — the exact
    /// misattribution the per-stream verdict below is decided from (CLAUDE.md,
    /// #3312: control and data must not share a channel).
    stream: Stream,
    /// **THIS HANDLE HAS ALREADY MADE ITS ONE TERMINAL TRANSITION** (#3652).
    ///
    /// A reader ends exactly once, and [`Transcript::reader_ended`] is the only
    /// way it ends. `read_failed` performs that transition with the result, so
    /// the `Drop` that follows it must not enter the transition path a second
    /// time — not because setting an already-`Ended` slot would corrupt anything
    /// (the write is idempotent) but because "exactly one terminal transition per
    /// reader" is then a property of this type rather than of the arithmetic in
    /// the store, and nothing later can make the second write non-idempotent
    /// without failing here first.
    ///
    /// A `Cell` and not an `AtomicBool`: a handle is owned by ONE reader thread —
    /// `spawn_reader` moves it in and every method takes `&self` — so the flag is
    /// never read across threads, and `Cell<bool>` is `Send`.
    ended: Cell<bool>,
}

impl ReaderHandle {
    /// The pipe this reader is attached to, so a caller pairs a handle with its
    /// own pipe rather than by position in a list.
    pub(super) fn stream(&self) -> Stream {
        self.stream
    }

    /// Append one line the child emitted, attributed to THIS reader's stream.
    ///
    /// A poisoned lock loses the line — the same outcome the old transcript had,
    /// and it is REPORTED rather than rendered as silence: a snapshot taken
    /// through a poisoned lock says "a reader thread panicked".
    pub fn record(&self, line: impl Into<String>) {
        if let Ok(mut log) = self.log.lock() {
            let stream = self.stream;
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
    /// **THE RESULT AND THE CLOSE ARE ONE TRANSITION, UNDER ONE LOCK** (#3652,
    /// job 262 finding 1 / job 271). This used to push the result and leave the
    /// reader's slot `Open` until `Drop` ran, so a snapshot taken in between held
    /// a failure for a reader still counted as open — see
    /// [`Transcript::reader_ended`] for what that made `pipe_status` and
    /// `stream_status` report.
    pub fn read_failed(&self, error: impl std::fmt::Display) {
        if let Ok(mut log) = self.log.lock() {
            let stream = self.stream;
            log.reader_ended(stream, Some(error.to_string()));
            // Recorded only where the transition actually happened: a poisoned
            // lock records nothing, and the `Drop` below must then still try.
            self.ended.set(true);
        }
    }
}

impl Drop for ReaderHandle {
    fn drop(&mut self) {
        // ALREADY TRANSITIONED, WITH ITS RESULT, UNDER ONE LOCK (#3652): a reader
        // that failed ended AT `read_failed`, so there is nothing left to do here
        // and nothing to record twice.
        if self.ended.get() {
            return;
        }
        if let Ok(mut log) = self.log.lock() {
            // WHICH pipe ended, not merely that one did (round 18): a bare count
            // cannot tell a wait whether the reader IT is waiting on is the one
            // that went away. `None`: this reader ended at EOF — the terminal
            // result is what distinguishes the two, and it goes through the SAME
            // transition (#3652).
            log.reader_ended(self.stream, None);
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

/// **WHY THE AWAITED READER'S OWN STATE CAN PRODUCE NOTHING FURTHER** (round 18).
///
/// A closed variant rather than a bool for the same reason [`PipeStatus`] is one:
/// each way an awaited stream can be finished has a DIFFERENT cause to report, and
/// exactly one of them is a defect in this harness rather than an observation
/// about the child.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEnded {
    /// That stream's reader ended AT EOF: the child closed it, or exited.
    Eof,
    /// That stream's reader ended in an I/O ERROR, so this harness could not read
    /// the pipe to its end. A statement about the pipe, NOT about the child
    /// (roborev job 255, finding 2, asked of ONE stream).
    ReadFailed { note: String },
    /// No reader was EVER attached to that stream, so nothing could ever be
    /// recorded on it. `ChildIo::attach` always attaches both, so this is
    /// reachable only from a harness helper that attached fewer — a defect in this
    /// test harness, and it is named as one rather than waited out.
    NeverAttached,
}

/// **WHAT THE AWAITED STREAM'S OWN READER ESTABLISHES** — the derivation a wait
/// consults about the pipe IT is waiting on (round 18).
///
/// [`PipeStatus`] answers "is output over ANYWHERE", which is the question a poll
/// has; a wait names a stream, and for a wait `PartiallyClosed` is not one answer
/// but two — the surviving pipe's and the ended one's — so consulting it alone
/// collapsed "the line can still arrive" and "the line has become impossible" onto
/// the permissive one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamStatus {
    /// The store could not be read, so NOTHING is established about that stream:
    /// not that its reader is attached, and not that it has ended.
    Unavailable,
    /// That stream's reader is still attached: the awaited line can still arrive.
    Open,
    /// That stream's reader can produce nothing further, for this reason.
    Ended(StreamEnded),
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
    /// The per-stream reader state at the moment of this read — copied under the
    /// SAME lock as `records`. PER STREAM rather than two counts (round 18): the
    /// counts below are derived from it, and a wait asks about the ONE stream it
    /// awaits, which no count can answer. Round 16 replaced a `pipes_closed` bool
    /// with those counts because "one of two pipes has ended" read as "the pipes
    /// were still open"; round 18 is the same collapse one level in — for a NAMED
    /// stream.
    readers: ReaderSlots,
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

    /// Readers ever attached, and readers still attached — DERIVED from the two
    /// per-stream lists, so a count and the per-stream state can never disagree.
    fn readers_total(&self) -> usize {
        self.readers.total()
    }

    fn readers_open(&self) -> usize {
        self.readers.open()
    }

    /// **THE ONE PLACE THE AWAITED STREAM'S OWN STATE IS DERIVED** (round 18),
    /// from the same snapshot every other verdict in this harness reads.
    ///
    /// Answered in the order the facts are established: an unreadable store
    /// establishes nothing; a stream nothing was ever attached to can never carry
    /// a record; an attached-and-not-ended reader can still deliver; and only when
    /// that stream's reader HAS ended does its terminal result decide which cause
    /// is named — from the recorded result and never from the count, which is
    /// identical for EOF and for a failed read (job 255, finding 2).
    pub fn stream_status(&self, want: Stream) -> StreamStatus {
        if !self.available {
            return StreamStatus::Unavailable;
        }
        match self.readers.get(want) {
            ReaderSlot::Unattached => StreamStatus::Ended(StreamEnded::NeverAttached),
            ReaderSlot::Open => StreamStatus::Open,
            ReaderSlot::Ended => match self.stream_failure_note(want) {
                Some(note) => StreamStatus::Ended(StreamEnded::ReadFailed { note }),
                None => StreamStatus::Ended(StreamEnded::Eof),
            },
        }
    }

    /// That ONE stream's recorded terminal result, rendered for a failure message;
    /// `None` when its reader recorded no read failure.
    fn stream_failure_note(&self, want: Stream) -> Option<String> {
        let notes = self
            .read_failures
            .iter()
            .filter(|(stream, _)| *stream == want)
            .map(|(stream, error)| format!("{} reader: {error}", stream.tag()))
            .collect::<Vec<_>>();
        (!notes.is_empty()).then(|| notes.join("; "))
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
        let ended = self.readers.ended();
        // CHECKED BEFORE ANY EOF CLAIM: a failed reader ends exactly as one at EOF
        // does, so the terminal results — not the count — decide (job 255, finding
        // 2, now applied to every consumer).
        let failure_note = self.read_failure_note();
        if self.readers_open() == 0 {
            return match failure_note {
                Some(note) => PipeStatus::ReaderFailed { note },
                None => PipeStatus::AllEof {
                    readers: self.readers_total(),
                },
            };
        }
        if ended > 0 {
            return PipeStatus::PartiallyClosed {
                open: self.readers_open(),
                ended,
                failure_note,
            };
        }
        PipeStatus::AllOpen {
            open: self.readers_open(),
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
    /// **THE READER FOR THE STREAM THIS WAIT WAS WATCHING HAD ENDED**, while at
    /// least one OTHER reader was still attached (round 18).
    ///
    /// No further line can arrive ON THE AWAITED STREAM, so the awaited line has
    /// become impossible — even though output is still possible on the surviving
    /// pipe, which is why neither `PipesClosed` nor `ReaderFailed` may be built
    /// here: both state that output is over everywhere.
    ///
    /// WHY IT IS A VARIANT AND NOT A NOTE ON `DeadlineReached`. Before this
    /// existed, `ended` consulted only the WHOLE-store state, so a partial closure
    /// was nonterminal whichever stream had gone: the wait slept out its entire
    /// remaining budget and then named the DEADLINE as the cause of a wait no
    /// deadline could have satisfied. #3515's own subject is a wrong cause — a
    /// bare timeout reported as "no graceful shutdown handler" — so a new
    /// wrong-cause path is this change's own defect, one level up.
    AwaitedStreamEnded {
        want: Stream,
        ended: StreamEnded,
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
            WaitEnd::AwaitedStreamEnded {
                want,
                ended,
                after,
                snapshot,
            } => format!(
                "how the wait ended: the child's {} reader — the ONE this wait was watching — had \
                 ENDED after {after:.3?}, so the awaited line could never arrive on it and the \
                 wait returned instead of sleeping out the rest of its budget. {}. The {} \
                 record(s) sequenced since this wait began, printed below, are the snapshot the \
                 verdict was taken from — the records, the per-stream reader state and the \
                 readers' terminal results were read under ONE lock.\n\
                 the WHOLE store's pipe state at the verdict, because the surviving pipe is where \
                 a reader of this failure should look next: {}",
                want.tag(),
                match ended {
                    StreamEnded::Eof => format!(
                        "IT ENDED AT EOF: the child closed its {} or exited (this measurement does \
                         not say which)",
                        want.tag()
                    ),
                    StreamEnded::ReadFailed { note } => format!(
                        "IT ENDED IN AN I/O ERROR rather than at EOF, so this is NOT an EOF \
                         measurement: {note}. WHAT THAT ESTABLISHES: only that this harness could \
                         not read that pipe to its end. It is a statement about the pipe, NOT \
                         about the child, and NOT about the property under test"
                    ),
                    StreamEnded::NeverAttached => format!(
                        "NO READER WAS EVER ATTACHED to the child's {}, so nothing could ever be \
                         recorded on it. This is a defect in this test harness — \
                         `ChildIo::attach` attaches both pipes — and it is NOT a statement about \
                         the child",
                        want.tag()
                    ),
                },
                snapshot.examined(),
                snapshot.pipe_status().describe()
            ),
        }
    }

    /// The transcript the DECISION examined, ready for a panic message.
    pub fn transcript(&self) -> String {
        match self {
            WaitEnd::DeadlineReached { snapshot } => snapshot.render(),
            WaitEnd::PipesClosed { snapshot, .. } => snapshot.render(),
            WaitEnd::ReaderFailed { snapshot, .. } => snapshot.render(),
            WaitEnd::AwaitedStreamEnded { snapshot, .. } => snapshot.render(),
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
/// output is over — a live awaited pipe, or an unreadable store. A caller
/// therefore cannot ask the question through one derivation and build its verdict
/// from another, and no arm of this match is unreachable.
///
/// **IT IS ASKED ABOUT THE STREAM THE WAIT NAMED, NOT ONLY ABOUT THE STORE**
/// (round 18). "Is output over" is the question a POLL has; a wait awaits a line
/// on ONE pipe, and for it a PARTIAL closure is two different answers depending on
/// WHICH pipe ended. Consulting the whole-store state alone gave the permissive
/// one for both: with the awaited reader gone and its sibling attached the wait
/// slept out its whole remaining budget — up to the 360s/600s bases in
/// `budgets.rs` — and then named the DEADLINE for a wait no deadline could have
/// satisfied.
///
/// ORDER MATTERS, AND IT IS THE WHOLE-STORE STATE FIRST. When EVERY reader has
/// ended, the established fact is the stronger one — output is over on both pipes
/// — and that is what `PipesClosed`/`ReaderFailed` say; narrowing those to the
/// awaited stream would report less than was measured. The awaited stream's own
/// state decides only where the store's does not: a partial closure, an
/// all-open store (where a stream nothing was attached to is still finished), and
/// an unreadable store (where it establishes nothing either, so the wait
/// continues).
fn ended(
    want: Stream,
    after: Duration,
    snapshot: TranscriptSnapshot,
) -> Result<WaitEnd, TranscriptSnapshot> {
    match snapshot.pipe_status() {
        PipeStatus::ReaderFailed { .. } => Ok(WaitEnd::ReaderFailed { after, snapshot }),
        PipeStatus::AllEof { .. } => Ok(WaitEnd::PipesClosed { after, snapshot }),
        PipeStatus::PartiallyClosed { .. }
        | PipeStatus::AllOpen { .. }
        | PipeStatus::Unavailable => match snapshot.stream_status(want) {
            StreamStatus::Ended(ended) => Ok(WaitEnd::AwaitedStreamEnded {
                want,
                ended,
                after,
                snapshot,
            }),
            // NOT established for the awaited pipe: its line may still arrive (or
            // the store could not be read), so no "could never arrive" verdict may
            // be built.
            StreamStatus::Open | StreamStatus::Unavailable => Err(snapshot),
        },
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
pub(super) fn spawn_reader<R: std::io::Read + Send + 'static>(reader: R, handle: ReaderHandle) {
    thread::spawn(move || {
        // Dropped when this thread ends by ANY path — normal return or unwind —
        // which is what marks THIS handle's stream closed exactly once.
        let handle = handle;
        let buf = BufReader::new(reader);
        for line in buf.lines() {
            // THE TERMINAL RESULT IS RECORDED, NOT DISCARDED (roborev job 255,
            // finding 2): `break` alone made an I/O failure indistinguishable from
            // EOF, and the wait then reported EOF as the cause.
            match line {
                Ok(line) => handle.record(line),
                Err(error) => {
                    handle.read_failed(error);
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
        let (io, handles) = Self::with_readers(&[Stream::Stdout, Stream::Stderr]);
        let mark = io.mark();
        let mut out = Some(child.stdout.take().expect("child stdout"));
        let mut err = Some(child.stderr.take().expect("child stderr"));
        // EACH HANDLE IS PAIRED WITH ITS OWN PIPE BY THE HANDLE'S OWN STREAM, not
        // by position in the list (round 18): the handle's stream is what marks a
        // pipe ended, so pairing it positionally would let a reader of stdout end
        // stderr — and a wait would then be told the wrong pipe had finished.
        for handle in handles {
            match handle.stream() {
                Stream::Stdout => {
                    spawn_reader(out.take().expect("stdout attached exactly once"), handle)
                }
                Stream::Stderr => {
                    spawn_reader(err.take().expect("stderr attached exactly once"), handle)
                }
            }
        }
        (io, mark)
    }

    /// A harness over a fresh store with one reader handle per named stream, in
    /// the order named.
    ///
    /// The one constructor: `attach` hands its handles to reader threads, and the
    /// unit tests keep theirs, so a test drives the same recording and
    /// end-of-stream paths a real reader does.
    ///
    /// TAKES THE STREAMS AND NOT A COUNT (round 18): every handle carries the
    /// stream it will record on and mark ended, so a caller cannot create a reader
    /// whose stream is decided later by whoever calls `record`. A helper may name
    /// FEWER streams than a real child has — that is how the "no reader was ever
    /// attached" state is reachable at all, and a wait on such a stream is
    /// answered rather than waited out.
    pub(super) fn with_readers(streams: &[Stream]) -> (Self, Vec<ReaderHandle>) {
        let log = Arc::new(Mutex::new(Transcript::new(streams)));
        let handles = streams
            .iter()
            .map(|stream| ReaderHandle {
                log: Arc::clone(&log),
                stream: *stream,
                ended: Cell::new(false),
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
                readers: log.readers,
                available: true,
                read_failures: log.read_failures.clone(),
            },
            Err(_) => TranscriptSnapshot {
                records: Vec::new(),
                mark: 0,
                readers: ReaderSlots::new(&[]),
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
                // whose AWAITED reader is live reports the deadline (job 236,
                // finding 3 — collapsing the two reported "pipes still open"
                // about a child whose readers had both ended).
                //
                // EOF and a failed read both END a reader, so the variant is chosen
                // from the readers' recorded TERMINAL RESULTS and never from a count
                // (job 255, finding 2). A state that establishes neither — including
                // a PARTIAL closure and an unreadable store — hands the snapshot
                // back, and the deadline is then the cause.
                return Err(match ended(want, stage.spent(), snapshot) {
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
            match ended(want, stage.spent(), snapshot) {
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
    ///
    /// **THE PROCESS-GLOBAL PANIC HOOK IS NOT TOUCHED — OPTION 1 OF THE TWO OFFERED
    /// (round 17, roborev job 262).** An earlier version of this helper silenced
    /// the panic below by installing an empty `std::panic::set_hook` and restoring
    /// the previous hook afterwards. That hook is PROCESS-GLOBAL and the two tests
    /// in this module run concurrently in one test binary, so their swaps could
    /// interleave — and a panic between the two calls skips the restore outright —
    /// leaving the SILENT hook installed for every test that ran afterwards. A
    /// later failure in this binary would then print NOTHING. `main`'s version of
    /// this file never touches the hook, so that was a hazard this change
    /// INTRODUCED: a self-inflicted loss of diagnosability inside a change whose
    /// entire subject is diagnosability.
    ///
    /// The suppression bought QUIET and nothing else — no assertion here reads the
    /// panic message; what these tests examine is the poisoned lock's observable
    /// consequence, `PipeStatus::Unavailable`. So the global mutation is DELETED
    /// rather than serialised behind a mutex (which would have left the mutation in
    /// place and merely ordered it), and the price is paid where it is harmless:
    /// the panic below prints, and its payload says that it is expected.
    fn poison(io: &ChildIo) {
        let log = Arc::clone(&io.log);
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = log.lock().expect("the store's lock before it is poisoned");
            panic!(
                "DELIBERATE TEST FIXTURE, NOT A FAILURE: poisoning the store's lock the way a \
                 reader thread panicking inside `record` would. This panic is caught by the \
                 helper that raised it, and this message is expected output of a PASSING test."
            );
        }));
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
        let (io, handles) = ChildIo::with_readers(&[Stream::Stdout, Stream::Stderr]);
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
        let (io, handles) = ChildIo::with_readers(&[Stream::Stdout]);
        let mark = io.mark();
        let handle = handles.into_iter().next().expect("one handle");
        handle.read_failed(std::io::Error::other("simulated failure"));
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
        let (io, _handles) = ChildIo::with_readers(&[Stream::Stdout, Stream::Stderr]);
        let mark = io.mark();
        poison(&io);
        assert_eq!(io.snapshot(mark).pipe_status(), PipeStatus::Unavailable);
        assert!(
            !io.snapshot(mark).pipe_status().is_terminal(),
            "an unreadable store has NOT established that output is over: treating it as \
             terminal would abandon a wait on the strength of a measurement that failed"
        );
    }

    /// **A SNAPSHOT CAN NEVER HOLD A FAILURE FOR A READER IT STILL COUNTS AS
    /// OPEN** (#3652, roborev job 262 finding 1, re-found independently by job
    /// 271).
    ///
    /// `read_failed` recorded the terminal result and `Drop` marked the reader
    /// ended under SEPARATE lock acquisitions, so a snapshot taken between them
    /// held exactly that impossible pairing. What it cost is not tidiness:
    /// `pipe_status` tests the OPEN COUNT before it looks at any terminal result,
    /// so the note was DISCARDED, and `stream_status` answered `Open` for a stream
    /// whose reader was finished — the wrong-cause path round 18 removed,
    /// reintroduced through a lock window.
    ///
    /// **THE INTERLEAVING IS FORCED, NOT RACED.** The state a snapshot could catch
    /// between those two acquisitions is precisely the state of a handle that has
    /// called `read_failed` and has not yet been dropped — a reader thread
    /// descheduled between the two lines of `spawn_reader`'s `Err` arm. So this
    /// test simply does not drop the handle, and the ordering holds on every host
    /// and at every load rather than depending on a scheduler.
    #[test]
    fn a_failed_reader_is_never_snapshotted_as_still_open() {
        // TWO readers: the one that fails, and a sibling that stays attached — the
        // shape where the old ordering lost the failure ENTIRELY, because
        // `readers_open() == 2` derived `AllOpen` and nothing consulted the note.
        let (io, handles) = ChildIo::with_readers(&[Stream::Stdout, Stream::Stderr]);
        let mark = io.mark();
        let mut handles = handles.into_iter();
        let out = handles.next().expect("stdout handle");
        let err = handles.next().expect("stderr handle");
        err.read_failed(std::io::Error::other("simulated pipe failure"));
        // DELIBERATELY NOT DROPPED YET: this is the interleaving point.

        let snapshot = io.snapshot(mark);
        match snapshot.stream_status(Stream::Stderr) {
            StreamStatus::Ended(StreamEnded::ReadFailed { note }) => assert!(
                note.contains("simulated pipe failure"),
                "the ended stream must carry its own terminal result: {note}"
            ),
            other => panic!(
                "a reader that has recorded a read failure is FINISHED, and this snapshot reports                  it as {other:?}: the failure and the close were written under separate locks, so                  a wait on this stream is told its line can still arrive and will sleep out the                  whole deadline before blaming it"
            ),
        }
        assert_eq!(
            snapshot.stream_status(Stream::Stdout),
            StreamStatus::Open,
            "the SIBLING reader is untouched: the transition must end the failed reader's stream              and no other"
        );
        match snapshot.pipe_status() {
            PipeStatus::PartiallyClosed {
                open: 1,
                ended: 1,
                failure_note: Some(note),
            } => assert!(
                note.contains("stderr reader") && note.contains("simulated pipe failure"),
                "the whole-store state must attribute the failure to the pipe that had it: {note}"
            ),
            other => panic!(
                "one reader had failed and one was still attached, and the whole-store derivation                  reports {other:?}. `AllOpen` here is the defect: the recorded failure is                  dropped on the floor because the open COUNT is what this derivation tests first"
            ),
        }
        drop((out, err));

        // AND THE CONSEQUENCE THAT DECIDES A WAIT, with a single reader: whether
        // output is OVER. `read_failed` on the only reader means no further line
        // can ever arrive — but the old ordering left that reader `Open` until its
        // handle dropped, so this derived `AllOpen`, `is_terminal()` was false, and
        // every wait and poll kept sleeping until the deadline.
        let (io, handles) = ChildIo::with_readers(&[Stream::Stdout]);
        let mark = io.mark();
        let only = handles.into_iter().next().expect("the one handle");
        only.read_failed(std::io::Error::other("the only reader failed"));
        let status = io.snapshot(mark).pipe_status();
        match &status {
            PipeStatus::ReaderFailed { note } => assert!(
                note.contains("the only reader failed"),
                "the terminal state must carry the result that produced it: {note}"
            ),
            other => panic!(
                "every reader had ended (in an I/O error) and the derivation reports {other:?}"
            ),
        }
        assert!(
            status.is_terminal(),
            "no further line can arrive once the only reader has failed, so continuing to wait              could only delay the same verdict: {}",
            status.describe()
        );
        drop(only);
    }

    /// **A WAIT AGAINST AN UNREADABLE STORE REPORTS THAT IT COULD NOT MEASURE** —
    /// it does not report that the pipes were open.
    ///
    /// The wait's VERDICT is unchanged (the awaited line is absent and the deadline
    /// is what bound it); what changes is that the message no longer asserts a pipe
    /// state the failed read could not establish.
    #[test]
    fn a_wait_against_an_unreadable_store_does_not_claim_the_pipes_were_open() {
        let (io, _handles) = ChildIo::with_readers(&[Stream::Stdout, Stream::Stderr]);
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
