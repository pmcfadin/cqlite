//! Unit coverage for the CHILD HARNESS in `mod.rs` + `transcript.rs`
//! (issues #1693, #3515).
//!
//! Split out of `mod.rs` under the campsite rule (#1135). The division of
//! responsibility is by SUBJECT, not by size — `budgets.rs` pins the ONE
//! deadline's invariants, `transcript.rs` holds THE ONE STORE and the waits that
//! read it, `mod.rs` holds the poll and the read side, and this file holds their
//! tests.
//!
//! **What round 13 deleted from this file, and why that is the point (design.md
//! D6b).** Several tests here existed to pin the boundary between TWO stores — a
//! transcript a `Mark` could window, and an `mpsc` queue it could not. With one
//! sequenced store those properties are not testable because they are not
//! EXPRESSIBLE: there is no publish step to be preempted before, no queue to hold
//! a copy of a served line, and no `Empty`-vs-`Disconnected` pair to collapse. A
//! test whose defect can no longer be written is removed rather than kept as
//! reassurance; what replaces it is the structure, plus
//! `a_matched_acknowledgement_cannot_satisfy_the_next_wait` below, which pins the
//! FALSE PASS the two-store design allowed (roborev job 247).
//!
//! A child module can see its parent's private items, so these tests exercise
//! `collect_both_streams`, `poll_with_progress_sampled` and the
//! `CollectEnd`/`WaitEnd` internals directly — the harness's public surface is
//! deliberately small and these are the seams the defects lived in.

use super::*;
use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

impl ChildIo {
    /// A `ChildIo` with no child behind it, plus the two [`ReaderHandle`]s that
    /// stand in for its reader threads.
    ///
    /// The handles are the REAL recording path — the same type and the same
    /// methods `spawn_reader` uses — so these tests drive the instrument rather
    /// than a mock of it, and dropping a handle marks that pipe closed exactly as
    /// a reader thread ending does.
    fn synthetic() -> (Self, ReaderHandle, ReaderHandle) {
        let (io, handles) = Self::with_readers(&[Stream::Stdout, Stream::Stderr]);
        let mut handles = handles.into_iter();
        let out = handles.next().expect("stdout reader handle");
        let err = handles.next().expect("stderr reader handle");
        (io, out, err)
    }

    /// A mark taken while the store is provably EMPTY — the whole sequence.
    ///
    /// The emptiness is CHECKED rather than assumed, through the harness's own
    /// rendering of the store, so a helper that silently stopped covering
    /// everything would fail here instead of narrowing a window unnoticed.
    fn mark_from_the_start(&self) -> Mark {
        assert!(
            self.transcript_text().contains("emitted nothing at all"),
            "this helper is only sound before anything has been recorded, and the store is not \
             empty:\n{}",
            self.transcript_text()
        );
        self.mark()
    }
}

/// The read side's ONE store, with `collectors` collector handles outstanding.
fn synthetic_bufs(collectors: usize) -> (Arc<Mutex<StreamBufs>>, Vec<BufHandle>) {
    let bufs = Arc::new(Mutex::new(StreamBufs {
        collectors_open: collectors,
        ..StreamBufs::default()
    }));
    let handles = (0..collectors)
        .map(|_| BufHandle {
            bufs: Arc::clone(&bufs),
        })
        .collect();
    (bufs, handles)
}

/// **A REAL CHILD FOR THE FAILURE PATHS THAT MUST TEAR ONE DOWN** (#3652, roborev
/// job 265 finding 4).
///
/// `await_write_ack` takes the child so a missing acknowledgement can kill AND reap
/// it, so its tests have to supply one. A `sleep` and not the product binary
/// deliberately: what is under test is this harness's teardown, and a stand-in that
/// cannot exit on its own makes "the child was still running afterwards"
/// unambiguous — with the product binary, an exit of its own would be
/// indistinguishable from a teardown.
fn stand_in_child() -> std::process::Child {
    std::process::Command::new("sleep")
        .arg("300")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn `sleep 300` as a stand-in child")
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
/// TERMINATES at all under continuous progress. It runs on a worker thread under a
/// 30s watchdog (100x the 300ms deadline under test) so a regression reports a
/// diagnosis instead of hanging the suite.
///
/// THE WORKER REPORTS THROUGH ONE MUTEX-GUARDED SLOT, not a channel: "the worker
/// has not reported yet" and "the worker is gone" are then two reads of one store
/// rather than two variants of a queue error — the same reason the harness itself
/// no longer has a channel (design.md D6b). A vanished worker is re-raised by
/// `join`, never reported as a hang, which is the opposite diagnosis.
#[test]
fn observed_progress_never_extends_the_deadline() {
    const WATCHDOG: Duration = Duration::from_secs(30);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let slot: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let worker_slot = Arc::clone(&slot);

    let worker = thread::spawn(move || {
        let (io, out, err) = ChildIo::synthetic();
        let deadline = TestDeadline::start(Duration::from_millis(300), Duration::from_millis(300));
        let stage = deadline.stage("synthetic");
        let outcome = poll_with_progress(&io, &data_dir, &stage, |slice, _artifacts| {
            // Progress on every single slice, through the same handle a real
            // reader thread uses. There is only one store to record into, so the
            // count the failure reports and the transcript it renders come from
            // the same read by construction.
            err.record("still working");
            thread::sleep(slice);
            None::<()>
        });
        let report = match outcome {
            Ok(_) => unreachable!("the step never completes"),
            Err(fail) => fail.observed(),
        };
        *worker_slot.lock().expect("report slot") = Some(report);
        drop(out);
    });

    let started = Instant::now();
    let observed = loop {
        if let Some(report) = slot.lock().expect("report slot").take() {
            break report;
        }
        if worker.is_finished() {
            // Re-raise the worker's own panic rather than reporting a
            // non-existent hang; `join` on a panicked thread resumes that panic.
            let _ = worker.join();
            panic!(
                "the poll worker ended without reporting: it PANICKED rather than failing to \
                 terminate (the join above should have re-raised it)"
            );
        }
        if started.elapsed() > WATCHDOG {
            panic!(
                "the progress-observing poll did not terminate within {WATCHDOG:?} despite a \
                 300ms deadline: observed progress is extending the bound, which is exactly what \
                 the round-8 descope removed"
            );
        }
        thread::sleep(Duration::from_millis(5));
    };
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

// ---------------------------------------------------------------------------
// EVIDENCE THAT ARRIVED BEFORE THE DEADLINE IS ACCEPTED AFTER IT LAPSES
// (roborev job 233, finding 1 — the round-9 ruling, applied where it had only
// been half-applied)
// ---------------------------------------------------------------------------
//
// Round 9 ruled that the deadline bounds how long the test WAITS FOR EVIDENCE
// and not whether it accepts evidence already in hand, and applied that to the
// SUCCESS path only. The three expiry sites got it wrong in the other direction:
// each declared a timeout without a last look at what had already been recorded.
// Under contention the awaited marker, the process exit or a collector's buffer
// can arrive well before the deadline and be observed only after it, because this
// thread is descheduled in between. The result was a false timeout on a working
// product — and, worst of all, a message CONTRADICTED BY ITS OWN TRANSCRIPT.
//
// The three tests below each record the evidence FIRST, then let the deadline
// lapse, then let the harness look: each must SUCCEED.
//
// ON THE SLEEPS: a `sleep` can only OVERSHOOT, and overshoot makes the
// precondition ("the deadline has already lapsed") MORE true, never less. No
// test here asserts that anything completed FAST, so this is the opposite of the
// #2642 wall-clock flake class.

/// `wait_for`: a line recorded before the deadline must still match.
#[test]
fn a_line_recorded_before_the_deadline_is_matched_after_it_lapses() {
    let (io, out, err) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();
    err.record(format!("cqlite: {MARKER_HANDLER_ENTERED} before exit..."));

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("recorded-before-expiry");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match io.wait_for(
        mark,
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        &stage,
    ) {
        Ok((line, _)) => assert!(line.contains(MARKER_HANDLER_ENTERED), "{line}"),
        Err(end) => panic!(
            "the marker had ALREADY been recorded when the deadline lapsed, and the wait reported \
             {end:?} instead of matching it. That is a false timeout on a working product; it is \
             also a self-contradicting diagnostic, because the transcript the failure prints \
             contains the very marker the message says was never observed:\n{}",
            end.transcript()
        ),
    }
    // The readers are kept alive to the end of the test on purpose: a dropped
    // handle means EOF, and this test is about the deadline, not about closure.
    drop((out, err));
}

/// `poll_with_progress`: a step whose evidence landed before the deadline must
/// still be observed, not reported as a timeout.
#[test]
fn a_step_completed_before_the_deadline_is_observed_after_it_lapses() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    // The durable artifact APPEARS before the deadline...
    std::fs::write(data_dir.join("nb-1-big-Data.db"), b"x").expect("plant an artifact");

    let (io, out, err) = ChildIo::synthetic();
    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    // ...and only then does the deadline lapse, before the poll ever looks.
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("recorded-before-expiry");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    let outcome = poll_with_progress(&io, &data_dir, &stage, |slice, artifacts| {
        // The poll's OWN sample, not a scan of our own: one scan per iteration is
        // what the documented overrun bound rests on (job 236, finding 2).
        if artifacts >= 1 {
            Some(())
        } else {
            thread::sleep(slice);
            None
        }
    });
    if let Err(fail) = outcome {
        panic!(
            "the artifact existed BEFORE the deadline lapsed, and the poll reported a timeout \
             anyway — a false failure on a working product: {}",
            fail.observed()
        );
    }
    drop((out, err));
}

/// `collect_both_streams`: buffers delivered before the deadline must still be
/// collected, not reported as a partial collection.
#[test]
fn read_side_buffers_delivered_before_the_deadline_are_collected_after_it_lapses() {
    let (bufs, handles) = synthetic_bufs(2);
    handles[0].deliver(Stream::Stdout, b"[]".to_vec());
    handles[1].deliver(Stream::Stderr, Vec::new());

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("delivered-before-expiry");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match collect_both_streams(&bufs, &stage) {
        CollectEnd::Both(out, err) => {
            assert_eq!(
                out, b"[]",
                "the delivered stdout buffer must be the one returned"
            );
            assert!(
                err.is_empty(),
                "the delivered stderr buffer must be returned"
            );
        }
        CollectEnd::DeadlineReached { collected } => panic!(
            "both collector buffers were delivered BEFORE the deadline lapsed, and the collection \
             reported a timeout with {collected}/2 collected — a false failure against a child \
             that had already exited successfully"
        ),
        CollectEnd::CollectorsEnded { collected } => panic!(
            "unexpected end-of-collectors with {collected}/2 collected: both handles are alive"
        ),
        CollectEnd::ReadFailed { failures, .. } => {
            panic!("no collector reported a read failure, yet one was reported: {failures}")
        }
        CollectEnd::Unavailable => panic!("the store's lock was poisoned unexpectedly"),
    }
    drop(handles);
}

// ---------------------------------------------------------------------------
// THE WINDOW IS A SEQUENCE POSITION, SO A SERVED RECORD CANNOT BE RE-SERVED
// (roborev job 247, finding 2 — the class that ended the two-store design)
// ---------------------------------------------------------------------------

/// The window is what keeps a CUMULATIVE store from turning into a false pass: a
/// record sequenced BEFORE a wait began belongs to an earlier stage, which already
/// consumed or discarded it. `await_write_ack` awaits five separate `OK`s in one
/// test, so without the window the first would satisfy all five and a wedged
/// session would read as green.
#[test]
fn a_line_recorded_before_the_wait_began_does_not_satisfy_it() {
    let (io, out, err) = ChildIo::synthetic();
    out.record("OK");
    // The mark is taken AFTER the earlier stage's line, exactly as a call site
    // takes it after the previous ack returned and before the next statement is
    // written. Moving the mark EARLIER (job 243, finding 1) closes a race at the
    // start of a wait; it must not widen the window backwards over a served record.
    let mark = io.mark();

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("stale-record");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match io.wait_for(mark, Stream::Stdout, |l| l.trim() == "OK", &stage) {
        Ok((line, _)) => panic!(
            "a record sequenced BEFORE this wait began satisfied it ({line:?}): the cumulative \
             store is being read as if every record were new, so one earlier acknowledgement \
             would satisfy every later wait for one"
        ),
        Err(WaitEnd::DeadlineReached { snapshot }) => assert_eq!(
            snapshot.examined(),
            0,
            "the wait must report the size of the window it examined, and the stale record is \
             outside it"
        ),
        Err(other) => panic!("expected a deadline, got {other:?}"),
    }
    drop((out, err));
}

/// **THE FALSE PASS THE TWO-STORE DESIGN ALLOWED (roborev job 247): an `OK` a
/// completed wait has already accepted must not satisfy the NEXT wait.**
///
/// The shape, on the pre-descope harness. A reader RECORDED the ack into the
/// transcript and was descheduled before PUBLISHING it to the channel. Write N's
/// wait expired, took its final look at the transcript, found the ack there and
/// returned `Ok` — correctly. The reader then published, and that queued COPY
/// outlived the wait that had already consumed it: `Mark` could window the
/// transcript and could not window a queue, so write N+1's wait received the
/// stale `OK` from `recv_timeout` and accepted it as its own acknowledgement. In
/// the sibling test's five-write loop, one ack could therefore satisfy several
/// waits — a wedged session reading as green, which is a vacuous pass and not a
/// diagnostic wart.
///
/// **What this test asserts now, and why the race is no longer expressible.**
/// There is ONE store and a [`Mark`] is a position in its SEQUENCE, so serving a
/// record cannot duplicate it: the second wait's window starts after the record
/// the first wait matched, and no "late publication" step exists to reintroduce
/// it. The assertion is exactly that — a completed wait's evidence is invisible
/// to the next wait's window — and it FAILS against the pre-descope behaviour,
/// where the queued copy satisfied the second wait (RED-verified against the
/// two-store harness in a throwaway worktree; recorded in tasks.md round 13).
///
/// A later record with the SAME TEXT is deliberately NOT excluded: it is a new
/// event at a new sequence position, which is precisely what write N+1's genuine
/// acknowledgement is.
#[test]
fn a_matched_acknowledgement_cannot_satisfy_the_next_wait() {
    let (io, out, err) = ChildIo::synthetic();
    let first_mark = io.mark_from_the_start();

    // Write N's acknowledgement, recorded by the reader.
    out.record("OK");

    let deadline = TestDeadline::start(Duration::from_secs(30), Duration::from_secs(30));
    let first = deadline.stage("ack-N");
    let (line, _) = io
        .wait_for(first_mark, Stream::Stdout, |l| l.trim() == "OK", &first)
        .expect("write N's acknowledgement was recorded, so its wait must match it");
    assert_eq!(line.trim(), "OK");
    let _ = first.finish();

    // Write N+1: the mark is taken before the statement is written, as every call
    // site does. The child then says NOTHING — the session is wedged.
    let next_mark = io.mark();

    let short = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let second = short.stage("ack-N+1");
    assert!(
        second.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match io.wait_for(next_mark, Stream::Stdout, |l| l.trim() == "OK", &second) {
        Ok((line, _)) => panic!(
            "write N+1's wait accepted {line:?} — the acknowledgement write N's wait had ALREADY \
             matched. One ack satisfied two waits, so a wedged session reads as green: this is \
             the vacuous pass the two-store design allowed (roborev job 247), and with one \
             sequenced store it must not be expressible"
        ),
        Err(WaitEnd::DeadlineReached { snapshot }) => assert_eq!(
            snapshot.examined(),
            0,
            "write N+1's window must be empty: the only record in the store was served to write \
             N's wait, and nothing has been recorded since"
        ),
        Err(other) => panic!(
            "expected a deadline with the pipes still open, got {other:?} — the readers are alive"
        ),
    }
    drop((out, err));
}

// ---------------------------------------------------------------------------
// END OF STREAM IS A FIELD OF THE ONE STORE, READ UNDER THE SAME LOCK AS THE
// RECORDS (design.md D6b — the `Empty`-vs-`Disconnected` family, retired rather
// than fixed at a fourth site)
// ---------------------------------------------------------------------------

/// An expiry racing pipe closure must name the cause the measurement supports
/// (roborev job 236, finding 3). Every reader has ended, so no further line can
/// arrive: reporting `DeadlineReached` would tell the reader the pipes were
/// "still open (more output was still possible)" about a child whose output is
/// over — a message contradicted by the same read that produced it.
///
/// A non-matching line is recorded FIRST, so the verdict must be taken from a read
/// that saw that record as well as the closure: the records and the end-of-stream
/// fact come from one lock acquisition, which is what makes "checked everything
/// before reporting closure" structural rather than a claim about `try_recv`.
#[test]
fn an_expiry_racing_pipe_closure_reports_closed_pipes() {
    let (io, out, err) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();
    err.record("some other output");
    drop((out, err));

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("expiry-races-eof");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match io.wait_for(
        mark,
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        &stage,
    ) {
        Err(WaitEnd::PipesClosed { .. }) => {}
        Err(WaitEnd::DeadlineReached { .. }) => panic!(
            "every reader had ended when the deadline lapsed, and the wait reported a deadline \
             with the pipes \"still open\" — the message names a cause its own final read \
             contradicts (AC2)"
        ),
        Err(WaitEnd::ReaderFailed { .. }) => panic!(
            "both readers here ended at EOF, yet the wait reported one of them as having failed"
        ),
        Err(WaitEnd::AwaitedStreamEnded { .. }) => panic!(
            "EVERY reader had ended here, so the established fact is the stronger whole-store one \
             — output is over on BOTH pipes — and narrowing the verdict to the awaited stream \
             alone would report less than this snapshot measured (round 18)"
        ),
        Ok((line, _)) => panic!("nothing matching was ever recorded, yet {line:?} matched"),
    }
}

/// **THE AWAITED STREAM'S OWN VERDICT** — the round-18 coverage, moved out under
/// the campsite rule (#1135): this file was 10 lines PAST the 1500-line test
/// threshold with those four tests in it. Split by SUBJECT: everything about
/// which pipe a WAIT was watching is there, everything else about the harness
/// stays here. A child module sees its parent's private items, so those tests
/// drive `ChildIo::synthetic` and `mark_from_the_start` exactly as the tests here
/// do.
#[cfg(test)]
mod awaited_stream_tests;

/// **THE HARNESS'S OWN SOURCE-LEVEL INVARIANTS** — properties about EVERY site,
/// which no behavioural test can cover because it cannot see a site nobody has
/// written yet (#3652). Split out under the campsite rule (#1135): with the
/// teardown guard inline this file was 28 lines PAST the 1500-line test threshold.
#[cfg(test)]
mod source_guards;

/// A pipe stand-in that yields one whole line and then FAILS — what a reader
/// thread sees when a `read(2)` on the child's pipe errors mid-stream.
///
/// It exists so the two tests below can drive `spawn_reader` and `collect_to_end`
/// THEMSELVES, at the exact `Err` branches roborev job 255 finding 2 was about.
/// Driving only the store beneath them would leave those branches uncovered, and
/// they are the sites that discarded the result.
#[derive(Default)]
struct FailsAfterOneLine {
    emitted: bool,
}

impl std::io::Read for FailsAfterOneLine {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.emitted {
            return Err(std::io::Error::other("simulated pipe failure"));
        }
        self.emitted = true;
        let line = b"a partial first line\n";
        let n = line.len().min(buf.len());
        buf[..n].copy_from_slice(&line[..n]);
        Ok(n)
    }
}

/// Poll a store until every reader has ended, bounded so a defect cannot hang the
/// suite. Returns whether it ended.
fn wait_for_readers_to_end(io: &ChildIo) -> bool {
    let mark = io.mark();
    for _ in 0..2_000 {
        if io.snapshot(mark).pipe_status().is_terminal() {
            return true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    false
}

/// **THE READER THREAD ITSELF RECORDS ITS FAILURE** (roborev job 255, finding 2) —
/// driven through `spawn_reader`, which is where the `Err` was discarded.
///
/// The store-level test below pins how the verdict is CHOSEN; this one pins that
/// the choice is ever reachable from a real failing read. Without it, reverting
/// `spawn_reader` to `let Ok(line) = line else { break }` would leave every test
/// green.
#[test]
fn a_reader_thread_records_a_failed_read_rather_than_ending_silently() {
    let (io, handles) = ChildIo::with_readers(&[Stream::Stdout]);
    let mark = io.mark();
    let handle = handles.into_iter().next().expect("one reader handle");
    spawn_reader(FailsAfterOneLine::default(), handle);

    assert!(
        wait_for_readers_to_end(&io),
        "the reader thread must end after its read fails"
    );
    let snapshot = io.snapshot(mark);
    assert!(
        matches!(snapshot.pipe_status(), PipeStatus::ReaderFailed { .. }),
        "the reader ended because its read FAILED, and the store records only that it ended: \
         a failure is then indistinguishable from EOF, which is the cause the wait goes on to \
         name (job 255, finding 2). Derived state: {:?}",
        snapshot.pipe_status()
    );
    let note = snapshot
        .read_failure_note()
        .expect("a failed reader must leave a terminal result");
    assert!(
        note.contains("simulated pipe failure") && note.contains("stdout"),
        "the recorded result must name the stream and carry the reader's own error: {note}"
    );
    assert_eq!(
        snapshot.examined(),
        1,
        "the line read BEFORE the failure must still be recorded: the failure is about the rest \
         of the stream, not about what was already seen"
    );
}

/// **THE COLLECTOR THREAD ITSELF RECORDS ITS FAILURE, AND DELIVERS NOTHING**
/// (roborev job 255, finding 2) — driven through `collect_to_end`, which is where
/// `read_to_end`'s result was discarded and the partial buffer delivered as whole.
#[test]
fn a_collector_thread_that_fails_mid_read_delivers_nothing() {
    let (bufs, handles) = synthetic_bufs(2);
    let mut handles = handles.into_iter();
    let failing = handles.next().expect("stdout collector handle");
    let ok = handles.next().expect("stderr collector handle");
    ok.deliver(Stream::Stderr, Vec::new());
    collect_to_end(Stream::Stdout, FailsAfterOneLine::default(), failing);

    let deadline = TestDeadline::start(Duration::from_secs(30), Duration::from_secs(30));
    let stage = deadline.stage("collector-thread-read-failure");
    match collect_both_streams(&bufs, &stage) {
        CollectEnd::ReadFailed { failures, .. } => assert!(
            failures.contains("simulated pipe failure"),
            "the verdict must carry the collector's own error: {failures}"
        ),
        CollectEnd::Both(out, _) => panic!(
            "the collector's read FAILED and the collection returned both buffers — the {} \
             partial byte(s) it had read were presented as the whole of the child's stdout \
             (job 255, finding 2)",
            out.len()
        ),
        other => panic!(
            "a failed read must be reported as such, not as {}",
            match other {
                CollectEnd::DeadlineReached { .. } => "a deadline (which was live)",
                CollectEnd::CollectorsEnded { .. } => "end-of-collectors",
                CollectEnd::Unavailable => "a poisoned lock",
                _ => unreachable!("handled above"),
            }
        ),
    }
    drop(ok);
}

/// **A READER THAT ENDS IN AN I/O ERROR IS NOT REPORTED AS EOF** (roborev job 255,
/// finding 2).
///
/// The reader threads did `let Ok(line) = line else { break }`, so a genuine pipe
/// read failure ended the reader exactly as EOF does — and the resulting failure
/// said, in as many words, that "the child's stdout AND stderr both reached EOF",
/// a cause the measurement had not established. The verdict is the same either way
/// (the awaited line is absent), so this is a WRONG-CAUSE diagnostic and not a
/// false pass; it is fixed as one, by recording each reader's terminal result and
/// choosing the variant from it.
#[test]
fn a_reader_that_ends_in_an_io_error_is_not_reported_as_eof() {
    let (io, out, err) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();
    err.record("some other output");
    // The same path a real reader thread takes when `BufRead::lines` yields `Err`.
    err.read_failed(std::io::Error::other("simulated pipe failure"));
    drop((out, err));

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("reader-io-error");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match io.wait_for(
        mark,
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        &stage,
    ) {
        Err(end @ WaitEnd::ReaderFailed { .. }) => {
            let described = end.describe();
            assert!(
                described.contains("simulated pipe failure"),
                "the failure must carry the reader's own terminal error: {described}"
            );
            assert!(
                !described.contains("both reached EOF"),
                "a failed read must not be described as EOF: {described}"
            );
        }
        Err(WaitEnd::PipesClosed { .. }) => panic!(
            "a reader ended in an I/O ERROR and the wait reported that the child's stdout AND \
             stderr both reached EOF — a cause this measurement never established (job 255, \
             finding 2)"
        ),
        Err(WaitEnd::DeadlineReached { .. }) => {
            panic!("every reader had ended, yet the wait reported the pipes still open")
        }
        Err(WaitEnd::AwaitedStreamEnded { .. }) => panic!(
            "EVERY reader had ended here, so the established fact is the stronger whole-store one \
             — output is over on BOTH pipes — and narrowing the verdict to the awaited stream \
             alone would report less than this snapshot measured (round 18)"
        ),
        Ok((line, _)) => panic!("nothing matching was ever recorded, yet {line:?} matched"),
    }
}

/// `collect_both_streams`: collector threads that END without delivering both
/// buffers must be reported as `CollectorsEnded`, not as a deadline.
///
/// The old expiry drain used `let Ok(..) = rx.try_recv() else { break }`, which
/// stopped identically on an empty queue and on a dead sender — so a harness
/// defect the variant exists to name was reported as a timeout against the
/// deadline instead. With one store the two facts are separate fields, so the
/// collapse is not expressible; the variant still carries `collected`, so the
/// message says how far the collection got.
#[test]
fn read_side_collectors_that_end_without_delivering_report_collectors_ended() {
    let (bufs, handles) = synthetic_bufs(2);
    // ONE buffer delivered, then every collector gone. The delivered buffer is
    // recorded in the SAME store as the collector count, so the count in the
    // verdict provably includes it.
    handles[0].deliver(Stream::Stdout, b"[]".to_vec());
    drop(handles);

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("expiry-races-collector-death");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match collect_both_streams(&bufs, &stage) {
        CollectEnd::CollectorsEnded { collected } => assert_eq!(
            collected, 1,
            "the verdict must count the delivered buffer, so the number in the message is how far \
             the collection actually got"
        ),
        CollectEnd::DeadlineReached { collected } => panic!(
            "every collector thread had ENDED without delivering ({collected}/2 collected) and \
             the collection reported a deadline — blaming the test's bound for a harness defect \
             the `CollectorsEnded` variant already exists to name"
        ),
        CollectEnd::Both(..) => {
            panic!("only one buffer was ever delivered, yet both were returned")
        }
        CollectEnd::ReadFailed { failures, .. } => {
            panic!("no collector reported a read failure, yet one was reported: {failures}")
        }
        CollectEnd::Unavailable => panic!("the store's lock was poisoned unexpectedly"),
    }
}

/// **A COLLECTOR THAT COULD NOT READ ITS PIPE TO THE END IS PROPAGATED, NOT
/// PRESENTED AS A COMPLETE STREAM** (roborev job 255, finding 2).
///
/// `collect_to_end` did `let _ = reader.read_to_end(&mut buf)` and then DELIVERED
/// `buf`, so a read that failed mid-stream handed the truncated bytes over as the
/// whole of the child's output — and stage (e) would have parsed its rows out of
/// them. HONEST SCOPE: a truncated JSON buffer would most likely fail loudly at the
/// parse, so what this closes is a WRONG-CAUSE diagnostic (a read failure reported
/// as malformed rows, or as a deadline) rather than a false pass. It is fixed as
/// one: the failure is recorded instead of delivered, and it is checked BEFORE the
/// "both buffers are here" branch so it can never be reported as success.
#[test]
fn a_read_side_collector_that_fails_mid_stream_is_reported_as_a_read_failure() {
    let (bufs, handles) = synthetic_bufs(2);
    // stderr delivered whole; stdout's collector failed after a partial read, so it
    // has nothing whole to deliver and delivers nothing.
    handles[1].deliver(Stream::Stderr, Vec::new());
    handles[0].read_failed(
        Stream::Stdout,
        std::io::Error::other("simulated pipe failure"),
        7,
    );

    // A LIVE deadline: the collection must not need to run out of time, and must not
    // blame one, to report a read failure.
    let deadline = TestDeadline::start(Duration::from_secs(30), Duration::from_secs(30));
    let stage = deadline.stage("collector-read-failure");

    match collect_both_streams(&bufs, &stage) {
        CollectEnd::ReadFailed {
            failures,
            collected,
        } => {
            assert!(
                failures.contains("simulated pipe failure") && failures.contains("Stdout"),
                "the verdict must name the failing stream and carry its own error: {failures}"
            );
            assert!(
                failures.contains('7'),
                "the verdict must say how much had been read, so the reader knows the buffer was \
                 partial: {failures}"
            );
            assert_eq!(
                collected, 1,
                "the count must be how many streams were collected WHOLE"
            );
        }
        CollectEnd::Both(out, _) => panic!(
            "a collector's read FAILED and the collection returned both buffers — the {} \
             partial byte(s) it managed to read would have been used as the whole of the \
             child's output (job 255, finding 2)",
            out.len()
        ),
        CollectEnd::DeadlineReached { collected } => panic!(
            "the deadline was live ({collected}/2 collected): a read failure must not be \
             reported as a timeout"
        ),
        CollectEnd::CollectorsEnded { collected } => panic!(
            "both handles are alive ({collected}/2 collected): a read failure must not be \
             reported as end-of-collectors"
        ),
        CollectEnd::Unavailable => panic!("the store's lock was poisoned unexpectedly"),
    }
    drop(handles);
}

/// `poll_with_progress`: a poll that gives up with BOTH pipes at EOF must say so.
///
/// The old `drain_new` was `while try_recv().is_ok()`, so it stopped identically on
/// an empty queue and on a dead sender and the poll could report "0 new output
/// lines" — implying more output was still possible — about a child whose stdout
/// and stderr had both closed. The fact now comes from the same snapshot the
/// counts do.
#[test]
fn a_poll_that_gives_up_with_closed_pipes_reports_closed_pipes() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (io, out, err) = ChildIo::synthetic();
    err.record("some output");
    drop((out, err));

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("poll-with-closed-pipes");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    let outcome = poll_with_progress(&io, dir.path(), &stage, |_slice, _artifacts| None::<()>);
    let fail = match outcome {
        Ok(_) => unreachable!("the step never completes"),
        Err(fail) => fail,
    };
    // THE STATE FIRST, THEN THE SENTENCE (round 16). Asserting only on the rendered
    // text lets the message drift away from what was derived; asserting only on the
    // state lets the message stop saying it.
    assert!(
        matches!(fail.pipes(), PipeStatus::AllEof { readers: 2 }),
        "both readers ended AT EOF, so that is the state the verdict must carry: {:?}",
        fail.pipes()
    );
    let observed = fail.observed();
    assert!(
        observed.contains("ended AT EOF"),
        "the poll gave up with every reader gone and did not report it, so the message implies \
         more output was still possible: {observed}"
    );
}

/// **A POLL THAT GIVES UP AFTER A FAILED READ MUST NOT REPORT EOF** (roborev job
/// 259, finding 1) — the poll-side twin of
/// `a_reader_that_ends_in_an_io_error_is_not_reported_as_eof`.
///
/// THIS IS THE TEST THAT WAS MISSING, and its absence is the whole shape of the
/// finding. Round 15 recorded each reader's terminal result, taught `WaitEnd` to
/// choose its variant from it, and covered THAT site — while `PollFail` went on
/// inferring pipe state from `readers_open` alone, so the identical wrong-cause
/// claim ("the child's stdout AND stderr had BOTH reached EOF") survived one
/// function over with every test green.
#[test]
fn a_poll_that_gives_up_after_a_failed_read_does_not_report_eof() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (io, out, err) = ChildIo::synthetic();
    err.record("some output");
    // The same path a real reader thread takes when `BufRead::lines` yields `Err`.
    err.read_failed(std::io::Error::other("simulated pipe failure"));
    drop((out, err));

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("poll-after-failed-read");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    let fail = match poll_with_progress(&io, dir.path(), &stage, |_slice, _artifacts| None::<()>) {
        Ok(_) => unreachable!("the step never completes"),
        Err(fail) => fail,
    };
    assert!(
        matches!(fail.pipes(), PipeStatus::ReaderFailed { .. }),
        "a reader ended in an I/O ERROR, so the verdict may not carry an EOF state: {:?}",
        fail.pipes()
    );
    let observed = fail.observed();
    assert!(
        observed.contains("simulated pipe failure"),
        "the poll must carry the reader's own terminal error: {observed}"
    );
    assert!(
        !observed.contains("ended AT EOF"),
        "a failed read must not be reported as EOF — the cause this measurement never \
         established (job 259, finding 1): {observed}"
    );
}

/// **ONE PIPE ENDED IS NOT \"THE PIPES WERE STILL OPEN\"** (roborev job 259,
/// finding 1): the second claim `readers_open` as a bool had to collapse.
///
/// With one of two readers gone, `readers_open == 1` left the old bool `false` and
/// the poll reported that "the child's pipes were still open ... so more output was
/// still possible" — true of the surviving pipe and false of the ended one. Here
/// the ended reader is the one whose stream the awaited evidence would come from,
/// which is exactly the case where reading that sentence sends a reader looking in
/// the wrong place.
#[test]
fn a_poll_with_one_reader_still_attached_reports_partial_closure() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (io, out, err) = ChildIo::synthetic();
    err.record("some output");
    // stderr's reader ends AT EOF; stdout's stays attached.
    drop(err);

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("poll-partial-closure");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    let fail = match poll_with_progress(&io, dir.path(), &stage, |_slice, _artifacts| None::<()>) {
        Ok(_) => unreachable!("the step never completes"),
        Err(fail) => fail,
    };
    assert!(
        matches!(
            fail.pipes(),
            PipeStatus::PartiallyClosed {
                open: 1,
                ended: 1,
                failure_note: None,
            }
        ),
        "one reader had ended at EOF and one was still attached: {:?}",
        fail.pipes()
    );
    let observed = fail.observed();
    assert!(
        observed.contains("still attached") && observed.contains("ONLY on the surviving"),
        "the poll must say WHICH pipes could still produce output: {observed}"
    );
    assert!(
        !observed.contains("were still attached when the verdict was taken"),
        "one of the two readers had ENDED, so the message may not report that all of them were \
         still attached (job 259, finding 1): {observed}"
    );
    drop(out);
}

// ---------------------------------------------------------------------------
// CLASS B: EXACTLY ONE ARTIFACT SCAN PER ITERATION, AND AT MOST ONE PAST THE
// DEADLINE (roborev job 243, finding 2)
// ---------------------------------------------------------------------------
//
// `poll_with_progress` documents an overrun bound of one slice plus ONE recursive
// `count_data_db` walk. That bound has been wrong three times, and each time it
// was wrong it was BELIEVED, because it was argued from reading the loop. These
// two tests MEASURE it through the `sample` seam, so the next redundant walk reds
// the fast loop instead of surviving another review round.
//
// Neither test asserts a duration, and neither depends on how many slices fit in
// a deadline: the scan census test terminates from INSIDE its own step after a
// fixed number of iterations, and the expiry test starts from an already-lapsed
// deadline (a sleep can only overshoot, which makes that precondition more true).

/// A poll entered when the deadline has ALREADY lapsed must take exactly ONE
/// artifact scan.
///
/// The old loop took the baseline and then immediately scanned again at the first
/// loop top, before any deadline check — so this poll walked the directory TWICE
/// after expiry while the documented bound promised one walk. On a loaded host,
/// where a recursive `read_dir` is not quick, that is the difference the bound
/// exists to state.
#[test]
fn an_already_expired_poll_takes_exactly_one_artifact_scan() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (io, out, err) = ChildIo::synthetic();

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("already-expired-scan-bound");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    let samples = Cell::new(0usize);
    let outcome = poll_with_progress_sampled(
        &io,
        dir.path(),
        || {
            samples.set(samples.get() + 1);
            0
        },
        &stage,
        // No triggering operation: this test is about the SCAN BOUND, and a
        // trigger would add a step between the window and the loop (#3652).
        || {},
        |_slice, _artifacts| None::<()>,
    );
    assert!(
        outcome.is_err(),
        "the step never completes, so an expired poll must give up"
    );
    assert_eq!(
        samples.get(),
        1,
        "a poll entered past its deadline took {} recursive artifact scans; the documented \
         overrun bound is ONE, and this bound gets FIXED rather than weakened",
        samples.get()
    );
    drop((out, err));
}

/// Every iteration takes exactly one artifact scan — no redundant walk.
///
/// Asserted as an EQUALITY against the step-invocation count, which is the
/// property ("one sample per iteration") rather than a magic number that would
/// have to be re-derived whenever the loop changes. The poll ends from inside its
/// own step after a fixed number of iterations, so nothing here depends on how
/// many 100ms slices fit into a deadline — this is not a wall-clock assert.
#[test]
fn every_poll_iteration_takes_exactly_one_artifact_scan() {
    const ITERATIONS: usize = 4;
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (io, out, err) = ChildIo::synthetic();

    // Generous, and deliberately never reached: the step below ends the poll.
    let deadline = TestDeadline::start(Duration::from_secs(30), Duration::from_secs(30));
    let stage = deadline.stage("scan-census");

    let samples = Cell::new(0usize);
    let steps = Cell::new(0usize);
    let outcome = poll_with_progress_sampled(
        &io,
        dir.path(),
        || {
            samples.set(samples.get() + 1);
            0
        },
        &stage,
        || {},
        |_slice, _artifacts| {
            steps.set(steps.get() + 1);
            if steps.get() >= ITERATIONS {
                Some(())
            } else {
                None
            }
        },
    );
    assert!(
        outcome.is_ok(),
        "the step completes on iteration {ITERATIONS}"
    );
    assert_eq!(
        steps.get(),
        ITERATIONS,
        "the poll must have run {ITERATIONS} iterations for the census below to mean anything"
    );
    assert_eq!(
        samples.get(),
        steps.get(),
        "{} recursive artifact scans across {} poll iterations: the bound the poll documents is \
         ONE sample per iteration, reused by the progress accounting, `step`, the expiry check \
         and the failure message",
        samples.get(),
        steps.get()
    );
    drop((out, err));
}

// ---------------------------------------------------------------------------
// CLASS A: THE DECISION AND THE MESSAGE ARE THE SAME SNAPSHOT, AND THE WINDOW
// OPENS BEFORE THE OPERATION (roborev job 243, finding 1)
// ---------------------------------------------------------------------------
//
// Round 11 established "decide from the store you print from" and implemented it
// as: decide from the transcript, then RE-READ the transcript to render. Two reads
// under separate locks is not one snapshot, and the mark still opened the window
// AFTER the operation that produces the awaited line. So the same class had two
// live boundary races, one at each end of the wait, and these two tests pin them.
//
// Both survive the round-13 descope unchanged in INTENT, and both got simpler in
// EVIDENCE: with one store there is no "recorded but not published" state to
// arrange, so the first test now records exactly what a reader records and the
// property under test — a line recorded between the mark and the wait is inside
// the window — is asserted against the real recording path.

/// A line RECORDED BEFORE THE WAIT WAS STARTED — but after the mark — must be
/// matched at expiry.
///
/// This is the race at the START of a wait. The call site sends a statement (or a
/// signal) and only then calls `wait_for`; a reader thread can record the response
/// in that gap and be descheduled before the wait ever runs. With the mark taken
/// INSIDE `wait_for`, that record was BEFORE the mark and outside the window — so
/// the harness declared a timeout against evidence it already held and printed the
/// marker in the very message denying it.
///
/// The interleaving is FORCED, not raced: the record is made before `wait_for` is
/// called, so the ordering holds on every host and at every load.
#[test]
fn a_line_recorded_before_the_wait_started_is_matched_at_expiry() {
    let (io, out, err) = ChildIo::synthetic();
    // Taken BEFORE the operation, as every call site now does.
    let mark = io.mark_from_the_start();

    let marker = format!("cqlite: {MARKER_HANDLER_ENTERED} — flushing memtable before exit...");
    // ...the "operation" happens here, and its response is recorded before the
    // wait is entered.
    err.record(marker.clone());

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("recorded-before-the-wait-started");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match io.wait_for(
        mark,
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        &stage,
    ) {
        Ok((line, _)) => assert_eq!(
            line, marker,
            "the matched line must be the child's own text: stream attribution is a FIELD of the \
             record, not a prefix on its text, so nothing has to be stripped back off"
        ),
        Err(end) => panic!(
            "the awaited marker was recorded BEFORE the wait was started and the wait reported \
             {end:?} anyway: the window opens after the operation, so a response recorded in the \
             gap between the operation and the wait is outside it.\n{}\ntranscript the message \
             would print:\n{}",
            end.describe(),
            end.transcript()
        ),
    }
    drop((out, err));
}

/// A line appended AFTER the expiry decision must not appear in the failure.
///
/// This is the race at the END of a wait. Round 11 decided from the transcript and
/// then re-read it to render, so a reader recording between those two reads put
/// the awaited marker into a message that had just called it absent — the
/// self-contradicting diagnostic, one lock acquisition later. One snapshot serves
/// both, so the message is literally the records the verdict was taken from.
#[test]
fn a_line_appended_after_the_decision_cannot_contradict_the_failure() {
    let (io, out, err) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("append-after-the-decision");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    let end = io
        .wait_for(
            mark,
            Stream::Stderr,
            |l| l.contains(MARKER_HANDLER_ENTERED),
            &stage,
        )
        .expect_err("nothing was ever recorded, so the wait must give up");

    // The reader thread lands the awaited marker AFTER the verdict was taken —
    // which on the real path is the window between the decision and the panic.
    err.record(format!("cqlite: {MARKER_HANDLER_ENTERED} — flushing..."));

    let rendered = end.transcript();
    assert!(
        !rendered.contains(MARKER_HANDLER_ENTERED),
        "the failure renders a line appended AFTER its verdict was taken, so the message prints \
         the very marker it says was never observed. The decision and the render must be ONE \
         snapshot, not two reads of one store:\n{rendered}"
    );
    let described = end.describe();
    assert!(
        described.contains("0 record(s)"),
        "the count in the message must come from the same snapshot it renders: {described}"
    );
    drop((out, err));
}

/// **THE MEASURED ACKNOWLEDGEMENT MUST INCLUDE THE WRITE IT ACKNOWLEDGES**
/// (roborev job 253, finding 1) — the property that keeps the calibration from
/// going inert.
///
/// `t_ack` is a CALIBRATION INPUT: `scale = max(1, t_ack /
/// QUIET_OBSERVATION_BASELINE)`. Both integration tests used to start their
/// acknowledgement timer AFTER the `writeln!`/`flush()` — the sibling by opening
/// the stage late, the five-write loop by taking its `Instant` late — so the timer
/// began after the operation whose round-trip it measures. A fast child, or a test
/// thread descheduled across the write, could then have its `OK` recorded before
/// timing started, collapsing the measurement to nearly zero: `scale` stays at
/// 1.000 and the deadline does not expand under contention, which is #3515's
/// ORIGINAL DEFECT reintroduced through a mis-placed timer.
///
/// It was masked in the one contended run ever observed (tasks.md round 13):
/// `t_boot` measured 68.5ms (scale 1.557) while `t_ack` measured 4.094ms (scale
/// 1.000), and `calibrate` takes the LARGEST scale. Under-measure both and the
/// mechanism is inert again, silently.
///
/// THIS IS A LOWER BOUND ON A MEASUREMENT, NOT A LATENCY BUDGET (the #2642
/// class): `thread::sleep` sleeps AT LEAST its argument, so overshoot — the only
/// direction a loaded host can move this — makes the assertion MORE true. There is
/// no threshold a busy box can breach.
#[test]
fn the_measured_acknowledgement_includes_the_write_itself() {
    // Stands in for `writeln!` + `flush()` to a child on a contended host. Any
    // value works: the assertion compares the measurement against THIS constant,
    // never against the calibration baseline, so it stays valid whatever that
    // baseline becomes.
    const WRITE_COST: Duration = Duration::from_millis(50);

    let deadline = TestDeadline::start(Duration::from_secs(30), Duration::from_secs(30));
    let (io, out, err) = ChildIo::synthetic();

    // The ordering both integration tests now use: open the stage, THEN mark, THEN
    // write.
    let stage = deadline.stage("b.write-ack");
    let mark = io.mark_from_the_start();
    thread::sleep(WRITE_COST);
    // The ack lands as part of the write, i.e. BEFORE the wait is ever entered —
    // exactly the case a timer started after the write cannot see. A wait that
    // returns instantly is what makes the mis-measurement invisible.
    out.record("OK");

    // A child is required because the FAILURE path tears one down (#3652); this
    // test takes the success path, which never touches it, and reaps it below.
    let mut child = stand_in_child();
    let t_ack = await_write_ack(&io, mark, "the stand-in write", &stage, &mut child);
    let _ = kill_and_reap(&mut child);
    assert!(
        t_ack >= WRITE_COST,
        "the acknowledgement measurement must span the write it acknowledges: got {t_ack:?} for \
         a write that took at least {WRITE_COST:?}. A measurement that excludes the write \
         collapses to ~0 whenever the ack is already in hand, `scale` stays at 1.000, and the \
         one deadline does not expand under contention — #3515's own original defect, \
         reintroduced through a mis-placed timer"
    );
    drop((out, err));
}

/// **A MISSING ACKNOWLEDGEMENT KILLS AND REAPS THE CHILD BEFORE IT FAILS** (#3652,
/// roborev job 265 finding 4).
///
/// An absent `OK` is the signature of a STALLED interactive child, and
/// `await_write_ack` used to panic with that child — the very process being
/// diagnosed — still running, along with the two reader threads blocked on its
/// pipes. Pre-existing on `main` (`assert!(ack.is_some(), ...)` there does the
/// same), which is why it was filed rather than treated as a regression.
///
/// The property is asserted on the CHILD'S STATE after the failure, not on the
/// message: a message can say anything. `try_wait` answers `Some(status)` only for
/// a process this harness has already reaped — a child left running answers `None`,
/// which is the pre-fix reading.
///
/// The panic below is EXPECTED OUTPUT OF A PASSING TEST and it prints, loudly. The
/// process-global panic hook is deliberately NOT touched: it is shared by every
/// test in this binary, a swap can interleave with a concurrent test's and a panic
/// between swap and restore skips the restore outright — the round-17 hazard
/// (roborev job 262) that silenced panic output for everything that ran afterwards.
#[test]
fn a_missing_acknowledgement_kills_and_reaps_the_child_before_failing() {
    let (io, out, err) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();
    // An already-lapsed deadline, so the wait fails without waiting: nothing here
    // depends on how long anything takes.
    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("b.write-ack");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    let mut child = stand_in_child();
    let pid = child.id();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        await_write_ack(&io, mark, "the stand-in write", &stage, &mut child)
    }));
    assert!(
        failed.is_err(),
        "no `OK` was ever recorded and the deadline had lapsed, so the wait must fail — this test          asserts what the FAILURE path does to the child"
    );

    match child.try_wait() {
        Ok(Some(_reaped)) => {}
        Ok(None) => panic!(
            "the interactive child (pid {pid}) was STILL RUNNING after the acknowledgement              failure: the panic left the stalled process it was diagnosing alive, plus its              reader threads, for the rest of this test binary (#3652, job 265 finding 4)"
        ),
        Err(error) => panic!("could not establish the child's state after the failure: {error}"),
    }
    drop((out, err));
}

/// **A POLL'S OBSERVATION WINDOW COVERS THE OPERATION IT TRIGGERS** (#3652, roborev
/// job 262 finding 2).
///
/// The poll's window is its transcript mark plus its artifact baseline, and both
/// are taken INSIDE the poll — so a caller that performed the triggering operation
/// first (`drop(stdin)`, a signal) had its window open AFTER the operation, and
/// every line the child emitted in that gap was outside it. A timeout could then
/// report `progress observed: NONE — 0 new output lines and 0 new durable
/// artifacts` against evidence this harness had already produced, which is the
/// wrong-cause class #3515 exists to remove.
///
/// The poll here runs against an already-lapsed deadline, so it declares its expiry
/// on the first iteration and nothing depends on timing: what is asserted is which
/// records the window CONTAINED.
#[test]
fn a_polls_window_covers_the_effects_of_the_operation_it_triggers() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (io, out, err) = ChildIo::synthetic();

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("triggered-window");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    let fail = match poll_with_progress_triggered(
        &io,
        dir.path(),
        &stage,
        // Stands in for `drop(stdin)`: the operation whose EARLY effects the poll
        // must be able to see. A real child emits exactly this kind of line while
        // shutting down, before the poll's first look.
        || out.record("flushing memtable before exit..."),
        |_slice, _artifacts| None::<()>,
    ) {
        Err(fail) => fail,
        Ok(((), _)) => panic!("the step never completes, so an expired poll must give up"),
    };

    let observed = fail.observed();
    assert!(
        observed.contains("1 new output line"),
        "the line the poll's OWN trigger produced must be inside the window the poll reports          from — a window opened after the operation excludes it and the failure then denies          evidence this harness produced: {observed}"
    );
    assert!(
        !observed.contains("progress observed while polling: NONE"),
        "progress WAS observed: reporting NONE here is the defect (#3652): {observed}"
    );
    assert!(
        fail.transcript().contains("flushing memtable before exit"),
        "and the transcript the verdict was taken from must print it: {}",
        fail.transcript()
    );
    drop((out, err));
}

/// **AN OPERATION INITIATED AFTER EXPIRY CANNOT SATISFY ITS WAIT** (roborev job
/// 253, finding 2) — and the round-9 ruling it must not break.
///
/// Two halves, in one test because the point is the boundary between them:
///
/// * The guard REFUSES to initiate the operation once the one deadline has passed,
///   naming what was not started. That is the fix: an `OK`, a handler-entry marker
///   or an exit produced by work ISSUED after expiry is fresh evidence
///   manufactured past the sole bound, and by the time such a line exists it is
///   indistinguishable from one that arrived in time.
/// * Had it been issued anyway, its evidence WOULD have satisfied the wait — and
///   that is CORRECT and stays (the round-9 ruling, `poll_with_progress` and
///   `wait_for`): the deadline bounds how long the test WAITS FOR evidence, never
///   whether it accepts evidence in hand, because failing a stage that observed
///   its signal is a false failure on a working product. So the check belongs at
///   the point of INITIATION, which is the only place the two cases are still
///   distinguishable.
#[test]
fn an_operation_initiated_after_expiry_cannot_satisfy_its_wait() {
    let deadline = TestDeadline::start(Duration::ZERO, Duration::ZERO);
    let stage = deadline.stage("b.write-ack");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-exhausted deadline"
    );

    let (io, out, err) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();

    // Half one: the write is REFUSED, and the refusal names it.
    let refused = stage
        .check_live("the INSERT (id=7) write to the child's stdin")
        .expect_err("an exhausted deadline must refuse to initiate new work");
    let described = refused.describe();
    for needle in [
        "the INSERT (id=7) write to the child's stdin",
        "NOT initiated",
        "b.write-ack",
    ] {
        assert!(
            described.contains(needle),
            "the refusal must name {needle:?} — what was not started, and where: {described}"
        );
    }

    // Half two: the evidence such a write would have produced satisfies the wait,
    // which is why the guard above is the fix and `wait_for` is deliberately NOT
    // changed.
    out.record("OK");
    let accepted = io.wait_for(mark, Stream::Stdout, |l| l.trim() == "OK", &stage);
    assert!(
        accepted.is_ok(),
        "evidence in hand must still be ACCEPTED as the deadline lapses (the round-9 ruling): \
         rejecting it would be a false failure on a working product. The fix for post-expiry \
         work is the initiation guard, not this wait"
    );
    drop((out, err));
}
