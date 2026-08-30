//! Unit coverage for the CHILD HARNESS in `mod.rs` (issues #1693, #3515).
//!
//! Split out of `mod.rs` under the campsite rule (#1135): round 12's
//! class-by-census fixes pushed that file toward the 1500-line test threshold.
//! The division of responsibility is by SUBJECT, not by size — `budgets.rs` pins
//! the ONE deadline's invariants, `mod.rs` holds the instrument, and this file
//! holds the instrument's own tests.
//!
//! A child module can see its parent's private items, so these tests exercise
//! `ChildIo::final_drain`, `collect_both_streams`, `poll_with_progress_sampled`
//! and the `CollectEnd`/`WaitEnd` internals directly — the harness's public
//! surface is deliberately small and these are the seams the defects lived in.

use super::*;
use std::cell::Cell;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// The shape [`ChildIo::synthetic_with_transcript`] hands back: the harness, the
/// stand-in reader's `Sender`, and the shared transcript that reader records
/// into. Named because the tuple is genuinely three coupled handles to ONE
/// instrument and clippy's `type_complexity` is right that the bare tuple is
/// unreadable — an `#[allow]` would just hide the same unreadable signature.
type SyntheticChildIo = (ChildIo, Sender<(Stream, String)>, Arc<Mutex<Vec<String>>>);

impl ChildIo {
    /// A `ChildIo` with no child behind it: the returned `Sender` stands in for a
    /// reader thread, so a unit test can make progress arrive on demand.
    fn synthetic() -> (Self, Sender<(Stream, String)>) {
        let (io, tx, _transcript) = Self::synthetic_with_transcript();
        (io, tx)
    }

    /// A mark of ZERO — the whole transcript. Only sound for a `ChildIo` that has
    /// recorded nothing yet, which is why it is spelled out at each use rather
    /// than hidden in the constructor.
    fn mark_from_the_start(&self) -> Mark {
        assert_eq!(
            self.transcript_len(),
            0,
            "this helper is only sound before anything has been recorded"
        );
        self.mark()
    }

    /// As [`ChildIo::synthetic`], plus a handle to the shared transcript, so a
    /// test can reproduce a reader thread that has RECORDED a line but not yet
    /// PUBLISHED it.
    fn synthetic_with_transcript() -> SyntheticChildIo {
        let (tx, rx) = mpsc::channel();
        let transcript: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                rx,
                transcript: Arc::clone(&transcript),
            },
            tx,
            transcript,
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
        let (io, lines, io_transcript) = ChildIo::synthetic_with_transcript();
        let deadline = TestDeadline::start(Duration::from_millis(300), Duration::from_millis(300));
        let stage = deadline.stage("synthetic");
        let outcome = poll_with_progress(&io, &data_dir, &stage, |slice, _artifacts| {
            // Progress on every single slice, in the order a REAL reader thread
            // produces it: recorded into the transcript first, published to the
            // channel second. The poll counts new lines from the transcript,
            // because that is the store its failure message renders (job 243,
            // finding 1).
            if let Ok(mut t) = io_transcript.lock() {
                t.push("[stderr] still working".to_string());
            }
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

    // TIMEOUT AND DISCONNECTED ARE NEVER COLLAPSED (roborev job 243, finding 3 —
    // the class, applied to this test's own plumbing). A bare `.expect` here would
    // report "the poll did not terminate" for a worker that PANICKED, which is the
    // opposite diagnosis: the worker is gone, so nothing is still running. On a
    // disconnect the join re-raises the worker's panic, which is the real cause.
    let observed = match done_rx.recv_timeout(Duration::from_secs(30)) {
        Ok(observed) => observed,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!(
            "the progress-observing poll did not terminate within 30s despite a 300ms deadline: \
             observed progress is extending the bound, which is exactly what the round-8 descope \
             removed"
        ),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            // Re-raise the worker's own panic rather than reporting a non-existent
            // hang; `join` on a panicked thread resumes that panic.
            let _ = worker.join();
            panic!(
                "the poll worker ended without reporting: its sender was dropped, so the poll \
                 PANICKED rather than failing to terminate (the join above should have re-raised \
                 it)"
            );
        }
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
// each declared a timeout without a last look at what was already queued. Under
// contention the awaited marker, the process exit or a reader's buffer can
// arrive well before the deadline and be consumed only after it, because this
// thread is descheduled in between. The result was a false timeout on a working
// product — and, worst of all, a message CONTRADICTED BY ITS OWN TRANSCRIPT.
//
// The three tests below each queue the evidence FIRST, then let the deadline
// lapse, then let the harness look: each must SUCCEED.
//
// ON THE SLEEPS: a `sleep` can only OVERSHOOT, and overshoot makes the
// precondition ("the deadline has already lapsed") MORE true, never less. No
// test here asserts that anything completed FAST, so this is the opposite of the
// #2642 wall-clock flake class.

/// `wait_for`: a line that arrived before the deadline must still match.
#[test]
fn a_line_queued_before_the_deadline_is_matched_after_it_lapses() {
    let (io, lines) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();
    lines
        .send((
            Stream::Stderr,
            format!("cqlite: {MARKER_HANDLER_ENTERED} before exit..."),
        ))
        .expect("queue the marker before the deadline lapses");

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("queued-before-expiry");
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
            "the marker had ALREADY ARRIVED when the deadline lapsed, and the wait reported \
             {end:?} instead of matching it. That is a false timeout on a working product; on \
             the real path it is also a self-contradicting diagnostic, because the transcript \
             the failure prints contains the very marker the message says was never observed. \
             (This synthetic `ChildIo` has no reader thread, so its transcript is empty by \
             construction and is deliberately not quoted here.)"
        ),
    }
}

/// `poll_with_progress`: a step whose evidence landed before the deadline must
/// still be observed, not reported as a timeout.
#[test]
fn a_step_completed_before_the_deadline_is_observed_after_it_lapses() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    // The durable artifact APPEARS before the deadline...
    std::fs::write(data_dir.join("nb-1-big-Data.db"), b"x").expect("plant an artifact");

    let (io, _lines) = ChildIo::synthetic();
    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    // ...and only then does the deadline lapse, before the poll ever looks.
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("queued-before-expiry");
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
}

/// `collect_both_streams`: buffers delivered before the deadline must still be
/// collected, not reported as a partial collection.
#[test]
fn read_side_buffers_queued_before_the_deadline_are_collected_after_it_lapses() {
    let (tx, rx) = mpsc::channel();
    tx.send((Stream::Stdout, b"[]".to_vec()))
        .expect("queue stdout");
    tx.send((Stream::Stderr, Vec::new())).expect("queue stderr");

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("queued-before-expiry");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match collect_both_streams(&rx, &stage) {
        CollectEnd::Both(out, err) => {
            assert_eq!(
                out, b"[]",
                "the queued stdout buffer must be the one returned"
            );
            assert!(err.is_empty(), "the queued stderr buffer must be returned");
        }
        CollectEnd::DeadlineReached { collected } => panic!(
            "both reader buffers were delivered BEFORE the deadline lapsed, and the collection \
             reported a timeout with {collected}/2 collected — a false failure against a child \
             that had already exited successfully"
        ),
        CollectEnd::Disconnected { collected } => panic!(
            "unexpected disconnect with {collected}/2 collected: the senders are still alive"
        ),
    }
}

// ---------------------------------------------------------------------------
// THE EXPIRY DECISION READS THE STORE THE FAILURE PRINTS FROM
// (roborev job 236, finding 1)
// ---------------------------------------------------------------------------

/// `spawn_reader` RECORDS into the transcript before it PUBLISHES to the
/// channel, so a reader preempted between those two operations leaves the
/// channel behind the transcript. Job 233's expiry drain consulted only the
/// channel, so in that window a timeout could be declared while the transcript
/// the message prints already held the awaited marker — the self-contradicting
/// diagnostic this change exists to prevent.
///
/// THE INTERLEAVING IS FORCED, NOT RACED. The plant does not sleep to arrange
/// the ordering (which would make the precondition timing-dependent in the
/// direction that produces flakes): the predicate itself is the synchronisation
/// point. A decoy line — the empty line the product really does print
/// immediately before the handler marker — is queued on the channel, and when
/// `wait_for` receives it the predicate records BOTH lines into the transcript
/// and returns false. So at expiry the transcript provably holds the marker and
/// the channel provably does not, on every host and at every load.
///
/// The channel is left CONNECTED (this test still owns the `Sender`), so a
/// failure here is `DeadlineReached`, not `PipesClosed`.
#[test]
fn a_line_recorded_but_not_yet_published_is_matched_at_expiry() {
    let (io, lines, transcript) = ChildIo::synthetic_with_transcript();
    let mark = io.mark_from_the_start();
    let marker = format!("cqlite: {MARKER_HANDLER_ENTERED} — flushing memtable before exit...");

    // The decoy the real reader publishes just before it is preempted.
    lines
        .send((Stream::Stderr, String::new()))
        .expect("queue the decoy line");

    let recorded = Arc::clone(&transcript);
    let marker_for_pred = marker.clone();
    // The reader records ONCE, as a real one does; the predicate is also applied
    // to the transcript window at expiry, and re-recording there would say the
    // child printed the marker twice.
    let recorded_once = std::sync::atomic::AtomicBool::new(false);
    let deadline = TestDeadline::start(Duration::from_millis(300), Duration::from_millis(300));
    let stage = deadline.stage("recorded-not-published");

    let matched = io.wait_for(
        mark,
        Stream::Stderr,
        |line| {
            if line.is_empty() && !recorded_once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                // The reader's transcript push for both lines has happened...
                let mut t = recorded.lock().expect("transcript lock");
                t.push("[stderr] ".to_string());
                t.push(format!("[stderr] {marker_for_pred}"));
                // ...and the marker's `tx.send` has NOT. The channel is now
                // provably behind the transcript for the rest of this wait.
            }
            line.contains(MARKER_HANDLER_ENTERED)
        },
        &stage,
    );

    match matched {
        Ok((line, _)) => assert_eq!(
            line, marker,
            "the matched line must be the child's own text, with the transcript's `[stderr] ` tag \
             stripped — `await_write_ack` matches on the WHOLE trimmed line, so a tagged line \
             would silently never match there"
        ),
        Err(end) => panic!(
            "the awaited marker was IN THE TRANSCRIPT when the deadline lapsed and the wait \
             reported {end:?} anyway. The decision read a different store from the one the \
             failure message renders, so this failure would print the very marker it claims was \
             never observed:\n{}\ntranscript the message would print:\n{}",
            end.describe(),
            end.transcript()
        ),
    }
    drop(lines);
}

/// The window is what keeps the transcript scan from turning a CUMULATIVE store
/// into a false pass: a line recorded BEFORE a wait began belongs to an earlier
/// stage, which already consumed or discarded it. `await_write_ack` awaits five
/// separate `OK`s in one test, so without the window the first would satisfy all
/// five and a wedged session would read as green.
#[test]
fn a_line_recorded_before_the_wait_began_does_not_satisfy_it() {
    let (io, lines, transcript) = ChildIo::synthetic_with_transcript();
    transcript
        .lock()
        .expect("transcript lock")
        .push("[stdout] OK".to_string());
    // The mark is taken AFTER the earlier stage's line, exactly as a call site
    // takes it after the previous ack returned and before the next statement is
    // written. Moving the mark EARLIER (job 243, finding 1) closes a race at the
    // start of a wait; it must not widen the window backwards over a consumed line.
    let mark = io.mark();

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("stale-transcript-line");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match io.wait_for(mark, Stream::Stdout, |l| l.trim() == "OK", &stage) {
        Ok((line, _)) => panic!(
            "a transcript line recorded BEFORE this wait began satisfied it ({line:?}): the \
             cumulative transcript is being read as if every line were new, so one earlier \
             acknowledgement would satisfy every later wait for one"
        ),
        Err(WaitEnd::DeadlineReached { snapshot }) => assert_eq!(
            snapshot.examined(),
            0,
            "the wait must report the size of the window it examined, and the stale line is \
             outside it"
        ),
        Err(other) => panic!("expected a deadline, got {other:?}"),
    }
    drop(lines);
}

/// An expiry racing pipe closure must name the cause the measurement supports
/// (roborev job 236, finding 3). Both readers have ended, so no further line can
/// arrive: reporting `DeadlineReached` would tell the reader the pipes were
/// "still open (more output was still possible)" about a child whose output is
/// over — a message contradicted by the same drain that produced it.
#[test]
fn an_expiry_racing_pipe_closure_reports_closed_pipes() {
    let (io, lines) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();
    // A queued non-matching line, so the drain must CHECK the queue before it can
    // observe the disconnect: `try_recv` reports `Disconnected` only once the
    // queue is empty.
    lines
        .send((Stream::Stderr, "some other output".to_string()))
        .expect("queue a non-matching line");
    drop(lines);

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
            "both readers had ended when the deadline lapsed, and the wait reported a deadline \
             with the pipes \"still open\" — the message names a cause its own final drain \
             contradicts (AC2)"
        ),
        Ok((line, _)) => panic!("nothing matching was ever queued, yet {line:?} matched"),
    }
}

// ---------------------------------------------------------------------------
// CLASS C: `Empty` AND `Disconnected` ARE NEVER COLLAPSED — AT EVERY SITE
// (roborev job 243, finding 3: job 236 finding 3's defect, at two more sites)
// ---------------------------------------------------------------------------

/// `collect_both_streams`: reader threads that END without delivering both
/// buffers must be reported as `Disconnected`, not as a deadline.
///
/// The expiry drain used `let Ok(..) = rx.try_recv() else { break }`, which stops
/// identically on an empty queue and on a dead sender — so a harness defect the
/// `Disconnected` variant exists to name was reported as a timeout against the
/// deadline instead. The variant carries `collected`, so the message can still
/// say how far the collection got.
#[test]
fn read_side_readers_that_end_without_delivering_report_a_disconnect() {
    let (tx, rx) = mpsc::channel();
    // ONE buffer delivered, then both senders gone: `try_recv` must therefore
    // yield the queued buffer FIRST and only then report the disconnect, which is
    // what "return the right variant AFTER all queued items have been checked"
    // means here.
    tx.send((Stream::Stdout, b"[]".to_vec()))
        .expect("queue stdout");
    drop(tx);

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("expiry-races-reader-death");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    match collect_both_streams(&rx, &stage) {
        CollectEnd::Disconnected { collected } => assert_eq!(
            collected, 1,
            "the drain must consume the queued buffer before reporting the disconnect, so the \
             count in the message is how far the collection actually got"
        ),
        CollectEnd::DeadlineReached { collected } => panic!(
            "both reader threads had ENDED without delivering ({collected}/2 collected) and the \
             collection reported a deadline — blaming the test's bound for a harness defect the \
             `Disconnected` variant already exists to name"
        ),
        CollectEnd::Both(..) => panic!("only one buffer was ever sent, yet both were collected"),
    }
}

/// `poll_with_progress`: a poll that gives up with BOTH pipes at EOF must say so.
///
/// `drain_new` was `while try_recv().is_ok()`, so it stopped identically on an
/// empty queue and on a dead sender and the poll could report "0 new output
/// lines" — implying more output was still possible — about a child whose stdout
/// and stderr had both closed.
#[test]
fn a_poll_that_gives_up_with_closed_pipes_reports_closed_pipes() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (io, lines) = ChildIo::synthetic();
    // A queued line, so the drain must CHECK the queue before it can observe the
    // disconnect — and the reported count must still include that line.
    lines
        .send((Stream::Stderr, "some output".to_string()))
        .expect("queue a line");
    drop(lines);

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("poll-with-closed-pipes");
    assert!(
        stage.remaining().is_zero(),
        "the precondition of this test is an already-lapsed deadline"
    );

    let outcome = poll_with_progress(&io, dir.path(), &stage, |_slice, _artifacts| None::<()>);
    let observed = match outcome {
        Ok(_) => unreachable!("the step never completes"),
        Err(fail) => fail.observed(),
    };
    assert!(
        observed.contains("BOTH reached EOF"),
        "the poll gave up with both reader threads gone and did not report it, so the message \
         implies more output was still possible: {observed}"
    );
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
    let (io, _lines) = ChildIo::synthetic();

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
    let (io, _lines) = ChildIo::synthetic();

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

/// A line RECORDED BEFORE THE WAIT WAS STARTED — but after the mark — must be
/// matched at expiry.
///
/// This is the race at the START of a wait. The call site sends a statement (or a
/// signal) and only then calls `wait_for`; a reader thread can record the response
/// into the transcript in that gap and be descheduled before publishing it. With
/// the mark taken INSIDE `wait_for`, that line was BEFORE the mark (outside the
/// window) and NOT YET in the channel — excluded from both halves of the expiry
/// check, so the harness declared a timeout against evidence it already held and
/// printed the marker in the very message denying it.
///
/// The interleaving is FORCED, not raced: the line is written straight into the
/// transcript and never published, so the channel provably lacks it on every host.
#[test]
fn a_line_recorded_before_the_wait_started_is_matched_at_expiry() {
    let (io, lines, transcript) = ChildIo::synthetic_with_transcript();
    // Taken BEFORE the operation, as every call site now does.
    let mark = io.mark_from_the_start();

    let marker = format!("cqlite: {MARKER_HANDLER_ENTERED} — flushing memtable before exit...");
    // ...the "operation" happens here, and its response is RECORDED but never
    // PUBLISHED — the reader was descheduled between the two.
    transcript
        .lock()
        .expect("transcript lock")
        .push(format!("[stderr] {marker}"));

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
            "the matched line must be the child's own text, with the `[stderr] ` tag stripped"
        ),
        Err(end) => panic!(
            "the awaited marker was recorded BEFORE the wait was started and the wait reported \
             {end:?} anyway: the window opens after the operation, so a response recorded in the \
             gap between the operation and the wait is outside the window AND outside the \
             channel.\n{}\ntranscript the message would print:\n{}",
            end.describe(),
            end.transcript()
        ),
    }
    drop(lines);
}

/// A line appended AFTER the expiry decision must not appear in the failure.
///
/// This is the race at the END of a wait. Round 11 decided from the transcript and
/// then re-read it to render, so a reader recording between those two reads put
/// the awaited marker into a message that had just called it absent — the
/// self-contradicting diagnostic, one lock acquisition later. One snapshot serves
/// both, so the message is literally the bytes the verdict was taken from.
#[test]
fn a_line_appended_after_the_decision_cannot_contradict_the_failure() {
    let (io, lines, transcript) = ChildIo::synthetic_with_transcript();
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
        .expect_err("nothing was ever recorded or queued, so the wait must give up");

    // The reader thread lands the awaited marker AFTER the verdict was taken —
    // which on the real path is the window between the decision and the panic.
    transcript.lock().expect("transcript lock").push(format!(
        "[stderr] cqlite: {MARKER_HANDLER_ENTERED} — flushing..."
    ));

    let rendered = end.transcript();
    assert!(
        !rendered.contains(MARKER_HANDLER_ENTERED),
        "the failure renders a line appended AFTER its verdict was taken, so the message prints \
         the very marker it says was never observed. The decision and the render must be ONE \
         snapshot, not two reads of one store:\n{rendered}"
    );
    let described = end.describe();
    assert!(
        described.contains("0 line(s)"),
        "the count in the message must come from the same snapshot it renders: {described}"
    );
    drop(lines);
}
