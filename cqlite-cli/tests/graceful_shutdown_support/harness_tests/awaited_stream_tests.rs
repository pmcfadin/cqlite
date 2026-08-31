//! **WHICH PIPE THE WAIT WAS WATCHING** — coverage for the per-stream terminal
//! verdict in `transcript.rs` (`TranscriptSnapshot::stream_status`, `ended`,
//! `WaitEnd::AwaitedStreamEnded`), issue #3515 round 18.
//!
//! Split out of `harness_tests.rs` under the campsite rule (#1135). The division
//! is by SUBJECT: this file is about a wait's verdict for the ONE stream it named,
//! the parent keeps the rest of the child harness's tests, and `transcript.rs`'s
//! own module covers the five WHOLE-store reader states.
//!
//! A child module can see its parent's private items, so these tests use the same
//! `ChildIo::synthetic` and `mark_from_the_start` helpers the parent's waits do —
//! the real recording path, not a mock of it.

use super::*;

/// **A WAIT WHOSE OWN READER HAS ENDED RETURNS AT ONCE, WITH THE PIPE'S RESULT —
/// IT DOES NOT SLEEP OUT THE DEADLINE AND THEN BLAME IT** (round 18).
///
/// `ended` used to consult only the WHOLE-store state, where a PARTIAL closure is
/// nonterminal whichever pipe went: with stderr's reader gone and stdout's still
/// attached, the awaited line could never arrive, and the wait nevertheless ran to
/// the end of its budget — up to the 360s/600s bases in `budgets.rs` — and then
/// reported a DEADLINE.
///
/// THE VERDICT AND THE CAUSE ARE BOTH WRONG THERE. The deadline is named for a
/// wait no deadline could have satisfied, and #3515's own subject is exactly that
/// shape one level up: a bare timeout reported as "no graceful shutdown handler".
///
/// A LIVE deadline is the whole point of this test: the wait must not need to run
/// out of time, and must not blame one, to report what its own pipe did. The
/// assertion is on the deadline's REMAINING BUDGET, not on a measured elapsed
/// time, so this is not a wall-clock threshold assert (#2642).
#[test]
fn a_wait_whose_awaited_reader_reached_eof_returns_the_pipes_result_not_a_deadline() {
    // `_out` is bound, not dropped: stdout's reader stays attached for the whole
    // test, which is the state that used to make this wait run its budget out.
    let (io, _out, err) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();
    // The AWAITED stream's reader ends at EOF; the other one stays attached, so
    // the whole-store state is a PARTIAL closure and establishes nothing about the
    // pipe this wait is watching.
    drop(err);

    let deadline = TestDeadline::start(Duration::from_secs(30), Duration::from_secs(30));
    let stage = deadline.stage("awaited-stream-eof");

    match io.wait_for(
        mark,
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        &stage,
    ) {
        Err(
            end @ WaitEnd::AwaitedStreamEnded {
                want: Stream::Stderr,
                ended: StreamEnded::Eof,
                ..
            },
        ) => {
            let described = end.describe();
            assert!(
                described.contains("stderr") && described.contains("ENDED AT EOF"),
                "the failure must name the ended pipe and what ended it: {described}"
            );
            assert!(
                !described.contains("deadline passed"),
                "no deadline bound this wait, and naming one is the wrong-cause defect this test \
                 exists for: {described}"
            );
            assert!(
                described.contains("still attached"),
                "output was still possible on the SURVIVING pipe, which is where a reader of \
                 this failure should look next: {described}"
            );
        }
        Err(WaitEnd::DeadlineReached { .. }) => panic!(
            "the reader for the AWAITED stream had ended, so the awaited line could never \
             arrive — and the wait slept out its whole budget and then reported a DEADLINE as \
             the cause of a wait no deadline could have satisfied (round 18)"
        ),
        Err(other) => panic!(
            "one pipe was still attached, so no verdict may claim output is over everywhere: {}",
            other.describe()
        ),
        Ok((line, _)) => panic!("nothing matching was ever recorded, yet {line:?} matched"),
    }
    assert!(
        !stage.remaining().is_zero(),
        "the wait must return as soon as its own pipe is finished, with budget left — running \
         the deadline out is what made the cause wrong"
    );
}

/// **AND WHEN THAT READER ENDED IN AN I/O ERROR, THE FAILURE SAYS SO** rather than
/// naming EOF or a deadline (round 18, the `want` half of roborev job 255 finding
/// 2).
///
/// EOF and a failed read END a reader identically, so the cause is chosen from the
/// reader's recorded terminal result for THAT stream. What is at stake is which
/// cause is named: the awaited line is absent either way, and the statement "this
/// harness could not read the pipe" is about the pipe, not about the child.
#[test]
fn a_wait_whose_awaited_reader_failed_reports_the_read_failure_not_a_deadline() {
    // Bound, not dropped: stdout's reader stays attached for the whole test.
    let (io, _out, err) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();
    // The same path a real reader thread takes when `BufRead::lines` yields `Err`,
    // on the AWAITED stream, while the other reader stays attached.
    err.read_failed(std::io::Error::other("simulated pipe failure"));
    drop(err);

    let deadline = TestDeadline::start(Duration::from_secs(30), Duration::from_secs(30));
    let stage = deadline.stage("awaited-stream-read-failure");

    match io.wait_for(
        mark,
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        &stage,
    ) {
        Err(
            end @ WaitEnd::AwaitedStreamEnded {
                want: Stream::Stderr,
                ended: StreamEnded::ReadFailed { .. },
                ..
            },
        ) => {
            let described = end.describe();
            assert!(
                described.contains("stderr reader: simulated pipe failure"),
                "the failure must carry that stream's own terminal result: {described}"
            );
            assert!(
                described.contains("I/O ERROR") && !described.contains("ENDED AT EOF"),
                "a failed read must not be described as EOF: {described}"
            );
            assert!(
                !described.contains("deadline passed"),
                "no deadline bound this wait: {described}"
            );
        }
        Err(WaitEnd::DeadlineReached { .. }) => panic!(
            "the awaited stream's reader had FAILED, so its line could never arrive — and the \
             wait ran out its budget and blamed the deadline (round 18)"
        ),
        Err(other) => panic!(
            "one pipe was still attached, and the awaited one ended in an I/O error, so neither \
             an EOF nor an everywhere-closed verdict may be built: {}",
            other.describe()
        ),
        Ok((line, _)) => panic!("nothing matching was ever recorded, yet {line:?} matched"),
    }
    assert!(
        !stage.remaining().is_zero(),
        "the wait must return as soon as its own pipe is finished, with budget left"
    );
}

/// **THE OTHER PIPE ENDING CHANGES NOTHING: A WAIT WHOSE OWN READER IS STILL
/// ATTACHED IS STILL BOUND BY THE DEADLINE** (round 18).
///
/// The narrowing must be to the AWAITED stream and not to "any stream", or a wait
/// would be abandoned while the line it is watching for could still arrive — a
/// false failure, which is strictly worse than the wrong-cause diagnostic being
/// fixed. Here stdout's reader has ended and stderr's — the awaited one — has not.
#[test]
fn a_wait_whose_awaited_reader_is_still_attached_is_still_bound_by_the_deadline() {
    // `_err` is bound, not dropped: the AWAITED stream's reader stays attached.
    let (io, out, _err) = ChildIo::synthetic();
    let mark = io.mark_from_the_start();
    // The OTHER stream's reader ends; the awaited one stays attached.
    drop(out);

    let deadline = TestDeadline::start(Duration::from_millis(1), Duration::from_millis(1));
    thread::sleep(Duration::from_millis(25));
    let stage = deadline.stage("other-stream-ended");
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
        Err(end @ WaitEnd::DeadlineReached { .. }) => {
            let described = end.describe();
            assert!(
                described.contains("had already ENDED") && described.contains("still attached"),
                "the deadline was the cause, and the message must still say which pipe had gone: \
                 {described}"
            );
        }
        Err(other) => panic!(
            "the AWAITED stream's reader was still attached, so its line could still have \
             arrived and the deadline is the only thing that bound this wait: {}",
            other.describe()
        ),
        Ok((line, _)) => panic!("nothing matching was ever recorded, yet {line:?} matched"),
    }
}

/// **A WAIT ON A STREAM NO READER WAS EVER ATTACHED TO IS NAMED AS A HARNESS
/// DEFECT**, not waited out (round 18).
///
/// `ChildIo::attach` attaches both pipes, so this is reachable only from a harness
/// helper that attached fewer — and then no record can EVER appear on that stream.
/// A wait there is unsatisfiable from the moment it starts, which is the same
/// impossibility as an ended reader with one extra fact worth printing: the pipe
/// was never read at all, so the failure is about this harness and not the child.
#[test]
fn a_wait_on_a_stream_that_was_never_attached_is_reported_as_a_harness_defect() {
    let (io, handles) = ChildIo::with_readers(&[Stream::Stdout]);
    let mark = io.mark();
    // Kept alive, so the whole-store state is "every attached reader is open" —
    // the state that used to make this wait sleep for its whole budget.
    let _out = handles.into_iter().next().expect("the stdout handle");

    let deadline = TestDeadline::start(Duration::from_secs(30), Duration::from_secs(30));
    let stage = deadline.stage("stream-never-attached");

    match io.wait_for(
        mark,
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        &stage,
    ) {
        Err(
            end @ WaitEnd::AwaitedStreamEnded {
                want: Stream::Stderr,
                ended: StreamEnded::NeverAttached,
                ..
            },
        ) => {
            let described = end.describe();
            assert!(
                described.contains("NO READER WAS EVER ATTACHED")
                    && described.contains("defect in this test harness"),
                "the failure must name what it is: {described}"
            );
        }
        Err(WaitEnd::DeadlineReached { .. }) => panic!(
            "nothing could ever be recorded on the awaited stream, and the wait spent its whole \
             budget before blaming the deadline (round 18)"
        ),
        Err(other) => panic!(
            "the attached reader was still open, so no everywhere-closed verdict may be built: {}",
            other.describe()
        ),
        Ok((line, _)) => panic!("nothing was ever recorded at all, yet {line:?} matched"),
    }
    assert!(
        !stage.remaining().is_zero(),
        "an unsatisfiable wait must be answered immediately, with budget left"
    );
}
