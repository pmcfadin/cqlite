//! Issue #1693 (AG4) — graceful shutdown integration test.
//! Issue #3515 — the *oracle* used to observe it.
//!
//! Drives the `cqlite` binary as a REAL child process in interactive
//! `--writable` mode, performs a write over stdin, sends it `SIGINT`, and
//! verifies the process:
//!   1. exits cleanly (success), and
//!   2. flushed the memtable to a durable SSTable before exiting — the written
//!      row is present when the write directory is reopened read-only.
//!
//! # The oracle (issue #3515)
//!
//! The subject property above is unchanged. What changed is *how it is
//! observed*. A single bare `wait_timeout(60s)` after `SIGINT` cannot tell a
//! broken shutdown handler from a child that was never scheduled, yet its expiry
//! message named an absent shutdown handler as the cause — a cause the
//! measurement cannot establish — and it expired on a contended gate host while
//! the handler worked (standalone: 0.34s; under six concurrent gates: >60s).
//!
//! The wait is therefore **staged**, and each stage's failure reports only what
//! that stage measured:
//!
//! | stage | awaited signal | what its expiry establishes |
//! |-------|----------------|-----------------------------|
//! | a. session up      | readiness banner (stderr)         | the banner was not observed in time |
//! | b. write ack       | `OK` (stdout), timed -> `t_ack`    | no write was acknowledged in time |
//! | c. handler entered | Ctrl-C handler-entry marker (stderr) | signal undelivered / handler not entered / marker text drifted |
//! | d. clean exit      | process exit, progress-observed    | the shutdown flush did not complete in time |
//!
//! Observing (c) proves three things at once — the signal was delivered, a
//! shutdown handler exists and was entered, and the child was scheduled — which
//! is exactly the conjunction the old message guessed at. So (d) may never claim
//! anything about the *existence* of a handler.
//!
//! # ONE deadline (round-8 descope, design.md D6a)
//!
//! The stages above are **attribution**, not budgets. The whole test is bounded
//! by a SINGLE deadline, calibrated once from the largest scale of its in-band
//! measurements (`t_boot`, `t_ack`) as `clamp(base x scale, base, cap)` with
//! `scale = max(1, observed / quiet_baseline)`. Any single stage may consume the
//! whole deadline; and observed progress is reported as evidence but never
//! extends anything, so no wait is granted or started past the declared bound.
//!
//! **WHAT THE FLOOR CLAIM IS, QUALIFIED (design.md D6c).** A stage running IN
//! ISOLATION — against a deadline earlier stages have not consumed — cannot fire
//! sooner than the 60s bound it replaced, because the base is at least 60s and no
//! stage has an allowance of its own; and the base equals an `OLD_BOUND` for every
//! wait sharing the deadline, so the test as a whole is not tighter in aggregate.
//! What does NOT hold, and cannot: a fresh 60s for a later wait once earlier
//! stages have consumed the deadline. The pre-#3515 code gave every wait an
//! INDEPENDENT 60s; one absolute deadline cannot reproduce that, and what it buys
//! instead is a bounded TOTAL, which the old design had none of. This module doc
//! carried the un-qualified version for two rounds after D6c corrected it
//! (roborev job 255, finding 3) — the property that does not hold is pinned by
//! `an_exhausted_deadline_leaves_a_later_stage_nothing` in `budgets.rs`.
//!
//! That bound is on WAITING FOR EVIDENCE, not on accepting evidence already in
//! hand: a stage that observes its signal as the deadline lapses still passes,
//! deliberately, because failing it would be a false failure on a working product.
//! `poll_with_progress` in the support module owns that decision and bounds how
//! late an accepted success can be.
//!
//! Unix-only: it sends a real `SIGINT` via `libc::kill`.

#![cfg(all(feature = "write-support", unix))]

mod graceful_shutdown_support;

use graceful_shutdown_support::*;
use std::io::Write;
use std::process::ExitStatus;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use wait_timeout::ChildExt;

/// AC (issue #1693): an interactive `--writable` session that receives SIGINT
/// after a write exits cleanly AND has flushed the row to a durable SSTable.
///
/// Oracle (issue #3515): five attribution stages under one deadline, each
/// reporting only what it measures.
#[test]
fn sigint_in_writable_session_flushes_before_exit() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");
    let data_dir = wd.join("data");
    // THE ONE BOUND. Live from here, so every stage including the first is
    // charged; still uncalibrated, because no measurement exists yet.
    let deadline = TestDeadline::start(T1_DEADLINE_BASE, T1_DEADLINE_CAP);

    // Stage (a): session up.
    let (mut child, io, t_boot) = start_writable_session(&wd, &schema, &[], &deadline);
    // First in-band measurement: fold it into the one deadline. Calibration is
    // monotone, so this can only ever move the deadline later.
    deadline.calibrate("t_boot", t_boot);

    // Keep the stdin handle alive for the whole test so the child exits via
    // SIGINT, NOT via stdin EOF (an EOF would also flush and mask the bug).
    let mut stdin = child.stdin.take().expect("child stdin");

    // Stage (b): write ack, timed -> `t_ack`.
    //
    // THE STAGE IS OPENED BEFORE THE WRITE, AND THAT ORDERING IS LOAD-BEARING
    // (roborev job 253, finding 1). `t_ack` is the stage's own spend, and it is a
    // CALIBRATION INPUT: `scale = max(1, t_ack / QUIET_OBSERVATION_BASELINE)`.
    // Opening the stage after the `writeln!`/`flush()` started the timer AFTER the
    // operation whose round-trip it measures, so a fast child — or a test thread
    // descheduled across the write — could have its `OK` recorded before timing
    // began, collapsing the measurement to nearly zero. `scale` then stays at
    // 1.000 and the deadline does not expand under contention: that is #3515's
    // ORIGINAL DEFECT reintroduced through a mis-placed timer.
    //
    // It was MASKED in the one contended run ever observed (tasks.md round 13):
    // `t_boot` measured 68.5ms and carried scale 1.557 while `t_ack` measured
    // 4.094ms and contributed 1.000. `calibrate` takes the LARGEST scale, so a
    // `t_boot` that happened to be slow hid it — if both are under-measured the
    // mechanism is inert again. Pinned by
    // `the_measured_acknowledgement_includes_the_write_itself`.
    let stage = deadline.stage("b.write-ack");
    // THE TRANSCRIPT MARK IS ALSO TAKEN BEFORE THE WRITE (roborev job 243,
    // finding 1). The ack can be RECORDED by a reader thread and left unpublished;
    // a mark taken after this `writeln!` would start the expiry check's window
    // after the line was already recorded, excluding it from the window.
    let mark = io.mark();
    // NOTHING NEW MAY BE INITIATED ONCE THE ONE DEADLINE HAS PASSED (roborev job
    // 253, finding 2). A write issued after expiry can still produce an `OK` that
    // the wait's final look accepts — evidence manufactured after the sole bound,
    // which is a different thing from accepting evidence already in hand (the
    // round-9 ruling, deliberately preserved).
    require_live_or_kill(
        &stage,
        &io,
        &mut child,
        "the INSERT (id=7) write to the child's stdin",
    );
    writeln!(
        stdin,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (7, 'Grace', 30, true);"
    )
    .expect("write INSERT to child stdin");
    stdin.flush().expect("flush child stdin");

    // The child is passed MUTABLY so a missing ack tears it down before failing
    // (#3652, roborev job 265 finding 4): a stalled interactive child is the very
    // thing an absent ack diagnoses, and it used to survive the panic.
    let t_ack = await_write_ack(&io, mark, "the INSERT (id=7)", &stage, &mut child);
    stage.finish();
    deadline.calibrate("t_ack", t_ack);

    // Stage (c): handler ENTERED. Observing this marker establishes, together,
    // that the signal was delivered, that a shutdown handler exists and was
    // entered, and that the child was scheduled — so stage (d) below may never
    // claim anything about a handler's existence.
    //
    // The stage is opened BEFORE the signal, for the same two reasons as stage (b):
    // the `kill` is the operation the stage measures, and the deadline must be
    // checked before it is DELIVERED (job 253, findings 1 and 2). A SIGINT
    // delivered after expiry can still make the child print the handler-entry
    // marker, which the wait's final look would then accept.
    let stage = deadline.stage("c.handler-entry");
    // The mark for stage (c) is taken BEFORE the signal — the operation that
    // produces the awaited handler-entry line (job 243, finding 1).
    let handler_mark = io.mark();
    let pid = child.id() as libc::pid_t;
    require_live_or_kill(
        &stage,
        &io,
        &mut child,
        "the SIGINT delivery to the interactive child",
    );
    let rc = unsafe { libc::kill(pid, libc::SIGINT) };
    assert_eq!(rc, 0, "failed to deliver SIGINT to child pid {pid}");
    let entered = io.wait_for(
        handler_mark,
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        &stage,
    );
    let t_handler = match entered {
        Ok((_, took)) => took,
        Err(end) => {
            let _ = child.kill();
            panic!(
            "stage (c) handler-entry: the shutdown handler's entry marker was not observed on the \
             child's stderr after SIGINT was delivered to pid {pid}.\n\
             awaited substring on stderr: {MARKER_HANDLER_ENTERED:?}\n\
             {}\n\
             {}\n\
             CANDIDATE CAUSES (this measurement does NOT select between them):\n\
             \x20 1. the signal was not delivered to / not received by the child;\n\
             \x20 2. a shutdown handler was not entered (absent, or the interrupt lost a race);\n\
             \x20 3. the product's marker text drifted, so this test awaited a string the child \
             no longer prints — compare the awaited substring against the transcript below.\n\
             child transcript (the snapshot the verdict was taken from):\n{}\n{}",
            stage.describe(),
            end.describe(),
            end.transcript(),
            stage.report()
        );
        }
    };
    stage.finish();

    // Stage (d): clean exit, with progress OBSERVED. A new child output line or a
    // new durable `-Data.db` artifact is reported as evidence in any failure
    // message — it does not, and may not, extend the deadline (design.md D6a).
    let stage = deadline.stage("d.clean-exit");
    let exited = poll_with_progress(&io, &data_dir, &stage, |slice, _artifacts| {
        child.wait_timeout(slice).expect("wait_timeout on child")
    });
    let (status, _t_exit): (ExitStatus, Duration) = match exited {
        Ok(v) => v,
        Err(fail) => {
            let _ = child.kill();
            panic!(
                "stage (d) clean-exit: the shutdown flush did not complete before the deadline.\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: the handler-entry marker {MARKER_HANDLER_ENTERED:?} WAS \
                 observed {:.3?} after SIGINT, so the shutdown handler exists, was entered, and \
                 the child was scheduled. This failure therefore establishes ONLY that the flush \
                 did not complete in time; it says nothing about whether a handler is present.\n\
                 child transcript (the snapshot the verdict was taken from):\n{}\n{}",
                fail.observed(),
                t_handler,
                fail.transcript(),
                stage.report()
            );
        }
    };
    stage.finish();
    // Release stdin only after the process has exited.
    drop(stdin);
    assert!(
        status.success(),
        "child exited uncleanly after SIGINT: {status:?}\nchild transcript:\n{}",
        io.transcript_text()
    );

    // Stage (e): durability. The SIGINT handler must have flushed the memtable to
    // a real SSTable — the row is present on an independent read-only reopen.
    let stage = deadline.stage("e.durability-read");
    let (rows, _t_read) = select_rows(&data_dir, &schema, "SELECT * FROM test_write.users", &stage);
    stage.finish();
    let grace = rows
        .iter()
        .find(|r| r.get("id").and_then(|v| v.as_i64()) == Some(7))
        .unwrap_or_else(|| {
            panic!(
                "row id=7 not durable after SIGINT; rows: {rows:?}\nchild transcript:\n{}",
                io.transcript_text()
            )
        });
    assert_eq!(
        grace["name"].as_str(),
        Some("Grace"),
        "durable row has wrong name: {grace}"
    );

    // Visible with `--nocapture`: the per-stage timings and the one deadline they
    // ran under, which is what makes a loaded-host run auditable (#3515 AC1).
    eprintln!(
        "[#3515] sigint_in_writable_session_flushes_before_exit\n{}\n[#3515]   {}",
        deadline.report(),
        deadline.describe()
    );

    // THE WAIT CENSUS IS CHECKED AGAINST THIS RUN (roborev job 253, finding 3).
    // The census is what the aggregate floor in `budgets.rs` is computed from, so a
    // stage that draws on the one deadline without appearing there means the floor
    // was asserted against an undercounted base — which is exactly how
    // `c.handler-entry` and `e.durability-read` came to be missing from it.
    assert_census_matches_run(
        "sigint_in_writable_session_flushes_before_exit",
        T1_WAIT_CENSUS,
        &deadline,
    );
}

/// Issue #1693 (roborev): the interactive writable loop must use the async,
/// threshold-flushing path (`execute_flushing`) rather than the sync `execute`
/// (which intentionally skips auto-flush in an async context). Otherwise a long
/// session grows the memtable past the flush threshold up to the hard limit and
/// then FAILS every write until exit.
///
/// This drives a real interactive session with a tiny flush threshold (env
/// override), writes several rows to cross it, and asserts a durable SSTable
/// appears MID-SESSION — before any Ctrl-D/Ctrl-C — and that writes keep being
/// accepted afterwards.
///
/// Oracle (issue #3515 AC4): this test carried the same defective shape in THREE
/// places (per-write ack, mid-session artifact wait, EOF exit), each a bare 60s
/// deadline whose expiry blamed a dead-ended session, or an interactive loop
/// that had bypassed the threshold-flushing path. A timeout establishes neither.
/// All three are now attribution stages under this test's ONE deadline, and the
/// two that poll observe (never credit) progress.
#[test]
fn writable_session_auto_flushes_mid_session_across_threshold() {
    const WRITES: i64 = 5;
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");
    let data_dir = wd.join("data");
    let deadline = TestDeadline::start(T2_DEADLINE_BASE, T2_DEADLINE_CAP);

    // Stage (a): session up. The tiny threshold makes a handful of small rows
    // cross it, forcing a mid-session flush without writing 64MB over stdin.
    let (mut child, io, t_boot) = start_writable_session(
        &wd,
        &schema,
        &[("CQLITE_MEMTABLE_FLUSH_THRESHOLD", "1")],
        &deadline,
    );
    deadline.calibrate("t_boot", t_boot);
    let mut stdin = child.stdin.take().expect("child stdin");

    // Stage (b): every write is acknowledged. Each of the five writes replaced an
    // INDEPENDENT 60s wait, and each may consume the whole deadline — which is why
    // the floor invariant needs no per-operation arithmetic here (roborev job 219,
    // finding 1: an aggregate argument is irrelevant per operation).
    //
    // `t_ack` is the SLOWEST SINGLE ack, which is the right CALIBRATION input (the
    // deadline should scale with how slow one round-trip is, not with how many were
    // done). It is NOT the stage's duration: recording it as such under-reported a
    // five-write stage by up to 5x, and the per-stage timing table is a deliverable
    // of this change (roborev job 222, finding 3). So the stage's own elapsed time
    // is what `Stage::finish` records, over the whole loop.
    let acks = deadline.stage("b.write-acks");
    let mut t_ack = Duration::ZERO;
    for id in 0..WRITES {
        // Per write, taken BEFORE the statement is sent (job 243, finding 1). Each
        // window still excludes the PREVIOUS ack — that line was recorded before
        // this mark — which is what stops one `OK` satisfying all five waits.
        //
        // THE PER-WRITE TIMER STARTS BEFORE THE WRITE (roborev job 253, finding
        // 1). `t_ack` is the calibration input, and a timer started after the
        // `writeln!`/`flush()` misses an ack recorded in that gap — the
        // mis-placed-timer form of this change's own original defect, which
        // leaves `scale` at 1.000 on a contended host. Unlike stage (b) in the
        // sibling test this cannot be the stage's own spend: the stage covers all
        // five writes (see above), so the per-write round-trip needs its own
        // instant, taken here.
        let before = Instant::now();
        let mark = io.mark();
        // Nothing new may be INITIATED past the one deadline (job 253, finding 2).
        require_live_or_kill(
            &acks,
            &io,
            &mut child,
            &format!("the id={id} write to the child's stdin"),
        );
        writeln!(
            stdin,
            "INSERT INTO test_write.users (id, name, age, active) VALUES ({id}, 'row{id}', {id}, true);"
        )
        .expect("write INSERT to child stdin");
        stdin.flush().expect("flush child stdin");

        await_write_ack(&io, mark, &format!("write id={id}"), &acks, &mut child);
        let this_ack = before.elapsed();
        t_ack = t_ack.max(this_ack);
        // **THE MEASUREMENT IS APPLIED THE MOMENT IT COMPLETES** (#3652, roborev
        // job 271 finding 5). Every ack used to reach `calibrate` only AFTER all
        // five waits had finished, so a slow FIRST ack could not extend the
        // deadline for the four writes that FOLLOWED it: the loop ran against a
        // deadline calibrated from `t_boot` alone, which is the
        // calibration-inertness class this harness treats as its most serious.
        //
        // Safe to call per write because calibration does not COMPOUND: the span
        // is `clamp(base x LARGEST scale, base, cap)`, derived from the base and
        // never from the current span, so five one-at-a-time calls and one call
        // with the slowest of the five produce the identical deadline (see
        // `TestDeadline::calibrate`). What it does add is one `observations` entry
        // per write, which is the diagnostic record — and five acks really did
        // happen.
        //
        // It takes `&self` for exactly this: through `&mut self` the live `acks`
        // stage's borrow made a mid-loop calibration unspellable.
        deadline.calibrate("t_ack(write)", this_ack);
    }
    // The stage's own elapsed time over all five writes — the DIAGNOSTIC — and the
    // slowest single ack, which is the value that DECIDED the scale above. It is
    // deliberately NOT re-folded here: it has already been applied, and a second
    // fold would record an observation for a measurement that was taken once.
    let t_acks_total = acks.finish();

    // Stage (c): a durable SSTable must exist BEFORE we close the session.
    let stage = deadline.stage("c.mid-session-flush");
    let flushed = poll_with_progress(&io, &data_dir, &stage, |slice, artifacts| {
        // The count the poll sampled for THIS iteration. Scanning again here would
        // be a second post-deadline directory walk on the expiry path, and that
        // overrun is what roborev job 236 finding 2 found the documented bound to
        // be silently permitting.
        if artifacts >= 1 {
            Some(())
        } else {
            thread::sleep(slice);
            None
        }
    });
    if let Err(fail) = flushed {
        let _ = child.kill();
        panic!(
            "stage (c) mid-session-flush: no durable `-Data.db` artifact appeared under {} \
             while the session was still open, after {WRITES} acknowledged writes with \
             CQLITE_MEMTABLE_FLUSH_THRESHOLD=1.\n\
             {}\n\
             WHAT THIS ESTABLISHES: only that no artifact was observed before the deadline. It \
             does NOT establish that the interactive loop skipped the threshold-flushing path \
             — a flush still in progress, or a child that was descheduled, produces the same \
             reading. The writes WERE acknowledged (stage (b) passed), so the session was \
             accepting statements.\n\
             child transcript (the snapshot the verdict was taken from):\n{}\n{}",
            data_dir.display(),
            fail.observed(),
            fail.transcript(),
            stage.report()
        );
    }
    stage.finish();

    // Stage (d): cleanly end via EOF; progress-observed exit wait. The stage is
    // opened BEFORE the `drop`, which is the operation that produces the awaited
    // exit — and the deadline is checked before it, because an EOF delivered after
    // expiry can still produce the exit this stage waits for (job 253, finding 2).
    let stage = deadline.stage("d.eof-exit");
    require_live_or_kill(
        &stage,
        &io,
        &mut child,
        "closing the child's stdin (the EOF that ends the session)",
    );
    // THE EOF IS PERFORMED BY THE POLL, AFTER THE POLL HAS ESTABLISHED ITS
    // OBSERVATION WINDOW (#3652, roborev job 262 finding 2). `drop(stdin)` used to
    // run HERE, before the call — and the poll's transcript mark and artifact
    // baseline are taken INSIDE it, so every line the child emitted and every
    // artifact it created between the `drop` and the poll's first read was outside
    // the window: a timeout could report "progress observed: NONE" against
    // evidence this harness had already produced. Moving the operation inside is
    // what makes the ordering structural instead of a convention this call site
    // has to remember; see `poll_with_progress_triggered`.
    let exited = poll_with_progress_triggered(
        &io,
        &data_dir,
        &stage,
        || drop(stdin),
        |slice, _artifacts| child.wait_timeout(slice).expect("wait_timeout on child"),
    );
    let (status, _t_exit): (ExitStatus, Duration) = match exited {
        Ok(v) => v,
        Err(fail) => {
            let _ = child.kill();
            panic!(
                "stage (d) eof-exit: the child had not exited after its stdin reached EOF.\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: only that no exit was observed before the deadline. The \
                 EOF path flushes and finalizes the engine before returning, so a slow flush and \
                 a wedged one read the same here; the progress observation above reports whether \
                 anything was still happening.\n\
                 child transcript (the snapshot the verdict was taken from):\n{}\n{}",
                fail.observed(),
                fail.transcript(),
                stage.report()
            );
        }
    };
    stage.finish();
    assert!(
        status.success(),
        "child exited uncleanly on EOF: {status:?}\nchild transcript:\n{}",
        io.transcript_text()
    );

    // Stage (e): all rows are durable on an independent read-only reopen.
    let stage = deadline.stage("e.durability-read");
    let (rows, _t_read) = select_rows(&data_dir, &schema, "SELECT * FROM test_write.users", &stage);
    stage.finish();
    for id in 0..WRITES {
        assert!(
            rows.iter()
                .any(|r| r.get("id").and_then(|v| v.as_i64()) == Some(id)),
            "row id={id} not durable after mid-session flush; rows: {rows:?}\n\
             child transcript:\n{}",
            io.transcript_text()
        );
    }

    eprintln!(
        "[#3515] writable_session_auto_flushes_mid_session_across_threshold\n{}\n[#3515]   {}",
        deadline.report(),
        deadline.describe()
    );
    eprintln!(
        "[#3515]   b.write-acks {WRITES} writes in {t_acks_total:.3?} (slowest single ack \
         {t_ack:.3?}; #3652: EVERY ack is folded into the deadline as it completes, and the \
         slowest is the one that decided the scale)"
    );

    // The census check, as in the sibling test (job 253, finding 3). This test's
    // census is where the FIVE waits inside one stage are declared, which is why
    // the census unit is a wait and not a stage.
    assert_census_matches_run(
        "writable_session_auto_flushes_mid_session_across_threshold",
        T2_WAIT_CENSUS,
        &deadline,
    );
}
