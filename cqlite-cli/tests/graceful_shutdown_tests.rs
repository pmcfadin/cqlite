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
//! whole deadline, so no wait here can fire sooner than the 60s bound it
//! replaced; and observed progress is reported as evidence but never extends
//! anything, so no wait is granted or started past the declared bound.
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
    let mut deadline = TestDeadline::start(T1_DEADLINE_BASE, T1_DEADLINE_CAP);

    // Stage (a): session up.
    let (mut child, io, t_boot) = start_writable_session(&wd, &schema, &[], &deadline);
    // First in-band measurement: fold it into the one deadline. Calibration is
    // monotone, so this can only ever move the deadline later.
    deadline.calibrate("t_boot", t_boot);

    // Keep the stdin handle alive for the whole test so the child exits via
    // SIGINT, NOT via stdin EOF (an EOF would also flush and mask the bug).
    let mut stdin = child.stdin.take().expect("child stdin");

    // Stage (b): write ack, timed -> `t_ack`.
    writeln!(
        stdin,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (7, 'Grace', 30, true);"
    )
    .expect("write INSERT to child stdin");
    stdin.flush().expect("flush child stdin");

    let stage = deadline.stage("b.write-ack");
    let t_ack = await_write_ack(&io, "the INSERT (id=7)", &stage);
    stage.finish();
    deadline.calibrate("t_ack", t_ack);

    // Send a real SIGINT to the child.
    let pid = child.id() as libc::pid_t;
    let rc = unsafe { libc::kill(pid, libc::SIGINT) };
    assert_eq!(rc, 0, "failed to deliver SIGINT to child pid {pid}");

    // Stage (c): handler ENTERED. Observing this marker establishes, together,
    // that the signal was delivered, that a shutdown handler exists and was
    // entered, and that the child was scheduled — so stage (d) below may never
    // claim anything about a handler's existence.
    let stage = deadline.stage("c.handler-entry");
    let entered = io.wait_for(
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
             child transcript:\n{}\n{}",
            stage.describe(),
            end.describe(),
            io.transcript_text(),
            stage.report()
        );
        }
    };
    stage.finish();

    // Stage (d): clean exit, with progress OBSERVED. A new child output line or a
    // new durable `-Data.db` artifact is reported as evidence in any failure
    // message — it does not, and may not, extend the deadline (design.md D6a).
    let stage = deadline.stage("d.clean-exit");
    let exited = poll_with_progress(&io, &data_dir, &stage, |slice| {
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
                 durable -Data.db artifacts under {}: {}\n\
                 child transcript:\n{}\n{}",
                fail.observed(),
                t_handler,
                data_dir.display(),
                count_data_db(&data_dir),
                io.transcript_text(),
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
    let mut deadline = TestDeadline::start(T2_DEADLINE_BASE, T2_DEADLINE_CAP);

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
        writeln!(
            stdin,
            "INSERT INTO test_write.users (id, name, age, active) VALUES ({id}, 'row{id}', {id}, true);"
        )
        .expect("write INSERT to child stdin");
        stdin.flush().expect("flush child stdin");

        let before = Instant::now();
        await_write_ack(&io, &format!("write id={id}"), &acks);
        t_ack = t_ack.max(before.elapsed());
    }
    let t_acks_total = acks.finish();
    deadline.calibrate("t_ack(slowest of 5)", t_ack);

    // Stage (c): a durable SSTable must exist BEFORE we close the session.
    let stage = deadline.stage("c.mid-session-flush");
    let flushed = poll_with_progress(&io, &data_dir, &stage, |slice| {
        if count_data_db(&data_dir) >= 1 {
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
             child transcript:\n{}\n{}",
            data_dir.display(),
            fail.observed(),
            io.transcript_text(),
            stage.report()
        );
    }
    stage.finish();

    // Stage (d): cleanly end via EOF; progress-observed exit wait.
    drop(stdin);
    let stage = deadline.stage("d.eof-exit");
    let exited = poll_with_progress(&io, &data_dir, &stage, |slice| {
        child.wait_timeout(slice).expect("wait_timeout on child")
    });
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
                 durable -Data.db artifacts under {}: {}\n\
                 child transcript:\n{}\n{}",
                fail.observed(),
                data_dir.display(),
                count_data_db(&data_dir),
                io.transcript_text(),
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
         {t_ack:.3?}, which is the calibration input)"
    );
}
