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
//! | d. clean exit      | process exit, progress-checked     | the shutdown flush did not complete in time |
//!
//! Observing (c) proves three things at once — the signal was delivered, a
//! shutdown handler exists and was entered, and the child was scheduled — which
//! is exactly the conjunction the old message guessed at. So (d) may never claim
//! anything about the *existence* of a handler.
//!
//! Every budget that follows a completed measurement is calibrated from that
//! measurement, taken on this host in this run: `clamp(base * scale, base, cap)`
//! with `scale = max(1, observed / quiet_baseline)`. The baselines are generous
//! (seconds), so a quiet host always yields `scale == 1`: calibration can only
//! loosen a budget, never tighten one.
//!
//! Unix-only: it sends a real `SIGINT` via `libc::kill`.

#![cfg(all(feature = "write-support", unix))]

mod graceful_shutdown_support;

use graceful_shutdown_support::*;
use std::io::Write;
use std::process::ExitStatus;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use wait_timeout::ChildExt;

/// AC (issue #1693): an interactive `--writable` session that receives SIGINT
/// after a write exits cleanly AND has flushed the row to a durable SSTable.
///
/// Oracle (issue #3515): four staged waits, each reporting only what it measures.
#[test]
fn sigint_in_writable_session_flushes_before_exit() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");
    let data_dir = wd.join("data");
    let mut clock = StageClock::new(TEST_TOTAL_BUDGET);

    // Stage (a): session up (bare deadline — the irreducible bound).
    let (mut child, io, t_boot) = start_writable_session(&wd, &schema, &[], &mut clock);

    // Keep the stdin handle alive for the whole test so the child exits via
    // SIGINT, NOT via stdin EOF (an EOF would also flush and mask the bug).
    let mut stdin = child.stdin.take().expect("child stdin");

    // Stage (b): write ack, timed -> `t_ack`. Budget calibrated from `t_boot`.
    writeln!(
        stdin,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (7, 'Grace', 30, true);"
    )
    .expect("write INSERT to child stdin");
    stdin.flush().expect("flush child stdin");

    let ack_budget = clock.clip(calibrated(T1_ACK, t_boot, "t_boot", BOOT_QUIET_BASELINE));
    let t_ack = await_write_ack(
        &io,
        "stage (b) write-ack",
        "the INSERT (id=7)",
        &ack_budget,
        &clock,
    );
    clock.record("b.write-ack", t_ack);

    // The stall window for the progress-checked exit wait is calibrated from the
    // same `t_ack`: on a host where a full write round-trip takes seconds, a
    // few seconds of silence is not evidence of a stall.
    let stall_window = calibrated(STALL_WINDOW, t_ack, "t_ack", ACK_QUIET_BASELINE);

    // Send a real SIGINT to the child.
    let pid = child.id() as libc::pid_t;
    let rc = unsafe { libc::kill(pid, libc::SIGINT) };
    assert_eq!(rc, 0, "failed to deliver SIGINT to child pid {pid}");

    // Stage (c): handler ENTERED. Observing this marker establishes, together,
    // that the signal was delivered, that a shutdown handler exists and was
    // entered, and that the child was scheduled — so stage (d) below may never
    // claim anything about a handler's existence.
    let handler_budget = clock.clip(calibrated(T1_HANDLER, t_ack, "t_ack", ACK_QUIET_BASELINE));
    let entered = io.wait_for(
        Stream::Stderr,
        |l| l.contains(MARKER_HANDLER_ENTERED),
        handler_budget.derived,
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
            handler_budget.describe(),
            end.describe(),
            io.transcript_text(),
            clock.report()
        );
        }
    };
    clock.record("c.handler-entry", t_handler);

    // Stage (d): clean exit, PROGRESS-CHECKED. A new child output line or a new
    // durable `-Data.db` artifact resets the stall window, so a flush that is
    // landing slowly is never mistaken for a stall.
    let exit_budget = clock.clip(calibrated(T1_EXIT, t_ack, "t_ack", ACK_QUIET_BASELINE));
    let envelope = clock.remaining();
    let exited = poll_with_progress(
        &io,
        &data_dir,
        &exit_budget,
        stall_window.derived,
        envelope,
        |slice| child.wait_timeout(slice).expect("wait_timeout on child"),
    );
    let (status, t_exit): (ExitStatus, Duration) = match exited {
        Ok(v) => v,
        Err(fail) => {
            let _ = child.kill();
            panic!(
                "stage (d) clean-exit: the shutdown flush did not complete within the budget.\n\
                 {}\n\
                 stall window {}\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: the handler-entry marker {MARKER_HANDLER_ENTERED:?} WAS \
                 observed {:.3?} after SIGINT, so the shutdown handler exists, was entered, and \
                 the child was scheduled. This failure therefore establishes ONLY that the flush \
                 did not complete in time; it says nothing about whether a handler is present.\n\
                 durable -Data.db artifacts under {}: {}\n\
                 child transcript:\n{}\n{}",
                exit_budget.describe(),
                stall_window.describe(),
                fail.observed(),
                t_handler,
                data_dir.display(),
                count_data_db(&data_dir),
                io.transcript_text(),
                clock.report()
            );
        }
    };
    clock.record("d.clean-exit", t_exit);
    // Release stdin only after the process has exited.
    drop(stdin);
    assert!(
        status.success(),
        "child exited uncleanly after SIGINT: {status:?}\nchild transcript:\n{}",
        io.transcript_text()
    );

    // Stage (e): durability. The SIGINT handler must have flushed the memtable to
    // a real SSTable — the row is present on an independent read-only reopen.
    // A fresh CLI process doing a read is the same shape of work as the session
    // boot, so this budget is calibrated from `t_boot`.
    let read_budget = clock.clip(calibrated(T1_READ, t_boot, "t_boot", BOOT_QUIET_BASELINE));
    let (rows, t_read) = select_rows(
        &data_dir,
        &schema,
        "SELECT * FROM test_write.users",
        &read_budget,
        &clock,
    );
    clock.record("e.durability-read", t_read);
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

    // Visible with `--nocapture`: the per-stage timings and the budgets they
    // derived, which is what makes a loaded-host run auditable (#3515 AC1).
    eprintln!(
        "[#3515] sigint_in_writable_session_flushes_before_exit\n{}",
        clock.report()
    );
    eprintln!("[#3515]   b.write-ack       {}", ack_budget.describe());
    eprintln!("[#3515]   c.handler-entry   {}", handler_budget.describe());
    eprintln!("[#3515]   d.clean-exit      {}", exit_budget.describe());
    eprintln!("[#3515]   e.durability-read {}", read_budget.describe());
    eprintln!("[#3515]   stall window      {}", stall_window.describe());
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
/// All three are now staged, calibrated and (where they poll) progress-checked.
#[test]
fn writable_session_auto_flushes_mid_session_across_threshold() {
    const WRITES: i64 = 5;
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");
    let data_dir = wd.join("data");
    let mut clock = StageClock::new(TEST_TOTAL_BUDGET);

    // Stage (a): session up (bare deadline — the irreducible bound). The tiny
    // threshold makes a handful of small rows cross it, forcing a mid-session
    // flush without writing 64MB over stdin.
    let (mut child, io, t_boot) = start_writable_session(
        &wd,
        &schema,
        &[("CQLITE_MEMTABLE_FLUSH_THRESHOLD", "1")],
        &mut clock,
    );
    let mut stdin = child.stdin.take().expect("child stdin");

    // Stage (b): every write is acknowledged. The first ack is calibrated from
    // `t_boot`; each later one from the slowest ack seen so far, so a session
    // that is merely slow keeps loosening its own budget.
    let mut t_ack = Duration::ZERO;
    // Writes id=1..4 replaced four INDEPENDENT 60s waits, so each carries the full
    // old bound as its base; `clock.clip` IS the group deadline and bounds their
    // aggregate (roborev job 219, finding 1).
    for id in 0..WRITES {
        writeln!(
            stdin,
            "INSERT INTO test_write.users (id, name, age, active) VALUES ({id}, 'row{id}', {id}, true);"
        )
        .expect("write INSERT to child stdin");
        stdin.flush().expect("flush child stdin");

        // The FIRST write's ack shares the old 60s bound with stage (a) (that
        // deadline covered boot as well), so it carries a larger base than the
        // later ones; see the floor invariant above.
        let (stage_spec, observed, name, baseline) = if id == 0 {
            (T2_ACK_FIRST, t_boot, "t_boot", BOOT_QUIET_BASELINE)
        } else {
            (
                T2_ACK_LATER,
                t_ack,
                "t_ack(slowest so far)",
                ACK_QUIET_BASELINE,
            )
        };
        let budget = clock.clip(calibrated(stage_spec, observed, name, baseline));
        let took = await_write_ack(
            &io,
            "stage (b) write-ack",
            &format!("write id={id}"),
            &budget,
            &clock,
        );
        t_ack = t_ack.max(took);
    }
    clock.record("b.write-acks", t_ack);

    let stall_window = calibrated(STALL_WINDOW, t_ack, "t_ack", ACK_QUIET_BASELINE);

    // Stage (c): a durable SSTable must exist BEFORE we close the session.
    // Progress-checked, and calibrated from `t_ack`.
    let sstable_budget = clock.clip(calibrated(T2_SSTABLE, t_ack, "t_ack", ACK_QUIET_BASELINE));
    let envelope = clock.remaining();
    let flushed = poll_with_progress(
        &io,
        &data_dir,
        &sstable_budget,
        stall_window.derived,
        envelope,
        |slice| {
            if count_data_db(&data_dir) >= 1 {
                Some(())
            } else {
                thread::sleep(slice);
                None
            }
        },
    );
    let t_sstable = match flushed {
        Ok((_, took)) => took,
        Err(fail) => {
            let _ = child.kill();
            panic!(
                "stage (c) mid-session-flush: no durable `-Data.db` artifact appeared under {} \
                 while the session was still open, after {WRITES} acknowledged writes with \
                 CQLITE_MEMTABLE_FLUSH_THRESHOLD=1.\n\
                 {}\n\
                 stall window {}\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: only that no artifact was observed within that budget. It \
                 does NOT establish that the interactive loop skipped the threshold-flushing path \
                 — a flush still in progress, or a child that was descheduled, produces the same \
                 reading. The writes WERE acknowledged (stage (b) passed), so the session was \
                 accepting statements.\n\
                 child transcript:\n{}\n{}",
                data_dir.display(),
                sstable_budget.describe(),
                stall_window.describe(),
                fail.observed(),
                io.transcript_text(),
                clock.report()
            );
        }
    };
    clock.record("c.mid-session-flush", t_sstable);

    // Stage (d): cleanly end via EOF; progress-checked exit wait.
    drop(stdin);
    let exit_budget = clock.clip(calibrated(T2_EOF_EXIT, t_ack, "t_ack", ACK_QUIET_BASELINE));
    let envelope = clock.remaining();
    let exited = poll_with_progress(
        &io,
        &data_dir,
        &exit_budget,
        stall_window.derived,
        envelope,
        |slice| child.wait_timeout(slice).expect("wait_timeout on child"),
    );
    let (status, t_exit): (ExitStatus, Duration) = match exited {
        Ok(v) => v,
        Err(fail) => {
            let _ = child.kill();
            panic!(
                "stage (d) eof-exit: the child had not exited after its stdin reached EOF.\n\
                 {}\n\
                 stall window {}\n\
                 {}\n\
                 WHAT THIS ESTABLISHES: only that no exit was observed within that budget. The \
                 EOF path flushes and finalizes the engine before returning, so a slow flush and \
                 a wedged one read the same here; the progress check above reports whether \
                 anything was still happening.\n\
                 durable -Data.db artifacts under {}: {}\n\
                 child transcript:\n{}\n{}",
                exit_budget.describe(),
                stall_window.describe(),
                fail.observed(),
                data_dir.display(),
                count_data_db(&data_dir),
                io.transcript_text(),
                clock.report()
            );
        }
    };
    clock.record("d.eof-exit", t_exit);
    assert!(
        status.success(),
        "child exited uncleanly on EOF: {status:?}\nchild transcript:\n{}",
        io.transcript_text()
    );

    // Stage (e): all rows are durable on an independent read-only reopen.
    let read_budget = clock.clip(calibrated(T2_READ, t_boot, "t_boot", BOOT_QUIET_BASELINE));
    let (rows, t_read) = select_rows(
        &data_dir,
        &schema,
        "SELECT * FROM test_write.users",
        &read_budget,
        &clock,
    );
    clock.record("e.durability-read", t_read);
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
        "[#3515] writable_session_auto_flushes_mid_session_across_threshold\n{}",
        clock.report()
    );
    eprintln!(
        "[#3515]   c.mid-session-flush {}",
        sstable_budget.describe()
    );
    eprintln!("[#3515]   d.eof-exit          {}", exit_budget.describe());
    eprintln!("[#3515]   e.durability-read   {}", read_budget.describe());
    eprintln!("[#3515]   stall window        {}", stall_window.describe());
}
