//! Issue #1693 (AG4) — graceful shutdown integration test.
//!
//! Drives the `cqlite` binary as a REAL child process in interactive
//! `--writable` mode, performs a write over stdin, sends it `SIGINT`, and
//! verifies the process:
//!   1. exits cleanly (success), and
//!   2. flushed the memtable to a durable SSTable before exiting — the written
//!      row is present when the write directory is reopened read-only.
//!
//! This is IMPOSSIBLE on the pre-fix code: there is no interactive writable
//! session and no SIGINT handler, so the process either exits immediately
//! (never reading the write) or, on Ctrl-C, dies without flushing — leaving the
//! row only in the WAL. The test is therefore RED before the CLI handler lands.
//!
//! Unix-only: it sends a real `SIGINT` via `libc::kill`.

#![cfg(all(feature = "write-support", unix))]

use serde_json::Value as Json;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use wait_timeout::ChildExt;

/// The `cqlite` binary this test crate built with `--features write-support`.
fn cqlite_bin() -> &'static str {
    env!("CARGO_BIN_EXE_cqlite")
}

/// Write the single-table schema used by the round-trip.
fn write_schema(dir: &Path) -> PathBuf {
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

/// Spawn a background reader that forwards each stdout line to a channel.
fn spawn_line_reader<R: std::io::Read + Send + 'static>(reader: R) -> Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let buf = BufReader::new(reader);
        for line in buf.lines() {
            match line {
                Ok(l) => {
                    if tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx
}

/// Block until a forwarded line satisfies `pred`, or `timeout` elapses.
fn wait_for_line<F: Fn(&str) -> bool>(
    rx: &Receiver<String>,
    pred: F,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match rx.recv_timeout(remaining.min(Duration::from_millis(250))) {
            Ok(line) if pred(&line) => return Some(line),
            Ok(_) => continue,
            Err(RecvTimeoutError::Timeout) => {
                if Instant::now() >= deadline {
                    return None;
                }
            }
            Err(RecvTimeoutError::Disconnected) => return None,
        }
    }
}

/// Reopen an SSTable directory read-only and SELECT, returning rows as JSON.
fn select_rows(data_dir: &Path, schema: &Path, query: &str) -> Vec<Json> {
    let out: Output = Command::new(cqlite_bin())
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
        .output()
        .expect("spawn read-side cqlite");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "SELECT failed: `{query}`\nstdout: {stdout}\nstderr: {stderr}"
    );
    match serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("SELECT did not emit JSON: {e}\nstdout: {stdout}"))
    {
        Json::Array(rows) => rows,
        other => panic!("expected a JSON array of rows, got: {other}"),
    }
}

/// AC (issue #1693): an interactive `--writable` session that receives SIGINT
/// after a write exits cleanly AND has flushed the row to a durable SSTable.
#[test]
fn sigint_in_writable_session_flushes_before_exit() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");

    let mut child = Command::new(cqlite_bin())
        .args([
            "--writable",
            "--write-dir",
            wd.to_str().unwrap(),
            "--schema",
            schema.to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cqlite interactive writable session");

    // Keep the stdin handle alive for the whole test so the child exits via
    // SIGINT, NOT via stdin EOF (an EOF would also flush and mask the bug).
    let mut stdin = child.stdin.take().expect("child stdin");
    let stdout_rx = spawn_line_reader(child.stdout.take().expect("child stdout"));

    // Perform a write and wait for the "OK" acknowledgement so we KNOW the row
    // is buffered in the memtable before we interrupt the process.
    writeln!(
        stdin,
        "INSERT INTO test_write.users (id, name, age, active) VALUES (7, 'Grace', 30, true);"
    )
    .expect("write INSERT to child stdin");
    stdin.flush().expect("flush child stdin");

    let ack = wait_for_line(&stdout_rx, |l| l.trim() == "OK", Duration::from_secs(60));
    assert!(
        ack.is_some(),
        "child never acknowledged the write with `OK` — no interactive writable session"
    );

    // Send a real SIGINT to the child.
    let pid = child.id() as libc::pid_t;
    let rc = unsafe { libc::kill(pid, libc::SIGINT) };
    assert_eq!(rc, 0, "failed to deliver SIGINT to child pid {pid}");

    // The child must exit cleanly within a generous window (saturated machine).
    let status = child
        .wait_timeout(Duration::from_secs(60))
        .expect("wait_timeout on child")
        .unwrap_or_else(|| {
            let _ = child.kill();
            panic!("child did not exit after SIGINT (no graceful shutdown handler)");
        });
    // Release stdin only after the process has exited.
    drop(stdin);
    assert!(
        status.success(),
        "child exited uncleanly after SIGINT: {status:?}"
    );

    // Durability: the SIGINT handler must have flushed the memtable to a real
    // SSTable — the row is present on an independent read-only reopen.
    let data_dir = wd.join("data");
    let rows = select_rows(&data_dir, &schema, "SELECT * FROM test_write.users");
    let grace = rows
        .iter()
        .find(|r| r.get("id").and_then(|v| v.as_i64()) == Some(7))
        .unwrap_or_else(|| panic!("row id=7 not durable after SIGINT; rows: {rows:?}"));
    assert_eq!(
        grace["name"].as_str(),
        Some("Grace"),
        "durable row has wrong name: {grace}"
    );
}

/// Count Data.db SSTable components under a write engine's data directory.
fn count_data_db(data_dir: &Path) -> usize {
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

/// Poll until `pred(count_data_db(..))` holds or `timeout` elapses.
fn wait_for_sstable<F: Fn(usize) -> bool>(data_dir: &Path, pred: F, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if pred(count_data_db(data_dir)) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(100));
    }
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
/// accepted afterwards. On the pre-fix code (sync `execute`) no SSTable is
/// produced until `close()`, so this test is RED before the fix.
#[test]
fn writable_session_auto_flushes_mid_session_across_threshold() {
    let tmp = TempDir::new().unwrap();
    let schema = write_schema(tmp.path());
    let wd = tmp.path().join("wd");
    let data_dir = wd.join("data");

    let mut child = Command::new(cqlite_bin())
        .args([
            "--writable",
            "--write-dir",
            wd.to_str().unwrap(),
            "--schema",
            schema.to_str().unwrap(),
        ])
        // Tiny threshold so a handful of small rows crosses it and forces a
        // mid-session flush without writing 64MB over stdin.
        .env("CQLITE_MEMTABLE_FLUSH_THRESHOLD", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cqlite interactive writable session");

    let mut stdin = child.stdin.take().expect("child stdin");
    let stdout_rx = spawn_line_reader(child.stdout.take().expect("child stdout"));

    // Write several rows; each write should trigger an auto-flush because the
    // threshold is effectively zero. Wait for each OK so the session is making
    // progress (and not dead-ending at the hard limit).
    for id in 0..5 {
        writeln!(
            stdin,
            "INSERT INTO test_write.users (id, name, age, active) VALUES ({id}, 'row{id}', {id}, true);"
        )
        .expect("write INSERT to child stdin");
        stdin.flush().expect("flush child stdin");
        let ack = wait_for_line(&stdout_rx, |l| l.trim() == "OK", Duration::from_secs(60));
        assert!(
            ack.is_some(),
            "write id={id} was not acknowledged — session dead-ended (no mid-session flush?)"
        );
    }

    // A durable SSTable must exist BEFORE we close the session. On the sync
    // `execute` path this stays 0 until close(); the flushing path produces one.
    let flushed = wait_for_sstable(&data_dir, |n| n >= 1, Duration::from_secs(60));
    assert!(
        flushed,
        "no Data.db SSTable appeared mid-session — interactive loop did not use the \
         threshold-flushing path (issue #1693)"
    );

    // Cleanly end via EOF and confirm the process exits successfully.
    drop(stdin);
    let status = child
        .wait_timeout(Duration::from_secs(60))
        .expect("wait_timeout on child")
        .unwrap_or_else(|| {
            let _ = child.kill();
            panic!("child did not exit on stdin EOF");
        });
    assert!(
        status.success(),
        "child exited uncleanly on EOF: {status:?}"
    );

    // All rows are durable on an independent read-only reopen.
    let rows = select_rows(&data_dir, &schema, "SELECT * FROM test_write.users");
    for id in 0..5 {
        assert!(
            rows.iter()
                .any(|r| r.get("id").and_then(|v| v.as_i64()) == Some(id)),
            "row id={id} not durable after mid-session flush; rows: {rows:?}"
        );
    }
}
