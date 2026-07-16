//! Wall-clock-bounded subprocess probe for the Docker-availability gate shared
//! by the write-support Cassandra e2e tests (issue #1819).
//!
//! # Why this exists
//!
//! The Docker-availability gate (`cassandra_5_image`) shells out to
//! `docker info` and `docker images` to decide whether the live `sstabledump`
//! comparison can run. Those calls used `std::process::Command::output()` with
//! **no timeout**. When the local Docker daemon is *present but unresponsive*
//! (the socket accepts the connection but never answers — observed on a CI
//! runner where `docker info` blocked > 30 s), `output()` blocks forever
//! draining the child's pipes, so the `#[tokio::test]` current-thread runtime —
//! and therefore `scripts/agent-gate.sh` — wedged permanently (16+ min at 0%
//! CPU). This is the same *blocking-wait-with-no-deadline* hazard family as
//! #1691 / #1692, except the blocking primitive here is a subprocess wait
//! rather than a channel `recv`.
//!
//! # The fix
//!
//! [`bounded_probe`] spawns the command, waits at most `timeout` via
//! `wait_timeout::ChildExt::wait_timeout`, and **kills the child on timeout**,
//! reporting [`ProbeOutcome::TimedOut`]. A healthy `docker info` / `docker
//! images` returns near-instantly, so a few-second budget only ever bites an
//! unresponsive daemon — which then degrades to a clean SKIP (or, in strict
//! mode, a loud FAIL) instead of an unbounded hang.
//!
//! The command *name/path* is a parameter (a mockable-command seam), so a test
//! can point it at a fake `docker` that sleeps forever and prove the probe
//! still returns within the budget.

use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::Duration;

use wait_timeout::ChildExt;

/// Default wall-clock budget for a Docker-availability probe. A healthy daemon
/// answers `docker info` / `docker images` in well under a second; this budget
/// only ever bites an unresponsive daemon.
#[allow(dead_code)]
pub const DOCKER_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Outcome of a single bounded subprocess probe.
#[derive(Debug)]
pub enum ProbeOutcome {
    /// The command completed within the budget. `success` is the exit status'
    /// success flag; `stdout` is the captured standard output.
    Completed { success: bool, stdout: String },
    /// The command did not finish within the budget and was killed. The
    /// underlying resource (e.g. an unresponsive Docker daemon) is treated as
    /// unavailable.
    TimedOut,
    /// The command could not be spawned at all (e.g. the `docker` binary is not
    /// on `PATH`) — equivalent to "not installed".
    SpawnFailed,
}

/// A concurrently-draining reader for one of the child's pipes.
///
/// The reader thread drains its pipe to end-of-file into a buffer and delivers
/// it over a channel. Draining happens *concurrently* with the main thread's
/// `wait_timeout` poll loop, mirroring what `Command::output()` does internally:
/// a child that emits more than one OS pipe buffer (~64 KB) of output would
/// otherwise block on its own `write()` — never exiting — and be misreported as
/// `TimedOut`. `read_to_end` errors are treated as "no more output" (the partial
/// buffer is kept), so the thread never panics.
struct PipeReader {
    handle: JoinHandle<()>,
    rx: Receiver<Vec<u8>>,
}

impl PipeReader {
    /// Spawn a reader draining `pipe` to EOF; `None` yields an already-satisfied
    /// reader that produces empty output.
    fn spawn<R: Read + Send + 'static>(pipe: Option<R>) -> Self {
        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let mut buf = Vec::new();
            if let Some(mut pipe) = pipe {
                let _ = pipe.read_to_end(&mut buf);
            }
            // The receiver may already be gone (abandoned reader); ignore.
            let _ = tx.send(buf);
        });
        PipeReader { handle, rx }
    }

    /// Collect the drained output, waiting at most `grace` for the reader.
    ///
    /// When the child has exited normally its pipes close, so the reader
    /// delivers immediately and we join the (finished) thread for cleanliness.
    /// If `grace` elapses first — which happens when an orphaned *grandchild*
    /// (e.g. a `sleep` the killed shell forked) still holds the pipe's write end
    /// open — the reader is abandoned rather than joined, so the probe stays
    /// bounded and never re-introduces the #1819 hang. A never-joined thread is
    /// reclaimed when the test process exits. Output is returned lossily; a
    /// panicked reader degrades to empty output rather than propagating.
    fn collect(self, grace: Duration) -> Vec<u8> {
        match self.rx.recv_timeout(grace) {
            Ok(buf) => {
                let _ = self.handle.join();
                buf
            }
            Err(_) => Vec::new(),
        }
    }
}

/// Run `bin args...` with a hard wall-clock `timeout`.
///
/// Never blocks longer than `timeout` (plus a small drain grace) even if the
/// child never exits: on timeout the child is killed and reaped and
/// [`ProbeOutcome::TimedOut`] is returned.
///
/// Both `stdout` and `stderr` are drained *concurrently* with the wait loop by
/// dedicated reader threads, so a child that writes more than one OS pipe
/// buffer (~64 KB) cannot deadlock on its own `write()` and be misreported as
/// `TimedOut` (mirrors `Command::output()`'s internal concurrent draining).
#[allow(dead_code)]
pub fn bounded_probe(bin: &str, args: &[&str], timeout: Duration) -> ProbeOutcome {
    // On timeout the killed child's pipes normally close and the readers finish
    // at once; this bounded grace only bites when an orphaned grandchild keeps a
    // write end open, in which case the reader is abandoned to keep the probe
    // prompt.
    const DRAIN_GRACE: Duration = Duration::from_secs(2);

    let mut child = match Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return ProbeOutcome::SpawnFailed,
    };

    // Start draining both pipes immediately, before the wait loop, so the child
    // never blocks on a full pipe buffer.
    let stdout_reader = PipeReader::spawn(child.stdout.take());
    let stderr_reader = PipeReader::spawn(child.stderr.take());

    match child.wait_timeout(timeout) {
        Ok(Some(status)) => {
            // Child exited: pipes are (or will be) closed, so the readers
            // deliver promptly. Collect the fully captured output.
            let stdout = stdout_reader.collect(DRAIN_GRACE);
            let _stderr = stderr_reader.collect(DRAIN_GRACE);
            ProbeOutcome::Completed {
                success: status.success(),
                stdout: String::from_utf8_lossy(&stdout).to_string(),
            }
        }
        // Timed out, or waiting itself errored: kill + reap and report a timeout
        // so an unresponsive daemon degrades to "unavailable", never a hang.
        Ok(None) | Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            let _stdout = stdout_reader.collect(DRAIN_GRACE);
            let _stderr = stderr_reader.collect(DRAIN_GRACE);
            ProbeOutcome::TimedOut
        }
    }
}

/// Convenience wrapper: run a bounded `docker <args...>` probe with the default
/// [`DOCKER_PROBE_TIMEOUT`]. Returns `Some(stdout)` only when `docker` ran and
/// exited successfully within budget; `None` on non-zero exit, a spawn failure,
/// or a timeout (all "Docker unavailable" from the caller's perspective).
#[allow(dead_code)]
pub fn docker_probe(args: &[&str]) -> Option<String> {
    match bounded_probe("docker", args, DOCKER_PROBE_TIMEOUT) {
        ProbeOutcome::Completed {
            success: true,
            stdout,
        } => Some(stdout),
        _ => None,
    }
}
