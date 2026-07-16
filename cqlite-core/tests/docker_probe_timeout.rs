//! Issue #1819: the Docker-availability probe used by the write-support
//! Cassandra e2e tests MUST return within a bounded time even when the
//! underlying `docker` command hangs forever (unresponsive daemon), instead of
//! wedging the test binary — and therefore `scripts/agent-gate.sh` — the way an
//! unbounded `Command::output()` did.
//!
//! These tests exercise the real shared seam (`common/docker_probe.rs`) through
//! a mockable command path: the command name is a parameter, so a `sh -c
//! 'sleep 3600'` stands in for an unresponsive `docker info`. The assertion is
//! about the probe's deadline, not about any actual Docker install.

#[path = "common/docker_probe.rs"]
mod docker_probe;

use std::time::{Duration, Instant};

use docker_probe::{bounded_probe, ProbeOutcome};

/// The core #1819 guarantee: a command that never returns is abandoned at the
/// deadline, so the probe reports `TimedOut` promptly rather than hanging.
#[cfg(unix)]
#[test]
fn bounded_probe_returns_promptly_when_command_hangs() {
    // Mimics an unresponsive `docker info`: accepts, then blocks indefinitely.
    let budget = Duration::from_millis(400);
    let start = Instant::now();
    let outcome = bounded_probe("sh", &["-c", "sleep 3600"], budget);
    let elapsed = start.elapsed();

    assert!(
        matches!(outcome, ProbeOutcome::TimedOut),
        "a hung command must report TimedOut, got {outcome:?}"
    );
    // Generous ceiling (kill + reap slack) that is still far below the old
    // unbounded 16+ min wedge.
    assert!(
        elapsed < Duration::from_secs(10),
        "probe must return near the {budget:?} budget, took {elapsed:?}"
    );
}

/// A fast, successful command completes within budget and its stdout is
/// captured — the "Docker healthy" path is unaffected by the deadline.
#[cfg(unix)]
#[test]
fn bounded_probe_completes_for_fast_command() {
    let outcome = bounded_probe("sh", &["-c", "echo ok"], Duration::from_secs(5));
    match outcome {
        ProbeOutcome::Completed { success, stdout } => {
            assert!(success, "fast command should exit 0");
            assert!(
                stdout.contains("ok"),
                "stdout should be captured: {stdout:?}"
            );
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// A non-zero exit is surfaced as `Completed { success: false }`, distinct from
/// a timeout — callers treat both as "unavailable" but for different reasons.
#[cfg(unix)]
#[test]
fn bounded_probe_reports_failure_exit() {
    let outcome = bounded_probe("sh", &["-c", "exit 1"], Duration::from_secs(5));
    assert!(
        matches!(outcome, ProbeOutcome::Completed { success: false, .. }),
        "a non-zero exit must be Completed{{success:false}}, got {outcome:?}"
    );
}

/// Regression guard for the concurrent-drain fix: a command that writes far
/// more than one OS pipe buffer (~64 KB) to stdout AND stderr but exits
/// promptly must be reported as `Completed` with its output fully captured —
/// NOT `TimedOut`. Under the old drain-after-wait approach the child would
/// block on its own `write()` once the pipe buffer filled, never exit, and get
/// killed at the deadline (a false `TimedOut` against a healthy command).
#[cfg(unix)]
#[test]
fn bounded_probe_captures_output_larger_than_pipe_buffer() {
    // ~200 KB per stream — comfortably past the ~64 KB Linux pipe buffer, so a
    // non-concurrent drain would deadlock. `yes X | head -c N` is portable
    // across POSIX shells and needs no external data files.
    const N: usize = 200_000;
    let script = format!("yes X | head -c {N} ; yes E | head -c {N} 1>&2");

    let budget = Duration::from_secs(10);
    let start = Instant::now();
    let outcome = bounded_probe("sh", &["-c", &script], budget);
    let elapsed = start.elapsed();

    match outcome {
        ProbeOutcome::Completed { success, stdout } => {
            assert!(success, "the command should exit 0");
            assert_eq!(
                stdout.len(),
                N,
                "all {N} stdout bytes must be captured, got {}",
                stdout.len()
            );
        }
        other => panic!(
            "large-output command must be Completed (not TimedOut) — got {other:?} \
             after {elapsed:?}"
        ),
    }
    // The command exits as fast as it can write; it must finish well within the
    // generous budget rather than being killed at the deadline.
    assert!(
        elapsed < Duration::from_secs(9),
        "large-output command should complete promptly, took {elapsed:?}"
    );
}

/// A missing binary yields `SpawnFailed`, never a hang or panic.
#[test]
fn bounded_probe_reports_spawn_failure() {
    let outcome = bounded_probe(
        "cqlite-nonexistent-docker-binary-1819",
        &["info"],
        Duration::from_secs(5),
    );
    assert!(
        matches!(outcome, ProbeOutcome::SpawnFailed),
        "a missing binary must be SpawnFailed, got {outcome:?}"
    );
}
