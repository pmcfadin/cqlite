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
