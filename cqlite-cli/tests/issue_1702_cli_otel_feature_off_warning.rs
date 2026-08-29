//! Issue #1702 (epic #1686, AI2 "observability honesty"): END-TO-END wiring
//! evidence that `CQLITE_OTEL_ENABLED=1` on a build with observability compiled
//! out is VISIBLE on the shipped `cqlite` binary's STDERR — and that stdout is
//! untouched.
//!
//! # The chain this proves
//!
//! ```text
//! CQLITE_OTEL_ENABLED=1                                   (the operator knob)
//!   -> cqlite_cli::config::Config::load                   (env/file/flag precedence)
//!   -> config.observability.to_core()                      (ObservabilityConfig)
//!   -> cqlite_cli::telemetry::init_telemetry               (the ORDERING fix, #1702)
//!        subscriber FIRST with the feature off, so the warn has a sink
//!   -> cqlite_core::observability::init                    (the emit site)
//!   -> tracing fmt layer -> STDERR                        (never stdout, #129)
//! ```
//!
//! It runs the REAL `CARGO_BIN_EXE_cqlite` executable, so unlike a library-level
//! test it covers `main.rs`'s actual init order. That order is the crux: with the
//! feature off, `init` used to run BEFORE the subscriber existed, so even a
//! correct `tracing::warn!` inside `init` went to the global no-op subscriber and
//! was lost — a second silent no-op stacked on the first. This test is red
//! against that order even with the core fix in place.
//!
//! # Why feature-off only
//!
//! Gated `#![cfg(not(feature = "observability"))]`, matching the build under
//! test: the emit exists ONLY in the `cfg(not(feature = "observability"))` copy
//! of `observability::init`, so AC2 ("no such warning in a feature-enabled
//! build") is structural — there is no compiled code to exercise. Driving the
//! feature-on binary with `enabled = true` would also try to stand up a real
//! OTLP exporter, which is not something a unit test should need.

#![cfg(not(feature = "observability"))]

use std::process::{Command, Output};

use tempfile::TempDir;

/// Substrings the warning must carry for an operator to act on it: the knob they
/// set, the cargo feature that is missing (this is what separates "built without
/// the feature" from "collector down"), and the consequence.
const REQUIRED_SUBSTRINGS: &[&str] = &[
    "CQLITE_OTEL_ENABLED",
    "observability",
    "will be emitted",
    "NOT a collector",
];

/// A stable, cheap invocation that reaches `run_main` (so telemetry init runs)
/// and writes deterministic bytes to stdout. `--version`/`--help` are NOT usable:
/// clap handles them during `parse()` and exits before any init happens.
const ARGS: &[&str] = &["info"];

/// Run the shipped binary in a throwaway cwd (`info` creates `cqlite.db`
/// relative to cwd, so this keeps the repo clean and each case isolated).
/// `RUST_LOG` and every `CQLITE_OTEL_*` var are cleared first so an ambient
/// environment can neither raise the filter above WARN nor pre-set the knob.
fn run(otel_enabled: Option<&str>) -> Output {
    let cwd = TempDir::new().expect("tempdir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cqlite"));
    cmd.args(ARGS)
        .current_dir(cwd.path())
        .env_remove("RUST_LOG")
        .env_remove("CQLITE_OTEL_ENABLED")
        .env_remove("CQLITE_OTEL_ENDPOINT")
        .env_remove("CQLITE_OTEL_PROTOCOL")
        .env_remove("CQLITE_OTEL_SERVICE_NAME")
        .env_remove("CQLITE_OTEL_SERVICE_VERSION")
        .env_remove("CQLITE_OTEL_SAMPLING_RATIO")
        .env_remove("CQLITE_OTEL_TIMEOUT_MS");
    if let Some(v) = otel_enabled {
        cmd.env("CQLITE_OTEL_ENABLED", v);
    }
    let out = cmd.output().expect("cqlite binary runs");
    assert!(
        out.status.success(),
        "`cqlite {}` must succeed; stderr:\n{}",
        ARGS.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Count occurrences of the warning's anchor phrase in a stream. Anchored on
/// the "built WITHOUT the feature" clause — the part that IS the diagnosis —
/// rather than on the env-var name, which the message only cites as one of
/// several possible sources of `enabled`.
fn warning_hits(stream: &str) -> usize {
    stream
        .matches("built WITHOUT the `observability` cargo feature")
        .count()
}

/// AC1: the warning reaches STDERR on the real binary, exactly once, carrying
/// every operator-actionable substring.
#[test]
fn otel_enabled_warns_on_stderr_of_the_shipped_binary() {
    let out = run(Some("1"));
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert_eq!(
        warning_hits(&stderr),
        1,
        "expected EXACTLY ONE #1702 warning on stderr (zero = the silent no-op, \
         or the subscriber was installed after init); stderr:\n{stderr}"
    );
    for needle in REQUIRED_SUBSTRINGS {
        assert!(
            stderr.contains(needle),
            "stderr warning must contain {needle:?}; stderr:\n{stderr}"
        );
    }
}

/// Stdout hygiene (issue #129): the warning must NEVER appear on stdout, which
/// carries machine-readable output, and stdout must be byte-identical to a run
/// without the knob — the warning changes visibility, never data.
#[test]
fn warning_never_touches_stdout() {
    let with_otel = run(Some("1"));
    let without = run(None);

    let stdout = String::from_utf8_lossy(&with_otel.stdout);
    assert_eq!(
        warning_hits(&stdout),
        0,
        "the #1702 warning must not appear on stdout; stdout:\n{stdout}"
    );
    assert!(
        !stdout.contains("observability"),
        "no observability chatter may leak into stdout; stdout:\n{stdout}"
    );
    assert_eq!(
        with_otel.stdout, without.stdout,
        "stdout must be byte-identical with and without CQLITE_OTEL_ENABLED"
    );
}

/// The negatives: an unset knob, and an explicitly falsey one, must both stay
/// silent. Otherwise every default-build process start would print the warning.
#[test]
fn no_warning_when_otel_is_not_requested() {
    for case in [None, Some("0"), Some("false")] {
        let out = run(case);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(
            warning_hits(&stderr),
            0,
            "CQLITE_OTEL_ENABLED={case:?} must not warn; stderr:\n{stderr}"
        );
    }
}
