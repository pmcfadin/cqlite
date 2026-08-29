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
//!   -> cqlite-cli/src/telemetry.rs::init_telemetry         (the ORDERING fix, #1702)
//!        (bin-only: `telemetry` is declared in main.rs, not lib.rs)
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
/// set and the consequence. The missing cargo feature is NOT listed here: the
/// bare needle "observability" would be satisfied by the fmt layer printing the
/// event TARGET (`cqlite_core::observability:`) even if the message never named
/// the feature, so it proves nothing — [`warning_hits`] anchors on the full
/// "built WITHOUT the `observability` cargo feature" phrase instead, which does.
const REQUIRED_SUBSTRINGS: &[&str] = &["CQLITE_OTEL_ENABLED", "will be emitted", "NOT a collector"];

/// A stable, cheap invocation that reaches `run_main` (so telemetry init runs)
/// and writes deterministic bytes to stdout. `--version`/`--help` are NOT usable:
/// clap handles them during `parse()` and exits before any init happens.
const ARGS: &[&str] = &["info"];

/// Run the shipped binary in a throwaway cwd (`info` creates `cqlite.db`
/// relative to cwd, so this keeps the repo clean and each case isolated).
///
/// The child environment is built from EMPTY (`env_clear`) rather than filtered
/// (roborev r2). Clearing only `RUST_LOG` + `CQLITE_OTEL_*` left two concrete
/// machine-dependent failure modes: the child inherited `HOME` /
/// `XDG_CONFIG_HOME`, so a developer's or fleet box's CQLite config file that
/// enables telemetry made the "no warning when not requested" case fail; and it
/// inherited the other env-bound CLI flags (`CQLITE_SCHEMA`, `CQLITE_DATA_DIR`,
/// `CQLITE_OUT`, `CQLITE_WRITABLE`, … — every `env = "CQLITE_*"` in
/// `cli_types.rs`), any of which can alter or break the `info` invocation. An
/// allowlist has to be maintained against that growing list; starting from empty
/// cannot drift. `HOME`/`XDG_CONFIG_HOME` are pointed at a fresh temp dir so
/// config discovery finds nothing, and only `PATH` (+ `TMPDIR` when set) is
/// carried over.
fn run(otel_enabled: Option<&str>) -> Output {
    run_with(otel_enabled, None, &[])
}

/// As [`run`], but with an explicit `RUST_LOG` value and/or extra CLI flags, so
/// the log-filtering cases below can drive `-q` and `RUST_LOG=error`.
fn run_with(otel_enabled: Option<&str>, rust_log: Option<&str>, extra_args: &[&str]) -> Output {
    let cwd = TempDir::new().expect("tempdir for cwd");
    // Held until after `output()` so the child's HOME still exists while it runs.
    let home = TempDir::new().expect("tempdir for HOME");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cqlite"));
    cmd.args(extra_args)
        .args(ARGS)
        .current_dir(cwd.path())
        .env_clear()
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", home.path());
    // The few inherited variables the child genuinely needs. `env_clear` also
    // drops the dynamic-loader search paths, so carry those: today's
    // default-feature `cqlite` links no dylib that needs them, but a future
    // default-on feature that does (the `duckdb-tests` amalgamation is the shape
    // to watch) would fail every case here with a loader error that reads as a
    // CLI bug. None of these can influence CQLite config resolution, so carrying
    // them does not reintroduce the ambient-config drift `env_clear` fixed.
    for var in [
        "PATH",
        "TMPDIR",
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
    ] {
        if let Ok(v) = std::env::var(var) {
            cmd.env(var, v);
        }
    }
    if let Some(v) = otel_enabled {
        cmd.env("CQLITE_OTEL_ENABLED", v);
    }
    if let Some(v) = rust_log {
        cmd.env("RUST_LOG", v);
    }
    let out = cmd.output().expect("cqlite binary runs");
    assert!(
        out.status.success(),
        "`cqlite {} {}` must succeed; stderr:\n{}",
        extra_args.join(" "),
        ARGS.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    // Positive control for EVERY case in this file, including the ones whose only
    // assertion is an absence (stdout hygiene, `-q`/`RUST_LOG=error` suppression):
    // `cqlite info` prints its report on stdout, so empty stdout means the command
    // did not actually do its work and an "absence" assertion would pass vacuously.
    assert!(
        !out.stdout.is_empty(),
        "`cqlite {} {}` produced no stdout, so this run proves nothing about what \
         it did or did not emit; stderr:\n{}",
        extra_args.join(" "),
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

/// The warning is an ORDINARY `WARN` event, so it respects the operator's
/// explicit filtering choice — it does NOT bypass the subscriber.
///
/// This pins that as a deliberate, documented property rather than leaving it an
/// accident: `--quiet` maps to the `error` level (`main.rs`'s `-v`/`-q` mapping)
/// and `RUST_LOG=error` sets the same floor through `EnvFilter`, so both hide a
/// WARN. Making the warning unfilterable would mean writing to stderr from
/// library code behind the subscriber's back, which is worse than respecting
/// `-q`. The default level (no `RUST_LOG`, no `-q`) is asserted here too, so a
/// future filter change that hid the warning by default cannot pass as "just
/// filtering".
#[test]
fn warning_respects_the_operator_log_level() {
    // Baseline: default level shows it. (Same property as the stderr test above,
    // re-asserted here so the three cases are comparable in one place.)
    let default_level = run_with(Some("1"), None, &[]);
    assert_eq!(
        warning_hits(&String::from_utf8_lossy(&default_level.stderr)),
        1,
        "the default log level must show the warning"
    );

    for (label, rust_log, extra) in [
        ("--quiet", None, &["--quiet"][..]),
        ("RUST_LOG=error", Some("error"), &[][..]),
    ] {
        let out = run_with(Some("1"), rust_log, extra);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(
            warning_hits(&stderr),
            0,
            "{label} filters WARN out, so the #1702 warning is suppressed BY \
             DESIGN — the operator asked for errors only. If this case starts \
             failing, something made the warning bypass the subscriber; that is \
             a regression, not a fix. stderr:\n{stderr}"
        );
    }
}
