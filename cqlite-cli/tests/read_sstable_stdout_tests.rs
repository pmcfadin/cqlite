//! Issue #1483 (AB8): `read-sstable` must keep decorative/progress chatter OFF
//! stdout so that piped/redirected output is machine-consumable.
//!
//! Acceptance criteria locked here:
//!   1. `read-sstable --format json` writes ONLY valid JSON to stdout — a
//!      consumer piping into `jq`/`serde_json` sees no emoji/status lines.
//!   2. Decorative messages still appear (on stderr) for interactive use.
//!
//! These are true regression guards: reverting any `eprintln!` back to a
//! `println!` in `cqlite-cli/src/commands/read_sstable.rs` makes
//! `test_read_sstable_json_stdout_is_clean_json` FAIL (verified during #1483).
//!
//! Tests degrade to a graceful skip (not a false failure) when the binary
//! Data.db fixtures are absent — see the project test-data guidance about
//! 0-row false passes in checkouts without `test-data/datasets` binaries.
//!
//! Gated on `state_machine` (the `read-sstable` subcommand only exists with it):
//! without the gate, `--no-default-features` would build a binary lacking the
//! subcommand and fail loudly instead of skipping.

#![cfg(feature = "state_machine")]

mod common;

use assert_cmd::Command;
use common::find_simple_table_data_db;
use std::path::Path;

/// Decorative markers that must NEVER appear on stdout (they belong on stderr).
/// This is the exact chatter class the issue calls out (e.g. `📖 Reading …`,
/// `📊 SSTable Statistics`, `✅ Displayed …`, plus the legacy `🔍 / 🚀 / 📂`).
const DECORATIVE_MARKERS: &[&str] = &[
    "📖", "📊", "✅", "🔍", "🚀", "📂", "📋", "📄", "📦", "🎯", "💡", "🔄", "⚠", "❌",
];

/// Run `read-sstable` with the given format/limit, returning (stdout, stderr).
fn run_read_sstable(data_file: &Path, format: &str, limit: usize) -> (String, String) {
    let output = Command::cargo_bin("cqlite")
        .expect("cqlite binary should be built for integration tests")
        .arg("read-sstable")
        .arg(data_file)
        .arg("--format")
        .arg(format)
        .arg("--limit")
        .arg(limit.to_string())
        .output()
        .expect("read-sstable command should execute");

    assert!(
        output.status.success(),
        "read-sstable exited non-zero: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

fn assert_no_decorative_markers(stream_name: &str, contents: &str) {
    for marker in DECORATIVE_MARKERS {
        assert!(
            !contents.contains(marker),
            "decorative marker {marker:?} leaked onto {stream_name}; \
             piped output must be data-only (issue #1483). Full {stream_name}:\n{contents}",
        );
    }
}

/// AC #1: piping `read-sstable --format json` yields valid JSON with zero
/// decorative lines on stdout.
#[test]
fn test_read_sstable_json_stdout_is_clean_json() {
    let Some(data_file) = find_simple_table_data_db() else {
        eprintln!("SKIP: test_basic/simple_table Data.db fixture not present");
        return;
    };

    let (stdout, _stderr) = run_read_sstable(&data_file, "json", 3);

    // No decorative chatter interleaved with the data.
    assert_no_decorative_markers("stdout", &stdout);

    // Stdout parses as a single JSON document — a piping consumer succeeds.
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("stdout must be valid JSON (issue #1483): {e}\nstdout:\n{stdout}")
    });
    assert!(
        parsed.is_array(),
        "read-sstable --format json should emit a JSON array, got: {stdout}"
    );
    // Guard against the project's "0-rows-when-present is a false pass" hazard.
    // We only reach here when the fixture IS present (the skip path returns early
    // above), so a present-but-empty result must not silently pass. Today a
    // zero-row read emits no stdout (the command returns early), which the
    // `from_str` parse above already rejects; this `!is_empty()` assertion also
    // fails closed on a hypothetical future `[]` emission — consistent with the
    // CSV test's non-empty payload check.
    let entries = parsed
        .as_array()
        .expect("parsed JSON is an array (asserted above)");
    assert!(
        !entries.is_empty(),
        "present simple_table fixture must yield ≥1 JSON entry, not an empty \
         array (0-rows-when-present is a false pass, issue #1483); stdout:\n{stdout}"
    );
}

/// AC #1 (CSV variant): the CSV data payload carries no decorative markers.
#[test]
fn test_read_sstable_csv_stdout_has_no_decorative_markers() {
    let Some(data_file) = find_simple_table_data_db() else {
        eprintln!("SKIP: test_basic/simple_table Data.db fixture not present");
        return;
    };

    let (stdout, _stderr) = run_read_sstable(&data_file, "csv", 3);
    assert_no_decorative_markers("stdout", &stdout);
    assert!(
        !stdout.trim().is_empty(),
        "CSV stdout should contain the data payload"
    );
}

/// AC #2: decorative messages are not silently dropped — they are routed to
/// stderr, where they remain available for interactive use.
#[test]
fn test_read_sstable_decorative_output_goes_to_stderr() {
    let Some(data_file) = find_simple_table_data_db() else {
        eprintln!("SKIP: test_basic/simple_table Data.db fixture not present");
        return;
    };

    let (stdout, stderr) = run_read_sstable(&data_file, "json", 3);

    // The "Reading SSTable" banner is decorative → belongs on stderr, not stdout.
    assert!(
        stderr.contains("Reading SSTable"),
        "expected the decorative 'Reading SSTable' banner on stderr; stderr:\n{stderr}"
    );
    assert!(
        !stdout.contains("Reading SSTable"),
        "the 'Reading SSTable' banner must not appear on stdout; stdout:\n{stdout}"
    );
}
