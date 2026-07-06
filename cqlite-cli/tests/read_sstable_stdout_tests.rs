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

use assert_cmd::Command;
use std::fs;
use std::path::PathBuf;

/// Decorative markers that must NEVER appear on stdout (they belong on stderr).
/// This is the exact chatter class the issue calls out (e.g. `📖 Reading …`,
/// `📊 SSTable Statistics`, `✅ Displayed …`, plus the legacy `🔍 / 🚀 / 📂`).
const DECORATIVE_MARKERS: &[&str] = &[
    "📖", "📊", "✅", "🔍", "🚀", "📂", "📋", "📄", "📦", "🎯", "💡", "🔄", "⚠", "❌",
];

/// Resolve the datasets root honoring `CQLITE_DATASETS_ROOT` (the CI/agent
/// convention), falling back to the in-repo `test-data/datasets/sstables`.
fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map(|p| p.join("test-data/datasets/sstables"))
                .unwrap_or_else(|| PathBuf::from("test-data/datasets/sstables"))
        })
}

/// Locate a `Data.db` for `test_basic/simple_table`, handling the UUID suffix
/// and the optional `sstables/` layer. Returns `None` (→ graceful skip) when
/// the binary fixtures are not present.
fn find_simple_table_data_file() -> Option<PathBuf> {
    let root = datasets_root();
    let keyspace_dir = {
        let with_sstables = root.join("sstables").join("test_basic");
        if with_sstables.exists() {
            with_sstables
        } else {
            root.join("test_basic")
        }
    };

    let entries = fs::read_dir(&keyspace_dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if name.to_string_lossy().starts_with("simple_table-") {
            // Prefer the canonical nb generation; fall back to any *-Data.db.
            let candidate = entry.path().join("nb-1-big-Data.db");
            if candidate.exists() {
                return Some(candidate);
            }
            if let Ok(files) = fs::read_dir(entry.path()) {
                for f in files.flatten() {
                    let fname = f.file_name();
                    if fname.to_string_lossy().ends_with("-Data.db") {
                        return Some(f.path());
                    }
                }
            }
        }
    }
    None
}

/// Run `read-sstable` with the given format/limit, returning (stdout, stderr).
fn run_read_sstable(data_file: &PathBuf, format: &str, limit: usize) -> (String, String) {
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
    let Some(data_file) = find_simple_table_data_file() else {
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
}

/// AC #1 (CSV variant): the CSV data payload carries no decorative markers.
#[test]
fn test_read_sstable_csv_stdout_has_no_decorative_markers() {
    let Some(data_file) = find_simple_table_data_file() else {
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
    let Some(data_file) = find_simple_table_data_file() else {
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
