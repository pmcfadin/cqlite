//! Issue #1626: a *present but unparseable* Statistics.db must HARD-FAIL
//! `SSTableReader::open` rather than being silently swallowed into zero
//! EncodingStats baselines + no SerializationHeader columns (which would make
//! every WRITETIME()/TTL/deletion-time from that SSTable silently wrong).
//!
//! This is the "default-on-parse-failure" anti-pattern the no-heuristics
//! mandate (issue #28) forbids. A *missing* Statistics.db is out of scope and
//! keeps its current (open-succeeds) behavior — this test only covers the
//! corrupt/truncated case.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Platform};

/// Resolve the datasets root: prefer `CQLITE_DATASETS_ROOT`, else fall back to
/// the in-repo `test-data/datasets` directory (relative to this crate).
fn datasets_root() -> PathBuf {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        return PathBuf::from(root);
    }
    // cqlite-core/tests/.. -> cqlite-core -> workspace root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("test-data")
        .join("datasets")
}

/// Recursively copy every file in `src` dir (flat SSTable dir) into `dst`.
fn copy_sstable_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_name() {
                std::fs::copy(&path, dst.join(name))?;
            }
        }
    }
    Ok(())
}

fn find_file_suffix(dir: &Path, suffix: &str) -> Option<PathBuf> {
    std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with(suffix))
                    .unwrap_or(false)
        })
}

/// `CQLITE_REQUIRE_FIXTURES=1` (the strict CI lanes) makes a missing fixture a
/// HARD failure so a renamed/dropped fixture can never let this hard-fail guard
/// false-pass. Without it, the test skips cleanly (fresh checkout, binaries
/// absent). Mirrors the repo-wide convention (see `issue_1007_complex_type_parity`).
fn require_fixtures_strict() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false)
}

/// Skip when the fixture is absent — unless strict mode is on, in which case
/// fail loud so the scenario cannot false-pass as exercised. Returns `true` when
/// the caller should skip.
fn skip_or_fail(reason: &str) -> bool {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but #1626 fixture unavailable: {reason}");
    }
    eprintln!("SKIP: {reason}");
    true
}

async fn platform() -> (Config, Arc<Platform>) {
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );
    (config, platform)
}

const FIXTURE_REL: &str = "sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9";

/// A present-but-truncated Statistics.db must make `open()` return `Err` whose
/// message names the component file ("Statistics.db"). On current `main` this
/// returns `Ok` (the parse error is swallowed into `None`) so this test FAILS
/// before the fix.
#[tokio::test]
async fn corrupt_statistics_hard_fails_open() {
    let fixture = datasets_root().join(FIXTURE_REL);
    match find_file_suffix(&fixture, "-Data.db") {
        Some(p) if p.exists() => {}
        _ => {
            if skip_or_fail(&format!(
                "fixture Data.db not present under {} (dataset not fetched)",
                fixture.display()
            )) {
                return;
            }
        }
    };
    // Guard: the fixture ships a Statistics.db to corrupt.
    if find_file_suffix(&fixture, "-Statistics.db").is_none()
        && skip_or_fail(&format!(
            "fixture has no Statistics.db under {}",
            fixture.display()
        ))
    {
        return;
    }

    let tmp = tempfile::tempdir().expect("create tempdir");
    let dst = tmp.path().join("sstable");
    copy_sstable_dir(&fixture, &dst).expect("copy sstable dir");

    // Truncate the copied Statistics.db so the file is present but unparseable.
    //
    // NOTE (empirical, see #1626 report): truncating to len/2 does NOT make the
    // minimal nb parser fail for this fixture — the SerializationHeader lives at
    // a TOC offset near the END of the file, and `parse_minimal_encoding_stats`
    // silently FALLS BACK to reading EncodingStats from the front when that
    // offset is past EOF, so a back-truncated file still parses Ok. The only
    // robust "present-but-unparseable" corruption is to truncate below the
    // 32-byte fixed header so `parse_nb_format_header` itself fails. We truncate
    // to `min(len/2, 16)` bytes (< 32, still > 0).
    let stats = find_file_suffix(&dst, "-Statistics.db").expect("copied Statistics.db");
    let len = std::fs::metadata(&stats).expect("stat Statistics.db").len();
    let target = (len / 2).clamp(1, 16);
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&stats)
            .expect("open Statistics.db for truncation");
        f.set_len(target).expect("truncate Statistics.db");
    }

    let copied_data = find_file_suffix(&dst, "-Data.db").expect("copied Data.db");
    let (config, platform) = platform().await;

    let result = SSTableReader::open(&copied_data, &config, platform).await;

    let err = match result {
        Ok(_) => panic!(
            "expected SSTableReader::open to HARD-FAIL on a truncated Statistics.db, \
             but it returned Ok (parse error was swallowed)"
        ),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("Statistics.db"),
        "error message must name the component file 'Statistics.db', got: {msg}"
    );
}

/// Acceptance guard: opening the UNMODIFIED copied SSTable still returns `Ok`.
/// Proves the hard-fail is scoped strictly to corrupt Statistics.db and does
/// not regress healthy files.
#[tokio::test]
async fn healthy_statistics_still_opens_ok() {
    let fixture = datasets_root().join(FIXTURE_REL);
    match find_file_suffix(&fixture, "-Data.db") {
        Some(p) if p.exists() => {}
        _ => {
            if skip_or_fail(&format!(
                "fixture Data.db not present under {} (dataset not fetched)",
                fixture.display()
            )) {
                return;
            }
        }
    };

    let tmp = tempfile::tempdir().expect("create tempdir");
    let dst = tmp.path().join("sstable");
    copy_sstable_dir(&fixture, &dst).expect("copy sstable dir");

    let copied_data = find_file_suffix(&dst, "-Data.db").expect("copied Data.db");
    let (config, platform) = platform().await;

    let result = SSTableReader::open(&copied_data, &config, platform).await;
    assert!(
        result.is_ok(),
        "healthy copied SSTable must still open Ok, got: {:?}",
        result.err()
    );
}
