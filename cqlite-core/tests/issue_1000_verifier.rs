//! Issue #1000 (epic #970): enforce the CQLite SSTable verifier contract.
//!
//! This is the disjoint enforcement test for the verifier defined in
//! `cqlite_core::storage::sstable::verify`. It asserts:
//!
//! 1. Every HEALTHY compressed fixture in `test_comp` AND the uncompressed
//!    fixture PASS FULL verification.
//! 2. Every CORRUPT fixture in `corruption/test_comp_corrupt` (manifest
//!    `status: active`) FAILS verification with a finding on the
//!    manifest-declared `expected_failing_component`. `status: planned` entries
//!    are skipped cleanly.
//! 3. No silent zero-row results on a corrupt index/BTI: the corrupt
//!    `Index.db` (BIG) and the corrupt `Partitions.db`/`Rows.db` (BTI) fixtures
//!    do NOT verify as "ok with 0 rows"; they hard-fail.
//!
//! Skip-clean when the dataset is absent (worktrees without fetched Data.db);
//! fail-loud when the dataset is present.

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{verify_sstable, VerifyMode, VerifyReport};
use cqlite_core::Config;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Manifest model (subset of corruption-manifest.yml)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct Manifest {
    fixtures: Vec<Fixture>,
}

#[derive(Debug, serde::Deserialize)]
struct Fixture {
    name: String,
    status: String,
    expected_failing_component: String,
    expected_error_class: String,
}

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
}

/// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value ("1"/"true").
/// In strict mode an unusable fixture set is a hard failure; otherwise it is a
/// clean skip. Mirrors the sibling parity tests' doctrine (issue #1094).
fn require_fixtures_strict() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Handle a fixture set that turned out to be unusable (e.g. directories are
/// git-tracked but the Data.db binaries were not fetched in this lane). Hard
/// failure under `CQLITE_REQUIRE_FIXTURES=1` (full-dataset CI / nightly), clean
/// skip otherwise — so PR lanes that set `CQLITE_DATASETS_ROOT` without shipping
/// the `test_comp` bundle do not fail spuriously (issue #1094).
fn skip_or_require(what: &str, reason: &str) {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {what} unavailable: {reason}");
    }
    eprintln!("[SKIP] {what}: {reason}");
}

/// `true` when a directory looks like a materialized SSTable generation (has a
/// `*-Data.db`). Used to skip-clean when binaries are not fetched.
fn has_data_db(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten().any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

async fn make_platform(config: &Config) -> Arc<Platform> {
    Arc::new(
        Platform::new(config)
            .await
            .expect("Platform::new must succeed"),
    )
}

async fn verify(dir: &Path, mode: VerifyMode) -> VerifyReport {
    let config = Config::default();
    let platform = make_platform(&config).await;
    verify_sstable(dir, mode, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("verify_sstable({}) returned Err: {e}", dir.display()))
}

// ---------------------------------------------------------------------------
// 1. Healthy fixtures PASS FULL verification
// ---------------------------------------------------------------------------

#[tokio::test]
async fn healthy_compressed_and_uncompressed_fixtures_pass_full_verification() {
    let Some(root) = datasets_root() else {
        skip_or_require(
            "issue_1000 verifier fixtures",
            "CQLITE_DATASETS_ROOT not set",
        );
        return;
    };
    let comp_dir = root.join("sstables/test_comp");
    if !comp_dir.exists() {
        skip_or_require(
            "test_comp healthy fixtures",
            "test_comp directory not present",
        );
        return;
    }

    let mut checked = 0usize;
    for entry in std::fs::read_dir(&comp_dir)
        .expect("read test_comp")
        .flatten()
    {
        let dir = entry.path();
        if !dir.is_dir() || !has_data_db(&dir) {
            continue;
        }
        let report = verify(&dir, VerifyMode::Full).await;
        assert!(
            report.is_ok(),
            "healthy fixture {} must PASS full verification, got findings: {:?}",
            dir.file_name().unwrap().to_string_lossy(),
            report.findings
        );
        // A healthy FULL verification must have actually scanned rows.
        assert!(
            report.rows_scanned.is_some(),
            "healthy fixture {} FULL verify must report a row count",
            dir.display()
        );
        checked += 1;
    }

    if checked == 0 {
        // test_comp dir is git-tracked (JSONL refs) but the Data.db binaries
        // were not fetched in this lane — skip clean unless strict mode demands
        // them. Avoids the per-PR failure reported in issue #1094.
        skip_or_require(
            "test_comp healthy fixtures",
            "no Data.db present (binaries not fetched)",
        );
        return;
    }
    assert!(
        checked >= 7,
        "expected at least 7 healthy test_comp fixtures with Data.db, found {checked}"
    );
}

// ---------------------------------------------------------------------------
// 2. Every active corrupt fixture FAILS with the manifest's expected component
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_corrupt_fixture_fails_on_expected_component() {
    let Some(root) = datasets_root() else {
        skip_or_require(
            "issue_1000 verifier fixtures",
            "CQLITE_DATASETS_ROOT not set",
        );
        return;
    };
    let corrupt_root = root.join("corruption/test_comp_corrupt");
    let manifest_path = corrupt_root.join("corruption-manifest.yml");
    if !manifest_path.exists() {
        skip_or_require(
            "test_comp_corrupt active fixtures",
            "corruption manifest not present",
        );
        return;
    }

    let manifest_raw = std::fs::read_to_string(&manifest_path).expect("read manifest");
    let manifest: Manifest = serde_yaml::from_str(&manifest_raw).expect("parse manifest yaml");

    let mut checked = 0usize;
    let mut skipped_planned = 0usize;
    for fx in &manifest.fixtures {
        if fx.status != "active" {
            eprintln!(
                "skipping non-active fixture {} (status={})",
                fx.name, fx.status
            );
            skipped_planned += 1;
            continue;
        }
        let dir = corrupt_root.join(&fx.name);
        if !dir.is_dir() || !has_data_db(&dir) {
            // BTI fixtures depend on a test_da source; if the corrupt dir was
            // not materialized, skip cleanly rather than fail.
            eprintln!(
                "corrupt fixture {} has no Data.db — skipping (clean skip)",
                fx.name
            );
            continue;
        }

        let report = verify(&dir, VerifyMode::Full).await;

        // Must FAIL.
        assert!(
            !report.is_ok(),
            "corrupt fixture {} ({}) MUST fail verification but passed",
            fx.name,
            fx.expected_error_class
        );

        // A finding must be attributable to the manifest's expected failing
        // component. Usually this is a finding whose `component` matches
        // exactly. The `toc_missing_component` fixture is special: the failing
        // component is `TOC.txt`, but the concrete symptom is the *dropped*
        // component (`Statistics.db`) missing from the TOC — so the finding's
        // detail references `TOC.txt`. Accept either: a finding ON the expected
        // component, OR a finding whose detail names it.
        let want = fx.expected_failing_component.trim();
        let attributable = report
            .findings
            .iter()
            .any(|f| f.component == want || f.detail.contains(want));
        assert!(
            attributable,
            "corrupt fixture {} expected a finding attributable to '{}', got: {:?}",
            fx.name,
            want,
            report
                .findings
                .iter()
                .map(|f| format!("[{}] {}: {}", f.class.code(), f.component, f.detail))
                .collect::<Vec<_>>()
        );

        // The error CLASS must match the manifest, not just the component, so a
        // regression that changes the verifier's classification is caught
        // (roborev). `expected_error_class` is "Code" or "Code/Subtype" (the
        // optional suffix is descriptive); accept a finding whose stable
        // class.code() equals ANY slash-separated segment.
        let want_classes: Vec<&str> = fx
            .expected_error_class
            .split('/')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        let class_matched = report
            .findings
            .iter()
            .any(|f| want_classes.contains(&f.class.code()));
        assert!(
            class_matched,
            "corrupt fixture {} expected error class '{}' (any of {:?}), got: {:?}",
            fx.name,
            fx.expected_error_class,
            want_classes,
            report
                .findings
                .iter()
                .map(|f| f.class.code())
                .collect::<Vec<_>>()
        );

        // Findings must carry locating context (non-empty detail).
        assert!(
            report.findings.iter().all(|f| !f.detail.trim().is_empty()),
            "corrupt fixture {} produced a finding with empty detail",
            fx.name
        );

        checked += 1;
    }

    if checked == 0 {
        // Manifest is git-tracked but the corrupt Data.db binaries were not
        // fetched in this lane — skip clean unless strict mode demands them
        // (issue #1094).
        skip_or_require(
            "test_comp_corrupt active fixtures",
            &format!(
                "no usable corrupt fixtures (skipped_planned={skipped_planned}); binaries not fetched"
            ),
        );
        return;
    }
    eprintln!("verified {checked} active corrupt fixtures ({skipped_planned} non-active skipped)");
}

// ---------------------------------------------------------------------------
// 3. No silent zero-row results on corrupt index / BTI components
// ---------------------------------------------------------------------------

#[tokio::test]
async fn corrupt_big_index_is_not_a_silent_zero_row_success() {
    let Some(root) = datasets_root() else {
        skip_or_require(
            "issue_1000 verifier fixtures",
            "CQLITE_DATASETS_ROOT not set",
        );
        return;
    };
    let dir = root.join("corruption/test_comp_corrupt/index_db_bit_flip_big");
    if !dir.is_dir() || !has_data_db(&dir) {
        skip_or_require(
            "corrupt fixture index_db_bit_flip_big",
            "not materialized (Data.db absent)",
        );
        return;
    }

    let report = verify(&dir, VerifyMode::Full).await;
    assert!(
        !report.is_ok(),
        "corrupt BIG Index.db must hard-fail, not pass: {:?}",
        report.findings
    );
    assert!(
        report.findings.iter().any(|f| f.component == "Index.db"),
        "corrupt Index.db must produce an Index.db finding (no silent fallback), got: {:?}",
        report.findings
    );
}

#[tokio::test]
async fn corrupt_bti_tries_are_not_a_silent_zero_row_success() {
    let Some(root) = datasets_root() else {
        skip_or_require(
            "issue_1000 verifier fixtures",
            "CQLITE_DATASETS_ROOT not set",
        );
        return;
    };
    let corrupt_root = root.join("corruption/test_comp_corrupt");

    for (fixture, component) in [
        ("bti_partitions_footer_flip", "Partitions.db"),
        ("bti_rows_truncation", "Rows.db"),
    ] {
        let dir = corrupt_root.join(fixture);
        if !dir.is_dir() || !has_data_db(&dir) {
            // Strict mode fails if this expected BTI fixture is absent;
            // otherwise skip just this one and continue.
            skip_or_require(
                &format!("corrupt BTI fixture {fixture}"),
                "not materialized (Data.db absent)",
            );
            continue;
        }

        let report = verify(&dir, VerifyMode::Full).await;
        assert!(
            !report.is_ok(),
            "corrupt BTI fixture {fixture} must hard-fail, not pass: {:?}",
            report.findings
        );
        assert!(
            report.findings.iter().any(|f| f.component == component),
            "corrupt BTI fixture {fixture} must produce a {component} finding, got: {:?}",
            report
                .findings
                .iter()
                .map(|f| format!("[{}] {}", f.class.code(), f.component))
                .collect::<Vec<_>>()
        );
    }
}

// ---------------------------------------------------------------------------
// 4. QUICK and FULL are distinct: QUICK skips the row scan.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn quick_mode_does_not_scan_rows_full_mode_does() {
    let Some(root) = datasets_root() else {
        skip_or_require(
            "issue_1000 verifier fixtures",
            "CQLITE_DATASETS_ROOT not set",
        );
        return;
    };
    // Pick a known-healthy compressed fixture.
    let comp_dir = root.join("sstables/test_comp");
    if !comp_dir.exists() {
        skip_or_require(
            "test_comp healthy fixtures",
            "test_comp directory not present",
        );
        return;
    }
    let Some(fixture) = std::fs::read_dir(&comp_dir)
        .expect("read test_comp")
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && has_data_db(p)
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("lz4_table"))
                    .unwrap_or(false)
        })
    else {
        skip_or_require(
            "healthy fixture lz4_table",
            "not materialized (Data.db absent)",
        );
        return;
    };

    let quick = verify(&fixture, VerifyMode::Quick).await;
    assert!(
        quick.is_ok(),
        "quick verify of healthy fixture must pass: {:?}",
        quick.findings
    );
    assert_eq!(
        quick.rows_scanned, None,
        "QUICK mode must NOT scan rows (it is metadata-only)"
    );

    let full = verify(&fixture, VerifyMode::Full).await;
    assert!(
        full.is_ok(),
        "full verify of healthy fixture must pass: {:?}",
        full.findings
    );
    assert!(
        full.rows_scanned.is_some(),
        "FULL mode must scan rows (distinct from QUICK)"
    );
}
