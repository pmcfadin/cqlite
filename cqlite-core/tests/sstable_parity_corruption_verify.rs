//! Issue #1236 (verify-parity): fail-closed Cassandra parity for the SSTable
//! verifier over the Cassandra-5.0.2-written corrupted-component corpus.
//!
//! This is the parity layer on top of the shipped verify surface
//! (`cqlite_core::storage::sstable::verify::verify_sstable`, the same call path
//! behind `cqlite verify --mode full`). For every corrupted fixture in the
//! `corruption/test_comp_corrupt` corpus — whose clean source was written by
//! Apache Cassandra 5.0.2 — it asserts:
//!
//! 1. **Class match.** The returned `VerifyReport.findings` contain the
//!    fixture's manifest-declared `expected_error_class`.
//! 2. **Verdict match.** CQLite's overall corrupt/clean verdict
//!    (`!report.is_ok()` ⇒ corrupt) agrees with the actual Cassandra 5.0.2
//!    `sstableverify --extended` verdict captured into the manifest
//!    (`cassandra_verdict`).
//! 3. **Clean baseline.** The uncorrupted Cassandra-written `lz4_table`
//!    generation verifies clean (zero findings), agreeing with Cassandra.
//!
//! Fixture-gating follows repo doctrine (issue #1094): skip-clean when the
//! corpus binaries are absent; FAIL when present-but-wrong; treat
//! zero-fixtures-evaluated-when-present as a failure. `CQLITE_REQUIRE_FIXTURES=1`
//! turns the skip into a hard failure (full-dataset CI / nightly).
//!
//! Out of scope (no CQLite VerifyErrorClass today, no parity claimed): out-of-order
//! key/row, negative/overflowed local-deletion-time, and scrub/recovery. See the
//! manifest scenario `cass.corruption_verify.component_corruption_detection`.

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{verify_sstable, VerifyMode, VerifyReport};
use cqlite_core::Config;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Manifest model (subset of corruption-manifest.yml, extended with the captured
// Cassandra verdict — issue #1236)
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
    /// Captured Apache Cassandra 5.0.2 `sstableverify --extended` verdict for
    /// this fixture's exact bytes: `clean` or `corrupt`. This is the parity
    /// oracle — never hand-encoded from reading Cassandra source.
    cassandra_verdict: String,
    /// `equivalent` when CQLite's corrupt/clean verdict matches Cassandra's;
    /// `divergent` when CQLite is intentionally STRICTER than Cassandra on this
    /// mutation (the verdict_note records why). The test asserts verdict
    /// equivalence for `equivalent` fixtures and asserts the recorded divergence
    /// for `divergent` ones, so a regression in either direction is caught.
    verdict_parity: String,
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

/// Handle a fixture set that turned out to be unusable (directories git-tracked
/// but the `*.db` binaries not regenerated/fetched in this lane). Hard failure
/// under `CQLITE_REQUIRE_FIXTURES=1`, clean skip otherwise.
fn skip_or_require(what: &str, reason: &str) {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {what} unavailable: {reason}");
    }
    eprintln!("[SKIP] {what}: {reason}");
}

/// `true` when a directory holds a materialized SSTable generation (has a
/// `*-Data.db`). Used to skip-clean when binaries are not regenerated.
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

/// Drive the PUBLIC verify surface (`cqlite verify --mode full` call path).
async fn verify_full(dir: &Path) -> VerifyReport {
    let config = Config::default();
    let platform = make_platform(&config).await;
    verify_sstable(dir, VerifyMode::Full, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("verify_sstable({}) returned Err: {e}", dir.display()))
}

/// Normalize a captured Cassandra verdict to a boolean "is corrupt".
/// Accepts `corrupt`/`corrupted` and `clean`/`ok` (case-insensitive); any other
/// value is a manifest authoring error and fails the test loudly.
fn cassandra_says_corrupt(fixture: &str, verdict: &str) -> bool {
    match verdict.trim().to_ascii_lowercase().as_str() {
        "corrupt" | "corrupted" => true,
        "clean" | "ok" => false,
        other => panic!(
            "fixture {fixture}: unrecognized cassandra_verdict '{other}' \
             (expected 'clean' or 'corrupt')"
        ),
    }
}

// ---------------------------------------------------------------------------
// Parity: per-fixture class match + verdict match vs the captured Cassandra
// verdict, over the public verify_sstable surface (fail-closed).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sstable_parity_corruption_verify_matches_cassandra_per_fixture() {
    let Some(root) = datasets_root() else {
        skip_or_require(
            "issue_1236 corruption-verify parity",
            "CQLITE_DATASETS_ROOT not set",
        );
        return;
    };
    let corrupt_root = root.join("corruption/test_comp_corrupt");
    let manifest_path = corrupt_root.join("corruption-manifest.yml");
    if !manifest_path.exists() {
        skip_or_require(
            "test_comp_corrupt corruption manifest",
            "corruption manifest not present",
        );
        return;
    }

    let manifest_raw = std::fs::read_to_string(&manifest_path).expect("read corruption manifest");
    let manifest: Manifest = serde_yaml::from_str(&manifest_raw).expect("parse manifest yaml");

    let mut checked = 0usize;
    let mut skipped_planned = 0usize;
    let mut skipped_absent = 0usize;

    for fx in &manifest.fixtures {
        if fx.status != "active" {
            skipped_planned += 1;
            continue;
        }
        let dir = corrupt_root.join(&fx.name);
        if !dir.is_dir() || !has_data_db(&dir) {
            // An active fixture must be materialized. In strict mode
            // (CQLITE_REQUIRE_FIXTURES=1) a partially-generated corpus must NOT
            // false-pass on the strength of the other fixtures — fail closed
            // immediately so every active fixture's parity coverage is enforced
            // (roborev #1236 Finding 1). Non-strict: clean per-fixture skip.
            if require_fixtures_strict() {
                panic!(
                    "CQLITE_REQUIRE_FIXTURES=1 but active fixture '{}' is absent: \
                     missing dir or Data.db at {} (binaries not regenerated)",
                    fx.name,
                    dir.display()
                );
            }
            // BTI fixtures depend on a test_da source; if the corrupt dir was
            // not materialized, skip it (clean skip in non-strict mode).
            eprintln!(
                "[skip-fixture] {} has no Data.db (binaries not regenerated)",
                fx.name
            );
            skipped_absent += 1;
            continue;
        }

        let report = verify_full(&dir).await;
        let cqlite_corrupt = !report.is_ok();

        // (1) Class match: a finding must carry the fixture's expected class.
        // `expected_error_class` is "Code" or "Code/Subtype"; accept a finding
        // whose stable class.code() equals ANY slash-separated segment.
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
            "fixture {} expected verify error class '{}' (any of {:?}), got: {:?}",
            fx.name,
            fx.expected_error_class,
            want_classes,
            report
                .findings
                .iter()
                .map(|f| f.class.code())
                .collect::<Vec<_>>()
        );

        // The finding must also be attributable to the declared failing
        // component (a finding ON it, or one whose detail names it — the
        // toc_missing_component fixture names the dropped Statistics.db in
        // detail while failing TOC.txt).
        let want_component = fx.expected_failing_component.trim();
        let attributable = report
            .findings
            .iter()
            .any(|f| f.component == want_component || f.detail.contains(want_component));
        assert!(
            attributable,
            "fixture {} expected a finding attributable to component '{}', got: {:?}",
            fx.name,
            want_component,
            report
                .findings
                .iter()
                .map(|f| format!("[{}] {}: {}", f.class.code(), f.component, f.detail))
                .collect::<Vec<_>>()
        );

        // (2) Verdict match: CQLite corrupt/clean vs Cassandra's captured
        // verdict on the SAME bytes, gated by the recorded verdict_parity.
        let cass_corrupt = cassandra_says_corrupt(&fx.name, &fx.cassandra_verdict);
        match fx.verdict_parity.trim().to_ascii_lowercase().as_str() {
            "equivalent" => {
                assert_eq!(
                    cqlite_corrupt,
                    cass_corrupt,
                    "fixture {} (verdict_parity=equivalent): CQLite verdict (corrupt={}) \
                     disagrees with captured Cassandra 5.0.2 verdict '{}' (corrupt={}). \
                     CQLite findings: {:?}",
                    fx.name,
                    cqlite_corrupt,
                    fx.cassandra_verdict,
                    cass_corrupt,
                    report
                        .findings
                        .iter()
                        .map(|f| f.to_string())
                        .collect::<Vec<_>>()
                );
            }
            "divergent" => {
                // CQLite is intentionally stricter here. Assert the EXACT recorded
                // divergence so a behavior change (Cassandra now agrees, or CQLite
                // stops flagging) is caught rather than silently passing. Today:
                // Cassandra=clean, CQLite=corrupt (e.g. toc_missing_component —
                // Cassandra's standalone verifier rebuilds the TOC from on-disk
                // components; CQLite treats TOC.txt as authoritative).
                assert!(
                    !cass_corrupt,
                    "fixture {} is recorded verdict_parity=divergent but its captured \
                     Cassandra verdict is 'corrupt'; a divergence implies Cassandra=clean. \
                     Update the manifest (likely now equivalent).",
                    fx.name
                );
                assert!(
                    cqlite_corrupt,
                    "fixture {} (verdict_parity=divergent) expects CQLite to be STRICTER \
                     (flag corrupt) where Cassandra verifies clean, but CQLite returned \
                     clean. The divergence no longer holds — re-capture and update the \
                     manifest. CQLite findings: {:?}",
                    fx.name,
                    report
                        .findings
                        .iter()
                        .map(|f| f.to_string())
                        .collect::<Vec<_>>()
                );
            }
            other => panic!(
                "fixture {}: unrecognized verdict_parity '{}' (expected 'equivalent' or \
                 'divergent')",
                fx.name, other
            ),
        }

        checked += 1;
    }

    if checked == 0 {
        // Manifest is git-tracked but the corrupt Data.db binaries were not
        // regenerated/fetched in this lane — skip clean unless strict mode
        // demands them (issue #1094). Zero-evaluated-when-present is impossible
        // here: if any active fixture had a Data.db we would have checked it.
        skip_or_require(
            "test_comp_corrupt active fixtures",
            &format!(
                "no usable corrupt fixtures (planned={skipped_planned}, absent={skipped_absent}); \
                 binaries not regenerated"
            ),
        );
        return;
    }
    eprintln!(
        "sstable_parity_corruption_verify: checked {checked} active fixtures \
         ({skipped_planned} planned, {skipped_absent} absent skipped)"
    );
}

// ---------------------------------------------------------------------------
// Clean baseline: the uncorrupted Cassandra-5.0.2-written lz4_table verifies
// clean (zero findings), agreeing with Cassandra's clean verdict.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sstable_parity_corruption_verify_clean_baseline_is_clean() {
    let Some(root) = datasets_root() else {
        skip_or_require("issue_1236 clean baseline", "CQLITE_DATASETS_ROOT not set");
        return;
    };
    let comp_dir = root.join("sstables/test_comp");
    if !comp_dir.exists() {
        skip_or_require(
            "test_comp clean baseline",
            "test_comp directory not present",
        );
        return;
    }

    // The clean parity oracle is the same Cassandra-written lz4_table source the
    // corrupted fixtures are derived from; Cassandra verifies it clean.
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
            "clean baseline lz4_table",
            "not materialized (Data.db absent)",
        );
        return;
    };

    let report = verify_full(&fixture).await;
    assert!(
        report.is_ok(),
        "clean Cassandra-written lz4_table must verify clean (zero findings), got: {:?}",
        report.findings
    );
    assert!(
        report.rows_scanned.is_some(),
        "FULL verify of the clean baseline must report a scanned row count"
    );
}

// ---------------------------------------------------------------------------
// Clean BTI baseline (Finding 2 / issue #1236): when the BTI corrupt fixtures
// (BtiRootPointerCorrupt / BtiTrieCorrupt) are active in the manifest, the
// uncorrupted Cassandra-5.0.2-written `test_da/wide_table` (`da` BTI) generation
// — the clean source those fixtures are mutated from — MUST verify clean (zero
// findings).
//
// Without this, a CQLite false-positive that flags EVERY clean BTI table as
// corrupt would go undetected: the corrupt-fixture assertions expect corrupt, so
// they would still pass. The clean BTI baseline catches that whole class of
// BTI-wide false positives. Fixture-gated identically to the corpus test:
// skip-clean when the clean source is absent, FAIL when present-but-wrong, honor
// CQLITE_REQUIRE_FIXTURES.
// ---------------------------------------------------------------------------

/// `true` when a fixture's failing component is a BTI (`da`) trie component, i.e.
/// it is derived from the clean BTI `test_da/wide_table` source.
fn is_bti_component(component: &str) -> bool {
    let c = component.trim();
    c.ends_with("Partitions.db") || c.ends_with("Rows.db")
}

#[tokio::test]
async fn sstable_parity_corruption_verify_clean_bti_baseline_is_clean() {
    let Some(root) = datasets_root() else {
        skip_or_require(
            "issue_1236 clean BTI baseline",
            "CQLITE_DATASETS_ROOT not set",
        );
        return;
    };

    // Only assert the clean BTI baseline when the BTI corrupt fixtures are ACTIVE
    // in the manifest (otherwise there is no BTI parity to anchor). The clean BTI
    // source is the same dataset the BTI fixtures are mutated from.
    let corrupt_root = root.join("corruption/test_comp_corrupt");
    let manifest_path = corrupt_root.join("corruption-manifest.yml");
    if !manifest_path.exists() {
        skip_or_require(
            "test_comp_corrupt corruption manifest",
            "corruption manifest not present",
        );
        return;
    }
    let manifest_raw = std::fs::read_to_string(&manifest_path).expect("read corruption manifest");
    let manifest: Manifest = serde_yaml::from_str(&manifest_raw).expect("parse manifest yaml");

    let bti_active = manifest
        .fixtures
        .iter()
        .any(|fx| fx.status == "active" && is_bti_component(&fx.expected_failing_component));
    if !bti_active {
        // No active BTI corrupt fixtures → no BTI parity to anchor a clean
        // baseline against. Skip-clean (strict mode still demands the corpus via
        // the corpus test, which would fail there if BTI fixtures were expected).
        skip_or_require(
            "issue_1236 clean BTI baseline",
            "no active BTI corrupt fixtures in manifest (nothing to anchor)",
        );
        return;
    }

    let da_dir = root.join("sstables/test_da");
    if !da_dir.exists() {
        skip_or_require(
            "test_da clean BTI baseline",
            "test_da directory not present",
        );
        return;
    }

    // Resolve the clean BTI source the same way the generator does: the
    // `test_da/wide_table-*` generation (a materialized `*-Data.db`).
    let Some(fixture) = std::fs::read_dir(&da_dir)
        .expect("read test_da")
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && has_data_db(p)
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("wide_table"))
                    .unwrap_or(false)
        })
    else {
        skip_or_require(
            "clean BTI baseline wide_table",
            "not materialized (Data.db absent)",
        );
        return;
    };

    let report = verify_full(&fixture).await;
    assert!(
        report.is_ok(),
        "clean Cassandra-written BTI wide_table must verify clean (zero findings) — a \
         non-empty finding set here means CQLite false-positives on clean BTI tables, which \
         would mask the BtiRootPointerCorrupt/BtiTrieCorrupt parity. Got: {:?}",
        report.findings
    );
    assert!(
        report.rows_scanned.is_some(),
        "FULL verify of the clean BTI baseline must report a scanned row count"
    );
}
