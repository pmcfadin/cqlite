//! Issue #4626 phase 6 (GD11): `verify` resolves components and Statistics through the generation
//! module and keeps every verdict.
//!
//! GD11.1: the oracle `test-data/verify-verdicts-4626.json` was captured on `origin/main` BEFORE
//! the migration (set `CQLITE_4626_DUMP=<path>` to write a fresh capture). Each case is one
//! directory of the `sstable-verify-parity` corrupted-component corpus or one clean Cassandra-written
//! table, in one mode; its value is `ok` plus the sorted `(class code, component)` pairs of the
//! findings. A case whose fixture binaries are absent is skipped unless
//! `CQLITE_REQUIRE_FIXTURES=1`, which fails it (#3220: per-case, never a suite-wide count).
//!
//! GD11.2: a below-floor `ma-1-big-Data.db` is refused with `UnsupportedVersion` and no component
//! is opened.

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{
    verify_sstable, verify_sstable_audit, verify_sstable_generation, VerifyMode,
};
use cqlite_core::storage::sstable::verify_audit::AuditVerdict;
use cqlite_core::{Config, Error};
use std::collections::BTreeMap;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const ORACLE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../test-data/verify-verdicts-4626.json"
);

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
}

fn strict() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn has_data_db(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        })
        .unwrap_or(false)
}

/// Every case directory (relative to the datasets root): the active corrupted fixtures and the
/// clean Cassandra-written `test_comp` and `test_da` tables.
fn case_dirs(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let corrupt = root.join("corruption/test_comp_corrupt");
    for sub in sorted_subdirs(&corrupt) {
        out.push(format!("corruption/test_comp_corrupt/{sub}"));
    }
    for ks in ["test_comp", "test_da"] {
        for sub in sorted_subdirs(&root.join("sstables").join(ks)) {
            out.push(format!("sstables/{ks}/{sub}"));
        }
    }
    out
}

fn sorted_subdirs(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.path().is_dir())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Verdict {
    ok: bool,
    findings: Vec<(String, String)>,
    /// Audit modes only: `(invariant id, "Pass" | "Fail" | "Skip: <reason>")` per row, in order.
    #[serde(default)]
    audit: Vec<(String, String)>,
}

async fn run_case(dir: &Path, mode: &str) -> Result<Verdict, String> {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.map_err(|e| e.to_string())?);
    let report = match mode {
        "quick" => verify_sstable(dir, VerifyMode::Quick, &config, platform).await,
        "full" => verify_sstable(dir, VerifyMode::Full, &config, platform).await,
        "audit-deep" => verify_sstable_audit(dir, true, &config, platform).await,
        other => panic!("unknown mode {other}"),
    }
    .map_err(|e| format!("Err: {e}"))?;
    let mut findings: Vec<(String, String)> = report
        .findings
        .iter()
        .map(|f| (f.class.code().to_string(), f.component.clone()))
        .collect();
    findings.sort();
    let audit = report
        .audit
        .as_ref()
        .map(|a| {
            a.rows
                .iter()
                .map(|r| {
                    let v = match &r.verdict {
                        AuditVerdict::Pass => "Pass".to_string(),
                        AuditVerdict::Fail => "Fail".to_string(),
                        AuditVerdict::Skip(why) => format!("Skip: {why}"),
                    };
                    (r.invariant.to_string(), v)
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Verdict {
        ok: report.is_ok(),
        findings,
        audit,
    })
}

const MODES: [&str; 3] = ["quick", "full", "audit-deep"];

/// GD11.1
#[tokio::test]
async fn verdicts_equal_the_pre_migration_capture() {
    let Some(root) = datasets_root() else {
        if strict() {
            panic!("CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT is not set");
        }
        eprintln!("[SKIP] CQLITE_DATASETS_ROOT not set");
        return;
    };
    let mut observed: BTreeMap<String, Result<Verdict, String>> = BTreeMap::new();
    for rel in case_dirs(&root) {
        let dir = root.join(&rel);
        if !has_data_db(&dir) {
            continue;
        }
        for mode in MODES {
            observed.insert(format!("{rel}#{mode}"), run_case(&dir, mode).await);
        }
    }
    if let Ok(path) = std::env::var("CQLITE_4626_DUMP") {
        let ok: BTreeMap<&String, &Verdict> = observed
            .iter()
            .map(|(k, v)| (k, v.as_ref().expect("capture case must not Err")))
            .collect();
        std::fs::write(path, serde_json::to_string_pretty(&ok).expect("json")).expect("write");
        return;
    }
    let oracle: BTreeMap<String, Verdict> =
        serde_json::from_str(&std::fs::read_to_string(ORACLE).expect("read oracle"))
            .expect("parse oracle");
    let mut failures = Vec::new();
    for (case, expected) in &oracle {
        match observed.get(case) {
            None if strict() => failures.push(format!("{case}: fixture absent (strict)")),
            None => eprintln!("[SKIP] {case}: fixture absent"),
            Some(Ok(v)) if v == expected => {}
            Some(got) => failures.push(format!("{case}: expected {expected:?}, got {got:?}")),
        }
    }
    for case in observed.keys() {
        if !oracle.contains_key(case) {
            failures.push(format!("{case}: not in the oracle"));
        }
    }
    assert!(
        failures.is_empty(),
        "verdict changes:\n{}",
        failures.join("\n")
    );
}

/// GD11.2: below-floor is a refusal naming the floor and opening no component. (Unix only: the
/// proof is mode-000 components.)
#[cfg(unix)]
#[tokio::test]
async fn below_floor_generation_is_refused_without_opening_a_component() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Every component exists but is mode 000: opening any of them would fail with an I/O error,
    // so a refusal that is `UnsupportedVersion` proves none was opened. (Listing reads names only.)
    for c in [
        "Data.db",
        "Statistics.db",
        "TOC.txt",
        "Digest.crc32",
        "Index.db",
    ] {
        let p = tmp.path().join(format!("ma-1-big-{c}"));
        std::fs::write(&p, b"x").expect("write");
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    }
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let by_dir = verify_sstable(tmp.path(), VerifyMode::Full, &config, platform.clone()).await;
    let by_gen = verify_sstable_generation(
        &tmp.path().join("ma-1-big-Data.db"),
        VerifyMode::Quick,
        &config,
        platform,
    )
    .await;
    for r in [by_dir, by_gen] {
        match r {
            Err(Error::UnsupportedVersion { version, floor }) => {
                assert_eq!(version, "ma");
                assert_eq!(floor, "na", "the refusal must name the floor");
            }
            other => panic!(
                "expected UnsupportedVersion, got {:?}",
                other.map(|r| r.findings)
            ),
        }
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for e in std::fs::read_dir(from).expect("read_dir").flatten() {
        if e.path().is_file() {
            std::fs::copy(e.path(), to.join(e.file_name())).expect("copy");
        }
    }
}

/// GD11 mapping: `TocMissing` is the existing missing-`TOC.txt` finding, nothing else.
#[tokio::test]
async fn toc_missing_is_the_existing_missing_toc_finding() {
    let Some(root) = datasets_root() else {
        if strict() {
            panic!("CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT is not set");
        }
        return;
    };
    let src = sorted_subdirs(&root.join("sstables/test_comp"))
        .into_iter()
        .find(|n| n.starts_with("lz4_table"))
        .map(|n| root.join("sstables/test_comp").join(n));
    let Some(src) = src.filter(|d| has_data_db(d)) else {
        assert!(
            !strict(),
            "lz4_table fixture absent under CQLITE_REQUIRE_FIXTURES=1"
        );
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    copy_dir(&src, tmp.path());
    let toc = std::fs::read_dir(tmp.path())
        .expect("read_dir")
        .flatten()
        .find(|e| e.file_name().to_string_lossy().ends_with("-TOC.txt"))
        .expect("TOC.txt in the copy");
    std::fs::remove_file(toc.path()).expect("remove TOC");
    let got = run_case(tmp.path(), "full").await.expect("verdict");
    assert!(!got.ok);
    assert_eq!(
        got.findings,
        vec![("MissingComponent".to_string(), "TOC.txt".to_string())]
    );
}

/// An Orphan (empty `Data.db`) is reported by name, never verified as live; and a directory input
/// is Scope Live, so a generation only under `snapshots/<tag>/` is not found.
#[tokio::test]
async fn orphans_are_reported_and_snapshots_are_not_entered() {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));

    let orphan = tempfile::tempdir().expect("tempdir");
    std::fs::write(orphan.path().join("nb-1-big-Data.db"), b"").expect("write");
    match verify_sstable(orphan.path(), VerifyMode::Quick, &config, platform.clone()).await {
        Err(Error::NotFound(msg)) => {
            assert!(msg.contains("orphan") && msg.contains("nb-1-big"), "{msg}")
        }
        other => panic!(
            "expected NotFound naming the orphan, got {:?}",
            other.map(|r| r.findings)
        ),
    }

    let snap = tempfile::tempdir().expect("tempdir");
    let tag = snap.path().join("snapshots/t1");
    std::fs::create_dir_all(&tag).expect("mkdir");
    std::fs::write(tag.join("nb-1-big-Data.db"), b"data").expect("write");
    match verify_sstable(snap.path(), VerifyMode::Quick, &config, platform).await {
        Err(Error::NotFound(_)) => {}
        other => panic!(
            "snapshots/ must not be entered, got {:?}",
            other.map(|r| r.findings)
        ),
    }
}
