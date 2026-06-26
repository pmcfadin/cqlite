//! Issue #1103: the FULL-mode BTI verifier must cross-check `Partitions.db`
//! against `Data.db` by partition-key IDENTITY, not just partition COUNT.
//!
//! A corrupt `Partitions.db` root that walks a wrong subtree yielding a
//! DIFFERENT set of partition keys with the SAME leaf count previously passed
//! verification (the count matched even though the identities did not). These
//! tests assert:
//!
//! 1. A HEALTHY BTI table (`test_da/wide_table`) PASSES FULL verification — i.e.
//!    no false-positive from the identity compare (the original blocker: the BTI
//!    trie key encoding and the Data.db key encoding are byte-disjoint and must
//!    be normalized before comparing).
//! 2. A same-count wrong-root corruption (a flipped trie transition byte that
//!    keeps 3 leaves but changes a key) FAILS with `BtiRootPointerCorrupt`.
//!
//! Skip-clean when the dataset is absent (worktrees without fetched Data.db);
//! fail-loud when the dataset is present.

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{
    verify_sstable, VerifyErrorClass, VerifyMode, VerifyReport,
};
use cqlite_core::Config;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
}

/// Locate the `test_da/wide_table-*` BTI generation directory, if present.
fn wide_table_dir() -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables/test_da");
    let rd = std::fs::read_dir(&base).ok()?;
    rd.flatten().map(|e| e.path()).find(|p| {
        p.is_dir()
            && p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("wide_table-"))
                .unwrap_or(false)
    })
}

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

async fn verify(dir: &Path, mode: VerifyMode) -> VerifyReport {
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform::new must succeed"),
    );
    verify_sstable(dir, mode, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("verify_sstable({}) returned Err: {e}", dir.display()))
}

/// (1) The healthy BTI table passes FULL verification — no false-positive from
/// the identity cross-check, and a non-zero row count (a present-but-empty scan
/// would be a silent-corruption regression).
#[tokio::test]
async fn healthy_wide_table_bti_passes_full_verification() {
    let Some(dir) = wide_table_dir() else {
        eprintln!("[SKIP] test_da/wide_table not present (set CQLITE_DATASETS_ROOT)");
        return;
    };
    if !has_data_db(&dir) {
        eprintln!(
            "[SKIP] {} has no Data.db (binaries not fetched)",
            dir.display()
        );
        return;
    }

    let report = verify(&dir, VerifyMode::Full).await;
    assert!(
        report.is_ok(),
        "healthy BTI wide_table must PASS full verification (no identity false-positive), got: {:?}",
        report.findings
    );
    assert!(
        matches!(report.rows_scanned, Some(n) if n > 0),
        "healthy BTI wide_table FULL verify must scan a non-zero row count, got {:?}",
        report.rows_scanned
    );
}

/// (2) A same-count wrong-root corruption is detected. We copy the healthy
/// fixture to a temp dir and flip a single Sparse-node transition byte in
/// `Partitions.db` (`0x47` -> `0x46` at offset 0x0E). The trie still yields 3
/// leaves, but one partition key changes, so it no longer matches the Data.db
/// key set. A count-only check would still pass (3 == 3); the identity check
/// must fail with `BtiRootPointerCorrupt` on `Partitions.db`.
#[tokio::test]
async fn same_count_wrong_root_bti_is_detected() {
    let Some(src) = wide_table_dir() else {
        eprintln!("[SKIP] test_da/wide_table not present (set CQLITE_DATASETS_ROOT)");
        return;
    };
    if !has_data_db(&src) {
        eprintln!(
            "[SKIP] {} has no Data.db (binaries not fetched)",
            src.display()
        );
        return;
    }

    // Materialize a mutable copy in a temp dir.
    let tmp = std::env::temp_dir().join(format!("cqlite-1103-wrongroot-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).expect("create temp dir");
    let mut partitions_path = None;
    for entry in std::fs::read_dir(&src).expect("read src dir").flatten() {
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let name = p.file_name().unwrap().to_owned();
        let dst = tmp.join(&name);
        std::fs::copy(&p, &dst).expect("copy fixture file");
        if name
            .to_str()
            .map(|n| n.ends_with("-Partitions.db"))
            .unwrap_or(false)
        {
            partitions_path = Some(dst);
        }
    }
    let partitions_path = partitions_path.expect("fixture must have a Partitions.db");

    // Flip the first Sparse-node transition byte: 0x47 -> 0x46 at offset 0x0E.
    // This keeps 3 leaves (same count) but changes the recovered partition key.
    let mut bytes = std::fs::read(&partitions_path).expect("read Partitions.db");
    const TRANSITION_OFFSET: usize = 0x0E;
    assert!(
        bytes.len() > TRANSITION_OFFSET,
        "Partitions.db unexpectedly short ({} bytes)",
        bytes.len()
    );
    assert_eq!(
        bytes[TRANSITION_OFFSET], 0x47,
        "fixture layout changed: expected transition byte 0x47 at offset 0x0E, found 0x{:02x}",
        bytes[TRANSITION_OFFSET]
    );
    bytes[TRANSITION_OFFSET] = 0x46;
    std::fs::write(&partitions_path, &bytes).expect("write corrupted Partitions.db");

    let report = verify(&tmp, VerifyMode::Full).await;
    let _ = std::fs::remove_dir_all(&tmp);

    assert!(
        !report.is_ok(),
        "same-count wrong-root BTI corruption MUST fail verification, but passed (rows={:?})",
        report.rows_scanned
    );
    assert!(
        report.findings.iter().any(|f| {
            f.class == VerifyErrorClass::BtiRootPointerCorrupt && f.component == "Partitions.db"
        }),
        "expected a BtiRootPointerCorrupt finding on Partitions.db, got: {:?}",
        report.findings
    );
}
