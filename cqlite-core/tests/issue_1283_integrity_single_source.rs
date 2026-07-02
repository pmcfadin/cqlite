//! Issue #1283 (OpenSpec `consolidate-verify`, Option A): `verify::verify_sstable`
//! is the SINGLE source of truth for SSTable integrity, and the legacy
//! `SSTableReader::perform_integrity_check` is a thin PROJECTION over it.
//!
//! ## The divergence this proves is gone
//!
//! The legacy integrity check walked only `Data.db` blocks. A corruption in a
//! SIBLING component that the block-walk never reads — a mismatched
//! `Digest.crc32` — left `Data.db` fully intact and parseable, so the old
//! implementation returned `IntegrityStatus::Healthy` for a file that
//! `verify_sstable` (Check 2, `DigestMismatch`) correctly FAILs. That is a
//! contradictory verdict from two "integrity" APIs.
//!
//! After consolidation `perform_integrity_check` delegates to `verify_sstable`,
//! so the Digest corruption is reported as `Corrupted`. This test FAILS against
//! the pre-#1283 block-walk (which reports `Healthy`) and passes against the
//! projection.
//!
//! Uses real Cassandra 5.0 dataset SSTables copied into a temp dir so the
//! corruption is applied to a throwaway copy, never the committed corpus.

use cqlite_core::storage::sstable::reader::{IntegrityStatus, SSTableReader};
use cqlite_core::{Config, Platform};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Candidate clean dataset tables (relative to `<datasets>/sstables`). The test
/// uses the FIRST one whose clean copy verifies `Healthy` as the base for the
/// corruption, so it stays robust if any single table gains a benign finding.
const CANDIDATES: &[&str] = &[
    "test_basic/simple_table",
    "test_basic/composite_key_table",
    "test_basic/multi_partition_table",
    "test_basic/compression_test_table",
];

fn datasets_root() -> Option<PathBuf> {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        let p = PathBuf::from(root);
        if p.is_dir() {
            return Some(p);
        }
    }
    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.join("test-data/datasets"))?;
    fallback.is_dir().then_some(fallback)
}

/// Resolve the `*-<uuid>` generation directory for a `keyspace/table` prefix.
fn resolve_table_dir(sstables: &Path, table_prefix: &str) -> Option<PathBuf> {
    let (keyspace, table) = table_prefix.split_once('/')?;
    let ks_dir = sstables.join(keyspace);
    for entry in std::fs::read_dir(&ks_dir).ok()?.flatten() {
        let name = entry.file_name();
        let name = name.to_str()?;
        if name.starts_with(&format!("{table}-")) && entry.path().is_dir() {
            // Must contain a Data.db to be usable.
            if entry.path().join("nb-1-big-Data.db").is_file() {
                return Some(entry.path());
            }
        }
    }
    None
}

/// Copy the SSTable *component* files (skipping `.jsonl` / `.db.txt` sidecars)
/// from `src` into a fresh temp dir. Returns the temp dir and the copied
/// `Data.db` path.
fn copy_sstable_components(src: &Path) -> std::io::Result<(tempfile::TempDir, PathBuf)> {
    let tmp = tempfile::tempdir()?;
    let mut data_db = None;
    for entry in std::fs::read_dir(src)?.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // Skip validation sidecars that are not real SSTable components.
        if name.ends_with(".jsonl") || name.ends_with(".db.txt") {
            continue;
        }
        if !entry.path().is_file() {
            continue;
        }
        let dest = tmp.path().join(name);
        std::fs::copy(entry.path(), &dest)?;
        if name.ends_with("-Data.db") {
            data_db = Some(dest);
        }
    }
    let data_db = data_db
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no -Data.db copied"))?;
    Ok((tmp, data_db))
}

async fn open_reader(data_db: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init should succeed"),
    );
    SSTableReader::open(data_db, &config, platform)
        .await
        .expect("opening a structurally valid SSTable copy should succeed")
}

/// Corrupt `Digest.crc32` in `dir` so it no longer matches `CRC32(Data.db)`.
/// Reads the recorded value and writes `recorded.wrapping_add(1)` — guaranteed
/// to differ from the (clean) computed CRC without depending on Data.db bytes.
fn corrupt_digest(dir: &Path) {
    let digest = dir.join("nb-1-big-Digest.crc32");
    let text = std::fs::read_to_string(&digest).expect("read Digest.crc32");
    let recorded: u32 = text.trim().parse().expect("Digest.crc32 is a u32");
    std::fs::write(&digest, recorded.wrapping_add(1).to_string()).expect("rewrite Digest.crc32");
}

/// A `Digest.crc32` corruption — invisible to the pre-#1283 Data.db block-walk
/// (which read `Healthy`) — now makes `perform_integrity_check` report
/// `Corrupted`, because it projects over the authoritative `verify_sstable`.
#[tokio::test]
async fn digest_corruption_makes_integrity_check_corrupted_not_healthy() {
    let Some(root) = datasets_root() else {
        eprintln!("SKIP: no datasets root; set CQLITE_DATASETS_ROOT.");
        return;
    };
    let sstables = root.join("sstables");

    // Find a candidate whose CLEAN copy verifies Healthy — that is our base.
    let mut base: Option<(tempfile::TempDir, PathBuf)> = None;
    for cand in CANDIDATES {
        let Some(src) = resolve_table_dir(&sstables, cand) else {
            continue;
        };
        let Ok((tmp, data_db)) = copy_sstable_components(&src) else {
            continue;
        };
        let reader = open_reader(&data_db).await;
        let report = reader
            .perform_integrity_check()
            .await
            .expect("integrity check should run");
        if report.overall_status == IntegrityStatus::Healthy {
            base = Some((tmp, data_db));
            break;
        }
    }

    let Some((tmp, data_db)) = base else {
        eprintln!(
            "SKIP: no candidate dataset table available or verifying Healthy; \
             fetch datasets (test-data/scripts/fetch-datasets.sh)."
        );
        return;
    };

    // Corrupt ONLY the sibling Digest.crc32 — Data.db is left byte-intact, so the
    // legacy block-walk would still parse every block and report Healthy.
    corrupt_digest(tmp.path());

    // Re-open on the (unchanged) Data.db and re-check: the projection over
    // verify_sstable now sees the DigestMismatch finding.
    let reader = open_reader(&data_db).await;
    let report = reader
        .perform_integrity_check()
        .await
        .expect("integrity check should run over the digest-corrupted copy");

    assert_eq!(
        report.overall_status,
        IntegrityStatus::Corrupted,
        "a Digest.crc32 mismatch (Data.db intact) must be Corrupted via the verify projection; \
         the pre-#1283 Data.db block-walk would have said Healthy. report: {report:?}"
    );
    assert!(
        report
            .parsing_errors
            .iter()
            .any(|e| e.contains("DigestMismatch") || e.contains("Digest.crc32")),
        "the corrupted verdict must carry the DigestMismatch finding detail: {:?}",
        report.parsing_errors
    );
}
