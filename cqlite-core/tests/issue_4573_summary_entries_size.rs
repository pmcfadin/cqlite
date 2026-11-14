//! Issue #4573 — a `Summary.db` whose header declares an entries size smaller than its own offset
//! table must be a NAMED corruption error, never an arithmetic panic (found by `fuzz_audit`, #4502).
//!
//! Inputs are a REAL Cassandra-written `Summary.db` (datasets-v4 `test_comp/lz4_table`: header
//! `min_index_interval=128, entries_count=1, summary_entries_size=16`) with ONE documented edit —
//! the u64 `summary_entries_size` at bytes 8..16 set to 0, so the declared entries size (0) is
//! below the offset table (1 entry x 4 = 4 bytes) — and the 31-byte minimised fuzz input
//! (the same edit, truncated). The control (the unedited file) must still open.

#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::summary_reader::SummaryReader;
use cqlite_core::storage::sstable::verify::{verify_sstable, VerifyMode};
use cqlite_core::Config;

fn strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn fixture() -> Option<PathBuf> {
    match datasets_root::resolve_table_generation_dir("test_comp", "lz4_table") {
        Ok(d) => Some(d),
        Err(e) if strict() => {
            panic!("test_comp.lz4_table missing under CQLITE_REQUIRE_FIXTURES: {e}")
        }
        Err(e) => {
            eprintln!("SKIP: {e}");
            None
        }
    }
}

fn summary_path(dir: &Path) -> PathBuf {
    std::fs::read_dir(dir)
        .expect("dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Summary.db"))
        .expect("Summary.db")
}

fn copy_with_summary(src: &Path, summary: &[u8]) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    for e in std::fs::read_dir(src).expect("dir").flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if e.path().is_file() && !name.ends_with(".jsonl") && !name.ends_with(".txt.jsonl") {
            std::fs::copy(e.path(), tmp.path().join(&name)).expect("copy");
        }
    }
    std::fs::write(summary_path(tmp.path()), summary).expect("write Summary.db");
    tmp
}

async fn open_err(dir: &Path) -> Result<(), String> {
    let platform = Arc::new(Platform::new(&Config::default()).await.expect("platform"));
    SummaryReader::open(&summary_path(dir), platform)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn an_entries_size_below_the_offset_table_is_a_named_error_not_a_panic() {
    let Some(src) = fixture() else { return };
    let real = std::fs::read(summary_path(&src)).expect("real Summary.db");
    assert_eq!(
        &real[4..8],
        &[0, 0, 0, 1],
        "precondition: the real file declares ONE entry"
    );

    // Control: the unedited, real file opens.
    open_err(&src).await.expect("the real Summary.db must open");

    // The documented edit: summary_entries_size (u64 BE, bytes 8..16) := 0 < 1 * 4.
    let mut edited = real.clone();
    edited[8..16].copy_from_slice(&0u64.to_be_bytes());
    let tmp = copy_with_summary(&src, &edited);
    let err = open_err(tmp.path())
        .await
        .expect_err("the damaged Summary.db must be rejected");
    assert!(
        err.contains("entries size 0 is smaller than its offset table 4"),
        "the error must name the cause, got: {err}"
    );

    // The minimised fuzz input (31 bytes: the same edit, truncated) is rejected the same way.
    let tmp2 = copy_with_summary(&src, &edited[..31]);
    let err2 = open_err(tmp2.path())
        .await
        .expect_err("the 31-byte input must be rejected");
    assert!(
        err2.contains("smaller than its offset table"),
        "got: {err2}"
    );

    // `verify --mode full` over the damaged generation reports instead of panicking.
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let report = verify_sstable(tmp.path(), VerifyMode::Full, &config, platform)
        .await
        .expect("verify returns a report");
    assert!(
        !report.is_ok(),
        "a damaged Summary.db must not verify clean: {}",
        report.summary_line()
    );
    assert!(
        report.findings.iter().any(|f| f.component == "Summary.db"),
        "a finding must name Summary.db: {:?}",
        report.findings
    );
}
