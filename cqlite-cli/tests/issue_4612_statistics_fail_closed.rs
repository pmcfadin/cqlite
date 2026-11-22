//! Issue #4612 through the compiled binary: `cqlite explain` and `cqlite find` over a
//! Cassandra-written generation whose `Statistics.db` is truncated or has one flipped byte must
//! fail closed (exit 2) naming the `Statistics.db` path, never print a trail / probe verdict
//! computed from a partial parse. Fixtures are git-COMMITTED, so absence fails closed (#3220).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::Command;

fn td() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../test-data")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    for e in std::fs::read_dir(src)
        .unwrap_or_else(|e| panic!("committed fixture {src:?} missing: {e}"))
        .flatten()
    {
        if e.file_type().expect("type").is_file() {
            std::fs::copy(e.path(), dst.join(e.file_name())).expect("copy");
        }
    }
}

fn damaged(pristine: &[u8]) -> Vec<(String, Vec<u8>)> {
    let n = pristine.len();
    let mut out = Vec::new();
    for len in [0, 4, 8, 40, n / 2, n - 5, n - 1] {
        out.push((format!("truncated to {len} of {n}"), pristine[..len].to_vec()));
    }
    for at in [0, 9, n / 2, n - 6, n - 1] {
        let mut b = pristine.to_vec();
        b[at] ^= 0x01;
        out.push((format!("byte {at} of {n} flipped"), b));
    }
    out
}

fn stats_of(dir: &Path) -> PathBuf {
    std::fs::read_dir(dir)
        .expect("read dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Statistics.db"))
        .expect("fixture Statistics.db")
}

#[test]
fn explain_refuses_every_damaged_statistics_db_naming_the_path() {
    let src = td()
        .join("datasets/sstables/test_explain/trace_decisions-4ad336f0df0611f181786327fffcebdf");
    let tmp = tempfile::tempdir().expect("tempdir");
    let staged = tmp.path().join("trace_decisions-4ad336f0df0611f181786327fffcebdf");
    copy_dir(&src, &staged);
    let stats = stats_of(&staged);
    let pristine = std::fs::read(&stats).expect("read");
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_cqlite"))
            .arg("--schema")
            .arg(td().join("schemas/explain-trace.cql"))
            .arg("--data-dir")
            .arg(&staged)
            .args(["explain", "test_explain.trace_decisions", "1", "--now", "1794535358"])
            .output()
            .expect("spawn")
    };
    // Control: the pristine copy explains, so a refusal below is caused by the damage alone.
    let ok = run();
    assert_eq!(ok.status.code(), Some(0), "{}", String::from_utf8_lossy(&ok.stderr));
    for (label, bytes) in damaged(&pristine) {
        std::fs::write(&stats, &bytes).expect("damage");
        let out = run();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{label}: stderr: {stderr}");
        assert!(out.stdout.is_empty(), "{label}: no trail may be printed");
        assert!(
            stderr.contains(&stats.display().to_string()),
            "{label}: stderr must name {}: {stderr}",
            stats.display()
        );
    }
}

#[test]
fn find_reports_a_damaged_generation_unreadable_with_its_path_and_exits_two() {
    let src = td().join("datasets/sstables/test_comp");
    let src = std::fs::read_dir(&src)
        .expect("test_comp")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("lz4_table-")))
        .expect("lz4_table fixture");
    let tmp = tempfile::tempdir().expect("tempdir");
    let staged = tmp.path().join(src.file_name().expect("name"));
    copy_dir(&src, &staged);
    let stats = stats_of(&staged);
    let pristine = std::fs::read(&stats).expect("read");
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_cqlite"))
            .arg("--schema")
            .arg(td().join("schemas/compression-parity.cql"))
            .arg("find")
            .arg(&staged)
            .arg("1")
            .output()
            .expect("spawn")
    };
    let ok = run();
    assert_eq!(ok.status.code(), Some(0), "{}", String::from_utf8_lossy(&ok.stderr));
    for (label, bytes) in damaged(&pristine) {
        std::fs::write(&stats, &bytes).expect("damage");
        let out = run();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(2), "{label}: stdout: {stdout}");
        assert!(stdout.contains("UNREADABLE"), "{label}: {stdout}");
        assert!(
            stdout.contains(&stats.display().to_string()),
            "{label}: the cause must name {}: {stdout}",
            stats.display()
        );
    }
}
