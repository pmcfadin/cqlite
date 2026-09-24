//! Issue #4199 (spec R-CLI-1) — `cqlite extract` through the compiled
//! binary. Covers R-CLI-1.1 (single partition, reconciled, exit 0) using the
//! same `test_comp.lz4_table` fixture `issue_4196_salvage_write_guard.rs`
//! stages (a real, committed, single-partition Cassandra 5.0 fixture with a
//! simple `INT` partition key, so the CLI literal is a bare `1`).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

const FIXTURE_RELATIVE: &str = "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77";
const FIXTURE_DIR_NAME: &str = "lz4_table-25801a0071a911f19b3225f9984c6a77";
const FIXTURE_COMPONENTS: &[&str] = &[
    "nb-1-big-Data.db",
    "nb-1-big-Index.db",
    "nb-1-big-Summary.db",
    "nb-1-big-Statistics.db",
    "nb-1-big-CompressionInfo.db",
    "nb-1-big-Filter.db",
    "nb-1-big-Digest.crc32",
    "nb-1-big-TOC.txt",
];
const FIXTURE_SCHEMA: &str = "compression-parity.cql";
const LIVE_PARTITION_KEY: &str = "1";

fn candidate_base_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(env_root) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(env_root));
    }
    let checkout = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .join("test-data/datasets");
    if !roots.contains(&checkout) {
        roots.push(checkout);
    }
    roots
}

fn resolve_committed_fixture() -> PathBuf {
    let roots = candidate_base_roots();
    for root in &roots {
        let dir = root.join(FIXTURE_RELATIVE);
        if FIXTURE_COMPONENTS.iter().all(|c| dir.join(c).is_file()) {
            return dir;
        }
    }
    panic!(
        "COMMITTED fixture {FIXTURE_RELATIVE} is absent (git-tracked binaries; broken checkout, \
         not an unfetched dataset). Searched: {roots:?}"
    );
}

fn stage_input(clean_dir: &Path, dest_parent: &Path) -> PathBuf {
    let dest = dest_parent.join(FIXTURE_DIR_NAME);
    std::fs::create_dir_all(&dest).unwrap_or_else(|e| panic!("create {dest:?}: {e}"));
    for component in FIXTURE_COMPONENTS {
        std::fs::copy(clean_dir.join(component), dest.join(component))
            .unwrap_or_else(|e| panic!("copy {component}: {e}"));
    }
    dest
}

fn schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .join("test-data/schemas")
        .join(FIXTURE_SCHEMA)
}

fn as_arg(path: &Path) -> &str {
    path.to_str()
        .unwrap_or_else(|| panic!("{path:?} is not utf-8"))
}

fn run_extract(input: &Path, out: &Path, extra: &[&str]) -> Output {
    let schema = schema_path();
    let mut args: Vec<&str> = vec![
        "--schema",
        as_arg(&schema),
        "extract",
        as_arg(input),
        "--out",
        as_arg(out),
    ];
    args.extend_from_slice(extra);
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(&args)
        .output()
        .expect("failed to execute cqlite binary")
}

#[test]
fn single_partition_reconciled_exits_0_and_writes_one_generation() {
    let clean = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input = stage_input(&clean, temp.path());
    let out = temp.path().join("out");

    let output = run_extract(
        &input,
        &out,
        &["--partition", LIVE_PARTITION_KEY, "--out-format", "json"],
    );
    assert!(
        output.status.success(),
        "expected exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let manifest: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("stdout was not valid JSON ({e}): {:?}", output.stdout));
    assert_eq!(manifest["not_found"].as_array().unwrap().len(), 0);
    assert_eq!(manifest["generations_written"].as_array().unwrap().len(), 1);
    assert!(manifest["refused"].is_null());

    let output_table_dir = out.join("test_comp").join("lz4_table");
    assert!(
        std::fs::read_dir(&output_table_dir)
            .expect("read output table dir")
            .flatten()
            .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db")),
        "the output table dir must hold a Data.db"
    );
}

#[test]
fn usage_error_when_no_selection_flag_is_given() {
    let clean = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input = stage_input(&clean, temp.path());
    let out = temp.path().join("out");

    let output = run_extract(&input, &out, &[]);
    assert_eq!(
        output.status.code(),
        Some(1),
        "no selection flag must be a usage error; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!out.exists() || std::fs::read_dir(&out).unwrap().next().is_none());
}
