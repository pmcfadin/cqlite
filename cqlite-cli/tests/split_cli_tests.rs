//! Issue #4199 (spec R-CLI-2) — `cqlite split` through the compiled binary.
//! Covers R-CLI-2.1 (`--parts N` produces N verified parts, exit 0) and
//! R-CLI-2.3 (a multi-generation-looking usage error is reported at exit 1)
//! using the committed `test_basic.composite_key_table` fixture (single
//! generation, 99 partitions).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

const FIXTURE_RELATIVE: &str =
    "sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9";
const FIXTURE_DIR_NAME: &str = "composite_key_table-6ab56990a25111f0a3fef1a551383fb9";
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
const FIXTURE_SCHEMA: &str = "basic-types.cql";

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

fn run_split(input: &Path, out: &Path, extra: &[&str]) -> Output {
    let schema = schema_path();
    let mut args: Vec<&str> = vec![
        "--schema",
        as_arg(&schema),
        "split",
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
fn parts_n_produces_n_verified_parts() {
    let clean = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input = stage_input(&clean, temp.path());
    let data_db = input.join("nb-1-big-Data.db");
    let out = temp.path().join("out");

    let output = run_split(&data_db, &out, &["--parts", "4", "--out-format", "json"]);
    assert!(
        output.status.success(),
        "expected exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let manifest: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("stdout was not valid JSON ({e}): {:?}", output.stdout));
    let parts = manifest["parts"].as_array().expect("parts array");
    assert_eq!(parts.len(), 4, "exactly 4 parts, got {parts:?}");
    for p in parts {
        assert_eq!(p["verify"], "pass");
    }
    let source_partitions = manifest["source_partitions"].as_u64().unwrap();
    let sum: u64 = parts
        .iter()
        .map(|p| p["partitions"].as_u64().unwrap())
        .sum();
    assert_eq!(sum, source_partitions);
}

#[test]
fn multi_generation_table_dir_without_explicit_data_db_is_a_usage_error() {
    let clean = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input = stage_input(&clean, temp.path());
    // Add a SECOND (bit-for-bit copy, generation "2") Data.db-shaped generation
    // so `input` (the table DIR) resolves to more than one generation.
    for component in FIXTURE_COMPONENTS {
        let dest_name = component.replacen("nb-1-big", "nb-2-big", 1);
        std::fs::copy(clean.join(component), input.join(dest_name)).expect("stage second gen");
    }
    let out = temp.path().join("out");

    let output = run_split(&input, &out, &["--parts", "2"]);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a table dir resolving to >1 generation with no explicit Data.db path must be a usage \
         error; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!out.exists() || std::fs::read_dir(&out).unwrap().next().is_none());
}
