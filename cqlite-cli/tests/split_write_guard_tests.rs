//! Issue #4199 (spec R-CLI-4.1) — `cqlite split --out` inside the input is
//! refused (exit 1) and the input gains no new files, via the SAME promoted
//! `WriteGuard` (`commands::write_guard`, issue #4196 round-23 F1/F4/F5)
//! `salvage`/`extract` already use.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

const FIXTURE_RELATIVE: &str = "sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9";
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
    panic!("COMMITTED fixture {FIXTURE_RELATIVE} is absent. Searched: {roots:?}");
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
    path.to_str().unwrap_or_else(|| panic!("{path:?} is not utf-8"))
}

fn run_split(input: &Path, out: &Path) -> Output {
    let schema = schema_path();
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            as_arg(&schema),
            "split",
            as_arg(input),
            "--parts",
            "2",
            "--out",
            as_arg(out),
        ])
        .output()
        .expect("failed to execute cqlite binary")
}

fn list_files(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .expect("read dir")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect()
}

#[test]
fn out_inside_the_input_is_refused_and_the_input_gains_no_new_files() {
    let clean = resolve_committed_fixture();
    let temp = TempDir::new().expect("tempdir");
    let input = stage_input(&clean, temp.path());
    let data_db = input.join("nb-1-big-Data.db");
    let before = list_files(&input);

    let out = input.join("parts");
    let output = run_split(&data_db, &out);
    assert_eq!(
        output.status.code(),
        Some(1),
        "--out inside the input must be refused; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("INPUT"),
        "the refusal must name what it collided with; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let after = list_files(&input);
    assert_eq!(
        before, after,
        "the input directory must gain NO new files from a refused --out"
    );
}
