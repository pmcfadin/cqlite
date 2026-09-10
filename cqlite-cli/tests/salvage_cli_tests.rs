//! Issue #4196 (spec R7) — `cqlite salvage` CLI surface: verb, inputs, exit
//! codes.
//!
//! Drives the REAL compiled `cqlite` binary (`CARGO_BIN_EXE_cqlite`), not the
//! handler function directly — `execute_salvage_command` enforces design D3's
//! exit codes via `std::process::exit`, which only a subprocess can observe.
//!
//! Dataset doctrine (issue #719): SKIP when the real fixtures are absent;
//! `CQLITE_REQUIRE_FIXTURES=1` turns that into a hard failure.

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
}

fn skip_or_require(what: &str, reason: &str) -> bool {
    if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but {what} unavailable: {reason}");
    }
    eprintln!("[SKIP] {what}: {reason}");
    true
}

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .join("test-data/schemas")
}

fn run_cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(args)
        .output()
        .expect("failed to execute cqlite binary")
}

fn usable(dir: &Path) -> bool {
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

/// R7.1 — a healthy table dir with TWO generations salvages each separately,
/// exit 0, JSON manifest names two entries each with `losses: []`.
#[test]
fn healthy_table_dir_two_generations_exit_0() {
    let Some(root) = datasets_root() else {
        skip_or_require("salvage_cli_tests R7.1", "CQLITE_DATASETS_ROOT not set");
        return;
    };
    let table_dir =
        root.join("sstables/test_tomb/resurrection_gc_positive-4cbfab10702011f1b8f419c9a388d558");
    if !usable(&table_dir) {
        skip_or_require(
            "resurrection_gc_positive fixture",
            &format!("{table_dir:?} not usable"),
        );
        return;
    }
    let schema = schemas_dir().join("tombstone-parity.cql");
    let temp = TempDir::new().expect("tempdir");
    let out = temp.path().join("out");

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "salvage",
        table_dir.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--out-format",
        "json",
    ]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "expected exit 0; stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let manifest: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout is not JSON: {e}\n{stdout}"));
    let entries = manifest
        .as_array()
        .unwrap_or_else(|| panic!("expected a JSON array (table-dir input); got {manifest}"));
    assert_eq!(
        entries.len(),
        2,
        "expected two generation entries; got {entries:?}"
    );
    for entry in entries {
        let losses = entry
            .get("losses")
            .and_then(|l| l.as_array())
            .unwrap_or_else(|| panic!("entry missing 'losses' array: {entry}"));
        assert!(losses.is_empty(), "expected zero losses; entry={entry}");
        assert!(
            entry.get("refused").map(|r| r.is_null()).unwrap_or(false),
            "expected refused: null; entry={entry}"
        );
    }

    // Two complete generation sets under --out.
    let out_table_dir = discover_output_table_dir(&out);
    let data_dbs: Vec<_> = std::fs::read_dir(&out_table_dir)
        .expect("read out table dir")
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        .collect();
    assert_eq!(
        data_dbs.len(),
        2,
        "expected two Data.db files under --out; got {data_dbs:?}"
    );
}

/// R7.2 — a damaged input's `--manifest` names every loss (chunk-crc class).
///
/// The corpus's ONLY `data_db_bit_flip` fixture (`test_comp.lz4_table`) holds
/// exactly ONE partition, entirely inside the flipped chunk (see
/// `issue_4196_salvage_corruption_corpus.rs`'s core-level test for the same
/// discovery), so it exercises the TOTAL-loss exit-2 arm (spec R5.2) rather
/// than R7.2's literal partial-recovery exit-3 scenario text — documented
/// here rather than asserting a code this fixture cannot produce. No corpus
/// fixture demonstrates a genuinely PARTIAL chunk-crc loss today; that arm
/// (exit 3, output still written despite losses) is a declared gap tracked
/// as a follow-up.
#[test]
fn damaged_input_manifest_names_every_loss() {
    let Some(root) = datasets_root() else {
        skip_or_require("salvage_cli_tests R7.2", "CQLITE_DATASETS_ROOT not set");
        return;
    };
    let corrupt_dir = root.join("corruption/test_comp_corrupt/data_db_bit_flip");
    if !usable(&corrupt_dir) {
        skip_or_require(
            "data_db_bit_flip fixture",
            &format!("{corrupt_dir:?} not usable"),
        );
        return;
    }
    let data_db = single_data_db(&corrupt_dir);
    let schema = schemas_dir().join("compression-parity.cql");
    let temp = TempDir::new().expect("tempdir");
    let out = temp.path().join("out");
    let manifest_path = temp.path().join("m.json");

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "salvage",
        data_db.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
        "--manifest",
        manifest_path.to_str().unwrap(),
    ]);

    // Exit is 2 (total loss, R5.2) for THIS fixture — see the doc comment
    // above. Both 2 and 3 write the manifest with the losses named; only the
    // exit code and (for 3) output presence differ, so both are accepted
    // here and the code observed is asserted to be one of them explicitly
    // (never treated as "any non-zero is fine").
    let code = output.status.code();
    assert!(
        code == Some(2) || code == Some(3),
        "expected exit 2 or 3; got {code:?}; stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let manifest_text = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("manifest not written to {manifest_path:?}: {e}"));
    let manifest: serde_json::Value =
        serde_json::from_str(&manifest_text).expect("manifest is not JSON");
    // Single-Data.db input -> a single manifest OBJECT (design D5 shape).
    let losses = manifest
        .get("losses")
        .and_then(|l| l.as_array())
        .expect("manifest missing 'losses' array");
    assert!(
        !losses.is_empty(),
        "expected losses.length > 0; manifest={manifest}"
    );
    for loss in losses {
        assert_eq!(
            loss.get("class").and_then(|c| c.as_str()),
            Some("chunk-crc"),
            "every loss from this fixture must classify chunk-crc; loss={loss}"
        );
    }

    // Exit 3 (partial recovery) writes a generation set; exit 2 (this
    // fixture's actual outcome) writes none — assert whichever applies.
    if code == Some(3) {
        let out_table_dir = discover_output_table_dir(&out);
        assert!(
            std::fs::read_dir(&out_table_dir)
                .map(|rd| rd
                    .flatten()
                    .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db")))
                .unwrap_or(false),
            "expected a Data.db under --out despite the losses"
        );
    } else {
        assert!(
            !out.exists()
                || std::fs::read_dir(&out)
                    .map(|rd| walk_no_data_db(&out, rd))
                    .unwrap_or(true),
            "--out must contain no Data.db when everything was lost"
        );
    }
}

/// R7.3 — a damaged boundary source refuses: exit 2, stderr names the
/// refusal + `rebuild` remedy, no `Data.db` under `--out`.
#[test]
fn refusal_exit_2_no_data_db_written() {
    let Some(root) = datasets_root() else {
        skip_or_require("salvage_cli_tests R7.3", "CQLITE_DATASETS_ROOT not set");
        return;
    };
    let corrupt_dir = root.join("corruption/test_comp_corrupt/index_db_bit_flip_big");
    if !usable(&corrupt_dir) {
        skip_or_require(
            "index_db_bit_flip_big fixture",
            &format!("{corrupt_dir:?} not usable"),
        );
        return;
    }
    let data_db = single_data_db(&corrupt_dir);
    let schema = schemas_dir().join("compression-parity.cql");
    let temp = TempDir::new().expect("tempdir");
    let out = temp.path().join("out");

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "salvage",
        data_db.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit 2; stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("boundary-source-unreadable"),
        "stderr must name the refusal reason (the manifest's kebab-case spelling, spec R7.3); \
         got: {stderr}"
    );
    assert!(
        stderr.contains("rebuild"),
        "stderr must name the rebuild remedy; got: {stderr}"
    );
    assert!(
        !out.exists()
            || std::fs::read_dir(&out)
                .map(|rd| walk_no_data_db(&out, rd))
                .unwrap_or(true),
        "--out must contain no Data.db after a refusal"
    );
}

/// R7.4 — usage errors exit 1 with the cause on stderr, nothing written.
#[test]
fn usage_errors_exit_1() {
    let Some(root) = datasets_root() else {
        skip_or_require("salvage_cli_tests R7.4", "CQLITE_DATASETS_ROOT not set");
        return;
    };
    let corrupt_dir = root.join("corruption/test_comp_corrupt/data_db_bit_flip");
    if !usable(&corrupt_dir) {
        skip_or_require(
            "data_db_bit_flip fixture",
            &format!("{corrupt_dir:?} not usable"),
        );
        return;
    }
    let data_db = single_data_db(&corrupt_dir);
    let schema = schemas_dir().join("compression-parity.cql");

    // (a) --out is a non-empty dir.
    let temp = TempDir::new().expect("tempdir");
    let out = temp.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("stray.txt"), b"not empty").unwrap();
    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "salvage",
        data_db.to_str().unwrap(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(1), "non-empty --out must exit 1");
    assert!(
        !String::from_utf8_lossy(&output.stderr).is_empty(),
        "stderr must name the cause"
    );

    // (b) no --schema resolves.
    let temp2 = TempDir::new().expect("tempdir");
    let out2 = temp2.path().join("out");
    let output2 = run_cli(&[
        "salvage",
        data_db.to_str().unwrap(),
        "--out",
        out2.to_str().unwrap(),
    ]);
    assert_eq!(
        output2.status.code(),
        Some(1),
        "missing --schema must exit 1"
    );
    assert!(
        !out2.exists()
            || std::fs::read_dir(&out2)
                .map(|mut rd| rd.next().is_none())
                .unwrap_or(true),
        "nothing should be written on a usage error"
    );

    // (c) input dir has no Data.db.
    let temp3 = TempDir::new().expect("tempdir");
    let empty_input = temp3.path().join("empty");
    std::fs::create_dir_all(&empty_input).unwrap();
    let out3 = temp3.path().join("out");
    let output3 = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "salvage",
        empty_input.to_str().unwrap(),
        "--out",
        out3.to_str().unwrap(),
    ]);
    assert_eq!(
        output3.status.code(),
        Some(1),
        "input dir with no Data.db must exit 1"
    );
}

fn single_data_db(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with("-Data.db") {
            found.push(e.path());
        }
    }
    match found.len() {
        1 => found.pop().unwrap(),
        n => panic!("{dir:?}: expected exactly ONE Data.db, found {n} ({found:?})"),
    }
}

/// `--out` names the raw root passed on the command line; the writer nests
/// `<out>/<keyspace>/<table>/`. Walk down to find it.
fn discover_output_table_dir(out_root: &Path) -> PathBuf {
    let ks_dir = std::fs::read_dir(out_root)
        .unwrap_or_else(|e| panic!("read {out_root:?}: {e}"))
        .flatten()
        .find(|e| e.path().is_dir())
        .unwrap_or_else(|| panic!("no keyspace dir under {out_root:?}"))
        .path();
    std::fs::read_dir(&ks_dir)
        .unwrap_or_else(|e| panic!("read {ks_dir:?}: {e}"))
        .flatten()
        .find(|e| e.path().is_dir())
        .unwrap_or_else(|| panic!("no table dir under {ks_dir:?}"))
        .path()
}

fn walk_no_data_db(dir: &Path, rd: std::fs::ReadDir) -> bool {
    for e in rd.flatten() {
        let path = e.path();
        if path.is_dir() {
            if let Ok(sub) = std::fs::read_dir(&path) {
                if !walk_no_data_db(&path, sub) {
                    return false;
                }
            }
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with("-Data.db"))
            .unwrap_or(false)
        {
            return false;
        }
    }
    let _ = dir;
    true
}
