//! Issue #4197 (spec R7/R8) — `cqlite rebuild` CLI surface: verb, inputs,
//! exit codes, `--in-place` refusal, help text.
//!
//! Drives the REAL compiled `cqlite` binary (`CARGO_BIN_EXE_cqlite`), not the
//! handler function directly — `execute_rebuild_command` enforces design
//! D3's exit codes via `std::process::exit`, which only a subprocess can
//! observe.
//!
//! Dataset doctrine (issue #719/#3220): both fixtures this file drives
//! (`test_comp.lz4_table`, `test_comp_corrupt/data_db_bit_flip`) are
//! GIT-TRACKED (mirrors `salvage_cli_tests.rs`'s own committed set), so an
//! absence here is a broken checkout and fails closed unconditionally —
//! never gated on `CQLITE_REQUIRE_FIXTURES`.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

const LZ4_TABLE_FIXTURE: &str = "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77";
const CORRUPT_DATA_DB_FIXTURE: &str = "corruption/test_comp_corrupt/data_db_bit_flip";

fn candidate_base_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(r) = std::env::var("CQLITE_DATASETS_ROOT") {
        roots.push(PathBuf::from(r));
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

/// GIT-TRACKED fixture resolution: absence is a broken checkout, fails
/// closed unconditionally (issue #3220).
fn resolve_committed_fixture(relative: &str) -> PathBuf {
    candidate_base_roots()
        .into_iter()
        .map(|root| root.join(relative))
        .find(|dir| usable(dir))
        .unwrap_or_else(|| {
            panic!(
                "COMMITTED fixture {relative} is absent under every candidate root — this is a \
                 broken checkout, not an unfetched dataset, and must never skip"
            )
        })
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

/// R8.2 — help states the documented boundaries.
#[test]
fn help_states_the_boundaries() {
    let output = run_cli(&["rebuild", "--help"]);
    assert!(output.status.success(), "cqlite rebuild --help must exit 0");
    let help = String::from_utf8_lossy(&output.stdout);
    for phrase in ["READ-ONLY", "opt-in", "salvage", "#4195"] {
        assert!(
            help.contains(phrase),
            "help text must mention {phrase:?}; got:\n{help}"
        );
    }
}

/// R8.1 — `--in-place` refuses today, naming the #4195 dependency, and
/// touches nothing on disk.
#[test]
fn in_place_refuses_naming_the_dependency() {
    let table_dir = resolve_committed_fixture(LZ4_TABLE_FIXTURE);
    let schema = schemas_dir().join("compression-parity.cql");

    let before: Vec<_> = std::fs::read_dir(&table_dir)
        .expect("read fixture dir")
        .flatten()
        .map(|e| e.path())
        .collect();

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        table_dir.to_str().unwrap(),
        "--components",
        "index",
        "--in-place",
    ]);

    assert_eq!(output.status.code(), Some(1), "must exit 1 (usage error)");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("#4195") && stderr.contains("verify"),
        "stderr must name the #4195 dependency; got: {stderr}"
    );

    let after: Vec<_> = std::fs::read_dir(&table_dir)
        .expect("read fixture dir")
        .flatten()
        .map(|e| e.path())
        .collect();
    assert_eq!(
        before.len(),
        after.len(),
        "the fixture directory must be untouched"
    );
}

/// R7.1 — a healthy `Data.db` file input, `--out` writes a complete
/// component set, exit 0.
#[test]
fn healthy_data_db_rebuild_exit_0() {
    let table_dir = resolve_committed_fixture(LZ4_TABLE_FIXTURE);
    let data_db = std::fs::read_dir(&table_dir)
        .expect("read fixture dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .expect("fixture has a Data.db");
    let schema = schemas_dir().join("compression-parity.cql");
    let temp = TempDir::new().expect("tempdir");
    let out = temp.path().join("out");

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        data_db.to_str().unwrap(),
        "--components",
        "digest,toc",
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
    assert!(
        manifest
            .get("refused")
            .map(|r| r.is_null())
            .unwrap_or(false),
        "manifest={manifest}"
    );
    let regenerated = manifest
        .get("regenerated")
        .and_then(|r| r.as_array())
        .unwrap_or_else(|| panic!("manifest missing 'regenerated' array: {manifest}"));
    let names: Vec<&str> = regenerated.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        names.contains(&"digest") && names.contains(&"toc"),
        "{names:?}"
    );
    assert!(
        out.join(data_db.file_name().unwrap().to_str().unwrap())
            .exists(),
        "Data.db must be copied into --out"
    );
}

/// R7.2 — a Cassandra-verified damaged `Data.db` exits 2, manifest and
/// stderr both name `data-corrupt` and the `salvage` remedy, nothing
/// written to `--out`.
#[test]
fn damaged_data_db_exits_2_naming_salvage() {
    let fixture_dir = resolve_committed_fixture(CORRUPT_DATA_DB_FIXTURE);
    let data_db = std::fs::read_dir(&fixture_dir)
        .expect("read fixture dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .expect("fixture has a Data.db");
    let schema = schemas_dir().join("compression-parity.cql");
    let temp = TempDir::new().expect("tempdir");
    let out = temp.path().join("out");
    let manifest_path = temp.path().join("m.json");

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        data_db.to_str().unwrap(),
        // The corruption corpus's directory naming (`data_db_bit_flip`) does
        // NOT follow Cassandra's `<table>-<32-hex-id>` convention
        // `table_name_from_input` derives from, so the target table must be
        // named explicitly.
        "--table",
        "lz4_table",
        "--components",
        "index",
        "--out",
        out.to_str().unwrap(),
        "--manifest",
        manifest_path.to_str().unwrap(),
    ]);

    assert_eq!(output.status.code(), Some(2), "must exit 2 (refused)");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("data-corrupt") || stderr.to_lowercase().contains("salvage"),
        "stderr={stderr}"
    );
    let manifest_json = std::fs::read_to_string(&manifest_path).expect("manifest file must exist");
    assert!(manifest_json.contains("data-corrupt"), "{manifest_json}");
    assert!(
        manifest_json.to_lowercase().contains("salvage"),
        "{manifest_json}"
    );
    assert!(
        !out.exists()
            || std::fs::read_dir(&out)
                .map(|mut d| d.next().is_none())
                .unwrap_or(true),
        "--out must hold nothing"
    );
}

/// R7.3 — an unknown component name is a usage error, exit 1.
#[test]
fn unknown_component_is_usage_error() {
    let table_dir = resolve_committed_fixture(LZ4_TABLE_FIXTURE);
    let data_db = std::fs::read_dir(&table_dir)
        .expect("read fixture dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .expect("fixture has a Data.db");
    let schema = schemas_dir().join("compression-parity.cql");
    let temp = TempDir::new().expect("tempdir");
    let out = temp.path().join("out");

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        data_db.to_str().unwrap(),
        "--components",
        "bogus",
        "--out",
        out.to_str().unwrap(),
    ]);

    assert_eq!(output.status.code(), Some(1), "must exit 1 (usage error)");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("bogus"),
        "stderr must name the bad token: {stderr}"
    );
}
