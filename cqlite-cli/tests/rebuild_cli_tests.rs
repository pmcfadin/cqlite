//! Issue #4197 (spec R7/R8) — `cqlite rebuild` CLI surface: verb, inputs,
//! exit codes, `--in-place` refusal, help text.
//!
//! Drives the REAL compiled `cqlite` binary (`CARGO_BIN_EXE_cqlite`), not the
//! handler function directly — `execute_rebuild_command` enforces design
//! D3's exit codes via `std::process::exit`, which only a subprocess can
//! observe.
//!
//! Dataset doctrine (issue #719/#3220), PER FIXTURE (roborev finding —
//! the two are NOT symmetric): `test_comp.lz4_table`'s whole `nb-1-big-*`
//! component set is GIT-TRACKED, so its absence is a broken checkout and
//! fails closed unconditionally. `test_comp_corrupt/data_db_bit_flip`'s
//! `Data.db` is NOT tracked (`git ls-files` shows only `Digest.crc32` and
//! `TOC.txt` committed) — it needs the FETCHED corpus, so it SKIPS when
//! absent (mirrors `salvage_cli_tests.rs`'s own `resolve_fixture`), turning
//! into a hard failure only under `CQLITE_REQUIRE_FIXTURES=1`.

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

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// FETCHED-corpus-only fixture resolution (unlike [`resolve_committed_fixture`]):
/// `None` when absent from every candidate root, unless
/// `CQLITE_REQUIRE_FIXTURES=1` — mirrors `salvage_cli_tests.rs`'s
/// `resolve_fixture` (roborev finding — `data_db_bit_flip`'s `Data.db` is
/// NOT git-tracked, so treating it as committed made this test panic on
/// any checkout that has not run `fetch-datasets.sh`).
fn resolve_fixture_or_skip(relative: &str) -> Option<PathBuf> {
    let found = candidate_base_roots()
        .into_iter()
        .map(|root| root.join(relative))
        .find(|dir| usable(dir));
    if found.is_none() && require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but fixture {relative} is absent under every candidate \
             root"
        );
    }
    found
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
    let Some(fixture_dir) = resolve_fixture_or_skip(CORRUPT_DATA_DB_FIXTURE) else {
        eprintln!(
            "[SKIP] corruption fixture {CORRUPT_DATA_DB_FIXTURE} unavailable (dataset not \
             fetched)"
        );
        return;
    };
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
        // NOT follow Cassandra's `<table>-<32-hex-id>` convention, so
        // `table_name_from_input` returns `None` (issue #4197 F4 — it used
        // to fall back to the bare directory name and invent the table
        // `data_db_bit_flip`) and omitting `--table` here would be a USAGE
        // error, exit 1, never the exit 2 this test is about. Naming the
        // table explicitly is therefore required, not merely tidier; the
        // `None` path itself is covered by
        // `underivable_table_name_without_table_flag_is_usage_error`.
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

/// `--out` resolving INSIDE the input tree is refused (usage error), never
/// silently allowed to overwrite a component in place — the bypass route
/// `--in-place`'s own refusal exists to close.
#[test]
fn out_inside_input_tree_refuses() {
    let table_dir = resolve_committed_fixture(LZ4_TABLE_FIXTURE);
    let data_db = std::fs::read_dir(&table_dir)
        .expect("read fixture dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .expect("fixture has a Data.db");
    let schema = schemas_dir().join("compression-parity.cql");
    let out = table_dir.join("nested-out");

    let before: Vec<_> = std::fs::read_dir(&table_dir)
        .expect("read fixture dir")
        .flatten()
        .map(|e| e.path())
        .collect();

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        data_db.to_str().unwrap(),
        "--components",
        "digest",
        "--out",
        out.to_str().unwrap(),
    ]);

    assert_eq!(output.status.code(), Some(1), "must exit 1 (usage error)");
    assert!(
        !out.exists(),
        "a refused --out must never be created on disk"
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

/// The real component set of a generation (excludes the corpus's `.jsonl` /
/// `.txt` sidecars, which are documentation, not SSTable components).
fn is_component_file(name: &str) -> bool {
    name.ends_with(".db")
        || name.ends_with("-TOC.txt")
        || name.ends_with("-Digest.crc32")
        || name.ends_with("-CRC.db")
}

/// Build a TABLE DIRECTORY holding `generations` copies of the committed lz4
/// fixture's component set, one per generation number, named exactly like a
/// real Cassandra table directory (`<table>-<32-hex-id>`) so the `--table`
/// derivation applies.
///
/// A generation is a pure FILENAME property (`<version>-<generation>-<format>-`)
/// — nothing inside any component encodes the generation number — so renaming
/// a verbatim copy produces a genuinely valid second generation. That matters
/// for what follows: `discover_generations` orders by that parsed number, so
/// generation 1 is always rebuilt before generation 2.
fn multi_generation_table_dir(root: &Path, generations: u32) -> PathBuf {
    let dir = root.join("lz4_table-25801a0071a911f19b3225f9984c6a77");
    std::fs::create_dir_all(&dir).expect("create table dir");
    for generation in 1..=generations {
        copy_generation_into(&dir, generation);
    }
    dir
}

/// Copy the committed lz4 fixture's whole component set into `dir`, renamed to
/// generation `generation`. Shared by [`multi_generation_table_dir`] and the
/// per-generation REFERENCE directories R10.1 reads back against.
fn copy_generation_into(dir: &Path, generation: u32) {
    let src = resolve_committed_fixture(LZ4_TABLE_FIXTURE);
    std::fs::create_dir_all(dir).expect("create generation dir");
    for entry in std::fs::read_dir(&src).expect("read fixture dir").flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !is_component_file(&name) {
            continue;
        }
        let renamed = name.replacen("nb-1-", &format!("nb-{generation}-"), 1);
        std::fs::copy(entry.path(), dir.join(renamed)).expect("copy component");
    }
}

/// Corrupt `Data.db` for one generation so the compressed-chunk pre-flight
/// refuses it (spec R5.1): flip every bit of one byte in the middle of the
/// compressed stream.
fn corrupt_generation_data_db(table_dir: &Path, generation: u32) {
    let path = table_dir.join(format!("nb-{generation}-big-Data.db"));
    let mut bytes = std::fs::read(&path).expect("read Data.db");
    assert!(bytes.len() > 64, "fixture Data.db is implausibly small");
    let at = bytes.len() / 2;
    bytes[at] ^= 0xFF;
    std::fs::write(&path, bytes).expect("write corrupted Data.db");
}

/// R7.1 — a TABLE DIRECTORY input rebuilds EVERY generation separately: one
/// output subdirectory per generation, and an ARRAY-shaped manifest with one
/// entry per generation.
///
/// This is the CLI's headline capability and no other test in this file
/// passes a DIRECTORY as the input at all (every other one resolves down to a
/// single `*-Data.db` first), so `discover_generations`, the per-generation
/// `<out>/<base>` naming and the array-vs-object manifest shape were
/// completely dark (issue #4197 F3).
#[test]
fn table_directory_rebuilds_every_generation() {
    let temp = TempDir::new().expect("tempdir");
    let table_dir = multi_generation_table_dir(temp.path(), 2);
    let schema = schemas_dir().join("compression-parity.cql");
    let out = temp.path().join("out");
    let manifest_path = temp.path().join("m.json");

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        table_dir.to_str().unwrap(),
        "--components",
        "digest,toc",
        "--out",
        out.to_str().unwrap(),
        "--manifest",
        manifest_path.to_str().unwrap(),
    ]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "expected exit 0; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    // (a) one output subdirectory per generation, named from the Data.db
    // base (`trim_end_matches("-Data.db")`).
    for generation in [1, 2] {
        let gen_out = out.join(format!("nb-{generation}-big"));
        assert!(
            gen_out.is_dir(),
            "generation {generation} must get its own output subdirectory; --out holds {:?}",
            std::fs::read_dir(&out)
                .map(|rd| rd.flatten().map(|e| e.file_name()).collect::<Vec<_>>())
                .unwrap_or_default()
        );
        assert!(
            gen_out
                .join(format!("nb-{generation}-big-Digest.crc32"))
                .exists(),
            "generation {generation}'s regenerated Digest.crc32 must be in its own subdirectory"
        );
    }

    // (b) ARRAY-shaped manifest, one entry per generation.
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).expect("manifest"))
            .expect("manifest is JSON");
    let entries = manifest
        .as_array()
        .unwrap_or_else(|| panic!("a table-dir manifest must be an ARRAY; got {manifest}"));
    assert_eq!(entries.len(), 2, "one entry per generation; got {manifest}");
    for entry in entries {
        assert!(entry["refused"].is_null(), "{entry}");
        assert_eq!(entry["rolled_back"], serde_json::json!(false), "{entry}");
        // (c) spec R9's consumer contract: every non-refused entry's
        // `output` names a path that really exists.
        let output_path = entry["output"].as_str().expect("output string");
        assert!(
            Path::new(output_path).is_dir(),
            "manifest names a non-existent output path {output_path}"
        );
    }
}

/// R7.1 + F2 — when a LATER generation refuses, the whole run exits 2, the
/// EARLIER generation's output is rolled back, and the manifest never names a
/// surviving path for it.
#[test]
fn table_directory_refusal_rolls_back_earlier_generations() {
    let temp = TempDir::new().expect("tempdir");
    let table_dir = multi_generation_table_dir(temp.path(), 2);
    corrupt_generation_data_db(&table_dir, 2);
    let schema = schemas_dir().join("compression-parity.cql");
    let out = temp.path().join("out");
    let manifest_path = temp.path().join("m.json");

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        table_dir.to_str().unwrap(),
        "--components",
        "digest,toc",
        "--out",
        out.to_str().unwrap(),
        "--manifest",
        manifest_path.to_str().unwrap(),
    ]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "a refused generation must exit 2; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Generation 1 completed BEFORE generation 2 refused, so its output must
    // have been removed again — design D3's "nothing written" for exit 2.
    assert!(
        !out.join("nb-1-big").exists(),
        "generation 1's output must be rolled back when a later generation refuses"
    );

    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).expect("manifest"))
            .expect("manifest is JSON");
    let entries = manifest
        .as_array()
        .unwrap_or_else(|| panic!("a table-dir manifest must be an ARRAY; got {manifest}"));
    // Non-vacuity: both generations must appear (the successful-then-rolled-
    // back one AND the refusing one), otherwise the assertion below passes
    // because there is nothing to check.
    assert_eq!(entries.len(), 2, "{manifest}");
    let refused: Vec<_> = entries.iter().filter(|e| !e["refused"].is_null()).collect();
    assert_eq!(refused.len(), 1, "exactly one entry refused; {manifest}");

    for entry in entries.iter().filter(|e| e["refused"].is_null()) {
        assert_eq!(
            entry["rolled_back"],
            serde_json::json!(true),
            "a non-refused entry whose output was removed MUST be marked rolled_back, or spec \
             R9's `select(.refused==null) | .output` names a deleted directory; {entry}"
        );
        assert_eq!(
            entry["regenerated"],
            serde_json::json!([]),
            "a rolled-back entry must claim no regenerated components; {entry}"
        );
        assert!(
            !Path::new(entry["output"].as_str().expect("output string")).exists(),
            "sanity: the rolled-back entry's output really is gone; {entry}"
        );
    }
}

/// R7.3 + F4 — a directory whose name does NOT follow Cassandra's
/// `<table>-<32-hex-id>` convention carries no derivable table name, so
/// omitting `--table` is a usage error that SAYS SO, rather than silently
/// deriving the bare directory name and failing later on an invented table.
#[test]
fn underivable_table_name_without_table_flag_is_usage_error() {
    let temp = TempDir::new().expect("tempdir");
    let table_dir = multi_generation_table_dir(temp.path(), 1);
    // Rename the directory to something that does not carry a table id.
    let odd = temp.path().join("not_a_table_dir");
    std::fs::rename(&table_dir, &odd).expect("rename table dir");
    let schema = schemas_dir().join("compression-parity.cql");
    let out = temp.path().join("out");

    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        odd.to_str().unwrap(),
        "--components",
        "digest",
        "--out",
        out.to_str().unwrap(),
    ]);

    assert_eq!(output.status.code(), Some(1), "must exit 1 (usage error)");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--table") && stderr.contains("could not derive a table name"),
        "stderr must name the actionable flag rather than an invented table: {stderr}"
    );
    assert!(!out.exists(), "a usage error must not create --out");
}

// ---------------------------------------------------------------------------
// R10.1 — verify + read-back parity (tasks.md 4.5)
// ---------------------------------------------------------------------------

/// The DERIVED components R10.1 deletes before rebuilding. `Statistics.db` and
/// `CompressionInfo.db` are deliberately LEFT in place: `statistics` is opt-in
/// (spec R4.4) and the original `Statistics.db`'s `SerializationHeader` is the
/// authoritative `EncodingStats` baseline an `index` rebuild must take (spec
/// R2) — deleting it would move this test onto R2.5's refusal path instead of
/// R10.1's success path.
const DERIVED_SUFFIXES: [&str; 5] = [
    "Index.db",
    "Summary.db",
    "Filter.db",
    "Digest.crc32",
    "TOC.txt",
];

/// Delete every [`DERIVED_SUFFIXES`] component of generation `generation` from
/// `table_dir`, asserting each one really existed first (a delete that removed
/// nothing would make the whole rebuild vacuous).
fn delete_derived_components(table_dir: &Path, generation: u32) {
    for suffix in DERIVED_SUFFIXES {
        let path = table_dir.join(format!("nb-{generation}-big-{suffix}"));
        assert!(
            path.is_file(),
            "fixture must carry {suffix} for generation {generation} before deletion: {}",
            path.display()
        );
        std::fs::remove_file(&path).unwrap_or_else(|e| panic!("delete {}: {e}", path.display()));
    }
}

/// Run `cqlite verify --mode full --out json` over `dir`, returning its exit
/// code and parsed report.
fn verify_full(dir: &Path, schema: &Path) -> (Option<i32>, serde_json::Value) {
    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "verify",
        dir.to_str().unwrap(),
        "--mode",
        "full",
        "--out",
        "json",
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let report: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "verify --out json must emit JSON on stdout: {e}\nstdout={stdout}\nstderr={}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code(), report)
}

/// Read every row of `data_db` back through `cqlite read-sstable --format
/// json`, with the PATH-DERIVED `table_id` field removed.
///
/// `table_id` is not SSTable content: `read-sstable` synthesises it from the
/// input file's own filesystem path (empirically `<grandparent>.<parent>` —
/// `.../r10/orig/nb-1-big-Data.db` renders `r10.orig`). A rebuilt generation
/// necessarily lives at a DIFFERENT path from the original it is compared
/// against, so leaving it in would compare this test's own directory layout
/// rather than the data. Every content-bearing field (`key`, `value`) is
/// compared verbatim, and the stripping is asserted non-vacuous: `table_id`
/// must have been PRESENT in each row object.
fn read_sstable_rows(data_db: &Path, schema: &Path) -> Vec<serde_json::Value> {
    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "read-sstable",
        data_db.to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "read-sstable must exit 0 for {}; stderr={}",
        data_db.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let rows: Vec<serde_json::Value> = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("read-sstable --format json must emit a JSON array: {e}"));
    rows.into_iter()
        .map(|row| {
            let rendered = row.to_string();
            let serde_json::Value::Object(mut obj) = row else {
                panic!("read-sstable row must be an object; got {rendered}")
            };
            assert!(
                obj.remove("table_id").is_some(),
                "a read-sstable row must carry the path-derived table_id this helper strips \
                 (its absence would make the normalisation silently vacuous); row={rendered}"
            );
            serde_json::Value::Object(obj)
        })
        .collect()
}

/// R10.1 — every generation the R7.1 table-directory run writes passes
/// `verify --mode full` with ZERO findings, and reads back row-for-row
/// identical to the ORIGINAL (pre-deletion) component set.
///
/// ## Why BOTH oracles, and which one carries which half
///
/// `read-sstable` performs a full `Data.db` scan and is MEASURABLY BLIND to
/// `Index.db` (verified while writing this test: a rebuilt output whose
/// `Index.db` had its first 40 bytes bit-flipped still produced byte-identical
/// `read-sstable --format json` output). On its own it would therefore prove
/// only that `Data.db` was copied intact — not that the rebuilt index serves
/// reads. `verify --mode full` is the oracle that CAN see an index defect: it
/// parses every `Index.db` entry and reports `IndexEntryCorrupt`. The negative
/// control below pins exactly that asymmetry, so neither half can quietly
/// become decorative (CLAUDE.md: "pick the oracle that can see your defect").
#[test]
fn rebuilt_generations_verify_full_and_read_back_identically() {
    let temp = TempDir::new().expect("tempdir");
    let schema = schemas_dir().join("compression-parity.cql");

    // (1) The INPUT: a 2-generation table dir (R7.1's shape) with every
    // derived component deleted.
    let input_dir = multi_generation_table_dir(&temp.path().join("input"), 2);
    for generation in [1, 2] {
        delete_derived_components(&input_dir, generation);
    }

    // (2) The REFERENCE: one untouched single-generation directory per
    // generation, holding the full Cassandra-written component set. This is
    // the pre-deletion read-back oracle; it is never passed to `rebuild`.
    let reference: Vec<PathBuf> = [1u32, 2u32]
        .iter()
        .map(|generation| {
            let dir = temp.path().join(format!("reference/gen{generation}"));
            copy_generation_into(&dir, *generation);
            dir
        })
        .collect();

    // (3) Rebuild every generation into --out.
    let out = temp.path().join("out");
    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        input_dir.to_str().unwrap(),
        "--components",
        "index,summary,filter,digest,toc",
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "rebuild of a healthy 2-generation table dir must exit 0; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    for (idx, generation) in [1u32, 2u32].iter().enumerate() {
        let gen_out = out.join(format!("nb-{generation}-big"));
        assert!(
            gen_out.is_dir(),
            "generation {generation} must have its own output dir"
        );

        // (4a) `verify --mode full` on the REBUILT generation: clean.
        let (code, report) = verify_full(&gen_out, &schema);
        let findings = report["findings"]
            .as_array()
            .unwrap_or_else(|| panic!("verify report must carry a findings array: {report}"));
        assert_eq!(
            code,
            Some(0),
            "verify --mode full must exit 0 on a rebuilt generation; report={report}"
        );
        assert_eq!(report["ok"], serde_json::json!(true), "report={report}");
        assert!(
            findings.is_empty(),
            "verify --mode full must report ZERO findings on a rebuilt generation; report={report}"
        );
        // Non-vacuity: a verify that scanned nothing proves nothing.
        let (ref_code, ref_report) = verify_full(&reference[idx], &schema);
        assert_eq!(ref_code, Some(0), "reference verify: {ref_report}");
        let rows_scanned = report["rows_scanned"].as_u64().unwrap_or_default();
        assert!(
            rows_scanned > 0,
            "verify --mode full scanned 0 rows — a vacuous clean report; report={report}"
        );
        assert_eq!(
            rows_scanned,
            ref_report["rows_scanned"].as_u64().unwrap_or_default(),
            "the rebuilt generation must scan the SAME row count as the original component set; \
             rebuilt={report} original={ref_report}"
        );
        // Every requested component must be back in the rebuilt TOC.txt.
        let toc: Vec<String> = report["toc_components"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        for suffix in DERIVED_SUFFIXES {
            assert!(
                toc.iter().any(|c| c == suffix),
                "the rebuilt TOC.txt must name {suffix}; toc={toc:?}"
            );
        }

        // (4b) Read-back parity against the ORIGINAL component set.
        let rebuilt_rows = read_sstable_rows(
            &gen_out.join(format!("nb-{generation}-big-Data.db")),
            &schema,
        );
        let original_rows = read_sstable_rows(
            &reference[idx].join(format!("nb-{generation}-big-Data.db")),
            &schema,
        );
        assert!(
            !original_rows.is_empty(),
            "the ORIGINAL component set read back 0 rows — a vacuous parity pass"
        );
        assert_eq!(
            rebuilt_rows.len(),
            original_rows.len(),
            "row count differs between the rebuilt and the original component set"
        );
        assert_eq!(
            rebuilt_rows, original_rows,
            "generation {generation}: the rebuilt component set must read back row-for-row \
             identical to the original Cassandra-written one"
        );
    }
}

/// R10.1 negative control — the `verify --mode full` half of the test above is
/// the ONLY half that can see an `Index.db` defect, and it really does.
///
/// Flipping the first 40 bytes of a rebuilt `Index.db`:
///   - `verify --mode full` exits non-zero with an `Index.db` finding, and
///   - `read-sstable` output is UNCHANGED (full `Data.db` scan, index-blind).
///
/// Without this control, a future change that stopped parsing `Index.db` in
/// FULL mode would leave `rebuilt_generations_verify_full_and_read_back_identically`
/// green while proving nothing about the rebuilt index.
#[test]
fn verify_full_is_the_oracle_that_sees_a_broken_rebuilt_index() {
    let temp = TempDir::new().expect("tempdir");
    let schema = schemas_dir().join("compression-parity.cql");

    let input_dir = multi_generation_table_dir(&temp.path().join("input"), 1);
    delete_derived_components(&input_dir, 1);
    let out = temp.path().join("out");
    let output = run_cli(&[
        "--schema",
        schema.to_str().unwrap(),
        "rebuild",
        input_dir.to_str().unwrap(),
        "--components",
        "index,summary,filter,digest,toc",
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "rebuild must exit 0; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    // A single-generation input writes a FLAT --out (no per-generation
    // subdirectory) — `discover_generations` found exactly one Data.db.
    let data_db = out.join("nb-1-big-Data.db");
    assert!(data_db.is_file(), "single-generation --out must be flat");

    let clean_rows = read_sstable_rows(&data_db, &schema);
    let (clean_code, clean_report) = verify_full(&out, &schema);
    assert_eq!(
        clean_code,
        Some(0),
        "baseline must be clean: {clean_report}"
    );

    // Break the REBUILT Index.db.
    let index_path = out.join("nb-1-big-Index.db");
    let mut index_bytes = std::fs::read(&index_path).expect("read rebuilt Index.db");
    assert!(
        index_bytes.len() >= 40,
        "rebuilt Index.db is implausibly small: {} bytes",
        index_bytes.len()
    );
    for byte in index_bytes.iter_mut().take(40) {
        *byte ^= 0xFF;
    }
    std::fs::write(&index_path, &index_bytes).expect("write corrupted Index.db");

    let (broken_code, broken_report) = verify_full(&out, &schema);
    assert_ne!(
        broken_code,
        Some(0),
        "verify --mode full must FAIL on a broken Index.db — otherwise the read-back parity test \
         above proves nothing about the rebuilt index; report={broken_report}"
    );
    assert_eq!(broken_report["ok"], serde_json::json!(false));
    let findings = broken_report["findings"]
        .as_array()
        .unwrap_or_else(|| panic!("findings array: {broken_report}"));
    assert!(
        findings
            .iter()
            .any(|f| f["component"].as_str() == Some("Index.db")),
        "verify must attribute the finding to Index.db; report={broken_report}"
    );

    // And the documented blindness: `read-sstable` cannot see it.
    assert_eq!(
        read_sstable_rows(&data_db, &schema),
        clean_rows,
        "read-sstable is expected to be INDEX-BLIND (full Data.db scan). If this assertion ever \
         fails, read-sstable has gained index sensitivity and the parity test above can be \
         strengthened to rely on it directly — update both together."
    );
}
