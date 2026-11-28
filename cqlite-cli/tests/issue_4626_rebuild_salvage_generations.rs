//! Issue #4626 phase 4 (tasks 4.1 and 4.2; GD2, GD4, GD10): `cqlite rebuild` and `cqlite salvage`
//! list their table-directory input through the generation module.
//!
//! Every case drives the BUILT BINARY. Oracles: the Cassandra 5.0.8-written fixtures in
//! `test-data/fixtures/issue_4626` (`snap_backup`, `uuid_ids`, `txn_inconsistent`), whose
//! `Digest.crc32` files and UUID ids are Cassandra's own bytes. A fixture that cannot be found
//! PANICS (#3220): no case skips. Every mutation (a deleted `TOC.txt`, a zero-length `Data.db`, a
//! placeholder `ma-1-big` descriptor) is applied to a tempdir COPY; the shared fixtures are never
//! touched and no SSTable byte is hand-written.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

#[path = "../../cqlite-core/tests/support/datasets_root.rs"]
mod datasets_root;
#[path = "../../cqlite-core/tests/support/generation_fixtures.rs"]
mod generation_fixtures;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use generation_fixtures::*;
use serde_json::Value;
use tempfile::TempDir;

fn schema_file(dir: &Path, table: &str) -> PathBuf {
    let p = dir.join(format!("{table}.cql"));
    std::fs::write(
        &p,
        format!("CREATE TABLE gd4626.{table} (id int PRIMARY KEY, v text);\n"),
    )
    .unwrap();
    p
}

fn cqlite(args: &[&std::ffi::OsStr]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(args)
        .output()
        .expect("spawn cqlite")
}

fn os(s: &str) -> &std::ffi::OsStr {
    std::ffi::OsStr::new(s)
}

fn out_text(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn err_text(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

/// A copy of a fixture table directory under `<root>/gd4626/`.
fn seeded(root: &Path, state: &str, table: &str) -> PathBuf {
    let src = fixture_table(state, table);
    let dir = root.join("gd4626").join(src.file_name().unwrap());
    copy_dir(&src, &dir);
    dir
}

/// Delete every file of generation `prefix` (e.g. `nb-2-big`) directly in `dir`.
fn remove_generation(dir: &Path, prefix: &str) {
    let dash = format!("{prefix}-");
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        if e.file_name().to_string_lossy().starts_with(&dash) {
            std::fs::remove_file(e.path()).unwrap();
        }
    }
}

/// `<table>-<id>` holding exactly one generation of `snap_backup` (`nb-1-big`), with the
/// fixture's `snapshots/` and `backups/` left in place.
fn single_generation_with_snapshots(root: &Path) -> PathBuf {
    let dir = seeded(root, "snap_backup", "snap_backup");
    for n in 2..=5 {
        remove_generation(&dir, &format!("nb-{n}-big"));
    }
    assert!(dir.join("snapshots").is_dir() && dir.join("backups").is_dir());
    dir
}

fn rebuild(schema: &Path, dir: &Path, out: &Path, manifest: &Path, components: &str) -> Output {
    cqlite(&[
        os("--schema"),
        schema.as_os_str(),
        os("rebuild"),
        dir.as_os_str(),
        os("--components"),
        os(components),
        os("--out"),
        out.as_os_str(),
        os("--manifest"),
        manifest.as_os_str(),
    ])
}

fn manifest(path: &Path) -> Vec<Value> {
    let v: Value = serde_json::from_str(&std::fs::read_to_string(path).expect("manifest written"))
        .expect("manifest is JSON");
    v.as_array()
        .unwrap_or_else(|| panic!("a table-dir manifest is an ARRAY: {v}"))
        .clone()
}

fn subdirs(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.path().is_dir())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

// ------------------------------------------------------------------------------------- rebuild

/// GD4.1: a below-floor `ma-1-big` beside live `nb` generations refuses the whole run (exit 2).
/// Its manifest entry is `refused` with `unsupported-version`, and nothing is left under `--out`
/// (R7.4): the `nb` generations rebuilt before it are rolled back.
#[test]
fn gd4_1_below_floor_generation_refuses_the_whole_run() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    touch(&dir.join("ma-1-big-Data.db"));
    touch(&dir.join("ma-1-big-TOC.txt"));
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let m = root.path().join("m.json");
    let o = rebuild(&schema, &dir, &out, &m, "digest,toc");
    assert_eq!(o.status.code(), Some(2), "{}", err_text(&o));
    let entries = manifest(&m);
    let refused: Vec<&Value> = entries.iter().filter(|e| !e["refused"].is_null()).collect();
    assert_eq!(refused.len(), 1, "{entries:?}");
    assert_eq!(refused[0]["refused"]["reason"], "unsupported-version");
    assert!(
        refused[0]["input"].as_str().unwrap().contains("ma-1-big"),
        "{:?}",
        refused[0]
    );
    let remedy = refused[0]["refused"]["remedy"].as_str().unwrap();
    assert!(remedy.contains("ma") && remedy.contains("na"), "{remedy}");
    assert!(
        refused[0]["classification"].as_object().unwrap().is_empty(),
        "a refusal is not a classification: {:?}",
        refused[0]
    );
    for e in entries.iter().filter(|e| e["refused"].is_null()) {
        assert_eq!(e["rolled_back"], true, "{e}");
    }
    assert!(
        subdirs(&out).is_empty(),
        "R7.4: no output subdirectory is left, found {:?}",
        subdirs(&out)
    );
}

/// GD4.2: `snapshots/` and `backups/` are never entered. The one live generation is the only
/// manifest entry and the only `Data.db` under `--out`, though both directories hold `nb-1-big`.
#[test]
fn gd4_2_snapshots_and_backups_are_not_rebuilt() {
    let root = TempDir::new().unwrap();
    let dir = single_generation_with_snapshots(root.path());
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let m = root.path().join("m.json");
    let o = rebuild(&schema, &dir, &out, &m, "digest,toc");
    assert_eq!(o.status.code(), Some(0), "{}", err_text(&o));
    let entries = manifest(&m);
    assert_eq!(entries.len(), 1, "{entries:?}");
    let data_dbs: Vec<_> = std::fs::read_dir(&out)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        .collect();
    assert_eq!(data_dbs.len(), 1);
    assert!(subdirs(&out).is_empty(), "{:?}", subdirs(&out));
}

/// GD2.1 through the CLI: a generation whose `TOC.txt` is gone is rebuilt, the manifest records
/// `TocMissing` as its input condition, and the other generations carry no such condition.
#[test]
fn gd2_1_toc_missing_generation_is_rebuilt_and_the_manifest_records_it() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    std::fs::remove_file(dir.join("nb-1-big-TOC.txt")).unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let m = root.path().join("m.json");
    let o = rebuild(&schema, &dir, &out, &m, "digest,toc");
    assert_eq!(o.status.code(), Some(0), "{}", err_text(&o));
    let entries = manifest(&m);
    assert_eq!(entries.len(), 5, "{entries:?}");
    for e in &entries {
        let is_one = e["input"].as_str().unwrap().ends_with("nb-1-big-Data.db");
        if is_one {
            assert_eq!(e["input_conditions"], serde_json::json!(["TocMissing"]));
        } else {
            assert!(e.get("input_conditions").is_none(), "{e}");
        }
    }
    assert!(out.join("nb-1-big").join("nb-1-big-TOC.txt").is_file());
    assert!(err_text(&o).contains("input_conditions: TocMissing"));
}

/// GD2.2 through the CLI on the Cassandra-written UUID corpus: both generations are rebuilt, each
/// output directory and every file in it carries the id text, and the `Digest.crc32` equals
/// Cassandra's.
#[test]
fn gd2_2_uuid_id_generations_keep_their_id_text() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "uuid_ids", "uuid_only");
    let schema = schema_file(root.path(), "uuid_only");
    let out = root.path().join("out");
    let m = root.path().join("m.json");
    let o = rebuild(&schema, &dir, &out, &m, "digest,toc");
    assert_eq!(o.status.code(), Some(0), "{}", err_text(&o));
    assert_eq!(manifest(&m).len(), 2);
    let bases = [
        "nb-3h5t_0i6t_1jigq2lxu6305iz8wl-big",
        "nb-3h5t_0i6u_0xfpc2lxu6305iz8wl-big",
    ];
    assert_eq!(subdirs(&out), bases);
    for base in bases {
        assert_eq!(
            std::fs::read(out.join(base).join(format!("{base}-Digest.crc32"))).unwrap(),
            std::fs::read(dir.join(format!("{base}-Digest.crc32"))).unwrap(),
            "{base}"
        );
    }
}

/// GD2.3 through the CLI: an incomplete transaction log whose inputs are gone refuses the run
/// (exit 1), names the log, and writes nothing.
#[test]
fn gd2_3_txn_inconsistent_directory_is_refused_naming_the_log() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "txn_inconsistent", "txn_compaction");
    let schema = schema_file(root.path(), "txn_compaction");
    let out = root.path().join("out");
    let m = root.path().join("m.json");
    let o = rebuild(&schema, &dir, &out, &m, "digest");
    assert_eq!(o.status.code(), Some(1), "{}", err_text(&o));
    assert!(
        err_text(&o).contains("nb_txn_compaction_"),
        "{}",
        err_text(&o)
    );
    assert!(!out.exists());
}

/// GD4 (Orphans): a zero-length `Data.db` is named in the manifest with its condition and is not
/// rebuilt; the live generations are, and the run still succeeds.
#[test]
fn gd4_orphan_is_reported_in_the_manifest_and_not_rebuilt() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    std::fs::write(dir.join("nb-9-big-Data.db"), b"").unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let m = root.path().join("m.json");
    let o = rebuild(&schema, &dir, &out, &m, "digest,toc");
    assert_eq!(o.status.code(), Some(0), "{}", err_text(&o));
    let entries = manifest(&m);
    assert_eq!(entries.len(), 6, "{entries:?}");
    let orphan = entries
        .iter()
        .find(|e| e["input"].as_str().unwrap().ends_with("nb-9-big-Data.db"))
        .expect("the orphan is in the manifest");
    assert_eq!(orphan["input_conditions"], serde_json::json!(["Orphan"]));
    assert!(orphan["refused"].is_null(), "{orphan}");
    assert_eq!(orphan["regenerated"], serde_json::json!([]));
    // No `output` key at all (an empty string would read as the current directory), while every
    // rebuilt entry still names its output as a string.
    assert!(orphan.get("output").is_none(), "{orphan}");
    for e in entries.iter().filter(|e| e["input_conditions"].is_null()) {
        assert!(e["output"].as_str().is_some_and(|o| !o.is_empty()), "{e}");
    }
    assert!(!out.join("nb-9-big").exists());
    assert_eq!(subdirs(&out).len(), 5, "{:?}", subdirs(&out));
}

// ------------------------------------------------------------------------------------- salvage

fn salvage(schema: &Path, input: &Path, out: &Path) -> Output {
    cqlite(&[
        os("--schema"),
        schema.as_os_str(),
        os("salvage"),
        input.as_os_str(),
        os("--out"),
        out.as_os_str(),
        os("--out-format"),
        os("json"),
    ])
}

fn salvage_manifest(o: &Output) -> Vec<Value> {
    let v: Value = serde_json::from_str(&out_text(o))
        .unwrap_or_else(|e| panic!("stdout is not JSON: {e}\n{}\n{}", out_text(o), err_text(o)));
    v.as_array()
        .expect("table-dir manifest is an array")
        .clone()
}

fn finding_classes(entry: &Value) -> Vec<String> {
    entry["component_findings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["class"].as_str().unwrap().to_string())
        .collect()
}

fn has_data_db(dir: &Path) -> bool {
    walk_names(dir).iter().any(|n| n.ends_with("-Data.db"))
}

fn walk_names(dir: &Path) -> Vec<String> {
    let mut names = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            if e.path().is_dir() {
                names.extend(walk_names(&e.path()));
            } else {
                names.push(e.file_name().to_string_lossy().into_owned());
            }
        }
    }
    names
}

/// A table directory holding only `nb-1-big` of `snap_backup` (no snapshots, no backups).
fn one_generation_dir(root: &Path) -> PathBuf {
    let src = fixture_table("snap_backup", "snap_backup");
    let dir = root.join("gd4626").join(src.file_name().unwrap());
    copy_generation_as(&src, "nb-1-big", &dir, "nb-1-big");
    dir
}

/// GD10.1: a generation whose `TOC.txt` is gone is salvaged (it used to be skipped), recorded in
/// the manifest as a `TocMissing` finding, and the run is imperfect (exit 3) because completeness
/// is unverifiable, exactly as the same file named explicitly.
#[test]
fn gd10_1_toc_missing_generation_is_salvaged_and_recorded() {
    let root = TempDir::new().unwrap();
    let dir = one_generation_dir(root.path());
    std::fs::remove_file(dir.join("nb-1-big-TOC.txt")).unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let o = salvage(&schema, &dir, &out);
    assert_eq!(o.status.code(), Some(3), "{}", err_text(&o));
    let entries = salvage_manifest(&o);
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0]["partitions"]["lost"], 0);
    assert!(
        finding_classes(&entries[0]).contains(&"UnpublishedInputGeneration".to_string()),
        "{:?}",
        entries[0]
    );
    let detail = entries[0]["component_findings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["class"] == "UnpublishedInputGeneration")
        .unwrap()["detail"]
        .as_str()
        .unwrap();
    assert!(detail.contains("TocMissing"), "{detail}");
    assert!(has_data_db(&out), "the generation must be salvaged");
}

/// GD10.2: a `recovered/` directory inside the input tree (a prior run's output) is not an input:
/// Scope Live never recurses, so only the generations directly in the table directory are salvaged.
#[test]
fn gd10_2_a_nested_output_directory_is_not_an_input() {
    let root = TempDir::new().unwrap();
    let dir = one_generation_dir(root.path());
    let src = fixture_table("snap_backup", "snap_backup");
    copy_generation_as(&src, "nb-1-big", &dir.join("recovered"), "nb-1-big");
    copy_generation_as(&src, "nb-1-big", &dir.join("recovered"), "nb-2-big");
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let o = salvage(&schema, &dir, &out);
    assert_eq!(o.status.code(), Some(0), "{}", err_text(&o));
    let entries = salvage_manifest(&o);
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert!(
        !entries[0]["input"].as_str().unwrap().contains("recovered"),
        "{:?}",
        entries[0]
    );
}

/// GD10.3: a UUID-id generation beside sequence generations is refused BY NAME and the run does
/// not proceed with the sequence generations alone: exit 1, nothing written.
#[test]
fn gd10_3_a_uuid_id_generation_refuses_the_whole_run_by_name() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "uuid_ids", "uuid_mixed");
    let schema = schema_file(root.path(), "uuid_mixed");
    let out = root.path().join("out");
    let o = salvage(&schema, &dir, &out);
    assert_eq!(o.status.code(), Some(1), "{}", err_text(&o));
    let err = err_text(&o);
    assert!(err.contains("nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big"), "{err}");
    assert!(err.contains("nb-3h5t_0i6s_1tsu22lxu6305iz8wl-big"), "{err}");
    assert!(err.contains("UUID"), "{err}");
    assert!(!has_data_db(&out), "nothing may be salvaged: {err}");
    assert!(out_text(&o).trim().is_empty(), "{}", out_text(&o));
}

/// GD10 (below the floor): `ma-1-big` is refused as `UnsupportedVersion` and never read; the
/// healthy generation is still salvaged and the run is imperfect (exit 3), the skip named in both
/// stderr and the manifest.
#[test]
fn gd10_below_floor_generation_is_refused_and_not_read() {
    let root = TempDir::new().unwrap();
    let dir = one_generation_dir(root.path());
    touch(&dir.join("ma-1-big-Data.db"));
    touch(&dir.join("ma-1-big-TOC.txt"));
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let o = salvage(&schema, &dir, &out);
    assert_eq!(o.status.code(), Some(3), "{}", err_text(&o));
    let err = err_text(&o);
    assert!(
        err.contains("ma-1-big") && err.contains("UnsupportedVersion"),
        "{err}"
    );
    let entries = salvage_manifest(&o);
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert!(finding_classes(&entries[0]).contains(&"SkippedInputGeneration".to_string()));
    assert!(has_data_db(&out));
}

/// GD10 (Orphans): a zero-length `Data.db` is named and skipped with its condition, not silently.
#[test]
fn gd10_orphan_is_named_and_skipped_with_its_condition() {
    let root = TempDir::new().unwrap();
    let dir = one_generation_dir(root.path());
    std::fs::write(dir.join("nb-9-big-Data.db"), b"").unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let o = salvage(&schema, &dir, &out);
    assert_eq!(o.status.code(), Some(3), "{}", err_text(&o));
    let err = err_text(&o);
    assert!(err.contains("nb-9-big") && err.contains("Orphan"), "{err}");
    let entries = salvage_manifest(&o);
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert!(finding_classes(&entries[0]).contains(&"SkippedInputGeneration".to_string()));
}

/// GD2.3 for salvage: an inconsistent transaction state refuses the run, naming the log.
#[test]
fn gd10_txn_inconsistent_directory_is_refused_naming_the_log() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "txn_inconsistent", "txn_compaction");
    let schema = schema_file(root.path(), "txn_compaction");
    let out = root.path().join("out");
    let o = salvage(&schema, &dir, &out);
    assert_eq!(o.status.code(), Some(1), "{}", err_text(&o));
    assert!(
        err_text(&o).contains("nb_txn_compaction_"),
        "{}",
        err_text(&o)
    );
    assert!(!has_data_db(&out));
}

// ------------------------------------------------------------- review follow-ups (PR #4673)

/// A bare relative `Data.db` (its `parent()` is `""`) names a file in the current directory: both
/// verbs list `.`, not the empty path.
#[test]
fn a_bare_relative_data_db_is_listed_from_the_current_directory() {
    let root = TempDir::new().unwrap();
    let dir = one_generation_dir(root.path());
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out_rebuild");
    let o = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .current_dir(&dir)
        .args([
            os("--schema"),
            schema.as_os_str(),
            os("rebuild"),
            os("nb-1-big-Data.db"),
            os("--table"),
            os("snap_backup"),
            os("--components"),
            os("digest,toc"),
            os("--out"),
            out.as_os_str(),
        ])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(0), "{}", err_text(&o));
    assert!(out.join("nb-1-big-Digest.crc32").is_file());

    let out = root.path().join("out_salvage");
    let o = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .current_dir(&dir)
        .args([
            os("--schema"),
            schema.as_os_str(),
            os("salvage"),
            os("nb-1-big-Data.db"),
            os("--table"),
            os("snap_backup"),
            os("--out"),
            out.as_os_str(),
            os("--out-format"),
            os("json"),
        ])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(0), "{}", err_text(&o));
    assert!(has_data_db(&out));
}

/// GD4: a directory holding only Orphans writes the manifest naming them, rebuilds nothing, and
/// exits 1 (nothing was rebuilt and nothing was refused; R7 has no partial-success code).
#[test]
fn an_orphan_only_directory_writes_its_manifest_and_exits_1() {
    let root = TempDir::new().unwrap();
    let dir = root
        .path()
        .join("gd4626")
        .join("snap_backup-0123456789abcdef0123456789abcdef");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("nb-9-big-Data.db"), b"").unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let m = root.path().join("m.json");
    let o = rebuild(&schema, &dir, &out, &m, "digest");
    assert_eq!(o.status.code(), Some(1), "{}", err_text(&o));
    let entries = manifest(&m);
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(
        entries[0]["input_conditions"],
        serde_json::json!(["Orphan"])
    );
    assert!(!out.exists());
}

/// A single explicit Orphan file is a usage error naming the condition (R7.3: nothing written, no
/// manifest): there is no table-directory run for an entry to belong to.
#[test]
fn an_explicit_orphan_file_is_a_named_usage_error() {
    let root = TempDir::new().unwrap();
    let dir = one_generation_dir(root.path());
    std::fs::write(dir.join("nb-9-big-Data.db"), b"").unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let m = root.path().join("m.json");
    let o = rebuild(&schema, &dir.join("nb-9-big-Data.db"), &out, &m, "digest");
    assert_eq!(o.status.code(), Some(1));
    assert!(err_text(&o).contains("Orphan"), "{}", err_text(&o));
    assert!(
        !err_text(&o).contains("no report generated"),
        "{}",
        err_text(&o)
    );
    assert!(!m.exists() && !out.exists());
}

/// An explicit `ma-1-big-Data.db` is an UnsupportedVersion error (GD1) without the spurious
/// "no report generated" line.
#[test]
fn an_explicit_below_floor_file_fails_without_a_spurious_report_error() {
    let root = TempDir::new().unwrap();
    let dir = one_generation_dir(root.path());
    touch(&dir.join("ma-1-big-Data.db"));
    let schema = schema_file(root.path(), "snap_backup");
    let o = rebuild(
        &schema,
        &dir.join("ma-1-big-Data.db"),
        &root.path().join("out"),
        &root.path().join("m.json"),
        "digest",
    );
    assert_eq!(o.status.code(), Some(1));
    let err = err_text(&o);
    assert!(err.contains("Unsupported SSTable version"), "{err}");
    assert!(!err.contains("no report generated"), "{err}");
}

/// Item 6: the below-floor refusal is decided before anything is built, so `--out` is never
/// created, and every non-refused entry is reported rolled back.
#[test]
fn a_below_floor_directory_creates_no_output_at_all() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    touch(&dir.join("ma-1-big-Data.db"));
    let schema = schema_file(root.path(), "snap_backup");
    let out = root.path().join("out");
    let m = root.path().join("m.json");
    let o = rebuild(&schema, &dir, &out, &m, "digest,toc");
    assert_eq!(o.status.code(), Some(2), "{}", err_text(&o));
    assert!(!out.exists(), "--out must never be created on this refusal");
    assert_eq!(manifest(&m).len(), 6);
}

fn salvage_single(schema: &Path, file: &Path, out: &Path) -> Output {
    salvage(schema, file, out)
}

/// Salvage's explicit-file form applies the module's conditions: each is a named refusal (exit 1,
/// nothing written), never a generic hard error from the reader.
#[test]
fn an_explicit_file_is_refused_by_name_for_each_module_condition() {
    let root = TempDir::new().unwrap();
    let schema = schema_file(root.path(), "snap_backup");

    // below the floor
    let dir = one_generation_dir(root.path());
    touch(&dir.join("ma-1-big-Data.db"));
    let out = root.path().join("o1");
    let o = salvage_single(&schema, &dir.join("ma-1-big-Data.db"), &out);
    assert_eq!(o.status.code(), Some(1), "{}", err_text(&o));
    assert!(
        err_text(&o).contains("UnsupportedVersion"),
        "{}",
        err_text(&o)
    );
    assert!(!has_data_db(&out));

    // Orphan
    std::fs::write(dir.join("nb-9-big-Data.db"), b"").unwrap();
    let o = salvage_single(
        &schema,
        &dir.join("nb-9-big-Data.db"),
        &root.path().join("o2"),
    );
    assert_eq!(o.status.code(), Some(1));
    assert!(err_text(&o).contains("Orphan"), "{}", err_text(&o));

    // UUID id
    let uuid_dir = seeded(root.path(), "uuid_ids", "uuid_only");
    let uuid_schema = schema_file(root.path(), "uuid_only");
    let o = salvage_single(
        &uuid_schema,
        &uuid_dir.join("nb-3h5t_0i6t_1jigq2lxu6305iz8wl-big-Data.db"),
        &root.path().join("o3"),
    );
    assert_eq!(o.status.code(), Some(1));
    assert!(err_text(&o).contains("UUID"), "{}", err_text(&o));

    // hidden by a committed transaction log
    let (_r, hidden_dir) = stamped_fixture_copy("txn_committed");
    let hidden_schema = schema_file(root.path(), "txn_compaction");
    let o = salvage_single(
        &hidden_schema,
        &hidden_dir.join("nb-1-big-Data.db"),
        &root.path().join("o4"),
    );
    assert_eq!(o.status.code(), Some(1));
    assert!(
        err_text(&o).contains("nb_txn_compaction_"),
        "{}",
        err_text(&o)
    );
}
