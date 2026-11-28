//! Issue #4626 phase 3 (tasks 3.1 to 3.4; spec deltas GD5 to GD8): `explain`, `find`, `diagnose`
//! and `diff` list their generations through the generation module.
//!
//! Every case drives the BUILT BINARY. Oracles: the Cassandra 5.0.8-written fixtures in
//! `test-data/fixtures/issue_4626` (`uuid_ids`, `txn_committed`, `txn_incomplete`,
//! `txn_inconsistent`, `snap_backup`), whose transaction logs and UUID ids are Cassandra's own
//! bytes. A fixture that cannot be found PANICS (#3220): no case skips. Every mutation (a deleted
//! `TOC.txt`, a truncated `Statistics.db`, a placeholder `ma-1-big` descriptor) is applied to a
//! tempdir COPY; the shared fixtures are never touched and no SSTable byte is hand-written.

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

const KS_TABLE: &str = "gd4626.snap_backup";
const NOW: &str = "1795588300";

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

fn out(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn err(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

/// `<root>/gd4626/<table>-<id>`: a table directory under a keyspace directory, seeded from a
/// fixture state, with `snapshots/` and `backups/` left in place (they must never be read).
fn seeded(root: &Path, state: &str, table: &str) -> PathBuf {
    let src = fixture_table(state, table);
    let dir = root.join("gd4626").join(src.file_name().unwrap());
    copy_dir(&src, &dir);
    dir
}

fn explain(root: &Path, schema: &Path, table: &str) -> Output {
    cqlite(&[
        os("--schema"),
        schema.as_os_str(),
        os("--data-dir"),
        root.as_os_str(),
        os("explain"),
        os(&format!("gd4626.{table}")),
        os("1"),
        os("--now"),
        os(NOW),
    ])
}

fn first_line(o: &Output) -> String {
    out(o).lines().next().unwrap_or_default().to_string()
}

/// Add a placeholder generation (never parsed: the listing only measures it).
fn add_placeholder(dir: &Path, prefix: &str) {
    touch(&dir.join(format!("{prefix}-Data.db")));
    touch(&dir.join(format!("{prefix}-TOC.txt")));
}

// ------------------------------------------------------------------------------------ explain

/// GD5.1 on Cassandra's own committed compaction log: the inputs `nb-1..3` stay on disk but are
/// not read; the first line lists only the output. The log names another absolute prefix, so the
/// run also says so on stderr (Q47).
#[test]
fn explain_committed_compaction_inputs_are_not_read() {
    let (root, dir) = stamped_fixture_copy("txn_committed");
    assert!(dir.join("nb-1-big-Data.db").is_file());
    let schema = schema_file(root.path(), "txn_compaction");
    let o = explain(root.path(), &schema, "txn_compaction");
    assert_eq!(o.status.code(), Some(0), "{}", err(&o));
    let first = first_line(&o);
    assert!(first.contains("generations=1 [nb-4-big]"), "{first}");
    assert!(err(&o).contains("relocated"), "{}", err(&o));
}

/// GD5.2 on the Cassandra-written UUID corpus: sequence ids first, then UUID ids in the module's
/// order, UUID ids included in the first line.
#[test]
fn explain_lists_uuid_ids_after_sequence_ids() {
    let root = TempDir::new().unwrap();
    seeded(root.path(), "uuid_ids", "uuid_mixed");
    let schema = schema_file(root.path(), "uuid_mixed");
    let o = explain(root.path(), &schema, "uuid_mixed");
    assert_eq!(o.status.code(), Some(0), "{}", err(&o));
    assert!(
        first_line(&o).contains(
            "generations=4 [nb-1-big, nb-2-big, nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big, \
             nb-3h5t_0i6s_1tsu22lxu6305iz8wl-big]"
        ),
        "{}",
        first_line(&o)
    );
}

/// GD5.3: a generation without `TOC.txt` is shown with `(toc-missing)` and the run succeeds.
#[test]
fn explain_shows_a_toc_missing_generation() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    std::fs::remove_file(dir.join("nb-1-big-TOC.txt")).unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let o = explain(root.path(), &schema, "snap_backup");
    assert_eq!(o.status.code(), Some(0), "{}", err(&o));
    let first = first_line(&o);
    assert!(
        first.contains("generations=5 [nb-1-big (toc-missing), nb-2-big,"),
        "{first}"
    );
}

/// GD5.4 on the Cassandra-derived inconsistent directory: exit 2, the log is named, stdout is
/// empty.
#[test]
fn explain_refuses_an_inconsistent_transaction_state() {
    let (root, _dir) = stamped_fixture_copy("txn_inconsistent");
    let schema = schema_file(root.path(), "txn_compaction");
    let o = explain(root.path(), &schema, "txn_compaction");
    assert_eq!(o.status.code(), Some(2), "{}", err(&o));
    assert!(
        err(&o).contains("nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log"),
        "{}",
        err(&o)
    );
    assert!(out(&o).is_empty(), "no cell lines: {}", out(&o));
}

/// GD5.1 rule detail: a below-floor generation exits 2 naming the path and the condition.
#[test]
fn explain_refuses_a_below_floor_generation_by_path() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    add_placeholder(&dir, "ma-9-big");
    let schema = schema_file(root.path(), "snap_backup");
    let o = explain(root.path(), &schema, "snap_backup");
    assert_eq!(o.status.code(), Some(2), "{}", err(&o));
    assert!(
        err(&o).contains("ma-9-big-Data.db") && err(&o).contains("UnsupportedVersion"),
        "{}",
        err(&o)
    );
    assert!(out(&o).is_empty(), "{}", out(&o));
}

/// GD5.1 rule detail: an Orphan is named on stderr and is not in the generation list.
#[test]
fn explain_names_an_orphan_on_stderr_and_does_not_list_it() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    std::fs::write(dir.join("nb-2-big-Data.db"), b"").unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let o = explain(root.path(), &schema, "snap_backup");
    assert_eq!(o.status.code(), Some(0), "{}", err(&o));
    assert!(
        err(&o).contains("nb-2-big-Data.db") && err(&o).contains("Orphan"),
        "{}",
        err(&o)
    );
    let first = first_line(&o);
    assert!(
        first.contains("generations=4 [nb-1-big, nb-3-big, nb-4-big, nb-5-big]"),
        "{first}"
    );
}

// ---------------------------------------------------------------------------------------- find

fn find(dir: &Path, schema: &Path, extra: &[&str]) -> Output {
    let mut args: Vec<&std::ffi::OsStr> = vec![
        os("--schema"),
        schema.as_os_str(),
        os("find"),
        dir.as_os_str(),
        os("1"),
    ];
    args.extend(extra.iter().map(|s| os(s)));
    cqlite(&args)
}

fn find_rows(o: &Output) -> Vec<Value> {
    let line = out(o)
        .lines()
        .find(|l| l.starts_with('{'))
        .unwrap_or_else(|| panic!("no JSON in {}", out(o)))
        .to_string();
    let v: Value = serde_json::from_str(&line).unwrap();
    v["generations"].as_array().expect("generations").clone()
}

fn basenames(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|r| r["basename"].as_str().unwrap().to_string())
        .collect()
}

/// GD7.1: a generation a committed compaction removed is not probed, though its files remain.
#[test]
fn find_does_not_probe_a_hidden_generation() {
    let (root, dir) = stamped_fixture_copy("txn_committed");
    let schema = schema_file(root.path(), "txn_compaction");
    let o = find(&dir, &schema, &["--out", "json"]);
    assert!(matches!(o.status.code(), Some(0)), "{}{}", out(&o), err(&o));
    assert_eq!(basenames(&find_rows(&o)), ["nb-4-big-Data.db"]);
}

/// GD7: the whole Live set is probed, UUID ids included, in id order.
#[test]
fn find_probes_uuid_ids_in_id_order() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "uuid_ids", "uuid_mixed");
    let schema = schema_file(root.path(), "uuid_mixed");
    let o = find(&dir, &schema, &["--out", "json"]);
    assert_eq!(o.status.code(), Some(0), "{}{}", out(&o), err(&o));
    assert_eq!(
        basenames(&find_rows(&o)),
        [
            "nb-1-big-Data.db",
            "nb-2-big-Data.db",
            "nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big-Data.db",
            "nb-3h5t_0i6s_1tsu22lxu6305iz8wl-big-Data.db",
        ]
    );
}

/// GD7.2: a generation whose `Statistics.db` is truncated is listed with its condition and makes
/// the run exit 2; it is never omitted and never reported absent.
#[test]
fn find_lists_a_corrupt_generation_with_its_condition() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    let stats = dir.join("nb-2-big-Statistics.db");
    let len = std::fs::metadata(&stats).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&stats)
        .unwrap()
        .set_len(len / 2)
        .unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let o = find(&dir, &schema, &["--out", "json"]);
    assert_eq!(o.status.code(), Some(2), "{}{}", out(&o), err(&o));
    let rows = find_rows(&o);
    let bad = rows
        .iter()
        .find(|r| r["basename"] == "nb-2-big-Data.db")
        .expect("nb-2-big is in the report");
    assert_eq!(bad["probe"], "unreadable");
    assert!(bad["cause"].as_str().unwrap().contains("Corrupt"), "{bad}");
    assert_eq!(rows.len(), 5, "every generation is reported");
}

/// GD7: a below-floor generation appears with its condition (exit 2) rather than vanishing.
#[test]
fn find_lists_a_below_floor_generation() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    add_placeholder(&dir, "ma-9-big");
    let schema = schema_file(root.path(), "snap_backup");
    let o = find(&dir, &schema, &["--out", "json"]);
    assert_eq!(o.status.code(), Some(2), "{}{}", out(&o), err(&o));
    let rows = find_rows(&o);
    let ma = rows
        .iter()
        .find(|r| r["basename"] == "ma-9-big-Data.db")
        .expect("below-floor generation is reported");
    assert!(
        ma["cause"].as_str().unwrap().contains("UnsupportedVersion"),
        "{ma}"
    );
}

/// #4671 finding 2: find LISTS an Orphan (as an UNREADABLE row), so its stderr note must not say
/// "not listed".
#[test]
fn find_orphan_note_is_verb_correct() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    std::fs::remove_file(dir.join("nb-3-big-Data.db")).unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let o = find(&dir, &schema, &[]);
    assert_eq!(o.status.code(), Some(2), "{}", err(&o));
    assert!(
        out(&o).contains("nb-3-big-Data.db: UNREADABLE"),
        "{}",
        out(&o)
    );
    assert!(
        err(&o).contains("excluded from the live set") && !err(&o).contains("not listed"),
        "{}",
        err(&o)
    );
}

/// #4671 finding 4 (Q44): a transaction log with a corrupt record is a note the verb refuses to
/// prove `unflushed` through. It appears as its own UNREADABLE row and the run exits 2.
#[test]
fn find_reports_a_corrupt_transaction_log_as_a_blocking_row() {
    let (root, dir) = stamped_fixture_copy("txn_committed");
    let log = dir.join("nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log");
    let text = std::fs::read_to_string(&log).unwrap();
    assert!(text.contains("[1949738987]"));
    std::fs::write(&log, text.replace("[1949738987]", "[1949738988]")).unwrap();
    let schema = schema_file(root.path(), "txn_compaction");
    let o = find(&dir, &schema, &["--out", "json"]);
    assert_eq!(o.status.code(), Some(2), "{}{}", out(&o), err(&o));
    let rows = find_rows(&o);
    let row = rows
        .iter()
        .find(|r| r["basename"].as_str().unwrap().ends_with(".log"))
        .unwrap_or_else(|| panic!("the corrupt log is a row: {rows:?}"));
    assert_eq!(row["probe"], "unreadable");
    assert!(
        row["cause"].as_str().unwrap().contains("CorruptTxnLog"),
        "{row}"
    );
}

/// GD7: an inconsistent transaction state is refused, exit 2 naming the log.
#[test]
fn find_refuses_an_inconsistent_transaction_state() {
    let (root, dir) = stamped_fixture_copy("txn_inconsistent");
    let schema = schema_file(root.path(), "txn_compaction");
    let o = find(&dir, &schema, &[]);
    assert_eq!(o.status.code(), Some(2), "{}", err(&o));
    assert!(
        err(&o).contains("nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log"),
        "{}",
        err(&o)
    );
}

// ------------------------------------------------------------------------------------ diagnose

fn diagnose_json(dir: &Path) -> (Option<i32>, Value, String) {
    let o = cqlite(&[
        os("diagnose"),
        dir.as_os_str(),
        os("--now"),
        os(NOW),
        os("--out"),
        os("json"),
    ]);
    let v = serde_json::from_str(&out(&o)).unwrap_or(Value::Null);
    (o.status.code(), v, err(&o))
}

/// GD8.1: a `TocMissing` generation is reported in `generations[]` with its condition, exit 0.
#[test]
fn diagnose_reports_toc_missing_with_exit_zero() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    std::fs::remove_file(dir.join("nb-1-big-TOC.txt")).unwrap();
    let (code, v, e) = diagnose_json(&dir);
    assert_eq!(code, Some(0), "{e}");
    let gens = v["generations"].as_array().unwrap();
    assert_eq!(gens.len(), 5);
    assert_eq!(gens[0]["generation"], 1);
    assert_eq!(gens[0]["conditions"], serde_json::json!(["TocMissing"]));
    assert!(
        gens[1].get("conditions").is_none(),
        "healthy keeps R7.1's keys"
    );
    assert!(v["unreadable_generations"].as_array().unwrap().is_empty());
}

/// GD8.2: a below-floor generation is listed under `unreadable_generations`, never dropped and
/// never a refusal.
#[test]
fn diagnose_lists_a_below_floor_generation_as_unreadable() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    add_placeholder(&dir, "ma-1-big");
    let (code, v, e) = diagnose_json(&dir);
    assert_eq!(code, Some(0), "{e}");
    let bad = v["unreadable_generations"].as_array().unwrap();
    assert_eq!(bad.len(), 1, "{v}");
    assert!(bad[0]["data_path"]
        .as_str()
        .unwrap()
        .ends_with("ma-1-big-Data.db"));
    assert!(bad[0]["cause"]
        .as_str()
        .unwrap()
        .contains("UnsupportedVersion"));
    assert_eq!(v["generations"].as_array().unwrap().len(), 5);
}

/// GD8.1: an Orphan appears in `unreadable_generations` with cause `Orphan`.
#[test]
fn diagnose_lists_an_orphan_as_unreadable() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    std::fs::remove_file(dir.join("nb-3-big-Data.db")).unwrap();
    let (code, v, e) = diagnose_json(&dir);
    assert_eq!(code, Some(0), "{e}");
    let bad = v["unreadable_generations"].as_array().unwrap();
    assert_eq!(bad.len(), 1, "{v}");
    assert!(bad[0]["data_path"]
        .as_str()
        .unwrap()
        .ends_with("nb-3-big-Data.db"));
    assert!(bad[0]["cause"].as_str().unwrap().starts_with("Orphan"));
    assert_eq!(v["generations"].as_array().unwrap().len(), 4);
}

/// GD8.1 (Q45): an inconsistent transaction state exits 1, naming the log.
#[test]
fn diagnose_exits_one_on_an_inconsistent_transaction_state() {
    let (_root, dir) = stamped_fixture_copy("txn_inconsistent");
    let o = cqlite(&[os("diagnose"), dir.as_os_str(), os("--now"), os(NOW)]);
    assert_eq!(o.status.code(), Some(1), "{}", err(&o));
    assert!(
        err(&o).contains("nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log"),
        "{}",
        err(&o)
    );
    assert!(out(&o).is_empty());
}

/// A committed compaction's inputs are not diagnosed (G1.1), and the UUID corpus is listed in id
/// order with each id verbatim.
#[test]
fn diagnose_lists_the_live_set_in_id_order() {
    let (_root, dir) = stamped_fixture_copy("txn_committed");
    let (code, v, e) = diagnose_json(&dir);
    assert_eq!(code, Some(0), "{e}");
    let ids: Vec<&Value> = v["generations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| &g["generation"])
        .collect();
    assert_eq!(ids, [&serde_json::json!(4)]);

    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "uuid_ids", "uuid_mixed");
    let (code, v, e) = diagnose_json(&dir);
    assert_eq!(code, Some(0), "{e}");
    let ids: Vec<String> = v["generations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["generation"].to_string().trim_matches('"').to_string())
        .collect();
    assert_eq!(
        ids,
        [
            "1",
            "2",
            "3h5t_0i6r_2dipm2lxu6305iz8wl",
            "3h5t_0i6s_1tsu22lxu6305iz8wl"
        ]
    );
}

// ---------------------------------------------------------------------------------------- diff

fn diff(a: &Path, b: &Path, schema: &Path) -> Output {
    cqlite(&[
        os("--schema"),
        schema.as_os_str(),
        os("diff"),
        a.as_os_str(),
        b.as_os_str(),
        os("--table"),
        os(KS_TABLE),
        os("--now"),
        os(NOW),
    ])
}

/// A side holding only generation `nb-1-big` of the `snap_backup` fixture.
fn single_generation_side(root: &Path, name: &str) -> PathBuf {
    let src = fixture_table("snap_backup", "snap_backup");
    let dir = root
        .join(name)
        .join("gd4626")
        .join(src.file_name().unwrap());
    copy_generation_as(&src, "nb-1-big", &dir, "nb-1-big");
    dir
}

/// GD6.1: `TocMissing` on side B is visible (the generation is still listed) and the run
/// succeeds.
#[test]
fn diff_shows_a_toc_missing_generation_on_side_b() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path(), "snap_backup");
    let a = single_generation_side(tmp.path(), "a");
    let b = single_generation_side(tmp.path(), "b");
    std::fs::remove_file(b.join("nb-1-big-TOC.txt")).unwrap();
    let o = diff(&a, &b, &schema);
    assert_eq!(o.status.code(), Some(0), "{}", err(&o));
    let first = first_line(&o);
    // #4671: the module's name for the generation, suffix included. The renderer quotes a field
    // that holds a space (R10), so the suffixed name is a quoted string.
    assert!(
        first.contains(r#"side_b: generations=1 ["nb-1-big (toc-missing)"]"#),
        "{first}"
    );
    assert!(
        first.contains("side_a: generations=1 [nb-1-big]"),
        "{first}"
    );
}

/// GD6.2: a below-floor generation on side A exits 2 naming `side=a` and the condition.
#[test]
fn diff_refuses_a_below_floor_generation_naming_the_side() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path(), "snap_backup");
    let a = single_generation_side(tmp.path(), "a");
    let b = single_generation_side(tmp.path(), "b");
    add_placeholder(&a, "ma-1-big");
    let o = diff(&a, &b, &schema);
    assert_eq!(o.status.code(), Some(2), "{}", err(&o));
    assert!(
        err(&o).contains("side=a") && err(&o).contains("UnsupportedVersion"),
        "{}",
        err(&o)
    );
    assert!(out(&o).is_empty(), "no partition findings: {}", out(&o));
}

/// GD6: an inconsistent transaction state on side B exits 2 naming `side=b` and the log.
#[test]
fn diff_refuses_an_inconsistent_transaction_state_naming_the_side() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path(), "txn_compaction");
    let (root_b, b) = stamped_fixture_copy("txn_inconsistent");
    let (root_a, a) = stamped_fixture_copy("txn_committed");
    let o = cqlite(&[
        os("--schema"),
        schema.as_os_str(),
        os("diff"),
        a.as_os_str(),
        b.as_os_str(),
        os("--table"),
        os("gd4626.txn_compaction"),
        os("--now"),
        os(NOW),
    ]);
    drop((root_a, root_b));
    assert_eq!(o.status.code(), Some(2), "{}", err(&o));
    assert!(
        err(&o).contains("side=b")
            && err(&o).contains("nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log"),
        "{}",
        err(&o)
    );
}

/// GD6.1 rule detail: a side that is itself `<table-dir>/snapshots/<tag>` is its own Live set.
#[test]
fn diff_accepts_a_snapshot_directory_as_a_side() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path(), "snap_backup");
    let table = seeded(tmp.path(), "snap_backup", "snap_backup");
    let snapshot = table.join("snapshots").join("pre-upgrade");
    assert!(snapshot.join("manifest.json").is_file());
    let o = diff(&snapshot, &table, &schema);
    assert_eq!(o.status.code(), Some(0), "{}", err(&o));
    let first = first_line(&o);
    assert!(
        first.contains("side_a: generations=2 [nb-1-big, nb-2-big]")
            && first.contains("side_b: generations=5 ["),
        "{first}"
    );
}

/// Roborev round 2 (1): a side holding ONLY orphans still resolves as a table directory, so diff
/// refuses it as an orphan (exit 2, `side=`, the orphan named), not as "no such table" (exit 1).
#[test]
fn diff_refuses_an_orphan_only_side_naming_it() {
    let tmp = TempDir::new().unwrap();
    let schema = schema_file(tmp.path(), "snap_backup");
    let a = single_generation_side(tmp.path(), "a");
    let b = single_generation_side(tmp.path(), "b");
    std::fs::remove_file(b.join("nb-1-big-Data.db")).unwrap();
    let o = diff(&a, &b, &schema);
    assert_eq!(o.status.code(), Some(2), "{}", err(&o));
    assert!(
        err(&o).contains("side=b")
            && err(&o).contains("nb-1-big-Data.db")
            && err(&o).contains("Orphan"),
        "{}",
        err(&o)
    );
}

/// Same directory under `explain`: nothing live to explain is a usage error (exit 1), after the
/// orphan has been named on stderr (GD5.1: named, not listed).
#[test]
fn explain_on_an_orphan_only_directory_names_it_and_has_nothing_to_explain() {
    let root = TempDir::new().unwrap();
    let dir = seeded(root.path(), "snap_backup", "snap_backup");
    for i in 2..=5 {
        for c in [
            "Data.db",
            "TOC.txt",
            "Statistics.db",
            "Index.db",
            "Filter.db",
            "Summary.db",
            "CompressionInfo.db",
            "Digest.crc32",
        ] {
            std::fs::remove_file(dir.join(format!("nb-{i}-big-{c}"))).unwrap();
        }
    }
    std::fs::remove_file(dir.join("nb-1-big-Data.db")).unwrap();
    let schema = schema_file(root.path(), "snap_backup");
    let o = explain(root.path(), &schema, "snap_backup");
    assert_eq!(o.status.code(), Some(1), "{}", err(&o));
    assert!(
        err(&o).contains("nb-1-big-Data.db") && err(&o).contains("Orphan"),
        "{}",
        err(&o)
    );
    assert!(out(&o).is_empty());
}
