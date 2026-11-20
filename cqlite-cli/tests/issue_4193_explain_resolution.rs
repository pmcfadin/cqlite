//! Issue #4193 roborev fixes: `explain` resolves tables by (keyspace, table) and reads only the
//! LIVE generations of the table directory (Cassandra `Directories`: snapshots/, backups/ and
//! `.<index>` subdirectories are never live sstables). Real Cassandra-written generations from
//! the committed `test_explain.trace_decisions` fixture, copied (never edited) into staging.

use assert_cmd::Command;
use serde_json::Value;
use std::path::{Path, PathBuf};

const TABLE_ID_A: &str = "4ad336f0df0611f181786327fffcebdf";
const TABLE_ID_B: &str = "5bd447a1ef1722a282897438affcebdf";

fn repo(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(rel)
}

fn fixture_dir() -> PathBuf {
    repo("test-data/datasets/sstables/test_explain").join(format!("trace_decisions-{TABLE_ID_A}"))
}

/// Copy fixture generation `from` (1 or 2) into `to`, renamed to generation `as_gen`.
fn copy_generation(from: u32, as_gen: u32, to: &Path) {
    std::fs::create_dir_all(to).expect("create staging dir");
    let prefix = format!("nb-{from}-big-");
    for entry in std::fs::read_dir(fixture_dir()).expect("fixture dir") {
        let entry = entry.expect("entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(rest) = name.strip_prefix(&prefix) {
            std::fs::copy(entry.path(), to.join(format!("nb-{as_gen}-big-{rest}")))
                .expect("copy component");
        }
    }
}

/// Two keyspaces each declaring `trace_decisions`: ks_a with both generations, ks_b with gen 1.
fn two_keyspace_schema(dir: &Path) -> PathBuf {
    let base =
        std::fs::read_to_string(repo("test-data/schemas/explain-trace.cql")).expect("schema");
    let body = base.split_once("CREATE TABLE").expect("CREATE TABLE").1;
    let path = dir.join("two.cql");
    std::fs::write(
        &path,
        format!(
            "CREATE KEYSPACE ks_a WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};\n\
             CREATE TABLE ks_a.{b}\nCREATE KEYSPACE ks_b WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};\n\
             CREATE TABLE ks_b.{b}\n",
            b = body.trim_start().trim_start_matches("test_explain.")
        ),
    )
    .expect("write schema");
    path
}

fn run(schema: &Path, data: &Path, args: &[&str]) -> std::process::Output {
    Command::cargo_bin("cqlite")
        .expect("cqlite binary")
        .args([
            "--schema",
            schema.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
        ])
        .args(args)
        .output()
        .expect("process starts")
}

fn explain_json(schema: &Path, data: &Path, table: &str) -> Value {
    let out = run(
        schema,
        data,
        &[
            "explain",
            table,
            "1",
            "--now",
            "1794535358",
            "--out",
            "json",
        ],
    );
    assert!(
        out.status.success(),
        "explain failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).expect("utf8");
    serde_json::from_str(text.lines().nth(1).expect("json line")).expect("json")
}

fn winner_v(report: &Value) -> Vec<String> {
    report["cells"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["verdict"] == "winner" && c["column"] == "v")
        .map(|c| c["value"]["Text"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn same_named_tables_in_two_keyspaces_resolve_by_keyspace() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("data");
    let a = data
        .join("ks_a")
        .join(format!("trace_decisions-{TABLE_ID_A}"));
    let b = data
        .join("ks_b")
        .join(format!("trace_decisions-{TABLE_ID_B}"));
    copy_generation(1, 1, &a);
    copy_generation(2, 2, &a);
    copy_generation(1, 1, &b);
    let schema = two_keyspace_schema(tmp.path());

    let report_a = explain_json(&schema, &data, "ks_a.trace_decisions");
    assert_eq!(report_a["generations"].as_array().unwrap().len(), 2);
    assert_eq!(winner_v(&report_a), ["newer"]);

    let report_b = explain_json(&schema, &data, "ks_b.trace_decisions");
    assert_eq!(report_b["generations"].as_array().unwrap().len(), 1);
    assert_eq!(winner_v(&report_b), ["older"]);
}

#[test]
fn unknown_keyspace_and_ambiguous_table_are_usage_errors() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("data");
    let a = data
        .join("ks_a")
        .join(format!("trace_decisions-{TABLE_ID_A}"));
    let a2 = data
        .join("ks_a")
        .join(format!("trace_decisions-{TABLE_ID_B}"));
    copy_generation(1, 1, &a);
    copy_generation(2, 2, &a2);
    let schema = two_keyspace_schema(tmp.path());

    // Same keyspace, two table directories: a named usage error, never a silent pick.
    let out = run(&schema, &data, &["explain", "ks_a.trace_decisions", "1"]);
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("multiple SSTable directories"),
        "stderr: {stderr}"
    );

    // ks_b has no directory here: another keyspace's SSTables must not be used.
    let out = run(&schema, &data, &["explain", "ks_b.trace_decisions", "1"]);
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("ks_b.trace_decisions"), "stderr: {stderr}");
}

/// Live dir holds ONLY generation 1 (`v='older'`). `snapshots/`, `backups/` and an index
/// subdirectory each hold generation 2 (`v='newer'`, newer writetime) renamed to a HIGHER
/// generation: if any were read, the winner would flip to 'newer'.
#[test]
fn snapshots_backups_and_index_dirs_are_not_live_generations() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("data");
    let t = data
        .join("test_explain")
        .join(format!("trace_decisions-{TABLE_ID_A}"));
    copy_generation(1, 1, &t);
    copy_generation(2, 9, &t.join("snapshots").join("tag1"));
    copy_generation(2, 8, &t.join("backups"));
    copy_generation(2, 7, &t.join(".v_idx"));
    let schema = repo("test-data/schemas/explain-trace.cql");

    let report = explain_json(&schema, &data, "test_explain.trace_decisions");
    assert_eq!(
        report["generations"].as_array().unwrap().len(),
        1,
        "{report}"
    );
    assert_eq!(winner_v(&report), ["older"]);

    // SELECT agrees (the normal read path sees the same live set).
    let out = run(
        &schema,
        &data,
        &[
            "query",
            "SELECT * FROM test_explain.trace_decisions WHERE id=1",
        ],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("older") && !stdout.contains("newer"),
        "SELECT: {stdout}"
    );
}
