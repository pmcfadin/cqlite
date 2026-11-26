//! Issue #4627 (field round 0.19 F7) -- `frozen<tuple<..>>` columns of HEALTHY,
//! Cassandra-written SSTables must decode, not fail with a false
//! `Data corruption: Cell '<c>': invalid cell flags 0x..`.
//!
//! # Oracle (#3041/#3042: Cassandra-written bytes, never a CQLite round trip)
//! `test-data/fixtures/issue_4627/` holds a Cassandra 5.0.8 generation of
//! `fix4627.t_tuple` in BOTH formats (`sstables/` = BTI `da-2-bti`, written by the
//! node; `sstables_big/` = BIG `nb-3-big`, written by Cassandra 5.0.8 `sstablescrub`
//! of that BTI generation) and the `sstabledump` of each under `goldens/`. The
//! expectations below are PARSED from those goldens at run time. Commands and
//! versions: `test-data/fixtures/issue_4627/REGEN-NOTES.md`.
//!
//! # Root cause (pinned `cassandra-5.0.8`)
//! A tuple is implicitly frozen (`CQL3Type.Raw.RawTuple.freeze()` returns itself;
//! `TupleType.toString()` is never wrapped in `FrozenType`), so a `frozen<tuple<..>>`
//! cell is `[flags][VUInt length][elements]`. The decoder's `frozen<..>` ladder
//! (`cell_value_complex.rs`) had no `tuple<` arm and recursed through the generic
//! cell entry point, which reads a SECOND cell-flags byte -- the tuple's own length
//! prefix (`0x25`) -- and rejects it.
//!
//! The committed fixture is checkout-relative and `must_run`: nothing here skips.

use assert_cmd::Command;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn fixture(rel: &str) -> PathBuf {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("test-data/fixtures/issue_4627")
        .join(rel);
    assert!(p.exists(), "committed fixture missing: {p:?}");
    p
}

fn cqlite(data_dir: &str, args: &[&str]) -> std::process::Output {
    Command::cargo_bin("cqlite")
        .expect("cqlite binary")
        .arg("--schema")
        .arg(fixture("schema.cql"))
        .arg("--data-dir")
        .arg(fixture(data_dir))
        .args(args)
        .output()
        .expect("spawn cqlite")
}

/// `(id, ck) -> { column -> expected JSON }` from the sstabledump golden. A column
/// absent from the golden row is absent here (CQLite reports it as JSON null).
fn golden_rows(golden: &str) -> BTreeMap<(i64, i64), BTreeMap<String, Value>> {
    let text = std::fs::read_to_string(fixture(golden)).expect("golden readable");
    let doc: Value = serde_json::from_str(&text).expect("golden is JSON");
    let mut out = BTreeMap::new();
    for part in doc.as_array().expect("partition array") {
        let id: i64 = part["partition"]["key"][0]
            .as_str()
            .expect("key string")
            .parse()
            .expect("int key");
        for row in part["rows"].as_array().expect("rows") {
            let ck = row["clustering"][0].as_i64().expect("ck");
            let mut cols: BTreeMap<String, Value> = BTreeMap::new();
            for cell in row["cells"].as_array().expect("cells") {
                let name = cell["name"].as_str().expect("name").to_string();
                let Some(value) = cell.get("value") else {
                    continue; // collection tombstone marker
                };
                if cell.get("path").is_some() {
                    // multicell list element: append in on-disk (path) order
                    cols.entry(name)
                        .or_insert_with(|| Value::Array(Vec::new()))
                        .as_array_mut()
                        .expect("array")
                        .push(value.clone());
                } else {
                    cols.insert(name, value.clone());
                }
            }
            out.insert((id, ck), cols);
        }
    }
    assert_eq!(out.len(), 5, "golden must carry all 5 rows");
    out
}

fn assert_query_matches_golden(data_dir: &str, golden: &str) {
    let out = cqlite(
        data_dir,
        &["--out", "json", "query", "select * from fix4627.t_tuple"],
    );
    assert!(
        out.status.success(),
        "query on healthy {data_dir} must succeed; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows: Value = serde_json::from_slice(&out.stdout).expect("query JSON");
    let rows = rows.as_array().expect("row array");
    let expected = golden_rows(golden);
    assert_eq!(rows.len(), expected.len(), "row count");
    for row in rows {
        let key = (row["id"].as_i64().unwrap(), row["ck"].as_i64().unwrap());
        let want = expected.get(&key).unwrap_or_else(|| panic!("row {key:?}"));
        for col in ["tp", "nt", "p", "lt", "tu"] {
            let got = &row[col];
            let exp = want.get(col).unwrap_or(&Value::Null);
            assert_eq!(got, exp, "{data_dir} row {key:?} column {col}");
        }
    }
}

#[test]
fn bti_frozen_tuple_query_matches_sstabledump() {
    assert_query_matches_golden("sstables", "goldens/da-2-bti-Data.db.sstabledump.json");
}

#[test]
fn big_frozen_tuple_query_matches_sstabledump() {
    assert_query_matches_golden("sstables_big", "goldens/nb-3-big-Data.db.sstabledump.json");
}

/// The reported verb: `explain` read every generation of the partition and died.
#[test]
fn bti_frozen_tuple_explain_reads_the_generation() {
    let out = cqlite(
        "sstables",
        &["--out", "json", "explain", "fix4627.t_tuple", "1"],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "explain on a healthy generation must succeed; stderr: {stderr}"
    );
    assert!(!stderr.contains("Data corruption"), "{stderr}");
    let body = String::from_utf8_lossy(&out.stdout);
    assert!(body.contains("\"probe\":\"hit\""), "{body}");
}
