//! Issue #4640 (owner ruling Q51): non-frozen collections of frozen UDTs reassemble
//! across generations, and a table that HAS such a collection does not break a query
//! that selects other columns.
//!
//! Fixture `test-data/fixtures/issue_4640b/` (Cassandra 5.0.8, three BTI generations):
//! `al list<frozen<addr>>`, `am map<int, frozen<addr>>` and `ak map<frozen<addr>, int>`
//! (a UDT-KEYED map, whose reassembly needs the UDT registry) with a list element
//! tombstone, a map entry tombstone and an overwrite in newer generations. Oracles are
//! the live CL=ALL reads committed beside it, for `SELECT *`-shaped columns and for the
//! scalar projection `id, c, v`.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database, QueryResult, Value};

type Rows = BTreeMap<Vec<i32>, Vec<String>>;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/fixtures/issue_4640b")
}

async fn open(mode: ReadPathMode) -> Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![root().join("schema.cql")],
        data_dir: root().join("sstables"),
        version_hint: None,
        core_config,
        table_directory_filter: None,
    })
    .await
    .expect("ingestion succeeds")
    .database
}

fn render(v: Option<&Value>, nested: bool) -> String {
    let join = |items: Vec<String>| items.join(", ");
    match v {
        None | Some(Value::Null) => "null".to_string(),
        Some(Value::Integer(n)) => n.to_string(),
        Some(Value::Text(b)) => {
            let s = String::from_utf8_lossy(b).into_owned();
            if nested {
                format!("'{s}'")
            } else {
                s
            }
        }
        Some(Value::Frozen(inner)) => render(Some(inner), nested),
        Some(Value::Udt(u)) => format!(
            "{{{}}}",
            join(
                u.fields
                    .iter()
                    .map(|f| format!("{}: {}", f.name, render(f.value.as_ref(), true)))
                    .collect()
            )
        ),
        Some(Value::List(items)) => {
            format!(
                "[{}]",
                join(items.iter().map(|i| render(Some(i), true)).collect())
            )
        }
        Some(Value::Map(entries)) => format!(
            "{{{}}}",
            join(
                entries
                    .iter()
                    .map(|(k, v)| format!("{}: {}", render(Some(k), true), render(Some(v), true)))
                    .collect()
            )
        ),
        Some(other) => format!("UNEXPECTED {other:?}"),
    }
}

fn oracle(file: &str, data_cols: usize, rows: usize) -> Rows {
    let text = std::fs::read_to_string(root().join(file)).expect("oracle is committed");
    let mut out = Rows::new();
    for line in text.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.first().and_then(|c| c.parse::<i32>().ok()).is_none() {
            continue;
        }
        let key = vec![cells[0].parse().unwrap(), cells[1].parse().unwrap()];
        out.insert(
            key,
            cells[2..2 + data_cols]
                .iter()
                .map(|c| c.to_string())
                .collect(),
        );
    }
    assert_eq!(out.len(), rows, "oracle row count of {file}");
    out
}

/// Render the result, asserting the column set both ways (#3890).
fn rows_of(result: &QueryResult, cols: &[&str], ctx: &str) -> Rows {
    let declared: BTreeSet<&str> = ["id", "c"]
        .into_iter()
        .chain(cols.iter().copied())
        .collect();
    let mut out = Rows::new();
    for row in &result.rows {
        let present: BTreeSet<&str> = row.values.keys().map(|k| &**k).collect();
        // Both ways: nothing undeclared appears, and every declared column is present
        // unless it is a null collection (absent reads null, and the render compares it).
        assert!(
            present.is_subset(&declared),
            "{ctx}: undeclared column in {present:?}"
        );
        for c in ["id", "c", "v"]
            .into_iter()
            .filter(|c| declared.contains(c))
        {
            assert!(present.contains(c), "{ctx}: {c} missing from {present:?}");
        }
        for c in &declared {
            assert!(
                present.contains(c) || render(row.get(c), false) == "null",
                "{ctx}: {c} missing"
            );
        }
        let int = |c: &str| match row.get(c) {
            Some(Value::Integer(n)) => *n,
            other => panic!("{ctx}: {c} is {other:?}"),
        };
        let data = cols.iter().map(|c| render(row.get(c), false)).collect();
        assert!(
            out.insert(vec![int("id"), int("c")], data).is_none(),
            "{ctx}: duplicate row"
        );
    }
    out
}

#[tokio::test]
async fn select_star_matches_live_for_collections_of_frozen_udt() {
    let want = oracle("live-select-consistency-all.txt", 4, 4);
    // Guard against a vacuous pass: the oracle holds the tombstoned/overwritten shape.
    assert_eq!(
        want[&vec![3, 0]][1],
        "[{street: 'b', num: 2}, {street: 'c', num: 3}, {street: 'd', num: 4}]"
    );
    assert_eq!(
        want[&vec![3, 0]][3],
        "{{street: 'k2', num: 2}: 20, {street: 'k3', num: 3}: 30}"
    );
    for (mode, name) in [(ReadPathMode::Auto, "auto"), (ReadPathMode::Full, "full")] {
        let db = open(mode).await;
        let all = db.execute("SELECT * FROM fix4640b.t").await.expect("scan");
        assert_eq!(
            rows_of(&all, &["v", "al", "am", "ak"], name),
            want,
            "{name}: SELECT *"
        );
        for id in [3, 4] {
            let r = db
                .execute(&format!("SELECT * FROM fix4640b.t WHERE id = {id}"))
                .await
                .unwrap_or_else(|e| panic!("{name} id={id}: {e}"));
            let got = rows_of(&r, &["v", "al", "am", "ak"], &format!("{name} id={id}"));
            let sub: Rows = want
                .iter()
                .filter(|(k, _)| k[0] == id)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            assert_eq!(got, sub, "{name} id={id}");
        }
    }
}

/// The roborev finding: projecting only scalar columns must not assemble (or fail on)
/// the UDT collections the query never reads.
#[tokio::test]
async fn scalar_projection_succeeds_on_a_table_with_udt_collections() {
    let want = oracle("live-select-scalar-consistency-all.txt", 1, 4);
    for (mode, name) in [(ReadPathMode::Auto, "auto"), (ReadPathMode::Full, "full")] {
        let db = open(mode).await;
        let all = db
            .execute("SELECT id, c, v FROM fix4640b.t")
            .await
            .unwrap_or_else(|e| panic!("{name} scan: {e}"));
        assert_eq!(rows_of(&all, &["v"], name), want, "{name}: scalar scan");
        for id in [3, 4] {
            let r = db
                .execute(&format!("SELECT id, c, v FROM fix4640b.t WHERE id = {id}"))
                .await
                .unwrap_or_else(|e| panic!("{name} id={id}: {e}"));
            let got = rows_of(&r, &["v"], &format!("{name} id={id}"));
            let sub: Rows = want
                .iter()
                .filter(|(k, _)| k[0] == id)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            assert_eq!(got, sub, "{name} id={id}");
        }
    }
}
