//! Issue #4279 — every clustering column is decoded BY SCHEMA, never as a blob.
//!
//! The legacy clustering decoder knew `int`/`uuid`/`bigint`/`timestamp`/`text` and returned
//! `Value::Blob` (after a WARN) for everything else, so a `DATE` or a `frozen<list<text>>`
//! clustering column reached callers as hex bytes and made schema-enforcing exports (Parquet)
//! fail. Oracle: the committed sstabledump goldens (`*-Data.db.jsonl`) of two Cassandra-5.0.8
//! tables; the varint clustering oracle lives in `issue_4360_varint_column_oracle.rs`.
//! Fixtures are required: a missing table is a failure, never a skip.

#![cfg(all(feature = "lz4", feature = "cli-helpers"))]

use std::collections::BTreeSet;
use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::Config;

#[path = "support/datasets_root.rs"]
mod datasets_root;

fn golden_lines(keyspace: &str, table: &str) -> (PathBuf, Vec<serde_json::Value>) {
    let root = datasets_root::sstables_root_for_table(keyspace, table).unwrap_or_else(|| {
        panic!(
            "fixture {keyspace}.{table} missing: {}",
            datasets_root::describe_search(keyspace, table)
        )
    });
    let dirs = datasets_root::table_generation_dirs(&root, keyspace, table);
    let dir = dirs
        .first()
        .unwrap_or_else(|| panic!("{keyspace}.{table}: no generation dir"));
    let text = std::fs::read_to_string(dir.join("nb-1-big-Data.db.jsonl"))
        .unwrap_or_else(|e| panic!("{keyspace}.{table}: golden unreadable: {e}"));
    let lines = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("golden line"))
        .collect();
    (root, lines)
}

fn uuid_text(b: &[u8; 16]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

fn civil_from_days(z: i64) -> String {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{:02}-{:02}", if m <= 2 { y + 1 } else { y }, m, d)
}

fn text_of(v: &Value) -> String {
    match v {
        Value::Text(b) => String::from_utf8(b.to_vec()).expect("utf8"),
        other => panic!("expected Text, got {other:?}"),
    }
}

async fn select_star(
    root: PathBuf,
    schema: &str,
    qualified: &str,
) -> Vec<std::collections::HashMap<String, Value>> {
    let schema = datasets_root::schema_path(schema).expect("committed schema readable");
    let db = ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .unwrap_or_else(|e| panic!("ingest failed: {e:?}"))
    .database;
    let result = db
        .execute(&format!("SELECT * FROM {qualified}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT * failed: {e:?}"));
    result
        .rows
        .iter()
        .map(|r| {
            r.values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect()
        })
        .collect()
}

/// DATE (and the other four clustering types of `wide_partition_table`) equal the golden, as the
/// SET of `(partition, clustering tuple)` — rendered from the decoded VALUES, so a `Blob` can never
/// match a date string.
#[tokio::test]
async fn date_clustering_equals_the_sstabledump_golden() {
    let (root, golden) = golden_lines("test_wide_rows", "wide_partition_table");
    let mut expected: BTreeSet<String> = BTreeSet::new();
    for part in &golden {
        let pk = part["partition"]["key"][0].as_str().expect("pk");
        for row in part["rows"].as_array().expect("rows") {
            if row["type"] != "row" {
                continue;
            }
            let c = row["clustering"].as_array().expect("clustering");
            let ts = c[0].as_str().expect("ts");
            expected.insert(format!(
                "{pk}|{ts}|{}|{}|{}|{}",
                c[1].as_str().expect("text"),
                c[2],
                c[3].as_str().expect("uuid"),
                c[4].as_str().expect("date")
            ));
        }
    }
    assert!(
        expected.len() > 50,
        "golden unexpectedly small: {}",
        expected.len()
    );

    let rows = select_star(root, "wide-rows.cql", "test_wide_rows.wide_partition_table").await;
    let mut actual: BTreeSet<String> = BTreeSet::new();
    for r in &rows {
        let pk = match r.get("partition_key") {
            Some(Value::Uuid(b)) => uuid_text(b),
            other => panic!("partition_key: {other:?}"),
        };
        let ts = match r.get("clustering_col1") {
            Some(Value::Timestamp(ms)) => *ms,
            other => panic!("clustering_col1: expected Timestamp, got {other:?}"),
        };
        let ts_text = {
            let secs = ts.div_euclid(1000);
            let ms = ts.rem_euclid(1000);
            let days = secs.div_euclid(86400);
            let sod = secs.rem_euclid(86400);
            format!(
                "{} {:02}:{:02}:{:02}.{:03}Z",
                civil_from_days(days),
                sod / 3600,
                (sod / 60) % 60,
                sod % 60,
                ms
            )
        };
        let date = match r.get("clustering_col5") {
            Some(Value::Date(d)) => civil_from_days(i64::from(*d)),
            other => panic!("clustering_col5: expected Value::Date, got {other:?}"),
        };
        let int = match r.get("clustering_col3") {
            Some(Value::Integer(i)) => *i,
            other => panic!("clustering_col3: {other:?}"),
        };
        let uuid = match r.get("clustering_col4") {
            Some(Value::Uuid(b)) => uuid_text(b),
            other => panic!("clustering_col4: {other:?}"),
        };
        actual.insert(format!(
            "{pk}|{ts_text}|{}|{int}|{uuid}|{date}",
            text_of(r.get("clustering_col2").expect("col2"))
        ));
    }
    assert_eq!(
        actual.len(),
        expected.len(),
        "row count differs from the golden"
    );
    assert_eq!(
        actual, expected,
        "decoded clustering tuples differ from the sstabledump golden"
    );
}

/// `clustering_key FROZEN<LIST<TEXT>>` decodes as a list of text, equal to the golden's list.
#[tokio::test]
async fn frozen_list_clustering_equals_the_sstabledump_golden() {
    let (root, golden) = golden_lines("test_collections", "collection_clustering_table");
    let mut expected: BTreeSet<String> = BTreeSet::new();
    for part in &golden {
        let pk = part["partition"]["key"][0].as_str().expect("pk");
        for row in part["rows"].as_array().expect("rows") {
            if row["type"] != "row" {
                continue;
            }
            let list: Vec<&str> = row["clustering"][0]
                .as_array()
                .expect("list clustering")
                .iter()
                .map(|x| x.as_str().expect("elem"))
                .collect();
            expected.insert(format!("{pk}|{}", list.join(",")));
        }
    }
    assert!(!expected.is_empty());

    let rows = select_star(
        root,
        "collections.cql",
        "test_collections.collection_clustering_table",
    )
    .await;
    let mut actual: BTreeSet<String> = BTreeSet::new();
    for r in &rows {
        let pk = match r.get("partition_key") {
            Some(Value::Uuid(b)) => uuid_text(b),
            other => panic!("partition_key: {other:?}"),
        };
        // `FROZEN<LIST<..>>` keeps its `Frozen` wrapper, as it does in every other column position.
        let list = match r.get("clustering_key").map(|v| match v {
            Value::Frozen(inner) => inner.as_ref(),
            other => other,
        }) {
            Some(Value::List(items)) => items.iter().map(text_of).collect::<Vec<_>>().join(","),
            other => panic!("clustering_key: expected Value::List, got {other:?}"),
        };
        actual.insert(format!("{pk}|{list}"));
    }
    assert_eq!(
        actual, expected,
        "decoded list clustering differs from the sstabledump golden"
    );
}
