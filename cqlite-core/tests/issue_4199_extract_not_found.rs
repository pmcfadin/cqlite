//! Issue #4199 (spec R3.1) — a `--keys-file`-style `Selection::KeySet` naming
//! a mix of live and guaranteed-absent keys reports the absent ones BY NAME
//! in `not_found`, and still writes the live keys' output dump-equal to the
//! source.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::storage::write_engine::extract_split::{extract_partitions, ExtractOptions, Selection};
use tempfile::TempDir;

#[path = "support/extract_split_fixture.rs"]
mod fixture;
use fixture::{datasets_root, decode_all_rows, require_fixtures_strict, single_data_db, table_schema, KS, TABLE};

#[tokio::test]
async fn keys_file_with_some_keys_absent_from_every_generation() {
    let Ok(table_dir) = datasets_root::resolve_table_generation_dir(KS, TABLE) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {KS}.{TABLE} is absent; {}",
                datasets_root::describe_search(KS, TABLE)
            );
        }
        eprintln!("[issue_4199] {KS}.{TABLE} fixture absent (dataset not fetched); skipping");
        return;
    };

    let schema = table_schema();
    let source_data_db = single_data_db(&table_dir);
    let source_rows = decode_all_rows(&source_data_db, &schema).await;

    // 3 distinct live keys, in file order.
    let mut live_keys: Vec<Vec<u8>> = Vec::new();
    for row in &source_rows {
        let k = row.key.0.to_vec();
        if !live_keys.contains(&k) {
            live_keys.push(k);
        }
        if live_keys.len() == 3 {
            break;
        }
    }
    assert_eq!(
        live_keys.len(),
        3,
        "fixture must carry at least 3 distinct live partitions"
    );

    // 2 synthetic keys guaranteed absent: random UUIDs, checked against every
    // live key (collision probability is negligible but checked anyway, not
    // assumed).
    let mut absent_keys: Vec<Vec<u8>> = Vec::new();
    while absent_keys.len() < 2 {
        let candidate = uuid::Uuid::new_v4().as_bytes().to_vec();
        if !live_keys.contains(&candidate) && !absent_keys.contains(&candidate) {
            absent_keys.push(candidate);
        }
    }

    let mut requested = live_keys.clone();
    requested.extend(absent_keys.iter().cloned());

    let temp = TempDir::new().expect("tempdir");
    let out_dir = temp.path().join("out");
    let options = ExtractOptions {
        out_dir: out_dir.clone(),
        raw: false,
    };
    let report = extract_partitions(&table_dir, Selection::KeySet(requested), &schema, options)
        .await
        .expect("extract_partitions must not error");

    assert!(
        report.refused.is_none(),
        "a healthy input naming absent keys is NOT a refusal; got {:?}",
        report.refused
    );

    let expected_not_found: std::collections::BTreeSet<String> =
        absent_keys.iter().map(hex::encode).collect();
    let actual_not_found: std::collections::BTreeSet<String> =
        report.not_found.iter().cloned().collect();
    assert_eq!(
        actual_not_found, expected_not_found,
        "not_found must name EXACTLY the 2 absent keys, by hex, never more or fewer"
    );

    let output_table_dir = out_dir.join(&schema.keyspace).join(&schema.table);
    let output_data_db = single_data_db(&output_table_dir);
    let output_rows = decode_all_rows(&output_data_db, &schema).await;

    for key in &live_keys {
        let expected: Vec<_> = source_rows
            .iter()
            .filter(|r| r.key.0.as_ref() == key.as_slice())
            .cloned()
            .collect();
        let actual: Vec<_> = output_rows
            .iter()
            .filter(|r| r.key.0.as_ref() == key.as_slice())
            .cloned()
            .collect();
        assert_eq!(
            actual, expected,
            "live key {} must be dump-equal to the source in the output",
            hex::encode(key)
        );
    }
}
