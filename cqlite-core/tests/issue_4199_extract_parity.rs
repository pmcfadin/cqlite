//! Issue #4199 (spec R1.1) — reconciled `extract` of a single-generation
//! partition matches a direct decode of the source's own bytes for that key.
//!
//! # Oracle (#3042)
//!
//! `test_basic.composite_key_table` is a real, single-generation Cassandra
//! 5.0 fixture (`test-data/datasets/`). With exactly one generation there is
//! no cross-generation reconciliation to prove separately from a plain
//! decode (R1.2's two-generation case is the one that needs a genuine
//! reconcile-vs-concatenate distinction; deferred — see the PR description),
//! so this test's oracle is a direct `iterate_all_partitions_for_compaction`
//! decode of the SOURCE `Data.db`, filtered to the needle key — the same
//! decode-equality oracle `issue_4196_salvage_partition_atomicity.rs` uses
//! for its own R1 healthy-parity case.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::storage::write_engine::extract_split::{
    extract_partitions, parse_key_literal, ExtractOptions, Selection,
};
use tempfile::TempDir;

#[path = "support/extract_split_fixture.rs"]
mod fixture;
use fixture::{datasets_root, decode_all_rows, require_fixtures_strict, single_data_db, table_schema, KS, TABLE};

#[tokio::test]
async fn reconciled_extract_matches_a_direct_decode_of_the_source_for_the_key() {
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
    assert!(
        !source_rows.is_empty(),
        "fixture must decode at least one row (0-rows-when-present = failure)"
    );
    let needle = &source_rows[0];
    let needle_key = needle.key.0.to_vec();

    // The needle's own literal (a bare UUID — composite_key_table's
    // partition key is single-column).
    let literal = uuid::Uuid::from_slice(&needle_key)
        .expect("partition_key is a UUID")
        .to_string();

    let temp = TempDir::new().expect("tempdir");
    let out_dir = temp.path().join("out");
    let key_bytes =
        parse_key_literal(&literal, &schema).expect("parse the needle's own literal");
    assert_eq!(
        key_bytes, needle_key,
        "parse_key_literal must round-trip to the SAME raw key bytes Index.db carries"
    );

    let options = ExtractOptions {
        out_dir: out_dir.clone(),
        raw: false,
    };
    let report = extract_partitions(&table_dir, Selection::Key(key_bytes), &schema, options)
        .await
        .expect("extract_partitions must not error for a healthy input");

    assert!(
        report.refused.is_none(),
        "a healthy single-generation extract must not refuse; got {:?}",
        report.refused
    );
    assert!(
        report.not_found.is_empty(),
        "the needle key is known-live; not_found must be empty, got {:?}",
        report.not_found
    );
    assert_eq!(
        report.generations_written.len(),
        1,
        "reconciled mode writes exactly ONE output generation"
    );

    let output_table_dir = out_dir.join(&schema.keyspace).join(&schema.table);
    let output_data_db = single_data_db(&output_table_dir);
    let output_rows = decode_all_rows(&output_data_db, &schema).await;

    let expected: Vec<_> = source_rows
        .iter()
        .filter(|r| r.key.0.as_ref() == needle_key.as_slice())
        .cloned()
        .collect();
    assert_eq!(
        output_rows, expected,
        "extract's output rows for the needle key must byte-match a direct decode of the \
         source's own rows for that key"
    );
}
