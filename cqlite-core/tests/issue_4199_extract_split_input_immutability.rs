//! Issue #4199 (spec R5.1) — neither `extract_partitions` nor `split_sstable`
//! ever writes, truncates, or otherwise alters a byte of any input SSTable
//! component. Covers the successful-run leg for both verbs (reconciled
//! extract of a live key, and a `--parts` split); the refusal-leg half of
//! R5.1 (a decode-failure run also leaving the input untouched) is exercised
//! incidentally by `issue_4199_extract_refuses_on_decode_failure.rs`'s own
//! before/after comparison on the corrupted fixture it stages.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::storage::write_engine::extract_split::{
    extract_partitions, split_sstable, ExtractOptions, Selection, SplitBoundary,
};
use tempfile::TempDir;

#[path = "support/extract_split_fixture.rs"]
mod fixture;
use fixture::{
    component_hashes, datasets_root, decode_all_rows, require_fixtures_strict, single_data_db,
    table_schema, KS, TABLE,
};

#[tokio::test]
async fn extract_leaves_every_input_component_byte_identical() {
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
    let needle_key = source_rows[0].key.0.to_vec();

    let before = component_hashes(&table_dir);
    assert!(
        !before.is_empty(),
        "the fixture dir must hold components to hash"
    );

    let temp = TempDir::new().expect("tempdir");
    let options = ExtractOptions {
        out_dir: temp.path().join("out"),
        raw: false,
    };
    let report = extract_partitions(&table_dir, Selection::Key(needle_key), &schema, options)
        .await
        .expect("extract must not error");
    assert!(report.refused.is_none());

    let after = component_hashes(&table_dir);
    assert_eq!(
        before, after,
        "every input component's sha256 must be UNCHANGED after a successful extract run"
    );
}

#[tokio::test]
async fn split_leaves_every_input_component_byte_identical() {
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
    let data_db = single_data_db(&table_dir);
    let before = component_hashes(&table_dir);
    assert!(!before.is_empty());

    let temp = TempDir::new().expect("tempdir");
    let report = split_sstable(
        &data_db,
        SplitBoundary::Parts(2),
        &temp.path().join("out"),
        &schema,
    )
    .await
    .expect("split must not error");
    assert!(report.refused.is_none(), "got {:?}", report.refused);

    let after = component_hashes(&table_dir);
    assert_eq!(
        before, after,
        "every input component's sha256 must be UNCHANGED after a successful split run"
    );
}
