//! The `TableProvider` surface: schema, projection, fail-closed open.

use super::support::*;

// ---------------------------------------------------------------------------
// Schema + projection mapping
// ---------------------------------------------------------------------------

#[test]
fn provider_schema_matches_the_producer_arrow_schema() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    let provider =
        CqliteTableProvider::open(schema.clone(), table_dir.clone(), TEST_BATCH_SIZE, false)
            .expect("provider");
    let expected = scan::build_producer(
        &ScanTarget {
            schema,
            dir: table_dir,
            batch_size: TEST_BATCH_SIZE,
        },
        ScanSpec::default(),
    )
    .expect("producer")
    .arrow_schema()
    .expect("arrow schema");
    assert_eq!(
        datafusion::catalog::TableProvider::schema(&provider).as_ref(),
        &expected
    );
}

#[test]
fn a_projected_query_emits_only_the_projected_columns_in_both_pushdown_modes() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        for pushdown_enabled in [false, true] {
            let rows = datafusion_rows(
                &table_dir,
                &schema,
                "SELECT ck, val FROM t",
                pushdown_enabled,
            );
            assert!(!rows.is_empty(), "the fixture must produce rows");
            for row in &rows {
                let columns: Vec<&String> = row.keys().collect();
                assert_eq!(
                    columns,
                    vec!["ck", "val"],
                    "pushdown={pushdown_enabled} must emit exactly the projected columns"
                );
            }
        }
    });
}

#[test]
fn opening_a_directory_with_no_sstables_fails_closed() {
    let temp = tempfile::TempDir::new().expect("tempdir");
    let error = CqliteTableProvider::open(
        testutil::clustering_schema(),
        temp.path().to_path_buf(),
        TEST_BATCH_SIZE,
        false,
    )
    .expect_err("an empty directory must not open as a 0-row table");
    assert!(
        error.to_string().contains("no SSTables found"),
        "unexpected error: {error}"
    );
}
