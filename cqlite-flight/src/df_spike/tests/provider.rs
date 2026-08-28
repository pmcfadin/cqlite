//! The `TableProvider` surface: schema, projection, fail-closed open.

use super::support::*;
// `scan()` is a `TableProvider` method; the trait must be in scope to call it.
use datafusion::datasource::TableProvider;

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

/// The result's COLUMN ORDER for `sql`, as the plan produces it.
///
/// `rows_of` renders a batch into a `BTreeMap` keyed by column name, which
/// discards order — so an order bug is invisible to every helper above. This
/// reads the schema the batches actually carry.
fn result_column_order(
    table_dir: &std::path::Path,
    schema: &TableSchema,
    sql: &str,
    pushdown: bool,
) -> Vec<String> {
    let provider = Arc::new(
        CqliteTableProvider::open(
            schema.clone(),
            table_dir.to_path_buf(),
            TEST_BATCH_SIZE,
            pushdown,
        )
        .expect("provider"),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async move {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        ctx.register_table("t", provider).expect("register");
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let batch = batches.first().expect("at least one batch");
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    })
}

#[test]
#[serial]
fn a_reversed_projection_keeps_the_requested_column_order_under_pushdown() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        // `val, ck` REVERSES the table's key-first schema order (pk, ck, val).
        // `MergeProducer::with_spec` honours WHICH columns to produce but emits
        // them key-first, so a pushed projection handed DataFusion `ck, val`
        // under a plan built for `val, ck` — a schema/order mismatch that gives
        // any positional consumer the wrong column. No published number was
        // affected (every DataFusion bench cell ran with pushdown OFF), but the
        // shipped feature-gated path was wrong.
        let pushed = result_column_order(&table_dir, &schema, "SELECT val, ck FROM t", true);
        assert_eq!(
            pushed,
            vec!["val".to_string(), "ck".to_string()],
            "a pushed projection must return the REQUESTED column order, not the producer's \
             key-first order"
        );
        // And the two pushdown modes must agree, values included.
        assert_eq!(
            pushed,
            result_column_order(&table_dir, &schema, "SELECT val, ck FROM t", false),
            "pushdown must not change the result's shape"
        );
        assert_eq!(
            datafusion_rows(&table_dir, &schema, "SELECT val, ck FROM t", true),
            datafusion_rows(&table_dir, &schema, "SELECT val, ck FROM t", false),
            "pushdown must not change the result's values"
        );
    });
}

#[test]
#[serial]
fn a_pushed_projection_returns_a_plan_in_the_requested_column_order() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        let provider =
            CqliteTableProvider::open(schema, table_dir, TEST_BATCH_SIZE, true).expect("provider");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let ctx = SessionContext::new();
            // Indices into the table schema (pk, ck, val), REVERSED: `val, ck`.
            let plan = provider
                .scan(&ctx.state(), Some(&vec![2, 1]), &[], None)
                .await
                .expect("scan plans");
            let order: Vec<String> = plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            // `MergeProducer::with_spec` RETAINS the table's key-first column
            // order when it applies a projection, so it emits `ck, val` for this
            // request. Returning that plan unchanged breaks the `TableProvider`
            // contract, which says the plan's schema is the table schema
            // PROJECTED — i.e. in the requested order. DataFusion 44 happens to
            // reconcile the mismatch above the scan, so no wrong answer was
            // observed end-to-end and no published number was affected (every
            // DataFusion bench cell ran with pushdown OFF); but the provider was
            // relying on that tolerance rather than honouring the contract, and
            // an engine that trusted the declared schema would read the wrong
            // column. This assertion is on the contract, and it FAILS without
            // the post-scan reorder.
            assert_eq!(
                order,
                vec!["val".to_string(), "ck".to_string()],
                "the plan must expose the REQUESTED column order, not the producer's key-first order"
            );
        });
    });
}
