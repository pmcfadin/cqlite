//! Arm equivalence — the guard against "faster because wrong".

use super::support::*;

// ---------------------------------------------------------------------------
// Arm equivalence — the guard against "faster because wrong"
// ---------------------------------------------------------------------------

#[test]
#[serial]
fn the_datafusion_arm_returns_the_row_engines_rows_in_the_same_order() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        let expected = direct_rows(&table_dir, &schema, ScanSpec::default());
        // Post-reconciliation the fixture keeps 7 of its 8 written rows: `(1, d)`
        // was row-deleted, `(1, b)` and `(2, c)` take their generation-2 values.
        assert_eq!(expected.len(), 7, "unexpected reconciled row count");
        assert!(
            expected
                .iter()
                .any(|r| r.get("val").is_some_and(|v| v == "21")),
            "the generation-2 overwrite of (1, b) must win: {expected:?}"
        );
        assert!(
            !expected
                .iter()
                .any(|r| r.get("val").is_some_and(|v| v == "40")),
            "the row-deleted (1, d) must not survive: {expected:?}"
        );

        let actual = datafusion_rows(&table_dir, &schema, "SELECT pk, ck, val FROM t", false);
        assert_eq!(
            actual, expected,
            "the DataFusion arm and the row-engine arm must return identical rows, values \
             and order over the same batches"
        );
    });
}

#[test]
#[serial]
fn exact_pushdown_selects_exactly_the_rows_datafusion_would_filter_itself() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        // Same SQL, both pushdown modes. With pushdown ON the predicate is
        // applied INSIDE the scan and reported `Exact`, so DataFusion drops its
        // own `FilterExec`; with it OFF DataFusion filters the full batches
        // itself. Identical results are what makes the `Exact` claim honest.
        let sql = "SELECT pk, ck, val FROM t WHERE val > 25";
        let with_pushdown = datafusion_rows(&table_dir, &schema, sql, true);
        let without_pushdown = datafusion_rows(&table_dir, &schema, sql, false);
        assert!(
            !with_pushdown.is_empty(),
            "the predicate must select some rows, or the test proves nothing"
        );
        assert_eq!(
            with_pushdown, without_pushdown,
            "an `Exact` pushdown must select exactly the rows DataFusion's own filter selects"
        );
    });
}

#[test]
#[serial]
fn the_row_wise_arm_counts_the_same_rows_as_a_vectorized_filter() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        // Row-wise count over the produced batches...
        let target = ScanTarget {
            schema: schema.clone(),
            dir: table_dir.clone(),
            batch_size: TEST_BATCH_SIZE,
        };
        let producer = Arc::new(scan::build_producer(&target, ScanSpec::default()).expect("p"));
        let paths = scan::resolve_paths(&producer, &target).expect("resolve");
        let mut running = scan::spawn_scan(producer, paths);
        let mut rowwise_matches: u64 = 0;
        while let Some(item) = running.batches.blocking_recv() {
            let batch = item.expect("batch");
            rowwise_matches +=
                count_matching_rowwise(&batch, "val", RowOp::Gt, &RowLiteral::parse("25"))
                    .expect("row-wise compare");
        }
        running.done.join().expect("thread").result.expect("scan");

        // ...must equal DataFusion's vectorized count over the same batches.
        let vectorized = datafusion_rows(
            &table_dir,
            &schema,
            "SELECT count(*) FROM t WHERE val > 25",
            false,
        );
        assert_eq!(vectorized.len(), 1, "count(*) yields one row");
        let counted = vectorized[0].values().next().expect("count column").clone();
        assert_eq!(
            counted,
            rowwise_matches.to_string(),
            "the row-wise arm and the vectorized arm must agree on the matching row count"
        );
    });
}

#[test]
fn the_row_wise_arm_rejects_a_type_it_cannot_compare_instead_of_matching_nothing() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    let target = ScanTarget {
        schema,
        dir: table_dir,
        batch_size: TEST_BATCH_SIZE,
    };
    let producer = Arc::new(scan::build_producer(&target, ScanSpec::default()).expect("p"));
    let paths = scan::resolve_paths(&producer, &target).expect("resolve");
    let mut running = scan::spawn_scan(producer, paths);
    let batch = running
        .batches
        .blocking_recv()
        .expect("at least one batch")
        .expect("batch");
    // `ck` is text; an integer operand must be REFUSED, not silently matched by
    // nothing — a filter that quietly matches nothing would make the arm look
    // arbitrarily fast.
    let error = count_matching_rowwise(&batch, "ck", RowOp::Gt, &RowLiteral::parse("25"))
        .expect_err("an integer operand against a text column must be refused");
    assert!(
        error.to_string().contains("does not match column"),
        "unexpected error: {error}"
    );
    // Drain so the producer thread finishes rather than being cancelled mid-scan.
    while running.batches.blocking_recv().is_some() {}
    let _ = running.done.join();
}

// ---------------------------------------------------------------------------
// `count(*)` under pushdown — the empty projection must not empty the scan
// ---------------------------------------------------------------------------

#[test]
#[serial]
fn count_star_is_correct_with_pushdown_enabled_and_disabled() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        // DataFusion asks a provider for ZERO columns when it only needs a row
        // count. Pushing that empty projection into the scan makes the producer
        // emit zero-column batches, which carry no explicit row count, so every
        // row disappears and `count(*)` answers 0 — instantly, and wrongly. The
        // fixture keeps 7 rows after reconciliation, so 0 (or 8, the pre-
        // reconciliation count) fails here.
        assert_eq!(
            datafusion_count(&table_dir, &schema, true),
            7,
            "count(*) with pushdown ENABLED must count the reconciled rows"
        );
        assert_eq!(
            datafusion_count(&table_dir, &schema, false),
            7,
            "count(*) with pushdown disabled must give the same answer"
        );
    });
}

#[test]
#[serial]
fn a_pushed_down_count_narrows_the_scan_to_one_column_and_keeps_the_rows() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        let provider = Arc::new(
            CqliteTableProvider::open(schema.clone(), table_dir.clone(), TEST_BATCH_SIZE, true)
                .expect("provider"),
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let provider_for_query = provider.clone();
        runtime.block_on(async move {
            let ctx =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
            ctx.register_table("t", provider_for_query)
                .expect("register");
            // The physical plan names what was pushed, so the narrowing is
            // asserted from the plan rather than from an internal field.
            let explained = ctx
                .sql("EXPLAIN SELECT count(*) FROM t")
                .await
                .expect("plan")
                .collect()
                .await
                .expect("explain");
            let text: String =
                explained
                    .iter()
                    .flat_map(rows_of)
                    .fold(String::new(), |mut acc, row| {
                        for value in row.values() {
                            acc.push_str(value);
                            acc.push('\n');
                        }
                        acc
                    });
            // The narrowing is the POINT of pushing an anchor column rather than
            // pushing nothing: the scan must still be narrow.
            assert!(
                text.contains("count-only anchor"),
                "the count scan should be anchored to one column:\n{text}"
            );
            let batches = ctx
                .sql("SELECT count(*) FROM t")
                .await
                .expect("plan")
                .collect()
                .await
                .expect("run");
            assert_eq!(batches.len(), 1);
        });
        let outcome = provider.last_scan_outcome().expect("a scan completed");
        // Every row must still reach the aggregate.
        assert_eq!(
            outcome.rows, 7,
            "the anchored scan must emit one row per reconciled row"
        );
    });
}

#[test]
#[serial]
fn the_row_wise_count_visits_every_row_of_every_batch() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        let target = ScanTarget {
            schema,
            dir: table_dir,
            batch_size: TEST_BATCH_SIZE,
        };
        let producer = Arc::new(scan::build_producer(&target, ScanSpec::default()).expect("p"));
        let paths = scan::resolve_paths(&producer, &target).expect("resolve");
        let mut running = scan::spawn_scan(producer, paths);
        let (mut counted, mut batches) = (0u64, 0u32);
        while let Some(next) = running.batches.blocking_recv() {
            let batch = next.expect("batch");
            counted += count_rows_rowwise(&batch);
            batches += 1;
        }
        let _ = running.done.join();
        // 7 rows survive reconciliation, spread over more than one batch at
        // TEST_BATCH_SIZE=4 — so a per-batch off-by-one cannot hide.
        assert!(batches > 1, "expected several batches, got {batches}");
        assert_eq!(
            counted, 7,
            "the row-wise count must total the reconciled rows"
        );
        // NOTE: that the loop is not OPTIMIZED AWAY cannot be asserted from
        // Rust — it is a codegen property. `count_rows_rowwise` prevents the fold
        // structurally with `std::hint::black_box`; this test pins the ANSWER,
        // and the doc comment there owns the timing claim.
    });
}
