//! Tests for the DataFusion spike (issue #2605).
//!
//! # The oracle these tests use, and why a CQLite-written fixture is legitimate
//!
//! `CLAUDE.md` is emphatic that a CQLite-written + CQLite-read round trip is
//! invariant to a uniform framing error and can never validate an ON-DISK
//! encoding property. That rule does not apply to the property under test here.
//! The property is **arm equivalence**: two EXECUTION engines, reading the SAME
//! bytes through the SAME decoder, must return the same rows in the same order.
//! Both arms share the whole read path, so a decode defect would move both
//! answers identically and could not manufacture a pass — exactly the shape of
//! the `#1918` point-vs-full differential lane and the `#3058` forced-path
//! differential lane, both of which use in-process fixtures for the same reason.
//!
//! The thing these tests exist to stop is a `TableProviderFilterPushDown::Exact`
//! claim for a predicate the scan does not actually apply. That would drop or
//! keep the wrong rows and make the DataFusion arm look FASTER by being WRONG —
//! the one failure mode that would invalidate the whole measurement.
//!
//! `now` is PINNED to a constant (`#2642`: never a wall-clock read), and the
//! fixture carries no TTL, so the read is deterministic.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{col, lit, Expr, TableProviderFilterPushDown};
use datafusion::prelude::{SessionConfig, SessionContext};
use serial_test::serial;

use cqlite_core::schema::TableSchema;

use crate::df_spike::bench::{ArmKind, BenchConfig, BenchRunner, Scenario, ScenarioKind};
use crate::df_spike::provider::CqliteTableProvider;
use crate::df_spike::pushdown;
use crate::df_spike::rowwise::{count_matching_rowwise, RowLiteral, RowOp};
use crate::df_spike::scan::{self, ScanTarget};
use crate::filter::ScanSpec;
use crate::testutil;

/// Debug-only read-clock pin (see the module docs).
const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";
/// Pinned base write timestamp, in microseconds.
const T_BASE_MICROS: i64 = 1_700_000_000_000_000;
/// Pinned read clock, in epoch seconds.
const PINNED_NOW_SECS: i64 = 1_700_000_100;
/// Batch size small enough that the fixture spans several batches, so a
/// per-batch bug cannot hide behind a single-batch result.
const TEST_BATCH_SIZE: usize = 4;

/// A row rendered as an ordered `column -> value` map, for order-sensitive
/// comparison between the two arms.
type Row = BTreeMap<String, String>;

/// A two-generation fixture with cross-generation reconciliation to perform:
/// generation 1 writes `(1, a..e)` and `(2, a..c)`; generation 2 overwrites two
/// of those rows at a NEWER timestamp and row-deletes a third. So the surviving
/// result set is only obtainable by reconciling the two generations — which is
/// what makes the "the merge arm ran" assertion meaningful.
fn two_generation_fixture() -> (tempfile::TempDir, std::path::PathBuf, TableSchema) {
    let schema = testutil::clustering_schema();
    let gen1 = vec![
        testutil::write_clustered(1, "a", 10, T_BASE_MICROS),
        testutil::write_clustered(1, "b", 20, T_BASE_MICROS),
        testutil::write_clustered(1, "c", 30, T_BASE_MICROS),
        testutil::write_clustered(1, "d", 40, T_BASE_MICROS),
        testutil::write_clustered(1, "e", 50, T_BASE_MICROS),
        testutil::write_clustered(2, "a", 60, T_BASE_MICROS),
        testutil::write_clustered(2, "b", 70, T_BASE_MICROS),
        testutil::write_clustered(2, "c", 80, T_BASE_MICROS),
    ];
    let gen2 = vec![
        // Newer values shadow generation 1 (last-write-wins).
        testutil::write_clustered(1, "b", 21, T_BASE_MICROS + 1_000),
        testutil::write_clustered(2, "c", 81, T_BASE_MICROS + 1_000),
        // A row tombstone removes `(1, d)` from the result set entirely.
        testutil::delete_clustered(1, "d", T_BASE_MICROS + 1_000),
    ];
    let (temp, _data_dir, table_dir) = testutil::build_sstables(&schema, vec![gen1, gen2]);
    (temp, table_dir, schema)
}

/// Run the pinned-clock body with the read clock override installed.
fn with_pinned_now<T>(body: impl FnOnce() -> T) -> T {
    let previous = std::env::var(TTL_NOW_ENV).ok();
    std::env::set_var(TTL_NOW_ENV, PINNED_NOW_SECS.to_string());
    let out = body();
    match previous {
        Some(v) => std::env::set_var(TTL_NOW_ENV, v),
        None => std::env::remove_var(TTL_NOW_ENV),
    }
    out
}

/// Render a batch as ordered rows of `column -> value` strings.
fn rows_of(batch: &RecordBatch) -> Vec<Row> {
    let schema = batch.schema();
    (0..batch.num_rows())
        .map(|i| {
            schema
                .fields()
                .iter()
                .enumerate()
                .map(|(c, field)| (field.name().clone(), cell_text(batch.column(c), i)))
                .collect()
        })
        .collect()
}

/// A single cell as text. Only the fixture's types appear; anything else is
/// reported loudly rather than rendered as a placeholder that could compare
/// equal to another unsupported cell.
fn cell_text(array: &dyn Array, index: usize) -> String {
    if array.is_null(index) {
        return "NULL".to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return a.value(index).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        // DataFusion's `count(*)` result column.
        return a.value(index).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return a.value(index).to_string();
    }
    panic!(
        "test fixture produced an unexpected Arrow type: {}",
        array.data_type()
    );
}

/// Drive the producer directly (the row-engine arm's batch source) and return
/// every row, in emission order.
fn direct_rows(table_dir: &std::path::Path, schema: &TableSchema, spec: ScanSpec) -> Vec<Row> {
    let target = ScanTarget {
        schema: schema.clone(),
        dir: table_dir.to_path_buf(),
        batch_size: TEST_BATCH_SIZE,
    };
    let producer = Arc::new(scan::build_producer(&target, spec).expect("producer"));
    let paths = scan::resolve_paths(&producer, &target).expect("resolve");
    assert!(
        paths.len() >= 2,
        "the fixture must present >= 2 generations to reconcile, found {}",
        paths.len()
    );
    let mut running = scan::spawn_scan(producer, paths);
    let mut rows = Vec::new();
    while let Some(item) = running.batches.blocking_recv() {
        rows.extend(rows_of(&item.expect("batch")));
    }
    let outcome = running.done.join().expect("producer thread");
    outcome.result.expect("scan completed");
    assert!(
        outcome.probe.merge_arm_observed(),
        "the k-way MERGE arm must have served the scan (reconcile_entries={}, \
         cell_metadata_maps={}); the bypass arm would make the arms incomparable",
        outcome.probe.reconcile_entries,
        outcome.probe.cell_metadata_maps
    );
    rows
}

/// Run `sql` through the DataFusion arm and return the result rows in order.
fn datafusion_rows(
    table_dir: &std::path::Path,
    schema: &TableSchema,
    sql: &str,
    pushdown_enabled: bool,
) -> Vec<Row> {
    let provider = Arc::new(
        CqliteTableProvider::open(
            schema.clone(),
            table_dir.to_path_buf(),
            TEST_BATCH_SIZE,
            pushdown_enabled,
        )
        .expect("provider"),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async move {
        // `target_partitions = 1` for the TESTS only. Our scan is a single
        // partition, so DataFusion's default parallelism can only add a
        // round-robin `RepartitionExec` above it — which is free to interleave
        // batches and makes output ORDER nondeterministic. SQL without `ORDER
        // BY` guarantees no order, so that is DataFusion behaving correctly; but
        // an order-sensitive equivalence assertion needs a deterministic plan.
        // The BENCH deliberately leaves the default in place (a realistic
        // configuration is what should be measured).
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        ctx.register_table("t", provider).expect("register");
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        batches.iter().flat_map(rows_of).collect()
    })
}

// ---------------------------------------------------------------------------
// Pushdown classification — the "never claim Exact for what we do not apply" rule
// ---------------------------------------------------------------------------

#[test]
fn translatable_comparisons_classify_as_exact() {
    let schema = testutil::clustering_schema();
    let filters = vec![
        col("val").eq(lit(30_i32)),
        col("val").lt(lit(30_i32)),
        col("val").gt_eq(lit(30_i32)),
        // Literal-first: must be mirrored to `val > 30`, not `val < 30`.
        lit(30_i32).lt(col("val")),
        col("val").not_eq(lit(30_i32)),
        col("val").is_null(),
        col("val").is_not_null(),
        col("ck").eq(lit("a")),
        col("val").in_list(vec![lit(10_i32), lit(20_i32)], false),
        col("val").between(lit(10_i32), lit(30_i32)),
    ];
    let refs: Vec<&Expr> = filters.iter().collect();
    let verdicts = pushdown::classify(&refs, &schema);
    for (filter, verdict) in filters.iter().zip(verdicts.iter()) {
        assert_eq!(
            *verdict,
            TableProviderFilterPushDown::Exact,
            "expected Exact for {filter:?}"
        );
    }
}

#[test]
fn literal_first_comparison_mirrors_the_operator() {
    let schema = testutil::clustering_schema();
    // `30 < val` must lower to `val > 30`. Asserted through the row sets the two
    // spellings select, so a mirroring bug cannot hide behind a matching verdict.
    let mirrored = pushdown::translate_all(&[lit(30_i32).lt(col("val"))], &schema)
        .expect("mirrored form is translatable");
    let direct = pushdown::translate_all(&[col("val").gt(lit(30_i32))], &schema)
        .expect("direct form is translatable");
    assert_eq!(
        format!("{mirrored:?}"),
        format!("{direct:?}"),
        "`30 < val` must translate identically to `val > 30`"
    );
}

#[test]
fn untranslatable_filters_classify_as_unsupported() {
    let schema = testutil::clustering_schema();
    let filters = [
        // Unknown column: production lowering rejects it, so the claim is refused.
        col("nope").eq(lit(1_i32)),
        // Column-to-column comparison: the scan path evaluates column-vs-literal.
        col("val").eq(col("pk")),
        // A cast can change comparison semantics, and no oracle proves CQLite's
        // coercion matches DataFusion's.
        Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(col("val")),
            arrow::datatypes::DataType::Int64,
        ))
        .eq(lit(1_i64)),
        // Operand type that does not coerce to the column's CQL type.
        col("val").eq(lit("not-an-int")),
    ];
    let refs: Vec<&Expr> = filters.iter().collect();
    let verdicts = pushdown::classify(&refs, &schema);
    for (filter, verdict) in filters.iter().zip(verdicts.iter()) {
        assert_eq!(
            *verdict,
            TableProviderFilterPushDown::Unsupported,
            "expected Unsupported for {filter:?}"
        );
    }
}

#[test]
fn pushdown_disabled_reports_every_filter_unsupported() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    let provider =
        CqliteTableProvider::open(schema, table_dir, TEST_BATCH_SIZE, false).expect("provider");
    let filter = col("val").eq(lit(30_i32));
    let verdicts =
        datafusion::catalog::TableProvider::supports_filters_pushdown(&provider, &[&filter])
            .expect("verdicts");
    assert_eq!(verdicts, vec![TableProviderFilterPushDown::Unsupported]);
}

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
// `rows_result` provenance — a projected VALUE is never a row COUNT
// ---------------------------------------------------------------------------

#[test]
fn only_the_count_scenarios_are_scalar_aggregates() {
    // The DataFusion arm decides whether to read `rows_result` out of a returned
    // scalar from THIS predicate, i.e. from the query it asked for. Deciding it
    // from the batch SHAPE instead is what let a 1-row/1-column projected batch
    // be reported as a row count.
    assert!(ScenarioKind::FullScanCount.is_scalar_aggregate());
    assert!(ScenarioKind::FilteredScan.is_scalar_aggregate());
    assert!(
        !ScenarioKind::ProjectedScan.is_scalar_aggregate(),
        "a projected scan returns ROWS; its result count is the number drained"
    );
}

#[test]
#[serial]
fn a_single_column_projected_scan_reports_rows_drained_not_a_cell_value() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    with_pinned_now(|| {
        let runner = BenchRunner::new(BenchConfig {
            dir: table_dir.clone(),
            schema: schema.clone(),
            // One row per batch: the shape that made the superseded
            // batch-shape test misclassify a projected batch as a scalar.
            batch_size: 1,
            projection: vec!["val".to_string()],
            filter_column: "ck".to_string(),
            filter_op: RowOp::Lt,
            filter_value: RowLiteral::parse("c"),
            iterations: 1,
            df_target_partitions: Some(1),
        });
        let outcome = runner
            .run_one(
                Scenario {
                    kind: ScenarioKind::ProjectedScan,
                    arm: ArmKind::DataFusion,
                },
                1,
            )
            .expect("the projected DataFusion arm runs");
        // 7 rows survive reconciliation, so 7 rows are drained — never `1` (a
        // retained single-row batch) and never a `val` cell value (10/21/30/...).
        assert_eq!(
            outcome.rows_result, 7,
            "rows_result must be the rows the query returned, not a cell value"
        );
        assert_eq!(
            outcome.rows_scanned, 7,
            "the scan feeds all reconciled rows"
        );
    });
}

// ---------------------------------------------------------------------------
// The harness's CLI contract
//
// These are the surfaces the bench binary parses operator input through, and a
// mistake in any of them silently produces a plausible number for the WRONG
// query — an operator/value pair that means something other than what was typed.
// ---------------------------------------------------------------------------

#[test]
fn row_op_spellings_parse_to_the_operator_they_name() {
    use crate::df_spike::rowwise::RowOp;
    for (spellings, expected, sql) in [
        (["eq", "="].as_slice(), RowOp::Eq, "="),
        (["ne", "neq", "!=", "<>"].as_slice(), RowOp::NotEq, "<>"),
        (["lt", "<"].as_slice(), RowOp::Lt, "<"),
        (["lte", "le", "<="].as_slice(), RowOp::LtEq, "<="),
        (["gt", ">"].as_slice(), RowOp::Gt, ">"),
        (["gte", "ge", ">="].as_slice(), RowOp::GtEq, ">="),
    ] {
        for spelling in spellings {
            assert_eq!(
                RowOp::parse(spelling),
                Some(expected),
                "spelling '{spelling}' must parse to {expected:?}"
            );
        }
        // The SQL rendering must be the SAME operator, or the DataFusion arm
        // would run a different query than the row arm evaluates.
        assert_eq!(expected.sql(), sql);
    }
    // An unrecognized operator is REFUSED, never defaulted — a default would
    // silently run a different comparison than the operator asked for.
    assert_eq!(RowOp::parse("approximately"), None);
    assert_eq!(RowOp::parse(""), None);
}

#[test]
fn row_literal_parsing_prefers_the_narrowest_type_and_escapes_sql() {
    use crate::df_spike::rowwise::RowLiteral;
    assert!(matches!(RowLiteral::parse("true"), RowLiteral::Bool(true)));
    assert!(matches!(
        RowLiteral::parse("false"),
        RowLiteral::Bool(false)
    ));
    assert!(matches!(RowLiteral::parse("-7"), RowLiteral::Int(-7)));
    assert!(matches!(RowLiteral::parse("1.5"), RowLiteral::Float(_)));
    assert!(matches!(RowLiteral::parse("abc"), RowLiteral::Text(_)));
    // A non-finite float has no JSON number form and no useful comparison, so it
    // must fall through to text rather than become a `Float(NaN)` that compares
    // false against everything.
    assert!(matches!(RowLiteral::parse("NaN"), RowLiteral::Text(_)));
    assert!(matches!(RowLiteral::parse("inf"), RowLiteral::Text(_)));

    // SQL string literals: the quote must be doubled, or an operand containing
    // one would terminate the literal and change the query.
    assert_eq!(RowLiteral::parse("O'Hara").sql(), "'O''Hara'");
    assert_eq!(RowLiteral::parse("7").sql(), "7");
    assert_eq!(RowLiteral::parse("true").sql(), "true");

    // The JSON form is what production lowering coerces against the CQL type.
    assert_eq!(RowLiteral::parse("7").json(), serde_json::json!(7));
    assert_eq!(RowLiteral::parse("x").json(), serde_json::json!("x"));
    assert_eq!(RowLiteral::parse("true").json(), serde_json::json!(true));
}

#[test]
fn scenario_and_arm_identifiers_round_trip() {
    use crate::df_spike::{ArmKind, ScenarioKind};
    for scenario in ScenarioKind::all() {
        assert_eq!(ScenarioKind::parse(scenario.id()), Some(scenario));
    }
    for arm in ArmKind::all() {
        assert_eq!(ArmKind::parse(arm.id()), Some(arm));
    }
    // Unknown identifiers are refused so a typo cannot silently select a
    // different scenario/arm than the one named on the command line.
    assert_eq!(ScenarioKind::parse("full_scan"), None);
    assert_eq!(ArmKind::parse("data_fusion"), None);
    // The ids are distinct, or two cells would overwrite each other's results.
    let scenario_ids: std::collections::BTreeSet<_> =
        ScenarioKind::all().iter().map(|s| s.id()).collect();
    assert_eq!(scenario_ids.len(), ScenarioKind::all().len());
    let arm_ids: std::collections::BTreeSet<_> = ArmKind::all().iter().map(|a| a.id()).collect();
    assert_eq!(arm_ids.len(), ArmKind::all().len());
}

#[test]
fn the_datafusion_sql_matches_the_scenario_and_the_row_arms_filter() {
    use crate::df_spike::bench::{BenchConfig, BenchRunner};
    use crate::df_spike::rowwise::{RowLiteral, RowOp};
    let runner = BenchRunner::new(BenchConfig {
        dir: std::path::PathBuf::from("/nonexistent"),
        schema: testutil::clustering_schema(),
        batch_size: TEST_BATCH_SIZE,
        projection: vec!["ck".into(), "val".into()],
        filter_column: "val".into(),
        filter_op: RowOp::Gt,
        filter_value: RowLiteral::parse("25"),
        iterations: 1,
        df_target_partitions: Some(1),
    });
    assert_eq!(
        runner.sql_for(crate::df_spike::ScenarioKind::FullScanCount),
        "SELECT count(*) FROM t"
    );
    // Column names are quoted, so a column whose name collides with a SQL
    // keyword still resolves to the column.
    assert_eq!(
        runner.sql_for(crate::df_spike::ScenarioKind::ProjectedScan),
        "SELECT \"ck\", \"val\" FROM t"
    );
    // The predicate rendered for DataFusion must be the SAME comparison the
    // row-wise arm evaluates (operator AND operand), or the two arms would be
    // answering different questions and the delta would be meaningless.
    assert_eq!(
        runner.sql_for(crate::df_spike::ScenarioKind::FilteredScan),
        "SELECT count(*) FROM t WHERE \"val\" > 25"
    );
}

#[test]
fn the_recorded_datafusion_parallelism_is_resolved_not_left_as_default() {
    use crate::df_spike::bench::{BenchConfig, BenchRunner};
    use crate::df_spike::rowwise::{RowLiteral, RowOp};
    let config = |tp| BenchConfig {
        dir: std::path::PathBuf::from("/nonexistent"),
        schema: testutil::clustering_schema(),
        batch_size: TEST_BATCH_SIZE,
        projection: vec!["ck".into()],
        filter_column: "val".into(),
        filter_op: RowOp::Gt,
        filter_value: RowLiteral::parse("1"),
        iterations: 1,
        df_target_partitions: tp,
    };
    assert_eq!(
        BenchRunner::new(config(Some(3))).effective_df_partitions(),
        3
    );
    // With no pin the RESOLVED core count is recorded, not "default": a results
    // file read on another machine must still state the parallelism the number
    // was produced with, or the tp1-vs-default comparison is unreadable.
    let resolved = BenchRunner::new(config(None)).effective_df_partitions();
    assert_eq!(
        resolved,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    );
    assert!(resolved >= 1);
}

// ---------------------------------------------------------------------------
// `count(*)` under pushdown — the empty projection must not empty the scan
// ---------------------------------------------------------------------------

/// Run `sql` through the DataFusion arm and return the single scalar it produces.
fn datafusion_count(table_dir: &std::path::Path, schema: &TableSchema, pushdown: bool) -> i64 {
    let rows = datafusion_rows(table_dir, schema, "SELECT count(*) FROM t", pushdown);
    assert_eq!(rows.len(), 1, "count(*) returns exactly one row: {rows:?}");
    rows[0]
        .values()
        .next()
        .expect("the count column")
        .parse()
        .expect("the count is an integer")
}

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
