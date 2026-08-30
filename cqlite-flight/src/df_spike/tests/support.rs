//! Fixtures and helpers shared by the spike's test files.
//!
//! Re-exports the imports every file needs, so each test module can open with a
//! single `use super::support::*;` rather than repeating the list.

#![allow(unused_imports)]

pub(super) use std::collections::BTreeMap;
pub(super) use std::sync::Arc;

pub(super) use arrow::array::{Array, Int32Array, Int64Array, StringArray};
pub(super) use arrow::record_batch::RecordBatch;
pub(super) use datafusion::logical_expr::{col, lit, Expr, TableProviderFilterPushDown};
pub(super) use datafusion::prelude::{SessionConfig, SessionContext};
pub(super) use serial_test::serial;

pub(super) use cqlite_core::schema::TableSchema;

pub(super) use crate::df_spike::bench::{
    ArmKind, BenchConfig, BenchRunner, Scenario, ScenarioKind,
};
pub(super) use crate::df_spike::provider::CqliteTableProvider;
pub(super) use crate::df_spike::pushdown;
pub(super) use crate::df_spike::rowwise::{
    count_matching_rowwise, count_rows_rowwise, RowLiteral, RowOp,
};
pub(super) use crate::df_spike::scan::{self, ScanTarget};
pub(super) use crate::filter::ScanSpec;
pub(super) use crate::testutil;

/// Debug-only read-clock pin (see the module docs).
pub(super) const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";
/// Pinned base write timestamp, in microseconds.
pub(super) const T_BASE_MICROS: i64 = 1_700_000_000_000_000;
/// Pinned read clock, in epoch seconds.
pub(super) const PINNED_NOW_SECS: i64 = 1_700_000_100;
/// Batch size small enough that the fixture spans several batches, so a
/// per-batch bug cannot hide behind a single-batch result.
pub(super) const TEST_BATCH_SIZE: usize = 4;

/// A row rendered as an ordered `column -> value` map, for order-sensitive
/// comparison between the two arms.
pub(super) type Row = BTreeMap<String, String>;

/// A two-generation fixture with cross-generation reconciliation to perform:
/// generation 1 writes `(1, a..e)` and `(2, a..c)`; generation 2 overwrites two
/// of those rows at a NEWER timestamp and row-deletes a third. So the surviving
/// result set is only obtainable by reconciling the two generations — which is
/// what makes the "the merge arm ran" assertion meaningful.
pub(super) fn two_generation_fixture() -> (tempfile::TempDir, std::path::PathBuf, TableSchema) {
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
pub(super) fn with_pinned_now<T>(body: impl FnOnce() -> T) -> T {
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
pub(super) fn rows_of(batch: &RecordBatch) -> Vec<Row> {
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
pub(super) fn cell_text(array: &dyn Array, index: usize) -> String {
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
pub(super) fn direct_rows(
    table_dir: &std::path::Path,
    schema: &TableSchema,
    spec: ScanSpec,
) -> Vec<Row> {
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
pub(super) fn datafusion_rows(
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

/// Run `SELECT count(*)` through the DataFusion arm and return the scalar.
pub(super) fn datafusion_count(
    table_dir: &std::path::Path,
    schema: &TableSchema,
    pushdown: bool,
) -> i64 {
    let rows = datafusion_rows(table_dir, schema, "SELECT count(*) FROM t", pushdown);
    assert_eq!(rows.len(), 1, "count(*) returns exactly one row: {rows:?}");
    rows[0]
        .values()
        .next()
        .expect("the count column")
        .parse()
        .expect("the count is an integer")
}
