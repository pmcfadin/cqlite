//! Predicate-eval / sort throughput micro-benches (issue #1644, K5, task 6.4).
//!
//! These measure the two consumer access patterns the K5 zero-copy change
//! targets beyond the raw decode path:
//!
//!   - `predicate/reject_most_rows` — a `WHERE ... ALLOW FILTERING` scan whose
//!     predicate rejects the large majority of decoded `Value::Text` rows.
//!     Pre-#1644 every decoded text value is copied (`String::from_utf8(x.to_vec())`)
//!     BEFORE the predicate is evaluated, so a rejected value still paid a full
//!     copy for nothing; post-#1644 the value is a zero-copy `Bytes` borrow
//!     until the predicate decision, so a rejected value's payload is a
//!     refcount bump, not an allocation (scan-window-borrow / value-zero-copy-
//!     decode spec, "a predicate-rejected value is never copied").
//!   - `sort/order_by_text` — a full `ORDER BY <text column>` scan, which
//!     forces every row through `Value`'s comparator (`PartialOrd`) on a
//!     `Bytes`-backed `Text` payload — proving the comparator borrow (stage 3)
//!     does not regress ordering throughput.
//!
//! Uses [`fixtures::ReadFixture::MANY_COLUMNS`] (`test_wide_rows.many_columns_table`,
//! 100 columns incl. `col_001..col_010: TEXT`) — the widest real fixture, already
//! the allocs-per-row target fixture for issue #1046/#1644. Optional (not present
//! in every checkout): skip-registers (no group, no measurement) when absent so
//! the perf gate reports SKIP rather than failing.
//!
//! Reproduce:
//! ```text
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo bench -p cqlite-core --features cli-helpers \
//!   --bench predicate_sort
//! ```

use criterion::{criterion_group, criterion_main, Criterion};

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "profiling/mod.rs"]
mod profiling;

#[cfg(feature = "cli-helpers")]
fn bench_predicate_reject_most_rows(c: &mut Criterion) {
    use criterion::{black_box, Throughput};

    let fx = fixtures::ReadFixture::MANY_COLUMNS;
    if !fixtures::fixture_present(&fx) {
        eprintln!(
            "predicate_sort/reject_most_rows: fixture {} not present — skipping (skip-register)",
            fx.qualified()
        );
        return;
    }

    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // `col_001` is a per-row-unique TEXT column (see wide-rows.cql fixture
    // generator) — matching one exact literal rejects all but a handful of
    // rows, forcing the predicate to evaluate (and reject) the decoded Text
    // value for the overwhelming majority of the scan.
    let baseline = rt
        .block_on(loaded.db.execute(&format!(
            "SELECT id, col_001 FROM {} LIMIT 1",
            fx.qualified()
        )))
        .expect("predicate_sort setup scan");
    let row_count = rt
        .block_on(
            loaded
                .db
                .execute(&format!("SELECT id FROM {}", fx.qualified())),
        )
        .expect("predicate_sort row-count scan")
        .rows
        .len();
    assert!(
        row_count > 1,
        "predicate_sort/reject_most_rows: {} has only {row_count} row(s) — need >1 for a \
         low-selectivity predicate to be meaningful",
        fx.qualified()
    );
    let needle = match baseline.rows.first().and_then(|r| r.values.get("col_001")) {
        Some(v @ cqlite_core::Value::Text(_)) => v
            .as_str()
            .expect("Value::Text always has a str view")
            .to_string(),
        other => panic!(
            "predicate_sort/reject_most_rows: {}.col_001 did not decode as Value::Text \
             (got {other:?})",
            fx.qualified()
        ),
    };
    let sql = format!(
        "SELECT id, col_001 FROM {} WHERE col_001 = '{}' ALLOW FILTERING",
        fx.qualified(),
        needle
    );

    // Guard: never silently measure a query that matches EVERY row (that would
    // not exercise the "reject most rows" predicate-rejection path at all).
    let setup = rt
        .block_on(loaded.db.execute(&sql))
        .expect("predicate_sort setup filtered scan");
    assert!(
        !setup.rows.is_empty() && setup.rows.len() < row_count,
        "predicate_sort/reject_most_rows: predicate matched {} of {row_count} rows on {} — \
         expected a low-selectivity match (>=1, <all) to exercise predicate rejection",
        setup.rows.len(),
        fx.qualified()
    );

    let mut group = c.benchmark_group("predicate_sort");
    group.throughput(Throughput::Elements(row_count as u64));
    group.bench_function("reject_most_rows", |bch| {
        bch.iter(|| {
            let res = rt
                .block_on(loaded.db.execute(black_box(&sql)))
                .expect("predicate_sort filtered scan");
            black_box(res.rows.len())
        });
    });
    group.finish();
}

#[cfg(feature = "cli-helpers")]
fn bench_sort_by_text_column(c: &mut Criterion) {
    use criterion::{black_box, Throughput};

    let fx = fixtures::ReadFixture::MANY_COLUMNS;
    if !fixtures::fixture_present(&fx) {
        eprintln!(
            "predicate_sort/order_by_text: fixture {} not present — skipping (skip-register)",
            fx.qualified()
        );
        return;
    }

    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let sql = format!(
        "SELECT id, col_001 FROM {} ORDER BY col_001",
        fx.qualified()
    );

    let setup = rt
        .block_on(loaded.db.execute(&sql))
        .expect("predicate_sort setup sorted scan");
    assert!(
        !setup.rows.is_empty(),
        "predicate_sort/order_by_text: sorted scan on {} returned zero rows — fixtures not \
         fetched?",
        fx.qualified()
    );

    let mut group = c.benchmark_group("predicate_sort");
    group.throughput(Throughput::Elements(setup.rows.len() as u64));
    group.bench_function("order_by_text", |bch| {
        bch.iter(|| {
            let res = rt
                .block_on(loaded.db.execute(black_box(&sql)))
                .expect("predicate_sort sorted scan");
            black_box(res.rows.len())
        });
    });
    group.finish();
}

#[cfg(feature = "cli-helpers")]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_predicate_reject_most_rows, bench_sort_by_text_column
);

#[cfg(not(feature = "cli-helpers"))]
fn bench_noop(_c: &mut Criterion) {
    // Nothing to bench without cli-helpers. The bench binary still compiles and
    // runs successfully; it just reports no measurements.
}

#[cfg(not(feature = "cli-helpers"))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_noop
);

criterion_main!(benches);
