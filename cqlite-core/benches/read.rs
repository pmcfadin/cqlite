//! Read micro-benchmarks for cqlite-core (Issue #538, Epic #541 Phase 1).
//!
//! Four benches measure the public query API over real Cassandra 5.0 SSTables:
//!
//! - `read/point_lookup`    — minimal single-row read (`SELECT * … LIMIT 1`)
//! - `read/clustering_slice` — bounded read of clustering-ordered rows (`LIMIT N`)
//! - `read/full_scan`       — `SELECT *` streaming all rows of simple_table
//! - `read/type_heavy`      — `SELECT *` over collection_table to isolate decode cost
//!
//! # Why LIMIT-based proxies instead of keyed `WHERE` lookups (issue #548)
//!
//! The query engine's `value_to_row_key` only handles Integer/Text/Float/Boolean;
//! `Value::Uuid` falls through, so `WHERE id = '<uuid-literal>'` returns 0 rows for
//! a UUID partition key (and the scan-filter path compares `Value::Text` against the
//! stored `Value::Uuid`, which never matches). Almost every fixture table is
//! UUID-keyed, and the one non-UUID single-key table ships empty, so a true keyed
//! point lookup is not currently expressible through the public API. This is tracked
//! in #548.
//!
//! Until #548 lands, `point_lookup` and `clustering_slice` use deterministic
//! `LIMIT`-bounded reads as honest proxies: `point_lookup` measures the
//! per-query + first-row decode latency (`LIMIT 1`), and `clustering_slice` measures
//! a bounded read over the clustering-ordered partition front (`LIMIT N`). Both
//! exercise the real open → seek → decode path and are distinct from `full_scan`.
//! When #548 is fixed, switch these two to keyed `WHERE` lookups for a sharper signal.
//!
//! All benches are gated on `cli-helpers` (required for `open_read_db`). Under
//! default features the bench compiles to an empty criterion group so the binary
//! is still valid.
//!
//! Determinism: these benches issue fixed SQL with fixed `LIMIT` constants over a
//! fixed fixture, so the result set is byte-for-byte identical across runs and
//! machines without needing randomness. (A bench that does randomized key
//! selection should draw from `fixtures::seeded_rng()` for the same guarantee.)

#[cfg(feature = "cli-helpers")]
use criterion::{black_box, Throughput};
use criterion::{criterion_group, criterion_main, Criterion};

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "profiling/mod.rs"]
mod profiling;

// ---------------------------------------------------------------------------
// cli-helpers benches
// ---------------------------------------------------------------------------

/// Bench: cost of returning one row from simple_table.
///
/// Uses `LIMIT 1` rather than `WHERE id = '<uuid>'` because the query engine
/// cannot currently resolve UUID partition-key equality to a RowKey (#548; see
/// module doc). Reports `Throughput::Elements(1)`.
#[cfg(feature = "cli-helpers")]
fn bench_point_lookup(c: &mut Criterion) {
    let fx = fixtures::ReadFixture::SIMPLE;
    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let sql = format!("SELECT * FROM {} LIMIT 1", fx.qualified());

    // Assert at setup: we must get ≥1 row.
    let setup = rt
        .block_on(loaded.db.execute(&sql))
        .expect("point_lookup setup query");
    assert!(
        !setup.rows.is_empty(),
        "point_lookup: LIMIT 1 on {} returned zero rows — fixtures not fetched?",
        fx.qualified()
    );

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(1));
    group.bench_function("point_lookup", |bch| {
        bch.iter(|| {
            let res = rt
                .block_on(loaded.db.execute(black_box(&sql)))
                .expect("point lookup");
            black_box(res.rows.len())
        });
    });
    group.finish();
}

/// Bench: bounded read of clustering-ordered rows from sensor_data.
///
/// sensor_data has `PRIMARY KEY (sensor_id, timestamp)` with `CLUSTERING ORDER BY
/// (timestamp DESC)`. Without a `WHERE sensor_id = …` predicate, `SELECT * … LIMIT N`
/// reads the first `N` rows in SSTable scan order — exercising multi-partition open
/// plus clustering-row decode, a real read path distinct from `full_scan`. A true
/// keyed single-partition slice (`WHERE sensor_id = '<uuid>' AND timestamp …`) is not
/// currently expressible because UUID partition-key equality doesn't resolve to a
/// RowKey (#548; see module doc). `N` is a fixed constant so the read is
/// deterministic, and `Throughput::Elements` uses the row count returned at setup.
#[cfg(feature = "cli-helpers")]
fn bench_clustering_slice(c: &mut Criterion) {
    /// Fixed clustering-slice width — deterministic across runs/machines.
    const SLICE_ROWS: usize = 64;

    let fx = fixtures::ReadFixture::CLUSTERING;
    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let sql = format!("SELECT * FROM {} LIMIT {}", fx.qualified(), SLICE_ROWS);
    let setup = rt
        .block_on(loaded.db.execute(&sql))
        .expect("clustering_slice setup query");
    let rows_measured = setup.rows.len() as u64;
    assert!(
        rows_measured > 0,
        "clustering_slice: LIMIT {} on {} returned zero rows — fixtures not fetched?",
        SLICE_ROWS,
        fx.qualified()
    );

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(rows_measured));
    group.bench_function("clustering_slice", |bch| {
        bch.iter(|| {
            let res = rt
                .block_on(loaded.db.execute(black_box(&sql)))
                .expect("clustering slice");
            black_box(res.rows.len())
        });
    });
    group.finish();
}

/// Bench: full-table scan of simple_table.
///
/// `SELECT * FROM test_basic.simple_table` streams all ~999 rows.
/// `Throughput::Elements(row_count)` is set from the count measured at setup
/// so the report shows rows/sec.
#[cfg(feature = "cli-helpers")]
fn bench_full_scan(c: &mut Criterion) {
    let fx = fixtures::ReadFixture::SIMPLE;
    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let sql = format!("SELECT * FROM {}", fx.qualified());

    // Measure row count once at setup.
    let setup = rt
        .block_on(loaded.db.execute(&sql))
        .expect("full scan setup");
    let row_count = setup.rows.len() as u64;
    assert!(
        row_count > 0,
        "full scan of {} returned zero rows — fixtures not fetched?",
        fx.qualified()
    );

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(row_count));
    group.bench_function("full_scan", |bch| {
        bch.iter(|| {
            let res = rt
                .block_on(loaded.db.execute(black_box(&sql)))
                .expect("full scan");
            black_box(res.rows.len())
        });
    });
    group.finish();
}

/// Bench: full scan of collection_table to isolate collection-decode cost.
///
/// `SELECT * FROM test_collections.collection_table` reads all rows which
/// include SET<TEXT>, LIST<INT>, MAP<TEXT,TEXT>, and related fields, exercising
/// the full deserialization path for collection types.
/// `Throughput::Elements(row_count)` so the report shows rows/sec.
#[cfg(feature = "cli-helpers")]
fn bench_type_heavy(c: &mut Criterion) {
    let fx = fixtures::ReadFixture::TYPE_HEAVY;
    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let sql = format!("SELECT * FROM {}", fx.qualified());

    // Measure row count once at setup for Throughput.
    let setup = rt
        .block_on(loaded.db.execute(&sql))
        .expect("type_heavy scan setup");
    let row_count = setup.rows.len() as u64;
    assert!(
        row_count > 0,
        "type_heavy scan of {} returned zero rows — fixtures not fetched?",
        fx.qualified()
    );

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(row_count));
    group.bench_function("type_heavy", |bch| {
        bch.iter(|| {
            let res = rt
                .block_on(loaded.db.execute(black_box(&sql)))
                .expect("type_heavy scan");
            black_box(res.rows.len())
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// criterion_group! / criterion_main! — feature-gated so the bench compiles
// under default features (no cli-helpers) with an empty but valid group.
// ---------------------------------------------------------------------------

#[cfg(feature = "cli-helpers")]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_point_lookup,
              bench_clustering_slice,
              bench_full_scan,
              bench_type_heavy
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
