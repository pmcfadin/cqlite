//! Read micro-benchmarks for cqlite-core (Issue #538, Epic #541 Phase 1).
//!
//! The benches measure the public query API over real Cassandra 5.0 SSTables:
//!
//! - `read/get_partition_big` — real partition-targeted point read (BIG/`nb`)
//! - `read/get_partition_bti` — real partition-targeted point read (BTI/`da`)
//! - `read/clustering_slice`  — bounded read of clustering-ordered rows (`LIMIT N`)
//! - `read/full_scan`         — `SELECT *` streaming all rows of simple_table
//! - `read/type_heavy`        — `SELECT *` over collection_table to isolate decode cost
//!
//! # Real point reads: `get_partition` (issue #1562)
//!
//! The old `read/point_lookup` bench ran `SELECT * … LIMIT 1` — a full scan
//! truncated to one row. That is a scan, not a point read, so it could not detect
//! a regression on the partition-targeted point path (bloom/BTI prune →
//! single-candidate seek → chunk decode). Since #956 a UUID partition key is
//! addressable via an unquoted-UUID literal (`WHERE id = <8-4-4-4-12>`), which
//! engages the #949 fast path, so a true keyed point read is now expressible
//! through the public API.
//!
//! `bench_get_partition` drives that real path: it learns a present `id` from a
//! one-shot scan, formats the unquoted-UUID literal, and benches
//! `SELECT id, name FROM <ks.tbl> WHERE id = <lit>`. Two guards at setup make the
//! measurement honest — it panics rather than silently benching the wrong thing:
//!
//!  1. `rows.len() >= 1` — never measure a 0-row query.
//!  2. `access_path.is_targeted()` — the query MUST report a partition-targeted
//!     `AccessPath` (`PartitionLookup`), proving #949/#956 are wired. If it comes
//!     back a full-scan variant, the setup panics with an actionable message.
//!
//! Query shape note: the projection (`SELECT id, name`, not `SELECT *`) is
//! deliberate. `QueryEngine::execute` routes a `SELECT` through the legacy
//! full-scan executor when `cql.contains("WHERE id =") && whitespace_tokens <= 8`
//! (a "simple id lookup"); `SELECT * FROM <ks.tbl> WHERE id = <lit>` is exactly 8
//! tokens and would fall into that legacy path (reporting `access_path = None`).
//! Projecting two columns is a faithful fully-constrained point read that routes
//! through the modern `SelectExecutor` and reports the real access path.
//!
//! `get_partition_big` uses the always-present canonical `test_basic.simple_table`
//! (`nb`); `get_partition_bti` uses the optional `test_da.simple_table` (`da`) and
//! **skip-registers** (creates no group, so the gate reports SKIP) when that
//! corpus is absent.
//!
//! All benches are gated on `cli-helpers` (required for `open_read_db`). Under
//! default features the bench compiles to an empty criterion group so the binary
//! is still valid.
//!
//! Determinism: the point-read benches pick the *first* row's key from a fixed
//! fixture scan (stable ordering) and issue fixed SQL; `clustering_slice` uses a
//! fixed `LIMIT`. The result set is identical across runs and machines without
//! randomness. (A bench that does randomized key selection should draw from
//! `fixtures::seeded_rng()` for the same guarantee.)

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

/// Format a 16-byte UUID as the canonical 8-4-4-4-12 unquoted-UUID literal the
/// SELECT parser accepts (issue #956). Mirrors the helper in
/// `tests/issue_956_uuid_literal_partition_lookup_parity.rs`.
#[cfg(feature = "cli-helpers")]
fn uuid_to_literal(bytes: &[u8; 16]) -> String {
    let h = |range: std::ops::Range<usize>| -> String {
        bytes[range].iter().map(|b| format!("{b:02x}")).collect()
    };
    format!(
        "{}-{}-{}-{}-{}",
        h(0..4),
        h(4..6),
        h(6..8),
        h(8..10),
        h(10..16)
    )
}

/// Shared driver for the real partition-targeted point-read benches.
///
/// Learns a present `id` from a one-shot scan, formats the unquoted-UUID literal,
/// and benches `SELECT id, name FROM <qualified> WHERE id = <lit>` — the real
/// #949/#956 partition-targeted path (see module doc). Two setup guards keep the
/// measurement honest (panic rather than silently mis-measure):
///   1. the point read returns ≥1 row, and
///   2. it reports a *targeted* `AccessPath` (not a full-scan fallback).
///
/// If the fixture's corpus is absent, returns early WITHOUT creating a group, so
/// the perf gate reports SKIP for that bench rather than failing.
#[cfg(feature = "cli-helpers")]
fn bench_get_partition(c: &mut Criterion, fx: fixtures::ReadFixture, bench_name: &str) {
    use cqlite_core::Value;

    if !fixtures::fixture_present(&fx) {
        // Optional fixture (e.g. the BTI test_da corpus) not present in this
        // checkout — skip-register: no group, no measurement, gate reports SKIP.
        eprintln!(
            "read/{bench_name}: fixture {} not present — skipping (skip-register)",
            fx.qualified()
        );
        return;
    }

    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // Learn a present partition key from a one-shot scan.
    let scan = rt
        .block_on(
            loaded
                .db
                .execute(&format!("SELECT id FROM {}", fx.qualified())),
        )
        .expect("get_partition setup scan");
    let first = scan.rows.first().unwrap_or_else(|| {
        panic!(
            "get_partition: scan of {} returned zero rows — fixtures not fetched?",
            fx.qualified()
        )
    });
    let id = match first.values.get("id") {
        Some(Value::Uuid(b)) => *b,
        other => panic!(
            "get_partition: first row `id` did not decode as Value::Uuid (got {other:?}) for {}",
            fx.qualified()
        ),
    };
    let literal = uuid_to_literal(&id);

    // Projected (>8 tokens) so it routes through the modern SelectExecutor and
    // engages the #949 fast path (see module doc on the legacy simple-id-lookup
    // routing quirk).
    let sql = format!(
        "SELECT id, name FROM {} WHERE id = {}",
        fx.qualified(),
        literal
    );

    // Guard 1: never silently measure a 0-row query.
    let setup = rt
        .block_on(loaded.db.execute(&sql))
        .expect("get_partition setup point read");
    assert!(
        !setup.rows.is_empty(),
        "get_partition: point read on {} returned zero rows for a known-present key — \
         #949/#956 regressed?",
        fx.qualified()
    );

    // Guard 2: the point read MUST take a partition-targeted path. If it fell back
    // to a full scan (or the legacy no-access-path route), fail loudly — otherwise
    // the bench would be a scan proxy again.
    let targeted = setup
        .metadata
        .access_path
        .as_ref()
        .map(|p| p.is_targeted())
        .unwrap_or(false);
    assert!(
        targeted,
        "get_partition: point read fell back to full scan (access_path = {:?}) on {} — \
         #956/#949 regressed, or the query routed to the legacy executor",
        setup.metadata.access_path,
        fx.qualified()
    );

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(1));
    group.bench_function(bench_name, |bch| {
        bch.iter(|| {
            let res = rt
                .block_on(loaded.db.execute(black_box(&sql)))
                .expect("get_partition point read");
            black_box(res.rows.len())
        });
    });
    group.finish();
}

/// Bench: real partition-targeted point read over the BIG (`nb`) fixture.
///
/// `test_basic.simple_table` is the always-present canonical fixture. Produces
/// the Criterion id `read/get_partition_big`.
#[cfg(feature = "cli-helpers")]
fn bench_get_partition_big(c: &mut Criterion) {
    bench_get_partition(c, fixtures::ReadFixture::SIMPLE, "get_partition_big");
}

/// Bench: real partition-targeted point read over the BTI (`da`) fixture.
///
/// `test_da.simple_table` is optional (absent in some checkouts); when missing
/// the bench skip-registers (no measurement). Produces the Criterion id
/// `read/get_partition_bti`.
#[cfg(feature = "cli-helpers")]
fn bench_get_partition_bti(c: &mut Criterion) {
    bench_get_partition(c, fixtures::ReadFixture::SIMPLE_BTI, "get_partition_bti");
}

/// Bench: a REPEATED identical point read served from the shared
/// decompressed-chunk cache (issue #1567, Epic B/B1). Produces the Criterion id
/// `read/point_lookup_repeated`.
///
/// Criterion warms up before measuring, so every timed iteration reads the same
/// key through the same long-lived `Database` — the target chunk is decompressed
/// exactly ONCE (the warm-up cold read) and served from the cache thereafter
/// (`Arc::clone`, no re-read, no re-decompress). This measures the steady-state
/// *cached* point-read latency, the metric the cache is built to reduce; the
/// integration suite proves the zero-work property directly (decompress delta 0).
///
/// Uses the BTI (`da`) fixture because its point-read path
/// (`bti_decompress_and_parse_target`) is a wired cache site; skip-registers (no
/// group, gate reports SKIP) when the optional `test_da` corpus is absent.
#[cfg(feature = "cli-helpers")]
fn bench_point_lookup_repeated(c: &mut Criterion) {
    bench_get_partition(
        c,
        fixtures::ReadFixture::SIMPLE_BTI,
        "point_lookup_repeated",
    );
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

/// Bench: partition-dense STREAMING full scan of simple_table (issue #1589).
///
/// `test_basic.simple_table` is UUID-keyed with no clustering column, so every row
/// is its OWN partition (~999 one-row partitions) — the Θ(P·W) pathological shape
/// the window-drain cursor targets. Unlike `full_scan` (which uses `db.execute`),
/// this drives `execute_streaming`, the sliding-`WindowCursor` path where confirmed
/// partitions used to be removed with a per-partition `window.drain(0..consumed)`
/// (memmoving the whole residual tail each time). It is the sensitive read bench for
/// a regression in the window-consume cost. `Throughput::Elements(row_count)` so the
/// report shows rows/sec.
#[cfg(feature = "cli-helpers")]
fn bench_scan_partition_dense_stream(c: &mut Criterion) {
    use cqlite_core::query::result::StreamingConfig;

    let fx = fixtures::ReadFixture::SIMPLE;
    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let sql = format!("SELECT * FROM {}", fx.qualified());

    // Drain one streaming scan to completion, returning the row count.
    let drain = |sql: &str| -> usize {
        rt.block_on(async {
            let mut iter = loaded
                .db
                .execute_streaming(sql, StreamingConfig::default())
                .await
                .expect("execute_streaming");
            let mut n = 0usize;
            while let Some(row) = iter.next_async().await {
                black_box(row.expect("streamed row"));
                n += 1;
            }
            n
        })
    };

    let row_count = drain(&sql) as u64;
    assert!(
        row_count > 0,
        "partition-dense stream scan of {} returned zero rows — fixtures not fetched?",
        fx.qualified()
    );

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(row_count));
    group.bench_function("scan_partition_dense_stream", |bch| {
        bch.iter(|| black_box(drain(black_box(&sql))));
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
    targets = bench_get_partition_big,
              bench_get_partition_bti,
              bench_point_lookup_repeated,
              bench_clustering_slice,
              bench_full_scan,
              bench_type_heavy,
              bench_scan_partition_dense_stream
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
