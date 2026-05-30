//! Smoke bench for the deterministic fixture loaders (Issue #537).
//!
//! This is the acceptance demonstration for Perf 1.2: it proves that the
//! `fixtures` module loads a known fixture deterministically with no external
//! services, and that the same fixture yields the same row/partition set on
//! every run. It is intentionally tiny — the real read/write suites land in
//! #538 and #539.
//!
//! Coverage scales with the features the bench is built with:
//!
//! - default — dataset-root resolution + seeded-RNG determinism.
//! - `cli-helpers` — open a queryable Database over a fixture, assert a stable
//!   row count across repeated scans.
//! - `write-support` — build a WriteEngine in a temp dir, assert a stable
//!   memtable row count for a fixed seeded input.
//!
//! Run all of it:
//!   cargo bench -p cqlite-core --features cli-helpers,write-support \
//!     --bench fixtures_smoke

use criterion::{black_box, criterion_group, criterion_main, Criterion};

#[path = "fixtures/mod.rs"]
mod fixtures;

/// Always-on: the loader resolves a dataset root and the seeded RNG is
/// reproducible. These are the determinism invariants every other bench relies
/// on.
fn bench_fixture_basics(c: &mut Criterion) {
    use rand::Rng;

    // Same seed must yield the same sequence — twice.
    let mut a = fixtures::seeded_rng();
    let mut b = fixtures::seeded_rng();
    let seq_a: Vec<u64> = (0..64).map(|_| a.gen()).collect();
    let seq_b: Vec<u64> = (0..64).map(|_| b.gen()).collect();
    assert_eq!(
        seq_a, seq_b,
        "seeded_rng must be deterministic across instances"
    );

    let root = fixtures::datasets_root();
    assert!(
        root.join("sstables").is_dir(),
        "fixture sstables/ not found under {} — run test-data/scripts/fetch-datasets.sh",
        root.display()
    );

    c.bench_function("fixture/seeded_rng_64", |bch| {
        bch.iter(|| {
            let mut rng = fixtures::seeded_rng();
            let mut acc = 0u64;
            for _ in 0..64 {
                acc ^= rng.gen::<u64>();
            }
            black_box(acc)
        });
    });
}

/// `cli-helpers`: open a queryable Database over `simple_table` and confirm the
/// scanned row set is stable run-to-run.
#[cfg(feature = "cli-helpers")]
fn bench_read_fixture(c: &mut Criterion) {
    let fx = fixtures::ReadFixture::SIMPLE;
    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let sql = format!("SELECT * FROM {}", fx.qualified());

    // Determinism: two full scans return the same row count.
    let n1 = rt
        .block_on(loaded.db.execute(&sql))
        .expect("scan fixture")
        .rows
        .len();
    let n2 = rt
        .block_on(loaded.db.execute(&sql))
        .expect("scan fixture")
        .rows
        .len();
    assert_eq!(
        n1, n2,
        "same fixture must yield the same row set every scan"
    );
    assert!(n1 > 0, "fixture {} scanned zero rows", fx.qualified());

    c.bench_function("fixture/read_scan_simple_table", |bch| {
        bch.iter(|| {
            let res = rt
                .block_on(loaded.db.execute(black_box(&sql)))
                .expect("scan");
            black_box(res.rows.len())
        });
    });
}

/// `write-support`: build a WriteEngine in a temp dir and confirm a fixed seeded
/// input produces a stable memtable row count.
#[cfg(feature = "write-support")]
fn bench_write_fixture(c: &mut Criterion) {
    use cqlite_core::storage::write_engine::WriteEngine;

    const ROWS: usize = 64;

    // Insert ROWS seeded mutations and report the resulting memtable row count.
    fn fill(engine: &mut WriteEngine) -> usize {
        use rand::Rng;
        let mut rng = fixtures::seeded_rng();
        for _ in 0..ROWS {
            let id = uuid::Uuid::from_u128(rng.gen());
            let stmt = format!(
                "INSERT INTO test_basic.simple_table (id, name, age, active) \
                 VALUES ({id}, 'row', {}, true)",
                rng.gen_range(0..100)
            );
            engine.execute(&stmt).expect("write seeded row");
        }
        engine.memtable_row_count()
    }

    let tmp1 = tempfile::TempDir::new().expect("temp dir");
    let mut e1 = fixtures::open_write_engine(tmp1.path(), usize::MAX);
    let c1 = fill(&mut e1);

    let tmp2 = tempfile::TempDir::new().expect("temp dir");
    let mut e2 = fixtures::open_write_engine(tmp2.path(), usize::MAX);
    let c2 = fill(&mut e2);

    assert_eq!(
        c1, c2,
        "same seeded input must yield the same memtable row count"
    );
    assert!(c1 > 0, "no rows landed in the memtable");

    c.bench_function("fixture/write_fill_64", |bch| {
        bch.iter_batched(
            || {
                let tmp = tempfile::TempDir::new().expect("temp dir");
                let engine = fixtures::open_write_engine(tmp.path(), usize::MAX);
                (tmp, engine)
            },
            |(_tmp, mut engine)| {
                let n = fill(&mut engine);
                black_box(n)
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

#[cfg(all(feature = "cli-helpers", feature = "write-support"))]
criterion_group!(
    benches,
    bench_fixture_basics,
    bench_read_fixture,
    bench_write_fixture
);
#[cfg(all(feature = "cli-helpers", not(feature = "write-support")))]
criterion_group!(benches, bench_fixture_basics, bench_read_fixture);
#[cfg(all(not(feature = "cli-helpers"), feature = "write-support"))]
criterion_group!(benches, bench_fixture_basics, bench_write_fixture);
#[cfg(all(not(feature = "cli-helpers"), not(feature = "write-support")))]
criterion_group!(benches, bench_fixture_basics);
criterion_main!(benches);
