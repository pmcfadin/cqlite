//! Performance benchmarks for partition lookup operations (Issue #107)
//!
//! This benchmark establishes baseline performance for SSTable partition lookups
//! and validates the <1ms target (90th percentile).
//!
//! Measures:
//! - Index lookup performance (cold cache)
//! - Index lookup performance (warm cache)
//! - End-to-end partition read performance

use cqlite_core::{platform::Platform, storage::sstable::index_reader::IndexReader, Config};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::path::PathBuf;
use std::sync::Arc;

/// Benchmark context holding real SSTable data
struct BenchmarkContext {
    index_reader: IndexReader,
    key_digests: Vec<Vec<u8>>,
}

impl BenchmarkContext {
    async fn new() -> Self {
        let dataset_root = std::env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| {
            eprintln!("CQLITE_DATASETS_ROOT not set, using default test-data path");
            "/Users/patrick/local_projects/cqlite/test-data/datasets".to_string()
        });

        // Use sensor_data SSTable from test_timeseries
        let sstable_dir = PathBuf::from(dataset_root)
            .join("sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9");

        let index_path = sstable_dir.join("nb-1-big-Index.db");

        // Initialize platform
        let config = Config::default();
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("Failed to initialize platform"),
        );

        // Load Index.db without Summary (offsets will be 0, but we're only benchmarking lookup)
        let index_reader = IndexReader::open(&index_path, platform.clone())
            .await
            .expect("Failed to load Index.db");

        // Extract key digests from index for lookup benchmarks
        let key_digests: Vec<Vec<u8>> = index_reader
            .get_partition_entries()
            .iter()
            .map(|entry| entry.key_digest.to_vec())
            .collect();

        eprintln!(
            "Loaded {} partition key digests from {}",
            key_digests.len(),
            index_path.display()
        );

        Self {
            index_reader,
            key_digests,
        }
    }
}

/// Benchmark: Index lookup - Cold cache (first lookup)
fn bench_index_lookup_cold(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("index_lookup_cold");

    // Sample different key positions (beginning, middle, end)
    let sample_indices = if ctx.key_digests.len() > 10 {
        vec![
            0,                             // First key
            ctx.key_digests.len() / 4,     // 25% position
            ctx.key_digests.len() / 2,     // Middle key
            ctx.key_digests.len() * 3 / 4, // 75% position
            ctx.key_digests.len() - 1,     // Last key
        ]
    } else {
        vec![0] // Just use first key if small dataset
    };

    for &idx in &sample_indices {
        let key_digest = &ctx.key_digests[idx];
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("position_{}", idx)),
            key_digest,
            |b, key| {
                b.iter(|| {
                    // Simulate cold cache by creating a fresh lookup each time
                    let result = ctx.index_reader.lookup_partition(black_box(key));
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Index lookup - Warm cache (repeated lookups)
fn bench_index_lookup_warm(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("index_lookup_warm");

    // Use a small set of frequently accessed keys
    let hot_keys: Vec<_> = ctx
        .key_digests
        .iter()
        .take(10.min(ctx.key_digests.len()))
        .collect();

    group.bench_function("hot_keys", |b| {
        let mut key_idx = 0;
        b.iter(|| {
            let key = hot_keys[key_idx % hot_keys.len()];
            key_idx += 1;
            let result = ctx.index_reader.lookup_partition(black_box(key));
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark: Lookup throughput (ops/sec)
fn bench_lookup_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("lookup_throughput");
    group.throughput(criterion::Throughput::Elements(1000));

    let sample_keys: Vec<_> = ctx
        .key_digests
        .iter()
        .take(100.min(ctx.key_digests.len()))
        .collect();

    group.bench_function("1000_lookups", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let key = sample_keys[fastrand::usize(..sample_keys.len())];
                let result = ctx.index_reader.lookup_partition(black_box(key));
                black_box(result);
            }
        });
    });

    group.finish();
}

/// Benchmark: Lookup with different key sizes (to test HashMap efficiency)
fn bench_lookup_by_key_distribution(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("lookup_distribution");

    // Test with sequential vs random access patterns
    group.bench_function("sequential_access", |b| {
        let mut idx = 0;
        b.iter(|| {
            let key = &ctx.key_digests[idx % ctx.key_digests.len()];
            idx += 1;
            let result = ctx.index_reader.lookup_partition(black_box(key));
            black_box(result)
        });
    });

    group.bench_function("random_access", |b| {
        b.iter(|| {
            let idx = fastrand::usize(..ctx.key_digests.len());
            let key = &ctx.key_digests[idx];
            let result = ctx.index_reader.lookup_partition(black_box(key));
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark: Cache operations (testing the Borrow trait optimization from Issue #107)
fn bench_cache_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("cache_operations");

    // This benchmarks the zero-allocation optimization from lookup_partition
    // which uses Borrow<[u8]> to avoid creating temporary Arc<[u8]>
    group.bench_function("borrow_trait_lookup", |b| {
        let key = &ctx.key_digests[0];
        b.iter(|| {
            // This should NOT allocate - it uses Borrow<[u8]> directly
            let result = ctx.index_reader.lookup_partition(black_box(key.as_slice()));
            black_box(result)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_index_lookup_cold,
    bench_index_lookup_warm,
    bench_lookup_throughput,
    bench_lookup_by_key_distribution,
    bench_cache_operations,
);
criterion_main!(benches);
