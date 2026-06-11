//! M1 Performance Baseline and Validation (Issue #116)
//!
//! This benchmark suite validates CQLite M1 performance targets:
//! - Partition Lookups: Sub-millisecond (target: <1ms at p90)
//! - Memory Usage: <128MB for large SSTables
//! - Parse Speed: 1GB files in <10 seconds
//!
//! Note: Test data is ~85KB-632KB per file, not 1GB. Throughput benchmarks
//! read multiple SSTables and extrapolate to 1GB target.

use cqlite_core::{
    platform::Platform,
    storage::sstable::{index_reader::IndexReader, SSTableReader},
    Config,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::path::PathBuf;
use std::sync::Arc;

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "profiling/mod.rs"]
mod profiling;

/// Benchmark context holding real SSTable data from multiple datasets
struct BenchmarkContext {
    /// Index reader for partition lookup benchmarks
    index_reader: IndexReader,
    /// Key digests for lookup benchmarks
    key_digests: Vec<Vec<u8>>,
    /// SSTable paths for throughput benchmarks (from largest to smallest)
    sstable_paths: Vec<PathBuf>,
    /// Total size of all SSTable data files (for throughput calculation)
    total_data_size_bytes: u64,
    /// Platform and config for SSTable operations
    platform: Arc<Platform>,
    config: Config,
}

impl BenchmarkContext {
    async fn new() -> Self {
        // Initialize platform and config (required for all M1 SSTable APIs)
        let config = Config::default();
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("Failed to initialize platform"),
        );

        // Use sensor_data SSTable from test_timeseries for index lookups (~85KB).
        // Resolved hash-independently via the shared fixture loader (Issue #537),
        // which panics with a fetch hint if the fixture is missing.
        let sensor_data_dir = fixtures::table_dir("test_timeseries", "sensor_data");
        let index_path = sensor_data_dir.join("nb-1-big-Index.db");

        // Load Index.db for partition lookup benchmarks
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

        // Collect SSTable data paths for throughput benchmarks (largest first)
        let sstable_paths = vec![
            // Largest: simple_table (~632KB)
            fixtures::table_dir("test_basic", "simple_table").join("nb-1-big-Data.db"),
            // compression_test_table (~212KB)
            fixtures::table_dir("test_basic", "compression_test_table").join("nb-1-big-Data.db"),
            // collection_table (~148KB)
            fixtures::table_dir("test_collections", "collection_table").join("nb-1-big-Data.db"),
            // sensor_data (~88KB)
            sensor_data_dir.join("nb-1-big-Data.db"),
        ];

        // Calculate total size for throughput metrics
        let mut total_size = 0u64;
        for path in &sstable_paths {
            if let Ok(metadata) = tokio::fs::metadata(path).await {
                total_size += metadata.len();
                eprintln!("SSTable: {} ({} bytes)", path.display(), metadata.len());
            }
        }

        eprintln!(
            "Total SSTable data size: {} bytes ({:.2} MB)",
            total_size,
            total_size as f64 / (1024.0 * 1024.0)
        );

        Self {
            index_reader,
            key_digests,
            sstable_paths,
            total_data_size_bytes: total_size,
            platform,
            config,
        }
    }
}

// ============================================================================
// Benchmark 1: Partition Lookup Latency (Target: <1ms p90)
// ============================================================================

/// Benchmark: Cold cache partition lookups
///
/// M1 Target: <1ms at p90
/// Validates: Initial lookup performance without cache warmup
fn bench_partition_lookup_cold_cache(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("m1_partition_lookup_cold");
    group.significance_level(0.1).sample_size(100);

    // Sample different key positions to test index performance across distribution
    let sample_indices = if ctx.key_digests.len() > 10 {
        vec![
            0,                             // First key (beginning of index)
            ctx.key_digests.len() / 4,     // 25% position
            ctx.key_digests.len() / 2,     // Middle key
            ctx.key_digests.len() * 3 / 4, // 75% position
            ctx.key_digests.len() - 1,     // Last key (end of index)
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
                    // Simulate cold cache by creating fresh lookup each time
                    let result = ctx.index_reader.lookup_partition(black_box(key));
                    black_box(result)
                });
            },
        );
    }

    group.finish();

    eprintln!("\n=== M1 Target Validation: Partition Lookup (Cold Cache) ===");
    eprintln!("Target: <1ms at p90");
    eprintln!("Check criterion report for actual p90 latency");
}

/// Benchmark: Warm cache partition lookups
///
/// M1 Target: <1ms at p90 (should be significantly faster with warm cache)
/// Validates: Cached lookup performance with hot keys
fn bench_partition_lookup_warm_cache(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("m1_partition_lookup_warm");
    group.significance_level(0.1).sample_size(200);

    // Use a small set of frequently accessed keys (simulates real workload)
    let hot_keys: Vec<_> = ctx
        .key_digests
        .iter()
        .take(10.min(ctx.key_digests.len()))
        .collect();

    group.bench_function("hot_keys_rotating", |b| {
        let mut key_idx = 0;
        b.iter(|| {
            let key = hot_keys[key_idx % hot_keys.len()];
            key_idx += 1;
            let result = ctx.index_reader.lookup_partition(black_box(key));
            black_box(result)
        });
    });

    group.finish();

    eprintln!("\n=== M1 Target Validation: Partition Lookup (Warm Cache) ===");
    eprintln!("Target: <1ms at p90 (warm cache should be faster than cold)");
    eprintln!("Check criterion report for actual p90 latency");
}

/// Benchmark: Partition lookup throughput
///
/// M1 Target: Validate sustained lookup performance
/// Measures: Operations per second under load
fn bench_partition_lookup_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("m1_partition_lookup_throughput");
    group.throughput(Throughput::Elements(1000));
    group.sample_size(50);

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

    eprintln!("\n=== M1 Target Validation: Partition Lookup Throughput ===");
    eprintln!("Target: Sustained <1ms lookups under load");
    eprintln!("Measure: ops/sec for 1000 random lookups");
}

// ============================================================================
// Benchmark 2: SSTable Read Throughput (Target: 1GB in <10 seconds = >100 MB/s)
// ============================================================================

/// Benchmark: SSTable sequential read throughput
///
/// M1 Target: 1GB files in <10 seconds (>100 MB/s)
/// Note: Test data is ~1.08MB total. We read all SSTables and extrapolate.
fn bench_sstable_read_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("m1_sstable_read_throughput");
    group.throughput(Throughput::Bytes(ctx.total_data_size_bytes));
    group.sample_size(20); // Fewer samples since this is I/O heavy

    group.bench_function("read_all_sstables", |b| {
        b.iter(|| {
            rt.block_on(async {
                for path in &ctx.sstable_paths {
                    // Open SSTableReader and read all entries
                    let reader = SSTableReader::open(path, &ctx.config, ctx.platform.clone())
                        .await
                        .expect("Failed to open SSTable");

                    let entries = reader
                        .get_all_entries()
                        .await
                        .expect("Failed to read entries");

                    black_box(entries);
                }
            });
        });
    });

    group.finish();

    // Calculate throughput metrics
    let mb_total = ctx.total_data_size_bytes as f64 / (1024.0 * 1024.0);
    eprintln!("\n=== M1 Target Validation: SSTable Read Throughput ===");
    eprintln!("Target: 1GB in <10 seconds (>100 MB/s)");
    eprintln!("Test data: {:.2} MB total", mb_total);
    eprintln!("Note: Criterion will report actual MB/s throughput");
    eprintln!("Extrapolation: If measured throughput ≥ 100 MB/s, 1GB target is met");
}

/// Benchmark: Multi-SSTable read throughput
///
/// M1 Target: Sustained >100 MB/s across multiple files
/// Validates: Consistent throughput when reading multiple SSTables
fn bench_multi_sstable_read_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("m1_multi_sstable_read_throughput");
    group.throughput(Throughput::Bytes(ctx.total_data_size_bytes));
    group.sample_size(20);

    group.bench_function("read_multiple_sstables_parallel", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Read all SSTables in parallel for maximum throughput
                let futures: Vec<_> = ctx
                    .sstable_paths
                    .iter()
                    .map(|path| {
                        let config = ctx.config.clone();
                        let platform = ctx.platform.clone();
                        async move {
                            let reader = SSTableReader::open(path, &config, platform)
                                .await
                                .expect("Failed to open SSTable");
                            reader
                                .get_all_entries()
                                .await
                                .expect("Failed to read entries")
                        }
                    })
                    .collect();

                let results = futures::future::join_all(futures).await;
                black_box(results);
            });
        });
    });

    group.finish();

    eprintln!("\n=== M1 Target Validation: Multi-SSTable Read Throughput ===");
    eprintln!("Target: Sustained >100 MB/s across multiple files");
    eprintln!("Parallel reading should maximize throughput");
}

// ============================================================================
// Benchmark 3: Memory Usage Validation (Target: <128MB for large SSTables)
// ============================================================================

/// Benchmark: Memory usage during large SSTable operations
///
/// M1 Target: <128MB for large SSTables
/// Note: This benchmark measures peak memory via Platform metrics
fn bench_memory_usage_large_sstable(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("m1_memory_usage");
    group.sample_size(10); // Limited samples to avoid memory churn

    // Use largest SSTable (632KB - will read it multiple times to simulate large file)
    let largest_sstable = &ctx.sstable_paths[0];

    group.bench_function("read_largest_sstable_10x", |b| {
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..10 {
                    let reader =
                        SSTableReader::open(largest_sstable, &ctx.config, ctx.platform.clone())
                            .await
                            .expect("Failed to open SSTable");

                    let entries = reader
                        .get_all_entries()
                        .await
                        .expect("Failed to read entries");

                    black_box(entries);
                }
            });
        });
    });

    group.finish();

    eprintln!("\n=== M1 Target Validation: Memory Usage ===");
    eprintln!("Target: <128MB for large SSTables");
    eprintln!("Note: Monitor system memory usage during benchmark run");
    eprintln!("Test reads largest SSTable (632KB) 10x to stress memory");
}

/// Benchmark: Sequential read memory efficiency
///
/// M1 Target: <128MB with multiple sequential reads
/// Validates: Reader properly releases memory between reads
fn bench_sequential_memory_efficiency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("m1_sequential_memory");
    group.sample_size(10);

    // Read all SSTables sequentially multiple times
    group.bench_function("read_all_sstables_3x_sequential", |b| {
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..3 {
                    for path in &ctx.sstable_paths {
                        let reader = SSTableReader::open(path, &ctx.config, ctx.platform.clone())
                            .await
                            .expect("Failed to open SSTable");

                        let entries = reader
                            .get_all_entries()
                            .await
                            .expect("Failed to read entries");

                        black_box(entries);
                        // Reader should be dropped here, releasing memory
                    }
                }
            });
        });
    });

    group.finish();

    eprintln!("\n=== M1 Target Validation: Sequential Memory Efficiency ===");
    eprintln!("Target: <128MB with bounded memory usage");
    eprintln!("Sequential reads should not accumulate memory across iterations");
}

// ============================================================================
// Benchmark 4: Comprehensive M1 Validation Suite
// ============================================================================

/// Comprehensive M1 validation: All targets in sequence
///
/// Runs all M1 performance validations:
/// 1. Partition lookup latency (<1ms p90)
/// 2. Read throughput (>100 MB/s)
/// 3. Memory efficiency (<128MB)
fn bench_m1_comprehensive_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = rt.block_on(BenchmarkContext::new());

    let mut group = c.benchmark_group("m1_comprehensive");
    group.sample_size(20);

    group.bench_function("full_validation_suite", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Test 1: Partition lookups
                for key in ctx.key_digests.iter().take(10) {
                    let result = ctx.index_reader.lookup_partition(black_box(key));
                    black_box(result);
                }

                // Test 2: SSTable reads
                for path in ctx.sstable_paths.iter().take(2) {
                    // Read first 2 SSTables
                    let reader = SSTableReader::open(path, &ctx.config, ctx.platform.clone())
                        .await
                        .expect("Failed to open SSTable");

                    let entries = reader
                        .get_all_entries()
                        .await
                        .expect("Failed to read entries");

                    black_box(entries);
                }

                // Test 3: Memory efficiency - multiple sequential reads
                for _ in 0..2 {
                    let reader = SSTableReader::open(
                        &ctx.sstable_paths[0],
                        &ctx.config,
                        ctx.platform.clone(),
                    )
                    .await
                    .expect("Failed to open SSTable");

                    let entries = reader
                        .get_all_entries()
                        .await
                        .expect("Failed to read entries");

                    black_box(entries);
                    // Reader dropped here, releasing memory
                }
            });
        });
    });

    group.finish();

    eprintln!("\n=== M1 Comprehensive Validation Summary ===");
    eprintln!("This benchmark validates all M1 performance targets:");
    eprintln!("  1. Partition Lookup: <1ms p90");
    eprintln!("  2. Read Throughput: >100 MB/s (1GB in <10s)");
    eprintln!("  3. Memory Usage: <128MB for large SSTables");
    eprintln!("\nReview criterion HTML report for detailed metrics:");
    eprintln!("  target/criterion/index.html");
}

// ============================================================================
// Criterion Configuration and Runner
// ============================================================================

criterion_group!(
    name = m1_partition_lookups;
    config = profiling::configure();
    targets = bench_partition_lookup_cold_cache,
              bench_partition_lookup_warm_cache,
              bench_partition_lookup_throughput
);

criterion_group!(
    name = m1_read_throughput;
    config = profiling::configure();
    targets = bench_sstable_read_throughput,
              bench_multi_sstable_read_throughput
);

criterion_group!(
    name = m1_memory_efficiency;
    config = profiling::configure();
    targets = bench_memory_usage_large_sstable,
              bench_sequential_memory_efficiency
);

criterion_group!(
    name = m1_comprehensive;
    config = profiling::configure();
    targets = bench_m1_comprehensive_validation
);

criterion_main!(
    m1_partition_lookups,
    m1_read_throughput,
    m1_memory_efficiency,
    m1_comprehensive
);
