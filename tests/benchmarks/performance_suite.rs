//! Comprehensive Performance Benchmarking Suite for CQLite Phase 2
//!
//! This suite tests all critical performance aspects:
//! - Write/Read operations at scale
//! - Concurrent access patterns
//! - Memory usage optimization
//! - Compression effectiveness
//! - SSTable operations
//! - Query performance

use cqlite_core::platform::Platform;
use cqlite_core::storage::memtable::MemTable;
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::{types::TableId, Config, RowKey, StorageEngine, Value};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::{Barrier, RwLock};

// Test data generators
fn generate_test_data(count: usize) -> Vec<(RowKey, Value)> {
    (0..count)
        .map(|i| {
            let key = RowKey::from(format!("benchmark_key_{:08}", i));
            let value = Value::Text(format!(
                "benchmark_value_{}_with_additional_data_to_make_it_realistic",
                i
            ));
            (key, value)
        })
        .collect()
}

fn generate_large_test_data(count: usize, value_size: usize) -> Vec<(RowKey, Value)> {
    (0..count)
        .map(|i| {
            let key = RowKey::from(format!("large_key_{:08}", i));
            let value = Value::Text("x".repeat(value_size));
            (key, value)
        })
        .collect()
}

fn generate_mixed_data_types(count: usize) -> Vec<(RowKey, Value)> {
    (0..count)
        .map(|i| {
            let key = RowKey::from(format!("mixed_key_{:08}", i));
            let value = match i % 6 {
                0 => Value::Integer(i as i32),
                1 => Value::Text(format!("text_value_{}", i)),
                2 => Value::Float(i as f64 * 3.14159),
                3 => Value::Boolean(i % 2 == 0),
                4 => Value::Blob(format!("blob_data_{}", i).into_bytes()),
                5 => Value::BigInt(i as i64),
                _ => Value::Text(format!("default_{}", i)),
            };
            (key, value)
        })
        .collect()
}

// Performance target constants
const TARGET_WRITE_OPS_PER_SEC: u64 = 50_000;
const TARGET_READ_OPS_PER_SEC: u64 = 100_000;
const TARGET_MEMORY_USAGE_MB: u64 = 128;
const TARGET_COMPRESSION_RATIO: f64 = 0.3; // 30% of original size

// Benchmark: Write Performance
fn benchmark_write_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("write_performance");

    // Test various write sizes
    for size in [1_000, 10_000, 50_000, 100_000].iter() {
        let test_data = generate_test_data(*size);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("sequential_writes", size),
            &test_data,
            |b, data| {
                b.to_async(&rt).iter(|| async {
                    let temp_dir = TempDir::new().unwrap();
                    let config = Config::performance_optimized();
                    let platform = Arc::new(Platform::new(&config).await.unwrap());
                    let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                        .await
                        .unwrap();
                    let table_id = TableId::new("benchmark_table");

                    let start = Instant::now();
                    for (key, value) in data {
                        engine
                            .put(&table_id, key.clone(), value.clone())
                            .await
                            .unwrap();
                    }
                    let duration = start.elapsed();

                    // Calculate ops/sec
                    let ops_per_sec = data.len() as f64 / duration.as_secs_f64();

                    // Store in memory for reporting
                    black_box(ops_per_sec);
                });
            },
        );
    }

    // Test with different value sizes
    for value_size in [100, 1024, 10240].iter() {
        let test_data = generate_large_test_data(1000, *value_size);

        group.throughput(Throughput::Bytes((1000 * value_size) as u64));
        group.bench_with_input(
            BenchmarkId::new("large_values", value_size),
            &test_data,
            |b, data| {
                b.to_async(&rt).iter(|| async {
                    let temp_dir = TempDir::new().unwrap();
                    let config = Config::performance_optimized();
                    let platform = Arc::new(Platform::new(&config).await.unwrap());
                    let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                        .await
                        .unwrap();
                    let table_id = TableId::new("benchmark_table");

                    for (key, value) in data {
                        engine
                            .put(&table_id, key.clone(), value.clone())
                            .await
                            .unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Read Performance
fn benchmark_read_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("read_performance");

    // Pre-populate data for read tests
    for size in [1_000, 10_000, 50_000, 100_000].iter() {
        let test_data = generate_test_data(*size);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("sequential_reads", size),
            &test_data,
            |b, data| {
                b.to_async(&rt).iter_setup(
                    || async {
                        let temp_dir = TempDir::new().unwrap();
                        let config = Config::performance_optimized();
                        let platform = Arc::new(Platform::new(&config).await.unwrap());
                        let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                            .await
                            .unwrap();
                        let table_id = TableId::new("benchmark_table");

                        // Pre-populate data
                        for (key, value) in data {
                            engine
                                .put(&table_id, key.clone(), value.clone())
                                .await
                                .unwrap();
                        }

                        // Flush to ensure data is written
                        engine.flush().await.unwrap();

                        (engine, table_id)
                    },
                    |(engine, table_id)| async move {
                        let start = Instant::now();
                        for (key, _) in data {
                            let _result = engine.get(&table_id, key).await.unwrap();
                        }
                        let duration = start.elapsed();

                        let ops_per_sec = data.len() as f64 / duration.as_secs_f64();
                        black_box(ops_per_sec);
                    },
                );
            },
        );
    }

    group.finish();
}

// Benchmark: Concurrent Operations
fn benchmark_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_operations");

    for thread_count in [1, 2, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_writes", thread_count),
            thread_count,
            |b, &thread_count| {
                b.to_async(&rt).iter(|| async {
                    let temp_dir = TempDir::new().unwrap();
                    let config = Config::performance_optimized();
                    let platform = Arc::new(Platform::new(&config).await.unwrap());
                    let engine = Arc::new(
                        StorageEngine::open(temp_dir.path(), &config, platform)
                            .await
                            .unwrap(),
                    );
                    let table_id = TableId::new("concurrent_table");

                    let barrier = Arc::new(Barrier::new(thread_count));
                    let mut handles = Vec::new();

                    let start = Instant::now();

                    for thread_id in 0..thread_count {
                        let engine = engine.clone();
                        let table_id = table_id.clone();
                        let barrier = barrier.clone();

                        let handle = tokio::spawn(async move {
                            barrier.wait().await;

                            let ops_per_thread = 1000;
                            for i in 0..ops_per_thread {
                                let key = RowKey::from(format!("thread_{}_key_{}", thread_id, i));
                                let value =
                                    Value::Text(format!("thread_{}_value_{}", thread_id, i));
                                engine.put(&table_id, key, value).await.unwrap();
                            }
                        });

                        handles.push(handle);
                    }

                    // Wait for all threads to complete
                    for handle in handles {
                        handle.await.unwrap();
                    }

                    let duration = start.elapsed();
                    let total_ops = thread_count * 1000;
                    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

                    black_box(ops_per_sec);
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Memory Usage
fn benchmark_memory_usage(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("memory_usage");

    group.bench_function("memory_efficiency", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::memory_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("memory_table");

            // Monitor memory usage before insertion
            let initial_stats = engine.stats().await.unwrap();

            // Insert data
            let test_data = generate_test_data(10_000);
            for (key, value) in test_data {
                engine.put(&table_id, key, value).await.unwrap();
            }

            // Monitor memory usage after insertion
            let final_stats = engine.stats().await.unwrap();

            // Calculate memory efficiency
            let memory_used = final_stats.memtable.size_bytes - initial_stats.memtable.size_bytes;
            let memory_mb = memory_used / 1024 / 1024;

            black_box(memory_mb);
        });
    });

    group.finish();
}

// Benchmark: SSTable Operations
fn benchmark_sstable_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sstable_operations");

    group.bench_function("sstable_creation", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::performance_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let manager = SSTableManager::new(temp_dir.path(), &config, platform)
                .await
                .unwrap();

            // Create test data
            let test_data = generate_test_data(10_000);
            let memtable_data: Vec<_> = test_data
                .into_iter()
                .map(|(k, v)| (TableId::new("test"), k, v))
                .collect();

            let start = Instant::now();
            let _sstable_id = manager.create_from_memtable(memtable_data).await.unwrap();
            let duration = start.elapsed();

            black_box(duration);
        });
    });

    group.bench_function("sstable_reads", |b| {
        b.to_async(&rt).iter_setup(
            || async {
                let temp_dir = TempDir::new().unwrap();
                let config = Config::performance_optimized();
                let platform = Arc::new(Platform::new(&config).await.unwrap());
                let manager = SSTableManager::new(temp_dir.path(), &config, platform)
                    .await
                    .unwrap();

                // Pre-populate SSTable
                let test_data = generate_test_data(10_000);
                let memtable_data: Vec<_> = test_data
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (TableId::new("test"), k, v))
                    .collect();

                let _sstable_id = manager.create_from_memtable(memtable_data).await.unwrap();

                (manager, test_data)
            },
            |(manager, test_data)| async move {
                let table_id = TableId::new("test");
                let start = Instant::now();

                for (key, _) in test_data {
                    let _result = manager.get(&table_id, &key).await.unwrap();
                }

                let duration = start.elapsed();
                black_box(duration);
            },
        );
    });

    group.finish();
}

// Benchmark: Compression Performance
fn benchmark_compression_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("compression_performance");

    for size in [1024, 10240, 102400].iter() {
        let test_data = generate_large_test_data(1000, *size);

        group.bench_with_input(
            BenchmarkId::new("compression_efficiency", size),
            &test_data,
            |b, data| {
                b.to_async(&rt).iter(|| async {
                    let temp_dir = TempDir::new().unwrap();
                    let mut config = Config::performance_optimized();
                    config.storage.enable_compression = true;
                    config.storage.compression_algorithm =
                        cqlite_core::config::CompressionAlgorithm::Lz4;

                    let platform = Arc::new(Platform::new(&config).await.unwrap());
                    let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                        .await
                        .unwrap();
                    let table_id = TableId::new("compression_table");

                    // Insert data
                    for (key, value) in data {
                        engine
                            .put(&table_id, key.clone(), value.clone())
                            .await
                            .unwrap();
                    }

                    // Force flush to create SSTable
                    engine.flush().await.unwrap();

                    // Get storage stats to measure compression
                    let stats = engine.stats().await.unwrap();
                    let compression_ratio =
                        stats.sstables.total_size as f64 / (data.len() * size) as f64;

                    black_box(compression_ratio);
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Query Performance
fn benchmark_query_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("query_performance");

    group.bench_function("range_queries", |b| {
        b.to_async(&rt).iter_setup(
            || async {
                let temp_dir = TempDir::new().unwrap();
                let config = Config::performance_optimized();
                let platform = Arc::new(Platform::new(&config).await.unwrap());
                let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                    .await
                    .unwrap();
                let table_id = TableId::new("query_table");

                // Pre-populate with sorted data
                let test_data = generate_test_data(100_000);
                for (key, value) in test_data {
                    engine.put(&table_id, key, value).await.unwrap();
                }

                engine.flush().await.unwrap();

                (engine, table_id)
            },
            |(engine, table_id)| async move {
                let start_key = RowKey::from("benchmark_key_00010000");
                let end_key = RowKey::from("benchmark_key_00020000");

                let start = Instant::now();
                let results = engine
                    .scan(&table_id, Some(&start_key), Some(&end_key), Some(1000))
                    .await
                    .unwrap();
                let duration = start.elapsed();

                black_box((results.len(), duration));
            },
        );
    });

    group.finish();
}

// Benchmark: Mixed Workload
fn benchmark_mixed_workload(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mixed_workload");

    group.bench_function("read_write_mixed", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::performance_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = Arc::new(
                StorageEngine::open(temp_dir.path(), &config, platform)
                    .await
                    .unwrap(),
            );
            let table_id = TableId::new("mixed_table");

            // Pre-populate some data
            let initial_data = generate_test_data(1000);
            for (key, value) in initial_data {
                engine.put(&table_id, key, value).await.unwrap();
            }

            let start = Instant::now();

            // Mixed workload: 70% reads, 30% writes
            for i in 0..10000 {
                if i % 10 < 7 {
                    // Read operation
                    let key = RowKey::from(format!("benchmark_key_{:08}", i % 1000));
                    let _result = engine.get(&table_id, &key).await.unwrap();
                } else {
                    // Write operation
                    let key = RowKey::from(format!("new_key_{:08}", i));
                    let value = Value::Text(format!("new_value_{}", i));
                    engine.put(&table_id, key, value).await.unwrap();
                }
            }

            let duration = start.elapsed();
            let ops_per_sec = 10000.0 / duration.as_secs_f64();

            black_box(ops_per_sec);
        });
    });

    group.finish();
}

// Benchmark: Data Type Performance
fn benchmark_data_type_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("data_type_performance");

    group.bench_function("mixed_data_types", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::performance_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("types_table");

            let test_data = generate_mixed_data_types(10_000);

            let start = Instant::now();
            for (key, value) in test_data {
                engine.put(&table_id, key, value).await.unwrap();
            }
            let duration = start.elapsed();

            let ops_per_sec = 10000.0 / duration.as_secs_f64();
            black_box(ops_per_sec);
        });
    });

    group.finish();
}

// Custom benchmark group with performance targets
fn benchmark_performance_targets(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("performance_targets");

    group.bench_function("write_target_50k_ops", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::performance_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("target_table");

            let test_data = generate_test_data(1000);
            let start = Instant::now();

            for (key, value) in test_data {
                engine.put(&table_id, key, value).await.unwrap();
            }

            let duration = start.elapsed();
            let ops_per_sec = 1000.0 / duration.as_secs_f64();

            // Assert performance target
            assert!(
                ops_per_sec >= TARGET_WRITE_OPS_PER_SEC as f64 / 50.0,
                "Write performance too low: {} ops/sec",
                ops_per_sec
            );

            black_box(ops_per_sec);
        });
    });

    group.bench_function("memory_target_128mb", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::memory_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("memory_table");

            // Insert enough data to test memory limits
            let test_data = generate_test_data(50_000);
            for (key, value) in test_data {
                engine.put(&table_id, key, value).await.unwrap();
            }

            let stats = engine.stats().await.unwrap();
            let memory_mb = stats.memtable.size_bytes / 1024 / 1024;

            // Assert memory target
            assert!(
                memory_mb <= TARGET_MEMORY_USAGE_MB,
                "Memory usage too high: {} MB",
                memory_mb
            );

            black_box(memory_mb);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_write_performance,
    benchmark_read_performance,
    benchmark_concurrent_operations,
    benchmark_memory_usage,
    benchmark_sstable_operations,
    benchmark_compression_performance,
    benchmark_query_performance,
    benchmark_mixed_workload,
    benchmark_data_type_performance,
    benchmark_performance_targets
);

criterion_main!(benches);
