//! Load Testing Suite for CQLite
//!
//! Multi-threaded stress testing for concurrent operations
//! and system stability under high load.

use cqlite_core::platform::Platform;
use cqlite_core::{types::TableId, Config, RowKey, StorageEngine, Value};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::{Barrier, RwLock};
use tokio::time::{sleep, timeout};

// Load test configuration
const LOAD_TEST_DURATION: Duration = Duration::from_secs(30);
const RAMP_UP_DURATION: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_THREADS: usize = 100;
const OPERATIONS_PER_THREAD: usize = 1000;

// Performance counters
struct PerformanceCounters {
    total_operations: AtomicU64,
    successful_operations: AtomicU64,
    failed_operations: AtomicU64,
    read_operations: AtomicU64,
    write_operations: AtomicU64,
    delete_operations: AtomicU64,
    total_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
    min_latency_ns: AtomicU64,
}

impl PerformanceCounters {
    fn new() -> Self {
        Self {
            total_operations: AtomicU64::new(0),
            successful_operations: AtomicU64::new(0),
            failed_operations: AtomicU64::new(0),
            read_operations: AtomicU64::new(0),
            write_operations: AtomicU64::new(0),
            delete_operations: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            max_latency_ns: AtomicU64::new(0),
            min_latency_ns: AtomicU64::new(u64::MAX),
        }
    }

    fn record_operation(&self, operation_type: &str, latency_ns: u64, success: bool) {
        self.total_operations.fetch_add(1, Ordering::SeqCst);

        if success {
            self.successful_operations.fetch_add(1, Ordering::SeqCst);
        } else {
            self.failed_operations.fetch_add(1, Ordering::SeqCst);
        }

        match operation_type {
            "read" => self.read_operations.fetch_add(1, Ordering::SeqCst),
            "write" => self.write_operations.fetch_add(1, Ordering::SeqCst),
            "delete" => self.delete_operations.fetch_add(1, Ordering::SeqCst),
            _ => 0,
        };

        self.total_latency_ns
            .fetch_add(latency_ns, Ordering::SeqCst);

        // Update max latency
        self.max_latency_ns.fetch_max(latency_ns, Ordering::SeqCst);

        // Update min latency
        self.min_latency_ns.fetch_min(latency_ns, Ordering::SeqCst);
    }

    fn get_stats(&self) -> LoadTestStats {
        let total_ops = self.total_operations.load(Ordering::SeqCst);
        let successful_ops = self.successful_operations.load(Ordering::SeqCst);
        let failed_ops = self.failed_operations.load(Ordering::SeqCst);
        let total_latency = self.total_latency_ns.load(Ordering::SeqCst);

        LoadTestStats {
            total_operations: total_ops,
            successful_operations: successful_ops,
            failed_operations: failed_ops,
            read_operations: self.read_operations.load(Ordering::SeqCst),
            write_operations: self.write_operations.load(Ordering::SeqCst),
            delete_operations: self.delete_operations.load(Ordering::SeqCst),
            success_rate: successful_ops as f64 / total_ops as f64,
            average_latency_ns: if total_ops > 0 {
                total_latency / total_ops
            } else {
                0
            },
            max_latency_ns: self.max_latency_ns.load(Ordering::SeqCst),
            min_latency_ns: self.min_latency_ns.load(Ordering::SeqCst),
        }
    }
}

#[derive(Debug, Clone)]
struct LoadTestStats {
    total_operations: u64,
    successful_operations: u64,
    failed_operations: u64,
    read_operations: u64,
    write_operations: u64,
    delete_operations: u64,
    success_rate: f64,
    average_latency_ns: u64,
    max_latency_ns: u64,
    min_latency_ns: u64,
}

// Generate random test data
fn generate_random_key() -> RowKey {
    let random_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();
    RowKey::from(format!("load_test_{}", random_string))
}

fn generate_random_value(size: usize) -> Value {
    let random_data: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect();
    Value::Text(random_data)
}

// Load test worker function
async fn load_test_worker(
    engine: Arc<StorageEngine>,
    table_id: TableId,
    operations_count: usize,
    counters: Arc<PerformanceCounters>,
    stop_signal: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Wait for all threads to be ready
    barrier.wait().await;

    let mut rng = thread_rng();
    let mut local_keys = Vec::new();

    for _ in 0..operations_count {
        if stop_signal.load(Ordering::SeqCst) {
            break;
        }

        let operation_type = match rng.gen_range(0..100) {
            0..=60 => "read",   // 60% reads
            61..=90 => "write", // 30% writes
            _ => "delete",      // 10% deletes
        };

        let start_time = Instant::now();
        let success = match operation_type {
            "read" => {
                if !local_keys.is_empty() {
                    let key = &local_keys[rng.gen_range(0..local_keys.len())];
                    engine.get(&table_id, key).await.is_ok()
                } else {
                    // Generate a random key for read
                    let key = generate_random_key();
                    engine.get(&table_id, &key).await.is_ok()
                }
            }
            "write" => {
                let key = generate_random_key();
                let value = generate_random_value(rng.gen_range(10..1000));

                local_keys.push(key.clone());
                if local_keys.len() > 1000 {
                    local_keys.remove(0);
                }

                engine.put(&table_id, key, value).await.is_ok()
            }
            "delete" => {
                if !local_keys.is_empty() {
                    let key = local_keys.remove(rng.gen_range(0..local_keys.len()));
                    engine.delete(&table_id, key).await.is_ok()
                } else {
                    let key = generate_random_key();
                    engine.delete(&table_id, key).await.is_ok()
                }
            }
            _ => false,
        };

        let latency = start_time.elapsed().as_nanos() as u64;
        counters.record_operation(operation_type, latency, success);

        // Small random delay to simulate real-world usage
        if rng.gen_bool(0.1) {
            sleep(Duration::from_micros(rng.gen_range(1..10))).await;
        }
    }

    Ok(())
}

// Benchmark: Basic Load Test
fn benchmark_basic_load_test(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("load_test");

    for thread_count in [1, 5, 10, 20, 50].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_load", thread_count),
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
                    let table_id = TableId::new("load_test_table");

                    let counters = Arc::new(PerformanceCounters::new());
                    let stop_signal = Arc::new(AtomicBool::new(false));
                    let barrier = Arc::new(Barrier::new(thread_count));

                    let mut handles = Vec::new();

                    // Spawn worker threads
                    for _ in 0..thread_count {
                        let engine = engine.clone();
                        let table_id = table_id.clone();
                        let counters = counters.clone();
                        let stop_signal = stop_signal.clone();
                        let barrier = barrier.clone();

                        let handle = tokio::spawn(async move {
                            load_test_worker(
                                engine,
                                table_id,
                                OPERATIONS_PER_THREAD,
                                counters,
                                stop_signal,
                                barrier,
                            )
                            .await
                        });

                        handles.push(handle);
                    }

                    // Wait for all workers to complete
                    for handle in handles {
                        handle.await.unwrap().unwrap();
                    }

                    let stats = counters.get_stats();
                    black_box(stats);
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Sustained Load Test
fn benchmark_sustained_load_test(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("sustained_load");

    group.bench_function("sustained_30s", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::performance_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = Arc::new(
                StorageEngine::open(temp_dir.path(), &config, platform)
                    .await
                    .unwrap(),
            );
            let table_id = TableId::new("sustained_test_table");

            let counters = Arc::new(PerformanceCounters::new());
            let stop_signal = Arc::new(AtomicBool::new(false));
            let barrier = Arc::new(Barrier::new(10));

            let mut handles = Vec::new();

            // Spawn 10 worker threads
            for _ in 0..10 {
                let engine = engine.clone();
                let table_id = table_id.clone();
                let counters = counters.clone();
                let stop_signal = stop_signal.clone();
                let barrier = barrier.clone();

                let handle = tokio::spawn(async move {
                    load_test_worker(
                        engine,
                        table_id,
                        usize::MAX, // Run until stop signal
                        counters,
                        stop_signal,
                        barrier,
                    )
                    .await
                });

                handles.push(handle);
            }

            // Let the test run for the specified duration
            sleep(LOAD_TEST_DURATION).await;

            // Signal stop
            stop_signal.store(true, Ordering::SeqCst);

            // Wait for all workers to complete
            for handle in handles {
                handle.await.unwrap().unwrap();
            }

            let stats = counters.get_stats();
            let ops_per_second = stats.total_operations as f64 / LOAD_TEST_DURATION.as_secs_f64();

            black_box((stats, ops_per_second));
        });
    });

    group.finish();
}

// Benchmark: Memory Pressure Test
fn benchmark_memory_pressure_test(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("memory_pressure");

    group.bench_function("high_memory_usage", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let mut config = Config::memory_optimized();

            // Set aggressive memory limits
            config.memory.max_memory = 64 * 1024 * 1024; // 64MB
            config.storage.memtable_size_threshold = 4 * 1024 * 1024; // 4MB

            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = Arc::new(
                StorageEngine::open(temp_dir.path(), &config, platform)
                    .await
                    .unwrap(),
            );
            let table_id = TableId::new("memory_pressure_table");

            let counters = Arc::new(PerformanceCounters::new());
            let stop_signal = Arc::new(AtomicBool::new(false));
            let barrier = Arc::new(Barrier::new(5));

            let mut handles = Vec::new();

            // Spawn 5 worker threads with large value operations
            for _ in 0..5 {
                let engine = engine.clone();
                let table_id = table_id.clone();
                let counters = counters.clone();
                let stop_signal = stop_signal.clone();
                let barrier = barrier.clone();

                let handle = tokio::spawn(async move {
                    barrier.wait().await;

                    let mut rng = thread_rng();
                    for _ in 0..1000 {
                        if stop_signal.load(Ordering::SeqCst) {
                            break;
                        }

                        let start_time = Instant::now();
                        let key = generate_random_key();
                        let value = generate_random_value(rng.gen_range(1000..10000)); // Large values

                        let success = engine.put(&table_id, key, value).await.is_ok();
                        let latency = start_time.elapsed().as_nanos() as u64;
                        counters.record_operation("write", latency, success);

                        // Force occasional flushes
                        if rng.gen_bool(0.1) {
                            let _ = engine.flush().await;
                        }
                    }
                });

                handles.push(handle);
            }

            // Run for 15 seconds
            sleep(Duration::from_secs(15)).await;
            stop_signal.store(true, Ordering::SeqCst);

            // Wait for all workers to complete
            for handle in handles {
                handle.await.unwrap();
            }

            let stats = counters.get_stats();
            let engine_stats = engine.stats().await.unwrap();

            black_box((stats, engine_stats));
        });
    });

    group.finish();
}

// Benchmark: Connection Stress Test
fn benchmark_connection_stress_test(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("connection_stress");

    group.bench_function("high_connection_count", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::performance_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = Arc::new(
                StorageEngine::open(temp_dir.path(), &config, platform)
                    .await
                    .unwrap(),
            );
            let table_id = TableId::new("connection_stress_table");

            let counters = Arc::new(PerformanceCounters::new());
            let stop_signal = Arc::new(AtomicBool::new(false));
            let barrier = Arc::new(Barrier::new(MAX_CONCURRENT_THREADS));

            let mut handles = Vec::new();

            // Spawn maximum concurrent threads
            for _ in 0..MAX_CONCURRENT_THREADS {
                let engine = engine.clone();
                let table_id = table_id.clone();
                let counters = counters.clone();
                let stop_signal = stop_signal.clone();
                let barrier = barrier.clone();

                let handle = tokio::spawn(async move {
                    load_test_worker(
                        engine,
                        table_id,
                        100, // Fewer operations per thread
                        counters,
                        stop_signal,
                        barrier,
                    )
                    .await
                });

                handles.push(handle);
            }

            // Wait for all workers to complete
            for handle in handles {
                handle.await.unwrap().unwrap();
            }

            let stats = counters.get_stats();
            black_box(stats);
        });
    });

    group.finish();
}

// Benchmark: Latency Distribution Test
fn benchmark_latency_distribution_test(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("latency_distribution");

    group.bench_function("p99_latency", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::performance_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = Arc::new(
                StorageEngine::open(temp_dir.path(), &config, platform)
                    .await
                    .unwrap(),
            );
            let table_id = TableId::new("latency_test_table");

            let mut latencies = Vec::new();

            // Perform 1000 operations and collect latencies
            for i in 0..1000 {
                let key = RowKey::from(format!("latency_key_{}", i));
                let value = Value::Text(format!("latency_value_{}", i));

                let start = Instant::now();
                engine.put(&table_id, key, value).await.unwrap();
                let latency = start.elapsed().as_nanos() as u64;

                latencies.push(latency);
            }

            // Calculate percentiles
            latencies.sort_unstable();
            let p50 = latencies[latencies.len() / 2];
            let p95 = latencies[latencies.len() * 95 / 100];
            let p99 = latencies[latencies.len() * 99 / 100];

            black_box((p50, p95, p99));
        });
    });

    group.finish();
}

// Benchmark: Failure Recovery Test
fn benchmark_failure_recovery_test(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("failure_recovery");

    group.bench_function("recovery_time", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::performance_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());

            // Create initial engine and populate data
            {
                let engine = StorageEngine::open(temp_dir.path(), &config, platform.clone())
                    .await
                    .unwrap();
                let table_id = TableId::new("recovery_test_table");

                // Insert initial data
                for i in 0..1000 {
                    let key = RowKey::from(format!("recovery_key_{}", i));
                    let value = Value::Text(format!("recovery_value_{}", i));
                    engine.put(&table_id, key, value).await.unwrap();
                }

                // Force flush to ensure data is persisted
                engine.flush().await.unwrap();
            } // Engine is dropped here, simulating a crash

            // Measure recovery time
            let start = Instant::now();
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let recovery_time = start.elapsed();

            // Verify data is still accessible
            let table_id = TableId::new("recovery_test_table");
            let key = RowKey::from("recovery_key_500");
            let result = engine.get(&table_id, &key).await.unwrap();

            black_box((recovery_time, result.is_some()));
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_basic_load_test,
    benchmark_sustained_load_test,
    benchmark_memory_pressure_test,
    benchmark_connection_stress_test,
    benchmark_latency_distribution_test,
    benchmark_failure_recovery_test
);

criterion_main!(benches);
