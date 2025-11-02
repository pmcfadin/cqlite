//! Performance benchmarks for component flattening pre-allocation optimization (Issue #209)
//!
//! This benchmark measures the performance improvement from pre-allocating vectors
//! when flattening multi-component keys.
//!
//! Optimization:
//! - Old approach: Vec::new() followed by multiple extend_from_slice() calls (O(n) allocations)
//! - New approach: Vec::with_capacity(total_size) followed by extend_from_slice() (O(1) allocations)
//!
//! Expected improvement: 20-40% faster for multi-component keys, increasing with component count

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Benchmark component flattening with different component counts
fn bench_component_flattening(c: &mut Criterion) {
    let mut group = c.benchmark_group("component_flattening");

    // Test different component counts: 1, 2, 3, 4, 6, 10, 256
    // These represent realistic scenarios:
    // - 1: Simple single partition key
    // - 2: Composite partition key (common)
    // - 3-4: Multi-component composite keys
    // - 6: Complex composite keys
    // - 10: Wide table scenarios
    // - 256: Stress test / worst case
    for component_count in [1, 2, 3, 4, 6, 10, 256] {
        // Generate realistic component sizes (16-40 bytes, typical for UUIDs, timestamps, keys)
        let components: Vec<Vec<u8>> = (0..component_count)
            .map(|i| vec![0u8; 16 + (i % 4) * 8]) // 16, 24, 32, 40 bytes
            .collect();

        let total_bytes: u64 = components.iter().map(|c| c.len() as u64).sum();
        group.throughput(Throughput::Bytes(total_bytes));

        // Benchmark WITH pre-allocation (optimized approach)
        group.bench_with_input(
            BenchmarkId::new("with_pre_allocation", component_count),
            &components,
            |b, comps| {
                b.iter(|| {
                    // Optimized approach (matches our implementation)
                    let total_size: usize = comps.iter().map(|c| c.len()).sum();
                    let mut key_data = Vec::with_capacity(total_size);
                    for component in black_box(comps) {
                        key_data.extend_from_slice(component);
                    }
                    black_box(key_data)
                });
            },
        );

        // Benchmark WITHOUT pre-allocation (baseline/old approach)
        group.bench_with_input(
            BenchmarkId::new("without_pre_allocation", component_count),
            &components,
            |b, comps| {
                b.iter(|| {
                    // Baseline (old approach)
                    let mut key_data = Vec::new();
                    for component in black_box(comps) {
                        key_data.extend_from_slice(component);
                    }
                    black_box(key_data)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark realistic key sizes and patterns
fn bench_realistic_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_patterns");

    // Pattern 1: UUID + Timestamp (common time-series pattern)
    let uuid_timestamp = vec![
        vec![0u8; 16], // UUID (16 bytes)
        vec![0u8; 8],  // i64 timestamp (8 bytes)
    ];
    let total_bytes: u64 = uuid_timestamp.iter().map(|c| c.len() as u64).sum();
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_with_input(
        BenchmarkId::new("uuid_timestamp", "with_prealloc"),
        &uuid_timestamp,
        |b, comps| {
            b.iter(|| {
                let total_size: usize = comps.iter().map(|c| c.len()).sum();
                let mut key_data = Vec::with_capacity(total_size);
                for component in black_box(comps) {
                    key_data.extend_from_slice(component);
                }
                black_box(key_data)
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("uuid_timestamp", "without_prealloc"),
        &uuid_timestamp,
        |b, comps| {
            b.iter(|| {
                let mut key_data = Vec::new();
                for component in black_box(comps) {
                    key_data.extend_from_slice(component);
                }
                black_box(key_data)
            });
        },
    );

    // Pattern 2: Composite text keys (tenant_id, user_id, session_id)
    let text_keys = vec![
        vec![0u8; 24], // tenant_id (variable text, avg 24 bytes)
        vec![0u8; 32], // user_id (variable text, avg 32 bytes)
        vec![0u8; 16], // session_id (variable text, avg 16 bytes)
    ];
    let total_bytes: u64 = text_keys.iter().map(|c| c.len() as u64).sum();
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_with_input(
        BenchmarkId::new("text_composite", "with_prealloc"),
        &text_keys,
        |b, comps| {
            b.iter(|| {
                let total_size: usize = comps.iter().map(|c| c.len()).sum();
                let mut key_data = Vec::with_capacity(total_size);
                for component in black_box(comps) {
                    key_data.extend_from_slice(component);
                }
                black_box(key_data)
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("text_composite", "without_prealloc"),
        &text_keys,
        |b, comps| {
            b.iter(|| {
                let mut key_data = Vec::new();
                for component in black_box(comps) {
                    key_data.extend_from_slice(component);
                }
                black_box(key_data)
            });
        },
    );

    group.finish();
}

/// Benchmark memory allocation behavior
fn bench_allocation_behavior(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocation_behavior");

    // Test that pre-allocation prevents reallocations
    let components = vec![vec![0u8; 32], vec![0u8; 32], vec![0u8; 32], vec![0u8; 32]];

    group.bench_function("prealloc_no_realloc", |b| {
        b.iter(|| {
            let total_size: usize = components.iter().map(|c| c.len()).sum();
            let mut key_data = Vec::with_capacity(total_size);
            let initial_capacity = key_data.capacity();

            for component in black_box(&components) {
                key_data.extend_from_slice(component);
                // In a real test, we'd verify capacity doesn't change
                // Here we just measure the fast path
            }

            // Verify capacity didn't change (would be caught by unit tests)
            assert_eq!(key_data.capacity(), initial_capacity);
            black_box(key_data)
        });
    });

    group.bench_function("no_prealloc_with_realloc", |b| {
        b.iter(|| {
            let mut key_data = Vec::new();

            for component in black_box(&components) {
                key_data.extend_from_slice(component);
                // This will trigger 1-2 reallocations as vector grows
            }

            black_box(key_data)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_component_flattening,
    bench_realistic_patterns,
    bench_allocation_behavior,
);
criterion_main!(benches);
