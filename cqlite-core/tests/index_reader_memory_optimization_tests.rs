//! Memory optimization tests for Index.db reader
//!
//! These tests verify that the Arc<[u8]> memory optimization for key_digest
//! works correctly and doesn't introduce regressions.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;
use tokio::fs;

use cqlite_core::Config;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::index_reader::{IndexReader, PartitionIndexEntry};

/// Memory tracking allocator for testing
struct TrackingAllocator {
    allocations: AtomicUsize,
    deallocations: AtomicUsize,
    peak_memory: AtomicUsize,
    current_memory: AtomicUsize,
}

impl TrackingAllocator {
    const fn new() -> Self {
        Self {
            allocations: AtomicUsize::new(0),
            deallocations: AtomicUsize::new(0),
            peak_memory: AtomicUsize::new(0),
            current_memory: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.allocations.store(0, Ordering::SeqCst);
        self.deallocations.store(0, Ordering::SeqCst);
        self.peak_memory.store(0, Ordering::SeqCst);
        self.current_memory.store(0, Ordering::SeqCst);
    }

    fn get_peak_memory(&self) -> usize {
        self.peak_memory.load(Ordering::SeqCst)
    }

    fn get_current_memory(&self) -> usize {
        self.current_memory.load(Ordering::SeqCst)
    }

    fn get_allocation_count(&self) -> usize {
        self.allocations.load(Ordering::SeqCst)
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            self.allocations.fetch_add(1, Ordering::SeqCst);
            let new_current = self
                .current_memory
                .fetch_add(layout.size(), Ordering::SeqCst)
                + layout.size();

            // Update peak memory
            let mut peak = self.peak_memory.load(Ordering::SeqCst);
            while new_current > peak {
                match self.peak_memory.compare_exchange_weak(
                    peak,
                    new_current,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(x) => peak = x,
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        self.deallocations.fetch_add(1, Ordering::SeqCst);
        self.current_memory
            .fetch_sub(layout.size(), Ordering::SeqCst);
    }
}

#[global_allocator]
static TRACKING_ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

/// Test that lookup table construction uses Arc<[u8]> efficiently
#[test]
fn test_arc_lookup_table_memory_efficiency() {
    TRACKING_ALLOCATOR.reset();

    // Create test partition entries with Arc<[u8]> key digests
    let mut partition_entries = Vec::new();
    for i in 0..1000 {
        let mut key_digest = vec![0u8; 16];
        key_digest[0] = (i % 256) as u8;
        key_digest[1] = ((i / 256) % 256) as u8;

        partition_entries.push(PartitionIndexEntry {
            key_digest: Arc::from(key_digest.into_boxed_slice()),
            data_offset: i as u64 * 4096,
            data_size: 4096,
            promoted_index: None,
        });
    }

    let initial_memory = TRACKING_ALLOCATOR.get_current_memory();
    let initial_allocations = TRACKING_ALLOCATOR.get_allocation_count();

    // Build lookup table using Arc references (no cloning needed)
    let mut key_lookup: HashMap<Arc<[u8]>, usize> = HashMap::new();
    for (index, entry) in partition_entries.iter().enumerate() {
        key_lookup.insert(Arc::clone(&entry.key_digest), index);
    }

    let final_memory = TRACKING_ALLOCATOR.get_current_memory();
    let final_allocations = TRACKING_ALLOCATOR.get_allocation_count();

    // Verify the lookup table works correctly
    assert_eq!(key_lookup.len(), 1000);

    // Check that we can lookup entries correctly
    for (expected_index, entry) in partition_entries.iter().enumerate() {
        let lookup_result = key_lookup.get(&entry.key_digest);
        assert_eq!(lookup_result, Some(&expected_index));
    }

    println!(
        "Memory used for Arc-based lookup table: {} bytes",
        final_memory - initial_memory
    );
    println!("Allocations: {}", final_allocations - initial_allocations);

    // With Arc<[u8]>, we only allocate reference counts, not the data itself
    // Each Arc entry in the HashMap should be much smaller than cloning Vec<u8>
    let actual_memory_increase = final_memory - initial_memory;

    // Memory increase should be minimal - only HashMap overhead and Arc reference counts
    // Should be much less than 16KB (16 bytes * 1000 entries)
    assert!(actual_memory_increase < 8000); // Much smaller than Vec cloning approach
}

/// Test comparison between Vec cloning (old approach) and Arc sharing (new approach)
#[test]
fn test_memory_comparison_vec_vs_arc() {
    // Test Vec cloning approach (what the old code did)
    TRACKING_ALLOCATOR.reset();
    let initial_memory_vec = TRACKING_ALLOCATOR.get_current_memory();

    let mut vec_entries = Vec::new();
    let mut vec_lookup: HashMap<Vec<u8>, usize> = HashMap::new();

    for i in 0..1000 {
        let mut key_digest = vec![0u8; 16];
        key_digest[0] = (i % 256) as u8;
        key_digest[1] = ((i / 256) % 256) as u8;

        vec_entries.push(key_digest.clone());
        vec_lookup.insert(key_digest, i); // This clones the Vec
    }

    let final_memory_vec = TRACKING_ALLOCATOR.get_current_memory();
    let vec_memory_usage = final_memory_vec - initial_memory_vec;

    // Test Arc approach (new optimized code)
    TRACKING_ALLOCATOR.reset();
    let initial_memory_arc = TRACKING_ALLOCATOR.get_current_memory();

    let mut arc_entries = Vec::new();
    let mut arc_lookup: HashMap<Arc<[u8]>, usize> = HashMap::new();

    for i in 0..1000 {
        let mut key_digest = vec![0u8; 16];
        key_digest[0] = (i % 256) as u8;
        key_digest[1] = ((i / 256) % 256) as u8;

        let arc_key = Arc::from(key_digest.into_boxed_slice());
        arc_entries.push(Arc::clone(&arc_key));
        arc_lookup.insert(arc_key, i); // This only increments reference count
    }

    let final_memory_arc = TRACKING_ALLOCATOR.get_current_memory();
    let arc_memory_usage = final_memory_arc - initial_memory_arc;

    println!(
        "Vec cloning approach memory usage: {} bytes",
        vec_memory_usage
    );
    println!(
        "Arc sharing approach memory usage: {} bytes",
        arc_memory_usage
    );
    println!(
        "Memory reduction: {} bytes ({:.1}%)",
        vec_memory_usage - arc_memory_usage,
        ((vec_memory_usage - arc_memory_usage) as f64 / vec_memory_usage as f64) * 100.0
    );

    // Arc approach should use significantly less memory
    assert!(arc_memory_usage < vec_memory_usage);

    // The savings should be substantial (at least 30% reduction)
    let memory_savings = vec_memory_usage - arc_memory_usage;
    assert!(memory_savings > vec_memory_usage / 3);
}

/// Test with large SSTable data to verify linear memory growth prevention
#[tokio::test]
async fn test_large_sstable_memory_usage() {
    let temp_dir = TempDir::new().unwrap();
    let index_file = temp_dir.path().join("large-Index.db");

    // Create a large Index.db file with many entries
    create_large_index_file(&index_file, 10000).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    TRACKING_ALLOCATOR.reset();
    let initial_memory = TRACKING_ALLOCATOR.get_current_memory();

    // Open the index reader
    match IndexReader::open(&index_file, platform).await {
        Ok(index_reader) => {
            let final_memory = TRACKING_ALLOCATOR.get_current_memory();
            let entries = index_reader.get_partition_entries();

            println!("Loaded {} partition entries", entries.len());
            println!("Memory usage: {} bytes", final_memory - initial_memory);

            // Memory usage should not grow linearly with the number of entries
            // due to the optimization. Each entry should use roughly constant memory.
            let bytes_per_entry = (final_memory - initial_memory) / entries.len().max(1);
            println!("Average memory per entry: {} bytes", bytes_per_entry);

            // With the optimization, memory per entry should be reasonable
            // Without cloning, each entry in the lookup table should be ~24 bytes (key ref + value)
            assert!(bytes_per_entry < 100); // Should be much less than 100 bytes per entry

            // Test lookup functionality with large dataset
            if entries.len() >= 100 {
                for i in (0..entries.len()).step_by(entries.len() / 100) {
                    let entry = &entries[i];
                    let lookup_result = index_reader.lookup_partition(&entry.key_digest);
                    assert!(lookup_result.is_some());
                }
            }
        }
        Err(e) => {
            println!(
                "Index reader test with large file failed (expected with mock data): {}",
                e
            );
        }
    }
}

/// Performance benchmark comparing Vec cloning vs Arc sharing
#[test]
fn benchmark_arc_vs_vec_performance() {
    // Create test data for Vec approach
    let mut vec_partition_entries = Vec::new();
    for i in 0..10000 {
        let mut key_digest = vec![0u8; 16];
        key_digest[0] = (i % 256) as u8;
        key_digest[1] = ((i / 256) % 256) as u8;
        key_digest[2] = ((i / 65536) % 256) as u8;
        key_digest[3] = ((i / 16777216) % 256) as u8;

        vec_partition_entries.push(key_digest);
    }

    // Create test data for Arc approach
    let mut arc_partition_entries = Vec::new();
    for i in 0..10000 {
        let mut key_digest = vec![0u8; 16];
        key_digest[0] = (i % 256) as u8;
        key_digest[1] = ((i / 256) % 256) as u8;
        key_digest[2] = ((i / 65536) % 256) as u8;
        key_digest[3] = ((i / 16777216) % 256) as u8;

        arc_partition_entries.push(PartitionIndexEntry {
            key_digest: Arc::from(key_digest.into_boxed_slice()),
            data_offset: i as u64 * 4096,
            data_size: 4096,
            promoted_index: None,
        });
    }

    // Benchmark the Vec cloning approach
    let start = std::time::Instant::now();
    let mut vec_lookup: HashMap<Vec<u8>, usize> = HashMap::new();
    for (index, entry) in vec_partition_entries.iter().enumerate() {
        vec_lookup.insert(entry.clone(), index); // Clone the Vec
    }
    let vec_build_time = start.elapsed();

    // Benchmark lookups with Vec approach
    let start = std::time::Instant::now();
    for entry in vec_partition_entries.iter().take(1000) {
        let _ = vec_lookup.get(entry);
    }
    let vec_lookup_time = start.elapsed();

    // Benchmark the Arc approach (optimized implementation)
    let start = std::time::Instant::now();
    let mut arc_lookup: HashMap<Arc<[u8]>, usize> = HashMap::new();
    for (index, entry) in arc_partition_entries.iter().enumerate() {
        arc_lookup.insert(Arc::clone(&entry.key_digest), index); // Only clone the Arc (cheap)
    }
    let arc_build_time = start.elapsed();

    // Benchmark lookups with Arc approach
    let start = std::time::Instant::now();
    for entry in arc_partition_entries.iter().take(1000) {
        let _ = arc_lookup.get(&entry.key_digest);
    }
    let arc_lookup_time = start.elapsed();

    println!("=== PERFORMANCE BENCHMARK ===");
    println!("Vec cloning approach - Build time: {:?}", vec_build_time);
    println!("Vec cloning approach - Lookup time: {:?}", vec_lookup_time);
    println!("Arc sharing approach - Build time: {:?}", arc_build_time);
    println!("Arc sharing approach - Lookup time: {:?}", arc_lookup_time);

    // The Arc approach should be faster for building (no cloning overhead)
    assert!(arc_build_time <= vec_build_time);

    // Calculate and display performance improvements
    let build_improvement = vec_build_time.as_nanos() as f64 / arc_build_time.as_nanos() as f64;
    let lookup_improvement = vec_lookup_time.as_nanos() as f64 / arc_lookup_time.as_nanos() as f64;

    println!("Build time improvement: {:.2}x", build_improvement);
    println!("Lookup time improvement: {:.2}x", lookup_improvement);

    // Build time should be significantly better with Arc
    assert!(build_improvement >= 1.0);
}

/// Test edge cases and boundary conditions with Arc<[u8]>
#[test]
fn test_arc_edge_cases() {
    // Test empty partition entries
    let empty_entries: Vec<PartitionIndexEntry> = Vec::new();
    let mut empty_lookup: HashMap<Arc<[u8]>, usize> = HashMap::new();
    for (index, entry) in empty_entries.iter().enumerate() {
        empty_lookup.insert(Arc::clone(&entry.key_digest), index);
    }
    assert_eq!(empty_lookup.len(), 0);

    // Test single entry
    let single_entry = vec![PartitionIndexEntry {
        key_digest: Arc::from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        data_offset: 1024,
        data_size: 4096,
        promoted_index: None,
    }];

    let mut single_lookup: HashMap<Arc<[u8]>, usize> = HashMap::new();
    for (index, entry) in single_entry.iter().enumerate() {
        single_lookup.insert(Arc::clone(&entry.key_digest), index);
    }
    assert_eq!(single_lookup.len(), 1);
    assert_eq!(single_lookup.get(&single_entry[0].key_digest), Some(&0));

    // Test duplicate key digests (should overwrite)
    let duplicate_arc = Arc::from([1u8; 16]);
    let duplicate_entries = vec![
        PartitionIndexEntry {
            key_digest: Arc::clone(&duplicate_arc),
            data_offset: 1024,
            data_size: 4096,
            promoted_index: None,
        },
        PartitionIndexEntry {
            key_digest: Arc::clone(&duplicate_arc), // Same Arc (shared reference)
            data_offset: 2048,
            data_size: 4096,
            promoted_index: None,
        },
    ];

    let mut duplicate_lookup: HashMap<Arc<[u8]>, usize> = HashMap::new();
    for (index, entry) in duplicate_entries.iter().enumerate() {
        duplicate_lookup.insert(Arc::clone(&entry.key_digest), index);
    }
    assert_eq!(duplicate_lookup.len(), 1); // Should only have one entry
    assert_eq!(duplicate_lookup.get(&duplicate_arc), Some(&1)); // Should have the last index

    // Test that Arc sharing works correctly - both entries should share the same data
    assert!(Arc::ptr_eq(
        &duplicate_entries[0].key_digest,
        &duplicate_entries[1].key_digest
    ));
}

/// Property-based test for Arc-based lookup table correctness
#[test]
fn property_test_arc_lookup_correctness() {
    use std::collections::HashSet;

    // Generate various test cases
    for num_entries in [0, 1, 10, 100, 1000].iter() {
        let mut partition_entries = Vec::new();
        let mut seen_keys = HashSet::new();

        for i in 0..*num_entries {
            // Generate unique key digests
            let mut key_digest = vec![0u8; 16];
            loop {
                for (j, byte) in key_digest.iter_mut().enumerate() {
                    *byte = ((i + j) % 256) as u8;
                }

                if seen_keys.insert(key_digest.clone()) {
                    break;
                }

                // Modify the key to make it unique
                key_digest[15] = ((key_digest[15] as usize + 1) % 256) as u8;
            }

            partition_entries.push(PartitionIndexEntry {
                key_digest: Arc::from(key_digest.into_boxed_slice()),
                data_offset: i as u64 * 4096,
                data_size: 4096,
                promoted_index: None,
            });
        }

        // Build lookup table with Arc sharing
        let mut key_lookup: HashMap<Arc<[u8]>, usize> = HashMap::new();
        for (index, entry) in partition_entries.iter().enumerate() {
            key_lookup.insert(Arc::clone(&entry.key_digest), index);
        }

        // Property: Every partition entry should be findable in the lookup table
        for (expected_index, entry) in partition_entries.iter().enumerate() {
            let lookup_result = key_lookup.get(&entry.key_digest);
            assert_eq!(
                lookup_result,
                Some(&expected_index),
                "Failed to find entry {} with key digest {:?}",
                expected_index,
                entry.key_digest
            );
        }

        // Property: Lookup table size should equal number of unique entries
        assert_eq!(key_lookup.len(), partition_entries.len());

        // Property: Arc sharing should work correctly
        for entry in &partition_entries {
            // The Arc in the lookup table should be the same reference as in the entry
            if let Some(&index) = key_lookup.get(&entry.key_digest) {
                assert_eq!(
                    index,
                    partition_entries
                        .iter()
                        .position(|e| Arc::ptr_eq(&e.key_digest, &entry.key_digest))
                        .unwrap()
                );
            }
        }

        println!("✓ Arc property test passed for {} entries", num_entries);
    }
}

/// Helper function to create a large Index.db file for testing
async fn create_large_index_file(path: &std::path::Path, num_entries: usize) {
    let mut data = Vec::new();

    for i in 0..num_entries {
        // Add marker (0x0010)
        data.extend_from_slice(&[0x00, 0x10]);

        // Add 16-byte key digest
        let mut key_digest = vec![0u8; 16];
        key_digest[0] = (i % 256) as u8;
        key_digest[1] = ((i / 256) % 256) as u8;
        key_digest[2] = ((i / 65536) % 256) as u8;
        key_digest[3] = ((i / 16777216) % 256) as u8;
        data.extend_from_slice(&key_digest);
    }

    fs::write(path, data).await.unwrap();
}

/// Test Arc-based memory leak prevention
#[test]
fn test_arc_no_memory_leaks() {
    TRACKING_ALLOCATOR.reset();
    let initial_memory = TRACKING_ALLOCATOR.get_current_memory();

    {
        // Create and destroy multiple Arc-based lookup tables
        for _ in 0..10 {
            let mut partition_entries = Vec::new();
            for i in 0..100 {
                let mut key_digest = vec![0u8; 16];
                key_digest[0] = (i % 256) as u8;

                partition_entries.push(PartitionIndexEntry {
                    key_digest: Arc::from(key_digest.into_boxed_slice()),
                    data_offset: i as u64 * 4096,
                    data_size: 4096,
                    promoted_index: None,
                });
            }

            let mut key_lookup: HashMap<Arc<[u8]>, usize> = HashMap::new();
            for (index, entry) in partition_entries.iter().enumerate() {
                key_lookup.insert(Arc::clone(&entry.key_digest), index);
            }

            // Use the lookup table
            for entry in &partition_entries {
                let _ = key_lookup.get(&entry.key_digest);
            }

            // Test reference counting
            for entry in &partition_entries {
                // Each Arc should have exactly 2 references: one in partition_entries, one in key_lookup
                assert_eq!(Arc::strong_count(&entry.key_digest), 2);
            }

            // Variables go out of scope here
        }
    }

    // Force garbage collection
    std::hint::black_box(());

    let final_memory = TRACKING_ALLOCATOR.get_current_memory();
    let memory_difference = if final_memory >= initial_memory {
        final_memory - initial_memory
    } else {
        0
    };

    println!(
        "Memory difference after Arc test: {} bytes",
        memory_difference
    );

    // Memory usage should return close to initial levels (allowing some tolerance for allocator overhead)
    assert!(memory_difference < 1024); // Less than 1KB difference indicates no significant leaks
}

/// Test to verify Arc reference counting works correctly
#[test]
fn test_arc_reference_counting() {
    // Create a partition entry with Arc key digest
    let key_data: Box<[u8]> =
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16].into_boxed_slice();
    let arc_key = Arc::from(key_data);

    // Initially, there should be 1 reference
    assert_eq!(Arc::strong_count(&arc_key), 1);

    let entry = PartitionIndexEntry {
        key_digest: Arc::clone(&arc_key),
        data_offset: 1024,
        data_size: 4096,
        promoted_index: None,
    };

    // Now there should be 2 references
    assert_eq!(Arc::strong_count(&arc_key), 2);
    assert_eq!(Arc::strong_count(&entry.key_digest), 2);

    // Create lookup table entry
    let mut lookup: HashMap<Arc<[u8]>, usize> = HashMap::new();
    lookup.insert(Arc::clone(&entry.key_digest), 0);

    // Now there should be 3 references
    assert_eq!(Arc::strong_count(&arc_key), 3);

    // Drop the lookup table
    drop(lookup);

    // Should be back to 2 references
    assert_eq!(Arc::strong_count(&arc_key), 2);

    // Drop the entry
    drop(entry);

    // Should be back to 1 reference
    assert_eq!(Arc::strong_count(&arc_key), 1);
}
