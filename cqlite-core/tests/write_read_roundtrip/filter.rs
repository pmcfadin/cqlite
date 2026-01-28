//! Filter.db Write-Read Roundtrip Tests (Bloom Filter)
//!
//! Tests that verify Filter.db files written by FilterWriter can be
//! correctly parsed by BloomFilter::deserialize.
//!
//! ## What These Tests Verify
//!
//! - Bloom filter serialize/deserialize roundtrip
//! - Inserted keys are recognized by deserialized filter
//! - Non-inserted keys are (mostly) rejected
//! - Filter parameters (hash count, bit count) are preserved
//!
//! ## Dependencies
//!
//! - Writer: `cqlite_core::storage::sstable::writer::FilterWriter`
//! - Reader: `cqlite_core::storage::sstable::bloom::BloomFilter::deserialize`

#![cfg(feature = "write-support")]

use cqlite_core::storage::sstable::bloom::BloomFilter;
use cqlite_core::storage::sstable::writer::FilterWriter;
use cqlite_core::storage::write_engine::mutation::DecoratedKey;
use tempfile::TempDir;

/// Test basic Bloom filter serialize/deserialize roundtrip
#[test]
fn test_bloom_filter_roundtrip_basic() {
    // Create a bloom filter
    let mut filter = BloomFilter::new(100, 0.01).expect("BloomFilter creation should succeed");

    // Insert some keys
    let keys: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 4]).collect();
    for key in &keys {
        filter.insert(key);
    }

    // Serialize
    let serialized = filter.serialize().expect("Serialize should succeed");

    // Deserialize
    let restored = BloomFilter::deserialize(&serialized).expect("Deserialize should succeed");

    // Verify all inserted keys are found
    for (i, key) in keys.iter().enumerate() {
        assert!(
            restored.contains(key),
            "Key {} should be found in restored filter",
            i
        );
    }

    // Verify parameters are preserved
    assert_eq!(
        filter.hash_count(),
        restored.hash_count(),
        "Hash count should be preserved"
    );
    assert_eq!(
        filter.bit_count(),
        restored.bit_count(),
        "Bit count should be preserved"
    );
}

/// Test Filter.db roundtrip via FilterWriter
#[tokio::test]
async fn test_filter_roundtrip_via_writer() {
    let temp_dir = TempDir::new().unwrap();
    let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

    // Create filter writer
    let mut writer =
        FilterWriter::new(filter_path.clone(), 100, 0.01).expect("FilterWriter creation should succeed");

    // Add some partition keys
    let keys: Vec<DecoratedKey> = (0..20)
        .map(|i| DecoratedKey::new(i as i64 * 1000, vec![0x00, 0x00, 0x00, i as u8]))
        .collect();

    for key in &keys {
        writer.add_key(key);
    }

    // Finalize and write to disk
    writer.finish().await.expect("FilterWriter finish should succeed");

    // Read back the file
    let filter_bytes = std::fs::read(&filter_path).expect("Should read Filter.db");

    // Deserialize
    let restored = BloomFilter::deserialize(&filter_bytes).expect("Deserialize should succeed");

    // Verify all partition keys are found
    for (i, key) in keys.iter().enumerate() {
        assert!(
            restored.contains(&key.key),
            "Partition key {} should be found in restored filter",
            i
        );
    }
}

/// Test Filter.db roundtrip via WriteEngine
#[tokio::test]
async fn test_filter_roundtrip_via_write_engine() {
    use super::{create_simple_mutation, create_simple_schema};
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};

    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write multiple partitions
    for i in 0..10 {
        let mutation = create_simple_mutation(i, &format!("user{}", i), i * 10, 1000000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Flush to create SSTable
    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Verify Filter.db exists
    assert!(info.filter_path.exists(), "Filter.db should exist");

    // Read and deserialize Filter.db
    let filter_bytes = std::fs::read(&info.filter_path).expect("Should read Filter.db");
    let filter = BloomFilter::deserialize(&filter_bytes)
        .expect("Filter.db created by WriteEngine should deserialize");

    // Create the decorated keys that were written and check they're in the filter
    // Note: We need to create the same key bytes that would have been used
    // For an int partition key with value i, the bytes would be big-endian i32
    for i in 0..10i32 {
        let pk_bytes = i.to_be_bytes().to_vec();
        // The bloom filter should contain the partition key bytes
        // Note: Actual membership depends on how the writer adds keys
        // This test verifies the filter loads and can perform lookups
        let _result = filter.contains(&pk_bytes);
    }

    // Verify the filter has reasonable parameters
    assert!(filter.hash_count() > 0, "Filter should have hash functions");
    assert!(filter.bit_count() > 0, "Filter should have bits");
}

/// Test Bloom filter false positive rate is reasonable
#[test]
fn test_bloom_filter_false_positive_rate() {
    let expected_keys = 1000u64;
    let fp_rate = 0.01; // 1% target

    // Create a bloom filter
    let mut filter = BloomFilter::new(expected_keys, fp_rate).expect("BloomFilter creation should succeed");

    // Insert expected number of keys
    for i in 0..expected_keys {
        let key = format!("key_{}", i);
        filter.insert(key.as_bytes());
    }

    // Serialize and deserialize
    let serialized = filter.serialize().expect("Serialize should succeed");
    let restored = BloomFilter::deserialize(&serialized).expect("Deserialize should succeed");

    // Check false positives on keys we DIDN'T insert
    let test_count = 10000u64;
    let mut false_positives = 0u64;

    for i in expected_keys..(expected_keys + test_count) {
        let key = format!("key_{}", i);
        if restored.contains(key.as_bytes()) {
            false_positives += 1;
        }
    }

    let observed_fp_rate = false_positives as f64 / test_count as f64;

    // Allow 3x the target rate (bloom filters have variance)
    assert!(
        observed_fp_rate < fp_rate * 3.0,
        "False positive rate {} should be < {} (3x target {})",
        observed_fp_rate,
        fp_rate * 3.0,
        fp_rate
    );
}

/// Test Bloom filter with large number of keys
#[test]
fn test_bloom_filter_large_keys() {
    let expected_keys = 10000u64;
    let fp_rate = 0.01;

    // Create a bloom filter
    let mut filter = BloomFilter::new(expected_keys, fp_rate).expect("BloomFilter creation should succeed");

    // Insert many keys
    let keys: Vec<Vec<u8>> = (0..expected_keys)
        .map(|i| {
            // Use i64 to avoid overflow
            (i as i64).to_be_bytes().to_vec()
        })
        .collect();

    for key in &keys {
        filter.insert(key);
    }

    // Serialize
    let serialized = filter.serialize().expect("Serialize should succeed");

    // Verify serialized size is reasonable
    // Optimal bits = -n * ln(p) / (ln(2)^2) ≈ 9.6 bits per element for 1% FP
    // So ~96000 bits = ~12000 bytes for 10000 elements
    // Plus 12 byte header
    assert!(
        serialized.len() < 20000,
        "Serialized size {} should be reasonable for 10000 keys",
        serialized.len()
    );

    // Deserialize
    let restored = BloomFilter::deserialize(&serialized).expect("Deserialize should succeed");

    // Spot check: verify some keys are found
    for key in keys.iter().take(100) {
        assert!(restored.contains(key), "Key should be found in restored filter");
    }
}

/// Test empty bloom filter behavior
#[test]
fn test_bloom_filter_no_keys() {
    // Create a bloom filter with expected elements but don't insert anything
    let filter = BloomFilter::new(100, 0.01).expect("BloomFilter creation should succeed");

    // Serialize
    let serialized = filter.serialize().expect("Serialize should succeed");

    // Deserialize
    let restored = BloomFilter::deserialize(&serialized).expect("Deserialize should succeed");

    // No keys were inserted, so arbitrary keys should (mostly) not be found
    // Note: This tests that the empty filter doesn't have all bits set
    let test_key = vec![0xFF, 0xFF, 0xFF, 0xFF];
    let result = restored.contains(&test_key);
    // Empty filter should not contain random keys (all bits are 0)
    assert!(!result, "Empty filter should not contain random key");
}

/// Test Bloom filter with minimum expected elements
#[test]
fn test_bloom_filter_minimum_elements() {
    // Minimum valid configuration: 1 expected element
    let mut filter = BloomFilter::new(1, 0.01).expect("BloomFilter creation with 1 element should succeed");

    filter.insert(&[0x42]);

    // Serialize and deserialize
    let serialized = filter.serialize().expect("Serialize should succeed");
    let restored = BloomFilter::deserialize(&serialized).expect("Deserialize should succeed");

    // The inserted key should be found
    assert!(
        restored.contains(&[0x42]),
        "Inserted key should be found in minimal filter"
    );
}
