//! Filter.db writer - writes Bloom filter
//!
//! Generates the Filter.db component using Murmur3 128-bit hashing.
//! Provides fast partition existence checks without reading Data.db.
//!
//! ## File Format
//!
//! The Filter.db file uses Cassandra's Bloom filter format:
//! ```text
//! [Hash Count: 4 bytes, big-endian u32]
//! [Bit Count: 8 bytes, big-endian u64]
//! [Bit Array: variable length, big-endian u64 words]
//! ```
//!
//! ## Algorithm
//!
//! Bloom filter sizing follows optimal formulas:
//! - Bits: m = -n * ln(p) / (ln(2)^2)
//! - Hash functions: k = (m/n) * ln(2)
//!
//! Where:
//! - n = expected number of keys
//! - p = target false positive rate (default: 0.01)
//! - m = total bits in filter
//! - k = number of hash functions

use crate::storage::sstable::bloom::BloomFilter;
use crate::storage::write_engine::mutation::DecoratedKey;
use crate::{Error, Result};
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Filter.db component writer (Bloom filter)
///
/// Creates a Bloom filter for fast partition existence checks.
/// Empty SSTables (no partitions) produce no Filter.db file.
///
/// # Example
///
/// ```no_run
/// use cqlite_core::storage::sstable::writer::FilterWriter;
/// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
/// use std::path::PathBuf;
///
/// # async fn example() -> cqlite_core::Result<()> {
/// let mut writer = FilterWriter::new(
///     PathBuf::from("data/ks/table/nb-1-big-Filter.db"),
///     1000,  // expected keys
///     0.01   // 1% false positive rate
/// )?;
///
/// // Add partition keys during SSTable flush
/// let key = DecoratedKey::new(12345, vec![0x00, 0x01, 0x02]);
/// writer.add_key(&key);
///
/// // Finalize and write to disk
/// writer.finish().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct FilterWriter {
    /// Output path for Filter.db
    path: PathBuf,
    /// Bloom filter instance.
    ///
    /// `None` when the bloom filter is disabled (`bloom_filter_fp_chance == 1.0`),
    /// matching Cassandra's `AlwaysPresentFilter`. In that mode `finish()` writes
    /// no `Filter.db` component at all (see [`FilterWriter::is_disabled`]).
    bloom: Option<BloomFilter>,
    /// Number of keys added (for validation)
    keys_added: usize,
}

impl FilterWriter {
    /// Create a new Filter.db writer
    ///
    /// # Parameters
    ///
    /// - `path`: Output path for Filter.db file
    /// - `expected_keys`: Expected number of partition keys (for optimal sizing)
    /// - `fp_chance`: Target false positive rate (typically 0.01 for 1%).
    ///   A value of exactly `1.0` **disables** the bloom filter, matching
    ///   Cassandra's `AlwaysPresentFilter`: no concrete filter is built and
    ///   `finish()` writes no `Filter.db` component.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `expected_keys` is zero
    /// - `fp_chance` is not in range `(0.0, 1.0]`
    ///
    /// # Disabled bloom filter (Issue #852)
    ///
    /// When `fp_chance == 1.0` the bloom filter is disabled. Cassandra backs such
    /// a table with `AlwaysPresentFilter`, which serializes nothing — the SSTable
    /// has **no `Filter.db` component**. This writer therefore builds no concrete
    /// filter, accepts keys as no-ops, and emits nothing from `finish()`. The
    /// caller is responsible for omitting `Filter.db` from the TOC (see
    /// [`FilterWriter::is_disabled`]). This matches Cassandra's BIG writer
    /// tolerating disabled bloom filters (ref `6ab1d9c0`).
    pub fn new(path: PathBuf, expected_keys: usize, fp_chance: f64) -> Result<Self> {
        if expected_keys == 0 {
            return Err(Error::configuration(
                "expected_keys must be greater than 0 for Filter.db",
            ));
        }

        // fp_chance == 1.0 means the bloom filter is disabled
        // (Cassandra's AlwaysPresentFilter). Do not error and do not build a
        // concrete filter — finish() will emit no Filter.db component.
        let bloom = if fp_chance == 1.0 {
            None
        } else {
            Some(BloomFilter::new(expected_keys as u64, fp_chance)?)
        };

        Ok(Self {
            path,
            bloom,
            keys_added: 0,
        })
    }

    /// Whether this writer's bloom filter is disabled
    /// (`bloom_filter_fp_chance == 1.0`, Cassandra's `AlwaysPresentFilter`).
    ///
    /// When `true`, `finish()` writes no `Filter.db` file and the caller must
    /// omit the `Filter.db` component from the TOC to stay byte-faithful to
    /// Cassandra, which writes no such component for disabled filters.
    pub fn is_disabled(&self) -> bool {
        self.bloom.is_none()
    }

    /// Add a partition key to the Bloom filter
    ///
    /// This should be called for each partition written to the SSTable.
    /// Uses the raw partition key bytes (not the token) for hashing.
    ///
    /// # Parameters
    ///
    /// - `key`: DecoratedKey containing the partition key bytes
    pub fn add_key(&mut self, key: &DecoratedKey) {
        // When the filter is disabled (AlwaysPresentFilter), adding a key is a
        // no-op: there is no concrete filter to populate.
        if let Some(bloom) = self.bloom.as_mut() {
            bloom.insert(&key.key);
        }
        self.keys_added += 1;
    }

    /// Finalize and write the Filter.db file
    ///
    /// Serializes the Bloom filter to disk in Cassandra-compatible format.
    /// This method consumes the writer.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - File I/O fails
    /// - Serialization fails
    pub async fn finish(self) -> Result<()> {
        // Disabled bloom filter (AlwaysPresentFilter, Issue #852): Cassandra
        // serializes nothing and writes no Filter.db component. Emit no file so
        // the on-disk layout matches byte-for-byte.
        //
        // Branch-review (#852): a stale Filter.db left over from a previous write
        // at this path/generation would otherwise survive. The reader probes the
        // sibling Filter.db directly (not via the TOC), so a leftover filter could
        // still affect reads despite the TOC omitting it. Remove any existing file,
        // ignoring not-found.
        let bloom = match self.bloom {
            Some(bloom) => bloom,
            None => {
                match std::fs::remove_file(&self.path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(Error::Storage(format!(
                            "Failed to remove stale Filter.db at {}: {}",
                            self.path.display(),
                            e
                        )))
                    }
                }
                return Ok(());
            }
        };

        // Serialize bloom filter to Cassandra format
        let data = bloom.serialize()?;

        // Write to file atomically
        let mut file = File::create(&self.path).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;

        Ok(())
    }

    /// Get the number of keys added so far
    pub fn keys_added(&self) -> usize {
        self.keys_added
    }

    /// Get the output path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get statistics about the Bloom filter
    ///
    /// Returns metadata about the filter's configuration and current state.
    ///
    /// Returns `None` when the bloom filter is disabled
    /// (`bloom_filter_fp_chance == 1.0`, Cassandra's `AlwaysPresentFilter`):
    /// there is no concrete filter to report statistics for.
    pub fn stats(&self) -> Option<BloomFilterStats> {
        let bloom = self.bloom.as_ref()?;
        let inner_stats = bloom.stats();
        Some(BloomFilterStats {
            expected_keys: inner_stats.expected_elements,
            keys_added: self.keys_added,
            bit_count: inner_stats.bit_count,
            hash_count: inner_stats.hash_count,
            target_fp_rate: inner_stats.false_positive_rate,
            current_fp_rate: bloom.current_false_positive_rate(self.keys_added as u64),
            memory_usage: inner_stats.memory_usage,
            fill_ratio: inner_stats.fill_ratio,
        })
    }
}

/// Statistics about a FilterWriter's Bloom filter
#[derive(Debug, Clone)]
pub struct BloomFilterStats {
    /// Expected number of keys (capacity)
    pub expected_keys: u64,
    /// Number of keys actually added
    pub keys_added: usize,
    /// Number of bits in the filter
    pub bit_count: u64,
    /// Number of hash functions
    pub hash_count: u32,
    /// Target false positive rate
    pub target_fp_rate: f64,
    /// Current estimated false positive rate
    pub current_fp_rate: f64,
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// Ratio of bits set (0.0 to 1.0)
    pub fill_ratio: f64,
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::sstable::bloom::BloomFilter as BloomFilterReader;
    use tempfile::TempDir;

    fn create_test_key(token: i64, key_bytes: Vec<u8>) -> DecoratedKey {
        DecoratedKey::new(token, key_bytes)
    }

    #[tokio::test]
    async fn test_filter_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let writer = FilterWriter::new(filter_path.clone(), 100, 0.01).unwrap();
        assert_eq!(writer.keys_added(), 0);
        assert_eq!(writer.path(), filter_path.as_path());
    }

    #[tokio::test]
    async fn test_filter_writer_invalid_params() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        // Zero expected keys
        let result = FilterWriter::new(filter_path.clone(), 0, 0.01);
        assert!(result.is_err());

        // Invalid false positive rate
        let result = FilterWriter::new(filter_path.clone(), 100, 0.0);
        assert!(result.is_err());

        // fp_chance == 1.0 disables the filter (AlwaysPresentFilter); it is NOT
        // an error (Issue #852).
        let result = FilterWriter::new(filter_path.clone(), 100, 1.0);
        assert!(result.is_ok());
        assert!(result.unwrap().is_disabled());
    }

    #[tokio::test]
    async fn test_filter_writer_disabled_emits_no_file() {
        // bloom_filter_fp_chance = 1.0 -> Cassandra AlwaysPresentFilter, which
        // serializes nothing: no Filter.db component is written (Issue #852).
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let mut writer = FilterWriter::new(filter_path.clone(), 100, 1.0).unwrap();
        assert!(writer.is_disabled());

        // Adding keys is a no-op but still counted.
        writer.add_key(&create_test_key(100, b"key1".to_vec()));
        writer.add_key(&create_test_key(200, b"key2".to_vec()));
        assert_eq!(writer.keys_added(), 2);

        // No stats for a disabled filter.
        assert!(writer.stats().is_none());

        writer.finish().await.unwrap();

        // Byte-parity: Cassandra writes NO Filter.db for a disabled filter.
        assert!(
            !filter_path.exists(),
            "disabled bloom filter must not emit a Filter.db component"
        );
    }

    #[tokio::test]
    async fn test_filter_writer_disabled_removes_stale_file() {
        // Branch-review (#852): if a Filter.db already exists at the output path
        // (e.g. a prior write or a reused generation), a disabled filter must
        // remove it so the reader cannot probe a stale sibling component.
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        // Simulate a stale Filter.db left over from a previous write.
        std::fs::write(&filter_path, b"stale bloom filter bytes").unwrap();
        assert!(filter_path.exists());

        let mut writer = FilterWriter::new(filter_path.clone(), 100, 1.0).unwrap();
        assert!(writer.is_disabled());
        writer.add_key(&create_test_key(100, b"key1".to_vec()));

        writer.finish().await.unwrap();

        // The disabled filter must have removed the stale component.
        assert!(
            !filter_path.exists(),
            "disabled bloom filter must remove a pre-existing stale Filter.db"
        );
    }

    #[tokio::test]
    async fn test_filter_writer_normal_still_builds_concrete_filter() {
        // A normal fp_chance must still build and emit a concrete filter.
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let mut writer = FilterWriter::new(filter_path.clone(), 100, 0.01).unwrap();
        assert!(!writer.is_disabled());
        assert!(writer.stats().is_some());

        let key = create_test_key(100, b"present".to_vec());
        writer.add_key(&key);
        writer.finish().await.unwrap();

        assert!(filter_path.exists(), "normal fp_chance must emit Filter.db");
        let data = std::fs::read(&filter_path).unwrap();
        let bloom = BloomFilterReader::deserialize(&data).unwrap();
        assert!(bloom.contains(&key.key));
    }

    #[tokio::test]
    async fn test_filter_writer_add_keys() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let mut writer = FilterWriter::new(filter_path, 10, 0.01).unwrap();

        // Add some keys
        writer.add_key(&create_test_key(100, vec![0x01, 0x02, 0x03]));
        writer.add_key(&create_test_key(200, vec![0x04, 0x05, 0x06]));
        writer.add_key(&create_test_key(300, vec![0x07, 0x08, 0x09]));

        assert_eq!(writer.keys_added(), 3);
    }

    #[tokio::test]
    async fn test_filter_writer_finish_and_verify() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let mut writer = FilterWriter::new(filter_path.clone(), 100, 0.01).unwrap();

        // Add test keys
        let keys = vec![
            create_test_key(100, b"key1".to_vec()),
            create_test_key(200, b"key2".to_vec()),
            create_test_key(300, b"key3".to_vec()),
            create_test_key(400, b"key4".to_vec()),
            create_test_key(500, b"key5".to_vec()),
        ];

        for key in &keys {
            writer.add_key(key);
        }

        // Finish writing
        writer.finish().await.unwrap();

        // Verify file exists
        assert!(filter_path.exists());

        // Read back and verify keys
        let data = std::fs::read(&filter_path).unwrap();
        let bloom = BloomFilterReader::deserialize(&data).unwrap();

        // All inserted keys should be present
        for key in &keys {
            assert!(
                bloom.contains(&key.key),
                "Key {:?} should be in bloom filter",
                key.key
            );
        }

        // A key we didn't insert should (probably) not be present
        // Note: There's a small chance of false positive
        let missing_key = b"key_not_inserted";
        let _is_present = bloom.contains(missing_key);
        // We can't assert !is_present due to false positives, but we can check
        // that the filter isn't completely broken (returning true for everything)
        let all_true = (0..10).all(|i| {
            let test_key = format!("nonexistent_key_{}", i);
            bloom.contains(test_key.as_bytes())
        });

        assert!(
            !all_true,
            "Bloom filter shouldn't return true for all nonexistent keys"
        );
    }

    #[tokio::test]
    async fn test_filter_writer_stats() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let mut writer = FilterWriter::new(filter_path, 50, 0.01).unwrap();

        // Add some keys
        for i in 0..25 {
            let key_bytes = format!("partition_key_{}", i).into_bytes();
            writer.add_key(&create_test_key(i, key_bytes));
        }

        let stats = writer.stats().expect("concrete filter has stats");
        assert_eq!(stats.expected_keys, 50);
        assert_eq!(stats.keys_added, 25);
        assert!(stats.bit_count > 0);
        assert!(stats.hash_count > 0);
        assert_eq!(stats.target_fp_rate, 0.01);
        assert!(stats.current_fp_rate >= 0.0);
        assert!(stats.current_fp_rate <= 1.0);
        assert!(stats.memory_usage > 0);
        assert!(stats.fill_ratio > 0.0);
        assert!(stats.fill_ratio < 1.0);
    }

    #[tokio::test]
    async fn test_filter_writer_false_positive_rate() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let expected_keys = 1000;
        let target_fp_rate = 0.01; // 1%
        let mut writer =
            FilterWriter::new(filter_path.clone(), expected_keys, target_fp_rate).unwrap();

        // Add expected number of keys
        for i in 0..expected_keys {
            let key_bytes = format!("key_{}", i).into_bytes();
            writer.add_key(&create_test_key(i as i64, key_bytes));
        }

        // Check that current FP rate is reasonable
        let stats = writer.stats().expect("concrete filter has stats");
        assert!(
            stats.current_fp_rate <= target_fp_rate * 5.0,
            "FP rate {} should be close to target {}",
            stats.current_fp_rate,
            target_fp_rate
        );

        writer.finish().await.unwrap();

        // Read back and test false positive rate empirically
        let data = std::fs::read(&filter_path).unwrap();
        let bloom = BloomFilterReader::deserialize(&data).unwrap();

        // All inserted keys should be present (true positives)
        for i in 0..expected_keys {
            let key_bytes = format!("key_{}", i).into_bytes();
            assert!(
                bloom.contains(&key_bytes),
                "Inserted key should always be found"
            );
        }

        // Test false positive rate with non-inserted keys
        let test_count = 1000;
        let false_positives = (0..test_count)
            .filter(|i| {
                let key_bytes = format!("nonexistent_key_{}", i).into_bytes();
                bloom.contains(&key_bytes)
            })
            .count();

        let measured_fp_rate = false_positives as f64 / test_count as f64;
        // Allow 5x the target rate for statistical variation
        assert!(
            measured_fp_rate <= target_fp_rate * 5.0,
            "Measured FP rate {} exceeds 5x target rate {}",
            measured_fp_rate,
            target_fp_rate
        );
    }

    #[tokio::test]
    async fn test_filter_writer_large_keys() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let mut writer = FilterWriter::new(filter_path.clone(), 10, 0.01).unwrap();

        // Add keys with large byte arrays
        let large_key = vec![0xAB; 1024]; // 1KB key
        writer.add_key(&create_test_key(999, large_key.clone()));

        writer.finish().await.unwrap();

        // Verify large key can be found
        let data = std::fs::read(&filter_path).unwrap();
        let bloom = BloomFilterReader::deserialize(&data).unwrap();
        assert!(bloom.contains(&large_key));
    }

    #[tokio::test]
    async fn test_filter_writer_duplicate_keys() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let mut writer = FilterWriter::new(filter_path.clone(), 10, 0.01).unwrap();

        // Add same key multiple times
        let key = create_test_key(100, b"duplicate_key".to_vec());
        writer.add_key(&key);
        writer.add_key(&key);
        writer.add_key(&key);

        assert_eq!(writer.keys_added(), 3); // Counts all additions

        writer.finish().await.unwrap();

        // Verify key is present
        let data = std::fs::read(&filter_path).unwrap();
        let bloom = BloomFilterReader::deserialize(&data).unwrap();
        assert!(bloom.contains(b"duplicate_key"));
    }

    #[tokio::test]
    async fn test_filter_writer_empty_key() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let mut writer = FilterWriter::new(filter_path.clone(), 10, 0.01).unwrap();

        // Add empty key (edge case)
        let empty_key = create_test_key(0, vec![]);
        writer.add_key(&empty_key);

        writer.finish().await.unwrap();

        // Verify empty key handling
        let data = std::fs::read(&filter_path).unwrap();
        let bloom = BloomFilterReader::deserialize(&data).unwrap();
        assert!(bloom.contains(&[]));
    }

    #[tokio::test]
    async fn test_filter_writer_file_format() {
        let temp_dir = TempDir::new().unwrap();
        let filter_path = temp_dir.path().join("nb-1-big-Filter.db");

        let mut writer = FilterWriter::new(filter_path.clone(), 100, 0.01).unwrap();

        // Add a key
        writer.add_key(&create_test_key(123, b"test".to_vec()));
        writer.finish().await.unwrap();

        // Verify file format matches Cassandra specification
        let data = std::fs::read(&filter_path).unwrap();

        // Minimum size: 4 bytes (hash count) + 4 bytes (num longs) + at least 8 bytes (bit array)
        assert!(data.len() >= 16, "Filter.db file too small");

        // Read hash count (4 bytes, big-endian)
        let hash_count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert!(hash_count > 0, "Hash count should be positive");
        assert!(hash_count < 100, "Hash count should be reasonable");

        // Read num longs (4 bytes, big-endian) - Cassandra OffHeapBitSet format
        let num_longs = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        assert!(num_longs > 0, "Num longs should be positive");

        // Verify bit array size
        let expected_size = 8 + (num_longs as usize * 8);
        assert_eq!(
            data.len(),
            expected_size,
            "File size doesn't match expected format"
        );
    }
}
