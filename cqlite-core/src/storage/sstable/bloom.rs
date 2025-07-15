//! Bloom filter implementation for efficient key lookups

use crate::{Error, Result};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

/// Bloom filter for efficient key existence checks
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BloomFilter {
    /// Bit array for the bloom filter
    bits: Vec<u64>,
    /// Number of hash functions
    hash_count: u32,
    /// Number of bits in the filter
    bit_count: u64,
    /// Expected number of elements
    expected_elements: u64,
    /// Target false positive rate
    false_positive_rate: f64,
}

impl BloomFilter {
    /// Create a new bloom filter
    pub fn new(expected_elements: u64, false_positive_rate: f64) -> Result<Self> {
        if false_positive_rate <= 0.0 || false_positive_rate >= 1.0 {
            return Err(Error::configuration(
                "false_positive_rate must be between 0 and 1",
            ));
        }

        if expected_elements == 0 {
            return Err(Error::configuration(
                "expected_elements must be greater than 0",
            ));
        }

        // Calculate optimal bit count: m = -(n * ln(p)) / (ln(2)^2)
        let bit_count = (-(expected_elements as f64 * false_positive_rate.ln())
            / (2.0_f64.ln().powi(2)))
        .ceil() as u64;

        // Calculate optimal hash count: k = (m / n) * ln(2)
        let hash_count =
            ((bit_count as f64 / expected_elements as f64) * 2.0_f64.ln()).ceil() as u32;

        // Ensure we have at least one hash function
        let hash_count = hash_count.max(1);

        // Calculate number of u64 words needed
        let word_count = (bit_count + 63) / 64;

        Ok(Self {
            bits: vec![0u64; word_count as usize],
            hash_count,
            bit_count,
            expected_elements,
            false_positive_rate,
        })
    }

    /// Insert a key into the bloom filter
    pub fn insert(&mut self, key: &[u8]) {
        let hashes = self.calculate_hashes(key);

        for i in 0..self.hash_count {
            let hash = hashes.0.wrapping_add(i as u64 * hashes.1);
            let bit_index = (hash % self.bit_count) as usize;
            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;

            if word_index < self.bits.len() {
                self.bits[word_index] |= 1u64 << bit_offset;
            }
        }
    }

    /// Check if a key might exist in the bloom filter
    pub fn contains(&self, key: &[u8]) -> bool {
        let hashes = self.calculate_hashes(key);

        for i in 0..self.hash_count {
            let hash = hashes.0.wrapping_add(i as u64 * hashes.1);
            let bit_index = (hash % self.bit_count) as usize;
            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;

            if word_index >= self.bits.len() {
                return false;
            }

            if (self.bits[word_index] & (1u64 << bit_offset)) == 0 {
                return false;
            }
        }

        true
    }

    /// Alias for contains method (Cassandra-style naming)
    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.contains(key)
    }

    /// Calculate two independent hash values for double hashing
    fn calculate_hashes(&self, key: &[u8]) -> (u64, u64) {
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();

        // Use different seeds for the two hash functions
        hasher1.write(key);
        hasher1.write(&[0xAA]);

        hasher2.write(key);
        hasher2.write(&[0x55]);

        (hasher1.finish(), hasher2.finish())
    }

    /// Get the number of hash functions
    pub fn hash_count(&self) -> u32 {
        self.hash_count
    }

    /// Get the number of bits in the filter
    pub fn bit_count(&self) -> u64 {
        self.bit_count
    }

    /// Get the expected false positive rate
    pub fn false_positive_rate(&self) -> f64 {
        self.false_positive_rate
    }

    /// Get the expected number of elements
    pub fn expected_elements(&self) -> u64 {
        self.expected_elements
    }

    /// Calculate the current false positive rate estimate
    pub fn current_false_positive_rate(&self, inserted_count: u64) -> f64 {
        if inserted_count == 0 {
            return 0.0;
        }

        // Calculate the probability that a bit is still 0
        let prob_bit_zero = (1.0 - 1.0 / self.bit_count as f64)
            .powf(self.hash_count as f64 * inserted_count as f64);

        // Calculate false positive rate
        (1.0 - prob_bit_zero).powf(self.hash_count as f64)
    }

    /// Serialize the bloom filter to bytes
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::serialization(e.to_string()))
    }

    /// Deserialize a bloom filter from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(|e| Error::serialization(e.to_string()))
    }

    /// Clear all bits in the bloom filter
    pub fn clear(&mut self) {
        for word in &mut self.bits {
            *word = 0;
        }
    }

    /// Get the memory usage of the bloom filter in bytes
    pub fn memory_usage(&self) -> usize {
        self.bits.len() * 8 + std::mem::size_of::<Self>()
    }

    /// Get statistics about the bloom filter
    pub fn stats(&self) -> BloomFilterStats {
        let bits_set = self.bits.iter().map(|word| word.count_ones() as u64).sum();
        let fill_ratio = bits_set as f64 / self.bit_count as f64;

        BloomFilterStats {
            bit_count: self.bit_count,
            hash_count: self.hash_count,
            expected_elements: self.expected_elements,
            false_positive_rate: self.false_positive_rate,
            memory_usage: self.memory_usage(),
            bits_set,
            fill_ratio,
        }
    }

    /// Load bloom filter from a file/reader
    pub async fn load<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        // For now, return a default bloom filter as a placeholder
        // In a real implementation, this would deserialize the bloom filter from the reader
        Self::new(1000, 0.01) // Default parameters
    }
}

/// Statistics about a bloom filter
#[derive(Debug, Clone)]
pub struct BloomFilterStats {
    /// Number of bits in the filter
    pub bit_count: u64,
    /// Number of hash functions
    pub hash_count: u32,
    /// Expected number of elements
    pub expected_elements: u64,
    /// Target false positive rate
    pub false_positive_rate: f64,
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// Number of bits set to 1
    pub bits_set: u64,
    /// Ratio of bits set (0.0 to 1.0)
    pub fill_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_creation() {
        let bloom = BloomFilter::new(1000, 0.01).unwrap();
        assert!(bloom.bit_count > 0);
        assert!(bloom.hash_count > 0);
        assert_eq!(bloom.expected_elements, 1000);
        assert_eq!(bloom.false_positive_rate, 0.01);
    }

    #[test]
    fn test_bloom_filter_insert_and_contains() {
        let mut bloom = BloomFilter::new(100, 0.01).unwrap();

        let key1 = b"test_key_1";
        let key2 = b"test_key_2";
        let key3 = b"test_key_3";

        // Initially, no keys should be present
        assert!(!bloom.contains(key1));
        assert!(!bloom.contains(key2));
        assert!(!bloom.contains(key3));

        // Insert key1
        bloom.insert(key1);
        assert!(bloom.contains(key1));
        assert!(!bloom.contains(key2));
        assert!(!bloom.contains(key3));

        // Insert key2
        bloom.insert(key2);
        assert!(bloom.contains(key1));
        assert!(bloom.contains(key2));
        assert!(!bloom.contains(key3));
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut bloom = BloomFilter::new(1000, 0.01).unwrap();

        // Insert some keys
        for i in 0..100 {
            let key = format!("key_{}", i);
            bloom.insert(key.as_bytes());
        }

        // Calculate false positive rate
        let fp_rate = bloom.current_false_positive_rate(100);
        assert!(fp_rate >= 0.0);
        assert!(fp_rate <= 1.0);
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut bloom = BloomFilter::new(100, 0.01).unwrap();

        // Insert some keys
        bloom.insert(b"key1");
        bloom.insert(b"key2");
        bloom.insert(b"key3");

        // Serialize and deserialize
        let serialized = bloom.serialize().unwrap();
        let deserialized = BloomFilter::deserialize(&serialized).unwrap();

        // Check that deserialized filter works the same
        assert!(deserialized.contains(b"key1"));
        assert!(deserialized.contains(b"key2"));
        assert!(deserialized.contains(b"key3"));
        assert!(!deserialized.contains(b"key4"));
    }

    #[test]
    fn test_bloom_filter_stats() {
        let mut bloom = BloomFilter::new(100, 0.01).unwrap();

        // Insert keys and check stats
        for i in 0..50 {
            let key = format!("key_{}", i);
            bloom.insert(key.as_bytes());
        }

        let stats = bloom.stats();
        assert_eq!(stats.bit_count, bloom.bit_count);
        assert_eq!(stats.hash_count, bloom.hash_count);
        assert_eq!(stats.expected_elements, 100);
        assert!(stats.bits_set > 0);
        assert!(stats.fill_ratio > 0.0);
        assert!(stats.memory_usage > 0);
    }

    #[test]
    fn test_bloom_filter_clear() {
        let mut bloom = BloomFilter::new(100, 0.01).unwrap();

        // Insert keys
        bloom.insert(b"key1");
        bloom.insert(b"key2");
        assert!(bloom.contains(b"key1"));
        assert!(bloom.contains(b"key2"));

        // Clear and verify
        bloom.clear();
        assert!(!bloom.contains(b"key1"));
        assert!(!bloom.contains(b"key2"));
    }

    #[test]
    fn test_bloom_filter_invalid_parameters() {
        // Test invalid false positive rate
        assert!(BloomFilter::new(1000, 0.0).is_err());
        assert!(BloomFilter::new(1000, 1.0).is_err());
        assert!(BloomFilter::new(1000, -0.1).is_err());
        assert!(BloomFilter::new(1000, 1.1).is_err());

        // Test invalid expected elements
        assert!(BloomFilter::new(0, 0.01).is_err());
    }
}
