//! Index.db writer - writes partition index
//!
//! Generates the Index.db component with BIG format (legacy row index).
//! Maps partition keys to Data.db file offsets for fast partition lookup.
//!
//! # BIG Index Format
//!
//! Index.db stores partition-to-Data.db offset mappings for efficient partition lookup.
//! For M5 Stage 0, we implement the simple BIG format without promoted index.
//!
//! ## Entry Structure (BIG format, NB variant)
//!
//! Each entry stores a partition's location:
//! ```text
//! [marker: 0x0010]              ← 2 bytes, big-endian, marks partition key digest follows
//! [digest: 16 bytes]            ← MD5 hash of partition key
//! [position: VInt]              ← Byte offset in Data.db (relative to data section)
//! [promoted_index_length: VInt] ← Length of promoted index data (0 for simple partitions)
//! [promoted_index_data: bytes]  ← Promoted index (only if length > 0)
//! ```
//!
//! ## Promoted Index
//!
//! Promoted index is used for wide partitions (many clustering keys) to enable fast
//! within-partition seeks. For M5 Stage 0, we skip promoted index writing (length = 0)
//! and can add it later for wide partition support.
//!
//! ## Key Requirements
//!
//! - Entries must be in token order (same as Data.db partition order)
//! - Position offsets must match Data.db partition positions EXACTLY
//! - Partition key digest is MD5 hash of raw key bytes
//! - VInt encoding follows Cassandra unsigned VInt format
//!
//! References:
//! - `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`
//! - `cqlite-core/src/storage/sstable/index_reader.rs` - BIG format parser

use crate::error::Result;
use crate::storage::serialization::vint::encode_unsigned;
use crate::storage::write_engine::mutation::DecoratedKey;

/// Index entry marker (0x0010) - marks partition key digest follows
const PARTITION_KEY_DIGEST_MARKER: u16 = 0x0010;

/// Index.db component writer
///
/// Writes partition index entries in BIG format (NB variant) for Cassandra 5.0 compatibility.
/// Each entry maps a partition key digest to a Data.db file offset.
#[derive(Debug)]
pub struct IndexWriter {
    /// Index entries (partition key + Data.db offset)
    /// Stored in insertion order, which MUST be token-sorted by caller
    entries: Vec<IndexEntry>,
}

/// Internal representation of an index entry
#[derive(Debug, Clone)]
struct IndexEntry {
    /// Partition key (for digest calculation)
    key: DecoratedKey,
    /// Byte offset in Data.db where partition starts
    data_offset: u64,
}

impl IndexWriter {
    /// Create a new Index.db writer
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    ///
    /// let writer = IndexWriter::new();
    /// assert_eq!(writer.entry_count(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add a partition to the index
    ///
    /// # Arguments
    ///
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `data_offset` - Byte offset in Data.db where this partition starts
    ///
    /// # Important
    ///
    /// Partitions MUST be added in token order (caller responsibility).
    /// The Index.db format requires entries to match Data.db partition ordering.
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = IndexWriter::new();
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// writer.add_partition(&key, 0).unwrap();
    /// assert_eq!(writer.entry_count(), 1);
    /// ```
    pub fn add_partition(&mut self, key: &DecoratedKey, data_offset: u64) -> Result<()> {
        self.entries.push(IndexEntry {
            key: key.clone(),
            data_offset,
        });
        Ok(())
    }

    /// Finish writing and return the Index.db bytes
    ///
    /// Serializes all entries in BIG format (NB variant) and returns the complete
    /// Index.db file content.
    ///
    /// # Format
    ///
    /// ```text
    /// [Entry 1]
    /// [Entry 2]
    /// ...
    /// [Entry N]
    /// ```
    ///
    /// Each entry is:
    /// ```text
    /// [0x0010: u16 BE]              ← Marker
    /// [digest: 16 bytes]            ← MD5 of partition key
    /// [position: VInt]              ← Data.db offset
    /// [promoted_length: VInt]       ← 0 (no promoted index in M5 Stage 0)
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = IndexWriter::new();
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// writer.add_partition(&key, 100).unwrap();
    ///
    /// let bytes = writer.finish().unwrap();
    /// assert!(!bytes.is_empty());
    /// ```
    pub fn finish(self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        for entry in &self.entries {
            // Write marker (0x0010, big-endian)
            buffer.extend_from_slice(&PARTITION_KEY_DIGEST_MARKER.to_be_bytes());

            // Write partition key digest (MD5 of raw key bytes)
            let digest = md5::compute(&entry.key.key);
            buffer.extend_from_slice(digest.as_ref());

            // Write position (VInt encoded)
            encode_unsigned(entry.data_offset, &mut buffer);

            // Write promoted index length (0 = no promoted index)
            // M5 Stage 0: Skip promoted index for simplicity
            encode_unsigned(0, &mut buffer);
        }

        Ok(buffer)
    }

    /// Get the number of index entries
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = IndexWriter::new();
    /// assert_eq!(writer.entry_count(), 0);
    ///
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// writer.add_partition(&key, 0).unwrap();
    /// assert_eq!(writer.entry_count(), 1);
    /// ```
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }
}

impl Default for IndexWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_writer_new() {
        let writer = IndexWriter::new();
        assert_eq!(writer.entry_count(), 0);
    }

    #[test]
    fn test_add_single_partition() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]); // int = 42

        writer.add_partition(&key, 0).unwrap();

        assert_eq!(writer.entry_count(), 1);
    }

    #[test]
    fn test_add_multiple_partitions() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);
        let key3 = DecoratedKey::new(300, vec![0x00, 0x00, 0x00, 0x03]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 150).unwrap();
        writer.add_partition(&key3, 300).unwrap();

        assert_eq!(writer.entry_count(), 3);
    }

    #[test]
    fn test_finish_single_entry() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Verify structure:
        // [0x00, 0x10] marker (2 bytes)
        // [digest: 16 bytes]
        // [position: VInt, 1 byte for 0]
        // [promoted_length: VInt, 1 byte for 0]
        assert!(!bytes.is_empty());

        // Check marker
        assert_eq!(bytes[0], 0x00);
        assert_eq!(bytes[1], 0x10);

        // Check digest (MD5 of [0x00, 0x00, 0x00, 0x2A])
        let expected_digest = md5::compute([0x00, 0x00, 0x00, 0x2A]);
        assert_eq!(&bytes[2..18], expected_digest.as_ref());

        // Check position (VInt 0)
        assert_eq!(bytes[18], 0x00);

        // Check promoted index length (VInt 0)
        assert_eq!(bytes[19], 0x00);

        // Total: 2 + 16 + 1 + 1 = 20 bytes
        assert_eq!(bytes.len(), 20);
    }

    #[test]
    fn test_finish_multiple_entries() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 150).unwrap();

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 (marker) + 16 (digest) + 1 (pos=0, VInt) + 1 (promoted_len=0, VInt) = 20 bytes
        // Entry 2: 2 (marker) + 16 (digest) + 2 (pos=150, VInt 0x80 0x96) + 1 (promoted_len=0, VInt) = 21 bytes
        // Total: 41 bytes
        assert_eq!(bytes.len(), 41);

        // Check first entry marker
        assert_eq!(&bytes[0..2], &[0x00, 0x10]);

        // Check second entry marker at offset 20
        assert_eq!(&bytes[20..22], &[0x00, 0x10]);
    }

    #[test]
    fn test_position_encoding() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        // Test various position values to verify VInt encoding
        writer.add_partition(&key, 127).unwrap(); // 1-byte VInt

        let bytes = writer.finish().unwrap();

        // Position at byte 18 (after marker + digest)
        // VInt(127) = 0x7F (single byte)
        assert_eq!(bytes[18], 0x7F);

        // Promoted index length at byte 19
        assert_eq!(bytes[19], 0x00);
    }

    #[test]
    fn test_position_encoding_large_offset() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        // Test large offset requiring multi-byte VInt
        writer.add_partition(&key, 12381).unwrap(); // 2-byte VInt: 0xB0 0x5D

        let bytes = writer.finish().unwrap();

        // Position at byte 18 (after marker + digest)
        // VInt(12381) = 0xB0 0x5D (two bytes)
        assert_eq!(bytes[18], 0xB0);
        assert_eq!(bytes[19], 0x5D);

        // Promoted index length at byte 20
        assert_eq!(bytes[20], 0x00);

        // Total: 2 + 16 + 2 + 1 = 21 bytes
        assert_eq!(bytes.len(), 21);
    }

    #[test]
    fn test_hex_dump_verification() {
        let mut writer = IndexWriter::new();

        // Create a simple partition key
        let key = DecoratedKey::new(12345, vec![0x01, 0x02, 0x03, 0x04]);
        writer.add_partition(&key, 0).unwrap();

        let bytes = writer.finish().unwrap();

        // Print hex dump for manual verification (useful for debugging)
        println!("\nIndex.db hex dump:");
        for (i, chunk) in bytes.chunks(16).enumerate() {
            print!("{:08x}: ", i * 16);
            for byte in chunk {
                print!("{:02x} ", byte);
            }
            println!();
        }

        // Verify marker
        assert_eq!(&bytes[0..2], &[0x00, 0x10], "Marker should be 0x0010");

        // Verify digest is 16 bytes
        assert_eq!(bytes.len(), 20, "Entry should be 20 bytes (marker + digest + pos + promoted_len)");
    }

    #[test]
    fn test_token_order_preservation() {
        let mut writer = IndexWriter::new();

        // Add partitions in token order (caller's responsibility)
        let key1 = DecoratedKey::new(100, vec![0x01]);
        let key2 = DecoratedKey::new(200, vec![0x02]);
        let key3 = DecoratedKey::new(300, vec![0x03]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 100).unwrap();
        writer.add_partition(&key3, 200).unwrap();

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 (marker) + 16 (digest) + 1 (pos=0, VInt) + 1 (promoted_len=0) = 20 bytes
        // Entry 2: 2 (marker) + 16 (digest) + 1 (pos=100, VInt 0x64) + 1 (promoted_len=0) = 20 bytes
        // Entry 3: 2 (marker) + 16 (digest) + 2 (pos=200, VInt 0x80 0xC8) + 1 (promoted_len=0) = 21 bytes
        // Total: 61 bytes
        assert_eq!(bytes.len(), 61);

        // Verify markers for all three entries
        assert_eq!(&bytes[0..2], &[0x00, 0x10]);
        assert_eq!(&bytes[20..22], &[0x00, 0x10]);
        assert_eq!(&bytes[40..42], &[0x00, 0x10]);
    }

    #[test]
    fn test_empty_index() {
        let writer = IndexWriter::new();
        let bytes = writer.finish().unwrap();

        // Empty index should produce empty byte array
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_digest_calculation() {
        let mut writer = IndexWriter::new();

        // Test that different keys produce different digests
        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 100).unwrap();

        let bytes = writer.finish().unwrap();

        // Extract digests
        let digest1 = &bytes[2..18];
        let digest2 = &bytes[22..38];

        // Digests should be different
        assert_ne!(digest1, digest2, "Different keys should produce different digests");

        // Verify digests match MD5 computation
        let expected_digest1 = md5::compute(&[0x00, 0x00, 0x00, 0x01]);
        let expected_digest2 = md5::compute(&[0x00, 0x00, 0x00, 0x02]);

        assert_eq!(digest1, expected_digest1.as_ref());
        assert_eq!(digest2, expected_digest2.as_ref());
    }

    #[test]
    fn test_large_partition_key() {
        let mut writer = IndexWriter::new();

        // Test with a larger partition key (composite key scenario)
        let large_key = vec![0xFF; 100];
        let key = DecoratedKey::new(12345, large_key.clone());

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Verify digest is computed correctly
        let expected_digest = md5::compute(&large_key);
        assert_eq!(&bytes[2..18], expected_digest.as_ref());
    }

    #[test]
    fn test_realistic_scenario() {
        let mut writer = IndexWriter::new();

        // Simulate a realistic SSTable with multiple partitions
        // Partitions must be added in token order (caller ensures this)

        // Partition 1: user_id = 1001
        let key1 = DecoratedKey::new(-5000000000, vec![0x00, 0x00, 0x03, 0xE9]); // 1001
        writer.add_partition(&key1, 0).unwrap();

        // Partition 2: user_id = 1002
        let key2 = DecoratedKey::new(-2000000000, vec![0x00, 0x00, 0x03, 0xEA]); // 1002
        writer.add_partition(&key2, 250).unwrap();

        // Partition 3: user_id = 1003
        let key3 = DecoratedKey::new(3000000000, vec![0x00, 0x00, 0x03, 0xEB]); // 1003
        writer.add_partition(&key3, 500).unwrap();

        // Check entry count before finish
        assert_eq!(writer.entry_count(), 3);

        let bytes = writer.finish().unwrap();

        // Verify structure
        assert!(bytes.len() >= 60); // At least 20 bytes per entry

        // Verify all markers are present
        assert_eq!(&bytes[0..2], &[0x00, 0x10]);
        assert!(bytes.len() > 20);

        // Find second marker (offset varies due to VInt encoding)
        let second_entry_offset = 20; // First entry is exactly 20 bytes (pos=0, 1 byte VInt)
        assert_eq!(&bytes[second_entry_offset..second_entry_offset+2], &[0x00, 0x10]);
    }

    #[test]
    fn test_vint_encoding_boundaries() {
        // Test VInt encoding at various boundaries to verify correct byte counts
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        // Test offset at 127 (max 1-byte VInt)
        writer.add_partition(&key, 127).unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 20); // 2 + 16 + 1 + 1

        // Test offset at 128 (min 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 128).unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 21); // 2 + 16 + 2 + 1

        // Test offset at 16383 (max 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16383).unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 21); // 2 + 16 + 2 + 1

        // Test offset at 16384 (min 3-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16384).unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 22); // 2 + 16 + 3 + 1
    }

    #[test]
    fn test_duplicate_offsets_allowed() {
        // Multiple partitions can have the same offset (e.g., empty partitions)
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x01]);
        let key2 = DecoratedKey::new(200, vec![0x02]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 0).unwrap(); // Same offset

        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 40); // Two entries, both with offset=0
    }

    #[test]
    fn test_zero_offset() {
        // First partition should start at offset 0
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Verify position is VInt 0 (single byte 0x00)
        assert_eq!(bytes[18], 0x00);
    }
}
