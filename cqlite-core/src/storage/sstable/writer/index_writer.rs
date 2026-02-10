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
//! [key_len: u16 BE]             ← writeWithShortLength prefix
//! [key_bytes: key_len bytes]    ← Raw partition key bytes (same as Data.db)
//! [position: unsigned VInt]     ← Byte offset in Data.db
//! [promoted_index_size: unsigned VInt] ← 0 for simple partitions
//! ```
//!
//! ## Key Requirements
//!
//! - Entries must be in token order (same as Data.db partition order)
//! - Position offsets must match Data.db partition positions EXACTLY
//! - Key bytes must match the raw partition key in Data.db exactly
//! - VInt encoding follows Cassandra unsigned VInt format
//!
//! References:
//! - `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`
//! - `cqlite-core/src/storage/sstable/index_reader.rs` - BIG format parser

use crate::error::{Error, Result};
use crate::storage::serialization::vint::encode_unsigned;
use crate::storage::write_engine::mutation::DecoratedKey;
use std::io::Write;


/// Index.db component writer
///
/// Writes partition index entries in BIG format (NB variant) for Cassandra 5.0 compatibility.
/// Each entry maps a partition key digest to a Data.db file offset.
///
/// # Memory Management
///
/// To avoid unbounded memory growth, this writer uses streaming writes:
/// - Entries are serialized immediately and stored in a temporary buffer
/// - Only metadata needed for Summary.db sampling is kept in memory
/// - Memory usage is bounded by the number of entries (not their size)
#[derive(Debug)]
pub struct IndexWriter {
    /// Serialized index data (written incrementally)
    buffer: Vec<u8>,
    /// Entry count (for validation)
    entry_count: usize,
}

/// Information about a written index entry
///
/// Returned by `add_partition()` to track Summary.db sampling points.
#[derive(Debug, Clone, Copy)]
pub struct IndexEntryInfo {
    /// Byte offset in Index.db where this entry starts
    pub index_offset: u64,
    /// Size of this entry in bytes
    pub entry_size: usize,
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
            buffer: Vec::new(),
            entry_count: 0,
        }
    }

    /// Add a partition to the index
    ///
    /// This method serializes the entry immediately and returns information about
    /// its position in the index. This enables streaming writes and accurate
    /// Summary.db offset tracking.
    ///
    /// # Arguments
    ///
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `data_offset` - Byte offset in Data.db where this partition starts
    ///
    /// # Returns
    ///
    /// `IndexEntryInfo` containing the exact byte offset where this entry was written
    /// in Index.db and the size of the entry. Use this for Summary.db sampling.
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
    /// let info = writer.add_partition(&key, 0).unwrap();
    /// assert_eq!(info.index_offset, 0); // First entry starts at offset 0
    /// assert_eq!(writer.entry_count(), 1);
    /// ```
    pub fn add_partition(
        &mut self,
        key: &DecoratedKey,
        data_offset: u64,
    ) -> Result<IndexEntryInfo> {
        // Capture the offset BEFORE writing
        let index_offset = self.buffer.len() as u64;

        // Write entry directly to buffer
        let entry_size = self.write_entry(key, data_offset)?;

        self.entry_count += 1;

        Ok(IndexEntryInfo {
            index_offset,
            entry_size,
        })
    }

    /// Write a single index entry to the buffer
    ///
    /// Cassandra BigFormat Index.db entry:
    /// ```text
    /// [key_len: u16 BE]              ← writeWithShortLength prefix
    /// [key_bytes: key_len bytes]     ← raw partition key bytes
    /// [position: unsigned VInt]      ← Data.db offset
    /// [promoted_index_size: unsigned VInt] ← 0 for simple partitions
    /// ```
    ///
    /// Returns the number of bytes written.
    fn write_entry(&mut self, key: &DecoratedKey, data_offset: u64) -> Result<usize> {
        let start_len = self.buffer.len();

        // Write partition key with short length prefix (Cassandra's writeWithShortLength)
        if key.key.len() > 65535 {
            return Err(Error::Storage(format!(
                "Partition key too large for Index.db: {} bytes (max 65535)",
                key.key.len()
            )));
        }
        self.buffer
            .write_all(&(key.key.len() as u16).to_be_bytes())
            .map_err(|e| Error::Storage(format!("Failed to write key length: {}", e)))?;
        self.buffer
            .write_all(&key.key)
            .map_err(|e| Error::Storage(format!("Failed to write key bytes: {}", e)))?;

        // Write position (unsigned VInt encoded)
        encode_unsigned(data_offset, &mut self.buffer);

        // Write promoted index length (0 = no promoted index)
        encode_unsigned(0, &mut self.buffer);

        let bytes_written = self.buffer.len() - start_len;
        Ok(bytes_written)
    }

    /// Finish writing and return the Index.db bytes
    ///
    /// Returns the complete Index.db file content.
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
        // Buffer is already complete - just return it
        Ok(self.buffer)
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
        self.entry_count
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

        let info = writer.add_partition(&key, 0).unwrap();

        assert_eq!(writer.entry_count(), 1);
        assert_eq!(info.index_offset, 0); // First entry starts at offset 0
        assert_eq!(info.entry_size, 8); // 2 (key_len) + 4 (key) + 1 (pos) + 1 (promoted)
    }

    #[test]
    fn test_add_multiple_partitions() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);
        let key3 = DecoratedKey::new(300, vec![0x00, 0x00, 0x00, 0x03]);

        let info1 = writer.add_partition(&key1, 0).unwrap();
        let info2 = writer.add_partition(&key2, 150).unwrap();
        let info3 = writer.add_partition(&key3, 300).unwrap();

        assert_eq!(writer.entry_count(), 3);

        // Verify offsets are tracked correctly
        assert_eq!(info1.index_offset, 0);
        assert_eq!(info2.index_offset, info1.entry_size as u64);
        assert_eq!(
            info3.index_offset,
            (info1.entry_size + info2.entry_size) as u64
        );
    }

    #[test]
    fn test_finish_single_entry() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Verify structure:
        // [key_len: 2 bytes u16 BE] = 0x00 0x04 (4 bytes)
        // [key_bytes: 4 bytes]
        // [position: VInt, 1 byte for 0]
        // [promoted_size: VInt, 1 byte for 0]
        assert!(!bytes.is_empty());

        // Check key_len (u16 BE = 4)
        assert_eq!(bytes[0], 0x00);
        assert_eq!(bytes[1], 0x04);

        // Check raw key bytes
        assert_eq!(&bytes[2..6], &[0x00, 0x00, 0x00, 0x2A]);

        // Check position (VInt 0)
        assert_eq!(bytes[6], 0x00);

        // Check promoted index size (VInt 0)
        assert_eq!(bytes[7], 0x00);

        // Total: 2 + 4 + 1 + 1 = 8 bytes
        assert_eq!(bytes.len(), 8);
    }

    #[test]
    fn test_finish_multiple_entries() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 150).unwrap();

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 (key_len) + 4 (key) + 1 (pos=0) + 1 (promoted=0) = 8 bytes
        // Entry 2: 2 (key_len) + 4 (key) + 2 (pos=150, VInt) + 1 (promoted=0) = 9 bytes
        // Total: 17 bytes
        assert_eq!(bytes.len(), 17);

        // Check first entry key_len
        assert_eq!(&bytes[0..2], &[0x00, 0x04]);

        // Check second entry key_len at offset 8
        assert_eq!(&bytes[8..10], &[0x00, 0x04]);
    }

    #[test]
    fn test_position_encoding() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        // Test various position values to verify VInt encoding
        writer.add_partition(&key, 127).unwrap(); // 1-byte VInt

        let bytes = writer.finish().unwrap();

        // Position at byte 6 (after key_len(2) + key(4))
        // VInt(127) = 0x7F (single byte)
        assert_eq!(bytes[6], 0x7F);

        // Promoted index size at byte 7
        assert_eq!(bytes[7], 0x00);
    }

    #[test]
    fn test_position_encoding_large_offset() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        // Test large offset requiring multi-byte VInt
        writer.add_partition(&key, 12381).unwrap(); // 2-byte VInt: 0xB0 0x5D

        let bytes = writer.finish().unwrap();

        // Position at byte 6 (after key_len(2) + key(4))
        // VInt(12381) = 0xB0 0x5D (two bytes)
        assert_eq!(bytes[6], 0xB0);
        assert_eq!(bytes[7], 0x5D);

        // Promoted index size at byte 8
        assert_eq!(bytes[8], 0x00);

        // Total: 2 + 4 + 2 + 1 = 9 bytes
        assert_eq!(bytes.len(), 9);
    }

    #[test]
    fn test_hex_dump_verification() {
        let mut writer = IndexWriter::new();

        // Create a simple partition key
        let key = DecoratedKey::new(12345, vec![0x01, 0x02, 0x03, 0x04]);
        writer.add_partition(&key, 0).unwrap();

        let bytes = writer.finish().unwrap();

        // Verify key_len prefix
        assert_eq!(&bytes[0..2], &[0x00, 0x04], "Key length should be 4");

        // Verify raw key bytes
        assert_eq!(&bytes[2..6], &[0x01, 0x02, 0x03, 0x04]);

        // Total: 2 (key_len) + 4 (key) + 1 (pos) + 1 (promoted) = 8
        assert_eq!(bytes.len(), 8);
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

        // Entry 1: 2 (key_len) + 1 (key) + 1 (pos=0) + 1 (promoted=0) = 5 bytes
        // Entry 2: 2 (key_len) + 1 (key) + 1 (pos=100) + 1 (promoted=0) = 5 bytes
        // Entry 3: 2 (key_len) + 1 (key) + 2 (pos=200, VInt) + 1 (promoted=0) = 6 bytes
        // Total: 16 bytes
        assert_eq!(bytes.len(), 16);

        // Verify key_len prefixes for all entries
        assert_eq!(&bytes[0..2], &[0x00, 0x01]); // key_len=1
        assert_eq!(&bytes[5..7], &[0x00, 0x01]); // key_len=1
        assert_eq!(&bytes[10..12], &[0x00, 0x01]); // key_len=1
    }

    #[test]
    fn test_empty_index() {
        let writer = IndexWriter::new();
        let bytes = writer.finish().unwrap();

        // Empty index should produce empty byte array
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_key_bytes_stored_correctly() {
        let mut writer = IndexWriter::new();

        // Test that raw key bytes are stored (not hashed)
        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 100).unwrap();

        let bytes = writer.finish().unwrap();

        // Extract raw key bytes
        // Entry 1: key_len(2) + key(4) + pos(1) + promoted(1) = 8
        let key_bytes1 = &bytes[2..6];
        // Entry 2: starts at offset 8
        let key_bytes2 = &bytes[10..14];

        // Keys should be the raw partition key bytes
        assert_eq!(key_bytes1, &[0x00, 0x00, 0x00, 0x01]);
        assert_eq!(key_bytes2, &[0x00, 0x00, 0x00, 0x02]);
    }

    #[test]
    fn test_large_partition_key() {
        let mut writer = IndexWriter::new();

        // Test with a larger partition key (composite key scenario)
        let large_key = vec![0xFF; 100];
        let key = DecoratedKey::new(12345, large_key.clone());

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Verify key_len = 100 (u16 BE)
        assert_eq!(&bytes[0..2], &[0x00, 0x64]);

        // Verify raw key bytes stored (not hashed)
        assert_eq!(&bytes[2..102], &large_key[..]);
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

        // Entry 1: 2 + 4 + 1 + 1 = 8 bytes
        // Entry 2: 2 + 4 + 2 (VInt 250) + 1 = 9 bytes
        // Entry 3: 2 + 4 + 2 (VInt 500) + 1 = 9 bytes
        assert_eq!(bytes.len(), 26);

        // Verify key_len prefix for all entries
        assert_eq!(&bytes[0..2], &[0x00, 0x04]);  // key_len=4

        // Second entry at offset 8
        assert_eq!(&bytes[8..10], &[0x00, 0x04]); // key_len=4
    }

    #[test]
    fn test_vint_encoding_boundaries() {
        // Test VInt encoding at various boundaries to verify correct byte counts
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        // Base size: 2 (key_len) + 4 (key) = 6 bytes, + 1 (promoted) = 7 bytes + pos VInt
        // Test offset at 127 (max 1-byte VInt)
        writer.add_partition(&key, 127).unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 8); // 6 + 1 + 1

        // Test offset at 128 (min 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 128).unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 9); // 6 + 2 + 1

        // Test offset at 16383 (max 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16383).unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 9); // 6 + 2 + 1

        // Test offset at 16384 (min 3-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16384).unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 10); // 6 + 3 + 1
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
        // 2 entries: (2+1+1+1)*2 = 10
        assert_eq!(bytes.len(), 10); // Two entries, both with 1-byte key and offset=0
    }

    #[test]
    fn test_zero_offset() {
        // First partition should start at offset 0
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Position at byte 6 (after key_len(2) + key(4))
        assert_eq!(bytes[6], 0x00);
    }

    #[test]
    fn test_index_offset_tracking() {
        // Test that IndexEntryInfo provides accurate offsets for Summary.db
        let mut writer = IndexWriter::new();

        // Entry 1: 4-byte key, position=0 (1-byte VInt)
        let key1 = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);
        let info1 = writer.add_partition(&key1, 0).unwrap();

        // Entry 2: 2-byte key, position=127 (1-byte VInt)
        let key2 = DecoratedKey::new(200, vec![0x05, 0x06]);
        let info2 = writer.add_partition(&key2, 127).unwrap();

        // Entry 3: 1-byte key, position=12381 (2-byte VInt)
        let key3 = DecoratedKey::new(300, vec![0x07]);
        let info3 = writer.add_partition(&key3, 12381).unwrap();

        // Verify offsets
        assert_eq!(info1.index_offset, 0, "First entry starts at offset 0");
        assert_eq!(info1.entry_size, 8, "Entry 1: 2 + 4 + 1 + 1 = 8 bytes");

        assert_eq!(
            info2.index_offset, 8,
            "Second entry starts after first (8 bytes)"
        );
        assert_eq!(info2.entry_size, 6, "Entry 2: 2 + 2 + 1 + 1 = 6 bytes");

        assert_eq!(
            info3.index_offset, 14,
            "Third entry starts after first two (14 bytes)"
        );
        assert_eq!(
            info3.entry_size, 6,
            "Entry 3: 2 + 1 + 2 + 1 = 6 bytes (2-byte VInt)"
        );

        // Verify that offsets match actual serialized positions
        let bytes = writer.finish().unwrap();

        // Check key_len prefixes at expected offsets
        assert_eq!(
            &bytes[info1.index_offset as usize..info1.index_offset as usize + 2],
            &[0x00, 0x04],
            "Entry 1 key_len=4 at offset 0"
        );
        assert_eq!(
            &bytes[info2.index_offset as usize..info2.index_offset as usize + 2],
            &[0x00, 0x02],
            "Entry 2 key_len=2 at offset 8"
        );
        assert_eq!(
            &bytes[info3.index_offset as usize..info3.index_offset as usize + 2],
            &[0x00, 0x01],
            "Entry 3 key_len=1 at offset 14"
        );

        // Verify total size matches sum of entries
        assert_eq!(
            bytes.len(),
            (info1.entry_size + info2.entry_size + info3.entry_size),
            "Total size matches sum of entry sizes"
        );
    }

    #[test]
    fn test_streaming_write_memory_efficiency() {
        // Verify that entries are written immediately, not buffered
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x01]);
        let _info1 = writer.add_partition(&key1, 0).unwrap();

        // Buffer should contain data after first write
        assert!(
            !writer.buffer.is_empty(),
            "Buffer should contain data after first write"
        );

        let buffer_size_after_one = writer.buffer.len();

        let key2 = DecoratedKey::new(200, vec![0x02]);
        let _info2 = writer.add_partition(&key2, 100).unwrap();

        // Buffer should grow after second write
        assert!(
            writer.buffer.len() > buffer_size_after_one,
            "Buffer should grow after second write"
        );
    }

    #[test]
    fn test_variable_vint_sizes() {
        // Test that entry sizes correctly account for variable VInt encoding
        let mut writer = IndexWriter::new();

        // All keys are 1 byte, so base = 2 + 1 + 1(promoted) = 4 + pos VInt
        let key1 = DecoratedKey::new(100, vec![0x01]);
        let info1 = writer.add_partition(&key1, 0).unwrap(); // 1-byte VInt

        let key2 = DecoratedKey::new(200, vec![0x02]);
        let info2 = writer.add_partition(&key2, 127).unwrap(); // 1-byte VInt (max)

        let key3 = DecoratedKey::new(300, vec![0x03]);
        let info3 = writer.add_partition(&key3, 128).unwrap(); // 2-byte VInt (min)

        let key4 = DecoratedKey::new(400, vec![0x04]);
        let info4 = writer.add_partition(&key4, 16383).unwrap(); // 2-byte VInt (max)

        let key5 = DecoratedKey::new(500, vec![0x05]);
        let info5 = writer.add_partition(&key5, 16384).unwrap(); // 3-byte VInt (min)

        // Base = 2 (key_len) + 1 (key) + 1 (promoted) = 4
        assert_eq!(info1.entry_size, 5, "1-byte VInt: 4 + 1");
        assert_eq!(info2.entry_size, 5, "1-byte VInt: 4 + 1");
        assert_eq!(info3.entry_size, 6, "2-byte VInt: 4 + 2");
        assert_eq!(info4.entry_size, 6, "2-byte VInt: 4 + 2");
        assert_eq!(info5.entry_size, 7, "3-byte VInt: 4 + 3");
    }
}
