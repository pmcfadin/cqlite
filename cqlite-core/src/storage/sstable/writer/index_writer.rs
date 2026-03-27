//! Index.db writer - writes partition index
//!
//! Generates the Index.db component with BIG format (legacy row index).
//! Maps partition keys to Data.db file offsets for fast partition lookup.
//!
//! # BIG Index Format
//!
//! Index.db stores partition-to-Data.db offset mappings for efficient partition lookup.
//!
//! ## Entry Structure (BIG format, NB variant)
//!
//! Each entry stores a partition's location:
//! ```text
//! [key_len: u16 BE]                ← Length of partition key bytes
//! [key_bytes: key_len bytes]       ← Raw partition key bytes
//! [position: unsigned VInt]        ← Byte offset in Data.db
//! [promoted_index_size: unsigned VInt] ← 0 for simple partitions
//! ```
//!
//! ## Key Requirements
//!
//! - Entries must be in token order (same as Data.db partition order)
//! - Position offsets must match Data.db partition positions EXACTLY
//! - Key bytes are the raw serialized partition key (same as in Data.db)
//! - VInt encoding follows Cassandra unsigned VInt format
//!
//! References:
//! - `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`
//! - `cqlite-core/src/storage/sstable/index_reader.rs` - BIG format parser

use crate::error::Result;
use crate::storage::serialization::vint::encode_unsigned;
use crate::storage::write_engine::mutation::DecoratedKey;

/// Index.db component writer
///
/// Writes partition index entries in BIG format (NB variant) for Cassandra 5.0 compatibility.
/// Each entry maps a partition key to a Data.db file offset.
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
    /// Partitions MUST be added in token order (caller responsibility).
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
        let index_offset = self.buffer.len() as u64;
        let entry_size = self.write_entry(key, data_offset)?;
        self.entry_count += 1;

        Ok(IndexEntryInfo {
            index_offset,
            entry_size,
        })
    }

    /// Write a single index entry to the buffer
    ///
    /// Cassandra BIG format Index.db entry (NB variant):
    /// ```text
    /// [key_len: u16 BE]                   ← Length of raw partition key
    /// [key_bytes: key_len bytes]          ← Raw partition key bytes
    /// [position: unsigned VInt]           ← Data.db offset
    /// [promoted_index_size: unsigned VInt] ← 0 for simple partitions
    /// ```
    fn write_entry(&mut self, key: &DecoratedKey, data_offset: u64) -> Result<usize> {
        let start_len = self.buffer.len();

        // Write key length (u16 big-endian)
        let key_len = key.key.len() as u16;
        self.buffer.extend_from_slice(&key_len.to_be_bytes());

        // Write raw partition key bytes
        self.buffer.extend_from_slice(&key.key);

        // Write position (unsigned VInt encoded)
        encode_unsigned(data_offset, &mut self.buffer);

        // Write promoted index length (0 = no promoted index)
        encode_unsigned(0, &mut self.buffer);

        let bytes_written = self.buffer.len() - start_len;
        Ok(bytes_written)
    }

    /// Finish writing and return the Index.db bytes
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
    fn test_add_single_partition_int_key() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]); // int = 42

        let info = writer.add_partition(&key, 0).unwrap();

        assert_eq!(writer.entry_count(), 1);
        assert_eq!(info.index_offset, 0);
        // 2 (key_len) + 4 (key bytes) + 1 (pos=0) + 1 (promoted=0) = 8
        assert_eq!(info.entry_size, 8);
    }

    #[test]
    fn test_add_single_partition_uuid_key() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0xBB; 16]); // UUID-sized key

        let info = writer.add_partition(&key, 0).unwrap();

        assert_eq!(writer.entry_count(), 1);
        // 2 (key_len) + 16 (key bytes) + 1 (pos=0) + 1 (promoted=0) = 20
        assert_eq!(info.entry_size, 20);
    }

    #[test]
    fn test_raw_key_bytes_written() {
        let mut writer = IndexWriter::new();
        let pk_bytes = vec![0x00, 0x00, 0x00, 0x2A];
        let key = DecoratedKey::new(12345, pk_bytes.clone());

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Format: [key_len:u16 BE][key_bytes][pos VInt][promoted VInt]
        // key_len = 4 -> 0x0004
        assert_eq!(&bytes[0..2], &[0x00, 0x04], "Key length should be 4");

        // Raw key bytes (not MD5!)
        assert_eq!(&bytes[2..6], &pk_bytes, "Should be raw key bytes");

        // Offset VInt(0) and promoted VInt(0)
        assert_eq!(bytes[6], 0x00, "Offset should be 0");
        assert_eq!(bytes[7], 0x00, "Promoted size should be 0");
    }

    #[test]
    fn test_uuid_key_raw_bytes() {
        let mut writer = IndexWriter::new();
        let pk_bytes = vec![
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let key = DecoratedKey::new(12345, pk_bytes.clone());

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // key_len = 16 -> 0x0010
        assert_eq!(&bytes[0..2], &[0x00, 0x10], "Key length should be 16");

        // Raw UUID bytes
        assert_eq!(&bytes[2..18], &pk_bytes, "Should be raw UUID bytes");
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

        assert_eq!(info1.index_offset, 0);
        assert_eq!(info2.index_offset, info1.entry_size as u64);
        assert_eq!(
            info3.index_offset,
            (info1.entry_size + info2.entry_size) as u64
        );
    }

    #[test]
    fn test_finish_multiple_entries() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 150).unwrap();

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 (len) + 4 (key) + 1 (pos=0) + 1 (promoted=0) = 8
        // Entry 2: 2 (len) + 4 (key) + 2 (pos=150, VInt) + 1 (promoted=0) = 9
        assert_eq!(bytes.len(), 17);

        // Check first entry key length
        assert_eq!(&bytes[0..2], &[0x00, 0x04]);

        // Check second entry key length at offset 8
        assert_eq!(&bytes[8..10], &[0x00, 0x04]);
    }

    #[test]
    fn test_position_encoding() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 127).unwrap(); // 1-byte VInt

        let bytes = writer.finish().unwrap();

        // Position at byte 6 (after key_len(2) + key(4))
        assert_eq!(bytes[6], 0x7F);
        assert_eq!(bytes[7], 0x00); // promoted
    }

    #[test]
    fn test_position_encoding_large_offset() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 12381).unwrap(); // 2-byte VInt: 0xB0 0x5D

        let bytes = writer.finish().unwrap();

        // Position at byte 6 (after key_len(2) + key(4))
        assert_eq!(bytes[6], 0xB0);
        assert_eq!(bytes[7], 0x5D);
        assert_eq!(bytes[8], 0x00); // promoted

        // Total: 2 + 4 + 2 + 1 = 9
        assert_eq!(bytes.len(), 9);
    }

    #[test]
    fn test_variable_key_sizes() {
        // 1-byte key
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0x42]);
        let info = writer.add_partition(&key, 0).unwrap();
        assert_eq!(info.entry_size, 5); // 2 + 1 + 1 + 1

        // 4-byte key (int)
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x2A]);
        let info = writer.add_partition(&key, 0).unwrap();
        assert_eq!(info.entry_size, 8); // 2 + 4 + 1 + 1

        // 8-byte key (bigint)
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0; 8]);
        let info = writer.add_partition(&key, 0).unwrap();
        assert_eq!(info.entry_size, 12); // 2 + 8 + 1 + 1

        // 16-byte key (uuid)
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0; 16]);
        let info = writer.add_partition(&key, 0).unwrap();
        assert_eq!(info.entry_size, 20); // 2 + 16 + 1 + 1
    }

    #[test]
    fn test_empty_index() {
        let writer = IndexWriter::new();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_token_order_preservation() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x01]);
        let key2 = DecoratedKey::new(200, vec![0x02]);
        let key3 = DecoratedKey::new(300, vec![0x03]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 100).unwrap();
        writer.add_partition(&key3, 200).unwrap();

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 (len) + 1 (key) + 1 (pos=0) + 1 (promoted=0) = 5
        // Entry 2: 2 (len) + 1 (key) + 1 (pos=100) + 1 (promoted=0) = 5
        // Entry 3: 2 (len) + 1 (key) + 2 (pos=200, 2-byte VInt) + 1 (promoted=0) = 6
        assert_eq!(bytes.len(), 16);

        // Check key length prefixes
        assert_eq!(&bytes[0..2], &[0x00, 0x01]);
        assert_eq!(&bytes[5..7], &[0x00, 0x01]);
        assert_eq!(&bytes[10..12], &[0x00, 0x01]);
    }

    #[test]
    fn test_vint_encoding_boundaries() {
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        // Base size: 2 (key_len) + 4 (key) + 1 (promoted) = 7 + vint_len(offset)

        // Test offset at 127 (max 1-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 127).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 8); // 7 + 1

        // Test offset at 128 (min 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 128).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 9); // 7 + 2

        // Test offset at 16383 (max 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16383).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 9); // 7 + 2

        // Test offset at 16384 (min 3-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16384).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 10); // 7 + 3
    }

    #[test]
    fn test_index_offset_tracking() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);
        let info1 = writer.add_partition(&key1, 0).unwrap(); // 1-byte VInt

        let key2 = DecoratedKey::new(200, vec![0x05, 0x06]);
        let info2 = writer.add_partition(&key2, 127).unwrap(); // 1-byte VInt

        let key3 = DecoratedKey::new(300, vec![0x07]);
        let info3 = writer.add_partition(&key3, 12381).unwrap(); // 2-byte VInt

        assert_eq!(info1.index_offset, 0);
        assert_eq!(info1.entry_size, 8, "Entry 1: 2 + 4 + 1 + 1 = 8");

        assert_eq!(info2.index_offset, 8);
        assert_eq!(info2.entry_size, 6, "Entry 2: 2 + 2 + 1 + 1 = 6");

        assert_eq!(info3.index_offset, 14);
        assert_eq!(info3.entry_size, 6, "Entry 3: 2 + 1 + 2 + 1 = 6");

        let bytes = writer.finish().unwrap();
        assert_eq!(
            bytes.len(),
            info1.entry_size + info2.entry_size + info3.entry_size,
            "Total size matches sum of entry sizes"
        );
    }

    #[test]
    fn test_realistic_scenario() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(-5000000000, vec![0x00, 0x00, 0x03, 0xE9]);
        writer.add_partition(&key1, 0).unwrap();

        let key2 = DecoratedKey::new(-2000000000, vec![0x00, 0x00, 0x03, 0xEA]);
        writer.add_partition(&key2, 250).unwrap();

        let key3 = DecoratedKey::new(3000000000, vec![0x00, 0x00, 0x03, 0xEB]);
        writer.add_partition(&key3, 500).unwrap();

        assert_eq!(writer.entry_count(), 3);

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 + 4 + 1 (pos=0) + 1 = 8
        // Entry 2: 2 + 4 + 2 (VInt 250) + 1 = 9
        // Entry 3: 2 + 4 + 2 (VInt 500) + 1 = 9
        assert_eq!(bytes.len(), 26);
    }
}
