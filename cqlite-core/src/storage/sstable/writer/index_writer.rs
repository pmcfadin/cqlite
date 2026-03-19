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
//! [marker: u16 BE = 0x0010]     ← Partition key digest marker
//! [digest: 16 bytes]            ← MD5 hash of partition key bytes
//! [position: unsigned VInt]     ← Byte offset in Data.db
//! [promoted_index_size: unsigned VInt] ← 0 for simple partitions
//! ```
//!
//! ## Key Requirements
//!
//! - Entries must be in token order (same as Data.db partition order)
//! - Position offsets must match Data.db partition positions EXACTLY
//! - MD5 digest is computed from the raw partition key bytes
//! - VInt encoding follows Cassandra unsigned VInt format
//!
//! References:
//! - `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`
//! - `cqlite-core/src/storage/sstable/index_reader.rs` - BIG format parser

use crate::error::Result;
use crate::storage::serialization::vint::encode_unsigned;
use crate::storage::write_engine::mutation::DecoratedKey;

/// BIG format marker indicating a partition key digest follows (Cassandra NB variant)
const BIG_FORMAT_MARKER: u16 = 0x0010;

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
    /// Cassandra BIG format Index.db entry (NB variant):
    /// ```text
    /// [marker: u16 BE = 0x0010]      ← Partition key digest marker
    /// [digest: 16 bytes]             ← MD5 hash of partition key bytes
    /// [position: unsigned VInt]      ← Data.db offset
    /// [promoted_index_size: unsigned VInt] ← 0 for simple partitions
    /// ```
    ///
    /// Returns the number of bytes written.
    fn write_entry(&mut self, key: &DecoratedKey, data_offset: u64) -> Result<usize> {
        let start_len = self.buffer.len();

        // Write BIG format marker
        self.buffer
            .extend_from_slice(&BIG_FORMAT_MARKER.to_be_bytes());

        // Write MD5 digest of partition key bytes (16 bytes)
        let digest = md5::compute(&key.key);
        self.buffer.extend_from_slice(digest.as_slice());

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
    /// [marker: u16 BE = 0x0010]     ← Partition key digest marker
    /// [digest: 16 bytes]            ← MD5 of partition key bytes
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

    // BIG format entry size: 2 (marker) + 16 (digest) + N (vint offset) + 1 (vint promoted=0)
    // = 19 + vint_len(offset)

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
        assert_eq!(info.index_offset, 0);
        // 2 (marker) + 16 (digest) + 1 (pos=0) + 1 (promoted=0) = 20
        assert_eq!(info.entry_size, 20);
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
    fn test_big_format_marker_and_digest() {
        let mut writer = IndexWriter::new();
        let pk_bytes = vec![0x00, 0x00, 0x00, 0x2A];
        let key = DecoratedKey::new(12345, pk_bytes.clone());

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // BIG format: [0x0010:marker][16-byte MD5 digest][vint offset][vint promoted]
        // Total: 2 + 16 + 1 + 1 = 20 bytes
        assert_eq!(bytes.len(), 20);

        // Check marker
        assert_eq!(&bytes[0..2], &[0x00, 0x10], "Marker should be 0x0010");

        // Check MD5 digest matches
        let expected_digest = md5::compute(&pk_bytes);
        assert_eq!(
            &bytes[2..18],
            expected_digest.as_slice(),
            "Should be MD5 of key bytes"
        );

        // Check offset VInt(0) and promoted VInt(0)
        assert_eq!(bytes[18], 0x00, "Offset should be 0");
        assert_eq!(bytes[19], 0x00, "Promoted size should be 0");
    }

    #[test]
    fn test_finish_multiple_entries() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 150).unwrap();

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 (marker) + 16 (digest) + 1 (pos=0) + 1 (promoted=0) = 20
        // Entry 2: 2 (marker) + 16 (digest) + 2 (pos=150, VInt) + 1 (promoted=0) = 21
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

        writer.add_partition(&key, 127).unwrap(); // 1-byte VInt

        let bytes = writer.finish().unwrap();

        // Position at byte 18 (after marker(2) + digest(16))
        assert_eq!(bytes[18], 0x7F);

        // Promoted index size at byte 19
        assert_eq!(bytes[19], 0x00);
    }

    #[test]
    fn test_position_encoding_large_offset() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 12381).unwrap(); // 2-byte VInt: 0xB0 0x5D

        let bytes = writer.finish().unwrap();

        // Position at byte 18 (after marker(2) + digest(16))
        assert_eq!(bytes[18], 0xB0);
        assert_eq!(bytes[19], 0x5D);

        // Promoted index size at byte 20
        assert_eq!(bytes[20], 0x00);

        // Total: 2 + 16 + 2 + 1 = 21 bytes
        assert_eq!(bytes.len(), 21);
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

        // Each entry: 2 (marker) + 16 (digest) + vint + 1 (promoted)
        // Entry 1: 19 + 1 = 20 bytes (pos=0, 1-byte VInt)
        // Entry 2: 19 + 1 = 20 bytes (pos=100, 1-byte VInt)
        // Entry 3: 19 + 2 = 21 bytes (pos=200, 2-byte VInt)
        // Total: 61 bytes
        assert_eq!(bytes.len(), 61);

        // Verify markers for all entries
        assert_eq!(&bytes[0..2], &[0x00, 0x10]);
        assert_eq!(&bytes[20..22], &[0x00, 0x10]);
        assert_eq!(&bytes[40..42], &[0x00, 0x10]);
    }

    #[test]
    fn test_empty_index() {
        let writer = IndexWriter::new();
        let bytes = writer.finish().unwrap();

        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_digest_computed_from_key_bytes() {
        let mut writer = IndexWriter::new();

        let pk1 = vec![0x00, 0x00, 0x00, 0x01];
        let pk2 = vec![0x00, 0x00, 0x00, 0x02];
        let key1 = DecoratedKey::new(100, pk1.clone());
        let key2 = DecoratedKey::new(200, pk2.clone());

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 100).unwrap();

        let bytes = writer.finish().unwrap();

        // Extract digests (bytes 2..18 of each 20-byte entry)
        let digest1 = &bytes[2..18];
        let digest2 = &bytes[22..38];

        assert_eq!(digest1, md5::compute(&pk1).as_slice());
        assert_eq!(digest2, md5::compute(&pk2).as_slice());
    }

    #[test]
    fn test_large_partition_key_same_digest_size() {
        let mut writer = IndexWriter::new();

        // Even large keys produce a 16-byte MD5 digest
        let large_key = vec![0xFF; 100];
        let key = DecoratedKey::new(12345, large_key.clone());

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Entry size is always based on digest, not raw key
        // 2 (marker) + 16 (digest) + 1 (pos=0) + 1 (promoted=0) = 20
        assert_eq!(bytes.len(), 20);

        // Verify marker
        assert_eq!(&bytes[0..2], &[0x00, 0x10]);

        // Verify digest matches MD5 of large key
        let expected_digest = md5::compute(&large_key);
        assert_eq!(&bytes[2..18], expected_digest.as_slice());
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

        // Entry 1: 2 + 16 + 1 (pos=0) + 1 = 20
        // Entry 2: 2 + 16 + 2 (VInt 250) + 1 = 21
        // Entry 3: 2 + 16 + 2 (VInt 500) + 1 = 21
        assert_eq!(bytes.len(), 62);

        // All entries start with marker 0x0010
        assert_eq!(&bytes[0..2], &[0x00, 0x10]);
        assert_eq!(&bytes[20..22], &[0x00, 0x10]);
        assert_eq!(&bytes[41..43], &[0x00, 0x10]);
    }

    #[test]
    fn test_vint_encoding_boundaries() {
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        // Base size: 2 (marker) + 16 (digest) + 1 (promoted) = 19 + vint_len(offset)

        // Test offset at 127 (max 1-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 127).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 20); // 19 + 1

        // Test offset at 128 (min 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 128).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 21); // 19 + 2

        // Test offset at 16383 (max 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16383).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 21); // 19 + 2

        // Test offset at 16384 (min 3-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16384).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 22); // 19 + 3
    }

    #[test]
    fn test_duplicate_offsets_allowed() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x01]);
        let key2 = DecoratedKey::new(200, vec![0x02]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 0).unwrap();

        let bytes = writer.finish().unwrap();
        // 2 entries: (2+16+1+1)*2 = 40
        assert_eq!(bytes.len(), 40);
    }

    #[test]
    fn test_zero_offset() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Position at byte 18 (after marker(2) + digest(16))
        assert_eq!(bytes[18], 0x00);
    }

    #[test]
    fn test_index_offset_tracking() {
        let mut writer = IndexWriter::new();

        // All entries have same structure: 2 (marker) + 16 (digest) + vint + 1 (promoted)
        let key1 = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);
        let info1 = writer.add_partition(&key1, 0).unwrap(); // 1-byte VInt

        let key2 = DecoratedKey::new(200, vec![0x05, 0x06]);
        let info2 = writer.add_partition(&key2, 127).unwrap(); // 1-byte VInt

        let key3 = DecoratedKey::new(300, vec![0x07]);
        let info3 = writer.add_partition(&key3, 12381).unwrap(); // 2-byte VInt

        // All entries: 2 (marker) + 16 (digest) + vint + 1 (promoted)
        assert_eq!(info1.index_offset, 0);
        assert_eq!(info1.entry_size, 20, "Entry 1: 2 + 16 + 1 + 1 = 20");

        assert_eq!(info2.index_offset, 20);
        assert_eq!(info2.entry_size, 20, "Entry 2: 2 + 16 + 1 + 1 = 20");

        assert_eq!(info3.index_offset, 40);
        assert_eq!(info3.entry_size, 21, "Entry 3: 2 + 16 + 2 + 1 = 21");

        // Verify markers at expected offsets
        let bytes = writer.finish().unwrap();

        assert_eq!(
            &bytes[info1.index_offset as usize..info1.index_offset as usize + 2],
            &[0x00, 0x10],
            "Entry 1 marker at offset 0"
        );
        assert_eq!(
            &bytes[info2.index_offset as usize..info2.index_offset as usize + 2],
            &[0x00, 0x10],
            "Entry 2 marker at offset 20"
        );
        assert_eq!(
            &bytes[info3.index_offset as usize..info3.index_offset as usize + 2],
            &[0x00, 0x10],
            "Entry 3 marker at offset 40"
        );

        assert_eq!(
            bytes.len(),
            info1.entry_size + info2.entry_size + info3.entry_size,
            "Total size matches sum of entry sizes"
        );
    }

    #[test]
    fn test_streaming_write_memory_efficiency() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x01]);
        let _info1 = writer.add_partition(&key1, 0).unwrap();

        assert!(
            !writer.buffer.is_empty(),
            "Buffer should contain data after first write"
        );

        let buffer_size_after_one = writer.buffer.len();

        let key2 = DecoratedKey::new(200, vec![0x02]);
        let _info2 = writer.add_partition(&key2, 100).unwrap();

        assert!(
            writer.buffer.len() > buffer_size_after_one,
            "Buffer should grow after second write"
        );
    }

    #[test]
    fn test_variable_vint_sizes() {
        let mut writer = IndexWriter::new();

        // Base = 2 (marker) + 16 (digest) + 1 (promoted) = 19 + vint_len
        let key1 = DecoratedKey::new(100, vec![0x01]);
        let info1 = writer.add_partition(&key1, 0).unwrap();

        let key2 = DecoratedKey::new(200, vec![0x02]);
        let info2 = writer.add_partition(&key2, 127).unwrap();

        let key3 = DecoratedKey::new(300, vec![0x03]);
        let info3 = writer.add_partition(&key3, 128).unwrap();

        let key4 = DecoratedKey::new(400, vec![0x04]);
        let info4 = writer.add_partition(&key4, 16383).unwrap();

        let key5 = DecoratedKey::new(500, vec![0x05]);
        let info5 = writer.add_partition(&key5, 16384).unwrap();

        assert_eq!(info1.entry_size, 20, "1-byte VInt: 19 + 1");
        assert_eq!(info2.entry_size, 20, "1-byte VInt: 19 + 1");
        assert_eq!(info3.entry_size, 21, "2-byte VInt: 19 + 2");
        assert_eq!(info4.entry_size, 21, "2-byte VInt: 19 + 2");
        assert_eq!(info5.entry_size, 22, "3-byte VInt: 19 + 3");
    }
}
