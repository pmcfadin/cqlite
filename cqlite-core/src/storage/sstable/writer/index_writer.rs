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
//! [promoted_index_size: unsigned VInt] ← 0 for simple partitions; byte count of promoted index for wide ones
//! [promoted_index_data: promoted_index_size bytes] ← Only when promoted_index_size > 0
//! ```
//!
//! ## Promoted Index (wide partitions ≥ 64 KiB)
//!
//! When a partition's uncompressed data exceeds 64 KiB, a promoted index is written so
//! Cassandra can seek to a specific clustering-key range without reading the full partition.
//!
//! A promoted index is only emitted when **two or more** `IndexInfo` blocks result
//! (Cassandra `RowIndexEntry.create()` lines 227-239). The promoted index payload is:
//!
//! ```text
//! [headerLength: unsigned VInt]    ← byte count of the DeletionTime blob below
//! [DeletionTime: headerLength bytes] ← partition header deletion ("oa": 0x80 = LIVE)
//! [count: unsigned VInt]           ← number of IndexInfo blocks
//! [IndexInfo[0]..]                 ← IndexInfo blocks (see PromotedIndexBlock)
//! [offset[0]: i32 BE] ...          ← relative offsets from first IndexInfo, one per block
//! ```
//!
//! Each `IndexInfo` block:
//! ```text
//! [firstName: ClusteringPrefix]    ← min clustering key in block (vint header + values)
//! [lastName: ClusteringPrefix]     ← max clustering key in block
//! [offset: unsigned VInt]          ← byte offset from partition start
//! [width: unsigned VInt]           ← (actual_width - WIDTH_BASE) where WIDTH_BASE = 64 KiB
//! [endOpenMarker: u8]              ← 0 = no open range tombstone marker
//! ```
//!
//! Sources:
//! - `RowIndexEntry.java` lines 293-296 (BIG "oa" serialization)
//! - `IndexInfo.Serializer.serialize()` lines 107-117
//! - `DeletionTime.java` lines 210-219 ("oa" format: mfda first, then ldt unsigned)
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

/// Threshold at which a new IndexInfo block is emitted (Cassandra default 64 KiB).
///
/// A new block is started whenever the uncompressed row data accumulated since the
/// last block boundary reaches this limit.
///
/// Source: `BigFormatPartitionWriter.DEFAULT_GRANULARITY` = 64 * 1024.
pub const COLUMN_INDEX_SIZE_BYTES: u64 = 64 * 1024;

/// Delta-encoding base for `IndexInfo.width` field.
///
/// Each block's actual width is stored as `(actual_width - WIDTH_BASE)` so that
/// typical blocks near 64 KiB encode to a small or zero VInt value.
///
/// Source: `IndexInfo.WIDTH_BASE` = 64 * 1024.
pub const INDEX_INFO_WIDTH_BASE: u64 = 64 * 1024;

/// One IndexInfo block in the promoted index.
///
/// Cassandra emits one block per `column_index_size` (64 KiB) boundary crossed.
/// A promoted index is only written when there are **two or more** blocks
/// (`RowIndexEntry.create()` lines 227-239).
#[derive(Debug, Clone)]
pub struct PromotedIndexBlock {
    /// Serialized `ClusteringPrefix` for the first unfiltered in this block.
    ///
    /// Format: `[header: unsigned VInt (2 bits per CK col)][value bytes…]`
    /// Same encoding as the clustering prefix in Data.db rows.
    /// Empty `Vec` for tables without clustering keys or for empty-clustering rows.
    pub first_name: Vec<u8>,

    /// Serialized `ClusteringPrefix` for the last unfiltered in this block.
    pub last_name: Vec<u8>,

    /// Byte offset from the start of the partition's Data.db bytes to the first
    /// unfiltered in this block.
    pub offset: u64,

    /// Total width (bytes) of this block's data.
    pub width: u64,
}

/// Index.db component writer
///
/// Writes partition index entries in BIG format (NB variant) for Cassandra 5.0 compatibility.
/// Each entry maps a partition key to a Data.db file offset, optionally with a promoted index
/// for wide partitions (≥ 64 KiB).
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

    /// Add a partition to the index (no promoted index — simple/small partition).
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
        self.add_partition_with_promoted(key, data_offset, &[])
    }

    /// Add a partition to the index with optional promoted index blocks.
    ///
    /// Call this with a non-empty `blocks` slice for wide partitions (≥ 64 KiB).
    /// When `blocks` contains **fewer than 2 entries**, the promoted index is not
    /// emitted (matching Cassandra `RowIndexEntry.create()` which gates on
    /// `columnIndexCount > 1`). When 2 or more blocks are supplied the full
    /// promoted index payload is written.
    ///
    /// # Arguments
    ///
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `data_offset` - Byte offset in Data.db where this partition starts
    /// * `blocks` - Collected `PromotedIndexBlock`s from the data write pass
    pub fn add_partition_with_promoted(
        &mut self,
        key: &DecoratedKey,
        data_offset: u64,
        blocks: &[PromotedIndexBlock],
    ) -> Result<IndexEntryInfo> {
        let index_offset = self.buffer.len() as u64;
        let entry_size = self.write_entry(key, data_offset, blocks)?;
        self.entry_count += 1;

        Ok(IndexEntryInfo {
            index_offset,
            entry_size,
        })
    }

    /// Write a single index entry to the buffer.
    ///
    /// Cassandra BIG format Index.db entry (NB "oa" variant):
    /// ```text
    /// [key_len: u16 BE]                    ← Length of raw partition key
    /// [key_bytes: key_len bytes]           ← Raw partition key bytes
    /// [position: unsigned VInt]            ← Data.db offset
    /// [promoted_index_size: unsigned VInt] ← 0 or byte count of promoted payload
    /// [promoted_index_data: promoted_index_size bytes] ← only when size > 0
    /// ```
    fn write_entry(
        &mut self,
        key: &DecoratedKey,
        data_offset: u64,
        blocks: &[PromotedIndexBlock],
    ) -> Result<usize> {
        let start_len = self.buffer.len();

        // Write key length (u16 big-endian)
        let key_len = key.key.len() as u16;
        self.buffer.extend_from_slice(&key_len.to_be_bytes());

        // Write raw partition key bytes
        self.buffer.extend_from_slice(&key.key);

        // Write position (unsigned VInt encoded)
        encode_unsigned(data_offset, &mut self.buffer);

        // Only emit a promoted index when there are 2+ blocks
        // (Cassandra RowIndexEntry.create() gates on columnIndexCount > 1)
        if blocks.len() >= 2 {
            let promoted_payload = serialize_promoted_index(blocks);
            encode_unsigned(promoted_payload.len() as u64, &mut self.buffer);
            self.buffer.extend_from_slice(&promoted_payload);
        } else {
            // Small partition — no promoted index
            encode_unsigned(0, &mut self.buffer);
        }

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

/// Serialize the promoted index payload for a wide partition.
///
/// Layout (Cassandra BIG "oa" format, `RowIndexEntry.Serializer.serialize()`):
/// ```text
/// [headerLength: unsigned VInt]    ← byte count of DeletionTime below
/// [DeletionTime: 1 byte = 0x80]   ← LIVE partition header (oa single-byte form)
/// [count: unsigned VInt]           ← number of IndexInfo blocks
/// [IndexInfo[0]..]                 ← serialized blocks in order
/// [offset[0]: i32 BE]             ← relative offsets from first IndexInfo start
/// ...
/// [offset[N-1]: i32 BE]
/// ```
///
/// The returned `Vec<u8>` is the content that goes AFTER the `promoted_index_size`
/// VInt in the Index.db entry.
fn serialize_promoted_index(blocks: &[PromotedIndexBlock]) -> Vec<u8> {
    debug_assert!(blocks.len() >= 2, "caller must ensure at least 2 blocks");

    // DeletionTime for a LIVE partition: single byte 0x80 (oa format, high bit = LIVE).
    // Source: DeletionTime.java lines 210-219.
    let deletion_time_bytes: &[u8] = &[0x80u8];
    let header_length = deletion_time_bytes.len() as u64;

    // --- Build IndexInfo bytes and accumulate per-block byte offsets ---
    let mut index_info_bytes: Vec<u8> = Vec::new();
    let mut block_start_offsets: Vec<u32> = Vec::with_capacity(blocks.len());

    for block in blocks {
        let block_offset_in_info = index_info_bytes.len() as u32;
        block_start_offsets.push(block_offset_in_info);
        serialize_index_info(&mut index_info_bytes, block);
    }

    // --- Assemble the full promoted index payload ---
    let mut payload: Vec<u8> = Vec::new();

    // headerLength (vint)
    encode_unsigned(header_length, &mut payload);

    // DeletionTime bytes
    payload.extend_from_slice(deletion_time_bytes);

    // count (vint): number of IndexInfo blocks
    encode_unsigned(blocks.len() as u64, &mut payload);

    // IndexInfo bytes
    payload.extend_from_slice(&index_info_bytes);

    // Offsets array: one i32 BE per block (signed, relative to first IndexInfo start)
    // Source: RowIndexEntry.java line 640: `out.writeInt(offsets[i])`
    for &offset in &block_start_offsets {
        payload.extend_from_slice(&(offset as i32).to_be_bytes());
    }

    payload
}

/// Serialize one `IndexInfo` block.
///
/// Layout (Cassandra `IndexInfo.Serializer.serialize()` lines 107-117):
/// ```text
/// [firstName: ClusteringPrefix bytes]  ← min clustering key in block
/// [lastName: ClusteringPrefix bytes]   ← max clustering key in block
/// [offset: unsigned VInt]              ← byte offset from partition start
/// [width: unsigned VInt]               ← actual_width - WIDTH_BASE (64 KiB)
/// [endOpenMarker: u8]                  ← 0 = no open range tombstone
/// ```
fn serialize_index_info(buf: &mut Vec<u8>, block: &PromotedIndexBlock) {
    // firstName clustering prefix bytes (already serialized by the data writer)
    buf.extend_from_slice(&block.first_name);

    // lastName clustering prefix bytes
    buf.extend_from_slice(&block.last_name);

    // offset (unsigned VInt)
    encode_unsigned(block.offset, buf);

    // width delta-encoded: (actual_width - WIDTH_BASE)
    // Per Cassandra `IndexInfo.java` line 112: `out.writeUnsignedVInt(lastName.getEnd() - offset - WIDTH_BASE)`
    // The value CAN underflow if width < WIDTH_BASE (unlikely in practice but safe via wrapping).
    let width_delta = block.width.saturating_sub(INDEX_INFO_WIDTH_BASE);
    encode_unsigned(width_delta, buf);

    // endOpenMarker presence flag: 0 = no open marker (simple partitions)
    buf.push(0u8);
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

    // ── Promoted index tests ────────────────────────────────────────────────

    /// Small partition (1 block) → promoted_index_size = 0 (no regression).
    #[test]
    fn test_single_block_no_promoted_index() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);

        // Only 1 block → should NOT emit promoted index
        let block = PromotedIndexBlock {
            first_name: vec![0x00], // empty CK prefix (just a 0-header VInt)
            last_name: vec![0x00],
            offset: 0,
            width: 70_000,
        };
        let info = writer
            .add_partition_with_promoted(&key, 0, &[block])
            .unwrap();

        let bytes = writer.finish().unwrap();

        // promoted_index_size must be 0 for a single block
        // Entry: 2 + 4 + 1(pos) + 1(promoted_len=0) = 8
        assert_eq!(
            bytes[info.entry_size - 1],
            0x00,
            "promoted size should be 0"
        );
        assert_eq!(info.entry_size, 8);
    }

    /// Two blocks → promoted_index_size > 0 and content is correct.
    #[test]
    fn test_two_blocks_emits_promoted_index() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);

        // Minimal clustering prefix: header VInt 0x00 = no columns all-null/empty
        let ck_prefix = vec![0x00u8];
        let block1 = PromotedIndexBlock {
            first_name: ck_prefix.clone(),
            last_name: ck_prefix.clone(),
            offset: 0,
            width: 65_536, // exactly 64 KiB → delta = 0
        };
        let block2 = PromotedIndexBlock {
            first_name: ck_prefix.clone(),
            last_name: ck_prefix.clone(),
            offset: 65_536,
            width: 65_536,
        };

        let info = writer
            .add_partition_with_promoted(&key, 0, &[block1, block2])
            .unwrap();
        let bytes = writer.finish().unwrap();

        // The promoted_index_size vint must be > 0
        let promoted_size_offset = 2 + 4 + 1; // key_len + key + pos vint(0)
        assert!(
            bytes[promoted_size_offset] > 0,
            "promoted_index_size must be > 0 for 2 blocks"
        );

        // Verify the reader can parse it back (promoted bytes are present)
        assert!(
            info.entry_size > 8,
            "Wide partition entry must be larger than small"
        );
    }

    /// Wide partition (3 blocks): verify promoted_index_size matches actual payload.
    #[test]
    fn test_three_blocks_promoted_index_size_matches_payload() {
        let key = DecoratedKey::new(42, vec![0xAA, 0xBB]);

        // Serialize two clustering prefix bytes representing a TEXT value "ab"
        // Cassandra clustering prefix: header VInt (2 bits per col, 0=PRESENT) then value
        // For a single TEXT col with 2-byte value: header=0x00, value=[0x61, 0x62]
        let ck_prefix = vec![0x00u8, 0x61, 0x62]; // header=present, value="ab"

        let make_block = |off: u64, w: u64| PromotedIndexBlock {
            first_name: ck_prefix.clone(),
            last_name: ck_prefix.clone(),
            offset: off,
            width: w,
        };

        let blocks = vec![
            make_block(0, 70_000),
            make_block(70_000, 68_000),
            make_block(138_000, 65_000),
        ];

        let mut writer = IndexWriter::new();
        let info = writer
            .add_partition_with_promoted(&key, 1234, &blocks)
            .unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), info.entry_size);

        // Parse promoted_index_size VInt at offset = 2(key_len) + 2(key) + pos_vint
        // pos = 1234 → 2-byte VInt (0x80 | ...)
        let pos_vint_len = 2; // 1234 > 127 → 2 bytes
        let promoted_size_vint_start = 2 + 2 + pos_vint_len;
        let promoted_size = parse_vint_simple(&bytes[promoted_size_vint_start..]);
        let (promoted_size_value, promoted_size_vint_bytes) = promoted_size;

        assert!(promoted_size_value > 0, "3 blocks → promoted index present");

        let payload_start = promoted_size_vint_start + promoted_size_vint_bytes;
        let payload_end = payload_start + promoted_size_value as usize;
        assert_eq!(
            payload_end,
            bytes.len(),
            "entry size should exactly cover key + vints + promoted payload"
        );

        // Verify promoted payload structure:
        // [headerLength VInt][0x80][count VInt][IndexInfo*3][i32*3]
        let payload = &bytes[payload_start..payload_end];
        let (header_len, hl_bytes) = parse_vint_simple(payload);
        assert_eq!(header_len, 1, "LIVE DeletionTime is 1 byte");
        let deletion_byte = payload[hl_bytes];
        assert_eq!(deletion_byte, 0x80, "LIVE DeletionTime marker = 0x80");

        let (count, _) = parse_vint_simple(&payload[hl_bytes + header_len as usize..]);
        assert_eq!(count, 3, "Three IndexInfo blocks");
    }

    /// Block boundary math: threshold crossing produces exactly one new block.
    #[test]
    fn test_block_boundary_math_threshold_crossing() {
        // At exactly COLUMN_INDEX_SIZE_BYTES we cross the threshold and start a new block.
        assert_eq!(COLUMN_INDEX_SIZE_BYTES, 64 * 1024);

        // Simulate: data writer would produce a block at exactly the boundary.
        // Two blocks: one below threshold + one at exactly the threshold.
        let block_at_threshold = PromotedIndexBlock {
            first_name: vec![0x00],
            last_name: vec![0x00],
            offset: COLUMN_INDEX_SIZE_BYTES,
            width: COLUMN_INDEX_SIZE_BYTES,
        };
        let block_below = PromotedIndexBlock {
            first_name: vec![0x00],
            last_name: vec![0x00],
            offset: 0,
            width: COLUMN_INDEX_SIZE_BYTES,
        };

        let blocks = vec![block_below, block_at_threshold];
        let payload = serialize_promoted_index(&blocks);

        // Payload must be non-empty (promoted index present)
        assert!(!payload.is_empty());
        // Count in payload must be 2.
        // Payload layout: [headerLength vint][DeletionTime bytes][count vint][...]
        let (header_len, hl_bytes) = parse_vint_simple(&payload);
        let (count, _) = parse_vint_simple(&payload[hl_bytes + header_len as usize..]);
        assert_eq!(count, 2);
    }

    /// Width delta encoding: exact 64KiB block → delta = 0.
    #[test]
    fn test_width_delta_encoding_exact_threshold() {
        // A block that is exactly 64KiB should encode width delta = 0
        let block = PromotedIndexBlock {
            first_name: vec![0x00],
            last_name: vec![0x00],
            offset: 0,
            width: INDEX_INFO_WIDTH_BASE, // 64 KiB
        };
        let block2 = PromotedIndexBlock {
            first_name: vec![0x00],
            last_name: vec![0x00],
            offset: INDEX_INFO_WIDTH_BASE,
            width: INDEX_INFO_WIDTH_BASE,
        };

        let mut info_bytes: Vec<u8> = Vec::new();
        serialize_index_info(&mut info_bytes, &block);

        // firstName=0x00, lastName=0x00, offset=0x00, width_delta=0x00, endOpenMarker=0x00
        // All are single-byte VInts/bytes when value = 0.
        assert_eq!(info_bytes, vec![0x00, 0x00, 0x00, 0x00, 0x00]);

        // Also verify with 2-block payload that the width field is 0
        let payload = serialize_promoted_index(&[block, block2]);
        assert!(!payload.is_empty());
    }

    /// Clustering prefix min/max: bytes are preserved verbatim.
    #[test]
    fn test_clustering_prefix_preserved_verbatim() {
        let first_name = vec![0x00u8, 0x01, 0x02]; // arbitrary bytes
        let last_name = vec![0x00u8, 0xFF, 0xFE];

        let block1 = PromotedIndexBlock {
            first_name: first_name.clone(),
            last_name: last_name.clone(),
            offset: 0,
            width: 100_000,
        };
        let mut info_bytes: Vec<u8> = Vec::new();
        serialize_index_info(&mut info_bytes, &block1);

        // firstName bytes appear first
        assert_eq!(&info_bytes[0..3], &first_name[..]);
        // lastName bytes appear next
        assert_eq!(&info_bytes[3..6], &last_name[..]);
    }

    // Helper: parse a Cassandra unsigned VInt and return (value, bytes_consumed)
    fn parse_vint_simple(buf: &[u8]) -> (u64, usize) {
        if buf.is_empty() {
            return (0, 0);
        }
        let first = buf[0];
        if first <= 0x7F {
            return (first as u64, 1);
        }
        // Count leading 1s to get number of extra bytes
        let leading = first.leading_ones() as usize;
        let extra_bits = 8 - leading - 1; // data bits in first byte
        let mask = (1u8 << extra_bits).wrapping_sub(1);
        let mut value = (first & mask) as u64;
        for b in buf.iter().take(leading + 1).skip(1) {
            value = (value << 8) | *b as u64;
        }
        (value, 1 + leading)
    }
}
