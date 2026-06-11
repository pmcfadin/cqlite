//! Summary.db writer - writes sampled index entries
//!
//! Generates the Summary.db component by sampling Index.db entries.
//! Used for efficient partition key range scanning without reading full index.
//!
//! Critical requirements:
//! - Little-endian offsets (ONLY LE component in SSTable!)
//! - Sampling every N entries (default: 128)
//! - First and last keys always included
//!
//! ## Summary.db Format
//!
//! ```text
//! +------------------------+
//! | Header (24 bytes)      |
//! +------------------------+
//! | Offset Table (LE u32[])| <- Little-endian!
//! +------------------------+
//! | Entry Data             |
//! |   key + position (BE)  |
//! +------------------------+
//! | First Key (serialized) |
//! +------------------------+
//! | Last Key (serialized)  |
//! +------------------------+
//! ```
//!
//! ## Header Format (24 bytes, big-endian)
//!
//! ```c
//! struct summary_header {
//!     be32 min_index_interval;      // Minimum partitions between entries (usually 128)
//!     be32 entries_count;           // Number of sampled entries
//!     be64 summary_entries_size;    // Size of offset table + entry data
//!     be32 sampling_level;          // Downsampling level (1–128). For a freshly written
//!                                   // SSTable this is always BASE_SAMPLING_LEVEL (128).
//!                                   // It only decreases when Cassandra downsamples during
//!                                   // compaction. It does NOT equal min_index_interval.
//!                                   // Source: IndexSummary.java:88-94, 226-229 (Cassandra 5.0.8)
//!     be32 size_at_full_sampling;   // Estimated entry count if sampled at BASE_SAMPLING_LEVEL.
//!                                   // = total_partition_count / min_index_interval.
//!                                   // Source: IndexSummary.java:235-237 (getMaxNumberOfEntries())
//! };
//! ```
//!
//! ## Entry Format
//!
//! Entries have no length prefix. Key boundaries are determined by offset differences.
//!
//! ```c
//! struct summary_entry {
//!     byte key[];        // Variable length - no prefix!
//!     be64 position;     // Position in Index.db file (big-endian)
//! };
//! ```
//!
//! ## Serialized Keys (File End)
//!
//! ```c
//! struct serialized_key {
//!     be32 size;        // Big-endian length
//!     byte key[size];
//! };
//! ```
//!
//! References:
//! - `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`
//! - `cqlite-core/src/storage/sstable/summary_reader.rs` - Format parser

use crate::error::Result;
use crate::storage::write_engine::mutation::DecoratedKey;

/// `sampling_level` written for a freshly constructed SSTable.
///
/// Cassandra stores a downsampling level between 1 and `BASE_SAMPLING_LEVEL` in the
/// Summary.db header.  For a new SSTable that has never been downsampled the level is
/// always `BASE_SAMPLING_LEVEL` (128).  It is **independent of `min_index_interval`**.
///
/// Source: `IndexSummary.java:88-94` (Cassandra 5.0.8, `org.apache.cassandra.io.sstable.indexsummary`).
pub const BASE_SAMPLING_LEVEL: u32 = 128;

/// Summary.db component writer
///
/// Writes sampled index entries for efficient partition lookup without scanning
/// the entire Index.db. Implements Cassandra 5.0 Summary.db format with proper
/// sampling and little-endian offset encoding.
///
/// # Sampling Strategy
///
/// Summary.db samples every Nth entry from Index.db where N = `min_index_interval`.
/// This trades memory for I/O efficiency:
/// - Smaller interval = more memory, faster lookups
/// - Larger interval = less memory, more I/O during lookups
///
/// Cassandra default: 128 entries between samples
///
/// # Example
///
/// ```
/// use cqlite_core::storage::sstable::writer::SummaryWriter;
/// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
///
/// let mut writer = SummaryWriter::new(128);
///
/// // Sample entries from Index.db
/// let key1 = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);
/// writer.add_entry(&key1, 0).unwrap();
///
/// let key2 = DecoratedKey::new(200, vec![0x05, 0x06, 0x07, 0x08]);
/// writer.add_entry(&key2, 1024).unwrap();
///
/// // Finalize to Summary.db bytes
/// let bytes = writer.finish().unwrap();
/// ```
#[derive(Debug)]
pub struct SummaryWriter {
    /// Minimum index interval (sampling rate)
    min_index_interval: u32,
    /// Total number of partitions seen (used for size_at_full_sampling calculation).
    ///
    /// `size_at_full_sampling` = `total_partition_count / min_index_interval`.
    /// Source: `IndexSummary.java:235-237` (`getMaxNumberOfEntries()`).
    total_partition_count: u32,
    /// Sampled entries (partition key + Index.db position)
    entries: Vec<SummaryEntry>,
    /// First partition key (always included)
    first_key: Option<Vec<u8>>,
    /// Last partition key (always included)
    last_key: Option<Vec<u8>>,
}

/// Internal representation of a summary entry
#[derive(Debug, Clone)]
struct SummaryEntry {
    /// Partition key bytes
    key: Vec<u8>,
    /// Byte offset in Index.db file
    index_position: u64,
}

impl SummaryWriter {
    /// Create a new Summary.db writer
    ///
    /// # Arguments
    ///
    /// * `min_index_interval` - Sampling rate (default: 128). Every Nth entry
    ///   from Index.db will be sampled. Smaller values = more memory, faster lookups.
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::SummaryWriter;
    ///
    /// let writer = SummaryWriter::new(128);
    /// assert_eq!(writer.entry_count(), 0);
    /// ```
    pub fn new(min_index_interval: u32) -> Self {
        Self {
            min_index_interval,
            total_partition_count: 0,
            entries: Vec::new(),
            first_key: None,
            last_key: None,
        }
    }

    /// Record that a partition was seen (called for EVERY partition, not just sampled ones).
    ///
    /// This method tracks:
    /// - `first_key` and `last_key` for the SSTable boundary metadata at the end of
    ///   Summary.db.  These must cover the **entire** SSTable, not just sampled
    ///   partitions.  Cassandra uses them for SSTable range queries: if they only
    ///   cover the first sampled partition, all other partitions become invisible to
    ///   range scans.  (Issue #666 root-cause investigation.)
    /// - `total_partition_count` for `size_at_full_sampling` computation.
    ///
    /// Call this method for every partition written to the SSTable, before calling
    /// `add_entry` (which is only called at sampling boundaries).
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::SummaryWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = SummaryWriter::new(128);
    ///
    /// // Note every partition (called for all partitions)
    /// let k = DecoratedKey::new(1, vec![0x01]);
    /// writer.note_partition(&k);
    ///
    /// // Add sampled entry (called every min_index_interval partitions)
    /// writer.add_entry(&k, 0).unwrap();
    /// ```
    pub fn note_partition(&mut self, key: &DecoratedKey) {
        let key_bytes = &key.key;

        // Track actual first key of the SSTable
        if self.first_key.is_none() {
            self.first_key = Some(key_bytes.clone());
        }

        // Track actual last key of the SSTable (updated for every partition)
        self.last_key = Some(key_bytes.clone());

        // Count every partition for size_at_full_sampling.
        self.total_partition_count = self.total_partition_count.saturating_add(1);
    }

    /// Add a sampled index entry to the summary.
    ///
    /// The caller is responsible for sampling at the correct interval.  This method
    /// does NOT enforce sampling — it records every entry provided.
    ///
    /// Call `note_partition` for **every** partition, and `add_entry` only at the
    /// sampling boundary.  `add_entry` no longer updates `first_key`, `last_key`,
    /// or `total_partition_count`; those are managed by `note_partition`.
    ///
    /// # Arguments
    ///
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `index_offset` - Byte offset in Index.db where this partition's entry starts
    ///
    /// # Important
    ///
    /// Entries MUST be added in token order (same as Index.db order).
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::SummaryWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = SummaryWriter::new(128);
    ///
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// writer.note_partition(&key);
    /// writer.add_entry(&key, 0).unwrap();
    /// assert_eq!(writer.entry_count(), 1);
    /// ```
    pub fn add_entry(&mut self, key: &DecoratedKey, index_offset: u64) -> Result<()> {
        // Add the sampled index entry.  `first_key`, `last_key`, and
        // `total_partition_count` are managed by `note_partition` instead.
        self.entries.push(SummaryEntry {
            key: key.key.clone(),
            index_position: index_offset,
        });

        Ok(())
    }

    /// Finish writing and return the Summary.db bytes
    ///
    /// Serializes all entries in Cassandra 5.0 Summary.db format:
    /// - 24-byte header (big-endian)
    /// - Offset table (little-endian u32 array)
    /// - Entry data (keys + positions, no length prefix)
    /// - First key (length-prefixed, big-endian)
    /// - Last key (length-prefixed, big-endian)
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::SummaryWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = SummaryWriter::new(128);
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// writer.add_entry(&key, 100).unwrap();
    ///
    /// let bytes = writer.finish().unwrap();
    /// assert!(!bytes.is_empty());
    /// ```
    pub fn finish(self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Handle empty summary
        if self.entries.is_empty() {
            // Write minimal header for empty summary
            self.write_header(&mut buffer, 0, 0);
            return Ok(buffer);
        }

        // Calculate total summary_entries_size (offset table + entry data).
        //
        // CRITICAL (Issue #666): Cassandra's IndexSummary.deserialize expects entry
        // offsets to be ABSOLUTE from the start of the combined (offset_table +
        // entry_data) region — i.e. offset[0] == offset_table_size, NOT 0.
        //
        // CQLite previously stored zero-based offsets (relative to entry_data). The
        // CQLite reader's `normalize_entry_offsets` accepted both layouts, hiding the
        // divergence from unit tests, but Cassandra's own deserializer asserts that
        // offsets increase monotonically and start no earlier than the end of the
        // offset table — so a zero-based offset[0] triggers an AssertionError.
        //
        // Fix: bias every offset by `offset_table_size` so that offset[i] equals the
        // byte position of entry i within the combined (offset_table + entry_data)
        // block. This matches what Cassandra writes (verified by hex-dumping
        // Cassandra-generated Summary.db files from the test corpus).
        let offset_table_size = self.entries.len() * 4; // u32 per entry, LE
        let mut entry_offsets = Vec::with_capacity(self.entries.len());
        let mut entry_data = Vec::new();

        for entry in &self.entries {
            // Offset is absolute: start of offset_table + current entry_data length.
            entry_offsets.push((offset_table_size + entry_data.len()) as u32);

            // Write key bytes (no length prefix!)
            entry_data.extend_from_slice(&entry.key);

            // Write position (big-endian u64)
            entry_data.extend_from_slice(&entry.index_position.to_be_bytes());
        }

        let summary_entries_size = (offset_table_size + entry_data.len()) as u64;

        // Write header (24 bytes, big-endian)
        self.write_header(&mut buffer, self.entries.len() as u32, summary_entries_size);

        // Write offset table (LITTLE-ENDIAN!)
        for offset in entry_offsets {
            buffer.extend_from_slice(&offset.to_le_bytes());
        }

        // Write entry data
        buffer.extend_from_slice(&entry_data);

        // Write first key (length-prefixed, big-endian)
        if let Some(first_key) = &self.first_key {
            buffer.extend_from_slice(&(first_key.len() as u32).to_be_bytes());
            buffer.extend_from_slice(first_key);
        }

        // Write last key (length-prefixed, big-endian)
        if let Some(last_key) = &self.last_key {
            buffer.extend_from_slice(&(last_key.len() as u32).to_be_bytes());
            buffer.extend_from_slice(last_key);
        }

        Ok(buffer)
    }

    /// Get the number of summary entries
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::SummaryWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = SummaryWriter::new(128);
    /// assert_eq!(writer.entry_count(), 0);
    ///
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// writer.add_entry(&key, 0).unwrap();
    /// assert_eq!(writer.entry_count(), 1);
    /// ```
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Write Summary.db header (24 bytes, big-endian)
    fn write_header(&self, buffer: &mut Vec<u8>, entries_count: u32, summary_entries_size: u64) {
        // min_index_interval (u32, BE)
        buffer.extend_from_slice(&self.min_index_interval.to_be_bytes());

        // entries_count (u32, BE)
        buffer.extend_from_slice(&entries_count.to_be_bytes());

        // summary_entries_size (u64, BE)
        buffer.extend_from_slice(&summary_entries_size.to_be_bytes());

        // sampling_level (u32, BE).
        //
        // For a freshly written SSTable this is ALWAYS BASE_SAMPLING_LEVEL (128),
        // regardless of min_index_interval.  Cassandra only writes a value < 128 when
        // it has downsampled an existing Summary.db during compaction.
        //
        // BUG FIX (Issue #636): Previously emitted `min_index_interval` here, which
        // is wrong.  Any reader that checks `sampling_level < BASE_SAMPLING_LEVEL` to
        // detect downsampling would incorrectly treat a CQLite-written Summary.db as
        // downsampled when min_index_interval ≠ 128.
        //
        // Source: IndexSummary.java:88–94, 226–229 (Cassandra 5.0.8).
        buffer.extend_from_slice(&BASE_SAMPLING_LEVEL.to_be_bytes());

        // size_at_full_sampling (u32, BE).
        //
        // For a freshly written SSTable (sampling_level == BASE_SAMPLING_LEVEL, i.e. never
        // downsampled), this field equals entries_count.  It only diverges from entries_count
        // after Cassandra downsamples an existing Summary.db during compaction — the sampled
        // count decreases while size_at_full_sampling retains the original count.
        //
        // Verified against real Cassandra 5.0 corpus:
        //   composite_key_table (1 entry):  size_at_full_sampling = 1 = entries_count
        //   simple_table (8 entries):       size_at_full_sampling = 8 = entries_count
        //
        // Source: IndexSummary.java:235–237 (`getMaxNumberOfEntries()`).
        buffer.extend_from_slice(&entries_count.to_be_bytes());
    }
}

impl Default for SummaryWriter {
    fn default() -> Self {
        Self::new(128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summary_writer_new() {
        let writer = SummaryWriter::new(128);
        assert_eq!(writer.entry_count(), 0);
        assert_eq!(writer.min_index_interval, 128);
    }

    #[test]
    fn test_add_single_entry() {
        let mut writer = SummaryWriter::new(128);
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_entry(&key, 0).unwrap();

        assert_eq!(writer.entry_count(), 1);
    }

    #[test]
    fn test_add_multiple_entries() {
        let mut writer = SummaryWriter::new(128);

        let key1 = DecoratedKey::new(100, vec![0x01]);
        let key2 = DecoratedKey::new(200, vec![0x02]);
        let key3 = DecoratedKey::new(300, vec![0x03]);

        writer.add_entry(&key1, 0).unwrap();
        writer.add_entry(&key2, 1024).unwrap();
        writer.add_entry(&key3, 2048).unwrap();

        assert_eq!(writer.entry_count(), 3);
    }

    #[test]
    fn test_finish_single_entry() {
        let mut writer = SummaryWriter::new(128);
        let key = DecoratedKey::new(12345, vec![0x01, 0x02, 0x03, 0x04]);

        // note_partition must be called for every partition (sets first_key/last_key)
        writer.note_partition(&key);
        writer.add_entry(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Verify structure:
        // [Header: 24 bytes]
        // [Offset table: 4 bytes (1 entry, LE)]
        // [Entry data: 4 (key) + 8 (position) = 12 bytes]
        // [First key: 4 (len) + 4 (data) = 8 bytes]
        // [Last key: 4 (len) + 4 (data) = 8 bytes]
        // Total: 24 + 4 + 12 + 8 + 8 = 56 bytes

        assert_eq!(bytes.len(), 56);

        // Verify header
        // min_index_interval = 128
        assert_eq!(&bytes[0..4], &[0x00, 0x00, 0x00, 0x80]);
        // entries_count = 1
        assert_eq!(&bytes[4..8], &[0x00, 0x00, 0x00, 0x01]);
        // summary_entries_size = 16 (4 bytes offset table + 12 bytes entry data)
        assert_eq!(
            &bytes[8..16],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10]
        );
        // sampling_level = 128
        assert_eq!(&bytes[16..20], &[0x00, 0x00, 0x00, 0x80]);
        // size_at_full_sampling = 1
        assert_eq!(&bytes[20..24], &[0x00, 0x00, 0x00, 0x01]);

        // Verify offset table (LITTLE-ENDIAN!)
        // Offset 0 for first entry: absolute = offset_table_size (4) + 0 = 4
        // Cassandra IndexSummary.deserialize expects offsets to be absolute from
        // start of (offset_table + entry_data), so offset[0] == offset_table_size.
        assert_eq!(&bytes[24..28], &[0x04, 0x00, 0x00, 0x00]);

        // Verify entry data
        // Key: [0x01, 0x02, 0x03, 0x04]
        assert_eq!(&bytes[28..32], &[0x01, 0x02, 0x03, 0x04]);
        // Position: 0 (BE u64)
        assert_eq!(
            &bytes[32..40],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );

        // Verify first key
        // Length: 4 (BE u32)
        assert_eq!(&bytes[40..44], &[0x00, 0x00, 0x00, 0x04]);
        // Data: [0x01, 0x02, 0x03, 0x04]
        assert_eq!(&bytes[44..48], &[0x01, 0x02, 0x03, 0x04]);

        // Verify last key (same as first for single entry)
        // Length: 4 (BE u32)
        assert_eq!(&bytes[48..52], &[0x00, 0x00, 0x00, 0x04]);
        // Data: [0x01, 0x02, 0x03, 0x04]
        assert_eq!(&bytes[52..56], &[0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_finish_multiple_entries() {
        let mut writer = SummaryWriter::new(128);

        // Entry 1: 2-byte key, position 0
        let key1 = DecoratedKey::new(100, vec![0xAA, 0xBB]);
        writer.note_partition(&key1);
        writer.add_entry(&key1, 0).unwrap();

        // Entry 2: 3-byte key, position 1024
        let key2 = DecoratedKey::new(200, vec![0xCC, 0xDD, 0xEE]);
        writer.note_partition(&key2);
        writer.add_entry(&key2, 1024).unwrap();

        let bytes = writer.finish().unwrap();

        // Verify entries_count in header
        assert_eq!(&bytes[4..8], &[0x00, 0x00, 0x00, 0x02]);

        // Verify offset table (LE)
        // Offsets are ABSOLUTE from start of (offset_table + entry_data).
        // offset_table_size = 2 entries * 4 bytes = 8
        // Offset 0: 8 (0x08 in LE) = offset_table_size + 0
        assert_eq!(&bytes[24..28], &[0x08, 0x00, 0x00, 0x00]);
        // Offset 1: 18 (0x12 in LE) = offset_table_size(8) + entry_1_size(10)
        // entry_1_size = 2 bytes key + 8 bytes pos = 10
        assert_eq!(&bytes[28..32], &[0x12, 0x00, 0x00, 0x00]);

        // Verify entry 1 data
        assert_eq!(&bytes[32..34], &[0xAA, 0xBB]); // key
        assert_eq!(
            &bytes[34..42],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        ); // position = 0

        // Verify entry 2 data
        assert_eq!(&bytes[42..45], &[0xCC, 0xDD, 0xEE]); // key
                                                         // position = 1024 (0x0000000000000400)
        assert_eq!(
            &bytes[45..53],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00]
        );

        // Verify first key (2 bytes)
        assert_eq!(&bytes[53..57], &[0x00, 0x00, 0x00, 0x02]); // length
        assert_eq!(&bytes[57..59], &[0xAA, 0xBB]);

        // Verify last key (3 bytes)
        assert_eq!(&bytes[59..63], &[0x00, 0x00, 0x00, 0x03]); // length
        assert_eq!(&bytes[63..66], &[0xCC, 0xDD, 0xEE]);
    }

    #[test]
    fn test_offset_table_little_endian() {
        let mut writer = SummaryWriter::new(128);

        // Create entries with known key sizes to verify offset calculation
        let key1 = DecoratedKey::new(100, vec![0x01; 16]); // 16 bytes
        let key2 = DecoratedKey::new(200, vec![0x02; 16]); // 16 bytes

        writer.note_partition(&key1);
        writer.add_entry(&key1, 0).unwrap();
        writer.note_partition(&key2);
        writer.add_entry(&key2, 100).unwrap();

        let bytes = writer.finish().unwrap();

        // Offset table starts at byte 24.
        // Offsets are ABSOLUTE from start of (offset_table + entry_data).
        // offset_table_size = 2 entries * 4 bytes = 8
        // Offset 0: 8 (LE: 0x08 0x00 0x00 0x00) = offset_table_size + 0
        assert_eq!(&bytes[24..28], &[0x08, 0x00, 0x00, 0x00]);

        // Offset 1: 32 (LE: 0x20 0x00 0x00 0x00)
        // = offset_table_size(8) + entry_1_size(24) = 32
        // Entry 1 is 16 bytes (key) + 8 bytes (position) = 24 bytes
        assert_eq!(&bytes[28..32], &[0x20, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_sampling_behavior() {
        // Simulate sampling every 128th entry
        let mut writer = SummaryWriter::new(128);

        // Sample entry 0, 128, 256
        let key0 = DecoratedKey::new(100, vec![0x00]);
        let key128 = DecoratedKey::new(200, vec![0x80]);
        let key256 = DecoratedKey::new(300, vec![0xFF]);

        writer.note_partition(&key0);
        writer.add_entry(&key0, 0).unwrap();
        writer.note_partition(&key128);
        writer.add_entry(&key128, 2048).unwrap();
        writer.note_partition(&key256);
        writer.add_entry(&key256, 4096).unwrap();

        assert_eq!(writer.entry_count(), 3);

        let bytes = writer.finish().unwrap();

        // Verify entries_count
        assert_eq!(&bytes[4..8], &[0x00, 0x00, 0x00, 0x03]);
    }

    #[test]
    fn test_first_and_last_keys() {
        let mut writer = SummaryWriter::new(128);

        let first_key_bytes = vec![0x01, 0x02];
        let middle_key_bytes = vec![0x03, 0x04];
        let last_key_bytes = vec![0x05, 0x06];

        let key1 = DecoratedKey::new(100, first_key_bytes.clone());
        let key2 = DecoratedKey::new(200, middle_key_bytes.clone());
        let key3 = DecoratedKey::new(300, last_key_bytes.clone());

        // note_partition must be called for every partition so first/last keys
        // reflect the full SSTable range.  add_entry is only for sampled entries.
        writer.note_partition(&key1);
        writer.add_entry(&key1, 0).unwrap();
        writer.note_partition(&key2);
        writer.add_entry(&key2, 1024).unwrap();
        writer.note_partition(&key3);
        writer.add_entry(&key3, 2048).unwrap();

        let bytes = writer.finish().unwrap();

        // Find first and last keys in output
        // They are at the end after entry data

        // Header: 24 bytes
        // Offset table: 12 bytes (3 entries * 4 bytes)
        // Entry data: 3 * (2 bytes key + 8 bytes pos) = 30 bytes
        // Total before first key: 24 + 12 + 30 = 66 bytes

        // First key
        let first_key_start = 66;
        assert_eq!(
            &bytes[first_key_start..first_key_start + 4],
            &[0x00, 0x00, 0x00, 0x02]
        ); // length
        assert_eq!(
            &bytes[first_key_start + 4..first_key_start + 6],
            &first_key_bytes[..]
        );

        // Last key
        let last_key_start = first_key_start + 6;
        assert_eq!(
            &bytes[last_key_start..last_key_start + 4],
            &[0x00, 0x00, 0x00, 0x02]
        ); // length
        assert_eq!(
            &bytes[last_key_start + 4..last_key_start + 6],
            &last_key_bytes[..]
        );
    }

    #[test]
    fn test_empty_summary() {
        let writer = SummaryWriter::new(128);
        let bytes = writer.finish().unwrap();

        // Empty summary should just have header (24 bytes) with zeros
        assert_eq!(bytes.len(), 24);

        // Verify zero entries
        assert_eq!(&bytes[4..8], &[0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_large_position_value() {
        let mut writer = SummaryWriter::new(128);

        let key = DecoratedKey::new(12345, vec![0xFF]);
        // Large position value: 1GB
        writer.note_partition(&key);
        writer.add_entry(&key, 1_073_741_824).unwrap();

        let bytes = writer.finish().unwrap();

        // Position is at offset: 24 (header) + 4 (offset table) + 1 (key) = 29
        // Position: 0x0000000040000000 (1GB in big-endian)
        assert_eq!(
            &bytes[29..37],
            &[0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn test_position_encoding() {
        let mut writer = SummaryWriter::new(128);
        let key = DecoratedKey::new(12345, vec![0x01]);

        // Test specific position value: 12381
        writer.note_partition(&key);
        writer.add_entry(&key, 12381).unwrap();

        let bytes = writer.finish().unwrap();

        // Position is at: 24 (header) + 4 (offset) + 1 (key) = 29
        // 12381 in big-endian u64: 0x000000000000305D
        assert_eq!(
            &bytes[29..37],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x5D]
        );
    }

    #[test]
    fn test_hex_dump_verification() {
        let mut writer = SummaryWriter::new(128);

        // Create a simple entry for hex verification
        let key = DecoratedKey::new(12345, vec![0x01, 0x02, 0x03, 0x04]);
        writer.note_partition(&key);
        writer.add_entry(&key, 0).unwrap();

        let bytes = writer.finish().unwrap();

        // Print hex dump for manual verification (useful for debugging)
        println!("\nSummary.db hex dump:");
        for (i, chunk) in bytes.chunks(16).enumerate() {
            print!("{:08x}: ", i * 16);
            for byte in chunk {
                print!("{:02x} ", byte);
            }
            println!();
        }

        // Verify key sections are correct
        assert_eq!(
            &bytes[0..2],
            &[0x00, 0x00],
            "Header should start with 0x0000"
        );
    }

    #[test]
    fn test_custom_min_index_interval() {
        let writer = SummaryWriter::new(64);
        assert_eq!(writer.min_index_interval, 64);

        let bytes = writer.finish().unwrap();

        // Verify min_index_interval in header
        assert_eq!(&bytes[0..4], &[0x00, 0x00, 0x00, 0x40]); // 64 in BE

        // sampling_level must always be BASE_SAMPLING_LEVEL (128) for a fresh SSTable,
        // independent of min_index_interval.  Previously the writer emitted
        // min_index_interval (64) here — that was wrong (Issue #636).
        // Source: IndexSummary.java:88–94, 226–229 (Cassandra 5.0.8).
        assert_eq!(&bytes[16..20], &[0x00, 0x00, 0x00, 0x80]); // 128 in BE
    }

    #[test]
    fn test_token_order_preservation() {
        let mut writer = SummaryWriter::new(128);

        // Add entries in token order (caller's responsibility)
        let key1 = DecoratedKey::new(-5000000000, vec![0x01]);
        let key2 = DecoratedKey::new(0, vec![0x02]);
        let key3 = DecoratedKey::new(5000000000, vec![0x03]);

        writer.note_partition(&key1);
        writer.add_entry(&key1, 0).unwrap();
        writer.note_partition(&key2);
        writer.add_entry(&key2, 1000).unwrap();
        writer.note_partition(&key3);
        writer.add_entry(&key3, 2000).unwrap();

        let bytes = writer.finish().unwrap();

        // Verify entry count
        assert_eq!(&bytes[4..8], &[0x00, 0x00, 0x00, 0x03]);
    }

    #[test]
    fn test_variable_key_sizes() {
        let mut writer = SummaryWriter::new(128);

        // Mix of different key sizes
        let key1 = DecoratedKey::new(100, vec![0x01]); // 1 byte
        let key2 = DecoratedKey::new(200, vec![0x02, 0x03]); // 2 bytes
        let key3 = DecoratedKey::new(300, vec![0x04, 0x05, 0x06, 0x07]); // 4 bytes

        writer.note_partition(&key1);
        writer.add_entry(&key1, 0).unwrap();
        writer.note_partition(&key2);
        writer.add_entry(&key2, 100).unwrap();
        writer.note_partition(&key3);
        writer.add_entry(&key3, 200).unwrap();

        let bytes = writer.finish().unwrap();

        // Verify offset table accounts for variable key sizes.
        // Offsets are ABSOLUTE from start of (offset_table + entry_data).
        // offset_table_size = 3 entries * 4 bytes = 12
        // Offset 0: 12 (LE: 0x0C 0x00 0x00 0x00) = offset_table_size + 0
        assert_eq!(&bytes[24..28], &[0x0C, 0x00, 0x00, 0x00]);
        // Offset 1: 21 (LE: 0x15 0x00 0x00 0x00) = 12 + 9 (1 byte key + 8 byte position)
        assert_eq!(&bytes[28..32], &[0x15, 0x00, 0x00, 0x00]);
        // Offset 2: 31 (LE: 0x1F 0x00 0x00 0x00) = 12 + 9 + 10 (2 byte key + 8 byte position)
        assert_eq!(&bytes[32..36], &[0x1F, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_large_key() {
        let mut writer = SummaryWriter::new(128);

        // Test with a large partition key (e.g., composite key)
        let large_key = vec![0xAB; 256];
        let key = DecoratedKey::new(12345, large_key.clone());

        writer.note_partition(&key);
        writer.add_entry(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Verify key is stored correctly
        // Entry data starts at: 24 (header) + 4 (offset)
        assert_eq!(&bytes[28..28 + 256], &large_key[..]);
    }

    #[test]
    fn test_realistic_scenario() {
        let mut writer = SummaryWriter::new(128);

        // Simulate realistic SSTable with sampled entries
        // Total partitions: 384 (samples at 0, 128, 256)

        let key0 = DecoratedKey::new(-5000000000, vec![0x00, 0x00, 0x03, 0xE9]); // partition 0
        let key128 = DecoratedKey::new(-1000000000, vec![0x00, 0x00, 0x03, 0xEA]); // partition 128
        let key256 = DecoratedKey::new(3000000000, vec![0x00, 0x00, 0x03, 0xEB]); // partition 256

        writer.note_partition(&key0);
        writer.add_entry(&key0, 0).unwrap();
        writer.note_partition(&key128);
        writer.add_entry(&key128, 25600).unwrap(); // ~100 bytes per partition
        writer.note_partition(&key256);
        writer.add_entry(&key256, 51200).unwrap();

        assert_eq!(writer.entry_count(), 3);

        let bytes = writer.finish().unwrap();

        // Verify structure
        assert!(bytes.len() > 24); // At least header

        // Verify entries_count
        assert_eq!(&bytes[4..8], &[0x00, 0x00, 0x00, 0x03]);
    }

    #[test]
    fn test_summary_entries_size_calculation() {
        let mut writer = SummaryWriter::new(128);

        let key1 = DecoratedKey::new(100, vec![0x01, 0x02]); // 2 bytes
        let key2 = DecoratedKey::new(200, vec![0x03, 0x04]); // 2 bytes

        writer.note_partition(&key1);
        writer.add_entry(&key1, 0).unwrap();
        writer.note_partition(&key2);
        writer.add_entry(&key2, 1024).unwrap();

        let bytes = writer.finish().unwrap();

        // Calculate expected summary_entries_size:
        // Offset table: 2 entries * 4 bytes = 8 bytes
        // Entry data: 2 * (2 bytes key + 8 bytes position) = 20 bytes
        // Total: 28 bytes = 0x000000000000001C

        // Verify summary_entries_size in header (bytes 8-16)
        assert_eq!(
            &bytes[8..16],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C]
        );
    }

    #[test]
    fn test_16_byte_key() {
        let mut writer = SummaryWriter::new(128);

        // Test with 16-byte key (common for MD5 digest)
        let key_bytes: [u8; 16] = [
            0xdc, 0x67, 0x26, 0xa6, 0x05, 0xc6, 0x48, 0x50, 0x86, 0xcd, 0x0f, 0xe3, 0x1b, 0x67,
            0x57, 0xaf,
        ];
        let key = DecoratedKey::new(12345, key_bytes.to_vec());

        writer.note_partition(&key);
        writer.add_entry(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Verify key is stored correctly
        // Entry data starts at: 24 (header) + 4 (offset) = 28
        assert_eq!(&bytes[28..44], &key_bytes[..]);
    }

    #[test]
    fn test_default_min_index_interval() {
        let writer = SummaryWriter::default();
        assert_eq!(writer.min_index_interval, 128);
    }

    // Note: Roundtrip tests with SummaryReader would require exposing parse_summary_data
    // as public API. For now, byte-level verification in other tests provides sufficient
    // format validation. Integration tests can verify end-to-end compatibility.

    /// Regression test for Issue #666: CQLite-written Summary.db was rejected by
    /// Cassandra 5's IndexSummary.deserialize because entry offsets were zero-based
    /// (relative to the entry_data region) instead of absolute (from the start of the
    /// combined offset_table + entry_data region).
    ///
    /// Cassandra's IndexSummary.deserialize asserts that every offset is >=
    /// offset_table_size.  With zero-based offsets the first entry had offset = 0,
    /// which is < offset_table_size for any non-empty SSTable, triggering an
    /// AssertionError and causing the readback test to exclude Summary.db via
    /// `tar --exclude='*Summary.db'`.
    ///
    /// Fix: offsets are now biased by `entries_count * 4` (= offset_table_size) so
    /// that offset[0] == offset_table_size, matching what Cassandra writes.
    ///
    /// Verified against hex dumps of real Cassandra 5.0.2-generated Summary.db files:
    /// - 1-entry file: offset[0] = 0x04 (= 1*4)
    /// - 8-entry file: offset[0] = 0x20 (= 8*4 = 32)
    #[test]
    fn issue_666_offset_table_absolute_not_relative() {
        // ── 1-entry case (matches Cassandra-generated test corpus) ──────────────
        // composite_key_table: 1 entry, 16-byte UUID key, position = 0
        // Expected offset[0] = 0x04 in LE (= offset_table_size = 1*4)
        {
            let mut writer = SummaryWriter::new(128);
            let key = DecoratedKey::new(12345, vec![0u8; 16]);
            writer.note_partition(&key);
            writer.add_entry(&key, 0).unwrap();
            let bytes = writer.finish().unwrap();

            // offset_table_size = 1 * 4 = 4
            // offset[0] must equal 4 (absolute), NOT 0 (relative)
            assert_eq!(
                &bytes[24..28],
                &[0x04, 0x00, 0x00, 0x00],
                "Issue #666: single-entry offset must be 4 (absolute), not 0 (relative)"
            );
        }

        // ── 8-entry case (matches Cassandra's simple_table Summary.db) ──────────
        // offset_table_size = 8 * 4 = 32 = 0x20
        // Each entry: 16-byte UUID key + 8-byte position = 24 bytes
        // Expected offsets: 32, 56, 80, 104, 128, 152, 176, 200
        //   = 0x20, 0x38, 0x50, 0x68, 0x80, 0x98, 0xB0, 0xC8
        // This matches exactly the offset table of test_basic/simple_table in the corpus.
        {
            let mut writer = SummaryWriter::new(128);
            for i in 0u8..8 {
                let key = DecoratedKey::new(i as i64 * 1000, vec![i; 16]);
                writer.note_partition(&key);
                writer.add_entry(&key, i as u64 * 1024).unwrap();
            }
            let bytes = writer.finish().unwrap();

            let expected_offsets: &[u32] = &[32, 56, 80, 104, 128, 152, 176, 200];
            for (idx, &expected) in expected_offsets.iter().enumerate() {
                let offset_pos = 24 + idx * 4;
                let actual = u32::from_le_bytes([
                    bytes[offset_pos],
                    bytes[offset_pos + 1],
                    bytes[offset_pos + 2],
                    bytes[offset_pos + 3],
                ]);
                assert_eq!(
                    actual, expected,
                    "Issue #666: offset[{idx}] = {actual}, want {expected} (absolute)"
                );
            }
        }

        // ── Writer-reader roundtrip: absolute offsets survive the CQLite reader ─
        // The CQLite summary_reader.rs `normalize_entry_offsets` must accept absolute
        // offsets (the canonical format) and return correct zero-based positions.
        {
            use crate::storage::sstable::summary_reader::parse_summary_header;
            use nom::error::Error as NomError;
            use nom::multi::count;
            use nom::number::complete::le_u32;

            let mut writer = SummaryWriter::new(128);
            let key_bytes = vec![0xAB; 4];
            let key = DecoratedKey::new(42, key_bytes.clone());
            writer.note_partition(&key);
            writer.add_entry(&key, 99).unwrap();
            let bytes = writer.finish().unwrap();

            // Parse header
            let (after_header, header) = parse_summary_header(&bytes).unwrap();
            assert_eq!(header.entries_count, 1);
            assert_eq!(header.summary_entries_size, 16); // 4 (offset table) + 12 (4-byte key + 8-byte pos)

            // Parse offset table: the single offset should be 4 (absolute)
            let (_, offsets) = count(le_u32::<_, NomError<_>>, 1usize)(after_header).unwrap();
            assert_eq!(
                offsets[0], 4,
                "Issue #666: writer must emit absolute offset 4, not 0"
            );
        }
    }

    /// Regression test for Issue #666 (Part 2): first_key and last_key in Summary.db
    /// must cover the ENTIRE SSTable, not just sampled partitions.
    ///
    /// Before the fix, `first_key`/`last_key` were set only when `add_entry` was called
    /// (i.e., at sampling boundaries).  For an SSTable with fewer than
    /// `min_index_interval` partitions, only partition[0] would be sampled, and thus
    /// `first_key == last_key == key[0]`.  Cassandra uses these fields for range queries
    /// — with both set to key[0], all other partitions become invisible.
    ///
    /// Fix: `note_partition` must be called for every partition.  It tracks first/last
    /// keys independently of the sampling decision.
    #[test]
    fn issue_666_first_last_keys_cover_all_partitions() {
        let mut writer = SummaryWriter::new(128);

        // 3 partitions, but only partition[0] is at a sampling boundary.
        // Simulates the basic-primitives e2e test (3 UUID rows, interval=128).
        let key_first = DecoratedKey::new(100, vec![0x01; 16]); // first in token order
        let key_mid = DecoratedKey::new(200, vec![0x02; 16]);
        let key_last = DecoratedKey::new(300, vec![0x03; 16]); // last in token order

        // Partition 0 is sampled (counter=0 → 0 % 128 == 0)
        writer.note_partition(&key_first);
        writer.add_entry(&key_first, 0).unwrap();

        // Partitions 1 and 2 are NOT sampled, but note_partition must still be called
        writer.note_partition(&key_mid);
        writer.note_partition(&key_last);

        let bytes = writer.finish().unwrap();

        // entries_count = 1 (only first was sampled)
        assert_eq!(
            &bytes[4..8],
            &[0x00, 0x00, 0x00, 0x01],
            "Issue #666: entries_count must be 1 (only sampled entry)"
        );

        // first_key must be key_first
        // Layout: header(24) + offset_table(4) + entry_data(16+8=24) = 52
        // first_key: len(4) + data(16) = 20 bytes at offset 52
        let first_key_len_pos = 52;
        let first_key_len = u32::from_be_bytes(
            bytes[first_key_len_pos..first_key_len_pos + 4]
                .try_into()
                .unwrap(),
        );
        assert_eq!(first_key_len, 16, "Issue #666: first_key len must be 16");
        assert_eq!(
            &bytes[first_key_len_pos + 4..first_key_len_pos + 20],
            &[0x01; 16],
            "Issue #666: first_key must be key_first"
        );

        // last_key must be key_last (the 3rd partition, which was only noted, not sampled)
        let last_key_len_pos = first_key_len_pos + 20; // 72
        let last_key_len = u32::from_be_bytes(
            bytes[last_key_len_pos..last_key_len_pos + 4]
                .try_into()
                .unwrap(),
        );
        assert_eq!(last_key_len, 16, "Issue #666: last_key len must be 16");
        assert_eq!(
            &bytes[last_key_len_pos + 4..last_key_len_pos + 20],
            &[0x03; 16],
            "Issue #666: last_key must be key_last (all 3 partitions visible)"
        );
    }
}
