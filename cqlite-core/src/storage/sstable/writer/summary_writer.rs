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
//!     be32 sampling_level;          // Sampling level (1-128)
//!     be32 size_at_full_sampling;   // Entries at full sampling
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
            entries: Vec::new(),
            first_key: None,
            last_key: None,
        }
    }

    /// Add a sampled entry to the summary
    ///
    /// The caller is responsible for sampling at the correct interval. This method
    /// does NOT enforce sampling - it adds every entry provided.
    ///
    /// # Arguments
    ///
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `index_offset` - Byte offset in Index.db where this partition's entry starts
    ///
    /// # Important
    ///
    /// Entries MUST be added in token order (same as Index.db order).
    /// First and last keys are tracked automatically.
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
    /// writer.add_entry(&key, 0).unwrap();
    /// assert_eq!(writer.entry_count(), 1);
    /// ```
    pub fn add_entry(&mut self, key: &DecoratedKey, index_offset: u64) -> Result<()> {
        let key_bytes = key.key.clone();

        // Track first key
        if self.first_key.is_none() {
            self.first_key = Some(key_bytes.clone());
        }

        // Always update last key
        self.last_key = Some(key_bytes.clone());

        // Add entry
        self.entries.push(SummaryEntry {
            key: key_bytes,
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

        // Calculate entry data sizes and offsets
        let mut entry_offsets = Vec::with_capacity(self.entries.len());
        let mut entry_data = Vec::new();

        for entry in &self.entries {
            // Record offset for this entry
            entry_offsets.push(entry_data.len() as u32);

            // Write key bytes (no length prefix!)
            entry_data.extend_from_slice(&entry.key);

            // Write position (big-endian u64)
            entry_data.extend_from_slice(&entry.index_position.to_be_bytes());
        }

        // Calculate total summary_entries_size (offset table + entry data)
        let offset_table_size = entry_offsets.len() * 4; // u32 per entry
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

        // sampling_level (u32, BE) - typically same as min_index_interval
        buffer.extend_from_slice(&self.min_index_interval.to_be_bytes());

        // size_at_full_sampling (u32, BE) - entries count at full sampling
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
        // Offset 0 for first entry
        assert_eq!(&bytes[24..28], &[0x00, 0x00, 0x00, 0x00]);

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
        writer.add_entry(&key1, 0).unwrap();

        // Entry 2: 3-byte key, position 1024
        let key2 = DecoratedKey::new(200, vec![0xCC, 0xDD, 0xEE]);
        writer.add_entry(&key2, 1024).unwrap();

        let bytes = writer.finish().unwrap();

        // Verify entries_count in header
        assert_eq!(&bytes[4..8], &[0x00, 0x00, 0x00, 0x02]);

        // Verify offset table (LE)
        // Offset 0: 0x00 0x00 0x00 0x00 (entry 1 starts at 0)
        assert_eq!(&bytes[24..28], &[0x00, 0x00, 0x00, 0x00]);
        // Offset 1: 0x0A 0x00 0x00 0x00 (entry 2 starts at 10 = 2 bytes key + 8 bytes pos)
        assert_eq!(&bytes[28..32], &[0x0A, 0x00, 0x00, 0x00]);

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

        writer.add_entry(&key1, 0).unwrap();
        writer.add_entry(&key2, 100).unwrap();

        let bytes = writer.finish().unwrap();

        // Offset table starts at byte 24
        // Offset 0: 0 (LE: 0x00 0x00 0x00 0x00)
        assert_eq!(&bytes[24..28], &[0x00, 0x00, 0x00, 0x00]);

        // Offset 1: 24 (LE: 0x18 0x00 0x00 0x00)
        // Entry 1 is 16 bytes (key) + 8 bytes (position) = 24 bytes
        assert_eq!(&bytes[28..32], &[0x18, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_sampling_behavior() {
        // Simulate sampling every 128th entry
        let mut writer = SummaryWriter::new(128);

        // Sample entry 0, 128, 256
        let key0 = DecoratedKey::new(100, vec![0x00]);
        let key128 = DecoratedKey::new(200, vec![0x80]);
        let key256 = DecoratedKey::new(300, vec![0xFF]);

        writer.add_entry(&key0, 0).unwrap();
        writer.add_entry(&key128, 2048).unwrap();
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

        writer.add_entry(&key1, 0).unwrap();
        writer.add_entry(&key2, 1024).unwrap();
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

        // Verify sampling_level matches
        assert_eq!(&bytes[16..20], &[0x00, 0x00, 0x00, 0x40]); // 64 in BE
    }

    #[test]
    fn test_token_order_preservation() {
        let mut writer = SummaryWriter::new(128);

        // Add entries in token order (caller's responsibility)
        let key1 = DecoratedKey::new(-5000000000, vec![0x01]);
        let key2 = DecoratedKey::new(0, vec![0x02]);
        let key3 = DecoratedKey::new(5000000000, vec![0x03]);

        writer.add_entry(&key1, 0).unwrap();
        writer.add_entry(&key2, 1000).unwrap();
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

        writer.add_entry(&key1, 0).unwrap();
        writer.add_entry(&key2, 100).unwrap();
        writer.add_entry(&key3, 200).unwrap();

        let bytes = writer.finish().unwrap();

        // Verify offset table accounts for variable key sizes
        // Offset 0: 0
        assert_eq!(&bytes[24..28], &[0x00, 0x00, 0x00, 0x00]);
        // Offset 1: 9 (1 byte key + 8 byte position)
        assert_eq!(&bytes[28..32], &[0x09, 0x00, 0x00, 0x00]);
        // Offset 2: 19 (9 + 2 byte key + 8 byte position)
        assert_eq!(&bytes[32..36], &[0x13, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_large_key() {
        let mut writer = SummaryWriter::new(128);

        // Test with a large partition key (e.g., composite key)
        let large_key = vec![0xAB; 256];
        let key = DecoratedKey::new(12345, large_key.clone());

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

        writer.add_entry(&key0, 0).unwrap();
        writer.add_entry(&key128, 25600).unwrap(); // ~100 bytes per partition
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

        writer.add_entry(&key1, 0).unwrap();
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
}
