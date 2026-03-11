//! Summary.db reader implementation for Cassandra 5.0 SSTable format
//!
//! This module provides parsing of Summary.db files which contain sampled
//! partition keys and their corresponding index positions for efficient
//! partition boundary detection.
//!
//! ## Cassandra 5.0 Summary.db Format (Issue #218 Fix)
//!
//! The Summary.db file has the following structure:
//!
//! ```text
//! +----------------------+
//! | Header (24 bytes)    |
//! +----------------------+
//! | Offset table (LE)    |  ← Little-endian u32 offsets!
//! +----------------------+
//! | Entry data           |  ← key_data + be_u64 position
//! +----------------------+
//! | First key (prefixed) |  ← be_u32 size + key data
//! +----------------------+
//! | Last key (prefixed)  |  ← be_u32 size + key data
//! +----------------------+
//! ```
//!
//! ### Header (24 bytes, all big-endian)
//! - `min_index_interval` (u32): Lower bound for partitions between index entries (e.g., 128)
//! - `entries_count` (u32): Number of summary entries
//! - `summary_entries_size` (u64): Total size of offset table + entry data in bytes
//! - `sampling_level` (u32): Sampling level (1-128, typically 128)
//! - `size_at_full_sampling` (u32): Entries count at full sampling
//!
//! ### Entry Format
//! - No length prefix for keys - boundaries determined by offset differences
//! - Entry = key_data (variable) + position (be_u64)
//! - Position is offset in Index.db file
//!
//! ### Critical: Offset Table is LITTLE-ENDIAN
//! Unlike all other Cassandra binary formats which use big-endian, the offset
//! table in Summary.db uses little-endian byte order for historical reasons.

use crate::{
    error::{Error, Result},
    platform::Platform,
};

use nom::{
    bytes::complete::take,
    error::Error as NomError,
    multi::count,
    number::complete::{be_u32, be_u64, le_u32},
    IResult,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

/// Summary.db file header - Cassandra 5.0 format (24 bytes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryHeader {
    /// Lower bound for average partitions between index entries (e.g., 128)
    pub min_index_interval: u32,
    /// Number of summary entries
    pub entries_count: u32,
    /// Total size of offset table + entry data in bytes
    pub summary_entries_size: u64,
    /// Sampling level (1-128, typically 128)
    pub sampling_level: u32,
    /// Entries count at full sampling
    pub size_at_full_sampling: u32,
}

/// Header size in bytes
const SUMMARY_HEADER_SIZE: usize = 24;

/// Maximum reasonable entries (sanity check)
const MAX_REASONABLE_ENTRIES: u32 = 100_000_000;

/// Summary entry representing a sampled partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryEntry {
    /// Sampled partition key
    pub partition_key: Vec<u8>,
    /// Position in Index.db file (byte offset)
    pub position: u64,
}

/// Complete Summary.db data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryData {
    /// File header
    pub header: SummaryHeader,
    /// All summary entries (in order)
    pub entries: Vec<SummaryEntry>,
    /// First partition key in the SSTable
    pub first_key: Vec<u8>,
    /// Last partition key in the SSTable
    pub last_key: Vec<u8>,
}

/// High-level Summary.db file reader
#[allow(dead_code)]
pub struct SummaryReader {
    /// Path to the Summary.db file
    file_path: PathBuf,
    /// Parsed summary data
    summary_data: SummaryData,
    /// Platform abstraction for file operations
    platform: Arc<Platform>,
}

impl SummaryReader {
    /// Open and parse a Summary.db file
    pub async fn open(path: &Path, platform: Arc<Platform>) -> Result<Self> {
        if !platform.fs().exists(path).await? {
            return Err(Error::not_found(format!(
                "Summary.db file not found: {}",
                path.display()
            )));
        }

        // Read the entire file
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        // Parse the summary data
        let summary_data = parse_summary_data(&buffer)
            .map_err(|e| Error::corruption(format!("Failed to parse Summary.db: {:?}", e)))?;

        Ok(Self {
            file_path: path.to_path_buf(),
            summary_data,
            platform,
        })
    }

    /// Get all summary entries
    pub fn get_entries(&self) -> &[SummaryEntry] {
        &self.summary_data.entries
    }

    /// Get the header
    pub fn get_header(&self) -> &SummaryHeader {
        &self.summary_data.header
    }

    /// Get the first partition key
    pub fn get_first_key(&self) -> &[u8] {
        &self.summary_data.first_key
    }

    /// Get the last partition key
    pub fn get_last_key(&self) -> &[u8] {
        &self.summary_data.last_key
    }

    /// Find the best summary entry for a given index position
    ///
    /// Returns the entry with the largest position <= target position.
    /// This is useful for finding which summary entry covers a given
    /// position in the Index.db file.
    pub fn find_entry_for_position(&self, target_position: u64) -> Option<&SummaryEntry> {
        let mut left = 0;
        let mut right = self.summary_data.entries.len();
        let mut best_entry = None;

        while left < right {
            let mid = left + (right - left) / 2;
            let entry = &self.summary_data.entries[mid];

            if entry.position <= target_position {
                best_entry = Some(entry);
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        best_entry
    }

    /// Find the entry at a specific index
    pub fn get_entry_at(&self, index: usize) -> Option<&SummaryEntry> {
        self.summary_data.entries.get(index)
    }

    /// Get summary statistics
    pub fn get_statistics(&self) -> SummaryStatistics {
        let header = &self.summary_data.header;
        let entries = &self.summary_data.entries;

        let avg_key_size = if !entries.is_empty() {
            entries.iter().map(|e| e.partition_key.len()).sum::<usize>() as f64
                / entries.len() as f64
        } else {
            0.0
        };

        SummaryStatistics {
            total_entries: entries.len(),
            min_index_interval: header.min_index_interval,
            sampling_level: header.sampling_level,
            size_at_full_sampling: header.size_at_full_sampling,
            average_key_size: avg_key_size,
            file_size: std::fs::metadata(&self.file_path)
                .map(|m| m.len())
                .unwrap_or(0),
        }
    }

    /// Validate summary integrity
    pub async fn validate_integrity(&self) -> Result<Vec<String>> {
        let mut issues = Vec::new();

        // Check if entries are sorted by position
        for i in 1..self.summary_data.entries.len() {
            let prev_pos = self.summary_data.entries[i - 1].position;
            let curr_pos = self.summary_data.entries[i].position;

            if prev_pos > curr_pos {
                issues.push(format!(
                    "Entries not sorted by position: entry {} has position {}, entry {} has position {}",
                    i - 1, prev_pos, i, curr_pos
                ));
            }
        }

        // Check entry count consistency
        if self.summary_data.entries.len() != self.summary_data.header.entries_count as usize {
            issues.push(format!(
                "Entry count mismatch: header says {}, but found {}",
                self.summary_data.header.entries_count,
                self.summary_data.entries.len()
            ));
        }

        Ok(issues)
    }
}

/// Summary statistics for analysis and validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryStatistics {
    /// Total number of summary entries
    pub total_entries: usize,
    /// Min index interval from header
    pub min_index_interval: u32,
    /// Sampling level from header
    pub sampling_level: u32,
    /// Size at full sampling from header
    pub size_at_full_sampling: u32,
    /// Average partition key size
    pub average_key_size: f64,
    /// File size in bytes
    pub file_size: u64,
}

/// Parse complete Summary.db file
fn parse_summary_data(input: &[u8]) -> Result<SummaryData> {
    if input.len() < SUMMARY_HEADER_SIZE {
        return Err(Error::corruption(format!(
            "Summary.db too small: {} bytes, need at least {} for header",
            input.len(),
            SUMMARY_HEADER_SIZE
        )));
    }

    // Parse header
    let (remaining, header) = parse_summary_header(input)
        .map_err(|e| Error::corruption(format!("Failed to parse Summary.db header: {:?}", e)))?;

    // Validate header
    if header.entries_count > MAX_REASONABLE_ENTRIES {
        return Err(Error::corruption(format!(
            "Summary.db entry count {} exceeds maximum {}",
            header.entries_count, MAX_REASONABLE_ENTRIES
        )));
    }

    // The remaining data should contain:
    // 1. Offset table: entries_count * 4 bytes (little-endian u32)
    // 2. Entry data: variable length
    // 3. First key: be_u32 size + data
    // 4. Last key: be_u32 size + data

    let offset_table_size = header.entries_count as usize * 4;

    if remaining.len() < offset_table_size {
        return Err(Error::corruption(format!(
            "Summary.db insufficient data for offset table: need {} bytes, have {}",
            offset_table_size,
            remaining.len()
        )));
    }

    // Parse offset table (LITTLE-ENDIAN!)
    let (after_offsets, offsets) = count(le_u32::<_, NomError<_>>, header.entries_count as usize)(
        remaining,
    )
    .map_err(|e: nom::Err<NomError<_>>| {
        Error::corruption(format!("Failed to parse offset table: {:?}", e))
    })?;

    // Calculate entry data size (total - offset table size)
    let entry_data_size = header.summary_entries_size as usize - offset_table_size;

    if after_offsets.len() < entry_data_size {
        return Err(Error::corruption(format!(
            "Summary.db insufficient entry data: need {} bytes, have {}",
            entry_data_size,
            after_offsets.len()
        )));
    }

    let entry_data = &after_offsets[..entry_data_size];
    let after_entries = &after_offsets[entry_data_size..];

    // Parse entries using offsets
    let entries = parse_entries_from_offsets(entry_data, &offsets, entry_data_size)?;

    // Parse first and last keys
    let (after_first, first_key) = parse_serialized_key(after_entries)
        .map_err(|e| Error::corruption(format!("Failed to parse first key: {:?}", e)))?;

    let (_, last_key) = parse_serialized_key(after_first)
        .map_err(|e| Error::corruption(format!("Failed to parse last key: {:?}", e)))?;

    Ok(SummaryData {
        header,
        entries,
        first_key,
        last_key,
    })
}

/// Parse Summary.db header (24 bytes, big-endian)
fn parse_summary_header(input: &[u8]) -> IResult<&[u8], SummaryHeader> {
    let (input, min_index_interval) = be_u32(input)?;
    let (input, entries_count) = be_u32(input)?;
    let (input, summary_entries_size) = be_u64(input)?;
    let (input, sampling_level) = be_u32(input)?;
    let (input, size_at_full_sampling) = be_u32(input)?;

    Ok((
        input,
        SummaryHeader {
            min_index_interval,
            entries_count,
            summary_entries_size,
            sampling_level,
            size_at_full_sampling,
        },
    ))
}

/// Parse entries using offset table
///
/// Each entry is: key_data (variable) + position (be_u64)
/// Key boundaries are determined by offset differences.
fn parse_entries_from_offsets(
    entry_data: &[u8],
    offsets: &[u32],
    total_size: usize,
) -> Result<Vec<SummaryEntry>> {
    let mut entries = Vec::with_capacity(offsets.len());

    for i in 0..offsets.len() {
        let start = offsets[i] as usize;

        // End is either the next offset or the total entry data size
        let end = if i + 1 < offsets.len() {
            offsets[i + 1] as usize
        } else {
            total_size
        };

        if start >= end {
            return Err(Error::corruption(format!(
                "Invalid offset at index {}: start {} >= end {}",
                i, start, end
            )));
        }

        if end > entry_data.len() {
            return Err(Error::corruption(format!(
                "Offset {} points beyond entry data (size {})",
                end,
                entry_data.len()
            )));
        }

        let entry_bytes = &entry_data[start..end];

        // Entry format: key_data + be_u64 position
        // Key length = entry length - 8 (for the position)
        if entry_bytes.len() < 8 {
            return Err(Error::corruption(format!(
                "Entry {} too small: {} bytes, need at least 8 for position",
                i,
                entry_bytes.len()
            )));
        }

        let key_len = entry_bytes.len() - 8;
        let partition_key = entry_bytes[..key_len].to_vec();

        // Parse position (last 8 bytes, big-endian)
        let position_bytes = &entry_bytes[key_len..];
        let position = u64::from_be_bytes([
            position_bytes[0],
            position_bytes[1],
            position_bytes[2],
            position_bytes[3],
            position_bytes[4],
            position_bytes[5],
            position_bytes[6],
            position_bytes[7],
        ]);

        entries.push(SummaryEntry {
            partition_key,
            position,
        });
    }

    Ok(entries)
}

/// Parse a length-prefixed key (be_u32 size + data)
fn parse_serialized_key(input: &[u8]) -> IResult<&[u8], Vec<u8>> {
    let (input, size) = be_u32(input)?;
    let (input, key_data) = take(size)(input)?;
    Ok((input, key_data.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summary_header_parsing() {
        // Real data pattern from Cassandra 5.0 Summary.db:
        // min_index_interval=128, entries_count=1, summary_entries_size=28,
        // sampling_level=128, size_at_full_sampling=1
        let data = vec![
            0x00, 0x00, 0x00, 0x80, // min_index_interval = 128
            0x00, 0x00, 0x00, 0x01, // entries_count = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1c, // summary_entries_size = 28
            0x00, 0x00, 0x00, 0x80, // sampling_level = 128
            0x00, 0x00, 0x00, 0x01, // size_at_full_sampling = 1
        ];

        let (remaining, header) = parse_summary_header(&data).unwrap();

        assert_eq!(header.min_index_interval, 128);
        assert_eq!(header.entries_count, 1);
        assert_eq!(header.summary_entries_size, 28);
        assert_eq!(header.sampling_level, 128);
        assert_eq!(header.size_at_full_sampling, 1);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_offset_table_little_endian() {
        // Offset table uses little-endian byte order
        // Two offsets: 0 and 24 (0x18)
        let offset_data: [u8; 8] = [
            0x00, 0x00, 0x00, 0x00, // offset[0] = 0 (LE)
            0x18, 0x00, 0x00, 0x00, // offset[1] = 24 (LE)
        ];

        let (_, offsets) = count(le_u32::<_, NomError<_>>, 2usize)(&offset_data[..]).unwrap();

        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1], 24);
    }

    #[test]
    fn test_entry_parsing_from_offsets() {
        // Entry data with one entry:
        // - Key: 16 bytes (partition key digest)
        // - Position: 8 bytes (be_u64)
        let key_bytes = vec![
            0xdc, 0x67, 0x26, 0xa6, 0x05, 0xc6, 0x48, 0x50, 0x86, 0xcd, 0x0f, 0xe3, 0x1b, 0x67,
            0x57, 0xaf,
        ];
        let position_bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; // position = 0

        let mut entry_data = key_bytes.clone();
        entry_data.extend_from_slice(&position_bytes);

        let offsets = vec![0u32];
        let entries = parse_entries_from_offsets(&entry_data, &offsets, entry_data.len()).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].partition_key, key_bytes);
        assert_eq!(entries[0].position, 0);
    }

    #[test]
    fn test_serialized_key_parsing() {
        // Length-prefixed key: be_u32(16) + 16 bytes of key data
        let data = vec![
            0x00, 0x00, 0x00, 0x10, // size = 16 (BE)
            0xdc, 0x67, 0x26, 0xa6, 0x05, 0xc6, 0x48, 0x50, 0x86, 0xcd, 0x0f, 0xe3, 0x1b, 0x67,
            0x57, 0xaf, // key data
        ];

        let (remaining, key) = parse_serialized_key(&data).unwrap();

        assert_eq!(key.len(), 16);
        assert_eq!(
            key,
            vec![
                0xdc, 0x67, 0x26, 0xa6, 0x05, 0xc6, 0x48, 0x50, 0x86, 0xcd, 0x0f, 0xe3, 0x1b, 0x67,
                0x57, 0xaf
            ]
        );
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_complete_summary_parsing() {
        // Complete Summary.db file with 1 entry:
        // Header (24 bytes) + Offset table (4 bytes) + Entry (24 bytes) + First key (20 bytes) + Last key (20 bytes)
        let mut data = vec![
            // Header (24 bytes)
            0x00, 0x00, 0x00, 0x80, // min_index_interval = 128
            0x00, 0x00, 0x00, 0x01, // entries_count = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x1c, // summary_entries_size = 28 (4 + 24)
            0x00, 0x00, 0x00, 0x80, // sampling_level = 128
            0x00, 0x00, 0x00, 0x01, // size_at_full_sampling = 1
        ];
        // Offset table (4 bytes, LE)
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // offset[0] = 0

        // Entry data (24 bytes): 16-byte key + 8-byte position
        let entry_key: [u8; 16] = [
            0xdc, 0x67, 0x26, 0xa6, 0x05, 0xc6, 0x48, 0x50, 0x86, 0xcd, 0x0f, 0xe3, 0x1b, 0x67,
            0x57, 0xaf,
        ];
        data.extend_from_slice(&entry_key);
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // position = 0

        // First key (20 bytes): be_u32(16) + 16 bytes
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]); // size = 16
        data.extend_from_slice(&entry_key);

        // Last key (20 bytes): be_u32(16) + 16 bytes
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]); // size = 16
        data.extend_from_slice(&entry_key);

        let summary = parse_summary_data(&data).unwrap();

        assert_eq!(summary.header.min_index_interval, 128);
        assert_eq!(summary.header.entries_count, 1);
        assert_eq!(summary.entries.len(), 1);
        assert_eq!(summary.entries[0].partition_key, entry_key.to_vec());
        assert_eq!(summary.entries[0].position, 0);
        assert_eq!(summary.first_key, entry_key.to_vec());
        assert_eq!(summary.last_key, entry_key.to_vec());
    }

    #[test]
    fn test_entry_position_sorted() {
        // Multiple entries should be sorted by position
        let mut data = vec![
            // Header (24 bytes)
            0x00, 0x00, 0x00, 0x80, // min_index_interval = 128
            0x00, 0x00, 0x00, 0x02, // entries_count = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x38, // summary_entries_size = 56 (8 + 48)
            0x00, 0x00, 0x00, 0x80, // sampling_level = 128
            0x00, 0x00, 0x00, 0x02, // size_at_full_sampling = 2
        ];
        // Offset table (8 bytes, LE)
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // offset[0] = 0
        data.extend_from_slice(&[0x18, 0x00, 0x00, 0x00]); // offset[1] = 24

        // Entry 0: 16-byte key + position 0
        let key0: [u8; 16] = [0x01; 16];
        data.extend_from_slice(&key0);
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);

        // Entry 1: 16-byte key + position 100
        let key1: [u8; 16] = [0x02; 16];
        data.extend_from_slice(&key1);
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]);

        // First key
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
        data.extend_from_slice(&key0);

        // Last key
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]);
        data.extend_from_slice(&key1);

        let summary = parse_summary_data(&data).unwrap();

        assert_eq!(summary.entries.len(), 2);
        assert_eq!(summary.entries[0].position, 0);
        assert_eq!(summary.entries[1].position, 100);
    }
}
