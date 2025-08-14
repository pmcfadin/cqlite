//! Summary.db reader implementation for Cassandra 5+ SSTable format
//!
//! This module provides comprehensive parsing of Summary.db files which contain
//! sampled partition keys and their corresponding index offsets for efficient
//! range queries and partition boundary detection.

use crate::{
    error::{Error, Result},
    platform::Platform,
};
use nom::{
    bytes::complete::take,
    multi::count,
    number::complete::{be_u16, be_u32, be_u64, be_i64},
    sequence::tuple,
    IResult,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

/// Summary.db file header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryHeader {
    /// Format version identifier
    pub version: u32,
    /// Number of summary entries
    pub entry_count: u32,
    /// Sampling rate (how many partitions between samples)
    pub sampling_rate: u32,
    /// Minimum token value in the SSTable
    pub min_token: i64,
    /// Maximum token value in the SSTable
    pub max_token: i64,
    /// Size of the summary data section
    pub data_size: u64,
    /// Checksum for validation
    pub checksum: u32,
}

/// Summary entry representing a sampled partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryEntry {
    /// Sampled partition key
    pub partition_key: Vec<u8>,
    /// Token value for this partition
    pub token: i64,
    /// Offset in Index.db file
    pub index_offset: u64,
    /// Position within the SSTable (for ordering)
    pub position: u32,
}

/// Complete Summary.db data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryData {
    /// File header
    pub header: SummaryHeader,
    /// All summary entries (sorted by token)
    pub entries: Vec<SummaryEntry>,
    /// Token range lookup for efficient range queries
    pub token_ranges: Vec<TokenRange>,
}

/// Token range for efficient lookup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRange {
    /// Start token (inclusive)
    pub start_token: i64,
    /// End token (exclusive)
    pub end_token: i64,
    /// Index of first entry in this range
    pub first_entry_index: usize,
    /// Number of entries in this range
    pub entry_count: usize,
}

/// High-level Summary.db file reader
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
        let summary_data = match parse_summary_data(&buffer) {
            Ok((_, data)) => data,
            Err(e) => {
                return Err(Error::corruption(format!(
                    "Failed to parse Summary.db: {:?}",
                    e
                )));
            }
        };

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

    /// Find entries within a token range
    pub fn find_entries_in_range(&self, start_token: i64, end_token: i64) -> Vec<&SummaryEntry> {
        self.summary_data
            .entries
            .iter()
            .filter(|entry| entry.token >= start_token && entry.token < end_token)
            .collect()
    }

    /// Find the best summary entry for a given token
    pub fn find_best_entry_for_token(&self, token: i64) -> Option<&SummaryEntry> {
        // Binary search for the entry with the largest token <= target token
        let mut left = 0;
        let mut right = self.summary_data.entries.len();
        let mut best_entry = None;

        while left < right {
            let mid = left + (right - left) / 2;
            let entry = &self.summary_data.entries[mid];

            if entry.token <= token {
                best_entry = Some(entry);
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        best_entry
    }

    /// Get token range information
    pub fn get_token_ranges(&self) -> &[TokenRange] {
        &self.summary_data.token_ranges
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
            sampling_rate: header.sampling_rate,
            token_range_span: header.max_token - header.min_token,
            min_token: header.min_token,
            max_token: header.max_token,
            average_key_size: avg_key_size,
            file_size: self.file_path.metadata().map(|m| m.len()).unwrap_or(0),
        }
    }

    /// Validate summary integrity
    pub async fn validate_integrity(&self) -> Result<Vec<String>> {
        let mut issues = Vec::new();

        // Check if entries are sorted by token
        for i in 1..self.summary_data.entries.len() {
            let prev_token = self.summary_data.entries[i - 1].token;
            let curr_token = self.summary_data.entries[i].token;
            
            if prev_token > curr_token {
                issues.push(format!(
                    "Entries not sorted by token: entry {} has token {}, entry {} has token {}",
                    i - 1, prev_token, i, curr_token
                ));
            }
        }

        // Check token range consistency
        let header = &self.summary_data.header;
        if let (Some(first), Some(last)) = (
            self.summary_data.entries.first(),
            self.summary_data.entries.last(),
        ) {
            if first.token < header.min_token {
                issues.push(format!(
                    "First entry token {} is less than header min_token {}",
                    first.token, header.min_token
                ));
            }
            
            if last.token > header.max_token {
                issues.push(format!(
                    "Last entry token {} is greater than header max_token {}",
                    last.token, header.max_token
                ));
            }
        }

        Ok(issues)
    }
}

/// Summary statistics for analysis and validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryStatistics {
    /// Total number of summary entries
    pub total_entries: usize,
    /// Sampling rate from header
    pub sampling_rate: u32,
    /// Token range span (max - min)
    pub token_range_span: i64,
    /// Minimum token value
    pub min_token: i64,
    /// Maximum token value
    pub max_token: i64,
    /// Average partition key size
    pub average_key_size: f64,
    /// File size in bytes
    pub file_size: u64,
}

/// Parse Summary.db file data
fn parse_summary_data(input: &[u8]) -> IResult<&[u8], SummaryData> {
    let (input, header) = parse_summary_header(input)?;
    let (input, entries) = count(parse_summary_entry, header.entry_count as usize)(input)?;

    // Build token ranges for efficient lookup
    let token_ranges = build_token_ranges(&entries, header.sampling_rate);

    Ok((
        input,
        SummaryData {
            header,
            entries,
            token_ranges,
        },
    ))
}

/// Parse Summary.db header
fn parse_summary_header(input: &[u8]) -> IResult<&[u8], SummaryHeader> {
    let (input, (version, entry_count, sampling_rate)) = tuple((be_u32, be_u32, be_u32))(input)?;
    let (input, (min_token, max_token)) = tuple((be_i64, be_i64))(input)?;
    let (input, (data_size, checksum)) = tuple((be_u64, be_u32))(input)?;

    Ok((
        input,
        SummaryHeader {
            version,
            entry_count,
            sampling_rate,
            min_token,
            max_token,
            data_size,
            checksum,
        },
    ))
}

/// Parse a single summary entry
fn parse_summary_entry(input: &[u8]) -> IResult<&[u8], SummaryEntry> {
    // Parse partition key length and data
    let (input, key_len) = be_u16(input)?;
    let (input, partition_key) = take(key_len)(input)?;
    
    // Parse token, index offset, and position
    let (input, token) = be_i64(input)?;
    let (input, index_offset) = be_u64(input)?;
    let (input, position) = be_u32(input)?;

    Ok((
        input,
        SummaryEntry {
            partition_key: partition_key.to_vec(),
            token,
            index_offset,
            position,
        },
    ))
}

/// Build token ranges for efficient lookup
fn build_token_ranges(entries: &[SummaryEntry], _sampling_rate: u32) -> Vec<TokenRange> {
    if entries.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let chunk_size = (entries.len() / 10).max(1); // Aim for ~10 ranges
    
    for (i, chunk) in entries.chunks(chunk_size).enumerate() {
        if let (Some(first), Some(_last)) = (chunk.first(), chunk.last()) {
            ranges.push(TokenRange {
                start_token: first.token,
                end_token: if i == entries.len() / chunk_size - 1 {
                    i64::MAX // Last range goes to infinity
                } else {
                    entries
                        .get((i + 1) * chunk_size)
                        .map(|e| e.token)
                        .unwrap_or(i64::MAX)
                },
                first_entry_index: i * chunk_size,
                entry_count: chunk.len(),
            });
        }
    }

    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_summary_header_parsing() {
        let data = vec![
            0x00, 0x00, 0x00, 0x01, // version = 1
            0x00, 0x00, 0x00, 0x64, // entry_count = 100
            0x00, 0x00, 0x00, 0x0A, // sampling_rate = 10
            0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // min_token = -9223372036854775808
            0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // max_token = 9223372036854775807
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // data_size = 4096
            0x12, 0x34, 0x56, 0x78, // checksum
        ];

        let (_, header) = parse_summary_header(&data).unwrap();
        
        assert_eq!(header.version, 1);
        assert_eq!(header.entry_count, 100);
        assert_eq!(header.sampling_rate, 10);
        assert_eq!(header.min_token, i64::MIN);
        assert_eq!(header.max_token, i64::MAX);
        assert_eq!(header.data_size, 4096);
        assert_eq!(header.checksum, 0x12345678);
    }

    #[test]
    fn test_summary_entry_parsing() {
        let data = vec![
            0x00, 0x08, // key_len = 8
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // partition_key
            0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x56, 0x78, // token = 305419896
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // index_offset = 4096
            0x00, 0x00, 0x00, 0x05, // position = 5
        ];

        let (_, entry) = parse_summary_entry(&data).unwrap();
        
        assert_eq!(entry.partition_key, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(entry.token, 305419896);
        assert_eq!(entry.index_offset, 4096);
        assert_eq!(entry.position, 5);
    }

    #[test]
    fn test_token_range_building() {
        let entries = vec![
            SummaryEntry {
                partition_key: vec![1],
                token: -1000,
                index_offset: 100,
                position: 0,
            },
            SummaryEntry {
                partition_key: vec![2],
                token: 0,
                index_offset: 200,
                position: 1,
            },
            SummaryEntry {
                partition_key: vec![3],
                token: 1000,
                index_offset: 300,
                position: 2,
            },
        ];

        let ranges = build_token_ranges(&entries, 10);
        
        assert!(!ranges.is_empty());
        assert_eq!(ranges[0].start_token, -1000);
        assert_eq!(ranges[0].first_entry_index, 0);
    }
}