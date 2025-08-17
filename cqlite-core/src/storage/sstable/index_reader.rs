//! Index.db reader implementation for Cassandra 5+ SSTable format
//!
//! This module provides comprehensive parsing of Index.db files which contain
//! partition-level index information including promoted index entries for wide partitions.
//! The index is used for efficient partition lookups and range queries.

use crate::{
    error::{Error, Result},
    platform::Platform,
};
use nom::{
    IResult,
    bytes::complete::take,
    multi::count,
    number::complete::{be_u8, be_u16, be_u32, be_u64},
    sequence::tuple,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

/// Index.db file header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexHeader {
    /// Format version identifier
    pub version: u32,
    /// Number of index entries
    pub entry_count: u32,
    /// Size of the index data section
    pub data_size: u64,
    /// Checksum for validation
    pub checksum: u32,
}

/// Partition index entry in Index.db
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionIndexEntry {
    /// Partition key hash/digest
    pub key_digest: Vec<u8>,
    /// Offset in Data.db file
    pub data_offset: u64,
    /// Size of partition data
    pub data_size: u32,
    /// Promoted index entries for wide partitions (optional)
    pub promoted_index: Option<PromotedIndexData>,
}

/// Promoted index for wide partitions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotedIndexData {
    /// Number of promoted index entries
    pub entry_count: u32,
    /// Individual promoted index entries
    pub entries: Vec<PromotedIndexEntry>,
}

/// Individual promoted index entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotedIndexEntry {
    /// Clustering key prefix
    pub clustering_key: Vec<u8>,
    /// Offset within the partition
    pub partition_offset: u32,
    /// Size of the indexed section
    pub section_size: u32,
}

/// Complete Index.db data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexData {
    /// File header
    pub header: IndexHeader,
    /// All partition index entries
    pub partition_entries: Vec<PartitionIndexEntry>,
    /// Lookup table for efficient partition access
    pub key_lookup: HashMap<Vec<u8>, usize>,
}

/// High-level Index.db file reader
#[allow(dead_code)]
pub struct IndexReader {
    /// Path to the Index.db file
    file_path: PathBuf,
    /// Parsed index data
    index_data: IndexData,
    /// Platform abstraction for file operations
    platform: Arc<Platform>,
}

impl IndexReader {
    /// Open and parse an Index.db file
    pub async fn open(path: &Path, platform: Arc<Platform>) -> Result<Self> {
        if !platform.fs().exists(path).await? {
            return Err(Error::not_found(format!(
                "Index.db file not found: {}",
                path.display()
            )));
        }

        // Read the entire file
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        // Parse the index data
        let index_data = match parse_index_data(&buffer) {
            Ok((_, data)) => data,
            Err(e) => {
                return Err(Error::corruption(format!(
                    "Failed to parse Index.db: {:?}",
                    e
                )));
            }
        };

        Ok(Self {
            file_path: path.to_path_buf(),
            index_data,
            platform,
        })
    }

    /// Get all partition entries
    pub fn get_partition_entries(&self) -> &[PartitionIndexEntry] {
        &self.index_data.partition_entries
    }

    /// Look up a partition by key digest
    pub fn lookup_partition(&self, key_digest: &[u8]) -> Option<&PartitionIndexEntry> {
        self.index_data
            .key_lookup
            .get(key_digest)
            .and_then(|&index| self.index_data.partition_entries.get(index))
    }

    /// Get statistics about the index
    pub fn get_statistics(&self) -> IndexStatistics {
        let mut promoted_count = 0;
        let mut total_promoted_entries = 0;

        for entry in &self.index_data.partition_entries {
            if let Some(ref promoted) = entry.promoted_index {
                promoted_count += 1;
                total_promoted_entries += promoted.entry_count as usize;
            }
        }

        IndexStatistics {
            total_partitions: self.index_data.partition_entries.len(),
            partitions_with_promoted_index: promoted_count,
            total_promoted_entries,
            file_size: self.file_path.metadata().map(|m| m.len()).unwrap_or(0),
        }
    }

    /// Validate index integrity against Data.db offsets
    pub async fn validate_integrity(&self) -> Result<Vec<String>> {
        let mut issues = Vec::new();

        // Check for overlapping offsets
        let mut offsets: Vec<_> = self
            .index_data
            .partition_entries
            .iter()
            .map(|e| (e.data_offset, e.data_size))
            .collect();

        offsets.sort_by_key(|&(offset, _)| offset);

        for i in 1..offsets.len() {
            let (prev_offset, prev_size) = offsets[i - 1];
            let (curr_offset, _) = offsets[i];

            if prev_offset + prev_size as u64 > curr_offset {
                issues.push(format!(
                    "Overlapping partitions: offset {} + size {} overlaps with offset {}",
                    prev_offset, prev_size, curr_offset
                ));
            }
        }

        Ok(issues)
    }
}

/// Index statistics for analysis and validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    /// Total number of partitions
    pub total_partitions: usize,
    /// Number of partitions with promoted index
    pub partitions_with_promoted_index: usize,
    /// Total number of promoted index entries
    pub total_promoted_entries: usize,
    /// File size in bytes
    pub file_size: u64,
}

/// Parse Index.db file data
fn parse_index_data(input: &[u8]) -> IResult<&[u8], IndexData> {
    let (input, header) = parse_index_header(input)?;
    let (input, partition_entries) =
        count(parse_partition_index_entry, header.entry_count as usize)(input)?;

    // Build lookup table
    let mut key_lookup = HashMap::new();
    for (index, entry) in partition_entries.iter().enumerate() {
        key_lookup.insert(entry.key_digest.clone(), index);
    }

    Ok((
        input,
        IndexData {
            header,
            partition_entries,
            key_lookup,
        },
    ))
}

/// Parse Index.db header
fn parse_index_header(input: &[u8]) -> IResult<&[u8], IndexHeader> {
    let (input, (version, entry_count, data_size, checksum)) =
        tuple((be_u32, be_u32, be_u64, be_u32))(input)?;

    Ok((
        input,
        IndexHeader {
            version,
            entry_count,
            data_size,
            checksum,
        },
    ))
}

/// Parse a single partition index entry
fn parse_partition_index_entry(input: &[u8]) -> IResult<&[u8], PartitionIndexEntry> {
    // Parse key digest length and data
    let (input, digest_len) = be_u16(input)?;
    let (input, key_digest) = take(digest_len)(input)?;

    // Parse data offset and size
    let (input, data_offset) = be_u64(input)?;
    let (input, data_size) = be_u32(input)?;

    // Check for promoted index marker
    let (input, has_promoted) = be_u8(input)?;
    let (input, promoted_index) = if has_promoted != 0 {
        let (input, promoted) = parse_promoted_index(input)?;
        (input, Some(promoted))
    } else {
        (input, None)
    };

    Ok((
        input,
        PartitionIndexEntry {
            key_digest: key_digest.to_vec(),
            data_offset,
            data_size,
            promoted_index,
        },
    ))
}

/// Parse promoted index data for wide partitions
fn parse_promoted_index(input: &[u8]) -> IResult<&[u8], PromotedIndexData> {
    let (input, entry_count) = be_u32(input)?;
    let (input, entries) = count(parse_promoted_index_entry, entry_count as usize)(input)?;

    Ok((
        input,
        PromotedIndexData {
            entry_count,
            entries,
        },
    ))
}

/// Parse a single promoted index entry
fn parse_promoted_index_entry(input: &[u8]) -> IResult<&[u8], PromotedIndexEntry> {
    // Parse clustering key length and data
    let (input, key_len) = be_u16(input)?;
    let (input, clustering_key) = take(key_len)(input)?;

    // Parse offset and size within partition
    let (input, partition_offset) = be_u32(input)?;
    let (input, section_size) = be_u32(input)?;

    Ok((
        input,
        PromotedIndexEntry {
            clustering_key: clustering_key.to_vec(),
            partition_offset,
            section_size,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_header_parsing() {
        let data = vec![
            0x00, 0x00, 0x00, 0x01, // version = 1
            0x00, 0x00, 0x00, 0x0A, // entry_count = 10
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, // data_size = 1024
            0x12, 0x34, 0x56, 0x78, // checksum
        ];

        let (_, header) = parse_index_header(&data).unwrap();

        assert_eq!(header.version, 1);
        assert_eq!(header.entry_count, 10);
        assert_eq!(header.data_size, 1024);
        assert_eq!(header.checksum, 0x12345678);
    }

    #[test]
    fn test_partition_index_entry_parsing() {
        let data = vec![
            0x00, 0x08, // digest_len = 8
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // data_offset = 4096
            0x00, 0x00, 0x02, 0x00, // data_size = 512
            0x00, // has_promoted = false
        ];

        let (_, entry) = parse_partition_index_entry(&data).unwrap();

        assert_eq!(entry.key_digest, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(entry.data_offset, 4096);
        assert_eq!(entry.data_size, 512);
        assert!(entry.promoted_index.is_none());
    }
}
