//! Index.db reader implementation for Cassandra 5+ SSTable format
//!
//! This module provides comprehensive parsing of Index.db files which contain
//! partition-level index information including promoted index entries for wide partitions.
//! The index is used for efficient partition lookups and range queries.

use crate::{
    error::{Error, Result},
    platform::Platform,
};

use super::header_spec::get_global_registry;
use nom::{bytes::complete::take, number::complete::be_u16, IResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use super::summary_reader::SummaryReader;

/// Size of each index entry in Cassandra 5.x BIG format (2-byte marker + 16-byte token digest)
const INDEX_ENTRY_SIZE: usize = 18;

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
#[derive(Debug, Clone)]
pub struct PartitionIndexEntry {
    /// Partition key hash/digest - using Arc to enable zero-copy sharing in lookup tables
    /// This eliminates memory explosion from cloning large numbers of partition digests
    pub key_digest: Arc<[u8]>,
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
#[derive(Debug, Clone)]
pub struct IndexData {
    /// File header
    pub header: IndexHeader,
    /// All partition index entries
    pub partition_entries: Vec<PartitionIndexEntry>,
    /// Lookup table for efficient partition access - uses Arc<[u8]> as key type
    ///
    /// ## Zero-Copy Design (Issue #107, Problem 1)
    ///
    /// - Keys are `Arc<[u8]>` to enable reference counting without cloning digest bytes
    /// - Lookups use `&[u8]` directly via Borrow trait (zero heap allocations)
    /// - `Arc<[u8]>` implements `Borrow<[u8]>` enabling HashMap::get(&[u8]) without temporary Arc creation
    pub key_lookup: HashMap<Arc<[u8]>, usize>,
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
        Self::open_with_summary(path, platform, None).await
    }

    /// Open and parse an Index.db file with Summary.db correlation for proper offset mapping
    pub async fn open_with_summary(
        path: &Path,
        platform: Arc<Platform>,
        summary_reader: Option<&SummaryReader>,
    ) -> Result<Self> {
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

        // Check for empty file
        if buffer.is_empty() {
            return Err(Error::corruption(format!(
                "Index.db file is empty: {}",
                path.display()
            )));
        }

        // Parse the index data with optional Summary.db correlation
        let index_data = match parse_index_data_with_summary(&buffer, summary_reader) {
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
    ///
    /// ## Zero-Allocation Optimization (Issue #107)
    ///
    /// This method performs HashMap lookup without heap allocation by leveraging
    /// the `Borrow` trait. Since `Arc<[u8]>` implements `Borrow<[u8]>`, we can
    /// lookup using `&[u8]` directly without creating a temporary Arc.
    ///
    /// **Before:** `let key_arc: Arc<[u8]> = key_digest.into();` (heap allocation per query)
    /// **After:** Direct `get(key_digest)` using Borrow trait (zero allocations)
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

/// Parse Index.db file data with optional Summary.db correlation using spec-driven approach
fn parse_index_data_with_summary<'a>(
    input: &'a [u8],
    summary_reader: Option<&SummaryReader>,
) -> IResult<&'a [u8], IndexData> {
    use nom::error::{Error as NomError, ErrorKind};

    // First try spec-driven header parsing
    let registry = get_global_registry();
    let (remaining, header) = match registry.parse_index_header(input) {
        Ok(parsed_header) => {
            log::debug!("Successfully parsed Index.db header using spec-driven approach");

            // Convert ParsedHeader to IndexHeader
            let header = IndexHeader {
                version: parsed_header
                    .fields
                    .get("version")
                    .and_then(|v| v.as_u32().ok())
                    .unwrap_or(1),
                entry_count: parsed_header
                    .fields
                    .get("entry_count")
                    .and_then(|v| v.as_u32().ok())
                    .unwrap_or(0),
                data_size: parsed_header
                    .fields
                    .get("data_size")
                    .and_then(|v| v.as_u64().ok())
                    .unwrap_or(input.len() as u64),
                checksum: parsed_header
                    .fields
                    .get("checksum")
                    .and_then(|v| v.as_u32().ok())
                    .unwrap_or(0),
            };

            // Skip header bytes for data parsing
            let header_size = parsed_header.header_size;
            if input.len() < header_size {
                return Err(nom::Err::Error(NomError::new(input, ErrorKind::Eof)));
            }
            (&input[header_size..], header)
        }
        Err(_) => {
            log::debug!("Spec-driven header parsing failed, assuming headerless format");

            // Parse all partition key digests - no header in some formats
            let header = IndexHeader {
                version: 1,
                entry_count: 0, // Will be updated after parsing entries
                data_size: input.len() as u64,
                checksum: 0,
            };
            (input, header)
        }
    };

    // Parse partition entries from remaining data
    let (remaining, partition_entries) =
        parse_all_partition_keys_with_summary(remaining, summary_reader)?;

    // Build lookup table with zero-copy approach using Arc::clone (reference counting only)
    // This eliminates the memory explosion from cloning Vec<u8> key digests
    let mut key_lookup = HashMap::new();
    for (index, entry) in partition_entries.iter().enumerate() {
        key_lookup.insert(Arc::clone(&entry.key_digest), index);
    }

    // Update header with actual entry count
    let header = IndexHeader {
        entry_count: partition_entries.len() as u32,
        ..header
    };

    Ok((
        remaining,
        IndexData {
            header,
            partition_entries,
            key_lookup,
        },
    ))
}

/// Parse all partition key digests from the Index.db file with Summary.db correlation
fn parse_all_partition_keys_with_summary<'a>(
    input: &'a [u8],
    summary_reader: Option<&SummaryReader>,
) -> IResult<&'a [u8], Vec<PartitionIndexEntry>> {
    let mut entries = Vec::new();
    let mut remaining = input;

    // Parse entries until we consume all input
    let mut entry_index = 0;
    while !remaining.is_empty() {
        match parse_simple_partition_key_with_offset(remaining, entry_index, summary_reader) {
            Ok((rest, entry)) => {
                entries.push(entry);
                remaining = rest;
                entry_index += 1;
            }
            Err(_) => {
                // Stop parsing if we can't parse more entries
                break;
            }
        }
    }

    Ok((remaining, entries))
}

/// Parse a single partition key from the Index.db format with proper offset calculation
fn parse_simple_partition_key_with_offset<'a>(
    input: &'a [u8],
    entry_index: usize,
    summary_reader: Option<&SummaryReader>,
) -> IResult<&'a [u8], PartitionIndexEntry> {
    // Parse simple format only: 00 10 followed by 16-byte key digest
    // The "enhanced format" check was triggering false positives and has been removed
    // per Issue #92 - we rely on Summary.db for offsets, not inline data
    let (input, _marker) = be_u16(input)?; // Should be 0x0010
    let (input, key_digest) = take(16_u8)(input)?; // Fixed 16-byte key digest

    // Calculate data offset using Summary.db correlation if available
    let (data_offset, data_size) = if let Some(summary) = summary_reader {
        calculate_data_offset_from_summary(summary, entry_index)
    } else {
        // Without Summary.db, we cannot determine offsets accurately (Issue #92)
        // Return 0 to indicate unknown offset - callers must handle this case
        log::warn!(
            "Index.db parsed without Summary.db - offsets unavailable (entry_index={})",
            entry_index
        );
        (0, 0)
    };

    Ok((
        input,
        PartitionIndexEntry {
            key_digest: Arc::from(key_digest), // Convert to Arc to avoid copying
            data_offset,
            data_size,
            promoted_index: None, // Not available in simple format
        },
    ))
}

// REMOVED: try_parse_enhanced_partition_entry
// The "enhanced format" with inline offsets was causing false positives
// Issue #92 mandates using Summary.db for offset correlation, not inline heuristics

/// Calculate actual Data.db offset using Summary.db correlation
///
/// ## Spec-Accurate Implementation (Issue #92)
///
/// Summary.db entries contain the actual Data.db offsets in the `position` field.
/// Index.db contains a flat list of partition tokens in order.
///
/// Algorithm:
/// 1. Summary.db samples contain: (token, index_offset, **position**)
/// 2. `index_offset` = byte position in Index.db where this token appears
/// 3. `position` = actual Data.db file offset for this partition
/// 4. For Index.db entries between Summary samples, interpolate based on file positions
fn calculate_data_offset_from_summary(
    summary_reader: &SummaryReader,
    entry_index: usize,
) -> (u64, u32) {
    let entries = summary_reader.get_entries();

    if entries.is_empty() {
        return (0, 0);
    }

    // Calculate Index.db byte position for this entry
    let index_byte_position = (entry_index * INDEX_ENTRY_SIZE) as u64;

    // Strategy 1: Find exact match by index_offset
    if let Some(exact_match) = entries
        .iter()
        .find(|e| e.index_offset == index_byte_position)
    {
        // Exact match - use the position field directly from Summary.db
        return (exact_match.position as u64, 0);
    }

    // Strategy 2: Find surrounding Summary entries and interpolate
    let mut prev_entry = None;
    let mut next_entry = None;

    for entry in entries {
        if entry.index_offset <= index_byte_position {
            prev_entry = Some(entry);
        }
        if entry.index_offset > index_byte_position && next_entry.is_none() {
            next_entry = Some(entry);
            break;
        }
    }

    match (prev_entry, next_entry) {
        (Some(prev), Some(next)) => {
            // Interpolate between two Summary samples
            let index_range = next.index_offset - prev.index_offset;
            let data_range = next.position.saturating_sub(prev.position) as u64;
            let index_delta = index_byte_position - prev.index_offset;

            let ratio = if index_range > 0 {
                index_delta as f64 / index_range as f64
            } else {
                0.0
            };

            let interpolated_offset = prev.position as u64 + (data_range as f64 * ratio) as u64;
            (interpolated_offset, 0)
        }
        (Some(prev), None) => {
            // Past last sample - use last known position (conservative)
            (prev.position as u64, 0)
        }
        (None, Some(next)) => {
            // Before first sample - use first position
            (next.position as u64, 0)
        }
        (None, None) => {
            // No Summary entries - should not happen with valid Summary.db
            (0, 0)
        }
    }
}

// REMOVED: Old heuristic functions that violated Issue #28 no-heuristics mandate
// - interpolate_data_offset_from_summary_position: Used arbitrary estimates
// - estimate_data_offset_from_index_position: Used hardcoded partition size guesses
//
// These are replaced by spec-accurate calculate_data_offset_from_summary() which uses
// actual Summary.db position fields and Index.db byte offsets for interpolation.

/// Parse Index.db file data - Legacy API for backward compatibility
#[allow(dead_code)]
fn parse_index_data(input: &[u8]) -> IResult<&[u8], IndexData> {
    parse_index_data_with_summary(input, None)
}

/// Parse all partition key digests from the Index.db file - Legacy API
#[allow(dead_code)]
fn parse_all_partition_keys(input: &[u8]) -> IResult<&[u8], Vec<PartitionIndexEntry>> {
    parse_all_partition_keys_with_summary(input, None)
}

/// Parse a single partition key from the simple Index.db format - Legacy API
#[allow(dead_code)]
fn parse_simple_partition_key(input: &[u8]) -> IResult<&[u8], PartitionIndexEntry> {
    parse_simple_partition_key_with_offset(input, 0, None)
}

// Note: Promoted index parsing removed as it's not present in the simple Index.db format
// Real Cassandra 5 Index.db files only contain partition key digests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_partition_key_parsing() {
        let data = vec![
            0x00, 0x10, // marker = 0x0010
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest (16 bytes)
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
        ];

        let (_, entry) = parse_simple_partition_key(&data).unwrap();

        assert_eq!(
            entry.key_digest.as_ref(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        // Without Summary.db, offset is 0 (Issue #92 - no heuristics)
        assert_eq!(entry.data_offset, 0);
        assert_eq!(entry.data_size, 0); // Size still not available in simple format
        assert!(entry.promoted_index.is_none());
    }

    #[test]
    fn test_partition_key_parsing_without_summary() {
        let data = vec![
            0x00, 0x10, // marker = 0x0010
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest (16 bytes)
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
        ];

        // Test with different entry indices - without Summary.db, offsets should be 0
        let (_, entry0) = parse_simple_partition_key_with_offset(&data, 0, None).unwrap();
        let (_, entry1) = parse_simple_partition_key_with_offset(&data, 1, None).unwrap();
        let (_, entry5) = parse_simple_partition_key_with_offset(&data, 5, None).unwrap();

        assert_eq!(
            entry0.key_digest.as_ref(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );

        // Without Summary.db, offsets are 0 (Issue #92 - no heuristics)
        assert_eq!(entry0.data_offset, 0);
        assert_eq!(entry1.data_offset, 0);
        assert_eq!(entry5.data_offset, 0);

        // All should have the same key digest in this test
        assert_eq!(entry0.key_digest.as_ref(), entry1.key_digest.as_ref());
        assert_eq!(entry1.key_digest.as_ref(), entry5.key_digest.as_ref());
    }

    // REMOVED: test_enhanced_partition_entry_parsing
    // Enhanced format parsing removed per Issue #92

    #[test]
    fn test_multiple_partition_keys_parsing() {
        let data = vec![
            0x00, 0x10, // marker = 0x0010
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest 1 (16 bytes)
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x00, 0x10, // marker = 0x0010
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // key_digest 2 (16 bytes)
            0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
        ];

        let (_, entries) = parse_all_partition_keys(&data).unwrap();

        assert_eq!(entries.len(), 2);

        if !entries.is_empty() {
            assert_eq!(
                entries[0].key_digest.as_ref(),
                &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
            );
        }

        if entries.len() >= 2 {
            assert_eq!(
                entries[1].key_digest.as_ref(),
                &[
                    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
                    0x1E, 0x1F, 0x20
                ]
            );

            // Without Summary.db, both offsets are 0 (Issue #92 - no heuristics)
            assert_eq!(entries[0].data_offset, 0);
            assert_eq!(entries[1].data_offset, 0);
        }
    }

    // REMOVED: test_data_offset_estimation_algorithm
    // This test validated the old heuristic estimation function which has been removed
    // in favor of spec-accurate Summary.db correlation (Issue #92)

    #[test]
    fn test_borrow_trait_zero_allocation_lookup() {
        // Test Issue #107 fix: Verify that lookup_partition uses Borrow trait
        // to avoid heap allocation on every lookup

        // Create index data with two partition entries
        let data = vec![
            0x00, 0x10, // marker = 0x0010
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest 1
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x00, 0x10, // marker = 0x0010
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // key_digest 2
            0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
        ];

        let (_, index_data) = parse_index_data(&data).unwrap();

        // Prepare lookup keys as slices (NOT Arc)
        let key1: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let key2: &[u8] = &[
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
            0x1F, 0x20,
        ];
        let key_not_found: &[u8] = &[0xFF; 16];

        // Test lookups - these should use Borrow trait without creating Arc
        // The key_lookup HashMap has Arc<[u8]> keys but accepts &[u8] for get()
        let result1 = index_data.key_lookup.get(key1);
        let result2 = index_data.key_lookup.get(key2);
        let result3 = index_data.key_lookup.get(key_not_found);

        assert!(result1.is_some(), "Should find first key");
        assert!(result2.is_some(), "Should find second key");
        assert!(result3.is_none(), "Should not find non-existent key");

        assert_eq!(*result1.unwrap(), 0, "First key should map to index 0");
        assert_eq!(*result2.unwrap(), 1, "Second key should map to index 1");

        // Verify the actual entries match
        assert_eq!(index_data.partition_entries[0].key_digest.as_ref(), key1);
        assert_eq!(index_data.partition_entries[1].key_digest.as_ref(), key2);
    }
}
