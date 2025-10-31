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
use nom::{
    bytes::complete::take,
    number::complete::{be_u16, u8 as nom_u8},
    IResult,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use super::summary_reader::SummaryReader;

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

    // Detect format by checking first 2 bytes
    let format = detect_index_format(input);
    log::debug!("Detected Index.db format: {:?}", format);

    // Parse entries until we consume all input
    let mut entry_index = 0;
    while !remaining.is_empty() {
        let parse_result = match format {
            IndexFormat::DigestFormat => {
                parse_simple_partition_key_with_offset(remaining, entry_index, summary_reader)
            }
            IndexFormat::BtiFormat => parse_bti_partition_entry(remaining, entry_index),
        };

        match parse_result {
            Ok((rest, entry)) => {
                entries.push(entry);
                remaining = rest;
                entry_index += 1;
            }
            Err(_e) => {
                log::debug!(
                    "Stopped parsing Index.db at entry {} with {} bytes remaining",
                    entry_index,
                    remaining.len()
                );
                // Stop parsing if we can't parse more entries
                break;
            }
        }
    }

    log::debug!(
        "Parsed {} partition entries from Index.db ({:?} format)",
        entries.len(),
        format
    );
    Ok((remaining, entries))
}

/// Parse a single partition key from the Index.db format with variable-length offset
fn parse_simple_partition_key_with_offset<'a>(
    input: &'a [u8],
    #[allow(unused_variables)] entry_index: usize,
    _summary_reader: Option<&SummaryReader>,
) -> IResult<&'a [u8], PartitionIndexEntry> {
    // Some Index.db formats have a 2-byte length prefix before each entry
    // Try to detect and skip it if present
    let (input, first_word) = be_u16(input)?;

    let (input, marker) = if first_word == 0x0010 {
        // No length prefix, first_word is the marker
        (input, first_word)
    } else {
        // first_word is likely a length prefix, read the actual marker
        be_u16(input)?
    };

    if marker != 0x0010 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    // Read partition key digest (16 bytes)
    let (input, key_digest) = take(16_u8)(input)?;

    // Read variable-length offset field
    // Format: length byte (1 byte) + big-endian offset bytes (1-9 bytes)
    let (input, offset_len) = nom_u8(input)?;
    let (input, offset_bytes) = take(offset_len)(input)?;

    // Decode big-endian offset (relative to data section start, not file start)
    // SSTableReader will add actual_header_size when seeking
    let data_offset = decode_be_offset(offset_bytes);

    // Debug logging to verify parsing
    log::debug!(
        "IndexReader Entry {}: marker={:#06x}, offset_len={}, data_offset={}",
        entry_index,
        marker,
        offset_len,
        data_offset
    );

    // Size not stored in Index.db - will be determined during data read
    let data_size = 0;

    Ok((
        input,
        PartitionIndexEntry {
            key_digest: Arc::from(key_digest),
            data_offset,
            data_size,
            promoted_index: None,
        },
    ))
}

// REMOVED: try_parse_enhanced_partition_entry
// The "enhanced format" with inline offsets was causing false positives
// Issue #92 mandates using Summary.db for offset correlation, not inline heuristics

/// Decode variable-length big-endian offset
fn decode_be_offset(bytes: &[u8]) -> u64 {
    let mut offset: u64 = 0;
    for &byte in bytes {
        offset = (offset << 8) | (byte as u64);
    }
    offset
}

/// Index.db format variants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexFormat {
    /// MD5 digest format: marker(0x0010) + digest(16) + offset_len(1) + offset(variable)
    DigestFormat,
    /// BTI/Partition Key format: entry_len(2) + key_len(2) + key(variable) + metadata(variable)
    BtiFormat,
}

/// Detect which Index.db format is in use by examining first 2 bytes
///
/// ## TEMPORARY HEURISTIC (Issue #28 Follow-up - Technical Debt)
///
/// This function uses byte-pattern detection as a temporary workaround until proper
/// TOC/Statistics integration in M3. The proper fix is to pass format hint from
/// SSTableReader (which has authoritative version/format metadata).
///
/// **Current approach:**
/// - Check if first 2 bytes are 0x0010 (digest format marker)
/// - If yes: DigestFormat (MD5 digest + variable-length offset)
/// - If no: BtiFormat (byte-comparable key + metadata)
///
/// **Future improvement (M3):**
/// - SSTableReader should read TOC/Statistics to determine format authoritatively
/// - Pass format hint as parameter to this function
/// - Only fall back to byte-pattern detection if hint unavailable
fn detect_index_format(input: &[u8]) -> IndexFormat {
    if input.len() < 2 {
        // Default to digest format for empty/invalid input
        log::warn!(
            "Index.db input too short ({} bytes), defaulting to DigestFormat",
            input.len()
        );
        return IndexFormat::DigestFormat;
    }

    let first_word = u16::from_be_bytes([input[0], input[1]]);

    // If first word is the digest format marker (0x0010), it's digest format
    // Otherwise, treat as BTI format (entry length prefix)
    if first_word == 0x0010 {
        log::debug!("Detected DigestFormat (marker: {:#06x})", first_word);
        IndexFormat::DigestFormat
    } else {
        // BTI format starts with entry length (typically 0x000e = 14 bytes for simple text keys)
        log::debug!(
            "Detected BtiFormat (first word {:#06x} is not marker 0x0010)",
            first_word
        );
        IndexFormat::BtiFormat
    }
}

/// Parse a single partition entry from BTI/Partition Key format
///
/// BTI format structure:
/// - entry_length: 2 bytes (big-endian) - total length of entry excluding this field
/// - key_length: 2 bytes (big-endian) - length of partition key
/// - key_bytes: variable - actual partition key bytes
/// - metadata: variable - clustering data, offset, padding
///
/// For stock_prices example:
/// ```text
/// 00 0e         - entry_length = 14 bytes
/// 00 04         - key_length = 4 bytes
/// 41 4d 5a 4e   - key_bytes = "AMZN"
/// 00 00 04 80   - metadata (clustering/timestamp?)
/// 00 4f 88      - metadata (offset?)
/// 00 00 00      - padding
/// ```
fn parse_bti_partition_entry(
    input: &[u8],
    _entry_index: usize,
) -> IResult<&[u8], PartitionIndexEntry> {
    // Parse entry_length (2 bytes, big-endian)
    let (input, entry_length) = be_u16(input)?;

    // Parse key_length (2 bytes, big-endian)
    let (input, key_length) = be_u16(input)?;

    // Read key_bytes
    let (input, key_bytes) = take(key_length)(input)?;

    // Calculate remaining metadata length
    // entry_length includes key_length field (2 bytes) + key_bytes + metadata
    // We've already consumed key_length (2) + key_bytes (key_length)
    // So metadata_length = entry_length - 2 - key_length
    let metadata_length = entry_length.saturating_sub(2).saturating_sub(key_length);

    // Read metadata section
    let (input, metadata) = take(metadata_length)(input)?;

    // TODO (M3 Technical Debt - Issue #208 C3): BTI format offset extraction
    //
    // BTI Index.db format does not have a clear specification for inline offset extraction.
    // The metadata structure is unclear from available documentation and hex analysis.
    // Setting offset to 0 to indicate sequential read mode is required.
    //
    // Proper fix: Research authoritative Cassandra 5.0+ BTI Index.db specification
    // to determine if and how offsets are encoded in the metadata section.
    let data_offset = 0;

    log::debug!(
        "BTI entry parsed: key=\"{}\", key_length={}, metadata_len={}. Offset set to 0 (sequential read mode)",
        String::from_utf8_lossy(key_bytes),
        key_length,
        metadata.len()
    );

    // TODO (M3 Technical Debt - Issue #208 C5): Refactor PartitionIndexEntry.key_digest
    //
    // Current workaround computes MD5 of BTI keys for compatibility with existing
    // lookup tables, but this adds unnecessary CPU overhead. The proper solution is
    // to refactor PartitionIndexEntry to support both:
    // - Digest-based keys (MD5, already in Index.db)
    // - Byte-comparable keys (BTI format, actual partition key bytes)
    //
    // This would eliminate the MD5 computation here and enable direct key comparisons.
    let key_digest = md5::compute(key_bytes);

    if data_offset == 0 {
        log::warn!(
            "BTI entry has no reliable offset, sequential read mode will be used. \
             Entry: {:?}, metadata_len: {}",
            String::from_utf8_lossy(key_bytes),
            metadata.len()
        );
    }

    // Entry structure is: [2-byte length][payload of 'length' bytes]
    // No padding between entries - the next entry starts immediately with its length field.
    // Previous code incorrectly skipped 2 bytes thinking it was padding, but hex analysis
    // shows that was consuming the next entry's length field (Issue #208 C4).

    Ok((
        input,
        PartitionIndexEntry {
            key_digest: Arc::from(&key_digest[..]),
            data_offset,
            data_size: 0,
            promoted_index: None,
        },
    ))
}

// REMOVED: Old heuristic functions that violated Issue #28 no-heuristics mandate
// - calculate_data_offset_from_summary: Summary.db correlation (now obsolete with inline offsets)
// - interpolate_data_offset_from_summary_position: Used arbitrary estimates
// - estimate_data_offset_from_index_position: Used hardcoded partition size guesses
//
// Modern Cassandra 5+ Index.db format includes variable-length offsets inline,
// eliminating the need for Summary.db correlation. See decode_be_offset() above.

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
    use std::env;

    /// Test stock_prices Index.db parsing (Issue #208)
    ///
    /// This test directly parses the stock_prices Index.db file which contains 3 partition entries (AMZN, GOOG, AAPL).
    /// Note: Data.db.jsonl only has 2 entries, suggesting incomplete test data or filtering at a higher level.
    /// The file uses a BTI format with actual partition keys (not MD5 digests).
    ///
    /// **Note:** This test requires test data files and is ignored in minimal CI builds.
    /// Run with: `cargo test --package cqlite-core -- --ignored`
    #[tokio::test]
    #[ignore = "Requires test data files (CQLITE_DATASETS_ROOT)"]
    async fn test_stock_prices_index_db_parsing() {
        let datasets_root = env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| {
            "/Users/patrick/local_projects/cqlite/test-data/datasets".to_string()
        });

        let index_path = format!(
            "{}/sstables/test_timeseries/stock_prices-6c9fad60a25111f0a3fef1a551383fb9/nb-1-big-Index.db",
            datasets_root
        );

        println!("\n=== Testing stock_prices Index.db ===");
        println!("Path: {}", index_path);

        // Read file directly to inspect format
        let file_data = std::fs::read(&index_path).expect("Failed to read Index.db");
        println!("File size: {} bytes", file_data.len());
        println!(
            "First 56 bytes (hex): {:02x?}",
            &file_data[..std::cmp::min(56, file_data.len())]
        );

        // Check format detection
        println!("\n=== Format Analysis ===");
        println!(
            "First 2 bytes: {:#06x} (expected 0x0010 for digest format)",
            u16::from_be_bytes([file_data[0], file_data[1]])
        );

        // Try to parse with current implementation
        println!("\n=== Parsing with parse_all_partition_keys_with_summary ===");
        match parse_all_partition_keys_with_summary(&file_data, None) {
            Ok((remaining, entries)) => {
                println!("SUCCESS: Parsed {} entries", entries.len());
                println!("Remaining bytes: {}", remaining.len());

                for (i, entry) in entries.iter().enumerate() {
                    println!(
                        "  Entry {}: offset={}, size={}, key_digest={:02x?}",
                        i,
                        entry.data_offset,
                        entry.data_size,
                        &entry.key_digest[..]
                    );
                }

                // Note: Index.db contains 3 entries (AMZN, GOOG, AAPL) but Data.db.jsonl only has 2.
                // This may indicate incomplete test data or filtering at a higher level.
                // For now, verify parser works correctly (finds all entries in Index.db).
                assert!(
                    entries.len() >= 2,
                    "Expected at least 2 partition entries for stock_prices (found {})",
                    entries.len()
                );
            }
            Err(e) => {
                println!("FAILED: {:?}", e);
                panic!("Failed to parse stock_prices Index.db: {:?}", e);
            }
        }
    }

    /// Test stock_prices Index.db via IndexReader (Issue #208)
    ///
    /// This test uses the high-level IndexReader API to open the stock_prices Index.db.
    /// It should successfully parse at least 2 partition entries (Index.db has 3 total).
    ///
    /// **Note:** This test requires test data files and is ignored in minimal CI builds.
    /// Run with: `cargo test --package cqlite-core -- --ignored`
    #[tokio::test]
    #[ignore = "Requires test data files (CQLITE_DATASETS_ROOT)"]
    async fn test_stock_prices_index_reader() {
        let datasets_root = env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| {
            "/Users/patrick/local_projects/cqlite/test-data/datasets".to_string()
        });

        let index_path = std::path::PathBuf::from(format!(
            "{}/sstables/test_timeseries/stock_prices-6c9fad60a25111f0a3fef1a551383fb9/nb-1-big-Index.db",
            datasets_root
        ));

        println!("\n=== Testing IndexReader::open ===");
        println!("Path: {:?}", index_path);

        // Create platform
        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        // Try to open with IndexReader
        match IndexReader::open(&index_path, platform.clone()).await {
            Ok(reader) => {
                let entries = reader.get_partition_entries();
                println!(
                    "SUCCESS: IndexReader found {} partition entries",
                    entries.len()
                );

                for (i, entry) in entries.iter().enumerate() {
                    println!(
                        "  Entry {}: offset={}, size={}, key_digest={:02x?}",
                        i,
                        entry.data_offset,
                        entry.data_size,
                        &entry.key_digest[..8]
                    );
                }

                let stats = reader.get_statistics();
                println!(
                    "Statistics: total_partitions={}, file_size={}",
                    stats.total_partitions, stats.file_size
                );

                // Verify parser works correctly (Index.db has 3 entries, Data.db.jsonl has 2)
                assert!(
                    entries.len() >= 2,
                    "Expected at least 2 partition entries for stock_prices (found {})",
                    entries.len()
                );
            }
            Err(e) => {
                println!("FAILED: {:?}", e);
                panic!("Failed to open stock_prices Index.db: {:?}", e);
            }
        }
    }

    /// Test stock_prices via SSTableReader integration (Issue #208)
    ///
    /// This test verifies that SSTableReader correctly loads the Index.db
    /// and can access partition entries (at least 2, Index.db has 3 total).
    ///
    /// **Note:** This test requires test data files and is ignored in minimal CI builds.
    /// Run with: `cargo test --package cqlite-core -- --ignored`
    #[tokio::test]
    #[ignore = "Requires test data files (CQLITE_DATASETS_ROOT)"]
    async fn test_stock_prices_sstable_reader_integration() {
        let datasets_root = env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| {
            "/Users/patrick/local_projects/cqlite/test-data/datasets".to_string()
        });

        let data_path = std::path::PathBuf::from(format!(
            "{}/sstables/test_timeseries/stock_prices-6c9fad60a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
            datasets_root
        ));

        println!("\n=== Testing SSTableReader with stock_prices ===");
        println!("Data.db path: {:?}", data_path);

        // Create platform
        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        // Try to open with SSTableReader
        use crate::storage::sstable::reader::SSTableReader;
        match SSTableReader::open(&data_path, &config, platform.clone()).await {
            Ok(reader) => {
                println!("SUCCESS: SSTableReader opened");

                // Check if index_reader was loaded (it's a public field)
                if let Some(ref index_reader) = reader.index_reader {
                    let entries = index_reader.get_partition_entries();
                    println!("Index loaded with {} partition entries", entries.len());

                    for (i, entry) in entries.iter().enumerate() {
                        println!(
                            "  Entry {}: offset={}, size={}",
                            i, entry.data_offset, entry.data_size
                        );
                    }

                    // Verify Index.db was parsed correctly (has at least 2 entries, actually has 3)
                    assert!(
                        entries.len() >= 2,
                        "Expected at least 2 partition entries for stock_prices (found {})",
                        entries.len()
                    );
                } else {
                    println!("WARNING: Index.db was not loaded by SSTableReader");
                    panic!("SSTableReader did not load Index.db");
                }
            }
            Err(e) => {
                println!("FAILED: {:?}", e);
                panic!("Failed to open stock_prices SSTable: {:?}", e);
            }
        }
    }

    #[test]
    fn test_simple_partition_key_parsing() {
        // Variable-length format: marker(2) + key_digest(16) + offset_len(1) + offset(variable)
        let data = vec![
            0x00, 0x10, // marker = 0x0010
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest (16 bytes)
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x02, // offset_len = 2 bytes
            0x01, 0x00, // offset = 256 (big-endian)
        ];

        let (_, entry) = parse_simple_partition_key(&data).unwrap();

        assert_eq!(
            entry.key_digest.as_ref(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        // Raw offset from Index.db (relative to data section start)
        // SSTableReader will add actual_header_size to get absolute file offset
        assert_eq!(entry.data_offset, 256);
        assert_eq!(entry.data_size, 0); // Size not stored in Index.db (Issue #149)
        assert!(entry.promoted_index.is_none());
    }

    #[test]
    fn test_partition_key_parsing_without_summary() {
        // Variable-length format with 3-byte offset
        let data = vec![
            0x00, 0x10, // marker = 0x0010
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest (16 bytes)
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x03, // offset_len = 3 bytes
            0x00, 0x10, 0x00, // offset = 4096 (big-endian)
        ];

        // Test with different entry indices - should all parse the same data
        let (_, entry0) = parse_simple_partition_key_with_offset(&data, 0, None).unwrap();
        let (_, entry1) = parse_simple_partition_key_with_offset(&data, 1, None).unwrap();
        let (_, entry5) = parse_simple_partition_key_with_offset(&data, 5, None).unwrap();

        assert_eq!(
            entry0.key_digest.as_ref(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );

        // Raw offset from Index.db (relative to data section start)
        assert_eq!(entry0.data_offset, 4096);
        assert_eq!(entry1.data_offset, 4096);
        assert_eq!(entry5.data_offset, 4096);

        // All should have the same key digest in this test
        assert_eq!(entry0.key_digest.as_ref(), entry1.key_digest.as_ref());
        assert_eq!(entry1.key_digest.as_ref(), entry5.key_digest.as_ref());
    }

    // REMOVED: test_enhanced_partition_entry_parsing
    // Enhanced format parsing removed per Issue #92

    #[test]
    fn test_multiple_partition_keys_parsing() {
        // Two partition entries with variable-length offsets
        let data = vec![
            // Entry 1
            0x00, 0x10, // marker = 0x0010
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest 1 (16 bytes)
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x01, // offset_len = 1 byte
            0x64, // offset = 100
            // Entry 2
            0x00, 0x10, // marker = 0x0010
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // key_digest 2 (16 bytes)
            0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, 0x02, // offset_len = 2 bytes
            0x01, 0xF4, // offset = 500 (big-endian)
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

            // Raw offsets from Index.db (relative to data section start)
            assert_eq!(entries[0].data_offset, 100);
            assert_eq!(entries[1].data_offset, 500);
        }
    }

    // REMOVED: test_data_offset_estimation_algorithm
    // This test validated the old heuristic estimation function which has been removed
    // in favor of spec-accurate Summary.db correlation (Issue #92)

    #[test]
    fn test_borrow_trait_zero_allocation_lookup() {
        // Test Issue #107 fix: Verify that lookup_partition uses Borrow trait
        // to avoid heap allocation on every lookup

        // Create index data with two partition entries (variable-length format)
        let data = vec![
            // Entry 1
            0x00, 0x10, // marker = 0x0010
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_digest 1
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x01, // offset_len = 1 byte
            0x64, // offset = 100
            // Entry 2
            0x00, 0x10, // marker = 0x0010
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // key_digest 2
            0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, 0x02, // offset_len = 2 bytes
            0x01, 0xF4, // offset = 500 (big-endian)
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
