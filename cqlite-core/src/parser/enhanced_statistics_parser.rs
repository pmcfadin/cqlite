//! Enhanced Statistics.db parser for Cassandra 5.0 'nb' format
//!
//! # Implementation Status (Issue #162)
//!
//! This module provides **MINIMAL PARSING** of nb-format Statistics.db files to support
//! delta-coded timestamp decoding in V5CompressedLegacy parser.
//!
//! ## Current Implementation
//!
//! Parses ONLY the EncodingStats fields required for delta decoding:
//! - Header (32 bytes): version, data_length, checksum, metadata
//! - EncodingStats section: partitioner, minTimestamp, minLocalDeletionTime, minTTL
//!
//! All other statistics (row counts, histograms, column stats, etc.) are populated with
//! placeholder values. This is sufficient for V5CompressedLegacy parser baseline values.
//!
//! ## Previous Implementation (REMOVED)
//!
//! The previous implementation violated the no-heuristics mandate (Issue #28) by fabricating
//! statistics from header metadata. It was removed and replaced with this minimal real-data
//! parser that extracts only what's needed from the actual binary format.
//!
//! ## Deferred to Future Milestones
//!
//! Complete Statistics.db parsing including:
//! - Row count statistics and distribution histograms
//! - Column-level statistics and cardinality estimates
//! - Partition size histograms and percentiles
//! - Compression ratio and performance metrics
//! - Checksum validation (header.checksum field not yet validated)
//!
//! ## References
//!
//! - Issue #162: Fix Statistics reader for Cassandra 5 nb format
//! - Issue #28: No-heuristics mandate for modern Cassandra 5.0 paths
//! - Issue #105: Remove heuristic estimation from enhanced_statistics_parser.rs
//! - `docs/development/rust_developer_guide.md`: Architecture decisions

use super::statistics::*;
use super::vint::parse_vuint;
use crate::error::{Error, Result};
use crate::storage::sstable::version_gate::VersionGates;
use nom::{bytes::complete::take, number::complete::be_u32, IResult};

/// Cassandra MetadataType enum ordinals (from MetadataType.java)
/// Used to identify component types in Statistics.db TOC
#[allow(dead_code)]
const METADATA_TYPE_VALIDATION: u32 = 0;
#[allow(dead_code)]
const METADATA_TYPE_COMPACTION: u32 = 1;
#[allow(dead_code)]
const METADATA_TYPE_STATS: u32 = 2;
const METADATA_TYPE_HEADER: u32 = 3; // SerializationHeader

/// Epoch constants matching Cassandra's EncodingStats.java (EncodingStats.Serializer)
/// Used for delta-encoding/decoding EncodingStats fields in Statistics.db SERIALIZATION_HEADER.
/// Cassandra serializes: writeUnsignedVInt(value - EPOCH)
/// Cassandra deserializes: readUnsignedVInt() + EPOCH
const TIMESTAMP_EPOCH: i64 = 1_442_880_000_000_000; // Sept 22, 2015 00:00:00 UTC in microseconds
const DELETION_TIME_EPOCH: i64 = 1_442_880_000; // Sept 22, 2015 00:00:00 UTC in seconds
                                                // TTL epoch is 0 in Cassandra, but kept for consistency with the delta-encoding pattern
const TTL_EPOCH: i64 = 0;

/// Type alias for EncodingStats parse result to reduce complexity
type EncodingStatsResult = (
    i64,
    i64,
    Option<i64>,
    Vec<super::header::ColumnInfo>,
    Vec<super::header::ColumnInfo>,
    Vec<super::header::ColumnInfo>,
);

/// Type alias for SerializationHeader parse result to reduce complexity
type SerializationHeaderResult = (Vec<String>, Vec<String>, Vec<super::header::ColumnInfo>);

/// Enhanced Statistics.db header parser for real 'nb' format
///
/// This function parses the actual 32-byte binary header structure from
/// Cassandra 5.0 Statistics.db files. Based on hex analysis of real files:
///
/// ```text
/// 00000000  00 00 00 04 26 29 1b 05  00 00 00 00 00 00 00 2c
/// 00000010  00 00 00 01 00 00 00 65  00 00 00 02 00 00 14 d4
/// ```
///
/// # Binary Format (32 bytes)
///
/// - Bytes 0-3:   `version_type` (u32 BE) - Format version identifier (e.g., 0x00000004)
/// - Bytes 4-7:   `statistics_kind` (u32 BE) - Statistics type marker (e.g., 0x26291b05)
/// - Bytes 8-11:  `reserved1` (u32 BE) - Reserved field (typically 0x00000000)
/// - Bytes 12-15: `data_length` (u32 BE) - Length of variable-length data section
/// - Bytes 16-19: `metadata1` (u32 BE) - Metadata field (purpose TBD in M2)
/// - Bytes 20-23: `metadata2` (u32 BE) - Metadata field (purpose TBD in M2)
/// - Bytes 24-27: `metadata3` (u32 BE) - Metadata field (purpose TBD in M2)
/// - Bytes 28-31: `checksum_or_more` (u32 BE) - Checksum or additional metadata
///
/// # Returns
///
/// `Ok((remaining_input, StatisticsHeader))` on successful parse of 32-byte header.
///
/// # Note
///
/// This is the ONLY function in this module that reads actual binary data.
/// All other parsing functions have been removed per Issue #28 mandate.
pub fn parse_nb_format_header(input: &[u8]) -> IResult<&[u8], StatisticsHeader> {
    let (input, version_type) = be_u32(input)?;
    let (input, statistics_kind) = be_u32(input)?;
    let (input, _reserved1) = be_u32(input)?;
    let (input, data_length) = be_u32(input)?;
    let (input, metadata1) = be_u32(input)?;
    let (input, metadata2) = be_u32(input)?;
    let (input, metadata3) = be_u32(input)?;
    let (input, checksum_or_more) = be_u32(input)?;

    Ok((
        input,
        StatisticsHeader {
            version: version_type,
            statistics_kind,
            data_length,
            metadata1,
            metadata2,
            metadata3,
            checksum: checksum_or_more,
            table_id: None,
        },
    ))
}

/// Parse Statistics.db Table of Contents to get component offsets (Issue #216)
///
/// Statistics.db format (from Cassandra MetadataSerializer.java):
/// - [4 bytes] number_of_components (u32 BE)
/// - [4 bytes] checksum (u32 BE)
/// - [TOC] component_type (u32) | offset (u32) for each component
/// - [Component data...]
///
/// MetadataType enum ordinals:
/// - 0 = VALIDATION
/// - 1 = COMPACTION
/// - 2 = STATS
/// - 3 = HEADER (SerializationHeader)
///
/// Returns the offset to the HEADER component (SerializationHeader), or None if not found.
fn parse_statistics_toc_for_header_offset(input: &[u8]) -> Option<usize> {
    if input.len() < 8 {
        log::debug!("Statistics.db too small for TOC: {} bytes", input.len());
        return None;
    }

    // Parse number of components
    let num_components = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    log::debug!("Statistics.db TOC: {} components", num_components);

    // Sanity check: Cassandra has exactly 4 MetadataType enum values
    // (VALIDATION=0, COMPACTION=1, STATS=2, HEADER=3)
    // A value > 100 indicates corrupted or malicious data
    if num_components > 100 {
        log::warn!(
            "Suspicious num_components={} in Statistics.db TOC (expected <=4)",
            num_components
        );
        return None;
    }

    // Skip checksum (bytes 4-7)
    // TOC starts at byte 8

    let toc_start: usize = 8;
    let toc_entry_size: usize = 8; // 4 bytes type + 4 bytes offset

    // Use checked_mul to prevent integer overflow on multiplication
    let toc_size = (num_components as usize)
        .checked_mul(toc_entry_size)
        .and_then(|size| size.checked_add(toc_start))?;

    if input.len() < toc_size {
        log::debug!(
            "Statistics.db too small for {} TOC entries: {} bytes (need {})",
            num_components,
            input.len(),
            toc_size
        );
        return None;
    }

    // Search for HEADER component (type 3)
    for i in 0..num_components as usize {
        // Use checked arithmetic to prevent overflow in entry offset calculation
        let entry_offset = i
            .checked_mul(toc_entry_size)
            .and_then(|offset| offset.checked_add(toc_start))?;
        let component_type = u32::from_be_bytes([
            input[entry_offset],
            input[entry_offset + 1],
            input[entry_offset + 2],
            input[entry_offset + 3],
        ]);
        let component_offset = u32::from_be_bytes([
            input[entry_offset + 4],
            input[entry_offset + 5],
            input[entry_offset + 6],
            input[entry_offset + 7],
        ]) as usize;

        log::debug!(
            "TOC entry {}: type={} offset=0x{:x}",
            i,
            component_type,
            component_offset
        );

        if component_type == METADATA_TYPE_HEADER {
            log::debug!(
                "Found HEADER component at offset 0x{:x} ({})",
                component_offset,
                component_offset
            );
            return Some(component_offset);
        }
    }

    log::debug!("HEADER component not found in Statistics.db TOC");
    None
}

/// Parse minimal nb-format statistics data for delta-coding baseline (Issue #162)
///
/// This implementation parses ONLY the EncodingStats fields required for delta decoding:
/// - partitioner (string)
/// - minTimestamp (VInt)
/// - minLocalDeletionTime (VInt)
/// - minTTL (VInt)
///
/// All other fields (histograms, column stats, etc.) are skipped to minimize complexity.
/// This is sufficient for V5CompressedLegacy parser which needs baseline values for
/// delta-coded timestamps and TTLs.
///
/// # Format (observed from real nb-format Statistics.db files)
///
/// After 32-byte header:
/// - metadata_type (u32 BE) = 0x00000003 (indicates EncodingStats section)
/// - data_length (VInt) - length of remaining data
/// - partitioner_length (VInt) - length of partitioner class name string
/// - partitioner (UTF-8 string) - e.g., "org.apache.cassandra.dht.Murmur3Partitioner"
/// - additional_metadata (various VInts) - skipped
/// - minTimestamp (VInt, microseconds)
/// - minLocalDeletionTime (VInt, seconds)
/// - minTTL (VInt, seconds)
///
/// # Returns
///
/// Partial statistics with only TimestampStatistics populated from real data.
#[allow(clippy::type_complexity)]
pub fn parse_nb_format_statistics_data(
    input: &[u8],
    header: &StatisticsHeader,
    full_input: &[u8],
    // VG3 plumbing: gates are threaded here so version-sensitive decisions in
    // parse_encoding_stats_vuints (e.g. has_uint_deletion_time) can be flipped
    // without re-deriving gates from the filename.
    // Pass `None` from callers that do not have gates (standalone tools, tests).
    gates: Option<&VersionGates>,
) -> Result<(
    RowStatistics,
    TimestampStatistics,
    TableStatistics,
    PartitionStatistics,
    CompressionStatistics,
    Vec<super::header::ColumnInfo>,
    Vec<super::header::ColumnInfo>,
    Vec<super::header::ColumnInfo>,
)> {
    // Get HEADER offset from TOC (Issue #216)
    let header_offset = parse_statistics_toc_for_header_offset(full_input);

    // Parse the EncodingStats section from the data following the header
    let result = parse_minimal_encoding_stats(input, full_input, header_offset, gates);

    match result {
        Ok((
            _,
            (
                min_timestamp,
                min_deletion_time,
                min_ttl,
                partition_columns,
                clustering_columns,
                regular_columns,
            ),
        )) => {
            // Create minimal statistics with only timestamp data populated
            let row_stats = RowStatistics {
                total_rows: 0,
                live_rows: 0,
                tombstone_count: 0,
                partition_count: 0,
                avg_rows_per_partition: 0.0,
                row_size_histogram: vec![],
            };

            let timestamp_stats = TimestampStatistics {
                min_timestamp,
                max_timestamp: min_timestamp, // Not parsed, use min as placeholder
                min_deletion_time,
                max_deletion_time: min_deletion_time,
                min_ttl,
                max_ttl: min_ttl,
                rows_with_ttl: 0,
            };

            let table_stats = TableStatistics {
                disk_size: 0,
                uncompressed_size: 0,
                compressed_size: 0,
                compression_ratio: 1.0,
                block_count: 0,
                avg_block_size: 0.0,
                index_size: 0,
                bloom_filter_size: 0,
                level_count: 0,
            };

            let partition_stats = PartitionStatistics {
                avg_partition_size: 0.0,
                min_partition_size: 0,
                max_partition_size: 0,
                large_partition_percentage: 0.0,
                size_histogram: vec![],
            };

            let compression_stats = CompressionStatistics {
                algorithm: "unknown".to_string(),
                original_size: 0,
                compressed_size: 0,
                ratio: 1.0,
                compression_speed: 0.0,
                decompression_speed: 0.0,
                compressed_blocks: 0,
            };

            Ok((
                row_stats,
                timestamp_stats,
                table_stats,
                partition_stats,
                compression_stats,
                partition_columns,
                clustering_columns,
                regular_columns,
            ))
        }
        Err(e) => {
            log::debug!(
                "Failed to parse minimal EncodingStats from Statistics.db: {:?}",
                e
            );
            Err(Error::UnsupportedFormat(format!(
                "Failed to parse minimal nb-format Statistics.db EncodingStats: {:?}. \
                         This is required for delta-coded timestamp decoding. \
                         Header checksum: 0x{:08x}, data_length: {}",
                e, header.checksum, header.data_length
            )))
        }
    }
}

/// Parse SerializationHeader from Statistics.db (Issue #163)
///
/// This function locates and parses the complete SerializationHeader section including:
/// 1. Partition key types
/// 2. Clustering key types
/// 3. Regular column definitions
///
/// Returns: (partition_key_types, clustering_key_types, regular_columns)
fn parse_serialization_header(input: &[u8]) -> IResult<&[u8], SerializationHeaderResult> {
    log::debug!(
        "Searching for SerializationHeader in {} bytes (max search: 8KB)",
        input.len()
    );

    // Log input buffer state at function entry
    let preview_len = std::cmp::min(64, input.len());
    let preview_hex: String = input[..preview_len]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");
    log::debug!(
        "Input buffer size: {} bytes, first 64 bytes: {}",
        input.len(),
        preview_hex
    );

    // Search for SerializationHeader start marker: VInt followed by 0x00 0x00 and '(' character
    // This marks the beginning of the partition key type descriptor
    let mut search_offset = 0;

    // Search for SerializationHeader by finding "org.apache.cassandra.db.marshal" string
    // and working backwards to find the 0x00 0x00 marker
    // Format: [VInt unknown] [0x00 0x00] [VInt partition_type_len] [partition_type_string]
    let marshal_pattern = b"org.apache.cassandra.db.marshal";

    while search_offset + marshal_pattern.len() < input.len() && search_offset < 8192 {
        if &input[search_offset..search_offset + marshal_pattern.len()] == marshal_pattern {
            let context_start = search_offset.saturating_sub(10);
            let context_end = (search_offset + 50).min(input.len());
            log::debug!(
                "Found 'org.apache.cassandra.db.marshal' at offset {}, context (offset-10 to offset+50): {:02x?}",
                search_offset,
                &input[context_start..context_end]
            );

            // Issue #216 fix: Look for the pattern [prev_zero] [pk_type_len] "org.apache..."
            // where pk_type_len is a valid VInt length (0x01-0x7F for single byte, or multi-byte VInt)
            // The prev_zero is typically the last byte of EncodingStats (minTTL=0) or another zero field.
            //
            // We need to find the START of the partition key type length, which is:
            // - 1 byte before "org.apache..." for single-byte lengths (0x28 = 40 bytes for UUIDType)
            // - 2 bytes before for two-byte VInt lengths (0x80 0xXX)

            for lookback in 1..=15 {
                if search_offset < lookback {
                    break;
                }
                let type_len_offset = search_offset - lookback;

                // Check if this could be a valid pk_type_len
                // For single-byte VInt: values 0x01-0x7F
                // For two-byte VInt: first byte has high bit set (0x80-0xFF)
                let first_byte = input[type_len_offset];

                // Common partition key type lengths:
                // - UUIDType: 40 bytes (0x28)
                // - UTF8Type: 40 bytes (0x28)
                // - Int32Type: 41 bytes (0x29)
                // - TimestampType: 45 bytes (0x2D)
                // - CompositeType: ~80-150 bytes (0x50-0x96 or multi-byte VInt)

                // Single-byte VInt: 0x20-0x7F are reasonable pk_type lengths (32-127 bytes)
                let is_valid_single_byte_len = (0x20..=0x7F).contains(&first_byte);

                // Two-byte VInt: 0x80-0xBF with continuation
                let is_multi_byte_vint = first_byte >= 0x80;

                if is_valid_single_byte_len || is_multi_byte_vint {
                    // Try parsing from this offset using sequential parser
                    let result = parse_serialization_header_sequential(&input[type_len_offset..]);
                    if let Ok((remaining, (pk_types, ck_types, cols))) = result {
                        // Validate: partition key type should contain expected substring
                        if !pk_types.is_empty()
                            && pk_types[0].contains("org.apache.cassandra.db.marshal")
                        {
                            log::debug!(
                                "Successfully parsed SerializationHeader at offset {} (lookback: {}): pk_type={}",
                                type_len_offset,
                                lookback,
                                pk_types[0]
                            );
                            return Ok((remaining, (pk_types, ck_types, cols)));
                        }
                    }
                }

                // Also try the legacy 0x00 0x00 marker for backward compatibility
                if type_len_offset > 0 {
                    let prev_offset = type_len_offset - 1;
                    if input[prev_offset] == 0x00 && input[type_len_offset] == 0x00 {
                        let result = parse_serialization_header_at_offset(&input[prev_offset..]);
                        if result.is_ok() {
                            log::debug!(
                                "Successfully parsed SerializationHeader at legacy marker offset {}",
                                prev_offset
                            );
                            return result;
                        }
                    }
                }
            }
        }
        search_offset += 1;
    }

    log::debug!(
        "Search completed: searched {} bytes, no partition key type found",
        search_offset
    );

    // Partition key type not found - try to find regular columns directly
    // This handles files where SerializationHeader contains only regular columns
    log::debug!("Attempting to parse regular columns without partition key metadata");
    let (remaining, (partition_keys, columns)) = parse_regular_columns(input)?;

    if !columns.is_empty() {
        log::debug!(
            "Successfully parsed {} regular columns, {} partition keys via backtracking",
            columns.len(),
            partition_keys.len()
        );
        return Ok((remaining, (partition_keys, Vec::new(), columns)));
    }

    // Nothing found - return empty results
    log::warn!(
        "Failed to locate SerializationHeader or regular columns: searched {} bytes",
        search_offset
    );

    if let Some((pk_types, ck_types, cols)) = fallback_parse_serialization_header_ascii(input) {
        log::debug!(
            "ASCII fallback extracted SerializationHeader: {} partition keys, {} clustering keys, {} regular columns",
            pk_types.len(),
            ck_types.len(),
            cols.len()
        );
        return Ok((input, (pk_types, ck_types, cols)));
    }

    Ok((input, (Vec::new(), Vec::new(), Vec::new())))
}

/// Parse SerializationHeader structure starting at a known offset
fn parse_serialization_header_at_offset(input: &[u8]) -> IResult<&[u8], SerializationHeaderResult> {
    use nom::bytes::complete::tag;
    use nom::number::complete::u8 as parse_u8;

    let _original_input = input;

    // Step 1: Expect 0x00 0x00 marker
    let (input, _) = tag(b"\x00\x00")(input)?;
    log::debug!("Found 0x00 0x00 marker");

    // Step 2: Parse partition key type (single byte length + string)
    let (input, partition_type_len) = parse_u8(input)?;
    log::debug!("Partition key type length: {} bytes", partition_type_len);

    let (input, partition_type_bytes) =
        nom::bytes::complete::take(partition_type_len as usize)(input)?;
    let partition_key_type = std::str::from_utf8(partition_type_bytes)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?
        .to_string();

    log::debug!("Partition key type: {}", partition_key_type);

    // Step 3: Parse clustering key count (single byte)
    let (input, clustering_count) = parse_u8(input)?;
    log::debug!("Clustering key count: {}", clustering_count);

    // Step 4: Parse clustering key types
    let mut clustering_key_types = Vec::with_capacity(clustering_count as usize);
    let mut input = input;

    for idx in 0..clustering_count {
        // Parse clustering type length (single byte)
        let (remaining, type_len) = parse_u8(input)?;
        log::debug!("Clustering key {} type length: {} bytes", idx, type_len);

        let (remaining, type_bytes) = nom::bytes::complete::take(type_len as usize)(remaining)?;
        let clustering_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        log::debug!("Clustering key {} type: {}", idx, clustering_type);

        clustering_key_types.push(clustering_type);
        input = remaining;
    }

    // Step 5: Parse static column count (NOT a separator - this was the bug!)
    // When static_count = 0, this byte is 0x00 which made simple tables work.
    // But when static_count > 0, parsing failed.
    let (input, static_count) = parse_u8(input)?;
    log::debug!("Static column count: {}", static_count);

    // Step 5a: Parse static columns
    let mut static_columns = Vec::with_capacity(static_count as usize);
    let mut input = input;

    for static_idx in 0..static_count {
        // Static column name length (single byte)
        let (remaining, name_len) = parse_u8(input)?;
        log::debug!(
            "Static column {} name length: {} bytes",
            static_idx,
            name_len
        );

        // Validate name length (match validation in parse_regular_columns)
        if name_len == 0 || name_len > 200 {
            log::debug!(
                "Static column {} name_len sanity check failed: {}",
                static_idx,
                name_len
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        // Static column name (UTF-8 string)
        let (remaining, name_bytes) = nom::bytes::complete::take(name_len as usize)(remaining)?;
        let column_name = std::str::from_utf8(name_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        // Static column type length (VInt - can exceed 127 for collection types)
        let (remaining, type_len_u64) = parse_vuint(remaining)?;
        log::debug!(
            "Static column {} ('{}') type length: {} bytes",
            static_idx,
            column_name,
            type_len_u64
        );

        // Validate type length (match validation in parse_regular_columns)
        if type_len_u64 == 0 || type_len_u64 > 5000 {
            log::debug!(
                "Static column {} ('{}') type_len sanity check failed: {}",
                static_idx,
                column_name,
                type_len_u64
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
        if type_len_u64 > 1000 {
            log::warn!(
                "Unusually long static column type string: {} bytes (typical <1000)",
                type_len_u64
            );
        }

        // Static column type (UTF-8 string)
        let (remaining, type_bytes) = nom::bytes::complete::take(type_len_u64 as usize)(remaining)?;
        let internal_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        let cql_type = convert_marshal_type_to_cql(&internal_type);

        log::debug!(
            "Static column {}: name='{}', type='{}' (CQL: '{}')",
            static_idx,
            column_name,
            internal_type,
            cql_type
        );

        static_columns.push(super::header::ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: true, // Mark as static column!
            is_clustering: false,
            clustering_reversed: false,
        });

        input = remaining;
    }

    log::debug!("Parsed {} static columns", static_columns.len());

    // Step 6: Parse regular column count (single byte)
    let (mut input, column_count) = parse_u8(input)?;
    log::debug!("Regular column count: {}", column_count);

    // Step 7: Parse each regular column
    let mut columns = Vec::with_capacity(column_count as usize + static_columns.len());

    for col_idx in 0..column_count {
        // Column name length (single byte)
        let (remaining, name_len) = parse_u8(input)?;
        log::debug!("Column {} name length: {} bytes", col_idx, name_len);

        // Column name (UTF-8 string)
        let (remaining, name_bytes) = nom::bytes::complete::take(name_len as usize)(remaining)?;
        let column_name = std::str::from_utf8(name_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        // Column type length (VInt - can exceed 127 for collection types)
        let (remaining, type_len_u64) = parse_vuint(remaining)?;
        log::debug!(
            "Column {} ('{}') type length: {} bytes",
            col_idx,
            column_name,
            type_len_u64
        );

        // Validate type length (consistent with parse_regular_columns and static columns)
        if type_len_u64 == 0 || type_len_u64 > 5000 {
            log::debug!(
                "Column {} ('{}') type_len validation failed: {}",
                col_idx,
                column_name,
                type_len_u64
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
        if type_len_u64 > 1000 {
            log::warn!(
                "Unusually long column type string: {} bytes (typical <1000)",
                type_len_u64
            );
        }

        // Column type (UTF-8 string)
        let (remaining, type_bytes) = nom::bytes::complete::take(type_len_u64 as usize)(remaining)?;
        let internal_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        input = remaining;

        // Convert to CQL type
        let cql_type = convert_marshal_type_to_cql(&internal_type);

        log::debug!(
            "Column {}: name='{}', type='{}' (CQL: '{}')",
            col_idx,
            column_name,
            internal_type,
            cql_type
        );

        columns.push(super::header::ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        });
    }

    // Merge static columns (first) with regular columns
    // Static columns come before regular columns in the combined list
    let mut all_columns = static_columns;
    all_columns.append(&mut columns);

    log::debug!(
        "Successfully parsed SerializationHeader: {} partition keys, {} clustering keys, {} static columns, {} regular columns ({} total)",
        1, // Always 1 partition key in current implementation
        clustering_key_types.len(),
        all_columns.iter().filter(|c| c.is_static).count(),
        all_columns.iter().filter(|c| !c.is_static).count(),
        all_columns.len()
    );

    Ok((
        input,
        (vec![partition_key_type], clustering_key_types, all_columns),
    ))
}

/// Extract partition key type by backtracking from the `0x00 0x00` marker
///
/// The partition key type descriptor ends immediately before the marker.
/// We try parsing VInt lengths at different offsets before the marker to find
/// a valid type string that matches Cassandra marshal type patterns.
fn extract_partition_key_before_marker(input: &[u8], marker_offset: usize) -> Option<String> {
    if marker_offset < 3 {
        return None;
    }

    log::debug!(
        "Backtracking from marker at offset {} (input len: {})",
        marker_offset,
        input.len()
    );

    // Try parsing VInt lengths at different positions before the marker
    // Type strings can be up to 200 bytes, and VInts can be 1-9 bytes,
    // so we need to search back at least 209 bytes (200 + 9)
    let max_lookback = 210;
    let search_start = marker_offset.saturating_sub(max_lookback);
    log::debug!(
        "Searching for VInt from offset {} to {} ({} positions)",
        search_start,
        marker_offset,
        marker_offset - search_start
    );

    for vint_start in (search_start..marker_offset).rev() {
        // Try to parse VInt at this position
        match parse_vuint(&input[vint_start..marker_offset]) {
            Ok((remaining, type_len)) => {
                // Validate type length is reasonable first (before any arithmetic)
                if !(10..200).contains(&type_len) {
                    continue;
                }

                // Calculate how many bytes the VInt consumed
                let vint_len = marker_offset - vint_start - remaining.len();
                let type_start = vint_start + vint_len;

                // Bounds check before addition to prevent overflow
                let type_len_usize = type_len as usize;
                if type_start > input.len() || type_len_usize > input.len() - type_start {
                    continue;
                }

                let type_end = type_start + type_len_usize;

                // Validate:
                // 1. The type string ends exactly at the marker
                // 2. The type string is valid UTF-8
                // 3. It matches Cassandra marshal type patterns
                if type_end == marker_offset {
                    if let Ok(type_str) = std::str::from_utf8(&input[type_start..type_end]) {
                        log::debug!(
                            "Candidate at vint_start={}: type_len={}, type_start={}, type_end={}, str={}",
                            vint_start, type_len, type_start, type_end, type_str
                        );
                        // Validate it's a Cassandra marshal type
                        // Note: Partition key types may or may not start with '('
                        // Both "(org.apache.cassandra..." and "org.apache.cassandra..." are valid
                        if type_str.contains("org.apache.cassandra") {
                            log::debug!(
                                "Found partition key type at offset {}: length={}, type={}",
                                vint_start,
                                type_len,
                                type_str
                            );
                            return Some(type_str.to_string());
                        } else {
                            log::debug!(
                                "Rejected candidate (starts_with='(': {}, contains 'org.apache.cassandra': {})",
                                type_str.starts_with('('),
                                type_str.contains("org.apache.cassandra")
                            );
                        }
                    } else {
                        log::debug!(
                            "Rejected candidate at vint_start={}: not valid UTF-8",
                            vint_start
                        );
                    }
                }
            }
            Err(_) => continue, // Try next offset
        }
    }

    None
}

/// Parse regular columns section from SerializationHeader
///
/// Returns: (partition_key_types, regular_columns)
/// Partition key types are extracted via backtracking when found before the column section marker.
fn parse_regular_columns(
    input: &[u8],
) -> IResult<&[u8], (Vec<String>, Vec<super::header::ColumnInfo>)> {
    use super::header::ColumnInfo;

    let mut search_offset = 0;
    let mut partition_key_types = Vec::new();

    while search_offset + 2 < input.len() && search_offset < 8192 {
        if input[search_offset] == 0x00 {
            let (marker_offset, count_offset) =
                if search_offset + 1 < input.len() && input[search_offset + 1] == 0x00 {
                    (search_offset, search_offset + 2)
                } else {
                    (search_offset, search_offset + 1)
                };

            if count_offset >= input.len() {
                break;
            }

            let column_count = input[count_offset] as usize;
            if column_count == 0 || column_count > 50 {
                search_offset += 1;
                continue;
            }

            log::debug!(
                "Attempting to extract partition key by backtracking from marker at offset {}",
                marker_offset
            );
            if let Some(pk_type) = extract_partition_key_before_marker(input, marker_offset) {
                log::debug!("Found partition key type before marker: {}", pk_type);
                partition_key_types.push(pk_type);
            } else {
                log::debug!(
                    "No partition key type found via backtracking at offset {}",
                    marker_offset
                );
            }

            let mut pos = count_offset + 1;

            let context_len = std::cmp::min(128, input.len() - marker_offset);
            let context_hex: String = input[marker_offset..marker_offset + context_len]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            log::debug!(
                "Pattern found at offset {}: count={}, next 128 bytes: {}",
                marker_offset,
                column_count,
                context_hex
            );

            // Try to parse all columns - if successful, we found the right section
            let mut parsed_columns = Vec::with_capacity(column_count);
            let mut parse_success = true;

            for col_idx in 0..column_count {
                if pos >= input.len() {
                    log::debug!(
                        "Column {} parsing failed at offset {}: position {} exceeds buffer length {}",
                        col_idx,
                        marker_offset,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                if pos >= input.len() {
                    log::debug!(
                        "Column {} parsing failed at offset {}: no data available for name length byte (pos={}, len={})",
                        col_idx,
                        marker_offset,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                let name_len = input[pos] as usize;
                pos += 1;

                if name_len == 0 || name_len > 200 || pos + name_len > input.len() {
                    log::debug!(
                        "Column {} parsing failed at offset {}: name_len sanity check failed (name_len={}, pos={}, buffer_len={})",
                        col_idx,
                        marker_offset,
                        name_len,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                // Column name (UTF-8 string)
                let name_bytes = &input[pos..pos + name_len];
                let column_name = match std::str::from_utf8(name_bytes) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        let name_hex: String = name_bytes
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join(" ");
                        log::debug!(
                            "Column {} parsing failed at offset {}: UTF-8 decode error for column name at pos {} (len={}): {:?}, bytes: {}",
                            col_idx,
                            marker_offset,
                            pos,
                            name_len,
                            e,
                            name_hex
                        );
                        parse_success = false;
                        break;
                    }
                };
                pos += name_len;

                if pos >= input.len() {
                    log::debug!(
                        "Column {} ('{}') parsing failed at offset {}: no data available for type length byte (pos={}, len={})",
                        col_idx,
                        column_name,
                        marker_offset,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                // Parse type length as VInt (can exceed 127 for collection types)
                let type_len_result = parse_vuint(&input[pos..]);
                let (type_remaining, type_len_u64) = match type_len_result {
                    Ok(r) => r,
                    Err(_) => {
                        log::debug!(
                            "Column {} ('{}') parsing failed at offset {}: VInt parse error at pos {}",
                            col_idx,
                            column_name,
                            marker_offset,
                            pos
                        );
                        parse_success = false;
                        break;
                    }
                };
                let type_len = type_len_u64 as usize;
                pos = input.len() - type_remaining.len();

                if type_len == 0 || type_len > 5000 || pos + type_len > input.len() {
                    log::debug!(
                        "Column {} ('{}') parsing failed at offset {}: type_len sanity check failed (type_len={}, pos={}, buffer_len={})",
                        col_idx,
                        column_name,
                        marker_offset,
                        type_len,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                // Column type (Cassandra internal type name)
                let type_bytes = &input[pos..pos + type_len];
                let internal_type = match std::str::from_utf8(type_bytes) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        let type_hex: String = type_bytes
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join(" ");
                        log::debug!(
                            "Column {} ('{}') parsing failed at offset {}: UTF-8 decode error for column type at pos {} (len={}): {:?}, bytes: {}",
                            col_idx,
                            column_name,
                            marker_offset,
                            pos,
                            type_len,
                            e,
                            type_hex
                        );
                        parse_success = false;
                        break;
                    }
                };
                pos += type_len;

                // Convert Cassandra marshal type to CQL type
                let cql_type = convert_marshal_type_to_cql(&internal_type);

                parsed_columns.push(ColumnInfo {
                    name: column_name,
                    column_type: cql_type,
                    is_primary_key: false, // Will be determined from partition/clustering info
                    key_position: None,
                    is_static: false,
                    is_clustering: false,
                    clustering_reversed: false,
                });
            }

            if parse_success && parsed_columns.len() == column_count {
                // Successfully parsed all columns
                let column_names: Vec<&str> =
                    parsed_columns.iter().map(|c| c.name.as_str()).collect();
                log::debug!(
                    "Successfully parsed {} columns at offset {}: {:?}",
                    parsed_columns.len(),
                    marker_offset,
                    column_names
                );
                if !partition_key_types.is_empty() {
                    log::debug!(
                        "Extracted {} partition key types via backtracking: {:?}",
                        partition_key_types.len(),
                        partition_key_types
                    );
                }

                let remaining = &input[pos..];
                return Ok((remaining, (partition_key_types, parsed_columns)));
            }
        }

        search_offset += 1;
    }

    // Column section not found - return empty vecs (not an error, some files may have no regular columns)
    log::debug!(
        "Regular column section not found: searched {} bytes",
        search_offset
    );
    Ok((input, (Vec::new(), Vec::new())))
}

/// ASCII fallback parser for SerializationHeader when structured parsing fails
fn fallback_parse_serialization_header_ascii(
    input: &[u8],
) -> Option<(Vec<String>, Vec<String>, Vec<super::header::ColumnInfo>)> {
    use super::header::ColumnInfo;

    // Helper to find subsequence
    fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    let mut partition_types = Vec::new();
    let mut clustering_types = Vec::new();
    let mut columns = Vec::new();

    // Extract partition key types from CompositeType(...)
    if let Some(comp_idx) = find_subsequence(input, b"CompositeType(") {
        let start = comp_idx + "CompositeType(".len();
        let mut end = start;
        while end < input.len() && input[end] != b')' {
            end += 1;
        }
        if end <= input.len() {
            if let Ok(inner) = std::str::from_utf8(&input[start..end]) {
                partition_types = inner
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }

            // Attempt to extract clustering types immediately after the composite type string
            let mut cursor = end + 1;
            while cursor < input.len() && input[cursor] < 0x20 {
                cursor += 1;
            }
            if cursor < input.len() && input[cursor] == b'(' {
                cursor += 1;
                let mut cluster_end = cursor;
                while cluster_end < input.len() && input[cluster_end] >= 0x20 {
                    cluster_end += 1;
                }
                if cluster_end > cursor {
                    if let Ok(cluster_str) = std::str::from_utf8(&input[cursor..cluster_end]) {
                        if cluster_str.contains("org.apache.cassandra.db.marshal") {
                            clustering_types = cluster_str
                                .split(',')
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect();
                        }
                    }
                }
                // Set scan position for column parsing after clustering types/control bytes
                let mut scan_start = cluster_end;
                while scan_start < input.len() && input[scan_start] < 0x20 {
                    scan_start += 1;
                }

                // Parse regular columns using [len][name][type] pattern with control-byte delimiters
                let mut idx = scan_start;
                while idx < input.len() {
                    let name_len = input[idx] as usize;
                    if name_len == 0 || name_len > 64 {
                        idx += 1;
                        continue;
                    }

                    let name_start = idx + 1;
                    let name_end = name_start + name_len;
                    if name_end > input.len() {
                        break;
                    }

                    let name_bytes = &input[name_start..name_end];
                    if !name_bytes
                        .iter()
                        .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
                    {
                        idx += 1;
                        continue;
                    }

                    if name_end >= input.len() || input[name_end] != b'(' {
                        idx += 1;
                        continue;
                    }

                    let type_start = name_end + 1;
                    let mut type_end = type_start;
                    while type_end < input.len() && input[type_end] >= 0x20 {
                        type_end += 1;
                    }

                    if type_end == type_start {
                        idx += 1;
                        continue;
                    }

                    let type_bytes = &input[type_start..type_end];
                    if !type_bytes.windows(10).any(|w| w == b"org.apach") {
                        idx += 1;
                        continue;
                    }

                    let column_name = match std::str::from_utf8(name_bytes) {
                        Ok(s) => s.to_string(),
                        Err(_) => {
                            idx += 1;
                            continue;
                        }
                    };

                    let internal_type = match std::str::from_utf8(type_bytes) {
                        Ok(s) => s.trim().to_string(),
                        Err(_) => {
                            idx += 1;
                            continue;
                        }
                    };

                    let cql_type = convert_marshal_type_to_cql(&internal_type);
                    columns.push(ColumnInfo {
                        name: column_name,
                        column_type: cql_type,
                        is_primary_key: false,
                        key_position: None,
                        is_static: false,
                        is_clustering: false,
                        clustering_reversed: false,
                    });

                    // Advance past control bytes to next potential column entry
                    idx = type_end;
                    while idx < input.len() && input[idx] < 0x20 {
                        idx += 1;
                    }
                }
            }
        }
    }

    if partition_types.is_empty() && columns.is_empty() {
        return None;
    }

    Some((partition_types, clustering_types, columns))
}

/// Extract inner type from parameterized type string with proper parenthesis matching
///
/// Given a string that starts AFTER the opening parenthesis of a wrapper type,
/// returns the content up to (but not including) the matching closing parenthesis.
///
/// Example: For input "ListType(Int32Type))" (after stripping "FrozenType("),
/// returns Some("ListType(Int32Type)") - the content before the MATCHING close paren.
fn extract_inner_type(type_with_close_paren: &str) -> Option<&str> {
    let mut depth = 1; // We're already inside one opening paren (the wrapper type)
    for (idx, ch) in type_with_close_paren.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    // Return None if extracted string is empty (malformed input like ")")
                    if idx == 0 {
                        return None;
                    }
                    return Some(&type_with_close_paren[..idx]);
                }
            }
            _ => {}
        }
    }
    None // Unmatched parentheses
}

/// Split a type argument list on top-level commas, ignoring nested parentheses
fn split_type_arguments(input: &str) -> Vec<&str> {
    let mut args = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth > 0 {
                    depth -= 1;
                } else {
                    log::warn!(
                        "Unmatched closing parenthesis at position {} in type arguments: '{}'",
                        idx,
                        input
                    );
                }
            }
            ',' if depth == 0 => {
                let part = input[start..idx].trim();
                if !part.is_empty() {
                    args.push(part);
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    let tail = input[start..].trim();
    if !tail.is_empty() {
        args.push(tail);
    }

    args
}

/// Detect whether a clustering comparator class denotes descending order.
///
/// Issue #759: Cassandra encodes a DESC clustering column by wrapping its
/// comparator class in `org.apache.cassandra.db.marshal.ReversedType(...)`.
/// This is the documented, authoritative signal for descending order
/// (definitive guide Ch.7 / Appendix B) — no heuristics. We mirror the
/// leading-paren stripping that `convert_marshal_type_to_cql` performs (the
/// header sometimes prefixes types with `(`), then check for the `ReversedType(`
/// prefix with or without the marshal package namespace.
fn is_reversed_comparator(marshal_type: &str) -> bool {
    let mut value = marshal_type.trim();
    // Strip the optional leading wrapping paren(s) the header adds to the
    // top-level type (matches `convert_marshal_type_to_cql`'s normalization).
    while value.starts_with('(') && value.ends_with(')') && value.len() > 2 {
        value = value[1..value.len() - 1].trim();
    }
    // Some accepted header encodings prefix the comparator list with a `[`
    // (e.g. `[org.apache.cassandra.db.marshal.ReversedType(...)`); strip a
    // leading bracket so the ReversedType detection matches that form too
    // (roborev job 43).
    let value = value.trim_start_matches('[').trim_start();
    value.starts_with("org.apache.cassandra.db.marshal.ReversedType(")
        || value.starts_with("ReversedType(")
}

/// Convert Cassandra internal marshal type to CQL type name
fn convert_marshal_type_to_cql(marshal_type: &str) -> String {
    fn strip_wrapping_parens(mut value: &str) -> &str {
        loop {
            // Also strip a leading structural `[` the header may prefix the
            // comparator list with (e.g. `[org...ReversedType(...)`), so the
            // wrapper-prefix checks below match the bracketed form too and the
            // inner CQL type is derived correctly (roborev job 48). This mirrors
            // the same normalization in `is_reversed_comparator`.
            let trimmed = value.trim().trim_start_matches('[').trim_start();
            if trimmed.starts_with('(') && trimmed.ends_with(')') && trimmed.len() > 2 {
                value = &trimmed[1..trimmed.len() - 1];
            } else {
                return trimmed;
            }
        }
    }

    fn strip_namespace(type_name: &str) -> &str {
        type_name.rsplit('.').next().unwrap_or(type_name)
    }

    fn strip_type_suffix(name: &str) -> &str {
        name.trim_end_matches("Type")
    }

    let mut cleaned = strip_wrapping_parens(marshal_type);

    // Special case: Preserve UserType definitions unchanged
    // UserType contains critical metadata (keyspace, type name, field definitions) that must
    // reach the parser intact. Converting it to a simplified CQL type would lose this information.
    if cleaned.contains("org.apache.cassandra.db.marshal.UserType(") {
        return marshal_type.to_string();
    }

    // Normalize known wrappers by recursively converting inner types
    // Use extract_inner_type() for proper parenthesis matching (fixes nested types)
    for prefix in [
        "org.apache.cassandra.db.marshal.ReversedType(",
        "ReversedType(",
    ] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                return convert_marshal_type_to_cql(inner);
            }
        }
    }

    for prefix in ["org.apache.cassandra.db.marshal.FrozenType(", "FrozenType("] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                return format!("frozen<{}>", convert_marshal_type_to_cql(inner));
            }
        }
    }

    for prefix in ["org.apache.cassandra.db.marshal.ListType(", "ListType("] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                return format!("list<{}>", convert_marshal_type_to_cql(inner));
            }
        }
    }

    for prefix in ["org.apache.cassandra.db.marshal.SetType(", "SetType("] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                return format!("set<{}>", convert_marshal_type_to_cql(inner));
            }
        }
    }

    for prefix in ["org.apache.cassandra.db.marshal.MapType(", "MapType("] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                let args = split_type_arguments(inner);
                if args.len() == 2 {
                    let key = convert_marshal_type_to_cql(args[0]);
                    let value = convert_marshal_type_to_cql(args[1]);
                    return format!("map<{}, {}>", key, value);
                } else if args.len() == 1 {
                    let value = convert_marshal_type_to_cql(args[0]);
                    return format!("map<text, {}>", value);
                }
            }
        }
    }

    cleaned = strip_wrapping_parens(cleaned);
    let base = strip_type_suffix(strip_namespace(cleaned)).trim_end_matches(')');

    // Map common types to CQL equivalents
    match base {
        "UTF8" => "text".to_string(),
        "Int32" => "int".to_string(),
        "Integer" => "int".to_string(),
        "Long" => "bigint".to_string(),
        "Short" => "smallint".to_string(),
        "Byte" => "tinyint".to_string(),
        "SimpleDate" => "date".to_string(),
        "Timestamp" => "timestamp".to_string(),
        "Boolean" => "boolean".to_string(),
        "Decimal" => "decimal".to_string(),
        "Float" => "float".to_string(),
        "Double" => "double".to_string(),
        "Bytes" => "blob".to_string(),
        "Ascii" => "ascii".to_string(),
        "InetAddress" => "inet".to_string(),
        "UUID" => "uuid".to_string(),
        "TimeUUID" => "timeuuid".to_string(),
        "Duration" => "duration".to_string(),
        "Time" => "time".to_string(),
        "Counter" | "CounterColumn" => "counter".to_string(),
        other => other.to_lowercase(),
    }
}

/// Construct ColumnInfo entries for partition key definitions found in SerializationHeader
fn build_partition_key_columns(partition_types: &[String]) -> Vec<super::header::ColumnInfo> {
    if partition_types.is_empty() {
        return Vec::new();
    }

    let total = partition_types.len();
    partition_types
        .iter()
        .enumerate()
        .map(|(idx, marshal_type)| {
            let cql_type = convert_marshal_type_to_cql(marshal_type);
            let name = if total == 1 {
                match cql_type.as_str() {
                    "uuid" | "timeuuid" => "id".to_string(),
                    _ => "partition_key".to_string(),
                }
            } else {
                format!("partition_key_{}", idx)
            };

            super::header::ColumnInfo {
                name,
                column_type: cql_type,
                is_primary_key: true,
                key_position: Some(idx as u16),
                is_static: false,
                is_clustering: false,
                clustering_reversed: false,
            }
        })
        .collect()
}

/// Construct ColumnInfo entries for clustering key definitions found in SerializationHeader
fn build_clustering_key_columns(clustering_types: &[String]) -> Vec<super::header::ColumnInfo> {
    if clustering_types.is_empty() {
        return Vec::new();
    }

    let total = clustering_types.len();
    clustering_types
        .iter()
        .enumerate()
        .map(|(idx, marshal_type)| {
            // Issue #759: a DESC clustering column is encoded by wrapping its
            // comparator class in `ReversedType(...)`. Detect that authoritative
            // signal here, BEFORE `convert_marshal_type_to_cql` unwraps it, so
            // schema discovery can report `ClusteringOrder::Desc`. The CQL type
            // continues to come from `convert_marshal_type_to_cql`, which strips
            // `ReversedType` so the inner type's deserialization is undisturbed.
            let clustering_reversed = is_reversed_comparator(marshal_type);
            let cql_type = convert_marshal_type_to_cql(marshal_type);
            let name = if total == 1 {
                "clustering_key".to_string()
            } else {
                format!("clustering_key_{}", idx)
            };

            super::header::ColumnInfo {
                name,
                column_type: cql_type,
                is_primary_key: true,
                key_position: Some(idx as u16),
                is_static: false,
                is_clustering: true,
                clustering_reversed,
            }
        })
        .collect()
}

/// Parse SerializationHeader using sequential VInt parsing (Issue #216)
///
/// This function assumes the input starts EXACTLY at the SerializationHeader
/// (immediately after EncodingStats). It does NOT search for markers.
///
/// Format (from SerializationHeader.java):
/// [VInt pk_type_len] [pk_type_string]
/// [VInt ck_count] [for each: VInt ck_type_len, ck_type_string]
/// [VInt static_count] [for each: VInt name_len, name, VInt type_len, type]
/// [VInt regular_count] [for each: VInt name_len, name, VInt type_len, type]
fn parse_serialization_header_sequential(
    input: &[u8],
) -> IResult<&[u8], SerializationHeaderResult> {
    // Step 1: Parse partition key type (VInt length + string)
    let (input, pk_type_len) = parse_vuint(input)?;

    // Validate partition key type length
    if pk_type_len == 0 || pk_type_len > 5000 {
        log::debug!(
            "Invalid partition key type length: {} (expected 1-2000)",
            pk_type_len
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    let (input, pk_type_bytes) = nom::bytes::complete::take(pk_type_len as usize)(input)?;
    let partition_key_type = std::str::from_utf8(pk_type_bytes)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?
        .to_string();

    log::debug!(
        "Sequential parser: partition key type (len={}): {}",
        pk_type_len,
        partition_key_type
    );

    // Step 2: Parse clustering key count and types
    let (input, clustering_count) = parse_vuint(input)?;
    let clustering_count = clustering_count as usize;

    if clustering_count > 100 {
        log::debug!(
            "Invalid clustering key count: {} (expected 0-100)",
            clustering_count
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    log::debug!(
        "Sequential parser: clustering key count: {}",
        clustering_count
    );

    let mut clustering_key_types = Vec::with_capacity(clustering_count);
    let mut input = input;

    for idx in 0..clustering_count {
        let (remaining, type_len) = parse_vuint(input)?;

        if type_len == 0 || type_len > 5000 {
            log::debug!("Invalid clustering key {} type length: {}", idx, type_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, type_bytes) = nom::bytes::complete::take(type_len as usize)(remaining)?;
        let clustering_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        log::debug!(
            "Sequential parser: clustering key {} type (len={}): {}",
            idx,
            type_len,
            clustering_type
        );

        clustering_key_types.push(clustering_type);
        input = remaining;
    }

    // Step 3: Parse static columns
    let (input, static_count) = parse_vuint(input)?;
    let static_count = static_count as usize;

    if static_count > 200 {
        log::debug!(
            "Invalid static column count: {} (expected 0-200)",
            static_count
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    log::debug!("Sequential parser: static column count: {}", static_count);

    let mut static_columns = Vec::with_capacity(static_count);
    let mut input = input;

    for idx in 0..static_count {
        // Column name (VInt length + UTF-8)
        let (remaining, name_len) = parse_vuint(input)?;

        if name_len == 0 || name_len > 200 {
            log::debug!("Invalid static column {} name length: {}", idx, name_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, name_bytes) = nom::bytes::complete::take(name_len as usize)(remaining)?;
        let column_name = std::str::from_utf8(name_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        // Column type (VInt length + UTF-8)
        let (remaining, type_len) = parse_vuint(remaining)?;

        if type_len == 0 || type_len > 5000 {
            log::debug!(
                "Invalid static column '{}' type length: {}",
                column_name,
                type_len
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, type_bytes) = nom::bytes::complete::take(type_len as usize)(remaining)?;
        let internal_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        let cql_type = convert_marshal_type_to_cql(&internal_type);

        log::debug!(
            "Sequential parser: static column {}: name='{}', type='{}'",
            idx,
            column_name,
            cql_type
        );

        static_columns.push(super::header::ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: true,
            is_clustering: false,
            clustering_reversed: false,
        });

        input = remaining;
    }

    // Step 4: Parse regular columns
    let (input, regular_count) = parse_vuint(input)?;
    let regular_count = regular_count as usize;

    if regular_count > 500 {
        log::debug!(
            "Invalid regular column count: {} (expected 0-500)",
            regular_count
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    log::debug!("Sequential parser: regular column count: {}", regular_count);

    let mut regular_columns = Vec::with_capacity(regular_count);
    let mut input = input;

    for idx in 0..regular_count {
        // Column name (VInt length + UTF-8)
        let (remaining, name_len) = parse_vuint(input)?;

        if name_len == 0 || name_len > 200 {
            log::debug!("Invalid regular column {} name length: {}", idx, name_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, name_bytes) = nom::bytes::complete::take(name_len as usize)(remaining)?;
        let column_name = std::str::from_utf8(name_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        // Column type (VInt length + UTF-8)
        let (remaining, type_len) = parse_vuint(remaining)?;

        if type_len == 0 || type_len > 5000 {
            log::debug!(
                "Invalid regular column '{}' type length: {}",
                column_name,
                type_len
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, type_bytes) = nom::bytes::complete::take(type_len as usize)(remaining)?;
        let internal_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        let cql_type = convert_marshal_type_to_cql(&internal_type);

        log::debug!(
            "Sequential parser: regular column {}: name='{}', type='{}'",
            idx,
            column_name,
            cql_type
        );

        regular_columns.push(super::header::ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        });

        input = remaining;
    }

    // Combine static and regular columns (static columns first)
    let mut all_columns = static_columns;
    all_columns.extend(regular_columns);

    log::debug!(
        "Sequential parser complete: partition_key='{}', {} clustering keys, {} total columns",
        partition_key_type,
        clustering_key_types.len(),
        all_columns.len()
    );

    Ok((
        input,
        (vec![partition_key_type], clustering_key_types, all_columns),
    ))
}

/// Parse the schema portion of a SerializationHeader (after EncodingStats have been consumed).
///
/// Format:
/// 1. keyType (VInt length + UTF-8 type string)
/// 2. clusteringTypes (VInt count + [VInt type_len + type]*)
/// 3. staticColumns (VInt count + [VInt name_len + name + VInt type_len + type]*)
/// 4. regularColumns (VInt count + [VInt name_len + name + VInt type_len + type]*)
fn parse_serialization_header_schema(input: &[u8]) -> IResult<&[u8], SerializationHeaderResult> {
    // Parse keyType (partition key type)
    let (input, pk_type_len) = parse_vuint(input)?;
    if pk_type_len == 0 || pk_type_len > 5000 {
        log::debug!("Invalid pk_type_len: {}", pk_type_len);
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    if pk_type_len > 1000 {
        log::warn!(
            "Unusually long partition key type string: {} bytes (typical <1000)",
            pk_type_len
        );
    }

    let (input, pk_type_bytes) = take(pk_type_len as usize)(input)?;
    let partition_key_type = match std::str::from_utf8(pk_type_bytes) {
        Ok(s) => convert_marshal_type_to_cql(s),
        Err(_) => {
            log::debug!("Invalid UTF-8 in partition key type");
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
    };

    log::debug!(
        "HEADER: Partition key type: {} ({} bytes)",
        partition_key_type,
        pk_type_len
    );

    // Step 3: Parse clusteringTypes
    let (input, clustering_count) = parse_vuint(input)?;
    // Sanity check: Cassandra tables rarely have >100 clustering keys
    if clustering_count > 1000 {
        log::warn!(
            "Suspicious clustering_count={} in SerializationHeader (expected <100)",
            clustering_count
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    log::debug!("HEADER: {} clustering key types", clustering_count);

    let mut input = input;
    let mut clustering_key_types = Vec::with_capacity(clustering_count as usize);

    for i in 0..clustering_count {
        let (remaining, ck_type_len) = parse_vuint(input)?;
        if ck_type_len == 0 || ck_type_len > 5000 {
            log::debug!("Invalid clustering key type length: {}", ck_type_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
        if ck_type_len > 1000 {
            log::warn!(
                "Unusually long clustering key type string: {} bytes (typical <1000)",
                ck_type_len
            );
        }

        let (remaining, ck_type_bytes) = take(ck_type_len as usize)(remaining)?;
        // Issue #759: preserve the RAW comparator class name (including any
        // `ReversedType(...)` wrapper) here. `build_clustering_key_columns` is
        // the single place that converts to a CQL type AND derives clustering
        // order from the wrapper, so converting eagerly would discard the DESC
        // signal. Conversion in `build_clustering_key_columns` is idempotent for
        // already-CQL strings, keeping the other parse paths correct.
        let ck_type = match std::str::from_utf8(ck_type_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => {
                log::debug!("Invalid UTF-8 in clustering key type {}", i);
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        };

        log::debug!(
            "HEADER: Clustering key {}: {} ({} bytes)",
            i,
            ck_type,
            ck_type_len
        );
        clustering_key_types.push(ck_type);
        input = remaining;
    }

    // Step 4: Parse staticColumns
    let (input, static_count) = parse_vuint(input)?;
    // Sanity check: Cassandra tables rarely have >1000 static columns
    if static_count > 10000 {
        log::warn!(
            "Suspicious static_count={} in SerializationHeader (expected <1000)",
            static_count
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    log::debug!("HEADER: {} static columns", static_count);

    let mut input = input;
    let mut static_columns = Vec::with_capacity(static_count as usize);

    for i in 0..static_count {
        // Column name
        let (remaining, name_len) = parse_vuint(input)?;
        if name_len == 0 || name_len > 200 {
            log::debug!("Invalid static column name length: {}", name_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, name_bytes) = take(name_len as usize)(remaining)?;
        let column_name = match std::str::from_utf8(name_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => {
                log::debug!("Invalid UTF-8 in static column name {}", i);
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        };

        // Column type
        let (remaining, type_len) = parse_vuint(remaining)?;
        if type_len == 0 || type_len > 5000 {
            log::debug!("Invalid static column type length: {}", type_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
        if type_len > 1000 {
            log::warn!(
                "Unusually long static column type string: {} bytes (typical <1000)",
                type_len
            );
        }

        let (remaining, type_bytes) = take(type_len as usize)(remaining)?;
        let cql_type = match std::str::from_utf8(type_bytes) {
            Ok(s) => convert_marshal_type_to_cql(s),
            Err(_) => {
                log::debug!("Invalid UTF-8 in static column type {}", i);
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        };

        log::debug!(
            "HEADER: Static column '{}': {} ({} bytes)",
            column_name,
            cql_type,
            type_len
        );

        static_columns.push(super::header::ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: true,
            is_clustering: false,
            clustering_reversed: false,
        });

        input = remaining;
    }

    // Step 5: Parse regularColumns
    let (input, regular_count) = parse_vuint(input)?;
    // Sanity check: Cassandra tables rarely have >1000 regular columns
    if regular_count > 10000 {
        log::warn!(
            "Suspicious regular_count={} in SerializationHeader (expected <1000)",
            regular_count
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    log::debug!("HEADER: {} regular columns", regular_count);

    let mut input = input;
    let mut regular_columns = Vec::with_capacity(regular_count as usize);

    for i in 0..regular_count {
        // Column name
        let (remaining, name_len) = parse_vuint(input)?;
        if name_len == 0 || name_len > 200 {
            log::debug!("Invalid regular column name length: {}", name_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, name_bytes) = take(name_len as usize)(remaining)?;
        let column_name = match std::str::from_utf8(name_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => {
                log::debug!("Invalid UTF-8 in regular column name {}", i);
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        };

        // Column type
        let (remaining, type_len) = parse_vuint(remaining)?;
        if type_len == 0 || type_len > 5000 {
            log::debug!("Invalid regular column type length: {}", type_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
        if type_len > 1000 {
            log::warn!(
                "Unusually long regular column type string: {} bytes (typical <1000)",
                type_len
            );
        }

        let (remaining, type_bytes) = take(type_len as usize)(remaining)?;
        let cql_type = match std::str::from_utf8(type_bytes) {
            Ok(s) => convert_marshal_type_to_cql(s),
            Err(_) => {
                log::debug!("Invalid UTF-8 in regular column type {}", i);
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        };

        log::debug!(
            "HEADER: Regular column '{}': {} ({} bytes)",
            column_name,
            cql_type,
            type_len
        );

        regular_columns.push(super::header::ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        });

        input = remaining;
    }

    // Combine static and regular columns
    let mut all_columns = static_columns;
    all_columns.extend(regular_columns);

    log::debug!(
        "HEADER parsing complete: partition_key='{}', {} clustering keys, {} total columns",
        partition_key_type,
        clustering_key_types.len(),
        all_columns.len()
    );

    Ok((
        input,
        (vec![partition_key_type], clustering_key_types, all_columns),
    ))
}

/// Parse minimal EncodingStats section from nb-format Statistics.db
///
/// Returns: (min_timestamp, min_deletion_time, min_ttl, partition_keys, clustering_keys, columns)
///
/// # Arguments
/// * `input` - The data starting at the STATS component
/// * `full_input` - The complete Statistics.db content (needed for TOC-based HEADER lookup)
/// * `header_offset` - Optional offset to SerializationHeader from TOC (Issue #216)
/// * `gates` - Optional VersionGates for VG3 version-sensitive decoding decisions.
///   Pass `None` from standalone tools/tests to use nb-compatible defaults.
fn parse_minimal_encoding_stats<'a>(
    input: &'a [u8],
    full_input: &'a [u8],
    header_offset: Option<usize>,
    gates: Option<&VersionGates>,
) -> IResult<&'a [u8], EncodingStatsResult> {
    // The SERIALIZATION_HEADER component (type 3) starts with EncodingStats:
    //   [vuint minTimestamp_delta] [vuint minLocalDeletionTime_delta] [vuint minTTL_delta]
    // These are unsigned VInt deltas from epoch constants (see EncodingStats.Serializer).
    // Use the TOC-based offset to read from the correct location.

    let Some(offset) = header_offset else {
        log::debug!("No HEADER TOC offset, using fallback EncodingStats parsing");
        return parse_encoding_stats_fallback(input, gates);
    };

    if offset >= full_input.len() {
        log::warn!(
            "TOC offset 0x{:x} exceeds input length {}, using fallback",
            offset,
            full_input.len()
        );
        return parse_encoding_stats_fallback(input, gates);
    }

    let header_data = &full_input[offset..];
    log::debug!(
        "Parsing EncodingStats + SerializationHeader at TOC offset 0x{:x} ({} bytes available)",
        offset,
        header_data.len()
    );

    // Parse EncodingStats (3 unsigned VInts at start of SERIALIZATION_HEADER)
    let (rest, (min_timestamp, min_deletion_time, min_ttl)) =
        parse_encoding_stats_vuints(header_data, gates)?;

    log::debug!(
        "EncodingStats from HEADER: min_timestamp={}, min_deletion_time={}, min_ttl={:?}",
        min_timestamp,
        min_deletion_time,
        min_ttl
    );

    // Parse the rest of the SerializationHeader (schema info)
    let (partition_types, clustering_types, columns) = match parse_serialization_header_schema(rest)
    {
        Ok((_, result)) => result,
        Err(e) => {
            log::warn!(
                "Schema parsing after EncodingStats failed: {:?}, falling back to marker search",
                e
            );
            parse_serialization_header(input)?.1
        }
    };

    let (partition_key_columns, clustering_key_columns) =
        build_column_infos(&partition_types, &clustering_types);

    Ok((
        input,
        (
            min_timestamp,
            min_deletion_time,
            min_ttl,
            partition_key_columns,
            clustering_key_columns,
            columns,
        ),
    ))
}

/// Parse 3 EncodingStats unsigned VInt deltas and convert to absolute values by adding epochs.
/// Returns (min_timestamp, min_deletion_time, min_ttl).
///
/// # VG3 authority note
///
/// The `EncodingStats.Serializer` (EncodingStats.java:274-276) uses the SAME unsigned-VInt
/// + epoch-offset format for **both** `nb` and `oa`:
///
/// ```text
/// out.writeUnsignedVInt(stats.minTimestamp - TIMESTAMP_EPOCH)
/// out.writeUnsignedVInt32((int)(stats.minLocalDeletionTime - DELETION_TIME_EPOCH))
/// out.writeUnsignedVInt32(stats.minTTL - TTL_EPOCH)
/// ```
///
/// The `hasUIntDeletionTime` gate (BigFormat.java:409) affects only the **StatsMetadata**
/// (STATS component in Statistics.db), not the SerializationHeader component where
/// EncodingStats lives.  The epoch-relative decoding here is correct for both nb and oa.
/// `gates` is accepted (not consumed) for API completeness; `None` is fine too.
fn parse_encoding_stats_vuints<'a>(
    input: &'a [u8],
    // VG3: gates threaded here for authority completeness.
    // Authority investigation: the EncodingStats.Serializer (EncodingStats.java:274-276)
    // uses the SAME unsigned VInt + epoch format for both nb and oa:
    //   out.writeUnsignedVInt(stats.minTimestamp - TIMESTAMP_EPOCH)
    //   out.writeUnsignedVInt32((int)(stats.minLocalDeletionTime - DELETION_TIME_EPOCH))
    //   out.writeUnsignedVInt32(stats.minTTL - TTL_EPOCH)
    // The `hasUIntDeletionTime` gate (BigFormat.java:409) affects ONLY the
    // StatsMetadata section (Statistics.db STATS component), NOT the
    // SerializationHeader component where EncodingStats lives.  No decode
    // difference applies here.  Gates accepted but not consumed.
    _gates: Option<&VersionGates>,
) -> IResult<&'a [u8], (i64, i64, Option<i64>)> {
    let (rest, min_ts_delta) = parse_vuint(input)?;
    let (rest, min_ldt_delta) = parse_vuint(rest)?;
    let (rest, min_ttl_delta) = parse_vuint(rest)?;

    Ok((
        rest,
        (
            min_ts_delta as i64 + TIMESTAMP_EPOCH,
            // EncodingStats.java:289: `long minLocalDeletionTime = in.readUnsignedVInt32() + DELETION_TIME_EPOCH`
            // Same formula for nb and oa — DELETION_TIME_EPOCH is always added back.
            min_ldt_delta as i64 + DELETION_TIME_EPOCH,
            Some(min_ttl_delta as i64 + TTL_EPOCH),
        ),
    ))
}

/// Build ColumnInfo vectors from parsed type strings.
fn build_column_infos(
    partition_types: &[String],
    clustering_types: &[String],
) -> (
    Vec<super::header::ColumnInfo>,
    Vec<super::header::ColumnInfo>,
) {
    let partition_key_columns = build_partition_key_columns(partition_types);
    let clustering_key_columns = build_clustering_key_columns(clustering_types);

    log::debug!(
        "Constructed ColumnInfo entries from SerializationHeader: {} partition keys, {} clustering keys",
        partition_key_columns.len(),
        clustering_key_columns.len()
    );

    (partition_key_columns, clustering_key_columns)
}

/// Fallback EncodingStats parser for when no TOC HEADER offset is available.
/// Uses ad-hoc parsing from the data following the file header.
fn parse_encoding_stats_fallback<'a>(
    input: &'a [u8],
    gates: Option<&VersionGates>,
) -> IResult<&'a [u8], EncodingStatsResult> {
    // Skip metadata_type (u32 BE) at start of data section
    let (rest, _metadata_type) = be_u32(input)?;

    // Parse data section length (VInt)
    let (rest, _data_length) = parse_vuint(rest)?;

    // Parse partitioner string length (VInt)
    let (rest, partitioner_len) = parse_vuint(rest)?;

    // Skip partitioner string
    let (rest, _) = take(partitioner_len as usize)(rest)?;

    // Skip additional metadata (observed: ~2 VInts before timestamp fields)
    let (rest, _metadata1) = parse_vuint(rest)?;
    let (rest, _metadata2) = parse_vuint(rest)?;

    // Parse EncodingStats fields (unsigned VInt deltas from epoch)
    let (rest, (min_timestamp, min_deletion_time, min_ttl)) =
        parse_encoding_stats_vuints(rest, gates)?;

    // Fall back to marker-based header search for schema
    let (_, (partition_types, clustering_types, columns)) = parse_serialization_header(rest)?;

    let (partition_key_columns, clustering_key_columns) =
        build_column_infos(&partition_types, &clustering_types);

    Ok((
        input,
        (
            min_timestamp,
            min_deletion_time,
            min_ttl,
            partition_key_columns,
            clustering_key_columns,
            columns,
        ),
    ))
}

/// Main enhanced parser for real Statistics.db files (minimal implementation for Issue #162)
///
/// This function parses the header and minimal EncodingStats fields from nb-format
/// Statistics.db files. Only timestamp-related fields are extracted; all other
/// statistics (histograms, column stats, etc.) are populated with placeholder values.
///
/// This is sufficient for V5CompressedLegacy parser which requires min_timestamp,
/// min_local_deletion_time, and min_ttl for delta decoding baseline.
///
/// # Arguments
///
/// * `gates` - Optional [`VersionGates`] for version-sensitive decoding decisions
///   (VG1 plumbing).  Pass `None` from standalone tools/tests to use nb-compatible
///   defaults; pass `Some(&gates)` from `SSTableReader` to enable VG3 gating.
///
/// # Returns
///
/// SSTableStatistics with only header and timestamp_stats populated from real data.
pub fn parse_enhanced_statistics_file<'a>(
    input: &'a [u8],
    gates: Option<&VersionGates>,
) -> IResult<&'a [u8], SSTableStatistics> {
    // Parse the 32-byte header
    let (remaining, header) = parse_nb_format_header(input)?;

    // Parse minimal statistics data (EncodingStats + SerializationHeader columns)
    // Pass full input for TOC-based HEADER offset lookup (Issue #216)
    let result = parse_nb_format_statistics_data(remaining, &header, input, gates);

    match result {
        Ok((
            row_stats,
            mut timestamp_stats,
            table_stats,
            partition_stats,
            compression_stats,
            partition_columns,
            clustering_columns,
            columns,
        )) => {
            log::debug!(
                "Successfully parsed Statistics.db serialization header: {} partition keys, {} clustering keys, {} regular columns",
                partition_columns.len(),
                clustering_columns.len(),
                columns.len()
            );

            // Best-effort STATS-extras decode over the FULL buffer (with TOC):
            // recover the authoritative maxLocalDeletionTime (the inner parser
            // leaves it as a placeholder equal to min_deletion_time) and the
            // estimated tombstone-drop-times histogram (Issue #1073). This is
            // strictly additive — a Statistics.db that parses today must keep
            // parsing, so an Err here is logged and the placeholders are kept.
            let tombstone_drop_times =
                match crate::parser::repair_metadata::parse_stats_extras(input, gates) {
                    Ok(extras) => {
                        // Only set max_deletion_time. Do NOT touch min_deletion_time
                        // (the EncodingStats min baseline consumed elsewhere).
                        timestamp_stats.max_deletion_time = extras.max_local_deletion_time;
                        extras.tombstone_drop_times
                    }
                    Err(e) => {
                        log::debug!(
                            "Best-effort STATS-extras decode failed; keeping placeholders: {:?}",
                            e
                        );
                        vec![]
                    }
                };

            let statistics = SSTableStatistics {
                header,
                row_stats,
                timestamp_stats,
                column_stats: vec![],
                table_stats,
                partition_stats,
                compression_stats,
                metadata: std::collections::HashMap::new(),
                serialization_header_columns: columns,
                serialization_header_partition_keys: partition_columns,
                serialization_header_clustering_keys: clustering_columns,
                tombstone_drop_times,
            };

            Ok((remaining, statistics))
        }
        Err(e) => {
            // Convert Error to nom::Err
            log::warn!("Failed to parse nb-format Statistics.db: {}", e);
            Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )))
        }
    }
}

/// Enhanced statistics reader with fallback (minimal implementation for Issue #162)
///
/// Attempts to parse nb-format Statistics.db with minimal EncodingStats extraction.
/// This provides the minimum fields needed for delta-coded timestamp decoding.
///
/// # Arguments
///
/// * `gates` - Optional [`VersionGates`] threaded from `SSTableReader` for VG3
///   version-sensitive decoding.  Pass `None` from standalone tools/tests; the
///   nb-compatible behaviour is used when `gates` is `None`.
///
/// # Returns
///
/// SSTableStatistics with minimal fields populated, or error if parsing fails.
pub fn parse_statistics_with_fallback<'a>(
    input: &'a [u8],
    gates: Option<&VersionGates>,
) -> IResult<&'a [u8], SSTableStatistics> {
    // Try the minimal enhanced parser
    parse_enhanced_statistics_file(input, gates)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Issue #759: ReversedType wrapping is the authoritative DESC signal.
    #[test]
    fn test_is_reversed_comparator() {
        // Fully-qualified and short forms both denote DESC.
        assert!(is_reversed_comparator(
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)"
        ));
        assert!(is_reversed_comparator("ReversedType(Int32Type)"));
        // The header sometimes prefixes the top-level type with '(' — still DESC.
        assert!(is_reversed_comparator(
            "(org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.SimpleDateType))"
        ));
        // ...and sometimes with a structural '[' (roborev job 43) — still DESC.
        assert!(is_reversed_comparator(
            "[org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)"
        ));
        assert!(is_reversed_comparator("[ReversedType(Int32Type)"));
        // Non-reversed comparators are ASC.
        assert!(!is_reversed_comparator(
            "org.apache.cassandra.db.marshal.UTF8Type"
        ));
        assert!(!is_reversed_comparator("Int32Type"));
        // ReversedType only counts as the outer wrapper, not nested as an arg.
        assert!(!is_reversed_comparator(
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type))"
        ));
    }

    // Issue #759: ReversedType drives ClusteringColumn order without disturbing
    // the inner type's CQL conversion (used for deserialization).
    #[test]
    fn test_build_clustering_key_columns_reversed_order() {
        let clustering_types = vec![
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)".to_string(),
            "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)".to_string(),
        ];

        let cols = build_clustering_key_columns(&clustering_types);
        assert_eq!(cols.len(), 3);

        // DESC columns flag clustering_reversed; inner CQL type is preserved.
        assert!(cols[0].clustering_reversed);
        assert_eq!(cols[0].column_type, "timestamp");
        assert!(!cols[1].clustering_reversed);
        assert_eq!(cols[1].column_type, "text");
        assert!(cols[2].clustering_reversed);
        assert_eq!(cols[2].column_type, "int");

        // Never leak the ReversedType wrapper into the resolved CQL type.
        for col in &cols {
            assert!(!col.column_type.contains("Reversed"));
            assert!(col.is_clustering);
        }
    }

    /// Regression (roborev job 48): a bracket-prefixed reversed comparator must
    /// be DESC *and* resolve to the correct inner CQL type (not `timestamptype`).
    #[test]
    fn test_build_clustering_key_columns_bracket_prefixed_reversed() {
        let clustering_types = vec![
            "[org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)"
                .to_string(),
        ];
        let cols = build_clustering_key_columns(&clustering_types);
        assert_eq!(cols.len(), 1);
        assert!(
            cols[0].clustering_reversed,
            "bracket-prefixed ReversedType is DESC"
        );
        assert_eq!(
            cols[0].column_type, "timestamp",
            "inner CQL type must resolve through the bracket, got {}",
            cols[0].column_type
        );
    }

    #[test]
    fn test_serialization_header_with_no_clustering_keys() {
        // Test SerializationHeader with partition key and regular columns, no clustering keys
        // Format: [VInt partition_type_len] [0x00 0x00] [partition_type] [clustering_count=0] [0x00 0x00 column_count] [columns...]

        let mut test_data = vec![];

        // Partition key type: 41 bytes "(org.apache.cassandra.db.marshal.UUIDType"
        let partition_type = b"(org.apache.cassandra.db.marshal.UUIDType";
        test_data.extend_from_slice(&[0x00, 0x00]); // Marker
        test_data.push(partition_type.len() as u8);
        test_data.extend_from_slice(partition_type);

        // Clustering key count = 0
        test_data.push(0x00);

        // Regular columns section: separator (0x00) + count
        test_data.push(0x00); // section separator
        test_data.push(0x02); // column count

        // Column 1: "id" (UUID)
        test_data.push(0x02); // name length = 2
        test_data.extend_from_slice(b"id");
        test_data.push(0x28); // type length = 40
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UUIDType");

        // Column 2: "name" (UTF8/text)
        test_data.push(0x04); // name length = 4
        test_data.extend_from_slice(b"name");
        test_data.push(0x28); // type length = 40
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

        // Add some garbage data before the SerializationHeader
        let mut full_data = vec![0xFF; 100];
        full_data.extend_from_slice(&test_data);

        let result = parse_serialization_header(&full_data);
        assert!(
            result.is_ok(),
            "Failed to parse SerializationHeader: {:?}",
            result.as_ref().err()
        );

        let (_remaining, (partition_types, clustering_types, columns)) = result.unwrap();

        // Verify partition key
        assert_eq!(partition_types.len(), 1, "Expected 1 partition key");
        assert!(partition_types[0].contains("UUIDType"));

        // Verify clustering keys (should be none)
        assert_eq!(clustering_types.len(), 0, "Expected 0 clustering keys");

        // Verify regular columns
        assert_eq!(columns.len(), 2, "Expected 2 columns");
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].column_type, "uuid");
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].column_type, "text");
    }

    #[test]
    fn test_serialization_header_with_clustering_keys() {
        // Test SerializationHeader with partition key, 2 clustering keys, and regular columns

        let mut test_data = vec![];

        // Partition key type: 41 bytes
        let partition_type = b"(org.apache.cassandra.db.marshal.UUIDType";
        test_data.extend_from_slice(&[0x00, 0x00]); // Marker
        test_data.push(partition_type.len() as u8);
        test_data.extend_from_slice(partition_type);

        // Clustering key count = 2
        test_data.push(0x02);

        // Clustering key 1: ReversedType(TimestampType)
        let ck1 =
            b"[org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)";
        test_data.push(ck1.len() as u8);
        test_data.extend_from_slice(ck1);

        // Clustering key 2: UTF8Type
        let ck2 = b"(org.apache.cassandra.db.marshal.UTF8Type)";
        test_data.push(ck2.len() as u8);
        test_data.extend_from_slice(ck2);

        // Regular columns section
        test_data.push(0x00); // separator
        test_data.push(0x02); // count

        // Column 1: "data" (UTF8)
        test_data.push(0x04); // name length
        test_data.extend_from_slice(b"data");
        test_data.push(0x28); // type length
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

        // Column 2: "value" (Int32)
        test_data.push(0x05); // name length
        test_data.extend_from_slice(b"value");
        test_data.push(0x29); // type length
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.Int32Type");

        // Add garbage data before SerializationHeader
        let mut full_data = vec![0xFF; 100];
        full_data.extend_from_slice(&test_data);

        let result = parse_serialization_header(&full_data);
        assert!(
            result.is_ok(),
            "Failed to parse SerializationHeader with clustering keys: {:?}",
            result.err()
        );

        let (_remaining, (partition_types, clustering_types, columns)) = result.unwrap();

        // Verify partition key
        assert_eq!(partition_types.len(), 1);
        assert!(partition_types[0].contains("UUIDType"));

        // Verify clustering keys
        assert_eq!(clustering_types.len(), 2, "Expected 2 clustering keys");
        assert!(clustering_types[0].contains("ReversedType"));
        assert!(clustering_types[0].contains("TimestampType"));
        assert!(clustering_types[1].contains("UTF8Type"));

        // Verify regular columns
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "data");
        assert_eq!(columns[0].column_type, "text");
        assert_eq!(columns[1].name, "value");
        assert_eq!(columns[1].column_type, "int");
    }

    #[test]
    fn test_serialization_header_with_static_columns() {
        // Test SerializationHeader with static columns (Issue #210)
        // Schema: partition key (uuid), clustering key (timestamp),
        //         static column (text), regular columns (text, int)

        let mut test_data = vec![];

        // Marker
        test_data.extend_from_slice(&[0x00, 0x00]);

        // Partition key type: UUIDType (40 bytes)
        let partition_type = b"org.apache.cassandra.db.marshal.UUIDType";
        test_data.push(partition_type.len() as u8);
        test_data.extend_from_slice(partition_type);

        // Clustering key count = 1
        test_data.push(0x01);

        // Clustering key 1: TimestampType (45 bytes)
        let ck1 = b"org.apache.cassandra.db.marshal.TimestampType";
        test_data.push(ck1.len() as u8);
        test_data.extend_from_slice(ck1);

        // Static column count = 1 (NOT a separator - this is the key fix!)
        test_data.push(0x01);

        // Static column 1: "static_data" (UTF8Type)
        test_data.push(0x0b); // name length = 11
        test_data.extend_from_slice(b"static_data");
        test_data.push(0x28); // type length = 40
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

        // Regular column count = 2
        test_data.push(0x02);

        // Regular column 1: "row_data" (UTF8)
        test_data.push(0x08); // name length
        test_data.extend_from_slice(b"row_data");
        test_data.push(0x28); // type length = 40
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

        // Regular column 2: "row_value" (Int32)
        test_data.push(0x09); // name length
        test_data.extend_from_slice(b"row_value");
        test_data.push(0x29); // type length = 41
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.Int32Type");

        // Add garbage data before SerializationHeader
        let mut full_data = vec![0xFF; 100];
        full_data.extend_from_slice(&test_data);

        let result = parse_serialization_header(&full_data);
        assert!(
            result.is_ok(),
            "Failed to parse SerializationHeader with static columns: {:?}",
            result.err()
        );

        let (_remaining, (partition_types, clustering_types, columns)) = result.unwrap();

        // Verify partition key
        assert_eq!(partition_types.len(), 1);
        assert!(partition_types[0].contains("UUIDType"));

        // Verify clustering keys
        assert_eq!(clustering_types.len(), 1);
        assert!(clustering_types[0].contains("TimestampType"));

        // Verify columns (static + regular = 3 total)
        assert_eq!(
            columns.len(),
            3,
            "Expected 3 columns (1 static + 2 regular)"
        );

        // Static column should be first and marked as static
        assert_eq!(columns[0].name, "static_data");
        assert_eq!(columns[0].column_type, "text");
        assert!(
            columns[0].is_static,
            "static_data should be marked as static"
        );

        // Regular columns should NOT be static
        assert_eq!(columns[1].name, "row_data");
        assert_eq!(columns[1].column_type, "text");
        assert!(
            !columns[1].is_static,
            "row_data should NOT be marked as static"
        );

        assert_eq!(columns[2].name, "row_value");
        assert_eq!(columns[2].column_type, "int");
        assert!(
            !columns[2].is_static,
            "row_value should NOT be marked as static"
        );
    }

    #[test]
    fn test_marshal_type_conversion() {
        // Simple types should be converted to CQL names
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.Int32Type"),
            "int"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.UTF8Type"),
            "text"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.UUIDType"),
            "uuid"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.TimestampType"),
            "timestamp"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.DecimalType"),
            "decimal"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.SimpleDataType"),
            "simpledata"
        );

        // UserType should be preserved unchanged (contains critical metadata)
        let udt = "org.apache.cassandra.db.marshal.UserType(test_collections,616464726573735f74797065,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type)";
        assert_eq!(
            convert_marshal_type_to_cql(udt),
            udt,
            "UserType definitions must be preserved to retain keyspace, type name, and field metadata"
        );

        // Frozen UserType should also be preserved
        let frozen_udt = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(test_collections,616464726573735f74797065,737472656574:org.apache.cassandra.db.marshal.UTF8Type))";
        assert!(
            convert_marshal_type_to_cql(frozen_udt).contains("UserType("),
            "UserType inside FrozenType should be preserved"
        );

        // List of frozen UDT should preserve the UserType
        let list_udt = "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(test_collections,616464726573735f74797065,737472656574:org.apache.cassandra.db.marshal.UTF8Type)))";
        assert!(
            convert_marshal_type_to_cql(list_udt).contains("UserType("),
            "UserType inside List should be preserved"
        );
    }

    #[test]
    fn test_nb_format_header_parsing() {
        // Test data based on real file hex dump
        let test_data = vec![
            0x00, 0x00, 0x00, 0x04, // version_type = 4
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14, 0xd4, // checksum/more = 5332
        ];

        let result = parse_nb_format_header(&test_data);
        assert!(result.is_ok());

        let (_, header) = result.unwrap();
        assert_eq!(header.version, 4);
        assert_eq!(header.statistics_kind, 0x2629_1b05);
        assert_eq!(header.data_length, 44);
        assert_eq!(header.metadata1, 1);
        assert_eq!(header.metadata2, 101);
        assert_eq!(header.metadata3, 2);
        assert_eq!(header.checksum, 0x14d4);
    }

    #[test]
    fn test_statistics_data_extraction_with_invalid_data() {
        // Test with insufficient/invalid data - should fail to parse VInts
        let header = StatisticsHeader {
            version: 4,
            statistics_kind: 0x2629_1b05,
            data_length: 44,
            metadata1: 1,
            metadata2: 101,
            metadata3: 2,
            checksum: 0x14d4,
            table_id: None,
        };

        let dummy_data = vec![0xFF; 10]; // Too short to parse properly
        let result = parse_nb_format_statistics_data(&dummy_data, &header, &dummy_data, None);

        // Should return error because data is too short for VInt parsing
        assert!(result.is_err());
    }

    #[test]
    fn test_enhanced_statistics_file_with_incomplete_data() {
        // Test data with valid header but missing data section
        let test_data = vec![
            0x00, 0x00, 0x00, 0x04, // version = 4
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14,
            0xd4, // checksum = 5332
                  // No data section - should fail parsing
        ];

        let result = parse_enhanced_statistics_file(&test_data, None);

        // Should fail since there's no data section to parse
        assert!(result.is_err());
    }

    #[test]
    fn test_parser_fallback_with_incomplete_data() {
        // Test with valid header but incomplete data
        let test_data = vec![
            0x00, 0x00, 0x00, 0x04, // version = 4
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14, 0xd4, // checksum = 5332
        ];

        let result = parse_statistics_with_fallback(&test_data, None);

        // Should fail - incomplete data
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_data_returns_error() {
        // Test with insufficient data
        let invalid_data = vec![0xFF; 10];
        let result = parse_statistics_with_fallback(&invalid_data, None);
        assert!(result.is_err(), "Invalid data should fail to parse");
    }

    #[test]
    fn test_partition_key_extraction_via_backtracking() {
        // Test the backtracking logic to extract partition key type before the column marker
        // This simulates the real ttl_test_table case where we have:
        // VInt(40) + "org.apache.cassandra.db.marshal.UUIDType" + 0x00 0x00 + [count]
        // Note: Real files use 2-byte VInt: 0x80 0x28 for length 40

        let mut test_data = vec![];

        // Add some garbage data before the partition key
        test_data.extend_from_slice(&[0xFF; 50]);

        // Partition key type: 40 bytes "org.apache.cassandra.db.marshal.UUIDType"
        test_data.extend_from_slice(&[0x80, 0x28]); // VInt: 40 (2-byte encoding)
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UUIDType");

        // Marker: 0x00 0x00 followed by column count
        // NOTE: In SerializationHeader, partition keys are NOT in the regular columns section
        // Only regular (non-key) columns are listed here
        test_data.push(0x00); // separator
        test_data.push(0x02); // 2 regular columns

        // Regular Column 1: "expiring_value" (Int32)
        test_data.push(0x0E); // name length = 14
        test_data.extend_from_slice(b"expiring_value");
        test_data.push(0x29); // type length = 41
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.Int32Type");

        // Regular Column 2: "session_info" (UTF8)
        test_data.push(0x0C); // name length = 12
        test_data.extend_from_slice(b"session_info");
        test_data.push(0x28); // type length = 40
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

        // Parse the regular columns section which should extract partition key via backtracking
        let result = parse_regular_columns(&test_data);
        assert!(
            result.is_ok(),
            "Failed to parse columns with backtracking: {:?}",
            result.err()
        );

        let (_remaining, (partition_keys, columns)) = result.unwrap();

        // Verify partition key was extracted
        assert_eq!(
            partition_keys.len(),
            1,
            "Expected 1 partition key via backtracking"
        );
        assert_eq!(
            partition_keys[0],
            "org.apache.cassandra.db.marshal.UUIDType"
        );

        // Verify regular columns
        assert_eq!(columns.len(), 2, "Expected 2 regular columns");
        assert_eq!(columns[0].name, "expiring_value");
        assert_eq!(columns[0].column_type, "int");
        assert!(!columns[0].is_primary_key);
        assert_eq!(columns[1].name, "session_info");
        assert_eq!(columns[1].column_type, "text");
        assert!(!columns[1].is_primary_key);
    }

    #[test]
    fn test_partition_key_extraction_with_longer_type() {
        // Test with a composite partition key type (longer type string)
        let mut test_data = vec![0xFF; 100]; // Garbage prefix

        // CompositeType with multiple components: 75 bytes
        let composite_type =
            "(org.apache.cassandra.db.marshal.CompositeType(UTF8Type,Int32Type,UUIDType)";
        let type_len = composite_type.len() as u8;

        // VInt encode the length (75 = 0x4B, fits in single byte)
        test_data.push(type_len);
        test_data.extend_from_slice(composite_type.as_bytes());

        // Marker + column count
        test_data.push(0x00); // separator
        test_data.push(0x01); // column count

        // Single column: "data" (UTF8)
        test_data.push(0x04);
        test_data.extend_from_slice(b"data");
        test_data.push(0x28);
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

        let result = parse_regular_columns(&test_data);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let (_remaining, (partition_keys, columns)) = result.unwrap();

        assert_eq!(partition_keys.len(), 1);
        assert_eq!(partition_keys[0], composite_type);

        // Expect 1 regular column
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "data");
        assert!(!columns[0].is_primary_key);
    }

    #[test]
    fn test_backtracking_with_no_partition_key() {
        // Test case where there's no partition key before the marker
        // This should still parse columns successfully but return empty partition key list

        let mut test_data = vec![];

        // Just the marker and columns, no partition key type before
        test_data.push(0x00); // separator
        test_data.push(0x01); // count

        // Column: "name" (UTF8)
        test_data.push(0x04);
        test_data.extend_from_slice(b"name");
        test_data.push(0x28);
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

        let result = parse_regular_columns(&test_data);
        assert!(result.is_ok());

        let (_remaining, (partition_keys, columns)) = result.unwrap();

        assert_eq!(partition_keys.len(), 0, "Should have no partition keys");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "name");
    }

    #[test]
    fn test_backtracking_rejects_invalid_types() {
        // Test that backtracking rejects strings that don't match Cassandra type patterns
        let mut test_data = vec![0xFF; 50];

        // Invalid type: doesn't start with '(' and doesn't contain "org.apache.cassandra"
        test_data.push(0x15); // VInt: 21 bytes
        test_data.extend_from_slice(b"InvalidTypeDescriptor");

        // Marker + column count
        test_data.extend_from_slice(&[0x00, 0x00, 0x01]);

        // Column
        test_data.push(0x04);
        test_data.extend_from_slice(b"test");
        test_data.push(0x28);
        test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

        let result = parse_regular_columns(&test_data);
        assert!(result.is_ok());

        let (_remaining, (partition_keys, _columns)) = result.unwrap();

        // Should not extract the invalid type
        assert_eq!(
            partition_keys.len(),
            0,
            "Should reject invalid type pattern"
        );
    }
}
