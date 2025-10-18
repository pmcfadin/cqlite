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
use super::vint::{parse_vint, parse_vuint};
use crate::error::{Error, Result};
use nom::{bytes::complete::take, number::complete::be_u32, IResult};

/// Type alias for EncodingStats parse result to reduce complexity
type EncodingStatsResult = (i64, i64, Option<i64>, Vec<super::header::ColumnInfo>);

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
pub fn parse_nb_format_statistics_data(
    input: &[u8],
    header: &StatisticsHeader,
) -> Result<(
    RowStatistics,
    TimestampStatistics,
    TableStatistics,
    PartitionStatistics,
    CompressionStatistics,
    Vec<super::header::ColumnInfo>,
)> {
    // Parse the EncodingStats section from the data following the header
    let result = parse_minimal_encoding_stats(input);

    match result {
        Ok((_, (min_timestamp, min_deletion_time, min_ttl, columns))) => {
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
                columns,
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

    // Search for partition key type section (max search 8KB to avoid scanning entire file)
    while search_offset + 10 < input.len() && search_offset < 8192 {
        // Look for pattern: 0x00 0x00 0x28 "org..."
        // where 0x28 is '(' indicating partition key type
        // The VInt length comes before the 0x00 0x00, but we search for the marker pattern
        if input[search_offset] == 0x00
            && input[search_offset + 1] == 0x00
            && input[search_offset + 2] == 0x28
        // '(' character
        {
            log::debug!(
                "Found potential partition key marker at offset {}",
                search_offset
            );
            // Found potential partition key type marker
            // Try to parse from the VInt before this offset
            // The VInt could be 1-9 bytes before the marker
            for vint_offset in 1..=9 {
                if search_offset < vint_offset {
                    break;
                }
                let start_offset = search_offset - vint_offset;
                let result = parse_serialization_header_at_offset(&input[start_offset..]);
                if result.is_ok() {
                    log::debug!(
                        "Successfully parsed SerializationHeader at offset {}",
                        start_offset
                    );
                    return result;
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
    Ok((input, (Vec::new(), Vec::new(), Vec::new())))
}

/// Parse SerializationHeader structure starting at a known offset
fn parse_serialization_header_at_offset(input: &[u8]) -> IResult<&[u8], SerializationHeaderResult> {
    let _original_input = input;

    // Step 1: Parse partition key type length (VInt)
    let (input, partition_type_len) = parse_vuint(input)?;

    log::debug!("Partition key type length: {} bytes", partition_type_len);

    // Step 2: Expect 0x00 0x00 marker
    if input.len() < 2 || input[0] != 0x00 || input[1] != 0x00 {
        log::debug!(
            "Expected 0x00 0x00 marker after partition key type length, found: {:02x} {:02x}",
            input[0],
            input[1]
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    let input = &input[2..];

    // Step 3: Parse partition key type string
    if input.len() < partition_type_len as usize {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let partition_type_bytes = &input[..partition_type_len as usize];
    let partition_key_type = std::str::from_utf8(partition_type_bytes)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?
        .to_string();

    log::debug!("Partition key type: {}", partition_key_type);

    let input = &input[partition_type_len as usize..];

    // Step 4: Parse clustering key count (single byte, can be 0)
    if input.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let clustering_count = input[0] as usize;
    let mut input = &input[1..];

    log::debug!("Clustering key count: {}", clustering_count);

    // Step 5: Parse clustering key types
    let mut clustering_key_types = Vec::with_capacity(clustering_count);

    for idx in 0..clustering_count {
        // Find the end of the clustering key type descriptor
        // Clustering types end with ')' when parentheses are balanced
        // They may start with '[' (composite marker) or '(' (simple type)
        // The '[' is just a prefix and doesn't have a matching ']'

        let mut type_end = 0;
        let mut paren_depth = 0;
        let mut found_end = false;

        for (pos, &byte) in input.iter().enumerate() {
            match byte {
                b'(' => paren_depth += 1,
                b')' => {
                    if paren_depth > 0 {
                        paren_depth -= 1;
                        if paren_depth == 0 {
                            // Found the end of this clustering key type
                            type_end = pos + 1;
                            found_end = true;
                            break;
                        }
                    }
                }
                b'[' => {
                    // Composite type marker - just skip it, doesn't affect balance
                }
                _ => {}
            }
        }

        if !found_end || type_end == 0 {
            log::debug!(
                "Failed to find end of clustering key type {} at position",
                idx
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let clustering_type_bytes = &input[..type_end];
        let clustering_type = std::str::from_utf8(clustering_type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        log::debug!("Clustering key {} type: {}", idx, clustering_type);

        clustering_key_types.push(clustering_type);
        input = &input[type_end..];
    }

    // Step 6: Parse regular columns section
    // Look for marker followed by column definitions
    let (input, (_backtrack_partition_keys, columns)) = parse_regular_columns(input)?;

    log::debug!(
        "Successfully parsed SerializationHeader: {} partition keys, {} clustering keys, {} regular columns",
        1, // Always 1 partition key in current implementation
        clustering_key_types.len(),
        columns.len()
    );

    Ok((
        input,
        (vec![partition_key_type], clustering_key_types, columns),
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

    // Search for column section marker pattern: 0x00 0x00 [count]
    // where [count] is the number of regular columns (1-50 typical)
    let mut search_offset = 0;
    let mut partition_key_types = Vec::new();

    // Search for column section (max search 8KB to handle large gaps)
    // The SerializationHeader can be up to 5KB into the file after histogram data
    while search_offset + 3 < input.len() && search_offset < 8192 {
        // Look for pattern: 0x00 0x00 [count] where count is 1-50
        // This is the section marker followed by column count
        if input[search_offset] == 0x00
            && input[search_offset + 1] == 0x00
            && input[search_offset + 2] > 0
            && input[search_offset + 2] <= 50
        {
            // NEW: Extract partition key by backtracking from the marker
            log::debug!(
                "Attempting to extract partition key by backtracking from marker at offset {}",
                search_offset
            );
            if let Some(pk_type) = extract_partition_key_before_marker(input, search_offset) {
                log::debug!("Found partition key type before marker: {}", pk_type);
                partition_key_types.push(pk_type);
            } else {
                log::debug!(
                    "No partition key type found via backtracking at offset {}",
                    search_offset
                );
            }

            let column_count = input[search_offset + 2] as usize;
            let mut pos = search_offset + 3;

            // Log pattern detection
            let context_len = std::cmp::min(128, input.len() - search_offset);
            let context_hex: String = input[search_offset..search_offset + context_len]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            log::debug!(
                "Pattern found at offset {}: count={}, next 128 bytes: {}",
                search_offset,
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
                        search_offset,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                // Column name length (VInt)
                let (remaining, name_len) = match parse_vuint(&input[pos..]) {
                    Ok(r) => r,
                    Err(e) => {
                        log::debug!(
                            "Column {} parsing failed at offset {}: VInt parse error for name_len at pos {}: {:?}",
                            col_idx,
                            search_offset,
                            pos,
                            e
                        );
                        parse_success = false;
                        break;
                    }
                };
                pos = input.len() - remaining.len();

                // Sanity check name length
                if name_len == 0 || name_len > 200 || pos + name_len as usize > input.len() {
                    log::debug!(
                        "Column {} parsing failed at offset {}: name_len sanity check failed (name_len={}, pos={}, buffer_len={})",
                        col_idx,
                        search_offset,
                        name_len,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                // Column name (UTF-8 string)
                let name_bytes = &input[pos..pos + name_len as usize];
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
                            search_offset,
                            pos,
                            name_len,
                            e,
                            name_hex
                        );
                        parse_success = false;
                        break;
                    }
                };
                pos += name_len as usize;

                // Column type length (VInt)
                let (remaining, type_len) = match parse_vuint(&input[pos..]) {
                    Ok(r) => r,
                    Err(e) => {
                        log::debug!(
                            "Column {} ('{}') parsing failed at offset {}: VInt parse error for type_len at pos {}: {:?}",
                            col_idx,
                            column_name,
                            search_offset,
                            pos,
                            e
                        );
                        parse_success = false;
                        break;
                    }
                };
                pos = input.len() - remaining.len();

                // Sanity check type length
                if type_len == 0 || type_len > 200 || pos + type_len as usize > input.len() {
                    log::debug!(
                        "Column {} ('{}') parsing failed at offset {}: type_len sanity check failed (type_len={}, pos={}, buffer_len={})",
                        col_idx,
                        column_name,
                        search_offset,
                        type_len,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                // Column type (Cassandra internal type name)
                let type_bytes = &input[pos..pos + type_len as usize];
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
                            search_offset,
                            pos,
                            type_len,
                            e,
                            type_hex
                        );
                        parse_success = false;
                        break;
                    }
                };
                pos += type_len as usize;

                // Convert Cassandra marshal type to CQL type
                let cql_type = convert_marshal_type_to_cql(&internal_type);

                parsed_columns.push(ColumnInfo {
                    name: column_name,
                    column_type: cql_type,
                    is_primary_key: false, // Will be determined from partition/clustering info
                    key_position: None,
                    is_static: false,
                    is_clustering: false,
                });
            }

            if parse_success && parsed_columns.len() == column_count {
                // Successfully parsed all columns
                let column_names: Vec<&str> =
                    parsed_columns.iter().map(|c| c.name.as_str()).collect();
                log::debug!(
                    "Successfully parsed {} columns at offset {}: {:?}",
                    parsed_columns.len(),
                    search_offset,
                    column_names
                );
                if !partition_key_types.is_empty() {
                    log::debug!(
                        "Extracted {} partition key types via backtracking: {:?}",
                        partition_key_types.len(),
                        partition_key_types
                    );
                }

                // NEW: Create synthetic ColumnInfo entries for partition keys
                // SerializationHeader provides partition key TYPES but not NAMES
                // We must create synthetic columns since partition keys are not in regular columns list
                let mut all_columns = Vec::new();

                for (pk_idx, pk_marshal_type) in partition_key_types.iter().enumerate() {
                    let pk_cql_type = convert_marshal_type_to_cql(pk_marshal_type);

                    // Generate synthetic partition key column name
                    // Format: pk_<position> (e.g., "pk_0", "pk_1" for composite keys)
                    let pk_name = if partition_key_types.len() == 1 {
                        "id".to_string() // Single partition key - use conventional name
                    } else {
                        format!("pk_{}", pk_idx) // Composite partition key
                    };

                    let pk_column = ColumnInfo {
                        name: pk_name.clone(),
                        column_type: pk_cql_type.clone(),
                        is_primary_key: true,
                        key_position: Some(pk_idx as u16),
                        is_static: false,
                        is_clustering: false,
                    };

                    log::debug!(
                        "Created synthetic partition key column '{}' at position {} (type: {})",
                        pk_name,
                        pk_idx,
                        pk_cql_type
                    );

                    all_columns.push(pk_column);
                }

                // Add all regular columns AFTER partition keys
                all_columns.extend(parsed_columns);

                let remaining = &input[pos..];
                return Ok((remaining, (partition_key_types, all_columns)));
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

/// Convert Cassandra internal marshal type to CQL type name
fn convert_marshal_type_to_cql(marshal_type: &str) -> String {
    // Extract type name from fully-qualified class name
    // Example: "org.apache.cassandra.db.marshal.Int32Type" -> "int"
    let type_name = marshal_type
        .split('.')
        .next_back()
        .unwrap_or(marshal_type)
        .trim_end_matches("Type");

    // Map common types to CQL equivalents
    match type_name {
        "UTF8" => "text".to_string(),
        "Int32" => "int".to_string(),
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
        "Counter" => "counter".to_string(),
        _ => {
            // Unknown type - use lowercase version of type name
            type_name.to_lowercase()
        }
    }
}

/// Parse minimal EncodingStats section from nb-format Statistics.db
///
/// Returns: (min_timestamp, min_deletion_time, min_ttl, columns)
fn parse_minimal_encoding_stats(input: &[u8]) -> IResult<&[u8], EncodingStatsResult> {
    // Skip metadata_type (u32 BE) at start of data section
    let (input, _metadata_type) = be_u32(input)?;

    // Parse data section length (VInt)
    let (input, _data_length) = parse_vuint(input)?;

    // Parse partitioner string length (VInt)
    let (input, partitioner_len) = parse_vuint(input)?;

    // Skip partitioner string (we don't need it)
    let (input, _) = take(partitioner_len as usize)(input)?;

    // The exact structure after partitioner varies, but we know EncodingStats fields appear
    // after some metadata. Based on observed data, we need to skip past additional metadata
    // before reaching minTimestamp, minLocalDeletionTime, and minTTL.
    //
    // Strategy: Parse VInts and look for patterns that match expected timestamp ranges
    // Expected minTimestamp: ~1759713124861209 (microseconds, large positive number)
    // Expected minLocalDeletionTime: ~1442880000 (seconds, reasonable epoch value)
    // Expected minTTL: 0 or small positive number

    // Skip additional metadata (observed: ~2 VInts before timestamp fields)
    let (input, _metadata1) = parse_vuint(input)?;
    let (input, _metadata2) = parse_vuint(input)?;

    // Now parse the EncodingStats fields
    // minTimestamp (VInt, signed microseconds)
    let (input, min_timestamp) = parse_vint(input)?;

    // minLocalDeletionTime (VInt, signed seconds)
    let (input, min_deletion_time) = parse_vint(input)?;

    // minTTL (VInt, signed seconds) - may be 0
    let (input, min_ttl_value) = parse_vint(input)?;
    let min_ttl = if min_ttl_value == 0 {
        Some(0)
    } else {
        Some(min_ttl_value)
    };

    // Parse SerializationHeader (Issue #163)
    // This parses partition keys, clustering keys, and regular columns
    let (input, (partition_types, clustering_types, columns)) = parse_serialization_header(input)?;

    log::debug!(
        "Parsed SerializationHeader: {} partition keys, {} clustering keys, {} regular columns",
        partition_types.len(),
        clustering_types.len(),
        columns.len()
    );

    // Mark clustering columns and assign positions (Issue #163)
    // Clustering keys need to be matched with regular columns and marked appropriately
    if !clustering_types.is_empty() {
        // For now, we don't have column names for clustering keys from the type descriptors
        // The clustering key information is just the types
        // We need to match these with columns based on schema knowledge or other means
        // For this initial implementation, we'll store clustering types as metadata
        // and defer column matching to the schema layer

        // Note: Proper implementation requires extracting column names for clustering keys
        // from the schema or matching with regular columns. This is a known limitation.
        log::debug!(
            "Clustering key types found: {:?}, but column name matching not yet implemented",
            clustering_types
        );
    }

    Ok((input, (min_timestamp, min_deletion_time, min_ttl, columns)))
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
/// # Returns
///
/// SSTableStatistics with only header and timestamp_stats populated from real data.
pub fn parse_enhanced_statistics_file(input: &[u8]) -> IResult<&[u8], SSTableStatistics> {
    // Parse the 32-byte header
    let (remaining, header) = parse_nb_format_header(input)?;

    // Parse minimal statistics data (EncodingStats + SerializationHeader columns)
    let result = parse_nb_format_statistics_data(remaining, &header);

    match result {
        Ok((
            row_stats,
            timestamp_stats,
            table_stats,
            partition_stats,
            compression_stats,
            columns,
        )) => {
            log::debug!(
                "Successfully parsed Statistics.db: {} columns from SerializationHeader",
                columns.len()
            );

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
/// # Returns
///
/// SSTableStatistics with minimal fields populated, or error if parsing fails.
pub fn parse_statistics_with_fallback(input: &[u8]) -> IResult<&[u8], SSTableStatistics> {
    // Try the minimal enhanced parser
    parse_enhanced_statistics_file(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_header_with_no_clustering_keys() {
        // Test SerializationHeader with partition key and regular columns, no clustering keys
        // Format: [VInt partition_type_len] [0x00 0x00] [partition_type] [clustering_count=0] [0x00 0x00 column_count] [columns...]

        let mut test_data = vec![];

        // Partition key type: 41 bytes "(org.apache.cassandra.db.marshal.UUIDType"
        test_data.push(0x29); // VInt: 41
        test_data.extend_from_slice(&[0x00, 0x00]); // Marker
        test_data.extend_from_slice(b"(org.apache.cassandra.db.marshal.UUIDType");

        // Clustering key count = 0
        test_data.push(0x00);

        // Regular columns section: marker (0x00 0x00) + count
        test_data.extend_from_slice(&[0x00, 0x00, 0x02]); // section marker (2 bytes) + count 2

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
        test_data.push(0x29); // VInt: 41
        test_data.extend_from_slice(&[0x00, 0x00]); // Marker
        test_data.extend_from_slice(b"(org.apache.cassandra.db.marshal.UUIDType");

        // Clustering key count = 2
        test_data.push(0x02);

        // Clustering key 1: ReversedType(TimestampType)
        test_data.extend_from_slice(b"[org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)");

        // Clustering key 2: UTF8Type
        test_data.extend_from_slice(b"(org.apache.cassandra.db.marshal.UTF8Type)");

        // Regular columns section
        test_data.extend_from_slice(&[0x00, 0x00, 0x02]); // section marker (2 bytes) + count 2

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
    fn test_marshal_type_conversion() {
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
        let result = parse_nb_format_statistics_data(&dummy_data, &header);

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

        let result = parse_enhanced_statistics_file(&test_data);

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

        let result = parse_statistics_with_fallback(&test_data);

        // Should fail - incomplete data
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_data_returns_error() {
        // Test with insufficient data
        let invalid_data = vec![0xFF; 10];
        let result = parse_statistics_with_fallback(&invalid_data);
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
        test_data.extend_from_slice(&[0x00, 0x00, 0x02]); // 2 regular columns

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

        // Verify columns: 1 synthetic partition key + 2 regular columns = 3 total
        assert_eq!(columns.len(), 3, "Expected 1 synthetic PK + 2 regular columns");

        // Column 0: synthetic partition key "id"
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].column_type, "uuid");
        assert_eq!(columns[0].is_primary_key, true);
        assert_eq!(columns[0].key_position, Some(0));

        // Column 1: regular column "expiring_value"
        assert_eq!(columns[1].name, "expiring_value");
        assert_eq!(columns[1].column_type, "int");
        assert_eq!(columns[1].is_primary_key, false);

        // Column 2: regular column "session_info"
        assert_eq!(columns[2].name, "session_info");
        assert_eq!(columns[2].column_type, "text");
        assert_eq!(columns[2].is_primary_key, false);
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
        test_data.extend_from_slice(&[0x00, 0x00, 0x01]);

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

        // Expect 2 columns: 1 synthetic partition key + 1 regular column
        assert_eq!(columns.len(), 2);

        // Column 0: synthetic partition key
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].is_primary_key, true);

        // Column 1: regular column "data"
        assert_eq!(columns[1].name, "data");
        assert_eq!(columns[1].is_primary_key, false);
    }

    #[test]
    fn test_backtracking_with_no_partition_key() {
        // Test case where there's no partition key before the marker
        // This should still parse columns successfully but return empty partition key list

        let mut test_data = vec![];

        // Just the marker and columns, no partition key type before
        test_data.extend_from_slice(&[0x00, 0x00, 0x01]); // Marker + count

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
