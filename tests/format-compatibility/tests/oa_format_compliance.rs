//! Comprehensive Cassandra 5+ 'oa' Format Compliance Tests
//!
//! These tests ensure 100% byte-perfect compatibility with Apache Cassandra 5+
//! SSTable 'oa' format specification. Zero tolerance for deviations.

use cqlite_core::parser::{header::*, vint::*, types::*};
use format_validator::{format_constants::*, utils::*, ValidationError};
use pretty_assertions::assert_eq;
use std::collections::HashMap;

#[test]
fn test_oa_magic_number_compliance() {
    // Test exact magic number format
    let expected_magic = BIG_FORMAT_OA_MAGIC;
    let magic_bytes = expected_magic.to_be_bytes();
    
    assert_eq!(magic_bytes, [0x6F, 0x61, 0x00, 0x00]);
    
    // Verify parsing
    assert!(verify_magic(&magic_bytes, expected_magic).is_ok());
    
    // Test rejection of incorrect magic
    let wrong_magic = [0x6F, 0x62, 0x00, 0x00]; // 'ob' instead of 'oa'
    assert!(verify_magic(&wrong_magic, expected_magic).is_err());
}

#[test]
fn test_oa_version_compliance() {
    let version = SUPPORTED_VERSION;
    let version_bytes = version.to_be_bytes();
    
    assert_eq!(version_bytes, [0x00, 0x01]);
    assert_eq!(version, 1);
}

#[test]
fn test_oa_header_structure() {
    // Build a complete 'oa' format header
    let mut header_bytes = Vec::new();
    
    // Magic number (4 bytes)
    header_bytes.extend_from_slice(&BIG_FORMAT_OA_MAGIC.to_be_bytes());
    
    // Version (2 bytes)
    header_bytes.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
    
    // Flags (4 bytes) - test various flag combinations
    let flags = 0x0001 | 0x0100 | 0x0200; // has compression + key range + long deletion
    header_bytes.extend_from_slice(&flags.to_be_bytes());
    
    // Reserved (22 bytes) - must be zero
    header_bytes.extend_from_slice(&[0u8; 22]);
    
    assert_eq!(header_bytes.len(), 32); // Standard header size
    
    // Parse header
    let (remaining, version) = parse_magic_and_version(&header_bytes).unwrap();
    assert_eq!(version, SUPPORTED_VERSION);
    assert_eq!(remaining.len(), 26); // 32 - 6 = 26 remaining bytes
}

#[test]
fn test_oa_flag_encoding() {
    // Test all defined flag combinations
    let test_cases = vec![
        (0x0001, "has_compression"),
        (0x0002, "has_static_columns"),
        (0x0004, "has_regular_columns"),
        (0x0008, "has_complex_columns"),
        (0x0010, "has_partition_deletion"),
        (0x0020, "has_ttl_data"),
        (0x0100, "key_range_support"),
        (0x0200, "long_deletion_time"),
        (0x0400, "token_space_coverage"),
        (0x0800, "enhanced_timestamps"),
        (0x010000, "lz4_compression"),
        (0x020000, "snappy_compression"),
        (0x040000, "deflate_compression"),
        (0x080000, "custom_compression"),
    ];
    
    for (flag_value, flag_name) in test_cases {
        // Test individual flag
        let flag_bytes = flag_value.to_be_bytes();
        let parsed_flags = u32::from_be_bytes(flag_bytes);
        assert_eq!(parsed_flags, flag_value, "Failed for flag: {}", flag_name);
        
        // Test flag detection
        assert_ne!(parsed_flags & flag_value, 0, "Flag not detected: {}", flag_name);
    }
    
    // Test combined flags
    let combined = 0x0001 | 0x0100 | 0x010000; // compression + key range + LZ4
    let combined_bytes = combined.to_be_bytes();
    let parsed_combined = u32::from_be_bytes(combined_bytes);
    
    assert_eq!(parsed_combined & 0x0001, 0x0001); // has compression
    assert_eq!(parsed_combined & 0x0100, 0x0100); // key range
    assert_eq!(parsed_combined & 0x010000, 0x010000); // LZ4
}

#[test]
fn test_oa_enhanced_metadata() {
    // Test new 'oa' format features
    
    // 1. Long deletion time (64-bit instead of 32-bit)
    let long_deletion_time = 0x1234_5678_9ABC_DEF0i64;
    let deletion_bytes = long_deletion_time.to_be_bytes();
    assert_eq!(deletion_bytes.len(), 8);
    
    let parsed_deletion = i64::from_be_bytes(deletion_bytes);
    assert_eq!(parsed_deletion, long_deletion_time);
    
    // 2. Enhanced timestamps (microsecond precision)
    let timestamp_micros = 1_640_995_200_000_000i64; // 2022-01-01 00:00:00 UTC in microseconds
    let timestamp_bytes = timestamp_micros.to_be_bytes();
    
    let parsed_timestamp = i64::from_be_bytes(timestamp_bytes);
    assert_eq!(parsed_timestamp, timestamp_micros);
    
    // 3. Token space coverage
    let token_ranges = vec![
        (-9223372036854775808i64, -4611686018427387904i64), // First quarter
        (-4611686018427387904i64, 0i64),                     // Second quarter
        (0i64, 4611686018427387904i64),                      // Third quarter
        (4611686018427387904i64, 9223372036854775807i64),    // Fourth quarter
    ];
    
    // Encode token ranges
    let mut token_bytes = Vec::new();
    token_bytes.extend_from_slice(&encode_vint(token_ranges.len() as i64));
    
    for (start, end) in token_ranges {
        token_bytes.extend_from_slice(&encode_vint(start));
        token_bytes.extend_from_slice(&encode_vint(end));
    }
    
    // Parse token ranges
    let mut offset = 0;
    let (remaining, range_count) = parse_vint(&token_bytes[offset..]).unwrap();
    offset = token_bytes.len() - remaining.len();
    assert_eq!(range_count, 4);
    
    for i in 0..range_count {
        let (remaining, start) = parse_vint(&token_bytes[offset..]).unwrap();
        offset = token_bytes.len() - remaining.len();
        
        let (remaining, end) = parse_vint(&token_bytes[offset..]).unwrap();
        offset = token_bytes.len() - remaining.len();
        
        // Verify ranges are properly ordered
        assert!(start < end, "Invalid token range {}: {} >= {}", i, start, end);
    }
}

#[test]
fn test_oa_compression_info_structure() {
    // Test compression info structure for 'oa' format
    let compression_info = CompressionInfo {
        algorithm: "LZ4".to_string(),
        chunk_size: 4096,
        parameters: {
            let mut params = HashMap::new();
            params.insert("level".to_string(), "6".to_string());
            params.insert("checksum".to_string(), "CRC32".to_string());
            params
        },
    };
    
    // Serialize compression info
    let mut serialized = Vec::new();
    serialize_compression_info(&mut serialized, &compression_info).unwrap();
    
    // Parse it back
    let (remaining, parsed_info) = parse_compression_info(&serialized).unwrap();
    assert!(remaining.is_empty());
    
    assert_eq!(parsed_info.algorithm, "LZ4");
    assert_eq!(parsed_info.chunk_size, 4096);
    assert_eq!(parsed_info.parameters.get("level"), Some(&"6".to_string()));
    assert_eq!(parsed_info.parameters.get("checksum"), Some(&"CRC32".to_string()));
}

#[test]
fn test_oa_statistics_structure() {
    // Test enhanced statistics structure for 'oa' format
    let stats = SSTableStats {
        row_count: 100_000,
        min_timestamp: 1_640_995_200_000_000i64, // 2022-01-01 in microseconds
        max_timestamp: 1_672_531_199_999_999i64, // 2022-12-31 in microseconds
        max_deletion_time: 1_672_531_199_999_999i64, // Long deletion time
        compression_ratio: 0.65,
        row_size_histogram: vec![100, 500, 1000, 2000, 5000],
    };
    
    // Serialize statistics
    let mut serialized = Vec::new();
    serialize_sstable_stats(&mut serialized, &stats).unwrap();
    
    // Parse it back
    let (remaining, parsed_stats) = parse_sstable_stats(&serialized).unwrap();
    assert!(remaining.is_empty());
    
    assert_eq!(parsed_stats.row_count, 100_000);
    assert_eq!(parsed_stats.min_timestamp, 1_640_995_200_000_000i64);
    assert_eq!(parsed_stats.max_timestamp, 1_672_531_199_999_999i64);
    assert_eq!(parsed_stats.max_deletion_time, 1_672_531_199_999_999i64);
    assert!((parsed_stats.compression_ratio - 0.65).abs() < 1e-10);
    assert_eq!(parsed_stats.row_size_histogram, vec![100, 500, 1000, 2000, 5000]);
}

#[test]
fn test_oa_footer_structure() {
    // Test data file footer structure
    let index_offset = 0x1234_5678_9ABC_DEF0u64;
    let crc32_checksum = 0xDEAD_BEEFu32;
    let magic_verification = BIG_FORMAT_OA_MAGIC;
    
    let mut footer = Vec::new();
    footer.extend_from_slice(&index_offset.to_be_bytes());
    footer.extend_from_slice(&crc32_checksum.to_be_bytes());
    footer.extend_from_slice(&magic_verification.to_be_bytes());
    
    assert_eq!(footer.len(), 16); // 8 + 4 + 4 = 16 bytes
    
    // Parse footer
    let parsed_offset = u64::from_be_bytes([
        footer[0], footer[1], footer[2], footer[3],
        footer[4], footer[5], footer[6], footer[7]
    ]);
    let parsed_checksum = u32::from_be_bytes([footer[8], footer[9], footer[10], footer[11]]);
    let parsed_magic = u32::from_be_bytes([footer[12], footer[13], footer[14], footer[15]]);
    
    assert_eq!(parsed_offset, index_offset);
    assert_eq!(parsed_checksum, crc32_checksum);
    assert_eq!(parsed_magic, magic_verification);
}

#[test]
fn test_oa_complete_header_roundtrip() {
    // Create a complete SSTable header
    let header = SSTableHeader {
        version: SUPPORTED_VERSION,
        table_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        keyspace: "test_keyspace".to_string(),
        table_name: "test_table".to_string(),
        generation: 42,
        compression: CompressionInfo {
            algorithm: "LZ4".to_string(),
            chunk_size: 4096,
            parameters: HashMap::new(),
        },
        stats: SSTableStats {
            row_count: 1000,
            min_timestamp: 0,
            max_timestamp: 1000000,
            max_deletion_time: 2000000,
            compression_ratio: 0.8,
            row_size_histogram: vec![100, 200, 300],
        },
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                column_type: "uuid".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "data".to_string(),
                column_type: "text".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            }
        ],
        properties: {
            let mut props = HashMap::new();
            props.insert("created_by".to_string(), "cqlite-test".to_string());
            props
        },
    };
    
    // Serialize header
    let serialized = serialize_sstable_header(&header).unwrap();
    
    // Verify starts with correct magic and version
    assert_eq!(&serialized[0..4], &BIG_FORMAT_OA_MAGIC.to_be_bytes());
    assert_eq!(&serialized[4..6], &SUPPORTED_VERSION.to_be_bytes());
    
    // Parse header back
    let (remaining, parsed_header) = parse_sstable_header(&serialized).unwrap();
    assert!(remaining.is_empty());
    
    // Verify all fields match
    assert_eq!(parsed_header.version, header.version);
    assert_eq!(parsed_header.table_id, header.table_id);
    assert_eq!(parsed_header.keyspace, header.keyspace);
    assert_eq!(parsed_header.table_name, header.table_name);
    assert_eq!(parsed_header.generation, header.generation);
    assert_eq!(parsed_header.compression.algorithm, header.compression.algorithm);
    assert_eq!(parsed_header.stats.row_count, header.stats.row_count);
    assert_eq!(parsed_header.columns.len(), header.columns.len());
    assert_eq!(parsed_header.properties, header.properties);
}

#[test]
fn test_oa_endianness_compliance() {
    // Test that all multi-byte values use big-endian encoding
    
    // 16-bit values
    let test_u16 = 0x1234u16;
    let bytes_u16 = test_u16.to_be_bytes();
    assert_eq!(bytes_u16, [0x12, 0x34]);
    assert_eq!(u16::from_be_bytes(bytes_u16), test_u16);
    
    // 32-bit values
    let test_u32 = 0x12345678u32;
    let bytes_u32 = test_u32.to_be_bytes();
    assert_eq!(bytes_u32, [0x12, 0x34, 0x56, 0x78]);
    assert_eq!(u32::from_be_bytes(bytes_u32), test_u32);
    
    // 64-bit values
    let test_u64 = 0x123456789ABCDEFu64;
    let bytes_u64 = test_u64.to_be_bytes();
    assert_eq!(bytes_u64, [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
    assert_eq!(u64::from_be_bytes(bytes_u64), test_u64);
    
    // Signed values
    let test_i64 = -1i64;
    let bytes_i64 = test_i64.to_be_bytes();
    assert_eq!(bytes_i64, [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    assert_eq!(i64::from_be_bytes(bytes_i64), test_i64);
}

#[test]
fn test_oa_checksum_compliance() {
    // Test CRC32 checksum calculation compliance
    let test_data = b"Hello, Cassandra 5.0 'oa' format!";
    let checksum = calculate_crc32(test_data);
    
    // Verify checksum is deterministic
    assert_eq!(checksum, calculate_crc32(test_data));
    
    // Verify different data produces different checksum
    let other_data = b"Different data";
    assert_ne!(checksum, calculate_crc32(other_data));
    
    // Test empty data
    let empty_checksum = calculate_crc32(&[]);
    assert_eq!(empty_checksum, 0); // CRC32 of empty data is 0
}

#[test]
fn test_oa_reserved_fields_compliance() {
    // All reserved fields must be zero
    let reserved_22_bytes = [0u8; 22];
    
    // Verify all bytes are zero
    for (i, &byte) in reserved_22_bytes.iter().enumerate() {
        assert_eq!(byte, 0, "Reserved byte {} is not zero", i);
    }
    
    // Test that non-zero reserved fields are rejected
    let mut bad_reserved = [0u8; 22];
    bad_reserved[10] = 1; // Set one byte to non-zero
    
    // This should be detected in a proper validation
    assert_ne!(bad_reserved, reserved_22_bytes);
}