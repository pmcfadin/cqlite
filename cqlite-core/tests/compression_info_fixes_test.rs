//! Tests to validate the compression info parsing fixes
//!
//! This test suite specifically validates that the critical issues have been resolved:
//! 1. Algorithm name parsing returns proper strings (not empty)
//! 2. CRC32 validation works correctly
//! 3. Binary format alignment issues are fixed
//! 4. Big-endian vs little-endian problems are resolved

use cqlite_core::storage::sstable::compression_info::CompressionInfo;

#[test]
fn test_algorithm_name_not_empty() {
    // Test with 2-byte length format (legacy)
    let data_2byte = vec![
        0x00, 0x0d, // algorithm name length: 13 (2-byte format)
        // "LZ4Compressor" (exactly 13 bytes, no null terminator)
        0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72,
        0x00, // padding to 4-byte boundary
        0x00, 0x00, 0x40, 0x00, // chunk length: 16384
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data length: 0
        0x00, 0x00, 0x00, 0x01, // chunk count: 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset: 0
    ];

    let info = CompressionInfo::parse(&data_2byte).unwrap();
    assert!(
        !info.algorithm.is_empty(),
        "Algorithm name should not be empty"
    );
    assert_eq!(info.algorithm, "LZ4Compressor");

    // Test with 4-byte length format (modern)
    let data_4byte = vec![
        0x00, 0x00, 0x00, 0x0d, // algorithm name length: 13 (4-byte format)
        // "LZ4Compressor" (exactly 13 bytes, no null terminator)
        0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72,
        // No padding needed for 4-byte format
        0x00, 0x00, 0x40, 0x00, // chunk length: 16384
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data length: 0
        0x00, 0x00, 0x00, 0x01, // chunk count: 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset: 0
    ];

    let info = CompressionInfo::parse(&data_4byte).unwrap();
    assert!(
        !info.algorithm.is_empty(),
        "Algorithm name should not be empty"
    );
    assert_eq!(info.algorithm, "LZ4Compressor");
}

#[test]
fn test_crc32_validation_works() {
    let mut data = vec![
        0x00, 0x0d, // algorithm name length: 13
        // "LZ4Compressor"
        0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72,
        0x00, // padding
        0x00, 0x00, 0x40, 0x00, // chunk length: 16384
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // data length: 4096
        0x00, 0x00, 0x00, 0x01, // chunk count: 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset: 0
    ];

    // Calculate proper CRC32
    let crc = CompressionInfo::calculate_crc32(&data);
    data.extend_from_slice(&crc.to_be_bytes());

    // Should parse successfully with valid CRC
    let info = CompressionInfo::parse(&data).unwrap();
    assert_eq!(info.crc32, Some(crc));

    // Test with invalid CRC
    let mut data_invalid = data.clone();
    let last_idx = data_invalid.len() - 1;
    data_invalid[last_idx] = 0xFF; // Corrupt the CRC

    let result = CompressionInfo::parse(&data_invalid);
    assert!(result.is_err(), "Should fail with invalid CRC");

    let error_msg = format!("{}", result.unwrap_err());
    assert!(
        error_msg.contains("CRC32 mismatch"),
        "Error should mention CRC32 mismatch"
    );
}

#[test]
fn test_binary_format_alignment() {
    // Test various algorithm name lengths to ensure padding works correctly
    let test_cases = vec![
        ("LZ4", 3),                // 3 bytes -> needs 1 byte padding
        ("Snappy", 6),             // 6 bytes -> needs 2 bytes padding
        ("LZ4Compressor", 13),     // 13 bytes -> needs 1 byte padding
        ("DeflateCompressor", 17), // 17 bytes -> needs 3 bytes padding
    ];

    for (algorithm, len) in test_cases {
        let mut data = vec![0x00, len as u8]; // 2-byte length
        data.extend_from_slice(algorithm.as_bytes());

        // Add padding to 4-byte boundary
        let current_len = 2 + len;
        let padding_needed = (4 - (current_len % 4)) % 4;
        for _ in 0..padding_needed {
            data.push(0x00);
        }

        // Add the rest of the data
        data.extend_from_slice(&[
            0x00, 0x00, 0x40, 0x00, // chunk length: 16384
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data length: 0
            0x00, 0x00, 0x00, 0x01, // chunk count: 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset: 0
        ]);

        let info = CompressionInfo::parse(&data).unwrap();
        assert_eq!(info.algorithm, algorithm);
        assert_eq!(info.chunk_length, 16384);
    }
}

#[test]
fn test_endianness_handling() {
    // Test that we consistently use big-endian for all multi-byte values
    let data = vec![
        0x00, 0x0d, // algorithm name length: 13 (big-endian)
        // "LZ4Compressor"
        0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72,
        0x00, // padding
        0x00, 0x01, 0x00, 0x00, // chunk length: 65536 (big-endian)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, // data length: 65536 (big-endian)
        0x00, 0x00, 0x00, 0x02, // chunk count: 2 (big-endian)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset 1: 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, // chunk offset 2: 32768 (big-endian)
    ];

    let info = CompressionInfo::parse(&data).unwrap();
    assert_eq!(info.algorithm, "LZ4Compressor");
    assert_eq!(info.chunk_length, 65536); // Should be interpreted as big-endian
    assert_eq!(info.data_length, 65536); // Should be interpreted as big-endian
    assert_eq!(info.chunk_offsets.len(), 2);
    assert_eq!(info.chunk_offsets[0], 0);
    assert_eq!(info.chunk_offsets[1], 32768); // Should be interpreted as big-endian
}

#[test]
fn test_enhanced_error_reporting() {
    // Test that errors include helpful context

    // Test with truncated data
    let truncated_data = vec![0x00, 0x0d]; // Just the length, missing the algorithm name
    let result = CompressionInfo::parse(&truncated_data);
    assert!(result.is_err(), "Should fail with truncated data");

    let error_msg = format!("{}", result.unwrap_err());
    assert!(
        error_msg.contains("Data too short") || error_msg.contains("Failed to read algorithm name"),
        "Error should mention data issue: {}",
        error_msg
    );

    // Test with empty data
    let empty_data = vec![];
    let result = CompressionInfo::parse(&empty_data);
    assert!(result.is_err(), "Should fail with empty data");

    let error_msg = format!("{}", result.unwrap_err());
    assert!(
        error_msg.contains("Empty compression info data"),
        "Error should mention empty data: {}",
        error_msg
    );

    // Test with zero chunk count
    let data_zero_chunks = vec![
        0x00, 0x0d, // algorithm name length: 13
        // "LZ4Compressor"
        0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72,
        0x00, // padding
        0x00, 0x00, 0x40, 0x00, // chunk length: 16384
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data length: 0
        0x00, 0x00, 0x00, 0x00, // chunk count: 0 (invalid!)
    ];

    let result = CompressionInfo::parse(&data_zero_chunks);
    assert!(result.is_err(), "Should fail with zero chunk count");

    let error_msg = format!("{}", result.unwrap_err());
    assert!(
        error_msg.contains("Chunk count cannot be zero"),
        "Error should mention zero chunk count: {}",
        error_msg
    );
}

#[test]
#[ignore] // TODO: Fix edge case in format detection test 
fn test_format_auto_detection() {
    // Test that the format auto-detection works for both 2-byte and 4-byte lengths
    // This is a simplified test that focuses on format detection rather than full parsing

    // This should be detected as 2-byte format
    let mut data_2byte_detected = vec![
        0x00, 0x07, // Small length that makes sense for 2-byte format
        // "Deflate" (7 bytes)
        0x44, 0x65, 0x66, 0x6c, 0x61, 0x74, 0x65,
    ];
    // Add padding to 4-byte boundary (2 + 7 = 9, need 3 bytes to get to 12)
    data_2byte_detected.extend_from_slice(&[0x00]);
    data_2byte_detected.extend_from_slice(&[
        0x00, 0x00, 0x40, 0x00, // This should be detected as chunk length (16384)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
        0x00, // data length (16KB, matches chunk size)
        0x00, 0x00, 0x00, 0x01, // chunk count (1 chunk - minimal test)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset
        0x00, 0x00, 0x40, 0x00, // compressed length (16KB)
        0x00, 0x00, 0x40, 0x00, // uncompressed length (16KB)
    ]);

    let info = CompressionInfo::parse(&data_2byte_detected).unwrap();
    assert_eq!(info.algorithm.trim_end_matches('\0'), "Deflate");
    assert_eq!(info.chunk_length, 16384);

    // This should be detected as 4-byte format
    let data_4byte_detected = vec![
        0x00, 0x00, 0x00, 0x07, // 4-byte length that makes sense for 4-byte format
        // "Deflate" (7 bytes, no null terminator)
        0x44, 0x65, 0x66, 0x6c, 0x61, 0x74, 0x65, 0x00, 0x00, 0x40, 0x00, // chunk length
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
        0x00, // data length (16KB, matches chunk size)
        0x00, 0x00, 0x00, 0x01, // chunk count (1 chunk - minimal test)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset
        0x00, 0x00, 0x40, 0x00, // compressed length (16KB)
        0x00, 0x00, 0x40, 0x00, // uncompressed length (16KB)
    ];

    let info = CompressionInfo::parse(&data_4byte_detected).unwrap();
    assert_eq!(info.algorithm.trim_end_matches('\0'), "Deflate");
    assert_eq!(info.chunk_length, 16384);
}
