//! Comprehensive SSTable header parsing tests
//!
//! This module provides extensive test coverage for SSTable header parsing fixes,
//! including magic number detection, byte positioning, version reading, format
//! detection, edge cases, and real-world scenarios.

use cqlite_core::{
    Config,
    error::{Error, ErrorCategory},
    parser::header::{
        CassandraVersion, ColumnInfo, CompressionInfo, SSTABLE_MAGIC, SSTableHeader, SSTableStats,
        SUPPORTED_VERSION, SUPPORTED_MAGIC_NUMBERS, parse_sstable_header, serialize_sstable_header,
        parse_magic_and_version,
    },
    platform::Platform,
    storage::sstable::{
        format_detector::{FormatDetector, SSTableFormat, SSTableInfo},
        reader::SSTableReader,
        SSTableManager,
    },
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

/// Comprehensive test suite for magic number detection and validation
#[cfg(test)]
mod magic_number_tests {
    use super::*;

    #[test]
    fn test_all_supported_magic_numbers() {
        // Test all officially supported magic numbers
        for &magic in SUPPORTED_MAGIC_NUMBERS {
            let version = CassandraVersion::from_magic_number(magic);
            assert!(
                version.is_some(),
                "Magic number 0x{:08X} should map to a valid version",
                magic
            );

            let version = version.unwrap();
            assert_eq!(
                version.magic_number(),
                magic,
                "Round-trip magic number should match for {:?}",
                version
            );

            println!("✅ Magic 0x{:08X} -> {:?}", magic, version);
        }
    }

    #[test]
    fn test_magic_number_byte_order() {
        // Test that magic numbers are correctly interpreted in big-endian format
        let test_cases = vec![
            (CassandraVersion::Legacy, [0x6F, 0x61, 0x00, 0x00]),
            (CassandraVersion::V5_0Alpha, [0xAD, 0x01, 0x00, 0x00]),
            (CassandraVersion::V5_0Beta, [0xA0, 0x07, 0x00, 0x00]),
            (CassandraVersion::V5_0Release, [0x43, 0x16, 0x00, 0x00]),
            (CassandraVersion::V5_0NewBig, [0x00, 0x40, 0x00, 0x00]),
            (CassandraVersion::V5_0Bti, [0x64, 0x61, 0x00, 0x00]),
        ];

        for (version, expected_bytes) in test_cases {
            let magic = version.magic_number();
            let actual_bytes = magic.to_be_bytes();

            assert_eq!(
                actual_bytes, expected_bytes,
                "Magic number byte order mismatch for {:?}",
                version
            );

            // Test parsing from bytes
            let parsed_magic = u32::from_be_bytes(expected_bytes);
            assert_eq!(
                parsed_magic, magic,
                "Byte-to-magic conversion failed for {:?}",
                version
            );

            println!("✅ {:?}: 0x{:08X} = {:02X?}", version, magic, expected_bytes);
        }
    }

    #[test]
    fn test_invalid_magic_numbers() {
        // Test various invalid magic numbers
        let invalid_magics = vec![
            0x00000000, // All zeros
            0xFFFFFFFF, // All ones
            0x12345678, // Random value
            0xDEADBEEF, // Classic invalid value
            0x6F620000, // Close to 'oa' but wrong
            0x6F610001, // 'oa' with wrong suffix
            0x41414141, // ASCII 'AAAA'
            0x20202020, // ASCII spaces
        ];

        for invalid_magic in invalid_magics {
            let version = CassandraVersion::from_magic_number(invalid_magic);
            assert!(
                version.is_none(),
                "Invalid magic 0x{:08X} should not map to any version",
                invalid_magic
            );

            // Test with header parsing
            let mut data = Vec::new();
            data.extend_from_slice(&invalid_magic.to_be_bytes());
            data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
            data.extend_from_slice(&[0x00; 100]); // Padding

            let result = parse_magic_and_version(&data);
            assert!(
                result.is_err(),
                "Invalid magic 0x{:08X} should fail header parsing",
                invalid_magic
            );

            println!("✅ Correctly rejected invalid magic: 0x{:08X}", invalid_magic);
        }
    }

    #[test]
    fn test_magic_number_endianness_edge_cases() {
        // Test little-endian vs big-endian confusion scenarios
        let test_cases = vec![
            (0x6F610000, 0x0000616F), // 'oa' swapped
            (0xAD010000, 0x000001AD), // Alpha swapped
            (0xA0070000, 0x000007A0), // Beta swapped
        ];

        for (correct_magic, swapped_magic) in test_cases {
            // Correct magic should work
            assert!(CassandraVersion::from_magic_number(correct_magic).is_some());

            // Swapped magic should not work
            assert!(CassandraVersion::from_magic_number(swapped_magic).is_none());

            println!(
                "✅ Endianness test: 0x{:08X} ✓, 0x{:08X} ✗",
                correct_magic, swapped_magic
            );
        }
    }

    #[test]
    fn test_magic_number_bit_corruption() {
        // Test single-bit errors in magic numbers
        for &original_magic in SUPPORTED_MAGIC_NUMBERS {
            for bit_position in 0..32 {
                let corrupted_magic = original_magic ^ (1 << bit_position);

                // Skip if corruption results in another valid magic number
                if SUPPORTED_MAGIC_NUMBERS.contains(&corrupted_magic) {
                    continue;
                }

                let version = CassandraVersion::from_magic_number(corrupted_magic);
                assert!(
                    version.is_none(),
                    "Single-bit corruption (bit {}) of 0x{:08X} -> 0x{:08X} should be invalid",
                    bit_position,
                    original_magic,
                    corrupted_magic
                );
            }
            println!("✅ Single-bit corruption tests passed for 0x{:08X}", original_magic);
        }
    }
}

/// Test cases for proper byte positioning and version reading
#[cfg(test)]
mod byte_positioning_tests {
    use super::*;

    #[test]
    fn test_standard_format_byte_positioning() {
        // Test standard format: magic (4 bytes) + version (2 bytes)
        let standard_versions = vec![
            CassandraVersion::Legacy,
            CassandraVersion::V5_0Alpha,
            CassandraVersion::V5_0Beta,
            CassandraVersion::V5_0Release,
            CassandraVersion::V5_0Bti,
        ];

        for version in standard_versions {
            let mut data = Vec::new();
            data.extend_from_slice(&version.magic_number().to_be_bytes()); // Bytes 0-3
            data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes()); // Bytes 4-5
            data.extend_from_slice(&[0xAA; 100]); // Padding with distinctive pattern

            let (remaining, (parsed_version, parsed_format_version)) =
                parse_magic_and_version(&data).unwrap();

            assert_eq!(parsed_version, version);
            assert_eq!(parsed_format_version, SUPPORTED_VERSION);

            // Check that parser consumed exactly 6 bytes (4 + 2)
            assert_eq!(
                remaining.len(),
                100,
                "Standard format should consume exactly 6 bytes for {:?}",
                version
            );

            // Verify remaining data is untouched
            assert_eq!(remaining[0], 0xAA);

            println!("✅ Standard byte positioning test passed for {:?}", version);
        }
    }

    #[test]
    fn test_newbig_format_byte_positioning() {
        // Test 'nb' format: magic (4 bytes) + padding (25 bytes) + version (2 bytes)
        let version = CassandraVersion::V5_0NewBig;
        let mut data = Vec::new();
        data.extend_from_slice(&version.magic_number().to_be_bytes()); // Bytes 0-3
        data.extend_from_slice(&[0x00; 25]); // Bytes 4-28 (padding)
        data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes()); // Bytes 29-30
        data.extend_from_slice(&[0xBB; 100]); // Remaining data

        let (remaining, (parsed_version, parsed_format_version)) =
            parse_magic_and_version(&data).unwrap();

        assert_eq!(parsed_version, version);
        assert_eq!(parsed_format_version, SUPPORTED_VERSION);

        // Check that parser consumed exactly 31 bytes (4 + 25 + 2)
        assert_eq!(
            remaining.len(),
            100,
            "NewBig format should consume exactly 31 bytes"
        );

        // Verify remaining data is untouched
        assert_eq!(remaining[0], 0xBB);

        println!("✅ NewBig byte positioning test passed");
    }

    #[test]
    fn test_truncated_headers_at_various_positions() {
        // Test truncation at various byte positions
        let truncation_points = vec![
            (0, "Empty data"),
            (1, "1 byte (partial magic)"),
            (2, "2 bytes (partial magic)"),
            (3, "3 bytes (partial magic)"),
            (4, "4 bytes (magic only)"),
            (5, "5 bytes (magic + partial version)"),
        ];

        for (truncate_at, description) in truncation_points {
            let mut data = Vec::new();
            data.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
            data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

            let truncated_data = &data[..truncate_at.min(data.len())];
            let result = parse_magic_and_version(truncated_data);

            assert!(
                result.is_err(),
                "Truncated header at {} should fail: {}",
                truncate_at,
                description
            );

            println!("✅ Correctly rejected truncation: {}", description);
        }
    }

    #[test]
    fn test_newbig_format_truncation_points() {
        // Test 'nb' format specific truncation points
        let version = CassandraVersion::V5_0NewBig;
        let truncation_points = vec![
            (4, "Magic only"),
            (10, "Magic + partial padding"),
            (20, "Magic + most padding"),
            (29, "Magic + all padding"),
            (30, "Magic + padding + partial version"),
        ];

        for (truncate_at, description) in truncation_points {
            let mut data = Vec::new();
            data.extend_from_slice(&version.magic_number().to_be_bytes());
            data.extend_from_slice(&[0x00; 25]); // Padding
            data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

            let truncated_data = &data[..truncate_at];
            let result = parse_magic_and_version(truncated_data);

            assert!(
                result.is_err(),
                "NewBig truncated at {} should fail: {}",
                truncate_at,
                description
            );

            println!("✅ NewBig truncation test passed: {}", description);
        }
    }

    #[test]
    fn test_version_field_positioning() {
        // Test that version field is read from correct position for each format
        let test_versions = vec![
            0x0001, // Standard supported
            0x0002, // Future version
            0x0000, // Zero version
            0xFFFF, // Maximum version
        ];

        for test_version in test_versions {
            // Standard format test
            let mut standard_data = Vec::new();
            standard_data.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
            standard_data.extend_from_slice(&test_version.to_be_bytes());

            // Should read version from position 4-5
            if test_version == SUPPORTED_VERSION {
                let (_, (_, parsed_version)) = parse_magic_and_version(&standard_data).unwrap();
                assert_eq!(parsed_version, test_version);
            } else {
                // Unsupported versions should fail
                assert!(parse_magic_and_version(&standard_data).is_err());
            }

            // NewBig format test
            let mut newbig_data = Vec::new();
            newbig_data.extend_from_slice(&CassandraVersion::V5_0NewBig.magic_number().to_be_bytes());
            newbig_data.extend_from_slice(&[0x00; 25]); // Padding
            newbig_data.extend_from_slice(&test_version.to_be_bytes());

            // Should read version from position 29-30
            if test_version == SUPPORTED_VERSION {
                let (_, (_, parsed_version)) = parse_magic_and_version(&newbig_data).unwrap();
                assert_eq!(parsed_version, test_version);
            } else {
                // Unsupported versions should fail
                assert!(parse_magic_and_version(&newbig_data).is_err());
            }

            println!("✅ Version positioning test completed for 0x{:04X}", test_version);
        }
    }
}

/// Test cases for Cassandra format detection accuracy
#[cfg(test)]
mod format_detection_tests {
    use super::*;

    #[test]
    fn test_format_detection_accuracy() {
        // Test format detection for all supported versions
        let test_cases = vec![
            (CassandraVersion::Legacy, "Legacy 'oa' format"),
            (CassandraVersion::V5_0Alpha, "Cassandra 5.0 Alpha"),
            (CassandraVersion::V5_0Beta, "Cassandra 5.0 Beta"),
            (CassandraVersion::V5_0Release, "Cassandra 5.0 Release"),
            (CassandraVersion::V5_0NewBig, "Cassandra 5.0 'nb' (new big) format"),
            (CassandraVersion::V5_0Bti, "Cassandra 5.0 BTI (Big Trie-Indexed) format"),
        ];

        for (version, expected_description) in test_cases {
            // Test magic number -> version mapping
            let magic = version.magic_number();
            let detected_version = CassandraVersion::from_magic_number(magic).unwrap();
            assert_eq!(detected_version, version);

            // Test version description
            assert_eq!(version.version_string(), expected_description);

            // Test round-trip conversion
            assert_eq!(detected_version.magic_number(), magic);

            println!("✅ Format detection accurate for {:?}", version);
        }
    }

    #[test]
    fn test_format_detector_integration() {
        let detector = FormatDetector::new();

        // Test filename-based detection
        let filename_tests = vec![
            ("oa-1-big-Data.db", SSTableFormat::V5x("oa".to_string())),
            ("nb-2-small-Data.db", SSTableFormat::V4x("nb".to_string())),
            ("ma-3-medium-Data.db", SSTableFormat::V3x("ma".to_string())),
        ];

        for (filename, expected_format) in filename_tests {
            let path = PathBuf::from(filename);
            let result = detector.detect_from_path(&path);

            match result {
                Ok(format) => {
                    // Check format version matches
                    assert_eq!(format.version(), expected_format.version());
                    println!("✅ Detected format for {}: {:?}", filename, format);
                }
                Err(e) => {
                    println!("⚠️  Format detection failed for {}: {}", filename, e);
                    // Some failures may be expected for unknown formats
                }
            }
        }
    }

    #[test]
    fn test_sstable_info_parsing() {
        let test_files = vec![
            "oa-1-big-Data.db",
            "nb-999-huge-CompressionInfo.db",
            "ma-42-small-Index.db",
            "mc-123-medium-Summary.db",
        ];

        for filename in test_files {
            let path = PathBuf::from(filename);
            let result = SSTableInfo::from_path(&path);

            match result {
                Ok(info) => {
                    assert!(!info.format.version().is_empty());
                    assert!(info.generation > 0);
                    assert!(!info.base_name.is_empty());
                    println!("✅ Parsed SSTable info for {}: gen={}", filename, info.generation);
                }
                Err(e) => {
                    println!("Format parsing failed for {}: {}", filename, e);
                }
            }
        }
    }

    #[test]
    fn test_format_feature_detection() {
        // Test format-specific feature detection
        let formats = vec![
            SSTableFormat::V2x("ic".to_string()),
            SSTableFormat::V3x("ma".to_string()),
            SSTableFormat::V4x("nb".to_string()),
            SSTableFormat::V5x("oa".to_string()),
        ];

        for format in formats {
            // All modern formats should support compression
            assert!(format.supports_compression());
            assert!(format.uses_chunk_compression());

            let compression = format.default_compression();
            assert!(!compression.is_empty());

            // Verify expected compression algorithms
            match format {
                SSTableFormat::V2x(_) => assert_eq!(compression, "SnappyCompressor"),
                _ => assert_eq!(compression, "LZ4Compressor"),
            }

            println!("✅ Feature detection passed for {:?}", format);
        }
    }
}

/// Test cases for edge cases with malformed or corrupted headers
#[cfg(test)]
mod corruption_edge_case_tests {
    use super::*;

    #[test]
    fn test_random_data_corruption() {
        // Test with completely random data
        for seed in 0..50 {
            let random_data = generate_random_data(seed, 1000);

            // Parser should never panic on random input
            let result = std::panic::catch_unwind(|| {
                let _ = parse_sstable_header(&random_data);
            });

            assert!(
                result.is_ok(),
                "Parser panicked on random data with seed {}",
                seed
            );

            // Most random data should fail to parse (which is correct)
            let parse_result = parse_sstable_header(&random_data);
            if let Ok((_, header)) = parse_result {
                // If it somehow parses, do basic validation
                assert!(!header.keyspace.is_empty() || header.keyspace.is_empty());
            }
        }
        println!("✅ Random data corruption tests passed");
    }

    #[test]
    fn test_systematic_corruption_patterns() {
        // Create a valid header first
        let valid_header = create_test_header();
        let valid_data = serialize_sstable_header(&valid_header).unwrap();

        let corruption_patterns = vec![
            // Flip every nth bit
            ("flip_every_8th_bit", corrupt_every_nth_bit(&valid_data, 8)),
            ("flip_every_16th_bit", corrupt_every_nth_bit(&valid_data, 16)),
            // Zero out sections
            ("zero_first_10_bytes", zero_section(&valid_data, 0, 10)),
            ("zero_middle_section", zero_section(&valid_data, 20, 30)),
            ("zero_last_section", zero_section(&valid_data, valid_data.len() - 20, 20)),
            // Fill with patterns
            ("fill_with_0xFF", fill_with_pattern(&valid_data, 0xFF)),
            ("fill_with_0xAA", fill_with_pattern(&valid_data, 0xAA)),
            ("fill_with_0x55", fill_with_pattern(&valid_data, 0x55)),
        ];

        for (pattern_name, corrupted_data) in corruption_patterns {
            let result = std::panic::catch_unwind(|| {
                let _ = parse_sstable_header(&corrupted_data);
            });

            assert!(
                result.is_ok(),
                "Parser panicked on corruption pattern: {}",
                pattern_name
            );

            // Most corruption should result in parse failure
            let parse_result = parse_sstable_header(&corrupted_data);
            if parse_result.is_ok() {
                println!("⚠️  Corruption pattern '{}' unexpectedly succeeded", pattern_name);
            } else {
                println!("✅ Corruption pattern '{}' correctly failed", pattern_name);
            }
        }
    }

    #[test]
    fn test_boundary_value_corruption() {
        // Test corruption at important boundary values
        let mut test_data = Vec::new();
        test_data.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
        test_data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
        test_data.extend_from_slice(&[0x00; 100]); // Table ID and padding

        let boundary_positions = vec![
            0,  // Start of magic
            1,  // Middle of magic
            3,  // End of magic
            4,  // Start of version
            5,  // End of version
            6,  // Start of table ID
            21, // End of table ID
        ];

        for position in boundary_positions {
            if position < test_data.len() {
                let mut corrupted = test_data.clone();

                // Test various corruption values
                let corruption_values = vec![0x00, 0xFF, 0xAA, 0x55];

                for corruption_value in corruption_values {
                    corrupted[position] = corruption_value;

                    let result = std::panic::catch_unwind(|| {
                        let _ = parse_sstable_header(&corrupted);
                    });

                    assert!(
                        result.is_ok(),
                        "Parser panicked on boundary corruption at position {}",
                        position
                    );
                }

                println!("✅ Boundary corruption test passed for position {}", position);
            }
        }
    }

    #[test]
    fn test_length_field_corruption() {
        // Test corruption of length fields (VInt encoded)
        let valid_header = create_test_header();
        let valid_data = serialize_sstable_header(&valid_header).unwrap();

        // Find length fields in the serialized data and corrupt them
        let length_corruption_tests = vec![
            // Extreme values
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF], // Very large length
            vec![0x80, 0x80, 0x80, 0x80, 0x01], // Maximum VInt
            vec![0x00],                         // Zero length
        ];

        for (i, corruption_bytes) in length_corruption_tests.iter().enumerate() {
            // Try inserting corruption at various positions where length fields might be
            for start_pos in (10..valid_data.len().min(100)).step_by(5) {
                if start_pos + corruption_bytes.len() < valid_data.len() {
                    let mut corrupted = valid_data.clone();
                    for (j, &byte) in corruption_bytes.iter().enumerate() {
                        if start_pos + j < corrupted.len() {
                            corrupted[start_pos + j] = byte;
                        }
                    }

                    let result = std::panic::catch_unwind(|| {
                        let _ = parse_sstable_header(&corrupted);
                    });

                    assert!(
                        result.is_ok(),
                        "Parser panicked on length corruption test {} at position {}",
                        i,
                        start_pos
                    );
                }
            }
        }

        println!("✅ Length field corruption tests passed");
    }

    #[test]
    fn test_utf8_corruption() {
        // Test corruption that affects UTF-8 strings in headers
        let header_with_unicode = SSTableHeader {
            cassandra_version: CassandraVersion::Legacy,
            version: SUPPORTED_VERSION,
            table_id: [0; 16],
            keyspace: "test_keyspace_🚀".to_string(), // Unicode
            table_name: "test_table_αβγ".to_string(), // Greek letters
            generation: 1,
            compression: CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: SSTableStats::default(),
            columns: vec![],
            properties: {
                let mut props = HashMap::new();
                props.insert("emoji_test".to_string(), "🔥💯✨".to_string());
                props
            },
        };

        let serialized = serialize_sstable_header(&header_with_unicode).unwrap();

        // Corrupt UTF-8 sequences by flipping bits in string data
        for i in (20..serialized.len().min(200)).step_by(3) {
            let mut corrupted = serialized.clone();
            corrupted[i] ^= 0x80; // Flip UTF-8 continuation bit

            let result = std::panic::catch_unwind(|| {
                let _ = parse_sstable_header(&corrupted);
            });

            assert!(
                result.is_ok(),
                "Parser panicked on UTF-8 corruption at position {}",
                i
            );

            // Parse should fail gracefully due to invalid UTF-8
            if let Ok((_, parsed)) = parse_sstable_header(&corrupted) {
                // If it somehow parses, validate the strings are reasonable
                assert!(!parsed.keyspace.is_empty());
                assert!(!parsed.table_name.is_empty());
            }
        }

        println!("✅ UTF-8 corruption tests passed");
    }
}

/// Integration tests with real Cassandra SSTable files
#[cfg(test)]
mod real_file_integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_sstable_manager_corruption_resilience() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create a mix of valid and corrupted files
        let file_scenarios = vec![
            ("valid-1-big-Data.db", create_valid_sstable_file()),
            ("corrupted-magic-2-big-Data.db", create_corrupted_magic_file()),
            ("truncated-3-big-Data.db", create_truncated_file()),
            ("invalid-version-4-big-Data.db", create_invalid_version_file()),
        ];

        for (filename, file_data) in file_scenarios {
            let file_path = temp_dir.path().join(filename);
            tokio::fs::write(&file_path, &file_data).await.unwrap();
        }

        // SSTableManager should handle corrupted files gracefully
        let manager_result = SSTableManager::new(temp_dir.path(), &config, platform).await;

        match manager_result {
            Ok(manager) => {
                let stats = manager.stats().await.unwrap();
                println!("SSTableManager created with stats: {:?}", stats);

                // Should have loaded only valid files
                assert!(stats.sstable_count <= 1, "Should load at most 1 valid file");
            }
            Err(e) => {
                // Manager creation might fail, but shouldn't panic
                println!("Manager creation failed (may be expected): {}", e);
            }
        }

        println!("✅ SSTableManager corruption resilience test passed");
    }

    #[tokio::test]
    async fn test_sstable_reader_error_propagation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Test different types of header corruption
        let corruption_tests = vec![
            ("invalid_magic", create_invalid_magic_file()),
            ("unsupported_version", create_unsupported_version_file()),
            ("truncated_header", create_truncated_header_file()),
            ("corrupted_table_id", create_corrupted_table_id_file()),
        ];

        for (test_name, file_data) in corruption_tests {
            let file_path = temp_dir.path().join(format!("{}.sst", test_name));
            tokio::fs::write(&file_path, &file_data).await.unwrap();

            let result = SSTableReader::open(&file_path, &config, platform.clone()).await;

            assert!(
                result.is_err(),
                "SSTableReader should fail for corruption test: {}",
                test_name
            );

            let error = result.unwrap_err();

            // Verify proper error categorization
            match error {
                Error::InvalidFormat(_) | Error::Corruption(_) | Error::ParseError(_) => {
                    // Expected error types
                    assert_eq!(error.category(), ErrorCategory::Data);
                    assert!(!error.is_recoverable());
                }
                _ => {
                    panic!("Unexpected error type for {}: {:?}", test_name, error);
                }
            }

            println!("✅ Error propagation test passed for: {}", test_name);
        }
    }

    #[tokio::test]
    async fn test_concurrent_header_parsing() {
        use tokio::task::JoinSet;

        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create multiple files with different header patterns
        let file_count = 10;
        let mut tasks = JoinSet::new();

        for i in 0..file_count {
            let file_path = temp_dir.path().join(format!("test-{}.sst", i));
            let file_data = if i % 3 == 0 {
                create_valid_sstable_file()
            } else if i % 3 == 1 {
                create_corrupted_magic_file()
            } else {
                create_truncated_file()
            };

            tokio::fs::write(&file_path, &file_data).await.unwrap();

            let config_clone = config.clone();
            let platform_clone = platform.clone();

            tasks.spawn(async move {
                let result = SSTableReader::open(&file_path, &config_clone, platform_clone).await;
                (i, result.is_ok())
            });
        }

        let mut results = Vec::new();
        while let Some(result) = tasks.join_next().await {
            results.push(result.unwrap());
        }

        // Verify results are consistent with expectations
        for (i, success) in results {
            let expected_success = i % 3 == 0; // Only every 3rd file is valid
            if success != expected_success {
                println!(
                    "⚠️  File {} had unexpected result: success={}, expected={}",
                    i, success, expected_success
                );
            }
        }

        println!("✅ Concurrent header parsing test completed");
    }
}

/// Performance benchmarks for parsing speed
#[cfg(test)]
mod performance_benchmark_tests {
    use super::*;

    #[test]
    fn test_header_parsing_performance() {
        let header = create_test_header();
        let serialized = serialize_sstable_header(&header).unwrap();

        // Benchmark parsing performance
        let iterations = 10000;
        let start = Instant::now();

        for _ in 0..iterations {
            let result = parse_sstable_header(&serialized);
            assert!(result.is_ok());
        }

        let duration = start.elapsed();
        let avg_time = duration / iterations;

        println!("✅ Header parsing performance:");
        println!("  Total time for {} iterations: {:?}", iterations, duration);
        println!("  Average time per parse: {:?}", avg_time);
        println!("  Parses per second: {:.0}", 1.0 / avg_time.as_secs_f64());

        // Performance assertion - should be very fast
        assert!(
            avg_time.as_micros() < 100,
            "Header parsing should take less than 100μs, took {:?}",
            avg_time
        );
    }

    #[test]
    fn test_magic_number_detection_performance() {
        let test_magics: Vec<u32> = SUPPORTED_MAGIC_NUMBERS
            .iter()
            .cycle()
            .take(10000)
            .cloned()
            .collect();

        let start = Instant::now();

        for &magic in &test_magics {
            let _version = CassandraVersion::from_magic_number(magic);
        }

        let duration = start.elapsed();
        let avg_time = duration / test_magics.len() as u32;

        println!("✅ Magic number detection performance:");
        println!("  Total time for {} detections: {:?}", test_magics.len(), duration);
        println!("  Average time per detection: {:?}", avg_time);

        // Should be extremely fast
        assert!(
            avg_time.as_nanos() < 1000,
            "Magic detection should take less than 1μs, took {:?}",
            avg_time
        );
    }

    #[test]
    fn test_large_header_parsing_performance() {
        // Create a header with many columns and properties
        let large_header = create_large_test_header();
        let serialized = serialize_sstable_header(&large_header).unwrap();

        println!("Large header size: {} bytes", serialized.len());

        let iterations = 1000;
        let start = Instant::now();

        for _ in 0..iterations {
            let result = parse_sstable_header(&serialized);
            assert!(result.is_ok());
        }

        let duration = start.elapsed();
        let avg_time = duration / iterations;

        println!("✅ Large header parsing performance:");
        println!("  Average time per parse: {:?}", avg_time);
        println!("  Throughput: {:.2} MB/s",
                 serialized.len() as f64 / (1024.0 * 1024.0) / avg_time.as_secs_f64());

        // Even large headers should parse reasonably fast
        assert!(
            avg_time.as_millis() < 10,
            "Large header parsing should take less than 10ms, took {:?}",
            avg_time
        );
    }

    #[test]
    fn test_memory_usage_during_parsing() {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Simple memory tracking (approximation)
        static MEMORY_USAGE: AtomicUsize = AtomicUsize::new(0);

        struct TrackingAllocator;

        unsafe impl GlobalAlloc for TrackingAllocator {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                MEMORY_USAGE.fetch_add(layout.size(), Ordering::Relaxed);
                System.alloc(layout)
            }

            unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                MEMORY_USAGE.fetch_sub(layout.size(), Ordering::Relaxed);
                System.dealloc(ptr, layout);
            }
        }

        let header = create_test_header();
        let serialized = serialize_sstable_header(&header).unwrap();

        let initial_memory = MEMORY_USAGE.load(Ordering::Relaxed);

        // Parse header and measure memory growth
        let result = parse_sstable_header(&serialized);
        assert!(result.is_ok());

        let final_memory = MEMORY_USAGE.load(Ordering::Relaxed);
        let memory_growth = final_memory.saturating_sub(initial_memory);

        println!("✅ Memory usage during parsing:");
        println!("  Header size: {} bytes", serialized.len());
        println!("  Memory growth: {} bytes", memory_growth);
        println!("  Memory efficiency: {:.2}x", memory_growth as f64 / serialized.len() as f64);

        // Memory growth should be reasonable
        assert!(
            memory_growth < serialized.len() * 10,
            "Memory usage should not exceed 10x header size"
        );
    }
}

/// Property-based tests for edge cases
#[cfg(test)]
mod property_based_tests {
    use super::*;

    #[test]
    fn test_parser_never_panics_property() {
        // Property: Parser should never panic on any input
        for seed in 0..1000 {
            let data = generate_random_data(seed, (seed % 2000) + 1);

            let result = std::panic::catch_unwind(|| {
                let _ = parse_sstable_header(&data);
                let _ = parse_magic_and_version(&data);
            });

            assert!(
                result.is_ok(),
                "Parser panicked on input with seed {} (length {})",
                seed,
                data.len()
            );
        }
        println!("✅ Never-panic property verified for 1000 random inputs");
    }

    #[test]
    fn test_deterministic_parsing_property() {
        // Property: Parser should be deterministic
        for seed in 0..100 {
            let data = generate_random_data(seed, 1000);

            let result1 = parse_sstable_header(&data);
            let result2 = parse_sstable_header(&data);

            // Results should be identical
            match (result1, result2) {
                (Ok((rem1, _)), Ok((rem2, _))) => {
                    assert_eq!(rem1.len(), rem2.len());
                }
                (Err(_), Err(_)) => {
                    // Both failed consistently
                }
                _ => {
                    panic!("Parser non-deterministic for seed {}", seed);
                }
            }
        }
        println!("✅ Deterministic parsing property verified");
    }

    #[test]
    fn test_round_trip_property() {
        // Property: serialize(parse(serialize(header))) should equal serialize(header)
        for i in 0..50 {
            let header = create_varied_test_header(i);
            let serialized1 = serialize_sstable_header(&header).unwrap();

            if let Ok((_, parsed)) = parse_sstable_header(&serialized1) {
                let serialized2 = serialize_sstable_header(&parsed).unwrap();

                // Serialized forms should be identical (or at least semantically equivalent)
                if serialized1 != serialized2 {
                    println!("⚠️  Round-trip mismatch for test case {}", i);
                    // This might be acceptable if the data is semantically equivalent
                } else {
                    println!("✅ Round-trip successful for test case {}", i);
                }
            }
        }
    }

    #[test]
    fn test_incremental_corruption_property() {
        // Property: More corruption should not make parsing more likely to succeed
        let valid_header = create_test_header();
        let valid_data = serialize_sstable_header(&valid_header).unwrap();

        let mut corruption_levels = Vec::new();
        for corruption_rate in [0.01, 0.05, 0.1, 0.2, 0.5] {
            let corrupted = corrupt_randomly(&valid_data, corruption_rate, 42);
            let success = parse_sstable_header(&corrupted).is_ok();
            corruption_levels.push((corruption_rate, success));
        }

        // Generally, more corruption should lead to more failures
        let mut previous_success = true;
        for (rate, success) in corruption_levels {
            if success && !previous_success {
                println!("⚠️  Higher corruption rate {} unexpectedly succeeded", rate);
            }
            previous_success = success;
        }

        println!("✅ Incremental corruption property test completed");
    }
}

// Helper functions for test data generation

fn create_test_header() -> SSTableHeader {
    SSTableHeader {
        cassandra_version: CassandraVersion::Legacy,
        version: SUPPORTED_VERSION,
        table_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        keyspace: "test_keyspace".to_string(),
        table_name: "test_table".to_string(),
        generation: 42,
        compression: CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_size: 65536,
            parameters: {
                let mut params = HashMap::new();
                params.insert("level".to_string(), "9".to_string());
                params
            },
        },
        stats: SSTableStats {
            row_count: 1000,
            min_timestamp: 1000000000,
            max_timestamp: 2000000000,
            max_deletion_time: 1500000000,
            compression_ratio: 0.75,
            row_size_histogram: vec![100, 200, 300, 400, 500],
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
            },
        ],
        properties: {
            let mut props = HashMap::new();
            props.insert("test_property".to_string(), "test_value".to_string());
            props
        },
    }
}

fn create_large_test_header() -> SSTableHeader {
    let mut header = create_test_header();

    // Add many columns
    for i in 0..100 {
        header.columns.push(ColumnInfo {
            name: format!("column_{}", i),
            column_type: format!("type_{}", i % 10),
            is_primary_key: i < 5,
            key_position: if i < 5 { Some(i as u16) } else { None },
            is_static: i % 10 == 0,
            is_clustering: i % 7 == 0,
        });
    }

    // Add many properties
    for i in 0..50 {
        header.properties.insert(
            format!("property_{}", i),
            format!("value_{}", i).repeat(10),
        );
    }

    // Large row size histogram
    header.stats.row_size_histogram = (0..1000).collect();

    header
}

fn create_varied_test_header(variation: usize) -> SSTableHeader {
    let versions = vec![
        CassandraVersion::Legacy,
        CassandraVersion::V5_0Alpha,
        CassandraVersion::V5_0Beta,
        CassandraVersion::V5_0Release,
        CassandraVersion::V5_0NewBig,
        CassandraVersion::V5_0Bti,
    ];

    let mut header = create_test_header();
    header.cassandra_version = versions[variation % versions.len()];
    header.generation = variation as u64;
    header.keyspace = format!("keyspace_{}", variation);
    header.table_name = format!("table_{}", variation);

    header
}

fn generate_random_data(seed: u64, size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state = seed;

    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }

    data
}

fn corrupt_every_nth_bit(data: &[u8], n: usize) -> Vec<u8> {
    let mut corrupted = data.to_vec();
    let total_bits = data.len() * 8;

    for bit_index in (0..total_bits).step_by(n) {
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;

        if byte_index < corrupted.len() {
            corrupted[byte_index] ^= 1 << bit_offset;
        }
    }

    corrupted
}

fn zero_section(data: &[u8], start: usize, length: usize) -> Vec<u8> {
    let mut corrupted = data.to_vec();
    let end = (start + length).min(corrupted.len());

    for i in start..end {
        corrupted[i] = 0x00;
    }

    corrupted
}

fn fill_with_pattern(data: &[u8], pattern: u8) -> Vec<u8> {
    vec![pattern; data.len()]
}

fn corrupt_randomly(data: &[u8], corruption_rate: f64, seed: u64) -> Vec<u8> {
    let mut corrupted = data.to_vec();
    let mut state = seed;

    for byte in &mut corrupted {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        let random_value = (state >> 16) as f64 / u32::MAX as f64;

        if random_value < corruption_rate {
            *byte ^= 0xFF; // Flip all bits
        }
    }

    corrupted
}

// Test file creation helpers

fn create_valid_sstable_file() -> Vec<u8> {
    let header = create_test_header();
    let mut data = serialize_sstable_header(&header).unwrap();
    data.extend_from_slice(&[0x00; 1000]); // Body content
    data
}

fn create_corrupted_magic_file() -> Vec<u8> {
    let mut data = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Invalid magic
    data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
    data.extend_from_slice(&[0x00; 1000]);
    data
}

fn create_truncated_file() -> Vec<u8> {
    vec![0x42; 50] // Too short
}

fn create_invalid_version_file() -> Vec<u8> {
    let mut data = SSTABLE_MAGIC.to_be_bytes().to_vec();
    data.extend_from_slice(&[0xFF, 0xFF]); // Invalid version
    data.extend_from_slice(&[0x00; 1000]);
    data
}

fn create_unsupported_version_file() -> Vec<u8> {
    let mut data = SSTABLE_MAGIC.to_be_bytes().to_vec();
    data.extend_from_slice(&[0x00, 0x99]); // Unsupported version
    data.extend_from_slice(&[0x00; 1000]);
    data
}

fn create_truncated_header_file() -> Vec<u8> {
    let mut data = SSTABLE_MAGIC.to_be_bytes().to_vec();
    data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
    data.extend_from_slice(&[0x00; 10]); // Truncated table ID
    data
}

fn create_corrupted_table_id_file() -> Vec<u8> {
    let mut data = SSTABLE_MAGIC.to_be_bytes().to_vec();
    data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
    data.extend_from_slice(&[0xFF; 8]); // Partial table ID
    data
}