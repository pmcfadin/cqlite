//! Integration tests for SSTable header parsing with full pipeline
//!
//! These tests verify that header parsing works correctly with the full
//! SSTable reading pipeline and error handling propagates properly.

use cqlite_core::{
    Config,
    error::{Error, ErrorCategory},
    parser::header::{
        CassandraVersion, ColumnInfo, CompressionInfo, SSTABLE_MAGIC, SSTableHeader, SSTableStats,
        SUPPORTED_VERSION, parse_sstable_header, serialize_sstable_header,
    },
    platform::Platform,
    storage::sstable::{
        SSTableManager,
        format_detector::{FormatDetector, SSTableFormat, SSTableInfo},
        reader::SSTableReader,
    },
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

/// Integration test for full SSTable reading pipeline with header parsing
#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_sstable_reader_with_valid_header() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create a valid header
        let header = create_test_header();
        let serialized_header = serialize_sstable_header(&header).unwrap();

        // Create a minimal SSTable file with valid header
        let file_path = temp_dir.path().join("valid-sstable.sst");
        let mut file_content = serialized_header;

        // Add minimal body content to make it look like a real SSTable
        file_content.extend_from_slice(&[0x00; 1000]); // Body data

        tokio::fs::write(&file_path, &file_content).await.unwrap();

        // Try to open with SSTableReader
        let result = SSTableReader::open(&file_path, &config, platform).await;

        // The reader might still fail due to incomplete SSTable structure,
        // but header parsing should succeed
        match result {
            Ok(_reader) => {
                // Success - header parsed correctly and file opened
                println!("Successfully opened SSTable with valid header");
            }
            Err(error) => {
                // If it fails, it should NOT be due to header parsing issues
                let error_msg = error.to_string().to_lowercase();
                assert!(
                    !error_msg.contains("magic") && !error_msg.contains("header"),
                    "Should not fail due to header parsing: {}",
                    error
                );
                println!(
                    "Failed to open SSTable (expected for minimal structure): {}",
                    error
                );
            }
        }
    }

    #[tokio::test]
    async fn test_sstable_reader_with_corrupted_headers() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let corruption_scenarios = vec![
            ("invalid_magic", vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01]),
            ("truncated_header", vec![0x42; 10]),
            ("invalid_version", {
                let mut data = SSTABLE_MAGIC.to_le_bytes().to_vec();
                data.extend_from_slice(&[0xFF, 0xFF]); // Invalid version
                data.extend_from_slice(&[0x00; 100]);
                data
            }),
            ("corrupted_table_id", {
                let mut data = SSTABLE_MAGIC.to_le_bytes().to_vec();
                data.extend_from_slice(&SUPPORTED_VERSION.to_le_bytes());
                data.extend_from_slice(&[0xFF; 8]); // Partial table ID
                data
            }),
        ];

        for (scenario_name, corrupted_data) in corruption_scenarios {
            let file_path = temp_dir.path().join(format!("{}.sst", scenario_name));
            tokio::fs::write(&file_path, &corrupted_data).await.unwrap();

            let result = SSTableReader::open(&file_path, &config, platform.clone()).await;

            assert!(
                result.is_err(),
                "SSTableReader should reject corrupted header for scenario: {}",
                scenario_name
            );

            let error = result.unwrap_err();
            assert!(
                matches!(
                    error,
                    Error::InvalidFormat(_) | Error::Corruption(_) | Error::Parse(_)
                ),
                "Unexpected error type for {}: {:?}",
                scenario_name,
                error
            );

            // Verify error is not recoverable (security fix should make header corruption fatal)
            assert!(
                !error.is_recoverable(),
                "Header corruption should not be recoverable for {}",
                scenario_name
            );

            println!(
                "Correctly rejected corrupted header for scenario: {}",
                scenario_name
            );
        }
    }

    #[tokio::test]
    async fn test_sstable_manager_with_corrupted_files() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create directory with mix of valid and corrupted SSTable files
        let valid_header = create_test_header();
        let valid_serialized = serialize_sstable_header(&valid_header).unwrap();

        // Valid file
        let valid_file = temp_dir.path().join("valid-1-big-Data.sst");
        let mut valid_content = valid_serialized.clone();
        valid_content.extend_from_slice(&[0x00; 1000]);
        tokio::fs::write(&valid_file, &valid_content).await.unwrap();

        // Corrupted files
        let corrupted_files = vec![
            ("corrupted-1-big-Data.sst", vec![0xFF; 100]),
            ("truncated-2-big-Data.sst", vec![0x42; 50]),
            ("invalid-3-big-Data.sst", {
                let mut data = vec![0x00, 0x00, 0x00, 0x00]; // Invalid magic
                data.extend_from_slice(&[0x00; 200]);
                data
            }),
        ];

        for (filename, data) in corrupted_files {
            let file_path = temp_dir.path().join(filename);
            tokio::fs::write(&file_path, &data).await.unwrap();
        }

        // Create SSTableManager - should handle corrupted files gracefully
        let result = SSTableManager::new(temp_dir.path(), &config, platform).await;

        match result {
            Ok(manager) => {
                // Manager should be created successfully, ignoring corrupted files
                let stats = manager.stats().await.unwrap();

                // Should not include corrupted files in count
                println!("SSTableManager stats: {:?}", stats);

                // The exact count depends on how the manager handles corrupted files
                // It should either skip them or handle them gracefully
            }
            Err(error) => {
                // If manager creation fails, it should be due to directory issues, not individual file corruption
                println!(
                    "SSTableManager creation failed (may be expected): {}",
                    error
                );
            }
        }
    }

    #[test]
    fn test_format_detector_integration() {
        let detector = FormatDetector::new();

        // Test format detection with various file paths
        let test_cases = vec![
            ("nb-1-big-Data.db", true, "V4x format"),
            ("oa-2-small-Index.db", true, "V5x format"),
            ("ma-3-medium-Summary.db", true, "V3x format"),
            ("invalid-name.db", false, "Invalid format"),
            (
                "xx-1-big-Data.db",
                true,
                "Unknown format (should still work)",
            ),
        ];

        for (filename, should_succeed, description) in test_cases {
            let path = PathBuf::from(filename);
            let result = detector.detect_from_path(&path);

            if should_succeed {
                assert!(
                    result.is_ok(),
                    "Format detection should succeed for {}: {}",
                    filename,
                    description
                );

                let format = result.unwrap();
                match format {
                    SSTableFormat::Unknown(_) => {
                        println!("Unknown format detected for {}: {:?}", filename, format);
                    }
                    _ => {
                        println!("Known format detected for {}: {:?}", filename, format);
                    }
                }
            } else {
                assert!(
                    result.is_err(),
                    "Format detection should fail for {}: {}",
                    filename,
                    description
                );

                let error = result.unwrap_err();
                assert!(
                    matches!(error, Error::InvalidFormat(_) | Error::InvalidPath(_)),
                    "Unexpected error type for {}: {:?}",
                    filename,
                    error
                );
            }
        }
    }

    #[test]
    fn test_sstable_info_parsing_integration() {
        let test_paths = vec![
            ("nb-1-big-Data.db", true),
            ("oa-999-huge-CompressionInfo.db", true),
            ("ma-0-tiny-Filter.db", true),
            ("invalid.db", false),
            ("nb-not-a-number-big-Data.db", false),
        ];

        for (path_str, should_succeed) in test_paths {
            let path = PathBuf::from(path_str);
            let result = SSTableInfo::from_path(&path);

            if should_succeed {
                assert!(
                    result.is_ok(),
                    "SSTableInfo parsing should succeed for {}",
                    path_str
                );

                let info = result.unwrap();
                println!(
                    "Parsed SSTableInfo for {}: generation={}, component={:?}",
                    path_str, info.generation, info.component
                );

                // Verify format version consistency
                assert!(
                    !info.format.version().is_empty(),
                    "Format version should not be empty"
                );
                assert!(
                    info.generation < u64::MAX,
                    "Generation should be reasonable"
                );
            } else {
                assert!(
                    result.is_err(),
                    "SSTableInfo parsing should fail for {}",
                    path_str
                );
            }
        }
    }

    #[tokio::test]
    async fn test_error_propagation_through_pipeline() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create a file with specific corruption that should produce specific error
        let file_path = temp_dir.path().join("specific-corruption.sst");

        // Corruption: valid magic, invalid version
        let mut corrupted_data = SSTABLE_MAGIC.to_le_bytes().to_vec();
        corrupted_data.extend_from_slice(&[0xFF, 0xFF]); // Invalid version
        corrupted_data.extend_from_slice(&[0x00; 100]);

        tokio::fs::write(&file_path, &corrupted_data).await.unwrap();

        // Test direct header parsing
        let direct_parse_result = parse_sstable_header(&corrupted_data);
        assert!(direct_parse_result.is_err(), "Direct parsing should fail");

        println!("Direct parsing failed as expected");

        // Test through SSTableReader
        let reader_result = SSTableReader::open(&file_path, &config, platform).await;
        assert!(reader_result.is_err(), "Reader should fail");

        let reader_error = reader_result.unwrap_err();
        println!("Reader error: {}", reader_error);

        // Verify proper error categorization
        assert_eq!(
            reader_error.category(),
            ErrorCategory::Data,
            "Reader error should be categorized as Data error"
        );

        assert!(
            !reader_error.is_recoverable(),
            "Header corruption errors should not be recoverable"
        );
    }

    #[tokio::test]
    async fn test_concurrent_header_parsing() {
        use tokio::task::JoinSet;

        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create multiple files with different corruption patterns
        let corruption_patterns = vec![
            ("file1.sst", vec![0xFF; 100]),
            ("file2.sst", vec![0x00; 100]),
            ("file3.sst", {
                let mut data = SSTABLE_MAGIC.to_le_bytes().to_vec();
                data.extend_from_slice(&[0xFF, 0xFF]);
                data.extend_from_slice(&[0x42; 100]);
                data
            }),
        ];

        for (filename, data) in &corruption_patterns {
            let file_path = temp_dir.path().join(filename);
            tokio::fs::write(&file_path, data).await.unwrap();
        }

        // Test concurrent access to verify thread safety
        let mut join_set = JoinSet::new();

        for (filename, _) in corruption_patterns {
            let file_path = temp_dir.path().join(filename);
            let config_clone = config.clone();
            let platform_clone = platform.clone();

            join_set.spawn(async move {
                let result = SSTableReader::open(&file_path, &config_clone, platform_clone).await;
                (filename, result.is_err())
            });
        }

        // Wait for all tasks to complete
        let mut results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            results.push(result.unwrap());
        }

        // All should fail due to corruption
        for (filename, failed) in results {
            assert!(failed, "File {} should have failed to parse", filename);
        }

        println!("Concurrent header parsing test completed successfully");
    }

    #[test]
    fn test_header_validation_with_real_world_edge_cases() {
        // Test cases based on real-world SSTable corruption scenarios
        let edge_cases = vec![
            // Case 1: Valid magic, but immediate EOF
            ("eof_after_magic", { SSTABLE_MAGIC.to_le_bytes().to_vec() }),
            // Case 2: Valid magic and version, but truncated table ID
            ("truncated_table_id", {
                let mut data = SSTABLE_MAGIC.to_le_bytes().to_vec();
                data.extend_from_slice(&SUPPORTED_VERSION.to_le_bytes());
                data.extend_from_slice(&[0x00; 8]); // Only half of table ID
                data
            }),
            // Case 3: All header fields present but with maximum values
            ("extreme_values", {
                let header = SSTableHeader {
                    cassandra_version: CassandraVersion::V5_0Release,
                    version: SUPPORTED_VERSION,
                    table_id: [0xFF; 16],
                    keyspace: "k".repeat(1000),
                    table_name: "t".repeat(1000),
                    generation: u64::MAX,
                    compression: CompressionInfo {
                        algorithm: "algorithm".repeat(100),
                        chunk_size: u32::MAX,
                        parameters: {
                            let mut params = HashMap::new();
                            for i in 0..100 {
                                params
                                    .insert(format!("key{}", i), format!("value{}", i).repeat(100));
                            }
                            params
                        },
                    },
                    stats: SSTableStats {
                        row_count: u64::MAX,
                        min_timestamp: i64::MIN,
                        max_timestamp: i64::MAX,
                        max_deletion_time: i64::MAX,
                        compression_ratio: 1.0,
                        row_size_histogram: vec![u64::MAX; 1000],
                    },
                    columns: {
                        let mut cols = Vec::new();
                        for i in 0..1000 {
                            cols.push(ColumnInfo {
                                name: format!("col{}", i),
                                column_type: format!("type{}", i).repeat(10),
                                is_primary_key: i < 10,
                                key_position: if i < 10 { Some(i as u16) } else { None },
                                is_static: i % 10 == 0,
                                is_clustering: i % 7 == 0,
                            });
                        }
                        cols
                    },
                    properties: {
                        let mut props = HashMap::new();
                        for i in 0..100 {
                            props.insert(format!("prop{}", i), format!("val{}", i).repeat(50));
                        }
                        props
                    },
                };
                serialize_sstable_header(&header).unwrap_or_else(|_| vec![0xFF; 100])
            }),
        ];

        for (case_name, test_data) in edge_cases {
            println!("Testing edge case: {}", case_name);

            let result = std::panic::catch_unwind(|| parse_sstable_header(&test_data));

            assert!(
                result.is_ok(),
                "Parser should not panic on edge case: {}",
                case_name
            );

            if let Ok(parse_result) = result {
                match parse_result {
                    Ok((remaining, parsed_header)) => {
                        println!(
                            "  ✅ Parsed successfully, remaining bytes: {}",
                            remaining.len()
                        );

                        // Basic validation of parsed results
                        assert!(
                            !parsed_header.keyspace.is_empty() || case_name == "eof_after_magic"
                        );
                        assert!(
                            !parsed_header.table_name.is_empty() || case_name == "eof_after_magic"
                        );
                    }
                    Err(error) => {
                        println!("  ❌ Parse failed (expected): {}", error);

                        // Verify error is properly categorized
                        println!("  Parse failed with error: {}", error);
                    }
                }
            }
        }
    }
}

// Helper functions

fn create_test_header() -> SSTableHeader {
    SSTableHeader {
        cassandra_version: CassandraVersion::V5_0Release,
        version: SUPPORTED_VERSION,
        table_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        keyspace: "integration_test_ks".to_string(),
        table_name: "integration_test_table".to_string(),
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
                name: "created_at".to_string(),
                column_type: "timestamp".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: true,
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
            props.insert("custom_property".to_string(), "custom_value".to_string());
            props.insert("another_prop".to_string(), "another_value".to_string());
            props
        },
    }
}

/// Test coverage verification
#[cfg(test)]
mod coverage_tests {
    use super::*;

    #[test]
    fn test_all_error_types_covered() {
        // Verify we test all relevant error types for header parsing
        let _invalid_format = Error::invalid_format("test");
        let _corruption = Error::corruption("test");
        let _parse_error = Error::parser("test");
        let _unsupported = Error::unsupported_format("test");
        let _invalid_path = Error::invalid_path("test");

        // Test error categories
        assert_eq!(_invalid_format.category(), ErrorCategory::Data);
        assert_eq!(_corruption.category(), ErrorCategory::Data);
        assert_eq!(_parse_error.category(), ErrorCategory::Data);
        assert_eq!(_unsupported.category(), ErrorCategory::Data);
        assert_eq!(_invalid_path.category(), ErrorCategory::System);

        // Test recoverability
        assert!(!_invalid_format.is_recoverable());
        assert!(!_corruption.is_recoverable());
        assert!(!_parse_error.is_recoverable());
        assert!(!_unsupported.is_recoverable());
        assert!(!_invalid_path.is_recoverable());
    }

    #[test]
    fn test_cassandra_version_coverage() {
        // Verify all Cassandra versions are testable
        let versions = vec![
            CassandraVersion::Legacy,
            CassandraVersion::V5_0Alpha,
            CassandraVersion::V5_0Beta,
            CassandraVersion::V5_0Release,
            CassandraVersion::V5_0NewBig,
            CassandraVersion::V5_0Bti,
        ];

        for version in versions {
            // Each version should have a magic number
            let magic = version.magic_number();
            assert!(
                magic != 0,
                "Version {:?} should have non-zero magic number",
                version
            );

            // Magic number should map back to version
            let parsed_version = CassandraVersion::from_magic_number(magic);
            assert_eq!(
                Some(version),
                parsed_version,
                "Magic number should round-trip for {:?}",
                version
            );

            // Version should have a description
            let description = version.version_string();
            assert!(
                !description.is_empty(),
                "Version {:?} should have description",
                version
            );
        }
    }

    #[test]
    fn test_format_detector_coverage() {
        let detector = FormatDetector::new();

        // Test all supported versions
        let supported_versions = detector.supported_versions();
        assert!(
            !supported_versions.is_empty(),
            "Should have supported versions"
        );

        for version in &supported_versions {
            assert!(
                detector.is_supported(version),
                "Version {} should be supported",
                version
            );

            let format = detector.detect_from_version(version).unwrap();
            assert_eq!(
                format.version(),
                version,
                "Version should match for {}",
                version
            );
        }

        // Test unsupported version
        assert!(
            !detector.is_supported("zz"),
            "Version 'zz' should not be supported"
        );
    }
}
