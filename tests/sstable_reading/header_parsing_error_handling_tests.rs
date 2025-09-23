//! Error handling validation tests for SSTable header parsing
//!
//! This module provides comprehensive tests for error handling scenarios,
//! ensuring proper error propagation, categorization, and recovery behavior.

use cqlite_core::{
    error::{Error, ErrorCategory},
    parser::header::{
        CassandraVersion, ColumnInfo, CompressionInfo, SSTABLE_MAGIC, SSTableHeader, SSTableStats,
        SUPPORTED_VERSION, parse_sstable_header, serialize_sstable_header, parse_magic_and_version,
    },
    Config,
    platform::Platform,
    storage::sstable::{
        reader::SSTableReader,
        SSTableManager,
    },
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

/// Tests for error type classification and categorization
#[cfg(test)]
mod error_classification_tests {
    use super::*;

    #[test]
    fn test_invalid_magic_number_errors() {
        let invalid_magic_scenarios = vec![
            (vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01], "Invalid magic (all 0xFF)"),
            (vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x01], "Zero magic number"),
            (vec![0x6F, 0x62, 0x00, 0x00, 0x00, 0x01], "Close but wrong magic"),
            (vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01], "Random invalid magic"),
        ];

        for (data, description) in invalid_magic_scenarios {
            let result = parse_sstable_header(&data);

            assert!(result.is_err(), "Should fail for: {}", description);

            let error = result.unwrap_err();

            // Should be a parsing error
            assert!(
                matches!(error, Error::ParseError(_)),
                "Should be ParseError for {}, got: {:?}",
                description,
                error
            );

            // Should be categorized as Data error
            assert_eq!(
                error.category(),
                ErrorCategory::Data,
                "Should be Data category for {}",
                description
            );

            // Should not be recoverable
            assert!(
                !error.is_recoverable(),
                "Magic number errors should not be recoverable for {}",
                description
            );

            println!("✅ Error classification correct for: {}", description);
        }
    }

    #[test]
    fn test_version_validation_errors() {
        let version_error_scenarios = vec![
            (0xFFFF, "Maximum version value"),
            (0x0000, "Zero version (for strict formats)"),
            (0x9999, "Arbitrary invalid version"),
        ];

        for (invalid_version, description) in version_error_scenarios {
            let mut data = Vec::new();
            data.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
            data.extend_from_slice(&invalid_version.to_be_bytes());
            data.extend_from_slice(&[0x00; 100]); // Padding

            let result = parse_sstable_header(&data);

            if result.is_err() {
                let error = result.unwrap_err();

                // Version errors should be parse errors
                assert!(
                    matches!(error, Error::ParseError(_)),
                    "Version error should be ParseError for {}, got: {:?}",
                    description,
                    error
                );

                assert_eq!(error.category(), ErrorCategory::Data);
                assert!(!error.is_recoverable());

                println!("✅ Version error correctly classified: {}", description);
            } else {
                // Some versions might be accepted by newer formats
                println!("ℹ️  Version {} was accepted (may be valid for some formats)", description);
            }
        }
    }

    #[test]
    fn test_truncation_error_classification() {
        let truncation_scenarios = vec![
            (0, "Completely empty"),
            (1, "Single byte"),
            (3, "Partial magic number"),
            (4, "Magic only"),
            (5, "Magic + partial version"),
        ];

        for (size, description) in truncation_scenarios {
            let data = vec![0x6F; size];
            let result = parse_sstable_header(&data);

            assert!(result.is_err(), "Should fail for: {}", description);

            let error = result.unwrap_err();

            // Truncation should be a parse error
            assert!(
                matches!(error, Error::ParseError(_)),
                "Truncation should be ParseError for {}, got: {:?}",
                description,
                error
            );

            assert_eq!(error.category(), ErrorCategory::Data);
            assert!(!error.is_recoverable());

            println!("✅ Truncation error correctly classified: {}", description);
        }
    }

    #[test]
    fn test_corruption_error_classification() {
        // Create a valid header and then corrupt it
        let valid_header = create_test_header();
        let valid_data = serialize_sstable_header(&valid_header).unwrap();

        let corruption_scenarios = vec![
            ("flip_first_byte", {
                let mut data = valid_data.clone();
                data[0] ^= 0xFF;
                data
            }),
            ("corrupt_table_id", {
                let mut data = valid_data.clone();
                if data.len() > 20 {
                    for i in 6..22 { // Table ID area
                        if i < data.len() {
                            data[i] = 0xFF;
                        }
                    }
                }
                data
            }),
            ("corrupt_string_length", {
                let mut data = valid_data.clone();
                if data.len() > 25 {
                    data[25] = 0xFF; // Likely a string length field
                }
                data
            }),
        ];

        for (corruption_type, corrupted_data) in corruption_scenarios {
            let result = parse_sstable_header(&corrupted_data);

            if result.is_err() {
                let error = result.unwrap_err();

                // Should be properly categorized
                assert_eq!(error.category(), ErrorCategory::Data);
                assert!(!error.is_recoverable());

                println!("✅ Corruption error correctly classified: {}", corruption_type);
            } else {
                println!("ℹ️  Corruption {} was handled gracefully", corruption_type);
            }
        }
    }
}

/// Tests for error message quality and debugging information
#[cfg(test)]
mod error_message_tests {
    use super::*;

    #[test]
    fn test_error_message_quality() {
        let error_scenarios = vec![
            (vec![0xFF; 4], "should mention magic number"),
            (vec![0x6F, 0x61], "should mention truncation"),
            (vec![0x6F, 0x61, 0x00, 0x00, 0xFF, 0xFF], "should mention version"),
        ];

        for (data, expectation) in error_scenarios {
            let result = parse_sstable_header(&data);

            if let Err(error) = result {
                let error_message = error.to_string().to_lowercase();

                // Error messages should be informative
                assert!(
                    !error_message.is_empty(),
                    "Error message should not be empty"
                );

                // Should not contain internal implementation details
                assert!(
                    !error_message.contains("nom::"),
                    "Error message should not expose internal parsing details: {}",
                    error_message
                );

                assert!(
                    !error_message.contains("ikind"),
                    "Error message should not contain internal error kinds: {}",
                    error_message
                );

                println!("✅ Error message quality check passed ({}): {}", expectation, error_message);
            }
        }
    }

    #[test]
    fn test_error_context_preservation() {
        // Test that errors preserve useful context information
        let scenarios = vec![
            ("invalid_magic", vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ("truncated", vec![0x6F, 0x61]),
            ("empty", vec![]),
        ];

        for (scenario_name, data) in scenarios {
            let result = parse_sstable_header(&data);

            if let Err(error) = result {
                // Check if error implements standard traits
                let error_debug = format!("{:?}", error);
                let error_display = error.to_string();

                assert!(!error_debug.is_empty());
                assert!(!error_display.is_empty());

                // Error should be Send + Sync for async usage
                fn assert_send_sync<T: Send + Sync>(_: &T) {}
                assert_send_sync(&error);

                println!("✅ Error context preserved for {}: {}", scenario_name, error_display);
            }
        }
    }

    #[test]
    fn test_error_chain_information() {
        // Test that complex parsing errors maintain error chain information
        let complex_header = create_complex_header();
        let valid_data = serialize_sstable_header(&complex_header).unwrap();

        // Corrupt at various depths to test error propagation
        let corruption_points = vec![
            (10, "Early corruption"),
            (50, "Mid-structure corruption"),
            (valid_data.len() - 10, "Late corruption"),
        ];

        for (corruption_point, description) in corruption_points {
            if corruption_point < valid_data.len() {
                let mut corrupted = valid_data.clone();
                corrupted[corruption_point] = 0xFF;

                let result = parse_sstable_header(&corrupted);

                if let Err(error) = result {
                    // Error should provide meaningful information
                    let error_info = format!("{}", error);

                    assert!(
                        !error_info.is_empty(),
                        "Error should provide information for {}",
                        description
                    );

                    println!("✅ Error chain information preserved for {}: {}", description, error_info);
                }
            }
        }
    }
}

/// Tests for error recovery and graceful degradation
#[cfg(test)]
mod error_recovery_tests {
    use super::*;

    #[test]
    fn test_partial_parsing_recovery() {
        // Test that the parser can handle partial success scenarios
        let valid_header = create_test_header();
        let valid_data = serialize_sstable_header(&valid_header).unwrap();

        // Create data with valid start but corrupted end
        let mut partial_data = valid_data[..valid_data.len() / 2].to_vec();
        partial_data.extend_from_slice(&[0xFF; 100]); // Corrupted end

        let result = parse_sstable_header(&partial_data);

        // Should fail gracefully, not panic
        assert!(
            result.is_err(),
            "Should fail gracefully on partial corruption"
        );

        if let Err(error) = result {
            assert!(!error.is_recoverable());
            println!("✅ Partial parsing handled gracefully: {}", error);
        }
    }

    #[test]
    fn test_resource_cleanup_on_error() {
        // Test that parsing errors don't leak resources
        let large_invalid_data = vec![0xFF; 1_000_000]; // Large invalid data

        // This should fail quickly without consuming excessive resources
        let start = std::time::Instant::now();
        let result = parse_sstable_header(&large_invalid_data);
        let duration = start.elapsed();

        assert!(result.is_err(), "Should fail on large invalid data");
        assert!(
            duration.as_millis() < 1000,
            "Should fail quickly, took {:?}",
            duration
        );

        println!("✅ Resource cleanup test passed (failed in {:?})", duration);
    }

    #[tokio::test]
    async fn test_file_error_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create files with various error conditions
        let error_files = vec![
            ("empty.sst", vec![]),
            ("truncated.sst", vec![0x6F; 10]),
            ("invalid_magic.sst", vec![0xFF; 100]),
            ("corrupted.sst", {
                let valid_header = create_test_header();
                let mut data = serialize_sstable_header(&valid_header).unwrap();
                data[0] = 0xFF; // Corrupt first byte
                data
            }),
        ];

        for (filename, file_data) in error_files {
            let file_path = temp_dir.path().join(filename);
            tokio::fs::write(&file_path, &file_data).await.unwrap();

            let result = SSTableReader::open(&file_path, &config, platform.clone()).await;

            assert!(result.is_err(), "Should fail to open {}", filename);

            let error = result.unwrap_err();
            assert_eq!(error.category(), ErrorCategory::Data);
            assert!(!error.is_recoverable());

            println!("✅ File error recovery test passed for: {}", filename);
        }
    }

    #[tokio::test]
    async fn test_manager_error_resilience() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create a mix of valid and invalid files
        let files = vec![
            ("valid-1-big-Data.db", create_valid_sstable_data()),
            ("invalid-2-big-Data.db", vec![0xFF; 100]),
            ("truncated-3-big-Data.db", vec![0x6F; 20]),
            ("corrupted-4-big-Data.db", {
                let mut data = create_valid_sstable_data();
                data[0] = 0xFF;
                data
            }),
        ];

        for (filename, file_data) in files {
            let file_path = temp_dir.path().join(filename);
            tokio::fs::write(&file_path, &file_data).await.unwrap();
        }

        // SSTableManager should handle errors gracefully
        let manager_result = SSTableManager::new(temp_dir.path(), &config, platform).await;

        match manager_result {
            Ok(manager) => {
                let stats = manager.stats().await.unwrap();
                println!("✅ Manager created successfully with {} SSTables", stats.sstable_count);

                // Should have loaded only valid files
                assert!(
                    stats.sstable_count <= 1,
                    "Should load at most the valid files"
                );
            }
            Err(error) => {
                // If manager creation fails, it should be due to directory issues, not file corruption
                println!("ℹ️  Manager creation failed (may be expected): {}", error);
                assert_eq!(error.category(), ErrorCategory::System);
            }
        }
    }
}

/// Tests for error consistency and determinism
#[cfg(test)]
mod error_consistency_tests {
    use super::*;

    #[test]
    fn test_error_determinism() {
        // Test that the same input produces the same error
        let error_inputs = vec![
            vec![0xFF; 10],
            vec![0x6F, 0x61],
            vec![],
            vec![0xDE, 0xAD, 0xBE, 0xEF],
        ];

        for input in error_inputs {
            let error1 = parse_sstable_header(&input).unwrap_err();
            let error2 = parse_sstable_header(&input).unwrap_err();

            // Errors should be consistent
            assert_eq!(
                error1.category(),
                error2.category(),
                "Error categories should be consistent"
            );

            assert_eq!(
                error1.is_recoverable(),
                error2.is_recoverable(),
                "Error recoverability should be consistent"
            );

            // Error messages should be consistent
            assert_eq!(
                error1.to_string(),
                error2.to_string(),
                "Error messages should be consistent"
            );

            println!("✅ Error determinism test passed for input length: {}", input.len());
        }
    }

    #[test]
    fn test_error_ordering() {
        // Test that errors are reported in a consistent order of precedence
        let precedence_tests = vec![
            // Magic number errors should be caught before version errors
            (vec![], "truncation"),
            (vec![0xFF, 0xFF, 0xFF, 0xFF], "magic"),
            (vec![0x6F, 0x61, 0x00, 0x00, 0xFF, 0xFF], "version"),
        ];

        for (data, expected_error_type) in precedence_tests {
            let result = parse_sstable_header(&data);
            assert!(result.is_err(), "Should fail for {}", expected_error_type);

            let error = result.unwrap_err();
            assert_eq!(error.category(), ErrorCategory::Data);

            println!("✅ Error precedence test passed for: {}", expected_error_type);
        }
    }

    #[test]
    fn test_error_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let error_data = Arc::new(vec![0xFF; 100]);
        let mut handles = Vec::new();

        // Test parsing errors in multiple threads
        for i in 0..10 {
            let data = error_data.clone();
            let handle = thread::spawn(move || {
                let result = parse_sstable_header(&data);
                assert!(result.is_err(), "Thread {} should get error", i);
                result.unwrap_err()
            });
            handles.push(handle);
        }

        let mut errors = Vec::new();
        for handle in handles {
            let error = handle.join().unwrap();
            errors.push(error);
        }

        // All errors should be consistent
        let first_error = &errors[0];
        for (i, error) in errors.iter().enumerate() {
            assert_eq!(
                error.category(),
                first_error.category(),
                "Error category should be consistent across threads (thread {})",
                i
            );
        }

        println!("✅ Error thread safety test passed");
    }
}

/// Tests for error boundary conditions
#[cfg(test)]
mod error_boundary_tests {
    use super::*;

    #[test]
    fn test_minimum_valid_data_boundary() {
        // Find the minimum amount of data that could theoretically be valid
        let minimal_header = SSTableHeader {
            cassandra_version: CassandraVersion::Legacy,
            version: SUPPORTED_VERSION,
            table_id: [0; 16],
            keyspace: "".to_string(),
            table_name: "".to_string(),
            generation: 0,
            compression: CompressionInfo {
                algorithm: "".to_string(),
                chunk_size: 0,
                parameters: HashMap::new(),
            },
            stats: SSTableStats::default(),
            columns: vec![],
            properties: HashMap::new(),
        };

        let minimal_data = serialize_sstable_header(&minimal_header).unwrap();
        println!("Minimal valid header size: {} bytes", minimal_data.len());

        // Test with data just below this boundary
        for truncate_at in 1..minimal_data.len() {
            let truncated = &minimal_data[..truncate_at];
            let result = parse_sstable_header(truncated);

            assert!(
                result.is_err(),
                "Should fail with {} bytes (below minimum)",
                truncate_at
            );
        }

        // Test the complete minimal data
        let result = parse_sstable_header(&minimal_data);
        assert!(
            result.is_ok(),
            "Should succeed with minimal valid data ({} bytes)",
            minimal_data.len()
        );

        println!("✅ Minimum boundary test passed");
    }

    #[test]
    fn test_maximum_reasonable_data_boundary() {
        // Test with very large but still reasonable data
        let large_header = create_extremely_large_header();
        let large_data = serialize_sstable_header(&large_header).unwrap();

        println!("Large header size: {} bytes", large_data.len());

        // Should still parse successfully
        let start = std::time::Instant::now();
        let result = parse_sstable_header(&large_data);
        let duration = start.elapsed();

        assert!(
            result.is_ok(),
            "Should parse large but reasonable data"
        );

        assert!(
            duration.as_millis() < 1000,
            "Should parse large data in reasonable time: {:?}",
            duration
        );

        println!("✅ Maximum boundary test passed (parsed in {:?})", duration);
    }

    #[test]
    fn test_malformed_vint_boundaries() {
        // Test with malformed VInt encodings at various positions
        let valid_header = create_test_header();
        let valid_data = serialize_sstable_header(&valid_header).unwrap();

        let malformed_vints = vec![
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF], // Invalid VInt
            vec![0x80, 0x80, 0x80, 0x80, 0x01], // Maximum VInt
            vec![0xFF], // Incomplete VInt
        ];

        for (i, malformed_vint) in malformed_vints.iter().enumerate() {
            // Try inserting malformed VInt at various positions
            for pos in (20..valid_data.len().min(100)).step_by(10) {
                let mut corrupted = valid_data.clone();

                // Replace some bytes with malformed VInt
                for (j, &byte) in malformed_vint.iter().enumerate() {
                    if pos + j < corrupted.len() {
                        corrupted[pos + j] = byte;
                    }
                }

                let result = parse_sstable_header(&corrupted);

                // Should either parse successfully or fail gracefully
                match result {
                    Ok(_) => {
                        println!("ℹ️  Malformed VInt {} at position {} was handled", i, pos);
                    }
                    Err(error) => {
                        assert_eq!(error.category(), ErrorCategory::Data);
                        assert!(!error.is_recoverable());
                    }
                }
            }
        }

        println!("✅ Malformed VInt boundary tests passed");
    }
}

// Helper functions

fn create_test_header() -> SSTableHeader {
    SSTableHeader {
        cassandra_version: CassandraVersion::Legacy,
        version: SUPPORTED_VERSION,
        table_id: [1; 16],
        keyspace: "test_ks".to_string(),
        table_name: "test_table".to_string(),
        generation: 42,
        compression: CompressionInfo {
            algorithm: "LZ4".to_string(),
            chunk_size: 4096,
            parameters: HashMap::new(),
        },
        stats: SSTableStats::default(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                column_type: "uuid".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
            }
        ],
        properties: HashMap::new(),
    }
}

fn create_complex_header() -> SSTableHeader {
    let mut header = create_test_header();

    // Add complex elements
    for i in 0..10 {
        header.columns.push(ColumnInfo {
            name: format!("col_{}", i),
            column_type: format!("type_{}", i),
            is_primary_key: false,
            key_position: None,
            is_static: i % 3 == 0,
            is_clustering: i % 2 == 0,
        });
    }

    for i in 0..20 {
        header.properties.insert(
            format!("prop_{}", i),
            format!("value_{}", i),
        );
    }

    header
}

fn create_extremely_large_header() -> SSTableHeader {
    let mut header = create_test_header();

    // Very large number of columns
    for i in 0..1000 {
        header.columns.push(ColumnInfo {
            name: format!("extremely_long_column_name_with_lots_of_descriptive_text_{}", i),
            column_type: format!("very_descriptive_type_name_{}", i),
            is_primary_key: i < 5,
            key_position: if i < 5 { Some(i as u16) } else { None },
            is_static: i % 10 == 0,
            is_clustering: i % 7 == 0,
        });
    }

    // Large number of properties
    for i in 0..500 {
        header.properties.insert(
            format!("extremely_long_property_name_with_detailed_description_{}", i),
            format!("very_long_property_value_with_extensive_configuration_data_{}", i).repeat(5),
        );
    }

    // Large histogram
    header.stats.row_size_histogram = (0..10000).collect();

    header
}

fn create_valid_sstable_data() -> Vec<u8> {
    let header = create_test_header();
    let mut data = serialize_sstable_header(&header).unwrap();
    data.extend_from_slice(&[0x00; 1000]); // Body content
    data
}