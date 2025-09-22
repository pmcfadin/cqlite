use cqlite_core::parser::header::{CassandraVersion, SUPPORTED_MAGIC_NUMBERS, SUPPORTED_VERSION};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::reader::SSTableReader;
/// Comprehensive tests for the strict header parsing fix
///
/// These tests verify that the SSTable reader properly rejects corrupted
/// headers and unsupported formats while maintaining compatibility with
/// valid legacy formats when the feature is enabled.
use cqlite_core::{Config, Error};
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;

/// Test that corrupted headers are properly rejected
#[tokio::test]
async fn test_corrupted_header_rejection() {
    // Create a file with corrupted header (too small)
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(&[0x12, 0x34])
        .expect("Failed to write corrupted header");

    // Attempt to read the corrupted file
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );
    let result = SSTableReader::open(file.path(), &config, platform).await;

    // Should return a corruption error
    assert!(result.is_err());
    let error = result.unwrap_err();
    match error {
        Error::Corruption(msg) => {
            assert!(msg.contains("Header buffer too small"));
            assert!(msg.contains("minimum 8 bytes required"));
        }
        _ => panic!("Expected corruption error, got: {:?}", error),
    }
}

/// Test that unsupported magic numbers are properly rejected
#[tokio::test]
async fn test_unsupported_magic_number_rejection() {
    // Create a file with unsupported magic number
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    let invalid_magic = 0xDEADBEEFu32;
    let mut header = Vec::new();
    header.extend_from_slice(&invalid_magic.to_be_bytes());
    header.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
    header.extend_from_slice(&[0u8; 100]); // Pad to ensure sufficient size
    file.write_all(&header).expect("Failed to write header");

    // Attempt to read the file with unsupported magic
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );
    let result = SSTableReader::open(file.path(), &config, platform).await;

    // Should return a corruption error containing the unsupported format message
    assert!(result.is_err());
    let error = result.unwrap_err();
    match error {
        Error::Corruption(msg) => {
            assert!(msg.contains("0xdeadbeef"));
            assert!(msg.contains("not recognized"));
            assert!(msg.contains("Supported formats"));
            assert!(msg.contains("Failed to parse SSTable header"));
        }
        _ => panic!(
            "Expected corruption error wrapping unsupported format, got: {:?}",
            error
        ),
    }
}

/// Test that valid magic numbers are recognized
#[tokio::test]
async fn test_valid_magic_number_recognition() {
    for &magic in SUPPORTED_MAGIC_NUMBERS {
        // Create a file with valid magic number but minimal header
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        let mut header = Vec::new();
        header.extend_from_slice(&magic.to_be_bytes());
        header.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
        header.extend_from_slice(&[0u8; 100]); // Pad to ensure sufficient size
        file.write_all(&header).expect("Failed to write header");

        // The reader should at least recognize the magic number
        // (parsing may still fail due to incomplete header structure)
        let config = Config::default();
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );
        let result = SSTableReader::open(file.path(), &config, platform).await;

        // If it fails, it should not be due to unsupported magic number
        if let Err(error) = result {
            match error {
                Error::UnsupportedFormat(msg) if msg.contains("not recognized") => {
                    panic!(
                        "Valid magic number 0x{:08x} was rejected as unsupported",
                        magic
                    );
                }
                Error::Corruption(_) => {
                    // Expected - minimal header structure may not be complete
                    // This is the proper behavior for incomplete but valid format headers
                }
                _ => {
                    // Other errors may be acceptable depending on header completeness
                    println!("Magic 0x{:08x} resulted in: {:?}", magic, error);
                }
            }
        }
    }
}

/// Test legacy format handling with feature gate
#[cfg(feature = "legacy-heuristics")]
#[tokio::test]
async fn test_legacy_format_with_feature_enabled() {
    // Create a file with legacy magic number
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    let legacy_magic = CassandraVersion::Legacy.magic_number();
    let mut header = Vec::new();
    header.extend_from_slice(&legacy_magic.to_be_bytes());
    header.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
    header.extend_from_slice(&[0u8; 100]); // Pad to ensure sufficient size
    file.write_all(&header).expect("Failed to write header");

    // With legacy-heuristics feature enabled, this might succeed with minimal header
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );
    let result = SSTableReader::open(file.path(), &config, platform).await;

    // Check the result - should either succeed or fail with appropriate error
    match result {
        Ok(_reader) => {
            // Success is acceptable with legacy-heuristics enabled
            println!("Legacy format successfully parsed with minimal header");
        }
        Err(Error::Corruption(_)) => {
            // Corruption error is acceptable if header structure is incomplete
            println!("Legacy format failed with corruption error (expected for minimal header)");
        }
        Err(error) => {
            panic!("Unexpected error for legacy format: {:?}", error);
        }
    }
}

/// Test legacy format handling without feature gate
#[cfg(not(feature = "legacy-heuristics"))]
#[tokio::test]
async fn test_legacy_format_without_feature_disabled() {
    // Create a file with legacy magic number but malformed header structure
    // that will fail main parsing and trigger the feature gate check
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    let legacy_magic = CassandraVersion::Legacy.magic_number();
    let mut header = Vec::new();
    header.extend_from_slice(&legacy_magic.to_be_bytes());
    header.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
    // Add some bytes that look like a table_id but then invalid data that will cause parsing to fail
    header.extend_from_slice(&[0u8; 16]); // table_id
    header.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // Invalid keyspace length - will cause parsing failure
    file.write_all(&header)
        .expect("Failed to write malformed legacy header");

    // Without legacy-heuristics feature, should fail when main parsing fails
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );
    let result = SSTableReader::open(file.path(), &config, platform).await;

    assert!(result.is_err());
    let error = result.unwrap_err();
    match error {
        Error::Corruption(msg) => {
            // Should fail with corruption error since main parsing fails and no fallback is allowed
            assert!(
                msg.contains("Failed to parse header")
                    || msg.contains("legacy-heuristics feature is disabled")
                    || msg.contains("Failed to parse header for modern format")
            );
        }
        _ => panic!("Expected corruption error, got: {:?}", error),
    }
}

/// Test error message quality and debugging information
#[tokio::test]
async fn test_error_message_quality() {
    // Test with corrupted header
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(&[0x12, 0x34, 0x56])
        .expect("Failed to write corrupted header");

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );
    let result = SSTableReader::open(file.path(), &config, platform).await;
    assert!(result.is_err());

    let error_message = result.unwrap_err().to_string();

    // Check that error message contains helpful debugging information
    assert!(error_message.contains("Header buffer too small"));
    assert!(error_message.contains("3 bytes"));
    assert!(error_message.contains("minimum 8 bytes required"));
    assert!(error_message.contains(&format!("{}", file.path().display())));
}

/// Test that modern format parsing failures are handled correctly
#[tokio::test]
async fn test_modern_format_parsing_failure() {
    // Create a file with valid modern magic but incomplete structure
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    let modern_magic = CassandraVersion::V5_0NewBig.magic_number();
    let mut header = Vec::new();
    header.extend_from_slice(&modern_magic.to_be_bytes());
    header.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
    header.extend_from_slice(&[0u8; 50]); // Insufficient data for modern format
    file.write_all(&header).expect("Failed to write header");

    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Failed to create platform"),
    );
    let result = SSTableReader::open(file.path(), &config, platform).await;

    // Should fail with corruption error (not fallback to legacy)
    assert!(result.is_err());
    let error = result.unwrap_err();
    match error {
        Error::Corruption(msg) => {
            assert!(msg.contains("modern format"));
            assert!(msg.contains("V5_0NewBig"));
        }
        _ => panic!(
            "Expected corruption error for modern format, got: {:?}",
            error
        ),
    }
}

/// Integration test for the complete header parsing flow
#[tokio::test]
async fn test_header_parsing_integration() {
    struct TestCase {
        name: &'static str,
        magic: u32,
        additional_data: Vec<u8>,
        expected_result_type: &'static str,
    }

    let test_cases = vec![
        TestCase {
            name: "corrupted_too_small",
            magic: 0x0000,
            additional_data: vec![],
            expected_result_type: "corruption",
        },
        TestCase {
            name: "unsupported_magic",
            magic: 0x12345678,
            additional_data: vec![0; 100],
            expected_result_type: "unsupported",
        },
        TestCase {
            name: "legacy_magic",
            magic: CassandraVersion::Legacy.magic_number(),
            additional_data: vec![0; 100],
            expected_result_type: "varies", // Depends on feature flag
        },
        TestCase {
            name: "modern_magic_insufficient_data",
            magic: CassandraVersion::V5_0NewBig.magic_number(),
            additional_data: vec![0; 50],
            expected_result_type: "corruption",
        },
    ];

    for test_case in test_cases {
        println!("Running test case: {}", test_case.name);

        let mut file = NamedTempFile::new().expect(&format!(
            "Failed to create temp file for {}",
            test_case.name
        ));

        if test_case.name != "corrupted_too_small" {
            let mut header = Vec::new();
            header.extend_from_slice(&test_case.magic.to_be_bytes());
            header.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
            header.extend_from_slice(&test_case.additional_data);
            file.write_all(&header)
                .expect(&format!("Failed to write header for {}", test_case.name));
        } else {
            file.write_all(&[0x12, 0x34])
                .expect("Failed to write corrupted header");
        }

        let config = Config::default();
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );
        let result = SSTableReader::open(file.path(), &config, platform).await;

        match test_case.expected_result_type {
            "corruption" => {
                assert!(result.is_err(), "Test case {} should fail", test_case.name);
                assert!(
                    matches!(result.unwrap_err(), Error::Corruption(_)),
                    "Test case {} should return corruption error",
                    test_case.name
                );
            }
            "unsupported" => {
                assert!(result.is_err(), "Test case {} should fail", test_case.name);
                let error = result.unwrap_err();
                assert!(
                    matches!(error, Error::Corruption(_)),
                    "Test case {} should return corruption error (wrapping unsupported format), got: {:?}",
                    test_case.name,
                    error
                );
                // Check that the error message indicates unsupported format
                let error_msg = error.to_string();
                assert!(
                    error_msg.contains("not recognized") || error_msg.contains("unsupported"),
                    "Error message should indicate unsupported format: {}",
                    error_msg
                );
            }
            "varies" => {
                // Result depends on feature flag and header completeness
                if result.is_err() {
                    let error = result.unwrap_err();
                    assert!(
                        matches!(error, Error::Corruption(_))
                            || matches!(error, Error::UnsupportedFormat(_)),
                        "Test case {} returned unexpected error: {:?}",
                        test_case.name,
                        error
                    );
                }
            }
            _ => panic!(
                "Unknown expected result type: {}",
                test_case.expected_result_type
            ),
        }
    }
}
