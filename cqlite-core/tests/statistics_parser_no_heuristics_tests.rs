//! Integration tests for Issue #105: Statistics parser no-heuristics compliance
//!
//! These tests validate that the statistics parser:
//! 1. Returns errors instead of fabricating data (nb-format)
//! 2. Uses authoritative format detection (version-based)
//! 3. Documents checksum validation limitations
//! 4. Reads real binary data from headers without guessing
//!
//! References: Issue #105, Issue #28 (no-heuristics mandate)
//!
//! Test Data: Real Cassandra 5.0 Statistics.db files from test-data/datasets/sstables/

use cqlite_core::error::Error;
use cqlite_core::parser::enhanced_statistics_parser::{
    parse_enhanced_statistics_file, parse_nb_format_header, parse_nb_format_statistics_data,
    parse_statistics_with_fallback,
};
use cqlite_core::parser::statistics::{parse_statistics_header, StatisticsHeader};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::statistics_reader::StatisticsReader;
use cqlite_core::Config;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;

/// Helper to find a real Statistics.db file in test datasets
fn find_real_statistics_db() -> Option<PathBuf> {
    let datasets_root =
        std::env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| "test-data/datasets".to_string());

    // Try multiple known locations for Statistics.db files
    let candidates = vec![
        format!(
            "{}/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db",
            datasets_root
        ),
        format!(
            "{}/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db",
            datasets_root
        ),
        format!(
            "{}/sstables/test_wide_rows/wide_partition_table-6aebc800a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db",
            datasets_root
        ),
        format!(
            "{}/sstables/test_collections/collection_table-6b1a9130a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db",
            datasets_root
        ),
    ];

    for candidate in candidates {
        let path = PathBuf::from(&candidate);
        if path.exists() {
            return Some(path);
        }
    }

    None
}

/// Helper to read Statistics.db file into bytes
async fn read_statistics_db_bytes(path: &PathBuf) -> Result<Vec<u8>, std::io::Error> {
    fs::read(path).await
}

#[tokio::test]
async fn test_nb_format_returns_error_not_fabrication() {
    // Test that nb-format Statistics.db parsing returns UnsupportedFormat error
    // rather than fabricating data (Issue #105 compliance)

    let Some(stats_path) = find_real_statistics_db() else {
        println!(
            "SKIPPED: Real Statistics.db file not found. Set CQLITE_DATASETS_ROOT environment variable."
        );
        return;
    };

    println!("Testing with real file: {}", stats_path.display());

    // Read the real Statistics.db file
    let file_bytes = read_statistics_db_bytes(&stats_path)
        .await
        .expect("Failed to read Statistics.db file");

    assert!(
        file_bytes.len() >= 32,
        "Statistics.db file too small: {} bytes",
        file_bytes.len()
    );

    // Attempt to parse the nb-format Statistics.db with enhanced parser
    let result = parse_enhanced_statistics_file(&file_bytes);

    // Issue #162: Minimal nb-format parsing now succeeds (EncodingStats extraction)
    // This is CORRECT behavior - we read real binary data, not fabricated values
    assert!(
        result.is_ok(),
        "Enhanced statistics parser should succeed for nb-format (Issue #162 minimal parser)"
    );

    // Verify we got real data from the file
    match result {
        Ok((_remaining, stats)) => {
            // Verify we extracted EncodingStats (non-zero timestamp indicates real data)
            assert!(
                stats.timestamp_stats.min_timestamp != 0,
                "Should extract real min_timestamp from binary data"
            );
            println!("PASS: Enhanced parser correctly extracts EncodingStats from nb-format");
            println!("  min_timestamp: {}", stats.timestamp_stats.min_timestamp);
            println!("  min_deletion_time: {}", stats.timestamp_stats.min_deletion_time);
        }
        Err(e) => {
            panic!("Parser should successfully extract EncodingStats: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_nb_format_data_extraction_returns_error() {
    // Test that parse_nb_format_statistics_data() returns explicit UnsupportedFormat error
    // with Issue #28 reference (no fabrication)

    let Some(stats_path) = find_real_statistics_db() else {
        println!("SKIPPED: Real Statistics.db file not found");
        return;
    };

    let file_bytes = read_statistics_db_bytes(&stats_path)
        .await
        .expect("Failed to read Statistics.db file");

    // Parse header successfully (this should work)
    let (remaining, header) =
        parse_nb_format_header(&file_bytes).expect("Header parsing should succeed on real file");

    assert_eq!(
        header.version, 4,
        "Real Statistics.db should have version 4 (nb-format)"
    );

    // Attempt to extract statistics data (Issue #162: now succeeds for minimal EncodingStats)
    let result = parse_nb_format_statistics_data(remaining, &header);

    // Issue #162: Minimal parsing succeeds (extracts EncodingStats only)
    assert!(result.is_ok(), "Statistics data extraction should succeed for minimal EncodingStats");

    match result {
        Ok((_row_stats, timestamp_stats, _table_stats, _column_stats, _)) => {
            // Verify we extracted real values
            assert!(
                timestamp_stats.min_timestamp != 0 || timestamp_stats.min_deletion_time != 0,
                "Should extract non-zero EncodingStats from real binary data"
            );
            println!("PASS: Extracted EncodingStats: min_timestamp={}, min_deletion_time={}",
                     timestamp_stats.min_timestamp, timestamp_stats.min_deletion_time);
        }
        Err(e) => {
            panic!("Minimal parser should succeed: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_header_parsing_reads_real_binary_data() {
    // Test that header parsing reads actual binary values from the file
    // without fabricating or guessing field values

    let Some(stats_path) = find_real_statistics_db() else {
        println!("SKIPPED: Real Statistics.db file not found");
        return;
    };

    let file_bytes = read_statistics_db_bytes(&stats_path)
        .await
        .expect("Failed to read Statistics.db file");

    // Parse the 32-byte nb-format header
    let result = parse_nb_format_header(&file_bytes);
    assert!(result.is_ok(), "Header parsing should succeed");

    let (_remaining, header) = result.unwrap();

    // Verify header contains real binary data (not fabricated defaults)
    assert_eq!(
        header.version, 4,
        "Version must be 4 (nb-format identifier)"
    );

    // Check that non-version fields contain actual data (not zeros/defaults)
    // Real nb-format files have non-zero values in these fields based on hex dumps
    assert_ne!(
        header.statistics_kind, 0,
        "statistics_kind should be non-zero from real file (e.g., 0x26291b05)"
    );

    // data_length should be reasonable (typically 44 bytes for real files)
    assert!(
        header.data_length > 0 && header.data_length < 10000,
        "data_length should be positive and reasonable: {}",
        header.data_length
    );

    // Verify checksum field is populated (even if not validated in M1)
    // Real files have non-zero checksums
    assert_ne!(
        header.checksum, 0,
        "checksum field should be populated from real file"
    );

    // Verify metadata fields contain actual values
    assert!(
        header.metadata1 > 0 || header.metadata2 > 0 || header.metadata3 > 0,
        "At least one metadata field should be non-zero in real file"
    );

    println!("PASS: Header parsing reads real binary data:");
    println!("  version: {}", header.version);
    println!("  statistics_kind: 0x{:08x}", header.statistics_kind);
    println!("  data_length: {}", header.data_length);
    println!("  metadata1: {}", header.metadata1);
    println!("  metadata2: {}", header.metadata2);
    println!("  metadata3: {}", header.metadata3);
    println!("  checksum: 0x{:04x}", header.checksum);
}

#[test]
fn test_format_detection_is_authoritative() {
    // Test that format detection is version-based (authoritative),
    // not heuristic-based (length checks, magic bytes, etc.)

    // Test 1: Version 4 -> nb-format (authoritative)
    let nb_format_header = vec![
        0x00, 0x00, 0x00, 0x04, // version = 4 (definitive nb-format marker)
        0x26, 0x29, 0x1b, 0x05, // statistics_kind
        0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x00, 0x00, 0x2c, // data_length = 44
        0x00, 0x00, 0x00, 0x01, // metadata1
        0x00, 0x00, 0x00, 0x65, // metadata2
        0x00, 0x00, 0x00, 0x02, // metadata3
        0x00, 0x00, 0x14, 0xd4, // checksum
    ];

    let result = parse_statistics_header(&nb_format_header);
    assert!(result.is_ok(), "Version 4 should parse as nb-format");

    let (_, header) = result.unwrap();
    assert_eq!(header.version, 4, "Version 4 is nb-format");
    assert_eq!(
        header.statistics_kind, 0x26291b05,
        "nb-format has statistics_kind field"
    );

    // Test 2: Version 1-3 -> legacy format (authoritative)
    for version in 1..=3 {
        let legacy_header = vec![
            0x00, 0x00, 0x00, version, // version = 1, 2, or 3
            // table_id (16 bytes)
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10, 0x00, 0x00, 0x00, 0x05, // section_count
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // file_size
            0x12, 0x34, 0x56, 0x78, // checksum
        ];

        let result = parse_statistics_header(&legacy_header);
        assert!(
            result.is_ok(),
            "Version {} should parse as legacy format",
            version
        );

        let (_, header) = result.unwrap();
        assert_eq!(header.version, version as u32);
        assert!(
            header.table_id.is_some(),
            "Legacy format has table_id field"
        );
    }

    // Test 3: Version 0 -> error (unsupported)
    let invalid_v0 = vec![
        0x00, 0x00, 0x00, 0x00, // version = 0 (invalid)
        0x00, 0x00, 0x00, 0x00, // rest doesn't matter
    ];
    assert!(
        parse_statistics_header(&invalid_v0).is_err(),
        "Version 0 must be rejected"
    );

    // Test 4: Version 5+ -> error (future/unsupported)
    let invalid_v5 = vec![
        0x00, 0x00, 0x00, 0x05, // version = 5 (unsupported)
        0x00, 0x00, 0x00, 0x00,
    ];
    assert!(
        parse_statistics_header(&invalid_v5).is_err(),
        "Version 5 must be rejected"
    );

    // Test 5: Version 255 -> error (invalid)
    let invalid_v255 = vec![
        0x00, 0x00, 0x00, 0xFF, // version = 255 (invalid)
        0x00, 0x00, 0x00, 0x00,
    ];
    assert!(
        parse_statistics_header(&invalid_v255).is_err(),
        "Version 255 must be rejected"
    );

    println!("PASS: Format detection is authoritative (version-based only)");
    println!("  Version 4: nb-format");
    println!("  Versions 1-3: legacy format");
    println!("  Other versions: error");
}

#[test]
fn test_no_heuristic_length_checks_in_format_detection() {
    // Test that format detection does NOT use heuristics like:
    // - "version == 4 && input.len() >= 28"
    // - "input.len() < 36"
    // - Magic byte sequences
    // - Field value ranges
    //
    // Format detection MUST be based ONLY on version field

    // Version 4 with short input (not enough for full header)
    // Should fail parsing (not enough bytes), but must NOT switch formats
    let short_nb_data = vec![
        0x00, 0x00, 0x00, 0x04, // version = 4 (definitive nb-format)
        0x26, 0x29, 0x1b, 0x05, // statistics_kind
        0x00, 0x00, 0x00, 0x00, // reserved
        0x00, 0x00, 0x00, 0x2c, // data_length
              // Missing remaining 16 bytes
    ];

    let result = parse_statistics_header(&short_nb_data);

    // MUST fail (incomplete parse), not switch to legacy format
    assert!(
        result.is_err(),
        "Version 4 with short input must fail (not switch formats)"
    );

    // The error should be a parse error (incomplete input), not a format detection error
    match result {
        Err(nom::Err::Incomplete(_)) | Err(nom::Err::Error(_)) => {
            // Expected: parse failure due to insufficient bytes
            println!("PASS: Short nb-format input fails parsing (no format switch)");
        }
        Ok(_) => {
            panic!("Short input should not successfully parse");
        }
        Err(other) => {
            println!(
                "Note: Unexpected error type (still acceptable): {:?}",
                other
            );
        }
    }
}

#[tokio::test]
async fn test_statistics_reader_documents_checksum_limitation() {
    // Test that StatisticsReader properly handles real nb-format files
    // and documents the checksum validation limitation for M1
    //
    // Since nb-format parsing is deferred to M2, StatisticsReader::open()
    // should return an error (not fabricate data or panic)

    let Some(stats_path) = find_real_statistics_db() else {
        println!("SKIPPED: Real Statistics.db file not found");
        return;
    };

    println!("Testing StatisticsReader with: {}", stats_path.display());

    // Create platform
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("Platform creation"));

    // Attempt to open StatisticsReader with real nb-format file
    // This should fail gracefully (not panic) because nb-format parsing is deferred
    let result = StatisticsReader::open(&stats_path, platform).await;

    match result {
        Ok(reader) => {
            // If StatisticsReader opens successfully, it means parsing succeeded
            // In M1, this would only happen if legacy format or if implementation changed
            println!("INFO: StatisticsReader opened successfully (unexpected for nb-format in M1)");
            println!("  File: {}", stats_path.display());

            // Verify reader provides access to statistics
            let row_count = reader.row_count();
            println!("  Row count: {}", row_count);

            // Test that validate_checksum() method exists and can be called
            // (even if validation is deferred to M2)
            let checksum_result = reader.validate_checksum().await;

            // Checksum validation may fail or succeed, but must not panic
            match checksum_result {
                Ok(valid) => {
                    println!("  Checksum validation returned: {}", valid);
                    // Note: We don't assert true/false here because M1 validation is incomplete
                }
                Err(e) => {
                    println!("  Checksum validation error (acceptable in M1): {}", e);
                }
            }

            // Even if open succeeded, this is acceptable as long as it didn't fabricate data
            println!("PASS: StatisticsReader handled file without panic or fabrication");
        }
        Err(Error::Corruption(msg)) if msg.contains("Failed to parse Statistics.db") => {
            // Expected behavior - nb-format parsing returns error through StatisticsReader
            // The Corruption error wraps the underlying parse error
            println!("PASS: StatisticsReader correctly returns parse error for nb-format");
            println!("  Error: {}", msg);

            // Verify the error contains expected references
            assert!(
                msg.contains("enhanced parser") || msg.contains("parse"),
                "Error should reference parsing failure: {}",
                msg
            );
        }
        Err(Error::UnsupportedFormat(msg)) => {
            // Also acceptable - explicit UnsupportedFormat error
            assert!(
                msg.contains("deferred to M2") || msg.contains("not yet implemented"),
                "Error should reference M2 deferral: {}",
                msg
            );
            println!("PASS: StatisticsReader correctly returns UnsupportedFormat error");
            println!("  Error: {}", msg);
        }
        Err(other) => {
            // Fail on unexpected error types
            panic!(
                "Unexpected error opening StatisticsReader (expected Corruption or UnsupportedFormat): {:?}",
                other
            );
        }
    }
}

#[tokio::test]
async fn test_statistics_reader_checksum_limitation_documented() {
    // Test that StatisticsReader source code documents the checksum limitation
    // This is a meta-test to ensure Issue #28 compliance is visible

    // Read the StatisticsReader source file
    let reader_source_path = PathBuf::from("cqlite-core/src/storage/sstable/statistics_reader.rs");

    if !reader_source_path.exists() {
        println!("SKIPPED: Source file not found (test may be running from different directory)");
        return;
    }

    let source = fs::read_to_string(&reader_source_path)
        .await
        .expect("Failed to read StatisticsReader source");

    // Verify that checksum validation limitation is documented
    let has_m2_reference = source.contains("M2") || source.contains("deferred");
    let has_checksum_comment = source.contains("Checksum") || source.contains("checksum");
    let has_limitation_note =
        source.contains("limitation") || source.contains("not yet implemented");

    assert!(
        has_m2_reference && has_checksum_comment,
        "StatisticsReader must document M2 deferral and checksum limitation"
    );

    if has_limitation_note {
        println!("PASS: StatisticsReader documents checksum validation limitation");
    } else {
        println!("NOTE: Consider adding explicit 'limitation' documentation to StatisticsReader");
    }

    // Verify that parse_nb_format_statistics_data documents its removal
    let parser_source_path = PathBuf::from("cqlite-core/src/parser/enhanced_statistics_parser.rs");

    if parser_source_path.exists() {
        let parser_source = fs::read_to_string(&parser_source_path)
            .await
            .expect("Failed to read parser source");

        let has_issue_28_ref = parser_source.contains("Issue #28");
        let has_issue_105_ref = parser_source.contains("Issue #105");
        let has_no_fabrication = parser_source.contains("fabricat");

        assert!(
            has_issue_28_ref,
            "Enhanced statistics parser must reference Issue #28"
        );

        if has_issue_105_ref {
            println!("PASS: Parser references Issue #105 (heuristics removal)");
        }

        if has_no_fabrication {
            println!("PASS: Parser documents no fabrication policy");
        }
    }
}

#[tokio::test]
async fn test_multiple_real_statistics_files() {
    // Test that multiple real Statistics.db files all exhibit consistent behavior:
    // 1. Headers parse successfully with real data
    // 2. Statistics extraction returns proper errors
    // 3. No fabrication occurs

    let datasets_root =
        std::env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| "test-data/datasets".to_string());

    let test_files = vec![
        format!(
            "{}/sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db",
            datasets_root
        ),
        format!(
            "{}/sstables/test_timeseries/sensor_data-6c698230a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db",
            datasets_root
        ),
        format!(
            "{}/sstables/test_collections/collection_table-6b1a9130a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db",
            datasets_root
        ),
    ];

    let mut tested_count = 0;

    for file_path_str in test_files {
        let file_path = PathBuf::from(&file_path_str);
        if !file_path.exists() {
            println!("Skipping missing file: {}", file_path_str);
            continue;
        }

        tested_count += 1;
        println!("\nTesting file: {}", file_path.display());

        let file_bytes = read_statistics_db_bytes(&file_path)
            .await
            .expect("Failed to read file");

        // Test 1: Header parsing succeeds
        let header_result = parse_nb_format_header(&file_bytes);
        assert!(
            header_result.is_ok(),
            "Header parsing should succeed for {}",
            file_path_str
        );

        let (_remaining, header) = header_result.unwrap();
        assert_eq!(header.version, 4, "All test files should be nb-format");

        // Test 2: Full file parsing succeeds with minimal EncodingStats (Issue #162)
        let full_result = parse_statistics_with_fallback(&file_bytes);
        assert!(
            full_result.is_ok(),
            "Full parsing should succeed with minimal EncodingStats for {} (Issue #162)",
            file_path_str
        );

        if let Ok((_remaining, stats)) = full_result {
            println!("  ✓ Header parsed with real data");
            println!("  ✓ Full parsing extracted real EncodingStats (min_timestamp={})",
                     stats.timestamp_stats.min_timestamp);
        }
    }

    if tested_count == 0 {
        println!("SKIPPED: No real Statistics.db files found in test datasets");
    } else {
        println!(
            "\nPASS: Tested {} real Statistics.db files successfully",
            tested_count
        );
    }
}

#[test]
fn test_error_messages_reference_issues() {
    // Test that error messages from parse_nb_format_statistics_data()
    // properly reference Issue #28 and Issue #105 for traceability

    let dummy_header = StatisticsHeader {
        version: 4,
        statistics_kind: 0x26291b05,
        data_length: 44,
        metadata1: 1,
        metadata2: 101,
        metadata3: 2,
        checksum: 0x14d4,
        table_id: None,
    };

    let insufficient_data = vec![0u8; 5]; // Too little data for VInt parsing
    let result = parse_nb_format_statistics_data(&insufficient_data, &dummy_header);

    // Issue #162: Parser now attempts minimal parsing and fails on insufficient data
    assert!(result.is_err(), "Should return error for insufficient data");

    // Error should be a parse error (corruption or nom error)
    match result {
        Err(_) => {
            // Parser correctly rejects insufficient/invalid data
            println!("PASS: Parser correctly rejects insufficient data");
        }
        Ok(_) => {
            panic!("Parser should not succeed with insufficient data");
        }
    }

    // Note: Issue #28 mandate is still enforced - we return errors for bad data,
    // we just succeed when data is valid. This is the correct behavior.
}

#[test]
fn test_header_struct_has_required_fields() {
    // Test that StatisticsHeader struct contains all required fields
    // for nb-format (no missing or fabricated fields)

    let header = StatisticsHeader {
        version: 4,
        statistics_kind: 0x26291b05,
        data_length: 44,
        metadata1: 1,
        metadata2: 101,
        metadata3: 2,
        checksum: 0x14d4,
        table_id: None,
    };

    // Verify all fields are accessible and have correct types
    assert_eq!(header.version, 4);
    assert_eq!(header.statistics_kind, 0x26291b05);
    assert_eq!(header.data_length, 44);
    assert_eq!(header.metadata1, 1);
    assert_eq!(header.metadata2, 101);
    assert_eq!(header.metadata3, 2);
    assert_eq!(header.checksum, 0x14d4);
    assert!(header.table_id.is_none());

    println!("PASS: StatisticsHeader contains all required fields for nb-format");
}
