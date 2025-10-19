//! Comprehensive CRC validation test for Issue #34
//!
//! This test implements the complete requirements from Issue #34:
//! - Validates CRC enforcement across all compression algorithms
//! - Tests matrix of 4 algorithms × 3 chunk sizes
//! - Ensures deterministic error reporting for CRC corruption
//! - Removes all decompression guessing paths

use cqlite_core::parser::header::CassandraVersion;
use cqlite_core::storage::sstable::chunk_decompressor::ChunkDecompressor;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use cqlite_core::Result;
use std::io::Cursor;

/// Test the complete compression matrix as specified in Issue #34
#[test]
fn test_complete_compression_matrix() {
    let algorithms = vec![
        "LZ4Compressor",
        "SnappyCompressor",
        "ZstdCompressor",
        "DeflateCompressor",
    ];
    let chunk_sizes = vec![16384, 65536, 131072]; // 16KB, 64KB, 128KB

    let mut total_tests = 0;
    let mut passed_tests = 0;

    for algorithm in &algorithms {
        for &chunk_size in &chunk_sizes {
            total_tests += 1;

            println!("🧪 Testing {} with {} byte chunks", algorithm, chunk_size);

            // Create valid compression info
            let compression_info = create_valid_compression_info(algorithm, chunk_size);

            // Test 1: Valid CRC should allow processing (might fail on actual decompression)
            match test_valid_crc_processing(&compression_info) {
                Ok(_) => {
                    println!("  ✅ Valid CRC processing: PASS");
                    passed_tests += 1;
                }
                Err(e) => {
                    // CRC validation should pass, but decompression might fail
                    // That's acceptable - we're testing CRC validation here
                    if e.to_string().contains("decompression failed")
                        && e.to_string()
                            .contains("No fallback allowed for modern formats")
                    {
                        println!(
                            "  ✅ Valid CRC processing: PASS (expected decompression failure)"
                        );
                        passed_tests += 1;
                    } else {
                        println!("  ❌ Valid CRC processing: FAIL - {}", e);
                    }
                }
            }

            // Test 2: Corrupted CRC should fail with deterministic error
            match test_corrupted_crc_detection(&compression_info, algorithm, chunk_size) {
                Ok(_) => println!("  ❌ Corrupted CRC detection: FAIL - corruption not detected"),
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("CRC32 mismatch")
                        && error_msg.contains("chunk")
                        && error_msg.contains("offset")
                        && error_msg.contains("stored=")
                        && error_msg.contains("calculated=")
                    {
                        println!("  ✅ Corrupted CRC detection: PASS");
                        passed_tests += 1;
                    } else {
                        println!(
                            "  ❌ Corrupted CRC detection: FAIL - wrong error format: {}",
                            e
                        );
                    }
                }
            }

            total_tests += 1; // Add one more for corruption test
        }
    }

    println!("\n📊 Test Matrix Results:");
    println!("  ✅ Passed: {}/{}", passed_tests, total_tests);
    println!(
        "  ❌ Failed: {}/{}",
        total_tests - passed_tests,
        total_tests
    );

    // CI requirement: All tests must pass
    assert_eq!(
        passed_tests, total_tests,
        "All CRC validation tests must pass for CI"
    );
}

/// Test that missing CRCs in modern format cause deterministic failure
#[test]
fn test_missing_crc_requirement_for_modern_format() {
    let compression_info = CompressionInfo {
        algorithm: "LZ4Compressor".to_string(),
        chunk_length: 16384,
        data_length: 16384,
        chunk_offsets: vec![0],
        crc32: None,
        chunk_crcs: vec![], // No per-chunk CRCs
    };

    let result = ChunkDecompressor::new(compression_info, CassandraVersion::V5_0Release);
    assert!(result.is_ok()); // Decompressor creation should succeed

    let mut decompressor = result.unwrap();
    let fake_data = vec![0u8; 100];
    let mut reader = Cursor::new(fake_data);

    // Attempting to read should fail due to missing CRCs in modern format
    let result = decompressor.read_data(&mut reader, 0, 10);
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("Modern format requires per-chunk CRCs"));
    assert!(error_msg.contains("chunk 0"));
    assert!(error_msg.contains("offset 0x0"));

    println!("✅ Missing CRC requirement test passed: {}", error_msg);
}

/// Test that no decompression guessing occurs in modern paths
#[test]
fn test_no_decompression_guessing() {
    let compression_info = CompressionInfo {
        algorithm: "LZ4Compressor".to_string(),
        chunk_length: 16384,
        data_length: 16384,
        chunk_offsets: vec![0],
        crc32: Some(0x12345678),
        chunk_crcs: vec![0xDEADBEEF], // Wrong CRC
    };

    let mut decompressor = ChunkDecompressor::new(compression_info, CassandraVersion::V5_0Release)
        .expect("Failed to create decompressor");

    let fake_compressed_data = vec![0xFF; 100]; // Invalid LZ4 data
    let mut reader = Cursor::new(fake_compressed_data);

    let result = decompressor.read_data(&mut reader, 0, 10);
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();

    // Should fail at CRC validation, not reach decompression guessing
    assert!(error_msg.contains("CRC32 mismatch"));
    assert!(!error_msg.contains("fallback")); // No fallback should be mentioned for CRC failures

    println!(
        "✅ No decompression guessing test passed: CRC validation failed before decompression"
    );
}

/// Test CI matrix integration - all combinations must work
#[test]
fn test_ci_matrix_integration() {
    println!("🚀 CI Matrix Integration Test for Issue #34");

    // This test verifies the CI matrix requirements:
    // 1. All 4 compressors × 3 chunk sizes = 12 combinations
    // 2. Each combination has deterministic CRC validation
    // 3. Failures block merge (simulated by test assertion)

    let compressors = [
        "LZ4Compressor",
        "SnappyCompressor",
        "ZstdCompressor",
        "DeflateCompressor",
    ];
    let chunk_sizes = [16384, 65536, 131072]; // 16KB, 64KB, 128KB

    let mut results = Vec::new();

    for compressor in &compressors {
        for &chunk_size in &chunk_sizes {
            let test_name = format!("{}-{}KB", compressor, chunk_size / 1024);

            // Test CRC validation for this combination
            let compression_info = create_valid_compression_info(compressor, chunk_size);

            // Corrupt the CRC and verify it fails deterministically
            let mut corrupted_info = compression_info.clone();
            if !corrupted_info.chunk_crcs.is_empty() {
                corrupted_info.chunk_crcs[0] = 0xBADC0FFE; // Corrupt first chunk CRC
            }

            let decompressor_result =
                ChunkDecompressor::new(corrupted_info, CassandraVersion::V5_0Release);

            match decompressor_result {
                Ok(mut decompressor) => {
                    let fake_data = vec![0u8; 100];
                    let mut reader = Cursor::new(fake_data);

                    let read_result = decompressor.read_data(&mut reader, 0, 10);

                    // Should fail with CRC mismatch
                    match read_result {
                        Err(e) if e.to_string().contains("CRC32 mismatch") => {
                            results.push((test_name, true, "CRC validation works".to_string()));
                        }
                        Ok(_) => {
                            results.push((
                                test_name,
                                false,
                                "CRC corruption not detected".to_string(),
                            ));
                        }
                        Err(e) => {
                            results.push((test_name, false, format!("Wrong error: {}", e)));
                        }
                    }
                }
                Err(e) => {
                    results.push((
                        test_name,
                        false,
                        format!("Decompressor creation failed: {}", e),
                    ));
                }
            }
        }
    }

    // Report results
    println!("\n📊 CI Matrix Results:");
    let mut all_passed = true;

    for (test_name, passed, message) in &results {
        let status = if *passed { "✅ PASS" } else { "❌ FAIL" };
        println!("  {}: {} - {}", test_name, status, message);

        if !passed {
            all_passed = false;
        }
    }

    println!(
        "\n🎯 CI Gate Result: {}",
        if all_passed {
            "✅ PASS - Safe to merge"
        } else {
            "❌ FAIL - Blocks merge"
        }
    );

    // CI requirement: all combinations must pass
    assert!(
        all_passed,
        "CI matrix failed - this would block merge in production"
    );
}

/// Create a valid compression info structure for testing
fn create_valid_compression_info(algorithm: &str, chunk_size: u32) -> CompressionInfo {
    CompressionInfo {
        algorithm: algorithm.to_string(),
        chunk_length: chunk_size,
        data_length: chunk_size as u64 * 4, // 4 chunks of data
        chunk_offsets: vec![
            0,
            chunk_size as u64,
            chunk_size as u64 * 2,
            chunk_size as u64 * 3,
        ],
        crc32: Some(0x12345678),
        chunk_crcs: vec![
            CompressionInfo::calculate_crc32(&[0u8; 100]), // Valid CRC for fake data
            CompressionInfo::calculate_crc32(&[1u8; 100]),
            CompressionInfo::calculate_crc32(&[2u8; 100]),
            CompressionInfo::calculate_crc32(&[3u8; 100]),
        ],
    }
}

/// Test that valid CRC allows processing to continue
fn test_valid_crc_processing(compression_info: &CompressionInfo) -> Result<Vec<u8>> {
    let mut decompressor =
        ChunkDecompressor::new(compression_info.clone(), CassandraVersion::V5_0Release)?;

    // Use fake data that matches the CRC
    let fake_data = vec![0u8; 100];
    let mut reader = Cursor::new(fake_data);

    // This might fail at decompression, but should pass CRC validation
    decompressor.read_data(&mut reader, 0, 10)
}

/// Test that corrupted CRC is detected with deterministic error
fn test_corrupted_crc_detection(
    compression_info: &CompressionInfo,
    _algorithm: &str,
    _chunk_size: u32,
) -> Result<Vec<u8>> {
    let mut corrupted_info = compression_info.clone();

    // Corrupt the first chunk's CRC
    if !corrupted_info.chunk_crcs.is_empty() {
        corrupted_info.chunk_crcs[0] = 0xDEADBEEF; // Obviously wrong CRC
    }

    let mut decompressor = ChunkDecompressor::new(corrupted_info, CassandraVersion::V5_0Release)?;

    let fake_data = vec![0u8; 100];
    let mut reader = Cursor::new(fake_data);

    // This should fail with CRC mismatch
    decompressor.read_data(&mut reader, 0, 10)
}
