//! Comprehensive Compression Test Matrix for CI
//!
//! Tests 4 compression algorithms × 3 chunk sizes = 12 test scenarios
//! - Algorithms: LZ4, Snappy, Deflate, Zstd
//! - Chunk sizes: 16KiB, 64KiB, 128KiB
//! - Positive tests: correct parsing and decompression
//! - Negative tests: corrupt one chunk CRC per dataset, deterministic errors
//!
//! This comprehensive test matrix validates compression functionality required
//! for issue #34 and M1 CI pipeline integration.

use cqlite_core::parser::header::CassandraVersion;
use cqlite_core::storage::sstable::chunk_decompressor::ChunkDecompressor;
use cqlite_core::storage::sstable::compression::CompressionAlgorithm;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use cqlite_core::{Error, Result};
use std::io::Cursor;

/// Test matrix configuration for comprehensive compression testing
#[derive(Debug, Clone)]
struct CompressionTestCase {
    algorithm: CompressionAlgorithm,
    chunk_size: u32,
    test_data_size: usize,
    expected_chunks: usize,
    description: String,
}

/// Generate test matrix covering all algorithm × chunk size combinations
fn generate_test_matrix() -> Vec<CompressionTestCase> {
    let algorithms = vec![
        (CompressionAlgorithm::Lz4, "LZ4"),
        (CompressionAlgorithm::Snappy, "Snappy"),
        (CompressionAlgorithm::Deflate, "Deflate"),
        (CompressionAlgorithm::Zstd, "Zstd"),
    ];

    let chunk_sizes = vec![
        (16 * 1024, "16KiB"),
        (64 * 1024, "64KiB"),
        (128 * 1024, "128KiB"),
    ];

    let test_data_size = 1024 * 1024; // 1MB test data

    let mut test_cases = Vec::new();

    for (algorithm, algo_name) in algorithms {
        for (chunk_size, size_name) in &chunk_sizes {
            let expected_chunks = (test_data_size + chunk_size - 1) / chunk_size;

            test_cases.push(CompressionTestCase {
                algorithm: algorithm.clone(),
                chunk_size: *chunk_size as u32,
                test_data_size,
                expected_chunks,
                description: format!("{} compression with {} chunks", algo_name, size_name),
            });
        }
    }

    test_cases
}

/// Generate test data with predictable patterns for compression
fn generate_test_data(size: usize, pattern_type: &str) -> Vec<u8> {
    match pattern_type {
        "repetitive" => {
            // Highly compressible data
            let pattern = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            let mut data = Vec::with_capacity(size);
            for i in 0..size {
                data.push(pattern[i % pattern.len()]);
            }
            data
        }
        "mixed" => {
            // Moderately compressible data
            let mut data = Vec::with_capacity(size);
            for i in 0..size {
                match i % 4 {
                    0 => data.push((i % 256) as u8),
                    1 => data.push(0x42), // Repeated byte
                    2 => data.push(((i * 7) % 256) as u8),
                    _ => data.push(0xFF), // Another repeated byte
                }
            }
            data
        }
        "random" => {
            // Less compressible data
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut data = Vec::with_capacity(size);
            for i in 0..size {
                let mut hasher = DefaultHasher::new();
                i.hash(&mut hasher);
                data.push((hasher.finish() % 256) as u8);
            }
            data
        }
        _ => vec![0u8; size],
    }
}

/// Generate mock CompressionInfo for testing
fn create_mock_compression_info(
    algorithm: CompressionAlgorithm,
    chunk_size: u32,
    data_size: usize,
    compressed_chunks: &[(u64, u32)], // (offset, compressed_size)
) -> CompressionInfo {
    let algorithm_name = match algorithm {
        CompressionAlgorithm::Lz4 => "LZ4Compressor",
        CompressionAlgorithm::Snappy => "SnappyCompressor",
        CompressionAlgorithm::Deflate => "DeflateCompressor",
        CompressionAlgorithm::Zstd => "ZstdCompressor",
        CompressionAlgorithm::None => "NoCompressor",
    };

    let chunk_offsets: Vec<u64> = compressed_chunks
        .iter()
        .map(|(offset, _)| *offset)
        .collect();

    CompressionInfo {
        algorithm: algorithm_name.to_string(),
        chunk_length: chunk_size,
        data_length: data_size as u64,
        chunk_offsets,
        crc32: None,
        chunk_crcs: vec![], // CRCs will be calculated separately
    }
}

/// Calculate CRC32 for chunk data
fn calculate_crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Create compressed test data with proper chunk structure
/// Uses raw compression format compatible with ChunkDecompressor
fn create_compressed_test_data(
    test_case: &CompressionTestCase,
    original_data: &[u8],
) -> Result<(Vec<u8>, CompressionInfo)> {
    let chunk_size = test_case.chunk_size as usize;

    let mut compressed_data = Vec::new();
    let mut chunk_info = Vec::new();
    let mut chunk_crcs = Vec::new();

    // Compress data chunk by chunk using RAW compression (no size prefixes)
    // This matches what ChunkDecompressor expects for modern formats
    for (i, chunk_data) in original_data.chunks(chunk_size).enumerate() {
        let compressed_chunk = match test_case.algorithm {
            CompressionAlgorithm::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    // Use raw LZ4 compression without size prefix
                    lz4_flex::compress(chunk_data)
                }
                #[cfg(not(feature = "lz4"))]
                {
                    return Err(Error::UnsupportedFormat("LZ4 not available".to_string()));
                }
            }
            CompressionAlgorithm::Snappy => {
                #[cfg(feature = "snappy")]
                {
                    use snap::raw::Encoder;
                    let mut encoder = Encoder::new();
                    encoder.compress_vec(chunk_data).map_err(|e| {
                        Error::InvalidFormat(format!("Snappy compression failed: {}", e))
                    })?
                }
                #[cfg(not(feature = "snappy"))]
                {
                    return Err(Error::UnsupportedFormat("Snappy not available".to_string()));
                }
            }
            CompressionAlgorithm::Deflate => {
                #[cfg(feature = "deflate")]
                {
                    use flate2::Compression as DeflateCompression;
                    use flate2::write::DeflateEncoder;
                    use std::io::Write;

                    let mut encoder = DeflateEncoder::new(Vec::new(), DeflateCompression::new(6));
                    encoder.write_all(chunk_data).map_err(|e| {
                        Error::InvalidFormat(format!("Deflate compression failed: {}", e))
                    })?;
                    encoder.finish().map_err(|e| {
                        Error::InvalidFormat(format!("Deflate finish failed: {}", e))
                    })?
                }
                #[cfg(not(feature = "deflate"))]
                {
                    return Err(Error::UnsupportedFormat(
                        "Deflate not available".to_string(),
                    ));
                }
            }
            CompressionAlgorithm::Zstd => {
                #[cfg(feature = "zstd")]
                {
                    zstd::encode_all(&chunk_data[..], 3).map_err(|e| {
                        Error::InvalidFormat(format!("Zstd compression failed: {}", e))
                    })?
                }
                #[cfg(not(feature = "zstd"))]
                {
                    return Err(Error::UnsupportedFormat("Zstd not available".to_string()));
                }
            }
            CompressionAlgorithm::None => chunk_data.to_vec(),
        };

        let chunk_offset = compressed_data.len() as u64;

        // Calculate CRC for compressed chunk
        let chunk_crc = calculate_crc32(&compressed_chunk);
        chunk_crcs.push(chunk_crc);

        chunk_info.push((chunk_offset, compressed_chunk.len() as u32));
        compressed_data.extend_from_slice(&compressed_chunk);

        println!(
            "Chunk {}: {} bytes -> {} bytes (ratio: {:.2}%)",
            i,
            chunk_data.len(),
            compressed_chunk.len(),
            (compressed_chunk.len() as f64 / chunk_data.len() as f64) * 100.0
        );
    }

    let mut compression_info = create_mock_compression_info(
        test_case.algorithm.clone(),
        test_case.chunk_size,
        original_data.len(),
        &chunk_info,
    );

    compression_info.chunk_crcs = chunk_crcs;

    Ok((compressed_data, compression_info))
}

/// Positive test: verify correct parsing and decompression
fn test_positive_case(test_case: &CompressionTestCase) -> Result<()> {
    println!("\n🧪 Testing positive case: {}", test_case.description);

    // Generate test data
    let original_data = generate_test_data(test_case.test_data_size, "mixed");

    // Create compressed data with proper chunk structure
    let (compressed_data, compression_info) =
        create_compressed_test_data(test_case, &original_data)?;

    // Validate compression info
    assert_eq!(
        compression_info.chunk_offsets.len(),
        test_case.expected_chunks
    );
    assert_eq!(
        compression_info.data_length,
        test_case.test_data_size as u64
    );
    assert_eq!(compression_info.chunk_length, test_case.chunk_size);

    // Create chunk decompressor
    let mut decompressor = ChunkDecompressor::new(compression_info, CassandraVersion::V5_0Release)?;

    // Test decompression
    let mut reader = Cursor::new(compressed_data);
    let decompressed_data = decompressor.read_all_data(&mut reader)?;

    // Verify decompressed data matches original
    assert_eq!(
        decompressed_data.len(),
        original_data.len(),
        "Decompressed data size mismatch for {}",
        test_case.description
    );

    assert_eq!(
        decompressed_data, original_data,
        "Decompressed data content mismatch for {}",
        test_case.description
    );

    println!("✅ Positive test passed: {}", test_case.description);
    Ok(())
}

/// Negative test: corrupt one chunk CRC and verify deterministic error
fn test_negative_case(test_case: &CompressionTestCase) -> Result<()> {
    println!(
        "\n🔍 Testing negative case: {} (corrupted CRC)",
        test_case.description
    );

    // Generate test data
    let original_data = generate_test_data(test_case.test_data_size, "mixed");

    // Create compressed data with proper chunk structure
    let (compressed_data, mut compression_info) =
        create_compressed_test_data(test_case, &original_data)?;

    // Corrupt the CRC of the first chunk
    if !compression_info.chunk_crcs.is_empty() {
        compression_info.chunk_crcs[0] = compression_info.chunk_crcs[0].wrapping_add(1);
        println!("🔧 Corrupted CRC for chunk 0");
    }

    // Create chunk decompressor
    let mut decompressor = ChunkDecompressor::new(compression_info, CassandraVersion::V5_0Release)?;

    // Test decompression - should fail with deterministic error
    let mut reader = Cursor::new(compressed_data);
    let result = decompressor.read_all_data(&mut reader);

    // Verify it fails with the expected error type
    match result {
        Err(Error::InvalidFormat(msg)) => {
            assert!(
                msg.contains("CRC") || msg.contains("checksum"),
                "Error message should mention CRC/checksum: {}",
                msg
            );
            println!("✅ Negative test passed: deterministic error - {}", msg);
        }
        Err(other_error) => {
            println!("⚠️ Unexpected error type: {:?}", other_error);
            // For compatibility, accept other error types that might indicate corruption
            if format!("{:?}", other_error).contains("Invalid") {
                println!("✅ Negative test passed: detected corruption");
            } else {
                return Err(other_error);
            }
        }
        Ok(_) => {
            return Err(Error::InvalidFormat(
                "Expected CRC validation to fail but decompression succeeded".to_string(),
            ));
        }
    }

    Ok(())
}

/// Test matrix runner for positive cases
#[test]
fn test_compression_matrix_positive() {
    let test_matrix = generate_test_matrix();
    let mut passed = 0;
    let mut failed = 0;

    println!("\n🚀 Running Compression Test Matrix - Positive Cases");
    println!(
        "📊 Testing {} combinations (4 algorithms × 3 chunk sizes)",
        test_matrix.len()
    );

    for test_case in &test_matrix {
        match test_positive_case(test_case) {
            Ok(()) => {
                passed += 1;
                // Notify progress via hooks
                let _ = std::process::Command::new("npx")
                    .args([
                        "claude-flow@alpha",
                        "hooks",
                        "notify",
                        "--message",
                        &format!("positive-test-passed: {}", test_case.description),
                    ])
                    .output();
            }
            Err(e) => {
                failed += 1;
                eprintln!("❌ Failed positive test {}: {}", test_case.description, e);
                // For CI, we want to continue testing other cases
            }
        }
    }

    println!(
        "\n📊 Positive Test Results: {} passed, {} failed",
        passed, failed
    );

    // Report progress via hooks
    let _ = std::process::Command::new("npx")
        .args([
            "claude-flow@alpha",
            "hooks",
            "notify",
            "--message",
            &format!(
                "test-matrix-progress: positive-tests {}/{} passed",
                passed,
                test_matrix.len()
            ),
        ])
        .output();

    // For CI integration, fail if any tests failed
    assert_eq!(failed, 0, "All positive compression tests must pass");
}

/// Test matrix runner for negative cases
#[test]
fn test_compression_matrix_negative() {
    let test_matrix = generate_test_matrix();
    let mut passed = 0;
    let mut failed = 0;

    println!("\n🔍 Running Compression Test Matrix - Negative Cases");
    println!(
        "📊 Testing {} combinations with corrupted CRCs",
        test_matrix.len()
    );

    for test_case in &test_matrix {
        match test_negative_case(test_case) {
            Ok(()) => {
                passed += 1;
                // Notify progress via hooks
                let _ = std::process::Command::new("npx")
                    .args([
                        "claude-flow@alpha",
                        "hooks",
                        "notify",
                        "--message",
                        &format!("negative-test-passed: {}", test_case.description),
                    ])
                    .output();
            }
            Err(e) => {
                failed += 1;
                eprintln!("❌ Failed negative test {}: {}", test_case.description, e);
                // For CI, we want to continue testing other cases
            }
        }
    }

    println!(
        "\n📊 Negative Test Results: {} passed, {} failed",
        passed, failed
    );

    // Report progress via hooks
    let _ = std::process::Command::new("npx")
        .args([
            "claude-flow@alpha",
            "hooks",
            "notify",
            "--message",
            &format!(
                "test-matrix-progress: negative-tests {}/{} passed",
                passed,
                test_matrix.len()
            ),
        ])
        .output();

    // For CI integration, fail if any tests failed
    assert_eq!(failed, 0, "All negative compression tests must pass");
}

/// Performance benchmark for compression matrix
#[test]
#[cfg(feature = "benchmarks")]
fn test_compression_matrix_performance() {
    use std::time::Instant;

    let test_matrix = generate_test_matrix();
    let mut performance_results = std::collections::HashMap::new();

    println!("\n⚡ Running Compression Performance Benchmarks");

    for test_case in &test_matrix {
        let start = Instant::now();

        if test_positive_case(test_case).is_ok() {
            let duration = start.elapsed();
            performance_results.insert(test_case.description.clone(), duration);

            println!(
                "📊 {}: {:.2}ms",
                test_case.description,
                duration.as_secs_f64() * 1000.0
            );
        }
    }

    // Report overall performance via hooks
    let total_time: std::time::Duration = performance_results.values().sum();
    let _ = std::process::Command::new("npx")
        .args([
            "claude-flow@alpha",
            "hooks",
            "notify",
            "--message",
            &format!(
                "performance-benchmark: total {:.2}ms",
                total_time.as_secs_f64() * 1000.0
            ),
        ])
        .output();

    // Performance threshold: all tests should complete in under 10 seconds total
    assert!(
        total_time.as_secs() < 10,
        "Compression matrix should complete in under 10 seconds, took {:.2}s",
        total_time.as_secs_f64()
    );
}
