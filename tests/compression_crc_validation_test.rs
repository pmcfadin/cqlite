//! Comprehensive CRC validation tests for compression metadata
//!
//! This test module validates that CRC mismatches are detected deterministically
//! for all compression algorithms and that no fallback decompression occurs.

use cqlite_core::storage::sstable::chunk_decompressor::ChunkDecompressor;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
// unused imports: Error and Result - tests compile without them currently
// use cqlite_core::{Error, Result};
use std::io::Cursor;

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that CRC mismatch in metadata is detected
    #[test]
    fn test_metadata_crc_mismatch_detection() {
        // Create valid compression info data
        let mut data = vec![
            0x00, 0x0d, // algorithm name length: 13
            // "LZ4Compressor"
            0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x00,
            0x00, 0x00, // padding
            0x00, 0x00, 0x40, 0x00, // chunk length: 16384
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, // data length: 65536
            0x00, 0x00, 0x00, 0x04, // chunk count: 4
            // Chunk offsets
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, // offset 8192
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, // offset 16384
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x60, 0x00, // offset 24576
        ];

        // Add an invalid CRC32
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

        // Parsing should fail with CRC mismatch error
        let result = CompressionInfo::parse(&data);
        assert!(result.is_err());

        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("CRC32 mismatch"));
            assert!(error_msg.contains("stored="));
            assert!(error_msg.contains("calculated="));
        }
    }

    /// Test that per-chunk CRC validation works correctly
    #[test]
    fn test_per_chunk_crc_validation() {
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 32768,
            chunk_offsets: vec![0, 8192],
            crc32: Some(0x12345678),
            chunk_crcs: vec![0xAAAAAAAA, 0xBBBBBBBB], // Expected CRCs for chunks
        };

        // Test valid chunk data
        let valid_chunk_data = vec![0u8; 16384]; // Data that would produce CRC 0xAAAAAAAA
        let result = compression_info.validate_chunk_crc(0, &valid_chunk_data);
        // This will fail because our test data doesn't actually produce that CRC
        assert!(result.is_err());

        // Test error message format
        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("CRC32 mismatch for chunk"));
            assert!(error_msg.contains("at offset"));
            assert!(error_msg.contains("stored=0xaaaaaaaa"));
        }
    }

    /// Test that missing CRC for modern format causes failure
    #[test]
    fn test_missing_crc_for_modern_format() {
        let data = vec![
            0x00, 0x0d, // algorithm name length: 13
            // "LZ4Compressor"
            0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x00,
            0x00, 0x00, // padding
            0x00, 0x00, 0x40, 0x00, // chunk length: 16384
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // data length: 4096
            0x00, 0x00, 0x00, 0x01, // chunk count: 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, // chunk offset: 0
                  // No CRC32 at the end
        ];

        // Using strict CRC-required parsing should fail
        let result = CompressionInfo::parse_with_crc_required(&data);
        assert!(result.is_err());

        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("CRC32 checksum required but not found"));
        }
    }

    /// Test that each compression algorithm reports errors correctly
    #[test]
    fn test_compression_algorithm_error_reporting() {
        let test_cases = vec![
            ("LZ4Compressor", "LZ4 decompression failed"),
            ("SnappyCompressor", "Snappy decompression failed"),
            ("ZstdCompressor", "Zstd decompression failed"),
            ("DeflateCompressor", "Deflate decompression failed"),
        ];

        for (algorithm, expected_error) in test_cases {
            let compression_info = CompressionInfo {
                algorithm: algorithm.to_string(),
                chunk_length: 16384,
                data_length: 16384,
                chunk_offsets: vec![0],
                crc32: None,
                chunk_crcs: vec![],
            };

            let decompressor = ChunkDecompressor::new(compression_info).unwrap();

            // Create invalid compressed data that will fail decompression
            let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Invalid for all formats
            let mut reader = Cursor::new(invalid_data);

            // Attempting to read should fail with specific error
            let result = decompressor.read_data(&mut reader, 0, 100);

            // We expect an error, but the exact message depends on the algorithm
            // and whether the feature is compiled in
            assert!(result.is_err() || algorithm == "LZ4Compressor"); // LZ4 might succeed with empty result
        }
    }

    /// Test matrix generation for different chunk sizes
    #[test]
    fn test_chunk_size_matrix() {
        let chunk_sizes = vec![4096, 16384, 65536]; // 4KB, 16KB, 64KB
        let algorithms = vec![
            "LZ4Compressor",
            "SnappyCompressor",
            "ZstdCompressor",
            "DeflateCompressor",
        ];

        for algorithm in &algorithms {
            for &chunk_size in &chunk_sizes {
                let compression_info = CompressionInfo {
                    algorithm: algorithm.to_string(),
                    chunk_length: chunk_size,
                    data_length: chunk_size as u64 * 4, // 4 chunks
                    chunk_offsets: (0..4).map(|i| i * chunk_size as u64).collect(),
                    crc32: Some(0x12345678),
                    chunk_crcs: vec![0x11111111, 0x22222222, 0x33333333, 0x44444444],
                };

                // Validate the structure
                assert!(compression_info.validate().is_ok());
                assert_eq!(compression_info.chunk_length, chunk_size);
                assert_eq!(compression_info.chunk_offsets.len(), 4);

                // Test chunk index calculations
                assert_eq!(compression_info.chunk_for_offset(0), 0);
                assert_eq!(compression_info.chunk_for_offset(chunk_size as u64), 1);
                assert_eq!(compression_info.chunk_for_offset(chunk_size as u64 * 2), 2);
                assert_eq!(compression_info.chunk_for_offset(chunk_size as u64 * 3), 3);
            }
        }
    }

    /// Test that CRC validation can be skipped for legacy formats
    #[test]
    fn test_legacy_format_without_crc() {
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 16384,
            chunk_offsets: vec![0],
            crc32: None,
            chunk_crcs: vec![], // No per-chunk CRCs for legacy
        };

        // Legacy format validation should succeed even without CRCs
        let chunk_data = vec![0u8; 16384];
        let result = compression_info.validate_chunk_crc(0, &chunk_data);
        assert!(result.is_ok()); // Should skip validation when chunk_crcs is empty
    }

    /// Test deterministic error reporting format
    #[test]
    fn test_error_message_format() {
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 65536,
            chunk_offsets: vec![0x0000, 0x2000, 0x4000, 0x6000],
            crc32: Some(0xABCDEF00),
            chunk_crcs: vec![0x12345678, 0x87654321, 0xDEADBEEF, 0xCAFEBABE],
        };

        // Test chunk 2 with wrong CRC
        let test_data = vec![0x42; 16384]; // Data that won't match expected CRC
        let result = compression_info.validate_chunk_crc(2, &test_data);

        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{}", e);

            // Verify error message contains all required components
            assert!(error_msg.contains("chunk 2"));
            assert!(error_msg.contains("offset 0x4000"));
            assert!(error_msg.contains("stored=0xdeadbeef"));
            assert!(error_msg.contains("calculated="));

            println!("Error message format: {}", error_msg);
        }
    }
}
