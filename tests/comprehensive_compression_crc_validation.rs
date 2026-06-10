//! Comprehensive CRC validation tests (updated for Bug #638 fix)
//!
//! Per Cassandra authority sources (CompressionMetadata.java lines 375-392):
//!   - CRCs are NOT stored in CompressionInfo.db
//!   - Each compressed chunk in Data.db is followed by a 4-byte inline CRC32
//!   - The ChunkDecompressor strips those 4 CRC bytes before decompressing
//!   - The CompressionInfo struct exposes: algorithm, option_pairs,
//!     chunk_length, max_compressed_length, data_length, chunk_offsets

use cqlite_core::parser::header::CassandraVersion;
use cqlite_core::storage::sstable::chunk_decompressor::ChunkDecompressor;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use std::io::Cursor;

/// Helper: build a minimal valid CompressionInfo for the given algorithm and chunk_size
fn make_compression_info(algorithm: &str, chunk_size: u32) -> CompressionInfo {
    CompressionInfo {
        algorithm: algorithm.to_string(),
        option_pairs: vec![],
        chunk_length: chunk_size,
        max_compressed_length: i32::MAX as u32,
        data_length: chunk_size as u64 * 4, // 4 chunks
        chunk_offsets: vec![
            0,
            chunk_size as u64,
            chunk_size as u64 * 2,
            chunk_size as u64 * 3,
        ],
    }
}

/// Verify that CompressionInfo can be constructed for all supported algorithms
/// and all standard chunk sizes without error (Bug #638 regression).
#[test]
fn test_compression_info_construction_matrix() {
    let algorithms = [
        "LZ4Compressor",
        "SnappyCompressor",
        "ZstdCompressor",
        "DeflateCompressor",
    ];
    let chunk_sizes: [u32; 3] = [16384, 65536, 131072];

    for algorithm in &algorithms {
        for &chunk_size in &chunk_sizes {
            let info = make_compression_info(algorithm, chunk_size);
            assert!(
                info.validate().is_ok(),
                "validate() failed for {algorithm} chunk_size={chunk_size}"
            );
            assert_eq!(info.algorithm, *algorithm);
            assert_eq!(info.chunk_length, chunk_size);
            assert_eq!(info.chunk_offsets.len(), 4);
            // CRCs are NOT fields of CompressionInfo (Bug #638)
            assert_eq!(info.option_pairs.len(), 0);
            assert_eq!(info.max_compressed_length, i32::MAX as u32);
        }
    }
}

/// Verify that ChunkDecompressor can be created for all algorithm/chunk-size pairs.
/// Decompressor creation must not require CRCs in CompressionInfo (Bug #638).
#[test]
fn test_decompressor_creation_matrix() {
    let algorithms = [
        "LZ4Compressor",
        "SnappyCompressor",
        "ZstdCompressor",
        "DeflateCompressor",
    ];
    let chunk_sizes: [u32; 3] = [16384, 65536, 131072];

    for algorithm in &algorithms {
        for &chunk_size in &chunk_sizes {
            let info = make_compression_info(algorithm, chunk_size);
            let result = ChunkDecompressor::new(info, CassandraVersion::V5_0Release);
            assert!(
                result.is_ok(),
                "ChunkDecompressor::new failed for {algorithm} chunk_size={chunk_size}"
            );
        }
    }
}

/// Verify that corrupt Data.db bytes (bad inline CRC) produce a meaningful error.
/// The error must come from CRC validation, not a decompression guess.
#[test]
fn test_corrupt_inline_crc_detected() {
    // Build a fake Data.db record: 8 bytes of compressed data + 4 bytes of wrong CRC
    let fake_compressed: Vec<u8> = vec![0xFF; 8];
    let wrong_crc: u32 = 0xDEADBEEF;
    let mut fake_data = fake_compressed.clone();
    fake_data.extend_from_slice(&wrong_crc.to_be_bytes());

    // Create a CompressionInfo with a single chunk at offset 0.
    // The "next" offset (i.e., end-of-file marker) determines the record size,
    // which is 8+4 = 12 bytes.  We encode this by giving two offsets: [0, 12].
    let info = CompressionInfo {
        algorithm: "LZ4Compressor".to_string(),
        option_pairs: vec![],
        chunk_length: 65536,
        max_compressed_length: i32::MAX as u32,
        data_length: 65536,
        chunk_offsets: vec![0, 12], // delta = 12 → compressed_len = 12 - 4 = 8
    };

    let mut decompressor =
        ChunkDecompressor::new(info, CassandraVersion::V5_0Release).expect("decompressor created");
    let mut reader = Cursor::new(fake_data);

    let result = decompressor.read_data(&mut reader, 0, 4);
    assert!(result.is_err(), "Expected error for corrupt CRC, got Ok");

    let err_msg = result.unwrap_err().to_string();
    // Error should mention CRC or checksum — not a vague "decompression failed" from guessing
    assert!(
        err_msg.to_lowercase().contains("crc")
            || err_msg.to_lowercase().contains("checksum")
            || err_msg.to_lowercase().contains("mismatch"),
        "Error message should reference CRC validation, got: {err_msg}"
    );
}

/// Verify chunk_for_offset calculations across all chunk sizes.
#[test]
fn test_chunk_for_offset_matrix() {
    let chunk_sizes: [u32; 3] = [4096, 16384, 65536];

    for &chunk_size in &chunk_sizes {
        let info = make_compression_info("LZ4Compressor", chunk_size);
        assert_eq!(info.chunk_for_offset(0), 0);
        assert_eq!(info.chunk_for_offset(chunk_size as u64), 1);
        assert_eq!(info.chunk_for_offset(chunk_size as u64 * 2), 2);
        assert_eq!(info.chunk_for_offset(chunk_size as u64 * 3), 3);
    }
}

/// Verify that max_compressed_length field is accessible (Bug #638 — old struct lacked it).
#[test]
fn test_max_compressed_length_accessible() {
    let info = make_compression_info("LZ4Compressor", 65536);
    // Default is i32::MAX when minCompressRatio=0 (the Cassandra default)
    assert_eq!(info.max_compressed_length, i32::MAX as u32);
}

/// Verify that option_pairs are accessible (Bug #638 — old parser skipped options).
#[test]
fn test_option_pairs_accessible() {
    let info = CompressionInfo {
        algorithm: "LZ4Compressor".to_string(),
        option_pairs: vec![("chunk_length_in_kb".to_string(), "64".to_string())],
        chunk_length: 65536,
        max_compressed_length: i32::MAX as u32,
        data_length: 65536,
        chunk_offsets: vec![0],
    };
    assert_eq!(info.option_pairs.len(), 1);
    assert_eq!(info.option_pairs[0].0, "chunk_length_in_kb");
    assert_eq!(info.option_pairs[0].1, "64");
}
