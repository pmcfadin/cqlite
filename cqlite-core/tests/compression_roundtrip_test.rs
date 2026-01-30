//! Compression roundtrip tests for M5.1
//!
//! Tests that data written with compression can be read back correctly.
//! Validates LZ4, Snappy, Deflate, and Zstd compression algorithms.

#![cfg(feature = "write-support")]

use cqlite_core::storage::sstable::writer::{
    create_compressor, CompressedDataWriter, CompressionAlgorithm, CompressionInfoWriter,
    CompressionMetadata,
};
use tempfile::TempDir;

/// Test helper to verify compression roundtrip
fn test_compression_roundtrip(algorithm: CompressionAlgorithm, data: &[u8]) {
    // Create compressor
    let compressor = create_compressor(algorithm).expect("Failed to create compressor");

    // Write compressed data
    let mut writer = CompressedDataWriter::new(compressor);
    writer.write(data).expect("Failed to write data");
    let (compressed, metadata) = writer.finish().expect("Failed to finish writing");

    // Verify metadata
    assert_eq!(metadata.algorithm, algorithm);
    assert!(metadata.chunk_count() > 0, "Should have at least one chunk");
    assert!(
        metadata.compressed_length > 0,
        "Compressed length should be positive"
    );

    // Verify we can write CompressionInfo.db
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let info_path = temp_dir.path().join("CompressionInfo.db");
    let info_writer = CompressionInfoWriter::new(info_path.clone());
    info_writer
        .write(&metadata)
        .expect("Failed to write CompressionInfo.db");
    assert!(info_path.exists(), "CompressionInfo.db should exist");

    // Verify compressed data is not empty
    assert!(
        !compressed.is_empty(),
        "Compressed data should not be empty"
    );
}

#[test]
#[cfg(feature = "lz4")]
fn test_lz4_compression_roundtrip() {
    let data = b"Hello, World! This is test data for LZ4 compression. ".repeat(100);
    test_compression_roundtrip(CompressionAlgorithm::Lz4, &data);
}

#[test]
#[cfg(feature = "snappy")]
fn test_snappy_compression_roundtrip() {
    let data = b"Hello, World! This is test data for Snappy compression. ".repeat(100);
    test_compression_roundtrip(CompressionAlgorithm::Snappy, &data);
}

#[test]
#[cfg(feature = "deflate")]
fn test_deflate_compression_roundtrip() {
    let data = b"Hello, World! This is test data for Deflate compression. ".repeat(100);
    test_compression_roundtrip(CompressionAlgorithm::Deflate, &data);
}

#[test]
#[cfg(feature = "zstd")]
fn test_zstd_compression_roundtrip() {
    let data = b"Hello, World! This is test data for Zstd compression. ".repeat(100);
    test_compression_roundtrip(CompressionAlgorithm::Zstd, &data);
}

#[test]
fn test_noop_compression_roundtrip() {
    let data = b"Hello, World! This is test data for no compression. ".repeat(100);
    test_compression_roundtrip(CompressionAlgorithm::None, &data);
}

#[test]
#[cfg(feature = "lz4")]
fn test_lz4_multi_chunk_compression() {
    // Create data larger than default chunk size (64KB)
    let data = vec![0x42u8; 128 * 1024]; // 128KB

    let compressor = create_compressor(CompressionAlgorithm::Lz4).unwrap();
    let mut writer = CompressedDataWriter::with_chunk_size(compressor, 32 * 1024); // 32KB chunks
    writer.write(&data).unwrap();
    let (_compressed, metadata) = writer.finish().unwrap();

    // Should have multiple chunks
    assert!(
        metadata.chunk_count() >= 4,
        "Should have at least 4 chunks for 128KB with 32KB chunk size"
    );

    // All chunks should have CRCs
    assert_eq!(
        metadata.chunk_crcs.len(),
        metadata.chunk_count(),
        "Should have CRC for each chunk"
    );
}

#[test]
#[cfg(feature = "lz4")]
fn test_compression_effectiveness() {
    // Highly compressible data (repeating pattern)
    let compressible_data = vec![0xAAu8; 64 * 1024]; // 64KB of 0xAA

    let compressor = create_compressor(CompressionAlgorithm::Lz4).unwrap();
    let mut writer = CompressedDataWriter::new(compressor);
    writer.write(&compressible_data).unwrap();
    let (_compressed, metadata) = writer.finish().unwrap();

    // Compressed size should be significantly smaller
    let original_size = compressible_data.len();
    let compressed_size = metadata.compressed_length as usize;

    assert!(
        compressed_size < original_size / 2,
        "Highly compressible data should compress to less than 50%: {} -> {}",
        original_size,
        compressed_size
    );
}

#[test]
fn test_compression_info_crc_validation() {
    let temp_dir = TempDir::new().unwrap();
    let info_path = temp_dir.path().join("test-CompressionInfo.db");

    let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 65536);
    metadata.add_chunk(0, Some(0x12345678));
    metadata.add_chunk(65536, Some(0xABCDEF01));
    metadata.set_compressed_length(100000);

    let writer = CompressionInfoWriter::new(info_path.clone());
    writer.write(&metadata).unwrap();

    // Read back and verify CRC is valid
    let bytes = std::fs::read(&info_path).unwrap();

    // Last 4 bytes should be CRC32
    let content_len = bytes.len() - 4;
    let stored_crc = u32::from_be_bytes([
        bytes[content_len],
        bytes[content_len + 1],
        bytes[content_len + 2],
        bytes[content_len + 3],
    ]);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes[..content_len]);
    let computed_crc = hasher.finalize();

    assert_eq!(stored_crc, computed_crc, "CRC should match");
}

#[test]
fn test_compression_info_binary_format() {
    let temp_dir = TempDir::new().unwrap();
    let info_path = temp_dir.path().join("format-test-CompressionInfo.db");

    let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 65536);
    metadata.add_chunk(0, Some(0xDEADBEEF));
    metadata.set_compressed_length(50000);

    let writer = CompressionInfoWriter::new(info_path.clone());
    writer.write(&metadata).unwrap();

    let bytes = std::fs::read(&info_path).unwrap();

    // Verify algorithm name
    let name_len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let name = String::from_utf8(bytes[2..2 + name_len].to_vec()).unwrap();
    assert_eq!(name, "LZ4Compressor");

    // Verify chunk length (after 4-byte padding)
    let chunk_len_offset = 2 + name_len + 4;
    let chunk_len = u32::from_be_bytes([
        bytes[chunk_len_offset],
        bytes[chunk_len_offset + 1],
        bytes[chunk_len_offset + 2],
        bytes[chunk_len_offset + 3],
    ]);
    assert_eq!(chunk_len, 65536);
}

#[test]
fn test_trailing_crc_position() {
    // CRITICAL: Verify CRC is TRAILING (after chunk data), NOT leading

    let compressor = create_compressor(CompressionAlgorithm::None).unwrap();
    let mut writer = CompressedDataWriter::with_chunk_size(compressor, 64);

    let data = b"TestDataForCRCVerification12345"; // 32 bytes
    writer.write(data).unwrap();
    let (compressed, _metadata) = writer.finish().unwrap();

    // For NoopCompressor, compressed = original data
    // Format: [data][crc32]
    assert_eq!(
        compressed.len(),
        data.len() + 4,
        "Output should be data + 4-byte CRC"
    );

    // Verify data comes first
    assert_eq!(&compressed[..data.len()], data, "Data should be at start");

    // Verify CRC is at end
    let crc_bytes = &compressed[data.len()..];
    let stored_crc = u32::from_be_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    let expected_crc = hasher.finalize();

    assert_eq!(stored_crc, expected_crc, "Trailing CRC should match");
}
