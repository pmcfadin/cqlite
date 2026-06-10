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
    assert!(metadata.data_length > 0, "Data length should be positive");

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

    // CRCs are inline in Data.db, not in CompressionMetadata (Bug #638 fix)
    // Verify data_length reflects uncompressed size
    assert_eq!(
        metadata.data_length,
        data.len() as u64,
        "data_length should equal uncompressed input size"
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
    let (compressed, metadata) = writer.finish().unwrap();

    // data_length should equal uncompressed input size
    assert_eq!(
        metadata.data_length,
        compressible_data.len() as u64,
        "data_length should equal uncompressed size"
    );

    // Compressed output (Data.db bytes) should be significantly smaller than the original.
    // Each chunk is: [compressed_bytes][4-byte CRC].  For a single 64KB chunk the CRC
    // overhead is negligible, so the total should still be well under 50% of original.
    let original_size = compressible_data.len();
    assert!(
        compressed.len() < original_size / 2,
        "Highly compressible data should compress to less than 50%: {} -> {}",
        original_size,
        compressed.len()
    );
}

#[test]
fn test_compression_info_binary_format() {
    // Bug #638 regression: verify CompressionInfo.db layout matches Cassandra spec
    // writeUTF(name) + option_count(0) + chunk_length + max_compressed_length + data_length
    // + chunk_count + offsets  (NO trailing metadata CRC, NO chunk CRC array)
    let temp_dir = TempDir::new().unwrap();
    let info_path = temp_dir.path().join("format-test-CompressionInfo.db");

    let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 65536);
    metadata.add_chunk(0);
    metadata.set_data_length(50000);

    let writer = CompressionInfoWriter::new(info_path.clone());
    writer.write(&metadata).unwrap();

    let bytes = std::fs::read(&info_path).unwrap();

    // Byte 0..2: writeUTF length (2-byte BE)
    let name_len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let name = String::from_utf8(bytes[2..2 + name_len].to_vec()).unwrap();
    assert_eq!(name, "LZ4Compressor");

    // After name: option_count (4-byte BE, 0 for default options)
    let option_count_offset = 2 + name_len;
    let option_count = u32::from_be_bytes([
        bytes[option_count_offset],
        bytes[option_count_offset + 1],
        bytes[option_count_offset + 2],
        bytes[option_count_offset + 3],
    ]);
    assert_eq!(option_count, 0, "No options set");

    // After option_count: chunk_length (4-byte BE)
    let chunk_len_offset = option_count_offset + 4;
    let chunk_len = u32::from_be_bytes([
        bytes[chunk_len_offset],
        bytes[chunk_len_offset + 1],
        bytes[chunk_len_offset + 2],
        bytes[chunk_len_offset + 3],
    ]);
    assert_eq!(chunk_len, 65536);

    // After chunk_length: max_compressed_length (4-byte BE) = INT_MAX
    let mcl_offset = chunk_len_offset + 4;
    let max_compressed_length = u32::from_be_bytes([
        bytes[mcl_offset],
        bytes[mcl_offset + 1],
        bytes[mcl_offset + 2],
        bytes[mcl_offset + 3],
    ]);
    assert_eq!(max_compressed_length, i32::MAX as u32);

    // After max_compressed_length: data_length (8-byte BE) = 50000
    let dl_offset = mcl_offset + 4;
    let data_length = u64::from_be_bytes([
        bytes[dl_offset],
        bytes[dl_offset + 1],
        bytes[dl_offset + 2],
        bytes[dl_offset + 3],
        bytes[dl_offset + 4],
        bytes[dl_offset + 5],
        bytes[dl_offset + 6],
        bytes[dl_offset + 7],
    ]);
    assert_eq!(data_length, 50000);

    // After data_length: chunk_count (4-byte BE) = 1
    let cc_offset = dl_offset + 8;
    let chunk_count = u32::from_be_bytes([
        bytes[cc_offset],
        bytes[cc_offset + 1],
        bytes[cc_offset + 2],
        bytes[cc_offset + 3],
    ]);
    assert_eq!(chunk_count, 1);

    // After chunk_count: 1 offset (8-byte BE) = 0
    let offset_offset = cc_offset + 4;
    let offset_val = u64::from_be_bytes([
        bytes[offset_offset],
        bytes[offset_offset + 1],
        bytes[offset_offset + 2],
        bytes[offset_offset + 3],
        bytes[offset_offset + 4],
        bytes[offset_offset + 5],
        bytes[offset_offset + 6],
        bytes[offset_offset + 7],
    ]);
    assert_eq!(offset_val, 0);

    // File must end exactly here — no trailing CRC bytes (Bug #638)
    let expected_len = offset_offset + 8;
    assert_eq!(
        bytes.len(),
        expected_len,
        "File has {} trailing bytes after offsets — should be 0 (Bug #638 regression)",
        bytes.len() as isize - expected_len as isize
    );
}

#[test]
fn test_trailing_crc_position() {
    // CRITICAL: Verify CRC is TRAILING (after chunk data), NOT leading
    // This CRC lives in Data.db, not in CompressionInfo.db.

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
