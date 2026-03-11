//! Compressed Data.db writer - writes compressed partition data
//!
//! Generates compressed Data.db files with per-chunk compression.
//! Supports LZ4, Snappy, Deflate, and Zstd compression algorithms.
//!
//! # Chunk Format
//!
//! CRITICAL: CRC32 is TRAILING (after chunk data), NOT leading!
//!
//! ```text
//! [compressed_chunk_0_bytes][crc32: 4 bytes BE]
//! [compressed_chunk_1_bytes][crc32: 4 bytes BE]
//! ...
//! ```
//!
//! # Compression Flow
//!
//! 1. Accumulate uncompressed data in buffer
//! 2. When buffer reaches chunk_size, compress and write
//! 3. Write CRC32 of compressed data (AFTER the compressed bytes)
//! 4. Track chunk offsets for CompressionInfo.db
//! 5. On finish(), flush remaining buffer
//!
//! References:
//! - Parser: `cqlite-core/src/storage/sstable/reader/compression.rs`
//! - Format docs: `docs/sstables-definitive-guide/chapters/09-compression.md`

use crate::error::{Error, Result};
use crate::storage::sstable::writer::compression_info_writer::{
    CompressionAlgorithm, CompressionMetadata,
};
use crc32fast::Hasher;
use std::io::Write;

/// Compressor trait for algorithm implementations
pub trait Compressor: Send + Sync {
    /// Compress input data
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Get the Cassandra algorithm name
    fn algorithm_name(&self) -> &'static str;

    /// Get the compression algorithm enum
    fn algorithm(&self) -> CompressionAlgorithm;
}

/// LZ4 compressor implementation
#[cfg(feature = "lz4")]
#[derive(Debug, Clone, Default)]
pub struct Lz4Compressor;

#[cfg(feature = "lz4")]
impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(lz4_flex::compress_prepend_size(data))
    }

    fn algorithm_name(&self) -> &'static str {
        "LZ4Compressor"
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Lz4
    }
}

/// Snappy compressor implementation
#[cfg(feature = "snappy")]
#[derive(Debug, Clone, Default)]
pub struct SnappyCompressor;

#[cfg(feature = "snappy")]
impl Compressor for SnappyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = snap::raw::Encoder::new();
        encoder
            .compress_vec(data)
            .map_err(|e| Error::Storage(format!("Snappy compression failed: {}", e)))
    }

    fn algorithm_name(&self) -> &'static str {
        "SnappyCompressor"
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Snappy
    }
}

/// Deflate compressor implementation
#[cfg(feature = "deflate")]
#[derive(Debug, Clone)]
pub struct DeflateCompressor {
    /// Compression level (0-9, higher = better compression, slower)
    level: u32,
}

#[cfg(feature = "deflate")]
impl Default for DeflateCompressor {
    fn default() -> Self {
        Self { level: 6 } // Default compression level
    }
}

#[cfg(feature = "deflate")]
impl DeflateCompressor {
    /// Create with custom compression level
    pub fn with_level(level: u32) -> Self {
        Self {
            level: level.min(9),
        }
    }
}

#[cfg(feature = "deflate")]
impl Compressor for DeflateCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::write::DeflateEncoder;
        use flate2::Compression;

        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(self.level));
        encoder
            .write_all(data)
            .map_err(|e| Error::Storage(format!("Deflate compression write failed: {}", e)))?;
        encoder
            .finish()
            .map_err(|e| Error::Storage(format!("Deflate compression finish failed: {}", e)))
    }

    fn algorithm_name(&self) -> &'static str {
        "DeflateCompressor"
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Deflate
    }
}

/// Zstd compressor implementation
#[cfg(feature = "zstd")]
#[derive(Debug, Clone)]
pub struct ZstdCompressor {
    /// Compression level (1-22, higher = better compression, slower)
    level: i32,
}

#[cfg(feature = "zstd")]
impl Default for ZstdCompressor {
    fn default() -> Self {
        Self { level: 3 } // Default compression level
    }
}

#[cfg(feature = "zstd")]
impl ZstdCompressor {
    /// Create with custom compression level
    pub fn with_level(level: i32) -> Self {
        Self {
            level: level.clamp(1, 22),
        }
    }
}

#[cfg(feature = "zstd")]
impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::encode_all(std::io::Cursor::new(data), self.level)
            .map_err(|e| Error::Storage(format!("Zstd compression failed: {}", e)))
    }

    fn algorithm_name(&self) -> &'static str {
        "ZstdCompressor"
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Zstd
    }
}

/// No-op compressor (passthrough)
#[derive(Debug, Clone, Default)]
pub struct NoopCompressor;

impl Compressor for NoopCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn algorithm_name(&self) -> &'static str {
        "NoopCompressor"
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::None
    }
}

/// Create a compressor for the given algorithm
pub fn create_compressor(algorithm: CompressionAlgorithm) -> Result<Box<dyn Compressor>> {
    match algorithm {
        #[cfg(feature = "lz4")]
        CompressionAlgorithm::Lz4 => Ok(Box::new(Lz4Compressor)),
        #[cfg(not(feature = "lz4"))]
        CompressionAlgorithm::Lz4 => Err(Error::InvalidInput(
            "LZ4 compression not enabled (feature 'lz4' required)".to_string(),
        )),

        #[cfg(feature = "snappy")]
        CompressionAlgorithm::Snappy => Ok(Box::new(SnappyCompressor)),
        #[cfg(not(feature = "snappy"))]
        CompressionAlgorithm::Snappy => Err(Error::InvalidInput(
            "Snappy compression not enabled (feature 'snappy' required)".to_string(),
        )),

        #[cfg(feature = "deflate")]
        CompressionAlgorithm::Deflate => Ok(Box::new(DeflateCompressor::default())),
        #[cfg(not(feature = "deflate"))]
        CompressionAlgorithm::Deflate => Err(Error::InvalidInput(
            "Deflate compression not enabled (feature 'deflate' required)".to_string(),
        )),

        #[cfg(feature = "zstd")]
        CompressionAlgorithm::Zstd => Ok(Box::new(ZstdCompressor::default())),
        #[cfg(not(feature = "zstd"))]
        CompressionAlgorithm::Zstd => Err(Error::InvalidInput(
            "Zstd compression not enabled (feature 'zstd' required)".to_string(),
        )),

        CompressionAlgorithm::None => Ok(Box::new(NoopCompressor)),
    }
}

/// Compressed Data.db writer
///
/// Accumulates uncompressed data in chunks, compresses each chunk,
/// and writes to the output with trailing CRC32 checksums.
pub struct CompressedDataWriter {
    /// Output buffer for compressed data
    output: Vec<u8>,
    /// Compressor implementation
    compressor: Box<dyn Compressor>,
    /// Uncompressed chunk size (default 65536)
    chunk_size: usize,
    /// Buffer for accumulating uncompressed data
    buffer: Vec<u8>,
    /// Byte offset of each compressed chunk (for CompressionInfo.db)
    chunk_offsets: Vec<u64>,
    /// CRC32 of each compressed chunk (for CompressionInfo.db)
    chunk_crcs: Vec<u32>,
    /// Current write position in output
    position: u64,
}

impl CompressedDataWriter {
    /// Default chunk size (64KB)
    pub const DEFAULT_CHUNK_SIZE: usize = 65536;

    /// Create a new compressed data writer
    pub fn new(compressor: Box<dyn Compressor>) -> Self {
        Self::with_chunk_size(compressor, Self::DEFAULT_CHUNK_SIZE)
    }

    /// Create with custom chunk size
    pub fn with_chunk_size(compressor: Box<dyn Compressor>, chunk_size: usize) -> Self {
        Self {
            output: Vec::new(),
            compressor,
            chunk_size,
            buffer: Vec::with_capacity(chunk_size),
            chunk_offsets: Vec::new(),
            chunk_crcs: Vec::new(),
            position: 0,
        }
    }

    /// Write uncompressed data
    ///
    /// Data is buffered until chunk_size is reached, then compressed and written.
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        let mut remaining = data;

        while !remaining.is_empty() {
            // Fill buffer up to chunk_size
            let space = self.chunk_size - self.buffer.len();
            let take = remaining.len().min(space);

            self.buffer.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];

            // If buffer is full, flush it
            if self.buffer.len() >= self.chunk_size {
                self.flush_chunk()?;
            }
        }

        Ok(())
    }

    /// Flush the current buffer as a compressed chunk
    fn flush_chunk(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Record chunk offset BEFORE writing
        self.chunk_offsets.push(self.position);

        // Compress the buffer
        let compressed = self.compressor.compress(&self.buffer)?;

        // Compute CRC32 of compressed data
        let mut hasher = Hasher::new();
        hasher.update(&compressed);
        let crc32 = hasher.finalize();

        self.chunk_crcs.push(crc32);

        // Write compressed data
        self.output.extend_from_slice(&compressed);
        self.position += compressed.len() as u64;

        // Write CRC32 (TRAILING - after compressed data)
        self.output.extend_from_slice(&crc32.to_be_bytes());
        self.position += 4;

        // Clear buffer for next chunk
        self.buffer.clear();

        Ok(())
    }

    /// Finish writing and return compression metadata
    ///
    /// Flushes any remaining data in the buffer.
    pub fn finish(mut self) -> Result<(Vec<u8>, CompressionMetadata)> {
        // Flush any remaining data
        self.flush_chunk()?;

        // Build compression metadata
        let mut metadata =
            CompressionMetadata::new(self.compressor.algorithm(), self.chunk_size as u32);

        metadata.set_compressed_length(self.position);

        for (offset, crc) in self.chunk_offsets.iter().zip(self.chunk_crcs.iter()) {
            metadata.add_chunk(*offset, Some(*crc));
        }

        Ok((self.output, metadata))
    }

    /// Get current output position
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Get number of chunks written so far
    pub fn chunk_count(&self) -> usize {
        self.chunk_offsets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_compressor() {
        let compressor = NoopCompressor;
        let data = b"Hello, World!";
        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed, data);
        assert_eq!(compressor.algorithm_name(), "NoopCompressor");
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::None);
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lz4_compressor() {
        let compressor = Lz4Compressor;
        let data = b"Hello, World! Hello, World! Hello, World!";
        let compressed = compressor.compress(data).unwrap();

        // LZ4 prepends size, so check it can be decompressed
        let decompressed = lz4_flex::decompress_size_prepended(&compressed).unwrap();
        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm_name(), "LZ4Compressor");
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    #[cfg(feature = "snappy")]
    fn test_snappy_compressor() {
        let compressor = SnappyCompressor;
        let data = b"Hello, World! Hello, World! Hello, World!";
        let compressed = compressor.compress(data).unwrap();

        // Verify it can be decompressed
        let mut decoder = snap::raw::Decoder::new();
        let decompressed = decoder.decompress_vec(&compressed).unwrap();
        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm_name(), "SnappyCompressor");
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Snappy);
    }

    #[test]
    #[cfg(feature = "deflate")]
    fn test_deflate_compressor() {
        use flate2::read::DeflateDecoder;
        use std::io::Read;

        let compressor = DeflateCompressor::default();
        let data = b"Hello, World! Hello, World! Hello, World!";
        let compressed = compressor.compress(data).unwrap();

        // Verify it can be decompressed
        let mut decoder = DeflateDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm_name(), "DeflateCompressor");
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Deflate);
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_zstd_compressor() {
        let compressor = ZstdCompressor::default();
        let data = b"Hello, World! Hello, World! Hello, World!";
        let compressed = compressor.compress(data).unwrap();

        // Verify it can be decompressed
        let decompressed = zstd::decode_all(std::io::Cursor::new(&compressed)).unwrap();
        assert_eq!(decompressed, data);
        assert_eq!(compressor.algorithm_name(), "ZstdCompressor");
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_compressed_data_writer_single_chunk() {
        let compressor = Box::new(NoopCompressor);
        let mut writer = CompressedDataWriter::with_chunk_size(compressor, 1024);

        // Write less than chunk size
        let data = b"Hello, World!";
        writer.write(data).unwrap();

        let (output, metadata) = writer.finish().unwrap();

        // Should have one chunk
        assert_eq!(metadata.chunk_count(), 1);
        assert_eq!(metadata.chunk_offsets, vec![0]);
        assert_eq!(metadata.chunk_length, 1024);
        assert_eq!(metadata.algorithm, CompressionAlgorithm::None);

        // Output should be: data + CRC32
        assert_eq!(output.len(), data.len() + 4);

        // Verify data
        assert_eq!(&output[..data.len()], data);

        // Verify CRC
        let mut hasher = Hasher::new();
        hasher.update(data);
        let expected_crc = hasher.finalize();
        let stored_crc = u32::from_be_bytes([
            output[data.len()],
            output[data.len() + 1],
            output[data.len() + 2],
            output[data.len() + 3],
        ]);
        assert_eq!(stored_crc, expected_crc);
    }

    #[test]
    fn test_compressed_data_writer_multiple_chunks() {
        let compressor = Box::new(NoopCompressor);
        let mut writer = CompressedDataWriter::with_chunk_size(compressor, 16);

        // Write 40 bytes - should create 3 chunks (16, 16, 8)
        let data = b"1234567890123456ABCDEFGHIJKLMNOPabcdefgh"; // 40 bytes total

        writer.write(data).unwrap();

        let (output, metadata) = writer.finish().unwrap();

        // Should have 3 chunks
        assert_eq!(metadata.chunk_count(), 3);

        // Chunk offsets: 0, 20 (16+4), 40 (20+16+4)
        assert_eq!(metadata.chunk_offsets[0], 0);
        assert_eq!(metadata.chunk_offsets[1], 20); // 16 bytes + 4 CRC
        assert_eq!(metadata.chunk_offsets[2], 40); // previous + 16 bytes + 4 CRC

        // Total output: 40 bytes data + 12 bytes CRC (3 * 4)
        assert_eq!(output.len(), 52);
    }

    #[test]
    fn test_compressed_data_writer_exact_chunk_boundary() {
        let compressor = Box::new(NoopCompressor);
        let mut writer = CompressedDataWriter::with_chunk_size(compressor, 16);

        // Write exactly 32 bytes - should create exactly 2 chunks
        let data = b"1234567890123456ABCDEFGHIJKLMNOP"; // 32 bytes total

        writer.write(data).unwrap();

        let (output, metadata) = writer.finish().unwrap();

        // Should have 2 chunks
        assert_eq!(metadata.chunk_count(), 2);

        // Total: 32 bytes data + 8 bytes CRC
        assert_eq!(output.len(), 40);
    }

    #[test]
    fn test_compressed_data_writer_empty() {
        let compressor = Box::new(NoopCompressor);
        let writer = CompressedDataWriter::with_chunk_size(compressor, 1024);

        let (output, metadata) = writer.finish().unwrap();

        // No chunks written
        assert_eq!(metadata.chunk_count(), 0);
        assert!(output.is_empty());
    }

    #[test]
    fn test_compressed_data_writer_incremental_writes() {
        let compressor = Box::new(NoopCompressor);
        let mut writer = CompressedDataWriter::with_chunk_size(compressor, 16);

        // Write in small increments
        writer.write(b"1234").unwrap();
        writer.write(b"5678").unwrap();
        writer.write(b"9012").unwrap();
        writer.write(b"3456").unwrap(); // Completes first chunk
        writer.write(b"ABCD").unwrap();

        let (output, metadata) = writer.finish().unwrap();

        // Should have 2 chunks (16 + 4 bytes)
        assert_eq!(metadata.chunk_count(), 2);
        assert_eq!(output.len(), 16 + 4 + 4 + 4); // 16 + CRC + 4 + CRC
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_compressed_data_writer_with_lz4() {
        let compressor = create_compressor(CompressionAlgorithm::Lz4).unwrap();
        let mut writer = CompressedDataWriter::with_chunk_size(compressor, 64);

        // Write compressible data
        let data = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        writer.write(data).unwrap();

        let (output, metadata) = writer.finish().unwrap();

        assert_eq!(metadata.chunk_count(), 1);
        assert_eq!(metadata.algorithm, CompressionAlgorithm::Lz4);

        // Compressed size should be less than original (64 bytes) + CRC (4 bytes)
        // LZ4 adds size prefix, but repeated 'A's should compress well
        assert!(
            output.len() < 68,
            "Expected compression, got {} bytes",
            output.len()
        );
    }

    #[test]
    fn test_create_compressor() {
        // NoopCompressor should always work
        let compressor = create_compressor(CompressionAlgorithm::None).unwrap();
        assert_eq!(compressor.algorithm(), CompressionAlgorithm::None);

        // LZ4 depends on feature flag
        #[cfg(feature = "lz4")]
        {
            let compressor = create_compressor(CompressionAlgorithm::Lz4).unwrap();
            assert_eq!(compressor.algorithm(), CompressionAlgorithm::Lz4);
        }

        #[cfg(not(feature = "lz4"))]
        {
            let result = create_compressor(CompressionAlgorithm::Lz4);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_crc_is_trailing() {
        // CRITICAL: Verify CRC is AFTER compressed data, not before
        let compressor = Box::new(NoopCompressor);
        let mut writer = CompressedDataWriter::with_chunk_size(compressor, 1024);

        let data = b"TestData";
        writer.write(data).unwrap();

        let (output, _metadata) = writer.finish().unwrap();

        // Data should come first
        assert_eq!(&output[..data.len()], data);

        // CRC should come after
        let crc_start = data.len();
        let mut hasher = Hasher::new();
        hasher.update(data);
        let expected_crc = hasher.finalize();

        let stored_crc = u32::from_be_bytes([
            output[crc_start],
            output[crc_start + 1],
            output[crc_start + 2],
            output[crc_start + 3],
        ]);

        assert_eq!(stored_crc, expected_crc, "CRC should match and be trailing");
    }
}
