//! CompressionInfo.db writer - writes compression metadata
//!
//! Generates the CompressionInfo.db component that describes how Data.db chunks
//! are compressed. This file is required for readers to decompress Data.db.
//!
//! # Binary Format (Cassandra 5.0 NB)
//!
//! ```text
//! [u16 BE: algorithm_name_length]    ← Length of algorithm name
//! [bytes: algorithm_name]            ← "LZ4Compressor", "SnappyCompressor", etc.
//! [4 bytes: padding]                 ← Fixed 0x00000000 padding
//! [u32 BE: chunk_length]             ← Uncompressed chunk size (typically 65536)
//! [u32 BE: options/flags]            ← Options field (0x7FFFFFFF typical)
//! [u64 BE: compressed_data_length]   ← Total compressed Data.db size
//! [u32 BE: chunk_count]              ← Number of chunks
//! [u64 BE * chunk_count: offsets]    ← Byte offset of each chunk in Data.db
//! [u32 BE * chunk_count: crcs]       ← CRC32 of each compressed chunk (optional)
//! [u32 BE: metadata_crc]             ← CRC32 of all preceding bytes
//! ```
//!
//! References:
//! - Parser: `cqlite-core/src/storage/sstable/compression_info.rs`
//! - Format docs: `docs/sstables-definitive-guide/chapters/09-compression.md`

use crate::error::{Error, Result};
use crc32fast::Hasher;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Compression algorithm identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// LZ4 compression (fast, moderate ratio)
    Lz4,
    /// Snappy compression (very fast, lower ratio)
    Snappy,
    /// Deflate/zlib compression (slower, better ratio)
    Deflate,
    /// Zstd compression (balanced speed/ratio)
    Zstd,
    /// No compression (passthrough)
    None,
}

impl CompressionAlgorithm {
    /// Get the Cassandra algorithm name string
    pub fn cassandra_name(&self) -> &'static str {
        match self {
            CompressionAlgorithm::Lz4 => "LZ4Compressor",
            CompressionAlgorithm::Snappy => "SnappyCompressor",
            CompressionAlgorithm::Deflate => "DeflateCompressor",
            CompressionAlgorithm::Zstd => "ZstdCompressor",
            CompressionAlgorithm::None => "NoopCompressor",
        }
    }

    /// Parse from Cassandra algorithm name
    pub fn from_cassandra_name(name: &str) -> Option<Self> {
        match name {
            "LZ4Compressor" | "org.apache.cassandra.io.compress.LZ4Compressor" => {
                Some(CompressionAlgorithm::Lz4)
            }
            "SnappyCompressor" | "org.apache.cassandra.io.compress.SnappyCompressor" => {
                Some(CompressionAlgorithm::Snappy)
            }
            "DeflateCompressor" | "org.apache.cassandra.io.compress.DeflateCompressor" => {
                Some(CompressionAlgorithm::Deflate)
            }
            "ZstdCompressor" | "org.apache.cassandra.io.compress.ZstdCompressor" => {
                Some(CompressionAlgorithm::Zstd)
            }
            "NoopCompressor" | "org.apache.cassandra.io.compress.NoopCompressor" => {
                Some(CompressionAlgorithm::None)
            }
            _ => None,
        }
    }
}

/// Metadata about compressed Data.db
///
/// Collected during compression and written to CompressionInfo.db
#[derive(Debug, Clone)]
pub struct CompressionMetadata {
    /// Compression algorithm used
    pub algorithm: CompressionAlgorithm,
    /// Uncompressed chunk size in bytes (typically 65536)
    pub chunk_length: u32,
    /// Total compressed data length (Data.db file size)
    pub compressed_length: u64,
    /// Byte offset of each compressed chunk in Data.db
    pub chunk_offsets: Vec<u64>,
    /// CRC32 checksum of each compressed chunk (optional)
    pub chunk_crcs: Vec<u32>,
}

impl CompressionMetadata {
    /// Create new compression metadata
    pub fn new(algorithm: CompressionAlgorithm, chunk_length: u32) -> Self {
        Self {
            algorithm,
            chunk_length,
            compressed_length: 0,
            chunk_offsets: Vec::new(),
            chunk_crcs: Vec::new(),
        }
    }

    /// Add a new chunk offset and optional CRC
    pub fn add_chunk(&mut self, offset: u64, crc: Option<u32>) {
        self.chunk_offsets.push(offset);
        if let Some(crc_value) = crc {
            self.chunk_crcs.push(crc_value);
        }
    }

    /// Set the total compressed data length
    pub fn set_compressed_length(&mut self, length: u64) {
        self.compressed_length = length;
    }

    /// Get the number of chunks
    pub fn chunk_count(&self) -> usize {
        self.chunk_offsets.len()
    }
}

/// CompressionInfo.db file writer
///
/// Writes compression metadata to disk in Cassandra's binary format.
#[derive(Debug)]
pub struct CompressionInfoWriter {
    /// Output file path
    path: PathBuf,
}

impl CompressionInfoWriter {
    /// Create a new CompressionInfo.db writer
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Write compression metadata to file
    ///
    /// # Arguments
    /// * `metadata` - Compression metadata collected during Data.db writing
    ///
    /// # Returns
    /// Ok(()) on success, or error if write fails
    pub fn write(&self, metadata: &CompressionMetadata) -> Result<()> {
        let file = File::create(&self.path).map_err(|e| {
            Error::Storage(format!(
                "Failed to create CompressionInfo.db at {}: {}",
                self.path.display(),
                e
            ))
        })?;
        let mut writer = BufWriter::new(file);

        // Build the binary content first to compute CRC
        let content = self.build_content(metadata)?;

        // Compute CRC32 of content (excluding the CRC itself)
        let mut hasher = Hasher::new();
        hasher.update(&content);
        let crc32 = hasher.finalize();

        // Write content + CRC
        writer.write_all(&content).map_err(|e| {
            Error::Storage(format!("Failed to write CompressionInfo.db content: {}", e))
        })?;

        // Write trailing CRC32
        writer.write_all(&crc32.to_be_bytes()).map_err(|e| {
            Error::Storage(format!("Failed to write CompressionInfo.db CRC: {}", e))
        })?;

        writer
            .flush()
            .map_err(|e| Error::Storage(format!("Failed to flush CompressionInfo.db: {}", e)))?;

        Ok(())
    }

    /// Build the binary content (everything before the trailing CRC)
    fn build_content(&self, metadata: &CompressionMetadata) -> Result<Vec<u8>> {
        let mut content = Vec::new();

        // Algorithm name
        let algorithm_name = metadata.algorithm.cassandra_name();
        let name_bytes = algorithm_name.as_bytes();

        if name_bytes.len() > u16::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Algorithm name too long: {} bytes (max {})",
                name_bytes.len(),
                u16::MAX
            )));
        }

        // Algorithm name length (u16 BE)
        content.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());

        // Algorithm name bytes
        content.extend_from_slice(name_bytes);

        // Fixed 4-byte padding (0x00000000) - ensures 8-byte alignment for chunk_length field
        content.extend_from_slice(&[0u8; 4]);

        // Chunk length (u32 BE)
        content.extend_from_slice(&metadata.chunk_length.to_be_bytes());

        // Options/flags field (u32 BE) - typically 0x7FFFFFFF
        content.extend_from_slice(&0x7FFFFFFFu32.to_be_bytes());

        // Compressed data length (u64 BE)
        content.extend_from_slice(&metadata.compressed_length.to_be_bytes());

        // Chunk count (u32 BE)
        if metadata.chunk_offsets.len() > u32::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Too many chunks: {} (max {})",
                metadata.chunk_offsets.len(),
                u32::MAX
            )));
        }
        content.extend_from_slice(&(metadata.chunk_offsets.len() as u32).to_be_bytes());

        // Chunk offsets (u64 BE each)
        for offset in &metadata.chunk_offsets {
            content.extend_from_slice(&offset.to_be_bytes());
        }

        // Chunk CRCs (u32 BE each) - only if we have them
        if !metadata.chunk_crcs.is_empty() {
            if metadata.chunk_crcs.len() != metadata.chunk_offsets.len() {
                return Err(Error::InvalidInput(format!(
                    "Chunk CRC count ({}) doesn't match chunk count ({})",
                    metadata.chunk_crcs.len(),
                    metadata.chunk_offsets.len()
                )));
            }

            for crc in &metadata.chunk_crcs {
                content.extend_from_slice(&crc.to_be_bytes());
            }
        }

        Ok(content)
    }

    /// Build content to a buffer instead of file (for testing)
    pub fn build_to_vec(&self, metadata: &CompressionMetadata) -> Result<Vec<u8>> {
        let content = self.build_content(metadata)?;

        // Compute CRC32
        let mut hasher = Hasher::new();
        hasher.update(&content);
        let crc32 = hasher.finalize();

        // Combine content + CRC
        let mut result = content;
        result.extend_from_slice(&crc32.to_be_bytes());

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_compression_algorithm_names() {
        assert_eq!(CompressionAlgorithm::Lz4.cassandra_name(), "LZ4Compressor");
        assert_eq!(
            CompressionAlgorithm::Snappy.cassandra_name(),
            "SnappyCompressor"
        );
        assert_eq!(
            CompressionAlgorithm::Deflate.cassandra_name(),
            "DeflateCompressor"
        );
        assert_eq!(
            CompressionAlgorithm::Zstd.cassandra_name(),
            "ZstdCompressor"
        );
        assert_eq!(
            CompressionAlgorithm::None.cassandra_name(),
            "NoopCompressor"
        );
    }

    #[test]
    fn test_compression_algorithm_from_name() {
        assert_eq!(
            CompressionAlgorithm::from_cassandra_name("LZ4Compressor"),
            Some(CompressionAlgorithm::Lz4)
        );
        assert_eq!(
            CompressionAlgorithm::from_cassandra_name(
                "org.apache.cassandra.io.compress.LZ4Compressor"
            ),
            Some(CompressionAlgorithm::Lz4)
        );
        assert_eq!(
            CompressionAlgorithm::from_cassandra_name("UnknownCompressor"),
            None
        );
    }

    #[test]
    fn test_compression_metadata_new() {
        let metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 65536);
        assert_eq!(metadata.algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(metadata.chunk_length, 65536);
        assert_eq!(metadata.compressed_length, 0);
        assert!(metadata.chunk_offsets.is_empty());
        assert!(metadata.chunk_crcs.is_empty());
    }

    #[test]
    fn test_compression_metadata_add_chunk() {
        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 65536);

        metadata.add_chunk(0, Some(0x12345678));
        metadata.add_chunk(8192, Some(0xABCDEF01));

        assert_eq!(metadata.chunk_count(), 2);
        assert_eq!(metadata.chunk_offsets, vec![0, 8192]);
        assert_eq!(metadata.chunk_crcs, vec![0x12345678, 0xABCDEF01]);
    }

    #[test]
    fn test_compression_info_writer_build_content() {
        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 65536);
        metadata.add_chunk(0, None);
        metadata.add_chunk(8192, None);
        metadata.set_compressed_length(16000);

        let writer = CompressionInfoWriter::new(PathBuf::from("/tmp/test"));
        let content = writer.build_content(&metadata).unwrap();

        // Verify structure:
        // 2 bytes: name length (13 for "LZ4Compressor")
        // 13 bytes: algorithm name
        // 4 bytes: padding
        // 4 bytes: chunk length
        // 4 bytes: options
        // 8 bytes: compressed length
        // 4 bytes: chunk count
        // 16 bytes: 2 chunk offsets (8 bytes each)
        // Total: 55 bytes (no CRCs)

        assert_eq!(content.len(), 55);

        // Check algorithm name length
        assert_eq!(&content[0..2], &[0x00, 0x0D]); // 13 in BE

        // Check algorithm name
        assert_eq!(&content[2..15], b"LZ4Compressor");

        // Check padding
        assert_eq!(&content[15..19], &[0x00, 0x00, 0x00, 0x00]);

        // Check chunk length (65536 = 0x00010000)
        assert_eq!(&content[19..23], &[0x00, 0x01, 0x00, 0x00]);

        // Check options (0x7FFFFFFF)
        assert_eq!(&content[23..27], &[0x7F, 0xFF, 0xFF, 0xFF]);

        // Check compressed length (16000 = 0x3E80)
        assert_eq!(
            &content[27..35],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3E, 0x80]
        );

        // Check chunk count (2)
        assert_eq!(&content[35..39], &[0x00, 0x00, 0x00, 0x02]);

        // Check first offset (0)
        assert_eq!(
            &content[39..47],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );

        // Check second offset (8192 = 0x2000)
        assert_eq!(
            &content[47..55],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00]
        );
    }

    #[test]
    fn test_compression_info_writer_with_crcs() {
        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Snappy, 16384);
        metadata.add_chunk(0, Some(0x11223344));
        metadata.add_chunk(4096, Some(0x55667788));
        metadata.set_compressed_length(8000);

        let writer = CompressionInfoWriter::new(PathBuf::from("/tmp/test"));
        let content = writer.build_content(&metadata).unwrap();

        // With CRCs: content should be longer
        // 2 + 16 (SnappyCompressor) + 4 + 4 + 4 + 8 + 4 + 16 + 8 = 66 bytes
        assert_eq!(content.len(), 66);

        // Check CRCs at end
        // First CRC: 0x11223344
        assert_eq!(&content[58..62], &[0x11, 0x22, 0x33, 0x44]);
        // Second CRC: 0x55667788
        assert_eq!(&content[62..66], &[0x55, 0x66, 0x77, 0x88]);
    }

    #[test]
    fn test_compression_info_writer_write_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("nb-1-big-CompressionInfo.db");

        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 65536);
        metadata.add_chunk(0, Some(0xDEADBEEF));
        metadata.set_compressed_length(32768);

        let writer = CompressionInfoWriter::new(path.clone());
        writer.write(&metadata).unwrap();

        // Verify file was created
        assert!(path.exists());

        // Read and verify content
        let bytes = std::fs::read(&path).unwrap();

        // Should have content + 4-byte CRC
        assert!(bytes.len() > 4);

        // Verify CRC32 is valid
        let content_len = bytes.len() - 4;
        let stored_crc = u32::from_be_bytes([
            bytes[content_len],
            bytes[content_len + 1],
            bytes[content_len + 2],
            bytes[content_len + 3],
        ]);

        let mut hasher = Hasher::new();
        hasher.update(&bytes[..content_len]);
        let computed_crc = hasher.finalize();

        assert_eq!(stored_crc, computed_crc, "CRC mismatch");
    }

    #[test]
    fn test_compression_info_writer_build_to_vec() {
        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Deflate, 32768);
        metadata.add_chunk(0, None);
        metadata.set_compressed_length(16384);

        let writer = CompressionInfoWriter::new(PathBuf::from("/tmp/test"));
        let bytes = writer.build_to_vec(&metadata).unwrap();

        // Verify CRC is appended
        assert!(bytes.len() > 4);

        // Verify CRC is correct
        let content_len = bytes.len() - 4;
        let stored_crc = u32::from_be_bytes([
            bytes[content_len],
            bytes[content_len + 1],
            bytes[content_len + 2],
            bytes[content_len + 3],
        ]);

        let mut hasher = Hasher::new();
        hasher.update(&bytes[..content_len]);
        let computed_crc = hasher.finalize();

        assert_eq!(stored_crc, computed_crc);
    }

    #[test]
    fn test_compression_metadata_crc_count_mismatch() {
        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 65536);
        metadata.add_chunk(0, Some(0x11111111));
        metadata.add_chunk(8192, None); // No CRC for this chunk

        // CRC count (1) doesn't match chunk count (2)
        // This happens because add_chunk only pushes CRC if Some

        let writer = CompressionInfoWriter::new(PathBuf::from("/tmp/test"));
        let result = writer.build_content(&metadata);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("CRC count"));
    }
}
