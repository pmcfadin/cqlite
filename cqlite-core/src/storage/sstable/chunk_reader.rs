//! Chunk-based reader for NB format Data.db files
//!
//! NB format Data.db has NO header - starts directly with compressed data.
//! Each chunk is followed by a 4-byte CRC32 checksum (big-endian).
//!
//! Format: [chunk_0_bytes][CRC32(chunk_0)][chunk_1_bytes][CRC32(chunk_1)]...

use crate::storage::sstable::compression_info::CompressionInfo;
use crate::{Error, Result};
use std::io::{Read, Seek, SeekFrom};

/// Reader for chunked Data.db files (NB format)
///
/// This reader handles NB format Data.db files where:
/// - No magic number or header exists (file starts with compressed data)
/// - Each compressed chunk is followed by a 4-byte CRC32 checksum
/// - CRC32 uses Java's `java.util.zip.CRC32` algorithm (IEEE polynomial 0x04C11DB7)
/// - Checksums are stored in big-endian format
pub struct ChunkReader<R: Read + Seek> {
    reader: R,
    compression_info: CompressionInfo,
    total_file_size: u64,
}

impl<R: Read + Seek> ChunkReader<R> {
    /// Create new chunk reader
    ///
    /// # Arguments
    ///
    /// * `reader` - The underlying reader for Data.db file
    /// * `compression_info` - Parsed CompressionInfo containing chunk metadata
    /// * `total_file_size` - Total size of the Data.db file in bytes
    ///
    /// # Returns
    ///
    /// A new `ChunkReader` instance ready to read chunks
    pub fn new(reader: R, compression_info: CompressionInfo, total_file_size: u64) -> Self {
        Self {
            reader,
            compression_info,
            total_file_size,
        }
    }

    /// Read and validate a specific chunk by index
    ///
    /// This method:
    /// 1. Seeks to the chunk offset in Data.db
    /// 2. Reads the compressed chunk bytes
    /// 3. Reads the trailing 4-byte CRC32 checksum
    /// 4. Validates the CRC32 (fail-fast on mismatch)
    ///
    /// # Arguments
    ///
    /// * `chunk_index` - Zero-based index of the chunk to read
    ///
    /// # Returns
    ///
    /// Compressed chunk bytes ready for decompression
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Chunk index is invalid
    /// - I/O error occurs during reading
    /// - CRC32 validation fails
    pub fn read_chunk(&mut self, chunk_index: usize) -> Result<Vec<u8>> {
        // 1. Get chunk offset from CompressionInfo
        let offset = self
            .compression_info
            .compressed_chunk_offset(chunk_index)
            .ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Chunk {} not found in CompressionInfo (total chunks: {})",
                    chunk_index,
                    self.compression_info.chunk_offsets.len()
                ))
            })?;

        // 2. Calculate chunk size (distance to next chunk or end of file)
        // NOTE: This includes the trailing 4-byte CRC32
        let total_chunk_size = self
            .compression_info
            .compressed_chunk_size(chunk_index, self.total_file_size)
            .ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Cannot determine size for chunk {} (file_size={})",
                    chunk_index, self.total_file_size
                ))
            })?;

        // 3. Seek to chunk offset in Data.db
        self.reader.seek(SeekFrom::Start(offset)).map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to seek to chunk {} at offset 0x{:x}: {}",
                    chunk_index, offset, e
                ),
            ))
        })?;

        // 4. Read chunk bytes (NOT including trailing CRC32)
        // Subtract 4 bytes for trailing CRC
        if total_chunk_size < 4 {
            return Err(Error::InvalidFormat(format!(
                "Chunk {} size too small: {} bytes (minimum 4 for CRC)",
                chunk_index, total_chunk_size
            )));
        }

        let chunk_size = (total_chunk_size - 4) as usize;
        let mut chunk_data = vec![0u8; chunk_size];
        self.reader.read_exact(&mut chunk_data).map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to read chunk {} data ({} bytes at offset 0x{:x}): {}",
                    chunk_index, chunk_size, offset, e
                ),
            ))
        })?;

        // 5. Read trailing CRC32 (4 bytes, big-endian)
        let mut crc_bytes = [0u8; 4];
        self.reader.read_exact(&mut crc_bytes).map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to read CRC32 for chunk {} at offset 0x{:x}: {}",
                    chunk_index,
                    offset + chunk_size as u64,
                    e
                ),
            ))
        })?;
        let expected_crc = u32::from_be_bytes(crc_bytes);

        // 6. Compute CRC32 of chunk bytes using crc32fast (Java-compatible algorithm)
        let computed_crc = crc32fast::hash(&chunk_data);

        // 7. Validate CRC (fail-fast on mismatch)
        if computed_crc != expected_crc {
            return Err(Error::InvalidFormat(format!(
                "CRC32 mismatch for chunk {} at offset 0x{:x}: expected=0x{:08x}, computed=0x{:08x}, chunk_size={}",
                chunk_index, offset, expected_crc, computed_crc, chunk_size
            )));
        }

        Ok(chunk_data)
    }

    /// Read all chunks and validate CRC32 for each
    ///
    /// This is a convenience method that reads all chunks in sequential order.
    ///
    /// # Returns
    ///
    /// A vector of compressed chunk byte arrays, one per chunk
    ///
    /// # Errors
    ///
    /// Returns an error on the first chunk that fails to read or validate
    pub fn read_all_chunks(&mut self) -> Result<Vec<Vec<u8>>> {
        let chunk_count = self.compression_info.chunk_offsets.len();
        let mut chunks = Vec::with_capacity(chunk_count);

        for i in 0..chunk_count {
            let chunk = self.read_chunk(i)?;
            chunks.push(chunk);
        }

        Ok(chunks)
    }

    /// Get the number of chunks in this file
    pub fn chunk_count(&self) -> usize {
        self.compression_info.chunk_offsets.len()
    }

    /// Get the compression algorithm name
    pub fn compression_algorithm(&self) -> &str {
        &self.compression_info.algorithm
    }

    /// Get the uncompressed chunk size
    pub fn chunk_length(&self) -> u32 {
        self.compression_info.chunk_length
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_chunk_with_valid_crc() {
        // Create synthetic NB format data: [compressed_bytes][CRC32]
        let compressed_data = b"test compressed chunk data";
        let crc = crc32fast::hash(compressed_data);
        let crc_bytes = crc.to_be_bytes();

        let mut data = Vec::new();
        data.extend_from_slice(compressed_data);
        data.extend_from_slice(&crc_bytes);

        let total_size = data.len() as u64;

        // Create mock CompressionInfo
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: compressed_data.len() as u64,
            chunk_offsets: vec![0],
            crc32: None,
            chunk_crcs: vec![],
        };

        let cursor = Cursor::new(data);
        let mut reader = ChunkReader::new(cursor, compression_info, total_size);

        let result = reader.read_chunk(0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), compressed_data);
    }

    #[test]
    fn test_read_chunk_with_invalid_crc() {
        // Create data with WRONG CRC
        let compressed_data = b"test compressed chunk data";
        let wrong_crc = 0xDEADBEEFu32;
        let crc_bytes = wrong_crc.to_be_bytes();

        let mut data = Vec::new();
        data.extend_from_slice(compressed_data);
        data.extend_from_slice(&crc_bytes);

        let total_size = data.len() as u64;

        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: compressed_data.len() as u64,
            chunk_offsets: vec![0],
            crc32: None,
            chunk_crcs: vec![],
        };

        let cursor = Cursor::new(data);
        let mut reader = ChunkReader::new(cursor, compression_info, total_size);

        let result = reader.read_chunk(0);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("CRC32 mismatch"));
        assert!(err_msg.contains("0xdeadbeef")); // Verify expected CRC is in error
    }

    #[test]
    fn test_read_multiple_chunks() {
        // Create two chunks with valid CRCs
        let chunk1_data = b"first chunk data";
        let chunk1_crc = crc32fast::hash(chunk1_data);

        let chunk2_data = b"second chunk data with more content";
        let chunk2_crc = crc32fast::hash(chunk2_data);

        let mut data = Vec::new();
        data.extend_from_slice(chunk1_data);
        data.extend_from_slice(&chunk1_crc.to_be_bytes());
        data.extend_from_slice(chunk2_data);
        data.extend_from_slice(&chunk2_crc.to_be_bytes());

        let chunk1_size = chunk1_data.len() + 4;
        let total_size = data.len() as u64;

        let compression_info = CompressionInfo {
            algorithm: "SnappyCompressor".to_string(),
            chunk_length: 16384,
            data_length: (chunk1_data.len() + chunk2_data.len()) as u64,
            chunk_offsets: vec![0, chunk1_size as u64],
            crc32: None,
            chunk_crcs: vec![],
        };

        let cursor = Cursor::new(data);
        let mut reader = ChunkReader::new(cursor, compression_info, total_size);

        // Read first chunk
        let result1 = reader.read_chunk(0);
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), chunk1_data);

        // Read second chunk
        let result2 = reader.read_chunk(1);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), chunk2_data);
    }

    #[test]
    fn test_read_all_chunks() {
        // Create three chunks
        let chunks_data = vec![b"chunk1".to_vec(), b"chunk2data".to_vec(), b"c3".to_vec()];

        let mut data = Vec::new();
        let mut offsets = vec![0u64];

        for chunk in &chunks_data {
            let crc = crc32fast::hash(chunk);
            data.extend_from_slice(chunk);
            data.extend_from_slice(&crc.to_be_bytes());
            offsets.push(data.len() as u64);
        }
        offsets.pop(); // Remove last offset (beyond file)

        let total_size = data.len() as u64;
        let total_uncompressed = chunks_data.iter().map(|c| c.len()).sum::<usize>() as u64;

        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: total_uncompressed,
            chunk_offsets: offsets,
            crc32: None,
            chunk_crcs: vec![],
        };

        let cursor = Cursor::new(data);
        let mut reader = ChunkReader::new(cursor, compression_info, total_size);

        let result = reader.read_all_chunks();
        assert!(result.is_ok());

        let all_chunks = result.unwrap();
        assert_eq!(all_chunks.len(), 3);
        assert_eq!(all_chunks[0], chunks_data[0]);
        assert_eq!(all_chunks[1], chunks_data[1]);
        assert_eq!(all_chunks[2], chunks_data[2]);
    }

    #[test]
    fn test_invalid_chunk_index() {
        let compressed_data = b"test data";
        let crc = crc32fast::hash(compressed_data);

        let mut data = Vec::new();
        data.extend_from_slice(compressed_data);
        data.extend_from_slice(&crc.to_be_bytes());

        let total_size = data.len() as u64;

        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: compressed_data.len() as u64,
            chunk_offsets: vec![0],
            crc32: None,
            chunk_crcs: vec![],
        };

        let cursor = Cursor::new(data);
        let mut reader = ChunkReader::new(cursor, compression_info, total_size);

        // Try to read chunk that doesn't exist
        let result = reader.read_chunk(1);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Chunk 1 not found"));
    }

    #[test]
    fn test_chunk_size_too_small() {
        // Create a chunk that's smaller than the CRC (invalid)
        let data = vec![0xAB, 0xCD]; // Only 2 bytes, need at least 4

        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 0,
            chunk_offsets: vec![0],
            crc32: None,
            chunk_crcs: vec![],
        };

        let cursor = Cursor::new(data.clone());
        let total_size = data.len() as u64;
        let mut reader = ChunkReader::new(cursor, compression_info, total_size);

        let result = reader.read_chunk(0);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("size too small"));
    }

    #[test]
    fn test_accessor_methods() {
        let compression_info = CompressionInfo {
            algorithm: "SnappyCompressor".to_string(),
            chunk_length: 32768,
            data_length: 65536,
            chunk_offsets: vec![0, 16384, 32768],
            crc32: None,
            chunk_crcs: vec![],
        };

        let cursor = Cursor::new(vec![]);
        let reader = ChunkReader::new(cursor, compression_info, 0);

        assert_eq!(reader.chunk_count(), 3);
        assert_eq!(reader.compression_algorithm(), "SnappyCompressor");
        assert_eq!(reader.chunk_length(), 32768);
    }
}
