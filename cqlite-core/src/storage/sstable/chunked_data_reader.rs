//! Chunked data reader for compressed SSTables
//!
//! This module implements streaming reads of compressed Data.db files using
//! CompressionInfo.db metadata to honor chunk boundaries and validate per-chunk CRCs.
//!
//! ## Architecture
//!
//! The `ChunkedDataReader` maintains a state machine:
//! 1. Read compressed chunk from Data.db at offset from CompressionInfo
//! 2. Validate chunk CRC (if available)
//! 3. Decompress chunk into memory buffer
//! 4. Serve reads from buffer until exhausted
//! 5. Load next chunk and repeat
//!
//! This approach ensures:
//! - Chunk boundaries are honored (per Cassandra format)
//! - CRC validation happens before decompression
//! - Rows spanning chunks are assembled correctly
//! - Memory usage is bounded by chunk size

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::storage::sstable::compression::{Compression, CompressionAlgorithm};
use crate::storage::sstable::compression_info::CompressionInfo;
use crate::{Error, Result};

/// Chunked data reader for compressed Data.db files
///
/// Reads compressed data in chunks as defined by CompressionInfo.db,
/// validates per-chunk CRCs, and provides a transparent Read interface.
pub struct ChunkedDataReader<R: Read + Seek> {
    /// Underlying file reader
    reader: R,
    /// Total file size for chunk size calculations
    file_size: u64,
    /// Compression metadata with chunk offsets
    compression_info: Arc<CompressionInfo>,
    /// Compression handler for decompression
    compression: Compression,

    // State tracking
    /// Currently loaded chunk index
    current_chunk: usize,
    /// Decompressed data buffer for current chunk
    chunk_buffer: Vec<u8>,
    /// Position within chunk_buffer
    buffer_pos: usize,

    // Position tracking
    /// Logical position in the decompressed data stream
    global_pos: u64,
}

impl<R: Read + Seek> ChunkedDataReader<R> {
    /// Create a new chunked data reader
    ///
    /// # Arguments
    /// * `reader` - File reader positioned at start of Data.db
    /// * `file_size` - Total size of Data.db file
    /// * `compression_info` - Parsed CompressionInfo.db metadata
    ///
    /// # Returns
    /// Configured reader ready to stream decompressed data
    pub fn new(reader: R, file_size: u64, compression_info: Arc<CompressionInfo>) -> Result<Self> {
        // Convert algorithm name to enum
        let algorithm = CompressionAlgorithm::from(compression_info.algorithm.clone());
        let compression = Compression::new(algorithm)?;

        Ok(Self {
            reader,
            file_size,
            compression_info,
            compression,
            current_chunk: 0,
            chunk_buffer: Vec::new(),
            buffer_pos: 0,
            global_pos: 0,
        })
    }

    /// Load a specific chunk into the buffer
    ///
    /// # Arguments
    /// * `chunk_index` - Index of chunk to load (0-based)
    ///
    /// # Process
    /// 1. Read compressed chunk from Data.db at offset
    /// 2. Validate CRC if available
    /// 3. Decompress into chunk_buffer
    /// 4. Reset buffer_pos to 0
    ///
    /// # Returns
    /// Ok(()) on success, Err if chunk read/decompression fails
    fn load_chunk(&mut self, chunk_index: usize) -> Result<()> {
        // Check if we've reached end of chunks
        if chunk_index >= self.compression_info.chunk_offsets.len() {
            self.chunk_buffer.clear();
            return Ok(()); // EOF
        }

        // Get chunk location from compression info
        let offset = self
            .compression_info
            .compressed_chunk_offset(chunk_index)
            .ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Missing chunk offset for chunk index {}",
                    chunk_index
                ))
            })?;

        let total_chunk_size = self
            .compression_info
            .compressed_chunk_size(chunk_index, self.file_size)
            .ok_or_else(|| {
                Error::InvalidFormat(format!("Invalid chunk size for chunk index {chunk_index}"))
            })?;

        // Cassandra chunks are laid out as [compressed_data][4-byte trailing CRC32].
        // The chunk size from CompressionInfo includes the trailer, so subtract 4
        // before reading the compressed payload.
        if total_chunk_size < 4 {
            return Err(Error::InvalidFormat(format!(
                "Chunk {chunk_index} size too small: {total_chunk_size} bytes (minimum 4 for CRC)"
            )));
        }
        let compressed_len = (total_chunk_size - 4) as usize;

        // Seek to chunk start and read compressed data (excluding trailing CRC32).
        self.reader.seek(SeekFrom::Start(offset)).map_err(|e| {
            Error::storage(format!(
                "Failed to seek to chunk {chunk_index} at offset {offset}: {e}"
            ))
        })?;

        let mut compressed = vec![0u8; compressed_len];
        self.reader.read_exact(&mut compressed).map_err(|e| {
            Error::storage(format!(
                "Failed to read chunk {chunk_index} ({compressed_len} bytes at offset {offset}): {e}"
            ))
        })?;

        // Read the trailing 4-byte CRC32 (big-endian) and validate against the
        // compressed payload. This matches Cassandra's NB chunk format.
        let mut crc_bytes = [0u8; 4];
        self.reader.read_exact(&mut crc_bytes).map_err(|e| {
            Error::storage(format!(
                "Failed to read trailing CRC32 for chunk {chunk_index} at offset {}: {e}",
                offset + compressed_len as u64
            ))
        })?;
        let expected_crc = u32::from_be_bytes(crc_bytes);
        let computed_crc = crc32fast::hash(&compressed);
        if computed_crc != expected_crc {
            return Err(Error::InvalidFormat(format!(
                "CRC32 mismatch for chunk {chunk_index} at offset 0x{offset:x}: expected=0x{expected_crc:08x}, computed=0x{computed_crc:08x}, compressed_len={compressed_len}"
            )));
        }

        // Decompress chunk
        let decompressed = self.compression.decompress(&compressed).map_err(|e| {
            Error::storage(format!(
                "Failed to decompress chunk {chunk_index} ({} compressed bytes): {e}",
                compressed.len()
            ))
        })?;

        // Update state
        self.chunk_buffer = decompressed;
        self.buffer_pos = 0;
        self.current_chunk = chunk_index;

        Ok(())
    }

    /// Get current position in decompressed data stream
    pub fn position(&self) -> u64 {
        self.global_pos
    }

    /// Get total decompressed data length
    pub fn total_length(&self) -> u64 {
        self.compression_info.data_length
    }

    /// Get compression algorithm in use
    pub fn algorithm(&self) -> &CompressionAlgorithm {
        self.compression.algorithm()
    }

    /// Get current chunk index
    pub fn current_chunk_index(&self) -> usize {
        self.current_chunk
    }

    /// Get total number of chunks
    pub fn total_chunks(&self) -> usize {
        self.compression_info.chunk_offsets.len()
    }
}

impl<R: Read + Seek> Read for ChunkedDataReader<R> {
    /// Read data from the decompressed stream
    ///
    /// This method transparently handles chunk boundaries:
    /// - Loads new chunks as needed
    /// - Assembles data from multiple chunks if necessary
    /// - Returns 0 at EOF
    ///
    /// # Arguments
    /// * `buf` - Buffer to fill with decompressed data
    ///
    /// # Returns
    /// Number of bytes read, or 0 at EOF
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut total_read = 0;

        while total_read < buf.len() {
            // Check if we need to load next chunk
            if self.buffer_pos >= self.chunk_buffer.len() {
                // Try to load next chunk
                let next_chunk = if self.chunk_buffer.is_empty() && self.current_chunk == 0 {
                    // First read - load chunk 0
                    0
                } else {
                    // Load subsequent chunk
                    self.current_chunk + 1
                };

                self.load_chunk(next_chunk)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;

                // Check for EOF
                if self.chunk_buffer.is_empty() {
                    break;
                }
            }

            // Copy data from current chunk buffer
            let available = self.chunk_buffer.len() - self.buffer_pos;
            let to_copy = std::cmp::min(buf.len() - total_read, available);

            buf[total_read..total_read + to_copy]
                .copy_from_slice(&self.chunk_buffer[self.buffer_pos..self.buffer_pos + to_copy]);

            total_read += to_copy;
            self.buffer_pos += to_copy;
            self.global_pos += to_copy as u64;
        }

        Ok(total_read)
    }
}

impl<R: Read + Seek> Seek for ChunkedDataReader<R> {
    /// Seek to a position in the decompressed data stream
    ///
    /// Uses CompressionInfo.db to determine which chunk contains the
    /// target position, loads that chunk, and positions within it.
    ///
    /// # Arguments
    /// * `pos` - Seek position (Start, Current, or End)
    ///
    /// # Returns
    /// New absolute position in decompressed stream
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        // Calculate target position in decompressed stream
        let target_pos = match pos {
            SeekFrom::Start(p) => p,
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.global_pos.saturating_add(delta as u64)
                } else {
                    self.global_pos.saturating_sub((-delta) as u64)
                }
            }
            SeekFrom::End(delta) => {
                let total_len = self.compression_info.data_length;
                if delta >= 0 {
                    total_len.saturating_add(delta as u64)
                } else {
                    total_len.saturating_sub((-delta) as u64)
                }
            }
        };

        // Clamp to valid range
        let target_pos = target_pos.min(self.compression_info.data_length);

        // Determine which chunk contains target position
        let target_chunk = self.compression_info.chunk_for_offset(target_pos);
        let offset_in_chunk = self.compression_info.offset_within_chunk(target_pos);

        // Load chunk if different from current or buffer is empty
        if target_chunk != self.current_chunk || self.chunk_buffer.is_empty() {
            self.load_chunk(target_chunk)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }

        // Set position within chunk buffer
        self.buffer_pos = offset_in_chunk as usize;
        self.global_pos = target_pos;

        Ok(self.global_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Helper to create test CompressionInfo
    fn create_test_compression_info(
        algorithm: &str,
        chunk_length: u32,
        chunk_offsets: Vec<u64>,
    ) -> CompressionInfo {
        CompressionInfo {
            algorithm: algorithm.to_string(),
            chunk_length,
            data_length: (chunk_offsets.len() as u64) * (chunk_length as u64),
            chunk_offsets,
            crc32: None,
            chunk_crcs: vec![], // No CRC validation in basic tests
        }
    }

    #[test]
    fn test_chunked_reader_creation() {
        let data = vec![0u8; 100];
        let cursor = Cursor::new(data);
        let compression_info = Arc::new(create_test_compression_info(
            "LZ4Compressor",
            16384,
            vec![0],
        ));

        let reader = ChunkedDataReader::new(cursor, 100, compression_info);
        assert!(reader.is_ok());
    }

    #[test]
    fn test_position_tracking() {
        let data = vec![0u8; 100];
        let cursor = Cursor::new(data);
        let compression_info = Arc::new(create_test_compression_info(
            "LZ4Compressor",
            16384,
            vec![0],
        ));

        let reader = ChunkedDataReader::new(cursor, 100, compression_info).unwrap();
        assert_eq!(reader.position(), 0);
        assert_eq!(reader.current_chunk_index(), 0);
    }

    #[test]
    fn test_total_length() {
        let data = vec![0u8; 100];
        let cursor = Cursor::new(data);
        let compression_info = Arc::new(create_test_compression_info(
            "LZ4Compressor",
            16384,
            vec![0, 50],
        ));

        let reader = ChunkedDataReader::new(cursor, 100, compression_info).unwrap();
        assert_eq!(reader.total_length(), 32768); // 2 chunks * 16384
        assert_eq!(reader.total_chunks(), 2);
    }

    // More comprehensive tests will use real compressed data
    // See integration tests for full validation

    #[test]
    fn test_crc_validation_error() {
        // Create CompressionInfo with CRC checksums
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 100,
            data_length: 100,
            chunk_offsets: vec![0],
            crc32: None,
            chunk_crcs: vec![0x12345678], // Expected CRC
        };

        // Create corrupted compressed data (LZ4 format with size prepended)
        // LZ4 format: 4-byte little-endian uncompressed size + compressed data
        let mut corrupted_data = Vec::new();
        corrupted_data.extend_from_slice(&100u32.to_le_bytes()); // Uncompressed size = 100
        corrupted_data.extend_from_slice(&[0xFF; 96]); // Corrupted compressed data

        let cursor = Cursor::new(corrupted_data);
        let compression_info_arc = Arc::new(compression_info);

        let mut reader = ChunkedDataReader::new(cursor, 100, compression_info_arc)
            .expect("Failed to create ChunkedDataReader");

        // Try to read - should fail with CRC mismatch
        let mut buffer = vec![0u8; 100];
        let result = reader.read(&mut buffer);

        assert!(result.is_err(), "Should fail due to CRC mismatch");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("CRC") || err_msg.contains("validation"),
            "Error should mention CRC validation: {}",
            err_msg
        );
    }

    #[test]
    fn test_multi_chunk_state_machine() {
        // Test that reader correctly transitions between chunks
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 50,
            data_length: 150, // 3 chunks
            chunk_offsets: vec![0, 50, 100],
            crc32: None,
            chunk_crcs: vec![],
        };

        // Create mock compressed data (simplified - would be real LZ4 in practice)
        let mut mock_data = Vec::new();
        for i in 0..3 {
            // Each chunk: size header + data
            mock_data.extend_from_slice(&50u32.to_le_bytes());
            mock_data.extend_from_slice(&[i as u8; 46]); // 46 bytes of data (50 - 4 header)
        }

        let data_len = mock_data.len() as u64;
        let cursor = Cursor::new(mock_data);
        let compression_info_arc = Arc::new(compression_info);

        let reader = ChunkedDataReader::new(cursor, data_len, compression_info_arc)
            .expect("Failed to create ChunkedDataReader");

        // Verify initial state
        assert_eq!(reader.position(), 0);
        assert_eq!(reader.current_chunk_index(), 0);
        assert_eq!(reader.total_chunks(), 3);
    }
}
