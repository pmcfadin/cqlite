//! CompressionInfo.db file parser for SSTable compression metadata
//!
//! This module provides bulletproof parsing of Cassandra's CompressionInfo.db files
//! which contain the metadata needed to decompress chunks from compressed Data.db files.

use crate::{Error, Result};
use crc32fast::Hasher;
use std::io::{Cursor, Read, Seek, SeekFrom};

/// CompressionInfo.db file content parsed from binary format
#[derive(Debug, Clone)]
pub struct CompressionInfo {
    /// Compression algorithm name (e.g., "LZ4Compressor")
    pub algorithm: String,
    /// Size of uncompressed data chunks
    pub chunk_length: u32,
    /// Total uncompressed data length
    pub data_length: u64,
    /// List of compressed chunk offsets in Data.db file
    pub chunk_offsets: Vec<u64>,
    /// CRC32 checksum of the metadata (if present)
    pub crc32: Option<u32>,
    /// Per-chunk CRC32 checksums for validation
    pub chunk_crcs: Vec<u32>,
}

impl CompressionInfo {
    /// Parse CompressionInfo.db file from binary data with CRC validation
    ///
    /// Binary format (observed from hex analysis):
    /// - 2 bytes: algorithm name length (big-endian)
    /// - N bytes: algorithm name string
    /// - 4 bytes: chunk length (default 16384 = 0x4000)
    /// - 8 bytes: total data length
    /// - 4 bytes: number of chunks
    /// - N * 8 bytes: chunk offsets (8 bytes each)
    /// - 4 bytes: CRC32 checksum (optional, at end of file)
    pub fn parse(data: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(data);
        let mut hasher = Hasher::new();

        // Calculate CRC32 of all data except the last 4 bytes (which might be the CRC itself)
        let data_for_crc = if data.len() >= 4 {
            &data[..data.len() - 4]
        } else {
            data
        };
        hasher.update(data_for_crc);
        let calculated_crc = hasher.finalize();

        // Parse algorithm name length (2 bytes, big-endian)
        let mut len_bytes = [0u8; 2];
        cursor.read_exact(&mut len_bytes).map_err(|e| {
            Error::InvalidFormat(format!("Failed to read algorithm name length: {}", e))
        })?;
        let algorithm_len = u16::from_be_bytes(len_bytes) as usize;

        if algorithm_len > 256 {
            return Err(Error::InvalidFormat(format!(
                "Algorithm name too long: {}",
                algorithm_len
            )));
        }

        // Parse algorithm name
        let mut algorithm_bytes = vec![0u8; algorithm_len];
        cursor
            .read_exact(&mut algorithm_bytes)
            .map_err(|e| Error::InvalidFormat(format!("Failed to read algorithm name: {}", e)))?;
        let algorithm = String::from_utf8(algorithm_bytes)
            .map_err(|e| Error::InvalidFormat(format!("Invalid algorithm name encoding: {}", e)))?;

        // Skip padding bytes if any (align to 4-byte boundary)
        let current_pos = cursor.position();
        let padding_needed = (4 - (current_pos % 4)) % 4;
        if padding_needed > 0 {
            cursor
                .seek(SeekFrom::Current(padding_needed as i64))
                .map_err(|e| Error::InvalidFormat(format!("Failed to skip padding: {}", e)))?;
        }

        // Parse chunk length (4 bytes, big-endian)
        let mut chunk_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut chunk_len_bytes)
            .map_err(|e| Error::InvalidFormat(format!("Failed to read chunk length: {}", e)))?;
        let chunk_length = u32::from_be_bytes(chunk_len_bytes);

        // Parse data length (8 bytes, big-endian)
        let mut data_len_bytes = [0u8; 8];
        cursor
            .read_exact(&mut data_len_bytes)
            .map_err(|e| Error::InvalidFormat(format!("Failed to read data length: {}", e)))?;
        let data_length = u64::from_be_bytes(data_len_bytes);

        // Parse number of chunks (4 bytes, big-endian)
        let mut chunk_count_bytes = [0u8; 4];
        cursor
            .read_exact(&mut chunk_count_bytes)
            .map_err(|e| Error::InvalidFormat(format!("Failed to read chunk count: {}", e)))?;
        let chunk_count = u32::from_be_bytes(chunk_count_bytes) as usize;

        if chunk_count > 1_000_000 {
            return Err(Error::InvalidFormat(format!(
                "Too many chunks: {}",
                chunk_count
            )));
        }

        // Parse chunk offsets
        let mut chunk_offsets = Vec::with_capacity(chunk_count);
        for i in 0..chunk_count {
            let mut offset_bytes = [0u8; 8];
            cursor.read_exact(&mut offset_bytes).map_err(|e| {
                Error::InvalidFormat(format!("Failed to read chunk offset {}: {}", i, e))
            })?;
            let offset = u64::from_be_bytes(offset_bytes);
            chunk_offsets.push(offset);
        }

        // Parse per-chunk CRC32 values (if present)
        let mut chunk_crcs = Vec::with_capacity(chunk_count);
        let remaining_before_crcs = data.len() - cursor.position() as usize;

        // Check if we have enough space for chunk CRCs plus optional metadata CRC
        let expected_crc_bytes = chunk_count * 4;
        if remaining_before_crcs >= expected_crc_bytes + 4 {
            // We have per-chunk CRCs
            for i in 0..chunk_count {
                let mut crc_bytes = [0u8; 4];
                cursor.read_exact(&mut crc_bytes).map_err(|e| {
                    Error::InvalidFormat(format!("Failed to read chunk CRC {}: {}", i, e))
                })?;
                let chunk_crc = u32::from_be_bytes(crc_bytes);
                chunk_crcs.push(chunk_crc);
            }
        }

        // Check if there's a CRC32 at the end of the file
        let mut crc32 = None;
        let remaining = data.len() - cursor.position() as usize;
        if remaining == 4 {
            let mut crc_bytes = [0u8; 4];
            cursor
                .read_exact(&mut crc_bytes)
                .map_err(|e| Error::InvalidFormat(format!("Failed to read CRC32: {}", e)))?;
            let stored_crc = u32::from_be_bytes(crc_bytes);

            // Validate CRC
            if stored_crc != calculated_crc {
                return Err(Error::InvalidFormat(format!(
                    "CRC32 mismatch: stored={:08x}, calculated={:08x}",
                    stored_crc, calculated_crc
                )));
            }
            crc32 = Some(stored_crc);
        }

        Ok(CompressionInfo {
            algorithm,
            chunk_length,
            data_length,
            chunk_offsets,
            crc32,
            chunk_crcs,
        })
    }

    /// Alternative parsing method for different CompressionInfo.db formats (legacy only)
    /// Some legacy Cassandra versions may use slightly different layouts
    /// DEPRECATED: Modern formats (BIG v5, BTI) should use structured CompressionInfo.db
    #[cfg(feature = "legacy-heuristics")]
    pub fn parse_alternative_format(data: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(data);

        // Try reading as little-endian first
        let mut len_bytes = [0u8; 2];
        cursor.read_exact(&mut len_bytes).map_err(|e| {
            Error::InvalidFormat(format!("Failed to read algorithm name length: {}", e))
        })?;

        // Try both big-endian and little-endian
        let algorithm_len_be = u16::from_be_bytes(len_bytes) as usize;
        let algorithm_len_le = u16::from_le_bytes(len_bytes) as usize;

        let algorithm_len = if algorithm_len_be > 0 && algorithm_len_be <= 64 {
            algorithm_len_be
        } else if algorithm_len_le > 0 && algorithm_len_le <= 64 {
            algorithm_len_le
        } else {
            return Err(Error::InvalidFormat(format!(
                "Invalid algorithm name length: BE={}, LE={}",
                algorithm_len_be, algorithm_len_le
            )));
        };

        // Parse algorithm name
        let mut algorithm_bytes = vec![0u8; algorithm_len];
        cursor
            .read_exact(&mut algorithm_bytes)
            .map_err(|e| Error::InvalidFormat(format!("Failed to read algorithm name: {}", e)))?;
        let algorithm = String::from_utf8(algorithm_bytes)
            .map_err(|e| Error::InvalidFormat(format!("Invalid algorithm name encoding: {}", e)))?;

        // For alternative format, try to find chunk length by looking for common values
        let remaining_data = &data[cursor.position() as usize..];

        // Look for common chunk sizes: 16KB (0x4000), 64KB (0x10000), 256KB (0x40000)
        let chunk_length = Self::detect_chunk_size(remaining_data).unwrap_or(16384);

        // Estimate data length and offsets based on remaining data
        let data_length = remaining_data.len() as u64;
        let chunk_offsets = vec![0]; // Single chunk assumption for fallback

        Ok(CompressionInfo {
            algorithm,
            chunk_length,
            data_length,
            chunk_offsets,
            crc32: None,
            chunk_crcs: vec![],
        })
    }

    /// Detect chunk size from binary data by looking for common patterns (legacy heuristic)
    #[cfg(feature = "legacy-heuristics")]
    fn detect_chunk_size(data: &[u8]) -> Option<u32> {
        if data.len() < 4 {
            return None;
        }

        // Check for common chunk sizes at various offsets
        for offset in 0..std::cmp::min(16, data.len() - 4) {
            let bytes = &data[offset..offset + 4];
            let value_be = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            let value_le = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

            // Check if either endianness gives us a common chunk size
            for &candidate in &[value_be, value_le] {
                match candidate {
                    4096 | 8192 | 16384 | 32768 | 65536 | 131072 | 262144 => {
                        return Some(candidate);
                    }
                    _ => {}
                }
            }
        }

        None
    }

    /// Get the chunk index for a given offset in the uncompressed data
    pub fn chunk_for_offset(&self, offset: u64) -> usize {
        (offset / self.chunk_length as u64) as usize
    }

    /// Get the offset within a chunk for a given global offset
    pub fn offset_within_chunk(&self, offset: u64) -> u64 {
        offset % self.chunk_length as u64
    }

    /// Get the compressed chunk offset for a given chunk index
    pub fn compressed_chunk_offset(&self, chunk_index: usize) -> Option<u64> {
        self.chunk_offsets.get(chunk_index).copied()
    }

    /// Get the size of a compressed chunk (distance to next chunk or end of file)
    pub fn compressed_chunk_size(
        &self,
        chunk_index: usize,
        total_compressed_size: u64,
    ) -> Option<u64> {
        let start_offset = self.compressed_chunk_offset(chunk_index)?;

        if chunk_index + 1 < self.chunk_offsets.len() {
            // Size is distance to next chunk
            let next_offset = self.chunk_offsets[chunk_index + 1];
            Some(next_offset - start_offset)
        } else {
            // Last chunk: size is distance to end of file
            Some(total_compressed_size - start_offset)
        }
    }

    /// Validate the compression info structure with optional CRC check
    pub fn validate(&self) -> Result<()> {
        if self.algorithm.is_empty() {
            return Err(Error::InvalidFormat(
                "Empty compression algorithm".to_string(),
            ));
        }

        if self.chunk_length == 0 {
            return Err(Error::InvalidFormat("Zero chunk length".to_string()));
        }

        if self.chunk_length > 1024 * 1024 {
            return Err(Error::InvalidFormat(format!(
                "Chunk length too large: {}",
                self.chunk_length
            )));
        }

        if self.chunk_offsets.is_empty() {
            return Err(Error::InvalidFormat("No chunk offsets".to_string()));
        }

        // Check that offsets are in ascending order
        for i in 1..self.chunk_offsets.len() {
            if self.chunk_offsets[i] <= self.chunk_offsets[i - 1] {
                return Err(Error::InvalidFormat(format!(
                    "Chunk offsets not in ascending order: {} <= {}",
                    self.chunk_offsets[i],
                    self.chunk_offsets[i - 1]
                )));
            }
        }

        Ok(())
    }

    /// Parse with strict CRC validation requirement
    pub fn parse_with_crc_required(data: &[u8]) -> Result<Self> {
        let info = Self::parse(data)?;
        if info.crc32.is_none() {
            return Err(Error::InvalidFormat(
                "CRC32 checksum required but not found".to_string(),
            ));
        }
        Ok(info)
    }

    /// Calculate CRC32 for given data
    pub fn calculate_crc32(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    /// Validate CRC32 for a specific chunk
    pub fn validate_chunk_crc(&self, chunk_index: usize, chunk_data: &[u8]) -> Result<()> {
        // If we don't have per-chunk CRCs, skip validation (legacy format)
        if self.chunk_crcs.is_empty() {
            return Ok(());
        }

        // Check if we have a CRC for this chunk
        if chunk_index >= self.chunk_crcs.len() {
            return Err(Error::InvalidFormat(format!(
                "No CRC available for chunk {}",
                chunk_index
            )));
        }

        let expected_crc = self.chunk_crcs[chunk_index];
        let actual_crc = Self::calculate_crc32(chunk_data);

        if expected_crc != actual_crc {
            // Get the chunk offset for better error reporting
            let chunk_offset = self.chunk_offsets.get(chunk_index).unwrap_or(&0);

            return Err(Error::InvalidFormat(format!(
                "CRC32 mismatch for chunk {} at offset 0x{:x}: stored=0x{:08x}, calculated=0x{:08x}",
                chunk_index, chunk_offset, expected_crc, actual_crc
            )));
        }

        Ok(())
    }

    /// Create a debug representation showing the hex dump analysis
    pub fn debug_hex_analysis(&self, original_data: &[u8]) -> String {
        let mut analysis = String::new();
        analysis.push_str(&format!("CompressionInfo Analysis:\n"));
        analysis.push_str(&format!("  Algorithm: {}\n", self.algorithm));
        analysis.push_str(&format!(
            "  Chunk Length: {} bytes (0x{:x})\n",
            self.chunk_length, self.chunk_length
        ));
        analysis.push_str(&format!("  Data Length: {} bytes\n", self.data_length));
        analysis.push_str(&format!("  Chunk Count: {}\n", self.chunk_offsets.len()));

        analysis.push_str("\nChunk Offsets:\n");
        for (i, offset) in self.chunk_offsets.iter().enumerate() {
            analysis.push_str(&format!(
                "  Chunk {}: offset {} (0x{:x})\n",
                i, offset, offset
            ));
        }

        analysis.push_str("\nHex Dump (first 64 bytes):\n");
        let dump_len = std::cmp::min(64, original_data.len());
        for (i, chunk) in original_data[..dump_len].chunks(16).enumerate() {
            analysis.push_str(&format!("  {:04x}: ", i * 16));
            for byte in chunk {
                analysis.push_str(&format!("{:02x} ", byte));
            }
            analysis.push_str("  ");
            for byte in chunk {
                let c = if byte.is_ascii_graphic() || *byte == b' ' {
                    *byte as char
                } else {
                    '.'
                };
                analysis.push(c);
            }
            analysis.push('\n');
        }

        analysis
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_compression_info() {
        // Example hex data from our analysis:
        // 00 0d 4c 5a 34 43 6f 6d 70 72 65 73 73 6f 72 (LZ4Compressor)
        let data = vec![
            0x00, 0x0d, // algorithm name length: 13
            // "LZ4Compressor"
            0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x00,
            0x00, 0x00, // padding
            0x00, 0x00, 0x40, 0x00, // chunk length: 16384
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data length: 0 (example)
            0x00, 0x00, 0x00, 0x01, // chunk count: 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset: 0
        ];

        let info = CompressionInfo::parse(&data).unwrap();
        assert_eq!(info.algorithm, "LZ4Compressor");
        assert_eq!(info.chunk_length, 16384);
        assert_eq!(info.chunk_offsets.len(), 1);
        assert_eq!(info.crc32, None); // No CRC in this test data
    }

    #[test]
    fn test_parse_compression_info_with_crc() {
        // Data with CRC32 at the end
        let mut data = vec![
            0x00, 0x0d, // algorithm name length: 13
            // "LZ4Compressor"
            0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x00,
            0x00, 0x00, // padding
            0x00, 0x00, 0x40, 0x00, // chunk length: 16384
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // data length: 4096
            0x00, 0x00, 0x00, 0x01, // chunk count: 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset: 0
        ];

        // Calculate and append CRC32
        let crc = CompressionInfo::calculate_crc32(&data);
        data.extend_from_slice(&crc.to_be_bytes());

        let info = CompressionInfo::parse(&data).unwrap();
        assert_eq!(info.algorithm, "LZ4Compressor");
        assert_eq!(info.chunk_length, 16384);
        assert_eq!(info.data_length, 4096);
        assert_eq!(info.chunk_offsets.len(), 1);
        assert_eq!(info.crc32, Some(crc));
    }

    #[test]
    fn test_parse_with_invalid_crc() {
        let mut data = vec![
            0x00, 0x0d, // algorithm name length: 13
            // "LZ4Compressor"
            0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x00,
            0x00, 0x00, // padding
            0x00, 0x00, 0x40, 0x00, // chunk length: 16384
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // data length: 4096
            0x00, 0x00, 0x00, 0x01, // chunk count: 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset: 0
        ];

        // Append invalid CRC32
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

        let result = CompressionInfo::parse(&data);
        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("CRC32 mismatch"));
        }
    }

    #[test]
    fn test_parse_with_crc_required() {
        // Data without CRC
        let data = vec![
            0x00, 0x0d, // algorithm name length: 13
            // "LZ4Compressor"
            0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x00,
            0x00, 0x00, // padding
            0x00, 0x00, 0x40, 0x00, // chunk length: 16384
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data length: 0
            0x00, 0x00, 0x00, 0x01, // chunk count: 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk offset: 0
        ];

        // Should fail when CRC is required
        let result = CompressionInfo::parse_with_crc_required(&data);
        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("CRC32 checksum required"));
        }
    }

    #[test]
    fn test_chunk_calculations() {
        let info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 32768,
            chunk_offsets: vec![0, 8192],
            crc32: None,
            chunk_crcs: vec![],
        };

        assert_eq!(info.chunk_for_offset(0), 0);
        assert_eq!(info.chunk_for_offset(16384), 1);
        assert_eq!(info.offset_within_chunk(100), 100);
        assert_eq!(info.offset_within_chunk(16484), 100);

        assert_eq!(info.compressed_chunk_offset(0), Some(0));
        assert_eq!(info.compressed_chunk_offset(1), Some(8192));

        assert_eq!(info.compressed_chunk_size(0, 20000), Some(8192));
        assert_eq!(info.compressed_chunk_size(1, 20000), Some(11808));
    }

    #[test]
    fn test_validation() {
        let valid_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 32768,
            chunk_offsets: vec![0, 8192],
            crc32: Some(0x12345678),
            chunk_crcs: vec![],
        };

        assert!(valid_info.validate().is_ok());

        let invalid_info = CompressionInfo {
            algorithm: "".to_string(),
            chunk_length: 0,
            data_length: 0,
            chunk_offsets: vec![],
            crc32: None,
            chunk_crcs: vec![],
        };

        assert!(invalid_info.validate().is_err());
    }
}
