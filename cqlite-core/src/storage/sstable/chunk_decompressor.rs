//! Bulletproof chunk-based decompression for SSTable Data.db files
//!
//! This module implements the proper decompression strategy for Cassandra SSTable files
//! using CompressionInfo.db metadata to decompress chunks on-demand.

use super::compression_info::CompressionInfo;
use crate::parser::header::CassandraVersion;
use crate::{Error, Result};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};

/// Chunk-based decompressor for SSTable Data.db files
pub struct ChunkDecompressor {
    /// Compression metadata from CompressionInfo.db
    compression_info: CompressionInfo,
    /// Cache of decompressed chunks
    chunk_cache: HashMap<usize, Vec<u8>>,
    /// Maximum number of chunks to cache
    max_cached_chunks: usize,
    /// Cassandra version for format detection
    cassandra_version: CassandraVersion,
    /// Data file path for error reporting
    data_file_path: Option<String>,
}

impl ChunkDecompressor {
    /// Create a new chunk decompressor with compression metadata and format detection
    pub fn new(
        compression_info: CompressionInfo,
        cassandra_version: CassandraVersion,
    ) -> Result<Self> {
        compression_info.validate()?;

        Ok(Self {
            compression_info,
            chunk_cache: HashMap::new(),
            max_cached_chunks: 16, // Cache up to 16 chunks (16 * 16KB = 256KB max memory)
            cassandra_version,
            data_file_path: None,
        })
    }

    /// Create a new chunk decompressor with file path for enhanced error reporting
    pub fn new_with_path(
        compression_info: CompressionInfo,
        cassandra_version: CassandraVersion,
        data_file_path: String,
    ) -> Result<Self> {
        compression_info.validate()?;

        Ok(Self {
            compression_info,
            chunk_cache: HashMap::new(),
            max_cached_chunks: 16,
            cassandra_version,
            data_file_path: Some(data_file_path),
        })
    }

    /// Read data from compressed SSTable at specified offset and length
    pub fn read_data<R: Read + Seek>(
        &mut self,
        reader: &mut R,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(length);
        let mut remaining = length;
        let mut current_offset = offset;

        while remaining > 0 {
            // Determine which chunk contains this offset
            let chunk_index = self.compression_info.chunk_for_offset(current_offset);
            let offset_in_chunk = self.compression_info.offset_within_chunk(current_offset);

            // Get the decompressed chunk
            let chunk_data = self.get_decompressed_chunk(reader, chunk_index)?;

            // Extract the requested data from this chunk
            let chunk_start = offset_in_chunk as usize;
            let chunk_end = std::cmp::min(chunk_start + remaining, chunk_data.len());

            if chunk_start >= chunk_data.len() {
                return Err(Error::InvalidFormat(format!(
                    "Offset {} beyond chunk {} size {}",
                    chunk_start,
                    chunk_index,
                    chunk_data.len()
                )));
            }

            let chunk_slice = &chunk_data[chunk_start..chunk_end];
            result.extend_from_slice(chunk_slice);

            let bytes_read = chunk_slice.len();
            remaining -= bytes_read;
            current_offset += bytes_read as u64;
        }

        Ok(result)
    }

    /// Get a decompressed chunk, using cache if available
    fn get_decompressed_chunk<R: Read + Seek>(
        &mut self,
        reader: &mut R,
        chunk_index: usize,
    ) -> Result<Vec<u8>> {
        // Check cache first
        if let Some(cached_chunk) = self.chunk_cache.get(&chunk_index) {
            return Ok(cached_chunk.clone());
        }

        // Decompress the chunk
        let chunk_data = self.decompress_chunk(reader, chunk_index)?;

        // Add to cache (with LRU eviction if necessary)
        if self.chunk_cache.len() >= self.max_cached_chunks {
            // Simple eviction: remove first entry
            if let Some(first_key) = self.chunk_cache.keys().next().copied() {
                self.chunk_cache.remove(&first_key);
            }
        }

        self.chunk_cache.insert(chunk_index, chunk_data.clone());
        Ok(chunk_data)
    }

    /// Decompress a specific chunk from the compressed data file
    fn decompress_chunk<R: Read + Seek>(
        &self,
        reader: &mut R,
        chunk_index: usize,
    ) -> Result<Vec<u8>> {
        // Get compressed chunk offset and size
        let compressed_offset = self
            .compression_info
            .compressed_chunk_offset(chunk_index)
            .ok_or_else(|| Error::InvalidFormat(format!("No offset for chunk {}", chunk_index)))?;

        // Determine chunk size by finding the file size
        let current_pos = reader
            .seek(SeekFrom::Current(0))
            .map_err(|e| Error::Io(e))?;

        let file_size = reader.seek(SeekFrom::End(0)).map_err(|e| Error::Io(e))?;

        reader
            .seek(SeekFrom::Start(current_pos))
            .map_err(|e| Error::Io(e))?;

        let compressed_size = self
            .compression_info
            .compressed_chunk_size(chunk_index, file_size)
            .ok_or_else(|| {
                Error::InvalidFormat(format!("Cannot determine size for chunk {}", chunk_index))
            })? as usize;

        // Seek to compressed chunk offset
        reader
            .seek(SeekFrom::Start(compressed_offset))
            .map_err(|e| Error::Io(e))?;

        // Read compressed chunk data
        let mut compressed_data = vec![0u8; compressed_size];
        reader
            .read_exact(&mut compressed_data)
            .map_err(|e| Error::Io(e))?;

        println!(
            "📦 Reading chunk {} at offset {} ({} bytes compressed)",
            chunk_index, compressed_offset, compressed_size
        );

        // For modern formats, enforce strict CRC validation
        // Legacy formats skip CRC validation for compatibility
        if self.cassandra_version != CassandraVersion::Legacy {
            // Modern formats require strict CRC validation for all chunks
            if self.compression_info.chunk_crcs.is_empty() {
                let file_info = match &self.data_file_path {
                    Some(path) => format!(" in file {}", path),
                    None => String::new(),
                };
                return Err(Error::InvalidFormat(format!(
                    "Modern format requires per-chunk CRCs but none found in CompressionInfo.db for chunk {} at offset 0x{:x}{}",
                    chunk_index,
                    compressed_offset,
                    file_info
                )));
            }
            
            // Validate CRC for the compressed chunk data
            self.compression_info
                .validate_chunk_crc(chunk_index, &compressed_data)?;
        }

        // Decompress based on algorithm
        let decompressed = match self.compression_info.algorithm.as_str() {
            "LZ4Compressor" => self.decompress_lz4_chunk(&compressed_data, chunk_index),
            "SnappyCompressor" => self.decompress_snappy_chunk(&compressed_data, chunk_index),
            "DeflateCompressor" => self.decompress_deflate_chunk(&compressed_data, chunk_index),
            "ZstdCompressor" => self.decompress_zstd_chunk(&compressed_data, chunk_index),
            algorithm => Err(Error::UnsupportedFormat(format!(
                "Unknown compression algorithm: {}",
                algorithm
            ))),
        }?;

        // Validate decompressed data size matches expected chunk length
        // (for all chunks except possibly the last one)
        if chunk_index < self.compression_info.chunk_offsets.len() - 1 {
            let expected_size = self.compression_info.chunk_length as usize;
            if decompressed.len() != expected_size {
                return Err(Error::InvalidFormat(format!(
                    "Decompressed chunk {} size mismatch: expected {}, got {}",
                    chunk_index,
                    expected_size,
                    decompressed.len()
                )));
            }
        }

        Ok(decompressed)
    }

    /// Decompress LZ4 chunk - strict mode for modern formats
    fn decompress_lz4_chunk(&self, compressed_data: &[u8], chunk_index: usize) -> Result<Vec<u8>> {
        let file_info = match &self.data_file_path {
            Some(path) => format!(" in file {}", path),
            None => String::new(),
        };

        if compressed_data.is_empty() {
            return Err(Error::InvalidFormat(format!(
                "Empty compressed data for chunk {}{}",
                chunk_index,
                file_info
            )));
        }

        // For modern formats, use strict decompression based on CompressionInfo metadata
        // Remove all decompression guessing - use metadata-driven approach only
        if self.cassandra_version != crate::parser::header::CassandraVersion::Legacy {
            // Modern format: strict metadata-driven decompression
            let expected_size = self.compression_info.chunk_length as usize;
            match lz4_flex::decompress(compressed_data, expected_size) {
                Ok(decompressed) => {
                    if decompressed.len() != expected_size {
                        return Err(Error::InvalidFormat(format!(
                            "LZ4 decompressed size mismatch for chunk {} at offset 0x{:x}: expected {}, got {}. No fallback allowed for modern formats{}",
                            chunk_index,
                            self.compression_info.chunk_offsets.get(chunk_index).unwrap_or(&0),
                            expected_size,
                            decompressed.len(),
                            file_info
                        )));
                    }
                    Ok(decompressed)
                },
                Err(e) => Err(Error::InvalidFormat(format!(
                    "LZ4 decompression failed for chunk {} at offset 0x{:x}: {}. No fallback allowed for modern formats{}",
                    chunk_index,
                    self.compression_info.chunk_offsets.get(chunk_index).unwrap_or(&0),
                    e,
                    file_info
                ))),
            }
        } else {
            // Legacy format: try multiple approaches for compatibility (ONLY for legacy formats)
            match lz4_flex::decompress_size_prepended(compressed_data) {
                Ok(decompressed) => Ok(decompressed),
                Err(e) => {
                    let expected_size = self.compression_info.chunk_length as usize;
                    match lz4_flex::decompress(compressed_data, expected_size) {
                        Ok(decompressed) => Ok(decompressed),
                        Err(_) => Err(Error::InvalidFormat(format!(
                            "LZ4 decompression failed for legacy chunk {} at offset 0x{:x}: {}{}",
                            chunk_index,
                            self.compression_info.chunk_offsets.get(chunk_index).unwrap_or(&0),
                            e,
                            file_info
                        ))),
                    }
                }
            }
        }
    }

    /// Decompress Snappy chunk - strict mode for modern formats
    fn decompress_snappy_chunk(
        &self,
        compressed_data: &[u8],
        chunk_index: usize,
    ) -> Result<Vec<u8>> {
        #[cfg(feature = "snappy")]
        {
            use snap::raw::Decoder;
            let mut decoder = Decoder::new();

            match decoder.decompress_vec(compressed_data) {
                Ok(decompressed) => Ok(decompressed),
                Err(e) => Err(Error::InvalidFormat(format!(
                    "Snappy decompression failed for chunk {} at offset 0x{:x}: {}. No fallback allowed for modern formats.",
                    chunk_index,
                    self.compression_info
                        .chunk_offsets
                        .get(chunk_index)
                        .unwrap_or(&0),
                    e
                ))),
            }
        }

        #[cfg(not(feature = "snappy"))]
        {
            let _ = (compressed_data, chunk_index); // Suppress unused warnings
            Err(Error::UnsupportedFormat(
                "Snappy support not compiled in".to_string(),
            ))
        }
    }

    /// Decompress Deflate chunk - strict mode for modern formats
    fn decompress_deflate_chunk(
        &self,
        compressed_data: &[u8],
        chunk_index: usize,
    ) -> Result<Vec<u8>> {
        #[cfg(feature = "deflate")]
        {
            use flate2::read::DeflateDecoder;
            use std::io::Read;

            let mut decoder = DeflateDecoder::new(&compressed_data[..]);
            let mut decompressed = Vec::new();

            match decoder.read_to_end(&mut decompressed) {
                Ok(_) => Ok(decompressed),
                Err(e) => Err(Error::InvalidFormat(format!(
                    "Deflate decompression failed for chunk {} at offset 0x{:x}: {}. No fallback allowed for modern formats.",
                    chunk_index,
                    self.compression_info
                        .chunk_offsets
                        .get(chunk_index)
                        .unwrap_or(&0),
                    e
                ))),
            }
        }

        #[cfg(not(feature = "deflate"))]
        {
            let _ = (compressed_data, chunk_index); // Suppress unused warnings
            Err(Error::UnsupportedFormat(
                "Deflate support not compiled in".to_string(),
            ))
        }
    }

    /// Decompress Zstd chunk - strict mode for modern formats
    fn decompress_zstd_chunk(&self, compressed_data: &[u8], chunk_index: usize) -> Result<Vec<u8>> {
        #[cfg(feature = "zstd")]
        {
            match zstd::decode_all(&compressed_data[..]) {
                Ok(decompressed) => Ok(decompressed),
                Err(e) => Err(Error::InvalidFormat(format!(
                    "Zstd decompression failed for chunk {} at offset 0x{:x}: {}. No fallback allowed for modern formats.",
                    chunk_index,
                    self.compression_info
                        .chunk_offsets
                        .get(chunk_index)
                        .unwrap_or(&0),
                    e
                ))),
            }
        }

        #[cfg(not(feature = "zstd"))]
        {
            let _ = (compressed_data, chunk_index); // Suppress unused warnings
            Err(Error::UnsupportedFormat(
                "Zstd support not compiled in".to_string(),
            ))
        }
    }

    /// Clear the chunk cache to free memory
    pub fn clear_cache(&mut self) {
        self.chunk_cache.clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, usize) {
        (self.chunk_cache.len(), self.max_cached_chunks)
    }

    /// Read all data from the compressed file (for testing/debugging)
    pub fn read_all_data<R: Read + Seek>(&mut self, reader: &mut R) -> Result<Vec<u8>> {
        self.read_data(reader, 0, self.compression_info.data_length as usize)
    }

    /// Get compression info
    pub fn compression_info(&self) -> &CompressionInfo {
        &self.compression_info
    }
}

/// Utility function to create a chunk decompressor from CompressionInfo.db file
pub fn create_decompressor_from_file(
    compression_info_path: &std::path::Path,
) -> Result<ChunkDecompressor> {
    let compression_data = std::fs::read(compression_info_path).map_err(|e| Error::Io(e))?;

    let compression_info = CompressionInfo::parse(&compression_data)
        .or_else(|_| CompressionInfo::parse_alternative_format(&compression_data))?;

    println!("📋 Loaded compression info:");
    println!("   Algorithm: {}", compression_info.algorithm);
    println!("   Chunk Length: {} bytes", compression_info.chunk_length);
    println!("   Data Length: {} bytes", compression_info.data_length);
    println!("   Chunk Count: {}", compression_info.chunk_offsets.len());

    ChunkDecompressor::new(compression_info, CassandraVersion::V5_0Release)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_decompressor_creation() {
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 32768,
            chunk_offsets: vec![0, 8192, 16384],
            crc32: None,
            chunk_crcs: vec![],
        };

        let decompressor =
            ChunkDecompressor::new(compression_info, CassandraVersion::V5_0Release).unwrap();
        assert_eq!(decompressor.compression_info.algorithm, "LZ4Compressor");
        assert_eq!(decompressor.compression_info.chunk_length, 16384);
        assert_eq!(decompressor.compression_info.chunk_offsets.len(), 3);
    }

    #[test]
    fn test_chunk_cache() {
        let compression_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: 16384,
            data_length: 16384,
            chunk_offsets: vec![0],
            crc32: None,
            chunk_crcs: vec![],
        };

        let mut decompressor =
            ChunkDecompressor::new(compression_info, CassandraVersion::V5_0Release).unwrap();

        let (cached, max) = decompressor.cache_stats();
        assert_eq!(cached, 0);
        assert_eq!(max, 16);

        decompressor.clear_cache();
        let (cached_after_clear, _) = decompressor.cache_stats();
        assert_eq!(cached_after_clear, 0);
    }
}
