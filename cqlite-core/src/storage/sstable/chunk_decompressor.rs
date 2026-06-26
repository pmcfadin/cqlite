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
    /// Data file path for error reporting
    data_file_path: Option<String>,
}

impl ChunkDecompressor {
    /// Create a new chunk decompressor with compression metadata.
    ///
    /// The `cassandra_version` parameter is accepted for API compatibility but is no longer
    /// used: the NB format (all Cassandra 5.0 files) always has inline CRC32 in Data.db
    /// regardless of version, which is handled deterministically in decompress_chunk().
    pub fn new(
        compression_info: CompressionInfo,
        _cassandra_version: CassandraVersion,
    ) -> Result<Self> {
        compression_info.validate()?;

        Ok(Self {
            compression_info,
            chunk_cache: HashMap::new(),
            max_cached_chunks: 16, // Cache up to 16 chunks (16 * 16KB = 256KB max memory)
            data_file_path: None,
        })
    }

    /// Create a new chunk decompressor with file path for enhanced error reporting.
    ///
    /// See `new()` for notes on `cassandra_version`.
    pub fn new_with_path(
        compression_info: CompressionInfo,
        _cassandra_version: CassandraVersion,
        data_file_path: String,
    ) -> Result<Self> {
        compression_info.validate()?;

        Ok(Self {
            compression_info,
            chunk_cache: HashMap::new(),
            max_cached_chunks: 16,
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

    /// Exact uncompressed size Cassandra wrote for chunk `chunk_index`, derived from
    /// `data_length` and `chunk_length`.
    ///
    /// Cassandra lays chunks out so that random access works via
    /// `position / chunk_length`: every chunk that holds data up to the final byte is
    /// exactly `chunk_length`, the chunk containing the final byte is the partial
    /// remainder, and any chunk whose start is at/after `data_length` is empty (some
    /// flush/close paths append a degenerate trailing chunk — e.g. `system/local`:
    /// data_length 708, chunk_length 16384, chunk 0 = 708 bytes, chunk 1 = 0). This is
    /// the precise per-chunk invariant: stricter than `<= chunk_length` (it still
    /// rejects a short chunk that has addressable data after it — corruption that would
    /// also break Cassandra's own position→chunk mapping), but not the over-strict
    /// `== chunk_length` that rejected valid partial/trailing chunks.
    fn expected_decompressed_len(&self, chunk_index: usize) -> u64 {
        let chunk_length = self.compression_info.chunk_length as u64;
        let start = (chunk_index as u64).saturating_mul(chunk_length);
        self.compression_info
            .data_length
            .saturating_sub(start)
            .min(chunk_length)
    }

    /// Decompress a specific chunk from the compressed data file
    fn decompress_chunk<R: Read + Seek>(
        &self,
        reader: &mut R,
        chunk_index: usize,
    ) -> Result<Vec<u8>> {
        // Get compressed chunk offset
        let compressed_offset = self
            .compression_info
            .compressed_chunk_offset(chunk_index)
            .ok_or_else(|| Error::InvalidFormat(format!("No offset for chunk {}", chunk_index)))?;

        // Determine record size (compressed payload + 4-byte inline CRC) using file size
        let current_pos = reader.stream_position().map_err(Error::Io)?;
        let file_size = reader.seek(SeekFrom::End(0)).map_err(Error::Io)?;
        reader
            .seek(SeekFrom::Start(current_pos))
            .map_err(Error::Io)?;

        // compressed_chunk_size returns the full record delta including the 4-byte inline CRC.
        // CompressedSequentialWriter.java:203: chunkOffset += compressedLength + 4
        let record_size = self
            .compression_info
            .compressed_chunk_size(chunk_index, file_size)
            .ok_or_else(|| {
                Error::InvalidFormat(format!("Cannot determine size for chunk {}", chunk_index))
            })?;

        // Bug #639 fix: subtract the 4-byte inline CRC from the delta.
        // The old code passed all (delta) bytes to the decompressor, which included the
        // trailing CRC and caused decompression failures on well-formed chunks.
        if record_size < 4 {
            return Err(Error::InvalidFormat(format!(
                "Chunk {} record size {} is too small (minimum 4 bytes for inline CRC)",
                chunk_index, record_size
            )));
        }
        let compressed_len = (record_size - 4) as usize;

        // Seek to compressed chunk offset and read compressed payload only
        reader
            .seek(SeekFrom::Start(compressed_offset))
            .map_err(Error::Io)?;

        let mut compressed_data = vec![0u8; compressed_len];
        reader.read_exact(&mut compressed_data).map_err(Error::Io)?;

        // Read the 4-byte inline CRC32 (big-endian) and validate it over the compressed bytes.
        // Authority: CompressedSequentialWriter.java:192 + read path lines 275-282.
        let mut crc_bytes = [0u8; 4];
        reader.read_exact(&mut crc_bytes).map_err(Error::Io)?;
        let stored_crc = u32::from_be_bytes(crc_bytes);
        let computed_crc = crc32fast::hash(&compressed_data);
        if stored_crc != computed_crc {
            let file_info = match &self.data_file_path {
                Some(path) => format!(" in file {}", path),
                None => String::new(),
            };
            return Err(Error::InvalidFormat(format!(
                "CRC32 mismatch for chunk {} at offset 0x{:x}{}: stored=0x{:08x}, computed=0x{:08x}, compressed_len={}",
                chunk_index, compressed_offset, file_info, stored_crc, computed_crc, compressed_len
            )));
        }

        log::debug!(
            "Reading chunk {} at offset {} ({} bytes compressed, CRC OK)",
            chunk_index,
            compressed_offset,
            compressed_len
        );

        // Incompressible-chunk fallback (Bug #639):
        // When compressedLength >= maxCompressedLength, Cassandra stored the chunk uncompressed.
        // CompressedSequentialWriter.java:160-177: if compressedLen >= maxCompressedLen, use raw buffer.
        let max_compressed_length = self.compression_info.max_compressed_length as usize;
        if compressed_len >= max_compressed_length {
            log::debug!(
                "Chunk {} is incompressible (compressed_len={} >= max_compressed_length={}), returning raw bytes",
                chunk_index, compressed_len, max_compressed_length
            );
            // A raw chunk's stored bytes ARE the uncompressed bytes, so they must match
            // the exact size Cassandra wrote for this chunk index — the same invariant
            // the compressed path checks below (see `expected_decompressed_len`).
            let expected_size = self.expected_decompressed_len(chunk_index);
            if compressed_data.len() as u64 != expected_size {
                return Err(Error::InvalidFormat(format!(
                    "Raw (incompressible) chunk {} size {} != expected {} (data_length {}, chunk_length {}) — corrupt or misdecoded",
                    chunk_index,
                    compressed_data.len(),
                    expected_size,
                    self.compression_info.data_length,
                    self.compression_info.chunk_length,
                )));
            }
            return Ok(compressed_data);
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

        // Validate the decompressed chunk size against the exact size Cassandra wrote
        // for this chunk index (see `expected_decompressed_len`). This handles full
        // chunks, the partial final-data chunk, and degenerate empty trailing chunks,
        // while still rejecting a short non-final chunk that has addressable data after
        // it (corruption). The inline CRC above is the primary integrity guard.
        let expected_size = self.expected_decompressed_len(chunk_index);
        if decompressed.len() as u64 != expected_size {
            return Err(Error::InvalidFormat(format!(
                "Decompressed chunk {} size {} != expected {} (data_length {}, chunk_length {}) — corrupt or misdecoded",
                chunk_index,
                decompressed.len(),
                expected_size,
                self.compression_info.data_length,
                self.compression_info.chunk_length,
            )));
        }

        Ok(decompressed)
    }

    /// Decompress LZ4 chunk - Cassandra uses 4-byte little-endian length prefix
    /// Upper bound on a single decompressed chunk: the configured uncompressed
    /// chunk length (the final chunk is shorter, never larger). Used to bound the
    /// streaming Deflate/Zstd decoders against decompression bombs. Falls back to
    /// the 128MB global cap if chunk_length is unset/zero.
    ///
    /// Only the streaming Deflate/Zstd paths consume this bound, so the method is
    /// gated to match its callers and stay dead-code-free under minimal builds.
    #[cfg(any(feature = "deflate", feature = "zstd"))]
    fn chunk_size_guard(&self) -> u64 {
        match self.compression_info.chunk_length {
            0 => 128 * 1024 * 1024,
            n => n as u64,
        }
    }

    fn decompress_lz4_chunk(&self, compressed_data: &[u8], chunk_index: usize) -> Result<Vec<u8>> {
        let file_info = match &self.data_file_path {
            Some(path) => format!(" in file {}", path),
            None => String::new(),
        };

        if compressed_data.len() < 4 {
            return Err(Error::InvalidFormat(format!(
                "LZ4 compressed data too short for chunk {}{} (need at least 4 bytes for length prefix, got {})",
                chunk_index, file_info, compressed_data.len()
            )));
        }

        // CRITICAL: Cassandra's LZ4Compressor prepends a 4-byte little-endian length prefix
        // See: org.apache.cassandra.io.compress.LZ4Compressor.decompress() lines 169-172
        // This is NOT the lz4_flex size-prepended format (which uses varint encoding)
        let decompressed_length = u32::from_le_bytes([
            compressed_data[0],
            compressed_data[1],
            compressed_data[2],
            compressed_data[3],
        ]) as usize;

        // Validate the LZ4 length prefix against the exact size Cassandra wrote for
        // this chunk index (full / partial-final / empty-trailing — see
        // `expected_decompressed_len`). Fails fast on a misdecoded prefix before
        // attempting decompression.
        let expected_size = self.expected_decompressed_len(chunk_index) as usize;
        if decompressed_length != expected_size {
            return Err(Error::InvalidFormat(format!(
                "LZ4 length prefix mismatch for chunk {} at offset 0x{:x}: expected {}, got {} (first 4 bytes: {:02x} {:02x} {:02x} {:02x}){}",
                chunk_index,
                self.compression_info
                    .chunk_offsets
                    .get(chunk_index)
                    .unwrap_or(&0),
                expected_size,
                decompressed_length,
                compressed_data[0],
                compressed_data[1],
                compressed_data[2],
                compressed_data[3],
                file_info
            )));
        }

        // Skip the 4-byte length prefix and decompress the actual LZ4 data
        let lz4_data = &compressed_data[4..];

        match lz4_flex::decompress(lz4_data, decompressed_length) {
            Ok(decompressed) => {
                if decompressed.len() != decompressed_length {
                    return Err(Error::InvalidFormat(format!(
                        "LZ4 decompression size mismatch for chunk {} at offset 0x{:x}: expected {}, got {}{}",
                        chunk_index,
                        self.compression_info
                            .chunk_offsets
                            .get(chunk_index)
                            .unwrap_or(&0),
                        decompressed_length,
                        decompressed.len(),
                        file_info
                    )));
                }
                Ok(decompressed)
            }
            Err(e) => Err(Error::InvalidFormat(format!(
                "LZ4 decompression failed for chunk {} at offset 0x{:x}: {}{}",
                chunk_index,
                self.compression_info
                    .chunk_offsets
                    .get(chunk_index)
                    .unwrap_or(&0),
                e,
                file_info
            ))),
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
            // Cassandra's DeflateCompressor uses java.util.zip.Deflater/Inflater,
            // which emit zlib-wrapped streams (2-byte header 0x78 0x9c + Adler-32
            // trailer), NOT raw DEFLATE. Decode with ZlibDecoder to match. (#1082)
            use flate2::read::ZlibDecoder;
            use std::io::Read;

            // Decompression-bomb guard: a chunk decompresses to at most
            // `chunk_length` bytes (the final chunk is shorter). Cap the reader at
            // chunk_length + 1 so a crafted zlib stream cannot expand into an
            // unbounded Vec before we validate the size (roborev).
            let max_chunk = self.chunk_size_guard();
            let mut decoder = ZlibDecoder::new(compressed_data).take(max_chunk + 1);
            let mut decompressed = Vec::new();

            match decoder.read_to_end(&mut decompressed) {
                Ok(_) if decompressed.len() as u64 > max_chunk => Err(Error::InvalidFormat(format!(
                    "Deflate chunk {} expands beyond chunk_length {} (decompression-bomb guard)",
                    chunk_index, max_chunk
                ))),
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
            use std::io::Read;
            use zstd::stream::read::Decoder as ZstdDecoder;

            // Decompression-bomb guard: stream through a reader capped at
            // chunk_length + 1 rather than `decode_all`, which would pre-allocate
            // whatever content size the frame declares (roborev).
            let max_chunk = self.chunk_size_guard();
            let mut decoder = match ZstdDecoder::new(compressed_data) {
                Ok(d) => d.take(max_chunk + 1),
                Err(e) => {
                    return Err(Error::InvalidFormat(format!(
                        "Zstd decoder init failed for chunk {}: {}",
                        chunk_index, e
                    )))
                }
            };
            let mut decompressed = Vec::new();
            match decoder.read_to_end(&mut decompressed) {
                Ok(_) if decompressed.len() as u64 > max_chunk => Err(Error::InvalidFormat(format!(
                    "Zstd chunk {} expands beyond chunk_length {} (decompression-bomb guard)",
                    chunk_index, max_chunk
                ))),
                Ok(_) => Ok(decompressed),
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

    /// Read all data from the compressed file (for testing/debugging).
    ///
    /// Bounded by `data_length`, so it does NOT touch a degenerate empty trailing
    /// chunk that holds no addressable data. To validate EVERY chunk record's inline
    /// CRC (including such trailing chunks), iterate `chunk_count()` and call
    /// `decompress_chunk_by_index`.
    pub fn read_all_data<R: Read + Seek>(&mut self, reader: &mut R) -> Result<Vec<u8>> {
        self.read_data(reader, 0, self.compression_info.data_length as usize)
    }

    /// Number of compressed chunk records (the CompressionInfo.db offset-table length).
    pub fn chunk_count(&self) -> usize {
        self.compression_info.chunk_offsets.len()
    }

    /// Decompress a single chunk by index, validating its inline CRC32 trailer.
    ///
    /// Exposed for strict per-chunk parity tests that must exercise every chunk record
    /// — including degenerate trailing chunks that `read_all_data` (bounded by
    /// `data_length`) never reads.
    pub fn decompress_chunk_by_index<R: Read + Seek>(
        &mut self,
        reader: &mut R,
        chunk_index: usize,
    ) -> Result<Vec<u8>> {
        self.get_decompressed_chunk(reader, chunk_index)
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
    let compression_data = std::fs::read(compression_info_path).map_err(Error::Io)?;

    // Parse CompressionInfo using the deterministic Cassandra format.
    // (Bug #638: old heuristic parse_alternative_format violated the no-heuristics mandate
    // and has been removed.  The standard parse() is authoritative for all supported files.)
    let compression_info = CompressionInfo::parse(&compression_data)?;

    log::info!("Loaded compression info:");
    log::info!("   Algorithm: {}", compression_info.algorithm);
    log::info!("   Chunk Length: {} bytes", compression_info.chunk_length);
    log::info!("   Data Length: {} bytes", compression_info.data_length);
    log::info!("   Chunk Count: {}", compression_info.chunk_offsets.len());

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
            option_pairs: vec![],
            max_compressed_length: i32::MAX as u32,
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
            option_pairs: vec![],
            max_compressed_length: i32::MAX as u32,
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
