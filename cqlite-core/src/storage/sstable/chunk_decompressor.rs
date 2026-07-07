//! Bulletproof chunk-based decompression for SSTable Data.db files
//!
//! This module implements the proper decompression strategy for Cassandra SSTable files
//! using CompressionInfo.db metadata to decompress chunks on-demand.

use super::compression_info::CompressionInfo;
#[cfg(feature = "zstd")]
use super::zstd_frame::zstd_dictionary_rejection;
use crate::parser::header::CassandraVersion;
use crate::{Error, Result};
use lru::LruCache;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Number of decompressed chunks the on-demand cache retains (16 * 16KB ≈ 256KB).
///
/// Kept as a `const NonZeroUsize` so [`LruCache::new`] takes it without any runtime
/// `unwrap()`/`expect()` (library code must be panic-free). The `match` folds the
/// `Option` at compile time; the `NonZeroUsize::MIN` arm is unreachable for a literal
/// `16` and exists only to keep the `const` total.
const CHUNK_CACHE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(16) {
    Some(n) => n,
    None => NonZeroUsize::MIN,
};

/// Chunk-based decompressor for SSTable Data.db files
pub struct ChunkDecompressor {
    /// Compression metadata from CompressionInfo.db
    compression_info: CompressionInfo,
    /// Recency-tracked cache of decompressed chunks keyed by authoritative chunk
    /// index (issue #1569, Epic B/B3).
    ///
    /// Values are `Arc<[u8]>`: a cache hit is an `Arc::clone` (refcount bump), never
    /// a chunk-sized memcpy of the cached bytes. Eviction is real LRU — `LruCache`
    /// tracks recency on every `get`/`put` and evicts the genuinely least-recently-
    /// used entry at capacity — replacing the previous `FxHashMap` that cloned on
    /// hit and evicted an arbitrary `keys().next()` entry. This cache is exercised
    /// single-threaded (the decompressor is owned `&mut` by one `BulletproofReader`),
    /// so no `Mutex`/sharding is needed here; contrast B1's shared
    /// `DecompressedChunkCache`.
    chunk_cache: LruCache<usize, Arc<[u8]>>,
    /// Count of actual chunk decompressions performed (miss path only). A cache hit
    /// never increments it, so a test can prove a repeated read of the same chunk
    /// decompressed exactly once (issue #1569).
    decompress_calls: u64,
    /// Data file path for error reporting
    data_file_path: Option<String>,
    /// Cached Data.db length, captured on the first chunk read (issue #1586).
    ///
    /// The file is an immutable SSTable, so its size is derived exactly once
    /// with a single seek probe and then reused for every subsequent chunk's
    /// `compressed_chunk_size` bounds math — instead of re-seeking to `End(0)`
    /// (and back) on every chunk read.
    cached_data_file_size: Option<u64>,
    /// The reader's logical position after the most recent chunk read within the
    /// current `read_data` / `decompress_chunk_by_index` call (issue #1586).
    ///
    /// Sequential chunk reads leave the reader positioned exactly at the next
    /// chunk's offset, so the explicit `seek(Start(offset))` can be skipped when
    /// it would be a no-op. Reset to `None` at each public entry point so it is
    /// only ever trusted within a single call against a single reader.
    stream_pos: Option<u64>,
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
            chunk_cache: LruCache::new(CHUNK_CACHE_CAPACITY),
            decompress_calls: 0,
            data_file_path: None,
            cached_data_file_size: None,
            stream_pos: None,
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
            chunk_cache: LruCache::new(CHUNK_CACHE_CAPACITY),
            decompress_calls: 0,
            data_file_path: Some(data_file_path),
            cached_data_file_size: None,
            stream_pos: None,
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

        // The reader's logical position is unknown at the start of a call (the
        // caller may have passed a fresh reader), so the first chunk read always
        // seeks; subsequent sequential reads within this call skip the redundant
        // seek (issue #1586).
        self.stream_pos = None;

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

    /// Get a decompressed chunk, using the cache if available.
    ///
    /// On a hit this bumps the chunk's LRU recency and returns an `Arc::clone`
    /// (a refcount bump, never a chunk-sized memcpy). On a miss it decompresses
    /// once, converts the buffer to `Arc<[u8]>` exactly once, and inserts it — the
    /// `LruCache` evicts the genuinely least-recently-used entry at capacity
    /// (issue #1569).
    fn get_decompressed_chunk<R: Read + Seek>(
        &mut self,
        reader: &mut R,
        chunk_index: usize,
    ) -> Result<Arc<[u8]>> {
        // Cache hit: refcount bump + recency update, no copy of the chunk bytes.
        if let Some(cached_chunk) = self.chunk_cache.get(&chunk_index) {
            return Ok(Arc::clone(cached_chunk));
        }

        // Miss: decompress, then convert `Vec<u8>` -> `Arc<[u8]>` exactly once.
        let chunk_data = self.decompress_chunk(reader, chunk_index)?;
        let arc: Arc<[u8]> = Arc::from(chunk_data.into_boxed_slice());

        // Real LRU eviction: `put` inserts as most-recently-used and evicts the
        // least-recently-used entry when at capacity.
        self.chunk_cache.put(chunk_index, Arc::clone(&arc));
        Ok(arc)
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
        &mut self,
        reader: &mut R,
        chunk_index: usize,
    ) -> Result<Vec<u8>> {
        // Count every actual decompression (miss path only). A cache hit returns
        // before reaching here, so a repeated read of a resident chunk leaves this
        // unchanged — the wiring evidence for issue #1569.
        self.decompress_calls = self.decompress_calls.saturating_add(1);

        // Get compressed chunk offset
        let compressed_offset = self
            .compression_info
            .compressed_chunk_offset(chunk_index)
            .ok_or_else(|| Error::InvalidFormat(format!("No offset for chunk {}", chunk_index)))?;

        // Determine record size (compressed payload + 4-byte inline CRC) using the
        // Data.db length. The SSTable is immutable, so its size is derived once
        // with a single seek probe and cached — never re-probed per chunk (#1586).
        let file_size = match self.cached_data_file_size {
            Some(size) => size,
            None => {
                let current_pos = reader.stream_position().map_err(Error::Io)?;
                let size = reader.seek(SeekFrom::End(0)).map_err(Error::Io)?;
                reader
                    .seek(SeekFrom::Start(current_pos))
                    .map_err(Error::Io)?;
                self.cached_data_file_size = Some(size);
                // The probe restored the reader to `current_pos`.
                self.stream_pos = Some(current_pos);
                size
            }
        };

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

        // Seek to compressed chunk offset and read compressed payload only.
        // Skip the seek when the previous sequential chunk read already left the
        // reader positioned exactly here (issue #1586 step 3).
        if self.stream_pos != Some(compressed_offset) {
            reader
                .seek(SeekFrom::Start(compressed_offset))
                .map_err(Error::Io)?;
        }
        // Position is unknown until the record's bytes are fully read.
        self.stream_pos = None;

        let mut compressed_data = vec![0u8; compressed_len];
        reader.read_exact(&mut compressed_data).map_err(Error::Io)?;

        // Read the 4-byte inline CRC32 (big-endian) and validate it over the compressed bytes.
        // Authority: CompressedSequentialWriter.java:192 + read path lines 275-282.
        let mut crc_bytes = [0u8; 4];
        reader.read_exact(&mut crc_bytes).map_err(Error::Io)?;
        // The record (payload + 4-byte CRC) has been consumed; the reader now sits
        // at the next chunk's offset, so a subsequent sequential read can skip its
        // seek (issue #1586).
        self.stream_pos = Some(compressed_offset.saturating_add(record_size));
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

        tracing::debug!(
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
            tracing::debug!(
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
    /// Only the Snappy/Deflate/Zstd paths consume this bound, so the method is
    /// gated to match its callers and stay dead-code-free under minimal builds.
    #[cfg(any(feature = "snappy", feature = "deflate", feature = "zstd"))]
    fn chunk_size_guard(&self) -> u64 {
        match self.compression_info.chunk_length {
            0 => 128 * 1024 * 1024,
            n => n as u64,
        }
    }

    #[cfg(feature = "lz4")]
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

    /// Decompress LZ4 chunk fallback when the `lz4` feature is disabled.
    #[cfg(not(feature = "lz4"))]
    fn decompress_lz4_chunk(&self, compressed_data: &[u8], chunk_index: usize) -> Result<Vec<u8>> {
        let _ = (compressed_data, chunk_index); // Suppress unused warnings
        Err(Error::UnsupportedFormat(
            "LZ4 support not compiled in".to_string(),
        ))
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

            // Decompression-bomb guard (issue #1588): a raw Snappy block advertises
            // its decompressed length as a leading varint, and `decompress_vec`
            // pre-allocates that size BEFORE decoding. Reject an over-limit advertised
            // length up front — a chunk decodes to at most `chunk_length` bytes — so a
            // crafted block cannot force a huge allocation (mirrors the Deflate/Zstd
            // chunk guards, which bound by `chunk_size_guard()`).
            let max_chunk = self.chunk_size_guard();
            let advertised = snap::raw::decompress_len(compressed_data).map_err(|e| {
                Error::InvalidFormat(format!(
                    "Snappy length decode failed for chunk {}: {}",
                    chunk_index, e
                ))
            })?;
            if advertised as u64 > max_chunk {
                return Err(Error::InvalidFormat(format!(
                    "Snappy chunk {} advertises {} bytes, beyond chunk_length {} (decompression-bomb guard)",
                    chunk_index, advertised, max_chunk
                )));
            }

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

            // AUTHORITATIVE dictionary detection (issue #1414): fail closed with a
            // typed, feature-naming error BEFORE a plain decode when the metadata or
            // frame header declares a dictionary — an unsupported FEATURE, not
            // corruption/checksum, so it classifies distinctly (never a guess, #28).
            if let Some(msg) =
                zstd_dictionary_rejection(&self.compression_info, compressed_data, chunk_index)
            {
                return Err(Error::UnsupportedFormat(msg));
            }

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

    /// Get cache statistics: `(resident entry count, capacity)`.
    pub fn cache_stats(&self) -> (usize, usize) {
        (self.chunk_cache.len(), self.chunk_cache.cap().get())
    }

    /// Number of actual chunk decompressions performed so far (miss path only).
    ///
    /// A cache hit does NOT increment this, so a repeated read of a resident chunk
    /// leaves it unchanged. Exposed for wiring-evidence tests (issue #1569).
    ///
    /// Consumed only by the lz4-gated multichunk round-trip tests, so it is gated to
    /// match and stay dead-code-free under single-feature test builds (issue #1873).
    #[cfg(all(test, feature = "lz4"))]
    pub(crate) fn decompress_call_count(&self) -> u64 {
        self.decompress_calls
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
        // Single-chunk entry point: the reader position is unknown here too, so
        // force the first (only) read to seek (issue #1586).
        self.stream_pos = None;
        // Cold per-chunk parity/verification path — hand callers an owned `Vec`
        // (explicit copy, off the hot hit path) so the public signature is
        // unchanged (issue #1569).
        Ok(self.get_decompressed_chunk(reader, chunk_index)?.to_vec())
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

    tracing::info!("Loaded compression info:");
    tracing::info!("   Algorithm: {}", compression_info.algorithm);
    tracing::info!("   Chunk Length: {} bytes", compression_info.chunk_length);
    tracing::info!("   Data Length: {} bytes", compression_info.data_length);
    tracing::info!("   Chunk Count: {}", compression_info.chunk_offsets.len());

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

    /// A `Read + Seek` wrapper that counts every `seek()` call (which includes
    /// `stream_position()`, since the default `Seek::stream_position` is
    /// `seek(SeekFrom::Current(0))`). Used to prove the chunk read path does not
    /// re-derive the immutable file size with a seek probe on every chunk
    /// (issue #1586).
    ///
    /// Only the lz4-gated multichunk tests construct it, so it is gated to match and
    /// stay dead-code-free under single-feature test builds (issue #1873).
    #[cfg(feature = "lz4")]
    struct SeekCountingReader {
        inner: std::io::Cursor<Vec<u8>>,
        seeks: std::rc::Rc<std::cell::Cell<usize>>,
    }

    #[cfg(feature = "lz4")]
    impl std::io::Read for SeekCountingReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.inner.read(buf)
        }
    }

    #[cfg(feature = "lz4")]
    impl Seek for SeekCountingReader {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.seeks.set(self.seeks.get() + 1);
            self.inner.seek(pos)
        }
    }

    /// Build a synthetic `k`-chunk LZ4 Data.db body plus its `CompressionInfo`.
    /// Each chunk record is `[LE u32 uncompressed_len][lz4 bytes][BE u32 CRC32]`
    /// exactly as Cassandra's `CompressedSequentialWriter` lays them out.
    #[cfg(feature = "lz4")]
    fn build_multichunk_lz4(
        k: usize,
        chunk_len: usize,
        last_len: usize,
    ) -> (Vec<u8>, CompressionInfo) {
        let mut data_db: Vec<u8> = Vec::new();
        let mut offsets: Vec<u64> = Vec::new();
        let mut data_length: u64 = 0;
        for i in 0..k {
            offsets.push(data_db.len() as u64);
            let unclen = if i + 1 == k { last_len } else { chunk_len };
            let uncompressed: Vec<u8> = (0..unclen).map(|j| ((i * 7 + j) % 251) as u8).collect();
            data_length += unclen as u64;
            let mut payload = (unclen as u32).to_le_bytes().to_vec();
            payload.extend_from_slice(&lz4_flex::compress(&uncompressed));
            let crc = crc32fast::hash(&payload);
            data_db.extend_from_slice(&payload);
            data_db.extend_from_slice(&crc.to_be_bytes());
        }
        let info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            chunk_length: chunk_len as u32,
            data_length,
            chunk_offsets: offsets,
            option_pairs: vec![],
            max_compressed_length: i32::MAX as u32,
        };
        (data_db, info)
    }

    /// Issue #1586: reading every chunk of a K-chunk file must NOT re-probe the
    /// immutable file size with a `seek(End)`/`seek(Start)` dance on each chunk.
    /// A full `read_all_data` scan must issue `<= K + O(1)` seeks, not the ~4*K
    /// seeks the per-chunk size probe produced. On unfixed code this asserts
    /// ~4*K and FAILS; after threading the cached file size it passes.
    #[cfg(feature = "lz4")]
    #[test]
    fn read_all_data_does_not_reprobe_file_size_per_chunk() {
        let k = 8usize;
        let (data_db, info) = build_multichunk_lz4(k, 200, 50);
        let mut decompressor = ChunkDecompressor::new(info, CassandraVersion::V5_0Release).unwrap();

        let seeks = std::rc::Rc::new(std::cell::Cell::new(0usize));
        let mut reader = SeekCountingReader {
            inner: std::io::Cursor::new(data_db),
            seeks: seeks.clone(),
        };

        let out = decompressor
            .read_all_data(&mut reader)
            .expect("read_all_data");
        assert_eq!(out.len(), 7 * 200 + 50, "all decompressed bytes returned");

        // Budget: one initial size probe (a small constant) plus at most one
        // positioning seek per chunk. The per-chunk file-size re-probe (3 extra
        // seeks/chunk) must be gone.
        let total = seeks.get();
        let budget = k + 4;
        assert!(
            total <= budget,
            "expected <= {budget} seeks for {k} chunks (K + O(1)), got {total} \
             — per-chunk file-size re-probe not eliminated (issue #1586)"
        );
    }

    /// Issue #1569 (B3), zero-copy hit: fetching the same chunk twice returns the
    /// SAME underlying buffer (`Arc` pointer identity) — a refcount bump, never a
    /// chunk-sized copy — and does not re-decompress. RED on old code, which
    /// returned a fresh `Vec` clone per hit.
    #[cfg(feature = "lz4")]
    #[test]
    fn hit_is_arc_refcount_bump_not_copy() {
        let (data_db, info) = build_multichunk_lz4(3, 200, 200);
        let mut dec = ChunkDecompressor::new(info, CassandraVersion::V5_0Release).unwrap();
        let mut reader = std::io::Cursor::new(data_db);

        let first = dec
            .get_decompressed_chunk(&mut reader, 1)
            .expect("cold read");
        assert_eq!(
            dec.decompress_call_count(),
            1,
            "cold read decompresses once"
        );

        let second = dec
            .get_decompressed_chunk(&mut reader, 1)
            .expect("cache hit");
        assert!(
            Arc::ptr_eq(&first, &second),
            "a hit must return the same Arc buffer, not a copy"
        );
        assert_eq!(
            dec.decompress_call_count(),
            1,
            "a hit must not re-decompress"
        );
    }

    /// Issue #1569 (B3), decompress-once through the real `read_data` path: reading
    /// the same in-chunk range twice decompresses exactly once (the second read is
    /// a cache hit).
    #[cfg(feature = "lz4")]
    #[test]
    fn read_same_chunk_twice_decompresses_once() {
        // 4 chunks * 200 bytes; a read at offset 400 len 100 lands wholly in chunk 2.
        let (data_db, info) = build_multichunk_lz4(4, 200, 200);
        let mut dec = ChunkDecompressor::new(info, CassandraVersion::V5_0Release).unwrap();
        let mut reader = std::io::Cursor::new(data_db);

        let a = dec.read_data(&mut reader, 400, 100).expect("first read");
        let calls_after_first = dec.decompress_call_count();
        assert_eq!(
            calls_after_first, 1,
            "one chunk decompressed on the cold read"
        );

        let b = dec.read_data(&mut reader, 400, 100).expect("second read");
        assert_eq!(a, b, "repeated read returns identical bytes");
        assert_eq!(
            dec.decompress_call_count(),
            calls_after_first,
            "second read of a resident chunk must not decompress"
        );
    }

    /// Issue #1569 (B3), real LRU eviction: at capacity 2, access A, B, A, then
    /// insert C. B (least recently used) must be evicted while A (re-accessed) and
    /// C (just inserted) survive. RED on old code, whose `keys().next()` eviction
    /// could drop A instead of B.
    #[cfg(feature = "lz4")]
    #[test]
    fn eviction_is_real_lru_not_arbitrary() {
        let (data_db, info) = build_multichunk_lz4(3, 200, 200);
        let mut dec = ChunkDecompressor::new(info, CassandraVersion::V5_0Release).unwrap();
        // Force capacity 2 for a deterministic A,B,A,C test (in-module test can set
        // the private cache field directly).
        dec.chunk_cache = LruCache::new(NonZeroUsize::new(2).expect("capacity 2"));
        let mut reader = std::io::Cursor::new(data_db);

        // A = chunk 0, B = chunk 1, C = chunk 2.
        let _a = dec.get_decompressed_chunk(&mut reader, 0).expect("A cold");
        let _b = dec.get_decompressed_chunk(&mut reader, 1).expect("B cold");
        let _a2 = dec.get_decompressed_chunk(&mut reader, 0).expect("A hit"); // bump A recency
        assert_eq!(
            dec.decompress_call_count(),
            2,
            "A,B decompressed; A re-access is a hit"
        );

        // Insert C → over capacity → evict LRU, which is now B (A was just accessed).
        let _c = dec.get_decompressed_chunk(&mut reader, 2).expect("C cold");
        assert_eq!(dec.decompress_call_count(), 3, "C is a fresh decompress");

        let calls = dec.decompress_call_count();
        // A survives (recently used) → hit, no new decompress.
        let _ = dec
            .get_decompressed_chunk(&mut reader, 0)
            .expect("A still resident");
        assert_eq!(
            dec.decompress_call_count(),
            calls,
            "A (recently used) must survive"
        );
        // C survives (just inserted) → hit.
        let _ = dec
            .get_decompressed_chunk(&mut reader, 2)
            .expect("C still resident");
        assert_eq!(
            dec.decompress_call_count(),
            calls,
            "C (just inserted) must survive"
        );
        // B was evicted → miss → re-decompress.
        let _ = dec
            .get_decompressed_chunk(&mut reader, 1)
            .expect("B re-read");
        assert_eq!(
            dec.decompress_call_count(),
            calls + 1,
            "B (least recently used) must have been evicted"
        );
    }

    /// Issue #1569 (B3), capacity bound: reading more distinct chunks than the
    /// cache capacity keeps the resident entry count within capacity throughout.
    #[cfg(feature = "lz4")]
    #[test]
    fn cache_never_exceeds_capacity() {
        let (data_db, info) = build_multichunk_lz4(20, 200, 200);
        let mut dec = ChunkDecompressor::new(info, CassandraVersion::V5_0Release).unwrap();
        let mut reader = std::io::Cursor::new(data_db);

        let (_, cap) = dec.cache_stats();
        for i in 0..20usize {
            let _ = dec
                .get_decompressed_chunk(&mut reader, i)
                .expect("chunk read");
            let (resident, _) = dec.cache_stats();
            assert!(
                resident <= cap,
                "resident {resident} must never exceed capacity {cap} (after chunk {i})"
            );
        }
        let (resident, _) = dec.cache_stats();
        assert_eq!(
            resident, cap,
            "reading 20 chunks fills the {cap}-entry cache"
        );
    }
}
