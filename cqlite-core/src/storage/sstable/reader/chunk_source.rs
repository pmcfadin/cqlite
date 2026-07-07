//! Single chunk decode plane: read → CRC → decompress → B1 cache (issue #1598, Epic G).
//!
//! Every query-path `Compression::decompress` call resolves here — the BTI target-chunk
//! read, the windowed streaming scan, the BIG point-read path, and the BIG reverse seek.
//! This is the ONLY module on the query path allowed to call `decompress`, proven by the
//! architecture test `tests/chunk_decode_single_plane.rs`.

use crate::storage::cache::{ChunkKey, DecompressedChunkCache};
use crate::storage::sstable::compression::Compression;
use crate::storage::sstable::compression_info::CompressionInfo;
use crate::storage::sstable::reader::block_io::read_compressed_chunk_at;
use crate::storage::sstable::reader::data_access::DECOMPRESS_CALLS;
use crate::storage::sstable::reader::read_at::ReadAt;
use crate::{Error, Result};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Single chunk decode plane: positioned read → CRC → decompress → B1 cache.
///
/// Composes the C2 read+CRC primitive (`read_compressed_chunk_at`) + the best-of-breed
/// decompress+cache tail (moved from the BTI block). Every query-path chunk read funnels
/// through one of three entry points:
/// - `chunk(index)`: whole-chunk read for the BTI/windowed-scan paths
/// - `range(offset, size)`: ranged read for the BIG point path (aux-keyed)
/// - `decode_and_cache`: shared decompress+cache tail for sites that already have compressed bytes
pub(crate) struct ChunkSource<'a> {
    /// Positioned read source (C2)
    source: &'a dyn ReadAt,
    /// Chunk offsets/sizes/algorithm
    comp_info: &'a CompressionInfo,
    /// Decompressor (None => raw passthrough)
    compression: Option<&'a Compression>,
    /// B1 decompressed-chunk cache
    cache: &'a DecompressedChunkCache,
    /// Data.db file size
    file_size: u64,
    /// Header offset (always 0 for NB/BTI)
    header_offset: u64,
    /// Cache-key namespace salt (NS_*)
    namespace: u64,
    /// Stable reader identity
    cache_id: u64,
}

impl<'a> ChunkSource<'a> {
    /// Construct a ChunkSource for positioned chunk reads.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        source: &'a dyn ReadAt,
        comp_info: &'a CompressionInfo,
        compression: Option<&'a Compression>,
        cache: &'a DecompressedChunkCache,
        file_size: u64,
        header_offset: u64,
        namespace: u64,
        cache_id: u64,
    ) -> Self {
        Self {
            source,
            comp_info,
            compression,
            cache,
            file_size,
            header_offset,
            namespace,
            cache_id,
        }
    }

    /// Whole-chunk read: positioned read → CRC → decompress → B1 cache.
    ///
    /// Used by the BTI target-chunk path (self-reading) and can be called by the windowed
    /// scan if it moves to positioned reads. Returns `Ok(None)` at EOF.
    pub(crate) fn chunk(&self, index: usize) -> Result<Option<Arc<[u8]>>> {
        // Build cache key from absolute chunk index in this namespace
        let key = ChunkKey::new(self.cache_id ^ self.namespace, index as u64);

        // B1 cache hit: Arc clone, no read, no decompress
        if let Some(hit) = self.cache.get(&key) {
            return Ok(Some(hit));
        }

        // Cache miss: positioned read + CRC (C2 primitive)
        let compressed = match read_compressed_chunk_at(
            self.source,
            self.comp_info,
            index,
            self.file_size,
            self.header_offset,
        )? {
            Some(c) => c,
            None => return Ok(None), // EOF
        };

        // Incompressible-raw passthrough or decompress, then cache
        let incompressible = compressed.len() >= self.comp_info.max_compressed_length as usize;
        Ok(Some(self.decode_and_cache(
            key,
            compressed,
            incompressible,
        )?))
    }

    /// Ranged (offset, size) read for the BIG point path (aux-keyed by size).
    ///
    /// The decompressed bytes depend on BOTH offset and size, so `size` is carried as
    /// the key's aux discriminant (roborev #1567).
    pub(crate) fn range(&self, offset: u64, size: u32) -> Result<Arc<[u8]>> {
        // Build ranged cache key (aux = size)
        let key = ChunkKey::with_aux(self.cache_id ^ self.namespace, offset, size as u64);

        // B1 cache hit: Arc clone, no read, no decompress
        if let Some(hit) = self.cache.get(&key) {
            return Ok(hit);
        }

        // Cache miss: positioned read at (offset, size)
        let mut buffer = vec![0u8; size as usize];
        self.source.read_exact_at(offset, &mut buffer)?;

        // BIG point read is always compressible (no incompressible-raw branch here)
        self.decode_and_cache(key, buffer, false)
    }

    /// Shared decompress+cache tail: decompress (or raw-passthrough) → insert → Arc.
    ///
    /// Called by `chunk()`/`range()` after their own read+CRC, and directly by the
    /// windowed-scan blocking half (which receives compressed bytes over a channel) and
    /// the BIG reverse seek (which reads via a cursor). This is the ONLY place on the
    /// query path where `Compression::decompress` is called — the architecture test
    /// proves it.
    pub(crate) fn decode_and_cache(
        &self,
        key: ChunkKey,
        compressed: Vec<u8>,
        incompressible: bool,
    ) -> Result<Arc<[u8]>> {
        let decompressed = if incompressible {
            // Stored uncompressed by Cassandra: pass raw bytes through (no decompress counter)
            compressed
        } else if let Some(compression) = self.compression {
            // Decompress: the single query-path decompress call site
            let d = compression.decompress(&compressed).map_err(|e| {
                Error::corruption(format!(
                    "ChunkSource: failed to decompress chunk (key={:?}): {}",
                    key, e
                ))
            })?;
            DECOMPRESS_CALLS.fetch_add(1, Ordering::Relaxed);
            d
        } else {
            // No compression reader: treat raw bytes as decompressed
            compressed
        };

        // Insert into B1 cache (Vec→Arc conversion happens once here) and return
        Ok(self.cache.insert(key, decompressed))
    }

    /// Decompress-only helper for the BIG reverse path: decompress without caching.
    ///
    /// Preserves the current uncached behavior of `pull_reverse_chunk` — no B1 cache
    /// insertion, no DECOMPRESS_CALLS counter (separate work_counters::add_chunk_decompressed).
    /// This exists ONLY to consolidate the `Compression::decompress` call site into this
    /// module while changing zero runtime behavior on the reverse path.
    pub(crate) fn decompress_only(
        compression: Option<&Compression>,
        compressed: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if let Some(c) = compression {
            c.decompress(&compressed).map_err(|e| {
                Error::corruption(format!(
                    "ChunkSource: reverse-path decompress failed: {}",
                    e
                ))
            })
        } else {
            Ok(compressed)
        }
    }
}
