//! Single chunk decode plane: read → CRC → decompress → B1 cache (issue #1598, Epic G).
//!
//! The point-read (BTI get, BIG point), windowed-scan, and BIG-reverse decode paths all
//! resolve their `Compression::decompress` here. The `iterate_all_partitions` and
//! `sequential_scan` decode site (`parse_block_entries` in `parsing/block_entries.rs`)
//! also routes its block decompress through `ChunkSource::decompress_only` (issue #2165),
//! so `parsing/` no longer calls `Compression::decompress` inline. `parsing/` retains the
//! legacy `self.file` + `compression_reader` block-read/CRC model (not `ReadAt` +
//! `CompressionInfo` + chunk-index); only the decompress step is consolidated here.
//! Architecture test: `tests/chunk_decode_single_plane.rs`.

use crate::observability::read_metrics;
use crate::storage::cache::{ChunkKey, DecompressedChunkCache};
use crate::storage::sstable::compression::{Compression, CompressionAlgorithm};
use crate::storage::sstable::compression_info::CompressionInfo;
use crate::storage::sstable::reader::block_io::read_compressed_chunk_at;
use crate::storage::sstable::reader::data_access::DECOMPRESS_CALLS;
use crate::storage::sstable::reader::read_at::ReadAt;
use crate::{Error, Result};
use bytes::Bytes;
use std::sync::atomic::Ordering;

/// Single chunk decode plane: positioned read → CRC → decompress → B1 cache.
///
/// Composes the C2 read+CRC primitive (`read_compressed_chunk_at`) + the best-of-breed
/// decompress+cache tail (moved from the BTI block). Every query-path chunk read funnels
/// through one of these entry points, each of which decompresses ONLY CRC-validated bytes:
/// - `chunk(index)`: whole-chunk read (self read+CRC) for the BTI/windowed-scan paths
/// - `decode_and_cache`: shared decompress+cache tail for sites that already have
///   CRC-validated compressed bytes (windowed-scan blocking half; the compressed
///   offset-read window of `read_compressed_offset_window`, issue #1773)
/// - `decompress_only`: uncached decompress for the BIG reverse path and the
///   compressed offset-read window (both already CRC-validated upstream)
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

/// The bounded [`crate::observability::catalog::attr::COMPRESSION`] label for a
/// decode site's configured algorithm (issue #1701) — `"none"` when there is none,
/// never an absent label. Free function taking the ALGORITHM so every decode site
/// resolves the label identically whether it holds a `Compression`, a
/// `CompressionReader`, or nothing.
pub(crate) fn compression_label_of(algorithm: Option<&CompressionAlgorithm>) -> &'static str {
    match algorithm {
        Some(a) => read_metrics::compression_attr(a),
        None => read_metrics::COMPRESSION_NONE,
    }
}

/// Count a chunk the caller hands through RAW — Cassandra stored it uncompressed
/// because its compressed length would have met `max_compressed_length`, or the
/// SSTable has no compressor at all — into
/// [`crate::observability::catalog::READ_BYTES`], exactly as a DECOMPRESSED chunk's
/// bytes are counted (issue #1701, roborev F3).
///
/// # Why this lives in the plane rather than at each call site
///
/// FIVE decode exits do that raw passthrough THEMSELVES (`len >=
/// max_compressed_length`, or "no compressor, the buffer already holds finished
/// bytes") and so never reach [`ChunkSource::decompress_only`] /
/// [`ChunkSource::decode_and_cache`]: the windowed scan's IO half (two exits), the
/// compressed offset-read window, the BIG promoted seek window, and the stitch path.
/// Every one was a silent hole in `read.bytes` — a read reporting rows and a duration
/// while reporting fewer bytes than it actually read. Counting them independently at
/// five sites would leave the NEXT such exit free to bypass the metric again, so the
/// counting is ONE named plane function they all call: greppable from the plane, and
/// the thing to reach for when a sixth raw exit appears.
pub(crate) fn count_raw_chunk(bytes: &[u8], algorithm: Option<&CompressionAlgorithm>) {
    read_metrics::record_decompressed_bytes(bytes.len(), compression_label_of(algorithm));
}

/// [`count_raw_chunk`] for a site that hands the buffer ON as its own value: counts,
/// then returns it unchanged. Same single boundary — this exists only so a site whose
/// raw exit is one expression (`Ok(whole)`) can route through it without restructuring.
pub(crate) fn counted_raw_chunk(
    bytes: Vec<u8>,
    algorithm: Option<&CompressionAlgorithm>,
) -> Vec<u8> {
    count_raw_chunk(&bytes, algorithm);
    bytes
}

/// Credit an UNCOMPRESSED block read to `cqlite.read.bytes` (issue #1701, roborev
/// F1/F3), returning the read unchanged.
///
/// Gated on `compression_info.is_none()`: for an SSTable with NO
/// `CompressionInfo.db` the block bytes ARE the finished `Data.db` payload and never
/// reach a decompress step, so this is their only counting opportunity. A COMPRESSED
/// read must NOT be counted here — those bytes are still compressed and the plane
/// counts that read's payload post-decompression, so counting both would report one
/// read twice under two different sizes.
///
/// Lives here rather than in `block_io` so every `read.bytes` increment in the crate
/// stays inside this module (grep `record_decompressed_bytes`).
pub(crate) fn count_uncompressed_block(
    compression_info: &Option<std::sync::Arc<CompressionInfo>>,
    read: Result<Option<Vec<u8>>>,
) -> Result<Option<Vec<u8>>> {
    if compression_info.is_none() {
        if let Ok(Some(block)) = &read {
            count_raw_chunk(block, None);
        }
    }
    read
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

    /// The bounded [`catalog::attr::COMPRESSION`] label for this source's
    /// decompressor (issue #1701). Derived from the `CompressionAlgorithm` ENUM,
    /// never from the algorithm string parsed out of `CompressionInfo.db` — a
    /// file-controlled string would be an unbounded metric dimension.
    ///
    /// No decompressor is `"none"`, NOT an absent label: an uncompressed SSTable is a
    /// real, first-class read path (CQLite's own writer emits only uncompressed
    /// SSTables — the #1406 claim boundary), so its bytes are attributed to a named
    /// series rather than an anonymous one.
    ///
    /// [`catalog::attr::COMPRESSION`]: crate::observability::catalog::attr::COMPRESSION
    fn compression_label(&self) -> &'static str {
        compression_label_of(self.compression.map(|c| c.algorithm()))
    }

    /// Whole-chunk read: positioned read → CRC → decompress → B1 cache.
    ///
    /// Used by the BTI target-chunk path (self-reading) and can be called by the windowed
    /// scan if it moves to positioned reads. Returns `Ok(None)` at EOF.
    pub(crate) fn chunk(&self, index: usize) -> Result<Option<Bytes>> {
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

    /// Shared decompress+cache tail: decompress (or raw-passthrough) → insert → Arc.
    ///
    /// Called by `chunk()` after its own read+CRC, and directly by the windowed-scan
    /// blocking half (which receives CRC-validated compressed bytes over a channel).
    /// Together with `decompress_only`, this is the ONLY place on the query path where
    /// `Compression::decompress` is called — the architecture test proves it.
    pub(crate) fn decode_and_cache(
        &self,
        key: ChunkKey,
        compressed: Vec<u8>,
        incompressible: bool,
    ) -> Result<Bytes> {
        if incompressible {
            // Stored uncompressed by Cassandra: pass the raw bytes through (no
            // decompress counter), but COUNT them — they are Data.db payload this
            // read materialised, exactly like a decompressed chunk's (issue #1701 F3).
            count_raw_chunk(&compressed, self.compression.map(|c| c.algorithm()));
            return Ok(self.cache.insert(key, compressed));
        }
        let decompressed = if let Some(compression) = self.compression {
            // Decompress: the single query-path decompress call site
            let d = compression.decompress(&compressed).map_err(|e| {
                Error::corruption(format!(
                    "ChunkSource: failed to decompress chunk (key={:?}): {}",
                    key, e
                ))
            })?;
            DECOMPRESS_CALLS.fetch_add(1, Ordering::Relaxed);
            // CHUNK_PATH_ALLOCS (consumer E3/#1940): the decompress OUTPUT buffer is
            // a per-chunk copy-chain heap allocation. It is the ONE surviving
            // allocation after the D2 substrate work — it flows zero-copy into the
            // B1 cache as `Bytes` (no `Arc::from` re-copy) and is the refcounted
            // substrate the window borrows. Recording it here (not the old
            // compressed-read-buffer site, which D2 turned into a reused scratch)
            // makes the ≤1-alloc/chunk bound measurable. No-op in release.
            crate::storage::sstable::read_work_counters::record_chunk_path_alloc();
            d
        } else {
            // No compression reader: treat raw bytes as decompressed
            compressed
        };

        // cqlite.read.bytes (issue #1701): the DECOMPRESSED Data.db payload this
        // chunk materialised, counted ONCE per chunk decode — the coarsest grain at
        // which the decompressed size is known, and never per row. A chunk served
        // from the B1 cache returns above without reaching here, which is correct:
        // a cache hit read no Data.db bytes.
        read_metrics::record_decompressed_bytes(decompressed.len(), self.compression_label());

        // Insert into B1 cache (Vec→Arc conversion happens once here) and return
        Ok(self.cache.insert(key, decompressed))
    }

    /// Decompress a CRC-validated COMPRESSIBLE chunk from a BORROWED slice, cache
    /// it, and return the resident [`Bytes`] — leaving the compressed input buffer
    /// owned by the caller so it can be RECYCLED as a per-loop scratch (issue #1940,
    /// D2). This is the windowed-scan IO half's decode entry: unlike
    /// [`decode_and_cache`](Self::decode_and_cache) (which takes the compressed `Vec`
    /// by value and drops it), this borrows `&compressed`, so the caller keeps the
    /// compressed buffer and reuses it for the next chunk read — the compressed-read
    /// side then performs no per-chunk allocation, and the ONE surviving copy-chain
    /// allocation is the decompress output (recorded here). Callers handle the
    /// incompressible-raw and no-compressor cases directly (they move the buffer);
    /// this method is only for the decompress path (`self.compression` is `Some`).
    /// A `None` compressor is treated as a raw passthrough of a COPY (rare/never on
    /// the windowed path, where compression is always resolved).
    pub(crate) fn decode_borrowed(&self, key: ChunkKey, compressed: &[u8]) -> Result<Bytes> {
        let decompressed = if let Some(compression) = self.compression {
            let d = compression.decompress(compressed).map_err(|e| {
                Error::corruption(format!(
                    "ChunkSource: failed to decompress chunk (key={:?}): {}",
                    key, e
                ))
            })?;
            DECOMPRESS_CALLS.fetch_add(1, Ordering::Relaxed);
            // CHUNK_PATH_ALLOCS (consumer E3/#1940): the decompress OUTPUT buffer is
            // the ONE surviving per-chunk copy-chain allocation after D2 — it flows
            // zero-copy into the B1 cache as `Bytes` (no `Arc::from` re-copy) and is
            // the refcounted substrate the window borrows. No-op in release.
            crate::storage::sstable::read_work_counters::record_chunk_path_alloc();
            d
        } else {
            compressed.to_vec()
        };
        // cqlite.read.bytes (issue #1701) — same per-chunk grain as
        // `decode_and_cache`; this is the windowed scan's decode entry.
        read_metrics::record_decompressed_bytes(decompressed.len(), self.compression_label());
        Ok(self.cache.insert(key, decompressed))
    }

    /// Decompress-only helper for already-CRC-validated compressed buffers: decompress
    /// without caching.
    ///
    /// Consumers (all uncached — no B1 cache insertion, no DECOMPRESS_CALLS counter;
    /// their own counters, e.g. work_counters::add_chunk_decompressed, apply):
    /// - the BIG reverse/seek window path (`decompress_partition_window` via
    ///   `block_io::read_compressed_chunk_at`),
    /// - the stitch path (`stitch_all_chunks`, `data_access/mod.rs`),
    /// - the sequential-scan block decode (`parse_block_entries`, issue #2165).
    ///
    /// This exists to consolidate the `Compression::decompress` call site into this
    /// module while changing zero runtime behavior on those paths.
    pub(crate) fn decompress_only(
        compression: Option<&Compression>,
        compressed: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if let Some(c) = compression {
            let compressed_len = compressed.len();
            let decompressed = c.decompress(&compressed).map_err(|e| {
                Error::corruption(format!(
                    "ChunkSource: decompress failed ({} compressed bytes): {}",
                    compressed_len, e
                ))
            })?;
            // cqlite.read.bytes (issue #1701): the uncached decompress sites (the
            // sequential-scan block decode, the stitch path, the BIG reverse/seek
            // window) read Data.db bytes exactly like the cached ones, so they are
            // counted at the same per-chunk grain.
            read_metrics::record_decompressed_bytes(
                decompressed.len(),
                read_metrics::compression_attr(c.algorithm()),
            );
            Ok(decompressed)
        } else {
            // No compressor: these bytes ARE the `Data.db` payload, so they are read
            // work and must be counted (issue #1701, roborev B2). Skipping this branch
            // left every UNCACHED UNCOMPRESSED read — the shape CQLite's own writer
            // produces (#1406) — invisible to `cqlite.read.bytes`.
            read_metrics::record_decompressed_bytes(
                compressed.len(),
                read_metrics::COMPRESSION_NONE,
            );
            Ok(compressed)
        }
    }
}
