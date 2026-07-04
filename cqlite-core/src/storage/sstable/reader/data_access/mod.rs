//! Data access methods for SSTableReader.
//!
//! This module contains all methods related to reading data from SSTables,
//! split by read path:
//! - [`model`] — shared model types and free helpers (clustering slice,
//!   table-id matching, token-order sorting, the `scan_for_key` counter).
//! - [`bti`] — BTI ("da") trie-resolved point lookups, single-partition seeks,
//!   clustering-slice narrowing, and the whole-Data.db BTI scan.
//! - [`sequential`] — sequential / index-driven range and full scans, the
//!   `scan_for_key` fallback, cell-metadata scan, and the streaming scan.
//! - [`compaction`] — timestamp-preserving compaction iteration and the bounded
//!   streaming compaction driver.
//!
//! `mod.rs` keeps the shared entry points and the I/O / parser infrastructure
//! every read path builds on (`get`, `build_v5_parser`, `new_scan_cursor`,
//! `read_next_block`, the offset-read helpers, the chunk-stitch helpers, and the
//! delta-scan bridge), plus the public-surface re-exports.

mod bti;
// BIG ("nb") promoted-index forward seek + reverse iterator (Issue #1184). The
// seek/reverse paths exist only on the default build, so the whole module is
// `not(tombstones)` gated.
#[cfg(not(feature = "tombstones"))]
mod big_promoted;
// In-crate proof that the promoted-index / reverse-lookup uncompressed read path
// verifies CRC.db before parsing (issue #1396, roborev Fix 1). It calls the
// pub(crate) `big_reverse_partition_rows`, so it cannot live in `tests/`.
#[cfg(all(test, not(feature = "tombstones")))]
mod big_promoted_crc_tests;
// In-crate proof that the BIG point-read chunk fetch (`get_cached_data`) consults
// the shared decompressed-chunk cache (issue #1567). Needs `pub(crate)` reader
// state (`actual_header_size`) to build a valid offset, so it cannot live in
// `tests/`.
#[cfg(test)]
mod chunk_cache_wiring_tests;
// BIG ("nb"/uncompressed) point lookup: raw-key Index.db resolve + covering-chunk
// seek (issue #1572), replacing the whole-file scan_for_key fallback.
mod big_point;
mod compaction;
mod model;
mod sequential;

// Public surface re-export (unchanged: `reader::mod` re-exports
// `data_access::ClusteringSlice`).
pub use model::ClusteringSlice;

// Re-export for the sibling `scan_stream_windowed` module, which references
// `data_access::table_ids_match` (unchanged path).
pub(in crate::storage::sstable::reader) use model::table_ids_match;

// Re-export the decompress-work counter so the sibling `scan_stream_windowed`
// module (outside `data_access`) can increment it on the windowed-scan miss path
// (issue #1567). `model` is a private submodule, so the raw path is not reachable
// from `reader::scan_stream_windowed`; this widens the path exactly enough.
pub(in crate::storage::sstable::reader) use model::DECOMPRESS_CALLS;

use super::source::ScanCursor;
use super::SSTableReader;
use crate::parser::DataFormat;
use crate::storage::cache::ChunkKey;
use crate::types::{CellWriteMetadata, ScanRow, TableId};
use crate::{Error, Result, RowKey};
use log::{debug, warn};
use std::io::SeekFrom;
use std::sync::atomic::Ordering;
use tokio::io::AsyncSeekExt;

// Per-site cache key namespaces (design D4): fold a site discriminator into the
// sstable-identity field of [`ChunkKey`] so numerically-overlapping keys from
// different read sites (an index-resolved `block_offset` vs a small chunk index)
// can never collide on the shared cache. The acceptance criterion is per-site
// consultation + repeat-read hits, not that a physical chunk shares one key
// across differently-granular sites, so distinct namespaces are correct.
pub(super) const NS_BIG_POINT: u64 = 0;
pub(super) const NS_BTI_CHUNK: u64 = 0x9E37_79B9_7F4A_7C15;
pub(super) const NS_WINDOWED_CHUNK: u64 = 0xC2B2_AE3D_27D4_EB4F;

impl SSTableReader {
    /// Build a [`ChunkKey`] for `chunk_index` in the given per-site `namespace`,
    /// bound to this reader's stable cache identity. See the `NS_*` salts.
    #[inline]
    pub(crate) fn chunk_cache_key(&self, namespace: u64, chunk_index: u64) -> ChunkKey {
        ChunkKey::new(self.chunk_cache_id ^ namespace, chunk_index)
    }

    /// Build a [`ChunkKey`] for a size-dependent range read (the BIG point-read
    /// path): the decompressed bytes depend on BOTH `offset` and `size`, so
    /// `size` is carried as the key's `aux` discriminant. Keying by `offset`
    /// alone would alias two reads at the same offset with different sizes and
    /// return the first-cached range (roborev #1567).
    #[inline]
    pub(crate) fn chunk_cache_key_ranged(
        &self,
        namespace: u64,
        offset: u64,
        size: u32,
    ) -> ChunkKey {
        ChunkKey::with_aux(self.chunk_cache_id ^ namespace, offset, size as u64)
    }

    /// The shared decompressed-chunk cache this reader consults (issue #1567).
    ///
    /// Exposed so callers/tests can observe cache residency and per-instance
    /// hit/miss counts (parallelism-immune, unlike the process-global
    /// [`decompress_call_count`](Self::decompress_call_count)).
    pub fn chunk_cache(&self) -> &std::sync::Arc<crate::storage::cache::DecompressedChunkCache> {
        &self.chunk_cache
    }

    /// Process-global count of actual chunk decompressions at the wired read
    /// sites (issue #1567). Tests reset around a cold/warm read pair and assert a
    /// warm-read delta of 0 to prove the hit skipped decompression.
    pub fn decompress_call_count() -> u64 {
        model::DECOMPRESS_CALLS.load(Ordering::Relaxed)
    }

    /// Reset the decompress-work counter to zero (test/instrumentation harness).
    pub fn reset_decompress_calls() {
        model::DECOMPRESS_CALLS.store(0, Ordering::Relaxed);
    }

    /// Process-global count of compressed-bytes reads at the BIG point-read site
    /// (issue #1567). A warm point read that hits the cache leaves this unchanged.
    pub fn chunk_read_call_count() -> u64 {
        model::CHUNK_READ_CALLS.load(Ordering::Relaxed)
    }

    /// Reset the chunk-read counter to zero (test/instrumentation harness).
    pub fn reset_chunk_read_calls() {
        model::CHUNK_READ_CALLS.store(0, Ordering::Relaxed);
    }

    /// Return `true` when Data.db uses the V5CompressedLegacy NB chunked format and
    /// therefore requires all chunks to be stitched before parsing.
    ///
    /// The correct predicate is:
    ///   data_format == V5CompressedLegacy  AND  is_nb_format()
    ///
    /// Rationale:
    /// - `V5CompressedLegacy` identifies the row serialization format (u16 length
    ///   prefixes, legacy encoding) used by all Cassandra 5 'nb' SSTables.
    /// - `is_nb_format()` identifies the chunked-compression read path. It intentionally
    ///   EXCLUDES `V5_0Uncompressed`, which uses the same row format but stores data as
    ///   a single contiguous block (no chunk boundaries, no stitching needed).
    /// - Using `is_compressed` (compression_reader.is_some()) would be wrong for NB
    ///   format because the per-chunk decompression is handled inside `stitch_and_parse_all_chunks`,
    ///   and `is_compressed` may differ from `is_nb_format` for edge-case versions.
    pub(super) fn requires_chunk_stitching(&self) -> bool {
        let data_format = self.header.cassandra_version.data_format();
        matches!(data_format, DataFormat::V5CompressedLegacy)
            && self.header.cassandra_version.is_nb_format()
    }

    /// Test-only: return the fully stitched + decompressed Data.db data section
    /// for a V5CompressedLegacy ("nb") SSTable, or `None` for any other format.
    ///
    /// This exposes the exact on-disk bytes (post-decompression) that the
    /// production scan path parses, so the Issue #1623 corpus-differential test
    /// can locate REAL Cassandra `writeUnsignedVInt` length prefixes and route
    /// those literal bytes through [`crate::parser::vint::parse_vint_length`].
    /// It replicates the seek + stitch preamble of `sequential_scan` exactly.
    #[cfg(test)]
    pub(crate) async fn stitched_data_section_for_tests(&self) -> Result<Option<Vec<u8>>> {
        if !self.requires_chunk_stitching() {
            return Ok(None);
        }
        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        let stitched = self.stitch_all_chunks(&cursor).await?;
        Ok(Some(stitched))
    }

    /// Get a value by key from the SSTable.
    ///
    /// Resolution-mode-agnostic entry point: callers that do not carry the
    /// manager's `resolve_reader_list` signal (e.g. the per-reader helpers in
    /// `partition_lookup`, `schema_aware_reader`, and benchmarks) get the STRICT
    /// table-consistency guard — `fully_qualified_match = false` reproduces exactly
    /// today's `table_ids_match_strict` behavior on the BTI point-lookup path, so
    /// this is a behavior-preserving conservative default. The manager's `get()`
    /// calls [`SSTableReader::get_with_resolution`] with the authoritative signal so
    /// an exact fully-qualified match can accept rows across a benign header-keyspace
    /// divergence (issue #1321, mirroring the seek path #1284).
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<ScanRow>> {
        self.get_with_resolution(table_id, key, false).await
    }

    /// Get a value by key, threading the authoritative resolution mode
    /// (`fully_qualified_match`) into the BTI point-lookup guard (issue #1321).
    ///
    /// See [`SSTableReader::get`] for the resolution-mode contract. Only the BTI
    /// ("da") point-lookup path consults `fully_qualified_match`; the bloom/Index.db/
    /// sequential fallbacks are unaffected by it.
    pub async fn get_with_resolution(
        &self,
        table_id: &TableId,
        key: &RowKey,
        fully_qualified_match: bool,
    ) -> Result<Option<ScanRow>> {
        // Issue #831 / #909: BTI ("da") readers resolve partitions via the
        // Partitions.db trie (O(log n)), never via Index.db (absent for BTI) or
        // the sequential scan. The trie is the AUTHORITATIVE presence oracle for a
        // BTI SSTable — it answers present/absent definitively — so we branch here
        // BEFORE the bloom-filter pre-check. Skipping the bloom filter for BTI is
        // both correct (the trie is authoritative; bloom is only an optimization)
        // and necessary: a writer-produced Filter.db whose hashing does not match
        // the reader's would otherwise cause false negatives and drop live
        // partitions (the writer→reader roundtrip #909 must read back). It also
        // guarantees a BTI get() can never fall through to scan_for_key.
        if self.bti_partitions_db.is_some() {
            return self
                .bti_point_lookup(table_id, key, fully_qualified_match)
                .await;
        }

        // BIG ("nb"/uncompressed) readers: raw-key Index.db resolve + covering-chunk
        // seek (issue #1572). The bloom pre-check, the fast Index.db-resolved
        // chunk-targeted decode, and the index-less `scan_for_key` fallback all live
        // in `big_point`. Before #1572 this path called `self.index.find_entry()`
        // with raw key bytes against a *digest*-keyed map (always a miss, issue
        // #517), so every lookup fell through to a whole-file `scan_for_key`.
        self.big_get_with_resolution(table_id, key, fully_qualified_match)
            .await
    }

    /// Stitch all compressed chunks and parse as a single buffer (V5CompressedLegacy)
    ///
    /// This helper method extracts the stitching logic from get_all_entries so it can be
    /// reused by sequential_scan and other methods that need to handle V5CompressedLegacy
    /// format where partitions can span chunk boundaries.
    ///
    /// `read_shadowing` (issue #1741): `true` for user-facing SELECT scans
    /// (`scan_for_key`, `sequential_scan`), `false` for the physical
    /// `get_all_entries` (integrity verification / data-manager) so it counts every
    /// on-disk row.
    pub(super) async fn stitch_and_parse_all_chunks(
        &self,
        cursor: &ScanCursor,
        schema: Option<&crate::schema::TableSchema>,
        read_shadowing: bool,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        let stitched_buffer = self.stitch_all_chunks(cursor).await?;
        let parser = self.build_v5_parser(read_shadowing);

        // Get schema (use provided schema or reader's schema)
        let reader_schema;
        let table_schema = if let Some(s) = schema {
            Some(s)
        } else {
            reader_schema = self.get_table_schema(None);
            reader_schema.as_ref()
        };

        // Parse the stitched decompressed buffer
        let entries = parser.parse_block(&stitched_buffer, table_schema, self)?;
        log::debug!(
            "stitch_and_parse_all_chunks: Parsed {} entries from stitched buffer",
            entries.len()
        );

        Ok(entries)
    }

    /// Like [`stitch_and_parse_all_chunks`] but also returns per-cell write metadata.
    ///
    /// Used when `ProjectionFlags::include_cell_metadata` is set (issue #693).
    ///
    /// [`stitch_and_parse_all_chunks`]: Self::stitch_and_parse_all_chunks
    pub(super) async fn stitch_and_parse_all_chunks_with_metadata(
        &self,
        cursor: &ScanCursor,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<
        Vec<(
            TableId,
            RowKey,
            ScanRow,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
    > {
        let stitched_buffer = self.stitch_all_chunks(cursor).await?;
        let parser = self.build_v5_parser(true);

        let reader_schema;
        let table_schema = if let Some(s) = schema {
            Some(s)
        } else {
            reader_schema = self.get_table_schema(None);
            reader_schema.as_ref()
        };

        let entries =
            parser.parse_block_with_cell_metadata(&stitched_buffer, table_schema, self)?;
        log::debug!(
            "stitch_and_parse_all_chunks_with_metadata: Parsed {} entries with metadata",
            entries.len()
        );

        Ok(entries)
    }

    /// Read, decompress, and concatenate every compressed chunk of the data
    /// section into a single buffer.
    ///
    /// V5CompressedLegacy partitions can span chunk boundaries, so the whole
    /// data section must be stitched before parsing. The returned buffer is
    /// bounded by the *uncompressed data-section size* — it scales with on-disk
    /// bytes, not row count (issue #790).
    ///
    /// Precondition: the caller has seeked `cursor`'s file to the start of the
    /// data section (the cursor's chunk index starts at 0 when freshly minted).
    pub(super) async fn stitch_all_chunks(&self, cursor: &ScanCursor) -> Result<Vec<u8>> {
        use crate::storage::sstable::compression::Compression;

        // Pre-allocate buffer for ~2.5MB (estimated max size for test data)
        let mut stitched_buffer = Vec::with_capacity(2_500_000);

        // Incompressible-chunk fallback (Bug #639, epic #970): Cassandra stores a
        // chunk RAW (not compressed) when its compressed length would meet or
        // exceed `max_compressed_length`. `ChunkDecompressor::decompress_chunk`
        // already honours this, but the stitch path did not — it blindly tried to
        // LZ4/Snappy/etc-decode a raw chunk, which fails on the `incompressible`
        // fixture. Mirror the writer rule here: when the (CRC-stripped) chunk
        // length >= max_compressed_length, the bytes are already plaintext.
        // Authority: CompressedSequentialWriter.java:160-177.
        let max_compressed_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.max_compressed_length as usize)
            .unwrap_or(usize::MAX);

        let mut chunk_count = 0;
        while let Some(compressed_chunk) = self.read_next_block(cursor).await? {
            let decompressed_chunk = if compressed_chunk.len() >= max_compressed_length {
                // Stored uncompressed by Cassandra — pass the raw bytes through.
                log::debug!(
                    "stitch_all_chunks: chunk {} is incompressible (len={} >= max_compressed_length={}), using raw bytes",
                    chunk_count,
                    compressed_chunk.len(),
                    max_compressed_length
                );
                compressed_chunk
            } else if let Some(compression_reader) = &self.compression_reader {
                let compression = Compression::new(*compression_reader.algorithm())?;
                match compression.decompress(&compressed_chunk) {
                    Ok(decompressed) => decompressed,
                    Err(e) => {
                        return Err(Error::corruption(format!(
                            "stitch_all_chunks: Failed to decompress chunk {}: {}",
                            chunk_count, e
                        )));
                    }
                }
            } else {
                // No compression (should not happen for V5CompressedLegacy)
                log::warn!("stitch_all_chunks: No compression reader, using raw chunk data");
                compressed_chunk
            };

            stitched_buffer.extend_from_slice(&decompressed_chunk);
            chunk_count += 1;
        }

        log::debug!(
            "stitch_all_chunks: Stitched {} chunks, total buffer: {} bytes",
            chunk_count,
            stitched_buffer.len()
        );

        Ok(stitched_buffer)
    }

    /// Build a [`V5CompressedLegacyParser`] configured from this reader's header,
    /// statistics (EncodingStats), version gates, and UDT registry.
    ///
    /// `read_shadowing` (issue #1741) MUST be `true` for user-facing query reads
    /// (`scan`/`scan_stream`/`scan_with_cell_metadata`/point `get`) so the emit path
    /// applies SELECT-semantic partition/range-tombstone shadowing and TTL expiry, and
    /// `false` for PHYSICAL consumers that must see every on-disk row — integrity
    /// verification via `get_all_entries`, `sstable_data_manager`, delta-scan, and the
    /// compaction read path (which reconciles tombstones itself across generations).
    ///
    /// [`V5CompressedLegacyParser`]: crate::storage::sstable::reader::parsing::V5CompressedLegacyParser
    pub(super) fn build_v5_parser(
        &self,
        read_shadowing: bool,
    ) -> crate::storage::sstable::reader::parsing::V5CompressedLegacyParser {
        let keyspace = self.header.keyspace.clone();
        let table_name = self.header.table_name.clone();

        // Extract EncodingStats from statistics_reader (if available)
        let (min_timestamp, min_local_deletion_time, min_ttl) =
            if let Some(stats_reader) = &self.statistics_reader {
                let ts_stats = &stats_reader.statistics().timestamp_stats;
                (
                    ts_stats.min_timestamp,
                    ts_stats.min_deletion_time,
                    ts_stats.min_ttl,
                )
            } else {
                (0, 0, None)
            };

        let parser = crate::storage::sstable::reader::parsing::V5CompressedLegacyParser::new(
            keyspace,
            table_name,
            min_timestamp,
            min_local_deletion_time,
            min_ttl,
        )
        // VG1: thread VersionGates from SSTableReader down to row parser so
        // that VG3 can flip gate-sensitive code paths without re-deriving gates.
        .with_version_gates(self.version_gates.clone())
        // Issue #1741: SELECT-semantic read shadowing (see fn docs).
        .with_read_shadowing(read_shadowing);
        // Add UDT registry if available for UDT-aware collection parsing (Issue #238)
        if let Some(ref registry) = self.udt_registry {
            parser.with_udt_registry(registry.clone())
        } else {
            parser
        }
    }

    /// Read value at a specific offset with caching
    pub async fn read_value_at_offset(&self, offset: u64, size: u32) -> Result<Option<ScanRow>> {
        use crate::parser::header::CassandraVersion;
        use crate::storage::sstable::compression::Compression;

        // Size must be non-zero for offset-based reading
        if size == 0 {
            return Err(Error::corruption(format!(
                "Cannot read value at offset {} with size=0. This should have been caught earlier and handled via sequential scan.",
                offset
            )));
        }

        // Read-time CRC verification for uncompressed BIG SSTables (issue #1396).
        // The index-based scan and point-lookup paths reach Data.db here (bypassing
        // read_next_block / read_uncompressed_data_block), so verify the CRC.db
        // chunk(s) covering [offset, offset+size) BEFORE returning any bytes. A
        // mismatch is a typed Error::Corruption naming the chunk + offset (never
        // wrong values / never a silent result). No-op when no CRC.db is present
        // (compressed tables / BTI / absent-CRC.db warn-and-proceed).
        self.verify_uncompressed_range(offset, size).await?;

        // Use cached reading with metrics tracking
        let buffer = self.get_cached_data(offset, size).await?;

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression = Compression::new(*compression_reader.algorithm())?;
            match compression.decompress(&buffer) {
                Ok(decompressed) => {
                    debug!(
                        "Successfully decompressed {} bytes to {} bytes",
                        buffer.len(),
                        decompressed.len()
                    );
                    decompressed
                }
                Err(e) => {
                    // For modern formats (4.x/5.x), decompression failure is an error
                    if self.header.cassandra_version != CassandraVersion::Legacy {
                        return Err(Error::corruption(format!(
                            "Decompression failed for modern format at offset={}, size={}, algorithm={:?}: {}",
                            offset,
                            size,
                            compression_reader.algorithm(),
                            e
                        )));
                    } else {
                        // Only allow fallback for legacy formats
                        warn!(
                            "Decompression failed for legacy format ({}), using raw data",
                            e
                        );
                        debug!(
                            "First 32 bytes of raw data: {:02x?}",
                            &buffer[..std::cmp::min(32, buffer.len())]
                        );
                        buffer
                    }
                }
            }
        } else {
            buffer
        };

        // Preserve raw data until schema is available. Pre-#1334 this offset-read
        // placeholder returned a bare `Value::Blob` of the row's raw value bytes,
        // which `SchemaAwareReader` then schema-decoded and the no-schema query
        // layer surfaced as a synthetic single-column "data" row. Carry that RAW
        // provenance explicitly (issue #1334): a schema-aware consumer decodes the
        // bytes; a no-schema consumer surfaces a single "data" blob — with no
        // downstream guessing from the value's shape.
        let row = ScanRow::RawRow(data.to_vec());

        // Extract write time from value (placeholder - would need to be parsed from SSTable)
        let _write_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or_else(|e| {
                warn!("Failed to get system time: {}; using fallback value 0", e);
                0
            });

        // Filter out tombstones and expired data
        if !self.filter_tombstone(&row) {
            return Ok(None);
        }

        Ok(Some(row))
    }

    /// Read block with caching support and hit/miss tracking.
    ///
    /// Wired to the shared [`DecompressedChunkCache`] (issue #1567): the cache is
    /// consulted BEFORE the file read, so a repeat read of a resident region is a
    /// refcount-bump hit that touches the file zero times and re-decompresses
    /// nothing. Keyed by the index-resolved `block_offset` in the BIG-point-read
    /// namespace ([`NS_BIG_POINT`]), which is disjoint from the chunk-index
    /// namespaces used by the windowed-scan and BTI sites. The returned buffer is
    /// already CRC-verified: [`read_value_at_offset`](Self::read_value_at_offset)
    /// runs `verify_uncompressed_range` before calling this, so a hit returns
    /// bytes verified when they were first inserted.
    ///
    /// [`DecompressedChunkCache`]: crate::storage::cache::DecompressedChunkCache
    async fn get_cached_data(&self, block_offset: u64, size: u32) -> Result<Vec<u8>> {
        use crate::parser::header::CassandraVersion;
        use crate::storage::sstable::compression::Compression;

        let key = self.chunk_cache_key_ranged(NS_BIG_POINT, block_offset, size);
        if let Some(hit) = self.chunk_cache.get(&key) {
            self.record_cache_hit();
            return Ok(hit.to_vec());
        }
        self.record_cache_miss();

        // Read from disk (counted so a repeat read can prove zero underlying reads).
        // Positioned read on the shared point source (issue #1573, C2): no cursor
        // mutex is held across this I/O, so concurrent point reads do not convoy.
        model::CHUNK_READ_CALLS.fetch_add(1, Ordering::Relaxed);
        let mut buffer = vec![0u8; size as usize];
        self.point_source.read_exact_at(block_offset, &mut buffer)?;

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression = Compression::new(*compression_reader.algorithm())?;
            match compression.decompress(&buffer) {
                Ok(decompressed) => {
                    model::DECOMPRESS_CALLS.fetch_add(1, Ordering::Relaxed);
                    decompressed
                }
                Err(e) => {
                    // Handle decompression errors based on format
                    if self.header.cassandra_version != CassandraVersion::Legacy {
                        return Err(Error::corruption(format!(
                            "Decompression failed at offset={}, size={}: {}",
                            block_offset, size, e
                        )));
                    } else {
                        buffer // Fall back to raw data for legacy formats
                    }
                }
            }
        } else {
            buffer
        };

        // Insert into the shared cache (converts the Vec to Arc<[u8]> once) and
        // return an owned copy (this site's callers consume a Vec).
        let arc = self.chunk_cache.insert(key, data);
        Ok(arc.to_vec())
    }

    /// Verify the `CRC.db` chunk(s) covering the uncompressed Data.db byte range
    /// `[offset, offset + size)` against their stored per-chunk CRC32 (issue
    /// #1396), on the offset-read path used by the index-based scan and point
    /// lookups.
    ///
    /// A partition read touches a sub-range of one or more `chunk_size` blocks; a
    /// chunk's CRC can only be checked over the WHOLE chunk, so this reads each
    /// covering chunk (bounded to one `chunk_size` block at a time) and compares.
    /// Each chunk is verified at most once per reader lifetime (memoized in
    /// [`SSTableReader::verified_uncompressed_chunks`]), keeping the cost at the
    /// budgeted one CRC32 pass per chunk even when many partitions share a chunk.
    ///
    /// No-op when this reader has no `CRC.db` (compressed tables carry inline
    /// per-chunk CRCs; BTI ships none; an absent `CRC.db` is warn-and-proceed).
    async fn verify_uncompressed_range(&self, offset: u64, size: u32) -> Result<()> {
        let Some(crc) = self.crc_reader.as_deref() else {
            return Ok(());
        };
        if size == 0 {
            return Ok(());
        }
        let cs = crc.chunk_size() as u64;
        if cs == 0 {
            return Err(Error::corruption(
                "CRC.db chunk size is zero; cannot verify uncompressed read",
            ));
        }
        let file_size = self.stats.file_size;
        // Fix 3 (issue #1396): a corrupt on-disk offset/size can overflow
        // `offset + size` (debug panic / wrapped range that misattributes CRC
        // chunks) or point past EOF. Use checked arithmetic and reject a range
        // that overflows or exceeds the Data.db length as typed corruption
        // BEFORE deriving any chunk index. `size >= 1` here (0 handled above),
        // so `end > offset` and `end - 1` never underflows.
        let end = offset
            .checked_add(size as u64)
            .filter(|end| *end <= file_size)
            .ok_or_else(|| {
                Error::corruption(format!(
                    "uncompressed read range [0x{offset:x}, +{size}) overflows or exceeds the \
                     Data.db length {file_size}; refusing to verify a corrupt offset"
                ))
            })?;
        let first = offset / cs;
        let last = (end - 1) / cs;
        for chunk in first..=last {
            // Skip chunks already verified for this reader.
            {
                let seen = self
                    .verified_uncompressed_chunks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if seen.contains(&chunk) {
                    continue;
                }
            }
            let lo = chunk * cs;
            let hi = ((chunk + 1) * cs).min(file_size);
            if hi <= lo {
                break; // range extends past EOF; nothing real to verify
            }
            let mut buf = vec![0u8; (hi - lo) as usize];
            // Positioned read on the shared point source (issue #1573, C2): the CRC
            // verifier never holds a cursor mutex across I/O, so it neither convoys
            // concurrent point reads nor disturbs any scan cursor's position.
            self.point_source.read_exact_at(lo, &mut buf).map_err(|e| {
                Error::corruption(format!(
                    "failed to read uncompressed chunk {chunk} at Data.db offset 0x{lo:x} for CRC verification: {e}"
                ))
            })?;
            let computed = crc32fast::hash(&buf);
            let expected = crc.crc_for_chunk(chunk as usize)?;
            if computed != expected {
                return Err(Error::corruption(format!(
                    "uncompressed CRC32 mismatch for chunk {} at Data.db offset 0x{:x} \
                     ({} bytes): expected=0x{:08x}, computed=0x{:08x} (CRC.db)",
                    chunk,
                    lo,
                    hi - lo,
                    expected,
                    computed
                )));
            }
            self.verified_uncompressed_chunks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(chunk);
        }
        Ok(())
    }

    /// The SINGLE CRC-checked chokepoint for reading a raw byte range from an
    /// *uncompressed* Data.db section (issue #1396).
    ///
    /// EVERY uncompressed offset read MUST flow through here so that no public
    /// read path can silently bypass `CRC.db` verification and hand corrupt
    /// bytes to the parser. It runs the shared, memoized verifier
    /// [`Self::verify_uncompressed_range`] over the covering chunk(s) BEFORE any
    /// bytes are returned; a mismatch is the typed [`Error::Corruption`] naming
    /// the failing chunk + Data.db offset. The verifier is a no-op when this
    /// reader has no `CRC.db` (compressed tables carry inline per-chunk CRCs,
    /// BTI ships none, an absent `CRC.db` is warn-and-proceed), so this helper is
    /// also the correct accessor for the raw-read step of compressed offset reads.
    ///
    /// `offset` is an ABSOLUTE Data.db file offset (post-header). `file` is the
    /// handle to read the range from — the shared point-read handle
    /// ([`Self::file`]) or a scan-local cursor's private handle (issue #815).
    /// CRC verification always uses the reader's own handle, so it is independent
    /// of `file` and never disturbs a scan cursor's position.
    ///
    /// Future uncompressed offset reads MUST call this instead of doing their own
    /// `seek` + `read_exact`, so the CRC check can never be forgotten again.
    pub(in crate::storage::sstable::reader) async fn read_uncompressed_verified(
        &self,
        file: &tokio::sync::Mutex<super::source::BlockSource>,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        use tokio::io::AsyncReadExt;

        let size = u32::try_from(len).map_err(|_| {
            Error::corruption(format!(
                "uncompressed read length {len} exceeds u32 range for CRC verification \
                 at Data.db offset 0x{offset:x}"
            ))
        })?;
        // Verify the covering CRC.db chunk(s) BEFORE returning any bytes.
        self.verify_uncompressed_range(offset, size).await?;

        let mut buf = vec![0u8; len];
        {
            let mut guard = file.lock().await;
            guard.seek(SeekFrom::Start(offset)).await?;
            guard.read_exact(&mut buf).await?;
        }
        Ok(buf)
    }

    /// Mint a fresh, independent cursor for one scan (issue #815).
    ///
    /// Each cursor owns a private file handle (or mmap cursor) and chunk index,
    /// so concurrent scans on this reader never share a mutable file position —
    /// they run in parallel without the per-scan serialization #805 required.
    pub(in crate::storage::sstable::reader) async fn new_scan_cursor(&self) -> Result<ScanCursor> {
        Ok(ScanCursor::new(
            self.scan_source.open(&self.file_path).await?,
        ))
    }

    /// Read the next block from a scan-local `cursor` (its own file position and
    /// chunk index). See [`Self::new_scan_cursor`].
    pub(in crate::storage::sstable::reader) async fn read_next_block(
        &self,
        cursor: &ScanCursor,
    ) -> Result<Option<Vec<u8>>> {
        use super::block_io;
        block_io::read_next_block(
            &cursor.file,
            &self.header.cassandra_version,
            &self.config,
            &self.compression_info,
            self.crc_reader.as_deref(),
            &cursor.chunk_index,
            self.actual_header_size as u64,
        )
        .await
    }

    /// Prepare for a delta-scan pass: stitch all compressed chunks of the data
    /// section and return the decompressed buffer together with a pre-configured
    /// parser.
    ///
    /// Uses its own per-scan cursor (issue #815), so it no longer needs the
    /// caller to serialize against concurrent reads. This method is gated on the
    /// `delta-scan` feature and is the only bridge between the SSTableReader
    /// internals and the `delta_scan` module, which cannot access private
    /// helpers directly.
    ///
    /// The `schema` parameter is not used here — it is threaded through the
    /// caller's `parse_block_emit_delta` invocation instead.  The parser is
    /// built via `build_v5_parser()` which handles version-gates and UDT
    /// registry without needing the schema at construction time.
    #[cfg(feature = "delta-scan")]
    pub async fn prepare_delta_scan(
        &self,
    ) -> Result<(Vec<u8>, super::parsing::V5CompressedLegacyParser)> {
        use tokio::io::AsyncSeekExt;

        // Seek the per-scan cursor to the start of the data section.
        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Stitch all compressed chunks (bounded by uncompressed data-section size).
        let stitched = self.stitch_all_chunks(&cursor).await?;

        // Build a parser (re-using the existing builder so version-gates and
        // UDT registry are threaded through correctly).
        let parser = self.build_v5_parser(false);

        Ok((stitched, parser))
    }
}
