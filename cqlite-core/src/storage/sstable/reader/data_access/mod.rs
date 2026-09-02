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
// BTI point-read path split out of `bti.rs` (issue #1599 / G3, campsite #1116):
// `bti_point_lookup` + the chunk-targeted single-partition decode machinery.
mod bti_point;
// BIG ("nb") promoted-index forward seek + reverse iterator (Issue #1184). The
// seek/reverse paths exist only on the default build, so the whole module is
// `not(tombstones)` gated.
#[cfg(not(feature = "tombstones"))]
mod big_promoted;
// In-crate regression proofs for the BIG promoted seek read path (issues #1396, #1869):
// the uncompressed arm verifies CRC.db before parsing; the compressed window builder
// fails closed / round-trips. Uses crate-visible internals, so not in `tests/`.
#[cfg(all(test, not(feature = "tombstones")))]
mod big_promoted_seek_tests;
// In-crate proof that the BIG point-read chunk fetch (`get_cached_data`) consults
// the shared decompressed-chunk cache (issue #1567). Needs `pub(crate)` reader
// state (`actual_header_size`) to build a valid offset, so it cannot live in
// `tests/`.
#[cfg(test)]
mod chunk_cache_wiring_tests;
// In-crate proof that the compaction streaming scan polls the reader's
// cooperative cancel token and abandons a multi-partition Data.db mid-scan
// (issue #2264 — the World-2 un-cancellable full-materialise loop). Needs the
// `pub(crate)` `set_scan_cancel` + `stream_all_partitions_for_compaction`, so it
// cannot live in `tests/`.
#[cfg(all(test, feature = "write-support"))]
mod compaction_cancel_tests;
// Issue #2299 (roborev should-fix): range-marker resume parity. Drives the
// row-granular `stream_partition_body_incremental` over a partition whose range
// tombstone START/END bounds land in SEPARATE window refill chunks, and asserts
// the emitted `CompactionRow`s are byte-identical to the buffered
// `parse_block_for_compaction` output on the SAME bytes — proving the
// cross-chunk `CompactionPartitionState::pending_range_start` carry. Needs
// `write-support` to synthesize the range-tombstone SSTable bytes.
#[cfg(all(test, feature = "write-support"))]
mod compaction_range_marker_resume_tests;
// BIG ("nb"/uncompressed) point lookup: raw-key Index.db resolve + covering-chunk
// seek (issue #1572), replacing the whole-file scan_for_key fallback.
mod big_point;
mod compaction;
// The partition point-read entry points (`get` / `get_with_resolution`), split out
// of this file per the campsite rule (epic #1116). Also the read-metric emission
// site for a point read (issue #1701).
mod point_read;
// CRC-validated compressed offset-read window (issue #1773).
mod compressed_offset;
// Full-Index.db partition enumeration (issue #2302).
mod full_index_scan;
// True-streaming full-Index.db enumeration (issue #2361).
pub(in crate::storage::sstable::reader) mod full_index_stream;
// Summary-guided streaming enumeration + token pushdown (issue #2412 §C / #2413).
mod summary_scan;
// Streaming + LIMIT + cancel coverage (issue #2361). The fixtures use
// `SSTableWriter` + `write_engine::mutation` (write-support-only APIs) to build
// a real Index.db-backed SSTable, so — like the sibling `compaction_cancel_tests`
// above — this module needs BOTH gates: `data_access` itself compiles on every
// build (it's the read path), but the test module does not (minimal-build gate
// finding).
#[cfg(all(test, feature = "write-support"))]
mod full_index_stream_tests;
mod model;
// Single-partition compaction seek (issue #2207): the public point-read primitive
// composing the presence oracle + BTI/BIG offset resolution into a byte-identical
// compaction-row seek for one partition. Gated `not(tombstones)` like the seek
// primitives it composes (`successor_partition_offset`, `point_read_whole_section`).
#[cfg(not(feature = "tombstones"))]
mod point_compaction;
// Fail-safe proof (issue #2207, roborev IMPORTANT-1): a corrupt/unreadable BTI
// Partitions.db must degrade the point-read primitive to a scan-fallback signal,
// never a hard `Err`. Needs `write-support` to synthesize a BTI fixture.
#[cfg(all(test, not(feature = "tombstones"), feature = "write-support"))]
mod point_compaction_fail_safe_tests;
// Opt-in presence-oracle false-negative verification method (issue #2163), kept
// out of this already-large entry-point file (campsite rule, epic #1116).
pub(in crate::storage::sstable) mod joined_scan_stream;
mod presence_verify;
// First/last-key range short-circuit (issue #1576, C5): an authoritative
// `[first_key, last_key]` bound check that answers out-of-range point reads as
// absence before any bloom/Index.db/trie work.
mod range_short_circuit;
mod sequential;

// Public surface re-export (unchanged: `reader::mod` re-exports
// `data_access::ClusteringSlice`).
pub use model::ClusteringSlice;
#[cfg(not(feature = "tombstones"))]
pub use point_compaction::SinglePartitionCompaction;
// Token-range bound pushed into the Summary-guided streaming walk (issue #2413
// Option A). Re-exported to the crate so the flight warm merge can construct one
// from its `TokenFilter`.
pub use summary_scan::{QueryRowBatch, QueryRowStream, ScanTokenBound, QUERY_ROWS_MAX_READ_AHEAD};

// Re-export the decompress-work counter so the sibling `scan_stream_windowed`
// module (outside `data_access`) can increment it on the windowed-scan miss path
// (issue #1567). `model` is a private submodule, so the raw path is not reachable
// from `reader::scan_stream_windowed`; this widens the path exactly enough.
pub(in crate::storage::sstable::reader) use model::DECOMPRESS_CALLS;

use super::source::ScanCursor;
use super::SSTableReader;
use crate::parser::DataFormat;
use crate::types::{CellWriteMetadata, ScanRow, TableId};
use crate::{Error, Result, RowKey};
use std::io::SeekFrom;
use std::sync::atomic::Ordering;
use tokio::io::AsyncSeekExt;
use tracing::warn;

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
        scan_cancel: &crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Vec<(TableId, RowKey, ScanRow)>> {
        let stitched_buffer = self
            .stitch_all_chunks_cancellable(cursor, scan_cancel)
            .await?;
        // Issue #3782: the buffer is EVERY chunk of the data section, so no further
        // bytes exist — a row that fails to decode is corruption/truncation and must
        // be reported, not silently truncate the scan.
        let parser = self
            .build_v5_parser(read_shadowing)
            .with_complete_buffer(true);

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
        tracing::debug!(
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
        // Issue #3782: whole stitched data section — see `stitch_and_parse_all_chunks`.
        let parser = self.build_v5_parser(true).with_complete_buffer(true);

        let reader_schema;
        let table_schema = if let Some(s) = schema {
            Some(s)
        } else {
            reader_schema = self.get_table_schema(None);
            reader_schema.as_ref()
        };

        let entries =
            parser.parse_block_with_cell_metadata(&stitched_buffer, table_schema, self)?;
        tracing::debug!(
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
    ///
    /// Existing callers use the reader's own [`SSTableReader::scan_cancel`]
    /// field (unchanged pre-#2346 behaviour); this is a thin wrapper over
    /// [`Self::stitch_all_chunks_cancellable`], which the per-call seam
    /// (`sequential_scan`) drives with its caller-supplied token instead.
    pub(super) async fn stitch_all_chunks(&self, cursor: &ScanCursor) -> Result<Vec<u8>> {
        self.stitch_all_chunks_cancellable(cursor, &self.scan_cancel)
            .await
    }

    /// [`Self::stitch_all_chunks`] with an explicit PER-CALL cancel token
    /// (issue #2346). `scan_cancel` is polled every 256 chunks so a cancelled
    /// caller abandons the stitch — the chunk-read/decompress loop is the
    /// I/O-bound phase of the stitched scan path, matching the compaction
    /// stream's per-256-chunk cadence (`stream_all_partitions_for_compaction`).
    pub(super) async fn stitch_all_chunks_cancellable(
        &self,
        cursor: &ScanCursor,
        scan_cancel: &crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Vec<u8>> {
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
            // Cooperative checkpoint (issue #2346): poll the per-call token so a
            // cancelled stitched scan abandons the I/O/decompress walk promptly
            // instead of stitching the entire data section first, yielding every
            // 256 chunks so the chokepoint timeout can elapse here too (#1695).
            scan_cancel.checkpoint(chunk_count).await?;
            let decompressed_chunk = if compressed_chunk.len() >= max_compressed_length {
                // Stored uncompressed by Cassandra — the raw bytes pass through, and
                // are COUNTED at the plane's one raw-chunk boundary (issue #1701 F3).
                super::chunk_source::count_raw_chunk(
                    &compressed_chunk,
                    self.compression_reader.as_ref().map(|r| r.algorithm()),
                );
                compressed_chunk
            } else if let Some(compression_reader) = &self.compression_reader {
                // Single decode plane (issue #1598, G2): route the stitch-path
                // decompress through ChunkSource so it is the ONLY query-path module
                // that calls Compression::decompress. Behavior-identical (no cache,
                // no counter) to the prior inline call.
                let compression = Compression::new(*compression_reader.algorithm())?;
                super::chunk_source::ChunkSource::decompress_only(
                    Some(&compression),
                    compressed_chunk,
                )
                .map_err(|e| {
                    Error::corruption(format!(
                        "stitch_all_chunks: Failed to decompress chunk {}: {}",
                        chunk_count, e
                    ))
                })?
            } else {
                // No compression (should not happen for V5CompressedLegacy)
                tracing::warn!("stitch_all_chunks: No compression reader, using raw chunk data");
                compressed_chunk
            };

            stitched_buffer.extend_from_slice(&decompressed_chunk);
            chunk_count += 1;
        }

        tracing::debug!(
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

    /// Read value at a specific offset with caching — the POINT-intent entry.
    ///
    /// Reads Data.db through the reader's dedicated `MADV_RANDOM` point mapping
    /// ([`point_source`](Self::point_source), issue #2210), which is the right
    /// advice for a scattered fault.
    ///
    /// Note (issue #2876, roborev job 4634): the index-driven range scan in
    /// `sequential.rs` also uses THIS point-intent entry, and that is deliberate.
    /// It looks like a scan, but `Index::get_range` yields entries in raw key-BYTE
    /// order while Data.db is laid out in Murmur3 TOKEN order — uncorrelated for the
    /// default partitioner — so its access really is scattered and `MADV_RANDOM` is
    /// correct. The genuinely sequential walks (the Summary-guided walk, the full
    /// index scan/stream, the windowed scan) reach the unadvised scan plane through
    /// the positional helpers below, which take their plane from the caller.
    pub async fn read_value_at_offset(&self, offset: u64, size: u32) -> Result<Option<ScanRow>> {
        self.read_value_at_offset_via(self.point_source.as_ref(), offset, size)
            .await
    }

    /// Shared body of the two offset-read entry points, parameterized by the
    /// positional plane its caller's read intent selects (issue #2876).
    ///
    /// `source` is threaded all the way down — through the `CRC.db` verifier AND
    /// the byte read / compressed-window decode — so a read never mixes planes:
    /// no helper below hardcodes one, which is what made an index-driven scan's CRC
    /// reads land on the advised point mapping while its bodies came off the scan
    /// mapping (roborev #2882, Finding 1) and, symmetrically, made a genuine point
    /// lookup lose the advised mapping entirely (Finding 2).
    async fn read_value_at_offset_via(
        &self,
        source: &dyn super::read_at::ReadAt,
        offset: u64,
        size: u32,
    ) -> Result<Option<ScanRow>> {
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
        // (compressed tables / BTI / absent-CRC.db warn-and-proceed). The CRC read
        // uses the CALLER's plane (issue #2876): a scan's covering-chunk reads are
        // part of its sequential walk, not scattered point faults.
        self.verify_uncompressed_range(source, offset, size).await?;

        // Read + decompress in ONE decode plane (issue #1598, G2). `get_cached_data`
        // returns the final DECODED window: for a compressed Data.db it routes through
        // the CRC-validated `read_compressed_offset_window` (issue #1773 — per-chunk
        // inline CRC32 checked BEFORE decompression, whose sole `Compression::decompress`
        // now resolves inside `ChunkSource`), or the CRC.db-verified raw bytes for an
        // uncompressed one. There is no second decode here — doing so used to
        // double-decompress the compressed path (and skip its inline CRC, the #1411 bug).
        let data = self.get_cached_data(source, offset, size).await?;

        // Preserve raw data until schema is available. Pre-#1334 this offset-read
        // placeholder returned a bare `Value::Blob` of the row's raw value bytes,
        // which the schema-aware decode path then schema-decoded and the no-schema query
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
    /// `source` is the positional plane the CALLER's read intent selected (issue
    /// #2876) — the advised point mapping for a point lookup, the unadvised scan
    /// mapping for the index-driven scan. It is threaded in rather than read off
    /// `self` precisely because this helper serves BOTH intents.
    ///
    /// [`DecompressedChunkCache`]: crate::storage::cache::DecompressedChunkCache
    async fn get_cached_data(
        &self,
        source: &dyn super::read_at::ReadAt,
        block_offset: u64,
        size: u32,
    ) -> Result<Vec<u8>> {
        // The shared B1 cache tracks its own hit/miss counters (issue #1567). The
        // ranged BIG-point key carries `size` as its aux discriminant so two reads at
        // the same offset with different sizes can never alias (roborev #1567).
        let key = crate::storage::cache::ChunkKey::with_aux(
            self.chunk_cache_id ^ NS_BIG_POINT,
            block_offset,
            size as u64,
        );
        if let Some(hit) = self.chunk_cache.get(&key) {
            return Ok(hit.to_vec());
        }

        // Produce the final DECOMPRESSED, integrity-verified window for this offset.
        let data = if let Some(comp_info) = self.compression_info.as_deref() {
            // COMPRESSED offset read (issue #1773): validate the inline per-chunk CRC32
            // before decompression via the shared CRC-enforcing chunk reader (multi-chunk
            // assembly, fail-closed past-EOF), NOT a raw read + blind LZ4 decode (#1411);
            // the single decompress resolves inside `ChunkSource` (#1598). Count one
            // backing read HERE (point-read site only), symmetric with the uncompressed
            // branch below (#2167) — scan callers of the shared helper must NOT bump it.
            model::CHUNK_READ_CALLS.fetch_add(1, Ordering::Relaxed);
            self.read_compressed_offset_window(source, comp_info, block_offset, size)
                .await?
        } else {
            // UNCOMPRESSED offset read. Positioned read on the caller's positional
            // plane (issue #1573/C2 for the lock-free part, issue #2876 for the
            // plane): no cursor mutex is held across this I/O, so concurrent reads
            // do not convoy. `get_cached_data` is reached both by genuine point
            // lookups (`big_point.rs`) and by every index-driven scan
            // (`sequential.rs`), so the plane comes from the caller's intent — a
            // point read keeps the dedicated `MADV_RANDOM` mapping (issue #2210), a
            // scan takes the unadvised one, for which that readahead suppression is
            // a deliberate loss. The covering CRC.db chunks were already verified on
            // the SAME plane by `verify_uncompressed_range` (issue #1396), so these
            // bytes are integrity-checked too.
            model::CHUNK_READ_CALLS.fetch_add(1, Ordering::Relaxed);
            let mut buffer = vec![0u8; size as usize];
            source.read_exact_at(block_offset, &mut buffer)?;
            // cqlite.read.bytes (issue #1701 roborev F3): an UNCOMPRESSED offset read
            // never reaches the decode plane — there is nothing to decompress — so its
            // bytes are counted at the plane's raw-chunk boundary here. Without this a
            // point read (or an index-driven partition read) of an uncompressed
            // SSTable reported rows and a duration but ZERO bytes. The cache-hit
            // early return above stays uncounted: it read no Data.db.
            super::chunk_source::count_raw_chunk(&buffer, None);
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
    ///
    /// `source` is the positional plane the CALLER's read intent selected (issue
    /// #2876). A chunk CRC read is I/O on the same Data.db bytes the caller is
    /// about to consume, so it belongs on the caller's plane: a scan's
    /// covering-chunk reads are part of its sequential walk (the advised
    /// `MADV_RANDOM` point mapping would suppress readahead over a whole 64 KiB
    /// chunk), while a point lookup's stay on the advised mapping (issue #2210).
    /// This is why the plane is a parameter and never read off `self` here —
    /// hardcoding it split one logical read across two planes (roborev #2882).
    async fn verify_uncompressed_range(
        &self,
        source: &dyn super::read_at::ReadAt,
        offset: u64,
        size: u32,
    ) -> Result<()> {
        let Some(crc) = self.crc_reader.as_deref() else {
            return Ok(());
        };
        if size == 0 {
            return Ok(());
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
        // Each covering chunk is CRC'd over its on-disk bytes via a positioned
        // read on the caller's plane (issue #1573 C2 for the lock-free part, issue
        // #2876 for the plane): the verifier never holds a cursor mutex across I/O,
        // so it neither convoys concurrent point reads nor disturbs any scan
        // cursor's position.
        self.verify_covering_chunks(crc, offset, end, |lo, hi, chunk| {
            let mut buf = vec![0u8; (hi - lo) as usize];
            source.read_exact_at(lo, &mut buf).map_err(|e| {
                Error::corruption(format!(
                    "failed to read uncompressed chunk {chunk} at Data.db offset 0x{lo:x} for CRC verification: {e}"
                ))
            })?;
            Ok(crc32fast::hash(&buf))
        })
    }

    /// In-buffer counterpart of [`Self::verify_uncompressed_range`] for the
    /// whole-section point-read fallback (issue #1573): the section is ALREADY
    /// resident in `section`, covering Data.db `[base, base + section.len())`, so
    /// each fully-contained covering chunk is CRC-checked directly from memory —
    /// the section is transferred from disk EXACTLY ONCE (the caller's windowed
    /// read), not a second time for CRC. A chunk that straddles below `base` (only
    /// possible when `base` is not `CRC.db`-chunk-aligned, e.g. a nonzero header
    /// that does not fall on a chunk boundary) re-reads just that one chunk so the
    /// CRC-before-use guarantee holds regardless of header alignment. Same CRC32
    /// algorithm, chunk layout, memoization, and mismatch/typed-error semantics as
    /// [`Self::verify_uncompressed_range`] — including its `source` contract: the
    /// straddling-chunk re-read uses the CALLER's positional plane (issue #2876),
    /// never a hardcoded one.
    async fn verify_uncompressed_section_in_buffer(
        &self,
        source: &dyn super::read_at::ReadAt,
        base: u64,
        section: &[u8],
    ) -> Result<()> {
        let Some(crc) = self.crc_reader.as_deref() else {
            return Ok(());
        };
        if section.is_empty() {
            return Ok(());
        }
        let file_size = self.stats.file_size;
        let end = base
            .checked_add(section.len() as u64)
            .filter(|end| *end <= file_size)
            .ok_or_else(|| {
                Error::corruption(format!(
                    "uncompressed section [0x{base:x}, +{}) overflows or exceeds the Data.db \
                     length {file_size}; refusing to verify a corrupt section",
                    section.len()
                ))
            })?;
        self.verify_covering_chunks(crc, base, end, |lo, hi, chunk| {
            if lo >= base && hi <= end {
                // Chunk fully resident in the already-read buffer — CRC from
                // memory, no second I/O.
                let s = (lo - base) as usize;
                let e = (hi - base) as usize;
                Ok(crc32fast::hash(&section[s..e]))
            } else {
                // Chunk extends below `base` (unaligned header) — read just it.
                let mut buf = vec![0u8; (hi - lo) as usize];
                source.read_exact_at(lo, &mut buf).map_err(|e| {
                    Error::corruption(format!(
                        "failed to read uncompressed chunk {chunk} at Data.db offset 0x{lo:x} for CRC verification: {e}"
                    ))
                })?;
                Ok(crc32fast::hash(&buf))
            }
        })
    }

    /// Walk the `CRC.db` chunk(s) covering Data.db `[start, end)` (`end` already
    /// bounds-checked `<= file_size` by the caller, `end > start`), skipping chunks
    /// already verified for this reader, comparing each covering chunk's CRC32 to
    /// `CRC.db`, and memoizing successes so a chunk is verified at most once per
    /// reader lifetime. `compute(lo, hi, chunk)` yields the CRC32 of the chunk's
    /// on-disk bytes `[lo, hi)`; callers supply either a positioned disk read or an
    /// in-buffer slice, which is what lets the whole-section fallback read its bytes
    /// exactly once. This is the single place the chunk geometry, mismatch error,
    /// and memoization live — the two public verify entry points differ ONLY in how
    /// they source each chunk's bytes. Module-visible so the windowed scan's
    /// synchronous uncompressed piece reader shares the same memoized path (#1940).
    pub(in crate::storage::sstable::reader) fn verify_covering_chunks(
        &self,
        crc: &super::crc::CrcDb,
        start: u64,
        end: u64,
        mut compute: impl FnMut(u64, u64, u64) -> Result<u32>,
    ) -> Result<()> {
        let cs = crc.chunk_size() as u64;
        if cs == 0 {
            return Err(Error::corruption(
                "CRC.db chunk size is zero; cannot verify uncompressed read",
            ));
        }
        let file_size = self.stats.file_size;
        let first = start / cs;
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
            let computed = compute(lo, hi, chunk)?;
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
    /// CRC verification reads positionally via `source` instead, so it is
    /// independent of `file` and never disturbs a scan cursor's position.
    ///
    /// `source` is the positional plane the CALLER's read intent selects (issue
    /// #2876), threaded through for the CRC chunk reads exactly as in
    /// [`Self::verify_uncompressed_range`]. Every caller today is a scan walk
    /// (`summary_scan.rs`, `full_index_scan.rs`, `full_index_stream.rs`) and passes
    /// the unadvised scan mapping; a future point caller passes the advised one and
    /// gets the right advice without editing this helper.
    ///
    /// SCOPE NOTE (#2876): `source` governs the **CRC chunk reads only**. The payload
    /// bytes below are read through `file` — a seek-based [`BlockSource`], which is NOT
    /// an mmap and therefore carries no `madvise` advice on any backend. So the
    /// uncompressed path never suffered the MADV_RANDOM readahead suppression this
    /// issue fixes (that regression was mmap-specific, hence compressed-walk-specific),
    /// and threading the plane here closes the CRC-read half completely: after this
    /// change NO read on an uncompressed scan touches an advised mapping. The
    /// separate cost on this path is the shared `file` mutex serializing concurrent
    /// scans (#815's domain), which is deliberately out of scope here.
    ///
    /// Future uncompressed offset reads MUST call this instead of doing their own
    /// `seek` + `read_exact`, so the CRC check can never be forgotten again.
    pub(in crate::storage::sstable::reader) async fn read_uncompressed_verified(
        &self,
        source: &dyn super::read_at::ReadAt,
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

        // Issue #2819 (BLOCKER): attribute the UNCOMPRESSED body page-in to
        // `stream_cold_fault`, but time ONLY the actual IO — NOT the
        // `file.lock().await` below on the reader-wide mutex. The warm registry
        // (#2356) shares one `Arc<SSTableReader>` across concurrent `do_get`s that
        // admission (#2420) runs at once, so a PEER scan's lock-wait must never be
        // attributed to cold-IO (it would inflate `stream_cold_fault` on a WARM run
        // and break the cold−warm delta = cold-IO meaning). Mirrors the compressed
        // path's "time only the positional read". `verify_uncompressed_range` reads
        // CRC.db via the positional `source` (NO shared mutex) — genuine cold IO —
        // so it is timed; the body read is timed under the already-held lock. No
        // `stream_decompress` (correctly absent).
        //
        // Issue #2819 (L1): capture the sink ONCE here, BEFORE any `.await`, and
        // build both timers from that captured `Option` — so no thread-local read
        // happens post-await (correct even if the future resumes on another
        // executor thread). `None` (zero cost) with no flight sink installed.
        let cold_sink = crate::observability::stream_subphase::current();
        {
            let _cold = crate::observability::stream_subphase::scoped_captured(
                &cold_sink,
                crate::observability::StreamSubPhase::ColdFault,
            );
            // Verify the covering CRC.db chunk(s) BEFORE returning any bytes.
            self.verify_uncompressed_range(source, offset, size).await?;
        }

        let mut buf = vec![0u8; len];
        {
            // Lock-wait is acquired OUTSIDE the cold-fault timer (peer-scan mutex
            // contention is not cold-IO — the BLOCKER).
            let mut guard = file.lock().await;
            let _cold = crate::observability::stream_subphase::scoped_captured(
                &cold_sink,
                crate::observability::StreamSubPhase::ColdFault,
            );
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
        // Load the (interior-mutable, #2383-rebindable) path once per scan; a
        // rebind swaps this to a live same-inode hardlink without re-parsing.
        let file_path = self.file_path();
        Ok(ScanCursor::new(self.scan_source.open(&file_path).await?))
    }

    /// Read the next block from a scan-local `cursor` (its own file position and
    /// chunk index). See [`Self::new_scan_cursor`].
    pub(in crate::storage::sstable::reader) async fn read_next_block(
        &self,
        cursor: &ScanCursor,
    ) -> Result<Option<Vec<u8>>> {
        // Non-recycling callers (compaction, sequential fallback, tests) pass a
        // throwaway scratch — one alloc/chunk, unchanged; the windowed IO half calls
        // `read_next_block_parts` with a REUSED per-loop scratch (issue #1940, D2).
        self.read_next_block_parts(&cursor.file, &cursor.chunk_index, &mut Vec::new())
            .await
    }

    /// Read the next block from an explicit `(file, chunk_index)` pair rather than
    /// a whole [`ScanCursor`] (issue #1593, F3).
    ///
    /// The windowed scan's `spawn_blocking` I/O loop for synchronously-faulting
    /// backends (mmap / `O_DIRECT`) owns `Arc` clones of these two fields, not the
    /// borrowed cursor, so it calls this directly. All other read state
    /// (compression info, CRC digest, version, header size) lives on `&self`.
    pub(in crate::storage::sstable::reader) async fn read_next_block_parts(
        &self,
        file: &std::sync::Arc<tokio::sync::Mutex<super::source::BlockSource>>,
        chunk_index: &std::sync::atomic::AtomicUsize,
        scratch: &mut Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        use super::block_io;
        block_io::read_next_block(
            file,
            &self.header.cassandra_version,
            &self.config,
            &self.compression_info,
            self.crc_reader.as_deref(),
            chunk_index,
            self.actual_header_size as u64,
            scratch,
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
