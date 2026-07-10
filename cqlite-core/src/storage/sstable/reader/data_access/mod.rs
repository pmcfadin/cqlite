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
// In-crate proof that the compaction streaming scan polls the reader's
// cooperative cancel token and abandons a multi-partition Data.db mid-scan
// (issue #2264 — the World-2 un-cancellable full-materialise loop). Needs the
// `pub(crate)` `set_scan_cancel` + `stream_all_partitions_for_compaction`, so it
// cannot live in `tests/`.
#[cfg(all(test, feature = "write-support"))]
mod compaction_cancel_tests;
// BIG ("nb"/uncompressed) point lookup: raw-key Index.db resolve + covering-chunk
// seek (issue #1572), replacing the whole-file scan_for_key fallback.
mod big_point;
mod compaction;
// CRC-validated compressed offset-read window (issue #1773): keeps the
// `read_compressed_offset_window` helper out of this already-large entry-point file.
mod compressed_offset;
mod model;
// Single-partition compaction seek (issue #2207): the public point-read primitive
// composing the presence oracle + BTI/BIG offset resolution into a byte-identical
// compaction-row seek for one partition. Kept out of this entry-point file.
mod point_compaction;
// Opt-in presence-oracle false-negative verification method (issue #2163), kept
// out of this already-large entry-point file (campsite rule, epic #1116).
mod presence_verify;
// First/last-key range short-circuit (issue #1576, C5): an authoritative
// `[first_key, last_key]` bound check that answers out-of-range point reads as
// absence before any bloom/Index.db/trie work.
mod range_short_circuit;
mod sequential;

// Public surface re-export (unchanged: `reader::mod` re-exports
// `data_access::ClusteringSlice`).
pub use model::ClusteringSlice;
pub use point_compaction::SinglePartitionCompaction;

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
        // Issue #1576 (C5): O(1) authoritative range short-circuit. If the query key
        // sorts outside this SSTable's [first_key, last_key] bound (Summary.db, in
        // Cassandra token order — no heuristics), the partition is definitely absent;
        // return absence BEFORE any bloom check, Index.db probe, or BTI trie descent.
        // Inclusive bound (== first/last stays in range), so it never drops a present
        // partition. A no-op when no authoritative bound exists (BTI/no Summary).
        if self.partition_key_out_of_range(key.as_bytes()) {
            crate::storage::sstable::read_work_counters::record_range_short_circuit();
            return Ok(None);
        }

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
        let (row, oracle_pruned) = if self.bti_partitions_db.is_some() {
            self.bti_point_lookup(table_id, key, fully_qualified_match)
                .await?
        } else {
            // BIG ("nb"/uncompressed) readers: raw-key Index.db resolve +
            // covering-chunk seek (issue #1572). The bloom pre-check, the fast
            // Index.db-resolved chunk-targeted decode, and the index-less
            // `scan_for_key` fallback all live in `big_point`.
            self.big_get_with_resolution(table_id, key, fully_qualified_match)
                .await?
        };

        // Issue #2163 (roborev r4): `oracle_pruned` is `true` ONLY when the
        // presence oracle itself (bloom-miss for BIG / trie-miss for BTI) excluded
        // this SSTable from the read BEFORE any decode or scan — the PRIMARY
        // single-reader point-read path, which the spec scenario "a partition
        // point lookup ... through the public read surface" names directly. This
        // is the SAME emit site `might_contain_partition[_encoded]` use (via
        // `emit_sstable_pruned`), so a candidate pre-pruned by
        // `SSTableManager::prune_candidates` (excluded from the candidate list, so
        // `get()` is never called on it for this read) is never double-counted:
        // exactly one of {prune-time check, this get-time check} runs per SSTable
        // per logical read.
        if oracle_pruned {
            self.emit_sstable_pruned();

            // Opt-in presence-oracle false-negative verification: when the
            // default-off switch is enabled, an AUTHORITATIVE confirmation scan
            // proves this exclusion truthful; a contradiction increments
            // `cqlite.read.bloom.false_negatives`. Off by default → this whole
            // block is skipped and the read costs nothing extra. Gated on
            // `oracle_pruned` (not merely `row.is_none()`) so a `None` reached via
            // the primary path's OWN authoritative `scan_for_key` — which already
            // IS the confirming scan — never triggers a REDUNDANT second scan.
            if super::presence_verification::enabled() {
                if let Err(e) = self
                    .verify_presence_oracle_negative(table_id, key.as_bytes())
                    .await
                {
                    // Issue #2163 (roborev r5): the READ stays fail-open — a
                    // verification-scan failure (e.g. `scan_for_key` erroring on
                    // corruption or an unreadable SSTable) must NEVER fail (or
                    // even affect) the actual read this opt-in check is merely
                    // double-checking; `row` above is returned unchanged either
                    // way. But a SILENT-MISS DETECTOR that itself fails silently
                    // defeats its own purpose, so the failure is surfaced LOUDLY
                    // instead of discarded: an error-level log with context, AND
                    // a record through the EXISTING error-rate signal
                    // (`cqlite.errors.total{category,subsystem}`, issue #1038) —
                    // never a new metric. `record_error` maps `Error::Corruption`
                    // (the typical `scan_for_key` failure mode) to the bounded
                    // `Corruption` category.
                    tracing::error!(
                        error = %e,
                        sstable_format = self.sstable_format_label(),
                        "opt-in presence-oracle false-negative verification scan FAILED — the \
                         read itself is unaffected (fail-open), but this soundness check could \
                         not run for this SSTable and needs investigation"
                    );
                    crate::observability::record_error(&e, "reader");
                }
            }
        }
        Ok(row)
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
                tracing::debug!(
                    "stitch_all_chunks: chunk {} is incompressible (len={} >= max_compressed_length={}), using raw bytes",
                    chunk_count,
                    compressed_chunk.len(),
                    max_compressed_length
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

    /// Read value at a specific offset with caching
    pub async fn read_value_at_offset(&self, offset: u64, size: u32) -> Result<Option<ScanRow>> {
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

        // Read + decompress in ONE decode plane (issue #1598, G2). `get_cached_data`
        // returns the final DECODED window: for a compressed Data.db it routes through
        // the CRC-validated `read_compressed_offset_window` (issue #1773 — per-chunk
        // inline CRC32 checked BEFORE decompression, whose sole `Compression::decompress`
        // now resolves inside `ChunkSource`), or the CRC.db-verified raw bytes for an
        // uncompressed one. There is no second decode here — doing so used to
        // double-decompress the compressed path (and skip its inline CRC, the #1411 bug).
        let data = self.get_cached_data(offset, size).await?;

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
    /// [`DecompressedChunkCache`]: crate::storage::cache::DecompressedChunkCache
    async fn get_cached_data(&self, block_offset: u64, size: u32) -> Result<Vec<u8>> {
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
            // COMPRESSED offset read (issue #1773): the authoritative inline per-chunk
            // CRC32 MUST be validated before decompression. `read_compressed_offset_window`
            // reuses the shared CRC-enforcing chunk reader (multi-chunk assembly,
            // fail-closed past-EOF) rather than reading `size` raw bytes and blindly
            // LZ4-decoding them — the latter re-introduced the exact #1411 CRC bypass.
            // Its single decompress resolves inside `ChunkSource` (issue #1598, G2), so
            // this is NOT a second decode plane.
            self.read_compressed_offset_window(comp_info, block_offset, size)
                .await?
        } else {
            // UNCOMPRESSED offset read. Positioned read on the shared point source
            // (issue #1573, C2): no cursor mutex is held across this I/O, so
            // concurrent point reads do not convoy. The covering CRC.db chunks were
            // already verified by `verify_uncompressed_range` before we got here
            // (issue #1396), so these bytes are integrity-checked too.
            model::CHUNK_READ_CALLS.fetch_add(1, Ordering::Relaxed);
            let mut buffer = vec![0u8; size as usize];
            self.point_source.read_exact_at(block_offset, &mut buffer)?;
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
        // read on the shared point source (issue #1573, C2): the verifier never
        // holds a cursor mutex across I/O, so it neither convoys concurrent point
        // reads nor disturbs any scan cursor's position.
        self.verify_covering_chunks(crc, offset, end, |lo, hi, chunk| {
            let mut buf = vec![0u8; (hi - lo) as usize];
            self.point_source.read_exact_at(lo, &mut buf).map_err(|e| {
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
    /// [`Self::verify_uncompressed_range`].
    async fn verify_uncompressed_section_in_buffer(&self, base: u64, section: &[u8]) -> Result<()> {
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
                self.point_source.read_exact_at(lo, &mut buf).map_err(|e| {
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
    /// they source each chunk's bytes.
    fn verify_covering_chunks(
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
        self.read_next_block_parts(&cursor.file, &cursor.chunk_index)
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
