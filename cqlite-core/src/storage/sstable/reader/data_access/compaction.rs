//! Compaction read paths: timestamp-preserving partition iteration, the bounded
//! streaming compaction driver, and the partition-key enumeration the verifier's
//! BTI cross-check relies on.
//!
//! Unlike the user-facing scans in `sequential`/`bti`, these paths PRESERVE
//! `Value::Tombstone` entries (with their authoritative deletion timestamps) so
//! the k-way merger can apply tombstone-shadowing semantics (Issue #505).

use super::super::source::ScanCursor;
use super::super::window_cursor::WindowCursor;
use super::super::SSTableReader;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::parsing::BufferExtent;
use crate::{Error, Result};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

impl SSTableReader {
    /// Stitch all compressed chunks and parse with per-row timestamps (for compaction).
    ///
    /// Identical to [`stitch_and_parse_all_chunks`] but delegates to
    /// [`V5CompressedLegacyParser::parse_block_with_timestamps`] so that each
    /// entry carries its actual row-level write timestamp rather than
    /// `SystemTime::now()`.  Row and cell tombstones are emitted as
    /// `Value::Tombstone` with their authoritative deletion timestamps.
    ///
    /// Used exclusively by the compaction k-way merger path (Issue #505).
    ///
    /// [`stitch_and_parse_all_chunks`]: crate::storage::sstable::SSTableReader
    /// [`V5CompressedLegacyParser::parse_block_with_timestamps`]: crate::storage::sstable::reader::parsing::V5CompressedLegacyParser
    async fn stitch_and_parse_all_chunks_for_compaction(
        &self,
        cursor: &ScanCursor,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<super::super::compaction_row::CompactionRow>> {
        tracing::debug!("stitch_and_parse_all_chunks_for_compaction: stitching chunks");

        let mut stitched_buffer = Vec::with_capacity(2_500_000);
        let mut chunk_count = 0;

        // Incompressible-chunk fallback (Bug #639, epic #970, issue #1104):
        // Cassandra stores a chunk RAW (not compressed) when its compressed length
        // would meet or exceed `max_compressed_length`. `stitch_all_chunks` (the
        // read/scan path) honours this, but the compaction stitch path did not —
        // it blindly tried to LZ4/Snappy/etc-decode a raw chunk, which fails on the
        // `incompressible` fixture with e.g. "the offset to copy is not contained
        // in the decompressed buffer". Mirror the writer rule here: when the
        // (CRC-stripped) chunk length >= max_compressed_length, the bytes are
        // already plaintext. Authority: CompressedSequentialWriter.java:160-177.
        let max_compressed_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.max_compressed_length as usize)
            .unwrap_or(usize::MAX);

        while let Some(compressed_chunk) = self.read_next_block(cursor).await? {
            use crate::storage::sstable::compression::Compression;
            let algorithm = self.compression_reader.as_ref().map(|r| r.algorithm());
            let decompressed_chunk = if compressed_chunk.len() >= max_compressed_length {
                // Raw bytes through, COUNTED at the plane's boundary (#1701 R3).
                super::super::chunk_source::counted_raw_chunk(compressed_chunk, algorithm)
            } else if let Some(compression_reader) = &self.compression_reader {
                let compression = Compression::new(*compression_reader.algorithm())?;
                super::super::chunk_source::ChunkSource::decompress_only(
                    Some(&compression),
                    compressed_chunk,
                )
                .map_err(|e| {
                    Error::corruption(format!(
                        "stitch_and_parse_all_chunks_for_compaction: Failed to decompress chunk {}: {}",
                        chunk_count, e
                    ))
                })?
            } else {
                compressed_chunk // counted by read_next_block; see count_uncompressed_block
            };
            stitched_buffer.extend_from_slice(&decompressed_chunk);
            chunk_count += 1;
        }

        tracing::debug!(
            "stitch_and_parse_all_chunks_for_compaction: {} chunks, {} bytes total",
            chunk_count,
            stitched_buffer.len()
        );

        let keyspace = self.header.keyspace.clone();
        let table_name = self.header.table_name.clone();

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
        // VG1: thread VersionGates from SSTableReader down to row parser.
        .with_version_gates(self.version_gates.clone());
        let parser = if let Some(ref registry) = self.udt_registry {
            parser.with_udt_registry(registry.clone())
        } else {
            parser
        };

        let reader_schema;
        let table_schema = if let Some(s) = schema {
            Some(s)
        } else {
            reader_schema = self.get_table_schema(None);
            reader_schema.as_ref()
        };

        // #3782: `stitched_buffer` drained EVERY chunk from the cursor above.
        let entries = parser.parse_block_for_compaction(
            &stitched_buffer,
            BufferExtent::Complete,
            table_schema,
            self,
        )?;
        tracing::debug!(
            "stitch_and_parse_all_chunks_for_compaction: parsed {} entries",
            entries.len()
        );

        Ok(entries)
    }

    /// Iterate all partitions with per-row timestamps, for use by the compaction merger.
    ///
    /// Returns `(RowKey, ScanRow, row_timestamp_micros)` for every row in the SSTable.
    /// Unlike [`iterate_all_partitions`]:
    ///
    /// - Row tombstones are returned as `Value::Tombstone(RowTombstone)` carrying
    ///   the actual deletion timestamp extracted from the on-disk row header.
    /// - Cell tombstones within live rows are stored as `Value::Tombstone(CellTombstone)`
    ///   inside the `Value::Map`, also carrying the actual cell-level deletion timestamp.
    /// - The third tuple element is the decoded row-level write timestamp, so the
    ///   merger can perform timestamp-accurate last-write-wins comparisons.
    ///
    /// Normal user-facing reads use [`scan`] / [`get`] / [`iterate_all_partitions`],
    /// which apply tombstone filtering.  Do NOT use this method for user-visible queries.
    ///
    /// (Issue #505)
    ///
    /// [`iterate_all_partitions`]: crate::storage::sstable::SSTableReader
    /// [`scan`]: crate::storage::sstable::SSTableReader::scan
    /// [`get`]: crate::storage::sstable::SSTableReader::get
    pub async fn iterate_all_partitions_for_compaction(
        &self,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<super::super::compaction_row::CompactionRow>> {
        let _scan = self.begin_scan(); // #3853 (no-op: merge readers are buffered)

        // Issue #2372: BTI (`da`) is chunk-compressed with the SAME V5 row layout
        // as nb (why `bti_scan_with_metadata` stitches+parses it with the V5
        // parser), but `requires_chunk_stitching()` gates on `is_nb_format()` and
        // EXCLUDES BTI — so without this branch a BTI table fell into the
        // block-by-block `parse_block_entries` fallback below and errored ("Blob
        // fallback not allowed … V5_0Bti"): the compaction path had no BTI branch,
        // unlike `get_all_entries`/`scan`. Route BTI through the SAME stitch path.
        // Otherwise only V5CompressedLegacy NB is supported here; other formats
        // fall back to iterate_all_partitions with timestamp 0 (LWW by run_index).
        if self.requires_chunk_stitching() || self.bti_partitions_db.is_some() {
            // We need schema; retrieve it once.
            // `schema` is Option<&TableSchema>; clone it into an owned value so we
            // can pass it to the async helper without borrow-checker issues.
            let owned_schema = schema.cloned().or_else(|| self.get_table_schema(None));

            // Reset chunk reader to start of data section (own per-scan cursor).
            let cursor = self.new_scan_cursor().await?;
            let header_size = self.calculate_header_size();
            {
                let mut file_guard = cursor.file.lock().await;
                file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
            }

            let entries = self
                .stitch_and_parse_all_chunks_for_compaction(&cursor, owned_schema.as_ref())
                .await?;

            return Ok(entries);
        }

        // Non-stitching fallback: use iterate_all_partitions and attach ts=0.
        let entries = self.iterate_all_partitions().await?;
        Ok(entries
            .into_iter()
            .map(|(key, value)| {
                super::super::compaction_row::CompactionRow::from_legacy_value(key, value, 0)
            })
            .collect())
    }

    /// Count the distinct PARTITION keys decoded from `Data.db` (issue #970).
    ///
    /// This is a partition-granular count — one per partition, never per row —
    /// used by the SSTable verifier's BTI cross-check (`verify.rs`). It must NOT
    /// be confused with [`get_all_entries`], whose `RowKey`s carry
    /// clustering/column/static suffixes and therefore over-count a multi-row
    /// partition as many distinct keys.
    ///
    /// Both supported Data.db layouts surface the partition key (without any
    /// clustering suffix) via the compaction read path:
    ///
    /// - BIG (`nb`, `V5CompressedLegacy` + `is_nb_format`):
    ///   [`iterate_all_partitions_for_compaction`] emits one
    ///   [`CompactionRow`](super::super::compaction_row::CompactionRow) per row, each
    ///   carrying the partition key (`CompactionRow::key`). Distinct keys ==
    ///   partition count.
    /// - BTI (`da`): the compaction iterator's non-stitching fallback would route
    ///   through [`iterate_all_partitions`], whose keys are row-granular. Instead
    ///   we stitch the data section and run the compaction parser directly, which
    ///   emits the same partition-key-only `CompactionRow::key`.
    ///
    /// No schema is required from the caller: the parser resolves it via the
    /// reader's header/registry (`get_table_schema`).
    ///
    /// [`get_all_entries`]: crate::storage::sstable::SSTableReader::get_all_entries
    /// [`iterate_all_partitions`]: crate::storage::sstable::SSTableReader
    pub async fn distinct_partition_count(&self) -> Result<usize> {
        Ok(self.distinct_partition_keys().await?.len())
    }

    /// Return the set of distinct **raw** partition keys decoded from `Data.db`,
    /// one entry per partition (NOT per row), in first-seen order.
    ///
    /// Each key is the raw serialized partition key as stored on disk (e.g. the
    /// 4-byte big-endian value for `pk int`, 16 bytes for a UUID) — the same form
    /// accepted by
    /// [`encode_partition_key_for_bti_trie`](crate::storage::sstable::bti::parser::encode_partition_key_for_bti_trie).
    /// This is used by the verifier's BTI `Partitions.db` cross-check to compare
    /// partition-key IDENTITY (issue #1103), not just partition count.
    ///
    /// The stitch/parse strategy mirrors [`Self::distinct_partition_count`]: we
    /// route through `stitch_all_chunks` (not the generic
    /// `iterate_all_partitions_for_compaction`) for BOTH BIG and BTI so the
    /// incompressible/raw-chunk fallback is honoured (issue #970), and parse with
    /// the compaction parser, which emits one `CompactionRow` per partition with
    /// the partition key in `key` (partition-granular). No schema is required
    /// from the caller: the parser resolves it via the reader's header/registry.
    pub async fn distinct_partition_keys(&self) -> Result<Vec<Vec<u8>>> {
        let _scan = self.begin_scan(); // #3853 (no-op: merge readers are buffered)
        use std::collections::HashSet;

        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        let whole = self.stitch_all_chunks(&cursor).await?;

        let effective_schema = self.get_table_schema(None);
        let parser = self.build_v5_parser(false);
        // #3782: `whole` is the entire data section (fresh cursor, seeked to the
        // data-section start, every chunk stitched).
        let rows = parser.parse_block_for_compaction(
            &whole,
            BufferExtent::Complete,
            effective_schema.as_ref(),
            self,
        )?;

        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for r in &rows {
            let k = r.key.as_bytes();
            if seen.insert(k.to_vec()) {
                keys.push(k.to_vec());
            }
        }
        Ok(keys)
    }

    /// Return the distinct raw partition keys decoded from `Data.db` together with
    /// the byte offset at which each partition begins in the DECOMPRESSED data
    /// section (issue #1103).
    ///
    /// Each tuple is `(data_position, raw_partition_key)`, where `data_position`
    /// is exactly the value a BTI `Partitions.db` leaf encodes as
    /// [`BtiPartitionLocation::DataOffset`](crate::storage::sstable::bti::parser::BtiPartitionLocation::DataOffset)
    /// (and the `data_position` recovered from a `RowsOffset` entry via
    /// [`resolve_rows_db_entry`](crate::storage::sstable::bti::parser::resolve_rows_db_entry)).
    /// The verifier's BTI cross-check resolves each leaf PAYLOAD back to its raw
    /// partition key through this map so it catches a corruption that keeps the
    /// emitted trie prefix but rewrites the payload to point at a DIFFERENT
    /// partition (a same-count wrong-IDENTITY corruption the prefix-only compare
    /// missed).
    ///
    /// The stitch/parse strategy mirrors [`Self::distinct_partition_keys`]; only
    /// the parser entry point differs (it threads the partition-start offset).
    pub async fn distinct_partition_keys_with_positions(&self) -> Result<Vec<(u64, Vec<u8>)>> {
        let _scan = self.begin_scan(); // #3853 (no-op: merge readers are buffered)
        use std::collections::HashSet;

        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        let whole = self.stitch_all_chunks(&cursor).await?;

        let effective_schema = self.get_table_schema(None);
        let parser = self.build_v5_parser(false);

        // `seen` dedups partition keys; the recorded position is the FIRST row's
        // offset for a partition (a partition spans contiguous rows, so the first
        // row's offset is the partition start). `result` preserves first-seen
        // order and is the only place the position is read back.
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut result: Vec<(u64, Vec<u8>)> = Vec::new();
        // #3782: `whole` is the entire data section (see above).
        parser.parse_block_for_compaction_emit_with_offset(
            &whole,
            BufferExtent::Complete,
            effective_schema.as_ref(),
            self,
            |partition_start, row| {
                let k = row.key.as_bytes().to_vec();
                if seen.insert(k.clone()) {
                    result.push((partition_start as u64, k));
                }
                Ok(std::ops::ControlFlow::Continue(()))
            },
        )?;

        Ok(result)
    }

    /// Verifier-facing scan (issue #1282): return, in on-disk order, every
    /// distinct partition's raw key together with its raw partition-level
    /// `localDeletionTime` (when the partition carries a tombstone).
    ///
    /// Each element is `(raw_partition_key, partition_local_deletion_time)` where
    /// the LDT is `Some(i32)` only for a DELETED partition (a live partition
    /// carries the `DeletionTime.LIVE` sentinel and yields `None`). The `i32` is
    /// exactly the value [`parse_partition_header_full`] decodes: for the legacy
    /// signed `nb` form it is the genuine signed `i32 BE`; for the unsigned
    /// `oa`/`da` form it is the wrapping `as u32 as i32` representation of the
    /// on-disk `u32` (far-future values in `[2^31, 2^32)` therefore appear
    /// negative and are LEGITIMATE — the caller must consult
    /// [`SSTableReader::has_uint_deletion_time`] before interpreting a negative
    /// value as corrupt).
    ///
    /// The ordering is the on-disk partition order, so the verifier can assert
    /// ascending Murmur3 token order (Cassandra stores partitions in token order)
    /// without a separate scan.
    ///
    /// [`parse_partition_header_full`]: crate::storage::sstable::reader::parsing::V5CompressedLegacyParser::parse_partition_header_full
    pub async fn partition_verify_scan(&self) -> Result<Vec<(Vec<u8>, Option<i32>)>> {
        let _scan = self.begin_scan(); // #3853 (no-op: merge readers are buffered)
        use std::collections::HashSet;

        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        let whole = self.stitch_all_chunks(&cursor).await?;

        let effective_schema = self.get_table_schema(None);
        let parser = self.build_v5_parser(false);

        // First pass: recover the distinct partition-start offsets in on-disk
        // order (a partition spans contiguous rows, so the first row's offset is
        // the partition start). Reuses the same emit-with-offset parser the BTI
        // identity cross-check relies on, so partition framing stays defined in
        // exactly one place.
        //
        // Issue #1282 (roborev): dedup by partition BOUNDARY (the per-partition
        // start offset), NOT by raw key. Deduping by key would silently DROP a
        // duplicate partition key that appears again later on disk — precisely the
        // corruption Cassandra's `Verifier` flags ("Key out of order" for a
        // non-increasing `(token, key)` step). Keying on the boundary offset
        // collapses the many rows WITHIN one partition (which all share that
        // offset) to a single entry while letting each DISTINCT on-disk
        // partition-start — including a duplicated key — reach the classifier.
        let mut seen: HashSet<usize> = HashSet::new();
        let mut starts: Vec<usize> = Vec::new();
        // #3782: `whole` is the entire data section (see above).
        parser.parse_block_for_compaction_emit_with_offset(
            &whole,
            BufferExtent::Complete,
            effective_schema.as_ref(),
            self,
            |partition_start, _row| {
                if seen.insert(partition_start) {
                    starts.push(partition_start);
                }
                Ok(std::ops::ControlFlow::Continue(()))
            },
        )?;

        // Second pass: for each partition start, decode the raw partition header
        // (authoritative key + partition-level DeletionTime). The header decode is
        // byte-exact (no wrapping for the signed `nb` form), so the verifier sees
        // a genuine negative `nb` `localDeletionTime`.
        let mut result: Vec<(Vec<u8>, Option<i32>)> = Vec::with_capacity(starts.len());
        for start in starts {
            let (row_key, _next, partition_deletion) =
                parser.parse_partition_header_full(&whole, start)?;
            let ldt = partition_deletion.map(|(_mfda, ldt)| ldt);
            result.push((row_key.as_bytes().to_vec(), ldt));
        }

        Ok(result)
    }

    /// Verifier-facing scan (issue #1282, roborev follow-up): return, in on-disk
    /// order, every partition's ordered list of decoded CLUSTERING-key tuples.
    ///
    /// Each element is `(partition_index, Vec<Vec<Value>>)`, where the inner
    /// `Vec<Value>` is one clustering row's values in schema clustering order, and
    /// the outer `Vec` holds those rows in the exact ORDER they appear on disk. The
    /// verifier compares consecutive tuples within a partition using the
    /// authoritative per-column [`crate::types::comparator::ComparatorType`] plus
    /// each clustering column's ASC/DESC order, and flags a non-increasing step as
    /// [`crate::storage::sstable::verify::VerifyErrorClass::OutOfOrderKeyOrRow`] —
    /// the "row" half of that class (Cassandra's `Verifier` rejects out-of-order
    /// clustering rows too).
    ///
    /// Rows carrying no clustering prefix (a static row, a partition-level
    /// tombstone carrier, or an unclustered table's single row) are OMITTED from
    /// the per-partition list: they do not participate in clustering order.
    ///
    /// Reuses the SAME on-disk-order emit-with-offset parser as
    /// [`Self::partition_verify_scan`], so the clustering values come from the
    /// authoritative schema-aware decode, not a heuristic. Uses only non-gated CQL
    /// comparators (no `write-support` dependency). When the table has no clustering
    /// columns the returned list is empty (nothing to order).
    pub async fn partition_clustering_verify_scan(
        &self,
    ) -> Result<Vec<(usize, Vec<Vec<crate::types::Value>>)>> {
        let _scan = self.begin_scan(); // #3853 (no-op: merge readers are buffered)
        use crate::types::Value;
        use std::collections::BTreeMap;

        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        let whole = self.stitch_all_chunks(&cursor).await?;

        let effective_schema = self.get_table_schema(None);

        // A table with no clustering columns has no intra-partition row order to
        // verify — return early (empty) without decoding rows.
        let has_clustering = effective_schema
            .as_ref()
            .map(|s| !s.clustering_keys.is_empty())
            .unwrap_or(false);
        if !has_clustering {
            return Ok(Vec::new());
        }

        let parser = self.build_v5_parser(false);

        // Group clustering tuples by partition-start offset (partitions appear in
        // ascending on-disk offset order; rows within a partition are emitted in
        // on-disk order). A BTreeMap keyed by the boundary offset both dedups the
        // partition boundary and preserves on-disk partition order.
        let mut by_partition: BTreeMap<usize, Vec<Vec<Value>>> = BTreeMap::new();
        let schema = effective_schema.as_ref();
        // #3782: `whole` is the entire data section (see above).
        parser.parse_block_for_compaction_emit_with_offset(
            &whole,
            BufferExtent::Complete,
            schema,
            self,
            |partition_start, row| {
                let bucket = by_partition.entry(partition_start).or_default();
                // A partition with only non-clustering rows still records an
                // (empty) bucket so its index stays stable.
                if let Some(tuple) = Self::clustering_tuple_of_compaction_row(&row, schema) {
                    bucket.push(tuple);
                }
                Ok(std::ops::ControlFlow::Continue(()))
            },
        )?;

        Ok(by_partition.into_values().enumerate().collect())
    }

    /// Extract the clustering-key tuple of a compaction row in schema order, or
    /// `None` when the row carries no clustering prefix (static row, partition-level
    /// tombstone carrier). Used only by the verifier's clustering-order check.
    ///
    /// Clustering columns are surfaced on the compaction read path as pseudo
    /// simple-cells (row tombstones carry them in their dedicated `clustering`
    /// field). We rebuild the tuple positionally from the schema's clustering
    /// columns so the comparison uses authoritative positions.
    fn clustering_tuple_of_compaction_row(
        row: &super::super::compaction_row::CompactionRow,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Option<Vec<crate::types::Value>> {
        use super::super::compaction_row::CompactionRowData;

        let schema = schema?;
        if schema.clustering_keys.is_empty() {
            return None;
        }

        match &row.row_data {
            CompactionRowData::Tombstone { clustering, .. } => {
                if clustering.len() != schema.clustering_keys.len() {
                    // A partial/absent clustering prefix is not a full point row
                    // (Cassandra's row order is over full clustering keys); skip.
                    None
                } else {
                    Some(clustering.iter().map(|(_, v)| v.clone()).collect())
                }
            }
            CompactionRowData::Live { simple, .. } => {
                // A clustering row surfaces every clustering column as a simple
                // pseudo-cell. Rebuild the tuple in schema order; if not every
                // clustering column is present the row has no full clustering
                // prefix (e.g. a static row) and is omitted.
                let mut values: Vec<crate::types::Value> =
                    Vec::with_capacity(schema.clustering_keys.len());
                for ck in &schema.clustering_keys {
                    // Any missing clustering column ⇒ the WHOLE key is discarded (the `?`
                    // returns `None`, treating the row as unclustered).
                    let cell = simple.iter().find(|c| c.column == ck.name)?;
                    values.push(cell.value.clone());
                }
                Some(values)
            }
            // Range markers / partition deletes are not clustering point rows.
            CompactionRowData::RangeMarker { .. } | CompactionRowData::PartitionDelete { .. } => {
                None
            }
        }
    }

    /// Streaming compaction read (issue #827): yield `(RowKey, ScanRow, ts)`
    /// entries via `emit` one partition at a time, so peak memory is bounded by
    /// `max_partition_size + one_chunk` rather than by the total input size.
    ///
    /// This is the incremental counterpart of
    /// [`iterate_all_partitions_for_compaction`], which fully materialises the
    /// decompressed data section and parses every entry into a `Vec` before
    /// returning. The k-way merge producer (`merge::producer_thread`) uses this
    /// to forward entries into its bounded channel directly, so a source's
    /// decompressed content is never fully resident.
    ///
    /// ## Sliding-window driver
    ///
    /// The V5CompressedLegacy chunk-stitching path keeps a sliding
    /// [`WindowCursor`](super::super::window_cursor::WindowCursor) of decompressed
    /// bytes. After refilling with each decompressed chunk it drains confirmed
    /// partitions via `parse_one_partition_with_timestamps`, advancing the cursor
    /// over the front after every `Emitted` (the reclaimed prefix is compacted once
    /// per refill, not memmoved per partition — issue #1589), and stopping at
    /// `NeedMore` to await the next chunk (a partition can straddle a chunk
    /// boundary). At EOF a final drain pass runs with
    /// `at_final_chunk = true` so the trailing (possibly truncated) partition is
    /// terminal rather than requesting a refill that will never come.
    ///
    /// Returning `ControlFlow::Break` from `emit` stops the scan early
    /// (consumer dropped). Tombstone / timestamp semantics are byte-identical to
    /// the Vec variant (Issue #505/#533).
    ///
    /// `scan_cancel` is an explicit PER-CALL cancellation token (issue #2346),
    /// not the reader's own [`SSTableReader::scan_cancel`] field: a cached/shared
    /// `Arc<SSTableReader>` (a future warm-handle registry) may drive two
    /// concurrent calls to this method with two INDEPENDENT tokens, so
    /// cancellation cannot live as mutable per-reader state (`set_scan_cancel`
    /// requires `&mut self`, uncallable through a shared `Arc`). Callers that
    /// want the reader's own field's semantics (the pre-#2346 default) pass
    /// `&self.scan_cancel` explicitly — see
    /// [`SSTableReader::iterate_all_partitions_cancellable`] for the analogous
    /// non-compaction seam.
    pub async fn stream_all_partitions_for_compaction<F>(
        &self,
        schema: Option<&crate::schema::TableSchema>,
        scan_cancel: &ScanCancel,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut(super::super::compaction_row::CompactionRow) -> Result<std::ops::ControlFlow<()>>,
    {
        let _scan = self.begin_scan(); // #3853 (no-op: merge readers are buffered)

        // Reset chunk reader to the start of the data section (mirrors
        // iterate_all_partitions_for_compaction) using an own per-scan cursor.
        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        // Non-stitching formats (uncompressed / non-nb BIG — the #2361 field
        // case): TRUE-stream each partition as the index walk resolves it, rather
        // than materialising the whole SSTable into one Vec before the first emit
        // (issue #2361). This is what lets a downstream `LIMIT` early-break and a
        // client-disconnect cancellation take effect promptly and keeps peak
        // memory bounded on a 1M-partition table. No producer-side `limit`
        // parameter (roborev round 2): a partition count is not a safe proxy for
        // a row-level `LIMIT` (see `stream_all_partitions_cancellable`'s doc) —
        // `LIMIT` is enforced purely downstream via the consumer's
        // post-reconciliation break + cancel-aware Drop teardown. Issue #2346:
        // the PER-CALL `scan_cancel` governs the whole walk. Issue #2372: BTI
        // (`da`) is EXCLUDED here even though `requires_chunk_stitching()` is false
        // for it — the non-stitch fallback (`sequential_scan`) has no BTI branch
        // and would hit the no-schema `parse_block_entries` error; the stitch+parse
        // drain below decodes BTI correctly (as `bti_scan_with_metadata` does).
        if !self.requires_chunk_stitching() && self.bti_partitions_db.is_none() {
            self.stream_all_partitions_cancellable(scan_cancel, None, |(key, value)| {
                let row =
                    super::super::compaction_row::CompactionRow::from_legacy_value(key, value, 0);
                emit(row)
            })
            .await?;
            return Ok(());
        }

        // Resolve the schema the parser needs (cells lack column names on disk).
        let owned_schema = schema.cloned().or_else(|| self.get_table_schema(None));
        let parser = self.build_v5_parser(false);

        // Issue #2299 (roborev blocker): build the column resolution ONCE for the
        // whole scan and thread it into every drain step. `RowColumnResolution`
        // is derived purely from the SSTable serialization header
        // (`self.header`) + `schema`, both INVARIANT across every partition of a
        // single SSTable, so one build is semantically identical to the buffered
        // `drive_partition_sliding`'s per-PARTITION build
        // (`partition_driver.rs:179`) — but WITHOUT the per-structure rebuild the
        // row-granular driver would otherwise incur. `build` allocates a HashMap
        // over `schema.columns` plus a fresh `Arc<str>` per header/clustering
        // column; rebuilding it per drain step (once per row on a wide partition)
        // turned an O(partitions) alloc cost into O(rows × header_cols) on exactly
        // the wide-partition workload this issue optimizes. Both `owned_schema`
        // (a local) and `self` (the reader) outlive the drain loop below, so the
        // borrow is sound for the whole scan.
        let resolution = owned_schema
            .as_ref()
            .map(|s| crate::storage::sstable::reader::parsing::RowColumnResolution::build(s, self));

        // Sliding window with a FRONT CURSOR (issue #1589): confirmed structures are
        // consumed by advancing the cursor, and the reclaimed prefix is compacted
        // once per refill — not memmoved per partition as the old front-drain did.
        let mut window = WindowCursor::new();
        let mut broke = false;

        // Issue #2299: row-granular resumable partition state. Unlike the buffered
        // `parse_one_partition_for_compaction` (which drained a WHOLE partition per
        // step, so a WIDE partition stayed fully resident in both the window and the
        // parser's `pending` vec), this carries the in-flight partition's decode
        // context across refills so the drain below advances the window cursor after
        // EVERY confirmed row — peak memory is bounded by one row + one chunk, not by
        // `max_partition_size`. This is what keeps a real compaction of CQLite's own
        // uncompressed output (one wide partition split across the merge inputs)
        // within the 128 MiB budget.
        let mut partition_state =
            crate::storage::sstable::reader::parsing::CompactionPartitionState::new();

        use crate::storage::sstable::compression::Compression;

        // Incompressible-chunk fallback (Bug #639, epic #970, issue #1104):
        // Cassandra stores a chunk RAW when its compressed length would meet or
        // exceed `max_compressed_length`. This is the path the real k-way merge
        // producer streams through, so it must honour the rule exactly like
        // `stitch_all_chunks`/`stitch_and_parse_all_chunks_for_compaction`: when
        // the (CRC-stripped) chunk length >= max_compressed_length, the bytes are
        // already plaintext. Authority: CompressedSequentialWriter.java:160-177.
        let max_compressed_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.max_compressed_length as usize)
            .unwrap_or(usize::MAX);

        let mut chunk_count = 0;
        loop {
            // Issue #2819 (B4): a BTI/`da` table (and any Summary-guided FellBack)
            // routes HERE, not the instrumented `compressed_offset.rs` path, so time
            // the page-in (cold_fault) + decompress (below) on this fallback too, or
            // a BTI scan would emit only 3 of 5 sub-phases. `scoped` is None (zero
            // cost) with no flight sink; the block scopes it to just the `.await`.
            // (#1116: this reader is over the campsite target; these are the minimal
            // instrumentation lines — full gate run with CQLITE_ALLOW_FILE_GROWTH=1.)
            let next_block = {
                let _t = crate::observability::stream_subphase::scoped(
                    crate::observability::StreamSubPhase::ColdFault,
                );
                self.read_next_block(&cursor).await?
            };
            let Some(compressed_chunk) = next_block else {
                break;
            };
            // Cooperative cancellation (issue #2264, roborev round 3): a poll
            // every 256 chunks catches the edge case `drain_compaction_window`'s
            // per-PARTITION poll cannot — a single partition so wide it spans
            // hundreds of chunks without ever completing (so the drain loop's
            // counter never advances).
            if chunk_count & 0xFF == 0 {
                scan_cancel.check()?;
            }
            let algorithm = self.compression_reader.as_ref().map(|r| r.algorithm());
            let decompressed_chunk = if compressed_chunk.len() >= max_compressed_length {
                // Raw bytes through, COUNTED at the plane's boundary (#1701 R3).
                super::super::chunk_source::counted_raw_chunk(compressed_chunk, algorithm)
            } else if let Some(compression_reader) = &self.compression_reader {
                let compression = Compression::new(*compression_reader.algorithm())?;
                // Issue #2819 (B4): LZ4 decompress — the `stream_decompress` scope
                // (reached only for a genuinely compressed chunk). #1701 R3: the
                // decompress resolves in the METERED plane.
                crate::observability::stream_subphase::timed(
                    crate::observability::StreamSubPhase::Decompress,
                    || {
                        super::super::chunk_source::ChunkSource::decompress_only(
                            Some(&compression),
                            compressed_chunk,
                        )
                    },
                )
                .map_err(|e| {
                    Error::corruption(format!(
                        "stream_all_partitions_for_compaction: Failed to decompress chunk {}: {}",
                        chunk_count, e
                    ))
                })?
            } else {
                compressed_chunk // counted by read_next_block; see count_uncompressed_block
            };
            // Refill the window: compact the already-consumed prefix ONCE
            // (issue #1589), then append the freshly decompressed chunk.
            window.refill(&decompressed_chunk);
            chunk_count += 1;

            // Not the final chunk yet: NeedMore means "await more bytes". Drain
            // every confirmed structure (row / marker / partition end) from the
            // front of the window, advancing the cursor per structure.
            self.drain_compaction_window(
                &parser,
                owned_schema.as_ref(),
                resolution.as_ref(),
                &mut window,
                false,
                &mut emit,
                &mut broke,
                &mut partition_state,
                scan_cancel,
            )?;
            if broke {
                return Ok(());
            }
        }

        // EOF: final drain — a truncated/unterminated trailing partition is now
        // terminal (Done), not a refill request.
        if !broke {
            self.drain_compaction_window(
                &parser,
                owned_schema.as_ref(),
                resolution.as_ref(),
                &mut window,
                true,
                &mut emit,
                &mut broke,
                &mut partition_state,
                scan_cancel,
            )?;
        }

        tracing::debug!(
            "stream_all_partitions_for_compaction: drained {} chunks (final window {} bytes)",
            chunk_count,
            window.len()
        );

        Ok(())
    }

    /// Drain every confirmed STRUCTURE (row / range marker / partition end) from
    /// the front of the sliding `window`, emitting each row via `emit` (issue #827,
    /// row-granular per issue #2299). After each confirmed structure the consumed
    /// prefix is advanced (the window's peak stays bounded by `one row + one
    /// chunk` — NOT `max_partition_size` — so a WIDE partition never has to be
    /// fully resident). Stops at `NeedMore` / `AllDone` (await the next chunk /
    /// genuine end) or when `emit` returns `Break` (sets `*broke`).
    ///
    /// `partition_state` carries the in-flight partition's decode context across
    /// refills (its key + in-flight range-tombstone start bound), so a partition
    /// straddling a chunk boundary resumes correctly.
    #[allow(clippy::too_many_arguments)]
    fn drain_compaction_window<F>(
        &self,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
        schema: Option<&crate::schema::TableSchema>,
        resolution: Option<&crate::storage::sstable::reader::parsing::RowColumnResolution>,
        window: &mut WindowCursor,
        at_final_chunk: bool,
        emit: &mut F,
        broke: &mut bool,
        partition_state: &mut crate::storage::sstable::reader::parsing::CompactionPartitionState,
        scan_cancel: &ScanCancel,
    ) -> Result<()>
    where
        F: FnMut(super::super::compaction_row::CompactionRow) -> Result<std::ops::ControlFlow<()>>,
    {
        use crate::storage::sstable::reader::parsing::PartitionStreamStep;
        let mut drained: usize = 0;
        loop {
            if *broke || window.is_empty() {
                return Ok(());
            }
            // Cooperative cancellation (issue #2264): for a chunk-stitched ('nb')
            // SSTable this is the per-STRUCTURE hot loop the compaction stream (and
            // thus a Flight `do_get`) spends its time in. Poll the PER-CALL cancel
            // token (issue #2346) at a bounded interval so a disconnected client
            // abandons the walk within milliseconds instead of the coarse ~1–2 min
            // backstop. Per-structure polling (rather than per-partition) also
            // catches a single partition so wide it spans hundreds of chunks.
            if drained & 0xFF == 0 {
                scan_cancel.check()?;
            }
            drained += 1;
            let step = parser.stream_partition_body_incremental(
                window.as_slice(),
                schema,
                self,
                resolution,
                at_final_chunk,
                partition_state,
                &mut |row: super::super::compaction_row::CompactionRow| emit(row),
            )?;
            match step {
                // A confirmed mid-partition structure: advance the cursor over
                // exactly the bytes it consumed (issue #1589: NO memmove here —
                // the reclaimed prefix is compacted once at the next refill).
                // `consume` clamps to the remaining length. The partition
                // CONTINUES, so the #2398 per-partition work-probe is NOT bumped
                // here (it counts partitions, not structures — see PartitionDone).
                //
                // Forward-progress guard (roborev blocker, issue #2299): the
                // pre-#2299 buffered driver clamped a zero-length "confirmed"
                // step to 1 (`let take = if consumed == 0 { 1 } else { consumed
                // };`) — this row-granular rewrite dropped that clamp. A
                // `Consumed(0)` here would leave `window` unchanged AND
                // `state.header_parsed` still `true`, so the NEXT call re-parses
                // the IDENTICAL front byte and can return the SAME step forever:
                // a live `do_get` consumer would spin, since `scan_cancel` is
                // only polled every 256 iterations and escapes on client
                // DISCONNECT, never on lack of forward progress. Every decode
                // path in `stream_partition_body_incremental` currently
                // guarantees `consumed >= 1` on a genuine `Ok` (the row-flags /
                // marker-flags / partition-key-length byte is always >= 1 on
                // success — see that function's doc comment), so this clamp is
                // defense-in-depth against a FUTURE decoder change silently
                // reintroducing the pre-#2299 hang, restoring the invariant the
                // type signature alone does not enforce.
                PartitionStreamStep::Consumed(consumed) => {
                    window.consume(clamp_forward_progress(consumed));
                }
                // The partition ended. Advance over its final bytes (`consumed ==
                // 0` for a terminal trailing partition makes no progress, but the
                // window is then empty so the top-of-loop guard returns).
                PartitionStreamStep::PartitionDone(consumed) => {
                    // Work-probe (issue #2398): one partition body decoded on the
                    // chunk-stitching ('nb') scan path — the counterpart to the
                    // streaming full-index walk's per-partition increment. Placed
                    // on PartitionDone (NOT the mid-partition Consumed) so a wide
                    // partition drained row-by-row (issue #2299) still counts
                    // exactly once. A token-range split keeps this bounded to its
                    // in-range slice, not the SSTable's whole partition count.
                    crate::storage::sstable::work_counters::add_stream_walk_partition_parsed();
                    window.consume(consumed);
                }
                // Consumer dropped mid-emit: advance over the breaking structure and
                // stop the whole scan.
                PartitionStreamStep::Break(consumed) => {
                    window.consume(consumed);
                    *broke = true;
                    return Ok(());
                }
                // Await the next chunk (a structure straddles this boundary) or a
                // genuine end of data.
                PartitionStreamStep::NeedMore | PartitionStreamStep::AllDone => return Ok(()),
            }
        }
    }
}

/// Forward-progress clamp for [`SSTableReader::drain_compaction_window`]'s
/// `PartitionStreamStep::Consumed` arm (roborev blocker, issue #2299): a
/// `consumed == 0` step must still advance the window by at least one byte, or
/// the drain loop can spin on a stalled decoder forever (see the call site's
/// doc comment for the full hang mechanism this restores protection against).
#[inline]
fn clamp_forward_progress(consumed: usize) -> usize {
    if consumed == 0 {
        1
    } else {
        consumed
    }
}

#[cfg(test)]
mod forward_progress_guard_tests {
    use super::clamp_forward_progress;
    use crate::storage::sstable::reader::window_cursor::WindowCursor;

    /// The clamp forces a zero-length step to advance by exactly one byte.
    #[test]
    fn zero_consumed_is_clamped_to_one() {
        assert_eq!(clamp_forward_progress(0), 1);
    }

    /// A genuinely nonzero consumed count is passed through unchanged.
    #[test]
    fn nonzero_consumed_is_unaffected() {
        for n in [1usize, 2, 64, 4096] {
            assert_eq!(clamp_forward_progress(n), n);
        }
    }

    /// RED-then-GREEN, deterministic bounded-iteration simulation (roborev
    /// blocker, issue #2299): NOT a live-hang repro through the real parser —
    /// every current decode path in `stream_partition_body_incremental`
    /// provably reports `consumed >= 1` on a genuine `Ok` (the row-flags /
    /// marker-flags / partition-key-length byte is always >= 1 on success), so
    /// no byte sequence drives today's parser to a `Consumed(0)` on a
    /// non-empty window. This test instead pins the CLAMP's own behavior: a
    /// hypothetical decoder that always reports 0 consumed bytes (exactly the
    /// pre-#2299-regression shape) must still drain a finite window in
    /// bounded steps, never an unbounded loop. The iteration bound is derived
    /// from the buffer length (deterministic), never a wall-clock timeout —
    /// pre-fix (no clamp) this loop would spin forever and the `iterations <=
    /// bound` assertion would be the only thing standing between a passing
    /// test and a hung test process.
    #[test]
    fn simulated_stalled_decoder_terminates_within_window_len_iterations() {
        let mut window = WindowCursor::new();
        let payload = vec![0xFFu8; 32];
        window.refill(&payload);

        let bound = payload.len() + 1;
        let mut iterations = 0usize;
        while !window.is_empty() {
            iterations += 1;
            assert!(
                iterations <= bound,
                "forward-progress guard failed to terminate within {bound} iterations \
                 (issue #2299 roborev blocker: a stalled decoder that always reports \
                 0 consumed bytes must still drain the window in bounded steps, never \
                 spin unboundedly)"
            );
            // Simulate a decoder that ALWAYS reports it consumed 0 bytes (the
            // hypothetical pre-#2299-regression behavior) — the clamp must
            // still force progress.
            let consumed = clamp_forward_progress(0);
            window.consume(consumed);
        }
        assert_eq!(
            iterations,
            payload.len(),
            "each iteration must advance by exactly 1 clamped byte"
        );
    }
}
