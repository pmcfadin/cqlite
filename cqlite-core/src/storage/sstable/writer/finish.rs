//! Finalization + component-path helpers for [`SSTableWriter`].
//!
//! Houses [`SSTableWriter::finish`] — the orchestration that writes every
//! component in the critical order and selects the BIG vs BTI component set —
//! together with the shared component-naming, bloom-filter, and CRC32 helpers
//! it relies on. The BTI-specific `Rows.db`/`Partitions.db` serialization is
//! delegated to [`SSTableWriter::write_bti_components`] (see `bti_state`), so
//! `finish` stays a thin format-aware orchestrator. All bytes are unchanged
//! from the prior inline implementation.

use super::{
    ComponentEntry, DigestWriter, SSTableFormat, SSTableInfo, SSTableWriter, StatisticsWriter,
    TocWriter,
};
use crate::error::Result;
use crate::schema::TableSchema;
use std::path::{Path, PathBuf};

impl SSTableWriter {
    /// Finish writing all components and return SSTable information
    ///
    /// This method:
    /// 1. Finalizes statistics metadata
    /// 2. Writes all component files in the correct order
    /// 3. Computes checksums
    /// 4. Writes TOC.txt (publication barrier)
    /// 5. Returns SSTableInfo with file paths and metadata
    ///
    /// # Returns
    ///
    /// SSTableInfo containing paths to all written files and metadata.
    ///
    /// # Errors
    ///
    /// Returns error if any component write fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let info = writer.finish().await?;
    /// println!("SSTable written to {}", info.data_path.display());
    /// ```
    #[tracing::instrument(name = "writer.finish", skip(self))]
    pub async fn finish(mut self) -> Result<SSTableInfo> {
        // Create keyspace/table subdirectory structure so the reader can
        // extract the table name from the parent directory path. Owned clone so
        // `finish_streaming()` can move `self.data_writer` out below.
        let sstable_dir = self.sstable_dir.clone();
        let sstable_dir = sstable_dir.as_path();
        // Ensure the keyspace/table directory tree exists (an empty flush never
        // opened a streaming sink, so this may be the point of creation). The
        // flush durability barrier later fsyncs the full leaf→data-root ancestor
        // chain unconditionally before truncating the WAL, so directory
        // *creation* no longer needs to record which ancestors it made (issue
        // #1392).
        crate::storage::write_engine::durability::create_dir_all(sstable_dir)?;

        // Finalize statistics metadata (normalize sentinel values)
        self.stats.finalize();

        // Capture format/generation up front so component paths can be computed
        // after `self`'s writers are partially moved out by their `finish()` calls.
        let format = self.format;
        let generation = self.generation;
        let is_bti = matches!(format, SSTableFormat::Bti);
        let cpath =
            |component: &str| Self::component_path_for(sstable_dir, generation, format, component);

        // 1. Write Statistics.db (FIRST - provides delta baseline).
        // BTI (`da`) requires the BtiFormat `StatsMetadata` layout (covered
        // clustering Slice, uint deletion times, key range, token-space coverage)
        // — Cassandra's sstabledump/sstablemetadata reject the legacy `nb` layout
        // on a `da` descriptor (issue #911). BIG keeps the legacy layout.
        let stats_path = cpath("Statistics.db");
        let stats_writer = if is_bti {
            StatisticsWriter::new_bti(stats_path.clone())
        } else {
            StatisticsWriter::new(stats_path.clone())
        };
        stats_writer.write(&self.stats, Some(&self.schema))?;
        // fsync Statistics.db contents (issue #1392): stats_writer uses
        // std::fs::write, which does not fsync.
        Self::fsync_component(&stats_path).await?;

        // 2. Finalize Data.db (Issue #492)
        // The DataWriter has been streaming each partition to disk as it was
        // written, so there is no whole-file buffer to write here. `finish_streaming`
        // flushes and fsyncs the sink and returns the total byte size. If no
        // partitions were written, lazily ensure an (empty) Data.db file exists so
        // the downstream Digest CRC re-read and TOC publication remain valid.
        //
        // The BTI `Data.db` row/partition serialization is identical to BIG in
        // Cassandra 5 (issue #908); only the filename descriptor differs.
        let data_path = cpath("Data.db");
        // The streamed Data.db is fsynced inside `finish_streaming` (issue
        // #1392). The empty-Data fallback below is written via tokio::fs::write
        // (no fsync), so it is fsynced explicitly.
        // `finish_streaming` returns the byte size plus the whole-file
        // (`Digest.crc32`) and per-chunk (`CRC.db`) checksums accumulated during
        // the streaming write, so finalization needs no full re-read of Data.db
        // (issue #1663).
        let stream = self.data_writer.finish_streaming()?;
        let data_size = stream.data_size;
        if data_size == 0 && !data_path.exists() {
            tokio::fs::write(&data_path, b"").await?;
            Self::fsync_component(&data_path).await?;
        }

        // 3. Finalize Index.db (Issue #753) — BIG only.
        // For BIG, the IndexWriter has been streaming each entry to Index.db; we
        // flush/sync it here. For BTI (issue #908) there is no Index.db: the
        // IndexWriter ran in counting-only mode (no sink, no retained entry bytes)
        // purely to compute offsets, so there is nothing to flush and we report no
        // path. (Calling `finish_streaming` on a non-streaming writer is an error,
        // so we skip it.)
        let index_path = if is_bti {
            None
        } else {
            let _index_size = self.index_writer.finish_streaming()?;
            Some(cpath("Index.db"))
        };

        // 4. Write Filter.db (path already set in constructor using the
        // format-aware component path).
        // A disabled bloom filter (bloom_filter_fp_chance = 1.0, Cassandra's
        // AlwaysPresentFilter) writes NO Filter.db component and must be omitted
        // from the TOC to stay byte-faithful (Issue #852).
        // `filter_path` is `Some` only when a concrete Filter.db is actually
        // written. A disabled filter (AlwaysPresentFilter) yields `None`, so
        // downstream consumers (compaction publish + byte accounting) skip the
        // non-existent component instead of trying to rename a missing file.
        let filter_path = match self.filter_writer {
            Some(filter_writer) => {
                let emitted = !filter_writer.is_disabled();
                let path = filter_writer.path().to_path_buf();
                filter_writer.finish().await?;
                if emitted {
                    Some(path)
                } else {
                    None
                }
            }
            None => None,
        };
        let filter_emitted = filter_path.is_some();

        // 5. Write Summary.db — BIG only.
        // BTI (issue #908) has no Summary.db; partition sampling is replaced by
        // the partition trie. We still drive `summary_writer.finish()` to keep its
        // accounting consistent, but for BTI we discard the bytes and write no file.
        let summary_bytes = self.summary_writer.finish()?;
        let summary_path = if is_bti {
            None
        } else {
            let path = cpath("Summary.db");
            tokio::fs::write(&path, summary_bytes).await?;
            // fsync Summary.db contents (issue #1392).
            Self::fsync_component(&path).await?;
            Some(path)
        };

        // 5.25. Write Rows.db + Partitions.db (BTI, issue #766 / #908 / #910).
        // Only emitted for SSTableFormat::Bti; for BIG both are None and nothing
        // is written, keeping the default path byte-for-byte unchanged. The
        // ordering rationale (Rows.db first, then the partition trie) and the
        // empty-BTI refusal live in `write_bti_components`.
        let bti_pending = self.bti_pending.take();
        let partitions_trie = self.partitions_trie.take();
        let (partitions_path, rows_path) =
            Self::write_bti_components(bti_pending, partitions_trie, &cpath).await?;

        // 5.5. CompressionInfo.db is omitted for uncompressed data.
        // Real Cassandra 5 SSTables do not include CompressionInfo.db when
        // data is uncompressed. The compression_info_writer module is retained
        // for future compressed SSTable support.

        // 6. Write Digest.crc32 (whole-file CRC32 of Data.db).
        // The streaming DataWriter accumulated this CRC as it wrote, so the
        // finished Data.db is NOT re-read here (issue #1663). The empty-Data.db
        // lazy path (no partitions streamed, an empty file written above) has no
        // accumulated bytes; it recomputes over the just-written empty file via
        // `compute_crc32` so the value stays correct (CRC32 of zero bytes = 0).
        let digest_path = cpath("Digest.crc32");
        let digest_writer = DigestWriter::new(digest_path.clone());
        let crc32_value = if data_size == 0 {
            Self::compute_crc32(&data_path).await?
        } else {
            stream.digest_crc32
        };
        digest_writer.write(crc32_value)?;

        // 6.5. Write CRC.db — per-chunk CRC32 for uncompressed BIG (issue #1197).
        // Cassandra 5.0 writes a CRC.db for every uncompressed BIG (`nb`)
        // SSTable, alongside Digest.crc32 (ChecksummedSequentialWriter). The
        // layout is a big-endian i32 chunk-size header (64 KiB) followed by one
        // big-endian u32 CRC32 per raw-data chunk. BTI (`da`) tables emit no
        // CRC.db (verified against the Cassandra-written `da` fixtures), and the
        // compressed path carries per-chunk CRCs inline (CompressionInfo.db),
        // so this is gated on the BIG + uncompressed write path only.
        // Flush vs compaction CRC.db tail (issue #1222). The compaction write
        // path appends one trailing empty-final-chunk CRC32 = 0 (Cassandra's
        // close-time zero-length buffer flush); the flush path does not. Selected
        // by `is_compaction_output` so the flush CRC.db stays byte-identical to
        // the #1190 goldens while the compaction CRC.db matches the #1017 goldens.
        let crc_path = if is_bti {
            None
        } else {
            use crate::storage::sstable::writer::crc_writer::{self, CrcTrailer};
            let trailer = if self.is_compaction_output {
                CrcTrailer::EmptyFinalChunk
            } else {
                CrcTrailer::None
            };
            // Assemble CRC.db from the per-chunk CRCs accumulated during the
            // streaming write — no re-read of the finished Data.db (issue #1663).
            // The trailer selection (flush vs compaction, issue #1222) and the
            // is_bti gate are unchanged; only the checksum INPUT SOURCE differs.
            let crc_path = cpath("CRC.db");
            let bytes = crc_writer::assemble_crc_bytes(&stream.chunk_crcs, trailer);
            tokio::fs::write(&crc_path, bytes).await?;
            // fsync CRC.db contents (issue #1392): tokio::fs::write does not fsync.
            Self::fsync_component(&crc_path).await?;
            Some(crc_path)
        };

        // 7. Write TOC.txt (LAST - publication barrier).
        //
        // The TOC lists exactly the component set actually written. BIG lists
        // Index.db + Summary.db; BTI (issue #908 / #910) omits both and lists
        // Partitions.db AND Rows.db instead (matching the real `da` fixtures,
        // which list Rows.db even when it is 0 bytes). TocWriter self-references
        // TOC.txt.
        use crate::storage::sstable::directory::types::SSTableComponent;
        let toc_path = cpath("TOC.txt");
        let toc_writer = TocWriter::new(toc_path.clone());
        let mut components = vec![ComponentEntry::new(SSTableComponent::Data)];
        // Filter.db is omitted entirely for a disabled bloom filter
        // (AlwaysPresentFilter), matching Cassandra (Issue #852).
        if filter_emitted {
            components.push(ComponentEntry::new(SSTableComponent::Filter));
        }
        components.extend([
            ComponentEntry::new(SSTableComponent::Statistics),
            ComponentEntry::new(SSTableComponent::Digest),
        ]);
        // CRC.db is listed for uncompressed BIG tables (issue #1197).
        if crc_path.is_some() {
            components.push(ComponentEntry::new(SSTableComponent::Crc));
        }
        // BIG lists Index.db + Summary.db; BTI (issue #908) omits both.
        if index_path.is_some() {
            components.push(ComponentEntry::new(SSTableComponent::Index));
        }
        if summary_path.is_some() {
            components.push(ComponentEntry::new(SSTableComponent::Summary));
        }
        // BTI (issue #766 / #908): list Partitions.db when the trie was emitted.
        if partitions_path.is_some() {
            components.push(ComponentEntry::new(SSTableComponent::Partitions));
        }
        // BTI (issue #910): list Rows.db when emitted (always for BTI).
        if rows_path.is_some() {
            components.push(ComponentEntry::new(SSTableComponent::Rows));
        }
        toc_writer.write(&components)?;

        // Directory durability barrier (issue #1959).
        //
        // Every component's *contents* is now fsynced (Data.db/Index.db at their
        // streaming-writer finish; Statistics/Summary/CRC/Rows/Partitions via
        // `fsync_component`; Filter/Digest/TOC inside their own writers), and the
        // TOC — the publication barrier — was written LAST. On POSIX, fsyncing a
        // file persists only its contents, NOT the parent directory's entries:
        // after a power loss the freshly created dirents (including TOC.txt) can be
        // missing even though the file contents reached the device, or (worse for
        // the crash-safety invariant) the TOC dirent could be durable while a
        // component's dirent is not. fsync the containing directory now — AFTER the
        // component contents and the TOC — so the directory entries for the whole
        // component set become durable, and only AFTER their contents. On Unix this
        // upholds the invariant "if TOC.txt is visible then every component it names
        // is present and complete" for any finalize that writes directly into the
        // final SSTable directory (flush, one-shot merge). The background
        // compaction path writes into a tmp directory and republishes via rename,
        // so it additionally fsyncs the *destination* directory (and its ancestor
        // chain) after the renames (see `finalize_merge_async`). The invariant is
        // Unix-scoped: directory fsync is a documented no-op on non-Unix (issue
        // #1392 / #1959), where the per-file fsyncs remain the durability guarantee
        // and the parent dirents are not separately persisted.
        crate::storage::write_engine::durability::sync_directory(sstable_dir)?;

        // Data.db bytes written by this SSTable writer (issue #1036). Counts
        // flush and compaction output alike; finalize sums all components.
        crate::observability::add_counter(
            crate::observability::catalog::WRITE_BYTES,
            data_size,
            &[],
        );

        Ok(SSTableInfo {
            data_path,
            index_path,
            filter_path,
            summary_path,
            stats_path,
            compression_info_path: None,
            partitions_path,
            rows_path,
            toc_path,
            digest_path,
            crc_path,
            partition_count: self.partition_count,
            data_size,
        })
    }

    /// Fsync a component file's *contents* to the storage device (issue #1392).
    ///
    /// Components written via `tokio::fs::write` / `std::fs::write` (Statistics,
    /// Summary, CRC, Rows, Partitions, and the empty-Data fallback) only reach
    /// the OS page cache; a crash after the WAL is truncated could lose their
    /// contents even though the directory entry was persisted. This reopens the
    /// finished file and calls `sync_all` so the bytes are durable before the
    /// caller (`SSTableWriter::finish`) returns — and therefore before the
    /// flush's directory fsync and WAL truncate run. (Data.db and Index.db are
    /// fsynced at their streaming-writer finish; Filter/Digest/TOC fsync inside
    /// their own writers — none are re-synced here.)
    pub(super) async fn fsync_component(path: &Path) -> Result<()> {
        // Reopen with write access, not read-only: `sync_all` on a read-only
        // handle can fail on Windows (issue #1392, FINDING 2). `read(true)` is
        // kept so behavior is identical on unix (the file is only fsynced, never
        // written through this handle).
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .await
            .map_err(|e| {
                crate::error::Error::Storage(format!(
                    "Failed to open {} for fsync: {e}",
                    path.display()
                ))
            })?;
        file.sync_all().await.map_err(|e| {
            crate::error::Error::Storage(format!("Failed to fsync {}: {e}", path.display()))
        })
    }

    /// Build a component file path for a given on-disk `format`.
    ///
    /// The Cassandra filename pattern is `<version>-<id>-<format>-<component>`
    /// (`SsTableDescriptor::parse`). BIG components use the `nb` version letter
    /// and the `big` format segment (`nb-<gen>-big-<component>`); BTI components
    /// use the `da` version letter and the `bti` format segment
    /// (`da-<gen>-bti-<component>`). This is the single source of truth for
    /// component naming so the version/format ordering stays consistent.
    pub(super) fn component_path_for(
        output_dir: &Path,
        generation: u64,
        format: SSTableFormat,
        component: &str,
    ) -> PathBuf {
        let (version, fmt) = match format {
            SSTableFormat::Big => ("nb", "big"),
            SSTableFormat::Bti => ("da", "bti"),
        };
        let filename = format!("{}-{}-{}-{}", version, generation, fmt, component);
        output_dir.join(filename)
    }

    /// Resolve the table's `bloom_filter_fp_chance` (Issue #852).
    ///
    /// The value is read from `schema.comments["bloom_filter_fp_chance"]` when
    /// present (the table-options bag carried on [`TableSchema`]). A value of
    /// exactly `1.0` disables the bloom filter (Cassandra's
    /// `AlwaysPresentFilter`); the [`FilterWriter`] then emits no `Filter.db`.
    ///
    /// When the schema does not carry a value, or it cannot be parsed, this
    /// falls back to Cassandra's default of `0.01`. Out-of-range values
    /// (outside `(0.0, 1.0]`) also fall back to the default so the writer never
    /// produces an invalid filter; `FilterWriter::new` still enforces the
    /// `(0.0, 1.0]` contract for explicitly supplied values.
    ///
    /// [`FilterWriter`]: super::FilterWriter
    pub(super) fn bloom_filter_fp_chance(schema: &TableSchema) -> f64 {
        const DEFAULT_FP_CHANCE: f64 = 0.01;
        match schema.comments.get("bloom_filter_fp_chance") {
            Some(raw) => match raw.trim().parse::<f64>() {
                Ok(v) if v > 0.0 && v <= 1.0 => v,
                _ => DEFAULT_FP_CHANCE,
            },
            None => DEFAULT_FP_CHANCE,
        }
    }

    /// Compute the CRC32 digest of a finished component by streaming it through
    /// a fixed-size buffer.
    ///
    /// Reads the file in 64 KiB pieces rather than slurping the whole component
    /// into one `Vec` (`tokio::fs::read`). For a multi-GB merge output the
    /// digest pass would otherwise allocate a buffer the size of the entire
    /// Data.db, making end-to-end compaction peak memory scale with the output
    /// size — defeating the bounded compaction-read work of issue #827. The
    /// CRC32 is order-sensitive but chunk-size-agnostic, so streaming yields the
    /// identical digest.
    pub(super) async fn compute_crc32(file_path: &PathBuf) -> Result<u32> {
        use tokio::io::AsyncReadExt;

        const DIGEST_READ_BUFFER_BYTES: usize = 64 * 1024;

        let mut file = tokio::fs::File::open(file_path).await?;
        // Full re-read of a finished component for checksums — count it so the
        // issue-#1663 work guard catches a regression that reintroduces the
        // finish-time Data.db re-read on the non-empty path.
        crate::storage::sstable::work_counters::add_data_db_checksum_full_read();
        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = vec![0u8; DIGEST_READ_BUFFER_BYTES];
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        Ok(hasher.finalize())
    }
}
