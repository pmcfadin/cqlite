//! SSTable writer components for producing Cassandra 5.0-compatible SSTables
//!
//! This module coordinates the generation of all SSTable components:
//! - Data.db: Row data with partition and clustering ordering
//! - Index.db: Partition index for fast lookups
//! - Filter.db: Bloom filter for existence checks
//! - Statistics.db: Metadata for delta encoding
//! - Summary.db: Sampled index entries
//! - CompressionInfo.db: Compression metadata (only when compressed)
//! - TOC.txt: Component manifest (publication barrier)
//!
//! Component generation order is critical (see M5 Council Recommendation):
//! 1. Statistics.db (provides delta encoding baseline)
//! 2. Data.db + Index.db (single pass, track offsets)
//! 3. Summary.db (sample Index.db entries)
//! 4. Filter.db (finalize Bloom filter)
//! 5. CompressionInfo.db (only when compressed)
//! 6. Digest.crc32
//! 7. TOC.txt (makes SSTable visible)
//!
//! TODO: Implementation in M5.0-7 through M5.0-13

// BIG vs BTI write-path split (issue #1128, epic #1116). `bti_state` holds the
// BTI (`da`) deferred-payload state + Rows.db/Partitions.db serialization;
// `finish` holds the format-aware finalization + component-path helpers. Both
// are private submodules that extend `SSTableWriter` via `impl` blocks; the
// public surface is re-exported from this `mod.rs` unchanged.
#[cfg(feature = "write-support")]
mod bti_state;
#[cfg(feature = "write-support")]
mod finish;

#[cfg(feature = "write-support")]
pub mod compressed_data_writer;
#[cfg(feature = "write-support")]
pub mod compression_info_writer;
#[cfg(feature = "write-support")]
pub mod data_writer;
#[cfg(feature = "write-support")]
pub mod digest_writer;
#[cfg(feature = "write-support")]
pub mod filter_writer;
#[cfg(feature = "write-support")]
pub mod index_writer;
#[cfg(feature = "write-support")]
pub mod partitions_writer;
#[cfg(feature = "write-support")]
pub mod stats_writer;
#[cfg(feature = "write-support")]
pub mod summary_writer;
#[cfg(feature = "write-support")]
pub mod toc_writer;

#[cfg(all(feature = "write-support", feature = "deflate"))]
pub use compressed_data_writer::DeflateCompressor;
#[cfg(all(feature = "write-support", feature = "lz4"))]
pub use compressed_data_writer::Lz4Compressor;
#[cfg(all(feature = "write-support", feature = "snappy"))]
pub use compressed_data_writer::SnappyCompressor;
#[cfg(all(feature = "write-support", feature = "zstd"))]
pub use compressed_data_writer::ZstdCompressor;
#[cfg(feature = "write-support")]
pub use compressed_data_writer::{
    create_compressor, CompressedDataWriter, Compressor, NoopCompressor,
};
#[cfg(feature = "write-support")]
pub use compression_info_writer::{
    CompressionAlgorithm, CompressionInfoWriter, CompressionMetadata,
};
#[cfg(feature = "write-support")]
pub use data_writer::DataWriter;
#[cfg(feature = "write-support")]
pub use digest_writer::DigestWriter;
#[cfg(feature = "write-support")]
pub use filter_writer::FilterWriter;
#[cfg(feature = "write-support")]
pub use index_writer::{
    IndexEntryInfo, IndexWriter, PromotedIndexBlock, COLUMN_INDEX_SIZE_BYTES, INDEX_INFO_WIDTH_BASE,
};
// Test-only oracle helper for the promoted-index reader round-trip (Issue #993):
// reachable in-crate without widening the published API.
#[cfg(feature = "write-support")]
pub(crate) use index_writer::serialize_promoted_index_for_test;
#[cfg(feature = "write-support")]
pub use stats_writer::{StatisticsMetadata, StatisticsWriter};
#[cfg(feature = "write-support")]
pub use summary_writer::SummaryWriter;
#[cfg(feature = "write-support")]
pub use toc_writer::{ComponentEntry, TocWriter};

use crate::error::{Error, Result};
use crate::schema::TableSchema;
#[cfg(feature = "write-support")]
use crate::schema::UdtRegistry;
use crate::storage::write_engine::mutation::{DecoratedKey, Mutation};
use std::path::PathBuf;

/// On-disk index format emitted by [`SSTableWriter`].
///
/// Issue #766 (epic #762, writer fidelity D4). The default is [`Big`], which
/// produces the legacy `Index.db`/`Summary.db` partition index and is byte-for-byte
/// unchanged from before this option existed. [`Bti`] additionally writes a BTI
/// `Partitions.db` trie (phase 1) so partition lookups can resolve `Data.db`
/// offsets via the trie reader.
///
/// [`Big`]: SSTableFormat::Big
/// [`Bti`]: SSTableFormat::Bti
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SSTableFormat {
    /// Legacy BIG format: `Index.db` + `Summary.db`. **Default.**
    #[default]
    Big,
    /// Cassandra-canonical BTI format (`da-<gen>-bti-*`).
    ///
    /// Issue #908 (epic #872). Emits a true BTI component set:
    /// - `Data.db` — the partition/row serialization is **identical** to BIG in
    ///   Cassandra 5 (the BTI format shares `BigTableWriter`'s row encoding; only
    ///   the descriptor/version and index components differ — see
    ///   `docs/sstables-definitive-guide/chapters/17-bti-formats.md`, which lists
    ///   `Data.db` as a *common component retained*). The bytes are therefore the
    ///   same as BIG; only the filename descriptor changes.
    /// - `Partitions.db` — partition trie (replaces `Index.db`/`Summary.db`).
    /// - `Rows.db` — within-partition row-index trie (issue #910). WIDE
    ///   partitions (`>= 2` × 64 KiB column-index blocks) get a per-partition
    ///   row index and a positive `RowsOffset` in their partition-trie leaf;
    ///   NARROW partitions keep a direct (negative) `Data.db` offset. `Rows.db`
    ///   is always emitted for a BTI SSTable — possibly 0 bytes when no partition
    ///   is wide, matching the real `da` fixtures.
    /// - `Statistics.db`, `Filter.db`, `Digest.crc32`, `TOC.txt`.
    ///
    /// **No `Index.db` and no `Summary.db`** — those are BIG-only components; BTI
    /// resolves partitions through the trie. All components use the `da` version
    /// letter and `bti` format segment.
    ///
    /// An **empty** BTI SSTable (zero partitions) is refused by `finish()`: a
    /// `da` SSTable requires a readable `Partitions.db` (8-byte root footer),
    /// which has no valid zero-partition form.
    Bti,
}

/// Information about a written SSTable
///
/// Returned by `SSTableWriter::finish()` after successfully writing all components.
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    /// Path to the Data.db file
    pub data_path: PathBuf,
    /// Path to the Index.db file.
    ///
    /// `Some` for the BIG format; `None` for [`SSTableFormat::Bti`], which has
    /// no `Index.db` (partition lookups use the `Partitions.db` trie). Issue #908.
    pub index_path: Option<PathBuf>,
    /// Path to the Filter.db file. `None` when the table disables its bloom
    /// filter (`bloom_filter_fp_chance = 1.0`, Cassandra's AlwaysPresentFilter),
    /// in which case NO Filter.db component is emitted (Issue #852). Downstream
    /// consumers (e.g. compaction publish/byte-accounting) must skip the filter
    /// when this is `None`.
    pub filter_path: Option<PathBuf>,
    /// Path to the Summary.db file.
    ///
    /// `Some` for the BIG format; `None` for [`SSTableFormat::Bti`], which has
    /// no `Summary.db`. Issue #908.
    pub summary_path: Option<PathBuf>,
    /// Path to the Statistics.db file
    pub stats_path: PathBuf,
    /// Path to the CompressionInfo.db file (None when data is uncompressed)
    pub compression_info_path: Option<PathBuf>,
    /// Path to the BTI `Partitions.db` trie (Some only for [`SSTableFormat::Bti`];
    /// None for the default BIG format). Issue #766.
    pub partitions_path: Option<PathBuf>,
    /// Path to the BTI `Rows.db` within-partition row-index trie (Some only for
    /// [`SSTableFormat::Bti`]; None for BIG). Always present for a non-empty BTI
    /// SSTable, even when 0 bytes (no wide partitions). Issue #910.
    pub rows_path: Option<PathBuf>,
    /// Path to the TOC.txt file
    pub toc_path: PathBuf,
    /// Path to the Digest.crc32 file
    pub digest_path: PathBuf,
    /// Number of partitions written
    pub partition_count: usize,
    /// Total size of Data.db file in bytes
    pub data_size: u64,
}

/// SSTable writer coordinator
///
/// Orchestrates the generation of all SSTable components in the correct order.
/// Produces valid Cassandra 5.0 BIG format SSTables.
///
/// # Write Order
///
/// Components are written in the following critical order:
/// 1. Statistics.db - Provides delta encoding baseline (FIRST)
/// 2. Data.db - Main partition/row data
/// 3. Index.db - Partition index (uses Data.db offsets)
/// 4. Filter.db - Bloom filter
/// 5. Summary.db - Sampled index entries
/// 6. CompressionInfo.db - Compression metadata (only when compressed)
/// 7. Digest.crc32 - Data.db checksum
/// 8. TOC.txt - Table of contents (LAST, publication barrier)
///
/// # File Naming
///
/// All components follow the pattern: `nb-{generation}-big-{Component}.db`
/// Example: `nb-1-big-Data.db`, `nb-1-big-Index.db`
///
/// # Partition Ordering
///
/// Partitions MUST be written in Murmur3 token order (caller responsibility).
/// The writer validates token ordering on each `write_partition()` call.
///
/// # Example
///
/// ```rust,ignore
/// use cqlite_core::storage::sstable::writer::SSTableWriter;
/// use cqlite_core::storage::write_engine::mutation::{Mutation, DecoratedKey};
/// use cqlite_core::schema::TableSchema;
///
/// // Create schema
/// let schema = TableSchema::from_json("...")?;
///
/// // Create writer
/// let mut writer = SSTableWriter::new(
///     PathBuf::from("data/ks/table"),
///     1,  // generation
///     &schema
/// )?;
///
/// // Write partitions (MUST be in token order)
/// let key = DecoratedKey::new(token, key_bytes);
/// let mutations = vec![/* ... */];
/// writer.write_partition(key, mutations)?;
///
/// // Finish writing
/// let info = writer.finish().await?;
/// println!("Wrote SSTable with {} partitions", info.partition_count);
/// ```
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub struct SSTableWriter {
    /// SSTable output directory: output_dir/keyspace/table/
    sstable_dir: PathBuf,
    /// SSTable generation number
    generation: u64,
    /// Table schema for column metadata
    schema: TableSchema,
    /// Statistics metadata (collected during writes)
    stats: StatisticsMetadata,
    /// Data.db writer
    data_writer: DataWriter,
    /// Index.db writer
    index_writer: IndexWriter,
    /// Filter.db writer
    filter_writer: Option<FilterWriter>,
    /// Summary.db writer
    summary_writer: SummaryWriter,
    /// Last token written (for ordering validation)
    last_token: Option<i64>,
    /// Number of partitions written
    partition_count: usize,
    /// Summary sampling counter (sample every N entries)
    summary_sample_counter: usize,
    /// Sampling interval for Summary.db (default: 128)
    summary_sample_interval: usize,
    /// Whether encoding baselines have been pre-seeded via `pre_seed_encoding_baselines`.
    ///
    /// When `true`, `write_partition` skips the incremental `data_writer.update_stats()`
    /// call so the pre-computed final baselines are not overwritten by an intermediate
    /// (and potentially higher) baseline from an earlier partition.
    ///
    /// Issue #729: two-pass flush baseline fix.
    baselines_locked: bool,
    /// Index format to emit (issue #766). Defaults to [`SSTableFormat::Big`].
    format: SSTableFormat,
    /// BTI partition trie accumulator. Populated only when `format` is
    /// [`SSTableFormat::Bti`]; `None` otherwise so the BIG path allocates nothing.
    partitions_trie: Option<partitions_writer::PartitionsTrieWriter>,
    /// BTI per-partition pending payloads (issue #910). Populated only for
    /// [`SSTableFormat::Bti`]. The partition-trie leaf payload (direct
    /// `Data.db` offset vs `Rows.db` `RowsOffset`) cannot be finalized until
    /// `Rows.db` is serialized in [`Self::finish`] (the `RowsOffset` is the
    /// `TrieIndexEntry` position), so we defer the decision: each entry records
    /// the partition's raw key, its `Data.db` offset, and — for WIDE partitions
    /// (>= 2 column-index blocks) — the row-index blocks. `None` for BIG so that
    /// path allocates nothing. See [`bti_state::PendingBtiPartition`].
    bti_pending: Option<Vec<bti_state::PendingBtiPartition>>,
}

#[cfg(feature = "write-support")]
impl SSTableWriter {
    /// Create a new SSTable writer
    ///
    /// # Arguments
    ///
    /// * `output_dir` - Directory where SSTable files will be written
    /// * `generation` - SSTable generation number (e.g., 1, 2, 3...)
    /// * `schema` - Table schema for column metadata
    ///
    /// # Returns
    ///
    /// A new SSTableWriter ready to accept partitions.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let writer = SSTableWriter::new(
    ///     PathBuf::from("data/test_ks/users"),
    ///     1,
    ///     &schema
    /// )?;
    /// ```
    pub fn new(output_dir: PathBuf, generation: u64, schema: &TableSchema) -> Result<Self> {
        Self::with_expected_partitions(output_dir, generation, schema, 128)
    }

    /// Create a new SSTable writer with an expected partition count hint
    ///
    /// The expected count is used to size the Bloom filter optimally.
    pub fn with_expected_partitions(
        output_dir: PathBuf,
        generation: u64,
        schema: &TableSchema,
        expected_partitions: usize,
    ) -> Result<Self> {
        Self::with_format(
            output_dir,
            generation,
            schema,
            expected_partitions,
            SSTableFormat::default(),
        )
    }

    /// Create a new SSTable writer selecting the on-disk index `format`
    /// (issue #766).
    ///
    /// [`SSTableFormat::Big`] (the default used by [`Self::new`] and
    /// [`Self::with_expected_partitions`]) is byte-for-byte unchanged.
    /// [`SSTableFormat::Bti`] additionally emits a `Partitions.db` trie at
    /// `finish()`.
    pub fn with_format(
        output_dir: PathBuf,
        generation: u64,
        schema: &TableSchema,
        expected_partitions: usize,
        format: SSTableFormat,
    ) -> Result<Self> {
        Self::with_format_and_registry(
            output_dir,
            generation,
            schema,
            expected_partitions,
            format,
            None,
        )
    }

    /// Create a new SSTable writer with an expected partition count hint and an
    /// optional [`UdtRegistry`] for resolving bare UDT column types (issue #929).
    ///
    /// When `registry` is `Some`, any column whose `data_type` is a TOP-LEVEL
    /// bare CQL UDT name (e.g. `person`) that resolves in the registry is
    /// rewritten to its full `UserType(...)` marshal string so the existing
    /// complex-cell decomposition path writes per-field cells. With `None`, or
    /// for an unregistered name, behavior is unchanged (single simple cell).
    pub fn with_expected_partitions_and_registry(
        output_dir: PathBuf,
        generation: u64,
        schema: &TableSchema,
        expected_partitions: usize,
        registry: Option<&UdtRegistry>,
    ) -> Result<Self> {
        Self::with_format_and_registry(
            output_dir,
            generation,
            schema,
            expected_partitions,
            SSTableFormat::default(),
            registry,
        )
    }

    /// Create a new SSTable writer selecting the on-disk index `format` and an
    /// optional [`UdtRegistry`] for bare-UDT resolution (issue #766 + #929).
    pub fn with_format_and_registry(
        output_dir: PathBuf,
        generation: u64,
        schema: &TableSchema,
        expected_partitions: usize,
        format: SSTableFormat,
        registry: Option<&UdtRegistry>,
    ) -> Result<Self> {
        // Issue #929: resolve TOP-LEVEL bare UDT column names to their marshal
        // form on the schema copy the writer holds, so the existing complex-cell
        // path treats them as multi-cell UDTs. No registry => no rewrite.
        let mut schema = schema.clone();
        if let Some(registry) = registry {
            crate::storage::sstable::writer::data_writer::normalize_schema_udts(
                &mut schema,
                registry,
            );
        }
        let schema = &schema;

        // Initialize statistics metadata with sentinel values
        let mut stats = StatisticsMetadata::new();
        // Pre-set min values to reasonable defaults (will be updated during writes)
        stats.min_timestamp = i64::MAX;
        stats.min_ttl = i32::MAX;
        stats.min_local_deletion_time = i32::MAX;

        // Compute the SSTable output directory: output_dir/keyspace/table/
        // This ensures the reader's extract_table_name() can map files to table names.
        let sstable_dir = output_dir.join(&schema.keyspace).join(&schema.table);

        // Create Data.db writer in streaming mode (Issue #492): each partition is
        // flushed to the Data.db file as it is written, bounding peak memory to a
        // single partition instead of buffering the whole component. The file is
        // opened lazily on the first partition (creating sstable_dir as needed).
        let data_path = Self::component_path_for(&sstable_dir, generation, format, "Data.db");
        let data_writer = DataWriter::with_sink(stats.clone(), data_path);

        // Index.db writer.
        //
        // BIG (Issue #753): streaming mode flushes each entry straight to
        // `Index.db` as it arrives, keeping only the current entry's bytes in
        // memory (O(1) in partition count).
        //
        // BTI (Issue #908): the BTI format has no `Index.db` — partition lookups
        // go through the `Partitions.db` trie instead. We use a *counting-only*
        // `IndexWriter` (no sink, so no file is created) purely to compute the
        // per-partition index offsets and promoted-block bookkeeping reused by
        // the write path. Counting mode serializes each entry into a scratch buffer
        // only to measure it, then clears the scratch, so peak memory stays
        // O(one entry). (The prior in-memory mode retained every serialized entry
        // forever — a full, never-emitted in-memory `Index.db` that defeated the
        // streaming memory budget on large BTI writes.) Nothing is persisted and no
        // `Index.db` is emitted.
        let index_writer = match format {
            SSTableFormat::Big => {
                let index_path =
                    Self::component_path_for(&sstable_dir, generation, format, "Index.db");
                IndexWriter::with_sink(index_path)
            }
            SSTableFormat::Bti => IndexWriter::counting(),
        };

        // Create Filter.db writer. The false-positive chance comes from the
        // table's `bloom_filter_fp_chance` (Issue #852): thread the schema's
        // actual value through instead of hardcoding 0.01. A value of exactly
        // 1.0 disables the filter (Cassandra's AlwaysPresentFilter) and the
        // FilterWriter then emits no Filter.db component; the default when the
        // schema does not specify one remains Cassandra's 0.01.
        let filter_path = Self::component_path_for(&sstable_dir, generation, format, "Filter.db");
        let fp_chance = Self::bloom_filter_fp_chance(schema);
        let filter_writer = Some(FilterWriter::new(
            filter_path,
            expected_partitions.max(1),
            fp_chance,
        )?);

        // Create Summary.db writer (sample every 128 entries per Cassandra default)
        let summary_sample_interval = 128;
        let summary_writer = SummaryWriter::new(summary_sample_interval as u32);

        // BTI phase 1 (issue #766): only allocate the partition-trie accumulator
        // when the BTI format is selected, so the default BIG path is unchanged.
        let partitions_trie = match format {
            SSTableFormat::Big => None,
            SSTableFormat::Bti => Some(partitions_writer::PartitionsTrieWriter::new()),
        };
        // BTI (issue #910): defer partition-trie payloads until Rows.db is built.
        let bti_pending = match format {
            SSTableFormat::Big => None,
            SSTableFormat::Bti => Some(Vec::new()),
        };

        Ok(Self {
            sstable_dir,
            generation,
            schema: schema.clone(),
            stats,
            data_writer,
            index_writer,
            filter_writer,
            summary_writer,
            last_token: None,
            partition_count: 0,
            summary_sample_counter: 0,
            summary_sample_interval,
            baselines_locked: false,
            format,
            partitions_trie,
            bti_pending,
        })
    }

    /// The on-disk index format this writer emits (issue #766).
    pub fn format(&self) -> SSTableFormat {
        self.format
    }

    /// Write a partition (partition key + all mutations)
    ///
    /// # Arguments
    ///
    /// * `key` - DecoratedKey (token + raw partition key bytes)
    /// * `mutations` - All mutations for this partition (must be in clustering order)
    ///
    /// # Returns
    ///
    /// Ok(()) on success, or an error if:
    /// - Partitions are not in token order
    /// - Schema validation fails
    /// - I/O error occurs
    ///
    /// # Ordering Requirement
    ///
    /// Partitions MUST be written in ascending token order. This method validates
    /// ordering and returns an error if violated.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x01, 0x02]);
    /// let mutations = vec![
    ///     Mutation::new(/* ... */)
    /// ];
    /// writer.write_partition(key, mutations)?;
    /// ```
    #[tracing::instrument(name = "writer.write_partition", skip(self, key, mutations))]
    pub fn write_partition(&mut self, key: DecoratedKey, mutations: Vec<Mutation>) -> Result<()> {
        // Validate token ordering
        if let Some(last_token) = self.last_token {
            if key.token <= last_token {
                return Err(Error::InvalidInput(format!(
                    "Partitions must be written in token order: got token {} after {}",
                    key.token, last_token
                )));
            }
        }
        self.last_token = Some(key.token);

        // Record the SSTable key range (lowest = first seen, highest = last) for
        // the `da`-format StatsMetadata `hasKeyRange` fields. Partitions arrive in
        // ascending token order (validated above), so first/last fall out for
        // free. Harmless for BIG (the legacy STATS body ignores these fields).
        self.stats.update_key_range(&key.key);

        // Sort mutations by clustering key (Cassandra requires sorted rows within partitions)
        let mut mutations = mutations;
        mutations.sort_by(|a, b| match (&a.clustering_key, &b.clustering_key) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(ck_a), Some(ck_b)) => ck_a
                .compare(ck_b, &self.schema)
                .unwrap_or_else(|_| ck_a.cmp(ck_b)),
        });

        // Update statistics from mutations
        for mutation in &mutations {
            self.stats.update_timestamp(mutation.timestamp_micros);
            if let Some(ttl) = mutation.ttl_seconds {
                self.stats.update_ttl(ttl as i32);
                let now_seconds = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i32)
                    .unwrap_or(0);
                let local_deletion_time = now_seconds.saturating_add(ttl as i32);
                self.stats.update_local_deletion_time(local_deletion_time);
            }
            // Track local deletion times for tombstones and TTL cells.
            // Issue #764: row/cell tombstones use the caller-supplied
            // `local_deletion_time` when present, else the timestamp-derived
            // value (`effective_local_deletion_time`).
            for op in &mutation.operations {
                match op {
                    crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                        ttl_seconds,
                        ..
                    } => {
                        // Track TTL
                        self.stats.update_ttl(*ttl_seconds as i32);
                        // CRITICAL: TTL cells need local_deletion_time tracked
                        // local_deletion_time = now + ttl
                        let now_seconds = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i32)
                            .unwrap_or(0);
                        let local_deletion_time = now_seconds.saturating_add(*ttl_seconds as i32);
                        self.stats.update_local_deletion_time(local_deletion_time);
                    }
                    op @ (crate::storage::write_engine::mutation::CellOperation::Delete { .. }
                    | crate::storage::write_engine::mutation::CellOperation::DeleteRow) => {
                        // Issue #764 / #921 finding 2: record the EXACT LDT the
                        // tombstone is emitted with. A `Delete` carrying a per-cell
                        // `local_deletion_time: Some(L)` is stamped with `L`
                        // verbatim by DataWriter; everything else derives the LDT
                        // from the mutation. Reuse `op_cell_local_deletion_time`
                        // (the emit path's helper) so stats and the bytes written
                        // to Data.db agree exactly — a per-cell `L` below the
                        // mutation-derived value would otherwise underflow the
                        // delta, and one above it would leave min/max wrong.
                        let local_deletion_time =
                            crate::storage::sstable::writer::data_writer::op_cell_local_deletion_time(
                                op, mutation,
                            );
                        self.stats.update_local_deletion_time(local_deletion_time);
                    }
                    // Issue #887: a `ComplexDeletion` marker is physically written
                    // with its OWN `marked_for_delete_at` / `local_deletion_time`
                    // (DataWriter delta-encodes both against `min_timestamp` /
                    // `min_local_deletion_time`). The mutation's row timestamp does
                    // NOT cover the marker — for a tombstone-only row whose marker
                    // strictly supersedes the row tombstone, `marked_for_delete_at`
                    // can lie ABOVE the row timestamp and the marker LDT below the
                    // row's LDT. Fold both into the stats so `max_timestamp` /
                    // `min_local_deletion_time` reflect what was actually emitted to
                    // Data.db (LIVE sentinels are filtered inside the `update_*`
                    // chokepoints, issue #851).
                    crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                        marked_for_delete_at,
                        local_deletion_time,
                        ..
                    } => {
                        self.stats.update_timestamp(*marked_for_delete_at);
                        self.stats.update_local_deletion_time(*local_deletion_time);
                    }
                    // Issue #887: a per-element complex cell carries its OWN explicit
                    // timestamp/ttl/local_deletion_time (DataWriter delta-encodes each
                    // against the SSTable baselines). The element timestamp can differ
                    // from the row liveness timestamp, and a deleted/expiring element
                    // supplies an LDT/TTL the row-level accumulation never sees. Fold
                    // them all in so the baselines cover every byte emitted by
                    // `write_complex_element_cell`.
                    crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                        timestamp_micros,
                        ttl_seconds,
                        local_deletion_time,
                        ..
                    } => {
                        self.stats.update_timestamp(*timestamp_micros);
                        if let Some(ttl) = ttl_seconds {
                            self.stats.update_ttl(*ttl as i32);
                        }
                        if let Some(ldt) = local_deletion_time {
                            self.stats.update_local_deletion_time(*ldt);
                        }
                    }
                    crate::storage::write_engine::mutation::CellOperation::Write { .. } => {}
                }
            }
            // Track stats for partition tombstones
            if let Some(pt) = &mutation.partition_tombstone {
                self.stats.update_timestamp(pt.deletion_time);
                self.stats
                    .update_local_deletion_time(pt.local_deletion_time);
                // Record the partition-level deletion marker for the `da`-format
                // StatsMetadata.hasPartitionLevelDeletions field. Authoritative:
                // the mutation explicitly carries a partition tombstone.
                self.stats.mark_partition_level_deletion();
            }

            // Track stats for range tombstones
            for rt in &mutation.range_tombstones {
                self.stats.update_timestamp(rt.deletion_time);
                self.stats
                    .update_local_deletion_time(rt.local_deletion_time);
            }

            // Issue #851: row_count (totalRows) and column_count
            // (totalColumnsSet) are NOT re-derived per-mutation here. The two
            // previous attempts re-grouped rows in this loop and kept diverging
            // from `DataWriter::merge_row_group`, which is what actually emits
            // rows/cells to Data.db (e.g. it drops partition/clustering-key
            // columns from cells, and merges static ops from ALL mutations into
            // a single static prelude that is a SEPARATE row from the clustering
            // row). Instead, the emitter returns `PartitionEmitCounts` and we add
            // them below (see the `write_partition_with_index_blocks` call) so
            // the stats can never drift from what was physically written.
        }

        // Update DataWriter's stats before writing, unless baselines were
        // pre-seeded for the whole SSTable (issue #729 two-pass flush).
        // When baselines are locked, the DataWriter already holds the final
        // minimum values computed over ALL partitions; overwriting them with
        // the incrementally-growing stats of this partition would raise the
        // baseline and corrupt delta encoding for earlier partitions.
        if !self.baselines_locked {
            self.data_writer.update_stats(self.stats.clone());
        }

        // Extract partition tombstone and range tombstones from mutations.
        // The tombstone can arrive on ANY mutation of the partition (a DELETE
        // typically follows earlier INSERTs), so scan all of them and keep the
        // newest deletion (Issue #716: taking only the first mutation dropped
        // the tombstone and left the partition header LIVE).
        let partition_tombstone = mutations
            .iter()
            .filter_map(|m| m.partition_tombstone.as_ref())
            .max_by_key(|pt| pt.deletion_time);

        // Collect all range tombstones from mutations
        let range_tombstones: Vec<_> = mutations
            .iter()
            .flat_map(|m| m.range_tombstones.iter())
            .cloned()
            .collect();

        // Write partition to Data.db, collecting promoted index blocks for wide partitions.
        // Wide partitions (≥ 64 KiB of row data) get a non-zero promoted index so Cassandra
        // can seek directly to a clustering-key range without reading the full partition.
        let (data_offset, promoted_blocks, emit_counts) =
            self.data_writer.write_partition_with_index_blocks(
                &key,
                &mutations,
                &self.schema,
                partition_tombstone,
                &range_tombstones,
            )?;

        // Issue #851: Statistics' totalRows / totalColumnsSet are fed directly
        // from what `DataWriter` physically emitted (the single source of truth),
        // so they cannot drift from Data.db. The empty static-row prelude and
        // range tombstone markers are already excluded by the emitter, matching
        // Cassandra `Row.isEmpty()` / `Row.columnCount()`.
        self.stats.row_count += emit_counts.rows;
        self.stats.column_count += emit_counts.columns;

        // Add partition to Index.db and get entry info.
        // Pass promoted blocks (writer gates on >= 2 blocks before emitting payload).
        // IMPORTANT: Capture index_offset AFTER the entry is written to Index.db.
        let entry_info =
            self.index_writer
                .add_partition_with_promoted(&key, data_offset, &promoted_blocks)?;

        // Add partition key to Filter.db
        if let Some(ref mut filter) = self.filter_writer {
            filter.add_key(&key);
        }

        // BTI (issue #766 / #910): defer this partition's Partitions.db trie
        // payload (a no-op for BIG). The wide/narrow gate, OSS50-separator
        // construction, and the deferred-finalization rationale live in
        // [`Self::queue_bti_partition`] (see `bti_state`).
        self.queue_bti_partition(&key, data_offset, &promoted_blocks, partition_tombstone);

        // Track every partition for first_key / last_key / total_partition_count.
        // These fields must cover the full SSTable, not just sampled entries.
        // (Issue #666: first/last keys in Summary.db must span the whole SSTable
        //  so Cassandra's range queries cover all partitions.)
        self.summary_writer.note_partition(&key);

        // Sample for Summary.db (every Nth entry, where N = summary_sample_interval)
        // CRITICAL: Use the actual index_offset from entry_info, not an estimate
        if self.summary_sample_counter % self.summary_sample_interval == 0 {
            self.summary_writer
                .add_entry(&key, entry_info.index_offset)?;
        }

        self.summary_sample_counter += 1;
        self.partition_count += 1;
        self.stats.increment_partition_count();

        // Partitions-written counter (issue #1036). One per successful partition.
        crate::observability::add_counter(crate::observability::catalog::WRITE_PARTITIONS, 1, &[]);

        Ok(())
    }

    /// Pre-seed the encoding baselines with pre-computed final values.
    ///
    /// Call this BEFORE any `write_partition` call with the minimum values
    /// computed over ALL partitions that will be written. This ensures that
    /// delta encoding in Data.db uses the same baselines that will be written
    /// to Statistics.db, preventing silently corrupted values on read.
    ///
    /// Two-pass flush (issue #729): caller iterates all partitions once to
    /// find final mins, then calls this, then iterates again calling
    /// `write_partition`.
    pub fn pre_seed_encoding_baselines(
        &mut self,
        min_timestamp: i64,
        min_local_deletion_time: i32,
        min_ttl: i32,
    ) {
        self.stats.min_timestamp = min_timestamp;
        self.stats.min_local_deletion_time = min_local_deletion_time;
        self.stats.min_ttl = min_ttl;
        // Push final baselines to DataWriter immediately so the very first
        // write_partition call uses them.
        self.data_writer.update_stats(self.stats.clone());
        // Lock baselines: write_partition will not call update_stats again.
        self.baselines_locked = true;
    }

    /// Compute the encoding baseline stats (min values only) from a slice of mutations.
    ///
    /// Used for the pre-pass before `write_partition` is called (issue #729
    /// two-pass flush).  Returns `(min_timestamp, min_local_deletion_time, min_ttl)`.
    /// Sentinel value `i64::MAX` / `i32::MAX` is returned for each field when no
    /// relevant data is found in the slice (caller should handle via `.min()`
    /// accumulation and then pass the final result to `pre_seed_encoding_baselines`).
    pub fn compute_mutations_baseline_stats(mutations_slice: &[Mutation]) -> (i64, i32, i32) {
        let mut min_timestamp = i64::MAX;
        let mut min_ldt = i32::MAX;
        let mut min_ttl = i32::MAX;

        for mutation in mutations_slice {
            min_timestamp = min_timestamp.min(mutation.timestamp_micros);

            for op in &mutation.operations {
                match op {
                    crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                        ttl_seconds,
                        ..
                    } => {
                        let ttl = *ttl_seconds as i32;
                        if ttl > 0 {
                            min_ttl = min_ttl.min(ttl);
                            let now_seconds = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs() as i32)
                                .unwrap_or(0);
                            let ldt = now_seconds.saturating_add(ttl);
                            min_ldt = min_ldt.min(ldt);
                        }
                    }
                    op @ (crate::storage::write_engine::mutation::CellOperation::Delete { .. }
                    | crate::storage::write_engine::mutation::CellOperation::DeleteRow) => {
                        // Issue #764 / #921 finding 2: the encoding baseline must
                        // match the LDT the row/cell tombstone will ACTUALLY be
                        // written with, else the delta underflows. A `Delete` with
                        // a per-cell `local_deletion_time: Some(L)` is emitted with
                        // `L` verbatim; reuse the emit path's
                        // `op_cell_local_deletion_time` helper so the pre-seeded
                        // baseline always covers the smallest LDT actually written.
                        let ldt =
                            crate::storage::sstable::writer::data_writer::op_cell_local_deletion_time(
                                op, mutation,
                            );
                        min_ldt = min_ldt.min(ldt);
                    }
                    // Issue #887: the pre-seeded baseline path must fold the SAME
                    // marker timestamps/LDTs the DataWriter delta-encodes (it
                    // subtracts `min_timestamp` / `min_local_deletion_time` from
                    // `marked_for_delete_at` and the element LDT). A
                    // `ComplexDeletion`/`WriteComplexElement` carrying a timestamp or
                    // LDT BELOW the mutation's own values would make the delta
                    // underflow when baselines are locked — exactly what #729's
                    // two-pass flush is meant to prevent. Mirror the non-pre-seeded
                    // accumulation in `write_partition`.
                    crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                        marked_for_delete_at,
                        local_deletion_time,
                        ..
                    } => {
                        // Exclude LIVE / NO_DELETION sentinels exactly as the
                        // `update_*` chokepoints do (issue #851), so a sentinel
                        // marker cannot drag the min baselines to `i64::MIN` /
                        // `i32::MAX`.
                        if *marked_for_delete_at != i64::MIN && *marked_for_delete_at != i64::MAX {
                            min_timestamp = min_timestamp.min(*marked_for_delete_at);
                        }
                        if *local_deletion_time != i32::MAX {
                            min_ldt = min_ldt.min(*local_deletion_time);
                        }
                    }
                    crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                        timestamp_micros,
                        ttl_seconds,
                        local_deletion_time,
                        ..
                    } => {
                        if *timestamp_micros != i64::MIN && *timestamp_micros != i64::MAX {
                            min_timestamp = min_timestamp.min(*timestamp_micros);
                        }
                        if let Some(ttl) = ttl_seconds {
                            let ttl = *ttl as i32;
                            if ttl > 0 {
                                min_ttl = min_ttl.min(ttl);
                            }
                        }
                        if let Some(ldt) = local_deletion_time {
                            if *ldt != i32::MAX {
                                min_ldt = min_ldt.min(*ldt);
                            }
                        }
                    }
                    crate::storage::write_engine::mutation::CellOperation::Write { .. } => {}
                }
            }

            // Partition-level TTL (top-level ttl_seconds on the Mutation)
            if let Some(ttl) = mutation.ttl_seconds {
                let ttl = ttl as i32;
                if ttl > 0 {
                    min_ttl = min_ttl.min(ttl);
                    let now_seconds = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i32)
                        .unwrap_or(0);
                    let ldt = now_seconds.saturating_add(ttl);
                    min_ldt = min_ldt.min(ldt);
                }
            }

            if let Some(pt) = &mutation.partition_tombstone {
                min_timestamp = min_timestamp.min(pt.deletion_time);
                min_ldt = min_ldt.min(pt.local_deletion_time);
            }

            for rt in &mutation.range_tombstones {
                min_timestamp = min_timestamp.min(rt.deletion_time);
                min_ldt = min_ldt.min(rt.local_deletion_time);
            }
        }

        (min_timestamp, min_ldt, min_ttl)
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, Column, KeyColumn};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, PartitionKey, TableId,
    };
    use crate::types::Value;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn create_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Issue #929: a bare-name non-frozen UDT column, after registry-backed
    /// normalization, must be advertised in the Statistics.db SERIALIZATION_HEADER
    /// as the full `UserType(...)` marshal — NOT `BytesType`. Otherwise Data.db
    /// would carry complex UDT cells while the header claims a blob, producing an
    /// inconsistent SSTable (roborev #999).
    #[tokio::test]
    async fn bare_udt_column_emits_usertype_in_serialization_header() {
        use crate::schema::{CqlType, UdtRegistry};
        use crate::types::{UdtField, UdtTypeDef, UdtValue};

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                // Declared with the BARE UDT name `person`.
                Column {
                    name: "addr".to_string(),
                    data_type: "person".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let mut registry = UdtRegistry::new();
        registry.register_udt(
            UdtTypeDef::new("test_ks".to_string(), "person".to_string())
                .with_field("name".to_string(), CqlType::Text, true)
                .with_field("age".to_string(), CqlType::Int, true),
        );

        let temp_dir = TempDir::new().unwrap();
        let mut writer = SSTableWriter::with_expected_partitions_and_registry(
            temp_dir.path().to_path_buf(),
            1,
            &schema,
            1,
            Some(&registry),
        )
        .unwrap();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let udt = Value::Udt(UdtValue {
            type_name: "person".to_string(),
            keyspace: "test_ks".to_string(),
            fields: vec![
                UdtField {
                    name: "name".to_string(),
                    value: Some(Value::Text("Alice".to_string())),
                },
                UdtField {
                    name: "age".to_string(),
                    value: Some(Value::Integer(30)),
                },
            ],
        });
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "addr".to_string(),
                value: udt,
            }],
            1_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();
        let info = writer.finish().await.unwrap();

        let stats_bytes = std::fs::read(&info.stats_path).unwrap();
        let expected = "org.apache.cassandra.db.marshal.UserType(test_ks,706572736f6e,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type)";
        let contains = stats_bytes
            .windows(expected.len())
            .any(|w| w == expected.as_bytes());
        assert!(
            contains,
            "serialization header must advertise the bare UDT column as UserType(...)"
        );
    }

    /// A schema with a single static column (and a clustering key, so static
    /// columns are meaningful). Used to exercise the empty static-row prelude.
    fn create_static_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_static".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "s".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// A clustered schema with NO static columns: partition `id`, clustering
    /// `ck`, regular `name`. Used to verify that a write whose only op targets a
    /// clustering-key column produces a live row with ZERO regular cells (#851
    /// review finding #1: `DataWriter::merge_row_group` drops clustering-key
    /// columns from the emitted cells, but the row stays live).
    fn create_clustered_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_clustered".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// A clustered schema with BOTH a static column `s` and a regular column
    /// `name`. Used to verify that a single clustered mutation carrying a static
    /// write AND a regular write emits TWO rows: the static prelude plus the
    /// clustering row (#851 review finding #2).
    fn create_static_and_regular_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_static_regular".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "s".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// A flat schema with TWO regular columns (`name`, `age`) and no clustering
    /// or static columns. Used to verify that an INSERT mixing a non-null write
    /// with a null-valued write counts only the cell that is physically
    /// serialized (#851 review): `write_merged_cells` skips the null write.
    fn create_two_regular_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_two_regular".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "age".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn create_test_mutation(
        keyspace: &str,
        table: &str,
        partition_id: i32,
        name: &str,
        timestamp: i64,
    ) -> Mutation {
        let table_id = TableId::new(keyspace, table);
        let pk = PartitionKey::single("id", Value::Integer(partition_id));

        Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text(name.to_string()),
            }],
            timestamp,
            None,
        )
    }

    #[tokio::test]
    async fn test_sstable_writer_single_partition() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Create a partition
        let mutation = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
        let key = mutation.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![mutation]).unwrap();

        let info = writer.finish().await.unwrap();

        // Verify all files were created (BIG format: Index.db + Summary.db present)
        assert!(info.data_path.exists());
        assert!(info.index_path.as_ref().expect("BIG has Index.db").exists());
        assert!(info.filter_path.as_ref().is_some_and(|p| p.exists()));
        assert!(info
            .summary_path
            .as_ref()
            .expect("BIG has Summary.db")
            .exists());
        assert!(info.stats_path.exists());
        assert!(info.compression_info_path.is_none());
        assert!(info.toc_path.exists());
        assert!(info.digest_path.exists());

        // Verify metadata
        assert_eq!(info.partition_count, 1);
        assert!(info.data_size > 0);

        // Verify file naming convention
        assert!(info
            .data_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("nb-1-big-Data.db"));
    }

    /// Issue #852: a table with `bloom_filter_fp_chance = 1.0` disables the
    /// bloom filter (Cassandra's AlwaysPresentFilter). The writer must not
    /// panic, must emit NO Filter.db component, and must omit Filter from TOC.txt
    /// — while every other component is still written normally.
    #[tokio::test]
    async fn test_sstable_writer_disabled_bloom_filter() {
        let temp_dir = TempDir::new().unwrap();
        let mut schema = create_test_schema();
        schema
            .comments
            .insert("bloom_filter_fp_chance".to_string(), "1.0".to_string());

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let mutation = create_test_mutation("test_ks", "test_table", 1, "Alice", 1_000_000);
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();

        let info = writer.finish().await.unwrap();

        // No panic, and the other components are still present.
        assert!(info.data_path.exists());
        assert!(info.index_path.as_ref().expect("BIG has Index.db").exists());
        assert!(info
            .summary_path
            .as_ref()
            .expect("BIG has Summary.db")
            .exists());
        assert!(info.stats_path.exists());
        assert!(info.toc_path.exists());
        assert!(info.digest_path.exists());

        // Byte-parity: Cassandra writes NO Filter.db for a disabled filter, and
        // SSTableInfo carries `None` so compaction skips the component.
        assert!(
            info.filter_path.is_none(),
            "disabled bloom filter must not report a Filter.db path"
        );

        // TOC.txt must NOT list the Filter component.
        let toc = std::fs::read_to_string(&info.toc_path).unwrap();
        assert!(
            !toc.contains("Filter.db"),
            "TOC must omit Filter.db for a disabled filter, got: {toc}"
        );
        // Sanity: other components ARE listed.
        assert!(toc.contains("Data.db"));
        assert!(toc.contains("Summary.db"));
    }

    /// Issue #852 (review finding 2): the disabled-filter behavior must work
    /// end-to-end from CQL. Parsing `CREATE TABLE ... WITH
    /// bloom_filter_fp_chance = 1.0` must thread the option through to the writer
    /// so that NO Filter.db component (file or TOC entry) is emitted — not just
    /// when `schema.comments` is hand-populated.
    #[tokio::test]
    async fn test_sstable_writer_disabled_filter_from_parsed_cql() {
        use crate::schema::cql_parser::parse_cql_schema;

        let temp_dir = TempDir::new().unwrap();
        let schema = parse_cql_schema(
            "CREATE TABLE test_ks.test_table (id int PRIMARY KEY, name text) \
             WITH bloom_filter_fp_chance = 1.0",
        )
        .expect("CQL with bloom_filter_fp_chance must parse");

        // The parser must have preserved the option (regression guard for the
        // previous `comments: HashMap::new()` drop).
        assert_eq!(
            schema
                .comments
                .get("bloom_filter_fp_chance")
                .map(String::as_str),
            Some("1.0")
        );

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();
        let mutation = create_test_mutation("test_ks", "test_table", 1, "Alice", 1_000_000);
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();

        let info = writer.finish().await.unwrap();

        // No Filter.db reported, file absent, and TOC omits the component.
        assert!(
            info.filter_path.is_none(),
            "parsed fp_chance=1.0 must not report a Filter.db path"
        );
        let toc = std::fs::read_to_string(&info.toc_path).unwrap();
        assert!(
            !toc.contains("Filter.db"),
            "TOC must omit Filter.db for a parsed disabled filter, got: {toc}"
        );
    }

    /// Issue #852: a normal `bloom_filter_fp_chance` (the default) still emits a
    /// concrete Filter.db component and lists it in TOC.txt.
    #[tokio::test]
    async fn test_sstable_writer_default_bloom_filter_still_emitted() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema(); // no fp_chance -> default 0.01

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let mutation = create_test_mutation("test_ks", "test_table", 1, "Bob", 2_000_000);
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();

        let info = writer.finish().await.unwrap();

        assert!(
            info.filter_path.as_ref().is_some_and(|p| p.exists()),
            "default fp_chance must emit a Filter.db file"
        );
        let toc = std::fs::read_to_string(&info.toc_path).unwrap();
        assert!(toc.contains("Filter.db"), "TOC must list Filter.db");
    }

    #[tokio::test]
    async fn test_sstable_writer_multiple_partitions() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Write 3 partitions in token order
        let mutations = vec![
            create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000),
            create_test_mutation("test_ks", "test_table", 2, "Bob", 1001000),
            create_test_mutation("test_ks", "test_table", 3, "Charlie", 1002000),
        ];

        // Sort by token
        let mut keyed_mutations: Vec<_> = mutations
            .into_iter()
            .map(|m| {
                let key = m.decorated_key(&schema).unwrap();
                (key, m)
            })
            .collect();
        keyed_mutations.sort_by_key(|(k, _)| k.token);

        for (key, mutation) in keyed_mutations {
            writer.write_partition(key, vec![mutation]).unwrap();
        }

        let info = writer.finish().await.unwrap();

        assert_eq!(info.partition_count, 3);
        assert!(info.data_size > 0);
    }

    #[tokio::test]
    async fn test_sstable_writer_token_ordering_validation() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Write first partition
        let mutation1 = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
        let key1 = mutation1.decorated_key(&schema).unwrap();
        let token1 = key1.token;

        writer
            .write_partition(key1.clone(), vec![mutation1])
            .unwrap();

        // Try to write a partition with lower token (should fail)
        let key2 = DecoratedKey::new(token1 - 1, vec![0x00, 0x00, 0x00, 0x02]);
        let mutation2 = create_test_mutation("test_ks", "test_table", 2, "Bob", 1001000);

        let result = writer.write_partition(key2, vec![mutation2]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("token order"));
    }

    #[tokio::test]
    async fn test_sstable_writer_component_paths() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let _writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 42, &schema).unwrap();

        // Verify generation number is used in paths
        // (we don't actually write anything, just test path construction)

        let big_path =
            SSTableWriter::component_path_for(temp_dir.path(), 42, SSTableFormat::Big, "Data.db");
        assert_eq!(
            big_path.file_name().unwrap().to_str().unwrap(),
            "nb-42-big-Data.db"
        );

        // BTI uses the `da` version letter and `bti` format segment (issue #908).
        let bti_path =
            SSTableWriter::component_path_for(temp_dir.path(), 42, SSTableFormat::Bti, "Data.db");
        assert_eq!(
            bti_path.file_name().unwrap().to_str().unwrap(),
            "da-42-bti-Data.db"
        );
    }

    #[tokio::test]
    async fn test_sstable_writer_toc_contents() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let mutation = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
        let key = mutation.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![mutation]).unwrap();
        let info = writer.finish().await.unwrap();

        // Read TOC.txt and verify contents
        let toc_contents = std::fs::read_to_string(&info.toc_path).unwrap();
        assert!(toc_contents.contains("Data.db"));
        assert!(toc_contents.contains("Index.db"));
        assert!(toc_contents.contains("Filter.db"));
        assert!(toc_contents.contains("Summary.db"));
        assert!(toc_contents.contains("Statistics.db"));
        assert!(!toc_contents.contains("CompressionInfo.db"));
        assert!(toc_contents.contains("Digest.crc32"));
        assert!(toc_contents.contains("TOC.txt"));
    }

    #[tokio::test]
    async fn test_sstable_writer_statistics_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Write partitions with varying timestamps and TTLs
        let mutations = vec![
            {
                let mut m = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
                m.ttl_seconds = Some(3600);
                m
            },
            create_test_mutation("test_ks", "test_table", 2, "Bob", 2000000),
            {
                let mut m = create_test_mutation("test_ks", "test_table", 3, "Charlie", 1500000);
                m.ttl_seconds = Some(7200);
                m
            },
        ];

        for mutation in mutations {
            let key = mutation.decorated_key(&schema).unwrap();
            writer.write_partition(key, vec![mutation]).unwrap();
        }

        // Check statistics were updated
        assert_eq!(writer.stats.min_timestamp, 1000000);
        assert_eq!(writer.stats.max_timestamp, 2000000);
        assert_eq!(writer.stats.min_ttl, 3600);
        assert_eq!(writer.stats.max_ttl, 7200);
        assert_eq!(writer.stats.partition_count, 3);

        let _info = writer.finish().await.unwrap();
    }

    /// A schema with one partition key, one clustering key, and a non-frozen
    /// `set<text>` regular column `tags` (a complex/multi-cell column). Used by the
    /// issue #887 complex-deletion / per-element stats regressions.
    fn create_complex_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_complex".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "tags".to_string(),
                    data_type: "set<text>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        }
    }

    /// Issue #887: a tombstone-only row that ALSO carries a surviving complex-deletion
    /// marker (its `marked_for_delete_at` strictly supersedes the row tombstone) must
    /// fold the marker's OWN `marked_for_delete_at` / `local_deletion_time` into the
    /// SSTable stats. The DataWriter delta-encodes the marker against `min_timestamp`
    /// / `min_local_deletion_time`, so if the stats only saw the row tombstone's
    /// timestamp/LDT the marker's bytes would be encoded against a baseline that
    /// Statistics.db never advertises.
    #[tokio::test]
    async fn test_complex_deletion_marker_folds_into_stats() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_complex_schema();
        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let table_id = TableId::new("test_ks", "test_complex");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ck = ClusteringKey::single("ck", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            Some(ck),
            vec![
                CellOperation::DeleteRow,
                CellOperation::ComplexDeletion {
                    column: "tags".to_string(),
                    marked_for_delete_at: 9_000_000,
                    local_deletion_time: 1_500,
                },
            ],
            5_000_000,
            None,
        )
        .with_local_deletion_time(1_700);
        let key = mutation.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![mutation]).unwrap();

        assert_eq!(
            writer.stats.max_timestamp, 9_000_000,
            "complex-deletion marked_for_delete_at must lift max_timestamp above the row timestamp"
        );
        assert_eq!(
            writer.stats.min_local_deletion_time, 1_500,
            "complex-deletion local_deletion_time must lower min_local_deletion_time below the row LDT"
        );

        let _info = writer.finish().await.unwrap();
    }

    /// Issue #887: a per-element complex cell carries its OWN timestamp/ttl/
    /// local_deletion_time, which the DataWriter delta-encodes against the SSTable
    /// baselines. A `WriteComplexElement`-only row must fold all three into the stats.
    #[tokio::test]
    async fn test_write_complex_element_folds_into_stats() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_complex_schema();
        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let table_id = TableId::new("test_ks", "test_complex");
        let pk = PartitionKey::single("id", Value::Integer(2));
        let ck = ClusteringKey::single("ck", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            Some(ck),
            vec![CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: b"hot".to_vec(),
                value: None,
                timestamp_micros: 9_500_000,
                ttl_seconds: Some(4_200),
                local_deletion_time: Some(1_400),
                is_deleted: false,
            }],
            3_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![mutation]).unwrap();

        assert_eq!(
            writer.stats.max_timestamp, 9_500_000,
            "per-element timestamp must lift max_timestamp above the row timestamp"
        );
        assert_eq!(
            writer.stats.min_ttl, 4_200,
            "per-element TTL must populate min_ttl"
        );
        assert_eq!(
            writer.stats.max_ttl, 4_200,
            "per-element TTL must populate max_ttl"
        );
        assert_eq!(
            writer.stats.min_local_deletion_time, 1_400,
            "per-element local_deletion_time must populate min_local_deletion_time"
        );

        let _info = writer.finish().await.unwrap();
    }

    /// Issue #887: the PRE-SEEDED baseline path (`compute_mutations_baseline_stats`,
    /// issue #729 two-pass flush) must fold the same marker/element timestamps the
    /// DataWriter delta-encodes. A marker LDT or element ts/ttl BELOW the mutation's
    /// own values would otherwise underflow the locked delta baseline.
    #[test]
    fn test_compute_baseline_folds_complex_ops() {
        let table_id = TableId::new("test_ks", "test_complex");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ck = ClusteringKey::single("ck", Value::Integer(1));

        let mutation = Mutation::new(
            table_id,
            pk,
            Some(ck),
            vec![
                CellOperation::DeleteRow,
                CellOperation::ComplexDeletion {
                    column: "tags".to_string(),
                    marked_for_delete_at: 9_000_000,
                    local_deletion_time: 1_500,
                },
                CellOperation::WriteComplexElement {
                    column: "tags".to_string(),
                    cell_path: b"warm".to_vec(),
                    value: None,
                    timestamp_micros: 2_000_000,
                    ttl_seconds: Some(600),
                    local_deletion_time: Some(1_300),
                    is_deleted: false,
                },
            ],
            5_000_000,
            None,
        )
        .with_local_deletion_time(1_700);

        let (min_ts, min_ldt, min_ttl) =
            SSTableWriter::compute_mutations_baseline_stats(std::slice::from_ref(&mutation));

        assert_eq!(
            min_ts, 2_000_000,
            "baseline min_timestamp must reflect the per-element timestamp below the row ts"
        );
        assert_eq!(
            min_ldt, 1_300,
            "baseline min_ldt must reflect the lowest complex LDT (the element's 1_300)"
        );
        assert_eq!(
            min_ttl, 600,
            "baseline min_ttl must reflect the per-element TTL"
        );
    }

    /// #921 finding 2 (roborev Medium): a `CellOperation::Delete` carrying an
    /// explicit per-cell `local_deletion_time: Some(L)` is stamped with `L`
    /// VERBATIM by `DataWriter` (via `op_cell_local_deletion_time`). The writer's
    /// STATS collection must record that SAME `L`, not just
    /// `mutation.effective_local_deletion_time()`:
    ///   * an `L` BELOW the mutation-derived value must (a) let the Data.db write
    ///     SUCCEED (no LDT-below-baseline delta underflow) and (b) lower
    ///     `min_local_deletion_time` to `L`;
    ///   * an `L` ABOVE the mutation-derived value must lift
    ///     `max_local_deletion_time` to `L`.
    /// RED before the fix: stats used the mutation LDT only, so the lower-`L`
    /// tombstone underflowed the baseline (write fails) and min/max were wrong.
    #[tokio::test]
    async fn test_per_cell_delete_ldt_drives_stats_and_write() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Mutation timestamp 5_000_000 micros => effective LDT = 5 seconds.
        // Per-cell LDTs straddle that: 2 (below) and 9_000 (above).
        const ROW_TS_MICROS: i64 = 5_000_000;
        const LOWER_LDT: i32 = 2;
        const HIGHER_LDT: i32 = 9_000;

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(7));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Delete {
                    column: "name".to_string(),
                    local_deletion_time: Some(LOWER_LDT),
                },
                CellOperation::Delete {
                    column: "name".to_string(),
                    local_deletion_time: Some(HIGHER_LDT),
                },
            ],
            ROW_TS_MICROS,
            None,
        );
        let key = mutation.decorated_key(&schema).unwrap();

        // (a) The write must SUCCEED: the per-cell LDT of 2 is below the
        // mutation-derived baseline of 5, so if stats recorded 5 the Data.db
        // delta (cell LDT 2 - baseline 5) underflows and the write errors.
        writer
            .write_partition(key, vec![mutation])
            .expect("per-cell Delete LDT below the mutation LDT must not underflow the baseline");

        // (b) Stats must describe the tombstones actually written.
        assert_eq!(
            writer.stats.min_local_deletion_time, LOWER_LDT,
            "min_local_deletion_time must reflect the lower per-cell Delete LDT"
        );
        assert_eq!(
            writer.stats.max_local_deletion_time, HIGHER_LDT,
            "max_local_deletion_time must reflect the higher per-cell Delete LDT"
        );

        let _info = writer.finish().await.unwrap();
    }

    /// #921 finding 2: the PRE-SEEDED baseline path
    /// (`compute_mutations_baseline_stats`, issue #729 two-pass flush) must lock
    /// `min_local_deletion_time` to the EXACT LDT the tombstone is written with.
    /// A per-cell `Delete` LDT below the mutation-derived value must drag the
    /// baseline down to it, else the locked delta underflows at write time.
    #[test]
    fn test_compute_baseline_uses_per_cell_delete_ldt() {
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(7));

        // Mutation LDT = 5 (5_000_000 micros). Per-cell LDT of 2 is lower.
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Delete {
                column: "name".to_string(),
                local_deletion_time: Some(2),
            }],
            5_000_000,
            None,
        );

        let (_min_ts, min_ldt, _min_ttl) =
            SSTableWriter::compute_mutations_baseline_stats(std::slice::from_ref(&mutation));

        assert_eq!(
            min_ldt, 2,
            "baseline min_ldt must reflect the per-cell Delete LDT (2), not the mutation LDT (5)"
        );
    }

    #[tokio::test]
    async fn test_sstable_writer_digest_crc32() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let mutation = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
        let key = mutation.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![mutation]).unwrap();
        let info = writer.finish().await.unwrap();

        // Verify Digest.crc32 was created and contains a number
        let digest_contents = std::fs::read_to_string(&info.digest_path).unwrap();
        assert!(!digest_contents.is_empty());
        assert!(digest_contents.parse::<u32>().is_ok());
    }

    /// Issue #851 review: a pure primary-key insert (a mutation with no cell
    /// operations and no tombstone payload, in a schema with NO static columns)
    /// is a LIVE row. `DataWriter::merge_row_group` emits it to Data.db via the
    /// `pure_pk_insert` liveness path, so Statistics must count it as
    /// `row_count == 1`, `column_count == 0`. Suppressing it (the rejected
    /// `operations.is_empty()` guard) undercounted `totalRows`.
    #[tokio::test]
    async fn test_pure_primary_key_insert_counts_as_live_row() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema(); // no static columns

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // INSERT of only the primary key: no cell operations, no tombstone.
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let pure_pk = Mutation::new(table_id, pk, None, vec![], 1_000_000, None);
        let key = pure_pk.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![pure_pk]).unwrap();

        assert_eq!(
            writer.stats.row_count, 1,
            "pure primary-key insert is a live row"
        );
        assert_eq!(
            writer.stats.column_count, 0,
            "pure primary-key insert sets no columns"
        );
        assert_eq!(writer.stats.partition_count, 1);

        let _info = writer.finish().await.unwrap();
    }

    /// Issue #851 / Cassandra `1502b0a9`: a partition that declares static
    /// columns but writes none produces an EMPTY static-row prelude. Cassandra
    /// emits the prelude for structural reasons but `Row.isEmpty()` is true, so
    /// it must NOT inflate `totalRows` (`row_count`) or `totalColumnsSet`
    /// (`column_count`). This requires a schema that actually HAS static columns;
    /// the no-static-column schema would make this a pure-PK live row instead.
    #[tokio::test]
    async fn test_empty_static_row_prelude_not_counted() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_static_schema(); // has a static column `s`

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // A static-row mutation (no clustering key) that writes NO static cells:
        // the empty static-row prelude.
        let table_id = TableId::new("test_ks", "test_static");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let empty_static = Mutation::new(table_id, pk, None, vec![], 1_000_000, None);
        let key = empty_static.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![empty_static]).unwrap();

        assert_eq!(
            writer.stats.row_count, 0,
            "empty static-row prelude must not inflate totalRows"
        );
        assert_eq!(
            writer.stats.column_count, 0,
            "empty static-row prelude must not inflate totalColumnsSet"
        );
        // The partition itself is still tracked.
        assert_eq!(writer.stats.partition_count, 1);

        let _info = writer.finish().await.unwrap();
    }

    /// A non-empty row is still counted normally after the empty-row guard.
    #[tokio::test]
    async fn test_non_empty_row_still_counted() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let mutation = create_test_mutation("test_ks", "test_table", 1, "Alice", 1_000_000);
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();

        assert_eq!(writer.stats.row_count, 1);
        assert_eq!(writer.stats.column_count, 1);

        let _info = writer.finish().await.unwrap();
    }

    /// A `DeleteRow` row tombstone is a non-empty row (counted) but sets no
    /// columns (mirrors Cassandra `Row.columnCount()`).
    #[tokio::test]
    async fn test_row_tombstone_counts_row_not_columns() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let row_tombstone = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::DeleteRow],
            1_000_000,
            None,
        );
        let key = row_tombstone.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![row_tombstone]).unwrap();

        assert_eq!(
            writer.stats.row_count, 1,
            "row tombstone is a non-empty row"
        );
        assert_eq!(
            writer.stats.column_count, 0,
            "row tombstone sets no columns"
        );

        let _info = writer.finish().await.unwrap();
    }

    /// Issue #851 review finding #1: a mutation whose ONLY op writes a
    /// clustering-key column must count as `row_count == 1`, `column_count == 0`.
    /// `DataWriter::merge_row_group` drops partition/clustering-key columns from
    /// the emitted cells (they are encoded positionally in the clustering
    /// prefix), but the write still confers row liveness. The stats are now
    /// derived from the emitter's `PartitionEmitCounts`, so they cannot inflate
    /// `totalColumnsSet` for a key-only write.
    #[tokio::test]
    async fn test_clustering_key_only_write_counts_row_zero_columns() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_clustered_schema(); // no static columns

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // The only op writes the clustering-key column `ck`.
        let table_id = TableId::new("test_ks", "test_clustered");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ck = ClusteringKey::single("ck", Value::Integer(7));
        let mutation = Mutation::new(
            table_id,
            pk,
            Some(ck),
            vec![CellOperation::Write {
                column: "ck".to_string(),
                value: Value::Integer(7),
            }],
            1_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();

        assert_eq!(
            writer.stats.row_count, 1,
            "a clustering-key-only write is a live row"
        );
        assert_eq!(
            writer.stats.column_count, 0,
            "clustering-key columns are not emitted as cells, so set no columns"
        );

        let _info = writer.finish().await.unwrap();
    }

    /// Issue #851 review finding #2: a single clustered mutation carrying BOTH a
    /// static write and a regular write produces TWO physical rows in Data.db —
    /// the non-empty static prelude (collected from all mutations) AND the
    /// clustering row (emitted after skipping the static op). The stats, derived
    /// from the emitter, must report `row_count == 2` and one column per row.
    #[tokio::test]
    async fn test_static_plus_regular_in_one_mutation_counts_two_rows() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_static_and_regular_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let table_id = TableId::new("test_ks", "test_static_regular");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ck = ClusteringKey::single("ck", Value::Integer(7));
        let mutation = Mutation::new(
            table_id,
            pk,
            Some(ck),
            vec![
                CellOperation::Write {
                    column: "s".to_string(),
                    value: Value::Text("static-val".to_string()),
                },
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("regular-val".to_string()),
                },
            ],
            1_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();

        assert_eq!(
            writer.stats.row_count, 2,
            "static prelude + clustering row are two physical rows"
        );
        assert_eq!(
            writer.stats.column_count, 2,
            "one static cell + one regular cell"
        );

        let _info = writer.finish().await.unwrap();
    }

    /// Issue #851: multiple mutations sharing one clustering key are merged into
    /// a SINGLE row by `DataWriter::merge_row_group`. The stats must follow the
    /// emitter and count one row, with one column per distinct surviving cell.
    #[tokio::test]
    async fn test_multiple_mutations_same_clustering_key_merge_to_one_row() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_clustered_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let table_id = TableId::new("test_ks", "test_clustered");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let make = |ts: i64, val: &str| {
            Mutation::new(
                table_id.clone(),
                pk.clone(),
                Some(ClusteringKey::single("ck", Value::Integer(7))),
                vec![CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text(val.to_string()),
                }],
                ts,
                None,
            )
        };
        let m1 = make(1_000_000, "first");
        let m2 = make(2_000_000, "second");
        let key = m1.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![m1, m2]).unwrap();

        assert_eq!(
            writer.stats.row_count, 1,
            "two mutations on the same clustering key merge to one row"
        );
        assert_eq!(
            writer.stats.column_count, 1,
            "both writes target the same column `name`, so one surviving cell"
        );

        let _info = writer.finish().await.unwrap();
    }

    /// Issue #851 review (this fix): an INSERT that writes one non-null column
    /// and one null-valued column must count only the cell that is physically
    /// serialized. `write_merged_cells` skips the null `Write`, so Statistics'
    /// `column_count` must equal 1 (not `row.ops.len() == 2`). The row stays
    /// live (`row_count == 1`).
    #[tokio::test]
    async fn test_insert_with_null_column_counts_only_non_null_cells() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_two_regular_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let table_id = TableId::new("test_ks", "test_two_regular");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                },
                CellOperation::Write {
                    column: "age".to_string(),
                    value: Value::Null,
                },
            ],
            1_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();

        assert_eq!(writer.stats.row_count, 1, "the insert is a live row");
        assert_eq!(
            writer.stats.column_count, 1,
            "only the non-null `name` cell is serialized; the null `age` write is skipped"
        );

        let _info = writer.finish().await.unwrap();
    }

    /// Issue #851 review (this fix): a row whose ONLY write is null-valued
    /// serializes no cell, yet the write still confers row liveness. Statistics
    /// must report `row_count == 1` and `column_count == 0`, matching Data.db
    /// (the previous `row.ops.len()` count over-reported one column).
    #[tokio::test]
    async fn test_row_with_only_null_write_counts_row_zero_columns() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_two_regular_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let table_id = TableId::new("test_ks", "test_two_regular");
        let pk = PartitionKey::single("id", Value::Integer(2));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Null,
            }],
            1_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();

        assert_eq!(
            writer.stats.row_count, 1,
            "a null-valued write is still a live row"
        );
        assert_eq!(
            writer.stats.column_count, 0,
            "the null write serializes no cell, so sets no columns"
        );

        let _info = writer.finish().await.unwrap();
    }

    /// Issue #851 review (this fix): the static path applies the same rule. A
    /// static null write serializes no static cell, so the non-empty static
    /// prelude (one row) must count ZERO columns. Here one static column is
    /// written non-null and one regular write follows, so the static prelude
    /// contributes 1 column and the clustering row contributes 1 — but a null
    /// static write must not be counted.
    #[tokio::test]
    async fn test_static_null_write_not_counted() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_static_schema(); // partition id, clustering ck, static s

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // A static-only mutation (no clustering key) writing the static column
        // `s` as NULL: the static prelude is present but serializes no cell.
        let table_id = TableId::new("test_ks", "test_static");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "s".to_string(),
                value: Value::Null,
            }],
            1_000_000,
            None,
        );
        let key = mutation.decorated_key(&schema).unwrap();
        writer.write_partition(key, vec![mutation]).unwrap();

        assert_eq!(
            writer.stats.column_count, 0,
            "a null static write serializes no static cell, so sets no columns"
        );

        let _info = writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_sstable_writer_empty_sstable() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Finish without writing any partitions
        let info = writer.finish().await.unwrap();

        assert_eq!(info.partition_count, 0);
        assert!(info.data_path.exists());
        assert!(info.toc_path.exists());
    }
}
