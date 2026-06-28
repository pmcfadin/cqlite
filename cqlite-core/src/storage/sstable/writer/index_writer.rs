//! Index.db writer - writes partition index
//!
//! Generates the Index.db component with BIG format (legacy row index).
//! Maps partition keys to Data.db file offsets for fast partition lookup.
//!
//! # BIG Index Format
//!
//! Index.db stores partition-to-Data.db offset mappings for efficient partition lookup.
//!
//! ## Entry Structure (BIG format, NB variant)
//!
//! Each entry stores a partition's location:
//! ```text
//! [key_len: u16 BE]                ← Length of partition key bytes
//! [key_bytes: key_len bytes]       ← Raw partition key bytes
//! [position: unsigned VInt]        ← Byte offset in Data.db
//! [promoted_index_size: unsigned VInt] ← 0 for simple partitions; byte count of promoted index for wide ones
//! [promoted_index_data: promoted_index_size bytes] ← Only when promoted_index_size > 0
//! ```
//!
//! ## Promoted Index (wide partitions ≥ 64 KiB)
//!
//! When a partition's uncompressed data exceeds 64 KiB, a promoted index is written so
//! Cassandra can seek to a specific clustering-key range without reading the full partition.
//!
//! A promoted index is only emitted when **two or more** `IndexInfo` blocks result
//! (Cassandra `RowIndexEntry.create()` lines 227-239). The promoted index payload is:
//!
//! ```text
//! [headerLength: unsigned VInt]    ← Data.db byte offset from partition start to first row
//!                                    (NB format, no static rows: 2 + key_len + 12)
//! [DeletionTime: 12 bytes]         ← partition-level DeletionTime in NB legacy format:
//!                                    [localDeletionTime: i32 BE][markedForDeleteAt: i64 BE]
//!                                    LIVE = Integer.MAX_VALUE + Long.MIN_VALUE
//! [count: unsigned VInt]           ← number of IndexInfo blocks
//! [IndexInfo[0]..]                 ← IndexInfo blocks (see PromotedIndexBlock)
//! [offset[0]: i32 BE] ...          ← relative offsets from first IndexInfo start, one per block
//! ```
//!
//! Each `IndexInfo` block:
//! ```text
//! [firstName: ClusteringPrefix]    ← min clustering key in block (vint header + values)
//! [lastName: ClusteringPrefix]     ← max clustering key in block
//! [offset: unsigned VInt]          ← byte offset from partition start
//! [width: signed VInt]             ← (actual_width - WIDTH_BASE) where WIDTH_BASE = 64 KiB,
//!                                    zigzag-encoded (Cassandra writeVInt)
//! [endOpenMarker: bool byte]       ← 0x00 = no open range tombstone marker
//! ```
//!
//! Sources:
//! - `RowIndexEntry.IndexedEntry.serialize()` (BIG "nb" serialization)
//! - `IndexInfo.Serializer.serialize()` lines 119-128
//! - `DeletionTime.LegacySerializer.serialize()` (NB "nb" format: ldt i32 BE, then mfda i64 BE)
//! - `SortedTablePartitionWriter.getHeaderLength()` (Data.db header byte count)
//!
//! ## Key Requirements
//!
//! - Entries must be in token order (same as Data.db partition order)
//! - Position offsets must match Data.db partition positions EXACTLY
//! - Key bytes are the raw serialized partition key (same as in Data.db)
//! - VInt encoding follows Cassandra unsigned VInt format
//!
//! References:
//! - `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`
//! - `cqlite-core/src/storage/sstable/index_reader.rs` - BIG format parser

use crate::error::{Error, Result};
use crate::storage::serialization::vint::{encode_signed, encode_unsigned};
use crate::storage::write_engine::mutation::DecoratedKey;
use std::io::Write;
use std::path::PathBuf;

/// Buffer capacity for the streaming Index.db `BufWriter`.
///
/// Mirrors `DATA_SINK_BUFFER_BYTES` in `data_writer.rs`: large enough that each
/// serialized entry coalesces into a handful of big `write()` syscalls while
/// keeping resident memory bounded to a single entry's scratch, not the whole file.
const INDEX_SINK_BUFFER_BYTES: usize = 512 * 1024;

/// Threshold at which a new IndexInfo block is emitted (Cassandra default 64 KiB).
///
/// A new block is started whenever the uncompressed row data accumulated since the
/// last block boundary reaches this limit.
///
/// Source: `BigFormatPartitionWriter.DEFAULT_GRANULARITY` = 64 * 1024.
pub const COLUMN_INDEX_SIZE_BYTES: u64 = 64 * 1024;

/// Delta-encoding base for `IndexInfo.width` field.
///
/// Each block's actual width is stored as `(actual_width - WIDTH_BASE)` so that
/// typical blocks near 64 KiB encode to a small or zero VInt value.
///
/// Source: `IndexInfo.WIDTH_BASE` = 64 * 1024.
pub const INDEX_INFO_WIDTH_BASE: u64 = 64 * 1024;

/// One IndexInfo block in the promoted index.
///
/// Cassandra emits one block per `column_index_size` (64 KiB) boundary crossed.
/// A promoted index is only written when there are **two or more** blocks
/// (`RowIndexEntry.create()` lines 227-239).
#[derive(Debug, Clone, Default)]
pub struct PromotedIndexBlock {
    /// Serialized `ClusteringPrefix` for the first unfiltered in this block.
    ///
    /// Format: `[header: unsigned VInt (2 bits per CK col)][value bytes…]`
    /// Same encoding as the clustering prefix in Data.db rows.
    /// Empty `Vec` for tables without clustering keys or for empty-clustering rows.
    pub first_name: Vec<u8>,

    /// Serialized `ClusteringPrefix` for the last unfiltered in this block.
    pub last_name: Vec<u8>,

    /// Byte offset from the start of the partition's Data.db bytes to the first
    /// unfiltered in this block.
    pub offset: u64,

    /// Total width (bytes) of this block's data.
    pub width: u64,

    /// OSS50 byte-comparable separator key for this block (the first unfiltered's
    /// clustering prefix), used by the BTI `Rows.db` row-index trie (issue #910).
    ///
    /// `None` for the BIG `Index.db` path (which uses `first_name`/`last_name`
    /// Data.db `ClusteringPrefix` bytes instead) and for tables without a
    /// clustering key. This is a separate, byte-comparable encoding that the BTI
    /// reader reconstructs during DFS; it is NOT the `first_name` form.
    pub oss50_separator: Option<Vec<u8>>,
}

/// Index.db component writer
///
/// Writes partition index entries in BIG format (NB variant) for Cassandra 5.0 compatibility.
/// Each entry maps a partition key to a Data.db file offset, optionally with a promoted index
/// for wide partitions (≥ 64 KiB).
///
/// # Memory model (Issue #753)
///
/// Two modes that produce **byte-identical** Index.db output:
///
/// * **In-memory mode** (`IndexWriter::new`): every serialized entry is appended to
///   `buffer`; `finish()` returns the full bytes. Used by unit tests that inspect
///   produced bytes directly.
///
/// * **Streaming mode** (`IndexWriter::with_sink`): each entry is serialized into
///   a small scratch `Vec`, written to a `BufWriter<File>` over the Index.db path,
///   and the scratch is cleared. Peak heap is therefore O(one entry) rather than
///   O(file), keeping a multi-GB compaction within the 128 MB target. The file is
///   opened lazily on the first `add_partition` call so the parent directory need
///   not exist before construction.
///
/// * **Counting mode** (`IndexWriter::counting`): like streaming, but with no sink —
///   each entry is serialized into the scratch `Vec` only to measure its size, then
///   the scratch is cleared. No file is created and no entry bytes are retained, so
///   peak heap is O(one entry). This is the BTI path (Issue #908): BTI has no
///   `Index.db`, but the write loop still needs the per-entry `index_offset`/size
///   bookkeeping. The earlier BTI code used in-memory mode, which retained every
///   serialized entry in `buffer` forever — a full, never-emitted in-memory `Index.db`
///   that defeated the streaming memory budget on large writes.
///
/// In all modes `index_offset` for a new entry = `position + buffer.len()` measured
/// before any bytes are written. In streaming/counting modes `buffer` is empty at that
/// point (cleared after the previous entry's flush) so the offset equals `position`; in
/// in-memory mode `position` is always 0 and `buffer` holds all prior entries, so
/// the offset equals `buffer.len()`.
#[derive(Debug)]
pub struct IndexWriter {
    /// Per-entry scratch buffer.
    ///
    /// In streaming/counting modes this holds exactly the bytes for one entry (cleared
    /// after each flush). In in-memory mode it accumulates the entire Index.db output.
    buffer: Vec<u8>,
    /// Streaming sink over the Index.db path (streaming mode only).
    ///
    /// Lazily opened on the first `add_partition` call. `None` in in-memory and
    /// counting modes.
    sink: Option<std::io::BufWriter<std::fs::File>>,
    /// Index.db output path (streaming mode only); used for lazy sink open.
    /// `None` in in-memory and counting modes.
    index_path: Option<PathBuf>,
    /// Whether this writer is in counting mode (no sink, no retention, but the
    /// per-entry scratch is still cleared so memory stays O(one entry)).
    counting: bool,
    /// Bytes that would have been written so far. Advances in streaming and counting
    /// modes (so `index_offset` tracks correctly); always 0 in in-memory mode.
    position: u64,
    /// Entry count (for validation)
    entry_count: usize,
}

/// Information about a written index entry
///
/// Returned by `add_partition()` to track Summary.db sampling points.
#[derive(Debug, Clone, Copy)]
pub struct IndexEntryInfo {
    /// Byte offset in Index.db where this entry starts
    pub index_offset: u64,
    /// Size of this entry in bytes
    pub entry_size: usize,
}

impl IndexWriter {
    /// Create a new in-memory Index.db writer.
    ///
    /// All entries accumulate in `buffer`; `finish()` returns the full bytes.
    /// Prefer [`IndexWriter::with_sink`] for production writes to bound memory.
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    ///
    /// let writer = IndexWriter::new();
    /// assert_eq!(writer.entry_count(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            sink: None,
            index_path: None,
            counting: false,
            position: 0,
            entry_count: 0,
        }
    }

    /// Create a counting-only Index.db writer that retains no entry bytes.
    ///
    /// Used by the BTI write path (Issue #908): BTI emits no `Index.db`, but the
    /// write loop still needs each partition's `index_offset`/`entry_size`
    /// bookkeeping. Each entry is serialized into a scratch buffer only to measure
    /// its size and advance `position`, then the scratch is cleared — so peak heap
    /// is O(one entry) instead of accumulating a full, never-emitted in-memory
    /// `Index.db`. No file is created.
    ///
    /// `index_offset`/`entry_size` values are byte-identical to what the streaming
    /// and in-memory modes would report for the same partition sequence.
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = IndexWriter::counting();
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// let info = writer.add_partition(&key, 0).unwrap();
    /// assert_eq!(info.index_offset, 0);
    /// // No entry bytes are retained.
    /// assert_eq!(writer.buffered_len(), 0);
    /// ```
    pub fn counting() -> Self {
        Self {
            buffer: Vec::new(),
            sink: None,
            index_path: None,
            counting: true,
            position: 0,
            entry_count: 0,
        }
    }

    /// Create a streaming Index.db writer that flushes each entry to `index_path`.
    ///
    /// The file is opened lazily on the first `add_partition` (creating the parent
    /// directory if needed). Peak memory is bounded to a single entry's bytes,
    /// keeping a 10 M-partition write within the 128 MB target (Issue #753).
    ///
    /// # Arguments
    /// * `index_path` - Destination path for the Index.db component
    pub fn with_sink(index_path: PathBuf) -> Self {
        Self {
            buffer: Vec::new(),
            sink: None,
            index_path: Some(index_path),
            counting: false,
            position: 0,
            entry_count: 0,
        }
    }

    /// Lazily open the streaming sink (and create the parent directory).
    ///
    /// No-op in in-memory mode or once the sink is already open.
    fn ensure_sink(&mut self) -> Result<()> {
        if self.sink.is_some() {
            return Ok(());
        }
        if let Some(path) = self.index_path.clone() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::File::create(&path)?;
            self.sink = Some(std::io::BufWriter::with_capacity(
                INDEX_SINK_BUFFER_BYTES,
                file,
            ));
        }
        Ok(())
    }

    /// Flush the per-entry scratch buffer, advance `position`, and clear the scratch
    /// so only one entry is ever resident.
    ///
    /// - Streaming mode: writes the scratch bytes to the sink.
    /// - Counting mode: writes nothing (no sink) but still advances `position` and
    ///   clears the scratch, so memory stays O(one entry) and offset bookkeeping is
    ///   identical to streaming.
    /// - In-memory mode: no-op (the scratch keeps accumulating the whole file).
    fn flush_entry(&mut self) -> Result<()> {
        if self.counting {
            // Counting mode: retain nothing, but track byte position for offsets.
            self.position += self.buffer.len() as u64;
            self.buffer.clear();
            return Ok(());
        }
        if self.index_path.is_none() {
            // In-memory mode: keep accumulating in `buffer`.
            return Ok(());
        }
        self.ensure_sink()?;
        if let Some(sink) = self.sink.as_mut() {
            sink.write_all(&self.buffer)?;
        }
        self.position += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }

    /// Add a partition to the index (no promoted index — simple/small partition).
    ///
    /// Partitions MUST be added in token order (caller responsibility).
    ///
    /// # Arguments
    ///
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `data_offset` - Byte offset in Data.db where this partition starts
    ///
    /// # Returns
    ///
    /// `IndexEntryInfo` containing the exact byte offset where this entry was written
    /// in Index.db and the size of the entry. Use this for Summary.db sampling.
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = IndexWriter::new();
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// let info = writer.add_partition(&key, 0).unwrap();
    /// assert_eq!(info.index_offset, 0); // First entry starts at offset 0
    /// assert_eq!(writer.entry_count(), 1);
    /// ```
    pub fn add_partition(
        &mut self,
        key: &DecoratedKey,
        data_offset: u64,
    ) -> Result<IndexEntryInfo> {
        self.add_partition_with_promoted(key, data_offset, &[])
    }

    /// Add a partition to the index with optional promoted index blocks.
    ///
    /// Call this with a non-empty `blocks` slice for wide partitions (≥ 64 KiB).
    /// When `blocks` contains **fewer than 2 entries**, the promoted index is not
    /// emitted (matching Cassandra `RowIndexEntry.create()` which gates on
    /// `columnIndexCount > 1`). When 2 or more blocks are supplied the full
    /// promoted index payload is written.
    ///
    /// # Arguments
    ///
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `data_offset` - Byte offset in Data.db where this partition starts
    /// * `blocks` - Collected `PromotedIndexBlock`s from the data write pass
    pub fn add_partition_with_promoted(
        &mut self,
        key: &DecoratedKey,
        data_offset: u64,
        blocks: &[PromotedIndexBlock],
    ) -> Result<IndexEntryInfo> {
        // Index offset = bytes already flushed + whatever is in the scratch buffer.
        // In streaming mode the scratch is empty here (cleared by previous flush_entry),
        // so index_offset == position. In in-memory mode position == 0 and the scratch
        // holds all prior entries, so index_offset == buffer.len().
        let index_offset = self.position + self.buffer.len() as u64;
        let entry_size = self.write_entry(key, data_offset, blocks)?;
        // In streaming mode, push the just-written scratch bytes to the sink and clear.
        self.flush_entry()?;
        self.entry_count += 1;

        Ok(IndexEntryInfo {
            index_offset,
            entry_size,
        })
    }

    /// Write a single index entry to the buffer.
    ///
    /// Cassandra BIG format Index.db entry (NB variant):
    /// ```text
    /// [key_len: u16 BE]                    ← Length of raw partition key
    /// [key_bytes: key_len bytes]           ← Raw partition key bytes
    /// [position: unsigned VInt]            ← Data.db offset
    /// [promoted_index_size: unsigned VInt] ← 0 or byte count of promoted payload
    /// [promoted_index_data: promoted_index_size bytes] ← only when size > 0
    /// ```
    fn write_entry(
        &mut self,
        key: &DecoratedKey,
        data_offset: u64,
        blocks: &[PromotedIndexBlock],
    ) -> Result<usize> {
        let start_len = self.buffer.len();

        // Write key length (u16 big-endian)
        let key_len = key.key.len() as u16;
        self.buffer.extend_from_slice(&key_len.to_be_bytes());

        // Write raw partition key bytes
        self.buffer.extend_from_slice(&key.key);

        // Write position (unsigned VInt encoded)
        encode_unsigned(data_offset, &mut self.buffer);

        // Only emit a promoted index when there are 2+ blocks
        // (Cassandra RowIndexEntry.create() gates on columnIndexCount > 1)
        if blocks.len() >= 2 {
            let promoted_payload = serialize_promoted_index(blocks, key.key.len());
            encode_unsigned(promoted_payload.len() as u64, &mut self.buffer);
            self.buffer.extend_from_slice(&promoted_payload);
        } else {
            // Small partition — no promoted index
            encode_unsigned(0, &mut self.buffer);
        }

        let bytes_written = self.buffer.len() - start_len;
        Ok(bytes_written)
    }

    /// Finish writing and return the Index.db bytes (in-memory mode only).
    ///
    /// Returns an error if called on a streaming writer; use
    /// [`IndexWriter::finish_streaming`] instead.
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = IndexWriter::new();
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// writer.add_partition(&key, 100).unwrap();
    ///
    /// let bytes = writer.finish().unwrap();
    /// assert!(!bytes.is_empty());
    /// ```
    pub fn finish(self) -> Result<Vec<u8>> {
        if self.index_path.is_some() {
            return Err(Error::InvalidInput(
                "IndexWriter::finish() called on a streaming writer; use finish_streaming()"
                    .to_string(),
            ));
        }
        Ok(self.buffer)
    }

    /// Finish writing in streaming mode, flush the sink, and return the total bytes written.
    ///
    /// Returns an error if called on an in-memory writer; use [`IndexWriter::finish`] instead.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    /// use std::path::PathBuf;
    ///
    /// let mut writer = IndexWriter::with_sink(PathBuf::from("/tmp/nb-1-big-Index.db"));
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// writer.add_partition(&key, 100).unwrap();
    ///
    /// let total_bytes = writer.finish_streaming().unwrap();
    /// assert!(total_bytes > 0);
    /// ```
    pub fn finish_streaming(mut self) -> Result<u64> {
        if self.index_path.is_none() {
            return Err(Error::InvalidInput(
                "finish_streaming() called on an in-memory IndexWriter".to_string(),
            ));
        }
        // Flush any residual scratch (normally empty after per-entry flushes).
        self.flush_entry()?;
        // Flush the BufWriter so all bytes reach the OS file.
        if let Some(mut sink) = self.sink.take() {
            sink.flush()?;
        }
        Ok(self.position)
    }

    /// Get the number of index entries
    ///
    /// # Example
    ///
    /// ```
    /// use cqlite_core::storage::sstable::writer::IndexWriter;
    /// use cqlite_core::storage::write_engine::mutation::DecoratedKey;
    ///
    /// let mut writer = IndexWriter::new();
    /// assert_eq!(writer.entry_count(), 0);
    ///
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    /// writer.add_partition(&key, 0).unwrap();
    /// assert_eq!(writer.entry_count(), 1);
    /// ```
    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    /// Number of bytes currently held in the per-entry scratch buffer.
    ///
    /// In streaming and counting modes this is 0 between entries (the scratch is
    /// cleared after each `add_partition`). In in-memory mode it grows to the full
    /// serialized `Index.db` size. Exposed mainly so the BTI path can assert it does
    /// not accumulate index entry bytes (Issue #908).
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }
}

impl Default for IndexWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Byte size of a LIVE `DeletionTime` in the NB (legacy) serializer.
///
/// Cassandra NB format uses `DeletionTime.LegacySerializer`:
///   [localDeletionTime: i32 BE = Integer.MAX_VALUE][markedForDeleteAt: i64 BE = Long.MIN_VALUE]
/// = 4 + 8 = 12 bytes.
///
/// Source: `DeletionTime.LegacySerializer.serialize()` and `serializedSize()`.
const NB_DELETION_TIME_LIVE_SIZE: usize = 12;

/// Serialize the promoted index payload for a wide partition.
///
/// Layout (Cassandra BIG "nb" format, `RowIndexEntry.IndexedEntry.serialize()`):
/// ```text
/// [headerLength: unsigned VInt]    ← Data.db bytes from partition start to first row
///                                    = 2 (key_len short) + raw_key_len + 12 (NB DeletionTime)
/// [DeletionTime: 12 bytes]         ← LIVE in NB legacy format:
///                                    [i32 BE: Integer.MAX_VALUE][i64 BE: Long.MIN_VALUE]
/// [count: unsigned VInt]           ← number of IndexInfo blocks
/// [IndexInfo[0]..]                 ← serialized blocks in order
/// [offset[0]: i32 BE]             ← relative offsets from first IndexInfo start
/// ...
/// [offset[N-1]: i32 BE]
/// ```
///
/// `raw_key_len` is the length of the raw partition key bytes (not counting the 2-byte prefix).
///
/// The returned `Vec<u8>` is the content that goes AFTER the `promoted_index_size`
/// VInt in the Index.db entry.
///
/// # Correctness notes
///
/// - `headerLength` must match `SortedTablePartitionWriter.getHeaderLength()` in Cassandra,
///   which is the Data.db byte offset to the first non-header unfiltered row.
///   For CQLite-written "nb" partitions without static rows this is fixed:
///   `2 + raw_key_len + NB_DELETION_TIME_LIVE_SIZE`.
/// - DeletionTime must use the "nb" `LegacySerializer` (ldt i32 BE, then mfda i64 BE),
///   NOT the "oa" single-byte `0x80` form.
/// - `IndexInfo.width` must be written as a signed VInt (zigzag), not unsigned VInt,
///   matching Cassandra's `writeVInt(width - WIDTH_BASE)`.
fn serialize_promoted_index(blocks: &[PromotedIndexBlock], raw_key_len: usize) -> Vec<u8> {
    debug_assert!(blocks.len() >= 2, "caller must ensure at least 2 blocks");

    // headerLength: Data.db byte count from partition start to the first clustering row.
    // For "nb" (no static columns): 2-byte key_len prefix + raw key bytes + 12-byte DeletionTime.
    // Source: SortedTablePartitionWriter.start() + addStaticRow() → getHeaderLength().
    let header_length: u64 = (2 + raw_key_len + NB_DELETION_TIME_LIVE_SIZE) as u64;

    // LIVE DeletionTime in NB (legacy) format:
    //   localDeletionTime = Integer.MAX_VALUE = 0x7FFFFFFF (i32 BE)
    //   markedForDeleteAt = Long.MIN_VALUE    = 0x8000000000000000 (i64 BE)
    // Source: DeletionTime.LegacySerializer.serialize(), serializedSize() = 12.
    let mut deletion_time_bytes = [0u8; NB_DELETION_TIME_LIVE_SIZE];
    deletion_time_bytes[0..4].copy_from_slice(&i32::MAX.to_be_bytes());
    deletion_time_bytes[4..12].copy_from_slice(&i64::MIN.to_be_bytes());

    // --- Build IndexInfo bytes and accumulate per-block byte offsets ---
    let mut index_info_bytes: Vec<u8> = Vec::new();
    let mut block_start_offsets: Vec<u32> = Vec::with_capacity(blocks.len());

    for block in blocks {
        let block_offset_in_info = index_info_bytes.len() as u32;
        block_start_offsets.push(block_offset_in_info);
        serialize_index_info(&mut index_info_bytes, block);
    }

    // --- Assemble the full promoted index payload ---
    let mut payload: Vec<u8> = Vec::new();

    // headerLength (unsigned VInt)
    encode_unsigned(header_length, &mut payload);

    // DeletionTime bytes (12-byte NB legacy LIVE form)
    payload.extend_from_slice(&deletion_time_bytes);

    // count (unsigned VInt): number of IndexInfo blocks
    encode_unsigned(blocks.len() as u64, &mut payload);

    // IndexInfo bytes
    payload.extend_from_slice(&index_info_bytes);

    // Offsets array: one i32 BE per block (signed, relative to first IndexInfo start)
    // Source: RowIndexEntry.IndexedEntry.serialize() → `out.writeInt(offsets[i])`
    for &offset in &block_start_offsets {
        payload.extend_from_slice(&(offset as i32).to_be_bytes());
    }

    payload
}

/// Serialize one `IndexInfo` block.
///
/// Layout (Cassandra `IndexInfo.Serializer.serialize()` lines 119-128):
/// ```text
/// [firstName: ClusteringPrefix bytes]  ← min clustering key in block
/// [lastName: ClusteringPrefix bytes]   ← max clustering key in block
/// [offset: unsigned VInt]              ← byte offset from partition start
/// [width: signed VInt]                 ← (actual_width - WIDTH_BASE), zigzag-encoded
///                                        Cassandra uses writeVInt (not writeUnsignedVInt)
/// [endOpenMarker: bool byte]           ← 0x00 = no open range tombstone
/// ```
///
/// IMPORTANT: `width - WIDTH_BASE` is written as a **signed** VInt (zigzag encoding),
/// not an unsigned VInt.  Cassandra's `IndexInfo.Serializer.serialize()` line 124:
/// `out.writeVInt(info.width - WIDTH_BASE)`.  For a width exactly equal to WIDTH_BASE,
/// the delta is 0 and both encodings produce `0x00`, but for any other value they differ.
fn serialize_index_info(buf: &mut Vec<u8>, block: &PromotedIndexBlock) {
    // firstName clustering prefix bytes (already serialized by the data writer)
    buf.extend_from_slice(&block.first_name);

    // lastName clustering prefix bytes
    buf.extend_from_slice(&block.last_name);

    // offset (unsigned VInt)
    encode_unsigned(block.offset, buf);

    // width delta: (actual_width - WIDTH_BASE), written as signed VInt (zigzag).
    // Cassandra: `out.writeVInt(info.width - WIDTH_BASE)` — writeVInt uses zigzag encoding.
    // We cast to i64 to allow negative delta (width < WIDTH_BASE), which encodes as a small
    // negative zigzag value; in practice widths are >= WIDTH_BASE but the format supports it.
    let width_delta = (block.width as i64) - (INDEX_INFO_WIDTH_BASE as i64);
    encode_signed(width_delta, buf);

    // endOpenMarker presence: writeBoolean(false) = 0x00 for no open tombstone
    // Source: IndexInfo.Serializer.serialize() line 126: `out.writeBoolean(info.endOpenMarker != null)`
    buf.push(0x00u8);
}

/// Test/oracle helper: serialize a promoted index payload from blocks.
///
/// Exposes the private [`serialize_promoted_index`] for the promoted-index *reader*
/// round-trip tests (Issue #993), which must encode with the authoritative writer
/// and then decode, asserting byte-exact field recovery. Not part of the public API.
#[doc(hidden)]
#[cfg(test)]
pub(crate) fn serialize_promoted_index_for_test(
    blocks: &[PromotedIndexBlock],
    raw_key_len: usize,
) -> Vec<u8> {
    serialize_promoted_index(blocks, raw_key_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_writer_new() {
        let writer = IndexWriter::new();
        assert_eq!(writer.entry_count(), 0);
    }

    #[test]
    fn test_add_single_partition_int_key() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]); // int = 42

        let info = writer.add_partition(&key, 0).unwrap();

        assert_eq!(writer.entry_count(), 1);
        assert_eq!(info.index_offset, 0);
        // 2 (key_len) + 4 (key bytes) + 1 (pos=0) + 1 (promoted=0) = 8
        assert_eq!(info.entry_size, 8);
    }

    #[test]
    fn test_add_single_partition_uuid_key() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0xBB; 16]); // UUID-sized key

        let info = writer.add_partition(&key, 0).unwrap();

        assert_eq!(writer.entry_count(), 1);
        // 2 (key_len) + 16 (key bytes) + 1 (pos=0) + 1 (promoted=0) = 20
        assert_eq!(info.entry_size, 20);
    }

    #[test]
    fn test_raw_key_bytes_written() {
        let mut writer = IndexWriter::new();
        let pk_bytes = vec![0x00, 0x00, 0x00, 0x2A];
        let key = DecoratedKey::new(12345, pk_bytes.clone());

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // Format: [key_len:u16 BE][key_bytes][pos VInt][promoted VInt]
        // key_len = 4 -> 0x0004
        assert_eq!(&bytes[0..2], &[0x00, 0x04], "Key length should be 4");

        // Raw key bytes (not MD5!)
        assert_eq!(&bytes[2..6], &pk_bytes, "Should be raw key bytes");

        // Offset VInt(0) and promoted VInt(0)
        assert_eq!(bytes[6], 0x00, "Offset should be 0");
        assert_eq!(bytes[7], 0x00, "Promoted size should be 0");
    }

    #[test]
    fn test_uuid_key_raw_bytes() {
        let mut writer = IndexWriter::new();
        let pk_bytes = vec![
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let key = DecoratedKey::new(12345, pk_bytes.clone());

        writer.add_partition(&key, 0).unwrap();
        let bytes = writer.finish().unwrap();

        // key_len = 16 -> 0x0010
        assert_eq!(&bytes[0..2], &[0x00, 0x10], "Key length should be 16");

        // Raw UUID bytes
        assert_eq!(&bytes[2..18], &pk_bytes, "Should be raw UUID bytes");
    }

    #[test]
    fn test_add_multiple_partitions() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);
        let key3 = DecoratedKey::new(300, vec![0x00, 0x00, 0x00, 0x03]);

        let info1 = writer.add_partition(&key1, 0).unwrap();
        let info2 = writer.add_partition(&key2, 150).unwrap();
        let info3 = writer.add_partition(&key3, 300).unwrap();

        assert_eq!(writer.entry_count(), 3);

        assert_eq!(info1.index_offset, 0);
        assert_eq!(info2.index_offset, info1.entry_size as u64);
        assert_eq!(
            info3.index_offset,
            (info1.entry_size + info2.entry_size) as u64
        );
    }

    #[test]
    fn test_finish_multiple_entries() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 150).unwrap();

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 (len) + 4 (key) + 1 (pos=0) + 1 (promoted=0) = 8
        // Entry 2: 2 (len) + 4 (key) + 2 (pos=150, VInt) + 1 (promoted=0) = 9
        assert_eq!(bytes.len(), 17);

        // Check first entry key length
        assert_eq!(&bytes[0..2], &[0x00, 0x04]);

        // Check second entry key length at offset 8
        assert_eq!(&bytes[8..10], &[0x00, 0x04]);
    }

    #[test]
    fn test_position_encoding() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 127).unwrap(); // 1-byte VInt

        let bytes = writer.finish().unwrap();

        // Position at byte 6 (after key_len(2) + key(4))
        assert_eq!(bytes[6], 0x7F);
        assert_eq!(bytes[7], 0x00); // promoted
    }

    #[test]
    fn test_position_encoding_large_offset() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        writer.add_partition(&key, 12381).unwrap(); // 2-byte VInt: 0xB0 0x5D

        let bytes = writer.finish().unwrap();

        // Position at byte 6 (after key_len(2) + key(4))
        assert_eq!(bytes[6], 0xB0);
        assert_eq!(bytes[7], 0x5D);
        assert_eq!(bytes[8], 0x00); // promoted

        // Total: 2 + 4 + 2 + 1 = 9
        assert_eq!(bytes.len(), 9);
    }

    #[test]
    fn test_variable_key_sizes() {
        // 1-byte key
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0x42]);
        let info = writer.add_partition(&key, 0).unwrap();
        assert_eq!(info.entry_size, 5); // 2 + 1 + 1 + 1

        // 4-byte key (int)
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x2A]);
        let info = writer.add_partition(&key, 0).unwrap();
        assert_eq!(info.entry_size, 8); // 2 + 4 + 1 + 1

        // 8-byte key (bigint)
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0; 8]);
        let info = writer.add_partition(&key, 0).unwrap();
        assert_eq!(info.entry_size, 12); // 2 + 8 + 1 + 1

        // 16-byte key (uuid)
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0; 16]);
        let info = writer.add_partition(&key, 0).unwrap();
        assert_eq!(info.entry_size, 20); // 2 + 16 + 1 + 1
    }

    #[test]
    fn test_empty_index() {
        let writer = IndexWriter::new();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_token_order_preservation() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x01]);
        let key2 = DecoratedKey::new(200, vec![0x02]);
        let key3 = DecoratedKey::new(300, vec![0x03]);

        writer.add_partition(&key1, 0).unwrap();
        writer.add_partition(&key2, 100).unwrap();
        writer.add_partition(&key3, 200).unwrap();

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 (len) + 1 (key) + 1 (pos=0) + 1 (promoted=0) = 5
        // Entry 2: 2 (len) + 1 (key) + 1 (pos=100) + 1 (promoted=0) = 5
        // Entry 3: 2 (len) + 1 (key) + 2 (pos=200, 2-byte VInt) + 1 (promoted=0) = 6
        assert_eq!(bytes.len(), 16);

        // Check key length prefixes
        assert_eq!(&bytes[0..2], &[0x00, 0x01]);
        assert_eq!(&bytes[5..7], &[0x00, 0x01]);
        assert_eq!(&bytes[10..12], &[0x00, 0x01]);
    }

    #[test]
    fn test_vint_encoding_boundaries() {
        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);

        // Base size: 2 (key_len) + 4 (key) + 1 (promoted) = 7 + vint_len(offset)

        // Test offset at 127 (max 1-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 127).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 8); // 7 + 1

        // Test offset at 128 (min 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 128).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 9); // 7 + 2

        // Test offset at 16383 (max 2-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16383).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 9); // 7 + 2

        // Test offset at 16384 (min 3-byte VInt)
        let mut writer = IndexWriter::new();
        writer.add_partition(&key, 16384).unwrap();
        assert_eq!(writer.finish().unwrap().len(), 10); // 7 + 3
    }

    #[test]
    fn test_index_offset_tracking() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);
        let info1 = writer.add_partition(&key1, 0).unwrap(); // 1-byte VInt

        let key2 = DecoratedKey::new(200, vec![0x05, 0x06]);
        let info2 = writer.add_partition(&key2, 127).unwrap(); // 1-byte VInt

        let key3 = DecoratedKey::new(300, vec![0x07]);
        let info3 = writer.add_partition(&key3, 12381).unwrap(); // 2-byte VInt

        assert_eq!(info1.index_offset, 0);
        assert_eq!(info1.entry_size, 8, "Entry 1: 2 + 4 + 1 + 1 = 8");

        assert_eq!(info2.index_offset, 8);
        assert_eq!(info2.entry_size, 6, "Entry 2: 2 + 2 + 1 + 1 = 6");

        assert_eq!(info3.index_offset, 14);
        assert_eq!(info3.entry_size, 6, "Entry 3: 2 + 1 + 2 + 1 = 6");

        let bytes = writer.finish().unwrap();
        assert_eq!(
            bytes.len(),
            info1.entry_size + info2.entry_size + info3.entry_size,
            "Total size matches sum of entry sizes"
        );
    }

    #[test]
    fn test_realistic_scenario() {
        let mut writer = IndexWriter::new();

        let key1 = DecoratedKey::new(-5000000000, vec![0x00, 0x00, 0x03, 0xE9]);
        writer.add_partition(&key1, 0).unwrap();

        let key2 = DecoratedKey::new(-2000000000, vec![0x00, 0x00, 0x03, 0xEA]);
        writer.add_partition(&key2, 250).unwrap();

        let key3 = DecoratedKey::new(3000000000, vec![0x00, 0x00, 0x03, 0xEB]);
        writer.add_partition(&key3, 500).unwrap();

        assert_eq!(writer.entry_count(), 3);

        let bytes = writer.finish().unwrap();

        // Entry 1: 2 + 4 + 1 (pos=0) + 1 = 8
        // Entry 2: 2 + 4 + 2 (VInt 250) + 1 = 9
        // Entry 3: 2 + 4 + 2 (VInt 500) + 1 = 9
        assert_eq!(bytes.len(), 26);
    }

    // ── Promoted index tests ────────────────────────────────────────────────

    /// Small partition (1 block) → promoted_index_size = 0 (no regression).
    #[test]
    fn test_single_block_no_promoted_index() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);

        // Only 1 block → should NOT emit promoted index
        let block = PromotedIndexBlock {
            first_name: vec![0x00], // empty CK prefix (just a 0-header VInt)
            last_name: vec![0x00],
            offset: 0,
            width: 70_000,
            oss50_separator: None,
        };
        let info = writer
            .add_partition_with_promoted(&key, 0, &[block])
            .unwrap();

        let bytes = writer.finish().unwrap();

        // promoted_index_size must be 0 for a single block
        // Entry: 2 + 4 + 1(pos) + 1(promoted_len=0) = 8
        assert_eq!(
            bytes[info.entry_size - 1],
            0x00,
            "promoted size should be 0"
        );
        assert_eq!(info.entry_size, 8);
    }

    /// Two blocks → promoted_index_size > 0 and content is correct.
    #[test]
    fn test_two_blocks_emits_promoted_index() {
        let mut writer = IndexWriter::new();
        let key = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);

        // Minimal clustering prefix: header VInt 0x00 = no columns all-null/empty
        let ck_prefix = vec![0x00u8];
        let block1 = PromotedIndexBlock {
            first_name: ck_prefix.clone(),
            last_name: ck_prefix.clone(),
            offset: 0,
            width: 65_536, // exactly 64 KiB → delta = 0
            oss50_separator: None,
        };
        let block2 = PromotedIndexBlock {
            first_name: ck_prefix.clone(),
            last_name: ck_prefix.clone(),
            offset: 65_536,
            width: 65_536,
            oss50_separator: None,
        };

        let info = writer
            .add_partition_with_promoted(&key, 0, &[block1, block2])
            .unwrap();
        let bytes = writer.finish().unwrap();

        // The promoted_index_size vint must be > 0
        let promoted_size_offset = 2 + 4 + 1; // key_len + key + pos vint(0)
        assert!(
            bytes[promoted_size_offset] > 0,
            "promoted_index_size must be > 0 for 2 blocks"
        );

        // Verify the reader can parse it back (promoted bytes are present)
        assert!(
            info.entry_size > 8,
            "Wide partition entry must be larger than small"
        );
    }

    /// Wide partition (3 blocks): verify promoted_index_size matches actual payload.
    #[test]
    fn test_three_blocks_promoted_index_size_matches_payload() {
        let key = DecoratedKey::new(42, vec![0xAA, 0xBB]);

        // Serialize two clustering prefix bytes representing a TEXT value "ab"
        // Cassandra clustering prefix: header VInt (2 bits per col, 0=PRESENT) then value
        // For a single TEXT col with 2-byte value: header=0x00, value=[0x61, 0x62]
        let ck_prefix = vec![0x00u8, 0x61, 0x62]; // header=present, value="ab"

        let make_block = |off: u64, w: u64| PromotedIndexBlock {
            first_name: ck_prefix.clone(),
            last_name: ck_prefix.clone(),
            offset: off,
            width: w,
            oss50_separator: None,
        };

        let blocks = vec![
            make_block(0, 70_000),
            make_block(70_000, 68_000),
            make_block(138_000, 65_000),
        ];

        let mut writer = IndexWriter::new();
        let info = writer
            .add_partition_with_promoted(&key, 1234, &blocks)
            .unwrap();
        let bytes = writer.finish().unwrap();
        assert_eq!(bytes.len(), info.entry_size);

        // Parse promoted_index_size VInt at offset = 2(key_len) + 2(key) + pos_vint
        // pos = 1234 → 2-byte VInt (0x80 | ...)
        let pos_vint_len = 2; // 1234 > 127 → 2 bytes
        let promoted_size_vint_start = 2 + 2 + pos_vint_len;
        let promoted_size = parse_vint_simple(&bytes[promoted_size_vint_start..]);
        let (promoted_size_value, promoted_size_vint_bytes) = promoted_size;

        assert!(promoted_size_value > 0, "3 blocks → promoted index present");

        let payload_start = promoted_size_vint_start + promoted_size_vint_bytes;
        let payload_end = payload_start + promoted_size_value as usize;
        assert_eq!(
            payload_end,
            bytes.len(),
            "entry size should exactly cover key + vints + promoted payload"
        );

        // Verify promoted payload structure (NB format):
        // [headerLength VInt][DeletionTime: 12 bytes][count VInt][IndexInfo*3][i32*3]
        //
        // key = [0xAA, 0xBB] (len=2), so headerLength = 2 + 2 + 12 = 16
        let payload = &bytes[payload_start..payload_end];
        let (header_len, hl_bytes) = parse_vint_simple(payload);
        assert_eq!(
            header_len, 16,
            "headerLength = 2 (key_len_prefix) + 2 (key bytes) + 12 (NB DeletionTime) = 16"
        );

        // DeletionTime: [i32 BE: i32::MAX][i64 BE: i64::MIN] = 12 bytes
        let dt_start = hl_bytes;
        let dt_end = dt_start + NB_DELETION_TIME_LIVE_SIZE;
        let ldt = i32::from_be_bytes(payload[dt_start..dt_start + 4].try_into().unwrap());
        let mfda = i64::from_be_bytes(payload[dt_start + 4..dt_end].try_into().unwrap());
        assert_eq!(ldt, i32::MAX, "NB LIVE DeletionTime: ldt = i32::MAX");
        assert_eq!(mfda, i64::MIN, "NB LIVE DeletionTime: mfda = i64::MIN");

        let (count, _) = parse_vint_simple(&payload[dt_end..]);
        assert_eq!(count, 3, "Three IndexInfo blocks");
    }

    /// Block boundary math: threshold crossing produces exactly one new block.
    #[test]
    fn test_block_boundary_math_threshold_crossing() {
        // At exactly COLUMN_INDEX_SIZE_BYTES we cross the threshold and start a new block.
        assert_eq!(COLUMN_INDEX_SIZE_BYTES, 64 * 1024);

        // Simulate: data writer would produce a block at exactly the boundary.
        // Two blocks: one below threshold + one at exactly the threshold.
        let block_at_threshold = PromotedIndexBlock {
            first_name: vec![0x00],
            last_name: vec![0x00],
            offset: COLUMN_INDEX_SIZE_BYTES,
            width: COLUMN_INDEX_SIZE_BYTES,
            oss50_separator: None,
        };
        let block_below = PromotedIndexBlock {
            first_name: vec![0x00],
            last_name: vec![0x00],
            offset: 0,
            width: COLUMN_INDEX_SIZE_BYTES,
            oss50_separator: None,
        };

        // Use a 4-byte raw key for a fixed, predictable headerLength = 2 + 4 + 12 = 18.
        let raw_key_len = 4usize;
        let blocks = vec![block_below, block_at_threshold];
        let payload = serialize_promoted_index(&blocks, raw_key_len);

        // Payload must be non-empty (promoted index present)
        assert!(!payload.is_empty());
        // Count in payload must be 2.
        // Payload layout: [headerLength vint][DeletionTime 12 bytes][count vint][...]
        let (header_len, hl_bytes) = parse_vint_simple(&payload);
        // headerLength = 2 + 4 + 12 = 18
        assert_eq!(
            header_len, 18,
            "headerLength for a 4-byte key = 2 + 4 + 12 = 18"
        );
        let dt_skip = NB_DELETION_TIME_LIVE_SIZE;
        let (count, _) = parse_vint_simple(&payload[hl_bytes + dt_skip..]);
        assert_eq!(count, 2);
    }

    /// Width delta encoding: exact 64KiB block → delta = 0.
    #[test]
    fn test_width_delta_encoding_exact_threshold() {
        // A block that is exactly 64KiB should encode width delta = 0
        let block = PromotedIndexBlock {
            first_name: vec![0x00],
            last_name: vec![0x00],
            offset: 0,
            width: INDEX_INFO_WIDTH_BASE, // 64 KiB
            oss50_separator: None,
        };
        let block2 = PromotedIndexBlock {
            first_name: vec![0x00],
            last_name: vec![0x00],
            offset: INDEX_INFO_WIDTH_BASE,
            width: INDEX_INFO_WIDTH_BASE,
            oss50_separator: None,
        };

        let mut info_bytes: Vec<u8> = Vec::new();
        serialize_index_info(&mut info_bytes, &block);

        // firstName=0x00, lastName=0x00, offset=0x00, width_delta=0x00, endOpenMarker=0x00
        // All are single-byte VInts/bytes when value = 0.
        assert_eq!(info_bytes, vec![0x00, 0x00, 0x00, 0x00, 0x00]);

        // Also verify with 2-block payload that the width field is 0 (use a 4-byte key).
        let payload = serialize_promoted_index(&[block, block2], 4);
        assert!(!payload.is_empty());
    }

    /// Clustering prefix min/max: bytes are preserved verbatim.
    #[test]
    fn test_clustering_prefix_preserved_verbatim() {
        let first_name = vec![0x00u8, 0x01, 0x02]; // arbitrary bytes
        let last_name = vec![0x00u8, 0xFF, 0xFE];

        let block1 = PromotedIndexBlock {
            first_name: first_name.clone(),
            last_name: last_name.clone(),
            offset: 0,
            width: 100_000,
            oss50_separator: None,
        };
        let mut info_bytes: Vec<u8> = Vec::new();
        serialize_index_info(&mut info_bytes, &block1);

        // firstName bytes appear first
        assert_eq!(&info_bytes[0..3], &first_name[..]);
        // lastName bytes appear next
        assert_eq!(&info_bytes[3..6], &last_name[..]);
    }

    // ── Streaming mode tests (Issue #753) ──────────────────────────────────────

    /// BYTE-IDENTICAL PROOF: streaming and in-memory modes produce the same bytes.
    ///
    /// This is the load-bearing correctness anchor for the #753 streaming refactor.
    /// Any divergence is a bug in the streaming path — do NOT relax this assertion.
    #[test]
    fn test_streaming_byte_identical_to_in_memory() {
        let partitions: Vec<(i64, Vec<u8>, u64)> = vec![
            (100, vec![0x00, 0x00, 0x00, 0x01], 0),
            (200, vec![0x00, 0x00, 0x00, 0x02], 256),
            (300, vec![0x00, 0x00, 0x00, 0x03], 512),
            (400, vec![0x00, 0x00, 0x00, 0x04], 12381), // 2-byte VInt offset
            (500, vec![0xBB; 16], 1_000_000_000),       // large offset, UUID key
        ];

        // --- In-memory path ---
        let mut mem_writer = IndexWriter::new();
        let mut mem_infos = Vec::new();
        for (token, key_bytes, offset) in &partitions {
            let key = DecoratedKey::new(*token, key_bytes.clone());
            let info = mem_writer.add_partition(&key, *offset).unwrap();
            mem_infos.push(info);
        }
        let mem_bytes = mem_writer.finish().unwrap();

        // --- Streaming path (writes to a temp file) ---
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let mut stream_writer = IndexWriter::with_sink(path.clone());
        let mut stream_infos = Vec::new();
        for (token, key_bytes, offset) in &partitions {
            let key = DecoratedKey::new(*token, key_bytes.clone());
            let info = stream_writer.add_partition(&key, *offset).unwrap();
            stream_infos.push(info);
        }
        let total_bytes = stream_writer.finish_streaming().unwrap();
        let stream_bytes = std::fs::read(&path).unwrap();

        // Byte-for-byte equality
        assert_eq!(
            mem_bytes, stream_bytes,
            "Streaming Index.db output must be byte-identical to in-memory output"
        );

        // Total bytes written must equal the file size
        assert_eq!(
            total_bytes as usize,
            stream_bytes.len(),
            "finish_streaming() must return exact byte count"
        );

        // IndexEntryInfo (offset + size) must be identical between modes
        assert_eq!(
            mem_infos.len(),
            stream_infos.len(),
            "Entry count must match"
        );
        for (i, (m, s)) in mem_infos.iter().zip(stream_infos.iter()).enumerate() {
            assert_eq!(
                m.index_offset, s.index_offset,
                "Entry {i}: index_offset must match between modes"
            );
            assert_eq!(
                m.entry_size, s.entry_size,
                "Entry {i}: entry_size must match between modes"
            );
        }
    }

    /// BYTE-IDENTICAL PROOF with promoted index blocks (wide partitions).
    ///
    /// Ensures the streaming path is correct even when promoted index payload is
    /// written — the serialized bytes must match the in-memory path exactly.
    #[test]
    fn test_streaming_byte_identical_with_promoted_index() {
        let ck_prefix = vec![0x00u8, 0x61, 0x62]; // header + "ab" value
        let make_block = |off: u64, w: u64| PromotedIndexBlock {
            first_name: ck_prefix.clone(),
            last_name: ck_prefix.clone(),
            offset: off,
            width: w,
            oss50_separator: None,
        };
        let blocks = vec![
            make_block(0, 70_000),
            make_block(70_000, 68_000),
            make_block(138_000, 65_000),
        ];
        let key = DecoratedKey::new(42, vec![0xAA, 0xBB]);

        // --- In-memory ---
        let mut mem_writer = IndexWriter::new();
        let mem_info = mem_writer
            .add_partition_with_promoted(&key, 1234, &blocks)
            .unwrap();
        let mem_bytes = mem_writer.finish().unwrap();

        // --- Streaming ---
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let mut stream_writer = IndexWriter::with_sink(path.clone());
        let stream_info = stream_writer
            .add_partition_with_promoted(&key, 1234, &blocks)
            .unwrap();
        let total_bytes = stream_writer.finish_streaming().unwrap();
        let stream_bytes = std::fs::read(&path).unwrap();

        assert_eq!(
            mem_bytes, stream_bytes,
            "Streaming wide-partition Index.db must be byte-identical to in-memory"
        );
        assert_eq!(total_bytes as usize, stream_bytes.len());
        assert_eq!(mem_info.index_offset, stream_info.index_offset);
        assert_eq!(mem_info.entry_size, stream_info.entry_size);
    }

    /// Streaming mode: entry_count increments correctly.
    #[test]
    fn test_streaming_entry_count() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = IndexWriter::with_sink(tmp.path().to_path_buf());

        assert_eq!(writer.entry_count(), 0);

        for i in 0..5u64 {
            let key = DecoratedKey::new(i as i64 * 100, vec![i as u8]);
            writer.add_partition(&key, i * 64).unwrap();
        }

        assert_eq!(writer.entry_count(), 5);
        let _ = writer.finish_streaming().unwrap();
    }

    /// Streaming mode: index_offset values track position correctly across entries.
    #[test]
    fn test_streaming_index_offset_tracking() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = IndexWriter::with_sink(tmp.path().to_path_buf());

        let key1 = DecoratedKey::new(100, vec![0x01, 0x02, 0x03, 0x04]);
        let info1 = writer.add_partition(&key1, 0).unwrap(); // entry: 2+4+1+1=8

        let key2 = DecoratedKey::new(200, vec![0x05, 0x06]);
        let info2 = writer.add_partition(&key2, 127).unwrap(); // entry: 2+2+1+1=6

        let key3 = DecoratedKey::new(300, vec![0x07]);
        let info3 = writer.add_partition(&key3, 12381).unwrap(); // entry: 2+1+2+1=6

        assert_eq!(info1.index_offset, 0);
        assert_eq!(info1.entry_size, 8);

        assert_eq!(info2.index_offset, 8);
        assert_eq!(info2.entry_size, 6);

        assert_eq!(info3.index_offset, 14);
        assert_eq!(info3.entry_size, 6);

        let total = writer.finish_streaming().unwrap();
        assert_eq!(total, 20, "Total bytes: 8 + 6 + 6 = 20");
    }

    /// Calling finish() on a streaming writer is an error.
    #[test]
    fn test_finish_on_streaming_writer_errors() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let writer = IndexWriter::with_sink(tmp.path().to_path_buf());
        let result = writer.finish();
        assert!(result.is_err(), "finish() on streaming writer must fail");
    }

    /// Calling finish_streaming() on an in-memory writer is an error.
    #[test]
    fn test_finish_streaming_on_in_memory_writer_errors() {
        let writer = IndexWriter::new();
        let result = writer.finish_streaming();
        assert!(
            result.is_err(),
            "finish_streaming() on in-memory writer must fail"
        );
    }

    // ── Counting mode tests (Issue #908 — BTI must not retain Index.db bytes) ──

    /// Finding 2 (roborev #908): the BTI path uses counting mode, which must NOT
    /// accumulate index entry bytes in memory. After writing many partitions the
    /// scratch buffer must be empty (O(one entry), and 0 between entries), in
    /// contrast to in-memory mode which retains the whole Index.db.
    #[test]
    fn test_counting_mode_does_not_retain_entry_bytes() {
        let mut counting = IndexWriter::counting();
        let mut in_memory = IndexWriter::new();

        for i in 0..1000u64 {
            // UUID-sized keys so each entry is ~20 bytes; in-memory would hold ~20 KB.
            let key = DecoratedKey::new(i as i64, vec![(i & 0xff) as u8; 16]);
            counting.add_partition(&key, i * 64).unwrap();
            in_memory.add_partition(&key, i * 64).unwrap();
        }

        // Counting mode retains nothing between entries.
        assert_eq!(
            counting.buffered_len(),
            0,
            "counting mode must not accumulate any Index.db entry bytes"
        );
        // In-memory mode, by contrast, holds the entire serialized Index.db.
        assert!(
            in_memory.buffered_len() > 1000,
            "in-memory mode retains all entries (sanity check the contrast)"
        );
        assert_eq!(counting.entry_count(), 1000);
    }

    /// Counting mode reports byte-identical `index_offset`/`entry_size` bookkeeping
    /// to in-memory mode for the same partition sequence. This is what the BTI write
    /// path relies on for Summary/offset accounting, so it must not drift.
    #[test]
    fn test_counting_mode_offsets_match_in_memory() {
        let partitions: Vec<(i64, Vec<u8>, u64)> = vec![
            (100, vec![0x01, 0x02, 0x03, 0x04], 0),
            (200, vec![0x05, 0x06], 127),
            (300, vec![0x07], 12381),
            (400, vec![0xBB; 16], 1_000_000_000),
        ];

        let mut counting = IndexWriter::counting();
        let mut in_memory = IndexWriter::new();

        for (token, key_bytes, offset) in &partitions {
            let key = DecoratedKey::new(*token, key_bytes.clone());
            let c = counting.add_partition(&key, *offset).unwrap();
            let m = in_memory.add_partition(&key, *offset).unwrap();
            assert_eq!(
                c.index_offset, m.index_offset,
                "counting index_offset must match in-memory"
            );
            assert_eq!(
                c.entry_size, m.entry_size,
                "counting entry_size must match in-memory"
            );
            // Scratch is cleared after every entry in counting mode.
            assert_eq!(counting.buffered_len(), 0);
        }
    }

    /// Counting mode handles promoted-index (wide partition) bookkeeping the same
    /// way in-memory mode does, without retaining the promoted payload bytes.
    #[test]
    fn test_counting_mode_with_promoted_index_offsets_match() {
        let ck_prefix = vec![0x00u8, 0x61, 0x62];
        let make_block = |off: u64, w: u64| PromotedIndexBlock {
            first_name: ck_prefix.clone(),
            last_name: ck_prefix.clone(),
            offset: off,
            width: w,
            oss50_separator: None,
        };
        let blocks = vec![
            make_block(0, 70_000),
            make_block(70_000, 68_000),
            make_block(138_000, 65_000),
        ];
        let key = DecoratedKey::new(42, vec![0xAA, 0xBB]);

        let mut counting = IndexWriter::counting();
        let c = counting
            .add_partition_with_promoted(&key, 1234, &blocks)
            .unwrap();

        let mut in_memory = IndexWriter::new();
        let m = in_memory
            .add_partition_with_promoted(&key, 1234, &blocks)
            .unwrap();

        assert_eq!(c.index_offset, m.index_offset);
        assert_eq!(c.entry_size, m.entry_size);
        assert_eq!(
            counting.buffered_len(),
            0,
            "counting mode must not retain the promoted payload"
        );
    }

    // Helper: parse a Cassandra unsigned VInt and return (value, bytes_consumed)
    fn parse_vint_simple(buf: &[u8]) -> (u64, usize) {
        if buf.is_empty() {
            return (0, 0);
        }
        let first = buf[0];
        if first <= 0x7F {
            return (first as u64, 1);
        }
        // Count leading 1s to get number of extra bytes
        let leading = first.leading_ones() as usize;
        let extra_bits = 8 - leading - 1; // data bits in first byte
        let mask = (1u8 << extra_bits).wrapping_sub(1);
        let mut value = (first & mask) as u64;
        for b in buf.iter().take(leading + 1).skip(1) {
            value = (value << 8) | *b as u64;
        }
        (value, 1 + leading)
    }
}
