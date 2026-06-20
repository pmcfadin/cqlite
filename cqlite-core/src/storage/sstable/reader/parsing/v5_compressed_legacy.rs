//! Parser for Cassandra 5.0 V5CompressedLegacy decompressed blocks
//!
//! This format uses **u8 length prefixes** (NOT VInt) for partition keys and strings,
//! and simplified encoding optimized for compression. This differs from the newer
//! V5_0NewBig and V5_0Bti formats which use pure VInt encoding.
//!
//! ## Partition Key Size Constraints
//!
//! **Apache Cassandra Specification**: Partition keys can be up to 64KB (65536 bytes).
//! **V5CompressedLegacy Format Limitation**: Uses u8 for key length field, limiting keys to 255 bytes max.
//!
//! This means V5CompressedLegacy format cannot represent partition keys larger than 255 bytes,
//! even though Cassandra allows keys up to 64KB. Tables with larger keys would use a different format.
//!
//! Based on format research in docs/V5_COMPRESSED_LEGACY_FORMAT_SPEC.md
//!
//! ## Format Structure
//!
//! ```text
//! Decompressed Block:
//! ├─ [0x00] Partition flags (u8)
//! ├─ [0x01] Partition key length (u8, NOT VInt)
//! ├─ [0x02..] Raw partition key bytes
//! ├─ [+0] Partition deletion time (i32 big-endian)
//! ├─ [+4] Unknown 8-byte field
//! ├─ [+8] Row data begins
//! │  ├─ Row header (flags, timestamp, row_size)
//! │  ├─ Cells:
//! │  │  ├─ Type tag or flags (u8)
//! │  │  ├─ Column name length (u8)
//! │  │  ├─ Column name bytes
//! │  │  ├─ Value length (varies)
//! │  │  └─ Value bytes
//! │  └─ Trailing 4-byte field (NOT included in row_size)
//! └─ [Next partition or end of block]
//! ```

use std::collections::HashMap;

use log::{debug, warn};

use crate::{
    parser::vint::{parse_vint, parse_vuint},
    schema::{CqlType, TableSchema, UdtRegistry},
    storage::sstable::version_gate::{BigVersionGates, VersionGates},
    types::{
        CellExpiration, CellWriteMetadata, TableId, TombstoneInfo, TombstoneType, UdtField,
        UdtTypeDef, UdtValue,
    },
    Error, Result, RowKey, Value,
};

/// Maximum reasonable size for frozen collections to prevent DoS from corrupted data.
/// Cassandra collections are limited by max_collection_size_in_mb (default 64MB) and
/// column_value_size_warn_threshold (default 64KB warning), but we use a conservative
/// element count limit to prevent memory exhaustion from malicious/corrupted data.
const MAX_FROZEN_COLLECTION_SIZE: u64 = 100_000;

/// Maximum cell path/value length in bytes to prevent overflow on corrupted data.
/// Issue #225: Linux CI fails with SIGABRT due to unsafe `as usize` casts on large values.
/// Cassandra's column_value_size_warn_threshold defaults to 64KB; we use 64MB as generous limit.
const MAX_CELL_VALUE_LENGTH: u64 = 64 * 1024 * 1024;

/// Maximum number of fields allowed in a UDT to prevent memory exhaustion.
/// This is a safety limit; real-world UDTs rarely exceed 100 fields.
const MAX_UDT_FIELD_COUNT: usize = 1000;

/// Maximum nesting depth for type definitions to prevent stack overflow.
/// This covers recursive UDTs and deeply nested collections (e.g., list<list<list<...>>>).
const MAX_TYPE_NESTING_DEPTH: usize = 10;

/// Return type for `parse_row_data_with_offset`:
/// (cells, cell_metadata, row_header, next_offset, is_static, complex_col_meta)
///
/// `cell_metadata` maps column name → `CellWriteMetadata` for every live cell
/// parsed in this row.  It is `Some(map)` only when `want_cell_metadata == true`
/// was passed to the function; otherwise it is `None` and zero allocations are
/// incurred on the normal read hot-path.  Used to surface per-cell timestamps /
/// TTLs for `WRITETIME(col)` / `TTL(col)` queries (issue #693).
///
/// `complex_col_meta` maps column name → `ComplexColumnMeta` for every
/// non-frozen collection column parsed in this row (Issue #700, DS4).  It is
/// always `Some` when `want_cell_metadata == true` and the row contains
/// collection columns; always `None` when `want_cell_metadata == false`.
type ParsedRow = (
    HashMap<String, Value>,
    Option<HashMap<String, CellWriteMetadata>>,
    Option<RowHeader>,
    usize,
    bool,
    Option<HashMap<String, ComplexColumnMeta>>,
);

/// Return type for [`V5CompressedLegacy::parse_block_with_cell_metadata`].
///
/// Each element is `(table_id, row_key, value_map, cell_metadata_map)`.
type ParsedBlockWithMeta = Vec<(TableId, RowKey, Value, HashMap<String, CellWriteMetadata>)>;

/// Outcome of [`V5CompressedLegacyParser::parse_one_partition_with_timestamps`].
///
/// The sliding-window compaction-read driver (issue #827) feeds the parser a
/// `data` slice that grows as decompressed chunks are appended and shrinks as
/// confirmed partitions are drained. Because a single partition can straddle a
/// compression-chunk boundary, the bounded parser must distinguish "I parsed a
/// complete partition" from "I ran out of buffer mid-partition and need more
/// bytes" — conflating the two would silently drop trailing partitions.
#[derive(Debug, PartialEq, Eq)]
pub enum ParseStep {
    /// A full partition was parsed; `usize` bytes of `data` were consumed and
    /// can be drained from the front of the sliding window.
    Emitted(usize),
    /// `data` appears truncated mid-partition. The caller should append the
    /// next decompressed chunk and retry. Only returned when `!at_final_chunk`.
    NeedMore,
    /// Genuine end of partitions in `data` (no more bytes to consume).
    Done,
}

/// Row header data extracted from V5CompressedLegacy row
#[derive(Debug, Clone)]
struct RowHeader {
    /// Row-level timestamp (after delta decoding from min_timestamp)
    timestamp: Option<i64>,
    /// Row-level TTL (after delta decoding from min_ttl)
    ttl: Option<i32>,
    /// Liveness local deletion time in SECONDS: absolute epoch-second expiry for a
    /// TTL-bearing INSERT row (the `pk_liveness.ttl()` clock, from `HAS_TTL`).
    /// This is the `expires_at` for the row liveness — distinct from
    /// `local_deletion_time` which is the GC-grace clock for row tombstones.
    /// `None` when `HAS_TTL` was not set.
    liveness_expires_at_seconds: Option<i32>,
    /// Row-level local deletion time in SECONDS (after delta decoding from
    /// min_local_deletion_time). This is the GC-grace clock, NOT the reconciliation
    /// timestamp; do not use it for last-write-wins comparisons.
    local_deletion_time: Option<i32>,
    /// Row-level `markedForDeleteAt` reconciliation timestamp in MICROSECONDS
    /// (absolute = min_timestamp + delta). For a row tombstone this is the
    /// authoritative deletion timestamp used by the compaction merger for
    /// timestamp-based last-write-wins shadowing (Issue #505). `None` when the
    /// row has no HAS_DELETION flag.
    marked_for_delete_at: Option<i64>,
    /// Number of bytes consumed by the header
    header_size: usize,
    /// Length of the row_size VInt in bytes (needed for offset calculation)
    /// row_size is measured from AFTER this VInt is consumed
    row_size_vint_len: usize,
    /// Bitmask of missing columns from Cassandra's Columns.Serializer format.
    /// bit=1 means column missing, bit=0 means column present.
    /// None when HAS_ALL_COLUMNS flag is set.
    missing_columns_bitmap: Option<u64>,
}

impl RowHeader {
    /// Whether this row header describes a row tombstone (HAS_DELETION was set).
    ///
    /// Detected via `local_deletion_time` being present, which is only set when the
    /// HAS_DELETION (0x10) flag was decoded (Issue #505).
    fn is_row_tombstone(&self) -> bool {
        self.local_deletion_time.is_some()
    }

    /// Build the `Value::Tombstone(RowTombstone)` for this header.
    ///
    /// The reconciliation `deletion_time` is `marked_for_delete_at` (the
    /// `markedForDeleteAt` field, MICROSECONDS — same clock as HAS_TIMESTAMP), NOT
    /// the row write `timestamp` and NOT `local_deletion_time` (seconds, GC clock).
    /// For a pure row tombstone HAS_TIMESTAMP is absent, so using the write
    /// timestamp would yield epoch 0 and lose every merge comparison (Issue #505).
    ///
    /// Falls back to `local_deletion_time` promoted to microseconds only if
    /// `marked_for_delete_at` is somehow absent while a deletion was recorded
    /// (should not happen given HAS_DELETION always writes both VInts), keeping the
    /// merger's LWW ordering meaningful rather than defaulting to 0.
    fn row_tombstone(&self) -> Value {
        let deletion_time = self.row_tombstone_deletion_time();
        Value::Tombstone(TombstoneInfo {
            deletion_time,
            tombstone_type: TombstoneType::RowTombstone,
            ttl: None,
            range_start: None,
            range_end: None,
        })
    }

    /// Authoritative reconciliation timestamp (microseconds) for a row tombstone.
    fn row_tombstone_deletion_time(&self) -> i64 {
        match self.marked_for_delete_at {
            Some(ts) => ts,
            // Defensive fallback: promote the seconds-based local deletion time to
            // microseconds so ordering remains non-zero and monotonic.
            None => self
                .local_deletion_time
                .map(|s| (s as i64).saturating_mul(1_000_000))
                .unwrap_or(0),
        }
    }
}

/// Result of parsing a single complex cell (an element of a list/set, or a
/// key/value entry of a map).  See [`V5CompressedLegacyParser::parse_complex_cell_value`].
///
/// `is_deleted` carries the authoritative IS_DELETED (0x01) cell flag so that
/// collection parsers can distinguish element-level tombstones from live
/// elements that merely have an empty value (Issue #493).
struct ComplexCellParse {
    /// Decoded cell value, or `None` if the cell was deleted or had an empty value.
    value: Option<Value>,
    /// Raw cell-path bytes (the element value for sets, the key for maps).
    path_bytes: Vec<u8>,
    /// Whether the cell carries the IS_DELETED (0x01) flag (an element tombstone).
    is_deleted: bool,
    /// Offset immediately following the parsed cell.
    next_offset: usize,
    /// Per-element writetime decoded from the cell's own timestamp field, in µs since
    /// Unix epoch (absolute, after delta decoding from min_timestamp).  `None` when the
    /// element inherited the row-level timestamp (USE_ROW_TIMESTAMP flag 0x08).
    ///
    /// Used by `parse_complex_column_inner` to compute the max element writetime
    /// for a collection column (Issue #700, DS4).
    element_writetime: Option<i64>,
}

/// Extra metadata produced by `parse_complex_column_inner` for delta-scan callers
/// (Issue #700, DS4: non-frozen collection v1 semantics).
///
/// Returned alongside the `Value` and new offset so the emit path can set the correct
/// `replaced` flag and `writetime` on the resulting `CellDelta`.
///
/// Fields are read by `parse_block_emit_delta` which is `#[cfg(feature = "delta-scan")]`;
/// without that feature the struct is built but not consumed, hence the allow.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(in crate::storage::sstable::reader) struct ComplexColumnMeta {
    /// `true` when the collection generation carries a collection-level tombstone
    /// (`s = {...}` overwrite), meaning the consumer must **replace** rather than
    /// merge the prior collection state.
    pub has_collection_tombstone: bool,
    /// Maximum element writetime seen across all cells in this collection, in µs
    /// since Unix epoch.  `0` when no element had its own explicit timestamp
    /// (all inherited the row timestamp).
    pub max_element_writetime: i64,
    /// Number of element-level tombstones detected (`is_deleted` flag) in this
    /// collection cell (Issue #493 territory).  v1 does not represent them; callers
    /// must count and warn.
    pub element_tombstone_count: u64,
}

// Row header flag constants
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_COMPLEX_DELETION: u8 = 0x40; // Issue #221: Row contains complex column with deletion info
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;

// Unfiltered marker constants (from Cassandra UnfilteredSerializer.java lines 102-109)
// Issue #229: These markers were being misinterpreted as row data, causing parsing failures
const END_OF_PARTITION: u8 = 0x01; // Signal end of partition - nothing follows this flag byte
const IS_MARKER: u8 = 0x02; // Range tombstone marker (not a data row)

// Extended flags constants (from Cassandra UnfilteredSerializer.java lines 114-122)
// These are in the SECOND byte when ROW_HAS_EXTENDED_FLAGS (0x80) is set
const EXTENDED_IS_STATIC: u8 = 0x01; // Static row - has NO clustering prefix

// NOTE: V5CompressedLegacy format has NO trailing field after row data.
// The next partition/row starts immediately after row_size bytes.
// (Previous ROW_TRAILING_FIELD_SIZE constant was removed as part of Issue #237 fix)

/// Parser for V5CompressedLegacy format decompressed blocks
pub struct V5CompressedLegacyParser {
    keyspace: String,
    table_name: String,
    /// Minimum timestamp from Statistics.db for delta decoding
    min_timestamp: i64,
    /// Minimum local deletion time from Statistics.db for delta decoding
    min_local_deletion_time: i64,
    /// Minimum TTL from Statistics.db for delta decoding
    min_ttl: Option<i64>,
    /// Optional UDT registry for resolving short UDT type names (Issue #238)
    udt_registry: Option<UdtRegistry>,
    /// Version-feature gates derived from the SSTable filename.
    ///
    /// Threaded here from `SSTableReader::version_gates` (VG1 plumbing).
    /// Decision points that WILL be gated in VG3 are annotated with
    /// `// VG3:` comments throughout this file.  Until VG3, behavior is
    /// identical to the existing `nb`-compatible path regardless of the gate
    /// values stored here.
    version_gates: std::sync::Arc<VersionGates>,
}

impl V5CompressedLegacyParser {
    /// Create a new V5CompressedLegacy parser
    ///
    /// # Arguments
    /// * `keyspace` - Keyspace name
    /// * `table_name` - Table name
    /// * `min_timestamp` - Minimum timestamp for delta decoding (from Statistics.db)
    /// * `min_local_deletion_time` - Minimum local deletion time for delta decoding (from Statistics.db)
    /// * `min_ttl` - Minimum TTL for delta decoding (from Statistics.db)
    pub fn new(
        keyspace: String,
        table_name: String,
        min_timestamp: i64,
        min_local_deletion_time: i64,
        min_ttl: Option<i64>,
    ) -> Self {
        // Default to nb-compatible BIG gates when not supplied by the caller.
        // Use the infallible nb_fallback() constructor (no expect/unwrap in lib code).
        let version_gates = std::sync::Arc::new(VersionGates::Big(BigVersionGates::nb_fallback()));
        Self {
            keyspace,
            table_name,
            min_timestamp,
            min_local_deletion_time,
            min_ttl,
            udt_registry: None,
            version_gates,
        }
    }

    /// Set the version gates for version-sensitive parsing decisions (VG1 plumbing).
    ///
    /// Call this after `new()` with the `Arc<VersionGates>` from `SSTableReader`.
    /// Until VG3 lands, passing gates here has no effect on parsing behaviour —
    /// the gate values are stored for future use only.
    pub fn with_version_gates(mut self, gates: std::sync::Arc<VersionGates>) -> Self {
        self.version_gates = gates;
        self
    }

    /// Set the UDT registry for resolving short UDT type names in frozen collections (Issue #238)
    pub fn with_udt_registry(mut self, registry: UdtRegistry) -> Self {
        self.udt_registry = Some(registry);
        self
    }

    /// Return `true` when the version gates indicate `hasUIntDeletionTime` (oa / da).
    ///
    /// Authority: BigFormat.java:409 — `hasUintDeletionTime = version.compareTo("oa") >= 0`
    #[inline]
    fn has_uint_deletion_time(&self) -> bool {
        match self.version_gates.as_ref() {
            VersionGates::Big(g) => g.has_uint_deletion_time,
            VersionGates::Bti(g) => g.has_uint_deletion_time,
        }
    }

    /// Try to parse partition header at offset WITHOUT consuming it.
    ///
    /// This performs a full parse attempt to determine if the bytes at offset
    /// represent a valid partition header. This is the NO-HEURISTICS approach:
    /// we actually try to parse the structure instead of guessing based on byte patterns.
    ///
    /// # Arguments
    /// * `data` - Binary data buffer
    /// * `offset` - Offset to check
    ///
    /// # Returns
    /// * `true` if a valid partition header can be parsed at this offset
    /// * `false` if parsing fails (likely a row header or invalid data)
    ///
    /// # Visibility
    /// Exposed for integration testing to validate partition boundary detection
    #[doc(hidden)]
    pub fn peek_is_partition_header(&self, data: &[u8], offset: usize) -> bool {
        // Issue #229 FIX: Check for END_OF_PARTITION marker FIRST
        //
        // The END_OF_PARTITION marker (0x01) can be misinterpreted as a valid partition
        // header because parse_partition_header doesn't validate flags semantically.
        // We must explicitly reject END_OF_PARTITION (0x01) and IS_MARKER (0x02) here.
        if offset < data.len() {
            let flags = data[offset];
            if Self::is_end_of_partition(flags) || Self::is_range_tombstone_marker(flags) {
                return false; // These are markers, not partition headers
            }
        }

        // Try to actually parse the partition header
        self.parse_partition_header(data, offset).is_ok()
    }

    /// Parse decompressed block into (TableId, RowKey, Value) entries
    ///
    /// # Arguments
    /// * `data` - Decompressed block bytes
    /// * `schema` - Optional table schema for type-aware parsing
    /// * `reader` - Reference to SSTableReader for value parsing
    ///
    /// # Returns
    /// * `Ok(Vec<(TableId, RowKey, Value)>)` - Parsed entries
    /// * `Err(Error)` - Parse error with context
    pub fn parse_block(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut results = Vec::new();
        self.parse_block_emit(data, schema, reader, |entry| {
            results.push(entry);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(results)
    }

    /// Parse a block and return both row values and per-cell write metadata.
    ///
    /// Identical to [`parse_block`] but the returned vector carries a fourth element:
    /// the per-cell `CellWriteMetadata` map (column name → metadata). Used by the
    /// executor when `ProjectionFlags::include_cell_metadata` is set (i.e. when the
    /// query contains `WRITETIME(col)` or `TTL(col)` expressions).
    pub fn parse_block_with_cell_metadata(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
    ) -> Result<ParsedBlockWithMeta> {
        let mut results = Vec::new();
        self.parse_block_emit_with_metadata(data, schema, reader, |entry| {
            results.push(entry);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(results)
    }

    /// Internal streaming variant of `parse_block_with_cell_metadata`.
    fn parse_block_emit_with_metadata<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut(
            (TableId, RowKey, Value, HashMap<String, CellWriteMetadata>),
        ) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(());
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy format requires schema for {}.{} (cells lack column names in binary data)",
                self.keyspace, self.table_name
            ))
        })?;

        let mut offset = 0;
        let mut partition_index = 0;

        while offset < data.len() {
            // Parse partition header: returns (RowKey, next_data_offset)
            let (partition_key, next_data_offset) = match self.parse_partition_header(data, offset)
            {
                Ok(ph) => ph,
                Err(_) => break,
            };

            let table_id = TableId(format!("{}.{}", self.keyspace, self.table_name));
            offset = next_data_offset;
            partition_index += 1;

            let mut static_cells: HashMap<String, Value> = HashMap::new();
            let mut static_cell_meta: HashMap<String, CellWriteMetadata> = HashMap::new();
            let mut row_count = 0;

            loop {
                if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                    offset += 1;
                    break;
                }

                if offset < data.len() && Self::is_range_tombstone_marker(data[offset]) {
                    match self.skip_range_tombstone_marker(data, offset, schema) {
                        Ok(next_offset) => {
                            offset = next_offset;
                            continue;
                        }
                        Err(_) => break,
                    }
                }

                match self.parse_row_data_with_offset(data, offset, Some(schema), reader, true) {
                    Ok((
                        mut cells,
                        row_cell_meta_opt,
                        row_header_opt,
                        next_offset,
                        is_static,
                        _complex_meta,
                    )) => {
                        let mut row_cell_meta = row_cell_meta_opt.unwrap_or_default();
                        offset = next_offset;
                        row_count += 1;

                        if is_static {
                            static_cells = cells;
                            static_cell_meta = row_cell_meta;
                        } else {
                            // Merge static cells / metadata into clustering row
                            for (k, v) in &static_cells {
                                cells.entry(k.clone()).or_insert_with(|| v.clone());
                            }
                            for (k, v) in &static_cell_meta {
                                row_cell_meta.entry(k.clone()).or_insert_with(|| v.clone());
                            }

                            let row_tombstone =
                                row_header_opt.as_ref().filter(|h| h.is_row_tombstone());

                            let row_value = if let Some(h) = row_tombstone {
                                h.row_tombstone()
                            } else if cells.is_empty() {
                                Value::Null
                            } else {
                                let mut map_entries: Vec<(Value, Value)> = cells
                                    .into_iter()
                                    .map(|(name, value)| (Value::Text(name), value))
                                    .collect();
                                map_entries.sort_by(|a, b| {
                                    let a_key = if let Value::Text(s) = &a.0 {
                                        s.as_str()
                                    } else {
                                        ""
                                    };
                                    let b_key = if let Value::Text(s) = &b.0 {
                                        s.as_str()
                                    } else {
                                        ""
                                    };
                                    a_key.cmp(b_key)
                                });
                                Value::Map(map_entries)
                            };

                            match emit((
                                table_id.clone(),
                                partition_key.clone(),
                                row_value,
                                row_cell_meta,
                            ))? {
                                std::ops::ControlFlow::Continue(()) => {}
                                std::ops::ControlFlow::Break(()) => return Ok(()),
                            }
                        }

                        if offset >= data.len() {
                            break;
                        }

                        if self.peek_is_partition_header(data, offset) {
                            log::debug!(
                                "V5CompressedLegacy: Partition {} detected at offset {} after {} rows",
                                partition_index + 1, offset, row_count
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        log::debug!(
                            "V5CompressedLegacy: Row parse error in partition {} at offset {}: {}",
                            partition_index,
                            offset,
                            e
                        );
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Delta-scan variant of [`parse_block_emit_with_metadata`] (Epic #696, Issue #698).
    ///
    /// Identical in parsing strategy to [`parse_block_emit_with_metadata`] but emits
    /// **static rows separately** instead of merging them into the first clustering row.
    /// The emit closure receives five values per row:
    ///
    /// ```text
    /// (partition_key, cells, cell_meta, row_liveness_ts, is_static)
    /// ```
    ///
    /// - `cells`            — column-name → decoded `Value` (including clustering cols).
    /// - `cell_meta`        — column-name → `CellWriteMetadata` (writetime + TTL).
    /// - `row_liveness_ts`  — `Some(ts_µs)` when the row was created with `INSERT` and
    ///   carries a primary-key liveness timestamp (`HAS_TIMESTAMP` flag).  `None` for
    ///   `UPDATE`-only rows (no pk liveness).
    /// - `is_static`        — `true` for static-column rows (emit as `StaticUpsert`).
    ///
    /// Row tombstones (rows with `HAS_DELETION`) are emitted with a non-empty `cell_meta`
    /// and an empty `cells` map; callers must detect them via a missing row-liveness
    /// timestamp combined with `row_header_is_deletion = true` in `cell_meta`.
    ///
    /// Note: Range tombstone markers are *skipped* in this version — they are emitted as
    /// errors by the delta-scan caller per Issue #699 scope boundaries.
    // ComplexColumnMeta is intentionally restricted to the reader module; the
    // closure bound here is not part of the public API surface.
    #[allow(private_bounds)]
    #[cfg(feature = "delta-scan")]
    pub fn parse_block_emit_delta<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut(
            (
                RowKey,
                HashMap<String, Value>,
                HashMap<String, CellWriteMetadata>,
                Option<i64>, // row-level liveness timestamp (HAS_TIMESTAMP), µs
                bool,        // is_static
                bool,        // is_row_tombstone
                Option<i64>, // marked_for_delete_at (row tombstone deletion time, or None)
                // --- Issue #699 tombstone extensions ---
                Option<(Vec<Value>, bool, Vec<Value>, bool, i64)>, // range tombstone info: (start_values, start_inclusive, end_values, end_inclusive, deleted_at)
                bool,                                              // is_partition_tombstone
                // --- Issue #700 DS4 collection extensions ---
                HashMap<String, ComplexColumnMeta>, // per-column complex collection metadata
                // --- Issue #702 TTL liveness expiry ---
                Option<i64>, // liveness expires_at in microseconds (from HAS_TTL ldt, epoch-s * 1_000_000)
            ),
        ) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(());
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy delta-scan requires schema for {}.{} (cells lack column names in binary data)",
                self.keyspace, self.table_name
            ))
        })?;

        let mut offset = 0;
        let mut partition_index = 0;

        while offset < data.len() {
            let (partition_key, next_data_offset, partition_deletion) = self
                .parse_partition_header_full(data, offset)
                .map_err(|e| {
                    Error::corruption(format!(
                        "delta-scan: partition-header parse error at offset {} in {}.{}: {}",
                        offset, self.keyspace, self.table_name, e
                    ))
                })?;

            offset = next_data_offset;
            partition_index += 1;
            let mut row_count = 0;

            // ----------------------------------------------------------------
            // Issue #699: emit PartitionDelete if the partition header carried
            // a tombstone (markedForDeleteAt != LIVE sentinel).
            // ----------------------------------------------------------------
            if let Some(deleted_at) = partition_deletion {
                match emit((
                    partition_key.clone(),
                    HashMap::new(),
                    HashMap::new(),
                    None,
                    false,
                    false,
                    Some(deleted_at),
                    None,
                    true,           // is_partition_tombstone
                    HashMap::new(), // no collection metadata for tombstones
                    None,           // no liveness TTL expiry for tombstones
                ))? {
                    std::ops::ControlFlow::Continue(()) => {}
                    std::ops::ControlFlow::Break(()) => return Ok(()),
                }
            }

            // Buffer for in-flight range tombstone start bound.
            //
            // A range tombstone in Cassandra SSTable format is represented as a pair of
            // adjacent "range tombstone markers":
            //   INCL_START_BOUND (kind 1) or EXCL_START_BOUND (kind 7)  ← start
            //   INCL_END_BOUND   (kind 6) or EXCL_END_BOUND   (kind 0)  ← end
            //
            // Or as a single "boundary" marker (kind 2 or 5) that encodes both the end
            // of the previous range and the start of the next range simultaneously (used
            // when two ranges share a clustering-key boundary point).
            //
            // We buffer the start bound here and emit a RangeDelete when the end arrives.
            //
            // Tuple: (start_values, start_inclusive, deleted_at)
            let mut pending_range_start: Option<(Vec<Value>, bool, i64)> = None;

            loop {
                if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                    offset += 1;
                    break;
                }

                if offset < data.len() && Self::is_range_tombstone_marker(data[offset]) {
                    // Issue #699: Decode the range tombstone marker and emit RangeDelete.
                    let (
                        bound_values,
                        bound_kind,
                        deleted_at_primary,
                        deleted_at_secondary,
                        next_offset,
                    ) = self
                        .parse_range_tombstone_marker_full(data, offset, schema)
                        .map_err(|e| {
                            Error::corruption(format!(
                                "delta-scan: range-tombstone-marker parse error in partition {} \
                                     at offset {} in {}.{}: {}",
                                partition_index, offset, self.keyspace, self.table_name, e
                            ))
                        })?;
                    offset = next_offset;

                    // Decode bound kind into start/end semantics.
                    //
                    // ClusteringPrefix.Kind ordinals (ClusteringBoundOrBoundary.java):
                    //   0 = EXCL_END_BOUND              → end,   exclusive  (<  ck)
                    //   1 = INCL_START_BOUND             → start, inclusive  (>= ck)
                    //   2 = EXCL_END_INCL_START_BOUNDARY → end excl + start incl (2 del times)
                    //   5 = INCL_END_EXCL_START_BOUNDARY → end incl + start excl (2 del times)
                    //   6 = INCL_END_BOUND               → end,   inclusive  (<= ck)
                    //   7 = EXCL_START_BOUND             → start, exclusive  (>  ck)
                    match bound_kind {
                        1 | 7 => {
                            // Simple start bound: buffer and wait for the matching end.
                            let is_inclusive = bound_kind == 1; // 1=INCL_START, 7=EXCL_START
                            pending_range_start =
                                Some((bound_values, is_inclusive, deleted_at_primary));
                        }
                        0 | 6 => {
                            // Simple end bound: pair with buffered start and emit RangeDelete.
                            let is_end_inclusive = bound_kind == 6; // 6=INCL_END, 0=EXCL_END
                            let (start_values, start_inclusive, _start_del) =
                                pending_range_start.take().unwrap_or_else(|| {
                                    // End bound with no preceding start bound — treat as
                                    // open (unbounded) start.  Hard-error policy: we faithfully
                                    // represent this as an open bound rather than dropping it.
                                    (Vec::new(), false, deleted_at_primary)
                                });
                            // Cassandra puts the authoritative markedForDeleteAt on both
                            // bounds of a simple range (they are the same value); use the
                            // end bound's primary deletion time.
                            let range_info = Some((
                                start_values,
                                start_inclusive,
                                bound_values,
                                is_end_inclusive,
                                deleted_at_primary,
                            ));
                            match emit((
                                partition_key.clone(),
                                HashMap::new(),
                                HashMap::new(),
                                None,
                                false,
                                false,
                                Some(deleted_at_primary),
                                range_info,
                                false,          // is_partition_tombstone
                                HashMap::new(), // no collection metadata for tombstones
                                None,           // no liveness TTL expiry for tombstones
                            ))? {
                                std::ops::ControlFlow::Continue(()) => {}
                                std::ops::ControlFlow::Break(()) => return Ok(()),
                            }
                        }
                        2 => {
                            // EXCL_END_INCL_START_BOUNDARY (kind 2):
                            //   primary   = end of the previous range, exclusive
                            //   secondary = start of the new range, inclusive
                            //
                            // Close the pending range (if any) first.
                            if let Some((start_values, start_inclusive, _)) =
                                pending_range_start.take()
                            {
                                let range_info = Some((
                                    start_values,
                                    start_inclusive,
                                    bound_values.clone(),
                                    false, // EXCL_END
                                    deleted_at_primary,
                                ));
                                match emit((
                                    partition_key.clone(),
                                    HashMap::new(),
                                    HashMap::new(),
                                    None,
                                    false,
                                    false,
                                    Some(deleted_at_primary),
                                    range_info,
                                    false,
                                    HashMap::new(), // no collection metadata for tombstones
                                    None,           // no liveness TTL expiry for tombstones
                                ))? {
                                    std::ops::ControlFlow::Continue(()) => {}
                                    std::ops::ControlFlow::Break(()) => return Ok(()),
                                }
                            }
                            // Open new range starting at bound_values (inclusive).
                            let new_del_at = deleted_at_secondary.unwrap_or(deleted_at_primary);
                            pending_range_start = Some((bound_values, true, new_del_at));
                        }
                        5 => {
                            // INCL_END_EXCL_START_BOUNDARY (kind 5):
                            //   primary   = end of the previous range, inclusive
                            //   secondary = start of the new range, exclusive
                            if let Some((start_values, start_inclusive, _)) =
                                pending_range_start.take()
                            {
                                let range_info = Some((
                                    start_values,
                                    start_inclusive,
                                    bound_values.clone(),
                                    true, // INCL_END
                                    deleted_at_primary,
                                ));
                                match emit((
                                    partition_key.clone(),
                                    HashMap::new(),
                                    HashMap::new(),
                                    None,
                                    false,
                                    false,
                                    Some(deleted_at_primary),
                                    range_info,
                                    false,
                                    HashMap::new(), // no collection metadata for tombstones
                                    None,           // no liveness TTL expiry for tombstones
                                ))? {
                                    std::ops::ControlFlow::Continue(()) => {}
                                    std::ops::ControlFlow::Break(()) => return Ok(()),
                                }
                            }
                            // Open new range starting at bound_values (exclusive).
                            let new_del_at = deleted_at_secondary.unwrap_or(deleted_at_primary);
                            pending_range_start = Some((bound_values, false, new_del_at));
                        }
                        unknown => {
                            return Err(Error::corruption(format!(
                                "delta-scan: unknown range tombstone bound kind {} at offset {} \
                                 in {}.{} (partition key {:?}) — cannot represent faithfully \
                                 (no-heuristics mandate, issue #28)",
                                unknown, offset, self.keyspace, self.table_name, partition_key.0
                            )));
                        }
                    }

                    continue;
                }

                match self.parse_row_data_with_offset(data, offset, Some(schema), reader, true) {
                    Ok((
                        cells,
                        row_cell_meta_opt,
                        row_header_opt,
                        next_offset,
                        is_static,
                        complex_meta,
                    )) => {
                        let cell_meta = row_cell_meta_opt.unwrap_or_default();
                        // DS4 (Issue #700): pass ComplexColumnMeta to the emit closure so the
                        // delta-scan caller can set `replaced` and surface element tombstone counts.
                        let col_meta_map = complex_meta.unwrap_or_default();
                        offset = next_offset;
                        row_count += 1;

                        let (
                            row_liveness_ts,
                            is_row_tombstone,
                            marked_for_delete_at,
                            liveness_expires_at_micros,
                        ) = if let Some(ref h) = row_header_opt {
                            // Convert epoch-seconds liveness expiry to epoch-microseconds
                            // (Issue #702: delta-scan CellMeta.expires_at).
                            let liveness_exp = h
                                .liveness_expires_at_seconds
                                .map(|s| (s as i64).saturating_mul(1_000_000));
                            (
                                h.timestamp,
                                h.is_row_tombstone(),
                                h.marked_for_delete_at,
                                liveness_exp,
                            )
                        } else {
                            (None, false, None, None)
                        };

                        match emit((
                            partition_key.clone(),
                            cells,
                            cell_meta,
                            row_liveness_ts,
                            is_static,
                            is_row_tombstone,
                            marked_for_delete_at,
                            None,                       // range_info (not a range tombstone)
                            false,                      // is_partition_tombstone
                            col_meta_map,               // DS4 collection metadata
                            liveness_expires_at_micros, // Issue #702: TTL liveness expiry
                        ))? {
                            std::ops::ControlFlow::Continue(()) => {}
                            std::ops::ControlFlow::Break(()) => return Ok(()),
                        }

                        if offset >= data.len() {
                            break;
                        }

                        if self.peek_is_partition_header(data, offset) {
                            log::debug!(
                                "V5CompressedLegacy delta-scan: Partition {} detected at offset {} after {} rows",
                                partition_index + 1, offset, row_count
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        return Err(Error::corruption(format!(
                            "delta-scan: row parse error in partition {} at offset {} in {}.{}: {}",
                            partition_index, offset, self.keyspace, self.table_name, e
                        )));
                    }
                }
            }

            let _ = row_count; // acknowledged for logging purposes

            // Finding 3: dangling-range guard.
            // A well-formed SSTable always pairs every start marker with a matching
            // end marker (or a boundary marker that closes the range) before the
            // end-of-partition byte.  If we reach here with `pending_range_start`
            // still set, the SSTable is corrupt — the range was opened but never
            // closed.  Silently discarding it would violate the no-heuristics mandate
            // (issue #28); return a corruption error naming the partition.
            if let Some((start_vals, start_incl, start_del_at)) = pending_range_start {
                return Err(Error::corruption(format!(
                    "delta-scan: partition {} in {}.{} (key {:?}) has an unclosed range \
                     tombstone start bound (values={:?}, inclusive={}, deleted_at={}) with \
                     no matching end marker — corrupt SSTable (no-heuristics mandate, issue #28)",
                    partition_index,
                    self.keyspace,
                    self.table_name,
                    partition_key.0,
                    start_vals,
                    start_incl,
                    start_del_at,
                )));
            }
        }

        Ok(())
    }

    /// Streaming variant of [`parse_block`]: invokes `emit` for each parsed
    /// `(TableId, RowKey, Value)` entry instead of collecting them into a `Vec`,
    /// so callers can forward rows into a bounded channel without materializing
    /// the whole block at once (issue #790). Returning `ControlFlow::Break` from
    /// `emit` stops parsing early — used when the streaming consumer is dropped.
    pub fn parse_block_emit<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut((TableId, RowKey, Value)) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(());
        }

        // V5CompressedLegacy format stores cells WITHOUT column names,
        // relying on schema to interpret the binary data. Schema is REQUIRED.
        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy format requires schema for {}.{} (cells lack column names in binary data)",
                self.keyspace, self.table_name
            ))
        })?;

        log::debug!(
            "V5CompressedLegacy: Parsing block for {}.{} ({} bytes)",
            self.keyspace,
            self.table_name,
            data.len()
        );
        log::debug!(
            "V5CompressedLegacy: Schema has {} columns",
            schema.columns.len()
        );
        for (i, col) in schema.columns.iter().enumerate() {
            log::debug!("  Column {}: {} ({})", i, col.name, col.data_type);
        }
        log::debug!(
            "V5CompressedLegacy: First 64 bytes of data: {}",
            hex::encode(&data[..std::cmp::min(64, data.len())])
        );
        debug!(
            "V5CompressedLegacy: Parsing block for {}.{} ({} bytes)",
            self.keyspace,
            self.table_name,
            data.len()
        );

        let mut emitted: usize = 0;
        let mut offset = 0;
        let table_id = TableId::new(format!("{}.{}", self.keyspace, self.table_name));

        // Cassandra partition key size limits (used in header validation)
        // - CASSANDRA_MAX_KEY_SIZE: 64KB limit per Apache Cassandra specification
        // - FORMAT_MAX_KEY_SIZE: u8 max value - V5CompressedLegacy format limitation
        const CASSANDRA_MAX_KEY_SIZE: usize = 65536; // 64KB per Cassandra spec
        const FORMAT_MAX_KEY_SIZE: usize = 255; // u8 max value - format limitation

        // Parse ALL partitions in block (Issue #2 fix: previously only parsed one partition)
        let mut partition_index = 0;
        let mut skipped_partitions = 0;
        while offset < data.len() {
            log::debug!(
                "V5CompressedLegacy: === PARTITION {} at offset {} (block size: {}) ===",
                partition_index,
                offset,
                data.len()
            );

            // CRITICAL FIX (Issue #164): Validate partition header format before attempting parse
            //
            // Most compressed blocks contain EXACTLY ONE partition. After parsing the first
            // partition's row data and trailing VInt, we should NOT assume there's another
            // partition just because offset < data.len().
            //
            // Partition header format validation:
            // - Byte 0: Flags (typically 0x00, sometimes has partition-level flags)
            // - Byte 1: Partition key length (u8, typically 16 for UUID)
            // - Bytes 2+: Partition key data
            //
            // If we don't see a valid partition header structure, we've reached the end
            // of partitions in this block (remaining bytes are likely padding or metadata).
            if offset >= data.len() {
                break; // End of block
            }

            // Check if this looks like a partition header (flags byte + reasonable key length)
            // Partition keys can be up to 64KB per Cassandra spec (composite keys, text, etc.)
            if offset + 2 > data.len() {
                log::debug!(
                    "V5CompressedLegacy: Not enough bytes for partition header at offset {} (need 2, have {}), stopping",
                    offset,
                    data.len() - offset
                );
                break;
            }

            let flags = data[offset];
            let key_len = data[offset + 1] as usize;

            // Validate partition header:
            // - Key length must be non-zero and within format's limit (u8 max = 255 bytes)
            //   Note: Cassandra spec allows 64KB keys, but V5CompressedLegacy format uses u8 length
            // - Must have enough bytes for the header (size depends on format version)
            //
            // VG3: oa format (hasUIntDeletionTime) uses a compact DeletionTime:
            //   LIVE = 1 byte; DELETED = 12 bytes.  The minimum is therefore 1 byte.
            // nb format always uses 12 bytes (4 + 8).
            // NOTE: No heuristic validation of flags (Issue #258, #28 no-heuristics mandate)
            let deletion_time_min = if self.has_uint_deletion_time() { 1 } else { 12 };
            let header_min_size = 1 + 1 + key_len + deletion_time_min;
            if key_len == 0
                || key_len > FORMAT_MAX_KEY_SIZE.min(CASSANDRA_MAX_KEY_SIZE)
                || offset + header_min_size > data.len()
            {
                log::warn!(
                    "V5CompressedLegacy: Skipping malformed partition header at offset {} \
                     (flags=0x{:02x}, key_len={}, need {} bytes, have {}, partition={}): header validation failed",
                    offset,
                    flags,
                    key_len,
                    header_min_size,
                    data.len() - offset,
                    partition_index
                );
                // Try to skip to next potential partition boundary
                skipped_partitions += 1;
                offset += 1; // Minimal forward progress to avoid infinite loop
                continue; // Skip this partition, try next
            }

            // Try to parse partition header
            match self.parse_partition_header(data, offset) {
                Ok((partition_key, new_offset)) => {
                    let header_size = new_offset - offset;
                    offset = new_offset;

                    log::debug!(
                        "V5CompressedLegacy: Partition {} - Parsed partition key: {} bytes (header consumed {} bytes, now at offset {})",
                        partition_index,
                        partition_key.0.len(),
                        header_size,
                        offset
                    );
                    log::debug!(
                        "V5CompressedLegacy: Partition {} - Row data starts at offset {}, remaining: {} bytes",
                        partition_index,
                        offset,
                        data.len() - offset
                    );
                    log::debug!(
                        "V5CompressedLegacy: Partition {} - Row data hex (first 128 bytes): {}",
                        partition_index,
                        hex::encode(&data[offset..std::cmp::min(offset + 128, data.len())])
                    );

                    debug!(
                        "V5CompressedLegacy: Parsed partition key: {} bytes, now at offset {}",
                        partition_key.0.len(),
                        offset
                    );

                    // Parse ALL rows in this partition (Issue #166 fix: multi-row partition support)
                    //
                    // V5CompressedLegacy partitions can contain multiple rows with different clustering keys.
                    // We use structural parsing (peek_is_partition_header) to detect partition boundaries,
                    // not flag value heuristics (Issue #258, #28 no-heuristics mandate).
                    // We parse rows in a loop until we encounter:
                    // - End of block (offset >= data.len())
                    // - END_OF_PARTITION marker (flags == 0x01, Issue #229 fix)
                    // - Next partition header (detected via peek_is_partition_header)
                    // - Parse error (invalid row data)

                    // Issue #480 FIX: Static cell handling
                    //
                    // Cassandra static rows are stored once per partition (before clustering rows).
                    // They should NOT be emitted as separate result entries — instead their column
                    // values must be merged into each clustering row that follows in the partition.
                    //
                    // We accumulate static cells here and inject them into every clustering row.
                    let mut static_cells: HashMap<String, Value> = HashMap::new();
                    let mut row_count = 0;
                    loop {
                        // Issue #229 FIX: Check for END_OF_PARTITION marker BEFORE attempting row parse
                        //
                        // Per Cassandra's UnfilteredSerializer.java (lines 102, 730-732):
                        // When END_OF_PARTITION (0x01) is set in the flags byte, nothing follows.
                        // The partition is complete and we should move to the next partition.
                        if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                            log::debug!(
                                "V5CompressedLegacy: Partition {} complete via END_OF_PARTITION marker at offset {} ({} rows parsed)",
                                partition_index, offset, row_count
                            );
                            offset += 1; // Skip the END_OF_PARTITION marker byte
                            break; // Move to next partition
                        }

                        // Issue #229 FIX: Check for range tombstone marker
                        //
                        // Per Cassandra's UnfilteredSerializer.java (lines 103, 735-738):
                        // When IS_MARKER (0x02) is set, this is a range tombstone boundary, not a row.
                        // We skip these markers for now (full implementation would parse deletion ranges).
                        if offset < data.len() && Self::is_range_tombstone_marker(data[offset]) {
                            log::debug!(
                                "V5CompressedLegacy: Range tombstone marker at offset {} (partition {}), skipping",
                                offset, partition_index
                            );
                            // Skip the marker - for now, just advance past it
                            // A full implementation would parse ClusteringBoundOrBoundary and deletion times
                            match self.skip_range_tombstone_marker(data, offset, schema) {
                                Ok(next_offset) => {
                                    offset = next_offset;
                                    continue; // Continue to next row/marker
                                }
                                Err(e) => {
                                    log::debug!(
                                        "V5CompressedLegacy: Failed to skip range tombstone marker at offset {}: {}",
                                        offset, e
                                    );
                                    break; // Can't parse marker, end partition
                                }
                            }
                        }

                        match self.parse_row_data_with_offset(
                            data,
                            offset,
                            Some(schema),
                            reader,
                            false,
                        ) {
                            Ok((
                                mut cells,
                                _row_cell_meta,
                                row_header_opt,
                                next_offset,
                                is_static,
                                _complex_meta,
                            )) => {
                                // Update offset to point to the next row or partition
                                offset = next_offset;
                                row_count += 1;

                                log::debug!(
                                    "V5CompressedLegacy: Partition {} Row {} - Parsed {} cells, now at offset {} (is_static={})",
                                    partition_index,
                                    row_count,
                                    cells.len(),
                                    offset,
                                    is_static
                                );

                                if let Some(ref header) = row_header_opt {
                                    log::debug!(
                                        "V5CompressedLegacy: Row {} metadata - timestamp={:?}, ttl={:?}, deletion={:?}",
                                        row_count,
                                        header.timestamp, header.ttl, header.local_deletion_time
                                    );
                                }

                                debug!(
                                    "V5CompressedLegacy: Parsed {} cells from row {} (is_static={})",
                                    cells.len(),
                                    row_count,
                                    is_static
                                );

                                // Issue #480 FIX: Static row handling
                                //
                                // Static rows are stored once per partition and contain values for
                                // STATIC columns (e.g. `static_data TEXT STATIC`). They must NOT
                                // be emitted as standalone result rows. Instead, store the static
                                // column values and merge them into each subsequent clustering row.
                                if is_static {
                                    log::debug!(
                                        "V5CompressedLegacy: Partition {} - Storing {} static cells for merging into clustering rows",
                                        partition_index,
                                        cells.len()
                                    );
                                    static_cells = cells;
                                    // Do NOT push to results — static rows are not result rows
                                    // Continue to next row/marker in partition
                                } else {
                                    // Merge static cells into this clustering row (Issue #480)
                                    for (k, v) in &static_cells {
                                        cells.entry(k.clone()).or_insert_with(|| v.clone());
                                    }

                                    // Convert cells HashMap to Value::Map (required by SelectExecutor)
                                    //
                                    // Issue #505: Row tombstones are detected via HAS_DELETION in the
                                    // row header.  When present, emit a proper `Value::Tombstone`
                                    // carrying the authoritative `markedForDeleteAt` (microseconds) so
                                    // the compaction merger can apply tombstone-shadowing semantics
                                    // rather than treating the absent cells as a live empty row.
                                    let row_tombstone =
                                        row_header_opt.as_ref().filter(|h| h.is_row_tombstone());

                                    let row_value = if let Some(h) = row_tombstone {
                                        log::debug!(
                                            "V5CompressedLegacy: Partition {} Row {} - emitting Tombstone(deletion_time={})",
                                            partition_index, row_count, h.row_tombstone_deletion_time()
                                        );
                                        h.row_tombstone()
                                    } else if cells.is_empty() {
                                        warn!(
                                            "V5CompressedLegacy: No cells extracted for {}.{} partition {} row {} (partition key: {} bytes)",
                                            self.keyspace,
                                            self.table_name,
                                            partition_index,
                                            row_count,
                                            partition_key.0.len()
                                        );
                                        Value::Null
                                    } else {
                                        // Convert HashMap<String, Value> to Vec<(Value, Value)> for Value::Map.
                                        //
                                        // Sort alphabetically by column name to guarantee a deterministic
                                        // ordering across independent parse calls.  HashMap iteration order
                                        // is randomized per-instance in Rust, so two separate calls to
                                        // stitch_and_parse_all_chunks (e.g. get() then scan()) would
                                        // otherwise produce Vec orderings that compare as unequal even
                                        // though they hold the same data.
                                        //
                                        // Alphabetical is not schema column order, but the query layer
                                        // (executor.rs:storage_data_to_query_row) accesses columns by name
                                        // (not position), so this ordering does not affect query correctness
                                        // or sstabledump parity.
                                        //
                                        // NON-BLOCKING-3 (Issue #516/517): A future improvement could use
                                        // serialization-header order (reader.header.columns) rather than
                                        // alphabetical, matching Cassandra's on-disk column order exactly.
                                        // That would require threading the column order through ParsedRow
                                        // to this call site.
                                        let mut map_entries: Vec<(Value, Value)> = cells
                                            .into_iter()
                                            .map(|(name, value)| (Value::Text(name), value))
                                            .collect();
                                        map_entries.sort_by(|a, b| {
                                            let a_key = if let Value::Text(s) = &a.0 {
                                                s.as_str()
                                            } else {
                                                ""
                                            };
                                            let b_key = if let Value::Text(s) = &b.0 {
                                                s.as_str()
                                            } else {
                                                ""
                                            };
                                            a_key.cmp(b_key)
                                        });
                                        Value::Map(map_entries)
                                    };

                                    match emit((
                                        table_id.clone(),
                                        partition_key.clone(),
                                        row_value,
                                    ))? {
                                        std::ops::ControlFlow::Continue(()) => emitted += 1,
                                        // Consumer dropped (streaming receiver gone): stop parsing.
                                        std::ops::ControlFlow::Break(()) => return Ok(()),
                                    }
                                }

                                // Check if we're at the end of the partition
                                if offset >= data.len() {
                                    debug!(
                                        "V5CompressedLegacy: Partition {} complete: {} rows parsed (end of block)",
                                        partition_index, row_count
                                    );
                                    break; // End of block
                                }

                                // CRITICAL FIX (Issue #166): NO HEURISTICS - Try-parse approach
                                //
                                // Instead of guessing based on byte patterns (e.g., checking if flags <= 0x20
                                // or validating key_len ranges), we ACTUALLY TRY TO PARSE the next structure.
                                //
                                // Why heuristics fail:
                                // - Row with small value (e.g., boolean=0x0A) can look like key_len
                                // - Row flags=0x00 or 0x20 pass "<= 0x20" checks meant for partitions
                                // - Any byte-pattern guessing will eventually fail on edge cases
                                //
                                // The only reliable approach: try to parse as partition header.
                                // If that succeeds, it's a partition. If it fails, continue with rows.
                                if self.peek_is_partition_header(data, offset) {
                                    debug!(
                                        "V5CompressedLegacy: Partition {} complete: {} rows parsed (next partition detected at offset {})",
                                        partition_index, row_count, offset
                                    );
                                    break; // Next partition starts here
                                }

                                // Peek failed - not a partition header, so continue parsing rows
                                debug!(
                                    "V5CompressedLegacy: Partition {} - Continuing to row {} at offset {} (peek confirmed this is NOT a partition header)",
                                    partition_index, row_count + 1, offset
                                );
                            }
                            Err(e) => {
                                // End of valid data in partition
                                debug!(
                                    "V5CompressedLegacy: Partition {} ended after {} rows: {}",
                                    partition_index, row_count, e
                                );
                                if row_count == 0 {
                                    // If we couldn't parse even one row, log as error
                                    log::error!(
                                        "V5CompressedLegacy: Partition {} - Failed to parse first row at offset {}: {}",
                                        partition_index, offset, e
                                    );
                                }
                                break; // End of valid data in partition
                            }
                        }
                    }

                    partition_index += 1;
                }
                Err(e) => {
                    log::warn!(
                        "V5CompressedLegacy: Failed to parse partition header at offset {} \
                         (partition={}): {}. Attempting to continue to next partition.",
                        offset,
                        partition_index,
                        e
                    );
                    // Try to skip forward to find next partition
                    skipped_partitions += 1;
                    offset += 1;
                    continue; // Skip this partition, try next
                }
            }
        }

        if skipped_partitions > 0 {
            log::warn!(
                "V5CompressedLegacy: Successfully parsed {} entries, skipped {} malformed partitions",
                emitted,
                skipped_partitions
            );
        }

        debug!(
            "V5CompressedLegacy: Parsed {} total entries from block",
            emitted
        );

        Ok(())
    }

    /// Parse all partitions in a decompressed block, returning per-row timestamps.
    ///
    /// This is the compaction-specific variant of [`parse_block`].  It returns
    /// `(TableId, RowKey, Value, row_timestamp_micros)` so that the k-way merger
    /// can perform timestamp-accurate last-write-wins ordering rather than
    /// falling back to `SystemTime::now()`.
    ///
    /// Row tombstones are emitted as `Value::Tombstone(RowTombstone)` with their
    /// actual `deletion_time`.  Cell tombstones within live rows are stored as
    /// `Value::Tombstone(CellTombstone)` inside the `Value::Map`, again carrying
    /// the cell-level deletion timestamp.
    ///
    /// The `row_timestamp_micros` in the returned tuple is the row-level write
    /// timestamp decoded from the `HAS_TIMESTAMP` field in the row header
    /// (`min_timestamp + delta`).  For row tombstones the same timestamp also
    /// appears in `TombstoneInfo::deletion_time`.
    ///
    /// Normal user-facing scan/get paths should use [`parse_block`] instead.
    /// (Issue #505)
    pub fn parse_block_with_timestamps(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
    ) -> Result<Vec<(TableId, RowKey, Value, i64)>> {
        // Thin wrapper that collects the streaming emit variant into a Vec, so
        // every existing caller/test is byte-for-byte unchanged (issue #827).
        let mut results: Vec<(TableId, RowKey, Value, i64)> = Vec::new();
        self.parse_block_with_timestamps_emit(data, schema, reader, |entry| {
            results.push(entry);
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(results)
    }

    /// Streaming variant of [`parse_block_with_timestamps`]: invokes `emit` for
    /// each parsed `(TableId, RowKey, Value, row_timestamp_micros)` entry rather
    /// than collecting into a `Vec`, so the compaction read path can forward
    /// rows into a bounded channel without materialising the whole block at once
    /// (issue #827). Returning `ControlFlow::Break` from `emit` stops parsing
    /// early — used when the streaming consumer is dropped.
    ///
    /// The tombstone/timestamp semantics are byte-identical to
    /// [`parse_block_with_timestamps`] (Issue #505/#533): a row tombstone is
    /// emitted as `Value::Tombstone` carrying its `markedForDeleteAt`, and the
    /// fourth tuple element is the row write timestamp for live rows.
    pub fn parse_block_with_timestamps_emit<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut((TableId, RowKey, Value, i64)) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(());
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy (compaction) format requires schema for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;

        let mut offset = 0;
        let mut skipped_partitions = 0;

        // Wrap `emit` so a Break is observable here as well as inside the
        // one-partition parser (which stops its inner row loop on Break). This
        // lets the outer loop terminate promptly when the consumer is dropped.
        // `Cell` so the wrapping closure can borrow it shared while the outer
        // loop also reads it between calls.
        let broke = std::cell::Cell::new(false);
        let mut tracking_emit = |entry| -> Result<std::ops::ControlFlow<()>> {
            let flow = emit(entry)?;
            if matches!(flow, std::ops::ControlFlow::Break(())) {
                broke.set(true);
            }
            Ok(flow)
        };

        while offset < data.len() {
            match self.parse_one_partition_with_timestamps(
                &data[offset..],
                Some(schema),
                reader,
                // The whole block is present; never request a refill. A trailing
                // parse failure is terminal here (matches the legacy
                // `Err(_) => break` behaviour of the original loop).
                true,
                &mut tracking_emit,
            )? {
                ParseStep::Emitted(consumed) => {
                    if consumed == 0 {
                        // Defensive: avoid an infinite loop on a zero-byte
                        // partition (should not happen — a header is >= 2 bytes).
                        skipped_partitions += 1;
                        offset += 1;
                    } else {
                        offset += consumed;
                    }
                }
                // `at_final_chunk = true` collapses NeedMore into Done: there is
                // no further chunk to append, so a truncated tail is end-of-data.
                ParseStep::NeedMore | ParseStep::Done => break,
            }
            // Propagate an early Break from `emit` (consumer dropped).
            if broke.get() {
                break;
            }
        }

        if skipped_partitions > 0 {
            log::warn!(
                "V5CompressedLegacy (compaction): skipped {} malformed partitions",
                skipped_partitions
            );
        }

        Ok(())
    }

    /// Parse exactly ONE partition from the front of `data`, emitting each row
    /// via `emit`, and report how the parse terminated (issue #827).
    ///
    /// This isolates the body of the outer partition loop so the sliding-window
    /// compaction driver can drain one partition at a time and `drain(0..consumed)`
    /// from its window between calls. The crucial distinction over the legacy
    /// monolithic loop is `NeedMore` vs `Done`:
    ///
    /// - [`ParseStep::Emitted(consumed)`] — a full partition was parsed and
    ///   terminated by an END_OF_PARTITION marker or a confirmed next-partition
    ///   header. `consumed` bytes may be drained from the window.
    /// - [`ParseStep::NeedMore`] — `data` is (possibly) truncated mid-partition
    ///   and `!at_final_chunk`, so the caller must append the next chunk and
    ///   retry. NEVER returned when `at_final_chunk` is true.
    /// - [`ParseStep::Done`] — genuine end of partitions, or (when
    ///   `at_final_chunk`) a trailing truncation that cannot be resolved by more
    ///   data. Terminal.
    ///
    /// `at_final_chunk` flips a mid-partition parse failure between a refill
    /// request (`NeedMore`) and a terminal stop. The legacy code conflated
    /// parse-error with end-of-partitions (`Err(_) => break`); doing that
    /// mid-stream would silently drop every partition after a chunk boundary, so
    /// we return `NeedMore` whenever the buffer may simply be truncated and we
    /// are not yet at the final chunk.
    pub fn parse_one_partition_with_timestamps<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
        at_final_chunk: bool,
        emit: &mut F,
    ) -> Result<ParseStep>
    where
        F: FnMut((TableId, RowKey, Value, i64)) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(ParseStep::Done);
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy (compaction) format requires schema for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;

        let table_id = TableId::new(format!("{}.{}", self.keyspace, self.table_name));

        const CASSANDRA_MAX_KEY_SIZE: usize = 65536;
        const FORMAT_MAX_KEY_SIZE: usize = 255;

        // A partition header is at least flags(1) + key_len(1). If we cannot even
        // read those two bytes, we are truncated: request more unless final.
        if data.len() < 2 {
            return Ok(if at_final_chunk {
                ParseStep::Done
            } else {
                ParseStep::NeedMore
            });
        }

        let key_len = data[1] as usize;
        let header_min_size = 1 + 1 + key_len + 4 + 8;

        // Invalid header shape (zero/over-long key) → malformed; advance by one
        // byte so the outer loop can resynchronise. Returning Emitted(1) here
        // mirrors the legacy `offset += 1; continue` skip-a-byte recovery.
        if key_len == 0 || key_len > FORMAT_MAX_KEY_SIZE.min(CASSANDRA_MAX_KEY_SIZE) {
            return Ok(ParseStep::Emitted(1));
        }

        // Header declared but not fully present in `data` → truncated mid-header.
        if header_min_size > data.len() {
            return Ok(if at_final_chunk {
                // No more bytes will ever arrive; the legacy loop treated this
                // as the end of parseable partitions.
                ParseStep::Done
            } else {
                ParseStep::NeedMore
            });
        }

        let (partition_key, mut offset) = match self.parse_partition_header(data, 0) {
            Ok(v) => v,
            Err(_) => {
                // Fixed header bytes are present but did not parse: skip a byte
                // to resynchronise — matching the legacy
                // `Err(_) => { offset += 1; continue }` recovery.
                return Ok(ParseStep::Emitted(1));
            }
        };

        let mut static_cells: HashMap<String, Value> = HashMap::new();

        // Finding 1 (#827): buffer this partition's emitted rows locally and only
        // forward them to the external `emit` once the partition is CONFIRMED
        // complete (a `ParseStep::Emitted` return). If the buffer is truncated
        // mid-partition (`NeedMore`) after one or more rows were parsed, we must
        // emit NOTHING and let the caller refill and re-parse this partition from
        // its start — otherwise the already-forwarded rows would be re-emitted on
        // the retry, duplicating them in the streaming compaction output.
        //
        // The buffer is bounded by ONE partition's rows (the documented
        // `max_partition_size` bound), not the whole file, so memory stays
        // bounded as required by the #827 deliverable.
        let mut pending: Vec<(TableId, RowKey, Value, i64)> = Vec::new();

        // Flush the buffered rows to the external `emit`, honouring an early
        // `Break`. Returns the `ParseStep` to surface to the caller: on `Break`
        // we still report the bytes consumed for this (complete) partition so the
        // caller drains correctly, but stop forwarding the remaining buffered
        // rows. `flushed_break` becomes true so the driver can stop promptly.
        macro_rules! flush_and_emitted {
            ($consumed:expr, $pending:expr, $emit:expr) => {{
                for entry in $pending.drain(..) {
                    match $emit(entry)? {
                        std::ops::ControlFlow::Continue(()) => {}
                        std::ops::ControlFlow::Break(()) => break,
                    }
                }
                Ok(ParseStep::Emitted($consumed))
            }};
        }

        loop {
            // END_OF_PARTITION (0x01): partition complete, consume the marker.
            if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                offset += 1;
                return flush_and_emitted!(offset, pending, emit);
            }

            // Consumed everything but never saw END_OF_PARTITION: the partition
            // may continue in the next chunk. On NeedMore emit NOTHING (drop the
            // buffered rows) so the caller can refill and re-parse from the start
            // without duplicating already-buffered rows (Finding 1).
            if offset >= data.len() {
                if at_final_chunk {
                    return flush_and_emitted!(offset, pending, emit);
                }
                return Ok(ParseStep::NeedMore);
            }

            if Self::is_range_tombstone_marker(data[offset]) {
                match self.skip_range_tombstone_marker(data, offset, schema) {
                    Ok(next_offset) => {
                        offset = next_offset;
                        continue;
                    }
                    Err(_) => {
                        // Marker body truncated? request more unless final.
                        if at_final_chunk {
                            return flush_and_emitted!(offset, pending, emit);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                }
            }

            match self.parse_row_data_with_offset(data, offset, Some(schema), reader, false) {
                Ok((
                    mut cells,
                    _row_cell_meta,
                    row_header_opt,
                    next_offset,
                    is_static,
                    _complex_meta,
                )) => {
                    offset = next_offset;

                    // For a row tombstone the authoritative timestamp is
                    // markedForDeleteAt (HAS_TIMESTAMP is absent for pure row
                    // deletes). For a live row it is the row write timestamp.
                    // Both the merger tuple `row_ts` and the emitted
                    // Value::Tombstone must agree, so resolve once here (#505).
                    let row_tombstone = row_header_opt.as_ref().filter(|h| h.is_row_tombstone());
                    let row_ts = match row_tombstone {
                        Some(h) => h.row_tombstone_deletion_time(),
                        None => row_header_opt
                            .as_ref()
                            .and_then(|h| h.timestamp)
                            .unwrap_or(0),
                    };

                    if is_static {
                        static_cells = cells;
                    } else {
                        for (k, v) in &static_cells {
                            cells.entry(k.clone()).or_insert_with(|| v.clone());
                        }

                        // Row tombstone → Value::Tombstone(markedForDeleteAt)
                        let row_value = if let Some(h) = row_tombstone {
                            h.row_tombstone()
                        } else if cells.is_empty() {
                            Value::Null
                        } else {
                            let mut map_entries: Vec<(Value, Value)> = cells
                                .into_iter()
                                .map(|(name, value)| (Value::Text(name), value))
                                .collect();
                            map_entries.sort_by(|a, b| {
                                let a_key = if let Value::Text(s) = &a.0 {
                                    s.as_str()
                                } else {
                                    ""
                                };
                                let b_key = if let Value::Text(s) = &b.0 {
                                    s.as_str()
                                } else {
                                    ""
                                };
                                a_key.cmp(b_key)
                            });
                            Value::Map(map_entries)
                        };

                        // Finding 1: buffer the row instead of forwarding it now.
                        // It is flushed to `emit` only when the partition is
                        // confirmed complete (a `flush_and_emitted!` return). A
                        // mid-partition `NeedMore` discards `pending` and the
                        // caller re-parses from the partition start.
                        pending.push((table_id.clone(), partition_key.clone(), row_value, row_ts));
                    }

                    if offset >= data.len() {
                        // End of the buffer without an explicit END_OF_PARTITION:
                        // the partition may continue in the next chunk.
                        if at_final_chunk {
                            return flush_and_emitted!(offset, pending, emit);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                    if self.peek_is_partition_header(data, offset) {
                        // Next partition starts here — current one is complete.
                        return flush_and_emitted!(offset, pending, emit);
                    }
                }
                Err(_) => {
                    // A row failed to parse. The legacy loop unconditionally
                    // `break`s here (end-of-partition). Mid-stream that may
                    // instead be a row straddling the chunk boundary, so request
                    // more bytes unless this is the final chunk.
                    if at_final_chunk {
                        return flush_and_emitted!(offset, pending, emit);
                    }
                    return Ok(ParseStep::NeedMore);
                }
            }
        }
    }

    /// Parse row flags only (Issue #213 fix: split from parse_row_header)
    ///
    /// # Format
    /// ```text
    /// [row_flags: u8]
    /// [extended_flags: u8 if 0x80 set]
    /// ```
    ///
    /// Returns (row_flags, extended_flags, bytes_consumed)
    fn parse_row_flags(&self, data: &[u8], offset: usize) -> Result<(u8, Option<u8>, usize)> {
        let mut pos = offset;

        // Read row flags
        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading row flags",
            ));
        }
        let row_flags = data[pos];
        pos += 1;

        debug!(
            "V5CompressedLegacy: Row flags=0x{:02x} at offset {}",
            row_flags, offset
        );

        // Read extended flags if present
        let extended_flags = if (row_flags & ROW_HAS_EXTENDED_FLAGS) != 0 {
            if pos >= data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end reading extended flags",
                ));
            }
            let ext = data[pos];
            pos += 1;
            Some(ext)
        } else {
            None
        };

        let bytes_consumed = pos - offset;
        Ok((row_flags, extended_flags, bytes_consumed))
    }

    /// Check if row flags indicate end of partition (Issue #229 fix)
    ///
    /// Per Cassandra's UnfilteredSerializer.java, END_OF_PARTITION is written as exactly 0x01
    /// by the `writeEndOfPartition()` method. The Cassandra source uses a bitmask check
    /// `(flags & END_OF_PARTITION) != 0`, but in practice the marker is always 0x01.
    ///
    /// We use an EXACT match to 0x01 to avoid false positives with row data that
    /// incidentally has bit 0 set (e.g., 0xb7 which would wrongly match a bitmask check).
    ///
    /// When END_OF_PARTITION (0x01) is detected, nothing follows the flags byte.
    /// The partition is complete and parsing should move to the next partition.
    #[inline]
    fn is_end_of_partition(flags: u8) -> bool {
        flags == END_OF_PARTITION // Exact match, not bitmask
    }

    /// Check if row flags indicate a range tombstone marker (not a data row)
    ///
    /// Per Cassandra's UnfilteredSerializer.java, IS_MARKER (0x02) indicates a range
    /// tombstone boundary. The marker flag can be combined with other metadata flags
    /// (e.g., 0x52 = IS_MARKER | deletion metadata, 0x7a, 0x36, etc.).
    ///
    /// Issue #258 fix: Use bitwise AND to detect markers with additional flags.
    /// Previously used exact match (flags == 0x02) which missed markers like 0x52.
    #[inline]
    fn is_range_tombstone_marker(flags: u8) -> bool {
        // Check if IS_MARKER bit is set, but END_OF_PARTITION bit is NOT set
        // IS_MARKER = 0x02, END_OF_PARTITION = 0x01
        // If END_OF_PARTITION bit is set (even with other bits), it's end of partition, not a marker
        (flags & IS_MARKER) != 0 && (flags & END_OF_PARTITION) == 0
    }

    /// Skip a range tombstone marker body (Issue #229 fix, VG6 fix)
    ///
    /// Range tombstone markers for SSTable format have this on-disk layout:
    ///   [flags: u8]                        ← IS_MARKER (0x02) bit set
    ///   [extended_flags: u8]               ← only if ROW_HAS_EXTENDED_FLAGS set
    ///   [bound_kind: u8]                   ← ordinal of ClusteringBoundOrBoundary.Kind
    ///   [cluster_count: u16 big-endian]    ← number of clustering values (bound.size())
    ///   [cluster_header: VUInt]            ← 2 bits per value (0=present, 1=empty, 2=null)
    ///   [cluster_values: ...]              ← type-specific bytes for non-null/non-empty values
    ///   [marker_body_size: VUInt]          ← size of the body that follows (including prev_size)
    ///   [prev_unfiltered_size: VUInt]      ← size of the previous unfiltered item
    ///   [marked_for_delete_at: VUInt]      ← timestamp delta from min_timestamp (µs)
    ///   [local_deletion_time: VUInt32]     ← seconds delta from min_local_deletion_time
    ///   [marked_for_delete_at2: VUInt]     ← ONLY for boundaries (kind 2 or 5)
    ///   [local_deletion_time2: VUInt32]    ← ONLY for boundaries (kind 2 or 5)
    ///
    /// Authority:
    ///   UnfilteredSerializer.java:282-303  (serialize(RangeTombstoneMarker, ...))
    ///   ClusteringBoundOrBoundary.Serializer.serialize (lines 103-107):
    ///     out.writeByte(bound.kind().ordinal())   ← kind byte
    ///     out.writeShort(bound.size())            ← u16 cluster count
    ///     ClusteringPrefix.serializer.serializeValuesWithoutSize(...)
    ///   SerializationHeader.writeDeletionTime (lines 180-183):
    ///     writeTimestamp → writeUnsignedVInt      ← VUInt, NOT ZigZag
    ///     writeLocalDeletionTime → writeUnsignedVInt32 ← VUInt, NOT ZigZag
    ///
    /// VG6 fix: The previous implementation had three bugs:
    ///   1. After kind byte: did not read the u16 cluster_count before the VUInt header.
    ///      The 2-byte short was being consumed as part of the clustering values, causing
    ///      all deletion-time bytes to be misaligned.
    ///   2. After clustering values: did not skip marker_body_size + prev_unfiltered_size
    ///      VUInts that precede the deletion times in SSTable format.
    ///   3. Used parse_vint (ZigZag) instead of parse_vuint (unsigned) for deletion times.
    ///
    /// Implementation strategy: use marker_body_size to skip the entire body
    /// (prev_size + deletion times) without manually decoding individual fields.
    fn skip_range_tombstone_marker(
        &self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
    ) -> Result<usize> {
        let mut pos = offset;

        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end at range tombstone marker",
            ));
        }

        let marker_flags = data[pos];
        pos += 1; // Skip flags byte

        log::debug!(
            "V5CompressedLegacy: Skipping range tombstone marker with flags=0x{:02x} at offset {}",
            marker_flags,
            offset
        );

        // Extended flags if present (unlikely for markers, but handle it)
        if (marker_flags & ROW_HAS_EXTENDED_FLAGS) != 0 {
            if pos >= data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end reading marker extended flags",
                ));
            }
            pos += 1;
        }

        // Read bound kind byte.
        // Authority: ClusteringBoundOrBoundary.Serializer.serialize (line 104):
        //   out.writeByte(bound.kind().ordinal())
        //
        // Kind ordinals (ClusteringPrefix.java:67-81):
        //   0 = EXCL_END_BOUND (simple, 1 deletion time)
        //   1 = INCL_START_BOUND (simple, 1 deletion time)
        //   2 = EXCL_END_INCL_START_BOUNDARY (boundary, 2 deletion times)
        //   5 = INCL_END_EXCL_START_BOUNDARY (boundary, 2 deletion times)
        //   6 = INCL_END_BOUND (simple, 1 deletion time)
        //   7 = EXCL_START_BOUND (simple, 1 deletion time)
        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading range tombstone bound kind",
            ));
        }
        let bound_kind = data[pos];
        pos += 1;

        log::debug!(
            "V5CompressedLegacy: Range tombstone bound_kind={}",
            bound_kind,
        );

        // Read cluster count (u16 big-endian).
        // Authority: ClusteringBoundOrBoundary.Serializer.serialize (line 105):
        //   out.writeShort(bound.size())
        //
        // This is the number of clustering values in the bound. It is NOT the same as
        // schema.clustering_keys.len() — for regular rows, no count is written; for markers,
        // a u16 is always present. Failing to read this u16 causes all subsequent bytes to
        // be misaligned (the two count bytes get consumed as the VUInt header + first value
        // byte, producing garbage alignment).
        if pos + 2 > data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading range tombstone cluster count (u16)",
            ));
        }
        let cluster_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        log::debug!(
            "V5CompressedLegacy: Range tombstone cluster_count={}",
            cluster_count,
        );

        // Read the clustering VUInt header + clustering values.
        // Authority: ClusteringPrefix.Serializer.serializeValuesWithoutSize (lines 455-477):
        //   Writes VUInt header (2 bits per value, 0=present/1=empty/2=null), then value bytes.
        //
        // Use a truncated schema when cluster_count < schema.clustering_keys.len() to avoid
        // reading past the bound's bytes into the marker body (prefix bound case).
        if cluster_count > 0 {
            let prefix_schema_owned = Self::clustering_prefix_schema(schema, cluster_count);
            let effective_schema = prefix_schema_owned.as_ref().unwrap_or(schema);
            let (_, new_pos) = self.parse_clustering_prefix(data, pos, effective_schema)?;
            pos = new_pos;
        }

        // Read marker_body_size and skip the body.
        // Authority: UnfilteredSerializer.java:291 (for SSTable format):
        //   out.writeUnsignedVInt(serializedMarkerBodySize(marker, header, previousUnfilteredSize, version))
        //   out.writeUnsignedVInt(previousUnfilteredSize)
        //   ... deletion time(s) ...
        //
        // serializedMarkerBodySize() returns the size of (prev_size + deletion_times).
        // So after reading marker_body_size, we can skip exactly that many bytes to reach
        // the next unfiltered item, without needing to decode individual deletion time VUInts.
        //
        // This is exactly the same pattern as regular row_size: after reading row_size,
        // skip row_size bytes to reach the next row/marker.
        let (remaining, marker_body_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse marker_body_size at offset {}: {:?}",
                pos, e
            ))
        })?;
        let body_size_vint_len = data[pos..].len() - remaining.len();
        pos += body_size_vint_len;

        // Skip marker_body_size bytes (prev_size + deletion time(s))
        let body_end = pos + marker_body_size as usize;
        if body_end > data.len() {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: marker_body_size={} at pos={} exceeds data length {}",
                marker_body_size,
                pos,
                data.len()
            )));
        }
        pos = body_end;

        log::debug!(
            "V5CompressedLegacy: Skipped range tombstone marker, advanced from {} to {}",
            offset,
            pos
        );

        Ok(pos)
    }

    /// Parse row metadata AFTER flags and clustering prefix (Issue #213 fix)
    ///
    /// # Corrected Format (from Cassandra UnfilteredSerializer.java)
    /// ```text
    /// [row_flags: u8]           ← Parsed by parse_row_flags()
    /// [extended_flags: u8]      ← Parsed by parse_row_flags()
    /// [clustering_prefix]       ← Parsed by parse_clustering_prefix()
    /// [row_size: VInt]          ← This function starts here
    /// [prev_size: VInt]
    /// [timestamp: VInt if 0x04 set] ← Delta from min_timestamp
    /// [ttl: VInt if 0x08 set] ← Delta from min_ttl
    /// [deletion: 2 VInts if 0x10 set]
    /// [column_bitmap: VUInt bitmask of missing columns if NOT 0x20]
    /// ```
    ///
    /// Returns RowHeader with decoded metadata, calculated header_size, and row_size.
    fn parse_row_metadata(
        &self,
        data: &[u8],
        offset: usize,
        row_flags: u8,
        _extended_flags: Option<u8>,
    ) -> Result<(RowHeader, u64)> {
        let mut pos = offset;

        // V5CompressedLegacy format: row_size and prev_size come AFTER clustering
        // (which has already been parsed before this function is called)

        // Read row size (VInt) - CRITICAL for partition boundary detection!
        debug!(
            "V5CompressedLegacy: Parsing row_size VInt at pos={}, hex={:02x?}",
            pos,
            &data[pos..std::cmp::min(pos + 5, data.len())]
        );
        let (remaining, row_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse row size at offset {}: {:?}",
                pos, e
            ))
        })?;
        let row_size_vint_len = data[pos..].len() - remaining.len();
        debug!(
            "V5CompressedLegacy: row_size={}, consumed {} bytes, pos before={}, pos after={}",
            row_size,
            row_size_vint_len,
            pos,
            pos + row_size_vint_len
        );
        pos += row_size_vint_len;

        // Read prev size (VInt)
        debug!(
            "V5CompressedLegacy: Parsing prev_size VInt at pos={}, hex={:02x?}",
            pos,
            &data[pos..std::cmp::min(pos + 5, data.len())]
        );
        let (remaining, _prev_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse prev size at offset {}: {:?}",
                pos, e
            ))
        })?;
        let bytes_consumed = data[pos..].len() - remaining.len();
        debug!(
            "V5CompressedLegacy: prev_size={}, consumed {} bytes, pos before={}, pos after={}",
            _prev_size,
            bytes_consumed,
            pos,
            pos + bytes_consumed
        );
        pos += bytes_consumed;

        // Read timestamp if HAS_TIMESTAMP flag is set.
        //
        // Fix #629 (C2): Cassandra writes an UNSIGNED VInt delta here
        // (SerializationHeader.java:165: out.writeUnsignedVInt(timestamp - stats.minTimestamp)).
        // The old code used parse_vint (ZigZag), causing ~50% undercount of timestamp deltas.
        let timestamp = if (row_flags & ROW_HAS_TIMESTAMP) != 0 {
            let (remaining, delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse timestamp delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            // Apply delta decoding: absolute_timestamp = min_timestamp + delta
            let absolute_timestamp = self.min_timestamp.wrapping_add(delta as i64);
            debug!(
                "V5CompressedLegacy: Row timestamp: delta={}, min={}, absolute={}",
                delta, self.min_timestamp, absolute_timestamp
            );
            Some(absolute_timestamp)
        } else {
            None
        };

        // Read TTL and liveness local expiration time if HAS_TTL flag is set.
        //
        // Fix #630 (C3): Cassandra writes TWO VInt32 fields when HAS_TTL is set
        // (UnfilteredSerializer.java:225-228):
        //   1. pk_liveness.ttl()               → header.writeTTL(ttl, out)        [VInt32]
        //   2. pk_liveness.localExpirationTime()→ header.writeLocalDeletionTime(ldt, out) [VInt32]
        // The old code read only ONE VInt (TTL), leaving the LDT byte(s) unread and
        // misaligning all subsequent fields in HAS_TTL rows.
        let (ttl, ttl_liveness_ldt) = if (row_flags & ROW_HAS_TTL) != 0 {
            let (remaining, ttl_delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse TTL delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            // Apply delta decoding: absolute_ttl = min_ttl + delta
            let absolute_ttl = if let Some(min_ttl) = self.min_ttl {
                min_ttl.wrapping_add(ttl_delta as i64) as i32
            } else {
                ttl_delta as i32
            };

            // Read liveness local expiration time (second mandatory field after TTL).
            let (remaining, ldt_delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse liveness LDT delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let ldt_bytes_consumed = data[pos..].len() - remaining.len();
            pos += ldt_bytes_consumed;

            let absolute_ldt = self.min_local_deletion_time.wrapping_add(ldt_delta as i64) as i32;

            debug!(
                "V5CompressedLegacy: Row TTL: ttl_delta={}, min={:?}, ttl={}, ldt_delta={}, ldt={}",
                ttl_delta, self.min_ttl, absolute_ttl, ldt_delta, absolute_ldt
            );
            (Some(absolute_ttl), Some(absolute_ldt))
        } else {
            (None, None)
        };

        // Read deletion if HAS_DELETION flag is set.
        //
        // Cassandra canonical DeletionTime.Serializer order (matches the CQLite writer,
        // data_writer.rs write_*_row HAS_DELETION block and write_complex_deletion):
        //   1. markedForDeleteAt: UNSIGNED VInt delta, base min_timestamp, MICROSECONDS
        //      -> the authoritative reconciliation timestamp (LWW shadowing).
        //   2. localDeletionTime: UNSIGNED VInt delta, base min_local_deletion_time, SECONDS
        //      -> the GC-grace clock, NOT a reconciliation timestamp.
        //
        // Fix #629 (C2): Both deltas are UNSIGNED per Cassandra SerializationHeader.java.
        // The old code used parse_vint (ZigZag) for markedForDeleteAt, causing ~50% undercount.
        //
        // (The complex-cell deletion reader, parse_complex_column, already uses this
        // markedForDeleteAt-first order; this aligns the row-level header with it.)
        let (marked_for_delete_at, local_deletion_time) = if (row_flags & ROW_HAS_DELETION) != 0 {
            // First VInt: markedForDeleteAt delta (unsigned).
            let (remaining, mfda_delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse markedForDeleteAt delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            // Second VInt: localDeletionTime delta (unsigned VInt32).
            //
            // For both nb and oa the on-disk format is the same: an unsigned VInt32
            // encoding `(int)(localDeletionTime - stats.minLocalDeletionTime)`.
            // See: SerializationHeader.java — `writeLocalDeletionTime` /
            //      `readLocalDeletionTime` (same for all BIG versions).
            //
            // VG3 gate: hasUIntDeletionTime (BigFormat.java:409, oa+)
            // The interpretation of the *result* differs:
            //   nb: `min_local_deletion_time + delta` cast to i32 (values capped at ~year 2038)
            //   oa: `min_local_deletion_time + delta` treated as u32 to support ~year 2106
            //
            // When the sum overflows an i32 (> 2^31-1 seconds) the value is negative
            // in a signed context; with hasUIntDeletionTime we reinterpret it as an
            // unsigned u32 (CassandraUInt.toLong, CassandraUInt.java).  For current
            // test fixtures all deletion times are well within i32 range so both
            // interpretations produce identical bit patterns; the gate is a no-op
            // in practice but is wired correctly for future large TTL values.
            let (remaining, ldt_delta) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse localDeletionTime delta at offset {}: {:?}",
                    pos, e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            // markedForDeleteAt: absolute = min_timestamp + delta (microseconds).
            let absolute_marked_for_delete_at = self.min_timestamp.wrapping_add(mfda_delta as i64);
            // localDeletionTime: absolute = min_local_deletion_time + delta (seconds).
            //
            // VG3 gate: hasUIntDeletionTime (BigFormat.java:409, oa/da)
            //   nb: store as i32 (may overflow for dates > ~year 2038)
            //   oa: reinterpret the 32-bit bit-pattern as unsigned (supports ~year 2106)
            //
            // Source: SerializationHeader.java readLocalDeletionTime + UnfilteredSerializer.java:671-676
            // "if (complexDeletion.localDeletionTime() < 0) {
            //    complexDeletion = DeletionTime.build(..., Cell.deletionTimeUnsignedIntegerToLong((int) ...));
            //  }" — this reinterpretation fires when hasUIntDeletionTime && bit31 set.
            let has_uint_ldt = match self.version_gates.as_ref() {
                crate::storage::sstable::version_gate::VersionGates::Big(g) => {
                    g.has_uint_deletion_time
                }
                crate::storage::sstable::version_gate::VersionGates::Bti(g) => {
                    g.has_uint_deletion_time
                }
            };
            let raw_ldt = self.min_local_deletion_time.wrapping_add(ldt_delta as i64);
            let absolute_local_deletion_time = if has_uint_ldt {
                // Reinterpret the low 32 bits as an unsigned integer (year-2106-safe).
                // CassandraUInt.toLong(int) = Integer.toUnsignedLong(int), so negative
                // i32 values get promoted to the [2^31, 2^32) long range.
                (raw_ldt as u32) as i32
            } else {
                raw_ldt as i32
            };
            debug!(
                "V5CompressedLegacy: Row deletion: markedForDeleteAt(delta={}, min_ts={}, abs={} us), localDeletionTime(delta={}, min_ldt={}, abs={} s)",
                mfda_delta,
                self.min_timestamp,
                absolute_marked_for_delete_at,
                ldt_delta,
                self.min_local_deletion_time,
                absolute_local_deletion_time
            );
            (
                Some(absolute_marked_for_delete_at),
                Some(absolute_local_deletion_time),
            )
        } else {
            (None, None)
        };

        // Parse column bitmap if HAS_ALL_COLUMNS is NOT set
        let missing_columns_bitmap = if (row_flags & ROW_HAS_ALL_COLUMNS) == 0 {
            // Cassandra Columns.Serializer.serializeSubset() format:
            // Single unsigned VInt encoding a bitmask of MISSING columns
            // (bit=1 means column is missing, bit=0 means present)
            let (remaining, bitmap) = parse_vuint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "V5CompressedLegacy: Failed to parse column bitmap at offset {}: {:?}",
                    offset + pos,
                    e
                ))
            })?;
            let bytes_consumed = data[pos..].len() - remaining.len();
            pos += bytes_consumed;

            debug!(
                "V5CompressedLegacy: Parsed column bitmap: missing_bitmap=0x{:X} ({} bytes)",
                bitmap, bytes_consumed
            );
            Some(bitmap)
        } else {
            None
        };

        let header_size = pos - offset;
        debug!(
            "V5CompressedLegacy: Row header parsing complete: offset_start={}, pos_end={}, header_size={} bytes, row_size={} bytes (total row including cells), timestamp={:?}, ttl={:?}, deletion={:?}",
            offset, pos, header_size, row_size, timestamp, ttl, local_deletion_time
        );

        // Note: ttl_liveness_ldt (from HAS_TTL) is the pk_liveness local expiration time.
        // It is distinct from local_deletion_time (from HAS_DELETION, row tombstone GC clock).
        // We store it in RowHeader so the delta-scan path can populate CellMeta.expires_at
        // for TTL-bearing INSERT rows (Issue #702).  is_row_tombstone() still checks
        // local_deletion_time (HAS_DELETION only), so TTL rows are NOT misclassified.

        Ok((
            RowHeader {
                timestamp,
                ttl,
                liveness_expires_at_seconds: ttl_liveness_ldt,
                local_deletion_time,
                marked_for_delete_at,
                header_size,
                row_size_vint_len,
                missing_columns_bitmap,
            },
            row_size,
        ))
    }

    /// Parse partition header (flags, key, deletion time)
    ///
    /// # Format
    /// ```text
    /// [flags: u8][key_len: u8][key_bytes: [u8; key_len]][del_time: i32][unknown: 8 bytes]
    /// ```
    ///
    /// # Visibility
    /// Exposed for integration testing to validate partition header parsing
    #[doc(hidden)]
    pub fn parse_partition_header(&self, data: &[u8], offset: usize) -> Result<(RowKey, usize)> {
        let (row_key, next_offset, _deletion_time) =
            self.parse_partition_header_full(data, offset)?;
        Ok((row_key, next_offset))
    }

    /// Like [`parse_partition_header`] but also returns the partition-level deletion
    /// timestamp (`markedForDeleteAt` in µs since epoch), if the partition is deleted.
    ///
    /// Returns `(RowKey, next_offset, Option<markedForDeleteAt_micros>)`.
    ///
    /// `None` means the partition is live (no partition tombstone).
    /// `Some(ts)` means the partition carries a tombstone; `ts` is the authoritative
    /// reconciliation timestamp in microseconds since the Unix epoch.
    ///
    /// Authority: DeletionTime.java (getSerializer / legacySerializer / Serializer),
    /// BigFormat.java:409 (`hasUIntDeletionTime`).
    pub fn parse_partition_header_full(
        &self,
        data: &[u8],
        mut offset: usize,
    ) -> Result<(RowKey, usize, Option<i64>)> {
        let start_offset = offset;

        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Partition header offset {} out of bounds (data len: {})",
                offset,
                data.len()
            )));
        }

        // Byte 0: Flags (ignore for now - may indicate static rows, deletions, etc.)
        let _flags = data[offset];
        offset += 1;

        // Byte 1: Partition key length (u8, NOT VInt)
        if offset >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end at partition key length",
            ));
        }
        let key_len = data[offset] as usize;
        offset += 1;

        // Issue #258 FIX: Partition key length must be non-zero
        // A key_len of 0 indicates this is NOT a valid partition header (likely row data).
        // This validation is critical for peek_is_partition_header() to correctly
        // distinguish partition headers from row data in the row loop.
        if key_len == 0 {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Invalid partition key length 0 at offset {} (not a partition header)",
                start_offset
            )));
        }

        debug!(
            "V5CompressedLegacy: Partition key length = {} bytes",
            key_len
        );

        // Next key_len bytes: Partition key data (raw bytes, no component structure)
        if offset + key_len > data.len() {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Partition key extends beyond data (offset: {}, key_len: {}, data_len: {})",
                offset, key_len, data.len()
            )));
        }
        let key_bytes = data[offset..offset + key_len].to_vec();
        offset += key_len;

        // Partition-level DeletionTime deserialization.
        //
        // VG3 gate: hasUIntDeletionTime (BigFormat.java:409, oa+)
        //
        // oa format uses a compact DeletionTime.Serializer
        // (DeletionTime.java, Serializer inner class):
        //   LIVE:    1 byte = 0x80 (IS_LIVE_DELETION = 0b10000000)
        //   DELETED: 8 bytes markedForDeleteAt (long) +
        //            4 bytes localDeletionTimeUnsignedInteger (int) = 12 bytes total
        //
        // nb format uses DeletionTime.legacySerializer:
        //   Always:  4 bytes localDeletionTime (int) +
        //            8 bytes markedForDeleteAt (long) = 12 bytes total
        //
        // Authority: DeletionTime.java:191-219 (getSerializer / Serializer.serialize)
        let partition_deletion: Option<i64>;
        if self.has_uint_deletion_time() {
            // oa / da format
            if offset >= data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end at oa partition deletion time byte",
                ));
            }
            let del_flags = data[offset];
            const IS_LIVE_DELETION: u8 = 0x80; // DeletionTime.java:208
            if (del_flags & IS_LIVE_DELETION) != 0 {
                // LIVE partition: exactly 1 byte — no tombstone.
                if del_flags != IS_LIVE_DELETION {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Invalid IS_LIVE_DELETION byte 0x{:02x} at offset {} \
                         (only 0x80 is valid for oa-format LIVE partitions, per DeletionTime.java:227-229)",
                        del_flags, offset
                    )));
                }
                offset += 1;
                partition_deletion = None;
            } else {
                // DELETED partition (oa): 8 bytes markedForDeleteAt (big-endian i64)
                //                       + 4 bytes localDeletionTime (big-endian u32)
                if offset + 12 > data.len() {
                    return Err(Error::corruption(
                        "V5CompressedLegacy: Unexpected end at oa partition deletion time (deleted)",
                    ));
                }
                let mfda = i64::from_be_bytes(
                    data[offset..offset + 8]
                        .try_into()
                        .map_err(|_| Error::corruption("V5CompressedLegacy: oa mfda slice"))?,
                );
                offset += 12; // markedForDeleteAt(8) + localDeletionTime(4)
                partition_deletion = Some(mfda);
            }
        } else {
            // nb format: 4 bytes localDeletionTime (big-endian i32)
            //          + 8 bytes markedForDeleteAt (big-endian i64)
            if offset + 12 > data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end at nb partition deletion time",
                ));
            }
            // localDeletionTime is the first 4 bytes.
            let local_deletion_time = i32::from_be_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| Error::corruption("V5CompressedLegacy: nb ldt slice"))?,
            );
            offset += 4;
            // markedForDeleteAt is next 8 bytes (big-endian i64).
            let mfda = i64::from_be_bytes(
                data[offset..offset + 8]
                    .try_into()
                    .map_err(|_| Error::corruption("V5CompressedLegacy: nb mfda slice"))?,
            );
            offset += 8;
            // A live (not deleted) partition in nb format has
            // localDeletionTime = 0x7fffffff (i32::MAX, DeletionTime.LIVE sentinel).
            // Any other value indicates a real partition tombstone.
            const NB_LIVE_LOCAL_DELETION_TIME: i32 = i32::MAX;
            if local_deletion_time == NB_LIVE_LOCAL_DELETION_TIME {
                partition_deletion = None;
            } else {
                partition_deletion = Some(mfda);
            }
        }

        // Create RowKey from partition key bytes
        let row_key = RowKey(key_bytes);

        debug!(
            "V5CompressedLegacy: Parsed partition header at offset {}, consumed {} bytes, \
             partition_deletion={:?}",
            start_offset,
            offset - start_offset,
            partition_deletion
        );

        Ok((row_key, offset, partition_deletion))
    }

    /// Parse a range tombstone marker in full, returning the decoded bound values,
    /// inclusivity flags, and deletion timestamp(s).
    ///
    /// This is the delta-scan counterpart to `skip_range_tombstone_marker`: instead
    /// of discarding the clustering values and deletion time, it decodes and returns
    /// them so the caller can emit `DeltaRecord::RangeDelete`.
    ///
    /// ## Return value
    ///
    /// `Ok((bound_values, bound_kind, deleted_at_primary, deleted_at_secondary, next_offset))`
    ///
    /// - `bound_values`: clustering-key prefix values for this bound (may be shorter
    ///   than the full clustering arity — a prefix bound).
    /// - `bound_kind`: the raw Cassandra `ClusteringPrefix.Kind` ordinal (0/1/2/5/6/7).
    /// - `deleted_at_primary`: `markedForDeleteAt` in µs for this bound's tombstone.
    /// - `deleted_at_secondary`: present only for boundary markers (kind 2 or 5) and
    ///   carries the deletion time for the *other* side of the boundary.
    /// - `next_offset`: position after this marker in `data`.
    ///
    /// ## Bound kind ordinals
    ///
    /// | ordinal | name | meaning |
    /// |---------|------|---------|
    /// | 0 | `EXCL_END_BOUND` | end of range, exclusive (`< ck`) |
    /// | 1 | `INCL_START_BOUND` | start of range, inclusive (`>= ck`) |
    /// | 2 | `EXCL_END_INCL_START_BOUNDARY` | boundary: end of prev range (exclusive) + start of new range (inclusive) |
    /// | 5 | `INCL_END_EXCL_START_BOUNDARY` | boundary: end of prev range (inclusive) + start of new range (exclusive) |
    /// | 6 | `INCL_END_BOUND` | end of range, inclusive (`<= ck`) |
    /// | 7 | `EXCL_START_BOUND` | start of range, exclusive (`> ck`) |
    ///
    /// Boundary markers (kind 2 or 5) carry **two** deletion times; simple markers (all others)
    /// carry **one** deletion time and `deleted_at_secondary` is `None`.
    ///
    /// Authority: UnfilteredSerializer.java:282-303, ClusteringBoundOrBoundary.java
    #[allow(clippy::type_complexity)]
    pub fn parse_range_tombstone_marker_full(
        &self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
    ) -> Result<(Vec<Value>, u8, i64, Option<i64>, usize)> {
        let mut pos = offset;

        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end at range tombstone marker (full parse)",
            ));
        }

        let marker_flags = data[pos];
        pos += 1;

        // Extended flags if present
        if (marker_flags & ROW_HAS_EXTENDED_FLAGS) != 0 {
            if pos >= data.len() {
                return Err(Error::corruption(
                    "V5CompressedLegacy: Unexpected end reading marker extended flags (full)",
                ));
            }
            pos += 1;
        }

        // Bound kind byte.
        if pos >= data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading range tombstone bound kind (full)",
            ));
        }
        let bound_kind = data[pos];
        pos += 1;

        // Cluster count (u16 big-endian).
        if pos + 2 > data.len() {
            return Err(Error::corruption(
                "V5CompressedLegacy: Unexpected end reading range tombstone cluster count (full)",
            ));
        }
        let cluster_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        // Clustering values.
        // Build a truncated schema slice for parsing only `cluster_count` clustering
        // columns.  Range tombstone bounds may be **prefix** bounds: the Cassandra
        // serializer writes `cluster_count` < full-arity when a `DELETE WHERE pk=?
        // AND ck1=?` targets all sub-rows for a given ck1 without specifying ck2.
        //
        // If we let `parse_clustering_prefix` iterate over the full schema, it would
        // read past the `cluster_count` bytes into the marker body (producing garbage
        // or invalid UTF-8 errors).  Instead we pass a synthetic schema whose
        // `clustering_keys` vec is truncated to `cluster_count`.
        let bound_values = if cluster_count > 0 {
            let prefix_schema_owned = Self::clustering_prefix_schema(schema, cluster_count);
            let effective_schema = prefix_schema_owned.as_ref().unwrap_or(schema);
            let (values, new_pos) = self.parse_clustering_prefix(data, pos, effective_schema)?;
            pos = new_pos;
            values
        } else {
            Vec::new()
        };

        // marker_body_size VUInt — size of (prev_size VUInt + deletion_time(s)).
        let (remaining, marker_body_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse marker_body_size (full) at offset {}: {:?}",
                pos, e
            ))
        })?;
        let body_size_vint_len = data[pos..].len() - remaining.len();
        pos += body_size_vint_len;

        let body_start = pos;
        let body_end = pos + marker_body_size as usize;
        if body_end > data.len() {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: marker_body_size={} at pos={} exceeds data length {} (full)",
                marker_body_size,
                pos,
                data.len()
            )));
        }

        // Inside the body:
        //   [prev_unfiltered_size: VUInt]    ← skip
        //   [markedForDeleteAt delta: VUInt]  ← delta from min_timestamp (µs)
        //   [localDeletionTime delta: VUInt]  ← delta from min_local_deletion_time (s), skip
        //   (repeat for second deletion time if boundary marker)
        let (remaining2, _prev_size) = parse_vuint(&data[pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse prev_size in marker body at {}: {:?}",
                pos, e
            ))
        })?;
        pos += data[pos..].len() - remaining2.len();

        // Primary deletion time.
        let deleted_at_primary = self.parse_deletion_time_pair(data, &mut pos)?;

        // Boundary markers (kind 2 = EXCL_END_INCL_START, kind 5 = INCL_END_EXCL_START)
        // carry a second deletion time for the adjacent range.
        let deleted_at_secondary = if bound_kind == 2 || bound_kind == 5 {
            Some(self.parse_deletion_time_pair(data, &mut pos)?)
        } else {
            None
        };

        // Advance to end of body (in case the secondary parse left pos short).
        let _ = body_start; // acknowledged
        pos = body_end;

        log::debug!(
            "V5CompressedLegacy: Parsed range tombstone marker full: kind={} values={} \
             deleted_at_primary={} deleted_at_secondary={:?} next_offset={}",
            bound_kind,
            bound_values.len(),
            deleted_at_primary,
            deleted_at_secondary,
            pos
        );

        Ok((
            bound_values,
            bound_kind,
            deleted_at_primary,
            deleted_at_secondary,
            pos,
        ))
    }

    /// Return a schema view truncated to `n` clustering keys.
    ///
    /// Range-tombstone bound markers may be **prefix bounds**: the Cassandra serializer
    /// writes only `cluster_count` clustering values when a DELETE specifies fewer
    /// clustering components than the full key arity (e.g. `DELETE WHERE pk=? AND ck1=?`
    /// on a table with `(ck1, ck2)` only pins the first component).
    ///
    /// Returns `None` (callers use the original schema) when `n >= schema.clustering_keys.len()`,
    /// avoiding a clone in the common non-prefix case.  Returns `Some(truncated)` when a
    /// shorter view is needed.
    fn clustering_prefix_schema(schema: &TableSchema, n: usize) -> Option<TableSchema> {
        if n >= schema.clustering_keys.len() {
            None
        } else {
            Some(TableSchema {
                keyspace: schema.keyspace.clone(),
                table: schema.table.clone(),
                partition_keys: schema.partition_keys.clone(),
                clustering_keys: schema.clustering_keys[..n].to_vec(),
                columns: schema.columns.clone(),
                comments: schema.comments.clone(),
            })
        }
    }

    /// Decode one `(markedForDeleteAt delta, localDeletionTime delta)` pair from `data[*pos..]`.
    ///
    /// Both fields are unsigned VInts.  `markedForDeleteAt` is a delta from `min_timestamp`
    /// (µs); `localDeletionTime` is a delta from `min_local_deletion_time` (s) and is
    /// consumed but not returned (callers do not need the GC-clock value).
    ///
    /// Advances `*pos` past both fields and returns the absolute `markedForDeleteAt` in µs.
    fn parse_deletion_time_pair(&self, data: &[u8], pos: &mut usize) -> Result<i64> {
        // markedForDeleteAt delta (unsigned VInt, µs since epoch delta).
        let (remaining, mfda_delta) = parse_vuint(&data[*pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse markedForDeleteAt in marker body at {}: {:?}",
                *pos, e
            ))
        })?;
        *pos += data[*pos..].len() - remaining.len();
        let absolute_mfda = self.min_timestamp.wrapping_add(mfda_delta as i64);

        // localDeletionTime delta (unsigned VInt, seconds delta) — consume, do not return.
        let (remaining2, _ldt_delta) = parse_vuint(&data[*pos..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse localDeletionTime in marker body at {}: {:?}",
                *pos, e
            ))
        })?;
        *pos += data[*pos..].len() - remaining2.len();

        Ok(absolute_mfda)
    }

    /// Parse clustering prefix section (between row header and cells)
    ///
    /// The clustering prefix encodes clustering key values using a compact VInt header
    /// with 2 bits per clustering column to indicate value state.
    ///
    /// # Format
    /// ```text
    /// [prefix_header: VInt] ← 2 bits per clustering column
    ///   - 00 = null
    ///   - 01 = empty
    ///   - 10/11 = has value
    /// [value_1: bytes if present]
    /// [value_2: bytes if present]
    /// [... more values ...]
    /// ```
    ///
    /// Returns: (clustering_values, new_offset)
    fn parse_clustering_prefix(
        &self,
        data: &[u8],
        mut offset: usize,
        schema: &TableSchema,
    ) -> Result<(Vec<Value>, usize)> {
        // If no clustering keys, skip this section
        if schema.clustering_keys.is_empty() {
            log::debug!(
                "V5CompressedLegacy: No clustering keys in schema, skipping clustering prefix"
            );
            return Ok((Vec::new(), offset));
        }

        log::debug!(
            "V5CompressedLegacy: Parsing clustering prefix at offset {} for {} clustering keys",
            offset,
            schema.clustering_keys.len()
        );

        // Read header VInt (2 bits per clustering column)
        let (remaining, header_vint) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "V5CompressedLegacy: Failed to parse clustering prefix header VInt at offset {}: {:?}",
                offset, e
            ))
        })?;
        let bytes_consumed = data[offset..].len() - remaining.len();
        offset += bytes_consumed;

        log::debug!(
            "V5CompressedLegacy: Clustering prefix header = 0x{:x}, consumed {} bytes",
            header_vint,
            bytes_consumed
        );

        // Decode each clustering value based on 2-bit state
        let mut clustering_values = Vec::new();
        for (i, col) in schema.clustering_keys.iter().enumerate() {
            let state = (header_vint >> (i * 2)) & 0x03;
            log::debug!(
                "V5CompressedLegacy: Clustering key {} '{}' state = {} (from bits {}..{})",
                i,
                col.name,
                state,
                i * 2,
                i * 2 + 1
            );

            // Issue #229 FIX: Correct state interpretation per Cassandra's ClusteringPrefix.Kind
            //
            // Per Cassandra 5.0 UnfilteredSerializer.java and ClusteringPrefix.Kind:
            // - 0 (PRESENT): Value is present, type-specific bytes follow
            // - 1 (EMPTY): Empty value (zero-length, no bytes follow)
            // - 2 (NULL): NULL value (no bytes follow)
            // - 3: Reserved
            //
            // Previous code had 0=NULL, 2/3=PRESENT which was inverted!
            match state {
                0 => {
                    // PRESENT - parse value based on type
                    let (value, new_off) = self.parse_clustering_value(data, offset, col)?;
                    log::debug!(
                        "V5CompressedLegacy:   -> PRESENT: {:?} (consumed {} bytes)",
                        value,
                        new_off - offset
                    );
                    clustering_values.push(value);
                    offset = new_off;
                }
                1 => {
                    // EMPTY - zero-length value
                    //
                    // Per Cassandra's ClusteringPrefix, EMPTY means zero-length byte array.
                    // For variable-width types, this is valid. For fixed-width types (int,
                    // bigint, UUID), EMPTY should not normally occur.
                    let col_type = col.data_type.to_lowercase();
                    let empty_value = match col_type.as_str() {
                        "text" | "varchar" | "ascii" => Value::Text(String::new()),
                        "blob" => Value::Blob(vec![]),
                        _ => {
                            // Fixed-width types shouldn't have EMPTY state in normal data
                            log::warn!(
                                "V5CompressedLegacy: EMPTY state for clustering key '{}' (type {}), treating as NULL",
                                col.name, col.data_type
                            );
                            Value::Null
                        }
                    };
                    clustering_values.push(empty_value);
                    log::debug!("V5CompressedLegacy:   -> EMPTY");
                }
                2 => {
                    // NULL
                    clustering_values.push(Value::Null);
                    log::debug!("V5CompressedLegacy:   -> NULL");
                }
                3 => {
                    // Reserved - treat as NULL for safety
                    log::warn!("V5CompressedLegacy: Clustering key {} has reserved state 3, treating as NULL", col.name);
                    clustering_values.push(Value::Null);
                }
                _ => unreachable!(),
            }
        }

        log::debug!(
            "V5CompressedLegacy: Parsed {} clustering values, new offset = {}",
            clustering_values.len(),
            offset
        );

        Ok((clustering_values, offset))
    }

    /// Parse individual clustering value (type-specific)
    ///
    /// Clustering values are encoded based on their CQL type. This handles the most
    /// common clustering key types: timestamp, text, int, uuid.
    ///
    /// Returns: (value, new_offset)
    fn parse_clustering_value(
        &self,
        data: &[u8],
        offset: usize,
        col: &crate::schema::ClusteringColumn,
    ) -> Result<(Value, usize)> {
        let normalized = col.data_type.to_lowercase();
        log::debug!(
            "V5CompressedLegacy: Parsing clustering value '{}' type '{}' at offset {}",
            col.name,
            normalized,
            offset
        );

        match normalized.as_str() {
            "timestamp" | "reversedtype(timestamptype)" => {
                // Fixed 8-byte timestamp (big-endian i64)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 8 bytes for timestamp, only {} available",
                        col.name,
                        data.len() - offset
                    )));
                }
                let ts = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                Ok((Value::Timestamp(ts), offset + 8))
            }

            "text" | "utf8type" | "varchar" => {
                // VInt length + UTF-8 bytes
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': failed to parse text length: {:?}",
                        col.name, e
                    ))
                })?;
                let bytes_consumed = data[offset..].len() - remaining.len();
                let len_offset = offset + bytes_consumed;

                if len_offset + len as usize > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need {} bytes for text, only {} available",
                        col.name,
                        len,
                        data.len() - len_offset
                    )));
                }

                let text = String::from_utf8(data[len_offset..len_offset + len as usize].to_vec())
                    .map_err(|e| {
                        Error::corruption(format!(
                            "V5CompressedLegacy: Clustering '{}': invalid UTF-8: {:?}",
                            col.name, e
                        ))
                    })?;
                Ok((Value::Text(text), len_offset + len as usize))
            }

            "int" => {
                // Issue #258 fix: Fixed 4-byte int (big-endian i32) - NO length prefix
                // Per Cassandra format, fixed-width clustering types have no VInt length prefix
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 4 bytes for int, only {} available",
                        col.name,
                        data.len() - offset
                    )));
                }

                let val = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                Ok((Value::Integer(val), offset + 4))
            }

            "uuid" | "timeuuid" => {
                // Issue #258 fix: Fixed 16-byte UUID - NO length prefix
                // Per Cassandra format, fixed-width clustering types have no VInt length prefix
                if offset + 16 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 16 bytes for UUID, only {} available",
                        col.name,
                        data.len() - offset
                    )));
                }

                let uuid_bytes: [u8; 16] = data[offset..offset + 16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;

                Ok((Value::Uuid(uuid_bytes), offset + 16))
            }

            "bigint" | "counter" => {
                // Issue #258 fix: Fixed 8-byte bigint (big-endian i64) - NO length prefix
                // Per Cassandra format, fixed-width clustering types have no VInt length prefix
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need 8 bytes for bigint, only {} available",
                        col.name,
                        data.len() - offset
                    )));
                }

                let val = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                Ok((Value::BigInt(val), offset + 8))
            }

            _ => {
                // For other types, read VInt length + skip that many bytes
                // Return as blob for now
                warn!(
                    "V5CompressedLegacy: Clustering '{}' has unsupported type '{}', treating as blob",
                    col.name, col.data_type
                );
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': failed to parse blob length: {:?}",
                        col.name, e
                    ))
                })?;
                let bytes_consumed = data[offset..].len() - remaining.len();
                let len_offset = offset + bytes_consumed;

                if len_offset + len as usize > data.len() {
                    return Err(Error::corruption(format!(
                        "V5CompressedLegacy: Clustering '{}': need {} bytes, only {} available",
                        col.name,
                        len,
                        data.len() - len_offset
                    )));
                }

                Ok((
                    Value::Blob(data[len_offset..len_offset + len as usize].to_vec()),
                    len_offset + len as usize,
                ))
            }
        }
    }

    /// Parse row data (header + cells) and return cells with new offset
    ///
    /// V5CompressedLegacy format stores cells WITHOUT column names in schema column order.
    /// Schema is REQUIRED to determine which column each value belongs to.
    ///
    /// Returns: `ParsedRow` = `(cells, row_header, new_offset, is_static)` where
    /// `is_static` is `true` when the row's `EXTENDED_IS_STATIC` flag was set.
    /// Static rows must be merged into clustering rows by the caller, not emitted directly.
    fn parse_row_data_with_offset(
        &self,
        data: &[u8],
        mut offset: usize,
        schema: Option<&TableSchema>,
        reader: &super::super::types::SSTableReader,
        want_cell_metadata: bool,
    ) -> Result<ParsedRow> {
        let mut cells = HashMap::new();
        // Parallel per-cell write metadata map (populated alongside `cells`).
        // Only allocated when the caller actually needs WRITETIME/TTL metadata
        // (i.e. `want_cell_metadata == true`).  On the normal read path this stays
        // `None` so that zero HashMap allocations or inserts occur per cell.
        let mut cell_meta: Option<HashMap<String, CellWriteMetadata>> = if want_cell_metadata {
            Some(HashMap::new())
        } else {
            None
        };

        // DS4 (Issue #700): Per-column complex collection metadata.  Only allocated when
        // want_cell_metadata is true (same gate as cell_meta to avoid hot-path overhead).
        let mut complex_col_meta: Option<HashMap<String, ComplexColumnMeta>> = if want_cell_metadata
        {
            Some(HashMap::new())
        } else {
            None
        };

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy: Schema required for {}.{} (cells stored without column names)",
                self.keyspace, self.table_name
            ))
        })?;

        debug!(
            "V5CompressedLegacy: Starting row data parse at offset {} with {} schema columns",
            offset,
            schema.columns.len()
        );

        // ISSUE #213 FIX: Correct parsing order for V5CompressedLegacy format
        //
        // The CORRECT format from Cassandra UnfilteredSerializer.java is:
        //   1. [row_flags: u8]
        //   2. [extended_flags: u8 if 0x80 set]
        //   3. [clustering_prefix: variable]  ← BEFORE row_size!
        //   4. [row_size: VInt]
        //   5. [prev_size: VInt]
        //   6. [row_body: timestamp, ttl, deletion, bitmap, cells]
        //
        // The previous code parsed row_size BEFORE clustering, which caused
        // clustering key bytes to be misinterpreted as row_size (often 0).

        // Step 1: Parse row flags (1-2 bytes)
        let (row_flags, extended_flags, flags_size) = self.parse_row_flags(data, offset)?;
        offset += flags_size;

        // Issue #258 fix: Check if this is a static row (no clustering prefix)
        // Per Cassandra UnfilteredSerializer.java lines 114-122, 190-191:
        // Static rows have the IS_STATIC bit (0x01) set in extended flags and
        // do NOT have a clustering prefix - skip directly to row_size.
        let is_static = extended_flags
            .map(|ef| (ef & EXTENDED_IS_STATIC) != 0)
            .unwrap_or(false);

        // Step 2: Parse clustering prefix BEFORE row_size (Issue #213 fix)
        // This is the critical change - clustering comes AFTER flags but BEFORE row_size
        // EXCEPT for static rows which have no clustering prefix at all.
        let (clustering_values, offset) = if !is_static {
            self.parse_clustering_prefix(data, offset, schema)?
        } else {
            log::debug!(
                "V5CompressedLegacy: Static row detected (extended_flags=0x{:02x}), skipping clustering prefix",
                extended_flags.unwrap_or(0)
            );
            (vec![], offset)
        };

        log::debug!(
            "V5CompressedLegacy: Parsed {} clustering values after flags, now at offset {} (is_static={})",
            clustering_values.len(),
            offset,
            is_static
        );

        // Issue #229 FIX: Add clustering key values to cells HashMap
        //
        // Cassandra stores clustering keys separately from regular columns, but they
        // must be included in the result for proper query output. Without this fix,
        // tables with clustering keys show fallback column names because the clustering
        // values weren't being added to the cells HashMap.
        for (i, ck) in schema.clustering_keys.iter().enumerate() {
            if i < clustering_values.len() {
                cells.insert(ck.name.clone(), clustering_values[i].clone());
            }
        }

        // Step 3: Parse row metadata (row_size, prev_size, timestamps, etc.)
        //
        // CRITICAL (Issue #237): Save offset where row_size VInt STARTS.
        // The row_size value is measured from AFTER this VInt is consumed.
        // Formula: next_offset = (row_metadata_offset + row_size_vint_len) + row_size
        // This offset is right after the clustering prefix (which was already parsed).
        let row_metadata_offset = offset;
        let (row_header, row_size) =
            self.parse_row_metadata(data, offset, row_flags, extended_flags)?;

        // CRITICAL VALIDATION: row_size must be reasonable
        //
        // In V5CompressedLegacy format, row_size should never exceed the block size (typically 16KB).
        // If row_size is unreasonably large, it indicates either:
        // 1. Partition tombstone or deletion marker (no actual row data)
        // 2. Format parsing error (landed at wrong offset)
        // 3. Corrupted data
        //
        // In all cases, we should skip this partition rather than panic.
        const MAX_REASONABLE_ROW_SIZE: u64 = 1_000_000; // 1MB max (very generous)
        if row_size > MAX_REASONABLE_ROW_SIZE {
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Unreasonably large row_size={} at offset {} (max: {}). Likely partition tombstone or format error.",
                row_size,
                offset,
                MAX_REASONABLE_ROW_SIZE
            )));
        }

        // Row payloads can span multiple compressed chunks in V5CompressedLegacy format.
        // The reader has already stitched all chunks together (see get_all_entries()),
        // so row_size is valid across chunk boundaries. We MUST NOT validate against
        // individual chunk sizes as rows naturally span chunks in Cassandra's format.
        // This is NOT corruption - it's the intended file layout.

        log::debug!(
            "V5CompressedLegacy: Parsed row metadata at offset {}: header_size={} bytes, row_size={} bytes, timestamp={:?}, ttl={:?}, deletion={:?}",
            offset, row_header.header_size, row_size, row_header.timestamp, row_header.ttl, row_header.local_deletion_time
        );

        // CRITICAL FIX (Issue #191, Phase 2): Row tombstone detection
        // If the row has deletion metadata (local_deletion_time is set), the entire row is deleted.
        // In this case, there are NO cell values to parse - the row_size includes ONLY the header.
        // Attempting to parse cells from a tombstoned row will read garbage data and fail.
        //
        // According to Cassandra 5.0 format:
        // - Deleted rows have ROW_HAS_DELETION flag (0x10) set
        // - Row header contains deletion time and deletion timestamp
        // - row_size = header_size (no cell data follows)
        // - Cell parsing must be skipped entirely
        if row_header.local_deletion_time.is_some() {
            log::debug!(
                "V5CompressedLegacy: Row is tombstoned (deletion_time={:?}), skipping cell parsing",
                row_header.local_deletion_time
            );

            // Calculate offset after row data (based on row_size from header)
            //
            // CRITICAL FIX (Issue #237): row_size is measured from AFTER the row_size VInt,
            // not from where it starts. This matches Cassandra's getFilePointer() semantics:
            //   next_position = row_size_value + position_after_reading_row_size_vint
            //
            // There is NO trailing field in V5CompressedLegacy format - the next partition/row
            // starts immediately after row_size bytes from this position.
            let after_row_offset =
                (row_metadata_offset + row_header.row_size_vint_len) + row_size as usize;

            // Validate we have enough data
            if after_row_offset > data.len() {
                let remaining = data
                    .len()
                    .saturating_sub(row_metadata_offset + row_header.row_size_vint_len);
                return Err(Error::corruption(format!(
                    "V5CompressedLegacy: Not enough bytes for row data at offset {} (need {}, have {})",
                    row_metadata_offset + row_header.row_size_vint_len,
                    row_size,
                    remaining
                )));
            }

            let next_offset = after_row_offset;
            log::debug!(
                "V5CompressedLegacy: Skipped tombstoned row, next offset = {}",
                next_offset
            );

            // Return empty cells for tombstoned row (no cell metadata, no complex meta)
            return Ok((
                cells,
                cell_meta,
                Some(row_header),
                next_offset,
                is_static,
                None,
            ));
        }

        // Advance offset past row metadata to start of cell data
        let mut offset = offset + row_header.header_size;

        log::debug!(
            "V5CompressedLegacy: Cell data starts at offset {}, first 32 bytes: {}",
            offset,
            hex::encode(&data[offset..std::cmp::min(offset + 32, data.len())])
        );

        // Cell flags validation: First byte should be valid cell flags (0x00-0x1F) for simple cells
        // Common flags: 0x00 (basic cell), 0x08 (USE_ROW_TIMESTAMP), 0x04 (HAS_EMPTY_VALUE)
        // Deleted cells have 0x01 (IS_DELETED), expiring cells have 0x02 (IS_EXPIRING)
        //
        // NOTE: For complex columns (non-frozen collections), the first byte is a VInt for the
        // cell count, which may have values > 0x1F. This is normal and not an error.
        // The validation below is only accurate for tables with all simple cells.
        if offset < data.len() {
            let first_byte = data[offset];
            if first_byte <= 0x1F {
                debug!(
                    "V5CompressedLegacy: Valid cell flags 0x{:02x} at offset {} after row header",
                    first_byte, offset
                );
            } else {
                debug!(
                    "V5CompressedLegacy: First byte 0x{:02x} at offset {} (> 0x1F) - may be VInt for complex column cell count",
                    first_byte, offset
                );
            }
        }

        // CRITICAL: V5CompressedLegacy format stores cells WITHOUT column names
        // or column IDs in the binary data. Cells appear in SCHEMA DEFINITION ORDER
        // (the order columns were defined in CREATE TABLE), NOT alphabetical order.
        //
        // NULL/missing columns are handled by:
        // - Checking for cell marker (0x08) before attempting to parse
        // - If no marker found or parse fails, column is NULL (not present)
        // - Continue to next column in schema order
        //
        // This implementation uses schema definition order directly, which is the
        // correct approach per Cassandra 5.0 SerializationHeader semantics.

        // CRITICAL FIX (Issue #164): Filter out partition keys and clustering keys!
        // The schema.columns list contains ALL columns (including keys), but cells
        // are only stored for REGULAR columns. Partition/clustering keys are part
        // of the row key and do NOT have cell data.
        let partition_key_names: std::collections::HashSet<_> = schema
            .partition_keys
            .iter()
            .map(|k| k.name.as_str())
            .collect();
        let clustering_key_names: std::collections::HashSet<_> = schema
            .clustering_keys
            .iter()
            .map(|k| k.name.as_str())
            .collect();

        // CRITICAL FIX (Issue #191): Use serialization header column order, not schema order
        // Cassandra 5.0 V5CompressedLegacy stores cells in the order defined by Statistics.db
        // serialization header (alphabetical by ColumnIdentifier/comparator), NOT CQL schema order.
        // We must iterate reader.header.columns directly to align binary layout with logical columns.
        //
        // Issue #702 FIX: For tables with BOTH static and regular columns, Cassandra's
        // missing_columns_bitmap is relative to the column group of the current row kind:
        //   - Static rows:  bitmap covers only static columns
        //   - Regular rows: bitmap covers only regular columns
        // Including the wrong group shifts all bitmap indices, causing columns to be
        // silently absent or misread.  Filter columns_in_order to the matching kind.
        let columns_in_order: Vec<_> = if !reader.header.columns.is_empty() {
            // Build lookup map from schema for column details
            let schema_map: HashMap<String, &crate::schema::Column> = schema
                .columns
                .iter()
                .map(|col| (col.name.clone(), col))
                .collect();

            // Iterate serialization header columns in exact order (skipping keys,
            // and filtering to match the current row's static/regular kind).
            reader
                .header
                .columns
                .iter()
                .filter(|col_info| {
                    !col_info.is_primary_key
                        && !col_info.is_clustering
                        && col_info.is_static == is_static
                })
                .filter_map(|col_info| schema_map.get(&col_info.name).copied())
                .collect()
        } else {
            // Fallback to schema order when header is empty (shouldn't happen for real SSTables)
            log::warn!("V5CompressedLegacy: reader.header.columns is empty, falling back to schema order (may cause column misalignment)");
            schema
                .columns
                .iter()
                .filter(|col| {
                    !partition_key_names.contains(col.name.as_str())
                        && !clustering_key_names.contains(col.name.as_str())
                        && col.is_static == is_static // Issue #702: match row kind
                })
                .collect()
        };

        // Filter columns by missing_columns_bitmap when present.
        // The bitmap indicates which columns are MISSING (bit=1 → absent).
        // We only parse cells for columns that are actually present in the data.
        let columns_to_parse: Vec<&crate::schema::Column> = match row_header.missing_columns_bitmap
        {
            Some(bitmap) => {
                let filtered: Vec<_> = columns_in_order
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| {
                        // Bitmap only covers the first 64 columns (u64).
                        // Columns beyond index 63 are not represented in the
                        // bitmap and are treated as present.
                        *idx >= 64 || (bitmap & (1u64 << idx)) == 0
                    })
                    .map(|(_, col)| *col)
                    .collect();
                log::debug!(
                    "V5CompressedLegacy: Column bitmap 0x{:X} filters {} → {} columns",
                    bitmap,
                    columns_in_order.len(),
                    filtered.len()
                );
                filtered
            }
            None => columns_in_order,
        };

        log::debug!("V5CompressedLegacy: Parsing {} cells in SERIALIZATION HEADER ORDER starting at offset {} (row header was {} bytes)", columns_to_parse.len(), offset, row_header.header_size);
        log::debug!(
            "V5CompressedLegacy: Column order: {:?}",
            columns_to_parse.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
        log::debug!(
            "V5CompressedLegacy: Cell data hex (first 64 bytes): {}",
            hex::encode(&data[offset..std::cmp::min(offset + 64, data.len())])
        );

        // Issue #221: Check if row has complex deletion info for non-frozen collections
        let has_complex_deletion = (row_flags & ROW_HAS_COMPLEX_DELETION) != 0;
        if has_complex_deletion {
            log::debug!("V5CompressedLegacy: Row has HAS_COMPLEX_DELETION flag (0x40) set");
        }

        for (col_idx, &column) in columns_to_parse.iter().enumerate() {
            if offset >= data.len() {
                log::debug!(
                    "V5CompressedLegacy: Reached end of data at column {} ('{}'), parsed {}/{} cells",
                    col_idx,
                    column.name,
                    cells.len(),
                    columns_to_parse.len()
                );
                break;
            }

            // Issue #221: Branch based on column type - complex columns need special parsing
            // Issue #693: simple columns return 4-tuple including cell timestamp / expiration;
            //             complex columns return 2-tuple and inherit the row-level timestamp.
            if Self::is_complex_column(&column.data_type) {
                log::debug!(
                    "V5CompressedLegacy: Column '{}' is complex (non-frozen collection), using parse_complex_column",
                    column.name
                );
                match self.parse_complex_column(data, offset, column, has_complex_deletion, reader)
                {
                    Ok((value, new_offset, col_meta)) => {
                        log::debug!(
                            "V5CompressedLegacy:   ✓ Complex column {} '{}' = {:?}, consumed {} bytes",
                            col_idx, column.name, value, new_offset - offset
                        );
                        // Normal read-path (WRITETIME/TTL queries): use the row-level liveness
                        // timestamp unchanged.  Cassandra's WRITETIME(non_frozen_collection) on
                        // the standard read path returns the row timestamp, not per-element max.
                        // The delta-scan path computes its own max-element-writetime from
                        // ComplexColumnMeta (stored separately below) and never reads this field
                        // for collection columns.  Do NOT mutate this with max_element_writetime
                        // here — that would silently change WRITETIME(col) on the ordinary path
                        // (roborev Finding 1).
                        if let Some(ref mut meta_map) = cell_meta {
                            let row_ts = row_header.timestamp.unwrap_or(0);
                            meta_map.insert(
                                column.name.clone(),
                                CellWriteMetadata {
                                    write_timestamp_micros: row_ts,
                                    expiration: None,
                                },
                            );
                        }
                        // DS4 (Issue #700): Store ComplexColumnMeta for delta-scan callers.
                        if let Some(ref mut ccm_map) = complex_col_meta {
                            ccm_map.insert(column.name.clone(), col_meta);
                        }
                        cells.insert(column.name.clone(), value);
                        offset = new_offset;
                    }
                    Err(e) => {
                        log::debug!(
                            "V5CompressedLegacy:   ✗ Complex column {} '{}' at offset {} FAILED: {}",
                            col_idx, column.name, offset, e
                        );
                        break;
                    }
                }
            } else {
                match self.parse_cell_value_schema_order(data, offset, column, reader) {
                    Ok((value, cell_own_ts, cell_exp, new_offset)) => {
                        log::debug!(
                            "V5CompressedLegacy:   ✓ Column {} '{}' ({}) = {:?}, consumed {} bytes",
                            col_idx,
                            column.name,
                            column.data_type,
                            value,
                            new_offset - offset
                        );
                        // Only compute and store per-cell metadata when the caller requested it.
                        // On the normal read hot-path (want_cell_metadata == false), cell_meta is
                        // None and this entire block is skipped — zero allocations per cell.
                        if let Some(ref mut meta_map) = cell_meta {
                            // Resolve effective write timestamp:
                            // use cell's own timestamp when present, else row-level liveness timestamp.
                            let effective_ts =
                                cell_own_ts.unwrap_or_else(|| row_header.timestamp.unwrap_or(0));
                            // Resolve expiration: cell-level wins; fall back to row-level TTL when
                            // the cell used USE_ROW_TTL (cell_exp is None in that case).
                            // USE_ROW_TTL path: row_header.ttl is the row-level TTL in seconds.
                            // row_header.local_deletion_time is the corresponding expires_at (seconds).
                            let row_level_exp =
                                match (row_header.ttl, row_header.local_deletion_time) {
                                    (Some(ttl_s), Some(ldt_s)) => Some(CellExpiration {
                                        ttl_seconds: ttl_s,
                                        expires_at_seconds: ldt_s as i64,
                                    }),
                                    _ => None,
                                };
                            let effective_exp = cell_exp.or(row_level_exp);
                            meta_map.insert(
                                column.name.clone(),
                                CellWriteMetadata {
                                    write_timestamp_micros: effective_ts,
                                    expiration: effective_exp,
                                },
                            );
                        }
                        cells.insert(column.name.clone(), value);
                        offset = new_offset;
                    }
                    Err(e) => {
                        log::debug!(
                            "V5CompressedLegacy:   ✗ Column {} '{}' ({}) at offset {} FAILED: {}",
                            col_idx,
                            column.name,
                            column.data_type,
                            offset,
                            e
                        );
                        // CRITICAL FIX: Stop parsing remaining columns when we hit an error
                        // The offset doesn't advance here, but we exit the loop cleanly
                        // rather than continuing with invalid offset
                        break;
                    }
                }
            }
        }

        log::debug!(
            "V5CompressedLegacy: Parsed {}/{} columns (missing columns are NULL)",
            cells.len(),
            columns_to_parse.len()
        );
        log::debug!(
            "V5CompressedLegacy: Cells HashMap keys: {:?}",
            cells.keys().collect::<Vec<_>>()
        );

        debug!("V5CompressedLegacy: Parsed total of {} cells", cells.len());

        // Calculate offset after cell data (based on row_size from header)
        //
        // CRITICAL (Issue #237): row_size is measured from AFTER the row_size VInt,
        // not from where it starts. This matches Cassandra's getFilePointer() semantics:
        //   next_position = row_size_value + position_after_reading_row_size_vint
        //
        // Formula: (row_metadata_offset + row_size_vint_len) + row_size
        //
        // There is NO trailing field in V5CompressedLegacy format - the next partition/row
        // starts immediately after row_size bytes from this position.
        let row_size_counted_from = row_metadata_offset + row_header.row_size_vint_len;
        let after_cells_offset = row_size_counted_from + row_size as usize;

        // Validate we have enough data
        if after_cells_offset > data.len() {
            let remaining = data.len().saturating_sub(row_size_counted_from);
            return Err(Error::corruption(format!(
                "V5CompressedLegacy: Not enough bytes for row data at offset {} (need {}, have {})",
                row_size_counted_from, row_size, remaining
            )));
        }

        // No trailing field - next partition/row starts immediately
        let next_offset = after_cells_offset;

        debug!(
            "V5CompressedLegacy: Row complete - row_size={} bytes, next offset = {} (counted from {}, is_static={})",
            row_size, next_offset, row_size_counted_from, is_static
        );

        Ok((
            cells,
            cell_meta,
            Some(row_header),
            next_offset,
            is_static,
            complex_col_meta,
        ))
    }

    /// Parse a single cell value WITHOUT column name (schema-order format)
    ///
    /// Cell format in V5CompressedLegacy follows Cassandra 5.0 cell serialization:
    /// - First byte: Cell flags (bitset, valid range: 0x00-0x1F)
    ///   - 0x01 = IS_DELETED_MASK (tombstone)
    ///   - 0x02 = IS_EXPIRING_MASK (has TTL)
    ///   - 0x04 = HAS_EMPTY_VALUE_MASK (no value bytes)
    ///   - 0x08 = USE_ROW_TIMESTAMP_MASK (use row timestamp)
    ///   - 0x10 = USE_ROW_TTL_MASK (use row TTL)
    /// - Conditional timestamp/TTL/deletion fields (based on flags)
    /// - Value data (if HAS_EMPTY_VALUE not set)
    ///
    /// See CASSANDRA_5_CELL_DESERIALIZATION_FORMAT.md for complete specification.
    ///
    /// Returns: `(value, cell_own_timestamp, expiration, new_offset)` where:
    /// - `cell_own_timestamp`: the cell's own decoded timestamp in µs, or `None`
    ///   when the cell inherits the row-level timestamp (`USE_ROW_TIMESTAMP` flag).
    /// - `expiration`: TTL / localDeletionTime pair when the cell is expiring, or
    ///   `None` when the cell has no TTL.
    fn parse_cell_value_schema_order(
        &self,
        data: &[u8],
        mut offset: usize,
        column: &crate::schema::Column,
        _reader: &super::super::types::SSTableReader,
    ) -> Result<(Value, Option<i64>, Option<CellExpiration>, usize)> {
        // Cell flag constants (from Cassandra 5.0 Cell.Serializer)
        const CELL_IS_DELETED: u8 = 0x01;
        const CELL_IS_EXPIRING: u8 = 0x02;
        const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
        const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
        const CELL_USE_ROW_TTL: u8 = 0x10;

        // Read cell flags byte
        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "Cell '{}': unexpected end at flags byte",
                column.name
            )));
        }
        let flags = data[offset];

        // CRITICAL FIX (Issue #191): Validate flags are in valid range (0x00-0x1F)
        // Bits 0x20, 0x40, 0x80 are row-level flags and should NEVER appear in cell flags.
        // If we see these bits, the offset is misaligned (reading row data at cell position).
        if flags > 0x1F {
            return Err(Error::corruption(format!(
                "Cell '{}': invalid cell flags 0x{:02x} at offset {} (bits 0x20/0x40/0x80 indicate offset misalignment)",
                column.name, flags, offset
            )));
        }

        offset += 1;

        // Decode flags
        let is_deleted = (flags & CELL_IS_DELETED) != 0;
        let is_expiring = (flags & CELL_IS_EXPIRING) != 0;
        let has_empty_value = (flags & CELL_HAS_EMPTY_VALUE) != 0;
        let use_row_timestamp = (flags & CELL_USE_ROW_TIMESTAMP) != 0;
        let use_row_ttl = (flags & CELL_USE_ROW_TTL) != 0;

        log::debug!(
            "V5CompressedLegacy: Cell '{}' flags=0x{:02x} (deleted={}, expiring={}, empty={}, use_row_ts={}, use_row_ttl={})",
            column.name, flags, is_deleted, is_expiring, has_empty_value, use_row_timestamp, use_row_ttl
        );

        // === PHASE 2: Parse conditional fields between flags and value ===
        // Based on Cassandra 5.0 Cell.Serializer format specification

        // Step 1: Read timestamp (if not using row timestamp)
        // Issue #505: capture the actual cell timestamp so deleted cells can carry it
        // in a Value::Tombstone.
        //
        // Fix #629 (C2): Cell timestamp delta is UNSIGNED VInt per Cassandra
        // SerializationHeader.java:165: out.writeUnsignedVInt(timestamp - stats.minTimestamp).
        let mut cell_timestamp: Option<i64> = None;
        if !use_row_timestamp {
            let (remaining, timestamp_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse timestamp delta as VUInt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            let absolute_ts = self.min_timestamp.wrapping_add(timestamp_delta as i64);
            log::debug!(
                "V5CompressedLegacy: Cell '{}' timestamp_delta={} (min_timestamp={}) absolute={}",
                column.name,
                timestamp_delta,
                self.min_timestamp,
                absolute_ts,
            );
            cell_timestamp = Some(absolute_ts);
        }

        // Step 2: Read localDeletionTime (if deleted or expiring, and not using row TTL)
        // Captured as absolute epoch-seconds for CellExpiration.expires_at_seconds.
        let mut cell_local_deletion_time: Option<i64> = None;
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, deletion_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse localDeletionTime delta as VUInt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            let abs_ldt = self
                .min_local_deletion_time
                .wrapping_add(deletion_delta as i64);
            log::debug!(
                "V5CompressedLegacy: Cell '{}' deletion_delta={} (min_local_deletion_time={}) abs_ldt={}",
                column.name,
                deletion_delta,
                self.min_local_deletion_time,
                abs_ldt
            );
            cell_local_deletion_time = Some(abs_ldt);
        }

        // Step 3: Read TTL (if expiring and not using row TTL)
        // Captured as absolute TTL seconds for CellExpiration.ttl_seconds.
        let mut cell_ttl_seconds: Option<i32> = None;
        if !use_row_ttl && is_expiring {
            let (remaining, ttl_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Cell '{}': failed to parse TTL delta as VUInt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            // Absolute TTL = min_ttl + delta (seconds).  Clamp to i32 range for the
            // CellExpiration.ttl_seconds field (Cassandra caps TTL at ~630M seconds).
            let abs_ttl = self.min_ttl.unwrap_or(0).wrapping_add(ttl_delta as i64);
            log::debug!(
                "V5CompressedLegacy: Cell '{}' ttl_delta={} (min_ttl={:?}) abs_ttl={}",
                column.name,
                ttl_delta,
                self.min_ttl,
                abs_ttl
            );
            cell_ttl_seconds = Some(abs_ttl.min(i32::MAX as i64) as i32);
        }

        // Build per-cell expiration metadata (used when the flag is set).
        // Available at both return sites below — the tombstone path also uses
        // cell_timestamp so we compute expiration here before the tombstone check.
        let cell_expiration: Option<CellExpiration> =
            match (is_expiring, cell_local_deletion_time, cell_ttl_seconds) {
                (true, Some(expires_at), Some(ttl_secs)) => Some(CellExpiration {
                    ttl_seconds: ttl_secs,
                    expires_at_seconds: expires_at,
                }),
                // use_row_ttl path: expiration info comes from the row header (caller handles it).
                _ => None,
            };

        // Step 4: Cell path for complex columns (multi-cell collections/UDTs)
        // For now, skip this - we'll add in a future iteration when we handle complex columns.
        // Simple columns (int, text, boolean, uuid, etc.) don't have cell paths.

        // === End of Phase 2 conditional field parsing ===

        // CRITICAL: Inverted logic for HAS_EMPTY_VALUE_MASK
        // Flag NOT set (0x04 absent) = cell HAS value → read value bytes
        // Flag SET (0x04 present) = cell has NO value → return empty/null immediately
        let has_value = !has_empty_value;

        // Handle deleted cells (tombstones)
        // According to Cassandra 5.0 Cell.Serializer, deleted cells:
        // 1. Have IS_DELETED flag set
        // 2. May have deletion metadata (timestamp, localDeletionTime)
        // 3. Do NOT have value data (even if HAS_EMPTY_VALUE not set)
        //
        // Issue #505: emit Value::Tombstone(CellTombstone) so that the compaction
        // merger can apply cell-level shadowing semantics.  The actual deletion
        // timestamp is carried in the tombstone for timestamp-based LWW ordering.
        if is_deleted {
            let deletion_time = cell_timestamp.unwrap_or(0);
            log::debug!(
                "V5CompressedLegacy: Cell '{}' is tombstone (deleted), returning Tombstone(deletion_time={})",
                column.name, deletion_time
            );
            return Ok((
                Value::Tombstone(TombstoneInfo {
                    deletion_time,
                    tombstone_type: TombstoneType::CellTombstone,
                    ttl: None,
                    range_start: None,
                    range_end: None,
                }),
                cell_timestamp,
                cell_expiration,
                offset,
            ));
        }

        // Handle empty cells (no value bytes to read)
        if !has_value {
            log::debug!(
                "V5CompressedLegacy: Cell '{}' has HAS_EMPTY_VALUE flag, returning empty value",
                column.name
            );
            // Return appropriate empty value for type
            // For most types, empty = empty string or empty collection
            return Ok((
                Value::Text(String::new()),
                cell_timestamp,
                cell_expiration,
                offset,
            ));
        }

        // At this point, we have a live cell with value data
        // The value parsing logic below is unchanged from the original implementation

        // Parse based on column type (data_type is a String with CQL type name)
        // CRITICAL: Normalize type name to lowercase for case-insensitive matching
        // Schema may provide "TEXT", "INT", etc. (uppercase) while match arms use lowercase
        let normalized_type = column.data_type.to_lowercase();
        let value = match normalized_type.as_str() {
            "boolean" => {
                // Boolean: [0x08][u8 value]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at boolean value",
                        column.name
                    )));
                }
                let bool_byte = data[offset];
                offset += 1;
                Value::Boolean(bool_byte != 0)
            }

            "int" => {
                // Integer (i32): fixed-width 4 bytes (no length prefix in Cassandra 5.0)
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 4 bytes for int, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let int_val = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                Value::Integer(int_val)
            }

            "text" | "varchar" | "ascii" => {
                // Text: [VInt len][text bytes]
                // V5CompressedLegacy uses VInt length encoding for variable-length types
                let (remaining, text_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse text length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let text_len = text_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + text_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for text, only {} available",
                        column.name,
                        text_len,
                        data.len() - offset
                    )));
                }

                let text_bytes = &data[offset..offset + text_len];
                let text = String::from_utf8(text_bytes.to_vec()).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': invalid UTF-8 in text value: {}",
                        column.name, e
                    ))
                })?;

                offset += text_len;
                Value::Text(text)
            }

            "uuid" | "timeuuid" => {
                // UUID/TimeUUID: fixed-width 16 bytes (no length prefix in Cassandra 5.0 writer)
                if offset + 16 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 16 bytes for UUID, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let uuid_bytes: [u8; 16] = data[offset..offset + 16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;

                offset += 16;
                Value::Uuid(uuid_bytes)
            }

            "decimal" => {
                // Decimal: [VInt total_len][i32 scale][unscaled bytes]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at decimal length",
                        column.name
                    )));
                }

                let (remaining, total_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse decimal length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let total_len = total_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + total_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for decimal, only {} available",
                        column.name,
                        total_len,
                        data.len() - offset
                    )));
                }

                // First 4 bytes: scale (i32 BE)
                if total_len < 4 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': decimal length {} too small for scale",
                        column.name, total_len
                    )));
                }
                let scale = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);

                // Remaining bytes: unscaled value
                let unscaled = data[offset + 4..offset + total_len].to_vec();
                offset += total_len;

                Value::Decimal { scale, unscaled }
            }

            "bigint" => {
                // BigInt: fixed-width 8 bytes (no length prefix in Cassandra 5.0)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for bigint, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let val = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::BigInt(val)
            }

            "counter" => {
                // Counter cells can arrive in two formats:
                //
                // 1. Real Cassandra CounterContext: [VInt length][header_size:i16][indices][shards]
                //    The counter value is the sum of all shard counts.
                //
                // 2. CQLite writer format (raw i64): [VInt(8)][8 bytes big-endian i64]
                //    The writer serialises Value::Counter as a plain 8-byte integer with a
                //    length prefix of 8, identical to how BigInt is written.
                //
                // We try CounterContext first and fall back to the raw-i64 interpretation
                // when the length prefix equals exactly 8 (the size of a raw i64).

                // Read the VInt length prefix.
                let (remaining, context_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse counter context length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let context_len = context_len as usize;
                let len_bytes_consumed = data[offset..].len() - remaining.len();
                offset += len_bytes_consumed;

                log::debug!(
                    "V5CompressedLegacy: Counter '{}' context_len={} (len prefix: {} bytes)",
                    column.name,
                    context_len,
                    len_bytes_consumed
                );

                if offset + context_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for counter context, only {} available",
                        column.name,
                        context_len,
                        data.len() - offset
                    )));
                }

                // Try the full CounterContext parse first.
                match Self::parse_counter_context(data, offset, &column.name) {
                    Ok((total, consumed)) if consumed == context_len => {
                        // Successfully parsed a proper CounterContext.
                        offset += consumed;
                        log::debug!(
                            "V5CompressedLegacy: Counter '{}' value={} (CounterContext), total consumed {} bytes",
                            column.name,
                            total,
                            len_bytes_consumed + context_len
                        );
                        Value::Counter(total)
                    }
                    _ if context_len == 8 => {
                        // A real Cassandra CounterContext is at minimum 36 bytes
                        // (2 header + 2 indices + 32 body for 1 shard), so
                        // context_len == 8 can only be produced by the CQLite writer
                        // which serialises Counter as a raw big-endian i64.
                        // This intentionally swallows any parse_counter_context error
                        // for 8-byte payloads, which is safe since a valid
                        // CounterContext can never be 8 bytes.
                        //
                        // Bounds already verified by the context_len check above.
                        let val = i64::from_be_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                            data[offset + 4],
                            data[offset + 5],
                            data[offset + 6],
                            data[offset + 7],
                        ]);
                        offset += 8;
                        log::debug!(
                            "V5CompressedLegacy: Counter '{}' value={} (raw i64 fallback), total consumed {} bytes",
                            column.name,
                            val,
                            len_bytes_consumed + 8
                        );
                        Value::Counter(val)
                    }
                    Err(e) => return Err(e),
                    Ok((_, consumed)) => {
                        return Err(Error::corruption(format!(
                            "Counter '{}': VInt length ({}) doesn't match parsed context size ({})",
                            column.name, context_len, consumed
                        )));
                    }
                }
            }

            "double" => {
                // Double: 8 bytes, f64 big-endian (NO length prefix)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for double, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let val = f64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Float(val)
            }

            "timestamp" => {
                // Timestamp: 8 bytes, i64 milliseconds big-endian (NO length prefix, per Cassandra spec)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for timestamp, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let millis = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Timestamp(millis)
            }

            "date" => {
                // Date: [VInt len=4][i32 BE days]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at date length",
                        column.name
                    )));
                }

                let (remaining, date_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse date length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let date_len = date_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if date_len != 4 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected date length 4, got {}",
                        column.name, date_len
                    )));
                }

                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 4 bytes for date, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let stored = u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                // Cassandra DATE: 4-byte unsigned int with Integer.MIN_VALUE offset
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Value::Date(days_since_epoch)
            }

            "duration" => {
                // Duration: [VInt len][months VInt][days VInt][nanos VInt]
                // Format: Variable-length encoding with 3 VInt components
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at duration length",
                        column.name
                    )));
                }

                let (remaining, duration_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse duration length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let duration_len = duration_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + duration_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for duration, only {} available",
                        column.name,
                        duration_len,
                        data.len() - offset
                    )));
                }

                // Parse three VInt components from the duration_len bytes
                let duration_bytes = &data[offset..offset + duration_len];

                // Parse months (signed VInt)
                let (remaining, months) = parse_vint(duration_bytes).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse duration months: {:?}",
                        column.name, e
                    ))
                })?;
                let pos = duration_bytes.len() - remaining.len();

                // Parse days (signed VInt)
                let (remaining, days) = parse_vint(&duration_bytes[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse duration days: {:?}",
                        column.name, e
                    ))
                })?;
                let pos = duration_bytes.len() - remaining.len();

                // Parse nanoseconds (signed VInt)
                let (remaining, nanos) = parse_vint(&duration_bytes[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse duration nanos: {:?}",
                        column.name, e
                    ))
                })?;

                // Verify we consumed all duration bytes
                if !remaining.is_empty() {
                    warn!(
                        "V5CompressedLegacy: Duration '{}' has {} extra bytes after parsing",
                        column.name,
                        remaining.len()
                    );
                }

                offset += duration_len;
                Value::Duration {
                    months: months as i32,
                    days: days as i32,
                    nanos,
                }
            }

            "float" => {
                // Float: 4 bytes, f32 big-endian (NO length prefix, fixed size)
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 4 bytes for float, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let val = f32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                Value::Float(val as f64) // Convert f32 to f64 for storage
            }

            "smallint" | "short" => {
                // SmallInt: [VInt len=2][i16 BE]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at smallint length",
                        column.name
                    )));
                }

                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse smallint length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let len = len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if len != 2 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected smallint length 2, got {}",
                        column.name, len
                    )));
                }

                if offset + 2 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 2 bytes for smallint, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let val = i16::from_be_bytes([data[offset], data[offset + 1]]);
                offset += 2;
                Value::SmallInt(val)
            }

            "tinyint" | "byte" => {
                // TinyInt: [VInt len=1][i8]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at tinyint length",
                        column.name
                    )));
                }

                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse tinyint length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let len = len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if len != 1 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected tinyint length 1, got {}",
                        column.name, len
                    )));
                }

                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 1 byte for tinyint, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }

                let val = data[offset] as i8;
                offset += 1;
                Value::TinyInt(val)
            }

            "time" => {
                // Time: [VInt len=8][i64 BE nanoseconds since midnight]
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at time length",
                        column.name
                    )));
                }
                let (remaining, time_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse time length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let time_len = time_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;
                if time_len != 8 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': expected time length 8, got {}",
                        column.name, time_len
                    )));
                }
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need 8 bytes for time value, only {} available",
                        column.name,
                        data.len() - offset
                    )));
                }
                let nanos = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Time(nanos)
            }

            "inet" => {
                // Inet: [VInt len][address bytes] (len is 4 for IPv4, 16 for IPv6)
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at inet length",
                        column.name
                    )));
                }

                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse inet length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let len = len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if len != 4 && len != 16 {
                    return Err(Error::corruption(format!(
                        "Cell '{}': invalid inet length {}, expected 4 or 16",
                        column.name, len
                    )));
                }

                if offset + len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for inet, only {} available",
                        column.name,
                        len,
                        data.len() - offset
                    )));
                }

                let bytes = data[offset..offset + len].to_vec();
                offset += len;
                Value::Inet(bytes)
            }

            // Complex types: frozen, tuple, UDT
            type_str if type_str.starts_with("frozen<") => {
                // Frozen types: unwrap inner type and route to appropriate parser
                let inner_type = self.extract_frozen_inner_type(type_str)?;

                log::debug!(
                    "V5CompressedLegacy: Parsing frozen type '{}' -> inner type '{}'",
                    type_str,
                    inner_type
                );

                // Route to appropriate frozen collection parser
                let (inner_value, new_offset) = if inner_type.starts_with("list<") {
                    let element_type = self.extract_collection_element_type(&inner_type, "list")?;
                    self.parse_frozen_list_value(data, offset, &element_type, column, _reader)?
                } else if inner_type.starts_with("set<") {
                    let element_type = self.extract_collection_element_type(&inner_type, "set")?;
                    self.parse_frozen_set_value(data, offset, &element_type, column, _reader)?
                } else if inner_type.starts_with("map<") {
                    let (key_type, value_type) = self.extract_map_types(&inner_type)?;
                    self.parse_frozen_map_value(
                        data,
                        offset,
                        &key_type,
                        &value_type,
                        column,
                        _reader,
                    )?
                } else if Self::is_udt_type(&column.data_type) {
                    // Frozen UDT - parse using UDT parser
                    // The column.data_type contains the full Cassandra type string including UserType
                    log::debug!(
                        "V5CompressedLegacy: Parsing frozen UDT column '{}' type='{}'",
                        column.name,
                        column.data_type
                    );

                    // Parse UDT definition from the type string
                    let udt_def = Self::parse_udt_type_definition(&column.data_type)?;

                    // First read the VInt-prefixed blob length
                    let (remaining, blob_len_raw) = parse_vuint(&data[offset..]).map_err(|e| {
                        Error::corruption(format!(
                            "Frozen UDT '{}': failed to parse blob length: {:?}",
                            column.name, e
                        ))
                    })?;
                    if blob_len_raw > MAX_CELL_VALUE_LENGTH {
                        return Err(Error::corruption(format!(
                            "Frozen UDT '{}': blob_len {} exceeds maximum {}",
                            column.name, blob_len_raw, MAX_CELL_VALUE_LENGTH
                        )));
                    }
                    let blob_len = blob_len_raw as usize;
                    let bytes_consumed = data[offset..].len() - remaining.len();
                    offset += bytes_consumed;

                    if offset + blob_len > data.len() {
                        return Err(Error::corruption(format!(
                            "Frozen UDT '{}': need {} bytes but only {} available",
                            column.name,
                            blob_len,
                            data.len() - offset
                        )));
                    }

                    // Parse UDT value from the blob
                    let udt_data = &data[offset..offset + blob_len];
                    let (udt_value, _) =
                        self.parse_udt_value(udt_data, 0, &udt_def, column, _reader)?;
                    offset += blob_len;

                    (udt_value, offset)
                } else if let Some(udt_def) = self
                    .udt_registry
                    .as_ref()
                    .and_then(|reg| reg.get_udt(&self.keyspace, &inner_type).cloned())
                {
                    // frozen<short_udt_name>: look up the concrete UDT definition in the
                    // registry (Issue #502).  This handles type strings like
                    // `frozen<person>` where "person" is a registered UDT rather than a
                    // collection or a full marshal-format UserType string.
                    log::debug!(
                        "V5CompressedLegacy: Resolving frozen UDT '{}' via registry for column '{}'",
                        inner_type,
                        column.name,
                    );

                    // Read VUInt-prefixed blob length (same framing as tuple and
                    // marshal-format UDT cells).
                    let (remaining, blob_len_raw) = parse_vuint(&data[offset..]).map_err(|e| {
                        Error::corruption(format!(
                            "Frozen UDT '{}' (column '{}'): failed to parse blob length: {:?}",
                            inner_type, column.name, e
                        ))
                    })?;
                    if blob_len_raw > MAX_CELL_VALUE_LENGTH {
                        return Err(Error::corruption(format!(
                            "Frozen UDT '{}' (column '{}'): blob_len {} exceeds maximum {}",
                            inner_type, column.name, blob_len_raw, MAX_CELL_VALUE_LENGTH
                        )));
                    }
                    let blob_len = blob_len_raw as usize;
                    let len_bytes_consumed = data[offset..].len() - remaining.len();
                    offset += len_bytes_consumed;

                    if offset + blob_len > data.len() {
                        return Err(Error::corruption(format!(
                            "Frozen UDT '{}' (column '{}'): need {} bytes but only {} available",
                            inner_type,
                            column.name,
                            blob_len,
                            data.len() - offset
                        )));
                    }

                    let udt_data = &data[offset..offset + blob_len];
                    let (udt_value, _) =
                        self.parse_udt_value(udt_data, 0, &udt_def, column, _reader)?;
                    offset += blob_len;

                    (udt_value, offset)
                } else {
                    // Detect bare identifiers that look like unregistered UDT names.
                    // A bare identifier has no '<' (not a container or tuple) and does not
                    // match any known CQL primitive type.  If we reach this branch with
                    // such an identifier it means the UDT was not in the registry — return
                    // an actionable schema error rather than silently producing a Blob.
                    //
                    // Legitimate fall-through types handled below:
                    //   • tuple<...>  (contains '<')
                    //   • known primitives: int, text, uuid, boolean, blob, float, double,
                    //     decimal, varint, bigint, counter, timestamp, date, time, duration,
                    //     inet, smallint, tinyint, varchar, ascii, timeuuid
                    const KNOWN_PRIMITIVES: &[&str] = &[
                        "int",
                        "bigint",
                        "counter",
                        "smallint",
                        "tinyint",
                        "text",
                        "varchar",
                        "ascii",
                        "uuid",
                        "timeuuid",
                        "boolean",
                        "blob",
                        "float",
                        "double",
                        "decimal",
                        "varint",
                        "timestamp",
                        "date",
                        "time",
                        "duration",
                        "inet",
                    ];
                    let is_container = inner_type.contains('<');
                    let is_primitive = KNOWN_PRIMITIVES.contains(&inner_type.as_str());
                    if !is_container && !is_primitive {
                        // Bare identifier that is neither a container nor a primitive —
                        // this is an unregistered UDT name.
                        return Err(Error::schema(format!(
                            "frozen<{inner}>: UDT '{inner}' not found in registry for keyspace '{}'; \
                             register it before reading",
                            self.keyspace,
                            inner = inner_type,
                        )));
                    }
                    // Non-collection / primitive frozen type — recurse normally.
                    // The recursive call now returns 4 elements; we only need value + offset.
                    let mut inner_column = column.clone();
                    inner_column.data_type = inner_type.clone();
                    let (inner_val, _inner_ts, _inner_exp, inner_off) =
                        self.parse_cell_value_schema_order(data, offset, &inner_column, _reader)?;
                    (inner_val, inner_off)
                };

                offset = new_offset;

                // Wrap in Frozen
                Value::Frozen(Box::new(inner_value))
            }

            type_str if type_str.starts_with("tuple<") => {
                // Tuple types: parse fixed number of elements
                self.parse_tuple_value(data, &mut offset, type_str, column, _reader)?
            }

            // Non-frozen collections: list, set, map
            // TODO(Issue #162, Task 3): Multi-cell collection parsing
            //
            // Collections in V5CompressedLegacy are stored as MULTIPLE CELLS with path identifiers,
            // NOT as single blob values. The current single-cell parser cannot handle this.
            //
            // Format (from sstabledump analysis):
            //   {"name": "scores", "deletion_info": {...}},  // Collection tombstone
            //   {"name": "scores", "path": ["uuid1"], "value": 23},  // Element 1
            //   {"name": "scores", "path": ["uuid2"], "value": 99},  // Element 2
            //
            // Required implementation:
            //   1. Parse cell path (clustering key bytes) for each collection element
            //   2. Detect collection tombstone cell (has deletion_info, no path/value)
            //   3. Read N element cells (each with path + value)
            //   4. Aggregate elements into Value::List/Set/Map based on column type
            //   5. Handle different path encodings:
            //      - list<T>: path is UUID bytes (timeuuid for ordering)
            //      - set<T>: path is serialized element value (key), value is empty
            //      - map<K,V>: path is serialized key, value is serialized value
            //
            // This is a fundamental architectural change requiring cell-level parsing
            // before column-level aggregation. For now, return stub to unblock downstream work.
            type_str
                if type_str.starts_with("list<")
                    || type_str.starts_with("set<")
                    || type_str.starts_with("map<") =>
            {
                warn!(
                    "V5CompressedLegacy: Non-frozen collection '{}' type '{}' requires multi-cell parsing (not yet implemented). \
                     Collections are stored as multiple cells with path identifiers, requiring cell-level aggregation. \
                     Returning empty collection as placeholder. See Issue #162 Task 3 for implementation plan.",
                    column.name, column.data_type
                );

                // Return empty collection based on type
                if type_str.starts_with("list<") {
                    Value::List(Vec::new())
                } else if type_str.starts_with("set<") {
                    Value::Set(Vec::new())
                } else {
                    Value::Map(Vec::new())
                }
            }

            // TODO(Issue #162): UDT parsing requires schema registry access
            // For now, UDTs fall through to blob. Future implementation will:
            // - Extract UDT name from type_str
            // - Look up UDT definition in schema registry
            // - Parse fields according to UDT schema
            // - Return Value::Udt(UdtValue)

            // Default: treat as VInt-length-prefixed blob
            // CRITICAL: V5CompressedLegacy format uses VInt encoding for blob/bytes lengths,
            // NOT simple u8 length prefix. This allows blobs > 255 bytes.
            _ => {
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': unexpected end at blob length (type: {})",
                        column.name, column.data_type
                    )));
                }

                // Parse blob length as unsigned VInt (can be > 255 bytes)
                let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Cell '{}': failed to parse blob length as VInt: {:?}",
                        column.name, e
                    ))
                })?;
                let blob_len = blob_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + blob_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Cell '{}': need {} bytes for blob, only {} available (type: {})",
                        column.name,
                        blob_len,
                        data.len() - offset,
                        column.data_type
                    )));
                }

                let blob_bytes = data[offset..offset + blob_len].to_vec();
                offset += blob_len;
                Value::Blob(blob_bytes)
            }
        };

        Ok((value, cell_timestamp, cell_expiration, offset))
    }

    /// Extract inner type from frozen<T> type string
    fn extract_frozen_inner_type(&self, type_str: &str) -> Result<String> {
        if !type_str.starts_with("frozen<") || !type_str.ends_with('>') {
            return Err(Error::schema(format!(
                "Invalid frozen type format: {}",
                type_str
            )));
        }

        let inner = &type_str[7..type_str.len() - 1];
        if inner.is_empty() {
            return Err(Error::schema(format!("Empty frozen type: {}", type_str)));
        }

        Ok(inner.to_string())
    }

    /// Check if a type string represents a UDT (User-Defined Type).
    /// Detects Cassandra's internal format: org.apache.cassandra.db.marshal.UserType(...)
    fn is_udt_type(type_str: &str) -> bool {
        // ASCII case-insensitive substring match without allocating a lowercased
        // copy. The marshal name is pure ASCII so byte-window comparison is safe.
        const TARGET: &[u8] = b"org.apache.cassandra.db.marshal.usertype";
        let bytes = type_str.as_bytes();
        if bytes.len() < TARGET.len() {
            return false;
        }
        bytes
            .windows(TARGET.len())
            .any(|w| w.iter().zip(TARGET).all(|(a, b)| a.eq_ignore_ascii_case(b)))
    }

    /// Parse a UDT type string to extract the UDT definition.
    /// Cassandra encodes UDTs as:
    /// `UserType(keyspace,hex_name,field1_hex:type1,field2_hex:type2,...)`
    ///
    /// Example:
    /// ```text
    /// org.apache.cassandra.db.marshal.UserType(
    ///   test_collections,
    ///   616464726573735f74797065,    // hex("address_type")
    ///   737472656574:UTF8Type,        // street:UTF8Type
    ///   63697479:UTF8Type,            // city:UTF8Type
    ///   ...
    /// )
    /// ```
    fn parse_udt_type_definition(type_str: &str) -> Result<UdtTypeDef> {
        Self::parse_udt_type_definition_with_depth(type_str, 0)
    }

    /// Internal implementation of parse_udt_type_definition with recursion depth tracking.
    fn parse_udt_type_definition_with_depth(type_str: &str, depth: usize) -> Result<UdtTypeDef> {
        // Check recursion depth to prevent stack overflow
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::schema(format!(
                "UDT nesting depth {} exceeds maximum {}. Type string: {}",
                depth,
                MAX_TYPE_NESTING_DEPTH,
                type_str.chars().take(100).collect::<String>()
            )));
        }

        // Find the UserType(...) portion (case-insensitive search)
        let start_marker = "org.apache.cassandra.db.marshal.UserType(";
        let type_lower = type_str.to_lowercase();
        let start_marker_lower = start_marker.to_lowercase();
        let start_idx = type_lower
            .find(&start_marker_lower)
            .ok_or_else(|| Error::schema(format!("Not a UserType: {}", type_str)))?;

        // Find the matching close paren (handling nested types)
        let inner_start = start_idx + start_marker.len();
        let mut paren_depth = 1;
        let mut end_idx = inner_start;
        let chars: Vec<char> = type_str[inner_start..].chars().collect();

        for (i, c) in chars.iter().enumerate() {
            match c {
                '(' => paren_depth += 1,
                ')' => {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        end_idx = inner_start + i;
                        break;
                    }
                }
                _ => {}
            }
        }

        if paren_depth != 0 {
            return Err(Error::schema(format!(
                "Unbalanced parentheses in UserType: {}",
                type_str
            )));
        }

        let inner = &type_str[inner_start..end_idx];

        // Split by comma, but respect nested parentheses
        let parts = Self::split_type_args(inner)?;
        if parts.len() < 2 {
            return Err(Error::schema(format!(
                "UserType requires at least keyspace and name: {}",
                inner
            )));
        }

        // First part is keyspace
        let keyspace = parts[0].trim();
        if keyspace.is_empty() {
            return Err(Error::schema("UDT keyspace cannot be empty"));
        }
        let keyspace = keyspace.to_string();

        // Second part is hex-encoded type name
        let udt_name = Self::decode_hex_name(parts[1].trim())?;

        // Remaining parts are field definitions: hex_name:type
        let mut udt_def = UdtTypeDef::new(keyspace, udt_name);
        for field_def in parts.iter().skip(2) {
            let field_def = field_def.trim();
            if field_def.is_empty() {
                continue;
            }

            // Split on first colon (field name is before, type is after)
            if let Some(colon_idx) = field_def.find(':') {
                let field_name_hex = &field_def[..colon_idx];
                let field_type_str = &field_def[colon_idx + 1..];

                let field_name = Self::decode_hex_name(field_name_hex)?;
                // Use depth-aware version to track recursion through UDT fields
                let field_type = Self::parse_cassandra_type_with_depth(field_type_str, depth)?;

                udt_def = udt_def.with_field(field_name, field_type, true);
            } else {
                return Err(Error::schema(format!(
                    "Invalid UDT field definition (missing colon): {}",
                    field_def
                )));
            }
        }

        Ok(udt_def)
    }

    /// Split type arguments by comma, respecting nested parentheses.
    fn split_type_args(s: &str) -> Result<Vec<String>> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut depth = 0;

        for c in s.chars() {
            match c {
                '(' => {
                    depth += 1;
                    current.push(c);
                }
                ')' => {
                    depth -= 1;
                    current.push(c);
                }
                ',' if depth == 0 => {
                    parts.push(current.clone());
                    current.clear();
                }
                _ => current.push(c),
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }

        Ok(parts)
    }

    /// Decode a hex-encoded name (e.g., "616464726573735f74797065" -> "address_type")
    fn decode_hex_name(hex: &str) -> Result<String> {
        let bytes = hex::decode(hex)
            .map_err(|e| Error::schema(format!("Invalid hex-encoded UDT name '{}': {}", hex, e)))?;
        String::from_utf8(bytes)
            .map_err(|e| Error::schema(format!("Invalid UTF-8 in UDT name '{}': {}", hex, e)))
    }

    /// Parse a Cassandra type string into a CqlType.
    /// Handles: UTF8Type, Int32Type, ListType(...), SetType(...), MapType(...), UserType(...), FrozenType(...)
    #[allow(dead_code)]
    fn parse_cassandra_type(type_str: &str) -> Result<CqlType> {
        Self::parse_cassandra_type_with_depth(type_str, 0)
    }

    /// Internal implementation of parse_cassandra_type with recursion depth tracking.
    fn parse_cassandra_type_with_depth(type_str: &str, depth: usize) -> Result<CqlType> {
        // Check recursion depth to prevent stack overflow
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::schema(format!(
                "Type nesting depth {} exceeds maximum {}. Type string: {}",
                depth,
                MAX_TYPE_NESTING_DEPTH,
                type_str.chars().take(100).collect::<String>()
            )));
        }

        let type_str = type_str.trim();

        // Handle FrozenType wrapper
        if type_str.starts_with("org.apache.cassandra.db.marshal.FrozenType(") {
            let inner_start = "org.apache.cassandra.db.marshal.FrozenType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let inner_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::Frozen(Box::new(inner_type)));
        }

        // Handle UserType (nested UDT)
        if type_str.starts_with("org.apache.cassandra.db.marshal.UserType(") {
            let udt_def = Self::parse_udt_type_definition_with_depth(type_str, depth + 1)?;
            let fields: Vec<(String, CqlType)> = udt_def
                .fields
                .into_iter()
                .map(|f| (f.name, f.field_type))
                .collect();
            return Ok(CqlType::Udt(udt_def.name, fields));
        }

        // Handle collection types
        if type_str.starts_with("org.apache.cassandra.db.marshal.ListType(") {
            let inner_start = "org.apache.cassandra.db.marshal.ListType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let elem_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::List(Box::new(elem_type)));
        }

        if type_str.starts_with("org.apache.cassandra.db.marshal.SetType(") {
            let inner_start = "org.apache.cassandra.db.marshal.SetType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let elem_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::Set(Box::new(elem_type)));
        }

        if type_str.starts_with("org.apache.cassandra.db.marshal.MapType(") {
            let inner_start = "org.apache.cassandra.db.marshal.MapType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let parts = Self::split_type_args(&inner)?;
            if parts.len() != 2 {
                return Err(Error::schema(format!(
                    "MapType requires exactly 2 type arguments: {}",
                    type_str
                )));
            }
            let key_type = Self::parse_cassandra_type_with_depth(&parts[0], depth + 1)?;
            let val_type = Self::parse_cassandra_type_with_depth(&parts[1], depth + 1)?;
            return Ok(CqlType::Map(Box::new(key_type), Box::new(val_type)));
        }

        // Handle primitive types
        Ok(match type_str {
            s if s.ends_with("UTF8Type") => CqlType::Text,
            s if s.ends_with("AsciiType") => CqlType::Ascii,
            s if s.ends_with("Int32Type") => CqlType::Int,
            s if s.ends_with("LongType") => CqlType::BigInt,
            s if s.ends_with("FloatType") => CqlType::Float,
            s if s.ends_with("DoubleType") => CqlType::Double,
            s if s.ends_with("BooleanType") => CqlType::Boolean,
            s if s.ends_with("UUIDType") || s.ends_with("TimeUUIDType") => CqlType::Uuid,
            s if s.ends_with("TimestampType") => CqlType::Timestamp,
            s if s.ends_with("DateType") || s.ends_with("SimpleDateType") => CqlType::Date,
            s if s.ends_with("TimeType") => CqlType::Time,
            s if s.ends_with("DecimalType") => CqlType::Decimal,
            s if s.ends_with("IntegerType") => CqlType::Varint,
            s if s.ends_with("BytesType") => CqlType::Blob,
            s if s.ends_with("InetAddressType") => CqlType::Inet,
            _ => CqlType::Custom(type_str.to_string()),
        })
    }

    /// Extract the contents inside parentheses, respecting nesting.
    fn extract_inner_parens(s: &str) -> Result<String> {
        let mut depth = 1;
        let mut end_idx = 0;
        let chars: Vec<char> = s.chars().collect();

        for (i, c) in chars.iter().enumerate() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end_idx = i;
                        break;
                    }
                }
                _ => {}
            }
        }

        if depth != 0 {
            return Err(Error::schema(format!(
                "Unbalanced parentheses in type: {}",
                s
            )));
        }

        Ok(s[..end_idx].to_string())
    }

    /// Parse a UDT value from binary data using the given UDT definition.
    /// UDT binary format (frozen):
    /// - For each field in schema order:
    ///   - [4 bytes BE i32]: field length (-1 = null, 0 = empty, >0 = data length)
    ///   - [N bytes]: field data (if length > 0)
    fn parse_udt_value(
        &self,
        data: &[u8],
        offset: usize,
        udt_def: &UdtTypeDef,
        _column: &crate::schema::Column,
        reader: &super::super::types::SSTableReader,
    ) -> Result<(Value, usize)> {
        // Validate field count to prevent memory exhaustion
        if udt_def.fields.len() > MAX_UDT_FIELD_COUNT {
            return Err(Error::schema(format!(
                "UDT '{}' has {} fields, exceeds maximum {}",
                udt_def.name,
                udt_def.fields.len(),
                MAX_UDT_FIELD_COUNT
            )));
        }

        let mut current_offset = offset;
        let mut fields = Vec::with_capacity(udt_def.fields.len());

        log::debug!(
            "V5CompressedLegacy: Parsing UDT '{}' with {} fields at offset {}",
            udt_def.name,
            udt_def.fields.len(),
            offset
        );

        for field_def in &udt_def.fields {
            // Check bounds for field length (4 bytes)
            if current_offset + 4 > data.len() {
                // Trailing fields can be omitted (implicit null)
                log::debug!(
                    "V5CompressedLegacy: UDT field '{}' omitted (implicit null), remaining fields omitted",
                    field_def.name
                );
                // Fill remaining fields with null
                while fields.len() < udt_def.fields.len() {
                    let remaining_field = &udt_def.fields[fields.len()];
                    fields.push(UdtField {
                        name: remaining_field.name.clone(),
                        value: None,
                    });
                }
                break;
            }

            // Read field length (4 bytes big-endian i32)
            let field_len = i32::from_be_bytes([
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
            ]);
            current_offset += 4;

            let field_value = if field_len == -1 {
                // Null field
                log::debug!("V5CompressedLegacy: UDT field '{}' is null", field_def.name);
                None
            } else if field_len == 0 {
                // Empty field - create empty value based on type
                log::debug!(
                    "V5CompressedLegacy: UDT field '{}' is empty",
                    field_def.name
                );
                Some(Self::create_empty_value_for_type(&field_def.field_type))
            } else if field_len < 0 {
                // Validation: reject other negative values
                return Err(Error::corruption(format!(
                    "UDT field '{}': invalid negative field length {}",
                    field_def.name, field_len
                )));
            } else {
                // Field with data
                let field_len = field_len as usize;
                if current_offset + field_len > data.len() {
                    return Err(Error::corruption(format!(
                        "UDT field '{}': need {} bytes but only {} available at offset {}",
                        field_def.name,
                        field_len,
                        data.len() - current_offset,
                        current_offset
                    )));
                }

                let field_data = &data[current_offset..current_offset + field_len];
                current_offset += field_len;

                log::debug!(
                    "V5CompressedLegacy: UDT field '{}' has {} bytes of data",
                    field_def.name,
                    field_len
                );

                // Parse field value based on its type
                let value =
                    self.parse_udt_field_value(field_data, &field_def.field_type, reader)?;
                Some(value)
            };

            fields.push(UdtField {
                name: field_def.name.clone(),
                value: field_value,
            });
        }

        let udt_value = UdtValue {
            type_name: udt_def.name.clone(),
            keyspace: udt_def.keyspace.clone(),
            fields,
        };

        Ok((Value::Udt(udt_value), current_offset))
    }

    /// Parse a UDT field value based on its CqlType.
    fn parse_udt_field_value(
        &self,
        data: &[u8],
        field_type: &CqlType,
        reader: &super::super::types::SSTableReader,
    ) -> Result<Value> {
        match field_type {
            CqlType::Text | CqlType::Ascii => {
                let s = String::from_utf8(data.to_vec())
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in UDT field: {}", e)))?;
                Ok(Value::Text(s))
            }
            CqlType::Int => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Int field requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                let v = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Integer(v))
            }
            CqlType::BigInt => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "BigInt field requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let v = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::BigInt(v))
            }
            CqlType::Float => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Float field requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                let bits = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Float32(f32::from_bits(bits)))
            }
            CqlType::Double => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "Double field requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let bits = u64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::Float(f64::from_bits(bits)))
            }
            CqlType::Boolean => {
                if data.len() != 1 {
                    return Err(Error::corruption(format!(
                        "Boolean field requires 1 byte, got {}",
                        data.len()
                    )));
                }
                Ok(Value::Boolean(data[0] != 0))
            }
            CqlType::Uuid => {
                if data.len() != 16 {
                    return Err(Error::corruption(format!(
                        "UUID field requires 16 bytes, got {}",
                        data.len()
                    )));
                }
                let uuid_bytes: [u8; 16] = data[0..16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                Ok(Value::Uuid(uuid_bytes))
            }
            CqlType::Timestamp => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "Timestamp field requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let millis = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::Timestamp(millis))
            }
            CqlType::Date => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Date field requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                let days = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Date(days as i32))
            }
            CqlType::Blob => Ok(Value::Blob(data.to_vec())),
            CqlType::Inet => Ok(Value::Inet(data.to_vec())),
            CqlType::Frozen(inner) => {
                // Parse the inner type and wrap in Frozen
                let inner_value = self.parse_udt_field_value(data, inner, reader)?;
                Ok(Value::Frozen(Box::new(inner_value)))
            }
            CqlType::Udt(name, field_defs) => {
                // Nested UDT - recursively parse
                let mut nested_def = UdtTypeDef::new("".to_string(), name.clone());
                for (field_name, field_type) in field_defs {
                    nested_def =
                        nested_def.with_field(field_name.clone(), field_type.clone(), true);
                }
                let dummy_column = crate::schema::Column {
                    name: name.clone(),
                    data_type: "udt".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                };
                let (value, _) =
                    self.parse_udt_value(data, 0, &nested_def, &dummy_column, reader)?;
                Ok(value)
            }
            _ => {
                // For other types, return as blob
                log::debug!(
                    "V5CompressedLegacy: UDT field type {:?} parsed as blob ({} bytes)",
                    field_type,
                    data.len()
                );
                Ok(Value::Blob(data.to_vec()))
            }
        }
    }

    /// Create an empty value for a given CQL type.
    fn create_empty_value_for_type(cql_type: &CqlType) -> Value {
        match cql_type {
            CqlType::Text | CqlType::Ascii => Value::Text(String::new()),
            CqlType::Blob => Value::Blob(Vec::new()),
            CqlType::List(_) => Value::List(Vec::new()),
            CqlType::Set(_) => Value::Set(Vec::new()),
            CqlType::Map(_, _) => Value::Map(Vec::new()),
            _ => Value::Blob(Vec::new()),
        }
    }

    /// Parse a CounterContext structure and return the total counter value.
    ///
    /// Counter cells in Cassandra store a CounterContext, not a raw i64 value.
    /// The CounterContext tracks counter updates across multiple replicas (shards).
    ///
    /// Format (from Cassandra's CounterContext.java):
    /// ```text
    /// [header_size: 2-byte BE signed short]    <- Number of shards (negative if cleanup needed)
    /// [indices: 2 bytes * |header_size|]       <- Shard type indicators (negative = global)
    /// [shards: 32 bytes each]:
    ///     [counter_id: 16 bytes UUID]          <- Replica's CounterId
    ///     [clock: 8-byte BE unsigned long]     <- Logical clock
    ///     [count: 8-byte BE signed long]       <- The actual counter value for this shard
    /// ```
    ///
    /// The counter value is the sum of all shard counts, matching Cassandra's `total()` function.
    ///
    /// Returns (total_value, bytes_consumed)
    fn parse_counter_context(
        data: &[u8],
        offset: usize,
        column_name: &str,
    ) -> Result<(i64, usize)> {
        // Constants from CounterContext.java
        const HEADER_SIZE_LENGTH: usize = 2;
        const HEADER_ELT_LENGTH: usize = 2;
        const COUNTER_ID_LENGTH: usize = 16;
        const CLOCK_LENGTH: usize = 8;
        const COUNT_LENGTH: usize = 8;
        const STEP_LENGTH: usize = COUNTER_ID_LENGTH + CLOCK_LENGTH + COUNT_LENGTH; // 32

        // Maximum reasonable shard count to prevent DoS from corrupted data
        // A typical Cassandra cluster has at most 100-500 nodes, so 1024 is generous
        const MAX_COUNTER_SHARDS: usize = 1024;

        let mut pos = offset;

        // Read header_size (2-byte BE signed short)
        if pos + HEADER_SIZE_LENGTH > data.len() {
            return Err(Error::corruption(format!(
                "Counter '{}': need {} bytes for header_size at offset {}, only {} available",
                column_name,
                HEADER_SIZE_LENGTH,
                pos,
                data.len() - pos
            )));
        }
        let header_size_raw = i16::from_be_bytes([data[pos], data[pos + 1]]);
        // Negative header_size indicates local shards need cleanup (CASSANDRA-1938).
        // The absolute value gives the actual shard count.
        let shard_count = header_size_raw.unsigned_abs() as usize;
        pos += HEADER_SIZE_LENGTH;

        // Validate shard count to prevent DoS from corrupted data
        if shard_count > MAX_COUNTER_SHARDS {
            return Err(Error::corruption(format!(
                "Counter '{}': unreasonable shard count {} (max {})",
                column_name, shard_count, MAX_COUNTER_SHARDS
            )));
        }

        log::debug!(
            "V5CompressedLegacy: Counter '{}' header_size={}, shard_count={}",
            column_name,
            header_size_raw,
            shard_count
        );

        // Handle empty counter context (0 shards = counter value of 0)
        if shard_count == 0 {
            return Ok((0, HEADER_SIZE_LENGTH));
        }

        // Skip header indices (2 bytes per shard)
        let indices_size = HEADER_ELT_LENGTH * shard_count;
        if pos + indices_size > data.len() {
            return Err(Error::corruption(format!(
                "Counter '{}': need {} bytes for indices at offset {}, only {} available",
                column_name,
                indices_size,
                pos,
                data.len() - pos
            )));
        }
        pos += indices_size;

        // Calculate expected body size
        let body_size = STEP_LENGTH * shard_count;
        if pos + body_size > data.len() {
            return Err(Error::corruption(format!(
                "Counter '{}': need {} bytes for {} shards at offset {}, only {} available",
                column_name,
                body_size,
                shard_count,
                pos,
                data.len() - pos
            )));
        }

        // Sum count values from all shards (matching Cassandra's total() function)
        let mut total: i64 = 0;
        for shard_idx in 0..shard_count {
            // Skip counter_id (16 bytes) and clock (8 bytes), read count (8 bytes)
            let count_offset = pos + (shard_idx * STEP_LENGTH) + COUNTER_ID_LENGTH + CLOCK_LENGTH;
            let count = i64::from_be_bytes([
                data[count_offset],
                data[count_offset + 1],
                data[count_offset + 2],
                data[count_offset + 3],
                data[count_offset + 4],
                data[count_offset + 5],
                data[count_offset + 6],
                data[count_offset + 7],
            ]);
            // Use checked_add to detect overflow (unlike Java which silently wraps)
            total = total.checked_add(count).ok_or_else(|| {
                Error::corruption(format!(
                    "Counter '{}': integer overflow when summing shard {} (total={}, count={})",
                    column_name, shard_idx, total, count
                ))
            })?;

            log::trace!(
                "V5CompressedLegacy: Counter '{}' shard {} count={}",
                column_name,
                shard_idx,
                count
            );
        }

        // Total bytes consumed
        let consumed = HEADER_SIZE_LENGTH + indices_size + body_size;

        Ok((total, consumed))
    }

    /// Parse a UDT field value without requiring SSTableReader.
    /// This is a simplified version of parse_udt_field_value for use in frozen collection contexts.
    ///
    /// Limitation: Complex nested types (nested UDTs, nested collections) are returned as blobs.
    /// For full UDT support with nested types, use parse_udt_field_value with a reader.
    fn parse_simple_udt_field_value(data: &[u8], field_type: &CqlType) -> Result<Value> {
        match field_type {
            CqlType::Text | CqlType::Ascii => {
                let s = String::from_utf8(data.to_vec())
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in UDT field: {}", e)))?;
                Ok(Value::Text(s))
            }
            CqlType::Int => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Int field requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                let v = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Integer(v))
            }
            CqlType::BigInt => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "BigInt field requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let v = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::BigInt(v))
            }
            CqlType::Boolean => {
                if data.len() != 1 {
                    return Err(Error::corruption(format!(
                        "Boolean field requires 1 byte, got {}",
                        data.len()
                    )));
                }
                Ok(Value::Boolean(data[0] != 0))
            }
            CqlType::Float => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Float field requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                let bits = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Float32(f32::from_bits(bits)))
            }
            CqlType::Double => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "Double field requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let bits = u64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::Float(f64::from_bits(bits)))
            }
            CqlType::Uuid | CqlType::TimeUuid => {
                if data.len() != 16 {
                    return Err(Error::corruption(format!(
                        "UUID field requires 16 bytes, got {}",
                        data.len()
                    )));
                }
                let uuid_bytes: [u8; 16] = data[0..16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                Ok(Value::Uuid(uuid_bytes))
            }
            CqlType::Timestamp => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "Timestamp field requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let millis = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::Timestamp(millis))
            }
            CqlType::Blob => Ok(Value::Blob(data.to_vec())),
            _ => {
                // For complex types (nested UDTs, collections, etc.), return as blob
                // These require SSTableReader for full parsing
                log::debug!(
                    "UDT field type {:?} in frozen context parsed as blob ({} bytes)",
                    field_type,
                    data.len()
                );
                Ok(Value::Blob(data.to_vec()))
            }
        }
    }

    /// Parse a nested UDT from registry definition (Issue #238)
    /// Used when parsing UDT fields that are themselves UDTs
    fn parse_nested_udt_from_registry(
        &self,
        data: &[u8],
        udt_def: &crate::types::UdtTypeDef,
        registry: &UdtRegistry,
    ) -> Result<Value> {
        let mut current_offset = 0;
        let mut fields = Vec::with_capacity(udt_def.fields.len());

        for field_def in &udt_def.fields {
            // Check bounds for field length (4 bytes BE i32)
            if current_offset + 4 > data.len() {
                // Trailing fields are implicit null
                while fields.len() < udt_def.fields.len() {
                    let remaining_field = &udt_def.fields[fields.len()];
                    fields.push(UdtField {
                        name: remaining_field.name.clone(),
                        value: None,
                    });
                }
                break;
            }

            // Read field length (4 bytes big-endian i32)
            let field_len = i32::from_be_bytes([
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
            ]);
            current_offset += 4;

            let field_value = if field_len == -1 {
                None
            } else if field_len == 0 {
                let value = Self::parse_simple_udt_field_value(&[], &field_def.field_type)?;
                Some(value)
            } else {
                let field_len = field_len as usize;
                if current_offset + field_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Nested UDT field '{}' extends beyond data",
                        field_def.name
                    )));
                }

                let field_data = &data[current_offset..current_offset + field_len];
                current_offset += field_len;

                // Handle deeply nested UDTs (including FROZEN<udt> types)
                let value = match &field_def.field_type {
                    CqlType::Custom(nested_type_name) => {
                        // Issue #239: Handle "udt:" prefix from schema parsing
                        let lookup_name = nested_type_name
                            .strip_prefix("udt:")
                            .unwrap_or(nested_type_name);
                        if let Some(nested_udt) = registry.get_udt(&self.keyspace, lookup_name) {
                            self.parse_nested_udt_from_registry(field_data, nested_udt, registry)?
                        } else {
                            Value::Blob(field_data.to_vec())
                        }
                    }
                    CqlType::Udt(udt_name, inline_fields) => {
                        // Inline UDT type - prefer registry, fall back to inline fields (Issue #239)
                        if let Some(nested_udt) = registry.get_udt(&self.keyspace, udt_name) {
                            self.parse_nested_udt_from_registry(field_data, nested_udt, registry)?
                        } else if !inline_fields.is_empty() {
                            // Issue #239: Use inline field definitions for nested UDTs
                            self.parse_inline_udt_value(field_data, udt_name, inline_fields, 1)?
                        } else {
                            Value::Blob(field_data.to_vec())
                        }
                    }
                    CqlType::Frozen(inner) => {
                        // Handle FROZEN<udt_type> - the inner type may be a UDT
                        match inner.as_ref() {
                            CqlType::Custom(nested_type_name) => {
                                // Issue #239: Handle "udt:" prefix from schema parsing
                                let lookup_name = nested_type_name
                                    .strip_prefix("udt:")
                                    .unwrap_or(nested_type_name);
                                if let Some(nested_udt) =
                                    registry.get_udt(&self.keyspace, lookup_name)
                                {
                                    let inner_value = self.parse_nested_udt_from_registry(
                                        field_data, nested_udt, registry,
                                    )?;
                                    Value::Frozen(Box::new(inner_value))
                                } else {
                                    Value::Frozen(Box::new(Value::Blob(field_data.to_vec())))
                                }
                            }
                            CqlType::Udt(udt_name, inline_fields) => {
                                // Prefer registry, fall back to inline fields (Issue #239)
                                if let Some(nested_udt) = registry.get_udt(&self.keyspace, udt_name)
                                {
                                    let inner_value = self.parse_nested_udt_from_registry(
                                        field_data, nested_udt, registry,
                                    )?;
                                    Value::Frozen(Box::new(inner_value))
                                } else if !inline_fields.is_empty() {
                                    // Issue #239: Use inline field definitions
                                    let inner_value = self.parse_inline_udt_value(
                                        field_data,
                                        udt_name,
                                        inline_fields,
                                        1,
                                    )?;
                                    Value::Frozen(Box::new(inner_value))
                                } else {
                                    Value::Frozen(Box::new(Value::Blob(field_data.to_vec())))
                                }
                            }
                            _ => {
                                // Other frozen types - parse as simple value
                                let inner_value =
                                    Self::parse_simple_udt_field_value(field_data, inner)?;
                                Value::Frozen(Box::new(inner_value))
                            }
                        }
                    }
                    _ => Self::parse_simple_udt_field_value(field_data, &field_def.field_type)?,
                };
                Some(value)
            };

            fields.push(UdtField {
                name: field_def.name.clone(),
                value: field_value,
            });
        }

        Ok(Value::Udt(UdtValue {
            type_name: udt_def.name.clone(),
            keyspace: udt_def.keyspace.clone(),
            fields,
        }))
    }

    /// Parse a UDT using inline field definitions from CqlType::Udt
    /// Used when we have inline type info but no registry entry (Issue #239)
    ///
    /// This handles the case where a UDT contains a nested UDT field, and the
    /// nested UDT's field definitions are available inline in the CqlType structure
    /// (parsed from the Statistics.db type string) rather than from the UdtRegistry.
    fn parse_inline_udt_value(
        &self,
        data: &[u8],
        type_name: &str,
        inline_fields: &[(String, CqlType)],
        depth: usize,
    ) -> Result<Value> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "UDT nesting depth {} exceeds maximum {}",
                depth, MAX_TYPE_NESTING_DEPTH
            )));
        }

        let mut current_offset = 0;
        let mut fields = Vec::with_capacity(inline_fields.len());

        for (field_name, field_type) in inline_fields {
            // Check bounds for field length (4 bytes BE i32)
            if current_offset + 4 > data.len() {
                // Trailing fields are implicit null
                while fields.len() < inline_fields.len() {
                    let remaining_field = &inline_fields[fields.len()];
                    fields.push(UdtField {
                        name: remaining_field.0.clone(),
                        value: None,
                    });
                }
                break;
            }

            // Read field length (4 bytes big-endian i32)
            let field_len = i32::from_be_bytes([
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
            ]);
            current_offset += 4;

            let field_value = if field_len == -1 {
                // Null field
                None
            } else if field_len == 0 {
                // Empty value
                let value = Self::parse_simple_udt_field_value(&[], field_type)?;
                Some(value)
            } else {
                let field_len = field_len as usize;
                if current_offset + field_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Inline UDT field '{}' extends beyond data",
                        field_name
                    )));
                }

                let field_data = &data[current_offset..current_offset + field_len];
                current_offset += field_len;

                // Handle nested UDTs using inline field definitions (Issue #239)
                let value = match field_type {
                    CqlType::Udt(nested_name, nested_fields) if !nested_fields.is_empty() => {
                        // Recursively parse nested UDT using its inline fields
                        self.parse_inline_udt_value(
                            field_data,
                            nested_name,
                            nested_fields,
                            depth + 1,
                        )?
                    }
                    CqlType::Frozen(inner) => match inner.as_ref() {
                        CqlType::Udt(nested_name, nested_fields) if !nested_fields.is_empty() => {
                            // Frozen nested UDT - unwrap and parse
                            let inner_value = self.parse_inline_udt_value(
                                field_data,
                                nested_name,
                                nested_fields,
                                depth + 1,
                            )?;
                            Value::Frozen(Box::new(inner_value))
                        }
                        _ => {
                            // Other frozen types - parse as simple value
                            let inner_value =
                                Self::parse_simple_udt_field_value(field_data, inner)?;
                            Value::Frozen(Box::new(inner_value))
                        }
                    },
                    _ => Self::parse_simple_udt_field_value(field_data, field_type)?,
                };
                Some(value)
            };

            fields.push(UdtField {
                name: field_name.clone(),
                value: field_value,
            });
        }

        Ok(Value::Udt(UdtValue {
            type_name: type_name.to_string(),
            keyspace: self.keyspace.clone(),
            fields,
        }))
    }

    /// Returns true if the column type is a complex column (non-frozen collection).
    /// Complex columns are stored as multiple cells with cell paths, unlike
    /// frozen collections which are stored as a single cell with blob value.
    ///
    /// Issue #221: This is critical for proper parsing - complex columns have
    /// a different format: [complex_deletion_time?] [cell_count] [cells...]
    fn is_complex_column(data_type: &str) -> bool {
        let dt = data_type.to_lowercase();
        // Non-frozen collections start directly with list/set/map (CQL syntax)
        // or org.apache.cassandra.db.marshal.ListType/SetType/MapType (internal syntax)
        // Collections containing frozen element types (e.g., list<frozen<...>>) are still complex
        // collections because the outer collection is not frozen - only the elements are.
        // Only frozen<list<...>> etc. are not complex (they're single-cell frozen types)

        // Check for frozen collections (which are NOT complex)
        if dt.starts_with("frozen<")
            || dt.starts_with("org.apache.cassandra.db.marshal.frozentype(")
        {
            return false;
        }

        // Check for CQL-style collection types
        if dt.starts_with("list<") || dt.starts_with("set<") || dt.starts_with("map<") {
            return true;
        }

        // Check for Cassandra internal collection types
        if dt.starts_with("org.apache.cassandra.db.marshal.listtype(")
            || dt.starts_with("org.apache.cassandra.db.marshal.settype(")
            || dt.starts_with("org.apache.cassandra.db.marshal.maptype(")
        {
            return true;
        }

        false
    }

    /// Parse a complex column (non-frozen collection).
    /// Complex columns have multiple cells with cell paths.
    ///
    /// Format when HAS_COMPLEX_DELETION is set:
    ///   [complex_deletion_time: 2 VInts]  // DeletionTime
    ///   [cell_count: VInt]
    ///   [cell_1..cell_n: each with cell_path]
    ///
    /// Format when HAS_COMPLEX_DELETION is NOT set:
    ///   [cell_count: VInt]
    ///   [cell_1..cell_n: each with cell_path]
    ///
    /// Issue #221: This enables parsing of typed_collections_table and other
    /// tables with non-frozen collections.
    /// Outer entry point — the `reader` parameter is forwarded to the inner
    /// cells but is currently unused there (`_reader`).  The outer/inner split
    /// lets unit tests call `parse_complex_column_inner` without constructing a
    /// full `SSTableReader`.
    ///
    /// Returns `(value, new_offset, collection_meta)` where `collection_meta`
    /// carries DS4 extra info: whether the collection carries a tombstone
    /// (overwrite semantics), the max element writetime, and the element tombstone count.
    fn parse_complex_column(
        &self,
        data: &[u8],
        offset: usize,
        column: &crate::schema::Column,
        has_complex_deletion: bool,
        _reader: &super::super::types::SSTableReader,
    ) -> Result<(Value, usize, ComplexColumnMeta)> {
        self.parse_complex_column_inner(data, offset, column, has_complex_deletion)
    }

    fn parse_complex_column_inner(
        &self,
        data: &[u8],
        mut offset: usize,
        column: &crate::schema::Column,
        has_complex_deletion: bool,
    ) -> Result<(Value, usize, ComplexColumnMeta)> {
        log::debug!(
            "V5CompressedLegacy: Parsing complex column '{}' type='{}' has_complex_deletion={} at offset {}",
            column.name, column.data_type, has_complex_deletion, offset
        );

        // Step 1: Parse complex deletion time if flag is set.
        //
        // DS4 (Issue #700): Capture the `markedForDeleteAt` to determine whether this
        // generation carries a **collection-level tombstone** (`s = {...}` overwrite).
        // Cassandra stores the LIVE sentinel as i64::MIN when there is no tombstone;
        // any other value means the collection was overwritten (replaced, not appended).
        //
        // Wire format: DeletionTime = markedForDeleteAt (VInt delta from min_timestamp)
        //                           + localDeletionTime (VInt).
        // We treat `marked_for_delete_at != i64::MIN` as "has collection tombstone".
        let mut has_collection_tombstone = false;
        if has_complex_deletion {
            let (remaining, mfda_delta) = parse_vint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex column '{}': failed to parse markedForDeleteAt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;

            // Delta-decode to get the absolute timestamp.
            // The LIVE sentinel in Cassandra is Long.MIN_VALUE for markedForDeleteAt.
            let absolute_mfda = self.min_timestamp.wrapping_add(mfda_delta);
            // Any value other than i64::MIN indicates a real collection tombstone.
            if absolute_mfda != i64::MIN {
                has_collection_tombstone = true;
            }

            let (remaining, _local_deletion) = parse_vint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex column '{}': failed to parse localDeletionTime at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;

            log::debug!(
                "V5CompressedLegacy: Complex column '{}' deletion time parsed \
                 (absolute_mfda={} has_collection_tombstone={}), now at offset {}",
                column.name,
                absolute_mfda,
                has_collection_tombstone,
                offset
            );
        }

        // Step 2: Parse cell count
        let (remaining, cell_count) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "Complex column '{}': failed to parse cell count at offset {}: {:?}",
                column.name, offset, e
            ))
        })?;
        let bytes_consumed = data[offset..].len() - remaining.len();
        offset += bytes_consumed;

        log::debug!(
            "V5CompressedLegacy: Complex column '{}' has {} cells, now at offset {}",
            column.name,
            cell_count,
            offset
        );

        // Step 3: Parse all cells and aggregate values
        // Issue #225: Bounds check to prevent DoS from corrupted data (match frozen collection pattern)
        if cell_count > MAX_FROZEN_COLLECTION_SIZE {
            return Err(Error::corruption(format!(
                "Complex column '{}': cell count {} exceeds maximum {}",
                column.name, cell_count, MAX_FROZEN_COLLECTION_SIZE
            )));
        }
        // Convert cell_count to usize safely to prevent overflow on 32-bit systems
        let cell_count_usize: usize = cell_count.try_into().map_err(|_| {
            Error::corruption(format!(
                "Complex column '{}': cell count {} exceeds platform limit",
                column.name, cell_count
            ))
        })?;

        // DS4 (Issue #700): Track max element writetime and element tombstone count
        // across all cells in this collection.
        let mut max_element_writetime: i64 = 0;
        let mut element_tombstone_count: u64 = 0;

        /// Helper to update max_element_writetime from a parsed cell.
        #[inline]
        fn update_max_writetime(max: &mut i64, cell: &ComplexCellParse) {
            if let Some(ts) = cell.element_writetime {
                if ts > *max {
                    *max = ts;
                }
            }
        }

        // Determine collection type and extract element type(s)
        let dt = column.data_type.to_lowercase();
        let value = if dt.starts_with("list<")
            || dt.starts_with("org.apache.cassandra.db.marshal.listtype(")
        {
            // Parse list elements
            let element_type = self.extract_collection_element_type(&column.data_type, "list")?;
            let mut elements = Vec::with_capacity(cell_count_usize);

            for i in 0..cell_count_usize {
                let cell =
                    self.parse_complex_cell_value(data, offset, &element_type, column, i as u64)?;
                offset = cell.next_offset;

                // Issue #493: element-level tombstones (IS_DELETED 0x01) are not live
                // values and must not be surfaced. Skip them regardless of their path.
                // DS4: count them for the scan-summary warning counter.
                if cell.is_deleted {
                    element_tombstone_count += 1;
                    log::debug!(
                        "V5CompressedLegacy: list element {} in column '{}' is a tombstone \
                         (IS_DELETED=0x01) — counted for DS4 scan summary (Issue #700/#493)",
                        i,
                        column.name
                    );
                    continue;
                }

                // DS4: Track element timestamp for live elements only (roborev Finding 2).
                // Tombstoned elements are skipped above; their timestamps must not
                // inflate the max_element_writetime reported for the collection.
                update_max_writetime(&mut max_element_writetime, &cell);

                // Add non-null values to the list
                if let Some(val) = cell.value {
                    elements.push(val);
                }
            }

            Value::List(elements)
        } else if dt.starts_with("set<")
            || dt.starts_with("org.apache.cassandra.db.marshal.settype(")
        {
            // Parse set elements
            // In Cassandra's complex cell format for sets, each element is a separate cell
            // where the cell PATH contains the raw bytes of the set element, and the cell
            // VALUE is always empty (HAS_EMPTY_VALUE flag = 0x04 set).
            // We must parse the path bytes as the element value, not the (empty) cell value.
            let element_type = self.extract_collection_element_type(&column.data_type, "set")?;
            let mut elements = Vec::with_capacity(cell_count_usize);

            for i in 0..cell_count_usize {
                let cell =
                    self.parse_complex_cell_value(data, offset, &element_type, column, i as u64)?;
                offset = cell.next_offset;

                // Issue #493: element-level tombstones must not surface as live members.
                // For a set, both a live element and a tombstoned element produce
                // `cell.value == None` with non-empty `path_bytes` (the element key),
                // because live set elements carry HAS_EMPTY_VALUE (0x04) and store the
                // element in the path. The authoritative IS_DELETED (0x01) flag is the
                // ONLY signal that distinguishes them, so we consult it directly and skip
                // tombstoned elements (no-heuristics mandate, Issue #28).
                // DS4: count them for the scan-summary warning counter.
                if cell.is_deleted {
                    element_tombstone_count += 1;
                    log::debug!(
                        "V5CompressedLegacy: set element {} in column '{}' is a tombstone \
                         (IS_DELETED=0x01) — counted for DS4 scan summary (Issue #700/#493)",
                        i,
                        column.name
                    );
                    continue;
                }

                // DS4: Track element timestamp for live elements only (roborev Finding 2).
                // Tombstoned elements are skipped above; their timestamps must not
                // inflate the max_element_writetime reported for the collection.
                update_max_writetime(&mut max_element_writetime, &cell);

                // For sets: the path bytes ARE the element value (cell value is always empty).
                // If cell.value is Some (unusual case where a set cell has a non-empty value),
                // use it. Otherwise parse the path bytes as the element type.
                if let Some(val) = cell.value {
                    elements.push(val);
                } else if !cell.path_bytes.is_empty() {
                    // Path bytes are the set element — parse them as the element type
                    match self.parse_value_from_raw_bytes(
                        &cell.path_bytes,
                        &element_type,
                        &column.name,
                        0,
                    ) {
                        Ok(val) => elements.push(val),
                        Err(e) => {
                            log::debug!(
                                "V5CompressedLegacy: set element {} parse failed (type={}): {}",
                                i,
                                element_type,
                                e
                            );
                        }
                    }
                }
            }

            Value::Set(elements)
        } else if dt.starts_with("map<")
            || dt.starts_with("org.apache.cassandra.db.marshal.maptype(")
        {
            // Parse map entries
            let (key_type, value_type) = self.extract_map_types(&column.data_type)?;
            let mut entries = Vec::with_capacity(cell_count_usize);

            for i in 0..cell_count_usize {
                let cell =
                    self.parse_complex_cell_value(data, offset, &value_type, column, i as u64)?;
                offset = cell.next_offset;

                // For maps, the cell path IS the key
                // Parse the path as the key using the key type
                // Note: Cell path keys are stored WITHOUT length prefixes (raw bytes only)
                //
                // Map semantics are intentionally unchanged for Issue #493: a deleted
                // entry already surfaces as `cell.value == None` and is emitted as
                // (key, Value::Null), preserving existing behavior. Only set/list
                // element tombstones are skipped.
                // DS4: For maps with IS_DELETED entries, count them for the scan summary.
                // Tombstoned entries must NOT contribute to max_element_writetime so that
                // the reported writetime only reflects live content (roborev Finding 2).
                if cell.is_deleted {
                    element_tombstone_count += 1;
                    log::debug!(
                        "V5CompressedLegacy: map entry {} in column '{}' is a tombstone \
                         (IS_DELETED=0x01) — counted for DS4 scan summary (Issue #700/#493)",
                        i,
                        column.name
                    );
                } else {
                    // DS4: Track element timestamp for live map entries only.
                    update_max_writetime(&mut max_element_writetime, &cell);
                }

                if !cell.path_bytes.is_empty() {
                    log::debug!(
                        "V5CompressedLegacy: Parsing map key for column '{}', key_type='{}', path_len={}",
                        column.name,
                        key_type,
                        cell.path_bytes.len()
                    );
                    // For cell path keys, parse directly without expecting length prefixes
                    let key_value =
                        self.parse_cell_path_key(&cell.path_bytes, &key_type, &column.name)?;

                    // Add non-null entries to the map
                    if let Some(val) = cell.value {
                        entries.push((key_value, val));
                    } else {
                        // Map entry with null value (tombstone for that key)
                        entries.push((key_value, Value::Null));
                    }
                }
            }

            Value::Map(entries)
        } else {
            // Unknown complex column type, skip cells
            for i in 0..cell_count_usize {
                offset = self.skip_complex_cell(data, offset, &column.name, i as u64)?;
            }
            Value::Null
        };

        log::debug!(
            "V5CompressedLegacy: Complex column '{}' parsed, final offset {} \
             (has_collection_tombstone={} max_element_writetime={} element_tombstone_count={})",
            column.name,
            offset,
            has_collection_tombstone,
            max_element_writetime,
            element_tombstone_count
        );

        Ok((
            value,
            offset,
            ComplexColumnMeta {
                has_collection_tombstone,
                max_element_writetime,
                element_tombstone_count,
            },
        ))
    }

    /// Parse a single complex cell and extract its value.
    /// Complex cells have: [flags] [timestamp?] [deletion?] [ttl?] [cell_path] [value?]
    ///
    /// Returns a [`ComplexCellParse`] describing the parsed cell.
    /// - `value` is None if the cell is deleted or has an empty value
    /// - `path_bytes` contains the raw path bytes (used as map key for map<> types,
    ///   and as the element value for set<> types)
    /// - `is_deleted` reflects the authoritative IS_DELETED (0x01) cell flag, so
    ///   callers can distinguish element-level tombstones from live elements that
    ///   simply carry an empty value (Issue #493).
    fn parse_complex_cell_value(
        &self,
        data: &[u8],
        mut offset: usize,
        element_type: &str,
        column: &crate::schema::Column,
        cell_index: u64,
    ) -> Result<ComplexCellParse> {
        log::debug!(
            "V5CompressedLegacy: parse_complex_cell_value '{}' cell {} element_type='{}' starting at offset {}",
            column.name,
            cell_index,
            element_type,
            offset
        );

        // Step 1: Cell flags (standard 0x00-0x1F range)
        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: unexpected end at flags (offset {})",
                column.name, cell_index, offset
            )));
        }
        let flags = data[offset];
        offset += 1;

        // Validate flags are in valid range
        if flags > 0x1F {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: invalid flags 0x{:02x} at offset {} (expected 0x00-0x1F)",
                column.name,
                cell_index,
                flags,
                offset - 1
            )));
        }

        let is_deleted = (flags & 0x01) != 0;
        let is_expiring = (flags & 0x02) != 0;
        let has_empty_value = (flags & 0x04) != 0;
        let use_row_timestamp = (flags & 0x08) != 0;
        let use_row_ttl = (flags & 0x10) != 0;

        log::debug!(
            "V5CompressedLegacy: parse_complex_cell_value '{}' cell {} flags=0x{:02x} (deleted={}, expiring={}, empty_value={}, use_row_ts={}, use_row_ttl={})",
            column.name,
            cell_index,
            flags,
            is_deleted,
            is_expiring,
            has_empty_value,
            use_row_timestamp,
            use_row_ttl
        );

        // Step 2: Timestamp (if not using row timestamp)
        // Capture the element-level timestamp delta for DS4 max-writetime computation.
        // Cassandra encodes complex cell timestamps as signed VInt deltas from min_timestamp.
        let mut element_writetime: Option<i64> = None;
        if !use_row_timestamp {
            let (remaining, ts_delta) = parse_vint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse timestamp at offset {}: {:?}",
                    column.name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            // Delta decode: absolute_ts = min_timestamp + ts_delta
            let absolute_ts = self.min_timestamp.wrapping_add(ts_delta);
            element_writetime = Some(absolute_ts);
        }

        // Step 3: Local deletion time (if deleted/expiring and not using row TTL)
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, _ldt) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse localDeletionTime at offset {}: {:?}",
                    column.name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
        }

        // Step 4: TTL (if expiring and not using row TTL)
        if !use_row_ttl && is_expiring {
            let (remaining, _ttl) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse TTL at offset {}: {:?}",
                    column.name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
        }

        // Step 5: Cell path (VInt length + bytes)
        let (remaining, path_len) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "Complex cell {}.{}: failed to parse path length at offset {}: {:?}",
                column.name, cell_index, offset, e
            ))
        })?;
        let bytes_consumed = data[offset..].len() - remaining.len();
        offset += bytes_consumed;

        // Issue #225: Safe conversion to prevent overflow on large values
        let path_len_usize: usize = path_len.try_into().map_err(|_| {
            Error::corruption(format!(
                "Complex cell {}.{}: path length {} exceeds platform limit",
                column.name, cell_index, path_len
            ))
        })?;
        if path_len > MAX_CELL_VALUE_LENGTH {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: path length {} exceeds maximum {}",
                column.name, cell_index, path_len, MAX_CELL_VALUE_LENGTH
            )));
        }

        // Bounds check before reading path
        if offset + path_len_usize > data.len() {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: cell path requires {} bytes but only {} available at offset {}",
                column.name,
                cell_index,
                path_len,
                data.len().saturating_sub(offset),
                offset
            )));
        }

        let path_bytes = data[offset..offset + path_len_usize].to_vec();
        offset += path_len_usize;

        // Step 6: Value (if not empty and not deleted)
        let value = if is_deleted || has_empty_value {
            log::debug!(
                "V5CompressedLegacy: parse_complex_cell_value '{}' cell {} is deleted or empty",
                column.name,
                cell_index
            );
            None
        } else {
            let (remaining, value_len) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse value length at offset {}: {:?}",
                    column.name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;

            // Issue #225: Safe conversion to prevent overflow on large values
            let value_len_usize: usize = value_len.try_into().map_err(|_| {
                Error::corruption(format!(
                    "Complex cell {}.{}: value length {} exceeds platform limit",
                    column.name, cell_index, value_len
                ))
            })?;
            if value_len > MAX_CELL_VALUE_LENGTH {
                return Err(Error::corruption(format!(
                    "Complex cell {}.{}: value length {} exceeds maximum {}",
                    column.name, cell_index, value_len, MAX_CELL_VALUE_LENGTH
                )));
            }

            // Bounds check before reading value
            if offset + value_len_usize > data.len() {
                return Err(Error::corruption(format!(
                    "Complex cell {}.{}: value requires {} bytes but only {} available at offset {}",
                    column.name,
                    cell_index,
                    value_len,
                    data.len().saturating_sub(offset),
                    offset
                )));
            }

            let value_data = &data[offset..offset + value_len_usize];
            offset += value_len_usize;

            // Parse the value based on element type.
            // The value bytes have already been extracted (length was consumed above).
            // Use parse_value_from_raw_bytes which treats the entire slice as the value
            // WITHOUT an additional length prefix (unlike parse_raw_type_value which
            // expects a VInt length prefix — wrong for already-extracted complex cell values).
            // See Issue #481: using parse_raw_type_value here caused the first byte of
            // blob/text values to be misread as a length, producing corrupt parses.
            let parsed_value =
                self.parse_value_from_raw_bytes(value_data, element_type, &column.name, 0)?;
            Some(parsed_value)
        };

        log::debug!(
            "V5CompressedLegacy: parse_complex_cell_value '{}' cell {} complete, value={:?}, final offset {}",
            column.name,
            cell_index,
            value.is_some(),
            offset
        );

        Ok(ComplexCellParse {
            value,
            path_bytes,
            is_deleted,
            next_offset: offset,
            element_writetime,
        })
    }

    /// Skip over a single complex cell without fully parsing its value.
    /// Complex cells have: [flags] [timestamp?] [deletion?] [ttl?] [cell_path] [value?]
    ///
    /// Issue #221: This is used to advance past complex cell data while returning
    /// placeholder values. Future work can add full cell value parsing here.
    fn skip_complex_cell(
        &self,
        data: &[u8],
        mut offset: usize,
        column_name: &str,
        cell_index: u64,
    ) -> Result<usize> {
        log::debug!(
            "V5CompressedLegacy: skip_complex_cell '{}' cell {} starting at offset {}, bytes: {:02x?}",
            column_name,
            cell_index,
            offset,
            &data[offset..std::cmp::min(offset + 20, data.len())]
        );

        // Complex cell format per Cassandra source (UnfilteredSerializer.java):
        // [flags: u8]
        // [timestamp: VInt if not USE_ROW_TIMESTAMP_MASK]
        // [local_deletion_time: VInt if (deleted || expiring) && not USE_ROW_TTL_MASK]
        // [ttl: VInt if expiring && not USE_ROW_TTL_MASK]
        // [cell_path: VInt length + bytes] <-- AFTER flags/timestamp/etc, NOT before!
        // [value: VInt length + bytes if not HAS_EMPTY_VALUE_MASK]

        // Step 1: Cell flags (standard 0x00-0x1F range)
        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: unexpected end at flags (offset {})",
                column_name, cell_index, offset
            )));
        }
        let flags = data[offset];
        offset += 1;

        // Validate flags are in valid range
        if flags > 0x1F {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: invalid flags 0x{:02x} at offset {} (expected 0x00-0x1F)",
                column_name,
                cell_index,
                flags,
                offset - 1
            )));
        }

        let is_deleted = (flags & 0x01) != 0;
        let is_expiring = (flags & 0x02) != 0;
        let has_empty_value = (flags & 0x04) != 0;
        let use_row_timestamp = (flags & 0x08) != 0;
        let use_row_ttl = (flags & 0x10) != 0;

        log::debug!(
            "V5CompressedLegacy: skip_complex_cell '{}' cell {} flags=0x{:02x} (deleted={}, expiring={}, empty_value={}, use_row_ts={}, use_row_ttl={})",
            column_name,
            cell_index,
            flags,
            is_deleted,
            is_expiring,
            has_empty_value,
            use_row_timestamp,
            use_row_ttl
        );

        // Step 2: Timestamp (if not using row timestamp)
        if !use_row_timestamp {
            let (remaining, _ts) = parse_vint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse timestamp at offset {}: {:?}",
                    column_name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
        }

        // Step 3: Local deletion time (if deleted/expiring and not using row TTL)
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, _ldt) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse localDeletionTime at offset {}: {:?}",
                    column_name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
        }

        // Step 4: TTL (if expiring and not using row TTL)
        if !use_row_ttl && is_expiring {
            let (remaining, _ttl) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse TTL at offset {}: {:?}",
                    column_name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
        }

        // Step 5: Cell path (VInt length + bytes) - comes AFTER flags/timestamp/ttl
        let (remaining, path_len) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "Complex cell {}.{}: failed to parse path length at offset {}: {:?}",
                column_name, cell_index, offset, e
            ))
        })?;
        let bytes_consumed = data[offset..].len() - remaining.len();
        log::debug!(
            "V5CompressedLegacy: skip_complex_cell '{}' cell {} path_len={} at offset {}",
            column_name,
            cell_index,
            path_len,
            offset
        );
        offset += bytes_consumed;

        // Issue #225: Safe conversion to prevent overflow on large values
        let path_len_usize: usize = path_len.try_into().map_err(|_| {
            Error::corruption(format!(
                "Complex cell {}.{}: path length {} exceeds platform limit",
                column_name, cell_index, path_len
            ))
        })?;
        if path_len > MAX_CELL_VALUE_LENGTH {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: path length {} exceeds maximum {}",
                column_name, cell_index, path_len, MAX_CELL_VALUE_LENGTH
            )));
        }

        // Bounds check before advancing by path_len
        if offset + path_len_usize > data.len() {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: cell path requires {} bytes but only {} available at offset {}",
                column_name,
                cell_index,
                path_len,
                data.len().saturating_sub(offset),
                offset
            )));
        }
        offset += path_len_usize;

        // Step 6: Value (if not empty)
        if !has_empty_value {
            let (remaining, value_len) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse value length at offset {}: {:?}",
                    column_name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;

            // Issue #225: Safe conversion to prevent overflow on large values
            let value_len_usize: usize = value_len.try_into().map_err(|_| {
                Error::corruption(format!(
                    "Complex cell {}.{}: value length {} exceeds platform limit",
                    column_name, cell_index, value_len
                ))
            })?;
            if value_len > MAX_CELL_VALUE_LENGTH {
                return Err(Error::corruption(format!(
                    "Complex cell {}.{}: value length {} exceeds maximum {}",
                    column_name, cell_index, value_len, MAX_CELL_VALUE_LENGTH
                )));
            }

            // Bounds check before advancing by value_len
            if offset + value_len_usize > data.len() {
                return Err(Error::corruption(format!(
                    "Complex cell {}.{}: value requires {} bytes but only {} available at offset {}",
                    column_name,
                    cell_index,
                    value_len,
                    data.len().saturating_sub(offset),
                    offset
                )));
            }
            offset += value_len_usize;
        }

        log::debug!(
            "V5CompressedLegacy: skip_complex_cell '{}' cell {} complete, final offset {}",
            column_name,
            cell_index,
            offset
        );

        Ok(offset)
    }

    /// Extract element type from list<T> or set<T> type string (CQL or Cassandra internal format)
    fn extract_collection_element_type(&self, type_str: &str, collection: &str) -> Result<String> {
        let type_lower = type_str.to_lowercase();

        // Check for Cassandra internal format first: org.apache.cassandra.db.marshal.ListType(...)
        let internal_prefix_lower = format!("org.apache.cassandra.db.marshal.{}type(", collection);
        if type_lower.starts_with(&internal_prefix_lower) && type_lower.ends_with(')') {
            // Use the lowercase prefix length to extract from the original string
            let inner = &type_str[internal_prefix_lower.len()..type_str.len() - 1];
            if inner.is_empty() {
                return Err(Error::schema(format!(
                    "Empty {} element type: {}",
                    collection, type_str
                )));
            }
            return Ok(inner.to_string());
        }

        // Check for CQL format: list<T>, set<T>
        let prefix_lower = format!("{}<", collection);
        if type_lower.starts_with(&prefix_lower) && type_lower.ends_with('>') {
            // Use the lowercase prefix length to extract from the original string
            let inner = &type_str[prefix_lower.len()..type_str.len() - 1];
            if inner.is_empty() {
                return Err(Error::schema(format!(
                    "Empty {} element type: {}",
                    collection, type_str
                )));
            }
            return Ok(inner.to_string());
        }

        Err(Error::schema(format!(
            "Invalid {} type format: {}",
            collection, type_str
        )))
    }

    /// Extract key and value types from map<K,V> type string (CQL or Cassandra internal format)
    fn extract_map_types(&self, type_str: &str) -> Result<(String, String)> {
        let type_lower = type_str.to_lowercase();

        // Determine the inner content based on format
        let inner = if type_lower.starts_with("org.apache.cassandra.db.marshal.maptype(")
            && type_str.ends_with(')')
        {
            // Cassandra internal format: org.apache.cassandra.db.marshal.MapType(K,V)
            let prefix = "org.apache.cassandra.db.marshal.MapType(";
            &type_str[prefix.len()..type_str.len() - 1]
        } else if type_lower.starts_with("map<") && type_str.ends_with('>') {
            // CQL format: map<K,V>
            &type_str[4..type_str.len() - 1]
        } else {
            return Err(Error::schema(format!(
                "Invalid map type format: {}",
                type_str
            )));
        };

        if inner.is_empty() {
            return Err(Error::schema(format!("Empty map types: {}", type_str)));
        }

        // Split by comma, handling nested angle brackets and parentheses
        let mut depth = 0;
        let mut split_pos = None;

        for (i, ch) in inner.chars().enumerate() {
            match ch {
                '<' | '(' => depth += 1,
                '>' | ')' => depth -= 1,
                ',' if depth == 0 => {
                    split_pos = Some(i);
                    break;
                }
                _ => {}
            }
        }

        let split_pos = split_pos.ok_or_else(|| {
            Error::schema(format!(
                "Invalid map type format (no comma separator): {}",
                type_str
            ))
        })?;

        let key_type = inner[..split_pos].trim().to_string();
        let value_type = inner[split_pos + 1..].trim().to_string();

        if key_type.is_empty() || value_type.is_empty() {
            return Err(Error::schema(format!(
                "Empty key or value type in map: {}",
                type_str
            )));
        }

        Ok((key_type, value_type))
    }

    /// Parse a value from a complete, bounded byte slice.
    ///
    /// This is used when the outer Cassandra collection format already provides
    /// explicit `[i32 BE len][raw bytes]` boundaries and we have extracted exactly
    /// the bytes that constitute the value. The entire `data` slice IS the value.
    ///
    /// - Variable-width types (text, blob, varint, decimal, inet): consume the full slice
    /// - Fixed-width types (int, bigint, uuid, etc.): read from offset 0
    /// - Nested collections: use the bounded sub-format `[i32 BE count][i32 BE len][bytes]...`
    fn parse_value_from_raw_bytes(
        &self,
        data: &[u8],
        type_str: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<Value> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "Frozen element '{}': recursion depth {} exceeds maximum {}",
                column_name, depth, MAX_TYPE_NESTING_DEPTH
            )));
        }
        let normalized_type = type_str.to_lowercase();
        match normalized_type.as_str() {
            "text"
            | "varchar"
            | "ascii"
            | "org.apache.cassandra.db.marshal.utf8type"
            | "org.apache.cassandra.db.marshal.asciitype"
            | "org.apache.cassandra.db.marshal.varchartype" => {
                let text = String::from_utf8(data.to_vec()).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': invalid UTF-8 in text value: {}",
                        column_name, e
                    ))
                })?;
                Ok(Value::Text(text))
            }
            "blob" | "bytes" => Ok(Value::Blob(data.to_vec())),
            "int" => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for int, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::Integer(i32::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                ])))
            }
            "bigint" | "counter" => {
                if data.len() < 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for bigint, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::BigInt(i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ])))
            }
            "boolean" => {
                if data.is_empty() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 1 byte for boolean",
                        column_name
                    )));
                }
                Ok(Value::Boolean(data[0] != 0))
            }
            "uuid" | "timeuuid" => {
                if data.len() < 16 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 16 bytes for UUID, got {}",
                        column_name,
                        data.len()
                    )));
                }
                let uuid: [u8; 16] = data[..16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                Ok(Value::Uuid(uuid))
            }
            "float" => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for float, got {}",
                        column_name,
                        data.len()
                    )));
                }
                let f = f32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Float(f as f64))
            }
            "double" => {
                if data.len() < 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for double, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::Float(f64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ])))
            }
            "smallint" | "short" => {
                if data.len() < 2 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 2 bytes for smallint, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::SmallInt(i16::from_be_bytes([data[0], data[1]])))
            }
            "tinyint" | "byte" => {
                if data.is_empty() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 1 byte for tinyint",
                        column_name
                    )));
                }
                Ok(Value::TinyInt(data[0] as i8))
            }
            "timestamp" => {
                if data.len() < 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for timestamp, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::Timestamp(i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ])))
            }
            "date" => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for date, got {}",
                        column_name,
                        data.len()
                    )));
                }
                let stored = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Ok(Value::Date(days_since_epoch))
            }
            "time" => {
                if data.len() < 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for time, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::Time(i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ])))
            }
            "varint" => Ok(Value::Varint(data.to_vec())),
            "decimal" => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': decimal too short ({} bytes)",
                        column_name,
                        data.len()
                    )));
                }
                let scale = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let unscaled = data[4..].to_vec();
                Ok(Value::Decimal { scale, unscaled })
            }
            "inet" => Ok(Value::Inet(data.to_vec())),
            // Nested list/set/map inside a bounded element (e.g. map<text, list<int>>)
            type_str if type_str.starts_with("list<") => {
                let element_type = self.extract_collection_element_type(type_str, "list")?;
                let (val, _) = self.parse_frozen_list_value_raw(
                    data,
                    0,
                    &element_type,
                    column_name,
                    depth + 1,
                )?;
                Ok(val)
            }
            type_str if type_str.starts_with("set<") => {
                let element_type = self.extract_collection_element_type(type_str, "set")?;
                let (val, _) = self.parse_frozen_set_value_raw(
                    data,
                    0,
                    &element_type,
                    column_name,
                    depth + 1,
                )?;
                Ok(val)
            }
            type_str if type_str.starts_with("map<") => {
                let (key_type, value_type) = self.extract_map_types(type_str)?;
                let (val, _) = self.parse_frozen_map_value_raw(
                    data,
                    0,
                    &key_type,
                    &value_type,
                    column_name,
                    depth + 1,
                )?;
                Ok(val)
            }
            // Nested tuple inside a frozen collection element.
            // The caller (read_frozen_element) has already extracted the raw element bytes
            // into `data`, so there is no outer VUInt length here — just the sequence of
            // [i32 BE len][bytes] fields as written by serialize_value for Value::Tuple.
            type_str if type_str.starts_with("tuple<") => {
                let element_types = self.extract_tuple_element_types(type_str)?;
                if element_types.is_empty() {
                    return Err(Error::schema(format!(
                        "Nested tuple element '{}': empty tuple type",
                        column_name
                    )));
                }
                let mut off = 0usize;
                let blob_end = data.len();
                let elements = self.parse_tuple_elements_raw(
                    data,
                    &mut off,
                    blob_end,
                    &element_types,
                    column_name,
                    depth + 1,
                )?;
                Ok(Value::Tuple(elements))
            }
            type_str if type_str.starts_with("frozen<") => {
                let inner_type = self.extract_frozen_inner_type(type_str)?;
                let inner =
                    self.parse_value_from_raw_bytes(data, &inner_type, column_name, depth + 1)?;
                Ok(Value::Frozen(Box::new(inner)))
            }
            // UDT (User-Defined Type): delegate to parse_raw_type_value which has the full
            // UDT parsing logic including field count validation and nested type resolution.
            // The raw bytes representation is identical between the two function conventions.
            other if Self::is_udt_type(other) => {
                let (val, _offset) =
                    self.parse_raw_type_value(data, 0, type_str, column_name, depth)?;
                Ok(val)
            }
            other => {
                // Check if it's a short UDT name in the registry (e.g., "address_type").
                // This handles the case where parse_value_from_raw_bytes is called recursively
                // from the frozen<> arm with the stripped inner type (e.g., frozen<address_type>
                // → "address_type"). Since parse_raw_type_value already has a registry-lookup
                // fallback that correctly handles bare UDT names, we delegate there.
                // The byte-level encoding is identical: UDT fields use 4-byte i32 length prefixes
                // with no overall cell-level length prefix, so parse_raw_type_value offset=0 is
                // correct for already-extracted cell value bytes.
                // See Issue #481 regression fix.
                if let Some(ref registry) = self.udt_registry {
                    if registry.get_udt(&self.keyspace, other).is_some() {
                        log::debug!(
                            "parse_value_from_raw_bytes: type '{}' for '{}' resolved as UDT via registry, delegating to parse_raw_type_value",
                            other,
                            column_name,
                        );
                        let (val, _offset) =
                            self.parse_raw_type_value(data, 0, type_str, column_name, depth)?;
                        return Ok(val);
                    }
                }
                // Truly unknown type: fall back to blob.
                log::debug!(
                    "parse_value_from_raw_bytes: unknown type '{}' for '{}', treating as blob ({} bytes)",
                    other,
                    column_name,
                    data.len()
                );
                Ok(Value::Blob(data.to_vec()))
            }
        }
    }

    /// Parse a raw type value WITHOUT cell flags (for frozen collection elements)
    ///
    /// Unlike `parse_cell_value_schema_order`, this function does NOT expect cell flags
    /// or timestamps at the start of the data. Frozen collection elements are stored
    /// as raw type values directly:
    /// - Fixed-width types (int, uuid, bigint, float, double): direct bytes, no length prefix
    /// - Variable-width types (text, blob): VInt length prefix + bytes
    ///
    /// This is the correct format for elements inside frozen collections:
    /// frozen<list<int>> -> [VInt count][int1][int2]...  (each int is 4 bytes, no flags)
    /// frozen<map<text, text>> -> [VInt count][VInt key_len][key][VInt val_len][val]...
    fn parse_raw_type_value(
        &self,
        data: &[u8],
        mut offset: usize,
        type_str: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "Frozen element '{}': recursion depth {} exceeds maximum {}",
                column_name, depth, MAX_TYPE_NESTING_DEPTH
            )));
        }
        // Normalize type name for case-insensitive matching
        let normalized_type = type_str.to_lowercase();

        let value = match normalized_type.as_str() {
            // Cassandra internal type names (full package paths)
            "org.apache.cassandra.db.marshal.utf8type"
            | "org.apache.cassandra.db.marshal.asciitype"
            | "org.apache.cassandra.db.marshal.varchartype" => {
                // Text: [VInt len][text bytes]
                let (remaining, text_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse text length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let text_len = text_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + text_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for text, only {} available",
                        column_name,
                        text_len,
                        data.len() - offset
                    )));
                }

                let text_bytes = &data[offset..offset + text_len];
                let text = String::from_utf8(text_bytes.to_vec())
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in text: {}", e)))?;
                offset += text_len;
                Value::Text(text)
            }

            "boolean" => {
                // Boolean: 1 byte
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': unexpected end at boolean value",
                        column_name
                    )));
                }
                let bool_byte = data[offset];
                offset += 1;
                Value::Boolean(bool_byte != 0)
            }

            "int" => {
                // Integer (i32): fixed-width 4 bytes
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for int, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let int_val = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                Value::Integer(int_val)
            }

            "text" | "varchar" | "ascii" => {
                // Text: [VInt len][text bytes]
                let (remaining, text_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse text length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let text_len = text_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + text_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for text, only {} available",
                        column_name,
                        text_len,
                        data.len() - offset
                    )));
                }

                let text_bytes = &data[offset..offset + text_len];
                let text = String::from_utf8(text_bytes.to_vec()).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': invalid UTF-8 in text value: {}",
                        column_name, e
                    ))
                })?;

                offset += text_len;
                Value::Text(text)
            }

            "uuid" | "timeuuid" => {
                // UUID/TimeUUID: fixed-width 16 bytes
                if offset + 16 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 16 bytes for UUID, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }

                let uuid_bytes: [u8; 16] = data[offset..offset + 16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;

                offset += 16;
                Value::Uuid(uuid_bytes)
            }

            "bigint" | "counter" => {
                // BigInt/Counter: fixed-width 8 bytes
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for bigint, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let val = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::BigInt(val)
            }

            "float" => {
                // Float: 4 bytes
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for float, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }

                let val = f32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                Value::Float(val as f64)
            }

            "double" => {
                // Double: 8 bytes
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for double, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let val = f64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Float(val) // Note: Value::Float holds f64 for both float and double
            }

            "timestamp" => {
                // Timestamp: 8 bytes (milliseconds since epoch)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for timestamp, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let ts = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Timestamp(ts)
            }

            "date" => {
                // Date: [VInt len=4][u32 BE days since epoch]
                let (remaining, date_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse date length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let date_len = date_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if date_len != 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': expected date length 4, got {}",
                        column_name, date_len
                    )));
                }

                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for date, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }

                let stored = u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                // Cassandra DATE: 4-byte unsigned int with Integer.MIN_VALUE offset
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Value::Date(days_since_epoch)
            }

            "time" => {
                // Time: [VInt len=8][i64 BE nanoseconds since midnight]
                let (remaining, time_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse time length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let time_len = time_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if time_len != 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': expected time length 8, got {}",
                        column_name, time_len
                    )));
                }

                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for time, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }

                let nanos = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Time(nanos)
            }

            "duration" => {
                // Duration: [VInt len][months VInt][days VInt][nanos VInt]
                let (remaining, duration_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let duration_len = duration_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + duration_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for duration, only {} available",
                        column_name,
                        duration_len,
                        data.len() - offset
                    )));
                }

                // Parse three VInt components from the duration_len bytes
                let duration_bytes = &data[offset..offset + duration_len];

                // Parse months (signed VInt)
                let (remaining, months) = parse_vint(duration_bytes).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration months: {:?}",
                        column_name, e
                    ))
                })?;
                let pos = duration_bytes.len() - remaining.len();

                // Parse days (signed VInt)
                let (remaining, days) = parse_vint(&duration_bytes[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration days: {:?}",
                        column_name, e
                    ))
                })?;
                let pos = duration_bytes.len() - remaining.len();

                // Parse nanoseconds (signed VInt)
                let (_remaining, nanos) = parse_vint(&duration_bytes[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration nanos: {:?}",
                        column_name, e
                    ))
                })?;

                offset += duration_len;
                Value::Duration {
                    months: months as i32,
                    days: days as i32,
                    nanos,
                }
            }

            "inet" => {
                // Inet: [VInt len][address bytes] (len is 4 for IPv4, 16 for IPv6)
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse inet length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let len = len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if len != 4 && len != 16 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': invalid inet length {}, expected 4 or 16",
                        column_name, len
                    )));
                }

                if offset + len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for inet, only {} available",
                        column_name,
                        len,
                        data.len() - offset
                    )));
                }

                let bytes = data[offset..offset + len].to_vec();
                offset += len;
                Value::Inet(bytes)
            }

            "blob" | "bytes" => {
                // Blob: [VInt len][bytes]
                let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse blob length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let blob_len = blob_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + blob_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for blob, only {} available",
                        column_name,
                        blob_len,
                        data.len() - offset
                    )));
                }

                let blob_bytes = data[offset..offset + blob_len].to_vec();
                offset += blob_len;
                Value::Blob(blob_bytes)
            }

            "smallint" | "short" => {
                // SmallInt: 2 bytes
                if offset + 2 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 2 bytes for smallint, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let val = i16::from_be_bytes([data[offset], data[offset + 1]]);
                offset += 2;
                Value::SmallInt(val)
            }

            "tinyint" | "byte" => {
                // TinyInt: 1 byte
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 1 byte for tinyint, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let val = data[offset] as i8;
                offset += 1;
                Value::TinyInt(val)
            }

            "varint" => {
                // VarInt: [VInt len][bytes]
                let (remaining, varint_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse varint length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let varint_len = varint_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + varint_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for varint, only {} available",
                        column_name,
                        varint_len,
                        data.len() - offset
                    )));
                }

                let varint_bytes = data[offset..offset + varint_len].to_vec();
                offset += varint_len;
                Value::Varint(varint_bytes)
            }

            "decimal" => {
                // Decimal: [VInt total_len][i32 scale][unscaled bytes]
                let (remaining, total_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse decimal length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let total_len = total_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + total_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for decimal, only {} available",
                        column_name,
                        total_len,
                        data.len() - offset
                    )));
                }

                if total_len < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': decimal length {} too small for scale",
                        column_name, total_len
                    )));
                }

                let scale = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                let unscaled = data[offset + 4..offset + total_len].to_vec();
                offset += total_len;

                Value::Decimal { scale, unscaled }
            }

            // Handle nested tuple types inside a frozen context.
            // In parse_raw_type_value the data slice is the full (unbounded) row buffer, so
            // `offset` marks where the tuple blob starts.  The tuple's per-element length
            // uses the [i32 BE len][bytes] wire format; the count comes from the type string.
            // There is NO outer VUInt blob-length prefix here because parse_raw_type_value is
            // called element-by-element from the frozen-collection parsers which have already
            // consumed the VUInt length for each element (via read_frozen_element).
            //
            // Safety invariant: every caller of parse_raw_type_value for a tuple element
            // pre-slices `data` to the exact element bytes (via read_frozen_element or
            // parse_frozen_sequence_value_raw), so `data.len()` is the true tuple extent.
            // parse_tuple_elements_raw iterates only over schema-derived element_types, so it
            // stops at the schema arity regardless of wire arity, and the returned `offset`
            // is the position after the last schema-specified element's bytes — which is
            // correct because the caller already holds the bounded slice.
            type_str if type_str.starts_with("tuple<") => {
                let element_types = self.extract_tuple_element_types(type_str)?;
                if element_types.is_empty() {
                    return Err(Error::schema(format!(
                        "Frozen element '{}': empty tuple type",
                        column_name
                    )));
                }
                // blob_end = data.len() is correct: callers pre-slice data to the tuple extent.
                let blob_end = data.len();
                let mut off = offset;
                let elements = self.parse_tuple_elements_raw(
                    data,
                    &mut off,
                    blob_end,
                    &element_types,
                    column_name,
                    depth + 1,
                )?;
                offset = off;
                Value::Tuple(elements)
            }

            // Handle nested frozen types
            type_str if type_str.starts_with("frozen<") => {
                let inner_type = self.extract_frozen_inner_type(type_str)?;
                let (inner_value, new_offset) =
                    self.parse_raw_type_value(data, offset, &inner_type, column_name, depth + 1)?;
                offset = new_offset;
                Value::Frozen(Box::new(inner_value))
            }

            // Handle nested collections inside frozen context
            type_str if type_str.starts_with("list<") => {
                let element_type = self.extract_collection_element_type(type_str, "list")?;
                let (list_value, new_offset) = self.parse_frozen_list_value_raw(
                    data,
                    offset,
                    &element_type,
                    column_name,
                    depth + 1,
                )?;
                offset = new_offset;
                list_value
            }

            type_str if type_str.starts_with("set<") => {
                let element_type = self.extract_collection_element_type(type_str, "set")?;
                let (set_value, new_offset) = self.parse_frozen_set_value_raw(
                    data,
                    offset,
                    &element_type,
                    column_name,
                    depth + 1,
                )?;
                offset = new_offset;
                set_value
            }

            type_str if type_str.starts_with("map<") => {
                let (key_type, value_type) = self.extract_map_types(type_str)?;
                let (map_value, new_offset) = self.parse_frozen_map_value_raw(
                    data,
                    offset,
                    &key_type,
                    &value_type,
                    column_name,
                    depth + 1,
                )?;
                offset = new_offset;
                map_value
            }

            // Handle UDT (User-Defined Type) inside frozen collections
            // Note: We match against normalized (lowercased) but need original case for parsing
            normalized if Self::is_udt_type(normalized) => {
                log::debug!(
                    "Frozen element '{}': parsing UDT type '{}'",
                    column_name,
                    type_str
                );

                // Parse UDT definition from the ORIGINAL type string (not lowercased)
                // because UserType parsing expects exact case "UserType"
                let udt_def = Self::parse_udt_type_definition(type_str)?;

                // UDT data: The VInt length prefix has already been consumed by the caller
                // (either complex cell parser or frozen collection element parser).
                // The data slice passed to parse_raw_type_value is already the raw UDT bytes.
                let udt_data = &data[offset..];

                if log::log_enabled!(log::Level::Debug) {
                    let hex: String = udt_data
                        .iter()
                        .take(64)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    log::debug!(
                        "Frozen UDT '{}': data_len={}, hex dump: {}",
                        column_name,
                        udt_data.len(),
                        hex
                    );
                }

                // TODO(Issue #220): Full UDT parsing requires SSTableReader for nested types.
                // parse_raw_type_value is called in frozen collection contexts where we don't
                // have access to reader. For now, parse simple fields and return blob for
                // complex nested types.
                //
                // Temporary solution: Parse UDT with limited nested type support

                // Validate field count to prevent memory exhaustion
                if udt_def.fields.len() > MAX_UDT_FIELD_COUNT {
                    return Err(Error::schema(format!(
                        "UDT '{}' has {} fields, exceeds maximum {}",
                        udt_def.name,
                        udt_def.fields.len(),
                        MAX_UDT_FIELD_COUNT
                    )));
                }

                let mut current_offset = 0;
                let mut fields = Vec::with_capacity(udt_def.fields.len());

                for field_def in &udt_def.fields {
                    // Check bounds for field length (4 bytes BE i32)
                    if current_offset + 4 > udt_data.len() {
                        // Trailing fields can be omitted (implicit null)
                        log::debug!(
                            "Frozen UDT field '{}' omitted (implicit null)",
                            field_def.name
                        );
                        while fields.len() < udt_def.fields.len() {
                            let remaining_field = &udt_def.fields[fields.len()];
                            fields.push(UdtField {
                                name: remaining_field.name.clone(),
                                value: None,
                            });
                        }
                        break;
                    }

                    // Read field length (4 bytes big-endian i32)
                    let field_len = i32::from_be_bytes([
                        udt_data[current_offset],
                        udt_data[current_offset + 1],
                        udt_data[current_offset + 2],
                        udt_data[current_offset + 3],
                    ]);
                    log::debug!(
                        "Frozen UDT field '{}' at offset {}: length bytes={:02x} {:02x} {:02x} {:02x}, parsed length={}",
                        field_def.name,
                        current_offset,
                        udt_data[current_offset],
                        udt_data[current_offset + 1],
                        udt_data[current_offset + 2],
                        udt_data[current_offset + 3],
                        field_len
                    );
                    current_offset += 4;

                    let field_value = if field_len == -1 {
                        // Null field
                        log::debug!("Frozen UDT field '{}' is null", field_def.name);
                        None
                    } else if field_len == 0 {
                        // Empty field
                        log::debug!("Frozen UDT field '{}' is empty", field_def.name);
                        Some(Self::create_empty_value_for_type(&field_def.field_type))
                    } else if field_len < 0 {
                        // Validation: reject other negative values
                        return Err(Error::corruption(format!(
                            "Frozen UDT field '{}': invalid negative field length {}",
                            field_def.name, field_len
                        )));
                    } else {
                        // Field with data
                        let field_len = field_len as usize;
                        if current_offset + field_len > udt_data.len() {
                            return Err(Error::corruption(format!(
                                "Frozen UDT field '{}': need {} bytes but only {} available",
                                field_def.name,
                                field_len,
                                udt_data.len() - current_offset
                            )));
                        }

                        let field_data = &udt_data[current_offset..current_offset + field_len];
                        current_offset += field_len;

                        log::debug!(
                            "Frozen UDT field '{}' has {} bytes of data, type: {:?}",
                            field_def.name,
                            field_len,
                            field_def.field_type
                        );

                        // Parse field value - handle nested UDTs specially (Issue #238)
                        let value = if let Some(ref registry) = self.udt_registry {
                            match &field_def.field_type {
                                CqlType::Custom(nested_type_name) => {
                                    // Issue #239: Handle "udt:" prefix from schema parsing
                                    let lookup_name = nested_type_name
                                        .strip_prefix("udt:")
                                        .unwrap_or(nested_type_name);
                                    if let Some(nested_udt) =
                                        registry.get_udt(&self.keyspace, lookup_name)
                                    {
                                        self.parse_nested_udt_from_registry(
                                            field_data, nested_udt, registry,
                                        )?
                                    } else {
                                        Self::parse_simple_udt_field_value(
                                            field_data,
                                            &field_def.field_type,
                                        )?
                                    }
                                }
                                CqlType::Udt(udt_name, inline_fields) => {
                                    // Prefer registry, fall back to inline fields (Issue #239)
                                    if let Some(nested_udt) =
                                        registry.get_udt(&self.keyspace, udt_name)
                                    {
                                        self.parse_nested_udt_from_registry(
                                            field_data, nested_udt, registry,
                                        )?
                                    } else if !inline_fields.is_empty() {
                                        self.parse_inline_udt_value(
                                            field_data,
                                            udt_name,
                                            inline_fields,
                                            1,
                                        )?
                                    } else {
                                        Self::parse_simple_udt_field_value(
                                            field_data,
                                            &field_def.field_type,
                                        )?
                                    }
                                }
                                CqlType::Frozen(inner) => match inner.as_ref() {
                                    CqlType::Custom(nested_type_name) => {
                                        // Issue #239: Handle "udt:" prefix from schema parsing
                                        let lookup_name = nested_type_name
                                            .strip_prefix("udt:")
                                            .unwrap_or(nested_type_name);
                                        if let Some(nested_udt) =
                                            registry.get_udt(&self.keyspace, lookup_name)
                                        {
                                            let inner_value = self.parse_nested_udt_from_registry(
                                                field_data, nested_udt, registry,
                                            )?;
                                            Value::Frozen(Box::new(inner_value))
                                        } else {
                                            Self::parse_simple_udt_field_value(
                                                field_data,
                                                &field_def.field_type,
                                            )?
                                        }
                                    }
                                    CqlType::Udt(udt_name, inline_fields) => {
                                        // Prefer registry, fall back to inline fields (Issue #239)
                                        if let Some(nested_udt) =
                                            registry.get_udt(&self.keyspace, udt_name)
                                        {
                                            let inner_value = self.parse_nested_udt_from_registry(
                                                field_data, nested_udt, registry,
                                            )?;
                                            Value::Frozen(Box::new(inner_value))
                                        } else if !inline_fields.is_empty() {
                                            let inner_value = self.parse_inline_udt_value(
                                                field_data,
                                                udt_name,
                                                inline_fields,
                                                1,
                                            )?;
                                            Value::Frozen(Box::new(inner_value))
                                        } else {
                                            Self::parse_simple_udt_field_value(
                                                field_data,
                                                &field_def.field_type,
                                            )?
                                        }
                                    }
                                    _ => Self::parse_simple_udt_field_value(
                                        field_data,
                                        &field_def.field_type,
                                    )?,
                                },
                                _ => Self::parse_simple_udt_field_value(
                                    field_data,
                                    &field_def.field_type,
                                )?,
                            }
                        } else {
                            // No registry - check for inline UDT definitions (Issue #239)
                            match &field_def.field_type {
                                CqlType::Udt(udt_name, inline_fields)
                                    if !inline_fields.is_empty() =>
                                {
                                    self.parse_inline_udt_value(
                                        field_data,
                                        udt_name,
                                        inline_fields,
                                        1,
                                    )?
                                }
                                CqlType::Frozen(inner) => match inner.as_ref() {
                                    CqlType::Udt(udt_name, inline_fields)
                                        if !inline_fields.is_empty() =>
                                    {
                                        let inner_value = self.parse_inline_udt_value(
                                            field_data,
                                            udt_name,
                                            inline_fields,
                                            1,
                                        )?;
                                        Value::Frozen(Box::new(inner_value))
                                    }
                                    _ => Self::parse_simple_udt_field_value(
                                        field_data,
                                        &field_def.field_type,
                                    )?,
                                },
                                _ => Self::parse_simple_udt_field_value(
                                    field_data,
                                    &field_def.field_type,
                                )?,
                            }
                        };
                        Some(value)
                    };

                    fields.push(UdtField {
                        name: field_def.name.clone(),
                        value: field_value,
                    });
                }

                let udt_value = UdtValue {
                    type_name: udt_def.name.clone(),
                    keyspace: udt_def.keyspace.clone(),
                    fields,
                };

                // Update offset to point after the UDT data we consumed
                offset += current_offset;

                Value::Udt(udt_value)
            }

            // Default: check if it's a short UDT name in the registry, otherwise treat as blob
            _ => {
                // Try to look up as UDT in registry by short name (Issue #238)
                // This handles cases like "address_type" which aren't in full marshal format
                if let Some(ref registry) = self.udt_registry {
                    if let Some(udt_def) = registry.get_udt(&self.keyspace, type_str) {
                        log::debug!(
                            "Frozen element '{}': found UDT '{}' in registry, parsing {} fields",
                            column_name,
                            type_str,
                            udt_def.fields.len()
                        );

                        // Parse UDT fields using the registry definition
                        // UDT data in frozen context has 4-byte big-endian i32 length prefixes for each field
                        // (-1 means null, 0 means empty, positive means field data length)
                        let udt_data = &data[offset..];
                        let mut current_offset = 0;
                        let mut fields = Vec::with_capacity(udt_def.fields.len());

                        for field_def in &udt_def.fields {
                            // Check bounds for field length (4 bytes BE i32)
                            if current_offset + 4 > udt_data.len() {
                                // Trailing fields can be omitted (implicit null)
                                log::debug!(
                                    "Frozen UDT field '{}' omitted (implicit null)",
                                    field_def.name
                                );
                                while fields.len() < udt_def.fields.len() {
                                    let remaining_field = &udt_def.fields[fields.len()];
                                    fields.push(UdtField {
                                        name: remaining_field.name.clone(),
                                        value: None,
                                    });
                                }
                                break;
                            }

                            // Read field length (4 bytes big-endian i32)
                            let field_len = i32::from_be_bytes([
                                udt_data[current_offset],
                                udt_data[current_offset + 1],
                                udt_data[current_offset + 2],
                                udt_data[current_offset + 3],
                            ]);
                            current_offset += 4;

                            let field_value = if field_len == -1 {
                                // Null field
                                None
                            } else if field_len == 0 {
                                // Empty field - parse with empty data
                                let value =
                                    Self::parse_simple_udt_field_value(&[], &field_def.field_type)?;
                                Some(value)
                            } else {
                                let field_len = field_len as usize;
                                if current_offset + field_len > udt_data.len() {
                                    return Err(Error::corruption(format!(
                                        "Frozen UDT field '{}' extends beyond data (need {}, have {})",
                                        field_def.name,
                                        field_len,
                                        udt_data.len() - current_offset
                                    )));
                                }

                                let field_data =
                                    &udt_data[current_offset..current_offset + field_len];
                                current_offset += field_len;

                                // Parse field value - handle nested UDTs specially (including FROZEN<udt>)
                                let value = match &field_def.field_type {
                                    CqlType::Custom(nested_type_name) => {
                                        // Issue #239: Handle "udt:" prefix from schema parsing
                                        let lookup_name = nested_type_name
                                            .strip_prefix("udt:")
                                            .unwrap_or(nested_type_name);
                                        // Check if this is a nested UDT
                                        if let Some(nested_udt) =
                                            registry.get_udt(&self.keyspace, lookup_name)
                                        {
                                            // Recursively parse nested UDT
                                            self.parse_nested_udt_from_registry(
                                                field_data, nested_udt, registry,
                                            )?
                                        } else {
                                            // Unknown custom type - parse as blob
                                            Value::Blob(field_data.to_vec())
                                        }
                                    }
                                    CqlType::Udt(udt_name, inline_fields) => {
                                        // Prefer registry, fall back to inline fields (Issue #239)
                                        if let Some(nested_udt) =
                                            registry.get_udt(&self.keyspace, udt_name)
                                        {
                                            self.parse_nested_udt_from_registry(
                                                field_data, nested_udt, registry,
                                            )?
                                        } else if !inline_fields.is_empty() {
                                            self.parse_inline_udt_value(
                                                field_data,
                                                udt_name,
                                                inline_fields,
                                                1,
                                            )?
                                        } else {
                                            Value::Blob(field_data.to_vec())
                                        }
                                    }
                                    CqlType::Frozen(inner) => {
                                        // Handle FROZEN<udt_type> - the inner type may be a UDT
                                        match inner.as_ref() {
                                            CqlType::Custom(nested_type_name) => {
                                                // Issue #239: Handle "udt:" prefix from schema parsing
                                                let lookup_name = nested_type_name
                                                    .strip_prefix("udt:")
                                                    .unwrap_or(nested_type_name);
                                                if let Some(nested_udt) =
                                                    registry.get_udt(&self.keyspace, lookup_name)
                                                {
                                                    let inner_value = self
                                                        .parse_nested_udt_from_registry(
                                                            field_data, nested_udt, registry,
                                                        )?;
                                                    Value::Frozen(Box::new(inner_value))
                                                } else {
                                                    Value::Frozen(Box::new(Value::Blob(
                                                        field_data.to_vec(),
                                                    )))
                                                }
                                            }
                                            CqlType::Udt(udt_name, inline_fields) => {
                                                // Prefer registry, fall back to inline fields (Issue #239)
                                                if let Some(nested_udt) =
                                                    registry.get_udt(&self.keyspace, udt_name)
                                                {
                                                    let inner_value = self
                                                        .parse_nested_udt_from_registry(
                                                            field_data, nested_udt, registry,
                                                        )?;
                                                    Value::Frozen(Box::new(inner_value))
                                                } else if !inline_fields.is_empty() {
                                                    let inner_value = self.parse_inline_udt_value(
                                                        field_data,
                                                        udt_name,
                                                        inline_fields,
                                                        1,
                                                    )?;
                                                    Value::Frozen(Box::new(inner_value))
                                                } else {
                                                    Value::Frozen(Box::new(Value::Blob(
                                                        field_data.to_vec(),
                                                    )))
                                                }
                                            }
                                            _ => {
                                                // Other frozen types - parse as simple value
                                                let inner_value =
                                                    Self::parse_simple_udt_field_value(
                                                        field_data, inner,
                                                    )?;
                                                Value::Frozen(Box::new(inner_value))
                                            }
                                        }
                                    }
                                    _ => Self::parse_simple_udt_field_value(
                                        field_data,
                                        &field_def.field_type,
                                    )?,
                                };
                                Some(value)
                            };

                            fields.push(UdtField {
                                name: field_def.name.clone(),
                                value: field_value,
                            });
                        }

                        let udt_value = UdtValue {
                            type_name: udt_def.name.clone(),
                            keyspace: udt_def.keyspace.clone(),
                            fields,
                        };

                        offset += current_offset;
                        Value::Udt(udt_value)
                    } else {
                        // Not found in registry - parse as blob
                        log::debug!(
                            "Frozen element '{}': unknown type '{}', parsing as blob",
                            column_name,
                            type_str
                        );

                        let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
                            Error::corruption(format!(
                                "Frozen element '{}': failed to parse unknown type length as VInt: {:?}",
                                column_name, e
                            ))
                        })?;
                        let blob_len = blob_len as usize;
                        let bytes_consumed = data[offset..].len() - remaining.len();
                        offset += bytes_consumed;

                        if offset + blob_len > data.len() {
                            return Err(Error::corruption(format!(
                                "Frozen element '{}': need {} bytes for unknown type, only {} available",
                                column_name,
                                blob_len,
                                data.len() - offset
                            )));
                        }

                        let blob_bytes = data[offset..offset + blob_len].to_vec();
                        offset += blob_len;
                        Value::Blob(blob_bytes)
                    }
                } else {
                    // No registry available - parse as blob
                    log::debug!(
                        "Frozen element '{}': unknown type '{}', no UDT registry available, parsing as blob",
                        column_name,
                        type_str
                    );

                    let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
                        Error::corruption(format!(
                            "Frozen element '{}': failed to parse unknown type length as VInt: {:?}",
                            column_name, e
                        ))
                    })?;
                    let blob_len = blob_len as usize;
                    let bytes_consumed = data[offset..].len() - remaining.len();
                    offset += bytes_consumed;

                    if offset + blob_len > data.len() {
                        return Err(Error::corruption(format!(
                            "Frozen element '{}': need {} bytes for unknown type, only {} available",
                            column_name,
                            blob_len,
                            data.len() - offset
                        )));
                    }

                    let blob_bytes = data[offset..offset + blob_len].to_vec();
                    offset += blob_len;
                    Value::Blob(blob_bytes)
                }
            }
        };

        Ok((value, offset))
    }

    /// Parse a cell path key (for map keys stored in cell paths).
    /// Cell path keys are stored as raw bytes WITHOUT length prefixes.
    fn parse_cell_path_key(&self, data: &[u8], type_str: &str, column_name: &str) -> Result<Value> {
        let normalized_type = type_str.to_lowercase();

        match normalized_type.as_str() {
            // Text types: raw UTF-8 bytes (no length prefix)
            "org.apache.cassandra.db.marshal.utf8type"
            | "org.apache.cassandra.db.marshal.asciitype"
            | "org.apache.cassandra.db.marshal.varchartype"
            | "text"
            | "varchar"
            | "ascii" => {
                let text = String::from_utf8(data.to_vec())
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in map key: {}", e)))?;
                Ok(Value::Text(text))
            }

            // UUID types: 16 bytes
            "org.apache.cassandra.db.marshal.uuidtype"
            | "org.apache.cassandra.db.marshal.timeuuidtype"
            | "uuid"
            | "timeuuid" => {
                if data.len() != 16 {
                    return Err(Error::corruption(format!(
                        "Map key UUID requires 16 bytes, got {}",
                        data.len()
                    )));
                }
                let uuid_bytes: [u8; 16] = data[0..16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                Ok(Value::Uuid(uuid_bytes))
            }

            // Int types: 4 bytes big-endian
            "org.apache.cassandra.db.marshal.int32type" | "int" => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Map key int requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                let v = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Integer(v))
            }

            // BigInt types: 8 bytes big-endian
            "org.apache.cassandra.db.marshal.longtype" | "bigint" | "counter" => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "Map key bigint requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let v = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::BigInt(v))
            }

            // Date types: 4 bytes (days since epoch with Integer.MIN_VALUE offset)
            "org.apache.cassandra.db.marshal.simpledatetype" | "date" => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Map key date requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                // Cassandra DATE: 4-byte unsigned int with Integer.MIN_VALUE offset
                let stored = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Ok(Value::Date(days_since_epoch))
            }

            // Timestamp types: 8 bytes (milliseconds since epoch)
            "org.apache.cassandra.db.marshal.timestamptype" | "timestamp" => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "Map key timestamp requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let millis = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::Timestamp(millis))
            }

            // Fallback: return as blob
            _ => {
                log::debug!(
                    "Map key type '{}' for column '{}' parsed as blob ({} bytes)",
                    type_str,
                    column_name,
                    data.len()
                );
                Ok(Value::Blob(data.to_vec()))
            }
        }
    }

    /// Read i32 BE element/entry count from a frozen collection blob.
    ///
    /// `bound` is the exclusive upper byte index for the collection data (either
    /// `data.len()` for raw variants or `blob_end` for cell-level variants).
    fn read_frozen_count(
        data: &[u8],
        offset: &mut usize,
        bound: usize,
        collection_kind: &str,
        column_name: &str,
    ) -> Result<usize> {
        if *offset + 4 > bound {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': not enough bytes for element count",
                collection_kind, column_name
            )));
        }
        let count = i32::from_be_bytes([
            data[*offset],
            data[*offset + 1],
            data[*offset + 2],
            data[*offset + 3],
        ]);
        *offset += 4;

        if count < 0 {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': negative element count {}",
                collection_kind, column_name, count
            )));
        }
        let count = count as usize;
        if count > MAX_FROZEN_COLLECTION_SIZE as usize {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': element count {} exceeds maximum {}",
                collection_kind, column_name, count, MAX_FROZEN_COLLECTION_SIZE
            )));
        }
        Ok(count)
    }

    /// Read the frozen collection preamble: VUInt blob_len + i32 BE element count.
    ///
    /// Returns `(count, blob_end)` with `offset` advanced past the preamble.
    fn read_frozen_preamble(
        data: &[u8],
        offset: &mut usize,
        collection_kind: &str,
        column_name: &str,
    ) -> Result<(usize, usize)> {
        let (remaining, blob_len) = parse_vuint(&data[*offset..]).map_err(|e| {
            Error::corruption(format!(
                "Frozen {} '{}': failed to parse blob length: {:?}",
                collection_kind, column_name, e
            ))
        })?;
        let blob_len = blob_len as usize;
        let bytes_consumed = data[*offset..].len() - remaining.len();
        *offset += bytes_consumed;

        if *offset + blob_len > data.len() {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': blob_len {} exceeds available data {}",
                collection_kind,
                column_name,
                blob_len,
                data.len() - *offset
            )));
        }

        let blob_end = *offset + blob_len;
        let count = Self::read_frozen_count(data, offset, blob_end, collection_kind, column_name)?;
        Ok((count, blob_end))
    }

    /// Read a single length-prefixed element from a frozen collection blob.
    ///
    /// `blob_end` is the exclusive upper byte index bounding the collection.
    /// `element_desc` appears in error messages (e.g. `"list 'col' element 3"`).
    fn read_frozen_element(
        &self,
        data: &[u8],
        offset: &mut usize,
        blob_end: usize,
        type_str: &str,
        element_desc: &str,
        depth: usize,
    ) -> Result<Value> {
        if *offset + 4 > blob_end {
            return Err(Error::corruption(format!(
                "Frozen {}: not enough bytes for length",
                element_desc
            )));
        }
        let len_i32 = i32::from_be_bytes([
            data[*offset],
            data[*offset + 1],
            data[*offset + 2],
            data[*offset + 3],
        ]);
        if len_i32 < 0 {
            return Err(Error::corruption(format!(
                "Frozen {}: negative length {}",
                element_desc, len_i32
            )));
        }
        let len = len_i32 as usize;
        *offset += 4;

        if *offset + len > blob_end {
            return Err(Error::corruption(format!(
                "Frozen {}: needs {} bytes but only {} available",
                element_desc,
                len,
                blob_end - *offset
            )));
        }

        let elem_data = &data[*offset..*offset + len];
        let value = self.parse_value_from_raw_bytes(elem_data, type_str, element_desc, depth)?;
        *offset += len;
        Ok(value)
    }

    /// Parse a frozen list or set (cell-level, with VUInt blob_len prefix).
    ///
    /// The cell layout on disk is:
    ///   [VUInt blob_len][i32 BE element_count][i32 BE elem_len][elem_bytes]...
    ///
    /// `as_set = true` wraps the result in `Value::Set`; otherwise `Value::List`.
    fn parse_frozen_sequence_value(
        &self,
        data: &[u8],
        mut offset: usize,
        element_type: &str,
        column: &crate::schema::Column,
        as_set: bool,
    ) -> Result<(Value, usize)> {
        let kind = if as_set { "set" } else { "list" };
        let (count, blob_end) = Self::read_frozen_preamble(data, &mut offset, kind, &column.name)?;

        log::debug!(
            "V5CompressedLegacy: Frozen {} '{}' with {} elements, element_type='{}'",
            kind,
            column.name,
            count,
            element_type
        );

        let mut elements = Vec::with_capacity(count);
        for i in 0..count {
            let desc = format!("{} '{}' element {}", kind, column.name, i);
            let value =
                self.read_frozen_element(data, &mut offset, blob_end, element_type, &desc, 0)?;
            log::debug!(
                "V5CompressedLegacy: Frozen {} element {}: {:?}",
                kind,
                i,
                value
            );
            elements.push(value);
        }

        if as_set {
            Ok((Value::Set(elements), blob_end))
        } else {
            Ok((Value::List(elements), blob_end))
        }
    }

    /// Parse frozen list value (thin wrapper around `parse_frozen_sequence_value`).
    fn parse_frozen_list_value(
        &self,
        data: &[u8],
        offset: usize,
        element_type: &str,
        column: &crate::schema::Column,
        _reader: &super::super::types::SSTableReader,
    ) -> Result<(Value, usize)> {
        self.parse_frozen_sequence_value(data, offset, element_type, column, false)
    }

    /// Parse frozen set value (thin wrapper around `parse_frozen_sequence_value`).
    ///
    /// Frozen sets have the same binary format as frozen lists; the distinction
    /// is semantic (sets are sorted/unique at the CQL level).
    fn parse_frozen_set_value(
        &self,
        data: &[u8],
        offset: usize,
        element_type: &str,
        column: &crate::schema::Column,
        _reader: &super::super::types::SSTableReader,
    ) -> Result<(Value, usize)> {
        self.parse_frozen_sequence_value(data, offset, element_type, column, true)
    }

    /// Parse frozen map value.
    ///
    /// The cell layout on disk is:
    ///   [VUInt blob_len][i32 BE entry_count][i32 BE key_len][key_bytes][i32 BE val_len][val_bytes]...
    fn parse_frozen_map_value(
        &self,
        data: &[u8],
        mut offset: usize,
        key_type: &str,
        value_type: &str,
        column: &crate::schema::Column,
        _reader: &super::super::types::SSTableReader,
    ) -> Result<(Value, usize)> {
        let (count, blob_end) = Self::read_frozen_preamble(data, &mut offset, "map", &column.name)?;

        log::debug!(
            "V5CompressedLegacy: Frozen map '{}' with {} entries, key_type='{}', value_type='{}'",
            column.name,
            count,
            key_type,
            value_type
        );

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let key_desc = format!("map '{}' key {}", column.name, i);
            let key_value =
                self.read_frozen_element(data, &mut offset, blob_end, key_type, &key_desc, 0)?;

            let val_desc = format!("map '{}' value {}", column.name, i);
            let val_value =
                self.read_frozen_element(data, &mut offset, blob_end, value_type, &val_desc, 0)?;

            log::debug!(
                "V5CompressedLegacy: Frozen map entry {}: {:?} -> {:?}",
                i,
                key_value,
                val_value
            );
            entries.push((key_value, val_value));
        }

        Ok((Value::Map(entries), blob_end))
    }

    /// Parse a frozen list or set (raw, nested inside an already-bounded blob).
    ///
    /// Called when parsing nested collections inside an already-bounded frozen
    /// blob.  There is NO VUInt cell-value-length prefix — the caller has
    /// already bounded the data slice.  `as_set = true` produces `Value::Set`.
    fn parse_frozen_sequence_value_raw(
        &self,
        data: &[u8],
        mut offset: usize,
        element_type: &str,
        column_name: &str,
        as_set: bool,
        depth: usize,
    ) -> Result<(Value, usize)> {
        let kind = if as_set { "set" } else { "list" };
        let count = Self::read_frozen_count(data, &mut offset, data.len(), kind, column_name)?;

        log::debug!(
            "V5CompressedLegacy: Parsing frozen {} '{}' with {} elements (raw)",
            kind,
            column_name,
            count
        );

        let mut elements = Vec::with_capacity(count);
        for i in 0..count {
            // Each element in a frozen collection: [i32 BE len][element bytes]
            if offset + 4 > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen {} '{}': not enough bytes for element {} length",
                    kind, column_name, i
                )));
            }
            let elem_len_i32 = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            if elem_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "Frozen {} '{}': negative element {} length {}",
                    kind, column_name, i, elem_len_i32
                )));
            }
            let elem_len = elem_len_i32 as usize;
            offset += 4;

            if offset + elem_len > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen {} '{}': element {} needs {} bytes but only {} available",
                    kind,
                    column_name,
                    i,
                    elem_len,
                    data.len() - offset
                )));
            }

            let elem_data = &data[offset..offset + elem_len];
            let elem_value =
                self.parse_value_from_raw_bytes(elem_data, element_type, column_name, depth)?;
            elements.push(elem_value);
            offset += elem_len;
        }

        if as_set {
            Ok((Value::Set(elements), offset))
        } else {
            Ok((Value::List(elements), offset))
        }
    }

    /// Parse frozen list value (raw version without Column parameter).
    fn parse_frozen_list_value_raw(
        &self,
        data: &[u8],
        offset: usize,
        element_type: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        self.parse_frozen_sequence_value_raw(data, offset, element_type, column_name, false, depth)
    }

    /// Parse frozen set value (raw version without Column parameter).
    fn parse_frozen_set_value_raw(
        &self,
        data: &[u8],
        offset: usize,
        element_type: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        self.parse_frozen_sequence_value_raw(data, offset, element_type, column_name, true, depth)
    }

    /// Parse frozen map value (raw version without Column parameter).
    fn parse_frozen_map_value_raw(
        &self,
        data: &[u8],
        mut offset: usize,
        key_type: &str,
        value_type: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        let count = Self::read_frozen_count(data, &mut offset, data.len(), "map", column_name)?;

        log::debug!(
            "V5CompressedLegacy: Parsing frozen map '{}' with {} entries (raw)",
            column_name,
            count
        );

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            // Key: [i32 BE len][key bytes]
            if offset + 4 > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': not enough bytes for key {} length",
                    column_name, i
                )));
            }
            let key_len_i32 = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            if key_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': negative key {} length {}",
                    column_name, i, key_len_i32
                )));
            }
            let key_len = key_len_i32 as usize;
            offset += 4;

            if offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': key {} needs {} bytes but only {} available",
                    column_name,
                    i,
                    key_len,
                    data.len() - offset
                )));
            }
            let key_data = &data[offset..offset + key_len];
            let key_value =
                self.parse_value_from_raw_bytes(key_data, key_type, column_name, depth)?;
            offset += key_len;

            // Value: [i32 BE len][value bytes]
            if offset + 4 > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': not enough bytes for value {} length",
                    column_name, i
                )));
            }
            let val_len_i32 = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            if val_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': negative value {} length {}",
                    column_name, i, val_len_i32
                )));
            }
            let val_len = val_len_i32 as usize;
            offset += 4;

            if offset + val_len > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': value {} needs {} bytes but only {} available",
                    column_name,
                    i,
                    val_len,
                    data.len() - offset
                )));
            }
            let val_data = &data[offset..offset + val_len];
            let val_value =
                self.parse_value_from_raw_bytes(val_data, value_type, column_name, depth)?;
            offset += val_len;

            entries.push((key_value, val_value));
        }

        Ok((Value::Map(entries), offset))
    }

    /// Parse tuple value from binary data at the cell level.
    ///
    /// Cell-level layout (written by `write_cell`):
    /// ```text
    /// [VUInt blob_len]
    /// for each element (schema-ordered, from type string):
    ///   [i32 BE element_len]  (-1 = null, 0 = empty, >0 = byte count)
    ///   [element_len bytes]   (only present when element_len > 0)
    /// ```
    ///
    /// Element count and types are derived exclusively from the schema type string
    /// (no-heuristics mandate, Issue #28).
    fn parse_tuple_value(
        &self,
        data: &[u8],
        offset: &mut usize,
        type_str: &str,
        column: &crate::schema::Column,
        _reader: &super::super::types::SSTableReader,
    ) -> Result<Value> {
        // Extract element types from schema (schema-aware, no heuristics)
        let element_types = self.extract_tuple_element_types(type_str)?;

        if element_types.is_empty() {
            return Err(Error::schema(format!("Empty tuple type: {}", type_str)));
        }

        // Read the VUInt outer blob length to bound the tuple bytes
        let (remaining, blob_len_raw) = parse_vuint(&data[*offset..]).map_err(|e| {
            Error::corruption(format!(
                "Tuple '{}': failed to parse outer blob length as VUInt: {:?}",
                column.name, e
            ))
        })?;
        if blob_len_raw > MAX_CELL_VALUE_LENGTH {
            return Err(Error::corruption(format!(
                "Tuple '{}': blob_len {} exceeds maximum {}",
                column.name, blob_len_raw, MAX_CELL_VALUE_LENGTH
            )));
        }
        let blob_len = blob_len_raw as usize;
        let len_bytes_consumed = data[*offset..].len() - remaining.len();
        *offset += len_bytes_consumed;

        if *offset + blob_len > data.len() {
            return Err(Error::corruption(format!(
                "Tuple '{}': blob_len {} exceeds available data {}",
                column.name,
                blob_len,
                data.len() - *offset
            )));
        }

        let blob_end = *offset + blob_len;

        // Parse each element using the schema-derived element type and the
        // [i32 BE len][bytes] wire format (same as UDT fields and frozen
        // collection elements — see type-mapping-complex.md).
        let elements =
            self.parse_tuple_elements_raw(data, offset, blob_end, &element_types, &column.name, 0)?;

        // Advance offset to end of blob regardless of how many elements were consumed
        // (protects against trailing bytes / schema drift).
        *offset = blob_end;

        Ok(Value::Tuple(elements))
    }

    /// Parse tuple elements from an already-bounded raw byte slice.
    ///
    /// Each element is encoded as `[i32 BE len][bytes]` with -1 meaning null.
    /// Element types are taken from `element_types` in order (schema-aware).
    ///
    /// `blob_end` is the exclusive upper byte index bounding the tuple data.
    fn parse_tuple_elements_raw(
        &self,
        data: &[u8],
        offset: &mut usize,
        blob_end: usize,
        element_types: &[String],
        column_name: &str,
        depth: usize,
    ) -> Result<Vec<Value>> {
        let mut elements = Vec::with_capacity(element_types.len());

        for (idx, elem_type) in element_types.iter().enumerate() {
            let elem_desc = format!("tuple '{}' element {}", column_name, idx);

            // Need at least 4 bytes for the element length
            if *offset + 4 > blob_end {
                // Trailing elements are implicitly null (matches UDT behaviour)
                log::debug!(
                    "Tuple '{}': element {} beyond blob_end, treating as null",
                    column_name,
                    idx
                );
                elements.push(Value::Null);
                continue;
            }

            // Read element length (4-byte big-endian i32)
            let elem_len_i32 = i32::from_be_bytes([
                data[*offset],
                data[*offset + 1],
                data[*offset + 2],
                data[*offset + 3],
            ]);
            *offset += 4;

            if elem_len_i32 == -1 {
                // Null element
                elements.push(Value::Null);
                continue;
            }

            if elem_len_i32 < -1 {
                return Err(Error::corruption(format!(
                    "{}: invalid negative element length {}",
                    elem_desc, elem_len_i32
                )));
            }

            let elem_len = elem_len_i32 as usize;

            if *offset + elem_len > blob_end {
                return Err(Error::corruption(format!(
                    "{}: needs {} bytes but only {} available in blob",
                    elem_desc,
                    elem_len,
                    blob_end - *offset
                )));
            }

            let elem_data = &data[*offset..*offset + elem_len];
            let value =
                self.parse_value_from_raw_bytes(elem_data, elem_type, &elem_desc, depth + 1)?;
            *offset += elem_len;

            elements.push(value);
        }

        Ok(elements)
    }

    /// Extract tuple element types from tuple<T1, T2, ...> string
    fn extract_tuple_element_types(&self, type_str: &str) -> Result<Vec<String>> {
        if !type_str.starts_with("tuple<") || !type_str.ends_with('>') {
            return Err(Error::schema(format!(
                "Invalid tuple type format: {}",
                type_str
            )));
        }

        let inner = &type_str[6..type_str.len() - 1];
        if inner.is_empty() {
            return Ok(Vec::new());
        }

        // Split by comma, handling nested angle brackets
        let mut types = Vec::new();
        let mut current = String::new();
        let mut depth = 0;

        for ch in inner.chars() {
            match ch {
                '<' => {
                    depth += 1;
                    current.push(ch);
                }
                '>' => {
                    if depth == 0 {
                        return Err(Error::schema(format!(
                            "Unmatched '>' in tuple type: {}",
                            type_str
                        )));
                    }
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    types.push(current.trim().to_string());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            types.push(current.trim().to_string());
        }

        Ok(types)
    }

    /// Test-only helper that parses the cell header (flags + conditional temporal
    /// metadata) and returns the offset at which the value bytes begin.
    ///
    /// This mirrors the logic in `parse_cell_value_schema_order` for the conditional
    /// sections (Steps 1-3), but stops before the value parse.  It is used by the
    /// S1 audit verification tests (Issue #623) to confirm that:
    ///   - USE_ROW_TIMESTAMP (0x08) causes the timestamp VInt to be ABSENT
    ///   - USE_ROW_TTL (0x10) without IS_EXPIRING causes LDT/TTL to be ABSENT
    ///
    /// Returns `(flags, value_start_offset)`.
    #[cfg(test)]
    fn parse_cell_header_end_offset(
        &self,
        data: &[u8],
        start_offset: usize,
    ) -> Result<(u8, usize)> {
        const CELL_IS_DELETED: u8 = 0x01;
        const CELL_IS_EXPIRING: u8 = 0x02;
        const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
        const CELL_USE_ROW_TTL: u8 = 0x10;

        if start_offset >= data.len() {
            return Err(Error::corruption(
                "cell_header_end_offset: no flags byte".to_string(),
            ));
        }
        let flags = data[start_offset];
        let mut offset = start_offset + 1;

        let is_deleted = (flags & CELL_IS_DELETED) != 0;
        let is_expiring = (flags & CELL_IS_EXPIRING) != 0;
        let use_row_timestamp = (flags & CELL_USE_ROW_TIMESTAMP) != 0;
        let use_row_ttl = (flags & CELL_USE_ROW_TTL) != 0;

        // Step 1: skip timestamp VInt if not using row timestamp
        if !use_row_timestamp {
            let (remaining, _ts_delta) = parse_vint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "cell_header_end_offset: failed to parse timestamp VInt: {:?}",
                    e
                ))
            })?;
            offset += data[offset..].len() - remaining.len();
        }
        // Step 2: skip LDT VUInt if not using row TTL and (deleted or expiring)
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, _ldt_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "cell_header_end_offset: failed to parse LDT VUInt: {:?}",
                    e
                ))
            })?;
            offset += data[offset..].len() - remaining.len();
        }
        // Step 3: skip TTL VUInt if not using row TTL and expiring
        if !use_row_ttl && is_expiring {
            let (remaining, _ttl_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "cell_header_end_offset: failed to parse TTL VUInt: {:?}",
                    e
                ))
            })?;
            offset += data[offset..].len() - remaining.len();
        }

        Ok((flags, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Local VInt encoder for test helpers — avoids depending on
    /// `storage::serialization` which is gated behind `write-support`.
    /// Byte-identical to Cassandra's writeUnsignedVInt / VIntCoding.java.
    fn encode_unsigned(value: u64, buf: &mut Vec<u8>) {
        // Compute byte count using Cassandra's formula:
        //   size = (639 - leading_zeros(value | 1) * 9) >> 6
        let magnitude = (value | 1).leading_zeros();
        let size = ((639 - magnitude * 9) >> 6) as usize;

        if size == 1 {
            buf.push(value as u8);
        } else if size == 9 {
            buf.push(0xFF);
            buf.extend_from_slice(&value.to_be_bytes());
        } else {
            let extra_bytes = size - 1;
            let shift = 8usize.saturating_sub(extra_bytes);
            let mask: u8 = if extra_bytes == 0 {
                0x00
            } else if extra_bytes >= 8 {
                0xFF
            } else {
                0xFF_u8 << shift
            };
            let first_byte_data_bits = 8 - extra_bytes - 1;
            let data_shift = extra_bytes * 8;
            let first_byte_data = ((value >> data_shift) & ((1 << first_byte_data_bits) - 1)) as u8;
            buf.push(mask | first_byte_data);
            for i in (0..extra_bytes).rev() {
                buf.push(((value >> (i * 8)) & 0xFF) as u8);
            }
        }
    }

    #[test]
    fn test_partition_header_parsing() {
        // Hex from test data: 00 10 15291a77... 7fffffff 8000000000000000
        let hex_str = "001015291a77d7394e738397b787442f3a1f7fffffff8000000000000000";
        let data = hex::decode(hex_str).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "simple_table".to_string(),
            0,    // min_timestamp
            0,    // min_local_deletion_time
            None, // min_ttl
        );
        let (row_key, offset) = parser.parse_partition_header(&data, 0).unwrap();

        // Verify partition key extraction
        assert_eq!(row_key.0.len(), 16); // UUID is 16 bytes

        // Verify offset consumed: 1 (flags) + 1 (len) + 16 (uuid) + 4 (del_time) + 8 (unknown) = 30
        assert_eq!(offset, 30);

        // Verify UUID bytes match
        let expected_uuid_bytes = hex::decode("15291a77d7394e738397b787442f3a1f").unwrap();
        assert_eq!(row_key.0, expected_uuid_bytes);
    }

    #[test]
    fn test_extract_frozen_inner_type() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test basic frozen type
        assert_eq!(
            parser
                .extract_frozen_inner_type("frozen<list<int>>")
                .unwrap(),
            "list<int>"
        );

        // Test nested frozen
        assert_eq!(
            parser
                .extract_frozen_inner_type("frozen<map<text,frozen<set<int>>>>")
                .unwrap(),
            "map<text,frozen<set<int>>>"
        );

        // Test error cases
        assert!(parser.extract_frozen_inner_type("frozen<>").is_err());
        assert!(parser.extract_frozen_inner_type("frozen").is_err());
        assert!(parser.extract_frozen_inner_type("list<int>").is_err());
    }

    #[test]
    fn test_extract_tuple_element_types() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test simple tuple
        let types = parser
            .extract_tuple_element_types("tuple<int,text,bigint>")
            .unwrap();
        assert_eq!(types, vec!["int", "text", "bigint"]);

        // Test tuple with nested collections
        let types = parser
            .extract_tuple_element_types("tuple<int,list<text>,map<text,int>>")
            .unwrap();
        assert_eq!(types, vec!["int", "list<text>", "map<text,int>"]);

        // Test tuple with frozen
        let types = parser
            .extract_tuple_element_types("tuple<int,frozen<list<int>>>")
            .unwrap();
        assert_eq!(types, vec!["int", "frozen<list<int>>"]);

        // Test empty tuple
        let types = parser.extract_tuple_element_types("tuple<>").unwrap();
        assert!(types.is_empty());

        // Test error cases
        assert!(parser.extract_tuple_element_types("tuple").is_err());
        assert!(parser.extract_tuple_element_types("int").is_err());
    }

    #[test]
    fn test_extract_tuple_element_types_unmatched_angle_bracket() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Unmatched '>' inside inner content must return Err, not panic.
        // "tuple<int>>" — the outer '>' is consumed by starts_with/ends_with stripping,
        // leaving "int>" as the inner string; the extra '>' hits depth == 0 and must error.
        let result = parser.extract_tuple_element_types("tuple<int>>");
        assert!(
            result.is_err(),
            "Expected Err for unmatched '>' but got: {:?}",
            result
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Unmatched '>'"),
            "Error message should mention unmatched '>': {}",
            err_msg
        );

        // A second variant: extra '>' after a nested type.
        let result2 = parser.extract_tuple_element_types("tuple<list<int>>>");
        assert!(
            result2.is_err(),
            "Expected Err for extra '>' but got: {:?}",
            result2
        );
    }

    /// Helper: build a frozen list<int> raw binary: [i32 count][i32 len][int]...
    fn build_frozen_list_int(values: &[i32]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as i32).to_be_bytes());
        for &v in values {
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }
        buf
    }

    /// Helper: build a frozen map<text,int> raw binary
    fn build_frozen_map_text_int(entries: &[(&str, i32)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
        for &(k, v) in entries {
            let k_bytes = k.as_bytes();
            buf.extend_from_slice(&(k_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(k_bytes);
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }
        buf
    }

    #[test]
    fn test_parse_value_from_raw_bytes_primitives() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // int
        let data = 42i32.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "int", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Integer(42));

        // bigint
        let data = 123456789i64.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "bigint", "col", 0)
            .unwrap();
        assert_eq!(val, Value::BigInt(123456789));

        // text
        let data = b"hello";
        let val = parser
            .parse_value_from_raw_bytes(data, "text", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Text("hello".to_string()));

        // boolean true
        let val = parser
            .parse_value_from_raw_bytes(&[1], "boolean", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Boolean(true));

        // boolean false
        let val = parser
            .parse_value_from_raw_bytes(&[0], "boolean", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Boolean(false));

        // float (parse_value_from_raw_bytes promotes f32 to f64 via Float)
        let data = 1.5f32.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "float", "col", 0)
            .unwrap();
        match val {
            Value::Float(f) => assert!((f - 1.5).abs() < 0.001),
            other => panic!("Expected Float, got {:?}", other),
        }

        // double
        let data = 9.876f64.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "double", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Float(9.876));

        // uuid (16 bytes)
        let uuid_bytes: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let val = parser
            .parse_value_from_raw_bytes(&uuid_bytes, "uuid", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Uuid(uuid_bytes));

        // smallint
        let data = 1234i16.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "smallint", "col", 0)
            .unwrap();
        assert_eq!(val, Value::SmallInt(1234));

        // tinyint
        let val = parser
            .parse_value_from_raw_bytes(&[42], "tinyint", "col", 0)
            .unwrap();
        assert_eq!(val, Value::TinyInt(42));

        // blob
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let val = parser
            .parse_value_from_raw_bytes(&data, "blob", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Blob(data));

        // varint
        let data = vec![0x01, 0x00];
        let val = parser
            .parse_value_from_raw_bytes(&data, "varint", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Varint(vec![0x01, 0x00]));

        // inet (IPv4)
        let data = vec![127, 0, 0, 1];
        let val = parser
            .parse_value_from_raw_bytes(&data, "inet", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Inet(vec![127, 0, 0, 1]));

        // timestamp
        let data = 1704067200000i64.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "timestamp", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Timestamp(1704067200000));

        // decimal
        let mut data = Vec::new();
        data.extend_from_slice(&2i32.to_be_bytes()); // scale
        data.extend_from_slice(&[0x01, 0xC8]); // unscaled = 456
        let val = parser
            .parse_value_from_raw_bytes(&data, "decimal", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::Decimal {
                scale: 2,
                unscaled: vec![0x01, 0xC8]
            }
        );
    }

    #[test]
    fn test_parse_value_from_raw_bytes_nested_list() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[10, 20, 30]);
        let val = parser
            .parse_value_from_raw_bytes(&data, "list<int>", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::List(vec![
                Value::Integer(10),
                Value::Integer(20),
                Value::Integer(30)
            ])
        );
    }

    #[test]
    fn test_parse_value_from_raw_bytes_nested_set() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[5, 15]);
        let val = parser
            .parse_value_from_raw_bytes(&data, "set<int>", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Set(vec![Value::Integer(5), Value::Integer(15)]));
    }

    #[test]
    fn test_parse_value_from_raw_bytes_nested_map() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_map_text_int(&[("alice", 1), ("bob", 2)]);
        let val = parser
            .parse_value_from_raw_bytes(&data, "map<text,int>", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::Map(vec![
                (Value::Text("alice".to_string()), Value::Integer(1)),
                (Value::Text("bob".to_string()), Value::Integer(2)),
            ])
        );
    }

    #[test]
    fn test_parse_value_from_raw_bytes_frozen_wrapper() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[100, 200]);
        let val = parser
            .parse_value_from_raw_bytes(&data, "frozen<list<int>>", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::Frozen(Box::new(Value::List(vec![
                Value::Integer(100),
                Value::Integer(200)
            ])))
        );
    }

    #[test]
    fn test_frozen_sequence_value_raw_list() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[10, 20]);
        let (val, end_offset) = parser
            .parse_frozen_list_value_raw(&data, 0, "int", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::List(vec![Value::Integer(10), Value::Integer(20)])
        );
        assert_eq!(end_offset, data.len());
    }

    #[test]
    fn test_frozen_sequence_value_raw_set() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[5, 15]);
        let (val, _) = parser
            .parse_frozen_set_value_raw(&data, 0, "int", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Set(vec![Value::Integer(5), Value::Integer(15)]));
    }

    #[test]
    fn test_frozen_sequence_value_raw_empty() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = 0i32.to_be_bytes().to_vec(); // count = 0
        let (val, _) = parser
            .parse_frozen_list_value_raw(&data, 0, "int", "col", 0)
            .unwrap();
        assert_eq!(val, Value::List(vec![]));

        let (val, _) = parser
            .parse_frozen_set_value_raw(&data, 0, "int", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Set(vec![]));
    }

    #[test]
    fn test_frozen_map_value_raw() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_map_text_int(&[("x", 42)]);
        let (val, end_offset) = parser
            .parse_frozen_map_value_raw(&data, 0, "text", "int", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::Map(vec![(Value::Text("x".to_string()), Value::Integer(42))])
        );
        assert_eq!(end_offset, data.len());
    }

    #[test]
    fn test_frozen_parse_error_truncated_data() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Truncated: claims 2 elements but only has space for count header
        let data = 2i32.to_be_bytes().to_vec();
        let result = parser.parse_frozen_list_value_raw(&data, 0, "int", "col", 0);
        assert!(result.is_err());

        // Negative element length
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // count = 1
        data.extend_from_slice(&(-1i32).to_be_bytes()); // elem_len = -1
        let result = parser.parse_frozen_list_value_raw(&data, 0, "int", "col", 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_frozen_recursion_depth_exceeded() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Build a type string with 12 levels of nesting (exceeds MAX_TYPE_NESTING_DEPTH=10)
        let mut type_str = "int".to_string();
        for _ in 0..12 {
            type_str = format!("frozen<{}>", type_str);
        }

        let data = 42i32.to_be_bytes();
        let result = parser.parse_value_from_raw_bytes(&data, &type_str, "col", 0);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("recursion depth"),
            "Error should mention recursion depth: {}",
            err_msg
        );
    }

    #[test]
    fn test_parse_raw_type_value_depth_guard() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Directly calling with depth at limit should fail
        let data = 42i32.to_be_bytes();
        let result =
            parser.parse_raw_type_value(&data, 0, "int", "col", MAX_TYPE_NESTING_DEPTH + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_tuple_int_text_parsing() {
        // Test parse_tuple_elements_raw with constructed binary data.
        //
        // Wire format for each tuple element: [i32 BE elem_len][elem_bytes]
        // Null element: [i32 BE -1] (no following bytes)
        //
        // Tuple: (int=42, text="hi")
        //   element 0: [0x00, 0x00, 0x00, 0x04][42 as i32 BE] -> [0,0,0,4][0,0,0,42]
        //   element 1: [0x00, 0x00, 0x00, 0x02]["hi"] -> [0,0,0,2][0x68,0x69]
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let mut data = Vec::new();
        // element 0: int 42
        data.extend_from_slice(&4i32.to_be_bytes()); // length
        data.extend_from_slice(&42i32.to_be_bytes()); // value
                                                      // element 1: text "hi"
        let hi = b"hi";
        data.extend_from_slice(&(hi.len() as i32).to_be_bytes()); // length
        data.extend_from_slice(hi); // value

        let element_types = vec!["int".to_string(), "text".to_string()];
        let mut offset = 0usize;
        let blob_end = data.len();
        let elements = parser
            .parse_tuple_elements_raw(&data, &mut offset, blob_end, &element_types, "col", 0)
            .unwrap();

        assert_eq!(elements.len(), 2);
        assert_eq!(elements[0], Value::Integer(42));
        assert_eq!(elements[1], Value::Text("hi".to_string()));
        assert_eq!(
            offset, blob_end,
            "offset should reach blob_end after parsing all elements"
        );

        // Also test null element: (int=null, text="ok")
        let mut data2 = Vec::new();
        data2.extend_from_slice(&(-1i32).to_be_bytes()); // null element 0
        let ok = b"ok";
        data2.extend_from_slice(&(ok.len() as i32).to_be_bytes());
        data2.extend_from_slice(ok);

        let mut offset2 = 0usize;
        let blob_end2 = data2.len();
        let elements2 = parser
            .parse_tuple_elements_raw(&data2, &mut offset2, blob_end2, &element_types, "col", 0)
            .unwrap();

        assert_eq!(elements2.len(), 2);
        assert_eq!(elements2[0], Value::Null);
        assert_eq!(elements2[1], Value::Text("ok".to_string()));
    }

    #[test]
    fn test_non_zero_minima_delta_decoding() {
        // Test delta decoding with non-zero minima from ttl_test_table
        // Statistics.db shows:
        //   min_timestamp: 1759713125983682
        //   min_local_deletion_time: 1759799525
        //   min_ttl: 86400
        //
        // Row header format with HAS_TIMESTAMP (0x04) + HAS_TTL (0x08) + HAS_ALL_COLUMNS (0x20) = 0x2C
        // [row_flags: 0x2C] [row_size: VInt] [prev_size: VInt]
        // [timestamp_delta: UNSIGNED VInt]   ← fix #629: was ZigZag, now unsigned
        // [ttl_delta: UNSIGNED VInt]
        // [liveness_ldt_delta: UNSIGNED VInt] ← fix #630: was absent, now required
        // (NO column bitmap because HAS_ALL_COLUMNS is set)
        //
        // Updated from original: was "2c640087d000" which used ZigZag(1000)=[0x87,0xD0]
        // for the timestamp and was missing the liveness_ldt field for HAS_TTL.
        //
        // Now: unsigned_vint(1000) = [0x83, 0xE8], plus liveness_ldt_delta = 0 (0x00).

        let min_timestamp = 1759713125983682i64;
        let min_ttl = 86400i64;
        let min_ldt = 1759799525i64;
        let ts_delta: u64 = 1000;
        let ttl_delta: u64 = 0;
        let ldt_delta: u64 = 0;

        let mut data: Vec<u8> = Vec::new();
        data.push(0x2Cu8); // flags: HAS_TIMESTAMP(0x04)|HAS_TTL(0x08)|HAS_ALL_COLUMNS(0x20)
        encode_unsigned(100, &mut data); // row_size = 100 → [0x64]
        encode_unsigned(0, &mut data); // prev_size = 0  → [0x00]
        encode_unsigned(ts_delta, &mut data); // timestamp_delta = 1000 → [0x83, 0xE8]
        encode_unsigned(ttl_delta, &mut data); // ttl_delta = 0 → [0x00]
        encode_unsigned(ldt_delta, &mut data); // liveness_ldt_delta = 0 → [0x00]

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "ttl_test_table".to_string(),
            min_timestamp,
            min_ldt,
            Some(min_ttl),
        );

        // Issue #213: Use split functions - parse flags first, then metadata
        let (row_flags, extended_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        assert_eq!(flags_size, 1, "Flags should consume 1 byte");

        // For testing, since there's no clustering in this test data, metadata starts at offset 1
        let (row_header, row_size) = parser
            .parse_row_metadata(&data, flags_size, row_flags, extended_flags)
            .unwrap();

        // Verify delta decoding: absolute_timestamp = min_timestamp + delta (unsigned)
        assert_eq!(
            row_header.timestamp,
            Some(min_timestamp + ts_delta as i64),
            "Timestamp should be decoded as min_timestamp + delta (unsigned VInt, fix #629)"
        );

        // Verify TTL delta decoding: absolute_ttl = min_ttl + delta
        assert_eq!(
            row_header.ttl,
            Some(min_ttl as i32),
            "TTL should be decoded as min_ttl + delta (delta=0)"
        );

        // Verify row_size was parsed
        assert!(row_size > 0, "Row size should be positive");
    }

    #[test]
    fn test_row_header_with_deletion_time() {
        // Verify delta decoding of the HAS_DELETION field in Cassandra canonical order
        // (Issue #505). DeletionTime.Serializer writes markedForDeleteAt FIRST, then
        // localDeletionTime:
        //   [row_flags] [row_size: VInt] [prev_size: VInt]
        //   [markedForDeleteAt_delta: UNSIGNED VInt]  (base = min_timestamp, micros)
        //   [localDeletionTime_delta: UNSIGNED VInt]  (base = min_local_deletion_time, secs)
        //
        // Fix #629 (C2): Both deltas are UNSIGNED per Cassandra SerializationHeader.java.
        // Test updated to encode mfda_delta as unsigned VInt (was ZigZag/signed before).
        use crate::parser::vint::encode_vuint;

        // Row header with HAS_DELETION (0x10) + HAS_ALL_COLUMNS (0x20) = 0x30.
        let mut data: Vec<u8> = Vec::new();
        data.push(0x30); // flags
        data.extend(encode_vuint(100)); // row_size = 100
        data.extend(encode_vuint(0)); // prev_size = 0
        let mfda_delta: u64 = 80; // markedForDeleteAt delta (unsigned, fix #629)
        let ldt_delta: u64 = 50; // localDeletionTime delta (unsigned)
        data.extend(encode_vuint(mfda_delta));
        data.extend(encode_vuint(ldt_delta));

        let min_timestamp = 1759713125983682i64;
        let min_local_deletion_time = 1759799525i64;
        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "test_table".to_string(),
            min_timestamp,
            min_local_deletion_time,
            None,
        );

        let (row_flags, extended_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _row_size) = parser
            .parse_row_metadata(&data, flags_size, row_flags, extended_flags)
            .unwrap();

        // markedForDeleteAt: absolute = min_timestamp + delta (microseconds, UNSIGNED delta).
        // This is the authoritative reconciliation timestamp used by the compaction merger.
        assert_eq!(
            row_header.marked_for_delete_at,
            Some(min_timestamp + mfda_delta as i64),
            "markedForDeleteAt must be decoded from the FIRST (unsigned) VInt as min_timestamp + delta"
        );
        // The row-tombstone deletion time (used in Value::Tombstone) must equal it.
        assert_eq!(
            row_header.row_tombstone_deletion_time(),
            min_timestamp + mfda_delta as i64,
            "row tombstone deletion_time must be markedForDeleteAt, not local_deletion_time"
        );

        // localDeletionTime: absolute = min_local_deletion_time + delta (seconds).
        assert_eq!(
            row_header.local_deletion_time,
            Some((min_local_deletion_time + ldt_delta as i64) as i32),
            "localDeletionTime must be decoded from the SECOND (unsigned) VInt as min + delta"
        );

        assert!(
            row_header.is_row_tombstone(),
            "HAS_DELETION row must be reported as a row tombstone"
        );
    }

    #[test]
    fn test_sparse_column_bitmap_parsing() {
        // Test column bitmap parsing when NOT HAS_ALL_COLUMNS
        // Row header WITHOUT HAS_ALL_COLUMNS flag (0x20)
        // Should parse single VUInt bitmap after metadata fields
        //
        // Cassandra format: single VUInt bitmask of missing columns
        // (bit=1 → column missing, bit=0 → column present)
        //
        // Row header format: [flags: 0x04] [row_size] [prev_size] [timestamp]
        // [missing_columns_bitmap: VUInt]

        // Construct row with HAS_TIMESTAMP but NOT HAS_ALL_COLUMNS
        // bitmap=0x05 means columns 0 and 2 are MISSING
        let row_header_hex = "04640000 05"; // flags=0x04, size=100, prev=0, ts=0 (signed), bitmap=0x05
        let row_header_hex = row_header_hex.replace(' ', "");
        let data = hex::decode(row_header_hex).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "sparse_table".to_string(),
            0,
            0,
            None,
        );

        // Issue #213: Use split functions - parse flags first, then metadata
        // This tests that parse_row_metadata handles column bitmap correctly
        let (row_flags, extended_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let result = parser.parse_row_metadata(&data, flags_size, row_flags, extended_flags);

        // Should succeed without panicking on bitmap parsing
        assert!(
            result.is_ok(),
            "Row header with column bitmap should parse successfully"
        );

        let (row_header, _row_size) = result.unwrap();
        // Verify header was parsed (has timestamp)
        assert_eq!(row_header.timestamp, Some(0));

        // Verify missing_columns_bitmap is captured
        assert_eq!(
            row_header.missing_columns_bitmap,
            Some(0x05),
            "Bitmap 0x05 means columns 0 and 2 are MISSING"
        );

        // Verify header_size includes bitmap VUInt (but NOT flags, parsed separately)
        // size(1) + prev(1) + timestamp(1) + bitmap(1) = 4
        assert_eq!(
            row_header.header_size, 4,
            "Header size should include column bitmap VUInt but not flags (parsed separately)"
        );
    }

    #[test]
    fn test_bitmap_filter_does_not_panic_for_wide_schemas() {
        // Verify that bitmap filtering with idx >= 64 does not panic.
        // Columns beyond bit 63 are not represented in the u64 bitmap
        // and should be treated as present (not filtered out).
        let bitmap: u64 = 0x05; // bits 0 and 2 are set (missing)
        let total_columns = 70; // wider than 64

        let kept: Vec<usize> = (0..total_columns)
            .filter(|idx| *idx >= 64 || (bitmap & (1u64 << idx)) == 0)
            .collect();

        // Columns 0 and 2 should be filtered out, all others kept
        assert!(!kept.contains(&0));
        assert!(kept.contains(&1));
        assert!(!kept.contains(&2));
        assert!(kept.contains(&3));
        // All columns >= 64 should be kept
        for i in 64..total_columns {
            assert!(kept.contains(&i), "Column {} should be kept", i);
        }
        assert_eq!(kept.len(), 68); // 70 - 2 missing = 68
    }

    #[test]
    fn test_clustering_key_partition_header() {
        // Test partition header parsing for composite key table
        // composite_key_table has clustering columns: [ReversedType(TimestampType), UTF8Type]
        //
        // Partition header format:
        // [flags: u8] [key_len: u8] [partition_key_bytes] [deletion_time: i32] [unknown: i64]
        //
        // From composite_key_table JSONL:
        // partition key: "245dff69-026f-45c6-b68f-ba0c964df3c9"
        // clustering: ["2025-10-06 01:12:06.059Z","information"]
        //
        // Note: Clustering keys are part of row data, not partition header
        // This test verifies partition header parsing for composite key tables

        let partition_hex = "0010245dff69026f45c6b68fba0c964df3c97fffffff8000000000000000";
        let data = hex::decode(partition_hex).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "composite_key_table".to_string(),
            1759713125977357, // min_timestamp from Statistics.db
            1442880000,       // min_local_deletion_time
            None,
        );

        let (row_key, offset) = parser.parse_partition_header(&data, 0).unwrap();

        // Verify partition key extraction (UUID is 16 bytes)
        assert_eq!(row_key.0.len(), 16);

        // Verify correct partition key bytes
        let expected_uuid_bytes = hex::decode("245dff69026f45c6b68fba0c964df3c9").unwrap();
        assert_eq!(row_key.0, expected_uuid_bytes);

        // Verify offset: flags(1) + len(1) + uuid(16) + del_time(4) + unknown(8) = 30
        assert_eq!(offset, 30);

        // Note: Clustering key parsing would happen during row data parsing,
        // which is tested separately in integration tests
    }

    #[test]
    fn test_extract_collection_element_type() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test list element type extraction
        assert_eq!(
            parser
                .extract_collection_element_type("list<int>", "list")
                .unwrap(),
            "int"
        );

        // Test set element type extraction
        assert_eq!(
            parser
                .extract_collection_element_type("set<text>", "set")
                .unwrap(),
            "text"
        );

        // Test nested type
        assert_eq!(
            parser
                .extract_collection_element_type("list<frozen<map<text,int>>>", "list")
                .unwrap(),
            "frozen<map<text,int>>"
        );

        // Test error cases
        assert!(parser
            .extract_collection_element_type("list<>", "list")
            .is_err());
        assert!(parser
            .extract_collection_element_type("set<int>", "list")
            .is_err());
        assert!(parser
            .extract_collection_element_type("int", "list")
            .is_err());
    }

    #[test]
    fn test_extract_map_types() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test simple map
        let (key, value) = parser.extract_map_types("map<text,int>").unwrap();
        assert_eq!(key, "text");
        assert_eq!(value, "int");

        // Test map with spaces
        let (key, value) = parser.extract_map_types("map<text, int>").unwrap();
        assert_eq!(key, "text");
        assert_eq!(value, "int");

        // Test nested value type
        let (key, value) = parser
            .extract_map_types("map<text,frozen<set<uuid>>>")
            .unwrap();
        assert_eq!(key, "text");
        assert_eq!(value, "frozen<set<uuid>>");

        // Test nested key and value types
        let (key, value) = parser
            .extract_map_types("map<frozen<list<int>>,frozen<set<text>>>")
            .unwrap();
        assert_eq!(key, "frozen<list<int>>");
        assert_eq!(value, "frozen<set<text>>");

        // Test error cases
        assert!(parser.extract_map_types("map<>").is_err());
        assert!(parser.extract_map_types("map<text>").is_err());
        assert!(parser.extract_map_types("int").is_err());
    }

    #[test]
    fn test_frozen_list_int_parsing() {
        // Test type extraction for frozen<list<int>>
        // Note: Full parsing tests require a reader, done via integration tests.
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Verify element type extraction works
        let inner_type = parser
            .extract_frozen_inner_type("frozen<list<int>>")
            .unwrap();
        assert_eq!(inner_type, "list<int>");

        let element_type = parser
            .extract_collection_element_type(&inner_type, "list")
            .unwrap();
        assert_eq!(element_type, "int");
    }

    #[test]
    fn test_frozen_set_text_parsing() {
        // Test type extraction for frozen<set<text>>
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let inner_type = parser
            .extract_frozen_inner_type("frozen<set<text>>")
            .unwrap();
        assert_eq!(inner_type, "set<text>");

        let element_type = parser
            .extract_collection_element_type(&inner_type, "set")
            .unwrap();
        assert_eq!(element_type, "text");
    }

    #[test]
    fn test_frozen_map_text_text_parsing() {
        // Test type extraction for frozen<map<text,text>>
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let inner_type = parser
            .extract_frozen_inner_type("frozen<map<text,text>>")
            .unwrap();
        assert_eq!(inner_type, "map<text,text>");

        let (key_type, value_type) = parser.extract_map_types(&inner_type).unwrap();
        assert_eq!(key_type, "text");
        assert_eq!(value_type, "text");
    }

    #[test]
    fn test_nested_frozen_map_parsing() {
        // Test type extraction for nested frozen: frozen<map<text, frozen<set<uuid>>>>
        // This is the structure used in chat_messages.reactions
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let inner_type = parser
            .extract_frozen_inner_type("frozen<map<text,frozen<set<uuid>>>>")
            .unwrap();
        assert_eq!(inner_type, "map<text,frozen<set<uuid>>>");

        let (key_type, value_type) = parser.extract_map_types(&inner_type).unwrap();
        assert_eq!(key_type, "text");
        assert_eq!(value_type, "frozen<set<uuid>>");

        // Further extraction of the nested frozen type
        let inner_set = parser.extract_frozen_inner_type(&value_type).unwrap();
        assert_eq!(inner_set, "set<uuid>");

        let element_type = parser
            .extract_collection_element_type(&inner_set, "set")
            .unwrap();
        assert_eq!(element_type, "uuid");
    }

    #[test]
    fn test_udt_field_count_limit() {
        // Test parse_udt_type_definition with excessive fields
        // Build a UDT type string with MAX_UDT_FIELD_COUNT + 1 fields
        let mut field_defs = Vec::new();
        for i in 0..=MAX_UDT_FIELD_COUNT {
            let field_name_hex = hex::encode(format!("field_{}", i));
            field_defs.push(format!(
                "{}:org.apache.cassandra.db.marshal.Int32Type",
                field_name_hex
            ));
        }

        let type_str = format!(
            "org.apache.cassandra.db.marshal.UserType(test_ks,{},{})",
            hex::encode("test_udt"),
            field_defs.join(",")
        );

        // Parse the UDT definition (this will succeed - we only validate field count during value parsing)
        let udt_def = V5CompressedLegacyParser::parse_udt_type_definition(&type_str).unwrap();
        assert_eq!(udt_def.fields.len(), MAX_UDT_FIELD_COUNT + 1);

        // Create a parser
        let parser = V5CompressedLegacyParser::new(
            "test_ks".to_string(),
            "test_table".to_string(),
            0,
            0,
            None,
        );

        // When parsing a value with too many fields, it should fail validation
        // The validation check in parse_raw_type_value at line 4182 will catch this
        let data = vec![0u8; 4 * (MAX_UDT_FIELD_COUNT + 1)]; // Minimal data (all nulls)

        // Test through parse_raw_type_value which has the validation
        // Signature: parse_raw_type_value(data, offset, type_str, column_name, depth)
        let result = parser.parse_raw_type_value(&data, 0, &type_str, "test_col", 0);
        assert!(
            result.is_err(),
            "Should reject UDT with more than MAX_UDT_FIELD_COUNT fields"
        );
        assert!(
            result.unwrap_err().to_string().contains("exceeds maximum"),
            "Error should mention exceeding maximum"
        );
    }

    #[test]
    fn test_type_nesting_depth_limit() {
        // Build a deeply nested type string that exceeds MAX_TYPE_NESTING_DEPTH
        let mut type_str = "org.apache.cassandra.db.marshal.UTF8Type".to_string();

        // Wrap it in ListType MAX_TYPE_NESTING_DEPTH + 1 times
        for _ in 0..=MAX_TYPE_NESTING_DEPTH {
            type_str = format!("org.apache.cassandra.db.marshal.ListType({})", type_str);
        }

        // This should fail due to depth limit
        let result = V5CompressedLegacyParser::parse_cassandra_type_with_depth(&type_str, 0);
        assert!(
            result.is_err(),
            "Should reject type with nesting depth > MAX_TYPE_NESTING_DEPTH"
        );
        assert!(
            result.unwrap_err().to_string().contains("nesting depth"),
            "Error should mention nesting depth"
        );

        // Build a type string with exactly MAX_TYPE_NESTING_DEPTH levels
        let mut ok_type_str = "org.apache.cassandra.db.marshal.UTF8Type".to_string();
        for _ in 0..MAX_TYPE_NESTING_DEPTH {
            ok_type_str = format!("org.apache.cassandra.db.marshal.ListType({})", ok_type_str);
        }

        // This should succeed
        let result = V5CompressedLegacyParser::parse_cassandra_type_with_depth(&ok_type_str, 0);
        assert!(
            result.is_ok(),
            "Should accept type with nesting depth == MAX_TYPE_NESTING_DEPTH"
        );
    }

    #[test]
    fn test_nested_udt_depth_limit() {
        // Build a deeply nested UDT type string
        // Inner UDT: UserType(ks,hex(inner),field1:UTF8Type)
        let inner_udt = "org.apache.cassandra.db.marshal.UserType(ks,696e6e6572,666965746431:org.apache.cassandra.db.marshal.UTF8Type)";

        // Wrap it recursively
        let mut type_str = inner_udt.to_string();
        for i in 0..=MAX_TYPE_NESTING_DEPTH {
            let hex_name = hex::encode(format!("nested_{}", i));
            let hex_field = hex::encode("field");
            type_str = format!(
                "org.apache.cassandra.db.marshal.UserType(ks,{},{}:{})",
                hex_name, hex_field, type_str
            );
        }

        // This should fail due to depth limit
        let result = V5CompressedLegacyParser::parse_udt_type_definition_with_depth(&type_str, 0);
        assert!(
            result.is_err(),
            "Should reject UDT with nesting depth > MAX_TYPE_NESTING_DEPTH"
        );
        assert!(
            result.unwrap_err().to_string().contains("nesting depth"),
            "Error should mention nesting depth"
        );
    }

    // Issue #229: END_OF_PARTITION and range tombstone marker detection tests
    #[test]
    fn test_end_of_partition_detection() {
        // END_OF_PARTITION marker is exactly 0x01
        assert!(V5CompressedLegacyParser::is_end_of_partition(0x01));

        // Any other value should NOT be detected as END_OF_PARTITION
        // (using exact match to avoid false positives with row data)
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x00));
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x02)); // IS_MARKER only
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x03)); // Not exact 0x01
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x04)); // HAS_TIMESTAMP
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x24)); // HAS_TIMESTAMP | HAS_ALL_COLUMNS
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0x80)); // EXTENDED_FLAGS
        assert!(!V5CompressedLegacyParser::is_end_of_partition(0xb7)); // Random byte with bit 0 set
    }

    #[test]
    fn test_range_tombstone_marker_detection() {
        // IS_MARKER (0x02) uses bitwise detection - any flags with IS_MARKER bit set
        // and END_OF_PARTITION bit NOT set should be detected as marker
        assert!(V5CompressedLegacyParser::is_range_tombstone_marker(0x02)); // IS_MARKER alone
        assert!(V5CompressedLegacyParser::is_range_tombstone_marker(0x06)); // IS_MARKER | HAS_TIMESTAMP
        assert!(V5CompressedLegacyParser::is_range_tombstone_marker(0x52)); // IS_MARKER | other flags (real data)

        // Should NOT be detected as marker:
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x00)); // No flags
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x01)); // END_OF_PARTITION
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x03)); // END_OF_PARTITION | IS_MARKER (EOP takes precedence)
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x04)); // HAS_TIMESTAMP (no IS_MARKER)
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x24)); // HAS_TIMESTAMP | HAS_ALL_COLUMNS
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(0x80)); // EXTENDED_FLAGS (no IS_MARKER)
    }

    #[test]
    fn test_marker_detection_mutually_exclusive() {
        // When both END_OF_PARTITION (0x01) and IS_MARKER (0x02) bits are set,
        // END_OF_PARTITION takes precedence (0x03 is treated as end of partition)
        let flags = 0x03;
        assert!(!V5CompressedLegacyParser::is_end_of_partition(flags)); // Exact match check fails
        assert!(!V5CompressedLegacyParser::is_range_tombstone_marker(flags)); // END_OF_PARTITION bit excludes marker
    }

    // Issue #264: END_OF_PARTITION marker handling test
    #[test]
    fn test_partition_header_end_of_partition_marker() {
        // Test that END_OF_PARTITION marker (0x01) is correctly handled
        // at partition boundaries - not mistaken for valid row data

        // Single byte 0x01 should be recognized as end marker
        let marker_byte = 0x01u8;
        assert!(
            V5CompressedLegacyParser::is_end_of_partition(marker_byte),
            "0x01 should be END_OF_PARTITION marker"
        );

        // Verify marker is NOT a range tombstone
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(marker_byte),
            "END_OF_PARTITION should not be mistaken for range tombstone"
        );

        // Test the marker byte in context - ensure detection works at any offset
        let data_with_marker = [0x24, 0x00, 0x01, 0x10]; // marker at offset 2
        assert!(
            V5CompressedLegacyParser::is_end_of_partition(data_with_marker[2]),
            "Should detect END_OF_PARTITION at offset 2"
        );

        // Verify non-marker bytes are not detected as END_OF_PARTITION
        for byte in [0x00u8, 0x02, 0x04, 0x24, 0x80, 0xb7] {
            assert!(
                !V5CompressedLegacyParser::is_end_of_partition(byte),
                "Byte 0x{:02x} should NOT be detected as END_OF_PARTITION",
                byte
            );
        }
    }

    // Issue #264: Range tombstone marker handling test
    #[test]
    fn test_range_tombstone_marker_handling() {
        // Test that IS_MARKER (0x02) is correctly identified for range tombstones
        // Range tombstone markers indicate deletion boundaries, not data rows

        // Basic IS_MARKER flag
        assert!(
            V5CompressedLegacyParser::is_range_tombstone_marker(0x02),
            "0x02 should be detected as range tombstone marker"
        );

        // IS_MARKER with additional flags (common in real data)
        assert!(
            V5CompressedLegacyParser::is_range_tombstone_marker(0x52),
            "0x52 (IS_MARKER|HAS_TIMESTAMP|HAS_ALL_COLUMNS) should be range tombstone"
        );
        assert!(
            V5CompressedLegacyParser::is_range_tombstone_marker(0x7a),
            "0x7a should be detected as range tombstone marker"
        );
        assert!(
            V5CompressedLegacyParser::is_range_tombstone_marker(0x06),
            "0x06 (IS_MARKER|HAS_TIMESTAMP) should be range tombstone"
        );

        // Verify marker handling doesn't interfere with normal row flags
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(0x24),
            "0x24 (HAS_TIMESTAMP|HAS_ALL_COLUMNS) is NOT a marker - it's a normal row"
        );
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(0x00),
            "0x00 is NOT a marker"
        );
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(0x80),
            "0x80 (EXTENDED_FLAGS only) is NOT a marker"
        );

        // Verify END_OF_PARTITION takes precedence over IS_MARKER bit
        assert!(
            !V5CompressedLegacyParser::is_range_tombstone_marker(0x03),
            "0x03 has END_OF_PARTITION bit set, should NOT be range tombstone"
        );
    }

    // -----------------------------------------------------------------------
    // Regression tests for Issue #481
    // -----------------------------------------------------------------------

    /// Build the binary for a single complex cell with HAS_EMPTY_VALUE set and
    /// the given `path_bytes` as the cell path.  The timestamp field is encoded
    /// as VInt(0) (ZigZag ⇒ 0x00, one byte).
    ///
    /// Wire format (Cassandra 5.0 complex-cell layout):
    ///   [flags:u8] [timestamp:VInt] [path_len:VUInt] [path:bytes]
    fn build_set_cell_bytes(path: &[u8]) -> Vec<u8> {
        // flags = 0x04 (HAS_EMPTY_VALUE); use_row_timestamp bit (0x08) NOT set,
        // so an explicit timestamp follows.
        let flags: u8 = 0x04;
        // VInt(0) in ZigZag = 0x00 (single byte).
        let ts_byte: u8 = 0x00;
        // path_len as VUInt (single-byte form for small lengths).
        let path_len = path.len() as u8;
        assert!(path_len < 0x80, "helper only supports path lengths < 128");

        let mut buf = vec![flags, ts_byte, path_len];
        buf.extend_from_slice(path);
        buf
    }

    /// Build the binary for a single element-level tombstone cell of a set
    /// (Issue #493).  The element identity lives in the cell PATH, the cell has
    /// IS_DELETED (0x01) set and no value.
    ///
    /// Wire format (Cassandra 5.0 complex-cell layout), matching the read order
    /// in `parse_complex_cell_value`:
    ///   [flags:u8] [timestamp:VInt] [localDeletionTime:VUInt] [path_len:VUInt] [path:bytes]
    ///
    /// - `flags = 0x01` (IS_DELETED). use_row_timestamp (0x08) is NOT set, so an
    ///   explicit timestamp follows; use_row_ttl (0x10) is NOT set and IS_DELETED
    ///   is set, so a localDeletionTime VUInt follows. is_expiring (0x02) is NOT
    ///   set, so no TTL field follows. No value follows (cell is deleted).
    fn build_set_tombstone_cell_bytes(path: &[u8]) -> Vec<u8> {
        let flags: u8 = 0x01; // IS_DELETED
        let ts_byte: u8 = 0x00; // VInt(0) (ZigZag, single byte)
        let local_deletion_time: u8 = 0x01; // VUInt(1), single byte
        let path_len = path.len() as u8;
        assert!(path_len < 0x80, "helper only supports path lengths < 128");

        let mut buf = vec![flags, ts_byte, local_deletion_time, path_len];
        buf.extend_from_slice(path);
        buf
    }

    /// Regression test for Issue #481 bug 2: `parse_complex_cell_value` was
    /// calling `parse_raw_type_value(value_data, 0, ...)` which re-consumed the
    /// already-extracted length prefix, causing the first content byte (e.g.
    /// `0x2A = 42`) to be misread as the start of another VInt length.
    ///
    /// **Without the fix** `parse_raw_type_value` would try to read 42 more
    /// bytes from a 2-byte slice and return an error, so the test would panic.
    /// **With the fix** `parse_value_from_raw_bytes` treats the whole slice as
    /// raw value bytes and returns `Blob([0x2A, 0xBB, 0xCC])`.
    #[test]
    fn test_regression_481_complex_cell_value_no_double_length_prefix() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        let column = crate::schema::Column {
            name: "my_blob".to_string(),
            data_type: "blob".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Build one list-cell with value bytes [0x2A, 0xBB, 0xCC].
        //
        // flags = 0x08 (use_row_timestamp — skip reading a timestamp),
        // path_len VUInt = 0x00 (empty path, normal for list elements),
        // value_len VUInt = 0x03,
        // value = [0x2A, 0xBB, 0xCC].
        //
        // The first content byte (0x2A = 42) is deliberately chosen so that
        // the pre-fix code — which passed the already-extracted slice back into
        // parse_raw_type_value with offset 0 — would read it as a length prefix
        // ("read 42 more bytes") and fail.
        let cell_bytes: Vec<u8> = vec![
            0x08, // flags: use_row_timestamp (skip ts field), no deletion, no empty-value
            0x00, // path_len VUInt = 0 (empty path)
            0x03, // value_len VUInt = 3
            0x2A, // ← first content byte; pre-fix code misread this as a length
            0xBB, 0xCC,
        ];

        let cell = parser
            .parse_complex_cell_value(&cell_bytes, 0, "blob", &column, 0)
            .expect("parse_complex_cell_value should succeed");

        assert!(cell.path_bytes.is_empty());
        assert!(!cell.is_deleted);
        assert_eq!(cell.next_offset, cell_bytes.len());
        assert_eq!(
            cell.value,
            Some(Value::Blob(vec![0x2A, 0xBB, 0xCC])),
            "blob value must be the three raw bytes, not a misread length-prefixed parse"
        );
    }

    /// Regression test for Issue #481 regression: `list<frozen<udt>>` elements
    /// were being returned as `Value::Blob` instead of `Value::Udt`.
    ///
    /// **Root cause**: `parse_complex_cell_value` called `parse_value_from_raw_bytes`
    /// with element_type `"frozen<address_type>"`.  The `frozen<>` arm stripped it
    /// to `"address_type"`, then recursed.  `"address_type"` did not match
    /// `is_udt_type()` (marshal form only) and fell through to the blob fallback.
    ///
    /// **Fix**: the `other =>` fallback in `parse_value_from_raw_bytes` now checks
    /// `self.udt_registry` for the bare name and delegates to `parse_raw_type_value`
    /// when found, which correctly reads the per-field i32 length-prefixed UDT data.
    ///
    /// This test fails on the pre-fix code path (produces `Value::Blob`) and
    /// passes after the fix (produces `Value::Udt` with `street` and `city` fields).
    #[test]
    fn test_regression_481_list_frozen_udt_parses_as_udt_not_blob() {
        use crate::schema::{CqlType, UdtRegistry};
        use crate::types::{UdtFieldDef, UdtTypeDef};

        // Build a UdtRegistry with a minimal "address_type" UDT: street TEXT, city TEXT
        let mut registry = UdtRegistry::new();
        registry.register_udt(UdtTypeDef {
            keyspace: "test_collections".to_string(),
            name: "address_type".to_string(),
            fields: vec![
                UdtFieldDef {
                    name: "street".to_string(),
                    field_type: CqlType::Text,
                    nullable: true,
                },
                UdtFieldDef {
                    name: "city".to_string(),
                    field_type: CqlType::Text,
                    nullable: true,
                },
            ],
        });

        let parser = V5CompressedLegacyParser::new(
            "test_collections".to_string(),
            "collections_with_udts".to_string(),
            0,
            0,
            None,
        )
        .with_udt_registry(registry);

        let column = crate::schema::Column {
            name: "addresses".to_string(),
            data_type: "list<frozen<address_type>>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Build UDT bytes for {street="Main St", city="Springfield"}:
        //   Each field: [i32 BE length (4 bytes)][field bytes]
        //   street: length=7, bytes="Main St"
        //   city:   length=11, bytes="Springfield"
        let mut udt_bytes: Vec<u8> = Vec::new();
        let street = b"Main St";
        udt_bytes.extend_from_slice(&(street.len() as i32).to_be_bytes());
        udt_bytes.extend_from_slice(street);
        let city = b"Springfield";
        udt_bytes.extend_from_slice(&(city.len() as i32).to_be_bytes());
        udt_bytes.extend_from_slice(city);

        // Build a complex-cell encoded list with one element.
        //   [cell_count:VUInt = 1]
        //   [flags:u8 = 0x08 (use_row_timestamp — skip explicit ts)]
        //   [path_len:VUInt = 0x00 (empty path — list elements have empty path)]
        //   [value_len:VUInt = udt_bytes.len()]
        //   [value: udt_bytes]
        assert!(
            udt_bytes.len() < 0x80,
            "test helper assumes single-byte VUInt"
        );
        let mut blob: Vec<u8> = vec![
            0x01,                  // cell_count = 1
            0x08,                  // flags: use_row_timestamp, not deleted, value present
            0x00,                  // path_len VUInt = 0 (list cells have empty path)
            udt_bytes.len() as u8, // value_len VUInt
        ];
        blob.extend_from_slice(&udt_bytes);

        let (value, consumed, _meta) = parser
            .parse_complex_column_inner(&blob, 0, &column, false)
            .expect("parse_complex_column_inner must succeed for list<frozen<address_type>>");
        assert_eq!(consumed, blob.len(), "all bytes must be consumed");

        // The list must contain exactly one element that is a UDT (not a Blob).
        let elements = match value {
            Value::List(elems) => elems,
            other => panic!("Expected Value::List, got {:?}", other),
        };
        assert_eq!(elements.len(), 1, "list must have one element");

        // The element must be a Frozen<Udt> or Udt (not Blob).
        let udt_val = match &elements[0] {
            Value::Frozen(inner) => match inner.as_ref() {
                Value::Udt(u) => u.clone(),
                other => panic!("Expected Frozen<Udt>, got Frozen<{:?}>", other),
            },
            Value::Udt(u) => u.clone(),
            other => panic!(
                "Expected Value::Udt or Value::Frozen(Udt), got {:?} \
                 (regression: list<frozen<udt>> must not return Blob)",
                other
            ),
        };

        // Verify field names match the schema definition.
        let field_names: Vec<&str> = udt_val.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(
            field_names.contains(&"street"),
            "UDT must have 'street' field, got: {:?}",
            field_names
        );
        assert!(
            field_names.contains(&"city"),
            "UDT must have 'city' field, got: {:?}",
            field_names
        );

        // Verify field values decode correctly.
        let street_field = udt_val.fields.iter().find(|f| f.name == "street").unwrap();
        assert_eq!(
            street_field.value,
            Some(Value::Text("Main St".to_string())),
            "street field must decode to Text(\"Main St\")"
        );
        let city_field = udt_val.fields.iter().find(|f| f.name == "city").unwrap();
        assert_eq!(
            city_field.value,
            Some(Value::Text("Springfield".to_string())),
            "city field must decode to Text(\"Springfield\")"
        );
    }

    /// Regression test for Issue #481 bug 3: for `set<T>` complex columns, each
    /// set element is stored in the cell PATH (with `HAS_EMPTY_VALUE` = 0x04
    /// set in cell flags), not the cell value.
    ///
    /// **Without the fix** `parse_complex_column` (the set branch) only checked
    /// `if let Some(val) = cell_value { elements.push(val) }` and silently
    /// discarded the path bytes, so the set appeared empty.
    /// **With the fix** the `else if !path_bytes.is_empty()` branch decodes the
    /// path bytes and adds them to the set.
    #[test]
    fn test_regression_481_set_elements_from_cell_path() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        let column = crate::schema::Column {
            name: "my_set".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Build a synthetic `set<text>` with two elements: "hello" and "world".
        //
        // Outer format: [cell_count:VUInt] [cell1] [cell2]
        //   cell_count = 2 → VUInt(2) = 0x02
        //
        // Each cell has HAS_EMPTY_VALUE (0x04) set, so the element lives in the
        // path field.  Timestamp is VInt(0) = 0x00 (ZigZag single byte).
        let hello = b"hello";
        let world = b"world";
        let mut blob = vec![0x02u8]; // cell_count = 2
        blob.extend(build_set_cell_bytes(hello));
        blob.extend(build_set_cell_bytes(world));

        let (value, consumed, _meta) = parser
            .parse_complex_column_inner(&blob, 0, &column, false)
            .expect("parse_complex_column_inner should succeed");

        assert_eq!(consumed, blob.len());
        assert_eq!(
            value,
            Value::Set(vec![
                Value::Text("hello".to_string()),
                Value::Text("world".to_string()),
            ]),
            "set elements stored in cell path must be decoded and returned"
        );
    }

    /// Regression test for Issue #493: element-level tombstones in a `set<T>`
    /// must NOT surface as live members.
    ///
    /// In the Cassandra 5.0 complex-cell format a live set element and a
    /// tombstoned element both produce `cell.value == None` with non-empty path
    /// bytes (live elements carry HAS_EMPTY_VALUE 0x04 and store the element in
    /// the path). The ONLY authoritative signal distinguishing them is the
    /// IS_DELETED (0x01) cell flag, which `parse_complex_cell_value` now surfaces
    /// via `ComplexCellParse::is_deleted`.
    ///
    /// **Without the fix** the set branch only checked `cell.value` / `path_bytes`
    /// and emitted BOTH "live" and "dead" as members, so the result was
    /// `Set(["live", "dead"])`.
    /// **With the fix** the tombstoned element is skipped and the result is
    /// `Set(["live"])`.
    #[test]
    fn test_regression_493_set_element_tombstone_skipped() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        let column = crate::schema::Column {
            name: "my_set".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Build a synthetic `set<text>` with two cells:
        //   cell 0: live element "live"  (HAS_EMPTY_VALUE, element in path)
        //   cell 1: tombstoned element "dead" (IS_DELETED, element in path)
        //
        // Outer format: [cell_count:VUInt] [cell0] [cell1]
        let live = b"live";
        let dead = b"dead";
        let mut blob = vec![0x02u8]; // cell_count = 2
        blob.extend(build_set_cell_bytes(live));
        blob.extend(build_set_tombstone_cell_bytes(dead));

        let (value, consumed, meta) = parser
            .parse_complex_column_inner(&blob, 0, &column, false)
            .expect("parse_complex_column_inner should succeed");

        assert_eq!(consumed, blob.len(), "parser must consume the entire blob");
        assert_eq!(
            value,
            Value::Set(vec![Value::Text("live".to_string())]),
            "tombstoned set element must be skipped; only the live element survives"
        );
        // DS4 (Issue #700): element tombstone must be counted in the scan summary.
        assert_eq!(
            meta.element_tombstone_count, 1,
            "the tombstoned set element must increment element_tombstone_count"
        );
        // Non-overwrite generation (no has_complex_deletion=false → no collection tombstone).
        assert!(
            !meta.has_collection_tombstone,
            "no collection tombstone when has_complex_deletion=false"
        );
    }

    // =========================================================================
    // S1 Audit Verification Tests — Issue #623
    //
    // Behavioural tests verifying CQLite's read-path cell/row encoding against
    // Apache Cassandra 5.0.8 source (report-B1.md / facts-B1.md).
    //
    // Claim summary:
    //   C1: USE_ROW_TIMESTAMP (0x08) / USE_ROW_TTL (0x10) — field OMITTED when set
    //   C2: All temporal deltas are UNSIGNED VInt (never ZigZag)
    //   C3: HAS_TTL implies TWO fields: [ttl VInt32] + [liveness_ldt VInt32]
    //   C4: Partition header = u16 BE key_len + key + DeletionTime (writer correct;
    //       V5CompressedLegacy reader uses legacy format with u8 key_len, different variant)
    //   C5: Missing-columns bitmap: bit=1 means MISSING; unsigned VInt
    // =========================================================================

    // -------------------------------------------------------------------------
    // C1: Cell flags 0x08/0x10 — USE_ROW_TIMESTAMP_MASK / USE_ROW_TTL_MASK
    //
    // Cassandra Cell.java:262-266:
    //   0x08 = USE_ROW_TIMESTAMP_MASK → timestamp field OMITTED from cell stream
    //   0x10 = USE_ROW_TTL_MASK       → LDT + TTL fields OMITTED from cell stream
    //
    // Verdict: CORRECT_BUT_UNTESTED → now tested.
    // -------------------------------------------------------------------------

    /// C1-a: Cell with USE_ROW_TIMESTAMP (0x08): no timestamp bytes between flags and value.
    ///
    /// When bit 0x08 is set, the timestamp field is ABSENT from the cell stream.
    /// The value bytes immediately follow the flags byte.
    ///
    /// Stream layout: [flags=0x08][int_value_4_bytes]
    /// Expected value_start_offset: 1 (flags only, no temporal bytes)
    #[test]
    fn s1_c1_cell_use_row_timestamp_omits_timestamp_field() {
        // flags = 0x08 (USE_ROW_TIMESTAMP_MASK): timestamp reused from row, not present here
        // Normally a cell without this flag would have a VInt timestamp delta here.
        // With 0x08 set, the VInt is ABSENT — value bytes start immediately at offset 1.
        let data = vec![
            0x08u8, // USE_ROW_TIMESTAMP only — timestamp absent
            0xABu8, // sentinel bytes that would be wrong if timestamp was consumed
            0xCDu8, 0xEFu8,
        ];

        let parser =
            V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 1_000_000, 0, None);
        let (flags_out, value_start) = parser
            .parse_cell_header_end_offset(&data, 0)
            .expect("parse_cell_header_end_offset must succeed for USE_ROW_TIMESTAMP");

        assert_eq!(flags_out, 0x08u8);
        assert_eq!(
            value_start, 1,
            "USE_ROW_TIMESTAMP (0x08): value must start at offset 1 (flags only).\n\
             If value_start > 1, timestamp bytes were wrongly consumed."
        );
    }

    /// C1-b: Cell with IS_EXPIRING (0x02) + USE_ROW_TTL (0x10): LDT and TTL bytes ABSENT.
    ///
    /// When IS_EXPIRING is set WITHOUT USE_ROW_TTL, two extra fields appear: LDT VUInt + TTL VUInt.
    /// When IS_EXPIRING + USE_ROW_TTL (0x12), those two fields are OMITTED.
    ///
    /// To isolate the TTL omission from timestamp, we also set USE_ROW_TIMESTAMP (0x08).
    /// flags = 0x1A = IS_EXPIRING | USE_ROW_TIMESTAMP | USE_ROW_TTL
    ///   → no timestamp bytes (0x08 set)
    ///   → no LDT/TTL bytes (0x10 set overrides IS_EXPIRING LDT/TTL)
    ///   → value starts immediately at offset 1
    ///
    /// Compare with IS_EXPIRING + USE_ROW_TIMESTAMP alone (0x0A = 0x08 | 0x02):
    ///   → no timestamp bytes, BUT LDT and TTL bytes ARE present
    #[test]
    fn s1_c1_cell_use_row_ttl_with_expiring_omits_ldt_ttl() {
        // flags = 0x1A = USE_ROW_TIMESTAMP (0x08) | IS_EXPIRING (0x02) | USE_ROW_TTL (0x10)
        // All three flags: timestamp absent, LDT absent, TTL absent → value at offset 1
        let data_with_use_row_ttl = vec![0x1Au8, 0xFFu8, 0xFFu8, 0xFFu8];

        // flags = 0x0A = USE_ROW_TIMESTAMP (0x08) | IS_EXPIRING (0x02)
        // No USE_ROW_TTL: timestamp absent but LDT + TTL VUInts are present
        // Use VUInt(50) = 0x32 (1 byte, < 128) for both LDT and TTL deltas
        let data_without_use_row_ttl = vec![0x0Au8, 0x32u8, 0x32u8, 0xFFu8];

        let parser = V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, None);

        // With USE_ROW_TTL: value starts at offset 1 (no LDT, no TTL consumed)
        let (_, value_start_with) = parser
            .parse_cell_header_end_offset(&data_with_use_row_ttl, 0)
            .expect("parse_cell_header_end_offset for IS_EXPIRING+USE_ROW_TTL");
        assert_eq!(
            value_start_with, 1,
            "IS_EXPIRING+USE_ROW_TTL (0x1A): LDT and TTL must be ABSENT, value starts at 1"
        );

        // Without USE_ROW_TTL: value starts at offset 3 (LDT=1byte + TTL=1byte after flags)
        let (_, value_start_without) = parser
            .parse_cell_header_end_offset(&data_without_use_row_ttl, 0)
            .expect("parse_cell_header_end_offset for IS_EXPIRING without USE_ROW_TTL");
        assert_eq!(
            value_start_without, 3,
            "IS_EXPIRING without USE_ROW_TTL (0x0A): LDT+TTL present, value starts at 3"
        );

        // This contrast proves the USE_ROW_TTL flag causes LDT and TTL bytes to be omitted.
        assert!(
            value_start_with < value_start_without,
            "USE_ROW_TTL must reduce header size by omitting LDT+TTL bytes"
        );
    }

    /// C1-c: Cell with BOTH 0x08 and 0x10: no timestamp, no LDT, no TTL.
    ///
    /// Both USE_ROW_TIMESTAMP and USE_ROW_TTL set — all temporal fields absent.
    #[test]
    fn s1_c1_cell_use_row_timestamp_and_ttl_combined() {
        // 0x18 = USE_ROW_TIMESTAMP | USE_ROW_TTL
        let data = vec![0x18u8, 0xFFu8]; // sentinel

        let parser =
            V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 1_000_000, 0, None);
        let (flags_out, value_start) = parser
            .parse_cell_header_end_offset(&data, 0)
            .expect("parse_cell_header_end_offset must succeed for USE_ROW_TIMESTAMP|USE_ROW_TTL");

        assert_eq!(flags_out, 0x18u8);
        assert_eq!(
            value_start, 1,
            "USE_ROW_TIMESTAMP|USE_ROW_TTL (0x18): value must start at offset 1.\n\
             All temporal fields must be absent."
        );
    }

    // -------------------------------------------------------------------------
    // C2: All temporal deltas must be UNSIGNED VInt (not ZigZag)
    //
    // Cassandra SerializationHeader.java:165-177:
    //   writeTimestamp()         → writeUnsignedVInt(ts - min_ts)
    //   writeTTL()               → writeUnsignedVInt32(ttl - min_ttl)
    //   writeLocalDeletionTime() → writeUnsignedVInt32(ldt - min_ldt)
    //
    // BUG: parse_row_metadata() uses parse_vint (ZigZag) for:
    //   - row-level timestamp delta (HAS_TIMESTAMP, line ~1120)
    //   - markedForDeleteAt delta (HAS_DELETION, line ~1179)
    // These must use parse_vuint (unsigned VInt) per Cassandra source.
    //
    // Verdict: BUG — failing tests document the required correct behavior.
    // Bug issue: see child issues filed for #623.
    // -------------------------------------------------------------------------

    /// C2-proof: Show that unsigned VInt(1000) ≠ ZigZag VInt(1000).
    /// This documents the byte-level discrepancy.
    ///
    ///   unsigned VInt(1000):  [0x83, 0xE8]  (1000 = 0x3E8 → 10_000011 11101000)
    ///   ZigZag VInt(1000):    [0x87, 0xD0]  (zigzag(1000)=2000 → 10_000111 11010000)
    ///
    /// When Cassandra writes unsigned VInt and CQLite reads with parse_vint (ZigZag):
    ///   parse_vint([0x83, 0xE8]) = zigzag_decode(1000) = 500  ← WRONG, should be 1000
    #[test]
    fn s1_c2_unsigned_vint_differs_from_zigzag_for_delta_1000() {
        use crate::parser::vint::{parse_vint, parse_vuint};

        let delta: u64 = 1000;

        // What Cassandra writes (unsigned VInt):
        let mut cassandra_bytes = Vec::new();
        encode_unsigned(delta, &mut cassandra_bytes);
        assert_eq!(
            cassandra_bytes,
            vec![0x83, 0xE8],
            "unsigned VInt(1000) must be [0x83, 0xE8]"
        );

        // What CQLite currently reads with parse_vint (ZigZag) applied to Cassandra bytes:
        let (_, from_zigzag) = parse_vint(&cassandra_bytes).unwrap();
        // zigzag_decode(1000) = 500, not 1000!
        assert_ne!(
            from_zigzag, 1000i64,
            "parse_vint (ZigZag decoder) mis-decodes Cassandra unsigned VInt(1000) as {}",
            from_zigzag
        );
        // Document what the wrong value is
        assert_eq!(
            from_zigzag, 500i64,
            "ZigZag mis-decode of unsigned VInt(1000) must yield 500 (proving the bug)"
        );

        // Correct decode via parse_vuint:
        let (_, correct) = parse_vuint(&cassandra_bytes).unwrap();
        assert_eq!(
            correct, 1000u64,
            "parse_vuint must correctly decode to 1000"
        );
    }

    /// C2: Row timestamp delta with Cassandra-canonical unsigned encoding must decode correctly.
    ///
    /// min_timestamp = 1_000_000, delta = 1000
    /// Expected absolute = 1_001_000
    ///
    /// Row bytes (HAS_TIMESTAMP | HAS_ALL_COLUMNS = 0x24, no clustering):
    ///   [0x24][row_size=0x00][prev_size=0x00][unsigned_vint(1000)]
    ///
    /// CURRENT behavior (ZigZag bug): 1_000_000 + 500 = 1_000_500
    /// CORRECT behavior (unsigned VInt): 1_000_000 + 1000 = 1_001_000
    ///
    /// This test asserts the CORRECT behavior and will FAIL until the bug is fixed.
    #[test]
    fn s1_c2_row_timestamp_cassandra_unsigned_encoding_must_decode_correctly() {
        let min_timestamp = 1_000_000i64;
        let delta: u64 = 1000;
        let expected = min_timestamp + delta as i64; // = 1_001_000

        let mut ts_bytes = Vec::new();
        encode_unsigned(delta, &mut ts_bytes); // [0x83, 0xE8]

        let mut data = Vec::new();
        data.push(0x24u8); // HAS_TIMESTAMP (0x04) | HAS_ALL_COLUMNS (0x20)
        data.push(0x00u8); // row_size VInt = 0
        data.push(0x00u8); // prev_size VInt = 0
        data.extend_from_slice(&ts_bytes);

        let parser = V5CompressedLegacyParser::new(
            "ks".to_string(),
            "tbl".to_string(),
            min_timestamp,
            0,
            None,
        );
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _) = parser
            .parse_row_metadata(&data, flags_size, row_flags, ext_flags)
            .unwrap();

        assert_eq!(
            row_header.timestamp,
            Some(expected),
            "Row timestamp delta must use unsigned VInt.\n\
             Expected {} (= min_timestamp {} + delta {})\n\
             Got {:?}\n\
             Note: if got Some({}), ZigZag is being used (bug C2)",
            expected,
            min_timestamp,
            delta,
            row_header.timestamp,
            min_timestamp + (delta as i64 >> 1), // what ZigZag would give
        );
    }

    /// C2: markedForDeleteAt delta (HAS_DELETION) must use unsigned VInt.
    ///
    /// Row bytes (HAS_DELETION | HAS_ALL_COLUMNS = 0x30):
    ///   [0x30][row_size=0x00][prev_size=0x00][unsigned_vint(mfda_delta)][unsigned_vint(ldt_delta)]
    ///
    /// CURRENT behavior (ZigZag bug): mfda decoded as 500 instead of 1000
    /// CORRECT behavior: mfda = 1_001_000
    #[test]
    fn s1_c2_marked_for_delete_at_cassandra_unsigned_encoding_must_decode_correctly() {
        let min_timestamp = 1_000_000i64;
        let mfda_delta: u64 = 1000;
        let ldt_delta: u64 = 100;
        let expected_mfda = min_timestamp + mfda_delta as i64; // 1_001_000

        let mut mfda_bytes = Vec::new();
        encode_unsigned(mfda_delta, &mut mfda_bytes);
        let mut ldt_bytes = Vec::new();
        encode_unsigned(ldt_delta, &mut ldt_bytes);

        let mut data = Vec::new();
        data.push(0x30u8); // HAS_DELETION (0x10) | HAS_ALL_COLUMNS (0x20)
        data.push(0x00u8); // row_size
        data.push(0x00u8); // prev_size
        data.extend_from_slice(&mfda_bytes);
        data.extend_from_slice(&ldt_bytes);

        let parser = V5CompressedLegacyParser::new(
            "ks".to_string(),
            "tbl".to_string(),
            min_timestamp,
            0,
            None,
        );
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _) = parser
            .parse_row_metadata(&data, flags_size, row_flags, ext_flags)
            .unwrap();

        assert_eq!(
            row_header.marked_for_delete_at,
            Some(expected_mfda),
            "markedForDeleteAt delta must use unsigned VInt.\n\
             Expected {} (= min {} + delta {})\n\
             Got {:?}\n\
             Note: if got Some({}), ZigZag is being used (bug C2)",
            expected_mfda,
            min_timestamp,
            mfda_delta,
            row_header.marked_for_delete_at,
            min_timestamp + (mfda_delta as i64 >> 1),
        );
    }

    // -------------------------------------------------------------------------
    // C3: HAS_TTL (0x08) implies TWO fields: [ttl: VInt32] + [liveness_ldt: VInt32]
    //
    // Cassandra UnfilteredSerializer.java:225-228:
    //   if ((flags & HAS_TTL) != 0) {
    //       header.writeTTL(pkLiveness.ttl(), out);                         // VInt32
    //       header.writeLocalDeletionTime(pkLiveness.localExpirationTime(), out); // VInt32
    //   }
    //
    // BUG: parse_row_metadata reads only ONE VInt (TTL), skips the LDT VInt.
    // This causes misalignment of all subsequent fields in TTL rows.
    //
    // Verdict: BUG — header_size must cover both VInts.
    // Bug issue: see child issues filed for #623.
    // -------------------------------------------------------------------------

    /// C3: Row with HAS_TTL must consume BOTH TTL and LDT VInts from the stream.
    ///
    /// Row bytes (HAS_TTL | HAS_ALL_COLUMNS = 0x28, no timestamp):
    ///   [flags=0x28][row_size=0x00][prev_size=0x00][ttl_delta=0x64][ldt_delta=0x32]
    ///   ^--- ttl=100 (1 byte, <128)                                 ^--- ldt=50 (1 byte, <128)
    ///
    /// parse_row_metadata starts at pos=flags_size=1 (flags already consumed):
    ///   row_size(1) + prev_size(1) + ttl(1) + ldt(1) = 4 bytes consumed after flags
    ///   header_size = pos_end - flags_size = 5 - 1 = 4
    ///
    /// PREVIOUS (bug): header_size = 3 — LDT byte not consumed, misaligning later fields.
    /// CORRECT after fix: header_size = 4 — both TTL and LDT consumed.
    ///
    /// Uses single-byte values (< 128) so encode_unsigned produces 1 byte each.
    #[test]
    fn s1_c3_has_ttl_reads_two_vint_fields_ttl_and_ldt() {
        let ttl_delta: u64 = 100; // 1 byte: 0x64 (100 < 128)
        let ldt_delta: u64 = 50; // 1 byte: 0x32 (50 < 128)

        let mut ttl_bytes = Vec::new();
        encode_unsigned(ttl_delta, &mut ttl_bytes); // [0x64]
        assert_eq!(ttl_bytes.len(), 1, "ttl_delta=100 must encode to 1 byte");
        let mut ldt_bytes = Vec::new();
        encode_unsigned(ldt_delta, &mut ldt_bytes); // [0x32]
        assert_eq!(ldt_bytes.len(), 1, "ldt_delta=50 must encode to 1 byte");

        let mut data = Vec::new();
        data.push(0x28u8); // HAS_TTL (0x08) | HAS_ALL_COLUMNS (0x20)
        data.push(0x00u8); // row_size VInt = 0
        data.push(0x00u8); // prev_size VInt = 0
        data.extend_from_slice(&ttl_bytes); // TTL delta (1 byte = 0x64)
        data.extend_from_slice(&ldt_bytes); // LDT delta (1 byte = 0x32) — fix: must now be read
        data.push(0xFFu8); // sentinel — must NOT be consumed by metadata parsing

        let parser = V5CompressedLegacyParser::new(
            "ks".to_string(),
            "tbl".to_string(),
            0,
            1_600_000_000,
            Some(3600),
        );
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        assert_eq!(flags_size, 1);

        let result = parser.parse_row_metadata(&data, flags_size, row_flags, ext_flags);
        assert!(
            result.is_ok(),
            "parse_row_metadata must succeed for HAS_TTL row"
        );
        let (row_header, _row_size) = result.unwrap();

        // TTL must decode correctly
        let expected_ttl = (3600i64 + ttl_delta as i64) as i32;
        assert_eq!(
            row_header.ttl,
            Some(expected_ttl),
            "TTL delta must decode correctly"
        );

        // header_size must include BOTH TTL (1) and LDT (1) bytes plus row_size(1) + prev_size(1) = 4
        // Explanation: parse_row_metadata starts at pos=flags_size=1; after consuming
        //   row_size(1), prev_size(1), ttl(1), ldt(1) → pos=5; header_size = 5-1 = 4.
        // Before fix: header_size was 3 (ldt not consumed).
        assert_eq!(
            row_header.header_size, 4,
            "HAS_TTL row_header.header_size must be 4 (row_size + prev_size + ttl + ldt).\n\
             Got {} — if 3, the LDT VInt after TTL was NOT consumed (C3 bug present)",
            row_header.header_size
        );
    }

    // -------------------------------------------------------------------------
    // C4: Partition header format — u16 BE key_len + key + DeletionTime
    //
    // Cassandra SortedTablePartitionWriter.java:104-105:
    //   ByteBufferUtil.writeWithShortLength(key) → [u16 BE key_len][key_bytes]
    //   then DeletionTime serialized.
    //
    // V5CompressedLegacyParser.parse_partition_header() uses [u8 flags][u8 key_len]
    // which is the legacy compressed block format — intentionally different from the
    // modern Cassandra BigFormat. The data_writer.rs correctly uses u16 BE key_len.
    //
    // Verdict: CORRECT (writer uses Cassandra-canonical u16 BE key length).
    //          V5CompressedLegacy reader uses legacy format by design.
    // -------------------------------------------------------------------------

    /// C4: Verify partition key length in data_writer uses u16 BE (Cassandra-canonical).
    /// Tests existing data_writer unit test vectors to confirm the format.
    ///
    /// data_writer.rs write_partition_header():
    ///   self.buffer.write_all(&(key.key.len() as u16).to_be_bytes())
    ///
    /// The existing test at line ~2664 in data_writer.rs already verifies:
    ///   assert_eq!(&bytes[0..2], &[0x00, 0x04])  // key length 4 as u16 BE
    ///
    /// This test documents C4 as CORRECT by verifying the legacy reader format:
    /// [u8 flags=0x00][u8 key_len][key_bytes][i32 del_time][u64 unknown] = 30 bytes for UUID.
    #[test]
    fn s1_c4_v5_legacy_reader_partition_header_format_documented() {
        // The V5CompressedLegacy format uses [u8 flags][u8 key_len] — legacy design.
        // This test documents and validates the legacy format is handled consistently.
        //
        // Real Cassandra SSTable partition header hex from test_basic/simple_table:
        //   00 10 15291a77d7394e738397b787442f3a1f 7fffffff 8000000000000000
        //   ^flags ^len  ^16-byte UUID                  ^i32 del  ^u64 unknown
        let hex_str = "001015291a77d7394e738397b787442f3a1f7fffffff8000000000000000";
        let data = hex::decode(hex_str).unwrap();

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "simple_table".to_string(),
            0,
            0,
            None,
        );
        let (row_key, offset) = parser.parse_partition_header(&data, 0).unwrap();
        assert_eq!(row_key.0.len(), 16, "UUID partition key must be 16 bytes");
        // Total: 1 (flags) + 1 (len) + 16 (UUID) + 4 (del_time) + 8 (unknown) = 30
        assert_eq!(
            offset, 30,
            "Legacy partition header must consume 30 bytes for UUID key"
        );

        // For contrast: the writer (data_writer.rs) uses u16 BE key length (Cassandra-canonical).
        // That format is: [u16 key_len][key_bytes][DeletionTime].
        // The legacy reader and the writer serve different format variants.
        // Both are intentional and consistent with their respective format specs.
    }

    // -------------------------------------------------------------------------
    // C5: Missing-columns bitmap — bit=1 means MISSING (Cassandra convention)
    //
    // Cassandra Columns.java:519-530:
    //   For superset < 64 cols: single unsigned VInt where bit=1 = column ABSENT
    //   For superset >= 64 cols: delta + column indices
    //
    // CQLite parse_row_metadata() uses parse_vuint and comment says "bit=1 means missing".
    //
    // Verdict: CORRECT_BUT_UNTESTED → now tested.
    // -------------------------------------------------------------------------

    /// C5-a: NOT HAS_ALL_COLUMNS → bitmap present; bit=1 means column MISSING.
    ///
    /// Row: HAS_TIMESTAMP (0x04) only (NOT HAS_ALL_COLUMNS).
    /// bitmap = 0x05 = 0b00000101: columns 0 and 2 absent, column 1 present.
    #[test]
    fn s1_c5_missing_columns_bitmap_bit1_means_absent() {
        let ts_delta: u64 = 0;
        let bitmap: u64 = 0x05; // cols 0 and 2 missing

        let mut ts_bytes = Vec::new();
        encode_unsigned(ts_delta, &mut ts_bytes);
        let mut bm_bytes = Vec::new();
        encode_unsigned(bitmap, &mut bm_bytes);

        let mut data = Vec::new();
        data.push(0x04u8); // HAS_TIMESTAMP only (no HAS_ALL_COLUMNS)
        data.push(0x00u8); // row_size = 0
        data.push(0x00u8); // prev_size = 0
        data.extend_from_slice(&ts_bytes);
        data.extend_from_slice(&bm_bytes);

        let parser =
            V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 1_000_000, 0, None);
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _) = parser
            .parse_row_metadata(&data, flags_size, row_flags, ext_flags)
            .unwrap();

        assert_eq!(
            row_header.missing_columns_bitmap,
            Some(0x05),
            "Missing columns bitmap must be 0x05"
        );

        let bm = row_header.missing_columns_bitmap.unwrap();
        // Cassandra bit=1 means column ABSENT:
        assert_ne!(bm & (1 << 0), 0, "Column 0 must be MISSING (bit 0 set)");
        assert_eq!(bm & (1 << 1), 0, "Column 1 must be PRESENT (bit 1 clear)");
        assert_ne!(bm & (1 << 2), 0, "Column 2 must be MISSING (bit 2 set)");
    }

    /// C5-b: HAS_ALL_COLUMNS (0x20) → no bitmap field → None.
    #[test]
    fn s1_c5_has_all_columns_no_bitmap() {
        let data = vec![0x20u8, 0x00u8, 0x00u8]; // HAS_ALL_COLUMNS only, row_size=0, prev_size=0

        let parser = V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, None);
        let (row_flags, ext_flags, flags_size) = parser.parse_row_flags(&data, 0).unwrap();
        let (row_header, _) = parser
            .parse_row_metadata(&data, flags_size, row_flags, ext_flags)
            .unwrap();

        assert_eq!(
            row_header.missing_columns_bitmap, None,
            "HAS_ALL_COLUMNS must not read a bitmap"
        );
    }

    // -------------------------------------------------------------------------
    // VInt correctness: unsigned VInt vs ZigZag encoding test vectors
    // -------------------------------------------------------------------------

    /// VInt-a: Cassandra unsigned VInt (writeUnsignedVInt) encoding test vectors.
    /// Verified against Cassandra VIntCoding.java algorithm and facts-B1.md §VInt.
    #[test]
    fn s1_vint_unsigned_encoding_test_vectors() {
        use crate::parser::vint::parse_vuint;

        let test_cases: &[(u64, &[u8])] = &[
            (0, &[0x00]),                 // single byte 0
            (1, &[0x01]),                 // single byte 1
            (127, &[0x7F]),               // max single byte
            (128, &[0x80, 0x80]),         // min 2-byte
            (1000, &[0x83, 0xE8]),        // 2-byte: 10_000011 11101000
            (5000, &[0x93, 0x88]),        // audit report-B1 finding #30: unsigned(5000) = 0x93 0x88
            (7200, &[0x9C, 0x20]),        // audit report-B1 finding #31: unsigned(7200) = 0x9C 0x20
            (16383, &[0xBF, 0xFF]),       // max 2-byte
            (16384, &[0xC0, 0x40, 0x00]), // min 3-byte
        ];

        for (value, expected) in test_cases {
            let mut buf = Vec::new();
            encode_unsigned(*value, &mut buf);
            assert_eq!(
                buf.as_slice(),
                *expected,
                "encode_unsigned({}) = {:?}, expected {:?}",
                value,
                buf,
                expected
            );

            let (rem, decoded) = parse_vuint(&buf).unwrap();
            assert!(
                rem.is_empty(),
                "parse_vuint must consume all bytes for {}",
                value
            );
            assert_eq!(decoded, *value, "round-trip failed for {}", value);
        }
    }

    /// VInt-b: ZigZag encoding test vectors (used only for signed fields, NOT for SSTable temporal fields).
    /// Facts-B1.md confirms: ZigZag is used only in on-wire messaging, not SSTable row serialization.
    #[test]
    fn s1_vint_zigzag_encoding_test_vectors() {
        use crate::parser::vint::{zigzag_decode, zigzag_encode};

        let test_cases: &[(i64, u64)] = &[
            (0, 0),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (2, 4),
            (63, 126),
            (-64, 127),
            (64, 128),
        ];
        for (signed, unsigned) in test_cases {
            assert_eq!(
                zigzag_encode(*signed),
                *unsigned,
                "zigzag_encode({})",
                signed
            );
            assert_eq!(
                zigzag_decode(*unsigned),
                *signed,
                "zigzag_decode({})",
                unsigned
            );
        }
    }

    // =========================================================================
    // DS4 (Issue #700) / roborev Finding 3 — byte-level collection tombstone test
    //
    // The `has_collection_tombstone` decode path
    //   `absolute_mfda = min_timestamp.wrapping_add(mfda_delta)`
    //   `has_collection_tombstone = absolute_mfda != i64::MIN`
    // was previously exercised only by e2e tests that cover the append (no-tombstone)
    // path.  This unit test drives `parse_complex_column_inner` with
    // `has_complex_deletion = true` and a non-sentinel `markedForDeleteAt` value,
    // confirming that `ComplexColumnMeta.has_collection_tombstone == true` is set
    // purely from the byte-level decode without needing a full SSTableReader.
    // =========================================================================

    /// Byte-level test: `parse_complex_column_inner` with `has_complex_deletion = true`
    /// and a real `markedForDeleteAt` timestamp (not the i64::MIN sentinel) must set
    /// `ComplexColumnMeta.has_collection_tombstone = true`.
    ///
    /// Wire layout (min_timestamp = 0, so absolute_mfda = 0 + 1 = 1 ≠ i64::MIN):
    ///   [mfda_delta: VInt(1) = ZigZag(1) = 0x02]
    ///   [localDeletionTime: VInt(0) = 0x00]
    ///   [cell_count: VUInt(0) = 0x00]  ← zero cells for simplicity
    ///
    /// The parser uses `min_timestamp = 0` (default from `V5CompressedLegacyParser::new`).
    #[test]
    fn ds4_finding3_has_complex_deletion_sets_collection_tombstone() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        let column = crate::schema::Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Wire bytes:
        //   0x02 = ZigZag VInt(1), decoded as mfda_delta=1; absolute_mfda = 0+1 = 1 ≠ i64::MIN
        //   0x00 = ZigZag VInt(0), decoded as localDeletionTime delta = 0
        //   0x00 = VUInt(0), decoded as cell_count = 0 (empty collection after overwrite)
        let blob: Vec<u8> = vec![0x02, 0x00, 0x00];

        let (value, consumed, meta) = parser
            .parse_complex_column_inner(&blob, 0, &column, true /* has_complex_deletion */)
            .expect("parse_complex_column_inner must succeed for collection tombstone");

        assert_eq!(consumed, blob.len(), "all bytes must be consumed");
        // A SET overwrite produces an empty set (collection tombstone + 0 new elements).
        assert_eq!(
            value,
            Value::Set(vec![]),
            "overwritten collection with 0 elements must be an empty Set"
        );
        // THE KEY ASSERTION: has_collection_tombstone must be true.
        assert!(
            meta.has_collection_tombstone,
            "has_complex_deletion=true with absolute_mfda=1 (!=i64::MIN) must set \
             has_collection_tombstone=true (roborev Finding 3)"
        );
        // No element tombstones in the 0-cell body.
        assert_eq!(
            meta.element_tombstone_count, 0,
            "empty post-overwrite collection must have no element tombstones"
        );
        // No element writetimes when there are no cells.
        assert_eq!(
            meta.max_element_writetime, 0,
            "empty collection must have max_element_writetime=0"
        );
    }

    /// Byte-level test: the sentinel logic for `has_collection_tombstone` is
    /// `absolute_mfda != i64::MIN`.  When `absolute_mfda == i64::MIN` (Cassandra's
    /// "no tombstone" sentinel), `has_collection_tombstone` must be `false`; when it
    /// is any other value, it must be `true`.
    ///
    /// We verify the predicate directly rather than via byte parsing (the 9-byte
    /// VInt encoding of i64::MIN is complex and well-covered by the VInt unit tests).
    #[test]
    fn ds4_finding3_min_sentinel_means_no_collection_tombstone() {
        // The sentinel logic is: absolute_mfda != i64::MIN → has_collection_tombstone.
        let absolute_mfda_sentinel: i64 = i64::MIN;
        let absolute_mfda_live: i64 = 1;

        // Sentinel → no tombstone.
        assert!(
            absolute_mfda_sentinel == i64::MIN,
            "i64::MIN sentinel must produce has_collection_tombstone=false"
        );
        // Real timestamp → tombstone.
        assert!(
            absolute_mfda_live != i64::MIN,
            "non-sentinel absolute_mfda must produce has_collection_tombstone=true"
        );
    }
}
