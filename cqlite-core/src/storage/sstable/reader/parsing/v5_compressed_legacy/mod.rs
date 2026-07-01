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

/// One on-disk column to decode, in serialization-header order.
///
/// `schema` is the resolved supplied-schema `Column` (drives column identity /
/// emit name) or `None` for a DROPPED / evolved-away column present on disk but
/// absent from the supplied schema — its bytes are still consumed to keep the
/// trailing columns byte-aligned (issue #1080 Part 2), but no cell is emitted.
///
/// `header_type` is the AUTHORITATIVE on-disk SerializationHeader marshal type
/// (`ColumnInfo.column_type`), the no-heuristics source of truth (issue #28) used
/// to decide complex-ness and to decode complex values. It is `None` only on the
/// header-empty fallback path (synthetic SSTables) where the supplied schema type
/// is all we have.
///
/// Both fields borrow: `schema` from the per-call `TableSchema`, `header_type`
/// from `reader.header`. The owning [`RowColumnResolution`] therefore borrows
/// from both for `'a`.
pub(super) struct ColumnToParse<'a> {
    pub(super) schema: Option<&'a crate::schema::Column>,
    pub(super) header_type: Option<&'a str>,
}

/// Pre-resolved header→schema column ordering for a whole SSTable block.
///
/// Issue #1046 (the true hoist): the header→schema-column resolution is CONSTANT
/// for every row in an SSTable — the serialization header is fixed and the
/// supplied schema does not change between rows. Building it once per block and
/// reusing it across every partition/row means the per-row decode performs ZERO
/// schema-lookup allocations (no per-row `HashMap`, no per-row `String` clone, no
/// per-row `Vec` of columns). The expensive `O(header_cols × schema_cols)` name
/// resolution + the `Vec` allocations happen ONCE per block, not once per row.
///
/// Two orderings are precomputed because the on-disk column group differs by row
/// kind (Cassandra's `missing_columns_bitmap` covers only the static columns for
/// a static row and only the regular columns for a regular row — issue #702):
///   - `regular`: header columns with `is_static == false`
///   - `static_`: header columns with `is_static == true`
///
/// The per-row missing-columns bitmap filter is applied by iterating the chosen
/// slice and skipping bitmapped-out indices INLINE (no per-row `Vec`), so the
/// row body still allocates nothing for column resolution.
///
/// Lifetime: borrows the header-type strings from `reader.header` and the
/// `Column`s from the supplied `schema`; both outlive the block's row loops
/// (resolution is built at the top of each `parse_block_emit*` / per-partition
/// driver, where `reader` and `schema` are in scope). This is a per-BLOCK hoist —
/// allocations scale with block count, not row count.
pub(super) struct RowColumnResolution<'a> {
    /// On-disk regular (non-static) columns in serialization-header order.
    regular: Vec<ColumnToParse<'a>>,
    /// On-disk static columns in serialization-header order.
    static_: Vec<ColumnToParse<'a>>,
}

impl<'a> RowColumnResolution<'a> {
    /// Build the resolution ONCE for a block from the on-disk serialization
    /// header (`reader.header`) and the supplied `schema`.
    ///
    /// Resolution is an exact column-name match, keep-FIRST on a duplicate name
    /// (Cassandra schemas cannot have duplicate column names, so this is purely
    /// defensive and matches the prior per-row `HashMap`/`iter().find()`
    /// semantics). On the header-empty fallback path (synthetic SSTables) the
    /// supplied schema order is used directly, every column schema-present by
    /// construction.
    pub(super) fn build(
        schema: &'a TableSchema,
        reader: &'a crate::storage::sstable::reader::types::SSTableReader,
    ) -> Self {
        if !reader.header.columns.is_empty() {
            // O(header_cols × schema_cols) name resolution, but ONCE per block via
            // a borrowed-key map (keys are `&str` slices into `schema.columns[].name`,
            // no `String` clones; values are `&Column`). Both borrow for `'a`.
            let mut resolve_lookup: HashMap<&'a str, &'a crate::schema::Column> =
                HashMap::with_capacity(schema.columns.len());
            for col in &schema.columns {
                resolve_lookup.entry(col.name.as_str()).or_insert(col);
            }

            let build_for = |want_static: bool| -> Vec<ColumnToParse<'a>> {
                reader
                    .header
                    .columns
                    .iter()
                    .filter(|col_info| {
                        !col_info.is_primary_key
                            && !col_info.is_clustering
                            && col_info.is_static == want_static
                    })
                    .map(|col_info| ColumnToParse {
                        schema: resolve_lookup.get(col_info.name.as_str()).copied(),
                        header_type: Some(col_info.column_type.as_str()),
                    })
                    .collect()
            };

            RowColumnResolution {
                regular: build_for(false),
                static_: build_for(true),
            }
        } else {
            // Fallback to schema order when header is empty (shouldn't happen for
            // real SSTables). Filter out partition/clustering keys (regular columns
            // only carry cell data) and split by row kind.
            log::warn!("V5CompressedLegacy: reader.header.columns is empty, falling back to schema order (may cause column misalignment)");
            let partition_key_names: std::collections::HashSet<&str> = schema
                .partition_keys
                .iter()
                .map(|k| k.name.as_str())
                .collect();
            let clustering_key_names: std::collections::HashSet<&str> = schema
                .clustering_keys
                .iter()
                .map(|k| k.name.as_str())
                .collect();
            let build_for = |want_static: bool| -> Vec<ColumnToParse<'a>> {
                schema
                    .columns
                    .iter()
                    .filter(|col| {
                        !partition_key_names.contains(col.name.as_str())
                            && !clustering_key_names.contains(col.name.as_str())
                            && col.is_static == want_static
                    })
                    .map(|col| ColumnToParse {
                        schema: Some(col),
                        header_type: None,
                    })
                    .collect()
            };
            RowColumnResolution {
                regular: build_for(false),
                static_: build_for(true),
            }
        }
    }

    /// The pre-resolved on-disk column ordering for a row of the given kind.
    pub(super) fn columns_for(&self, is_static: bool) -> &[ColumnToParse<'a>] {
        if is_static {
            &self.static_
        } else {
            &self.regular
        }
    }
}

/// Per-column complex-element capture for the compaction read path (epic #899).
///
/// Maps a complex (non-frozen collection / UDT) column name to its optional
/// complex deletion `(markedForDeleteAt µs, localDeletionTime s)` plus the
/// per-element cells in on-disk order. Populated by `parse_row_data_with_offset`
/// only when the caller passes a `Some(&mut _)` collector (the compaction path);
/// `None` on every user-facing read path.
type CompactionComplexColumns = HashMap<
    String,
    (
        Option<(i64, i32)>,
        Vec<crate::storage::sstable::reader::compaction_row::ComplexElement>,
        // The whole-collection collapsed `Value` (epic #899 Phase A neutrality:
        // the merge OUTPUT path replays this byte-identically to pre-Phase-A).
        Value,
    ),
>;

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
    /// Only read by the delta-scan emit path (`parse_block_emit_delta`); allow it
    /// to be unused when that feature is off so non-delta builds compile under
    /// `-D warnings`.
    #[cfg_attr(not(feature = "delta-scan"), allow(dead_code))]
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
            // Carry the on-disk `localDeletionTime` (GC clock, seconds) so the
            // compaction merge→rewrite path can preserve it (#873). Absent for a
            // non-tombstone header, hence the `0` fallback.
            local_deletion_time: self.local_deletion_time.unwrap_or(0) as i64,
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
    /// Whether the cell carries the HAS_EMPTY_VALUE (0x04) flag (an on-disk
    /// empty-value cell — e.g. a SET member whose identity lives entirely in the
    /// cell_path). Surfaced so the compaction writer can reproduce the SAME
    /// on-disk emptiness byte-for-byte rather than re-deriving it from the
    /// decoded value (epic #899, Phase C: a SET element's decoded member is the
    /// path bytes, NOT a cell value).
    has_empty_value: bool,
    /// Offset immediately following the parsed cell.
    next_offset: usize,
    /// Per-element writetime decoded from the cell's own timestamp field, in µs since
    /// Unix epoch (absolute, after delta decoding from min_timestamp).  `None` when the
    /// element inherited the row-level timestamp (USE_ROW_TIMESTAMP flag 0x08).
    ///
    /// Used by `parse_complex_column_inner` to compute the max element writetime
    /// for a collection column (Issue #700, DS4).
    element_writetime: Option<i64>,
    /// Per-element TTL in seconds when the cell is expiring (`IS_EXPIRING`
    /// 0x02 set and not `USE_ROW_TTL`). `None` otherwise. Surfaced for the
    /// per-element compaction contract (epic #899).
    element_ttl: Option<u32>,
    /// Per-element `localDeletionTime` in SECONDS for an expiring / deleted
    /// element. Far-future values in `[2^31, 2^32)` are kept as the wrapping
    /// `as u32 as i32` representation (epic #899 invariant). `None` when the
    /// element carries no local deletion time.
    element_local_deletion_time: Option<i32>,
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
pub(crate) struct ComplexColumnMeta {
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
    /// Real complex deletion `(markedForDeleteAt µs, localDeletionTime s)` for
    /// this column, or `None` for the `LIVE` sentinel (epic #899). Used by the
    /// compaction read path to populate `ComplexColumn.complex_deletion`.
    /// Always `None` on the user-facing read path (where the field is unused).
    pub complex_deletion: Option<(i64, i32)>,
}

// Row header flag constants
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_COMPLEX_DELETION: u8 = 0x40; // Issue #221: Row contains complex column with deletion info
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;

/// Issue #932: does the decoded cell map hold any NON-primary-key data cell?
///
/// Primary-key (partition + clustering) columns are surfaced into the cell map
/// as pseudo-cells (#229) so the read-back path can recover the clustering
/// identity; they are NOT row data. A row carrying `HAS_DELETION` is a PURE row
/// tombstone only when no such data cell survives — otherwise the row deletion
/// COEXISTS with surviving (strictly-newer) cells and the row displays as live.
fn row_has_non_key_cell(cells: &HashMap<String, Value>, schema: &TableSchema) -> bool {
    cells.keys().any(|name| {
        !schema.partition_keys.iter().any(|k| &k.name == name)
            && !schema.clustering_keys.iter().any(|c| &c.name == name)
    })
}

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

mod block_emit;
mod block_emit_windowed;
mod cell_value;
mod compaction;
mod complex_column;
mod frozen;
mod raw_type_value;
mod raw_value;
mod row_data;
mod row_framing;
mod udt;

#[cfg(test)]
mod test_support;

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
}
