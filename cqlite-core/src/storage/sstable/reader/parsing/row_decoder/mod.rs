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
use std::sync::Arc;

use tracing::{debug, warn};

use crate::{
    parser::vint::{parse_vint, parse_vuint},
    schema::{CqlType, TableSchema, UdtRegistry},
    storage::sstable::version_gate::{BigVersionGates, VersionGates},
    types::{
        CellExpiration, CellWriteMetadata, TableId, TombstoneInfo, TombstoneType, UdtField,
        UdtTypeDef, UdtValue,
    },
    Error, Result, RowCells, RowKey, ScanRow, Value,
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
    RowCells,
    Option<HashMap<String, CellWriteMetadata>>,
    Option<RowHeader>,
    usize,
    bool,
    Option<HashMap<String, ComplexColumnMeta>>,
);

/// Return type for [`V5CompressedLegacy::parse_block_with_cell_metadata`].
///
/// Each element is `(table_id, row_key, value_map, cell_metadata_map)`.
type ParsedBlockWithMeta = Vec<(TableId, RowKey, ScanRow, HashMap<String, CellWriteMetadata>)>;

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
    /// The column's emit name, interned ONCE per block as a shared `Arc<str>`
    /// (issue #1334). Populating a decoded cell with its name is then an
    /// `Arc::clone` refcount bump instead of a per-cell, per-row heap `String`
    /// allocation. For an emitted column this is the supplied-schema column
    /// name; for a DROPPED column (never emitted) it mirrors the on-disk header
    /// name and is unused.
    pub(super) name: Arc<str>,
    /// Precomputed value-decode dispatch (Epic J / issue #1635), resolved ONCE per
    /// block from the AUTHORITATIVE declared type (the supplied-schema type when
    /// matched, else the on-disk header marshal type for a DROPPED column — exactly
    /// the type `parse_cell_value_schema_order` decoded against per cell before J1).
    /// The per-cell decode `match`es on this instead of re-lowercasing + string-
    /// matching every cell (no per-cell `to_lowercase`, no per-cell allocation).
    pub(super) kind: CellKind,
    /// Precomputed complex-ness (Epic J / issue #1635): `true` for a non-frozen
    /// collection / multicell UDT (routed to `parse_complex_column`), `false` for a
    /// single-cell scalar / frozen value. Resolved ONCE per block via
    /// `is_complex_column` on the authoritative complex-ness type (on-disk header
    /// marshal type preferred, supplied-schema type on the header-empty fallback) —
    /// the row body reads this instead of calling `is_complex_column` per row.
    pub(super) is_complex: bool,
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
pub(in crate::storage::sstable::reader) struct RowColumnResolution<'a> {
    /// On-disk regular (non-static) columns in serialization-header order.
    regular: Vec<ColumnToParse<'a>>,
    /// On-disk static columns in serialization-header order.
    static_: Vec<ColumnToParse<'a>>,
    /// Clustering-key names interned once per block as shared `Arc<str>` handles
    /// (issue #1334), positionally aligned with `schema.clustering_keys`. Reused
    /// so the per-row clustering-key cell insert is an `Arc::clone`, not a
    /// `String` allocation.
    clustering: Vec<Arc<str>>,
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
    pub(in crate::storage::sstable::reader) fn build(
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
                    .map(|col_info| {
                        let schema = resolve_lookup.get(col_info.name.as_str()).copied();
                        // Intern the emit name ONCE per block (issue #1334): the
                        // supplied-schema column name when matched, else the
                        // on-disk header name for a DROPPED column (never emitted).
                        let name: Arc<str> = Arc::from(
                            schema
                                .map(|c| c.name.as_str())
                                .unwrap_or(col_info.name.as_str()),
                        );
                        // J1 (issue #1635): resolve dispatch ONCE per column here.
                        // `value_type` drives the scalar decode dispatch — the
                        // supplied-schema type when matched, else the on-disk header
                        // marshal type for a DROPPED column (exactly the type the
                        // per-cell path decoded against). `is_complex` uses the
                        // header marshal type (authoritative complex-ness source,
                        // carries `UserType(...)`).
                        let value_type = schema
                            .map(|c| c.data_type.as_str())
                            .unwrap_or(col_info.column_type.as_str());
                        ColumnToParse {
                            schema,
                            header_type: Some(col_info.column_type.as_str()),
                            name,
                            kind: CellKind::from_type(value_type),
                            is_complex: V5CompressedLegacyParser::is_complex_column(
                                col_info.column_type.as_str(),
                            ),
                        }
                    })
                    .collect()
            };

            RowColumnResolution {
                regular: build_for(false),
                static_: build_for(true),
                clustering: schema
                    .clustering_keys
                    .iter()
                    .map(|k| Arc::from(k.name.as_str()))
                    .collect(),
            }
        } else {
            // Fallback to schema order when header is empty (shouldn't happen for
            // real SSTables). Filter out partition/clustering keys (regular columns
            // only carry cell data) and split by row kind.
            tracing::warn!("V5CompressedLegacy: reader.header.columns is empty, falling back to schema order (may cause column misalignment)");
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
                        name: Arc::from(col.name.as_str()),
                        // J1 (issue #1635): header-empty fallback — the supplied
                        // schema type is the only authoritative source for both the
                        // scalar dispatch and complex-ness (matches the pre-J1
                        // `complex_type = header_type.unwrap_or(&column.data_type)`).
                        kind: CellKind::from_type(&col.data_type),
                        is_complex: V5CompressedLegacyParser::is_complex_column(&col.data_type),
                    })
                    .collect()
            };
            RowColumnResolution {
                regular: build_for(false),
                static_: build_for(true),
                clustering: schema
                    .clustering_keys
                    .iter()
                    .map(|k| Arc::from(k.name.as_str()))
                    .collect(),
            }
        }
    }

    /// The interned clustering-key name at position `i` (issue #1334), or `None`
    /// when out of range. Cloning the returned handle into the cells map is an
    /// `Arc::clone`, not a `String` allocation.
    pub(super) fn clustering_name(&self, i: usize) -> Option<&Arc<str>> {
        self.clustering.get(i)
    }

    /// Number of interned clustering-key name handles (issue #1642). Used to
    /// size the per-row cell `Vec` capacity hint: clustering-key cells are
    /// pushed FIRST, before the data columns, so a clustered table would
    /// otherwise reallocate once past the data-column-only hint.
    pub(super) fn clustering_len(&self) -> usize {
        self.clustering.len()
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

// Struct-size regression guard (issue #1616, Epic H/H3; see
// docs/reports/parser-performance-audit-2026-07-01.md §Epic H (finding H3)). `ParseStep`
// is returned once per partition by the bounded compaction-read driver on the
// scan hot path. Measured 16 bytes today (discriminant + inlined `usize`) on
// 64-bit targets. Update this pin DELIBERATELY, never silently: any change —
// growth or shrink — must be a reviewed edit here.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<ParseStep>() == 16);

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
    ///
    /// Stored as `i64` (not `i32`): on oa/da (`hasUIntDeletionTime`) a post-2038 expiry
    /// is decoded as an UNSIGNED 32-bit value in `[2^31, 2^32)`; keeping it `i64`
    /// preserves the large positive second count so the read-time TTL filter does not
    /// wrap it negative and hide a still-live row (#1741 F1). See the decode site in
    /// `row_framing.rs` for the reinterpretation.
    liveness_expires_at_seconds: Option<i64>,
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
    /// Issue #1741 read-side shadowing aggregate: max write timestamp (µs) across
    /// this row's decoded DATA cells (excludes partition/clustering pseudo-cells;
    /// a cell inheriting the row timestamp contributes the row liveness timestamp,
    /// and a non-frozen collection contributes its newest element ts). Includes
    /// shadow/TTL-dropped cells so a fully-reduced row is still recognised as
    /// shadowed, but NEVER a tombstone cell (#3094 — that rides as the timestamp-less
    /// `has_deleted_data_cell`). `None` when the row decoded no LIVE data cell.
    /// Combined with `timestamp` to decide whether a tombstone shadows the WHOLE row.
    max_data_cell_timestamp: Option<i64>,
    /// Issue #1741: max effective expiry (epoch seconds) across this row's expiring
    /// DATA cells (explicit per-cell TTL or a `USE_ROW_TTL` cell inheriting the
    /// row's expiry; includes expired cells). `None` when none is expiring.
    max_data_cell_expires_at: Option<i64>,
    /// Issue #1741: `true` when at least one decoded DATA cell is LIVE (not
    /// shadowed, not expired) and live-forever (no TTL). Keeps the row visible
    /// regardless of liveness expiry. A shadowed/expired cell never sets this.
    has_live_forever_data_cell: bool,
    /// Issue #3094: `true` when the row decoded at least one TOMBSTONE cell — a
    /// PRESENCE fact carrying NO timestamp (one could only RAISE the row maximum,
    /// i.e. UN-hide a row): see [`PartitionShadow::has_shadow_evidence`].
    has_deleted_data_cell: bool,
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
        Value::Tombstone(Box::new(TombstoneInfo {
            deletion_time,
            tombstone_type: TombstoneType::RowTombstone,
            // Carry the on-disk `localDeletionTime` (GC clock, seconds) so the
            // compaction merge→rewrite path can preserve it (#873). Absent for a
            // non-tombstone header, hence the `0` fallback.
            local_deletion_time: self.local_deletion_time.unwrap_or(0) as i64,
            ttl: None,
            range_start: None,
            range_end: None,
        }))
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

    /// Issue #2374/#2789: the primary-key (row-marker) liveness of this row for
    /// the READ path. `has_marker` = the row carried `HAS_TIMESTAMP` (an INSERT
    /// liveness marker, not a data-cell-only UPDATE); `expires_at_seconds` = the
    /// marker's TTL expiry when `HAS_TTL` was set, else `None` (live-forever).
    /// Carry-only for reads; the compaction write path never consults it.
    pub(crate) fn row_liveness(
        &self,
    ) -> crate::storage::sstable::reader::compaction_row::RowLiveness {
        crate::storage::sstable::reader::compaction_row::RowLiveness {
            has_marker: self.timestamp.is_some(),
            expires_at_seconds: self.liveness_expires_at_seconds,
            // Issue #2374/#2789: the authoritative marker write timestamp (µs)
            // from the row header — the last-write-wins key the cross-generation
            // fold uses. Never inferred (no-heuristics, #28).
            marker_timestamp: self.timestamp,
        }
    }

    /// Issue #1741: max LIVE write ts (µs) — liveness marker vs decoded live cells.
    fn max_write_timestamp(&self) -> i64 {
        let m = self.timestamp.unwrap_or(i64::MIN);
        self.max_data_cell_timestamp.map_or(m, |c| m.max(c))
    }

    /// Issue #1741: `true` when a deletion at `deleted_at_micros` shadows the WHOLE
    /// row — i.e. every piece of the row's data is older than (or equal to) the
    /// deletion. Follows Cassandra `DeletionTime.deletes(ts) = ts <= markedForDeleteAt`.
    ///
    /// Fail-safe (#28): a row with NO authoritative timestamp (the `i64::MIN`
    /// sentinel — no liveness, no decoded live cell) is NOT shadowed. #3094: a decoded
    /// cell TOMBSTONE defeats that fail-safe WITHOUT contributing a timestamp. Both
    /// rules live in [`PartitionShadow::has_shadow_evidence`].
    fn shadowed_by_deletion_at(&self, deleted_at_micros: i64) -> bool {
        let max_ts = self.max_write_timestamp();
        PartitionShadow::has_shadow_evidence(max_ts, self.has_deleted_data_cell)
            && max_ts <= deleted_at_micros
    }

    /// Issue #1741: read-time TTL expiry. `true` iff the row carries a TTL somewhere
    /// AND every piece of its data (primary-key liveness + cells) has expired at
    /// `now_secs`, matching what a Cassandra `SELECT` hides. Returns `false` for any
    /// row with no TTL at all (never touches the tombstone-free common case).
    fn row_liveness_expired(&self, now_secs: i64) -> bool {
        let has_ttl =
            self.liveness_expires_at_seconds.is_some() || self.max_data_cell_expires_at.is_some();
        if !has_ttl {
            return false;
        }
        // A live-forever data cell (written with no TTL) keeps the row visible.
        if self.has_live_forever_data_cell {
            return false;
        }
        // Primary-key liveness still live? Present iff HAS_TIMESTAMP; live-forever
        // when it carries no TTL expiry, otherwise live until its expiry passes.
        let liveness_live = self.timestamp.is_some()
            && self
                .liveness_expires_at_seconds
                .is_none_or(|s| s > now_secs);
        if liveness_live {
            return false;
        }
        // Any expiring data cell still live keeps the row visible.
        if self.max_data_cell_expires_at.is_some_and(|e| e > now_secs) {
            return false;
        }
        true
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
    /// Issue #1741 (Finding 3): whether the element carries the IS_EXPIRING
    /// (0x02) flag, i.e. it has a TTL (either an explicit per-element TTL, or an
    /// inherited row TTL via `USE_ROW_TTL`). Needed for read-time collection-TTL
    /// expiry: an expiring element is NOT live-forever, so an otherwise-expired
    /// row must not be kept alive merely because it carries such an element.
    is_expiring: bool,
}

/// Issue #1741 (per-element filtering): read-side per-element shadow/TTL filter
/// context threaded into the complex-column element loop. `Some` ONLY on the
/// user-facing SELECT read path (a covering partition/range deletion and/or the
/// read clock is active); `None` for every physical consumer (compaction /
/// delta-scan / unit tests), which stay byte-unchanged (the filter never drops an
/// element when it is `None`).
#[derive(Clone, Copy)]
pub(crate) struct ElementShadow {
    /// Covering deletion `markedForDeleteAt` (µs) for the row this collection
    /// belongs to (the partition tombstone folded with the open range tombstone
    /// when the row falls inside it), or `None` when nothing covers the row.
    pub cover: Option<i64>,
    /// Read clock (epoch seconds) for per-element TTL expiry.
    pub now: i64,
    /// Row-liveness write timestamp (µs) inherited by `USE_ROW_TIMESTAMP` elements
    /// (flag 0x08 — the element carries no own timestamp); `None` when the row
    /// carries no liveness marker, in which case such an element has no
    /// authoritative write ts and is NEVER shadowed (no-heuristics, issue #28).
    pub row_ts: Option<i64>,
    /// Row-liveness expiry (epoch seconds) inherited by `USE_ROW_TTL` elements
    /// (flag 0x10 — an expiring element that carries NO explicit per-element
    /// `localDeletionTime`). This is the SAME effective row expiry the scalar
    /// `USE_ROW_TTL` cell path computes (`liveness_expires_at_seconds`, falling
    /// back to the row `local_deletion_time`), so scalar and collection-element
    /// inherited-TTL semantics stay identical (issue #1741). `None` when the row
    /// carries no liveness expiry, in which case such an element has no
    /// authoritative expiry and is never TTL-expired here (no-heuristics, issue #28).
    pub row_expires_at: Option<i64>,
    /// Issue #2038 (round 3): row-liveness TTL in SECONDS — `row_header.ttl`,
    /// paired with `row_expires_at` above to resolve a `USE_ROW_TTL` element's
    /// EFFECTIVE `CellExpiration` for the per-cell-metadata `TTL()` value (a
    /// statement-level `INSERT ... USING TTL n` on a non-frozen collection/UDT
    /// column). `None` when the row carries no liveness TTL (`HAS_TTL` unset),
    /// in which case such an element's expiry cannot be resolved here
    /// (no-heuristics, issue #28) — mirrors the scalar `USE_ROW_TTL` cell
    /// path's `(row_header.ttl, row_expiry)` pairing (row_data.rs ~line 726).
    pub row_ttl_seconds: Option<i32>,
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
    /// Issue #1741 (Finding 3): read-time TTL aggregate over this collection's
    /// LIVE (non-deleted) elements. `true` when at least one live element carries
    /// no TTL of any kind (genuinely live-forever) — such an element keeps the
    /// row visible regardless of the row-liveness TTL. Computed from authoritative
    /// per-element cell flags (no heuristics), scalar-only (no extra allocation).
    pub has_live_forever_element: bool,
    /// Issue #1741 (Finding 3): max effective expiry (epoch seconds) across this
    /// collection's live elements that carry an EXPLICIT per-element TTL. `None`
    /// when no live element has an explicit expiry (elements that inherit the row
    /// TTL via `USE_ROW_TTL` are governed by the row-liveness expiry instead).
    /// Folded into the row's `max_data_cell_expires_at` so a collection whose
    /// elements are all expired does not keep an otherwise-expired row alive.
    pub max_element_expires_at: Option<i64>,
    /// Issue #2038 (roborev Medium finding): the `CellExpiration` surfaced in
    /// per-cell metadata for `TTL(non_frozen_collection/UDT)`, IFF every VISIBLE
    /// (post shadow/TTL-filter, non-tombstone) element shares the IDENTICAL
    /// explicit `(ttl_seconds, expires_at_seconds)` pair. Decoded from the
    /// authoritative per-element cell fields (no heuristics, #28) via the
    /// `ExpiryHomogeneity` tracker.
    ///
    /// Deliberately NARROWER than `max_element_expires_at` above: a dropped
    /// (shadow/TTL-filtered) element never contributes here (it does for
    /// `max_element_expires_at`, which drives the orthogonal #1741 row-hidden
    /// decision), and a MIXED collection (elements with different TTLs, or a
    /// live-forever element mixed with an expiring one) has no single TTL that
    /// describes it — this is `None` in that case, correctness over
    /// over-approximating with one element's expiry. This is the complex-cell
    /// analogue of the scalar #1743 fix, which surfaces a single-cell expiry
    /// unambiguously.
    pub visible_uniform_expiration: Option<CellExpiration>,
    /// Issue #1741 (per-element filtering): number of LIVE (non-tombstone)
    /// elements DROPPED from the emitted container by the read-side per-element
    /// shadow/TTL filter — i.e. elements shadowed by the covering deletion (own
    /// write ts `<= cover`) or TTL-expired at the read clock. Always `0` for every
    /// physical consumer (the filter is `None`), so their output is byte-unchanged.
    /// The read call site uses `> 0` (together with an emptied container) to tell a
    /// collection that the filter reduced to nothing (read as absent/null) apart
    /// from one that was genuinely empty / all element-tombstones (kept as-is).
    pub shadow_filtered_element_count: usize,
}

// Row header flag constants
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_COMPLEX_DELETION: u8 = 0x40; // Issue #221: Row contains complex column with deletion info
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;

// Issue #3095 / epic #1116: the row DISPLAY + static-merge helpers
// (`row_has_non_key_cell`, `merge_static_cells`, `build_display_row`,
// `extract_clustering_values`) live in `display_row`.
mod display_row;
use display_row::{
    build_display_row, build_display_row_read_path, extract_clustering_values, merge_static_cells,
    row_is_visible,
};
// campsite split of `block_emit_windowed` (epic #1116): the streaming-scan
// `SlidingPartitionPolicy`.
mod timestamp_policy;
use timestamp_policy::TimestampPolicy;

// Issue #1741 / #1853: `now_epoch_secs()` (the read-time TTL "now" clock, with
// its `CQLITE_TTL_NOW_OVERRIDE_SECS` test seam) lives in `now_clock` — split
// out to keep this module under the file-size ratchet (epic #1116).
use now_clock::now_epoch_secs;

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
    /// Issue #1741: apply read-side SELECT-semantic shadowing (hide partition/
    /// range-tombstone-shadowed and TTL-expired rows) in the emit paths. `true`
    /// ONLY for user-facing query reads (`scan`/`scan_stream`/`scan_with_cell_metadata`/
    /// point `get`). `false` (the default) for PHYSICAL consumers that must see every
    /// on-disk row — integrity verification (`get_all_entries` → `verify_sstable`),
    /// `sstable_data_manager`, delta-scan, and the compaction read path (which
    /// reconciles tombstones itself across generations). Un-gated: read correctness
    /// does not depend on the `write-support` feature (AC2).
    read_shadowing: bool,
    /// Issue #1741 (F2): the read-time TTL "now" clock (epoch seconds), captured ONCE
    /// when this parser (the user-facing scan/read context) is constructed and reused
    /// for EVERY shadowing decision across all blocks/partitions of the read. A single
    /// scan builds this parser once and drives many blocks through it, so sampling the
    /// clock here — rather than per block in the emit loop — gives one consistent `now`
    /// for the whole operation (a row exactly at an expiration-second boundary is then
    /// decided uniformly regardless of which block parsed it). Only consulted when
    /// `read_shadowing` is `true`; physical consumers ignore it and stay byte-unchanged.
    now_secs: i64,
}

mod block_emit;
mod block_emit_windowed;
// Issue #3782: the explicit buffer-extent contract the block-emit parses take.
mod buffer_extent;
pub(crate) use buffer_extent::BufferExtent;
mod cell_kind;
mod cell_value;
// campsite split of `cell_value` (issue #1795): scalar arms + complex ladder.
mod cell_value_complex;
mod cell_value_scalar;
mod compaction;
mod compaction_stream; // issue #2299 (split of `compaction`, campsite #1116)
pub(in crate::storage::sstable::reader) use compaction_stream::{
    CompactionPartitionState, PartitionStreamStep,
};
mod complex_column;
mod frozen;
mod marshal_element;
pub(crate) mod now_clock;
mod partition_driver;
pub(crate) mod partition_shadow;
mod raw_type_value;
mod raw_value;
mod row_data;
mod row_framing;
mod udt;

use partition_driver::{row_write_timestamp, MarkerOutcome, SlidingPartitionPolicy};
// Per-column decode dispatch tag (Epic J / issue #1635). Imported into this
// module's namespace so the `use super::*` sibling modules (`cell_value`,
// `row_data`) can name it via `super::*`.
use cell_kind::CellKind;
use partition_shadow::{clustering_reversed_flags, PartitionShadow};
// #1741: shared partition-header need-more classifier used by both sliding
// parsers (`block_emit_windowed` + `compaction`) via their `use super::*` glob.
use row_framing::PartitionHeaderReadiness;
// #1641 (K2): non-allocating partition-boundary peek result, used to reimplement
// `peek_is_partition_header` without a per-row header try-parse.
use row_framing::BoundaryPeek;

#[cfg(test)]
mod test_support;

// Issue #1617 (Epic H / finding H4): decoder + codec lockstep parity net.
// Pins equivalence between the v5 string-ladder and block/`ComparatorType`
// decoders (and the write side) so the J1/J2 consolidation refactors are safe.
#[cfg(test)]
pub(crate) mod decoder_lockstep_tests;

// Issue #1636 (Epic #1603, finding J2): decoder-consolidation equivalence net.
// Pins that the two live `ComparatorType` decoders share one structural body.
#[cfg(test)]
mod decoder_consolidation_tests;

#[cfg(test)]
mod regression_1741c_tests;

#[cfg(test)]
mod regression_1741d_tests;

#[cfg(all(test, feature = "write-support"))]
mod regression_1741h_tests;

#[cfg(test)]
mod regression_1741k_tests;

// Issue #1641 (Epic K, finding K2): drift guard for the non-allocating
// partition-boundary peek — `peek_partition_boundary == Header` ⟺ the old
// allocating semantics (`!marker && parse_partition_header_full.is_ok()`).
#[cfg(test)]
mod regression_1641_boundary_peek_tests;

// Issue #1795: per-cell VInt-length bounds guards reject adversarial lengths
// (return `Err`, never overflow-panic).
#[cfg(test)]
mod regression_1795_overflow_tests;

// Issue #2807: the DECODE surface for keyspace-qualified UDT type names — the
// registry-backed fallback must split `ks.udt` before the bare-keyed lookup, or
// the value silently degrades to `Blob`.
#[cfg(test)]
mod regression_2807_qualified_udt_decode_tests;

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
            read_shadowing: false,
            // Issue #1741 (F2): sample the read clock ONCE per parser (== once per
            // read/scan operation); every block/partition below reuses this value.
            now_secs: now_epoch_secs(),
        }
    }

    /// Issue #1741: enable read-side SELECT-semantic shadowing on this parser. Call
    /// with `true` ONLY when building the parser for a user-facing query read; leave
    /// the default (`false`) for physical/verification/compaction/delta reads.
    pub fn with_read_shadowing(mut self, on: bool) -> Self {
        self.read_shadowing = on;
        self
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

    /// Issue #1741 (Finding 2): `true` when this SSTable's authoritative
    /// EncodingStats prove it carries NO deletions of any kind — hence NO range
    /// tombstones. A clustering-slice read can then keep the O(slice) row-index
    /// fast-forward and skip prefix priming entirely (a range tombstone opening
    /// before the slice is impossible).
    ///
    /// `min_local_deletion_time` is `EncodingStats.minLocalDeletionTime`, the MIN
    /// of every cell's `localDeletionTime`. A live cell contributes the LIVE
    /// sentinel `Cell.NO_DELETION_TIME == Integer.MAX_VALUE`; a partition/row/
    /// range/cell tombstone OR an expiring cell contributes a smaller value. So
    /// the min equals `Integer.MAX_VALUE` iff the SSTable has no deletion and no
    /// TTL. This OVER-approximates range-tombstone presence (a cell tombstone or
    /// TTL also trips it), which is safe: priming then runs and stays correct. No
    /// stats (`min == 0` from the `build_v5_parser` fallback) conservatively primes.
    /// No heuristics — authoritative metadata only (issue #28).
    #[inline]
    fn sstable_may_have_range_tombstones(&self) -> bool {
        // Integer.MAX_VALUE — Cassandra `Cell.NO_DELETION_TIME` LIVE sentinel.
        const NO_DELETION_TIME: i64 = i32::MAX as i64;
        self.min_local_deletion_time != NO_DELETION_TIME
    }

    /// Whether the bytes at `offset` begin a new partition header, WITHOUT
    /// consuming them.
    ///
    /// This is the NO-HEURISTICS approach: we validate the actual structure
    /// instead of guessing from byte patterns. Issue #1641 (K2) made it
    /// non-allocating on the fast-reject paths — it delegates to
    /// [`peek_partition_boundary`], which shares the structural walk of
    /// `parse_partition_header_full` (via `scan_partition_header`) but always
    /// skips the success-path key `to_vec` and the `PARTITION_HEADER_TRY_PARSES`
    /// counter. The marker pre-check and readiness gate allocate nothing; the
    /// strict scan on a `Ready` buffer may still build a discarded error string
    /// on a structural mismatch. The boolean result is identical to the former
    /// allocating implementation (marker pre-check + full-parse `is_ok`), proved
    /// by the `peek_matches_full_parse` proptest.
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
    ///
    /// [`peek_partition_boundary`]: Self::peek_partition_boundary
    #[doc(hidden)]
    pub fn peek_is_partition_header(&self, data: &[u8], offset: usize) -> bool {
        matches!(
            self.peek_partition_boundary(data, offset),
            BoundaryPeek::Header
        )
    }
}
