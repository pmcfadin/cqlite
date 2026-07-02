//! Per-element / per-cell compaction read contract (epic #899, Phase A).
//!
//! The compaction read path historically emitted `(RowKey, ScanRow, i64)` per row,
//! collapsing every non-frozen collection / UDT into a single nested [`Value`]
//! with one row-level timestamp. That representation cannot reconcile two
//! SSTables that wrote DISJOINT elements of the same multi-cell column (each
//! element has its own timestamp / ttl / local-deletion-time / cell-path), nor
//! can it carry a real per-column complex deletion (Cassandra's
//! `markedForDeleteAt` + `localDeletionTime` marker written ahead of a
//! multi-cell column's elements).
//!
//! [`CompactionRow`] replaces that tuple on the **compaction-only** read path. It
//! preserves the on-disk per-element granularity so the k-way merge can perform
//! byte-faithful per-`(column, cell_path)` reconciliation. The user-facing read
//! path (`scan` / `get` / `iterate_all_partitions` / `WRITETIME(collection)`) is
//! UNCHANGED — it still uses the collapsed [`Value`] representation.
//!
//! Byte-format invariants this representation must preserve (see
//! `docs/sstables-definitive-guide/` Ch.5 + Appendix B):
//! - Element order is the on-disk order (SET by serialized bytes, MAP by key
//!   bytes, LIST insertion order with 16-byte TimeUUID paths). Per-element
//!   timestamps must NOT reorder elements.
//! - A complex deletion's `LIVE` sentinel is `(i64::MIN, i32::MAX)`; a real
//!   deletion carries `(markedForDeleteAt µs, localDeletionTime s)`.
//! - Far-future local deletion times in `[2^31, 2^32)` are preserved as the
//!   wrapping `as u32 as i32` value — never widened to i64.

use crate::types::{RowKey, ScanRow, TombstoneType, Value};

/// One row surfaced by the compaction read path, carrying per-element complex
/// cells and the real per-column complex deletion (epic #899, Phase A).
///
/// This is the compaction-only counterpart of the old `(RowKey, ScanRow, i64)`
/// tuple. `row_timestamp` is the row-level write timestamp (for a tombstone it
/// is `markedForDeleteAt`); `row_data` holds either a row tombstone or the live
/// simple + complex cells.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionRow {
    /// Partition key bytes (token derived downstream).
    pub key: RowKey,
    /// Row-level write timestamp in microseconds (for a tombstone:
    /// `markedForDeleteAt`).
    pub row_timestamp: i64,
    /// Row payload: tombstone or live cells.
    pub row_data: CompactionRowData,
}

impl CompactionRow {
    /// Build a [`CompactionRow`] from the legacy collapsed `(RowKey, ScanRow,
    /// timestamp)` representation (the non-V5 compaction fallback path).
    ///
    /// This loses per-element complex granularity (the legacy fallback has none
    /// to begin with): a live `ScanRow::Row` becomes simple cells, a
    /// `ScanRow::Marker(Value::Tombstone(RowTombstone))` becomes a row tombstone,
    /// any other marker becomes a single `value` cell. The V5CompressedLegacy
    /// path bypasses this and builds per-element rows directly.
    ///
    /// Issue #1334: the reader carries every row through the single [`ScanRow`]
    /// carrier — this consumer disassembles that same carrier (no `Value::Map`
    /// bifurcation).
    pub fn from_legacy_value(key: RowKey, row: ScanRow, row_timestamp: i64) -> Self {
        let row_data = match row {
            // A live row's interned cells become simple cells.
            ScanRow::Row(entries) => {
                let simple = entries
                    .into_iter()
                    .map(|(k, v)| {
                        let timestamp = match &v {
                            Value::Tombstone(info) => info.deletion_time,
                            _ => row_timestamp,
                        };
                        SimpleCell {
                            column: k.to_string(),
                            value: v,
                            timestamp,
                            ttl: None,
                            local_deletion_time: None,
                        }
                    })
                    .collect();
                CompactionRowData::Live {
                    simple,
                    complex: Vec::new(),
                    // A live `ScanRow::Row` never carries a coexisting row deletion
                    // (a row tombstone arrives as a `ScanRow::Marker`, handled below).
                    row_deletion: None,
                }
            }
            // A row tombstone marker becomes a row tombstone.
            ScanRow::Marker(Value::Tombstone(info))
                if info.tombstone_type == TombstoneType::RowTombstone =>
            {
                CompactionRowData::Tombstone {
                    deletion_time: info.deletion_time,
                    local_deletion_time: 0,
                    // The legacy collapsed-value fallback has no clustering capture
                    // (the clustering prefix is not surfaced on this path), so the
                    // tombstone lands in the partition's `None` clustering bucket
                    // exactly as before (#912 carries clustering only on the V5
                    // per-element path).
                    clustering: Vec::new(),
                }
            }
            // Any other marker (null row, cell tombstone, …) collapses to a single
            // `value` cell, exactly as the pre-#1334 fallback did.
            ScanRow::Marker(other) => CompactionRowData::Live {
                simple: vec![SimpleCell {
                    column: "value".to_string(),
                    value: other,
                    timestamp: row_timestamp,
                    ttl: None,
                    local_deletion_time: None,
                }],
                complex: Vec::new(),
                row_deletion: None,
            },
        };
        CompactionRow {
            key,
            row_timestamp,
            row_data,
        }
    }
}

/// A clustering bound of a range-tombstone marker surfaced on the compaction
/// read path (issue #933).
///
/// Reader-native counterpart of
/// [`crate::storage::write_engine::mutation::ClusteringBound`]; kept here so the
/// compaction read contract does not depend on the write-engine types. Each
/// bound carries its clustering-prefix `(name, value)` pairs (possibly a PREFIX
/// shorter than the full clustering arity). An open bound (the writer emits these
/// as an inclusive bound with zero clustering values) is [`Self::Bottom`] /
/// [`Self::Top`].
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionBound {
    /// Inclusive bound (the clustering prefix is part of the deletion range).
    Inclusive(Vec<(String, Value)>),
    /// Exclusive bound (the clustering prefix is NOT part of the deletion range).
    Exclusive(Vec<(String, Value)>),
    /// Before all clustering keys (start of partition).
    Bottom,
    /// After all clustering keys (end of partition).
    Top,
}

/// Live-or-tombstone payload of a [`CompactionRow`].
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionRowData {
    /// A complete range tombstone (issue #933): the paired start + end bounds of
    /// a clustering-range delete, with the authoritative deletion timestamps.
    ///
    /// The reader pairs the on-disk start/end bound markers (or boundary markers)
    /// into one self-contained range so the compaction merge can shadow covered
    /// cells AND re-emit the surviving marker to the output SSTable. `deletion_time`
    /// is `markedForDeleteAt` (microseconds); `local_deletion_time` is the GC-grace
    /// clock (seconds, carried as the wrapping `as u32 as i32` for far-future LDTs).
    RangeMarker {
        /// Start bound of the deleted clustering range.
        start: CompactionBound,
        /// End bound of the deleted clustering range.
        end: CompactionBound,
        /// `markedForDeleteAt` in microseconds.
        deletion_time: i64,
        /// `localDeletionTime` in seconds (GC-grace clock).
        local_deletion_time: i32,
    },
    /// Partition-level tombstone (whole-partition delete) carrying its
    /// authoritative timestamps (issue #1072).
    ///
    /// Surfaced by the compaction read path as a synthetic carrier row (no
    /// clustering) so the cross-generation merge can apply the partition deletion
    /// as the OUTERMOST floor — shadowing every older cell/row/range/complex
    /// marker across ALL merge sources — and re-emit the surviving partition
    /// tombstone to the output SSTable. Without this carrier a newer partition
    /// tombstone in one SSTable failed to shadow older live rows in another,
    /// resurrecting deleted partitions. `deletion_time` is `markedForDeleteAt`
    /// (microseconds); `local_deletion_time` is the GC-grace clock (seconds,
    /// carried as the wrapping `as u32 as i32` for far-future LDTs).
    PartitionDelete {
        /// `markedForDeleteAt` in microseconds.
        deletion_time: i64,
        /// `localDeletionTime` in seconds (GC-grace clock).
        local_deletion_time: i32,
    },
    /// Row tombstone (whole-row delete) carrying its authoritative timestamps.
    Tombstone {
        /// `markedForDeleteAt` in microseconds.
        deletion_time: i64,
        /// `localDeletionTime` in seconds (GC-grace clock).
        local_deletion_time: i32,
        /// Clustering columns `(name, value)` in schema order identifying which
        /// clustering row this tombstone deletes (#912). On disk a row tombstone
        /// still carries its clustering prefix; capturing it here lets the merge
        /// route the tombstone into its own clustering bucket instead of
        /// collapsing every row tombstone (and the static row) into the single
        /// `None` bucket. Empty for an unclustered table (the partition's single
        /// row) and for the legacy collapsed-value fallback.
        clustering: Vec<(String, Value)>,
    },
    /// Live row: simple (single-cell) columns plus complex (multi-cell)
    /// columns with their per-element cells and optional complex deletion.
    Live {
        /// Simple, single-cell columns (incl. clustering columns surfaced as
        /// cells, and cell tombstones for deleted simple columns).
        simple: Vec<SimpleCell>,
        /// Complex (non-frozen collection / UDT) columns, each with its
        /// per-element cells and optional complex deletion.
        complex: Vec<ComplexColumn>,
        /// Row-level deletion that COEXISTS with the surviving live cells
        /// (issue #932). `Some((markedForDeleteAt µs, localDeletionTime s))`
        /// when this row carried `HAS_DELETION` AND still has surviving cells
        /// (the cells the merge kept are strictly newer than the deletion). The
        /// deletion is preserved so it keeps shadowing older cells of OTHER
        /// columns in SSTables not part of a partial compaction. `None` for a
        /// plain live row with no row deletion. A row whose ONLY payload is the
        /// deletion (no surviving cells) is a [`Self::Tombstone`], not a `Live`
        /// with this field set.
        row_deletion: Option<(i64, i32)>,
    },
}

/// A single-cell (simple) column value with its write metadata.
///
/// Cell tombstones for simple columns are represented by `value` holding a
/// `Value::Tombstone(CellTombstone)` (matching the legacy compaction stream).
#[derive(Debug, Clone, PartialEq)]
pub struct SimpleCell {
    /// Column name.
    pub column: String,
    /// Decoded cell value (or `Value::Tombstone` for a cell delete).
    pub value: Value,
    /// Effective cell write timestamp in microseconds (cell-own timestamp when
    /// present, else the row liveness timestamp).
    pub timestamp: i64,
    /// TTL in seconds when the cell is expiring (`None` otherwise).
    pub ttl: Option<u32>,
    /// `localDeletionTime` in seconds for an expiring / tombstone cell
    /// (`None` when not applicable).
    pub local_deletion_time: Option<i32>,
}

/// A complex (non-frozen collection / UDT) column: its per-element cells plus
/// an optional complex deletion marker covering elements written at or before
/// `marked_for_delete_at`.
#[derive(Debug, Clone, PartialEq)]
pub struct ComplexColumn {
    /// Column name.
    pub column: String,
    /// `Some((markedForDeleteAt µs, localDeletionTime s))` when a real complex
    /// deletion is present; `None` for the `LIVE` sentinel (no overwrite).
    pub complex_deletion: Option<(i64, i32)>,
    /// Per-element cells in on-disk order (epic #899 substrate / contract).
    pub elements: Vec<ComplexElement>,
    /// The whole-collection `Value` the reader collapses this column into
    /// (`Value::List` / `Value::Set` / `Value::Map`), EXACTLY as the
    /// pre-Phase-A read path produced it (SET/LIST element tombstones dropped,
    /// MAP null/tombstoned entries kept as `(key, Null)`, empty/overwritten
    /// collections kept as the empty collection).
    ///
    /// PHASE A NEUTRALITY (roborev #863, Finding 3): the merge OUTPUT path uses
    /// this collapsed value so the (untouched) writer emits byte-identical bytes
    /// to pre-Phase-A. The per-element `elements` ride alongside as the Phase-C
    /// foundation and are asserted by the reader-contract tests; per-element
    /// writer emit is Phase C.
    pub collapsed_value: Value,
}

/// A single element of a complex column (a list/set member, or a map entry).
#[derive(Debug, Clone, PartialEq)]
pub struct ComplexElement {
    /// Raw cell-path bytes that identify this element (the serialized element
    /// for a SET, the key for a MAP, the 16-byte TimeUUID for a LIST). Must be
    /// preserved byte-for-byte so the writer can round-trip it.
    pub cell_path: Vec<u8>,
    /// Decoded element value (`None` for a tombstoned or empty-value element,
    /// e.g. SET members which store the element in the path with an empty
    /// value).
    pub value: Option<Value>,
    /// Decoded element key for a MAP entry (the map key parsed from the
    /// `cell_path`). `None` for LIST / SET / UDT elements. Used to reconstruct a
    /// whole `Value::Map` for the writer-facing mutation while reconcile still
    /// keys on the raw `cell_path` bytes (epic #899, Phase A bridge).
    pub decoded_key: Option<Value>,
    /// Per-element write timestamp in microseconds (element-own when present,
    /// else the row liveness timestamp).
    pub timestamp: i64,
    /// TTL in seconds when the element is expiring (`None` otherwise).
    pub ttl: Option<u32>,
    /// `localDeletionTime` in seconds for an expiring / deleted element
    /// (`None` when not applicable). Far-future values in `[2^31, 2^32)` are
    /// kept as the wrapping `as u32 as i32` representation.
    pub local_deletion_time: Option<i32>,
    /// Whether this element carries the IS_DELETED (0x01) flag (an
    /// element-level tombstone).
    pub is_deleted: bool,
    /// Whether the on-disk cell carried the HAS_EMPTY_VALUE (0x04) flag.
    ///
    /// `true` for a SET member (whose value lives in the `cell_path`, not the
    /// cell value) and for any genuinely empty-value element. The compaction
    /// writer uses THIS flag — not the decoded [`value`](Self::value) — to decide
    /// whether to emit an on-disk value, so a SET element round-trips byte-for-
    /// byte (its decoded member is reconstructed from `cell_path`, never written
    /// as a cell value). Distinct from `is_deleted`: an empty-value live element
    /// is not a tombstone.
    pub has_empty_value: bool,
}
