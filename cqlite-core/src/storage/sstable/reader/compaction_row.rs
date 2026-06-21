//! Per-element / per-cell compaction read contract (epic #899, Phase A).
//!
//! The compaction read path historically emitted `(RowKey, Value, i64)` per row,
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

use crate::types::{RowKey, TombstoneType, Value};

/// One row surfaced by the compaction read path, carrying per-element complex
/// cells and the real per-column complex deletion (epic #899, Phase A).
///
/// This is the compaction-only counterpart of the old `(RowKey, Value, i64)`
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
    /// Build a [`CompactionRow`] from the legacy collapsed `(RowKey, Value,
    /// timestamp)` representation (the non-V5 compaction fallback path).
    ///
    /// This loses per-element complex granularity (the legacy fallback has none
    /// to begin with): a `Value::Map` row becomes simple cells, a
    /// `Value::Tombstone(RowTombstone)` becomes a row tombstone, any other value
    /// becomes a single `value` cell. The V5CompressedLegacy path bypasses this
    /// and builds per-element rows directly.
    pub fn from_legacy_value(key: RowKey, value: Value, row_timestamp: i64) -> Self {
        let row_data = match value {
            Value::Tombstone(info) if info.tombstone_type == TombstoneType::RowTombstone => {
                CompactionRowData::Tombstone {
                    deletion_time: info.deletion_time,
                    local_deletion_time: 0,
                }
            }
            Value::Map(entries) => {
                let simple = entries
                    .into_iter()
                    .map(|(k, v)| {
                        let column = match k {
                            Value::Text(s) => s,
                            other => format!("{:?}", other),
                        };
                        let timestamp = match &v {
                            Value::Tombstone(info) => info.deletion_time,
                            _ => row_timestamp,
                        };
                        SimpleCell {
                            column,
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
                }
            }
            other => CompactionRowData::Live {
                simple: vec![SimpleCell {
                    column: "value".to_string(),
                    value: other,
                    timestamp: row_timestamp,
                    ttl: None,
                    local_deletion_time: None,
                }],
                complex: Vec::new(),
            },
        };
        CompactionRow {
            key,
            row_timestamp,
            row_data,
        }
    }
}

/// Live-or-tombstone payload of a [`CompactionRow`].
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionRowData {
    /// Row tombstone (whole-row delete) carrying its authoritative timestamps.
    Tombstone {
        /// `markedForDeleteAt` in microseconds.
        deletion_time: i64,
        /// `localDeletionTime` in seconds (GC-grace clock).
        local_deletion_time: i32,
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
    /// Per-element cells in on-disk order.
    pub elements: Vec<ComplexElement>,
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
}
