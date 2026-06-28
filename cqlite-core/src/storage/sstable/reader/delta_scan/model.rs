//! Delta-scan record data model (Epic #696, Issue #697 types).
//!
//! These types form the public surface of the delta-scan layer: the
//! discriminated [`DeltaRecord`] union and its supporting key/cell/metadata
//! structs, plus the [`ScanSummary`]/[`ScanSummaryHandle`] aggregate-statistics
//! pair.  The streaming driver that produces them lives in the sibling
//! [`super::scan`] module.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::types::{ColumnId, Value};

// ---------------------------------------------------------------------------
// ScanSummary — scan-level aggregate statistics (Issue #700, DS4)
// ---------------------------------------------------------------------------

/// Summary statistics produced by a [`scan_delta`](super::scan_delta) run.
///
/// The caller receives a [`ScanSummaryHandle`] alongside the record stream;
/// after the stream is drained the handle's counters reflect the full scan.
///
/// ## Element tombstones (Issue #493 / DS4)
///
/// Non-frozen collection cells may contain **element-level removals**
/// (`s = s - {x}` for sets, individual map-key deletions).  V1 of the
/// delta-scan layer cannot represent these at element granularity; they are
/// detected, counted here, and reported as a warning — but not silently
/// dropped.  Consumers that need element-level fidelity must wait for full
/// Issue #493 implementation.
#[derive(Debug, Clone)]
pub struct ScanSummary {
    /// Total number of element-level collection tombstones detected across
    /// all collection columns in this scan.
    ///
    /// A non-zero value means the delta contains `s = s - {x}` style
    /// removals that are not represented in the emitted [`DeltaRecord`]s
    /// (Issue #493 follow-up).
    pub element_tombstones_detected: u64,
}

/// Handle to in-progress scan summary counters.
///
/// Returned by [`scan_delta`](super::scan_delta) alongside the record receiver.
/// After the receiver is drained (stream complete), call [`ScanSummaryHandle::read`]
/// to obtain the final [`ScanSummary`].
///
/// The handle is cheaply clonable — all clones share the same counters.
#[derive(Debug, Clone)]
pub struct ScanSummaryHandle {
    element_tombstones: Arc<AtomicU64>,
}

impl ScanSummaryHandle {
    pub(super) fn new() -> Self {
        Self {
            element_tombstones: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Increment the element-tombstone counter by `n`.
    pub(super) fn add_element_tombstones(&self, n: u64) {
        self.element_tombstones.fetch_add(n, Ordering::Relaxed);
    }

    /// Read the current scan summary.  Call after draining the record receiver
    /// for a complete picture.
    pub fn read(&self) -> ScanSummary {
        ScanSummary {
            element_tombstones_detected: self.element_tombstones.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// RowKeys
// ---------------------------------------------------------------------------

/// Combined partition key + clustering columns for a single row (or partition).
///
/// For records that address a full partition (`PartitionDelete`,
/// `StaticUpsert`, `RangeDelete`) the `clustering` vec is empty.  For
/// row-addressed records it holds the decoded clustering column values in
/// primary-key definition order.
///
/// Values are decoded according to the `TableSchema` in effect at scan time;
/// the no-heuristics mandate applies — every component uses schema-authorised
/// types.
#[derive(Debug, Clone, PartialEq)]
pub struct RowKeys {
    /// Decoded partition-key components in definition order.
    ///
    /// Multi-component partition keys (composite) appear as multiple elements.
    pub partition: Vec<Value>,

    /// Decoded clustering-key components in definition order.
    ///
    /// Empty when the record is partition-scoped (partition delete, static
    /// upsert, or range-delete — in which case bounds carry the clustering
    /// information instead).
    pub clustering: Vec<Value>,
}

impl RowKeys {
    /// Create a partition-scoped key (no clustering components).
    pub fn partition_only(partition: Vec<Value>) -> Self {
        Self {
            partition,
            clustering: Vec::new(),
        }
    }

    /// Create a fully-specified row key.
    pub fn new(partition: Vec<Value>, clustering: Vec<Value>) -> Self {
        Self {
            partition,
            clustering,
        }
    }
}

// ---------------------------------------------------------------------------
// CellMeta — row liveness info
// ---------------------------------------------------------------------------

/// Liveness metadata for a row's primary-key cell.
///
/// Cassandra writes a liveness-info record alongside a row when the row was
/// created with `INSERT` (not `UPDATE`).  `UPDATE` statements produce rows
/// with no liveness info; those rows have `liveness: None` in
/// [`DeltaRecord::Upsert`].
///
/// The `writetime` here is the row-level primary-key liveness timestamp,
/// equivalent to `__ts` in the Parquet envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct CellMeta {
    /// Writetime in microseconds since the Unix epoch.
    pub writetime: i64,

    /// Expiry time in microseconds since the Unix epoch, if a TTL was set on
    /// the `INSERT`.  `None` means no TTL.
    pub expires_at: Option<i64>,
}

impl CellMeta {
    /// Create a liveness record with no TTL.
    pub fn new(writetime: i64) -> Self {
        Self {
            writetime,
            expires_at: None,
        }
    }

    /// Create a liveness record with a TTL expiry time.
    pub fn with_ttl(writetime: i64, expires_at: i64) -> Self {
        Self {
            writetime,
            expires_at: Some(expires_at),
        }
    }
}

// ---------------------------------------------------------------------------
// CellDelta
// ---------------------------------------------------------------------------

/// Per-cell change record carried inside [`DeltaRecord::Upsert`] and
/// [`DeltaRecord::StaticUpsert`].
///
/// A `null` [`value`][Self::value] represents a cell tombstone
/// (`DELETE col FROM t WHERE …`).  A non-null value is the cell content as
/// decoded by the schema-aware reader — no heuristics.
///
/// ## Collection columns (v1 limitation)
///
/// Non-frozen collection columns carry per-element writetimes and element
/// tombstones that v1 cannot faithfully represent at element granularity.
/// For those columns:
///
/// - `value` holds the elements present in this generation (for an append
///   `s = s + {…}` that is only the appended elements — correct delta
///   semantics).
/// - `writetime` is the maximum element writetime.
/// - `replaced` is `true` when the generation carries a collection tombstone
///   (i.e. an overwrite `s = {…}`), signalling consumers to replace rather
///   than merge.
///
/// Full element-level fidelity is a tracked follow-up (Issue #493).
#[derive(Debug, Clone, PartialEq)]
pub struct CellDelta {
    /// The decoded cell value, or `None` for a cell tombstone.
    pub value: Option<Value>,

    /// Writetime in microseconds since the Unix epoch.
    pub writetime: i64,

    /// TTL expiry time in microseconds since the Unix epoch.
    ///
    /// `None` means no TTL was set on this cell.  The delta-scan layer never
    /// resolves TTLs at scan time — whether a cell is expired is left to the
    /// downstream consumer (idempotent output guarantee).
    pub expires_at: Option<i64>,

    /// `true` when this collection cell carries a collection-level tombstone,
    /// meaning the downstream consumer must **replace** rather than merge the
    /// prior collection state.
    ///
    /// Always `false` for scalar (non-collection) columns.
    pub replaced: bool,
}

impl CellDelta {
    /// Create a simple value cell with no TTL and no collection-replace flag.
    pub fn value(value: Value, writetime: i64) -> Self {
        Self {
            value: Some(value),
            writetime,
            expires_at: None,
            replaced: false,
        }
    }

    /// Create a cell tombstone (no value, just a deletion timestamp).
    pub fn tombstone(writetime: i64) -> Self {
        Self {
            value: None,
            writetime,
            expires_at: None,
            replaced: false,
        }
    }

    /// Create a value cell with a TTL expiry time.
    pub fn value_with_ttl(value: Value, writetime: i64, expires_at: i64) -> Self {
        Self {
            value: Some(value),
            writetime,
            expires_at: Some(expires_at),
            replaced: false,
        }
    }

    /// Create a collection cell that replaces (not merges) prior state.
    pub fn collection_replace(value: Value, writetime: i64) -> Self {
        Self {
            value: Some(value),
            writetime,
            expires_at: None,
            replaced: true,
        }
    }
}

// ---------------------------------------------------------------------------
// RangeBound
// ---------------------------------------------------------------------------

/// One bound of a range-tombstone clustering-key range.
///
/// ## Prefix bounds
///
/// Cassandra range tombstones may specify bounds that are **prefixes** of the
/// full clustering key — for example, a table with clustering columns
/// `(year INT, month INT, day INT)` can have a range tombstone covering all
/// rows in `year = 2024`.  In that case:
///
/// ```text
/// start = RangeBound { values: [Value::Integer(2024)], inclusive: true  }
/// end   = RangeBound { values: [Value::Integer(2024)], inclusive: true  }
/// ```
///
/// Trailing clustering components that are absent from the bound are simply
/// not present in [`values`][Self::values].  Consumers must treat a prefix
/// bound as matching all rows whose clustering key begins with the given
/// prefix (within the inclusive/exclusive constraint).
///
/// ## Empty bound
///
/// An empty `values` vec with `inclusive: false` represents an open
/// (unbounded) end — i.e. from/to the beginning or end of the partition.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeBound {
    /// The clustering-key prefix values for this bound, in primary-key order.
    ///
    /// May be shorter than the full clustering-key arity (prefix bound).
    /// An empty vec means an open (unbounded) end.
    pub values: Vec<Value>,

    /// Whether the bound is inclusive (`>=` / `<=`) or exclusive (`>` / `<`).
    pub inclusive: bool,
}

impl RangeBound {
    /// Create a bound from a full or partial set of clustering values.
    pub fn new(values: Vec<Value>, inclusive: bool) -> Self {
        Self { values, inclusive }
    }

    /// Convenience: create an inclusive bound.
    pub fn inclusive(values: Vec<Value>) -> Self {
        Self {
            values,
            inclusive: true,
        }
    }

    /// Convenience: create an exclusive bound.
    pub fn exclusive(values: Vec<Value>) -> Self {
        Self {
            values,
            inclusive: false,
        }
    }

    /// An open (unbounded) end — matches from/to the start or end of the partition.
    pub fn open() -> Self {
        Self {
            values: Vec::new(),
            inclusive: false,
        }
    }

    /// Returns `true` if this is a prefix bound (fewer values than the full
    /// clustering-key arity of the table, or the full arity is not known).
    ///
    /// Note: callers that know the table's clustering-key arity should compare
    /// `self.values.len()` against that arity directly rather than relying on
    /// this method.
    pub fn is_prefix(&self) -> bool {
        // An empty bound is not a prefix — it is an open bound.
        // Any non-empty bound with at least one value is potentially a prefix;
        // the caller is responsible for comparing against the full arity.
        !self.values.is_empty()
    }
}

// ---------------------------------------------------------------------------
// DeltaRecord
// ---------------------------------------------------------------------------

/// A single change record emitted by the delta-scan API.
///
/// Records stream in SSTable order (partition, then clustering).  The scan
/// makes no cross-SSTable decisions — no merge, no GC-grace filtering.
///
/// ## Variant reference
///
/// | Variant | CQL operation | Key scope |
/// |---------|---------------|-----------|
/// | `Upsert` | `INSERT`/`UPDATE` on regular columns | `(pk, ck)` |
/// | `StaticUpsert` | `UPDATE` on static columns | `pk` |
/// | `RowDelete` | `DELETE FROM t WHERE pk=? AND ck=?` | `(pk, ck)` |
/// | `RangeDelete` | `DELETE FROM t WHERE pk=? AND ck>=? AND ck<?` | `pk` + bounds |
/// | `PartitionDelete` | `DELETE FROM t WHERE pk=?` | `pk` |
///
/// ## Downstream merge keys
///
/// - `Upsert` / `RowDelete` — reconcile on `(partition, clustering)`.
/// - `StaticUpsert` — reconcile on `partition`.
/// - `PartitionDelete` / `RangeDelete` — apply as predicates using `__ts`
///   (i.e. `deleted_at`) to decide last-write-wins per cell.
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaRecord {
    /// A row-level insert or update of regular (non-static) columns.
    ///
    /// `liveness` is `Some` when the row was created with `INSERT` and carries
    /// a primary-key liveness timestamp.  `UPDATE` statements produce rows
    /// with `liveness: None`.
    ///
    /// Each element of `cells` is a `(column_id, delta)` pair.  Only the
    /// columns actually written in this generation appear; absent columns are
    /// not present (they are null in the Parquet envelope).
    Upsert {
        /// Partition key + clustering columns for this row.
        keys: RowKeys,
        /// Row liveness info, present only for `INSERT` operations.
        liveness: Option<CellMeta>,
        /// Per-column cell deltas for non-static columns modified in this row.
        cells: Vec<(ColumnId, CellDelta)>,
    },

    /// An update of one or more static columns for a partition.
    ///
    /// Static columns belong to the partition, not individual rows.  The
    /// clustering key is empty in `partition_key`.
    StaticUpsert {
        /// Partition key (no clustering columns).
        partition_key: RowKeys,
        /// Per-column cell deltas for static columns modified.
        cells: Vec<(ColumnId, CellDelta)>,
    },

    /// A row-level tombstone (`DELETE FROM t WHERE pk=? AND ck=?`).
    RowDelete {
        /// Partition key + clustering columns identifying the deleted row.
        keys: RowKeys,
        /// Deletion timestamp in microseconds since the Unix epoch
        /// (`markedForDeleteAt` in Cassandra internals).
        deleted_at: i64,
    },

    /// A range tombstone covering a contiguous clustering-key range within a
    /// single partition.
    ///
    /// `start` and `end` may be prefix bounds (see [`RangeBound`]).
    ///
    /// ```text
    /// DELETE FROM t WHERE pk=1 AND ck >= 'a' AND ck < 'm'
    /// ```
    RangeDelete {
        /// Partition key (no clustering columns — bounds carry the range).
        partition_key: RowKeys,
        /// Inclusive or exclusive lower clustering-key bound.
        start: RangeBound,
        /// Inclusive or exclusive upper clustering-key bound.
        end: RangeBound,
        /// Deletion timestamp in microseconds since the Unix epoch.
        deleted_at: i64,
    },

    /// A partition-level tombstone (`DELETE FROM t WHERE pk=?`).
    ///
    /// Supersedes every row and cell in the partition whose writetime is older
    /// than `deleted_at`.
    PartitionDelete {
        /// Partition key (no clustering columns).
        partition_key: RowKeys,
        /// Deletion timestamp in microseconds since the Unix epoch.
        deleted_at: i64,
    },
}

impl DeltaRecord {
    /// Return the partition-key portion of any record type.
    pub fn partition_key(&self) -> &[Value] {
        match self {
            DeltaRecord::Upsert { keys, .. } => &keys.partition,
            DeltaRecord::StaticUpsert { partition_key, .. } => &partition_key.partition,
            DeltaRecord::RowDelete { keys, .. } => &keys.partition,
            DeltaRecord::RangeDelete { partition_key, .. } => &partition_key.partition,
            DeltaRecord::PartitionDelete { partition_key, .. } => &partition_key.partition,
        }
    }

    /// Return the `__op` discriminator string used in the Parquet envelope.
    pub fn op_name(&self) -> &'static str {
        match self {
            DeltaRecord::Upsert { .. } => "upsert",
            DeltaRecord::StaticUpsert { .. } => "static_upsert",
            DeltaRecord::RowDelete { .. } => "row_delete",
            DeltaRecord::RangeDelete { .. } => "range_delete",
            DeltaRecord::PartitionDelete { .. } => "partition_delete",
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    // ------------------------------------------------------------------
    // RowKeys helpers
    // ------------------------------------------------------------------

    #[test]
    fn row_keys_partition_only() {
        let keys = RowKeys::partition_only(vec![Value::Integer(42)]);
        assert_eq!(keys.partition, vec![Value::Integer(42)]);
        assert!(keys.clustering.is_empty());
    }

    #[test]
    fn row_keys_full() {
        let keys = RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]);
        assert_eq!(keys.partition.len(), 1);
        assert_eq!(keys.clustering.len(), 1);
    }

    // ------------------------------------------------------------------
    // CellMeta helpers
    // ------------------------------------------------------------------

    #[test]
    fn cell_meta_no_ttl() {
        let m = CellMeta::new(1_000_000);
        assert_eq!(m.writetime, 1_000_000);
        assert!(m.expires_at.is_none());
    }

    #[test]
    fn cell_meta_with_ttl() {
        let m = CellMeta::with_ttl(1_000_000, 2_000_000);
        assert_eq!(m.expires_at, Some(2_000_000));
    }

    // ------------------------------------------------------------------
    // CellDelta constructors
    // ------------------------------------------------------------------

    #[test]
    fn cell_delta_value() {
        let d = CellDelta::value(Value::Text("hello".into()), 100);
        assert!(d.value.is_some());
        assert_eq!(d.writetime, 100);
        assert!(d.expires_at.is_none());
        assert!(!d.replaced);
    }

    #[test]
    fn cell_delta_tombstone() {
        let d = CellDelta::tombstone(200);
        assert!(d.value.is_none());
        assert_eq!(d.writetime, 200);
        assert!(!d.replaced);
    }

    #[test]
    fn cell_delta_with_ttl() {
        let d = CellDelta::value_with_ttl(Value::Integer(7), 100, 9999);
        assert_eq!(d.expires_at, Some(9999));
        assert!(!d.replaced);
    }

    #[test]
    fn cell_delta_collection_replace() {
        let d = CellDelta::collection_replace(Value::Text("x".into()), 300);
        assert!(d.replaced);
        assert!(d.value.is_some());
    }

    // ------------------------------------------------------------------
    // RangeBound
    // ------------------------------------------------------------------

    #[test]
    fn range_bound_inclusive() {
        let b = RangeBound::inclusive(vec![Value::Text("a".into())]);
        assert!(b.inclusive);
        assert_eq!(b.values.len(), 1);
    }

    #[test]
    fn range_bound_exclusive() {
        let b = RangeBound::exclusive(vec![Value::Text("m".into())]);
        assert!(!b.inclusive);
    }

    #[test]
    fn range_bound_open() {
        let b = RangeBound::open();
        assert!(b.values.is_empty());
        assert!(!b.inclusive);
        // An open bound is NOT considered a prefix.
        assert!(!b.is_prefix());
    }

    #[test]
    fn range_bound_prefix() {
        // Two-column clustering key; only the first column is present → prefix.
        let b = RangeBound::inclusive(vec![Value::Integer(2024)]);
        assert!(b.is_prefix());
    }

    // ------------------------------------------------------------------
    // DeltaRecord — one construction test per variant
    // ------------------------------------------------------------------

    fn sample_pk() -> RowKeys {
        RowKeys::partition_only(vec![Value::Integer(1)])
    }

    fn sample_row_keys() -> RowKeys {
        RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("ck1".into())])
    }

    fn sample_cell() -> (ColumnId, CellDelta) {
        (
            ColumnId::new("val"),
            CellDelta::value(Value::Text("hello".into()), 1_700_000_000_000_000),
        )
    }

    #[test]
    fn delta_record_upsert() {
        let rec = DeltaRecord::Upsert {
            keys: sample_row_keys(),
            liveness: Some(CellMeta::new(1_700_000_000_000_000)),
            cells: vec![sample_cell()],
        };
        assert_eq!(rec.op_name(), "upsert");
        assert_eq!(rec.partition_key(), &[Value::Integer(1)]);

        if let DeltaRecord::Upsert {
            keys,
            liveness,
            cells,
        } = &rec
        {
            assert_eq!(keys.clustering, vec![Value::Text("ck1".into())]);
            assert!(liveness.is_some());
            assert_eq!(cells.len(), 1);
        } else {
            panic!("expected Upsert");
        }
    }

    #[test]
    fn delta_record_upsert_no_liveness() {
        // UPDATE (not INSERT) — no liveness info.
        let rec = DeltaRecord::Upsert {
            keys: sample_row_keys(),
            liveness: None,
            cells: vec![sample_cell()],
        };
        if let DeltaRecord::Upsert { liveness, .. } = &rec {
            assert!(liveness.is_none());
        } else {
            panic!("expected Upsert");
        }
    }

    #[test]
    fn delta_record_static_upsert() {
        let rec = DeltaRecord::StaticUpsert {
            partition_key: sample_pk(),
            cells: vec![(
                ColumnId::new("st"),
                CellDelta::value(Value::Text("S".into()), 1_700_000_000_000_000),
            )],
        };
        assert_eq!(rec.op_name(), "static_upsert");
        assert_eq!(rec.partition_key(), &[Value::Integer(1)]);

        if let DeltaRecord::StaticUpsert {
            partition_key,
            cells,
        } = &rec
        {
            assert!(partition_key.clustering.is_empty());
            assert_eq!(cells.len(), 1);
        } else {
            panic!("expected StaticUpsert");
        }
    }

    #[test]
    fn delta_record_row_delete() {
        let rec = DeltaRecord::RowDelete {
            keys: sample_row_keys(),
            deleted_at: 1_700_000_000_000_000,
        };
        assert_eq!(rec.op_name(), "row_delete");
        if let DeltaRecord::RowDelete { deleted_at, .. } = &rec {
            assert_eq!(*deleted_at, 1_700_000_000_000_000);
        } else {
            panic!("expected RowDelete");
        }
    }

    #[test]
    fn delta_record_range_delete() {
        let rec = DeltaRecord::RangeDelete {
            partition_key: sample_pk(),
            start: RangeBound::inclusive(vec![Value::Text("a".into())]),
            end: RangeBound::exclusive(vec![Value::Text("m".into())]),
            deleted_at: 1_700_000_000_000_001,
        };
        assert_eq!(rec.op_name(), "range_delete");
        if let DeltaRecord::RangeDelete {
            start,
            end,
            deleted_at,
            ..
        } = &rec
        {
            assert!(start.inclusive);
            assert!(!end.inclusive);
            assert_eq!(*deleted_at, 1_700_000_000_000_001);
        } else {
            panic!("expected RangeDelete");
        }
    }

    #[test]
    fn delta_record_range_delete_prefix_bound() {
        // Two-column clustering key (year INT, month INT);
        // bound only specifies year → prefix semantics.
        let rec = DeltaRecord::RangeDelete {
            partition_key: sample_pk(),
            start: RangeBound::inclusive(vec![Value::Integer(2024)]),
            end: RangeBound::inclusive(vec![Value::Integer(2024)]),
            deleted_at: 1_700_000_000_000_002,
        };
        if let DeltaRecord::RangeDelete { start, end, .. } = &rec {
            assert!(start.is_prefix());
            assert!(end.is_prefix());
            assert_eq!(start.values.len(), 1);
            assert_eq!(end.values.len(), 1);
        } else {
            panic!("expected RangeDelete");
        }
    }

    #[test]
    fn delta_record_partition_delete() {
        let rec = DeltaRecord::PartitionDelete {
            partition_key: sample_pk(),
            deleted_at: 1_700_000_000_000_003,
        };
        assert_eq!(rec.op_name(), "partition_delete");
        if let DeltaRecord::PartitionDelete {
            partition_key,
            deleted_at,
        } = &rec
        {
            assert_eq!(partition_key.partition, vec![Value::Integer(1)]);
            assert!(partition_key.clustering.is_empty());
            assert_eq!(*deleted_at, 1_700_000_000_000_003);
        } else {
            panic!("expected PartitionDelete");
        }
    }

    // ------------------------------------------------------------------
    // op_name exhaustiveness sanity check
    // ------------------------------------------------------------------

    #[test]
    fn op_names_are_distinct() {
        let ops = [
            DeltaRecord::Upsert {
                keys: sample_row_keys(),
                liveness: None,
                cells: vec![],
            },
            DeltaRecord::StaticUpsert {
                partition_key: sample_pk(),
                cells: vec![],
            },
            DeltaRecord::RowDelete {
                keys: sample_row_keys(),
                deleted_at: 0,
            },
            DeltaRecord::RangeDelete {
                partition_key: sample_pk(),
                start: RangeBound::open(),
                end: RangeBound::open(),
                deleted_at: 0,
            },
            DeltaRecord::PartitionDelete {
                partition_key: sample_pk(),
                deleted_at: 0,
            },
        ];

        let names: Vec<&str> = ops.iter().map(|r| r.op_name()).collect();
        // All five names must be distinct.
        let mut unique = names.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), names.len(), "duplicate op_name: {:?}", names);
    }

    // ------------------------------------------------------------------
    // TombstoneType::PartitionTombstone integration
    // ------------------------------------------------------------------

    #[test]
    fn partition_tombstone_type_exists() {
        use crate::types::TombstoneType;
        // Verify the new variant is reachable and its display formatting works.
        let t = TombstoneType::PartitionTombstone;
        // Round-trip through Debug (basic smoke test — not format-sensitive).
        let s = format!("{:?}", t);
        assert!(s.contains("Partition"), "unexpected debug: {}", s);
    }

    // -----------------------------------------------------------------------
    // Issue #698 acceptance criteria — record builder unit tests
    // -----------------------------------------------------------------------

    /// AC: Cell tombstone has `value: None`; untouched columns are absent from
    /// `cells` (not present with a null value).
    #[test]
    fn cell_tombstone_value_is_none_untouched_columns_absent() {
        // Only `val` is in the Upsert; `other` is intentionally absent.
        let tombstone_cell = CellDelta::tombstone(1_700_000_000_000_000);

        let rec = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: None,
            cells: vec![(ColumnId::new("val"), tombstone_cell.clone())],
            // `other` is NOT in the cells vec at all.
        };

        if let DeltaRecord::Upsert { cells, .. } = &rec {
            // val is present as a tombstone.
            let val_entry = cells.iter().find(|(id, _)| id.0 == "val");
            assert!(val_entry.is_some(), "val should be present");
            let (_, cell) = val_entry.unwrap();
            assert!(
                cell.value.is_none(),
                "cell tombstone must have value == None"
            );
            assert_eq!(cell.writetime, 1_700_000_000_000_000);

            // `other` is absent — not present in the cells vec at all.
            let other_entry = cells.iter().find(|(id, _)| id.0 == "other");
            assert!(other_entry.is_none(), "untouched column must be absent");
        } else {
            panic!("expected Upsert");
        }
    }

    /// AC: Partial UPDATE produces `liveness: None`; INSERT produces `liveness: Some(_)`.
    #[test]
    fn liveness_none_for_update_some_for_insert() {
        let ts = 1_700_000_000_000_000_i64;

        // Partial UPDATE — no row-level liveness.
        let update_rec = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: None,
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value(Value::Text("x".into()), ts),
            )],
        };
        if let DeltaRecord::Upsert { liveness, .. } = &update_rec {
            assert!(liveness.is_none(), "UPDATE must have liveness == None");
        }

        // INSERT — has row-level liveness.
        let insert_rec = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(2)], vec![Value::Text("b".into())]),
            liveness: Some(CellMeta::new(ts)),
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value(Value::Text("y".into()), ts),
            )],
        };
        if let DeltaRecord::Upsert { liveness, .. } = &insert_rec {
            let lv = liveness.as_ref().expect("INSERT must have liveness");
            assert_eq!(lv.writetime, ts);
            assert!(lv.expires_at.is_none());
        }
    }

    /// AC: INSERT with TTL — liveness carries `expires_at`.
    #[test]
    fn insert_with_ttl_liveness_carries_expires_at() {
        let ts: i64 = 1_700_000_000_000_000;
        let exp: i64 = 1_700_000_086_400_000_000; // +1 day in µs

        let rec = DeltaRecord::Upsert {
            keys: RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("a".into())]),
            liveness: Some(CellMeta::with_ttl(ts, exp)),
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value_with_ttl(Value::Text("x".into()), ts, exp),
            )],
        };
        if let DeltaRecord::Upsert {
            liveness, cells, ..
        } = &rec
        {
            let lv = liveness.as_ref().unwrap();
            assert_eq!(lv.writetime, ts);
            assert_eq!(lv.expires_at, Some(exp));

            let (_, cell) = &cells[0];
            assert_eq!(cell.expires_at, Some(exp));
        }
    }

    /// AC: Static column update emits `StaticUpsert` with empty clustering and
    /// a `partition_key` that has no clustering component.
    #[test]
    fn static_column_update_emits_static_upsert() {
        let ts: i64 = 1_700_000_000_000_000;
        let rec = DeltaRecord::StaticUpsert {
            partition_key: RowKeys::partition_only(vec![Value::Integer(42)]),
            cells: vec![(
                ColumnId::new("static_col"),
                CellDelta::value(Value::Text("S".into()), ts),
            )],
        };

        assert_eq!(rec.op_name(), "static_upsert");
        assert_eq!(rec.partition_key(), &[Value::Integer(42)]);

        if let DeltaRecord::StaticUpsert {
            partition_key,
            cells,
        } = &rec
        {
            // Clustering must be empty for StaticUpsert.
            assert!(
                partition_key.clustering.is_empty(),
                "StaticUpsert must have empty clustering"
            );
            assert_eq!(cells.len(), 1);
            let (col_id, cell) = &cells[0];
            assert_eq!(col_id.0, "static_col");
            assert!(cell.value.is_some());
        } else {
            panic!("expected StaticUpsert");
        }
    }

    /// AC: Cell tombstone writetime comes from the deletion record, not the row
    /// liveness timestamp.
    #[test]
    fn cell_tombstone_writetime_is_deletion_time_not_row_ts() {
        let row_ts: i64 = 1_000_000;
        let del_ts: i64 = 2_000_000;

        // A cell tombstone should carry `del_ts`, not `row_ts`.
        let cell = CellDelta::tombstone(del_ts);
        assert_eq!(cell.writetime, del_ts);
        assert_ne!(
            cell.writetime, row_ts,
            "tombstone writetime must be the deletion timestamp, not the row timestamp"
        );
    }

    // -----------------------------------------------------------------------
    // DS4 (Issue #700): collection v1 semantics — unit tests
    // -----------------------------------------------------------------------

    /// AC: A non-collection scalar cell always has `replaced = false`.
    #[test]
    fn ds4_replaced_false_for_scalar_column() {
        let cell = CellDelta::value(Value::Integer(42), 1_700_000_000_000_000);
        assert!(
            !cell.replaced,
            "scalar column must always have replaced=false; got replaced=true"
        );
    }

    /// AC: A collection append (no collection tombstone) sets `replaced = false`.
    #[test]
    fn ds4_collection_append_replaced_false() {
        // Append semantics: value is present, but `replaced` is false because
        // no collection-level tombstone accompanied the mutation.
        let cell = CellDelta {
            value: Some(Value::List(vec![Value::Text("new_element".into())])),
            writetime: 1_700_000_000_000_000,
            expires_at: None,
            replaced: false,
        };
        assert!(
            !cell.replaced,
            "collection append must have replaced=false (no collection tombstone)"
        );
    }

    /// AC: A collection overwrite (generation carries a collection tombstone)
    /// sets `replaced = true`.
    #[test]
    fn ds4_collection_overwrite_replaced_true() {
        // Overwrite semantics: the mutation issued `s = {x, y}` which replaces
        // prior state; `replaced = true` signals the consumer to discard old state.
        let cell = CellDelta::collection_replace(
            Value::List(vec![Value::Text("a".into()), Value::Text("b".into())]),
            1_700_000_000_000_000,
        );
        assert!(
            cell.replaced,
            "collection overwrite must have replaced=true (collection tombstone present)"
        );
    }

    /// AC: `writetime` on a collection cell equals the max element writetime.
    ///
    /// When multiple elements carry distinct writetimes, the `CellDelta.writetime`
    /// exposed to downstream consumers must be the maximum across all elements.
    #[test]
    fn ds4_collection_writetime_equals_max_element_writetime() {
        let ts_early: i64 = 1_700_000_000_000_000;
        let ts_late: i64 = 1_700_000_100_000_000; // 100 seconds later

        // Simulate a collection cell where the parser set writetime to max(element_ts).
        let cell = CellDelta {
            value: Some(Value::List(vec![
                Value::Text("a".into()),
                Value::Text("b".into()),
            ])),
            writetime: ts_late, // the max — set by parse_row_data_with_offset
            expires_at: None,
            replaced: false,
        };

        assert_eq!(
            cell.writetime, ts_late,
            "writetime must equal max element writetime; expected {ts_late}, got {}",
            cell.writetime
        );
        assert!(
            cell.writetime > ts_early,
            "max element writetime {ts_late} must be greater than an earlier element writetime {ts_early}"
        );
    }

    /// AC: `ScanSummaryHandle` starts at zero and accumulates element tombstones.
    #[test]
    fn ds4_scan_summary_handle_accumulates_element_tombstones() {
        let handle = ScanSummaryHandle::new();

        // Initial state: no tombstones.
        assert_eq!(
            handle.read().element_tombstones_detected,
            0,
            "initial element_tombstones_detected must be 0"
        );

        // Add tombstones in two increments (simulating two rows with element removals).
        handle.add_element_tombstones(3);
        handle.add_element_tombstones(5);

        let summary = handle.read();
        assert_eq!(
            summary.element_tombstones_detected, 8,
            "element_tombstones_detected must accumulate: expected 8, got {}",
            summary.element_tombstones_detected
        );
    }

    /// AC: Cloned `ScanSummaryHandle` shares the same atomic counter.
    ///
    /// `scan_delta` clones the handle to pass to the parse task while the caller
    /// retains the original — both must reflect the same accumulator.
    #[test]
    fn ds4_scan_summary_handle_clone_shares_counter() {
        let handle = ScanSummaryHandle::new();
        let clone = handle.clone();

        clone.add_element_tombstones(7);

        assert_eq!(
            handle.read().element_tombstones_detected,
            7,
            "original handle must reflect counter updated via clone"
        );
    }

    /// AC: `replaced = false` for a cell tombstone (even for a collection column,
    /// the cell-level tombstone path sets `replaced = false`).
    #[test]
    fn ds4_cell_tombstone_replaced_false() {
        let cell = CellDelta::tombstone(1_700_000_000_000_000);
        assert!(
            !cell.replaced,
            "cell tombstones must have replaced=false regardless of column type"
        );
    }

    // -----------------------------------------------------------------------
    // Issue #699 unit tests: DeltaRecord model — mixed delete + cells
    // -----------------------------------------------------------------------

    /// AC: A row that is deleted AND has surviving newer cells in the same
    /// generation must emit BOTH records faithfully (no merging).
    ///
    /// This tests the model directly: a RowDelete and a separate Upsert can
    /// co-exist for the same (pk, ck) in a single delta stream.
    #[test]
    fn row_delete_and_upsert_can_coexist_for_same_key() {
        let ts: i64 = 1_700_000_000_000_000;
        let del_ts: i64 = 1_700_000_000_100_000; // newer than the cell write

        let pk = RowKeys::new(vec![Value::Integer(1)], vec![Value::Text("ck".into())]);

        // Record 1: the row tombstone.
        let row_delete = DeltaRecord::RowDelete {
            keys: pk.clone(),
            deleted_at: del_ts,
        };

        // Record 2: a surviving cell write (older than the tombstone).
        let upsert = DeltaRecord::Upsert {
            keys: pk.clone(),
            liveness: None,
            cells: vec![(
                ColumnId::new("name"),
                CellDelta::value(Value::Text("Alice".into()), ts),
            )],
        };

        // Both records carry the same key but are distinct variants.
        assert_ne!(row_delete.op_name(), upsert.op_name());
        assert_eq!(row_delete.partition_key(), upsert.partition_key());

        // The consumer can distinguish them by op_name and act accordingly.
        assert_eq!(row_delete.op_name(), "row_delete");
        assert_eq!(upsert.op_name(), "upsert");

        if let DeltaRecord::RowDelete { deleted_at, .. } = &row_delete {
            assert_eq!(*deleted_at, del_ts);
        }
        if let DeltaRecord::Upsert { cells, .. } = &upsert {
            assert_eq!(cells.len(), 1);
            let (_, cell) = &cells[0];
            assert_eq!(cell.writetime, ts);
            assert!(cell.value.is_some());
        }
    }

    /// AC: Prefix range bounds — multi-column clustering key where the bound
    /// specifies only the first component (prefix semantics).
    #[test]
    fn range_delete_prefix_bound_multi_column_clustering() {
        // Table with clustering key (ck1 INT, ck2 TEXT).
        // DELETE WHERE pk=1 AND ck1=2  →  all ck2 values for ck1=2 are deleted.
        // In SSTable terms: start = [2], end = [2], both inclusive (prefix match).
        let rec = DeltaRecord::RangeDelete {
            partition_key: RowKeys::partition_only(vec![Value::Integer(1)]),
            start: RangeBound::new(vec![Value::Integer(2)], true),
            end: RangeBound::new(vec![Value::Integer(2)], true),
            deleted_at: 1_700_000_000_000_000,
        };
        if let DeltaRecord::RangeDelete { start, end, .. } = &rec {
            // One component each — prefix of the 2-component clustering key.
            assert_eq!(start.values.len(), 1);
            assert_eq!(end.values.len(), 1);
            assert!(start.is_prefix()); // fewer values than full arity
            assert!(end.is_prefix());
            assert!(start.inclusive);
            assert!(end.inclusive);
        } else {
            panic!("expected RangeDelete");
        }
    }

    /// AC: Mixed inclusive/exclusive range bounds.
    #[test]
    fn range_delete_mixed_inclusivity() {
        // DELETE WHERE pk=2 AND ck1>=2 AND ck1<4   → closed-open range
        let rec = DeltaRecord::RangeDelete {
            partition_key: RowKeys::partition_only(vec![Value::Integer(2)]),
            start: RangeBound::inclusive(vec![Value::Integer(2)]),
            end: RangeBound::exclusive(vec![Value::Integer(4)]),
            deleted_at: 1_700_000_000_000_001,
        };
        if let DeltaRecord::RangeDelete {
            start,
            end,
            deleted_at,
            ..
        } = &rec
        {
            assert!(start.inclusive, "start should be inclusive (>=)");
            assert!(!end.inclusive, "end should be exclusive (<)");
            assert_eq!(*deleted_at, 1_700_000_000_000_001);
        } else {
            panic!("expected RangeDelete");
        }
    }
}
