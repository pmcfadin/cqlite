//! Shared writer value types: merged-op records, complex-element descriptors, the assembled row, and the public PartitionEmitCounts.
//!
//! Part of the `data_writer` responsibility split (issue #1118). `use super::*`
//! provides the crate imports and sibling helpers re-exported from
//! `data_writer/mod.rs`. No emitted bytes change.

use super::*;

/// A surviving cell operation in a merged row, tagged with the timestamp and
/// row-level TTL of the mutation it came from.
///
/// Epic #899 (Phase B): for whole-column ops (`Write`/`WriteWithTtl`/`Delete`)
/// `timestamp_micros` is the originating mutation's row timestamp. For the
/// per-element complex ops (`WriteComplexElement`/`ComplexDeletion`) the
/// element's OWN timestamp/ttl/ldt/cell_path live INSIDE the op itself; the
/// `timestamp_micros` field still carries the originating mutation's row
/// timestamp so the writer can decide `USE_ROW_TIMESTAMP` vs an explicit delta
/// per element.
pub(crate) struct MergedOp<'a> {
    pub(crate) op: &'a crate::storage::write_engine::mutation::CellOperation,
    pub(crate) timestamp_micros: i64,
    /// Row-level TTL (`Mutation::ttl_seconds`) of the originating mutation.
    /// Per-cell TTL lives inside `CellOperation::WriteWithTtl` itself.
    pub(crate) row_ttl_seconds: Option<u32>,
    /// Local deletion time (seconds since epoch) for a `Delete` cell tombstone,
    /// honoring the originating mutation's explicit `local_deletion_time` when
    /// present (Issue #764). Derived from the timestamp otherwise.
    pub(crate) cell_local_deletion_time: i32,
}

/// One element to emit inside a per-element complex column (epic #899, Phase B).
///
/// Carries the element's OWN write metadata and its PRESERVED source cell path
/// (never regenerated). `value == None` with `is_deleted` true is an
/// element-level tombstone; `value == None` without `is_deleted` is an
/// empty-value element (e.g. a SET member). The writer stamps each element with
/// `USE_ROW_TIMESTAMP` only when `timestamp_micros` equals the row timestamp,
/// otherwise it clears the flag and writes an explicit unsigned delta.
#[derive(Debug, Clone)]
pub(crate) struct ComplexElementWrite {
    pub(crate) cell_path: Vec<u8>,
    pub(crate) value: Option<Value>,
    pub(crate) timestamp_micros: i64,
    pub(crate) ttl_seconds: Option<u32>,
    pub(crate) local_deletion_time: Option<i32>,
    pub(crate) is_deleted: bool,
}

/// One complex column's reconciled contents while grouping per-element ops:
/// `(optional complex deletion (markedForDeleteAt µs, localDeletionTime s),
/// surviving elements)` (epic #899, Phase B).
pub(crate) type ComplexColumnGroup = (Option<(i64, i32)>, Vec<ComplexElementWrite>);

/// A surviving static-column operation, tagged with the timestamp and explicit
/// local deletion time of the mutation it came from.
///
/// Issue #764: static-column tombstones must be stamped with their ORIGINATING
/// mutation's `local_deletion_time` (and timestamp), not a single synthetic
/// value taken from the newest static-contributing mutation. A surviving delete
/// from an older mutation otherwise inherits the wrong LDT — corrupting the
/// unsigned-VInt delta when stats were seeded from that older delete's explicit
/// (lower) LDT.
pub(crate) struct StaticMergedOp {
    pub(crate) op: crate::storage::write_engine::mutation::CellOperation,
    /// Timestamp (µs) of the originating mutation.
    pub(crate) timestamp_micros: i64,
    /// Local deletion time (s) for a `Delete` tombstone, honoring the
    /// originating mutation's explicit `local_deletion_time` when set.
    pub(crate) cell_local_deletion_time: i32,
    /// Statement-level TTL (`Mutation::ttl_seconds`) of the originating mutation
    /// (issue #1196 regression fix). Cassandra encodes a static `USING TTL`
    /// write as an EXPIRING CELL (per-cell TTL), never as row-level liveness on
    /// the static block. A plain `Write` carrying this TTL must therefore be
    /// emitted via `write_cell_with_ttl`, not as a non-expiring cell; a per-cell
    /// `WriteWithTtl` keeps its own TTL (which takes precedence, matching the
    /// regular-row path where per-cell TTL is honored verbatim).
    pub(crate) row_ttl_seconds: Option<u32>,
}

/// The exact rows and cells `DataWriter` emitted to Data.db for one partition.
///
/// Issue #851: Statistics' `totalRows` (`row_count`) and `totalColumnsSet`
/// (`column_count`) MUST equal what is physically written. Rather than
/// re-deriving the counts from the raw mutations in a parallel loop (which kept
/// diverging from the emitter — rejected commit `5afce78c`), the emission code
/// is the single source of truth: it tallies a row whenever it writes a row
/// (static prelude or merged clustering row) and tallies cells from the same
/// reconciled `ops` it serializes. The empty static-row prelude and range
/// tombstone markers write no `Row`, so they contribute nothing — matching
/// Cassandra `Row.isEmpty()` / `Row.columnCount()`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PartitionEmitCounts {
    /// Rows physically written to Data.db (static prelude + merged clustering
    /// rows). Excludes the empty static prelude and range tombstone markers.
    pub rows: u64,
    /// Regular + static cells physically written. Primary-key (partition +
    /// clustering) columns are encoded positionally and never counted; row
    /// tombstones (`DeleteRow`) set no columns.
    pub columns: u64,
}

/// One Data.db row assembled by merging every mutation of a partition that
/// shares the same clustering key (Issues #716/#717: a partition must never
/// contain two rows with equal clustering).
pub(crate) struct RowWrite<'a> {
    pub(crate) clustering_key: Option<&'a crate::storage::write_engine::mutation::ClusteringKey>,
    /// Primary-key liveness timestamp. `None` for pure row tombstones —
    /// Cassandra serializes those without HAS_TIMESTAMP.
    pub(crate) liveness_ts: Option<i64>,
    /// Row-level TTL from the liveness-providing mutation.
    pub(crate) ttl_seconds: Option<u32>,
    /// Row deletion as (marked_for_delete_at µs, local_deletion_time s).
    pub(crate) row_deletion: Option<(i64, i32)>,
    /// Surviving WHOLE-COLUMN cell operations (already reconciled, unsorted).
    /// `Write`/`WriteWithTtl`/`Delete` — one per column (last-write-wins).
    pub(crate) ops: Vec<MergedOp<'a>>,
    /// Surviving PER-ELEMENT complex ops (epic #899, Phase B):
    /// `WriteComplexElement` + `ComplexDeletion`. These are NOT deduped per
    /// column (a column has many elements) and are written via
    /// `write_complex_column_per_element`. Empty for every existing scenario —
    /// the real pipeline (`merge_entry_to_mutation`) does not yet emit these
    /// ops, so this stays empty until Phase C (keeping output byte-neutral).
    pub(crate) complex_element_ops: Vec<MergedOp<'a>>,
}
