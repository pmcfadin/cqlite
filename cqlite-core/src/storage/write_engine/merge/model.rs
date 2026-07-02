//! Merge data model — the entities that flow through the k-way merge stream.
//!
//! Extracted from `merge.rs` (issue #945) as a pure, behavior-preserving
//! relocation: `MergeEntry`, `RowData`, `CellData`, `ComplexDeletion`,
//! `MergeStep`, and `MergeStats` plus their builders. The parent `merge`
//! module re-exports every item, so external `write_engine::merge::MergeEntry`
//! paths keep resolving unchanged. No logic moved here — see `mod.rs` for the
//! merge machinery and `reconcile.rs` for the reconciliation kernel.

#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey, RangeTombstone};
#[cfg(feature = "write-support")]
use crate::types::Value;

#[cfg(feature = "write-support")]
use std::cmp::Ordering;
#[cfg(feature = "write-support")]
use std::path::PathBuf;
#[cfg(feature = "write-support")]
use std::time::Duration;

/// Entry in the merge stream
///
/// Represents a single row from one of the input SSTables. This is the
/// fundamental unit that flows through the merge heap.
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeEntry {
    /// Which SSTable this came from (0 = newest)
    pub run_index: usize,
    /// Partition key with token
    pub key: DecoratedKey,
    /// Clustering key (None for tables without clustering)
    pub clustering_key: Option<ClusteringKey>,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp: i64,
    /// Row data (live cells or tombstone)
    pub row_data: RowData,
    /// Complex (collection / UDT) deletion markers for the multi-cell columns
    /// of this `(pk, ck)` (issue #886 substrate).
    ///
    /// Carried through the merge so the per-cell-path collection/UDT followup
    /// (#844) and shadow-before-purge (#887) can preserve collection/UDT
    /// deletion timestamps. `reconcile_cluster` unions and preserves these
    /// across a cluster, but reconciliation does **not yet consult** them and
    /// the writer does not yet apply them — defaults to empty, so output is
    /// byte-unchanged. Population (per-element reader emit) lands in #899.
    pub complex_deletions: Vec<ComplexDeletion>,
    /// Range-deletion marker covering a span of clustering keys (issue #886
    /// substrate).
    ///
    /// A first-class slot so range tombstones can flow through the merge stream
    /// instead of being skipped by the parser; applying them to shadow covered
    /// cells is the follow-up #846. Carried (and timestamp-max-preserved)
    /// through `reconcile_cluster` but **not yet consulted** by reconciliation
    /// or the writer, so output is byte-unchanged. `None` when this entry
    /// carries no range deletion.
    pub range_deletion: Option<RangeTombstone>,
    /// Row-level deletion that COEXISTS with this entry's surviving live cells
    /// (issue #932).
    ///
    /// `Some((deletion_time_micros, local_deletion_time_secs))` when a row
    /// tombstone whose timestamp is OLDER than the surviving cells must be kept
    /// alongside `RowData::Live`. `RowData::Live` has no row-deletion slot, so —
    /// mirroring the carry-only `complex_deletions` / `range_deletion` fields —
    /// the coexisting deletion rides here instead of forcing a `RowData` change
    /// across ~100 `RowData::Live` sites. `reconcile_cluster` folds it into the
    /// winning row deletion and re-attaches it to the emitted live entry;
    /// `merge_entry_to_mutation` emits it as `Mutation::row_tombstone`. Without
    /// this, a partial compaction that keeps newer cells would DROP the row
    /// deletion and let older cells of other columns (in SSTables not part of the
    /// compaction) resurrect. `None` for a row with no coexisting deletion.
    pub row_deletion: Option<(i64, i32)>,
    /// Partition-level deletion `(markedForDeleteAt µs, localDeletionTime s)`
    /// carried by a synthetic partition-tombstone carrier entry (issue #1072).
    ///
    /// `Some(..)` ONLY on the carrier entry the reader emits for a partition
    /// header that has a tombstone (empty `RowData::Live`, `clustering_key:
    /// None`). The merge extracts the MAX `markedForDeleteAt` across sources,
    /// applies it as the OUTERMOST per-cell `<=` shadow floor for the whole
    /// partition, and re-emits the surviving tombstone via
    /// `merge_entry_to_mutation` (→ `Mutation::partition_tombstone`). Without
    /// this a newer partition tombstone in one SSTable failed to shadow older
    /// live rows in another, resurrecting the deleted partition. `None` for every
    /// non-carrier entry.
    pub partition_deletion: Option<(i64, i32)>,
}

impl MergeEntry {
    /// Create a new merge entry.
    ///
    /// The carry-only #886 substrate fields (`complex_deletions`,
    /// `range_deletion`) default to empty/`None`; attach them with
    /// [`with_complex_deletions`](Self::with_complex_deletions) /
    /// [`with_range_deletion`](Self::with_range_deletion) once the reader emit
    /// surfaces them (#899).
    pub fn new(
        run_index: usize,
        key: DecoratedKey,
        clustering_key: Option<ClusteringKey>,
        timestamp: i64,
        row_data: RowData,
    ) -> Self {
        Self {
            run_index,
            key,
            clustering_key,
            timestamp,
            row_data,
            complex_deletions: Vec::new(),
            range_deletion: None,
            row_deletion: None,
            partition_deletion: None,
        }
    }

    /// Attach complex-deletion markers (issue #886 substrate; carry-only).
    #[must_use]
    pub fn with_complex_deletions(mut self, complex_deletions: Vec<ComplexDeletion>) -> Self {
        self.complex_deletions = complex_deletions;
        self
    }

    /// Attach a coexisting row-level deletion (issue #932).
    ///
    /// `deletion_time` is `markedForDeleteAt` (microseconds); `ldt` is the
    /// `localDeletionTime` (GC-clock seconds, `0` = not surfaced). Used when a
    /// row's surviving cells are newer than a row tombstone that must still be
    /// preserved so it keeps shadowing older cells in non-compacted SSTables.
    #[must_use]
    pub fn with_row_deletion(mut self, deletion_time: i64, ldt: i32) -> Self {
        self.row_deletion = Some((deletion_time, ldt));
        self
    }

    /// Attach a range-deletion marker (issue #886 substrate; carry-only).
    #[must_use]
    pub fn with_range_deletion(mut self, range_deletion: RangeTombstone) -> Self {
        self.range_deletion = Some(range_deletion);
        self
    }

    /// Attach a partition-level deletion (issue #1072).
    ///
    /// `deletion_time` is `markedForDeleteAt` (microseconds); `ldt` is the
    /// `localDeletionTime` (GC-clock seconds). Marks this entry as the synthetic
    /// partition-tombstone carrier so the merge applies the partition floor and
    /// re-emits the surviving tombstone.
    #[must_use]
    pub fn with_partition_deletion(mut self, partition_deletion: (i64, i32)) -> Self {
        self.partition_deletion = Some(partition_deletion);
        self
    }

    /// True when this entry has NO writer-emittable content at all — an empty
    /// `RowData::Live { cells: vec![] }` with no row tombstone and no carried
    /// deletion metadata. Routing such an entry to the writer would create a
    /// PHANTOM live empty (pure-PK) row at timestamp 0 (`DataWriter::merge_row_group`
    /// treats a no-op mutation as a primary-key insert), so `merge` filters it.
    ///
    /// Epic #899 Phase C: a COMPLEX DELETION is consumed by the writer (a real
    /// `CellOperation::ComplexDeletion` marker), so a complex-deletion-only entry
    /// is NOT a no-op.
    ///
    /// Issue #933: a RANGE DELETION is now ALSO consumed by the compaction writer —
    /// `merge_entry_to_mutation` threads it onto the mutation's `range_tombstones`,
    /// which the writer interleaves as on-disk bound markers AND uses to shadow
    /// covered same-partition rows. A range-deletion-only carrier is therefore NO
    /// LONGER a no-op and MUST reach the writer (dropping it would resurrect the
    /// covered rows it shadowed — roborev #959 High #2). In practice
    /// `reconcile_cluster` never emits a truly-empty entry (one with empty cells,
    /// no row tombstone, and no carried metadata reconciles to `None`), so this
    /// guard is now effectively unreachable but kept as a defensive phantom-row
    /// filter.
    #[must_use]
    pub fn is_metadata_only_no_op(&self) -> bool {
        matches!(&self.row_data, RowData::Live { cells } if cells.is_empty())
            && self.complex_deletions.is_empty()
            && self.range_deletion.is_none()
            && self.row_deletion.is_none()
            && self.partition_deletion.is_none()
    }

    /// True when this entry is a synthetic partition-tombstone carrier (issue
    /// #1072): an empty live row whose only payload is `partition_deletion`.
    /// Produced by [`Self::build_merge_entry`] from a reader `PartitionDelete`.
    /// Such an entry produces NO output row (it yields a
    /// `Mutation::partition_tombstone`, not a clustering row), so the merge must
    /// not count it toward output row counts — but it MUST reach the writer.
    #[must_use]
    pub fn is_partition_delete_carrier(&self) -> bool {
        self.partition_deletion.is_some()
    }
}

/// Ord implementation for min-heap routing ONLY (not LWW winner selection).
///
/// This orders entries so the heap yields them grouped by partition and
/// clustering key. The actual equal-timestamp Delete-vs-Live winner is chosen
/// in `merge_partition_rows` (timestamp → liveness → run_index), NOT here.
///
/// Order by:
/// 1. Token (ascending)
/// 2. Key bytes (ascending, for hash collisions)
/// 3. Clustering key (ascending, schema-aware)
/// 4. Run index (ascending) - stable routing tiebreak only
#[cfg(feature = "write-support")]
impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary: by token
        match self.key.token.cmp(&other.key.token) {
            Ordering::Equal => {
                // Secondary: by key bytes (hash collision resolution)
                match self.key.key.cmp(&other.key.key) {
                    Ordering::Equal => {
                        // Tertiary: by clustering key
                        match (&self.clustering_key, &other.clustering_key) {
                            (None, None) => {
                                // Quaternary: by run_index (lower = newer)
                                self.run_index.cmp(&other.run_index)
                            }
                            (None, Some(_)) => Ordering::Less,
                            (Some(_), None) => Ordering::Greater,
                            (Some(a), Some(b)) => {
                                // Use fallback Ord (not schema-aware at this level)
                                // Schema-aware comparison happens during partition merge
                                match a.cmp(b) {
                                    Ordering::Equal => {
                                        // Equal clustering keys: prefer lower run_index
                                        self.run_index.cmp(&other.run_index)
                                    }
                                    other_ord => other_ord,
                                }
                            }
                        }
                    }
                    other_ord => other_ord,
                }
            }
            other_ord => other_ord,
        }
    }
}

#[cfg(feature = "write-support")]
impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Row data: live cells or tombstone
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowData {
    /// Live row with cell data
    Live {
        /// Cell data for this row
        cells: Vec<CellData>,
    },
    /// Row tombstone
    Tombstone {
        /// Deletion timestamp (microseconds)
        deletion_time: i64,
        /// Local deletion time (seconds since epoch)
        local_deletion_time: i32,
    },
}

/// Cell data with timestamp, optional TTL, and (for complex columns) cell path.
///
/// ## Per-cell merge metadata (issue #886 — byte-parity foundation)
///
/// To reconcile per-cell and per-element data byte-faithfully (Cassandra
/// `Cells#reconcile`), the merge entry must carry more than a single row-level
/// timestamp. The fields below thread that richer state from the reader toward
/// the followup behaviors (#844 per-cell-path collection/UDT merge, #848
/// tombstone-vs-expiring TTL tie-break). They are **carried but not yet acted
/// on** by reconciliation — this struct change is plumbing only and must not
/// alter output bytes.
///
/// Where the reader does not yet surface a value the field is left `None`; the
/// dependent issues fill it in once the reader is extended.
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellData {
    /// Column name
    pub column: String,
    /// Column value
    pub value: Value,
    /// Cell timestamp (microseconds)
    pub timestamp: i64,
    /// TTL in seconds (None = no expiration)
    pub ttl: Option<u32>,
    /// Cell path for a complex (collection / non-frozen UDT) element — the
    /// serialized element key/index that distinguishes one element of a
    /// multi-cell column from another (issue #886 substrate).
    ///
    /// **Carry-only.** This field is threaded through the merge entry so that
    /// per-element reconciliation can become byte-faithful, but nothing
    /// populates or consumes it yet: the reader still collapses collections to
    /// a single whole-column [`CellData`] and the writer does not read this
    /// field. Population (per-element reader emit) and consumption (per-path
    /// merge #844) land in the follow-up #899. `None` for simple cells.
    pub cell_path: Option<Vec<u8>>,
    /// Local deletion time in **seconds** since the Unix epoch for this cell
    /// (the on-disk `localDeletionTime`), used by gc_grace purging and
    /// expiring-cell tie-breaks (issue #886 substrate).
    ///
    /// For an expiring (TTL) cell this is the cell's expiration instant; for a
    /// cell tombstone it is when the delete was applied.
    ///
    /// Populated for complex (collection / UDT) elements on the compaction read
    /// path (epic #899, Phase C); `None` for simple cells whose LDT the reader
    /// does not surface, and for live simple cells.
    pub local_deletion_time: Option<i32>,
    /// True when this `CellData` represents a single ELEMENT of a non-frozen
    /// complex column (a list/set member or a map entry), as opposed to a simple
    /// single-cell column (epic #899, Phase C).
    ///
    /// When `true`, [`cell_path`](Self::cell_path) is the element's authoritative
    /// on-disk path and the merge→mutation step emits a
    /// [`CellOperation::WriteComplexElement`] (preserving per-element
    /// ts/ttl/ldt/path) rather than a whole-column `Write`. `false` for every
    /// simple cell (whole-column collapse no longer happens on the production
    /// path).
    ///
    /// [`CellOperation::WriteComplexElement`]: crate::storage::write_engine::mutation::CellOperation::WriteComplexElement
    pub is_complex_element: bool,
    /// Authoritative IS_DELETED (0x01) flag for a complex element (epic #899,
    /// Phase C). Carried verbatim from [`ComplexElement::is_deleted`] — NOT
    /// re-derived from value/ttl/ldt shape, so an expiring SET member (empty
    /// value, ttl + ldt set) is correctly NOT treated as a tombstone
    /// (no-heuristics mandate). Always `false` for simple cells (a simple cell
    /// tombstone rides in [`value`](Self::value) as `Value::Tombstone`).
    ///
    /// [`ComplexElement::is_deleted`]: crate::storage::sstable::reader::compaction_row::ComplexElement::is_deleted
    pub is_deleted: bool,
    /// On-disk HAS_EMPTY_VALUE (0x04) flag for a complex element (epic #899,
    /// Phase C). `true` for a SET member (whose identity lives in the cell_path)
    /// and any genuinely empty-value element, so the writer reproduces the same
    /// on-disk emptiness rather than re-deriving it from the decoded value.
    /// Always `false` for simple cells.
    pub has_empty_value: bool,
}

#[cfg(feature = "write-support")]
impl CellData {
    /// Construct a simple live cell with no TTL, local-deletion-time, or cell
    /// path. The richer fields default to `None`; populate them explicitly when
    /// the reader supplies them (issues #844 / #848).
    pub fn new(column: String, value: Value, timestamp: i64) -> Self {
        Self {
            column,
            value,
            timestamp,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }
    }
}

/// Complex (collection / non-frozen UDT) deletion marker for one column
/// (issue #886 substrate).
///
/// Cassandra writes a complex-deletion marker ahead of a multi-cell column's
/// elements to delete every element written at or before `marked_for_delete_at`.
/// A merged complex deletion is dropped unless it **strictly supersedes** the
/// active one (Cassandra commit `bd244649`). CQLite currently reduces this to a
/// boolean and discards the timestamps; this first-class entity preserves them
/// so per-path merge (#844) and shadow-before-purge (#887) can be byte-faithful.
///
/// **Carry-only.** Carried on [`MergeEntry`] (unioned through
/// `reconcile_cluster`) but not yet populated by the reader or applied during
/// the merge — that is the follow-up #899/#887.
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComplexDeletion {
    /// Name of the complex column this deletion covers.
    pub column: String,
    /// Deletion timestamp (`markedForDeleteAt`) in microseconds since the epoch.
    pub marked_for_delete_at: i64,
    /// Local deletion time in seconds since the epoch.
    pub local_deletion_time: i32,
}

/// Result of a merge step (incremental merge)
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub enum MergeStep {
    /// Merged partition with all its rows
    Partition {
        /// Partition key
        key: DecoratedKey,
        /// All rows in this partition (already merged)
        rows: Vec<MergeEntry>,
    },
    /// Merge is complete
    Complete,
}

/// Statistics collected during merge
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
pub struct MergeStats {
    /// Number of input files
    pub input_files: usize,
    /// Number of output partitions
    pub output_partitions: u64,
    /// Number of output rows
    pub output_rows: u64,
    /// Bytes written to output
    pub bytes_written: u64,
    /// Elapsed time
    pub elapsed: Duration,
    /// SSTables DROPPED WHOLE by the fully-expired fast path (issue #1388),
    /// distinct from the merged inputs: each of these was proven fully expired by
    /// authoritative `Statistics.db` metadata (`max_deletion_time < gcBefore`) and
    /// overlap-safe, so it was EXCLUDED from the K-way merger's input list (never
    /// read/decoded) and its components are reclaimed after the output publishes.
    /// Empty for a compaction that drops nothing (byte-identical to pre-#1388
    /// behavior). Paths are input Data.db paths.
    pub dropped_whole: Vec<PathBuf>,
}
