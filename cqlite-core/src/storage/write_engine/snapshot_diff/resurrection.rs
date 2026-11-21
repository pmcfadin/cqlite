//! The resurrection-risk predicates (issue #4201, spec R4 / R4A / R5).
//!
//! Every rule here is derived from Cassandra at the pinned `cassandra-5.0.8` tag
//! (`git show cassandra-5.0.8:<path>`), never from CQLite's own behaviour (#3041):
//!
//! * **Shadow rule** — a cell is deleted by a tombstone iff `timestamp <= markedForDeleteAt`:
//!   `src/java/org/apache/cassandra/db/DeletionTime.java:173-175`
//!   (`deletes(long timestamp) { return timestamp <= markedForDeleteAt(); }`). A tombstone that
//!   ties a live cell's timestamp therefore wins, exactly as cell-vs-cell reconciliation does at
//!   `src/java/org/apache/cassandra/db/rows/Cells.java:92-98` ("Tombstones always win reconciliation
//!   with live cells of the same timstamp").
//! * **Purge boundary** — `gcBefore = nowInSec - gc_grace_seconds`
//!   (`src/java/org/apache/cassandra/db/ColumnFamilyStore.java:1967-1970`), and a tombstone is
//!   purgeable iff `localDeletionTime < gcBefore`, a STRICT `<`
//!   (`src/java/org/apache/cassandra/db/partitions/PurgeFunction.java:41-42`).
//! * **Range span** — a clustering is inside a slice iff `start <= clustering <= end`
//!   (`src/java/org/apache/cassandra/db/Slice.java:181-184`), where an inclusive start bound sorts
//!   before the clusterings it names, an exclusive start bound after them, an inclusive end bound
//!   after them and an exclusive end bound before them
//!   (`src/java/org/apache/cassandra/db/ClusteringPrefix.java:70-79`, `comparedToClustering`).
//!
//! The scopes a tombstone covers follow the trace's own [`TombstoneKind`]: a partition tombstone
//! covers every cell of the partition (static included), a row tombstone the cells of its
//! clustering, a cell tombstone exactly its `(clustering, column)`, a collection tombstone the
//! elements of its `(clustering, column)`, and a range tombstone the cells of every clustering
//! inside its span.
//!
//! Declared gap: a row's liveness marker (the `INSERT` primary-key timestamp) is not a cell in the
//! reconciliation trail, so a row that holds ONLY a liveness marker (no cells) is invisible to the
//! shadow check; a tombstone whose only victim is such a row reports `resurrection_risk: no`.

use std::cmp::Ordering;

use crate::schema::TableSchema;
use crate::storage::write_engine::merge::trace::{CellDecision, TombstoneKind, TombstoneRecord};
use crate::storage::write_engine::mutation::{ClusteringBound, ClusteringKey};
use crate::Result;

/// `DeletionTime.deletes(timestamp)` (`DeletionTime.java:173-175`).
#[inline]
fn deletes(marked_for_delete_at: i64, cell_timestamp: i64) -> bool {
    cell_timestamp <= marked_for_delete_at
}

/// The gc_grace phase of a tombstone at `gc_before` (= `now - gc_grace_seconds`): `past-gc-grace`
/// iff `localDeletionTime < gcBefore` (strict, `PurgeFunction.java:41-42`).
///
/// `local_deletion_time` is the trail's wrapped `i32`; the on-disk value is an unsigned 32-bit
/// GC-clock second count (`DeletionTime.java:98`, `Cell.deletionTimeUnsignedIntegerToLong`).
#[must_use]
pub(super) fn gc_phase(local_deletion_time: i32, gc_before: i64) -> &'static str {
    if i64::from(local_deletion_time as u32) < gc_before {
        "past-gc-grace"
    } else {
        "before-gc-grace"
    }
}

/// Whether `tombstone` (held by one side) shadows `cell` (a reconciled winner of the OTHER side).
pub(super) fn shadows(
    tombstone: &TombstoneRecord,
    cell: &CellDecision,
    schema: &TableSchema,
) -> Result<bool> {
    if !deletes(tombstone.deletion_time, cell.writetime) {
        return Ok(false);
    }
    Ok(match tombstone.kind {
        TombstoneKind::Partition => true,
        TombstoneKind::Row => cell.clustering == tombstone.clustering,
        // A cell tombstone on a collection element covers ONLY that element's path.
        TombstoneKind::Cell => {
            cell.clustering == tombstone.clustering
                && tombstone.column.as_deref() == Some(cell.column.as_str())
                && cell.cell_path == tombstone.cell_path
        }
        // A complex deletion covers every element of its column.
        TombstoneKind::Collection => {
            cell.clustering == tombstone.clustering
                && tombstone.column.as_deref() == Some(cell.column.as_str())
        }
        TombstoneKind::Range => match (
            &cell.clustering,
            &tombstone.range_start,
            &tombstone.range_end,
        ) {
            (Some(ck), Some(start), Some(end)) => range_covers(start, end, ck, schema)?,
            _ => false,
        },
    })
}

/// `start <= ck <= end` for a range tombstone's bounds (`Slice.java:181-184`), comparing
/// schema-aware (per-column ASC/DESC) over the bound's own components so a PREFIX bound covers
/// every extension of it.
fn range_covers(
    start: &ClusteringBound,
    end: &ClusteringBound,
    ck: &ClusteringKey,
    schema: &TableSchema,
) -> Result<bool> {
    // Compare `ck` to a bound key over the bound's component count only.
    let cmp = |bound: &ClusteringKey| -> Result<Ordering> {
        let truncated = ClusteringKey {
            columns: ck
                .columns
                .iter()
                .take(bound.columns.len())
                .cloned()
                .collect(),
        };
        truncated.compare(bound, schema)
    };
    let after_start = match start {
        // INCL_START_BOUND sorts before the clusterings it names (comparedToClustering -1).
        ClusteringBound::Inclusive(bound) => cmp(bound)? != Ordering::Less,
        // EXCL_START_BOUND sorts after them (+1): a clustering equal on the prefix is excluded.
        ClusteringBound::Exclusive(bound) => cmp(bound)? == Ordering::Greater,
        ClusteringBound::Bottom => true,
        ClusteringBound::Top => false,
    };
    let before_end = match end {
        // INCL_END_BOUND sorts after the clusterings it names (+1).
        ClusteringBound::Inclusive(bound) => cmp(bound)? != Ordering::Greater,
        // EXCL_END_BOUND sorts before them (-1).
        ClusteringBound::Exclusive(bound) => cmp(bound)? == Ordering::Less,
        ClusteringBound::Top => true,
        ClusteringBound::Bottom => false,
    };
    Ok(after_start && before_end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_phase_is_strict_less_than() {
        // localDeletionTime == gcBefore is NOT purgeable yet (PurgeFunction.java:41-42).
        assert_eq!(gc_phase(100, 100), "before-gc-grace");
        assert_eq!(gc_phase(100, 101), "past-gc-grace");
        assert_eq!(gc_phase(100, 99), "before-gc-grace");
    }

    #[test]
    fn gc_phase_reads_the_wrapped_i32_as_unsigned() {
        // A far-future unsigned ldt in [2^31, 2^32) is stored wrapped negative; it is NOT in the past.
        let far_future = (3_000_000_000_u32) as i32;
        assert!(far_future < 0);
        assert_eq!(gc_phase(far_future, 2_000_000_000), "before-gc-grace");
    }

    #[test]
    fn deletes_ties_go_to_the_tombstone() {
        assert!(deletes(2000, 2000));
        assert!(deletes(2000, 1999));
        assert!(!deletes(2000, 2001));
    }
}
