//! Partition-scoped tombstone-carrier pre-scan (issue #1668, stage 1).
//!
//! `merge_partition_rows` (in `mod.rs`) buffers a whole partition and, in a
//! single pass, splits three PARTITION-LEVEL markers out of the per-`(pk, ck)`
//! row stream before per-cluster reconciliation:
//!
//!   * **range-tombstone carriers** (issue #933) — empty live rows carrying a
//!     [`RangeTombstone`] spanning a clustering range;
//!   * **partition-deletion carriers** (issue #1072) — synthetic empty live
//!     rows whose only payload is `partition_deletion`. The MAX
//!     `markedForDeleteAt` across sources is the partition's outermost shadow
//!     floor.
//!
//! Per the #933/#846/#959 correctness invariants, these carriers must be
//! collected across the WHOLE partition BEFORE per-cluster shadowing
//! (`apply_range_shadowing` / `apply_partition_shadowing`) can run — a single
//! cluster cannot be shadowed until the full coalesced range set is known.
//!
//! This module extracts that carrier collection into a standalone, pure,
//! independently testable pre-scan ([`scan_partition_carriers`]). It is a
//! behavior-preserving refactor: the whole-partition buffering path in
//! `merge_partition_rows` is unchanged and still the only production path — it
//! now calls this helper for the carrier set, then buffers the remaining
//! (non-carrier) rows exactly as before. This pre-scan is the seed for the
//! streaming second pass in later stages (#1668 stages 2+), which will apply
//! per-cluster shadowing over the coalesced carriers without re-buffering the
//! whole partition.

#[cfg(feature = "write-support")]
use super::model::{MergeEntry, RowData};
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{DecoratedKey, RangeTombstone};

/// The partition-scoped tombstone markers extracted from a partition's buffered
/// merge entries by [`scan_partition_carriers`].
///
/// These are the markers that span a clustering range or the whole partition —
/// they are NOT per-`(pk, ck)` rows and are kept OUT of the clustered-row
/// stream. `merge_partition_rows` coalesces `range_tombstones`, applies
/// `max_partition_deletion` as the outermost per-cell shadow floor, and
/// re-emits the survivors so the writer persists the markers.
#[cfg(feature = "write-support")]
#[derive(Debug, Default)]
pub(super) struct PartitionCarriers {
    /// Range-tombstone carriers, one `(key, range)` per input marker, in
    /// first-seen (heap) order. Coalesced into a non-overlapping canonical
    /// sequence by the caller (issue #933 / #959).
    pub range_tombstones: Vec<(DecoratedKey, RangeTombstone)>,
    /// MAX partition deletion `(markedForDeleteAt µs, localDeletionTime s)`
    /// across all sources — the partition's outermost shadow floor (issue
    /// #1072). `None` when no partition-deletion carrier is present.
    pub max_partition_deletion: Option<(i64, i32)>,
    /// The [`DecoratedKey`] of the first-seen partition-deletion carrier, kept
    /// so the surviving partition tombstone can be re-emitted (issue #1072).
    /// `None` when no partition-deletion carrier is present.
    pub partition_delete_key: Option<DecoratedKey>,
}

/// True when this entry is a range-tombstone carrier (issue #933): an empty
/// live row whose only payload is a [`RangeTombstone`] spanning a clustering
/// range — no cells, no row deletion, no complex deletions. Such a marker is
/// partition-level (spans many `(pk, ck)`), NOT a single reconcilable row, so
/// it must be split out of the clustered-row stream.
#[cfg(feature = "write-support")]
pub(super) fn is_range_marker_carrier(entry: &MergeEntry) -> bool {
    entry.range_deletion.is_some()
        && entry.complex_deletions.is_empty()
        && entry.row_deletion.is_none()
        && matches!(&entry.row_data, RowData::Live { cells } if cells.is_empty())
}

/// Pre-scan a partition's buffered merge entries and extract the
/// partition-scoped tombstone carriers ([`PartitionCarriers`]).
///
/// This is a light, read-only first pass that touches ONLY the (typically few)
/// range/partition-tombstone carriers — it borrows `rows` and does not consume
/// them, so `merge_partition_rows` can still buffer the non-carrier rows into
/// `clustered_rows` afterward. It reproduces exactly the carrier-splitting logic
/// that previously lived inline in `merge_partition_rows`:
///
///   * A partition-deletion carrier (checked FIRST, matching the original
///     precedence) contributes its `(mfda, ldt)` to `max_partition_deletion`,
///     keeping the MAX `markedForDeleteAt` (ties keep the first-seen), and
///     records the first-seen key in `partition_delete_key`.
///   * Otherwise a range-marker carrier ([`is_range_marker_carrier`])
///     contributes its `(key, range)` to `range_tombstones` in first-seen order.
///   * Every other entry is a normal `(pk, ck)` row and is ignored here (the
///     caller buffers it into `clustered_rows`).
#[cfg(feature = "write-support")]
pub(super) fn scan_partition_carriers(rows: &[MergeEntry]) -> PartitionCarriers {
    let mut carriers = PartitionCarriers::default();

    for row in rows {
        // Partition-deletion carriers take precedence over range markers,
        // exactly as the original inline pass did.
        if row.is_partition_delete_carrier() {
            if let Some((mfda, ldt)) = row.partition_deletion {
                carriers
                    .partition_delete_key
                    .get_or_insert_with(|| row.key.clone());
                match carriers.max_partition_deletion {
                    Some((cur_mfda, _)) if cur_mfda >= mfda => {}
                    _ => carriers.max_partition_deletion = Some((mfda, ldt)),
                }
                continue;
            }
        }
        if is_range_marker_carrier(row) {
            if let Some(rt) = row.range_deletion.clone() {
                carriers.range_tombstones.push((row.key.clone(), rt));
            }
        }
    }

    carriers
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::merge::CellData;
    use crate::storage::write_engine::mutation::ClusteringBound;
    use crate::types::Value;

    fn key(token: i64) -> DecoratedKey {
        DecoratedKey::new(token, vec![token as u8])
    }

    fn range_tombstone(deletion_time: i64, local_deletion_time: i32) -> RangeTombstone {
        RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time,
            local_deletion_time,
        }
    }

    /// An ordinary live `(pk, ck)` row — must be ignored by the pre-scan.
    fn live_row(token: i64) -> MergeEntry {
        MergeEntry::new(
            0,
            key(token),
            None,
            100,
            RowData::Live {
                cells: vec![CellData::new("c".to_string(), Value::Integer(1), 100)],
            },
        )
    }

    /// A range-tombstone carrier: empty live row + a range deletion.
    fn range_carrier(token: i64, deletion_time: i64, ldt: i32) -> MergeEntry {
        MergeEntry::new(
            0,
            key(token),
            None,
            deletion_time,
            RowData::Live { cells: Vec::new() },
        )
        .with_range_deletion(range_tombstone(deletion_time, ldt))
    }

    /// A partition-deletion carrier: empty live row + a partition deletion.
    fn partition_carrier(token: i64, mfda: i64, ldt: i32) -> MergeEntry {
        MergeEntry::new(
            0,
            key(token),
            None,
            mfda,
            RowData::Live { cells: Vec::new() },
        )
        .with_partition_deletion((mfda, ldt))
    }

    #[test]
    fn empty_input_yields_no_carriers() {
        let carriers = scan_partition_carriers(&[]);
        assert!(carriers.range_tombstones.is_empty());
        assert_eq!(carriers.max_partition_deletion, None);
        assert!(carriers.partition_delete_key.is_none());
    }

    #[test]
    fn plain_rows_yield_no_carriers() {
        let rows = vec![live_row(1), live_row(1), live_row(1)];
        let carriers = scan_partition_carriers(&rows);
        assert!(carriers.range_tombstones.is_empty());
        assert_eq!(carriers.max_partition_deletion, None);
        assert!(carriers.partition_delete_key.is_none());
    }

    #[test]
    fn extracts_range_carriers_in_first_seen_order() {
        let rows = vec![
            range_carrier(1, 200, 20),
            live_row(1),
            range_carrier(1, 100, 10),
        ];
        let carriers = scan_partition_carriers(&rows);
        assert_eq!(carriers.range_tombstones.len(), 2);
        // First-seen (heap) order preserved: 200 before 100.
        assert_eq!(carriers.range_tombstones[0].1.deletion_time, 200);
        assert_eq!(carriers.range_tombstones[1].1.deletion_time, 100);
        // Range carriers do not populate the partition-deletion floor.
        assert_eq!(carriers.max_partition_deletion, None);
        assert!(carriers.partition_delete_key.is_none());
    }

    #[test]
    fn keeps_max_partition_deletion_across_carriers() {
        // Out-of-order mfda: 50, then 300 (the max), then 200 — max wins.
        let rows = vec![
            partition_carrier(7, 50, 5),
            live_row(7),
            partition_carrier(7, 300, 30),
            partition_carrier(7, 200, 20),
        ];
        let carriers = scan_partition_carriers(&rows);
        assert_eq!(carriers.max_partition_deletion, Some((300, 30)));
        // First-seen carrier's key is retained.
        assert_eq!(carriers.partition_delete_key, Some(key(7)));
        assert!(carriers.range_tombstones.is_empty());
    }

    #[test]
    fn equal_mfda_keeps_first_seen_local_deletion_time() {
        // On an mfda tie the FIRST-seen (mfda, ldt) is kept (>= guard).
        let rows = vec![partition_carrier(3, 100, 11), partition_carrier(3, 100, 22)];
        let carriers = scan_partition_carriers(&rows);
        assert_eq!(carriers.max_partition_deletion, Some((100, 11)));
    }

    #[test]
    fn mixed_carriers_and_rows_split_correctly() {
        let rows = vec![
            live_row(5),
            range_carrier(5, 400, 40),
            partition_carrier(5, 250, 25),
            live_row(5),
            range_carrier(5, 150, 15),
        ];
        let carriers = scan_partition_carriers(&rows);
        assert_eq!(carriers.range_tombstones.len(), 2);
        assert_eq!(carriers.range_tombstones[0].1.deletion_time, 400);
        assert_eq!(carriers.range_tombstones[1].1.deletion_time, 150);
        assert_eq!(carriers.max_partition_deletion, Some((250, 25)));
        assert_eq!(carriers.partition_delete_key, Some(key(5)));
    }

    #[test]
    fn is_range_marker_carrier_rejects_non_empty_live_row() {
        // A row with a range deletion AND live cells is NOT a pure carrier.
        let entry = live_row(1).with_range_deletion(range_tombstone(100, 10));
        assert!(!is_range_marker_carrier(&entry));
        // The pure empty-live carrier is recognized.
        assert!(is_range_marker_carrier(&range_carrier(1, 100, 10)));
    }
}
