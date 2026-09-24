//! Dry, no-write full-compaction-at-`--now` reclaim prediction (issue #4204,
//! `design.md` D1's `reclaim_at_now`).
//!
//! # Scope decision (flagged, issue #4204 implementer report)
//!
//! D1 describes this as reusing "the existing `MergeStats`/purge tally CODE
//! PATH in a dry, no-write invocation". The only write-engine primitive that
//! performs a full cross-generation K-way merge without an attached output
//! writer is [`crate::storage::write_engine::merge::KWayMerger::step`], which
//! requires a full [`crate::schema::TableSchema`] (a hard requirement of
//! `KWayMerger::new*`) — in direct tension with R6's "no `--schema` required for
//! the cheap tier" contract, and pulling the full reconciliation/shadowing
//! machinery into a REPORT-ONLY verb whose stated boundary (`design.md`,
//! "vs. #4200 cqlite tombstones") is explicitly to stay at the CHEAP, per-generation,
//! un-reconciled tier for exactly this kind of count.
//!
//! Instead, this reclaim prediction is a light-weight ESTIMATE folded into the
//! SAME `--deep` scan pass ([`super::deep_scan`]) that already walks every row
//! once, PER GENERATION (`design.md` D5 nests `reclaim_at_now` inside each
//! generation's own `deep` object, not at the table level): `rows_in` is the
//! total rows that generation's scan observed, `tombstones_purged` is the count
//! of tombstone markers whose `local_deletion_time` is a genuinely decoded
//! (non-zero) LDT strictly less than the report's `gcBefore`, and `rows_out` is
//! the simple difference. This is NOT a byte-for-byte replica of what a real
//! `compact --major` would produce (no cross-generation shadow reconciliation,
//! no per-cell purge-safety check) — it is a cheap, self-consistent,
//! honestly-scoped ESTIMATE, exactly the same spirit as the cheap tier's
//! `estimated_droppable_tombstone_ratio`.

use super::deep_scan::DeepScanResult;

/// `--deep`'s dry, full-compaction-at-`--now` reclaim prediction (D1's
/// `reclaim_at_now` JSON shape: `{"rows_in", "rows_out", "tombstones_purged"}`),
/// scoped to ONE generation's own scan.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReclaimPrediction {
    pub rows_in: u64,
    pub rows_out: u64,
    pub tombstones_purged: u64,
}

/// Derive one generation's [`ReclaimPrediction`] from its [`DeepScanResult`]
/// (issue #4204).
pub(crate) fn compute_reclaim_prediction(scan: &DeepScanResult) -> ReclaimPrediction {
    let rows_in = scan.rows_scanned;
    let tombstones_purged = scan.tombstones_droppable_at_gc_before;
    let rows_out = rows_in.saturating_sub(tombstones_purged);
    ReclaimPrediction {
        rows_in,
        rows_out,
        tombstones_purged,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_from_one_generation_scan() {
        let scan = DeepScanResult {
            rows_scanned: 100,
            tombstones_droppable_at_gc_before: 10,
            ..Default::default()
        };
        let prediction = compute_reclaim_prediction(&scan);
        assert_eq!(prediction.rows_in, 100);
        assert_eq!(prediction.tombstones_purged, 10);
        assert_eq!(prediction.rows_out, 90);
    }

    #[test]
    fn empty_scan_all_zero() {
        let prediction = compute_reclaim_prediction(&DeepScanResult::default());
        assert_eq!(prediction.rows_in, 0);
        assert_eq!(prediction.rows_out, 0);
        assert_eq!(prediction.tombstones_purged, 0);
    }
}
