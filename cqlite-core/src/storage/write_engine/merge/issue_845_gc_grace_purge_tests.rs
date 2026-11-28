//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::schema::{KeyColumn, TableSchema};
use crate::storage::write_engine::mutation::{ClusteringBound, DecoratedKey};
use crate::types::{TombstoneInfo, TombstoneType};
use std::collections::HashMap;

fn dk(byte: u8) -> DecoratedKey {
    DecoratedKey::from_key_bytes(vec![byte]).expect("token")
}

fn live(run_index: usize, row_ts: i64, cells: Vec<CellData>) -> MergeEntry {
    MergeEntry::new(run_index, dk(1), None, row_ts, RowData::Live { cells })
}

fn unclustered_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks845".to_string(),
        table: "t845".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn tombstone_entry(run_index: usize, deletion_time: i64, ldt: i32) -> MergeEntry {
    MergeEntry::new(
        run_index,
        DecoratedKey::new(500, 7i32.to_be_bytes().to_vec()),
        None,
        deletion_time,
        RowData::Tombstone {
            deletion_time,
            local_deletion_time: ldt,
        },
    )
}

/// A single-cell tombstone `CellData` for column `v` carrying its LDT.
fn cell_tombstone(ts: i64, ldt: i32) -> CellData {
    CellData {
        column: "v".to_string(),
        value: Value::Tombstone(Box::new(TombstoneInfo {
            deletion_time: ts,
            tombstone_type: TombstoneType::CellTombstone,
            local_deletion_time: ldt as i64,
            ttl: None,
            range_start: None,
            range_end: None,
        })),
        timestamp: ts,
        ttl: None,
        cell_path: None,
        local_deletion_time: Some(ldt),
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    }
}

/// A complex-deletion marker whose LDT is older than gcBefore is purged; an
/// equivalent marker within grace is retained. Boundary: `== gcBefore` is
/// RETAINED (only strictly-less purges).
#[test]
fn issue_845_complex_deletion_purged_when_older_than_gc_before() {
    const GC_BEFORE: i64 = 1_700_000_000;
    // Marker covers NOTHING (no live elements), so purging cannot resurrect.
    let make = |ldt: i32| {
        MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] }).with_complex_deletions(
            vec![ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: 50,
                local_deletion_time: ldt,
            }],
        )
    };

    // Older than gcBefore → purgeable → the marker is dropped, and with no
    // surviving data + no row tombstone the whole entry vanishes.
    let purged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![make((GC_BEFORE - 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    );
    assert!(
        purged.is_none(),
        "a complex-deletion marker older than gcBefore must be purged, \
             leaving nothing to emit"
    );

    // Within grace (strictly newer) → retained.
    let retained = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![make((GC_BEFORE + 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a within-grace marker must keep a metadata-only entry");
    assert_eq!(
        retained.complex_deletions.len(),
        1,
        "a complex-deletion marker within grace must be retained"
    );

    // Boundary: LDT == gcBefore is RETAINED (only `< gcBefore` purges).
    let boundary = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![make(GC_BEFORE as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a marker at exactly gcBefore must be retained");
    assert_eq!(
        boundary.complex_deletions.len(),
        1,
        "localDeletionTime == gcBefore is within grace (only `<` purges)"
    );
}

/// A row tombstone whose LDT is older than gcBefore is purged; an equivalent
/// one within grace is retained. Boundary `== gcBefore` is RETAINED.
#[test]
fn issue_845_row_tombstone_purged_when_older_than_gc_before() {
    const GC_BEFORE: i64 = 1_700_000_000;
    // deletion_time (micros) chosen so it does not equal LDT (seconds).
    let dt = 1_000_000_000_000_000i64;

    let older = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, dt, (GC_BEFORE - 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    );
    assert!(
        older.is_none(),
        "a row tombstone older than gcBefore must be purged (nothing emitted)"
    );

    let within = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, dt, (GC_BEFORE + 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a within-grace row tombstone must be retained");
    assert!(
        matches!(within.row_data, RowData::Tombstone { .. }),
        "a within-grace row tombstone must survive"
    );

    let boundary = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, dt, GC_BEFORE as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a row tombstone at exactly gcBefore must be retained");
    assert!(
        matches!(boundary.row_data, RowData::Tombstone { .. }),
        "localDeletionTime == gcBefore is within grace (only `<` purges)"
    );
}

/// A simple cell tombstone whose LDT is older than gcBefore is purged; one
/// within grace is retained. Boundary `== gcBefore` is RETAINED.
#[test]
fn issue_845_cell_tombstone_purged_when_older_than_gc_before() {
    const GC_BEFORE: i64 = 1_700_000_000;
    // A live cell on a SEPARATE column keeps the row alive so we observe the
    // tombstone cell being dropped (not the row collapsing). Its ts is well
    // above any tombstone ts so row-tombstone shadowing is irrelevant here.
    let keep = CellData::new("name".to_string(), Value::text("alive".to_string()), 500);

    let count_tombstone_cells = |ldt: i32| -> usize {
        let merged = KWayMerger::<NoTrace>::reconcile_cluster(
            None,
            vec![live(0, 500, vec![keep.clone(), cell_tombstone(100, ldt)])],
            &::std::collections::HashMap::new(),
            Some(GC_BEFORE),
        )
        .expect("a live row must be emitted");
        match merged.row_data {
            RowData::Live { cells } => cells
                .iter()
                .filter(|c| KWayMerger::<NoTrace>::is_cell_tombstone(c))
                .count(),
            other => panic!("expected Live, got {other:?}"),
        }
    };

    assert_eq!(
        count_tombstone_cells((GC_BEFORE - 1) as i32),
        0,
        "a cell tombstone older than gcBefore must be purged from the row"
    );
    assert_eq!(
        count_tombstone_cells((GC_BEFORE + 1) as i32),
        1,
        "a cell tombstone within grace must be retained"
    );
    assert_eq!(
        count_tombstone_cells(GC_BEFORE as i32),
        1,
        "localDeletionTime == gcBefore is within grace (only `<` purges)"
    );
}

/// When `gc_before_secs` is `None` the purge stage is a strict no-op: an
/// ancient tombstone (LDT far in the past) is RETAINED, preserving the
/// pre-#845 behavior.
#[test]
fn issue_845_no_gc_before_retains_everything() {
    let dt = 1_000_000_000_000_000i64;
    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, dt, 1)],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("without gcBefore the tombstone must be retained");
    assert!(
        matches!(merged.row_data, RowData::Tombstone { .. }),
        "with gc_before_secs = None nothing is purged"
    );
}

/// CRITICAL no-resurrection guard: a complex-deletion marker that SHADOWS a
/// covered element (ts <= mfda) must remove that element in Step 2b BEFORE
/// the marker is purged in the gc stage. After purging the marker, the
/// covered element must NOT reappear.
#[test]
fn issue_845_purge_does_not_resurrect_shadowed_element() {
    const GC_BEFORE: i64 = 1_700_000_000;
    const MFDA: i64 = 200;
    // A complex ELEMENT (carries a cell_path) of column `tags` written at
    // ts == MFDA, so it is shadowed (`ts <= mfda`) by the marker.
    let covered = CellData {
        column: "tags".to_string(),
        value: Value::text("ghost".to_string()),
        timestamp: MFDA,
        ttl: None,
        cell_path: Some(vec![1, 2, 3]),
        local_deletion_time: None,
        is_complex_element: true,
        is_deleted: false,
        has_empty_value: false,
    };
    let entry = MergeEntry::new(
        0,
        dk(1),
        None,
        MFDA,
        RowData::Live {
            cells: vec![covered],
        },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: MFDA,
        // Marker is older than gcBefore → purgeable.
        local_deletion_time: (GC_BEFORE - 1) as i32,
    }]);

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![entry],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    );
    // The covered element was shadowed (removed) in Step 2b, then the marker
    // was purged: nothing survives, so no entry is emitted — and crucially
    // the shadowed element is NOT resurrected as a live cell.
    assert!(
        merged.is_none(),
        "purging the marker must not resurrect the element it shadowed"
    );
}

/// `compute_gc_before` derives `now - gc_grace_seconds` from the schema's
/// `gc_grace_seconds` comment. When ABSENT it falls back to Cassandra's
/// table DEFAULT of 864000s (#921 finding 3); INVALID values return `None`.
#[test]
fn issue_845_compute_gc_before_from_schema() {
    let mut schema = unclustered_schema();
    let now = 2_000_000_000i64;

    // gc_grace_seconds = 0 → gcBefore == now (immediate purge eligibility).
    schema
        .comments
        .insert("gc_grace_seconds".to_string(), "0".to_string());
    assert_eq!(compute_gc_before(&schema, now), Some(now));

    // gc_grace_seconds = 864000 (10 days) → gcBefore == now - 864000.
    schema
        .comments
        .insert("gc_grace_seconds".to_string(), "864000".to_string());
    assert_eq!(compute_gc_before(&schema, now), Some(now - 864_000));

    // Unparseable → None (never purge on bad metadata).
    schema
        .comments
        .insert("gc_grace_seconds".to_string(), "not-a-number".to_string());
    assert_eq!(compute_gc_before(&schema, now), None);
}

/// #921 finding 3: a table that OMITS `gc_grace_seconds` must use
/// Cassandra's DEFAULT of 864000s (10 days), not disable purging. A NEGATIVE
/// or unparseable value is rejected conservatively (`None`, no purge); 0 is
/// valid (immediate grace).
#[test]
fn issue_921_compute_gc_before_defaults_to_864000_when_absent() {
    let mut schema = unclustered_schema();
    let now = 2_000_000_000i64;

    // ABSENT → Cassandra default 864000s (NOT None / disabled).
    assert_eq!(
        compute_gc_before(&schema, now),
        Some(now - 864_000),
        "missing gc_grace_seconds must use the Cassandra default of 864000s"
    );

    // 0 is a VALID value (immediate grace) per Cassandra → gcBefore == now.
    schema
        .comments
        .insert("gc_grace_seconds".to_string(), "0".to_string());
    assert_eq!(
        compute_gc_before(&schema, now),
        Some(now),
        "gc_grace_seconds = 0 is valid (immediate grace)"
    );

    // NEGATIVE → None (never purge on out-of-range metadata).
    schema
        .comments
        .insert("gc_grace_seconds".to_string(), "-1".to_string());
    assert_eq!(
        compute_gc_before(&schema, now),
        None,
        "a negative gc_grace_seconds must disable purging (no-op)"
    );

    // Unparseable → None.
    schema
        .comments
        .insert("gc_grace_seconds".to_string(), "garbage".to_string());
    assert_eq!(
        compute_gc_before(&schema, now),
        None,
        "an unparseable gc_grace_seconds must disable purging (no-op)"
    );
}

/// #921 finding 2: on-disk `localDeletionTime` is an UNSIGNED 32-bit count.
/// A far-future LDT with bit 31 set (e.g. `0x8000_0000` ≈ year 2038) is
/// carried as a NEGATIVE `i32`. The purge compare must normalize it as
/// unsigned (`i64::from(ldt as u32)`) so it reads as a LARGE future second
/// and is NOT purged by a normal `gcBefore`. The pre-fix `as i64` path made
/// it look ancient and purged the tombstone immediately (resurrection bug).
#[test]
fn issue_921_unsigned_local_deletion_time_not_purged() {
    // bit 31 set: as i32 this is negative (-2147483648); as u32 it is
    // 2_147_483_648 seconds ≈ 2038-01-19 — far in the future.
    let future_ldt_bits = 0x8000_0000u32 as i32;
    assert!(future_ldt_bits < 0, "the wrapped LDT is a negative i32");
    // A normal gcBefore well below the unsigned value: nothing should purge.
    const GC_BEFORE: i64 = 1_700_000_000;
    assert!(
        i64::from(future_ldt_bits as u32) > GC_BEFORE,
        "the unsigned LDT is in the future relative to gcBefore"
    );

    // (a) Row tombstone with the far-future LDT must be RETAINED.
    let dt = 1_000_000_000_000_000i64;
    let row = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, dt, future_ldt_bits)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a far-future row tombstone must NOT be purged");
    assert!(
        matches!(row.row_data, RowData::Tombstone { .. }),
        "an unsigned far-future row tombstone must survive a normal gcBefore"
    );

    // (b) Complex-deletion marker with the far-future LDT must be RETAINED.
    let complex = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![
            MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
                .with_complex_deletions(vec![ComplexDeletion {
                    column: "tags".to_string(),
                    marked_for_delete_at: 50,
                    local_deletion_time: future_ldt_bits,
                }]),
        ],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a far-future complex-deletion marker must NOT be purged");
    assert_eq!(
        complex.complex_deletions.len(),
        1,
        "an unsigned far-future complex marker must survive a normal gcBefore"
    );

    // (c) Cell tombstone with the far-future LDT must be RETAINED.
    let keep = CellData::new("name".to_string(), Value::text("alive".to_string()), 500);
    let cell = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![live(
            0,
            500,
            vec![keep, cell_tombstone(100, future_ldt_bits)],
        )],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a live row must be emitted");
    let tombstone_cells = match cell.row_data {
        RowData::Live { cells } => cells
            .iter()
            .filter(|c| KWayMerger::<NoTrace>::is_cell_tombstone(c))
            .count(),
        other => panic!("expected Live, got {other:?}"),
    };
    assert_eq!(
        tombstone_cells, 1,
        "an unsigned far-future cell tombstone must survive a normal gcBefore"
    );

    // Control: a genuinely ancient LDT (bit 31 clear, < gcBefore) IS purged,
    // proving the normalization did not disable purging wholesale.
    let ancient = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, dt, (GC_BEFORE - 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    );
    assert!(
        ancient.is_none(),
        "a genuinely ancient row tombstone must still be purged"
    );
}

/// #921 finding 1 (SAFETY-CRITICAL): a PARTIAL compaction (not overlap-safe)
/// must NOT purge tombstones — purging one could resurrect data shadowed in a
/// non-included overlapping SSTable. A MAJOR/full compaction (overlap-safe)
/// purges. Drives the gate through `merge_partition_rows`, which collapses
/// the gc_grace cutoff to `None` unless `purge_safe` is set.
#[test]
fn issue_921_partial_compaction_does_not_purge_but_major_does() {
    const GC_BEFORE: i64 = 1_700_000_000;
    let dt = 1_000_000_000_000_000i64;
    // A row tombstone old enough to be purgeable (LDT < gcBefore).
    let ancient_ldt = (GC_BEFORE - 1) as i32;

    // Build a merger with NO runs (merge_partition_rows ignores `runs`),
    // gc_before set, and toggle only `purge_safe`.
    let merger = |purge_safe: bool| KWayMerger {
        runs: vec![],
        heap: BinaryHeap::new(),
        current_partition: None,
        schema: unclustered_schema(),
        schema_arc: std::sync::Arc::new(unclustered_schema()),
        gc_before_secs: Some(GC_BEFORE),
        now_secs: None,
        purge_safe,
        max_purgeable_timestamp: None,
        _egress_slot: None,
        trace: NoTrace,
    };

    // PARTIAL compaction (purge_safe = false): the purgeable row tombstone is
    // RETAINED — the partial path must never drop a tombstone.
    let partial = merger(false)
        .merge_partition_rows(vec![tombstone_entry(0, dt, ancient_ldt)])
        .expect("merge must succeed");
    assert_eq!(
        partial.len(),
        1,
        "a partial (non-overlap-safe) compaction must RETAIN the tombstone"
    );
    assert!(
        matches!(partial[0].row_data, RowData::Tombstone { .. }),
        "the retained entry must still be a row tombstone (no resurrection risk)"
    );

    // MAJOR compaction (purge_safe = true): the same purgeable tombstone IS
    // dropped — overlap-safe, so purging cannot resurrect anything.
    let major = merger(true)
        .merge_partition_rows(vec![tombstone_entry(0, dt, ancient_ldt)])
        .expect("merge must succeed");
    assert!(
        major.is_empty(),
        "a major (overlap-safe) compaction must PURGE the ancient tombstone"
    );
}

/// #1061 (SAFETY-CRITICAL): the surviving-range-tombstone re-emit loop in
/// `merge_partition_rows` must gate a marker purge on BOTH `gc_before` AND
/// the overlap bound (`rt.deletion_time < max_purgeable_timestamp`), exactly
/// like the cell/row/complex purge paths. In a PARTIAL compaction with a
/// finite overlap bound, a range tombstone at/above that bound may still
/// shadow data in a non-included SSTable, so dropping it would resurrect
/// that data. This drives the gate through the real `merge_partition_rows`.
#[test]
fn issue_1061_range_tombstone_purge_respects_overlap_bound() {
    const GC_BEFORE: i64 = 1_700_000_000;
    // Old enough for the gc_grace condition to be satisfied on its own.
    let ancient_ldt = (GC_BEFORE - 1) as i32;
    // Finite overlap bound (min write timestamp of a non-included SSTable).
    const BOUND: i64 = 2_000;

    // Build a merger exactly like #921's: no runs, gc_before set, toggle
    // `purge_safe` and `max_purgeable_timestamp`.
    let merger = |purge_safe: bool, bound: Option<i64>| KWayMerger {
        runs: vec![],
        heap: BinaryHeap::new(),
        current_partition: None,
        schema: unclustered_schema(),
        schema_arc: std::sync::Arc::new(unclustered_schema()),
        gc_before_secs: Some(GC_BEFORE),
        now_secs: None,
        purge_safe,
        max_purgeable_timestamp: bound,
        _egress_slot: None,
        trace: NoTrace,
    };

    // A whole-partition range-tombstone CARRIER entry (issue #933): empty
    // live row whose only payload is the range deletion.
    let carrier = |deletion_time: i64| {
        let rt = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time,
            local_deletion_time: ancient_ldt,
        };
        MergeEntry::new(
            0,
            dk(1),
            None,
            deletion_time,
            RowData::Live { cells: vec![] },
        )
        .with_range_deletion(rt)
    };

    // (1) PARTIAL compaction, marker AT the bound (`deletion_time == BOUND`):
    // RETAINED. `BOUND < BOUND` is false, so the overlap guard blocks the
    // purge even though the gc_grace condition (`ancient_ldt < gcBefore`) holds.
    let at_bound = merger(false, Some(BOUND))
        .merge_partition_rows(vec![carrier(BOUND)])
        .expect("merge must succeed");
    assert_eq!(
        at_bound.len(),
        1,
        "a range tombstone AT the overlap bound must be RETAINED (#1061)"
    );
    assert!(
        at_bound[0].range_deletion.is_some(),
        "the retained entry must still carry the range-tombstone marker"
    );

    // (2) PARTIAL compaction, marker ABOVE the bound: also RETAINED.
    let above_bound = merger(false, Some(BOUND))
        .merge_partition_rows(vec![carrier(BOUND + 1_000)])
        .expect("merge must succeed");
    assert_eq!(
        above_bound.len(),
        1,
        "a range tombstone ABOVE the overlap bound must be RETAINED (#1061)"
    );

    // (3) Control: PARTIAL compaction, marker strictly BELOW the bound AND
    // gc_grace-expired: PURGED — proving the overlap guard does not disable
    // purging wholesale.
    let below_bound = merger(false, Some(BOUND))
        .merge_partition_rows(vec![carrier(BOUND - 1)])
        .expect("merge must succeed");
    assert!(
        below_bound.is_empty(),
        "a gc-expired range tombstone strictly BELOW the overlap bound is purgeable"
    );

    // (4) Control: a MAJOR/overlap-safe compaction (`purge_safe = true`,
    // bound collapses to i64::MAX) purges the same at-bound marker — the fix
    // does not weaken the overlap-safe purge path.
    let overlap_safe = merger(true, None)
        .merge_partition_rows(vec![carrier(BOUND)])
        .expect("merge must succeed");
    assert!(
        overlap_safe.is_empty(),
        "an overlap-safe (major) compaction still purges the gc-expired marker"
    );
}

/// #921 finding 2: a row tombstone with `local_deletion_time == 0` is the
/// "LDT not surfaced" placeholder used by legacy/pre-V5 paths. It must be
/// treated as UNKNOWN and RETAINED under purge-safe compaction (never purge
/// on unknown LDT). Only a real, non-zero LDT strictly below `gcBefore`
/// purges; a within-grace LDT is retained.
#[test]
fn issue_921_row_tombstone_ldt_zero_is_unknown_and_retained() {
    const GC_BEFORE: i64 = 1_700_000_000;
    let dt = 1_000_000_000_000_000i64;

    // LDT == 0 (unknown placeholder): RETAINED even though `0 < gcBefore`.
    let unknown = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, dt, 0)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a row tombstone with unknown (0) LDT must be RETAINED, not purged");
    assert!(
        matches!(unknown.row_data, RowData::Tombstone { .. }),
        "LDT==0 is the unknown placeholder and must be retained (never purge \
             on unknown LDT)"
    );

    // A REAL, non-zero ancient LDT (< gcBefore) still purges.
    let ancient = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, dt, (GC_BEFORE - 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    );
    assert!(
        ancient.is_none(),
        "a real non-zero ancient row tombstone (LDT < gcBefore) must still purge"
    );

    // Within-grace LDT (>= gcBefore) is retained.
    let within = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone_entry(0, dt, (GC_BEFORE + 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a within-grace row tombstone must be retained");
    assert!(
        matches!(within.row_data, RowData::Tombstone { .. }),
        "a within-grace row tombstone (LDT >= gcBefore) must survive"
    );
}

/// #921 finding 3: a CLUSTERED row whose only non-key data is a PURGEABLE
/// cell tombstone must emit NOTHING after the gc purge — no phantom key-only
/// live row. The `purged_to_empty` determination must run AFTER the cell
/// tombstone purge. A clustered row with REAL surviving (non-key) data still
/// emits a live row.
#[test]
fn issue_921_clustered_row_with_only_purgeable_cell_tombstone_emits_nothing() {
    const GC_BEFORE: i64 = 1_700_000_000;
    // Clustering key column `ck` (a pseudo-cell that stays in the cell list).
    let ck = ClusteringKey {
        columns: vec![("ck".to_string(), Value::text("c1".to_string()))],
    };
    // The clustering-key pseudo-cell carried alongside the data cells (mirrors
    // `extract_clustering_key` keeping CK columns in the cell list for
    // read-back). It is NOT a data cell (its column name is in `ck_names`).
    let ck_cell = CellData::new("ck".to_string(), Value::text("c1".to_string()), 100);

    let make = |extra: Vec<CellData>| {
        let mut cells = vec![ck_cell.clone()];
        cells.extend(extra);
        MergeEntry::new(0, dk(1), Some(ck.clone()), 100, RowData::Live { cells })
    };

    // Only non-key data is a purgeable (ancient LDT) cell tombstone on `v`.
    // After the gc purge the row has only the CK pseudo-cell left → it must
    // be recognized as purged-to-empty and emit NOTHING (no phantom live row).
    let purged = KWayMerger::<NoTrace>::reconcile_cluster(
        Some(ck.clone()),
        vec![make(vec![cell_tombstone(50, (GC_BEFORE - 1) as i32)])],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    );
    assert!(
        purged.is_none(),
        "a clustered row whose only non-key data is a purgeable cell tombstone \
             must emit NOTHING (no phantom key-only live row) after the gc purge"
    );

    // Control: a clustered row with REAL surviving non-key data still emits a
    // live row (the purge of the tombstone does not collapse a real row).
    let kept = KWayMerger::<NoTrace>::reconcile_cluster(
        Some(ck.clone()),
        vec![make(vec![
            CellData::new("v".to_string(), Value::text("real".to_string()), 60),
            cell_tombstone(50, (GC_BEFORE - 1) as i32),
        ])],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
    )
    .expect("a clustered row with real surviving data must emit a live row");
    match kept.row_data {
        RowData::Live { cells } => {
            assert!(
                cells
                    .iter()
                    .any(|c| c.column == "v" && !KWayMerger::<NoTrace>::is_cell_tombstone(c)),
                "the real surviving `v` cell must remain in the emitted live row"
            );
            assert!(
                !cells.iter().any(KWayMerger::<NoTrace>::is_cell_tombstone),
                "the purgeable cell tombstone must still be purged from the live row"
            );
        }
        other => panic!("expected Live row, got {other:?}"),
    }
}

// ── Issue #935: overlap-aware partial-compaction purging ─────────────────
//
// Parity Cassandra `CompactionController.maxPurgeableTimestamp` /
// `getPurgeEvaluator` (`time -> time < minTimestamp`): in a PARTIAL
// compaction a tombstone is purgeable only when BOTH its gc grace has
// elapsed (`localDeletionTime < gcBefore`) AND its own deletion timestamp
// (`markedForDeleteAt`) is STRICTLY LESS THAN the minimum write timestamp of
// every non-included overlapping SSTable — so it provably shadows nothing
// outside the compaction set. `reconcile_cluster_with_overlap` takes that
// bound as `max_purgeable_timestamp`; `i64::MAX` is the full-compaction fast
// path (unrestricted, identical to #845).

/// A row tombstone that PROVABLY predates all non-included overlapping data
/// (`markedForDeleteAt < bound`) and is past grace is PURGED in a partial
/// compaction.
#[test]
fn issue_935_partial_compaction_purges_row_tombstone_below_overlap_bound() {
    const GC_BEFORE: i64 = 1_700_000_000;
    // Outside SSTables' min write timestamp. The tombstone's markedForDeleteAt
    // is strictly below it, so it shadows nothing outside the set.
    const BOUND: i64 = 1_000_000_000_000_000;
    let mfda = BOUND - 1; // strictly older than every outside cell

    let purged = KWayMerger::<NoTrace>::reconcile_cluster_with_overlap(
        None,
        vec![tombstone_entry(0, mfda, (GC_BEFORE - 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
        BOUND,
    );
    assert!(
        purged.is_none(),
        "a row tombstone older than every non-included overlapping SSTable \
             (markedForDeleteAt < bound) and past grace must be purged even in a \
             partial compaction (#935)"
    );
}

/// A row tombstone whose `markedForDeleteAt >= bound` COULD shadow data in a
/// non-included overlapping SSTable, so it is RETAINED even though its gc
/// grace has elapsed. Boundary `== bound` is retained (only strictly-less
/// purges).
#[test]
fn issue_935_partial_compaction_retains_row_tombstone_at_or_above_overlap_bound() {
    const GC_BEFORE: i64 = 1_700_000_000;
    const BOUND: i64 = 1_000_000_000_000_000;

    // markedForDeleteAt strictly ABOVE the bound: outside data could be older
    // than the tombstone and thus shadowed → must retain.
    let above = KWayMerger::<NoTrace>::reconcile_cluster_with_overlap(
        None,
        vec![tombstone_entry(0, BOUND + 1, (GC_BEFORE - 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
        BOUND,
    )
    .expect("a tombstone that could shadow outside data must be retained");
    assert!(
        matches!(above.row_data, RowData::Tombstone { .. }),
        "a row tombstone with markedForDeleteAt > overlap bound must survive a \
             partial compaction (#935): outside data may be shadowed"
    );

    // Boundary: markedForDeleteAt EXACTLY at the bound is retained — an outside
    // SSTable could hold a cell at exactly the bound that the tombstone would
    // shadow (`time < minTimestamp` is the purge predicate, so `==` retains).
    let boundary = KWayMerger::<NoTrace>::reconcile_cluster_with_overlap(
        None,
        vec![tombstone_entry(0, BOUND, (GC_BEFORE - 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
        BOUND,
    )
    .expect("a tombstone at exactly the overlap bound must be retained");
    assert!(
        matches!(boundary.row_data, RowData::Tombstone { .. }),
        "markedForDeleteAt == overlap bound is retained (only `<` purges, #935)"
    );
}

/// The overlap gate is independent of the gc-grace gate: a tombstone below the
/// overlap bound but still WITHIN grace is retained (both gates must allow the
/// purge).
#[test]
fn issue_935_within_grace_tombstone_retained_despite_overlap_bound() {
    const GC_BEFORE: i64 = 1_700_000_000;
    const BOUND: i64 = 1_000_000_000_000_000;

    let within = KWayMerger::<NoTrace>::reconcile_cluster_with_overlap(
        None,
        vec![tombstone_entry(0, BOUND - 1, (GC_BEFORE + 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
        BOUND,
    )
    .expect("a within-grace tombstone must be retained regardless of overlap");
    assert!(
        matches!(within.row_data, RowData::Tombstone { .. }),
        "a tombstone within gc grace must be retained even when its \
             markedForDeleteAt is below the overlap bound (#935)"
    );
}

/// A purgeable simple cell tombstone is dropped in a partial compaction only
/// when its write timestamp is below the overlap bound; at/above the bound it
/// is retained. The control row carries a live cell on another column so the
/// row survives and we observe the per-cell decision.
#[test]
fn issue_935_partial_compaction_cell_tombstone_overlap_gate() {
    const GC_BEFORE: i64 = 1_700_000_000;
    const BOUND: i64 = 5_000;

    // Below the bound (and past grace) → purged from the surviving cells.
    let purged = KWayMerger::<NoTrace>::reconcile_cluster_with_overlap(
        None,
        vec![live(
            0,
            100,
            vec![
                CellData::new("keep".to_string(), Value::text("x".to_string()), 9_000),
                cell_tombstone(BOUND - 1, (GC_BEFORE - 1) as i32),
            ],
        )],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
        BOUND,
    )
    .expect("the live `keep` cell keeps the row alive");
    match purged.row_data {
        RowData::Live { cells } => assert!(
            !cells.iter().any(KWayMerger::<NoTrace>::is_cell_tombstone),
            "a cell tombstone below the overlap bound must be purged (#935)"
        ),
        other => panic!("expected Live row, got {other:?}"),
    }

    // At/above the bound → retained (could shadow an outside cell).
    let retained = KWayMerger::<NoTrace>::reconcile_cluster_with_overlap(
        None,
        vec![live(
            0,
            100,
            vec![
                CellData::new("keep".to_string(), Value::text("x".to_string()), 9_000),
                cell_tombstone(BOUND, (GC_BEFORE - 1) as i32),
            ],
        )],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
        BOUND,
    )
    .expect("the live `keep` cell keeps the row alive");
    match retained.row_data {
        RowData::Live { cells } => assert!(
            cells.iter().any(KWayMerger::<NoTrace>::is_cell_tombstone),
            "a cell tombstone at the overlap bound must be retained (#935)"
        ),
        other => panic!("expected Live row, got {other:?}"),
    }
}

/// A complex-deletion marker is purged in a partial compaction only when its
/// `marked_for_delete_at` is below the overlap bound.
#[test]
fn issue_935_partial_compaction_complex_deletion_overlap_gate() {
    const GC_BEFORE: i64 = 1_700_000_000;
    const BOUND: i64 = 5_000;
    let make = |mfda: i64| {
        MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] }).with_complex_deletions(
            vec![ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: mfda,
                local_deletion_time: (GC_BEFORE - 1) as i32,
            }],
        )
    };

    // Below the bound and past grace → purged (nothing left to emit).
    let purged = KWayMerger::<NoTrace>::reconcile_cluster_with_overlap(
        None,
        vec![make(BOUND - 1)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
        BOUND,
    );
    assert!(
        purged.is_none(),
        "a complex-deletion marker below the overlap bound must be purged (#935)"
    );

    // At the bound → retained as a metadata-only entry.
    let retained = KWayMerger::<NoTrace>::reconcile_cluster_with_overlap(
        None,
        vec![make(BOUND)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
        BOUND,
    )
    .expect("a marker at the overlap bound must be retained");
    assert_eq!(
        retained.complex_deletions.len(),
        1,
        "a complex-deletion marker at the overlap bound must be retained (#935)"
    );
}

/// The full-compaction fast path (`max_purgeable_timestamp == i64::MAX`)
/// purges a past-grace tombstone regardless of its timestamp — identical to
/// the pre-#935 behavior, so the overlap gate is a strict no-op there.
#[test]
fn issue_935_full_compaction_bound_is_unrestricted() {
    const GC_BEFORE: i64 = 1_700_000_000;
    // A very large markedForDeleteAt that no realistic outside bound would
    // exceed still purges under the full-compaction +inf bound.
    let purged = KWayMerger::<NoTrace>::reconcile_cluster_with_overlap(
        None,
        vec![tombstone_entry(0, i64::MAX - 1, (GC_BEFORE - 1) as i32)],
        &::std::collections::HashMap::new(),
        Some(GC_BEFORE),
        i64::MAX,
    );
    assert!(
        purged.is_none(),
        "the full-compaction +inf overlap bound must purge any past-grace \
             tombstone, identical to #845 (#935 no-op for full compactions)"
    );
}

/// #921 finding 2: the dropped-column survivor pre-pass
/// (`compute_surviving_dropped_columns`) must count a RETAINED
/// `ComplexDeletion` marker for a dropped COMPLEX column as a survivor.
///
/// A complex (collection) tombstone lives in `row.complex_deletions`, NOT in
/// `cells`. The pre-fix pre-pass counted only live `cells`, so a dropped
/// complex column whose only survivor is a within-grace / purge-unsafe
/// complex-deletion marker was stripped from the output schema — and since the
/// writer only emits complex-element columns present in the schema, the marker
/// was silently dropped despite the merge deciding to RETAIN it.
///
/// This drives the FULL merge→writer→on-disk path: it writes a `tags`
/// complex-deletion marker (no live elements) to a real SSTable, then runs the
/// survivor pre-pass over that file with `tags` dropped. With no gc (`None`),
/// the marker is RETAINED so `tags` MUST be counted as a survivor; with a gc
/// cutoff strictly above the marker's LDT (and `purge_safe`), the marker is
/// PURGED so `tags` MUST NOT be counted. Mirrors the cell-vs-marker asymmetry
/// the writer sees.
#[tokio::test]
async fn issue_921_complex_deletion_marker_counts_as_dropped_column_survivor() {
    use crate::schema::Column;

    // An in-memory run yielding a fixed `MergeEntry` so we drive the FULL
    // production merge→writer path onto a real on-disk SSTable.
    struct VecIterator(std::vec::IntoIter<MergeEntry>);
    impl SSTableRowIterator for VecIterator {
        fn next(&mut self) -> Option<Result<MergeEntry>> {
            self.0.next().map(Ok)
        }
    }

    // Schema with a non-frozen complex column `tags set<text>`. Drop map is
    // applied per-call below.
    fn schema_with_tags(dropped: HashMap<String, i64>) -> TableSchema {
        TableSchema {
            keyspace: "ks921".to_string(),
            table: "t921".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "tags".to_string(),
                    data_type: "set<text>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: dropped,
        }
    }

    const MARKER_LDT: i32 = 1_700_000_000;
    const MARKER_MFDA: i64 = 1_600_000_000_000_000; // micros

    // Write an SSTable whose single partition carries ONLY a `tags`
    // complex-deletion marker (no surviving live elements, no other cells), so
    // the column's sole survivor is the marker — exactly the case the pre-fix
    // pre-pass missed.
    let write_schema = schema_with_tags(HashMap::new());
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(7, 7i32.to_be_bytes().to_vec()),
        None,
        0,
        RowData::Live { cells: vec![] },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: MARKER_MFDA,
        local_deletion_time: MARKER_LDT,
    }]);

    let merger = KWayMerger {
        runs: vec![RunReader::new(Box::new(VecIterator(
            vec![entry].into_iter(),
        )))],
        heap: std::collections::BinaryHeap::new(),
        current_partition: None,
        gc_before_secs: None,
        now_secs: None,
        purge_safe: false,
        max_purgeable_timestamp: None,
        schema: write_schema.clone(),
        schema_arc: std::sync::Arc::new(write_schema.clone()),
        _egress_slot: None,
        trace: NoTrace,
    };

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
        temp_dir.path().to_path_buf(),
        1,
        &write_schema,
    )
    .expect("create writer");
    // Issue #1668 stage 5c-iv part 2: `KWayMerger::merge` now streams
    // partitions through `begin_partition_incremental`, which requires
    // pre-seeded encoding baselines — exactly what the one real
    // production caller (`compact_sstables_with_registry`) always does.
    // Seed with this test's own known marker minimums.
    writer.pre_seed_encoding_baselines(MARKER_MFDA, MARKER_LDT, i32::MAX);
    merger.merge(&mut writer).expect("merge+write must succeed");
    let info = writer.finish().await.expect("finish must succeed");
    let data_path = info.data_path;

    // Drop `tags`; the column stays in `columns` (decode contract) with a drop
    // time well before the marker so a re-add filter does not interfere.
    let mut dropped = HashMap::new();
    dropped.insert("tags".to_string(), 1_i64);
    let drop_schema = schema_with_tags(dropped);

    // (a) No gc → the marker is RETAINED → `tags` MUST be a survivor.
    let retained = compute_surviving_dropped_columns(
        vec![data_path.clone()],
        &drop_schema,
        None,
        None,
        false,
        None,
    )
    .expect("survivor pre-pass (no gc) must succeed");
    assert!(
        retained.contains("tags"),
        "a RETAINED complex-deletion marker for a dropped complex column must \
             count as a survivor so the writer keeps the column to emit it; got {retained:?}"
    );

    // (b) gc strictly above the marker's LDT + purge_safe → the marker is
    // PURGED → `tags` MUST NOT be a survivor (stripped from the output schema).
    let gc_before = i64::from(MARKER_LDT) + 1;
    let purged = compute_surviving_dropped_columns(
        vec![data_path],
        &drop_schema,
        Some(gc_before),
        Some(gc_before),
        true,
        None,
    )
    .expect("survivor pre-pass (purging) must succeed");
    assert!(
        !purged.contains("tags"),
        "a PURGED complex-deletion marker must NOT count as a survivor (the \
             dropped column is stripped from the output schema); got {purged:?}"
    );
}
