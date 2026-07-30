//! Read-side tombstone/TTL shadowing unit pins — the `#[cfg(test)] mod tests` of
//! `partition_shadow.rs`, extracted VERBATIM to a sibling file so the parent stays
//! under the campsite-rule size threshold (epic #1116 / #1135). Included via
//! `#[cfg(test)] #[path = "partition_shadow_tests.rs"] mod tests;`, so `super` is
//! still the `partition_shadow` module and every path resolves exactly as before.
//!
//! Issue #1741: deterministic pins for the read-side shadowing primitives.
//! These exercise the exact partition-deletion, range-tombstone-FSM, and TTL
//! decision logic the emit paths call, independent of on-disk fixtures.
use super::*;

/// Build a `RowHeader` describing a live row whose max data timestamp is
/// `write_ts_micros` and (optionally) whose row-liveness TTL expires at
/// `liveness_expiry_secs`. `has_live_forever` marks a no-TTL data cell.
fn row(
    write_ts_micros: i64,
    liveness_expiry_secs: Option<i64>,
    max_cell_expires_at_secs: Option<i64>,
    has_live_forever: bool,
) -> RowHeader {
    RowHeader {
        timestamp: Some(write_ts_micros),
        ttl: None,
        liveness_expires_at_seconds: liveness_expiry_secs,
        local_deletion_time: None,
        marked_for_delete_at: None,
        header_size: 0,
        row_size_vint_len: 0,
        missing_columns_bitmap: None,
        max_data_cell_timestamp: Some(write_ts_micros),
        max_data_cell_expires_at: max_cell_expires_at_secs,
        has_live_forever_data_cell: has_live_forever,
        has_deleted_data_cell: false,
    }
}

#[test]
fn partition_deletion_shadows_only_older_or_equal_rows() {
    // Partition tombstone at markedForDeleteAt = 2000µs.
    let shadow = PartitionShadow::open(0, Some((2000, 12345)), vec![]);
    // Row older than the deletion → hidden.
    assert!(shadow.row_hidden(&row(1000, None, None, false), &[]));
    // Row exactly at the deletion → hidden (deletes: ts <= markedForDeleteAt).
    assert!(shadow.row_hidden(&row(2000, None, None, false), &[]));
    // Row strictly newer than the deletion → survives.
    assert!(!shadow.row_hidden(&row(3000, None, None, false), &[]));
}

#[test]
fn row_with_no_authoritative_timestamp_is_not_shadowed() {
    // A row with no liveness and no decodable data cell (max_ts = i64::MIN, e.g.
    // an empty/undecodable row) must NOT be hidden — we only shadow when
    // authoritative metadata proves the row predates the deletion (no guessing).
    let mut h = row(0, None, None, false);
    h.timestamp = None;
    h.max_data_cell_timestamp = None;
    let shadow = PartitionShadow::open(0, Some((8_000_000_000_000_000_000, 12345)), vec![]);
    assert!(!shadow.row_hidden(&h, &[]));
}

#[test]
fn live_partition_hides_nothing() {
    let shadow = PartitionShadow::open(0, None, vec![]);
    assert!(!shadow.row_hidden(&row(1000, None, None, false), &[]));
}

#[test]
fn range_tombstone_fsm_shadows_covered_older_rows() {
    // ASC single clustering column (no reversal). INCL_START at ck=[10],
    // INCL_END at ck=[20], deleted_at = 5000µs.
    let mut shadow = PartitionShadow::open(0, None, vec![false]);
    shadow
        .feed_range_marker(vec![Value::Integer(10)], 1, 5000, None)
        .unwrap();
    assert!(shadow.needs_clustering());
    // Covered older row (ck=15, ts < deleted_at) → hidden.
    assert!(shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(15)]));
    // Covered but strictly-newer row (ts > deleted_at) → survives.
    assert!(!shadow.row_hidden(&row(9000, None, None, false), &[Value::Integer(15)]));
    // Row before the open start bound (ck=5) → not covered → survives.
    assert!(!shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(5)]));
    // Close the range; subsequent rows are no longer covered.
    shadow
        .feed_range_marker(vec![Value::Integer(20)], 6, 5000, None)
        .unwrap();
    assert!(!shadow.needs_clustering());
    assert!(!shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(15)]));
}

#[test]
fn range_tombstone_coverage_honors_desc_clustering_order() {
    // Table with `CLUSTERING ORDER BY (ck DESC)`: physical storage order is the
    // REVERSE of value order, so an INCL_START marker at ck=[10] opens a range
    // that physically covers rows with ck < 10 (which come AFTER [10] on disk),
    // NOT rows with ck > 10. deleted_at = 5000µs; rows below are older (ts=1000).
    //
    // Revert-verify: with the pre-fix raw `partial_cmp` (ignoring DESC), the
    // coverage sides invert — ck=15 is wrongly hidden and ck=5 wrongly kept.
    let mut shadow = PartitionShadow::open(0, None, vec![true]);
    shadow
        .feed_range_marker(vec![Value::Integer(10)], 1, 5000, None)
        .unwrap();
    assert!(shadow.needs_clustering());
    // ck=5 is physically AFTER the DESC start [10] → covered → hidden.
    assert!(shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(5)]));
    // ck=15 is physically BEFORE the DESC start [10] → not covered → survives.
    assert!(!shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(15)]));
    // Exact inclusive boundary ck=10 → covered → hidden.
    assert!(shadow.row_hidden(&row(1000, None, None, false), &[Value::Integer(10)]));
    // A covered DESC row that is strictly newer than the deletion still survives.
    assert!(!shadow.row_hidden(&row(9000, None, None, false), &[Value::Integer(5)]));
}

#[test]
fn range_tombstone_boundary_reopens_new_range() {
    // EXCL_END_INCL_START_BOUNDARY (kind 2) closes the prev range and opens a new
    // one (inclusive start) using the secondary deletion time.
    let mut shadow = PartitionShadow::open(0, None, vec![false]);
    shadow
        .feed_range_marker(vec![Value::Integer(10)], 2, 5000, Some(6000))
        .unwrap();
    // New range starts inclusive at ck=10 with deleted_at=6000.
    assert!(shadow.row_hidden(&row(5500, None, None, false), &[Value::Integer(12)]));
    assert!(!shadow.row_hidden(&row(7000, None, None, false), &[Value::Integer(12)]));
}

#[test]
fn unknown_bound_kind_is_rejected() {
    let mut shadow = PartitionShadow::open(0, None, vec![false]);
    assert!(shadow
        .feed_range_marker(vec![Value::Integer(1)], 9, 1, None)
        .is_err());
}

#[test]
fn ttl_expired_row_is_hidden_and_live_ttl_is_kept() {
    // now = 1_000_000 epoch-seconds.
    let now = 1_000_000i64;
    let shadow = PartitionShadow::open(now, None, vec![]);
    // Row-liveness TTL already expired, no live-forever cell → hidden.
    assert!(shadow.row_hidden(&row(10, Some(500_000), None, false), &[]));
    // Row-liveness TTL still in the future → visible.
    assert!(!shadow.row_hidden(&row(10, Some(2_000_000), None, false), &[]));
    // Expired liveness but a live-forever data cell (later UPDATE) → visible.
    assert!(!shadow.row_hidden(&row(10, Some(500_000), None, true), &[]));
    // Expired liveness, only expiring cells whose max expiry is still future → visible.
    assert!(!shadow.row_hidden(&row(10, Some(500_000), Some(2_000_000), false), &[]));
    // Row with NO TTL anywhere is never TTL-hidden.
    assert!(!shadow.row_hidden(&row(10, None, None, false), &[]));
}

/// Issue #1741 (Finding 1, test 2 / per-cell drop decision): a data cell is
/// dropped when its effective write ts is shadowed by the covering deletion OR it
/// is TTL-expired at `now`; a cell without an authoritative write ts is never
/// shadowed. The per-cell TTL branch is pinned here (not end-to-end) because the
/// writer stamps an expiring cell's `localDeletionTime` as `now + ttl`, so a
/// PAST-expired per-cell TTL is not synthesizable via a fresh writer flush; the
/// row-data cell loop calls exactly this function.
///
/// Revert-verify: dropping the `expired` term makes the TTL assertions FALSE;
/// dropping the `shadowed` term makes the shadow assertions FALSE.
#[test]
fn per_cell_shadow_and_ttl_drop_decision() {
    use super::PartitionShadow as PS;
    let now = 1_000_000i64;
    // Shadow: cell older/equal to the covering deletion (2000µs) → dropped.
    assert!(PS::cell_shadowed_or_expired(
        Some(2000),
        now,
        Some(1000),
        None
    ));
    assert!(PS::cell_shadowed_or_expired(
        Some(2000),
        now,
        Some(2000),
        None
    ));
    // Shadow: cell strictly newer than the deletion → kept.
    assert!(!PS::cell_shadowed_or_expired(
        Some(2000),
        now,
        Some(3000),
        None
    ));
    // No covering deletion → never shadowed.
    assert!(!PS::cell_shadowed_or_expired(None, now, Some(1), None));
    // No authoritative write ts → never shadowed (no-heuristics).
    assert!(!PS::cell_shadowed_or_expired(Some(2000), now, None, None));
    // TTL: expiry at/BEFORE now → expired → dropped (regardless of covering).
    assert!(PS::cell_shadowed_or_expired(
        None,
        now,
        Some(9999),
        Some(now)
    ));
    assert!(PS::cell_shadowed_or_expired(
        None,
        now,
        Some(9999),
        Some(now - 1)
    ));
    // TTL: expiry in the future → kept.
    assert!(!PS::cell_shadowed_or_expired(
        None,
        now,
        Some(9999),
        Some(now + 1)
    ));
}

/// Issue #1741 (Finding 1, test 3): a row whose non-key data cells are ALL
/// shadowed/expired and which has no live pk-liveness marker must be hidden. The
/// row-data cell loop drops the stale cells from the emitted map BUT still folds
/// their write ts / expiry into the row aggregate, so `row_hidden` recognises the
/// reduced row as fully shadowed/expired. A single decoded-then-dropped cell
/// leaves `max_data_cell_timestamp = Some(<= covering)` (NOT the `i64::MIN`
/// sentinel a truncated no-cell parse leaves), which is exactly the signal that
/// distinguishes a genuinely reduced row (hidden) from a truncated parse (kept).
///
/// Revert-verify: if a shadowed/expired cell were EXCLUDED from the aggregate
/// (leaving `None`), `max_write_timestamp` would be the `i64::MIN` sentinel and
/// the first two assertions (hidden) would become FALSE.
#[test]
fn reduced_to_primary_key_row_is_hidden() {
    // (a) All non-key cells shadowed by a partition tombstone (2000µs), no live
    //     marker: the dropped cells' ts folds to max_data_cell_timestamp <= 2000,
    //     so row_hidden shadows the whole row.
    let shadow = PartitionShadow::open(0, Some((2000, 12345)), vec![]);
    let mut h = row(0, None, None, false);
    h.timestamp = None; // UPDATE-only row (no pk-liveness marker)
    h.max_data_cell_timestamp = Some(1500); // newest shadowed cell, <= 2000
    assert!(shadow.row_hidden(&h, &[]));

    // (b) All non-key cells TTL-expired (no covering deletion), no live marker:
    //     the dropped cells fold an expiry in the past and are NOT live-forever.
    let now = 1_000_000i64;
    let shadow_ttl = PartitionShadow::open(now, None, vec![]);
    let mut h2 = row(0, None, Some(now - 1), false);
    h2.timestamp = None;
    assert!(shadow_ttl.row_hidden(&h2, &[]));

    // (c) A truncated / marker-only parse (NO decoded cells → sentinel) is NOT
    //     hidden — it is a partial read, not a genuinely reduced row.
    let mut h3 = row(0, None, None, false);
    h3.timestamp = None;
    h3.max_data_cell_timestamp = None;
    assert!(!shadow.row_hidden(&h3, &[]));

    // (d) A surviving non-key cell newer than the deletion keeps the row visible.
    let mut h4 = row(0, None, None, false);
    h4.timestamp = None;
    h4.max_data_cell_timestamp = Some(3000); // > 2000
    assert!(!shadow.row_hidden(&h4, &[]));
}

/// Issue #1849: the multi-generation read path's row-level partition-shadow
/// reuse. A merged row is hidden by a partition tombstone iff its max DATA-cell
/// write ts is `<= markedForDeleteAt`; a row with no data-cell ts (`None`) or no
/// cover is NEVER hidden. Revert-verify: dropping the `<=` (using `<`) makes the
/// exact-boundary assertion FALSE; returning `true` on `None` cover would hide
/// live rows.
#[cfg(feature = "write-support")]
#[test]
fn merged_row_partition_shadow_reuse() {
    use super::merged_row_shadowed_by_partition as f;
    // Signature: (cover, marker_ts, max_data_cell_ts, has_deleted_data_cell).
    // No cover → never hidden.
    assert!(!f(None, None, Some(1_000), false));
    // Cover but no evidence at all (pk-only / undecodable) → never hidden.
    assert!(!f(Some(2_000), None, None, false));
    // Data older than the deletion → hidden.
    assert!(f(Some(2_000), None, Some(1_000), false));
    // Data exactly at the deletion → hidden (deletes: ts <= markedForDeleteAt).
    assert!(f(Some(2_000), None, Some(2_000), false));
    // Data strictly newer than the deletion → survives.
    assert!(!f(Some(2_000), None, Some(3_000), false));
}

/// Issue #3094 (round-4 blocker): the two evidence channels the MULTI-GENERATION
/// caller threads in — the surviving liveness marker's timestamp and the mere
/// PRESENCE of a merged cell tombstone. Presence may only ever defeat the `i64::MIN`
/// fail-safe (hidden-ward); the marker is the one piece of LIVE evidence that can
/// keep such a row visible.
///
/// Revert-verify: hardcoding `has_deleted_data_cell: false` makes the first assertion
/// FALSE (the phantom row #3094 closes); hardcoding `timestamp: None` makes the third
/// FALSE (a row whose marker outlives the deletion would be wrongly hidden).
#[cfg(feature = "write-support")]
#[test]
fn merged_row_shadow_uses_tombstone_presence_and_the_surviving_marker() {
    use super::merged_row_shadowed_by_partition as f;
    // Tombstone presence alone, no live evidence → the fail-safe is defeated → hidden.
    assert!(f(Some(5_000), None, None, true));
    // No cover → presence hides nothing (#3121 residual).
    assert!(!f(None, None, None, true));
    // A surviving liveness marker NEWER than the deletion keeps the row visible even
    // with a tombstone present (Cassandra: the marker outlives the deletion).
    assert!(!f(Some(5_000), Some(7_000), None, true));
    // A marker at/below the deletion is covered by it → still hidden.
    assert!(f(Some(5_000), Some(5_000), None, true));
    // Live data newer than the deletion keeps the row visible.
    assert!(!f(Some(5_000), None, Some(9_000), true));
}

/// Issue #3094: the cell-tombstone drop decision. A DELETED CELL is dropped
/// from a user-facing row (so the column reads NULL) and kept verbatim on the
/// physical read paths (so compaction / sstabledump / delta-scan streams stay
/// byte-unchanged with their authoritative deletion timestamps, #505).
///
/// Revert-verify: hardcoding `false` (the pre-fix behaviour) makes the first
/// assertion FALSE and a deleted cell reaches the Arrow encoder as a raw
/// `Value::Tombstone`, failing the whole `do_get` stream; hardcoding `true`
/// makes the third assertion FALSE and silently strips cell tombstones out of
/// the compaction merge input, resurrecting deleted cells on rewrite.
#[test]
fn cell_tombstone_dropped_only_on_the_user_facing_path() {
    use super::PartitionShadow as PS;
    // User-facing read + a cell tombstone → dropped (column reads NULL).
    assert!(PS::cell_tombstone_dropped(true, true));
    // User-facing read + a live cell → kept (the tombstone rule must not
    // touch live data; its shadow/TTL fate is `cell_shadowed_or_expired`).
    assert!(!PS::cell_tombstone_dropped(true, false));
    // PHYSICAL read + a cell tombstone → kept verbatim (#505).
    assert!(!PS::cell_tombstone_dropped(false, true));
    // Physical read + a live cell → kept.
    assert!(!PS::cell_tombstone_dropped(false, false));
}

/// Issue #3094 (nit 3): only a `TombstoneType::CellTombstone` is a DELETED CELL.
/// The simple-cell loop cannot surface any other tombstone shape today, so this
/// pins that the predicate names the variant instead of accepting the broader
/// `Value::Tombstone` discriminant — a row/range/partition tombstone value must
/// NOT be silently dropped as if it were a deleted cell (no-heuristics, #28).
#[test]
fn only_a_cell_tombstone_counts_as_a_deleted_cell() {
    fn tombstone(kind: TombstoneType) -> Value {
        Value::Tombstone(Box::new(TombstoneInfo {
            deletion_time: 1_000,
            tombstone_type: kind,
            local_deletion_time: 1,
            ttl: None,
            range_start: None,
            range_end: None,
        }))
    }
    assert!(PartitionShadow::is_cell_tombstone(&tombstone(
        TombstoneType::CellTombstone
    )));
    for other in [
        TombstoneType::RowTombstone,
        TombstoneType::RangeTombstone,
        TombstoneType::PartitionTombstone,
        TombstoneType::TtlExpiration,
    ] {
        assert!(
            !PartitionShadow::is_cell_tombstone(&tombstone(other)),
            "{other:?} is not a deleted CELL and must not be dropped as one"
        );
    }
    // A live value is obviously not a deleted cell.
    assert!(!PartitionShadow::is_cell_tombstone(&Value::Integer(7)));
}

/// Issue #3094 (blocker): tombstone evidence DEFEATS the `i64::MIN` fail-safe and
/// NOTHING ELSE — a pure presence predicate cannot carry a timestamp into the row
/// maximum, which is what makes the fix one-directional (visible → hidden only).
/// Revert-verify: dropping the `|| has_deleted_data_cell` term makes the first
/// assertion FALSE (a deletion-only row resurrects inside a deleted partition);
/// returning `true` unconditionally makes the last FALSE (hiding truncated parses,
/// against the #28 fail-safe).
#[test]
fn tombstone_presence_defeats_the_failsafe_without_a_timestamp() {
    let f = PartitionShadow::has_shadow_evidence;
    // Deletion-only row: no live write at all, but a decoded tombstone cell.
    assert!(f(i64::MIN, true));
    // Live write evidence needs no tombstone.
    assert!(f(1_000, false));
    assert!(f(1_000, true));
    // Nothing decoded at all → fail-safe holds (never hide a truncated parse).
    assert!(!f(i64::MIN, false));
}

/// Issue #3094 (blocker, header-level consequence): both resurrection shapes —
/// (a) no liveness marker, only a tombstone cell; (b) a live liveness marker at
/// 1_000µs, no live data cell, and a tombstone cell at 6_000µs under a deletion
/// at 5_000µs, which must stay HIDDEN because the tombstone contributes no
/// timestamp. Revert-verify: folding that 6_000µs into `max_data_cell_timestamp`
/// (the pre-fix "fallback max") makes (b) FALSE — an all-null phantom row out of
/// a deleted partition. End-to-end pin:
/// `cqlite-core/tests/issue_3094_partition_deleted_row_not_resurrected.rs`.
#[test]
fn deletion_only_and_liveness_only_rows_are_shadowed_by_a_covering_deletion() {
    // (a) No liveness marker (UPDATE-only); the row's ONLY cell is a tombstone.
    let mut h = row(0, None, None, false);
    h.timestamp = None;
    h.max_data_cell_timestamp = None;
    h.has_deleted_data_cell = true;
    assert!(PartitionShadow::open(0, Some((5_000, 12345)), vec![]).row_hidden(&h, &[]));

    // (b) Live liveness marker @1_000, no live data cell, tombstone cell (whose
    //     6_000µs write ts must NOT reach the aggregate) under a deletion @5_000.
    let mut h2 = row(1_000, None, None, false);
    h2.max_data_cell_timestamp = None;
    h2.has_deleted_data_cell = true;
    assert!(PartitionShadow::open(0, Some((5_000, 12345)), vec![]).row_hidden(&h2, &[]));
    // Same row under an OLDER deletion @500: the liveness marker survives it, so
    // the row stays visible on LIVE evidence.
    assert!(!PartitionShadow::open(0, Some((500, 12345)), vec![]).row_hidden(&h2, &[]));

    // (c) A live data cell newer than the deletion still keeps the row visible,
    //     tombstone presence notwithstanding.
    let mut h3 = row(1_000, None, None, false);
    h3.max_data_cell_timestamp = Some(9_000);
    h3.has_deleted_data_cell = true;
    assert!(!PartitionShadow::open(0, Some((5_000, 12345)), vec![]).row_hidden(&h3, &[]));
}

/// Issue #1741 (Finding 2, header-level consequence): a row whose aggregated
/// max data-cell timestamp (which now folds in a non-frozen collection's newest
/// `max_element_writetime`) exceeds the covering deletion survives, even when
/// the row-liveness `timestamp` predates the deletion. The end-to-end fold is
/// pinned by `collection_element_newer_than_partition_tombstone_survives`; this
/// pins the decision the fold feeds.
#[test]
fn newer_collection_element_ts_keeps_row_visible() {
    let shadow = PartitionShadow::open(0, Some((2000, 12345)), vec![]);
    // Row marker older than the deletion (1000 <= 2000) but a folded element ts
    // (3000) newer than it → NOT shadowed → visible.
    let mut h = row(1000, None, None, false);
    h.max_data_cell_timestamp = Some(3000);
    assert!(!shadow.row_hidden(&h, &[]));
    // Without the fold (max cell ts == row ts == 1000 <= 2000) → shadowed.
    let mut h_prefold = row(1000, None, None, false);
    h_prefold.max_data_cell_timestamp = Some(1000);
    assert!(shadow.row_hidden(&h_prefold, &[]));
}
