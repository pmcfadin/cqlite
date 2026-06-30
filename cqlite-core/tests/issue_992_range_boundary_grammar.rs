//! Issue #992 (Epic #969): strict byte-for-byte coverage for Cassandra 5.0
//! `Data.db` range-tombstone BOUND markers AND BOUNDARY markers.
//!
//! This is the range/boundary half of the issue #992 suite; the TTL cell and
//! partition/row/cell tombstone byte parity lives in the sibling
//! `issue_992_ttl_tombstone_parity.rs`. The two files were split from the
//! original `issue_992_ttl_tombstone_range_parity.rs` (issue #1267) to keep each
//! `#[test]` file under the file-size ratchet (#1135) WITHOUT
//! `CQLITE_ALLOW_FILE_GROWTH`. The split is purely a move/reorganize — every test
//! keeps its name and assertions.
//!
//! Cassandra oracle: `ClusteringBoundOrBoundary.Serializer` (range-tombstone
//! marker grammar: kind ordinal, u16 cluster count, marker_body_size/prev_size,
//! and ONE deletion-time pair for a bound vs TWO for a boundary). Local Cassandra
//! source: `RangeTombstoneTest.java`, `RangeTombstoneListTest.java`,
//! `CQLSSTableWriterTest.java`.
//!
//! Two assertion families per criterion 5:
//!   * BYTE/OFFSET parity — walk the real (decompressed) `Data.db` (or the
//!     deterministic writer output) and assert the EXACT marker bytes, kind
//!     ordinals, field ordering, deletion-time deltas, and byte offsets.
//!   * JSONL/semantic parity — cross-check the SAME fixture's decoded
//!     timestamps/inclusivity/clustering against the sstabledump JSONL golden.
//!
//! Skip-on-absence / fail-on-0-when-present (local-only-fixtures doctrine): the
//! `test_deltas/*` fixtures are LOCAL-ONLY (not yet in the pinned CI dataset), so
//! the fixture lanes SKIP when the `*-Data.db` binary is absent — but when it is
//! present they FAIL if the body is empty or they find 0 markers. The
//! deterministic writer lanes (which exercise the bound-marker grammar and the
//! issue #1220 boundary coalescing through the public `DataWriter` surface) run
//! everywhere, so coverage of the marker forms is never lost to a skip.
//!
//! Shared offset-context helpers + fixture access live in
//! `issue_992_ttl_tombstone_range_parity_helpers/mod.rs` (a `tests/` SUBDIRECTORY
//! module, so it is NOT its own test binary).

#![cfg(feature = "write-support")]

use cqlite_core::storage::write_engine::mutation::{
    ClusteringBound, ClusteringKey, Mutation, PartitionKey, RangeTombstone, TableId,
};
use cqlite_core::types::Value;

#[path = "issue_992_ttl_tombstone_range_parity_helpers/mod.rs"]
mod helpers;
use helpers::*;

// ===========================================================================
// Section 5 — RANGE TOMBSTONE BOUND markers.
// (manifest: cass.data_db_decode.range_tombstone.bound_markers)
// (acceptance criterion 4)
//
// Cassandra oracle: ClusteringBoundOrBoundary.Serializer — a bound marker is
// [IS_MARKER 0x02][kind ordinal u8][cluster_count u16 BE][clustering prefix]
// [marker_body_size VInt][prev_size VInt][mfda delta][ldt delta]. A BOUND carries
// ONE deletion-time pair; the kind ordinal encodes both side (start/end) and
// inclusivity.
// ===========================================================================

/// Deterministic BYTE parity for a START + END bound pair (the form CQLite's
/// writer emits). Assert the EXACT marker grammar at absolute offsets: flag,
/// kind ordinal (inclusivity + side), u16 cluster count, clustering value,
/// body_size/prev_size, and the single mfda/ldt deltas.
#[test]
fn range_bound_markers_exact_grammar_writer() {
    let schema = int_clustering_schema();
    // Open inclusive [2 .. 4) close exclusive — a single range tombstone.
    let rt = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(2))),
        end: ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(4))),
        deletion_time: 1_500_000, // mfda µs
        local_deletion_time: 1_700,
    };
    let m = Mutation::new(
        TableId::new("issue992", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![write_op("val", "x")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m], None, &[rt]);

    // Find the first IS_MARKER byte after the header (the open START bound).
    let start = find_marker(&bytes, INT_PK_HEADER_SIZE, INT_CLUSTERING)
        .expect("a range tombstone START marker");
    let (start_next, start_kind, start_ck, start_mfda, start_ldt) =
        walk_bound_marker(&bytes, start);
    fail_flag(
        read_u8_loc(&bytes, start),
        IS_MARKER,
        "START marker IS_MARKER flag",
    );
    assert_eq!(
        start_kind, INCL_START_BOUND,
        "open inclusive START bound kind ordinal must be INCL_START_BOUND (1)"
    );
    assert_eq!(start_ck, Some(2), "START bound clustering value must be 2");
    fail_vint(start_mfda, 1_500_000 - 1_000_000, "START bound mfda delta");
    fail_vint(start_ldt, 1_700, "START bound ldt delta");

    // The END bound marker immediately follows.
    let end =
        find_marker(&bytes, start_next, INT_CLUSTERING).expect("a range tombstone END marker");
    assert_eq!(
        end, start_next,
        "END marker must directly follow the START marker"
    );
    let (_end_next, end_kind, end_ck, end_mfda, end_ldt) = walk_bound_marker(&bytes, end);
    assert_eq!(
        end_kind, EXCL_END_BOUND,
        "close exclusive END bound kind ordinal must be EXCL_END_BOUND (0)"
    );
    assert_eq!(end_ck, Some(4), "END bound clustering value must be 4");
    fail_vint(
        end_mfda,
        1_500_000 - 1_000_000,
        "END bound mfda delta (same as start)",
    );
    fail_vint(end_ldt, 1_700, "END bound ldt delta (same as start)");
}

/// FIXTURE BYTE parity for bound markers: real range_tombstones Data.db PK=2
/// (golden line 2) has an INCLUSIVE-start [2] / EXCLUSIVE-end [4] range. Walk
/// both bound markers and assert kind ordinals (inclusivity + side), the prefix
/// clustering value, and the single mfda/ldt deltas reconstructing the golden µs.
#[test]
fn fixture_range_bound_markers_byte_parity() {
    let Some((raw, jsonl)) = load_local_only(RANGE_TOMBSTONES_DIR) else {
        eprintln!(
            "SKIP fixture_range_bound_markers_byte_parity: local-only fixture absent \
             (covered deterministically by range_bound_markers_exact_grammar_writer)"
        );
        return;
    };
    let p2 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"2\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=2"));
    assert!(
        p2.contains(
            "\"range_tombstone_bound\",\"start\":{\"type\":\"inclusive\",\"clustering\":[2,\"*\"]"
        ) && p2.contains("\"end\":{\"type\":\"exclusive\",\"clustering\":[4,\"*\"]"),
        "PK=2 must carry an inclusive-start [2] / exclusive-end [4] range bound pair: {p2}"
    );
    let golden_mfda = golden_first_range_marked_deleted_micros(p2);

    // PK=2 partition starts at golden position; walk past the header to the rows.
    let p2_pos = golden_partition_position(p2);
    let rows_start = partition_rows_start(&raw, p2_pos);
    let start = find_marker(&raw, rows_start, INT_TEXT_CLUSTERING)
        .expect("a range tombstone START marker in PK=2");
    let (start_next, start_kind, start_ck, start_mfda, start_ldt) = walk_bound_marker(&raw, start);
    assert_eq!(
        start_kind, INCL_START_BOUND,
        "PK=2 START bound must be INCL_START_BOUND (1); got kind {start_kind} @ {start}"
    );
    assert_eq!(
        start_ck,
        Some(2),
        "PK=2 START bound prefix clustering value must be 2 (the '*' second component is omitted)"
    );
    let abs_start_mfda = minima::RANGE_MIN_TS + start_mfda.value as i64;
    assert_eq!(
        abs_start_mfda,
        golden_mfda,
        "START bound mfda delta {} (at offset {}) + min {} = {} must equal the golden \
         marked_deleted µs {}",
        start_mfda.value,
        start_mfda.start,
        minima::RANGE_MIN_TS,
        abs_start_mfda,
        golden_mfda
    );
    let abs_start_ldt = minima::SHARED_MIN_LDT + start_ldt.value as i64;
    assert_eq!(
        abs_start_ldt,
        minima::SHARED_MIN_LDT,
        "START bound ldt (delta {} + min) must equal the golden local_delete_time secs",
        start_ldt.value
    );

    let end = find_marker(&raw, start_next, INT_TEXT_CLUSTERING)
        .expect("a range tombstone END marker in PK=2");
    let (_end_next, end_kind, end_ck, end_mfda, _end_ldt) = walk_bound_marker(&raw, end);
    assert_eq!(
        end_kind, EXCL_END_BOUND,
        "PK=2 END bound must be EXCL_END_BOUND (0); got kind {end_kind} @ {end}"
    );
    assert_eq!(
        end_ck,
        Some(4),
        "PK=2 END bound prefix clustering value must be 4"
    );
    let abs_end_mfda = minima::RANGE_MIN_TS + end_mfda.value as i64;
    assert_eq!(
        abs_end_mfda, golden_mfda,
        "END bound mfda must equal the START bound / golden µs (a simple range shares one mfda)"
    );
}

/// FIXTURE: PK=1 of range_tombstones is an INCLUSIVE-start [2] / INCLUSIVE-end
/// [2] single-point range — anchors the INCL_END_BOUND (6) ordinal at the byte
/// level distinct from the exclusive end above.
#[test]
fn fixture_range_inclusive_end_bound_kind() {
    let Some((raw, jsonl)) = load_local_only(RANGE_TOMBSTONES_DIR) else {
        eprintln!("SKIP fixture_range_inclusive_end_bound_kind: local-only fixture absent");
        return;
    };
    let p1 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"1\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=1"));
    assert!(
        p1.contains("\"end\":{\"type\":\"inclusive\",\"clustering\":[2,\"*\"]"),
        "PK=1 must carry an inclusive-end [2] bound: {p1}"
    );
    let p1_pos = golden_partition_position(p1);
    let rows_start = partition_rows_start(&raw, p1_pos);
    let start = find_marker(&raw, rows_start, INT_TEXT_CLUSTERING).expect("PK=1 START marker");
    let (start_next, start_kind, _ck, _mfda, _ldt) = walk_bound_marker(&raw, start);
    assert_eq!(start_kind, INCL_START_BOUND, "PK=1 START must be inclusive");
    let end = find_marker(&raw, start_next, INT_TEXT_CLUSTERING).expect("PK=1 END marker");
    let (_n, end_kind, end_ck, _m, _l) = walk_bound_marker(&raw, end);
    assert_eq!(
        end_kind, INCL_END_BOUND,
        "PK=1 END bound must be INCL_END_BOUND (6); got {end_kind} @ {end}"
    );
    assert_eq!(end_ck, Some(2), "PK=1 END bound clustering value must be 2");
}

// ===========================================================================
// Section 6 — RANGE TOMBSTONE BOUNDARY markers (two deletion-time pairs).
// (manifest: cass.data_db_decode.range_tombstone.boundary_markers)
// (acceptance criterion 4)
//
// Cassandra oracle: a BOUNDARY marker (kind 2 = EXCL_END_INCL_START_BOUNDARY or
// 5 = INCL_END_EXCL_START_BOUNDARY) closes one range and opens the next at the
// SAME clustering point. Its body carries TWO deletion-time pairs (primary = end
// of the previous range, secondary = start of the new range). As of issue #1220
// CQLite's writer COALESCES two adjacent range tombstones that share a boundary
// point (complementary inclusivity) into exactly this marker, so the form now has
// a deterministic writer round-trip lane in addition to the fixture byte walks.
// ===========================================================================

/// Deterministic BYTE parity for a coalesced BOUNDARY marker (issue #1220), the
/// boundary sibling of `range_bound_markers_exact_grammar_writer`. Two ADJACENT
/// range tombstones meeting at clustering [4] — rt1 closes EXCLUSIVE(4), rt2 opens
/// INCLUSIVE(4) — must be emitted by the public `DataWriter` as a SINGLE kind-2
/// (EXCL_END_INCL_START) boundary marker carrying TWO deletion-time pairs, NOT as
/// a separate end-BOUND + start-BOUND pair. Assert the EXACT marker grammar at
/// absolute offsets: the bracketing rt1 START / rt2 END bounds, the boundary's
/// IS_MARKER flag + kind-2 ordinal + u16 count + clustering value, and BOTH
/// deletion-time pairs (primary = rt1's end, secondary = rt2's start).
#[test]
fn range_boundary_marker_exact_grammar_writer() {
    let schema = int_clustering_schema();
    // rt1 = [2 .. 4) (close EXCLUSIVE at 4), rt2 = [4 .. 6) (open INCLUSIVE at 4).
    let rt1 = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(2))),
        end: ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(4))),
        deletion_time: 1_500_000, // end/PRIMARY mfda µs
        local_deletion_time: 1_700,
    };
    let rt2 = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(4))),
        end: ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(6))),
        deletion_time: 1_800_000, // start/SECONDARY mfda µs
        local_deletion_time: 1_900,
    };
    let m = Mutation::new(
        TableId::new("issue992", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![write_op("val", "x")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m], None, &[rt1, rt2]);

    // (1) First marker after the header: rt1's open START bound at [2].
    let start = find_marker(&bytes, INT_PK_HEADER_SIZE, INT_CLUSTERING)
        .expect("a range tombstone START marker (rt1.start)");
    let (after_start, start_kind, start_ck, _sm, _sl) = walk_bound_marker(&bytes, start);
    assert_eq!(
        start_kind, INCL_START_BOUND,
        "rt1 open inclusive START bound kind ordinal must be INCL_START_BOUND (1)"
    );
    assert_eq!(
        start_ck,
        Some(2),
        "rt1 START bound clustering value must be 2"
    );

    // (2) The COALESCED BOUNDARY marker at [4] — the heart of issue #1220.
    let boundary = find_marker(&bytes, after_start, INT_CLUSTERING)
        .expect("a range tombstone BOUNDARY marker at [4]");
    fail_flag(
        read_u8_loc(&bytes, boundary),
        IS_MARKER,
        "boundary marker IS_MARKER flag",
    );
    assert_eq!(
        bytes[boundary + 1],
        EXCL_END_INCL_START_BOUNDARY,
        "adjacent exclusive-end(4) / inclusive-start(4) ranges MUST coalesce into a single \
         kind-2 EXCL_END_INCL_START_BOUNDARY marker — not separate end+start bounds"
    );
    let (b_kind, b_ck, primary, secondary) = walk_boundary_marker(&bytes, boundary);
    assert_eq!(
        b_kind, EXCL_END_INCL_START_BOUNDARY,
        "boundary kind ordinal"
    );
    assert_eq!(b_ck, Some(4), "boundary clustering value must be 4");
    // PRIMARY pair = rt1's end (close of the previous range).
    fail_vint(
        primary,
        1_500_000 - 1_000_000,
        "boundary PRIMARY (end) mfda delta = rt1",
    );
    let primary_ldt = read_uvint_loc(&bytes, primary.end());
    fail_vint(primary_ldt, 1_700, "boundary PRIMARY (end) ldt delta = rt1");
    // SECONDARY pair = rt2's start (open of the next range).
    fail_vint(
        secondary,
        1_800_000 - 1_000_000,
        "boundary SECONDARY (start) mfda delta = rt2",
    );
    let secondary_ldt = read_uvint_loc(&bytes, secondary.end());
    fail_vint(
        secondary_ldt,
        1_900,
        "boundary SECONDARY (start) ldt delta = rt2",
    );

    // (3) rt2's close EXCLUSIVE END bound at [6] follows the boundary body.
    let end = find_marker(&bytes, secondary_ldt.end(), INT_CLUSTERING)
        .expect("a range tombstone END marker (rt2.end)");
    let (_after_end, end_kind, end_ck, end_mfda, end_ldt) = walk_bound_marker(&bytes, end);
    assert_eq!(
        end_kind, EXCL_END_BOUND,
        "rt2 close exclusive END bound kind ordinal must be EXCL_END_BOUND (0)"
    );
    assert_eq!(end_ck, Some(6), "rt2 END bound clustering value must be 6");
    fail_vint(
        end_mfda,
        1_800_000 - 1_000_000,
        "rt2 END bound mfda delta = rt2",
    );
    fail_vint(end_ldt, 1_900, "rt2 END bound ldt delta = rt2");
}

/// Deterministic BYTE parity for a coalesced KIND-5 BOUNDARY marker (issue #1220,
/// roborev finding 1 — the kind-5 sibling of `range_boundary_marker_exact_grammar_writer`).
/// Two ADJACENT range tombstones meeting at clustering [4] — rt1 closes INCLUSIVE(4),
/// rt2 opens EXCLUSIVE(4) — must be emitted by the public `DataWriter` as a SINGLE
/// kind-5 (INCL_END_EXCL_START) boundary carrying TWO deletion-time pairs, NOT a
/// separate INCL_END + EXCL_START bound pair. A LIVE row also sits at clustering [4]
/// (its write timestamp outranks the covering rt1, so it survives): because the
/// kind-5 boundary's bounds are close-inclusive / open-exclusive (comparedToClustering
/// = +1), the boundary MUST sort AFTER that row at the same clustering. Assert the
/// EXACT marker grammar at absolute offsets: the row-then-boundary positioning, the
/// boundary's IS_MARKER flag + kind-5 ordinal + u16 count + clustering value, and
/// BOTH deletion-time pairs (primary = rt1's end/close, secondary = rt2's start/open).
#[test]
fn range_boundary_marker_kind5_exact_grammar_writer() {
    let schema = int_clustering_schema();
    // rt1 = [2 .. 4] (close INCLUSIVE at 4), rt2 = (4 .. 6] (open EXCLUSIVE at 4).
    let rt1 = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(2))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(4))),
        deletion_time: 1_500_000, // end/PRIMARY mfda µs
        local_deletion_time: 1_700,
    };
    let rt2 = RangeTombstone {
        start: ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(4))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(6))),
        deletion_time: 1_800_000, // start/SECONDARY mfda µs
        local_deletion_time: 1_900,
    };
    // A live row at the boundary clustering [4]. rt1 [2..4] inclusive covers it, but
    // the row's timestamp (2_000_000) outranks rt1's deletion (1_500_000), so the row
    // survives and we can prove the boundary sorts AFTER it.
    let row_at_boundary = Mutation::new(
        TableId::new("issue992", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(4))),
        vec![write_op("val", "y")],
        2_000_000,
        None,
    );
    let m = Mutation::new(
        TableId::new("issue992", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![write_op("val", "x")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(
        det_stats(),
        &schema,
        1,
        &[m, row_at_boundary],
        None,
        &[rt1, rt2],
    );

    // (1) First marker after the header: rt1's open INCLUSIVE START bound at [2].
    let start = find_marker(&bytes, INT_PK_HEADER_SIZE, INT_CLUSTERING)
        .expect("a range tombstone START marker (rt1.start)");
    let (after_start, start_kind, start_ck, _sm, _sl) = walk_bound_marker(&bytes, start);
    assert_eq!(
        start_kind, INCL_START_BOUND,
        "rt1 open inclusive START bound kind ordinal must be INCL_START_BOUND (1)"
    );
    assert_eq!(
        start_ck,
        Some(2),
        "rt1 START bound clustering value must be 2"
    );

    // (2) The LIVE row at clustering [4] must appear BEFORE the boundary at [4].
    // The first unfiltered after rt1.start is that regular row (not a marker / EOP).
    let row4 = after_start;
    assert!(
        bytes[row4] & (IS_MARKER | END_OF_PARTITION) == 0,
        "expected a regular row (not a marker/EOP) at clustering [4] before the kind-5 \
         boundary; got leading byte 0x{:02X} @ {row4}",
        bytes[row4]
    );
    let row4_header = read_uvint_loc(&bytes, row4 + 1);
    let row4_ck = read_be_i32(&bytes, row4_header.end(), "row@4 clustering value");
    assert_eq!(row4_ck, 4, "the surviving row must be at clustering [4]");

    // (3) The COALESCED KIND-5 BOUNDARY marker at [4] — the heart of finding 1. It
    // sits AFTER the row@4 (close-inclusive / open-exclusive => comparedToClustering +1).
    let boundary = find_marker(&bytes, after_start, INT_CLUSTERING)
        .expect("a range tombstone BOUNDARY marker at [4]");
    assert!(
        boundary > row4,
        "kind-5 boundary (close-inclusive/open-exclusive, weight +1) MUST sort AFTER the row \
         at the same clustering [4]: boundary @ {boundary}, row @ {row4}"
    );
    fail_flag(
        read_u8_loc(&bytes, boundary),
        IS_MARKER,
        "boundary marker IS_MARKER flag",
    );
    assert_eq!(
        bytes[boundary + 1],
        INCL_END_EXCL_START_BOUNDARY,
        "adjacent inclusive-end(4) / exclusive-start(4) ranges MUST coalesce into a single \
         kind-5 INCL_END_EXCL_START_BOUNDARY marker — not separate end+start bounds"
    );
    let (b_kind, b_ck, primary, secondary) = walk_boundary_marker(&bytes, boundary);
    assert_eq!(
        b_kind, INCL_END_EXCL_START_BOUNDARY,
        "boundary kind ordinal must be 5"
    );
    assert_eq!(b_ck, Some(4), "boundary clustering value must be 4");
    // PRIMARY pair = rt1's end (close of the previous range).
    fail_vint(
        primary,
        1_500_000 - 1_000_000,
        "boundary PRIMARY (end) mfda delta = rt1",
    );
    let primary_ldt = read_uvint_loc(&bytes, primary.end());
    fail_vint(primary_ldt, 1_700, "boundary PRIMARY (end) ldt delta = rt1");
    // SECONDARY pair = rt2's start (open of the next range).
    fail_vint(
        secondary,
        1_800_000 - 1_000_000,
        "boundary SECONDARY (start) mfda delta = rt2",
    );
    let secondary_ldt = read_uvint_loc(&bytes, secondary.end());
    fail_vint(
        secondary_ldt,
        1_900,
        "boundary SECONDARY (start) ldt delta = rt2",
    );

    // (4) rt2's close INCLUSIVE END bound at [6] follows the boundary body.
    let end = find_marker(&bytes, secondary_ldt.end(), INT_CLUSTERING)
        .expect("a range tombstone END marker (rt2.end)");
    let (_after_end, end_kind, end_ck, end_mfda, end_ldt) = walk_bound_marker(&bytes, end);
    assert_eq!(
        end_kind, INCL_END_BOUND,
        "rt2 close inclusive END bound kind ordinal must be INCL_END_BOUND (6)"
    );
    assert_eq!(end_ck, Some(6), "rt2 END bound clustering value must be 6");
    fail_vint(
        end_mfda,
        1_800_000 - 1_000_000,
        "rt2 END bound mfda delta = rt2",
    );
    fail_vint(end_ldt, 1_900, "rt2 END bound ldt delta = rt2");
}

/// Coalescing must NOT depend on the order the `range_tombstones` arrive in
/// (issue #1220, roborev finding 2). Two adjacent ranges meeting at clustering [4]
/// (rt1 closes EXCLUSIVE(4), rt2 opens INCLUSIVE(4)) coalesce into a kind-2 boundary
/// when supplied in start-key order; supplying them REVERSED (rt2 before rt1) must
/// produce BYTE-IDENTICAL output and still emit the single coalesced boundary —
/// proving the Kind-ordinal sort tiebreak (close before open) is order-independent
/// rather than relying on the input vector's order surviving a stable sort.
#[test]
fn range_boundary_coalesces_regardless_of_input_order() {
    let schema = int_clustering_schema();
    let make_rts = || {
        let rt1 = RangeTombstone {
            start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(2))),
            end: ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(4))),
            deletion_time: 1_500_000,
            local_deletion_time: 1_700,
        };
        let rt2 = RangeTombstone {
            start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(4))),
            end: ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(6))),
            deletion_time: 1_800_000,
            local_deletion_time: 1_900,
        };
        (rt1, rt2)
    };
    let make_row = || {
        Mutation::new(
            TableId::new("issue992", "t"),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(1))),
            vec![write_op("val", "x")],
            2_000_000,
            None,
        )
    };

    let (rt1, rt2) = make_rts();
    let forward = write_one_partition(det_stats(), &schema, 1, &[make_row()], None, &[rt1, rt2]);
    let (rt1, rt2) = make_rts();
    let reversed = write_one_partition(det_stats(), &schema, 1, &[make_row()], None, &[rt2, rt1]);

    assert_eq!(
        forward, reversed,
        "coalescing must be independent of range_tombstone input order: forward and reversed \
         input vectors must produce byte-identical Data.db bodies"
    );

    // And the (identical) output must carry the single coalesced kind-2 boundary,
    // NOT a degenerate open-before-close bound pair.
    let start =
        find_marker(&forward, INT_PK_HEADER_SIZE, INT_CLUSTERING).expect("rt1.start bound marker");
    let (after_start, start_kind, _ck, _m, _l) = walk_bound_marker(&forward, start);
    assert_eq!(
        start_kind, INCL_START_BOUND,
        "first marker must be the open INCL_START bound at [2]"
    );
    let boundary = find_marker(&forward, after_start, INT_CLUSTERING)
        .expect("a coalesced BOUNDARY marker at [4]");
    assert_eq!(
        forward[boundary + 1],
        EXCL_END_INCL_START_BOUNDARY,
        "reversed input must STILL coalesce into a single kind-2 EXCL_END_INCL_START_BOUNDARY"
    );
    let (b_kind, b_ck, _p, _s) = walk_boundary_marker(&forward, boundary);
    assert_eq!(
        b_kind, EXCL_END_INCL_START_BOUNDARY,
        "coalesced boundary kind = 2"
    );
    assert_eq!(b_ck, Some(4), "boundary clustering value must be 4");
}

/// FIXTURE BYTE parity for a BOUNDARY marker: real adjacent_ranges Data.db PK=1
/// (golden line 1) has a `range_tombstone_boundary` at clustering [20] with
/// start inclusive / end exclusive (kind 2 = EXCL_END_INCL_START_BOUNDARY). Walk
/// it and assert: IS_MARKER flag, the kind-2 ordinal, u16 count, prefix value 20,
/// and TWO deletion-time pairs (primary end + secondary start) reconstructing the
/// golden end/start marked_deleted µs respectively.
#[test]
fn fixture_range_boundary_marker_byte_parity() {
    let Some((raw, jsonl)) = load_local_only(ADJACENT_RANGES_DIR) else {
        eprintln!(
            "SKIP fixture_range_boundary_marker_byte_parity: local-only fixture absent \
             (boundary markers are only emitted by real Cassandra; no deterministic writer lane)"
        );
        return;
    };
    let p1 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"1\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=1"));
    assert!(
        p1.contains("\"range_tombstone_boundary\""),
        "PK=1 golden must contain a range_tombstone_boundary: {p1}"
    );
    // Golden boundary: start inclusive [20] mfda ...000002, end exclusive [20]
    // mfda ...000001. Primary = end (...000001), secondary = start (...000002).
    let (golden_start_mfda, golden_end_mfda) = golden_boundary_start_end_micros(p1);

    // Scan all markers in PK=1 for the FIRST boundary (kind 2 or 5).
    let p1_pos = golden_partition_position(p1);
    let mut pos = partition_rows_start(&raw, p1_pos);
    let boundary = loop {
        let Some(mk) = find_marker(&raw, pos, INT_CLUSTERING) else {
            panic!("no boundary marker (kind 2/5) found in PK=1 of adjacent_ranges");
        };
        let kind = raw[mk + 1];
        if kind == EXCL_END_INCL_START_BOUNDARY || kind == INCL_END_EXCL_START_BOUNDARY {
            break mk;
        }
        // Skip this bound marker and continue scanning.
        let (next, _k, _ck, _m, _l) = walk_bound_marker(&raw, mk);
        pos = next;
    };

    fail_flag(
        read_u8_loc(&raw, boundary),
        IS_MARKER,
        "boundary marker IS_MARKER flag",
    );
    let (kind, ck, primary, secondary) = walk_boundary_marker(&raw, boundary);
    assert_eq!(
        kind, EXCL_END_INCL_START_BOUNDARY,
        "PK=1 first boundary must be EXCL_END_INCL_START_BOUNDARY (kind 2); got {kind} @ {boundary}"
    );
    assert_eq!(ck, Some(20), "boundary clustering value must be 20");

    // TWO deletion-time pairs: primary = end of previous range, secondary =
    // start of next range. Reconstruct against the golden end/start µs.
    let abs_primary = minima::ADJACENT_MIN_TS + primary.value as i64;
    assert_eq!(
        abs_primary,
        golden_end_mfda,
        "boundary PRIMARY mfda delta {} (at offset {}) + min {} = {} must equal the golden \
         END marked_deleted µs {} (primary = end of the previous range)",
        primary.value,
        primary.start,
        minima::ADJACENT_MIN_TS,
        abs_primary,
        golden_end_mfda
    );
    let abs_secondary = minima::ADJACENT_MIN_TS + secondary.value as i64;
    assert_eq!(
        abs_secondary,
        golden_start_mfda,
        "boundary SECONDARY mfda delta {} (at offset {}) + min {} = {} must equal the golden \
         START marked_deleted µs {} (secondary = start of the next range)",
        secondary.value,
        secondary.start,
        minima::ADJACENT_MIN_TS,
        abs_secondary,
        golden_start_mfda
    );
}

/// FIXTURE: adjacent_ranges PK=2 has a `range_tombstone_boundary` whose start is
/// EXCLUSIVE / end INCLUSIVE (kind 5 = INCL_END_EXCL_START_BOUNDARY). Anchors the
/// kind-5 ordinal at the byte level distinct from the kind-2 boundary above.
#[test]
fn fixture_range_boundary_kind5_byte_parity() {
    let Some((raw, jsonl)) = load_local_only(ADJACENT_RANGES_DIR) else {
        eprintln!("SKIP fixture_range_boundary_kind5_byte_parity: local-only fixture absent");
        return;
    };
    let p2 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"2\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=2"));
    assert!(
        p2.contains(
            "\"range_tombstone_boundary\",\"start\":{\"type\":\"exclusive\",\"clustering\":[15]"
        ) && p2.contains("\"end\":{\"type\":\"inclusive\",\"clustering\":[15]"),
        "PK=2 must carry an exclusive-start/inclusive-end boundary at [15]: {p2}"
    );
    let p2_pos = golden_partition_position(p2);
    let mut pos = partition_rows_start(&raw, p2_pos);
    let boundary = loop {
        let Some(mk) = find_marker(&raw, pos, INT_CLUSTERING) else {
            panic!("no kind-5 boundary found in PK=2");
        };
        let kind = raw[mk + 1];
        if kind == INCL_END_EXCL_START_BOUNDARY {
            break mk;
        }
        let (next, _k, _ck, _m, _l) = walk_bound_marker(&raw, mk);
        pos = next;
    };
    let (kind, ck, _primary, _secondary) = walk_boundary_marker(&raw, boundary);
    assert_eq!(
        kind, INCL_END_EXCL_START_BOUNDARY,
        "PK=2 boundary must be INCL_END_EXCL_START_BOUNDARY (kind 5); got {kind} @ {boundary}"
    );
    assert_eq!(ck, Some(15), "kind-5 boundary clustering value must be 15");
}

// ===========================================================================
// Local byte-walk helpers for range-tombstone markers (kept here, NOT in the
// shared helpers, because they encode this suite's marker-grammar expectations
// and the int32-clustering prefix shape).
// ===========================================================================

/// Clustering-column on-disk widths for the fixtures' row clustering prefixes.
/// `Int` is a fixed 4-byte big-endian value; `Text` is a VInt length prefix +
/// that many bytes. Used to skip a row's clustering prefix to reach row_size.
#[derive(Clone, Copy)]
enum ClType {
    Int,
    Text,
}

/// The row clustering layout for each fixture (a marker is found by skipping
/// regular rows, which requires knowing the clustering on-disk widths).
const INT_CLUSTERING: &[ClType] = &[ClType::Int];
const INT_TEXT_CLUSTERING: &[ClType] = &[ClType::Int, ClType::Text];

/// Offset where the row region begins for an int32-PK partition whose header
/// starts at `partition_pos`: skip [u16 key_len][key][i32 LDT][i64 mfda].
fn partition_rows_start(data: &[u8], partition_pos: usize) -> usize {
    let key_len = read_be_u16(data, partition_pos, "partition key length") as usize;
    partition_pos + 2 + key_len + 12
}

/// Find the next `IS_MARKER` (0x02) leading byte at or after `from`, stopping at
/// END_OF_PARTITION. Returns the marker offset, or `None` if the partition ends
/// first. Regular rows are skipped via their row_size framing, which requires
/// skipping the clustering prefix first (`clustering`).
fn find_marker(data: &[u8], from: usize, clustering: &[ClType]) -> Option<usize> {
    let mut pos = from;
    while pos < data.len() {
        let b = data[pos];
        if b == END_OF_PARTITION {
            return None;
        }
        if b == IS_MARKER {
            return Some(pos);
        }
        // Skip a regular row by reading its framing.
        pos = skip_regular_row(data, pos, clustering)?;
    }
    None
}

/// Skip the clustering prefix at `pos` (a 2-bit-per-column header VInt — here
/// always all-PRESENT = 0 — followed by per-column values) and return the offset
/// of row_size.
fn skip_clustering_prefix(data: &[u8], pos: usize, clustering: &[ClType]) -> usize {
    let header = read_uvint_loc(data, pos);
    let mut p = header.end();
    for ct in clustering {
        match ct {
            ClType::Int => p += 4,
            ClType::Text => {
                let len = read_uvint_loc(data, p);
                p = len.end() + len.value as usize;
            }
        }
    }
    p
}

/// Skip one regular (non-marker, non-EOP) row, returning the offset of the next
/// unfiltered. Layout: [flags][clustering prefix][row_size VInt][prev_size + body];
/// `row_size` frames `prev_size + body`.
fn skip_regular_row(data: &[u8], pos: usize, clustering: &[ClType]) -> Option<usize> {
    let flags = data.get(pos).copied()?;
    let mut p = pos + 1;
    // Extended flags (static) — not expected in these fixtures, but handle it.
    if flags & ROW_HAS_EXTENDED_FLAGS != 0 {
        p += 1;
    }
    p = skip_clustering_prefix(data, p, clustering);
    let row_size = read_uvint_loc(data, p);
    Some(row_size.end() + row_size.value as usize)
}

/// Walk one BOUND marker (single deletion-time pair). Returns
/// `(next_offset, kind, Some(int32 ck) | None, mfda_delta_loc, ldt_delta_loc)`.
/// Asserts the IS_MARKER flag and that cluster_count is 0 or 1 (these fixtures
/// pin at most the first int32 clustering component).
fn walk_bound_marker(data: &[u8], offset: usize) -> (usize, u8, Option<i32>, Loc, Loc) {
    assert_eq!(
        data[offset], IS_MARKER,
        "expected IS_MARKER (0x02) at offset {offset}, got 0x{:02X}",
        data[offset]
    );
    let kind = data[offset + 1];
    let cluster_count = read_be_u16(data, offset + 2, "bound marker cluster count");
    let mut pos = offset + 4;
    let ck = if cluster_count == 1 {
        let header = read_uvint_loc(data, pos);
        fail_vint(header, 0, "bound marker clustering header (PRESENT)");
        let v = read_be_i32(data, header.end(), "bound marker int32 clustering value");
        pos = header.end() + 4;
        Some(v)
    } else {
        assert_eq!(
            cluster_count, 0,
            "bound marker cluster_count must be 0 or 1 for these fixtures (got {cluster_count})"
        );
        None
    };
    let body_size = read_uvint_loc(data, pos);
    let body_start = body_size.end();
    let prev_size = read_uvint_loc(data, body_start);
    let mfda = read_uvint_loc(data, prev_size.end());
    let ldt = read_uvint_loc(data, mfda.end());
    // body covers prev_size VInt + the single deletion-time pair.
    let body_end = body_start + body_size.value as usize;
    assert_eq!(
        ldt.end(),
        body_end,
        "bound marker body (size {}) must end right after the single mfda/ldt pair \
         (ldt ends at {}, body_end {})",
        body_size.value,
        ldt.end(),
        body_end
    );
    (body_end, kind, ck, mfda, ldt)
}

/// Walk one BOUNDARY marker (TWO deletion-time pairs). Returns
/// `(kind, Some(int32 ck) | None, primary_mfda_loc, secondary_mfda_loc)`.
fn walk_boundary_marker(data: &[u8], offset: usize) -> (u8, Option<i32>, Loc, Loc) {
    assert_eq!(data[offset], IS_MARKER, "expected IS_MARKER at {offset}");
    let kind = data[offset + 1];
    assert!(
        kind == EXCL_END_INCL_START_BOUNDARY || kind == INCL_END_EXCL_START_BOUNDARY,
        "walk_boundary_marker called on a non-boundary kind {kind} @ {offset}"
    );
    let cluster_count = read_be_u16(data, offset + 2, "boundary marker cluster count");
    let mut pos = offset + 4;
    let ck = if cluster_count == 1 {
        let header = read_uvint_loc(data, pos);
        fail_vint(header, 0, "boundary marker clustering header (PRESENT)");
        let v = read_be_i32(data, header.end(), "boundary marker int32 clustering value");
        pos = header.end() + 4;
        Some(v)
    } else {
        None
    };
    let body_size = read_uvint_loc(data, pos);
    let body_start = body_size.end();
    let prev_size = read_uvint_loc(data, body_start);
    // TWO deletion-time pairs: primary (end of prev range), secondary (start of next).
    let primary_mfda = read_uvint_loc(data, prev_size.end());
    let primary_ldt = read_uvint_loc(data, primary_mfda.end());
    let secondary_mfda = read_uvint_loc(data, primary_ldt.end());
    let secondary_ldt = read_uvint_loc(data, secondary_mfda.end());
    let body_end = body_start + body_size.value as usize;
    assert_eq!(
        secondary_ldt.end(),
        body_end,
        "boundary marker body (size {}) must end right after the SECOND mfda/ldt pair \
         (a boundary carries TWO deletion-time pairs); secondary_ldt ends at {}, body_end {}",
        body_size.value,
        secondary_ldt.end(),
        body_end
    );
    (kind, ck, primary_mfda, secondary_mfda)
}

// ===========================================================================
// JSONL golden field extractors used only by the range/boundary suite
// (string-scan; no JSON crate, mirroring #991). Cross-suite extractors
// (`extract_after`, `golden_partition_position`) live in the shared helpers.
// ===========================================================================

/// The first range-tombstone bound's `marked_deleted` ISO → epoch µs.
fn golden_first_range_marked_deleted_micros(line: &str) -> i64 {
    let rb = line
        .find("\"range_tombstone_bound\"")
        .unwrap_or_else(|| panic!("no range_tombstone_bound: {line}"));
    let rest = &line[rb..];
    let s = extract_after(rest, "\"marked_deleted\":\"").expect("range marked_deleted");
    iso8601_to_micros(&s)
}

/// A range_tombstone_boundary's (start_mfda_µs, end_mfda_µs). The golden shape is
/// `{"type":"range_tombstone_boundary","start":{...,"marked_deleted":"S"...},
///   "end":{...,"marked_deleted":"E"...}}`.
fn golden_boundary_start_end_micros(line: &str) -> (i64, i64) {
    let b = line
        .find("\"range_tombstone_boundary\"")
        .unwrap_or_else(|| panic!("no range_tombstone_boundary: {line}"));
    let rest = &line[b..];
    // The "start" object's marked_deleted comes first, then the "end" object's.
    let start_at = rest.find("\"start\":").expect("boundary start");
    let start_mfda = extract_after(&rest[start_at..], "\"marked_deleted\":\"")
        .expect("boundary start marked_deleted");
    let end_at = rest.find("\"end\":").expect("boundary end");
    let end_mfda = extract_after(&rest[end_at..], "\"marked_deleted\":\"")
        .expect("boundary end marked_deleted");
    (iso8601_to_micros(&start_mfda), iso8601_to_micros(&end_mfda))
}
