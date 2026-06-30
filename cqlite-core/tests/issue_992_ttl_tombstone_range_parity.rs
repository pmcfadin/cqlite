//! Issue #992 (Epic #969): strict byte-for-byte coverage for Cassandra 5.0
//! `Data.db` TTL cells, partition/row/cell tombstones, and range-tombstone bound
//! AND boundary markers.
//!
//! Cassandra oracles: `UnfilteredSerializer.java` (row flags + row deletion
//! times), `Cell.Serializer` (cell IS_DELETED / IS_EXPIRING flags + ts/ttl/ldt),
//! `DeletionTime.Serializer` (partition-header FIXED i32 LDT + i64 mfda, vs the
//! delta form used inside rows/markers), and
//! `ClusteringBoundOrBoundary.Serializer` (range-tombstone marker grammar: kind
//! ordinal, u16 cluster count, marker_body_size/prev_size, and ONE deletion-time
//! pair for a bound vs TWO for a boundary). Local Cassandra source:
//! `RangeTombstoneTest.java`, `RangeTombstoneListTest.java`, `TTLExpiryTest.java`,
//! `CQLSSTableWriterTest.java`.
//!
//! This file is the TTL/tombstone/range sibling of issue #990's
//! `issue_990_data_db_row_framing_parity.rs` and #991's
//! `issue_991_static_clustering_parity.rs`; it MIRRORS their structure — shared
//! offset-context helpers + fixture access live in
//! `issue_992_ttl_tombstone_range_parity_helpers/mod.rs` (a `tests/`
//! SUBDIRECTORY module, so it is NOT its own test binary, keeping this file under
//! the file-size ratchet #1135).
//!
//! Two assertion families per criterion 5:
//!   * BYTE/OFFSET parity — walk the real (decompressed) `Data.db` (or the
//!     deterministic writer output) and assert the EXACT marker/flag bytes, kind
//!     ordinals, field ordering, deletion-time deltas, and byte offsets. These
//!     FAIL on wrong marker bytes, field ordering, or offsets — not just decoded
//!     semantics.
//!   * JSONL/semantic parity — cross-check the SAME fixture's decoded
//!     timestamps/inclusivity/clustering against the sstabledump JSONL golden.
//!
//! Skip-on-absence / fail-on-0-when-present (local-only-fixtures doctrine): the
//! `test_deltas/*` fixtures are LOCAL-ONLY (not yet in the pinned CI dataset), so
//! the fixture lanes SKIP when the `*-Data.db` binary is absent — but when it is
//! present they FAIL if the body is empty or they find 0 markers/rows. The
//! deterministic writer lanes (which exercise the SAME on-disk grammar through
//! the public `DataWriter` surface) run everywhere, so coverage of the 6 marker
//! forms is never lost to a skip.

#![cfg(feature = "write-support")]

use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, PartitionTombstone,
    RangeTombstone, TableId,
};
use cqlite_core::types::Value;

#[path = "issue_992_ttl_tombstone_range_parity_helpers/mod.rs"]
mod helpers;
use helpers::*;

// ===========================================================================
// Section 1 — PARTITION deletion time: FIXED header form, NOT the delta form.
// (manifest: cass.data_db_decode.tombstone.partition_deletion_time)
// (acceptance criterion 1)
//
// Cassandra oracle: ColumnIndex / partition header writes DeletionTime via
// `DeletionTime.Serializer.serialize` = [int localDeletionTime][long
// markedForDeleteAt], FIXED big-endian widths, NOT the unsigned-VInt deltas used
// inside rows/markers. LIVE is the (Integer.MAX_VALUE, Long.MIN_VALUE) sentinel.
// ===========================================================================

/// Deterministic BYTE parity: a partition tombstone is encoded in the partition
/// HEADER as a fixed `[i32 LDT BE][i64 mfda BE]` immediately after the key — the
/// FIRST row's `prev_size` (the partition-header byte size) is therefore the SAME
/// 18 bytes as a LIVE partition (key_len 2 + key 4 + LDT 4 + mfda 8), proving the
/// deletion time consumes the fixed header slot, not extra delta bytes.
#[test]
fn partition_deletion_uses_fixed_header_form_not_delta() {
    let schema = int_clustering_schema();
    // Distinctive LDT/mfda bytes, with mfda OLDER than the row timestamp
    // (2_000_000 µs) so the surviving row is not shadowed and the first row's
    // prev_size can be asserted as the fixed header size.
    let pt = PartitionTombstone {
        deletion_time: 0x0011_2233, // mfda (µs) = 1_122_867, older than the row's 2_000_000
        local_deletion_time: 0x0A0B_0C0D, // LDT (s)
    };
    let m = Mutation::new(
        TableId::new("issue992", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![write_op("val", "x")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m], Some(&pt), &[]);

    // Header: [u16 key_len=4][i32 key=1][i32 LDT][i64 mfda].
    let key_len = read_be_u16(&bytes, 0, "partition key length");
    assert_eq!(key_len, 4, "int32 PK is 4 bytes");
    let key = read_be_i32(&bytes, 2, "partition key (int32)");
    assert_eq!(key, 1);

    // CRITICAL (criterion 1): the LDT is a FIXED 4-byte big-endian int at offset
    // 6, and the mfda a FIXED 8-byte big-endian long at offset 10 — NOT VInt
    // deltas. Assert the exact bytes/offsets.
    let ldt = read_be_i32(&bytes, 6, "partition tombstone LDT (fixed i32 BE)");
    assert_eq!(
        ldt, 0x0A0B_0C0D,
        "partition LDT must be a FIXED i32 BE at offset 6 (not a delta)"
    );
    assert_eq!(
        &bytes[6..10],
        &[0x0A, 0x0B, 0x0C, 0x0D],
        "partition LDT bytes at [6,10) must be the raw big-endian i32"
    );
    let mfda = read_be_i64(&bytes, 10, "partition tombstone mfda (fixed i64 BE)");
    assert_eq!(
        mfda, 0x0011_2233,
        "partition mfda must be a FIXED i64 BE at offset 10 (not a delta)"
    );
    assert_eq!(
        &bytes[10..18],
        &0x0011_2233i64.to_be_bytes(),
        "partition mfda bytes at [10,18) must be the raw big-endian i64"
    );

    // The first row begins right after the 18-byte fixed header — its prev_size
    // equals 18, proving the deletion time did NOT add VInt-delta bytes anywhere.
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    let header = read_uvint_loc(&bytes, flags.end()); // clustering header
    let ck = read_be_i32(&bytes, header.end(), "first row clustering");
    assert_eq!(ck, 1);
    let row_size = read_uvint_loc(&bytes, header.end() + 4);
    let prev_size = read_uvint_loc(&bytes, row_size.end());
    fail_vint(
        prev_size,
        INT_PK_HEADER_SIZE as u64,
        "first-row prev_size equals the FIXED partition-header size (18) even WITH a \
         partition tombstone — the deletion time lives in the header's fixed i32/i64 slots",
    );
}

/// A LIVE partition uses the (Integer.MAX_VALUE, Long.MIN_VALUE) sentinel in the
/// SAME fixed header slots — byte-distinct from a real deletion. This anchors the
/// "fixed slot" claim: live vs deleted differ only in those fixed bytes.
#[test]
fn live_partition_header_uses_live_sentinel_in_same_fixed_slots() {
    let schema = int_clustering_schema();
    let m = Mutation::new(
        TableId::new("issue992", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![write_op("val", "x")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m], None, &[]);
    assert_eq!(
        &bytes[6..10],
        &i32::MAX.to_be_bytes(),
        "LIVE partition LDT slot must be Integer.MAX_VALUE (0x7FFFFFFF) at [6,10)"
    );
    assert_eq!(
        &bytes[10..18],
        &i64::MIN.to_be_bytes(),
        "LIVE partition mfda slot must be Long.MIN_VALUE (0x8000…00) at [10,18)"
    );
}

/// FIXTURE BYTE parity: the real partition_tombstones Data.db's PK=2 partition
/// (golden line 3) carries a partition deletion in the FIXED header form. Walk
/// the on-disk header bytes and reconstruct the absolute LDT/mfda from the FIXED
/// i32/i64 (NOT a delta), matching the JSONL golden exactly.
#[test]
fn fixture_partition_tombstone_fixed_header_byte_parity() {
    let Some((raw, jsonl)) = load_local_only(PARTITION_TOMBSTONES_DIR) else {
        eprintln!(
            "SKIP fixture_partition_tombstone_fixed_header_byte_parity: local-only fixture absent \
             (covered deterministically by partition_deletion_uses_fixed_header_form_not_delta)"
        );
        return;
    };

    // Golden: a deletion_info partition with key "2". Find its on-disk position.
    let p2 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"2\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=2 partition tombstone"));
    assert!(
        p2.contains("\"deletion_info\""),
        "PK=2 must be a partition tombstone (deletion_info present): {p2}"
    );
    let pos = golden_partition_position(p2);

    // Header at `pos`: [u16 key_len=4][i32 key=2][i32 LDT BE][i64 mfda BE].
    let key_len = read_be_u16(&raw, pos, "partition key length") as usize;
    assert_eq!(key_len, 4, "int32 PK is 4 bytes");
    let key = read_be_i32(&raw, pos + 2, "partition key (int32)");
    assert_eq!(key, 2, "deletion partition on disk must be PK=2");

    // FIXED i32 LDT then FIXED i64 mfda — NOT VInt deltas (criterion 1).
    let ldt = read_be_i32(&raw, pos + 6, "partition tombstone LDT (fixed i32 BE)");
    let mfda = read_be_i64(&raw, pos + 10, "partition tombstone mfda (fixed i64 BE)");
    assert_ne!(
        ldt,
        i32::MAX,
        "PK=2 partition LDT must NOT be the LIVE sentinel (it is a real deletion)"
    );
    assert_ne!(
        mfda,
        i64::MIN,
        "PK=2 partition mfda must NOT be the LIVE sentinel"
    );

    // Cross-check the FIXED-form values against the golden (criterion 5,
    // semantic family): mfda µs and LDT seconds.
    let golden_mfda = golden_partition_marked_deleted_micros(p2);
    assert_eq!(
        mfda,
        golden_mfda,
        "fixed-header mfda (i64 BE at offset {}) must equal the golden marked_deleted µs",
        pos + 10
    );
    let golden_ldt = golden_partition_local_delete_secs(p2);
    assert_eq!(
        ldt as i64,
        golden_ldt,
        "fixed-header LDT (i32 BE at offset {}) must equal the golden local_delete_time secs",
        pos + 6
    );
}

// ===========================================================================
// Section 2 — ROW tombstone: own ts/ldt fields, no value bytes consumed.
// (manifest: cass.data_db_decode.tombstone.row_deletion_time)
// (acceptance criterion 2)
//
// Cassandra oracle: UnfilteredSerializer — a HAS_DELETION row writes
// [markedForDeleteAt delta][localDeletionTime delta] (both UNSIGNED VInt) right
// after the clustering prefix + row_size/prev_size, and a PURE row tombstone has
// NO surviving cells (no value bytes).
// ===========================================================================

/// Deterministic BYTE parity: a pure `DeleteRow` row carries HAS_DELETION (and
/// NOT HAS_TIMESTAMP), then exactly two UNSIGNED VInt deltas (mfda then ldt),
/// then the column bitmap, and NO cell value bytes — the body closes on
/// END_OF_PARTITION right after the bitmap.
#[test]
fn row_tombstone_carries_own_deltas_and_no_value_bytes() {
    let schema = int_clustering_schema();
    let m = Mutation::new(
        TableId::new("issue992", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(7))),
        vec![CellOperation::DeleteRow],
        2_000_000,
        None,
    )
    .with_local_deletion_time(1_500);
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m], None, &[]);

    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    fail_flag(flags, ROW_HAS_DELETION, "pure row tombstone leading flag");
    assert_eq!(
        flags.value as u8 & ROW_HAS_TIMESTAMP,
        0,
        "a pure row tombstone must NOT set HAS_TIMESTAMP"
    );

    // clustering header (PRESENT) + ck int, then row_size/prev_size.
    let cl_header = read_uvint_loc(&bytes, flags.end());
    fail_vint(cl_header, 0, "row tombstone clustering header (PRESENT)");
    let ck = read_be_i32(&bytes, cl_header.end(), "row tombstone clustering value");
    assert_eq!(ck, 7);
    let row_size = read_uvint_loc(&bytes, cl_header.end() + 4);
    let prev_size = read_uvint_loc(&bytes, row_size.end());
    fail_vint(
        prev_size,
        INT_PK_HEADER_SIZE as u64,
        "row tombstone first-row prev_size equals the partition-header size",
    );

    // Body: [mfda delta][ldt delta][column bitmap]. These are the row's OWN
    // deletion fields (criterion 2), distinct from any cell.
    let mfda_delta = read_uvint_loc(&bytes, prev_size.end());
    fail_vint(
        mfda_delta,
        2_000_000 - 1_000_000,
        "row tombstone mfda delta (ts - min_timestamp), UNSIGNED VInt",
    );
    let ldt_delta = read_uvint_loc(&bytes, mfda_delta.end());
    fail_vint(
        ldt_delta,
        1_500,
        "row tombstone ldt delta (local_deletion_time - min_local_deletion_time)",
    );
    // NOT-all-columns row → a column-subset bitmap VInt follows, then the body
    // ends. The row body must close on END_OF_PARTITION with NO cell value bytes.
    let bitmap = read_uvint_loc(&bytes, ldt_delta.end());
    let body_end = prev_size.start + row_size.value as usize;
    assert_eq!(
        bitmap.end(),
        body_end,
        "row tombstone body must end right after the column bitmap — NO cell value bytes \
         (bitmap ends at {}, body_end {})",
        bitmap.end(),
        body_end
    );
    assert_eq!(
        bytes[body_end], END_OF_PARTITION,
        "row tombstone body must close on END_OF_PARTITION (0x01) at offset {body_end}"
    );
}

/// FIXTURE BYTE parity: the real row_tombstones Data.db — PK=1's clustering [3]
/// row is a row tombstone (golden line 1). Walk it: HAS_DELETION, own mfda/ldt
/// deltas reconstructing the golden marked_deleted µs, NO cell value bytes.
#[test]
fn fixture_row_tombstone_byte_parity() {
    let Some((raw, jsonl)) = load_local_only(ROW_TOMBSTONES_DIR) else {
        eprintln!(
            "SKIP fixture_row_tombstone_byte_parity: local-only fixture absent \
             (covered deterministically by row_tombstone_carries_own_deltas_and_no_value_bytes)"
        );
        return;
    };
    // Golden PK=1 line; the clustering [3] row has a deletion_info, no cells.
    let p1 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"1\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=1"));
    assert!(
        p1.contains("\"clustering\":[3],\"deletion_info\""),
        "PK=1 clustering [3] must be a row tombstone: {p1}"
    );
    let golden_mfda = golden_first_row_tombstone_micros(p1);
    let row_pos = golden_first_row_tombstone_position(p1);

    // Walk the row tombstone header at row_pos.
    let flags = read_u8_loc(&raw, row_pos);
    assert_eq!(
        flags.value as u8 & ROW_HAS_DELETION,
        ROW_HAS_DELETION,
        "row at offset {row_pos} must set HAS_DELETION (flag 0x{:02X})",
        flags.value
    );
    assert_eq!(
        flags.value as u8 & ROW_HAS_TIMESTAMP,
        0,
        "a pure row tombstone must NOT set HAS_TIMESTAMP (flag 0x{:02X})",
        flags.value
    );
    let cl_header = read_uvint_loc(&raw, flags.end());
    fail_vint(
        cl_header,
        0,
        "fixture row tombstone clustering header (PRESENT)",
    );
    let ck = read_be_i32(&raw, cl_header.end(), "fixture row tombstone clustering");
    assert_eq!(ck, 3, "row tombstone clustering value must be 3");
    let row_size = read_uvint_loc(&raw, cl_header.end() + 4);
    let prev_size = read_uvint_loc(&raw, row_size.end());

    // Own mfda/ldt deltas → reconstruct the absolute mfda and match the golden.
    let mfda_delta = read_uvint_loc(&raw, prev_size.end());
    let abs_mfda = minima::ROW_MIN_TS + mfda_delta.value as i64;
    assert_eq!(
        abs_mfda,
        golden_mfda,
        "row tombstone mfda delta {} (at offset {}) + min {} = {} must equal the golden \
         marked_deleted µs {}",
        mfda_delta.value,
        mfda_delta.start,
        minima::ROW_MIN_TS,
        abs_mfda,
        golden_mfda
    );
    let ldt_delta = read_uvint_loc(&raw, mfda_delta.end());
    let abs_ldt = minima::SHARED_MIN_LDT + ldt_delta.value as i64;
    assert_eq!(
        abs_ldt,
        minima::SHARED_MIN_LDT,
        "row tombstone ldt (delta {} + min {}) must equal the golden local_delete_time secs",
        ldt_delta.value,
        minima::SHARED_MIN_LDT
    );

    // No cell value bytes: bitmap (NOT-all-columns) then the body closes exactly
    // at the framed end — proving the row tombstone consumed NO value bytes
    // (criterion 2). PK=1 ck=3 is a MIDDLE row (ck=4,5 follow), so the next byte
    // is the next row's leading flag, not END_OF_PARTITION; we assert the framing
    // closes on the bitmap rather than the partition end.
    let bitmap = read_uvint_loc(&raw, ldt_delta.end());
    let body_end = prev_size.start + row_size.value as usize;
    assert_eq!(
        bitmap.end(),
        body_end,
        "fixture row tombstone body ends right after the column bitmap — NO cell value bytes \
         (bitmap ends at {}, framed body_end {})",
        bitmap.end(),
        body_end
    );
    // The next unfiltered (ck=4 live row) begins at body_end and is a regular
    // row (no IS_MARKER / END_OF_PARTITION sentinel), confirming the framing.
    assert!(
        raw[body_end] & (IS_MARKER | END_OF_PARTITION) == 0,
        "the unfiltered after the row tombstone at offset {body_end} must be a regular row \
         (got leading byte 0x{:02X})",
        raw[body_end]
    );
}

// ===========================================================================
// Section 3 — CELL tombstone: own ts/ldt fields, no value bytes consumed.
// (manifest: cass.data_db_decode.tombstone.cell_deletion_time)
// (acceptance criterion 2)
//
// Cassandra oracle: Cell.Serializer — a deleted cell sets IS_DELETED |
// HAS_EMPTY_VALUE, writes its OWN [timestamp delta][localDeletionTime delta]
// (both UNSIGNED VInt, NOT USE_ROW_TIMESTAMP), and NO value length / value bytes.
// ===========================================================================

/// Deterministic BYTE parity for a cell tombstone in a LIVE row: the row carries
/// a live `col_a` cell AND a deleted `col_b` cell. The deleted cell's flag byte
/// is IS_DELETED | HAS_EMPTY_VALUE (no USE_ROW_TIMESTAMP), followed by its own
/// ts/ldt deltas and then the NEXT cell — never any value bytes for the tombstone.
#[test]
fn cell_tombstone_carries_own_deltas_and_no_value_bytes() {
    // Schema: int PK, int clustering, two regular text cols col_a, col_b.
    let mut schema = int_clustering_schema();
    schema.columns = vec![
        cqlite_core::schema::Column {
            name: "col_a".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        },
        cqlite_core::schema::Column {
            name: "col_b".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        },
    ];
    let m = Mutation::new(
        TableId::new("issue992", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(2))),
        vec![
            write_op("col_a", "a_val"),
            CellOperation::Delete {
                column: "col_b".to_string(),
                local_deletion_time: Some(2_000),
            },
        ],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m], None, &[]);

    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    // Row carries a liveness timestamp for the live col_a cell + a deletion-less
    // row marker; HAS_TIMESTAMP set, all-columns set (both cols touched).
    assert_eq!(
        flags.value as u8 & ROW_HAS_TIMESTAMP,
        ROW_HAS_TIMESTAMP,
        "row with a live cell must set HAS_TIMESTAMP (flag 0x{:02X})",
        flags.value
    );

    // Walk: clustering header + ck, row_size/prev_size, ts delta, then cells.
    let cl_header = read_uvint_loc(&bytes, flags.end());
    let row_size = read_uvint_loc(&bytes, cl_header.end() + 4);
    let prev_size = read_uvint_loc(&bytes, row_size.end());
    let mut pos = prev_size.end();
    if flags.value as u8 & ROW_HAS_TIMESTAMP != 0 {
        pos = read_uvint_loc(&bytes, pos).end(); // row liveness ts delta
    }
    if flags.value as u8 & ROW_HAS_ALL_COLUMNS == 0 {
        pos = read_uvint_loc(&bytes, pos).end(); // column subset bitmap
    }

    // Cell 1 (col_a, live): USE_ROW_TIMESTAMP, then a length-prefixed value.
    let c1_flags = read_u8_loc(&bytes, pos);
    assert_eq!(
        c1_flags.value as u8 & CELL_USE_ROW_TIMESTAMP,
        CELL_USE_ROW_TIMESTAMP,
        "live col_a cell must reuse the row timestamp (flag 0x{:02X} @ {})",
        c1_flags.value,
        c1_flags.start
    );
    let c1_len = read_uvint_loc(&bytes, c1_flags.end());
    fail_vint(c1_len, 5, "live col_a value length ('a_val')");
    let after_c1 = c1_len.end() + c1_len.value as usize;

    // Cell 2 (col_b, tombstone): IS_DELETED | HAS_EMPTY_VALUE, own ts/ldt, NO value.
    let c2_flags = read_u8_loc(&bytes, after_c1);
    fail_flag(
        c2_flags,
        CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE,
        "cell tombstone flag (IS_DELETED | HAS_EMPTY_VALUE, no USE_ROW_TIMESTAMP)",
    );
    assert_eq!(
        c2_flags.value as u8 & CELL_USE_ROW_TIMESTAMP,
        0,
        "cell tombstone must NOT reuse the row timestamp — it carries its own"
    );
    let c2_ts = read_uvint_loc(&bytes, c2_flags.end());
    fail_vint(
        c2_ts,
        2_000_000 - 1_000_000,
        "cell tombstone ts delta (its OWN timestamp, UNSIGNED VInt)",
    );
    let c2_ldt = read_uvint_loc(&bytes, c2_ts.end());
    fail_vint(c2_ldt, 2_000, "cell tombstone ldt delta (its OWN ldt)");

    // The body closes immediately after the cell tombstone's ldt — NO value
    // length / value bytes were consumed for the tombstone cell (criterion 2).
    let body_end = prev_size.start + row_size.value as usize;
    assert_eq!(
        c2_ldt.end(),
        body_end,
        "cell tombstone has NO value bytes: body must end right after its ldt delta \
         (ldt ends at {}, body_end {})",
        c2_ldt.end(),
        body_end
    );
}

/// FIXTURE BYTE parity: real cell_tombstones Data.db — PK=1's clustering [2] row
/// has a live `col_a` and a deleted `col_b`. Walk the deleted cell's flag byte
/// and own ts/ldt deltas; assert IS_DELETED | HAS_EMPTY_VALUE and that the golden
/// col_b deletion µs is reconstructed from the cell's OWN delta.
#[test]
fn fixture_cell_tombstone_byte_parity() {
    let Some((raw, jsonl)) = load_local_only(CELL_TOMBSTONES_DIR) else {
        eprintln!(
            "SKIP fixture_cell_tombstone_byte_parity: local-only fixture absent \
             (covered deterministically by cell_tombstone_carries_own_deltas_and_no_value_bytes)"
        );
        return;
    };
    let p1 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"1\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=1"));
    // Golden: clustering [2] has col_a live + col_b deletion_info with a tstamp.
    assert!(
        p1.contains("\"col_b\",\"deletion_info\""),
        "PK=1 must carry a col_b cell tombstone: {p1}"
    );
    let golden_cell_ts = golden_cell_tombstone_micros(p1);
    let row_pos = golden_cell_tombstone_row_position(p1);

    let flags = read_u8_loc(&raw, row_pos);
    let cl_header = read_uvint_loc(&raw, flags.end());
    let ck = read_be_i32(&raw, cl_header.end(), "cell tombstone row clustering");
    assert_eq!(ck, 2, "cell tombstone row clustering must be 2");
    let row_size = read_uvint_loc(&raw, cl_header.end() + 4);
    let prev_size = read_uvint_loc(&raw, row_size.end());
    let mut pos = prev_size.end();
    if flags.value as u8 & ROW_HAS_TIMESTAMP != 0 {
        pos = read_uvint_loc(&raw, pos).end();
    }
    if flags.value as u8 & ROW_HAS_ALL_COLUMNS == 0 {
        pos = read_uvint_loc(&raw, pos).end();
    }

    // Cell 1 = col_a (live, USE_ROW_TIMESTAMP) then a length-prefixed value.
    let c1_flags = read_u8_loc(&raw, pos);
    assert_eq!(
        c1_flags.value as u8 & CELL_USE_ROW_TIMESTAMP,
        CELL_USE_ROW_TIMESTAMP,
        "fixture live col_a cell must reuse the row timestamp (flag 0x{:02X} @ {})",
        c1_flags.value,
        c1_flags.start
    );
    let c1_len = read_uvint_loc(&raw, c1_flags.end());
    let after_c1 = c1_len.end() + c1_len.value as usize;

    // Cell 2 = col_b tombstone: IS_DELETED | HAS_EMPTY_VALUE, own ts/ldt, no value.
    let c2_flags = read_u8_loc(&raw, after_c1);
    fail_flag(
        c2_flags,
        CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE,
        "fixture cell tombstone flag (IS_DELETED | HAS_EMPTY_VALUE)",
    );
    let c2_ts = read_uvint_loc(&raw, c2_flags.end());
    let abs_ts = minima::CELL_MIN_TS + c2_ts.value as i64;
    assert_eq!(
        abs_ts,
        golden_cell_ts,
        "cell tombstone ts delta {} (at offset {}) + min {} = {} must equal the golden \
         col_b deletion µs {}",
        c2_ts.value,
        c2_ts.start,
        minima::CELL_MIN_TS,
        abs_ts,
        golden_cell_ts
    );
    let c2_ldt = read_uvint_loc(&raw, c2_ts.end());
    let body_end = prev_size.start + row_size.value as usize;
    assert_eq!(
        c2_ldt.end(),
        body_end,
        "fixture cell tombstone has NO value bytes: body ends right after its ldt delta"
    );
}

// ===========================================================================
// Section 4 — TTL cells: preserve TTL + derived expiration/local-deletion-time.
// (manifest: cass.data_db_decode.ttl.local_deletion_time_delta)
// (acceptance criterion 3)
//
// Cassandra oracle: UnfilteredSerializer / Cell.Serializer write a TTL row as
// [ts delta][ttl delta][localDeletionTime delta] (all UNSIGNED VInt deltas from
// the EncodingStats minima); localDeletionTime is the derived expiration second
// (now + ttl).
// ===========================================================================

/// Deterministic BYTE parity with NON-ZERO baselines: a row TTL writes the row
/// flags HAS_TIMESTAMP | HAS_TTL, then [ts delta][ttl delta][ldt delta]. The TTL
/// delta is `ttl - min_ttl` and the ldt delta is `expiration - min_ldt`, both
/// UNSIGNED. Using non-zero minima makes the deltas a controlled, exact value so
/// a regression in the baseline subtraction is caught at the byte level.
#[test]
fn ttl_row_preserves_ttl_and_derived_ldt_deltas() {
    let mut stats = det_stats();
    stats.min_ttl = 100;
    stats.min_local_deletion_time = 1_000;

    let schema = int_clustering_schema();
    let m = Mutation::new(
        TableId::new("issue992", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![write_op("val", "x")],
        2_000_000,
        Some(900), // TTL seconds
    );
    let bytes = write_one_partition(stats, &schema, 1, &[m], None, &[]);

    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    assert_eq!(
        flags.value as u8 & (ROW_HAS_TIMESTAMP | ROW_HAS_TTL),
        ROW_HAS_TIMESTAMP | ROW_HAS_TTL,
        "TTL row must carry HAS_TIMESTAMP | HAS_TTL (flag 0x{:02X})",
        flags.value
    );
    let cl_header = read_uvint_loc(&bytes, flags.end());
    let row_size = read_uvint_loc(&bytes, cl_header.end() + 4);
    let prev_size = read_uvint_loc(&bytes, row_size.end());

    // Body: [ts delta][ttl delta][ldt delta].
    let ts = read_uvint_loc(&bytes, prev_size.end());
    fail_vint(ts, 2_000_000 - 1_000_000, "TTL row ts delta");
    let ttl = read_uvint_loc(&bytes, ts.end());
    fail_vint(
        ttl,
        900 - 100,
        "TTL delta = ttl_seconds - min_ttl (UNSIGNED, preserves the original TTL)",
    );
    let ldt = read_uvint_loc(&bytes, ttl.end());
    assert!(
        ldt.value >= 1, // expiration is wall-clock now+ttl, far above the 1_000 baseline delta
        "derived local-deletion-time (expiration) delta must be a positive UNSIGNED value \
         (got {} @ offset {})",
        ldt.value,
        ldt.start
    );
    // The expiring cell that follows reuses BOTH the row timestamp and row TTL.
    let mut pos = ldt.end();
    if flags.value as u8 & ROW_HAS_ALL_COLUMNS == 0 {
        pos = read_uvint_loc(&bytes, pos).end();
    }
    let cell = read_u8_loc(&bytes, pos);
    fail_flag(
        cell,
        CELL_IS_EXPIRING | CELL_USE_ROW_TIMESTAMP | CELL_USE_ROW_TTL,
        "row-TTL cell reuses row timestamp + row TTL (IS_EXPIRING | USE_ROW_TIMESTAMP | USE_ROW_TTL)",
    );
}

/// FIXTURE BYTE parity: real ttl_cells Data.db — PK=1's first row carries
/// ttl=3600. With the fixture's EncodingStats minTTL=3600 and minLocalDeletionTime
/// = the expiration second, the ttl delta and ldt delta are both 0 on disk. Walk
/// the row and assert HAS_TTL is set and the deltas reconstruct ttl=3600 /
/// expires_at exactly (criterion 3). The golden's `notll` partition (PK=10) is a
/// no-TTL control whose first row must NOT set HAS_TTL.
#[test]
fn fixture_ttl_row_byte_parity() {
    let Some((raw, jsonl)) = load_local_only(TTL_CELLS_DIR) else {
        eprintln!(
            "SKIP fixture_ttl_row_byte_parity: local-only fixture absent \
             (covered deterministically by ttl_row_preserves_ttl_and_derived_ldt_deltas)"
        );
        return;
    };
    // PK=1 (golden line 2) carries ttl=3600 rows; PK=10 (line 1) is the no-TTL
    // control.
    let p1 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"1\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=1 TTL partition"));
    assert!(
        p1.contains("\"ttl\":3600") && p1.contains("\"expires_at\":\"2026-06-24T23:50:57Z\""),
        "PK=1 first row must carry ttl=3600 / expires_at 23:50:57Z: {p1}"
    );
    let row_pos = golden_first_data_row_position(p1);

    let flags = read_u8_loc(&raw, row_pos);
    assert_eq!(
        flags.value as u8 & ROW_HAS_TTL,
        ROW_HAS_TTL,
        "TTL row at offset {row_pos} must set HAS_TTL (flag 0x{:02X})",
        flags.value
    );
    assert_eq!(
        flags.value as u8 & ROW_HAS_TIMESTAMP,
        ROW_HAS_TIMESTAMP,
        "TTL row must also set HAS_TIMESTAMP"
    );
    let cl_header = read_uvint_loc(&raw, flags.end());
    let row_size = read_uvint_loc(&raw, cl_header.end() + 4);
    let prev_size = read_uvint_loc(&raw, row_size.end());
    // Body: [ts delta][ttl delta][ldt delta]. Reconstruct absolute ttl + expiration.
    let ts = read_uvint_loc(&raw, prev_size.end());
    let ttl = read_uvint_loc(&raw, ts.end());
    let abs_ttl = minima::TTL_MIN_TTL + ttl.value;
    assert_eq!(
        abs_ttl,
        3_600,
        "TTL row ttl delta {} (at offset {}) + minTTL {} = {} must equal the golden ttl 3600",
        ttl.value,
        ttl.start,
        minima::TTL_MIN_TTL,
        abs_ttl
    );
    let ldt = read_uvint_loc(&raw, ttl.end());
    let abs_ldt = minima::TTL_MIN_LDT + ldt.value as i64;
    let golden_expires = iso8601_to_micros("2026-06-24T23:50:57Z") / 1_000_000;
    assert_eq!(
        abs_ldt,
        golden_expires,
        "TTL row ldt delta {} (at offset {}) + minLDT {} = {} must equal the golden expires_at \
         second {}",
        ldt.value,
        ldt.start,
        minima::TTL_MIN_LDT,
        abs_ldt,
        golden_expires
    );

    // Control: the PK=10 `notll` partition's first row must NOT set HAS_TTL.
    let p10 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"10\"]"))
        .unwrap_or_else(|| panic!("golden must contain the PK=10 no-TTL control partition"));
    assert!(
        !p10.contains("\"ttl\""),
        "PK=10 control partition must have NO ttl in the golden: {p10}"
    );
    let p10_pos = golden_partition_position(p10);
    let p10_row_pos = golden_first_data_row_position(p10);
    assert!(
        p10_pos < p10_row_pos,
        "PK=10 partition position {p10_pos} must precede its first row {p10_row_pos}"
    );
    let p10_flags = read_u8_loc(&raw, p10_row_pos);
    assert_eq!(
        p10_flags.value as u8 & ROW_HAS_TTL,
        0,
        "PK=10 control row must NOT set HAS_TTL (flag 0x{:02X} @ {})",
        p10_flags.value,
        p10_flags.start
    );
}

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
// JSONL golden field extractors (string-scan; no JSON crate, mirroring #991).
// ===========================================================================

fn extract_after(line: &str, marker: &str) -> Option<String> {
    let start = line.find(marker)? + marker.len();
    let rest = &line[start..];
    let end = rest.find(['"', ',', '}'])?;
    Some(rest[..end].to_string())
}

/// `"position":<N>` of a `partition` object (the FIRST one in the line).
fn golden_partition_position(line: &str) -> usize {
    let p = line
        .find("\"partition\":")
        .unwrap_or_else(|| panic!("no partition object: {line}"));
    let rest = &line[p..];
    extract_after(rest, "\"position\":")
        .unwrap_or_else(|| panic!("no partition position: {line}"))
        .parse()
        .expect("partition position int")
}

/// The first `"type":"row","position":<N>` row position after the partition obj.
fn golden_first_data_row_position(line: &str) -> usize {
    let marker = "\"type\":\"row\",\"position\":";
    extract_after(line, marker)
        .unwrap_or_else(|| panic!("no data row position: {line}"))
        .parse()
        .expect("row position int")
}

/// Partition tombstone `marked_deleted` ISO → epoch µs.
fn golden_partition_marked_deleted_micros(line: &str) -> i64 {
    // The partition deletion_info is the first marked_deleted in the line.
    let s = extract_after(line, "\"marked_deleted\":\"")
        .unwrap_or_else(|| panic!("no marked_deleted: {line}"));
    iso8601_to_micros(&s)
}

/// Partition tombstone `local_delete_time` ISO → epoch seconds.
fn golden_partition_local_delete_secs(line: &str) -> i64 {
    let s = extract_after(line, "\"local_delete_time\":\"")
        .unwrap_or_else(|| panic!("no local_delete_time: {line}"));
    iso8601_to_micros(&s) / 1_000_000
}

/// The first ROW tombstone's `position` (a row carrying `deletion_info`).
fn golden_first_row_tombstone_position(line: &str) -> usize {
    // Locate the deletion_info-carrying row and read its position field which
    // precedes the clustering/deletion in the same object.
    let di = line
        .find("\"deletion_info\"")
        .unwrap_or_else(|| panic!("no row deletion_info: {line}"));
    // position appears before deletion_info within the same row object.
    let prefix = &line[..di];
    let pos_marker = "\"position\":";
    let last = prefix
        .rfind(pos_marker)
        .unwrap_or_else(|| panic!("no position before row deletion_info: {line}"));
    let rest = &prefix[last + pos_marker.len()..];
    let end = rest.find([',', '}']).expect("position terminator");
    rest[..end].parse().expect("row position int")
}

/// The first ROW tombstone's `marked_deleted` ISO → epoch µs.
fn golden_first_row_tombstone_micros(line: &str) -> i64 {
    let di = line.find("\"deletion_info\"").expect("row deletion_info");
    let rest = &line[di..];
    let s = extract_after(rest, "\"marked_deleted\":\"").expect("row marked_deleted");
    iso8601_to_micros(&s)
}

/// The CELL tombstone `tstamp` (col_b deletion) ISO → epoch µs. The cell
/// tombstone golden shape is `{"name":"col_b","deletion_info":{...},"tstamp":"..."}`.
fn golden_cell_tombstone_micros(line: &str) -> i64 {
    let cb = line
        .find("\"col_b\",\"deletion_info\"")
        .unwrap_or_else(|| panic!("no col_b cell tombstone: {line}"));
    let rest = &line[cb..];
    let s = extract_after(rest, "\"tstamp\":\"").expect("cell tombstone tstamp");
    iso8601_to_micros(&s)
}

/// The row position of the row carrying the col_b cell tombstone.
fn golden_cell_tombstone_row_position(line: &str) -> usize {
    let cb = line
        .find("\"col_b\",\"deletion_info\"")
        .unwrap_or_else(|| panic!("no col_b cell tombstone: {line}"));
    let prefix = &line[..cb];
    let pos_marker = "\"position\":";
    let last = prefix
        .rfind(pos_marker)
        .expect("position before col_b tombstone");
    let rest = &prefix[last + pos_marker.len()..];
    let end = rest.find([',', '}']).expect("position terminator");
    rest[..end].parse().expect("row position int")
}

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
