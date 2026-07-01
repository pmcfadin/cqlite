//! Issue #992 (Epic #969): strict byte-for-byte coverage for Cassandra 5.0
//! `Data.db` TTL cells and partition/row/cell tombstones.
//!
//! This is the TTL/tombstone half of the issue #992 suite; the range-tombstone
//! BOUND and BOUNDARY marker grammar lives in the sibling
//! `issue_992_range_boundary_grammar.rs`. The two files were split from the
//! original `issue_992_ttl_tombstone_range_parity.rs` (issue #1267) to keep each
//! `#[test]` file under the file-size ratchet (#1135) WITHOUT
//! `CQLITE_ALLOW_FILE_GROWTH`. The split is purely a move/reorganize — every test
//! keeps its name and assertions.
//!
//! Cassandra oracles: `UnfilteredSerializer.java` (row flags + row deletion
//! times), `Cell.Serializer` (cell IS_DELETED / IS_EXPIRING flags + ts/ttl/ldt),
//! and `DeletionTime.Serializer` (partition-header FIXED i32 LDT + i64 mfda, vs
//! the delta form used inside rows). Local Cassandra source: `TTLExpiryTest.java`,
//! `CQLSSTableWriterTest.java`.
//!
//! Two assertion families per criterion 5:
//!   * BYTE/OFFSET parity — walk the real (decompressed) `Data.db` (or the
//!     deterministic writer output) and assert the EXACT flag bytes, field
//!     ordering, deletion-time deltas, and byte offsets. These FAIL on wrong
//!     flag bytes, field ordering, or offsets — not just decoded semantics.
//!   * JSONL/semantic parity — cross-check the SAME fixture's decoded
//!     timestamps against the sstabledump JSONL golden.
//!
//! Skip-on-absence / fail-on-0-when-present (local-only-fixtures doctrine): the
//! `test_deltas/*` fixtures are LOCAL-ONLY (not yet in the pinned CI dataset), so
//! the fixture lanes SKIP when the `*-Data.db` binary is absent — but when it is
//! present they FAIL if the body is empty or they find 0 rows. The deterministic
//! writer lanes (which exercise the SAME on-disk grammar through the public
//! `DataWriter` surface) run everywhere, so coverage is never lost to a skip.
//!
//! Shared offset-context helpers + fixture access live in
//! `issue_992_ttl_tombstone_range_parity_helpers/mod.rs` (a `tests/` SUBDIRECTORY
//! module, so it is NOT its own test binary).

#![cfg(feature = "write-support")]

use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, PartitionTombstone, TableId,
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
// JSONL golden field extractors used only by the TTL/tombstone suite
// (string-scan; no JSON crate, mirroring #991). Cross-suite extractors
// (`extract_after`, `golden_partition_position`) live in the shared helpers.
// ===========================================================================

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
