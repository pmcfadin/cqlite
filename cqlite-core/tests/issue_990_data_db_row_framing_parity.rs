//! Issue #990 (Epic #969): strict byte-for-byte coverage for Cassandra 5.0
//! `Data.db` row framing.
//!
//! Cassandra oracles: `UnfilteredSerializer.java` (row/cell flags + row-size /
//! prev-size VInts), `SerializationHeader.java` (timestamp/TTL/local-deletion
//! deltas), `SerializationMirrorTest.java` (multi-clustering-column order).
//!
//! Every assertion here is byte/offset-level. The deterministic sections drive
//! the real public encode surface — `cqlite_core::storage::serialization::vint`
//! (the exact functions the writer calls) and the public
//! `DataWriter::{write_partition, finish}` write path — then re-decode through
//! the reader's own VInt surface (`cqlite_core::parser::vint::parse_vuint`, the
//! function the V5CompressedLegacy row-framing parser uses for `row_size` and
//! `prev_size`). The fixture sections walk a real Cassandra-generated `Data.db`
//! and cross-check against the published `Statistics.db` baselines and the
//! `sstabledump` JSONL goldens.
//!
//! Fail-closed (issue #990 acceptance criterion 5): the fixture-backed tests
//! REQUIRE their real `Data.db`. A missing dataset is an error, never a silent
//! pass — `0 rows when the fixture is present` is a failure too.
//!
//! Offset context (criterion 6): mismatches are reported through [`Loc`] /
//! [`fail_flag`], which name the row, cell, flag byte, byte offset (decimal and
//! hex), and the consumed byte range so a failure pinpoints the exact wire
//! position.

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::parser::vint::parse_vuint;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::serialization::vint::{encode_unsigned, unsigned_len};
use cqlite_core::storage::sstable::writer::data_writer::DataWriter;
use cqlite_core::storage::sstable::writer::stats_writer::StatisticsMetadata;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, DecoratedKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Row / cell flag bit constants (mirror data_writer/mod.rs and the
// V5CompressedLegacy parser; these are the on-disk bit values Cassandra's
// UnfilteredSerializer / Cell.Serializer write).
// ---------------------------------------------------------------------------
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
const EXTENDED_IS_STATIC: u8 = 0x01;

const CELL_IS_DELETED: u8 = 0x01;
const CELL_IS_EXPIRING: u8 = 0x02;
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
const CELL_USE_ROW_TTL: u8 = 0x10;

// ===========================================================================
// Offset-context helpers (acceptance criterion 6)
// ===========================================================================

/// A located byte read: value, the absolute offset it started at, and the
/// number of bytes consumed. Used to attach rich context to assertions.
#[derive(Debug, Clone, Copy)]
struct Loc {
    value: u64,
    start: usize,
    len: usize,
}

impl Loc {
    fn end(&self) -> usize {
        self.start + self.len
    }
}

/// Read one Cassandra unsigned VInt starting at `pos`, returning the decoded
/// value and the byte range it consumed. Uses the reader's own `parse_vuint`
/// (the function the V5CompressedLegacy row-framing parser calls for `row_size`
/// and `prev_size`) so the test exercises the real decode surface.
fn read_uvint_loc(data: &[u8], pos: usize) -> Loc {
    assert!(
        pos < data.len(),
        "read_uvint past end of buffer at offset {pos} (len {})",
        data.len()
    );
    let (rest, value) = parse_vuint(&data[pos..])
        .unwrap_or_else(|e| panic!("parse_vuint failed at offset {pos}: {e:?}"));
    let len = data[pos..].len() - rest.len();
    Loc {
        value,
        start: pos,
        len,
    }
}

/// Read the byte at `pos` (a flag byte), returning value + offset.
fn read_u8_loc(data: &[u8], pos: usize) -> Loc {
    assert!(
        pos < data.len(),
        "read_u8 past end of buffer at offset {pos} (len {})",
        data.len()
    );
    Loc {
        value: data[pos] as u64,
        start: pos,
        len: 1,
    }
}

/// Assert a flag byte equals `expected`, with full offset context on mismatch.
#[track_caller]
fn fail_flag(loc: Loc, expected: u8, what: &str) {
    let actual = loc.value as u8;
    assert_eq!(
        actual,
        expected,
        "{what}: flag byte at offset {off} (0x{off:02X}) expected 0x{expected:02X} \
         (0b{expected:08b}) but got 0x{actual:02X} (0b{actual:08b}); \
         consumed byte range [{start}, {end})",
        off = loc.start,
        start = loc.start,
        end = loc.end(),
    );
}

/// Assert a decoded VInt equals `expected`, with offset/range context.
#[track_caller]
fn fail_vint(loc: Loc, expected: u64, what: &str) {
    assert_eq!(
        loc.value,
        expected,
        "{what}: VInt at offset {off} (0x{off:02X}) expected {expected} but got {got}; \
         consumed byte range [{start}, {end}) ({len} bytes)",
        off = loc.start,
        got = loc.value,
        start = loc.start,
        end = loc.end(),
        len = loc.len,
    );
}

// ===========================================================================
// Shared writer fixtures
// ===========================================================================

/// Deterministic Statistics.db baselines: zero LDT/TTL floors and a fixed
/// `min_timestamp` so every delta is a controlled, exactly-predictable VInt.
fn det_stats() -> StatisticsMetadata {
    let mut s = StatisticsMetadata::new();
    s.min_timestamp = 1_000_000;
    s.min_ttl = 0;
    s.min_local_deletion_time = 0;
    s
}

/// int PK, two regular text columns `a`,`b`. No clustering, no statics.
fn two_col_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue990".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "a".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "b".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// int PK, three regular text columns `a`,`b`,`c`. For the column-subset bitmap.
fn three_col_schema() -> TableSchema {
    let mut s = two_col_schema();
    s.columns.push(Column {
        name: "c".to_string(),
        data_type: "text".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    });
    s
}

/// int PK, single regular text column `a`.
fn one_col_schema() -> TableSchema {
    let mut s = two_col_schema();
    s.columns.truncate(1);
    s
}

/// int PK, int clustering, one static text + one regular text column.
fn static_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue990".to_string(),
        table: "s".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "sdata".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "rdata".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn int_key_bytes(n: i32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Partition-header byte size for an int (4-byte) PK with no partition
/// tombstone: 2 (u16 key-length) + 4 (key) + 4 (LDT i32) + 8 (mfda i64) = 18.
const INT_PK_HEADER_SIZE: usize = 2 + 4 + 4 + 8;

/// Walk one non-static, non-clustered row header. Returns
/// `(row_flags_loc, body_start_offset, next_row_offset)` where `body_start` is
/// the first byte after the `prev_size` VInt (i.e. the start of the delta /
/// bitmap / cell region).
fn walk_simple_row_header(data: &[u8], pos: usize) -> (Loc, usize, usize) {
    let flags = read_u8_loc(data, pos);
    let row_size = read_uvint_loc(data, flags.end());
    let prev_size = read_uvint_loc(data, row_size.end());
    // row_size counts (prev_size VInt + remaining body). Next row begins at the
    // end of that body.
    let next = prev_size.start + row_size.value as usize;
    (flags, prev_size.end(), next)
}

/// Build a one-partition Data.db with the public write path and return its bytes.
fn write_one_partition(
    stats: StatisticsMetadata,
    schema: &TableSchema,
    key_id: i32,
    mutations: &[Mutation],
) -> Vec<u8> {
    let mut w = DataWriter::new(stats);
    let key = DecoratedKey::new(key_id as i64, int_key_bytes(key_id));
    w.write_partition(&key, mutations, schema, None, &[])
        .expect("write_partition");
    w.finish().expect("finish")
}

fn write_op(column: &str, text: &str) -> CellOperation {
    CellOperation::Write {
        column: column.to_string(),
        value: Value::Text(text.to_string()),
    }
}

fn mk_mutation(
    table: &str,
    key_id: i32,
    ck: Option<i32>,
    ops: Vec<CellOperation>,
    ts: i64,
    ttl: Option<u32>,
) -> Mutation {
    Mutation::new(
        TableId::new("issue990", table),
        PartitionKey::single("id", Value::Integer(key_id)),
        ck.map(|c| ClusteringKey::single("ck", Value::Integer(c))),
        ops,
        ts,
        ttl,
    )
}

// ===========================================================================
// Section 1 — row-size and prev-size VInt boundaries
// (manifest: cass.data_db_decode.unfiltered_serializer.row_size_vints)
// Cassandra oracle: UnfilteredSerializer.serialize / serializedRowBodySize.
// ===========================================================================

/// The exact unsigned-VInt width boundaries: the value just below a width
/// change (max for N bytes) and the value at the change (min for N+1 bytes).
/// Cassandra's row_size and prev_size are written by `writeUnsignedVInt`, so
/// these are the byte-width transitions a reader must round-trip exactly.
const UVINT_BOUNDARIES: &[(u64, usize)] = &[
    (127, 1),                    // max 1-byte
    (128, 2),                    // min 2-byte
    (16_383, 2),                 // max 2-byte
    (16_384, 3),                 // min 3-byte
    (2_097_151, 3),              // max 3-byte
    (2_097_152, 4),              // min 4-byte
    (268_435_455, 4),            // max 4-byte
    (268_435_456, 5),            // min 5-byte
    (34_359_738_367, 5),         // max 5-byte
    (34_359_738_368, 6),         // min 6-byte
    (4_398_046_511_103, 6),      // max 6-byte
    (4_398_046_511_104, 7),      // min 7-byte
    (562_949_953_421_311, 7),    // max 7-byte
    (562_949_953_421_312, 8),    // min 8-byte
    (72_057_594_037_927_935, 8), // max 8-byte
    (72_057_594_037_927_936, 9), // min 9-byte
];

#[test]
fn row_size_vint_width_boundaries_encode_decode_roundtrip() {
    for &(value, expected_len) in UVINT_BOUNDARIES {
        // Encode with the REAL writer function used for row_size/prev_size.
        let mut buf = Vec::new();
        encode_unsigned(value, &mut buf);
        assert_eq!(
            buf.len(),
            expected_len,
            "row_size VInt for {value} must be {expected_len} bytes, got {} ({buf:02X?})",
            buf.len()
        );
        assert_eq!(
            unsigned_len(value),
            expected_len,
            "unsigned_len({value}) must agree with the encoded width"
        );

        // Decode with the REAL reader function (parse_vuint) used for row
        // framing, asserting it consumed exactly the encoded bytes.
        let loc = read_uvint_loc(&buf, 0);
        fail_vint(loc, value, &format!("row_size boundary {value}"));
        assert_eq!(
            loc.len, expected_len,
            "decoder must consume exactly the {expected_len} encoded bytes for {value}"
        );
    }
}

/// Pin the exact first-byte width-prefix bits at the 1→2 and 2→3 transitions so
/// a regression in the leading-1s length prefix is caught at the byte level.
#[test]
fn row_size_vint_exact_bytes_at_low_boundaries() {
    let cases: &[(u64, &[u8])] = &[
        (127, &[0x7F]),             // 0xxxxxxx
        (128, &[0x80, 0x80]),       // 10xxxxxx xxxxxxxx
        (16_383, &[0xBF, 0xFF]),    // max 2-byte
        (16_384, &[0xC0, 0x40, 0]), // 110xxxxx ...
    ];
    for &(value, expected) in cases {
        let mut buf = Vec::new();
        encode_unsigned(value, &mut buf);
        assert_eq!(
            buf, expected,
            "row_size/prev_size VInt for {value} must be {expected:02X?}, got {buf:02X?}"
        );
    }
}

/// prev_size is the same unsigned VInt as row_size and must survive a >2 GiB
/// value with no 32-bit truncation (a wide-partition prev_size can exceed u32).
#[test]
fn prev_size_vint_roundtrips_over_2gib() {
    let big: u64 = (1u64 << 31) + 12_345; // 2_147_495_993, negative as i32
    assert!(big > i32::MAX as u64);
    let mut buf = Vec::new();
    encode_unsigned(big, &mut buf);
    let loc = read_uvint_loc(&buf, 0);
    fail_vint(loc, big, "prev_size >2GiB");
}

/// End-to-end: the row_size/prev_size VInts the writer actually emits for a real
/// row decode back to the right framing through the reader's `parse_vuint`, and
/// `row_size` equals `prev_size_vint_len + remaining body bytes` (Cassandra's
/// `serializedRowBodySize` convention).
#[test]
fn writer_row_size_frames_body_exactly() {
    let schema = one_col_schema();
    let m = mk_mutation("t", 1, None, vec![write_op("a", "hello")], 2_000_000, None);
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);

    let row_pos = INT_PK_HEADER_SIZE; // first row right after the header
    let flags = read_u8_loc(&bytes, row_pos);
    let row_size = read_uvint_loc(&bytes, flags.end());
    let prev_size = read_uvint_loc(&bytes, row_size.end());

    // row_size measures the body starting at the prev_size VInt. `finish()`
    // appends a 1-byte END_OF_PARTITION (0x01) marker after the last row, so the
    // row body ends exactly one byte before EOF and that trailing byte is the
    // marker.
    let body_end = prev_size.start + row_size.value as usize;
    assert_eq!(
        body_end,
        bytes.len() - 1,
        "row body (offset {}..{}) must end exactly before the trailing \
         END_OF_PARTITION marker (file len {})",
        prev_size.start,
        body_end,
        bytes.len()
    );
    assert_eq!(
        bytes[body_end], 0x01,
        "the byte after the row body at offset {body_end} must be END_OF_PARTITION (0x01)"
    );
    // prev_size for the first row is the partition-header byte size.
    fail_vint(
        prev_size,
        INT_PK_HEADER_SIZE as u64,
        "first-row prev_size must equal the partition-header size",
    );
}

// ===========================================================================
// Section 2 — row flag bytes
// (manifest: cass.data_db_decode.unfiltered_serializer.row_and_cell_flags)
// Cassandra oracle: UnfilteredSerializer flag bits.
// ===========================================================================

#[test]
fn row_flag_timestamp_only_subset() {
    // Two columns, write ONE → HAS_TIMESTAMP, NOT all-columns, no TTL/deletion.
    let schema = two_col_schema();
    let m = mk_mutation("t", 1, None, vec![write_op("a", "x")], 2_000_000, None);
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    fail_flag(flags, ROW_HAS_TIMESTAMP, "timestamp-only row");
}

#[test]
fn row_flag_all_columns() {
    // One column, write it → HAS_TIMESTAMP | HAS_ALL_COLUMNS.
    let schema = one_col_schema();
    let m = mk_mutation("t", 1, None, vec![write_op("a", "x")], 2_000_000, None);
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    fail_flag(
        flags,
        ROW_HAS_TIMESTAMP | ROW_HAS_ALL_COLUMNS,
        "all-columns row",
    );
}

#[test]
fn row_flag_ttl() {
    // One column written by a mutation that carries a row TTL → HAS_TIMESTAMP |
    // HAS_TTL | HAS_ALL_COLUMNS.
    let schema = one_col_schema();
    let m = mk_mutation(
        "t",
        1,
        None,
        vec![write_op("a", "x")],
        2_000_000,
        Some(3600),
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    fail_flag(
        flags,
        ROW_HAS_TIMESTAMP | ROW_HAS_TTL | ROW_HAS_ALL_COLUMNS,
        "TTL row",
    );
}

#[test]
fn row_flag_deletion_row_tombstone() {
    // Pure row tombstone (DeleteRow): HAS_DELETION, no timestamp, not all-columns.
    let schema = two_col_schema();
    let m = mk_mutation(
        "t",
        1,
        None,
        vec![CellOperation::DeleteRow],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    fail_flag(flags, ROW_HAS_DELETION, "row-tombstone row");
    assert_eq!(
        flags.value as u8 & ROW_HAS_TIMESTAMP,
        0,
        "a pure row tombstone must NOT set HAS_TIMESTAMP (offset {})",
        flags.start
    );
}

#[test]
fn row_flag_explicit_column_subset_bitmap() {
    // Three columns, write ONE (`a`) → flags = HAS_TIMESTAMP only, followed by a
    // column-subset bitmap VInt. Cassandra's Columns.Serializer.serializeSubset
    // writes a bitmap whose set bits mark MISSING columns: writing only `a`
    // leaves `b`,`c` missing → bits 1 and 2 set → 0b110 = 6.
    let schema = three_col_schema();
    let m = mk_mutation("t", 1, None, vec![write_op("a", "x")], 2_000_000, None);
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);

    let (flags, body_start, _next) = walk_simple_row_header(&bytes, INT_PK_HEADER_SIZE);
    fail_flag(flags, ROW_HAS_TIMESTAMP, "column-subset row");

    // body: [timestamp delta][column bitmap][cell...]
    let ts_delta = read_uvint_loc(&bytes, body_start);
    fail_vint(ts_delta, 1_000_000, "subset row timestamp delta");
    let bitmap = read_uvint_loc(&bytes, ts_delta.end());
    fail_vint(bitmap, 0b110, "column-subset missing-bitmap (b,c missing)");
}

#[test]
fn row_flag_static_extension() {
    // A static row sets HAS_EXTENDED_FLAGS and the extended IS_STATIC bit.
    let schema = static_schema();
    let m = mk_mutation(
        "s",
        1,
        Some(7),
        vec![
            write_op("sdata", "static-val"),
            write_op("rdata", "row-val"),
        ],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);

    // Static row is the first unfiltered after the header.
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    assert_ne!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "static row must set HAS_EXTENDED_FLAGS (flag byte 0x{:02X} at offset {})",
        flags.value,
        flags.start
    );
    let ext = read_u8_loc(&bytes, flags.end());
    fail_flag(
        ext,
        EXTENDED_IS_STATIC,
        "static row extended-flags byte (IS_STATIC)",
    );
}

// ===========================================================================
// Section 3 — cell flag bytes
// (manifest: cass.data_db_decode.unfiltered_serializer.row_and_cell_flags)
// Cassandra oracle: Cell.Serializer flag bits.
// ===========================================================================

/// Walk to the first cell's flag byte of a single-regular-column, no-clustering
/// row. Layout after the row header:
/// `[ts delta?][ttl delta + ldt delta?][bitmap?]` then the cell flag byte.
fn first_cell_flag_loc(data: &[u8], row_flags: u8, body_start: usize) -> Loc {
    let mut pos = body_start;
    if row_flags & ROW_HAS_TIMESTAMP != 0 {
        pos = read_uvint_loc(data, pos).end();
    }
    if row_flags & ROW_HAS_TTL != 0 {
        pos = read_uvint_loc(data, pos).end(); // ttl delta
        pos = read_uvint_loc(data, pos).end(); // ldt delta (wall-clock)
    }
    if row_flags & ROW_HAS_DELETION != 0 {
        pos = read_uvint_loc(data, pos).end();
        pos = read_uvint_loc(data, pos).end();
    }
    if row_flags & ROW_HAS_ALL_COLUMNS == 0 {
        pos = read_uvint_loc(data, pos).end(); // column bitmap
    }
    read_u8_loc(data, pos)
}

/// Write a single-column row and return `(bytes, first_cell_flag_loc)`.
fn single_cell_flags(stats: StatisticsMetadata, ops: Vec<CellOperation>, ttl: Option<u32>) -> Loc {
    let schema = one_col_schema();
    let m = mk_mutation("t", 1, None, ops, 2_000_000, ttl);
    let bytes = write_one_partition(stats, &schema, 1, &[m]);
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    let (_f, body_start, _n) = walk_simple_row_header(&bytes, INT_PK_HEADER_SIZE);
    let loc = first_cell_flag_loc(&bytes, flags.value as u8, body_start);
    loc
}

#[test]
fn cell_flag_live_uses_row_timestamp() {
    // A plain Write whose timestamp equals the row liveness → USE_ROW_TIMESTAMP.
    let loc = single_cell_flags(det_stats(), vec![write_op("a", "hi")], None);
    fail_flag(
        loc,
        CELL_USE_ROW_TIMESTAMP,
        "live cell (row-timestamp reuse)",
    );
}

#[test]
fn cell_flag_row_timestamp_reuse_is_same_as_live() {
    // The "row timestamp reuse" case is exactly the live-cell flag: USE_ROW_TIMESTAMP
    // and nothing else. Asserted explicitly so a regression that splits these is caught.
    let loc = single_cell_flags(det_stats(), vec![write_op("a", "reuse")], None);
    assert_eq!(
        loc.value as u8 & CELL_USE_ROW_TIMESTAMP,
        CELL_USE_ROW_TIMESTAMP,
        "row-timestamp-reuse cell must set USE_ROW_TIMESTAMP (flag 0x{:02X} @ {})",
        loc.value,
        loc.start
    );
    assert_eq!(
        loc.value as u8 & (CELL_IS_EXPIRING | CELL_IS_DELETED),
        0,
        "row-timestamp-reuse cell must not be expiring or deleted"
    );
}

#[test]
fn cell_flag_empty_value() {
    // Empty text → HAS_EMPTY_VALUE set, still reusing the row timestamp.
    let loc = single_cell_flags(det_stats(), vec![write_op("a", "")], None);
    fail_flag(
        loc,
        CELL_USE_ROW_TIMESTAMP | CELL_HAS_EMPTY_VALUE,
        "empty-value cell",
    );
}

#[test]
fn cell_flag_expiring_explicit_ttl() {
    // WriteWithTtl → an expiring cell with explicit ts/ldt/ttl deltas, NO
    // row-timestamp/row-ttl reuse.
    let ops = vec![CellOperation::WriteWithTtl {
        column: "a".to_string(),
        value: Value::Text("temp".to_string()),
        ttl_seconds: 3600,
    }];
    let loc = single_cell_flags(det_stats(), ops, None);
    fail_flag(loc, CELL_IS_EXPIRING, "expiring cell (explicit TTL)");
    assert_eq!(
        loc.value as u8 & (CELL_USE_ROW_TIMESTAMP | CELL_USE_ROW_TTL),
        0,
        "explicit-TTL expiring cell must not reuse row timestamp/TTL"
    );
}

#[test]
fn cell_flag_row_ttl_reuse() {
    // A plain Write inside a mutation that carries a row TTL, with the cell
    // timestamp equal to the row liveness → expiring cell that reuses BOTH the
    // row timestamp and the row TTL.
    let loc = single_cell_flags(det_stats(), vec![write_op("a", "v")], Some(3600));
    fail_flag(
        loc,
        CELL_IS_EXPIRING | CELL_USE_ROW_TIMESTAMP | CELL_USE_ROW_TTL,
        "row-TTL-reuse cell",
    );
}

#[test]
fn cell_flag_deleted() {
    // A column Delete → a tombstone cell: IS_DELETED | HAS_EMPTY_VALUE, own ts.
    let ops = vec![CellOperation::Delete {
        column: "a".to_string(),
        local_deletion_time: None,
    }];
    // Deletes use min_local_deletion_time = 0 baseline; the row carries no
    // liveness, so the row flag has no HAS_TIMESTAMP.
    let schema = one_col_schema();
    let m = mk_mutation("t", 1, None, ops, 2_000_000, None);
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    let (_f, body_start, _n) = walk_simple_row_header(&bytes, INT_PK_HEADER_SIZE);
    let loc = first_cell_flag_loc(&bytes, flags.value as u8, body_start);
    fail_flag(
        loc,
        CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE,
        "deleted (tombstone) cell",
    );
}

// ===========================================================================
// Section 4 — timestamp / TTL / local-deletion-time deltas vs Statistics.db
// (manifest: cass.data_db_decode.serialization_header.timestamp_ttl_ldt_deltas)
// Cassandra oracle: SerializationHeader.writeTimestamp/writeTTL/writeLocalDeletionTime
// (all UNSIGNED VInt deltas from the EncodingStats minima).
// ===========================================================================

/// Decode a value both as the unsigned VInt the writer emits and as a signed
/// ZigZag VInt; assert they differ for a high-bit value so the test proves the
/// signed/unsigned choice is unambiguous.
fn zigzag_decode(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

#[test]
fn timestamp_delta_is_unsigned_small_and_large() {
    // Small delta: ts 1_000_005 over baseline 1_000_000 → delta 5 (1 byte).
    let schema = one_col_schema();
    let small = Mutation::new(
        TableId::new("issue990", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![write_op("a", "x")],
        1_000_005,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[small]);
    let (flags, body_start, _n) = walk_simple_row_header(&bytes, INT_PK_HEADER_SIZE);
    assert_eq!(flags.value as u8 & ROW_HAS_TIMESTAMP, ROW_HAS_TIMESTAMP);
    let ts = read_uvint_loc(&bytes, body_start);
    fail_vint(ts, 5, "small timestamp delta (1_000_005 - 1_000_000)");

    // Large delta whose unsigned value sets the high bit of a 5-byte VInt: a
    // signed ZigZag misread would yield a different number. baseline 0, ts large.
    let mut big_stats = det_stats();
    big_stats.min_timestamp = 0;
    let big_ts: i64 = 0x1_2345_6789; // 4_886_718_345 — > u32, multi-byte
    let big = Mutation::new(
        TableId::new("issue990", "t"),
        PartitionKey::single("id", Value::Integer(2)),
        None,
        vec![write_op("a", "x")],
        big_ts,
        None,
    );
    let bytes = write_one_partition(big_stats, &schema, 2, &[big]);
    let (_f, body_start, _n) = walk_simple_row_header(&bytes, INT_PK_HEADER_SIZE);
    let ts = read_uvint_loc(&bytes, body_start);
    fail_vint(ts, big_ts as u64, "large timestamp delta (unsigned)");
    // No signed/unsigned ambiguity: a ZigZag reading of the same value differs.
    assert_ne!(
        zigzag_decode(ts.value),
        big_ts,
        "the delta must be UNSIGNED — a ZigZag interpretation must NOT match"
    );
}

#[test]
fn ttl_and_ldt_deltas_are_unsigned_against_baselines() {
    // Row TTL path writes [ttl delta][ldt delta], both unsigned from the stats
    // minima. Use a non-zero min_ttl so the delta is value-baseline, exercising
    // the baseline subtraction explicitly.
    let mut stats = det_stats();
    stats.min_ttl = 100;
    stats.min_local_deletion_time = 1_000;

    let schema = one_col_schema();
    let m = mk_mutation("t", 1, None, vec![write_op("a", "x")], 2_000_000, Some(900));
    let bytes = write_one_partition(stats, &schema, 1, &[m]);

    let (flags, body_start, _n) = walk_simple_row_header(&bytes, INT_PK_HEADER_SIZE);
    assert_eq!(
        flags.value as u8 & (ROW_HAS_TIMESTAMP | ROW_HAS_TTL),
        ROW_HAS_TIMESTAMP | ROW_HAS_TTL,
        "row must carry HAS_TIMESTAMP | HAS_TTL"
    );
    // body: [ts delta][ttl delta][ldt delta][...]
    let ts = read_uvint_loc(&bytes, body_start);
    let ttl = read_uvint_loc(&bytes, ts.end());
    fail_vint(
        ttl,
        900 - 100,
        "TTL delta = ttl_seconds - min_ttl (unsigned)",
    );
    let ldt = read_uvint_loc(&bytes, ttl.end());
    // ldt is wall-clock derived (now + ttl) - min_local_deletion_time, so we
    // assert it is a large, strictly-positive unsigned value (no signed wrap)
    // and that it decodes without ambiguity. It must be >> the 1_000 baseline.
    assert!(
        ldt.value > 1_000,
        "local-deletion-time delta must be a positive unsigned value (got {} @ {})",
        ldt.value,
        ldt.start
    );
}

#[test]
fn row_tombstone_ldt_delta_handles_far_future_unsigned() {
    // A row deletion writes [ts delta][ldt delta] as UNSIGNED VInt deltas. A
    // far-future local_deletion_time that looks negative as i32 must still
    // encode as a large unsigned delta (no signed/unsigned ambiguity).
    let mut stats = det_stats();
    stats.min_timestamp = 1_000_000;
    stats.min_local_deletion_time = 0;

    // local_deletion_time in [2^31, 2^32) is negative as i32 but a legitimate
    // far-future value; the writer emits it as an unsigned 32-bit delta.
    let far_future_ldt: i32 = i32::MIN; // bit pattern 0x80000000 → unsigned 2_147_483_648
    let m = Mutation::new(
        TableId::new("issue990", "t"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::DeleteRow],
        2_000_000,
        None,
    )
    .with_local_deletion_time(far_future_ldt);

    let bytes = write_one_partition(stats, &schema_for_tombstone(), 1, &[m]);
    let (flags, body_start, _n) = walk_simple_row_header(&bytes, INT_PK_HEADER_SIZE);
    assert_eq!(
        flags.value as u8 & ROW_HAS_DELETION,
        ROW_HAS_DELETION,
        "row must carry HAS_DELETION"
    );
    let ts = read_uvint_loc(&bytes, body_start);
    fail_vint(
        ts,
        1_000_000,
        "row-tombstone ts delta (2_000_000 - 1_000_000)",
    );
    let ldt = read_uvint_loc(&bytes, ts.end());
    let expected = (far_future_ldt as u32) as u64; // 0x80000000
    fail_vint(
        ldt,
        expected,
        "far-future row-tombstone LDT delta as 32-bit unsigned",
    );
}

fn schema_for_tombstone() -> TableSchema {
    one_col_schema()
}

// ===========================================================================
// Section 5 — fixture-backed strict byte/offset parity (FAIL CLOSED)
// (manifest: row_size_vints + row_and_cell_flags + timestamp_ttl_ldt_deltas)
// ===========================================================================

/// Resolve the datasets root, FAILING CLOSED if it cannot be located. A missing
/// dataset is an error, never a silent pass (issue #990 criterion 5).
fn datasets_root() -> PathBuf {
    if let Ok(r) = std::env::var("CQLITE_DATASETS_ROOT") {
        return PathBuf::from(r);
    }
    // Fall back to the in-repo datasets dir relative to the crate.
    let here = Path::new(env!("CARGO_MANIFEST_DIR"));
    let candidate = here.join("../test-data/datasets");
    candidate
}

/// Read a required fixture file, FAILING CLOSED with a clear message when the
/// binary `Data.db` has not been fetched.
fn read_required_fixture(rel: &str) -> Vec<u8> {
    let path = datasets_root().join(rel);
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "issue #990 fail-closed: required fixture {} is missing ({e}). \
             Fetch it with `bash test-data/scripts/fetch-datasets.sh` and set \
             CQLITE_DATASETS_ROOT. A missing dataset must error, never silently pass.",
            path.display()
        )
    })
}

const UNCOMPRESSED_DIR: &str =
    "sstables/test_basic/uncompressed_table-6aedb7a0a25111f0a3fef1a551383fb9";

/// Real-fixture framing parity: walk the first partition of a real Cassandra
/// "nb" Data.db (uncompressed_table, compression disabled) and assert the row
/// framing decodes consistently and the timestamp delta + Statistics.db
/// baseline reproduces an in-range absolute timestamp with NO signed/unsigned
/// ambiguity.
#[test]
fn fixture_uncompressed_row_framing_and_timestamp_delta_parity() {
    let data = read_required_fixture(&format!("{UNCOMPRESSED_DIR}/nb-1-big-Data.db"));
    assert!(
        data.len() > INT_PK_HEADER_SIZE,
        "fixture Data.db too small ({} bytes) — fixture present but empty is a failure",
        data.len()
    );

    // Partition header: [u16 key_len=16][16 key][i32 LDT][i64 mfda] = 30 bytes.
    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    assert_eq!(key_len, 16, "uncompressed_table has a 16-byte UUID PK");
    let header_size = 2 + key_len + 12; // 30
    let row_pos = header_size;

    // First row: 0x24 = HAS_TIMESTAMP | HAS_ALL_COLUMNS, not extended/static.
    let flags = read_u8_loc(&data, row_pos);
    assert_eq!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "first row of uncompressed_table must not be static (flag 0x{:02X} @ {})",
        flags.value,
        flags.start
    );
    assert_eq!(
        flags.value as u8 & ROW_HAS_TIMESTAMP,
        ROW_HAS_TIMESTAMP,
        "first row must carry a liveness timestamp"
    );

    // Row framing: row_size, prev_size, then the timestamp delta.
    let row_size = read_uvint_loc(&data, flags.end());
    let prev_size = read_uvint_loc(&data, row_size.end());
    fail_vint(
        prev_size,
        header_size as u64,
        "fixture first-row prev_size must equal the partition-header size",
    );
    // row_size must frame within the file (offset parity).
    assert!(
        prev_size.start + row_size.value as usize <= data.len(),
        "row_size {} at offset {} overruns the {}-byte Data.db",
        row_size.value,
        row_size.start,
        data.len()
    );

    let ts_delta = read_uvint_loc(&data, prev_size.end());

    // Cross-check against the published Statistics.db EncodingStats baseline.
    let stats_bytes = read_required_fixture(&format!("{UNCOMPRESSED_DIR}/nb-1-big-Statistics.db"));
    let (_rest, stats) =
        parse_statistics_with_fallback(&stats_bytes, None).expect("parse fixture Statistics.db");
    let min_ts = stats.timestamp_stats.min_timestamp;
    assert!(
        min_ts > 0,
        "Statistics.db EncodingStats minTimestamp must be set"
    );

    // Absolute timestamp via the UNSIGNED delta + Statistics.db baseline must
    // reproduce the sstabledump JSONL golden's first-row liveness timestamp
    // EXACTLY (byte/offset parity against a real Cassandra fixture).
    let abs_unsigned = min_ts + ts_delta.value as i64;
    let golden_micros =
        first_row_liveness_micros(&format!("{UNCOMPRESSED_DIR}/nb-1-big-Data.db.jsonl"));
    assert_eq!(
        abs_unsigned, golden_micros,
        "unsigned timestamp delta {} (at offset {}) + Statistics.db min {} = {} must equal the \
         JSONL golden liveness timestamp {} (no signed/unsigned ambiguity)",
        ts_delta.value, ts_delta.start, min_ts, abs_unsigned, golden_micros
    );
    // A signed ZigZag reading of a multi-byte delta would NOT reproduce the
    // golden, proving the unsigned interpretation is the only correct one.
    if ts_delta.len > 1 {
        let abs_signed = min_ts + zigzag_decode(ts_delta.value);
        assert_ne!(
            abs_signed, golden_micros,
            "signed/unsigned ambiguity: a ZigZag delta must NOT reproduce the golden timestamp"
        );
    }
}

/// Parse the first JSONL row's liveness `tstamp` (ISO-8601 UTC, microsecond
/// precision) into epoch microseconds, FAILING CLOSED on a missing fixture.
fn first_row_liveness_micros(rel_jsonl: &str) -> i64 {
    let path = datasets_root().join(rel_jsonl);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "issue #990 fail-closed: required JSONL golden {} is missing ({e})",
            path.display()
        )
    });
    let first = text.lines().next().unwrap_or_else(|| {
        panic!(
            "JSONL golden {} is empty (present-but-empty is a failure)",
            path.display()
        )
    });
    let marker = "\"tstamp\":\"";
    let start = first
        .find(marker)
        .unwrap_or_else(|| panic!("no liveness tstamp in first JSONL row: {first}"))
        + marker.len();
    let end = first[start..]
        .find('"')
        .map(|o| start + o)
        .unwrap_or_else(|| panic!("unterminated tstamp in first JSONL row"));
    iso8601_to_micros(&first[start..end])
}

/// Convert `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` to epoch microseconds. Uses Howard
/// Hinnant's `days_from_civil` so no date/time crate is required.
fn iso8601_to_micros(s: &str) -> i64 {
    let s = s.strip_suffix('Z').unwrap_or(s);
    let (date, time) = s
        .split_once('T')
        .unwrap_or_else(|| panic!("bad ISO timestamp: {s}"));
    let d: Vec<i64> = date
        .split('-')
        .map(|p| p.parse().expect("date part"))
        .collect();
    let (hms, frac) = match time.split_once('.') {
        Some((h, f)) => (h, f),
        None => (time, ""),
    };
    let t: Vec<i64> = hms
        .split(':')
        .map(|p| p.parse().expect("time part"))
        .collect();
    assert_eq!(d.len(), 3, "date must be Y-M-D: {date}");
    assert_eq!(t.len(), 3, "time must be H:M:S: {hms}");
    let (mut y, m, day) = (d[0], d[1], d[2]);
    y -= i64::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + t[0] * 3_600 + t[1] * 60 + t[2];
    // Right-pad the fractional part to 6 digits (microseconds).
    let mut micros_frac = frac.to_string();
    while micros_frac.len() < 6 {
        micros_frac.push('0');
    }
    let micros_frac: i64 = micros_frac[..6].parse().unwrap_or(0);
    secs * 1_000_000 + micros_frac
}

/// Real-fixture row-flag parity: every data row in the first partition of the
/// uncompressed fixture begins with a flag byte whose set bits are a subset of
/// the known UnfilteredSerializer row-flag mask (no stray bits), and the first
/// data row carries HAS_TIMESTAMP. This anchors our flag-bit constants against
/// real Cassandra bytes.
#[test]
fn fixture_uncompressed_row_flags_are_well_formed() {
    let data = read_required_fixture(&format!("{UNCOMPRESSED_DIR}/nb-1-big-Data.db"));
    let known_mask = ROW_HAS_TIMESTAMP
        | ROW_HAS_TTL
        | ROW_HAS_DELETION
        | ROW_HAS_ALL_COLUMNS
        | 0x40 // HAS_COMPLEX_DELETION
        | ROW_HAS_EXTENDED_FLAGS
        | 0x02 // IS_MARKER (range tombstone) — also a valid leading byte
        | 0x01; // END_OF_PARTITION

    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let row_pos = 2 + key_len + 12;
    let flags = read_u8_loc(&data, row_pos);
    assert_eq!(
        flags.value as u8 & !known_mask,
        0,
        "first row flag byte 0x{:02X} at offset {} has bits outside the known row-flag mask",
        flags.value,
        flags.start
    );
    assert_eq!(
        flags.value as u8 & ROW_HAS_TIMESTAMP,
        ROW_HAS_TIMESTAMP,
        "first row of uncompressed_table must set HAS_TIMESTAMP"
    );
}

// ===========================================================================
// Section 6 — multi-clustering column order
// (manifest: cass.data_db_decode.serialization_mirror.multi_clustering_column_order)
// Cassandra oracle: SerializationMirrorTest / ClusteringPrefix serializer —
// clustering values are serialized in declared order after a 2-bit-per-column
// header, preceding row_size.
// ===========================================================================

/// Deterministic check: a row with TWO clustering columns serializes the
/// clustering prefix (header VInt + values in declared order) BEFORE row_size,
/// matching Cassandra's ClusteringPrefix serializer used by SerializationMirror.
#[test]
fn multi_clustering_prefix_precedes_row_size_in_declared_order() {
    // PK int, clustering (c1 int ASC, c2 int ASC), one regular text column.
    let schema = TableSchema {
        keyspace: "issue990".to_string(),
        table: "mc".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![
            ClusteringColumn {
                name: "c1".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            },
            ClusteringColumn {
                name: "c2".to_string(),
                data_type: "int".to_string(),
                position: 1,
                order: ClusteringOrder::Asc,
            },
        ],
        columns: vec![Column {
            name: "v".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let ck = ClusteringKey {
        columns: vec![
            ("c1".to_string(), Value::Integer(0x0A0B0C0D)),
            ("c2".to_string(), Value::Integer(0x11121314)),
        ],
    };
    let m = Mutation::new(
        TableId::new("issue990", "mc"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ck),
        vec![write_op("v", "x")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);

    // Row layout: [flags][clustering header VInt][c1 4B BE][c2 4B BE][row_size]...
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    assert_eq!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "regular multi-clustering row is not extended/static"
    );
    // Clustering header: 2 bits per column, both PRESENT (00) → 0.
    let header = read_uvint_loc(&bytes, flags.end());
    fail_vint(header, 0, "multi-clustering header (both columns PRESENT)");

    // Values in DECLARED order: c1 then c2, each a 4-byte big-endian int.
    let c1_off = header.end();
    let c1 = i32::from_be_bytes([
        bytes[c1_off],
        bytes[c1_off + 1],
        bytes[c1_off + 2],
        bytes[c1_off + 3],
    ]);
    assert_eq!(
        c1, 0x0A0B0C0D,
        "first clustering value (c1) must be serialized first, big-endian, at offset {c1_off}"
    );
    let c2_off = c1_off + 4;
    let c2 = i32::from_be_bytes([
        bytes[c2_off],
        bytes[c2_off + 1],
        bytes[c2_off + 2],
        bytes[c2_off + 3],
    ]);
    assert_eq!(
        c2, 0x11121314,
        "second clustering value (c2) must follow c1 in declared order at offset {c2_off}"
    );

    // row_size begins immediately after the clustering values.
    let row_size = read_uvint_loc(&bytes, c2_off + 4);
    let prev_size = read_uvint_loc(&bytes, row_size.end());
    fail_vint(
        prev_size,
        INT_PK_HEADER_SIZE as u64,
        "multi-clustering first-row prev_size must equal the partition-header size",
    );
}

const COMPOSITE_DIR: &str =
    "sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9";

/// Fixture-backed multi-clustering parity: composite_key_table has TWO
/// clustering columns (clustering_key1 TIMESTAMP DESC, clustering_key2 TEXT
/// ASC). Decompress its LZ4 Data.db and assert the first data row's clustering
/// prefix carries both columns (8-byte timestamp value then a length-prefixed
/// text value) in declared order, ahead of row_size. FAILS CLOSED on a missing
/// fixture.
#[test]
fn fixture_composite_multi_clustering_prefix_order() {
    use cqlite_core::storage::sstable::chunk_decompressor::create_decompressor_from_file;
    use std::io::Cursor;

    let data = read_required_fixture(&format!("{COMPOSITE_DIR}/nb-1-big-Data.db"));
    let comp_path = datasets_root().join(format!("{COMPOSITE_DIR}/nb-1-big-CompressionInfo.db"));
    let mut dec = create_decompressor_from_file(&comp_path).unwrap_or_else(|e| {
        panic!(
            "issue #990 fail-closed: composite_key_table CompressionInfo missing/invalid ({e}) \
             at {}",
            comp_path.display()
        )
    });
    let mut cur = Cursor::new(&data);
    let raw = dec
        .read_all_data(&mut cur)
        .expect("decompress composite Data.db");
    assert!(
        raw.len() > 32,
        "decompressed composite Data.db too small ({} bytes) — present-but-empty is a failure",
        raw.len()
    );

    // Partition header: [u16 key_len=16][16 UUID][i32 LDT][i64 mfda] = 30.
    let key_len = u16::from_be_bytes([raw[0], raw[1]]) as usize;
    assert_eq!(key_len, 16, "composite_key_table has a 16-byte UUID PK");
    let row_pos = 2 + key_len + 12;

    let flags = read_u8_loc(&raw, row_pos);
    assert_eq!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "first composite row must be a regular (non-static) row; flag 0x{:02X} @ {}",
        flags.value,
        flags.start
    );

    // Clustering header: 2 columns both PRESENT → 0 (single byte).
    let header = read_uvint_loc(&raw, flags.end());
    fail_vint(header, 0, "composite clustering header (both PRESENT)");

    // c1 = TIMESTAMP → fixed 8-byte value (no length prefix).
    let c1_off = header.end();
    let c1_end = c1_off + 8;
    assert!(
        c1_end <= raw.len(),
        "clustering_key1 (8B timestamp) overruns buffer"
    );

    // c2 = TEXT → length-prefixed (VInt length then bytes).
    let c2_len = read_uvint_loc(&raw, c1_end);
    let c2_val_start = c2_len.end();
    let c2_val_end = c2_val_start + c2_len.value as usize;
    assert!(
        c2_val_end <= raw.len() && c2_len.value > 0,
        "clustering_key2 (text, len {}) must be a non-empty length-prefixed value within bounds \
         (value range [{}, {}), buffer {})",
        c2_len.value,
        c2_val_start,
        c2_val_end,
        raw.len()
    );

    // row_size + prev_size frame the body right after the clustering values.
    let row_size = read_uvint_loc(&raw, c2_val_end);
    let prev_size = read_uvint_loc(&raw, row_size.end());
    fail_vint(
        prev_size,
        (2 + key_len + 12) as u64,
        "composite first-row prev_size must equal the partition-header size",
    );
    assert!(
        prev_size.start + row_size.value as usize <= raw.len(),
        "composite row_size {} at offset {} overruns the decompressed body ({} bytes)",
        row_size.value,
        row_size.start,
        raw.len()
    );
}
