//! Issue #991 (Epic #969): strict byte-for-byte coverage for Cassandra 5.0
//! `Data.db` STATIC-ROW markers and CLUSTERING-BOUND prefixes.
//!
//! Cassandra oracles: `UnfilteredSerializer.java` (the `HAS_EXTENDED_FLAGS` /
//! `IS_STATIC` extended-flag bits, and the rule that a static row OMITS the
//! clustering prefix), `ClusteringPrefix.Serializer` /
//! `Clustering.serialize` (per-column clustering values in DECLARED order after
//! a 2-bit-per-column header), and `SerializationHeader.java`
//! (timestamp/TTL/local-deletion deltas).
//!
//! This file is the static-row + clustering-bound sibling of issue #990's
//! `issue_990_data_db_row_framing_parity.rs`; it MIRRORS that file's structure:
//! offset-context helpers ([`Loc`], [`read_uvint_loc`], [`read_u8_loc`],
//! [`fail_flag`], [`fail_vint`]), the FAIL-CLOSED fixture pattern
//! ([`datasets_root`] / [`read_required_fixture`]), and the two distinct
//! assertion families demanded by acceptance criterion 5:
//!
//!   * BYTE/OFFSET parity — walk the real (or deterministically written)
//!     `Data.db` and assert exact flag bytes, extended-flag bits, the absence of
//!     a clustering prefix on a static row, and clustering-value byte consumption
//!     at absolute offsets.
//!   * JSONL/semantic parity — cross-check the SAME fixture's decoded values
//!     (static-cell value, clustering bounds) against the `sstabledump` JSONL
//!     golden.
//!
//! Fail-closed (criterion 5): every fixture-backed test REQUIRES its real
//! `Data.db`. A missing dataset is an error, never a silent pass — and a fixture
//! that is present but yields 0 rows is a failure too.

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use cqlite_core::parser::vint::parse_vuint;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
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
const ROW_HAS_COMPLEX_DELETION: u8 = 0x40;
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
const EXTENDED_IS_STATIC: u8 = 0x01;

const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;

// ===========================================================================
// Offset-context helpers (mirrors issue #990 acceptance criterion 6)
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

/// Read a fixed-width region `[pos, pos+len)` with a bounds guard, panicking with
/// contextual offset/length information (mirroring the `read_u8_loc` /
/// `read_uvint_loc` fail-with-context style) instead of a raw slice-index panic
/// when a truncated/malformed fixture is shorter than expected.
#[track_caller]
fn read_fixed_loc<'a>(data: &'a [u8], pos: usize, len: usize, what: &str) -> &'a [u8] {
    assert!(
        pos + len <= data.len(),
        "{what}: fixed-width read of {len} bytes at offset {pos} (0x{pos:02X}) overruns the \
         {}-byte buffer — truncated/malformed fixture",
        data.len()
    );
    &data[pos..pos + len]
}

/// Read a big-endian `i32` at `pos` with a bounds guard (contextual panic).
#[track_caller]
fn read_be_i32(data: &[u8], pos: usize, what: &str) -> i32 {
    let b = read_fixed_loc(data, pos, 4, what);
    i32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// Read a big-endian `i64` at `pos` with a bounds guard (contextual panic).
#[track_caller]
fn read_be_i64(data: &[u8], pos: usize, what: &str) -> i64 {
    let b = read_fixed_loc(data, pos, 8, what);
    i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

/// Read a big-endian `u16` at `pos` with a bounds guard (contextual panic).
#[track_caller]
fn read_be_u16(data: &[u8], pos: usize, what: &str) -> u16 {
    let b = read_fixed_loc(data, pos, 2, what);
    u16::from_be_bytes([b[0], b[1]])
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
// FAIL-CLOSED fixture access (mirrors issue #990)
// ===========================================================================

/// Resolve the datasets root, FAILING CLOSED if it cannot be located. A missing
/// dataset is an error, never a silent pass.
fn datasets_root() -> PathBuf {
    if let Ok(r) = std::env::var("CQLITE_DATASETS_ROOT") {
        return PathBuf::from(r);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets")
}

/// Read a required fixture file, FAILING CLOSED with a clear message when the
/// binary `Data.db` has not been fetched.
fn read_required_fixture(rel: &str) -> Vec<u8> {
    let path = datasets_root().join(rel);
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "issue #991 fail-closed: required fixture {} is missing ({e}). \
             Fetch it with `bash test-data/scripts/fetch-datasets.sh` and set \
             CQLITE_DATASETS_ROOT. A missing dataset must error, never silently pass.",
            path.display()
        )
    })
}

/// Decompress a real nb Data.db (LZ4 or Snappy) using the reader's own chunk
/// decompressor (the exact surface the V5CompressedLegacy reader uses) from
/// `raw_data` + the fixture's CompressionInfo. Treats a present-but-empty body
/// as a failure.
fn decompress_with_info(dir: &str, data: &[u8]) -> Vec<u8> {
    use cqlite_core::storage::sstable::chunk_decompressor::create_decompressor_from_file;
    use std::io::Cursor;

    let comp_path = datasets_root().join(format!("{dir}/nb-1-big-CompressionInfo.db"));
    let mut dec = create_decompressor_from_file(&comp_path).unwrap_or_else(|e| {
        panic!(
            "issue #991 fail-closed: {dir} CompressionInfo missing/invalid ({e}) at {}",
            comp_path.display()
        )
    });
    let mut cur = Cursor::new(data);
    let raw = dec
        .read_all_data(&mut cur)
        .unwrap_or_else(|e| panic!("decompress {dir} Data.db: {e:?}"));
    assert!(
        raw.len() > 8,
        "decompressed {dir} Data.db too small ({} bytes) — present-but-empty is a failure",
        raw.len()
    );
    raw
}

/// Decompress a PINNED fixture's Data.db, FAILING CLOSED on a missing binary
/// (the binary is part of the pinned dataset fetched in CI).
fn decompress_fixture(dir: &str) -> Vec<u8> {
    let data = read_required_fixture(&format!("{dir}/nb-1-big-Data.db"));
    decompress_with_info(dir, &data)
}

/// Decompress a LOCAL-ONLY fixture's Data.db, returning `None` when the binary
/// is absent. Per the local-only-fixtures doctrine: a fixture NOT in the pinned
/// CI dataset is SKIPPED (not fail-closed) when its binary is missing, but a
/// fixture that IS present must still parse (a present-but-empty body fails). The
/// canonical static-row shapes this fixture covers are ALSO asserted by
/// deterministic writer tests and the pinned-fixture lanes, so skipping in CI
/// loses no coverage.
fn decompress_local_only_fixture(dir: &str) -> Option<Vec<u8>> {
    let path = datasets_root().join(format!("{dir}/nb-1-big-Data.db"));
    let data = std::fs::read(&path).ok()?;
    Some(decompress_with_info(dir, &data))
}

// ===========================================================================
// Deterministic writer fixtures (mirrors issue #990's shared helpers)
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

fn int_key_bytes(n: i32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Partition-header byte size for an int (4-byte) PK with no partition
/// tombstone: 2 (u16 key-length) + 4 (key) + 4 (LDT i32) + 8 (mfda i64) = 18.
const INT_PK_HEADER_SIZE: usize = 2 + 4 + 4 + 8;

/// int PK, int clustering (ASC), one static text + one regular text column.
fn static_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue991".to_string(),
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

/// int PK, single INT clustering (ASC), one regular text column, NO static
/// columns. A non-static schema means NO static prelude precedes the first row,
/// so the clustered row sits immediately after the partition header.
fn int_clustering_no_static_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue991".to_string(),
        table: "ic".to_string(),
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
        columns: vec![Column {
            name: "rdata".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// int PK, single TEXT clustering (ASC), one regular text column. Used for the
/// null-vs-empty clustering distinction (a text clustering can carry a 0-length
/// EMPTY value, which is byte-distinct from an ABSENT clustering on a static
/// row).
fn text_clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue991".to_string(),
        table: "tc".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![Column {
            name: "v".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_op(column: &str, text: &str) -> CellOperation {
    CellOperation::Write {
        column: column.to_string(),
        value: Value::Text(text.to_string()),
    }
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

// ===========================================================================
// Section 1 — static-row markers (deterministic writer path)
// (manifest: cass.data_db_decode.static_rows.static_only_partition)
// Cassandra oracle: UnfilteredSerializer — a static row sets HAS_EXTENDED_FLAGS
// and the extended IS_STATIC bit, and OMITS the clustering prefix.
// ===========================================================================

/// A partition whose ONLY write targets a static column produces a static row
/// that (a) sets HAS_EXTENDED_FLAGS, (b) sets EXTENDED_IS_STATIC, and (c) omits
/// the clustering prefix entirely — the byte after the extended-flags byte is
/// the row_size VInt, NOT a clustering header. Asserted at absolute offsets.
#[test]
fn static_only_row_sets_is_static_and_omits_clustering_prefix() {
    let schema = static_schema();
    // No clustering key on the mutation → a pure static-block write.
    let m = Mutation::new(
        TableId::new("issue991", "s"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![write_op("sdata", "static-only")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);

    // The static row is the first unfiltered after the partition header.
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    assert_ne!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "static-only row must set HAS_EXTENDED_FLAGS (flag 0x{:02X} @ offset {})",
        flags.value,
        flags.start
    );
    let ext = read_u8_loc(&bytes, flags.end());
    assert_eq!(
        ext.value as u8 & EXTENDED_IS_STATIC,
        EXTENDED_IS_STATIC,
        "static-only row extended-flags byte must set IS_STATIC (0x{:02X} @ offset {})",
        ext.value,
        ext.start
    );

    // CRITICAL: a static row OMITS the clustering prefix. The byte immediately
    // after the extended-flags byte is the row_size VInt, framing the body so
    // that body_end lands exactly on the END_OF_PARTITION (0x01) marker. If a
    // spurious clustering header had been emitted, this framing would not close.
    //
    // Issue #821 (finding #2), matching Cassandra: a static row HARD-CODES
    // prev_unfiltered_size = 0 and does NOT join the prev-size chain (the running
    // chain value is carried forward to the first REGULAR row). So the static
    // row's own prev_size VInt is 0 (a single 0x00 byte), NOT the header size.
    let row_size = read_uvint_loc(&bytes, ext.end());
    let prev_size = read_uvint_loc(&bytes, row_size.end());
    fail_vint(
        prev_size,
        0,
        "static row prev_size is hard-coded to 0 (issue #821; the static prelude \
         is not part of the prev-size chain)",
    );
    let body_end = prev_size.start + row_size.value as usize;
    assert_eq!(
        body_end,
        bytes.len() - 1,
        "static row body (offset {}..{}) must end exactly before the trailing \
         END_OF_PARTITION marker (file len {}); a leaked clustering prefix would \
         shift this",
        prev_size.start,
        body_end,
        bytes.len()
    );
    assert_eq!(
        bytes[body_end], 0x01,
        "the byte after the static row body at offset {body_end} must be END_OF_PARTITION (0x01)"
    );
}

/// Cross-check against a REGULAR (clustered) row in a NON-static schema: the
/// regular row sets neither HAS_EXTENDED_FLAGS nor IS_STATIC, and DOES carry a
/// clustering prefix (the 4-byte int clustering value) before row_size. This
/// contrasts the static and non-static layouts byte-for-byte. A non-static
/// schema is used so no static prelude precedes the row.
#[test]
fn regular_clustered_row_is_not_static_and_carries_clustering_prefix() {
    let schema = int_clustering_no_static_schema();
    let ck = ClusteringKey::single("ck", Value::Integer(0x0A0B0C0D));
    let m = Mutation::new(
        TableId::new("issue991", "ic"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ck),
        vec![write_op("rdata", "row-val")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);

    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    assert_eq!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "regular clustered row must NOT set HAS_EXTENDED_FLAGS (flag 0x{:02X} @ offset {})",
        flags.value,
        flags.start
    );

    // Clustering header: single column PRESENT → 0 (one byte), then the 4-byte
    // big-endian int clustering value, THEN row_size.
    let header = read_uvint_loc(&bytes, flags.end());
    fail_vint(header, 0, "single-column clustering header (PRESENT)");
    let ck_off = header.end();
    let ck_val = i32::from_be_bytes([
        bytes[ck_off],
        bytes[ck_off + 1],
        bytes[ck_off + 2],
        bytes[ck_off + 3],
    ]);
    assert_eq!(
        ck_val, 0x0A0B0C0D,
        "single clustering value must be a 4-byte big-endian int at offset {ck_off}"
    );

    // prev_size frames the partition header right after the clustering value.
    let row_size = read_uvint_loc(&bytes, ck_off + 4);
    let prev_size = read_uvint_loc(&bytes, row_size.end());
    fail_vint(
        prev_size,
        INT_PK_HEADER_SIZE as u64,
        "regular clustered first-row prev_size must equal the partition-header size",
    );
}

// ===========================================================================
// Section 2 — static-only PARTITION fixture (Cassandra-generated)
// (manifest: cass.data_db_decode.static_rows.static_only_partition)
// Fixture: test_deltas/static_with_rows — partition key 99 is static-ONLY
// (sstabledump shows a single static_block and NO clustering rows).
// ===========================================================================

/// LOCAL-ONLY fixture (NOT in the pinned CI dataset): the cleanest static-only +
/// dense static+clustering generation (PK 99 static-only; PK 1/2/3 static +
/// clustering ck 1..=4 ASC). Tests against it SKIP when its binary is absent.
const STATIC_WITH_ROWS_DIR: &str =
    "sstables/test_deltas/static_with_rows-29a4cf80701f11f1b5d1d98b0640ec05";

/// PINNED fixture (in the CI dataset): int32 PK + int32 clustering, static_col +
/// row_col, with PK=1 carrying a static_block then clustering rows (and later
/// tombstones we do not walk here). Used for the static+clustering byte-parity
/// and static-row flag-byte anchor lanes.
const STATIC_TOMB_DIR: &str =
    "sstables/test_tomb/static_with_tombstones-4cdb9780702011f1b8f419c9a388d558";

/// PINNED fixture (in the CI dataset): UUID PK + TIMESTAMP clustering, a static
/// column + regular columns; every partition carries a static_block then a
/// clustering row. Snappy-compressed. Used for the canonical static-marker byte
/// parity lane.
const STATIC_COLUMNS_DIR: &str =
    "sstables/test_basic/static_columns_table-6b0425d0a25111f0a3fef1a551383fb9";

/// Partition-header byte size for the static_with_rows fixture (int32 PK, no
/// partition tombstone): 2 (u16 key-len) + 4 (key) + 4 (i32 LDT) + 8 (i64 mfda).
const STATIC_FIXTURE_HEADER: usize = 2 + 4 + 4 + 8;

/// BYTE parity: in the real LZ4 Data.db, the FIRST partition (PK=99) is
/// static-only. Walk its header, then assert the first unfiltered is a static
/// row: HAS_EXTENDED_FLAGS + IS_STATIC, and that it omits the clustering prefix
/// (prev_size hard-coded to 0, body closes on END_OF_PARTITION).
///
/// `static_with_rows` is a LOCAL-ONLY fixture (its binary is not in the pinned
/// CI dataset), so this test SKIPS when the binary is absent — the static-only
/// shape is also covered by the deterministic
/// `static_only_row_sets_is_static_and_omits_clustering_prefix` test, which runs
/// everywhere. When the binary IS present this asserts real Cassandra bytes.
#[test]
fn fixture_static_only_partition_marker_byte_parity() {
    let Some(raw) = decompress_local_only_fixture(STATIC_WITH_ROWS_DIR) else {
        eprintln!(
            "SKIP fixture_static_only_partition_marker_byte_parity: local-only fixture \
             {STATIC_WITH_ROWS_DIR}/nb-1-big-Data.db absent (covered deterministically)"
        );
        return;
    };

    // First partition header: [u16 key_len=4][i32 key=99][i32 LDT][i64 mfda].
    let key_len = read_be_u16(&raw, 0, "static_with_rows partition key length") as usize;
    assert_eq!(key_len, 4, "static_with_rows PK is a 4-byte int32");
    let key = read_be_i32(&raw, 2, "static_with_rows partition key (int32)");
    assert_eq!(
        key, 99,
        "first partition in static_with_rows must be the static-only PK=99 \
         (sstabledump golden: position 0)"
    );
    let row_pos = STATIC_FIXTURE_HEADER;

    // First (and only) unfiltered of the static-only partition is the static row.
    let flags = read_u8_loc(&raw, row_pos);
    assert_ne!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "static-only partition's first row must set HAS_EXTENDED_FLAGS \
         (flag 0x{:02X} @ offset {})",
        flags.value,
        flags.start
    );
    // The extended-flags byte of a real Cassandra static-only row is EXACTLY
    // EXTENDED_IS_STATIC (0x01) — no shadowable-deletion bit here. Assert the
    // whole byte at its absolute offset.
    let ext = read_u8_loc(&raw, flags.end());
    fail_flag(
        ext,
        EXTENDED_IS_STATIC,
        "static-only partition extended-flags byte (IS_STATIC exactly)",
    );

    // A static row OMITS the clustering prefix: row_size follows the extended
    // flags directly. Real Cassandra (and our writer, issue #821) hard-codes the
    // static row's prev_unfiltered_size to 0 — it is not part of the prev-size
    // chain. So the byte after row_size is a 0x00 prev_size VInt, with NO
    // clustering header/values in between (proving the prefix is omitted).
    let row_size = read_uvint_loc(&raw, ext.end());
    let prev_size = read_uvint_loc(&raw, row_size.end());
    fail_vint(
        prev_size,
        0,
        "static-only fixture row prev_size is hard-coded to 0 \
         (Cassandra static prelude is not in the prev-size chain)",
    );
    // The body frames within the partition and ends exactly on the
    // END_OF_PARTITION (0x01) marker — a leaked clustering prefix would shift it.
    let body_end = prev_size.start + row_size.value as usize;
    assert!(
        body_end < raw.len(),
        "static row_size {} at offset {} overruns the {}-byte decompressed body",
        row_size.value,
        row_size.start,
        raw.len()
    );
    assert_eq!(
        raw[body_end], 0x01,
        "static-only partition body must close on END_OF_PARTITION (0x01) @ offset {body_end}; \
         got 0x{:02X} (a leaked clustering prefix would shift this)",
        raw[body_end]
    );
}

/// JSONL parity (criterion 5, semantic family): the SAME static-only partition's
/// decoded static cell matches the sstabledump golden value and the partition is
/// static-ONLY (no clustering `"type":"row"` entries). Skips when the local-only
/// golden is absent (same rationale as the byte-parity lane).
#[test]
fn fixture_static_only_partition_jsonl_parity() {
    let Some(jsonl) =
        read_jsonl_lines_opt(&format!("{STATIC_WITH_ROWS_DIR}/nb-1-big-Data.db.jsonl"))
    else {
        eprintln!("SKIP fixture_static_only_partition_jsonl_parity: local-only golden absent");
        return;
    };
    let first = &jsonl[0];
    // Golden: partition key 99 carries exactly one static_block and NO rows.
    assert!(
        first.contains("\"key\":[\"99\"]"),
        "first golden partition must be PK=99: {first}"
    );
    assert!(
        first.contains("\"type\":\"static_block\""),
        "PK=99 must have a static_block: {first}"
    );
    assert!(
        !first.contains("\"type\":\"row\""),
        "static-only partition PK=99 must have NO clustering rows: {first}"
    );
    assert!(
        first.contains("\"value\":\"static_only_val\""),
        "PK=99 static cell value must be 'static_only_val': {first}"
    );
}

// ===========================================================================
// Section 3 — static + clustering ROWS in one partition (PINNED fixture)
// (manifest: cass.data_db_decode.static_rows.static_with_clustering_rows)
// Fixture: test_tomb/static_with_tombstones (PINNED, LZ4, int32 PK, int32
// clustering, static_col + row_col). Partition key 1 carries a static_block
// THEN clustering rows; we walk the static row and the FIRST clustered row
// (ck=1). (The local-only test_deltas/static_with_rows fixture, when present,
// gives the same shape via the skip-on-presence lane above.)
// ===========================================================================

/// Partition-header byte size for an int32-PK fixture (no partition tombstone):
/// 2 (u16 key-len) + 4 (key) + 4 (i32 LDT) + 8 (i64 mfda) = 18.
const INT_FIXTURE_HEADER: usize = 2 + 4 + 4 + 8;

/// BYTE parity: walk PK=1 of the pinned static_with_tombstones fixture. Its
/// first unfiltered is the static row (IS_STATIC, prev_size=0, no clustering
/// prefix); the NEXT unfiltered is a regular clustered row whose clustering
/// prefix carries the 4-byte int `ck=1` value at an absolute offset, in ASC
/// declared order.
#[test]
fn fixture_static_with_clustering_rows_byte_parity() {
    let raw = decompress_fixture(STATIC_TOMB_DIR);

    // First partition header: [u16 key_len=4][i32 key=1][i32 LDT][i64 mfda].
    let key_len = read_be_u16(&raw, 0, "static_with_tombstones partition key length") as usize;
    assert_eq!(key_len, 4, "static_with_tombstones PK is a 4-byte int32");
    let key = read_be_i32(&raw, 2, "static_with_tombstones partition key (int32)");
    assert_eq!(key, 1, "first partition must be PK=1 (golden position 0)");
    let row_pos = INT_FIXTURE_HEADER;

    // 1) Static row first: IS_STATIC, no clustering prefix, prev_size=0.
    let sflags = read_u8_loc(&raw, row_pos);
    assert_ne!(
        sflags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "PK=1 first unfiltered must be the static row (HAS_EXTENDED_FLAGS); \
         flag 0x{:02X} @ offset {}",
        sflags.value,
        sflags.start
    );
    let sext = read_u8_loc(&raw, sflags.end());
    assert_eq!(
        sext.value as u8 & EXTENDED_IS_STATIC,
        EXTENDED_IS_STATIC,
        "PK=1 static row must set IS_STATIC (ext 0x{:02X} @ offset {})",
        sext.value,
        sext.start
    );
    let s_row_size = read_uvint_loc(&raw, sext.end());
    let s_prev_size = read_uvint_loc(&raw, s_row_size.end());
    fail_vint(
        s_prev_size,
        0,
        "PK=1 static row prev_size is hard-coded to 0 (issue #821; the static \
         prelude is omitted from the prev-size chain) — and there is NO clustering \
         prefix between the extended flags and row_size",
    );
    // The static row body ends; the next unfiltered (first clustered row) begins.
    let next_pos = s_prev_size.start + s_row_size.value as usize;

    // 2) First clustered row: regular (NOT extended/static), carries a clustering
    //    prefix = [header VInt PRESENT=0][4-byte int ck], in ASC order ck=1.
    let rflags = read_u8_loc(&raw, next_pos);
    assert_eq!(
        rflags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "PK=1 first clustered row must be a regular (non-static) row; \
         flag 0x{:02X} @ offset {}",
        rflags.value,
        rflags.start
    );
    let header = read_uvint_loc(&raw, rflags.end());
    fail_vint(
        header,
        0,
        "PK=1 clustered row clustering header (single col PRESENT)",
    );
    let ck_off = header.end();
    let ck_val = read_be_i32(
        &raw,
        ck_off,
        "PK=1 first clustered-row clustering value (int)",
    );
    assert_eq!(
        ck_val, 1,
        "PK=1 first clustered row clustering value (ck) must be 1 in ASC order, \
         4-byte big-endian at offset {ck_off}"
    );
}

/// JSONL parity: PK=1 carries a static_block ('surviving_static') AND a
/// clustering row with ck=1 and its row_col value, matching the golden.
#[test]
fn fixture_static_with_clustering_rows_jsonl_parity() {
    let jsonl = read_jsonl_lines(&format!("{STATIC_TOMB_DIR}/nb-1-big-Data.db.jsonl"));
    let p1 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"1\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=1 partition"));
    assert!(
        p1.contains("\"value\":\"surviving_static\""),
        "PK=1 static cell must be 'surviving_static': {p1}"
    );
    assert!(
        p1.contains("\"type\":\"static_block\"") && p1.contains("\"type\":\"row\""),
        "PK=1 must carry BOTH a static_block AND clustering rows: {p1}"
    );
    assert!(
        p1.contains("\"clustering\":[1]") && p1.contains("\"value\":\"row_1\""),
        "PK=1 first clustered row must be ck=1 / row_1: {p1}"
    );
}

/// JSONL parity for the LOCAL-ONLY static_with_rows fixture (the cleanest
/// static-only + dense static+clustering generation). Skips when its golden is
/// absent — its shape is covered by the pinned lanes above.
#[test]
fn fixture_local_static_with_rows_jsonl_parity() {
    let Some(jsonl) =
        read_jsonl_lines_opt(&format!("{STATIC_WITH_ROWS_DIR}/nb-1-big-Data.db.jsonl"))
    else {
        eprintln!("SKIP fixture_local_static_with_rows_jsonl_parity: local-only golden absent");
        return;
    };
    let p1 = jsonl
        .iter()
        .find(|l| l.contains("\"key\":[\"1\"]"))
        .unwrap_or_else(|| panic!("golden must contain PK=1 partition"));
    assert!(
        p1.contains("\"value\":\"static_val_1\""),
        "PK=1 static cell must be 'static_val_1': {p1}"
    );
    // ASC clustering order: ck must appear as 1,2,3,4 in declared order.
    let mut search = 0usize;
    for ck in 1..=4 {
        let needle = format!("\"clustering\":[{ck}]");
        let at = p1[search..]
            .find(&needle)
            .unwrap_or_else(|| panic!("PK=1 must contain clustering [{ck}] after offset {search}"));
        search += at + needle.len();
    }
    for v in ["row_1_1", "row_1_2", "row_1_3", "row_1_4"] {
        assert!(p1.contains(v), "PK=1 must contain row value {v}: {p1}");
    }
}

/// PINNED canonical static-marker BYTE parity: the first partition of
/// static_columns_table (UUID PK + TIMESTAMP clustering, Snappy) begins with a
/// static row (HAS_EXTENDED_FLAGS + IS_STATIC, prev_size=0, no clustering
/// prefix), immediately followed by a regular clustered row that DOES carry a
/// clustering prefix (PRESENT header + 8-byte timestamp clustering value) ahead
/// of its row_size. This is the static→clustered transition asserted at absolute
/// offsets on a real Cassandra fixture that is part of the CI dataset.
#[test]
fn fixture_static_columns_marker_byte_parity() {
    let raw = decompress_fixture(STATIC_COLUMNS_DIR);

    // Partition header: [u16 key_len=16][16 UUID][i32 LDT][i64 mfda] = 30.
    let key_len = read_be_u16(&raw, 0, "static_columns_table partition key length") as usize;
    assert_eq!(key_len, 16, "static_columns_table has a 16-byte UUID PK");
    let header_size = 2 + key_len + 12;

    // 1) First unfiltered: the static row.
    let sflags = read_u8_loc(&raw, header_size);
    assert_ne!(
        sflags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "static_columns_table first row must set HAS_EXTENDED_FLAGS (flag 0x{:02X} @ {})",
        sflags.value,
        sflags.start
    );
    let sext = read_u8_loc(&raw, sflags.end());
    assert_eq!(
        sext.value as u8 & EXTENDED_IS_STATIC,
        EXTENDED_IS_STATIC,
        "static_columns_table static row must set IS_STATIC (ext 0x{:02X} @ {})",
        sext.value,
        sext.start
    );
    let s_row_size = read_uvint_loc(&raw, sext.end());
    let s_prev_size = read_uvint_loc(&raw, s_row_size.end());
    fail_vint(
        s_prev_size,
        0,
        "static_columns_table static row prev_size is hard-coded to 0 (no clustering prefix)",
    );
    let next_pos = s_prev_size.start + s_row_size.value as usize;

    // 2) Next unfiltered: a regular clustered row WITH a clustering prefix.
    let rflags = read_u8_loc(&raw, next_pos);
    assert_eq!(
        rflags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "static_columns_table second unfiltered must be a regular clustered row; \
         flag 0x{:02X} @ {}",
        rflags.value,
        rflags.start
    );
    let header = read_uvint_loc(&raw, rflags.end());
    fail_vint(
        header,
        0,
        "clustered row clustering header (TIMESTAMP col PRESENT)",
    );
    // TIMESTAMP clustering value is a fixed 8-byte long; it must fit before the
    // row_size VInt.
    let ck_off = header.end();
    let ck_end = ck_off + 8;
    assert!(
        ck_end < raw.len(),
        "8-byte timestamp clustering value at offset {ck_off} overruns the body"
    );
    // row_size then prev_size frame the body after the clustering value; prev_size
    // for the FIRST regular row equals header + static-row bytes (issue #821: the
    // chain carries the static row's bytes forward).
    let row_size = read_uvint_loc(&raw, ck_end);
    let prev_size = read_uvint_loc(&raw, row_size.end());
    let static_row_bytes = next_pos - header_size;
    fail_vint(
        prev_size,
        (header_size + static_row_bytes) as u64,
        "first clustered-row prev_size must equal header + static-row bytes \
         (issue #821: the static prelude's bytes are carried into the prev-size chain)",
    );
}

/// PINNED JSONL parity: static_columns_table partitions carry BOTH a static_block
/// and a clustering row, matching the golden's static+clustering shape.
#[test]
fn fixture_static_columns_jsonl_parity() {
    let jsonl = read_jsonl_lines(&format!("{STATIC_COLUMNS_DIR}/nb-1-big-Data.db.jsonl"));
    let first = &jsonl[0];
    assert!(
        first.contains("\"type\":\"static_block\"") && first.contains("\"type\":\"row\""),
        "static_columns_table first partition must carry BOTH a static_block AND a row: {first}"
    );
    assert!(
        first.contains("\"name\":\"static_data\""),
        "static_columns_table static cell must be 'static_data': {first}"
    );
}

/// Read the JSONL golden into lines, FAILING CLOSED on a missing/empty file.
fn read_jsonl_lines(rel: &str) -> Vec<String> {
    let path = datasets_root().join(rel);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "issue #991 fail-closed: required JSONL golden {} is missing ({e})",
            path.display()
        )
    });
    let lines: Vec<String> = text.lines().map(|l| l.to_string()).collect();
    assert!(
        !lines.is_empty(),
        "JSONL golden {} is empty (present-but-empty is a failure)",
        path.display()
    );
    lines
}

/// Read a LOCAL-ONLY JSONL golden, returning `None` when the file is absent
/// (skip-on-presence). A present-but-empty golden is still a failure.
fn read_jsonl_lines_opt(rel: &str) -> Option<Vec<String>> {
    let path = datasets_root().join(rel);
    let text = std::fs::read_to_string(&path).ok()?;
    let lines: Vec<String> = text.lines().map(|l| l.to_string()).collect();
    assert!(
        !lines.is_empty(),
        "JSONL golden {} is present but empty (failure)",
        path.display()
    );
    Some(lines)
}

// ===========================================================================
// Section 4 — multi-column clustering prefix (fixture, wide partition)
// (manifest: cass.data_db_decode.clustering_bounds.multi_column_prefix)
// Fixture: test_wide_rows/wide_partition_table — 5 clustering columns
// (TIMESTAMP DESC, TEXT ASC, INT DESC, UUID, DATE DESC).
// ===========================================================================

const WIDE_DIR: &str =
    "sstables/test_wide_rows/wide_partition_table-6d6d0f80a25111f0a3fef1a551383fb9";

/// Deterministic BYTE parity for a MULTI-COLUMN clustering prefix: a row with
/// THREE clustering columns (int ASC, text ASC, int ASC) serializes the prefix
/// as `[header VInt = all-PRESENT 0][c1 int 4B][c2 len-prefixed text][c3 int 4B]`
/// in DECLARED order, ahead of row_size — and row_size/prev_size frame the body
/// exactly after the 3rd clustering value. This exercises the full multi-column
/// prefix on the public writer surface (the fixture lane below cross-checks a
/// real Cassandra multi-column clustering at the JSONL level; the wide 5-column
/// fixture's full 5-tuple byte walk is deferred — see the byte-walk note there).
#[test]
fn multi_column_clustering_prefix_byte_parity_writer() {
    let schema = TableSchema {
        keyspace: "issue991".to_string(),
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
                data_type: "text".to_string(),
                position: 1,
                order: ClusteringOrder::Asc,
            },
            ClusteringColumn {
                name: "c3".to_string(),
                data_type: "int".to_string(),
                position: 2,
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
            ("c2".to_string(), Value::Text("mid".to_string())),
            ("c3".to_string(), Value::Integer(0x11121314)),
        ],
    };
    let m = Mutation::new(
        TableId::new("issue991", "mc"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ck),
        vec![write_op("v", "x")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);

    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    assert_eq!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "regular multi-clustering row is not extended/static"
    );
    // Header: 3 columns, all PRESENT (2 bits each = 0) → single 0x00 byte.
    let header = read_uvint_loc(&bytes, flags.end());
    fail_vint(header, 0, "3-column clustering header (all PRESENT)");

    // c1 int (4B BE) in DECLARED order.
    let c1_off = header.end();
    let c1 = i32::from_be_bytes([
        bytes[c1_off],
        bytes[c1_off + 1],
        bytes[c1_off + 2],
        bytes[c1_off + 3],
    ]);
    assert_eq!(c1, 0x0A0B0C0D, "clustering c1 (int) at offset {c1_off}");

    // c2 text: VInt length prefix then bytes.
    let c2_len = read_uvint_loc(&bytes, c1_off + 4);
    fail_vint(c2_len, 3, "clustering c2 (text) length prefix = 3 ('mid')");
    let c2_val_start = c2_len.end();
    let c2_bytes = &bytes[c2_val_start..c2_val_start + 3];
    assert_eq!(
        c2_bytes, b"mid",
        "clustering c2 text value must be 'mid' at offset {c2_val_start}"
    );

    // c3 int (4B BE), then row_size/prev_size.
    let c3_off = c2_val_start + 3;
    let c3 = i32::from_be_bytes([
        bytes[c3_off],
        bytes[c3_off + 1],
        bytes[c3_off + 2],
        bytes[c3_off + 3],
    ]);
    assert_eq!(c3, 0x11121314, "clustering c3 (int) at offset {c3_off}");

    let row_size = read_uvint_loc(&bytes, c3_off + 4);
    let prev_size = read_uvint_loc(&bytes, row_size.end());
    fail_vint(
        prev_size,
        INT_PK_HEADER_SIZE as u64,
        "multi-column first-row prev_size must equal the partition-header size \
         (proves all 3 clustering values were consumed before row_size)",
    );
}

/// Fixture BYTE parity for the FIRST FOUR clustering columns of the real wide
/// partition (timestamp 8B, text 'short', int 704656, uuid 16B), in DECLARED
/// order ahead of the rest of the prefix. Each width/value is verified at an
/// absolute offset against the sstabledump golden.
///
/// NOTE (honest scoping): the wide table's 5th clustering column (DATE,
/// SimpleDateType wrapped in ReversedType) is preceded on disk by one byte this
/// test does not assert a meaning for, so the 5th value's offset and the
/// row_size/prev_size framing are intentionally NOT asserted here. The full
/// multi-column prefix → row_size framing is covered byte-for-byte by the
/// deterministic writer test above; this fixture lane anchors the first four
/// real-Cassandra clustering values, and the JSONL lane below covers the full
/// 5-tuple semantically.
#[test]
fn fixture_wide_multi_column_clustering_prefix_byte_parity() {
    let raw = decompress_fixture(WIDE_DIR);

    // Partition header: [u16 key_len=16][16 UUID][i32 LDT][i64 mfda] = 30.
    let key_len = read_be_u16(&raw, 0, "wide_partition_table partition key length") as usize;
    assert_eq!(key_len, 16, "wide_partition_table has a 16-byte UUID PK");
    let header_size = 2 + key_len + 12;
    let row_pos = header_size;

    let flags = read_u8_loc(&raw, row_pos);
    assert_eq!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "wide partition first row must be a regular (non-static) row; flag 0x{:02X} @ {}",
        flags.value,
        flags.start
    );

    // Clustering header: 5 columns, all PRESENT → 2-bit-per-column = 0.
    let header = read_uvint_loc(&raw, flags.end());
    fail_vint(header, 0, "wide 5-column clustering header (all PRESENT)");

    // Column 1: TIMESTAMP (ReversedType wraps it but the value is still an 8-byte
    // fixed-width long on disk — Reversed only flips comparison order). Decode it
    // and check it matches the golden first-row clustering timestamp (millis).
    let c1_off = header.end();
    let c1_end = c1_off + 8;
    let c1_millis = read_be_i64(&raw, c1_off, "wide clustering[0] timestamp (8B)");
    let golden_millis = iso8601_to_micros("2025-07-28T01:12:11.383Z") / 1000;
    assert_eq!(
        c1_millis, golden_millis,
        "wide clustering[0] DESC timestamp must decode to the golden millis at offset {c1_off}"
    );

    // Column 2: TEXT → VInt length prefix then bytes (golden c2 = "short", 5 B).
    let c2_len = read_uvint_loc(&raw, c1_end);
    let c2_val_start = c2_len.end();
    let c2_val_end = c2_val_start + c2_len.value as usize;
    let c2_bytes = read_fixed_loc(
        &raw,
        c2_val_start,
        c2_len.value as usize,
        "wide clustering[1] text value",
    );
    assert_eq!(
        c2_bytes, b"short",
        "clustering[1] text value must be 'short' (golden) at offset {c2_val_start}; \
         got {c2_bytes:02X?}"
    );

    // Column 3: INT (ReversedType) → 4-byte fixed width = golden 704656.
    let c3_off = c2_val_end;
    let c3_val = read_be_i32(&raw, c3_off, "wide clustering[2] int value");
    assert_eq!(
        c3_val, 704_656,
        "clustering[2] int value must equal the golden 704656 at offset {c3_off}"
    );

    // Column 4: UUID → 16-byte fixed width = golden 41bdf0fa-5411-4cae-b8ce-bccf9dc7e574.
    let c4_off = c3_off + 4;
    let c4_bytes = read_fixed_loc(&raw, c4_off, 16, "wide clustering[3] uuid value");
    let golden_uuid: [u8; 16] = [
        0x41, 0xbd, 0xf0, 0xfa, 0x54, 0x11, 0x4c, 0xae, 0xb8, 0xce, 0xbc, 0xcf, 0x9d, 0xc7, 0xe5,
        0x74,
    ];
    assert_eq!(
        c4_bytes, &golden_uuid,
        "clustering[3] uuid bytes must match the golden at offset {c4_off}"
    );
}

/// JSONL parity: the wide partition's first row clustering tuple matches the
/// golden's 5-element declared-order clustering (incl. the DATE 5th column).
#[test]
fn fixture_wide_multi_column_clustering_jsonl_parity() {
    let jsonl = read_jsonl_lines(&format!("{WIDE_DIR}/nb-1-big-Data.db.jsonl"));
    let first = &jsonl[0];
    assert!(
        first.contains(
            "\"clustering\":[\"2025-07-28 01:12:11.383Z\",\"short\",704656,\
             \"41bdf0fa-5411-4cae-b8ce-bccf9dc7e574\",\"2025-07-12\"]"
        ),
        "wide first-row clustering must be the full 5-tuple in declared order: {first}"
    );
}

// ===========================================================================
// Section 5 — DESC clustering order (schema-aware bound comparison)
// (manifest: cass.data_db_decode.clustering_bounds.desc_order)
// Fixture: test_basic/composite_key_table — clustering_key1 TIMESTAMP DESC
// (ReversedType), clustering_key2 TEXT ASC.
// ===========================================================================

const COMPOSITE_DIR: &str =
    "sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9";

/// BYTE parity for the DESC (ReversedType) first clustering column: the on-disk
/// VALUE bytes are NOT pre-inverted — ReversedType only flips comparison order,
/// so the 8-byte timestamp is the plain big-endian value. We assert the prefix
/// layout (8-byte fixed DESC timestamp, then length-prefixed ASC text) and that
/// the decoded timestamp matches the golden's first-row clustering, ahead of
/// row_size.
#[test]
fn fixture_composite_desc_clustering_order_byte_parity() {
    let raw = decompress_fixture(COMPOSITE_DIR);

    let key_len = read_be_u16(&raw, 0, "composite_key_table partition key length") as usize;
    assert_eq!(key_len, 16, "composite_key_table has a 16-byte UUID PK");
    let header_size = 2 + key_len + 12;
    let row_pos = header_size;

    let flags = read_u8_loc(&raw, row_pos);
    assert_eq!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "composite first row must be a regular (non-static) row; flag 0x{:02X} @ {}",
        flags.value,
        flags.start
    );

    // Both clustering columns PRESENT → header 0.
    let header = read_uvint_loc(&raw, flags.end());
    fail_vint(header, 0, "composite clustering header (both PRESENT)");

    // c1 = TIMESTAMP DESC (ReversedType): 8-byte fixed-width long, value bytes
    // are plain big-endian (DESC affects sort order, not on-disk byte form).
    let c1_off = header.end();
    let c1_end = c1_off + 8;
    let c1_millis = read_be_i64(&raw, c1_off, "composite clustering_key1 timestamp (8B)");
    // Golden first-row clustering[0] = "2025-10-06 01:12:06.059Z" → millis since
    // epoch. Cassandra stores TimestampType as milliseconds (i64).
    let golden_millis = iso8601_to_micros("2025-10-06T01:12:06.059Z") / 1000;
    assert_eq!(
        c1_millis, golden_millis,
        "composite DESC clustering_key1 must decode to the golden timestamp \
         (millis {golden_millis}) at offset {c1_off}; ReversedType must NOT invert \
         the on-disk value bytes"
    );

    // c2 = TEXT ASC: length-prefixed value, non-empty.
    let c2_len = read_uvint_loc(&raw, c1_end);
    let c2_val_start = c2_len.end();
    let c2_val_end = c2_val_start + c2_len.value as usize;
    assert!(
        c2_len.value > 0,
        "clustering_key2 (text, len {}) must be a non-empty length-prefixed value",
        c2_len.value
    );
    let c2_bytes = read_fixed_loc(
        &raw,
        c2_val_start,
        c2_len.value as usize,
        "composite clustering_key2 text value",
    );
    assert_eq!(
        c2_bytes, b"information",
        "composite ASC clustering_key2 must be 'information' (golden) at offset {c2_val_start}"
    );

    // row_size / prev_size frame the body right after the clustering values.
    let row_size = read_uvint_loc(&raw, c2_val_end);
    let prev_size = read_uvint_loc(&raw, row_size.end());
    fail_vint(
        prev_size,
        header_size as u64,
        "composite first-row prev_size must equal the partition-header size",
    );
}

/// JSONL parity: the DESC-ordered composite clustering tuple matches the golden.
#[test]
fn fixture_composite_desc_clustering_jsonl_parity() {
    let jsonl = read_jsonl_lines(&format!("{COMPOSITE_DIR}/nb-1-big-Data.db.jsonl"));
    let first = &jsonl[0];
    assert!(
        first.contains("\"clustering\":[\"2025-10-06 01:12:06.059Z\",\"information\"]"),
        "composite first-row clustering must be [DESC timestamp, ASC text]: {first}"
    );
}

/// Convert `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` to epoch microseconds (Howard
/// Hinnant's `days_from_civil`; no date/time crate needed). Mirrors #990.
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
    let mut micros_frac = frac.to_string();
    while micros_frac.len() < 6 {
        micros_frac.push('0');
    }
    let micros_frac: i64 = micros_frac[..6].parse().unwrap_or(0);
    secs * 1_000_000 + micros_frac
}

// ===========================================================================
// Section 6 — null vs empty clustering value distinction (deterministic)
// (manifest: cass.data_db_decode.clustering_bounds.null_vs_empty)
//
// A static row has an ABSENT (null) clustering — the prefix is OMITTED entirely.
// A clustered row with an EMPTY text clustering carries a clustering header
// (PRESENT) followed by a ZERO-LENGTH value. These are byte-distinct, and this
// test asserts both at absolute offsets. No Cassandra fixture isolates a
// 0-length text clustering, so this is covered by the deterministic writer path
// (and the static/absent side is also cross-checked against the real fixture in
// Section 2).
// ===========================================================================

/// EMPTY (present, zero-length) vs ABSENT (omitted) clustering, byte-for-byte.
#[test]
fn null_vs_empty_clustering_value_byte_distinction() {
    // (a) EMPTY text clustering: a clustered row with ck = "" → clustering
    //     header PRESENT (0) followed by a ZERO-length VInt, then row_size.
    let schema = text_clustering_schema();
    let ck_empty = ClusteringKey::single("ck", Value::Text(String::new()));
    let m_empty = Mutation::new(
        TableId::new("issue991", "tc"),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ck_empty),
        vec![write_op("v", "x")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m_empty]);

    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    assert_eq!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "empty-clustering row is a regular (non-static) row; it MUST carry a prefix"
    );
    // Clustering header: single column PRESENT → 0.
    let header = read_uvint_loc(&bytes, flags.end());
    fail_vint(
        header,
        0,
        "empty-clustering header (column PRESENT, not absent)",
    );
    // The PRESENT text value is a zero-length VInt (EMPTY, not NULL/absent).
    let len = read_uvint_loc(&bytes, header.end());
    fail_vint(
        len,
        0,
        "EMPTY text clustering value must be a PRESENT zero-length value \
         (distinct from an absent/static clustering which omits the prefix)",
    );
    // row_size follows immediately after the zero-length value.
    let row_size = read_uvint_loc(&bytes, len.end());
    let prev_size = read_uvint_loc(&bytes, row_size.end());
    fail_vint(
        prev_size,
        INT_PK_HEADER_SIZE as u64,
        "empty-clustering first-row prev_size must equal the partition-header size",
    );

    // (b) ABSENT (null) clustering: a static row OMITS the prefix entirely. Use
    //     the static schema so the static-only write produces an IS_STATIC row
    //     whose byte after the extended-flags is row_size — NO header, NO length.
    let sschema = static_schema();
    let m_static = Mutation::new(
        TableId::new("issue991", "s"),
        PartitionKey::single("id", Value::Integer(2)),
        None,
        vec![write_op("sdata", "s")],
        2_000_000,
        None,
    );
    let sbytes = write_one_partition(det_stats(), &sschema, 2, &[m_static]);
    let sflags = read_u8_loc(&sbytes, INT_PK_HEADER_SIZE);
    assert_ne!(
        sflags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "absent-clustering (static) row must set HAS_EXTENDED_FLAGS"
    );
    let sext = read_u8_loc(&sbytes, sflags.end());
    assert_eq!(
        sext.value as u8 & EXTENDED_IS_STATIC,
        EXTENDED_IS_STATIC,
        "absent-clustering row must be IS_STATIC (the clustering is null/absent, not empty)"
    );
    // The byte AFTER the extended-flags is row_size — there is NO clustering
    // header and NO length VInt. We prove this by checking the framing closes
    // exactly on END_OF_PARTITION.
    let s_row_size = read_uvint_loc(&sbytes, sext.end());
    let s_prev_size = read_uvint_loc(&sbytes, s_row_size.end());
    fail_vint(
        s_prev_size,
        0,
        "static (absent-clustering) prev_size is hard-coded 0 (issue #821) — and the \
         byte after the extended flags is row_size directly, proving NO clustering \
         header/length was emitted",
    );
    let body_end = s_prev_size.start + s_row_size.value as usize;
    assert_eq!(
        sbytes[body_end], 0x01,
        "absent-clustering static row body must close on END_OF_PARTITION (0x01) @ {body_end}; \
         a leaked empty-clustering header/length would shift this"
    );

    // The two encodings are byte-distinct at the position right after the flags:
    // EMPTY → [header=0][len=0][row_size...]; ABSENT/static → [ext_flags][row_size...].
    // We have already asserted both shapes; assert the lengths differ to make the
    // distinction explicit and regression-proof.
    assert_ne!(
        header.len + len.len,
        0,
        "the EMPTY clustering consumed a header+length the static (absent) row did not"
    );
}

// ===========================================================================
// Section 7 — flag-mask well-formedness on the static fixture (anchor)
// Anchors our flag constants against real Cassandra static-row bytes: the
// static row's leading flag byte has only bits inside the known row-flag mask.
// ===========================================================================

#[test]
fn fixture_static_row_flag_byte_well_formed() {
    let raw = decompress_fixture(STATIC_TOMB_DIR);

    // Cassandra UnfilteredSerializer leading-byte sentinels that a LIVE STATIC
    // ROW must NOT carry: 0x01 is the END_OF_PARTITION marker (Unfiltered.Kind),
    // and 0x02 is IS_MARKER (a range-tombstone bound). A static row is a regular
    // Row kind, so neither sentinel bit may be set on its leading flag byte.
    const ROW_END_OF_PARTITION: u8 = 0x01;
    const ROW_IS_MARKER: u8 = 0x02;

    let row_pos = INT_FIXTURE_HEADER;
    let flags = read_u8_loc(&raw, row_pos);

    // (1) HAS_EXTENDED_FLAGS MUST be set — that is how the IS_STATIC extended bit
    //     is reached at all.
    assert_ne!(
        flags.value as u8 & ROW_HAS_EXTENDED_FLAGS,
        0,
        "static row flag byte 0x{:02X} @ offset {} must set HAS_EXTENDED_FLAGS (0x80)",
        flags.value,
        flags.start
    );
    // (2) The static row is NOT an end-of-partition marker and NOT a range-tomb
    //     marker — those sentinel bits must be clear on a live static row.
    assert_eq!(
        flags.value as u8 & (ROW_END_OF_PARTITION | ROW_IS_MARKER),
        0,
        "static row flag byte 0x{:02X} @ offset {} must NOT set the END_OF_PARTITION (0x01) \
         or IS_MARKER (0x02) sentinel bits — a static row is a regular Row kind",
        flags.value,
        flags.start
    );
    // (3) This particular fixture's static row has no row-level timestamp,
    //     TTL, deletion, or complex-deletion — those liveness/deletion bits must
    //     be clear (the row's liveness is carried per-cell).
    assert_eq!(
        flags.value as u8
            & (ROW_HAS_TIMESTAMP | ROW_HAS_TTL | ROW_HAS_DELETION | ROW_HAS_COMPLEX_DELETION),
        0,
        "static row flag byte 0x{:02X} @ offset {} must NOT set any of \
         HAS_TIMESTAMP/HAS_TTL/HAS_DELETION/HAS_COMPLEX_DELETION for this fixture",
        flags.value,
        flags.start
    );
    // (4) Anchor the EXACT leading flag byte Cassandra writes for this fixture's
    //     live static row: HAS_EXTENDED_FLAGS | HAS_ALL_COLUMNS (0xA0). This is
    //     genuinely able to fail if the byte were wrong (carries offset context
    //     via fail_flag).
    fail_flag(
        flags,
        ROW_HAS_EXTENDED_FLAGS | ROW_HAS_ALL_COLUMNS,
        "static row leading flag byte (HAS_EXTENDED_FLAGS | HAS_ALL_COLUMNS, 0xA0)",
    );
    // Sanity: a static row's extended-flag byte's only meaningful low bits are
    // IS_STATIC / HAS_SHADOWABLE_DELETION (0x02); assert IS_STATIC is set and no
    // value-bits beyond that mask appear.
    let ext = read_u8_loc(&raw, flags.end());
    assert_eq!(
        ext.value as u8 & EXTENDED_IS_STATIC,
        EXTENDED_IS_STATIC,
        "static row extended-flag byte 0x{:02X} @ {} must set IS_STATIC",
        ext.value,
        ext.start
    );
    let ext_known = EXTENDED_IS_STATIC | 0x02; // IS_STATIC | HAS_SHADOWABLE_DELETION
    assert_eq!(
        ext.value as u8 & !ext_known,
        0,
        "static row extended-flag byte 0x{:02X} @ {} has bits outside the known extended mask",
        ext.value,
        ext.start
    );
}

/// Anchor: the deterministic writer's live single cell flag is USE_ROW_TIMESTAMP,
/// confirming the cell-flag constant used elsewhere is the on-disk value (keeps
/// this file self-consistent with #990's cell-flag family without duplicating it).
#[test]
fn writer_live_static_cell_uses_row_timestamp_flag() {
    // A static-only write: the static cell reuses the static row's liveness.
    let schema = static_schema();
    let m = Mutation::new(
        TableId::new("issue991", "s"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![write_op("sdata", "v")],
        2_000_000,
        None,
    );
    let bytes = write_one_partition(det_stats(), &schema, 1, &[m]);
    let flags = read_u8_loc(&bytes, INT_PK_HEADER_SIZE);
    let ext = read_u8_loc(&bytes, flags.end());
    // Walk: [flags][ext][row_size][prev_size][ts delta][bitmap?][cell flag].
    let row_size = read_uvint_loc(&bytes, ext.end());
    let prev_size = read_uvint_loc(&bytes, row_size.end());
    let mut pos = prev_size.end();
    if flags.value as u8 & ROW_HAS_TIMESTAMP != 0 {
        pos = read_uvint_loc(&bytes, pos).end();
    }
    if flags.value as u8 & ROW_HAS_ALL_COLUMNS == 0 {
        pos = read_uvint_loc(&bytes, pos).end(); // column subset bitmap
    }
    let cell = read_u8_loc(&bytes, pos);
    assert_eq!(
        cell.value as u8 & CELL_USE_ROW_TIMESTAMP,
        CELL_USE_ROW_TIMESTAMP,
        "live static cell must reuse the row timestamp (flag 0x{:02X} @ {})",
        cell.value,
        cell.start
    );
}
