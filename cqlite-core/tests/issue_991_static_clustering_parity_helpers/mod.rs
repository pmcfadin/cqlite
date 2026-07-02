//! Shared, non-test helpers + constants for the issue #991 static-row /
//! clustering-bound byte-parity suite.
//!
//! This lives in a SUBDIRECTORY of `tests/`, so it is NOT compiled as its own
//! test binary — it is included into the single
//! `issue_991_static_clustering_parity` test target via `#[path = ...] mod`.
//! Keeping the shared offset-context helpers, fixture-access helpers, writer
//! fixtures, and JSONL golden readers here (with the actual `#[test]` functions
//! in the sibling file) satisfies the file-size ratchet (#1135) without
//! renaming or splitting the test binary.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::parser::vint::parse_vuint;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::data_writer::DataWriter;
use cqlite_core::storage::sstable::writer::stats_writer::StatisticsMetadata;
use cqlite_core::storage::write_engine::mutation::{CellOperation, DecoratedKey, Mutation};
use cqlite_core::types::Value;

// ---------------------------------------------------------------------------
// Row / cell flag bit constants (mirror data_writer/mod.rs and the
// V5CompressedLegacy parser; these are the on-disk bit values Cassandra's
// UnfilteredSerializer / Cell.Serializer write).
// ---------------------------------------------------------------------------
pub const ROW_HAS_TIMESTAMP: u8 = 0x04;
pub const ROW_HAS_TTL: u8 = 0x08;
pub const ROW_HAS_DELETION: u8 = 0x10;
pub const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
pub const ROW_HAS_COMPLEX_DELETION: u8 = 0x40;
pub const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
pub const EXTENDED_IS_STATIC: u8 = 0x01;

pub const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;

// ===========================================================================
// Offset-context helpers (mirrors issue #990 acceptance criterion 6)
// ===========================================================================

/// A located byte read: value, the absolute offset it started at, and the
/// number of bytes consumed. Used to attach rich context to assertions.
#[derive(Debug, Clone, Copy)]
pub struct Loc {
    pub value: u64,
    pub start: usize,
    pub len: usize,
}

impl Loc {
    pub fn end(&self) -> usize {
        self.start + self.len
    }
}

/// Read one Cassandra unsigned VInt starting at `pos`, returning the decoded
/// value and the byte range it consumed. Uses the reader's own `parse_vuint`
/// (the function the V5CompressedLegacy row-framing parser calls for `row_size`
/// and `prev_size`) so the test exercises the real decode surface.
pub fn read_uvint_loc(data: &[u8], pos: usize) -> Loc {
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
pub fn read_u8_loc(data: &[u8], pos: usize) -> Loc {
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
pub fn read_fixed_loc<'a>(data: &'a [u8], pos: usize, len: usize, what: &str) -> &'a [u8] {
    // Use checked arithmetic so a `pos` derived from a corrupt VInt near
    // usize::MAX cannot wrap `pos + len` (which would silently pass the guard and
    // lose this helper's contextual panic to a raw slice-index panic instead).
    assert!(
        pos.checked_add(len).is_some_and(|end| end <= data.len()),
        "{what}: fixed-width read of {len} bytes at offset {pos} (0x{pos:02X}) overruns the \
         {}-byte buffer — truncated/malformed fixture",
        data.len()
    );
    &data[pos..pos + len]
}

/// Read a big-endian `i32` at `pos` with a bounds guard (contextual panic).
#[track_caller]
pub fn read_be_i32(data: &[u8], pos: usize, what: &str) -> i32 {
    let b = read_fixed_loc(data, pos, 4, what);
    i32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// Read a big-endian `i64` at `pos` with a bounds guard (contextual panic).
#[track_caller]
pub fn read_be_i64(data: &[u8], pos: usize, what: &str) -> i64 {
    let b = read_fixed_loc(data, pos, 8, what);
    i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

/// Read a big-endian `u16` at `pos` with a bounds guard (contextual panic).
#[track_caller]
pub fn read_be_u16(data: &[u8], pos: usize, what: &str) -> u16 {
    let b = read_fixed_loc(data, pos, 2, what);
    u16::from_be_bytes([b[0], b[1]])
}

/// Assert a flag byte equals `expected`, with full offset context on mismatch.
#[track_caller]
pub fn fail_flag(loc: Loc, expected: u8, what: &str) {
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
pub fn fail_vint(loc: Loc, expected: u64, what: &str) {
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
pub fn datasets_root() -> PathBuf {
    if let Ok(r) = std::env::var("CQLITE_DATASETS_ROOT") {
        return PathBuf::from(r);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets")
}

/// Read a required fixture file, FAILING CLOSED with a clear message when the
/// binary `Data.db` has not been fetched.
pub fn read_required_fixture(rel: &str) -> Vec<u8> {
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
pub fn decompress_with_info(dir: &str, data: &[u8]) -> Vec<u8> {
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
pub fn decompress_fixture(dir: &str) -> Vec<u8> {
    let data = read_required_fixture(&format!("{dir}/nb-1-big-Data.db"));
    decompress_with_info(dir, &data)
}

// ===========================================================================
// Deterministic writer fixtures (mirrors issue #990's shared helpers)
// ===========================================================================

/// Deterministic Statistics.db baselines: zero LDT/TTL floors and a fixed
/// `min_timestamp` so every delta is a controlled, exactly-predictable VInt.
pub fn det_stats() -> StatisticsMetadata {
    let mut s = StatisticsMetadata::new();
    s.min_timestamp = 1_000_000;
    s.min_ttl = 0;
    s.min_local_deletion_time = 0;
    s
}

pub fn int_key_bytes(n: i32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Partition-header byte size for an int (4-byte) PK with no partition
/// tombstone: 2 (u16 key-length) + 4 (key) + 4 (LDT i32) + 8 (mfda i64) = 18.
pub const INT_PK_HEADER_SIZE: usize = 2 + 4 + 4 + 8;

/// int PK, int clustering (ASC), one static text + one regular text column.
pub fn static_schema() -> TableSchema {
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
pub fn int_clustering_no_static_schema() -> TableSchema {
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
pub fn text_clustering_schema() -> TableSchema {
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

pub fn write_op(column: &str, text: &str) -> CellOperation {
    CellOperation::Write {
        column: column.to_string(),
        value: Value::Text(text.to_string()),
    }
}

/// Build a one-partition Data.db with the public write path and return its bytes.
pub fn write_one_partition(
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
// Fixture path / header constants
// ===========================================================================

/// COMMITTED reference fixture (issue #1400 force-added its tiny Cassandra 5.0.2
/// `nb` binaries + golden into git): the cleanest static-only + dense
/// static+clustering generation (PK 99 static-only; PK 1/2/3 static + clustering
/// ck 1..=4 ASC). Tests against it are FAIL-CLOSED (a missing binary errors).
pub const STATIC_WITH_ROWS_DIR: &str =
    "sstables/test_deltas/static_with_rows-29a4cf80701f11f1b5d1d98b0640ec05";

/// COMMITTED reference fixture (issue #1400 force-added its Data.db +
/// CompressionInfo.db into git; the golden was already tracked): int32 PK + int32
/// clustering, static_col + row_col, with PK=1 carrying a static_block then
/// clustering rows (and later tombstones we do not walk here). Used for the
/// static+clustering byte-parity and static-row flag-byte anchor lanes,
/// FAIL-CLOSED (a missing binary errors).
pub const STATIC_TOMB_DIR: &str =
    "sstables/test_tomb/static_with_tombstones-4cdb9780702011f1b8f419c9a388d558";

/// PINNED fixture (in the CI dataset): UUID PK + TIMESTAMP clustering, a static
/// column + regular columns; every partition carries a static_block then a
/// clustering row. Snappy-compressed. Used for the canonical static-marker byte
/// parity lane.
pub const STATIC_COLUMNS_DIR: &str =
    "sstables/test_basic/static_columns_table-6b0425d0a25111f0a3fef1a551383fb9";

/// Partition-header byte size for the static_with_rows fixture (int32 PK, no
/// partition tombstone): 2 (u16 key-len) + 4 (key) + 4 (i32 LDT) + 8 (i64 mfda).
pub const STATIC_FIXTURE_HEADER: usize = 2 + 4 + 4 + 8;

/// Partition-header byte size for an int32-PK fixture (no partition tombstone):
/// 2 (u16 key-len) + 4 (key) + 4 (i32 LDT) + 8 (i64 mfda) = 18.
pub const INT_FIXTURE_HEADER: usize = 2 + 4 + 4 + 8;

pub const WIDE_DIR: &str =
    "sstables/test_wide_rows/wide_partition_table-6d6d0f80a25111f0a3fef1a551383fb9";

pub const COMPOSITE_DIR: &str =
    "sstables/test_basic/composite_key_table-6ab56990a25111f0a3fef1a551383fb9";

// ===========================================================================
// JSONL golden readers
// ===========================================================================

/// Read the JSONL golden into lines, FAILING CLOSED on a missing/empty file.
pub fn read_jsonl_lines(rel: &str) -> Vec<String> {
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

/// Extract the FIRST partition's int32 partition key from a JSONL golden line.
///
/// Cassandra orders partitions on disk by murmur3 TOKEN, not by key value, so
/// which key lands at file offset 0 is a property of the pinned dataset — not
/// something a byte walk may hard-code. Deriving the expected on-disk PK from the
/// golden's first line ties the assertion to the SAME ordering source the byte
/// walk validates: a dataset regeneration that reshuffles token order then yields
/// a clear "first golden PK X != on-disk PK Y" ordering signal here, rather than a
/// confusing "PK mismatch at offset 2" deep in the byte walk.
///
/// The golden shape is `{"...","partition":{"key":["<pk>"],"position":0},...}`;
/// for these int32-PK fixtures the key renders as a decimal string.
pub fn golden_first_partition_int_key(golden_first_line: &str) -> i32 {
    let marker = "\"key\":[\"";
    let start = golden_first_line.find(marker).unwrap_or_else(|| {
        panic!("golden first line has no \"key\":[\"...\"] partition key: {golden_first_line}")
    }) + marker.len();
    let rest = &golden_first_line[start..];
    let end = rest
        .find('"')
        .unwrap_or_else(|| panic!("golden first-line partition key is unterminated: {rest}"));
    rest[..end].parse::<i32>().unwrap_or_else(|e| {
        panic!(
            "golden first-partition key {:?} is not an int32 ({e})",
            &rest[..end]
        )
    })
}

/// Convert `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` to epoch microseconds (Howard
/// Hinnant's `days_from_civil`; no date/time crate needed). Mirrors #990.
pub fn iso8601_to_micros(s: &str) -> i64 {
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
