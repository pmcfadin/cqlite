//! Shared, non-test helpers + constants for the issue #992 TTL / tombstone /
//! range-marker byte-parity suite.
//!
//! This lives in a SUBDIRECTORY of `tests/`, so it is NOT compiled as its own
//! test binary — it is included into the single
//! `issue_992_ttl_tombstone_range_parity` test target via `#[path = ...] mod`.
//! Mirrors the issue #990 / #991 pattern: offset-context helpers ([`Loc`],
//! [`read_uvint_loc`], [`read_u8_loc`], [`fail_flag`], [`fail_vint`]), the
//! FAIL-CLOSED / SKIP-ON-ABSENCE fixture access, deterministic writer fixtures,
//! and JSONL golden readers. Splitting them out keeps the `#[test]` file under
//! the file-size ratchet (#1135).

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::parser::vint::parse_vuint;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::data_writer::DataWriter;
use cqlite_core::storage::sstable::writer::stats_writer::StatisticsMetadata;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, DecoratedKey, Mutation, PartitionTombstone, RangeTombstone,
};
use cqlite_core::types::Value;

// ---------------------------------------------------------------------------
// Row / cell / marker flag bit constants (mirror data_writer/mod.rs and the
// V5CompressedLegacy parser; the on-disk bit values Cassandra's
// UnfilteredSerializer / Cell.Serializer / ClusteringBoundOrBoundary write).
// ---------------------------------------------------------------------------
pub const ROW_HAS_TIMESTAMP: u8 = 0x04;
pub const ROW_HAS_TTL: u8 = 0x08;
pub const ROW_HAS_DELETION: u8 = 0x10;
pub const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
pub const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;

pub const CELL_IS_DELETED: u8 = 0x01;
pub const CELL_IS_EXPIRING: u8 = 0x02;
pub const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
pub const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
pub const CELL_USE_ROW_TTL: u8 = 0x10;

/// Range tombstone marker sentinel (Unfiltered.Kind: IS_MARKER).
pub const IS_MARKER: u8 = 0x02;
/// End-of-partition sentinel byte.
pub const END_OF_PARTITION: u8 = 0x01;

// ClusteringPrefix.Kind ordinals written on disk by
// ClusteringBoundOrBoundary.Serializer.serialize (ClusteringPrefix.java):
//   0 = EXCL_END_BOUND, 1 = INCL_START_BOUND,
//   2 = EXCL_END_INCL_START_BOUNDARY, 5 = INCL_END_EXCL_START_BOUNDARY,
//   6 = INCL_END_BOUND, 7 = EXCL_START_BOUND.
pub const EXCL_END_BOUND: u8 = 0;
pub const INCL_START_BOUND: u8 = 1;
pub const EXCL_END_INCL_START_BOUNDARY: u8 = 2;
pub const INCL_END_EXCL_START_BOUNDARY: u8 = 5;
pub const INCL_END_BOUND: u8 = 6;

// ===========================================================================
// Offset-context helpers (mirror issue #990 acceptance criterion 6)
// ===========================================================================

/// A located byte read: value, the absolute offset it started at, and the
/// number of bytes consumed.
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

/// Read one Cassandra unsigned VInt starting at `pos` through the reader's own
/// `parse_vuint` (the function the V5CompressedLegacy row/marker parser uses for
/// row_size / prev_size / deltas), returning the decoded value + consumed range.
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

/// Read the byte at `pos` (a flag/kind byte), returning value + offset.
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

/// Read a fixed-width region `[pos, pos+len)` with a contextual bounds guard.
#[track_caller]
pub fn read_fixed_loc<'a>(data: &'a [u8], pos: usize, len: usize, what: &str) -> &'a [u8] {
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

/// Assert a flag/kind byte equals `expected`, with full offset context.
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
// FAIL-CLOSED / SKIP-ON-ABSENCE fixture access
// ===========================================================================

/// Resolve the datasets root (env first, else the in-repo path).
pub fn datasets_root() -> PathBuf {
    if let Ok(r) = std::env::var("CQLITE_DATASETS_ROOT") {
        return PathBuf::from(r);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets")
}

/// Decompress a real nb Data.db using the reader's own chunk decompressor (the
/// exact surface the V5CompressedLegacy reader uses). A present-but-empty body
/// is a failure.
fn decompress_with_info(dir: &str, data: &[u8]) -> Vec<u8> {
    use cqlite_core::storage::sstable::chunk_decompressor::create_decompressor_from_file;
    use std::io::Cursor;

    let comp_path = datasets_root().join(format!("{dir}/nb-1-big-CompressionInfo.db"));
    let mut dec = create_decompressor_from_file(&comp_path).unwrap_or_else(|e| {
        panic!(
            "issue #992: {dir} CompressionInfo missing/invalid ({e}) at {}",
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

/// `true` when strict fixture mode is requested. Either `CQLITE_REQUIRE_FIXTURES`
/// (the repo-wide convention used by the #992 manifest `comparison_command`s) or
/// `CQLITE_PARITY_REQUIRE_DATASETS` (the name in issue #1205) set to a truthy
/// value ("1"/"true") flips the fixture lanes FAIL-CLOSED: an absent binary
/// PANICS (test failure) instead of skipping, so a required CI gate cannot
/// false-green if a `test_deltas/*` fixture ever disappears (issue #1205 AC#2).
/// When neither is set, the default skip-on-absence behavior is preserved so
/// local dev without the binaries still works.
pub fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    ) || matches!(
        std::env::var("CQLITE_PARITY_REQUIRE_DATASETS").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Decompress a LOCAL-ONLY fixture's Data.db, returning `None` when the binary
/// is absent (skip-on-presence). The `test_deltas/*` tombstone/TTL/range
/// fixtures ship in the pinned CI dataset, so under strict mode
/// (`require_fixtures_strict`) a missing binary PANICS instead of skipping — the
/// required lane must fail-closed, never false-green on missing data (#1205).
/// In the default (non-strict) local-dev mode a missing binary is SKIPPED.
/// Either way, a fixture that IS present must parse and yield markers, and a
/// present-but-empty body is a failure (asserted in `decompress_with_info`).
pub fn decompress_local_only(dir: &str) -> Option<Vec<u8>> {
    let path = datasets_root().join(format!("{dir}/nb-1-big-Data.db"));
    let data = match std::fs::read(&path) {
        Ok(data) => data,
        Err(_) => {
            if require_fixtures_strict() {
                panic!(
                    "CQLITE_REQUIRE_FIXTURES=1 but fixture {dir} absent — \
                     fetch with bash test-data/scripts/fetch-datasets.sh (looked for {})",
                    path.display()
                );
            }
            return None;
        }
    };
    Some(decompress_with_info(dir, &data))
}

// ===========================================================================
// JSONL golden readers (skip-on-absence; present-but-empty is a failure)
// ===========================================================================

/// Read a LOCAL-ONLY JSONL golden, returning `None` when the file is absent.
/// A present-but-empty golden is a failure.
pub fn read_jsonl_lines_opt(rel: &str) -> Option<Vec<String>> {
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

/// Load BOTH the decompressed Data.db and its JSONL golden for a local-only
/// fixture, returning `None` when EITHER is absent (skip). When present, a
/// 0-marker / empty body fails (enforced by `decompress_with_info` + the
/// non-empty golden assertion). This pairs the byte walk with the semantic
/// oracle so the two never drift.
pub fn load_local_only(dir: &str) -> Option<(Vec<u8>, Vec<String>)> {
    let raw = decompress_local_only(dir)?;
    let jsonl_rel = format!("{dir}/nb-1-big-Data.db.jsonl");
    let jsonl = match read_jsonl_lines_opt(&jsonl_rel) {
        Some(jsonl) => jsonl,
        None => {
            if require_fixtures_strict() {
                panic!(
                    "CQLITE_REQUIRE_FIXTURES=1 but fixture JSONL golden {jsonl_rel} absent \
                     (binary present) — fetch with bash test-data/scripts/fetch-datasets.sh"
                );
            }
            return None;
        }
    };
    Some((raw, jsonl))
}

/// Convert `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` to epoch microseconds (Howard
/// Hinnant's `days_from_civil`; no date/time crate needed). Mirrors #990/#991.
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

// ===========================================================================
// Deterministic writer fixtures (mirror #990/#991 shared helpers)
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

/// Partition-header byte size for an int32 PK with no partition tombstone:
/// 2 (u16 key-len) + 4 (key) + 4 (i32 LDT) + 8 (i64 mfda) = 18.
pub const INT_PK_HEADER_SIZE: usize = 2 + 4 + 4 + 8;

/// int PK, single INT clustering (ASC), one regular text column, NO statics.
pub fn int_clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue992".to_string(),
        table: "t".to_string(),
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
            name: "val".to_string(),
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
    partition_tombstone: Option<&PartitionTombstone>,
    range_tombstones: &[RangeTombstone],
) -> Vec<u8> {
    let mut w = DataWriter::new(stats);
    let key = DecoratedKey::new(key_id as i64, int_key_bytes(key_id));
    w.write_partition(
        &key,
        mutations,
        schema,
        partition_tombstone,
        range_tombstones,
    )
    .expect("write_partition");
    w.finish().expect("finish")
}

// ===========================================================================
// LOCAL-ONLY fixture paths (test_deltas — NOT in the pinned CI dataset).
// Tests SKIP when the binary is absent; FAIL (not skip) when present and they
// find 0 markers / 0 rows. Each carries: int32 PK + int32 clustering (range
// fixtures additionally have a UTF8 second clustering on `range_tombstones`),
// `val` UTF8 regular column, LZ4-compressed.
// ===========================================================================

pub const PARTITION_TOMBSTONES_DIR: &str =
    "sstables/test_deltas/partition_tombstones-299258f0701f11f1b5d1d98b0640ec05";
pub const ROW_TOMBSTONES_DIR: &str =
    "sstables/test_deltas/row_tombstones-297f1f10701f11f1b5d1d98b0640ec05";
pub const CELL_TOMBSTONES_DIR: &str =
    "sstables/test_deltas/cell_tombstones-29733830701f11f1b5d1d98b0640ec05";
pub const TTL_CELLS_DIR: &str = "sstables/test_deltas/ttl_cells-299c9220701f11f1b5d1d98b0640ec05";
pub const RANGE_TOMBSTONES_DIR: &str =
    "sstables/test_deltas/range_tombstones-298894f0701f11f1b5d1d98b0640ec05";
pub const ADJACENT_RANGES_DIR: &str =
    "sstables/test_deltas/adjacent_ranges-29bdd5c0701f11f1b5d1d98b0640ec05";

/// Statistics.db EncodingStats baselines, captured from the pinned fixtures'
/// `Statistics.db.txt` (the published EncodingStats minima). The byte walks
/// reconstruct absolute mfda/ldt/ttl from the on-disk delta + these minima and
/// cross-check against the JSONL golden.
pub mod minima {
    /// row_tombstones EncodingStats minTimestamp (µs).
    pub const ROW_MIN_TS: i64 = 1_782_341_457_468_043;
    /// cell_tombstones EncodingStats minTimestamp (µs).
    pub const CELL_MIN_TS: i64 = 1_782_341_457_428_748;
    /// ttl_cells EncodingStats minTTL (s) / minLocalDeletionTime (s). (minTimestamp
    /// is not needed: the partition_tombstones / ttl LDT use FIXED-header / derived
    /// reconstruction, not a min_timestamp delta walk.)
    pub const TTL_MIN_TTL: u64 = 3_600;
    pub const TTL_MIN_LDT: i64 = 1_782_345_057;
    /// range_tombstones EncodingStats minTimestamp (µs).
    pub const RANGE_MIN_TS: i64 = 1_782_341_457_502_909;
    /// adjacent_ranges EncodingStats minTimestamp (µs) — a deliberately-old
    /// 2001-09-09 floor so the marker mfda deltas are tiny (0/1/2…).
    pub const ADJACENT_MIN_TS: i64 = 1_000_000_000_000_001;
    /// Shared minLocalDeletionTime floor for the row/cell/range/adjacent
    /// fixtures (2026-06-24T22:50:57Z, seconds).
    pub const SHARED_MIN_LDT: i64 = 1_782_341_457;
}
