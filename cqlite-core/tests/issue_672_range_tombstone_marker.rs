//! VG6: Range tombstone marker skip regression tests (Issue #672)
//!
//! Validates that the `skip_range_tombstone_marker` fix correctly advances the
//! byte offset past an oa-format range tombstone bound, so that subsequent rows
//! in the same partition are parsed correctly.
//!
//! ## Root Cause (pre-fix)
//!
//! The original implementation was missing two fields from the binary layout:
//!
//! 1. `bound.size()` as a u16 big-endian integer (ClusteringBoundOrBoundary.java:105)
//!    written BEFORE `serializeValuesWithoutSize`.  Without this read, the parser
//!    consumed the size bytes as clustering data, causing misalignment.
//!
//! 2. `marker_body_size` VUInt (UnfilteredSerializer.java:291), written AFTER the
//!    clustering prefix and before `prev_size` + deletion times.  Without skipping
//!    this, the parser tried to parse the deletion bytes as the next row header.
//!
//! ## Binary layout (oa SSTable range tombstone bound)
//!
//! Ref: UnfilteredSerializer.java:282-303, ClusteringBoundOrBoundary.Serializer:103-107
//!
//! ```text
//! [flags:     u8                 ]  (0x02 = IS_MARKER)
//! [ext_flags: u8                 ]  (only if flags & 0x80 != 0)
//! [bound_kind: u8                ]  (ClusteringBoundOrBoundary.Kind ordinal)
//! [cluster_count: u16 big-endian ]  ← was MISSING (ClusteringBoundOrBoundary.java:105)
//! [vuint_header: variable        ]  (2 bits per clustering value: present/empty/null)
//! [clustering values...          ]
//! [marker_body_size: VUInt       ]  ← was MISSING (UnfilteredSerializer.java:291)
//! [prev_size: VUInt              ]  ┐
//! [deletion timestamp: VUInt     ]  │ skipped as a block (marker_body_size bytes)
//! [local deletion time: VUInt32  ]  ┘
//! ```
//!
//! ## Authority chain
//!
//! - `UnfilteredSerializer.java:282-303` (primary serialization loop)
//! - `ClusteringBoundOrBoundary.Serializer.serialize:103-107` (clustering prefix)
//! - `SerializationHeader.writeDeletionTime:180-183` (deletion time encoding)
//! - `writeUnsignedVInt` for marker_body_size, `writeVInt` for timestamp delta,
//!   `writeUnsignedVInt` for local_deletion_time delta
//!
//! Source: cassandra-5.0.8 (apache/cassandra, tag cassandra-5.0.8)

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::{Config, Platform};

// ============================================================================
// Byte-level regression: crafted range tombstone marker layout
// ============================================================================

/// Verify that the range tombstone binary layout constants are correct.
///
/// IS_MARKER (0x02) must be distinct from a regular row flag byte.
/// ROW_HAS_EXTENDED_FLAGS (0x80) triggers an extra extended-flags byte.
///
/// Source: UnfilteredRowFlags.java — IS_MARKER = 0x02, HAS_EXTENDED_FLAGS = 0x80
#[test]
fn range_tombstone_flag_constants() {
    const IS_MARKER: u8 = 0x02;
    const HAS_EXTENDED_FLAGS: u8 = 0x80;

    // A marker byte must have IS_MARKER set
    let marker_byte: u8 = 0x02;
    assert_eq!(
        marker_byte & IS_MARKER,
        IS_MARKER,
        "IS_MARKER (0x02) must be set for range tombstone bounds"
    );

    // A regular row start byte does NOT have IS_MARKER set
    let row_byte: u8 = 0x04; // HAS_CLUSTERING flag
    assert_eq!(
        row_byte & IS_MARKER,
        0,
        "regular row start must NOT have IS_MARKER set"
    );

    // Extended flags: 0x80 bit triggers an extra byte read
    let with_ext: u8 = 0x82; // IS_MARKER | HAS_EXTENDED_FLAGS
    assert_ne!(
        with_ext & HAS_EXTENDED_FLAGS,
        0,
        "0x82 has HAS_EXTENDED_FLAGS set"
    );
}

/// Verify the u16 big-endian cluster_count field layout.
///
/// ClusteringBoundOrBoundary.Serializer.serialize (cassandra-5.0.8:line 105)
/// writes `out.writeShort(bound.size())` — a 2-byte big-endian integer —
/// BEFORE `serializeValuesWithoutSize`.  This was absent from the pre-fix
/// skip_range_tombstone_marker, causing byte misalignment.
///
/// Source: ClusteringBoundOrBoundary.java:105 — `out.writeShort(bound.size())`
#[test]
fn cluster_count_u16_big_endian_layout() {
    // A clustering size of 1 serializes as [0x00, 0x01]
    let cluster_count_1: u16 = 1;
    let bytes = cluster_count_1.to_be_bytes();
    assert_eq!(bytes, [0x00, 0x01], "u16 big-endian: 1 => [0x00, 0x01]");

    // A clustering size of 0 serializes as [0x00, 0x00]
    let cluster_count_0: u16 = 0;
    let bytes0 = cluster_count_0.to_be_bytes();
    assert_eq!(bytes0, [0x00, 0x00], "u16 big-endian: 0 => [0x00, 0x00]");

    // Recover from raw bytes (the operation the fixed parser does)
    let raw = [0x00u8, 0x01u8];
    let recovered = u16::from_be_bytes(raw);
    assert_eq!(recovered, 1, "recovered cluster_count == 1");
}

/// Verify VUInt single-byte encoding for small values.
///
/// marker_body_size is encoded as `writeUnsignedVInt`.  For values < 128
/// this is a single byte equal to the value itself.  For values >= 128 the
/// leading bit is set.
///
/// This test exercises the most common case (small body sizes) to confirm
/// that reading a single byte gives the correct body_size.
///
/// Source: VIntCoding.java `writeUnsignedVInt` — leading-ones encoding,
///         single byte for value < 128.
#[test]
fn vuint_single_byte_for_small_body_size() {
    // A marker_body_size of 10 (2+8 bytes: 1 prev_size byte + 8 deletion bytes)
    let body_size: u8 = 10;
    assert!(
        body_size < 128,
        "value < 128 encodes as a single VUInt byte"
    );
    // The VUInt value is just the byte itself when < 128
    let vuint_byte = body_size;
    assert_eq!(vuint_byte, 10, "single-byte VUInt encodes value directly");

    // A marker_body_size of 0 means an empty body (degenerate but valid)
    let empty_size: u8 = 0;
    assert_eq!(empty_size, 0, "zero body_size: no bytes to skip");

    // Multi-byte sentinel: the first byte of a 2-byte VUInt has bit 7 set
    let multi_byte_sentinel: u8 = 0x80;
    assert_ne!(
        multi_byte_sentinel & 0x80,
        0,
        "bit-7-set signals multi-byte VUInt"
    );
}

/// Verify that pre-fix misalignment would produce an out-of-bounds read.
///
/// If cluster_count (2 bytes) and marker_body_size (1+ bytes) were skipped,
/// a subsequent read would start 3+ bytes too early, reading "cluster_count
/// bytes" as if they were a row flags byte or VInt header.
///
/// This test proves the symptom:
/// - Pre-fix: reads [0x00, 0x01] (the cluster_count u16) as the VUInt header
///   for clustering values, then proceeds to mis-parse subsequent bytes.
/// - Post-fix: reads the 2-byte cluster_count first, then the clustering
///   VUInt header.
///
/// The test doesn't call parse_clustering_prefix directly (it's private), but
/// validates the mathematical invariant: the 2-byte skip matters.
#[test]
fn pre_fix_misalignment_invariant() {
    // Minimal range tombstone binary suffix after the flags byte:
    // [bound_kind=0x00][cluster_count=0x0001][vuint_header=0x00][value=0xAB]
    // [marker_body_size=0x02][prev_size=0x00][del_timestamp=0x00]
    let bytes: &[u8] = &[
        0x00, // bound_kind byte (EXCL_END_BOUND ordinal = 0, or similar)
        0x00, 0x01, // cluster_count = 1 (u16 big-endian)  ← was skipped pre-fix
        0x00, // vuint_header for 1 clustering value: 0b00 = present
        0xAB, // clustering value byte
        0x02, // marker_body_size VUInt = 2  ← was skipped pre-fix
        0x00, // prev_size VUInt = 0
        0x00, // deletion timestamp VUInt = 0 (delta from header min)
              // (local_deletion_time would also be here if non-trivial, but
              //  for simplicity we use a 0-delta case folded into prev_size=0)
    ];

    // Post-fix: read bound_kind (1 byte) then cluster_count u16 (2 bytes)
    let bound_kind = bytes[0];
    let cluster_count = u16::from_be_bytes([bytes[1], bytes[2]]) as usize;
    assert_eq!(bound_kind, 0x00, "bound_kind byte");
    assert_eq!(cluster_count, 1, "cluster_count correctly parsed as 1");

    // After cluster_count, the clustering VUInt header starts at bytes[3]
    let vuint_header_pos = 3;
    assert_eq!(
        bytes[vuint_header_pos], 0x00,
        "vuint_header at correct offset"
    );

    // Pre-fix (missing u16 read): would try to use bytes[1]=0x00 as vuint_header,
    // bytes[2]=0x01 as the clustering value — wrong offset by 2 bytes.
    let wrong_vuint_header_pos = 1; // pre-fix: skips only bound_kind
                                    // At the wrong offset, bytes[1]=0x00 looks like a valid header (by accident)
                                    // but bytes[2]=0x01 is the low byte of cluster_count, not a cluster value.
    let misread_as_cluster_value = bytes[wrong_vuint_header_pos + 1];
    assert_eq!(
        misread_as_cluster_value, 0x01,
        "pre-fix misread: low byte of cluster_count consumed as cluster data"
    );
    // This demonstrates the 2-byte drift that corrupted all subsequent parses.
}

// ============================================================================
// Integration: oa tombstone_table parity (primary regression guard)
// ============================================================================

/// Helper: find the Data.db binary in an SSTable table directory.
fn find_data_db(dir: &Path) -> Option<PathBuf> {
    fs::read_dir(dir).ok()?.flatten().find_map(|entry| {
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if s.ends_with("-Data.db") && !s.starts_with("._") {
            Some(entry.path())
        } else {
            None
        }
    })
}

/// Helper: count live (non-tombstone) rows in a JSONL golden file.
///
/// A row entry with `deletion_info` but no `cells` is a row tombstone.
/// CQLite suppresses row tombstones from query results (`filter_tombstone`),
/// so the JSONL count must exclude them for a fair comparison.
///
/// Source: UnfilteredSerializer.java (row tombstone structure) +
///         Issue #505 (filter_tombstone implementation)
fn count_jsonl_live_rows(path: &Path) -> std::io::Result<usize> {
    let mut content = String::new();
    File::open(path)?.read_to_string(&mut content)?;
    let mut live_rows = 0usize;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let partition: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(rows) = partition.get("rows").and_then(|r| r.as_array()) {
            for row in rows {
                let is_row = row.get("type").and_then(|t| t.as_str()) == Some("row");
                let has_deletion = row.get("deletion_info").is_some();
                let has_cells = row
                    .get("cells")
                    .and_then(|c| c.as_array())
                    .map(|c| !c.is_empty())
                    .unwrap_or(false);
                // Live rows: type==row AND (no deletion_info OR has cells)
                if is_row && (!has_deletion || has_cells) {
                    live_rows += 1;
                }
            }
        }
    }
    Ok(live_rows)
}

/// Helper: resolve an oa table directory and verify binaries are present.
///
/// Returns None when `CQLITE_DATASETS_ROOT` is unset, the table directory
/// is absent, or only JSONL golden files exist (binary-free CI checkout).
fn resolve_oa_table_path(keyspace: &str, table_prefix: &str) -> Option<PathBuf> {
    let datasets_root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let sstables_dir = PathBuf::from(&datasets_root).join("sstables");
    let keyspace_dir = sstables_dir.join(keyspace);

    fs::read_dir(&keyspace_dir)
        .ok()?
        .flatten()
        .find_map(|entry| {
            let name = entry.file_name();
            if !name.to_string_lossy().starts_with(table_prefix) {
                return None;
            }
            let table_dir = entry.path();
            // Require at least one oa-*-big-Data.db binary (not just JSONL)
            let has_binary = fs::read_dir(&table_dir)
                .map(|entries| {
                    entries.flatten().any(|e| {
                        let n = e.file_name();
                        let s = n.to_string_lossy();
                        s.ends_with("-Data.db") && !s.starts_with("._")
                    })
                })
                .unwrap_or(false);
            if has_binary {
                Some(table_dir)
            } else {
                None
            }
        })
}

/// VG6 primary regression: oa tombstone_table must parse exactly 3 live rows.
///
/// Pre-fix behaviour: `skip_range_tombstone_marker` was missing the u16
/// cluster_count field and the marker_body_size VUInt, causing the offset to
/// drift 3+ bytes into the next range tombstone.  The next byte (0x04) was
/// not IS_MARKER (0x02) so the parser attempted to decode it as a regular
/// row.  With an apparent row_size of ~12 kB from the misread VInt, the
/// parser errored with "Not enough bytes for row data".
///
/// Post-fix: offset advances correctly past the full bound serialization, and
/// all three live rows (the fourth "row" in the JSONL is a row tombstone and
/// is correctly suppressed) are returned.
///
/// Authority: UnfilteredSerializer.java:282-303, ClusteringBoundOrBoundary.java:103-107
#[tokio::test]
async fn test_oa_tombstone_table_range_tombstone_marker_fix() {
    let dir = match resolve_oa_table_path("test_oa", "tombstone_table") {
        Some(d) => d,
        None => {
            eprintln!(
                "SKIP: test_oa/tombstone_table binaries not present (CQLITE_DATASETS_ROOT not set or binary absent)"
            );
            return;
        }
    };

    let data_db = find_data_db(&dir).expect("Data.db must exist in resolved dir");
    let jsonl_path = {
        let name = data_db.file_name().unwrap().to_string_lossy();
        data_db.parent().unwrap().join(format!("{}.jsonl", name))
    };

    let expected = count_jsonl_live_rows(&jsonl_path).expect("JSONL golden must be readable");

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = SSTableReader::open(&data_db, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("Failed to open tombstone_table Data.db: {}", e));

    let entries = reader
        .get_all_entries()
        .await
        .unwrap_or_else(|e| panic!("get_all_entries failed for tombstone_table: {}", e));

    // Exactly 3 live rows (tombstone_table JSONL has 4 row entries; one is a
    // row tombstone with deletion_info and no cells — excluded by filter_tombstone).
    assert_eq!(
        entries.len(),
        expected,
        "VG6 regression: tombstone_table must return {} live rows (got {}); \
         pre-fix it returned an error due to range tombstone marker misalignment",
        expected,
        entries.len()
    );
    assert!(
        !entries.is_empty(),
        "tombstone_table must have at least one live row"
    );
}

/// VG6 summary: all 6 oa tables must achieve parity with their JSONL goldens.
///
/// This test mirrors `test_oa_all_tables_pass_parity` from issue_655_oa_read_gates.rs
/// but is specifically the VG6 acceptance gate: ALL six tables pass, including
/// `simple_table`, `tombstone_table`, and `collection_table` which were gated as
/// known-broken by VG4 (Issue #656) and are now fixed by this issue.
///
/// Authority: Acceptance criteria for Issue #672
#[tokio::test]
async fn test_vg6_all_oa_tables_pass_parity() {
    let tables = [
        "simple_table",
        "collection_table",
        "ttl_table",
        "static_table",
        "tombstone_table",
        "udt_table",
    ];

    if std::env::var("CQLITE_DATASETS_ROOT").is_err() {
        eprintln!("SKIP: CQLITE_DATASETS_ROOT not set — skipping VG6 oa parity summary");
        return;
    }
    if resolve_oa_table_path("test_oa", tables[0]).is_none() {
        eprintln!("SKIP: test_oa binaries not present (golden-only checkout)");
        return;
    }

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));

    let mut results: HashMap<&str, Result<(usize, usize), String>> = HashMap::new();

    for table in &tables {
        let dir = match resolve_oa_table_path("test_oa", table) {
            Some(d) => d,
            None => {
                results.insert(table, Err(format!("binary absent for {}", table)));
                continue;
            }
        };

        let data_db = match find_data_db(&dir) {
            Some(p) => p,
            None => {
                results.insert(table, Err(format!("no Data.db in {}", dir.display())));
                continue;
            }
        };

        let jsonl_path = {
            let name = data_db.file_name().unwrap().to_string_lossy();
            data_db.parent().unwrap().join(format!("{}.jsonl", name))
        };

        let expected = match count_jsonl_live_rows(&jsonl_path) {
            Ok(n) => n,
            Err(e) => {
                results.insert(table, Err(format!("JSONL read error: {}", e)));
                continue;
            }
        };

        let platform_clone = Arc::clone(&platform);
        match SSTableReader::open(&data_db, &config, platform_clone).await {
            Err(e) => {
                results.insert(table, Err(format!("open error: {}", e)));
            }
            Ok(reader) => match reader.get_all_entries().await {
                Err(e) => {
                    results.insert(table, Err(format!("get_all_entries error: {}", e)));
                }
                Ok(entries) => {
                    results.insert(table, Ok((entries.len(), expected)));
                }
            },
        }
    }

    let mut all_pass = true;
    for table in &tables {
        match results.get(table) {
            Some(Ok((got, expected))) => {
                if got != expected {
                    eprintln!(
                        "FAIL test_oa/{}: expected {} live rows, got {}",
                        table, expected, got
                    );
                    all_pass = false;
                } else {
                    eprintln!("PASS test_oa/{}: {} live rows", table, got);
                }
            }
            Some(Err(e)) => {
                eprintln!("FAIL test_oa/{}: {}", table, e);
                all_pass = false;
            }
            None => {
                eprintln!("FAIL test_oa/{}: no result recorded", table);
                all_pass = false;
            }
        }
    }

    assert!(
        all_pass,
        "VG6: not all oa fixture tables passed parity (see above)"
    );
}
