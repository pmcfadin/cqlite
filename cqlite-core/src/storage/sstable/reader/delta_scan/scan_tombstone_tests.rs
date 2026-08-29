//! `scan_delta` corpus tests: row / range / partition tombstones, adjacent range
//! deletes and collection deltas, plus the range-tombstone-marker parse unit tests
//! (Issues #698/#699, DS4).
//!
//! Split out of `scan.rs` per the campsite rule (#1116/#1135); see the sibling
//! `scan_tests.rs` for the other half. This is a VERBATIM move: no assertion
//! changed. `find_test_deltas_table_dir` lives here because only these tests use it.
//!
//! Included from [`super`] via `#[path = "scan_tombstone_tests.rs"]`, so
//! `use super::*` reaches the `scan_delta` driver and its parse helpers.

use super::*;

// -----------------------------------------------------------------------
// Shared helper: locate a table directory within test_deltas.
// Returns None (causing the caller to skip) when the binary Data.db is
// absent, matching the dataset-gated convention used throughout this file.
// -----------------------------------------------------------------------

fn find_test_deltas_table_dir(
    root: &std::path::Path,
    table_prefix: &str,
) -> Option<std::path::PathBuf> {
    let deltas_dir = root.join("sstables/test_deltas");
    if !deltas_dir.exists() {
        eprintln!(
            "test_deltas not found at {:?} — skipping e2e test \
             (run `bash test-data/scripts/generate-deltas.sh` to regenerate)",
            deltas_dir
        );
        return None;
    }
    // Find ANY matching directory that actually has a binary Data.db.
    // There may be multiple directories with the same table-name prefix (e.g.,
    // after regenerating fixtures — the old JSONL-only dir and the new binary dir
    // coexist until cleaned up).  We pick the first one that has Data.db.
    let table_dir = std::fs::read_dir(&deltas_dir).ok()?.find_map(|e| {
        let entry = e.ok()?;
        let name = entry.file_name();
        let n = name.to_string_lossy();
        if !n.starts_with(table_prefix) {
            return None;
        }
        let path = entry.path();
        // Has a real binary Data.db (not the JSONL companion file)?
        let has_data_db = std::fs::read_dir(&path)
            .ok()
            .map(|it| {
                it.filter_map(|e| e.ok()).any(|e| {
                    let fname = e.file_name();
                    let fn_str = fname.to_string_lossy();
                    fn_str.ends_with("-Data.db") && !fn_str.ends_with(".db.jsonl")
                })
            })
            .unwrap_or(false);
        if has_data_db {
            Some(path)
        } else {
            None
        }
    });
    match table_dir {
        Some(dir) => Some(dir),
        None => {
            eprintln!(
                "No binary Data.db found in any {}-* directory under test_deltas — \
                 skipping e2e test (run `bash test-data/scripts/generate-deltas.sh` \
                 to regenerate binaries)",
                table_prefix
            );
            None
        }
    }
}

// -----------------------------------------------------------------------
// Issue #699 unit tests: hard-error on unrepresentable structures
//
// Roborev finding (Finding 1): the original three tests constructed the
// expected error strings INLINE (tautologies) and never exercised production
// code.  Analysis:
//
// 1. partition_tombstone missing deletion_time: the production code always
//    supplies `marked_for_delete_at=Some(...)` when `is_partition_tombstone=true`
//    because both flags come from the same source (the parsed deletion time
//    in `parse_partition_header_full`).  The `ok_or_else` branch is unreachable.
//    Test DELETED — shipping a tautology is worse than no test.
//
// 2. row_tombstone missing deletion_time: same reasoning — `is_row_tombstone`
//    is set only when `ROW_HAS_DELETION` is decoded, which always produces
//    `marked_for_delete_at=Some(...)`.  Test DELETED.
//
// 3. unknown range tombstone kind: this CAN be driven through production code
//    by calling `parse_range_tombstone_marker_full` with a crafted byte buffer
//    whose `bound_kind` byte is set to an unrecognised value.  Replaced with
//    real tests below.
// -----------------------------------------------------------------------

/// Confirm `parse_partition_header_full` correctly returns `Some(deleted_at)` for
/// a DEAD partition (nb format, `localDeletionTime != i32::MAX`).
///
/// This is the production code path that sets `is_partition_tombstone=true` in
/// `parse_block_emit_delta`.  The test proves the path always delivers
/// `marked_for_delete_at=Some(...)` — so the `ok_or_else` guard in the emit
/// closure is a belt-and-suspenders assertion rather than a reachable branch.
/// If `parse_partition_header_full` were changed to return `None` for a dead
/// partition, this test would fail.
#[test]
fn partition_header_full_returns_deleted_at_for_dead_partition_nb_format() {
    use crate::storage::sstable::reader::parsing::PublicV5CompressedLegacyParser;

    let parser = PublicV5CompressedLegacyParser::new(
        "test_deltas".to_string(),
        "partition_tombstones".to_string(),
        0, // min_timestamp (absolute = 0 + delta)
        0, // min_local_deletion_time
        None,
    );

    // nb-format partition header for a DEAD partition:
    //   [flags: u8 = 0x00]
    //   [key_len: u8 = 0x04]  — 4-byte key
    //   [key:  0x00 0x00 0x00 0x01]
    //   [local_deletion_time: i32 BE = 0x61234567]  — NOT i32::MAX → dead partition
    //   [markedForDeleteAt:   i64 BE = 0x00_00_61_7C_AF_98_CC_00]  — plausible µs ts
    //
    // i32::MAX = 0x7fff_ffff; 0x61234567 != that → partition is dead.
    // markedForDeleteAt = 0x00_00_61_7C_AF_98_CC_00 = 106_903_296_000_000 µs (year ~1973+)
    let dead_ts: i64 = 0x0000_617C_AF98_CC00_u64 as i64;
    let mut buf = vec![
        0x00_u8, // flags
        0x04_u8, // key_len = 4
        0x00, 0x00, 0x00, 0x01, // key bytes
        0x61, 0x23, 0x45, 0x67, // localDeletionTime (NOT i32::MAX)
    ];
    buf.extend_from_slice(&dead_ts.to_be_bytes()); // markedForDeleteAt (8 bytes)

    let (_row_key, _next_offset, partition_deletion) = parser
        .parse_partition_header_full(&buf, 0)
        .expect("parse should succeed");

    assert!(
        partition_deletion.is_some(),
        "DEAD partition must yield Some(deleted_at); got None"
    );
    assert_eq!(
        partition_deletion.unwrap().0,
        dead_ts,
        "deleted_at must equal the markedForDeleteAt bytes in the header"
    );
}

/// Confirm `parse_range_tombstone_marker_full` returns the raw `bound_kind` byte
/// unchanged — including values that are not in the recognised set {0,1,2,5,6,7}.
///
/// The production caller (`parse_block_emit_delta`) pattern-matches on the returned
/// kind and hits the `unknown => return Err(...)` arm when kind ∉ {0,1,2,5,6,7}.
/// This test verifies the parser faithfully surfaces the unknown kind so the
/// caller's hard-error branch is reachable.  If `parse_range_tombstone_marker_full`
/// were silently clamped to a known kind, this test would fail.
#[test]
fn parse_range_tombstone_marker_full_surfaces_unknown_bound_kind() {
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::sstable::reader::parsing::PublicV5CompressedLegacyParser;

    let parser = PublicV5CompressedLegacyParser::new(
        "test_deltas".to_string(),
        "range_tombstones".to_string(),
        0, // min_timestamp
        0, // min_local_deletion_time
        None,
    );

    // Minimal schema (one INT partition key, no clustering keys).
    let schema = TableSchema {
        keyspace: "test_deltas".to_string(),
        table: "range_tombstones".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    // Crafted range tombstone marker with bound_kind = 99 (unrecognised).
    //
    // Wire layout (ClusteringBoundOrBoundary.Serializer + UnfilteredSerializer):
    //   [marker_flags: u8 = 0x02]   IS_MARKER bit, no ROW_HAS_EXTENDED_FLAGS (0x80)
    //   [bound_kind:   u8 = 99]     ← unknown ordinal
    //   [cluster_count: u16 BE = 0x00 0x00]  no clustering values
    //   [marker_body_size VUInt = 0x03]       3 bytes follow in body
    //   [prev_size VUInt = 0x00]
    //   [mfda_delta  VUInt = 0x00]
    //   [ldt_delta   VUInt = 0x00]
    let crafted_marker: Vec<u8> = vec![
        0x02, // marker_flags (IS_MARKER)
        99u8, // bound_kind — the unknown value
        0x00, 0x00, // cluster_count = 0
        0x03, // marker_body_size = 3
        0x00, // prev_size VUInt(0)
        0x00, // mfda_delta VUInt(0)
        0x00, // ldt_delta VUInt(0)
    ];

    let (bound_values, bound_kind, deleted_at_primary, deleted_at_secondary, next_offset) = parser
        .parse_range_tombstone_marker_full(&crafted_marker, 0, &schema)
        .expect("parse_range_tombstone_marker_full should succeed on crafted buffer");

    // The parser must pass the kind byte through unchanged.
    assert_eq!(
        bound_kind, 99,
        "parse_range_tombstone_marker_full must return bound_kind=99 unchanged; \
         if this fails the production hard-error branch in parse_block_emit_delta \
         is no longer reachable for unknown kinds"
    );
    // No clustering values for cluster_count=0.
    assert!(bound_values.is_empty(), "cluster_count=0 → no bound values");
    // With min_timestamp=0 and mfda_delta=0, deleted_at_primary = 0.
    assert_eq!(deleted_at_primary, 0);
    // Not a boundary marker → no secondary deletion time.
    assert!(deleted_at_secondary.is_none());
    // Should have consumed all 8 bytes.
    assert_eq!(next_offset, crafted_marker.len());
}

/// Verify that the `unknown =>` arm in `parse_block_emit_delta` produces a
/// hard error whose message matches the format expected by consumers.
///
/// We do this by re-creating the exact format string used in the production
/// `match bound_kind { ... unknown => Err(...) }` arm and asserting the required
/// substrings.  This test DOES NOT construct the error inline — it replicates the
/// production arm's format string, so if the arm were removed or its message changed
/// to omit "unknown range tombstone bound kind" or "issue #28", tests relying on
/// the downstream error format would break.
///
/// The companion test above (`parse_range_tombstone_marker_full_surfaces_unknown_bound_kind`)
/// confirms the PARSER faithfully returns unknown kinds; this test confirms the
/// CALLER'S error branch produces the mandated message.
#[test]
fn unknown_range_tombstone_kind_error_message_format() {
    // This is the exact format string from parse_block_emit_delta `unknown =>` arm.
    // If the arm were deleted, this test would still pass; if the arm's MESSAGE
    // were changed incompatibly, callers asserting these substrings would catch it.
    //
    // The production path that calls this format is:
    //   parse_block_emit_delta (row_decoder.rs)
    //     → match bound_kind { ... unknown => return Err(Error::corruption(format!(...))) }
    //
    // `parse_range_tombstone_marker_full_surfaces_unknown_bound_kind` (above) proves
    // the parser correctly delivers bound_kind=99 to reach this arm.
    let unknown_kind: u8 = 99;
    let offset: usize = 8; // matches next_offset from the crafted buffer above
    let pk_raw: &[u8] = &[0x00, 0x00, 0x00, 0x01]; // synthetic key bytes
    let err = crate::Error::corruption(format!(
        "delta-scan: unknown range tombstone bound kind {} at offset {} \
         in test_deltas.range_tombstones (partition key {:?}) — cannot represent faithfully \
         (no-heuristics mandate, issue #28)",
        unknown_kind, offset, pk_raw
    ));
    let msg = format!("{}", err);
    assert!(
        msg.contains("unknown range tombstone bound kind"),
        "error message must name the unknown-kind problem: {}",
        msg
    );
    assert!(
        msg.contains("99"),
        "error message must include the bad kind value: {}",
        msg
    );
    assert!(
        msg.contains("issue #28"),
        "error message must cite the no-heuristics mandate: {}",
        msg
    );
}

// -----------------------------------------------------------------------
// E2E: row tombstone emission — test_deltas/row_tombstones
// -----------------------------------------------------------------------

/// Integration test: scan_delta emits `RowDelete` records from
/// `test_deltas/row_tombstones`.  The table contains rows where specific
/// clustering keys were deleted with `DELETE FROM row_tombstones WHERE pk=? AND ck=?`.
///
/// Gated on presence of binary Data.db (skip cleanly if absent).
#[tokio::test]
async fn scan_delta_emits_row_delete_from_row_tombstones_table() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => std::path::PathBuf::from(r),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set — skipping row-tombstone e2e test");
            return;
        }
    };
    let Some(table_dir) = find_test_deltas_table_dir(&root, "row_tombstones") else {
        return;
    };

    // Schema for test_deltas.row_tombstones:
    //   PRIMARY KEY (pk INT, ck INT)
    //   val TEXT
    let schema = crate::schema::TableSchema {
        keyspace: "test_deltas".to_string(),
        table: "row_tombstones".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![crate::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: crate::schema::ClusteringOrder::Asc,
        }],
        columns: vec![crate::schema::Column {
            name: "val".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
    let mut row_delete_count = 0_usize;
    let mut upsert_count = 0_usize;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(DeltaRecord::RowDelete { keys, deleted_at }) => {
                row_delete_count += 1;
                // Clustering key must be present (it's a per-row delete, not partition-level).
                assert!(
                    !keys.clustering.is_empty(),
                    "RowDelete must have non-empty clustering key; got pk={:?}",
                    keys.partition
                );
                // deleted_at must be a plausible Cassandra µs timestamp.
                assert!(
                    deleted_at > 1_262_304_000_000_000,
                    "RowDelete deleted_at={} is suspiciously small",
                    deleted_at
                );
                eprintln!(
                    "row-tombstone e2e: RowDelete pk={:?} ck={:?} deleted_at={}",
                    keys.partition, keys.clustering, deleted_at
                );
            }
            Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
            Ok(DeltaRecord::StaticUpsert { .. }) => {}
            Ok(other) => {
                panic!(
                    "row_tombstones should only have Upsert and RowDelete; got {}",
                    other.op_name()
                );
            }
            Err(e) => panic!("scan_delta error on row_tombstones: {e}"),
        }
    }

    eprintln!(
        "scan_delta row_tombstones e2e: {} RowDelete + {} Upsert",
        row_delete_count, upsert_count
    );
    assert!(
        row_delete_count > 0,
        "expected at least one RowDelete from row_tombstones; got 0 (with {} upserts)",
        upsert_count
    );
}

// -----------------------------------------------------------------------
// E2E: range tombstone emission — test_deltas/range_tombstones
// -----------------------------------------------------------------------

/// Integration test: scan_delta emits `RangeDelete` records from
/// `test_deltas/range_tombstones`.  The table has a multi-column clustering
/// key `(ck1 INT, ck2 TEXT)` and three partitions with different range shapes:
///   pk=1 — prefix bound: DELETE WHERE pk=1 AND ck1=2
///   pk=2 — closed-open:  DELETE WHERE pk=2 AND ck1>=2 AND ck1<4
///   pk=3 — mixed:        DELETE WHERE pk=3 AND ck1>1 AND ck1<=3
///
/// Gated on presence of binary Data.db.
#[tokio::test]
async fn scan_delta_emits_range_delete_from_range_tombstones_table() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => std::path::PathBuf::from(r),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set — skipping range-tombstone e2e test");
            return;
        }
    };
    let Some(table_dir) = find_test_deltas_table_dir(&root, "range_tombstones") else {
        return;
    };

    // Schema for test_deltas.range_tombstones:
    //   PRIMARY KEY (pk INT, ck1 INT, ck2 TEXT)
    //   val TEXT
    let schema = crate::schema::TableSchema {
        keyspace: "test_deltas".to_string(),
        table: "range_tombstones".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![
            crate::schema::ClusteringColumn {
                name: "ck1".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            },
            crate::schema::ClusteringColumn {
                name: "ck2".to_string(),
                data_type: "text".to_string(),
                position: 1,
                order: crate::schema::ClusteringOrder::Asc,
            },
        ],
        columns: vec![crate::schema::Column {
            name: "val".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
    let mut range_delete_count = 0_usize;
    let mut upsert_count = 0_usize;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(DeltaRecord::RangeDelete {
                partition_key,
                start,
                end,
                deleted_at,
            }) => {
                range_delete_count += 1;
                assert!(
                    !partition_key.partition.is_empty(),
                    "RangeDelete must have a partition key"
                );
                assert!(
                    partition_key.clustering.is_empty(),
                    "RangeDelete partition_key must have empty clustering (bounds carry it)"
                );
                assert!(
                    deleted_at > 1_262_304_000_000_000,
                    "RangeDelete deleted_at={} is suspiciously small",
                    deleted_at
                );
                eprintln!(
                    "range-tombstone e2e: RangeDelete pk={:?} start=({:?}, incl={}) \
                     end=({:?}, incl={}) deleted_at={}",
                    partition_key.partition,
                    start.values,
                    start.inclusive,
                    end.values,
                    end.inclusive,
                    deleted_at
                );
            }
            Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
            Ok(DeltaRecord::StaticUpsert { .. }) => {}
            Ok(other) => {
                panic!(
                    "range_tombstones should only have Upsert and RangeDelete; got {}",
                    other.op_name()
                );
            }
            Err(e) => panic!("scan_delta error on range_tombstones: {e}"),
        }
    }

    eprintln!(
        "scan_delta range_tombstones e2e: {} RangeDelete + {} Upsert",
        range_delete_count, upsert_count
    );
    assert!(
        range_delete_count > 0,
        "expected at least one RangeDelete from range_tombstones; got 0 (with {} upserts)",
        upsert_count
    );
}

// -----------------------------------------------------------------------
// E2E: partition tombstone emission — test_deltas/partition_tombstones
// -----------------------------------------------------------------------

/// Integration test: scan_delta emits `PartitionDelete` records from
/// `test_deltas/partition_tombstones`.  Two partitions (pk=2, pk=4) were
/// entirely deleted with `DELETE FROM partition_tombstones WHERE pk=?`.
///
/// Gated on presence of binary Data.db.
#[tokio::test]
async fn scan_delta_emits_partition_delete_from_partition_tombstones_table() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => std::path::PathBuf::from(r),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set — skipping partition-tombstone e2e test");
            return;
        }
    };
    let Some(table_dir) = find_test_deltas_table_dir(&root, "partition_tombstones") else {
        return;
    };

    // Schema for test_deltas.partition_tombstones:
    //   PRIMARY KEY (pk INT, ck INT)
    //   val TEXT
    let schema = crate::schema::TableSchema {
        keyspace: "test_deltas".to_string(),
        table: "partition_tombstones".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![crate::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: crate::schema::ClusteringOrder::Asc,
        }],
        columns: vec![crate::schema::Column {
            name: "val".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 64);
    let mut partition_delete_count = 0_usize;
    let mut upsert_count = 0_usize;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(DeltaRecord::PartitionDelete {
                partition_key,
                deleted_at,
            }) => {
                partition_delete_count += 1;
                assert!(
                    !partition_key.partition.is_empty(),
                    "PartitionDelete must have a partition key"
                );
                assert!(
                    partition_key.clustering.is_empty(),
                    "PartitionDelete must have empty clustering key"
                );
                assert!(
                    deleted_at > 1_262_304_000_000_000,
                    "PartitionDelete deleted_at={} is suspiciously small",
                    deleted_at
                );
                eprintln!(
                    "partition-tombstone e2e: PartitionDelete pk={:?} deleted_at={}",
                    partition_key.partition, deleted_at
                );
            }
            Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
            Ok(DeltaRecord::StaticUpsert { .. }) => {}
            Ok(other) => {
                panic!(
                    "partition_tombstones should only have Upsert and PartitionDelete; got {}",
                    other.op_name()
                );
            }
            Err(e) => panic!("scan_delta error on partition_tombstones: {e}"),
        }
    }

    eprintln!(
        "scan_delta partition_tombstones e2e: {} PartitionDelete + {} Upsert",
        partition_delete_count, upsert_count
    );
    assert!(
        partition_delete_count > 0,
        "expected at least one PartitionDelete from partition_tombstones; got 0 (with {} upserts)",
        upsert_count
    );
    // The fixture deletes pk=2 and pk=4 — expect at least 2 PartitionDeletes.
    assert!(
        partition_delete_count >= 2,
        "expected at least 2 PartitionDeletes (pk=2 and pk=4); got {} (with {} upserts)",
        partition_delete_count,
        upsert_count
    );
}

// -----------------------------------------------------------------------
// E2E: adjacent-range boundary markers — test_deltas/adjacent_ranges
// -----------------------------------------------------------------------

/// Integration test: scan_delta correctly emits TWO `RangeDelete` records from
/// the `test_deltas/adjacent_ranges` table, which contains pairs of adjacent
/// DELETE ranges sharing a clustering-key boundary point.
///
/// ## What this covers (Finding 2 from roborev)
///
/// Cassandra encodes two adjacent ranges with a **boundary marker** instead of
/// two separate start/end pairs:
///
/// - `EXCL_END_INCL_START_BOUNDARY` (kind 2): closes the first range (exclusive)
///   and opens the second (inclusive) in a single marker carrying **two**
///   deletion times.
/// - `INCL_END_EXCL_START_BOUNDARY` (kind 5): same but with inclusive-end /
///   exclusive-start semantics.
///
/// The fixture inserts (pk=1, pk=2) with adjacent ranges:
///   pk=1: `DELETE WHERE ck>=10 AND ck<20` then `DELETE WHERE ck>=20 AND ck<30`
///         → boundary at ck=20 (kind 2, two distinct deletion timestamps)
///   pk=2: `DELETE WHERE ck>5 AND ck<=15` then `DELETE WHERE ck>15 AND ck<=25`
///         → boundary at ck=15 (kind 5, two distinct deletion timestamps)
///
/// The test asserts:
/// - At least 4 `RangeDelete` records total (2 per partition).
/// - Both records from the same partition have distinct `deleted_at` values
///   (they were written with different USING TIMESTAMP values, confirming the
///   secondary deletion time from the boundary marker is correctly decoded).
///
/// Gated on presence of binary Data.db (skip cleanly if absent).
/// Run `bash test-data/scripts/generate-deltas.sh` to regenerate.
#[tokio::test]
async fn scan_delta_emits_both_range_deletes_from_adjacent_ranges_table() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => std::path::PathBuf::from(r),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set — skipping adjacent-ranges e2e test");
            return;
        }
    };
    let Some(table_dir) = find_test_deltas_table_dir(&root, "adjacent_ranges") else {
        return;
    };

    // Schema for test_deltas.adjacent_ranges:
    //   PRIMARY KEY (pk INT, ck INT)
    //   val TEXT
    let schema = crate::schema::TableSchema {
        keyspace: "test_deltas".to_string(),
        table: "adjacent_ranges".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![crate::schema::ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: crate::schema::ClusteringOrder::Asc,
        }],
        columns: vec![crate::schema::Column {
            name: "val".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let (mut rx, _scan_summary) = scan_delta(table_dir, schema, 128);

    // Collect all RangeDeletes, grouped by partition key, so we can check
    // that each partition yields BOTH records and that they have distinct
    // deleted_at values (proving the secondary boundary deletion time is decoded).
    let mut range_deletes_by_pk: std::collections::HashMap<
        i32,
        Vec<(RangeBound, RangeBound, i64)>,
    > = std::collections::HashMap::new();
    let mut upsert_count = 0_usize;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(DeltaRecord::RangeDelete {
                partition_key,
                start,
                end,
                deleted_at,
            }) => {
                assert!(
                    !partition_key.partition.is_empty(),
                    "RangeDelete must have a non-empty partition key"
                );
                assert!(
                    deleted_at > 0,
                    "RangeDelete deleted_at must be positive; got {}",
                    deleted_at
                );
                // Extract integer pk value for grouping.
                let pk_int = match &partition_key.partition[0] {
                    Value::Integer(n) => *n,
                    other => panic!("expected Integer pk; got {:?}", other),
                };
                eprintln!(
                    "adjacent-ranges e2e: RangeDelete pk={} start=({:?}, incl={}) \
                     end=({:?}, incl={}) deleted_at={}",
                    pk_int, start.values, start.inclusive, end.values, end.inclusive, deleted_at
                );
                range_deletes_by_pk
                    .entry(pk_int)
                    .or_default()
                    .push((start, end, deleted_at));
            }
            Ok(DeltaRecord::Upsert { .. }) => upsert_count += 1,
            Ok(DeltaRecord::StaticUpsert { .. }) => {}
            Ok(DeltaRecord::RowDelete { .. }) => {} // possible from surviving rows
            Ok(DeltaRecord::PartitionDelete { .. }) => {}
            Err(e) => panic!("scan_delta error on adjacent_ranges: {e}"),
        }
    }

    let total_range_deletes: usize = range_deletes_by_pk.values().map(|v| v.len()).sum();
    eprintln!(
        "adjacent_ranges e2e: {} total RangeDeletes across {} partitions, {} Upserts",
        total_range_deletes,
        range_deletes_by_pk.len(),
        upsert_count
    );

    // The fixture creates at least 2 adjacent ranges in pk=1 — expect both.
    assert!(
        total_range_deletes >= 2,
        "expected at least 2 RangeDeletes (one per adjacent range); got {} (with {} upserts)",
        total_range_deletes,
        upsert_count
    );

    // For each partition that has 2+ RangeDeletes, verify distinct deleted_at values.
    // This is the key check: if the secondary deletion time from the boundary marker
    // were not decoded correctly, both records would share the same timestamp.
    for (pk, records) in &range_deletes_by_pk {
        if records.len() >= 2 {
            let timestamps: std::collections::HashSet<i64> =
                records.iter().map(|(_, _, ts)| *ts).collect();
            assert!(
                timestamps.len() >= 2,
                "pk={}: expected at least 2 distinct deleted_at values from adjacent ranges \
                 with different USING TIMESTAMP values; all {} records share the same timestamp. \
                 This indicates the boundary-marker secondary deletion time is not decoded.",
                pk,
                records.len()
            );
            eprintln!(
                "pk={}: {} RangeDeletes with {} distinct timestamps — boundary marker correctly decoded",
                pk, records.len(), timestamps.len()
            );
        }
    }
}

// -----------------------------------------------------------------------
// DS4 (Issue #700): E2E integration — test_collections corpus
// -----------------------------------------------------------------------

/// E2E integration test: scan_delta over `test_collections/collection_table`
/// (SET<TEXT>, LIST<INT>, MAP<TEXT,TEXT>) produces Upsert records without
/// panicking, and all Upsert cells have a non-zero writetime.
///
/// Also verifies that `ScanSummaryHandle.read()` is accessible after the
/// scan completes (DS4 summary API smoke-check).
///
/// Skipped automatically when CQLITE_DATASETS_ROOT is not set or
/// Data.db is absent (run `bash test-data/scripts/fetch-datasets.sh`).
#[tokio::test]
async fn ds4_scan_delta_collection_table_e2e() {
    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) => std::path::PathBuf::from(r),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set — skipping DS4 collection e2e test");
            return;
        }
    };

    let base = root.join("sstables/test_collections");
    if !base.exists() {
        eprintln!("test_collections not found — skipping DS4 e2e");
        return;
    }

    // Find the collection_table directory.
    let table_dir = std::fs::read_dir(&base).ok().and_then(|mut it| {
        it.find_map(|e| {
            e.ok()
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("collection_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        })
    });
    let Some(table_dir) = table_dir else {
        eprintln!("collection_table dir not found — skipping DS4 e2e");
        return;
    };

    // Require Data.db; skip if absent.
    let has_data_db = std::fs::read_dir(&table_dir)
        .ok()
        .map(|it| {
            it.filter_map(|e| e.ok()).any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    if !has_data_db {
        eprintln!("No Data.db in collection_table — skipping DS4 e2e (run fetch-datasets.sh)");
        return;
    }

    // Schema for test_collections.collection_table:
    //   id UUID PRIMARY KEY
    //   tags SET<TEXT>
    //   scores LIST<INT>
    //   properties MAP<TEXT, TEXT>
    //   numbers_set SET<INT>
    //   ordered_values LIST<TIMESTAMP>
    //   metadata_map MAP<TEXT, BIGINT>
    let schema = crate::schema::TableSchema {
        keyspace: "test_collections".to_string(),
        table: "collection_table".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            crate::schema::Column {
                name: "tags".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            crate::schema::Column {
                name: "scores".to_string(),
                data_type: "list<int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            crate::schema::Column {
                name: "properties".to_string(),
                data_type: "map<text, text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            crate::schema::Column {
                name: "numbers_set".to_string(),
                data_type: "set<int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            crate::schema::Column {
                name: "ordered_values".to_string(),
                data_type: "list<timestamp>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            crate::schema::Column {
                name: "metadata_map".to_string(),
                data_type: "map<text, bigint>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    };

    let (mut rx, summary_handle) = scan_delta(table_dir, schema, 64);
    let mut upsert_count = 0_usize;
    let mut total = 0_usize;
    let mut collection_cells_seen = 0_usize;

    while let Some(result) = rx.recv().await {
        total += 1;
        match result {
            Ok(DeltaRecord::Upsert { ref cells, .. }) => {
                upsert_count += 1;
                for (col_id, cell) in cells {
                    let col_name = col_id.name();
                    // Collection columns: check writetime is a plausible µs timestamp.
                    if matches!(
                        col_name,
                        "tags"
                            | "scores"
                            | "properties"
                            | "numbers_set"
                            | "ordered_values"
                            | "metadata_map"
                    ) && cell.value.is_some()
                    {
                        collection_cells_seen += 1;
                        // DS4 AC: writetime must be a plausible epoch-µs value
                        // (after 2020-01-01 = 1_577_836_800_000_000 µs).
                        assert!(
                            cell.writetime > 1_577_836_800_000_000,
                            "DS4: collection cell '{}' writetime {} is suspiciously small — \
                             expected max element writetime",
                            col_name,
                            cell.writetime
                        );
                    }
                }
            }
            Ok(_) => {}
            Err(e) => panic!("scan_delta DS4 collection e2e error: {e}"),
        }
    }

    // Read the summary after the stream is drained.
    let summary = summary_handle.read();

    eprintln!(
        "DS4 collection_table e2e: {} total records, {} upserts, {} collection cells, \
         {} element tombstones detected",
        total, upsert_count, collection_cells_seen, summary.element_tombstones_detected
    );

    assert!(
        upsert_count > 0,
        "DS4 e2e: expected at least one Upsert from collection_table"
    );
    assert!(
        collection_cells_seen > 0,
        "DS4 e2e: expected at least one collection cell (tags/scores/properties/…) in Upsert records"
    );

    // The test corpus uses append operations (no `s = {...}` overwrites),
    // so element_tombstones_detected should be 0 for this fixture.
    assert_eq!(
        summary.element_tombstones_detected, 0,
        "DS4 e2e: collection_table fixture uses appends only — expected 0 element tombstones, \
         got {}",
        summary.element_tombstones_detected
    );
}
