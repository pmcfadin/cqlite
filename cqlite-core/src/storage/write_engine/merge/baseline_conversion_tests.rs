//! `MergeEntry` to mutation conversion and baseline-prep tests (split out of `merge/mod.rs`
//! `tests`, #4658).

use super::*;
use crate::storage::write_engine::mutation::DecoratedKey;

#[test]
fn test_merge_step_variants() {
    let key = DecoratedKey::new(100, vec![1, 2, 3]);
    let rows = vec![];

    let partition_step = MergeStep::Partition { key, rows };

    match partition_step {
        MergeStep::Partition { key, rows } => {
            assert_eq!(key.token, 100);
            assert_eq!(rows.len(), 0);
        }
        _ => panic!("Expected Partition variant"),
    }

    let complete_step = MergeStep::Complete;
    match complete_step {
        MergeStep::Complete => {}
        _ => panic!("Expected Complete variant"),
    }
}

#[test]
fn test_cell_merge_last_write_wins_higher_timestamp() {
    // Two cells with different timestamps
    let cell1 = CellData {
        column: "name".to_string(),
        value: Value::text("Old".to_string()),
        timestamp: 1000,
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };

    let cell2 = CellData {
        column: "name".to_string(),
        value: Value::text("New".to_string()),
        timestamp: 2000, // Higher timestamp wins
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };

    // Cell2 should win in last-write-wins merge
    assert!(cell2.timestamp > cell1.timestamp);
}

#[test]
fn test_memory_budget_calculation() {
    // For k=10 SSTables, memory budget should be ~80KB
    let k = 10;
    let buffer_size_per_run = RunReader::DEFAULT_BUFFER_SIZE;
    let total_memory = k * buffer_size_per_run;

    assert_eq!(buffer_size_per_run, 8 * 1024); // 8KB
    assert_eq!(total_memory, 80 * 1024); // 80KB total
}

#[test]
fn test_merge_entry_to_mutation_live_cells() {
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::{CellOperation, DecoratedKey};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // Encode key as 4-byte big-endian int (42)
    let key_bytes = 42i32.to_be_bytes().to_vec();

    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(1000, key_bytes),
        None,
        999_000_000,
        RowData::Live {
            cells: vec![
                CellData {
                    column: "name".to_string(),
                    value: Value::text("Alice".to_string()),
                    timestamp: 999_000_000,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                },
                CellData {
                    column: "age".to_string(),
                    value: Value::Integer(30),
                    timestamp: 999_000_000,
                    ttl: Some(3600),
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                },
            ],
        },
    );

    let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(entry, &schema)
        .expect("conversion should succeed");

    // Partition key should have one column named "id"
    assert_eq!(mutation.partition_key.columns.len(), 1);
    assert_eq!(mutation.partition_key.columns[0].0, "id");

    // Two operations: one Write and one WriteWithTtl
    assert_eq!(mutation.operations.len(), 2);
    assert_eq!(mutation.timestamp_micros, 999_000_000);

    let has_write = mutation
        .operations
        .iter()
        .any(|op| matches!(op, CellOperation::Write { column, .. } if column == "name"));
    let has_ttl_write = mutation.operations.iter().any(|op| {
        matches!(op, CellOperation::WriteWithTtl { column, ttl_seconds, .. }
                if column == "age" && *ttl_seconds == 3600)
    });
    assert!(has_write, "Expected Write operation for 'name'");
    assert!(has_ttl_write, "Expected WriteWithTtl operation for 'age'");
}

/// Issue #1018: a reconciled row whose surviving cells carry DIFFERENT write
/// timestamps must preserve each cell's OWN writetime through the
/// merge→mutation step. The row's `timestamp_micros` is the MAX surviving
/// timestamp; cells whose timestamp differs from that max must be recorded in
/// `Mutation::cell_write_timestamps` so the writer does not promote them to
/// the row max. Cells at the row max are NOT recorded (they correctly inherit
/// the row timestamp), keeping the side-channel empty for single-writetime
/// rows.
#[test]
fn merge_entry_preserves_per_cell_write_timestamps() {
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::DecoratedKey;
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let cell = |column: &str, ts: i64| CellData {
        column: column.to_string(),
        value: Value::text(column.to_string()),
        timestamp: ts,
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };

    // Row max writetime is 300 (the `late` cell); `early`@100 is a live
    // sibling that must keep its own writetime.
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(1000, 42i32.to_be_bytes().to_vec()),
        None,
        300,
        RowData::Live {
            cells: vec![cell("early", 100), cell("late", 300)],
        },
    );

    let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(entry, &schema)
        .expect("conversion should succeed");

    assert_eq!(mutation.timestamp_micros, 300);
    let cwt = mutation
        .cell_write_timestamps
        .as_ref()
        .expect("per-cell write timestamps must be recorded for a mixed-writetime row");
    // Only the cell whose ts differs from the row max is recorded.
    assert_eq!(cwt.get("early"), Some(&100));
    assert_eq!(cwt.get("late"), None);
    // The lookup helper falls back to the row timestamp for unrecorded cells.
    assert_eq!(mutation.cell_write_timestamp("early"), 100);
    assert_eq!(mutation.cell_write_timestamp("late"), 300);
}

/// Issue #1018: a single-writetime row leaves the per-cell side-channel unset
/// (zero behavior change for the common case).
#[test]
fn merge_entry_single_writetime_row_has_no_per_cell_overrides() {
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::DecoratedKey;
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let cell = |column: &str| CellData {
        column: column.to_string(),
        value: Value::text(column.to_string()),
        timestamp: 555,
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };

    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(1000, 7i32.to_be_bytes().to_vec()),
        None,
        555,
        RowData::Live {
            cells: vec![cell("a"), cell("b")],
        },
    );

    let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(entry, &schema)
        .expect("conversion should succeed");
    assert!(
        mutation.cell_write_timestamps.is_none(),
        "single-writetime row must not record any per-cell overrides"
    );
    assert_eq!(mutation.cell_write_timestamp("a"), 555);
}

#[test]
fn test_merge_entry_to_mutation_tombstone() {
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::{CellOperation, DecoratedKey};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let key_bytes = 7i32.to_be_bytes().to_vec();

    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(500, key_bytes),
        None,
        888_000_000,
        RowData::Tombstone {
            deletion_time: 888_000_000,
            local_deletion_time: 1_700_000_000,
        },
    );

    let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(entry, &schema)
        .expect("conversion should succeed");

    assert_eq!(mutation.operations.len(), 1);
    assert!(
        matches!(mutation.operations[0], CellOperation::DeleteRow),
        "Expected DeleteRow operation for tombstone entry"
    );
}

/// Regression (#853/#886 branch-review, Finding 2): compaction baseline
/// seeding must NOT discard a far-future local-deletion-time in [2^31, 2^32).
/// Such LDTs are legitimate (negative i32 bit patterns) after the deletion
/// marker fixes (#853 / range tombstones). The parser reconstructs them as i64
/// values ABOVE i32::MAX, and the old `ldt < i32::MAX` guard dropped them, so
/// the seeded `DataWriter` baseline diverged from the final Statistics.db
/// baseline. The fix normalizes as unsigned-32 and reinterprets the bits as
/// i32.
///
/// End-to-end: write a Statistics.db whose `min_local_deletion_time` is a
/// far-future value, then assert `compute_baseline_min` seeds that exact i32
/// bit pattern (round-tripping through both the writer's i32->u32 header
/// encoding and the parser).
#[test]
fn compute_baseline_min_keeps_far_future_ldt() {
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("temp dir");
    // compute_baseline_min derives the Statistics.db path from a Data.db path.
    let data_path = tmp.path().join("nb-1-big-Data.db");
    std::fs::write(&data_path, b"").expect("touch Data.db");
    let stats_path = tmp.path().join("nb-1-big-Statistics.db");

    // Far-future LDT in [2^31, 2^32): 2^31 + 5, a negative i32 bit pattern.
    let far_future_bits: i32 = ((1u32 << 31) + 5) as i32;
    assert!(
        far_future_bits < 0,
        "sanity: far-future LDT is negative i32"
    );

    let mut meta = StatisticsMetadata::new();
    // A REAL far-future tombstone: drive it through `update_local_deletion_time`
    // (not a bare field assignment) so BOTH the min AND the tombstone histogram are
    // populated — the authoritative "this SSTable has a tombstone" signal the
    // EncodingStats no-deletion sentinel now keys on (#1410).
    meta.update_local_deletion_time(far_future_bits);
    StatisticsWriter::new(stats_path)
        .write(&meta, None)
        .expect("write Statistics.db with far-future LDT");

    let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
    assert_eq!(
        baseline_ldt, far_future_bits,
        "far-future LDT baseline must round-trip as its i32 bit pattern, not be dropped"
    );
}

/// The live/no-deletion sentinel (i32::MAX, DeletionTime.LIVE) must NOT lower
/// the seeded baseline: a Statistics.db with no tombstones leaves the baseline
/// at i32::MAX.
#[test]
fn compute_baseline_min_skips_live_sentinel() {
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("temp dir");
    let data_path = tmp.path().join("nb-1-big-Data.db");
    std::fs::write(&data_path, b"").expect("touch Data.db");
    let stats_path = tmp.path().join("nb-1-big-Statistics.db");

    // i32::MAX = no deletions (DeletionTime.LIVE sentinel).
    let mut meta = StatisticsMetadata::new();
    meta.min_local_deletion_time = i32::MAX;
    StatisticsWriter::new(stats_path)
        .write(&meta, None)
        .expect("write Statistics.db with LIVE sentinel");

    let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
    assert_eq!(
        baseline_ldt,
        i32::MAX,
        "live/no-deletion sentinel must not lower the seeded baseline"
    );
}

/// #1410: a LIVE-ONLY input (no tombstones recorded → EMPTY tombstone histogram)
/// must NOT drag the merged LDT baseline down. `compute_baseline_min` excludes it
/// via the authoritative `tombstone_drop_times` histogram (empty ⇒ no tombstone),
/// matching `EncodingStats.merge`, which treats a no-deletion input as the merge
/// identity. When a live-only input is mixed with a tombstone-carrying input (a
/// real wall-clock LDT), the seeded baseline must equal the TOMBSTONE's LDT.
/// Regressing this re-introduces the #1410 wrong-delta (raw LDT vs `LDT - minLDT`).
#[test]
fn compute_baseline_min_skips_no_deletion_sentinel_mixed_with_tombstone() {
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("temp dir");

    // Live-only input: NO tombstones recorded (empty drop-time histogram).
    let live_data = tmp.path().join("nb-1-big-Data.db");
    std::fs::write(&live_data, b"").expect("touch live Data.db");
    let live_meta = StatisticsMetadata::new(); // no update_local_deletion_time call
    StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
        .write(&live_meta, None)
        .expect("write live-only Statistics.db");

    // Tombstone-carrying input: a real wall-clock LDT, driven through the
    // authentic `update_local_deletion_time` path (populates min AND histogram).
    let tomb_ldt: i32 = 1_782_950_059; // 2026-07-01 (matches the #1387 fixture LDT).
    let tomb_data = tmp.path().join("nb-2-big-Data.db");
    std::fs::write(&tomb_data, b"").expect("touch tombstone Data.db");
    let mut tomb_meta = StatisticsMetadata::new();
    tomb_meta.update_local_deletion_time(tomb_ldt);
    StatisticsWriter::new(tmp.path().join("nb-2-big-Statistics.db"))
        .write(&tomb_meta, None)
        .expect("write tombstone Statistics.db");

    let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[live_data, tomb_data]);
    assert_eq!(
        baseline_ldt, tomb_ldt,
        "the live-only input's no-deletion sentinel must not lower the baseline \
             below the tombstone input's real LDT (#1410)"
    );
}

/// #1410: an ALL-live compaction (every input a no-deletion sentinel) leaves the
/// LDT baseline at its `i32::MAX` "unseeded" value, so the writer falls back to its
/// own live-sentinel encoding rather than being dragged down.
#[test]
fn compute_baseline_min_all_live_stays_unseeded() {
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("temp dir");
    let data_path = tmp.path().join("nb-1-big-Data.db");
    std::fs::write(&data_path, b"").expect("touch Data.db");
    let meta = StatisticsMetadata::new(); // no tombstones -> no-deletion sentinel
    StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
        .write(&meta, None)
        .expect("write live-only Statistics.db");

    let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
    assert_eq!(
        baseline_ldt,
        i32::MAX,
        "an all-live compaction must leave the LDT baseline unseeded"
    );
}

/// #1410 regression guard: a GENUINE tombstone whose real `localDeletionTime` is a
/// tiny value (e.g. `0`, as an old row tombstone with a sub-second write timestamp
/// produces — the compaction_integration scenario) must be INCLUDED in the LDT
/// baseline, NOT mistaken for the no-deletion sentinel. Driving it through
/// `update_local_deletion_time` populates the tombstone histogram, so
/// `compute_baseline_min` sees a non-empty `tombstone_drop_times` and includes the
/// real `0`, taking the baseline to `0` — otherwise the writer would reject the
/// re-emitted below-baseline LDT.
#[test]
fn compute_baseline_min_includes_genuine_zero_ldt_tombstone() {
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("temp dir");
    let data_path = tmp.path().join("nb-1-big-Data.db");
    std::fs::write(&data_path, b"").expect("touch Data.db");
    let mut meta = StatisticsMetadata::new();
    meta.update_local_deletion_time(0); // real tombstone at LDT 0 (histogram set)
    StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
        .write(&meta, None)
        .expect("write Statistics.db with a genuine LDT-0 tombstone");

    let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
    assert_eq!(
        baseline_ldt, 0,
        "a genuine LDT-0 tombstone must be included in the baseline, not excluded \
             as the no-deletion sentinel (#1410)"
    );
}

/// #1410 (roborev Finding 3): when the STATS-extras histogram decode FAILS
/// (corrupt / version-mismatched STATS component) `compute_baseline_min` must
/// stay CONSERVATIVE and INCLUDE the input's LDT baseline — it must NOT treat an
/// extras PARSE FAILURE like an empty histogram (which would skip a possibly-real
/// LDT baseline and make the writer reject a re-emitted below-baseline tombstone).
///
/// Setup: write a valid tombstone-carrying `Statistics.db`, then corrupt the FIRST
/// 4 bytes of the STATS component (the `estimatedPartitionSize` histogram bucket
/// count) to a negative i32 so `parse_stats_extras` fails with `Corruption`, while
/// the SERIALIZATION_HEADER EncodingStats (reached via its own TOC offset, a LATER
/// component) still decodes. Assert the tombstone LDT is included regardless.
#[test]
fn compute_baseline_min_conservative_when_stats_extras_unparseable() {
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("temp dir");
    let data_path = tmp.path().join("nb-1-big-Data.db");
    std::fs::write(&data_path, b"").expect("touch Data.db");
    let stats_path = tmp.path().join("nb-1-big-Statistics.db");

    let tomb_ldt: i32 = 1_782_950_059; // a real wall-clock tombstone LDT.
    let mut meta = StatisticsMetadata::new();
    meta.update_local_deletion_time(tomb_ldt);
    StatisticsWriter::new(stats_path.clone())
        .write(&meta, None)
        .expect("write tombstone Statistics.db");

    // Locate the STATS component (MetadataType ordinal 2) offset from the TOC and
    // corrupt its first field (estimatedPartitionSize histogram bucket count) to a
    // negative i32 so parse_stats_extras fails, without touching the header offset.
    let mut bytes = std::fs::read(&stats_path).expect("read stats");
    let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut stats_offset = None;
    let mut offsets = Vec::new();
    for i in 0..count {
        let entry = 8 + i * 8; // after count(4)+CRC(4); each entry = type(4)+offset(4)
        let ty = u32::from_be_bytes([
            bytes[entry],
            bytes[entry + 1],
            bytes[entry + 2],
            bytes[entry + 3],
        ]);
        offsets.push(u32::from_be_bytes([
            bytes[entry + 4],
            bytes[entry + 5],
            bytes[entry + 6],
            bytes[entry + 7],
        ]) as usize);
        if ty == 2 {
            stats_offset = Some(u32::from_be_bytes([
                bytes[entry + 4],
                bytes[entry + 5],
                bytes[entry + 6],
                bytes[entry + 7],
            ]) as usize);
        }
    }
    let off = stats_offset.expect("STATS component (type 2) in TOC");
    // Negative bucket count → skip_estimated_histogram returns Corruption.
    bytes[off..off + 4].copy_from_slice(&(-1i32).to_be_bytes());
    // Re-sign the STATS component (its body is followed by a CRC32 of the body, up to the
    // next component or the end of the file): the file now passes the strict #4612 integrity
    // check the generation module applies, so only the STATS-extras decode is undecodable.
    let end = offsets
        .iter()
        .copied()
        .filter(|&o| o > off)
        .min()
        .unwrap_or(bytes.len());
    let crc = crc32fast::hash(&bytes[off..end - 4]);
    bytes[end - 4..end].copy_from_slice(&crc.to_be_bytes());
    std::fs::write(&stats_path, &bytes).expect("rewrite corrupted stats");

    let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
    assert_eq!(
        baseline_ldt, tomb_ldt,
        "an unparseable STATS-extras section must be treated conservatively \
             (INCLUDE the LDT baseline), not as an empty no-tombstone histogram (#1410)"
    );
}

/// #2299 / roborev job 1723 — fail-CLOSED guard for the direct-stream gate.
///
/// `compute_baseline_min` fails OPEN: an input with a MISSING or top-level
/// UNPARSEABLE `Statistics.db` never lowers `baseline_min_ldt`, so a skipped
/// tombstone-bearing input would leave the live sentinel intact and the
/// `#2299` gate (`baseline_min_ldt == i32::MAX`) would wrongly select the
/// direct-stream path, dropping that input's tombstones (data resurrection).
/// `all_input_stats_readable` closes that hole: it returns `false` unless EVERY
/// input's deletion metadata was actually observed, letting the caller force
/// the always-correct buffered path. Removing the AND in `compaction.rs`
/// re-opens the data-loss hole and re-greens this test's negative cases.
#[test]
fn all_input_stats_readable_fails_closed_on_missing_or_unparseable_stats() {
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("temp dir");

    // A well-formed input: Data.db + a valid Statistics.db written through the
    // real writer. Readable → the guard permits proving "no deletions".
    let good_data = tmp.path().join("nb-1-big-Data.db");
    std::fs::write(&good_data, b"").expect("touch good Data.db");
    StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
        .write(&StatisticsMetadata::new(), None)
        .expect("write valid Statistics.db");
    assert!(
        all_input_stats_readable(std::slice::from_ref(&good_data)),
        "a well-formed input with a valid Statistics.db must be readable"
    );

    // Missing Statistics.db (roborev's primary scenario): the paired
    // Statistics.db is simply absent. Must fail closed.
    let missing_data = tmp.path().join("nb-2-big-Data.db");
    std::fs::write(&missing_data, b"").expect("touch missing-stats Data.db");
    // (deliberately write NO nb-2-big-Statistics.db)
    assert!(
        !all_input_stats_readable(std::slice::from_ref(&missing_data)),
        "an input whose Statistics.db is missing must NOT be provably deletion-free"
    );

    // Top-level-unparseable Statistics.db: too short to even carry the TOC
    // count/CRC header, so `parse_statistics_with_fallback` errors. Must fail
    // closed (do not infer "no deletions" from an undecodable component).
    let corrupt_data = tmp.path().join("nb-3-big-Data.db");
    std::fs::write(&corrupt_data, b"").expect("touch corrupt-stats Data.db");
    std::fs::write(tmp.path().join("nb-3-big-Statistics.db"), b"\x00\x00")
        .expect("write truncated Statistics.db");
    assert!(
        !all_input_stats_readable(std::slice::from_ref(&corrupt_data)),
        "an input whose Statistics.db is unparseable at the top level must fail closed"
    );

    // MIXED: one readable input + one missing-stats input. A single unreadable
    // input taints the whole merge — the guard must fail closed so the direct
    // path is never taken when any input's deletion metadata is unknown.
    assert!(
        !all_input_stats_readable(&[good_data, missing_data, corrupt_data]),
        "any single unreadable input must force the whole merge to fail closed"
    );
}

/// #935: `compute_max_purgeable_timestamp` returns the MIN write timestamp
/// across the non-included overlapping SSTables (from each `Statistics.db`
/// `min_timestamp`), so a tombstone older than that bound can be purged.
#[test]
fn compute_max_purgeable_timestamp_returns_min_over_outside_sstables() {
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("temp dir");
    let write_stats = |gen: u32, min_ts: i64| -> PathBuf {
        let data_path = tmp.path().join(format!("nb-{gen}-big-Data.db"));
        std::fs::write(&data_path, b"").expect("touch Data.db");
        let stats_path = tmp.path().join(format!("nb-{gen}-big-Statistics.db"));
        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = min_ts;
        StatisticsWriter::new(stats_path)
            .write(&meta, None)
            .expect("write Statistics.db");
        data_path
    };

    let a = write_stats(1, 5_000);
    let b = write_stats(2, 2_500);
    let c = write_stats(3, 9_000);

    let bound = compute_max_purgeable_timestamp(&[a, b, c]);
    assert_eq!(
        bound,
        Some(2_500),
        "the bound must be the minimum min_timestamp across all outside SSTables"
    );

    // Empty outside set → None (caller treats as full / no overlap).
    assert_eq!(
        compute_max_purgeable_timestamp(&[]),
        None,
        "no outside SSTables means no overlap bound"
    );
}

/// #935: a missing/unreadable outside `Statistics.db` leaves the bound UNKNOWN
/// (returns `None`), disabling overlap-aware purging — never resurrect data.
#[test]
fn compute_max_purgeable_timestamp_unreadable_outside_disables_purging() {
    use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
    use tempfile::TempDir;

    let tmp = TempDir::new().expect("temp dir");
    // One readable outside SSTable.
    let readable = tmp.path().join("nb-1-big-Data.db");
    std::fs::write(&readable, b"").expect("touch Data.db");
    let mut meta = StatisticsMetadata::new();
    meta.min_timestamp = 1_000;
    StatisticsWriter::new(tmp.path().join("nb-1-big-Statistics.db"))
        .write(&meta, None)
        .expect("write Statistics.db");

    // A second outside SSTable whose Statistics.db does NOT exist.
    let missing = tmp.path().join("nb-2-big-Data.db");
    std::fs::write(&missing, b"").expect("touch Data.db");

    assert_eq!(
        compute_max_purgeable_timestamp(&[readable, missing]),
        None,
        "an unreadable outside Statistics.db must disable overlap-aware purging"
    );
}
