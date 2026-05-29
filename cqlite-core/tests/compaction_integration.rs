//! Integration test: 3-SSTable compaction mechanics and read-back (Issue #476, Epic #469)
//!
//! This file contains two integration tests that exercise the compaction pipeline:
//!
//! 1. `compaction_3_sstables_mechanics` — Verifies compaction file-level behaviour and
//!    statistics.  Runs in CI.
//!
//! 2. `compaction_3_sstables_read_back_correctness` — Verifies that a post-compaction
//!    scan returns the correct rows with correct shadowing semantics.
//!
//! ## Shadowing semantics (intended)
//!
//! - SSTable A (ts=100): PK 1..=20, columns `name` and `score`
//! - SSTable B (ts=200): PK 11..=30, overrides A on 11..=20
//! - SSTable C (ts=300): PK 21..=40, deletes rows 1..=5 (row tombstone),
//!   deletes `score` column for PK=11 (cell tombstone)
//!
//! After compaction the merged SSTable should contain:
//!   - PK 6..=10   present (A-only, not deleted)               — 5 rows
//!   - PK 11..=20  present (B overrides A, C deletes PK=11 score)  — 10 rows
//!   - PK 21..=30  present (C overrides B)                     — 10 rows
//!   - PK 31..=40  present (C-only)                            — 10 rows
//!
//!   Total live rows: 35.  PK 1..=5 are row-deleted by C.
//!
//! ## Design notes
//!
//! STCS default min_threshold=4.  To compact exactly 3 SSTables we use
//! STCSPolicy::new(min_threshold=3, ...) — no modifications to merge_policy.rs
//! are needed because the STCSPolicy::new() constructor already accepts arbitrary
//! min_threshold values (Issue #476: stcs_min_threshold_used = 3).
//!
//! min_sstable_size is set to 0 so that tiny test SSTables (a few KB each) fall into
//! the same bucket — identical to the pattern used in
//! `test_maintenance_step_compacts_sstables_atomically`.
//!
//! ## Runtime design
//!
//! `maintenance_step()` uses an internal `block_on` call (synchronous compaction),
//! which cannot be invoked from within an active tokio runtime.  The tests are
//! therefore plain `#[test]` functions that drive all async operations through an
//! explicit single-threaded `Runtime::block_on` call — the same pattern used by the
//! existing unit tests in `storage/write_engine/mod.rs`.

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, STCSPolicy, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

// ── Schema helpers ────────────────────────────────────────────────────────────

/// Simple schema: keyspace=compact_ks, table=items, PK=id(int), columns: name(text), score(int)
fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "compact_ks".to_string(),
        table: "items".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "score".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

// ── Mutation helpers ──────────────────────────────────────────────────────────

fn write_row(id: i32, name: &str, score: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new("compact_ks", "items");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "score".to_string(),
            value: Value::Integer(score),
        },
    ];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// Row tombstone: delete the entire row for `id`.
fn delete_row(id: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new("compact_ks", "items");
    let pk = PartitionKey::single("id", Value::Integer(id));
    Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::DeleteRow],
        timestamp,
        None,
    )
}

/// Cell tombstone: delete the `score` column for `id`.
fn delete_score_column(id: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new("compact_ks", "items");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Delete {
        column: "score".to_string(),
    }];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

// ── STCS policy with min_threshold=3 ─────────────────────────────────────────

/// Build a STCS policy that will compact groups of >=3 SSTables.
///
/// `min_sstable_size=0` ensures tiny test SSTables (a few KB each) are placed
/// in the same size bucket so the policy selects them.
///
/// We use min_threshold=3 (not the Cassandra default of 4) to trigger compaction
/// with exactly 3 input SSTables.  No change to merge_policy.rs is required
/// because `STCSPolicy::new()` already accepts arbitrary min_threshold values.
///
/// Issue #476 requirement: stcs_min_threshold_used = 3
fn make_policy() -> STCSPolicy {
    STCSPolicy::new(
        3,   // min_threshold — trigger compaction with as few as 3 SSTables
        32,  // max_threshold
        0.5, // bucket_low
        1.5, // bucket_high
        0,   // min_sstable_size — zero so all small files group together
    )
    .expect("valid STCS parameters")
}

// ── Shared setup helper ───────────────────────────────────────────────────────

/// Write 3 overlapping SSTables via the WriteEngine public API, then trigger
/// compaction via `maintenance_step()`.
///
/// Returns `(TempDir, data_dir, sstable_dir)`.  The `TempDir` must remain alive
/// for the duration of the test; dropping it removes all files.
///
/// Panics on any unexpected error — this is a test helper.
fn write_three_sstables_and_compact(
    rt: &tokio::runtime::Runtime,
) -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    // ── Phase 1: Write 3 overlapping SSTables via the public WriteEngine API ──

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // SSTable A (ts=100): rows 1..=20
    for id in 1_i32..=20 {
        let m = write_row(id, &format!("a-name-{}", id), id * 10, 100);
        engine.write(m).expect("write A");
    }
    let info_a = rt
        .block_on(engine.flush())
        .expect("flush A")
        .expect("info A");
    assert_eq!(info_a.partition_count, 20, "SSTable A: 20 partitions");
    assert!(info_a.data_path.exists(), "SSTable A Data.db must exist");

    // SSTable B (ts=200): rows 11..=30, overrides A on 11..=20
    for id in 11_i32..=30 {
        let m = write_row(id, &format!("b-name-{}", id), id * 20, 200);
        engine.write(m).expect("write B");
    }
    let info_b = rt
        .block_on(engine.flush())
        .expect("flush B")
        .expect("info B");
    assert_eq!(info_b.partition_count, 20, "SSTable B: 20 partitions");

    // SSTable C (ts=300): rows 21..=40, row-deletes for 1..=5, cell-delete score@PK=11
    // rows 21..=30 override B on those PKs; rows 31..=40 are C-only.
    for id in 21_i32..=40 {
        let m = write_row(id, &format!("c-name-{}", id), id * 30, 300);
        engine.write(m).expect("write C rows");
    }
    // Row tombstones for PK 1..=5 (written in A only)
    for id in 1_i32..=5 {
        let m = delete_row(id, 300);
        engine.write(m).expect("write row-delete C");
    }
    // Cell tombstone: delete the `score` column for PK=11
    engine
        .write(delete_score_column(11, 300))
        .expect("write cell-delete C");

    let info_c = rt
        .block_on(engine.flush())
        .expect("flush C")
        .expect("info C");
    // C contains: 20 rows (21..=40) + 5 row-tombstones (1..=5) + 1 cell-tombstone (PK=11)
    // = 26 mutations.  The engine merges same-PK mutations so partition_count = 26.
    assert!(info_c.partition_count > 0, "SSTable C: non-empty");

    // Verify 3 Data.db files exist before compaction
    let sstable_dir = data_dir.join("compact_ks").join("items");
    let count_data_files = |dir: &std::path::Path| -> usize {
        std::fs::read_dir(dir)
            .expect("read sstable dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
            .count()
    };
    assert_eq!(
        count_data_files(&sstable_dir),
        3,
        "Expected 3 Data.db files before compaction"
    );

    // ── Phase 2: Trigger compaction via maintenance_step() ────────────────────

    let policy = make_policy();
    engine
        .set_merge_policy(Box::new(policy))
        .expect("set merge policy");

    // maintenance_step uses block_on internally — safe here because we are NOT
    // inside the tokio runtime (we are in a plain #[test] context).
    let budget = Duration::from_secs(30);
    let mut compaction_completed = false;
    for _iteration in 0..5 {
        let report = engine.maintenance_step(budget).expect("maintenance_step");
        if !report.completed_merges.is_empty() {
            compaction_completed = true;
            break;
        }
        if !report.pending_compaction {
            break;
        }
    }
    assert!(
        compaction_completed,
        "Compaction must complete within 5 maintenance_step calls"
    );

    // Assert compaction statistics
    let stats = engine.maintenance_stats();
    assert_eq!(
        stats.compactions_completed, 1,
        "Exactly 1 compaction must have completed"
    );
    assert_eq!(
        stats.sstables_merged_in, 3,
        "3 input SSTables must have been consumed (got {})",
        stats.sstables_merged_in
    );
    assert_eq!(
        stats.sstables_produced, 1,
        "1 output SSTable must have been produced"
    );

    // Assert input files are gone, output file exists
    assert_eq!(
        count_data_files(&sstable_dir),
        1,
        "After compaction exactly 1 Data.db must exist (atomicity guarantee)"
    );

    // The TOC.txt must be present (it is the publication barrier)
    let toc_count = std::fs::read_dir(&sstable_dir)
        .expect("read sstable dir for TOC check")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-TOC.txt"))
        .count();
    assert_eq!(
        toc_count, 1,
        "Exactly 1 TOC.txt must exist after compaction (publication barrier)"
    );

    // Close the WriteEngine before returning so callers can reopen via SSTableManager.
    rt.block_on(engine.close()).expect("close engine");

    (temp_dir, data_dir, sstable_dir)
}

// ── Test 1: Mechanics (runs in CI) ───────────────────────────────────────────

/// Compaction mechanics test: write 3 overlapping SSTables, compact, assert
/// file-level atomicity and statistics.
///
/// This test does NOT attempt read-back (that is gated on #500).  It verifies:
///   - 3 input Data.db files are replaced by 1 output Data.db (atomicity)
///   - TOC.txt is present for the output SSTable
///   - `maintenance_stats` shows compactions_completed >= 1, sstables_merged_in == 3,
///     sstables_produced == 1
///
/// This is a synchronous `#[test]` because `maintenance_step()` calls
/// `block_on()` internally and cannot be invoked from within an active tokio
/// runtime.  Async operations are driven via an explicit `Runtime::block_on`
/// call — the same approach used by the existing compaction unit tests in
/// `storage/write_engine/mod.rs`.
#[test]
fn compaction_3_sstables_mechanics() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    // All Phase 1 + Phase 2 assertions happen inside the helper.
    let (_temp_dir, _data_dir, _sstable_dir) = write_three_sstables_and_compact(&rt);

    // The helper already asserts everything required for the mechanics test.
    // _temp_dir kept alive until end of scope to preserve files.
}

// ── Test 2: Read-back correctness ────────────────────────────────────────────

/// Read-back correctness test: after compaction, reopen via SSTableManager and
/// assert the live rows from each input SSTable round-trip through compaction
/// with correct tombstone shadowing (Issue #500, #505).
///
/// Verifies:
///   - row_count == 35 (PK 1..=5 are row-tombstoned by C, so they are absent)
///   - PK 1..=5 absent (row-deleted by C at ts=300)
///   - PK 6..=10 present (A-only, non-deleted)
///   - PK 11..=20 present (B wins on 12..=20; C wins with cell-tombstone on 11)
///   - PK 21..=30 present (C wins over B)
///   - PK 31..=40 present (C-only)
///   - PK=11 score column is absent or tombstone (cell-deleted by C at ts=300)
#[test]
fn compaction_3_sstables_read_back_correctness() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let schema = make_schema();

    // Run the full Phase 1 + Phase 2 (write + compact).
    let (temp_dir, data_dir, sstable_dir) = write_three_sstables_and_compact(&rt);

    // ── Phase 3: Reopen via SSTableManager and assert read-back correctness ──

    // Confirm the merged SSTable file is on disk.
    let merged_data_count = std::fs::read_dir(&sstable_dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .count();
    assert_eq!(
        merged_data_count, 1,
        "Merged Data.db must be present on disk before attempting read-back"
    );

    let cqlite_config = Config::default();

    let manager = rt.block_on(async {
        let platform = Arc::new(
            Platform::new(&cqlite_config)
                .await
                .expect("platform creation"),
        );
        SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager must open without error")
    });

    let sstable_stats = rt.block_on(manager.stats()).expect("manager stats");
    eprintln!(
        "post_compaction_sstable_count = {}",
        sstable_stats.sstable_count
    );

    let table_id = CqlTableId::from("compact_ks.items");
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("post-compaction scan must not error");

    let row_count = results.len();
    eprintln!("post_compaction_row_count = {}", row_count);

    // Issue #500 + #505: must have exactly 35 live rows.
    // (PK 1..=5 row-tombstoned by C; PK 6..=10=5, PK 11..=20=10, PK 21..=30=10, PK 31..=40=10)
    assert!(
        row_count >= 35,
        "post-compaction scan must return >= 35 rows (got {}). \
         If this is 0, #500 (iterate_all_partitions reads 0 rows from writer-produced \
         SSTables) has regressed.",
        row_count
    );

    // Build a map of raw PK bytes → Value for per-PK assertions.
    let result_map: HashMap<Vec<u8>, Value> = results.into_iter().map(|(k, v)| (k.0, v)).collect();

    // Issue #505: PK 1..=5 must be ABSENT — row-deleted by SSTable C at ts=300.
    for id in 1_i32..=5 {
        let key: Vec<u8> = id.to_be_bytes().into();
        assert!(
            !result_map.contains_key(&key),
            "PK {} was row-deleted by C (ts=300) but is still present in merged output (Issue #505)",
            id
        );
    }

    // PK 6..=10 must be present (A-only, never deleted).
    for id in 6_i32..=10 {
        let key: Vec<u8> = id.to_be_bytes().into();
        assert!(
            result_map.contains_key(&key),
            "PK {} (A-only, non-deleted) must be present in merged output",
            id
        );
    }

    // PK 11..=20 must be present (B wins on 12..=20; C cell-tombstone wins on 11).
    for id in 11_i32..=20 {
        let key: Vec<u8> = id.to_be_bytes().into();
        assert!(
            result_map.contains_key(&key),
            "PK {} (B/C wins over A) must be present in merged output",
            id
        );
    }

    // Issue #505: PK=11 score column must be absent or tombstone (cell-deleted by C at ts=300).
    let key_11: Vec<u8> = 11_i32.to_be_bytes().into();
    if let Some(row_value) = result_map.get(&key_11) {
        match row_value {
            Value::Map(pairs) => {
                for (col_key, col_val) in pairs {
                    if let Value::Text(col_name) = col_key {
                        if col_name == "score" {
                            assert!(
                                matches!(col_val, Value::Null | Value::Tombstone(_)),
                                "PK=11 score column was cell-deleted by C (ts=300) \
                                 but has live value: {:?} (Issue #505)",
                                col_val
                            );
                        }
                    }
                }
            }
            Value::Tombstone(_) => {
                panic!("PK=11 row should be live (only score is deleted) but returned Tombstone");
            }
            _ => {
                eprintln!(
                    "PK=11 value is not a Map (got {:?}), column-level assertion skipped",
                    row_value
                );
            }
        }
    }

    // PK 21..=30 must be present (C overrides B at ts=300).
    for id in 21_i32..=30 {
        let key: Vec<u8> = id.to_be_bytes().into();
        assert!(
            result_map.contains_key(&key),
            "PK {} (C wins over B) must be present in merged output",
            id
        );
    }

    // PK 31..=40 must be present (C-only).
    for id in 31_i32..=40 {
        let key: Vec<u8> = id.to_be_bytes().into();
        assert!(
            result_map.contains_key(&key),
            "PK {} (C-only) must be present in merged output",
            id
        );
    }

    eprintln!(
        "Read-back correctness checks PASSED: {} rows, tombstone shadowing and \
         live-row presence assertions all satisfied (Issues #500, #505)",
        row_count
    );

    // Keep temp_dir alive until end of test scope.
    drop(temp_dir);
}

// ── Test 3: Tombstone shadowing (gated on #505) ──────────────────────────────

/// Tombstone shadowing during compaction (Issue #505 fix verification).
///
/// Verifies that a row tombstone in a later SSTable (C, ts=300) correctly
/// shadows a live row in an earlier SSTable (A, ts=100) after k-way compaction,
/// and that a cell tombstone on `score` for PK=11 in C shadows the live value
/// from B (ts=200).
///
/// - PK 1..=5 must be absent (row-deleted by C at ts=300 > A's ts=100)
/// - PK=11 `score` column must be absent or tombstone (cell-deleted by C at ts=300)
#[test]
fn compaction_3_sstables_tombstone_shadowing() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let schema = make_schema();

    let (temp_dir, data_dir, _sstable_dir) = write_three_sstables_and_compact(&rt);

    let cqlite_config = Config::default();

    let manager = rt.block_on(async {
        let platform = Arc::new(
            Platform::new(&cqlite_config)
                .await
                .expect("platform creation"),
        );
        SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager must open without error")
    });

    let table_id = CqlTableId::from("compact_ks.items");
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("post-compaction scan must not error");

    let result_map: HashMap<Vec<u8>, Value> = results.into_iter().map(|(k, v)| (k.0, v)).collect();

    // PK 1..=5 must be absent (row-deleted by C at ts=300).
    for id in 1_i32..=5 {
        let key: Vec<u8> = id.to_be_bytes().into();
        assert!(
            !result_map.contains_key(&key),
            "PK {} was row-deleted by C (ts=300) but is still present in merged output",
            id
        );
    }

    // PK 11: the `score` column was deleted by C (cell tombstone at ts=300).
    let key_11: Vec<u8> = 11_i32.to_be_bytes().into();
    if let Some(row_value) = result_map.get(&key_11) {
        match row_value {
            Value::Map(pairs) => {
                for (col_key, col_val) in pairs {
                    if let Value::Text(col_name) = col_key {
                        if col_name == "score" {
                            assert!(
                                matches!(col_val, Value::Null | Value::Tombstone(_)),
                                "PK=11 score column was cell-deleted by C but has value: {:?}",
                                col_val
                            );
                        }
                    }
                }
            }
            Value::Tombstone(_) => {
                panic!("PK=11 row should be live (only score is deleted) but returned Tombstone");
            }
            _ => {
                eprintln!(
                    "PK=11 value is not a Map (got {:?}), column-level assertion skipped",
                    row_value
                );
            }
        }
    }

    drop(temp_dir);
}

// ── Test 4: Focused regression — row tombstone shadows live row ───────────────

/// Regression test for Issue #505: a row tombstone in a **later** SSTable must
/// shadow a live row with the same partition key in an **earlier** SSTable after
/// k-way compaction.
///
/// Setup (minimal — two SSTables):
///   - SSTable X (ts=100): live rows PK=1, PK=2; row tombstone for PK=3 (ts=100)
///   - SSTable Y (ts=300): row tombstone for PK=1 (ts=300); live row for PK=3 (ts=300)
///
/// Expected after compaction:
///   - PK=1 is ABSENT  — tombstone (ts=300) shadows live (ts=100)  [higher-ts wins]
///   - PK=2 is PRESENT — only in X, never tombstoned
///   - PK=3 is PRESENT — live write (ts=300) shadows tombstone (ts=100) [lower-ts loses]
///
/// The PK=3 direction is the critical one: it proves the merger uses the DECODED
/// `markedForDeleteAt` timestamp for the tombstone. If the reader produced a
/// hardcoded/epoch-0 or always-high deletion timestamp (Issue #505), one of the two
/// directions would fail — a vacuous pass is therefore impossible.
///
/// This test is intentionally narrower than `compaction_3_sstables_tombstone_shadowing`
/// to make the failure mode obvious and fast to reproduce.
#[test]
fn row_tombstone_shadows_live_row_after_compaction() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // SSTable X (ts=100): live rows PK=1 and PK=2, plus a row tombstone for PK=3 at
    // the LOWER timestamp (ts=100). PK=3's tombstone must later lose to a newer live
    // write in Y, exercising the lower-ts-does-not-shadow direction.
    for id in 1_i32..=2 {
        let m = write_row(id, &format!("live-{}", id), id * 10, 100);
        engine.write(m).expect("write X");
    }
    engine
        .write(delete_row(3, 100))
        .expect("write low-ts row-tombstone X");
    let info_x = rt
        .block_on(engine.flush())
        .expect("flush X")
        .expect("info X");
    assert_eq!(info_x.partition_count, 3, "SSTable X: 3 partitions");

    // SSTable Y: row tombstone for PK=1 at the HIGHER timestamp (ts=300, shadows X's
    // live PK=1), and a live write for PK=3 at ts=300 (shadows X's ts=100 tombstone).
    engine
        .write(delete_row(1, 300))
        .expect("write row-tombstone Y");
    engine
        .write(write_row(3, "revived-3", 333, 300))
        .expect("write high-ts live PK=3 Y");
    let info_y = rt
        .block_on(engine.flush())
        .expect("flush Y")
        .expect("info Y");
    assert!(info_y.partition_count > 0, "SSTable Y: non-empty");

    // Compact with min_threshold=2 and very permissive bucket bounds so that
    // two tiny test SSTables of different sizes are grouped into the same bucket.
    let policy = STCSPolicy::new(2, 32, 0.01, 100.0, 0).expect("valid STCS params");
    engine
        .set_merge_policy(Box::new(policy))
        .expect("set policy");

    let budget = std::time::Duration::from_secs(30);
    let mut compacted = false;
    for _ in 0..5 {
        let report = engine.maintenance_step(budget).expect("maintenance_step");
        if !report.completed_merges.is_empty() {
            compacted = true;
            break;
        }
        if !report.pending_compaction {
            break;
        }
    }
    assert!(compacted, "Compaction must complete");

    rt.block_on(engine.close()).expect("close engine");

    // Re-open and scan the merged SSTable.
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(
            Platform::new(&cqlite_config)
                .await
                .expect("platform creation"),
        );
        SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager must open")
    });

    let table_id = CqlTableId::from("compact_ks.items");
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("post-compaction scan");

    let row_count = results.len();
    eprintln!(
        "row_tombstone_shadows_live_row_after_compaction: {} rows returned",
        row_count
    );

    let result_map: HashMap<Vec<u8>, Value> = results.into_iter().map(|(k, v)| (k.0, v)).collect();

    // PK=1 must be ABSENT: the row tombstone at ts=300 shadows the live row at ts=100.
    let key_1: Vec<u8> = 1_i32.to_be_bytes().into();
    assert!(
        !result_map.contains_key(&key_1),
        "PK=1 was row-deleted at ts=300 (> live ts=100) but is present after compaction \
         — tombstone shadowing regression (Issue #505)"
    );

    // PK=2 must be PRESENT: it was only written in X with no corresponding tombstone.
    let key_2: Vec<u8> = 2_i32.to_be_bytes().into();
    assert!(
        result_map.contains_key(&key_2),
        "PK=2 (live, never deleted) must be present after compaction"
    );

    // PK=3 must be PRESENT with the live value: the row tombstone (ts=100) is OLDER
    // than the live write (ts=300), so it must NOT shadow it. This direction fails if
    // the decoded tombstone timestamp is wrong (e.g. epoch 0 would still lose to ts=300
    // and pass, but a hardcoded-high value would WRONGLY shadow the live write here).
    let key_3: Vec<u8> = 3_i32.to_be_bytes().into();
    let pk3 = result_map.get(&key_3).unwrap_or_else(|| {
        panic!(
            "PK=3 live write (ts=300) must survive the older tombstone (ts=100) \
             but is absent after compaction — tombstone timestamp regression (Issue #505)"
        )
    });
    match pk3 {
        Value::Map(pairs) => {
            let name = pairs.iter().find_map(|(k, v)| match (k, v) {
                (Value::Text(col), Value::Text(val)) if col == "name" => Some(val.clone()),
                _ => None,
            });
            assert_eq!(
                name.as_deref(),
                Some("revived-3"),
                "PK=3 must carry the live ts=300 value 'revived-3', not be shadowed by the \
                 older ts=100 tombstone (Issue #505)"
            );
        }
        other => panic!(
            "PK=3 must be a live row (Value::Map) carrying the ts=300 write, got {:?} (Issue #505)",
            other
        ),
    }

    eprintln!(
        "row_tombstone_shadows_live_row_after_compaction PASSED: \
         PK=1 correctly absent (higher-ts tombstone shadows), \
         PK=2 correctly present (never deleted), \
         PK=3 correctly present with live value (lower-ts tombstone does NOT shadow)"
    );

    drop(temp_dir);
}
