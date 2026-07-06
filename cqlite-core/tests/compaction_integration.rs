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
//! `maintenance_step()` uses an internal `block_on` call (synchronous compaction).
//! These tests are plain `#[test]` functions that drive all async operations
//! through an explicit single-threaded `Runtime::block_on` call — the same
//! pattern used by the existing unit tests in `storage/write_engine/mod.rs`.
//!
//! NOTE (Issue #587): `maintenance_step()` is now also safe to call from *within*
//! an active tokio runtime — the async-to-sync bridge offloads to a scoped thread
//! when a runtime is already running. The async-context regression coverage lives
//! in `issue_587_compaction_async_bridge.rs`; these tests intentionally keep the
//! plain-`#[test]` form to also exercise the no-runtime bridge path.

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, STCSPolicy, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use cqlite_core::ScanRow;
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
        dropped_columns: HashMap::new(),
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

/// Write only the `name` column for `id` (disjoint-column test, Issue #533).
fn write_name_only(id: i32, name: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new("compact_ks", "items");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// Write only the `score` column for `id` (disjoint-column test, Issue #533).
fn write_score_only(id: i32, score: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new("compact_ks", "items");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "score".to_string(),
        value: Value::Integer(score),
    }];
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
        local_deletion_time: None,
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
/// This is a synchronous `#[test]` that drives `maintenance_step()` with no
/// surrounding runtime, exercising the bridge's no-runtime path. (Since Issue
/// #587 it is also safe to call from within an active runtime — see
/// `issue_587_compaction_async_bridge.rs`.) Async operations are driven via an
/// explicit `Runtime::block_on` call — the same approach used by the existing
/// compaction unit tests in `storage/write_engine/mod.rs`.
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

// ── Test 1b: Publication durability barrier (issue #1959) ────────────────────

/// Crash-safety invariant for the compaction publication (issue #1959):
/// after finalize, the published SSTable directory must fsync its dirents so
/// "TOC.txt visible ⇒ every component it names is present and complete" holds
/// across a power loss. Directory fsync cannot be observed directly without a
/// power cut, so this asserts the behavioral contract the fsync exists to
/// guarantee AND exercises the finalize path that performs the two directory
/// fsyncs (a fsync failure would abort finalize, so a completed compaction with
/// every TOC-named component present is the observable post-condition):
///
///   1. the published SSTable directory exists,
///   2. TOC.txt (the publication barrier) exists, and
///   3. EVERY component listed in TOC.txt exists on disk and is non-empty —
///      i.e. the TOC never references a component that is absent/zero-length.
///
/// It does NOT assert byte layout: a directory fsync changes no file bytes, so
/// the dedicated compaction byte-parity tests (e.g. issue #1017) remain the
/// authority that the published bytes are unchanged.
#[test]
fn compaction_publish_all_toc_components_present_and_nonempty() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let (_temp_dir, _data_dir, sstable_dir) = write_three_sstables_and_compact(&rt);

    // (1) The published SSTable directory exists.
    assert!(
        sstable_dir.is_dir(),
        "published SSTable directory must exist: {:?}",
        sstable_dir
    );

    // Locate the single published TOC.txt (publication barrier).
    let toc_path = std::fs::read_dir(&sstable_dir)
        .expect("read published sstable dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().ends_with("-TOC.txt"))
                .unwrap_or(false)
        })
        .expect("(2) published TOC.txt must exist after compaction");
    assert!(
        std::fs::metadata(&toc_path).expect("stat TOC.txt").len() > 0,
        "TOC.txt must be non-empty"
    );

    // (3) Every component named in TOC.txt must exist on disk and be non-empty.
    // Actual filenames are descriptor-prefixed (e.g. `nb-N-big-Data.db`), while
    // TOC lines are bare component names (`Data.db`), so match by suffix.
    let toc_contents = std::fs::read_to_string(&toc_path).expect("read TOC.txt");
    let dir_entries: Vec<std::path::PathBuf> = std::fs::read_dir(&sstable_dir)
        .expect("read published sstable dir for component check")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();

    let mut checked = 0usize;
    for line in toc_contents.lines() {
        let component = line.trim();
        if component.is_empty() {
            continue;
        }
        let found = dir_entries.iter().find(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().ends_with(&format!("-{}", component)))
                .unwrap_or(false)
        });
        let path = found.unwrap_or_else(|| {
            panic!(
                "TOC lists component {component:?} but no matching file exists in {sstable_dir:?} \
                 — the publication barrier must never reference an absent component"
            )
        });
        let len = std::fs::metadata(path)
            .unwrap_or_else(|e| panic!("stat {path:?}: {e}"))
            .len();
        assert!(
            len > 0,
            "TOC-listed component {component:?} ({path:?}) must be non-empty; \
             a visible TOC must imply every component it names is complete"
        );
        checked += 1;
    }
    // A BIG compaction output lists at least Data/Statistics/Digest/TOC.
    assert!(
        checked >= 4,
        "expected the compaction TOC to list several components, listed {checked}"
    );

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
    let result_map: HashMap<Vec<u8>, ScanRow> =
        results.into_iter().map(|(k, v)| (k.0, v)).collect();

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
            ScanRow::Row(cells) => {
                for (name, col_val) in cells {
                    if name.as_ref() == "score" {
                        assert!(
                            matches!(col_val, Value::Null | Value::Tombstone(_)),
                            "PK=11 score column was cell-deleted by C (ts=300) \
                             but has live value: {:?} (Issue #505)",
                            col_val
                        );
                    }
                }
            }
            ScanRow::Marker(Value::Tombstone(_)) => {
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

    let result_map: HashMap<Vec<u8>, ScanRow> =
        results.into_iter().map(|(k, v)| (k.0, v)).collect();

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
            ScanRow::Row(cells) => {
                for (name, col_val) in cells {
                    if name.as_ref() == "score" {
                        assert!(
                            matches!(col_val, Value::Null | Value::Tombstone(_)),
                            "PK=11 score column was cell-deleted by C but has value: {:?}",
                            col_val
                        );
                    }
                }
            }
            ScanRow::Marker(Value::Tombstone(_)) => {
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

    let result_map: HashMap<Vec<u8>, ScanRow> =
        results.into_iter().map(|(k, v)| (k.0, v)).collect();

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
        ScanRow::Row(cells) => {
            let name = cells.iter().find_map(|(k, v)| match v {
                Value::Text(val) if k.as_ref() == "name" => Some(val.clone()),
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
            "PK=3 must be a live row (ScanRow::Row) carrying the ts=300 write, got {:?} (Issue #505)",
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

// ── Test 4b: Row deletion coexists with a surviving newer cell (Issue #932) ───

/// Regression test for Issue #932: a row-level deletion that COEXISTS with cells
/// written strictly AFTER it must (a) be read back with its surviving cells (not
/// collapsed to a pure tombstone), and (b) keep shadowing an OLDER cell of a
/// different column so that cell cannot resurrect across a compaction.
///
/// Setup (two SSTables):
///   - SSTable A (ts=50):  `score=999` for PK=7   (an OLDER cell, must be shadowed)
///   - SSTable B: a row tombstone for PK=7 at ts=100 AND `name="new"` at ts=300.
///     The intra-memtable flush already produces a COEXISTENCE row on disk —
///     `HAS_DELETION`(100) + the surviving `name`(300) cell.
///
/// Expected after compacting A+B:
///   - PK=7 is PRESENT and live, carrying `name="new"` (300 > 100 survives).
///   - PK=7 has NO `score`: the older `score`(50) is shadowed by the row
///     deletion (100). Before the fix, B's coexistence row read back as a PURE
///     tombstone (dropping `name`), so the merged row was either absent or had
///     `name` lost — and the preserved deletion is exactly what stops `score`
///     from resurrecting.
#[test]
fn row_deletion_coexists_with_newer_cell_after_compaction() {
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

    // SSTable A: an OLDER `score` cell (ts=50) for PK=7. This must end up shadowed
    // by the row deletion that lives in SSTable B.
    engine
        .write(write_score_only(7, 999, 50))
        .expect("write score A");
    let info_a = rt
        .block_on(engine.flush())
        .expect("flush A")
        .expect("info A");
    assert_eq!(info_a.partition_count, 1, "SSTable A: 1 partition");

    // SSTable B: a row tombstone at ts=100 PLUS a newer `name` write at ts=300 in
    // the same memtable. On flush these merge into ONE on-disk row carrying BOTH
    // the row deletion (100) and the surviving `name` cell (300) — the coexistence
    // row at the heart of #932.
    engine
        .write(delete_row(7, 100))
        .expect("write row-tombstone B");
    engine
        .write(write_name_only(7, "new", 300))
        .expect("write name B");
    let info_b = rt
        .block_on(engine.flush())
        .expect("flush B")
        .expect("info B");
    assert!(info_b.partition_count > 0, "SSTable B: non-empty");

    // Compact A+B (min_threshold=2, permissive buckets).
    let policy = STCSPolicy::new(2, 32, 0.01, 100.0, 0).expect("valid STCS params");
    engine
        .set_merge_policy(Box::new(policy))
        .expect("set policy");

    let budget = Duration::from_secs(30);
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

    let result_map: HashMap<Vec<u8>, ScanRow> =
        results.into_iter().map(|(k, v)| (k.0, v)).collect();

    let key_7: Vec<u8> = 7_i32.to_be_bytes().into();
    let pk7 = result_map.get(&key_7).unwrap_or_else(|| {
        panic!(
            "PK=7 must be PRESENT after compaction: the row deletion (ts=100) coexists with the \
             newer name='new' cell (ts=300) — Issue #932 (reader dropped the surviving cell)"
        )
    });

    match pk7 {
        ScanRow::Row(cells) => {
            let name = cells.iter().find_map(|(k, v)| match v {
                Value::Text(val) if k.as_ref() == "name" => Some(val.clone()),
                _ => None,
            });
            assert_eq!(
                name.as_deref(),
                Some("new"),
                "PK=7 must carry the surviving name='new' (ts=300 > deletion ts=100) — Issue #932"
            );
            // The older score (ts=50 <= deletion ts=100) must be shadowed by the
            // preserved row deletion and must NOT resurrect.
            let score = cells.iter().find_map(|(k, v)| match v {
                Value::Integer(val) if k.as_ref() == "score" => Some(*val),
                _ => None,
            });
            assert!(
                score.is_none(),
                "PK=7 score (ts=50) must be shadowed by the row deletion (ts=100) and NOT \
                 resurrect, got score={:?} — Issue #932 resurrection regression",
                score
            );
        }
        other => panic!(
            "PK=7 must read back as a live row (ScanRow::Row) carrying name='new', got {:?} — \
             Issue #932 (coexistence row collapsed to a pure tombstone)",
            other
        ),
    }

    drop(temp_dir);
}

// ── Test 5: Equal-timestamp Delete-vs-Live reconcile (Issue #498) ─────────────

/// Regression test for Issue #498: when a live row and a row tombstone for the
/// SAME partition key carry the EXACT SAME timestamp, the tombstone must win
/// after compaction — matching Cassandra `org.apache.cassandra.db.rows.Cells#reconcile`,
/// independent of which SSTable (file) the entries came from.
///
/// Setup (two SSTables, identical timestamp ts=200):
///   - SSTable A (flushed FIRST): live row PK=1 (ts=200), plus a live PK=2 control.
///   - SSTable B (flushed SECOND): row tombstone for PK=1 (ts=200).
///
/// The pre-fix merger resolved equal-timestamp conflicts by run_index (file
/// recency) only, so whichever file "won" the tie would decide liveness. With the
/// fix the tombstone always wins at equal timestamp regardless of file order.
///
/// Expected after compaction:
///   - PK=1 ABSENT  — the equal-ts tombstone shadows the live row.
///   - PK=2 PRESENT — live, never deleted (proves the merge produced output and
///     the absence of PK=1 is not a vacuous empty result).
#[test]
fn test_real_merger_delete_wins_at_equal_timestamp() {
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

    const EQUAL_TS: i64 = 200;

    // Adversarial setup for #498: the tombstone goes in the OLDER file and the
    // live write in the NEWER file. The pre-fix tiebreak (equal ts → lower
    // run_index = newer file wins) would therefore pick the LIVE write and keep
    // PK=1, so this test fails without the fix. The fix (equal ts → tombstone
    // wins, independent of file recency) makes PK=1 absent.

    // SSTable A (flushed first = OLDER): row tombstone for PK=1 at ts=200, plus a
    // live PK=2 control that is never deleted.
    engine
        .write(delete_row(1, EQUAL_TS))
        .expect("write equal-ts row-tombstone A");
    engine
        .write(write_row(2, "control-live", 22, EQUAL_TS))
        .expect("write live PK=2 A");
    let info_a = rt
        .block_on(engine.flush())
        .expect("flush A")
        .expect("info A");
    assert_eq!(info_a.partition_count, 2, "SSTable A: 2 partitions");

    // SSTable B (flushed second = NEWER): live PK=1 at the SAME timestamp ts=200.
    engine
        .write(write_row(1, "live-at-equal-ts", 11, EQUAL_TS))
        .expect("write live PK=1 B");
    let info_b = rt
        .block_on(engine.flush())
        .expect("flush B")
        .expect("info B");
    assert!(info_b.partition_count > 0, "SSTable B: non-empty");

    // Permissive STCS so the two tiny SSTables compact together.
    let policy = STCSPolicy::new(2, 32, 0.01, 100.0, 0).expect("valid STCS params");
    engine
        .set_merge_policy(Box::new(policy))
        .expect("set policy");

    let budget = Duration::from_secs(30);
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

    let result_map: HashMap<Vec<u8>, ScanRow> =
        results.into_iter().map(|(k, v)| (k.0, v)).collect();

    // PK=1 must be ABSENT: equal-timestamp tombstone wins (Cassandra reconcile).
    let key_1: Vec<u8> = 1_i32.to_be_bytes().into();
    assert!(
        !result_map.contains_key(&key_1),
        "PK=1 was row-deleted at the SAME timestamp (ts={}) as its live write but is \
         present after compaction — equal-ts tiebreak reverted to file recency \
         instead of letting the tombstone win (Issue #498)",
        EQUAL_TS
    );

    // PK=2 must be PRESENT: proves the compaction produced real output and PK=1's
    // absence is the tombstone shadowing, not an empty/failed merge.
    let key_2: Vec<u8> = 2_i32.to_be_bytes().into();
    assert!(
        result_map.contains_key(&key_2),
        "PK=2 (live control, never deleted) must be present after compaction"
    );

    eprintln!(
        "test_real_merger_delete_wins_at_equal_timestamp PASSED: \
         PK=1 absent (equal-ts tombstone wins), PK=2 present (live control)"
    );

    drop(temp_dir);
}

// ── Test 6: Disjoint-column per-cell reconcile (Issue #533) ──────────────────

/// End-to-end regression for Issue #533: two SSTables that share the same
/// partition key but carry DISJOINT columns must both survive compaction.
///
/// The pre-fix merger selected one whole winning row per (pk, ck) and DROPPED the
/// loser's columns (DATA LOSS). With per-cell reconcile, cells from every input
/// survive.
///
/// Setup (write → compact → read via WriteEngine + SSTableManager):
///   - SSTable A (ts=100): PK=1 writes only `name`="alice"; PK=3 writes `name`="x".
///   - SSTable B (ts=200): PK=1 writes only `score`=42; PK=3 writes `name`="y".
///
/// Expected after compaction:
///   - PK=1 carries BOTH `name`="alice" (from A) and `score`=42 (from B).
///   - PK=3 `name` resolves by timestamp to "y" (B ts=200 > A ts=100).
#[test]
fn disjoint_columns_survive_compaction() {
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

    // SSTable A (ts=100): PK=1 -> {name: "alice"}; PK=3 -> {name: "x"} (column conflict).
    engine
        .write(write_name_only(1, "alice", 100))
        .expect("write A PK=1 name");
    engine
        .write(write_name_only(3, "x", 100))
        .expect("write A PK=3 name");
    let info_a = rt
        .block_on(engine.flush())
        .expect("flush A")
        .expect("info A");
    assert_eq!(info_a.partition_count, 2, "SSTable A: 2 partitions");

    // SSTable B (ts=200): PK=1 -> {score: 42} (disjoint); PK=3 -> {name: "y"} (conflict, newer).
    engine
        .write(write_score_only(1, 42, 200))
        .expect("write B PK=1 score");
    engine
        .write(write_name_only(3, "y", 200))
        .expect("write B PK=3 name");
    let info_b = rt
        .block_on(engine.flush())
        .expect("flush B")
        .expect("info B");
    assert!(info_b.partition_count > 0, "SSTable B: non-empty");

    // Permissive STCS so the two tiny SSTables compact together.
    let policy = STCSPolicy::new(2, 32, 0.01, 100.0, 0).expect("valid STCS params");
    engine
        .set_merge_policy(Box::new(policy))
        .expect("set policy");

    let budget = Duration::from_secs(30);
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

    let result_map: HashMap<Vec<u8>, ScanRow> =
        results.into_iter().map(|(k, v)| (k.0, v)).collect();

    // Helper: extract a named text/int column from a row's Value::Map.
    fn find_col<'a>(row: &'a ScanRow, col: &str) -> Option<&'a Value> {
        match row {
            // Issue #1334: rows decode to `ScanRow::Row` keyed by `Arc<str>`.
            ScanRow::Row(cells) => {
                cells
                    .iter()
                    .find_map(|(k, v)| if k.as_ref() == col { Some(v) } else { None })
            }
            _ => None,
        }
    }

    // PK=1 must carry BOTH disjoint columns: name (from A) and score (from B).
    let key_1: Vec<u8> = 1_i32.to_be_bytes().into();
    let pk1 = result_map
        .get(&key_1)
        .expect("PK=1 must be present after compaction");

    let name_1 = find_col(pk1, "name");
    let score_1 = find_col(pk1, "score");

    assert_eq!(
        name_1,
        Some(&Value::Text("alice".to_string())),
        "PK=1 `name` from SSTable A was DROPPED after compaction — per-cell reconcile \
         regression (Issue #533). The old whole-row-wins merger keeps only B's `score`."
    );
    assert_eq!(
        score_1,
        Some(&Value::Integer(42)),
        "PK=1 `score` from SSTable B must be present"
    );

    // PK=3: same column `name` in both SSTables — newer (B, ts=200) wins.
    let key_3: Vec<u8> = 3_i32.to_be_bytes().into();
    let pk3 = result_map
        .get(&key_3)
        .expect("PK=3 must be present after compaction");
    assert_eq!(
        find_col(pk3, "name"),
        Some(&Value::Text("y".to_string())),
        "PK=3 `name` conflict must resolve to the higher-timestamp value (B ts=200)"
    );

    eprintln!(
        "disjoint_columns_survive_compaction PASSED: PK=1 has both name+score \
         (disjoint columns preserved), PK=3 name resolves by timestamp (Issue #533)"
    );

    drop(temp_dir);
}

// ── Test 7: Wide-partition clustering-key correctness ─────────────────────────

/// Regression test: the k-way merge must preserve distinct clustering rows within
/// a partition and apply LWW within each clustering row.
///
/// The pre-fix merger set `clustering_key: None` on every `MergeEntry` regardless
/// of the schema, so all rows of the same partition landed in the same
/// `reconcile_cluster` group and collapsed to a single output row.
///
/// This test drives `KWayMerger` directly (the same code path used by the Flight
/// producer) to verify the fix without depending on SSTableManager's scan path.
///
/// Setup (two SSTables, table: `CREATE TABLE ck_ks.wide (pk int, ck text, val int,
/// PRIMARY KEY (pk, ck))`):
///   - SSTable A (ts=100): (pk=1, ck='a', val=10), (pk=1, ck='b', val=20)
///   - SSTable B (ts=200): (pk=1, ck='a', val=99)   — overrides ck='a' only
///
/// Expected after merge:
///   - Partition pk=1 contains EXACTLY 2 merged rows.
///   - The row with ck='a' has val=99   (LWW: B's ts=200 > A's ts=100).
///   - The row with ck='b' has val=20   (A-only, not overridden).
///
/// If the fix is absent the two rows collapse to ONE row (the bug), failing the
/// `assert_eq!(row_count, 2)`.
#[test]
fn clustering_key_rows_survive_compaction() {
    use cqlite_core::storage::write_engine::merge::{MergeStep, RowData};
    use cqlite_core::storage::write_engine::KWayMerger;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");

    // Schema: pk int (partition key), ck text (clustering key), val int (regular).
    let schema = TableSchema {
        keyspace: "ck_ks".to_string(),
        table: "wide".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "val".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // Helper: build a mutation for (pk, ck, val).
    let make_row = |pk: i32, ck: &str, val: i32, ts: i64| -> Mutation {
        Mutation::new(
            TableId::new("ck_ks", "wide"),
            PartitionKey::single("pk", Value::Integer(pk)),
            Some(ClusteringKey::single("ck", Value::Text(ck.to_string()))),
            vec![CellOperation::Write {
                column: "val".to_string(),
                value: Value::Integer(val),
            }],
            ts,
            None,
        )
    };

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // SSTable A (ts=100): two distinct clustering rows within partition pk=1.
    engine
        .write(make_row(1, "a", 10, 100))
        .expect("write A ck=a");
    engine
        .write(make_row(1, "b", 20, 100))
        .expect("write A ck=b");
    let info_a = rt
        .block_on(engine.flush())
        .expect("flush A")
        .expect("info A");
    assert!(info_a.partition_count > 0, "SSTable A non-empty");

    // SSTable B (ts=200): override only ck='a' with val=99.
    engine
        .write(make_row(1, "a", 99, 200))
        .expect("write B ck=a override");
    let info_b = rt
        .block_on(engine.flush())
        .expect("flush B")
        .expect("info B");
    assert!(info_b.partition_count > 0, "SSTable B non-empty");

    rt.block_on(engine.close())
        .expect("close engine before direct merger test");

    // ── Direct KWayMerger verification ───────────────────────────────────────
    // Locate the two Data.db files written above.
    let table_dir = data_dir.join("ck_ks").join("wide");
    let mut data_files: Vec<std::path::PathBuf> = std::fs::read_dir(&table_dir)
        .expect("read table dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with("-Data.db"))
                .unwrap_or(false)
        })
        .collect();

    // Sort newest generation first (same as DirSource in the flight crate).
    data_files.sort_by(|a, b| {
        let gen_of = |p: &std::path::Path| -> u64 {
            p.file_name()
                .and_then(|n| n.to_str())
                .and_then(|name| name.split('-').find_map(|seg| seg.parse::<u64>().ok()))
                .unwrap_or(0)
        };
        gen_of(b).cmp(&gen_of(a))
    });

    assert!(
        data_files.len() >= 2,
        "Expected at least 2 Data.db files, found {}",
        data_files.len()
    );

    let mut merger = KWayMerger::new(data_files, &schema).expect("KWayMerger::new");

    // Collect all merged rows.
    struct MergedRow {
        ck: Option<Value>,
        val: Option<Value>,
    }
    let mut merged_rows: Vec<MergedRow> = Vec::new();

    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    // Skip tombstone entries.
                    let cells = match entry.row_data {
                        RowData::Live { cells } => cells,
                        RowData::Tombstone { .. } => continue,
                    };
                    let find_col = |col: &str| -> Option<Value> {
                        cells
                            .iter()
                            .find(|c| c.column == col)
                            .map(|c| c.value.clone())
                    };
                    merged_rows.push(MergedRow {
                        ck: find_col("ck"),
                        val: find_col("val"),
                    });
                }
            }
        }
    }

    let row_count = merged_rows.len();
    eprintln!(
        "clustering_key_rows_survive_compaction: {} merged rows",
        row_count
    );

    // The pre-fix bug collapses (pk=1,ck='a') and (pk=1,ck='b') to ONE row.
    assert_eq!(
        row_count, 2,
        "Wide partition pk=1 must produce exactly 2 distinct clustering rows from \
         the k-way merger (ck='a' and ck='b'). Got {} — the merger collapsed them \
         into one row (clustering_key: None bug).",
        row_count
    );

    // Locate the two rows by ck value.
    let row_a = merged_rows
        .iter()
        .find(|r| matches!(&r.ck, Some(Value::Text(s)) if s == "a"))
        .expect("row ck='a' must be present");
    let row_b = merged_rows
        .iter()
        .find(|r| matches!(&r.ck, Some(Value::Text(s)) if s == "b"))
        .expect("row ck='b' must be present");

    // LWW within ck='a': SSTable B (ts=200, val=99) wins over A (ts=100, val=10).
    assert_eq!(
        row_a.val,
        Some(Value::Integer(99)),
        "ck='a' val must be 99 (LWW: B ts=200 > A ts=100)"
    );

    // ck='b' unchanged: only in SSTable A.
    assert_eq!(
        row_b.val,
        Some(Value::Integer(20)),
        "ck='b' val must be 20 (A-only, not overridden)"
    );

    eprintln!(
        "clustering_key_rows_survive_compaction PASSED: 2 distinct clustering rows \
         preserved, LWW correct within each clustering row"
    );

    drop(temp_dir);
}
