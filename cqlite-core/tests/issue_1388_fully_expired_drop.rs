//! Issue #1388: fully-expired SSTable drop (design-driven).
//!
//! When a compaction's input set contains an SSTable proven FULLY EXPIRED by
//! authoritative `Statistics.db` metadata (`max_deletion_time < gcBefore`, no cell
//! scan — no-heuristics mandate #28) AND overlap-safe against the SSTables outside
//! the compaction set, that SSTable is DROPPED WHOLE: excluded from the K-way
//! merger's input list (never read/decoded — the perf win) and its components
//! reclaimed only after the merged output publishes, via the same reclamation path
//! as the merged inputs. This mirrors Cassandra's
//! `CompactionController.getFullyExpiredSSTables` / `TTLExpiryTest`.
//!
//! Every test drives a REAL public compaction surface:
//!   - `compact_sstables` (the CLI one-shot `compact --major` path, OQ-1 → (A)); and
//!   - `WriteEngine::maintenance_step` (the production background path).
//!
//! Determinism: a fully-expired SSTable is built from ROW TOMBSTONES stamped with
//! a small, explicit past `localDeletionTime` and low write timestamps, under a
//! schema with `gc_grace_seconds = 0` so the production `gcBefore = now_secs`
//! (wall clock, ~1.7e9) is unconditionally above the tiny tombstone LDT. The drop
//! decision therefore never races the wall clock: `max_deletion_time` (tiny) is
//! always `< gcBefore`. A live SSTable (a plain write) is never expired
//! (`max_deletion_time == NO_DELETION_TIME`).
//!
//! Unit-level R1/R2 (metadata detection + overlap gate) live in the merge module's
//! `#[cfg]` tests (`fully_expired_sstables`); this file covers R3 (exclusion +
//! reclamation on the real surfaces), R4 (report names the dropped set), and R5
//! (read parity before/after == merged-purge result).

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{
    compact_sstables, fully_expired_sstables, MergeStep, RowData,
};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, KWayMerger, MergePolicy, Mutation, PartitionKey, TableId,
    WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

// ===========================================================================
// Schema + mutation helpers
// ===========================================================================

/// `gc_grace_seconds = 0` ⇒ production `gcBefore = now_secs` (wall clock), which
/// is always above a tiny explicit tombstone LDT, so a tombstone-only SSTable is
/// unconditionally fully expired regardless of the sampled wall clock.
fn make_schema() -> TableSchema {
    let mut comments = HashMap::new();
    comments.insert("gc_grace_seconds".to_string(), "0".to_string());
    TableSchema {
        keyspace: "exp_ks".to_string(),
        table: "items".to_string(),
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
            col("id", "int", false),
            col("ck", "int", false),
            col("name", "text", true),
        ],
        comments,
        dropped_columns: HashMap::new(),
    }
}

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable,
        default: None,
        is_static: false,
    }
}

/// A ROW TOMBSTONE stamped with an explicit past `localDeletionTime` (GC-clock
/// seconds) and a low write timestamp. An SSTable of only these has
/// `max_deletion_time == ldt_secs`, which is `< gcBefore` for any real wall clock.
fn delete_row(id: i32, ck: i32, ldt_secs: i32, ts_micros: i64) -> Mutation {
    Mutation::new(
        TableId::new("exp_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::DeleteRow],
        ts_micros,
        None,
    )
    .with_local_deletion_time(ldt_secs)
}

/// A plain live write (never expiring): `max_deletion_time == NO_DELETION_TIME`.
fn write_live_row(id: i32, ck: i32, name: &str, ts_micros: i64) -> Mutation {
    Mutation::new(
        TableId::new("exp_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }],
        ts_micros,
        None,
    )
}

// ===========================================================================
// Runtime + I/O helpers
// ===========================================================================

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

/// Flush a batch of mutations into ONE input SSTable under `data_dir`.
fn flush_batch(data_dir: &Path, wal_dir: &Path, schema: &TableSchema, muts: Vec<Mutation>) {
    let config = WriteEngineConfig::new(
        data_dir.to_path_buf(),
        wal_dir.to_path_buf(),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in muts {
        engine.write(m).expect("write mutation");
    }
    let r = rt();
    r.block_on(engine.flush()).expect("flush").expect("info");
    r.block_on(engine.close()).expect("close engine");
}

/// Discover `nb-*-big-Data.db` inputs, newest-generation first.
fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect(dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect(dir: &Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            let base = name.trim_end_matches("-Data.db");
            if !path.with_file_name(format!("{base}-TOC.txt")).exists() {
                continue;
            }
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect(&path, out, depth - 1);
        }
    }
}

/// Read every surviving live `name` cell across a directory's SSTables back
/// through the merge read path (raw on-disk state, expiry disabled).
fn read_name_values(inputs: &[PathBuf], schema: &TableSchema, purge_safe: bool) -> Vec<String> {
    let non_empty: Vec<PathBuf> = inputs
        .iter()
        .filter(|p| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false))
        .cloned()
        .collect();
    if non_empty.is_empty() {
        return Vec::new();
    }
    let mut merger = KWayMerger::new_with_gc(non_empty, schema, None, None)
        .expect("merger")
        .with_purge_safe(purge_safe);
    let mut out = Vec::new();
    loop {
        match merger.step().expect("step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    if let RowData::Live { cells } = &entry.row_data {
                        for c in cells {
                            if c.column == "name" {
                                if let Value::Text(v) = &c.value {
                                    out.push(v.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    out.sort();
    out
}

/// The pinned "now" / gcBefore for the one-shot surface. A tiny tombstone LDT
/// (`< gc_before`) is fully expired; a live cell (NO_DELETION_TIME) never is.
const NOW_SECS: i64 = 10_000;
const GC_BEFORE: i64 = 5_000; // > every tombstone LDT below, < NO_DELETION_TIME

/// Tombstone LDTs used by the fixtures (all strictly below GC_BEFORE).
const TOMB_LDT: i32 = 100;

// ===========================================================================
// R3 (one-shot / CLI --major) + R4 (report) + acceptance-criterion 1
// ===========================================================================

/// Major compaction of an all-expired SSTable + a live SSTable: the expired
/// SSTable is DROPPED WHOLE (excluded from the merge, never read), the live rows
/// all survive, and the report names exactly the dropped SSTable.
#[test]
fn major_drops_expired_sstable_and_keeps_live() {
    let temp = TempDir::new().unwrap();
    let expired_dir = temp.path().join("expired");
    let live_dir = temp.path().join("live");
    let wal = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    // All-expired SSTable: row tombstones at a tiny LDT and LOW write timestamps.
    flush_batch(
        &expired_dir,
        &wal.join("e"),
        &schema,
        vec![
            delete_row(1, 0, TOMB_LDT, 100),
            delete_row(2, 0, TOMB_LDT, 100),
        ],
    );
    // Live SSTable at HIGHER write timestamps.
    flush_batch(
        &live_dir,
        &wal.join("l"),
        &schema,
        vec![
            write_live_row(10, 0, "alive-a", 5_000_000),
            write_live_row(11, 0, "alive-b", 5_000_000),
        ],
    );

    let expired = discover_inputs(&expired_dir);
    let live = discover_inputs(&live_dir);
    assert_eq!(expired.len(), 1, "one expired input");
    assert_eq!(live.len(), 1, "one live input");
    let expired_path = expired[0].clone();

    // Sanity: the drop-set classifier (metadata-only) selects the expired one and
    // NOT the live one for a major compaction (empty outside set).
    let all_inputs: Vec<PathBuf> = vec![live[0].clone(), expired_path.clone()];
    let drop_set = fully_expired_sstables(&all_inputs, &[], Some(GC_BEFORE));
    assert_eq!(
        drop_set,
        vec![expired_path.clone()],
        "metadata classifier must pick only the expired SSTable for a major compaction"
    );

    // Real one-shot major compaction (purge_safe = true ⇒ empty outside ⇒ drop).
    let report = rt()
        .block_on(compact_sstables(
            all_inputs.clone(),
            &out_dir,
            &schema,
            1,
            Some(GC_BEFORE),
            Some(NOW_SECS),
            true,
        ))
        .expect("compaction succeeds");

    // R4: the report names exactly the dropped SSTable, distinct from merged inputs.
    assert_eq!(
        report.stats.dropped_whole,
        vec![expired_path.clone()],
        "report must name exactly the dropped-whole SSTable"
    );
    // The merger's input_files counts only the SSTables actually merged (live one).
    assert_eq!(
        report.stats.input_files, 1,
        "only the live SSTable was fed to the merger (expired dropped whole)"
    );

    // R3: the dropped SSTable's components were reclaimed after publish.
    assert!(
        !expired_path.exists(),
        "dropped-whole SSTable's Data.db must be reclaimed after publish"
    );

    // Output holds every live row, none of the expired rows.
    let out_inputs = discover_inputs(&out_dir);
    let values = read_name_values(&out_inputs, &schema, true);
    assert_eq!(
        values,
        vec!["alive-a".to_string(), "alive-b".to_string()],
        "output keeps all live rows and no expired rows"
    );
}

/// A compaction that drops nothing (no input is fully expired) reports an EMPTY
/// dropped-whole set.
#[test]
fn no_expired_input_reports_empty_dropped_set() {
    let temp = TempDir::new().unwrap();
    let a_dir = temp.path().join("a");
    let b_dir = temp.path().join("b");
    let wal = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    flush_batch(
        &a_dir,
        &wal.join("a"),
        &schema,
        vec![write_live_row(1, 0, "a", 1_000_000)],
    );
    flush_batch(
        &b_dir,
        &wal.join("b"),
        &schema,
        vec![write_live_row(2, 0, "b", 2_000_000)],
    );
    let mut inputs = discover_inputs(&a_dir);
    inputs.extend(discover_inputs(&b_dir));

    let report = rt()
        .block_on(compact_sstables(
            inputs,
            &out_dir,
            &schema,
            1,
            Some(GC_BEFORE),
            Some(NOW_SECS),
            true,
        ))
        .expect("compaction succeeds");

    assert!(
        report.stats.dropped_whole.is_empty(),
        "a compaction that drops nothing must report an empty dropped-whole set"
    );
    let out_inputs = discover_inputs(&out_dir);
    assert_eq!(
        read_name_values(&out_inputs, &schema, true),
        vec!["a".to_string(), "b".to_string()],
        "all live rows survive"
    );
}

/// Non-major (conservative) one-shot compaction does NOT drop, even an expired
/// input (OQ-1 → (A): drop only under `--major`).
#[test]
fn non_major_one_shot_does_not_drop() {
    let temp = TempDir::new().unwrap();
    let expired_dir = temp.path().join("expired");
    let live_dir = temp.path().join("live");
    let wal = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    flush_batch(
        &expired_dir,
        &wal.join("e"),
        &schema,
        vec![delete_row(1, 0, TOMB_LDT, 100)],
    );
    flush_batch(
        &live_dir,
        &wal.join("l"),
        &schema,
        vec![write_live_row(10, 0, "alive", 5_000_000)],
    );
    let expired = discover_inputs(&expired_dir);
    let live = discover_inputs(&live_dir);
    let inputs: Vec<PathBuf> = vec![live[0].clone(), expired[0].clone()];

    // purge_safe = false ⇒ no drop.
    let report = rt()
        .block_on(compact_sstables(
            inputs,
            &out_dir,
            &schema,
            1,
            Some(GC_BEFORE),
            Some(NOW_SECS),
            false,
        ))
        .expect("compaction succeeds");
    assert!(
        report.stats.dropped_whole.is_empty(),
        "a non-major one-shot compaction must never drop whole (OQ-1 → (A))"
    );
    // Both inputs were fed to the merger (nothing dropped).
    assert_eq!(report.stats.input_files, 2);
}

/// Roborev F1 (former data-loss) regression: a MIXED SSTable — an old row
/// tombstone whose `localDeletionTime` is below `gcBefore` PLUS a LIVE non-TTL
/// cell in the SAME SSTable — must NOT be classified fully expired and must NOT
/// be dropped by a major `compact_sstables`. Since #1728 the writer stamps live
/// cells with `NO_DELETION_TIME`, so the finalized `max_local_deletion_time`
/// (authoritative `Statistics.db`) is the `i32::MAX` live sentinel (parses back as
/// `i64::MAX`), never `< gcBefore`. Dropping such an SSTable whole would lose the
/// live cell (the former data-loss bug); it must instead be MERGED so the live
/// row survives.
#[test]
fn mixed_tombstone_and_live_sstable_not_dropped() {
    let temp = TempDir::new().unwrap();
    let mixed_dir = temp.path().join("mixed");
    let wal = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    // ONE SSTable holding BOTH an expired row tombstone (tiny LDT < GC_BEFORE)
    // AND a live non-TTL write (NO_DELETION_TIME). The mix lifts the SSTable's
    // authoritative max_local_deletion_time to the live sentinel (#1728).
    flush_batch(
        &mixed_dir,
        &wal.join("m"),
        &schema,
        vec![
            delete_row(1, 0, TOMB_LDT, 100),
            write_live_row(2, 0, "keep-me", 5_000_000),
        ],
    );
    let mixed = discover_inputs(&mixed_dir);
    assert_eq!(mixed.len(), 1, "one mixed input SSTable");
    let mixed_path = mixed[0].clone();

    // Metadata classifier: the mixed SSTable is NOT fully expired (its authoritative
    // max_local_deletion_time is the live sentinel), so it is NOT in the drop-set —
    // even for a major compaction (empty outside set).
    let drop_set = fully_expired_sstables(&[mixed_path.clone()], &[], Some(GC_BEFORE));
    assert!(
        drop_set.is_empty(),
        "a mixed tombstone+live SSTable must never be classified fully expired (#1728 F1)"
    );

    // Public surface: a major compaction must NOT drop it whole (would lose the
    // live cell); it is merged and the live row survives.
    let report = rt()
        .block_on(compact_sstables(
            vec![mixed_path.clone()],
            &out_dir,
            &schema,
            1,
            Some(GC_BEFORE),
            Some(NOW_SECS),
            true,
        ))
        .expect("compaction succeeds");
    assert!(
        report.stats.dropped_whole.is_empty(),
        "compact_sstables must not drop a mixed tombstone+live SSTable whole (F1 data-loss)"
    );
    assert!(
        mixed_path.exists(),
        "the mixed SSTable was merged (retained until publish), not dropped whole"
    );
    // The live cell survives the merge; the tombstoned row is purged.
    let values = read_name_values(&discover_inputs(&out_dir), &schema, true);
    assert_eq!(
        values,
        vec!["keep-me".to_string()],
        "the live cell in the mixed SSTable must survive (not lost to a whole drop)"
    );
}

// ===========================================================================
// R3 (WriteEngine background path) + R4 (MaintenanceReport)
// ===========================================================================

/// STCS-style policy that selects EVERY candidate (a full/major compaction), so
/// `maintenance_step` computes `purge_safe == true` (empty outside set).
#[derive(Debug)]
struct SelectAllPolicy;

impl MergePolicy for SelectAllPolicy {
    fn select_merge(&self, candidates: &[PathBuf]) -> Result<Vec<PathBuf>, cqlite_core::Error> {
        Ok(candidates.to_vec())
    }
}

/// Background full compaction drops a fully-expired candidate whole: it is
/// excluded from the merger (never read), reclaimed after publish, and named in
/// the `MaintenanceReport`.
#[test]
fn background_full_compaction_drops_expired_candidate() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal = temp.path().join("wal");
    let schema = make_schema();
    let table_dir = data_dir.join(&schema.keyspace).join(&schema.table);

    let config = WriteEngineConfig::new(data_dir.clone(), wal, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    let r = rt();

    // gen1: fully-expired SSTable (row tombstones, tiny LDT, low write ts).
    engine
        .write(delete_row(1, 0, TOMB_LDT, 100))
        .expect("write");
    engine
        .write(delete_row(2, 0, TOMB_LDT, 100))
        .expect("write");
    r.block_on(engine.flush()).expect("flush").expect("info");
    // gen2: live SSTable (higher write ts).
    engine
        .write(write_live_row(10, 0, "alive", 5_000_000))
        .expect("write");
    r.block_on(engine.flush()).expect("flush").expect("info");

    let before: Vec<PathBuf> = discover_inputs(&table_dir);
    assert_eq!(
        before.len(),
        2,
        "two input SSTables (expired gen1, live gen2)"
    );
    // Identify the expired one (gen1 has the tombstones; it is the older gen).
    let expired_before = before
        .iter()
        .min_by_key(|p| gen_of(p).unwrap_or(u64::MAX))
        .cloned()
        .expect("expired input");

    engine
        .set_merge_policy(Box::new(SelectAllPolicy))
        .expect("policy");
    let mut dropped_reported: Vec<PathBuf> = Vec::new();
    let mut guard = 0;
    loop {
        let report = engine
            .maintenance_step(std::time::Duration::from_millis(500))
            .expect("maintenance step");
        dropped_reported.extend(report.dropped_whole.iter().cloned());
        if !report.pending_compaction {
            break;
        }
        guard += 1;
        assert!(guard < 1000, "maintenance did not converge");
    }
    r.block_on(engine.close()).expect("close");

    // R4: the MaintenanceReport named exactly the expired input as dropped whole.
    assert_eq!(
        dropped_reported,
        vec![expired_before.clone()],
        "background report must name exactly the dropped-whole SSTable"
    );

    // R3: after compaction only ONE SSTable (the merged output) remains; the
    // expired input's Data.db is gone (reclaimed).
    let after: Vec<PathBuf> = discover_inputs(&table_dir);
    assert_eq!(after.len(), 1, "expired dropped + live merged ⇒ one output");
    assert!(!expired_before.exists(), "expired input reclaimed");

    // The live row survives; no expired rows appear.
    let values = read_name_values(&after, &schema, true);
    assert_eq!(
        values,
        vec!["alive".to_string()],
        "only the live row survives"
    );
}

fn gen_of(p: &Path) -> Option<u64> {
    let name = p.file_name()?.to_str()?;
    name.strip_prefix("nb-")?
        .split("-big-")
        .next()?
        .parse::<u64>()
        .ok()
}

// ===========================================================================
// R5 — read parity: drop-whole output == merged-purge output, and equals the
// pre-compaction read (a dropped SSTable contributes no live data).
// ===========================================================================

/// The query result over the drop-whole compaction output equals (a) the result
/// over the raw inputs before compaction and (b) the result of a compaction that
/// MERGED (rather than dropped) the fully-expired SSTable through the normal purge
/// path. All three are the live rows only.
#[test]
fn read_parity_drop_whole_equals_merged_purge() {
    let temp = TempDir::new().unwrap();
    let schema = make_schema();

    // Each compaction reclaims (deletes) its input SSTables, so build a FRESH,
    // independent input set for every read/compaction rather than sharing paths.
    let build_inputs = |tag: &str| -> Vec<PathBuf> {
        let expired_dir = temp.path().join(format!("{tag}_expired"));
        let live_dir = temp.path().join(format!("{tag}_live"));
        let wal = temp.path().join(format!("{tag}_wal"));
        flush_batch(
            &expired_dir,
            &wal.join("e"),
            &schema,
            vec![
                delete_row(1, 0, TOMB_LDT, 100),
                delete_row(2, 0, TOMB_LDT, 100),
            ],
        );
        flush_batch(
            &live_dir,
            &wal.join("l"),
            &schema,
            vec![
                write_live_row(10, 0, "a", 5_000_000),
                write_live_row(11, 0, "b", 5_000_000),
            ],
        );
        let live = discover_inputs(&live_dir);
        let expired = discover_inputs(&expired_dir);
        vec![live[0].clone(), expired[0].clone()]
    };

    // (a) BEFORE: read the raw inputs (purge_safe so tombstones shadow nothing
    // external). Row tombstones delete their own rows; the live rows survive.
    let before_values = read_name_values(&build_inputs("before"), &schema, true);

    // (b) DROP-WHOLE compaction (major): the expired SSTable is dropped whole.
    let drop_out = temp.path().join("drop_out");
    let drop_report = rt()
        .block_on(compact_sstables(
            build_inputs("drop"),
            &drop_out,
            &schema,
            1,
            Some(GC_BEFORE),
            Some(NOW_SECS),
            true,
        ))
        .expect("drop-whole compaction");
    assert!(
        !drop_report.stats.dropped_whole.is_empty(),
        "the drop-whole path must actually drop the expired SSTable"
    );
    let drop_values = read_name_values(&discover_inputs(&drop_out), &schema, true);

    // (c) MERGED-PURGE compaction: force the expired SSTable through the merger by
    // classifying it as NOT droppable. We do this with gc_before = None (dropping
    // AND gc-purge disabled) so the expired SSTable is READ and merged; its row
    // tombstones still delete their own rows via reconciliation, so the live rows
    // are the same surviving set. This proves the drop is observationally
    // equivalent to reading+merging the expired SSTable.
    let merge_out = temp.path().join("merge_out");
    let merge_report = rt()
        .block_on(compact_sstables(
            build_inputs("merge"),
            &merge_out,
            &schema,
            1,
            None, // gc_before = None ⇒ no drop, expired SSTable is merged
            Some(NOW_SECS),
            true,
        ))
        .expect("merged-purge compaction");
    assert!(
        merge_report.stats.dropped_whole.is_empty(),
        "the merged-purge path must NOT drop (gc_before = None)"
    );
    assert_eq!(
        merge_report.stats.input_files, 2,
        "both inputs were fed to the merger on the merged path"
    );
    let merge_values = read_name_values(&discover_inputs(&merge_out), &schema, true);

    // R5: all three agree — the live rows only.
    let expected = vec!["a".to_string(), "b".to_string()];
    assert_eq!(before_values, expected, "pre-compaction read");
    assert_eq!(drop_values, expected, "drop-whole output read");
    assert_eq!(merge_values, expected, "merged-purge output read");
    assert_eq!(
        drop_values, merge_values,
        "drop-whole output must equal merged-purge output (read parity)"
    );
}
