//! Issue #1385: ±1s exactness of the strict-`<` gc_grace purge boundary, driven
//! through the REAL public compaction surface ([`compact_sstables`]).
//!
//! Purge at every tombstone site uses a STRICT less-than compare against
//! `gcBefore`:
//!
//! ```text
//! i64::from(ldt as u32) < gc_before   // cell tombstone   (reconcile.rs)
//! i64::from(row_del_ldt as u32) < gc_before   // row tombstone    (reconcile.rs)
//! i64::from(cd.local_deletion_time as u32) < gc_before   // complex marker
//! ```
//!
//! `<` means the boundary itself matters to the SECOND: a tombstone whose
//! `localDeletionTime == gcBefore` is RETAINED (its grace has NOT elapsed); one
//! at `gcBefore - 1` is PURGED. An off-by-one on either side is a real
//! correctness bug — purging one second early RESURRECTS shadowed data; retaining
//! one second too long DIVERGES from Cassandra's on-disk bytes. Existing tests
//! use coarse (>= 1000s) gaps and cannot catch a ±1s slip; these pin the boundary
//! to the exact second.
//!
//! ## Determinism (no wall-clock races)
//!
//! Every tombstone is stamped with an EXPLICIT `localDeletionTime` at write time
//! (via `Mutation::with_local_deletion_time`, `with_row_tombstone`, or the
//! `ComplexDeletion` op's own `local_deletion_time`). Each test then READS that
//! authoritative on-disk LDT back from the flushed input and pins `gcBefore`
//! RELATIVE to it (`ldt` or `ldt + 1`). No assertion ever samples a wall clock;
//! `now_secs` is pinned far in the future so the (unrelated) TTL-expiry stage is
//! a no-op for these explicit tombstones. Full compaction (`purge_safe = true`)
//! makes the `#935` overlap gate `+inf`, isolating the pure gc-grace decision.
//!
//! ## Observing a purge on the public surface
//!
//! The internal `PurgeCounts` tally is private and not surfaced on
//! [`CompactReport`]. The public-surface equivalent of `PurgeCounts.* == 1` is
//! OBSERVED ABSENCE: each scenario writes EXACTLY ONE purgeable tombstone paired
//! with an independent live survivor (so the row is never a phantom key-only
//! drop), then asserts the tombstone is present at the boundary and absent one
//! second below it. "Absent purgeable tombstone + present survivor" is precisely
//! `PurgeCounts.<kind> == 1` made observable through the merge read path.
//!
//! ## Acceptance criteria (issue #1385)
//!
//!   1. `cell_tombstone_boundary_*` — cell tombstone: `ldt == gcBefore` RETAINED;
//!      `ldt == gcBefore - 1` PURGED (absent).
//!   2. `row_tombstone_boundary_*` — row deletion through the row-deletion purge
//!      site: `RowData::Tombstone` emitted at the boundary, omitted one below.
//!   3. `complex_deletion_boundary_*` — complex-deletion marker through the marker
//!      purge site: `complex_deletions` entry present at the boundary, absent one
//!      below.
//!   4. `shadow_before_purge_at_boundary` — a purgeable-at-boundary
//!      (`ldt == gcBefore - 1`) tombstone STILL shadows an older covered cell
//!      before it is purged: covered cell ABSENT and tombstone ABSENT.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{
    compact_sstables, CellData, ComplexDeletion, MergeEntry, MergeStep, RowData,
};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::{TombstoneType, Value};
use tempfile::TempDir;

// ===========================================================================
// Schema + mutation helpers
// ===========================================================================

/// `id int PK, ck int clustering, name text, score int, tags set<text>`.
/// `score`/`tags` give an independent live survivor so a purged `name`/`tags`
/// tombstone never leaves a phantom key-only row.
fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "gc_ks".to_string(),
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
            col("score", "int", true),
            col("tags", "set<text>", true),
        ],
        comments: HashMap::new(),
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

/// A row with a `name` CELL TOMBSTONE stamped at explicit `ldt_secs`, plus a
/// LIVE `score` cell so the row survives regardless of the tombstone's fate.
fn write_name_cell_tombstone(id: i32, ck: i32, score: i32, ts: i64, ldt_secs: i32) -> Mutation {
    Mutation::new(
        TableId::new("gc_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![
            CellOperation::Delete {
                column: "name".to_string(),
                local_deletion_time: None, // stamped by with_local_deletion_time below
            },
            CellOperation::Write {
                column: "score".to_string(),
                value: Value::Integer(score),
            },
        ],
        ts,
        None,
    )
    .with_local_deletion_time(ldt_secs)
}

/// A plain live row (used as an older covered cell for the shadow scenario).
fn write_live_name(id: i32, ck: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("gc_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }],
        ts,
        None,
    )
}

/// A row with a ROW TOMBSTONE (coexisting with a live `score` cell) whose
/// `localDeletionTime` is stamped at explicit `ldt_secs` and whose
/// `markedForDeleteAt` is `del_ts` (microseconds). `with_row_tombstone` decouples
/// the deletion from the row's liveness timestamp so the row keeps its live
/// `score` cell while carrying the row deletion — matching the row-deletion purge
/// site (a real, non-zero, surfaced `row_del_ldt`).
fn write_row_tombstone_with_live(
    id: i32,
    ck: i32,
    score: i32,
    row_ts: i64,
    del_ts: i64,
    ldt_secs: i32,
) -> Mutation {
    Mutation::new(
        TableId::new("gc_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "score".to_string(),
            value: Value::Integer(score),
        }],
        row_ts,
        None,
    )
    .with_row_tombstone(del_ts, ldt_secs)
}

/// A row with a `tags` COMPLEX-DELETION MARKER stamped at explicit `ldt_secs`
/// (marked-for-delete-at `mfda` micros), a LIVE `tags` element written strictly
/// AFTER `mfda` so an element survives the marker, and a live `score` cell so the
/// row survives. Drives the complex-deletion-marker purge site.
fn write_tags_complex_deletion_with_live(
    id: i32,
    ck: i32,
    score: i32,
    row_ts: i64,
    mfda: i64,
    ldt_secs: i32,
    live_elem_ts: i64,
) -> Mutation {
    Mutation::new(
        TableId::new("gc_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![
            CellOperation::ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: mfda,
                local_deletion_time: ldt_secs,
            },
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                // SET member identity lives in the cell path; empty value.
                cell_path: b"survivor".to_vec(),
                value: None,
                timestamp_micros: live_elem_ts,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
            CellOperation::Write {
                column: "score".to_string(),
                value: Value::Integer(score),
            },
        ],
        row_ts,
        None,
    )
}

// ===========================================================================
// Runtime + I/O helpers (mirrors issue #1382 harness)
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

/// Filter out empty (0-byte) Data.db files: a fully-purged compaction produces an
/// empty output the reader cannot (and need not) parse.
fn non_empty(inputs: &[PathBuf]) -> Vec<PathBuf> {
    inputs
        .iter()
        .filter(|p| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false))
        .cloned()
        .collect()
}

/// Collect every merged [`MergeEntry`] from a directory of SSTables via the merge
/// read path with expiry/purge DISABLED (`gc_before = None`, `now = None`,
/// `purge_safe = false`) so the RAW on-disk state after compaction is observed.
fn read_entries(inputs: &[PathBuf], schema: &TableSchema) -> Vec<MergeEntry> {
    let inputs = non_empty(inputs);
    if inputs.is_empty() {
        return Vec::new();
    }
    let mut merger = KWayMerger::new_with_gc(inputs, schema, None, None).expect("merger");
    let mut out = Vec::new();
    loop {
        match merger.step().expect("step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => out.extend(rows),
        }
    }
    out
}

/// Read the authoritative on-disk `localDeletionTime` (GC-clock seconds) of the
/// single `name` CELL TOMBSTONE in `inputs`. A simple cell tombstone surfaces its
/// LDT in the `Value::Tombstone` payload (issue #921 finding 1), which is exactly
/// what the purge stage compares (`cell_effective_ldt`).
fn cell_tombstone_ldt(inputs: &[PathBuf], schema: &TableSchema) -> i64 {
    for entry in read_entries(inputs, schema) {
        if let RowData::Live { cells } = &entry.row_data {
            for c in cells {
                if c.column == "name" {
                    if let Value::Tombstone(info) = &c.value {
                        if info.tombstone_type == TombstoneType::CellTombstone {
                            return info.local_deletion_time;
                        }
                    }
                }
            }
        }
    }
    panic!("no `name` cell tombstone with a localDeletionTime found in inputs");
}

/// Read the authoritative on-disk row-tombstone `localDeletionTime` (GC seconds).
fn row_tombstone_ldt(inputs: &[PathBuf], schema: &TableSchema) -> i64 {
    for entry in read_entries(inputs, schema) {
        if let RowData::Tombstone {
            local_deletion_time,
            ..
        } = &entry.row_data
        {
            return i64::from(*local_deletion_time);
        }
    }
    panic!("no row tombstone found in inputs");
}

/// Read the authoritative on-disk complex-deletion marker `localDeletionTime`.
fn complex_deletion_ldt(inputs: &[PathBuf], schema: &TableSchema, column: &str) -> i64 {
    for entry in read_entries(inputs, schema) {
        for cd in &entry.complex_deletions {
            if cd.column == column {
                return i64::from(cd.local_deletion_time);
            }
        }
    }
    panic!("no `{column}` complex-deletion marker found in inputs");
}

/// Run the real one-shot compaction with a PINNED `gc_before_secs`/`now_secs` and
/// full-compaction (`purge_safe = true`) semantics, then return the compacted
/// output's merged entries.
fn compact_and_read(
    inputs: Vec<PathBuf>,
    out_dir: &Path,
    schema: &TableSchema,
    gc_before: i64,
    now_secs: i64,
) -> Vec<MergeEntry> {
    rt().block_on(compact_sstables(
        inputs,
        out_dir,
        schema,
        1,
        Some(gc_before),
        Some(now_secs),
        true,
    ))
    .expect("compaction succeeds");
    read_entries(&discover_inputs(out_dir), schema)
}

/// The single surviving `name` cell tombstone in a compacted output, if any.
fn find_name_cell_tombstone(entries: &[MergeEntry]) -> Option<&CellData> {
    for entry in entries {
        if let RowData::Live { cells } = &entry.row_data {
            for c in cells {
                if c.column == "name" {
                    if let Value::Tombstone(info) = &c.value {
                        if info.tombstone_type == TombstoneType::CellTombstone {
                            return Some(c);
                        }
                    }
                }
            }
        }
    }
    None
}

/// Whether a `score` live cell survived (proves the row was NOT dropped whole).
fn has_live_score(entries: &[MergeEntry]) -> bool {
    entries.iter().any(|e| match &e.row_data {
        RowData::Live { cells } => cells
            .iter()
            .any(|c| c.column == "score" && matches!(c.value, Value::Integer(_))),
        RowData::Tombstone { .. } => false,
    })
}

/// The single surviving complex-deletion marker for `column`, if any.
fn find_complex_deletion<'a>(
    entries: &'a [MergeEntry],
    column: &str,
) -> Option<&'a ComplexDeletion> {
    entries
        .iter()
        .flat_map(|e| e.complex_deletions.iter())
        .find(|cd| cd.column == column)
}

/// Whether a row tombstone survived in the compacted output.
fn has_row_tombstone(entries: &[MergeEntry]) -> bool {
    entries
        .iter()
        .any(|e| matches!(e.row_data, RowData::Tombstone { .. }))
}

// ===========================================================================
// Criterion 1 — cell tombstone: boundary retained, one-below purged
// ===========================================================================

#[test]
fn cell_tombstone_boundary_retained_at_gc_before() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    // Stamp the cell tombstone at an explicit, far-in-the-past LDT.
    let stamped_ldt: i32 = 1_600_000_000;
    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![write_name_cell_tombstone(1, 0, 7, 100, stamped_ldt)],
    );
    let inputs = discover_inputs(&in_dir);
    assert!(!inputs.is_empty(), "expected an input SSTable");

    // Authoritative on-disk LDT read back; pin gcBefore == ldt → NOT (ldt < gc).
    let ldt = cell_tombstone_ldt(&inputs, &schema);
    let now_secs = ldt + 10_000_000; // far future: TTL stage is a no-op here
    let gc_before = ldt; // ldt == gcBefore → RETAINED (strict `<` is false)

    let out = compact_and_read(inputs, &out_dir, &schema, gc_before, now_secs);
    let cell = find_name_cell_tombstone(&out).unwrap_or_else(|| {
        panic!("cell tombstone at ldt == gcBefore must be RETAINED, entries: {out:?}")
    });
    match &cell.value {
        Value::Tombstone(info) => assert_eq!(
            info.local_deletion_time, ldt,
            "retained tombstone keeps its original LDT"
        ),
        other => panic!("expected a retained CellTombstone, got {other:?}"),
    }
    assert!(has_live_score(&out), "the live `score` cell must survive");
}

#[test]
fn cell_tombstone_boundary_purged_one_second_below() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    let stamped_ldt: i32 = 1_600_000_000;
    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![write_name_cell_tombstone(1, 0, 7, 100, stamped_ldt)],
    );
    let inputs = discover_inputs(&in_dir);
    let ldt = cell_tombstone_ldt(&inputs, &schema);
    let now_secs = ldt + 10_000_000;
    // ldt == gcBefore - 1  ⟺  gcBefore == ldt + 1  ⟺  ldt < gcBefore → PURGED.
    let gc_before = ldt + 1;

    let out = compact_and_read(inputs, &out_dir, &schema, gc_before, now_secs);
    assert!(
        find_name_cell_tombstone(&out).is_none(),
        "cell tombstone one second below gcBefore must be PURGED (observed absence == \
         PurgeCounts.cell_tombstones == 1), entries: {out:?}"
    );
    // The independent live `score` cell keeps the row present (not a phantom drop).
    assert!(
        has_live_score(&out),
        "purging only the `name` tombstone must leave the live `score` cell present"
    );
}

// ===========================================================================
// Criterion 2 — row tombstone: boundary emitted, one-below omitted
// ===========================================================================

// NOTE (issue #1721): a direct `WriteEngine` flush of a decoupled row tombstone
// (`Mutation::with_row_tombstone`) whose LDT is below the incremental
// `min_local_deletion_time` baseline HARD-ERRORS at flush time — the writer's
// per-partition stats loop folds `Delete`/`DeleteRow`/`ComplexDeletion`/
// `partition_tombstone`/`range_tombstones` LDTs but NOT the `row_tombstone`
// field, so the below-baseline guard (rows.rs:954) rejects the row. This is a
// real writer stats bug (distinct from the #1410 baseline class), not a test
// error, so per the test-authoring rule these two boundary tests are ignored
// pending #1721 rather than fixed inline. Un-ignore when #1721 lands.
#[test]
#[ignore = "blocked on #1721 (row_tombstone LDT dropped from flush stats)"]
fn row_tombstone_boundary_retained_at_gc_before() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    let stamped_ldt: i32 = 1_600_000_000;
    // Row deletion at markedForDeleteAt = 100 micros, LDT stamped explicitly.
    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![write_row_tombstone_with_live(
            1,
            0,
            7,
            500,
            100,
            stamped_ldt,
        )],
    );
    let inputs = discover_inputs(&in_dir);
    let ldt = row_tombstone_ldt(&inputs, &schema);
    let now_secs = ldt + 10_000_000;
    let gc_before = ldt; // ldt == gcBefore → row deletion RETAINED

    let out = compact_and_read(inputs, &out_dir, &schema, gc_before, now_secs);
    assert!(
        has_row_tombstone(&out),
        "row tombstone at ldt == gcBefore must be RETAINED (emitted), entries: {out:?}"
    );
    // The row's coexisting live `score` (issue #932 decoupled deletion) survives.
    assert!(
        has_live_score(&out),
        "the row's coexisting live `score` cell must survive"
    );
}

#[test]
#[ignore = "blocked on #1721 (row_tombstone LDT dropped from flush stats)"]
fn row_tombstone_boundary_purged_one_second_below() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    let stamped_ldt: i32 = 1_600_000_000;
    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![write_row_tombstone_with_live(
            1,
            0,
            7,
            500,
            100,
            stamped_ldt,
        )],
    );
    let inputs = discover_inputs(&in_dir);
    let ldt = row_tombstone_ldt(&inputs, &schema);
    let now_secs = ldt + 10_000_000;
    let gc_before = ldt + 1; // ldt == gcBefore - 1 → row deletion PURGED

    let out = compact_and_read(inputs, &out_dir, &schema, gc_before, now_secs);
    assert!(
        !has_row_tombstone(&out),
        "row tombstone one second below gcBefore must be PURGED (omitted == \
         PurgeCounts.row_tombstones == 1), entries: {out:?}"
    );
    assert!(
        has_live_score(&out),
        "purging the row deletion must leave the coexisting live `score` cell present"
    );
}

// ===========================================================================
// Criterion 3 — complex-deletion marker: boundary present, one-below absent
// ===========================================================================

#[test]
fn complex_deletion_boundary_retained_at_gc_before() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    let stamped_ldt: i32 = 1_600_000_000;
    // Marker mfda = 100 micros; a live element at ts = 300 > mfda survives it.
    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![write_tags_complex_deletion_with_live(
            1,
            0,
            7,
            500,
            100,
            stamped_ldt,
            300,
        )],
    );
    let inputs = discover_inputs(&in_dir);
    let ldt = complex_deletion_ldt(&inputs, &schema, "tags");
    let now_secs = ldt + 10_000_000;
    let gc_before = ldt; // ldt == gcBefore → marker RETAINED

    let out = compact_and_read(inputs, &out_dir, &schema, gc_before, now_secs);
    let cd = find_complex_deletion(&out, "tags").unwrap_or_else(|| {
        panic!("complex-deletion marker at ldt == gcBefore must be RETAINED, entries: {out:?}")
    });
    assert_eq!(
        i64::from(cd.local_deletion_time),
        ldt,
        "retained marker keeps its original LDT"
    );
    assert!(has_live_score(&out), "the live `score` cell must survive");
}

#[test]
fn complex_deletion_boundary_purged_one_second_below() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    let stamped_ldt: i32 = 1_600_000_000;
    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![write_tags_complex_deletion_with_live(
            1,
            0,
            7,
            500,
            100,
            stamped_ldt,
            300,
        )],
    );
    let inputs = discover_inputs(&in_dir);
    let ldt = complex_deletion_ldt(&inputs, &schema, "tags");
    let now_secs = ldt + 10_000_000;
    let gc_before = ldt + 1; // ldt == gcBefore - 1 → marker PURGED

    let out = compact_and_read(inputs, &out_dir, &schema, gc_before, now_secs);
    assert!(
        find_complex_deletion(&out, "tags").is_none(),
        "complex-deletion marker one second below gcBefore must be PURGED (absent == \
         PurgeCounts.complex_deletions == 1), entries: {out:?}"
    );
    assert!(
        has_live_score(&out),
        "purging the marker must leave the live `score` cell present"
    );
}

// ===========================================================================
// Criterion 4 — shadow-before-purge at the boundary: a purgeable-at-boundary
// (ldt == gcBefore - 1) cell tombstone STILL shadows an older covered `name`
// cell before it is purged. Result: covered cell ABSENT and tombstone ABSENT.
// ===========================================================================

#[test]
fn shadow_before_purge_at_boundary() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    // Two contributors to the SAME (id=1, ck=0) row's `name` column:
    //   * an OLDER live `name` cell at write ts = 50 (covered), and
    //   * a NEWER `name` cell tombstone at write ts = 100 (shadows the older cell)
    //     stamped at an explicit LDT; a live `score` keeps the row present.
    // The tombstone's write ts (100) > the covered cell's (50) so it shadows;
    // full compaction → the #935 overlap gate is +inf so the purge is allowed.
    let stamped_ldt: i32 = 1_600_000_000;
    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![
            write_live_name(1, 0, "covered", 50),
            write_name_cell_tombstone(1, 0, 7, 100, stamped_ldt),
        ],
    );
    let inputs = discover_inputs(&in_dir);
    let ldt = cell_tombstone_ldt(&inputs, &schema);
    let now_secs = ldt + 10_000_000;
    let gc_before = ldt + 1; // ldt == gcBefore - 1 → tombstone is purgeable

    let out = compact_and_read(inputs, &out_dir, &schema, gc_before, now_secs);

    // The covered older `name` cell must be ABSENT (shadowed by the tombstone
    // BEFORE the tombstone itself was purged — no resurrection).
    let name_values: Vec<&Value> = out
        .iter()
        .filter_map(|e| match &e.row_data {
            RowData::Live { cells } => Some(cells),
            RowData::Tombstone { .. } => None,
        })
        .flat_map(|cells| cells.iter())
        .filter(|c| c.column == "name")
        .map(|c| &c.value)
        .collect();
    assert!(
        !name_values
            .iter()
            .any(|v| matches!(v, Value::Text(t) if t == "covered")),
        "the older covered `name` cell must be SHADOWED (absent), not resurrected; \
         found name values: {name_values:?}"
    );
    // The tombstone itself must be ABSENT (purged at the boundary).
    assert!(
        find_name_cell_tombstone(&out).is_none(),
        "the purgeable-at-boundary `name` tombstone must be PURGED (absent), entries: {out:?}"
    );
    // The independent live `score` keeps the row present (not a phantom drop).
    assert!(
        has_live_score(&out),
        "the live `score` cell must survive the shadow-then-purge"
    );
}
