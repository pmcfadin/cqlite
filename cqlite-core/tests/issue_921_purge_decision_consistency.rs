//! Issue #921 (epic) — purge-decision consistency between the dropped-column
//! survivor PRE-PASS and the real merge WRITE PASS, and per-cell tombstone LDT
//! preservation through the writer.
//!
//! FINDING 1 (pre-pass vs write-pass purge mismatch): `compact_sstables` runs a
//! merge PRE-PASS (`compute_surviving_dropped_columns`) to decide which dropped
//! columns still have surviving cells (so the output serialization header retains
//! them) before the real WRITE PASS. Before the fix the pre-pass built its merger
//! with the DEFAULT `purge_safe = false`, while a major compaction's write pass
//! used `purge_safe = true`. A purgeable cell tombstone in a dropped column was
//! therefore counted as a SURVIVOR by the pre-pass (column retained in the
//! header) yet PURGED by the write pass — leaving an empty dropped column in the
//! output header that defeats the post-drop header stripping. The fix threads the
//! SAME `purge_safe` (and gc cutoff) into the pre-pass so the two agree.
//!
//! FINDING 2 (retained cell-tombstone LDT lost through writer): a retained simple
//! cell tombstone's SOURCE `localDeletionTime` was dropped when converting to
//! `CellOperation::Delete`; the writer then re-stamped it with a mutation-derived
//! LDT. A within-grace cell tombstone that SURVIVES this compaction could thus
//! get a DIFFERENT GC clock in the output → purged too early / kept too long in a
//! LATER compaction. The fix preserves the per-cell tombstone's own LDT through
//! the mutation→writer path.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::{CompactionRowData, SSTableReader};
use cqlite_core::storage::write_engine::merge::{compact_sstables, MergeStep, RowData};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::Config;
use tempfile::TempDir;

const KEYSPACE: &str = "purge_ks";
const TABLE: &str = "items";

/// Schema: PK=id(int), CK=ck(int), columns name(text), score(int). `name` sorts
/// before `score` in the serialization header, so a stale (empty) `name` entry
/// in the output header would misalign `score` for a post-drop reader.
fn schema_with_drops(dropped: HashMap<String, i64>) -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
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
            col("id", "int"),
            col("ck", "int"),
            col("name", "text"),
            col("score", "int"),
        ],
        comments: HashMap::new(),
        dropped_columns: dropped,
    }
}

fn col(name: &str, ty: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// A row whose `name` column carries ONLY a simple cell tombstone (with an
/// explicit, far-in-the-past `localDeletionTime`) and whose `score` column is a
/// live integer. The tombstone is written at `ts` (microseconds) and stamped with
/// `ldt_secs` (the GC clock the purge stage compares against `gcBefore`).
fn write_name_tombstone_score_live(
    id: i32,
    ck: i32,
    score: i32,
    ts: i64,
    name_ldt_secs: i32,
) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![
            CellOperation::Delete {
                column: "name".to_string(),
                // The mutation-level LDT below would otherwise stamp this; the
                // writer honors the per-op LDT (#921 finding 2) — here it equals
                // the mutation LDT, so this test isolates Finding 1.
                local_deletion_time: None,
            },
            CellOperation::Write {
                column: "score".to_string(),
                value: Value::Integer(score),
            },
        ],
        ts,
        None,
    )
    .with_local_deletion_time(name_ldt_secs)
}

/// Flush `mutations` into one SSTable and return its Data.db input paths.
fn build_input(temp: &TempDir, mutations: Vec<Mutation>) -> Vec<PathBuf> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let schema = schema_with_drops(HashMap::new());

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in mutations {
        engine.write(m).expect("write row");
    }
    rt.block_on(engine.flush()).expect("flush").expect("info");
    rt.block_on(engine.close()).expect("close");

    discover_inputs(&data_dir)
}

fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p
                    .file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
                {
                    out.push(p);
                }
            }
        }
    }
    walk(dir, &mut out);
    out
}

/// Run a MAJOR compaction (`purge_safe = true`) with explicit gc settings.
fn compact_major(
    inputs: Vec<PathBuf>,
    out_dir: &Path,
    schema: &TableSchema,
    gc_before_secs: i64,
    now_secs: i64,
) -> PathBuf {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt
        .block_on(compact_sstables(
            inputs,
            out_dir,
            schema,
            921,
            Some(gc_before_secs),
            Some(now_secs),
            true, // purge_safe: major compaction spanning every SSTable
        ))
        .expect("compaction must succeed");
    report.output.data_path
}

/// Schema describing the table AFTER `name` was dropped: it omits `name`
/// entirely (the natural post-drop reader schema). Reading the compacted output
/// with this schema proves the dropped column is ABSENT from the output header
/// and `score` is not misaligned.
fn post_drop_schema_without_name() -> TableSchema {
    let mut s = schema_with_drops(HashMap::new());
    s.columns.retain(|c| c.name != "name");
    s
}

/// Read surviving (column, value) cells out of `data_path` using `read_schema`.
fn surviving_columns_with(data_path: PathBuf, read_schema: &TableSchema) -> Vec<(String, Value)> {
    let mut merger = KWayMerger::new(vec![data_path], read_schema).expect("merger over output");
    let mut cells = Vec::new();
    loop {
        match merger.step().expect("step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in rows {
                    if let RowData::Live { cells: row_cells } = row.row_data {
                        for c in row_cells {
                            cells.push((c.column, c.value));
                        }
                    }
                }
            }
        }
    }
    cells
}

// ---------------------------------------------------------------------------
// FINDING 1
// ---------------------------------------------------------------------------

/// FINDING 1 AC: in a MAJOR compaction (`purge_safe = true`), a DROPPED column
/// whose only data is a PURGEABLE cell tombstone must NOT be retained in the
/// output serialization header — the survivor pre-pass must make the SAME purge
/// decision as the write pass.
///
/// `name` (dropped, sorts before `score`) carries only a cell tombstone whose
/// `ts > drop_time` (survives the dropped-column filter) but whose
/// `localDeletionTime < gcBefore` (purgeable). Before the fix the pre-pass (with
/// `purge_safe = false`) counts the tombstone as surviving and retains `name` in
/// the header; the write pass (with `purge_safe = true`) then purges it, leaving
/// a STALE empty `name` header entry that misaligns `score` for a post-drop
/// reader. After the fix the pre-pass purges it too, strips `name`, and `score`
/// decodes correctly.
#[test]
fn dropped_column_purgeable_tombstone_not_retained_in_header() {
    let temp = TempDir::new().expect("tempdir");

    // Cell tombstone for `name` at ts=300 (microseconds), stamped with an OLD
    // localDeletionTime of 1000 seconds. `score` is a live cell.
    let name_ldt_secs = 1_000_i32;
    let mutations = vec![write_name_tombstone_score_live(
        1,
        0,
        42,
        300,
        name_ldt_secs,
    )];
    let inputs = build_input(&temp, mutations);
    assert!(!inputs.is_empty(), "expected at least one input SSTable");

    // Drop `name` at drop_time=150 (BEFORE the tombstone's ts=300, so the
    // dropped-column filter alone does NOT remove it — only the gc_grace purge
    // does). `name` sorts before `score`.
    let mut dropped = HashMap::new();
    dropped.insert("name".to_string(), 150_i64);
    let drop_schema = schema_with_drops(dropped);

    // gcBefore = 2000s > name_ldt(1000s) → the tombstone is PURGEABLE. now is in
    // the far future so the live `score` cell is never TTL-expired.
    let gc_before_secs = 2_000_i64;
    let now_secs = 10_000_i64;

    let out_dir = temp.path().join("out");
    let data_path = compact_major(inputs, &out_dir, &drop_schema, gc_before_secs, now_secs);

    // Read the output with a POST-DROP schema that omits `name` entirely.
    let cols = surviving_columns_with(data_path, &post_drop_schema_without_name());

    assert!(
        cols.iter().all(|(c, _)| c != "name"),
        "dropped+purged `name` must not appear in the output, got: {:?}",
        cols.iter().map(|(c, _)| c).collect::<Vec<_>>()
    );
    // `score` must decode as an integer. If the pre-pass had wrongly retained the
    // purged `name` column in the header, a post-drop reader (no `name`) would
    // misalign and fail to surface `score` as a clean integer.
    assert!(
        cols.iter()
            .any(|(c, v)| c == "score" && matches!(v, Value::Integer(42))),
        "surviving `score`=42 must parse correctly under a post-drop reader schema, got: {:?}",
        cols
    );
}

// ---------------------------------------------------------------------------
// FINDING 2
// ---------------------------------------------------------------------------

const F2_KEYSPACE: &str = "ldt_ks";
const F2_TABLE: &str = "items";

/// Schema with no clustering: PK=id(int), columns name(text), data(text).
fn f2_schema() -> TableSchema {
    TableSchema {
        keyspace: F2_KEYSPACE.to_string(),
        table: F2_TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col_in("id", "int", F2_KEYSPACE),
            col_in("name", "text", F2_KEYSPACE),
            col_in("data", "text", F2_KEYSPACE),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn col_in(name: &str, ty: &str, _ks: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// Write one SSTable for the Finding-2 schema and return its Data.db paths.
fn f2_build_input(temp: &TempDir, mutations: Vec<Mutation>) -> Vec<PathBuf> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, f2_schema());
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in mutations {
        engine.write(m).expect("write row");
    }
    rt.block_on(engine.flush()).expect("flush").expect("info");
    rt.block_on(engine.close()).expect("close");

    discover_inputs(&data_dir)
}

fn f2_compact(inputs: Vec<PathBuf>, out_dir: &Path, gc_before_secs: i64, now_secs: i64) -> PathBuf {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt
        .block_on(compact_sstables(
            inputs,
            out_dir,
            &f2_schema(),
            922,
            Some(gc_before_secs),
            Some(now_secs),
            true,
        ))
        .expect("compaction must succeed");
    report.output.data_path
}

/// Read the `name`-column cell tombstone's `localDeletionTime` (seconds) out of
/// the compacted output via the compaction reader's `SimpleCell` surface.
fn read_name_tombstone_ldt(data_path: &Path) -> Option<i32> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let mut config = Config::default();
        config.storage.use_mmap = false;
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let reader = SSTableReader::open(data_path, &config, platform)
            .await
            .expect("open reader");
        let schema = f2_schema();
        let rows = reader
            .iterate_all_partitions_for_compaction(Some(&schema))
            .await
            .expect("iterate compaction rows");
        for row in rows {
            if let CompactionRowData::Live { simple, .. } = &row.row_data {
                for cell in simple {
                    if cell.column == "name" {
                        // A simple cell tombstone carries its localDeletionTime in
                        // its `Value::Tombstone` payload (seconds), NOT in
                        // `SimpleCell.local_deletion_time` (the reader fills that
                        // only for expiring cells). Read the authoritative LDT off
                        // the tombstone value.
                        if let Value::Tombstone(info) = &cell.value {
                            if info.tombstone_type
                                == cqlite_core::types::TombstoneType::CellTombstone
                            {
                                return Some(info.local_deletion_time as i32);
                            }
                        }
                    }
                }
            }
        }
        None
    })
}

/// FINDING 2 AC: a WITHIN-GRACE simple cell tombstone with an explicit, non-zero
/// `localDeletionTime` that SURVIVES compaction must carry the SAME LDT in the
/// output — not a mutation-derived one.
///
/// The `name` cell tombstone is stamped with an explicit `EXPLICIT_LDT` that is
/// DELIBERATELY DIFFERENT from the timestamp-derived value
/// (`timestamp_micros / 1_000_000`). It is within grace (`LDT >= gcBefore`), so
/// it survives the purge. Before the fix the writer re-derived the LDT from the
/// mutation timestamp (a different number); after the fix it preserves the
/// source cell tombstone's own LDT.
#[test]
fn surviving_cell_tombstone_preserves_source_ldt() {
    let temp = TempDir::new().expect("tempdir");

    // ts = 8_000_000_000 µs → timestamp-derived LDT would be 8_000 seconds.
    // EXPLICIT_LDT = 9_000 seconds is BOTH != 8_000 (so a regression that
    // re-derives the LDT from the timestamp is detectable as a DIFFERENT LDT, not
    // merely an early purge) AND, like the derived value, WITHIN grace relative to
    // gcBefore=1_000 (both >= 1_000 → both would be retained). Isolating the
    // LDT-VALUE check this way means the RED is "wrong LDT in the output", exactly
    // the drift Finding 2 fixes.
    const TS_MICROS: i64 = 8_000_000_000;
    const EXPLICIT_LDT: i32 = 9_000;
    const TIMESTAMP_DERIVED_LDT: i32 = (TS_MICROS / 1_000_000) as i32; // = 8_000

    let mutation = Mutation::new(
        TableId::new(F2_KEYSPACE, F2_TABLE),
        PartitionKey::single("id", Value::Integer(7)),
        None,
        vec![
            // `data` stays live so the row is not reduced to an empty/absent row.
            CellOperation::Write {
                column: "data".to_string(),
                value: Value::Text("keep".to_string()),
            },
            CellOperation::Delete {
                column: "name".to_string(),
                local_deletion_time: Some(EXPLICIT_LDT),
            },
        ],
        TS_MICROS,
        None,
    );

    let inputs = f2_build_input(&temp, vec![mutation]);
    assert!(!inputs.is_empty(), "expected at least one input SSTable");

    // gcBefore = 1_000s: EXPLICIT_LDT (9_000) >= gcBefore → the tombstone is
    // WITHIN grace and SURVIVES. now far in the future (no TTL involved).
    let out_dir = temp.path().join("out");
    let data_path = f2_compact(inputs, &out_dir, 1_000, 1_000_000);

    let ldt = read_name_tombstone_ldt(&data_path)
        .expect("a surviving `name` cell tombstone must be present in the output");

    assert_ne!(
        TIMESTAMP_DERIVED_LDT, EXPLICIT_LDT,
        "test precondition: explicit LDT must differ from the timestamp-derived one"
    );
    assert_eq!(
        ldt, EXPLICIT_LDT,
        "surviving cell tombstone must keep its SOURCE localDeletionTime ({EXPLICIT_LDT}), \
         not a mutation-derived one ({TIMESTAMP_DERIVED_LDT}); got {ldt}"
    );
}
