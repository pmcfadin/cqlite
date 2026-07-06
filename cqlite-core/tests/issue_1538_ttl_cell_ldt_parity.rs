//! Issue #1538: a surviving live TTL cell must be re-emitted byte-identically
//! after compaction — `CellOperation::WriteWithTtl` carries an authoritative
//! per-cell `localDeletionTime`.
//!
//! Cassandra serializes an expiring cell with BOTH its `ttl` AND its
//! `localExpirationTime` (= `writetime_seconds + ttl`). Before #1538 the CQLite
//! writer derived the expiring cell's `localDeletionTime` from
//! `SystemTime::now() + ttl` and the compaction merge dropped the source cell's
//! LDT entirely (`cells_to_cell_operations` built `WriteWithTtl { column, value,
//! ttl_seconds }`), so a live TTL cell that survived a compaction was re-stamped
//! with a fresh `now + ttl` LDT — NOT byte-identical to the source cell / to
//! Cassandra's compaction output.
//!
//! These tests pin the invariant with an AUTHORITATIVE, DETERMINISTIC per-cell
//! LDT (no wall-clock sampling): a `WriteWithTtl` carrying an explicit
//! `local_deletion_time` is stamped VERBATIM, and a live TTL cell that survives a
//! compaction keeps that exact LDT.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{compact_sstables, CellData, MergeStep, RowData};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

// A writetime clearly in the past + a large TTL, so the pinned expiration
// (`writetime_s + ttl`) is deterministic and distinct from any wall-clock
// `now + ttl` the buggy writer would derive at flush/compaction time.
const WRITETIME_MICROS: i64 = 1_600_000_000_000_000; // 2020-09-13T12:26:40Z
const TTL_SECONDS: u32 = 10_000_000;
// Authoritative per-cell localDeletionTime = writetime_seconds + ttl.
const PINNED_LDT: i32 = (WRITETIME_MICROS / 1_000_000) as i32 + TTL_SECONDS as i32; // 1_610_000_000

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "ttl_ks".to_string(),
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
        columns: vec![col("id", "int"), col("ck", "int"), col("name", "text")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
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

/// A row whose `name` cell is an expiring cell carrying an AUTHORITATIVE pinned
/// per-cell `localDeletionTime` (issue #1538).
fn pinned_ttl_row(id: i32, ck: i32, name: &str) -> Mutation {
    Mutation::new(
        TableId::new("ttl_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::WriteWithTtl {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
            ttl_seconds: TTL_SECONDS,
            local_deletion_time: Some(PINNED_LDT),
        }],
        WRITETIME_MICROS,
        None,
    )
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

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

fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect(dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect(dir: &Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    if depth == 0 {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            collect(&p, out, depth - 1);
        } else if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
            if name.ends_with("-big-Data.db") {
                let gen = name
                    .split('-')
                    .nth(1)
                    .and_then(|g| g.parse::<u64>().ok())
                    .unwrap_or(0);
                out.push((gen, p));
            }
        }
    }
}

/// Read every surviving `name` cell back through the merge read path with expiry
/// DISABLED (`now_secs = None`) so the RAW on-disk cell state (including its
/// authoritative `localDeletionTime`) is observed.
fn read_name_cells(inputs: &[PathBuf], schema: &TableSchema) -> Vec<CellData> {
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
        .with_purge_safe(false);
    let mut cells = Vec::new();
    loop {
        match merger.step().expect("step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    if let RowData::Live { cells: row_cells } = &entry.row_data {
                        for c in row_cells {
                            if c.column == "name" {
                                cells.push(c.clone());
                            }
                        }
                    }
                }
            }
        }
    }
    cells
}

fn ldt_of(cell: &CellData) -> i32 {
    cell.local_deletion_time
        .expect("expiring cell must carry a localDeletionTime")
}

// ===========================================================================
// (1) Direct flush: WriteWithTtl stamps the authoritative per-cell LDT verbatim
// ===========================================================================

#[test]
fn write_with_ttl_stamps_authoritative_local_deletion_time() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let schema = make_schema();

    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![pinned_ttl_row(1, 0, "alive")],
    );
    let inputs = discover_inputs(&in_dir);
    assert!(!inputs.is_empty(), "expected an input SSTable");

    let cells = read_name_cells(&inputs, &schema);
    assert_eq!(cells.len(), 1, "expected exactly one name cell");
    assert_eq!(cells[0].value, Value::Text("alive".to_string()));
    assert_eq!(cells[0].ttl, Some(TTL_SECONDS), "TTL preserved");
    // The on-disk localDeletionTime is the AUTHORITATIVE pinned value, NOT a
    // wall-clock-derived `now + ttl`.
    assert_eq!(
        ldt_of(&cells[0]),
        PINNED_LDT,
        "WriteWithTtl must stamp the authoritative per-cell localDeletionTime verbatim \
         (got {}, expected {PINNED_LDT})",
        ldt_of(&cells[0])
    );
}

// ===========================================================================
// (2) A surviving live TTL cell keeps its EXACT LDT through a compaction
// ===========================================================================

#[test]
fn surviving_live_ttl_cell_ldt_preserved_through_compaction() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![pinned_ttl_row(1, 0, "alive")],
    );
    let inputs = discover_inputs(&in_dir);
    assert!(!inputs.is_empty(), "expected an input SSTable");

    // Sanity: the input carries the authoritative LDT.
    let in_ldt = ldt_of(&read_name_cells(&inputs, &schema)[0]);
    assert_eq!(in_ldt, PINNED_LDT, "input cell carries the pinned LDT");

    // Pin the compaction evaluation instant strictly BEFORE the expiration so the
    // cell is genuinely live (survives). `gc_before` well below the creation time
    // (`ldt - ttl`) keeps any purge gate inert. No wall-clock is sampled.
    let now_secs = i64::from(PINNED_LDT) - 1;
    let gc_before = Some(i64::from(PINNED_LDT) - i64::from(TTL_SECONDS) - 1);

    rt().block_on(compact_sstables(
        inputs,
        &out_dir,
        &schema,
        1,
        gc_before,
        Some(now_secs),
        true,
    ))
    .expect("compaction succeeds");

    let out_inputs = discover_inputs(&out_dir);
    let cells = read_name_cells(&out_inputs, &schema);
    assert_eq!(cells.len(), 1, "live TTL cell must survive compaction");
    assert_eq!(
        cells[0].value,
        Value::Text("alive".to_string()),
        "value survives live"
    );
    assert_eq!(cells[0].ttl, Some(TTL_SECONDS), "TTL preserved");
    // The whole point of #1538: the surviving cell's localDeletionTime is the
    // ORIGINAL per-cell value, byte-identical to the source — NOT recomputed from
    // the compaction wall clock.
    assert_eq!(
        ldt_of(&cells[0]),
        PINNED_LDT,
        "surviving live TTL cell must keep its authoritative localDeletionTime \
         through compaction (got {}, expected {PINNED_LDT})",
        ldt_of(&cells[0])
    );
}
