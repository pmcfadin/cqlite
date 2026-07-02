//! Issue #1388: `cqlite compact --major` drops a fully-expired SSTable whole.
//!
//! Acceptance-criterion 1 on the CLI one-shot surface (OQ-1 → (A)): under
//! `--major` (the operator's assertion that `<input-dir>` holds every overlapping
//! SSTable for the table ⇒ empty outside set ⇒ +inf overlap bound), an input
//! SSTable proven fully expired by authoritative `Statistics.db` metadata is
//! DROPPED WHOLE — excluded from the merge, its components reclaimed after publish,
//! and NAMED in the `CompactResult.dropped_whole` plan field (assertable from the
//! plan, not just output absence). Without `--major` no drop occurs.
//!
//! Drives the REAL CLI handler `cqlite_cli::commands::write::handle_compact`.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_cli::cli_types::CompactArgs;
use cqlite_cli::commands::write::handle_compact;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

const GC_BEFORE: i64 = 5_000;
const NOW_SECS: i64 = 10_000;
const TOMB_LDT: i32 = 100;

fn schema() -> TableSchema {
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

fn schema_cql() -> &'static str {
    "CREATE TABLE exp_ks.items (id int, ck int, name text, PRIMARY KEY (id, ck));"
}

fn delete_row(id: i32, ck: i32, ldt: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("exp_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
    .with_local_deletion_time(ldt)
}

fn write_live_row(id: i32, ck: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("exp_ks", "items"),
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

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

/// Flush one SSTable into `data_dir/<sub>` (a distinct subdir so generations do
/// not collide), returning the discovered Data.db path.
fn flush_one(data_dir: &Path, sub: &str, muts: Vec<Mutation>) -> PathBuf {
    let sch = schema();
    let sub_dir = data_dir.join(sub);
    let wal = data_dir.join(format!("{sub}_wal"));
    let config = WriteEngineConfig::new(sub_dir.clone(), wal, sch.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in muts {
        engine.write(m).expect("write");
    }
    let r = rt();
    r.block_on(engine.flush()).expect("flush").expect("info");
    r.block_on(engine.close()).expect("close");
    discover(&sub_dir).into_iter().next().expect("one Data.db")
}

fn discover(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(dir, &mut out, 8);
    out
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>, depth: usize) {
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
            if path.with_file_name(format!("{base}-TOC.txt")).exists() {
                out.push(path);
            }
        } else if depth > 0 && path.is_dir() {
            walk(&path, out, depth - 1);
        }
    }
}

fn write_schema_file(dir: &Path) -> PathBuf {
    let p = dir.join("schema.cql");
    std::fs::write(&p, schema_cql()).expect("write schema");
    p
}

#[test]
fn compact_major_drops_expired_sstable_and_names_it() {
    let temp = TempDir::new().unwrap();
    let input_dir = temp.path().join("input");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&input_dir).unwrap();

    // Two SSTables under the SAME input_dir (distinct subdirs): one fully expired
    // (row tombstones, tiny LDT, low write ts), one live (higher write ts).
    let expired = flush_one(
        &input_dir,
        "expired",
        vec![
            delete_row(1, 0, TOMB_LDT, 100),
            delete_row(2, 0, TOMB_LDT, 100),
        ],
    );
    let _live = flush_one(
        &input_dir,
        "live",
        vec![write_live_row(10, 0, "alive", 5_000_000)],
    );

    let schema_path = write_schema_file(temp.path());

    let args = CompactArgs {
        input_dir: input_dir.clone(),
        output: out_dir.clone(),
        schema: schema_path,
        gc_before: Some(GC_BEFORE),
        now_sec: Some(NOW_SECS),
        generation: 1,
        major: true,
    };

    let result = rt()
        .block_on(handle_compact(&args))
        .expect("compact --major");

    // The plan names exactly the expired SSTable as dropped whole (assertable).
    assert_eq!(
        result.dropped_whole,
        vec![expired.clone()],
        "compact --major must name the fully-expired SSTable in the dropped-whole plan set"
    );
    // Only the live SSTable was merged.
    assert_eq!(
        result.input_files, 1,
        "only the live SSTable was fed to the merger"
    );
    // The dropped SSTable's Data.db is reclaimed after publish.
    assert!(
        !expired.exists(),
        "dropped-whole SSTable reclaimed after publish"
    );
    // The output holds the live row (one partition/row).
    assert_eq!(result.output_rows, 1, "output holds only the live row");
}

#[test]
fn compact_without_major_does_not_drop() {
    let temp = TempDir::new().unwrap();
    let input_dir = temp.path().join("input");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&input_dir).unwrap();

    let expired = flush_one(&input_dir, "expired", vec![delete_row(1, 0, TOMB_LDT, 100)]);
    let _live = flush_one(
        &input_dir,
        "live",
        vec![write_live_row(10, 0, "alive", 5_000_000)],
    );
    let schema_path = write_schema_file(temp.path());

    let args = CompactArgs {
        input_dir: input_dir.clone(),
        output: out_dir.clone(),
        schema: schema_path,
        gc_before: Some(GC_BEFORE),
        now_sec: Some(NOW_SECS),
        generation: 1,
        major: false, // conservative: no drop (OQ-1 → (A))
    };

    let result = rt()
        .block_on(handle_compact(&args))
        .expect("compact (non-major)");
    assert!(
        result.dropped_whole.is_empty(),
        "a non-major compaction must never drop whole"
    );
    assert_eq!(
        result.input_files, 2,
        "both inputs were merged (nothing dropped)"
    );
    // The one-shot CLI compaction does NOT delete its inputs (only dropped-whole
    // SSTables are reclaimed); the expired input remains on disk, having been
    // merged through the normal path rather than dropped.
    assert!(
        expired.exists(),
        "a non-major compaction leaves its (merged) inputs in place; only drops are reclaimed"
    );
}
