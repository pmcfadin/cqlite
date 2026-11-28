//! Issue #4666: `cqlite compact --major` must not remove ANY input until the output generation is
//! published (Cassandra: `LifecycleTransaction` obsoletes originals only after commit).
//!
//! 0.19 backport: 0.19's `compact` writes the output straight into `--output` (no staged publish),
//! so the "publish" is `SSTableWriter::finish` (TOC last, fsynced) plus an output-directory fsync.
//! The fault is real: the built binary is killed through the debug-only
//! `CQLITE_TEST_COMPACT_ABORT_BEFORE_RECLAIM` seam, AFTER compaction ran and the output is durable
//! but BEFORE any input is removed.
//! Inputs are Cassandra-format SSTables flushed by the write engine: one fully expired (row
//! tombstones past gc_grace) and one live, so the drop-set classifier selects the expired one.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

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
            value: Value::text(name.to_string()),
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
    let written = discover(&sub_dir).into_iter().next().expect("one Data.db");
    relocate_flat(&written, data_dir)
}

/// `compact` reads ONE table directory (#4626, G8: no recursion), so move the flushed generation
/// into `data_dir` itself under the next free sequence id (each flush wrote `nb-1-big-*`).
fn relocate_flat(written: &Path, data_dir: &Path) -> PathBuf {
    let taken = std::fs::read_dir(data_dir)
        .expect("read data_dir")
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with("-big-Data.db"))
        .count();
    let n = taken + 1;
    let src_dir = written.parent().expect("parent");
    for entry in std::fs::read_dir(src_dir).expect("read stage").flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(rest) = name.strip_prefix("nb-1-big-") {
            std::fs::rename(entry.path(), data_dir.join(format!("nb-{n}-big-{rest}")))
                .expect("move component");
        }
    }
    data_dir.join(format!("nb-{n}-big-Data.db"))
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

/// (tempdir, input dir, expired Data.db, live Data.db, schema file)
fn stage() -> (TempDir, PathBuf, PathBuf, PathBuf, PathBuf) {
    let temp = TempDir::new().unwrap();
    let input_dir = temp.path().join("input");
    std::fs::create_dir_all(&input_dir).unwrap();
    let expired = flush_one(
        &input_dir,
        "expired",
        vec![
            delete_row(1, 0, TOMB_LDT, 100),
            delete_row(2, 0, TOMB_LDT, 100),
        ],
    );
    let live = flush_one(
        &input_dir,
        "live",
        vec![write_live_row(10, 0, "alive", 5_000_000)],
    );
    let schema_path = write_schema_file(temp.path());
    (temp, input_dir, expired, live, schema_path)
}

fn compact_bin(input: &Path, out: &Path, schema: &Path, abort: bool) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_cqlite"));
    cmd.arg("compact")
        .arg(input)
        .arg("--output")
        .arg(out)
        .arg("--schema")
        .arg(schema)
        .args(["--major", "--gc-before", "5000", "--now-sec", "10000"])
        .args(["--generation", "9"]);
    if abort {
        cmd.env("CQLITE_TEST_COMPACT_ABORT_BEFORE_RECLAIM", "1");
    } else {
        cmd.env_remove("CQLITE_TEST_COMPACT_ABORT_BEFORE_RECLAIM");
    }
    cmd.output().expect("spawn cqlite")
}

/// Every component of the generation `data` belongs to.
fn components(data: &Path) -> Vec<PathBuf> {
    let base = data
        .to_string_lossy()
        .trim_end_matches("Data.db")
        .to_string();
    let dir = data.parent().unwrap();
    let prefix = Path::new(&base)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(&prefix)
        })
        .collect()
}

fn out_published(out: &Path) -> Vec<PathBuf> {
    // The output is written straight into `<out>/<keyspace>/<table>` (no staging dir in 0.19).
    let dir = out.join("exp_ks").join("items");
    let Ok(rd) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    rd.flatten()
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with("-big-Data.db"))
        // Published means TOC.txt is present too (TOC is written LAST).
        .filter(|p| {
            let s = p.to_string_lossy();
            std::path::Path::new(&format!("{}TOC.txt", &s[..s.len() - "Data.db".len()])).exists()
        })
        .collect()
}

/// The core of the defect: a failure after compaction ran but before the output is reported
/// published must leave the fully-expired input (and the live one) intact.
// Depends on the debug-build abort seam; a release build has no seam.
#[cfg(debug_assertions)]
#[test]
fn abort_before_reclaim_keeps_fully_expired_input() {
    let (temp, input_dir, expired, live, schema) = stage();
    let before_expired = components(&expired);
    let before_live = components(&live);
    assert!(before_expired.len() > 3, "real multi-component generation");
    let out = temp.path().join("out");

    let o = compact_bin(&input_dir, &out, &schema, true);
    assert!(!o.status.success(), "the seam must kill the process");
    assert_eq!(
        out_published(&out).len(),
        1,
        "the output is durably written (Data.db + TOC.txt)"
    );
    assert_eq!(
        components(&expired),
        before_expired,
        "fully-expired input survives a crash before reclaim"
    );
    assert_eq!(components(&live), before_live, "live input survives");
}

#[test]
fn publish_success_removes_dropped_input() {
    let (temp, input_dir, expired, live, schema) = stage();
    let out = temp.path().join("out");
    let o = compact_bin(&input_dir, &out, &schema, false);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(out_published(&out).len(), 1);
    assert!(
        components(&expired).is_empty(),
        "dropped input fully reclaimed after publish"
    );
    assert!(live.exists());
}
