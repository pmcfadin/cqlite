//! Issue #4612 (roborev round 1): compaction must refuse a damaged input `Statistics.db`
//! BEFORE any lenient metadata read can classify it (fully-expired drop-set) or bound purging
//! (`compute_max_purgeable_timestamp`), so a damaged file can never cause a delete, a publish,
//! or extra tombstone purging.
//!
//! The damage used is a flip of the final byte (the trailing component CRC): the old lenient
//! parser still decodes the file (asserted below), so it would have read as fully expired.

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

fn damage_last_byte(data_db: &Path) -> PathBuf {
    let base = data_db
        .to_string_lossy()
        .trim_end_matches("Data.db")
        .to_string();
    let stats = PathBuf::from(format!("{base}Statistics.db"));
    let mut bytes = std::fs::read(&stats).expect("read Statistics.db");
    // The defect is only real if the OLD lenient parser still accepts the damaged file.
    let n = bytes.len();
    bytes[n - 1] ^= 0x01;
    assert!(
        cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &bytes, None
        )
        .is_ok(),
        "the lenient parser must still accept this damage, or the test shows nothing"
    );
    std::fs::write(&stats, &bytes).expect("write damaged Statistics.db");
    stats
}

fn args(input_dir: &Path, out: &Path, schema: PathBuf) -> CompactArgs {
    CompactArgs {
        input_dir: input_dir.to_path_buf(),
        output: out.to_path_buf(),
        schema,
        gc_before: Some(GC_BEFORE),
        now_sec: Some(NOW_SECS),
        generation: 1,
        major: true,
    }
}

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

#[test]
fn damaged_fully_expired_candidate_stops_compaction_and_nothing_is_deleted_or_published() {
    // Control: the intact candidate IS dropped, so the damaged case below is the dangerous one.
    {
        let (temp, input_dir, expired, _live, schema_path) = stage();
        let out = temp.path().join("out");
        let r = rt()
            .block_on(handle_compact(&args(&input_dir, &out, schema_path)))
            .expect("intact");
        assert_eq!(r.dropped_whole, vec![expired]);
    }
    let (temp, input_dir, expired, live, schema_path) = stage();
    let stats = damage_last_byte(&expired);
    let out = temp.path().join("out");
    let err = rt()
        .block_on(handle_compact(&args(&input_dir, &out, schema_path)))
        .err()
        .expect("a damaged Statistics.db must stop compaction");
    let err = format!("{err:#}");
    assert!(
        err.contains(&stats.display().to_string()),
        "must name the path: {err}"
    );
    assert!(
        expired.exists() && stats.exists(),
        "the damaged candidate must not be deleted"
    );
    assert!(live.exists(), "no input may be deleted");
    let published: Vec<PathBuf> = discover(&out);
    assert!(
        published.is_empty(),
        "nothing may be published: {published:?}"
    );
}

#[test]
fn damaged_overlapping_sstable_never_allows_more_purging_than_an_intact_one() {
    use cqlite_core::storage::write_engine::merge::{
        compute_max_purgeable_timestamp, fully_expired_sstables,
    };
    let (_temp, _dir, expired, live, _schema) = stage();
    // Intact overlapping SSTable: the bound is its min write timestamp and the candidate
    // (max ts 100) is droppable against it.
    let bound = compute_max_purgeable_timestamp(std::slice::from_ref(&live));
    assert_eq!(bound, Some(5_000_000), "control: the intact bound");
    let intact = fully_expired_sstables(
        std::slice::from_ref(&expired),
        std::slice::from_ref(&live),
        Some(GC_BEFORE),
    );
    assert_eq!(
        intact,
        vec![expired.clone()],
        "control: droppable against the intact bound"
    );

    damage_last_byte(&live);
    assert_eq!(
        compute_max_purgeable_timestamp(std::slice::from_ref(&live)),
        None,
        "an unreadable overlapping SSTable disables overlap-aware purging"
    );
    let damaged = fully_expired_sstables(
        std::slice::from_ref(&expired),
        std::slice::from_ref(&live),
        Some(GC_BEFORE),
    );
    assert!(
        damaged.is_empty(),
        "nothing may be dropped against an unknown bound: {damaged:?}"
    );
}
