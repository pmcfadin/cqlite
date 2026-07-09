//! Issue #1578 (Epic D / D2), roborev Finding 1: the O(1) streaming aggregate
//! fold must reconcile MULTIPLE SSTable generations identically to the
//! materializing path — every other #1578 fixture flushes exactly ONCE, so
//! nothing pins the fold's cross-generation reconciliation (`execute()`'s fold
//! calls `storage.scan_stream`, which for >1 reader + a schema delegates to
//! `merge_generations_for_read` — issue #957's LWW + tombstone-shadowing
//! lockstep merge — rather than a naive per-reader concatenation that would
//! double-count an overwritten row or resurrect a tombstoned one).
//!
//! ## Fixture (write + flush, then overwrite + delete + flush again, no compaction)
//!
//! Gen1 (ts=100): id=1 (v=10,  name="alpha"),  id=2 (v=20, name="bravo"),
//!                id=3 (v=5,   name="charlie")
//! Gen2 (ts=200): id=1 OVERWRITTEN (v=999, name="alpha2"),
//!                id=2 ROW-DELETED,
//!                id=4 NEW (v=40, name="delta")
//!
//! Reconciled live rows (computed BY HAND from the fixture, not from the code
//! under test): id=1 (v=999, "alpha2"), id=3 (v=5, "charlie", unchanged from
//! gen1), id=4 (v=40, "delta"). id=2 is gone (row tombstone shadows gen1).
//!
//!   COUNT(*)  = 3
//!   COUNT(v)  = 3      (no NULLs among the 3 live rows)
//!   SUM(v)    = 999 + 5 + 40 = 1044
//!   MIN(v)    = 5
//!   MAX(v)    = 999
//!   AVG(v)    = 1044 / 3 = 348   (v is int → integer division, issue #2202)
//!   MIN(name) = "alpha2"   ('a' < 'c' < 'd': alpha2 < charlie < delta)
//!   MAX(name) = "delta"
//!
//! Both `execute()` (the O(1) fold) and `execute_streaming()` (routed through
//! `execute_and_stream` -> `execute()` for any aggregate) must agree with each
//! other AND with these hand-computed values.
//!
//! Run:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_1578_streaming_aggregate_multigen_parity

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use tempfile::TempDir;

const KS: &str = "agg_multigen_ks";
const TBL: &str = "metrics";

fn schema_cql() -> String {
    format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  v int,\n  name text\n);\n")
}

fn write_row(id: i32, v: i32, name: &str, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "v".to_string(),
            value: Value::Integer(v),
        },
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
    ];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn delete_row(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    Mutation::new(
        TableId::new(KS, TBL),
        pk,
        None,
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name();
            let n = n.to_string_lossy();
            n.ends_with("-big-Data.db") || n.ends_with("-Data.db")
        })
        .count()
}

/// Build the 2-generation fixture described in the module doc under `data_dir`,
/// with NO compaction between flushes — leaving two on-disk Data.db files.
fn build_fixture(data_dir: &std::path::Path, wal_dir: &std::path::Path) {
    use cqlite_core::schema::parse_cql_schema;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let schema = parse_cql_schema(&schema_cql()).expect("parse fixture schema");
    let config = WriteEngineConfig::new(data_dir.to_path_buf(), wal_dir.to_path_buf(), schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // Gen1: three rows.
    for m in [
        write_row(1, 10, "alpha", 100),
        write_row(2, 20, "bravo", 100),
        write_row(3, 5, "charlie", 100),
    ] {
        engine.write(m).expect("write gen1 row");
    }
    rt.block_on(engine.flush())
        .expect("flush gen1")
        .expect("gen1 produced no SSTable");

    // Gen2: overwrite id=1, row-delete id=2, add id=4.
    engine
        .write(write_row(1, 999, "alpha2", 200))
        .expect("write gen2 overwrite");
    engine.write(delete_row(2, 200)).expect("write gen2 delete");
    engine
        .write(write_row(4, 40, "delta", 200))
        .expect("write gen2 new");
    rt.block_on(engine.flush())
        .expect("flush gen2")
        .expect("gen2 produced no SSTable");

    rt.block_on(engine.close()).expect("close engine");

    let sstable_dir = data_dir.join(KS).join(TBL);
    assert_eq!(
        count_data_files(&sstable_dir),
        2,
        "fixture must produce exactly 2 generations (no compaction)"
    );
}

async fn open_multigen_db() -> (Database, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || build_fixture(&data_dir, &wal_dir))
            .await
            .expect("fixture build task");
    }

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest multi-generation fixture");
    (result.database, temp_dir)
}

fn single_value(rows: &[cqlite_core::query::result::QueryRow]) -> Value {
    assert_eq!(rows.len(), 1, "a global aggregate yields exactly one row");
    let mut vals = rows[0].values.values();
    let v = vals.next().cloned().expect("aggregate row has one value");
    assert!(
        vals.next().is_none(),
        "a single-aggregate SELECT yields exactly one value in the row"
    );
    v
}

async fn agg_execute(db: &Database, sql: &str) -> Value {
    let result = db.execute(sql).await.expect("execute aggregate");
    single_value(&result.rows)
}

async fn agg_streaming(db: &Database, sql: &str) -> Value {
    let mut iter = db
        .execute_streaming(
            sql,
            StreamingConfig {
                buffer_size: 1,
                ..StreamingConfig::default()
            },
        )
        .await
        .expect("execute_streaming aggregate");
    let mut rows = Vec::new();
    while let Some(r) = iter.next_async().await {
        rows.push(r.expect("streamed aggregate row"));
    }
    single_value(&rows)
}

/// Assert execute()/execute_streaming() agree across generations, and both
/// equal the by-hand oracle from the module doc.
async fn assert_agg(db: &Database, sql: &str, expected: Value) {
    let e = agg_execute(db, sql).await;
    let s = agg_streaming(db, sql).await;
    assert_eq!(
        e, s,
        "Issue #1578 Finding 1: '{sql}' — execute()={e:?} != execute_streaming()={s:?} \
         across a multi-generation fixture"
    );
    assert_eq!(
        e, expected,
        "Issue #1578 Finding 1: '{sql}' — result {e:?} != by-hand oracle {expected:?}"
    );
}

/// Sanity: confirm the fixture reconciles to exactly the 3 live rows the
/// module-doc oracle assumes. If this fails, the oracle values are meaningless.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multigen_fixture_reconciles_to_three_live_rows() {
    let (db, _tmp) = open_multigen_db().await;
    let result = db
        .execute(&format!("SELECT * FROM {KS}.{TBL}"))
        .await
        .expect("execute SELECT *");
    assert_eq!(
        result.rows.len(),
        3,
        "multi-gen fixture must reconcile to 3 live rows (id=1 overwritten, \
         id=2 deleted, id=3 unchanged, id=4 new)"
    );
}

/// Core assertion: the O(1) streaming fold reconciles cross-generation exactly
/// like the buffered/materializing path, matching the by-hand oracle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_aggregate_multigen_parity() {
    let (db, _tmp) = open_multigen_db().await;
    let from = format!("FROM {KS}.{TBL}");

    assert_agg(&db, &format!("SELECT COUNT(*) {from}"), Value::BigInt(3)).await;
    assert_agg(&db, &format!("SELECT COUNT(v) {from}"), Value::BigInt(3)).await;
    // Issue #2202: v is int → SUM(int) → int, AVG(int) → int (integer division).
    assert_agg(&db, &format!("SELECT SUM(v) {from}"), Value::Integer(1044)).await;
    assert_agg(&db, &format!("SELECT MIN(v) {from}"), Value::Integer(5)).await;
    assert_agg(&db, &format!("SELECT MAX(v) {from}"), Value::Integer(999)).await;
    assert_agg(&db, &format!("SELECT AVG(v) {from}"), Value::Integer(348)).await;
    assert_agg(
        &db,
        &format!("SELECT MIN(name) {from}"),
        Value::Text("alpha2".to_string()),
    )
    .await;
    assert_agg(
        &db,
        &format!("SELECT MAX(name) {from}"),
        Value::Text("delta".to_string()),
    )
    .await;
}
