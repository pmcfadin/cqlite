//! Issue #957 (Epic #951): the streaming read path
//! (`Database::execute_streaming`) must reconcile multiple SSTable generations
//! identically to the materializing path (`Database::execute`).
//!
//! ## The bug this guards
//!
//! In the default (`not(tombstones)`) build with `write-support`,
//! `SSTableManager::scan` reconciles multiple generations via
//! `merge_generations_for_read` — the same last-write-wins + tombstone-shadowing
//! k-way merge compaction uses (Issue #883). `SSTableManager::scan_stream`, the
//! streaming analog, originally did a *pure key-ordered k-way merge* over
//! per-reader heads with NO LWW collapse and NO tombstone shadowing. For a
//! partition that lives in more than one generation that diverges from `scan`:
//!
//!   - An overwrite in a newer generation made the row appear TWICE in the
//!     stream (duplicate) while `scan` returned one merged row.
//!   - A row tombstone (`CellOperation::DeleteRow`) in a newer generation made
//!     the deleted row REAPPEAR in the stream (resurrected) while `scan`
//!     suppressed it.
//!
//! ## What this test asserts
//!
//! Build a genuinely multi-generation fixture via the public `WriteEngine` API
//! (one `flush()` per generation, no compaction — same pattern as #883 / #958),
//! then assert `execute()` and `execute_streaming()` return the **same row set**
//! for `SELECT *`. Two independent cases each trip the pre-fix bug:
//!
//!   1. **Overwrite**: gen1 writes id=1, gen2 overwrites id=1 with a newer ts.
//!      Pre-fix the stream emitted id=1 twice.
//!   2. **Row tombstone**: gen1 writes id=1, gen2 deletes id=1 with a higher ts.
//!      Pre-fix the stream resurrected id=1.
//!
//! The single-generation case is already covered by
//! `test_issue_790_streaming_parity.rs`; that test uses single-generation
//! datasets and therefore CANNOT catch this — the bug only manifests across
//! generations, which is why this fixture builds several.
//!
//! NOTE: excluded under `tombstones`. The `tombstones`-variant `scan_stream`
//! already delegates wholesale to the materializing `scan`, so the bug is masked
//! there (same gating lesson as #958). The default build is the one under test.
//!
//! Run with:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_957_streaming_materializing_parity

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use std::collections::HashMap;
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database, RowKey};
use tempfile::TempDir;

const KS: &str = "stream_ks";
const TBL: &str = "items";

fn make_schema_cql() -> String {
    format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  name text,\n  score int\n);\n")
}

fn write_row(id: i32, name: &str, score: i32, ts: i64) -> Mutation {
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

/// Build a two-generation fixture under `data_dir`. `gen2` is applied as the
/// second flush, on top of gen1, with no compaction in between, leaving two
/// Data.db files on disk.
fn build_two_generation_fixture<F>(
    data_dir: &std::path::Path,
    wal_dir: &std::path::Path,
    gen1: &[Mutation],
    gen2: F,
) where
    F: FnOnce(&mut WriteEngine),
{
    use cqlite_core::schema::parse_cql_schema;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let schema = parse_cql_schema(&make_schema_cql()).expect("parse fixture schema");
    let config = WriteEngineConfig::new(data_dir.to_path_buf(), wal_dir.to_path_buf(), schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");

    for m in gen1 {
        engine.write(m.clone()).expect("write gen1 row");
    }
    rt.block_on(engine.flush())
        .expect("flush gen1")
        .expect("gen1 produced no SSTable");

    gen2(&mut engine);
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

/// A comparable, order-independent snapshot of a result row keyed by raw key
/// bytes. We compare as a *set* keyed on row key bytes so a duplicate row
/// (pre-fix overwrite symptom) collapses in the materializing side but NOT the
/// streaming side, exposing the divergence.
type RowSnapshot = (Vec<u8>, HashMap<String, Value>);

fn snapshot_key(key: &RowKey) -> Vec<u8> {
    key.as_bytes().to_vec()
}

async fn collect_execute(db: &Database, sql: &str) -> Vec<RowSnapshot> {
    let result = db.execute(sql).await.expect("execute should succeed");
    let mut rows: Vec<RowSnapshot> = result
        .rows
        .into_iter()
        .map(|r| (snapshot_key(&r.key), r.values))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

async fn collect_streaming(db: &Database, sql: &str, buffer_size: usize) -> Vec<RowSnapshot> {
    let config = StreamingConfig {
        buffer_size,
        ..StreamingConfig::default()
    };
    let mut iter = db
        .execute_streaming(sql, config)
        .await
        .expect("execute_streaming should succeed");

    let mut rows = Vec::new();
    while let Some(row) = iter.next_async().await {
        let row = row.expect("streamed row should be Ok");
        rows.push((snapshot_key(&row.key), row.values));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// Open the full query stack over the multi-generation directory.
async fn open_db(data_dir: std::path::PathBuf, schema_path: std::path::PathBuf) -> Database {
    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest multi-generation fixture");
    assert!(
        result.schema_load_result.schemas_loaded >= 1,
        "schema must load"
    );
    result.database
}

/// Core assertion: streaming and materializing paths agree, across several
/// buffer sizes (including buffer_size=1, which forces per-row backpressure).
async fn assert_stream_matches_execute(db: &Database, sql: &str) {
    let expected = collect_execute(db, sql).await;

    for buffer_size in [1usize, 4, 1024] {
        let streamed = collect_streaming(db, sql, buffer_size).await;

        assert_eq!(
            streamed.len(),
            expected.len(),
            "Issue #957: streaming '{sql}' (buffer_size={buffer_size}) returned {} rows, \
             materializing execute returned {} — a multi-generation divergence \
             (duplicate or resurrected row).",
            streamed.len(),
            expected.len()
        );

        for (i, (got, want)) in streamed.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                got.0, want.0,
                "Issue #957: row {i} key mismatch (buffer_size={buffer_size})"
            );
            assert_eq!(
                got.1, want.1,
                "Issue #957: row {i} value mismatch (buffer_size={buffer_size})"
            );
        }
    }
}

/// Overwrite case: gen2 overwrites a partition written in gen1 with a newer
/// timestamp. Pre-fix `execute_streaming` emitted the partition TWICE; `execute`
/// returned the single LWW-merged row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_matches_execute_on_cross_generation_overwrite() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, make_schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || {
            build_two_generation_fixture(
                &data_dir,
                &wal_dir,
                // gen1: two partitions.
                &[
                    write_row(1, "n1-v1", 10, 100),
                    write_row(2, "n2-v1", 20, 100),
                ],
                // gen2: OVERWRITE id=1 with a newer ts; id=3 is brand new.
                |engine| {
                    engine
                        .write(write_row(1, "n1-v2", 999, 200))
                        .expect("write gen2 overwrite");
                    engine
                        .write(write_row(3, "n3-v2", 30, 200))
                        .expect("write gen2 new");
                },
            )
        })
        .await
        .expect("fixture build task");
    }

    let db = open_db(data_dir, schema_path).await;
    let db = Arc::new(db);

    let sql = format!("SELECT * FROM {KS}.{TBL}");

    // Sanity: the materializing path already reconciles to exactly 3 live rows
    // (id=1 merged, id=2, id=3). If this is wrong the divergence test below is
    // meaningless.
    let expected = collect_execute(&db, &sql).await;
    assert_eq!(
        expected.len(),
        3,
        "materializing execute must reconcile the overwrite to 3 distinct rows"
    );

    assert_stream_matches_execute(&db, &sql).await;

    drop(temp_dir);
}

/// Row-tombstone case: gen2 deletes a partition written in gen1 with a higher
/// timestamp. Pre-fix `execute_streaming` RESURRECTED the deleted row from gen1;
/// `execute` suppressed it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_matches_execute_on_cross_generation_row_tombstone() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, make_schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || {
            build_two_generation_fixture(
                &data_dir,
                &wal_dir,
                // gen1: two partitions.
                &[
                    write_row(1, "n1-v1", 10, 100),
                    write_row(2, "n2-v1", 20, 100),
                ],
                // gen2: row-DELETE id=1 (higher ts); id=3 is brand new.
                |engine| {
                    engine
                        .write(delete_row(1, 300))
                        .expect("write gen2 row tombstone");
                    engine
                        .write(write_row(3, "n3-v2", 30, 200))
                        .expect("write gen2 new");
                },
            )
        })
        .await
        .expect("fixture build task");
    }

    let db = open_db(data_dir, schema_path).await;
    let db = Arc::new(db);

    let sql = format!("SELECT * FROM {KS}.{TBL}");

    // Sanity: the materializing path suppresses id=1, leaving exactly 2 rows
    // (id=2, id=3).
    let expected = collect_execute(&db, &sql).await;
    assert_eq!(
        expected.len(),
        2,
        "materializing execute must suppress the tombstoned row, leaving 2 rows"
    );

    assert_stream_matches_execute(&db, &sql).await;

    drop(temp_dir);
}
