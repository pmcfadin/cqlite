//! Issue #1579 (Epic D / D3): streaming the multi-generation merge must emit
//! rows in the SAME order the pre-D3 collect+sort path emitted — Cassandra token
//! order `(murmur3_token(key), key)`.
//!
//! ## What D3 changes
//!
//! Pre-D3, `SSTableManager::scan_stream` over >1 generation collected the ENTIRE
//! reconciled result from the `KWayMerger`, ran `sort_by_token_order`, and only
//! then dribbled it down the channel. D3 feeds each stepped partition from the
//! merger STRAIGHT into the channel. The merger already yields partitions in
//! `DecoratedKey` order = `(token, key)`, which is byte-identical to what
//! `sort_by_token_order` produced — so removing the collect+sort must NOT change
//! the emitted order.
//!
//! ## What this test asserts
//!
//! Over a genuinely multi-generation fixture (overwrite AND row-tombstone across
//! generations, several partitions whose token order differs from their numeric
//! key order), the streamed rows come out in NON-DECREASING `(token, key)` order
//! — the exact order `sort_by_token_order` yields. This is the byte-identical
//! ordering guardrail: if the merger's step order ever diverged from token order,
//! this pins it red. It also cross-checks that the streamed SET+VALUES match the
//! materializing `execute` path (the reconciliation oracle), so "same order" is
//! not vacuously satisfied by a wrong/empty result.
//!
//! Excluded under `tombstones` (that build's `scan_stream` delegates to the
//! materializing `scan`, so the streaming order is a non-issue there).
//!
//! Run with:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_1579_streaming_multigen_order

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
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_core::{Config, Database, RowKey};
use tempfile::TempDir;

const KS: &str = "order_ks";
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

/// Build a two-generation fixture: gen1 then gen2 flushed with no compaction, so
/// two Data.db files remain and the read path must reconcile across generations.
fn build_two_generation_fixture(
    data_dir: &std::path::Path,
    wal_dir: &std::path::Path,
    gen1: &[Mutation],
    gen2: &[Mutation],
) {
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

    for m in gen2 {
        engine.write(m.clone()).expect("write gen2 row");
    }
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

/// The ordered comparison key: `(token, raw key bytes)` — exactly what
/// `sort_by_token_order` orders by.
fn order_key(key: &RowKey) -> (i64, Vec<u8>) {
    (
        cassandra_murmur3_token(key.as_bytes()),
        key.as_bytes().to_vec(),
    )
}

/// Collect streamed rows IN EMITTED ORDER (no re-sort) so the raw stream order is
/// observable.
async fn collect_streaming_ordered(
    db: &Database,
    sql: &str,
    buffer_size: usize,
) -> Vec<(RowKey, HashMap<Arc<str>, Value>)> {
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
        rows.push((row.key, row.values));
    }
    rows
}

async fn collect_execute_sorted(
    db: &Database,
    sql: &str,
) -> Vec<(Vec<u8>, HashMap<Arc<str>, Value>)> {
    let result = db.execute(sql).await.expect("execute should succeed");
    let mut rows: Vec<(Vec<u8>, HashMap<Arc<str>, Value>)> = result
        .rows
        .into_iter()
        .map(|r| (r.key.as_bytes().to_vec(), r.values))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// The gen1/gen2 mutation split shared by the assertions below. Several
/// partitions (ids 1..=8) whose murmur3 token order differs from numeric order,
/// with a cross-generation OVERWRITE (id=3) and a cross-generation row-TOMBSTONE
/// (id=6) so reconciliation is genuinely exercised, not just concatenation.
fn gen1() -> Vec<Mutation> {
    (1..=8)
        .map(|id| write_row(id, &format!("n{id}-v1"), id * 10, 100))
        .collect()
}

fn gen2() -> Vec<Mutation> {
    vec![
        write_row(3, "n3-v2", 999, 200), // overwrite id=3 (newer ts wins)
        delete_row(6, 300),              // row-tombstone id=6 (suppressed)
        write_row(9, "n9-v2", 90, 200),  // brand-new partition id=9
    ]
}

async fn open_fixture_db() -> (Database, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, make_schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || {
            build_two_generation_fixture(&data_dir, &wal_dir, &gen1(), &gen2())
        })
        .await
        .expect("fixture build task");
    }

    let db = open_db(data_dir, schema_path).await;
    (db, temp_dir)
}

/// The streamed multi-generation rows come out in non-decreasing `(token, key)`
/// order — byte-identical to the pre-D3 `sort_by_token_order` emission order —
/// across buffer sizes (including `buffer_size = 1`, per-row backpressure).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_multigen_rows_are_in_token_order() {
    let (db, temp_dir) = open_fixture_db().await;
    let sql = format!("SELECT * FROM {KS}.{TBL}");

    for buffer_size in [1usize, 4, 1024] {
        let streamed = collect_streaming_ordered(&db, &sql, buffer_size).await;

        // Reconciliation sanity: id=3 overwritten (one row), id=6 tombstoned
        // (absent), id=9 added ⇒ 8 live partitions (1,2,3,4,5,7,8,9).
        assert_eq!(
            streamed.len(),
            8,
            "Issue #1579: expected 8 reconciled rows (buffer_size={buffer_size}), got {}",
            streamed.len()
        );

        // The core guardrail: emitted order is non-decreasing (token, key).
        for pair in streamed.windows(2) {
            let a = order_key(&pair[0].0);
            let b = order_key(&pair[1].0);
            assert!(
                a <= b,
                "Issue #1579: streamed rows out of token order (buffer_size={buffer_size}): \
                 {a:?} then {b:?} — the streaming merge must emit in the same \
                 (token, key) order the pre-D3 collect+sort path produced"
            );
        }
    }

    drop(temp_dir);
}

/// The streamed SET+VALUES equal the materializing `execute` path — the
/// reconciliation oracle — so the token-order assertion above is not vacuously
/// satisfied by a wrong or empty result. (Order-independent: both sides sorted by
/// raw key before comparison; the raw stream ORDER is pinned by the test above.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_multigen_rows_match_materializing_execute() {
    let (db, temp_dir) = open_fixture_db().await;
    let sql = format!("SELECT * FROM {KS}.{TBL}");

    let expected = collect_execute_sorted(&db, &sql).await;

    for buffer_size in [1usize, 4, 1024] {
        let mut streamed: Vec<(Vec<u8>, HashMap<Arc<str>, Value>)> =
            collect_streaming_ordered(&db, &sql, buffer_size)
                .await
                .into_iter()
                .map(|(k, v)| (k.as_bytes().to_vec(), v))
                .collect();
        streamed.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            streamed.len(),
            expected.len(),
            "Issue #1579: streamed row count (buffer_size={buffer_size}) diverged from execute"
        );
        for (i, (got, want)) in streamed.iter().zip(expected.iter()).enumerate() {
            assert_eq!(got.0, want.0, "Issue #1579: row {i} key mismatch");
            assert_eq!(got.1, want.1, "Issue #1579: row {i} value mismatch");
        }
    }

    drop(temp_dir);
}
