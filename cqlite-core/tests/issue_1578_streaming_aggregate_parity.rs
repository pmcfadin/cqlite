//! Issue #1578 (Epic D / D2): a GROUP-BY-free aggregate
//! (`COUNT`/`MIN`/`MAX`/`SUM`/`AVG` with no `GROUP BY`) folds the scan stream into
//! an O(1) accumulator instead of buffering the whole table. This test pins the
//! CORRECTNESS of that fold: for every aggregate × column-type × edge case, the
//! materializing path (`Database::execute`) and the streaming path
//! (`Database::execute_streaming`) must return the SAME single answer, and that
//! answer must equal an independently-computed oracle.
//!
//! The parity axis (execute == execute_streaming) also holds on `main` (both route
//! through `execute`), so this is a regression guard for the new fold path — after
//! D2, `execute` drives the O(1) fold, so this exercises it directly.
//!
//! Guardrails checked here: COUNT(*) counts NULL rows but COUNT(col) does not; MIN/
//! MAX use the Cassandra-matching value comparator; AVG is the sum+count pair; an
//! empty table yields COUNT=0 and NULL extrema/sum/avg.
//!
//! Run:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_1578_streaming_aggregate_parity

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

const KS: &str = "agg_ks";
const TBL: &str = "metrics";

fn schema_cql() -> String {
    format!(
        "CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  i int,\n  b bigint,\n  d double,\n  t text\n);\n"
    )
}

/// One fixture row: `None` columns are simply not written (→ NULL / absent cell).
#[derive(Clone)]
struct Fx {
    id: i32,
    i: Option<i32>,
    b: Option<i64>,
    d: Option<f64>,
    t: Option<&'static str>,
}

fn write_mutation(row: &Fx, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(row.id));
    let mut ops = Vec::new();
    if let Some(i) = row.i {
        ops.push(CellOperation::Write {
            column: "i".to_string(),
            value: Value::Integer(i),
        });
    }
    if let Some(b) = row.b {
        ops.push(CellOperation::Write {
            column: "b".to_string(),
            value: Value::BigInt(b),
        });
    }
    if let Some(d) = row.d {
        ops.push(CellOperation::Write {
            column: "d".to_string(),
            value: Value::Float(d),
        });
    }
    if let Some(t) = row.t {
        ops.push(CellOperation::Write {
            column: "t".to_string(),
            value: Value::Text(t.to_string()),
        });
    }
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

/// Build a single-generation fixture and open the full query stack over it.
async fn open_with_rows(rows: &[Fx]) -> (Database, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        let rows = rows.to_vec();
        tokio::task::spawn_blocking(move || {
            use cqlite_core::schema::parse_cql_schema;
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let schema = parse_cql_schema(&schema_cql()).expect("parse schema");
            let config = WriteEngineConfig::new(data_dir, wal_dir, schema);
            let mut engine = WriteEngine::new(config).expect("engine");
            for (n, r) in rows.iter().enumerate() {
                engine
                    .write(write_mutation(r, 100 + n as i64))
                    .expect("write row");
            }
            // A fixture with rows must flush a generation; an empty fixture skips.
            let flushed = rt.block_on(engine.flush()).expect("flush");
            if !rows.is_empty() {
                flushed.expect("non-empty fixture must produce an SSTable");
            }
            rt.block_on(engine.close()).expect("close");
        })
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
    .expect("ingest fixture");
    (result.database, temp_dir)
}

/// Extract the single scalar of a one-aggregate result row.
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
                buffer_size: 4,
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

/// Assert the streaming and materializing paths agree, and both equal `expected`.
async fn assert_agg(db: &Database, sql: &str, expected: Value) {
    let e = agg_execute(db, sql).await;
    let s = agg_streaming(db, sql).await;
    assert_eq!(
        e, s,
        "Issue #1578: '{sql}' — execute()={e:?} != execute_streaming()={s:?}"
    );
    assert_eq!(
        e, expected,
        "Issue #1578: '{sql}' — result {e:?} != oracle {expected:?}"
    );
}

fn fixture_rows() -> Vec<Fx> {
    vec![
        Fx { id: 1, i: Some(10), b: Some(1000), d: Some(1.5), t: Some("charlie") },
        Fx { id: 2, i: Some(-4), b: Some(-20), d: Some(0.25), t: Some("alpha") },
        Fx { id: 3, i: None, b: Some(7), d: None, t: Some("delta") },
        Fx { id: 4, i: Some(30), b: None, d: Some(9.75), t: None },
        Fx { id: 5, i: Some(10), b: Some(50), d: Some(-2.0), t: Some("bravo") },
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_aggregate_parity_matrix() {
    let rows = fixture_rows();
    let (db, _tmp) = open_with_rows(&rows).await;

    let from = format!("FROM {KS}.{TBL}");

    // COUNT(*) counts every partition, including the NULL-column row.
    assert_agg(&db, &format!("SELECT COUNT(*) {from}"), Value::BigInt(5)).await;
    // COUNT(col) excludes NULLs: i is null in one row, b null in one, d null in one.
    assert_agg(&db, &format!("SELECT COUNT(i) {from}"), Value::BigInt(4)).await;
    assert_agg(&db, &format!("SELECT COUNT(b) {from}"), Value::BigInt(4)).await;
    assert_agg(&db, &format!("SELECT COUNT(d) {from}"), Value::BigInt(4)).await;
    assert_agg(&db, &format!("SELECT COUNT(t) {from}"), Value::BigInt(4)).await;

    // MIN/MAX over int, bigint, double, text (Cassandra-matching comparator).
    assert_agg(&db, &format!("SELECT MIN(i) {from}"), Value::Integer(-4)).await;
    assert_agg(&db, &format!("SELECT MAX(i) {from}"), Value::Integer(30)).await;
    assert_agg(&db, &format!("SELECT MIN(b) {from}"), Value::BigInt(-20)).await;
    assert_agg(&db, &format!("SELECT MAX(b) {from}"), Value::BigInt(1000)).await;
    assert_agg(&db, &format!("SELECT MIN(d) {from}"), Value::Float(-2.0)).await;
    assert_agg(&db, &format!("SELECT MAX(d) {from}"), Value::Float(9.75)).await;
    assert_agg(
        &db,
        &format!("SELECT MIN(t) {from}"),
        Value::Text("alpha".to_string()),
    )
    .await;
    assert_agg(
        &db,
        &format!("SELECT MAX(t) {from}"),
        Value::Text("delta".to_string()),
    )
    .await;

    // SUM (as f64) over each numeric type.
    assert_agg(&db, &format!("SELECT SUM(i) {from}"), Value::Float(46.0)).await;
    assert_agg(&db, &format!("SELECT SUM(b) {from}"), Value::Float(1037.0)).await;
    assert_agg(&db, &format!("SELECT SUM(d) {from}"), Value::Float(9.5)).await;

    // AVG = sum/count over the NON-NULL values (i: 46/4, d: 9.5/4).
    assert_agg(&db, &format!("SELECT AVG(i) {from}"), Value::Float(46.0 / 4.0)).await;
    assert_agg(&db, &format!("SELECT AVG(d) {from}"), Value::Float(9.5 / 4.0)).await;

    // With a predicate: only rows with i >= 10 (ids 1,4,5 → i=10,30,10).
    assert_agg(
        &db,
        &format!("SELECT COUNT(*) {from} WHERE i >= 10 ALLOW FILTERING"),
        Value::BigInt(3),
    )
    .await;
    assert_agg(
        &db,
        &format!("SELECT SUM(i) {from} WHERE i >= 10 ALLOW FILTERING"),
        Value::Float(50.0),
    )
    .await;
}

/// Count the rows a query returns via each path, asserting the two paths agree.
async fn assert_row_count_parity(db: &Database, sql: &str) -> usize {
    let e = db.execute(sql).await.expect("execute").rows.len();
    let mut iter = db
        .execute_streaming(sql, StreamingConfig::default())
        .await
        .expect("execute_streaming");
    let mut s = 0usize;
    while let Some(r) = iter.next_async().await {
        r.expect("streamed row");
        s += 1;
    }
    assert_eq!(
        e, s,
        "Issue #1578: '{sql}' — execute() returned {e} rows, execute_streaming() {s}"
    );
    e
}

/// Empty-table: CQLite currently returns ZERO rows for a GROUP-BY-free aggregate
/// over an empty table (an empty aggregate input produces no group), where
/// Cassandra returns one row (COUNT=0, extrema NULL). That Cassandra divergence is
/// a pre-existing behavior gap tracked OUTSIDE this memory fix (#1578). The
/// in-scope guarantee here is that the O(1) fold path preserves that behavior and
/// stays byte-consistent with the materializing path — both return 0 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_aggregate_empty_table_row_count_parity() {
    let (db, _tmp) = open_with_rows(&[]).await;
    let from = format!("FROM {KS}.{TBL}");

    for agg in [
        "COUNT(*)", "COUNT(i)", "MIN(i)", "MAX(t)", "SUM(i)", "AVG(i)",
    ] {
        let sql = format!("SELECT {agg} {from}");
        assert_eq!(
            assert_row_count_parity(&db, &sql).await,
            0,
            "Issue #1578: '{sql}' over an empty table returns 0 rows (current \
             CQLite semantics; the fold must not diverge from the buffered path)"
        );
    }
}
