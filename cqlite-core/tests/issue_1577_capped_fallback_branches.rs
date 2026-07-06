//! Issue #1577 (Epic D / D1): end-to-end coverage for the LIMIT-pushdown
//! `capped_fallback_scan` branches that the single-reader `simple_table` fixture
//! never exercises — the MULTI-READER / MULTI-GENERATION merge-under-cap path and
//! the SHORT-STREAM reconciliation path.
//!
//! A `SELECT ... LIMIT n` with no partition-key restriction routes through
//! `FallbackFullScan` → `capped_fallback_scan`, whose `scan_stream` (with
//! `write-support` + a resolved schema + more than one generation) reconciles
//! across generations via `merge_generations_for_read` — the exact multi-reader
//! merge the materializing `scan` uses. That merge branch is only taken when the
//! query executor resolves a schema for the table (see
//! `SSTableManager::scan`: `if reader_list.len() > 1 { if let Some(schema) = schema`).
//! A bare `Database::open` over a raw data directory registers NO schema, so
//! `schema_opt` is `None` and the cross-generation merge is skipped — the fixture
//! would be VACUOUS for its stated purpose. This test therefore opens the fixture
//! through the ingestion path (`ingestion::ingest`) with a generated CQL schema,
//! which loads and REGISTERS `test_capped.items` into the schema registry, so the
//! executor resolves a real schema and the `merge_generations_for_read` branch runs.
//!
//! To make the merge OBSERVABLE (not just present), the fixture writes an
//! OVERLAPPING "hot" partition (`id = 0`) into EVERY generation with an increasing
//! timestamp, alongside a distinct per-generation partition (`id = 1..=n_gens`).
//! The cross-generation merge reconciles the hot partition to a SINGLE
//! last-write-wins row; the non-merge concat / k-way fallback (issue #883) would
//! instead return the hot partition once PER generation. So the row count and the
//! hot row's winning value are a direct, meaningful signal that the merge branch
//! actually ran:
//!   * merge branch: `n_gens + 1` rows, hot `value == "hot{n_gens}"` (latest gen)
//!   * concat/k-way fallback: `2 * n_gens` rows (hot duplicated across generations)
//!
//! This test builds several REAL CQLite generations (the write path is byte-parity
//! with Cassandra, M5), opens them via the ingestion path, and asserts every
//! bounded result is byte-identical (same rows, same order) to the corresponding
//! prefix of the UNBOUNDED oracle:
//!
//! * `LIMIT n <= rows` drives the TRUSTED full-cap stream fast path over multiple
//!   generations (and, in debug builds, the `debug_assert_trusted_prefix`
//!   token-order invariant guard).
//! * `LIMIT n > rows` drives the SHORT-STREAM reconciliation branch end-to-end
//!   across multiple generations (the stream ends before the cap, so the method
//!   re-runs the authoritative `scan` and returns its first-`cap` accepted rows).
//!
//! Fixtures are generated in-test, so an "absent dataset" case cannot silently
//! skip: a fixture-write failure fails the test, and the oracle row count is an
//! exact assertion (never `>= 0`), so a 0-rows-on-present-data regression fails
//! loudly.
//!
//! Gated on `write-support` (to build the generations) and `cli-helpers` (for the
//! `ingestion::ingest` schema-registering open path; `cli-helpers` implies
//! `state_machine`, so `Database::execute` is available), so the file is empty in
//! builds without those features.
//!
//! Run with:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers \
//!     --test issue_1577_capped_fallback_branches

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;

const KEYSPACE: &str = "test_capped";
const TABLE: &str = "items";
/// The overlapping partition rewritten into EVERY generation. Its cross-generation
/// last-write-wins reconciliation is the observable signal that the merge branch ran.
const HOT_ID: i32 = 0;

fn items_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "value".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Write a CQL schema for `test_capped.items` that the ingestion path loads and
/// REGISTERS with the schema registry, so the query executor resolves a schema and
/// the multi-generation `merge_generations_for_read` branch is eligible to run.
/// Matches `items_schema()` (int PK `id`, text `value`).
fn write_cql_schema(root: &Path) -> std::path::PathBuf {
    let path = root.join("items.cql");
    std::fs::write(
        &path,
        format!(
            "CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = \
             {{'class': 'SimpleStrategy', 'replication_factor': 1}};\n\
             USE {KEYSPACE};\n\
             CREATE TABLE IF NOT EXISTS {TABLE} (\n\
             \x20   id int PRIMARY KEY,\n\
             \x20   value text\n\
             );\n"
        ),
    )
    .expect("write CQL schema");
    path
}

/// Write `n_gens` SSTable generations, flushing between each so no compaction
/// merges them. Each generation gets a DISTINCT partition (`id = gen`) AND rewrites
/// the shared `HOT_ID` partition with an increasing timestamp, so the hot partition
/// exists in every generation and only the cross-generation merge reconciles it to a
/// single last-write-wins row. Every generation lands in
/// `<data_dir>/<keyspace>/<table>/nb-<gen>-big-*`.
async fn build_generations(root: &Path, n_gens: i32) {
    let data_dir = root.join("data");
    let wal_dir = root.join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, items_schema());
    let mut engine = WriteEngine::new(config).expect("write engine");

    for gen in 1..=n_gens {
        // Distinct per-generation partition.
        let unique = Mutation::new(
            TableId::new(KEYSPACE, TABLE),
            PartitionKey::single("id", Value::Integer(gen)),
            None,
            vec![CellOperation::Write {
                column: "value".to_string(),
                value: Value::Text(format!("v{gen}")),
            }],
            1_000 + gen as i64,
            None,
        );
        engine.write_async(unique).await.expect("write partition");

        // Overlapping hot partition, rewritten every generation with a strictly
        // increasing timestamp so the latest generation wins under LWW.
        let hot = Mutation::new(
            TableId::new(KEYSPACE, TABLE),
            PartitionKey::single("id", Value::Integer(HOT_ID)),
            None,
            vec![CellOperation::Write {
                column: "value".to_string(),
                value: Value::Text(format!("hot{gen}")),
            }],
            2_000 + gen as i64,
            None,
        );
        engine.write_async(hot).await.expect("write hot partition");

        engine.flush().await.expect("flush generation");
    }

    let table_dir = data_dir.join(KEYSPACE).join(TABLE);
    for gen in 1..=n_gens {
        assert!(
            table_dir.join(format!("nb-{gen}-big-Data.db")).exists(),
            "generation {gen} must exist on disk (multi-generation directory required)"
        );
    }
}

/// Open the multi-generation fixture through the ingestion path so the generated
/// CQL schema is REGISTERED — without this the executor resolves no schema and the
/// `merge_generations_for_read` branch is never taken (the finding this test guards).
async fn open_with_schema(root: &Path) -> Database {
    let schema_path = write_cql_schema(root);
    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: root.join("data"),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest multi-generation fixture with registered schema");
    assert!(
        result.schema_load_result.schemas_loaded >= 1,
        "schema must be registered (schemas_loaded >= 1) or the merge branch cannot run"
    );
    result.database
}

/// The ordered `(id, value)` identity of a result, in RESULT order — the strict
/// byte-identity signal for oracle-vs-bounded prefix checks. The single-int
/// partition key is the 4-byte big-endian row key.
fn ordered_rows(rows: &[cqlite_core::query::result::QueryRow]) -> Vec<(i32, String)> {
    rows.iter()
        .map(|r| {
            let b = r.key.as_bytes();
            assert_eq!(b.len(), 4, "int partition key must be 4 bytes, got {b:?}");
            let id = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
            let value = match r.values.get("value") {
                Some(Value::Text(s)) => s.clone(),
                other => panic!("row id={id} missing text `value` column: {other:?}"),
            };
            (id, value)
        })
        .collect()
}

/// Multi-generation LIMIT pushdown must be RESULT-PRESERVING: every bounded
/// result equals the matching prefix of the unbounded oracle, both for the
/// trusted full-cap stream fast path (`LIMIT <= rows`) and the short-stream
/// reconciliation branch (`LIMIT > rows`) — across a real multi-reader merge.
#[tokio::test]
async fn multi_generation_limit_matches_unbounded_oracle_prefix() {
    const N_GENS: i32 = 6;
    // hot partition + one distinct partition per generation, AFTER reconciliation.
    const MERGED_ROWS: usize = N_GENS as usize + 1;

    let tmp = TempDir::new().unwrap();
    build_generations(tmp.path(), N_GENS).await;

    let db = open_with_schema(tmp.path()).await;

    // Oracle: the UNBOUNDED scan, reconciled across all N generations.
    let full = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await
        .expect("unbounded scan");
    let oracle = ordered_rows(&full.rows);

    // PROOF the cross-generation merge branch ran (not the concat/k-way fallback):
    // the hot partition is present in every generation. The merge reconciles it to
    // ONE last-write-wins row (=> N_GENS + 1 rows); the non-merge fallback would
    // duplicate it once per generation (=> 2 * N_GENS rows). Exact count is a hard
    // assertion (never `>=`), so a 0/low-rows read regression fails loudly.
    assert_eq!(
        oracle.len(),
        MERGED_ROWS,
        "cross-generation merge must reconcile the overlapping hot partition to a \
         single row ({MERGED_ROWS} total); {} rows means the merge branch did NOT run \
         (concat/k-way fallback duplicates the hot partition per generation)",
        oracle.len()
    );
    let hot_value = oracle
        .iter()
        .find(|(id, _)| *id == HOT_ID)
        .map(|(_, v)| v.as_str())
        .expect("hot partition must be present in the merged result");
    assert_eq!(
        hot_value,
        format!("hot{N_GENS}"),
        "merge must resolve the hot partition to the LATEST generation's value \
         (last-write-wins), proving cross-generation reconciliation actually ran"
    );

    // TRUSTED full-cap stream fast path: LIMIT n (n <= rows) equals the first n
    // oracle rows, in order, over the multi-reader cross-generation merge. In
    // debug builds this also exercises `debug_assert_trusted_prefix`.
    for n in [1usize, 3, 5] {
        let limited = db
            .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE} LIMIT {n}"))
            .await
            .expect("bounded multi-generation scan");
        assert_eq!(
            ordered_rows(&limited.rows),
            oracle[..n].to_vec(),
            "multi-generation LIMIT {n} must equal the first {n} oracle rows, in order"
        );
    }

    // SHORT-STREAM reconciliation branch: LIMIT larger than the table ends the
    // stream before the cap, so `capped_fallback_scan` re-runs the authoritative
    // scan and returns all accepted rows — every row, in oracle order.
    let oversized = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE} LIMIT 100"))
        .await
        .expect("oversized-limit multi-generation scan");
    assert_eq!(
        ordered_rows(&oversized.rows),
        oracle,
        "an oversized LIMIT must reconcile to every row, in unbounded oracle order"
    );
}

/// LIMIT + OFFSET over multiple generations equals the `k..k+n` slice of the
/// unbounded oracle (the cap is `limit + offset`, so the downstream slice always
/// has enough merged rows).
#[tokio::test]
async fn multi_generation_limit_offset_matches_oracle_slice() {
    const N_GENS: i32 = 6;
    const MERGED_ROWS: usize = N_GENS as usize + 1;

    let tmp = TempDir::new().unwrap();
    build_generations(tmp.path(), N_GENS).await;

    let db = open_with_schema(tmp.path()).await;

    let full = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await
        .expect("unbounded scan");
    let oracle = ordered_rows(&full.rows);
    // Same merge-branch proof as above: the overlapping hot partition reconciles to
    // a single last-write-wins row only if the cross-generation merge ran.
    assert_eq!(
        oracle.len(),
        MERGED_ROWS,
        "cross-generation merge must reconcile the hot partition to a single row"
    );
    let hot_value = oracle
        .iter()
        .find(|(id, _)| *id == HOT_ID)
        .map(|(_, v)| v.as_str())
        .expect("hot partition must be present in the merged result");
    assert_eq!(
        hot_value,
        format!("hot{N_GENS}"),
        "merge must resolve the hot partition to the latest generation's value"
    );

    for (n, k) in [(2usize, 0usize), (2, 1), (3, 2), (2, 4)] {
        let q = format!("SELECT * FROM {KEYSPACE}.{TABLE} LIMIT {n} OFFSET {k}");
        let limited = db.execute(&q).await.expect("limit+offset scan");
        assert_eq!(
            ordered_rows(&limited.rows),
            oracle[k..(k + n)].to_vec(),
            "multi-generation LIMIT {n} OFFSET {k} must equal oracle[{k}..{}]",
            k + n
        );
    }
}
