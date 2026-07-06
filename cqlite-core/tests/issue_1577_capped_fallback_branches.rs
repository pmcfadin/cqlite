//! Issue #1577 (Epic D / D1): end-to-end coverage for the LIMIT-pushdown
//! `capped_fallback_scan` branches that the single-reader `simple_table` fixture
//! never exercises — the MULTI-READER / MULTI-GENERATION merge-under-cap path and
//! the SHORT-STREAM reconciliation path.
//!
//! A `SELECT ... LIMIT n` with no partition-key restriction routes through
//! `FallbackFullScan` → `capped_fallback_scan`, whose `scan_stream` (with
//! `write-support` + a resolved schema + more than one generation) reconciles
//! across generations via `merge_generations_for_read` — the exact multi-reader
//! merge the materializing `scan` uses. This test builds several REAL CQLite
//! generations (the write path is byte-parity with Cassandra, M5), opens them via
//! `Database`, and asserts every bounded result is byte-identical (same rows,
//! same order) to the corresponding prefix of the UNBOUNDED oracle:
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
//! Gated on `write-support` (to build the generations) and `state_machine` (to
//! run SELECTs via `Database::execute`), so the file is empty in the minimal
//! build.
//!
//! Run with:
//!   cargo test --package cqlite-core \
//!     --features write-support,state_machine \
//!     --test issue_1577_capped_fallback_branches

#![cfg(all(feature = "write-support", feature = "state_machine"))]

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

/// Write `n_gens` SSTable generations, one distinct partition (`id = 1..=n_gens`)
/// per generation, flushing between each so no compaction merges them. Every
/// generation lands in `<data_dir>/<keyspace>/<table>/nb-<gen>-big-*`.
async fn build_generations(root: &Path, n_gens: i32) {
    let data_dir = root.join("data");
    let wal_dir = root.join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, items_schema());
    let mut engine = WriteEngine::new(config).expect("write engine");

    for id in 1..=n_gens {
        let pk = PartitionKey::single("id", Value::Integer(id));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(format!("v{id}")),
        }];
        engine
            .write_async(Mutation::new(
                TableId::new(KEYSPACE, TABLE),
                pk,
                None,
                ops,
                1_000 + id as i64,
                None,
            ))
            .await
            .expect("write partition");
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

    let tmp = TempDir::new().unwrap();
    build_generations(tmp.path(), N_GENS).await;

    let db = Database::open(&tmp.path().join("data"), Config::default())
        .await
        .expect("open db over multi-generation directory");

    // Oracle: the UNBOUNDED scan, reconciled across all N generations.
    let full = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await
        .expect("unbounded scan");
    let oracle = ordered_rows(&full.rows);
    assert_eq!(
        oracle.len(),
        N_GENS as usize,
        "present multi-generation fixture must return exactly {N_GENS} rows \
         (0/low = read regression, not a skip)"
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

    let tmp = TempDir::new().unwrap();
    build_generations(tmp.path(), N_GENS).await;

    let db = Database::open(&tmp.path().join("data"), Config::default())
        .await
        .expect("open db over multi-generation directory");

    let full = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await
        .expect("unbounded scan");
    let oracle = ordered_rows(&full.rows);
    assert_eq!(
        oracle.len(),
        N_GENS as usize,
        "fixture must return all rows"
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
