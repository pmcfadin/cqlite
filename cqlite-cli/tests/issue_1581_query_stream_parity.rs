//! Byte-parity guard for issue #1581 (Epic D5): the CLI query path now routes
//! through the BOUNDED streaming engine (`collect_query_result` →
//! `Database::execute_streaming`) instead of the materialize-everything
//! `Database::execute`. This test pins the acceptance bar for the cutover:
//! **the CLI's output bytes must be byte-identical to the pre-change
//! `execute()`-based output** across a matrix of tables × query shapes × output
//! formats (JSON + CSV).
//!
//! Both paths are compared through the SAME CLI writers on the SAME
//! `OutputConfig`, so the only variable is the `QueryResult` produced by the
//! streaming collector vs the materializing `execute()`. Byte-equality therefore
//! proves the cutover preserves rows, row order, and column metadata exactly.
//!
//! Oracle = `Database::execute(q)` (the pre-change data source). Actual =
//! `collect_query_result(db, q, limit)` (the new bounded path). A divergence here
//! is a real regression in the CLI query surface.
//!
//! Requires real Data.db fixtures + `CQLITE_DATASETS_ROOT`. Fails closed once a
//! keyspace's fixtures are present (an ingest/setup failure must FAIL, never let
//! the guard pass vacuously); the only legitimate skip is the genuine absence of
//! the external dataset.

#![cfg(feature = "state_machine")]

use std::path::PathBuf;

use cqlite_cli::commands::collect_query_result;
use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::{CSVWriter, JSONWriter};
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> PathBuf {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().join("test-data").join("schemas")
}

fn keyspace_has_data(keyspace: &str) -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let dir = root.join("sstables").join(keyspace);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return false;
    };
    for entry in entries.flatten() {
        if let Ok(files) = std::fs::read_dir(entry.path()) {
            for file in files.flatten() {
                if file
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with("-Data.db"))
                {
                    return true;
                }
            }
        }
    }
    false
}

async fn setup_db(schema_file: &str, keyspace: &str) -> Database {
    let root = datasets_root().expect("CQLITE_DATASETS_ROOT must be set");
    let schema_path = schemas_dir().join(schema_file);
    assert!(
        schema_path.exists(),
        "schema fixture missing: {schema_path:?}"
    );

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: root.join("sstables"),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    ingest(config)
        .await
        .unwrap_or_else(|e| panic!("ingest({keyspace}) must succeed with fixtures present: {e}"))
        .database
}

fn config_with_limit(limit: Option<usize>) -> OutputConfig {
    OutputConfig {
        limit,
        ..OutputConfig::default()
    }
}

/// Assert JSON + CSV byte-parity between the materializing `execute()` (oracle)
/// and the bounded streaming collector (actual) for one query under one limit.
async fn assert_parity(db: &Database, query: &str, limit: Option<usize>) {
    let cfg = config_with_limit(limit);

    let oracle = db
        .execute(query)
        .await
        .unwrap_or_else(|e| panic!("oracle execute failed for `{query}`: {e}"));
    let actual = collect_query_result(db, query, limit)
        .await
        .unwrap_or_else(|e| panic!("streaming collect failed for `{query}`: {e}"));

    let oracle_json =
        JSONWriter::write(&oracle, &cfg).unwrap_or_else(|e| panic!("oracle json failed: {e}"));
    let actual_json =
        JSONWriter::write(&actual, &cfg).unwrap_or_else(|e| panic!("actual json failed: {e}"));
    assert_eq!(
        oracle_json, actual_json,
        "JSON output diverged for `{query}` (limit={limit:?}): streaming path is not byte-identical to execute()"
    );

    let oracle_csv =
        CSVWriter::write(&oracle, &cfg).unwrap_or_else(|e| panic!("oracle csv failed: {e}"));
    let actual_csv =
        CSVWriter::write(&actual, &cfg).unwrap_or_else(|e| panic!("actual csv failed: {e}"));
    assert_eq!(
        oracle_csv, actual_csv,
        "CSV output diverged for `{query}` (limit={limit:?}): streaming path is not byte-identical to execute()"
    );
}

/// Run the byte-parity query-shape matrix for one (keyspace, table): SELECT *
/// (no limit / SQL LIMIT / display `--limit` / display cap below SQL LIMIT) and a
/// projection of the first column.
///
/// NOTE: partition-key equality predicates (`WHERE pk = ?`) are covered by the
/// dedicated `point_lookup_returns_full_row` test, NOT here — the pre-change
/// `execute()` materializing point-lookup path has a pre-existing core bug (it
/// returns EMPTY `metadata.columns`, so the CLI printed `[{}]`), which the
/// streaming cutover FIXES. Asserting byte-parity against that broken oracle would
/// be asserting parity with a bug. See the point-lookup test for the correctness
/// oracle (the matching full-scan row).
async fn assert_table_matrix(db: &Database, qualified: &str) {
    // SELECT * — no limit, SQL LIMIT, and display (--limit) cap.
    assert_parity(db, &format!("SELECT * FROM {qualified}"), None).await;
    assert_parity(db, &format!("SELECT * FROM {qualified} LIMIT 2"), None).await;
    assert_parity(db, &format!("SELECT * FROM {qualified}"), Some(2)).await;
    // display cap SMALLER than SQL LIMIT — both must truncate to the display cap.
    assert_parity(db, &format!("SELECT * FROM {qualified} LIMIT 5"), Some(1)).await;

    // Discover the result columns from a live SELECT * to build a projection
    // without hard-coding per-table schema.
    let probe = db
        .execute(&format!("SELECT * FROM {qualified} LIMIT 1"))
        .await
        .unwrap_or_else(|e| panic!("probe failed for {qualified}: {e}"));
    assert!(
        !probe.metadata.columns.is_empty(),
        "fixture problem: {qualified} SELECT * produced no columns"
    );
    let first_col = probe.metadata.columns[0].name.clone();

    // Projection of the first column (with and without a display cap).
    assert_parity(
        db,
        &format!("SELECT {first_col} FROM {qualified} LIMIT 3"),
        None,
    )
    .await;
    assert_parity(db, &format!("SELECT {first_col} FROM {qualified}"), Some(2)).await;
}

/// Render a UUID `Value` as a canonical (unquoted) CQL literal.
fn uuid_literal(v: &cqlite_core::Value) -> Option<String> {
    match v {
        cqlite_core::Value::Uuid(bytes) => {
            let h: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            Some(format!(
                "{}-{}-{}-{}-{}",
                &h[0..8],
                &h[8..12],
                &h[12..16],
                &h[16..20],
                &h[20..32]
            ))
        }
        _ => None,
    }
}

/// Regression + correctness guard for the point-lookup FIX (issue #1581).
///
/// On `main` the CLI (via `Database::execute`) prints `[{}]` for
/// `SELECT * FROM t WHERE pk = ?` because the materializing point-lookup path
/// returns an empty `metadata.columns`. The streaming cutover
/// (`collect_query_result`) returns the correct full row.
///
/// The correctness ORACLE here is the matching row from the FULL scan (which
/// `assert_table_matrix` already byte-parity-verified against `execute()`), NOT
/// the buggy `execute()` point-lookup path.
#[tokio::test]
async fn point_lookup_returns_full_row() {
    // (schema, keyspace, table) tables whose partition key is the `id` uuid.
    let cases: &[(&str, &str, &str)] = &[
        ("basic-types.cql", "test_basic", "simple_table"),
        ("collections.cql", "test_collections", "collection_table"),
    ];

    let mut exercised = 0usize;
    for (schema, keyspace, table) in cases {
        if !keyspace_has_data(keyspace) {
            eprintln!("skip {keyspace}: no Data.db fixtures");
            continue;
        }
        let db = setup_db(schema, keyspace).await;
        let qualified = format!("{keyspace}.{table}");
        let cfg = config_with_limit(None);

        // Full scan via the SAME streaming path — the correctness oracle.
        let full = collect_query_result(&db, &format!("SELECT * FROM {qualified}"), None)
            .await
            .unwrap_or_else(|e| panic!("full scan failed for {qualified}: {e}"));
        assert!(
            !full.rows.is_empty(),
            "fixture problem: {qualified} full scan returned no rows"
        );

        // Pick a real `id` value from the first row and build the point lookup.
        let id_val = full.rows[0]
            .values
            .get("id")
            .unwrap_or_else(|| panic!("{qualified} row has no `id` column"));
        let literal = uuid_literal(id_val)
            .unwrap_or_else(|| panic!("{qualified} `id` is not a uuid: {id_val:?}"));

        let pl = collect_query_result(
            &db,
            &format!("SELECT * FROM {qualified} WHERE id = {literal}"),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("point lookup failed for {qualified}: {e}"));

        // The fix: point lookup must NOT be the broken empty result.
        assert!(
            !pl.metadata.columns.is_empty(),
            "REGRESSION: {qualified} point lookup produced empty metadata.columns \
             (the main-branch `[{{}}]` bug)"
        );
        assert_eq!(
            pl.rows.len(),
            1,
            "{qualified} point lookup should return exactly the one matching row"
        );
        assert!(
            pl.rows[0].values.contains_key("id"),
            "{qualified} point-lookup row must include the partition-key `id` column"
        );

        // Correctness oracle: the point-lookup row must byte-match the full-scan
        // row with the same id, rendered through the identical writer + metadata.
        let oracle_rows: Vec<_> = full
            .rows
            .iter()
            .filter(|r| r.values.get("id").map(uuid_literal) == Some(Some(literal.clone())))
            .cloned()
            .collect();
        let oracle = cqlite_core::query::result::QueryResult {
            rows: oracle_rows,
            rows_affected: 0,
            execution_time_ms: 0,
            metadata: full.metadata.clone(),
        };
        let oracle_json = JSONWriter::write(&oracle, &cfg).unwrap();
        let pl_json = JSONWriter::write(&pl, &cfg).unwrap();
        assert_eq!(
            oracle_json, pl_json,
            "{qualified} point-lookup output must equal the matching full-scan row"
        );

        exercised += 1;
    }

    // Fail closed only when real Data.db fixtures are actually present (path
    // existence alone is NOT enough — a worktree checkout has the dataset dir but
    // not the gitignored Data.db binaries; keying off `keyspace_has_data` avoids a
    // false failure there while still catching a present-but-unexercised dataset).
    if cases.iter().any(|(_, ks, _)| keyspace_has_data(ks)) {
        assert!(
            exercised > 0,
            "no point-lookup tables exercised though Data.db fixtures are present"
        );
    } else {
        eprintln!("no Data.db fixtures present — point-lookup test skipped");
    }
}

#[tokio::test]
async fn query_stream_parity_matrix() {
    // (schema file, keyspace, tables) — a representative slice across simple
    // types, collections, time-series, and wide rows.
    let cases: &[(&str, &str, &[&str])] = &[
        (
            "basic-types.cql",
            "test_basic",
            &[
                "simple_table",
                "compression_test_table",
                "static_columns_table",
            ],
        ),
        (
            "collections.cql",
            "test_collections",
            &["collection_table", "typed_collections_table"],
        ),
        (
            "time-series.cql",
            "test_timeseries",
            &["sensor_data", "user_activity"],
        ),
        (
            "wide-rows.cql",
            "test_wide_rows",
            &["wide_partition_table", "large_blob_table"],
        ),
    ];

    let mut exercised = 0usize;
    for (schema, keyspace, tables) in cases {
        if !keyspace_has_data(keyspace) {
            eprintln!("skip {keyspace}: no Data.db fixtures (run fetch-datasets.sh)");
            continue;
        }
        let db = setup_db(schema, keyspace).await;
        for table in *tables {
            let qualified = format!("{keyspace}.{table}");
            assert_table_matrix(&db, &qualified).await;
            exercised += 1;
        }
    }

    // Fail closed only when real Data.db fixtures are actually present (see the
    // point-lookup test above — a worktree has the dataset dir but not the
    // gitignored Data.db binaries, so path existence alone must not fail the run).
    if cases.iter().any(|(_, ks, _)| keyspace_has_data(ks)) {
        assert!(
            exercised > 0,
            "no tables exercised though Data.db fixtures are present"
        );
    } else {
        eprintln!("no Data.db fixtures present — parity matrix skipped");
    }
}
