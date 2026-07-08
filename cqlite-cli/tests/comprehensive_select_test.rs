//! Comprehensive CLI integration tests for all 33 test tables
//!
//! Tests that `SELECT * FROM keyspace.table LIMIT 10` succeeds on all tables
//! across all 4 keyspaces: test_basic, test_collections, test_timeseries, test_wide_rows
//!
//! Requirements:
//! - Test data must exist at test-data/datasets/sstables/
//! - All 4 schema files must exist in test-data/schemas/
//!
//! Validates:
//! 1. Exit code 0 - Command succeeded
//! 2. Valid JSON array output - Parses correctly
//! 3. Non-empty results - At least 1 row returned
//! 4. No ERROR messages in stderr - No parsing failures
//! 5. No invalid data markers in output - No corrupted values like <invalid-timestamp>
//!
//! Run with:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-cli comprehensive_select -- --nocapture
//! ```

#![allow(clippy::all)]

use rstest::rstest;
use std::path::{Path, PathBuf};
use std::process::Command;

// =============================================================================
// Test Configuration
// =============================================================================

/// Get datasets root from environment or default path
fn get_datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("test-data/datasets")
        })
}

/// Get schemas directory
fn get_schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/schemas")
}

// =============================================================================
// Test Helper Functions
// =============================================================================

/// Result of a SELECT query test
#[derive(Debug)]
struct QueryTestResult {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    row_count: Option<usize>,
    is_valid_json: bool,
    has_errors: bool,
    has_warnings: bool,
    has_invalid_data: bool,
    error_count: usize,
    warning_count: usize,
}

impl QueryTestResult {
    fn is_success(&self) -> bool {
        self.exit_code == Some(0)
            && self.is_valid_json
            && self.row_count.unwrap_or(0) > 0
            && !self.has_errors
            && !self.has_invalid_data
    }

    /// Check if stderr contains parsing errors or warnings
    fn analyze_stderr(stderr: &str) -> (bool, bool, usize, usize) {
        let has_errors = stderr.contains("ERROR");
        let has_warnings = stderr.contains("WARN")
            && (stderr.contains("malformed")
                || stderr.contains("corruption")
                || stderr.contains("Skipping")
                || stderr.contains("invalid"));

        let error_count = stderr.matches("ERROR").count();
        let warning_count = stderr.matches("WARN").count();

        (has_errors, has_warnings, error_count, warning_count)
    }

    /// Check if output contains invalid data markers
    fn has_invalid_output(stdout: &str) -> bool {
        stdout.contains("<invalid-")
            || stdout.contains("invalid-timestamp")
            || stdout.contains("invalid-date")
            || stdout.contains("invalid-uuid")
    }
}

/// Run SELECT * FROM keyspace.table LIMIT 10 and return results
fn run_select_query(keyspace: &str, table: &str, schema_file: &str) -> QueryTestResult {
    let data_dir = get_datasets_root().join("sstables");
    let schema_path = get_schemas_dir().join(schema_file);
    let query = format!("SELECT * FROM {}.{} LIMIT 10", keyspace, table);

    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args([
            "--schema",
            schema_path.to_str().unwrap(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--query",
            &query,
            "--out",
            "json",
        ])
        .output()
        .expect("Failed to execute CLI command");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // Check if output is valid JSON array
    let trimmed = stdout.trim();
    let is_valid_json = trimmed.starts_with('[') && trimmed.ends_with(']');

    // Count rows if valid JSON
    let row_count = if is_valid_json {
        serde_json::from_str::<Vec<serde_json::Value>>(&stdout)
            .ok()
            .map(|v| v.len())
    } else {
        None
    };

    // Check for errors and warnings in stderr
    let (has_errors, has_warnings, error_count, warning_count) =
        QueryTestResult::analyze_stderr(&stderr);

    // Check for invalid data in output
    let has_invalid_data = QueryTestResult::has_invalid_output(&stdout);

    QueryTestResult {
        exit_code: output.status.code(),
        stdout,
        stderr,
        row_count,
        is_valid_json,
        has_errors,
        has_warnings,
        has_invalid_data,
        error_count,
        warning_count,
    }
}

/// Assert test data is available, skip test if not
fn ensure_test_data_available() -> bool {
    let data_dir = get_datasets_root().join("sstables");
    let schemas_dir = get_schemas_dir();

    if !data_dir.exists() {
        eprintln!(
            "SKIP: Test data not available at {:?}. Set CQLITE_DATASETS_ROOT.",
            data_dir
        );
        return false;
    }

    if !schemas_dir.exists() {
        eprintln!("SKIP: Schema files not available at {:?}", schemas_dir);
        return false;
    }

    true
}

/// Run a test for a specific table
fn run_table_test(keyspace: &str, table: &str, schema_file: &str) {
    if !ensure_test_data_available() {
        return;
    }

    let result = run_select_query(keyspace, table, schema_file);

    // Diagnostic output
    eprintln!("=== {}.{} ===", keyspace, table);
    eprintln!("Exit code: {:?}", result.exit_code);
    eprintln!("Valid JSON: {}", result.is_valid_json);
    eprintln!("Row count: {:?}", result.row_count);
    eprintln!(
        "Errors: {} | Warnings: {} | Invalid data: {}",
        result.error_count, result.warning_count, result.has_invalid_data
    );
    if result.has_errors || result.has_warnings {
        eprintln!(
            "STDERR (first 1000 chars):\n{}",
            &result.stderr[..result.stderr.len().min(1000)]
        );
    }

    // Assertions
    assert_eq!(
        result.exit_code,
        Some(0),
        "{}.{}: Expected exit code 0, got {:?}. STDERR: {}",
        keyspace,
        table,
        result.exit_code,
        &result.stderr[..result.stderr.len().min(500)]
    );

    assert!(
        result.is_valid_json,
        "{}.{}: Expected valid JSON array output. Got: {}...",
        keyspace,
        table,
        &result.stdout[..result.stdout.len().min(200)]
    );

    assert!(
        result.row_count.unwrap_or(0) > 0,
        "{}.{}: Expected non-empty results",
        keyspace,
        table
    );

    // New: Check for parsing errors in stderr
    assert!(
        !result.has_errors,
        "{}.{}: Found {} ERROR messages in stderr. This indicates parsing failures.\nSTDERR:\n{}",
        keyspace,
        table,
        result.error_count,
        &result.stderr[..result.stderr.len().min(2000)]
    );

    // New: Check for invalid data in output
    assert!(
        !result.has_invalid_data,
        "{}.{}: Output contains invalid data markers (e.g., <invalid-timestamp>). This indicates data corruption.\nOutput sample:\n{}",
        keyspace,
        table,
        &result.stdout[..result.stdout.len().min(500)]
    );
}

/// The query LIMIT used by `run_select_query` — expected live counts are capped
/// to this since the CLI SELECT is `... LIMIT 10`.
const SELECT_LIMIT: usize = 10;

/// Locate the `*-Data.db.jsonl` sstabledump golden for `keyspace.table`.
/// Returns `None` when the datasets root, the table directory, or the golden is
/// absent (so callers can distinguish "no fixture" from "0 live rows").
fn find_golden_jsonl(keyspace: &str, table: &str) -> Option<PathBuf> {
    let ks_dir = get_datasets_root().join("sstables").join(keyspace);
    let table_dir = std::fs::read_dir(&ks_dir).ok()?.flatten().find_map(|e| {
        let name = e.file_name();
        let s = name.to_str()?;
        // Directory names are `<table>-<uuid>`; match the exact table prefix.
        if s.starts_with(&format!("{}-", table)) {
            Some(e.path())
        } else {
            None
        }
    })?;
    std::fs::read_dir(&table_dir).ok()?.flatten().find_map(|e| {
        let name = e.file_name();
        let s = name.to_str()?;
        (s.ends_with("-Data.db.jsonl")).then(|| e.path())
    })
}

/// Count rows in a sstabledump JSONL golden that are LIVE under Cassandra
/// `SELECT` semantics at the current wall clock — the same TTL-aware logic the
/// Python/Node parity harnesses use (`count_live_rows_in_jsonl`). A row is
/// excluded when: it is a range-tombstone marker (`type != "row"`), a row
/// tombstone (deletion_info with no cells), or its row-liveness TTL has elapsed
/// and no non-deleted cell keeps it alive (explicit future per-cell expiry, or a
/// live-forever cell marked by its own `tstamp`). Returns `None` on read error.
fn count_live_golden_rows(jsonl_path: &Path) -> Option<usize> {
    use chrono::{DateTime, Utc};

    let content = std::fs::read_to_string(jsonl_path).ok()?;
    let now = Utc::now();

    let parse_expires = |v: Option<&serde_json::Value>| -> Option<DateTime<Utc>> {
        v.and_then(|x| x.as_str())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
    };

    let mut total = 0_usize;
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let partition: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(rows) = partition["rows"].as_array() else {
            continue;
        };
        for row in rows {
            if row["type"].as_str() != Some("row") {
                continue;
            }
            let cells = row["cells"].as_array().cloned().unwrap_or_default();
            if !row["deletion_info"].is_null() && cells.is_empty() {
                continue;
            }

            let liveness = &row["liveness_info"];
            let row_expires = parse_expires(Some(&liveness["expires_at"]));
            let has_ttl = !liveness["ttl"].is_null();
            if has_ttl && row_expires.is_some_and(|exp| exp <= now) {
                // Row-liveness TTL elapsed: survives only if a non-deleted cell
                // keeps it alive (explicit future per-cell expiry, or a
                // live-forever cell marked by its own `tstamp`).
                let cell_still_live = cells.iter().any(|cell| {
                    if !cell["deletion_info"].is_null() {
                        return false;
                    }
                    match parse_expires(Some(&cell["expires_at"])) {
                        Some(cell_exp) => cell_exp > now,
                        None => !cell["tstamp"].is_null(),
                    }
                });
                if !cell_still_live {
                    continue;
                }
            }
            total += 1;
        }
    }
    Some(total)
}

/// TTL-aware table test (issue #1935). Instead of asserting `row_count > 0`
/// (wrong for a wall-clock-expired TTL fixture), assert the CLI's live row count
/// equals the golden-derived expected LIVE count, capped at the query LIMIT.
/// Preserves the missing-fixture guard: 0 live rows is only accepted when the
/// golden is present AND non-empty, so an absent/empty dataset still surfaces as
/// a skip (no fixture) rather than a false pass.
fn run_ttl_aware_table_test(keyspace: &str, table: &str, schema_file: &str) {
    if !ensure_test_data_available() {
        return;
    }

    let Some(golden_path) = find_golden_jsonl(keyspace, table) else {
        eprintln!(
            "SKIP: {}.{}: no sstabledump JSONL golden found; cannot derive TTL-aware expectation",
            keyspace, table
        );
        return;
    };

    let Some(physical_rows) = count_golden_physical_rows(&golden_path) else {
        eprintln!(
            "SKIP: {}.{}: could not read golden {}",
            keyspace,
            table,
            golden_path.display()
        );
        return;
    };
    // Guard against a vacuous pass on an empty golden (issue #1853 finding):
    // 0 live rows must provably mean "expired", not "nothing was written".
    if physical_rows == 0 {
        eprintln!(
            "SKIP: {}.{}: golden has 0 physical rows (empty fixture)",
            keyspace, table
        );
        return;
    }

    let Some(live_rows) = count_live_golden_rows(&golden_path) else {
        eprintln!("SKIP: {}.{}: could not parse golden", keyspace, table);
        return;
    };
    let expected = live_rows.min(SELECT_LIMIT);

    let result = run_select_query(keyspace, table, schema_file);

    eprintln!(
        "=== {}.{} (TTL-aware) === exit={:?} valid_json={} rows={:?} expected_live={} (physical={})",
        keyspace,
        table,
        result.exit_code,
        result.is_valid_json,
        result.row_count,
        expected,
        physical_rows
    );

    assert_eq!(
        result.exit_code,
        Some(0),
        "{}.{}: Expected exit code 0, got {:?}. STDERR: {}",
        keyspace,
        table,
        result.exit_code,
        &result.stderr[..result.stderr.len().min(500)]
    );

    assert!(
        result.is_valid_json,
        "{}.{}: Expected valid JSON array output. Got: {}...",
        keyspace,
        table,
        &result.stdout[..result.stdout.len().min(200)]
    );

    assert_eq!(
        result.row_count.unwrap_or(0),
        expected,
        "{}.{}: TTL-aware live row-count mismatch: CLI returned {:?}, golden-derived expected {} \
         (capped at LIMIT {}; physical golden rows {}). A TTL fixture returning 0 LIVE rows is \
         CORRECT when all rows are wall-clock-expired — see issue #1935/#1853.",
        keyspace,
        table,
        result.row_count,
        expected,
        SELECT_LIMIT,
        physical_rows
    );

    assert!(
        !result.has_errors,
        "{}.{}: Found {} ERROR messages in stderr. This indicates parsing failures.\nSTDERR:\n{}",
        keyspace,
        table,
        result.error_count,
        &result.stderr[..result.stderr.len().min(2000)]
    );

    assert!(
        !result.has_invalid_data,
        "{}.{}: Output contains invalid data markers.\nOutput sample:\n{}",
        keyspace,
        table,
        &result.stdout[..result.stdout.len().min(500)]
    );
}

/// Count total physical `type == "row"` entries in the golden (no TTL/tombstone
/// filtering) — used only to prove the fixture is non-empty so a 0-live result
/// can be attributed to expiry rather than a missing/empty fixture.
fn count_golden_physical_rows(jsonl_path: &Path) -> Option<usize> {
    let content = std::fs::read_to_string(jsonl_path).ok()?;
    let mut total = 0_usize;
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let partition: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(rows) = partition["rows"].as_array() {
            total += rows
                .iter()
                .filter(|r| r["type"].as_str() == Some("row"))
                .count();
        }
    }
    Some(total)
}

// =============================================================================
// test_basic Keyspace Tests (8 tables)
// =============================================================================

#[rstest]
#[case("simple_table")]
#[case("composite_key_table")]
#[case("compression_test_table")]
#[case("counters")]
#[case("multi_partition_table")]
#[case("static_columns_table")] // Issue #255: STATIC column schema parsing
#[case("uncompressed_table")]
// NOTE: `ttl_test_table` is intentionally NOT here — it KEEPS its TTL (the #1853
// seam) so its rows are all wall-clock-expired and a SELECT correctly returns 0
// LIVE rows. It is covered by `test_select_ttl_aware` below, which derives the
// expected LIVE count from the golden instead of asserting `> 0`. See issue #1935.
#[cfg(feature = "state_machine")]
fn test_select_test_basic(#[case] table: &str) {
    run_table_test("test_basic", table, "basic-types.cql");
}

// =============================================================================
// test_collections Keyspace Tests (8 tables)
// =============================================================================

#[rstest]
#[case("collection_clustering_table")]
#[case("collection_table")]
#[case("collections_with_udts")]
#[case("empty_collections_table")]
#[case("frozen_collections_table")]
#[case("large_collections_table")]
#[case("nested_collections_table")]
#[case("typed_collections_table")]
#[cfg(feature = "state_machine")]
fn test_select_test_collections(#[case] table: &str) {
    run_table_test("test_collections", table, "collections.cql");
}

// =============================================================================
// test_timeseries Keyspace Tests (9 tables)
// =============================================================================

#[rstest]
#[case("event_store")]
#[case("sensor_data")]
#[case("stock_prices")]
#[case("time_bucketed_counters")] // Issue #256: Counter table returns 0 rows
#[case("user_activity")]
#[case("user_sessions")]
// NOTE: `app_metrics`, `log_entries`, `tick_data` are covered by
// `test_select_ttl_aware` below (issue #1935). Their `default_time_to_live` was
// removed from the schema; until the corpus binaries are regenerated WITHOUT TTL
// (CI-owned), the shipped fixtures are wall-clock-expired and a SELECT returns 0
// LIVE rows. The TTL-aware assertion derives the expected LIVE count from the
// golden, so it passes both before regen (expected 0) and after (expected > 0).
#[cfg(feature = "state_machine")]
fn test_select_test_timeseries(#[case] table: &str) {
    run_table_test("test_timeseries", table, "time-series.cql");
}

// =============================================================================
// TTL-aware tables (issue #1935 / #1896 cluster A)
// =============================================================================
//
// These tables carry (or carried) a `default_time_to_live`, so their shipped
// fixtures may be entirely wall-clock-expired — a Cassandra `SELECT` (and the
// reader, via #1790 read-time TTL shadowing) then returns 0 LIVE rows. Asserting
// `row_count > 0` is therefore WRONG for them. Instead we derive the expected
// LIVE row count from the sstabledump JSONL golden (the same TTL-aware logic the
// Python/Node parity harnesses use) and assert the CLI matches it (capped at the
// query LIMIT). This is robust across the pending corpus regeneration:
//   - `test_basic.ttl_test_table` KEEPS its TTL (the #1853 seam) → expected 0.
//   - `app_metrics`/`log_entries`/`tick_data` had TTL removed from the schema;
//     pre-regen goldens are expired (expected 0), post-regen goldens carry no
//     TTL (expected = physical count). Either way the derived expectation tracks
//     the fixture, so the test never goes stale on a hardcoded number.
#[rstest]
#[case("test_basic", "ttl_test_table", "basic-types.cql")]
#[case("test_timeseries", "app_metrics", "time-series.cql")]
#[case("test_timeseries", "log_entries", "time-series.cql")]
#[case("test_timeseries", "tick_data", "time-series.cql")]
#[cfg(feature = "state_machine")]
fn test_select_ttl_aware(#[case] keyspace: &str, #[case] table: &str, #[case] schema_file: &str) {
    run_ttl_aware_table_test(keyspace, table, schema_file);
}

// =============================================================================
// test_wide_rows Keyspace Tests (8 tables)
// =============================================================================

#[rstest]
#[case("chat_messages")]
#[case("document_versions")]
#[case("large_blob_table")]
#[case("many_columns_table")]
#[case("multi_metric_timeseries")]
#[case("product_catalog")]
#[case("sparse_data_table")]
#[case("wide_partition_table")]
#[cfg(feature = "state_machine")]
fn test_select_test_wide_rows(#[case] table: &str) {
    run_table_test("test_wide_rows", table, "wide-rows.cql");
}

// =============================================================================
// Summary Report Test
// =============================================================================

/// Run all 33 tables and produce a summary report.
/// This test is ignored by default - run with: cargo test test_all_tables_summary -- --ignored
#[test]
#[ignore]
#[cfg(feature = "state_machine")]
fn test_all_tables_summary() {
    if !ensure_test_data_available() {
        return;
    }

    let mut passed = Vec::new();
    let mut failed = Vec::new();

    // All tables organized by keyspace
    let test_configs = [
        (
            "test_basic",
            "basic-types.cql",
            vec![
                "simple_table",
                "composite_key_table",
                "compression_test_table",
                "counters",
                "multi_partition_table",
                "static_columns_table",
                "ttl_test_table",
                "uncompressed_table",
            ],
        ),
        (
            "test_collections",
            "collections.cql",
            vec![
                "collection_clustering_table",
                "collection_table",
                "collections_with_udts",
                "empty_collections_table",
                "frozen_collections_table",
                "large_collections_table",
                "nested_collections_table",
                "typed_collections_table",
            ],
        ),
        (
            "test_timeseries",
            "time-series.cql",
            vec![
                "app_metrics",
                "event_store",
                "log_entries",
                "sensor_data",
                "stock_prices",
                "tick_data",
                "time_bucketed_counters",
                "user_activity",
                "user_sessions",
            ],
        ),
        (
            "test_wide_rows",
            "wide-rows.cql",
            vec![
                "chat_messages",
                "document_versions",
                "large_blob_table",
                "many_columns_table",
                "multi_metric_timeseries",
                "product_catalog",
                "sparse_data_table",
                "wide_partition_table",
            ],
        ),
    ];

    // TTL-carrying tables can legitimately return 0 LIVE rows (all rows
    // wall-clock-expired) — for them "success" means the CLI's live count equals
    // the golden-derived expectation, not `> 0` (issue #1935).
    let is_ttl_aware = |ks: &str, tbl: &str| {
        matches!(
            (ks, tbl),
            ("test_basic", "ttl_test_table")
                | ("test_timeseries", "app_metrics")
                | ("test_timeseries", "log_entries")
                | ("test_timeseries", "tick_data")
        )
    };

    for (keyspace, schema, tables) in test_configs {
        for table in tables {
            let result = run_select_query(keyspace, table, schema);
            let full_name = format!("{}.{}", keyspace, table);

            let success = if is_ttl_aware(keyspace, table) {
                let expected = find_golden_jsonl(keyspace, table)
                    .and_then(|p| count_live_golden_rows(&p))
                    .map(|n| n.min(SELECT_LIMIT));
                result.exit_code == Some(0)
                    && result.is_valid_json
                    && !result.has_errors
                    && !result.has_invalid_data
                    && expected.is_some_and(|e| result.row_count.unwrap_or(0) == e)
            } else {
                result.is_success()
            };

            if success {
                passed.push(full_name);
            } else {
                failed.push((
                    full_name,
                    format!(
                        "exit={:?}, valid_json={}, rows={:?}",
                        result.exit_code, result.is_valid_json, result.row_count
                    ),
                ));
            }
        }
    }

    // Print summary
    let total = passed.len() + failed.len();
    println!("\n========================================");
    println!("   COMPREHENSIVE TABLE TEST SUMMARY");
    println!("========================================");
    println!("Passed: {}/{}", passed.len(), total);
    println!("Failed: {}/{}", failed.len(), total);

    if !failed.is_empty() {
        println!("\nFailed tables:");
        for (table, reason) in &failed {
            println!("  - {}: {}", table, reason);
        }
    }

    println!("\nPassed tables:");
    for table in &passed {
        println!("  - {}", table);
    }

    // Assert all passed
    assert!(
        failed.is_empty(),
        "{} out of {} tables failed. See details above.",
        failed.len(),
        total
    );
}
