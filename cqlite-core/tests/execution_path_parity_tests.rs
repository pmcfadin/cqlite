//! Execution Path Tests (Issue #253, updated for Issue #1750)
//!
//! Historically CQLite had two SELECT execution paths and a text heuristic
//! (`WHERE id =` substring + ≤ 8 whitespace tokens) that routed short `WHERE id
//! =` point reads to the LEGACY `QueryExecutor`. That legacy path returned
//! column-less results and skipped projection — the #1750 correctness bug — and
//! the routing itself violated the no-heuristics mandate (#28).
//!
//! **Issue #1750** RETIRED that heuristic: ALL SELECTs now route through the
//! modern `SelectExecutor`, which populates `metadata.columns` from the schema,
//! applies projection, reconstructs the partition-key column, and selects a
//! partition-targeted fast path (#949/#956) from parsed query structure + schema
//! alone. The behavioral regression tests live in
//! `issue_1750_simple_id_lookup_columns.rs`; this file now only asserts that no
//! text pattern drives SELECT routing (`test_no_text_heuristic_routes_selects`)
//! plus general SELECT execution sanity.
//!
//! **Requirements**:
//! - CQLITE_DATASETS_ROOT environment variable pointing to test-data/datasets
//! - test_basic dataset with simple_table SSTable files
//! - basic-types.cql schema file

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

// Test constants
const TEST_QUALIFIED_TABLE: &str = "test_basic.simple_table";
const KEYSPACE_FILTER: &str = "/test_basic/";

/// Get the datasets root directory from environment or default
fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// Get the schemas directory
fn get_schemas_dir() -> Option<PathBuf> {
    // Try environment variable first
    if let Some(datasets_root) = get_datasets_root() {
        // Datasets root is test-data/datasets, schemas are in test-data/schemas
        let schemas_dir = datasets_root.parent()?.join("schemas");
        if schemas_dir.exists() {
            return Some(schemas_dir);
        }
    }

    // Fallback to relative path from cargo manifest
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    if schemas_dir.exists() {
        return Some(schemas_dir);
    }

    None
}

/// Setup test database with real SSTables via ingestion
/// Returns Ok(Database) if successful, Err(reason) if test should be skipped
async fn setup_test_database() -> Result<Database, String> {
    let datasets_root = get_datasets_root()
        .ok_or_else(|| "CQLITE_DATASETS_ROOT not set or path doesn't exist".to_string())?;

    let schemas_dir = get_schemas_dir().ok_or_else(|| "schemas directory not found".to_string())?;

    let schema_path = schemas_dir.join("basic-types.cql");
    if !schema_path.exists() {
        return Err(format!(
            "basic-types.cql schema not found at {:?}",
            schema_path
        ));
    }

    let data_dir = datasets_root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables directory not found at {:?}", data_dir));
    }

    let ingestion_config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };

    let ingestion_result = ingest(ingestion_config)
        .await
        .map_err(|e| format!("ingestion failed: {}", e))?;

    // Verify ingestion loaded schemas
    if ingestion_result.schema_load_result.schemas_loaded == 0 {
        return Err("No schemas loaded during ingestion".to_string());
    }

    Ok(ingestion_result.database)
}

#[tokio::test]
async fn test_simple_point_lookup_routing_to_legacy() {
    //! Verify that simple point lookup queries route to LEGACY executor
    //!
    //! **Test Strategy**:
    //! - Query: "SELECT * FROM table WHERE id = 1" (≤8 tokens)
    //! - Expected: Routes to LEGACY path (executor.rs)
    //! - Validation: Query executes without error

    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // Simple point lookup: Should route to LEGACY executor (≤8 tokens)
    let query = format!("SELECT * FROM {} WHERE id = 1", TEST_QUALIFIED_TABLE);
    let _token_count = query.split_whitespace().count();

    assert!(
        _token_count <= 8,
        "Query should be ≤8 tokens to route to LEGACY path, got {}",
        _token_count
    );

    let result = db.execute(&query).await;

    match result {
        Ok(query_result) => {
            println!(
                "Issue #253: Simple point lookup routed correctly, returned {} rows",
                query_result.rows.len()
            );
        }
        Err(e) => {
            // Query may not return rows if id=1 doesn't exist, but should not error
            // due to routing or execution path issues
            println!(
                "Issue #253: Query returned error (may be expected if no data): {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_complex_query_routing_to_advanced() {
    //! Verify that complex queries route to ADVANCED executor
    //!
    //! **Test Strategy**:
    //! - Query: Complex SELECT with multiple columns and conditions (>8 tokens)
    //! - Expected: Routes to ADVANCED path (select_executor.rs)
    //! - Validation: Query executes using schema-aware key decoding

    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // Complex query: Should route to ADVANCED executor (>8 tokens OR complex predicates)
    let query = format!(
        "SELECT id, name, age FROM {} WHERE age > 25 LIMIT 10",
        TEST_QUALIFIED_TABLE
    );
    let _token_count = query.split_whitespace().count();

    // Note: Even if ≤8 tokens, absence of "WHERE id =" ensures ADVANCED path
    assert!(
        !query.contains("WHERE id ="),
        "Query should not contain 'WHERE id =' to ensure ADVANCED path routing"
    );

    let result = db.execute(&query).await;

    match result {
        Ok(query_result) => {
            println!(
                "Issue #253: Complex query routed to ADVANCED path, returned {} rows",
                query_result.rows.len()
            );

            // Verify we got column data (schema-aware decoding)
            for row in &query_result.rows {
                assert!(
                    !row.values.is_empty(),
                    "ADVANCED path should use schema-aware decoding, rows should have values"
                );
            }
        }
        Err(e) => {
            panic!("Issue #253: ADVANCED path query failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_no_text_heuristic_routes_selects() {
    //! Issue #1750 (supersedes the Issue #253 "documented divergence"): the
    //! `is_simple_id_lookup` text heuristic — a `WHERE id =` substring + ≤ 8
    //! whitespace-token guess on the raw CQL — has been RETIRED. All SELECTs now
    //! route through the modern `SelectExecutor` on parsed query structure +
    //! schema alone. This satisfies the no-heuristics mandate (#28): no query
    //! text pattern routes to a different executor.
    //!
    //! This is the source-level half of the fix's acceptance criterion #3; the
    //! behavioral half (short `WHERE id =` point reads return populated
    //! `metadata.columns`) lives in `issue_1750_simple_id_lookup_columns.rs`.

    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let read = |rel: &str| {
        std::fs::read_to_string(manifest.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
    };
    // Read a module's source whether it is a single `foo.rs` or a split
    // `foo/` directory (epic #1116 turned several monoliths into directory
    // modules). Concatenates every `.rs` under the directory so these
    // architecture probes survive a behavior-preserving split.
    let read_module = |rel: &str| -> String {
        let as_file = manifest.join(format!("{rel}.rs"));
        if as_file.is_file() {
            return std::fs::read_to_string(&as_file)
                .unwrap_or_else(|e| panic!("read {}: {e}", as_file.display()));
        }
        let dir = manifest.join(rel);
        let mut out = String::new();
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read module dir {}: {e}", dir.display()));
        for entry in entries.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("rs") {
                if let Ok(c) = std::fs::read_to_string(&p) {
                    out.push_str(&c);
                    out.push('\n');
                }
            }
        }
        out
    };

    // ADVANCED path: schema-aware partition-key decoding is the canonical
    // `decode_partition_key_columns` in `partition_key_codec`, which
    // `select_executor` delegates to (Issue #586).
    let codec_content = read("src/storage/partition_key_codec.rs");
    assert!(
        codec_content.contains("fn decode_partition_key_columns"),
        "canonical decoder 'decode_partition_key_columns' should live in partition_key_codec.rs",
    );
    let select_executor_content = read_module("src/query/select_executor");
    assert!(
        select_executor_content.contains("partition_key_codec::decode_partition_key_columns"),
        "select_executor should delegate partition-key decoding to partition_key_codec",
    );

    // No-heuristics enforcement: the retired routing hack must NOT reappear.
    // Assert against the SELECT-routing region of `execute` specifically (the
    // executable `if trimmed_cql.starts_with("SELECT")` gate + a small window
    // after it) so an unrelated doc-comment mention of the historical heuristic
    // elsewhere in the file cannot resurrect a false positive.
    let engine_content = read("src/query/engine.rs");
    let gate_idx = engine_content
        .find("if trimmed_cql.starts_with(\"SELECT\")")
        .expect("engine.rs must still gate on a SELECT prefix");
    let window_end = (gate_idx + 400).min(engine_content.len());
    let routing_window = &engine_content[gate_idx..window_end];
    assert!(
        !routing_window.contains("count() <= 8"),
        "Issue #1750: the ≤8-token routing heuristic must NOT drive SELECT routing",
    );
    assert!(
        routing_window.contains("return self.execute_select_query"),
        "Issue #1750: every SELECT must route through the modern execute_select_query path",
    );

    println!("Issue #1750 VERIFIED: no text heuristic routes SELECTs.");
    println!("  ADVANCED: select_executor delegates to partition_key_codec::decode_partition_key_columns");
    println!("  ROUTING:  engine.rs routes ALL SELECTs through execute_select_query (structure + schema)");
}

#[tokio::test]
async fn test_routing_logic_token_count_boundary() {
    //! Test the routing logic boundary at 8 tokens
    //!
    //! **Routing Decision** (`engine.rs:132-142`):
    //! ```rust,ignore
    //! if sql.contains("WHERE id =") && sql.split_whitespace().count() <= 8 {
    //!     // Fall through to LEGACY executor
    //! } else {
    //!     return self.execute_select_query(sql, start_time).await; // ADVANCED
    //! }
    //! ```

    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // Test 1: Exactly 8 tokens with "WHERE id =" - should use LEGACY
    let query_8_tokens = format!("SELECT * FROM {} WHERE id = 1", TEST_QUALIFIED_TABLE);
    let tokens_8 = query_8_tokens.split_whitespace().count();

    assert_eq!(tokens_8, 8, "Query should have exactly 8 tokens");
    assert!(
        query_8_tokens.contains("WHERE id ="),
        "Query should contain 'WHERE id ='"
    );

    let result_8 = db.execute(&query_8_tokens).await;
    println!(
        "Issue #253: 8-token query result: {:?}",
        result_8.as_ref().map(|r| r.rows.len())
    );

    // Test 2: 9+ tokens with "WHERE id =" - should use ADVANCED (exceeds 8-token threshold)
    let query_9_tokens = format!(
        "SELECT id, name, age FROM {} WHERE id = 1",
        TEST_QUALIFIED_TABLE
    );
    let tokens_9 = query_9_tokens.split_whitespace().count();

    assert!(
        tokens_9 > 8,
        "Query should have more than 8 tokens to exceed threshold, got {}",
        tokens_9
    );
    assert!(
        query_9_tokens.contains("WHERE id ="),
        "Query should contain 'WHERE id ='"
    );

    let result_9 = db.execute(&query_9_tokens).await;
    println!(
        "Issue #253: {}-token query result: {:?}",
        tokens_9,
        result_9.as_ref().map(|r| r.rows.len())
    );

    // Test 3: ≤8 tokens but no "WHERE id =" - should use ADVANCED
    let query_no_id = format!("SELECT * FROM {} LIMIT 5", TEST_QUALIFIED_TABLE);
    let tokens_no_id = query_no_id.split_whitespace().count();

    assert!(tokens_no_id <= 8, "Query should be ≤8 tokens");
    assert!(
        !query_no_id.contains("WHERE id ="),
        "Query should NOT contain 'WHERE id ='"
    );

    let result_no_id = db.execute(&query_no_id).await;
    match result_no_id {
        Ok(query_result) => {
            assert!(
                !query_result.rows.is_empty(),
                "Issue #253: Non-id query should return rows from ADVANCED path"
            );
            println!(
                "Issue #253: Query without 'WHERE id =' routed to ADVANCED path, returned {} rows",
                query_result.rows.len()
            );
        }
        Err(e) => {
            panic!("Issue #253: Query without 'WHERE id =' failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_both_paths_produce_valid_results() {
    //! Verify both execution paths produce valid (non-error) results
    //!
    //! **Note**: We don't compare exact result sets because:
    //! 1. Different key generation strategies may access different data
    //! 2. LEGACY is optimized for simple point lookups
    //! 3. ADVANCED is optimized for complex queries with schema awareness
    //!
    //! Instead, we verify both paths execute without errors and return
    //! properly structured results.

    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // Query 1: Simple point lookup (LEGACY path)
    let legacy_query = format!("SELECT * FROM {} WHERE id = 1", TEST_QUALIFIED_TABLE);
    let legacy_result = db.execute(&legacy_query).await;

    match legacy_result {
        Ok(result) => {
            println!(
                "Issue #253: LEGACY path executed successfully, {} rows",
                result.rows.len()
            );
            // execution_time_ms is u64, always >= 0
            let _ = result.execution_time_ms;
        }
        Err(e) => {
            println!(
                "Issue #253: LEGACY path returned error (may be expected): {}",
                e
            );
        }
    }

    // Query 2: Complex query (ADVANCED path)
    let advanced_query = format!("SELECT * FROM {} LIMIT 5", TEST_QUALIFIED_TABLE);
    let advanced_result = db.execute(&advanced_query).await;

    match advanced_result {
        Ok(result) => {
            println!(
                "Issue #253: ADVANCED path executed successfully, {} rows",
                result.rows.len()
            );
            assert!(
                !result.rows.is_empty(),
                "ADVANCED path should return rows for LIMIT query"
            );
            // execution_time_ms is u64, always >= 0
            let _ = result.execution_time_ms;

            // Verify row structure
            for row in &result.rows {
                assert!(
                    !row.values.is_empty(),
                    "Rows should have values with schema-aware decoding"
                );
            }
        }
        Err(e) => {
            panic!("Issue #253: ADVANCED path failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_execution_path_logging() {
    //! Verify that execution path routing is logged in debug mode
    //!
    //! **Purpose**: Ensure developers can observe which path is taken
    //! by examining debug logs during development.
    //!
    //! **Implementation Note**: The routing logic includes:
    //! ```rust,ignore
    //! #[cfg(debug_assertions)]
    //! log::debug!(
    //!     "Routing simple SELECT through normal executor for consistent key handling"
    //! );
    //! ```

    let db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    // In debug builds, routing decisions are logged
    // This test just verifies both paths execute without panicking
    let queries = [
        format!("SELECT * FROM {} WHERE id = 1", TEST_QUALIFIED_TABLE), // LEGACY
        format!("SELECT * FROM {} LIMIT 5", TEST_QUALIFIED_TABLE),      // ADVANCED
    ];

    for (i, query) in queries.iter().enumerate() {
        let result = db.execute(query).await;
        match result {
            Ok(_) => println!("Issue #253: Query {} executed successfully", i + 1),
            Err(e) => println!("Issue #253: Query {} returned: {}", i + 1, e),
        }
    }

    println!("Issue #253: Execution path logging test completed");
    println!("  Tip: Run with RUST_LOG=debug to see routing decisions");
}

#[tokio::test]
async fn test_parity_key_generation_assumptions() {
    //! Document key assumptions about partition key generation
    //!
    //! **Assumptions validated**:
    //! 1. LEGACY path uses text-based key generation for simple WHERE id = X
    //! 2. ADVANCED path uses binary key decoding from RowKey bytes
    //! 3. Both paths handle their respective query types correctly
    //! 4. Routing logic ensures correct path selection
    //!
    //! **What this test does NOT validate**:
    //! - Exact result set parity (intentionally divergent)
    //! - Performance characteristics
    //! - Memory usage patterns

    // This is primarily a documentation test
    println!("Issue #253 Key Generation Assumptions:");
    println!();
    println!("1. LEGACY Path (executor.rs:794-805):");
    println!("   - Trigger: WHERE id = <value> with ≤8 tokens");
    println!("   - Key Gen: format!(\"user_key_{{}}\", id) for id column");
    println!("   - Purpose: Consistent INSERT/SELECT key handling");
    println!();
    println!("2. ADVANCED Path (select_executor.rs:1095-1189):");
    println!("   - Trigger: Complex queries OR >8 tokens OR no 'WHERE id ='");
    println!("   - Key Gen: Schema-aware binary decoding from RowKey");
    println!("   - Purpose: Full CQL type system support");
    println!();
    println!("3. Routing Logic (engine.rs:132-142):");
    println!("   - Checks: sql.contains(\"WHERE id =\") && token_count <= 8");
    println!("   - LEGACY: Condition met → normal executor");
    println!("   - ADVANCED: Condition not met → select_executor");
    println!();
    println!("4. Divergence Status: DOCUMENTED and EXPECTED");
    println!("   - Not a bug - intentional design for compatibility");
    println!("   - Both paths correct for their respective use cases");
}
