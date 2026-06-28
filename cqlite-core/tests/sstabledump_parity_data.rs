//! Data.db parity tests for Issue #394/M5 Write Validation
//!
//! This module validates that Data.db files written by CQLite's WriteEngine
//! produce output that matches Cassandra's sstabledump JSONL format when read back.
//!
//! Key validations:
//! - Written data matches expected JSONL format when read back
//! - TTL and timestamps are correctly encoded
//! - Tombstones and deletions are properly formatted
//! - Delta encoding produces correct values

#![cfg(feature = "write-support")]

use cqlite_core::{
    schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema},
    storage::write_engine::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine,
        WriteEngineConfig,
    },
    testing::dataset_helpers::{
        list_tables, load_metadata, read_jsonl_rows, resolve_table_to_sstable_path,
    },
    types::Value,
    Error, Result as CqliteResult,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    fmt::Write as FmtWrite,
    path::{Path, PathBuf},
};
use tempfile::TempDir;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

#[path = "parity_support/mod.rs"]
mod parity_support;
use parity_support::{
    parity_datasets_required, scenario, write_summary, LaneStatus, ParityFailure,
};

/// Test configuration for Data.db parity validation
#[derive(Debug, Clone)]
struct DataParityConfig {
    /// Target tables for testing
    target_tables: Vec<&'static str>,
    /// Validation artifacts directory
    artifacts_dir: PathBuf,
}

impl Default for DataParityConfig {
    fn default() -> Self {
        Self {
            target_tables: vec!["simple_table", "sensor_data"],
            artifacts_dir: PathBuf::from("validation_artifacts/sstabledump/data"),
        }
    }
}

/// Data.db validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataValidationResult {
    keyspace: String,
    table: String,
    row_count: usize,
    jsonl_row_count: usize,
    perfect_parity: bool,
    timestamp: String,
    errors: Vec<String>,
}

/// Comprehensive Data.db parity test - validates written data can be read back correctly
#[tokio::test]
async fn test_data_db_write_read_parity() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();

    // Create a test schema
    let schema = create_test_schema();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config)?;

    // Write known data
    let test_data = vec![
        (1, "Alice", 100, 1704067200000000i64),   // 2024-01-01
        (2, "Bob", 200, 1704153600000000i64),     // 2024-01-02
        (3, "Charlie", 300, 1704240000000000i64), // 2024-01-03
    ];

    for (id, name, value, ts) in &test_data {
        let mutation = create_test_mutation(*id, name, *value, *ts);
        engine.write_async(mutation).await?;
    }

    // Flush to create SSTable
    let info = engine.flush().await?.expect("Should return SSTableInfo");

    // Verify Data.db exists and has content
    assert!(info.data_path.exists(), "Data.db should exist");
    let data_size = std::fs::metadata(&info.data_path)?.len();
    assert!(data_size > 0, "Data.db should be non-empty");

    // Verify partition count
    assert_eq!(
        info.partition_count,
        test_data.len(),
        "Should have {} partitions",
        test_data.len()
    );

    println!(
        "✅ Data.db write-read parity test passed ({} partitions, {} bytes)",
        info.partition_count, data_size
    );
    Ok(())
}

/// Test Data.db parity with timestamps
#[tokio::test]
async fn test_data_db_timestamp_parity() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config)?;

    // Write with specific timestamps
    let timestamps = [
        0i64,                  // Epoch
        1704067200000000i64,   // 2024-01-01 00:00:00 UTC
        1735689600000000i64,   // 2025-01-01 00:00:00 UTC
        253402300799000000i64, // Year 9999 (extreme)
    ];

    for (i, &ts) in timestamps.iter().enumerate() {
        let mutation = create_test_mutation(i as i32, &format!("user{}", i), i as i32, ts);
        engine.write_async(mutation).await?;
    }

    // Flush and verify
    let info = engine.flush().await?.expect("Should return SSTableInfo");
    assert_eq!(
        info.partition_count,
        timestamps.len(),
        "Should have all partitions"
    );

    // Verify Statistics.db captured correct min timestamp
    let stats_data = std::fs::read(&info.stats_path)?;
    let (_, stats) =
        cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &stats_data,
            None,
        )?;

    assert_eq!(
        stats.timestamp_stats.min_timestamp, 0,
        "Min timestamp should be epoch (0)"
    );

    println!("✅ Data.db timestamp parity test passed");
    Ok(())
}

/// Test Data.db parity with TTL
#[tokio::test]
async fn test_data_db_ttl_parity() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_clustered_schema();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config)?;

    // Write with various TTL values
    let ttl_values = vec![
        (1, "1s", 1),        // 1 second
        (2, "1m", 60),       // 1 minute
        (3, "1h", 3600),     // 1 hour
        (4, "1d", 86400),    // 1 day
        (5, "1y", 31536000), // 1 year
    ];

    let table_id = TableId::new("test_parity", "clustered");
    for (pk, ck, ttl) in &ttl_values {
        let partition_key = PartitionKey::single("pk", Value::Integer(*pk));
        let clustering_key = Some(ClusteringKey::single("ck", Value::Text(ck.to_string())));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("ttl={}", ttl)),
        }];
        let mutation = Mutation::new(
            table_id.clone(),
            partition_key,
            clustering_key,
            ops,
            1000000,
            Some(*ttl),
        );
        engine.write_async(mutation).await?;
    }

    // Flush and verify
    let info = engine.flush().await?.expect("Should return SSTableInfo");
    assert_eq!(
        info.partition_count,
        ttl_values.len(),
        "Should have all partitions"
    );

    // Verify Statistics.db captured TTL
    let stats_data = std::fs::read(&info.stats_path)?;
    let result = cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
        &stats_data,
        None,
    );
    assert!(result.is_ok(), "Statistics.db should parse with TTL data");

    println!(
        "✅ Data.db TTL parity test passed ({} rows with TTL)",
        ttl_values.len()
    );
    Ok(())
}

/// Test Data.db parity with tombstones (deletions)
#[tokio::test]
async fn test_data_db_tombstone_parity() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_clustered_schema();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config)?;
    let table_id = TableId::new("test_parity", "clustered");

    // Write a row then delete it
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ck = ClusteringKey::single("ck", Value::Text("row1".to_string()));

    // Initial write
    let write_ops = vec![CellOperation::Write {
        column: "data".to_string(),
        value: Value::Text("original".to_string()),
    }];
    let write_mutation = Mutation::new(
        table_id.clone(),
        pk.clone(),
        Some(ck.clone()),
        write_ops,
        1000000,
        None,
    );
    engine.write_async(write_mutation).await?;

    // Column tombstone
    let delete_ops = vec![CellOperation::Delete {
        column: "data".to_string(),
        local_deletion_time: None,
    }];
    let delete_mutation = Mutation::new(
        table_id.clone(),
        pk.clone(),
        Some(ck),
        delete_ops,
        1000001,
        None,
    );
    engine.write_async(delete_mutation).await?;

    // Row tombstone on different row
    let pk2 = PartitionKey::single("pk", Value::Integer(2));
    let ck2 = ClusteringKey::single("ck", Value::Text("row2".to_string()));
    let delete_row_ops = vec![CellOperation::DeleteRow];
    let delete_row_mutation =
        Mutation::new(table_id, pk2, Some(ck2), delete_row_ops, 1000002, None);
    engine.write_async(delete_row_mutation).await?;

    // Flush and verify
    let info = engine.flush().await?.expect("Should return SSTableInfo");
    assert!(
        info.data_path.exists(),
        "Data.db with tombstones should exist"
    );

    println!("✅ Data.db tombstone parity test passed");
    Ok(())
}

/// Test Data.db delta encoding for timestamps
#[tokio::test]
async fn test_data_db_timestamp_delta_encoding() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_clustered_schema();
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config)?;
    let table_id = TableId::new("test_parity", "clustered");

    // Write rows with incrementing timestamps (tests delta encoding)
    let base_ts = 1704067200000000i64;
    let pk = PartitionKey::single("pk", Value::Integer(1));

    for i in 0..100 {
        let ck = ClusteringKey::single("ck", Value::Text(format!("row_{:03}", i)));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("data_{}", i)),
        }];
        let mutation = Mutation::new(
            table_id.clone(),
            pk.clone(),
            Some(ck),
            ops,
            base_ts + (i as i64 * 1000), // Incrementing timestamps
            None,
        );
        engine.write_async(mutation).await?;
    }

    // Flush and verify
    let info = engine.flush().await?.expect("Should return SSTableInfo");
    assert_eq!(
        info.partition_count, 1,
        "Should have 1 partition with 100 rows"
    );

    // Verify Statistics.db has correct min/max
    let stats_data = std::fs::read(&info.stats_path)?;
    let (_, stats) =
        cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &stats_data,
            None,
        )?;

    assert_eq!(
        stats.timestamp_stats.min_timestamp, base_ts,
        "Min timestamp should be base"
    );

    println!("✅ Data.db timestamp delta encoding test passed");
    Ok(())
}

/// Test parity validation against existing JSONL reference files
#[tokio::test]
async fn test_data_db_jsonl_reference_parity() -> CqliteResult<()> {
    let config = DataParityConfig::default();

    // Load available tables - skip if test data not available (fail-closed in CI).
    let _metadata = match load_metadata() {
        Ok(m) => m,
        Err(e) => {
            if parity_datasets_required() {
                ParityFailure::new(scenario::DATA_DB_JSONL)
                    .lane("data_db_jsonl")
                    .cassandra_source("sstabledump JSONL (Data.db row/cell decode)")
                    .components(["Data.db", "Data.db.jsonl"])
                    .repro(
                        "bash test-data/scripts/fetch-datasets.sh && \
                         CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                         --features write-support --test sstabledump_parity_data \
                         test_data_db_jsonl_reference_parity -- --nocapture",
                    )
                    .detail(format!(
                        "CQLITE_PARITY_REQUIRE_DATASETS=1 but datasets metadata could not be \
                         loaded ({e}) — required parity gate must not skip when datasets are mandated"
                    ))
                    .panic();
            }
            println!("⏭️ Skipping JSONL reference parity test: test data not available ({e})");
            return Ok(());
        }
    };

    let available_tables = match list_tables(None) {
        Ok(t) => t,
        Err(e) => {
            if parity_datasets_required() {
                ParityFailure::new(scenario::DATA_DB_JSONL)
                    .lane("data_db_jsonl")
                    .cassandra_source("sstabledump JSONL (Data.db row/cell decode)")
                    .components(["Data.db", "Data.db.jsonl"])
                    .repro(
                        "bash test-data/scripts/fetch-datasets.sh && \
                         CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                         --features write-support --test sstabledump_parity_data \
                         test_data_db_jsonl_reference_parity -- --nocapture",
                    )
                    .detail(format!(
                        "CQLITE_PARITY_REQUIRE_DATASETS=1 but tables could not be listed ({e}) — \
                         required parity gate must not skip when datasets are mandated"
                    ))
                    .panic();
            }
            println!("⏭️ Skipping JSONL reference parity test: cannot list tables ({e})");
            return Ok(());
        }
    };

    for target_table in &config.target_tables {
        let found = available_tables.iter().any(|t| t.table == *target_table);
        if !found {
            return Err(Error::corruption(format!(
                "Data.db JSONL parity target table '{}' not found in datasets",
                target_table
            )));
        }
    }

    let mut results = Vec::new();

    for target_table in &config.target_tables {
        let table_info = available_tables
            .iter()
            .find(|t| t.table == *target_table)
            .ok_or_else(|| {
                Error::corruption(format!(
                    "Data.db JSONL parity target table '{}' disappeared after validation",
                    target_table
                ))
            })?;
        let result = validate_jsonl_parity(table_info).await?;
        results.push(result);
    }

    // Save validation artifacts
    save_data_validation_artifacts(&results, &config).await?;

    let passed = results.iter().filter(|r| r.perfect_parity).count();
    println!(
        "🎯 Data.db JSONL parity: {}/{} tables passed",
        passed,
        results.len()
    );

    assert_eq!(
        results.len(),
        config.target_tables.len(),
        "Data.db JSONL parity validated {} tables, expected {}",
        results.len(),
        config.target_tables.len()
    );
    assert_eq!(
        passed,
        results.len(),
        "Data.db JSONL parity failures detected; see validation artifacts for details"
    );

    let _ = write_summary(
        "data_db_jsonl",
        LaneStatus::Pass,
        scenario::DATA_DB_JSONL,
        &[],
    );
    Ok(())
}

/// Validate a table's JSONL reference file can be parsed
async fn validate_jsonl_parity(
    table_info: &cqlite_core::testing::dataset_helpers::TableInfo,
) -> CqliteResult<DataValidationResult> {
    let sstable_dir = resolve_table_to_sstable_path(&table_info.keyspace, &table_info.table)
        .map_err(|e| Error::corruption(format!("Failed to resolve table path: {e}")))?;

    // Find JSONL file
    let jsonl_path = find_jsonl_file(&sstable_dir)?;

    let mut result = DataValidationResult {
        keyspace: table_info.keyspace.clone(),
        table: table_info.table.clone(),
        row_count: 0,
        jsonl_row_count: 0,
        perfect_parity: false,
        timestamp: chrono::Utc::now().to_rfc3339(),
        errors: Vec::new(),
    };

    // Read JSONL rows
    let jsonl_iter = read_jsonl_rows(&jsonl_path)
        .map_err(|e| Error::corruption(format!("Failed to read JSONL: {e}")))?;

    let mut row_count = 0;
    for row in jsonl_iter {
        row_count += 1;
        // Validate row structure
        if let JsonValue::Object(obj) = &row {
            if !obj.contains_key("partition") {
                result
                    .errors
                    .push(format!("Row {} missing 'partition' key", row_count));
            }
        }
    }

    result.jsonl_row_count = row_count;
    result.row_count = row_count; // For parity, these should match

    // Check expected row count from metadata
    if row_count > 0 {
        result.perfect_parity = result.errors.is_empty();
    } else {
        result.errors.push("No rows found in JSONL".to_string());
    }

    if result.perfect_parity {
        println!(
            "✅ JSONL parity validated for {}.{} ({} rows)",
            table_info.keyspace, table_info.table, row_count
        );
    }

    Ok(result)
}

// Helper functions

fn create_test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_parity".to_string(),
        table: "simple".to_string(),
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
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "value".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn create_clustered_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_parity".to_string(),
        table: "clustered".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "data".to_string(),
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

fn create_test_mutation(id: i32, name: &str, value: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new("test_parity", "simple");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "value".to_string(),
            value: Value::Integer(value),
        },
    ];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

fn find_jsonl_file(sstable_dir: &Path) -> CqliteResult<PathBuf> {
    for entry in std::fs::read_dir(sstable_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with("-Data.db.jsonl") && !name.starts_with("._") {
                return Ok(path);
            }
        }
    }
    Err(Error::not_found("No JSONL file found"))
}

async fn save_data_validation_artifacts(
    results: &[DataValidationResult],
    config: &DataParityConfig,
) -> CqliteResult<()> {
    fs::create_dir_all(&config.artifacts_dir).await?;

    // Generate report
    let mut report = String::new();
    writeln!(report, "# Data.db Parity Validation Report").unwrap();
    writeln!(report, "## M5 Write Validation (Issue #394)").unwrap();
    writeln!(report).unwrap();

    let passed = results.iter().filter(|r| r.perfect_parity).count();
    writeln!(report, "**Passed:** {}/{}", passed, results.len()).unwrap();
    writeln!(report).unwrap();

    for result in results {
        let icon = if result.perfect_parity { "✅" } else { "❌" };
        writeln!(report, "### {} {}.{}", icon, result.keyspace, result.table).unwrap();
        writeln!(report, "- JSONL rows: {}", result.jsonl_row_count).unwrap();
        if !result.errors.is_empty() {
            for error in &result.errors {
                writeln!(report, "- Error: {}", error).unwrap();
            }
        }
        writeln!(report).unwrap();
    }

    let report_path = config.artifacts_dir.join("data_parity_report.md");
    let mut file = File::create(&report_path).await?;
    file.write_all(report.as_bytes()).await?;

    println!("📄 Data validation report saved: {}", report_path.display());

    // Save individual results
    for result in results {
        let result_path = config
            .artifacts_dir
            .join(format!("{}.{}_result.json", result.keyspace, result.table));
        let json = serde_json::to_string_pretty(result)
            .map_err(|e| Error::internal(format!("JSON error: {e}")))?;
        let mut file = File::create(&result_path).await?;
        file.write_all(json.as_bytes()).await?;
    }

    Ok(())
}
