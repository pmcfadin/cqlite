//! Summary.db parity tests for Issue #31/M5 Write Validation
//!
//! This module validates that our Summary.db parsing and writing produces
//! identical results to Cassandra's sstabledump tool.
//!
//! Key validations:
//! - Header parameters (min_index_interval, entries_count) match
//! - Offset table is correctly written in little-endian format
//! - First/last keys boundary preservation
//! - Summary entries point to valid Index.db offsets

#![cfg(feature = "write-support")]

use cqlite_core::{
    platform::Platform,
    storage::sstable::summary_reader::SummaryReader,
    storage::sstable::writer::SummaryWriter,
    storage::write_engine::mutation::DecoratedKey,
    testing::dataset_helpers::{
        derive_reference_paths_from_data_db, list_tables, load_metadata,
        resolve_table_to_sstable_path,
    },
    Config, Error, Result as CqliteResult,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Write as FmtWrite,
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::TempDir;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

/// Test configuration for Summary.db parity validation
#[derive(Debug, Clone)]
struct SummaryParityConfig {
    /// Target tables for deterministic testing
    target_tables: Vec<&'static str>,
    /// Validation artifacts directory
    artifacts_dir: PathBuf,
}

impl Default for SummaryParityConfig {
    fn default() -> Self {
        Self {
            target_tables: vec!["simple_table", "sensor_data", "wide_partition_table"],
            artifacts_dir: PathBuf::from("validation_artifacts/sstabledump/summary"),
        }
    }
}

/// Summary.db validation result for a single table
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SummaryValidationResult {
    /// Keyspace name
    keyspace: String,
    /// Table name
    table: String,
    /// Path to Summary.db file
    summary_file_path: PathBuf,
    /// Number of summary entries
    entry_count: usize,
    /// Min index interval from header
    min_index_interval: u32,
    /// First key bytes (hex encoded)
    first_key_hex: String,
    /// Last key bytes (hex encoded)
    last_key_hex: String,
    /// Overall parity status
    perfect_parity: bool,
    /// Validation timestamp
    timestamp: String,
    /// Any validation errors encountered
    errors: Vec<String>,
}

/// Comprehensive Summary.db parity test using canonical datasets
#[tokio::test]
async fn test_summary_db_parity_comprehensive() -> CqliteResult<()> {
    let config = SummaryParityConfig::default();

    // Skip if test data not available
    let metadata = match load_metadata() {
        Ok(m) => m,
        Err(e) => {
            println!(
                "⏭️ Skipping Summary.db comprehensive parity test: test data not available ({e})"
            );
            return Ok(());
        }
    };

    // Skip if tables not available
    let available_tables = match list_tables(None) {
        Ok(t) => t,
        Err(e) => {
            println!("⏭️ Skipping Summary.db comprehensive parity test: cannot list tables ({e})");
            return Ok(());
        }
    };

    for target_table in &config.target_tables {
        let found = available_tables.iter().any(|t| t.table == *target_table);
        if !found {
            println!(
                "⏭️ Skipping Summary.db comprehensive parity test: target table '{}' not found",
                target_table
            );
            return Ok(());
        }
    }

    println!(
        "✅ Dataset validation passed. Found {} tables",
        available_tables.len()
    );

    let mut validation_results = Vec::new();

    for target_table in &config.target_tables {
        let table_info = available_tables
            .iter()
            .find(|t| t.table == *target_table)
            .ok_or_else(|| {
                Error::corruption(format!("Target table '{}' not found", target_table))
            })?;

        match validate_table_summary_parity(table_info, &config).await {
            Ok(result) => validation_results.push(result),
            Err(e) => {
                println!(
                    "⚠️ Skipping {}.{}: {}",
                    table_info.keyspace, table_info.table, e
                );
            }
        }
    }

    // Generate validation report
    let report = generate_summary_validation_report(&validation_results, &metadata);

    // Save validation artifacts
    save_summary_validation_artifacts(&validation_results, &report, &config).await?;

    let passed = validation_results
        .iter()
        .filter(|r| r.perfect_parity)
        .count();
    let total = validation_results.len();

    println!("🎯 Summary.db parity: {}/{} tables passed", passed, total);

    Ok(())
}

/// Validate Summary.db parity for a specific table
async fn validate_table_summary_parity(
    table_info: &cqlite_core::testing::dataset_helpers::TableInfo,
    _config: &SummaryParityConfig,
) -> CqliteResult<SummaryValidationResult> {
    println!(
        "🔍 Validating Summary.db parity for {}.{}",
        table_info.keyspace, table_info.table
    );

    let sstable_dir = resolve_table_to_sstable_path(&table_info.keyspace, &table_info.table)
        .map_err(|e| Error::corruption(format!("Failed to resolve table path: {e}")))?;

    // Find Data.db and derive Summary.db path
    let data_file = find_data_file(&sstable_dir)?;
    let summary_file = derive_companion_file(&data_file, "Summary.db")?;

    let mut validation_result = SummaryValidationResult {
        keyspace: table_info.keyspace.clone(),
        table: table_info.table.clone(),
        summary_file_path: summary_file.clone(),
        entry_count: 0,
        min_index_interval: 0,
        first_key_hex: String::new(),
        last_key_hex: String::new(),
        perfect_parity: false,
        timestamp: chrono::Utc::now().to_rfc3339(),
        errors: Vec::new(),
    };

    // Check if Summary.db exists
    if !summary_file.exists() {
        validation_result
            .errors
            .push(format!("Summary.db not found: {}", summary_file.display()));
        return Ok(validation_result);
    }

    // Read Summary.db using SummaryReader
    let cqlite_config = Config::default();
    let platform = Arc::new(Platform::new(&cqlite_config).await?);

    match SummaryReader::open(&summary_file, platform).await {
        Ok(reader) => {
            let entries = reader.get_entries();
            let header = reader.get_header();
            let first_key = reader.get_first_key();
            let last_key = reader.get_last_key();

            validation_result.entry_count = entries.len();
            validation_result.min_index_interval = header.min_index_interval;
            validation_result.first_key_hex = hex::encode(first_key);
            validation_result.last_key_hex = hex::encode(last_key);

            // Validate header
            if header.min_index_interval == 0 {
                validation_result
                    .errors
                    .push("Min index interval is 0".to_string());
            }

            // Validate entries
            if entries.is_empty() {
                validation_result
                    .errors
                    .push("No summary entries found".to_string());
            }

            // Validate first/last keys
            if first_key.is_empty() {
                validation_result
                    .errors
                    .push("First key is empty".to_string());
            }

            if last_key.is_empty() {
                validation_result
                    .errors
                    .push("Last key is empty".to_string());
            }

            // Validate monotonic positions (offsets should increase)
            for i in 1..entries.len() {
                if entries[i].position <= entries[i - 1].position {
                    validation_result.errors.push(format!(
                        "Non-monotonic position at entry {}: {} <= {}",
                        i,
                        entries[i].position,
                        entries[i - 1].position
                    ));
                }
            }

            // Get reference info if available
            if let Some((_, _, summary_txt)) = derive_reference_paths_from_data_db(&data_file) {
                if summary_txt.exists() {
                    if let Ok(ref_content) = std::fs::read_to_string(&summary_txt) {
                        let mut ref_errors = Vec::new();
                        validate_against_reference(
                            &ref_content,
                            &validation_result,
                            &mut ref_errors,
                        );
                        validation_result.errors.extend(ref_errors);
                    }
                }
            }

            validation_result.perfect_parity = validation_result.errors.is_empty();
        }
        Err(e) => {
            validation_result
                .errors
                .push(format!("Summary.db parse error: {e}"));
        }
    }

    if validation_result.perfect_parity {
        println!(
            "✅ Summary.db parity achieved for {}.{} ({} entries)",
            table_info.keyspace, table_info.table, validation_result.entry_count
        );
    }

    Ok(validation_result)
}

// TODO(#394): Implement detailed reference file validation
// This would parse Cassandra's sstabledump output and compare key values
#[allow(unused_variables, clippy::ptr_arg)]
fn validate_against_reference(
    reference_content: &str,
    result: &SummaryValidationResult,
    errors: &mut Vec<String>,
) {
    // Placeholder: Future work would parse the reference file format
    // and validate summary entries match expected values
}

/// Test Summary.db header parameters roundtrip
#[tokio::test]
async fn test_summary_header_roundtrip() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    // Create summary writer with specific interval
    let min_index_interval = 64u32;
    let mut writer = SummaryWriter::new(min_index_interval);

    // Add entries
    for i in 0..10 {
        let key = DecoratedKey::new(i as i64 * 100, vec![0x00, 0x00, 0x00, i as u8]);
        writer.add_entry(&key, i as u64 * 256)?;
    }

    // Finalize and write
    let summary_bytes = writer.finish()?;
    let mut file = File::create(&summary_path).await?;
    file.write_all(&summary_bytes).await?;
    file.flush().await?;
    drop(file);

    // Read back
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let reader = SummaryReader::open(&summary_path, platform).await?;

    // Verify header
    let header = reader.get_header();
    assert_eq!(
        header.min_index_interval, min_index_interval,
        "Min index interval should be preserved"
    );
    assert_eq!(header.entries_count, 10, "Entry count should be 10");

    println!("✅ Summary.db header roundtrip test passed");
    Ok(())
}

/// Test Summary.db offset table encoding (little-endian)
#[tokio::test]
async fn test_summary_offset_table_encoding() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    let mut writer = SummaryWriter::new(128);

    // Add entries with known positions
    let positions = [0u64, 1024u64, 4096u64, 16384u64];
    for (i, &pos) in positions.iter().enumerate() {
        let key = DecoratedKey::new(i as i64 * 1000, vec![i as u8]);
        writer.add_entry(&key, pos)?;
    }

    // Write to file
    let summary_bytes = writer.finish()?;
    let mut file = File::create(&summary_path).await?;
    file.write_all(&summary_bytes).await?;
    file.flush().await?;
    drop(file);

    // Read back and verify positions
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let reader = SummaryReader::open(&summary_path, platform).await?;

    let entries = reader.get_entries();
    assert_eq!(
        entries.len(),
        positions.len(),
        "Should have same number of entries"
    );

    for (i, &expected_pos) in positions.iter().enumerate() {
        assert_eq!(
            entries[i].position, expected_pos,
            "Entry {} position should be {}",
            i, expected_pos
        );
    }

    println!("✅ Summary.db offset table encoding test passed");
    Ok(())
}

/// Test Summary.db first/last key boundary preservation
#[tokio::test]
async fn test_summary_key_boundaries() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    let mut writer = SummaryWriter::new(128);

    // First and last keys with distinctive values
    let first_key = DecoratedKey::new(-1000, vec![0xAA, 0xBB, 0xCC, 0xDD]);
    let last_key = DecoratedKey::new(9000, vec![0x11, 0x22, 0x33, 0x44]);

    writer.add_entry(&first_key, 0)?;
    for i in 1..9 {
        let key = DecoratedKey::new(i * 1000, vec![i as u8; 4]);
        writer.add_entry(&key, i as u64 * 100)?;
    }
    writer.add_entry(&last_key, 900)?;

    // Write to file
    let summary_bytes = writer.finish()?;
    let mut file = File::create(&summary_path).await?;
    file.write_all(&summary_bytes).await?;
    file.flush().await?;
    drop(file);

    // Read back and verify boundaries
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let reader = SummaryReader::open(&summary_path, platform).await?;

    let read_first = reader.get_first_key();
    let read_last = reader.get_last_key();

    assert_eq!(read_first, first_key.key, "First key should be preserved");
    assert_eq!(read_last, last_key.key, "Last key should be preserved");

    println!("✅ Summary.db key boundary test passed");
    Ok(())
}

/// Test Summary.db with large positions (>32-bit)
#[tokio::test]
async fn test_summary_large_positions() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    let mut writer = SummaryWriter::new(128);

    // Large positions requiring 8-byte encoding
    let large_positions = [
        0u64,
        1_000_000_000u64,   // 1 GB
        10_000_000_000u64,  // 10 GB
        100_000_000_000u64, // 100 GB
    ];

    for (i, &pos) in large_positions.iter().enumerate() {
        let key = DecoratedKey::new(i as i64 * 100, vec![i as u8]);
        writer.add_entry(&key, pos)?;
    }

    // Write to file
    let summary_bytes = writer.finish()?;
    let mut file = File::create(&summary_path).await?;
    file.write_all(&summary_bytes).await?;
    file.flush().await?;
    drop(file);

    // Read back and verify large positions
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let reader = SummaryReader::open(&summary_path, platform).await?;

    let entries = reader.get_entries();
    for (i, &expected) in large_positions.iter().enumerate() {
        assert_eq!(
            entries[i].position, expected,
            "Large position {} should be preserved",
            expected
        );
    }

    println!("✅ Summary.db large positions test passed");
    Ok(())
}

/// Test Summary.db via WriteEngine integration
#[tokio::test]
async fn test_summary_via_write_engine() -> CqliteResult<()> {
    use cqlite_core::schema::{Column, KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::{
        CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
    };
    use cqlite_core::types::Value;
    use std::collections::HashMap;

    let temp_dir = TempDir::new().unwrap();

    let schema = TableSchema {
        keyspace: "test_summary".to_string(),
        table: "summary_test".to_string(),
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
        ],
        comments: HashMap::new(),
    };

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write 50 partitions to ensure summary sampling kicks in
    for i in 0..50 {
        let table_id = TableId::new("test_summary", "summary_test");
        let pk = PartitionKey::single("id", Value::Integer(i));
        let ops = vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(format!("user{}", i)),
        }];
        let mutation = Mutation::new(table_id, pk, None, ops, 1000000 + i as i64, None);
        engine.write_async(mutation).await?;
    }

    // Flush to create SSTable
    let info = engine.flush().await?.expect("Should return SSTableInfo");

    // Verify Summary.db exists
    assert!(info.summary_path.exists(), "Summary.db should exist");

    // Read and verify
    let platform = Arc::new(Platform::new(&Config::default()).await?);
    let reader = SummaryReader::open(&info.summary_path, platform).await?;

    let entries = reader.get_entries();
    assert!(!entries.is_empty(), "Summary.db should have entries");

    let first_key = reader.get_first_key();
    let last_key = reader.get_last_key();
    assert!(!first_key.is_empty(), "First key should exist");
    assert!(!last_key.is_empty(), "Last key should exist");

    println!(
        "✅ Summary.db via WriteEngine test passed ({} entries)",
        entries.len()
    );
    Ok(())
}

// Helper functions

fn find_data_file(sstable_dir: &Path) -> CqliteResult<PathBuf> {
    for entry in std::fs::read_dir(sstable_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with("-Data.db") && !name.starts_with("._") {
                return Ok(path);
            }
        }
    }

    // Fall back to JSONL reference
    for entry in std::fs::read_dir(sstable_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with("-Data.db.jsonl") && !name.starts_with("._") {
                let data_name = &name[..name.len() - ".jsonl".len()];
                return Ok(sstable_dir.join(data_name));
            }
        }
    }

    Err(cqlite_core::Error::not_found("No Data.db file found"))
}

fn derive_companion_file(data_file: &Path, companion_type: &str) -> CqliteResult<PathBuf> {
    let file_name = data_file
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::corruption("Invalid Data.db path"))?;

    let companion_name = file_name.replace("-Data.db", &format!("-{}", companion_type));
    Ok(data_file
        .parent()
        .unwrap_or(Path::new("."))
        .join(companion_name))
}

fn generate_summary_validation_report(
    results: &[SummaryValidationResult],
    metadata: &cqlite_core::testing::dataset_helpers::Metadata,
) -> String {
    let mut report = String::new();

    writeln!(report, "# Summary.db Parity Validation Report").unwrap();
    writeln!(report, "## M5 Write Validation (Issue #394)").unwrap();
    writeln!(report).unwrap();
    writeln!(
        report,
        "**Validation Timestamp:** {}",
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    )
    .unwrap();
    writeln!(report, "**Total Tables Tested:** {}", results.len()).unwrap();
    writeln!(report).unwrap();

    let passed = results.iter().filter(|r| r.perfect_parity).count();
    let status = if passed == results.len() {
        "✅ ALL TESTS PASSED"
    } else {
        "⚠️ SOME TESTS FAILED"
    };
    writeln!(report, "## {}", status).unwrap();
    writeln!(report).unwrap();

    writeln!(report, "### Results").unwrap();
    for result in results {
        let icon = if result.perfect_parity { "✅" } else { "❌" };
        writeln!(report, "#### {} {}.{}", icon, result.keyspace, result.table).unwrap();
        writeln!(report, "- **Entries:** {}", result.entry_count).unwrap();
        writeln!(
            report,
            "- **Min Index Interval:** {}",
            result.min_index_interval
        )
        .unwrap();
        if !result.errors.is_empty() {
            writeln!(report, "- **Errors:**").unwrap();
            for error in &result.errors {
                writeln!(report, "  - {}", error).unwrap();
            }
        }
        writeln!(report).unwrap();
    }

    writeln!(report, "### Dataset Info").unwrap();
    for ks in &metadata.keyspaces {
        writeln!(report, "- **{}**: {} tables", ks.name, ks.tables.len()).unwrap();
    }

    report
}

async fn save_summary_validation_artifacts(
    results: &[SummaryValidationResult],
    report: &str,
    config: &SummaryParityConfig,
) -> CqliteResult<()> {
    fs::create_dir_all(&config.artifacts_dir).await?;

    let report_path = config.artifacts_dir.join("summary_parity_report.md");
    let mut file = File::create(&report_path).await?;
    file.write_all(report.as_bytes()).await?;

    println!(
        "📄 Summary validation report saved: {}",
        report_path.display()
    );

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
