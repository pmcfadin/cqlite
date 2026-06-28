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

#[path = "parity_support/mod.rs"]
mod parity_support;
use parity_support::{parity_datasets_required, scenario, ParityFailure};

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

    // Skip if test data not available (fail-closed in CI).
    let metadata = match load_metadata() {
        Ok(m) => m,
        Err(e) => {
            if parity_datasets_required() {
                ParityFailure::new(scenario::SUMMARY_DB_BIG)
                    .cassandra_source("IndexSummaryTest / SSTableReaderTest (Summary.db)")
                    .components(["Summary.db", "Data.db", "Index.db"])
                    .repro(
                        "bash test-data/scripts/fetch-datasets.sh && \
                         CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                         --features write-support --test sstabledump_parity_summary \
                         test_summary_db_parity_comprehensive -- --nocapture",
                    )
                    .detail(format!(
                        "CQLITE_PARITY_REQUIRE_DATASETS=1 but datasets metadata could not be \
                         loaded ({e}) — required parity gate must not skip when datasets are mandated"
                    ))
                    .panic();
            }
            println!(
                "⏭️ Skipping Summary.db comprehensive parity test: test data not available ({e})"
            );
            return Ok(());
        }
    };

    // Skip if tables not available (fail-closed in CI).
    let available_tables = match list_tables(None) {
        Ok(t) => t,
        Err(e) => {
            if parity_datasets_required() {
                ParityFailure::new(scenario::SUMMARY_DB_BIG)
                    .cassandra_source("IndexSummaryTest / SSTableReaderTest (Summary.db)")
                    .components(["Summary.db", "Data.db", "Index.db"])
                    .repro(
                        "bash test-data/scripts/fetch-datasets.sh && \
                         CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                         --features write-support --test sstabledump_parity_summary \
                         test_summary_db_parity_comprehensive -- --nocapture",
                    )
                    .detail(format!(
                        "CQLITE_PARITY_REQUIRE_DATASETS=1 but tables could not be listed ({e}) — \
                         required parity gate must not skip when datasets are mandated"
                    ))
                    .panic();
            }
            println!("⏭️ Skipping Summary.db comprehensive parity test: cannot list tables ({e})");
            return Ok(());
        }
    };

    for target_table in &config.target_tables {
        let found = available_tables.iter().any(|t| t.table == *target_table);
        if !found {
            if parity_datasets_required() {
                ParityFailure::new(scenario::SUMMARY_DB_BIG)
                    .cassandra_source("IndexSummaryTest / SSTableReaderTest (Summary.db)")
                    .components(["Summary.db", "Data.db", "Index.db"])
                    .repro(
                        "bash test-data/scripts/fetch-datasets.sh && \
                         CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                         --features write-support --test sstabledump_parity_summary \
                         test_summary_db_parity_comprehensive -- --nocapture",
                    )
                    .detail(format!(
                        "CQLITE_PARITY_REQUIRE_DATASETS=1 but target table '{}' was not found in \
                         the datasets — required parity gate must not skip when datasets are mandated",
                        target_table
                    ))
                    .panic();
            }
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

        let result = validate_table_summary_parity(table_info, &config).await?;
        validation_results.push(result);
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

    assert_eq!(
        total,
        config.target_tables.len(),
        "Summary.db parity validated {} tables, expected {}",
        total,
        config.target_tables.len()
    );
    assert_eq!(
        passed, total,
        "Summary.db parity failures detected; see validation artifacts for details"
    );

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

/// Validate the live `SummaryValidationResult` against a Cassandra-emitted
/// `*-Summary.db.txt` reference dump, when one is published.
///
/// The reference dumps published in this repo are sstabledump-style key/value
/// text. We extract the fields Cassandra records (min_index_interval, the
/// entry count, and the first/last decorated keys) and require byte/value
/// equality with what CQLite parsed. Any field present in the reference that
/// disagrees pushes an explicit error (fail-closed); a missing reference field
/// is tolerated only when the reference simply does not record it.
fn validate_against_reference(
    reference_content: &str,
    result: &SummaryValidationResult,
    errors: &mut Vec<String>,
) {
    for raw_line in reference_content.lines() {
        let line = raw_line.trim();
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "min_index_interval" | "minindexinterval" => {
                if let Ok(expected) = value.parse::<u32>() {
                    if expected != result.min_index_interval {
                        errors.push(format!(
                            "min_index_interval mismatch vs reference: expected {expected}, got {}",
                            result.min_index_interval
                        ));
                    }
                }
            }
            "entries" | "entries_count" | "entrycount" => {
                if let Ok(expected) = value.parse::<usize>() {
                    if expected != result.entry_count {
                        errors.push(format!(
                            "entry_count mismatch vs reference: expected {expected}, got {}",
                            result.entry_count
                        ));
                    }
                }
            }
            "first_key" | "firstkey" => {
                let expected = value.trim_start_matches("0x").to_ascii_lowercase();
                if !expected.is_empty() && expected != result.first_key_hex {
                    errors.push(format!(
                        "first_key mismatch vs reference: expected {expected}, got {}",
                        result.first_key_hex
                    ));
                }
            }
            "last_key" | "lastkey" => {
                let expected = value.trim_start_matches("0x").to_ascii_lowercase();
                if !expected.is_empty() && expected != result.last_key_hex {
                    errors.push(format!(
                        "last_key mismatch vs reference: expected {expected}, got {}",
                        result.last_key_hex
                    ));
                }
            }
            _ => {}
        }
    }
}

/// Test Summary.db header parameters roundtrip
#[tokio::test]
async fn test_summary_header_roundtrip() -> CqliteResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("nb-1-big-Summary.db");

    // Create summary writer with specific interval
    let min_index_interval = 64u32;
    let mut writer = SummaryWriter::new(min_index_interval);

    // Add entries (note_partition must be called for every partition so that
    // first_key and last_key are tracked; add_entry is only called at sampling
    // boundaries but note_partition covers all partitions).
    for i in 0..10 {
        let key = DecoratedKey::new(i as i64 * 100, vec![0x00, 0x00, 0x00, i as u8]);
        writer.note_partition(&key);
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

    // Add entries with known positions (note_partition required before add_entry)
    let positions = [0u64, 1024u64, 4096u64, 16384u64];
    for (i, &pos) in positions.iter().enumerate() {
        let key = DecoratedKey::new(i as i64 * 1000, vec![i as u8]);
        writer.note_partition(&key);
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

    // First and last keys with distinctive values (note_partition required for
    // all partitions so first_key/last_key cover the full range)
    let first_key = DecoratedKey::new(-1000, vec![0xAA, 0xBB, 0xCC, 0xDD]);
    let last_key = DecoratedKey::new(9000, vec![0x11, 0x22, 0x33, 0x44]);

    writer.note_partition(&first_key);
    writer.add_entry(&first_key, 0)?;
    for i in 1..9 {
        let key = DecoratedKey::new(i * 1000, vec![i as u8; 4]);
        writer.note_partition(&key);
        writer.add_entry(&key, i as u64 * 100)?;
    }
    writer.note_partition(&last_key);
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

    // Large positions requiring 8-byte encoding (note_partition required)
    let large_positions = [
        0u64,
        1_000_000_000u64,   // 1 GB
        10_000_000_000u64,  // 10 GB
        100_000_000_000u64, // 100 GB
    ];

    for (i, &pos) in large_positions.iter().enumerate() {
        let key = DecoratedKey::new(i as i64 * 100, vec![i as u8]);
        writer.note_partition(&key);
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
        dropped_columns: HashMap::new(),
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
    assert!(
        info.summary_path.as_ref().unwrap().exists(),
        "Summary.db should exist"
    );

    // Read and verify
    let platform = Arc::new(Platform::new(&Config::default()).await?);
    let reader = SummaryReader::open(info.summary_path.as_ref().unwrap(), platform).await?;

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

// ============================================================================
// Strict Summary.db byte + offset parity (Epic #968 / issue #984)
// ============================================================================
//
// The tests above prove read/round-trip behaviour. The module below proves
// *byte-for-byte* parity against the on-disk `Summary.db` images Cassandra 5.0
// wrote, with no parse-success-only or shape-only checks:
//
//   * Header fields are decoded straight from the 24 raw header bytes and must
//     equal what `SummaryReader` exposes (min_index_interval, entries_count,
//     summary_entries_size, sampling_level, size_at_full_sampling).
//   * The little-endian offset table is decoded independently; its length must
//     equal entries_count, offsets must be strictly increasing and bounded by
//     summary_entries_size, and the first offset must equal the offset-table
//     byte length (the Cassandra absolute-offset layout).
//   * Each entry's key bytes are reconstructed from the offset table and must
//     byte-match `SummaryReader`'s entries; entries must be ordered by ascending
//     offset and ascending (little-endian) Index.db position. The on-disk
//     position is little-endian (proven against Index.db); as of issue #1054 the
//     production `SummaryReader` decodes it little-endian too, so the reader's
//     returned position is asserted byte-for-byte against the LE on-disk value.
//   * The trailing length-prefixed first/last decorated keys are decoded and
//     byte-compared to `SummaryReader`, and the first summary sample's key must
//     equal the SSTable's first key (Cassandra always samples partition 0).
//   * Every entry position is checked as a valid `Index.db` byte offset
//     (in-bounds, with the first sample at offset 0).
//   * Truncated / malformed / offset-inconsistent images are fed to the parser
//     and must produce explicit `Err`s — never a panic and never a silent OK.
//   * BTI (`da`) SSTables are classified: they MUST NOT carry a `Summary.db`
//     (the trie `Partitions.db` replaces it), proving format separation.
//
// Strict lane fails closed. Because the binary `*.db` images are not committed
// to git (only JSONL/TXT references are), each fixture is *skipped only when its
// own Data.db is absent*; when present, any discrepancy turns the lane red.

#[cfg(feature = "write-support")]
mod strict {
    use super::*;
    use cqlite_core::storage::sstable::directory::{parse_toc_file_detailed, SSTableComponent};
    use cqlite_core::storage::sstable::summary_reader::SummaryHeader;
    use cqlite_core::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};

    /// Raw, independent re-decode of a `Summary.db` image used to cross-check the
    /// production `SummaryReader`. Implemented from first principles (no shared
    /// parser code) so a bug in either side is caught by the byte comparison.
    struct RawSummary {
        header: SummaryHeader,
        /// Offsets exactly as stored (little-endian u32), unmodified.
        raw_offsets: Vec<u32>,
        /// Per-entry decoded data reconstructed from the offset table.
        entries: Vec<RawEntry>,
        first_key: Vec<u8>,
        last_key: Vec<u8>,
    }

    /// One summary entry decoded from raw bytes. Cassandra stores the trailing
    /// Index.db position as a **little-endian** u64 (verified byte-for-byte:
    /// the LE value lands exactly on the matching `Index.db` partition entry,
    /// while the big-endian interpretation produces an out-of-range offset).
    /// The authoritative LE offset is retained and asserted against `Index.db`;
    /// as of issue #1054 the production `SummaryReader` returns this field
    /// little-endian too, so the entry-parity assertion checks the reader's
    /// returned position byte-for-byte against this value.
    struct RawEntry {
        key: Vec<u8>,
        /// Authoritative Index.db offset (little-endian, on-disk truth).
        position_le: u64,
    }

    /// Decode a `Summary.db` buffer from scratch. Returns an explicit error on
    /// any truncation or offset inconsistency (used both for parity and for the
    /// malformation tests below).
    fn decode_raw_summary(buf: &[u8]) -> std::result::Result<RawSummary, String> {
        const HEADER_LEN: usize = 24;
        if buf.len() < HEADER_LEN {
            return Err(format!(
                "truncated header: {} bytes < {HEADER_LEN}",
                buf.len()
            ));
        }
        let be_u32 = |o: usize| u32::from_be_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]]);
        let be_u64 = |o: usize| {
            u64::from_be_bytes([
                buf[o],
                buf[o + 1],
                buf[o + 2],
                buf[o + 3],
                buf[o + 4],
                buf[o + 5],
                buf[o + 6],
                buf[o + 7],
            ])
        };
        let header = SummaryHeader {
            min_index_interval: be_u32(0),
            entries_count: be_u32(4),
            summary_entries_size: be_u64(8),
            sampling_level: be_u32(16),
            size_at_full_sampling: be_u32(20),
        };

        let entries_count = header.entries_count as usize;
        let offset_table_len = entries_count
            .checked_mul(4)
            .ok_or_else(|| "offset table size overflow".to_string())?;
        let summary_block = header.summary_entries_size as usize;
        if summary_block < offset_table_len {
            return Err(format!(
                "summary_entries_size {summary_block} < offset table length {offset_table_len}"
            ));
        }
        let summary_start = HEADER_LEN;
        let summary_end = summary_start
            .checked_add(summary_block)
            .ok_or_else(|| "summary block end overflow".to_string())?;
        if buf.len() < summary_end {
            return Err(format!(
                "truncated summary block: need {summary_end} bytes, have {}",
                buf.len()
            ));
        }

        // Little-endian offset table.
        let mut raw_offsets = Vec::with_capacity(entries_count);
        for i in 0..entries_count {
            let o = summary_start + i * 4;
            raw_offsets.push(u32::from_le_bytes([
                buf[o],
                buf[o + 1],
                buf[o + 2],
                buf[o + 3],
            ]));
        }

        // Cassandra stores absolute offsets into the summary block (offset 0 ==
        // start of the offset table). Normalize to entry-data-relative offsets.
        let entry_data = &buf[summary_start + offset_table_len..summary_end];
        let mut norm: Vec<usize> = Vec::with_capacity(entries_count);
        for (i, &off) in raw_offsets.iter().enumerate() {
            let off = off as usize;
            if off < offset_table_len {
                return Err(format!(
                    "offset[{i}] = {off} falls inside the offset table (len {offset_table_len})"
                ));
            }
            if off > summary_block {
                return Err(format!(
                    "offset[{i}] = {off} exceeds summary block {summary_block}"
                ));
            }
            norm.push(off - offset_table_len);
        }

        let mut entries = Vec::with_capacity(entries_count);
        for i in 0..entries_count {
            let start = norm[i];
            let end = if i + 1 < entries_count {
                norm[i + 1]
            } else {
                entry_data.len()
            };
            if start >= end {
                return Err(format!("offset[{i}] start {start} >= end {end}"));
            }
            if end > entry_data.len() {
                return Err(format!(
                    "offset[{i}] end {end} exceeds entry data len {}",
                    entry_data.len()
                ));
            }
            let slice = &entry_data[start..end];
            if slice.len() < 8 {
                return Err(format!("entry {i} too small for 8-byte position"));
            }
            let key_len = slice.len() - 8;
            let key = slice[..key_len].to_vec();
            let pos_bytes: [u8; 8] = [
                slice[key_len],
                slice[key_len + 1],
                slice[key_len + 2],
                slice[key_len + 3],
                slice[key_len + 4],
                slice[key_len + 5],
                slice[key_len + 6],
                slice[key_len + 7],
            ];
            entries.push(RawEntry {
                key,
                position_le: u64::from_le_bytes(pos_bytes),
            });
        }

        // Trailing first/last keys: be_u32 length prefix + bytes.
        let read_key = |start: usize| -> std::result::Result<(Vec<u8>, usize), String> {
            if buf.len() < start + 4 {
                return Err("truncated key length prefix".to_string());
            }
            let len =
                u32::from_be_bytes([buf[start], buf[start + 1], buf[start + 2], buf[start + 3]])
                    as usize;
            let key_start = start + 4;
            let key_end = key_start
                .checked_add(len)
                .ok_or_else(|| "key length overflow".to_string())?;
            if buf.len() < key_end {
                return Err(format!(
                    "truncated key body: need {key_end} bytes, have {}",
                    buf.len()
                ));
            }
            Ok((buf[key_start..key_end].to_vec(), key_end))
        };
        let (first_key, after_first) = read_key(summary_end)?;
        let (last_key, _after_last) = read_key(after_first)?;

        Ok(RawSummary {
            header,
            raw_offsets,
            entries,
            first_key,
            last_key,
        })
    }

    /// Datasets root (`CQLITE_DATASETS_ROOT` override, else workspace tree).
    fn datasets_sstables_root() -> PathBuf {
        let root = std::env::var("CQLITE_DATASETS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .map(|ws| ws.join("test-data/datasets"))
                    .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
            });
        root.join("sstables")
    }

    fn collect_by_suffix(dir: &Path, suffix: &str, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("._") {
                continue;
            }
            if path.is_dir() {
                collect_by_suffix(&path, suffix, out);
            } else if name.ends_with(suffix) {
                out.push(path);
            }
        }
    }

    /// Strict byte + offset parity for every committed BIG `Summary.db` whose
    /// sibling `Data.db` is present. Fails closed on any discrepancy.
    #[tokio::test]
    async fn test_summary_db_strict_byte_parity() -> CqliteResult<()> {
        let root = datasets_sstables_root();
        let mut summaries = Vec::new();
        collect_by_suffix(&root, "-Summary.db", &mut summaries);
        summaries.sort();

        if summaries.is_empty() {
            // Binary fixtures absent in this checkout (only JSONL refs committed).
            // Skip-on-absence per project doctrine; do NOT pass silently with 0 work.
            if parity_datasets_required() {
                ParityFailure::new(scenario::SUMMARY_DB_BIG)
                    .cassandra_source("IndexSummaryTest (Summary.db byte/offset parity)")
                    .fixture(root.clone())
                    .components(["Summary.db", "Data.db", "Index.db"])
                    .repro(
                        "bash test-data/scripts/fetch-datasets.sh && \
                         CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                         --features write-support --test sstabledump_parity_summary \
                         strict::test_summary_db_strict_byte_parity -- --nocapture",
                    )
                    .detail(
                        "CQLITE_PARITY_REQUIRE_DATASETS=1 but no *-Summary.db images were present \
                         — required parity gate must not skip when datasets are mandated",
                    )
                    .panic();
            }
            eprintln!(
                "skip: no *-Summary.db images under {} (binary fixtures not fetched)",
                root.display()
            );
            return Ok(());
        }

        let platform = Arc::new(Platform::new(&Config::default()).await?);
        let mut validated = 0usize;
        // Count Summary.db images that actually have their sibling Data.db on
        // disk. This distinguishes "binaries unfetched → nothing to validate"
        // (skip) from "fixtures present but none validated" (fail-closed).
        let mut with_data_db = 0usize;

        for summary_path in &summaries {
            let prefix = summary_path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix("-Summary.db"))
                .ok_or_else(|| Error::corruption("bad Summary.db name"))?;
            let dir = summary_path
                .parent()
                .ok_or_else(|| Error::corruption("Summary.db has no parent dir"))?;
            let data_path = dir.join(format!("{prefix}-Data.db"));
            let index_path = dir.join(format!("{prefix}-Index.db"));

            // Skip-on-absence: only validate fixtures whose Data.db is present.
            if !data_path.exists() {
                continue;
            }
            with_data_db += 1;

            // BIG-only: a Summary.db must come from a BIG descriptor.
            let descriptor = SsTableDescriptor::parse(summary_path).map_err(|e| {
                Error::corruption(format!("descriptor parse {}: {e}", summary_path.display()))
            })?;
            assert_eq!(
                descriptor.format,
                SsTableFormat::Big,
                "{}: Summary.db must belong to a BIG SSTable, got {:?}",
                summary_path.display(),
                descriptor.format
            );

            let buf = std::fs::read(summary_path)
                .map_err(|e| Error::corruption(format!("read {}: {e}", summary_path.display())))?;
            let raw = decode_raw_summary(&buf).map_err(|e| {
                Error::corruption(format!(
                    "strict decode of {} failed: {e}",
                    summary_path.display()
                ))
            })?;

            // ---- Header field parity vs SummaryReader ----
            let reader = SummaryReader::open(summary_path, platform.clone()).await?;
            let header = reader.get_header();
            assert_eq!(
                header.min_index_interval,
                raw.header.min_index_interval,
                "{}: min_index_interval",
                summary_path.display()
            );
            assert_eq!(
                header.entries_count,
                raw.header.entries_count,
                "{}: entries_count",
                summary_path.display()
            );
            assert_eq!(
                header.summary_entries_size,
                raw.header.summary_entries_size,
                "{}: summary_entries_size",
                summary_path.display()
            );
            assert_eq!(
                header.sampling_level,
                raw.header.sampling_level,
                "{}: sampling_level",
                summary_path.display()
            );
            assert_eq!(
                header.size_at_full_sampling,
                raw.header.size_at_full_sampling,
                "{}: size_at_full_sampling",
                summary_path.display()
            );

            // Sampling metadata sanity (authoritative, not heuristic).
            assert!(
                header.min_index_interval > 0,
                "{}: min_index_interval must be > 0",
                summary_path.display()
            );
            assert!(
                header.sampling_level > 0 && header.sampling_level <= 128,
                "{}: sampling_level {} out of (0,128]",
                summary_path.display(),
                header.sampling_level
            );

            // ---- Offset table parity ----
            assert_eq!(
                raw.raw_offsets.len(),
                header.entries_count as usize,
                "{}: offset table length != entries_count",
                summary_path.display()
            );
            let offset_table_len = header.entries_count as usize * 4;
            assert_eq!(
                raw.raw_offsets.first().copied(),
                Some(offset_table_len as u32),
                "{}: first offset must equal offset-table byte length (absolute layout)",
                summary_path.display()
            );
            for w in raw.raw_offsets.windows(2) {
                assert!(
                    w[1] > w[0],
                    "{}: offset table not strictly increasing: {} then {}",
                    summary_path.display(),
                    w[0],
                    w[1]
                );
            }
            for (i, &off) in raw.raw_offsets.iter().enumerate() {
                assert!(
                    (off as u64) < header.summary_entries_size,
                    "{}: offset[{i}] = {off} >= summary_entries_size {}",
                    summary_path.display(),
                    header.summary_entries_size
                );
            }

            // ---- Entry byte parity (keys + Index.db positions) ----
            let entries = reader.get_entries();
            assert_eq!(
                entries.len(),
                raw.entries.len(),
                "{}: entry count mismatch reader={} raw={}",
                summary_path.display(),
                entries.len(),
                raw.entries.len()
            );
            for (i, (entry, raw_entry)) in entries.iter().zip(raw.entries.iter()).enumerate() {
                // Key bytes must byte-match between the production reader and the
                // independent raw decode.
                assert_eq!(
                    &entry.partition_key,
                    &raw_entry.key,
                    "{}: entry[{i}] key bytes mismatch",
                    summary_path.display()
                );
                // The on-disk truth is the little-endian position, proven below
                // by resolving it against Index.db. As of issue #1054 the
                // production `SummaryReader` decodes this field little-endian, so
                // its returned position must byte-match the raw LE on-disk value.
                assert_eq!(
                    entry.position,
                    raw_entry.position_le,
                    "{}: entry[{i}] position mismatch reader={} raw_le={} (issue #1054)",
                    summary_path.display(),
                    entry.position,
                    raw_entry.position_le
                );
            }

            // Entry ordering: keys are stored in ascending offset order, so the
            // offset table (already asserted strictly increasing) defines order.
            // The authoritative Index.db positions (little-endian) are also
            // non-decreasing across samples; first sample points at offset 0.
            for w in raw.entries.windows(2) {
                assert!(
                    w[1].position_le >= w[0].position_le,
                    "{}: Index.db positions not monotonic ({} then {})",
                    summary_path.display(),
                    w[0].position_le,
                    w[1].position_le
                );
            }
            assert_eq!(
                raw.entries[0].position_le,
                0,
                "{}: first summary sample must point at Index.db offset 0",
                summary_path.display()
            );

            // ---- First/last key byte parity ----
            assert_eq!(
                reader.get_first_key(),
                raw.first_key.as_slice(),
                "{}: first_key bytes mismatch",
                summary_path.display()
            );
            assert_eq!(
                reader.get_last_key(),
                raw.last_key.as_slice(),
                "{}: last_key bytes mismatch",
                summary_path.display()
            );
            // First sampled entry key is the SSTable's first decorated key.
            assert_eq!(
                &raw.entries[0].key,
                &raw.first_key,
                "{}: first summary entry key must equal SSTable first key",
                summary_path.display()
            );

            // ---- Index.db offset references are valid (authoritative LE) ----
            // Each sampled position must be a real byte offset inside Index.db,
            // and the entry that lives at that offset must carry the exact same
            // partition key the summary recorded — proving the Index.db
            // reference is byte-correct, not merely in-bounds.
            assert!(
                index_path.exists(),
                "{}: BIG SSTable missing sibling Index.db",
                summary_path.display()
            );
            let index_bytes = std::fs::read(&index_path)
                .map_err(|e| Error::corruption(format!("read Index.db: {e}")))?;
            let index_len = index_bytes.len() as u64;
            for (i, raw_entry) in raw.entries.iter().enumerate() {
                assert!(
                    raw_entry.position_le < index_len,
                    "{}: entry[{i}] Index.db position {} >= Index.db size {}",
                    summary_path.display(),
                    raw_entry.position_le,
                    index_len
                );
                // Index.db partition entry = be16 key length + key bytes.
                let off = raw_entry.position_le as usize;
                assert!(
                    off + 2 <= index_bytes.len(),
                    "{}: entry[{i}] Index.db offset {off} has no room for key length",
                    summary_path.display()
                );
                let idx_key_len =
                    u16::from_be_bytes([index_bytes[off], index_bytes[off + 1]]) as usize;
                let key_start = off + 2;
                let key_end = key_start + idx_key_len;
                assert!(
                    key_end <= index_bytes.len(),
                    "{}: entry[{i}] Index.db key at {off} runs past EOF",
                    summary_path.display()
                );
                assert_eq!(
                    &index_bytes[key_start..key_end],
                    raw_entry.key.as_slice(),
                    "{}: entry[{i}] Index.db key at offset {off} does not match summary key",
                    summary_path.display()
                );
            }

            validated += 1;
            println!(
                "strict OK {} ({} entries, mii={}, sampling={}, first_key_len={})",
                summary_path.display(),
                entries.len(),
                header.min_index_interval,
                header.sampling_level,
                raw.first_key.len(),
            );
        }

        if validated == 0 {
            // Distinguish "nothing fetched to validate" (clean skip) from
            // "fixtures present but none validated" (fail-closed).
            if with_data_db == 0 {
                // Summary.db images were discovered, but NONE had a sibling
                // Data.db on disk — the binary fixtures simply were not fetched
                // in this checkout. There is nothing to validate, so skip
                // cleanly; do NOT claim a fail-closed pass.
                if parity_datasets_required() {
                    ParityFailure::new(scenario::SUMMARY_DB_BIG)
                        .cassandra_source("IndexSummaryTest (Summary.db byte/offset parity)")
                        .fixture(root.clone())
                        .components(["Summary.db", "Data.db", "Index.db"])
                        .repro(
                            "bash test-data/scripts/fetch-datasets.sh && \
                             CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                             --features write-support --test sstabledump_parity_summary \
                             strict::test_summary_db_strict_byte_parity -- --nocapture",
                        )
                        .detail(format!(
                            "CQLITE_PARITY_REQUIRE_DATASETS=1 but {} Summary.db image(s) had no \
                             sibling Data.db (binaries unfetched) — required parity gate must not \
                             skip when datasets are mandated",
                            summaries.len()
                        ))
                        .panic();
                }
                eprintln!(
                    "skip: {} Summary.db image(s) found under {} but none had a sibling Data.db \
                     (binary fixtures not fetched)",
                    summaries.len(),
                    root.display()
                );
                return Ok(());
            }
            // Summary.db images WERE discovered WITH their sibling Data.db
            // present, yet none reached `validated += 1`. That is a real
            // regression in the lane (every present fixture should validate),
            // not an absent-binary skip — fail closed.
            panic!(
                "{with_data_db} Summary.db image(s) under {} had a sibling Data.db present but \
                 none were validated — strict parity lane proved nothing",
                root.display()
            );
        }

        println!("strict Summary.db byte parity validated {validated} BIG SSTable(s)");
        Ok(())
    }

    /// Truncated / malformed / offset-inconsistent Summary.db images must
    /// produce explicit errors, never a panic and never a silent success.
    #[test]
    fn test_summary_db_malformation_detection() {
        // A minimal valid single-entry image to mutate.
        // Header(24) + offset table(4) + entry(16-byte key + 8-byte pos) +
        // first key(4+16) + last key(4+16).
        let mut img: Vec<u8> = Vec::new();
        img.extend_from_slice(&128u32.to_be_bytes()); // min_index_interval
        img.extend_from_slice(&1u32.to_be_bytes()); // entries_count
        img.extend_from_slice(&28u64.to_be_bytes()); // summary_entries_size = 4 + 24
        img.extend_from_slice(&128u32.to_be_bytes()); // sampling_level
        img.extend_from_slice(&1u32.to_be_bytes()); // size_at_full_sampling
        img.extend_from_slice(&4u32.to_le_bytes()); // offset[0] = 4 (absolute)
        let key = [0x22u8; 16];
        img.extend_from_slice(&key); // entry key
        img.extend_from_slice(&0u64.to_be_bytes()); // entry position
        img.extend_from_slice(&16u32.to_be_bytes()); // first key len
        img.extend_from_slice(&key);
        img.extend_from_slice(&16u32.to_be_bytes()); // last key len
        img.extend_from_slice(&key);

        // Baseline image decodes cleanly.
        assert!(
            decode_raw_summary(&img).is_ok(),
            "baseline image should decode"
        );

        // 1. Truncated header.
        assert!(
            decode_raw_summary(&img[..10]).is_err(),
            "truncated header must error"
        );

        // 2. Truncated summary block (chop the entry data).
        assert!(
            decode_raw_summary(&img[..30]).is_err(),
            "truncated summary block must error"
        );

        // 3. Truncated trailing keys (drop the last key bytes).
        let no_last_key = &img[..img.len() - 20];
        assert!(
            decode_raw_summary(no_last_key).is_err(),
            "truncated trailing keys must error"
        );

        // 4. Offset pointing inside the offset table (inconsistent).
        let mut bad_off = img.clone();
        bad_off[24] = 0x00; // offset[0] LE -> 0, which is inside the table
        assert!(
            decode_raw_summary(&bad_off).is_err(),
            "offset inside offset table must error"
        );

        // 5. summary_entries_size smaller than the offset table.
        let mut bad_size = img.clone();
        bad_size[8..16].copy_from_slice(&2u64.to_be_bytes());
        assert!(
            decode_raw_summary(&bad_size).is_err(),
            "summary_entries_size < offset table must error"
        );
    }

    /// BTI (`da`) SSTables are classified separately: they MUST NOT carry a
    /// `Summary.db` component (the trie `Partitions.db` replaces it). Proven via
    /// the authoritative `TOC.txt` manifest plus the on-disk component set.
    #[test]
    fn test_bti_summary_discovery_classification() {
        let root = datasets_sstables_root();
        let mut tocs = Vec::new();
        collect_by_suffix(&root, "-TOC.txt", &mut tocs);
        tocs.sort();

        if tocs.is_empty() {
            if parity_datasets_required() {
                ParityFailure::new(scenario::COMPONENT_MANIFEST)
                    .cassandra_source("BTI/BIG TOC manifest classification (Summary.db presence)")
                    .fixture(root.clone())
                    .components(["TOC.txt", "Summary.db", "Partitions.db"])
                    .repro(
                        "bash test-data/scripts/fetch-datasets.sh && \
                         CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test -p cqlite-core \
                         --features write-support --test sstabledump_parity_summary \
                         strict::test_bti_summary_discovery_classification -- --nocapture",
                    )
                    .detail(
                        "CQLITE_PARITY_REQUIRE_DATASETS=1 but no *-TOC.txt fixtures were present — \
                         required parity gate must not skip when datasets are mandated",
                    )
                    .panic();
            }
            eprintln!(
                "skip: no *-TOC.txt fixtures under {} (datasets not present)",
                root.display()
            );
            return;
        }

        let mut big_with_summary = 0usize;
        let mut bti_without_summary = 0usize;

        for toc in &tocs {
            let descriptor = SsTableDescriptor::parse(toc)
                .unwrap_or_else(|e| panic!("descriptor parse {}: {e}", toc.display()));
            let (components, unknown) = parse_toc_file_detailed(toc)
                .unwrap_or_else(|e| panic!("parse {} failed: {e}", toc.display()));
            assert!(
                unknown.is_empty(),
                "{}: unrecognized component(s) {:?}",
                toc.display(),
                unknown
            );
            let has_summary = components.contains(&SSTableComponent::Summary);
            let has_index = components.contains(&SSTableComponent::Index);
            let has_partitions = components.contains(&SSTableComponent::Partitions);

            match descriptor.format {
                SsTableFormat::Big => {
                    assert!(
                        has_summary,
                        "{}: BIG SSTable must declare Summary.db",
                        toc.display()
                    );
                    assert!(
                        has_index,
                        "{}: BIG SSTable must declare Index.db",
                        toc.display()
                    );
                    assert!(
                        !has_partitions,
                        "{}: BIG SSTable must not declare BTI Partitions.db",
                        toc.display()
                    );
                    big_with_summary += 1;
                }
                SsTableFormat::Bti => {
                    assert!(
                        !has_summary,
                        "{}: BTI SSTable must NOT declare Summary.db (trie Partitions.db replaces it)",
                        toc.display()
                    );
                    assert!(
                        has_partitions,
                        "{}: BTI SSTable must declare Partitions.db",
                        toc.display()
                    );
                    // The Summary.db image must also be physically absent.
                    let dir = toc.parent().expect("TOC has parent");
                    let prefix = toc
                        .file_name()
                        .and_then(|n| n.to_str())
                        .and_then(|n| n.strip_suffix("-TOC.txt"))
                        .expect("bad TOC name");
                    let summary_image = dir.join(format!("{prefix}-Summary.db"));
                    assert!(
                        !summary_image.exists(),
                        "{}: BTI SSTable has an unexpected Summary.db image",
                        summary_image.display()
                    );
                    bti_without_summary += 1;
                }
            }
        }

        println!(
            "BTI/BIG summary classification: {big_with_summary} BIG(+Summary), \
             {bti_without_summary} BTI(no Summary)"
        );
        // Fail closed: a checkout with TOCs but neither BIG nor BTI classified
        // means the discovery path is broken.
        assert!(
            big_with_summary + bti_without_summary > 0,
            "no SSTables classified from {} TOC fixture(s)",
            tocs.len()
        );
    }
}
