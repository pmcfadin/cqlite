//! Write command handlers for CLI write operations (Issue #392)
//!
//! This module provides command handlers for write operations including:
//! - Mutation writes (JSON format)
//! - Maintenance (compaction) operations
//! - Write engine statistics
//! - SSTable export
//!
//! All handlers require the `write-support` feature flag.

#[cfg(feature = "write-support")]
use anyhow::Context;
use anyhow::Result;
#[cfg(feature = "write-support")]
use std::io::{BufRead, BufReader};
#[allow(unused_imports)]
use std::path::Path;
#[cfg(feature = "write-support")]
use std::time::{Duration, Instant};

#[cfg(feature = "write-support")]
use cqlite_core::storage::write_engine::{ExportOptions, MaintenanceReport, Mutation, WriteEngine};

/// Result of a single write operation
#[derive(Debug)]
pub struct WriteResult {
    /// Number of rows affected
    pub rows_affected: u64,
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
}

impl WriteResult {
    /// Display the result to stdout
    pub fn display(&self) {
        println!(
            "OK: {} row(s) affected ({:.1}ms)",
            self.rows_affected, self.execution_time_ms
        );
    }
}

/// Result of a batch write operation
#[derive(Debug)]
pub struct BatchWriteResult {
    /// Total number of rows affected
    pub total_rows: u64,
    /// Number of successful writes
    pub successful_writes: u64,
    /// Number of failed writes
    pub failed_writes: u64,
    /// Total execution time in milliseconds
    pub execution_time_ms: f64,
}

impl BatchWriteResult {
    /// Display the result to stdout
    pub fn display(&self) {
        println!(
            "Batch complete: {} row(s) affected ({} succeeded, {} failed) in {:.1}ms",
            self.total_rows, self.successful_writes, self.failed_writes, self.execution_time_ms
        );
    }
}

/// Write engine statistics
#[derive(Debug)]
pub struct WriteStats {
    /// Current memtable size in bytes
    pub memtable_size: usize,
    /// Current memtable row count
    pub memtable_rows: usize,
    /// Current WAL size in bytes
    pub wal_size: u64,
    /// Current SSTable generation number
    pub generation: u64,
}

impl WriteStats {
    /// Display the statistics to stdout
    pub fn display(&self) {
        println!("Write Engine Statistics:");
        println!("  Memtable size: {} bytes", self.memtable_size);
        println!("  Memtable rows: {}", self.memtable_rows);
        println!("  WAL size: {} bytes", self.wal_size);
        println!("  Generation: {}", self.generation);
    }
}

/// Export operation report
#[derive(Debug)]
pub struct ExportResult {
    /// Output path of the Data.db file
    pub output_path: std::path::PathBuf,
    /// Number of rows exported
    pub row_count: u64,
    /// Size of the Data.db file in bytes
    pub data_file_size: u64,
    /// Total execution time in milliseconds
    pub execution_time_ms: f64,
}

impl ExportResult {
    /// Display the result to stdout
    pub fn display(&self) {
        println!("Export complete:");
        println!("  Output: {}", self.output_path.display());
        println!("  Rows: {}", self.row_count);
        println!("  Size: {} bytes", self.data_file_size);
        println!("  Time: {:.1}ms", self.execution_time_ms);
    }
}

/// Handle a single mutation write from JSON
///
/// # Arguments
///
/// * `write_engine` - The write engine to use
/// * `mutation_json` - JSON string representing the mutation
///
/// # Returns
///
/// A WriteResult on success, or an error if the mutation fails
#[cfg(feature = "write-support")]
pub async fn handle_mutation_write(
    write_engine: &mut WriteEngine,
    mutation_json: &str,
) -> Result<WriteResult> {
    let start = Instant::now();

    // Parse the mutation from JSON
    let mutation: Mutation = serde_json::from_str(mutation_json)
        .with_context(|| "Failed to parse mutation JSON")?;

    // Write the mutation
    write_engine
        .write_async(mutation)
        .await
        .with_context(|| "Failed to write mutation")?;

    Ok(WriteResult {
        rows_affected: 1,
        execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
    })
}

/// Handle a file containing mutations in JSONL format
///
/// # Arguments
///
/// * `write_engine` - The write engine to use
/// * `file_path` - Path to the JSONL file
///
/// # Returns
///
/// A BatchWriteResult on success, or an error if file reading fails
#[cfg(feature = "write-support")]
pub async fn handle_mutations_file(
    write_engine: &mut WriteEngine,
    file_path: &Path,
) -> Result<BatchWriteResult> {
    let start = Instant::now();

    let file = std::fs::File::open(file_path)
        .with_context(|| format!("Failed to open mutations file: {}", file_path.display()))?;

    let reader = BufReader::new(file);
    let mut successful_writes = 0u64;
    let mut failed_writes = 0u64;
    let mut line_number = 0u64;

    for line in reader.lines() {
        line_number += 1;
        let line = line
            .with_context(|| format!("Failed to read line {} from mutations file", line_number))?;

        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            // Skip empty lines and comments
            continue;
        }

        match serde_json::from_str::<Mutation>(trimmed) {
            Ok(mutation) => match write_engine.write_async(mutation).await {
                Ok(()) => {
                    successful_writes += 1;
                }
                Err(e) => {
                    eprintln!("Line {}: Write failed: {}", line_number, e);
                    failed_writes += 1;
                }
            },
            Err(e) => {
                eprintln!("Line {}: Invalid JSON: {}", line_number, e);
                failed_writes += 1;
            }
        }
    }

    Ok(BatchWriteResult {
        total_rows: successful_writes,
        successful_writes,
        failed_writes,
        execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
    })
}

/// Handle the maintenance subcommand
///
/// # Arguments
///
/// * `write_engine` - The write engine to use
/// * `budget_ms` - Time budget in milliseconds
///
/// # Returns
///
/// A MaintenanceReport on success
#[cfg(feature = "write-support")]
pub fn handle_maintenance(
    write_engine: &mut WriteEngine,
    budget_ms: u64,
) -> Result<MaintenanceReport> {
    let budget = Duration::from_millis(budget_ms);
    write_engine
        .maintenance_step(budget)
        .with_context(|| "Maintenance step failed")
}

/// Display a maintenance report
#[cfg(feature = "write-support")]
pub fn display_maintenance_report(report: &MaintenanceReport) {
    println!("Maintenance complete:");
    println!("  Time spent: {:?}", report.time_spent);
    println!("  Rows merged: {}", report.rows_merged);
    println!("  Bytes written: {} bytes", report.bytes_written);
    println!("  Pending compaction: {}", report.pending_compaction);
    if !report.completed_merges.is_empty() {
        println!("  Completed merges:");
        for path in &report.completed_merges {
            println!("    - {}", path.display());
        }
    }
}

/// Handle the write-stats subcommand
///
/// # Arguments
///
/// * `write_engine` - The write engine to query
///
/// # Returns
///
/// WriteStats containing current engine statistics
#[cfg(feature = "write-support")]
pub fn handle_write_stats(write_engine: &WriteEngine) -> Result<WriteStats> {
    Ok(WriteStats {
        memtable_size: write_engine.memtable_size(),
        memtable_rows: write_engine.memtable_row_count(),
        wal_size: write_engine.wal_size(),
        generation: write_engine.generation(),
    })
}

/// Handle the export-sstable subcommand
///
/// # Arguments
///
/// * `write_engine` - The write engine to export from
/// * `output_dir` - Output directory for the SSTable files
/// * `keyspace` - Keyspace name for the exported SSTable
/// * `table` - Table name for the exported SSTable
/// * `skip_compact` - Skip compaction before export
/// * `skip_validate` - Skip validation after export
///
/// # Returns
///
/// An ExportResult on success
#[cfg(feature = "write-support")]
pub async fn handle_export(
    write_engine: &mut WriteEngine,
    output_dir: &Path,
    keyspace: &str,
    table: &str,
    skip_compact: bool,
    skip_validate: bool,
) -> Result<ExportResult> {
    let start = Instant::now();

    // Use the current generation from the write engine
    let generation = write_engine.generation();

    let mut options = ExportOptions::new(keyspace, table, generation);
    if skip_compact {
        options = options.skip_compaction();
    }
    if skip_validate {
        options = options.skip_validation();
    }

    let report = write_engine
        .export_sstable(output_dir, options)
        .await
        .with_context(|| "SSTable export failed")?;

    Ok(ExportResult {
        output_path: report.output_path,
        row_count: report.row_count,
        data_file_size: report.data_file_size,
        execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
    })
}

/// Handle the flush operation
///
/// # Arguments
///
/// * `write_engine` - The write engine to flush
///
/// # Returns
///
/// Ok with SSTableInfo if data was flushed, or None if memtable was empty
#[cfg(feature = "write-support")]
pub async fn handle_flush(
    write_engine: &mut WriteEngine,
) -> Result<Option<cqlite_core::storage::sstable::writer::SSTableInfo>> {
    write_engine
        .flush()
        .await
        .with_context(|| "Flush operation failed")
}

/// Display the result of a flush operation
#[cfg(feature = "write-support")]
pub fn display_flush_result(info: Option<&cqlite_core::storage::sstable::writer::SSTableInfo>) {
    match info {
        Some(info) => {
            println!(
                "Flushed: {} partitions, {} bytes",
                info.partition_count, info.data_size
            );
            println!("  Output: {}", info.data_path.display());
        }
        None => {
            println!("Nothing to flush (memtable empty)");
        }
    }
}

// Stubs for when write-support feature is not enabled
#[cfg(not(feature = "write-support"))]
pub async fn handle_mutation_write(
    _write_engine: &mut (),
    _mutation_json: &str,
) -> Result<WriteResult> {
    Err(anyhow::anyhow!(
        "Write support is not enabled. Build with --features write-support to enable write operations."
    ))
}

#[cfg(not(feature = "write-support"))]
pub async fn handle_mutations_file(
    _write_engine: &mut (),
    _file_path: &Path,
) -> Result<BatchWriteResult> {
    Err(anyhow::anyhow!(
        "Write support is not enabled. Build with --features write-support to enable write operations."
    ))
}

#[cfg(not(feature = "write-support"))]
pub fn handle_maintenance(_write_engine: &mut (), _budget_ms: u64) -> Result<()> {
    Err(anyhow::anyhow!(
        "Write support is not enabled. Build with --features write-support to enable write operations."
    ))
}

#[cfg(not(feature = "write-support"))]
pub fn handle_write_stats(_write_engine: &()) -> Result<WriteStats> {
    Err(anyhow::anyhow!(
        "Write support is not enabled. Build with --features write-support to enable write operations."
    ))
}

#[cfg(not(feature = "write-support"))]
pub async fn handle_export(
    _write_engine: &mut (),
    _output_dir: &Path,
    _keyspace: &str,
    _table: &str,
    _skip_compact: bool,
    _skip_validate: bool,
) -> Result<ExportResult> {
    Err(anyhow::anyhow!(
        "Write support is not enabled. Build with --features write-support to enable write operations."
    ))
}

#[cfg(not(feature = "write-support"))]
pub async fn handle_flush(_write_engine: &mut ()) -> Result<Option<()>> {
    Err(anyhow::anyhow!(
        "Write support is not enabled. Build with --features write-support to enable write operations."
    ))
}
