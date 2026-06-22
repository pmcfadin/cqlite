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
    let mutation: Mutation =
        serde_json::from_str(mutation_json).with_context(|| "Failed to parse mutation JSON")?;

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
/// * `compact` - Run compaction before export to merge multiple SSTables
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
    compact: bool,
    skip_validate: bool,
) -> Result<ExportResult> {
    let start = Instant::now();

    // If --compact was requested, run maintenance_step() before export
    if compact {
        let budget = std::time::Duration::from_secs(300); // 5-minute budget
        write_engine
            .maintenance_step(budget)
            .with_context(|| "Compaction before export failed")?;
    }

    // Use the current generation from the write engine
    let generation = write_engine.generation();

    let mut options = ExportOptions::new(keyspace, table, generation);
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

/// Result of a one-shot `compact` operation
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub struct CompactResult {
    /// Path to the written output Data.db
    pub output_path: std::path::PathBuf,
    /// Number of input SSTables merged
    pub input_files: usize,
    /// Partitions written to the output
    pub output_partitions: u64,
    /// Rows written to the output
    pub output_rows: u64,
    /// Size of the output Data.db in bytes
    pub data_file_size: u64,
    /// Wall-clock execution time in milliseconds
    pub execution_time_ms: f64,
}

#[cfg(feature = "write-support")]
impl CompactResult {
    /// Display the result to stdout
    pub fn display(&self) {
        println!(
            "OK: compacted {} SSTable(s) → {}",
            self.input_files,
            self.output_path.display()
        );
        println!(
            "  partitions: {}, rows: {}, Data.db: {} bytes ({:.1}ms)",
            self.output_partitions, self.output_rows, self.data_file_size, self.execution_time_ms
        );
    }
}

/// Handle the `compact` subcommand: one-shot, policy-free compaction of an
/// explicit set of input SSTables into a single output SSTable (Issue #842).
///
/// Reads the published `nb-*-big-Data.db` files under `args.input_dir`
/// (newest-generation first, so the newest run wins last-write-wins ties),
/// merges them, and writes the result under `args.output`.
///
/// `args.gc_before` / `args.now_sec` are threaded into the merge for
/// deterministic purge decisions but are not yet applied (issues #845/#848).
/// Map the `compact` subcommand args to the `purge_safe` flag threaded into
/// the merge (#921 finding 1).
///
/// `cqlite compact` compacts only the SSTables found under `<input-dir>`, which
/// is NOT necessarily every overlapping SSTable for the table. Tombstone
/// purging is only overlap-safe when that input set is complete, so the default
/// is CONSERVATIVE (`false`, no purge) and purging is enabled only when the
/// operator explicitly asserts a complete/major compaction via `--major`
/// (alias `--purge-tombstones`).
#[cfg(feature = "write-support")]
fn compact_purge_safe(args: &crate::cli_types::CompactArgs) -> bool {
    args.major
}

#[cfg(feature = "write-support")]
pub async fn handle_compact(args: &crate::cli_types::CompactArgs) -> Result<CompactResult> {
    let start = Instant::now();

    let schema = crate::commands::load_schema_file(&args.schema, false, None)?;

    let inputs = discover_input_sstables(&args.input_dir)?;
    if inputs.is_empty() {
        return Err(anyhow::anyhow!(
            "No published SSTables (nb-*-big-Data.db with a sibling TOC.txt) found under {}",
            args.input_dir.display()
        ));
    }

    // Overlap-safety gate for tombstone purging (#921 finding 1): map the
    // explicit `--major` opt-in to `purge_safe`. Default (flag absent) is
    // conservative — see `compact_purge_safe`.
    let purge_safe = compact_purge_safe(args);
    let report = cqlite_core::storage::write_engine::merge::compact_sstables(
        inputs,
        &args.output,
        &schema,
        args.generation,
        args.gc_before,
        args.now_sec,
        purge_safe,
    )
    .await
    .with_context(|| "compaction failed")?;

    Ok(CompactResult {
        output_path: report.output.data_path,
        input_files: report.stats.input_files,
        output_partitions: report.stats.output_partitions,
        output_rows: report.stats.output_rows,
        data_file_size: report.output.data_size,
        execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
    })
}

/// Recursively discover published input SSTables under `dir`, ordered
/// newest-to-oldest by generation (the order `compact_sstables` expects).
#[cfg(feature = "write-support")]
fn discover_input_sstables(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut found: Vec<(u64, std::path::PathBuf)> = Vec::new();
    collect_data_files(dir, &mut found, 8)?;
    // Highest generation first: run_index 0 must be the newest run.
    found.sort_by(|a, b| b.0.cmp(&a.0));
    Ok(found.into_iter().map(|(_, p)| p).collect())
}

/// Recursively collect `nb-<gen>-big-Data.db` files that have a sibling
/// `TOC.txt` (the publication barrier), pairing each with its generation.
#[cfg(feature = "write-support")]
fn collect_data_files(
    dir: &Path,
    out: &mut Vec<(u64, std::path::PathBuf)>,
    depth: usize,
) -> Result<()> {
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("Failed to read input directory: {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            // Honor the TOC.txt publication barrier: a Data.db without a sibling
            // TOC.txt is an unpublished/partial SSTable and must not be compacted.
            let base = name.trim_end_matches("-Data.db");
            let toc = path.with_file_name(format!("{base}-TOC.txt"));
            if !toc.exists() {
                continue;
            }
            // Parse generation from nb-<gen>-big-Data.db
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect_data_files(&path, out, depth - 1)?;
        }
    }
    Ok(())
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
    _compact: bool,
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

#[cfg(all(test, feature = "write-support"))]
mod compact_purge_safe_tests {
    use super::compact_purge_safe;
    use crate::cli_types::{Cli, Commands};
    use clap::Parser;

    /// Parse a `cqlite compact ...` invocation and return its `CompactArgs`.
    fn parse_compact(extra: &[&str]) -> crate::cli_types::CompactArgs {
        let mut argv = vec![
            "cqlite",
            "compact",
            "/tmp/in",
            "--output",
            "/tmp/out",
            "--schema",
            "/tmp/s.cql",
        ];
        argv.extend_from_slice(extra);
        match Cli::parse_from(argv).command {
            Some(Commands::Compact(args)) => args,
            _ => panic!("expected the Compact subcommand to parse"),
        }
    }

    /// #921 finding 1: with NO flag the CLI defaults to `purge_safe = false` —
    /// a purgeable tombstone is RETAINED (conservative; subset compaction must
    /// never resurrect shadowed data).
    #[test]
    fn issue_921_compact_defaults_to_no_purge() {
        let args = parse_compact(&[]);
        assert!(!args.major, "--major must default to false");
        assert!(
            !compact_purge_safe(&args),
            "default (no flag) must map to purge_safe = false (tombstones retained)"
        );
    }

    /// #921 finding 1: the explicit `--major` opt-in maps to `purge_safe = true`
    /// (the operator asserts the input set is the complete SSTable set).
    #[test]
    fn issue_921_compact_major_flag_enables_purge() {
        let args = parse_compact(&["--major"]);
        assert!(args.major, "--major must set major = true");
        assert!(
            compact_purge_safe(&args),
            "--major must map to purge_safe = true (purging enabled)"
        );
    }

    /// The `--purge-tombstones` alias is equivalent to `--major`.
    #[test]
    fn issue_921_compact_purge_tombstones_alias_enables_purge() {
        let args = parse_compact(&["--purge-tombstones"]);
        assert!(
            compact_purge_safe(&args),
            "--purge-tombstones alias must map to purge_safe = true"
        );
    }
}
