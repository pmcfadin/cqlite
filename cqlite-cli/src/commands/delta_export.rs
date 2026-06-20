//! Delta-export command handler (Issue #705, Epic #696 DS9).
//!
//! Wires `scan_delta` + `DeltaParquetWriter` end-to-end as the `delta-export`
//! CLI subcommand.  The subcommand is compiled only when the `delta-export`
//! feature is enabled (which also enables `cqlite-core/delta-scan` and
//! `cqlite-core/parquet`).
//!
//! ```bash
//! cqlite delta-export <sstable-dir> \
//!     --schema <file.cql> \
//!     --out parquet \
//!     -o <file.parquet>
//! ```

use anyhow::Result;

/// Result summary from a completed delta-export run.
#[derive(Debug)]
pub struct DeltaExportResult {
    /// Path to the output Parquet file.
    pub output_path: std::path::PathBuf,
    /// Total records written (all DeltaRecord variants).
    pub records_written: u64,
    /// Wall-clock time in milliseconds.
    pub execution_time_ms: f64,
    /// Number of collection element tombstones detected (v1 limitation warning).
    pub element_tombstone_warnings: u64,
}

impl DeltaExportResult {
    /// Print a one-line summary to stdout and warnings to stderr.
    pub fn display(&self) {
        println!(
            "delta-export: {} record(s) written to {} ({:.1}ms)",
            self.records_written,
            self.output_path.display(),
            self.execution_time_ms,
        );
        if self.element_tombstone_warnings > 0 {
            eprintln!(
                "warning: {} collection element tombstone(s) detected but not represented \
                 in v1 delta output (issue #493 — element-level fidelity is a tracked follow-up)",
                self.element_tombstone_warnings
            );
        }
    }
}

/// Run the `delta-export` subcommand.
///
/// This function is the `#[cfg(feature = "delta-export")]` path.  When the
/// feature is absent the caller emits a "rebuild with --features delta-export"
/// error instead of calling this function.
#[cfg(feature = "delta-export")]
pub async fn handle_delta_export(
    args: &crate::cli_types::DeltaExportArgs,
) -> Result<DeltaExportResult> {
    use crate::cli_types::DeltaCompressionCodec;
    use cqlite_core::export::delta_parquet::{
        DeltaParquetCompression, DeltaParquetOptions, DeltaParquetWriter,
    };
    use cqlite_core::export::delta_schema::DeltaSchemaOpts;
    use cqlite_core::storage::sstable::reader::delta_scan::scan_delta;
    use std::io::BufWriter;
    use std::time::Instant;

    let start = Instant::now();

    // -----------------------------------------------------------------------
    // Validate output path: refuse to overwrite unless --overwrite
    // -----------------------------------------------------------------------
    if args.output.exists() && !args.overwrite {
        return Err(anyhow::anyhow!(
            "Output file already exists: {}\n\
             Use --overwrite to replace it.",
            args.output.display()
        ));
    }

    // -----------------------------------------------------------------------
    // Load schema (errors at schema-derivation time, before any I/O)
    // -----------------------------------------------------------------------
    let schema = crate::commands::load_schema_file(&args.schema, false, None)?;

    // -----------------------------------------------------------------------
    // Build delta-parquet options (includes schema derivation via DS7).
    // Schema errors (counter table, column collision) surface here.
    // -----------------------------------------------------------------------
    let schema_opts = DeltaSchemaOpts::with_prefix(&args.envelope_prefix);
    let parquet_compression = match args.compression {
        DeltaCompressionCodec::Snappy => DeltaParquetCompression::Snappy,
        DeltaCompressionCodec::Zstd => DeltaParquetCompression::Zstd,
        DeltaCompressionCodec::Uncompressed => DeltaParquetCompression::Uncompressed,
    };

    // Derive source string: caller-supplied or the sstable_dir base name.
    let source = args.source.clone().unwrap_or_else(|| {
        args.sstable_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| args.sstable_dir.to_string_lossy().into_owned())
    });

    // Build options — this calls derive_delta_schema internally, so counter
    // tables and column collisions error here, before the output file is created.
    // We do a dry-run schema derivation first for early error detection with
    // richer CLI error messages.  DeltaParquetWriter::new will derive it a
    // second time internally; the double derivation is intentional: the first
    // call gives us nicer errors before any file is created, and the second
    // call is the writer's own internal schema build.
    {
        let schema_opts_check = DeltaSchemaOpts::with_prefix(&args.envelope_prefix);
        cqlite_core::export::delta_schema::derive_delta_schema(&schema, &schema_opts_check)
            .map_err(|e| {
                // Map DeltaSchemaError to anyhow with richer CLI context.
                let msg = match &e {
                    cqlite_core::export::DeltaSchemaError::ColumnCollision {
                        column,
                        reserved,
                    } => format!(
                        "Column '{column}' collides with envelope reserved name '{reserved}'.\n\
                         Use --envelope-prefix to choose a different prefix \
                         (e.g. --envelope-prefix \"_cqlite_\" gives \"_cqlite_op\", etc.).\n\
                         Original error: {e}"
                    ),
                    cqlite_core::export::DeltaSchemaError::CounterTable {
                        keyspace,
                        table,
                        columns,
                    } => format!(
                        "Counter tables cannot be exported as delta Parquet.\n\
                         Table '{keyspace}.{table}' contains counter column(s): {columns}.\n\
                         Counter semantics are incompatible with the per-cell writetime delta model."
                    ),
                    _ => format!("Schema error: {e}"),
                };
                anyhow::anyhow!("{}", msg)
            })?;
    }

    // -----------------------------------------------------------------------
    // Create output file (after all pre-flight checks pass)
    // -----------------------------------------------------------------------
    // Create parent directories if needed.
    if let Some(parent) = args.output.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create output directory '{}': {}",
                    parent.display(),
                    e
                )
            })?;
        }
    }

    let output_file = std::fs::File::create(&args.output).map_err(|e| {
        anyhow::anyhow!(
            "Failed to create output file '{}': {}",
            args.output.display(),
            e
        )
    })?;

    // Use a BufWriter to batch I/O; the Parquet writer already has its own
    // internal buffering but BufWriter reduces syscall overhead on large files.
    let buf_writer = BufWriter::new(output_file);

    // -----------------------------------------------------------------------
    // Create DeltaParquetWriter
    // -----------------------------------------------------------------------
    let opts = DeltaParquetOptions {
        row_group_size: args.row_group_size,
        compression: parquet_compression,
        schema_opts: schema_opts.clone(),
        source: source.clone(),
    };

    let mut writer = DeltaParquetWriter::new(buf_writer, &schema, opts).map_err(|e| {
        // If writer creation fails (schema error), the output file was created
        // but is empty/corrupt — remove it to ensure no partial file remains.
        let _ = std::fs::remove_file(&args.output);
        anyhow::anyhow!("Failed to initialise delta Parquet writer: {}", e)
    })?;

    // -----------------------------------------------------------------------
    // Stream records from scan_delta.
    //
    // scan_delta returns a (Receiver, ScanSummaryHandle) tuple.  The receiver
    // streams DeltaRecords; the handle accumulates scan-level statistics
    // (element tombstone count) and is read after the stream is drained.
    // -----------------------------------------------------------------------
    let (mut rx, summary) = scan_delta(
        args.sstable_dir.clone(),
        schema.clone(),
        /* buffer_size */ 64,
    );

    while let Some(result) = rx.recv().await {
        match result {
            Ok(record) => {
                writer.write_record(record).map_err(|e| {
                    // On write error, remove partial output file.
                    let _ = std::fs::remove_file(&args.output);
                    anyhow::anyhow!("Error writing delta record to Parquet: {}", e)
                })?;
            }
            Err(e) => {
                // Hard parse error — remove partial output file and propagate.
                let _ = std::fs::remove_file(&args.output);
                return Err(anyhow::anyhow!(
                    "Error scanning SSTable '{}': {}",
                    args.sstable_dir.display(),
                    e
                ));
            }
        }
    }

    // Stream is fully drained — read the final scan summary.
    // element_tombstone_warnings is plumbed from the ScanSummaryHandle, not
    // hardcoded, so the DS4 warning path is now reachable (issue #493).
    let element_tombstone_warnings = summary.read().element_tombstones_detected;

    let records_written = writer.records_written();

    // -----------------------------------------------------------------------
    // Finalize (writes Parquet footer + metadata)
    // -----------------------------------------------------------------------
    writer.finalize().map_err(|e| {
        let _ = std::fs::remove_file(&args.output);
        anyhow::anyhow!("Failed to finalise delta Parquet file: {}", e)
    })?;

    // Flush the BufWriter by dropping the file (already done when writer was finalized).

    Ok(DeltaExportResult {
        output_path: args.output.clone(),
        records_written,
        execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
        element_tombstone_warnings,
    })
}
