//! Data export command handler (streaming export to CSV/JSON/CQL/Parquet).
//!
//! Extracted verbatim from `commands/mod.rs` during the module split (issue #1126).

#![allow(dead_code)]

#[cfg(feature = "state_machine")]
use anyhow::Context;
use anyhow::Result;
#[cfg(feature = "state_machine")]
use cqlite_core::Database;
#[cfg(feature = "state_machine")]
use indicatif::{ProgressBar, ProgressStyle};
#[cfg(feature = "state_machine")]
use std::fs::File;
#[cfg(feature = "state_machine")]
use std::io::{BufWriter, Write};
use std::path::Path;

/// Format duration for export statistics display
fn format_export_duration(duration: std::time::Duration) -> String {
    let secs = duration.as_secs();
    if secs == 0 {
        let millis = duration.as_millis();
        if millis > 0 {
            format!("{}ms", millis)
        } else {
            "<1ms".to_string()
        }
    } else if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m {}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    }
}

/// Export data using true streaming execution (Issue #280)
///
/// This function uses `execute_streaming()` to process rows incrementally,
/// avoiding the need to materialize all query results in memory at once.
#[cfg(feature = "state_machine")]
pub async fn export_data(
    database: &Database,
    source: &str,
    file: &Path,
    format: crate::cli::ExportFormat,
    query_filter: Option<&str>,
    limit: Option<usize>,
    quiet: bool,
) -> Result<()> {
    use crate::cli::ExportFormat;
    use cqlite_core::query::result::StreamingConfig;
    use std::io::IsTerminal;
    use std::time::Instant;

    use crate::output::{
        create_streaming_parquet_writer, StreamingCSVWriter, StreamingJSONWriter, StreamingWriter,
    };
    use crate::status_metrics::format_bytes;

    // Determine if progress should be shown (not quiet, and output is a TTY)
    let show_progress = !quiet && std::io::stdout().is_terminal();

    if show_progress {
        println!("Exporting data from: {source}");
        println!("Output file: {}, Format: {}", file.display(), format);
    }

    // Create output directory if it doesn't exist
    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create output directory: {}", parent.display()))?;
    }

    // Determine if source is a table name or a query
    let query = if source.to_uppercase().trim().starts_with("SELECT") {
        // If source is already a SELECT query, append LIMIT if specified
        // but only if the query doesn't already have a LIMIT clause
        match limit {
            Some(n) => {
                let upper = source.to_uppercase();
                if upper.contains(" LIMIT ") {
                    // Query already has LIMIT - use as-is to avoid invalid SQL
                    source.to_string()
                } else {
                    format!("{} LIMIT {}", source.trim_end_matches(';'), n)
                }
            }
            None => source.to_string(),
        }
    } else {
        // Source is a table name - build SELECT with optional WHERE and LIMIT
        let mut q = format!("SELECT * FROM {}", source);
        if let Some(filter) = query_filter {
            q.push_str(&format!(" WHERE {}", filter));
        }
        if let Some(n) = limit {
            q.push_str(&format!(" LIMIT {}", n));
        }
        q
    };

    if show_progress {
        println!(
            "Executing query: {}",
            query.chars().take(100).collect::<String>() + "..."
        );
    }

    // Configure streaming based on format
    let config = match format {
        ExportFormat::Parquet => StreamingConfig::for_parquet(),
        _ => StreamingConfig::for_text_formats(),
    };

    // Execute the query with streaming (Issue #280 - true end-to-end streaming)
    let mut result_iter = database
        .execute_streaming(&query, config.clone())
        .await
        .with_context(|| format!("Failed to execute streaming export query: {query}"))?;

    // Get column names from metadata
    let column_names: Vec<String> = result_iter
        .metadata
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();

    if column_names.is_empty() {
        return Err(anyhow::anyhow!(
            "Could not determine column names for export"
        ));
    }

    if show_progress {
        println!("Columns: {}", column_names.join(", "));
        println!("Streaming export in progress...");
    }

    // Track timing for statistics
    let start_time = Instant::now();

    // Create spinner progress bar (unknown total for streaming)
    let pb = if show_progress {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg} ({pos} rows)")
                .unwrap(),
        );
        pb.set_message("Exporting");
        pb
    } else {
        ProgressBar::hidden()
    };

    // Chunk size for collecting rows before writing
    let chunk_size = config.chunk_size;
    let mut rows_exported: u64 = 0;
    // Track remaining rows for limit enforcement (streaming doesn't automatically enforce LIMIT)
    let mut rows_remaining: Option<usize> = limit;

    // Export based on format with true streaming
    match format {
        ExportFormat::Csv => {
            let output_file = File::create(file)
                .with_context(|| format!("Failed to create CSV file: {}", file.display()))?;
            let buf_writer = BufWriter::new(output_file);
            let mut writer = StreamingCSVWriter::new(buf_writer);

            writer
                .write_header(&result_iter.metadata)
                .map_err(|e| anyhow::anyhow!("Failed to write CSV header: {}", e))?;

            // Stream rows in chunks
            loop {
                // Check if we've hit the limit
                if rows_remaining == Some(0) {
                    break;
                }

                let chunk = result_iter
                    .collect_chunk(chunk_size)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to collect chunk: {}", e))?;

                if chunk.is_empty() {
                    break;
                }

                // Truncate chunk if it exceeds remaining limit
                let chunk_to_write = if let Some(remaining) = rows_remaining {
                    if chunk.len() > remaining {
                        chunk.into_iter().take(remaining).collect::<Vec<_>>()
                    } else {
                        chunk
                    }
                } else {
                    chunk
                };

                let written = chunk_to_write.len();
                writer
                    .write_chunk(&chunk_to_write)
                    .map_err(|e| anyhow::anyhow!("Failed to write CSV chunk: {}", e))?;

                rows_exported += written as u64;
                pb.set_position(rows_exported);

                // Update remaining count
                if let Some(ref mut remaining) = rows_remaining {
                    *remaining = remaining.saturating_sub(written);
                }
            }

            writer
                .finalize()
                .map_err(|e| anyhow::anyhow!("Failed to finalize CSV: {}", e))?;
        }
        ExportFormat::Json => {
            let output_file = File::create(file)
                .with_context(|| format!("Failed to create JSON file: {}", file.display()))?;
            let buf_writer = BufWriter::new(output_file);
            let mut writer = StreamingJSONWriter::new(buf_writer);

            writer
                .write_header(&result_iter.metadata)
                .map_err(|e| anyhow::anyhow!("Failed to write JSON header: {}", e))?;

            // Stream rows in chunks
            loop {
                // Check if we've hit the limit
                if rows_remaining == Some(0) {
                    break;
                }

                let chunk = result_iter
                    .collect_chunk(chunk_size)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to collect chunk: {}", e))?;

                if chunk.is_empty() {
                    break;
                }

                // Truncate chunk if it exceeds remaining limit
                let chunk_to_write = if let Some(remaining) = rows_remaining {
                    if chunk.len() > remaining {
                        chunk.into_iter().take(remaining).collect::<Vec<_>>()
                    } else {
                        chunk
                    }
                } else {
                    chunk
                };

                let written = chunk_to_write.len();
                writer
                    .write_chunk(&chunk_to_write)
                    .map_err(|e| anyhow::anyhow!("Failed to write JSON chunk: {}", e))?;

                rows_exported += written as u64;
                pb.set_position(rows_exported);

                // Update remaining count
                if let Some(ref mut remaining) = rows_remaining {
                    *remaining = remaining.saturating_sub(written);
                }
            }

            writer
                .finalize()
                .map_err(|e| anyhow::anyhow!("Failed to finalize JSON: {}", e))?;
        }
        ExportFormat::Cql => {
            // CQL format needs special handling - collect all for table name extraction
            // For now, fall back to non-streaming for CQL
            let output_file = File::create(file)
                .with_context(|| format!("Failed to create CQL file: {}", file.display()))?;
            let mut buf_writer = BufWriter::new(output_file);

            // Extract table name from source
            let table_name = if source.to_uppercase().contains("FROM") {
                source
                    .split_whitespace()
                    .skip_while(|&word| word.to_uppercase() != "FROM")
                    .nth(1)
                    .unwrap_or("exported_table")
            } else {
                source
            };

            // Write header comment
            writeln!(buf_writer, "-- CQL Export from CQLite (streaming)")?;
            writeln!(buf_writer, "-- Source: {source}")?;
            writeln!(
                buf_writer,
                "-- Generated: {}",
                chrono::Utc::now().to_rfc3339()
            )?;
            writeln!(buf_writer)?;

            // Stream rows
            loop {
                // Check if we've hit the limit
                if rows_remaining == Some(0) {
                    break;
                }

                let chunk = result_iter
                    .collect_chunk(chunk_size)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to collect chunk: {}", e))?;

                if chunk.is_empty() {
                    break;
                }

                // Truncate chunk if it exceeds remaining limit
                let chunk_to_write: Vec<_> = if let Some(remaining) = rows_remaining {
                    if chunk.len() > remaining {
                        chunk.into_iter().take(remaining).collect()
                    } else {
                        chunk
                    }
                } else {
                    chunk
                };

                for row in &chunk_to_write {
                    let values: Vec<String> = column_names
                        .iter()
                        .map(|col| {
                            row.values
                                .get(col)
                                .map(|v| match v {
                                    cqlite_core::Value::Text(s) => {
                                        format!("'{}'", s.replace("'", "''"))
                                    }
                                    cqlite_core::Value::Null => "NULL".to_string(),
                                    _ => v.to_string(),
                                })
                                .unwrap_or_else(|| "NULL".to_string())
                        })
                        .collect();

                    writeln!(
                        buf_writer,
                        "INSERT INTO {} ({}) VALUES ({});",
                        table_name,
                        column_names.join(", "),
                        values.join(", ")
                    )?;
                }

                let written = chunk_to_write.len();
                rows_exported += written as u64;
                pb.set_position(rows_exported);

                // Update remaining count
                if let Some(ref mut remaining) = rows_remaining {
                    *remaining = remaining.saturating_sub(written);
                }
            }

            buf_writer.flush()?;
        }
        ExportFormat::Parquet => {
            let output_file = File::create(file)
                .with_context(|| format!("Failed to create Parquet file: {}", file.display()))?;

            let mut writer =
                create_streaming_parquet_writer(output_file, &result_iter.metadata, chunk_size)
                    .map_err(|e| anyhow::anyhow!("Failed to initialize Parquet writer: {}", e))?;

            writer
                .write_header(&result_iter.metadata)
                .map_err(|e| anyhow::anyhow!("Failed to write Parquet header: {}", e))?;

            // Stream rows in chunks
            loop {
                // Check if we've hit the limit
                if rows_remaining == Some(0) {
                    break;
                }

                let chunk = result_iter
                    .collect_chunk(chunk_size)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to collect chunk: {}", e))?;

                if chunk.is_empty() {
                    break;
                }

                // Truncate chunk if it exceeds remaining limit
                let chunk_to_write = if let Some(remaining) = rows_remaining {
                    if chunk.len() > remaining {
                        chunk.into_iter().take(remaining).collect::<Vec<_>>()
                    } else {
                        chunk
                    }
                } else {
                    chunk
                };

                let written = chunk_to_write.len();
                writer
                    .write_chunk(&chunk_to_write)
                    .map_err(|e| anyhow::anyhow!("Failed to write Parquet chunk: {}", e))?;

                rows_exported += written as u64;
                pb.set_position(rows_exported);

                // Update remaining count
                if let Some(ref mut remaining) = rows_remaining {
                    *remaining = remaining.saturating_sub(written);
                }
            }

            writer
                .finalize()
                .map_err(|e| anyhow::anyhow!("Failed to finalize Parquet: {}", e))?;
        }
    }

    pb.finish_and_clear();

    // Display statistics (unless quiet)
    if !quiet {
        let duration = start_time.elapsed();
        let file_size = std::fs::metadata(file)?.len();

        println!("\nExport complete:");
        println!("  Rows: {}", rows_exported);
        println!("  Size: {}", format_bytes(file_size));
        println!("  Time: {}", format_export_duration(duration));
        let secs_f64 = duration.as_secs_f64();
        if secs_f64 > 0.0 {
            let rate = rows_exported as f64 / secs_f64;
            if rate.is_finite() {
                println!("  Rate: {:.0} rows/sec", rate);
            }
        }
    }

    Ok(())
}

#[cfg(not(feature = "state_machine"))]
pub async fn export_data(
    _database: &cqlite_core::Database,
    _source: &str,
    _file: &Path,
    _format: crate::cli::ExportFormat,
    _query_filter: Option<&str>,
    _limit: Option<usize>,
    _quiet: bool,
) -> Result<()> {
    Err(anyhow::anyhow!(
        "Data export is not available in M1.\n\
         Build with --features state_machine to enable this feature.\n\
         See CLAUDE.md for M1 API examples."
    ))
}

/// Export query result to CSV format using streaming writer (Issue #280)
///
/// Uses `StreamingCSVWriter` for memory-efficient chunked export.
/// Rows are written directly to file in chunks.
#[cfg(feature = "state_machine")]
async fn export_to_csv(
    result: &cqlite_core::query::result::QueryResult,
    file: &Path,
    _column_names: &[String],
    pb: &ProgressBar,
) -> Result<()> {
    use crate::output::{StreamingCSVWriter, StreamingWriter};

    // Chunk size for CSV streaming
    const CHUNK_SIZE: usize = 5_000;

    let output_file = File::create(file)
        .with_context(|| format!("Failed to create CSV file: {}", file.display()))?;

    // Create streaming CSV writer with buffering for I/O efficiency
    let buf_writer = BufWriter::new(output_file);
    let mut writer = StreamingCSVWriter::new(buf_writer);

    // Write header (column names from metadata)
    writer
        .write_header(&result.metadata)
        .map_err(|e| anyhow::anyhow!("Failed to write CSV header: {}", e))?;

    // Process rows in chunks for memory efficiency
    for chunk in result.rows.chunks(CHUNK_SIZE) {
        writer
            .write_chunk(chunk)
            .map_err(|e| anyhow::anyhow!("Failed to write CSV chunk: {}", e))?;
        pb.inc(chunk.len() as u64);
    }

    // Finalize (flush)
    writer
        .finalize()
        .map_err(|e| anyhow::anyhow!("Failed to finalize CSV file: {}", e))?;

    Ok(())
}

/// Export query result to JSON format using streaming writer (Issue #280)
///
/// Uses `StreamingJSONWriter` for memory-efficient chunked export.
/// Rows are processed in chunks to avoid building entire JSON array in memory.
#[cfg(feature = "state_machine")]
async fn export_to_json(
    result: &cqlite_core::query::result::QueryResult,
    file: &Path,
    _column_names: &[String],
    pb: &ProgressBar,
) -> Result<()> {
    use crate::output::{StreamingJSONWriter, StreamingWriter};

    // Chunk size for JSON streaming (smaller than Parquet since JSON is text-heavy)
    const CHUNK_SIZE: usize = 5_000;

    let output_file = File::create(file)
        .with_context(|| format!("Failed to create JSON file: {}", file.display()))?;
    let buf_writer = BufWriter::new(output_file);

    // Create streaming JSON writer with pretty-printing
    let mut writer = StreamingJSONWriter::new(buf_writer);

    // Write header (opening bracket and store column order)
    writer
        .write_header(&result.metadata)
        .map_err(|e| anyhow::anyhow!("Failed to write JSON header: {}", e))?;

    // Process rows in chunks for memory efficiency
    for chunk in result.rows.chunks(CHUNK_SIZE) {
        writer
            .write_chunk(chunk)
            .map_err(|e| anyhow::anyhow!("Failed to write JSON chunk: {}", e))?;
        pb.inc(chunk.len() as u64);
    }

    // Finalize (write closing bracket)
    writer
        .finalize()
        .map_err(|e| anyhow::anyhow!("Failed to finalize JSON file: {}", e))?;

    Ok(())
}

/// Export query result to CQL INSERT statements
#[cfg(feature = "state_machine")]
async fn export_to_cql(
    result: &cqlite_core::query::result::QueryResult,
    file: &Path,
    source: &str,
    column_names: &[String],
    pb: &ProgressBar,
) -> Result<()> {
    let output_file = File::create(file)
        .with_context(|| format!("Failed to create CQL file: {}", file.display()))?;
    let mut writer = BufWriter::new(output_file);

    // Extract table name from source
    let table_name = if source.to_uppercase().contains("FROM") {
        // Try to extract table name from SELECT query
        source
            .split_whitespace()
            .skip_while(|&word| word.to_uppercase() != "FROM")
            .nth(1)
            .unwrap_or("exported_table")
    } else {
        source
    };

    // Write header comment
    writeln!(writer, "-- CQL Export from CQLite")?;
    writeln!(writer, "-- Source: {source}")?;
    writeln!(writer, "-- Generated: {}", chrono::Utc::now().to_rfc3339())?;
    writeln!(writer, "-- Rows: {}", result.rows.len())?;
    writeln!(writer)?;

    // Write INSERT statements
    for (index, row) in result.rows.iter().enumerate() {
        pb.set_position(index as u64 + 1);

        let values: Vec<String> = column_names
            .iter()
            .map(|col| {
                row.get(col)
                    .map(|v| match v {
                        cqlite_core::Value::Text(s) => format!("'{}'", s.replace("'", "''")),
                        cqlite_core::Value::Null => "NULL".to_string(),
                        _ => v.to_string(),
                    })
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect();

        writeln!(
            writer,
            "INSERT INTO {} ({}) VALUES ({});",
            table_name,
            column_names.join(", "),
            values.join(", ")
        )?;
    }

    writer
        .flush()
        .with_context(|| "Failed to flush CQL writer")?;

    Ok(())
}

/// Export query result to Parquet format using streaming writer (Issue #280)
///
/// Uses `StreamingParquetWriter` for memory-efficient chunked export.
/// Rows are processed in chunks (default 10,000) matching Parquet row group size.
#[cfg(feature = "state_machine")]
async fn export_to_parquet(
    result: &cqlite_core::query::result::QueryResult,
    file: &Path,
    _column_names: &[String],
    pb: &ProgressBar,
) -> Result<()> {
    use crate::output::{create_streaming_parquet_writer, StreamingWriter};

    // Default chunk size matches Parquet row group size
    const CHUNK_SIZE: usize = 10_000;

    pb.set_message("Initializing Parquet writer...");

    // Create file for streaming output
    let output_file = File::create(file)
        .with_context(|| format!("Failed to create Parquet file: {}", file.display()))?;

    // Create streaming writer with row group size = chunk size
    let mut writer = create_streaming_parquet_writer(output_file, &result.metadata, CHUNK_SIZE)
        .map_err(|e| anyhow::anyhow!("Failed to initialize Parquet writer: {}", e))?;

    // Write header (initializes Arrow schema)
    writer
        .write_header(&result.metadata)
        .map_err(|e| anyhow::anyhow!("Failed to write Parquet header: {}", e))?;

    pb.set_message("Streaming rows to Parquet...");

    // Process rows in chunks for memory efficiency
    for chunk in result.rows.chunks(CHUNK_SIZE) {
        writer
            .write_chunk(chunk)
            .map_err(|e| anyhow::anyhow!("Failed to write Parquet chunk: {}", e))?;
        pb.inc(chunk.len() as u64);
    }

    // Finalize (flush remaining rows, write footer)
    pb.set_message("Finalizing Parquet file...");
    writer
        .finalize()
        .map_err(|e| anyhow::anyhow!("Failed to finalize Parquet file: {}", e))?;

    Ok(())
}
