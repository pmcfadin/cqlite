// Read SSTable Command Implementation - Issue #124
// Direct SSTable reading and display with intelligent formatting

use anyhow::{Context, Result};
use cqlite_core::types::TableId;
use cqlite_core::{storage::sstable::reader::SSTableReader, Config as CoreConfig, RowKey, Value};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::Path;
use std::sync::Arc;

use crate::cli::OutputFormat;

/// Execute the read-sstable command
pub async fn execute_read_sstable_command(
    file_path: &Path,
    format: OutputFormat,
    limit: Option<usize>,
    skip: usize,
    keys_only: bool,
    raw: bool,
    verbose: bool,
) -> Result<()> {
    eprintln!("📖 Reading SSTable: {}", file_path.display());

    // Validate file exists
    if !file_path.exists() {
        return Err(anyhow::anyhow!(
            "SSTable file not found: {}",
            file_path.display()
        ));
    }

    // Create progress indicator
    let pb = create_progress_bar("Opening SSTable");

    // Initialize Platform and Config (following initialize_database pattern)
    let config = CoreConfig::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .with_context(|| {
                format!(
                    "Failed to initialize platform for SSTable at {}. \
                     Check file permissions and system resources.",
                    file_path.display()
                )
            })?,
    );

    pb.set_message("Opening SSTable reader...");

    // Open the SSTable
    let reader = SSTableReader::open(file_path, &config, platform)
        .await
        .with_context(|| format!("Failed to open SSTable: {}", file_path.display()))?;

    pb.set_message("Reading SSTable entries...");

    // Read all entries from the SSTable
    let entries = reader
        .get_all_entries()
        .await
        .context("Failed to read SSTable entries")?;

    let total_entries = entries.len();
    pb.finish_with_message(format!("✅ Read {} entries", total_entries));

    // Show basic stats if verbose
    if verbose {
        let stats = reader.stats().await?;
        eprintln!("\n📊 SSTable Statistics:");
        eprintln!("  Total entries: {}", stats.entry_count);
        eprintln!("  Table count: {}", stats.table_count);
        eprintln!("  Block count: {}", stats.block_count);
        eprintln!("  Index size: {} bytes", stats.index_size);
        eprintln!("  Bloom filter size: {} bytes", stats.bloom_filter_size);
        eprintln!(
            "  Compression ratio: {:.2}%",
            stats.compression_ratio * 100.0
        );
        eprintln!("  Cache hit rate: {:.2}%", stats.cache_hit_rate * 100.0);
        eprintln!();
    }

    // Apply skip and limit pagination
    let display_entries: Vec<_> = entries
        .into_iter()
        .skip(skip)
        .take(limit.unwrap_or(usize::MAX))
        .collect();

    let displayed_count = display_entries.len();

    if displayed_count == 0 {
        eprintln!(
            "No entries to display (total: {}, skip: {})",
            total_entries, skip
        );
        return Ok(());
    }

    eprintln!(
        "Displaying {} of {} entries (skip: {})\n",
        displayed_count, total_entries, skip
    );

    // Display based on format
    match format {
        OutputFormat::Table => {
            display_table_format(&display_entries, keys_only, raw)?;
        }
        OutputFormat::Json => {
            display_json_format(&display_entries, keys_only, raw)?;
        }
        OutputFormat::Csv => {
            display_csv_format(&display_entries, keys_only, raw)?;
        }
        OutputFormat::Yaml => {
            display_yaml_format(&display_entries, keys_only, raw)?;
        }
        OutputFormat::Parquet => {
            return Err(anyhow::anyhow!("Parquet format is not supported for this command. Use --out json or --out csv instead."));
        }
    }

    eprintln!(
        "\n✅ Displayed {} entries (total: {}, skipped: {})",
        displayed_count, total_entries, skip
    );

    Ok(())
}

/// Create a progress bar for operations
fn create_progress_bar(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg}")
            .expect("Failed to create progress bar template"),
    );
    pb.set_message(message.to_string());
    pb
}

/// Display entries in table format
fn display_table_format(
    entries: &[(TableId, RowKey, Value)],
    keys_only: bool,
    raw: bool,
) -> Result<()> {
    use prettytable::{Cell, Row, Table};

    let mut table = Table::new();

    // Add header
    if keys_only {
        table.set_titles(Row::new(vec![
            Cell::new("#"),
            Cell::new("Table ID"),
            Cell::new("Row Key"),
        ]));
    } else {
        table.set_titles(Row::new(vec![
            Cell::new("#"),
            Cell::new("Table ID"),
            Cell::new("Row Key"),
            Cell::new("Value"),
        ]));
    }

    // Add data rows
    for (idx, (table_id, key, value)) in entries.iter().enumerate() {
        let key_str = format_row_key(key, raw);
        let table_id_str = table_id.to_string();
        let mut row = Row::new(vec![
            Cell::new(&(idx + 1).to_string()),
            Cell::new(&table_id_str),
            Cell::new(&key_str),
        ]);

        if !keys_only {
            let value_str = format_value(value, raw);
            row.add_cell(Cell::new(&value_str));
        }

        table.add_row(row);
    }

    table.printstd();
    Ok(())
}

/// Display entries in JSON format
fn display_json_format(
    entries: &[(TableId, RowKey, Value)],
    keys_only: bool,
    raw: bool,
) -> Result<()> {
    let mut json_entries = Vec::new();

    for (table_id, key, value) in entries {
        let key_str = format_row_key(key, raw);
        let table_id_str = table_id.to_string();

        let entry = if keys_only {
            serde_json::json!({
                "table_id": table_id_str,
                "key": key_str,
            })
        } else {
            let value_str = format_value(value, raw);
            serde_json::json!({
                "table_id": table_id_str,
                "key": key_str,
                "value": value_str,
            })
        };

        json_entries.push(entry);
    }

    println!("{}", serde_json::to_string_pretty(&json_entries)?);
    Ok(())
}

/// Display entries in CSV format
fn display_csv_format(
    entries: &[(TableId, RowKey, Value)],
    keys_only: bool,
    raw: bool,
) -> Result<()> {
    let mut wtr = csv::Writer::from_writer(std::io::stdout());

    // Write header
    if keys_only {
        wtr.write_record(["table_id", "key"])?;
    } else {
        wtr.write_record(["table_id", "key", "value"])?;
    }

    // Write data rows
    for (table_id, key, value) in entries {
        let key_str = format_row_key(key, raw);
        let table_id_str = table_id.to_string();

        if keys_only {
            wtr.write_record([&table_id_str, &key_str])?;
        } else {
            let value_str = format_value(value, raw);
            wtr.write_record([&table_id_str, &key_str, &value_str])?;
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Display entries in YAML format
fn display_yaml_format(
    entries: &[(TableId, RowKey, Value)],
    keys_only: bool,
    raw: bool,
) -> Result<()> {
    let mut yaml_entries = Vec::new();

    for (table_id, key, value) in entries {
        let key_str = format_row_key(key, raw);
        let table_id_str = table_id.to_string();

        let entry = if keys_only {
            serde_json::json!({
                "table_id": table_id_str,
                "key": key_str,
            })
        } else {
            let value_str = format_value(value, raw);
            serde_json::json!({
                "table_id": table_id_str,
                "key": key_str,
                "value": value_str,
            })
        };

        yaml_entries.push(entry);
    }

    println!("{}", serde_yaml::to_string(&yaml_entries)?);
    Ok(())
}

/// Format a row key for display
fn format_row_key(key: &RowKey, _raw: bool) -> String {
    // Note: Raw formatting not yet implemented for RowKey
    // Both modes use Debug format currently
    format!("{:?}", key)
}

/// Format a value for display
fn format_value(value: &Value, raw: bool) -> String {
    if raw {
        // Show raw representation
        format!("{:?}", value)
    } else {
        // Use Display trait for user-friendly output
        format!("{}", value)
    }
}
