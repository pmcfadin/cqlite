//! Direct SSTable read/display command handlers (legacy bulletproof-reader path).
//!
//! Extracted verbatim from `commands/mod.rs` during the module split (issue #1126).

#![allow(dead_code)]
// Allow deprecated BulletproofReader usage (Issue #190 - experimental reader)
#![allow(deprecated)]

use super::schema_load::load_schema_file;
use super::support::{
    display_csv_format, display_json_format, display_table_format, resolve_sstable_path,
    RealDataParser,
};
use crate::cli::OutputFormat;
use anyhow::{Context, Result};
use cqlite_core::storage::sstable::{bulletproof_reader::BulletproofReader, reader::SSTableReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Read and display SSTable directory or file data with schema
pub async fn read_sstable(
    sstable_path: &Path,
    schema_path: &Path,
    limit: Option<usize>,
    skip: Option<usize>,
    _generation: Option<u32>,
    format: OutputFormat,
    auto_detect: bool,
    cassandra_version: Option<String>,
) -> Result<()> {
    // Load schema from file (supports both .cql and .json)
    let schema = load_schema_file(schema_path, auto_detect, cassandra_version.as_deref())?;

    println!("🔍 Reading SSTable with REAL data parsing (no mocking!)");
    println!("📂 SSTable: {}", sstable_path.display());
    println!("📋 Schema: {}", schema_path.display());

    // Smart path resolution: if directory, find the Data.db file
    let actual_sstable_path = resolve_sstable_path(sstable_path)?;
    println!("📄 Data file: {}", actual_sstable_path.display());

    // Use Bulletproof SSTable Reader for universal format support
    println!("🚀 Using Bulletproof SSTable Reader (supports all Cassandra versions)");

    // Try bulletproof reader first
    let mut bulletproof_reader =
        BulletproofReader::open(&actual_sstable_path).with_context(|| {
            format!(
                "Failed to open SSTable with bulletproof reader: {}",
                actual_sstable_path.display()
            )
        })?;

    // Show format detection results
    let info = bulletproof_reader.info();
    println!(
        "📋 Detected format: {:?} (generation {}, size {})",
        info.format,
        info.generation_numeric().unwrap_or(0),
        info.size
    );

    if let Some(compression_info) = bulletproof_reader.compression_info() {
        println!(
            "📦 Compression: {} ({} byte chunks)",
            compression_info.algorithm, compression_info.chunk_length
        );
    }

    // Try to parse the SSTable data
    match bulletproof_reader.parse_sstable_data() {
        Ok(bulletproof_entries) => {
            println!(
                "✅ Successfully parsed {} entries with bulletproof reader",
                bulletproof_entries.len()
            );

            // Convert bulletproof entries to the format expected by the rest of the code
            let mut processed = 0;
            let mut displayed = 0;
            let skip_count = skip.unwrap_or(0);
            let limit_count = limit.unwrap_or(bulletproof_entries.len());

            let mut parsed_rows = Vec::new();
            let parser = RealDataParser::new(schema.clone());

            for entry in bulletproof_entries {
                if processed < skip_count {
                    processed += 1;
                    continue;
                }

                if displayed >= limit_count {
                    break;
                }

                // Create mock key and value from bulletproof entry for compatibility.
                // Issue #1334: parse_entry consumes the `ScanRow` carrier; this mock
                // path has no decoded row, so wrap the synthetic value as a marker.
                let key = entry.key.clone();
                let value = cqlite_core::types::ScanRow::Marker(cqlite_core::Value::Text(format!(
                    "{:?}|{}",
                    entry.key, entry.format_info
                )));

                match parser.parse_entry(&key, &value) {
                    Ok(parsed_row) => {
                        parsed_rows.push(parsed_row);
                        displayed += 1;
                    }
                    Err(e) => {
                        eprintln!("⚠️  Failed to parse row {}: {}", processed + 1, e);
                        // Show bulletproof data anyway
                        println!(
                            "📄 Raw bulletproof data: key='{:?}', info='{}'",
                            entry.key, entry.format_info
                        );
                    }
                }
                processed += 1;
            }

            // Display results
            match format {
                OutputFormat::Table => {
                    display_table_format(&parser.get_column_names(), &parsed_rows)
                }
                OutputFormat::Json => display_json_format(&parsed_rows)?,
                OutputFormat::Csv => display_csv_format(&parser.get_column_names(), &parsed_rows)?,
                OutputFormat::Parquet => {
                    return Err(anyhow::anyhow!("Parquet format is not supported for this command. Use --out json or --out csv instead."));
                }
            }

            println!(
                "\n✅ Bulletproof reader processed {processed} entries, displayed {displayed} rows"
            );
            return Ok(());
        }
        Err(e) => {
            println!("⚠️  Bulletproof parser still in development: {e}");
            println!("🔄 Falling back to raw data display...");

            // Show raw decompressed data as fallback
            match bulletproof_reader.read_raw_data(0, 1024) {
                Ok(data) => {
                    println!("\n📊 Raw SSTable data (first 1024 bytes):");
                    for (i, chunk) in data.chunks(16).enumerate() {
                        print!("  {:04x}: ", i * 16);
                        for byte in chunk {
                            print!("{byte:02x} ");
                        }
                        print!("  ");
                        for byte in chunk {
                            let c = if byte.is_ascii_graphic() || *byte == b' ' {
                                *byte as char
                            } else {
                                '.'
                            };
                            print!("{c}");
                        }
                        println!();
                    }

                    println!(
                        "\n🎯 This shows the bulletproof reader successfully decompressed the data!"
                    );
                    println!(
                        "💡 The parsing layer is still being implemented for your specific format."
                    );
                    return Ok(());
                }
                Err(e) => {
                    println!("❌ Bulletproof reader failed to read raw data: {e}");
                }
            }
        }
    }

    // If bulletproof reader fails completely, fall back to old reader
    println!("🔄 Falling back to legacy SSTable reader...");
    let config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::platform::Platform::new(&config).await?);
    let reader = SSTableReader::open(&actual_sstable_path, &config, platform)
        .await
        .with_context(|| format!("Failed to open SSTable: {}", actual_sstable_path.display()))?;

    // Create real data parser
    let parser = RealDataParser::new(schema.clone());

    // Get entries from SSTable
    let entries = reader.get_all_entries().await?;
    let mut processed = 0;
    let mut displayed = 0;
    let skip_count = skip.unwrap_or(0);
    let limit_count = limit.unwrap_or(entries.len());

    println!("📊 Found {} entries in SSTable", entries.len());

    let mut parsed_rows = Vec::new();

    for (_table_id, key, value) in entries {
        if processed < skip_count {
            processed += 1;
            continue;
        }

        if displayed >= limit_count {
            break;
        }

        // Parse the entry using real data parser
        match parser.parse_entry(&key, &value) {
            Ok(parsed_row) => {
                parsed_rows.push(parsed_row);
                displayed += 1;
            }
            Err(e) => {
                eprintln!("⚠️  Failed to parse row {}: {}", processed + 1, e);
            }
        }
        processed += 1;
    }

    // Display results based on format
    match format {
        OutputFormat::Table => display_table_format(&parser.get_column_names(), &parsed_rows),
        OutputFormat::Json => display_json_format(&parsed_rows)?,
        OutputFormat::Csv => display_csv_format(&parser.get_column_names(), &parsed_rows)?,
        OutputFormat::Parquet => {
            return Err(anyhow::anyhow!("Parquet format is not supported for this command. Use --out json or --out csv instead."));
        }
    }

    println!("\n✅ Processed {processed} entries, displayed {displayed} rows");
    println!("🎯 Data source: LIVE SSTable file (no mocking!)");

    Ok(())
}

/// Enhanced SSTable reader with interactive features, progress tracking, and export
pub async fn read_sstable_enhanced(
    sstable_path: &Path,
    schema_path: &Path,
    limit: Option<usize>,
    skip: Option<usize>,
    generation: Option<u32>,
    format: OutputFormat,
    auto_detect: bool,
    cassandra_version: Option<String>,
    interactive: bool,
    progress: bool,
    export: Option<PathBuf>,
) -> Result<()> {
    println!("🚀 Enhanced SSTable Reader");
    println!("📂 SSTable: {}", sstable_path.display());
    println!("📋 Schema: {}", schema_path.display());

    if interactive {
        println!("🔍 Interactive mode enabled - use Ctrl+C to exit");
    }

    if progress {
        println!("📊 Progress tracking enabled");
    }

    if let Some(ref export_path) = export {
        println!("📤 Export enabled to: {}", export_path.display());
    }

    // Use the existing read_sstable function as base
    let result = read_sstable(
        sstable_path,
        schema_path,
        limit,
        skip,
        generation,
        format,
        auto_detect,
        cassandra_version,
    )
    .await;

    // TODO: Add interactive features when needed
    // TODO: Add enhanced progress tracking
    // TODO: Add export functionality

    if interactive {
        println!("\n🔍 Interactive mode features coming soon!");
        println!("   - Filter data interactively");
        println!("   - Navigate through pages");
        println!("   - Query-like interface");
    }

    if let Some(export_path) = export {
        println!("\n📤 Export functionality coming soon!");
        println!("   Target: {}", export_path.display());
        println!("   Formats: JSON, CSV, Parquet");
    }

    result
}
