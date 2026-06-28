//! SSTable-to-file export command handler (direct reader export).
//!
//! Extracted verbatim from `commands/mod.rs` during the module split (issue #1126).

#![allow(dead_code)]
// Allow deprecated BulletproofReader usage (Issue #190 - experimental reader)
#![allow(deprecated)]

#[cfg(feature = "state_machine")]
use super::schema_load::load_schema_file;
#[cfg(feature = "state_machine")]
use super::support::RealDataParser;
#[cfg(feature = "state_machine")]
use crate::cli::ExportFormat;
#[cfg(feature = "state_machine")]
use anyhow::Context;
use anyhow::Result;
#[cfg(feature = "state_machine")]
use cqlite_core::{schema::TableSchema, storage::sstable::reader::SSTableReader};
#[cfg(feature = "state_machine")]
use indicatif::{ProgressBar, ProgressStyle};
#[cfg(feature = "state_machine")]
use std::fs::File;
use std::path::Path;
#[cfg(feature = "state_machine")]
use std::sync::Arc;

/// Export SSTable data to file
#[cfg(feature = "state_machine")]
pub async fn export_sstable(
    sstable_path: &Path,
    schema_path: &Path,
    output_path: &Path,
    format: ExportFormat,
) -> Result<()> {
    // Load schema with auto-detection
    let schema = load_schema_file(schema_path, false, None)?;

    let config = cqlite_core::Config::default();
    let platform = Arc::new(cqlite_core::platform::Platform::new(&config).await?);
    let reader = SSTableReader::open(sstable_path, &config, platform)
        .await
        .with_context(|| format!("Failed to open SSTable: {}", sstable_path.display()))?;

    let mut output_file = File::create(output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;

    println!("Exporting SSTable: {}", sstable_path.display());
    println!("Output: {} ({})", output_path.display(), format);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {pos} rows exported")
            .unwrap(),
    );

    match format {
        ExportFormat::Json => export_as_json(&reader, &schema, &mut output_file, &pb).await,
        ExportFormat::Csv => export_as_csv(&reader, &schema, &mut output_file, &pb).await,
        ExportFormat::Parquet => {
            // Parquet writer manages its own file handle, so we drop the one we created
            drop(output_file);
            export_as_parquet(&reader, &schema, output_path, &pb).await
        }
        ExportFormat::Cql => export_as_cql(&reader, &schema, &mut output_file, &pb).await,
    }
}

/// Export SSTable data as JSON
#[cfg(feature = "state_machine")]
async fn export_as_json(
    reader: &SSTableReader,
    schema: &TableSchema,
    output_file: &mut File,
    pb: &ProgressBar,
) -> Result<()> {
    use std::io::Write;

    let parser = RealDataParser::new(schema.clone());
    let entries = reader.get_all_entries().await?;

    let mut json_objects = Vec::new();

    for (index, (_table_id, key, value)) in entries.iter().enumerate() {
        pb.set_position(index as u64);

        match parser.parse_entry(key, value) {
            Ok(parsed_row) => {
                json_objects.push(parsed_row.to_json());
            }
            Err(e) => {
                eprintln!("⚠️  Failed to parse row {}: {}", index + 1, e);
            }
        }
    }

    let json_output = serde_json::to_string_pretty(&json_objects)?;
    output_file.write_all(json_output.as_bytes())?;

    pb.finish_with_message(format!("Exported {} rows to JSON", json_objects.len()));
    Ok(())
}

/// Export SSTable data as CSV
#[cfg(feature = "state_machine")]
async fn export_as_csv(
    reader: &SSTableReader,
    schema: &TableSchema,
    output_file: &mut File,
    pb: &ProgressBar,
) -> Result<()> {
    let parser = RealDataParser::new(schema.clone());
    let entries = reader.get_all_entries().await?;

    let mut wtr = csv::Writer::from_writer(output_file);
    let column_names = parser.get_column_names();

    // Write header
    wtr.write_record(&column_names)?;

    let mut exported_count = 0;

    for (index, (_table_id, key, value)) in entries.iter().enumerate() {
        pb.set_position(index as u64);

        match parser.parse_entry(key, value) {
            Ok(parsed_row) => {
                let mut record = Vec::new();
                for column in &column_names {
                    let cell_value = parsed_row
                        .get(column)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "NULL".to_string());
                    record.push(cell_value);
                }
                wtr.write_record(&record)?;
                exported_count += 1;
            }
            Err(e) => {
                eprintln!("⚠️  Failed to parse row {}: {}", index + 1, e);
            }
        }
    }

    wtr.flush()?;
    pb.finish_with_message(format!("Exported {exported_count} rows to CSV"));
    Ok(())
}

/// Export SSTable data as CQL INSERT statements
#[cfg(feature = "state_machine")]
async fn export_as_cql(
    reader: &SSTableReader,
    schema: &TableSchema,
    output_file: &mut File,
    pb: &ProgressBar,
) -> Result<()> {
    use std::io::Write;

    let parser = RealDataParser::new(schema.clone());
    let entries = reader.get_all_entries().await?;
    let column_names = parser.get_column_names();

    // Write header
    writeln!(output_file, "-- CQL Export from CQLite")?;
    writeln!(
        output_file,
        "-- Table: {}.{}",
        schema.keyspace, schema.table
    )?;
    writeln!(
        output_file,
        "-- Generated: {}",
        chrono::Utc::now().to_rfc3339()
    )?;
    writeln!(output_file)?;

    let mut exported_count = 0;

    for (index, (_table_id, key, value)) in entries.iter().enumerate() {
        pb.set_position(index as u64);

        match parser.parse_entry(key, value) {
            Ok(parsed_row) => {
                let values: Vec<String> = column_names
                    .iter()
                    .map(|col| {
                        parsed_row
                            .get(col)
                            .map(|_v| "NULL".to_string())
                            .unwrap_or_else(|| "NULL".to_string())
                    })
                    .collect();

                writeln!(
                    output_file,
                    "INSERT INTO {}.{} ({}) VALUES ({});",
                    schema.keyspace,
                    schema.table,
                    column_names.join(", "),
                    values.join(", ")
                )?;
                exported_count += 1;
            }
            Err(e) => {
                eprintln!("⚠️  Failed to parse row {}: {}", index + 1, e);
            }
        }
    }

    pb.finish_with_message(format!("Exported {exported_count} rows to CQL"));
    Ok(())
}

/// Export SSTable data as Parquet using StreamingParquetWriter
///
/// This function converts SSTable entries to QueryRow format and uses
/// the StreamingParquetWriter for memory-efficient export.
#[cfg(feature = "state_machine")]
async fn export_as_parquet(
    reader: &SSTableReader,
    schema: &TableSchema,
    output_path: &Path,
    pb: &ProgressBar,
) -> Result<()> {
    use crate::output::parquet::create_streaming_parquet_writer;
    use crate::output::StreamingWriter;

    let entries = reader.get_all_entries().await?;

    if entries.is_empty() {
        pb.finish_with_message("No data to export");
        // Create empty Parquet file
        let output_file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
        let metadata = build_query_metadata_from_schema(schema);
        let mut writer = create_streaming_parquet_writer(output_file, &metadata, 10_000)
            .map_err(|e| anyhow::anyhow!("Failed to create Parquet writer: {}", e))?;
        writer
            .finalize()
            .map_err(|e| anyhow::anyhow!("Failed to finalize Parquet: {}", e))?;
        return Ok(());
    }

    // Build QueryMetadata from schema
    let metadata = build_query_metadata_from_schema(schema);

    // Create streaming Parquet writer
    let output_file = File::create(output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    let mut writer = create_streaming_parquet_writer(output_file, &metadata, 10_000)
        .map_err(|e| anyhow::anyhow!("Failed to create Parquet writer: {}", e))?;

    let mut chunk = Vec::with_capacity(1000);
    let mut exported_count = 0;

    for (index, (_table_id, row_key, value)) in entries.iter().enumerate() {
        pb.set_position(index as u64);

        // Convert SSTable entry to QueryRow
        let query_row = convert_entry_to_query_row(row_key, value, schema);
        chunk.push(query_row);

        if chunk.len() >= 1000 {
            writer
                .write_chunk(&chunk)
                .map_err(|e| anyhow::anyhow!("Failed to write Parquet chunk: {}", e))?;
            exported_count += chunk.len();
            chunk.clear();
        }
    }

    // Write remaining rows
    if !chunk.is_empty() {
        writer
            .write_chunk(&chunk)
            .map_err(|e| anyhow::anyhow!("Failed to write Parquet chunk: {}", e))?;
        exported_count += chunk.len();
    }

    writer
        .finalize()
        .map_err(|e| anyhow::anyhow!("Failed to finalize Parquet: {}", e))?;

    pb.finish_with_message(format!("Exported {} rows to Parquet", exported_count));
    Ok(())
}

/// Build QueryMetadata from TableSchema for Parquet export
#[cfg(feature = "state_machine")]
fn build_query_metadata_from_schema(schema: &TableSchema) -> cqlite_core::query::QueryMetadata {
    use cqlite_core::query::{ColumnInfo, QueryMetadata};

    let mut columns = Vec::new();
    let mut position = 0;

    // Add partition keys
    // Mark as nullable because direct SSTable export may not extract all key values
    // from the raw binary RowKey format
    for pk in &schema.partition_keys {
        columns.push(ColumnInfo {
            name: pk.name.clone(),
            data_type: parse_cql_type_string(&pk.data_type),
            nullable: true,
            position,
            table_name: Some(format!("{}.{}", schema.keyspace, schema.table)),
            cql_type: None,
        });
        position += 1;
    }

    // Add clustering keys
    // Mark as nullable because direct SSTable export may not extract all key values
    for ck in &schema.clustering_keys {
        columns.push(ColumnInfo {
            name: ck.name.clone(),
            data_type: parse_cql_type_string(&ck.data_type),
            nullable: true,
            position,
            table_name: Some(format!("{}.{}", schema.keyspace, schema.table)),
            cql_type: None,
        });
        position += 1;
    }

    // Add regular columns
    for col in &schema.columns {
        columns.push(ColumnInfo {
            name: col.name.clone(),
            data_type: parse_cql_type_string(&col.data_type),
            nullable: true,
            position,
            table_name: Some(format!("{}.{}", schema.keyspace, schema.table)),
            cql_type: None,
        });
        position += 1;
    }

    QueryMetadata {
        columns,
        ..Default::default()
    }
}

/// Parse CQL type string to DataType
#[cfg(feature = "state_machine")]
fn parse_cql_type_string(type_str: &str) -> cqlite_core::types::DataType {
    use cqlite_core::types::DataType;

    match type_str.to_lowercase().as_str() {
        "text" | "varchar" | "ascii" => DataType::Text,
        "int" | "integer" => DataType::Integer,
        "bigint" => DataType::BigInt,
        "smallint" => DataType::SmallInt,
        "tinyint" => DataType::TinyInt,
        "float" => DataType::Float32,
        "double" => DataType::Float,
        "boolean" => DataType::Boolean,
        "timestamp" => DataType::Timestamp,
        "date" => DataType::Timestamp, // Map date to Timestamp
        "time" => DataType::BigInt,    // Map time to BigInt (nanoseconds)
        "uuid" | "timeuuid" => DataType::Uuid,
        "blob" => DataType::Blob,
        "counter" => DataType::BigInt, // Map counter to BigInt
        "varint" => DataType::Blob,    // Map varint to Blob
        "decimal" => DataType::Text,   // Map decimal to Text (for now)
        s if s.starts_with("list") => DataType::List,
        s if s.starts_with("set") => DataType::Set,
        s if s.starts_with("map") => DataType::Map,
        s if s.starts_with("frozen") => DataType::Frozen,
        s if s.starts_with("tuple") => DataType::Tuple,
        _ => DataType::Text, // Default fallback
    }
}

/// Convert SSTable entry to QueryRow for Parquet export
#[cfg(feature = "state_machine")]
fn convert_entry_to_query_row(
    row_key: &cqlite_core::RowKey,
    value: &cqlite_core::Value,
    schema: &TableSchema,
) -> cqlite_core::query::QueryRow {
    use cqlite_core::query::{QueryRow, RowMetadata};
    use cqlite_core::Value;
    use std::collections::HashMap;

    let mut values: HashMap<String, Value> = HashMap::new();

    // Extract values from the Value (which is typically a Map for parsed rows)
    match value {
        Value::Map(pairs) => {
            // Each pair is (key_value, column_value)
            for (k, v) in pairs {
                if let Value::Text(col_name) = k {
                    values.insert(col_name.clone(), v.clone());
                }
            }
        }
        Value::Blob(data) => {
            // For raw blob data, assign to first regular column if available
            if let Some(first_col) = schema.columns.first() {
                values.insert(first_col.name.clone(), Value::Blob(data.clone()));
            }
        }
        Value::Text(s) => {
            if let Some(first_col) = schema.columns.first() {
                values.insert(first_col.name.clone(), Value::Text(s.clone()));
            }
        }
        other => {
            // For other value types, assign to first column
            if let Some(first_col) = schema.columns.first() {
                values.insert(first_col.name.clone(), other.clone());
            }
        }
    }

    // Ensure all schema columns have entries (use Null for missing)
    for pk in &schema.partition_keys {
        values.entry(pk.name.clone()).or_insert(Value::Null);
    }
    for ck in &schema.clustering_keys {
        values.entry(ck.name.clone()).or_insert(Value::Null);
    }
    for col in &schema.columns {
        values.entry(col.name.clone()).or_insert(Value::Null);
    }

    QueryRow {
        values,
        key: row_key.clone(),
        metadata: RowMetadata::default(),
        cell_metadata: None,
    }
}
