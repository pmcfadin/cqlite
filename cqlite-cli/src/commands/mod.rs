use crate::cli::{ExportFormat, ImportFormat, OutputFormat};
use anyhow::{Context, Result};
use cqlite_core::{
    schema::TableSchema,
    storage::sstable::reader::SSTableReader,
    types::{CqlType, CqlValue},
};
use indicatif::{ProgressBar, ProgressStyle};
use prettytable::{Cell, Row, Table};
use serde_json;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

pub mod admin;
pub mod bench;
pub mod schema;

pub async fn execute_query(
    db_path: &Path,
    query: &str,
    explain: bool,
    timing: bool,
    format: OutputFormat,
) -> Result<()> {
    // TODO: Implement query execution
    println!("Executing query: {}", query);
    println!("Database: {}", db_path.display());
    println!(
        "Explain: {}, Timing: {}, Format: {}",
        explain, timing, format
    );

    // Placeholder implementation
    Ok(())
}

pub async fn import_data(
    db_path: &Path,
    file: &Path,
    format: ImportFormat,
    table: Option<&str>,
) -> Result<()> {
    // TODO: Implement data import
    println!("Importing data from: {}", file.display());
    println!("Database: {}", db_path.display());
    println!("Format: {}, Table: {:?}", format, table);

    // Placeholder implementation
    Ok(())
}

pub async fn export_data(
    db_path: &Path,
    source: &str,
    file: &Path,
    format: ExportFormat,
) -> Result<()> {
    // TODO: Implement data export
    println!("Exporting data from: {}", source);
    println!("Database: {}", db_path.display());
    println!("Output file: {}, Format: {}", file.display(), format);

    // Placeholder implementation
    Ok(())
}

/// Read and display SSTable data with schema
pub async fn read_sstable(
    sstable_path: &Path,
    schema_path: &Path,
    limit: Option<usize>,
    skip: Option<usize>,
    format: OutputFormat,
) -> Result<()> {
    // Load schema
    let schema_content = std::fs::read_to_string(schema_path)
        .with_context(|| format!("Failed to read schema file: {}", schema_path.display()))?;

    let schema: TableSchema =
        serde_json::from_str(&schema_content).with_context(|| "Failed to parse schema JSON")?;

    // Create SSTable reader
    let mut reader = SSTableReader::open(sstable_path)
        .with_context(|| format!("Failed to open SSTable: {}", sstable_path.display()))?;

    println!("Reading SSTable: {}", sstable_path.display());
    println!(
        "Schema: {} ({})",
        schema.table_name(),
        schema.columns().len()
    );

    let skip_count = skip.unwrap_or(0);
    let limit_count = limit.unwrap_or(100); // Default limit to avoid overwhelming output

    // Create progress bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {pos} rows processed")
            .unwrap(),
    );

    match format {
        OutputFormat::Table => {
            display_as_table(&mut reader, &schema, skip_count, limit_count, &pb).await
        }
        OutputFormat::Json => {
            display_as_json(&mut reader, &schema, skip_count, limit_count, &pb).await
        }
        OutputFormat::Csv => {
            display_as_csv(&mut reader, &schema, skip_count, limit_count, &pb).await
        }
        OutputFormat::Yaml => {
            display_as_yaml(&mut reader, &schema, skip_count, limit_count, &pb).await
        }
    }
}

/// Display SSTable information
pub async fn sstable_info(sstable_path: &Path) -> Result<()> {
    let reader = SSTableReader::open(sstable_path)
        .with_context(|| format!("Failed to open SSTable: {}", sstable_path.display()))?;

    let metadata = reader.metadata();

    println!("SSTable Information");
    println!("==================");
    println!("File: {}", sstable_path.display());
    println!("Size: {} bytes", std::fs::metadata(sstable_path)?.len());

    if let Some(index_count) = metadata.get("index_entries") {
        println!("Index entries: {}", index_count);
    }

    if let Some(compression) = metadata.get("compression") {
        println!("Compression: {}", compression);
    }

    println!(
        "Format version: {}",
        metadata
            .get("format_version")
            .unwrap_or(&"unknown".to_string())
    );

    Ok(())
}

/// Export SSTable data to file
pub async fn export_sstable(
    sstable_path: &Path,
    schema_path: &Path,
    output_path: &Path,
    format: ExportFormat,
) -> Result<()> {
    // Load schema
    let schema_content = std::fs::read_to_string(schema_path)
        .with_context(|| format!("Failed to read schema file: {}", schema_path.display()))?;

    let schema: TableSchema =
        serde_json::from_str(&schema_content).with_context(|| "Failed to parse schema JSON")?;

    let mut reader = SSTableReader::open(sstable_path)
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
        ExportFormat::Json => export_as_json(&mut reader, &schema, &mut output_file, &pb).await,
        ExportFormat::Csv => export_as_csv(&mut reader, &schema, &mut output_file, &pb).await,
        ExportFormat::Parquet => {
            anyhow::bail!("Parquet export not yet implemented");
        }
        ExportFormat::Sql => export_as_sql(&mut reader, &schema, &mut output_file, &pb).await,
    }
}

// Helper functions for different display formats
async fn display_as_table(
    reader: &mut SSTableReader,
    schema: &TableSchema,
    skip: usize,
    limit: usize,
    pb: &ProgressBar,
) -> Result<()> {
    let mut table = Table::new();

    // Add header row
    let mut header = Row::empty();
    for column in schema.columns() {
        header.add_cell(Cell::new(&column.name));
    }
    table.add_row(header);

    let mut processed = 0;
    let mut displayed = 0;

    for entry in reader.iter() {
        pb.set_position(processed as u64);

        if processed < skip {
            processed += 1;
            continue;
        }

        if displayed >= limit {
            break;
        }

        let (key, value) = entry?;
        let mut row = Row::empty();

        for (i, column) in schema.columns().iter().enumerate() {
            let cell_value = if i < value.len() {
                format_cql_value(&value[i], &column.data_type)
            } else {
                "NULL".to_string()
            };
            row.add_cell(Cell::new(&cell_value));
        }

        table.add_row(row);
        processed += 1;
        displayed += 1;
    }

    pb.finish_with_message(format!("Displayed {} rows", displayed));
    table.printstd();

    Ok(())
}

async fn display_as_json(
    reader: &mut SSTableReader,
    schema: &TableSchema,
    skip: usize,
    limit: usize,
    pb: &ProgressBar,
) -> Result<()> {
    let mut rows = Vec::new();
    let mut processed = 0;
    let mut displayed = 0;

    for entry in reader.iter() {
        pb.set_position(processed as u64);

        if processed < skip {
            processed += 1;
            continue;
        }

        if displayed >= limit {
            break;
        }

        let (key, value) = entry?;
        let mut row = serde_json::Map::new();

        for (i, column) in schema.columns().iter().enumerate() {
            let cell_value = if i < value.len() {
                value_to_json(&value[i])
            } else {
                serde_json::Value::Null
            };
            row.insert(column.name.clone(), cell_value);
        }

        rows.push(serde_json::Value::Object(row));
        processed += 1;
        displayed += 1;
    }

    pb.finish_with_message(format!("Displayed {} rows", displayed));
    println!("{}", serde_json::to_string_pretty(&rows)?);

    Ok(())
}

async fn display_as_csv(
    reader: &mut SSTableReader,
    schema: &TableSchema,
    skip: usize,
    limit: usize,
    pb: &ProgressBar,
) -> Result<()> {
    let mut wtr = csv::Writer::from_writer(std::io::stdout());

    // Write header
    let headers: Vec<&str> = schema.columns().iter().map(|c| c.name.as_str()).collect();
    wtr.write_record(&headers)?;

    let mut processed = 0;
    let mut displayed = 0;

    for entry in reader.iter() {
        pb.set_position(processed as u64);

        if processed < skip {
            processed += 1;
            continue;
        }

        if displayed >= limit {
            break;
        }

        let (key, value) = entry?;
        let mut record = Vec::new();

        for (i, column) in schema.columns().iter().enumerate() {
            let cell_value = if i < value.len() {
                format_cql_value(&value[i], &column.data_type)
            } else {
                "NULL".to_string()
            };
            record.push(cell_value);
        }

        wtr.write_record(&record)?;
        processed += 1;
        displayed += 1;
    }

    pb.finish_with_message(format!("Displayed {} rows", displayed));
    wtr.flush()?;

    Ok(())
}

async fn display_as_yaml(
    reader: &mut SSTableReader,
    schema: &TableSchema,
    skip: usize,
    limit: usize,
    pb: &ProgressBar,
) -> Result<()> {
    // For YAML, we'll create the same structure as JSON and convert it
    let mut rows = Vec::new();
    let mut processed = 0;
    let mut displayed = 0;

    for entry in reader.iter() {
        pb.set_position(processed as u64);

        if processed < skip {
            processed += 1;
            continue;
        }

        if displayed >= limit {
            break;
        }

        let (key, value) = entry?;
        let mut row = serde_json::Map::new();

        for (i, column) in schema.columns().iter().enumerate() {
            let cell_value = if i < value.len() {
                value_to_json(&value[i])
            } else {
                serde_json::Value::Null
            };
            row.insert(column.name.clone(), cell_value);
        }

        rows.push(serde_json::Value::Object(row));
        processed += 1;
        displayed += 1;
    }

    pb.finish_with_message(format!("Displayed {} rows", displayed));
    println!("{}", serde_yaml::to_string(&rows)?);

    Ok(())
}

// Export helper functions
async fn export_as_json(
    reader: &mut SSTableReader,
    schema: &TableSchema,
    output: &mut File,
    pb: &ProgressBar,
) -> Result<()> {
    writeln!(output, "[")?;
    let mut first = true;
    let mut processed = 0;

    for entry in reader.iter() {
        pb.set_position(processed as u64);
        let (key, value) = entry?;

        if !first {
            writeln!(output, ",")?;
        }
        first = false;

        let mut row = serde_json::Map::new();
        for (i, column) in schema.columns().iter().enumerate() {
            let cell_value = if i < value.len() {
                value_to_json(&value[i])
            } else {
                serde_json::Value::Null
            };
            row.insert(column.name.clone(), cell_value);
        }

        write!(output, "  {}", serde_json::to_string(&row)?)?;
        processed += 1;
    }

    writeln!(output, "\n]")?;
    pb.finish_with_message(format!("Exported {} rows", processed));
    Ok(())
}

async fn export_as_csv(
    reader: &mut SSTableReader,
    schema: &TableSchema,
    output: &mut File,
    pb: &ProgressBar,
) -> Result<()> {
    let mut wtr = csv::Writer::from_writer(output);

    // Write header
    let headers: Vec<&str> = schema.columns().iter().map(|c| c.name.as_str()).collect();
    wtr.write_record(&headers)?;

    let mut processed = 0;

    for entry in reader.iter() {
        pb.set_position(processed as u64);
        let (key, value) = entry?;

        let mut record = Vec::new();
        for (i, column) in schema.columns().iter().enumerate() {
            let cell_value = if i < value.len() {
                format_cql_value(&value[i], &column.data_type)
            } else {
                "NULL".to_string()
            };
            record.push(cell_value);
        }

        wtr.write_record(&record)?;
        processed += 1;
    }

    pb.finish_with_message(format!("Exported {} rows", processed));
    wtr.flush()?;
    Ok(())
}

async fn export_as_sql(
    reader: &mut SSTableReader,
    schema: &TableSchema,
    output: &mut File,
    pb: &ProgressBar,
) -> Result<()> {
    writeln!(output, "-- SQL export for table: {}", schema.table_name())?;
    writeln!(output, "-- Generated by CQLite CLI")?;
    writeln!(output)?;

    let mut processed = 0;

    for entry in reader.iter() {
        pb.set_position(processed as u64);
        let (key, value) = entry?;

        write!(output, "INSERT INTO {} (", schema.table_name())?;
        let column_names: Vec<&str> = schema.columns().iter().map(|c| c.name.as_str()).collect();
        write!(output, "{}", column_names.join(", "))?;
        write!(output, ") VALUES (")?;

        let mut values = Vec::new();
        for (i, column) in schema.columns().iter().enumerate() {
            let cell_value = if i < value.len() {
                format_cql_value_for_sql(&value[i], &column.data_type)
            } else {
                "NULL".to_string()
            };
            values.push(cell_value);
        }

        write!(output, "{}", values.join(", "))?;
        writeln!(output, ");")?;
        processed += 1;
    }

    pb.finish_with_message(format!("Exported {} rows", processed));
    Ok(())
}

// Helper functions for value formatting
fn format_cql_value(value: &CqlValue, data_type: &CqlType) -> String {
    match value {
        CqlValue::Null => "NULL".to_string(),
        CqlValue::Boolean(b) => b.to_string(),
        CqlValue::TinyInt(i) => i.to_string(),
        CqlValue::SmallInt(i) => i.to_string(),
        CqlValue::Int(i) => i.to_string(),
        CqlValue::BigInt(i) => i.to_string(),
        CqlValue::Float(f) => f.to_string(),
        CqlValue::Double(d) => d.to_string(),
        CqlValue::Text(s) => s.clone(),
        CqlValue::Blob(b) => format!("0x{}", hex::encode(b)),
        CqlValue::Uuid(u) => u.to_string(),
        CqlValue::TimeUuid(u) => u.to_string(),
        CqlValue::List(items) => {
            let formatted: Vec<String> = items
                .iter()
                .map(|item| format_cql_value(item, data_type))
                .collect();
            format!("[{}]", formatted.join(", "))
        }
        CqlValue::Set(items) => {
            let formatted: Vec<String> = items
                .iter()
                .map(|item| format_cql_value(item, data_type))
                .collect();
            format!("{{{}}}", formatted.join(", "))
        }
        CqlValue::Map(map) => {
            let formatted: Vec<String> = map
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}: {}",
                        format_cql_value(k, data_type),
                        format_cql_value(v, data_type)
                    )
                })
                .collect();
            format!("{{{}}}", formatted.join(", "))
        }
        _ => format!("{:?}", value),
    }
}

fn format_cql_value_for_sql(value: &CqlValue, data_type: &CqlType) -> String {
    match value {
        CqlValue::Null => "NULL".to_string(),
        CqlValue::Text(s) => format!("'{}'", s.replace("'", "''")), // Escape single quotes
        CqlValue::Blob(b) => format!("'\\x{}'", hex::encode(b)),
        _ => format_cql_value(value, data_type),
    }
}

fn value_to_json(value: &CqlValue) -> serde_json::Value {
    match value {
        CqlValue::Null => serde_json::Value::Null,
        CqlValue::Boolean(b) => serde_json::Value::Bool(*b),
        CqlValue::TinyInt(i) => serde_json::Value::Number((*i as i64).into()),
        CqlValue::SmallInt(i) => serde_json::Value::Number((*i as i64).into()),
        CqlValue::Int(i) => serde_json::Value::Number((*i as i64).into()),
        CqlValue::BigInt(i) => serde_json::Value::Number((*i).into()),
        CqlValue::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        CqlValue::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        CqlValue::Text(s) => serde_json::Value::String(s.clone()),
        CqlValue::Blob(b) => serde_json::Value::String(format!("0x{}", hex::encode(b))),
        CqlValue::Uuid(u) => serde_json::Value::String(u.to_string()),
        CqlValue::TimeUuid(u) => serde_json::Value::String(u.to_string()),
        CqlValue::List(items) => {
            let json_items: Vec<serde_json::Value> = items.iter().map(value_to_json).collect();
            serde_json::Value::Array(json_items)
        }
        CqlValue::Set(items) => {
            let json_items: Vec<serde_json::Value> = items.iter().map(value_to_json).collect();
            serde_json::Value::Array(json_items)
        }
        CqlValue::Map(map) => {
            let mut json_map = serde_json::Map::new();
            for (k, v) in map {
                if let CqlValue::Text(key_str) = k {
                    json_map.insert(key_str.clone(), value_to_json(v));
                } else {
                    json_map.insert(format!("{:?}", k), value_to_json(v));
                }
            }
            serde_json::Value::Object(json_map)
        }
        _ => serde_json::Value::String(format!("{:?}", value)),
    }
}
