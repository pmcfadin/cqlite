//! Shared support types and helpers for the legacy SSTable command handlers.
//!
//! Extracted verbatim from `commands/mod.rs` during the module split (issue #1126).
//! These are the stub parser/result types that back the direct-read, export, inspect,
//! and benchmark handlers, plus the small display/path helpers they share.

#![allow(dead_code)]
// Allow deprecated BulletproofReader usage (Issue #190 - experimental reader)
#![allow(deprecated)]

use anyhow::Result;
use std::path::{Path, PathBuf};

// Temporary stub types for disabled modules
#[derive(Debug, Clone)]
pub struct ParsedRow {
    pub data: std::collections::HashMap<String, String>,
}

impl ParsedRow {
    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::Value::Object(
            self.data
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct RealDataParser {
    pub schema: cqlite_core::schema::TableSchema,
}

impl RealDataParser {
    pub fn new(schema: cqlite_core::schema::TableSchema) -> Self {
        Self { schema }
    }

    pub fn parse_entry(
        &self,
        _key: &cqlite_core::RowKey,
        _value: &cqlite_core::Value,
    ) -> Result<ParsedRow> {
        Ok(ParsedRow {
            data: std::collections::HashMap::new(),
        })
    }

    pub fn get_column_names(&self) -> Vec<String> {
        self.schema.columns.iter().map(|c| c.name.clone()).collect()
    }
}

// Stub for QueryExecutor
#[derive(Debug)]
pub struct QueryExecutor;

impl QueryExecutor {
    pub fn new(_config: QueryExecutorConfig) -> Self {
        Self
    }

    pub async fn execute_select(&self, _query: &str) -> Result<QueryResult> {
        Ok(QueryResult {
            rows: Vec::new(),
            execution_time_ms: 0.0,
        })
    }
}

#[derive(Debug, Default)]
pub struct QueryExecutorConfig;

// Wrapper struct for query results
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<ParsedRow>,
    pub execution_time_ms: f64,
}

impl QueryResult {
    pub fn display_table(&self) {
        if self.rows.is_empty() {
            println!("No rows returned");
            return;
        }

        // Create a simple table display
        let mut table = prettytable::Table::new();

        // Add headers if we can determine them from first row
        if let Some(first_row) = self.rows.first() {
            let headers: Vec<_> = first_row.data.keys().cloned().collect();
            table.set_titles(prettytable::Row::new(
                headers.iter().map(|h| prettytable::Cell::new(h)).collect(),
            ));

            // Add data rows
            for row in &self.rows {
                let cells: Vec<_> = headers
                    .iter()
                    .map(|h| prettytable::Cell::new(row.data.get(h).unwrap_or(&String::new())))
                    .collect();
                table.add_row(prettytable::Row::new(cells));
            }
        }

        table.printstd();
    }

    pub fn display_json(&self) -> Result<()> {
        let json_rows: Vec<_> = self.rows.iter().map(|r| r.to_json()).collect();
        println!("{}", serde_json::to_string_pretty(&json_rows)?);
        Ok(())
    }

    pub fn display_csv(&self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }

        let headers: Vec<_> = self.rows[0].data.keys().cloned().collect();

        // Print headers
        println!("{}", headers.join(","));

        // Print data
        for row in &self.rows {
            let values: Vec<_> = headers
                .iter()
                .map(|h| row.data.get(h).unwrap_or(&String::new()).clone())
                .collect();
            println!("{}", values.join(","));
        }

        Ok(())
    }
}

/// Resolve SSTable path: if directory, find the Data.db file
pub(crate) fn resolve_sstable_path(sstable_path: &Path) -> Result<PathBuf> {
    if sstable_path.is_file() {
        // If it's already a file, use it directly
        return Ok(sstable_path.to_path_buf());
    }

    if sstable_path.is_dir() {
        // If it's a directory, look for SSTable data files
        println!("📁 Directory detected, looking for SSTable files...");

        // Look for common SSTable data file patterns
        let patterns = ["*-Data.db", "*-big-Data.db", "nb-*-big-Data.db"];

        for pattern in &patterns {
            if let Ok(entries) = std::fs::read_dir(sstable_path) {
                for entry in entries.flatten() {
                    let file_name = entry.file_name();
                    let file_name_str = file_name.to_string_lossy();

                    // Match the pattern
                    if pattern.contains("*") {
                        let pattern_parts: Vec<&str> = pattern.split('*').collect();
                        if pattern_parts.len() == 2 {
                            let starts_with = pattern_parts[0];
                            let ends_with = pattern_parts[1];

                            if file_name_str.starts_with(starts_with)
                                && file_name_str.ends_with(ends_with)
                            {
                                let data_file = entry.path();
                                println!("✓ Found SSTable data file: {}", data_file.display());
                                return Ok(data_file);
                            }
                        } else if pattern_parts.len() == 3 {
                            let starts_with = pattern_parts[0];
                            let middle = pattern_parts[1];
                            let ends_with = pattern_parts[2];

                            if file_name_str.starts_with(starts_with)
                                && file_name_str.contains(middle)
                                && file_name_str.ends_with(ends_with)
                            {
                                let data_file = entry.path();
                                println!("✓ Found SSTable data file: {}", data_file.display());
                                return Ok(data_file);
                            }
                        }
                    }
                }
            }
        }

        return Err(anyhow::anyhow!(
            "No SSTable data files found in directory: {}\nLooked for: {}",
            sstable_path.display(),
            patterns.join(", ")
        ));
    }

    Err(anyhow::anyhow!(
        "Path is neither a file nor a directory: {}",
        sstable_path.display()
    ))
}

/// Display results in table format
pub(crate) fn display_table_format(column_names: &[String], rows: &[ParsedRow]) {
    use prettytable::{Cell, Row, Table};

    if rows.is_empty() {
        println!("📭 No results found");
        return;
    }

    let mut table = Table::new();

    // Add header
    let mut header = Row::empty();
    for column in column_names {
        header.add_cell(Cell::new(column));
    }
    table.add_row(header);

    // Add data rows
    for parsed_row in rows {
        let mut row = Row::empty();
        for column in column_names {
            let cell_value = parsed_row
                .get(column)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string());
            row.add_cell(Cell::new(&cell_value));
        }
        table.add_row(row);
    }

    println!("\n📊 Live SSTable Data Results:");
    println!("{}", "=".repeat(50));
    table.printstd();
}

/// Display results in JSON format
pub(crate) fn display_json_format(rows: &[ParsedRow]) -> Result<()> {
    let json_rows: Vec<serde_json::Value> = rows.iter().map(|row| row.to_json()).collect();

    println!("{}", serde_json::to_string_pretty(&json_rows)?);
    Ok(())
}

/// Display results in CSV format
pub(crate) fn display_csv_format(column_names: &[String], rows: &[ParsedRow]) -> Result<()> {
    let mut wtr = csv::Writer::from_writer(std::io::stdout());

    // Write header
    wtr.write_record(column_names)?;

    // Write data rows
    for parsed_row in rows {
        let mut record = Vec::new();
        for column in column_names {
            let cell_value = parsed_row
                .get(column)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string());
            record.push(cell_value);
        }
        wtr.write_record(&record)?;
    }

    wtr.flush()?;
    Ok(())
}
