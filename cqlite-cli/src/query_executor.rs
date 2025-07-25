//! Query executor for running SELECT queries against real SSTable data
//!
//! This module provides functionality to execute CQL SELECT queries directly
//! against SSTable files, showing live table data instead of mocked data.

use anyhow::{Context, Result};
use cqlite_core::{
    schema::TableSchema,
    storage::sstable::{reader::SSTableReader, bulletproof_reader::BulletproofReader},
};
use std::path::Path;
use std::sync::Arc;

use crate::data_parser::{RealDataParser, ParsedRow, ParsedValue};
use crate::formatter::CqlshTableFormatter;

/// Configuration for query executor performance settings
#[derive(Debug, Clone)]
pub struct QueryExecutorConfig {
    /// Pagination configuration
    // pub pagination: PaginationConfig,
    /// Enable streaming processing for large datasets
    pub enable_streaming: bool,
    /// Cache size for frequently accessed data
    pub cache_size_mb: usize,
    /// I/O timeout in milliseconds
    pub io_timeout_ms: u64,
}

impl Default for QueryExecutorConfig {
    fn default() -> Self {
        Self {
            // pagination: PaginationConfig::default(),
            enable_streaming: true,
            cache_size_mb: 50,
            io_timeout_ms: 30000,
        }
    }
}

/// Query executor for running SELECT statements against SSTable data
pub struct QueryExecutor {
    /// Table schema for data interpretation
    schema: TableSchema,
    /// Bulletproof SSTable reader for data access
    reader: BulletproofReader,
    /// Data parser for converting binary data to readable format
    parser: RealDataParser,
    /// Performance configuration
    config: QueryExecutorConfig,
}

/// Query result containing parsed rows
pub struct QueryResult {
    /// Column names in order
    pub columns: Vec<String>,
    /// Parsed rows
    pub rows: Vec<ParsedRow>,
    /// Total rows found before limit
    pub total_rows: usize,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl QueryExecutor {
    /// Create a new query executor
    pub async fn new(sstable_path: &Path, schema: TableSchema) -> Result<Self> {
        Self::with_config(sstable_path, schema, QueryExecutorConfig::default()).await
    }

    /// Create a new query executor with custom configuration
    pub async fn with_config(sstable_path: &Path, schema: TableSchema, executor_config: QueryExecutorConfig) -> Result<Self> {
        let reader = BulletproofReader::open(sstable_path)
            .with_context(|| format!("Failed to open SSTable with bulletproof reader: {}", sstable_path.display()))?;

        let parser = RealDataParser::new(schema.clone());

        Ok(Self {
            schema,
            reader,
            parser,
            config: executor_config,
        })
    }

    /// Execute a SELECT query with streaming for better performance (simplified version)
    pub async fn execute_select_streaming(&mut self, query: &str) -> Result<QueryResult> {
        // For now, just use the regular execute_select method
        // Streaming features will be re-enabled when pagination is implemented
        self.execute_select(query).await
    }

    /// Execute a SELECT query against the SSTable data using bulletproof reader
    pub async fn execute_select(&mut self, query: &str) -> Result<QueryResult> {
        let start_time = std::time::Instant::now();
        
        // Parse the query (simplified - just handle basic SELECT queries)
        let query_info = self.parse_select_query(query)?;
        
        println!("🔍 Executing query against live SSTable data...");
        println!("📝 Query: {}", query);
        println!("📊 Columns requested: {}", query_info.columns.join(", "));
        
        // Get all entries from SSTable using bulletproof reader
        let entries = self.reader.parse_sstable_data()?;
        let mut matching_rows = Vec::new();
        let mut processed = 0;

        println!("📋 Scanning {} entries in SSTable...", entries.len());

        for entry in entries {
            processed += 1;
            
            // Parse the entry using bulletproof reader data
            match self.parser.parse_bulletproof_entry(&entry) {
                Ok(parsed_row) => {
                    // Apply WHERE clause if present
                    if query_info.where_clause.is_none() || self.matches_where(&parsed_row, &query_info.where_clause)? {
                        matching_rows.push(parsed_row);
                    }
                }
                Err(e) => {
                    eprintln!("⚠️  Failed to parse row {}: {}", processed, e);
                }
            }

            // Apply LIMIT if specified
            if let Some(limit) = query_info.limit {
                if matching_rows.len() >= limit {
                    break;
                }
            }
        }

        let execution_time = start_time.elapsed();
        
        println!("✅ Query completed in {:.2}ms", execution_time.as_millis());
        println!("   Processed {} entries, found {} matching rows", processed, matching_rows.len());

        Ok(QueryResult {
            columns: query_info.columns,
            rows: matching_rows,
            total_rows: processed,
            execution_time_ms: execution_time.as_millis() as u64,
        })
    }

    /// Get table schema
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Process entries using streaming for memory efficiency
    /* Temporarily disabled - streaming pending
    async fn process_entries_streaming(
        &self,
        entries: Vec<(u64, Vec<u8>, Vec<u8>)>,
        query_info: &QueryInfo,
        progress: &PaginationProgress,
    ) -> Result<Vec<ParsedRow>> {
        // Create streaming processor
        let processor = StreamingProcessor::new(
            |entry: &(u64, Vec<u8>, Vec<u8>)| -> Result<Option<ParsedRow>> {
                let (_table_id, key, value) = entry;
                
                // Parse the entry
                match self.parser.parse_entry(key, value) {
                    Ok(parsed_row) => {
                        // Apply WHERE clause if present
                        if query_info.where_clause.is_none() || 
                           self.matches_where(&parsed_row, &query_info.where_clause)? {
                            Ok(Some(parsed_row))
                        } else {
                            Ok(None)
                        }
                    }
                    Err(_) => Ok(None), // Skip failed rows
                }
            },
            self.config.pagination.clone(),
        );

        // Process entries in stream
        let results = processor.process_stream(entries).await?;
        
        // Filter out None results and apply LIMIT
        let mut matching_rows: Vec<ParsedRow> = results.into_iter().filter_map(|r| r).collect();
        
        // Apply pagination limits
        let skip = self.config.pagination.skip;
        let limit = query_info.limit.or(self.config.pagination.limit);
        
        if skip > 0 {
            matching_rows = matching_rows.into_iter().skip(skip).collect();
        }
        
        if let Some(limit_count) = limit {
            matching_rows.truncate(limit_count);
        }

        Ok(matching_rows)
    }

    /// Process entries sequentially (fallback method)
    async fn process_entries_sequential(
        &self,
        entries: Vec<(u64, Vec<u8>, Vec<u8>)>,
        query_info: &QueryInfo,
        progress: &PaginationProgress,
    ) -> Result<Vec<ParsedRow>> {
        let mut matching_rows = Vec::new();
        let mut processed = 0;
        let skip = self.config.pagination.skip;
        let limit = query_info.limit.or(self.config.pagination.limit);

        for (_table_id, key, value) in entries {
            progress.update(processed as u64, None);
            processed += 1;

            // Parse the entry
            match self.parser.parse_entry(&key, &value) {
                Ok(parsed_row) => {
                    // Apply WHERE clause if present
                    if query_info.where_clause.is_none() || 
                       self.matches_where(&parsed_row, &query_info.where_clause)? {
                        
                        // Apply skip
                        if matching_rows.len() < skip {
                            continue;
                        }
                        
                        matching_rows.push(parsed_row);
                        
                        // Apply LIMIT if specified
                        if let Some(limit_count) = limit {
                            if matching_rows.len() >= limit_count + skip {
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("⚠️  Failed to parse row {}: {}", processed, e);
                }
            }
        }

        // Remove skipped rows from the result
        if skip > 0 && matching_rows.len() > skip {
            matching_rows = matching_rows.into_iter().skip(skip).collect();
        }

        Ok(matching_rows)
    }
    */ // End streaming methods

    /// Parse a SELECT query (simplified implementation)
    fn parse_select_query(&self, query: &str) -> Result<QueryInfo> {
        let query = query.trim();
        let query_upper = query.to_uppercase();

        if !query_upper.starts_with("SELECT") {
            return Err(anyhow::anyhow!("Only SELECT queries are supported"));
        }

        // Extract columns (simplified parsing)
        let columns = if query_upper.contains("SELECT *") {
            self.parser.get_column_names()
        } else {
            // Try to extract column names between SELECT and FROM
            let select_part = query_upper
                .split("FROM")
                .next()
                .unwrap_or("")
                .strip_prefix("SELECT")
                .unwrap_or("")
                .trim();
            
            if select_part.is_empty() || select_part == "*" {
                self.parser.get_column_names()
            } else {
                select_part
                    .split(',')
                    .map(|col| col.trim().to_lowercase())
                    .collect()
            }
        };

        // Extract WHERE clause (simplified)
        let where_clause = if query_upper.contains("WHERE") {
            let where_part = query
                .split_whitespace()
                .skip_while(|&word| word.to_uppercase() != "WHERE")
                .skip(1)
                .collect::<Vec<_>>()
                .join(" ");
            
            if where_part.is_empty() {
                None
            } else {
                Some(where_part)
            }
        } else {
            None
        };

        // Extract LIMIT (simplified)
        let limit = if query_upper.contains("LIMIT") {
            let limit_part = query
                .split_whitespace()
                .skip_while(|&word| word.to_uppercase() != "LIMIT")
                .nth(1)
                .and_then(|s| s.parse::<usize>().ok());
            limit_part
        } else {
            None
        };

        Ok(QueryInfo {
            columns,
            where_clause,
            limit,
        })
    }

    /* Temporarily disabled - scanner integration pending
    /// Create a ScanFilter from a WHERE clause
    fn create_scan_filter(&self, where_clause: &Option<String>) -> Result<ScanFilter> {
        match where_clause {
            None => {
                // Empty filter that matches everything
                Ok(ScanFilter {
                    column_filters: std::collections::HashMap::new(),
                    operator: FilterOperator::And,
                })
            }
            Some(clause) => {
                use std::collections::HashMap;
                let mut column_filters = HashMap::new();
                
                // Very simplified WHERE clause parsing
                if clause.contains('=') {
                    let parts: Vec<&str> = clause.split('=').collect();
                    if parts.len() == 2 {
                        let column_name = parts[0].trim().to_lowercase();
                        let expected_value = parts[1].trim().trim_matches('\'').trim_matches('"');
                        
                        // Try to parse the value as appropriate type
                        let parsed_value = if let Ok(int_val) = expected_value.parse::<i64>() {
                            ParsedValue::Integer(int_val)
                        } else if let Ok(float_val) = expected_value.parse::<f64>() {
                            ParsedValue::Float(float_val)
                        } else if expected_value.eq_ignore_ascii_case("true") {
                            ParsedValue::Boolean(true)
                        } else if expected_value.eq_ignore_ascii_case("false") {
                            ParsedValue::Boolean(false)
                        } else if expected_value.eq_ignore_ascii_case("null") {
                            ParsedValue::Null
                        } else {
                            ParsedValue::Text(expected_value.to_string())
                        };
                        
                        column_filters.insert(column_name, ColumnFilter {
                            operator: ComparisonOperator::Equals,
                            value: parsed_value,
                        });
                    }
                }
                
                Ok(ScanFilter {
                    column_filters,
                    operator: FilterOperator::And,
                })
            }
        }
    }
    */ // End scanner integration

    /// Check if a row matches the WHERE clause (simplified implementation)
    fn matches_where(&self, row: &ParsedRow, where_clause: &Option<String>) -> Result<bool> {
        match where_clause {
            None => Ok(true),
            Some(clause) => {
                // Very simplified WHERE clause evaluation
                // In a real implementation, you'd parse this properly
                
                if clause.contains('=') {
                    let parts: Vec<&str> = clause.split('=').collect();
                    if parts.len() == 2 {
                        let column_name = parts[0].trim().to_lowercase();
                        let expected_value = parts[1].trim().trim_matches('\'').trim_matches('"');
                        
                        if let Some(actual_value) = row.get(&column_name) {
                            return Ok(actual_value.to_string() == expected_value);
                        }
                    }
                }
                
                // If we can't parse the WHERE clause, just return true
                println!("⚠️  Complex WHERE clauses not yet supported: {}", clause);
                Ok(true)
            }
        }
    }
}

/// Parsed query information
#[derive(Debug)]
struct QueryInfo {
    /// Columns to select
    columns: Vec<String>,
    /// WHERE clause if present
    where_clause: Option<String>,
    /// LIMIT if specified
    limit: Option<usize>,
}

impl QueryResult {
    /// Display the results in table format (cqlsh-compatible)
    pub fn display_table(&self) {
        let mut formatter = CqlshTableFormatter::new();
        formatter.set_headers(self.columns.clone());
        
        // Convert ParsedRow to Vec<Vec<String>>
        let string_rows: Vec<Vec<String>> = self.rows.iter()
            .map(|row| row.to_string_vec(&self.columns))
            .collect();
        formatter.add_rows(string_rows);
        
        let table_output = formatter.format();
        
        println!("{}", table_output);
        println!("✅ Query completed in {}ms", self.execution_time_ms);
    }

    /// Display the results in JSON format
    pub fn display_json(&self) -> Result<()> {
        let json_rows: Vec<serde_json::Value> = self.rows
            .iter()
            .map(|row| row.to_json())
            .collect();

        println!("{}", serde_json::to_string_pretty(&json_rows)?);
        println!("\n✅ {} rows returned in {}ms", self.rows.len(), self.execution_time_ms);
        
        Ok(())
    }

    /// Display the results in CSV format
    pub fn display_csv(&self) -> Result<()> {
        let mut wtr = csv::Writer::from_writer(std::io::stdout());

        // Write header
        wtr.write_record(&self.columns)?;

        // Write data rows
        for parsed_row in &self.rows {
            let mut record = Vec::new();
            for column in &self.columns {
                let cell_value = parsed_row
                    .get(column)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string());
                record.push(cell_value);
            }
            wtr.write_record(&record)?;
        }

        wtr.flush()?;
        eprintln!("\n✅ {} rows returned in {}ms", self.rows.len(), self.execution_time_ms);
        
        Ok(())
    }
}