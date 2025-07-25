//! Output handling and normalization for query results

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents the output of a query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOutput {
    /// Raw output as received from the system
    pub raw_output: String,
    
    /// Standard error output
    pub stderr: String,
    
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    
    /// Output format (table, json, csv, etc.)
    pub format: String,
    
    /// Parsed rows of data
    pub rows: Vec<Vec<String>>,
    
    /// Column headers if available
    pub column_headers: Vec<String>,
    
    /// Number of rows returned
    pub row_count: Option<usize>,
    
    /// JSON representation if available
    pub json_data: Option<serde_json::Value>,
    
    /// Metadata about the query execution
    pub metadata: HashMap<String, String>,
    
    /// Any warnings or notices
    pub warnings: Vec<String>,
}

impl Default for QueryOutput {
    fn default() -> Self {
        Self {
            raw_output: String::new(),
            stderr: String::new(),
            execution_time_ms: 0,
            format: "unknown".to_string(),
            rows: Vec::new(),
            column_headers: Vec::new(),
            row_count: None,
            json_data: None,
            metadata: HashMap::new(),
            warnings: Vec::new(),
        }
    }
}

impl QueryOutput {
    /// Create a new empty query output
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create a query output with error
    pub fn error(message: &str) -> Self {
        let mut output = Self::default();
        output.stderr = message.to_string();
        output.metadata.insert("error".to_string(), message.to_string());
        output
    }

    /// Normalize the output for comparison
    pub fn normalize(&mut self, config: &crate::config::ComparisonConfig) {
        // Normalize whitespace
        if config.ignore_whitespace {
            self.normalize_whitespace();
        }

        // Normalize case
        if config.ignore_case {
            self.normalize_case();
        }

        // Normalize timestamps
        if config.ignore_timestamps {
            self.normalize_timestamps(config.timestamp_tolerance_ms);
        }

        // Normalize UUIDs
        if config.normalize_uuids {
            self.normalize_uuids();
        }

        // Normalize numeric values
        if config.numeric_precision_tolerance > 0.0 {
            self.normalize_numeric_precision(config.numeric_precision_tolerance);
        }

        // Sort rows if order should be ignored
        if config.ignore_row_order {
            self.sort_rows();
        }

        // Sort columns if order should be ignored
        if config.ignore_column_order {
            self.sort_columns();
        }
    }

    /// Normalize whitespace in all text fields
    fn normalize_whitespace(&mut self) {
        self.raw_output = normalize_whitespace_string(&self.raw_output);
        
        for row in &mut self.rows {
            for cell in row {
                *cell = normalize_whitespace_string(cell);
            }
        }

        for header in &mut self.column_headers {
            *header = normalize_whitespace_string(header);
        }
    }

    /// Convert to lowercase for case-insensitive comparison
    fn normalize_case(&mut self) {
        self.raw_output = self.raw_output.to_lowercase();
        
        for row in &mut self.rows {
            for cell in row {
                *cell = cell.to_lowercase();
            }
        }

        for header in &mut self.column_headers {
            *header = header.to_lowercase();
        }
    }

    /// Normalize timestamp representations
    fn normalize_timestamps(&mut self, _tolerance_ms: u64) {
        use regex::Regex;
        
        // Patterns for common timestamp formats
        let timestamp_patterns = vec![
            // ISO 8601 with timezone
            Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?[+-]\d{4}").unwrap(),
            // ISO 8601 UTC
            Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z").unwrap(),
            // Simple date-time
            Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3})?").unwrap(),
            // TimeUUID (Cassandra)
            Regex::new(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-1[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}").unwrap(),
        ];

        for row in &mut self.rows {
            for cell in row {
                for pattern in &timestamp_patterns {
                    *cell = pattern.replace_all(cell, "NORMALIZED_TIMESTAMP").to_string();
                }
            }
        }

        for pattern in &timestamp_patterns {
            self.raw_output = pattern.replace_all(&self.raw_output, "NORMALIZED_TIMESTAMP").to_string();
        }
    }

    /// Normalize UUID representations
    fn normalize_uuids(&mut self) {
        use regex::Regex;
        
        // Standard UUID pattern
        let uuid_pattern = Regex::new(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}").unwrap();

        for row in &mut self.rows {
            for cell in row {
                *cell = uuid_pattern.replace_all(cell, "NORMALIZED_UUID").to_string();
            }
        }

        self.raw_output = uuid_pattern.replace_all(&self.raw_output, "NORMALIZED_UUID").to_string();
    }

    /// Normalize numeric precision
    fn normalize_numeric_precision(&mut self, tolerance: f64) {
        for row in &mut self.rows {
            for cell in row {
                if let Ok(num) = cell.parse::<f64>() {
                    // Round to tolerance precision
                    let precision = (-tolerance.log10()) as usize;
                    *cell = format!("{:.precision$}", num, precision = precision);
                }
            }
        }
    }

    /// Sort rows for order-independent comparison
    fn sort_rows(&mut self) {
        if !self.rows.is_empty() {
            // Keep header row separate if present
            if !self.column_headers.is_empty() && self.rows.len() > 1 {
                let mut data_rows = self.rows[1..].to_vec();
                data_rows.sort();
                self.rows = [vec![self.rows[0].clone()], data_rows].concat();
            } else {
                self.rows.sort();
            }
        }
    }

    /// Sort columns for order-independent comparison  
    fn sort_columns(&mut self) {
        if self.rows.is_empty() || self.column_headers.is_empty() {
            return;
        }

        // Create column index mapping
        let mut indexed_headers: Vec<(usize, String)> = self.column_headers
            .iter()
            .enumerate()
            .map(|(i, h)| (i, h.clone()))
            .collect();
        
        indexed_headers.sort_by(|a, b| a.1.cmp(&b.1));
        
        let sorted_indices: Vec<usize> = indexed_headers.iter().map(|(i, _)| *i).collect();
        
        // Reorder headers
        self.column_headers = indexed_headers.into_iter().map(|(_, h)| h).collect();
        
        // Reorder data in all rows
        for row in &mut self.rows {
            let original_row = row.clone();
            for (new_pos, &original_pos) in sorted_indices.iter().enumerate() {
                if original_pos < original_row.len() && new_pos < row.len() {
                    row[new_pos] = original_row[original_pos].clone();
                }
            }
        }
    }

    /// Get a summary of the output
    pub fn summary(&self) -> OutputSummary {
        OutputSummary {
            format: self.format.clone(),
            row_count: self.row_count.unwrap_or(self.rows.len()),
            column_count: if self.column_headers.is_empty() {
                self.rows.first().map(|r| r.len()).unwrap_or(0)
            } else {
                self.column_headers.len()
            },
            execution_time_ms: self.execution_time_ms,
            has_errors: !self.stderr.is_empty(),
            warning_count: self.warnings.len(),
        }
    }

    /// Check if output indicates an error
    pub fn has_error(&self) -> bool {
        !self.stderr.is_empty() || self.metadata.contains_key("error")
    }

    /// Extract column names from the first row if headers are not set
    pub fn extract_headers_from_first_row(&mut self) {
        if self.column_headers.is_empty() && !self.rows.is_empty() {
            self.column_headers = self.rows[0].clone();
            self.rows.remove(0);
        }
    }

    /// Convert to a standardized JSON representation
    pub fn to_standard_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();
        
        result.insert("format".to_string(), serde_json::Value::String(self.format.clone()));
        result.insert("execution_time_ms".to_string(), serde_json::Value::Number(self.execution_time_ms.into()));
        result.insert("row_count".to_string(), serde_json::Value::Number(self.rows.len().into()));
        
        if !self.column_headers.is_empty() {
            result.insert("headers".to_string(), serde_json::json!(self.column_headers));
        }
        
        result.insert("data".to_string(), serde_json::json!(self.rows));
        
        if !self.warnings.is_empty() {
            result.insert("warnings".to_string(), serde_json::json!(self.warnings));
        }
        
        if self.has_error() {
            result.insert("error".to_string(), serde_json::Value::String(self.stderr.clone()));
        }
        
        serde_json::Value::Object(result)
    }
}

/// Summary information about query output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputSummary {
    pub format: String,
    pub row_count: usize,
    pub column_count: usize,
    pub execution_time_ms: u64,
    pub has_errors: bool,
    pub warning_count: usize,
}

/// Normalize whitespace in a string
fn normalize_whitespace_string(s: &str) -> String {
    s.split_whitespace().collect::<Vec<&str>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ComparisonConfig;

    #[test]
    fn test_normalize_whitespace() {
        let mut output = QueryOutput {
            raw_output: "  hello    world  ".to_string(),
            rows: vec![vec!["  test  data  ".to_string()]],
            ..Default::default()
        };

        let config = ComparisonConfig {
            ignore_whitespace: true,
            ..Default::default()
        };

        output.normalize(&config);
        assert_eq!(output.raw_output, "hello world");
        assert_eq!(output.rows[0][0], "test data");
    }

    #[test]
    fn test_normalize_case() {
        let mut output = QueryOutput {
            raw_output: "Hello World".to_string(),
            rows: vec![vec!["Test Data".to_string()]],
            column_headers: vec!["Column Name".to_string()],
            ..Default::default()
        };

        let config = ComparisonConfig {
            ignore_case: true,
            ..Default::default()
        };

        output.normalize(&config);
        assert_eq!(output.raw_output, "hello world");
        assert_eq!(output.rows[0][0], "test data");
        assert_eq!(output.column_headers[0], "column name");
    }

    #[test]
    fn test_normalize_uuids() {
        let mut output = QueryOutput {
            rows: vec![vec!["550e8400-e29b-41d4-a716-446655440000".to_string()]],
            ..Default::default()
        };

        let config = ComparisonConfig {
            normalize_uuids: true,
            ..Default::default()
        };

        output.normalize(&config);
        assert_eq!(output.rows[0][0], "NORMALIZED_UUID");
    }

    #[test]
    fn test_sort_rows() {
        let mut output = QueryOutput {
            rows: vec![
                vec!["c".to_string()],
                vec!["a".to_string()],
                vec!["b".to_string()],
            ],
            ..Default::default()
        };

        let config = ComparisonConfig {
            ignore_row_order: true,
            ..Default::default()
        };

        output.normalize(&config);
        assert_eq!(output.rows[0][0], "a");
        assert_eq!(output.rows[1][0], "b");
        assert_eq!(output.rows[2][0], "c");
    }

    #[test]
    fn test_output_summary() {
        let output = QueryOutput {
            format: "table".to_string(),
            rows: vec![vec!["test".to_string()]],
            column_headers: vec!["col1".to_string()],
            execution_time_ms: 100,
            warnings: vec!["warning".to_string()],
            ..Default::default()
        };

        let summary = output.summary();
        assert_eq!(summary.format, "table");
        assert_eq!(summary.row_count, 1);
        assert_eq!(summary.column_count, 1);
        assert_eq!(summary.execution_time_ms, 100);
        assert_eq!(summary.warning_count, 1);
    }
}