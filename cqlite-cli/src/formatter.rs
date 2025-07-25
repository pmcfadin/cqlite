//! CQLite table formatter for cqlsh-compatible output
//!
//! This module implements table formatting that exactly matches cqlsh output format,
//! based on the comprehensive research in CQLSH_FORMAT_SPECIFICATION.md

use cqlite_core::storage::sstable::bulletproof_reader::SSTableEntry;


/// Constants for cqlsh-compatible formatting
pub const COLUMN_SEPARATOR: &str = " | ";
pub const HEADER_BORDER_CHAR: char = '-';
pub const HEADER_SEPARATOR_JUNCTION: &str = "-+-";
pub const ROW_PREFIX: &str = " ";

/// Table formatter for cqlsh-compatible output
pub struct CqlshTableFormatter {
    pub column_headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub show_row_count: bool,
    pub color_support: bool,
}

impl Default for CqlshTableFormatter {
    fn default() -> Self {
        Self {
            column_headers: Vec::new(),
            rows: Vec::new(),
            show_row_count: true,
            color_support: false,
        }
    }
}

impl CqlshTableFormatter {
    /// Create a new formatter
    pub fn new() -> Self {
        Self::default()
    }

    /// Set column headers
    pub fn set_headers(&mut self, headers: Vec<String>) {
        self.column_headers = headers;
    }

    /// Add a data row
    pub fn add_row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    /// Add multiple rows
    pub fn add_rows(&mut self, rows: Vec<Vec<String>>) {
        self.rows.extend(rows);
    }

    /// Convert SSTable entries to formatted table
    pub fn from_sstable_entries(&mut self, entries: &[SSTableEntry], table_name: &str) {
        // Set default headers based on known schema
        self.column_headers = vec!["id".to_string(), "data".to_string()];
        
        // Convert entries to rows
        for entry in entries {
            let mut row = Vec::new();
            
            // Add partition key (usually UUID)
            row.push(entry.partition_key.clone());
            
            // Add format info as a simple representation of the data
            row.push(entry.format_info.clone());
            
            // Ensure row has correct number of columns
            while row.len() < self.column_headers.len() {
                row.push(String::new());
            }
            
            self.rows.push(row);
        }
        
        println!("📊 Formatted {} entries from {} into table format", entries.len(), table_name);
    }

    /// Calculate optimal column widths (cqlsh algorithm)
    fn calculate_column_widths(&self) -> Vec<usize> {
        let column_count = self.column_headers.len().max(
            self.rows.first().map(|r| r.len()).unwrap_or(0)
        );
        
        let mut widths = vec![0; column_count];
        
        // Start with header widths
        for (i, header) in self.column_headers.iter().enumerate() {
            if i < widths.len() {
                widths[i] = header.chars().count();
            }
        }
        
        // Expand based on data content
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(cell.chars().count());
                }
            }
        }
        
        widths
    }

    /// Format as cqlsh-compatible table
    pub fn format(&self) -> String {
        if self.rows.is_empty() && self.column_headers.is_empty() {
            return String::new();
        }

        let widths = self.calculate_column_widths();
        let mut result = String::new();

        // Format headers (left-aligned)
        if !self.column_headers.is_empty() {
            result.push_str(ROW_PREFIX);
            for (i, header) in self.column_headers.iter().enumerate() {
                if i > 0 {
                    result.push_str(COLUMN_SEPARATOR);
                }
                let width = widths.get(i).copied().unwrap_or(header.len());
                result.push_str(&format!("{:<width$}", header, width = width));
            }
            result.push('\n');

            // Format separator line
            result.push_str(&self.format_separator_line(&widths));
            result.push('\n');
        }

        // Format data rows (right-aligned)
        for row in &self.rows {
            result.push_str(ROW_PREFIX);
            for (i, cell) in row.iter().enumerate() {
                if i > 0 {
                    result.push_str(COLUMN_SEPARATOR);
                }
                let width = widths.get(i).copied().unwrap_or(cell.len());
                result.push_str(&format!("{:>width$}", cell, width = width));
            }
            result.push('\n');
        }

        // Add row count summary
        if self.show_row_count && !self.rows.is_empty() {
            result.push('\n');
            result.push_str(&format!("({} rows)", self.rows.len()));
        }

        result
    }

    /// Format the separator line between headers and data
    fn format_separator_line(&self, widths: &[usize]) -> String {
        let mut separator = String::new();
        separator.push(HEADER_BORDER_CHAR);
        
        for (i, &width) in widths.iter().enumerate() {
            if i > 0 {
                separator.push_str(HEADER_SEPARATOR_JUNCTION);
            }
            separator.push_str(&HEADER_BORDER_CHAR.to_string().repeat(width));
        }
        
        separator.push(HEADER_BORDER_CHAR);
        separator
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.column_headers.clear();
        self.rows.clear();
    }

    /// Get row count
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get column count
    pub fn column_count(&self) -> usize {
        self.column_headers.len()
    }

    /// Check if table is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.column_headers.is_empty()
    }

    /// Format as JSON for API compatibility
    pub fn format_as_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();
        
        result.insert("format".to_string(), serde_json::Value::String("table".to_string()));
        result.insert("headers".to_string(), serde_json::json!(self.column_headers));
        result.insert("rows".to_string(), serde_json::json!(self.rows));
        result.insert("row_count".to_string(), serde_json::Value::Number(self.rows.len().into()));
        
        serde_json::Value::Object(result)
    }

    /// Create formatter from JSON data
    pub fn from_json(value: &serde_json::Value) -> Result<Self, String> {
        let mut formatter = Self::new();
        
        if let Some(headers) = value.get("headers").and_then(|h| h.as_array()) {
            formatter.column_headers = headers.iter()
                .filter_map(|h| h.as_str())
                .map(|s| s.to_string())
                .collect();
        }
        
        if let Some(rows) = value.get("rows").and_then(|r| r.as_array()) {
            for row in rows {
                if let Some(row_array) = row.as_array() {
                    let row_data: Vec<String> = row_array.iter()
                        .filter_map(|cell| cell.as_str())
                        .map(|s| s.to_string())
                        .collect();
                    formatter.rows.push(row_data);
                }
            }
        }
        
        Ok(formatter)
    }

    /// Apply data type specific formatting
    pub fn format_cell_value(&self, value: &str, column_name: &str) -> String {
        // Handle special formatting based on column type/name
        match column_name.to_lowercase().as_str() {
            "id" | "uuid" => {
                // UUID values should be lowercase
                if self.is_uuid_like(value) {
                    value.to_lowercase()
                } else {
                    value.to_string()
                }
            }
            "timestamp" | "created_at" | "updated_at" => {
                // Timestamp formatting (keep as-is for now)
                value.to_string()
            }
            _ => {
                // Default formatting
                value.to_string()
            }
        }
    }

    /// Check if a value looks like a UUID
    fn is_uuid_like(&self, value: &str) -> bool {
        value.len() == 36 && 
        value.chars().filter(|&c| c == '-').count() == 4 &&
        value.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
    }

    /// Set color support
    pub fn set_color_support(&mut self, enabled: bool) {
        self.color_support = enabled;
    }

    /// Enable/disable row count display
    pub fn set_show_row_count(&mut self, show: bool) {
        self.show_row_count = show;
    }
}

/// Utility function to format SSTable entries for display
pub fn format_sstable_entries_as_table(entries: &[SSTableEntry], table_name: &str) -> String {
    let mut formatter = CqlshTableFormatter::new();
    formatter.from_sstable_entries(entries, table_name);
    formatter.format()
}

/// Format data for cqlsh comparison
pub fn format_for_cqlsh_comparison(entries: &[SSTableEntry]) -> String {
    let mut formatter = CqlshTableFormatter::new();
    formatter.set_headers(vec!["id".to_string(), "data".to_string()]);
    
    for entry in entries {
        let mut row = vec![entry.partition_key.clone()];
        
        // Add format info as data representation
        if entry.format_info.is_empty() {
            row.push(String::new());
        } else {
            row.push(entry.format_info.clone());
        }
        
        formatter.add_row(row);
    }
    
    formatter.format()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_table_formatting() {
        let mut formatter = CqlshTableFormatter::new();
        formatter.set_headers(vec!["id".to_string(), "name".to_string()]);
        formatter.add_row(vec!["1".to_string(), "John".to_string()]);
        formatter.add_row(vec!["2".to_string(), "Jane".to_string()]);

        let output = formatter.format();
        assert!(output.contains("id | name"));
        assert!(output.contains("---+-----"));
        assert!(output.contains("(2 rows)"));
    }

    #[test]
    fn test_column_width_calculation() {
        let mut formatter = CqlshTableFormatter::new();
        formatter.set_headers(vec!["short".to_string(), "very_long_header".to_string()]);
        formatter.add_row(vec!["test".to_string(), "x".to_string()]);

        let widths = formatter.calculate_column_widths();
        assert_eq!(widths[0], 5); // "short".len()
        assert_eq!(widths[1], 16); // "very_long_header".len()
    }

    #[test]
    fn test_right_aligned_data() {
        let mut formatter = CqlshTableFormatter::new();
        formatter.set_headers(vec!["id".to_string()]);
        formatter.add_row(vec!["123".to_string()]);

        let output = formatter.format();
        // Should be right-aligned: " id\n----\n 123"
        let lines: Vec<&str> = output.lines().collect();
        assert!(lines.len() >= 3);
        // Data should be right-aligned
        assert!(lines[2].ends_with("123"));
    }

    #[test]
    fn test_empty_table() {
        let formatter = CqlshTableFormatter::new();
        let output = formatter.format();
        assert!(output.is_empty());
    }

    #[test]
    fn test_uuid_formatting() {
        let formatter = CqlshTableFormatter::new();
        let uuid = "A8F167F0-EBE7-4F20-A386-31FF138BEC3B";
        let formatted = formatter.format_cell_value(uuid, "id");
        assert_eq!(formatted, "a8f167f0-ebe7-4f20-a386-31ff138bec3b");
    }

    #[test]
    fn test_json_conversion() {
        let mut formatter = CqlshTableFormatter::new();
        formatter.set_headers(vec!["id".to_string(), "name".to_string()]);
        formatter.add_row(vec!["1".to_string(), "John".to_string()]);

        let json = formatter.format_as_json();
        assert!(json.get("headers").is_some());
        assert!(json.get("rows").is_some());
        assert!(json.get("row_count").is_some());
    }
}