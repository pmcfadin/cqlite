use anyhow::{Result, anyhow};
use std::path::Path;
use tokio::fs;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use regex::Regex;
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedData {
    pub keyspace: String,
    pub table: String,
    pub partitions: Vec<ParsedPartition>,
    pub metadata: DumpMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedPartition {
    pub partition_key: String,
    pub rows: Vec<ParsedRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedRow {
    pub clustering_key: Option<String>,
    pub cells: Vec<ParsedCell>,
    pub timestamp: Option<i64>,
    pub ttl: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedCell {
    pub column_name: String,
    pub value: CellValue,
    pub timestamp: i64,
    pub ttl: Option<i32>,
    pub deletion_info: Option<DeletionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CellValue {
    Text(String),
    Integer(i64),
    Boolean(bool),
    Float(f64),
    Uuid(String),
    Timestamp(i64),
    Blob(String), // hex-encoded
    Null,
    Collection {
        collection_type: String,
        elements: Vec<CellValue>,
    },
    UserDefinedType {
        type_name: String,
        fields: HashMap<String, CellValue>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionInfo {
    pub marked_for_deletion_at: i64,
    pub local_deletion_time: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpMetadata {
    pub format_version: String,
    pub generation: Option<u32>,
    pub sstable_level: Option<u32>,
    pub created_at: Option<String>,
    pub row_count: Option<u64>,
    pub data_size: Option<u64>,
}

pub struct SstableDumpParser {
    // Regex patterns for parsing different dump formats
    cassandra_patterns: CassandraPatterns,
    cqlite_patterns: CqlitePatterns,
}

struct CassandraPatterns {
    partition_start: Regex,
    row_start: Regex,
    cell_pattern: Regex,
    metadata_pattern: Regex,
    timestamp_pattern: Regex,
    ttl_pattern: Regex,
}

struct CqlitePatterns {
    partition_start: Regex,
    row_start: Regex,
    cell_pattern: Regex,
    metadata_pattern: Regex,
    // CQLite might have different output format
}

impl SstableDumpParser {
    pub fn new() -> Self {
        Self {
            cassandra_patterns: CassandraPatterns::new(),
            cqlite_patterns: CqlitePatterns::new(),
        }
    }
    
    /// Parse Cassandra's sstabledump output
    pub async fn parse_cassandra_dump(&self, dump_path: &Path) -> Result<ParsedData> {
        debug!("Parsing Cassandra dump: {:?}", dump_path);
        
        let content = fs::read_to_string(dump_path).await?;
        let lines: Vec<&str> = content.lines().collect();
        
        let mut parsed = ParsedData {
            keyspace: String::new(),
            table: String::new(),
            partitions: Vec::new(),
            metadata: DumpMetadata::default(),
        };
        
        let mut current_partition: Option<ParsedPartition> = None;
        let mut current_row: Option<ParsedRow> = None;
        let mut line_idx = 0;
        
        while line_idx < lines.len() {
            let line = lines[line_idx].trim();
            
            // Skip empty lines
            if line.is_empty() {
                line_idx += 1;
                continue;
            }
            
            // Parse metadata (table info, etc.)
            if let Some(metadata) = self.parse_cassandra_metadata(line) {
                parsed.metadata = metadata;
                if let Some(captures) = self.cassandra_patterns.metadata_pattern.captures(line) {
                    if let Some(ks) = captures.get(1) {
                        parsed.keyspace = ks.as_str().to_string();
                    }
                    if let Some(table) = captures.get(2) {
                        parsed.table = table.as_str().to_string();
                    }
                }
            }
            
            // Parse partition start
            else if let Some(captures) = self.cassandra_patterns.partition_start.captures(line) {
                // Save previous partition
                if let Some(partition) = current_partition.take() {
                    parsed.partitions.push(partition);
                }
                
                let partition_key = captures.get(1)
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                
                current_partition = Some(ParsedPartition {
                    partition_key,
                    rows: Vec::new(),
                });
            }
            
            // Parse row start
            else if let Some(captures) = self.cassandra_patterns.row_start.captures(line) {
                // Save previous row
                if let Some(mut row) = current_row.take() {
                    if let Some(ref mut partition) = current_partition {
                        partition.rows.push(row);
                    }
                }
                
                let clustering_key = captures.get(1).map(|m| m.as_str().to_string());
                
                current_row = Some(ParsedRow {
                    clustering_key,
                    cells: Vec::new(),
                    timestamp: None,
                    ttl: None,
                });
            }
            
            // Parse cell data
            else if let Some(cell) = self.parse_cassandra_cell(line) {
                if let Some(ref mut row) = current_row {
                    row.cells.push(cell);
                }
            }
            
            line_idx += 1;
        }
        
        // Save final partition and row
        if let Some(row) = current_row {
            if let Some(ref mut partition) = current_partition {
                partition.rows.push(row);
            }
        }
        if let Some(partition) = current_partition {
            parsed.partitions.push(partition);
        }
        
        debug!("Parsed {} partitions from Cassandra dump", parsed.partitions.len());
        Ok(parsed)
    }
    
    /// Parse CQLite's dump output
    pub async fn parse_cqlite_dump(&self, dump_path: &Path) -> Result<ParsedData> {
        debug!("Parsing CQLite dump: {:?}", dump_path);
        
        let content = fs::read_to_string(dump_path).await?;
        
        // For now, assume CQLite outputs JSON format
        // This would need to be adapted based on actual CQLite output format
        if content.trim_start().starts_with('{') {
            // JSON format
            let parsed: ParsedData = serde_json::from_str(&content)?;
            Ok(parsed)
        } else {
            // Text format similar to Cassandra
            self.parse_cqlite_text_format(&content).await
        }
    }
    
    async fn parse_cqlite_text_format(&self, content: &str) -> Result<ParsedData> {
        // Similar parsing logic to Cassandra but adapted for CQLite format
        // This is a placeholder - actual implementation would depend on CQLite output format
        
        let mut parsed = ParsedData {
            keyspace: "cqlite_output".to_string(),
            table: "parsed_table".to_string(),
            partitions: Vec::new(),
            metadata: DumpMetadata::default(),
        };
        
        // Parse CQLite-specific format here
        // For now, return empty structure
        Ok(parsed)
    }
    
    fn parse_cassandra_metadata(&self, line: &str) -> Option<DumpMetadata> {
        // Parse Cassandra metadata lines
        if line.contains("SSTable") || line.contains("Generation") {
            Some(DumpMetadata::default())
        } else {
            None
        }
    }
    
    fn parse_cassandra_cell(&self, line: &str) -> Option<ParsedCell> {
        if let Some(captures) = self.cassandra_patterns.cell_pattern.captures(line) {
            let column_name = captures.get(1)?.as_str().to_string();
            let value_str = captures.get(2)?.as_str();
            let timestamp_str = captures.get(3)?.as_str();
            
            let value = self.parse_cell_value(value_str, &column_name);
            let timestamp = timestamp_str.parse().unwrap_or(0);
            
            Some(ParsedCell {
                column_name,
                value,
                timestamp,
                ttl: None,
                deletion_info: None,
            })
        } else {
            None
        }
    }
    
    fn parse_cell_value(&self, value_str: &str, _column_name: &str) -> CellValue {
        // Parse different cell value types
        if value_str == "null" || value_str.is_empty() {
            return CellValue::Null;
        }
        
        // Try to infer type from value format
        if value_str.starts_with('"') && value_str.ends_with('"') {
            CellValue::Text(value_str[1..value_str.len()-1].to_string())
        } else if let Ok(int_val) = value_str.parse::<i64>() {
            CellValue::Integer(int_val)
        } else if let Ok(float_val) = value_str.parse::<f64>() {
            CellValue::Float(float_val)
        } else if value_str == "true" || value_str == "false" {
            CellValue::Boolean(value_str == "true")
        } else if value_str.len() == 36 && value_str.chars().nth(8) == Some('-') {
            // UUID format
            CellValue::Uuid(value_str.to_string())
        } else if value_str.starts_with("0x") {
            // Blob/hex format
            CellValue::Blob(value_str[2..].to_string())
        } else {
            // Default to text
            CellValue::Text(value_str.to_string())
        }
    }
}

impl CassandraPatterns {
    fn new() -> Self {
        Self {
            partition_start: Regex::new(r"Partition: (.+)").unwrap(),
            row_start: Regex::new(r"Row: (.*)").unwrap(),
            cell_pattern: Regex::new(r"([^:]+): (.+) @ (\d+)").unwrap(),
            metadata_pattern: Regex::new(r"([^.]+)\.([^.]+)").unwrap(),
            timestamp_pattern: Regex::new(r"@ (\d+)").unwrap(),
            ttl_pattern: Regex::new(r"ttl (\d+)").unwrap(),
        }
    }
}

impl CqlitePatterns {
    fn new() -> Self {
        Self {
            partition_start: Regex::new(r"Partition: (.+)").unwrap(),
            row_start: Regex::new(r"Row: (.*)").unwrap(),
            cell_pattern: Regex::new(r"([^:]+): (.+) @ (\d+)").unwrap(),
            metadata_pattern: Regex::new(r"([^.]+)\.([^.]+)").unwrap(),
        }
    }
}

impl Default for DumpMetadata {
    fn default() -> Self {
        Self {
            format_version: "unknown".to_string(),
            generation: None,
            sstable_level: None,
            created_at: None,
            row_count: None,
            data_size: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;
    
    #[tokio::test]
    async fn test_parse_cassandra_cell() {
        let parser = SstableDumpParser::new();
        let cell = parser.parse_cassandra_cell("name: \"John Doe\" @ 1234567890");
        assert!(cell.is_some());
        
        let cell = cell.unwrap();
        assert_eq!(cell.column_name, "name");
        assert_eq!(cell.timestamp, 1234567890);
    }
    
    #[test]
    fn test_parse_cell_values() {
        let parser = SstableDumpParser::new();
        
        // Test text value
        let text_val = parser.parse_cell_value("\"Hello World\"", "text_col");
        matches!(text_val, CellValue::Text(s) if s == "Hello World");
        
        // Test integer value
        let int_val = parser.parse_cell_value("42", "int_col");
        matches!(int_val, CellValue::Integer(42));
        
        // Test null value
        let null_val = parser.parse_cell_value("null", "nullable_col");
        matches!(null_val, CellValue::Null);
    }
}