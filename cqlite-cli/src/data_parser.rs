//! Real data parser for converting SSTable binary data to human-readable format
//!
//! This module handles the conversion of raw SSTable binary data into properly
//! formatted CQL values that can be displayed in the CLI.

use anyhow::{Context, Result};
use cqlite_core::{
    parser::types::{parse_cql_value, CqlTypeId},
    schema::{TableSchema, Column},
    storage::sstable::bulletproof_reader::SSTableEntry,
    types::{Value, RowKey},
    Error,
};
use serde_json;
use std::collections::HashMap;

/// Parsed row data with human-readable values
#[derive(Debug, Clone)]
pub struct ParsedRow {
    /// Column name to value mapping
    pub columns: HashMap<String, ParsedValue>,
    /// Original row key
    pub row_key: RowKey,
}

impl ParsedRow {
    /// Convert to a vector of strings for table display
    pub fn to_string_vec(&self, column_order: &[String]) -> Vec<String> {
        column_order.iter().map(|col_name| {
            self.columns.get(col_name)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        }).collect()
    }
}

/// Human-readable parsed value
#[derive(Debug, Clone)]
pub enum ParsedValue {
    /// Text/varchar values
    Text(String),
    /// Integer values (int, bigint, smallint, tinyint)
    Integer(i64),
    /// Floating point values (float, double)
    Float(f64),
    /// Boolean values
    Boolean(bool),
    /// UUID values
    Uuid(String),
    /// Timestamp values (formatted as ISO string)
    Timestamp(String),
    /// Binary data (base64 encoded)
    Blob(String),
    /// Collection values (list, set, map)
    Collection(Vec<ParsedValue>),
    /// Map values
    Map(HashMap<String, ParsedValue>),
    /// NULL values
    Null,
    /// Unparseable values (with error info)
    Unparseable(String),
}

impl std::fmt::Display for ParsedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParsedValue::Text(s) => write!(f, "{}", s),
            ParsedValue::Integer(i) => write!(f, "{}", i),
            ParsedValue::Float(fl) => write!(f, "{:.6}", fl),
            ParsedValue::Boolean(b) => write!(f, "{}", b),
            ParsedValue::Uuid(u) => write!(f, "{}", u),
            ParsedValue::Timestamp(ts) => write!(f, "{}", ts),
            ParsedValue::Blob(b) => write!(f, "0x{}", b),
            ParsedValue::Collection(items) => {
                let items_str: Vec<String> = items.iter().map(|v| v.to_string()).collect();
                write!(f, "[{}]", items_str.join(", "))
            }
            ParsedValue::Map(map) => {
                let pairs: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect();
                write!(f, "{{{}}}", pairs.join(", "))
            }
            ParsedValue::Null => write!(f, "NULL"),
            ParsedValue::Unparseable(err) => write!(f, "<PARSE_ERROR: {}>", err),
        }
    }
}

/// Real data parser that converts binary SSTable data to readable format
pub struct RealDataParser {
    /// Table schema for interpreting data types
    schema: TableSchema,
    /// Column name to type mapping for quick lookup
    column_types: HashMap<String, String>,
}

impl RealDataParser {
    /// Create a new data parser with the given schema
    pub fn new(schema: TableSchema) -> Self {
        let mut column_types = HashMap::new();
        
        // Build column type mapping from schema
        for column in &schema.columns {
            column_types.insert(column.name.clone(), column.data_type.clone());
        }
        
        // Add partition key columns
        for pk in &schema.partition_keys {
            column_types.insert(pk.name.clone(), pk.data_type.clone());
        }
        
        // Add clustering key columns
        for ck in &schema.clustering_keys {
            column_types.insert(ck.name.clone(), ck.data_type.clone());
        }

        Self {
            schema,
            column_types,
        }
    }

    /// Parse raw SSTable entry into human-readable row
    pub fn parse_entry(&self, key: &RowKey, value: &Value) -> Result<ParsedRow> {
        let mut columns = HashMap::new();
        
        // Parse the key components (partition key + clustering key)
        if let Err(e) = self.parse_key_components(&key, &mut columns) {
            eprintln!("Warning: Failed to parse key components: {}", e);
        }

        // Parse the value components (regular columns)
        if let Err(e) = self.parse_value_components(&value, &mut columns) {
            eprintln!("Warning: Failed to parse value components: {}", e);
        }

        Ok(ParsedRow {
            columns,
            row_key: key.clone(),
        })
    }

    /// Parse SSTableEntry from bulletproof reader into human-readable row
    pub fn parse_bulletproof_entry(&self, entry: &SSTableEntry) -> Result<ParsedRow> {
        let mut columns = HashMap::new();
        
        // Clean up the partition key for better display
        let clean_key = self.clean_partition_key(&entry.partition_key);
        
        // Add partition key from the entry
        if let Some(pk) = self.schema.partition_keys.first() {
            columns.insert(pk.name.clone(), ParsedValue::Text(clean_key.clone()));
        }
        
        // For counter tables, try to extract counter value from data
        if let Some(counter_col) = self.schema.columns.iter().find(|c| c.data_type == "counter") {
            // For now, just show that it's a counter (would need proper parsing)
            columns.insert(counter_col.name.clone(), ParsedValue::Text("[Counter]".to_string()));
        }
        
        // Create a dummy row key for now
        let row_key = RowKey::new(entry.data.clone());
        
        // Log what we found with cleaned key
        println!("🔑 Found partition key: {} ({})", clean_key, entry.format_info);
        
        Ok(ParsedRow {
            columns,
            row_key,
        })
    }

    /// Clean partition key for better display using proper Cassandra deserialization
    fn clean_partition_key(&self, raw_key: &str) -> String {
        // Convert string back to bytes for proper binary processing
        let key_bytes = raw_key.as_bytes();
        
        // Try to parse as Cassandra binary format
        if let Ok(parsed) = self.parse_cassandra_partition_key(key_bytes) {
            return parsed;
        }
        
        // Fallback: Try to extract UTF-8 text from the binary data
        if let Ok(utf8_text) = self.extract_utf8_from_binary(key_bytes) {
            if !utf8_text.trim().is_empty() {
                return utf8_text;
            }
        }
        
        // Final fallback: Show as binary key
        format!("[Binary Key: {} bytes]", key_bytes.len())
    }
    
    /// Parse Cassandra partition key binary format
    fn parse_cassandra_partition_key(&self, data: &[u8]) -> Result<String, String> {
        if data.is_empty() {
            return Err("Empty key".to_string());
        }
        
        // Cassandra partition keys are often length-prefixed
        // Try different parsing strategies
        
        // Strategy 1: Skip first 2 bytes (common length prefix) and read UTF-8
        if data.len() > 2 {
            if let Ok(text) = String::from_utf8(data[2..].to_vec()) {
                let clean = text.chars()
                    .filter(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
                    .collect::<String>()
                    .trim()
                    .to_string();
                if !clean.is_empty() && clean.len() > 2 {
                    return Ok(clean);
                }
            }
        }
        
        // Strategy 2: Try reading after first non-zero byte
        for i in 0..std::cmp::min(8, data.len()) {
            if data[i] != 0 && i + 1 < data.len() {
                if let Ok(text) = String::from_utf8(data[i+1..].to_vec()) {
                    let clean = text.chars()
                        .filter(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
                        .collect::<String>()
                        .trim()
                        .to_string();
                    if !clean.is_empty() && clean.len() > 4 {
                        return Ok(clean);
                    }
                }
            }
        }
        
        // Strategy 3: Look for consecutive readable bytes
        let mut clean_segments = Vec::new();
        let mut current_segment = String::new();
        
        for &byte in data {
            if byte >= 32 && byte <= 126 { // Printable ASCII
                current_segment.push(byte as char);
            } else {
                if current_segment.len() >= 3 { // Only keep segments of 3+ chars
                    clean_segments.push(current_segment.trim().to_string());
                }
                current_segment.clear();
            }
        }
        
        // Add final segment
        if current_segment.len() >= 3 {
            clean_segments.push(current_segment.trim().to_string());
        }
        
        // Return the longest readable segment
        if let Some(longest) = clean_segments.iter().max_by_key(|s| s.len()) {
            if longest.len() >= 3 {
                return Ok(longest.clone());
            }
        }
        
        Err("No readable text found".to_string())
    }
    
    /// Extract UTF-8 text from binary data with better error handling
    fn extract_utf8_from_binary(&self, data: &[u8]) -> Result<String, String> {
        // Try direct UTF-8 conversion first
        if let Ok(text) = String::from_utf8(data.to_vec()) {
            let clean = text.chars()
                .filter(|c| !c.is_control() || c.is_ascii_whitespace())
                .collect::<String>()
                .trim()
                .to_string();
            if !clean.is_empty() {
                return Ok(clean);
            }
        }
        
        // Try lossy UTF-8 conversion
        let lossy = String::from_utf8_lossy(data);
        let clean = lossy.chars()
            .filter(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
            .collect::<String>()
            .trim()
            .to_string();
            
        if !clean.is_empty() && clean.len() > 2 {
            Ok(clean)
        } else {
            Err("No valid UTF-8 text".to_string())
        }
    }

    /// Parse key components (partition + clustering keys)
    fn parse_key_components(&self, key: &RowKey, columns: &mut HashMap<String, ParsedValue>) -> Result<()> {
        // For now, we'll do a simplified key parsing
        // In a real implementation, you'd need to properly deserialize the key based on the schema
        
        // Add partition key columns
        for (i, pk) in self.schema.partition_keys.iter().enumerate() {
            let value = if i == 0 {
                // For the first partition key, try to extract from the key
                // RowKey is a tuple struct with Vec<u8> at index 0
                self.parse_key_data(&key.0, &pk.data_type)
                    .unwrap_or_else(|e| ParsedValue::Unparseable(format!("PK parse error: {}", e)))
            } else {
                ParsedValue::Unparseable("Multi-key parsing not implemented".to_string())
            };
            columns.insert(pk.name.clone(), value);
        }

        // Add clustering key columns (simplified)
        for ck in &self.schema.clustering_keys {
            columns.insert(
                ck.name.clone(),
                ParsedValue::Unparseable("Clustering key parsing not implemented".to_string())
            );
        }

        Ok(())
    }

    /// Parse value components (regular columns)
    fn parse_value_components(&self, value: &Value, columns: &mut HashMap<String, ParsedValue>) -> Result<()> {
        // For now, we'll do a simplified value parsing
        // In a real implementation, you'd need to properly deserialize based on Cassandra's serialization format
        
        match value {
            Value::Blob(data) => {
                // Try to parse as different data types based on length and content
                let parsed = self.parse_binary_data(data);
                
                // If we have regular columns in schema, try to assign values
                for (i, column) in self.schema.columns.iter().enumerate() {
                    if !columns.contains_key(&column.name) {
                        let col_value = if i == 0 && data.len() > 0 {
                            parsed.clone()
                        } else {
                            ParsedValue::Null
                        };
                        columns.insert(column.name.clone(), col_value);
                    }
                }
            }
            Value::Text(s) => {
                // If it's already a string, use it directly
                if let Some(first_col) = self.schema.columns.first() {
                    columns.insert(first_col.name.clone(), ParsedValue::Text(s.clone()));
                }
            }
            Value::Integer(i) => {
                if let Some(first_col) = self.schema.columns.first() {
                    columns.insert(first_col.name.clone(), ParsedValue::Integer((*i).into()));
                }
            }
            Value::BigInt(i) => {
                if let Some(first_col) = self.schema.columns.first() {
                    columns.insert(first_col.name.clone(), ParsedValue::Integer(*i));
                }
            }
            Value::Float(f) => {
                if let Some(first_col) = self.schema.columns.first() {
                    columns.insert(first_col.name.clone(), ParsedValue::Float(*f));
                }
            }
            Value::Boolean(b) => {
                if let Some(first_col) = self.schema.columns.first() {
                    columns.insert(first_col.name.clone(), ParsedValue::Boolean(*b));
                }
            }
            Value::Null => {
                // Add NULL values for all columns
                for column in &self.schema.columns {
                    if !columns.contains_key(&column.name) {
                        columns.insert(column.name.clone(), ParsedValue::Null);
                    }
                }
            }
            // Handle other Value variants that might exist
            _ => {
                // For any other variants, try to convert to string representation
                if let Some(first_col) = self.schema.columns.first() {
                    columns.insert(first_col.name.clone(), ParsedValue::Text(format!("{:?}", value)));
                }
            }
        }

        Ok(())
    }

    /// Parse key data based on data type
    fn parse_key_data(&self, data: &[u8], data_type: &str) -> Result<ParsedValue> {
        if data.is_empty() {
            return Ok(ParsedValue::Null);
        }

        match data_type.to_lowercase().as_str() {
            "text" | "varchar" | "ascii" => {
                match String::from_utf8(data.to_vec()) {
                    Ok(s) => Ok(ParsedValue::Text(s)),
                    Err(_) => Ok(ParsedValue::Text(format!("binary_data_{}_bytes", data.len()))),
                }
            }
            "int" | "integer" => {
                if data.len() >= 4 {
                    let bytes = [data[0], data[1], data[2], data[3]];
                    let val = i32::from_be_bytes(bytes) as i64;
                    Ok(ParsedValue::Integer(val))
                } else {
                    Ok(ParsedValue::Unparseable("Invalid int data".to_string()))
                }
            }
            "bigint" | "long" => {
                if data.len() >= 8 {
                    let bytes = [data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]];
                    let val = i64::from_be_bytes(bytes);
                    Ok(ParsedValue::Integer(val))
                } else {
                    Ok(ParsedValue::Unparseable("Invalid bigint data".to_string()))
                }
            }
            "uuid" => {
                if data.len() == 16 {
                    let uuid_str = format!(
                        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                        data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15]
                    );
                    Ok(ParsedValue::Uuid(uuid_str))
                } else {
                    Ok(ParsedValue::Unparseable("Invalid UUID data".to_string()))
                }
            }
            "timestamp" => {
                if data.len() >= 8 {
                    let bytes = [data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]];
                    let timestamp_ms = i64::from_be_bytes(bytes);
                    let dt = chrono::DateTime::from_timestamp_millis(timestamp_ms)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                    Ok(ParsedValue::Timestamp(dt.to_rfc3339()))
                } else {
                    Ok(ParsedValue::Unparseable("Invalid timestamp data".to_string()))
                }
            }
            "boolean" | "bool" => {
                if !data.is_empty() {
                    Ok(ParsedValue::Boolean(data[0] != 0))
                } else {
                    Ok(ParsedValue::Unparseable("Invalid boolean data".to_string()))
                }
            }
            "blob" | "bytes" => {
                Ok(ParsedValue::Blob(hex::encode(data)))
            }
            _ => {
                // Default: try as text first, then as blob
                match String::from_utf8(data.to_vec()) {
                    Ok(s) if s.chars().all(|c| c.is_ascii_graphic() || c.is_whitespace()) => {
                        Ok(ParsedValue::Text(s))
                    }
                    _ => Ok(ParsedValue::Blob(hex::encode(data))),
                }
            }
        }
    }

    /// Parse binary data with heuristics
    fn parse_binary_data(&self, data: &[u8]) -> ParsedValue {
        if data.is_empty() {
            return ParsedValue::Null;
        }

        // Try different parsing strategies based on data characteristics
        
        // 1. Try as UTF-8 text
        if let Ok(text) = String::from_utf8(data.to_vec()) {
            if text.chars().all(|c| c.is_ascii_graphic() || c.is_whitespace()) && !text.is_empty() {
                return ParsedValue::Text(text);
            }
        }

        // 2. Try as integer based on length
        match data.len() {
            4 => {
                let bytes = [data[0], data[1], data[2], data[3]];
                let val = i32::from_be_bytes(bytes) as i64;
                return ParsedValue::Integer(val);
            }
            8 => {
                let bytes = [data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]];
                let val = i64::from_be_bytes(bytes);
                return ParsedValue::Integer(val);
            }
            16 => {
                // Could be UUID
                let uuid_str = format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15]
                );
                return ParsedValue::Uuid(uuid_str);
            }
            1 => {
                // Could be boolean
                return ParsedValue::Boolean(data[0] != 0);
            }
            _ => {}
        }

        // 3. Default to blob
        ParsedValue::Blob(hex::encode(data))
    }

    /// Get column names in display order
    pub fn get_column_names(&self) -> Vec<String> {
        let mut columns = Vec::new();
        
        // Add partition keys first
        for pk in &self.schema.partition_keys {
            columns.push(pk.name.clone());
        }
        
        // Add clustering keys
        for ck in &self.schema.clustering_keys {
            columns.push(ck.name.clone());
        }
        
        // Add regular columns
        for col in &self.schema.columns {
            columns.push(col.name.clone());
        }
        
        columns
    }

    /// Get schema reference
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Convert parsed row to JSON for export
impl ParsedRow {
    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        
        for (col_name, value) in &self.columns {
            let json_value = match value {
                ParsedValue::Text(s) => serde_json::Value::String(s.clone()),
                ParsedValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
                ParsedValue::Float(f) => {
                    serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0)))
                }
                ParsedValue::Boolean(b) => serde_json::Value::Bool(*b),
                ParsedValue::Uuid(u) => serde_json::Value::String(u.clone()),
                ParsedValue::Timestamp(ts) => serde_json::Value::String(ts.clone()),
                ParsedValue::Blob(b) => serde_json::Value::String(format!("0x{}", b)),
                ParsedValue::Collection(items) => {
                    let json_items: Vec<serde_json::Value> = items
                        .iter()
                        .map(|item| match item {
                            ParsedValue::Text(s) => serde_json::Value::String(s.clone()),
                            ParsedValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
                            ParsedValue::Float(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(serde_json::Number::from(0))),
                            ParsedValue::Boolean(b) => serde_json::Value::Bool(*b),
                            _ => serde_json::Value::String(item.to_string()),
                        })
                        .collect();
                    serde_json::Value::Array(json_items)
                }
                ParsedValue::Map(map_data) => {
                    let mut json_map = serde_json::Map::new();
                    for (k, v) in map_data {
                        json_map.insert(k.clone(), serde_json::Value::String(v.to_string()));
                    }
                    serde_json::Value::Object(json_map)
                }
                ParsedValue::Null => serde_json::Value::Null,
                ParsedValue::Unparseable(err) => serde_json::Value::String(format!("PARSE_ERROR: {}", err)),
            };
            
            map.insert(col_name.clone(), json_value);
        }
        
        serde_json::Value::Object(map)
    }

    /// Get value for a specific column
    pub fn get(&self, column_name: &str) -> Option<&ParsedValue> {
        self.columns.get(column_name)
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }
}