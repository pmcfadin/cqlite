//! Query result types for CQLite
//!
//! This module provides result types and utilities for query execution results.
//! It includes result set management, row iteration, and result metadata.

use crate::{RowKey, Value};
use std::collections::HashMap;
use std::fmt;

/// Query result containing rows and metadata
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Result rows
    pub rows: Vec<QueryRow>,
    /// Number of rows affected (for INSERT/UPDATE/DELETE)
    pub rows_affected: u64,
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
    /// Query metadata
    pub metadata: QueryMetadata,
}

/// Individual row in query result
#[derive(Debug, Clone)]
pub struct QueryRow {
    /// Column values mapped by column name
    pub values: HashMap<String, Value>,
    /// Original row key
    pub key: RowKey,
    /// Row metadata
    pub metadata: RowMetadata,
}

/// Metadata for query results
#[derive(Debug, Clone, Default)]
pub struct QueryMetadata {
    /// Column information
    pub columns: Vec<ColumnInfo>,
    /// Total row count (may be different from returned rows due to LIMIT)
    pub total_rows: Option<u64>,
    /// Query execution plan information
    pub plan_info: Option<PlanInfo>,
    /// Performance metrics
    pub performance: PerformanceMetrics,
    /// Warnings generated during execution
    pub warnings: Vec<String>,
}

/// Information about a column in the result set
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: crate::types::DataType,
    /// Whether column can be null
    pub nullable: bool,
    /// Column position in result set
    pub position: usize,
    /// Original table name (for joined queries)
    pub table_name: Option<String>,
}

/// Row metadata
#[derive(Debug, Clone, Default)]
pub struct RowMetadata {
    /// Row version/timestamp
    pub version: Option<u64>,
    /// Row TTL (time to live)
    pub ttl: Option<u64>,
    /// Row tags or labels
    pub tags: HashMap<String, String>,
}

/// Query execution plan information
#[derive(Debug, Clone)]
pub struct PlanInfo {
    /// Plan type used
    pub plan_type: String,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Actual cost
    pub actual_cost: f64,
    /// Indexes used
    pub indexes_used: Vec<String>,
    /// Steps executed
    pub steps: Vec<String>,
    /// Parallelization information
    pub parallelization: Option<ParallelizationInfo>,
}

/// Parallelization information for query execution
#[derive(Debug, Clone)]
pub struct ParallelizationInfo {
    /// Number of threads used
    pub threads_used: usize,
    /// Whether parallelization was effective
    pub effective: bool,
    /// Partition information
    pub partitions: Vec<PartitionInfo>,
}

/// Information about a partition processed in parallel
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition ID
    pub id: usize,
    /// Rows processed by this partition
    pub rows_processed: u64,
    /// Processing time for this partition
    pub processing_time_ms: u64,
}

/// Performance metrics for query execution
#[derive(Debug, Clone, Default)]
pub struct PerformanceMetrics {
    /// Parse time in microseconds
    pub parse_time_us: u64,
    /// Planning time in microseconds
    pub planning_time_us: u64,
    /// Execution time in microseconds
    pub execution_time_us: u64,
    /// Total time in microseconds
    pub total_time_us: u64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// I/O operations performed
    pub io_operations: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
}

impl QueryResult {
    /// Create a new empty query result
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            rows_affected: 0,
            execution_time_ms: 0,
            metadata: QueryMetadata::default(),
        }
    }

    /// Create a result with rows
    pub fn with_rows(rows: Vec<QueryRow>) -> Self {
        let mut result = Self::new();
        result.rows = rows;
        result
    }

    /// Create a result for DML operations (INSERT/UPDATE/DELETE)
    pub fn with_affected_rows(rows_affected: u64) -> Self {
        let mut result = Self::new();
        result.rows_affected = rows_affected;
        result
    }

    /// Get the number of rows in the result
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get a specific row by index
    pub fn get_row(&self, index: usize) -> Option<&QueryRow> {
        self.rows.get(index)
    }

    /// Get column information
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.metadata.columns
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<String> {
        self.metadata
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect()
    }

    /// Get execution time in milliseconds
    pub fn execution_time(&self) -> u64 {
        self.execution_time_ms
    }

    /// Get performance metrics
    pub fn performance(&self) -> &PerformanceMetrics {
        &self.metadata.performance
    }

    /// Get warnings
    pub fn warnings(&self) -> &[String] {
        &self.metadata.warnings
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: String) {
        self.metadata.warnings.push(warning);
    }

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        // Add rows
        let rows_json: Vec<serde_json::Value> = self.rows.iter().map(|row| row.to_json()).collect();
        result.insert("rows".to_string(), serde_json::Value::Array(rows_json));

        // Add metadata
        result.insert(
            "rows_affected".to_string(),
            serde_json::Value::Number(self.rows_affected.into()),
        );
        result.insert(
            "execution_time_ms".to_string(),
            serde_json::Value::Number(self.execution_time_ms.into()),
        );
        result.insert(
            "row_count".to_string(),
            serde_json::Value::Number(self.rows.len().into()),
        );

        // Add column information
        let columns_json: Vec<serde_json::Value> = self
            .metadata
            .columns
            .iter()
            .map(|col| col.to_json())
            .collect();
        result.insert(
            "columns".to_string(),
            serde_json::Value::Array(columns_json),
        );

        // Add performance metrics
        result.insert(
            "performance".to_string(),
            self.metadata.performance.to_json(),
        );

        // Add warnings
        let warnings_json: Vec<serde_json::Value> = self
            .metadata
            .warnings
            .iter()
            .map(|w| serde_json::Value::String(w.clone()))
            .collect();
        result.insert(
            "warnings".to_string(),
            serde_json::Value::Array(warnings_json),
        );

        serde_json::Value::Object(result)
    }

    /// Create result iterator
    pub fn iter(&self) -> std::slice::Iter<QueryRow> {
        self.rows.iter()
    }
}

impl QueryRow {
    /// Create a new query row
    pub fn new(key: RowKey) -> Self {
        Self {
            values: HashMap::new(),
            key,
            metadata: RowMetadata::default(),
        }
    }

    /// Create a row with values
    pub fn with_values(key: RowKey, values: HashMap<String, Value>) -> Self {
        Self {
            values,
            key,
            metadata: RowMetadata::default(),
        }
    }

    /// Get a value by column name
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.values.get(column)
    }

    /// Set a value for a column
    pub fn set(&mut self, column: String, value: Value) {
        self.values.insert(column, value);
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
    }

    /// Get the row key
    pub fn key(&self) -> &RowKey {
        &self.key
    }

    /// Get row metadata
    pub fn metadata(&self) -> &RowMetadata {
        &self.metadata
    }

    /// Set row metadata
    pub fn set_metadata(&mut self, metadata: RowMetadata) {
        self.metadata = metadata;
    }

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        // Add column values
        for (column, value) in &self.values {
            result.insert(column.clone(), value.to_json());
        }

        // Add row key
        result.insert(
            "_key".to_string(),
            serde_json::Value::String(format!("{:?}", self.key)),
        );

        // Add metadata if present
        if self.metadata.version.is_some()
            || self.metadata.ttl.is_some()
            || !self.metadata.tags.is_empty()
        {
            result.insert("_metadata".to_string(), self.metadata.to_json());
        }

        serde_json::Value::Object(result)
    }
}

impl ColumnInfo {
    /// Create new column info
    pub fn new(
        name: String,
        data_type: crate::types::DataType,
        nullable: bool,
        position: usize,
    ) -> Self {
        Self {
            name,
            data_type,
            nullable,
            position,
            table_name: None,
        }
    }

    /// Set table name
    pub fn with_table_name(mut self, table_name: String) -> Self {
        self.table_name = Some(table_name);
        self
    }

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        result.insert(
            "name".to_string(),
            serde_json::Value::String(self.name.clone()),
        );
        result.insert(
            "data_type".to_string(),
            serde_json::Value::String(format!("{:?}", self.data_type)),
        );
        result.insert(
            "nullable".to_string(),
            serde_json::Value::Bool(self.nullable),
        );
        result.insert(
            "position".to_string(),
            serde_json::Value::Number(self.position.into()),
        );

        if let Some(table_name) = &self.table_name {
            result.insert(
                "table_name".to_string(),
                serde_json::Value::String(table_name.clone()),
            );
        }

        serde_json::Value::Object(result)
    }
}

impl RowMetadata {
    /// Create new row metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Set version
    pub fn with_version(mut self, version: u64) -> Self {
        self.version = Some(version);
        self
    }

    /// Set TTL
    pub fn with_ttl(mut self, ttl: u64) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Add tag
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key, value);
        self
    }

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        if let Some(version) = self.version {
            result.insert(
                "version".to_string(),
                serde_json::Value::Number(version.into()),
            );
        }

        if let Some(ttl) = self.ttl {
            result.insert("ttl".to_string(), serde_json::Value::Number(ttl.into()));
        }

        if !self.tags.is_empty() {
            let tags_json: serde_json::Map<String, serde_json::Value> = self
                .tags
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            result.insert("tags".to_string(), serde_json::Value::Object(tags_json));
        }

        serde_json::Value::Object(result)
    }
}

impl PerformanceMetrics {
    /// Create new performance metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total time in milliseconds
    pub fn total_time_ms(&self) -> u64 {
        self.total_time_us / 1000
    }

    /// Get cache hit ratio
    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        result.insert(
            "parse_time_us".to_string(),
            serde_json::Value::Number(self.parse_time_us.into()),
        );
        result.insert(
            "planning_time_us".to_string(),
            serde_json::Value::Number(self.planning_time_us.into()),
        );
        result.insert(
            "execution_time_us".to_string(),
            serde_json::Value::Number(self.execution_time_us.into()),
        );
        result.insert(
            "total_time_us".to_string(),
            serde_json::Value::Number(self.total_time_us.into()),
        );
        result.insert(
            "memory_usage_bytes".to_string(),
            serde_json::Value::Number(self.memory_usage_bytes.into()),
        );
        result.insert(
            "io_operations".to_string(),
            serde_json::Value::Number(self.io_operations.into()),
        );
        result.insert(
            "cache_hits".to_string(),
            serde_json::Value::Number(self.cache_hits.into()),
        );
        result.insert(
            "cache_misses".to_string(),
            serde_json::Value::Number(self.cache_misses.into()),
        );
        result.insert(
            "cache_hit_ratio".to_string(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(self.cache_hit_ratio())
                    .unwrap_or(serde_json::Number::from(0)),
            ),
        );

        serde_json::Value::Object(result)
    }
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.rows.is_empty() {
            return write!(f, "Empty result set ({} rows affected)", self.rows_affected);
        }

        // Create table header
        let column_names = self.column_names();
        if column_names.is_empty() {
            return write!(f, "No columns in result set");
        }

        // Calculate column widths
        let mut col_widths = Vec::new();
        for (i, col_name) in column_names.iter().enumerate() {
            let mut max_width = col_name.len();
            for row in &self.rows {
                if let Some(value) = row.values.get(col_name) {
                    max_width = max_width.max(format!("{}", value).len());
                }
            }
            col_widths.push(max_width);
        }

        // Print header
        write!(f, "┌")?;
        for (i, width) in col_widths.iter().enumerate() {
            write!(f, "{}", "─".repeat(width + 2))?;
            if i < col_widths.len() - 1 {
                write!(f, "┬")?;
            }
        }
        writeln!(f, "┐")?;

        // Print column names
        write!(f, "│")?;
        for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
            write!(f, " {:width$} ", col_name, width = width)?;
            if i < column_names.len() - 1 {
                write!(f, "│")?;
            }
        }
        writeln!(f, "│")?;

        // Print separator
        write!(f, "├")?;
        for (i, width) in col_widths.iter().enumerate() {
            write!(f, "{}", "─".repeat(width + 2))?;
            if i < col_widths.len() - 1 {
                write!(f, "┼")?;
            }
        }
        writeln!(f, "┤")?;

        // Print rows
        for row in &self.rows {
            write!(f, "│")?;
            for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
                let value = row
                    .values
                    .get(col_name)
                    .map(|v| format!("{}", v))
                    .unwrap_or_else(|| "NULL".to_string());
                write!(f, " {:width$} ", value, width = width)?;
                if i < column_names.len() - 1 {
                    write!(f, "│")?;
                }
            }
            writeln!(f, "│")?;
        }

        // Print footer
        write!(f, "└")?;
        for (i, width) in col_widths.iter().enumerate() {
            write!(f, "{}", "─".repeat(width + 2))?;
            if i < col_widths.len() - 1 {
                write!(f, "┴")?;
            }
        }
        writeln!(f, "┘")?;

        // Print summary
        writeln!(
            f,
            "{} rows returned in {}ms",
            self.rows.len(),
            self.execution_time_ms
        )?;

        // Print warnings if any
        if !self.metadata.warnings.is_empty() {
            writeln!(f, "\nWarnings:")?;
            for warning in &self.metadata.warnings {
                writeln!(f, "  - {}", warning)?;
            }
        }

        Ok(())
    }
}

impl Default for QueryResult {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoIterator for QueryResult {
    type Item = QueryRow;
    type IntoIter = std::vec::IntoIter<QueryRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a> IntoIterator for &'a QueryResult {
    type Item = &'a QueryRow;
    type IntoIter = std::slice::Iter<'a, QueryRow>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

// Helper trait for converting values to JSON
trait ToJson {
    fn to_json(&self) -> serde_json::Value;
}

impl ToJson for Value {
    fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(b) => serde_json::Value::Bool(*b),
            Value::Integer(i) => serde_json::Value::Number((*i).into()),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Text(s) => serde_json::Value::String(s.clone()),
            Value::Blob(b) => {
                use base64::Engine;
                let engine = base64::engine::general_purpose::STANDARD;
                serde_json::Value::String(engine.encode(b))
            },
            Value::List(list) => {
                let json_list: Vec<serde_json::Value> = list.iter().map(|v| v.to_json()).collect();
                serde_json::Value::Array(json_list)
            }
            Value::Map(map) => {
                let json_map: serde_json::Map<String, serde_json::Value> =
                    map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
                serde_json::Value::Object(json_map)
            }
            Value::BigInt(i) => serde_json::Value::Number((*i).into()),
            Value::Timestamp(ts) => serde_json::Value::Number((*ts).into()),
            Value::Uuid(uuid) => {
                use base64::Engine;
                let engine = base64::engine::general_purpose::STANDARD;
                serde_json::Value::String(engine.encode(uuid))
            }
            Value::Json(json) => json.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn test_query_result_creation() {
        let result = QueryResult::new();
        assert!(result.is_empty());
        assert_eq!(result.row_count(), 0);
        assert_eq!(result.execution_time(), 0);
    }

    #[test]
    fn test_query_result_with_rows() {
        let mut row1 = QueryRow::new(RowKey::from_bytes(vec![1]));
        row1.set("id".to_string(), Value::Integer(1));
        row1.set("name".to_string(), Value::Text("Alice".to_string()));

        let mut row2 = QueryRow::new(RowKey::from_bytes(vec![2]));
        row2.set("id".to_string(), Value::Integer(2));
        row2.set("name".to_string(), Value::Text("Bob".to_string()));

        let result = QueryResult::with_rows(vec![row1, row2]);
        assert_eq!(result.row_count(), 2);
        assert!(!result.is_empty());

        let first_row = result.get_row(0).unwrap();
        assert_eq!(first_row.get("id"), Some(&Value::Integer(1)));
        assert_eq!(
            first_row.get("name"),
            Some(&Value::Text("Alice".to_string()))
        );
    }

    #[test]
    fn test_query_row_operations() {
        let mut row = QueryRow::new(RowKey::from_bytes(vec![1]));
        row.set("id".to_string(), Value::Integer(42));
        row.set("active".to_string(), Value::Boolean(true));

        assert_eq!(row.get("id"), Some(&Value::Integer(42)));
        assert_eq!(row.get("active"), Some(&Value::Boolean(true)));
        assert_eq!(row.get("nonexistent"), None);

        let column_names = row.column_names();
        assert_eq!(column_names.len(), 2);
        assert!(column_names.contains(&"id".to_string()));
        assert!(column_names.contains(&"active".to_string()));
    }

    #[test]
    fn test_column_info() {
        let column = ColumnInfo::new(
            "user_id".to_string(),
            crate::types::DataType::Integer,
            false,
            0,
        )
        .with_table_name("users".to_string());

        assert_eq!(column.name, "user_id");
        assert_eq!(column.data_type, crate::types::DataType::Integer);
        assert!(!column.nullable);
        assert_eq!(column.position, 0);
        assert_eq!(column.table_name, Some("users".to_string()));
    }

    #[test]
    fn test_row_metadata() {
        let metadata = RowMetadata::new()
            .with_version(123)
            .with_ttl(3600)
            .with_tag("source".to_string(), "import".to_string());

        assert_eq!(metadata.version, Some(123));
        assert_eq!(metadata.ttl, Some(3600));
        assert_eq!(metadata.tags.get("source"), Some(&"import".to_string()));
    }

    #[test]
    fn test_performance_metrics() {
        let mut metrics = PerformanceMetrics::new();
        metrics.cache_hits = 8;
        metrics.cache_misses = 2;
        metrics.total_time_us = 5000;

        assert_eq!(metrics.cache_hit_ratio(), 0.8);
        assert_eq!(metrics.total_time_ms(), 5);
    }

    #[test]
    fn test_json_serialization() {
        let mut row = QueryRow::new(RowKey::from_bytes(vec![1]));
        row.set("id".to_string(), Value::Integer(1));
        row.set("name".to_string(), Value::Text("test".to_string()));

        let json = row.to_json();
        assert!(json.is_object());

        let obj = json.as_object().unwrap();
        assert_eq!(obj.get("id"), Some(&serde_json::Value::Number(1.into())));
        assert_eq!(
            obj.get("name"),
            Some(&serde_json::Value::String("test".to_string()))
        );
    }

    #[test]
    fn test_result_iteration() {
        let row1 = QueryRow::new(RowKey::from_bytes(vec![1]));
        let row2 = QueryRow::new(RowKey::from_bytes(vec![2]));
        let result = QueryResult::with_rows(vec![row1, row2]);

        let mut count = 0;
        for _row in &result {
            count += 1;
        }
        assert_eq!(count, 2);

        let mut count = 0;
        for _row in result {
            count += 1;
        }
        assert_eq!(count, 2);
    }
}
