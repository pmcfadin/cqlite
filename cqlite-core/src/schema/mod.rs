//! Schema definition and parsing for CQLite
//!
//! This module handles JSON-based schema definitions that describe
//! the structure of Cassandra tables for schema-aware SSTable reading.

use crate::error::{Error, Result};
use crate::storage::StorageEngine;
use crate::Config;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// Table schema definition loaded from JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Keyspace name
    pub keyspace: String,

    /// Table name
    pub table: String,

    /// Partition key columns (ordered)
    pub partition_keys: Vec<KeyColumn>,

    /// Clustering key columns (ordered)  
    pub clustering_keys: Vec<ClusteringColumn>,

    /// All columns in the table
    pub columns: Vec<Column>,

    /// Optional metadata
    #[serde(default)]
    pub comments: HashMap<String, String>,
}

/// Partition key column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyColumn {
    /// Column name
    pub name: String,

    /// CQL data type
    #[serde(rename = "type")]
    pub data_type: String,

    /// Position in composite key (0-based)
    pub position: usize,
}

/// Clustering key column with ordering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusteringColumn {
    /// Column name
    pub name: String,

    /// CQL data type
    #[serde(rename = "type")]
    pub data_type: String,

    /// Position in clustering key (0-based)
    pub position: usize,

    /// Sort order (ASC or DESC)
    #[serde(default = "default_order")]
    pub order: String,
}

/// Regular column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,

    /// CQL data type (e.g., "text", "bigint", "list<int>")
    #[serde(rename = "type")]
    pub data_type: String,

    /// Whether column can be null
    #[serde(default)]
    pub nullable: bool,

    /// Default value (if any)
    #[serde(default)]
    pub default: Option<serde_json::Value>,
}

/// Parsed CQL data type
#[derive(Debug, Clone, PartialEq)]
pub enum CqlType {
    // Primitive types
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal,
    Text,
    Ascii,
    Varchar,
    Blob,
    Timestamp,
    Date,
    Time,
    Uuid,
    TimeUuid,
    Inet,
    Duration,

    // Collection types (implemented as tuples)
    List(Box<CqlType>),
    Set(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),

    // Complex types
    Tuple(Vec<CqlType>),
    Udt(String, Vec<(String, CqlType)>), // name, fields
    Frozen(Box<CqlType>),

    // Custom/Unknown
    Custom(String),
}

impl TableSchema {
    /// Load schema from JSON file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)
            .map_err(|e| Error::schema(format!("Failed to read schema file: {}", e)))?;

        Self::from_json(&content)
    }

    /// Parse schema from JSON string
    pub fn from_json(json: &str) -> Result<Self> {
        let schema: TableSchema = serde_json::from_str(json)
            .map_err(|e| Error::schema(format!("Invalid JSON schema: {}", e)))?;

        schema.validate()?;
        Ok(schema)
    }

    /// Save schema to JSON file
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| Error::serialization(format!("Failed to serialize schema: {}", e)))?;

        fs::write(path, json)
            .map_err(|e| Error::schema(format!("Failed to write schema file: {}", e)))?;

        Ok(())
    }

    /// Validate schema consistency
    pub fn validate(&self) -> Result<()> {
        // Validate keyspace and table names
        if self.keyspace.is_empty() {
            return Err(Error::schema("Keyspace name cannot be empty".to_string()));
        }

        if self.table.is_empty() {
            return Err(Error::schema("Table name cannot be empty".to_string()));
        }

        // Must have at least one partition key
        if self.partition_keys.is_empty() {
            return Err(Error::schema(
                "Table must have at least one partition key".to_string(),
            ));
        }

        // Validate partition key positions are contiguous
        let mut positions: Vec<_> = self.partition_keys.iter().map(|k| k.position).collect();
        positions.sort();
        for (i, &pos) in positions.iter().enumerate() {
            if pos != i {
                return Err(Error::schema(format!(
                    "Partition key positions must be contiguous starting from 0, found gap at position {}", 
                    i
                )));
            }
        }

        // Validate clustering key positions (if any)
        if !self.clustering_keys.is_empty() {
            let mut positions: Vec<_> = self.clustering_keys.iter().map(|k| k.position).collect();
            positions.sort();
            for (i, &pos) in positions.iter().enumerate() {
                if pos != i {
                    return Err(Error::schema(format!(
                        "Clustering key positions must be contiguous starting from 0, found gap at position {}", 
                        i
                    )));
                }
            }
        }

        // Validate data types
        for column in &self.columns {
            CqlType::parse(&column.data_type).map_err(|e| {
                Error::schema(format!(
                    "Invalid data type '{}' for column '{}': {}",
                    column.data_type, column.name, e
                ))
            })?;
        }

        // Validate all key columns exist in columns list
        for key in &self.partition_keys {
            if !self.columns.iter().any(|c| c.name == key.name) {
                return Err(Error::schema(format!(
                    "Partition key '{}' not found in columns list",
                    key.name
                )));
            }
        }

        for key in &self.clustering_keys {
            if !self.columns.iter().any(|c| c.name == key.name) {
                return Err(Error::schema(format!(
                    "Clustering key '{}' not found in columns list",
                    key.name
                )));
            }
        }

        Ok(())
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Check if column is a partition key
    pub fn is_partition_key(&self, name: &str) -> bool {
        self.partition_keys.iter().any(|k| k.name == name)
    }

    /// Check if column is a clustering key
    pub fn is_clustering_key(&self, name: &str) -> bool {
        self.clustering_keys.iter().any(|k| k.name == name)
    }

    /// Get partition key columns in order
    pub fn ordered_partition_keys(&self) -> Vec<&KeyColumn> {
        let mut keys = self.partition_keys.iter().collect::<Vec<_>>();
        keys.sort_by_key(|k| k.position);
        keys
    }

    /// Get clustering key columns in order
    pub fn ordered_clustering_keys(&self) -> Vec<&ClusteringColumn> {
        let mut keys = self.clustering_keys.iter().collect::<Vec<_>>();
        keys.sort_by_key(|k| k.position);
        keys
    }
}

impl CqlType {
    /// Parse CQL type string into structured type
    pub fn parse(type_str: &str) -> Result<Self> {
        let type_str = type_str.trim();

        // Handle frozen types
        if let Some(inner) = type_str.strip_prefix("frozen<") {
            if let Some(inner) = inner.strip_suffix('>') {
                return Ok(CqlType::Frozen(Box::new(Self::parse(inner)?)));
            }
        }

        // Handle collection types
        if let Some(inner) = type_str.strip_prefix("list<") {
            if let Some(inner) = inner.strip_suffix('>') {
                return Ok(CqlType::List(Box::new(Self::parse(inner)?)));
            }
        }

        if let Some(inner) = type_str.strip_prefix("set<") {
            if let Some(inner) = inner.strip_suffix('>') {
                return Ok(CqlType::Set(Box::new(Self::parse(inner)?)));
            }
        }

        if let Some(inner) = type_str.strip_prefix("map<") {
            if let Some(inner) = inner.strip_suffix('>') {
                let parts: Vec<&str> = inner.splitn(2, ',').collect();
                if parts.len() != 2 {
                    return Err(Error::schema(format!("Invalid map type: {}", type_str)));
                }
                return Ok(CqlType::Map(
                    Box::new(Self::parse(parts[0].trim())?),
                    Box::new(Self::parse(parts[1].trim())?),
                ));
            }
        }

        // Handle tuple types
        if let Some(inner) = type_str.strip_prefix("tuple<") {
            if let Some(inner) = inner.strip_suffix('>') {
                let parts: Vec<&str> = inner.split(',').collect();
                let mut types = Vec::new();
                for part in parts {
                    types.push(Self::parse(part.trim())?);
                }
                return Ok(CqlType::Tuple(types));
            }
        }

        // Primitive types
        match type_str.to_lowercase().as_str() {
            "boolean" | "bool" => Ok(CqlType::Boolean),
            "tinyint" => Ok(CqlType::TinyInt),
            "smallint" => Ok(CqlType::SmallInt),
            "int" | "integer" => Ok(CqlType::Int),
            "bigint" | "long" => Ok(CqlType::BigInt),
            "float" => Ok(CqlType::Float),
            "double" => Ok(CqlType::Double),
            "decimal" => Ok(CqlType::Decimal),
            "text" | "varchar" => Ok(CqlType::Text),
            "ascii" => Ok(CqlType::Ascii),
            "blob" => Ok(CqlType::Blob),
            "timestamp" => Ok(CqlType::Timestamp),
            "date" => Ok(CqlType::Date),
            "time" => Ok(CqlType::Time),
            "uuid" => Ok(CqlType::Uuid),
            "timeuuid" => Ok(CqlType::TimeUuid),
            "inet" => Ok(CqlType::Inet),
            "duration" => Ok(CqlType::Duration),
            _ => Ok(CqlType::Custom(type_str.to_string())),
        }
    }

    /// Get the expected byte size for fixed-size types
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            CqlType::Boolean => Some(1),
            CqlType::TinyInt => Some(1),
            CqlType::SmallInt => Some(2),
            CqlType::Int => Some(4),
            CqlType::BigInt => Some(8),
            CqlType::Float => Some(4),
            CqlType::Double => Some(8),
            CqlType::Timestamp => Some(8),
            CqlType::Date => Some(4),
            CqlType::Time => Some(8),
            CqlType::Uuid | CqlType::TimeUuid => Some(16),
            CqlType::Inet => Some(16), // IPv6, IPv4 is variable
            // Variable size types
            CqlType::Text
            | CqlType::Ascii
            | CqlType::Varchar
            | CqlType::Blob
            | CqlType::Decimal
            | CqlType::Duration => None,
            // Collections and complex types are variable
            CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _) => None,
            CqlType::Frozen(inner) => inner.fixed_size(),
            CqlType::Custom(_) => None,
        }
    }

    /// Check if this type is a collection
    pub fn is_collection(&self) -> bool {
        matches!(
            self,
            CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _)
        )
    }
}

// Default functions for serde
fn default_order() -> String {
    "ASC".to_string()
}

/// Schema management service for handling table schemas
#[derive(Debug)]
pub struct SchemaManager {
    storage: Arc<StorageEngine>,
    schemas: HashMap<String, TableSchema>,
}

impl SchemaManager {
    /// Create a new schema manager
    pub async fn new(storage: Arc<StorageEngine>, _config: &Config) -> Result<Self> {
        Ok(Self {
            storage,
            schemas: HashMap::new(),
        })
    }

    /// Load schema for a table
    pub async fn load_schema(&mut self, table_name: &str) -> Result<&TableSchema> {
        if !self.schemas.contains_key(table_name) {
            // Try to load from storage or default
            let schema = self.create_default_schema(table_name);
            self.schemas.insert(table_name.to_string(), schema);
        }

        Ok(self.schemas.get(table_name).unwrap())
    }

    /// Create a default schema for unknown tables
    fn create_default_schema(&self, table_name: &str) -> TableSchema {
        TableSchema {
            keyspace: "default".to_string(),
            table: table_name.to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                nullable: false,
                default: None,
            }],
            comments: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_validation() {
        let schema_json = r#"
        {
            "keyspace": "test",
            "table": "users",
            "partition_keys": [
                {"name": "id", "type": "bigint", "position": 0}
            ],
            "clustering_keys": [],
            "columns": [
                {"name": "id", "type": "bigint", "nullable": false},
                {"name": "name", "type": "text", "nullable": true}
            ]
        }
        "#;

        let schema = TableSchema::from_json(schema_json).unwrap();
        assert_eq!(schema.keyspace, "test");
        assert_eq!(schema.table, "users");
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.columns.len(), 2);
    }

    #[test]
    fn test_cql_type_parsing() {
        assert_eq!(CqlType::parse("text").unwrap(), CqlType::Text);
        assert_eq!(CqlType::parse("bigint").unwrap(), CqlType::BigInt);

        match CqlType::parse("list<int>").unwrap() {
            CqlType::List(inner) => assert_eq!(*inner, CqlType::Int),
            _ => panic!("Expected List type"),
        }

        match CqlType::parse("map<text, bigint>").unwrap() {
            CqlType::Map(key, value) => {
                assert_eq!(*key, CqlType::Text);
                assert_eq!(*value, CqlType::BigInt);
            }
            _ => panic!("Expected Map type"),
        }
    }

    #[test]
    fn test_schema_validation_failures() {
        // Missing partition key
        let invalid_schema = r#"
        {
            "keyspace": "test",
            "table": "users", 
            "partition_keys": [],
            "clustering_keys": [],
            "columns": []
        }
        "#;

        assert!(TableSchema::from_json(invalid_schema).is_err());

        // Invalid type
        let invalid_type = r#"
        {
            "keyspace": "test",
            "table": "users",
            "partition_keys": [
                {"name": "id", "type": "invalid_type", "position": 0}
            ],
            "clustering_keys": [],
            "columns": [
                {"name": "id", "type": "invalid_type", "nullable": false}
            ]
        }
        "#;

        // This should succeed as we allow custom types
        assert!(TableSchema::from_json(invalid_type).is_ok());
    }
}
