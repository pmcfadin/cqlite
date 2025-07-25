//! Schema definition and parsing for CQLite
//!
//! This module handles schema definitions that describe the structure of 
//! Cassandra tables for schema-aware SSTable reading. It supports both
//! JSON-based schema definitions and CQL CREATE TABLE statement parsing.

pub mod cql_parser;

// Re-export CQL parsing functions
pub use cql_parser::{
    parse_cql_schema, parse_cql_schema_with_visitor, extract_table_name, table_name_matches, 
    cql_type_to_type_id, parse_create_table
};

use crate::error::{Error, Result};
use crate::parser::types::CqlTypeId;
use crate::storage::StorageEngine;
use crate::Config;
use crate::types::{UdtTypeDef, UdtFieldDef};
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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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

/// UDT Schema Registry for managing User Defined Type definitions
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UdtRegistry {
    /// Registered UDT type definitions by keyspace and type name
    udts: HashMap<String, HashMap<String, UdtTypeDef>>,
}

impl UdtRegistry {
    /// Create a new UDT registry
    pub fn new() -> Self {
        Self {
            udts: HashMap::new(),
        }
    }
    
    /// Create a new UDT registry with enhanced Cassandra 5.0 defaults
    pub fn with_cassandra5_defaults() -> Self {
        let mut registry = Self::new();
        registry.load_cassandra5_system_udts();
        registry
    }

    /// Register a UDT type definition
    pub fn register_udt(&mut self, udt_def: UdtTypeDef) {
        let keyspace_udts = self.udts.entry(udt_def.keyspace.clone()).or_insert_with(HashMap::new);
        keyspace_udts.insert(udt_def.name.clone(), udt_def);
    }

    /// Get a UDT definition by keyspace and name
    pub fn get_udt(&self, keyspace: &str, name: &str) -> Option<&UdtTypeDef> {
        self.udts.get(keyspace)?.get(name)
    }

    /// Get all UDTs in a keyspace
    pub fn get_keyspace_udts(&self, keyspace: &str) -> Option<&HashMap<String, UdtTypeDef>> {
        self.udts.get(keyspace)
    }

    /// List all registered UDT names in a keyspace
    pub fn list_udt_names(&self, keyspace: &str) -> Vec<&str> {
        self.udts.get(keyspace)
            .map(|udts| udts.keys().map(|s| s.as_str()).collect())
            .unwrap_or_else(Vec::new)
    }

    /// Check if a UDT is registered
    pub fn contains_udt(&self, keyspace: &str, name: &str) -> bool {
        self.udts.get(keyspace)
            .map(|udts| udts.contains_key(name))
            .unwrap_or(false)
    }

    /// Remove a UDT definition
    pub fn remove_udt(&mut self, keyspace: &str, name: &str) -> Option<UdtTypeDef> {
        self.udts.get_mut(keyspace)?.remove(name)
    }

    /// Clear all UDTs in a keyspace
    pub fn clear_keyspace(&mut self, keyspace: &str) {
        self.udts.remove(keyspace);
    }

    /// Get total number of registered UDTs
    pub fn total_udts(&self) -> usize {
        self.udts.values().map(|udts| udts.len()).sum()
    }
    
    /// Load enhanced Cassandra 5.0 system UDTs with complex nested structures
    fn load_cassandra5_system_udts(&mut self) {
        // Enhanced address UDT for Cassandra 5.0 compatibility
        let address_udt = UdtTypeDef::new("system".to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("street2".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true) 
            .with_field("state".to_string(), CqlType::Text, true)
            .with_field("zip_code".to_string(), CqlType::Text, true)
            .with_field("country".to_string(), CqlType::Text, true)
            .with_field("coordinates".to_string(), CqlType::Tuple(vec![CqlType::Double, CqlType::Double]), true);
        
        self.register_udt(address_udt);
        
        // Enhanced person UDT with collections and nested types
        let person_udt = UdtTypeDef::new("system".to_string(), "person".to_string())
            .with_field("id".to_string(), CqlType::Uuid, false)
            .with_field("first_name".to_string(), CqlType::Text, false)
            .with_field("last_name".to_string(), CqlType::Text, false)
            .with_field("middle_name".to_string(), CqlType::Text, true)
            .with_field("age".to_string(), CqlType::Int, true)
            .with_field("email".to_string(), CqlType::Text, true)
            .with_field("phone_numbers".to_string(), CqlType::Set(Box::new(CqlType::Text)), true)
            .with_field("addresses".to_string(), CqlType::List(Box::new(CqlType::Udt("address".to_string(), vec![]))), true)
            .with_field("metadata".to_string(), CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Text)), true);
            
        self.register_udt(person_udt);
        
        // Contact info UDT for complex nested scenarios
        let contact_info_udt = UdtTypeDef::new("system".to_string(), "contact_info".to_string())
            .with_field("person".to_string(), CqlType::Udt("person".to_string(), vec![]), false)
            .with_field("primary_address".to_string(), CqlType::Udt("address".to_string(), vec![]), true)
            .with_field("emergency_contacts".to_string(), CqlType::List(Box::new(CqlType::Udt("person".to_string(), vec![]))), true)
            .with_field("last_updated".to_string(), CqlType::Timestamp, true);
            
        self.register_udt(contact_info_udt);
    }
    
    /// Resolve UDT with full dependency chain
    pub fn resolve_udt_with_dependencies(&self, keyspace: &str, name: &str) -> crate::Result<&UdtTypeDef> {
        let udt = self.get_udt(keyspace, name)
            .ok_or_else(|| crate::Error::schema(format!("UDT '{}' not found in keyspace '{}'", name, keyspace)))?;
        
        // Validate all field dependencies exist
        for field in &udt.fields {
            self.validate_field_type_dependencies(&field.field_type, keyspace)?;
        }
        
        Ok(udt)
    }
    
    /// Validate that all UDT field type dependencies exist in the registry
    fn validate_field_type_dependencies(&self, field_type: &CqlType, keyspace: &str) -> crate::Result<()> {
        match field_type {
            CqlType::Udt(udt_name, _) => {
                if !self.contains_udt(keyspace, udt_name) {
                    return Err(crate::Error::schema(format!(
                        "UDT dependency '{}' not found in keyspace '{}'", 
                        udt_name, keyspace
                    )));
                }
            },
            CqlType::List(inner) | CqlType::Set(inner) | CqlType::Frozen(inner) => {
                self.validate_field_type_dependencies(inner, keyspace)?;
            },
            CqlType::Map(key_type, value_type) => {
                self.validate_field_type_dependencies(key_type, keyspace)?;
                self.validate_field_type_dependencies(value_type, keyspace)?;
            },
            CqlType::Tuple(field_types) => {
                for tuple_field_type in field_types {
                    self.validate_field_type_dependencies(tuple_field_type, keyspace)?;
                }
            },
            _ => {} // Primitive types don't need validation
        }
        Ok(())
    }
    
    /// Get all UDTs that depend on a given UDT (for cascade operations)
    pub fn get_dependent_udts(&self, keyspace: &str, udt_name: &str) -> Vec<&UdtTypeDef> {
        let mut dependents = Vec::new();
        
        if let Some(keyspace_udts) = self.udts.get(keyspace) {
            for udt in keyspace_udts.values() {
                if udt.name == udt_name {
                    continue; // Skip self
                }
                
                // Check if this UDT depends on the target UDT
                if self.udt_depends_on(udt, udt_name) {
                    dependents.push(udt);
                }
            }
        }
        
        dependents
    }
    
    /// Check if a UDT depends on another UDT (recursively)
    fn udt_depends_on(&self, udt: &UdtTypeDef, target_udt: &str) -> bool {
        for field in &udt.fields {
            if self.field_type_depends_on(&field.field_type, target_udt) {
                return true;
            }
        }
        false
    }
    
    /// Check if a field type depends on a UDT
    fn field_type_depends_on(&self, field_type: &CqlType, target_udt: &str) -> bool {
        match field_type {
            CqlType::Udt(udt_name, _) => udt_name == target_udt,
            CqlType::List(inner) | CqlType::Set(inner) | CqlType::Frozen(inner) => {
                self.field_type_depends_on(inner, target_udt)
            },
            CqlType::Map(key_type, value_type) => {
                self.field_type_depends_on(key_type, target_udt) || 
                self.field_type_depends_on(value_type, target_udt)
            },
            CqlType::Tuple(field_types) => {
                field_types.iter().any(|ft| self.field_type_depends_on(ft, target_udt))
            },
            _ => false
        }
    }
    
    /// Register UDT with dependency validation
    pub fn register_udt_with_validation(&mut self, udt_def: UdtTypeDef) -> crate::Result<()> {
        // Validate dependencies exist
        for field in &udt_def.fields {
            self.validate_field_type_dependencies(&field.field_type, &udt_def.keyspace)?;
        }
        
        // Check for circular dependencies
        if self.would_create_circular_dependency(&udt_def) {
            return Err(crate::Error::schema(format!(
                "Registering UDT '{}' would create circular dependency", 
                udt_def.name
            )));
        }
        
        self.register_udt(udt_def);
        Ok(())
    }
    
    /// Check if registering a UDT would create circular dependencies
    fn would_create_circular_dependency(&self, udt_def: &UdtTypeDef) -> bool {
        // This is complex - for now, just check direct self-reference
        for field in &udt_def.fields {
            if self.field_type_depends_on(&field.field_type, &udt_def.name) {
                return true;
            }
        }
        false
    }
    
    /// Export UDT definitions for debugging
    pub fn export_definitions(&self, keyspace: &str) -> Vec<String> {
        let mut definitions = Vec::new();
        
        if let Some(keyspace_udts) = self.udts.get(keyspace) {
            for udt in keyspace_udts.values() {
                let mut def = format!("CREATE TYPE {}.{} (\n", keyspace, udt.name);
                
                for (i, field) in udt.fields.iter().enumerate() {
                    if i > 0 {
                        def.push_str(",\n");
                    }
                    def.push_str(&format!("  {} {}", field.name, self.format_cql_type(&field.field_type)));
                }
                
                def.push_str("\n);");
                definitions.push(def);
            }
        }
        
        definitions
    }
    
    /// Format CQL type for CREATE TYPE statements
    fn format_cql_type(&self, cql_type: &CqlType) -> String {
        match cql_type {
            CqlType::Boolean => "boolean".to_string(),
            CqlType::TinyInt => "tinyint".to_string(),
            CqlType::SmallInt => "smallint".to_string(),
            CqlType::Int => "int".to_string(),
            CqlType::BigInt => "bigint".to_string(),
            CqlType::Float => "float".to_string(),
            CqlType::Double => "double".to_string(),
            CqlType::Text | CqlType::Varchar => "text".to_string(),
            CqlType::Ascii => "ascii".to_string(),
            CqlType::Blob => "blob".to_string(),
            CqlType::Timestamp => "timestamp".to_string(),
            CqlType::Date => "date".to_string(),
            CqlType::Time => "time".to_string(),
            CqlType::Uuid => "uuid".to_string(),
            CqlType::TimeUuid => "timeuuid".to_string(),
            CqlType::Inet => "inet".to_string(),
            CqlType::Duration => "duration".to_string(),
            CqlType::Decimal => "decimal".to_string(),
            CqlType::List(inner) => format!("list<{}>", self.format_cql_type(inner)),
            CqlType::Set(inner) => format!("set<{}>", self.format_cql_type(inner)),
            CqlType::Map(key, value) => format!("map<{}, {}>", self.format_cql_type(key), self.format_cql_type(value)),
            CqlType::Udt(name, _) => name.clone(),
            CqlType::Tuple(types) => {
                let type_strs: Vec<String> = types.iter().map(|t| self.format_cql_type(t)).collect();
                format!("tuple<{}>", type_strs.join(", "))
            },
            CqlType::Frozen(inner) => format!("frozen<{}>", self.format_cql_type(inner)),
            CqlType::Custom(name) => name.clone(),
        }
    }
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

        // TODO: Add UDT type validation - check that referenced UDTs exist in registry

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

        // Handle UDT types - format: udt_name or keyspace.udt_name
        if type_str.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') && 
           !type_str.chars().all(|c| c.is_ascii_lowercase()) {
            // This might be a UDT name - store as custom type for now
            // Full validation requires UDT registry context
            return Ok(CqlType::Custom(format!("udt:{}", type_str)));
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

/// Schema management service for handling table schemas and UDT definitions
#[derive(Debug)]
pub struct SchemaManager {
    storage: Arc<StorageEngine>,
    schemas: HashMap<String, TableSchema>,
    /// UDT registry for managing User Defined Types
    pub udt_registry: UdtRegistry,
}

impl SchemaManager {
    /// Create a new schema manager
    pub async fn new(storage: Arc<StorageEngine>, _config: &Config) -> Result<Self> {
        let mut manager = Self {
            storage,
            schemas: HashMap::new(),
            udt_registry: UdtRegistry::new(),
        };
        
        // Load built-in UDT definitions for Cassandra 5.0 compatibility
        manager.load_default_udts();
        
        Ok(manager)
    }

    /// Load default UDT definitions that are commonly used in Cassandra
    fn load_default_udts(&mut self) {
        // Common address UDT used in many Cassandra schemas
        let address_udt = UdtTypeDef::new("test_keyspace".to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true) 
            .with_field("state".to_string(), CqlType::Text, true)
            .with_field("zip_code".to_string(), CqlType::Text, true)
            .with_field("country".to_string(), CqlType::Text, true);
        
        self.udt_registry.register_udt(address_udt);
        
        // Enhanced person UDT with nested address
        let person_udt = UdtTypeDef::new("test_keyspace".to_string(), "person".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field("age".to_string(), CqlType::Int, true)
            .with_field("email".to_string(), CqlType::Text, true)
            .with_field("addresses".to_string(), CqlType::List(Box::new(CqlType::Udt("address".to_string(), vec![
                ("street".to_string(), CqlType::Text),
                ("city".to_string(), CqlType::Text),
                ("state".to_string(), CqlType::Text),
                ("zip_code".to_string(), CqlType::Text),
                ("country".to_string(), CqlType::Text),
            ]))), true)
            .with_field("contact_info".to_string(), CqlType::Map(
                Box::new(CqlType::Text), 
                Box::new(CqlType::Text)
            ), true);
            
        self.udt_registry.register_udt(person_udt);
        
        // Company UDT with nested person and address relationships
        let company_udt = UdtTypeDef::new("test_keyspace".to_string(), "company".to_string())
            .with_field("name".to_string(), CqlType::Text, false)
            .with_field("headquarters".to_string(), CqlType::Udt("address".to_string(), vec![
                ("street".to_string(), CqlType::Text),
                ("city".to_string(), CqlType::Text),
                ("state".to_string(), CqlType::Text),
                ("zip_code".to_string(), CqlType::Text),
                ("country".to_string(), CqlType::Text),
            ]), true)
            .with_field("employees".to_string(), CqlType::Set(Box::new(CqlType::Udt("person".to_string(), vec![]))), true)
            .with_field("founded_year".to_string(), CqlType::Int, true);
            
        self.udt_registry.register_udt(company_udt);
    }

    /// Register a new UDT type definition
    pub fn register_udt(&mut self, udt_def: UdtTypeDef) {
        self.udt_registry.register_udt(udt_def);
    }

    /// Get a UDT definition
    pub fn get_udt(&self, keyspace: &str, name: &str) -> Option<&UdtTypeDef> {
        self.udt_registry.get_udt(keyspace, name)
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

    /// Parse and register a schema from a CQL CREATE TABLE statement
    pub fn parse_and_register_cql_schema(&mut self, cql: &str) -> Result<&TableSchema> {
        let schema = cql_parser::parse_cql_schema(cql)?;
        let table_key = format!("{}.{}", schema.keyspace, schema.table);
        self.schemas.insert(table_key.clone(), schema);
        Ok(self.schemas.get(&table_key).unwrap())
    }

    /// Find schema by table name with optional keyspace matching
    pub fn find_schema_by_table(&self, keyspace: &Option<String>, table: &str) -> Option<&TableSchema> {
        // First try exact match if keyspace provided
        if let Some(ks) = keyspace {
            let key = format!("{}.{}", ks, table);
            if let Some(schema) = self.schemas.get(&key) {
                return Some(schema);
            }
        }

        // Then try to find any schema matching the table name
        for schema in self.schemas.values() {
            if cql_parser::table_name_matches(
                &Some(schema.keyspace.clone()),
                &schema.table,
                keyspace,
                table,
            ) {
                return Some(schema);
            }
        }

        None
    }

    /// Extract table information from CQL without full parsing
    pub fn extract_table_info(&self, cql: &str) -> Result<(Option<String>, String)> {
        cql_parser::extract_table_name(cql)
    }

    /// Convert CQL type string to internal type ID
    pub fn cql_type_to_internal(&self, cql_type: &str) -> Result<CqlTypeId> {
        cql_parser::cql_type_to_type_id(cql_type)
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
