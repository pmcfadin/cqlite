//! Schema management for CQLite

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Error;
use crate::storage::StorageEngine;
use crate::{types::TableId, Config, DataType, Result, Value};

/// Schema manager for database metadata
#[derive(Debug)]
pub struct SchemaManager {
    /// Storage engine reference
    storage: Arc<StorageEngine>,

    /// Schema cache
    schemas: Arc<RwLock<HashMap<TableId, TableSchema>>>,

    /// Configuration
    config: Config,
}

/// Table schema definition
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table identifier
    pub table_id: TableId,

    /// Column definitions
    pub columns: Vec<ColumnSchema>,

    /// Primary key columns
    pub primary_key: Vec<String>,

    /// Schema version
    pub version: u32,

    /// Creation timestamp
    pub created_at: u64,

    /// Last modified timestamp
    pub modified_at: u64,
}

/// Column schema definition
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    /// Column name
    pub name: String,

    /// Data type
    pub data_type: DataType,

    /// Whether column allows NULL values
    pub nullable: bool,

    /// Default value
    pub default_value: Option<Value>,

    /// Whether column is part of primary key
    pub is_primary_key: bool,

    /// Column position in table
    pub position: u32,
}

impl SchemaManager {
    /// Create a new schema manager
    pub async fn new(storage: Arc<StorageEngine>, config: &Config) -> Result<Self> {
        let schemas = Arc::new(RwLock::new(HashMap::new()));

        let manager = Self {
            storage,
            schemas,
            config: config.clone(),
        };

        // Load existing schemas
        manager.load_schemas().await?;

        Ok(manager)
    }

    /// Load schemas from storage
    async fn load_schemas(&self) -> Result<()> {
        // TODO: Load schemas from storage
        Ok(())
    }

    /// Create a new table schema
    pub async fn create_table(&self, schema: TableSchema) -> Result<()> {
        // Validate schema
        self.validate_schema(&schema)?;

        // Store schema
        {
            let mut schemas = self.schemas.write().await;
            schemas.insert(schema.table_id.clone(), schema.clone());
        }

        // Persist to storage
        self.persist_schema(&schema).await?;

        Ok(())
    }

    /// Get table schema
    pub async fn get_table_schema(&self, table_id: &TableId) -> Result<Option<TableSchema>> {
        let schemas = self.schemas.read().await;
        Ok(schemas.get(table_id).cloned())
    }

    /// Update table schema
    pub async fn update_table(&self, table_id: &TableId, new_schema: TableSchema) -> Result<()> {
        // Validate schema
        self.validate_schema(&new_schema)?;

        // Update schema
        {
            let mut schemas = self.schemas.write().await;
            if let Some(existing) = schemas.get_mut(table_id) {
                existing.columns = new_schema.columns;
                existing.version += 1;
                existing.modified_at = self.current_timestamp();
            } else {
                return Err(Error::schema(format!(
                    "Table {} not found",
                    table_id.as_str()
                )));
            }
        }

        // Persist changes
        let updated_schema = self.get_table_schema(table_id).await?.unwrap();
        self.persist_schema(&updated_schema).await?;

        Ok(())
    }

    /// Drop table schema
    pub async fn drop_table(&self, table_id: &TableId) -> Result<()> {
        // Remove from cache
        {
            let mut schemas = self.schemas.write().await;
            if schemas.remove(table_id).is_none() {
                return Err(Error::schema(format!(
                    "Table {} not found",
                    table_id.as_str()
                )));
            }
        }

        // Remove from storage
        self.remove_schema(table_id).await?;

        Ok(())
    }

    /// List all tables
    pub async fn list_tables(&self) -> Vec<TableId> {
        let schemas = self.schemas.read().await;
        schemas.keys().cloned().collect()
    }

    /// Validate schema
    fn validate_schema(&self, schema: &TableSchema) -> Result<()> {
        if schema.columns.is_empty() {
            return Err(Error::schema(
                "Table must have at least one column".to_string(),
            ));
        }

        if schema.primary_key.is_empty() {
            return Err(Error::schema("Table must have a primary key".to_string()));
        }

        // Check primary key columns exist
        for pk_col in &schema.primary_key {
            if !schema.columns.iter().any(|c| &c.name == pk_col) {
                return Err(Error::schema(format!(
                    "Primary key column '{}' not found",
                    pk_col
                )));
            }
        }

        // Check for duplicate column names
        let mut column_names = std::collections::HashSet::new();
        for column in &schema.columns {
            if !column_names.insert(&column.name) {
                return Err(Error::schema(format!(
                    "Duplicate column name: {}",
                    column.name
                )));
            }
        }

        Ok(())
    }

    /// Persist schema to storage
    async fn persist_schema(&self, schema: &TableSchema) -> Result<()> {
        // TODO: Implement schema persistence
        Ok(())
    }

    /// Remove schema from storage
    async fn remove_schema(&self, table_id: &TableId) -> Result<()> {
        // TODO: Implement schema removal
        Ok(())
    }

    /// Get current timestamp
    fn current_timestamp(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }
}

impl TableSchema {
    /// Create a new table schema
    pub fn new(table_id: TableId, columns: Vec<ColumnSchema>, primary_key: Vec<String>) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        Self {
            table_id,
            columns,
            primary_key,
            version: 1,
            created_at: timestamp,
            modified_at: timestamp,
        }
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get primary key columns
    pub fn get_primary_key_columns(&self) -> Vec<&ColumnSchema> {
        self.columns
            .iter()
            .filter(|c| self.primary_key.contains(&c.name))
            .collect()
    }
}

impl ColumnSchema {
    /// Create a new column schema
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
            default_value: None,
            is_primary_key: false,
            position: 0,
        }
    }

    /// Set as primary key
    pub fn primary_key(mut self) -> Self {
        self.is_primary_key = true;
        self.nullable = false; // Primary key cannot be null
        self
    }

    /// Set default value
    pub fn default_value(mut self, value: Value) -> Self {
        self.default_value = Some(value);
        self
    }

    /// Set position
    pub fn position(mut self, position: u32) -> Self {
        self.position = position;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_schema_creation() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false).primary_key(),
            ColumnSchema::new("name".to_string(), DataType::Text, false),
            ColumnSchema::new("email".to_string(), DataType::Text, true),
        ];

        let schema = TableSchema::new(TableId::new("users"), columns, vec!["id".to_string()]);

        assert_eq!(schema.table_id.as_str(), "users");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.primary_key, vec!["id".to_string()]);
        assert_eq!(schema.version, 1);
    }

    #[test]
    fn test_column_schema_builder() {
        let column = ColumnSchema::new("id".to_string(), DataType::Integer, false)
            .primary_key()
            .position(0);

        assert_eq!(column.name, "id");
        assert_eq!(column.data_type, DataType::Integer);
        assert!(!column.nullable);
        assert!(column.is_primary_key);
        assert_eq!(column.position, 0);
    }

    #[test]
    fn test_table_schema_get_column() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Text, false),
        ];

        let schema = TableSchema::new(TableId::new("users"), columns, vec!["id".to_string()]);

        let id_column = schema.get_column("id");
        assert!(id_column.is_some());
        assert_eq!(id_column.unwrap().name, "id");

        let missing_column = schema.get_column("missing");
        assert!(missing_column.is_none());
    }
}
