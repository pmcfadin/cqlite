//! Test data fixtures and builders
//!
//! This module provides utilities for creating test data fixtures,
//! including SSTable files, schema definitions, and sample data.

use super::TestResult;
use serde_json::Value;
use std::path::PathBuf;

/// Builder for creating test data fixtures
#[derive(Debug)]
pub struct TestDataBuilder {
    output_dir: PathBuf,
    fixtures: Vec<TestFixture>,
}

/// Represents a test fixture
#[derive(Debug, Clone)]
pub struct TestFixture {
    pub name: String,
    pub file_path: PathBuf,
    pub content: FixtureContent,
}

/// Different types of fixture content
#[derive(Debug, Clone)]
pub enum FixtureContent {
    Json(Value),
    Text(String),
    Binary(Vec<u8>),
    SSTable(SSTableData),
    Schema(SchemaData),
}

/// SSTable fixture builder
#[derive(Debug)]
pub struct SSTableFixture {
    table_name: String,
    data_rows: Vec<Vec<Value>>,
    column_names: Vec<String>,
    compression: Option<String>,
    metadata: std::collections::HashMap<String, Value>,
}

/// Schema fixture builder
#[derive(Debug)]
pub struct SchemaFixture {
    keyspace: String,
    tables: Vec<TableSchema>,
    custom_types: Vec<CustomType>,
}

/// SSTable data structure
#[derive(Debug, Clone)]
pub struct SSTableData {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub rows: Vec<Row>,
    pub metadata: std::collections::HashMap<String, Value>,
}

/// Schema data structure
#[derive(Debug, Clone)]
pub struct SchemaData {
    pub keyspace: String,
    pub tables: Vec<TableSchema>,
    pub custom_types: Vec<CustomType>,
}

/// Table schema definition
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub options: std::collections::HashMap<String, Value>,
}

/// Column definition
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_static: bool,
}

/// Custom type definition
#[derive(Debug, Clone)]
pub struct CustomType {
    pub name: String,
    pub fields: Vec<ColumnDef>,
}

/// Row data
#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

impl TestDataBuilder {
    /// Create a new test data builder
    pub fn new<P: Into<PathBuf>>(output_dir: P) -> TestResult<Self> {
        let output_dir = output_dir.into();
        std::fs::create_dir_all(&output_dir)?;

        Ok(Self {
            output_dir,
            fixtures: Vec::new(),
        })
    }

    /// Add a fixture to the builder
    pub fn add_fixture(mut self, fixture: TestFixture) -> Self {
        self.fixtures.push(fixture);
        self
    }

    /// Add an SSTable fixture
    pub fn add_sstable<S: Into<String>>(
        mut self,
        name: S,
        sstable: SSTableFixture,
    ) -> TestResult<Self> {
        let name = name.into();
        let file_path = self.output_dir.join(format!("{}.sstable", name));

        let fixture = TestFixture {
            name: name.clone(),
            file_path,
            content: FixtureContent::SSTable(sstable.build()),
        };

        self.fixtures.push(fixture);
        Ok(self)
    }

    /// Add a schema fixture
    pub fn add_schema<S: Into<String>>(
        mut self,
        name: S,
        schema: SchemaFixture,
    ) -> TestResult<Self> {
        let name = name.into();
        let file_path = self.output_dir.join(format!("{}.json", name));

        let fixture = TestFixture {
            name: name.clone(),
            file_path,
            content: FixtureContent::Schema(schema.build()),
        };

        self.fixtures.push(fixture);
        Ok(self)
    }

    /// Add a JSON fixture
    pub fn add_json<S: Into<String>>(mut self, name: S, json: Value) -> Self {
        let name = name.into();
        let file_path = self.output_dir.join(format!("{}.json", name));

        let fixture = TestFixture {
            name: name.clone(),
            file_path,
            content: FixtureContent::Json(json),
        };

        self.fixtures.push(fixture);
        self
    }

    /// Add a text fixture
    pub fn add_text<S: Into<String>, T: Into<String>>(mut self, name: S, text: T) -> Self {
        let name = name.into();
        let file_path = self.output_dir.join(format!("{}.txt", name));

        let fixture = TestFixture {
            name: name.clone(),
            file_path,
            content: FixtureContent::Text(text.into()),
        };

        self.fixtures.push(fixture);
        self
    }

    /// Build and write all fixtures to disk
    pub fn build(self) -> TestResult<Vec<TestFixture>> {
        let mut built_fixtures = Vec::new();

        for fixture in self.fixtures {
            fixture.write_to_disk()?;
            built_fixtures.push(fixture);
        }

        Ok(built_fixtures)
    }

    /// Create common test fixtures
    pub fn create_common_fixtures(output_dir: PathBuf) -> TestResult<Vec<TestFixture>> {
        let mut builder = Self::new(output_dir)?;

        // Add basic schema fixture
        let schema = SchemaFixture::new("test_keyspace")
            .add_table(
                TableSchema::new("users")
                    .add_column("id", "UUID", false)
                    .add_column("name", "TEXT", false)
                    .add_column("email", "TEXT", true)
                    .add_column("age", "INT", true)
                    .with_primary_key(vec!["id".to_string()]),
            )?
            .add_table(
                TableSchema::new("posts")
                    .add_column("user_id", "UUID", false)
                    .add_column("post_id", "UUID", false)
                    .add_column("title", "TEXT", false)
                    .add_column("content", "TEXT", true)
                    .add_column("created_at", "TIMESTAMP", false)
                    .with_primary_key(vec!["user_id".to_string(), "post_id".to_string()]),
            )?;

        builder = builder.add_schema("basic_schema", schema)?;

        // Add basic SSTable fixture
        let sstable = SSTableFixture::new("users")
            .add_column("id", "UUID")
            .add_column("name", "TEXT")
            .add_column("email", "TEXT")
            .add_column("age", "INT")
            .add_row(vec![
                Value::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
                Value::String("John Doe".to_string()),
                Value::String("john@example.com".to_string()),
                Value::Number(serde_json::Number::from(30)),
            ])
            .add_row(vec![
                Value::String("550e8400-e29b-41d4-a716-446655440001".to_string()),
                Value::String("Jane Smith".to_string()),
                Value::String("jane@example.com".to_string()),
                Value::Number(serde_json::Number::from(25)),
            ]);

        builder = builder.add_sstable("basic_users", sstable)?;

        // Add sample CQL queries
        builder = builder.add_text("sample_queries", 
            "CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};\n\
             USE test;\n\
             CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, email TEXT, age INT);\n\
             INSERT INTO users (id, name, email, age) VALUES (uuid(), 'Test User', 'test@example.com', 30);\n\
             SELECT * FROM users;"
        );

        builder.build()
    }
}

impl TestFixture {
    /// Write fixture content to disk
    pub fn write_to_disk(&self) -> TestResult<()> {
        match &self.content {
            FixtureContent::Json(json) => {
                let content = serde_json::to_string_pretty(json)?;
                std::fs::write(&self.file_path, content)?;
            }
            FixtureContent::Text(text) => {
                std::fs::write(&self.file_path, text)?;
            }
            FixtureContent::Binary(data) => {
                std::fs::write(&self.file_path, data)?;
            }
            FixtureContent::SSTable(sstable_data) => {
                // Convert SSTable data to JSON for now
                // In a real implementation, this would create actual SSTable files
                let json = serde_json::to_value(sstable_data)?;
                let content = serde_json::to_string_pretty(&json)?;
                std::fs::write(&self.file_path, content)?;
            }
            FixtureContent::Schema(schema_data) => {
                let json = serde_json::to_value(schema_data)?;
                let content = serde_json::to_string_pretty(&json)?;
                std::fs::write(&self.file_path, content)?;
            }
        }
        Ok(())
    }

    /// Read fixture content from disk
    pub fn read_from_disk(&self) -> TestResult<String> {
        Ok(std::fs::read_to_string(&self.file_path)?)
    }
}

impl SSTableFixture {
    /// Create a new SSTable fixture
    pub fn new<S: Into<String>>(table_name: S) -> Self {
        Self {
            table_name: table_name.into(),
            data_rows: Vec::new(),
            column_names: Vec::new(),
            compression: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Add a column definition
    pub fn add_column<S: Into<String>, T: Into<String>>(mut self, name: S, data_type: T) -> Self {
        self.column_names.push(name.into());
        self
    }

    /// Add a data row
    pub fn add_row(mut self, row: Vec<Value>) -> Self {
        self.data_rows.push(row);
        self
    }

    /// Set compression type
    pub fn with_compression<S: Into<String>>(mut self, compression: S) -> Self {
        self.compression = Some(compression.into());
        self
    }

    /// Add metadata
    pub fn with_metadata<S: Into<String>>(mut self, key: S, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Build the SSTable data
    pub fn build(self) -> SSTableData {
        let columns = self
            .column_names
            .into_iter()
            .enumerate()
            .map(|(i, name)| ColumnDef {
                name,
                data_type: format!("TYPE_{}", i), // Placeholder
                is_nullable: true,
                is_static: false,
            })
            .collect();

        let rows = self
            .data_rows
            .into_iter()
            .map(|values| Row { values })
            .collect();

        SSTableData {
            table_name: self.table_name,
            columns,
            rows,
            metadata: self.metadata,
        }
    }
}

impl SchemaFixture {
    /// Create a new schema fixture
    pub fn new<S: Into<String>>(keyspace: S) -> Self {
        Self {
            keyspace: keyspace.into(),
            tables: Vec::new(),
            custom_types: Vec::new(),
        }
    }

    /// Add a table schema
    pub fn add_table(mut self, table: TableSchema) -> TestResult<Self> {
        self.tables.push(table);
        Ok(self)
    }

    /// Add a custom type
    pub fn add_custom_type(mut self, custom_type: CustomType) -> Self {
        self.custom_types.push(custom_type);
        self
    }

    /// Build the schema data
    pub fn build(self) -> SchemaData {
        SchemaData {
            keyspace: self.keyspace,
            tables: self.tables,
            custom_types: self.custom_types,
        }
    }
}

impl TableSchema {
    /// Create a new table schema
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            primary_key: Vec::new(),
            clustering_key: Vec::new(),
            options: std::collections::HashMap::new(),
        }
    }

    /// Add a column
    pub fn add_column<S: Into<String>, T: Into<String>>(
        mut self,
        name: S,
        data_type: T,
        is_nullable: bool,
    ) -> Self {
        self.columns.push(ColumnDef {
            name: name.into(),
            data_type: data_type.into(),
            is_nullable,
            is_static: false,
        });
        self
    }

    /// Set primary key
    pub fn with_primary_key(mut self, primary_key: Vec<String>) -> Self {
        self.primary_key = primary_key;
        self
    }

    /// Set clustering key
    pub fn with_clustering_key(mut self, clustering_key: Vec<String>) -> Self {
        self.clustering_key = clustering_key;
        self
    }

    /// Add table option
    pub fn with_option<S: Into<String>>(mut self, key: S, value: Value) -> Self {
        self.options.insert(key.into(), value);
        self
    }
}

// Implement Serialize for all data structures
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SerializableSSTableData {
    table_name: String,
    columns: Vec<SerializableColumnDef>,
    rows: Vec<SerializableRow>,
    metadata: std::collections::HashMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
struct SerializableSchemaData {
    keyspace: String,
    tables: Vec<SerializableTableSchema>,
    custom_types: Vec<SerializableCustomType>,
}

#[derive(Serialize, Deserialize)]
struct SerializableTableSchema {
    name: String,
    columns: Vec<SerializableColumnDef>,
    primary_key: Vec<String>,
    clustering_key: Vec<String>,
    options: std::collections::HashMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
struct SerializableColumnDef {
    name: String,
    data_type: String,
    is_nullable: bool,
    is_static: bool,
}

#[derive(Serialize, Deserialize)]
struct SerializableCustomType {
    name: String,
    fields: Vec<SerializableColumnDef>,
}

#[derive(Serialize, Deserialize)]
struct SerializableRow {
    values: Vec<Value>,
}

// Helper conversion implementations
impl From<&SSTableData> for SerializableSSTableData {
    fn from(data: &SSTableData) -> Self {
        Self {
            table_name: data.table_name.clone(),
            columns: data.columns.iter().map(|c| c.into()).collect(),
            rows: data.rows.iter().map(|r| r.into()).collect(),
            metadata: data.metadata.clone(),
        }
    }
}

impl From<&ColumnDef> for SerializableColumnDef {
    fn from(col: &ColumnDef) -> Self {
        Self {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            is_nullable: col.is_nullable,
            is_static: col.is_static,
        }
    }
}

impl From<&Row> for SerializableRow {
    fn from(row: &Row) -> Self {
        Self {
            values: row.values.clone(),
        }
    }
}

impl serde::Serialize for SSTableData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerializableSSTableData::from(self).serialize(serializer)
    }
}

impl serde::Serialize for SchemaData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let serializable = SerializableSchemaData {
            keyspace: self.keyspace.clone(),
            tables: self
                .tables
                .iter()
                .map(|t| SerializableTableSchema {
                    name: t.name.clone(),
                    columns: t.columns.iter().map(|c| c.into()).collect(),
                    primary_key: t.primary_key.clone(),
                    clustering_key: t.clustering_key.clone(),
                    options: t.options.clone(),
                })
                .collect(),
            custom_types: self
                .custom_types
                .iter()
                .map(|ct| SerializableCustomType {
                    name: ct.name.clone(),
                    fields: ct.fields.iter().map(|f| f.into()).collect(),
                })
                .collect(),
        };
        serializable.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_test_data_builder() {
        let temp_dir = TempDir::new().unwrap();
        let builder = TestDataBuilder::new(temp_dir.path()).unwrap();

        let fixtures = builder
            .add_json("test", serde_json::json!({"test": "data"}))
            .add_text("sample", "Hello, World!")
            .build()
            .unwrap();

        assert_eq!(fixtures.len(), 2);
        assert!(fixtures[0].file_path.exists());
        assert!(fixtures[1].file_path.exists());
    }

    #[test]
    fn test_sstable_fixture() {
        let sstable = SSTableFixture::new("test_table")
            .add_column("id", "UUID")
            .add_column("name", "TEXT")
            .add_row(vec![
                Value::String("test-id".to_string()),
                Value::String("test-name".to_string()),
            ])
            .with_compression("LZ4")
            .build();

        assert_eq!(sstable.table_name, "test_table");
        assert_eq!(sstable.columns.len(), 2);
        assert_eq!(sstable.rows.len(), 1);
    }

    #[test]
    fn test_schema_fixture() {
        let schema = SchemaFixture::new("test_keyspace")
            .add_table(
                TableSchema::new("users")
                    .add_column("id", "UUID", false)
                    .add_column("name", "TEXT", false)
                    .with_primary_key(vec!["id".to_string()]),
            )
            .unwrap()
            .build();

        assert_eq!(schema.keyspace, "test_keyspace");
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "users");
        assert_eq!(schema.tables[0].columns.len(), 2);
    }
}
