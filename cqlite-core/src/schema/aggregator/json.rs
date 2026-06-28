//! JSON schema handling for the schema aggregator.
//!
//! Parses JSON schema files (minimal single-table format and full
//! multi-table + UDT format) into the intermediate [`ParsedSchema`] format,
//! and converts the JSON-shaped definitions into [`TableSchema`]/[`UdtTypeDef`].

use std::collections::HashMap;
use std::path::Path;

use crate::error::{Error, Result};
use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use crate::types::UdtTypeDef;

use super::{ParsedSchema, SchemaAggregator};

/// JSON schema formats
#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub(super) enum JsonSchemaFormat {
    /// Minimal format: single table with "table" field
    Minimal(MinimalTableSchema),
    /// Full format: with "tables" array and optional "udts" array
    Full(FullSchema),
}

/// Minimal JSON schema format (single table)
#[derive(Debug, serde::Deserialize)]
pub(super) struct MinimalTableSchema {
    keyspace: String,
    table: String,
    columns: Vec<JsonColumn>,
    #[serde(default)]
    partition_keys: Vec<String>,
    #[serde(default)]
    primary_key: Vec<String>, // Synonym for partition_keys when no clustering
    #[serde(default)]
    clustering_keys: Vec<JsonClusteringKey>,
    /// Dropped-column drop times (column → drop_time_micros), used for
    /// dropped-column filtering during compaction (#904/#847).
    #[serde(default)]
    dropped_columns: HashMap<String, i64>,
}

/// Full JSON schema format (multiple tables + UDTs)
/// Note: Both `udts` and `tables` are optional to support UDT-only or table-only files
#[derive(Debug, serde::Deserialize)]
pub(super) struct FullSchema {
    keyspace: String,
    #[serde(default)]
    udts: Vec<JsonUdt>,
    #[serde(default)]
    tables: Vec<JsonTable>,
}

/// JSON table definition
#[derive(Debug, serde::Deserialize)]
struct JsonTable {
    name: String,
    columns: Vec<JsonColumn>,
    #[serde(default)]
    partition_keys: Vec<String>,
    #[serde(default)]
    primary_key: Vec<String>,
    #[serde(default)]
    clustering_keys: Vec<JsonClusteringKey>,
    /// Dropped-column drop times (column → drop_time_micros), used for
    /// dropped-column filtering during compaction (#904/#847).
    #[serde(default)]
    dropped_columns: HashMap<String, i64>,
}

/// JSON column definition
#[derive(Debug, serde::Deserialize)]
struct JsonColumn {
    name: String,
    #[serde(alias = "data_type")]
    r#type: String,
    #[serde(default)]
    nullable: bool,
}

/// JSON clustering key definition
#[derive(Debug, serde::Deserialize)]
struct JsonClusteringKey {
    name: String,
    #[serde(alias = "data_type")]
    r#type: String,
    #[serde(default)]
    order: Option<String>,
}

/// JSON UDT definition
#[derive(Debug, serde::Deserialize)]
struct JsonUdt {
    name: String,
    fields: Vec<JsonUdtField>,
}

/// JSON UDT field definition
#[derive(Debug, serde::Deserialize)]
struct JsonUdtField {
    name: String,
    #[serde(alias = "data_type")]
    r#type: String,
    #[serde(default = "default_nullable")]
    nullable: bool,
}

fn default_nullable() -> bool {
    true
}

impl SchemaAggregator {
    /// Parse a JSON file (either minimal or full format)
    pub(super) async fn parse_json_file(&self, path: &Path) -> Result<Option<ParsedSchema>> {
        let content = std::fs::read_to_string(path)?;

        let json_schema: JsonSchemaFormat = serde_json::from_str(&content)
            .map_err(|e| Error::schema(format!("Invalid JSON in {}: {}", path.display(), e)))?;

        match json_schema {
            JsonSchemaFormat::Minimal(minimal) => self.parse_minimal_format(path, minimal).await,
            JsonSchemaFormat::Full(full) => self.parse_full_format(path, full).await,
        }
    }

    /// Parse minimal JSON format (single table)
    async fn parse_minimal_format(
        &self,
        _path: &Path,
        minimal: MinimalTableSchema,
    ) -> Result<Option<ParsedSchema>> {
        let table_schema = self.convert_minimal_to_table_schema(minimal)?;
        let keyspace = table_schema.keyspace.clone();

        let mut tables = HashMap::new();
        // Key by fully-qualified name (keyspace.table) to avoid multi-keyspace collisions
        let qualified_name = format!("{}.{}", table_schema.keyspace, table_schema.table);
        tables.insert(qualified_name, table_schema);

        Ok(Some(ParsedSchema {
            keyspace,
            tables,
            udts: HashMap::new(),
        }))
    }

    /// Parse full JSON format (multiple tables + UDTs)
    async fn parse_full_format(
        &self,
        _path: &Path,
        full: FullSchema,
    ) -> Result<Option<ParsedSchema>> {
        let keyspace = full.keyspace.clone();
        let mut tables = HashMap::new();
        let mut udts = HashMap::new();

        // Parse UDTs
        for udt_json in full.udts {
            let udt_def = self.convert_json_udt_to_typedef(&keyspace, udt_json)?;
            // Key by fully-qualified name (keyspace.typename) to avoid multi-keyspace collisions
            let qualified_name = format!("{}.{}", udt_def.keyspace, udt_def.name);
            udts.insert(qualified_name, udt_def);
        }

        // Parse tables
        for table_json in full.tables {
            let table_schema = self.convert_json_table_to_table_schema(&keyspace, table_json)?;
            // Key by fully-qualified name (keyspace.table) to avoid multi-keyspace collisions
            let qualified_name = format!("{}.{}", table_schema.keyspace, table_schema.table);
            tables.insert(qualified_name, table_schema);
        }

        Ok(Some(ParsedSchema {
            keyspace,
            tables,
            udts,
        }))
    }

    /// Convert minimal JSON format to TableSchema
    pub(super) fn convert_minimal_to_table_schema(
        &self,
        minimal: MinimalTableSchema,
    ) -> Result<TableSchema> {
        // Determine partition keys (prefer partition_keys, fallback to primary_key)
        let partition_key_names = if !minimal.partition_keys.is_empty() {
            minimal.partition_keys
        } else if !minimal.primary_key.is_empty() {
            minimal.primary_key
        } else {
            return Err(Error::schema(
                "Table must have partition_keys or primary_key".to_string(),
            ));
        };

        // Build columns
        let columns: Vec<Column> = minimal
            .columns
            .iter()
            .map(|col| Column {
                name: col.name.clone(),
                data_type: col.r#type.clone(),
                nullable: col.nullable,
                default: None,
                is_static: false, // TODO: minimal schema doesn't track static columns yet
            })
            .collect();

        // Build partition keys
        let partition_keys: Vec<KeyColumn> = partition_key_names
            .iter()
            .enumerate()
            .map(|(pos, name)| {
                let col = minimal
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .ok_or_else(|| {
                        Error::schema(format!("Partition key '{}' not found in columns", name))
                    })?;

                Ok(KeyColumn {
                    name: col.name.clone(),
                    data_type: col.r#type.clone(),
                    position: pos,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Build clustering keys
        let clustering_keys: Vec<ClusteringColumn> = minimal
            .clustering_keys
            .iter()
            .enumerate()
            .map(|(pos, ck)| ClusteringColumn {
                name: ck.name.clone(),
                data_type: ck.r#type.clone(),
                position: pos,
                order: ck.order.as_deref().map(|s| s.into()).unwrap_or_default(),
            })
            .collect();

        let schema = TableSchema {
            keyspace: minimal.keyspace,
            table: minimal.table,
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
            dropped_columns: minimal.dropped_columns,
        };

        schema.validate()?;
        Ok(schema)
    }

    /// Convert JSON table to TableSchema
    fn convert_json_table_to_table_schema(
        &self,
        keyspace: &str,
        table_json: JsonTable,
    ) -> Result<TableSchema> {
        let partition_key_names = if !table_json.partition_keys.is_empty() {
            table_json.partition_keys
        } else if !table_json.primary_key.is_empty() {
            table_json.primary_key
        } else {
            return Err(Error::schema(format!(
                "Table '{}' must have partition_keys or primary_key",
                table_json.name
            )));
        };

        let columns: Vec<Column> = table_json
            .columns
            .iter()
            .map(|col| Column {
                name: col.name.clone(),
                data_type: col.r#type.clone(),
                nullable: col.nullable,
                default: None,
                is_static: false, // TODO: JSON schema doesn't track static columns yet
            })
            .collect();

        let partition_keys: Vec<KeyColumn> = partition_key_names
            .iter()
            .enumerate()
            .map(|(pos, name)| {
                let col = table_json
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .ok_or_else(|| {
                        Error::schema(format!(
                            "Partition key '{}' not found in columns of table '{}'",
                            name, table_json.name
                        ))
                    })?;

                Ok(KeyColumn {
                    name: col.name.clone(),
                    data_type: col.r#type.clone(),
                    position: pos,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let clustering_keys: Vec<ClusteringColumn> = table_json
            .clustering_keys
            .iter()
            .enumerate()
            .map(|(pos, ck)| ClusteringColumn {
                name: ck.name.clone(),
                data_type: ck.r#type.clone(),
                position: pos,
                order: ck.order.as_deref().map(|s| s.into()).unwrap_or_default(),
            })
            .collect();

        let schema = TableSchema {
            keyspace: keyspace.to_string(),
            table: table_json.name,
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
            dropped_columns: table_json.dropped_columns,
        };

        schema.validate()?;
        Ok(schema)
    }

    /// Convert JSON UDT to UdtTypeDef
    fn convert_json_udt_to_typedef(&self, keyspace: &str, udt_json: JsonUdt) -> Result<UdtTypeDef> {
        let mut udt_def = UdtTypeDef::new(keyspace.to_string(), udt_json.name);

        for field in udt_json.fields {
            let field_type = crate::schema::CqlType::parse(&field.r#type)?;
            udt_def = udt_def.with_field(field.name, field_type, field.nullable);
        }

        Ok(udt_def)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::super::{AggregatorConfig, LoadErrorType, SchemaAggregator};
    use super::MinimalTableSchema;
    use crate::platform::Platform;
    use crate::schema::registry::{SchemaRegistry, SchemaRegistryConfig};
    use crate::schema::UdtRegistry;
    use crate::Config;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_load_single_json_file() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let json_content = r#"
        {
            "keyspace": "test_ks",
            "table": "users",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "name", "type": "text"}
            ],
            "partition_keys": ["id"],
            "clustering_keys": []
        }
        "#;

        let json_path = write_file(temp_dir.path(), "users.json", json_content);
        let result = aggregator.load_from_paths(&[json_path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 0);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_minimal_json_preserves_dropped_columns() {
        // A JSON schema may carry dropped-column drop times (#904/#847). The
        // dropped column stays declared in `columns` (decode contract) and is
        // listed in `dropped_columns` with its drop time in micros.
        let (aggregator, _temp_dir) = setup_test_aggregator().await;

        let json_content = r#"
        {
            "keyspace": "test_ks",
            "table": "events",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "legacy", "type": "int"}
            ],
            "partition_keys": ["id"],
            "clustering_keys": [],
            "dropped_columns": {"legacy": 1700000000000000}
        }
        "#;

        let minimal: MinimalTableSchema =
            serde_json::from_str(json_content).expect("minimal schema parses");
        let schema = aggregator
            .convert_minimal_to_table_schema(minimal)
            .expect("conversion succeeds");

        assert_eq!(
            schema.dropped_columns.get("legacy"),
            Some(&1_700_000_000_000_000_i64),
            "dropped_columns drop time must survive JSON loading"
        );
    }

    #[tokio::test]
    async fn test_directory_scanning_lexical_order() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // Create files in non-lexical order, verify they're processed lexically
        write_file(
            temp_dir.path(),
            "c_table.json",
            r#"{"keyspace":"ks","table":"c","columns":[{"name":"id","type":"uuid"}],"partition_keys":["id"]}"#,
        );
        write_file(
            temp_dir.path(),
            "a_table.json",
            r#"{"keyspace":"ks","table":"a","columns":[{"name":"id","type":"uuid"}],"partition_keys":["id"]}"#,
        );
        write_file(
            temp_dir.path(),
            "b_table.json",
            r#"{"keyspace":"ks","table":"b","columns":[{"name":"id","type":"uuid"}],"partition_keys":["id"]}"#,
        );

        let result = aggregator
            .load_from_paths(&[temp_dir.path().to_path_buf()])
            .await
            .unwrap();

        assert_eq!(result.schemas_loaded, 3);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_last_wins_for_duplicate_tables() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let first_json = r#"
        {
            "keyspace": "ks",
            "table": "users",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "name", "type": "text"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let second_json = r#"
        {
            "keyspace": "ks",
            "table": "users",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "name", "type": "text"},
                {"name": "email", "type": "text"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let path1 = write_file(temp_dir.path(), "users_v1.json", first_json);
        let path2 = write_file(temp_dir.path(), "users_v2.json", second_json);

        let result = aggregator.load_from_paths(&[path1, path2]).await.unwrap();

        // Last wins, so we should have 1 schema (the second one)
        assert_eq!(result.schemas_loaded, 1);

        // Verify the schema has 3 columns (from second definition)
        let registry = aggregator.registry.read().await;
        let schema = registry.get_schema("ks", "users").await.unwrap();
        assert_eq!(schema.columns.len(), 3);
    }

    #[tokio::test]
    async fn test_two_pass_udt_then_tables() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let full_schema_json = r#"
        {
            "keyspace": "ks",
            "udts": [
                {
                    "name": "address",
                    "fields": [
                        {"name": "street", "type": "text"},
                        {"name": "city", "type": "text"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "addr", "type": "frozen<address>"}
                    ],
                    "partition_keys": ["id"],
                    "clustering_keys": []
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "schema.json", full_schema_json);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 1);
        assert!(result.errors.is_empty());

        // Verify UDT was registered
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(udt_registry.contains_udt("ks", "address"));
    }

    /// Test for Issue #230: REPL fails when schema directory contains UDT-only JSON files
    /// UDT-only files (without "tables" array) should parse successfully
    #[tokio::test]
    async fn test_udt_only_json_schema_issue_230() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // This JSON has "udts" but no "tables" - should work after fix
        let udt_only_json = r#"
        {
            "keyspace": "test_keyspace",
            "udts": [
                {
                    "name": "address_type",
                    "fields": [
                        { "name": "street", "type": "text" },
                        { "name": "city", "type": "text" },
                        { "name": "zip", "type": "int" }
                    ]
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "address.json", udt_only_json);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // Should load successfully with no errors
        assert!(
            result.errors.is_empty(),
            "Expected no errors but got: {:?}",
            result.errors
        );
        assert_eq!(result.udts_loaded, 1, "Expected 1 UDT to be loaded");
        assert_eq!(
            result.schemas_loaded, 0,
            "Expected 0 tables (UDT-only file)"
        );

        // Verify UDT was registered
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(
            udt_registry.contains_udt("test_keyspace", "address_type"),
            "UDT address_type should be registered in test_keyspace"
        );
    }

    /// Test symmetric case: table-only JSON files (tables without UDTs)
    /// This validates that #[serde(default)] works for both fields
    #[tokio::test]
    async fn test_table_only_json_schema_symmetry() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // This JSON has "tables" but no "udts" - symmetric to Issue #230 fix
        let table_only_json = r#"
        {
            "keyspace": "test_keyspace",
            "tables": [
                {
                    "name": "simple_table",
                    "columns": [
                        { "name": "id", "type": "uuid" },
                        { "name": "data", "type": "text" }
                    ],
                    "partition_keys": ["id"],
                    "clustering_keys": []
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "table_only.json", table_only_json);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // Should load successfully with no errors
        assert!(
            result.errors.is_empty(),
            "Expected no errors but got: {:?}",
            result.errors
        );
        assert_eq!(result.udts_loaded, 0, "Expected 0 UDTs (table-only file)");
        assert_eq!(result.schemas_loaded, 1, "Expected 1 table to be loaded");

        // Verify table was registered
        let registry = aggregator.registry.read().await;
        assert!(
            registry
                .get_schema("test_keyspace", "simple_table")
                .await
                .is_ok(),
            "Table simple_table should be registered in test_keyspace"
        );
    }

    #[tokio::test]
    async fn test_invalid_json_error_collection() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let invalid_json = r#"{"keyspace": "ks", "table": "broken""#; // Missing closing brace

        let path = write_file(temp_dir.path(), "broken.json", invalid_json);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 0);
        assert!(!result.errors.is_empty());
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::InvalidJson
        ));
    }

    #[tokio::test]
    async fn test_minimal_format_with_primary_key_synonym() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let json_content = r#"
        {
            "keyspace": "ks",
            "table": "items",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "data", "type": "text"}
            ],
            "primary_key": ["id"]
        }
        "#;

        let path = write_file(temp_dir.path(), "items.json", json_content);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_data_type_alias_support() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        let json_content = r#"
        {
            "keyspace": "ks",
            "table": "legacy",
            "columns": [
                {"name": "id", "data_type": "uuid"},
                {"name": "value", "data_type": "text"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let path = write_file(temp_dir.path(), "legacy.json", json_content);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        assert_eq!(result.schemas_loaded, 1);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_graceful_degradation_false_fails_on_invalid_json() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let registry_config = SchemaRegistryConfig::default();
        let registry = Arc::new(RwLock::new(
            SchemaRegistry::new(registry_config, platform, config)
                .await
                .unwrap(),
        ));
        let udt_registry = Arc::new(RwLock::new(UdtRegistry::new()));

        // Create aggregator with strict mode (graceful_degradation = false)
        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: false,
                validate_udt_dependencies: true,
            },
        );

        // Create invalid JSON file followed by valid JSON file
        let invalid_json = r#"{"keyspace": "ks", "table": "broken""#; // Missing closing brace
        let valid_json = r#"
        {
            "keyspace": "ks",
            "table": "valid_table",
            "columns": [
                {"name": "id", "type": "uuid"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let invalid_path = write_file(temp_dir.path(), "01_invalid.json", invalid_json);
        let valid_path = write_file(temp_dir.path(), "02_valid.json", valid_json);

        let result = aggregator
            .load_from_paths(&[invalid_path, valid_path])
            .await
            .unwrap();

        // In strict mode, should fail immediately after first error
        assert_eq!(result.schemas_loaded, 0);
        assert_eq!(result.udts_loaded, 0);
        assert!(!result.errors.is_empty());
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::InvalidJson
        ));
    }

    #[tokio::test]
    async fn test_graceful_degradation_true_continues_after_error() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let registry_config = SchemaRegistryConfig::default();
        let registry = Arc::new(RwLock::new(
            SchemaRegistry::new(registry_config, platform, config)
                .await
                .unwrap(),
        ));
        let udt_registry = Arc::new(RwLock::new(UdtRegistry::new()));

        // Create aggregator with graceful mode (graceful_degradation = true)
        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: true,
                validate_udt_dependencies: true,
            },
        );

        // Create invalid JSON file followed by valid JSON file
        let invalid_json = r#"{"keyspace": "ks", "table": "broken""#; // Missing closing brace
        let valid_json = r#"
        {
            "keyspace": "ks",
            "table": "valid_table",
            "columns": [
                {"name": "id", "type": "uuid"}
            ],
            "partition_keys": ["id"]
        }
        "#;

        let invalid_path = write_file(temp_dir.path(), "01_invalid.json", invalid_json);
        let valid_path = write_file(temp_dir.path(), "02_valid.json", valid_json);

        let result = aggregator
            .load_from_paths(&[invalid_path, valid_path])
            .await
            .unwrap();

        // In graceful mode, should continue and load valid table
        assert_eq!(result.schemas_loaded, 1);
        assert_eq!(result.udts_loaded, 0);
        assert_eq!(result.errors.len(), 1); // Should collect the error but continue
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::InvalidJson
        ));
    }

    #[tokio::test]
    #[ignore = "Test fails because register_udt_with_validation does not catch invalid UDT references - pre-existing limitation"]
    async fn test_graceful_degradation_false_fails_on_invalid_udt() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let registry_config = SchemaRegistryConfig::default();
        let registry = Arc::new(RwLock::new(
            SchemaRegistry::new(registry_config, platform, config)
                .await
                .unwrap(),
        ));
        let udt_registry = Arc::new(RwLock::new(UdtRegistry::new()));

        // Create aggregator with strict mode
        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: false,
                validate_udt_dependencies: true,
            },
        );

        // Create schema with UDT that references non-existent UDT
        let schema_with_invalid_udt = r#"
        {
            "keyspace": "ks",
            "udts": [
                {
                    "name": "user_type",
                    "fields": [
                        {"name": "addr", "type": "frozen<nonexistent_udt>"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "data", "type": "text"}
                    ],
                    "partition_keys": ["id"]
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "schema.json", schema_with_invalid_udt);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // In strict mode, should fail on UDT validation and NOT load tables
        // NOTE: This test currently fails because register_udt_with_validation
        // does not validate nested UDT references. This is a pre-existing limitation.
        // TODO: Implement proper UDT dependency validation before enabling this test.
        assert_eq!(result.schemas_loaded, 0); // Tables should NOT be loaded
        assert_eq!(result.udts_loaded, 0); // UDT should not be loaded
        assert!(!result.errors.is_empty());
        // Error should be about circular/missing dependency
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::CircularUdtDependency
        ));
    }

    #[tokio::test]
    #[ignore = "Test fails because register_udt_with_validation does not catch invalid UDT references - pre-existing limitation"]
    async fn test_graceful_degradation_true_loads_tables_despite_invalid_udt() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let registry_config = SchemaRegistryConfig::default();
        let registry = Arc::new(RwLock::new(
            SchemaRegistry::new(registry_config, platform, config)
                .await
                .unwrap(),
        ));
        let udt_registry = Arc::new(RwLock::new(UdtRegistry::new()));

        // Create aggregator with graceful mode
        let mut aggregator = SchemaAggregator::new(
            registry,
            udt_registry,
            AggregatorConfig {
                graceful_degradation: true,
                validate_udt_dependencies: true,
            },
        );

        // Create schema with UDT that references non-existent UDT
        let schema_with_invalid_udt = r#"
        {
            "keyspace": "ks",
            "udts": [
                {
                    "name": "user_type",
                    "fields": [
                        {"name": "addr", "type": "frozen<nonexistent_udt>"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "data", "type": "text"}
                    ],
                    "partition_keys": ["id"]
                }
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "schema.json", schema_with_invalid_udt);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // In graceful mode, should continue and load tables despite UDT failure
        assert_eq!(result.schemas_loaded, 1); // Table SHOULD be loaded
        assert_eq!(result.udts_loaded, 0); // UDT should not be loaded
        assert_eq!(result.errors.len(), 1); // Should collect the error
        assert!(matches!(
            result.errors[0].error_type,
            LoadErrorType::CircularUdtDependency
        ));
    }

    #[tokio::test]
    async fn test_error_schema_validation_not_mislabeled_as_file_read() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // JSON file with structural validation error (missing partition_keys)
        let invalid_schema = r#"
        {
            "keyspace": "ks",
            "table": "broken_table",
            "columns": [
                {"name": "id", "type": "uuid"},
                {"name": "data", "type": "text"}
            ]
        }
        "#;

        let path = write_file(temp_dir.path(), "invalid_schema.json", invalid_schema);
        let result = aggregator.load_from_paths(&[path]).await.unwrap();

        // Should fail with ValidationFailed, NOT FileRead
        assert_eq!(result.schemas_loaded, 0);
        assert!(!result.errors.is_empty());
        assert!(
            matches!(result.errors[0].error_type, LoadErrorType::ValidationFailed),
            "Expected ValidationFailed for missing partition_keys, got: {:?}",
            result.errors[0].error_type
        );
        assert!(
            result.errors[0].message.contains("partition_keys")
                || result.errors[0].message.contains("primary_key"),
            "Error message should mention missing keys: {}",
            result.errors[0].message
        );
    }

    #[tokio::test]
    async fn test_multi_keyspace_json_files_no_collision() {
        let (mut aggregator, temp_dir) = setup_test_aggregator().await;

        // First JSON file for keyspace ks_a
        let json_ks_a = r#"
        {
            "keyspace": "ks_a",
            "udts": [
                {
                    "name": "address",
                    "fields": [
                        {"name": "street", "type": "text"},
                        {"name": "city", "type": "text"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "name", "type": "text"}
                    ],
                    "partition_keys": ["id"]
                }
            ]
        }
        "#;

        // Second JSON file for keyspace ks_b with SAME UDT and table names
        let json_ks_b = r#"
        {
            "keyspace": "ks_b",
            "udts": [
                {
                    "name": "address",
                    "fields": [
                        {"name": "country", "type": "text"},
                        {"name": "postal_code", "type": "text"}
                    ]
                }
            ],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "uuid"},
                        {"name": "email", "type": "text"}
                    ],
                    "partition_keys": ["id"]
                }
            ]
        }
        "#;

        let path_a = write_file(temp_dir.path(), "ks_a.json", json_ks_a);
        let path_b = write_file(temp_dir.path(), "ks_b.json", json_ks_b);

        let result = aggregator.load_from_paths(&[path_a, path_b]).await.unwrap();

        // Both UDTs and both tables should be loaded (no collision)
        assert_eq!(
            result.udts_loaded, 2,
            "Expected 2 UDTs from different keyspaces"
        );
        assert_eq!(
            result.schemas_loaded, 2,
            "Expected 2 tables from different keyspaces"
        );
        assert!(
            result.errors.is_empty(),
            "Expected no errors, got: {:?}",
            result.errors
        );

        // Verify both UDTs are registered with correct keyspaces
        let udt_registry = aggregator.udt_registry.read().await;
        assert!(
            udt_registry.contains_udt("ks_a", "address"),
            "ks_a.address should be registered"
        );
        assert!(
            udt_registry.contains_udt("ks_b", "address"),
            "ks_b.address should be registered"
        );

        // Verify both tables are registered with correct keyspaces and different columns
        let registry = aggregator.registry.read().await;
        let schema_a = registry.get_schema("ks_a", "users").await.unwrap();
        assert_eq!(schema_a.keyspace, "ks_a");
        assert_eq!(schema_a.table, "users");
        assert!(
            schema_a.columns.iter().any(|c| c.name == "name"),
            "ks_a.users should have 'name' column"
        );

        let schema_b = registry.get_schema("ks_b", "users").await.unwrap();
        assert_eq!(schema_b.keyspace, "ks_b");
        assert_eq!(schema_b.table, "users");
        assert!(
            schema_b.columns.iter().any(|c| c.name == "email"),
            "ks_b.users should have 'email' column"
        );
    }
}
