//! Tests for the SchemaAwareReader module

#[cfg(test)]
mod tests {
    use super::super::schema_aware_reader::*;
    use crate::{
        Config,
        platform::Platform,
        schema::{ClusteringColumn, Column, KeyColumn, TableSchema, registry::SchemaRegistry},
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "timestamp".to_string(),
                data_type: "timestamp".to_string(),
                position: 0,
                order: "ASC".to_string(),
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "data".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        }
    }

    #[test]
    fn test_schema_validation() {
        let schema = create_test_schema();
        assert!(SchemaAwareReader::validate_schema_completeness(&schema).is_ok());
    }

    #[test]
    fn test_invalid_schema_validation() {
        let mut schema = create_test_schema();
        schema.partition_keys.clear(); // Remove partition keys

        assert!(SchemaAwareReader::validate_schema_completeness(&schema).is_err());
    }

    #[test]
    fn test_parsing_context_creation() {
        let schema = create_test_schema();
        let registry = Arc::new(SchemaRegistry::new());

        let result = SchemaAwareReader::create_parsing_context(&schema, &registry);
        assert!(result.is_ok());

        let context = result.unwrap();
        assert_eq!(context.schema.keyspace, "test_ks");
        assert_eq!(context.schema.table, "test_table");
        assert_eq!(context.partition_comparators.len(), 1);
        assert_eq!(context.clustering_comparators.len(), 1);
        assert!(context.column_comparators.contains_key("id"));
        assert!(context.column_comparators.contains_key("timestamp"));
        assert!(context.column_comparators.contains_key("data"));
    }

    #[test]
    fn test_non_contiguous_partition_keys() {
        let mut schema = create_test_schema();
        schema.partition_keys.push(KeyColumn {
            name: "other_id".to_string(),
            data_type: "text".to_string(),
            position: 2, // Non-contiguous - should be 1
        });

        assert!(SchemaAwareReader::validate_schema_completeness(&schema).is_err());
    }

    #[test]
    fn test_non_contiguous_clustering_keys() {
        let mut schema = create_test_schema();
        schema.clustering_keys.push(ClusteringColumn {
            name: "other_ts".to_string(),
            data_type: "timestamp".to_string(),
            position: 2, // Non-contiguous - should be 1
            order: "ASC".to_string(),
        });

        assert!(SchemaAwareReader::validate_schema_completeness(&schema).is_err());
    }

    #[test]
    fn test_invalid_column_types() {
        let mut schema = create_test_schema();
        schema.columns[0].data_type = "invalid_type".to_string();

        // Invalid types should be treated as custom types, so this should actually pass
        assert!(SchemaAwareReader::validate_schema_completeness(&schema).is_ok());
    }

    #[test]
    fn test_schema_aware_reader_config_defaults() {
        let config = SchemaAwareReaderConfig::default();
        assert!(config.validate_schema_completeness);
        assert!(config.strict_schema_validation);
        assert!(config.enable_format_optimizations);
        assert!(config.cache_parsed_values);
    }

    #[test]
    fn test_error_types() {
        use super::super::schema_aware_reader::SchemaAwareReaderError;

        let err = SchemaAwareReaderError::SchemaValidation("test".to_string());
        assert!(err.to_string().contains("Schema validation failed"));

        let err = SchemaAwareReaderError::IncompleteContext("test".to_string());
        assert!(err.to_string().contains("Parsing context incomplete"));

        let err = SchemaAwareReaderError::KeyValidation("test".to_string());
        assert!(err.to_string().contains("Key validation failed"));

        let err = SchemaAwareReaderError::ValueParsing {
            column: "test_col".to_string(),
            reason: "test_reason".to_string(),
        };
        assert!(err.to_string().contains("Value parsing failed"));
        assert!(err.to_string().contains("test_col"));
        assert!(err.to_string().contains("test_reason"));
    }

    #[tokio::test]
    async fn test_schema_aware_stats() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create a simple test file for the SSTable reader
        std::fs::write(temp_dir.path().join("test.sst"), b"dummy_data").unwrap();

        let schema = create_test_schema();
        let registry = Arc::new(SchemaRegistry::new());

        // This would normally fail without a proper SSTable file, but demonstrates the API
        let result = SchemaAwareReader::new(
            &temp_dir.path().join("test.sst"),
            schema,
            registry,
            &config,
            platform,
        )
        .await;

        // We expect this to fail since we don't have a real SSTable file
        assert!(result.is_err());
    }
}
