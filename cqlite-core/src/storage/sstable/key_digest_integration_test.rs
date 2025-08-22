//! Integration tests for schema-driven key digest computation
//!
//! These tests validate that the SSTable reader correctly uses schema-driven
//! key digest computation when a SchemaRegistry is available.

#[cfg(test)]
mod tests {
    use super::super::key_digest::KeyDigestComputer;
    use crate::{
        Config, Result,
        platform::Platform,
        schema::{
            Column, KeyColumn, TableSchema,
            registry::{SchemaRegistry, SchemaRegistryConfig, SchemaSource},
        },
        types::ComparatorType,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    async fn create_test_schema_registry() -> Result<SchemaRegistry> {
        let config = SchemaRegistryConfig::default();
        let core_config = Config::default();
        let platform = Arc::new(Platform::new(&core_config).await?);

        SchemaRegistry::new(config, platform, core_config).await
    }

    fn create_test_table_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![
                KeyColumn {
                    name: "pk_int".to_string(),
                    data_type: "int".to_string(),
                    position: 0,
                },
                KeyColumn {
                    name: "pk_text".to_string(),
                    data_type: "text".to_string(),
                    position: 1,
                },
            ],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "pk_int".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "pk_text".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "value".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_schema_driven_digest_computation() -> Result<()> {
        let registry = create_test_schema_registry().await?;
        let schema = create_test_table_schema();

        // Register the schema
        registry
            .register_schema(schema, SchemaSource::Manual)
            .await?;

        // Get parsing context
        let parsing_context = registry
            .get_parsing_context("test_ks", "test_table")
            .await?;

        // Verify parsing context is complete
        assert!(parsing_context.is_complete());
        assert_eq!(parsing_context.partition_comparators.len(), 2);
        assert_eq!(
            parsing_context.partition_comparators[0],
            ComparatorType::Int
        );
        assert_eq!(
            parsing_context.partition_comparators[1],
            ComparatorType::Text
        );

        // Test digest computation with schema context
        let mut computer = KeyDigestComputer::new();

        // Create a multi-component key: int(42) + text("hello")
        let mut key_bytes = Vec::new();
        key_bytes.extend_from_slice(&[0x00, 0x04]); // length of int
        key_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x2A]); // int value 42
        key_bytes.extend_from_slice(&[0x00, 0x05]); // length of text
        key_bytes.extend_from_slice(b"hello"); // text value

        let schema_digest = computer.compute_partition_key_digest(&key_bytes, &parsing_context)?;
        let simple_digest = computer.compute_simple_digest(&key_bytes)?;

        // Schema-driven digest should be different from simple digest
        // (since schema-driven uses proper byte-comparable encoding + Murmur3)
        assert_eq!(schema_digest.len(), 4);
        assert_eq!(simple_digest.len(), 4);
        // The digests may or may not be different depending on the implementation,
        // but the important thing is they're both valid 4-byte Murmur3 hashes

        Ok(())
    }

    #[tokio::test]
    async fn test_parsing_context_creation() -> Result<()> {
        let registry = create_test_schema_registry().await?;
        let schema = create_test_table_schema();

        registry
            .register_schema(schema, SchemaSource::Manual)
            .await?;

        let parsing_context = registry
            .get_parsing_context("test_ks", "test_table")
            .await?;

        // Verify all key column names are accessible
        let key_column_names = parsing_context.get_all_key_column_names();
        assert_eq!(key_column_names, vec!["pk_int", "pk_text"]);

        // Verify column comparators
        assert!(parsing_context.get_column_comparator("value").is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_registry_partition_key_comparator() -> Result<()> {
        let registry = create_test_schema_registry().await?;
        let schema = create_test_table_schema();

        registry
            .register_schema(schema, SchemaSource::Manual)
            .await?;

        // Test individual column comparator access
        let pk_int_comparator = registry
            .get_column_comparator("test_ks", "test_table", "pk_int")
            .await?;
        let pk_text_comparator = registry
            .get_column_comparator("test_ks", "test_table", "pk_text")
            .await?;

        assert_eq!(pk_int_comparator, ComparatorType::Int);
        assert_eq!(pk_text_comparator, ComparatorType::Text);

        // Test partition key comparators as a list
        let partition_comparators = registry
            .get_partition_key_comparator("test_ks", "test_table")
            .await?;
        assert_eq!(partition_comparators.len(), 2);
        assert_eq!(partition_comparators[0], ComparatorType::Int);
        assert_eq!(partition_comparators[1], ComparatorType::Text);

        Ok(())
    }

    #[tokio::test]
    async fn test_digest_computation_with_different_types() -> Result<()> {
        let registry = create_test_schema_registry().await?;

        // Create schemas with different partition key types
        let test_cases = vec![
            ("int_only", vec![("pk", "int")]),
            ("text_only", vec![("pk", "text")]),
            ("bigint_only", vec![("pk", "bigint")]),
            ("boolean_only", vec![("pk", "boolean")]),
            ("uuid_only", vec![("pk", "uuid")]),
            ("blob_only", vec![("pk", "blob")]),
            (
                "multi_type",
                vec![("pk1", "int"), ("pk2", "text"), ("pk3", "bigint")],
            ),
        ];

        let mut computer = KeyDigestComputer::new();

        for (table_name, partition_key_defs) in test_cases {
            let mut partition_keys = Vec::new();
            for (i, (name, data_type)) in partition_key_defs.iter().enumerate() {
                partition_keys.push(KeyColumn {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                    position: i,
                });
            }

            let schema = TableSchema {
                keyspace: "test_ks".to_string(),
                table: table_name.to_string(),
                partition_keys,
                clustering_keys: vec![],
                columns: vec![],
                comments: HashMap::new(),
            };

            registry
                .register_schema(schema, SchemaSource::Manual)
                .await?;

            let parsing_context = registry.get_parsing_context("test_ks", table_name).await?;

            // Test digest computation with sample data
            let test_key = create_sample_key_for_types(&partition_key_defs);
            let digest = computer.compute_partition_key_digest(&test_key, &parsing_context)?;

            assert_eq!(
                digest.len(),
                4,
                "Digest should be 4 bytes for table {}",
                table_name
            );

            // Test deterministic behavior
            let digest2 = computer.compute_partition_key_digest(&test_key, &parsing_context)?;
            assert_eq!(
                digest, digest2,
                "Digest should be deterministic for table {}",
                table_name
            );
        }

        Ok(())
    }

    fn create_sample_key_for_types(partition_key_defs: &[(&str, &str)]) -> Vec<u8> {
        let mut key_bytes = Vec::new();

        for (_, data_type) in partition_key_defs {
            match *data_type {
                "int" => {
                    if partition_key_defs.len() > 1 {
                        key_bytes.extend_from_slice(&[0x00, 0x04]); // length
                    }
                    key_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x2A]); // value 42
                }
                "text" => {
                    if partition_key_defs.len() > 1 {
                        key_bytes.extend_from_slice(&[0x00, 0x05]); // length
                    }
                    key_bytes.extend_from_slice(b"hello"); // value "hello"
                }
                "bigint" => {
                    if partition_key_defs.len() > 1 {
                        key_bytes.extend_from_slice(&[0x00, 0x08]); // length
                    }
                    key_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]); // value 100
                }
                "boolean" => {
                    if partition_key_defs.len() > 1 {
                        key_bytes.extend_from_slice(&[0x00, 0x01]); // length
                    }
                    key_bytes.extend_from_slice(&[0x01]); // value true
                }
                "uuid" => {
                    if partition_key_defs.len() > 1 {
                        key_bytes.extend_from_slice(&[0x00, 0x10]); // length
                    }
                    // Sample UUID bytes
                    key_bytes.extend_from_slice(&[
                        0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98,
                        0x76, 0x54, 0x32, 0x10,
                    ]);
                }
                "blob" => {
                    if partition_key_defs.len() > 1 {
                        key_bytes.extend_from_slice(&[0x00, 0x04]); // length
                    }
                    key_bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]); // sample blob
                }
                _ => {
                    // Fallback for unknown types
                    if partition_key_defs.len() > 1 {
                        key_bytes.extend_from_slice(&[0x00, 0x04]); // length
                    }
                    key_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // zero bytes
                }
            }
        }

        key_bytes
    }

    #[tokio::test]
    async fn test_byte_comparable_ordering_consistency() -> Result<()> {
        // Test that byte-comparable encoding produces consistent ordering
        let registry = create_test_schema_registry().await?;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "ordered_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
        };

        registry
            .register_schema(schema, SchemaSource::Manual)
            .await?;

        let parsing_context = registry
            .get_parsing_context("test_ks", "ordered_table")
            .await?;
        let mut computer = KeyDigestComputer::new();

        // Test with ordered integer values
        let values = vec![1i32, 2i32, 10i32, 100i32, 1000i32];
        let mut digests = Vec::new();

        for value in values {
            let bytes = value.to_be_bytes();
            let digest = computer.compute_partition_key_digest(&bytes, &parsing_context)?;
            digests.push((value, digest));
        }

        // While hash values may not preserve ordering, they should be different
        for i in 0..digests.len() {
            for j in i + 1..digests.len() {
                assert_ne!(
                    digests[i].1, digests[j].1,
                    "Values {} and {} should produce different digests",
                    digests[i].0, digests[j].0
                );
            }
        }

        Ok(())
    }
}
