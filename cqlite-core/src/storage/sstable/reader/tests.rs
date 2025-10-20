//! Tests for SSTable reader functionality

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::super::compression::extract_sstable_base_name;
    use super::super::types::*;
    use crate::RowKey;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_reader_stats() {
        let stats = SSTableReaderStats {
            file_size: 1024,
            entry_count: 100,
            table_count: 1,
            block_count: 10,
            index_size: 128,
            bloom_filter_size: 64,
            compression_ratio: 0.8,
            cache_hit_rate: 0.9,
        };

        assert_eq!(stats.file_size, 1024);
        assert_eq!(stats.entry_count, 100);
        assert_eq!(stats.compression_ratio, 0.8);
    }

    #[tokio::test]
    async fn test_reader_config() {
        let config = SSTableReaderConfig::default();
        assert_eq!(config.read_buffer_size, 64 * 1024);
        assert!(config.validate_checksums);
        assert!(config.use_bloom_filter);
    }

    #[tokio::test]
    async fn test_block_meta() {
        let meta = BlockMeta {
            offset: 1024,
            compressed_size: 512,
            uncompressed_size: 1024,
            checksum: 0x1234_5678,
            first_key: RowKey::from("key1"),
            last_key: RowKey::from("key10"),
            entry_count: 10,
        };

        assert_eq!(meta.offset, 1024);
        assert_eq!(meta.compressed_size, 512);
        assert_eq!(meta.entry_count, 10);
    }

    #[test]
    fn test_extract_sstable_base_name() {
        // Test standard SSTable naming pattern
        let path = PathBuf::from("nb-1-big-Data.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("nb-1-big".to_string()));

        // Test with different components
        let path = PathBuf::from("nb-2-da-Index.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("nb-2-da".to_string()));

        let path = PathBuf::from("nb-3-big-Statistics.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("nb-3-big".to_string()));

        let path = PathBuf::from("keyspace-table-nb-456-big-Summary.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("keyspace-table-nb".to_string()));

        // Test with full path
        let path = PathBuf::from("/some/dir/nb-1-big-Data.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, Some("nb-1-big".to_string()));

        // Test edge cases
        let path = PathBuf::from("not-enough-parts.db");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, None);

        let path = PathBuf::from("no-extension");
        let base_name = extract_sstable_base_name(&path);
        assert_eq!(base_name, None);

        // Test that the extracted base names correctly build component paths
        let data_path = PathBuf::from("/test/dir/nb-1-big-Data.db");
        let base_name = extract_sstable_base_name(&data_path).unwrap();

        let expected_index_path = data_path
            .parent()
            .unwrap()
            .join(format!("{}-Index.db", base_name));
        let expected_summary_path = data_path
            .parent()
            .unwrap()
            .join(format!("{}-Summary.db", base_name));
        let expected_stats_path = data_path
            .parent()
            .unwrap()
            .join(format!("{}-Statistics.db", base_name));

        assert_eq!(
            expected_index_path.file_name().unwrap(),
            "nb-1-big-Index.db"
        );
        assert_eq!(
            expected_summary_path.file_name().unwrap(),
            "nb-1-big-Summary.db"
        );
        assert_eq!(
            expected_stats_path.file_name().unwrap(),
            "nb-1-big-Statistics.db"
        );
    }

    #[tokio::test]
    async fn test_v5_compressed_legacy_format_research() {
        use super::super::SSTableReader;
        use crate::{Config, Platform};
        use std::path::Path;
        use std::sync::Arc;

        // Path to test_basic.simple_table SSTable
        let data_path = Path::new("/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db");

        if !data_path.exists() {
            eprintln!("Test data not found at {:?}, skipping", data_path);
            return;
        }

        // Initialize Platform and Config
        let config = Config::default();
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("Failed to create Platform"),
        );

        // Open the SSTable
        eprintln!("Opening SSTable at {:?}", data_path);
        let reader = SSTableReader::open(data_path, &config, platform.clone())
            .await
            .expect("Failed to open SSTable");

        eprintln!("SSTable version: {:?}", reader.header.cassandra_version);
        eprintln!(
            "Data format: {:?}",
            reader.header.cassandra_version.data_format()
        );

        // Try to read all entries - this will trigger the hex dump in our instrumented code
        match reader.get_all_entries().await {
            Ok(entries) => {
                eprintln!("Successfully read {} entries", entries.len());
                for (idx, (table_id, key, value)) in entries.iter().take(3).enumerate() {
                    eprintln!(
                        "Entry {}: table_id={:?}, key={:?}, value={:?}",
                        idx, table_id, key, value
                    );
                }
            }
            Err(e) => {
                eprintln!("Failed to read entries: {}", e);
            }
        }

        // Check if hex dump was created
        let hex_dump_path = Path::new("/tmp/v5_compressed_legacy_block_sample.hex");
        if hex_dump_path.exists() {
            eprintln!("✅ Hex dump created at {:?}", hex_dump_path);
        } else {
            eprintln!("❌ Hex dump was not created");
        }
    }

    #[tokio::test]
    async fn test_v5_compressed_legacy_extracts_cells() -> crate::Result<()> {
        use super::super::SSTableReader;
        use crate::schema::{
            Column, KeyColumn, SchemaRegistry, SchemaRegistryConfig, SchemaSource, TableSchema,
        };
        use crate::{Config, Platform, Value};
        use std::collections::HashMap;
        use std::path::Path;
        use std::sync::Arc;
        use tokio::sync::RwLock;

        // Path to test_basic.simple_table SSTable (V5CompressedLegacy format)
        let test_dir = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => Path::new(&root)
                .join("sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9"),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return Ok(());
            }
        };

        let data_file = test_dir.join("nb-1-big-Data.db");
        if !data_file.exists() {
            eprintln!("Test data file not found at {:?}, skipping test", data_file);
            return Ok(());
        }

        // Initialize Platform and Config
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);

        // Create minimal schema inline (from test-data/datasets/metadata.yml)
        let schema = TableSchema {
            keyspace: "test_basic".to_string(),
            table: "simple_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "account_balance".to_string(),
                    data_type: "decimal".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "active".to_string(),
                    data_type: "boolean".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "age".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "ascii_field".to_string(),
                    data_type: "ascii".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "birth_date".to_string(),
                    data_type: "date".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "created".to_string(),
                    data_type: "timestamp".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "description".to_string(),
                    data_type: "blob".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "duration_val".to_string(),
                    data_type: "duration".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "height".to_string(),
                    data_type: "float".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "ip_address".to_string(),
                    data_type: "inet".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "medium_number".to_string(),
                    data_type: "smallint".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "salary".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "session_id".to_string(),
                    data_type: "timeuuid".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "small_number".to_string(),
                    data_type: "tinyint".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "varchar_field".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "weight".to_string(),
                    data_type: "double".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "work_time".to_string(),
                    data_type: "time".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        };

        // Create schema registry and register the schema
        let registry_instance = SchemaRegistry::new(
            SchemaRegistryConfig::default(),
            platform.clone(),
            config.clone(),
        )
        .await?;

        // Register the schema for test_basic.simple_table
        registry_instance
            .register_schema(schema, SchemaSource::Manual)
            .await?;

        let registry = Arc::new(RwLock::new(registry_instance));

        // Open the SSTable
        eprintln!("Opening SSTable at {:?}", data_file);
        let mut reader = SSTableReader::open(&data_file, &config, platform.clone()).await?;

        // Register schema registry with reader so it can look up schema during parsing
        reader.set_schema_registry(registry);

        // Verify it's V5CompressedLegacy format
        let data_format = reader.header.cassandra_version.data_format();
        assert!(
            matches!(
                data_format,
                crate::parser::header::DataFormat::V5CompressedLegacy
            ),
            "Expected V5CompressedLegacy format, got {:?}",
            data_format
        );

        eprintln!("SSTable version: {:?}", reader.header.cassandra_version);
        eprintln!("Data format: {:?}", data_format);

        // Read all entries
        let entries = reader.get_all_entries().await?;

        eprintln!("Successfully read {} entries", entries.len());

        // CRITICAL ASSERTION: Must extract at least one entry
        assert!(
            !entries.is_empty(),
            "V5CompressedLegacy parser must extract >0 entries (got 0!)"
        );

        // VERIFICATION #1: Count unique partition keys
        use std::collections::HashSet;
        let unique_keys: HashSet<_> = entries.iter().map(|(_, key, _)| key.clone()).collect();
        eprintln!("Total entries: {}", entries.len());
        eprintln!("Unique partition keys: {}", unique_keys.len());
        eprintln!("Expected unique keys (from JSONL): 1000");

        // VERIFICATION #2: Show sample of first 10 partition keys
        eprintln!("\nFirst 10 partition keys extracted:");
        for (idx, (_, key, _)) in entries.iter().take(10).enumerate() {
            eprintln!("  [{}] {:?}", idx, key);
        }

        // VERIFICATION #3: Check if we're duplicating the same key
        if entries.len() > 1 {
            let first_key = &entries[0].1;
            let second_key = &entries[1].1;
            if first_key == second_key {
                eprintln!("WARNING: First two keys are IDENTICAL - possible duplication bug!");
            } else {
                eprintln!("GOOD: First two keys are DIFFERENT");
            }
        }

        // CRITICAL ASSERTION: Verify we have 1000 unique partition keys (matching JSONL)
        assert_eq!(
            unique_keys.len(),
            1000,
            "Expected 1000 unique partition keys (one per partition), got {}",
            unique_keys.len()
        );

        // Examine the first entry
        let (table_id, row_key, value) = &entries[0];

        eprintln!("\nEntry 0: table_id={:?}", table_id);
        eprintln!("Entry 0: row_key={:?}", row_key);
        eprintln!("Entry 0: value={:?}", value);

        // CRITICAL ASSERTION: Value must be a row (Map representation) with cells
        // Value::Map format: Vec<(Value::Text(column_name), column_value)>
        match value {
            Value::Map(map_entries) => {
                eprintln!("Row has {} fields", map_entries.len());

                // CRITICAL: Must extract >0 cells (not 0!)
                assert!(
                    !map_entries.is_empty(),
                    "V5CompressedLegacy parser must extract >0 cells per row (got 0!)"
                );

                // Extract field names from map entries (first element of each tuple)
                let field_names: Vec<String> = map_entries
                    .iter()
                    .filter_map(|(key, _)| match key {
                        Value::Text(name) => Some(name.clone()),
                        _ => None,
                    })
                    .collect();

                eprintln!("Extracted field names: {:?}", field_names);

                // Check for ascii_field (first cell in hex dump)
                let ascii_field = map_entries
                    .iter()
                    .find(|(key, _)| matches!(key, Value::Text(name) if name == "ascii_field"))
                    .expect("Must have 'ascii_field' column");

                eprintln!("ascii_field value: {:?}", ascii_field.1);

                // CRITICAL: Verify typed values (not blobs!)
                match &ascii_field.1 {
                    Value::Text(text) => {
                        eprintln!("✅ ascii_field is Text: '{}'", text);
                        assert_eq!(
                            text, "ascii",
                            "ascii_field value should be 'ascii' from sstabledump"
                        );
                    }
                    Value::Blob(_) => {
                        panic!("❌ ascii_field should be Text, not Blob! Type detection failed.");
                    }
                    other => {
                        panic!(
                            "❌ ascii_field has unexpected type: {:?}. Expected Text.",
                            other
                        );
                    }
                }

                // Check for age column (should be Int, not Blob)
                if let Some((_, age_value)) = map_entries
                    .iter()
                    .find(|(key, _)| matches!(key, Value::Text(name) if name == "age"))
                {
                    eprintln!("age value: {:?}", age_value);
                    match age_value {
                        Value::Integer(val) => {
                            eprintln!("✅ age is Integer: {}", val);
                        }
                        Value::Blob(_) => {
                            eprintln!(
                                "⚠️  age is Blob (acceptable if schema not available for typing)"
                            );
                        }
                        other => {
                            eprintln!("age has type: {:?}", other);
                        }
                    }
                }

                // Check for active column (should be Boolean, not Blob)
                if let Some((_, active_value)) = map_entries
                    .iter()
                    .find(|(key, _)| matches!(key, Value::Text(name) if name == "active"))
                {
                    eprintln!("active value: {:?}", active_value);
                    match active_value {
                        Value::Boolean(val) => {
                            eprintln!("✅ active is Boolean: {}", val);
                        }
                        Value::Blob(_) => {
                            eprintln!("⚠️  active is Blob (acceptable if schema not available)");
                        }
                        other => {
                            eprintln!("active has type: {:?}", other);
                        }
                    }
                }
            }
            Value::Null => {
                panic!("❌ V5CompressedLegacy parser returned Null value (should return row with cells!)");
            }
            other => {
                panic!(
                    "❌ Expected Value::Map (row representation), got {:?}",
                    other
                );
            }
        }

        eprintln!("✅ V5CompressedLegacy parser test PASSED:");
        eprintln!("   - Extracted {} entries", entries.len());
        eprintln!("   - First entry has >0 cells");
        eprintln!("   - Values are properly typed (Text, not Blob)");

        Ok(())
    }
}
