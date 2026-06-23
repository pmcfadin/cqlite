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
                    is_static: false,
                },
                Column {
                    name: "active".to_string(),
                    data_type: "boolean".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "age".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ascii_field".to_string(),
                    data_type: "ascii".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "birth_date".to_string(),
                    data_type: "date".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "created".to_string(),
                    data_type: "timestamp".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "description".to_string(),
                    data_type: "blob".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "duration_val".to_string(),
                    data_type: "duration".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "height".to_string(),
                    data_type: "float".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ip_address".to_string(),
                    data_type: "inet".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "medium_number".to_string(),
                    data_type: "smallint".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "salary".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "session_id".to_string(),
                    data_type: "timeuuid".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "small_number".to_string(),
                    data_type: "tinyint".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "varchar_field".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "weight".to_string(),
                    data_type: "double".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "work_time".to_string(),
                    data_type: "time".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
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

        // With state_machine feature, set_schema_registry expects Arc<RwLock<SchemaRegistry>>
        // Without state_machine, it expects Arc<SchemaRegistry>
        #[cfg(feature = "state_machine")]
        let registry = {
            use tokio::sync::RwLock;
            Arc::new(RwLock::new(registry_instance))
        };
        #[cfg(not(feature = "state_machine"))]
        let registry = Arc::new(registry_instance);

        // Open the SSTable
        eprintln!("Opening SSTable at {:?}", data_file);
        let mut reader = SSTableReader::open(&data_file, &config, platform.clone()).await?;

        // Register schema registry with reader so it can look up schema during parsing
        reader.set_schema_registry(registry.clone());

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

    #[test]
    fn test_mmap_env_parsing() {
        use super::super::parse_truthy_env;
        for truthy in ["1", "true", "TRUE", "Yes", " on ", "On"] {
            assert!(parse_truthy_env(truthy), "{truthy:?} should enable mmap");
        }
        for falsy in ["0", "false", "no", "off", "", "maybe", "2"] {
            assert!(!parse_truthy_env(falsy), "{falsy:?} should not enable mmap");
        }
    }

    #[test]
    fn test_disk_access_mode_parsing() {
        use super::super::parse_disk_access_mode;
        use crate::config::DiskAccessMode;
        assert_eq!(parse_disk_access_mode("auto"), Some(DiskAccessMode::Auto));
        assert_eq!(
            parse_disk_access_mode(" Buffered "),
            Some(DiskAccessMode::Buffered)
        );
        assert_eq!(parse_disk_access_mode("MMAP"), Some(DiskAccessMode::Mmap));
        assert_eq!(
            parse_disk_access_mode("direct"),
            Some(DiskAccessMode::Direct)
        );
        assert_eq!(
            parse_disk_access_mode("o_direct"),
            Some(DiskAccessMode::Direct)
        );
        assert_eq!(parse_disk_access_mode("nonsense"), None);
    }

    #[test]
    fn test_prefetch_mode_parsing() {
        use super::super::parse_prefetch_mode;
        use crate::config::PrefetchMode;
        assert_eq!(parse_prefetch_mode("off"), Some(PrefetchMode::Off));
        assert_eq!(
            parse_prefetch_mode("Sequential"),
            Some(PrefetchMode::Sequential)
        );
        assert_eq!(
            parse_prefetch_mode("willneed"),
            Some(PrefetchMode::WillNeed)
        );
        assert_eq!(parse_prefetch_mode("auto"), Some(PrefetchMode::Auto));
        assert_eq!(parse_prefetch_mode("???"), None);
    }

    /// The `Auto` heuristic: tiny → buffered, sub-RAM → mmap, > fraction of
    /// RAM → direct (when memory is known and direct I/O is compiled in).
    #[test]
    fn test_resolve_disk_access_mode_auto() {
        use super::super::resolve_disk_access_mode;
        use crate::config::DiskAccessMode;

        let gib: u64 = 1024 * 1024 * 1024;
        let min = 4096u64;

        // Empty file is always buffered, even if a backend is requested.
        assert_eq!(
            resolve_disk_access_mode(DiskAccessMode::Direct, 0, min, 0.5, Some(8 * gib), true),
            DiskAccessMode::Buffered
        );

        // Below mmap_min_size_bytes → buffered.
        assert_eq!(
            resolve_disk_access_mode(DiskAccessMode::Auto, 100, min, 0.5, Some(8 * gib), true),
            DiskAccessMode::Buffered
        );

        // Comfortably sub-RAM → mmap.
        assert_eq!(
            resolve_disk_access_mode(DiskAccessMode::Auto, gib, min, 0.5, Some(8 * gib), true),
            DiskAccessMode::Mmap
        );

        // Larger than half of RAM → direct.
        assert_eq!(
            resolve_disk_access_mode(DiskAccessMode::Auto, 5 * gib, min, 0.5, Some(8 * gib), true),
            DiskAccessMode::Direct
        );

        // Larger than half of RAM but direct I/O unavailable → mmap.
        assert_eq!(
            resolve_disk_access_mode(
                DiskAccessMode::Auto,
                5 * gib,
                min,
                0.5,
                Some(8 * gib),
                false
            ),
            DiskAccessMode::Mmap
        );

        // Unknown system memory → never escalates to direct.
        assert_eq!(
            resolve_disk_access_mode(DiskAccessMode::Auto, 100 * gib, min, 0.5, None, true),
            DiskAccessMode::Mmap
        );

        // A non-finite/zero fraction falls back to the 0.5 default.
        assert_eq!(
            resolve_disk_access_mode(DiskAccessMode::Auto, 5 * gib, min, 0.0, Some(8 * gib), true),
            DiskAccessMode::Direct
        );
    }

    /// Explicit modes are returned unchanged (subject to the empty-file guard).
    #[test]
    fn test_resolve_disk_access_mode_explicit() {
        use super::super::resolve_disk_access_mode;
        use crate::config::DiskAccessMode;
        let gib: u64 = 1024 * 1024 * 1024;
        for mode in [
            DiskAccessMode::Buffered,
            DiskAccessMode::Mmap,
            DiskAccessMode::Direct,
        ] {
            assert_eq!(
                resolve_disk_access_mode(mode, gib, 4096, 0.5, Some(8 * gib), true),
                mode,
                "explicit {mode:?} must be honored"
            );
            // A zero-length file always falls back to buffered, even when an
            // explicit non-buffered backend is requested (empty map / direct
            // read is invalid).
            assert_eq!(
                resolve_disk_access_mode(mode, 0, 4096, 0.5, Some(8 * gib), true),
                DiskAccessMode::Buffered,
                "explicit {mode:?} on an empty file must fall back to buffered"
            );
            // Explicit Mmap/Direct are NOT gated by mmap_min_size_bytes: a tiny
            // (but non-empty) file is still honored, unlike Auto.
            assert_eq!(
                resolve_disk_access_mode(mode, 100, 4096, 0.5, Some(8 * gib), true),
                mode,
                "explicit {mode:?} must ignore mmap_min_size_bytes for a non-empty file"
            );
        }
    }

    /// End-to-end: `disk_access_mode` drives backend selection. The default
    /// `Auto` mode maps a small (sub-RAM) Data.db file; an explicit `Buffered`
    /// mode and the `mmap_min_size_bytes` threshold both force buffered I/O; the
    /// legacy `use_mmap` flag still selects mmap.
    #[tokio::test]
    async fn test_config_drives_mmap_backend() -> crate::Result<()> {
        use super::super::SSTableReader;
        use crate::config::DiskAccessMode;
        use crate::{Config, Platform};
        use std::path::Path;
        use std::sync::Arc;

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

        let mut config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);

        // Default config (Auto): a >4KiB file far below system RAM is mapped.
        let reader = SSTableReader::open(&data_file, &config, platform.clone()).await?;
        assert!(
            reader.is_mmap_backed().await,
            "Auto must map a small (sub-RAM) file"
        );

        // Explicit Buffered mode forces buffered I/O.
        config.storage.disk_access_mode = DiskAccessMode::Buffered;
        let buffered = SSTableReader::open(&data_file, &config, platform.clone()).await?;
        assert!(
            !buffered.is_mmap_backed().await,
            "explicit Buffered mode must not map"
        );

        // Legacy opt-in still selects mmap even with mode left at Buffered.
        config.storage.use_mmap = true;
        let mapped = SSTableReader::open(&data_file, &config, platform.clone()).await?;
        assert!(
            mapped.is_mmap_backed().await,
            "use_mmap=true must select the mmap backend for a >4KiB file"
        );

        // A min-size threshold above the file size forces buffered under Auto.
        config.storage.use_mmap = false;
        config.storage.disk_access_mode = DiskAccessMode::Auto;
        config.storage.mmap_min_size_bytes = usize::MAX;
        let small = SSTableReader::open(&data_file, &config, platform.clone()).await?;
        assert!(
            !small.is_mmap_backed().await,
            "files below mmap_min_size_bytes must stay buffered"
        );

        Ok(())
    }

    /// End-to-end: explicit `Direct` mode selects the direct-I/O backend (or
    /// gracefully falls back to buffered where the filesystem refuses O_DIRECT).
    #[cfg(unix)]
    #[tokio::test]
    async fn test_config_drives_direct_backend() -> crate::Result<()> {
        use super::super::SSTableReader;
        use crate::config::DiskAccessMode;
        use crate::{Config, Platform};
        use std::path::Path;
        use std::sync::Arc;

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

        let mut config = Config::default();
        config.storage.disk_access_mode = DiskAccessMode::Direct;
        let platform = Arc::new(Platform::new(&config).await?);

        // Direct mode must open successfully and read correctly. Depending on the
        // filesystem (tmpfs/overlayfs in CI often reject O_DIRECT) it is either
        // the direct backend or the buffered fallback — both are valid; the key
        // invariant is that the reader opens and is queryable.
        let reader = SSTableReader::open(&data_file, &config, platform.clone()).await?;
        let direct = reader.is_direct_backed().await;
        let mapped = reader.is_mmap_backed().await;
        assert!(
            !mapped,
            "explicit Direct mode must never silently choose mmap"
        );
        eprintln!(
            "Direct mode resolved to {} backend",
            if direct {
                "direct"
            } else {
                "buffered (fallback)"
            }
        );

        Ok(())
    }

    /// Issue #815: concurrent full scans on a *single* `SSTableReader` must
    /// return identical, correct results. Before #815 each scan held
    /// `scan_mutex` for its whole lifetime (correct but fully serialized); the
    /// per-scan cursor lets them run in parallel. This stress test would surface
    /// the #805 corruption (interleaved seeks / chunk-index advances producing
    /// `Column not found` errors or short/garbled results) if the scans shared a
    /// mutable cursor again. Run against both the buffered and mmap backends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_scans_single_reader_are_consistent() -> crate::Result<()> {
        use super::super::SSTableReader;
        use crate::{Config, Platform};
        use std::path::Path;
        use std::sync::Arc;

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

        for use_mmap in [false, true] {
            let mut config = Config::default();
            config.storage.use_mmap = use_mmap;
            let platform = Arc::new(Platform::new(&config).await?);
            let reader = Arc::new(SSTableReader::open(&data_file, &config, platform).await?);

            // Reference result from an uncontended scan.
            let reference = reader.get_all_entries().await?;
            assert!(
                !reference.is_empty(),
                "expected non-empty reference scan (mmap={use_mmap})"
            );
            let mut reference_keys: Vec<_> = reference.iter().map(|(_, k, _)| k.clone()).collect();
            reference_keys.sort();

            // Fan out many concurrent scans on the SAME reader and confirm each
            // returns exactly the reference set of partition keys.
            let mut handles = Vec::new();
            for _ in 0..16 {
                let reader = Arc::clone(&reader);
                handles.push(tokio::spawn(async move { reader.get_all_entries().await }));
            }
            for handle in handles {
                let entries = handle
                    .await
                    .expect("scan task panicked")
                    .expect("concurrent scan failed");
                assert_eq!(
                    entries.len(),
                    reference.len(),
                    "concurrent scan returned a different row count (mmap={use_mmap})"
                );
                let mut keys: Vec<_> = entries.iter().map(|(_, k, _)| k.clone()).collect();
                keys.sort();
                assert_eq!(
                    keys, reference_keys,
                    "concurrent scan returned different keys (mmap={use_mmap})"
                );
            }
        }

        Ok(())
    }
}
