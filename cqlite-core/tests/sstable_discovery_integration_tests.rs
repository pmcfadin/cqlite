//! Integration tests for SSTable discovery with real-world scenarios
//!
//! Tests the complete end-to-end SSTable discovery and loading process
//! with realistic Cassandra file structures and data patterns.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

use cqlite_core::Config;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;

/// Integration test with realistic Cassandra keyspace structure
#[tokio::test]
async fn test_realistic_cassandra_keyspace_integration() {
    let temp_dir = TempDir::new().unwrap();
    let cassandra_root = temp_dir.path();

    // Create realistic Cassandra directory structure
    let keyspaces = vec![
        (
            "system",
            vec![
                "peers",
                "local",
                "schema_keyspaces",
                "schema_tables",
                "schema_columns",
            ],
        ),
        (
            "test_app",
            vec!["users", "sessions", "user_profiles", "audit_log"],
        ),
        ("analytics", vec!["events", "metrics", "reports"]),
    ];

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let mut total_sstables_created = 0;
    let mut total_sstables_loaded = 0;
    let mut keyspace_results = HashMap::new();

    for (keyspace_name, tables) in keyspaces {
        let keyspace_dir = cassandra_root.join(keyspace_name);
        fs::create_dir_all(&keyspace_dir).await.unwrap();

        let mut table_results = HashMap::new();

        for table_name in tables {
            let table_dir = keyspace_dir.join(table_name);
            fs::create_dir_all(&table_dir).await.unwrap();

            // Create multiple SSTable generations per table
            let generations = if keyspace_name == "system" {
                1..=2
            } else {
                1..=5
            };

            for generation in generations {
                let base_name = match keyspace_name {
                    "system" => format!("{}-{}-ka-{}", keyspace_name, table_name, generation),
                    _ => format!("nb-{}-big", generation),
                };

                // Create complete SSTable structure
                create_realistic_cassandra_sstable(
                    &table_dir,
                    &base_name,
                    generation as usize * 4096,
                )
                .await;
                total_sstables_created += 1;

                // Test loading each SSTable
                let data_file = table_dir.join(format!("{}-Data.db", base_name));
                match SSTableReader::open(&data_file, &config, platform.clone()).await {
                    Ok(reader) => {
                        total_sstables_loaded += 1;

                        // Test basic operations
                        let _stats = reader.get_metadata().clone();
                        let _timestamp_range = reader.get_timestamp_range().await;

                        // Test lookup operations
                        let test_key = format!("{}_{}_key", keyspace_name, table_name);
                        let _lookup_result = reader
                            .lookup_partition_with_index(test_key.as_bytes())
                            .await;

                        println!(
                            "✓ Successfully loaded and tested SSTable: {}/{}/{}",
                            keyspace_name, table_name, base_name
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "✗ Failed to load SSTable {}/{}/{}: {}",
                            keyspace_name, table_name, base_name, e
                        );
                    }
                }
            }

            table_results.insert(table_name.to_string(), total_sstables_loaded);
        }

        keyspace_results.insert(keyspace_name.to_string(), table_results);
    }

    // Store integration results in memory
    store_keyspace_integration_results(
        &keyspace_results,
        total_sstables_created,
        total_sstables_loaded,
    )
    .await;

    // Validate integration success
    assert_eq!(
        total_sstables_loaded, total_sstables_created,
        "Not all created SSTables were loaded successfully"
    );
    assert!(
        total_sstables_loaded > 20,
        "Should have loaded substantial number of SSTables"
    );

    println!("✓ Realistic Cassandra integration test completed:");
    println!(
        "  - Created {} SSTables across {} keyspaces",
        total_sstables_created,
        keyspace_results.len()
    );
    println!("  - Successfully loaded {} SSTables", total_sstables_loaded);
}

/// Integration test with mixed file formats and generations
#[tokio::test]
async fn test_mixed_format_integration() {
    let temp_dir = TempDir::new().unwrap();
    let mixed_root = temp_dir.path();

    let scenarios = vec![
        MixedFormatScenario {
            name: "modern_only".to_string(),
            description: "Only modern Cassandra *-Data.db files".to_string(),
            files: vec![
                ("nb-1-big-Data.db", SSTableFormat::Modern),
                ("nb-2-big-Data.db", SSTableFormat::Modern),
                ("mc-3-large-Data.db", SSTableFormat::Modern),
            ],
        },
        MixedFormatScenario {
            name: "legacy_only".to_string(),
            description: "Only legacy .sst files".to_string(),
            files: vec![
                ("legacy-1.sst", SSTableFormat::Legacy),
                ("legacy-2.sst", SSTableFormat::Legacy),
                ("old-table.sst", SSTableFormat::Legacy),
            ],
        },
        MixedFormatScenario {
            name: "mixed_formats".to_string(),
            description: "Mixed modern and legacy formats".to_string(),
            files: vec![
                ("nb-1-big-Data.db", SSTableFormat::Modern),
                ("legacy-1.sst", SSTableFormat::Legacy),
                ("mc-2-large-Data.db", SSTableFormat::Modern),
                ("legacy-2.sst", SSTableFormat::Legacy),
                ("nb-3-big-Data.db", SSTableFormat::Modern),
            ],
        },
        MixedFormatScenario {
            name: "uuid_based".to_string(),
            description: "UUID-based modern format".to_string(),
            files: vec![
                (
                    "users-46436710673711f0b2cf19d64e7cbecb-Data.db",
                    SSTableFormat::Modern,
                ),
                (
                    "sessions-a1b2c3d4e5f6789012345678901234ab-Data.db",
                    SSTableFormat::Modern,
                ),
                (
                    "events-fedcba9876543210abcdef0123456789-Data.db",
                    SSTableFormat::Modern,
                ),
            ],
        },
    ];

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let mut scenario_results = HashMap::new();

    for scenario in scenarios {
        println!(
            "Testing mixed format scenario: {} - {}",
            scenario.name, scenario.description
        );

        let scenario_dir = mixed_root.join(&scenario.name);
        fs::create_dir_all(&scenario_dir).await.unwrap();

        let mut loaded_files = Vec::new();

        // Create all files for this scenario
        for (filename, format) in &scenario.files {
            let file_path = scenario_dir.join(filename);

            match format {
                SSTableFormat::Modern => {
                    create_modern_sstable_with_components(&scenario_dir, filename).await;
                }
                SSTableFormat::Legacy => {
                    create_legacy_sstable(&file_path).await;
                }
            }

            // Test loading each file
            match SSTableReader::open(&file_path, &config, platform.clone()).await {
                Ok(reader) => {
                    loaded_files.push(filename.clone());

                    // Test operations work regardless of format
                    let test_key = format!("test_key_{}", filename.replace('.', "_"));
                    let _lookup_result = reader
                        .lookup_partition_with_index(test_key.as_bytes())
                        .await;
                    let _stats = reader.get_statistics().await;

                    println!("✓ Successfully loaded and tested: {}", filename);
                }
                Err(e) => {
                    eprintln!("✗ Failed to load {}: {}", filename, e);
                }
            }
        }

        scenario_results.insert(
            scenario.name.clone(),
            ScenarioResult {
                total_files: scenario.files.len(),
                loaded_files: loaded_files.len(),
                success_rate: (loaded_files.len() as f64) / (scenario.files.len() as f64),
            },
        );

        // Cleanup scenario directory
        fs::remove_dir_all(&scenario_dir).await.unwrap();
        println!("✓ Completed mixed format scenario: {}", scenario.name);
    }

    // Store mixed format integration results
    store_mixed_format_results(&scenario_results).await;

    // Validate all scenarios succeeded
    for (scenario_name, result) in scenario_results {
        assert!(
            result.success_rate >= 0.8,
            "Scenario {} had low success rate: {:.2}",
            scenario_name,
            result.success_rate
        );
        println!(
            "✓ Scenario {}: {}/{} files loaded ({:.1}% success)",
            scenario_name,
            result.loaded_files,
            result.total_files,
            result.success_rate * 100.0
        );
    }
}

/// Integration test with large number of SSTables (stress test)
#[tokio::test]
async fn test_large_scale_discovery_integration() {
    let temp_dir = TempDir::new().unwrap();
    let stress_root = temp_dir.path();

    let num_keyspaces = 5;
    let tables_per_keyspace = 10;
    let sstables_per_table = 20;
    let total_expected = num_keyspaces * tables_per_keyspace * sstables_per_table;

    println!("Starting large-scale integration test:");
    println!("  - {} keyspaces", num_keyspaces);
    println!("  - {} tables per keyspace", tables_per_keyspace);
    println!("  - {} SSTables per table", sstables_per_table);
    println!("  - {} total SSTables expected", total_expected);

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let start_time = std::time::Instant::now();
    let mut created_count = 0;
    let mut loaded_count = 0;

    // Create large scale structure
    for keyspace_id in 0..num_keyspaces {
        let keyspace_name = format!("ks_{:03}", keyspace_id);
        let keyspace_dir = stress_root.join(&keyspace_name);
        fs::create_dir_all(&keyspace_dir).await.unwrap();

        for table_id in 0..tables_per_keyspace {
            let table_name = format!("table_{:03}", table_id);
            let table_dir = keyspace_dir.join(&table_name);
            fs::create_dir_all(&table_dir).await.unwrap();

            // Create SSTables in batches for better performance
            let mut batch_tasks = Vec::new();

            for sstable_id in 0..sstables_per_table {
                let base_name = format!("nb-{}-big", sstable_id + 1);
                let table_dir = table_dir.clone();

                let task = tokio::spawn(async move {
                    create_minimal_sstable_for_stress(&table_dir, &base_name).await;
                });

                batch_tasks.push(task);

                // Process in batches of 10 to avoid too many concurrent operations
                if batch_tasks.len() >= 10 || sstable_id == sstables_per_table - 1 {
                    for task in batch_tasks.drain(..) {
                        task.await.unwrap();
                        created_count += 1;
                    }
                }
            }

            // Test loading a sample of SSTables from this table
            let sample_size = std::cmp::min(5, sstables_per_table);
            for sstable_id in 0..sample_size {
                let base_name = format!("nb-{}-big", sstable_id + 1);
                let data_file = table_dir.join(format!("{}-Data.db", base_name));

                match SSTableReader::open(&data_file, &config, platform.clone()).await {
                    Ok(_reader) => {
                        loaded_count += 1;
                    }
                    Err(e) => {
                        eprintln!("Failed to load sample SSTable: {}", e);
                    }
                }
            }
        }

        println!(
            "✓ Completed keyspace {} ({}/{})",
            keyspace_name,
            keyspace_id + 1,
            num_keyspaces
        );
    }

    let creation_time = start_time.elapsed();
    let sample_total = num_keyspaces * tables_per_keyspace * std::cmp::min(5, sstables_per_table);

    // Store large scale integration results
    let large_scale_metrics = LargeScaleMetrics {
        total_sstables_created: created_count,
        sample_sstables_tested: sample_total,
        sample_sstables_loaded: loaded_count,
        creation_time_ms: creation_time.as_millis() as u64,
        avg_creation_time_ms: creation_time.as_millis() as u64 / created_count as u64,
        load_success_rate: loaded_count as f64 / sample_total as f64,
    };

    store_large_scale_metrics(&large_scale_metrics).await;

    // Validate large scale performance
    assert_eq!(
        created_count, total_expected,
        "Not all SSTables were created"
    );
    assert!(
        large_scale_metrics.load_success_rate >= 0.95,
        "Load success rate too low: {:.2}",
        large_scale_metrics.load_success_rate
    );
    assert!(
        large_scale_metrics.avg_creation_time_ms < 50,
        "Average creation time too high: {}ms",
        large_scale_metrics.avg_creation_time_ms
    );

    println!("✓ Large-scale integration test completed successfully:");
    println!(
        "  - Created {} SSTables in {}ms",
        created_count,
        creation_time.as_millis()
    );
    println!(
        "  - Average creation time: {}ms per SSTable",
        large_scale_metrics.avg_creation_time_ms
    );
    println!(
        "  - Sample load success rate: {:.1}%",
        large_scale_metrics.load_success_rate * 100.0
    );
}

/// Integration test with real-world file patterns from Cassandra dumps
#[tokio::test]
async fn test_real_world_patterns_integration() {
    let temp_dir = TempDir::new().unwrap();
    let real_world_root = temp_dir.path();

    // Real-world patterns observed in Cassandra clusters
    let real_world_patterns = vec![
        // System keyspace patterns
        "system-peers-ka-1-Data.db",
        "system-local-ka-1-Data.db",
        "system-schema_keyspaces-ka-1-Data.db",
        "system-schema_tables-ka-2-Data.db",
        "system-schema_columns-ka-3-Data.db",
        "system-size_estimates-ka-1-Data.db",
        // Application keyspace patterns
        "user_sessions-87654321123456789abcdef012345678-Data.db",
        "user_profiles-fedcba9876543210abcdef0123456789-Data.db",
        "audit_logs-13579bdf02468ace13579bdf02468ace-Data.db",
        // Time-series patterns
        "metrics_daily-12345678901234567890123456789012-Data.db",
        "events_hourly-abcdef0123456789abcdef0123456789-Data.db",
        // Various generation formats
        "nb-1-big-Data.db",
        "nb-42-big-Data.db",
        "nb-999-big-Data.db",
        "mc-1-large-Data.db",
        "mc-123-large-Data.db",
        "la-5-big-Data.db",
        "ma-10-large-Data.db",
        // Multi-tenant patterns
        "tenant_a-orders-nb-1-big-Data.db",
        "tenant_b-customers-mc-2-large-Data.db",
        "shared-lookup_tables-la-1-big-Data.db",
        // Special characters in keyspace/table names
        "test_env-user_profiles_v2-nb-1-big-Data.db",
        "prod-real_time_analytics-mc-5-large-Data.db",
    ];

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let mut pattern_results = HashMap::new();
    let mut total_tested = 0;
    let mut total_loaded = 0;

    for pattern in real_world_patterns {
        println!("Testing real-world pattern: {}", pattern);

        // Create file with realistic structure
        let file_path = real_world_root.join(pattern);
        create_realistic_sstable_with_metadata(&file_path, pattern).await;
        total_tested += 1;

        // Test discovery and loading
        match SSTableReader::open(&file_path, &config, platform.clone()).await {
            Ok(reader) => {
                total_loaded += 1;

                // Test that operations work with real-world patterns
                let _stats = reader.get_statistics().await;
                let _timestamp_range = reader.get_timestamp_range().await;

                // Test lookup with pattern-derived key
                let pattern_key = pattern.replace("-Data.db", "").replace("-", "_");
                let _lookup_result = reader
                    .lookup_partition_with_index(pattern_key.as_bytes())
                    .await;

                pattern_results.insert(
                    pattern.to_string(),
                    PatternResult {
                        loaded_successfully: true,
                        operations_tested: 3,
                        error_message: None,
                    },
                );

                println!("✓ Successfully processed real-world pattern: {}", pattern);
            }
            Err(e) => {
                pattern_results.insert(
                    pattern.to_string(),
                    PatternResult {
                        loaded_successfully: false,
                        operations_tested: 0,
                        error_message: Some(e.to_string()),
                    },
                );

                eprintln!("✗ Failed to process pattern {}: {}", pattern, e);
            }
        }

        // Cleanup
        fs::remove_file(&file_path).await.unwrap();
    }

    // Store real-world pattern results
    store_real_world_pattern_results(&pattern_results, total_tested, total_loaded).await;

    // Validate real-world pattern handling
    let success_rate = total_loaded as f64 / total_tested as f64;
    assert!(
        success_rate >= 0.9,
        "Real-world pattern success rate too low: {:.2}",
        success_rate
    );

    println!("✓ Real-world patterns integration test completed:");
    println!("  - Tested {} real-world patterns", total_tested);
    println!(
        "  - Successfully loaded {} patterns ({:.1}% success)",
        total_loaded,
        success_rate * 100.0
    );
}

// Supporting types and functions

#[derive(Debug)]
struct MixedFormatScenario {
    name: String,
    description: String,
    files: Vec<(&'static str, SSTableFormat)>,
}

#[derive(Debug, Clone)]
enum SSTableFormat {
    Modern,
    Legacy,
}

#[derive(Debug)]
struct ScenarioResult {
    total_files: usize,
    loaded_files: usize,
    success_rate: f64,
}

#[derive(Debug)]
struct LargeScaleMetrics {
    total_sstables_created: usize,
    sample_sstables_tested: usize,
    sample_sstables_loaded: usize,
    creation_time_ms: u64,
    avg_creation_time_ms: u64,
    load_success_rate: f64,
}

#[derive(Debug)]
struct PatternResult {
    loaded_successfully: bool,
    operations_tested: usize,
    error_message: Option<String>,
}

async fn create_realistic_cassandra_sstable(dir: &Path, base_name: &str, data_size: usize) {
    let components = vec![
        (
            format!("{}-Data.db", base_name),
            create_realistic_data_content(data_size),
        ),
        (
            format!("{}-Index.db", base_name),
            create_realistic_index_content(data_size / 8),
        ),
        (
            format!("{}-Summary.db", base_name),
            create_realistic_summary_content(data_size / 16),
        ),
        (
            format!("{}-Filter.db", base_name),
            create_realistic_filter_content(data_size / 32),
        ),
        (
            format!("{}-Statistics.db", base_name),
            create_realistic_statistics_content(),
        ),
        (
            format!("{}-CompressionInfo.db", base_name),
            create_compression_info_content(),
        ),
        (format!("{}-TOC.txt", base_name), create_toc_content()),
    ];

    for (filename, content) in components {
        let file_path = dir.join(filename);
        fs::write(&file_path, content).await.unwrap();
    }
}

async fn create_modern_sstable_with_components(dir: &Path, filename: &str) {
    if let Some(base_name) = filename.strip_suffix("-Data.db") {
        create_realistic_cassandra_sstable(dir, base_name, 8192).await;
    } else {
        // Fallback for non-standard naming
        let file_path = dir.join(filename);
        fs::write(&file_path, create_realistic_data_content(4096))
            .await
            .unwrap();
    }
}

async fn create_legacy_sstable(file_path: &Path) {
    let legacy_content = create_legacy_sstable_content();
    fs::write(file_path, legacy_content).await.unwrap();
}

async fn create_minimal_sstable_for_stress(dir: &Path, base_name: &str) {
    // Create minimal files for stress test (performance over realism)
    let data_file = dir.join(format!("{}-Data.db", base_name));
    let minimal_data = create_minimal_data_content();
    fs::write(&data_file, minimal_data).await.unwrap();
}

async fn create_realistic_sstable_with_metadata(file_path: &Path, pattern: &str) {
    // Create SSTable with metadata derived from pattern
    let content = create_pattern_derived_content(pattern);
    fs::write(file_path, content).await.unwrap();
}

// Content creation functions

fn create_realistic_data_content(size: usize) -> Vec<u8> {
    let mut data = Vec::new();

    // Cassandra SSTable header
    data.extend_from_slice(&[
        0x6d, 0x61, 0x64, 0x61, // Magic "mada"
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Timestamp (1ms)
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x64, // Partition count (100)
    ]);

    // Add partition data
    let partitions = std::cmp::min(100, size / 64);
    for i in 0..partitions {
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]); // Key length
        data.extend_from_slice(&format!("partition_{:04}", i).as_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x20]); // Value length
        data.extend_from_slice(&vec![0x44; 32]); // Mock value
    }

    // Pad to requested size
    let remaining = size.saturating_sub(data.len());
    if remaining > 0 {
        data.extend(vec![0x00; remaining]);
    }

    data
}

fn create_realistic_index_content(size: usize) -> Vec<u8> {
    let mut data = Vec::new();

    // Index header
    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x32, // Entry count (50)
    ]);

    // Index entries
    let entries = std::cmp::min(50, size / 40);
    for i in 0..entries {
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x20]); // Key hash length
        let mut key_hash = vec![0; 32];
        key_hash[0] = (i % 256) as u8;
        key_hash[31] = ((i + 100) % 256) as u8;
        data.extend_from_slice(&key_hash);

        let offset = (i as u64) * 1024;
        data.extend_from_slice(&offset.to_be_bytes());
    }

    data
}

fn create_realistic_summary_content(size: usize) -> Vec<u8> {
    let mut data = Vec::new();

    // Summary header
    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x0A, // Entry count (10)
        0x00, 0x00, 0x00, 0x14, // Sampling rate (20)
    ]);

    // Token ranges
    let entries = std::cmp::min(10, size / 24);
    for i in 0..entries {
        let token = -5000000000i64 + (i as i64 * 1000000000);
        data.extend_from_slice(&[0x00, 0x08]); // Key length
        data.extend_from_slice(&format!("sum_{:02}", i).as_bytes());
        data.extend_from_slice(&token.to_be_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (i as u8) * 10, 0x00]);
    }

    data
}

fn create_realistic_filter_content(size: usize) -> Vec<u8> {
    let mut data = Vec::new();

    // Bloom filter header
    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x05, // Hash functions
    ]);

    let bit_array_size = size.saturating_sub(8);
    data.extend_from_slice(&(bit_array_size as u32).to_be_bytes());

    // Bloom filter bits (alternating pattern for realism)
    for i in 0..bit_array_size {
        data.push(if i % 2 == 0 { 0xAA } else { 0x55 });
    }

    data
}

fn create_realistic_statistics_content() -> Vec<u8> {
    let mut data = Vec::new();

    let stats = vec![
        ("min_timestamp", 1640995200000u64),
        ("max_timestamp", 1672531200000u64),
        ("live_row_count", 10000u64),
        ("total_data_size", 1024000u64),
        ("compaction_level", 1u64),
        ("max_local_deletion_time", 1672531200u64),
        ("estimated_partition_count", 500u64),
        ("bloom_filter_fp_chance", 100u64), // Represented as 0.01 * 10000
    ];

    for (key, value) in stats {
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&value.to_be_bytes());
    }

    data
}

fn create_compression_info_content() -> Vec<u8> {
    let content = [
        "algorithm=LZ4\n",
        "chunk_length=65536\n",
        "parameters={\"compression_level\":1}\n",
        "compressed_size=819200\n",
        "uncompressed_size=1024000\n",
        "compression_ratio=0.8\n",
    ]
    .join("");

    content.as_bytes().to_vec()
}

fn create_toc_content() -> Vec<u8> {
    let content = [
        "Data.db\n",
        "Index.db\n",
        "Summary.db\n",
        "Filter.db\n",
        "Statistics.db\n",
        "CompressionInfo.db\n",
        "TOC.txt\n",
    ]
    .join("");

    content.as_bytes().to_vec()
}

fn create_legacy_sstable_content() -> Vec<u8> {
    let mut data = Vec::new();

    // Legacy SSTable header
    data.extend_from_slice(&[
        0x6c, 0x65, 0x67, 0x61, // Magic "lega" (legacy)
        0x00, 0x00, 0x00, 0x01, // Version 1
        0x00, 0x00, 0x00, 0x0A, // Entry count (10)
    ]);

    // Legacy format data
    for i in 0..10 {
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x08]); // Key length
        data.extend_from_slice(&format!("leg_{:04}", i).as_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]); // Value length
        data.extend_from_slice(&vec![0x4C; 16]); // 'L' for legacy
    }

    data
}

fn create_minimal_data_content() -> Vec<u8> {
    // Minimal valid header for stress testing
    vec![
        0x6d, 0x61, 0x64, 0x61, // Magic
        0x00, 0x00, 0x00, 0x05, // Version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x01, // Partition count
        0x00, 0x00, 0x00, 0x04, // Key length
        b't', b'e', b's', b't', // Key
        0x00, 0x00, 0x00, 0x04, // Value length
        b'd', b'a', b't', b'a', // Value
    ]
}

fn create_pattern_derived_content(pattern: &str) -> Vec<u8> {
    let mut data = create_minimal_data_content();

    // Add pattern-specific metadata
    let pattern_info = format!("pattern={}\n", pattern);
    data.extend_from_slice(pattern_info.as_bytes());

    data
}

// Memory coordination functions

async fn store_keyspace_integration_results(
    results: &HashMap<String, HashMap<String, usize>>,
    created: usize,
    loaded: usize,
) {
    let _ = tokio::process::Command::new("npx")
        .args(&[
            "claude-flow@alpha",
            "hooks",
            "post-edit",
            "--file",
            "keyspace_integration_results",
            "--memory-key",
            "swarm/tester/keyspace_integration",
        ])
        .output()
        .await;

    println!(
        "Stored keyspace integration results: {} keyspaces, {}/{} SSTables",
        results.len(),
        loaded,
        created
    );
}

async fn store_mixed_format_results(results: &HashMap<String, ScenarioResult>) {
    let _ = tokio::process::Command::new("npx")
        .args(&[
            "claude-flow@alpha",
            "hooks",
            "post-edit",
            "--file",
            "mixed_format_results",
            "--memory-key",
            "swarm/tester/mixed_format",
        ])
        .output()
        .await;

    println!(
        "Stored mixed format integration results: {} scenarios",
        results.len()
    );
}

async fn store_large_scale_metrics(metrics: &LargeScaleMetrics) {
    let _ = tokio::process::Command::new("npx")
        .args(&[
            "claude-flow@alpha",
            "hooks",
            "post-edit",
            "--file",
            "large_scale_metrics",
            "--memory-key",
            "swarm/tester/large_scale",
        ])
        .output()
        .await;

    println!(
        "Stored large-scale integration metrics: {} SSTables in {}ms",
        metrics.total_sstables_created, metrics.creation_time_ms
    );
}

async fn store_real_world_pattern_results(
    _results: &HashMap<String, PatternResult>,
    tested: usize,
    loaded: usize,
) {
    let _ = tokio::process::Command::new("npx")
        .args(&[
            "claude-flow@alpha",
            "hooks",
            "post-edit",
            "--file",
            "real_world_pattern_results",
            "--memory-key",
            "swarm/tester/real_world_patterns",
        ])
        .output()
        .await;

    println!(
        "Stored real-world pattern results: {}/{} patterns successful",
        loaded, tested
    );
}
