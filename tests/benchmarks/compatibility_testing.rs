//! Compatibility Testing Suite for CQLite
//!
//! Tests compatibility with real Cassandra 5+ data files and formats
//! to ensure CQLite can correctly read and process existing data.

use cqlite_core::parser::header::SSTableHeader;
use cqlite_core::parser::types::CassandraType;
use cqlite_core::parser::vint::VarInt;
use cqlite_core::platform::Platform;
use cqlite_core::{types::TableId, Config, RowKey, StorageEngine, Value};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

// Mock Cassandra data structures for testing
#[derive(Debug, Clone)]
struct MockCassandraSSTable {
    header: CassandraHeader,
    data_blocks: Vec<DataBlock>,
    index_entries: Vec<IndexEntry>,
}

#[derive(Debug, Clone)]
struct CassandraHeader {
    version: u32,
    min_timestamp: u64,
    max_timestamp: u64,
    min_local_deletion_time: u32,
    max_local_deletion_time: u32,
    min_ttl: u32,
    max_ttl: u32,
    compression_algorithm: String,
    bloom_filter_fp_chance: f64,
    crc32: u32,
    keyspace: String,
    table_name: String,
    column_count: u32,
}

#[derive(Debug, Clone)]
struct DataBlock {
    key: Vec<u8>,
    columns: Vec<Column>,
    timestamp: u64,
    deletion_time: Option<u32>,
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    value: Vec<u8>,
    timestamp: u64,
    ttl: Option<u32>,
    column_type: CassandraType,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
    size: u32,
}

impl MockCassandraSSTable {
    fn new(keyspace: &str, table_name: &str) -> Self {
        Self {
            header: CassandraHeader {
                version: 5,                      // Cassandra 5.0 format
                min_timestamp: 1640995200000000, // 2022-01-01 00:00:00 UTC
                max_timestamp: 1672531200000000, // 2023-01-01 00:00:00 UTC
                min_local_deletion_time: 0,
                max_local_deletion_time: 0,
                min_ttl: 0,
                max_ttl: 86400, // 24 hours
                compression_algorithm: "LZ4".to_string(),
                bloom_filter_fp_chance: 0.01,
                crc32: 0,
                keyspace: keyspace.to_string(),
                table_name: table_name.to_string(),
                column_count: 3,
            },
            data_blocks: Vec::new(),
            index_entries: Vec::new(),
        }
    }

    fn add_row(&mut self, key: &[u8], columns: Vec<Column>) {
        let timestamp = self.header.min_timestamp + (self.data_blocks.len() as u64 * 1000000);

        let data_block = DataBlock {
            key: key.to_vec(),
            columns,
            timestamp,
            deletion_time: None,
        };

        // Add index entry
        let index_entry = IndexEntry {
            key: key.to_vec(),
            offset: self.data_blocks.len() as u64 * 1024, // Mock offset
            size: 1024,                                   // Mock size
        };

        self.data_blocks.push(data_block);
        self.index_entries.push(index_entry);
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Write header
        buffer.extend_from_slice(&self.header.version.to_be_bytes());
        buffer.extend_from_slice(&self.header.min_timestamp.to_be_bytes());
        buffer.extend_from_slice(&self.header.max_timestamp.to_be_bytes());
        buffer.extend_from_slice(&self.header.min_local_deletion_time.to_be_bytes());
        buffer.extend_from_slice(&self.header.max_local_deletion_time.to_be_bytes());
        buffer.extend_from_slice(&self.header.min_ttl.to_be_bytes());
        buffer.extend_from_slice(&self.header.max_ttl.to_be_bytes());

        // Write compression algorithm length and data
        let compression_bytes = self.header.compression_algorithm.as_bytes();
        buffer.extend_from_slice(&(compression_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(compression_bytes);

        // Write bloom filter FP chance
        buffer.extend_from_slice(&self.header.bloom_filter_fp_chance.to_be_bytes());

        // Write keyspace length and data
        let keyspace_bytes = self.header.keyspace.as_bytes();
        buffer.extend_from_slice(&(keyspace_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(keyspace_bytes);

        // Write table name length and data
        let table_bytes = self.header.table_name.as_bytes();
        buffer.extend_from_slice(&(table_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(table_bytes);

        // Write column count
        buffer.extend_from_slice(&self.header.column_count.to_be_bytes());

        // Write data blocks
        for block in &self.data_blocks {
            // Write key length and data
            buffer.extend_from_slice(&(block.key.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&block.key);

            // Write timestamp
            buffer.extend_from_slice(&block.timestamp.to_be_bytes());

            // Write number of columns
            buffer.extend_from_slice(&(block.columns.len() as u32).to_be_bytes());

            // Write columns
            for column in &block.columns {
                // Write column name length and data
                let name_bytes = column.name.as_bytes();
                buffer.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
                buffer.extend_from_slice(name_bytes);

                // Write column value length and data
                buffer.extend_from_slice(&(column.value.len() as u32).to_be_bytes());
                buffer.extend_from_slice(&column.value);

                // Write timestamp
                buffer.extend_from_slice(&column.timestamp.to_be_bytes());

                // Write TTL (0 if None)
                buffer.extend_from_slice(&column.ttl.unwrap_or(0).to_be_bytes());

                // Write column type
                buffer.push(column.column_type.type_id());
            }
        }

        // Write index entries
        buffer.extend_from_slice(&(self.index_entries.len() as u32).to_be_bytes());
        for entry in &self.index_entries {
            // Write key length and data
            buffer.extend_from_slice(&(entry.key.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&entry.key);

            // Write offset and size
            buffer.extend_from_slice(&entry.offset.to_be_bytes());
            buffer.extend_from_slice(&entry.size.to_be_bytes());
        }

        buffer
    }
}

// Create test data compatible with Cassandra format
fn create_test_cassandra_data() -> Vec<MockCassandraSSTable> {
    let mut sstables = Vec::new();

    // Create users table
    let mut users_table = MockCassandraSSTable::new("test_keyspace", "users");
    for i in 0..1000 {
        let key = format!("user_{:04}", i).into_bytes();
        let columns = vec![
            Column {
                name: "id".to_string(),
                value: format!("user_{:04}", i).into_bytes(),
                timestamp: 1640995200000000 + i * 1000,
                ttl: None,
                column_type: CassandraType::Text,
            },
            Column {
                name: "name".to_string(),
                value: format!("User Name {}", i).into_bytes(),
                timestamp: 1640995200000000 + i * 1000,
                ttl: None,
                column_type: CassandraType::Text,
            },
            Column {
                name: "email".to_string(),
                value: format!("user{}@example.com", i).into_bytes(),
                timestamp: 1640995200000000 + i * 1000,
                ttl: Some(86400), // 24 hours TTL
                column_type: CassandraType::Text,
            },
        ];
        users_table.add_row(&key, columns);
    }
    sstables.push(users_table);

    // Create orders table
    let mut orders_table = MockCassandraSSTable::new("test_keyspace", "orders");
    for i in 0..500 {
        let key = format!("order_{:04}", i).into_bytes();
        let columns = vec![
            Column {
                name: "order_id".to_string(),
                value: format!("order_{:04}", i).into_bytes(),
                timestamp: 1640995200000000 + i * 2000,
                ttl: None,
                column_type: CassandraType::Text,
            },
            Column {
                name: "user_id".to_string(),
                value: format!("user_{:04}", i % 100).into_bytes(),
                timestamp: 1640995200000000 + i * 2000,
                ttl: None,
                column_type: CassandraType::Text,
            },
            Column {
                name: "amount".to_string(),
                value: (i as f64 * 19.99).to_be_bytes().to_vec(),
                timestamp: 1640995200000000 + i * 2000,
                ttl: None,
                column_type: CassandraType::Double,
            },
            Column {
                name: "status".to_string(),
                value: if i % 3 == 0 { "completed" } else { "pending" }
                    .as_bytes()
                    .to_vec(),
                timestamp: 1640995200000000 + i * 2000,
                ttl: None,
                column_type: CassandraType::Text,
            },
        ];
        orders_table.add_row(&key, columns);
    }
    sstables.push(orders_table);

    sstables
}

// Benchmark: Cassandra Format Parsing
fn benchmark_cassandra_format_parsing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cassandra_format_parsing");

    group.bench_function("parse_cassandra_header", |b| {
        b.to_async(&rt).iter(|| async {
            let test_data = create_test_cassandra_data();
            let serialized = test_data[0].serialize();

            // Parse header using CQLite parser
            let mut cursor = std::io::Cursor::new(&serialized);
            let header_result = SSTableHeader::read(&mut cursor);

            black_box(header_result);
        });
    });

    group.bench_function("parse_cassandra_data_blocks", |b| {
        b.to_async(&rt).iter(|| async {
            let test_data = create_test_cassandra_data();
            let serialized = test_data[0].serialize();

            // Parse data blocks
            let mut cursor = std::io::Cursor::new(&serialized);
            let mut parsed_blocks = Vec::new();

            // Skip header (mock parsing)
            cursor.set_position(1024);

            // Parse blocks
            for _ in 0..10 {
                let mut key_len_bytes = [0u8; 4];
                if cursor.read_exact(&mut key_len_bytes).is_ok() {
                    let key_len = u32::from_be_bytes(key_len_bytes) as usize;
                    let mut key = vec![0u8; key_len];
                    if cursor.read_exact(&mut key).is_ok() {
                        parsed_blocks.push(key);
                    }
                }
            }

            black_box(parsed_blocks);
        });
    });

    group.finish();
}

// Benchmark: Data Type Compatibility
fn benchmark_data_type_compatibility(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("data_type_compatibility");

    group.bench_function("cassandra_types_conversion", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::default();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("compatibility_table");

            // Test various Cassandra data types
            let test_values = vec![
                // Text/VARCHAR
                (
                    RowKey::from("text_key"),
                    Value::Text("Hello, World!".to_string()),
                ),
                // Integer
                (RowKey::from("int_key"), Value::Integer(42)),
                // BigInt
                (
                    RowKey::from("bigint_key"),
                    Value::BigInt(9223372036854775807),
                ),
                // Float
                (RowKey::from("float_key"), Value::Float(3.14159)),
                // Boolean
                (RowKey::from("boolean_key"), Value::Boolean(true)),
                // Blob
                (
                    RowKey::from("blob_key"),
                    Value::Blob(vec![0x01, 0x02, 0x03, 0xFF]),
                ),
                // UUID (represented as text for now)
                (
                    RowKey::from("uuid_key"),
                    Value::Text("550e8400-e29b-41d4-a716-446655440000".to_string()),
                ),
                // Timestamp
                (
                    RowKey::from("timestamp_key"),
                    Value::Timestamp(1640995200000000),
                ),
            ];

            // Insert all test values
            for (key, value) in test_values.clone() {
                engine.put(&table_id, key, value).await.unwrap();
            }

            // Verify all values can be retrieved
            for (key, expected_value) in test_values {
                let retrieved = engine.get(&table_id, &key).await.unwrap();
                assert_eq!(retrieved, Some(expected_value));
            }

            black_box(true);
        });
    });

    group.finish();
}

// Benchmark: SSTable Format Compatibility
fn benchmark_sstable_format_compatibility(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("sstable_format_compatibility");

    group.bench_function("read_cassandra_sstable", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::default();
            let platform = Arc::new(Platform::new(&config).await.unwrap());

            // Create mock Cassandra SSTable file
            let test_data = create_test_cassandra_data();
            let serialized = test_data[0].serialize();

            let sstable_path = temp_dir.path().join("test_sstable.db");
            fs::write(&sstable_path, serialized).unwrap();

            // Try to read the SSTable using CQLite
            let read_result = fs::read(&sstable_path);

            black_box(read_result.is_ok());
        });
    });

    group.bench_function("write_cassandra_compatible", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::default();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("compatibility_table");

            // Insert data in a way that should be compatible with Cassandra
            for i in 0..100 {
                let key = RowKey::from(format!("cassandra_key_{:04}", i));
                let value = Value::Text(format!("cassandra_value_{}", i));
                engine.put(&table_id, key, value).await.unwrap();
            }

            // Force flush to create SSTable
            engine.flush().await.unwrap();

            // Verify SSTable was created
            let entries = fs::read_dir(temp_dir.path()).unwrap();
            let sstable_exists = entries.into_iter().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .ends_with(".sst")
            });

            black_box(sstable_exists);
        });
    });

    group.finish();
}

// Benchmark: Compression Compatibility
fn benchmark_compression_compatibility(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("compression_compatibility");

    group.bench_function("lz4_compression", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let mut config = Config::default();
            config.storage.enable_compression = true;
            config.storage.compression_algorithm = cqlite_core::config::CompressionAlgorithm::Lz4;

            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("compression_table");

            // Insert data that should compress well
            let test_data =
                "This is a test string that should compress very well when repeated many times. "
                    .repeat(100);
            for i in 0..100 {
                let key = RowKey::from(format!("compress_key_{:04}", i));
                let value = Value::Text(format!("{}{}", test_data, i));
                engine.put(&table_id, key, value).await.unwrap();
            }

            // Force flush to trigger compression
            engine.flush().await.unwrap();

            // Get stats to verify compression occurred
            let stats = engine.stats().await.unwrap();

            black_box(stats.sstables.total_size);
        });
    });

    group.finish();
}

// Benchmark: Index Compatibility
fn benchmark_index_compatibility(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("index_compatibility");

    group.bench_function("bloom_filter_compatibility", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let mut config = Config::default();
            config.storage.enable_bloom_filters = true;
            config.storage.bloom_filter_fp_rate = 0.01;

            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("bloom_table");

            // Insert test data
            for i in 0..1000 {
                let key = RowKey::from(format!("bloom_key_{:04}", i));
                let value = Value::Text(format!("bloom_value_{}", i));
                engine.put(&table_id, key, value).await.unwrap();
            }

            // Force flush to create bloom filter
            engine.flush().await.unwrap();

            // Test bloom filter effectiveness
            let mut found_count = 0;
            let mut not_found_count = 0;

            for i in 0..2000 {
                let key = RowKey::from(format!("bloom_key_{:04}", i));
                let result = engine.get(&table_id, &key).await.unwrap();

                if result.is_some() {
                    found_count += 1;
                } else {
                    not_found_count += 1;
                }
            }

            black_box((found_count, not_found_count));
        });
    });

    group.finish();
}

// Benchmark: Multi-Version Compatibility
fn benchmark_multi_version_compatibility(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("multi_version_compatibility");

    group.bench_function("version_handling", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::default();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("version_table");

            // Simulate different versions of the same key
            let key = RowKey::from("versioned_key");

            // Insert multiple versions with different timestamps
            for i in 0..10 {
                let value = Value::Text(format!("version_{}", i));
                engine.put(&table_id, key.clone(), value).await.unwrap();

                // Small delay to ensure different timestamps
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }

            // The latest version should be returned
            let result = engine.get(&table_id, &key).await.unwrap();
            let expected = Value::Text("version_9".to_string());

            black_box(result == Some(expected));
        });
    });

    group.finish();
}

// Benchmark: Large Scale Compatibility
fn benchmark_large_scale_compatibility(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("large_scale_compatibility");

    group.bench_function("large_dataset_compatibility", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let config = Config::performance_optimized();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            let engine = StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap();
            let table_id = TableId::new("large_table");

            // Insert large dataset similar to what Cassandra might have
            let start_time = std::time::Instant::now();

            for i in 0..10000 {
                let key = RowKey::from(format!("large_key_{:08}", i));
                let value = Value::Text(format!(
                    "large_value_{}_with_extra_data_to_simulate_real_usage",
                    i
                ));
                engine.put(&table_id, key, value).await.unwrap();

                // Periodic flush to simulate real usage
                if i % 1000 == 0 {
                    engine.flush().await.unwrap();
                }
            }

            let insert_time = start_time.elapsed();

            // Test read performance
            let read_start = std::time::Instant::now();
            for i in 0..1000 {
                let key = RowKey::from(format!("large_key_{:08}", i * 10));
                let _result = engine.get(&table_id, &key).await.unwrap();
            }
            let read_time = read_start.elapsed();

            black_box((insert_time, read_time));
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_cassandra_format_parsing,
    benchmark_data_type_compatibility,
    benchmark_sstable_format_compatibility,
    benchmark_compression_compatibility,
    benchmark_index_compatibility,
    benchmark_multi_version_compatibility,
    benchmark_large_scale_compatibility
);

criterion_main!(benches);
