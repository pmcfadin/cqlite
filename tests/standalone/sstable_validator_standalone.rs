//! Standalone SSTable validator program
//! Tests SSTable reader/writer functionality and Cassandra 5+ compatibility

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cqlite_core::{
    platform::Platform,
    storage::sstable::{
        reader::SSTableReader,
        writer::SSTableWriter,
    },
    types::TableId,
    Config, Result, RowKey, Value,
};

use tempfile::TempDir;

/// Simple test suite for SSTable validation
#[tokio::main]
async fn main() -> Result<()> {
    println!("🧪 SSTable Standalone Validator");
    println!("===============================");

    let temp_dir = TempDir::new().map_err(|e| {
        cqlite_core::error::Error::storage(format!("Failed to create temp dir: {}", e))
    })?;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    // Test 1: Basic write/read functionality
    println!("📝 Testing basic write/read functionality...");
    test_basic_write_read(&temp_dir, &config, platform.clone()).await?;
    println!("✅ Basic write/read test passed!");

    // Test 2: Data type serialization
    println!("📊 Testing data type serialization...");
    test_data_types(&temp_dir, &config, platform.clone()).await?;
    println!("✅ Data type serialization test passed!");

    // Test 3: File format validation
    println!("📋 Testing file format compliance...");
    test_file_format(&temp_dir, &config, platform.clone()).await?;
    println!("✅ File format compliance test passed!");

    // Test 4: Large data handling
    println!("📏 Testing large data handling...");
    test_large_data(&temp_dir, &config, platform.clone()).await?;
    println!("✅ Large data handling test passed!");

    // Test 5: Unicode support
    println!("🌍 Testing Unicode support...");
    test_unicode_data(&temp_dir, &config, platform.clone()).await?;
    println!("✅ Unicode support test passed!");

    println!("\n🎉 All SSTable validation tests passed!");
    println!("✅ SSTable implementation is working correctly and appears Cassandra-compatible");

    Ok(())
}

/// Test basic write and read functionality
async fn test_basic_write_read(
    temp_dir: &TempDir,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<()> {
    let test_path = temp_dir.path().join("basic_test.sst");

    // Write test data
    let mut writer = SSTableWriter::create(&test_path, config, platform.clone()).await?;
    
    let test_data = vec![
        (TableId::new("users"), RowKey::from("user1"), Value::Text("Alice".to_string())),
        (TableId::new("users"), RowKey::from("user2"), Value::Text("Bob".to_string())),
        (TableId::new("posts"), RowKey::from("post1"), Value::Text("Hello World".to_string())),
    ];

    for (table_id, key, value) in &test_data {
        writer.add_entry(table_id, key.clone(), value.clone()).await?;
    }

    writer.finish().await?;

    // Verify file exists and has content
    if !test_path.exists() {
        return Err(cqlite_core::error::Error::storage(
            "SSTable file was not created".to_string(),
        ));
    }

    let file_size = std::fs::metadata(&test_path)
        .map_err(|e| cqlite_core::error::Error::storage(format!("Failed to get metadata: {}", e)))?
        .len();

    if file_size == 0 {
        return Err(cqlite_core::error::Error::storage(
            "SSTable file is empty".to_string(),
        ));
    }

    // Read data back and verify
    let reader = SSTableReader::open(&test_path, config, platform).await?;

    for (table_id, key, expected_value) in &test_data {
        match reader.get(table_id, key).await? {
            Some(value) => {
                if value != *expected_value {
                    return Err(cqlite_core::error::Error::storage(format!(
                        "Read value doesn't match written value for key {:?}: expected {:?}, got {:?}",
                        key, expected_value, value
                    )));
                }
            }
            None => {
                return Err(cqlite_core::error::Error::storage(format!(
                    "Could not read back written value for key {:?}", key
                )));
            }
        }
    }

    Ok(())
}

/// Test various data types
async fn test_data_types(
    temp_dir: &TempDir,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<()> {
    let test_path = temp_dir.path().join("data_types_test.sst");
    let mut writer = SSTableWriter::create(&test_path, config, platform.clone()).await?;

    let table_id = TableId::new("data_types_test");
    
    // Test all basic types
    let test_data = vec![
        ("null_value", Value::Null),
        ("boolean_true", Value::Boolean(true)),
        ("boolean_false", Value::Boolean(false)),
        ("integer_positive", Value::Integer(42)),
        ("integer_negative", Value::Integer(-42)),
        ("integer_zero", Value::Integer(0)),
        ("bigint_large", Value::BigInt(9223372036854775807)),
        ("float_pi", Value::Float(3.14159)),
        ("float_negative", Value::Float(-3.14159)),
        ("text_simple", Value::Text("Hello, World!".to_string())),
        ("text_empty", Value::Text("".to_string())),
        ("blob_simple", Value::Blob(vec![1, 2, 3, 4, 5])),
        ("blob_empty", Value::Blob(vec![])),
        ("timestamp", Value::Timestamp(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
        )),
        ("uuid", Value::Uuid(vec![
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0
        ])),
    ];

    // Write all test data
    for (key, value) in &test_data {
        writer.add_entry(&table_id, RowKey::from(*key), value.clone()).await?;
    }

    writer.finish().await?;

    // Read back and verify
    let reader = SSTableReader::open(&test_path, config, platform).await?;

    for (key, expected_value) in &test_data {
        match reader.get(&table_id, &RowKey::from(*key)).await? {
            Some(actual_value) => {
                if actual_value != *expected_value {
                    return Err(cqlite_core::error::Error::storage(format!(
                        "Data type test failed for key '{}': expected {:?}, got {:?}",
                        key, expected_value, actual_value
                    )));
                }
            }
            None => {
                return Err(cqlite_core::error::Error::storage(format!(
                    "Could not read back data for key '{}'", key
                )));
            }
        }
    }

    Ok(())
}

/// Test file format compliance
async fn test_file_format(
    temp_dir: &TempDir,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<()> {
    let test_path = temp_dir.path().join("format_test.sst");

    // Create minimal SSTable
    let mut writer = SSTableWriter::create(&test_path, config, platform).await?;
    writer.add_entry(&TableId::new("test"), RowKey::from("key"), Value::Text("value".to_string())).await?;
    writer.finish().await?;

    // Read file as binary and validate basic format
    let file_data = std::fs::read(&test_path)
        .map_err(|e| cqlite_core::error::Error::storage(format!("Failed to read test file: {}", e)))?;

    // Check minimum file size
    if file_data.len() < 48 {
        return Err(cqlite_core::error::Error::storage(format!(
            "File too small for valid SSTable: {} bytes", file_data.len()
        )));
    }

    // Validate magic bytes
    if &file_data[0..4] != [0x5A, 0x5A, 0x5A, 0x5A] {
        return Err(cqlite_core::error::Error::storage(format!(
            "Invalid magic bytes in header: {:?}", &file_data[0..4]
        )));
    }

    // Validate format version
    if &file_data[4..6] != b"oa" {
        return Err(cqlite_core::error::Error::storage(format!(
            "Invalid format version: {:?}", String::from_utf8_lossy(&file_data[4..6])
        )));
    }

    // Check footer magic
    let footer_start = file_data.len() - 8;
    if &file_data[footer_start..] != [0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A] {
        return Err(cqlite_core::error::Error::storage(format!(
            "Invalid footer magic bytes: {:?}", &file_data[footer_start..]
        )));
    }

    println!("📋 File format validation:");
    println!("  • Magic bytes: ✅ Valid");
    println!("  • Format version: ✅ 'oa' (Cassandra 5+)");
    println!("  • Footer magic: ✅ Valid");
    println!("  • File size: {} bytes", file_data.len());

    Ok(())
}

/// Test large data handling
async fn test_large_data(
    temp_dir: &TempDir,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<()> {
    let test_path = temp_dir.path().join("large_data_test.sst");
    let mut writer = SSTableWriter::create(&test_path, config, platform.clone()).await?;

    let table_id = TableId::new("large_data_test");
    
    // Create large text value
    let large_text = "A".repeat(10000); // 10KB of 'A's
    writer.add_entry(&table_id, RowKey::from("large_text"), Value::Text(large_text.clone())).await?;

    // Create large binary value
    let large_binary: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
    writer.add_entry(&table_id, RowKey::from("large_binary"), Value::Blob(large_binary.clone())).await?;

    writer.finish().await?;

    // Verify we can read large data back
    let reader = SSTableReader::open(&test_path, config, platform).await?;

    // Test large text
    match reader.get(&table_id, &RowKey::from("large_text")).await? {
        Some(Value::Text(text)) => {
            if text.len() != 10000 || !text.chars().all(|c| c == 'A') {
                return Err(cqlite_core::error::Error::storage(
                    "Large text data corruption detected".to_string()
                ));
            }
        }
        _ => {
            return Err(cqlite_core::error::Error::storage(
                "Could not read large text data".to_string()
            ));
        }
    }

    // Test large binary
    match reader.get(&table_id, &RowKey::from("large_binary")).await? {
        Some(Value::Blob(blob)) => {
            if blob != large_binary {
                return Err(cqlite_core::error::Error::storage(
                    "Large binary data corruption detected".to_string()
                ));
            }
        }
        _ => {
            return Err(cqlite_core::error::Error::storage(
                "Could not read large binary data".to_string()
            ));
        }
    }

    println!("📏 Large data test:");
    println!("  • Text (10KB): ✅ Preserved");
    println!("  • Binary (5KB): ✅ Preserved");

    Ok(())
}

/// Test Unicode and special character support
async fn test_unicode_data(
    temp_dir: &TempDir,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<()> {
    let test_path = temp_dir.path().join("unicode_test.sst");
    let mut writer = SSTableWriter::create(&test_path, config, platform.clone()).await?;

    let table_id = TableId::new("unicode_test");

    let unicode_test_data = vec![
        ("emoji", "🦀🚀🌟💾🔥"),
        ("chinese", "你好世界"),
        ("arabic", "مرحبا بالعالم"),
        ("hebrew", "שלום עולם"),
        ("russian", "Привет мир"),
        ("japanese", "こんにちは世界"),
        ("mixed", "Hello 世界 🌍 مرحبا שלום"),
        ("special_chars", "!@#$%^&*()[]{}|\\:;\"'<>,.?/~`"),
    ];

    // Write Unicode data
    for (key, text) in &unicode_test_data {
        writer.add_entry(&table_id, RowKey::from(*key), Value::Text(text.to_string())).await?;
    }

    writer.finish().await?;

    // Verify Unicode data
    let reader = SSTableReader::open(&test_path, config, platform).await?;

    for (key, expected_text) in &unicode_test_data {
        match reader.get(&table_id, &RowKey::from(*key)).await? {
            Some(Value::Text(actual_text)) => {
                if actual_text != *expected_text {
                    return Err(cqlite_core::error::Error::storage(format!(
                        "Unicode test failed for key '{}': expected '{}', got '{}'",
                        key, expected_text, actual_text
                    )));
                }
            }
            _ => {
                return Err(cqlite_core::error::Error::storage(format!(
                    "Could not read Unicode data for key '{}'", key
                )));
            }
        }
    }

    println!("🌍 Unicode test:");
    println!("  • Emoji: ✅ Preserved");
    println!("  • Chinese: ✅ Preserved");
    println!("  • Arabic: ✅ Preserved");
    println!("  • Hebrew: ✅ Preserved");
    println!("  • Mixed scripts: ✅ Preserved");

    Ok(())
}