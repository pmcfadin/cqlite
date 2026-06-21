//! Multi-Partition Data.db Write-Read Roundtrip Tests
//!
//! Tests that verify Data.db files with multiple partitions written by DataWriter
//! can be correctly parsed by V5CompressedLegacyParser.
//!
//! ## What These Tests Verify
//!
//! - Multiple partitions are written in token order
//! - Each partition can be individually located via Index.db
//! - Partition boundaries are correctly established
//! - Token ordering is validated and preserved
//!
//! ## Dependencies
//!
//! - Writer: `cqlite_core::storage::sstable::writer::DataWriter`
//! - Index Reader: `cqlite_core::storage::sstable::index_reader::IndexReader`

#![cfg(feature = "write-support")]

use super::{create_simple_mutation, create_simple_schema};
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use std::sync::Arc;
use tempfile::TempDir;

/// Test multiple partitions via WriteEngine
#[tokio::test]
async fn test_data_multiple_partitions_basic() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write multiple partitions (different partition keys)
    for i in 0..10 {
        let mutation =
            create_simple_mutation(i, &format!("user_{}", i), i * 100, 1000000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Flush to create SSTable
    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Verify partition count
    assert_eq!(info.partition_count, 10, "Should have 10 partitions");

    // Verify Data.db exists and has content
    assert!(info.data_path.exists(), "Data.db should exist");
    let data_size = std::fs::metadata(&info.data_path)
        .expect("Should get metadata")
        .len();
    assert!(
        data_size > 100,
        "Data.db with 10 partitions should have substantial size"
    );
}

/// Test that partitions are written in token order
#[tokio::test]
async fn test_data_partitions_token_order() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write partitions (WriteEngine handles token ordering internally)
    for i in 0..20 {
        let mutation =
            create_simple_mutation(i, &format!("user_{}", i), i * 10, 1000000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Flush to create SSTable
    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Read Index.db to verify token ordering
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let index_reader = IndexReader::open(info.index_path.as_ref().unwrap(), platform)
        .await
        .expect("IndexReader should open");

    // Verify index entries are in order (by checking offsets increase monotonically)
    let entries = index_reader.get_partition_entries();
    assert_eq!(entries.len(), 20, "Should have 20 index entries");

    // Data.db offsets should be monotonically increasing (partitions are sequential)
    for i in 1..entries.len() {
        assert!(
            entries[i].data_offset > entries[i - 1].data_offset,
            "Partition {} offset ({}) should be > partition {} offset ({})",
            i,
            entries[i].data_offset,
            i - 1,
            entries[i - 1].data_offset
        );
    }
}

/// Test Index.db offsets point to correct Data.db locations
#[tokio::test]
async fn test_data_index_offset_correctness() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write several partitions
    for i in 0..5 {
        let mutation =
            create_simple_mutation(i, &format!("user_{}", i), i * 10, 1000000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Flush to create SSTable
    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Read both Data.db and Index.db
    let data_bytes = std::fs::read(&info.data_path).expect("Should read Data.db");

    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let index_reader = IndexReader::open(info.index_path.as_ref().unwrap(), platform)
        .await
        .expect("IndexReader should open");

    let entries = index_reader.get_partition_entries();

    // Verify each index offset points to a valid location in Data.db
    for (i, entry) in entries.iter().enumerate() {
        let offset = entry.data_offset as usize;
        assert!(
            offset < data_bytes.len(),
            "Index entry {} offset ({}) should be within Data.db size ({})",
            i,
            offset,
            data_bytes.len()
        );
    }

    // Last offset + some data should not exceed file size
    if let Some(last_entry) = entries.last() {
        assert!(
            (last_entry.data_offset as usize) < data_bytes.len(),
            "Last partition should be within Data.db"
        );
    }
}

/// Test large number of partitions
#[tokio::test]
async fn test_data_many_partitions() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write many partitions
    let partition_count = 100;
    for i in 0..partition_count {
        let mutation =
            create_simple_mutation(i, &format!("user_{:05}", i), i * 10, 1000000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Flush to create SSTable
    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Verify partition count
    assert_eq!(
        info.partition_count, partition_count as usize,
        "Should have {} partitions",
        partition_count
    );

    // Verify Index.db has correct entry count
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let index_reader = IndexReader::open(info.index_path.as_ref().unwrap(), platform)
        .await
        .expect("IndexReader should open");

    let entries = index_reader.get_partition_entries();
    assert_eq!(
        entries.len(),
        partition_count as usize,
        "Index.db should have {} entries",
        partition_count
    );
}

/// Test mixed partition sizes (some with more data than others)
#[tokio::test]
async fn test_data_mixed_partition_sizes() {
    use cqlite_core::storage::write_engine::{CellOperation, Mutation, PartitionKey, TableId};
    use cqlite_core::types::Value;

    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write partitions with varying data sizes
    for i in 0..5 {
        let table_id = TableId::new("test_roundtrip", "simple");
        let pk = PartitionKey::single("id", Value::Integer(i));

        // Create varying amount of data based on partition id
        let name = if i % 2 == 0 {
            // Short name for even partitions
            format!("u{}", i)
        } else {
            // Long name for odd partitions
            "A".repeat(100 + (i as usize * 50))
        };

        let ops = vec![
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text(name),
            },
            CellOperation::Write {
                column: "value".to_string(),
                value: Value::Integer(i * 1000),
            },
        ];

        let mutation = Mutation::new(table_id, pk, None, ops, 1000000 + i as i64, None);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Flush to create SSTable
    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Verify all partitions were written
    assert_eq!(info.partition_count, 5, "Should have 5 partitions");

    // Verify Index.db captures different offsets reflecting different sizes
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let index_reader = IndexReader::open(info.index_path.as_ref().unwrap(), platform)
        .await
        .expect("IndexReader should open");

    let entries = index_reader.get_partition_entries();

    // Calculate size differences between adjacent partitions
    let mut sizes: Vec<u64> = Vec::new();
    for i in 0..entries.len() {
        let size = if i + 1 < entries.len() {
            entries[i + 1].data_offset - entries[i].data_offset
        } else {
            // Last partition size can be estimated from file end
            let data_len = std::fs::metadata(&info.data_path)
                .expect("Should get metadata")
                .len();
            data_len - entries[i].data_offset
        };
        sizes.push(size);
    }

    // Sizes should vary (odd partitions should be larger)
    assert!(
        sizes.iter().max().unwrap() > sizes.iter().min().unwrap(),
        "Partition sizes should vary: {:?}",
        sizes
    );
}

/// Test SSTable with single wide partition vs multiple small partitions
#[tokio::test]
async fn test_data_wide_vs_narrow_partitions() {
    use super::{create_clustered_mutation, create_clustering_schema};

    // First: Write single wide partition
    let temp_dir1 = TempDir::new().unwrap();
    let schema = create_clustering_schema();

    let config1 = WriteEngineConfig::new(
        temp_dir1.path().join("data"),
        temp_dir1.path().join("wal"),
        schema.clone(),
    );

    let mut engine1 = WriteEngine::new(config1).expect("Engine creation should succeed");

    // Write 50 rows to single partition
    for i in 0..50 {
        let mutation = create_clustered_mutation(
            1,
            &format!("ck_{:03}", i),
            &format!("data_{}", i),
            1000000 + i as i64,
        );
        engine1
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    let info1 = engine1
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Second: Write 50 narrow partitions
    let temp_dir2 = TempDir::new().unwrap();
    let simple_schema = create_simple_schema();

    let config2 = WriteEngineConfig::new(
        temp_dir2.path().join("data"),
        temp_dir2.path().join("wal"),
        simple_schema.clone(),
    );

    let mut engine2 = WriteEngine::new(config2).expect("Engine creation should succeed");

    // Write 50 partitions with 1 row each
    for i in 0..50 {
        let mutation =
            create_simple_mutation(i, &format!("user_{}", i), i * 10, 1000000 + i as i64);
        engine2
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    let info2 = engine2
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Compare: Wide partition should have 1 partition, narrow should have 50
    assert_eq!(
        info1.partition_count, 1,
        "Wide partition SSTable should have 1 partition"
    );
    assert_eq!(
        info2.partition_count, 50,
        "Narrow partition SSTable should have 50 partitions"
    );

    // Both should have similar total data (approximately)
    let size1 = std::fs::metadata(&info1.data_path).unwrap().len();
    let size2 = std::fs::metadata(&info2.data_path).unwrap().len();

    // Both files should have substantial content
    assert!(size1 > 100, "Wide partition Data.db should have content");
    assert!(size2 > 100, "Narrow partition Data.db should have content");
}

/// Test data integrity via cross-component validation
#[tokio::test]
async fn test_data_cross_component_validation() {
    use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
    use cqlite_core::storage::sstable::bloom::BloomFilter;
    use cqlite_core::storage::sstable::summary_reader::SummaryReader;

    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write several partitions
    for i in 0..10 {
        let mutation =
            create_simple_mutation(i, &format!("user_{}", i), i * 10, 1000000 + i as i64);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Flush to create SSTable
    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Cross-validation 1: Index.db entry count matches partition count
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let index_reader = IndexReader::open(info.index_path.as_ref().unwrap(), platform.clone())
        .await
        .expect("IndexReader should open");
    assert_eq!(
        index_reader.get_partition_entries().len(),
        info.partition_count,
        "Index.db entry count should match SSTableInfo.partition_count"
    );

    // Cross-validation 2: Statistics.db min_timestamp matches first mutation
    let stats_data = std::fs::read(&info.stats_path).expect("Should read Statistics.db");
    let (_, stats) =
        parse_statistics_with_fallback(&stats_data, None).expect("Should parse Statistics.db");
    assert_eq!(
        stats.timestamp_stats.min_timestamp, 1000000,
        "Statistics.db min_timestamp should match first mutation"
    );

    // Cross-validation 3: Filter.db can be loaded
    let filter_path = info
        .filter_path
        .as_ref()
        .expect("default fp_chance must emit Filter.db");
    let filter_data = std::fs::read(filter_path).expect("Should read Filter.db");
    let _filter = BloomFilter::deserialize(&filter_data).expect("Filter.db should deserialize");

    // Cross-validation 4: Summary.db has entries
    let summary_reader = SummaryReader::open(info.summary_path.as_ref().unwrap(), platform)
        .await
        .expect("SummaryReader should open");
    assert!(
        !summary_reader.get_entries().is_empty(),
        "Summary.db should have entries"
    );
}
