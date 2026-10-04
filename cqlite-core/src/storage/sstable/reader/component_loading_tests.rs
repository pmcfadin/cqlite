use super::*;
use std::path::PathBuf;

// =========================================================================
// extract_keyspace_and_table tests (via SSTableReader)
// =========================================================================

#[test]
fn test_extract_keyspace_and_table_standard_path() {
    // Standard Cassandra directory structure
    let path = PathBuf::from(
        "/var/lib/cassandra/data/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    );
    let result = SSTableReader::extract_keyspace_and_table(&path);
    assert!(result.is_ok(), "Should extract from standard path");

    let (keyspace, table) = result.unwrap();
    assert_eq!(keyspace, "test_basic");
    assert_eq!(table, "simple_table");
}

#[test]
fn test_extract_keyspace_and_table_different_keyspace() {
    // UUID must be exactly 32 hex characters for proper extraction
    let path =
        PathBuf::from("/data/system/local-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db");
    let result = SSTableReader::extract_keyspace_and_table(&path);
    assert!(result.is_ok(), "Should extract from system keyspace path");

    let (keyspace, table) = result.unwrap();
    assert_eq!(keyspace, "system");
    assert_eq!(table, "local");
}

#[test]
fn test_extract_keyspace_and_table_complex_table_name() {
    // UUID must be exactly 32 hex characters for proper extraction
    let path = PathBuf::from(
        "/data/my_keyspace/complex_table_name-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    );
    let result = SSTableReader::extract_keyspace_and_table(&path);
    assert!(result.is_ok(), "Should handle complex table names");

    let (keyspace, table) = result.unwrap();
    assert_eq!(keyspace, "my_keyspace");
    assert_eq!(table, "complex_table_name");
}

#[test]
fn test_extract_keyspace_and_table_too_shallow_path() {
    // Path too shallow - missing keyspace directory level
    let path = PathBuf::from("nb-1-big-Data.db");
    let result = SSTableReader::extract_keyspace_and_table(&path);
    assert!(result.is_err(), "Should fail for too shallow path");
}

// =========================================================================
// Component path construction tests
// =========================================================================

#[test]
fn test_component_path_construction() {
    let data_path = PathBuf::from("/test/keyspace/table-uuid/nb-1-big-Data.db");
    let base_name = extract_sstable_base_name(&data_path).unwrap();
    let parent = data_path.parent().unwrap();

    // Verify component paths are constructed correctly
    let index_path = parent.join(format!("{}-Index.db", base_name));
    assert_eq!(
        index_path.file_name().unwrap().to_str().unwrap(),
        "nb-1-big-Index.db"
    );

    let filter_path = parent.join(format!("{}-Filter.db", base_name));
    assert_eq!(
        filter_path.file_name().unwrap().to_str().unwrap(),
        "nb-1-big-Filter.db"
    );

    let summary_path = parent.join(format!("{}-Summary.db", base_name));
    assert_eq!(
        summary_path.file_name().unwrap().to_str().unwrap(),
        "nb-1-big-Summary.db"
    );

    let statistics_path = parent.join(format!("{}-Statistics.db", base_name));
    assert_eq!(
        statistics_path.file_name().unwrap().to_str().unwrap(),
        "nb-1-big-Statistics.db"
    );
}

#[test]
fn test_component_path_with_different_generation() {
    let data_path = PathBuf::from("/test/keyspace/table-uuid/nb-45-big-Data.db");
    let base_name = extract_sstable_base_name(&data_path).unwrap();
    let parent = data_path.parent().unwrap();

    let compression_info_path = parent.join(format!("{}-CompressionInfo.db", base_name));
    assert_eq!(
        compression_info_path.file_name().unwrap().to_str().unwrap(),
        "nb-45-big-CompressionInfo.db"
    );
}

// =========================================================================
// Async integration tests (use #[tokio::test])
// =========================================================================

#[tokio::test]
async fn test_detect_component_files_with_real_data() {
    // This test requires CQLITE_DATASETS_ROOT to be set
    let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(root) => PathBuf::from(root),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
            return;
        }
    };

    let simple_table_dir = datasets_root.join("sstables/test_basic");
    if !simple_table_dir.exists() {
        eprintln!("test_basic directory not found, skipping test");
        return;
    }

    // Find simple_table directory
    let table_dir = std::fs::read_dir(&simple_table_dir)
        .expect("Should read directory")
        .filter_map(|e| e.ok())
        .find(|e| {
            e.path().is_dir()
                && e.file_name()
                    .to_str()
                    .map(|n| n.starts_with("simple_table"))
                    .unwrap_or(false)
        });

    let Some(table_entry) = table_dir else {
        eprintln!("simple_table not found, skipping test");
        return;
    };

    // Find Data.db file
    let data_file = std::fs::read_dir(table_entry.path())
        .expect("Should read table dir")
        .filter_map(|e| e.ok())
        .find(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.ends_with("-Data.db"))
                .unwrap_or(false)
        });

    let Some(data_entry) = data_file else {
        eprintln!("Data.db not found, skipping test");
        return;
    };

    let data_path = data_entry.path();

    // Test detect_component_files
    let components = SSTableReader::detect_component_files(&data_path)
        .await
        .expect("Should detect component files");

    eprintln!("Detected {} component files:", components.len());
    for (component_type, path) in &components {
        eprintln!("  {}: {}", component_type, path.display());
    }

    // simple_table should have standard components
    assert!(
        components.contains_key("Index") || components.contains_key("Statistics"),
        "Should detect at least Index or Statistics component"
    );
}

#[tokio::test]
async fn test_validate_component_integrity_with_real_data() {
    // This test requires CQLITE_DATASETS_ROOT to be set
    let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(root) => PathBuf::from(root),
        Err(_) => {
            eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
            return;
        }
    };

    let simple_table_dir = datasets_root.join("sstables/test_basic");
    if !simple_table_dir.exists() {
        eprintln!("test_basic directory not found, skipping test");
        return;
    }

    // Find simple_table directory
    let table_dir = std::fs::read_dir(&simple_table_dir)
        .expect("Should read directory")
        .filter_map(|e| e.ok())
        .find(|e| {
            e.path().is_dir()
                && e.file_name()
                    .to_str()
                    .map(|n| n.starts_with("simple_table"))
                    .unwrap_or(false)
        });

    let Some(table_entry) = table_dir else {
        eprintln!("simple_table not found, skipping test");
        return;
    };

    // Find Data.db file
    let data_file = std::fs::read_dir(table_entry.path())
        .expect("Should read table dir")
        .filter_map(|e| e.ok())
        .find(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.ends_with("-Data.db"))
                .unwrap_or(false)
        });

    let Some(data_entry) = data_file else {
        eprintln!("Data.db not found, skipping test");
        return;
    };

    let data_path = data_entry.path();

    // First detect components
    let components = SSTableReader::detect_component_files(&data_path)
        .await
        .expect("Should detect component files");

    // Then validate integrity
    let issues = SSTableReader::validate_component_integrity(&data_path, &components)
        .await
        .expect("Should validate integrity");

    eprintln!("Validation issues: {:?}", issues);

    // Real test data should be valid
    assert!(
        issues.is_empty(),
        "Real test data should have no integrity issues: {:?}",
        issues
    );
}

#[tokio::test]
async fn test_detect_component_files_nonexistent_path() {
    let nonexistent_path = PathBuf::from("/nonexistent/path/nb-1-big-Data.db");

    // This should not panic - should return empty or handle gracefully
    let result = SSTableReader::detect_component_files(&nonexistent_path).await;

    // Result depends on implementation - either Ok with empty map or error
    match result {
        Ok(components) => {
            assert!(
                components.is_empty(),
                "Should return empty components for nonexistent path"
            );
        }
        Err(_) => {
            // Also acceptable - error for invalid path
        }
    }
}
