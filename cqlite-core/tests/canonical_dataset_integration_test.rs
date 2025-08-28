//! Integration test demonstrating migration from legacy paths to canonical dataset helpers
//! This test replaces usage of non-canonical paths like "tests/test-data/real-cassandra"
//! with the new canonical dataset helpers from Issue #78

use cqlite_core::testing::{list_tables, load_metadata, resolve_table_to_sstable_path};
use std::env;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_canonical_dataset_integration() {
    // This test demonstrates migrating from legacy non-canonical paths to canonical helpers

    // BEFORE (legacy approach using non-canonical paths):
    // let legacy_path = "tests/test-data/real-cassandra/test_basic/simple_table-abc123";

    // AFTER (canonical approach using Issue #78 helpers):

    // Create test environment with canonical dataset structure
    let temp_dir = TempDir::new().unwrap();
    let datasets_root = temp_dir.path().join("test-data").join("datasets");
    fs::create_dir_all(&datasets_root).unwrap();

    // Create test metadata.yml
    let metadata_content = r#"
keyspaces:
  - name: test_basic
    tables:
      - name: simple_table
        row_count: 1000
      - name: users  
        row_count: 2500
  - name: system_test
    tables:
      - name: keyspaces
        row_count: 5
"#;
    fs::write(datasets_root.join("metadata.yml"), metadata_content).unwrap();

    // Create canonical SSTable directory structure
    let sstables_dir = datasets_root.join("sstables");
    let test_basic_dir = sstables_dir.join("test_basic");
    let simple_table_dir = test_basic_dir.join("simple_table-abc123def456");
    let users_dir = test_basic_dir.join("users-def456abc123");

    fs::create_dir_all(&simple_table_dir).unwrap();
    fs::create_dir_all(&users_dir).unwrap();

    // Create sample SSTable files following Cassandra naming convention
    fs::write(simple_table_dir.join("nb-1-big-Data.db"), "test data").unwrap();
    fs::write(simple_table_dir.join("nb-1-big-Index.db"), "test index").unwrap();
    fs::write(users_dir.join("nb-1-big-Data.db"), "user data").unwrap();

    // Set environment variable to point to our test datasets
    unsafe {
        env::set_var("CQLITE_DATASETS_ROOT", &datasets_root);
    }

    // Test 1: Load metadata using canonical helper
    let metadata = load_metadata().expect("Failed to load metadata");
    assert_eq!(metadata.keyspaces.len(), 2);
    assert_eq!(metadata.keyspaces[0].name, "test_basic");
    assert_eq!(metadata.keyspaces[0].tables.len(), 2);

    // Test 2: List all tables using canonical helper (replaces manual directory traversal)
    let tables = list_tables(None).expect("Failed to list tables");
    assert_eq!(tables.len(), 3);

    let table_names: Vec<String> = tables
        .iter()
        .map(|t| format!("{}.{}", t.keyspace, t.table))
        .collect();
    assert!(table_names.contains(&"test_basic.simple_table".to_string()));
    assert!(table_names.contains(&"test_basic.users".to_string()));
    assert!(table_names.contains(&"system_test.keyspaces".to_string()));

    // Test 3: Resolve table to SSTable path (replaces hardcoded path construction)
    let simple_table_path = resolve_table_to_sstable_path("test_basic", "simple_table")
        .expect("Failed to resolve simple_table path");
    assert!(simple_table_path.exists());
    assert!(simple_table_path.ends_with("simple_table-abc123def456"));

    let users_path =
        resolve_table_to_sstable_path("test_basic", "users").expect("Failed to resolve users path");
    assert!(users_path.exists());
    assert!(users_path.ends_with("users-def456abc123"));

    // Test 4: Verify SSTable files are detected correctly
    assert!(simple_table_path.join("nb-1-big-Data.db").exists());
    assert!(users_path.join("nb-1-big-Data.db").exists());

    // Test 5: Filter tables by keyspace (replaces manual filtering)
    let basic_tables = list_tables(Some("test_basic")).expect("Failed to list test_basic tables");
    assert_eq!(basic_tables.len(), 2);

    let system_tables =
        list_tables(Some("system_test")).expect("Failed to list system_test tables");
    assert_eq!(system_tables.len(), 1);
    assert_eq!(system_tables[0].table, "keyspaces");

    // Test 6: Handle table not found (replaces manual error handling)
    let result = resolve_table_to_sstable_path("test_basic", "nonexistent");
    assert!(result.is_err());

    // Clean up environment variable
    unsafe {
        env::remove_var("CQLITE_DATASETS_ROOT");
    }
}

#[test]
fn test_migration_from_legacy_script_patterns() {
    // This test shows how to migrate specific patterns from repl_real_data_validation.sh

    // Create test environment
    let temp_dir = TempDir::new().unwrap();
    let datasets_root = temp_dir.path().join("datasets");

    // BEFORE (from repl_real_data_validation.sh line 14):
    // TEST_DATA_DIR="tests/test-data/real-cassandra"
    // CASSANDRA_DATA_DIRS=(
    //     "/var/lib/cassandra/data"
    //     "tests/fixtures/cassandra-data"
    //     "tests/integration/test-data"
    // )

    // AFTER: Use canonical dataset helpers with configurable root
    unsafe {
        env::set_var("CQLITE_DATASETS_ROOT", &datasets_root);
    }

    // Test the error handling when no metadata exists (replaces manual directory checks)
    let result = load_metadata();
    assert!(result.is_err());

    // Test table listing when no data exists (graceful fallback)
    let result = list_tables(None);
    assert!(result.is_err());

    unsafe {
        env::remove_var("CQLITE_DATASETS_ROOT");
    }
}

#[cfg(test)]
mod legacy_path_replacement_examples {
    use super::*;

    // This module demonstrates specific replacements for legacy paths

    #[test]
    fn replace_hardcoded_paths_with_canonical_helpers() {
        // LEGACY APPROACH (what we're replacing):
        // let legacy_test_data = "tests/test-data/real-cassandra/test_keyspace/users-12345";
        // let legacy_fixtures = "tests/fixtures/cassandra-data/basic/simple-123abc";
        // let legacy_integration = "tests/integration/test-data/system/keyspaces-456def";

        // NEW CANONICAL APPROACH:
        let temp_dir = TempDir::new().unwrap();
        let datasets_root = temp_dir.path();

        // Create minimal canonical structure for testing
        fs::create_dir_all(datasets_root.join("sstables/test_keyspace")).unwrap();

        let metadata_content = r#"
keyspaces:
  - name: test_keyspace
    tables:
      - name: users
        row_count: 100
"#;
        fs::write(datasets_root.join("metadata.yml"), metadata_content).unwrap();

        // Create table directory and SSTable file
        let table_dir = datasets_root.join("sstables/test_keyspace/users-12345");
        fs::create_dir_all(&table_dir).unwrap();
        fs::write(table_dir.join("nb-1-big-Data.db"), "test").unwrap();

        unsafe {
            env::set_var("CQLITE_DATASETS_ROOT", datasets_root);
        }

        // Now use canonical helpers instead of hardcoded paths:
        let canonical_path = resolve_table_to_sstable_path("test_keyspace", "users")
            .expect("Should find canonical path");

        // Verify we get the same functional result but through canonical means
        assert!(canonical_path.exists());
        assert!(canonical_path.join("nb-1-big-Data.db").exists());

        // This replaces all the manual path construction and directory traversal
        // from the legacy script with a single, reliable canonical helper call

        unsafe {
            env::remove_var("CQLITE_DATASETS_ROOT");
        }
    }
}
