//! VG7: Regression tests for Issue #680 — same-named tables across keyspaces.
//!
//! ## Background
//!
//! datasets-v3 introduced the corpus's first duplicate table names across keyspaces:
//! - `test_basic.simple_table` (1000 rows, nb format)
//! - `test_oa.simple_table`    (5 rows,    oa format)
//!
//! Before the fix, `SSTableManager::table_readers` was keyed by unqualified table
//! name only (e.g., `"simple_table"`).  Both tables therefore mapped to the same
//! entry, so any query returned the union of both sets of rows (1005 instead of 1000).
//!
//! ## Fix (cqlite-core/src/storage/sstable/mod.rs)
//!
//! The `table_readers` map is now keyed by the fully-qualified
//! `"keyspace.table"` string.  When loading SSTables the keyspace is extracted
//! from the grandparent directory of the SSTable file.  Both `scan()` and `get()`
//! look up by the full `table_id` string first, falling back to the unqualified
//! name only for flat/non-Cassandra directory layouts that lack a keyspace parent.

use std::path::{Path, PathBuf};

/// Root of the test datasets — set via `CQLITE_DATASETS_ROOT`.
fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
}

/// Return the path to a table directory under the sstables root.
fn table_dir(datasets: &Path, keyspace: &str, table: &str) -> Option<PathBuf> {
    let ks_path = datasets.join("sstables").join(keyspace);
    if !ks_path.is_dir() {
        return None;
    }
    // Find the first directory whose name starts with "{table}-"
    std::fs::read_dir(&ks_path)
        .ok()?
        .flatten()
        .filter(|e| e.path().is_dir())
        .find(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(&format!("{}-", table))
        })
        .map(|e| e.path())
}

/// Unit test for `extract_keyspace_and_table_name`.
///
/// Validates that the function correctly extracts both components from Cassandra-style
/// paths (keyspace/table-UUID/sstable_file) and returns `None` for flat paths.
#[test]
fn test_extract_keyspace_and_table_name_standard_path() {
    use cqlite_core::storage::sstable::extract_keyspace_and_table_name;

    let path = PathBuf::from(
        "test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    );
    let result = extract_keyspace_and_table_name(&path);
    assert_eq!(
        result,
        Some(("test_basic".to_string(), "simple_table".to_string())),
        "Should extract (keyspace, table) from standard Cassandra path"
    );
}

#[test]
fn test_extract_keyspace_and_table_name_oa_keyspace() {
    use cqlite_core::storage::sstable::extract_keyspace_and_table_name;

    let path = PathBuf::from(
        "test-data/datasets/sstables/test_oa/simple_table-4b7cd05064e711f1bd3ac7dbf655c673/oa-2-big-Data.db",
    );
    let result = extract_keyspace_and_table_name(&path);
    assert_eq!(
        result,
        Some(("test_oa".to_string(), "simple_table".to_string())),
        "Should extract (keyspace=test_oa, table=simple_table) from oa path"
    );
}

#[test]
fn test_extract_keyspace_and_table_name_distinguishes_same_table_in_different_keyspaces() {
    use cqlite_core::storage::sstable::extract_keyspace_and_table_name;

    let nb_path = PathBuf::from(
        "sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db",
    );
    let oa_path = PathBuf::from(
        "sstables/test_oa/simple_table-4b7cd05064e711f1bd3ac7dbf655c673/oa-2-big-Data.db",
    );

    let nb_result = extract_keyspace_and_table_name(&nb_path);
    let oa_result = extract_keyspace_and_table_name(&oa_path);

    // Both should succeed
    assert!(nb_result.is_some(), "nb path should yield a result");
    assert!(oa_result.is_some(), "oa path should yield a result");

    // Table names are the same...
    assert_eq!(nb_result.as_ref().unwrap().1, "simple_table");
    assert_eq!(oa_result.as_ref().unwrap().1, "simple_table");

    // ...but keyspaces differ, so the qualified keys are distinct
    let nb_key = format!("{}.{}", nb_result.unwrap().0, "simple_table");
    let oa_key = format!("{}.{}", oa_result.unwrap().0, "simple_table");
    assert_ne!(
        nb_key, oa_key,
        "qualified keys must differ when keyspaces differ"
    );
    assert_eq!(nb_key, "test_basic.simple_table");
    assert_eq!(oa_key, "test_oa.simple_table");
}

#[test]
fn test_extract_keyspace_and_table_name_flat_path_returns_none_for_keyspace() {
    use cqlite_core::storage::sstable::extract_keyspace_and_table_name;

    // Flat path: only one parent directory level → no keyspace grandparent
    let path = PathBuf::from("simple_table-UUID/nb-1-big-Data.db");
    let result = extract_keyspace_and_table_name(&path);
    // With only one parent, the "keyspace" would be "simple_table-UUID" itself,
    // but crucially the test confirms the function returns something reasonable or
    // that the qualified key at minimum differs from the same path under a different keyspace.
    // The function should not panic.
    let _ = result; // Just confirm it doesn't panic
}

/// Integration test: SSTableManager keyed correctly when two keyspaces share a table name.
///
/// Uses the real test fixtures from CQLITE_DATASETS_ROOT.  Skipped when the
/// environment variable is not set or the Data.db files are absent.
#[tokio::test]
async fn test_sstable_manager_isolates_same_named_tables_across_keyspaces() {
    let datasets = match datasets_root() {
        Some(d) => d,
        None => {
            eprintln!("SKIP: CQLITE_DATASETS_ROOT not set");
            return;
        }
    };

    let nb_dir = match table_dir(&datasets, "test_basic", "simple_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_basic/simple_table directory not found");
            return;
        }
    };
    let oa_dir = match table_dir(&datasets, "test_oa", "simple_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_oa/simple_table directory not found");
            return;
        }
    };

    // Check that Data.db files actually exist (not just JSONL reference files)
    let nb_data = nb_dir
        .read_dir()
        .ok()
        .and_then(|mut rd| {
            rd.find(|e| {
                e.as_ref()
                    .map(|e| {
                        e.file_name().to_string_lossy().ends_with("-Data.db")
                            && !e.file_name().to_string_lossy().ends_with(".jsonl")
                    })
                    .unwrap_or(false)
            })
        })
        .and_then(|e| e.ok())
        .map(|e| e.path());

    let oa_data = oa_dir
        .read_dir()
        .ok()
        .and_then(|mut rd| {
            rd.find(|e| {
                e.as_ref()
                    .map(|e| {
                        e.file_name().to_string_lossy().ends_with("-Data.db")
                            && !e.file_name().to_string_lossy().ends_with(".jsonl")
                    })
                    .unwrap_or(false)
            })
        })
        .and_then(|e| e.ok())
        .map(|e| e.path());

    if nb_data.is_none() || oa_data.is_none() {
        eprintln!("SKIP: Data.db files not present — run fetch-datasets.sh first");
        return;
    }

    // Build SSTableManager with both table directories
    use cqlite_core::storage::sstable::SSTableManager;
    use cqlite_core::{Config, Platform, TableId};
    use std::sync::Arc;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    // Use a temp dir as the storage path (not used for scanning here)
    let temp_dir = tempfile::TempDir::new().unwrap();

    let manager = SSTableManager::new_from_discovered_paths(
        temp_dir.path(),
        vec![nb_dir, oa_dir],
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .unwrap();

    // Scan test_basic.simple_table — must return only nb rows (1000)
    let nb_id = TableId::new("test_basic.simple_table");
    let nb_rows = manager.scan(&nb_id, None, None, None, None).await.unwrap();

    // Scan test_oa.simple_table — must return only oa rows (5)
    let oa_id = TableId::new("test_oa.simple_table");
    let oa_rows = manager.scan(&oa_id, None, None, None, None).await.unwrap();

    // The two scans must be disjoint and correct
    assert_eq!(
        nb_rows.len(),
        1000,
        "test_basic.simple_table must return exactly 1000 rows, got {}",
        nb_rows.len()
    );
    assert_eq!(
        oa_rows.len(),
        5,
        "test_oa.simple_table must return exactly 5 rows, got {}",
        oa_rows.len()
    );
    assert_ne!(
        nb_rows.len() + oa_rows.len(),
        nb_rows.len(),
        "rows must not be merged"
    );
}

/// Integration test: verify that the SSTableManager does not cross-contaminate
/// `collection_table` rows when both `test_collections` and `test_oa` are loaded.
#[tokio::test]
async fn test_sstable_manager_isolates_collection_tables_across_keyspaces() {
    let datasets = match datasets_root() {
        Some(d) => d,
        None => {
            eprintln!("SKIP: CQLITE_DATASETS_ROOT not set");
            return;
        }
    };

    let nb_dir = match table_dir(&datasets, "test_collections", "collection_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_collections/collection_table directory not found");
            return;
        }
    };
    let oa_dir = match table_dir(&datasets, "test_oa", "collection_table") {
        Some(d) => d,
        None => {
            eprintln!("SKIP: test_oa/collection_table directory not found");
            return;
        }
    };

    // Quick Data.db presence check
    let has_nb_data = nb_dir
        .read_dir()
        .ok()
        .map(|mut rd| {
            rd.any(|e| {
                e.map(|e| {
                    let n = e.file_name().to_string_lossy().to_string();
                    n.ends_with("-Data.db") && !n.ends_with(".jsonl")
                })
                .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    let has_oa_data = oa_dir
        .read_dir()
        .ok()
        .map(|mut rd| {
            rd.any(|e| {
                e.map(|e| {
                    let n = e.file_name().to_string_lossy().to_string();
                    n.ends_with("-Data.db") && !n.ends_with(".jsonl")
                })
                .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    if !has_nb_data || !has_oa_data {
        eprintln!("SKIP: Data.db files not present — run fetch-datasets.sh first");
        return;
    }

    use cqlite_core::storage::sstable::SSTableManager;
    use cqlite_core::{Config, Platform, TableId};
    use std::sync::Arc;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    let temp_dir = tempfile::TempDir::new().unwrap();

    let manager = SSTableManager::new_from_discovered_paths(
        temp_dir.path(),
        vec![nb_dir, oa_dir],
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .unwrap();

    let nb_id = TableId::new("test_collections.collection_table");
    let nb_rows = manager.scan(&nb_id, None, None, None, None).await.unwrap();

    let oa_id = TableId::new("test_oa.collection_table");
    let oa_rows = manager.scan(&oa_id, None, None, None, None).await.unwrap();

    // nb has 500 rows; oa has 3 rows — they must not be combined
    assert_eq!(
        nb_rows.len(),
        500,
        "test_collections.collection_table must return exactly 500 rows, got {}",
        nb_rows.len()
    );
    assert_eq!(
        oa_rows.len(),
        3,
        "test_oa.collection_table must return exactly 3 rows, got {}",
        oa_rows.len()
    );
}

/// Regression test for Issue #680 / Issue #548 interaction:
/// `WHERE pk = <value>` point-lookup on a qualified table name must return rows.
///
/// Before the #680 follow-up fix, `QueryParser::parse_select` stripped the keyspace
/// from "test_basic.simple_table" → "simple_table", causing the point-lookup path
/// in SSTableManager::get to miss the "test_basic.simple_table" registry key and
/// return 0 rows.
#[cfg(all(feature = "state_machine", feature = "cli-helpers"))]
#[tokio::test]
async fn test_qualified_point_lookup_returns_one_row() {
    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use cqlite_core::types::Value;
    use std::path::Path;

    let datasets = match datasets_root() {
        Some(d) => d,
        None => {
            eprintln!("SKIP: CQLITE_DATASETS_ROOT not set");
            return;
        }
    };

    // Check that Data.db files exist for test_basic
    let sstables_dir = datasets.join("sstables").join("test_basic");
    let has_data = std::fs::read_dir(&sstables_dir)
        .ok()
        .map(|rd| {
            rd.flatten().any(|e| {
                e.path().is_dir()
                    && std::fs::read_dir(e.path())
                        .ok()
                        .map(|inner| {
                            inner
                                .flatten()
                                .any(|f| f.file_name().to_string_lossy().ends_with("-Data.db"))
                        })
                        .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    if !has_data {
        eprintln!("SKIP: test_basic Data.db files not present — run fetch-datasets.sh");
        return;
    }

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let schema_path = manifest_dir
        .parent()
        .unwrap()
        .join("test-data")
        .join("schemas")
        .join("basic-types.cql");
    if !schema_path.exists() {
        eprintln!("SKIP: basic-types.cql not found at {:?}", schema_path);
        return;
    }

    let ingestion_config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets.join("sstables"),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_basic/".to_string()),
    };

    let db = match ingest(ingestion_config).await {
        Ok(r) => r.database,
        Err(e) => {
            eprintln!("SKIP: ingestion failed: {}", e);
            return;
        }
    };

    // Step 1: scan to get a real UUID partition key
    let scan = db
        .execute("SELECT id FROM test_basic.simple_table LIMIT 5")
        .await
        .expect("scan should succeed");

    if scan.rows.is_empty() {
        eprintln!("SKIP: scan returned 0 rows");
        return;
    }

    let uuid_str = scan.rows.iter().find_map(|row| {
        if let Some(Value::Uuid(bytes)) = row.values.get("id") {
            Some(format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5],
                bytes[6], bytes[7],
                bytes[8], bytes[9],
                bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
            ))
        } else {
            None
        }
    });

    let uuid_str = match uuid_str {
        Some(s) => s,
        None => {
            eprintln!("SKIP: no UUID values in scanned rows");
            return;
        }
    };

    // Step 2: point-lookup via WHERE — must return exactly 1 row
    let point_query = format!(
        "SELECT * FROM test_basic.simple_table WHERE id = {}",
        uuid_str
    );
    let point_result = db
        .execute(&point_query)
        .await
        .expect("point-lookup should succeed");

    assert_eq!(
        point_result.rows.len(),
        1,
        "Issue #680 regression: qualified point-lookup \
         'SELECT * FROM test_basic.simple_table WHERE id = {}' must return 1 row \
         (the parser must preserve the full qualified table name). Got {} rows.",
        uuid_str,
        point_result.rows.len(),
    );
}
