//! Minimal dataset helpers for Issue #78
//!
//! Provides simple functions to resolve canonical dataset paths from metadata.yml

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// Error type for dataset operations
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("Dataset not found: {keyspace}.{table}. Available datasets: {available}")]
    DatasetNotFound {
        keyspace: String,
        table: String,
        available: String,
    },
    #[error("Metadata file not found at {path}")]
    MetadataNotFound { path: String },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML parsing error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("Directory not found: {path}")]
    DirectoryNotFound { path: String },
}

/// Keyspace metadata from metadata.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keyspace {
    pub name: String,
    pub tables: Vec<Table>,
}

/// Table metadata from metadata.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub row_count: u64,
}

/// Root metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub keyspaces: Vec<Keyspace>,
}

/// Table information for listing
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub keyspace: String,
    pub table: String,
    pub row_count: u64,
}

/// Get the datasets root directory, checking CQLITE_DATASETS_ROOT env var first
fn get_datasets_root() -> PathBuf {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        PathBuf::from(root)
    } else {
        PathBuf::from("../test-data/datasets")
    }
}

/// Load metadata.yml from datasets root
pub fn load_metadata() -> Result<Metadata, DatasetError> {
    load_metadata_at(&get_datasets_root())
}

/// Load metadata.yml from specified root path
pub fn load_metadata_at(root: &Path) -> Result<Metadata, DatasetError> {
    let metadata_path = root.join("metadata.yml");

    if !metadata_path.exists() {
        return Err(DatasetError::MetadataNotFound {
            path: metadata_path.to_string_lossy().to_string(),
        });
    }

    let content = fs::read_to_string(metadata_path)?;
    let metadata: Metadata = serde_yaml::from_str(&content)?;

    Ok(metadata)
}

/// Resolve table to SSTable path under datasets/sstables/
/// This is the main function required by Issue #78
pub fn resolve_table_to_sstable_path(keyspace: &str, table: &str) -> Result<PathBuf, DatasetError> {
    resolve_table_to_sstable_path_at(&get_datasets_root(), keyspace, table)
}

/// Resolve table to SSTable path under specified root/sstables/
pub fn resolve_table_to_sstable_path_at(
    root: &Path,
    keyspace: &str,
    table: &str,
) -> Result<PathBuf, DatasetError> {
    let metadata = load_metadata_at(root)?;

    // Verify table exists in metadata
    let mut found = false;
    for ks in &metadata.keyspaces {
        if ks.name == keyspace {
            for tbl in &ks.tables {
                if tbl.name == table {
                    found = true;
                    break;
                }
            }
        }
    }

    if !found {
        let available = list_tables_at(root, None)?
            .into_iter()
            .map(|t| format!("{}.{}", t.keyspace, t.table))
            .collect::<Vec<_>>()
            .join(", ");

        return Err(DatasetError::DatasetNotFound {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            available,
        });
    }

    // Look for directory under root/sstables/keyspace/
    let sstables_dir = root.join("sstables").join(keyspace);

    if !sstables_dir.exists() {
        return Err(DatasetError::DirectoryNotFound {
            path: sstables_dir.to_string_lossy().to_string(),
        });
    }

    // Find table directory (format: table-{hash})
    let entries = fs::read_dir(&sstables_dir)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with(&format!("{}-", table)) {
                    // Verify this directory has SSTable files
                    if has_sstable_files(&path)? {
                        return Ok(path);
                    }
                }
            }
        }
    }

    Err(DatasetError::DirectoryNotFound {
        path: format!("{}/{}-*", sstables_dir.to_string_lossy(), table),
    })
}

/// List tables, optionally filtered by keyspace
pub fn list_tables(keyspace_filter: Option<&str>) -> Result<Vec<TableInfo>, DatasetError> {
    list_tables_at(&get_datasets_root(), keyspace_filter)
}

/// List tables from specified root, optionally filtered by keyspace  
pub fn list_tables_at(
    root: &Path,
    keyspace_filter: Option<&str>,
) -> Result<Vec<TableInfo>, DatasetError> {
    let metadata = load_metadata_at(root)?;
    let mut tables = Vec::new();

    for keyspace in &metadata.keyspaces {
        if let Some(filter) = keyspace_filter {
            if keyspace.name != filter {
                continue;
            }
        }

        for table in &keyspace.tables {
            tables.push(TableInfo {
                keyspace: keyspace.name.clone(),
                table: table.name.clone(),
                row_count: table.row_count,
            });
        }
    }

    Ok(tables)
}

/// Check if a directory contains SSTable files
fn has_sstable_files(dir: &Path) -> Result<bool, DatasetError> {
    let entries = fs::read_dir(dir)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with("-Data.db") {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

// === Issue #89: Reference path derivation and parsers ===

/// Given a path to a Data.db file, derive sibling reference paths produced by export.sh
pub fn derive_reference_paths_from_data_db(data_db: &Path) -> Option<(PathBuf, PathBuf, PathBuf)> {
    let file_name = data_db.file_name()?.to_str()?;
    if !file_name.ends_with("-Data.db") {
        return None;
    }
    let prefix = &file_name[..file_name.len() - "-Data.db".len()];
    let dir = data_db.parent()?;
    let data_jsonl = dir.join(format!("{}-Data.db.jsonl", prefix));
    let stats_txt = dir.join(format!("{}-Statistics.db.txt", prefix));
    let summary_txt = dir.join(format!("{}-Summary.db.txt", prefix));
    Some((data_jsonl, stats_txt, summary_txt))
}

/// Stream JSONL rows from sstabledump output file, yielding serde_json::Value for each line
pub fn read_jsonl_rows(
    path: &Path,
) -> Result<impl Iterator<Item = serde_json::Value>, DatasetError> {
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let iter = reader
        .lines()
        .filter_map(|l| l.ok())
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(&l).ok());
    Ok(iter)
}

/// Parse sstablemetadata text to a simple key->value map for assertions
pub fn parse_sstablemetadata_text(
    path: &Path,
) -> Result<std::collections::HashMap<String, String>, DatasetError> {
    let content = std::fs::read_to_string(path)?;
    let mut map = std::collections::HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn create_test_metadata(datasets_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all(datasets_root)?;

        let metadata = Metadata {
            keyspaces: vec![Keyspace {
                name: "test_basic".to_string(),
                tables: vec![Table {
                    name: "simple_table".to_string(),
                    row_count: 1000,
                }],
            }],
        };

        let metadata_content = serde_yaml::to_string(&metadata)?;
        fs::write(datasets_root.join("metadata.yml"), metadata_content)?;

        // Create sstables directory structure
        let sstables_dir = datasets_root.join("sstables/test_basic/simple_table-abc123def456");
        fs::create_dir_all(&sstables_dir)?;
        fs::write(sstables_dir.join("nb-1-big-Data.db"), "test data")?;

        Ok(())
    }

    #[test]
    fn test_load_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let datasets_root = temp_dir.path().join("datasets");

        create_test_metadata(&datasets_root).unwrap();

        let metadata = load_metadata_at(&datasets_root).unwrap();
        assert_eq!(metadata.keyspaces.len(), 1);
        assert_eq!(metadata.keyspaces[0].name, "test_basic");
        assert_eq!(metadata.keyspaces[0].tables.len(), 1);
        assert_eq!(metadata.keyspaces[0].tables[0].name, "simple_table");
    }

    #[test]
    fn test_resolve_table_to_sstable_path() {
        let temp_dir = TempDir::new().unwrap();
        let datasets_root = temp_dir.path().join("datasets");

        create_test_metadata(&datasets_root).unwrap();

        let path =
            resolve_table_to_sstable_path_at(&datasets_root, "test_basic", "simple_table").unwrap();
        assert!(path.ends_with("simple_table-abc123def456"));
        assert!(path.exists());
    }

    #[test]
    fn test_list_tables() {
        let temp_dir = TempDir::new().unwrap();
        let datasets_root = temp_dir.path().join("datasets");

        create_test_metadata(&datasets_root).unwrap();

        let tables = list_tables_at(&datasets_root, None).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].keyspace, "test_basic");
        assert_eq!(tables[0].table, "simple_table");
        assert_eq!(tables[0].row_count, 1000);

        let filtered = list_tables_at(&datasets_root, Some("test_basic")).unwrap();
        assert_eq!(filtered.len(), 1);

        let empty = list_tables_at(&datasets_root, Some("nonexistent")).unwrap();
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn test_table_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let datasets_root = temp_dir.path().join("datasets");

        create_test_metadata(&datasets_root).unwrap();

        let result = resolve_table_to_sstable_path_at(&datasets_root, "test_basic", "nonexistent");
        assert!(result.is_err());
        assert!(matches!(result, Err(DatasetError::DatasetNotFound { .. })));
    }
}
