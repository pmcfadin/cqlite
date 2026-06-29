//! Minimal dataset helpers for Issue #78
//!
//! Provides simple functions to resolve canonical dataset paths from metadata.yml
//!
//! ## AppleDouble File Handling
//! This module includes robust filtering for AppleDouble files (macOS metadata files
//! with ._ prefix) to prevent CI test failures. All file iteration functions use
//! should_ignore_file() to consistently filter these metadata files.

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
        // Use compile-time manifest dir to calculate workspace root
        // CARGO_MANIFEST_DIR points to cqlite-core/, parent is workspace root
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .map(|workspace| workspace.join("test-data/datasets"))
            .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
    }
}

/// `true` when strict fixture mode is requested (issue #1230). Either
/// `CQLITE_REQUIRE_FIXTURES` (the repo-wide convention used by the newer
/// `issue_10xx`/`issue_99x` parity tests) or `CQLITE_PARITY_REQUIRE_DATASETS`
/// (issue #1205) set to a truthy value ("1"/"true") flips dataset-dependent
/// tests FAIL-CLOSED: an absent/empty fixture PANICS (test failure) instead of
/// skipping, so a required CI lane cannot false-green when a table is dropped or
/// a #773-class path regression hides the data. When neither is set, the default
/// skip-on-absence behavior is preserved so local dev without the binaries still
/// works. This is the single shared definition — do NOT fork a parallel copy.
pub fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    ) || matches!(
        std::env::var("CQLITE_PARITY_REQUIRE_DATASETS").as_deref(),
        Ok("1") | Ok("true")
    )
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

    // Find table directories (format: table-{hash}) and prefer one with Data.db
    let mut data_db_candidate: Option<PathBuf> = None;
    let mut index_db_candidate: Option<PathBuf> = None;
    let mut any_candidate: Option<PathBuf> = None;

    for entry in fs::read_dir(&sstables_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if !name.starts_with(&format!("{}-", table)) {
                continue;
            }
        }

        // Scan the directory to classify candidates
        let mut has_data = false;
        let mut has_index = false;
        let mut has_any = false;
        if let Ok(files) = fs::read_dir(&path) {
            for f in files.flatten() {
                if let Some(fname) = f.file_name().to_str() {
                    if should_ignore_file(fname) {
                        continue;
                    }
                    if fname.ends_with("-Data.db") {
                        has_data = true;
                        has_any = true;
                        break;
                    } else if fname.ends_with("-Index.db") {
                        has_index = true;
                        has_any = true;
                    } else if fname.ends_with("-Data.db.jsonl")
                        || fname.ends_with("-Statistics.db.txt")
                        || fname.ends_with("-Summary.db.txt")
                    {
                        has_any = true;
                    }
                }
            }
        }

        if has_data {
            data_db_candidate = Some(path);
            // Highest priority met; we can stop searching further
            break;
        }
        if has_index && index_db_candidate.is_none() {
            index_db_candidate = Some(path.clone());
        }
        if has_any && any_candidate.is_none() {
            any_candidate = Some(path.clone());
        }
    }

    if let Some(p) = data_db_candidate.or(index_db_candidate).or(any_candidate) {
        return Ok(p);
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

/// Check if a filename should be ignored (AppleDouble or other metadata files)
///
/// AppleDouble files are macOS metadata files with ._ prefix that should be
/// ignored when scanning for SSTable files to prevent CI test failures.
pub fn should_ignore_file(filename: &str) -> bool {
    // Filter AppleDouble files (macOS metadata with ._ prefix)
    filename.starts_with("._")
}

// (removed) has_sstable_files: no longer needed after prioritized directory resolution

// === Issue #89: Reference path derivation and parsers ===

/// Given a path to a Data.db file, derive sibling reference paths produced by export.sh
///
/// This function first tries to find files with exact naming patterns, then falls back to searching
/// for any valid reference files in the directory. This handles cases where CI datasets have
/// different UUIDs or naming patterns than expected.
pub fn derive_reference_paths_from_data_db(data_db: &Path) -> Option<(PathBuf, PathBuf, PathBuf)> {
    let file_name = data_db.file_name()?.to_str()?;
    if !file_name.ends_with("-Data.db") {
        return None;
    }
    let prefix = &file_name[..file_name.len() - "-Data.db".len()];
    let dir = data_db.parent()?;

    // First, try exact naming patterns
    let expected_jsonl = dir.join(format!("{}-Data.db.jsonl", prefix));
    let expected_stats = dir.join(format!("{}-Statistics.db.txt", prefix));
    let expected_summary = dir.join(format!("{}-Summary.db.txt", prefix));

    // If all expected files exist, return them
    if expected_jsonl.exists() && expected_stats.exists() && expected_summary.exists() {
        return Some((expected_jsonl, expected_stats, expected_summary));
    }

    // Otherwise, search for any valid reference files in the directory
    let mut found_jsonl = None;
    let mut found_stats = None;
    let mut found_summary = None;

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                // Skip AppleDouble files
                if should_ignore_file(name) {
                    continue;
                }

                // Look for any JSONL reference file
                if found_jsonl.is_none() && name.ends_with("-Data.db.jsonl") {
                    found_jsonl = Some(entry.path());
                }

                // Look for any Statistics reference file
                if found_stats.is_none() && name.ends_with("-Statistics.db.txt") {
                    found_stats = Some(entry.path());
                }

                // Look for any Summary reference file
                if found_summary.is_none() && name.ends_with("-Summary.db.txt") {
                    found_summary = Some(entry.path());
                }
            }
        }
    }

    // Return found files, or expected paths as fallback
    let data_jsonl = found_jsonl.unwrap_or(expected_jsonl);
    let stats_txt = found_stats.unwrap_or(expected_stats);
    let summary_txt = found_summary.unwrap_or(expected_summary);

    Some((data_jsonl, stats_txt, summary_txt))
}

/// Derive a companion SSTable file path (e.g., Index.db, Summary.db) from a Data.db path
///
/// This function handles cases where files might have different naming patterns or UUIDs
/// by searching for any valid companion file if the expected one doesn't exist.
pub fn derive_companion_file(data_file: &Path, companion_type: &str) -> Option<PathBuf> {
    let data_name = data_file.file_name()?.to_str()?;
    if !data_name.ends_with("-Data.db") {
        return None;
    }

    let prefix = &data_name[..data_name.len() - "-Data.db".len()];
    let dir = data_file.parent()?;

    // First, try exact naming pattern
    let expected_companion = dir.join(format!("{}-{}", prefix, companion_type));
    if expected_companion.exists() {
        return Some(expected_companion);
    }

    // Otherwise, search for any valid companion file in the directory
    let companion_suffix = format!("-{}", companion_type);
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                // Skip AppleDouble files
                if should_ignore_file(name) {
                    continue;
                }

                // Look for any file with the companion suffix
                if name.ends_with(&companion_suffix) {
                    return Some(entry.path());
                }
            }
        }
    }

    // Return expected path as fallback (for error messages)
    Some(expected_companion)
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
        .map_while(Result::ok)
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

// === references.yml support (Issue #89 deterministic selection) ===

#[derive(Debug, Clone, Deserialize)]
struct RefManifest {
    #[allow(dead_code)]
    refs_version: Option<u32>,
    #[allow(dead_code)]
    generated_at: Option<String>,
    tables: Vec<RefEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct RefEntry {
    keyspace: String,
    table: String,
    #[allow(dead_code)]
    sstable_dir: String,
    #[allow(dead_code)]
    prefix: String,
}

/// Load references.yml if present
pub fn load_references_manifest_at(root: &Path) -> Option<RefManifest> {
    let path = root.join("references.yml");
    let content = std::fs::read_to_string(path).ok()?;
    serde_yaml::from_str::<RefManifest>(&content).ok()
}

/// Resolve a stable table directory using references.yml when available.
/// We normalize absolute paths inside the manifest by replacing their root
/// with the provided datasets root.
pub fn resolve_table_dir_via_manifest(root: &Path, keyspace: &str, table: &str) -> Option<PathBuf> {
    let manifest = load_references_manifest_at(root)?;
    let entry = manifest
        .tables
        .into_iter()
        .find(|e| e.keyspace == keyspace && e.table == table)?;

    // Extract the hashed directory basename from the recorded path
    let hashed_dir_name = std::path::Path::new(&entry.sstable_dir)
        .file_name()
        .and_then(|n| n.to_str())?
        .to_string();

    let normalized = root.join("sstables").join(keyspace).join(hashed_dir_name);
    Some(normalized)
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
