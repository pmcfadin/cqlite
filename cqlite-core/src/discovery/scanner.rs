//! Filesystem scanner for SSTable discovery
//!
//! This module provides functionality for scanning a Cassandra data directory
//! and discovering SSTables, keyspaces, and tables.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Keyspace information
#[derive(Debug, Clone)]
pub struct KeyspaceInfo {
    /// Keyspace name
    pub name: String,
    /// Tables in this keyspace
    pub tables: Vec<TableInfo>,
}

/// Table information
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Fully qualified table name (keyspace.table)
    pub qualified_name: String,
    /// Keyspace name
    pub keyspace: String,
    /// Table name
    pub name: String,
    /// SSTable count
    pub sstable_count: usize,
    /// Table directory path
    pub path: PathBuf,
}

/// Result of scanning a data directory
#[derive(Debug, Clone)]
pub struct ScanResult {
    /// Keyspace names discovered (excluding system keyspaces)
    pub keyspaces: Vec<String>,
    /// Fully qualified table names discovered (excluding system tables)
    pub tables: Vec<String>,
    /// Total number of SSTables found
    pub sstable_count: usize,
    /// Detailed keyspace information
    pub keyspace_info: Vec<KeyspaceInfo>,
    /// Warnings about potential issues with the directory structure
    pub warnings: Vec<String>,
}

/// Check if a directory name has the expected Cassandra table format (name-uuid)
///
/// Cassandra table directories follow the pattern: `table_name-table_id`
/// where table_id is a 32-character hexadecimal UUID.
///
/// # Examples
/// - `simple_table-6aa08200a25111f0a3fef1a551383fb9` -> true
/// - `users-abc123def456789012345678901234567890` -> true (if 32 hex chars)
/// - `test_basic` -> false (no hyphen/uuid)
/// - `my-table` -> false (suffix too short)
fn has_cassandra_table_uuid_suffix(dir_name: &str) -> bool {
    if let Some(pos) = dir_name.rfind('-') {
        let suffix = &dir_name[pos + 1..];
        // Cassandra table UUIDs are 32 hex characters (no hyphens in directory name)
        suffix.len() == 32 && suffix.chars().all(|c| c.is_ascii_hexdigit())
    } else {
        false
    }
}

/// Scanner for discovering SSTables in a data directory
pub struct Scanner {
    data_dir: PathBuf,
    version_hint: Option<String>,
}

impl Scanner {
    /// Create a new scanner for the given data directory
    pub fn new(data_dir: &Path, version_hint: Option<String>) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            version_hint,
        }
    }

    /// Scan the data directory for SSTables
    ///
    /// This method scans the data directory structure and discovers:
    /// - Keyspaces (excluding system keyspaces)
    /// - Tables (excluding system tables)
    /// - SSTable files (Data.db files)
    ///
    /// Cassandra data directory structure is:
    /// data_dir/keyspace_name/table_name-table_id/sstable_files
    pub fn scan(&self) -> Result<ScanResult> {
        let mut keyspaces = Vec::new();
        let mut tables = Vec::new();
        let mut sstable_count = 0;
        let mut keyspace_info = Vec::new();

        // Read top-level directory entries (keyspaces)
        let entries = std::fs::read_dir(&self.data_dir).map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to read data directory {}: {}",
                    self.data_dir.display(),
                    e
                ),
            ))
        })?;

        for entry in entries.flatten() {
            if !entry.path().is_dir() {
                continue;
            }

            let keyspace_name = entry.file_name().to_string_lossy().to_string();

            // Skip system keyspaces
            if keyspace_name.starts_with("system") {
                continue;
            }

            keyspaces.push(keyspace_name.clone());

            // Scan tables in this keyspace
            let mut keyspace_tables = Vec::new();
            if let Ok(table_entries) = std::fs::read_dir(entry.path()) {
                for table_entry in table_entries.flatten() {
                    if !table_entry.path().is_dir() {
                        continue;
                    }

                    let table_dir_name = table_entry.file_name().to_string_lossy().to_string();

                    // Extract table name (format: table_name-table_id)
                    let table_name = table_dir_name
                        .split('-')
                        .next()
                        .unwrap_or(&table_dir_name)
                        .to_string();

                    let qualified_name = format!("{}.{}", keyspace_name, table_name);

                    // Count SSTable files (Data.db files)
                    let mut table_sstable_count = 0;
                    if let Ok(sstable_files) = std::fs::read_dir(table_entry.path()) {
                        for sstable_file in sstable_files.flatten() {
                            let file_name = sstable_file.file_name().to_string_lossy().to_string();
                            // Match both old and new SSTable naming conventions
                            if file_name.ends_with("-Data.db") || file_name == "Data.db" {
                                table_sstable_count += 1;
                                sstable_count += 1;
                            }
                        }
                    }

                    tables.push(qualified_name.clone());
                    keyspace_tables.push(TableInfo {
                        qualified_name,
                        keyspace: keyspace_name.clone(),
                        name: table_name,
                        sstable_count: table_sstable_count,
                        path: table_entry.path(),
                    });
                }
            }

            if !keyspace_tables.is_empty() {
                keyspace_info.push(KeyspaceInfo {
                    name: keyspace_name,
                    tables: keyspace_tables,
                });
            }
        }

        // Validate directory structure: check if table directories have expected UUID format
        let mut warnings = Vec::new();
        if !tables.is_empty() {
            let valid_table_dir_count = keyspace_info
                .iter()
                .flat_map(|k| &k.tables)
                .filter(|t| {
                    t.path
                        .file_name()
                        .map(|n| has_cassandra_table_uuid_suffix(&n.to_string_lossy()))
                        .unwrap_or(false)
                })
                .count();

            if valid_table_dir_count == 0 {
                warnings.push(format!(
                    "Warning: No table directories with expected 'name-uuid' format found.\n\
                     The --data-dir may be pointing to the wrong directory level.\n\
                     Current path: {}\n\
                     Expected structure: <data-dir>/<keyspace>/<table>-<uuid>/\n\
                     Hint: Try using a subdirectory like: {}/sstables or {}/data",
                    self.data_dir.display(),
                    self.data_dir.display(),
                    self.data_dir.display()
                ));
            }
        }

        Ok(ScanResult {
            keyspaces,
            tables,
            sstable_count,
            keyspace_info,
            warnings,
        })
    }

    /// Resolve Cassandra version using precedence:
    /// 1. version_hint (if provided)
    /// 2. SSTable metadata (from Data.db headers)
    /// 3. metadata.yml (cluster metadata)
    /// 4. "unknown" (fallback)
    pub fn resolve_version(&self, _scan_result: &ScanResult) -> Result<Option<String>> {
        // Precedence 1: Use version hint if provided
        if let Some(hint) = &self.version_hint {
            return Ok(Some(hint.clone()));
        }

        // Precedence 2: Try to read version from SSTable metadata
        // TODO: Implement SSTable header version detection
        // This would require reading the first few bytes of a Data.db file

        // Precedence 3: Try to read metadata.yml
        let metadata_path = self.data_dir.join("metadata.yml");
        if metadata_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&metadata_path) {
                // Parse YAML for version field (simple string search, not full YAML parsing)
                for line in content.lines() {
                    if line.trim().starts_with("version:") {
                        let version = line
                            .trim()
                            .strip_prefix("version:")
                            .unwrap_or("")
                            .trim()
                            .trim_matches('"')
                            .trim_matches('\'')
                            .to_string();
                        if !version.is_empty() {
                            return Ok(Some(version));
                        }
                    }
                }
            }
        }

        // Precedence 4: Unknown
        Ok(Some("unknown".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_scanner_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = Scanner::new(temp_dir.path(), None);
        let result = scanner.scan().unwrap();

        assert_eq!(result.sstable_count, 0);
        assert!(result.keyspaces.is_empty());
        assert!(result.tables.is_empty());
        assert!(result.keyspace_info.is_empty());
    }

    #[test]
    fn test_scanner_with_structure() {
        let temp_dir = TempDir::new().unwrap();

        // Create keyspace/table directory structure
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();

        // Use valid 32-char hex UUID suffix (Cassandra table directory format)
        let table_dir = keyspace_dir.join("users-6aa08200a25111f0a3fef1a551383fb9");
        fs::create_dir(&table_dir).unwrap();

        // Create mock SSTable files
        fs::write(table_dir.join("na-1-big-Data.db"), b"mock data").unwrap();
        fs::write(table_dir.join("na-2-big-Data.db"), b"mock data").unwrap();

        let scanner = Scanner::new(temp_dir.path(), None);
        let result = scanner.scan().unwrap();

        assert_eq!(result.sstable_count, 2);
        assert_eq!(result.keyspaces.len(), 1);
        assert!(result.keyspaces.contains(&"test_ks".to_string()));
        assert_eq!(result.tables.len(), 1);
        assert!(result.tables.iter().any(|t| t.starts_with("test_ks.users")));
        assert_eq!(result.keyspace_info.len(), 1);
        assert_eq!(result.keyspace_info[0].name, "test_ks");
        assert_eq!(result.keyspace_info[0].tables.len(), 1);
        assert_eq!(result.keyspace_info[0].tables[0].sstable_count, 2);
        // No warnings for valid structure
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_scanner_skips_system_keyspaces() {
        let temp_dir = TempDir::new().unwrap();

        // Create system keyspace
        let system_dir = temp_dir.path().join("system");
        fs::create_dir(&system_dir).unwrap();
        let system_table_dir = system_dir.join("local-6aa08200a25111f0a3fef1a551383fb9");
        fs::create_dir(&system_table_dir).unwrap();
        fs::write(system_table_dir.join("Data.db"), b"mock").unwrap();

        // Create user keyspace with valid UUID suffix
        let user_dir = temp_dir.path().join("user_ks");
        fs::create_dir(&user_dir).unwrap();
        let user_table_dir = user_dir.join("table-7bb09311b36222f1b4fef2b662494fc0");
        fs::create_dir(&user_table_dir).unwrap();
        fs::write(user_table_dir.join("na-1-big-Data.db"), b"mock").unwrap();

        let scanner = Scanner::new(temp_dir.path(), None);
        let result = scanner.scan().unwrap();

        // Should only find user keyspace
        assert_eq!(result.keyspaces.len(), 1);
        assert!(result.keyspaces.contains(&"user_ks".to_string()));
        assert!(!result.keyspaces.iter().any(|k| k.starts_with("system")));
        assert_eq!(result.sstable_count, 1);
        // No warnings for valid structure
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_resolve_version_with_hint() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = Scanner::new(temp_dir.path(), Some("5.0".to_string()));
        let result = scanner.scan().unwrap();
        let version = scanner.resolve_version(&result).unwrap();

        assert_eq!(version, Some("5.0".to_string()));
    }

    #[test]
    fn test_resolve_version_from_metadata_yml() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_content = "version: 5.0.1\nother: field\n";
        fs::write(temp_dir.path().join("metadata.yml"), metadata_content).unwrap();

        let scanner = Scanner::new(temp_dir.path(), None);
        let result = scanner.scan().unwrap();
        let version = scanner.resolve_version(&result).unwrap();

        assert_eq!(version, Some("5.0.1".to_string()));
    }

    #[test]
    fn test_resolve_version_unknown() {
        let temp_dir = TempDir::new().unwrap();
        let scanner = Scanner::new(temp_dir.path(), None);
        let result = scanner.scan().unwrap();
        let version = scanner.resolve_version(&result).unwrap();

        assert_eq!(version, Some("unknown".to_string()));
    }

    #[test]
    fn test_scanner_multiple_keyspaces() {
        let temp_dir = TempDir::new().unwrap();

        // Use different valid UUIDs for each keyspace
        let uuids = [
            "6aa08200a25111f0a3fef1a551383fb9",
            "7bb09311b36222f1b4fef2b662494fc0",
            "8cc0a422c47333f2c5fef3c773505fd1",
        ];

        // Create multiple keyspaces with valid UUID table directories
        for (i, ks_name) in ["keyspace1", "keyspace2", "keyspace3"].iter().enumerate() {
            let ks_dir = temp_dir.path().join(ks_name);
            fs::create_dir(&ks_dir).unwrap();

            let table_dir = ks_dir.join(format!("{}_table-{}", ks_name, uuids[i]));
            fs::create_dir(&table_dir).unwrap();
            fs::write(table_dir.join("na-1-big-Data.db"), b"mock").unwrap();
        }

        let scanner = Scanner::new(temp_dir.path(), None);
        let result = scanner.scan().unwrap();

        assert_eq!(result.keyspaces.len(), 3);
        assert_eq!(result.tables.len(), 3);
        assert_eq!(result.sstable_count, 3);
        // No warnings for valid structure
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_scanner_warns_on_invalid_table_directory_format() {
        let temp_dir = TempDir::new().unwrap();

        // Create directory structure that LOOKS like Cassandra data but has wrong format
        // This simulates user pointing to parent directory instead of data directory
        let sstables_dir = temp_dir.path().join("sstables");
        fs::create_dir(&sstables_dir).unwrap();

        // Create directories that look like keyspaces but are actually tables
        // (missing UUID suffix - this is what happens when pointing to wrong level)
        for ks_name in &["test_basic", "test_collections"] {
            let dir = sstables_dir.join(ks_name);
            fs::create_dir(&dir).unwrap();
            // Create a file so it counts as having sstables
            fs::write(dir.join("na-1-big-Data.db"), b"mock").unwrap();
        }

        let scanner = Scanner::new(temp_dir.path(), None);
        let result = scanner.scan().unwrap();

        // Should find tables (even though structure is wrong)
        assert!(!result.tables.is_empty());
        // But should have a warning about the structure
        assert!(!result.warnings.is_empty());
        assert!(result.warnings[0].contains("name-uuid"));
        assert!(result.warnings[0].contains("wrong directory level"));
    }

    #[test]
    fn test_scanner_invalid_directory() {
        let scanner = Scanner::new(Path::new("/nonexistent/path"), None);
        let result = scanner.scan();

        assert!(result.is_err());
        if let Err(Error::Io(io_err)) = result {
            assert_eq!(io_err.kind(), std::io::ErrorKind::NotFound);
        } else {
            panic!("Expected Io error");
        }
    }

    #[test]
    fn test_has_cassandra_table_uuid_suffix() {
        // Valid Cassandra table directory names (32 hex chars after hyphen)
        assert!(has_cassandra_table_uuid_suffix(
            "simple_table-6aa08200a25111f0a3fef1a551383fb9"
        ));
        assert!(has_cassandra_table_uuid_suffix(
            "users-0123456789abcdef0123456789abcdef"
        ));
        assert!(has_cassandra_table_uuid_suffix(
            "my_table-ABCDEF0123456789ABCDEF0123456789"
        )); // uppercase hex

        // Invalid - no hyphen
        assert!(!has_cassandra_table_uuid_suffix("test_basic"));
        assert!(!has_cassandra_table_uuid_suffix("users"));

        // Invalid - suffix too short
        assert!(!has_cassandra_table_uuid_suffix("users-abc123"));
        assert!(!has_cassandra_table_uuid_suffix("table-456"));

        // Invalid - suffix too long
        assert!(!has_cassandra_table_uuid_suffix(
            "table-6aa08200a25111f0a3fef1a551383fb9extra"
        ));

        // Invalid - suffix contains non-hex characters
        assert!(!has_cassandra_table_uuid_suffix(
            "table-6aa08200a25111f0a3fef1a551383fgz"
        )); // 'g' and 'z' not hex
    }
}
