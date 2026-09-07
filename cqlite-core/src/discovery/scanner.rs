//! Filesystem scanner for SSTable discovery
//!
//! This module provides functionality for scanning a Cassandra data directory
//! and discovering SSTables, keyspaces, and tables.
//!
//! # Every directory read is FAIL-CLOSED (issue #4159)
//!
//! [`Scanner::scan`] used to swallow every filesystem error below the top-level
//! `read_dir`: `if let Ok(table_entries) = read_dir(..)` with no `else`,
//! `entries.flatten()` over a `ReadDir` (whose items are `io::Result<DirEntry>`, so
//! `Iterator::flatten` drops each `Err` with no log at all), and
//! `entry.path().is_dir()` (which answers `false` for a directory it could not
//! `stat`). The observable results were: an unreadable KEYSPACE contributed zero
//! tables and was indistinguishable from an empty keyspace; an unreadable TABLE
//! directory was still reported with `sstable_count: 0`; and an entry that errored
//! mid-iteration silently never existed.
//!
//! That is the issue #4159 defect one layer out from the manager — the discovery
//! leg. `DiscoveryService::scan` propagates only the top-level `Error::Io`, so none
//! of those ever reached a `?`, and a caller that then opened the reported (short)
//! table set got a successful, silently incomplete answer. Each read now names its
//! path and propagates.

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

/// Read `dir`, naming it (and what it was being read AS) on failure.
///
/// Issue #4159: a directory this scanner cannot read must not read as an EMPTY
/// directory — an unreadable keyspace and a keyspace with no tables are different
/// facts, and a caller cannot tell them apart from a short result set.
fn read_dir_named(dir: &Path, what: &str) -> Result<std::fs::ReadDir> {
    std::fs::read_dir(dir).map_err(|e| {
        Error::Io(std::io::Error::new(
            e.kind(),
            format!("Failed to read {what} directory {}: {e}", dir.display()),
        ))
    })
}

/// Unwrap one `ReadDir` item, naming the directory it came from on failure.
///
/// `ReadDir` yields `io::Result<DirEntry>`: an entry can fail MID-ITERATION (a
/// concurrent unlink, an I/O fault). `Iterator::flatten` discards those silently,
/// which is how an entry came to "never exist".
fn dir_entry_named(
    item: std::io::Result<std::fs::DirEntry>,
    dir: &Path,
) -> Result<std::fs::DirEntry> {
    item.map_err(|e| {
        Error::Io(std::io::Error::new(
            e.kind(),
            format!("Failed to read an entry of {}: {e}", dir.display()),
        ))
    })
}

/// Is `entry` a directory? Propagates the `stat` failure instead of answering
/// `false`, which is what `Path::is_dir()` does for a directory it cannot stat.
fn entry_is_dir(entry: &std::fs::DirEntry) -> Result<bool> {
    entry.file_type().map(|ft| ft.is_dir()).map_err(|e| {
        Error::Io(std::io::Error::new(
            e.kind(),
            format!(
                "Failed to determine the file type of {}: {e}",
                entry.path().display()
            ),
        ))
    })
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
        let entries = read_dir_named(&self.data_dir, "data")?;

        for entry in entries {
            let entry = dir_entry_named(entry, &self.data_dir)?;
            if !entry_is_dir(&entry)? {
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
            {
                let keyspace_dir = entry.path();
                let table_entries = read_dir_named(&keyspace_dir, "keyspace")?;
                for table_entry in table_entries {
                    let table_entry = dir_entry_named(table_entry, &keyspace_dir)?;
                    if !entry_is_dir(&table_entry)? {
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
                    {
                        let table_dir = table_entry.path();
                        let sstable_files = read_dir_named(&table_dir, "table")?;
                        for sstable_file in sstable_files {
                            let sstable_file = dir_entry_named(sstable_file, &table_dir)?;
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
            // #4159: a metadata.yml that EXISTS and cannot be read is a real fault,
            // not "no version recorded" — it is propagated rather than falling
            // through to the "unknown" answer below. (An ABSENT metadata.yml is the
            // ordinary case and is handled by the `exists()` guard, not here.)
            let content = std::fs::read_to_string(&metadata_path).map_err(|e| {
                Error::Io(std::io::Error::new(
                    e.kind(),
                    format!(
                        "metadata.yml exists at {} but could not be read: {e}",
                        metadata_path.display()
                    ),
                ))
            })?;
            {
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

    /// Issue #4159: an UNREADABLE keyspace directory must not read as an EMPTY one.
    ///
    /// Staged with a mode-0 directory, which is what an EACCES on a real deployment
    /// looks like. Skipped for root (whose `read_dir` ignores the mode), and the skip
    /// is LOUD rather than silent — a case that cannot be staged must not read as a
    /// case that passed.
    // Permission-staged, so Unix-only: `PermissionsExt` and `geteuid` do not exist
    // elsewhere, and there is no portable way to make a directory unreadable.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_keyspace_directory_is_an_error_not_an_empty_keyspace() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();
        let table_dir = keyspace_dir.join("users-6aa08200a25111f0a3fef1a551383fb9");
        fs::create_dir(&table_dir).unwrap();
        fs::write(table_dir.join("na-1-big-Data.db"), b"mock data").unwrap();

        // Control: readable ⇒ the table and its SSTable are discovered.
        let before = Scanner::new(temp_dir.path(), None)
            .scan()
            .expect("a readable corpus must scan");
        assert_eq!(before.tables, vec!["test_ks.users".to_string()]);
        assert_eq!(before.sstable_count, 1);

        fs::set_permissions(&keyspace_dir, fs::Permissions::from_mode(0o000)).unwrap();
        let result = Scanner::new(temp_dir.path(), None).scan();
        // Restore before asserting so a failure cannot leave an unremovable TempDir.
        fs::set_permissions(&keyspace_dir, fs::Permissions::from_mode(0o755)).unwrap();

        if nix_running_as_root() {
            eprintln!(
                "SKIPPED (running as root, which bypasses the directory mode): the \
                 unreadable-keyspace case cannot be staged here"
            );
            return;
        }

        let e = result.expect_err(
            "an unreadable keyspace directory used to be swallowed by \
             `if let Ok(table_entries) = read_dir(..)` with no else, so the keyspace \
             contributed ZERO tables and was indistinguishable from an empty one",
        );
        let msg = e.to_string();
        assert!(
            msg.contains("keyspace") && msg.contains("test_ks"),
            "the error must name what it failed to read and where: {msg}"
        );
    }

    /// Issue #4159: an unreadable TABLE directory must not be reported with
    /// `sstable_count: 0` — that claimed the table exists and holds nothing.
    // Permission-staged, so Unix-only: `PermissionsExt` and `geteuid` do not exist
    // elsewhere, and there is no portable way to make a directory unreadable.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_table_directory_is_an_error_not_a_zero_sstable_table() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();
        let table_dir = keyspace_dir.join("users-6aa08200a25111f0a3fef1a551383fb9");
        fs::create_dir(&table_dir).unwrap();
        fs::write(table_dir.join("na-1-big-Data.db"), b"mock data").unwrap();

        fs::set_permissions(&table_dir, fs::Permissions::from_mode(0o000)).unwrap();
        let result = Scanner::new(temp_dir.path(), None).scan();
        fs::set_permissions(&table_dir, fs::Permissions::from_mode(0o755)).unwrap();

        if nix_running_as_root() {
            eprintln!(
                "SKIPPED (running as root, which bypasses the directory mode): the \
                 unreadable-table case cannot be staged here"
            );
            return;
        }

        let e = result.expect_err("an unreadable table directory must not report 0 SSTables");
        let msg = e.to_string();
        assert!(
            msg.contains("table") && msg.contains("users-"),
            "the error must name what it failed to read and where: {msg}"
        );
    }

    /// A mode-0 directory is still readable by root, so the two permission-staged
    /// cases above cannot assert anything there. Answered from the effective uid,
    /// never from whether the operation happened to succeed — the latter would make
    /// the guard indistinguishable from the defect it exists to catch.
    #[cfg(unix)]
    fn nix_running_as_root() -> bool {
        // SAFETY: `geteuid` takes no arguments, touches no memory and cannot fail.
        unsafe { libc::geteuid() == 0 }
    }
}
