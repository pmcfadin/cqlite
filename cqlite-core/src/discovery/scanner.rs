//! Filesystem scanner for SSTable discovery
//!
//! This module provides functionality for scanning a Cassandra data directory
//! and discovering SSTables, keyspaces, and tables.
//!
//! # An unreadable directory is REPORTED, never swallowed and never fatal (#4159)
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
//! table set got a successful, silently incomplete answer.
//!
//! Making each of those `?` instead was the opposite error: one inaccessible
//! directory then failed the WHOLE scan, and a root-owned `lost+found` at mode 0700
//! — present on essentially every ext4 data volume — would make discovery
//! impossible on the commonest real layout. So each read below the top level
//! RECORDS what it could not see into [`ScanResult::unreadable_dirs`] and carries
//! on, preserving the [`std::io::ErrorKind`] so `PermissionDenied` stays
//! distinguishable from `NotFound`.
//!
//! The one read that still propagates is the TOP-LEVEL `data_dir`: that path is the
//! caller's own argument, an unreadable one makes every possible answer empty, and
//! a direct error is the actionable answer there.
//!
//! **A recorded gap is not a logged gap.** `unreadable_dirs` is carried into
//! [`DiscoverySummary`](crate::discovery::DiscoverySummary), rendered by its
//! `summary_text()`, and — via `Database::note_incomplete_discovery` — seeded into
//! the `SSTableManager` that the discovered directories are opened with, so a query
//! for a table this scan could not enumerate fails closed with
//! [`Error::IncompleteDiscovery`] instead of answering an empty success.

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

/// One directory a scan could not read.
///
/// Structured rather than a `warnings` string: a caller has to be able to FAIL
/// CLOSED on this (and to name the path to an operator) without parsing prose, and
/// the [`std::io::ErrorKind`] is what separates a permissions problem from a
/// directory that was removed mid-scan.
#[derive(Debug, Clone)]
pub struct UnreadableDirectory {
    /// The directory that could not be read.
    pub path: PathBuf,
    /// What it was being read as ("keyspace", "table", …).
    pub role: String,
    /// The original failure's kind.
    pub kind: std::io::ErrorKind,
    /// The original failure, rendered.
    pub message: String,
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
    /// Directories the scan could NOT read (issue #4159).
    ///
    /// Non-empty means this result is INCOMPLETE: the keyspace/table lists are what
    /// was reachable, not what exists. A caller that reports absence — or opens the
    /// listed directories and then answers queries — must fail closed on this.
    pub unreadable_dirs: Vec<UnreadableDirectory>,
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

/// Record `e` against `dir` as a gap, rather than swallowing it or aborting.
fn note_unreadable(
    into: &mut Vec<UnreadableDirectory>,
    dir: &Path,
    role: &str,
    e: &Error,
    kind: std::io::ErrorKind,
) {
    tracing::warn!(
        "SSTable discovery could not read {role} directory {}: {e}. The scan is \
         INCOMPLETE.",
        dir.display()
    );
    into.push(UnreadableDirectory {
        path: dir.to_path_buf(),
        role: role.to_string(),
        kind,
        message: e.to_string(),
    });
}

/// The [`std::io::ErrorKind`] behind a crate [`Error`], for the gap record.
fn kind_of(e: &Error) -> std::io::ErrorKind {
    match e {
        Error::Io(io) => io.kind(),
        _ => std::io::ErrorKind::Other,
    }
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
        let mut unreadable_dirs: Vec<UnreadableDirectory> = Vec::new();

        // Read top-level directory entries (keyspaces). This ONE read still
        // propagates: `data_dir` is the caller's own argument, and if it cannot be
        // read then every answer is empty and the direct error is the actionable
        // one. Everything below it records and carries on — see the module doc.
        let entries = read_dir_named(&self.data_dir, "data")?;

        for entry in entries {
            let entry = match dir_entry_named(entry, &self.data_dir) {
                Ok(entry) => entry,
                Err(e) => {
                    let kind = kind_of(&e);
                    note_unreadable(&mut unreadable_dirs, &self.data_dir, "data", &e, kind);
                    continue;
                }
            };
            match entry_is_dir(&entry) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    let kind = kind_of(&e);
                    let path = entry.path();
                    note_unreadable(&mut unreadable_dirs, &path, "keyspace", &e, kind);
                    continue;
                }
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
                let table_entries = match read_dir_named(&keyspace_dir, "keyspace") {
                    Ok(entries) => entries,
                    Err(e) => {
                        let kind = kind_of(&e);
                        note_unreadable(&mut unreadable_dirs, &keyspace_dir, "keyspace", &e, kind);
                        continue;
                    }
                };
                for table_entry in table_entries {
                    let table_entry = match dir_entry_named(table_entry, &keyspace_dir) {
                        Ok(entry) => entry,
                        Err(e) => {
                            let kind = kind_of(&e);
                            note_unreadable(
                                &mut unreadable_dirs,
                                &keyspace_dir,
                                "keyspace",
                                &e,
                                kind,
                            );
                            continue;
                        }
                    };
                    match entry_is_dir(&table_entry) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            let kind = kind_of(&e);
                            let p = table_entry.path();
                            note_unreadable(&mut unreadable_dirs, &p, "table", &e, kind);
                            continue;
                        }
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
                        // The table directory EXISTS — we just saw it — so the
                        // table is still REPORTED even when its contents cannot be
                        // read. That keeps it in `table_directories`, so the
                        // SSTableManager re-reads the same directory, records its
                        // own gap, and fails closed at query time. Dropping the
                        // table here would hide it from that check entirely. What
                        // must not happen is reporting `sstable_count: 0` with no
                        // gap record, which is what made "unreadable" read as
                        // "empty".
                        let sstable_files = match read_dir_named(&table_dir, "table") {
                            Ok(entries) => Some(entries),
                            Err(e) => {
                                let kind = kind_of(&e);
                                note_unreadable(
                                    &mut unreadable_dirs,
                                    &table_dir,
                                    "table",
                                    &e,
                                    kind,
                                );
                                None
                            }
                        };
                        for sstable_file in sstable_files.into_iter().flatten() {
                            let sstable_file = match dir_entry_named(sstable_file, &table_dir) {
                                Ok(entry) => entry,
                                Err(e) => {
                                    let kind = kind_of(&e);
                                    note_unreadable(
                                        &mut unreadable_dirs,
                                        &table_dir,
                                        "table",
                                        &e,
                                        kind,
                                    );
                                    continue;
                                }
                            };
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
            unreadable_dirs,
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

    /// Issue #4159: an UNREADABLE keyspace directory is neither an EMPTY one nor a
    /// reason to abandon the scan.
    ///
    /// Both halves matter and each rules out a different wrong fix. Swallowing the
    /// error (the original defect: `if let Ok(table_entries) = read_dir(..)` with no
    /// `else`) makes the keyspace contribute zero tables, indistinguishable from an
    /// empty one. Propagating it (this branch's first attempt) fails the WHOLE scan
    /// over one directory, which a root-owned `lost+found` at mode 0700 — present on
    /// essentially every ext4 data volume — would trigger on the commonest real
    /// layout. The contract is: keep scanning, and RECORD what could not be read.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_keyspace_is_recorded_while_the_rest_of_the_scan_survives() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let blocked_ks = temp_dir.path().join("test_ks");
        fs::create_dir(&blocked_ks).unwrap();
        let table_dir = blocked_ks.join("users-6aa08200a25111f0a3fef1a551383fb9");
        fs::create_dir(&table_dir).unwrap();
        fs::write(table_dir.join("na-1-big-Data.db"), b"mock data").unwrap();

        // A HEALTHY sibling keyspace, so "the scan survived" is observable and not
        // merely "the scan returned Ok having found nothing".
        let ok_ks = temp_dir.path().join("other_ks");
        fs::create_dir(&ok_ks).unwrap();
        let ok_table = ok_ks.join("events-6aa08200a25111f0a3fef1a551383fb9");
        fs::create_dir(&ok_table).unwrap();
        fs::write(ok_table.join("na-1-big-Data.db"), b"mock data").unwrap();

        // Control: readable ⇒ both tables and both SSTables are discovered.
        let before = Scanner::new(temp_dir.path(), None)
            .scan()
            .expect("a readable corpus must scan");
        assert_eq!(before.tables.len(), 2, "control: both tables discovered");
        assert_eq!(before.sstable_count, 2, "control: both SSTables counted");
        assert!(
            before.unreadable_dirs.is_empty(),
            "control: a readable corpus has no gaps"
        );

        fs::set_permissions(&blocked_ks, fs::Permissions::from_mode(0o000)).unwrap();
        let staged_unreadable = fs::read_dir(&blocked_ks).is_err();
        let result = Scanner::new(temp_dir.path(), None).scan();
        // Restore before asserting so a failure cannot leave an unremovable TempDir.
        fs::set_permissions(&blocked_ks, fs::Permissions::from_mode(0o755)).unwrap();

        if !staged_unreadable {
            eprintln!(
                "SKIPPED: this process can still read a 0-mode directory (root, or \
                 CAP_DAC_OVERRIDE), so the unreadable-keyspace case cannot be staged \
                 here"
            );
            return;
        }

        let result = result.expect(
            "one unreadable keyspace must not fail the whole scan — a stock \
             root-owned lost+found would then make discovery impossible",
        );
        assert_eq!(
            result.tables,
            vec!["other_ks.events".to_string()],
            "the healthy keyspace must still be discovered"
        );
        assert_eq!(result.sstable_count, 1, "its SSTable must still be counted");
        assert_eq!(
            result.unreadable_dirs.len(),
            1,
            "the gap must be RECORDED, not swallowed: {result:?}"
        );
        let gap = &result.unreadable_dirs[0];
        assert_eq!(gap.path, blocked_ks, "the gap must name the directory");
        assert_eq!(gap.role, "keyspace");
        assert_eq!(
            gap.kind,
            std::io::ErrorKind::PermissionDenied,
            "the original io::ErrorKind must survive so PermissionDenied stays \
             distinguishable from NotFound"
        );
    }

    /// Issue #4159: an unreadable TABLE directory must not be reported as a table
    /// that exists and holds nothing.
    ///
    /// The table IS still reported — its directory demonstrably exists — because
    /// that keeps it in `table_directories`, so the `SSTableManager` opens the same
    /// directory, records its own gap and fails closed at query time. Dropping it
    /// would hide it from that check entirely. What must not happen is
    /// `sstable_count: 0` with no gap recorded, which is the claim "this table is
    /// empty".
    #[cfg(unix)]
    #[test]
    fn an_unreadable_table_directory_is_recorded_not_reported_as_zero_sstables() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();
        let table_dir = keyspace_dir.join("users-6aa08200a25111f0a3fef1a551383fb9");
        fs::create_dir(&table_dir).unwrap();
        fs::write(table_dir.join("na-1-big-Data.db"), b"mock data").unwrap();

        fs::set_permissions(&table_dir, fs::Permissions::from_mode(0o000)).unwrap();
        let staged_unreadable = fs::read_dir(&table_dir).is_err();
        let result = Scanner::new(temp_dir.path(), None).scan();
        fs::set_permissions(&table_dir, fs::Permissions::from_mode(0o755)).unwrap();

        if !staged_unreadable {
            eprintln!(
                "SKIPPED: this process can still read a 0-mode directory (root, or \
                 CAP_DAC_OVERRIDE), so the unreadable-table case cannot be staged here"
            );
            return;
        }

        let result = result.expect("one unreadable table directory must not fail the scan");
        assert_eq!(
            result.tables,
            vec!["test_ks.users".to_string()],
            "the table's directory exists, so it stays in the list the manager opens"
        );
        assert_eq!(
            result.unreadable_dirs.len(),
            1,
            "reporting sstable_count: 0 with NO gap recorded is the claim 'this \
             table is empty': {result:?}"
        );
        let gap = &result.unreadable_dirs[0];
        assert_eq!(gap.path, table_dir);
        assert_eq!(gap.role, "table");
        assert_eq!(gap.kind, std::io::ErrorKind::PermissionDenied);
    }
}
