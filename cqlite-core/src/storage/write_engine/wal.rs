//! Write-ahead log (WAL) for crash recovery
//!
//! Provides durability guarantees for mutations before they reach the memtable.
//! Every mutation is fsync'd to the WAL before being acknowledged.
//!
//! ## WAL Entry Format
//!
//! Each entry in the WAL follows this binary format:
//!
//! ```text
//! [u32 LE: entry_length] (4 bytes)
//! [u32 LE: crc32]        (4 bytes)
//! [bytes: serialized Mutation] (entry_length bytes)
//! ```
//!
//! The CRC32 checksum is computed over the serialized mutation bytes only.
//! This format allows for:
//! - Detection of corrupted entries during replay
//! - Safe truncation at partial writes (crash during append)
//! - Sequential append with minimal overhead
//!
//! ## Memory Budget
//!
//! - 4 KB buffer for sequential append (configurable)
//! - Flushes to disk on explicit sync() or buffer full
//!
//! ## Crash Recovery
//!
//! On startup, replay() reads all valid entries:
//! - Corrupted entries: logged as warnings, skipped
//! - Truncated entries: stop replay (incomplete write)
//! - Valid entries: returned in order for memtable replay

use crate::error::{Error, Result};
use crate::storage::write_engine::mutation::Mutation;
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Sync directory metadata to ensure file entries are persisted
///
/// On POSIX systems this is critical for crash safety - without syncing the
/// directory, newly created or renamed files may not appear after a crash.
///
/// Windows does not allow opening a directory as a file (ERROR_ACCESS_DENIED).
/// NTFS commits directory metadata together with the contained file's data
/// when `sync_all` is called on the file itself, so an explicit directory
/// sync is unnecessary on Windows and we skip it.
#[cfg(unix)]
fn sync_directory(dir: &Path) -> Result<()> {
    let dir_file = File::open(dir)
        .map_err(|e| Error::Storage(format!("Failed to open directory for sync: {}", e)))?;

    dir_file
        .sync_all()
        .map_err(|e| Error::Storage(format!("Failed to sync directory: {}", e)))?;

    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> Result<()> {
    Ok(())
}

/// Validate WAL directory path for security
///
/// This prevents path traversal attacks and ensures the directory is safe to use.
///
/// # Security Checks
///
/// - Directory must exist
/// - Path is canonicalized to resolve symlinks and `..' sequences
/// - Path must not contain control characters
///
/// # Arguments
///
/// * `dir` - Directory path to validate
///
/// # Errors
///
/// Returns an error if validation fails
fn validate_wal_directory(dir: &Path) -> Result<PathBuf> {
    // Check directory exists
    if !dir.exists() {
        return Err(Error::InvalidPath(format!(
            "WAL directory does not exist: {:?}",
            dir
        )));
    }

    if !dir.is_dir() {
        return Err(Error::InvalidPath(format!(
            "WAL path is not a directory: {:?}",
            dir
        )));
    }

    // Canonicalize to resolve symlinks and '..' sequences
    let canonical = dir
        .canonicalize()
        .map_err(|e| Error::InvalidPath(format!("Failed to canonicalize WAL directory: {}", e)))?;

    // Check for control characters in the path
    let path_str = canonical.to_string_lossy();
    if path_str.chars().any(|c| c.is_control()) {
        return Err(Error::InvalidPath(
            "WAL directory path contains control characters".to_string(),
        ));
    }

    Ok(canonical)
}

/// Set secure file permissions on Unix platforms
///
/// This restricts WAL file access to the owner only (0o600)
#[cfg(unix)]
fn set_secure_permissions(file: &File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut perms = file
        .metadata()
        .map_err(|e| Error::Storage(format!("Failed to read file metadata: {}", e)))?
        .permissions();

    perms.set_mode(0o600);

    file.set_permissions(perms)
        .map_err(|e| Error::Storage(format!("Failed to set file permissions: {}", e)))?;

    Ok(())
}

/// Set secure file permissions (no-op on non-Unix platforms)
#[cfg(not(unix))]
fn set_secure_permissions(_file: &File) -> Result<()> {
    // No-op on Windows - NTFS permissions are handled differently
    Ok(())
}

/// Write-ahead log for crash recovery
///
/// Provides durable storage for mutations before they reach the memtable.
/// Every mutation is serialized to an append-only log and fsync'd to disk.
///
/// ## Usage
///
/// ```no_run
/// use cqlite_core::storage::write_engine::{WriteAheadLog, Mutation};
/// use std::path::Path;
///
/// # fn example() -> cqlite_core::error::Result<()> {
/// // Create a new WAL
/// let mut wal = WriteAheadLog::create(Path::new("/data"))?;
///
/// // Append mutations (serialized with CRC32)
/// // let mutation = Mutation::new(...);
/// // wal.append(&mutation)?;
///
/// // Explicit sync to disk
/// wal.sync()?;
///
/// // On recovery, replay all valid entries
/// // let mutations = wal.replay()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct WriteAheadLog {
    /// Buffered writer for sequential appends
    file: BufWriter<File>,
    /// Path to the WAL file
    path: PathBuf,
    /// Buffer size (4KB default) - stored for diagnostic purposes
    #[allow(dead_code)]
    buffer_size: usize,
    /// Current size of the WAL file (in bytes)
    current_size: u64,
}

impl WriteAheadLog {
    /// Default buffer size (4 KB)
    pub const DEFAULT_BUFFER_SIZE: usize = 4096;

    /// WAL file name
    pub const WAL_FILENAME: &'static str = "commitlog.wal";

    /// Create a new WAL in the specified directory
    ///
    /// This creates a new WAL file with the default buffer size (4 KB).
    /// If a WAL already exists in the directory, it will be truncated.
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory where the WAL file will be created
    ///
    /// # Returns
    ///
    /// A new `WriteAheadLog` instance ready for appending.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory doesn't exist or the file cannot be created.
    pub fn create(dir: &Path) -> Result<Self> {
        Self::create_with_buffer_size(dir, Self::DEFAULT_BUFFER_SIZE)
    }

    /// Create a new WAL with a custom buffer size
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory where the WAL file will be created
    /// * `buffer_size` - Size of the append buffer in bytes
    pub fn create_with_buffer_size(dir: &Path, buffer_size: usize) -> Result<Self> {
        // Validate directory path for security
        let validated_dir = validate_wal_directory(dir)?;
        let path = validated_dir.join(Self::WAL_FILENAME);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Error::Storage(format!("Failed to create WAL at {:?}: {}", path, e)))?;

        // Set secure file permissions (Unix: 0o600)
        set_secure_permissions(&file)?;

        // Sync directory to ensure file entry is persisted
        sync_directory(&validated_dir)?;

        Ok(Self {
            file: BufWriter::with_capacity(buffer_size, file),
            path,
            buffer_size,
            current_size: 0,
        })
    }

    /// Open an existing WAL file for appending
    ///
    /// This opens an existing WAL and seeks to the end, ready for new appends.
    /// Use this for recovery scenarios where you want to append to an existing log.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the existing WAL file
    ///
    /// # Returns
    ///
    /// A `WriteAheadLog` positioned at the end of the file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file doesn't exist or cannot be opened.
    pub fn open_existing(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .map_err(|e| Error::Storage(format!("Failed to open WAL at {:?}: {}", path, e)))?;

        let metadata = file
            .metadata()
            .map_err(|e| Error::Storage(format!("Failed to read WAL metadata: {}", e)))?;

        let current_size = metadata.len();

        Ok(Self {
            file: BufWriter::with_capacity(Self::DEFAULT_BUFFER_SIZE, file),
            path: path.to_path_buf(),
            buffer_size: Self::DEFAULT_BUFFER_SIZE,
            current_size,
        })
    }

    /// Append a mutation to the WAL
    ///
    /// This serializes the mutation using bincode and writes it to the buffer.
    /// The entry is not guaranteed to be on disk until `sync()` is called.
    ///
    /// # Entry Format
    ///
    /// ```text
    /// [u32 LE: entry_length]
    /// [u32 LE: crc32]
    /// [bytes: serialized mutation]
    /// ```
    ///
    /// # Arguments
    ///
    /// * `mutation` - The mutation to append
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the write fails.
    pub fn append(&mut self, mutation: &Mutation) -> Result<()> {
        // Serialize mutation using bincode
        let mutation_bytes = bincode::serialize(mutation)
            .map_err(|e| Error::Storage(format!("Failed to serialize mutation: {}", e)))?;

        let entry_length = mutation_bytes.len() as u32;

        // Calculate CRC32 over the mutation bytes
        let mut hasher = Hasher::new();
        hasher.update(&mutation_bytes);
        let crc32 = hasher.finalize();

        // Write entry: [length][crc32][mutation_bytes]
        self.file
            .write_all(&entry_length.to_le_bytes())
            .map_err(|e| Error::Storage(format!("Failed to write entry length: {}", e)))?;

        self.file
            .write_all(&crc32.to_le_bytes())
            .map_err(|e| Error::Storage(format!("Failed to write CRC32: {}", e)))?;

        self.file
            .write_all(&mutation_bytes)
            .map_err(|e| Error::Storage(format!("Failed to write mutation bytes: {}", e)))?;

        // Update size (8 bytes header + mutation bytes)
        self.current_size += 8 + entry_length as u64;

        Ok(())
    }

    /// Sync the WAL to disk (fsync)
    ///
    /// This flushes the buffer and calls fsync to ensure all data is written
    /// to persistent storage. This is required for durability guarantees.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush or sync operation fails.
    pub fn sync(&mut self) -> Result<()> {
        self.file
            .flush()
            .map_err(|e| Error::Storage(format!("Failed to flush WAL buffer: {}", e)))?;

        self.file
            .get_ref()
            .sync_all()
            .map_err(|e| Error::Storage(format!("Failed to sync WAL to disk: {}", e)))?;

        Ok(())
    }

    /// Replay all valid entries from the WAL
    ///
    /// Reads the WAL from the beginning and deserializes all valid entries.
    /// This is used during crash recovery to rebuild the memtable.
    ///
    /// ## Corruption Handling
    ///
    /// - **Corrupted entries** (CRC mismatch): Logged as warnings, skipped
    /// - **Truncated entries** (incomplete write): Stops replay, returns valid entries
    /// - **Valid entries**: Deserialized and returned in order
    ///
    /// # Returns
    ///
    /// A vector of all valid mutations read from the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be opened or read.
    pub fn replay(&self) -> Result<Vec<Mutation>> {
        let mut file = File::open(&self.path)
            .map_err(|e| Error::Storage(format!("Failed to open WAL for replay: {}", e)))?;

        let mut mutations = Vec::new();
        let mut offset = 0u64;

        loop {
            // Read entry header: [length][crc32]
            let mut header = [0u8; 8];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file or truncated header - stop replay
                    break;
                }
                Err(e) => {
                    return Err(Error::Storage(format!(
                        "Failed to read WAL header at offset {}: {}",
                        offset, e
                    )));
                }
            }

            let entry_length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

            // Sanity check: entry length should be reasonable (<16MB)
            if entry_length > 16 * 1024 * 1024 {
                log::warn!(
                    "WAL entry at offset {} has unreasonable length {} - stopping replay",
                    offset,
                    entry_length
                );
                break;
            }

            // Read mutation bytes
            let mut mutation_bytes = vec![0u8; entry_length as usize];
            match file.read_exact(&mut mutation_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Truncated entry - stop replay
                    log::warn!(
                        "WAL entry at offset {} is truncated (expected {} bytes) - stopping replay",
                        offset,
                        entry_length
                    );
                    break;
                }
                Err(e) => {
                    return Err(Error::Storage(format!(
                        "Failed to read WAL entry at offset {}: {}",
                        offset, e
                    )));
                }
            }

            // Verify CRC32
            let mut hasher = Hasher::new();
            hasher.update(&mutation_bytes);
            let actual_crc = hasher.finalize();

            if actual_crc != expected_crc {
                log::warn!(
                    "WAL entry at offset {} has CRC mismatch (expected 0x{:08x}, got 0x{:08x}) - skipping",
                    offset,
                    expected_crc,
                    actual_crc
                );
                offset += 8 + entry_length as u64;
                continue;
            }

            // Deserialize mutation
            match bincode::deserialize::<Mutation>(&mutation_bytes) {
                Ok(mutation) => {
                    mutations.push(mutation);
                }
                Err(e) => {
                    log::warn!(
                        "WAL entry at offset {} failed to deserialize: {} - skipping",
                        offset,
                        e
                    );
                }
            }

            offset += 8 + entry_length as u64;
        }

        Ok(mutations)
    }

    /// Truncate the WAL (clear all entries)
    ///
    /// This is used after a successful flush to memtable/SSTable, removing
    /// old entries that are no longer needed for recovery.
    ///
    /// # Errors
    ///
    /// Returns an error if the truncate operation fails.
    pub fn truncate(&mut self) -> Result<()> {
        // Flush any pending writes first
        self.file
            .flush()
            .map_err(|e| Error::Storage(format!("Failed to flush before truncate: {}", e)))?;

        // Truncate the file to zero length
        self.file
            .get_mut()
            .set_len(0)
            .map_err(|e| Error::Storage(format!("Failed to truncate WAL: {}", e)))?;

        // Fsync after truncate to ensure operation is persisted
        self.file
            .get_ref()
            .sync_all()
            .map_err(|e| Error::Storage(format!("Failed to sync after truncate: {}", e)))?;

        // Seek to beginning
        self.file
            .get_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|e| Error::Storage(format!("Failed to seek after truncate: {}", e)))?;

        self.current_size = 0;

        Ok(())
    }

    /// Get the current size of the WAL in bytes
    pub fn size(&self) -> u64 {
        self.current_size
    }

    /// Get the path to the WAL file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Rotate the WAL (create a new one, keeping the old)
    ///
    /// This creates a new WAL file with a timestamp suffix and returns a new
    /// `WriteAheadLog` instance. The old WAL file is left intact for archival
    /// or backup purposes.
    ///
    /// The old file is renamed to: `commitlog.wal.{timestamp}`
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory where the new WAL will be created
    ///
    /// # Returns
    ///
    /// A new `WriteAheadLog` instance ready for appending.
    ///
    /// # Errors
    ///
    /// Returns an error if the rotation fails.
    pub fn rotate(mut self, dir: &Path) -> Result<Self> {
        // Flush and sync the current WAL
        self.sync()?;

        // Generate timestamp suffix
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let old_path = self.path.clone();
        let archived_path = dir.join(format!("commitlog.wal.{}", timestamp));

        // Drop the writer to close the file
        drop(self.file);

        // Rename the old WAL
        std::fs::rename(&old_path, &archived_path)
            .map_err(|e| Error::Storage(format!("Failed to rename WAL during rotation: {}", e)))?;

        // Sync directory to ensure rename is persisted
        sync_directory(dir)?;

        // Create a new WAL
        Self::create(dir)
    }

    /// Delete an old WAL file
    ///
    /// This is used to clean up archived WAL files after a successful flush
    /// or when they are no longer needed for recovery.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the WAL file to delete
    ///
    /// # Errors
    ///
    /// Returns an error if the delete operation fails.
    pub fn delete_old(path: &Path) -> Result<()> {
        std::fs::remove_file(path)
            .map_err(|e| Error::Storage(format!("Failed to delete old WAL: {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
    };
    use crate::types::Value;
    use tempfile::TempDir;

    fn create_test_mutation(id: i32, name: &str) -> Mutation {
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(id));
        let ops = vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }];

        Mutation::new(table_id, pk, None, ops, 1234567890, None)
    }

    #[test]
    fn test_wal_create() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        assert_eq!(wal.size(), 0);
        assert!(wal.path().exists());
    }

    #[test]
    fn test_wal_append_and_sync() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        wal.append(&mutation).unwrap();

        assert!(wal.size() > 0);

        wal.sync().unwrap();
    }

    #[test]
    fn test_wal_replay_empty() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 0);
    }

    #[test]
    fn test_wal_replay_single_entry() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].table.keyspace, "test_ks");
        assert_eq!(mutations[0].table.table, "test_table");
    }

    #[test]
    fn test_wal_replay_multiple_entries() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        for i in 0..10 {
            let mutation = create_test_mutation(i, &format!("User{}", i));
            wal.append(&mutation).unwrap();
        }
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 10);

        for (i, mutation) in mutations.iter().enumerate() {
            assert_eq!(mutation.table.keyspace, "test_ks");
            match &mutation.operations[0] {
                CellOperation::Write { column, value } => {
                    assert_eq!(column, "name");
                    if let Value::Text(name) = value {
                        assert_eq!(name, &format!("User{}", i));
                    } else {
                        panic!("Expected Text value");
                    }
                }
                _ => panic!("Expected Write operation"),
            }
        }
    }

    #[test]
    fn test_wal_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        assert!(wal.size() > 0);

        wal.truncate().unwrap();
        assert_eq!(wal.size(), 0);

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 0);
    }

    #[test]
    fn test_wal_crc_corruption() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        // Corrupt the CRC32 field (bytes 4-7)
        let wal_path = wal.path().to_path_buf();
        drop(wal);

        let mut file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.seek(SeekFrom::Start(4)).unwrap();
        file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Replay should skip the corrupted entry
        let wal = WriteAheadLog::open_existing(&wal_path).unwrap();
        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 0);
    }

    #[test]
    fn test_wal_truncated_entry() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let wal_path = wal.path().to_path_buf();
        let original_size = wal.size();
        drop(wal);

        // Truncate the file to simulate incomplete write
        let file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.set_len(original_size - 10).unwrap();
        drop(file);

        // Replay should stop at truncated entry
        let wal = WriteAheadLog::open_existing(&wal_path).unwrap();
        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 0);
    }

    #[test]
    fn test_wal_rotate() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        // Rotate the WAL
        let wal = wal.rotate(temp_dir.path()).unwrap();

        // New WAL should be empty
        assert_eq!(wal.size(), 0);

        // Old WAL should be archived
        let archived_files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("commitlog.wal.")
            })
            .collect();

        assert_eq!(archived_files.len(), 1);
    }

    #[test]
    fn test_wal_delete_old() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");

        // Create a dummy WAL file
        File::create(&wal_path).unwrap();
        assert!(wal_path.exists());

        // Delete it
        WriteAheadLog::delete_old(&wal_path).unwrap();
        assert!(!wal_path.exists());
    }

    #[test]
    fn test_wal_open_existing() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation1 = create_test_mutation(1, "Alice");
        wal.append(&mutation1).unwrap();
        wal.sync().unwrap();

        let wal_path = wal.path().to_path_buf();
        drop(wal);

        // Reopen the WAL
        let mut wal = WriteAheadLog::open_existing(&wal_path).unwrap();

        // Append another entry
        let mutation2 = create_test_mutation(2, "Bob");
        wal.append(&mutation2).unwrap();
        wal.sync().unwrap();

        // Replay should get both entries
        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 2);
    }

    #[test]
    fn test_wal_with_clustering_key() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ck = Some(ClusteringKey::single("ts", Value::Timestamp(1000)));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text("test".to_string()),
        }];

        let mutation = Mutation::new(table_id, pk, ck, ops, 1234567890, None);
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(mutations[0].clustering_key.is_some());
    }

    #[test]
    fn test_wal_with_ttl() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text("test".to_string()),
        }];

        let mutation = Mutation::new(table_id, pk, None, ops, 1234567890, Some(3600));
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].ttl_seconds, Some(3600));
    }

    #[test]
    fn test_wal_delete_operation() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::Delete {
            column: "name".to_string(),
        }];

        let mutation = Mutation::new(table_id, pk, None, ops, 1234567890, None);
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(matches!(
            &mutations[0].operations[0],
            CellOperation::Delete { .. }
        ));
    }

    #[test]
    fn test_wal_delete_row_operation() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::DeleteRow];

        let mutation = Mutation::new(table_id, pk, None, ops, 1234567890, None);
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(matches!(
            &mutations[0].operations[0],
            CellOperation::DeleteRow
        ));
    }

    #[test]
    fn test_wal_buffer_size() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::create_with_buffer_size(temp_dir.path(), 8192).unwrap();

        assert_eq!(wal.buffer_size, 8192);
    }

    #[test]
    fn test_wal_directory_sync_on_create() {
        // Test that directory is synced after WAL creation
        let temp_dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        // Verify WAL file exists
        assert!(wal.path().exists());

        // The sync operation should have completed without error
        // (we can't directly test that fsync was called, but we verify no error)
    }

    #[test]
    fn test_wal_directory_sync_on_rotate() {
        // Test that directory is synced after WAL rotation
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        // Rotate WAL
        let new_wal = wal.rotate(temp_dir.path()).unwrap();

        // Verify new WAL exists
        assert!(new_wal.path().exists());

        // Verify archived WAL exists
        let archived_files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("commitlog.wal.")
            })
            .collect();

        assert_eq!(archived_files.len(), 1);
    }

    #[test]
    fn test_wal_fsync_after_truncate() {
        // Test that fsync is called after truncate
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let size_before = wal.size();
        assert!(size_before > 0);

        // Truncate should sync to disk
        wal.truncate().unwrap();

        assert_eq!(wal.size(), 0);

        // Verify file is actually empty
        let metadata = std::fs::metadata(wal.path()).unwrap();
        assert_eq!(metadata.len(), 0);
    }

    #[test]
    fn test_validate_wal_directory_nonexistent() {
        // Test that validation fails for non-existent directory
        let nonexistent = PathBuf::from("/nonexistent/path/that/does/not/exist");
        let result = validate_wal_directory(&nonexistent);

        assert!(result.is_err());
        match result {
            Err(Error::InvalidPath(_)) => {}
            _ => panic!("Expected InvalidPath error"),
        }
    }

    #[test]
    fn test_validate_wal_directory_is_file() {
        // Test that validation fails when path is a file, not a directory
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("not_a_dir");
        File::create(&file_path).unwrap();

        let result = validate_wal_directory(&file_path);

        assert!(result.is_err());
        match result {
            Err(Error::InvalidPath(_)) => {}
            _ => panic!("Expected InvalidPath error"),
        }
    }

    #[test]
    fn test_validate_wal_directory_valid() {
        // Test that validation succeeds for valid directory
        let temp_dir = TempDir::new().unwrap();
        let result = validate_wal_directory(temp_dir.path());

        assert!(result.is_ok());
        let canonical = result.unwrap();
        assert!(canonical.is_absolute());
    }

    #[test]
    #[cfg(unix)]
    fn test_wal_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        // Test that WAL files have secure permissions (0o600) on Unix
        let temp_dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let metadata = std::fs::metadata(wal.path()).unwrap();
        let permissions = metadata.permissions();
        let mode = permissions.mode();

        // Check that permissions are 0o600 (owner read/write only)
        // Mask with 0o777 to get only permission bits
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn test_wal_create_validates_directory() {
        // Test that WAL creation validates the directory path
        let temp_dir = TempDir::new().unwrap();

        // This should succeed because temp_dir exists
        let result = WriteAheadLog::create(temp_dir.path());
        assert!(result.is_ok());

        // This should fail because the directory doesn't exist
        let nonexistent = temp_dir.path().join("nonexistent");
        let result = WriteAheadLog::create(&nonexistent);
        assert!(result.is_err());
    }

    #[test]
    fn test_sync_directory_invalid_path() {
        // Test that sync_directory fails for invalid paths
        let invalid_path = PathBuf::from("/nonexistent/path");
        let result = sync_directory(&invalid_path);

        assert!(result.is_err());
        match result {
            Err(Error::Storage(_)) => {}
            _ => panic!("Expected Storage error"),
        }
    }
}
