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
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, PartitionTombstone, RangeTombstone,
    TableId,
};
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Maximum plausible WAL entry payload length (16 MiB).
///
/// A declared length larger than this signals a torn/garbage tail rather than a
/// real entry. Used both when scanning for the last valid boundary on open and
/// as the replay-time sanity bound.
const MAX_ENTRY_LENGTH: u32 = 16 * 1024 * 1024;

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

/// Legacy `CellOperation` layout (pre-Issue #921).
///
/// bincode is positional and NOT self-describing: it encodes an enum as a `u32`
/// variant index followed by that variant's fields, with no field names and no
/// length/version prefix. Issue #921 added a `local_deletion_time: Option<i32>`
/// field to [`CellOperation::Delete`], turning the on-disk shape of that variant
/// from `{ column }` into `{ column, local_deletion_time }`. A `#[serde(default)]`
/// attribute does NOT help bincode here — there are simply no bytes for the new
/// field in an older record, so decoding the new `Delete` reads the bytes of the
/// following operation as the missing `Option<i32>` and the whole record
/// misaligns.
///
/// This mirror enum reproduces the EXACT pre-#921 variant order and shapes so a
/// record written by an older binary round-trips. Only `Delete` differs (no
/// `local_deletion_time`). It MUST stay in lockstep with [`CellOperation`]:
/// variant order is the bincode discriminant, so any reordering / insertion in
/// the live enum must be reflected here or legacy decoding breaks.
#[derive(serde::Serialize, serde::Deserialize)]
enum LegacyCellOperation {
    Write {
        column: String,
        value: crate::types::Value,
    },
    WriteWithTtl {
        column: String,
        value: crate::types::Value,
        ttl_seconds: u32,
    },
    /// Pre-#921 `Delete` had no `local_deletion_time` field.
    Delete {
        column: String,
    },
    DeleteRow,
    WriteComplexElement {
        column: String,
        cell_path: Vec<u8>,
        value: Option<crate::types::Value>,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
        local_deletion_time: Option<i32>,
        is_deleted: bool,
    },
    ComplexDeletion {
        column: String,
        marked_for_delete_at: i64,
        local_deletion_time: i32,
    },
}

impl From<LegacyCellOperation> for CellOperation {
    fn from(op: LegacyCellOperation) -> Self {
        match op {
            LegacyCellOperation::Write { column, value } => CellOperation::Write { column, value },
            LegacyCellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
            } => CellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
            },
            // Pre-#921 cell tombstone: no surfaced source LDT, so the writer
            // derives it from the enclosing mutation (historical behavior).
            LegacyCellOperation::Delete { column } => CellOperation::Delete {
                column,
                local_deletion_time: None,
            },
            LegacyCellOperation::DeleteRow => CellOperation::DeleteRow,
            LegacyCellOperation::WriteComplexElement {
                column,
                cell_path,
                value,
                timestamp_micros,
                ttl_seconds,
                local_deletion_time,
                is_deleted,
            } => CellOperation::WriteComplexElement {
                column,
                cell_path,
                value,
                timestamp_micros,
                ttl_seconds,
                local_deletion_time,
                is_deleted,
            },
            LegacyCellOperation::ComplexDeletion {
                column,
                marked_for_delete_at,
                local_deletion_time,
            } => CellOperation::ComplexDeletion {
                column,
                marked_for_delete_at,
                local_deletion_time,
            },
        }
    }
}

/// Legacy on-disk `Mutation` layout (pre-Issue #764 / pre-Issue #921).
///
/// The WAL has no per-record version field; mutations are bincode-serialized
/// directly and bincode is positional, so the `local_deletion_time` field added
/// to [`Mutation`] in #764 — and the `local_deletion_time` field added to
/// [`CellOperation::Delete`] in #921 — both change the byte layout. To keep
/// recovering WAL records written by an older binary, `decode_mutation` first
/// tries the current layout and, on failure, falls back to this legacy layout
/// (which lacks the `Mutation`-level `local_deletion_time` AND uses the pre-#921
/// [`LegacyCellOperation`] for `operations`), upgrading both to the historical
/// `None` behavior. The field order here MUST mirror the historical `Mutation`
/// struct.
#[derive(serde::Serialize, serde::Deserialize)]
struct LegacyMutation {
    table: TableId,
    partition_key: PartitionKey,
    clustering_key: Option<ClusteringKey>,
    operations: Vec<LegacyCellOperation>,
    timestamp_micros: i64,
    ttl_seconds: Option<u32>,
    partition_tombstone: Option<PartitionTombstone>,
    range_tombstones: Vec<RangeTombstone>,
}

impl From<LegacyMutation> for Mutation {
    fn from(m: LegacyMutation) -> Self {
        Mutation {
            table: m.table,
            partition_key: m.partition_key,
            clustering_key: m.clustering_key,
            operations: m.operations.into_iter().map(CellOperation::from).collect(),
            timestamp_micros: m.timestamp_micros,
            ttl_seconds: m.ttl_seconds,
            partition_tombstone: m.partition_tombstone,
            range_tombstones: m.range_tombstones,
            // Pre-#764 records had no explicit local deletion time; preserving
            // None means the writer derives it from the timestamp as before.
            local_deletion_time: None,
            // Pre-#932 records had no coexisting row tombstone field.
            row_tombstone: None,
            // Pre-#1018 records had no per-cell write-timestamp side-channel.
            cell_write_timestamps: None,
        }
    }
}

/// Intermediate on-disk `Mutation` layout (post-Issue #764, pre-Issue #921).
///
/// There are three historical WAL record layouts because the WAL has no
/// per-record version field and bincode is positional:
///
/// - **(A)** pre-#764: no `Mutation`-level `local_deletion_time`, old
///   `Delete { column }` operations — see [`LegacyMutation`].
/// - **(B)** post-#764 / pre-#921 (THIS struct): the `Mutation`-level
///   `local_deletion_time: Option<i32>` trailing field is PRESENT, but the
///   operations still use the pre-#921 `Delete { column }` shape.
/// - **(C)** current: `Mutation`-level LDT present AND
///   `Delete { column, local_deletion_time }`.
///
/// Layout (B) records are NOT covered by [`LegacyMutation`] (which lacks the
/// mutation-level LDT) nor by the current [`Mutation`] (whose `Delete` carries
/// an extra `Option<i32>`), so without this struct a (B) record would fail to
/// replay and silently lose its mutation-level local deletion time.
///
/// The field order MUST mirror the current [`Mutation`] struct exactly, with
/// only `operations` swapped to [`LegacyCellOperation`].
#[derive(serde::Serialize, serde::Deserialize)]
struct LegacyMutationWithLdt {
    table: TableId,
    partition_key: PartitionKey,
    clustering_key: Option<ClusteringKey>,
    operations: Vec<LegacyCellOperation>,
    timestamp_micros: i64,
    ttl_seconds: Option<u32>,
    partition_tombstone: Option<PartitionTombstone>,
    range_tombstones: Vec<RangeTombstone>,
    /// Mutation-level local deletion time added in #764; preserved on upgrade.
    local_deletion_time: Option<i32>,
}

impl From<LegacyMutationWithLdt> for Mutation {
    fn from(m: LegacyMutationWithLdt) -> Self {
        Mutation {
            table: m.table,
            partition_key: m.partition_key,
            clustering_key: m.clustering_key,
            operations: m.operations.into_iter().map(CellOperation::from).collect(),
            timestamp_micros: m.timestamp_micros,
            ttl_seconds: m.ttl_seconds,
            partition_tombstone: m.partition_tombstone,
            range_tombstones: m.range_tombstones,
            // Preserve the mutation-level LDT that layout (B) carries; only the
            // pre-#921 cell `Delete` ops lose their (never-present) LDT to None.
            local_deletion_time: m.local_deletion_time,
            // Layout (B) predates #932; no coexisting row tombstone field.
            row_tombstone: None,
            // Layout (B) predates #1018; no per-cell write-timestamp side-channel.
            cell_write_timestamps: None,
        }
    }
}

/// On-disk `Mutation` layout post-Issue #921, pre-Issue #932 (layout (C)).
///
/// Issue #932 appended a trailing `row_tombstone: Option<(i64, i32)>` field to
/// [`Mutation`]. Because the WAL has no per-record version field and bincode is
/// positional, a record written by a #921-era binary (layout (C): current
/// `CellOperation` shapes and mutation-level LDT, but NO trailing
/// `row_tombstone`) has no bytes for the new field. Decoding it as the current
/// [`Mutation`] runs out of bytes when reading the trailing `Option`. This
/// mirror reproduces the EXACT layout-(C) field order — identical to the current
/// `Mutation` minus the new trailing field — so such records still replay,
/// upgrading `row_tombstone` to `None` (historical behavior).
///
/// The field order MUST stay in lockstep with [`Mutation`] (current
/// [`CellOperation`]), with only the trailing `row_tombstone` omitted.
#[derive(serde::Serialize, serde::Deserialize)]
struct PreRowTombstoneMutation {
    table: TableId,
    partition_key: PartitionKey,
    clustering_key: Option<ClusteringKey>,
    operations: Vec<CellOperation>,
    timestamp_micros: i64,
    ttl_seconds: Option<u32>,
    partition_tombstone: Option<PartitionTombstone>,
    range_tombstones: Vec<RangeTombstone>,
    local_deletion_time: Option<i32>,
}

impl From<PreRowTombstoneMutation> for Mutation {
    fn from(m: PreRowTombstoneMutation) -> Self {
        Mutation {
            table: m.table,
            partition_key: m.partition_key,
            clustering_key: m.clustering_key,
            operations: m.operations,
            timestamp_micros: m.timestamp_micros,
            ttl_seconds: m.ttl_seconds,
            partition_tombstone: m.partition_tombstone,
            range_tombstones: m.range_tombstones,
            local_deletion_time: m.local_deletion_time,
            // Pre-#932 records carry no coexisting row tombstone.
            row_tombstone: None,
            // Pre-#1018 records carry no per-cell write-timestamp side-channel.
            cell_write_timestamps: None,
        }
    }
}

/// On-disk `Mutation` layout post-Issue #932, pre-Issue #1018.
///
/// Issue #1018 appended a trailing `cell_write_timestamps:
/// Option<HashMap<String, i64>>` field to [`Mutation`]. Because the WAL has no
/// per-record version field and bincode is positional, a record written by a
/// #932-era binary (layout (D): current op shapes, mutation LDT AND
/// `row_tombstone`, but NO trailing `cell_write_timestamps`) has no bytes for
/// the new field, so decoding it as the current [`Mutation`] runs out of bytes.
/// This mirror reproduces the EXACT layout-(D) field order so such records still
/// replay, upgrading `cell_write_timestamps` to `None` (historical behavior:
/// every cell inherits the row timestamp).
///
/// The field order MUST stay in lockstep with [`Mutation`], with only the
/// trailing `cell_write_timestamps` omitted.
#[derive(serde::Serialize, serde::Deserialize)]
struct PreCellWriteTimestampsMutation {
    table: TableId,
    partition_key: PartitionKey,
    clustering_key: Option<ClusteringKey>,
    operations: Vec<CellOperation>,
    timestamp_micros: i64,
    ttl_seconds: Option<u32>,
    partition_tombstone: Option<PartitionTombstone>,
    range_tombstones: Vec<RangeTombstone>,
    local_deletion_time: Option<i32>,
    row_tombstone: Option<(i64, i32)>,
}

impl From<PreCellWriteTimestampsMutation> for Mutation {
    fn from(m: PreCellWriteTimestampsMutation) -> Self {
        Mutation {
            table: m.table,
            partition_key: m.partition_key,
            clustering_key: m.clustering_key,
            operations: m.operations,
            timestamp_micros: m.timestamp_micros,
            ttl_seconds: m.ttl_seconds,
            partition_tombstone: m.partition_tombstone,
            range_tombstones: m.range_tombstones,
            local_deletion_time: m.local_deletion_time,
            row_tombstone: m.row_tombstone,
            // Pre-#1018 records carry no per-cell write-timestamp side-channel.
            cell_write_timestamps: None,
        }
    }
}

/// Deserialize a `Mutation` from WAL bytes, tolerating records written by an
/// older binary that predates the `Mutation::local_deletion_time` field
/// (Issue #764) or the `CellOperation::Delete::local_deletion_time` field
/// (Issue #921).
///
/// Attempts the layouts most-recent-first: current (C), then layout (B)
/// (post-#764/pre-#921: mutation LDT present, old `Delete` op shape), then
/// layout (A) (pre-#764: no mutation LDT, old `Delete` op shape). Each maps to
/// the current [`Mutation`]/[`CellOperation`] with `Delete.local_deletion_time
/// = None`, while layout (B) additionally preserves the mutation-level LDT.
fn decode_mutation(bytes: &[u8]) -> std::result::Result<Mutation, bincode::Error> {
    match bincode::deserialize::<Mutation>(bytes) {
        Ok(mutation) => Ok(mutation),
        Err(current_err) => {
            // Layout (D): post-#932, pre-#1018 — current op shapes + mutation LDT
            // + `row_tombstone` but no trailing `cell_write_timestamps`. Tried
            // first so a #932-era record decodes correctly rather than misaligning
            // under the older mirrors below.
            if let Ok(m) = bincode::deserialize::<PreCellWriteTimestampsMutation>(bytes) {
                return Ok(Mutation::from(m));
            }
            // Layout (C): post-#921, pre-#932 — current op shapes + mutation LDT
            // but no trailing `row_tombstone`. Tried first so a #921-era record
            // with current `Delete { column, local_deletion_time }` ops decodes
            // correctly rather than misaligning under the pre-#921 mirrors below.
            if let Ok(m) = bincode::deserialize::<PreRowTombstoneMutation>(bytes) {
                return Ok(Mutation::from(m));
            }
            // Layout (B): mutation-level LDT present, pre-#921 Delete ops.
            if let Ok(m) = bincode::deserialize::<LegacyMutationWithLdt>(bytes) {
                return Ok(Mutation::from(m));
            }
            // Layout (A): pre-#764 (no mutation LDT), pre-#921 Delete ops. If
            // this also fails, surface the original (current-layout) error,
            // which is the more informative one.
            bincode::deserialize::<LegacyMutation>(bytes)
                .map(Mutation::from)
                .map_err(|_| current_err)
        }
    }
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
        // Determine the authoritative end-of-log boundary BEFORE opening for
        // append. A crash can leave a partial (torn) entry at the tail; if it is
        // retained, every subsequent append lands AFTER the garbage and is
        // silently unrecoverable on the next replay (issue #1390). The boundary
        // is derived from the length-prefixed + CRC32 framing (authoritative,
        // not a byte-pattern guess), matching the no-heuristics mandate.
        let valid_end = Self::scan_last_valid_offset(path)?;

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .map_err(|e| Error::Storage(format!("Failed to open WAL at {:?}: {}", path, e)))?;

        let metadata = file
            .metadata()
            .map_err(|e| Error::Storage(format!("Failed to read WAL metadata: {}", e)))?;

        let current_size = metadata.len();

        if valid_end < current_size {
            // Physically trim the torn tail so future appends resume exactly at
            // the last CRC-valid boundary.
            file.set_len(valid_end).map_err(|e| {
                Error::Storage(format!("Failed to trim torn WAL tail at {:?}: {}", path, e))
            })?;
            file.sync_all().map_err(|e| {
                Error::Storage(format!(
                    "Failed to sync WAL after trim at {:?}: {}",
                    path, e
                ))
            })?;
            if let Some(parent) = path.parent() {
                sync_directory(parent)?;
            }
            log::warn!(
                "WAL {:?} had a torn tail: trimmed {} byte(s) ({} -> {})",
                path,
                current_size - valid_end,
                current_size,
                valid_end
            );
        }

        Ok(Self {
            file: BufWriter::with_capacity(Self::DEFAULT_BUFFER_SIZE, file),
            path: path.to_path_buf(),
            buffer_size: Self::DEFAULT_BUFFER_SIZE,
            current_size: valid_end,
        })
    }

    /// Scan the WAL and return the byte offset at the end of the last CRC-valid
    /// entry — the boundary at which future appends must resume.
    ///
    /// Reads entries sequentially using the same length-prefixed + CRC32 framing
    /// as [`replay`](Self::replay). Scanning stops (the valid prefix ends) at the
    /// first entry that is:
    /// - missing/short in its 8-byte header (torn header),
    /// - declaring an implausible length (> [`MAX_ENTRY_LENGTH`], garbage tail),
    /// - short in its payload (torn body), or
    /// - CRC-invalid (corrupt framing — offsets past it cannot be trusted).
    ///
    /// The returned offset is therefore the end of a contiguous run of fully
    /// written, CRC-valid entries. Bytes beyond it are a partial/corrupt tail
    /// left by an interrupted append and must not be retained ahead of new
    /// appends.
    fn scan_last_valid_offset(path: &Path) -> Result<u64> {
        let mut file = File::open(path)
            .map_err(|e| Error::Storage(format!("Failed to open WAL for scan: {}", e)))?;

        let mut offset = 0u64;

        loop {
            // Read entry header: [length][crc32].
            let mut header = [0u8; 8];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(Error::Storage(format!(
                        "Failed to scan WAL header at offset {}: {}",
                        offset, e
                    )));
                }
            }

            let entry_length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

            // Implausible length => torn/garbage tail.
            if entry_length > MAX_ENTRY_LENGTH {
                break;
            }

            // Read the declared payload; a short read means a torn body.
            let mut payload = vec![0u8; entry_length as usize];
            match file.read_exact(&mut payload) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(Error::Storage(format!(
                        "Failed to scan WAL payload at offset {}: {}",
                        offset, e
                    )));
                }
            }

            // Verify CRC: a mismatch means the framing (including the length we
            // just trusted) is unreliable, so the valid prefix ends here.
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            if hasher.finalize() != expected_crc {
                break;
            }

            offset += 8 + entry_length as u64;
        }

        Ok(offset)
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
    #[tracing::instrument(name = "wal.append", skip(self, mutation))]
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
    #[tracing::instrument(name = "wal.sync", skip(self))]
    pub fn sync(&mut self) -> Result<()> {
        self.file
            .flush()
            .map_err(|e| Error::Storage(format!("Failed to flush WAL buffer: {}", e)))?;

        // Record fsync latency in seconds (issue #1036). The histogram captures
        // only the durable-write step, which dominates WAL sync cost.
        let fsync_start = std::time::Instant::now();
        self.file
            .get_ref()
            .sync_all()
            .map_err(|e| Error::Storage(format!("Failed to sync WAL to disk: {}", e)))?;
        crate::observability::record_histogram(
            crate::observability::catalog::WAL_SYNC_DURATION,
            fsync_start.elapsed().as_secs_f64(),
            &[],
        );

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
            if entry_length > MAX_ENTRY_LENGTH {
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

            // Deserialize mutation (tolerating legacy pre-#764 records).
            match decode_mutation(&mutation_bytes) {
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
            local_deletion_time: None,
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
    fn test_wal_roundtrips_explicit_local_deletion_time() {
        // Issue #764: an explicit local_deletion_time must survive a WAL
        // append + replay round-trip.
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(7));
        let ops = vec![CellOperation::DeleteRow];
        let mutation = Mutation::new(table_id, pk, None, ops, 1_700_000_000_000_000, None)
            .with_local_deletion_time(1_650_000_000);

        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(
            mutations[0].local_deletion_time,
            Some(1_650_000_000),
            "Explicit local_deletion_time must round-trip through the WAL"
        );
    }

    #[test]
    fn test_wal_default_local_deletion_time_is_none() {
        // Default (None) must round-trip unchanged, preserving historical
        // timestamp-derived behavior.
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        assert_eq!(mutation.local_deletion_time, None);
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].local_deletion_time, None);
    }

    #[test]
    fn test_wal_decodes_legacy_record_without_mutation_local_deletion_time() {
        // Issue #764: the WAL has no per-record version field. A record written
        // by an older binary (legacy Mutation layout, no Mutation-level
        // local_deletion_time) must still decode, upgrading to None.
        let legacy = LegacyMutation {
            table: TableId::new("ks", "tbl"),
            partition_key: PartitionKey::single("id", Value::Integer(3)),
            clustering_key: None,
            operations: vec![LegacyCellOperation::Delete {
                column: "name".to_string(),
            }],
            timestamp_micros: 1_234_567_890,
            ttl_seconds: None,
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        };

        // Serialize using the LEGACY layout.
        let legacy_bytes = bincode::serialize(&legacy).unwrap();

        // The fallback must decode the legacy bytes into None.
        let decoded = decode_mutation(&legacy_bytes).expect("legacy record must decode");
        assert_eq!(decoded.local_deletion_time, None);
        assert_eq!(decoded.timestamp_micros, 1_234_567_890);
        assert!(matches!(
            &decoded.operations[0],
            CellOperation::Delete {
                local_deletion_time: None,
                ..
            }
        ));
    }

    #[test]
    fn test_wal_decodes_legacy_delete_op_without_local_deletion_time() {
        // Issue #921: `CellOperation::Delete` gained a `local_deletion_time:
        // Option<i32>` field. bincode is positional and NOT self-describing, so a
        // pre-#921 record encoding `Delete { column }` (no LDT) has no bytes for
        // the new field. Decoding it with the NEW enum reads the following
        // operation's bytes as the missing Option and misaligns the record;
        // `#[serde(default)]` does NOT save bincode. The legacy fallback must
        // recover such a record as `Delete { column, local_deletion_time: None }`.
        //
        // The Delete is placed FIRST (followed by a Write) so that a naive new
        // decode genuinely misreads the trailing operation — this is the case
        // `#[serde(default)]` cannot handle and that the prior test (which used
        // only the new enum) failed to exercise.
        let legacy = LegacyMutation {
            table: TableId::new("ks", "tbl"),
            partition_key: PartitionKey::single("id", Value::Integer(7)),
            clustering_key: None,
            operations: vec![
                LegacyCellOperation::Delete {
                    column: "dropped_col".to_string(),
                },
                LegacyCellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Bob".to_string()),
                },
            ],
            timestamp_micros: 999_000,
            ttl_seconds: None,
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        };

        // Bytes as written by a pre-#921 binary.
        let legacy_bytes = bincode::serialize(&legacy).unwrap();

        // Sanity: the NEW layout adds exactly one byte for the Delete's
        // Option<i32> discriminant (None), so the legacy bytes are genuinely a
        // different, shorter shape that the current decode cannot consume cleanly.
        let current = Mutation::new(
            TableId::new("ks", "tbl"),
            PartitionKey::single("id", Value::Integer(7)),
            None,
            vec![
                CellOperation::Delete {
                    column: "dropped_col".to_string(),
                    local_deletion_time: None,
                },
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Bob".to_string()),
                },
            ],
            999_000,
            None,
        );
        let current_bytes = bincode::serialize(&current).unwrap();
        assert!(
            current_bytes.len() > legacy_bytes.len(),
            "Current Delete layout is strictly longer (adds the Option<i32> \
             discriminant for local_deletion_time): current={}, legacy={}",
            current_bytes.len(),
            legacy_bytes.len()
        );

        // The legacy fallback must recover the record with both ops intact and
        // the Delete upgraded to local_deletion_time: None.
        let decoded = decode_mutation(&legacy_bytes).expect("legacy #921 record must decode");
        assert_eq!(decoded.timestamp_micros, 999_000);
        assert_eq!(decoded.operations.len(), 2);
        match &decoded.operations[0] {
            CellOperation::Delete {
                column,
                local_deletion_time,
            } => {
                assert_eq!(column, "dropped_col");
                assert_eq!(*local_deletion_time, None);
            }
            other => panic!("expected Delete, got {other:?}"),
        }
        match &decoded.operations[1] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "name");
                assert_eq!(value, &Value::Text("Bob".to_string()));
            }
            other => panic!("expected Write, got {other:?}"),
        }
    }

    #[test]
    fn test_wal_roundtrips_delete_with_explicit_local_deletion_time() {
        // The current layout must round-trip a Delete carrying an explicit
        // per-cell local_deletion_time (the #921 compaction path).
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = Mutation::new(
            TableId::new("ks", "tbl"),
            PartitionKey::single("id", Value::Integer(11)),
            None,
            vec![CellOperation::Delete {
                column: "name".to_string(),
                local_deletion_time: Some(1_700_000_000),
            }],
            1_700_000_000_000_000,
            None,
        );
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 1);
        match &mutations[0].operations[0] {
            CellOperation::Delete {
                column,
                local_deletion_time,
            } => {
                assert_eq!(column, "name");
                assert_eq!(*local_deletion_time, Some(1_700_000_000));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn test_wal_decodes_layout_b_record_preserving_mutation_local_deletion_time() {
        // Layout (B): post-#764 / pre-#921. The mutation-level
        // local_deletion_time trailing field is PRESENT (Some(x)), but the
        // operations still use the pre-#921 `Delete { column }` shape, with the
        // Delete placed FIRST so a naive current decode misreads the trailing
        // op. Neither the current `Mutation` layout (whose Delete carries an
        // extra Option<i32>) nor `LegacyMutation` (which lacks the mutation-level
        // LDT) can decode these bytes, so the (B) compat layout is required. The
        // decoded mutation must preserve the mutation-level LDT AND recover both
        // ops with `Delete.local_deletion_time = None`.
        let legacy = LegacyMutationWithLdt {
            table: TableId::new("ks", "tbl"),
            partition_key: PartitionKey::single("id", Value::Integer(13)),
            clustering_key: None,
            operations: vec![
                LegacyCellOperation::Delete {
                    column: "dropped_col".to_string(),
                },
                LegacyCellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Carol".to_string()),
                },
            ],
            timestamp_micros: 1_650_000_000_000_000,
            ttl_seconds: None,
            partition_tombstone: None,
            range_tombstones: Vec::new(),
            local_deletion_time: Some(1_650_000_000),
        };

        // Bytes as written by a post-#764/pre-#921 binary.
        let legacy_bytes = bincode::serialize(&legacy).unwrap();

        // Sanity: the current `Mutation` layout cannot decode (B) bytes
        // correctly. The pre-#921 `Delete { column }` op (first in the list) has
        // no `local_deletion_time` byte, so a current decode either errors or
        // misaligns and reads a wrong number of operations — it can never
        // recover the two-op mutation faithfully. This is what forces the (B)
        // compat layout to run.
        let current_attempt = bincode::deserialize::<Mutation>(&legacy_bytes);
        let current_recovers_faithfully = matches!(
            &current_attempt,
            Ok(m) if m.operations.len() == 2
                && matches!(&m.operations[0], CellOperation::Delete { column, .. } if column == "dropped_col")
                && matches!(&m.operations[1], CellOperation::Write { column, value }
                    if column == "name" && value == &Value::Text("Carol".to_string()))
        );
        assert!(
            !current_recovers_faithfully,
            "current layout must NOT faithfully decode a layout (B) record; \
             otherwise the (B) compat path is never exercised"
        );

        // The (B) compat layout must recover the record: mutation-level LDT
        // preserved, both ops intact, Delete upgraded to local_deletion_time: None.
        let decoded = decode_mutation(&legacy_bytes).expect("layout (B) record must decode");
        assert_eq!(decoded.local_deletion_time, Some(1_650_000_000));
        assert_eq!(decoded.timestamp_micros, 1_650_000_000_000_000);
        assert_eq!(decoded.operations.len(), 2);
        match &decoded.operations[0] {
            CellOperation::Delete {
                column,
                local_deletion_time,
            } => {
                assert_eq!(column, "dropped_col");
                assert_eq!(*local_deletion_time, None);
            }
            other => panic!("expected Delete, got {other:?}"),
        }
        match &decoded.operations[1] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "name");
                assert_eq!(value, &Value::Text("Carol".to_string()));
            }
            other => panic!("expected Write, got {other:?}"),
        }
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

    // ---- Issue #1390: torn-tail trim on open_existing -------------------
    //
    // `open_existing` must scan the log and position/truncate to the end of the
    // last CRC-valid entry so that a partial (torn) tail left by a crash is not
    // retained AHEAD of future appends. Otherwise every post-reopen append lands
    // after the garbage and is silently unrecoverable on the next replay.

    /// Append A then B (both synced), return (path, end_of_A_offset, total_size).
    fn write_two_entries(temp_dir: &TempDir) -> (PathBuf, u64, u64) {
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        wal.append(&create_test_mutation(1, "Alice")).unwrap();
        wal.sync().unwrap();
        let end_of_a = wal.size();

        wal.append(&create_test_mutation(2, "Bob")).unwrap();
        wal.sync().unwrap();
        let total = wal.size();

        let path = wal.path().to_path_buf();
        drop(wal);
        (path, end_of_a, total)
    }

    /// Criterion 1: torn-tail trim on open. A valid + B truncated mid-payload;
    /// open_existing must truncate the file back to the end of A (the last
    /// CRC-valid boundary), leaving no torn bytes ahead of future appends.
    #[test]
    fn test_wal_open_existing_trims_torn_body() {
        let temp_dir = TempDir::new().unwrap();
        let (path, end_of_a, total) = write_two_entries(&temp_dir);

        // Cut B mid-payload: keep its 8-byte header + 2 payload bytes.
        let torn_len = end_of_a + 10;
        assert!(
            torn_len < total,
            "test setup: B must be longer than 2 payload bytes"
        );
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(torn_len).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let wal = WriteAheadLog::open_existing(&path).unwrap();

        // The torn tail must be physically trimmed to the end of A.
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            end_of_a,
            "open_existing must truncate the torn tail to the last CRC-valid boundary"
        );
        assert_eq!(wal.size(), end_of_a);

        let mutations = wal.replay().unwrap();
        assert_eq!(mutations.len(), 1, "only A survives the trim");
    }

    /// Criterion 2: post-reopen writes recoverable. Continuing criterion 1 —
    /// append C after reopen, then replay must yield exactly [A, C] with C
    /// PRESENT. This is the assertion that fails before the fix (C lands after
    /// the retained torn bytes and is lost on the next replay).
    #[test]
    fn test_wal_post_reopen_write_is_recoverable() {
        let temp_dir = TempDir::new().unwrap();
        let (path, end_of_a, total) = write_two_entries(&temp_dir);

        let torn_len = end_of_a + 10;
        assert!(torn_len < total);
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(torn_len).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Reopen (must trim the torn B), append C, sync, drop.
        {
            let mut wal = WriteAheadLog::open_existing(&path).unwrap();
            wal.append(&create_test_mutation(3, "Carol")).unwrap();
            wal.sync().unwrap();
        }

        // Fresh reopen + replay must recover exactly [A, C].
        let wal = WriteAheadLog::open_existing(&path).unwrap();
        let mutations = wal.replay().unwrap();
        assert_eq!(
            mutations.len(),
            2,
            "must recover A and C (C must be present)"
        );

        let names: Vec<&str> = mutations
            .iter()
            .map(|m| match &m.operations[0] {
                CellOperation::Write {
                    value: Value::Text(name),
                    ..
                } => name.as_str(),
                other => panic!("expected Write op, got {other:?}"),
            })
            .collect();
        assert_eq!(names, vec!["Alice", "Carol"]);
    }

    /// Criterion 3: torn-header variant. Tail cut mid-header (< 8 bytes) — same
    /// expected outcomes: trim to the end of A and remain appendable.
    #[test]
    fn test_wal_open_existing_trims_torn_header() {
        let temp_dir = TempDir::new().unwrap();
        let (path, end_of_a, _total) = write_two_entries(&temp_dir);

        // Cut mid-header: keep A + only 3 bytes of B's 8-byte header.
        let torn_len = end_of_a + 3;
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(torn_len).unwrap();
        file.sync_all().unwrap();
        drop(file);

        {
            let mut wal = WriteAheadLog::open_existing(&path).unwrap();
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                end_of_a,
                "torn header must be trimmed to the last CRC-valid boundary"
            );
            wal.append(&create_test_mutation(3, "Carol")).unwrap();
            wal.sync().unwrap();
        }

        let wal = WriteAheadLog::open_existing(&path).unwrap();
        let mutations = wal.replay().unwrap();
        assert_eq!(
            mutations.len(),
            2,
            "must recover A and C after torn-header trim"
        );
    }

    /// Criterion 4: clean log unaffected. open_existing on a clean log preserves
    /// all entries and appends normally (regression guard for the trim logic).
    #[test]
    fn test_wal_open_existing_clean_log_unaffected() {
        let temp_dir = TempDir::new().unwrap();
        let (path, _end_of_a, total) = write_two_entries(&temp_dir);

        {
            let wal = WriteAheadLog::open_existing(&path).unwrap();
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                total,
                "a clean log must not be truncated"
            );
            assert_eq!(wal.size(), total);
            let mutations = wal.replay().unwrap();
            assert_eq!(mutations.len(), 2, "clean log preserves all entries");
        }

        // Appending after a clean reopen must extend the log normally.
        {
            let mut wal = WriteAheadLog::open_existing(&path).unwrap();
            wal.append(&create_test_mutation(3, "Carol")).unwrap();
            wal.sync().unwrap();
        }
        let wal = WriteAheadLog::open_existing(&path).unwrap();
        assert_eq!(wal.replay().unwrap().len(), 3);
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
