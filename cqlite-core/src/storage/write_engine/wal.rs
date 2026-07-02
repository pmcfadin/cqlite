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

/// Outcome of a WAL crash-recovery replay (issue #1391).
///
/// Replaying a WAL is not always lossless: a crash can leave a torn tail, and
/// on-disk bit-rot can corrupt an entry in the MIDDLE of the log. The previous
/// `replay()` returned a bare `Ok(Vec<Mutation>)` in every case, so a corrupt
/// recovery was indistinguishable from a clean one — and the next flush then
/// truncated the WAL, making the loss permanent and invisible.
///
/// This report makes lossiness explicit. Callers MUST consult
/// [`is_clean`](Self::is_clean) before treating the recovery as complete;
/// [`WriteEngine`](super::WriteEngine) preserves the raw WAL segment aside and
/// surfaces the report rather than silently truncating over corruption.
///
/// # Recovery posture (fail-fast, then report)
///
/// The WAL framing has no sync markers, so once an entry's CRC does not verify
/// (or its declared length is implausible) the byte offset of the *next* entry
/// cannot be recovered authoritatively — trusting the corrupt length is exactly
/// the misalignment bug that decoded arbitrary garbage. Replay therefore matches
/// Cassandra's `CommitLogReplayer` default: it recovers the valid *prefix*, then
/// **stops at the first unrecoverable corruption** ([`stopped_early`]) rather
/// than guessing a resync point. The only exception is a CRC-*valid* entry whose
/// payload fails to deserialize: the CRC guarantees the entry boundary, so that
/// single entry is skipped (counted in [`corrupt_entries`]) and replay continues
/// — an authoritative, non-heuristic resync.
///
/// [`stopped_early`]: Self::stopped_early
/// [`corrupt_entries`]: Self::corrupt_entries
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    /// Mutations recovered, in log order. On corruption this is the valid
    /// prefix that precedes the first unrecoverable entry.
    pub mutations: Vec<Mutation>,
    /// Number of entries that could not be recovered (CRC mismatch, implausible
    /// length, or CRC-valid-but-undecodable payload).
    pub corrupt_entries: usize,
    /// True when replay stopped before end-of-file because it hit corruption it
    /// could not authoritatively resync past. Any entries after the stop point
    /// are NOT in [`mutations`](Self::mutations) and are covered by
    /// [`bytes_skipped`](Self::bytes_skipped).
    pub stopped_early: bool,
    /// Number of trailing bytes that were present on disk but not recovered
    /// (the corrupt entry plus everything after it when `stopped_early`).
    pub bytes_skipped: u64,
}

impl RecoveryReport {
    /// A recovery is clean iff no entry was corrupt and replay reached EOF
    /// without stopping early. A torn tail (an incomplete final append that was
    /// never acknowledged) is NOT corruption and keeps a report clean.
    pub fn is_clean(&self) -> bool {
        self.corrupt_entries == 0 && !self.stopped_early
    }
}

/// Why a sequential WAL scan stopped advancing (issue #1390 + #1391).
///
/// Distinguishing a torn tail from mid-stream corruption is the crux of safe
/// recovery: a torn tail is an interrupted final append (safe to trim), whereas
/// a CRC mismatch / implausible length on a fully-present entry is *unexpected
/// corruption* whose successors must NOT be silently trimmed away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalStop {
    /// Reached end-of-file exactly on an entry boundary — a fully clean log.
    CleanEof,
    /// The trailing entry was short (header or payload cut off): an interrupted
    /// append that was never acknowledged. Safe to trim.
    TornTail,
    /// A fully-present entry failed CRC or declared an implausible length. The
    /// framing past this point is untrustworthy; successors must be preserved,
    /// not silently discarded.
    Corruption,
}

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
pub(crate) fn sync_directory(dir: &Path) -> Result<()> {
    let dir_file = File::open(dir)
        .map_err(|e| Error::Storage(format!("Failed to open directory for sync: {}", e)))?;

    dir_file
        .sync_all()
        .map_err(|e| Error::Storage(format!("Failed to sync directory: {}", e)))?;

    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn sync_directory(_dir: &Path) -> Result<()> {
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
    /// Byte offset of the last CRC-valid prefix when `open_existing` detected
    /// mid-stream corruption (issue #1391). The corrupt tail is intentionally
    /// left on disk so the caller can preserve it aside as forensic evidence
    /// FIRST; once that is done the caller invokes
    /// [`reset_to_valid_prefix`](Self::reset_to_valid_prefix) so post-recovery
    /// appends land at a replayable position instead of after the corrupt bytes
    /// (where a synced write would be lost on the next replay). `None` for a
    /// freshly created WAL, a clean reopen, or a torn tail (already trimmed).
    pending_valid_prefix: Option<u64>,
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
            pending_valid_prefix: None,
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
        let (valid_end, stop) = Self::scan_valid_prefix(path)?;

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .map_err(|e| Error::Storage(format!("Failed to open WAL at {:?}: {}", path, e)))?;

        let metadata = file
            .metadata()
            .map_err(|e| Error::Storage(format!("Failed to read WAL metadata: {}", e)))?;

        let file_len = metadata.len();

        // Only a TORN TAIL (an interrupted, never-acknowledged final append) may
        // be trimmed HERE — that is #1390's guarantee that future appends resume
        // at the last valid boundary. A CRC mismatch / implausible length on a
        // fully-present entry (`Corruption`) may have VALID successors, so
        // silently `set_len`-ing them away at open time would discard evidence
        // before it can be preserved. We therefore leave a corrupt log
        // physically intact so `replay()` surfaces the loss and `WriteEngine`
        // can copy the raw segment aside; the caller then invokes
        // `reset_to_valid_prefix` (issue #1391) to trim the LIVE log back to the
        // valid prefix BEFORE accepting writes, so a post-recovery synced append
        // lands at a replayable position rather than after the corrupt bytes.
        let mut pending_valid_prefix = None;
        let current_size = if stop == WalStop::TornTail && valid_end < file_len {
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
                file_len - valid_end,
                file_len,
                valid_end
            );
            valid_end
        } else {
            if stop == WalStop::Corruption && valid_end < file_len {
                // Do not paper over corruption HERE: leave the corrupt segment on
                // disk so the caller can preserve it aside first, and record the
                // valid-prefix boundary so `reset_to_valid_prefix` can trim the
                // live log once the evidence is safe.
                log::error!(
                    "WAL {:?} has corruption at offset {} ({} valid prefix byte(s) of {}); \
                     leaving the segment intact for evidence preservation before reset",
                    path,
                    valid_end,
                    valid_end,
                    file_len
                );
                pending_valid_prefix = Some(valid_end);
            }
            file_len
        };

        Ok(Self {
            file: BufWriter::with_capacity(Self::DEFAULT_BUFFER_SIZE, file),
            path: path.to_path_buf(),
            buffer_size: Self::DEFAULT_BUFFER_SIZE,
            current_size,
            pending_valid_prefix,
        })
    }

    /// True if `open_existing` found mid-stream corruption whose valid prefix has
    /// not yet been reset (issue #1391). While this holds, the LIVE WAL still
    /// carries the corrupt tail and appends would land after it (lost on the next
    /// replay) — the caller must preserve the segment aside and then call
    /// [`reset_to_valid_prefix`](Self::reset_to_valid_prefix) before any write.
    pub fn has_pending_corrupt_tail(&self) -> bool {
        self.pending_valid_prefix.is_some()
    }

    /// After a lossy (`Corruption`) recovery, trim the LIVE WAL back to its last
    /// CRC-valid prefix so post-recovery appends resume at a replayable position
    /// (issue #1391). This is a no-op unless `open_existing` recorded a pending
    /// corrupt tail; the caller MUST first preserve the raw segment aside (the
    /// trim is destructive to the on-disk corrupt bytes, though the aside copy
    /// and the retained `RecoveryReport` survive).
    ///
    /// Returns the new (valid-prefix) length when a trim occurred, or `None` when
    /// there was nothing pending.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer flush, `set_len`, or fsync fails.
    pub fn reset_to_valid_prefix(&mut self) -> Result<Option<u64>> {
        // Peek the pending value WITHOUT clearing it (issue #1391, roborev r3): if
        // any step below (flush / set_len / fsync / directory sync) fails, the
        // corrupt tail is still (partly) on disk and the guard MUST stay set so
        // `append()` remains fail-closed. Clearing here — as `.take()` did — left
        // the WAL appendable after the corrupt tail again on any mid-reset error,
        // reintroducing the acknowledged-write-loss window. The guard is cleared
        // only after ALL steps succeed (see the end of this function).
        let valid_end = match self.pending_valid_prefix.as_ref() {
            Some(end) => *end,
            None => return Ok(None),
        };

        // Flush any buffered bytes first (there should be none — nothing has been
        // appended since open — but do not rely on that).
        self.file
            .flush()
            .map_err(|e| Error::Storage(format!("Failed to flush WAL before reset: {}", e)))?;

        let file_len = self
            .file
            .get_ref()
            .metadata()
            .map_err(|e| Error::Storage(format!("Failed to read WAL metadata for reset: {}", e)))?
            .len();

        if valid_end >= file_len {
            // Nothing beyond the valid prefix to trim (e.g. already reset). This is
            // a benign success, so it is safe to lift the guard.
            self.current_size = file_len;
            self.pending_valid_prefix = None;
            return Ok(None);
        }

        self.file.get_mut().set_len(valid_end).map_err(|e| {
            Error::Storage(format!(
                "Failed to reset WAL to valid prefix at {:?}: {}",
                self.path, e
            ))
        })?;
        self.file.get_ref().sync_all().map_err(|e| {
            Error::Storage(format!(
                "Failed to sync WAL after reset at {:?}: {}",
                self.path, e
            ))
        })?;
        if let Some(parent) = self.path.parent() {
            sync_directory(parent)?;
        }

        log::warn!(
            "WAL {:?} reset to last valid prefix after lossy recovery: {} -> {} \
             ({} corrupt byte(s) dropped from the LIVE log; evidence preserved aside)",
            self.path,
            file_len,
            valid_end,
            file_len - valid_end
        );

        self.current_size = valid_end;
        // All steps succeeded — only now is it safe to lift the fail-closed guard.
        self.pending_valid_prefix = None;
        Ok(Some(valid_end))
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
    fn scan_valid_prefix(path: &Path) -> Result<(u64, WalStop)> {
        let mut file = File::open(path)
            .map_err(|e| Error::Storage(format!("Failed to open WAL for scan: {}", e)))?;

        let mut offset = 0u64;

        loop {
            // Read entry header: [length][crc32].
            let mut header = [0u8; 8];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                // Clean boundary vs a header cut short: a full-length read that
                // hit EOF exactly on the entry boundary (`file len == offset`) is
                // a clean end-of-log; any surplus bytes are a torn header. This
                // holds at `offset == 0` too: a WAL that is JUST a 1-7 byte torn
                // first header (nothing valid before it) is a torn tail to be
                // trimmed, NOT a clean EOF — an `offset == 0` short-circuit here
                // would leave those garbage bytes in place and every later append
                // would land after them, unrecoverable on the next replay
                // (issue #1391).
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    let stop = if Self::at_clean_eof(&mut file, offset)? {
                        WalStop::CleanEof
                    } else {
                        WalStop::TornTail
                    };
                    return Ok((offset, stop));
                }
                Err(e) => {
                    return Err(Error::Storage(format!(
                        "Failed to scan WAL header at offset {}: {}",
                        offset, e
                    )));
                }
            }

            let entry_length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

            // Implausible length on a fully-present header => corruption, not a
            // torn tail: the framing is untrustworthy and any successors must be
            // preserved rather than discarded.
            if entry_length > MAX_ENTRY_LENGTH {
                return Ok((offset, WalStop::Corruption));
            }

            // Read the declared payload; a short read means a torn body (the
            // final append was interrupted).
            let mut payload = vec![0u8; entry_length as usize];
            match file.read_exact(&mut payload) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok((offset, WalStop::TornTail));
                }
                Err(e) => {
                    return Err(Error::Storage(format!(
                        "Failed to scan WAL payload at offset {}: {}",
                        offset, e
                    )));
                }
            }

            // Verify CRC. A mismatch on a fully-present entry means the framing
            // (including the length we just trusted) is unreliable, so this is
            // corruption — offsets past it cannot be resynced authoritatively.
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            if hasher.finalize() != expected_crc {
                return Ok((offset, WalStop::Corruption));
            }

            offset += 8 + entry_length as u64;
        }
    }

    /// Distinguish a clean end-of-log from a torn header after a short header
    /// read: seek to `offset` and check that exactly zero bytes remain.
    fn at_clean_eof(file: &mut File, offset: u64) -> Result<bool> {
        let len = file
            .metadata()
            .map_err(|e| Error::Storage(format!("Failed to read WAL metadata during scan: {}", e)))?
            .len();
        Ok(len == offset)
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
        // Refuse to append while a mid-stream corrupt tail is still on disk
        // (issue #1391). `open_existing` deliberately leaves the corrupt segment
        // intact for evidence preservation and records the valid-prefix boundary
        // in `pending_valid_prefix`. If a direct public-API consumer appended now
        // (without the `WriteEngine::new` reset), the new entry would land AFTER
        // the corrupt tail — exactly where the next `replay()` stops — so the
        // acknowledged write would be silently lost. The caller must preserve the
        // segment aside and call `reset_to_valid_prefix()` first (which clears
        // this flag). `WriteEngine::new` already does this before any write.
        if self.pending_valid_prefix.is_some() {
            return Err(Error::Storage(
                "WAL has an unreset corrupt tail; reset_to_valid_prefix required before appending"
                    .to_string(),
            ));
        }

        // Serialize mutation using bincode
        let mutation_bytes = bincode::serialize(mutation)
            .map_err(|e| Error::Storage(format!("Failed to serialize mutation: {}", e)))?;

        // Fail-closed size ceiling (issue #1391, roborev r3). `replay()` /
        // `scan_valid_prefix` classify any entry whose declared length exceeds
        // `MAX_ENTRY_LENGTH` as corruption and STOP. If `append()` accepted a
        // larger entry, a >16 MiB write could be fsync-acknowledged here and then
        // silently dropped as "corrupt" on the next recovery — acknowledged-write
        // loss. The write path's accepted max MUST equal the replay/scan limit, so
        // reject BEFORE writing anything. The comparison is done in `u64` so a
        // multi-GiB length cannot truncate through the `as u32` cast below into a
        // small, wrongly-accepted value.
        if mutation_bytes.len() as u64 > MAX_ENTRY_LENGTH as u64 {
            return Err(Error::Storage(format!(
                "WAL entry exceeds MAX_ENTRY_LENGTH (16 MiB): {} bytes",
                mutation_bytes.len()
            )));
        }

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
    /// ## Corruption Handling (issue #1391)
    ///
    /// Recovery is fail-fast-then-report — see [`RecoveryReport`] for the full
    /// rationale. In summary:
    ///
    /// - **CRC mismatch / implausible length** on a fully-present entry: the
    ///   framing past this point cannot be resynced authoritatively (no sync
    ///   markers), so replay STOPS ([`RecoveryReport::stopped_early`]) and
    ///   records the loss. It does NOT advance by the untrusted length (that was
    ///   the misalignment bug that decoded garbage).
    /// - **CRC-valid but undecodable payload**: the entry boundary is
    ///   trustworthy, so this single entry is skipped
    ///   ([`RecoveryReport::corrupt_entries`]) and replay continues.
    /// - **Torn tail** (short header/payload at EOF): an interrupted final
    ///   append that was never acknowledged — replay stops cleanly (not counted
    ///   as corruption).
    /// - **Valid entries**: deserialized and returned in order.
    ///
    /// # Returns
    ///
    /// A [`RecoveryReport`]. Callers MUST check [`RecoveryReport::is_clean`]
    /// before treating recovery as complete; a non-clean report signals data
    /// loss that must be surfaced, not silently truncated over.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be opened or read (an I/O fault
    /// distinct from in-band corruption, which is reported, not errored).
    pub fn replay(&self) -> Result<RecoveryReport> {
        let mut file = File::open(&self.path)
            .map_err(|e| Error::Storage(format!("Failed to open WAL for replay: {}", e)))?;
        let total_len = file
            .metadata()
            .map_err(|e| Error::Storage(format!("Failed to read WAL metadata for replay: {}", e)))?
            .len();

        let mut report = RecoveryReport::default();
        let mut offset = 0u64;

        loop {
            // Read entry header: [length][crc32]
            let mut header = [0u8; 8];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Clean EOF or a torn (never-acknowledged) final append -
                    // stop replay without flagging corruption.
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

            // Implausible length: cannot trust the framing to find the next
            // entry, so fail fast and report rather than skipping by a bogus
            // length into arbitrary bytes.
            if entry_length > MAX_ENTRY_LENGTH {
                log::error!(
                    "WAL entry at offset {} declares implausible length {} (> {}) - stopping \
                     replay; {} trailing byte(s) not recovered",
                    offset,
                    entry_length,
                    MAX_ENTRY_LENGTH,
                    total_len.saturating_sub(offset)
                );
                report.corrupt_entries += 1;
                report.stopped_early = true;
                break;
            }

            // Read mutation bytes
            let mut mutation_bytes = vec![0u8; entry_length as usize];
            match file.read_exact(&mut mutation_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Torn payload at the tail - interrupted final append, not
                    // corruption. Stop cleanly.
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
                // CRC mismatch on a fully-present entry: the length we just
                // trusted is unreliable, so the offset of the next entry is
                // unknown. Fail fast and report; do NOT advance-and-continue.
                log::error!(
                    "WAL entry at offset {} has CRC mismatch (expected 0x{:08x}, got 0x{:08x}) - \
                     stopping replay; {} trailing byte(s) not recovered",
                    offset,
                    expected_crc,
                    actual_crc,
                    total_len.saturating_sub(offset)
                );
                report.corrupt_entries += 1;
                report.stopped_early = true;
                break;
            }

            // CRC valid => the entry boundary is authoritative. Deserialize
            // (tolerating legacy pre-#764 records). A decode failure here is a
            // format skew our compat layers do not cover; because the boundary
            // is trustworthy we skip just this entry and continue.
            match decode_mutation(&mutation_bytes) {
                Ok(mutation) => {
                    report.mutations.push(mutation);
                }
                Err(e) => {
                    log::error!(
                        "WAL entry at offset {} passed CRC but failed to deserialize: {} - \
                         skipping this entry and continuing",
                        offset,
                        e
                    );
                    report.corrupt_entries += 1;
                }
            }

            offset += 8 + entry_length as u64;
        }

        report.bytes_skipped = total_len.saturating_sub(offset);
        Ok(report)
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

        let mutations = wal.replay().unwrap().mutations;
        assert_eq!(mutations.len(), 0);
    }

    #[test]
    fn test_wal_replay_single_entry() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mutation = create_test_mutation(1, "Alice");
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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

        // The corrupt (fully-present) entry must be reported, not silently
        // dropped: no mutation is recovered, and the report is not clean.
        let wal = WriteAheadLog::open_existing(&wal_path).unwrap();
        let report = wal.replay().unwrap();
        assert_eq!(report.mutations.len(), 0);
        assert!(
            !report.is_clean(),
            "CRC corruption must surface as non-clean"
        );
        assert_eq!(report.corrupt_entries, 1);
        assert!(report.stopped_early);
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
        let mutations = wal.replay().unwrap().mutations;
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
        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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

        let mutations = wal.replay().unwrap().mutations;
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
        let mutations = wal.replay().unwrap().mutations;
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
        let mutations = wal.replay().unwrap().mutations;
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
            let mutations = wal.replay().unwrap().mutations;
            assert_eq!(mutations.len(), 2, "clean log preserves all entries");
        }

        // Appending after a clean reopen must extend the log normally.
        {
            let mut wal = WriteAheadLog::open_existing(&path).unwrap();
            wal.append(&create_test_mutation(3, "Carol")).unwrap();
            wal.sync().unwrap();
        }
        let wal = WriteAheadLog::open_existing(&path).unwrap();
        assert_eq!(wal.replay().unwrap().mutations.len(), 3);
    }

    /// Regression (#1390 roborev finding, superseded by #1391): a COMPLETE but
    /// CRC-corrupt entry in the MIDDLE of the log is NOT a torn tail, so
    /// `open_existing` must NOT truncate at it — the acknowledged bytes after it
    /// must survive on disk. #1390 originally recovered them by skipping the
    /// corrupt entry by its (untrusted) declared length and continuing; #1391
    /// SUPERSEDES that with fail-fast-then-report: because the length field is not
    /// CRC-protected, trusting it to locate the next entry is a heuristic, so
    /// replay STOPS at the corruption, reports it (never silent), and the segment
    /// (including any successors) is PRESERVED on disk for evidence rather than
    /// recovered.
    ///
    /// WAL = [valid A][complete CRC-corrupt B][valid C]: the file length is
    /// preserved through C (no truncation), `open_existing` leaves the corrupt
    /// tail pending (not trimmed), and `replay` surfaces the valid prefix [A] plus
    /// a non-clean, stopped-early report. Before #1390, `scan_last_valid_offset`
    /// stopped at B's CRC mismatch and `open_existing` truncated back to the end
    /// of A, permanently and SILENTLY discarding the acknowledged C.
    #[test]
    fn test_wal_open_existing_preserves_entries_after_midstream_crc_corruption() {
        let temp_dir = TempDir::new().unwrap();

        // [A][B][C], all synced. Capture the end-of-A offset (start of B's frame)
        // and the total size.
        let (path, end_of_a, total) = {
            let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();
            wal.append(&create_test_mutation(1, "Alice")).unwrap();
            wal.sync().unwrap();
            let end_of_a = wal.size();
            wal.append(&create_test_mutation(2, "Bob")).unwrap();
            wal.sync().unwrap();
            wal.append(&create_test_mutation(3, "Carol")).unwrap();
            wal.sync().unwrap();
            let total = wal.size();
            let path = wal.path().to_path_buf();
            drop(wal);
            (path, end_of_a, total)
        };

        // Corrupt B's CRC (the 4 bytes at end_of_a+4) with a guaranteed-different
        // value via a bit flip. B stays structurally complete (header + full
        // payload intact); only its checksum is wrong.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(end_of_a + 4)).unwrap();
        let mut crc = [0u8; 4];
        file.read_exact(&mut crc).unwrap();
        crc[0] ^= 0xFF;
        file.seek(SeekFrom::Start(end_of_a + 4)).unwrap();
        file.write_all(&crc).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // open_existing must NOT truncate: B is complete and has bytes (C) after
        // it, so it is not a torn tail. Under #1391 the corrupt tail is left
        // pending (preserved for evidence), not silently trimmed.
        let wal = WriteAheadLog::open_existing(&path).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            total,
            "a mid-stream CRC-corrupt (but structurally complete) entry must NOT be trimmed"
        );
        assert_eq!(wal.size(), total);
        assert!(
            wal.has_pending_corrupt_tail(),
            "mid-stream corruption must be recorded as a pending corrupt tail, not trimmed away"
        );

        // replay surfaces the valid prefix [A] and REPORTS the mid-stream
        // corruption (never silent): it stops at B rather than trusting B's
        // uncovered length to skip to C. C is preserved on disk (see file-length
        // assertion above) and surfaced via the non-clean report, not lost.
        let report = wal.replay().unwrap();
        assert!(
            !report.is_clean(),
            "mid-stream CRC corruption must be reported, not silently swallowed"
        );
        assert!(
            report.stopped_early,
            "replay must stop at the untrusted mid-stream corruption"
        );
        assert_eq!(report.corrupt_entries, 1, "B must be reported corrupt");
        let names: Vec<&str> = report
            .mutations
            .iter()
            .map(|m| match &m.operations[0] {
                CellOperation::Write {
                    value: Value::Text(name),
                    ..
                } => name.as_str(),
                other => panic!("expected Write op, got {other:?}"),
            })
            .collect();
        assert_eq!(
            names,
            vec!["Alice"],
            "replay recovers the valid prefix [A] and reports the rest as lossy (never silent)"
        );
    }

    /// Issue #1391 (Finding A): a WAL that is JUST a torn FIRST header — 1..=7
    /// garbage bytes at offset 0 with no valid entry before them — must be
    /// classified as a torn tail and trimmed to 0 on open, exactly like a torn
    /// header after a valid entry. The prior `offset == 0` short-circuit
    /// misclassified it as a clean EOF, so the garbage was left in place and
    /// every later acknowledged append landed after it (unrecoverable on the
    /// next replay). Verify: trim to 0, append C, replay yields exactly [C].
    #[test]
    fn test_wal_open_existing_trims_torn_first_header() {
        for garbage_len in 1..8usize {
            let temp_dir = TempDir::new().unwrap();
            let path = temp_dir.path().join(WriteAheadLog::WAL_FILENAME);

            // Write ONLY a partial (torn) 8-byte header — nothing valid precedes it.
            std::fs::write(&path, vec![0xABu8; garbage_len]).unwrap();

            // Reopen: the torn first header must be trimmed back to offset 0.
            {
                let mut wal = WriteAheadLog::open_existing(&path).unwrap();
                assert_eq!(
                    std::fs::metadata(&path).unwrap().len(),
                    0,
                    "torn first header ({garbage_len} byte(s)) must be trimmed to 0, not kept"
                );
                assert_eq!(wal.size(), 0);
                assert!(
                    !wal.has_pending_corrupt_tail(),
                    "a torn tail is trimmed in open, not left pending"
                );

                // Append C after the trim, synced.
                wal.append(&create_test_mutation(3, "Carol")).unwrap();
                wal.sync().unwrap();
            }

            // Fresh reopen + replay must recover EXACTLY [C]: the append landed at
            // offset 0, not after the garbage.
            let wal = WriteAheadLog::open_existing(&path).unwrap();
            let report = wal.replay().unwrap();
            assert!(
                report.is_clean(),
                "recovery must be clean after trimming the torn first header \
                 (garbage_len={garbage_len})"
            );
            let names: Vec<&str> = report
                .mutations
                .iter()
                .map(|m| match &m.operations[0] {
                    CellOperation::Write {
                        value: Value::Text(name),
                        ..
                    } => name.as_str(),
                    other => panic!("expected Write op, got {other:?}"),
                })
                .collect();
            assert_eq!(
                names,
                vec!["Carol"],
                "replay must yield exactly [C] (garbage_len={garbage_len})"
            );
        }
    }

    /// Issue #1391 (roborev r2, Finding 2): a WAL opened with a mid-stream
    /// corrupt tail (`pending_valid_prefix` set) must REFUSE `append()` until the
    /// caller resets to the valid prefix. A direct public-WAL-API consumer that
    /// does not go through `WriteEngine::new` (which resets first) would otherwise
    /// append AFTER the corrupt entry — where the next `replay()` stops — silently
    /// losing the acknowledged write. Verify: append errors while pending; after
    /// `reset_to_valid_prefix()` the append succeeds and replays as [A, C].
    #[test]
    fn test_wal_append_rejected_until_reset_after_midstream_corruption() {
        let temp_dir = TempDir::new().unwrap();
        let (path, end_of_a, total) = write_two_entries(&temp_dir);

        // Corrupt B's payload IN PLACE (flip its first payload byte) without
        // changing any length — a fully-present entry with a bad CRC, which
        // `scan_valid_prefix` classifies as `Corruption` (not a torn tail). The
        // valid prefix ends at A, so `open_existing` records a pending corrupt
        // tail rather than trimming it away.
        let b_first_payload = end_of_a + 8;
        assert!(
            b_first_payload < total,
            "test setup: B must have at least one payload byte"
        );
        {
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[b_first_payload as usize] ^= 0xFF;
            std::fs::write(&path, &bytes).unwrap();
        }

        // Open: the corrupt segment is left intact (evidence) and the valid
        // prefix (end of A) is recorded as pending.
        let mut wal = WriteAheadLog::open_existing(&path).unwrap();
        assert!(
            wal.has_pending_corrupt_tail(),
            "mid-stream corruption must leave a pending valid prefix, not silently trim"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            total,
            "the corrupt segment must remain on disk for evidence preservation"
        );

        // Finding 2: appending BEFORE reset must fail-closed (otherwise the write
        // lands after the corrupt tail and is lost on the next replay).
        let err = wal
            .append(&create_test_mutation(3, "Carol"))
            .expect_err("append must be rejected while a corrupt tail is unreset");
        match err {
            Error::Storage(msg) => assert!(
                msg.contains("reset_to_valid_prefix"),
                "error must direct the caller to reset first, got: {msg}"
            ),
            other => panic!("expected Error::Storage, got {other:?}"),
        }

        // Reset to the valid prefix (clears the pending flag) and confirm the
        // corrupt tail is gone from the LIVE log.
        let reset_to = wal.reset_to_valid_prefix().unwrap();
        assert_eq!(reset_to, Some(end_of_a), "reset trims back to the end of A");
        assert!(
            !wal.has_pending_corrupt_tail(),
            "reset_to_valid_prefix must clear the pending flag"
        );

        // After reset, append succeeds and lands at the valid boundary.
        wal.append(&create_test_mutation(3, "Carol")).unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Fresh reopen + replay must recover EXACTLY [A, C]: C landed at end_of_a,
        // not after the (now removed) corrupt B.
        let wal = WriteAheadLog::open_existing(&path).unwrap();
        let report = wal.replay().unwrap();
        assert!(
            report.is_clean(),
            "recovery must be clean after reset + post-recovery append"
        );
        let names: Vec<&str> = report
            .mutations
            .iter()
            .map(|m| match &m.operations[0] {
                CellOperation::Write {
                    value: Value::Text(name),
                    ..
                } => name.as_str(),
                other => panic!("expected Write op, got {other:?}"),
            })
            .collect();
        assert_eq!(
            names,
            vec!["Alice", "Carol"],
            "replay must yield exactly [A, C]"
        );
    }

    /// Finding 1 (roborev r3): the write path's accepted max entry size MUST equal
    /// the replay/scan limit. An entry whose serialized length exceeds
    /// `MAX_ENTRY_LENGTH` must be rejected BEFORE it is written (never
    /// fsync-acknowledged), otherwise `replay()` would classify it as corruption
    /// and drop it — silent acknowledged-write loss. A just-under-limit entry must
    /// still succeed and replay intact.
    #[test]
    fn test_wal_append_rejects_oversize_entry_matching_replay_limit() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();
        let path = wal.path().to_path_buf();

        // Over-limit: serialized length > MAX_ENTRY_LENGTH (the payload alone
        // already exceeds the ceiling). Must be rejected and NOT acknowledged.
        let over = "x".repeat(MAX_ENTRY_LENGTH as usize + 1);
        let err = wal
            .append(&create_test_mutation(1, &over))
            .expect_err("over-limit append must be rejected, not acknowledged");
        match err {
            Error::Storage(msg) => assert!(
                msg.contains("MAX_ENTRY_LENGTH"),
                "error must cite the size ceiling, got: {msg}"
            ),
            other => panic!("expected Error::Storage, got {other:?}"),
        }
        assert_eq!(
            wal.size(),
            0,
            "a rejected append must not write or grow the WAL"
        );

        // Just-under-limit: payload sized so the whole entry stays below the
        // ceiling; it must be accepted and survive a fresh reopen + replay.
        let under = "y".repeat(MAX_ENTRY_LENGTH as usize - 8192);
        wal.append(&create_test_mutation(2, &under)).unwrap();
        wal.sync().unwrap();
        drop(wal);

        let wal = WriteAheadLog::open_existing(&path).unwrap();
        let report = wal.replay().unwrap();
        assert!(
            report.is_clean(),
            "just-under-limit entry must replay cleanly"
        );
        assert_eq!(report.mutations.len(), 1, "exactly the accepted entry");
        match &report.mutations[0].operations[0] {
            CellOperation::Write {
                value: Value::Text(name),
                ..
            } => assert_eq!(name.len(), under.len(), "recovered payload must match"),
            other => panic!("expected Write op, got {other:?}"),
        }
    }

    /// Finding 2 (roborev r3): if any step of the reset sequence fails, the
    /// fail-closed guard must remain set so `append()` keeps rejecting writes.
    /// Injects a failure by removing the containing directory after the WAL fd is
    /// open: `flush`/`set_len`/`sync_all` still act on the open inode, but the
    /// final `sync_directory(parent)` opens the now-missing dir and errors. The
    /// old `.take()` cleared the guard up front, so an error left the WAL wrongly
    /// appendable; the guard must now survive the failure.
    #[cfg(unix)]
    #[test]
    fn test_wal_reset_failure_keeps_guard_set() {
        let temp_dir = TempDir::new().unwrap();
        let subdir = temp_dir.path().join("wal_sub");
        std::fs::create_dir(&subdir).unwrap();

        // Write A + B inside the removable subdir, then corrupt B's payload in
        // place to produce a mid-stream corrupt tail (valid prefix ends at A).
        let (path, end_of_a, total) = {
            let mut wal = WriteAheadLog::create(&subdir).unwrap();
            wal.append(&create_test_mutation(1, "Alice")).unwrap();
            wal.sync().unwrap();
            let end_of_a = wal.size();
            wal.append(&create_test_mutation(2, "Bob")).unwrap();
            wal.sync().unwrap();
            let total = wal.size();
            let path = wal.path().to_path_buf();
            (path, end_of_a, total)
        };
        let b_first_payload = end_of_a + 8;
        assert!(
            b_first_payload < total,
            "test setup: B must have a payload byte"
        );
        {
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[b_first_payload as usize] ^= 0xFF;
            std::fs::write(&path, &bytes).unwrap();
        }

        let mut wal = WriteAheadLog::open_existing(&path).unwrap();
        assert!(
            wal.has_pending_corrupt_tail(),
            "mid-stream corruption must record a pending valid prefix"
        );

        // Inject the reset-sequence failure: remove the dir the WAL lives in.
        std::fs::remove_dir_all(&subdir).unwrap();

        let err = wal
            .reset_to_valid_prefix()
            .expect_err("reset must propagate the directory-sync failure");
        assert!(matches!(err, Error::Storage(_)), "expected Error::Storage");
        assert!(
            wal.has_pending_corrupt_tail(),
            "the guard MUST remain set when the reset sequence fails partway"
        );

        // With the guard still set, append stays fail-closed.
        let append_err = wal
            .append(&create_test_mutation(3, "Carol"))
            .expect_err("append must remain rejected after a failed reset");
        match append_err {
            Error::Storage(msg) => assert!(
                msg.contains("reset_to_valid_prefix"),
                "error must still direct the caller to reset first, got: {msg}"
            ),
            other => panic!("expected Error::Storage, got {other:?}"),
        }
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
