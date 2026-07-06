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
    /// without stopping early. A torn HEADER (an incomplete final append that
    /// never finished its 8-byte header, so there is provably no complete entry
    /// to lose) is NOT corruption and keeps a report clean; a complete-header
    /// entry that cannot be satisfied is corruption and makes a report lossy
    /// (issue #1391 r5).
    pub fn is_clean(&self) -> bool {
        self.corrupt_entries == 0 && !self.stopped_early
    }
}

/// Why a sequential WAL scan stopped advancing (issue #1390 + #1391).
///
/// Distinguishing a torn tail from mid-stream corruption is the crux of safe
/// recovery: a torn tail is an interrupted final append (safe to trim), whereas
/// a CRC mismatch / implausible length / unsatisfiable length on a *complete-
/// header* entry is *unexpected corruption* whose successors must NOT be
/// silently trimmed away.
///
/// # The torn-tail boundary is the header, not the payload (issue #1391, r5)
///
/// A torn tail is trimmable-as-clean ONLY when the trailing entry's 8-byte
/// header is itself incomplete (< 8 bytes at EOF): there is provably no complete
/// entry to lose. Once a COMPLETE header is present, an entry that cannot be
/// satisfied (short payload at EOF, bad CRC, or an implausible length) is
/// [`Corruption`](Self::Corruption): using only the allowed no-heuristics facts
/// (header completeness, declared-length vs remaining-bytes, physical-tail
/// position) an interrupted final write is INDISTINGUISHABLE from a bit-flipped
/// length whose declared size overshoots EOF and swallows valid, acknowledged
/// successors. Trusting it to be "just a torn tail" is exactly the silent lossy
/// trim #1391 eliminates, so a complete-header entry that is dropped is always
/// preserved + surfaced as lossy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalStop {
    /// Reached end-of-file exactly on an entry boundary — a fully clean log.
    CleanEof,
    /// The trailing entry's 8-byte HEADER was cut short (< 8 bytes at EOF): an
    /// interrupted append that never even finished its header, so there is
    /// provably no complete entry to lose. This is the only structurally-
    /// incomplete tail that may be trimmed-as-clean (issue #1391, r5).
    TornTail,
    /// A COMPLETE-header entry could not be trusted: it failed CRC, declared an
    /// implausible length (> [`MAX_ENTRY_LENGTH`]), or declared a length that
    /// cannot be satisfied by the remaining bytes (short payload at EOF). The
    /// framing past this point is untrustworthy and the dropped entry may be a
    /// bit-flipped record swallowing valid successors, so it must be preserved,
    /// not silently discarded.
    Corruption,
}

/// Outcome of a failed [`WriteAheadLog::truncate_checked`], distinguishing a
/// failure that leaves the WAL intact from one that has already destroyed it
/// (issue #1392).
///
/// `set_len(0)` is the point of no return: once it succeeds the on-disk WAL is
/// empty and is no longer a replayable recovery marker, even if a subsequent
/// fsync/seek fails. Swallowing such a post-mutation failure as if the WAL were
/// still intact would let the caller clear the memtable and report a durable,
/// replayable WAL that no longer exists — silent data loss.
#[derive(Debug)]
pub enum TruncateError {
    /// Failure at or before `set_len(0)`: the WAL contents were not mutated and
    /// remain a valid replay marker. Safe to leave the WAL in place.
    BeforeMutation(Error),
    /// Failure after `set_len(0)` zeroed the WAL: the WAL is no longer a valid
    /// replay marker. Callers MUST NOT treat the WAL as intact.
    AfterMutation(Error),
}

impl TruncateError {
    /// Unwrap to the underlying [`Error`], discarding the phase distinction.
    /// Used by the phase-agnostic [`WriteAheadLog::truncate`] wrapper.
    pub fn into_inner(self) -> Error {
        match self {
            TruncateError::BeforeMutation(e) | TruncateError::AfterMutation(e) => e,
        }
    }
}

impl std::fmt::Display for TruncateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TruncateError::BeforeMutation(e) => {
                write!(f, "WAL truncate failed before mutation (WAL intact): {e}")
            }
            TruncateError::AfterMutation(e) => write!(
                f,
                "WAL truncate failed after mutation (WAL contents already zeroed): {e}"
            ),
        }
    }
}

impl std::error::Error for TruncateError {}

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
/// record written by an older binary round-trips. `Delete` differs (no
/// `local_deletion_time`, pre-#921) and `WriteWithTtl` differs (no per-cell
/// `local_deletion_time`, pre-#1538). It MUST stay in lockstep with [`CellOperation`]:
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
            // Pre-#1538 expiring cell: no surfaced source LDT, so the writer
            // derives `now + ttl` (historical behavior).
            LegacyCellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
            } => CellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
                local_deletion_time: None,
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
    // Layout (C) predates #1538, so its on-disk `WriteWithTtl` is the 3-field
    // shape. Decode ops through the pre-#1538 mirror (which keeps the post-#921
    // `Delete { column, local_deletion_time }` shape this layout carries), NOT
    // the current 4-field `CellOperation`, which would positionally misalign.
    operations: Vec<PreCellLdtWriteTtlCellOperation>,
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
            operations: m.operations.into_iter().map(CellOperation::from).collect(),
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
    // Layout (D) predates #1538, so its on-disk `WriteWithTtl` is the 3-field
    // shape. Decode ops through the pre-#1538 mirror (which keeps the post-#921
    // `Delete { column, local_deletion_time }` shape this layout carries), NOT
    // the current 4-field `CellOperation`, which would positionally misalign.
    operations: Vec<PreCellLdtWriteTtlCellOperation>,
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
            operations: m.operations.into_iter().map(CellOperation::from).collect(),
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

/// On-disk cell-op layout immediately before Issue #1538 (i.e. post-#921,
/// pre-#1538 — the shape [`CellOperation`] had for the entire #921..#1538 span,
/// covering the post-#932/pre-#1018 (D), post-#921/pre-#932 (C), and
/// post-#1018/pre-#1538 eras).
///
/// Issue #1538 appended a trailing `local_deletion_time: Option<i32>` field to
/// [`CellOperation::WriteWithTtl`], turning its on-disk shape from
/// `{ column, value, ttl_seconds }` into `{ column, value, ttl_seconds,
/// local_deletion_time }`. Because the WAL has no per-record version field and
/// bincode is positional, a record whose ops were written by a pre-#1538 binary
/// encodes a 3-field `WriteWithTtl` with NO bytes for the new field, so decoding
/// it with the current [`CellOperation`] reads the following operation's bytes as
/// the missing `Option` and misaligns the whole record.
///
/// This mirror reproduces the EXACT pre-#1538 variant order and shapes — it is
/// the current [`CellOperation`] with ONLY `WriteWithTtl` reverted to 3 fields.
/// Crucially the `Delete` variant keeps its post-#921 `{ column,
/// local_deletion_time }` shape (unlike the older [`LegacyCellOperation`], whose
/// pre-#921 `Delete { column }` would misdecode a real mixed
/// `WriteWithTtl` + `Delete` record — e.g. `UPDATE … USING TTL SET a=1, b=null`),
/// so every op of a pre-#1538 record round-trips faithfully. It MUST stay in
/// lockstep with [`CellOperation`]: variant order is the bincode discriminant.
#[derive(serde::Serialize, serde::Deserialize)]
enum PreCellLdtWriteTtlCellOperation {
    Write {
        column: String,
        value: crate::types::Value,
    },
    /// Pre-#1538 expiring cell: no per-cell `local_deletion_time` field.
    WriteWithTtl {
        column: String,
        value: crate::types::Value,
        ttl_seconds: u32,
    },
    /// Post-#921 `Delete` carries its per-cell `local_deletion_time` (current shape).
    Delete {
        column: String,
        local_deletion_time: Option<i32>,
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

impl From<PreCellLdtWriteTtlCellOperation> for CellOperation {
    fn from(op: PreCellLdtWriteTtlCellOperation) -> Self {
        match op {
            PreCellLdtWriteTtlCellOperation::Write { column, value } => {
                CellOperation::Write { column, value }
            }
            // Pre-#1538 expiring cell: no surfaced source LDT, so the writer
            // derives `now + ttl` (historical behavior).
            PreCellLdtWriteTtlCellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
            } => CellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
                local_deletion_time: None,
            },
            PreCellLdtWriteTtlCellOperation::Delete {
                column,
                local_deletion_time,
            } => CellOperation::Delete {
                column,
                local_deletion_time,
            },
            PreCellLdtWriteTtlCellOperation::DeleteRow => CellOperation::DeleteRow,
            PreCellLdtWriteTtlCellOperation::WriteComplexElement {
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
            PreCellLdtWriteTtlCellOperation::ComplexDeletion {
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

/// On-disk `Mutation` layout post-Issue #1018, pre-Issue #1538 (layout (E)).
///
/// Issue #1538 appended `local_deletion_time` to
/// [`CellOperation::WriteWithTtl`] (inside `operations: Vec<CellOperation>`). A
/// record written by the immediately-preceding (post-#1018, pre-#1538) binary
/// carries ALL current mutation-level trailing fields (`local_deletion_time`,
/// `row_tombstone`, `cell_write_timestamps`) but encodes any `WriteWithTtl` op
/// with the pre-#1538 3-field shape. Decoding it as the current [`Mutation`]
/// (whose `WriteWithTtl` now expects a 4th field) misaligns: bincode either
/// errors, or worse succeeds-but-wrong by reading the tag byte of the following
/// mutation field as the missing `Option` — silent corruption. This mirror is
/// identical to the current [`Mutation`] field-for-field, with only `operations`
/// swapped to [`PreCellLdtWriteTtlCellOperation`], so such records replay
/// faithfully with `WriteWithTtl.local_deletion_time = None`.
///
/// The field order MUST stay in lockstep with [`Mutation`].
#[derive(serde::Serialize, serde::Deserialize)]
struct PreCellLdtWriteTtlMutation {
    table: TableId,
    partition_key: PartitionKey,
    clustering_key: Option<ClusteringKey>,
    operations: Vec<PreCellLdtWriteTtlCellOperation>,
    timestamp_micros: i64,
    ttl_seconds: Option<u32>,
    partition_tombstone: Option<PartitionTombstone>,
    range_tombstones: Vec<RangeTombstone>,
    local_deletion_time: Option<i32>,
    row_tombstone: Option<(i64, i32)>,
    cell_write_timestamps: Option<std::collections::HashMap<String, i64>>,
}

impl From<PreCellLdtWriteTtlMutation> for Mutation {
    fn from(m: PreCellLdtWriteTtlMutation) -> Self {
        Mutation {
            table: m.table,
            partition_key: m.partition_key,
            clustering_key: m.clustering_key,
            operations: m.operations.into_iter().map(CellOperation::from).collect(),
            timestamp_micros: m.timestamp_micros,
            ttl_seconds: m.ttl_seconds,
            partition_tombstone: m.partition_tombstone,
            range_tombstones: m.range_tombstones,
            local_deletion_time: m.local_deletion_time,
            row_tombstone: m.row_tombstone,
            cell_write_timestamps: m.cell_write_timestamps,
        }
    }
}

/// Deserialize a `Mutation` from WAL bytes, tolerating records written by an
/// older binary that predates the `Mutation::local_deletion_time` field
/// (Issue #764), the `CellOperation::Delete::local_deletion_time` field
/// (Issue #921), the `Mutation::row_tombstone` (Issue #932) /
/// `cell_write_timestamps` (Issue #1018) fields, or the
/// `CellOperation::WriteWithTtl::local_deletion_time` field (Issue #1538).
///
/// Attempts the layouts most-recent-first (each subsequent layout has fewer
/// trailing fields / an older op shape): current, then layout (E) (post-#1018/
/// pre-#1538: all current mutation fields but a 3-field `WriteWithTtl`), then
/// (D), (C), (B), (A). Each maps to the current [`Mutation`]/[`CellOperation`]
/// with the newer fields upgraded to their historical defaults (`None`).
fn decode_mutation(bytes: &[u8]) -> std::result::Result<Mutation, bincode::Error> {
    match bincode::deserialize::<Mutation>(bytes) {
        Ok(mutation) => Ok(mutation),
        Err(current_err) => {
            // Layout (E): post-#1018, pre-#1538 — ALL current mutation-level
            // trailing fields present, but `WriteWithTtl` ops carry the pre-#1538
            // 3-field shape. Tried FIRST among the fallbacks because it has the
            // most trailing fields (same count as the current layout), so a
            // pre-#1538 `WriteWithTtl` record decodes here rather than misaligning
            // under the fewer-trailing-field mirrors below. A current record never
            // reaches this block (the current layout above already decoded it), and
            // an older (D/C/B/A) record lacks bytes for the extra trailing field(s)
            // and errors out of this mirror into the correct one below.
            if let Ok(m) = bincode::deserialize::<PreCellLdtWriteTtlMutation>(bytes) {
                return Ok(Mutation::from(m));
            }
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
    /// Reusable scratch buffer for serializing a mutation in `append`, so the
    /// steady-state write path does not allocate a fresh `Vec` per call. Cleared
    /// and refilled on every `append`; never shared across threads (`&mut self`).
    append_scratch: Vec<u8>,
    /// Byte offset of the last CRC-valid prefix when `open_existing` detected
    /// mid-stream corruption (issue #1391). The corrupt tail is intentionally
    /// left on disk so the caller can preserve it aside as forensic evidence
    /// FIRST; once that is done the caller invokes
    /// [`reset_to_valid_prefix`](Self::reset_to_valid_prefix) so post-recovery
    /// appends land at a replayable position instead of after the corrupt bytes
    /// (where a synced write would be lost on the next replay). `None` for a
    /// freshly created WAL, a clean reopen, or a torn tail (already trimmed).
    pending_valid_prefix: Option<u64>,
    /// Set when a post-`set_len(0)` restore step (issue #1392, FINDING 1) left
    /// the WAL in a state where the file cursor's position no longer
    /// authoritatively matches `current_size`. The canonical case is a failed
    /// `seek(0)` after truncation: the cursor may remain at the stale OLD
    /// end-of-file while `current_size` has been reset to 0, so a later
    /// `append()` would write at that stale offset over the now-zeroed file,
    /// producing a sparse / misparsed WAL (silent corruption). Once poisoned,
    /// every `append`/`sync`/`truncate` fails closed with a typed
    /// [`Error::Storage`] until the WAL is reopened.
    poisoned: Option<String>,
    /// Test-only fault injection: when set, force the `sync_all` performed
    /// *after* `set_len(0)` inside [`WriteAheadLog::truncate_checked`] to fail,
    /// exercising the `AfterMutation` state-restore path (issue #1392).
    #[cfg(test)]
    fail_sync_after_truncate: bool,
    /// Test-only fault injection: when set, force the `seek(0)` performed
    /// *after* `set_len(0)` inside [`WriteAheadLog::truncate_checked`] to fail,
    /// exercising the poison-on-partial-failure path (issue #1392, FINDING 1).
    #[cfg(test)]
    fail_seek_after_truncate: bool,
}

// Manual `Debug` (not derived) so the reusable `append_scratch` buffer — which
// holds the last serialized mutation's raw row bytes — is never printed under
// `{:?}`. Leaking those bytes would expose user data and could produce huge log
// lines; we summarize the buffer by len/capacity only. All other fields keep
// their usual `Debug` representation.
impl std::fmt::Debug for WriteAheadLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg = f.debug_struct("WriteAheadLog");
        dbg.field("file", &self.file)
            .field("path", &self.path)
            .field("buffer_size", &self.buffer_size)
            .field("current_size", &self.current_size)
            .field(
                "append_scratch",
                &format_args!(
                    "<{} bytes, cap {}>",
                    self.append_scratch.len(),
                    self.append_scratch.capacity()
                ),
            )
            .field("pending_valid_prefix", &self.pending_valid_prefix)
            .field("poisoned", &self.poisoned);
        #[cfg(test)]
        dbg.field("fail_sync_after_truncate", &self.fail_sync_after_truncate)
            .field("fail_seek_after_truncate", &self.fail_seek_after_truncate);
        dbg.finish()
    }
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
            append_scratch: Vec::new(),
            pending_valid_prefix: None,
            poisoned: None,
            #[cfg(test)]
            fail_sync_after_truncate: false,
            #[cfg(test)]
            fail_seek_after_truncate: false,
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

        // Only a TORN TAIL — now strictly a torn HEADER (< 8 bytes at EOF), the
        // sole structurally-incomplete tail with provably no complete entry to
        // lose (issue #1391 r5) — may be trimmed HERE; that is #1390's guarantee
        // that future appends resume at the last valid boundary. Any COMPLETE-
        // header entry that cannot be trusted (`Corruption`: bad CRC, implausible
        // length, or a declared length that overshoots EOF) may be a bit-flipped
        // record swallowing VALID successors, so silently `set_len`-ing it away at
        // open time would discard evidence before it can be preserved. We
        // therefore leave a corrupt log
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
            append_scratch: Vec::new(),
            pending_valid_prefix,
            poisoned: None,
            #[cfg(test)]
            fail_sync_after_truncate: false,
            #[cfg(test)]
            fail_seek_after_truncate: false,
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
    /// - missing/short in its 8-byte header ([`WalStop::TornTail`] — a torn
    ///   header is the only structurally-incomplete tail that may be trimmed as
    ///   clean),
    /// - declaring an implausible length (> [`MAX_ENTRY_LENGTH`], garbage tail —
    ///   [`WalStop::Corruption`]),
    /// - complete in its header but short in its payload at EOF
    ///   ([`WalStop::Corruption`] — a complete-header entry whose declared length
    ///   cannot be satisfied is indistinguishable from a length bit-flip that
    ///   overshoots EOF, so it is preserved, not trimmed; issue #1391 r5), or
    /// - CRC-invalid ([`WalStop::Corruption`] — corrupt framing, offsets past it
    ///   cannot be trusted).
    ///
    /// The returned offset is therefore the end of a contiguous run of fully
    /// written, CRC-valid entries. Bytes beyond it are either a torn HEADER left
    /// by an interrupted append (trimmed as clean) or a corrupt/unsatisfiable
    /// complete-header entry (preserved for evidence, reset separately).
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

            // Read the declared payload. A short read (EOF before `entry_length`
            // bytes) on a COMPLETE 8-byte header is CORRUPTION, not a torn tail
            // (issue #1391, roborev r5). Using only the allowed no-heuristics
            // facts (header completeness, declared-length vs remaining-bytes,
            // physical-tail position) an interrupted final write of THIS entry is
            // indistinguishable from a bit-flipped length on a fully-written entry
            // whose declared size overshoots EOF and swallows valid successors —
            // both are "complete header, declared length > remaining bytes, at the
            // physical tail". Guessing which would be a byte-pattern heuristic. So
            // we treat it as corruption: leave the tail intact for evidence and let
            // the caller preserve + report it, rather than silently trimming an
            // acknowledged successor away as a clean torn tail. Only a torn HEADER
            // (< 8 bytes, handled above) is a structurally-incomplete tail that may
            // be trimmed-as-clean.
            let mut payload = vec![0u8; entry_length as usize];
            match file.read_exact(&mut payload) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok((offset, WalStop::Corruption));
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
        // Fail closed if a prior truncate poisoned the WAL (issue #1392,
        // FINDING 1): appending at a stale cursor over a zeroed file would
        // produce a sparse / misparsed WAL.
        self.ensure_not_poisoned()?;

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

        // Serialize mutation using bincode into a reusable scratch buffer so the
        // steady-state write path does not allocate a fresh `Vec` per call.
        // `serialize_into` yields byte-identical output to `bincode::serialize`.
        self.append_scratch.clear();
        bincode::serialize_into(&mut self.append_scratch, mutation)
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
        if self.append_scratch.len() as u64 > MAX_ENTRY_LENGTH as u64 {
            let serialized_len = self.append_scratch.len();
            // A single oversized (rejected) mutation must not permanently bloat the
            // WAL instance's retained scratch capacity: release it before returning.
            self.append_scratch.clear();
            self.append_scratch.shrink_to_fit();
            return Err(Error::Storage(format!(
                "WAL entry exceeds MAX_ENTRY_LENGTH (16 MiB): {} bytes",
                serialized_len
            )));
        }

        let entry_length = self.append_scratch.len() as u32;

        // Calculate CRC32 over the mutation bytes
        let mut hasher = Hasher::new();
        hasher.update(&self.append_scratch);
        let crc32 = hasher.finalize();

        // Write entry: [length][crc32][mutation_bytes]
        self.file
            .write_all(&entry_length.to_le_bytes())
            .map_err(|e| Error::Storage(format!("Failed to write entry length: {}", e)))?;

        self.file
            .write_all(&crc32.to_le_bytes())
            .map_err(|e| Error::Storage(format!("Failed to write CRC32: {}", e)))?;

        self.file
            .write_all(&self.append_scratch)
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
        // Fail closed if the WAL was poisoned by a partial truncate-restore
        // (issue #1392, FINDING 1): its buffered/cursor state is not trustworthy.
        self.ensure_not_poisoned()?;

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
    /// - **Complete header + short payload at EOF**: a complete-header entry
    ///   whose declared length cannot be satisfied is CORRUPTION (issue #1391
    ///   r5), not a clean torn tail — it is indistinguishable from a length
    ///   bit-flip that overshoots EOF and swallows valid successors. Replay stops
    ///   early and records the loss ([`RecoveryReport::stopped_early`]).
    /// - **Torn header** (short 8-byte header at EOF): an interrupted final
    ///   append that never finished its header — there is provably no complete
    ///   entry to lose, so replay stops cleanly (not counted as corruption).
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
        // Thin wrapper over `replay_each` (issue #1661): collect the streamed
        // mutations into the report's Vec so existing callers/tests are
        // unchanged. `replay_each` itself never materialises the whole log.
        let mut out = Vec::new();
        let mut report = self.replay_each(|m| {
            out.push(m);
            Ok(())
        })?;
        report.mutations = out;
        Ok(report)
    }

    /// Streaming form of [`replay`](Self::replay): invoke `f` once per recovered
    /// mutation instead of accumulating a whole-log `Vec<Mutation>` (issue #1661).
    ///
    /// Control flow — decode/skip/stop/error semantics and the on-disk framing —
    /// is byte-for-byte identical to [`replay`](Self::replay); this is a pure
    /// memory refactor. The two differences are internal:
    ///
    /// 1. A single payload buffer is reused across entries (its capacity grows to
    ///    the largest entry seen and is retained), so the peak read buffer is
    ///    bounded by the largest single entry rather than allocated fresh per
    ///    entry.
    /// 2. Each successfully decoded mutation is passed to `f` rather than pushed
    ///    onto a `Vec`, so the caller (e.g. crash recovery) can insert it into the
    ///    memtable immediately and never hold the whole log resident.
    ///
    /// The returned [`RecoveryReport`] carries the same corruption metadata
    /// ([`corrupt_entries`](RecoveryReport::corrupt_entries),
    /// [`stopped_early`](RecoveryReport::stopped_early),
    /// [`bytes_skipped`](RecoveryReport::bytes_skipped)) as `replay`, but its
    /// [`mutations`](RecoveryReport::mutations) vec is left empty — the mutations
    /// were streamed to `f`. Callers MUST still consult
    /// [`RecoveryReport::is_clean`].
    ///
    /// If `f` returns an error, replay stops and the error propagates unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be opened or read (an I/O fault
    /// distinct from in-band corruption, which is reported, not errored), or if
    /// `f` returns an error for a recovered mutation.
    pub fn replay_each<F>(&self, mut f: F) -> Result<RecoveryReport>
    where
        F: FnMut(Mutation) -> Result<()>,
    {
        let mut file = File::open(&self.path)
            .map_err(|e| Error::Storage(format!("Failed to open WAL for replay: {}", e)))?;
        let total_len = file
            .metadata()
            .map_err(|e| Error::Storage(format!("Failed to read WAL metadata for replay: {}", e)))?
            .len();

        let mut report = RecoveryReport::default();
        let mut offset = 0u64;
        // Reuse ONE payload buffer across entries (issue #1661): `resize` retains
        // the allocation's capacity, so the peak read buffer is bounded by the
        // largest single entry rather than a fresh `vec![0u8; len]` per entry.
        let mut mutation_bytes: Vec<u8> = Vec::new();

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

            // Read mutation bytes into the reused buffer (issue #1661). `resize`
            // grows/shrinks the length to exactly `entry_length` (reusing the
            // retained capacity); `read_exact` then overwrites all of it.
            mutation_bytes.resize(entry_length as usize, 0);
            match file.read_exact(&mut mutation_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Short payload on a COMPLETE 8-byte header is CORRUPTION, not
                    // a clean torn tail (issue #1391, roborev r5). A bit-flipped
                    // length that overshoots EOF is structurally indistinguishable
                    // from an interrupted final write here (both: complete header,
                    // declared length > remaining bytes, at the physical tail), and
                    // distinguishing them would be a byte-pattern heuristic. If the
                    // length was corrupted, the swallowed bytes may hold VALID
                    // acknowledged successors; breaking silently reported that loss
                    // as clean. Report it (stop early) so `is_clean()` is false and
                    // the caller preserves the tail aside. Only a torn HEADER
                    // (< 8 bytes, the header read above) is a clean torn tail.
                    log::error!(
                        "WAL entry at offset {} has a complete header declaring length {} but \
                         only {} payload byte(s) remain to EOF - stopping replay; {} trailing \
                         byte(s) not recovered (corruption, not a clean torn tail)",
                        offset,
                        entry_length,
                        total_len.saturating_sub(offset + 8),
                        total_len.saturating_sub(offset)
                    );
                    report.corrupt_entries += 1;
                    report.stopped_early = true;
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
                    // Stream the mutation to the caller instead of accumulating a
                    // whole-log Vec (issue #1661). An `f` error propagates
                    // unchanged.
                    f(mutation)?;
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
    /// On success this also lifts any pending corrupt-tail guard (issue #1391):
    /// an emptied WAL has no corrupt tail, so subsequent `append()` calls must
    /// not be rejected by the stale fail-closed guard.
    ///
    /// # Errors
    ///
    /// Returns an error if the truncate operation fails. This flattens the
    /// phase distinction of [`WriteAheadLog::truncate_checked`]; the
    /// durability handoff (issue #1392) calls `truncate_checked` directly so it
    /// can react differently to a failure that occurs *after* the WAL contents
    /// have already been zeroed.
    pub fn truncate(&mut self) -> Result<()> {
        self.truncate_checked().map_err(TruncateError::into_inner)
    }

    /// Truncate the WAL, distinguishing failures **before** the WAL contents
    /// are mutated from failures **after** `set_len(0)` has already zeroed it
    /// (issue #1392).
    ///
    /// The sequence is: flush pending writes, `set_len(0)`, fsync, seek to
    /// start. The `set_len(0)` call is the point of no return: once it
    /// succeeds the on-disk WAL is empty and is **no longer a replayable
    /// recovery marker**, even if a following fsync/seek fails.
    ///
    /// * A failure at or before `set_len(0)` returns
    ///   [`TruncateError::BeforeMutation`] — the WAL is still intact and can be
    ///   safely left in place for replay.
    /// * A failure after `set_len(0)` returns [`TruncateError::AfterMutation`]
    ///   — the WAL has been zeroed and callers MUST NOT treat it as intact.
    pub fn truncate_checked(&mut self) -> std::result::Result<(), TruncateError> {
        // A previously poisoned WAL cannot be truncated: its cursor/size state is
        // untrustworthy (issue #1392, FINDING 1). Fail closed as AfterMutation so
        // the caller does not treat the WAL as an intact replay marker.
        if let Some(reason) = &self.poisoned {
            return Err(TruncateError::AfterMutation(Error::Storage(format!(
                "WAL is poisoned and cannot be truncated: {reason}"
            ))));
        }

        // Flush any pending writes first (before mutation).
        self.file.flush().map_err(|e| {
            TruncateError::BeforeMutation(Error::Storage(format!(
                "Failed to flush before truncate: {}",
                e
            )))
        })?;

        // Truncate the file to zero length. This is the point of no return:
        // after it succeeds the WAL contents are gone.
        self.file.get_mut().set_len(0).map_err(|e| {
            TruncateError::BeforeMutation(Error::Storage(format!("Failed to truncate WAL: {}", e)))
        })?;

        // From here on the WAL has already been mutated (zeroed). Any failure
        // is AfterMutation: the WAL is no longer a valid replay marker.
        //
        // The post-mutation restore sequence (sync_all + seek(0) +
        // current_size=0) must be atomic-in-effect (issue #1392, FINDING 1): we
        // must never leave the WAL in a state where a later `append()` writes at
        // a stale cursor over the zeroed file (a SPARSE / misparsed WAL). We run
        // both fallible steps, then reconcile:
        //
        // * `seek(0)` succeeds  -> the file cursor is authoritatively at offset
        //   0 and matches the reset `current_size`, so a subsequent append is
        //   safe. A `sync_all` failure here only means the zeroing is not yet
        //   durable, which we surface as AfterMutation (the WAL is still no
        //   longer a replay marker) — but the handle remains usable.
        // * `seek(0)` fails     -> the cursor may remain at the stale OLD
        //   end-of-file while `current_size` was reset to 0. Rather than allow a
        //   corrupting append, we POISON the WAL: every future
        //   append/sync/truncate fails closed with a typed error until the WAL
        //   is reopened (which re-establishes an authoritative cursor).
        let sync_res = self.sync_after_truncate();
        let seek_res = self.seek_after_truncate();
        self.current_size = 0;

        // Post-mutation restore reconciliation (issue #1392, FINDING 1). These
        // checks run BEFORE the #1391 corrupt-tail guard is lifted below: on a
        // failure we return early with `pending_valid_prefix` still set, so the
        // guard is cleared ONLY after a fully successful truncate.
        if let Err(e) = seek_res {
            // Cursor position is now unknown/stale while size was reset. Poison
            // so no later append can silently create a sparse WAL.
            let reason = format!("seek after truncate failed: {e}");
            self.poisoned = Some(reason);
            return Err(TruncateError::AfterMutation(Error::Storage(format!(
                "Failed to seek after truncate; WAL poisoned to prevent a sparse \
                 append: {e}"
            ))));
        }

        // Seek succeeded: cursor is authoritatively at 0. Surface a sync failure
        // (durability of the zeroing) as AfterMutation, but the WAL stays usable.
        if let Err(e) = sync_res {
            return Err(TruncateError::AfterMutation(Error::Storage(format!(
                "Failed to sync after truncate: {}",
                e
            ))));
        }

        // Clear any pending corrupt-tail guard (issue #1391, roborev r4). If this
        // WAL was opened over a mid-stream corrupt tail, `open_existing` recorded
        // the valid-prefix boundary in `pending_valid_prefix` and `append()` stays
        // fail-closed until it is lifted. `truncate()` has just cleared the file to
        // zero length and fsync'd + sought to the start, so the corrupt tail is
        // gone from the LIVE log — leaving the guard set would wrongly reject ALL
        // future appends against a now-empty WAL (deadlocked appends). The guard is
        // lifted only HERE, after the flush/set_len/fsync/seek steps have all
        // succeeded; any earlier failure returns early with the guard still set,
        // so the on-disk state and the fail-closed guard remain consistent.
        self.pending_valid_prefix = None;

        Ok(())
    }

    /// Return an error if the WAL has been poisoned by a partial
    /// truncate-restore failure (issue #1392, FINDING 1).
    fn ensure_not_poisoned(&self) -> Result<()> {
        if let Some(reason) = &self.poisoned {
            return Err(Error::Storage(format!(
                "WAL is poisoned after a failed truncate-restore and must be \
                 reopened before further writes: {reason}"
            )));
        }
        Ok(())
    }

    /// Perform the post-`set_len(0)` fsync, with a test-only fault hook.
    ///
    /// In production this simply calls `sync_all` on the underlying file. Under
    /// `cfg(test)` a fault can be injected (see `fail_sync_after_truncate`) so
    /// the `AfterMutation` state-restore path in `truncate_checked` can be
    /// exercised deterministically (issue #1392, FINDING 2).
    fn sync_after_truncate(&self) -> std::io::Result<()> {
        #[cfg(test)]
        if self.fail_sync_after_truncate {
            return Err(std::io::Error::other("injected post-set_len(0) sync fault"));
        }
        self.file.get_ref().sync_all()
    }

    /// Perform the post-`set_len(0)` seek back to the start of the now-empty
    /// file, with a test-only fault hook.
    ///
    /// In production this seeks the underlying handle to offset 0. Under
    /// `cfg(test)` a fault can be injected (see `fail_seek_after_truncate`) so
    /// the poison-on-partial-failure path in `truncate_checked` can be exercised
    /// deterministically (issue #1392, FINDING 1).
    fn seek_after_truncate(&mut self) -> std::io::Result<u64> {
        #[cfg(test)]
        if self.fail_seek_after_truncate {
            return Err(std::io::Error::other("injected post-set_len(0) seek fault"));
        }
        self.file.get_mut().seek(SeekFrom::Start(0))
    }

    /// Test-only: arm/disarm the post-truncate sync fault (issue #1392).
    ///
    /// `pub(crate)` so the sibling `write_engine` modules' `#[cfg(test)]` tests
    /// (e.g. the WriteEngine-level post-mutation-truncate regression) can drive
    /// the `AfterMutation` path through the real durability barrier.
    #[cfg(test)]
    pub(crate) fn set_fail_sync_after_truncate(&mut self, fail: bool) {
        self.fail_sync_after_truncate = fail;
    }

    /// Test-only: arm/disarm the post-truncate seek fault (issue #1392,
    /// FINDING 1), driving the poison-on-partial-failure path.
    #[cfg(test)]
    pub(crate) fn set_fail_seek_after_truncate(&mut self, fail: bool) {
        self.fail_seek_after_truncate = fail;
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

    // Issue #1392 (FINDING 2): a `truncate_checked` whose `sync_all` fails
    // AFTER `set_len(0)` has already zeroed the WAL must (a) report
    // `AfterMutation` so the flush error propagates, and (b) still restore the
    // in-memory + file-handle state to offset 0. Otherwise a subsequent append
    // lands at the stale OLD end-of-file, producing a SPARSE WAL that replay
    // skips or misparses. This test injects that post-`set_len(0)` sync fault,
    // then performs a fresh append and verifies replay returns exactly that
    // write (no sparse / lost / misparsed WAL).
    #[test]
    fn truncate_post_setlen_sync_failure_resets_state_for_replay() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        // Seed a few entries so the OLD end-of-file is well past offset 0.
        for i in 0..3 {
            wal.append(&create_test_mutation(i, &format!("Old{}", i)))
                .unwrap();
        }
        wal.sync().unwrap();
        assert!(wal.size() > 0);

        // Arm the fault: the sync AFTER set_len(0) will fail.
        wal.set_fail_sync_after_truncate(true);
        let err = wal
            .truncate_checked()
            .expect_err("post-set_len(0) sync fault must surface");
        assert!(
            matches!(err, TruncateError::AfterMutation(_)),
            "a failure after set_len(0) must be AfterMutation (WAL already zeroed)"
        );

        // Despite the propagated error, in-memory state was restored to the
        // start of the now-empty file.
        assert_eq!(wal.size(), 0, "current_size must be reset to 0");

        // Disarm the fault and perform a subsequent write, as the still-usable
        // engine would after the propagated flush error.
        wal.set_fail_sync_after_truncate(false);
        wal.append(&create_test_mutation(42, "New")).unwrap();
        wal.sync().unwrap();

        // Replay must return exactly the new entry, read from offset 0 — proving
        // the append landed at 0 (not the stale old EOF) and the WAL is neither
        // sparse nor misparsed.
        let mutations = wal.replay().unwrap().mutations;
        assert_eq!(
            mutations.len(),
            1,
            "replay must return exactly the post-fault write, with no sparse gap"
        );
        assert_eq!(mutations[0].table.keyspace, "test_ks");
        assert_eq!(
            mutations[0].partition_key.columns[0].1,
            Value::Integer(42),
            "the replayed entry must be the new write, correctly parsed"
        );
    }

    // Issue #1392 (FINDING 1): if the `seek(0)` performed AFTER `set_len(0)`
    // fails, the file cursor may remain at the stale OLD end-of-file while
    // `current_size` has been reset to 0. A naive implementation would let a
    // later `append()` land at that stale offset over the now-zeroed file,
    // producing a SPARSE / misparsed WAL (silent corruption). The truncate must
    // instead POISON the WAL so a subsequent append fails closed rather than
    // corrupting it. This test injects that post-`set_len(0)` seek fault, then
    // proves the subsequent append (and sync) fail closed.
    #[test]
    fn truncate_post_setlen_seek_failure_poisons_wal_no_sparse_append() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        // Seed a few entries so the OLD end-of-file is well past offset 0.
        for i in 0..3 {
            wal.append(&create_test_mutation(i, &format!("Old{i}")))
                .unwrap();
        }
        wal.sync().unwrap();
        assert!(wal.size() > 0);

        // Arm the fault: set_len(0) + sync succeed, but the seek back to 0 fails,
        // leaving the cursor at the stale OLD end-of-file.
        wal.set_fail_seek_after_truncate(true);
        let err = wal
            .truncate_checked()
            .expect_err("post-set_len(0) seek fault must surface");
        assert!(
            matches!(err, TruncateError::AfterMutation(_)),
            "a failure after set_len(0) must be AfterMutation (WAL already zeroed)"
        );

        // The WAL is now poisoned. Disarm the seek fault to prove the fail-closed
        // behavior comes from the poison state, NOT the injected fault.
        wal.set_fail_seek_after_truncate(false);

        // A subsequent append MUST fail closed rather than silently write at the
        // stale cursor and create a sparse WAL.
        let append_err = wal
            .append(&create_test_mutation(42, "New"))
            .expect_err("append on a poisoned WAL must fail closed");
        assert!(
            matches!(append_err, Error::Storage(_)),
            "poisoned-WAL append must return a typed storage error, got {append_err:?}"
        );

        // sync must likewise fail closed.
        assert!(
            wal.sync().is_err(),
            "sync on a poisoned WAL must fail closed"
        );

        // And re-truncation must fail closed as AfterMutation (untrustworthy).
        assert!(
            matches!(wal.truncate_checked(), Err(TruncateError::AfterMutation(_))),
            "truncate on a poisoned WAL must fail closed as AfterMutation"
        );

        // The on-disk WAL is empty (set_len(0) succeeded) and no corrupting
        // append occurred, so replay from a freshly opened handle yields nothing
        // — proving no sparse / misparsed entry was written.
        let wal_path = wal.path().to_path_buf();
        drop(wal);
        let reopened = WriteAheadLog::open_existing(&wal_path).unwrap();
        assert_eq!(
            reopened.replay().unwrap().mutations.len(),
            0,
            "no sparse / lost / misparsed entry may exist after a poisoned truncate"
        );
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
    fn test_wal_decodes_layout_e_pre_1538_write_with_ttl_preserving_trailing_fields() {
        // Layout (E): post-#1018 / pre-#1538. Issue #1538 appended
        // `local_deletion_time: Option<i32>` to `CellOperation::WriteWithTtl`
        // (inside `operations: Vec<CellOperation>`). A record written by the
        // immediately-preceding binary carries ALL current mutation-level trailing
        // fields (`local_deletion_time`, `row_tombstone`, `cell_write_timestamps`)
        // but encodes `WriteWithTtl` with the pre-#1538 3-field shape. Decoding it
        // as the current `Mutation` (whose `WriteWithTtl` now expects a 4th field)
        // misaligns.
        //
        // The `WriteWithTtl` is placed FIRST, followed by a `Delete` carrying an
        // explicit post-#921 `local_deletion_time: Some(..)`. This exercises two
        // things at once: (1) the missing per-op LDT byte genuinely misaligns a
        // naive current decode of the trailing op, and (2) the mirror must use the
        // post-#921 `Delete { column, local_deletion_time }` shape — reusing the
        // pre-#921 `LegacyCellOperation::Delete { column }` here would misdecode
        // this real mixed record (e.g. `UPDATE … USING TTL SET a=1, b=null`).
        let pre1538 = PreCellLdtWriteTtlMutation {
            table: TableId::new("ks", "tbl"),
            partition_key: PartitionKey::single("id", Value::Integer(17)),
            clustering_key: None,
            operations: vec![
                PreCellLdtWriteTtlCellOperation::WriteWithTtl {
                    column: "session".to_string(),
                    value: Value::Text("abc".to_string()),
                    ttl_seconds: 3600,
                },
                PreCellLdtWriteTtlCellOperation::Delete {
                    column: "dropped_col".to_string(),
                    local_deletion_time: Some(1_710_000_000),
                },
            ],
            timestamp_micros: 1_710_000_000_000_000,
            ttl_seconds: None,
            partition_tombstone: None,
            range_tombstones: Vec::new(),
            local_deletion_time: Some(1_710_000_001),
            row_tombstone: Some((1_710_000_002_000_000, 1_710_000_002)),
            cell_write_timestamps: Some(
                [("session".to_string(), 1_710_000_003_000_000)]
                    .into_iter()
                    .collect(),
            ),
        };

        // Bytes as written by a post-#1018/pre-#1538 binary.
        let pre1538_bytes = bincode::serialize(&pre1538).unwrap();

        // Sanity: the current `Mutation` layout cannot FAITHFULLY decode these
        // bytes. The pre-#1538 `WriteWithTtl` (first in the list) has no
        // `local_deletion_time` byte, so a current decode either errors or
        // misaligns — it can never recover the two-op mutation faithfully. This is
        // what forces the layout (E) compat path to run (and guards against a
        // silent "succeed-but-wrong" current decode).
        let current_attempt = bincode::deserialize::<Mutation>(&pre1538_bytes);
        let current_recovers_faithfully = matches!(
            &current_attempt,
            Ok(m) if m.operations.len() == 2
                && matches!(&m.operations[0],
                    CellOperation::WriteWithTtl { column, ttl_seconds, .. }
                    if column == "session" && *ttl_seconds == 3600)
                && matches!(&m.operations[1],
                    CellOperation::Delete { column, local_deletion_time }
                    if column == "dropped_col" && *local_deletion_time == Some(1_710_000_000))
                && m.local_deletion_time == Some(1_710_000_001)
                && m.row_tombstone == Some((1_710_000_002_000_000, 1_710_000_002))
        );
        assert!(
            !current_recovers_faithfully,
            "current layout must NOT faithfully decode a layout (E) record; \
             otherwise the (E) compat path is never exercised / a pre-#1538 \
             WriteWithTtl record would decode-but-wrong"
        );

        // The (E) compat layout must recover the record: the pre-#1538
        // WriteWithTtl upgraded to `local_deletion_time: None`, the Delete's
        // post-#921 LDT preserved, and ALL mutation-level trailing fields intact.
        let decoded = decode_mutation(&pre1538_bytes).expect("layout (E) record must decode");
        assert_eq!(decoded.timestamp_micros, 1_710_000_000_000_000);
        assert_eq!(decoded.local_deletion_time, Some(1_710_000_001));
        assert_eq!(
            decoded.row_tombstone,
            Some((1_710_000_002_000_000, 1_710_000_002))
        );
        assert_eq!(
            decoded
                .cell_write_timestamps
                .as_ref()
                .and_then(|m| m.get("session").copied()),
            Some(1_710_000_003_000_000)
        );
        assert_eq!(decoded.operations.len(), 2);
        match &decoded.operations[0] {
            CellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
                local_deletion_time,
            } => {
                assert_eq!(column, "session");
                assert_eq!(value, &Value::Text("abc".to_string()));
                assert_eq!(*ttl_seconds, 3600);
                // pre-#1538 WriteWithTtl had no surfaced source LDT → None.
                assert_eq!(*local_deletion_time, None);
            }
            other => panic!("expected WriteWithTtl, got {other:?}"),
        }
        match &decoded.operations[1] {
            CellOperation::Delete {
                column,
                local_deletion_time,
            } => {
                assert_eq!(column, "dropped_col");
                // The Delete's post-#921 per-cell LDT must survive — a pre-#921
                // LegacyCellOperation mirror would have corrupted this.
                assert_eq!(*local_deletion_time, Some(1_710_000_000));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn test_wal_decodes_layout_c_pre_1538_write_with_ttl_no_row_tombstone() {
        // Layout (C): post-#921 / pre-#932 (and therefore also pre-#1538). The
        // mutation-level `local_deletion_time` trailing field is PRESENT, but the
        // record predates #932's trailing `row_tombstone` AND #1018's trailing
        // `cell_write_timestamps`, so NEITHER trailing field exists on disk. Its
        // `WriteWithTtl` op is the pre-#1538 3-field shape, while its `Delete`
        // carries the post-#921 `local_deletion_time`. Serialized here with the
        // production (C) mirror, which now IS this exact era shape (3-field
        // `WriteWithTtl` ops via `PreCellLdtWriteTtlCellOperation`, no trailing
        // row-tombstone / cell-write-timestamps).
        let era_c = PreRowTombstoneMutation {
            table: TableId::new("ks", "tbl"),
            partition_key: PartitionKey::single("id", Value::Integer(21)),
            clustering_key: None,
            operations: vec![
                PreCellLdtWriteTtlCellOperation::WriteWithTtl {
                    column: "session".to_string(),
                    value: Value::Text("cee".to_string()),
                    ttl_seconds: 900,
                },
                PreCellLdtWriteTtlCellOperation::Delete {
                    column: "dropped_col".to_string(),
                    local_deletion_time: Some(1_650_000_000),
                },
            ],
            timestamp_micros: 1_650_000_000_000_000,
            ttl_seconds: None,
            partition_tombstone: None,
            range_tombstones: Vec::new(),
            local_deletion_time: Some(1_650_000_001),
        };

        // Bytes as written by a post-#921/pre-#932 binary.
        let era_c_bytes = bincode::serialize(&era_c).unwrap();

        // Sanity: the current `Mutation` layout cannot FAITHFULLY decode these
        // bytes. The pre-#1538 3-field `WriteWithTtl` (first in the list) has no
        // per-op `local_deletion_time` byte, so a current decode misaligns and can
        // never recover the two-op mutation faithfully. This is what makes the
        // silent-corruption gap real, and forces a compat mirror to run.
        let current_attempt = bincode::deserialize::<Mutation>(&era_c_bytes);
        let current_recovers_faithfully = matches!(
            &current_attempt,
            Ok(m) if m.operations.len() == 2
                && matches!(&m.operations[0],
                    CellOperation::WriteWithTtl { column, ttl_seconds, .. }
                    if column == "session" && *ttl_seconds == 900)
                && matches!(&m.operations[1],
                    CellOperation::Delete { column, local_deletion_time }
                    if column == "dropped_col" && *local_deletion_time == Some(1_650_000_000))
        );
        assert!(
            !current_recovers_faithfully,
            "current layout must NOT faithfully decode a layout (C) record; \
             otherwise a pre-#1538 WriteWithTtl would decode-but-wrong (silent \
             corruption gap)"
        );

        // The (C) mirror must recover the record: the pre-#1538 `WriteWithTtl`
        // upgraded to `local_deletion_time: None`, the `Delete`'s post-#921 LDT
        // preserved, mutation LDT preserved, and — critically for ladder ordering
        // — NO trailing `row_tombstone` / `cell_write_timestamps` (both None),
        // proving the record landed on (C), not the more-trailing-field (D)/(E)
        // mirrors.
        let decoded = decode_mutation(&era_c_bytes).expect("layout (C) record must decode");
        assert_eq!(decoded.timestamp_micros, 1_650_000_000_000_000);
        assert_eq!(decoded.local_deletion_time, Some(1_650_000_001));
        assert_eq!(decoded.row_tombstone, None);
        assert_eq!(decoded.cell_write_timestamps, None);
        assert_eq!(decoded.operations.len(), 2);
        match &decoded.operations[0] {
            CellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
                local_deletion_time,
            } => {
                assert_eq!(column, "session");
                assert_eq!(value, &Value::Text("cee".to_string()));
                assert_eq!(*ttl_seconds, 900);
                assert_eq!(*local_deletion_time, None);
            }
            other => panic!("expected WriteWithTtl, got {other:?}"),
        }
        match &decoded.operations[1] {
            CellOperation::Delete {
                column,
                local_deletion_time,
            } => {
                assert_eq!(column, "dropped_col");
                assert_eq!(*local_deletion_time, Some(1_650_000_000));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn test_wal_decodes_layout_d_pre_1538_write_with_ttl_with_row_tombstone() {
        // Layout (D): post-#932 / pre-#1018 (and therefore also pre-#1538). The
        // mutation-level `local_deletion_time` AND the #932 trailing
        // `row_tombstone` are PRESENT, but the record predates #1018's trailing
        // `cell_write_timestamps` (absent on disk). Its `WriteWithTtl` op is the
        // pre-#1538 3-field shape, while its `Delete` carries the post-#921
        // `local_deletion_time`. Serialized here with the production (D) mirror,
        // which now IS this exact era shape (3-field `WriteWithTtl` ops, trailing
        // `row_tombstone` but no `cell_write_timestamps`).
        let era_d = PreCellWriteTimestampsMutation {
            table: TableId::new("ks", "tbl"),
            partition_key: PartitionKey::single("id", Value::Integer(23)),
            clustering_key: None,
            operations: vec![
                PreCellLdtWriteTtlCellOperation::WriteWithTtl {
                    column: "session".to_string(),
                    value: Value::Text("dee".to_string()),
                    ttl_seconds: 1800,
                },
                PreCellLdtWriteTtlCellOperation::Delete {
                    column: "dropped_col".to_string(),
                    local_deletion_time: Some(1_680_000_000),
                },
            ],
            timestamp_micros: 1_680_000_000_000_000,
            ttl_seconds: None,
            partition_tombstone: None,
            range_tombstones: Vec::new(),
            local_deletion_time: Some(1_680_000_001),
            row_tombstone: Some((1_680_000_002_000_000, 1_680_000_002)),
        };

        // Bytes as written by a post-#932/pre-#1018 binary.
        let era_d_bytes = bincode::serialize(&era_d).unwrap();

        // Sanity: the current `Mutation` layout cannot FAITHFULLY decode these
        // bytes — the pre-#1538 3-field `WriteWithTtl` (first op) misaligns the
        // decode (silent-corruption gap).
        let current_attempt = bincode::deserialize::<Mutation>(&era_d_bytes);
        let current_recovers_faithfully = matches!(
            &current_attempt,
            Ok(m) if m.operations.len() == 2
                && matches!(&m.operations[0],
                    CellOperation::WriteWithTtl { column, ttl_seconds, .. }
                    if column == "session" && *ttl_seconds == 1800)
                && matches!(&m.operations[1],
                    CellOperation::Delete { column, local_deletion_time }
                    if column == "dropped_col" && *local_deletion_time == Some(1_680_000_000))
                && m.row_tombstone == Some((1_680_000_002_000_000, 1_680_000_002))
        );
        assert!(
            !current_recovers_faithfully,
            "current layout must NOT faithfully decode a layout (D) record; \
             otherwise a pre-#1538 WriteWithTtl would decode-but-wrong (silent \
             corruption gap)"
        );

        // The (D) mirror must recover the record: `WriteWithTtl` LDT upgraded to
        // None, `Delete` LDT preserved, mutation LDT preserved, the #932
        // `row_tombstone` preserved, and — proving it landed on (D) not (E) — NO
        // trailing `cell_write_timestamps` (None).
        let decoded = decode_mutation(&era_d_bytes).expect("layout (D) record must decode");
        assert_eq!(decoded.timestamp_micros, 1_680_000_000_000_000);
        assert_eq!(decoded.local_deletion_time, Some(1_680_000_001));
        assert_eq!(
            decoded.row_tombstone,
            Some((1_680_000_002_000_000, 1_680_000_002))
        );
        assert_eq!(decoded.cell_write_timestamps, None);
        assert_eq!(decoded.operations.len(), 2);
        match &decoded.operations[0] {
            CellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds,
                local_deletion_time,
            } => {
                assert_eq!(column, "session");
                assert_eq!(value, &Value::Text("dee".to_string()));
                assert_eq!(*ttl_seconds, 1800);
                assert_eq!(*local_deletion_time, None);
            }
            other => panic!("expected WriteWithTtl, got {other:?}"),
        }
        match &decoded.operations[1] {
            CellOperation::Delete {
                column,
                local_deletion_time,
            } => {
                assert_eq!(column, "dropped_col");
                assert_eq!(*local_deletion_time, Some(1_680_000_000));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn test_wal_current_write_with_ttl_ldt_decodes_via_current_layout() {
        // Guard the other direction: a CURRENT record whose `WriteWithTtl` carries
        // an explicit `local_deletion_time: Some(..)` (the #1538 compaction path)
        // must still decode via the current layout (layout 1) and round-trip that
        // LDT verbatim — adding the layout (E) fallback must NOT regress it.
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        let mut mutation = Mutation::new(
            TableId::new("ks", "tbl"),
            PartitionKey::single("id", Value::Integer(19)),
            None,
            vec![CellOperation::WriteWithTtl {
                column: "session".to_string(),
                value: Value::Text("xyz".to_string()),
                ttl_seconds: 7200,
                local_deletion_time: Some(1_720_007_200),
            }],
            1_720_000_000_000_000,
            None,
        );
        mutation.local_deletion_time = Some(1_720_000_000);
        wal.append(&mutation).unwrap();
        wal.sync().unwrap();

        let mutations = wal.replay().unwrap().mutations;
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].local_deletion_time, Some(1_720_000_000));
        match &mutations[0].operations[0] {
            CellOperation::WriteWithTtl {
                column,
                ttl_seconds,
                local_deletion_time,
                ..
            } => {
                assert_eq!(column, "session");
                assert_eq!(*ttl_seconds, 7200);
                assert_eq!(*local_deletion_time, Some(1_720_007_200));
            }
            other => panic!("expected WriteWithTtl, got {other:?}"),
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

    /// Criterion 1 (updated for issue #1391 r5): a COMPLETE-header entry with a
    /// short payload is NOT a clean torn tail. A valid A + B with a complete
    /// 8-byte header but a truncated payload is structurally indistinguishable
    /// from a length bit-flip that overshoots EOF (both: complete header,
    /// declared length > remaining bytes, at the physical tail), so open_existing
    /// must PRESERVE it (leave the bytes on disk, record a pending valid prefix)
    /// rather than silently trimming it back to A and reporting clean. Only a
    /// torn HEADER (< 8 bytes, see the torn-header criterion) is trimmed as clean.
    #[test]
    fn test_wal_open_existing_preserves_complete_header_short_payload() {
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

        // The complete-header short-payload entry must be PRESERVED, not trimmed:
        // the file keeps its bytes and a pending valid prefix is recorded.
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            torn_len,
            "a complete-header short-payload entry must be preserved on disk, not trimmed"
        );
        assert!(
            wal.has_pending_corrupt_tail(),
            "a complete-header short-payload entry must record a pending valid prefix"
        );

        // Replay recovers only A and reports the loss (not clean).
        let report = wal.replay().unwrap();
        assert_eq!(report.mutations.len(), 1, "only A is recovered");
        assert!(
            !report.is_clean(),
            "the dropped complete-header entry is lossy"
        );
        assert!(report.stopped_early);
    }

    /// Criterion 2: post-reopen writes recoverable. Continuing criterion 1 —
    /// after reopen the complete-header short-payload B is preserved pending, so
    /// (issue #1391 r5) the caller must reset to the valid prefix before
    /// appending C; replay must then yield exactly [A, C] with C PRESENT. This is
    /// the guarantee that C lands at a replayable position, not after retained
    /// torn bytes where it would be lost on the next replay.
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

        // Reopen: B (complete header, short payload) is preserved pending. Reset
        // to the valid prefix (end of A) so the append lands at a replayable
        // position, then append C, sync, drop.
        {
            let mut wal = WriteAheadLog::open_existing(&path).unwrap();
            let reset_to = wal.reset_to_valid_prefix().unwrap();
            assert_eq!(reset_to, Some(end_of_a), "reset trims back to the end of A");
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

    /// Issue #1391 (roborev r4): discarding a pending corrupt tail via `truncate()`
    /// must lift the fail-closed append guard. `truncate()` clears the file to zero
    /// length; an empty WAL has no corrupt tail, so `pending_valid_prefix` must not
    /// remain set — otherwise every subsequent `append()` is rejected forever
    /// (deadlocked appends) even though the live log is empty. Red-before: the guard
    /// stayed set after truncate and the append errored. Green-after: the guard is
    /// cleared and the append succeeds and replays.
    #[test]
    fn test_wal_truncate_clears_pending_corrupt_tail_guard() {
        let temp_dir = TempDir::new().unwrap();
        let (path, end_of_a, total) = write_two_entries(&temp_dir);

        // Corrupt B's payload in place so `open_existing` records a pending corrupt
        // tail (valid prefix ends at A) rather than trimming a torn tail.
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

        let mut wal = WriteAheadLog::open_existing(&path).unwrap();
        assert!(
            wal.has_pending_corrupt_tail(),
            "mid-stream corruption must leave a pending valid prefix"
        );

        // Discard the corrupt tail wholesale via truncate() (as a direct WAL user
        // choosing to drop the log rather than reset-to-prefix).
        wal.truncate().unwrap();

        // After truncate the live WAL is empty, so the fail-closed guard MUST be
        // lifted (red-before: guard stayed set here).
        assert!(
            !wal.has_pending_corrupt_tail(),
            "truncate() must clear the pending corrupt-tail guard on an emptied WAL"
        );
        assert_eq!(wal.size(), 0, "truncate() must reset current_size to zero");

        // A subsequent append must succeed (red-before: rejected by the stale guard)
        // and land at offset 0 in the emptied log.
        wal.append(&create_test_mutation(3, "Carol")).unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Fresh reopen + replay must recover EXACTLY [C]: the truncate dropped both
        // A and the corrupt B, and C landed at the start of the emptied log.
        let wal = WriteAheadLog::open_existing(&path).unwrap();
        let report = wal.replay().unwrap();
        assert!(
            report.is_clean(),
            "recovery must be clean after truncate + post-truncate append"
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
        assert_eq!(names, vec!["Carol"], "replay must yield exactly [C]");
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

    /// Append A, B, C (all synced); return (path, end_of_A, end_of_B, total).
    fn write_three_entries(temp_dir: &TempDir) -> (PathBuf, u64, u64, u64) {
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();

        wal.append(&create_test_mutation(1, "Alice")).unwrap();
        wal.sync().unwrap();
        let end_of_a = wal.size();

        wal.append(&create_test_mutation(2, "Bob")).unwrap();
        wal.sync().unwrap();
        let end_of_b = wal.size();

        wal.append(&create_test_mutation(3, "Carol")).unwrap();
        wal.sync().unwrap();
        let total = wal.size();

        let path = wal.path().to_path_buf();
        drop(wal);
        (path, end_of_a, end_of_b, total)
    }

    /// Issue #1391 (roborev r5, HIGH): a COMPLETE 8-byte header whose declared
    /// length is `<= MAX_ENTRY_LENGTH` but LARGER than the bytes remaining to EOF
    /// must be classified as CORRUPTION, not a torn tail. For an A,B,C log where
    /// B's length field is bit-flipped to a plausible-but-too-large value that
    /// runs past EOF, the OLD classifier read B's payload, hit EOF, called it a
    /// `TornTail`, trimmed the live WAL back to A, and reported a CLEAN recovery —
    /// silently discarding B AND C (an acknowledged, durable entry). That is the
    /// exact "silent lossy recovery reported as clean" #1391 exists to eliminate.
    ///
    /// A complete-header entry that cannot be satisfied by the remaining bytes is
    /// structurally INDISTINGUISHABLE from an interrupted final write using only
    /// the allowed no-heuristics facts (header completeness, declared-length vs
    /// remaining-bytes, physical-tail position); guessing which it is would be a
    /// byte-pattern heuristic. The safe, consistent rule therefore preserves the
    /// tail + reports it lossy rather than silent-trimming as clean.
    ///
    /// Verify: NOT reported clean; the B/C tail is preserved on disk (not trimmed)
    /// for the caller to copy aside; the report is lossy (`stopped_early`, a
    /// corrupt entry, `bytes_skipped > 0`); only A is recovered. Red-before: the
    /// OLD `TornTail` path trimmed to A + reported clean, so `is_clean()` was true
    /// and the file was truncated to `end_of_A`.
    #[test]
    fn test_wal_midstream_length_past_eof_is_lossy_not_clean() {
        let temp_dir = TempDir::new().unwrap();
        let (path, end_of_a, _end_of_b, total) = write_three_entries(&temp_dir);

        // Remaining payload bytes after B's 8-byte header, all the way to EOF:
        // B's real payload + C's full framing. Bit-flip B's length UP to a value
        // that overshoots EOF (> remaining) yet stays within MAX_ENTRY_LENGTH, so
        // the payload read hits EOF (the misclassified-as-torn path) rather than
        // the oversize-length path.
        let remaining_after_b_header = total - (end_of_a + 8);
        let bogus_len = remaining_after_b_header + 4096;
        assert!(
            bogus_len <= MAX_ENTRY_LENGTH as u64,
            "test setup: bogus length must stay within MAX_ENTRY_LENGTH so it \
             exercises the past-EOF path, not the oversize path"
        );
        assert!(
            bogus_len > remaining_after_b_header,
            "test setup: bogus length must overshoot EOF"
        );
        {
            let mut bytes = std::fs::read(&path).unwrap();
            let len_field = (end_of_a as usize)..(end_of_a as usize + 4);
            bytes[len_field].copy_from_slice(&(bogus_len as u32).to_le_bytes());
            std::fs::write(&path, &bytes).unwrap();
        }

        // Open: the corrupt segment (B + C) must be left INTACT on disk (evidence
        // preservation) with the valid prefix (end of A) recorded as pending, not
        // silently trimmed away.
        let wal = WriteAheadLog::open_existing(&path).unwrap();
        assert!(
            wal.has_pending_corrupt_tail(),
            "a complete-header entry with an unsatisfiable length must be preserved \
             pending (not silently trimmed as a torn tail)"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            total,
            "the B/C tail must remain on disk for evidence preservation"
        );

        // Replay must NOT be clean: A recovered, B/C dropped and surfaced.
        let report = wal.replay().unwrap();
        assert!(
            !report.is_clean(),
            "a length-past-EOF middle entry must NOT report a clean recovery"
        );
        assert!(
            report.stopped_early,
            "replay must stop at the corrupt frame"
        );
        assert_eq!(report.corrupt_entries, 1, "the mis-sized entry is corrupt");
        assert!(
            report.bytes_skipped > 0,
            "the discarded B/C tail must be counted as skipped, not lost silently"
        );
        assert_eq!(
            report.mutations.len(),
            1,
            "only the valid prefix A survives"
        );
        match &report.mutations[0].operations[0] {
            CellOperation::Write {
                value: Value::Text(name),
                ..
            } => assert_eq!(name, "Alice", "the sole recovered entry must be A"),
            other => panic!("expected Write op, got {other:?}"),
        }
    }

    /// Issue #1661: `replay_each` must be semantically identical to `replay()` —
    /// it must yield the SAME mutations in the SAME order and produce the SAME
    /// corruption metadata for a mixed WAL (valid entries plus a CRC-corrupt
    /// entry). This proves the streaming refactor changed only allocation
    /// behavior, not decode/skip/stop semantics.
    #[test]
    fn test_replay_each_matches_replay_for_mixed_wal() {
        let temp_dir = TempDir::new().unwrap();

        // [A][B][C], all synced. Capture the end-of-A offset (start of B's frame).
        let (path, end_of_a) = {
            let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();
            wal.append(&create_test_mutation(1, "Alice")).unwrap();
            wal.sync().unwrap();
            let end_of_a = wal.size();
            wal.append(&create_test_mutation(2, "Bob")).unwrap();
            wal.sync().unwrap();
            wal.append(&create_test_mutation(3, "Carol")).unwrap();
            wal.sync().unwrap();
            let path = wal.path().to_path_buf();
            drop(wal);
            (path, end_of_a)
        };

        // Corrupt B's CRC (4 bytes at end_of_a + 4) via a bit flip; B stays
        // structurally complete so this exercises the CRC-mismatch stop path.
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

        let wal = WriteAheadLog::open_existing(&path).unwrap();

        // Reference: the whole-log `replay()`.
        let report = wal.replay().unwrap();

        // Streaming: collect the callback-delivered mutations.
        let mut streamed = Vec::new();
        let stream_report = wal
            .replay_each(|m| {
                streamed.push(m);
                Ok(())
            })
            .unwrap();

        // Same mutations, same order (compare via bincode bytes — Mutation has no
        // PartialEq).
        let ser = |m: &Mutation| bincode::serialize(m).unwrap();
        let expected: Vec<Vec<u8>> = report.mutations.iter().map(ser).collect();
        let actual: Vec<Vec<u8>> = streamed.iter().map(ser).collect();
        assert_eq!(
            actual, expected,
            "replay_each must yield the same mutations in the same order as replay()"
        );
        // Only the valid prefix A survives the mid-stream corruption.
        assert_eq!(streamed.len(), 1, "only the valid prefix A is recovered");

        // Same corruption metadata; `replay_each`'s report leaves mutations empty
        // (they were streamed to the callback), while `replay()` populates them.
        assert_eq!(stream_report.corrupt_entries, report.corrupt_entries);
        assert_eq!(stream_report.stopped_early, report.stopped_early);
        assert_eq!(stream_report.bytes_skipped, report.bytes_skipped);
        assert!(
            stream_report.stopped_early,
            "must stop at the corrupt entry"
        );
        assert_eq!(stream_report.corrupt_entries, 1);
        assert!(
            stream_report.mutations.is_empty(),
            "replay_each streams mutations to the callback, so its report Vec stays empty"
        );
    }

    /// Issue #1661: a fully clean multi-entry WAL must stream every entry in
    /// order and report clean, matching `replay()`.
    #[test]
    fn test_replay_each_clean_multi_entry() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::create(temp_dir.path()).unwrap();
        wal.append(&create_test_mutation(1, "Alice")).unwrap();
        wal.append(&create_test_mutation(2, "Bob")).unwrap();
        wal.append(&create_test_mutation(3, "Carol")).unwrap();
        wal.sync().unwrap();

        let report = wal.replay().unwrap();
        let mut streamed = Vec::new();
        let stream_report = wal
            .replay_each(|m| {
                streamed.push(m);
                Ok(())
            })
            .unwrap();

        let ser = |m: &Mutation| bincode::serialize(m).unwrap();
        let expected: Vec<Vec<u8>> = report.mutations.iter().map(ser).collect();
        let actual: Vec<Vec<u8>> = streamed.iter().map(ser).collect();
        assert_eq!(actual, expected);
        assert_eq!(streamed.len(), 3);
        assert!(stream_report.is_clean());
        assert!(report.is_clean());
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
