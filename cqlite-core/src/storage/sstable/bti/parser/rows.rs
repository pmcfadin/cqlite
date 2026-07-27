//! `Rows.db` in-trie payload decoding (`RowIndexReader` / `TrieIndexEntry`).
//!
//! IMPORTANT: `Rows.db` in-trie payloads are NOT the `Partitions.db` payload
//! format (which is a hash byte + SizedInts *signed* position).  A `Rows.db`
//! trie leaf carries a `RowIndexReader.IndexInfo` whose byte layout is defined
//! authoritatively by `RowIndexReader.readPayload`
//! (cassandra-5.0.0 `RowIndexReader.java:111-125`):
//!
//!   static IndexInfo readPayload(ByteBuffer buf, int ppos, int bits, Version v) {
//!       if (bits == 0) return null;
//!       int bytes = bits & ~FLAG_OPEN_MARKER;            // FLAG_OPEN_MARKER = 8
//!       long offset = SizedInts.read(buf, ppos, bytes);  // SizedInts, NOT a vint
//!       ppos += bytes;
//!       DeletionTime del = (bits & FLAG_OPEN_MARKER) != 0
//!                          ? DeletionTime.deserialize(buf, ppos) : null;
//!       return new IndexInfo(offset, del);
//!   }
//!
//! So the low nibble of the node header byte (`payloadBits`) splits as:
//!   - low 3 bits  → the number of `SizedInts` bytes encoding the block offset
//!   - bit 0x8     → FLAG_OPEN_MARKER: an open-deletion `DeletionTime` follows
//!
//! The `offset` field is the block's offset **relative to the partition start**
//! in `Data.db`, so absolute Data.db position = `entry.data_position + offset`.
//!
//! Reference: docs/sstables-definitive-guide chapter 17 (Rows.db footer);
//!            cassandra-5.0.0 `RowIndexReader.java`, `RowIndexWriter.java`,
//!            `TrieIndexEntry.java`, `SizedInts.java`.

use crate::{error::Error, storage::sstable::bti::node::BtiResult};
use std::io::{Read, Seek, SeekFrom};

use super::partitions::sized_ints_read_from_slice;
use super::rows_root::{validate_rows_trie_root, RowsTrieRootRejection, ValidatedRowsTrieRoot};
use super::traversal::{dfs_collect_in_order, load_bti_trie_via_footer};

#[allow(unused_imports)] // referenced in doc-links
use super::partitions::BtiPartitionLocation;

/// The `FLAG_OPEN_MARKER` bit in a `Rows.db` trie node's `payloadBits`
/// (low nibble of the header byte).  When set, an open-deletion `DeletionTime`
/// follows the `SizedInts` block offset.  Mirrors
/// `RowIndexReader.FLAG_OPEN_MARKER`.
pub const FLAG_OPEN_MARKER: u8 = 0x8;

/// A decoded `Rows.db` in-trie row-index block entry (`RowIndexReader.IndexInfo`).
///
/// The headline field is [`data_offset`](Self::data_offset): the block's offset
/// **relative to the partition start** in `Data.db`.  To obtain the absolute
/// `Data.db` byte position, add the partition's data position (see
/// [`BtiRowIndexHeader::data_position`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtiRowIndexEntry {
    /// Block offset **relative to the partition start**, decoded via
    /// `SizedInts.read(buf, ppos, payloadBits & ~FLAG_OPEN_MARKER)`.
    pub data_offset: u64,
    /// Open-deletion time `(local_deletion_time, marked_for_delete_at)`,
    /// present only when the `FLAG_OPEN_MARKER` payload bit is set.
    pub open_marker: Option<(i32, i64)>,
}

/// Read an unsigned VInt (Cassandra count-leading-ones encoding, **not** ZigZag)
/// from `data`, returning `(value, bytes_consumed)`.
///
/// This is the encoding Cassandra uses for Data.db positions in the row index
/// (`DataOutputPlus.writeUnsignedVInt`).  The number of extra bytes equals the
/// number of leading 1-bits in the first byte; the value is big-endian across
/// the remaining bits.
fn read_unsigned_vint_from_slice(data: &[u8]) -> BtiResult<(u64, usize)> {
    if data.is_empty() {
        return Err(Error::Parse(
            "Rows.db payload: unexpected end of data reading unsigned vint".to_string(),
        ));
    }
    let first = data[0];
    let extra_bytes = first.leading_ones() as usize;
    if extra_bytes > 8 {
        return Err(Error::Parse(format!(
            "Rows.db payload: invalid unsigned vint first byte 0x{first:02x}"
        )));
    }
    let total = extra_bytes + 1;
    if data.len() < total {
        return Err(Error::Parse(format!(
            "Rows.db payload: unsigned vint needs {total} bytes, have {}",
            data.len()
        )));
    }

    // Data bits in the first byte: 8 - extra_bytes - 1 (the separator 0 bit),
    // except when extra_bytes == 8 (first byte is all ones, no data bits).
    let mut value: u64 = if extra_bytes >= 8 {
        0
    } else {
        let data_bits = 8 - extra_bytes - 1;
        let mask = if data_bits == 0 {
            0
        } else {
            (1u16 << data_bits) - 1
        };
        (first as u16 & mask) as u64
    };
    for &b in &data[1..total] {
        value = (value << 8) | (b as u64);
    }
    Ok((value, total))
}

/// Read a signed VInt (Cassandra zig-zag, `DataInputPlus.readVInt`) from `data`,
/// returning `(value, bytes_consumed)`.
fn read_signed_vint_from_slice(data: &[u8]) -> BtiResult<(i64, usize)> {
    let (u, n) = read_unsigned_vint_from_slice(data)?;
    // ZigZag decode: (u >>> 1) ^ -(u & 1)
    let value = ((u >> 1) as i64) ^ -((u & 1) as i64);
    Ok((value, n))
}

/// Test-only re-export of [`read_unsigned_vint_from_slice`] so the BTI `Rows.db`
/// writer (`writer::partitions_writer`) can assert its unsigned-VInt encoder is
/// the exact inverse of this reader decoder.
#[doc(hidden)]
pub fn read_unsigned_vint_from_slice_for_test(data: &[u8]) -> BtiResult<(u64, usize)> {
    read_unsigned_vint_from_slice(data)
}

/// Test-only re-export of [`read_signed_vint_from_slice`].
#[doc(hidden)]
pub fn read_signed_vint_from_slice_for_test(data: &[u8]) -> BtiResult<(i64, usize)> {
    read_signed_vint_from_slice(data)
}

/// The modern (DA/BTI) `DeletionTime` "live" sentinel byte.
///
/// In the `da`-family on-disk serializer, a `DeletionTime` written by the BTI
/// row-index / trie-index path is encoded as a single `0x80` byte when it is
/// `DeletionTime.LIVE` (no deletion), and otherwise as the full value (see
/// [`decode_da_deletion_time`]).
const DA_DELETION_TIME_LIVE_SENTINEL: u8 = 0x80;

/// Width of a non-live modern (DA) `DeletionTime` body: `i64 markedForDeleteAt`
/// followed by `u32 localDeletionTime`.
const DA_DELETION_TIME_BODY_LEN: usize = 12;

/// Decode a modern (DA/BTI) `DeletionTime` at `data[start..]`, returning
/// `(deletion, bytes_consumed)` where `deletion` is `None` for the LIVE
/// sentinel (issue #832 Finding 2).
///
/// Layout (mirrors `org.apache.cassandra.db.DeletionTime.Serializer` in the
/// `da`/trie-index format, cassandra-5.0.0):
///
///   - a single `0x80` byte → `DeletionTime.LIVE` (no deletion); consumes 1 byte.
///   - otherwise the body is `[markedForDeleteAt : i64 BE][localDeletionTime :
///     u32 BE]` — `markedForDeleteAt` FIRST, then `localDeletionTime`; consumes
///     12 bytes.  This differs from the LEGACY layout in BOTH field order and
///     the width/signedness of `localDeletionTime` (modern: `u32`).
///
/// Returns the deletion as `(local_deletion_time, marked_for_delete_at)` to
/// match [`BtiRowIndexEntry::open_marker`]'s existing tuple ordering, even
/// though the modern wire order is the reverse.
///
/// # Errors
/// Returns a parse error if `start` is out of bounds or a non-live value is
/// truncated.
fn decode_da_deletion_time(data: &[u8], start: usize) -> BtiResult<(Option<(i32, i64)>, usize)> {
    if start >= data.len() {
        return Err(Error::Parse(format!(
            "DA DeletionTime: start {start} beyond buffer size {}",
            data.len()
        )));
    }
    if data[start] == DA_DELETION_TIME_LIVE_SENTINEL {
        return Ok((None, 1));
    }
    if start + DA_DELETION_TIME_BODY_LEN > data.len() {
        return Err(Error::Parse(format!(
            "DA DeletionTime: non-live value needs {DA_DELETION_TIME_BODY_LEN} bytes, have {}",
            data.len().saturating_sub(start)
        )));
    }
    let b = &data[start..start + DA_DELETION_TIME_BODY_LEN];
    // markedForDeleteAt FIRST (i64), then localDeletionTime (u32).
    let marked_for_delete_at = i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
    let local_deletion_time = u32::from_be_bytes([b[8], b[9], b[10], b[11]]) as i32;
    Ok((
        Some((local_deletion_time, marked_for_delete_at)),
        DA_DELETION_TIME_BODY_LEN,
    ))
}

/// Encoded byte length of the modern (DA/BTI) `DeletionTime` at `data[start..]`
/// (1 for the LIVE sentinel, [`DA_DELETION_TIME_BODY_LEN`] otherwise).
///
/// Shared with [`super::rows_root`], whose node-extent computation needs the
/// payload's length without decoding it, so the LIVE-sentinel/body widths live in
/// exactly one place.
///
/// # Errors
/// Same as [`decode_da_deletion_time`]: out-of-bounds `start` or a truncated
/// non-live value.
pub(super) fn da_deletion_time_encoded_len(data: &[u8], start: usize) -> BtiResult<usize> {
    decode_da_deletion_time(data, start).map(|(_deletion, consumed)| consumed)
}

/// Decode a `Rows.db` in-trie payload (`RowIndexReader.IndexInfo`) at
/// `payload_start` inside `trie_data`, given the node's `payload_bits` (low
/// nibble of the header byte).
///
/// Layout (mirrors `RowIndexReader.readPayload`, cassandra-5.0.0
/// `RowIndexReader.java:111-125`):
///   - `bytes = payload_bits & !FLAG_OPEN_MARKER` → block offset is a
///     `SizedInts` value of `bytes` bytes (the offset is relative to the
///     partition's data position).
///   - if `payload_bits & FLAG_OPEN_MARKER`, an open-deletion `DeletionTime`
///     follows in the MODERN DA form ([`decode_da_deletion_time`]).
///
/// A `payload_bits` of `0` is not a valid leaf payload here (the caller filters
/// such nodes out) and yields an error.
pub fn decode_bti_row_payload(
    trie_data: &[u8],
    payload_start: usize,
    payload_bits: u8,
) -> BtiResult<BtiRowIndexEntry> {
    if payload_start > trie_data.len() {
        return Err(Error::Parse(format!(
            "Rows.db payload start {payload_start} beyond trie size {}",
            trie_data.len()
        )));
    }

    // Low 3 bits = number of SizedInts bytes; bit 0x8 = open-marker flag.
    let offset_bytes = (payload_bits & !FLAG_OPEN_MARKER) as usize;
    if offset_bytes == 0 || offset_bytes > 7 {
        // RowIndexWriter asserts `bytes < 8` ("rows larger than 32 PiB"); a
        // 0-byte offset would mean an empty payload, which the trie does not
        // emit for a real row-index block.
        return Err(Error::Parse(format!(
            "Rows.db payload: invalid SizedInts byte count {offset_bytes} \
             (payload_bits=0x{payload_bits:02x}); expected 1..=7"
        )));
    }
    if payload_start + offset_bytes > trie_data.len() {
        return Err(Error::Parse(format!(
            "Rows.db payload: SizedInts offset needs {offset_bytes} bytes, have {}",
            trie_data.len().saturating_sub(payload_start)
        )));
    }

    // SizedInts is a signed sign-extended big-endian read (SizedInts.read).
    // Block offsets are non-negative in practice, but we decode faithfully.
    let raw = sized_ints_read_from_slice(&trie_data[payload_start..payload_start + offset_bytes])?;
    let data_offset = raw as u64;

    let open_marker = if payload_bits & FLAG_OPEN_MARKER != 0 {
        // DA/BTI modern DeletionTime (issue #832 Finding 2).
        let dt_start = payload_start + offset_bytes;
        let (deletion, _consumed) = decode_da_deletion_time(trie_data, dt_start)?;
        deletion
    } else {
        None
    };

    Ok(BtiRowIndexEntry {
        data_offset,
        open_marker,
    })
}

/// Enumerate every row-index entry in a `Rows.db` trie (rooted at `root_offset`)
/// in byte-comparable order: `(reconstructed_clustering_key, BtiRowIndexEntry)`.
///
/// The per-node payload primitive lives in [`super::rows_floor`], alongside the
/// O(key-length) floor/ceiling walks that share it (issue #1647 / L1).
pub(crate) fn dfs_collect_row_entries(
    trie_data: &[u8],
    root_offset: usize,
) -> BtiResult<Vec<(Vec<u8>, BtiRowIndexEntry)>> {
    dfs_collect_in_order(trie_data, root_offset, |data, off, node| {
        super::rows_floor::read_row_node_payload(data, off, Some(node))
    })
}

/// Enumerate every row-index entry in a `Rows.db` row-index trie **rooted at an
/// explicit `root_offset`**, in byte-comparable order
/// (`(reconstructed_clustering_key, BtiRowIndexEntry)`).
///
/// ## Why the root must be supplied by the caller
///
/// A real Cassandra 5.0 `Rows.db` is NOT a single whole-file trie: it holds
/// **many independent per-partition row-index tries** concatenated together.
/// There is one row-index trie per (wide) partition, and the root of a given
/// partition's trie is the `RowsOffset` returned from the corresponding
/// `Partitions.db` lookup ([`BtiPartitionLocation::RowsOffset`]) — it is NOT the
/// 8-byte file footer, which spans the whole file and would misparse any
/// multi-partition `Rows.db`.
///
/// This is therefore the correct general entry point: pass the full `Rows.db`
/// bytes as `trie_data` and the partition's `RowsOffset` as `root_offset`.
///
/// An out-of-bounds `root_offset` (e.g. on empty `trie_data`) yields a clean
/// parse error rather than a panic.
pub fn iterate_rows_in_bti_trie(
    trie_data: &[u8],
    root_offset: usize,
) -> BtiResult<Vec<(Vec<u8>, BtiRowIndexEntry)>> {
    dfs_collect_row_entries(trie_data, root_offset)
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-partition Rows.db entry resolution — TrieIndexEntry (issue #832 Finding A)
// ─────────────────────────────────────────────────────────────────────────────
//
// The positive `position` stored in a `Partitions.db` leaf payload
// (`BtiPartitionLocation::RowsOffset`) does NOT point at a row-index trie root.
// It points at this partition's **row-index entry** in `Rows.db`, which must be
// deserialized to recover the actual trie root (plus the partition's Data.db
// position, block count and partition-level deletion).
//
// On-disk layout at `RowsOffset`:
//   [u16 key_length][partition key bytes]      ← short-length-prefixed key
//   [data file position : unsigned vint]       ← partition start in Data.db
//   [trie_root - base    : SIGNED vint]        ← base = RowsOffset + 2 + key_length
//   [row index block count : unsigned vint32]
//   [partition DeletionTime]                    ← delta/compact form; best-effort
//
// `TrieIndexEntry.deserialize` computes `indexTrieRoot = readVInt() + base`, where
// `base` is the position AFTER the short-length-prefixed key: cassandra-5.0.8 takes
// it as `rowIndexWriter.position()` after `writeWithShortLength`
// (`BtiTableWriter.IndexWriter.append`) and as `in.getFilePointer()` after
// `readWithShortLength` (`BtiTableReader.retrieveEntryIfAcceptable`) — NOT
// `RowsOffset + key_length`, which is 2 bytes low and drops the root node's own
// (empty-separator = block 0) payload (issue #3002).
// ─────────────────────────────────────────────────────────────────────────────

/// A single `Rows.db` row-index entry paired with its reconstructed
/// byte-comparable clustering separator key, as yielded by the in-order DFS.
pub type BtiRowIndexEntryWithKey = (Vec<u8>, BtiRowIndexEntry);

/// A deserialized per-partition `Rows.db` row-index entry (Cassandra
/// `TrieIndexEntry`).  Produced by [`resolve_rows_db_entry`] from the
/// `RowsOffset` returned by a `Partitions.db` lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtiRowIndexHeader {
    /// Absolute byte position of the partition's start in `Data.db`.  Block
    /// offsets in [`BtiRowIndexEntry::data_offset`] are relative to this.
    ///
    /// Always decoded, INDEPENDENT of the row-index root's validity: the
    /// point-lookup and successor-walk paths consume only this field (plus
    /// [`Self::block_count`]) and are unaffected by an unusable root (issue #3002).
    pub data_position: u64,
    /// This partition's row-index trie ROOT — `Ok` only when the offset the entry's
    /// SIGNED delta resolved to passed the structural validation in
    /// [`super::rows_root`] (issue #3002), else `Err` carrying WHICH invariant it
    /// violated.
    ///
    /// The validated newtype is the capability: traversal entry points take a
    /// `usize` root, and the only way to obtain one from an entry is
    /// [`ValidatedRowsTrieRoot::offset`] (or [`Self::require_trie_root`]), so a
    /// future caller cannot walk from an unvalidated root by accident. A clustering
    /// reader that gets `Err` must take its "cannot narrow" fallback (decode the
    /// full partition) rather than return a bogus window — see
    /// `reader::data_access::bti::bti_clustering_row_window`.
    pub trie_root: Result<ValidatedRowsTrieRoot, RowsTrieRootRejection>,
    /// Number of row-index blocks indexed by this partition's trie.
    pub block_count: u32,
    /// Partition-level deletion `(local_deletion_time, marked_for_delete_at)`,
    /// decoded via the MODERN DA `DeletionTime` form
    /// ([`decode_da_deletion_time`], issue #832 Finding 2); `None` for the `0x80`
    /// LIVE sentinel or when too few trailing bytes remain.
    pub partition_deletion: Option<(i32, i64)>,
}

impl BtiRowIndexHeader {
    /// The validated row-index trie root offset, or `None` when the resolved root
    /// failed structural validation (issue #3002).
    pub fn trie_root_offset(&self) -> Option<usize> {
        self.trie_root.as_ref().ok().map(|root| root.offset())
    }

    /// The validated row-index trie root offset, or a parse error naming the
    /// violated structural invariant.
    ///
    /// Use this where an unusable root genuinely IS an error (full-partition
    /// row-index enumeration, `verify`); a reader that can fall back to a
    /// full-partition decode should match on [`Self::trie_root`] instead and take
    /// the honest fallback.
    ///
    /// # Errors
    /// [`Error::Parse`] describing the rejected root.
    pub fn require_trie_root(&self) -> BtiResult<usize> {
        match &self.trie_root {
            Ok(root) => Ok(root.offset()),
            Err(rejection) => Err(Error::Parse(format!("Rows.db entry: {rejection}"))),
        }
    }
}

/// Resolve a partition's row-index entry in `Rows.db`, given the `RowsOffset`
/// from a `Partitions.db` lookup ([`BtiPartitionLocation::RowsOffset`]).
///
/// This is the fix for issue #832 Finding A: `RowsOffset` is the offset of the
/// per-partition `TrieIndexEntry`, NOT a trie root.  This deserializes that
/// entry — recovering the partition's Data.db position, the actual row-index
/// trie root, the block count and the partition deletion — so traversal can be
/// rooted correctly.
///
/// `rows_db` is the full `Rows.db` file contents; `rows_offset` is the
/// `RowsOffset` value.  All reads are bounds-checked.
///
/// The recovered trie root is STRUCTURALLY VALIDATED before it is exposed
/// ([`super::rows_root::validate_rows_trie_root`], issue #3002): an unusable root
/// yields `Ok` with [`BtiRowIndexHeader::trie_root`] set to `Err(reason)`, NOT a
/// failed resolution — `data_position`/`block_count` are decoded independently and
/// the paths that consume only those (point lookup, successor walk) must keep
/// working.
///
/// # Errors
/// Returns a parse error if `rows_offset` is out of bounds, the key length is
/// implausible, or the vint fields are truncated.
pub fn resolve_rows_db_entry(rows_db: &[u8], rows_offset: usize) -> BtiResult<BtiRowIndexHeader> {
    // Issue #1647 (L1): count every `TrieIndexEntry.deserialize` on the CLUSTERING
    // read path so it can prove it resolves the per-partition entry EXACTLY once.
    crate::storage::sstable::read_work_counters::record_rows_db_entry_resolve();
    resolve_rows_db_entry_uncounted(rows_db, rows_offset)
}

/// `TrieIndexEntry.deserialize` WITHOUT the L1 `ROWS_DB_ENTRY_RESOLVES` counter
/// (issue #2058). Identical decode to [`resolve_rows_db_entry`]; used by the
/// next-partition SUCCESSOR walk, which resolves a WIDE successor partition's
/// `data_position` only to compute the target partition's exclusive END bound — that
/// is seek-bound work, NOT the clustering-window per-partition resolve the L1
/// invariant (`ROWS_DB_ENTRY_RESOLVES == 1`) accounts for, so it must not bump it.
pub(crate) fn resolve_rows_db_entry_uncounted(
    rows_db: &[u8],
    rows_offset: usize,
) -> BtiResult<BtiRowIndexHeader> {
    if rows_offset + 2 > rows_db.len() {
        return Err(Error::Parse(format!(
            "Rows.db entry: rows_offset {rows_offset} + 2 (key length) exceeds file size {}",
            rows_db.len()
        )));
    }

    // [u16 key_length][key bytes]
    let key_length = u16::from_be_bytes([rows_db[rows_offset], rows_db[rows_offset + 1]]) as usize;
    let entry_start = rows_offset + 2 + key_length;
    if entry_start > rows_db.len() {
        return Err(Error::Parse(format!(
            "Rows.db entry: key length {key_length} at offset {rows_offset} overruns file size {}",
            rows_db.len()
        )));
    }

    // base for the SIGNED root delta = `entry_start`, the position immediately AFTER
    // the short-length-prefixed key (issue #3002; see the module note above).
    let base = entry_start;

    let mut cur = entry_start;
    let (data_position, n) = read_unsigned_vint_from_slice(&rows_db[cur..])?;
    cur += n;

    let (root_delta, n) = read_signed_vint_from_slice(&rows_db[cur..])?;
    cur += n;

    // indexTrieRoot = readVInt() + base   (TrieIndexEntry.deserialize)
    //
    // STRUCTURAL VALIDATION (issue #3002): the resolved offset is only a candidate.
    // The single shared validator below is reached by BOTH public entry points (the
    // counted `resolve_rows_db_entry` delegates here), so the two cannot drift. A
    // rejection invalidates ONLY the root capability — `data_position` and
    // `block_count` below are decoded regardless, so the point-lookup and
    // successor-walk consumers of those fields are untouched.
    let trie_root_signed = root_delta + base as i64;
    let trie_root = validate_rows_trie_root(rows_db, trie_root_signed, rows_offset);

    let (block_count_u64, n) = read_unsigned_vint_from_slice(&rows_db[cur..])?;
    cur += n;
    let block_count = u32::try_from(block_count_u64).map_err(|_| {
        Error::Parse(format!(
            "Rows.db entry: implausible block count {block_count_u64}"
        ))
    })?;

    // Partition DeletionTime: decode the MODERN DA/BTI form (issue #832
    // Finding 2) best-effort: if too few trailing bytes remain, leave it `None`
    // rather than failing (it is not required for traversal correctness).
    let partition_deletion = match decode_da_deletion_time(rows_db, cur) {
        Ok((deletion, _consumed)) => deletion,
        Err(_) => None,
    };

    Ok(BtiRowIndexHeader {
        data_position,
        trie_root,
        block_count,
        partition_deletion,
    })
}

/// Select the row-index blocks that may contain clustering keys in the
/// inclusive byte-comparable range `[start, end]`, applying row-index
/// **separator** semantics (issue #832 Finding B).
///
/// ## Why naive `[start, end]` filtering is wrong
///
/// A `Rows.db` row-index trie stores **separators**, not block start keys.  For
/// consecutive blocks the writer (`RowIndexWriter.add`) stores the shortest
/// `sep` with `prevMax < sep <= nextBlockFirstKey`, and `complete()` appends a
/// trailing separator after the last block.  Consequently the separator `s_i`
/// labels the boundary at the START of block `i`'s key range, and block `i`
/// covers the half-open key interval `[s_i, s_{i+1})` (the final block runs to
/// the trailing separator).  A reader locates the block for a key `K` via the
/// trie *floor* of `K` (`RowIndexReader.separatorFloor`).
///
/// Therefore a block `i` overlaps the requested clustering range `[start, end]`
/// iff its key interval `[s_i, s_{i+1})` intersects `[start, end]`:
///
///   `s_i <= end`  AND  `s_{i+1} > start`
///
/// (For the last block, `s_{i+1}` is treated as +∞.)
///
/// `entries` MUST be the full ascending-order `(separator, block)` list for one
/// partition (as produced by [`iterate_rows_in_bti_trie`]).  `start`/`end` are
/// byte-comparable clustering bounds in the **same encoding as the trie keys**.
/// Reversed bounds (`start > end`) yield an empty result.
///
/// ## Implicit first block (issue #1968)
///
/// The trie stores a separator per block EXCEPT the first: the block covering
/// keys BELOW `entries[0]`'s separator lives at the partition body start and has
/// NO entry here (mirroring `RowIndexReader.separatorFloor`, which returns the
/// partition start for a key below the first separator).  This function therefore
/// only ever returns STORED blocks — it CANNOT return that implicit first block.
/// A caller whose `start` sorts below `entries[0]`'s separator (e.g. an OPEN lower
/// bound, `start == b""`) MUST additionally decode from the partition body start
/// so the earliest clustering rows are not dropped; see
/// `resolve_bti_clustering_seek_window` in `reader/data_access/bti.rs`.
pub fn select_row_index_blocks_for_range(
    entries: &[(Vec<u8>, BtiRowIndexEntry)],
    start: &[u8],
    end: &[u8],
) -> Vec<BtiRowIndexEntry> {
    if start > end || entries.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    for (i, (sep_i, block)) in entries.iter().enumerate() {
        // s_{i+1}: the next separator, or +∞ for the last block.
        let next_is_greater_than_start = match entries.get(i + 1) {
            Some((sep_next, _)) => sep_next.as_slice() > start,
            None => true, // +∞ > start
        };
        let overlaps = sep_i.as_slice() <= end && next_is_greater_than_start;
        if overlaps {
            out.push(block.clone());
        }
    }
    out
}

/// Enumerate every row-index block entry for the partition whose `Rows.db`
/// row-index entry is at `rows_offset` (the `RowsOffset` from `Partitions.db`),
/// in ascending byte-comparable (clustering) order.
///
/// This is the convenience entry point that combines [`resolve_rows_db_entry`]
/// (Finding A) with [`iterate_rows_in_bti_trie`]: it resolves the real trie root
/// from the per-partition entry and then traverses from that root.  Each
/// returned [`BtiRowIndexEntry::data_offset`] is **relative to the partition
/// start**; add `header.data_position` for an absolute `Data.db` position.
///
/// Returns `(header, entries)`.
///
/// # Errors
/// Propagates the entry-resolution error and, per issue #3002, FAILS when the
/// entry's root did not pass structural validation — a full enumeration has no
/// narrower fallback to take, and a malformed row index is exactly what
/// `verify`'s `BtiTrieCorrupt` finding should report.
pub fn iterate_rows_for_partition(
    rows_db: &[u8],
    rows_offset: usize,
) -> BtiResult<(BtiRowIndexHeader, Vec<BtiRowIndexEntryWithKey>)> {
    let header = resolve_rows_db_entry(rows_db, rows_offset)?;
    let entries = iterate_rows_in_bti_trie(rows_db, header.require_trie_root()?)?;
    Ok((header, entries))
}

/// Enumerate row-index entries in a `Rows.db` file that is a **single-partition**
/// trie rooted at its 8-byte footer, in byte-comparable order.
///
/// ## Precondition
///
/// This treats the WHOLE file as one trie whose root is named by the trailing
/// 8-byte footer.  That is only correct when the `Rows.db` contains exactly one
/// partition's row-index trie (or is empty).  For a real multi-partition
/// `Rows.db` you MUST instead use [`iterate_rows_in_bti_trie`] with the
/// per-partition `RowsOffset` obtained from `Partitions.db`.
///
/// A `< 8`-byte (e.g. 0-byte) `Rows.db` yields an empty Vec without erroring.
pub fn iterate_rows_in_bti_file<R: Read + Seek>(
    reader: &mut R,
) -> BtiResult<Vec<(Vec<u8>, BtiRowIndexEntry)>> {
    let file_size = reader.seek(SeekFrom::End(0))?;
    if file_size < 8 {
        return Ok(Vec::new());
    }
    let (trie_data, root_offset) = load_bti_trie_via_footer(reader)?;
    iterate_rows_in_bti_trie(&trie_data, root_offset)
}

#[cfg(test)]
#[path = "rows_tests.rs"]
mod tests;
