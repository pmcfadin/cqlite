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

use super::node_decode::parse_bti_node;
use super::partitions::{payload_start_in_node, sized_ints_read_from_slice};
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

/// Test-only re-export of [`read_signed_vint_from_slice`] (see
/// [`read_unsigned_vint_from_slice_for_test`]).
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

/// Read the `BtiRowIndexEntry` from the payload attached to a `Rows.db` node at
/// `node_offset`, or `None` if the node carries no payload.
///
/// Structurally parallels
/// [`read_node_payload`](super::partitions::read_node_payload) but decodes the
/// Rows.db payload format via [`decode_bti_row_payload`].
fn read_row_node_payload(
    trie_data: &[u8],
    node_offset: usize,
) -> BtiResult<Option<BtiRowIndexEntry>> {
    if node_offset >= trie_data.len() {
        return Err(Error::Parse(format!(
            "Rows.db payload read: node_offset {node_offset} out of bounds"
        )));
    }

    let header_byte = trie_data[node_offset];
    let ordinal = (header_byte >> 4) & 0x0F;
    let payload_flags = header_byte & 0x0F;

    // SingleNoPayload variants (ordinals 1, 3) carry their delta in the low
    // nibble and never have a payload.  See `read_node_payload`.
    if ordinal == 1 || ordinal == 3 {
        return Ok(None);
    }

    if ordinal == 0 {
        if payload_flags == 0 {
            return Err(Error::Parse(
                "Rows.db PayloadOnly node has zero payload_flags".to_string(),
            ));
        }
        let payload_start = node_offset + 1;
        Ok(Some(decode_bti_row_payload(
            trie_data,
            payload_start,
            payload_flags,
        )?))
    } else if payload_flags != 0 {
        // Slice must start at the node (see note in `read_node_payload`).
        let node = parse_bti_node(&trie_data[node_offset..], node_offset as u64)?;
        let payload_start = payload_start_in_node(&node, trie_data, node_offset)?;
        Ok(Some(decode_bti_row_payload(
            trie_data,
            payload_start,
            payload_flags,
        )?))
    } else {
        Ok(None)
    }
}

/// Enumerate every row-index entry in a `Rows.db` trie (rooted at `root_offset`)
/// in byte-comparable order: `(reconstructed_clustering_key, BtiRowIndexEntry)`.
pub(crate) fn dfs_collect_row_entries(
    trie_data: &[u8],
    root_offset: usize,
) -> BtiResult<Vec<(Vec<u8>, BtiRowIndexEntry)>> {
    dfs_collect_in_order(trie_data, root_offset, |data, off| {
        read_row_node_payload(data, off)
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
//   [trie_root - base    : SIGNED vint]         ← base = RowsOffset + key_length
//   [row index block count : unsigned vint32]
//   [partition DeletionTime]                    ← delta/compact form; best-effort
//
// `TrieIndexEntry.deserialize` computes `indexTrieRoot = readVInt() + base`.
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
    pub data_position: u64,
    /// Byte offset, within the `Rows.db` file, of this partition's row-index
    /// trie root node.  Feed this to [`iterate_rows_in_bti_trie`] /
    /// `RowsParser::range_query` / `RowsParser::iterate_rows`.
    pub trie_root: usize,
    /// Number of row-index blocks indexed by this partition's trie.
    pub block_count: u32,
    /// Partition-level deletion `(local_deletion_time, marked_for_delete_at)`,
    /// decoded via the MODERN DA `DeletionTime` form
    /// ([`decode_da_deletion_time`], issue #832 Finding 2); `None` for the `0x80`
    /// LIVE sentinel or when too few trailing bytes remain.
    pub partition_deletion: Option<(i32, i64)>,
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
/// # Errors
/// Returns a parse error if `rows_offset` is out of bounds, the key length is
/// implausible, the vint fields are truncated, or the recovered trie root falls
/// outside `rows_db`.
pub fn resolve_rows_db_entry(rows_db: &[u8], rows_offset: usize) -> BtiResult<BtiRowIndexHeader> {
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

    // base for the SIGNED root delta = RowsOffset + key_length (see module note).
    let base = rows_offset + key_length;

    let mut cur = entry_start;
    let (data_position, n) = read_unsigned_vint_from_slice(&rows_db[cur..])?;
    cur += n;

    let (root_delta, n) = read_signed_vint_from_slice(&rows_db[cur..])?;
    cur += n;

    // indexTrieRoot = readVInt() + base   (TrieIndexEntry.deserialize)
    let trie_root_signed = root_delta + base as i64;
    if trie_root_signed < 0 || (trie_root_signed as usize) >= rows_db.len() {
        return Err(Error::Parse(format!(
            "Rows.db entry: recovered trie root {trie_root_signed} out of bounds \
             (base={base}, delta={root_delta}, file size={})",
            rows_db.len()
        )));
    }
    let trie_root = trie_root_signed as usize;

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
pub fn iterate_rows_for_partition(
    rows_db: &[u8],
    rows_offset: usize,
) -> BtiResult<(BtiRowIndexHeader, Vec<BtiRowIndexEntryWithKey>)> {
    let header = resolve_rows_db_entry(rows_db, rows_offset)?;
    let entries = iterate_rows_in_bti_trie(rows_db, header.trie_root)?;
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
mod tests {
    use super::*;

    /// Dense16 (ordinal 11): [0xB0|pf] [start] [len-1] [range * 2-byte deltas]
    fn dense16_node(payload_flags: u8, start: u8, deltas: &[u16]) -> Vec<u8> {
        let len = deltas.len() as u8;
        let mut v = vec![0xB0 | (payload_flags & 0x0F), start, len - 1];
        for &d in deltas {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    /// A `Rows.db` PayloadOnly leaf with no open marker (payloadBits=1): a
    /// single-byte unsigned-vint Data.db position (value 0..=127).
    fn row_leaf_no_marker(pos: u8) -> Vec<u8> {
        assert!(pos <= 127, "use a 1-byte unsigned vint position");
        vec![0x01, pos] // ordinal=0, payloadBits=1 (no FLAG_OPEN_MARKER)
    }

    /// Build a 3-leaf Rows.db-style trie via a Sparse8 root.  Returns
    /// `(trie_bytes, root_offset)`.
    fn make_rows_trie_three(
        (k1, p1): (u8, u8),
        (k2, p2): (u8, u8),
        (k3, p3): (u8, u8),
    ) -> (Vec<u8>, usize) {
        let mut trie = Vec::new();
        let o1 = trie.len() as u64; // 0
        trie.extend_from_slice(&row_leaf_no_marker(p1));
        let o2 = trie.len() as u64; // 2
        trie.extend_from_slice(&row_leaf_no_marker(p2));
        let o3 = trie.len() as u64; // 4
        trie.extend_from_slice(&row_leaf_no_marker(p3));
        let root = trie.len() as u64; // 6
        trie.push(0x50); // Sparse8
        trie.push(0x03); // count=3
        trie.push(k1);
        trie.push(k2);
        trie.push(k3);
        trie.push((root - o1) as u8);
        trie.push((root - o2) as u8);
        trie.push((root - o3) as u8);
        (trie, root as usize)
    }

    /// (5) DFS row collector yields clustering keys in byte order with correct
    /// Rows.db-decoded payloads.
    #[test]
    fn dfs_rows_yields_byte_order_with_row_payloads() {
        let (trie, root) = make_rows_trie_three((0x10, 5), (0x20, 17), (0x30, 99));
        let entries = dfs_collect_row_entries(&trie, root).unwrap();
        assert_eq!(
            entries,
            vec![
                (
                    vec![0x10],
                    BtiRowIndexEntry {
                        data_offset: 5,
                        open_marker: None
                    }
                ),
                (
                    vec![0x20],
                    BtiRowIndexEntry {
                        data_offset: 17,
                        open_marker: None
                    }
                ),
                (
                    vec![0x30],
                    BtiRowIndexEntry {
                        data_offset: 99,
                        open_marker: None
                    }
                ),
            ],
            "Rows.db DFS must yield byte-ordered keys with decoded Data.db positions"
        );
    }

    /// Build a `Rows.db`-style trie that is a single-transition **chain** of
    /// `n_links` `SingleNoPayload4` nodes descending to a `row_leaf_no_marker`
    /// leaf at offset 0.  Returns `(trie_bytes, root_offset)`.
    ///
    /// Every BTI transition encodes exactly one key byte, so the leaf's
    /// reconstructed clustering-key path is `n_links` bytes long.
    fn row_chain_to_leaf(n_links: usize, leaf_pos: u8) -> (Vec<u8>, usize) {
        let mut trie = Vec::new();
        trie.extend_from_slice(&row_leaf_no_marker(leaf_pos)); // leaf at offset 0 (2 bytes)
        let mut child_off = 0usize;
        for i in 0..n_links {
            let node_off = trie.len();
            let delta = node_off - child_off;
            assert!(
                delta <= 0x0F,
                "chain link delta {delta} does not fit a SingleNoPayload4 nibble"
            );
            // SingleNoPayload4 (ordinal 1): [0x10|delta] [transition]; no payload.
            trie.push(0x10 | (delta as u8 & 0x0F));
            trie.push((i % 255) as u8 + 1); // transition byte (nonzero, varies)
            child_off = node_off;
        }
        let root = trie.len() - 2; // last chain node
        (trie, root)
    }

    /// Issue #1629 (roborev "Rows.db regression"): a legitimate reconstructed
    /// clustering-key path LONGER than the old `u16::MAX` (65535) cap must decode
    /// WITHOUT error on the row side too.  This exercises the SAME shared
    /// [`dfs_collect_in_order`] code path via `dfs_collect_row_entries`; the
    /// trie-size-relative bounds accept a 70 000-byte encoded path (trie ~140 002
    /// bytes).  FAILS on 846e1e03 (fixed 65535 cap), passes after.
    #[test]
    fn dfs_long_encoded_row_path_over_u16_max_decodes() {
        let (trie, root) = row_chain_to_leaf(70_000, 7);
        assert!(
            70_000 > u16::MAX as usize,
            "the path must exceed the removed u16::MAX cap to be a regression"
        );
        let entries = dfs_collect_row_entries(&trie, root)
            .expect("a >65535-byte encoded clustering-key path must decode");
        assert_eq!(entries.len(), 1, "the single row leaf must be emitted");
        assert_eq!(
            entries[0].0.len(),
            70_000,
            "reconstructed clustering-key path is 70000 bytes (one per transition)"
        );
        assert_eq!(entries[0].1.data_offset, 7);
    }

    /// Finding 1 (issue #832): a Dense node whose FIRST real child is at
    /// absolute trie offset 0 AND that has a "no transition" gap elsewhere.
    #[test]
    fn dfs_dense_emits_offset_zero_child_and_skips_gap() {
        let mut trie = Vec::new();
        trie.extend_from_slice(&row_leaf_no_marker(5)); // offset 0
        trie.extend_from_slice(&row_leaf_no_marker(9)); // offset 2
        let root = trie.len() as u64; // 4

        // Dense16 root: delta 0 is the "no transition" sentinel.
        let deltas = [root as u16, 0x0000, (root - 2) as u16];
        trie.extend(dense16_node(0, 0x10, &deltas));

        let entries = dfs_collect_row_entries(&trie, root as usize).unwrap();
        assert_eq!(
            entries,
            vec![
                (
                    vec![0x10],
                    BtiRowIndexEntry {
                        data_offset: 5,
                        open_marker: None
                    }
                ),
                (
                    vec![0x12],
                    BtiRowIndexEntry {
                        data_offset: 9,
                        open_marker: None
                    }
                ),
            ],
            "DFS must emit the real child at absolute offset 0 and skip the \
             no-transition gap (0x11)"
        );
    }

    /// Rows.db payload with FLAG_OPEN_MARKER decodes a trailing MODERN DA
    /// DeletionTime (issue #832 Finding 2): markedForDeleteAt FIRST.
    #[test]
    fn decode_row_payload_open_marker_modern() {
        // payloadBits = 0x9 → FLAG_OPEN_MARKER (0x8) set.
        let mut data = vec![0x07u8]; // pos = 7
        data.extend_from_slice(&567890i64.to_be_bytes());
        data.extend_from_slice(&1234u32.to_be_bytes());
        let entry = decode_bti_row_payload(&data, 0, 0x9).unwrap();
        assert_eq!(
            entry,
            BtiRowIndexEntry {
                data_offset: 7,
                // open_marker tuple is (local_deletion_time, marked_for_delete_at).
                open_marker: Some((1234, 567890)),
            }
        );
    }

    /// Rows.db payload with FLAG_OPEN_MARKER but a `0x80` LIVE sentinel decodes
    /// to NO open deletion (issue #832 Finding 2).
    #[test]
    fn decode_row_payload_open_marker_live_sentinel() {
        let data = vec![0x07u8, 0x80u8];
        let entry = decode_bti_row_payload(&data, 0, 0x9).unwrap();
        assert_eq!(
            entry,
            BtiRowIndexEntry {
                data_offset: 7,
                open_marker: None,
            }
        );
    }

    /// Direct coverage of the modern DA `DeletionTime` decoder (issue #832
    /// Finding 2).
    #[test]
    fn da_deletion_time_decoder() {
        // Live sentinel.
        assert_eq!(decode_da_deletion_time(&[0x80], 0).unwrap(), (None, 1));
        // Live sentinel mid-buffer (trailing bytes ignored, consumes 1).
        assert_eq!(
            decode_da_deletion_time(&[0x00, 0x80, 0xFF], 1).unwrap(),
            (None, 1)
        );

        // Non-live: markedForDeleteAt FIRST (i64), then localDeletionTime (u32).
        let mut buf = Vec::new();
        buf.extend_from_slice(&987_654_321_000i64.to_be_bytes()); // mfda
        buf.extend_from_slice(&1_700_000_000u32.to_be_bytes()); // ldt
        let (del, n) = decode_da_deletion_time(&buf, 0).unwrap();
        assert_eq!(n, 12);
        assert_eq!(del, Some((1_700_000_000i32, 987_654_321_000i64)));

        // The leading byte of mfda here is 0x00 (not 0x80).
        assert_ne!(buf[0], 0x80);

        // Truncated non-live value errors.
        assert!(decode_da_deletion_time(&[0x00, 0x01, 0x02], 0).is_err());
        // Out-of-bounds start errors.
        assert!(decode_da_deletion_time(&[0x00], 5).is_err());
    }

    /// Multi-byte unsigned vint decode (count-leading-ones, NOT zigzag).
    #[test]
    fn read_unsigned_vint_multibyte() {
        // 300 = 0x12C → two-byte vint: 0b1000_0001 0b0010_1100 = [0x81, 0x2C]
        let (v, n) = read_unsigned_vint_from_slice(&[0x81, 0x2C]).unwrap();
        assert_eq!((v, n), (300, 2));
        // 127 fits in one byte: [0x7F]
        let (v, n) = read_unsigned_vint_from_slice(&[0x7F]).unwrap();
        assert_eq!((v, n), (127, 1));
    }

    /// (4) range filter over a synthetic 3-leaf rows-style trie.
    #[test]
    fn range_filter_subset_and_empty_and_reversed() {
        let (trie, root) = make_rows_trie_three((0x10, 5), (0x20, 17), (0x30, 99));
        let all = dfs_collect_row_entries(&trie, root).unwrap();

        // Inclusive filter helper mirroring range_query's filter.
        let filter = |lo: &[u8], hi: &[u8]| -> Vec<u64> {
            if lo > hi {
                return Vec::new();
            }
            all.iter()
                .filter(|(k, _)| k.as_slice() >= lo && k.as_slice() <= hi)
                .map(|(_, e)| e.data_offset)
                .collect()
        };

        assert_eq!(filter(&[0x10], &[0x20]), vec![5, 17]); // k1..=k2 excludes k3
        assert_eq!(filter(&[0x20], &[0x30]), vec![17, 99]); // k2..=k3 excludes k1
        assert_eq!(filter(&[0x00], &[0x0F]), Vec::<u64>::new()); // below range
        assert_eq!(filter(&[0x31], &[0xFF]), Vec::<u64>::new()); // above range
        assert_eq!(filter(&[0x30], &[0x10]), Vec::<u64>::new()); // reversed bounds
        assert_eq!(filter(&[0x10], &[0x30]), vec![5, 17, 99]); // full inclusive
    }

    /// Finding B (issue #832): `select_row_index_blocks_for_range` applies
    /// row-index SEPARATOR semantics.
    #[test]
    fn select_blocks_separator_semantics() {
        let entries = vec![
            (
                vec![0x10u8],
                BtiRowIndexEntry {
                    data_offset: 5,
                    open_marker: None,
                },
            ),
            (
                vec![0x20u8],
                BtiRowIndexEntry {
                    data_offset: 17,
                    open_marker: None,
                },
            ),
            (
                vec![0x30u8],
                BtiRowIndexEntry {
                    data_offset: 99,
                    open_marker: None,
                },
            ),
        ];
        let offs = |start: &[u8], end: &[u8]| -> Vec<u64> {
            select_row_index_blocks_for_range(&entries, start, end)
                .into_iter()
                .map(|b| b.data_offset)
                .collect()
        };

        // Floor block must be selected even though its separator <= start.
        assert_eq!(
            offs(&[0x18], &[0x18]),
            vec![5],
            "floor block must be selected"
        );

        // A range spanning block 1 into block 2 selects both.
        assert_eq!(offs(&[0x18], &[0x28]), vec![5, 17]);

        // A range starting exactly at a separator includes that block.
        assert_eq!(offs(&[0x20], &[0x2F]), vec![17]);

        // A range above the last separator selects only the open-ended last block.
        assert_eq!(offs(&[0x40], &[0x50]), vec![99]);

        // Below the first separator: nothing selected.
        assert_eq!(offs(&[0x00], &[0x0F]), Vec::<u64>::new());

        // Full range selects all blocks.
        assert_eq!(offs(&[0x00], &[0xFF]), vec![5, 17, 99]);

        // Reversed bounds → empty.
        assert_eq!(offs(&[0x30], &[0x10]), Vec::<u64>::new());

        // Empty entries → empty.
        assert!(select_row_index_blocks_for_range(&[], &[0x00], &[0xFF]).is_empty());
    }

    /// Finding A (issue #832): `resolve_rows_db_entry` deserializes a synthetic
    /// per-partition `TrieIndexEntry` and recovers the trie root.
    #[test]
    fn resolve_rows_db_entry_recovers_root_and_metadata() {
        // Place a 1-byte pad so the trie "root" lives at a non-zero offset.
        let mut buf = vec![0xEEu8; 4]; // bytes 0..4: pretend trie nodes
        let rows_offset = buf.len(); // entry starts here, e.g. 4

        // key: length 4, value 0x00000007
        buf.extend_from_slice(&4u16.to_be_bytes());
        buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x07]);
        let base = rows_offset + 4; // RowsOffset + key_length

        // dataPos = 123 (unsigned vint, 1 byte since < 128)
        buf.push(123);
        // rootΔ such that trie_root = 2 (a node inside the pad region): Δ = 2 - base
        let root_delta: i64 = 2 - base as i64;
        // zigzag-encode then unsigned-vint (1 byte for small magnitudes)
        let zig = ((root_delta << 1) ^ (root_delta >> 63)) as u64;
        assert!(zig < 128, "test setup expects a 1-byte vint");
        buf.push(zig as u8);
        // blockCount = 38 (unsigned vint)
        buf.push(38);
        // partition DeletionTime (MODERN DA non-live): [i64 mfda=17][u32 ldt=9].
        buf.extend_from_slice(&17i64.to_be_bytes());
        buf.extend_from_slice(&9u32.to_be_bytes());

        let header = resolve_rows_db_entry(&buf, rows_offset).unwrap();
        assert_eq!(header.data_position, 123);
        assert_eq!(
            header.trie_root, 2,
            "trie root = rootΔ + (RowsOffset + keylen)"
        );
        assert_eq!(header.block_count, 38);
        // (local_deletion_time, marked_for_delete_at) = (9, 17).
        assert_eq!(header.partition_deletion, Some((9, 17)));

        // Out-of-bounds RowsOffset → clean error, no panic.
        assert!(resolve_rows_db_entry(&buf, buf.len() + 10).is_err());
    }

    /// Finding 2 (issue #832): a `TrieIndexEntry` whose partition DeletionTime is
    /// the MODERN `0x80` LIVE sentinel decodes to `partition_deletion == None`.
    #[test]
    fn resolve_rows_db_entry_live_partition_deletion() {
        let mut buf = vec![0xEEu8; 4];
        let rows_offset = buf.len();
        buf.extend_from_slice(&4u16.to_be_bytes());
        buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x07]);
        let base = rows_offset + 4;
        buf.push(123); // dataPos
        let root_delta: i64 = 2 - base as i64;
        let zig = ((root_delta << 1) ^ (root_delta >> 63)) as u64;
        buf.push(zig as u8); // rootΔ
        buf.push(38); // blockCount
        buf.push(0x80); // MODERN DA live sentinel → no deletion

        let header = resolve_rows_db_entry(&buf, rows_offset).unwrap();
        assert_eq!(header.block_count, 38);
        assert_eq!(
            header.partition_deletion, None,
            "0x80 live sentinel must decode to no partition deletion"
        );
    }

    /// Signed vint (zig-zag) decode round-trips small +/- values.
    #[test]
    fn read_signed_vint_zigzag() {
        // -10 → zigzag 19 → 1-byte vint 0x13 (the real wide_table rootΔ).
        let (v, n) = read_signed_vint_from_slice(&[0x13]).unwrap();
        assert_eq!((v, n), (-10, 1));
        // 0 → 0x00
        assert_eq!(read_signed_vint_from_slice(&[0x00]).unwrap(), (0, 1));
        // 63 → zigzag 126 → 0x7E
        assert_eq!(read_signed_vint_from_slice(&[0x7E]).unwrap(), (63, 1));
    }

    /// The Rows.db payload offset is a SizedInts value (NOT an unsigned vint).
    #[test]
    fn decode_row_payload_sizedints_two_bytes() {
        // payloadBits = 2 → 2 SizedInts bytes, no open marker.
        let data = vec![0x40u8, 0x80];
        let entry = decode_bti_row_payload(&data, 0, 0x2).unwrap();
        assert_eq!(
            entry,
            BtiRowIndexEntry {
                data_offset: 16512,
                open_marker: None,
            }
        );
    }

    /// Finding 2 (issue #832): the rooted Rows.db API must handle empty /
    /// out-of-bounds cases cleanly (no panic).
    #[test]
    fn iterate_rows_in_bti_trie_empty_and_oob_root() {
        // Empty trie_data with root 0 → out-of-bounds → clean error, no panic.
        let err = iterate_rows_in_bti_trie(&[], 0);
        assert!(err.is_err(), "empty Rows.db trie must error, not panic");

        // Non-empty trie but root beyond bounds → clean error.
        let (trie, _root) = make_rows_trie_three((0x10, 5), (0x20, 17), (0x30, 99));
        let err = iterate_rows_in_bti_trie(&trie, trie.len() + 100);
        assert!(err.is_err(), "out-of-bounds root must error, not panic");

        // A valid per-partition root traverses correctly.
        let (trie, root) = make_rows_trie_three((0x10, 5), (0x20, 17), (0x30, 99));
        let entries = iterate_rows_in_bti_trie(&trie, root).unwrap();
        assert_eq!(entries.len(), 3);
    }

    /// Finding 3 (issue #832): range_query soundness over reconstructed keys
    /// when an internal-node payload's key is a STRICT PREFIX of a deeper leaf
    /// key.
    #[test]
    fn range_filter_prefix_relationship_is_sound() {
        let mut trie = Vec::new();
        // offset 0: leaf for K2 = [0x10,0x20], pos=2
        trie.extend_from_slice(&row_leaf_no_marker(2));
        // offset 2: Single8 with payload — the K = [0x10] node.
        let k_off = trie.len() as u64; // 2
        trie.push(0x21); // Single8 + payloadBits=1
        trie.push(0x20); // transition byte
        trie.push(k_off as u8); // delta=2 → child at offset 0
        trie.push(0x01); // payload: unsigned vint pos=1

        // offset 6: Single8 root, no payload, transition=0x10 → child = k_off (2).
        let root = trie.len() as u64; // 6
        trie.push(0x20); // Single8, payloadBits=0 (no payload)
        trie.push(0x10); // transition byte
        trie.push((root - k_off) as u8); // delta=4 → child at k_off=2

        let all = iterate_rows_in_bti_trie(&trie, root as usize).unwrap();
        // DFS emits K's own payload before descending to K2.
        assert_eq!(
            all,
            vec![
                (
                    vec![0x10],
                    BtiRowIndexEntry {
                        data_offset: 1,
                        open_marker: None
                    }
                ),
                (
                    vec![0x10, 0x20],
                    BtiRowIndexEntry {
                        data_offset: 2,
                        open_marker: None
                    }
                ),
            ],
            "K (internal payload) must sort before its descendant K2"
        );

        // Inclusive byte-comparable filter mirroring range_query's filter.
        let filter = |lo: &[u8], hi: &[u8]| -> Vec<u64> {
            if lo > hi {
                return Vec::new();
            }
            all.iter()
                .filter(|(k, _)| k.as_slice() >= lo && k.as_slice() <= hi)
                .map(|(_, e)| e.data_offset)
                .collect()
        };

        let k = [0x10u8];
        let k2 = [0x10u8, 0x20u8];

        assert_eq!(
            filter(&k, &k),
            vec![1],
            "[K..=K] must include K and exclude the longer K2"
        );
        assert_eq!(
            filter(&k, &k2),
            vec![1, 2],
            "[K..=K2] must include both K and K2"
        );
        assert_eq!(
            filter(&[0x10, 0x00], &[0x10, 0x10]),
            Vec::<u64>::new(),
            "a range strictly between K and K2 must exclude both"
        );
    }
}
