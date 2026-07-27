//! Structural validation of a per-partition `Rows.db` row-index trie ROOT
//! (issue #3002).
//!
//! ## The invariant (a spec property, not a provenance guess)
//!
//! Cassandra's row-index trie is emitted by an INCREMENTAL writer that serializes
//! **children before parents** (`IncrementalTrieWriterPageAware` /
//! `RowIndexWriter.complete`, mirrored by CQLite's own `write_row_node`), and each
//! partition's `TrieIndexEntry` is appended immediately AFTER that partition's trie
//! body (`BtiTableWriter.IndexWriter.append`). Two consequences are therefore
//! properties of the FORMAT, independent of what wrote the file:
//!
//!   1. the trie ROOT is the LAST node written before the entry, so the root's
//!      serialized extent ends EXACTLY at the entry's offset (`RowsOffset`), and
//!   2. the root precedes the entry (`root < RowsOffset`).
//!
//! A resolved root that violates either is not a root — the file is malformed by
//! spec. Nothing here infers *who* wrote the file from byte patterns
//! (no-heuristics, issue #28): every check is a structural equality derived from
//! the serialization order above.
//!
//! ## Why it matters (the fail-open this closes)
//!
//! The pre-#3002 base for the entry's SIGNED root delta was
//! `RowsOffset + key_length` — 2 bytes low, because it omitted the `u16`
//! short-length prefix. A root resolved that way lands 2 bytes INSIDE the real
//! root node's own body, which a bare `root < rows_db.len()` bounds check happily
//! accepts; the floor/ceiling walks then either error or parse a plausible-looking
//! node (a separator byte `0x80` reads as ordinal 8 / `SPARSE_24`) and return a
//! structurally valid but BOGUS clustering window that silently drops rows.
//! Validating the root turns that into an honest "cannot narrow" fallback
//! (full-partition decode): correct-but-slower, never wrong-and-fast.
//!
//! ## Node extents are computed from RAW BYTES on purpose
//!
//! [`rows_node_serialized_extent_end`] reads the node's declared shape straight
//! from the bytes rather than parsing it via
//! [`parse_bti_node`](super::node_decode::parse_bti_node). Parsing would allocate
//! transitions and bump the read-work pointer-decode counters (issues #1650 / H5),
//! perturbing the L1/L3 targeted-descent invariants for a check that only needs a
//! length. The layout it encodes is kept provably in sync with the parsed-node
//! layout by a unit test that cross-checks it against
//! [`payload_start_in_node`](super::partitions::payload_start_in_node) for every
//! node family (see this module's tests).
//!
//! Reference: cassandra-5.0.8 `TrieNode.java` (the 16 node-type ordinals),
//! `RowIndexWriter.java`, `BtiTableWriter.IndexWriter#append`,
//! `TrieIndexEntry#deserialize`; docs/sstables-definitive-guide chapter 17.

use std::fmt;

use super::node_decode::pointer_bytes_for_ordinal;
use super::rows::{da_deletion_time_encoded_len, FLAG_OPEN_MARKER};

/// `TrieNode` ordinal of `SINGLE_NOPAYLOAD_4` — the delta lives in the header's
/// low nibble, so this node type structurally CANNOT carry a payload.
const ORDINAL_SINGLE_NOPAYLOAD_4: u8 = 1;
/// `TrieNode` ordinal of `SINGLE_NOPAYLOAD_12` — 12-bit delta across the first two
/// bytes; likewise payload-incapable.
const ORDINAL_SINGLE_NOPAYLOAD_12: u8 = 3;

/// A `Rows.db` row-index trie root offset that has PASSED
/// [`validate_rows_trie_root`].
///
/// The type is the proof: it can only be constructed by that validator, so a
/// future caller cannot accidentally traverse from an unvalidated (possibly
/// mis-based) root. Read the offset with [`Self::offset`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedRowsTrieRoot(usize);

impl ValidatedRowsTrieRoot {
    /// The validated absolute byte offset of the root node within `Rows.db`.
    pub fn offset(self) -> usize {
        self.0
    }
}

/// Why a resolved `Rows.db` root offset was rejected as unusable.
///
/// Each variant is a violated STRUCTURAL invariant of the on-disk format, never a
/// judgement about which writer produced the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsTrieRootRejectReason {
    /// The resolved offset is negative, or at/after the entry it belongs to. The
    /// trie body always precedes its `TrieIndexEntry`.
    NotBelowEntry,
    /// The node type at the resolved offset structurally cannot carry a payload
    /// (`SINGLE_NOPAYLOAD_4` / `SINGLE_NOPAYLOAD_12`, ordinals 1 and 3), so it
    /// cannot be a row-index root: the root is the node that carries block 0's
    /// `ByteComparable.EMPTY` separator payload when one is stored at all.
    PayloadIncapableNodeType {
        /// The offending node header byte (high nibble = ordinal).
        header_byte: u8,
    },
    /// The node's declared shape (or its payload) runs past the end of `Rows.db`.
    TruncatedNode,
    /// The node's `payloadBits` do not describe a `RowIndexReader.IndexInfo`
    /// (`SizedInts` width must be 1..=7 once `FLAG_OPEN_MARKER` is masked off).
    InvalidPayloadBits {
        /// The payload-bits nibble that could not be a valid `IndexInfo`.
        payload_bits: u8,
    },
    /// The node parses, but its serialized bytes do NOT end exactly at the entry
    /// — so it is not the last-written (root) node. This is the signature of a
    /// root resolved against a wrong delta base (issue #3002).
    ExtentNotAtEntry {
        /// Where the node at the resolved offset actually ends.
        extent_end: usize,
    },
}

impl fmt::Display for RowsTrieRootRejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBelowEntry => write!(
                f,
                "the resolved root does not lie in the trie region below the entry"
            ),
            Self::PayloadIncapableNodeType { header_byte } => write!(
                f,
                "the node byte 0x{header_byte:02x} (ordinal {}) is a SingleNoPayload type, \
                 which structurally cannot carry the root's block-0 payload",
                header_byte >> 4
            ),
            Self::TruncatedNode => write!(
                f,
                "the node's declared shape runs past the end of Rows.db (truncated)"
            ),
            Self::InvalidPayloadBits { payload_bits } => write!(
                f,
                "payloadBits 0x{payload_bits:x} is not a valid IndexInfo width (expected a \
                 SizedInts width of 1..=7 after masking FLAG_OPEN_MARKER)"
            ),
            Self::ExtentNotAtEntry { extent_end } => write!(
                f,
                "the node's serialized bytes end at {extent_end}, not at the entry — the trie \
                 root is the LAST node written before its entry, so a mismatch means this is \
                 not the root (issue #3002)"
            ),
        }
    }
}

/// A rejected `Rows.db` root: the offset the entry's delta resolved to, the entry
/// it was resolved from, and the structural invariant it violated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowsTrieRootRejection {
    /// The offset the SIGNED root delta resolved to (never trusted as a root;
    /// retained for diagnostics only).
    pub resolved_offset: i64,
    /// The `RowsOffset` of the `TrieIndexEntry` this root was resolved from.
    pub rows_offset: usize,
    /// The violated structural invariant.
    pub reason: RowsTrieRootRejectReason,
}

impl fmt::Display for RowsTrieRootRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "row-index trie root {} resolved from the entry at RowsOffset({}) is unusable: {}",
            self.resolved_offset, self.rows_offset, self.reason
        )
    }
}

/// Number of bytes the node STRUCTURE at `node_offset` occupies — header byte,
/// transition bytes and child pointers, EXCLUDING any attached payload.
///
/// Mirrors the 16 `TrieNode` ordinals (`TrieNode.java`) and, for payload-bearing
/// nodes, agrees by construction with
/// [`payload_start_in_node`](super::partitions::payload_start_in_node)
/// (`payload_start == node_offset + structure_len`); a unit test pins that
/// equality for every family.
fn node_structure_len(
    rows_db: &[u8],
    node_offset: usize,
) -> Result<usize, RowsTrieRootRejectReason> {
    let header_byte = *rows_db
        .get(node_offset)
        .ok_or(RowsTrieRootRejectReason::TruncatedNode)?;
    let ordinal = (header_byte >> 4) & 0x0F;
    let ptr_bytes = pointer_bytes_for_ordinal(ordinal) as usize;
    let len = match ordinal {
        // PayloadOnly: header only.
        0 => 1,
        // SingleNoPayload4: header (delta in the low nibble) + transition.
        1 => 2,
        // Single8: header + transition + 1-byte delta.
        2 => 3,
        // SingleNoPayload12: header + delta low byte + transition.
        3 => 3,
        // Single16: header + transition + 2-byte delta.
        4 => 4,
        // Sparse family: header + count + count transitions + count pointers
        // (ordinal 6 packs two 12-bit pointers per 3 bytes).
        5..=9 => {
            let count = *rows_db
                .get(node_offset + 1)
                .ok_or(RowsTrieRootRejectReason::TruncatedNode)? as usize;
            if count == 0 {
                // `TrieNode.Sparse` always stores >= 1 transition.
                return Err(RowsTrieRootRejectReason::TruncatedNode);
            }
            let ptr_area = if ordinal == 6 {
                (count * 3).div_ceil(2)
            } else {
                count * ptr_bytes
            };
            2 + count + ptr_area
        }
        // Dense family: header + start byte + (range_len - 1) + range_len pointers
        // (ordinal 10 packs two 12-bit pointers per 3 bytes).
        _ => {
            let range_len = *rows_db
                .get(node_offset + 2)
                .ok_or(RowsTrieRootRejectReason::TruncatedNode)?
                as usize
                + 1;
            let ptr_area = if ordinal == 10 {
                (range_len * 3).div_ceil(2)
            } else {
                range_len * ptr_bytes
            };
            3 + ptr_area
        }
    };
    Ok(len)
}

/// Byte length of the `RowIndexReader.IndexInfo` payload that starts at
/// `payload_start`, given a NON-ZERO `payload_bits` nibble.
///
/// Layout (`RowIndexReader.readPayload`): a `SizedInts` block offset of
/// `payload_bits & !FLAG_OPEN_MARKER` bytes, followed by a modern DA
/// `DeletionTime` when `FLAG_OPEN_MARKER` is set.
fn rows_payload_len(
    rows_db: &[u8],
    payload_start: usize,
    payload_bits: u8,
) -> Result<usize, RowsTrieRootRejectReason> {
    let offset_bytes = (payload_bits & !FLAG_OPEN_MARKER) as usize;
    if offset_bytes == 0 || offset_bytes > 7 {
        return Err(RowsTrieRootRejectReason::InvalidPayloadBits { payload_bits });
    }
    let after_offset = payload_start
        .checked_add(offset_bytes)
        .ok_or(RowsTrieRootRejectReason::TruncatedNode)?;
    if after_offset > rows_db.len() {
        return Err(RowsTrieRootRejectReason::TruncatedNode);
    }
    if payload_bits & FLAG_OPEN_MARKER == 0 {
        return Ok(offset_bytes);
    }
    // An open range-tombstone marker: a modern DA `DeletionTime` follows (1 byte
    // for the LIVE sentinel, else a 12-byte body).
    let deletion_len = da_deletion_time_encoded_len(rows_db, after_offset)
        .map_err(|_| RowsTrieRootRejectReason::TruncatedNode)?;
    Ok(offset_bytes + deletion_len)
}

/// EXCLUSIVE end offset of the node serialized at `node_offset` in a `Rows.db`
/// trie — its structure plus any attached `IndexInfo` payload.
///
/// This is the extent the writer-ordering invariant is stated in terms of: the
/// root's extent must end exactly at its `TrieIndexEntry`'s `RowsOffset`.
pub(crate) fn rows_node_serialized_extent_end(
    rows_db: &[u8],
    node_offset: usize,
) -> Result<usize, RowsTrieRootRejectReason> {
    let header_byte = *rows_db
        .get(node_offset)
        .ok_or(RowsTrieRootRejectReason::TruncatedNode)?;
    let payload_bits = header_byte & 0x0F;
    let structure_len = node_structure_len(rows_db, node_offset)?;
    let payload_start = node_offset
        .checked_add(structure_len)
        .ok_or(RowsTrieRootRejectReason::TruncatedNode)?;
    if payload_start > rows_db.len() {
        return Err(RowsTrieRootRejectReason::TruncatedNode);
    }
    let ordinal = (header_byte >> 4) & 0x0F;
    // Ordinals 1 and 3 spend their low nibble on the child delta, so those bits are
    // NOT payload bits and no payload follows.
    let payload_len = if payload_bits == 0
        || ordinal == ORDINAL_SINGLE_NOPAYLOAD_4
        || ordinal == ORDINAL_SINGLE_NOPAYLOAD_12
    {
        0
    } else {
        rows_payload_len(rows_db, payload_start, payload_bits)?
    };
    let end = payload_start
        .checked_add(payload_len)
        .ok_or(RowsTrieRootRejectReason::TruncatedNode)?;
    if end > rows_db.len() {
        return Err(RowsTrieRootRejectReason::TruncatedNode);
    }
    Ok(end)
}

/// Validate the root offset a `TrieIndexEntry`'s SIGNED delta resolved to, before
/// anything traverses from it (issue #3002).
///
/// `resolved_root` is `root_delta + base`, `rows_offset` the entry's `RowsOffset`.
/// Every check is structural:
///
/// 1. the root lies in the trie region strictly BELOW the entry
///    (`0 <= root < rows_offset`);
/// 2. the node there is payload-CAPABLE (not a `SingleNoPayload` ordinal) and its
///    declared shape/payload fit inside the file;
/// 3. its serialized extent ends EXACTLY at `rows_offset` — the root is the last
///    node written before its entry.
///
/// Note on (2): the root is *permitted* to carry no payload
/// (`payloadBits == 0`) — Cassandra stores block 0's `ByteComparable.EMPTY`
/// separator there, but CQLite's own writer fail-closed refuses that separator
/// (`RowsTrieWriter`/`insert_row`) and emits a payload-less internal root, which is
/// a perfectly readable trie. Requiring a payload would reject CQLite-written row
/// indexes; requiring payload-CAPABILITY still rejects the pre-#3002 mis-based
/// root, which lands on a `SINGLE_NOPAYLOAD_4` node in the real fixture.
pub(crate) fn validate_rows_trie_root(
    rows_db: &[u8],
    resolved_root: i64,
    rows_offset: usize,
) -> Result<ValidatedRowsTrieRoot, RowsTrieRootRejection> {
    let reject = |reason| RowsTrieRootRejection {
        resolved_offset: resolved_root,
        rows_offset,
        reason,
    };

    // (1) The trie body always precedes its entry. `rows_offset` is already known
    // to be within the file, so this also bounds the root inside `rows_db`.
    if resolved_root < 0 || resolved_root >= rows_offset as i64 {
        return Err(reject(RowsTrieRootRejectReason::NotBelowEntry));
    }
    let root = resolved_root as usize;

    // (2) Payload-capable, known node type, in-bounds shape.
    let header_byte = *rows_db
        .get(root)
        .ok_or_else(|| reject(RowsTrieRootRejectReason::TruncatedNode))?;
    let ordinal = (header_byte >> 4) & 0x0F;
    if ordinal == ORDINAL_SINGLE_NOPAYLOAD_4 || ordinal == ORDINAL_SINGLE_NOPAYLOAD_12 {
        return Err(reject(RowsTrieRootRejectReason::PayloadIncapableNodeType {
            header_byte,
        }));
    }
    let extent_end = rows_node_serialized_extent_end(rows_db, root).map_err(reject)?;

    // (3) The writer-ordering invariant: children before parents, entry last.
    if extent_end != rows_offset {
        return Err(reject(RowsTrieRootRejectReason::ExtentNotAtEntry {
            extent_end,
        }));
    }
    Ok(ValidatedRowsTrieRoot(root))
}

/// Test-only re-export of [`rows_node_serialized_extent_end`] so the writer-side
/// canonical-base test (`issue_908_bti_canonical_write.rs`) can cross-check its
/// INDEPENDENT hand-rolled extent formula against the production one.
///
/// That test deliberately keeps its own formula (an oracle must not share code with
/// what it checks); this hook makes the two provably agree instead of silently
/// drifting. `None` = the production helper rejected the node.
#[doc(hidden)]
pub fn rows_node_serialized_extent_end_for_test(
    rows_db: &[u8],
    node_offset: usize,
) -> Option<usize> {
    rows_node_serialized_extent_end(rows_db, node_offset).ok()
}

#[cfg(test)]
#[path = "rows_root_tests.rs"]
mod tests;
