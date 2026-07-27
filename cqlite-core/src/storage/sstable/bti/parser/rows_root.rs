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
//! ## Why it matters, and exactly how far it goes
//!
//! The pre-#3002 base for the entry's SIGNED root delta was
//! `RowsOffset + key_length` — 2 bytes low, because it omitted the `u16`
//! short-length prefix. A root resolved that way lands 2 bytes INSIDE the real
//! root node's own body, which a bare `root < rows_db.len()` bounds check happily
//! accepts; the floor/ceiling walks then either error or parse a plausible-looking
//! node (a separator byte `0x80` reads as ordinal 8 / `SPARSE_24`) and return a
//! structurally valid but BOGUS clustering window that silently drops rows.
//! Where these checks fire, that becomes an honest "cannot narrow" fallback
//! (full-partition decode): correct-but-slower, never wrong-and-fast.
//!
//! They are a NECESSARY condition on a root, **not a sufficient** one. The extent
//! equality is a single-position test, so a mis-based offset whose bytes happen to
//! decode to a node whose serialized extent ALSO ends exactly at `RowsOffset` still
//! validates — and still narrows to a bogus window. Concretely: this catches the
//! real `test_da/wide_table` mis-basing (both directions, pinned in the tests
//! below), but it is not a general detector for "this file was written against the
//! wrong base". What it does guarantee is the converse — an ACCEPTED root satisfies
//! the writer-ordering invariant, and every REJECTION degrades to correct rows
//! instead of wrong ones. A file written by CQLite <= 0.16 must still be rewritten
//! (re-flush/re-compact); validation is a safety net, not a repair.
//!
//! ## Node extents are computed from RAW BYTES on purpose
//!
//! [`rows_node_serialized_extent`] reads the node's declared shape straight
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

use super::node_decode::pointer_bytes_for_ordinal;
use super::rows::{
    da_deletion_time_body_len, da_deletion_time_is_live_sentinel_byte, FLAG_OPEN_MARKER,
};

/// `TrieNode` ordinal of `PAYLOAD_ONLY` — a CHILDLESS node: the header byte carries
/// nothing but `payloadBits`, so all it can encode is a payload.
const ORDINAL_PAYLOAD_ONLY: u8 = 0;
/// `TrieNode` ordinal of `SINGLE_NOPAYLOAD_4` — the delta lives in the header's
/// low nibble, so this node type structurally CANNOT carry a payload.
const ORDINAL_SINGLE_NOPAYLOAD_4: u8 = 1;
/// `TrieNode` ordinal of `SINGLE_NOPAYLOAD_12` — 12-bit delta across the first two
/// bytes; likewise payload-incapable.
const ORDINAL_SINGLE_NOPAYLOAD_12: u8 = 3;

/// A `Rows.db` row-index trie root offset that has PASSED
/// [`validate_rows_trie_root`].
///
/// The type is the proof of VALIDATION: it can only be constructed by that
/// validator, so a caller cannot accidentally traverse from an unvalidated (possibly
/// mis-based) root. Read the offset with [`Self::offset`].
///
/// It is not proof of WHICH buffer: the newtype is `Copy` and carries no lifetime
/// tie to the `rows_db` slice it was validated against, so it does not by itself
/// prevent a caller from pairing it with a different buffer. Production validates
/// and traverses the same slice; tightening this into a buffer-bound capability is a
/// tracked follow-up.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RowsTrieRootRejectReason {
    /// The resolved offset is negative, or at/after the entry it belongs to. The
    /// trie body always precedes its `TrieIndexEntry`.
    #[error("the resolved root does not lie in the trie region below the entry")]
    NotBelowEntry,
    /// The node type at the resolved offset structurally cannot carry a payload
    /// (`SINGLE_NOPAYLOAD_4` / `SINGLE_NOPAYLOAD_12`, ordinals 1 and 3), so it
    /// cannot be a row-index root: the root is the node that carries block 0's
    /// `ByteComparable.EMPTY` separator payload when one is stored at all.
    #[error(
        "the node byte 0x{header_byte:02x} (ordinal {}) is a SingleNoPayload type, which \
         structurally cannot carry the root's block-0 payload",
        header_byte >> 4
    )]
    PayloadIncapableNodeType {
        /// The offending node header byte (high nibble = ordinal).
        header_byte: u8,
    },
    /// The node is `PayloadOnly` (ordinal 0, i.e. CHILDLESS) with
    /// `payloadBits == 0`, so it has neither a transition nor a payload: it encodes
    /// nothing at all. `TrieNode.typeFor` never emits that shape (a node with no
    /// children takes `PAYLOAD_ONLY`, which exists precisely to carry a payload), and
    /// a row index with `blockCount >= 1` must reach at least one `IndexInfo` from its
    /// root. A `0x00` byte immediately before the entry is the common way a mis-based
    /// delta lands on this shape (issue #3002).
    #[error(
        "the node byte 0x{header_byte:02x} is a childless PayloadOnly node with payloadBits = 0, \
         so it encodes neither a transition nor an IndexInfo payload — TrieNode.typeFor never \
         emits that shape, so it cannot be a row-index root"
    )]
    ChildlessRootWithoutPayload {
        /// The offending node header byte (`ordinal == 0`, `payloadBits == 0`).
        header_byte: u8,
    },
    /// The node's declared shape (or its payload) runs past the end of `Rows.db`.
    #[error("the node's declared shape runs past the end of Rows.db (truncated)")]
    TruncatedNode,
    /// A `Sparse*` node declares ZERO transitions. `TrieNode.Sparse` always stores
    /// `>= 1` transition (`Sparse.sizeofNode` asserts a non-empty transition list;
    /// a childless node is written as `PAYLOAD_ONLY` instead), so this is a violated
    /// node-shape invariant — NOT a truncation.
    #[error(
        "the Sparse node declares 0 transitions, but TrieNode.Sparse always stores at least one \
         (a childless node is emitted as PayloadOnly) — the node shape is invalid"
    )]
    SparseNodeWithoutTransitions,
    /// The node's `payloadBits` do not describe a `RowIndexReader.IndexInfo`
    /// (`SizedInts` width must be 1..=7 once `FLAG_OPEN_MARKER` is masked off).
    #[error(
        "payloadBits 0x{payload_bits:x} is not a valid IndexInfo width (expected a SizedInts \
         width of 1..=7 after masking FLAG_OPEN_MARKER)"
    )]
    InvalidPayloadBits {
        /// The payload-bits nibble that could not be a valid `IndexInfo`.
        payload_bits: u8,
    },
    /// The node parses, but its serialized bytes do NOT end exactly at the entry
    /// — so it is not the last-written (root) node. This is the signature of a
    /// root resolved against a wrong delta base (issue #3002).
    #[error(
        "the node's serialized bytes end at {extent_end}, not at the entry — the trie root is \
         the LAST node written before its entry, so a mismatch means this is not the root \
         (issue #3002)"
    )]
    ExtentNotAtEntry {
        /// Where the node at the resolved offset actually ends (its SHORTEST legal
        /// end; see [`RowsNodeExtent`] for the one ambiguous case).
        extent_end: usize,
    },
}

impl RowsTrieRootRejectReason {
    /// Stable, bounded label for this rejection — a closed set of `&'static str`
    /// suitable as a low-cardinality metric attribute
    /// ([`crate::observability::catalog::attr::ROWS_ROOT_REJECT_REASON`]).
    ///
    /// STAMPED per variant, never derived from the message text or the file bytes.
    pub fn label(self) -> &'static str {
        match self {
            Self::NotBelowEntry => "not_below_entry",
            Self::PayloadIncapableNodeType { .. } => "payload_incapable_node_type",
            Self::ChildlessRootWithoutPayload { .. } => "childless_root_without_payload",
            Self::TruncatedNode => "truncated_node",
            Self::SparseNodeWithoutTransitions => "sparse_node_without_transitions",
            Self::InvalidPayloadBits { .. } => "invalid_payload_bits",
            Self::ExtentNotAtEntry { .. } => "extent_not_at_entry",
        }
    }
}

/// A rejected `Rows.db` root: the offset the entry's delta resolved to, the entry
/// it was resolved from, and the structural invariant it violated.
///
/// A real [`std::error::Error`] (via `thiserror`), because it sits in the `Err`
/// position of [`super::rows::BtiRowIndexHeader::trie_root`] — so
/// `header.trie_root?` composes in a function returning `anyhow::Result` /
/// `Box<dyn Error>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "row-index trie root {resolved_offset} resolved from the entry at RowsOffset({rows_offset}) \
     is unusable: {reason}"
)]
pub struct RowsTrieRootRejection {
    /// The offset the SIGNED root delta resolved to (never trusted as a root;
    /// retained for diagnostics only).
    pub resolved_offset: i64,
    /// The `RowsOffset` of the `TrieIndexEntry` this root was resolved from.
    pub rows_offset: usize,
    /// The violated structural invariant. Not marked `#[source]` deliberately: it is
    /// already rendered inline above, so a `{:#}`-style error chain would repeat it.
    pub reason: RowsTrieRootRejectReason,
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
                // `TrieNode.Sparse` always stores >= 1 transition. This is a node-SHAPE
                // violation, not a truncation — the file may be entirely intact.
                return Err(RowsTrieRootRejectReason::SparseNodeWithoutTransitions);
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

/// The structurally possible EXCLUSIVE end offsets of one serialized `Rows.db`
/// trie node.
///
/// Almost every node has exactly one: its shape and `payloadBits` fully determine
/// its length. The single exception is a payload whose `FLAG_OPEN_MARKER`
/// `DeletionTime` begins with `0x80`, which the DA encoding leaves genuinely
/// AMBIGUOUS in prefix form (`DeletionTime.Serializer`): `0x80` is BOTH the
/// one-byte LIVE sentinel AND the first big-endian byte of a 12-byte body whose
/// `markedForDeleteAt` falls in the `Long.MIN_VALUE` octant. Nothing later in the
/// payload disambiguates it, so such a node has TWO legal extents and this type
/// carries both.
///
/// Reporting both is what keeps root validation free of FALSE rejections: measuring
/// only the 1-byte (LIVE) reading would make a valid 12-byte open marker come up 11
/// bytes short and degrade a correct file to full-partition scans. It is a
/// structural statement about the encoding, not a guess about the writer
/// (no-heuristics, issue #28).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowsNodeExtent {
    /// End under the SHORTEST legal reading of the node's payload.
    shortest_end: usize,
    /// End under the ALTERNATE reading of an ambiguous trailing `DeletionTime`
    /// (`Some` only for the `0x80`-prefixed open-marker case above, and only when
    /// the longer body actually fits inside the file).
    ambiguous_end: Option<usize>,
}

impl RowsNodeExtent {
    /// The node's end under the shortest legal reading of its payload. Equal to THE
    /// end whenever [`Self::is_ambiguous`] is false.
    pub(crate) fn shortest_end(self) -> usize {
        self.shortest_end
    }

    /// Whether this node has two structurally possible ends.
    ///
    /// Production only ever asks [`Self::ends_at`] (an offset either is or is not a
    /// legal end); this predicate exists so the tests can pin WHICH nodes the
    /// encoding leaves ambiguous — and, just as importantly, that every other node
    /// family has exactly one legal end.
    #[cfg(test)]
    pub(crate) fn is_ambiguous(self) -> bool {
        self.ambiguous_end.is_some()
    }

    /// Whether ANY structurally possible end of this node is exactly `offset`.
    pub(crate) fn ends_at(self, offset: usize) -> bool {
        self.shortest_end == offset || self.ambiguous_end == Some(offset)
    }
}

/// Structurally possible byte lengths of the `RowIndexReader.IndexInfo` payload
/// that starts at `payload_start`, given a NON-ZERO `payload_bits` nibble:
/// `(shortest, alternate_when_ambiguous)`.
///
/// Layout (`RowIndexReader.readPayload`): a `SizedInts` block offset of
/// `payload_bits & !FLAG_OPEN_MARKER` bytes, followed by a modern DA
/// `DeletionTime` when `FLAG_OPEN_MARKER` is set. The trailing `DeletionTime` is
/// measured from its DECLARED shape — the two widths the DA encoding defines (1
/// LIVE-sentinel byte, or a 12-byte body) — and where the leading byte cannot
/// distinguish them BOTH are returned, rather than re-using the sentinel-first
/// decode, which would silently pick the 1-byte reading for a real body whose
/// `markedForDeleteAt` MSB happens to be `0x80`.
fn rows_payload_lens(
    rows_db: &[u8],
    payload_start: usize,
    payload_bits: u8,
) -> Result<(usize, Option<usize>), RowsTrieRootRejectReason> {
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
        return Ok((offset_bytes, None));
    }
    // An open range-tombstone marker: a modern DA `DeletionTime` follows.
    let first = *rows_db
        .get(after_offset)
        .ok_or(RowsTrieRootRejectReason::TruncatedNode)?;
    let body_len = da_deletion_time_body_len();
    let body_fits = after_offset
        .checked_add(body_len)
        .is_some_and(|end| end <= rows_db.len());
    if da_deletion_time_is_live_sentinel_byte(first) {
        // AMBIGUOUS: 1-byte LIVE sentinel, or a 12-byte body starting 0x80.
        let alternate = body_fits.then_some(offset_bytes + body_len);
        Ok((offset_bytes + 1, alternate))
    } else if body_fits {
        Ok((offset_bytes + body_len, None))
    } else {
        Err(RowsTrieRootRejectReason::TruncatedNode)
    }
}

/// The structurally possible EXCLUSIVE end offsets of the node serialized at
/// `node_offset` in a `Rows.db` trie — its structure plus any attached `IndexInfo`
/// payload (see [`RowsNodeExtent`] for the one ambiguous case).
///
/// This is the extent the writer-ordering invariant is stated in terms of: the
/// root's extent must end exactly at its `TrieIndexEntry`'s `RowsOffset`.
pub(crate) fn rows_node_serialized_extent(
    rows_db: &[u8],
    node_offset: usize,
) -> Result<RowsNodeExtent, RowsTrieRootRejectReason> {
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
    let (payload_len, ambiguous_payload_len) = if payload_bits == 0
        || ordinal == ORDINAL_SINGLE_NOPAYLOAD_4
        || ordinal == ORDINAL_SINGLE_NOPAYLOAD_12
    {
        (0, None)
    } else {
        rows_payload_lens(rows_db, payload_start, payload_bits)?
    };
    let end = payload_start
        .checked_add(payload_len)
        .ok_or(RowsTrieRootRejectReason::TruncatedNode)?;
    if end > rows_db.len() {
        return Err(RowsTrieRootRejectReason::TruncatedNode);
    }
    // `rows_payload_lens` only offers the longer reading when it fits, so the
    // alternate is in-bounds by construction; re-checked here rather than assumed.
    let ambiguous_end = ambiguous_payload_len
        .and_then(|len| payload_start.checked_add(len))
        .filter(|alt| *alt <= rows_db.len());
    Ok(RowsNodeExtent {
        shortest_end: end,
        ambiguous_end,
    })
}

/// Validate the root offset a `TrieIndexEntry`'s SIGNED delta resolved to, before
/// anything traverses from it (issue #3002).
///
/// `resolved_root` is `root_delta + base`, `rows_offset` the entry's `RowsOffset`.
/// Every check is structural:
///
/// 1. the root lies in the trie region strictly BELOW the entry
///    (`0 <= root < rows_offset`);
/// 2. the node there is payload-CAPABLE (not a `SingleNoPayload` ordinal), is not
///    the empty `PayloadOnly`-without-payload shape, and its declared
///    shape/payload fit inside the file;
/// 3. its serialized extent ends EXACTLY at `rows_offset` — the root is the last
///    node written before its entry.
///
/// Note on (2): a root WITH children is *permitted* to carry no payload
/// (`payloadBits == 0`) — Cassandra stores block 0's `ByteComparable.EMPTY`
/// separator there, but CQLite's own writer fail-closed refuses that separator
/// (`RowsTrieWriter`/`insert_row`) and emits a payload-less internal root, which is
/// a perfectly readable trie. Requiring a payload outright would reject
/// CQLite-written row indexes; requiring payload-CAPABILITY still rejects the
/// pre-#3002 mis-based root, which lands on a `SINGLE_NOPAYLOAD_4` node in the real
/// fixture. The one payload-less shape that IS rejected is `PayloadOnly` (ordinal 0)
/// with `payloadBits == 0`: that node is childless AND payload-less, so it indexes
/// nothing and `TrieNode.typeFor` never emits it — the check is a node-shape
/// invariant, not a "is this byte plausible" judgement. It matters because a `0x00`
/// byte sitting one byte before the entry would otherwise satisfy (3) by accident
/// and be traversed as a root.
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
    // A childless PayloadOnly node with no payload encodes nothing at all, so it
    // cannot root a row index (which indexes `blockCount >= 1` blocks).
    if ordinal == ORDINAL_PAYLOAD_ONLY && header_byte & 0x0F == 0 {
        return Err(reject(
            RowsTrieRootRejectReason::ChildlessRootWithoutPayload { header_byte },
        ));
    }
    let extent = rows_node_serialized_extent(rows_db, root).map_err(reject)?;

    // (3) The writer-ordering invariant: children before parents, entry last.
    if !extent.ends_at(rows_offset) {
        return Err(reject(RowsTrieRootRejectReason::ExtentNotAtEntry {
            extent_end: extent.shortest_end(),
        }));
    }
    Ok(ValidatedRowsTrieRoot(root))
}

/// Test-only re-export of [`rows_node_serialized_extent`]'s SHORTEST end, so the
/// writer-side canonical-base test (`issue_908_bti_canonical_write.rs`) can
/// cross-check its INDEPENDENT hand-rolled extent formula against the production
/// one.
///
/// That test deliberately keeps its own formula (an oracle must not share code with
/// what it checks); this hook makes the two provably agree instead of silently
/// drifting. `None` = the production helper rejected the node.
///
/// For every node the writer emits this IS the node's only end; it differs only for
/// the ambiguous `0x80`-prefixed open-marker `DeletionTime` documented on
/// [`RowsNodeExtent`], which an oracle comparing a single number cannot express.
#[doc(hidden)]
pub fn rows_node_serialized_extent_end_for_test(
    rows_db: &[u8],
    node_offset: usize,
) -> Option<usize> {
    rows_node_serialized_extent(rows_db, node_offset)
        .ok()
        .map(|extent| extent.shortest_end())
}

#[cfg(test)]
#[path = "rows_root_tests.rs"]
mod tests;
