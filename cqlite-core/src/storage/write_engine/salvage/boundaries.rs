//! Authoritative partition-boundary enumeration for salvage (design D1;
//! spec R4).
//!
//! Boundaries come ONLY from `Index.db` (BIG) or the `Partitions.db` trie
//! (BTI) — this module never scans `Data.db` for a plausible header (R4.3;
//! `scripts/tests/test_salvage_no_resync_scan.sh` enforces the absence of any
//! byte-pattern search primitive in this directory outside tests).
//!
//! Both walkers are deliberately STRICTER than the production read path:
//! `IndexReader::get_partition_entries` (BIG) silently truncates the
//! partition list at the first entry that fails to parse (see
//! `verify::check_big_index`'s doc), which would make salvage under-report
//! partitions instead of refusing on a corrupt index. So this module walks
//! `Index.db` itself with [`parse_big_index_entry`] directly, mirroring
//! `check_big_index`'s exhaustive walk (issue #1000) rather than trusting
//! the lenient reader.
//!
//! # Memory: O(index size), NOT O(1) (roborev, issue #4196; declared gap)
//!
//! This module reads `Index.db` (or `Partitions.db` + `Rows.db`) WHOLE into
//! memory and materializes one [`BoundaryEntry`] — a COPY of every
//! partition's raw key — per partition, all resident for the run's whole
//! lifetime. Spec R6 ("at most one partition resident on the read side")
//! describes `recover.rs`'s per-partition DECODE loop, which this module
//! feeds; it does NOT hold for boundary enumeration itself, and the
//! `memory-budget` lane entry that would measure this (R6.1) is a declared
//! gap. On a table with a very large partition count this residency —
//! O(index size), roughly doubled by the per-entry key copy — can itself
//! approach or exceed the <128 MB target before a single partition is
//! decoded. Streaming the boundary walk (yielding entries instead of
//! collecting them) is tracked as follow-up work, not implemented here.

use super::{Refusal, RefusalReason};
use crate::storage::sstable::index_reader::parse_big_index_entry;
use std::path::Path;

/// Which authoritative source produced a [`Boundaries`] enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundarySourceKind {
    Index,
    BtiTrie,
}

impl BoundarySourceKind {
    /// Design D5's `boundary_source` manifest string.
    pub fn manifest_label(self) -> &'static str {
        match self {
            BoundarySourceKind::Index => "index",
            BoundarySourceKind::BtiTrie => "bti-trie",
        }
    }
}

/// One partition slot named by the boundary source: its `Data.db` start
/// offset and, when the source carries one independently of `Data.db`, the
/// partition's raw key bytes (design D1's key cross-check, spec R4.2).
#[derive(Debug, Clone)]
pub(super) struct BoundaryEntry {
    /// The boundary source's own key for this slot, when it carries one
    /// independently: always `Some` for BIG (`Index.db` entries carry the
    /// raw key directly); for BTI only a `RowsOffset` (wide-partition) leaf
    /// carries one (the inline key in `Rows.db`) — a `DataOffset` (narrow)
    /// leaf's trie payload is a byte-comparable PREFIX, not the reconstructed
    /// raw key, so there is nothing independently known to cross-check and
    /// this is `None`. This is a fact about what the BTI format's
    /// `Partitions.db` trie carries, not a heuristic: the format simply does
    /// not store a narrow partition's raw key anywhere but `Data.db` itself.
    pub(super) expected_key: Option<Vec<u8>>,
    pub(super) data_offset: u64,
    /// The BTI trie's byte-comparable PREFIX for this slot — present ONLY
    /// when `expected_key` is `None` (a `DataOffset`/narrow BTI leaf). NOT a
    /// raw key (it is a truncated, byte-comparable-order artifact of the
    /// trie), but it is the only thing the boundary source can name for this
    /// slot when a loss must be reported (roborev, issue #4196): without it
    /// every narrow-BTI loss carried an EMPTY `key_hex`, giving an operator
    /// nothing to locate the slot by. `None` for BIG and for BTI `RowsOffset`
    /// leaves, where `expected_key` already carries the real key.
    pub(super) diagnostic_prefix: Option<Vec<u8>>,
}

/// The enumerated boundary source for one input SSTable.
pub(super) struct Boundaries {
    pub(super) kind: BoundarySourceKind,
    pub(super) entries: Vec<BoundaryEntry>,
}

const REBUILD_REMEDY: &str = "cqlite rebuild --components index (issue #4197)";

fn refusal(detail: impl std::fmt::Display) -> Refusal {
    Refusal {
        reason: RefusalReason::BoundarySourceUnreadable,
        remedy: format!("boundary source unreadable: {detail} — remedy: {REBUILD_REMEDY}"),
    }
}

/// Enumerate the authoritative partition boundaries for `dir` (the SSTable's
/// component directory), given `base` (the shared component-name prefix,
/// e.g. `nb-1-big`) and whether the on-disk format is BTI.
pub(super) fn enumerate_boundaries(
    dir: &Path,
    base: &str,
    is_bti: bool,
) -> Result<Boundaries, Refusal> {
    if is_bti {
        bti_boundaries(dir, base)
    } else {
        big_boundaries(dir, base)
    }
}

/// Walk `Index.db` exhaustively via [`parse_big_index_entry`], mirroring
/// `verify::check_big_index`'s corruption posture: a mid-stream parse
/// failure, a non-progressing entry, or zero entries parsed from a non-empty
/// file are all refused rather than silently truncated (issue #1000).
fn big_boundaries(dir: &Path, base: &str) -> Result<Boundaries, Refusal> {
    let index_path = dir.join(format!("{base}-Index.db"));
    let bytes = std::fs::read(&index_path).map_err(|e| refusal(format!("Index.db: {e}")))?;
    if bytes.is_empty() {
        return Err(refusal("Index.db is empty (no partition entries)"));
    }

    let total = bytes.len();
    let mut remaining: &[u8] = &bytes;
    let mut entries = Vec::new();
    loop {
        if remaining.is_empty() {
            break;
        }
        let consumed_before = total - remaining.len();
        match parse_big_index_entry(remaining) {
            Ok((rest, entry)) => {
                if rest.len() >= remaining.len() {
                    return Err(refusal(format!(
                        "Index.db entry {} at byte offset {consumed_before} made no forward \
                         progress (corrupt length field)",
                        entries.len()
                    )));
                }
                remaining = rest;
                entries.push(BoundaryEntry {
                    expected_key: entry
                        .raw_key
                        .as_deref()
                        .map(<[u8]>::to_vec)
                        .or_else(|| Some(entry.key_digest.to_vec())),
                    data_offset: entry.data_offset,
                    diagnostic_prefix: None,
                });
            }
            Err(e) => {
                return Err(refusal(format!(
                    "Index.db entry {} at byte offset {consumed_before} ({consumed_before} of \
                     {total} bytes consumed) failed to parse: {e:?}",
                    entries.len()
                )));
            }
        }
    }

    if entries.is_empty() {
        return Err(refusal(format!(
            "Index.db parsed zero partition entries from {total} bytes"
        )));
    }

    Ok(Boundaries {
        kind: BoundarySourceKind::Index,
        entries,
    })
}

/// Walk `Partitions.db`'s trie exhaustively via [`iterate_partitions_in_bti_file`],
/// resolving every `RowsOffset` (wide-partition) leaf's `Data.db` position and
/// inline raw key from `Rows.db` via [`resolve_rows_db_entry_uncounted`] —
/// mirroring `verify::check_bti_structure`'s resolution (issue #1103), minus
/// that function's ADDITIONAL row-index-trie structural walk: salvage only
/// needs `data_position` (decoded independently of the row-index root's
/// validity per `BtiRowIndexHeader`'s own doc), not the clustering sub-index.
fn bti_boundaries(dir: &Path, base: &str) -> Result<Boundaries, Refusal> {
    use crate::storage::sstable::bti::parser::{
        iterate_partitions_in_bti_file, resolve_rows_db_entry_uncounted, BtiPartitionLocation,
    };
    use std::io::Cursor;

    let partitions_path = dir.join(format!("{base}-Partitions.db"));
    let partitions_bytes =
        std::fs::read(&partitions_path).map_err(|e| refusal(format!("Partitions.db: {e}")))?;
    if partitions_bytes.len() < 8 {
        return Err(refusal(format!(
            "Partitions.db is {} bytes — shorter than the mandatory 8-byte trie root footer \
             (truncated)",
            partitions_bytes.len()
        )));
    }

    let mut cursor = Cursor::new(&partitions_bytes);
    let partitions = iterate_partitions_in_bti_file(&mut cursor)
        .map_err(|e| refusal(format!("Partitions.db trie walk failed: {e}")))?;
    if partitions.is_empty() {
        return Err(refusal(format!(
            "Partitions.db ({} bytes) yielded zero partition keys — the root pointer is corrupt",
            partitions_bytes.len()
        )));
    }

    let rows_path = dir.join(format!("{base}-Rows.db"));
    let rows_bytes = std::fs::read(&rows_path).ok();

    let mut entries = Vec::with_capacity(partitions.len());
    for (prefix, location) in partitions {
        match location {
            BtiPartitionLocation::DataOffset(off) => {
                entries.push(BoundaryEntry {
                    expected_key: None,
                    data_offset: off,
                    diagnostic_prefix: Some(prefix),
                });
            }
            BtiPartitionLocation::RowsOffset(off) => {
                let rows_bytes = rows_bytes.as_ref().ok_or_else(|| {
                    refusal("Rows.db is missing but Partitions.db references a RowsOffset leaf")
                })?;
                let off_usize = off as usize;
                let header = resolve_rows_db_entry_uncounted(rows_bytes, off_usize)
                    .map_err(|e| refusal(format!("Rows.db entry at offset {off}: {e}")))?;
                if off_usize + 2 > rows_bytes.len() {
                    return Err(refusal(format!(
                        "Rows.db entry at offset {off} is truncated (key length)"
                    )));
                }
                let key_length =
                    u16::from_be_bytes([rows_bytes[off_usize], rows_bytes[off_usize + 1]]) as usize;
                let key_start = off_usize + 2;
                let key_end = key_start + key_length;
                if key_end > rows_bytes.len() {
                    return Err(refusal(format!(
                        "Rows.db entry at offset {off} declares an inline key length \
                         {key_length} that overruns the file ({} bytes)",
                        rows_bytes.len()
                    )));
                }
                entries.push(BoundaryEntry {
                    expected_key: Some(rows_bytes[key_start..key_end].to_vec()),
                    data_offset: header.data_position,
                    diagnostic_prefix: None,
                });
            }
        }
    }

    Ok(Boundaries {
        kind: BoundarySourceKind::BtiTrie,
        entries,
    })
}
