//! O(key-length) `Rows.db` separator-floor / strict-ceiling trie walks (issue
//! #1647 / L1).
//!
//! A `Rows.db` row-index trie stores **separators**, not block start keys: the
//! separator `s_i` labels the boundary at the START of block `i`, and block `i`
//! covers the half-open clustering interval `[s_i, s_{i+1})`.  Cassandra locates
//! the block for a clustering key `K` with a downward `separatorFloor(K)` walk
//! (`org.apache.cassandra.io.sstable.format.bti.RowIndexReader#separatorFloor`,
//! built on `Walker#prefixAndNeighbours`) that visits O(len(K)) nodes — NOT by
//! enumerating every block.  Before L1, CQLite's clustering read path materialized
//! ALL row-index blocks (a full DFS) and then linearly filtered them; on the
//! acceptance fixture that is ~42 visited nodes per read regardless of how few
//! blocks a slice touches.
//!
//! This module provides the two authoritative walks the clustering-window path
//! needs, each a byte-by-byte descent that tracks only the closest lesser/greater
//! branch (never a collection of all blocks):
//!
//! - [`rows_floor_block`] — `separatorFloor(K)`: the block whose interval contains
//!   `K` (the largest separator `<= K`), or `None` when `K` sorts below the first
//!   separator (the "implicit first block" at the partition body start).
//! - [`rows_strict_ceiling_block`] — the block with the smallest separator
//!   **strictly greater** than `K` (`K`'s successor block), or `None` when `K`
//!   sorts at/after the last separator (the window then runs to the partition end).
//!
//! Both are byte-for-byte equivalent to selecting from the full ascending
//! `(separator, block)` list ([`super::rows::iterate_rows_in_bti_trie`] +
//! [`super::rows::select_row_index_blocks_for_range`]); that enumerate-then-filter
//! path is retained for genuine full-partition enumeration.
//!
//! Reference: cassandra-5.0.0 `Walker.java` (`follow` / `prefixAndNeighbours` /
//! `goMin` / `goMax`), `RowIndexReader.java` (`separatorFloor`);
//! docs/sstables-definitive-guide chapter 17.

use crate::{error::Error, storage::sstable::bti::node::BtiResult};

use super::node_decode::parse_bti_node;
use super::partitions::{parse_bti_node_for_traversal, payload_start_in_node};
use super::rows::{decode_bti_row_payload, BtiRowIndexEntry};
use super::traversal::ordered_children;

/// Read the `BtiRowIndexEntry` from the payload attached to a `Rows.db` node at
/// `node_offset`, or `None` if the node carries no payload.
///
/// Structurally parallels
/// [`read_node_payload`](super::partitions::read_node_payload) but decodes the
/// Rows.db payload format via [`decode_bti_row_payload`].  Shared by the
/// full-partition DFS ([`super::rows::dfs_collect_row_entries`]) and the
/// floor/ceiling walks below.
pub(super) fn read_row_node_payload(
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

/// Result of searching a node's ascending child list for the next key byte
/// (mirrors `TrieNode.search`).  `insertion` is the number of children whose
/// transition byte is `< b` (the lower-bound index); `exact` is the child index
/// when some child's byte equals `b`.
///
/// For `b == END_OF_STREAM` (key exhausted, which sorts below every real 0..=255
/// transition byte) the result is `{ insertion: 0, exact: None }`, exactly as
/// Cassandra's `search` returns `-1` for the end-of-stream sentinel.
struct ChildSearch {
    /// Number of children with transition byte strictly `< b` (lower bound).
    insertion: usize,
    /// The child index when `children[i].byte == b`, else `None`.
    exact: Option<usize>,
}

/// Locate `b` in the node's ascending `(byte, child)` children.
fn search_children(children: &[(u8, usize)], b: Option<u8>) -> ChildSearch {
    let Some(byte) = b else {
        // END_OF_STREAM sorts below every real transition byte.
        return ChildSearch {
            insertion: 0,
            exact: None,
        };
    };
    // Children are ascending by byte; find the first index with byte >= `byte`.
    let insertion = children.partition_point(|&(cb, _)| cb < byte);
    let exact = match children.get(insertion) {
        Some(&(cb, _)) if cb == byte => Some(insertion),
        _ => None,
    };
    ChildSearch { insertion, exact }
}

/// Descend to the MAXIMUM (rightmost) leaf under `node_offset` and return its
/// row-index payload (mirrors `Walker#goMax`, always following the last child
/// until a leaf, then reading the payload).
fn go_max_payload(trie_data: &[u8], mut node_offset: usize) -> BtiResult<BtiRowIndexEntry> {
    let mut steps = 0usize;
    loop {
        record_visit(trie_data, &mut steps)?;
        let node = parse_bti_node_for_traversal(trie_data, node_offset)?;
        match ordered_children(&node).last() {
            Some(&(_, child)) => node_offset = child,
            None => break,
        }
    }
    read_row_node_payload(trie_data, node_offset)?.ok_or_else(|| {
        Error::Parse(format!(
            "Rows.db floor: max leaf at offset {node_offset} carries no row-index payload \
             (corrupt trie)"
        ))
    })
}

/// Descend to the MINIMUM (leftmost) key under `node_offset` and return its
/// row-index payload (mirrors `Walker#goMin`, stopping at the first node that
/// carries a payload, else following the first child to a leaf).
fn go_min_payload(trie_data: &[u8], mut node_offset: usize) -> BtiResult<BtiRowIndexEntry> {
    let mut steps = 0usize;
    loop {
        record_visit(trie_data, &mut steps)?;
        if let Some(payload) = read_row_node_payload(trie_data, node_offset)? {
            return Ok(payload);
        }
        let node = parse_bti_node_for_traversal(trie_data, node_offset)?;
        match ordered_children(&node).first() {
            Some(&(_, child)) => node_offset = child,
            None => {
                return Err(Error::Parse(format!(
                    "Rows.db ceiling: min leaf at offset {node_offset} carries no row-index \
                     payload (corrupt trie)"
                )))
            }
        }
    }
}

/// Count one visited node and enforce the acyclic total-work bound: an acyclic
/// trie has each node on a downward path occupy `>= 1` byte, so a walk that visits
/// more nodes than the trie has bytes is proof of a cycle/corruption. Mirrors the
/// DFS bound in [`super::traversal`].
#[inline]
fn record_visit(trie_data: &[u8], steps: &mut usize) -> BtiResult<()> {
    crate::storage::sstable::read_work_counters::record_bti_node_visited();
    *steps = steps.saturating_add(1);
    if *steps > trie_data.len().saturating_add(1) {
        return Err(Error::Parse(format!(
            "Rows.db walk exceeded total work bound ({} nodes > trie size {}; corrupt or \
             cyclic trie)",
            *steps,
            trie_data.len()
        )));
    }
    Ok(())
}

/// Compute `separatorFloor(target_key)`: the row-index block whose half-open
/// clustering interval `[s_i, s_{i+1})` contains `target_key` — i.e. the block for
/// the largest separator `<= target_key`.
///
/// Returns `Ok(None)` when `target_key` sorts strictly below the first stored
/// separator: there is no stored block for it, only the trie-implicit first block
/// at the partition body start (the caller decodes from rel 0 in that case; see
/// `select_row_index_blocks_for_range`'s "implicit first block" note and
/// `reader/data_access/bti.rs`).
///
/// This is byte-for-byte equivalent to taking the block with the maximum
/// separator `<= target_key` from the full ascending `(separator, block)` list,
/// but visits only O(len(target_key)) nodes.  Mirrors
/// `RowIndexReader#separatorFloor` (`prefixAndNeighbours` + `goMax(lesserBranch)`).
pub(crate) fn rows_floor_block(
    trie_data: &[u8],
    root_offset: usize,
    target_key: &[u8],
) -> BtiResult<Option<BtiRowIndexEntry>> {
    // `payload` holds the closest prefix separator payload; `lesser_branch` holds
    // the closest strictly-lesser subtree root (mirrors `prefixAndNeighbours`).
    let mut payload: Option<BtiRowIndexEntry> = None;
    let mut lesser_branch: Option<usize> = None;
    let mut node_offset = root_offset;
    let mut key_idx = 0usize;
    let mut steps = 0usize;

    loop {
        record_visit(trie_data, &mut steps)?;
        let node = parse_bti_node_for_traversal(trie_data, node_offset)?;
        let children = ordered_children(&node);

        let b = target_key.get(key_idx).copied();
        key_idx += 1;
        let ChildSearch { insertion, exact } = search_children(&children, b);

        if insertion == 0 {
            // `search` in {0 (exact-first), -1 (below all)}: the current node's
            // separator is still a valid prefix floor candidate — keep it.
            if let Some(p) = read_row_node_payload(trie_data, node_offset)? {
                payload = Some(p);
            }
        } else {
            // A strictly-lesser child exists: it (its max) is a closer floor than
            // the current prefix, so record it and drop the prefix candidate.
            lesser_branch = Some(children[insertion - 1].1);
            payload = None;
        }

        match exact {
            // Exact transition: descend and keep matching the key.
            Some(i) => node_offset = children[i].1,
            // No exact transition: the walk ends here.
            None => break,
        }
    }

    if let Some(p) = payload {
        return Ok(Some(p));
    }
    // No prefix separator matched: the floor is the maximum of the closest lesser
    // branch (or `None` — `target_key` sorts below the first separator).
    match lesser_branch {
        Some(lb) => Ok(Some(go_max_payload(trie_data, lb)?)),
        None => Ok(None),
    }
}

/// Compute the block with the smallest separator **strictly greater** than
/// `target_key` (`target_key`'s successor block).
///
/// Returns `Ok(None)` when `target_key` sorts at or after the last separator: no
/// stored block follows it, so the caller's row-body window runs to the partition
/// end. Used to bound the END of a clustering slice's decode window: the exclusive
/// window end is the successor of `floor(end)`, which equals the smallest separator
/// strictly greater than `end`.
///
/// Byte-for-byte equivalent to the minimum separator `> target_key` in the full
/// ascending list, but visits only O(len(target_key)) nodes.  Mirrors
/// `Walker#followWithGreater` + `goMin(greaterBranch)`.
pub(crate) fn rows_strict_ceiling_block(
    trie_data: &[u8],
    root_offset: usize,
    target_key: &[u8],
) -> BtiResult<Option<BtiRowIndexEntry>> {
    // `greater_branch` holds the closest subtree whose keys are all > target_key.
    let mut greater_branch: Option<usize> = None;
    let mut node_offset = root_offset;
    let mut key_idx = 0usize;
    let mut steps = 0usize;

    loop {
        record_visit(trie_data, &mut steps)?;
        let node = parse_bti_node_for_traversal(trie_data, node_offset)?;
        let children = ordered_children(&node);

        let b = target_key.get(key_idx).copied();
        key_idx += 1;
        let ChildSearch { insertion, exact } = search_children(&children, b);

        // The child immediately greater than the search position: exact match at
        // `i` -> child `i+1`; no exact match -> child at `insertion`. A deeper
        // greater branch shares a longer prefix with the key, so its minimum is a
        // closer successor — overwrite when the current node has one.
        let greater_idx = match exact {
            Some(i) => i + 1,
            None => insertion,
        };
        if greater_idx < children.len() {
            greater_branch = Some(children[greater_idx].1);
        }

        match exact {
            Some(i) => node_offset = children[i].1,
            None => break,
        }
    }

    match greater_branch {
        Some(gb) => Ok(Some(go_min_payload(trie_data, gb)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::super::rows::{iterate_rows_in_bti_trie, select_row_index_blocks_for_range};
    use super::*;

    /// A `Rows.db` PayloadOnly leaf with no open marker (payloadBits=1): a
    /// single-byte SizedInts Data.db position (value 0..=127).
    fn row_leaf(pos: u8) -> Vec<u8> {
        assert!(pos <= 127, "use a 1-byte SizedInts position");
        vec![0x01, pos] // ordinal=0, payloadBits=1 (no FLAG_OPEN_MARKER)
    }

    /// Build a Rows.db-style trie whose separators are the single bytes `seps`
    /// (ascending), each mapping to a distinct 1-byte payload position. Returns
    /// `(trie_bytes, root_offset)`. Uses a Sparse8 root over single-byte leaves,
    /// so every separator is exactly one byte — the canonical single-column
    /// clustering shape.
    fn make_trie(seps: &[u8]) -> (Vec<u8>, usize) {
        let mut trie = Vec::new();
        let mut leaf_offsets = Vec::new();
        for (i, _) in seps.iter().enumerate() {
            leaf_offsets.push(trie.len() as u64);
            // distinct positions 1,2,3,... so payloads are identifiable.
            trie.extend_from_slice(&row_leaf((i + 1) as u8));
        }
        let root = trie.len() as u64;
        trie.push(0x50); // Sparse8
        trie.push(seps.len() as u8); // count
        for &s in seps {
            trie.push(s);
        }
        for &off in &leaf_offsets {
            trie.push((root - off) as u8);
        }
        (trie, root as usize)
    }

    /// The floor walk equals the maximum stored separator `<= key` (or `None`
    /// below the first separator), for EVERY boundary class.
    #[test]
    fn floor_matches_enumerate_and_filter() {
        let seps = [0x10u8, 0x20, 0x30, 0x40];
        let (trie, root) = make_trie(&seps);
        let all = iterate_rows_in_bti_trie(&trie, root).unwrap();
        assert_eq!(all.len(), 4);

        // Oracle: the block for the largest separator <= key, else None.
        let oracle = |key: &[u8]| -> Option<u64> {
            all.iter()
                .filter(|(sep, _)| sep.as_slice() <= key)
                .last()
                .map(|(_, e)| e.data_offset)
        };

        // exact matches, between-block keys, before-first, after-last, multi-byte.
        let probes: &[&[u8]] = &[
            &[0x00],
            &[0x0F],
            &[0x10],
            &[0x18],
            &[0x20],
            &[0x2F],
            &[0x30],
            &[0x40],
            &[0x41],
            &[0xFF],
            &[0x10, 0x00],
            &[0x30, 0x99],
            &[],
        ];
        for key in probes {
            let got = rows_floor_block(&trie, root, key)
                .unwrap()
                .map(|e| e.data_offset);
            assert_eq!(got, oracle(key), "floor mismatch for key {key:02x?}");
        }
    }

    /// The strict-ceiling walk equals the minimum stored separator `> key` (or
    /// `None` at/after the last separator), for EVERY boundary class.
    #[test]
    fn strict_ceiling_matches_enumerate_and_filter() {
        let seps = [0x10u8, 0x20, 0x30, 0x40];
        let (trie, root) = make_trie(&seps);
        let all = iterate_rows_in_bti_trie(&trie, root).unwrap();

        let oracle = |key: &[u8]| -> Option<u64> {
            all.iter()
                .find(|(sep, _)| sep.as_slice() > key)
                .map(|(_, e)| e.data_offset)
        };

        let probes: &[&[u8]] = &[
            &[0x00],
            &[0x0F],
            &[0x10],
            &[0x18],
            &[0x20],
            &[0x30],
            &[0x40],
            &[0x41],
            &[0xFF],
            &[0x10, 0x00],
            &[],
        ];
        for key in probes {
            let got = rows_strict_ceiling_block(&trie, root, key)
                .unwrap()
                .map(|e| e.data_offset);
            assert_eq!(got, oracle(key), "ceiling mismatch for key {key:02x?}");
        }
    }

    /// End-to-end `[body_start, body_end)` window equivalence: for every `(start,
    /// end)` the new floor+ceiling window equals the pre-L1 enumerate-then-select
    /// window, byte-for-byte. This is the exact oracle `bti_clustering_row_window`
    /// relies on — modelled here with `has_static == false` (the synthetic trie has
    /// no static row), so `body_start` narrows whenever the start is not below the
    /// first separator.
    #[test]
    fn window_bounds_match_select_row_index_blocks() {
        const END_INF: usize = usize::MAX;
        let seps = [0x10u8, 0x20, 0x30, 0x40];
        let (trie, root) = make_trie(&seps);
        let all = iterate_rows_in_bti_trie(&trie, root).unwrap();
        let first_sep = all.first().map(|(sep, _)| sep.clone()).unwrap();

        let probes: &[u8] = &[0x00, 0x0F, 0x10, 0x18, 0x20, 0x30, 0x40, 0x41, 0xFF];
        for &s in probes {
            for &e in probes {
                if s > e {
                    continue;
                }
                let start = [s];
                let end = [e];

                // Pre-L1 window (transcribed from the old bti_clustering_row_window
                // block-selection math, has_static == false).
                let blocks = select_row_index_blocks_for_range(&all, &start, &end);
                let includes_implicit = start.as_slice() < first_sep.as_slice();
                let pre: Option<(usize, usize)> = if blocks.is_empty() {
                    // blocks empty <=> range below the first separator (implicit).
                    assert!(includes_implicit, "empty selection must be implicit-first");
                    let first_off = all.first().map(|(_, b)| b.data_offset as usize).unwrap();
                    Some((0, first_off))
                } else {
                    let body_start = if includes_implicit {
                        0
                    } else {
                        blocks.iter().map(|b| b.data_offset as usize).min().unwrap()
                    };
                    let last = blocks.iter().map(|b| b.data_offset).max().unwrap();
                    let body_end = all
                        .iter()
                        .map(|(_, b)| b.data_offset)
                        .filter(|&o| o > last)
                        .min()
                        .map(|o| o as usize)
                        .unwrap_or(END_INF);
                    Some((body_start, body_end))
                };

                // New window (floor + strict-ceiling walks, has_static == false).
                let floor = rows_floor_block(&trie, root, &start).unwrap();
                let ceil = rows_strict_ceiling_block(&trie, root, &end).unwrap();
                let body_start = match &floor {
                    Some(b) => b.data_offset as usize, // not implicit => floor narrows
                    None => 0,                         // implicit-first => partition body start
                };
                let body_end = ceil.map(|b| b.data_offset as usize).unwrap_or(END_INF);
                let new = Some((body_start, body_end));

                assert_eq!(new, pre, "window mismatch for start {s:02x} end {e:02x}");
            }
        }
    }

    /// A key path longer than the separators (deep prefix) still floors correctly,
    /// and an out-of-bounds/empty trie errors cleanly rather than panicking.
    #[test]
    fn floor_deep_key_and_bad_root() {
        let seps = [0x10u8, 0x20];
        let (trie, root) = make_trie(&seps);
        // Long key beyond 0x20 floors to the last separator.
        let got = rows_floor_block(&trie, root, &[0x30, 0x40, 0x50])
            .unwrap()
            .map(|e| e.data_offset);
        assert_eq!(got, Some(2));

        // Out-of-bounds root errors, no panic.
        assert!(rows_floor_block(&trie, trie.len() + 100, &[0x10]).is_err());
        assert!(rows_strict_ceiling_block(&[], 0, &[0x10]).is_err());
    }
}
