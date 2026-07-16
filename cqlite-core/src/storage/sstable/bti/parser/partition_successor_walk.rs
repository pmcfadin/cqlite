//! O(depth) next-partition (in-order successor) walk over `Partitions.db` (issue
//! #2058, audit C4 part 2).
//!
//! The within-SSTable single-partition seek bounds its decompression window to
//! exactly one partition's byte extent `[target_offset, successor_offset)`. The
//! successor's start offset is the target partition's exclusive END, so it must be
//! resolved from authoritative trie layout, never a heuristic boundary scan.
//!
//! Before #2058 the reader resolved this by enumerating EVERY partition's Data.db
//! offset with a whole-trie DFS ([`super::traversal::dfs_collect_partition_locations`]),
//! sorting the offsets ascending, and memoising the array in a `OnceLock` — the
//! first seek on a reader paid an O(N-partitions) DFS. This module replaces that
//! with a single **strict-ceiling** trie walk that visits O(len(key)) nodes:
//!
//! - A BTI `Partitions.db` trie stores partitions in **byte-comparable key order**,
//!   which for `Murmur3Partitioner` equals **token order**, which equals **Data.db
//!   layout order** (partitions are laid out ascending by token). Therefore the
//!   trie **in-order successor** of a partition is exactly its **offset successor** —
//!   the smallest partition start offset strictly greater than the target's.
//! - Walking with the target partition's OWN full trie key, the strict-ceiling walk
//!   follows exact transitions down to (past) the target and tracks the closest
//!   strictly-greater sibling subtree; the successor is the MINIMUM (leftmost, first
//!   payload-bearing) node of that subtree (`go_min`). This is byte-for-byte the same
//!   partition the sorted-offset-array `partition_point(<= target)` returned, for
//!   EVERY partition (see `tests/issue_2058_bti_local_successor_walk.rs`), but visits
//!   O(depth) nodes instead of the whole trie.
//!
//! Node-family coverage is inherited unchanged from the SAME decoders the DFS uses:
//! [`super::partitions::parse_bti_node_for_traversal`] +
//! [`super::traversal::ordered_children`] handle all six families (PayloadOnly /
//! Single{NoPayload4,8,NoPayload12,16} / Sparse{8,12,16,24,40} / Dense{12,16,24,32,40}
//! / LongDense), so a family the DFS enumerates the local walk descends identically.
//! [`super::partitions::read_node_payload`] recovers the leaf/embedded payload.
//!
//! Concurrency: each walk is a stateless, read-only function of the immutable
//! `Partitions.db` bytes + the target key — no shared cache, no `OnceLock`, so
//! concurrent point reads on the same reader never race or double-walk.
//!
//! Reference: cassandra-5.0.0 `Walker.java` (`follow` / `prefixAndNeighbours` /
//! `goMin`), `PartitionIndex.java`; docs/sstables-definitive-guide chapter 17. The
//! total-work bound (a walk visiting more nodes than the trie has bytes proves a
//! cycle) mirrors [`super::traversal::dfs_visit_in_order`].

use crate::{
    error::Error,
    storage::sstable::bti::node::{BtiNode, BtiResult},
};

use super::partitions::{parse_bti_node_for_traversal, read_node_payload, BtiPartitionLocation};
use super::traversal::ordered_children;

/// Resolve the in-order (== offset-order == token-order) SUCCESSOR partition of the
/// partition whose byte-comparable key is `encoded_key`, directly in a resident
/// `Partitions.db` byte buffer WITHOUT copying the trie.
///
/// `file_bytes` is the full `Partitions.db` file (trie bytes + 8-byte big-endian
/// root-offset footer). Zero-copy analogue of the point
/// [`super::slice_walk::lookup_partition_in_bti_slice`]: it parses the footer and
/// runs [`partition_strict_ceiling_location`] on a borrowed view.
///
/// Returns `Ok(Some(loc))` — the successor partition's location (`DataOffset` for a
/// narrow partition, `RowsOffset` for a wide one, which the caller resolves through
/// `Rows.db`) — or `Ok(None)` when `encoded_key` is the LAST partition (no successor).
pub(crate) fn partition_successor_in_bti_slice(
    file_bytes: &[u8],
    encoded_key: &[u8],
) -> BtiResult<Option<BtiPartitionLocation>> {
    let file_size = file_bytes.len();
    if file_size < 8 {
        return Err(Error::Parse(format!(
            "BTI Partitions.db is too small ({file_size} bytes; need at least 8 for footer)"
        )));
    }
    let trie_size = file_size - 8;
    let root_offset = u64::from_be_bytes([
        file_bytes[trie_size],
        file_bytes[trie_size + 1],
        file_bytes[trie_size + 2],
        file_bytes[trie_size + 3],
        file_bytes[trie_size + 4],
        file_bytes[trie_size + 5],
        file_bytes[trie_size + 6],
        file_bytes[trie_size + 7],
    ]);
    if root_offset as usize >= trie_size {
        return Err(Error::Parse(format!(
            "BTI Partitions.db: root_offset {root_offset} >= trie_size {trie_size}"
        )));
    }
    partition_strict_ceiling_location(&file_bytes[..trie_size], root_offset as usize, encoded_key)
}

/// Count one visited node and enforce the acyclic total-work bound: an acyclic trie
/// has each node on a downward path occupy `>= 1` byte, so a walk that visits more
/// nodes than the trie has bytes is proof of a cycle/corruption. Mirrors the DFS
/// bound in [`super::traversal::dfs_visit_in_order`] and the `Rows.db` walks in
/// [`super::rows_floor`].
#[inline]
fn record_visit(trie_data: &[u8], steps: &mut usize) -> BtiResult<()> {
    crate::storage::sstable::read_work_counters::record_bti_node_visited();
    *steps = steps.saturating_add(1);
    if *steps > trie_data.len().saturating_add(1) {
        return Err(Error::Parse(format!(
            "Partitions.db successor walk exceeded total work bound ({} nodes > trie size {}; \
             corrupt or cyclic trie)",
            *steps,
            trie_data.len()
        )));
    }
    Ok(())
}

/// Result of searching a node's ascending child list for the next key byte.
/// `insertion` is the number of children with transition byte `< b` (lower bound);
/// `exact` is the child index when some child's byte equals `b`. For `b == None`
/// (key exhausted, sorts below every real 0..=255 transition) → `{insertion: 0,
/// exact: None}`.
struct ChildSearch {
    insertion: usize,
    exact: Option<usize>,
}

fn search_children(children: &[(u8, usize)], b: Option<u8>) -> ChildSearch {
    let Some(byte) = b else {
        return ChildSearch {
            insertion: 0,
            exact: None,
        };
    };
    // Children are ascending by transition byte; first index with byte >= `byte`.
    let insertion = children.partition_point(|&(cb, _)| cb < byte);
    let exact = match children.get(insertion) {
        Some(&(cb, _)) if cb == byte => Some(insertion),
        _ => None,
    };
    ChildSearch { insertion, exact }
}

/// Descend to the MINIMUM (leftmost, in byte-comparable order) partition under
/// `node_offset` and return its [`BtiPartitionLocation`] (mirrors `Walker#goMin`:
/// stop at the first node that carries a payload, else follow the first child to a
/// leaf). A payload-bearing internal node sorts BEFORE its children, so it IS the
/// subtree minimum.
fn go_min_location(trie_data: &[u8], mut node_offset: usize) -> BtiResult<BtiPartitionLocation> {
    let mut steps = 0usize;
    loop {
        record_visit(trie_data, &mut steps)?;
        if let Some(loc) = read_node_payload(trie_data, node_offset, None)? {
            return Ok(loc);
        }
        let node: BtiNode = parse_bti_node_for_traversal(trie_data, node_offset)?;
        match ordered_children(&node).first() {
            Some(&(_, child)) => node_offset = child,
            None => {
                return Err(Error::Parse(format!(
                    "Partitions.db successor walk: min node at offset {node_offset} carries no \
                     partition payload and has no children (corrupt trie)"
                )))
            }
        }
    }
}

/// Compute the partition with the smallest byte-comparable key **strictly greater**
/// than `target_key` (`target_key`'s in-order / offset successor), or `Ok(None)`
/// when `target_key` sorts at or after the last partition (it is the LAST partition;
/// the caller bounds the end with the authoritative data-section length).
///
/// Walking with a partition's OWN full trie key returns that partition's offset
/// successor, byte-for-byte equal to the sorted-offset-array `partition_point(<=
/// target)` the old whole-trie DFS produced — but in O(len(key)) node visits.
/// Mirrors Cassandra `Walker#follow` + `goMin(greaterBranch)`; identical in shape to
/// [`super::rows_floor::rows_strict_ceiling_block`], resolving partition locations
/// instead of `Rows.db` row-index blocks.
pub(crate) fn partition_strict_ceiling_location(
    trie_data: &[u8],
    root_offset: usize,
    target_key: &[u8],
) -> BtiResult<Option<BtiPartitionLocation>> {
    // `greater_branch` holds the closest subtree whose keys are ALL > target_key.
    let mut greater_branch: Option<usize> = None;
    let mut node_offset = root_offset;
    let mut key_idx = 0usize;
    let mut steps = 0usize;

    loop {
        record_visit(trie_data, &mut steps)?;
        if node_offset >= trie_data.len() {
            return Err(Error::Parse(format!(
                "Partitions.db successor walk: node offset {node_offset} out of bounds (trie size \
                 {})",
                trie_data.len()
            )));
        }
        let node: BtiNode = parse_bti_node_for_traversal(trie_data, node_offset)?;
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
            // Exact transition: descend and keep matching the key.
            Some(i) => node_offset = children[i].1,
            // No exact transition (incl. a PayloadOnly leaf with no children, or key
            // exhausted): the walk ends here.
            None => break,
        }
    }

    match greater_branch {
        Some(gb) => Ok(Some(go_min_location(trie_data, gb)?)),
        None => Ok(None),
    }
}

/// Test-only hook exposing the next-partition successor walk
/// ([`partition_successor_in_bti_slice`]) so the `issue_2058_bti_local_successor_walk`
/// integration binary can prove — on REAL `da` fixtures — that the O(depth) local
/// walk resolves the identical end-bound the old whole-trie DFS produced, for every
/// partition. Not part of the semver surface (mirrors `find_child_offset_for_test`).
#[doc(hidden)]
pub fn partition_successor_in_bti_slice_for_test(
    file_bytes: &[u8],
    encoded_key: &[u8],
) -> BtiResult<Option<BtiPartitionLocation>> {
    partition_successor_in_bti_slice(file_bytes, encoded_key)
}

#[cfg(test)]
mod tests {
    use super::super::traversal::dfs_collect_partition_entries;
    use super::*;

    /// A `Partitions.db` PayloadOnly leaf with payloadBits=8 (hash + 1-byte SizedInts
    /// position). `position` is the *signed* byte; negative → DataOffset(~position).
    fn partition_leaf(hash: u8, position: i8) -> Vec<u8> {
        vec![0x08, hash, position as u8]
    }

    /// Build a Sparse8-root `Partitions.db`-style trie over single-byte-keyed
    /// PayloadOnly leaves in ASCENDING transition-byte order. `entries` is
    /// `(transition_byte, position)`; returns `(trie_bytes, root_offset)`.
    fn make_sparse_trie(entries: &[(u8, i8)]) -> (Vec<u8>, usize) {
        let mut trie = Vec::new();
        let mut leaf_offsets = Vec::new();
        for &(_, pos) in entries {
            leaf_offsets.push(trie.len() as u64);
            trie.extend_from_slice(&partition_leaf(0x11, pos));
        }
        let root = trie.len() as u64;
        trie.push(0x50); // Sparse8
        trie.push(entries.len() as u8); // count
        for &(b, _) in entries {
            trie.push(b);
        }
        for &off in &leaf_offsets {
            trie.push((root - off) as u8);
        }
        (trie, root as usize)
    }

    fn loc_offset(loc: &BtiPartitionLocation) -> u64 {
        match loc {
            BtiPartitionLocation::DataOffset(o) => *o,
            BtiPartitionLocation::RowsOffset(o) => *o,
        }
    }

    /// The successor walk, keyed on each partition's OWN trie key, returns the
    /// sorted-offset-array successor for EVERY partition, and `None` for the last —
    /// exactly the offsets the whole-trie DFS + sort produced. This is the local
    /// oracle for the reader-level real-fixture proof.
    #[test]
    fn strict_ceiling_matches_dfs_offset_successor_for_all_partitions() {
        // Positions chosen so ~position (Data.db offset) is ASCENDING with the
        // transition byte: -1 -> 0, -11 -> 10, -21 -> 20, -31 -> 30.
        let entries = [(0x10u8, -1i8), (0x20, -11), (0x30, -21), (0x40, -31)];
        let (trie, root) = make_sparse_trie(&entries);

        // Reference: the whole-trie DFS, offsets sorted ascending (the OLD path).
        let dfs = dfs_collect_partition_entries(&trie, root).unwrap();
        let mut offsets: Vec<u64> = dfs.iter().map(|(_, l)| loc_offset(l)).collect();
        offsets.sort_unstable();
        assert_eq!(offsets, vec![0, 10, 20, 30]);

        // For each partition, walk with its OWN reconstructed trie key and assert the
        // resolved successor equals the sorted-offset successor.
        for (key, loc) in &dfs {
            let target_off = loc_offset(loc);
            let idx = offsets.partition_point(|&o| o <= target_off);
            let expected = offsets.get(idx).copied();
            let got = partition_strict_ceiling_location(&trie, root, key)
                .unwrap()
                .map(|l| loc_offset(&l));
            assert_eq!(
                got, expected,
                "successor of partition key {key:02x?} (offset {target_off}) must match the \
                 sorted-offset successor",
            );
        }
    }

    /// A key BELOW the first partition returns the FIRST partition (offset 0); a key
    /// at/after the last returns `None`.
    #[test]
    fn strict_ceiling_below_first_and_after_last() {
        let entries = [(0x10u8, -1i8), (0x20, -11), (0x30, -21)];
        let (trie, root) = make_sparse_trie(&entries);

        // Below the first transition byte -> first partition (offset 0).
        let below = partition_strict_ceiling_location(&trie, root, &[0x00])
            .unwrap()
            .map(|l| loc_offset(&l));
        assert_eq!(below, Some(0));

        // At/after the last -> None (last partition, no successor).
        let after = partition_strict_ceiling_location(&trie, root, &[0xFF])
            .unwrap()
            .map(|l| loc_offset(&l));
        assert_eq!(after, None);
    }

    /// A single-partition trie has no successor for any key.
    #[test]
    fn strict_ceiling_single_partition_has_no_successor() {
        let entries = [(0x10u8, -1i8)];
        let (trie, root) = make_sparse_trie(&entries);
        let got = partition_strict_ceiling_location(&trie, root, &[0x10])
            .unwrap()
            .map(|l| loc_offset(&l));
        assert_eq!(got, None);
    }

    /// A Dense-node root (all six families flow through the same `ordered_children`;
    /// Dense exercises the gap-skipping + start_byte+i path) produces the same
    /// successor as the DFS for every partition.
    #[test]
    fn strict_ceiling_dense_root_matches_dfs() {
        // Dense16 root over three leaves at 0x10/0x11/0x12 (0x11 is a gap).
        let mut trie = vec![0x00u8]; // pad at offset 0 so no child sits at a footer byte
        let l0 = trie.len() as u64;
        trie.extend_from_slice(&partition_leaf(0x11, -1)); // DataOffset(0)
        let l2 = trie.len() as u64;
        trie.extend_from_slice(&partition_leaf(0x22, -21)); // DataOffset(20)
        let dense_off = trie.len() as u64;
        trie.push(0xB0); // Dense16
        trie.push(0x10); // start_byte
        trie.push(0x02); // range_len - 1 = 2 -> 3 slots
        trie.extend_from_slice(&((dense_off - l0) as u16).to_be_bytes()); // 0x10 -> DataOffset(0)
        trie.extend_from_slice(&0u16.to_be_bytes()); // 0x11 -> gap
        trie.extend_from_slice(&((dense_off - l2) as u16).to_be_bytes()); // 0x12 -> DataOffset(20)
        let root = dense_off as usize;

        let dfs = dfs_collect_partition_entries(&trie, root).unwrap();
        let mut offsets: Vec<u64> = dfs.iter().map(|(_, l)| loc_offset(l)).collect();
        offsets.sort_unstable();

        for (key, loc) in &dfs {
            let target_off = loc_offset(loc);
            let idx = offsets.partition_point(|&o| o <= target_off);
            let expected = offsets.get(idx).copied();
            let got = partition_strict_ceiling_location(&trie, root, key)
                .unwrap()
                .map(|l| loc_offset(&l));
            assert_eq!(
                got, expected,
                "Dense root successor mismatch for {key:02x?}"
            );
        }
    }

    /// An out-of-bounds root errors cleanly (no panic).
    #[test]
    fn strict_ceiling_bad_root_errors() {
        let entries = [(0x10u8, -1i8)];
        let (trie, root) = make_sparse_trie(&entries);
        assert!(
            partition_strict_ceiling_location(&trie, root + trie.len() + 100, &[0x10]).is_err()
        );
    }
}
