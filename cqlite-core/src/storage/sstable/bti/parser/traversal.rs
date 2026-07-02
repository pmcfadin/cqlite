//! Full trie traversal (DFS) primitives — issue #832.
//!
//! These free functions operate purely on in-memory trie bytes plus a root
//! offset (mirroring [`walk_bti_trie`](super::partitions::walk_bti_trie)).  They
//! enumerate *every* leaf/payload in **byte-comparable order** by performing an
//! explicit, depth-capped, stack-based depth-first search.
//!
//! In-order semantics (matches Cassandra `Walker`/`ReverseValueIterator`):
//!   - A node that carries its own payload sorts BEFORE all of its children
//!     (the key that terminates here is a prefix of every continuation).
//!   - Children are visited in ascending transition-byte order.
//!
//! The reconstructed key for each emitted entry is the concatenation of the
//! transition bytes from the root down to (and including) the node that carries
//! the payload.  This is a *byte-comparable / token* encoding, NOT the original
//! partition/clustering key — callers that need the original key must resolve it
//! from Data.db.  The payload OFFSETS, however, are definitive.

use crate::{
    error::Error,
    storage::sstable::bti::node::{BtiNode, BtiNodeData, BtiResult},
};
use std::io::{Read, Seek, SeekFrom};

use super::partitions::{parse_bti_node_for_traversal, read_node_payload, BtiPartitionLocation};

/// Maximum accumulated key-path bytes along a single root→node path.
///
/// A BTI trie key path is a byte-comparable key (partition token prefix or
/// clustering separator); Cassandra limits each partition/clustering key
/// component to `u16::MAX` bytes (64 KiB − 1), so a reconstructed path longer
/// than this is corruption — NOT a legitimate long key.  This replaces the old
/// `DFS_MAX_DEPTH = 128` byte cap (issue #1629 defect 1), which spuriously
/// rejected any legitimate key path longer than 128 bytes.
const MAX_KEY_PATH_BYTES: usize = u16::MAX as usize;

/// Maximum DFS **node** depth (stack depth), counted in NODES / transitions
/// taken — NOT in accumulated key bytes.
///
/// In the BTI on-disk format every trie transition encodes exactly one key byte
/// (there is no multi-byte "chain" node — see `node_decode`), so a leaf's node
/// depth equals its key-path length + 1.  This cap must therefore be at least
/// [`MAX_KEY_PATH_BYTES`] + 1, or it would re-introduce the very false positive
/// this fix removes (issue #1629): the historical
/// [`crate::storage::sstable::bti::MAX_TRIE_DEPTH`] value of 128 conflated node
/// depth with key bytes and rejected legitimate long keys.  It is kept as a
/// distinct, explicitly node-counted guard (defense in depth) alongside the
/// key-byte and total-work bounds.
const DFS_MAX_NODE_DEPTH: usize = MAX_KEY_PATH_BYTES + 1;

/// Read the root offset from the 8-byte big-endian footer of a BTI file and
/// load the entire trie (everything before the footer) into memory.
///
/// Real Cassandra 5.0 BTI files (`Partitions.db` / `Rows.db`) have **no
/// header** — the root node's absolute offset is the last 8 bytes of the file.
/// This is the footer-based loader used by the traversal iterators; it does NOT
/// rely on the fictional [`BtiHeader`](super::reader::BtiHeader) (whose `parse`
/// would misread byte 0 of a real BTI file).
///
/// Returns `(trie_data, root_offset)`.
pub(crate) fn load_bti_trie_via_footer<R: Read + Seek>(
    reader: &mut R,
) -> BtiResult<(Vec<u8>, usize)> {
    let file_size = reader.seek(SeekFrom::End(0))?;
    if file_size < 8 {
        return Err(Error::Parse(format!(
            "BTI file too small ({file_size} bytes; need at least 8 for footer)"
        )));
    }

    reader.seek(SeekFrom::End(-8))?;
    let mut footer = [0u8; 8];
    reader.read_exact(&mut footer)?;
    let root_offset = u64::from_be_bytes(footer);

    let trie_size = file_size - 8;
    if root_offset >= trie_size {
        return Err(Error::Parse(format!(
            "BTI file: root_offset {root_offset} >= trie_size {trie_size}"
        )));
    }

    reader.seek(SeekFrom::Start(0))?;
    let mut trie_data = vec![0u8; trie_size as usize];
    reader.read_exact(&mut trie_data)?;
    Ok((trie_data, root_offset as usize))
}

/// Collect the ascending-transition-byte child list `(transition_byte, child_offset)`
/// for a parsed BTI node, ready for in-order DFS.
///
/// Crucially this iterates `Dense` children directly by index (transition byte =
/// `start_byte + i`) and **skips** the `None` slots, which are the Dense "no
/// transition" sentinels (raw delta `0`).  A `Some(ptr)` slot is a real child
/// and is always emitted — *including* one whose absolute offset is `0` (the
/// first-written leaf in BTI's bottom-up layout legitimately lives at offset 0).
/// ([`BtiNode::get_transitions`] returns an empty Vec for Dense nodes and must
/// NOT be used for traversal.)
///
/// `Sparse` transitions are returned in their stored order; the parser preserves
/// the on-disk ascending order, and we sort defensively.
fn ordered_children(node: &BtiNode) -> Vec<(u8, usize)> {
    match &node.data {
        BtiNodeData::PayloadOnly { .. } => Vec::new(),
        BtiNodeData::Single { transition } => {
            vec![(transition.byte, transition.child.distance as usize)]
        }
        BtiNodeData::Sparse { transitions } => {
            let mut out: Vec<(u8, usize)> = transitions
                .iter()
                .map(|t| (t.byte, t.child.distance as usize))
                .collect();
            // Defensive: BTI Sparse transitions are stored ascending; enforce it.
            out.sort_by_key(|&(b, _)| b);
            out
        }
        BtiNodeData::Dense {
            start_byte,
            children,
        } => {
            let mut out = Vec::new();
            for (i, child) in children.iter().enumerate() {
                // `None` is the Dense "no transition" sentinel (raw delta 0):
                // skip it.  `Some(ptr)` is a real child and is always emitted,
                // even when `ptr.distance == 0` — a real child can live at
                // absolute trie offset 0 (the first-written leaf).
                if let Some(ptr) = child {
                    let transition_byte = start_byte.wrapping_add(i as u8);
                    out.push((transition_byte, ptr.distance as usize));
                }
            }
            out
        }
    }
}

/// Generic in-order DFS over a BTI trie, decoding each node payload with
/// `decode_payload` and pushing `(reconstructed_key, decoded_payload)` onto the
/// result Vec in byte-comparable order.
///
/// The traversal is iterative (explicit stack), bounds-checks every offset, and
/// enforces three independent limits so corrupt or cyclic tries produce an error
/// rather than an infinite loop, unbounded memory, or a panic:
///
/// 1. **Node depth** ([`DFS_MAX_NODE_DEPTH`]) — the number of nodes/transitions
///    on the current root→node path (stack depth), counted in NODES, not bytes.
/// 2. **Accumulated key bytes** ([`MAX_KEY_PATH_BYTES`]) — the length of the
///    reconstructed key path; longer than a Cassandra key component (64 KiB − 1)
///    is corruption.  (Because BTI encodes one key byte per transition, this and
///    the node-depth cap are numerically coupled, but both are checked so each
///    corruption mode has a precise, distinct error.)
/// 3. **Total work** — a running `nodes_visited` counter.  Every distinct node
///    occupies at least one trie byte, so `nodes_visited > trie_data.len()`
///    proves reconvergence/cycling and terminates adversarial tries whose paths
///    stay individually short but whose total node visits blow up.
pub(crate) fn dfs_collect_in_order<T, F>(
    trie_data: &[u8],
    root_offset: usize,
    mut decode_payload: F,
) -> BtiResult<Vec<(Vec<u8>, T)>>
where
    F: FnMut(&[u8], usize) -> BtiResult<Option<T>>,
{
    let mut results: Vec<(Vec<u8>, T)> = Vec::new();

    // Stack frame: (node_offset, node_depth, accumulated_key_bytes).
    // `node_depth` counts NODES on the path (root = 1), independent of the key
    // byte length.  We use an explicit stack and push children in *reverse*
    // order so the smallest transition byte is processed first (LIFO).
    let mut stack: Vec<(usize, usize, Vec<u8>)> = vec![(root_offset, 1, Vec::new())];

    // Total nodes popped across ALL paths — the cycle / reconvergence guard.
    let mut nodes_visited: usize = 0;

    while let Some((node_offset, node_depth, key_bytes)) = stack.pop() {
        nodes_visited = nodes_visited.saturating_add(1);
        if nodes_visited > trie_data.len() {
            return Err(Error::Parse(format!(
                "BTI DFS exceeded total work bound ({nodes_visited} nodes visited > \
                 trie size {}; corrupt or cyclic trie)",
                trie_data.len()
            )));
        }
        if node_depth > DFS_MAX_NODE_DEPTH {
            return Err(Error::Parse(format!(
                "BTI DFS exceeded max node depth {DFS_MAX_NODE_DEPTH} (corrupt or cyclic trie)"
            )));
        }
        if key_bytes.len() > MAX_KEY_PATH_BYTES {
            return Err(Error::Parse(format!(
                "BTI DFS key path exceeded max {MAX_KEY_PATH_BYTES} bytes (corrupt trie)"
            )));
        }
        if node_offset >= trie_data.len() {
            return Err(Error::Parse(format!(
                "BTI DFS: node_offset {node_offset} out of bounds (trie size {})",
                trie_data.len()
            )));
        }

        // 1) Emit this node's own payload (if any) BEFORE descending — the key
        //    terminating here sorts before any continuation.
        if let Some(payload) = decode_payload(trie_data, node_offset)? {
            results.push((key_bytes.clone(), payload));
        }

        // 2) Descend children in ASCENDING transition-byte order.  Because the
        //    stack is LIFO, push them in DESCENDING order.
        let node = parse_bti_node_for_traversal(trie_data, node_offset)?;
        let children = ordered_children(&node);
        let child_depth = node_depth.saturating_add(1);
        for &(transition_byte, child_offset) in children.iter().rev() {
            let mut child_key = key_bytes.clone();
            child_key.push(transition_byte);
            stack.push((child_offset, child_depth, child_key));
        }
    }

    Ok(results)
}

/// Enumerate every partition entry in a `Partitions.db` trie in byte-comparable
/// order: `(reconstructed_token_key, BtiPartitionLocation)`.
pub(crate) fn dfs_collect_partition_entries(
    trie_data: &[u8],
    root_offset: usize,
) -> BtiResult<Vec<(Vec<u8>, BtiPartitionLocation)>> {
    dfs_collect_in_order(trie_data, root_offset, |data, off| {
        read_node_payload(data, off)
    })
}

/// Enumerate **all** partitions in a real Cassandra 5.0 `Partitions.db` BTI file
/// (issue #832), in byte-comparable order.
///
/// This is the headerless public entry point: the trie is loaded via the 8-byte
/// footer (NOT the fictional [`BtiHeader`](super::reader::BtiHeader)).  Each
/// returned tuple is `(reconstructed_token_key, BtiPartitionLocation)`; the
/// offset is definitive, the key is a byte-comparable token prefix (see the DFS
/// module note).
///
/// Returns an empty Vec for a `< 8`-byte (e.g. empty) file.
pub fn iterate_partitions_in_bti_file<R: Read + Seek>(
    reader: &mut R,
) -> BtiResult<Vec<(Vec<u8>, BtiPartitionLocation)>> {
    let file_size = reader.seek(SeekFrom::End(0))?;
    if file_size < 8 {
        return Ok(Vec::new());
    }
    let (trie_data, root_offset) = load_bti_trie_via_footer(reader)?;
    dfs_collect_partition_entries(&trie_data, root_offset)
}

#[cfg(test)]
mod tests {
    use super::super::node_decode::parse_bti_node;
    use super::*;
    use std::io::Cursor;

    // ----- partition DFS unit tests (issue #832) -----

    /// Build a complete in-memory Partitions.db-style file (trie + 8-byte
    /// big-endian root-offset footer).
    fn make_partitions_db(trie_bytes: Vec<u8>, root_offset: u64) -> Vec<u8> {
        let mut v = trie_bytes;
        v.extend_from_slice(&root_offset.to_be_bytes());
        v
    }

    /// A `Partitions.db` PayloadOnly leaf with payloadBits=8 (hash + 1-byte
    /// SizedInts position).  `position` is the *signed* byte; negative →
    /// DataOffset(~position).
    fn partition_leaf(hash: u8, position: i8) -> Vec<u8> {
        vec![0x08, hash, position as u8]
    }

    /// Dense16 (ordinal 11): [0xB0|pf] [start] [len-1] [range * 2-byte deltas]
    fn dense16_node(payload_flags: u8, start: u8, deltas: &[u16]) -> Vec<u8> {
        let len = deltas.len() as u8;
        let mut v = vec![0xB0 | (payload_flags & 0x0F), start, len - 1];
        for &d in deltas {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    /// (1) partition DFS over a synthetic Sparse trie yields entries in
    /// ascending transition-byte order with correct payloads/offsets.
    #[test]
    fn dfs_partition_sparse_ascending_order_with_offsets() {
        let mut trie = vec![0u8; 12];
        trie[0..3].copy_from_slice(&partition_leaf(0x11, -1));
        trie[3..6].copy_from_slice(&partition_leaf(0x22, -65));
        trie[6] = 0x50; // Sparse8
        trie[7] = 0x02; // count
        trie[8] = 0xAA;
        trie[9] = 0xBB;
        trie[10] = 0x06; // child = 6-6 = 0
        trie[11] = 0x03; // child = 6-3 = 3

        let entries = dfs_collect_partition_entries(&trie, 6).unwrap();
        assert_eq!(
            entries,
            vec![
                (vec![0xAA], BtiPartitionLocation::DataOffset(0)),
                (vec![0xBB], BtiPartitionLocation::DataOffset(64)),
            ],
            "Sparse DFS must emit ascending transition bytes with correct offsets"
        );
    }

    /// (2) partition DFS over a Dense node skips distance==0 gaps and emits in
    /// start_byte+i order.
    #[test]
    fn dfs_partition_dense_skips_gaps() {
        let mut trie = vec![0x00u8]; // pad at offset 0
        let l1 = trie.len() as u64; // 1
        trie.extend_from_slice(&partition_leaf(0x11, -1)); // DataOffset(0)
        let l2 = trie.len() as u64; // 4
        trie.extend_from_slice(&partition_leaf(0x22, -65)); // DataOffset(64)
        let dense_off = trie.len() as u64; // 7
        trie.push(0xB0); // Dense16 (ordinal 11)
        trie.push(0x10); // start_byte
        trie.push(0x02); // range_len - 1 = 2 → 3 entries
        trie.extend_from_slice(&((dense_off - l1) as u16).to_be_bytes()); // 0x10 → leaf 1
        trie.extend_from_slice(&0u16.to_be_bytes()); // 0x11 → gap (sentinel)
        trie.extend_from_slice(&((dense_off - l2) as u16).to_be_bytes()); // 0x12 → leaf 4

        let entries = dfs_collect_partition_entries(&trie, dense_off as usize).unwrap();
        assert_eq!(
            entries,
            vec![
                (vec![0x10], BtiPartitionLocation::DataOffset(0)),
                (vec![0x12], BtiPartitionLocation::DataOffset(64)),
            ],
            "Dense DFS must skip distance==0 gaps and emit start_byte+i order"
        );
    }

    /// (3) partition DFS emits an internal node's own payload BEFORE its children.
    #[test]
    fn dfs_partition_internal_payload_before_children() {
        let mut trie = Vec::new();
        trie.extend_from_slice(&partition_leaf(0x11, -1)); // offset 0 → DataOffset(0)
        let node_off = trie.len() as u64; // 3
        trie.push(0x28); // Single8 (ordinal 2), payloadBits=8
        trie.push(0xCC); // transition byte
        trie.push(node_off as u8); // delta=3 → child at offset 0
        trie.push(0x99); // payload hash
        trie.push((-65i8) as u8); // payload position → DataOffset(64)

        let entries = dfs_collect_partition_entries(&trie, node_off as usize).unwrap();
        assert_eq!(
            entries,
            vec![
                // The node's OWN payload (empty key prefix) sorts first.
                (vec![], BtiPartitionLocation::DataOffset(64)),
                // Then its child via transition 0xCC.
                (vec![0xCC], BtiPartitionLocation::DataOffset(0)),
            ],
            "An internal node's payload must be emitted before its children"
        );
    }

    /// PartitionIterator footer-based loading: build a complete in-memory
    /// Partitions.db (trie + footer) and prove the footer loader + DFS produce
    /// the correct entries.
    #[test]
    fn partition_iterator_full_traversal_synthetic() {
        let mut trie = vec![0u8; 12];
        trie[0..3].copy_from_slice(&partition_leaf(0x11, -1));
        trie[3..6].copy_from_slice(&partition_leaf(0x22, -65));
        trie[6] = 0x50;
        trie[7] = 0x02;
        trie[8] = 0xAA;
        trie[9] = 0xBB;
        trie[10] = 0x06;
        trie[11] = 0x03;
        let file = make_partitions_db(trie, 6);

        let (trie_data, root) = load_bti_trie_via_footer(&mut Cursor::new(file)).unwrap();
        assert_eq!(root, 6);
        let entries = dfs_collect_partition_entries(&trie_data, root).unwrap();
        assert_eq!(
            entries,
            vec![
                (vec![0xAA], BtiPartitionLocation::DataOffset(0)),
                (vec![0xBB], BtiPartitionLocation::DataOffset(64)),
            ]
        );
    }

    // ----- issue #1629: node-depth vs key-bytes vs total-work bounds -----

    /// Build a `Partitions.db`-style trie that is a single-transition **chain**
    /// of `n_links` `SingleNoPayload4` nodes descending to a `PayloadOnly`
    /// partition leaf at offset 0.  Returns `(trie_bytes, root_offset)`.
    ///
    /// Because every BTI transition encodes exactly one key byte, the leaf's
    /// reconstructed key path is `n_links` bytes long and the root→leaf node
    /// depth is `n_links + 1`.  `partition_leaf(hash, -1)` yields
    /// `DataOffset(0)` at the leaf.
    fn partition_chain_to_leaf(n_links: usize) -> (Vec<u8>, usize) {
        let mut trie = Vec::new();
        trie.extend_from_slice(&partition_leaf(0x11, -1)); // leaf at offset 0 (3 bytes)
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

    /// Issue #1629 defect 1 (HEADLINE REGRESSION): a legitimate partition/clustering
    /// key path longer than the old 128 cap (here ~200 bytes) must decode WITHOUT
    /// error.  On pre-fix code this returns `Err("... exceeded max depth 128 ...")`
    /// because the cap was applied to `key_bytes.len()` (accumulated key bytes),
    /// not the trie node depth.
    #[test]
    fn dfs_long_partition_key_over_128_bytes_decodes() {
        let (trie, root) = partition_chain_to_leaf(200);
        let entries =
            dfs_collect_partition_entries(&trie, root).expect("a ~200-byte key path must decode");
        assert_eq!(entries.len(), 1, "the single leaf must be emitted");
        assert_eq!(
            entries[0].0.len(),
            200,
            "reconstructed key path is 200 bytes (one per transition)"
        );
        assert_eq!(entries[0].1, BtiPartitionLocation::DataOffset(0));
    }

    /// Issue #1629: a chain just past the OLD 128 cap (129 key bytes) must decode —
    /// locks the regression right at the former false-positive boundary.
    #[test]
    fn dfs_chain_just_over_old_128_cap_decodes() {
        let (trie, root) = partition_chain_to_leaf(129);
        let entries = dfs_collect_partition_entries(&trie, root)
            .expect("a 129-byte key path must decode (old cap was 128)");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0.len(), 129);
    }

    /// Issue #1629 defect 2: an adversarial *reconverging* trie whose node visits
    /// grow super-linearly (each internal node has two transitions pointing at the
    /// SAME child) must terminate via the total-work bound rather than hang.  Every
    /// individual path stays short (so the key-bytes cap never fires), but
    /// `nodes_visited` blows past `trie_data.len()` almost immediately.
    #[test]
    fn dfs_reconverging_trie_hits_work_bound_no_hang() {
        // 30 levels ⇒ ~2^30 node visits without a work bound (would hang).
        let mut trie = Vec::new();
        trie.extend_from_slice(&partition_leaf(0x11, -1)); // N0 leaf at offset 0
        let mut child_off = 0usize;
        let levels = 30;
        for _ in 0..levels {
            let node_off = trie.len();
            let delta = (node_off - child_off) as u8; // stays <= 255
                                                      // Sparse8 with two transitions, BOTH pointing at the same child.
            trie.push(0x50); // Sparse8, no payload
            trie.push(0x02); // count = 2
            trie.push(0x01); // transition a
            trie.push(0x02); // transition b
            trie.push(delta); // child a
            trie.push(delta); // child b
            child_off = node_off;
        }
        let root = trie.len() - 6;

        let err = dfs_collect_partition_entries(&trie, root)
            .expect_err("a reconverging trie must error, not hang");
        let msg = format!("{err}");
        assert!(
            msg.contains("total work bound"),
            "expected the total-work-bound error, got: {msg}"
        );
    }

    /// Issue #1629: the node-depth cap still fires on a chain deeper than the cap.
    /// In the BTI format node depth equals key-path length + 1, so the cap sits at
    /// `u16::MAX + 1` nodes (see `DFS_MAX_NODE_DEPTH`); a chain past it errors with
    /// the node-depth message (distinct from both the work-bound and the leaf
    /// out-of-bounds errors).
    #[test]
    fn dfs_node_depth_cap_rejects_overlong_chain() {
        // One node deeper than the cap: key path = DFS_MAX_NODE_DEPTH bytes,
        // node depth = DFS_MAX_NODE_DEPTH + 1 (root..leaf) > the cap.
        let (trie, root) = partition_chain_to_leaf(DFS_MAX_NODE_DEPTH);
        let err = dfs_collect_partition_entries(&trie, root)
            .expect_err("a chain past the node-depth cap must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("node depth"),
            "expected the node-depth error, got: {msg}"
        );
    }

    /// Dense find_child agreement: a Dense node whose FIRST real child is at
    /// absolute offset 0 must route 0x10 → 0, skip the gap at 0x11, and find
    /// 0x12.
    #[test]
    fn dense_find_child_offset_zero_and_gap() {
        // offset 0,2: row-leaf-style nodes; Dense16 root at offset 4.
        let mut trie = vec![0x01u8, 5, 0x01u8, 9];
        let root = trie.len() as u64; // 4
        let deltas = [root as u16, 0x0000, (root - 2) as u16];
        trie.extend(dense16_node(0, 0x10, &deltas));

        let node = parse_bti_node(&trie[root as usize..], root).unwrap();
        let c10 = node.find_child(0x10).expect("0x10 child must be found");
        assert_eq!(c10.distance, 0, "0x10 must route to absolute offset 0");
        assert!(
            node.find_child(0x11).is_none(),
            "0x11 is the no-transition gap"
        );
        assert!(node.find_child(0x12).is_some());
    }
}
