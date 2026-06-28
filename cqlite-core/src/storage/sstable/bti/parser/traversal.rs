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

/// Maximum DFS stack depth, mirroring [`crate::storage::sstable::bti::MAX_TRIE_DEPTH`].
const DFS_MAX_DEPTH: usize = 128;

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
/// The traversal is iterative (explicit stack), depth-capped at
/// [`DFS_MAX_DEPTH`], and bounds-checks every offset — corrupt or cyclic tries
/// produce an error rather than an infinite loop or panic.
pub(crate) fn dfs_collect_in_order<T, F>(
    trie_data: &[u8],
    root_offset: usize,
    mut decode_payload: F,
) -> BtiResult<Vec<(Vec<u8>, T)>>
where
    F: FnMut(&[u8], usize) -> BtiResult<Option<T>>,
{
    let mut results: Vec<(Vec<u8>, T)> = Vec::new();

    // Stack frame: (node_offset, accumulated_key_bytes).
    // We use an explicit stack and push children in *reverse* order so the
    // smallest transition byte is processed first (LIFO).
    let mut stack: Vec<(usize, Vec<u8>)> = vec![(root_offset, Vec::new())];

    while let Some((node_offset, key_bytes)) = stack.pop() {
        if key_bytes.len() > DFS_MAX_DEPTH {
            return Err(Error::Parse(format!(
                "BTI DFS exceeded max depth {DFS_MAX_DEPTH} (corrupt or cyclic trie)"
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
        for &(transition_byte, child_offset) in children.iter().rev() {
            let mut child_key = key_bytes.clone();
            child_key.push(transition_byte);
            stack.push((child_offset, child_key));
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
