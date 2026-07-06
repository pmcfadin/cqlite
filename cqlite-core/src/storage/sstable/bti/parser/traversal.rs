//! Full trie traversal (DFS) primitives — issue #832.
//!
//! These free functions operate purely on in-memory trie bytes plus a root
//! offset (mirroring [`walk_bti_trie`](super::partitions::walk_bti_trie)).  They
//! enumerate *every* leaf/payload in **byte-comparable order** by performing an
//! explicit, stack-based depth-first search whose termination bounds are all
//! relative to the trie's own byte length (see [`dfs_collect_in_order`]).
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
/// (The former `get_transitions` helper returned an empty Vec for Dense nodes
/// and must NOT be used for traversal — now removed.)
///
/// `Sparse` transitions are returned in their stored order; the parser preserves
/// the on-disk ascending order, and we sort defensively.
///
/// Shared with [`super::rows_floor`], whose O(key-length) floor/ceiling walk
/// follows the SAME ascending `(transition_byte, absolute_child_offset)` child
/// order the DFS uses (issue #1647 / L1).
pub(super) fn ordered_children(node: &BtiNode) -> Vec<(u8, usize)> {
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
/// `decode_payload` and invoking `visit(&reconstructed_key, payload)` in
/// byte-comparable order — the key is handed to the visitor as a **borrowed
/// slice** into the single reusable path buffer, so nothing is copied per node.
///
/// This is the zero-clone core (issue #1649 / L2): a SINGLE mutable `path`
/// accumulates the transition bytes from the root to the current node, and the
/// visitor sees that borrow directly.  Callers that need an owned key call
/// `.to_vec()` inside their visitor (paying **per emitted result**, never per
/// child edge — [`dfs_collect_in_order`]); offset-only callers (e.g. the
/// next-partition seek-bound resolver) drop the key and allocate **nothing**
/// per node ([`dfs_collect_partition_locations`]).
///
/// The traversal is iterative (explicit stack), bounds-checks every offset, and
/// enforces a **visited-offset guard** plus three independent limits so corrupt or
/// cyclic tries produce an error rather than an infinite loop, unbounded memory, or
/// a panic.  The visited-offset guard fires FIRST for cycles/reconvergence: a BTI
/// trie is a TREE (each node written once, referenced by exactly one parent), so no
/// offset is legitimately entered twice during a full DFS.  Rejecting the first
/// re-entry bounds the PENDING OP STACK — a small cyclic/high-fan-out node cannot
/// push `Enter` ops faster than the total-work counter climbs toward a large padded
/// trie length (issue #1629 adversarial reconvergence).  **Every remaining bound is
/// relative to the trie's own byte length (`trie_data.len()`), never a fixed
/// constant.**  The reason: on an acyclic trie every node on a root→node path is
/// distinct and occupies at least one trie byte, so any legitimately reconstructed
/// path length — and any legitimate node depth — is `<= trie_data.len()`.  A path
/// or depth exceeding the trie's own byte length is structurally impossible
/// without revisiting a node, i.e. proof of a cycle/corruption.  Bounding by
/// `trie_data.len()` therefore can NEVER reject a key that legitimately fits in
/// the file (unlike a fixed cap such as the old 128-byte or `u16::MAX` limits,
/// which spuriously rejected long-but-legal byte-comparable encoded key paths —
/// issue #1629), while still catching every cycle:
///
/// 1. **Total work** — a running `nodes_visited` counter across ALL paths.  Every
///    distinct node occupies at least one trie byte, so `nodes_visited >
///    trie_data.len()` proves reconvergence/cycling and terminates adversarial
///    tries whose paths stay individually short but whose total node visits blow
///    up.  This is the airtight cycle guard.
/// 2. **Per-path key-path length** — `path.len() > trie_data.len()` is a
///    structurally impossible reconstructed path on an acyclic trie.
/// 3. **Node depth** — the number of nodes on the current root→node path (stack
///    depth, root = 1).  A legal leaf's depth is at most `trie_data.len() + 1`
///    (one distinct node per byte, plus the root count), so a greater depth
///    proves a cycle.
///
/// Each guard carries a distinct error message so corruption modes stay
/// diagnosable.
pub(crate) fn dfs_visit_in_order<T, F, V>(
    trie_data: &[u8],
    root_offset: usize,
    mut decode_payload: F,
    mut visit: V,
) -> BtiResult<()>
where
    F: FnMut(&[u8], usize) -> BtiResult<Option<T>>,
    V: FnMut(&[u8], T),
{
    // Op-based iterative DFS.  A SINGLE mutable `path` accumulates the transition
    // bytes from the root down to the current node: `Enter` pushes a node's
    // transition byte (and schedules the matching `Pop`), `Pop` backtracks it.  The
    // visitor sees `path` as a BORROWED slice — we never copy it per node.  Owned-key
    // callers copy inside the visitor (per emitted result); offset-only callers copy
    // nothing.  This avoids the O(depth^2) prefix copying that a per-edge
    // `key_bytes.clone()` incurs on long key paths (a 70 000-byte legal chain would
    // otherwise copy ~2.45 GB; issue #1629 roborev).
    enum DfsOp {
        Enter {
            node_offset: usize,
            // The transition byte from the parent to this node (root = `None`).
            transition: Option<u8>,
            // Number of NODES on the root→this-node path (root = 1).
            depth: usize,
        },
        // Backtrack: remove the transition byte pushed by the matching `Enter`.
        Pop,
    }

    // The single reusable root→current-node transition-byte path.
    let mut path: Vec<u8> = Vec::new();
    let mut stack: Vec<DfsOp> = vec![DfsOp::Enter {
        node_offset: root_offset,
        transition: None,
        depth: 1,
    }];

    // Total nodes entered across ALL paths — the cycle / reconvergence guard.
    let mut nodes_visited: usize = 0;

    // Per-offset visited bitset (1 bit per trie byte) — the FIRST-re-entry guard.
    // A BTI trie is structurally a TREE: every node is written once and referenced
    // by exactly one parent, so during a full DFS no node offset is legitimately
    // entered more than once.  Rejecting the first re-entry bounds the PENDING OP
    // STACK: it stops a small cyclic/reconvergent node with high fan-out (e.g. a
    // Dense node whose up-to-256 children all point back at itself or a shared
    // descendant) from pushing `Enter` ops far faster than `nodes_visited` climbs
    // toward a large padded `trie_data.len()` — which the total-work guard alone
    // would let exhaust memory before firing (issue #1629 adversarial reconvergence).
    // A bit-per-offset respects the <128 MB memory target (1/8th of a `Vec<bool>`).
    let mut visited = vec![0u8; trie_data.len().div_ceil(8)];

    while let Some(op) = stack.pop() {
        let DfsOp::Enter {
            node_offset,
            transition,
            depth: node_depth,
        } = op
        else {
            // `DfsOp::Pop`: this node's whole subtree is done — backtrack its
            // transition byte.  Every `Enter` that pushed a byte scheduled exactly
            // one `Pop`, so this always removes exactly the byte this frame pushed
            // (the discarded `Option` is always `Some`); ignore it explicitly.
            let _ = path.pop();
            continue;
        };

        // Extend the shared path with this node's transition byte and schedule its
        // removal AFTER the entire subtree is processed.  The `Pop` is pushed
        // BEFORE this node's children so, LIFO, it runs once every child subtree
        // has been fully walked.  The root (transition `None`) contributes no byte
        // and therefore needs no `Pop`.
        if let Some(b) = transition {
            path.push(b);
            stack.push(DfsOp::Pop);
        }

        nodes_visited = nodes_visited.saturating_add(1);
        // Issue #1618 (H5): count every node the DFS enters (L1/L3: <40 nodes visited).
        crate::storage::sstable::read_work_counters::record_bti_node_visited();
        if nodes_visited > trie_data.len() {
            return Err(Error::Parse(format!(
                "BTI DFS exceeded total work bound ({nodes_visited} nodes visited > \
                 trie size {}; corrupt or cyclic trie)",
                trie_data.len()
            )));
        }
        if path.len() > trie_data.len() {
            return Err(Error::Parse(format!(
                "BTI DFS key path exceeds trie size {} (corrupt or cyclic trie)",
                trie_data.len()
            )));
        }
        if node_depth > trie_data.len().saturating_add(1) {
            return Err(Error::Parse(format!(
                "BTI DFS exceeded node depth (path of {node_depth} nodes > trie size {} + 1; \
                 corrupt or cyclic trie)",
                trie_data.len()
            )));
        }
        if node_offset >= trie_data.len() {
            return Err(Error::Parse(format!(
                "BTI DFS: node_offset {node_offset} out of bounds (trie size {})",
                trie_data.len()
            )));
        }

        // Visited-offset guard: reject the FIRST re-entry of any node offset.
        // Fires BEFORE this node's children are decoded/scheduled, so a cycle or a
        // DAG-style reconvergence cannot inflate the op stack (or `nodes_visited`)
        // past a single extra visit.  Runs after the OOB check so `node_offset` is
        // a valid index into the bitset.
        let word = node_offset >> 3;
        let bit = 1u8 << (node_offset & 7);
        if visited[word] & bit != 0 {
            return Err(Error::Parse(format!(
                "BTI DFS revisited node offset {node_offset} (corrupt or cyclic trie)"
            )));
        }
        visited[word] |= bit;

        // 1) Emit this node's own payload (if any) BEFORE descending — the key
        //    terminating here sorts before any continuation.  The visitor receives
        //    `path` as a borrowed slice; any copy is the visitor's choice (per
        //    emitted result), never per child edge.
        if let Some(payload) = decode_payload(trie_data, node_offset)? {
            visit(&path, payload);
        }

        // 2) Descend children in ASCENDING transition-byte order.  Because the
        //    stack is LIFO, push their `Enter` ops in DESCENDING order.  The `Pop`
        //    scheduled above already sits below these ops, so it fires after the
        //    entire subtree and backtracks this node's transition byte.
        let node = parse_bti_node_for_traversal(trie_data, node_offset)?;
        let children = ordered_children(&node);
        let child_depth = node_depth.saturating_add(1);
        for &(transition_byte, child_offset) in children.iter().rev() {
            stack.push(DfsOp::Enter {
                node_offset: child_offset,
                transition: Some(transition_byte),
                depth: child_depth,
            });
        }
    }

    Ok(())
}

/// Thin owned-key wrapper over [`dfs_visit_in_order`]: collects
/// `(reconstructed_key, decoded_payload)` in byte-comparable order.  Each result
/// pays ONE `to_vec()` for its key (inherent to the owned return type), never a
/// per-child-edge copy.  Offset-only callers should use [`dfs_visit_in_order`]
/// directly (e.g. [`dfs_collect_partition_locations`]) to allocate nothing.
pub(crate) fn dfs_collect_in_order<T, F>(
    trie_data: &[u8],
    root_offset: usize,
    decode_payload: F,
) -> BtiResult<Vec<(Vec<u8>, T)>>
where
    F: FnMut(&[u8], usize) -> BtiResult<Option<T>>,
{
    let mut results: Vec<(Vec<u8>, T)> = Vec::new();
    dfs_visit_in_order(trie_data, root_offset, decode_payload, |key, payload| {
        results.push((key.to_vec(), payload));
    })?;
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

/// Offset-only counterpart to [`dfs_collect_partition_entries`]: enumerate every
/// partition [`BtiPartitionLocation`] in byte-comparable order WITHOUT
/// materializing the reconstructed token keys.
///
/// The reconstructed key is handed to the visitor as a borrowed slice and
/// dropped, so a caller that needs only the locations (e.g. the next-partition
/// seek-bound resolver in `partition_successor`) pays **zero** per-entry key-`Vec`
/// allocations — only the single `Vec<BtiPartitionLocation>` grows (issue #1649).
pub(crate) fn dfs_collect_partition_locations(
    trie_data: &[u8],
    root_offset: usize,
) -> BtiResult<Vec<BtiPartitionLocation>> {
    let mut locations: Vec<BtiPartitionLocation> = Vec::new();
    dfs_visit_in_order(
        trie_data,
        root_offset,
        |data, off| read_node_payload(data, off),
        |_key, location| locations.push(location),
    )?;
    Ok(locations)
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

/// Offset-only counterpart to [`iterate_partitions_in_bti_file`]: enumerate every
/// partition [`BtiPartitionLocation`] in a real Cassandra 5.0 `Partitions.db` BTI
/// file in byte-comparable order, WITHOUT reconstructing (or allocating) the
/// token keys (issue #1649 / L2).
///
/// Use this when the reconstructed byte-comparable token key is not needed (the
/// offsets are definitive) — the DFS then performs **zero** per-partition
/// key-`Vec` allocations. Returns an empty Vec for a `< 8`-byte (e.g. empty) file.
pub fn iterate_partition_locations_in_bti_file<R: Read + Seek>(
    reader: &mut R,
) -> BtiResult<Vec<BtiPartitionLocation>> {
    let file_size = reader.seek(SeekFrom::End(0))?;
    if file_size < 8 {
        return Ok(Vec::new());
    }
    let (trie_data, root_offset) = load_bti_trie_via_footer(reader)?;
    dfs_collect_partition_locations(&trie_data, root_offset)
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

    /// Issue #1629 (roborev regression): a legitimate reconstructed key path
    /// LONGER than the old `u16::MAX` (65535) cap must decode WITHOUT error.
    ///
    /// This locks the fix for roborev's finding: the byte-comparable *encoded*
    /// trie path (one transition byte per edge) can legitimately exceed the
    /// 64 KiB−1 raw-component limit because OSS50 byte-comparable encoding adds
    /// component terminators, 0x00/0xFF escapes and multi-component framing.  A
    /// fixed `u16::MAX` cap therefore rejects a legal key.  The trie-size-relative
    /// bounds accept it: here the reconstructed path is 70 000 bytes and the trie
    /// is ~140 003 bytes, so no bound fires.  This FAILS on 846e1e03 (the 65535
    /// cap rejects it) and passes with the trie-relative bounds.
    #[test]
    fn dfs_long_encoded_path_over_u16_max_decodes() {
        let (trie, root) = partition_chain_to_leaf(70_000);
        assert!(
            70_000 > u16::MAX as usize,
            "the path must exceed the removed u16::MAX cap to be a regression"
        );
        let entries = dfs_collect_partition_entries(&trie, root)
            .expect("a >65535-byte encoded key path must decode (no fixed cap)");
        assert_eq!(entries.len(), 1, "the single leaf must be emitted");
        assert_eq!(
            entries[0].0.len(),
            70_000,
            "reconstructed key path is 70000 bytes (one per transition)"
        );
        assert_eq!(entries[0].1, BtiPartitionLocation::DataOffset(0));
    }

    /// Issue #1629 defect 2: an adversarial *reconverging* trie whose node visits
    /// would grow super-linearly (each internal node has two transitions pointing at
    /// the SAME child) must terminate rather than hang.  With the visited-offset
    /// guard the SHARED child trips the revisit/cyclic error on its second entry
    /// (before the total-work bound), so we assert the revisit message and, above
    /// all, that it does NOT hang or explode.
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
            msg.contains("revisited node offset") || msg.contains("total work bound"),
            "expected the revisit/total-work error, got: {msg}"
        );
    }

    /// Issue #1629: a genuinely CYCLIC trie (a back-edge self-loop) must error
    /// with one of the trie-size-relative messages and must NOT hang.
    ///
    /// A `SingleNoPayload4` node whose transition points back at itself (delta 0)
    /// creates an infinite root→node path.  Because in DFS every ancestor is
    /// popped before its descendants, `nodes_visited` is always `>=` the current
    /// node depth, so the airtight total-work bound (`nodes_visited >
    /// trie_data.len()`) fires first — but any of the three trie-relative
    /// messages is an acceptable, diagnosable outcome.  The key property is that
    /// a fixed-constant cap is gone yet the cycle still terminates cleanly.
    #[test]
    fn dfs_cyclic_trie_errors_with_trie_relative_message() {
        // SingleNoPayload4 (ordinal 1): [0x10 | delta][transition].  delta 0 ⇒
        // child.distance resolves to this node's own offset ⇒ self-loop.
        let trie = vec![0x10u8, 0x01u8];
        let err = dfs_collect_partition_entries(&trie, 0)
            .expect_err("a cyclic (self-looping) trie must error, not hang");
        let msg = format!("{err}");
        assert!(
            msg.contains("revisited node offset")
                || msg.contains("total work bound")
                || msg.contains("key path exceeds trie size")
                || msg.contains("node depth"),
            "expected a trie-size-relative error, got: {msg}"
        );
    }

    /// Issue #1629 (roborev): the total-work guard scales with the ENTIRE trie byte
    /// length, so it does NOT bound the PENDING OP STACK.  A small high-fan-out node
    /// that self-loops (or reconverges), sitting in a LARGELY PADDED trie, can push
    /// `Enter` ops far faster than `nodes_visited` climbs toward `trie_data.len()` —
    /// exhausting memory before the work bound fires.  The visited-offset guard must
    /// reject the FIRST re-entry, so this returns Err (the revisit/cyclic message)
    /// after visiting a handful of nodes, never exploding the op stack.
    #[test]
    fn dfs_padded_reconvergence_rejected_before_stack_blowup() {
        // Sparse8 at offset 0 with 200 transitions, EVERY delta = 0 ⇒ every child
        // resolves to offset 0 (self-loop).  Without the visited guard, each entry
        // would push 200 more `Enter` ops while `nodes_visited` rose by only 1, so
        // the op stack would balloon to ~200 × trie_len before the total-work bound
        // tripped.
        let fan_out: usize = 200;
        let mut trie = vec![0x50u8, fan_out as u8]; // Sparse8, no payload
        trie.extend((1..=fan_out).map(|b| b as u8)); // 200 distinct ascending transitions
        trie.resize(trie.len() + fan_out, 0); // 200 deltas, all 0 → self-loop
        let live_len = trie.len();
        // Pad the trie MUCH larger than the live node so the old total-work bound
        // (== trie_data.len()) would have allowed ~100k self-visits.
        trie.resize(live_len + 100_000, 0);
        assert!(
            trie.len() > live_len * 100,
            "padded trie must dwarf the live node so total-work alone would not save us"
        );

        let err = dfs_collect_partition_entries(&trie, 0)
            .expect_err("a padded high-fan-out self-loop must error, not blow up the stack");
        let msg = format!("{err}");
        assert!(
            msg.contains("revisited node offset"),
            "the visited guard must fire first (before total work), got: {msg}"
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
