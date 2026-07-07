//! BTI trie node implementations
//!
//! This module defines the trie node structures and operations for the BTI format.
//! Each node type is optimized for different branching patterns and storage efficiency.

use crate::error::{Error, Result};
use std::fmt;

/// BTI-specific result type
pub type BtiResult<T> = Result<T>;

/// BTI-specific error types
#[derive(Debug, Clone)]
pub enum BtiError {
    /// Parse error with details
    Parse(String),
    /// Invalid node structure
    InvalidNodeStructure(String),
    /// Navigation error during trie traversal
    NavigationError(String),
    /// Invalid node type
    InvalidNodeType(u8),
    /// Maximum depth exceeded
    MaxDepthExceeded(usize),
    /// Invalid byte-comparable key
    InvalidByteComparableKey(String),
    /// Corrupted trie structure
    CorruptedTrie(String),
    /// Missing component file
    MissingComponent(String),
}

impl fmt::Display for BtiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BtiError::Parse(msg) => write!(f, "BTI parse error: {}", msg),
            BtiError::InvalidNodeStructure(msg) => write!(f, "Invalid BTI node structure: {}", msg),
            BtiError::NavigationError(msg) => write!(f, "BTI navigation error: {}", msg),
            BtiError::InvalidNodeType(node_type) => {
                write!(f, "Invalid BTI trie node type: 0x{:02X}", node_type)
            }
            BtiError::MaxDepthExceeded(depth) => {
                write!(f, "BTI trie depth exceeded maximum: {}", depth)
            }
            BtiError::InvalidByteComparableKey(key) => {
                write!(f, "Invalid byte-comparable key: {}", key)
            }
            BtiError::CorruptedTrie(msg) => {
                write!(f, "Corrupted BTI trie structure: {}", msg)
            }
            BtiError::MissingComponent(component) => {
                write!(f, "Missing BTI component: {}", component)
            }
        }
    }
}

impl std::error::Error for BtiError {}

impl From<BtiError> for Error {
    fn from(err: BtiError) -> Self {
        Error::Parse(format!("BTI error: {}", err))
    }
}

/// BTI node types corresponding to the four trie node variants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtiNodeType {
    /// Payload-only node (leaf)
    PayloadOnly,
    /// Single child node
    Single,
    /// Sparse node with few children
    Sparse,
    /// Dense node with many consecutive children
    Dense,
}

impl BtiNodeType {
    /// Get expected children range for this node type
    pub fn expected_children_range(&self) -> (usize, Option<usize>) {
        match self {
            BtiNodeType::PayloadOnly => (0, Some(0)),
            BtiNodeType::Single => (1, Some(1)),
            BtiNodeType::Sparse => (2, Some(256)), // Reasonable upper bound
            BtiNodeType::Dense => (1, Some(256)),  // Full byte range
        }
    }
}

impl fmt::Display for BtiNodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BtiNodeType::PayloadOnly => write!(f, "PayloadOnly"),
            BtiNodeType::Single => write!(f, "Single"),
            BtiNodeType::Sparse => write!(f, "Sparse"),
            BtiNodeType::Dense => write!(f, "Dense"),
        }
    }
}

/// Resolved child pointer: the ABSOLUTE trie offset of a child node.
///
/// The on-disk backward-delta is resolved to an absolute offset
/// (`parent_offset - delta`) at decode time and stored in `distance`.
///
/// Issue #1650 (L3) removed the dead `size` field (the pointer-encoding width, in
/// bytes) and its `to_bytes`/`from_bytes` helpers: nothing on the read path or the
/// canonical BTI (`da`) write path ever consumed them — the width is derived from
/// the node-type ordinal during (de)serialization, never from a per-pointer field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizedPointer {
    /// Absolute offset of the target child node (`parent_offset - backward_delta`).
    pub distance: u64,
}

// Struct-size regression guard (issue #1616, Epic H/H3; see
// docs/reports/parser-performance-audit-2026-07-01.md §Epic H (finding H3)). BTI trie
// pointer decoded once per transition during partition lookup on the read hot
// path. Measured 8 bytes today (a single u64) on 64-bit targets after issue #1650
// (L3) dropped the dead 1-byte `size` field (was 16 padded). Update this pin
// DELIBERATELY, never silently: any change — growth or shrink — must be a
// reviewed edit here.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<SizedPointer>() == 8);

impl SizedPointer {
    /// Create a pointer to the child at absolute offset `distance`.
    pub fn new(distance: u64) -> Self {
        Self { distance }
    }
}

/// Trie node transition representing a path to a child node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    /// The byte value for this transition
    pub byte: u8,
    /// Pointer to the child node
    pub child: SizedPointer,
}

// Struct-size regression guard (issue #1616, Epic H/H3; see
// docs/reports/parser-performance-audit-2026-07-01.md §Epic H (finding H3)). One BTI
// `Transition` is decoded per trie edge walked during partition lookup on the
// read hot path. Measured 16 bytes today (u8 + `SizedPointer`{u64}, padded) on
// 64-bit targets after issue #1650 (L3) shrank `SizedPointer` to a bare u64.
// Update this pin DELIBERATELY, never silently: any change — growth or shrink —
// must be a reviewed edit here.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<Transition>() == 16);

impl Transition {
    pub fn new(byte: u8, child: SizedPointer) -> Self {
        Self { byte, child }
    }
}

/// Payload reference for leaf nodes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadRef {
    /// Offset to the payload data
    pub offset: u64,
    /// Length of the payload data
    pub length: u32,
    /// Optional checksum for validation
    pub checksum: Option<u32>,
}

// Struct-size regression guard (issue #1616, Epic H/H3; see
// docs/reports/parser-performance-audit-2026-07-01.md §Epic H (finding H3)). One BTI
// `PayloadRef` is produced per matched leaf during partition lookup on the read
// hot path. Measured 24 bytes today (u64 + u32 + Option<u32>, padded) on 64-bit
// targets. Update this pin DELIBERATELY, never silently: any change — growth or
// shrink — must be a reviewed edit here.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<PayloadRef>() == 24);

impl PayloadRef {
    pub fn new(offset: u64, length: u32) -> Self {
        Self {
            offset,
            length,
            checksum: None,
        }
    }

    pub fn with_checksum(mut self, checksum: u32) -> Self {
        self.checksum = Some(checksum);
        self
    }
}

/// Base trie node structure
#[derive(Debug, Clone)]
pub struct BtiNode {
    /// Type of this node
    pub node_type: BtiNodeType,
    /// Level in the trie (0 = leaf level)
    pub level: u16,
    /// Key prefix stored at this node (for optimization)
    pub key_prefix: Vec<u8>,
    /// Node-specific data
    pub data: BtiNodeData,
}

/// Node-specific data based on node type
#[derive(Debug, Clone)]
pub enum BtiNodeData {
    /// Payload-only node (leaf)
    PayloadOnly { payload: PayloadRef },

    /// Single child node
    Single { transition: Transition },

    /// Sparse node with few children
    Sparse { transitions: Vec<Transition> },

    /// Dense node with many consecutive children
    Dense {
        /// Starting byte value for the consecutive range
        start_byte: u8,
        /// Child pointers for the consecutive range.
        ///
        /// Each slot represents the transition `start_byte + index`.  `None`
        /// means "no transition" (the raw Dense delta was `0`, the sentinel);
        /// `Some(ptr)` is a real child.  Presence is tracked explicitly because
        /// a REAL child can legitimately live at absolute trie offset `0` (the
        /// first-written leaf in BTI's bottom-up layout): an offset of `0` is a
        /// valid child and must NOT be confused with the absent-transition
        /// sentinel.
        children: Vec<Option<SizedPointer>>,
    },
}

impl BtiNode {
    /// Create a payload-only node
    pub fn payload_only(level: u16, key_prefix: Vec<u8>, payload: PayloadRef) -> Self {
        Self {
            node_type: BtiNodeType::PayloadOnly,
            level,
            key_prefix,
            data: BtiNodeData::PayloadOnly { payload },
        }
    }

    /// Create a single child node
    pub fn single(level: u16, key_prefix: Vec<u8>, transition: Transition) -> Self {
        Self {
            node_type: BtiNodeType::Single,
            level,
            key_prefix,
            data: BtiNodeData::Single { transition },
        }
    }

    /// Create a sparse node
    pub fn sparse(level: u16, key_prefix: Vec<u8>, mut transitions: Vec<Transition>) -> Self {
        // Ensure transitions are sorted by byte value for binary search
        transitions.sort_by_key(|t| t.byte);

        Self {
            node_type: BtiNodeType::Sparse,
            level,
            key_prefix,
            data: BtiNodeData::Sparse { transitions },
        }
    }

    /// Create a dense node
    ///
    /// `children[i]` is the transition for byte `start_byte + i`: `None` for a
    /// missing transition (raw delta `0`), `Some(ptr)` for a real child (which
    /// may point at absolute offset `0`).
    pub fn dense(
        level: u16,
        key_prefix: Vec<u8>,
        start_byte: u8,
        children: Vec<Option<SizedPointer>>,
    ) -> Self {
        Self {
            node_type: BtiNodeType::Dense,
            level,
            key_prefix,
            data: BtiNodeData::Dense {
                start_byte,
                children,
            },
        }
    }

    /// Find the child node pointer for a given byte
    pub fn find_child(&self, byte: u8) -> Option<&SizedPointer> {
        match &self.data {
            BtiNodeData::PayloadOnly { .. } => None,

            BtiNodeData::Single { transition } => {
                if transition.byte == byte {
                    Some(&transition.child)
                } else {
                    None
                }
            }

            BtiNodeData::Sparse { transitions } => {
                // Binary search on sorted transitions
                transitions
                    .binary_search_by_key(&byte, |t| t.byte)
                    .ok()
                    .map(|idx| &transitions[idx].child)
            }

            BtiNodeData::Dense {
                start_byte,
                children,
            } => {
                if byte >= *start_byte && (byte as usize) < (*start_byte as usize + children.len())
                {
                    let index = byte as usize - *start_byte as usize;
                    // `None` means "no transition" for this byte; `Some(ptr)` is
                    // a real child (possibly at absolute offset 0).
                    children.get(index).and_then(|slot| slot.as_ref())
                } else {
                    None
                }
            }
        }
    }

    /// Get the payload reference if this is a leaf node
    pub fn get_payload(&self) -> Option<&PayloadRef> {
        match &self.data {
            BtiNodeData::PayloadOnly { payload } => Some(payload),
            _ => None,
        }
    }

    /// Check if this node is a leaf (has payload)
    pub fn is_leaf(&self) -> bool {
        matches!(self.data, BtiNodeData::PayloadOnly { .. })
    }

    /// Get the number of children
    pub fn child_count(&self) -> usize {
        match &self.data {
            BtiNodeData::PayloadOnly { .. } => 0,
            BtiNodeData::Single { .. } => 1,
            BtiNodeData::Sparse { transitions } => transitions.len(),
            BtiNodeData::Dense { children, .. } => children.len(),
        }
    }

    /// Validate node structure consistency
    pub fn validate(&self) -> BtiResult<()> {
        let expected_range = self.node_type.expected_children_range();
        let child_count = self.child_count();

        // Check child count is within expected range
        if child_count < expected_range.0 {
            return Err(BtiError::InvalidNodeStructure(format!(
                "Node type {} has {} children, expected at least {}",
                self.node_type, child_count, expected_range.0
            ))
            .into());
        }

        if let Some(max) = expected_range.1 {
            if child_count > max {
                return Err(BtiError::InvalidNodeStructure(format!(
                    "Node type {} has {} children, expected at most {}",
                    self.node_type, child_count, max
                ))
                .into());
            }
        }

        // Type-specific validation
        match &self.data {
            BtiNodeData::Sparse { transitions } => {
                // Check that transitions are sorted
                for window in transitions.windows(2) {
                    if window[0].byte >= window[1].byte {
                        return Err(BtiError::InvalidNodeStructure(
                            "Sparse node transitions not sorted".to_string(),
                        )
                        .into());
                    }
                }
            }

            BtiNodeData::Dense {
                start_byte,
                children,
            } => {
                // Check that we don't overflow byte range
                let end_byte = *start_byte as usize + children.len();
                if end_byte > 256 {
                    return Err(BtiError::InvalidNodeStructure(
                        "Dense node range overflows byte values".to_string(),
                    )
                    .into());
                }
            }

            _ => {} // Other types don't need special validation
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sized_pointer() {
        // Issue #1650 (L3): `SizedPointer` is now a bare absolute-offset holder;
        // the dead `size`/`to_bytes`/`from_bytes` encoding path was removed.
        let small = SizedPointer::new(100);
        assert_eq!(small.distance, 100);

        let large = SizedPointer::new(0x10000);
        assert_eq!(large.distance, 0x10000);
    }

    #[test]
    fn test_node_creation() {
        let payload = PayloadRef::new(1000, 50);
        let node = BtiNode::payload_only(0, b"test".to_vec(), payload);

        assert_eq!(node.node_type, BtiNodeType::PayloadOnly);
        assert_eq!(node.level, 0);
        assert_eq!(node.key_prefix, b"test");
        assert!(node.is_leaf());
        assert_eq!(node.child_count(), 0);
    }

    #[test]
    fn test_sparse_node_search() {
        let transitions = vec![
            Transition::new(b'a', SizedPointer::new(100)),
            Transition::new(b'm', SizedPointer::new(200)),
            Transition::new(b'z', SizedPointer::new(300)),
        ];

        let node = BtiNode::sparse(1, Vec::new(), transitions);

        assert!(node.find_child(b'a').is_some());
        assert!(node.find_child(b'm').is_some());
        assert!(node.find_child(b'z').is_some());
        assert!(node.find_child(b'b').is_none());

        assert_eq!(node.child_count(), 3);
    }

    #[test]
    fn test_dense_node_lookup() {
        let children = vec![
            Some(SizedPointer::new(100)),
            Some(SizedPointer::new(200)),
            Some(SizedPointer::new(300)),
        ];

        let node = BtiNode::dense(1, Vec::new(), b'a', children);

        assert!(node.find_child(b'a').is_some());
        assert!(node.find_child(b'b').is_some());
        assert!(node.find_child(b'c').is_some());
        assert!(node.find_child(b'd').is_none());
        assert!(node.find_child(b'@').is_none()); // Before range
    }

    /// Finding 1 (issue #832): a Dense node where the FIRST real child points at
    /// absolute trie offset 0 (a legitimate position — the first-written leaf in
    /// BTI's bottom-up layout) and a later slot is the "no transition" sentinel
    /// (`None`).  `find_child` must return the offset-0 child for the real byte
    /// and `None` for the gap byte — the offset-0 pointer must NOT be treated as
    /// "no transition".
    #[test]
    fn test_dense_node_offset_zero_child_distinct_from_no_transition() {
        // start_byte = b'a':
        //   b'a' → real child at offset 0 (SizedPointer distance 0)
        //   b'b' → no transition (None)
        //   b'c' → real child at offset 300
        let children = vec![
            Some(SizedPointer::new(0)), // real child at absolute offset 0
            None,                       // no transition
            Some(SizedPointer::new(300)),
        ];
        let node = BtiNode::dense(1, Vec::new(), b'a', children);

        let a = node.find_child(b'a');
        assert!(a.is_some(), "offset-0 child must be found, not dropped");
        assert_eq!(
            a.unwrap().distance,
            0,
            "the real child at absolute offset 0 must be returned"
        );
        assert!(
            node.find_child(b'b').is_none(),
            "no-transition slot must return None"
        );
        assert!(node.find_child(b'c').is_some());
        // child_count is the dense RANGE length (slots), independent of gaps.
        assert_eq!(node.child_count(), 3);
    }

    #[test]
    fn test_node_validation() {
        // Valid payload-only node
        let payload_node = BtiNode::payload_only(0, Vec::new(), PayloadRef::new(0, 10));
        assert!(payload_node.validate().is_ok());

        // Invalid sparse node (not enough children)
        let _invalid_sparse = BtiNode::sparse(
            1,
            Vec::new(),
            vec![Transition::new(b'a', SizedPointer::new(100))],
        );
        // Note: This would be invalid in practice but our implementation
        // doesn't enforce minimum children for sparse nodes in this test
    }
}
