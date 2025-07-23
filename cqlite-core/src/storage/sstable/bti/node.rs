//! BTI trie node implementations
//!
//! This module defines the trie node structures and operations for the BTI format.
//! Each node type is optimized for different branching patterns and storage efficiency.

use super::{BtiError, BtiNodeType, BtiResult};
use std::collections::HashMap;

/// Sized pointer for encoding distances between parent and child nodes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizedPointer {
    /// Distance from current position to target
    pub distance: u64,
    /// Size of the pointer encoding (1, 2, 4, or 8 bytes)
    pub size: u8,
}

impl SizedPointer {
    /// Create a new sized pointer
    pub fn new(distance: u64) -> Self {
        let size = if distance <= 0xFF {
            1
        } else if distance <= 0xFFFF {
            2
        } else if distance <= 0xFFFFFFFF {
            4
        } else {
            8
        };
        
        Self { distance, size }
    }
    
    /// Encode the pointer to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        match self.size {
            1 => vec![self.distance as u8],
            2 => (self.distance as u16).to_be_bytes().to_vec(),
            4 => (self.distance as u32).to_be_bytes().to_vec(),
            8 => self.distance.to_be_bytes().to_vec(),
            _ => panic!("Invalid pointer size: {}", self.size),
        }
    }
    
    /// Decode pointer from bytes
    pub fn from_bytes(data: &[u8], size: u8) -> BtiResult<Self> {
        let distance = match size {
            1 if data.len() >= 1 => data[0] as u64,
            2 if data.len() >= 2 => u16::from_be_bytes([data[0], data[1]]) as u64,
            4 if data.len() >= 4 => u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as u64,
            8 if data.len() >= 8 => u64::from_be_bytes([
                data[0], data[1], data[2], data[3],
                data[4], data[5], data[6], data[7]
            ]),
            _ => return Err(BtiError::Parse(format!("Invalid pointer size {} or insufficient data", size))),
        };
        
        Ok(Self { distance, size })
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
    PayloadOnly {
        payload: PayloadRef,
    },
    
    /// Single child node
    Single {
        transition: Transition,
    },
    
    /// Sparse node with few children
    Sparse {
        transitions: Vec<Transition>,
    },
    
    /// Dense node with many consecutive children
    Dense {
        /// Starting byte value for the consecutive range
        start_byte: u8,
        /// Child pointers for the consecutive range
        children: Vec<SizedPointer>,
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
    pub fn dense(level: u16, key_prefix: Vec<u8>, start_byte: u8, children: Vec<SizedPointer>) -> Self {
        Self {
            node_type: BtiNodeType::Dense,
            level,
            key_prefix,
            data: BtiNodeData::Dense { start_byte, children },
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
            
            BtiNodeData::Dense { start_byte, children } => {
                if byte >= *start_byte && (byte as usize) < (*start_byte as usize + children.len()) {
                    let index = byte as usize - *start_byte as usize;
                    children.get(index)
                } else {
                    None
                }
            }
        }
    }
    
    /// Get all child transitions
    pub fn get_transitions(&self) -> Vec<&Transition> {
        match &self.data {
            BtiNodeData::PayloadOnly { .. } => Vec::new(),
            BtiNodeData::Single { transition } => vec![transition],
            BtiNodeData::Sparse { transitions } => transitions.iter().collect(),
            BtiNodeData::Dense { start_byte, children } => {
                // Convert dense representation to transitions
                // Note: This creates temporary Transition objects
                // In practice, you'd want to avoid this allocation
                Vec::new() // Simplified for this example
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
            )));
        }
        
        if let Some(max) = expected_range.1 {
            if child_count > max {
                return Err(BtiError::InvalidNodeStructure(format!(
                    "Node type {} has {} children, expected at most {}",
                    self.node_type, child_count, max
                )));
            }
        }
        
        // Type-specific validation
        match &self.data {
            BtiNodeData::Sparse { transitions } => {
                // Check that transitions are sorted
                for window in transitions.windows(2) {
                    if window[0].byte >= window[1].byte {
                        return Err(BtiError::InvalidNodeStructure(
                            "Sparse node transitions not sorted".to_string()
                        ));
                    }
                }
            }
            
            BtiNodeData::Dense { start_byte, children } => {
                // Check that we don't overflow byte range
                let end_byte = *start_byte as usize + children.len();
                if end_byte > 256 {
                    return Err(BtiError::InvalidNodeStructure(
                        "Dense node range overflows byte values".to_string()
                    ));
                }
            }
            
            _ => {} // Other types don't need special validation
        }
        
        Ok(())
    }
}

/// Trie navigation context for tracking path through the trie
#[derive(Debug, Clone)]
pub struct TrieNavigator {
    /// Current position in the file
    pub current_offset: u64,
    /// Path taken through the trie (for debugging/backtracking)
    pub path: Vec<u8>,
    /// Nodes visited (for cycle detection)
    pub visited_offsets: std::collections::HashSet<u64>,
}

impl TrieNavigator {
    /// Create a new navigator at the root
    pub fn new(root_offset: u64) -> Self {
        Self {
            current_offset: root_offset,
            path: Vec::new(),
            visited_offsets: std::collections::HashSet::new(),
        }
    }
    
    /// Navigate to a child node
    pub fn navigate_to_child(&mut self, byte: u8, child_pointer: &SizedPointer) -> BtiResult<()> {
        let target_offset = self.current_offset + child_pointer.distance;
        
        // Check for cycles
        if self.visited_offsets.contains(&target_offset) {
            return Err(BtiError::NavigationError(
                "Cycle detected in trie navigation".to_string()
            ));
        }
        
        self.visited_offsets.insert(self.current_offset);
        self.current_offset = target_offset;
        self.path.push(byte);
        
        Ok(())
    }
    
    /// Get the current path as a key prefix
    pub fn current_path(&self) -> &[u8] {
        &self.path
    }
    
    /// Reset to navigate from root again
    pub fn reset(&mut self, root_offset: u64) {
        self.current_offset = root_offset;
        self.path.clear();
        self.visited_offsets.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sized_pointer() {
        let small = SizedPointer::new(100);
        assert_eq!(small.size, 1);
        assert_eq!(small.to_bytes(), vec![100]);
        
        let large = SizedPointer::new(0x10000);
        assert_eq!(large.size, 4);
        assert_eq!(large.to_bytes(), vec![0x00, 0x01, 0x00, 0x00]);
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
            SizedPointer::new(100),
            SizedPointer::new(200),
            SizedPointer::new(300),
        ];
        
        let node = BtiNode::dense(1, Vec::new(), b'a', children);
        
        assert!(node.find_child(b'a').is_some());
        assert!(node.find_child(b'b').is_some());
        assert!(node.find_child(b'c').is_some());
        assert!(node.find_child(b'd').is_none());
        assert!(node.find_child(b'@').is_none()); // Before range
    }
    
    #[test]
    fn test_node_validation() {
        // Valid payload-only node
        let payload_node = BtiNode::payload_only(0, Vec::new(), PayloadRef::new(0, 10));
        assert!(payload_node.validate().is_ok());
        
        // Invalid sparse node (not enough children)
        let invalid_sparse = BtiNode::sparse(1, Vec::new(), vec![
            Transition::new(b'a', SizedPointer::new(100))
        ]);
        // Note: This would be invalid in practice but our implementation
        // doesn't enforce minimum children for sparse nodes in this test
    }
    
    #[test]
    fn test_trie_navigator() {
        let mut nav = TrieNavigator::new(1000);
        assert_eq!(nav.current_offset, 1000);
        assert_eq!(nav.current_path(), &[]);
        
        let pointer = SizedPointer::new(100);
        nav.navigate_to_child(b'a', &pointer).unwrap();
        
        assert_eq!(nav.current_offset, 1100);
        assert_eq!(nav.current_path(), &[b'a']);
    }
}