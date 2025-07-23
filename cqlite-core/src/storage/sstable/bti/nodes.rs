//! BTI trie node types and operations
//!
//! Implements the four BTI trie node types: PAYLOAD_ONLY, SINGLE, SPARSE, and DENSE

use crate::error::Result;
use super::{BtiError, MAX_TRIE_DEPTH, BTI_PAGE_SIZE};
use nom::{
    bytes::complete::take,
    number::complete::{be_u8, be_u16, be_u32, be_u64},
    IResult,
};
use std::collections::HashMap;

/// BTI trie node types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    /// Final node with no transitions (leaf nodes)
    PayloadOnly = 0,
    /// Node with exactly one transition
    Single = 1,
    /// Node with multiple transitions, binary-searched
    Sparse = 2,
    /// Node with consecutive character range transitions
    Dense = 3,
}

impl NodeType {
    /// Parse node type from header byte
    pub fn from_header_byte(header: u8) -> Result<NodeType> {
        match (header >> 4) & 0x0F {
            0 => Ok(NodeType::PayloadOnly),
            1 => Ok(NodeType::Single),
            2 => Ok(NodeType::Sparse),
            3 => Ok(NodeType::Dense),
            other => Err(BtiError::InvalidNodeType(other).into()),
        }
    }

    /// Get header byte for this node type
    pub fn to_header_byte(self, payload_flags: u8) -> u8 {
        ((self as u8) << 4) | (payload_flags & 0x0F)
    }
}

/// Node reference (pointer to another node)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeRef {
    /// Offset from current position (for compressed pointers)
    pub offset: i64,
    /// Absolute file position (computed)
    pub absolute_position: u64,
}

impl NodeRef {
    /// Create a new node reference
    pub fn new(offset: i64, base_position: u64) -> Self {
        Self {
            offset,
            absolute_position: (base_position as i64 + offset) as u64,
        }
    }

    /// Create null reference
    pub fn null() -> Self {
        Self {
            offset: 0,
            absolute_position: 0,
        }
    }

    /// Check if this is a null reference
    pub fn is_null(&self) -> bool {
        self.absolute_position == 0
    }
}

/// BTI trie node
#[derive(Debug, Clone)]
pub enum TrieNode {
    /// Final node with payload data
    PayloadOnly {
        payload: Vec<u8>,
    },
    /// Node with single transition
    Single {
        character: u8,
        target: NodeRef,
        payload: Option<Vec<u8>>,
    },
    /// Node with multiple transitions (binary searched)
    Sparse {
        transitions: Vec<(u8, NodeRef)>,
        payload: Option<Vec<u8>>,
    },
    /// Node with dense character range
    Dense {
        first_char: u8,
        last_char: u8,
        targets: Vec<Option<NodeRef>>,
        payload: Option<Vec<u8>>,
    },
}

impl TrieNode {
    /// Get node type
    pub fn node_type(&self) -> NodeType {
        match self {
            TrieNode::PayloadOnly { .. } => NodeType::PayloadOnly,
            TrieNode::Single { .. } => NodeType::Single,
            TrieNode::Sparse { .. } => NodeType::Sparse,
            TrieNode::Dense { .. } => NodeType::Dense,
        }
    }

    /// Get payload data if present
    pub fn payload(&self) -> Option<&[u8]> {
        match self {
            TrieNode::PayloadOnly { payload } => Some(payload),
            TrieNode::Single { payload, .. } => payload.as_deref(),
            TrieNode::Sparse { payload, .. } => payload.as_deref(),
            TrieNode::Dense { payload, .. } => payload.as_deref(),
        }
    }

    /// Find target node for given character
    pub fn find_transition(&self, ch: u8) -> Option<NodeRef> {
        match self {
            TrieNode::PayloadOnly { .. } => None,
            TrieNode::Single { character, target, .. } => {
                if *character == ch {
                    Some(*target)
                } else {
                    None
                }
            }
            TrieNode::Sparse { transitions, .. } => {
                // Binary search through transitions
                transitions
                    .binary_search_by_key(&ch, |(c, _)| *c)
                    .ok()
                    .map(|idx| transitions[idx].1)
            }
            TrieNode::Dense { first_char, last_char, targets, .. } => {
                if ch >= *first_char && ch <= *last_char {
                    let idx = (ch - first_char) as usize;
                    targets.get(idx).and_then(|t| *t)
                } else {
                    None
                }
            }
        }
    }

    /// Get all valid transitions from this node
    pub fn get_transitions(&self) -> Vec<(u8, NodeRef)> {
        match self {
            TrieNode::PayloadOnly { .. } => Vec::new(),
            TrieNode::Single { character, target, .. } => {
                vec![(*character, *target)]
            }
            TrieNode::Sparse { transitions, .. } => transitions.clone(),
            TrieNode::Dense { first_char, last_char, targets, .. } => {
                let mut result = Vec::new();
                for (i, target) in targets.iter().enumerate() {
                    if let Some(target_ref) = target {
                        let ch = *first_char + i as u8;
                        result.push((ch, *target_ref));
                    }
                }
                result
            }
        }
    }

    /// Estimate serialized size of this node
    pub fn estimated_size(&self) -> usize {
        match self {
            TrieNode::PayloadOnly { payload } => 1 + payload.len(),
            TrieNode::Single { payload, .. } => {
                1 + 1 + 8 + payload.as_ref().map_or(0, |p| p.len())
            }
            TrieNode::Sparse { transitions, payload } => {
                1 + 2 + transitions.len() * (1 + 8) + payload.as_ref().map_or(0, |p| p.len())
            }
            TrieNode::Dense { first_char, last_char, targets, payload } => {
                let range_size = (*last_char - *first_char + 1) as usize;
                1 + 2 + range_size * 8 + payload.as_ref().map_or(0, |p| p.len())
            }
        }
    }
}

/// BTI trie node parser
pub struct NodeParser {
    /// Current parsing position
    position: u64,
    /// Page cache for efficient reading
    page_cache: HashMap<u64, Vec<u8>>,
}

impl NodeParser {
    /// Create new node parser
    pub fn new() -> Self {
        Self {
            position: 0,
            page_cache: HashMap::new(),
        }
    }

    /// Parse a BTI trie node from bytes
    pub fn parse_node(&mut self, input: &[u8], position: u64) -> IResult<&[u8], TrieNode> {
        self.position = position;
        
        let (input, header) = be_u8(input)?;
        let node_type = NodeType::from_header_byte(header)
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;
        let payload_flags = header & 0x0F;
        
        match node_type {
            NodeType::PayloadOnly => self.parse_payload_only_node(input, payload_flags),
            NodeType::Single => self.parse_single_node(input, payload_flags),
            NodeType::Sparse => self.parse_sparse_node(input, payload_flags),
            NodeType::Dense => self.parse_dense_node(input, payload_flags),
        }
    }

    /// Parse PAYLOAD_ONLY node
    fn parse_payload_only_node(&self, input: &[u8], flags: u8) -> IResult<&[u8], TrieNode> {
        let has_payload = (flags & 0x01) != 0;
        
        if has_payload {
            let (input, payload_size) = be_u16(input)?;
            let (input, payload) = take(payload_size as usize)(input)?;
            Ok((input, TrieNode::PayloadOnly {
                payload: payload.to_vec(),
            }))
        } else {
            Ok((input, TrieNode::PayloadOnly {
                payload: Vec::new(),
            }))
        }
    }

    /// Parse SINGLE node
    fn parse_single_node(&self, input: &[u8], flags: u8) -> IResult<&[u8], TrieNode> {
        let has_payload = (flags & 0x01) != 0;
        
        let (input, character) = be_u8(input)?;
        let (input, target_offset) = self.parse_compressed_pointer(input)?;
        let target = NodeRef::new(target_offset, self.position);
        
        let (input, payload) = if has_payload {
            let (input, payload_size) = be_u16(input)?;
            let (input, payload_data) = take(payload_size as usize)(input)?;
            (input, Some(payload_data.to_vec()))
        } else {
            (input, None)
        };
        
        Ok((input, TrieNode::Single {
            character,
            target,
            payload,
        }))
    }

    /// Parse SPARSE node
    fn parse_sparse_node(&self, input: &[u8], flags: u8) -> IResult<&[u8], TrieNode> {
        let has_payload = (flags & 0x01) != 0;
        
        let (input, transition_count) = be_u8(input)?;
        let mut transitions = Vec::with_capacity(transition_count as usize);
        let mut remaining = input;
        
        // Parse characters
        let mut characters = Vec::with_capacity(transition_count as usize);
        for _ in 0..transition_count {
            let (new_remaining, ch) = be_u8(remaining)?;
            characters.push(ch);
            remaining = new_remaining;
        }
        
        // Parse targets
        for ch in characters {
            let (new_remaining, target_offset) = self.parse_compressed_pointer(remaining)?;
            let target = NodeRef::new(target_offset, self.position);
            transitions.push((ch, target));
            remaining = new_remaining;
        }
        
        let (remaining, payload) = if has_payload {
            let (remaining, payload_size) = be_u16(remaining)?;
            let (remaining, payload_data) = take(payload_size as usize)(remaining)?;
            (remaining, Some(payload_data.to_vec()))
        } else {
            (remaining, None)
        };
        
        Ok((remaining, TrieNode::Sparse {
            transitions,
            payload,
        }))
    }

    /// Parse DENSE node
    fn parse_dense_node(&self, input: &[u8], flags: u8) -> IResult<&[u8], TrieNode> {
        let has_payload = (flags & 0x01) != 0;
        
        let (input, first_char) = be_u8(input)?;
        let (input, last_char) = be_u8(input)?;
        
        if first_char > last_char {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
        }
        
        let range_size = (last_char - first_char + 1) as usize;
        let mut targets = Vec::with_capacity(range_size);
        let mut remaining = input;
        
        // Parse targets (may include null pointers)
        for _ in 0..range_size {
            let (new_remaining, target_offset) = self.parse_compressed_pointer(remaining)?;
            let target = if target_offset == 0 {
                None
            } else {
                Some(NodeRef::new(target_offset, self.position))
            };
            targets.push(target);
            remaining = new_remaining;
        }
        
        let (remaining, payload) = if has_payload {
            let (remaining, payload_size) = be_u16(remaining)?;
            let (remaining, payload_data) = take(payload_size as usize)(remaining)?;
            (remaining, Some(payload_data.to_vec()))
        } else {
            (remaining, None)
        };
        
        Ok((remaining, TrieNode::Dense {
            first_char,
            last_char,
            targets,
            payload,
        }))
    }

    /// Parse compressed pointer (variable-size based on distance)
    fn parse_compressed_pointer(&self, input: &[u8]) -> IResult<&[u8], i64> {
        // For now, use fixed 64-bit pointers - can be optimized later
        let (input, offset) = be_u64(input)?;
        Ok((input, offset as i64))
    }
}

/// Node type selection for optimal storage
pub fn select_optimal_node_type(transitions: &[(u8, NodeRef)]) -> NodeType {
    match transitions.len() {
        0 => NodeType::PayloadOnly,
        1 => NodeType::Single,
        n => {
            // Check if DENSE encoding would be more efficient
            if let Some((min_char, max_char)) = get_character_range(transitions) {
                let range_size = (max_char - min_char + 1) as usize;
                let dense_size = 1 + 2 + range_size * 8; // header + range + targets
                let sparse_size = 1 + 1 + n * (1 + 8);   // header + count + char+target pairs
                
                if dense_size <= sparse_size && range_size <= 256 {
                    NodeType::Dense
                } else {
                    NodeType::Sparse
                }
            } else {
                NodeType::Sparse
            }
        }
    }
}

/// Get character range for transitions
fn get_character_range(transitions: &[(u8, NodeRef)]) -> Option<(u8, u8)> {
    if transitions.is_empty() {
        return None;
    }
    
    let mut min_char = transitions[0].0;
    let mut max_char = transitions[0].0;
    
    for &(ch, _) in transitions.iter().skip(1) {
        min_char = min_char.min(ch);
        max_char = max_char.max(ch);
    }
    
    Some((min_char, max_char))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_type_parsing() {
        assert_eq!(NodeType::from_header_byte(0x00).unwrap(), NodeType::PayloadOnly);
        assert_eq!(NodeType::from_header_byte(0x10).unwrap(), NodeType::Single);
        assert_eq!(NodeType::from_header_byte(0x20).unwrap(), NodeType::Sparse);
        assert_eq!(NodeType::from_header_byte(0x30).unwrap(), NodeType::Dense);
        
        assert!(NodeType::from_header_byte(0x40).is_err());
    }

    #[test]
    fn test_node_type_header_byte() {
        assert_eq!(NodeType::PayloadOnly.to_header_byte(0x05), 0x05);
        assert_eq!(NodeType::Single.to_header_byte(0x03), 0x13);
        assert_eq!(NodeType::Sparse.to_header_byte(0x01), 0x21);
        assert_eq!(NodeType::Dense.to_header_byte(0x07), 0x37);
    }

    #[test]
    fn test_node_ref() {
        let node_ref = NodeRef::new(100, 1000);
        assert_eq!(node_ref.offset, 100);
        assert_eq!(node_ref.absolute_position, 1100);
        
        let null_ref = NodeRef::null();
        assert!(null_ref.is_null());
    }

    #[test]
    fn test_optimal_node_type_selection() {
        // Empty transitions -> PayloadOnly
        assert_eq!(select_optimal_node_type(&[]), NodeType::PayloadOnly);
        
        // Single transition -> Single
        let single = vec![(b'a', NodeRef::null())];
        assert_eq!(select_optimal_node_type(&single), NodeType::Single);
        
        // Dense range -> Dense (a, b, c)
        let dense = vec![
            (b'a', NodeRef::null()),
            (b'b', NodeRef::null()),
            (b'c', NodeRef::null()),
        ];
        assert_eq!(select_optimal_node_type(&dense), NodeType::Dense);
        
        // Sparse transitions -> Sparse (a, x, z)
        let sparse = vec![
            (b'a', NodeRef::null()),
            (b'x', NodeRef::null()),
            (b'z', NodeRef::null()),
        ];
        assert_eq!(select_optimal_node_type(&sparse), NodeType::Sparse);
    }

    #[test]
    fn test_trie_node_transitions() {
        let payload_node = TrieNode::PayloadOnly {
            payload: vec![1, 2, 3],
        };
        assert_eq!(payload_node.find_transition(b'a'), None);
        assert_eq!(payload_node.get_transitions().len(), 0);
        
        let single_node = TrieNode::Single {
            character: b'a',
            target: NodeRef::new(100, 1000),
            payload: None,
        };
        assert!(single_node.find_transition(b'a').is_some());
        assert_eq!(single_node.find_transition(b'b'), None);
        assert_eq!(single_node.get_transitions().len(), 1);
    }

    #[test]
    fn test_character_range() {
        let transitions = vec![
            (b'a', NodeRef::null()),
            (b'c', NodeRef::null()),
            (b'b', NodeRef::null()),
        ];
        
        let (min, max) = get_character_range(&transitions).unwrap();
        assert_eq!(min, b'a');
        assert_eq!(max, b'c');
        
        assert_eq!(get_character_range(&[]), None);
    }
}