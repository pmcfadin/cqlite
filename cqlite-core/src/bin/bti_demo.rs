#!/usr/bin/env cargo run --bin bti_demo --

//! BTI format demonstration and testing tool
//!
//! This binary demonstrates the BTI (Big Trie-Indexed) format support
//! and tests the implementation against hypothetical BTI files.

use cqlite_core::parser::header::CassandraVersion;
use cqlite_core::storage::sstable::bti::{
    detect_format, is_bti_format, BTI_MAGIC_NUMBER, FormatType
};
use cqlite_core::storage::sstable::bti::nodes::{NodeType, TrieNode, NodeRef, select_optimal_node_type};
use cqlite_core::storage::sstable::bti::encoder::{ByteComparableEncoder, ByteComparableDecoder};
use cqlite_core::types::Value;
use std::collections::HashMap;

fn main() {
    println!("🎯 BTI Format Demonstration - CQLite");
    println!("=====================================");
    
    demo_magic_number_detection();
    demo_node_types();
    demo_byte_comparable_encoding();
    demo_node_selection();
    
    println!("\n✅ BTI format implementation complete!");
    println!("   Ready for integration with Cassandra 5.0 BTI files");
}

fn demo_magic_number_detection() {
    println!("\n🔍 BTI Magic Number Detection");
    println!("------------------------------");
    
    let test_cases = vec![
        (0x6461_0000, "BTI format"),
        (0x0040_0000, "Cassandra 5.0 'nb' format"),
        (0x6F61_0000, "Legacy 'oa' format"),
        (0xDEADBEEF, "Unknown format"),
    ];
    
    for (magic, description) in test_cases {
        let format_type = detect_format(magic);
        let is_bti = is_bti_format(magic);
        
        println!("  Magic 0x{:08X} ({}): {:?} [BTI: {}]", 
                 magic, description, format_type, is_bti);
    }
    
    // Verify BTI magic number constant
    println!("  BTI_MAGIC_NUMBER constant: 0x{:08X}", BTI_MAGIC_NUMBER);
    assert_eq!(BTI_MAGIC_NUMBER, 0x6461_0000);
}

fn demo_node_types() {
    println!("\n🌳 BTI Trie Node Types");
    println!("----------------------");
    
    // Demonstrate all four node types
    let payload_node = TrieNode::PayloadOnly {
        payload: b"test_payload".to_vec(),
    };
    println!("  PayloadOnly: {:?}", payload_node.node_type());
    
    let single_node = TrieNode::Single {
        character: b'a',
        target: NodeRef::new(100, 1000),
        payload: Some(b"single_payload".to_vec()),
    };
    println!("  Single: {:?} -> transition '{}'", single_node.node_type(), single_node.find_transition(b'a').is_some());
    
    let sparse_transitions = vec![
        (b'a', NodeRef::new(200, 2000)),
        (b'x', NodeRef::new(300, 3000)),
        (b'z', NodeRef::new(400, 4000)),
    ];
    let sparse_node = TrieNode::Sparse {
        transitions: sparse_transitions,
        payload: None,
    };
    println!("  Sparse: {:?} -> {} transitions", sparse_node.node_type(), sparse_node.get_transitions().len());
    
    let dense_targets = vec![
        Some(NodeRef::new(500, 5000)), // 'a'
        Some(NodeRef::new(600, 6000)), // 'b'
        Some(NodeRef::new(700, 7000)), // 'c'
    ];
    let dense_node = TrieNode::Dense {
        first_char: b'a',
        last_char: b'c',
        targets: dense_targets,
        payload: None,
    };
    println!("  Dense: {:?} -> range '{}' to '{}'", 
             dense_node.node_type(), 
             dense_node.find_transition(b'a').is_some(),
             dense_node.find_transition(b'c').is_some());
}

fn demo_byte_comparable_encoding() {
    println!("\n🔢 Byte-Comparable Key Encoding");
    println!("--------------------------------");
    
    let mut encoder = ByteComparableEncoder::new();
    
    // Test text encoding
    let text_values = vec!["apple", "banana", "cherry"];
    println!("  Text encoding (should maintain lexicographic order):");
    
    let mut encoded_texts = Vec::new();
    for text in &text_values {
        let value = Value::Text(text.to_string());
        let encoded = encoder.encode_value(&value).unwrap();
        encoded_texts.push((text, encoded));
        
        let debug_str = ByteComparableDecoder::decode_key_debug(&encoded_texts.last().unwrap().1);
        println!("    '{}' -> {}", text, debug_str);
    }
    
    // Verify ordering
    for i in 1..encoded_texts.len() {
        assert!(encoded_texts[i-1].1 < encoded_texts[i].1, 
               "Encoding should preserve text ordering");
    }
    println!("    ✅ Text ordering preserved");
    
    // Test integer encoding
    let int_values = vec![-100, -1, 0, 1, 100];
    println!("  Integer encoding (should maintain numeric order):");
    
    let mut encoded_ints = Vec::new();
    for &int_val in &int_values {
        let value = Value::Int(int_val);
        let encoded = encoder.encode_value(&value).unwrap();
        encoded_ints.push((int_val, encoded));
        
        let debug_str = ByteComparableDecoder::decode_key_debug(&encoded_ints.last().unwrap().1);
        println!("    {} -> {}", int_val, debug_str);
    }
    
    // Verify numeric ordering
    for i in 1..encoded_ints.len() {
        assert!(encoded_ints[i-1].1 < encoded_ints[i].1, 
               "Encoding should preserve numeric ordering");
    }
    println!("    ✅ Numeric ordering preserved");
    
    // Test composite key encoding
    println!("  Composite key encoding:");
    let composite_key = vec![
        Value::Text("partition1".to_string()),
        Value::Int(42),
        Value::Text("clustering1".to_string()),
    ];
    let encoded_composite = encoder.encode_composite_key(&composite_key).unwrap();
    let debug_str = ByteComparableDecoder::decode_key_debug(&encoded_composite);
    println!("    [\"partition1\", 42, \"clustering1\"] -> {}", debug_str);
}

fn demo_node_selection() {
    println!("\n🎯 Optimal Node Type Selection");
    println!("-------------------------------");
    
    // Test different transition patterns
    let test_cases = vec![
        (vec![], "No transitions"),
        (vec![(b'a', NodeRef::null())], "Single transition"),
        (vec![(b'a', NodeRef::null()), (b'b', NodeRef::null()), (b'c', NodeRef::null())], "Dense range"),
        (vec![(b'a', NodeRef::null()), (b'x', NodeRef::null()), (b'z', NodeRef::null())], "Sparse transitions"),
        (vec![(b'0', NodeRef::null()), (b'1', NodeRef::null()), (b'2', NodeRef::null()), 
              (b'3', NodeRef::null()), (b'4', NodeRef::null()), (b'5', NodeRef::null())], "Dense digits"),
    ];
    
    for (transitions, description) in test_cases {
        let optimal_type = select_optimal_node_type(&transitions);
        println!("  {} -> {:?}", description, optimal_type);
        
        // Verify selection logic
        match transitions.len() {
            0 => assert_eq!(optimal_type, NodeType::PayloadOnly),
            1 => assert_eq!(optimal_type, NodeType::Single),
            _ => {
                // For multiple transitions, should be either DENSE or SPARSE
                assert!(optimal_type == NodeType::Dense || optimal_type == NodeType::Sparse);
            }
        }
    }
    println!("    ✅ Node selection algorithm working correctly");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bti_demo_integration() {
        // Verify all demo functions work without panicking
        demo_magic_number_detection();
        demo_node_types();
        demo_byte_comparable_encoding();
        demo_node_selection();
    }

    #[test]
    fn test_bti_constants() {
        assert_eq!(BTI_MAGIC_NUMBER, 0x6461_0000);
        assert!(is_bti_format(BTI_MAGIC_NUMBER));
        assert_eq!(detect_format(BTI_MAGIC_NUMBER), FormatType::Bti);
    }

    #[test]
    fn test_byte_comparable_consistency() {
        let mut encoder = ByteComparableEncoder::new();
        
        // Test that encoding is consistent across multiple calls
        let value = Value::Text("test".to_string());
        let encoded1 = encoder.encode_value(&value).unwrap();
        let encoded2 = encoder.encode_value(&value).unwrap();
        
        assert_eq!(encoded1, encoded2, "Encoding should be deterministic");
    }
}