//! BTI Test Data Structures and Generators
//!
//! This module provides comprehensive test data structures for validating
//! BTI (Big Trie Index) format functionality with complex scenarios.

use cqlite_core::types::Value;
use serde::{Deserialize, Serialize};

/// Comprehensive BTI test dataset for Issue #36 validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BtiTestDataset {
    /// Dataset name for identification
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// Partition key components (multi-component keys supported)
    pub partition_keys: Vec<Vec<BtiTestValue>>,
    /// Clustering key components for each partition
    pub clustering_keys: Vec<Vec<BtiTestValue>>,
    /// Whether this dataset contains wide partitions
    pub has_wide_partitions: bool,
    /// Whether this dataset contains range tombstones
    pub has_range_tombstones: bool,
    /// Expected trie depth for this dataset
    pub expected_trie_depth: usize,
}

/// Test value types with enhanced type support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BtiTestValue {
    /// Null value
    Null,
    /// Boolean value
    Boolean(bool),
    /// 8-bit signed integer
    TinyInt(i8),
    /// 16-bit signed integer
    SmallInt(i16),
    /// 32-bit signed integer
    Integer(i32),
    /// 64-bit signed integer
    BigInt(i64),
    /// 32-bit floating point
    Float32(f32),
    /// 64-bit floating point
    Float64(f64),
    /// UTF-8 text string
    Text(String),
    /// Binary blob data
    Blob(Vec<u8>),
    /// UUID as string
    UUID(String),
    /// Timestamp as microseconds since epoch
    Timestamp(i64),
    /// JSON as string
    Json(String),
    /// List of values
    List(Vec<BtiTestValue>),
    /// Set of values (unordered, unique)
    Set(Vec<BtiTestValue>),
    /// Map as key-value pairs
    Map(Vec<(BtiTestValue, BtiTestValue)>),
    /// Tuple with positional elements
    Tuple(Vec<BtiTestValue>),
    /// User Defined Type with keyspace, type name, and fields
    UDT(String, Vec<(String, BtiTestValue)>),
    /// Frozen complex type
    Frozen(Box<BtiTestValue>),
    /// Nested collection type (for testing complex scenarios)
    NestedCollection(NestedCollectionType),
    /// Tombstone marker with timestamp
    Tombstone(i64),
}

/// Complex nested collection types for comprehensive testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NestedCollectionType {
    /// List of lists
    ListOfLists,
    /// Set of sets
    SetOfSets,
    /// Map with list values
    MapOfLists,
    /// Map with set values
    MapOfSets,
    /// List of maps
    ListOfMaps,
    /// Map of maps
    MapOfMaps,
    /// List of UDTs
    ListOfUDTs,
    /// Set of UDTs
    SetOfUDTs,
    /// Map with UDT values
    MapOfUDTs,
}

impl BtiTestValue {
    /// Convert to CQLite Value type
    pub fn to_cqlite_value(&self) -> Value {
        match self {
            BtiTestValue::Null => Value::Null,
            BtiTestValue::Boolean(b) => Value::Boolean(*b),
            BtiTestValue::TinyInt(i) => Value::TinyInt(*i),
            BtiTestValue::SmallInt(i) => Value::SmallInt(*i),
            BtiTestValue::Integer(i) => Value::Integer(*i),
            BtiTestValue::BigInt(i) => Value::BigInt(*i),
            BtiTestValue::Float32(f) => Value::Float32(*f),
            BtiTestValue::Float64(f) => Value::Float(*f),
            BtiTestValue::Text(s) => Value::Text(s.clone()),
            BtiTestValue::Blob(b) => Value::Blob(b.clone()),
            BtiTestValue::UUID(s) => {
                if let Ok(uuid) = uuid::Uuid::parse_str(s) {
                    Value::Uuid(*uuid.as_bytes())
                } else {
                    Value::Text(s.clone()) // Fallback for invalid UUIDs
                }
            }
            BtiTestValue::Timestamp(ts) => Value::Timestamp(*ts),
            BtiTestValue::Json(j) => {
                if let Ok(json_val) = serde_json::from_str(j) {
                    Value::Json(json_val)
                } else {
                    Value::Text(j.clone()) // Fallback for invalid JSON
                }
            }
            BtiTestValue::List(items) => {
                Value::List(items.iter().map(|item| item.to_cqlite_value()).collect())
            }
            BtiTestValue::Set(items) => {
                Value::Set(items.iter().map(|item| item.to_cqlite_value()).collect())
            }
            BtiTestValue::Map(pairs) => Value::Map(
                pairs
                    .iter()
                    .map(|(k, v)| (k.to_cqlite_value(), v.to_cqlite_value()))
                    .collect(),
            ),
            BtiTestValue::Tuple(items) => {
                Value::Tuple(items.iter().map(|item| item.to_cqlite_value()).collect())
            }
            BtiTestValue::UDT(type_name, fields) => {
                use cqlite_core::types::{UdtField, UdtValue};

                let udt_fields = fields
                    .iter()
                    .map(|(name, value)| UdtField {
                        name: name.clone(),
                        value: Some(value.to_cqlite_value()),
                    })
                    .collect();

                Value::Udt(UdtValue {
                    keyspace: "test".to_string(),
                    type_name: type_name.clone(),
                    fields: udt_fields,
                })
            }
            BtiTestValue::Frozen(inner) => Value::Frozen(Box::new(inner.to_cqlite_value())),
            BtiTestValue::NestedCollection(nested_type) => {
                // Generate appropriate nested collection based on type
                match nested_type {
                    NestedCollectionType::ListOfLists => Value::List(vec![
                        Value::List(vec![Value::Integer(1), Value::Integer(2)]),
                        Value::List(vec![Value::Integer(3), Value::Integer(4)]),
                    ]),
                    NestedCollectionType::SetOfSets => Value::Set(vec![
                        Value::Set(vec![
                            Value::Text("a".to_string()),
                            Value::Text("b".to_string()),
                        ]),
                        Value::Set(vec![
                            Value::Text("c".to_string()),
                            Value::Text("d".to_string()),
                        ]),
                    ]),
                    NestedCollectionType::MapOfLists => Value::Map(vec![
                        (
                            Value::Text("key1".to_string()),
                            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
                        ),
                        (
                            Value::Text("key2".to_string()),
                            Value::List(vec![Value::Integer(3), Value::Integer(4)]),
                        ),
                    ]),
                    // ... implement other nested types
                    _ => Value::List(vec![Value::Integer(1)]), // Simplified fallback
                }
            }
            BtiTestValue::Tombstone(ts) => Value::cell_tombstone(*ts),
        }
    }

    /// Get estimated serialized size for capacity planning
    pub fn estimated_size(&self) -> usize {
        match self {
            BtiTestValue::Null => 1,
            BtiTestValue::Boolean(_) => 1,
            BtiTestValue::TinyInt(_) => 2,
            BtiTestValue::SmallInt(_) => 3,
            BtiTestValue::Integer(_) => 5,
            BtiTestValue::BigInt(_) | BtiTestValue::Timestamp(_) => 9,
            BtiTestValue::Float32(_) => 5,
            BtiTestValue::Float64(_) => 9,
            BtiTestValue::Text(s) => 1 + s.len() + 1, // prefix + content + terminator
            BtiTestValue::Blob(b) => 1 + b.len() + 1,
            BtiTestValue::UUID(_) => 17,
            BtiTestValue::Json(j) => 1 + j.len() + 1,
            BtiTestValue::List(items) => {
                1 + 4
                    + items
                        .iter()
                        .map(|item| item.estimated_size())
                        .sum::<usize>()
                    + 1
            }
            BtiTestValue::Set(items) => {
                1 + 4
                    + items
                        .iter()
                        .map(|item| item.estimated_size())
                        .sum::<usize>()
                    + 1
            }
            BtiTestValue::Map(pairs) => {
                1 + 4
                    + pairs
                        .iter()
                        .map(|(k, v)| k.estimated_size() + v.estimated_size())
                        .sum::<usize>()
                    + 1
            }
            BtiTestValue::Tuple(items) => {
                1 + 4
                    + items
                        .iter()
                        .map(|item| item.estimated_size())
                        .sum::<usize>()
                    + 1
            }
            BtiTestValue::UDT(type_name, fields) => {
                1 + type_name.len()
                    + 1
                    + 4
                    + fields
                        .iter()
                        .map(|(name, value)| name.len() + 1 + value.estimated_size())
                        .sum::<usize>()
                    + 1
            }
            BtiTestValue::Frozen(inner) => 1 + inner.estimated_size(),
            BtiTestValue::NestedCollection(_) => 100, // Estimate for complex nested structures
            BtiTestValue::Tombstone(_) => 10,
        }
    }
}

/// BTI test data generator for creating comprehensive test scenarios
pub struct BtiTestDataGenerator {
    /// Random seed for reproducible tests
    pub seed: u64,
    /// Configuration for data generation
    pub config: BtiTestGeneratorConfig,
}

/// Configuration for BTI test data generation
#[derive(Debug, Clone)]
pub struct BtiTestGeneratorConfig {
    /// Maximum nesting depth for collections
    pub max_nesting_depth: usize,
    /// Maximum number of elements in collections
    pub max_collection_size: usize,
    /// Include wide partition scenarios
    pub include_wide_partitions: bool,
    /// Include range tombstone scenarios
    pub include_range_tombstones: bool,
    /// Include complex type scenarios
    pub include_complex_types: bool,
    /// Maximum key component count
    pub max_key_components: usize,
}

impl Default for BtiTestGeneratorConfig {
    fn default() -> Self {
        Self {
            max_nesting_depth: 5,
            max_collection_size: 100,
            include_wide_partitions: true,
            include_range_tombstones: true,
            include_complex_types: true,
            max_key_components: 5,
        }
    }
}

impl BtiTestDataGenerator {
    /// Create new generator with seed
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            config: BtiTestGeneratorConfig::default(),
        }
    }

    /// Create generator with custom configuration
    pub fn with_config(seed: u64, config: BtiTestGeneratorConfig) -> Self {
        Self { seed, config }
    }

    /// Generate comprehensive test datasets for Issue #36 requirements
    pub fn generate_comprehensive_datasets(&self) -> Vec<BtiTestDataset> {
        let mut datasets = Vec::new();

        // Dataset 1: Multi-component partition keys
        datasets.push(self.generate_multi_component_partition_dataset());

        // Dataset 2: Nested collections and UDTs
        datasets.push(self.generate_nested_collections_dataset());

        // Dataset 3: Wide partitions
        if self.config.include_wide_partitions {
            datasets.push(self.generate_wide_partitions_dataset());
        }

        // Dataset 4: Range tombstones
        if self.config.include_range_tombstones {
            datasets.push(self.generate_range_tombstones_dataset());
        }

        // Dataset 5: Complex type combinations
        if self.config.include_complex_types {
            datasets.push(self.generate_complex_types_dataset());
        }

        // Dataset 6: Edge cases and boundary conditions
        datasets.push(self.generate_edge_cases_dataset());

        datasets
    }

    /// Generate multi-component partition key dataset
    fn generate_multi_component_partition_dataset(&self) -> BtiTestDataset {
        BtiTestDataset {
            name: "multi_component_partition_keys".to_string(),
            description: "Multi-component partition keys with various data types".to_string(),
            partition_keys: vec![
                vec![
                    BtiTestValue::Text("user_123".to_string()),
                    BtiTestValue::Integer(2023),
                    BtiTestValue::UUID("550e8400-e29b-41d4-a716-446655440000".to_string()),
                ],
                vec![
                    BtiTestValue::Text("tenant_456".to_string()),
                    BtiTestValue::BigInt(1640995200000000i64),
                    BtiTestValue::Boolean(true),
                ],
            ],
            clustering_keys: vec![vec![
                BtiTestValue::Timestamp(1640995200000000i64),
                BtiTestValue::Text("event_type_A".to_string()),
            ]],
            has_wide_partitions: true,
            has_range_tombstones: true,
            expected_trie_depth: 3,
        }
    }

    /// Generate nested collections dataset
    fn generate_nested_collections_dataset(&self) -> BtiTestDataset {
        BtiTestDataset {
            name: "nested_collections_udts".to_string(),
            description: "Complex nested collections and user-defined types".to_string(),
            partition_keys: vec![vec![
                BtiTestValue::Text("complex_key".to_string()),
                BtiTestValue::NestedCollection(NestedCollectionType::MapOfLists),
            ]],
            clustering_keys: vec![vec![BtiTestValue::UDT(
                "address".to_string(),
                vec![
                    (
                        "street".to_string(),
                        BtiTestValue::Text("123 Main St".to_string()),
                    ),
                    ("city".to_string(), BtiTestValue::Text("Boston".to_string())),
                    ("zipcode".to_string(), BtiTestValue::Integer(02101)),
                ],
            )]],
            has_wide_partitions: false,
            has_range_tombstones: false,
            expected_trie_depth: 4,
        }
    }

    /// Generate wide partitions dataset
    fn generate_wide_partitions_dataset(&self) -> BtiTestDataset {
        let clustering_keys = (0..10000)
            .map(|i| {
                vec![
                    BtiTestValue::Integer(i),
                    BtiTestValue::Timestamp(1640995200000000i64 + i as i64),
                ]
            })
            .collect();

        BtiTestDataset {
            name: "wide_partitions".to_string(),
            description: "Wide partitions with thousands of clustering keys".to_string(),
            partition_keys: vec![vec![BtiTestValue::Text("wide_partition".to_string())]],
            clustering_keys,
            has_wide_partitions: true,
            has_range_tombstones: true,
            expected_trie_depth: 2,
        }
    }

    /// Generate range tombstones dataset
    fn generate_range_tombstones_dataset(&self) -> BtiTestDataset {
        BtiTestDataset {
            name: "range_tombstones".to_string(),
            description: "Dataset with range tombstones for deletion scenarios".to_string(),
            partition_keys: vec![vec![BtiTestValue::Text("tombstone_test".to_string())]],
            clustering_keys: vec![
                vec![
                    BtiTestValue::Text("range_start".to_string()),
                    BtiTestValue::Tombstone(1640995200000000i64),
                ],
                vec![
                    BtiTestValue::Text("range_end".to_string()),
                    BtiTestValue::Tombstone(1640995300000000i64),
                ],
            ],
            has_wide_partitions: false,
            has_range_tombstones: true,
            expected_trie_depth: 2,
        }
    }

    /// Generate complex types dataset
    fn generate_complex_types_dataset(&self) -> BtiTestDataset {
        BtiTestDataset {
            name: "complex_types".to_string(),
            description: "Complex data types including frozen collections and nested UDTs"
                .to_string(),
            partition_keys: vec![vec![BtiTestValue::Frozen(Box::new(BtiTestValue::Map(
                vec![
                    (
                        BtiTestValue::Text("key1".to_string()),
                        BtiTestValue::Integer(1),
                    ),
                    (
                        BtiTestValue::Text("key2".to_string()),
                        BtiTestValue::Integer(2),
                    ),
                ],
            )))]],
            clustering_keys: vec![vec![BtiTestValue::Tuple(vec![
                BtiTestValue::Text("tuple_element_1".to_string()),
                BtiTestValue::Integer(42),
                BtiTestValue::Boolean(true),
            ])]],
            has_wide_partitions: false,
            has_range_tombstones: false,
            expected_trie_depth: 3,
        }
    }

    /// Generate edge cases dataset
    fn generate_edge_cases_dataset(&self) -> BtiTestDataset {
        BtiTestDataset {
            name: "edge_cases".to_string(),
            description: "Edge cases and boundary conditions for robust testing".to_string(),
            partition_keys: vec![
                vec![
                    BtiTestValue::Text("".to_string()), // Empty string
                    BtiTestValue::Blob(vec![]),         // Empty blob
                    BtiTestValue::List(vec![]),         // Empty list
                ],
                vec![
                    BtiTestValue::Integer(i32::MIN), // Minimum integer
                    BtiTestValue::Integer(i32::MAX), // Maximum integer
                    BtiTestValue::BigInt(i64::MIN),  // Minimum bigint
                    BtiTestValue::BigInt(i64::MAX),  // Maximum bigint
                ],
                vec![
                    BtiTestValue::Float32(f32::NEG_INFINITY), // Negative infinity
                    BtiTestValue::Float32(f32::INFINITY),     // Positive infinity
                    BtiTestValue::Float64(f64::NAN),          // NaN
                ],
            ],
            clustering_keys: vec![vec![
                BtiTestValue::Null, // Null clustering key
                BtiTestValue::Text("null_test".to_string()),
            ]],
            has_wide_partitions: false,
            has_range_tombstones: false,
            expected_trie_depth: 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bti_test_value_conversion() {
        let test_value = BtiTestValue::Text("hello".to_string());
        let cqlite_value = test_value.to_cqlite_value();

        match cqlite_value {
            Value::Text(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected text value"),
        }
    }

    #[test]
    fn test_bti_test_data_generator() {
        let generator = BtiTestDataGenerator::new(12345);
        let datasets = generator.generate_comprehensive_datasets();

        assert!(!datasets.is_empty());
        assert!(datasets
            .iter()
            .any(|d| d.name == "multi_component_partition_keys"));
        assert!(datasets.iter().any(|d| d.name == "nested_collections_udts"));
        assert!(datasets.iter().any(|d| d.name == "wide_partitions"));
    }

    #[test]
    fn test_estimated_size_calculation() {
        let test_value = BtiTestValue::Text("hello".to_string());
        let size = test_value.estimated_size();
        assert_eq!(size, 7); // 1 (prefix) + 5 (content) + 1 (terminator)

        let list_value =
            BtiTestValue::List(vec![BtiTestValue::Integer(1), BtiTestValue::Integer(2)]);
        let list_size = list_value.estimated_size();
        assert_eq!(list_size, 16); // 1 + 4 + 5 + 5 + 1
    }

    #[test]
    fn test_nested_collection_generation() {
        let nested = BtiTestValue::NestedCollection(NestedCollectionType::ListOfLists);
        let cqlite_value = nested.to_cqlite_value();

        match cqlite_value {
            Value::List(items) => {
                assert_eq!(items.len(), 2);
                match &items[0] {
                    Value::List(inner) => assert_eq!(inner.len(), 2),
                    _ => panic!("Expected nested list"),
                }
            }
            _ => panic!("Expected list value"),
        }
    }
}
