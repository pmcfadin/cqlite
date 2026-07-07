//! Comprehensive tests for ComparatorType functionality
//!
//! This test suite validates the ComparatorType system including:
//! - Primitive type comparisons
//! - Collection type comparisons
//! - UDT type comparisons
//! - Frozen type comparisons
//! - Ordering and equality semantics
//! - Integration with SchemaRegistry and TableSchema

use cqlite_core::schema::{ClusteringColumn, Column, CqlType, KeyColumn, TableSchema};
use cqlite_core::types::{ComparatorType, UdtField, UdtValue, Value};
use std::{cmp::Ordering, collections::HashMap};

#[cfg(test)]
mod primitive_type_tests {
    use super::*;

    #[test]
    fn test_boolean_comparison() {
        let comparator = ComparatorType::Boolean;

        let true_val = Value::Boolean(true);
        let false_val = Value::Boolean(false);

        assert_eq!(
            comparator.compare(&false_val, &true_val).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            comparator.compare(&true_val, &false_val).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            comparator.compare(&true_val, &true_val).unwrap(),
            Ordering::Equal
        );

        assert!(comparator.equals(&true_val, &true_val).unwrap());
        assert!(!comparator.equals(&true_val, &false_val).unwrap());
        assert!(comparator.less_than(&false_val, &true_val).unwrap());
        assert!(comparator.greater_than(&true_val, &false_val).unwrap());
    }

    #[test]
    fn test_integer_comparison() {
        let comparator = ComparatorType::Int;

        let val_10 = Value::Integer(10);
        let val_20 = Value::Integer(20);
        let val_10_dup = Value::Integer(10);

        assert_eq!(
            comparator.compare(&val_10, &val_20).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            comparator.compare(&val_20, &val_10).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            comparator.compare(&val_10, &val_10_dup).unwrap(),
            Ordering::Equal
        );

        assert!(comparator.supports_ordering());
        assert!(comparator.less_than(&val_10, &val_20).unwrap());
        assert!(comparator.greater_than_or_equal(&val_20, &val_10).unwrap());
    }

    #[test]
    fn test_text_comparison() {
        let comparator = ComparatorType::Text;

        let apple = Value::Text("apple".to_string());
        let banana = Value::Text("banana".to_string());
        let apple_dup = Value::Text("apple".to_string());

        assert_eq!(comparator.compare(&apple, &banana).unwrap(), Ordering::Less);
        assert_eq!(
            comparator.compare(&banana, &apple).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            comparator.compare(&apple, &apple_dup).unwrap(),
            Ordering::Equal
        );

        assert!(comparator.supports_ordering());
        assert!(comparator.equals(&apple, &apple_dup).unwrap());
    }

    #[test]
    fn test_uuid_comparison() {
        let comparator = ComparatorType::Uuid;

        let uuid1 = Value::Uuid([1; 16]);
        let uuid2 = Value::Uuid([2; 16]);
        let uuid1_dup = Value::Uuid([1; 16]);

        assert_eq!(comparator.compare(&uuid1, &uuid2).unwrap(), Ordering::Less);
        assert_eq!(
            comparator.compare(&uuid2, &uuid1).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            comparator.compare(&uuid1, &uuid1_dup).unwrap(),
            Ordering::Equal
        );

        assert!(comparator.supports_ordering());
    }

    #[test]
    fn test_timestamp_comparison() {
        let comparator = ComparatorType::Timestamp;

        let ts1 = Value::Timestamp(1000);
        let ts2 = Value::Timestamp(2000);
        let ts1_dup = Value::Timestamp(1000);

        assert_eq!(comparator.compare(&ts1, &ts2).unwrap(), Ordering::Less);
        assert_eq!(comparator.compare(&ts2, &ts1).unwrap(), Ordering::Greater);
        assert_eq!(comparator.compare(&ts1, &ts1_dup).unwrap(), Ordering::Equal);

        assert!(comparator.supports_ordering());
    }

    #[test]
    fn test_varint_comparison() {
        let comparator = ComparatorType::Varint;

        let var1 = Value::Varint(vec![1, 2, 3]);
        let var2 = Value::Varint(vec![1, 2, 4]);
        let var1_dup = Value::Varint(vec![1, 2, 3]);

        assert_eq!(comparator.compare(&var1, &var2).unwrap(), Ordering::Less);
        assert_eq!(comparator.compare(&var2, &var1).unwrap(), Ordering::Greater);
        assert_eq!(
            comparator.compare(&var1, &var1_dup).unwrap(),
            Ordering::Equal
        );
    }

    #[test]
    fn test_decimal_comparison() {
        let comparator = ComparatorType::Decimal;

        let dec1 = Value::Decimal {
            scale: 2,
            unscaled: vec![1, 2],
        };
        let dec2 = Value::Decimal {
            scale: 2,
            unscaled: vec![1, 3],
        };
        let dec3 = Value::Decimal {
            scale: 3,
            unscaled: vec![1, 2],
        };

        assert_eq!(comparator.compare(&dec1, &dec2).unwrap(), Ordering::Less);
        assert_eq!(comparator.compare(&dec1, &dec3).unwrap(), Ordering::Less); // Different scale
        assert_eq!(comparator.compare(&dec1, &dec1).unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_duration_comparison() {
        let comparator = ComparatorType::Duration;

        let dur1 = Value::Duration {
            months: 1,
            days: 2,
            nanos: 3,
        };
        let dur2 = Value::Duration {
            months: 1,
            days: 2,
            nanos: 4,
        };
        let dur3 = Value::Duration {
            months: 1,
            days: 3,
            nanos: 2,
        };
        let dur4 = Value::Duration {
            months: 2,
            days: 1,
            nanos: 1,
        };

        assert_eq!(comparator.compare(&dur1, &dur2).unwrap(), Ordering::Less);
        assert_eq!(comparator.compare(&dur1, &dur3).unwrap(), Ordering::Less);
        assert_eq!(comparator.compare(&dur1, &dur4).unwrap(), Ordering::Less);
        assert_eq!(comparator.compare(&dur1, &dur1).unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_null_comparison() {
        let comparator = ComparatorType::Int;

        let null_val = Value::Null;
        let int_val = Value::Integer(42);

        // Nulls are always less than non-nulls
        assert_eq!(
            comparator.compare(&null_val, &null_val).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            comparator.compare(&null_val, &int_val).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            comparator.compare(&int_val, &null_val).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_type_mismatch_error() {
        let comparator = ComparatorType::Int;

        let int_val = Value::Integer(42);
        let text_val = Value::Text("hello".to_string());

        // Should return error for type mismatch
        assert!(comparator.compare(&int_val, &text_val).is_err());
    }
}

#[cfg(test)]
mod collection_type_tests {
    use super::*;

    #[test]
    fn test_list_comparison() {
        let element_comparator = ComparatorType::Int;
        let list_comparator = ComparatorType::List(Box::new(element_comparator));

        let list1 = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let list2 = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(4),
        ]);
        let list3 = Value::List(vec![Value::Integer(1), Value::Integer(2)]);

        // Compare by elements first, then by length
        assert_eq!(
            list_comparator.compare(&list1, &list2).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            list_comparator.compare(&list3, &list1).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            list_comparator.compare(&list1, &list1).unwrap(),
            Ordering::Equal
        );

        assert!(list_comparator.supports_ordering());
    }

    #[test]
    fn test_set_comparison() {
        let element_comparator = ComparatorType::Int;
        let set_comparator = ComparatorType::Set(Box::new(element_comparator));

        let set1 = Value::Set(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let set2 = Value::Set(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let set3 = Value::Set(vec![Value::Integer(1), Value::Integer(2)]);

        // Sets only support equality
        assert_eq!(
            set_comparator.compare(&set1, &set2).unwrap(),
            Ordering::Equal
        );
        assert_ne!(
            set_comparator.compare(&set1, &set3).unwrap(),
            Ordering::Equal
        );

        assert!(!set_comparator.supports_ordering()); // Sets don't support ordering
    }

    #[test]
    fn test_map_comparison() {
        let key_comparator = ComparatorType::Text;
        let value_comparator = ComparatorType::Int;
        let map_comparator =
            ComparatorType::Map(Box::new(key_comparator), Box::new(value_comparator));

        let map1 = Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(1)),
            (Value::Text("key2".to_string()), Value::Integer(2)),
        ]);

        let map2 = Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(1)),
            (Value::Text("key2".to_string()), Value::Integer(2)),
        ]);

        let map3 = Value::Map(vec![(Value::Text("key1".to_string()), Value::Integer(1))]);

        // Maps only support equality
        assert_eq!(
            map_comparator.compare(&map1, &map2).unwrap(),
            Ordering::Equal
        );
        assert_ne!(
            map_comparator.compare(&map1, &map3).unwrap(),
            Ordering::Equal
        );

        assert!(!map_comparator.supports_ordering()); // Maps don't support ordering
    }
}

#[cfg(test)]
mod complex_type_tests {
    use super::*;

    #[test]
    fn test_tuple_comparison() {
        let field_comparators = vec![ComparatorType::Int, ComparatorType::Text];
        let tuple_comparator = ComparatorType::Tuple(field_comparators);

        let tuple1 = Value::Tuple(vec![Value::Integer(1), Value::Text("hello".to_string())]);
        let tuple2 = Value::Tuple(vec![Value::Integer(1), Value::Text("world".to_string())]);
        let tuple3 = Value::Tuple(vec![Value::Integer(2), Value::Text("hello".to_string())]);

        // Compare field by field
        assert_eq!(
            tuple_comparator.compare(&tuple1, &tuple2).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            tuple_comparator.compare(&tuple1, &tuple3).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            tuple_comparator.compare(&tuple1, &tuple1).unwrap(),
            Ordering::Equal
        );

        assert!(tuple_comparator.supports_ordering());
    }

    #[test]
    fn test_udt_comparison() {
        let field_comparators = vec![
            ("name".to_string(), ComparatorType::Text),
            ("age".to_string(), ComparatorType::Int),
        ];
        let udt_comparator = ComparatorType::Udt {
            type_name: "Person".to_string(),
            keyspace: Some("test".to_string()),
            field_comparators,
        };

        let udt1 = Value::Udt(Box::new(UdtValue {
            type_name: "Person".to_string(),
            keyspace: "test".to_string(),
            fields: vec![
                UdtField {
                    name: "name".to_string(),
                    value: Some(Value::Text("Alice".to_string())),
                },
                UdtField {
                    name: "age".to_string(),
                    value: Some(Value::Integer(30)),
                },
            ],
        }));

        let udt2 = Value::Udt(Box::new(UdtValue {
            type_name: "Person".to_string(),
            keyspace: "test".to_string(),
            fields: vec![
                UdtField {
                    name: "name".to_string(),
                    value: Some(Value::Text("Bob".to_string())),
                },
                UdtField {
                    name: "age".to_string(),
                    value: Some(Value::Integer(25)),
                },
            ],
        }));

        // Compare field by field in definition order
        assert_eq!(
            udt_comparator.compare(&udt1, &udt2).unwrap(),
            Ordering::Less
        ); // Alice < Bob
        assert_eq!(
            udt_comparator.compare(&udt1, &udt1).unwrap(),
            Ordering::Equal
        );

        assert!(udt_comparator.supports_ordering());
    }

    #[test]
    fn test_frozen_comparison() {
        let inner_comparator = ComparatorType::List(Box::new(ComparatorType::Int));
        let frozen_comparator = ComparatorType::Frozen(Box::new(inner_comparator));

        let list1 = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
        let list2 = Value::List(vec![Value::Integer(1), Value::Integer(3)]);

        let frozen1 = Value::Frozen(Box::new(list1));
        let frozen2 = Value::Frozen(Box::new(list2));

        // Compare the inner values
        assert_eq!(
            frozen_comparator.compare(&frozen1, &frozen2).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            frozen_comparator.compare(&frozen1, &frozen1).unwrap(),
            Ordering::Equal
        );

        assert!(frozen_comparator.supports_ordering());
    }
}

#[cfg(test)]
mod comparator_type_creation_tests {
    use super::*;

    #[test]
    fn test_from_cql_type() {
        // Test primitive types
        assert_eq!(
            ComparatorType::from_cql_type(&CqlType::Int).unwrap(),
            ComparatorType::Int
        );
        assert_eq!(
            ComparatorType::from_cql_type(&CqlType::Text).unwrap(),
            ComparatorType::Text
        );

        // Test collection types
        let list_type = CqlType::List(Box::new(CqlType::Int));
        let list_comparator = ComparatorType::from_cql_type(&list_type).unwrap();
        assert!(matches!(list_comparator, ComparatorType::List(_)));

        let map_type = CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int));
        let map_comparator = ComparatorType::from_cql_type(&map_type).unwrap();
        assert!(matches!(map_comparator, ComparatorType::Map(_, _)));

        // Test UDT type
        let udt_type = CqlType::Udt(
            "Person".to_string(),
            vec![
                ("name".to_string(), CqlType::Text),
                ("age".to_string(), CqlType::Int),
            ],
        );
        let udt_comparator = ComparatorType::from_cql_type(&udt_type).unwrap();
        assert!(matches!(udt_comparator, ComparatorType::Udt { .. }));

        // Test frozen type
        let frozen_type = CqlType::Frozen(Box::new(CqlType::Text));
        let frozen_comparator = ComparatorType::from_cql_type(&frozen_type).unwrap();
        assert!(matches!(frozen_comparator, ComparatorType::Frozen(_)));
    }

    #[test]
    fn test_from_type_string() {
        // Test primitive types
        assert_eq!(
            ComparatorType::from_type_string("int").unwrap(),
            ComparatorType::Int
        );
        assert_eq!(
            ComparatorType::from_type_string("text").unwrap(),
            ComparatorType::Text
        );

        // Test collection types
        let list_comparator = ComparatorType::from_type_string("list<int>").unwrap();
        assert!(matches!(list_comparator, ComparatorType::List(_)));

        let map_comparator = ComparatorType::from_type_string("map<text, int>").unwrap();
        assert!(matches!(map_comparator, ComparatorType::Map(_, _)));
    }

    #[test]
    fn test_type_name() {
        assert_eq!(ComparatorType::Boolean.type_name(), "boolean");
        assert_eq!(ComparatorType::Int.type_name(), "int");
        assert_eq!(ComparatorType::Text.type_name(), "text");
        assert_eq!(
            ComparatorType::List(Box::new(ComparatorType::Int)).type_name(),
            "list"
        );
        assert_eq!(
            ComparatorType::Custom("custom_type".to_string()).type_name(),
            "custom_type"
        );
    }

    #[test]
    fn test_supports_ordering() {
        // Primitive types support ordering
        assert!(ComparatorType::Int.supports_ordering());
        assert!(ComparatorType::Text.supports_ordering());
        assert!(ComparatorType::Timestamp.supports_ordering());

        // Collections have different ordering support
        assert!(ComparatorType::List(Box::new(ComparatorType::Int)).supports_ordering());
        assert!(!ComparatorType::Set(Box::new(ComparatorType::Int)).supports_ordering());
        assert!(!ComparatorType::Map(
            Box::new(ComparatorType::Text),
            Box::new(ComparatorType::Int)
        )
        .supports_ordering());

        // Complex types
        assert!(
            ComparatorType::Tuple(vec![ComparatorType::Int, ComparatorType::Text])
                .supports_ordering()
        );
        assert!(ComparatorType::Frozen(Box::new(ComparatorType::Text)).supports_ordering());
        assert!(!ComparatorType::Custom("custom".to_string()).supports_ordering());
    }

    #[test]
    fn test_display_formatting() {
        assert_eq!(format!("{}", ComparatorType::Int), "int");
        assert_eq!(
            format!("{}", ComparatorType::List(Box::new(ComparatorType::Text))),
            "list<text>"
        );
        assert_eq!(
            format!(
                "{}",
                ComparatorType::Map(
                    Box::new(ComparatorType::Text),
                    Box::new(ComparatorType::Int)
                )
            ),
            "map<text, int>"
        );
        assert_eq!(
            format!(
                "{}",
                ComparatorType::Tuple(vec![ComparatorType::Int, ComparatorType::Text])
            ),
            "tuple<int, text>"
        );
        assert_eq!(
            format!("{}", ComparatorType::Frozen(Box::new(ComparatorType::Text))),
            "frozen<text>"
        );

        let udt_comparator = ComparatorType::Udt {
            type_name: "Person".to_string(),
            keyspace: Some("test".to_string()),
            field_comparators: vec![],
        };
        assert_eq!(format!("{}", udt_comparator), "test.Person");
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_table_schema_integration() {
        let schema = TableSchema {
            keyspace: "test".to_string(),
            table: "users".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "created_at".to_string(),
                data_type: "timestamp".to_string(),
                position: 0,
                order: cqlite_core::schema::ClusteringOrder::Desc,
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "created_at".to_string(),
                    data_type: "timestamp".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "tags".to_string(),
                    data_type: "list<text>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        // Test getting column comparator
        let name_comparator = schema.get_column_comparator("name").unwrap();
        assert_eq!(name_comparator, ComparatorType::Text);

        let tags_comparator = schema.get_column_comparator("tags").unwrap();
        assert!(matches!(tags_comparator, ComparatorType::List(_)));

        // Test getting all comparators
        let all_comparators = schema.get_all_comparators().unwrap();
        assert_eq!(all_comparators.len(), 4);
        assert_eq!(all_comparators["name"], ComparatorType::Text);
        assert_eq!(all_comparators["id"], ComparatorType::Uuid);

        // Test partition key comparators
        let pk_comparators = schema.get_partition_key_comparators().unwrap();
        assert_eq!(pk_comparators.len(), 1);
        assert_eq!(pk_comparators[0], ComparatorType::Uuid);

        // Test clustering key comparators
        let ck_comparators = schema.get_clustering_key_comparators().unwrap();
        assert_eq!(ck_comparators.len(), 1);
        assert_eq!(ck_comparators[0], ComparatorType::Timestamp);

        // Test type compatibility
        assert!(schema.is_column_type_compatible("name", "text").unwrap());
        assert!(schema.is_column_type_compatible("name", "varchar").unwrap()); // Compatible types
        assert!(!schema.is_column_type_compatible("name", "int").unwrap());
    }

    #[test]
    fn test_complex_nested_types() {
        // Test deeply nested collection types
        let nested_list_type = CqlType::List(Box::new(CqlType::List(Box::new(CqlType::Int))));
        let nested_comparator = ComparatorType::from_cql_type(&nested_list_type).unwrap();

        assert!(matches!(nested_comparator, ComparatorType::List(_)));
        if let ComparatorType::List(inner) = nested_comparator {
            assert!(matches!(*inner, ComparatorType::List(_)));
        }

        // Test map with complex value type
        let complex_map_type = CqlType::Map(
            Box::new(CqlType::Text),
            Box::new(CqlType::Udt(
                "Person".to_string(),
                vec![
                    ("name".to_string(), CqlType::Text),
                    (
                        "addresses".to_string(),
                        CqlType::List(Box::new(CqlType::Text)),
                    ),
                ],
            )),
        );
        let complex_map_comparator = ComparatorType::from_cql_type(&complex_map_type).unwrap();

        assert!(matches!(complex_map_comparator, ComparatorType::Map(_, _)));
    }

    #[test]
    fn test_error_handling() {
        let schema = TableSchema {
            keyspace: "test".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        // Test non-existent column
        assert!(schema.get_column_comparator("non_existent").is_err());

        // Test type mismatch in comparison
        let int_comparator = ComparatorType::Int;
        let int_val = Value::Integer(42);
        let text_val = Value::Text("hello".to_string());

        assert!(int_comparator.compare(&int_val, &text_val).is_err());
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;

    #[test]
    fn test_large_collection_comparison() {
        let element_comparator = ComparatorType::Int;
        let list_comparator = ComparatorType::List(Box::new(element_comparator));

        // Create large lists for performance testing
        let large_values: Vec<Value> = (0..1000).map(|i| Value::Integer(i)).collect();

        let list1 = Value::List(large_values.clone());
        let list2 = Value::List(large_values);

        // This should complete in reasonable time
        let start = std::time::Instant::now();
        let result = list_comparator.compare(&list1, &list2).unwrap();
        let duration = start.elapsed();

        assert_eq!(result, Ordering::Equal);
        assert!(duration.as_millis() < 100); // Should be fast
    }

    #[test]
    fn test_deep_nesting_performance() {
        // Create deeply nested structure
        let mut current_type = CqlType::Int;
        for _ in 0..10 {
            current_type = CqlType::List(Box::new(current_type));
        }

        let start = std::time::Instant::now();
        let comparator = ComparatorType::from_cql_type(&current_type).unwrap();
        let duration = start.elapsed();

        assert!(duration.as_millis() < 10); // Should create quickly
        assert!(matches!(comparator, ComparatorType::List(_)));
    }
}
