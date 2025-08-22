//! Comprehensive unit tests for schema-driven parsing

#[cfg(test)]
mod tests {
    use crate::{
        schema::{
            ClusteringColumn, Column, KeyColumn, TableSchema, parser::SchemaParser,
            registry::ParsingContext,
        },
        types::{ComparatorType, Value},
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    fn create_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "timestamp".to_string(),
                data_type: "bigint".to_string(),
                position: 0,
                order: "ASC".to_string(),
            }],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "timestamp".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "data".to_string(),
                    data_type: "blob".to_string(),
                    nullable: true,
                    default: None,
                },
                Column {
                    name: "tags".to_string(),
                    data_type: "list<text>".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        }
    }

    fn create_test_context() -> ParsingContext {
        let schema = create_test_schema();
        let mut column_comparators = HashMap::new();

        column_comparators.insert("id".to_string(), ComparatorType::Int);
        column_comparators.insert("timestamp".to_string(), ComparatorType::BigInt);
        column_comparators.insert("name".to_string(), ComparatorType::Text);
        column_comparators.insert("data".to_string(), ComparatorType::Blob);
        column_comparators.insert(
            "tags".to_string(),
            ComparatorType::List(Box::new(ComparatorType::Text)),
        );

        ParsingContext {
            schema,
            partition_comparators: vec![ComparatorType::Int],
            clustering_comparators: vec![ComparatorType::BigInt],
            column_comparators,
        }
    }

    #[test]
    fn test_schema_parser_creation() {
        let context = create_test_context();
        let parser = SchemaParser::new(context);
        assert!(parser.is_ok());
    }

    #[test]
    fn test_parse_partition_key() {
        let context = create_test_context();
        let parser = SchemaParser::new(context).unwrap();

        // Create a 4-byte integer (42)
        let data = vec![0, 0, 0, 42];
        let result = parser.parse_partition_key(&data);

        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], Value::Integer(42));
    }

    #[test]
    fn test_parse_clustering_keys() {
        let context = create_test_context();
        let parser = SchemaParser::new(context).unwrap();

        // Create an 8-byte bigint (1234567890)
        let data = vec![0, 0, 0, 0, 73, 150, 2, 210];
        let result = parser.parse_clustering_keys(&data);

        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], Value::BigInt(1234567890));
    }

    #[test]
    fn test_parse_text_column() {
        let context = create_test_context();
        let parser = SchemaParser::new(context).unwrap();

        // Create length-prefixed text "hello" (4 bytes length + 5 bytes content)
        let data = vec![0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o'];
        let result = parser.parse_column_value("name", &data);

        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_parse_blob_column() {
        let context = create_test_context();
        let parser = SchemaParser::new(context).unwrap();

        // Create length-prefixed blob (4 bytes length + 3 bytes content)
        let data = vec![0, 0, 0, 3, 1, 2, 3];
        let result = parser.parse_column_value("data", &data);

        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value, Value::Blob(vec![1, 2, 3]));
    }

    #[test]
    fn test_parse_list_column() {
        let context = create_test_context();
        let parser = SchemaParser::new(context).unwrap();

        // Create a list with 2 text elements
        // Format: [count=2][len=5]["hello"][len=5]["world"]
        let mut data = vec![0, 0, 0, 2]; // count = 2
        data.extend_from_slice(&[0, 0, 0, 5]); // length of "hello"
        data.extend_from_slice(b"hello");
        data.extend_from_slice(&[0, 0, 0, 5]); // length of "world"
        data.extend_from_slice(b"world");

        let result = parser.parse_column_value("tags", &data);

        assert!(result.is_ok());
        let value = result.unwrap();
        match value {
            Value::List(elements) => {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], Value::Text("hello".to_string()));
                assert_eq!(elements[1], Value::Text("world".to_string()));
            }
            _ => panic!("Expected List value"),
        }
    }

    #[test]
    fn test_multi_component_partition_key() {
        // Create schema with composite partition key
        let mut schema = create_test_schema();
        schema.partition_keys.push(KeyColumn {
            name: "region".to_string(),
            data_type: "text".to_string(),
            position: 1,
        });

        let mut column_comparators = HashMap::new();
        column_comparators.insert("id".to_string(), ComparatorType::Int);
        column_comparators.insert("region".to_string(), ComparatorType::Text);

        let context = ParsingContext {
            schema,
            partition_comparators: vec![ComparatorType::Int, ComparatorType::Text],
            clustering_comparators: vec![],
            column_comparators,
        };

        let parser = SchemaParser::new(context).unwrap();

        // Create data: int(42) + text("US")
        let mut data = vec![0, 0, 0, 42]; // int 42
        data.extend_from_slice(&[0, 0, 0, 2]); // length of "US"
        data.extend_from_slice(b"US");

        let result = parser.parse_partition_key(&data);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Integer(42));
        assert_eq!(values[1], Value::Text("US".to_string()));
    }

    #[test]
    fn test_parse_uuid_type() {
        let mut schema = create_test_schema();
        schema.columns.push(Column {
            name: "user_id".to_string(),
            data_type: "uuid".to_string(),
            nullable: false,
            default: None,
        });

        let mut column_comparators = HashMap::new();
        column_comparators.insert("user_id".to_string(), ComparatorType::Uuid);

        let context = ParsingContext {
            schema,
            partition_comparators: vec![],
            clustering_comparators: vec![],
            column_comparators,
        };

        let parser = SchemaParser::new(context).unwrap();

        // Create a valid UUID bytes (16 bytes)
        let uuid = Uuid::new_v4();
        let data = uuid.as_bytes().to_vec();

        let result = parser.parse_column_value("user_id", &data);
        assert!(result.is_ok());

        let value = result.unwrap();
        assert!(matches!(value, Value::Uuid(_)));
    }

    #[test]
    fn test_parse_nested_collection() {
        let mut schema = create_test_schema();
        schema.columns.push(Column {
            name: "nested".to_string(),
            data_type: "map<text,list<int>>".to_string(),
            nullable: true,
            default: None,
        });

        let map_comparator = ComparatorType::Map(
            Box::new(ComparatorType::Text),
            Box::new(ComparatorType::List(Box::new(ComparatorType::Int))),
        );

        let mut column_comparators = HashMap::new();
        column_comparators.insert("nested".to_string(), map_comparator);

        let context = ParsingContext {
            schema,
            partition_comparators: vec![],
            clustering_comparators: vec![],
            column_comparators,
        };

        let parser = SchemaParser::new(context).unwrap();

        // Create a map with 1 entry: {"key": [1, 2]}
        let mut data = vec![0, 0, 0, 1]; // map count = 1

        // Key: "key"
        data.extend_from_slice(&[0, 0, 0, 3]); // length of "key"
        data.extend_from_slice(b"key");

        // Value: list with 2 integers
        data.extend_from_slice(&[0, 0, 0, 2]); // list count = 2
        data.extend_from_slice(&[0, 0, 0, 1]); // int 1
        data.extend_from_slice(&[0, 0, 0, 2]); // int 2

        let result = parser.parse_column_value("nested", &data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_frozen_type() {
        let mut schema = create_test_schema();
        schema.columns.push(Column {
            name: "frozen_set".to_string(),
            data_type: "frozen<set<int>>".to_string(),
            nullable: true,
            default: None,
        });

        let frozen_comparator =
            ComparatorType::Frozen(Box::new(ComparatorType::Set(Box::new(ComparatorType::Int))));

        let mut column_comparators = HashMap::new();
        column_comparators.insert("frozen_set".to_string(), frozen_comparator);

        let context = ParsingContext {
            schema,
            partition_comparators: vec![],
            clustering_comparators: vec![],
            column_comparators,
        };

        let parser = SchemaParser::new(context).unwrap();

        // Create a frozen set with 3 integers
        let mut data = vec![0, 0, 0, 3]; // set count = 3
        data.extend_from_slice(&[0, 0, 0, 10]); // int 10
        data.extend_from_slice(&[0, 0, 0, 20]); // int 20
        data.extend_from_slice(&[0, 0, 0, 30]); // int 30

        let result = parser.parse_column_value("frozen_set", &data);
        assert!(result.is_ok());

        let value = result.unwrap();
        assert!(matches!(value, Value::Frozen(_)));
    }

    #[test]
    fn test_error_on_missing_column() {
        let context = create_test_context();
        let parser = SchemaParser::new(context).unwrap();

        let data = vec![0, 0, 0, 42];
        let result = parser.parse_column_value("nonexistent", &data);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not found in schema"));
    }

    #[test]
    fn test_error_on_insufficient_data() {
        let context = create_test_context();
        let parser = SchemaParser::new(context).unwrap();

        // Only 2 bytes when int needs 4
        let data = vec![0, 0];
        let result = parser.parse_column_value("id", &data);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Insufficient data"));
    }

    #[test]
    fn test_incomplete_context_rejected() {
        let context = ParsingContext {
            schema: TableSchema {
                keyspace: "test".to_string(),
                table: "test".to_string(),
                partition_keys: vec![],
                clustering_keys: vec![],
                columns: vec![],
                comments: HashMap::new(),
            },
            partition_comparators: vec![],
            clustering_comparators: vec![],
            column_comparators: HashMap::new(),
        };

        let result = SchemaParser::new(context);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Incomplete parsing context")
        );
    }

    #[test]
    fn test_multi_component_partition_and_clustering_keys() {
        // Create schema with 2 partition keys and 2 clustering keys
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "multi_key_table".to_string(),
            partition_keys: vec![
                KeyColumn {
                    name: "region".to_string(),
                    data_type: "text".to_string(),
                    position: 0,
                },
                KeyColumn {
                    name: "bucket".to_string(),
                    data_type: "int".to_string(),
                    position: 1,
                },
            ],
            clustering_keys: vec![
                ClusteringColumn {
                    name: "timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    position: 0,
                    order: "ASC".to_string(),
                },
                ClusteringColumn {
                    name: "sequence".to_string(),
                    data_type: "bigint".to_string(),
                    position: 1,
                    order: "ASC".to_string(),
                },
            ],
            columns: vec![
                Column {
                    name: "region".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "bucket".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "sequence".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "value".to_string(),
                    data_type: "double".to_string(),
                    nullable: true,
                    default: None,
                },
            ],
            comments: HashMap::new(),
        };

        let mut column_comparators = HashMap::new();
        column_comparators.insert("region".to_string(), ComparatorType::Text);
        column_comparators.insert("bucket".to_string(), ComparatorType::Int);
        column_comparators.insert("timestamp".to_string(), ComparatorType::Timestamp);
        column_comparators.insert("sequence".to_string(), ComparatorType::BigInt);
        column_comparators.insert("value".to_string(), ComparatorType::Float);

        let context = ParsingContext {
            schema,
            partition_comparators: vec![ComparatorType::Text, ComparatorType::Int],
            clustering_comparators: vec![ComparatorType::Timestamp, ComparatorType::BigInt],
            column_comparators,
        };

        let parser = SchemaParser::new(context).unwrap();

        // Test partition key parsing: text("US-WEST") + int(42)
        let mut partition_data = vec![0, 0, 0, 7]; // length of "US-WEST"
        partition_data.extend_from_slice(b"US-WEST");
        partition_data.extend_from_slice(&[0, 0, 0, 42]); // int 42

        let result = parser.parse_partition_key(&partition_data);
        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Text("US-WEST".to_string()));
        assert_eq!(values[1], Value::Integer(42));

        // Test clustering key parsing: timestamp(1640995200000) + bigint(123456789)
        let mut clustering_data = vec![0, 0, 1, 126, 45, 67, 89, 0]; // timestamp
        clustering_data.extend_from_slice(&[0, 0, 0, 0, 7, 91, 205, 21]); // bigint 123456789

        let result = parser.parse_clustering_keys(&clustering_data);
        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert!(matches!(values[0], Value::Timestamp(_)));
        assert_eq!(values[1], Value::BigInt(123456789));
    }

    #[test]
    fn test_nested_udt_in_frozen_collection() {
        // Create schema with a frozen list containing UDT
        let mut schema = create_test_schema();
        schema.columns.push(Column {
            name: "user_profiles".to_string(),
            data_type: "frozen<list<frozen<user_profile>>>".to_string(),
            nullable: true,
            default: None,
        });

        // Create comparator for frozen<list<frozen<user_profile>>>
        let udt_comparator = ComparatorType::Udt {
            type_name: "user_profile".to_string(),
            keyspace: Some("test_ks".to_string()),
            field_comparators: vec![
                ("name".to_string(), ComparatorType::Text),
                ("age".to_string(), ComparatorType::Int),
                ("email".to_string(), ComparatorType::Text),
            ],
        };

        let frozen_udt_comparator = ComparatorType::Frozen(Box::new(udt_comparator));
        let list_comparator = ComparatorType::List(Box::new(frozen_udt_comparator));
        let frozen_list_comparator = ComparatorType::Frozen(Box::new(list_comparator));

        let mut column_comparators = HashMap::new();
        column_comparators.insert("id".to_string(), ComparatorType::Int);
        column_comparators.insert("timestamp".to_string(), ComparatorType::BigInt);
        column_comparators.insert("name".to_string(), ComparatorType::Text);
        column_comparators.insert("data".to_string(), ComparatorType::Blob);
        column_comparators.insert(
            "tags".to_string(),
            ComparatorType::List(Box::new(ComparatorType::Text)),
        );
        column_comparators.insert("user_profiles".to_string(), frozen_list_comparator);

        let context = ParsingContext {
            schema,
            partition_comparators: vec![ComparatorType::Int],
            clustering_comparators: vec![ComparatorType::BigInt],
            column_comparators,
        };

        let parser = SchemaParser::new(context).unwrap();

        // Create test data: frozen<list<frozen<user_profile>>> with 1 UDT
        let mut data = vec![0, 0, 0, 1]; // list count = 1

        // UDT with 3 fields: name="John", age=30, email="john@example.com"
        // Field 1: name
        data.extend_from_slice(&[0, 0, 0, 4]); // length for "John"
        data.extend_from_slice(b"John");

        // Field 2: age
        data.extend_from_slice(&[0, 0, 0, 30]); // int 30

        // Field 3: email
        data.extend_from_slice(&[0, 0, 0, 16]); // length for "john@example.com"
        data.extend_from_slice(b"john@example.com");

        let result = parser.parse_column_value("user_profiles", &data);
        if let Err(e) = &result {
            eprintln!("Error parsing UDT: {:?}", e);
        }
        assert!(result.is_ok());

        let value = result.unwrap();
        assert!(matches!(value, Value::Frozen(_)));

        // Verify nested structure
        if let Value::Frozen(inner) = value {
            assert!(matches!(*inner, Value::List(_)));
        }
    }

    #[test]
    fn test_ordering_equivalence_byte_comparable_vs_typed() {
        let context = create_test_context();
        let parser = SchemaParser::new(context).unwrap();

        // Test with text values that should maintain ordering
        let values = ["apple", "banana", "cherry", "date"];
        let mut serialized_values = Vec::new();

        // Serialize each value
        for value_str in &values {
            let mut data = vec![0, 0, 0, value_str.len() as u8]; // length prefix
            data.extend_from_slice(value_str.as_bytes());
            serialized_values.push(data);
        }

        // Parse values back
        let mut parsed_values = Vec::new();
        for data in &serialized_values {
            let result = parser.parse_column_value("name", data);
            assert!(result.is_ok());
            parsed_values.push(result.unwrap());
        }

        // Verify ordering is preserved
        for i in 0..parsed_values.len() - 1 {
            let current = &parsed_values[i];
            let next = &parsed_values[i + 1];

            // Compare using the text comparator
            let comparator = ComparatorType::Text;
            let comparison = comparator.compare(current, next).unwrap();
            assert_eq!(
                comparison,
                std::cmp::Ordering::Less,
                "Value '{}' should be less than '{}'",
                values[i],
                values[i + 1]
            );
        }

        // Test byte-comparable ordering with integers
        let int_values = [10, 20, 30, 40];
        let mut int_data_values = Vec::new();

        for &value in &int_values {
            let data = vec![
                (value >> 24) as u8,
                (value >> 16) as u8,
                (value >> 8) as u8,
                value as u8,
            ];
            int_data_values.push(data);
        }

        let mut parsed_int_values = Vec::new();
        for data in &int_data_values {
            let result = parser.parse_column_value("id", data);
            assert!(result.is_ok());
            parsed_int_values.push(result.unwrap());
        }

        // Verify integer ordering
        for i in 0..parsed_int_values.len() - 1 {
            let current = &parsed_int_values[i];
            let next = &parsed_int_values[i + 1];

            let comparator = ComparatorType::Int;
            let comparison = comparator.compare(current, next).unwrap();
            assert_eq!(
                comparison,
                std::cmp::Ordering::Less,
                "Value {} should be less than {}",
                int_values[i],
                int_values[i + 1]
            );
        }

        // Test that byte-wise ordering matches typed ordering for these types
        // This ensures byte-comparable keys work correctly
        for i in 0..int_data_values.len() - 1 {
            let current_bytes = &int_data_values[i];
            let next_bytes = &int_data_values[i + 1];

            // Byte-wise comparison should match typed comparison
            assert!(
                current_bytes < next_bytes,
                "Byte ordering should match typed ordering for integers"
            );
        }
    }
}
