//! data_writer tests, group 3/6 (issue #1118 split).
//! Relocated verbatim from the original inline `mod tests`.

#![allow(unused_imports)]

use super::super::*;
use super::support::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema};
use crate::storage::serialization::types::TypeSerializer;
use crate::storage::write_engine::mutation::{CellOperation, ClusteringKey, PartitionKey, TableId};
use crate::types::UdtValue;
use std::collections::HashMap;

/// Large static-column subsets use the same delta encoding as regular columns.
#[test]
fn test_column_subset_65_static_columns_uses_missing_indexes_when_present_majority() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    // Create schema with 65 static columns
    let columns: Vec<Column> = (0..65)
        .map(|i| Column {
            name: format!("scol_{:03}", i),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: true,
        })
        .collect();

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Write all but one static column so the encoding emits missing indexes.
    let mut operations = Vec::new();
    for i in 0..65 {
        if i == 17 {
            continue;
        }
        operations.push(CellOperation::Write {
            column: format!("scol_{:03}", i),
            value: Value::Text(format!("value-{}", i)),
        });
    }

    let mutation = Mutation::new(table_id, pk, None, operations, 1001000, None);
    let static_ops: Vec<StaticMergedOp> = mutation
        .operations
        .iter()
        .map(|op| StaticMergedOp {
            op: op.clone(),
            timestamp_micros: mutation.timestamp_micros,
            cell_local_deletion_time: mutation.effective_local_deletion_time(),
        })
        .collect();

    let mut buf = Vec::new();
    writer
        .write_static_column_bitmap(&mut buf, &static_ops, &schema)
        .unwrap();

    // missing_count=1, followed by the missing column index.
    assert_eq!(buf, vec![1, 17]);
}

/// Smaller subsets still use the missing-column bitmap.
#[test]
fn test_column_subset_under_64_regular_columns_uses_bitmap() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let columns: Vec<Column> = (0..4)
        .map(|i| Column {
            name: format!("col_{i}"),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        })
        .collect();

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Only col_1 is present, so bits 0, 2, and 3 are set.
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "col_1".to_string(),
            value: Value::Text("present".to_string()),
        }],
        1001000,
        None,
    );

    let mut buf = Vec::new();
    writer
        .write_column_bitmap(&mut buf, &mutation, &schema)
        .unwrap();

    assert_eq!(buf, vec![0b1101]);
}

#[test]
fn test_regular_columns_sort_simple_before_complex() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "z_simple".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "a_complex".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "m_simple".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let ordered = writer.regular_columns(&schema);
    let names: Vec<_> = ordered.iter().map(|column| column.name.as_str()).collect();

    assert_eq!(names, vec!["m_simple", "z_simple", "a_complex"]);
}

#[test]
fn test_static_columns_sort_simple_before_complex() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "z_static_simple".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "a_static_complex".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "m_static_simple".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let ordered = writer.static_columns(&schema);
    let names: Vec<_> = ordered.iter().map(|column| column.name.as_str()).collect();

    assert_eq!(
        names,
        vec!["m_static_simple", "z_static_simple", "a_static_complex"]
    );
}

#[test]
fn test_write_column_bitmap_zero_when_all_columns_present() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let columns: Vec<Column> = (0..65)
        .map(|i| Column {
            name: format!("col_{:03}", i),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        })
        .collect();

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));

    let operations: Vec<_> = (0..65)
        .map(|i| CellOperation::Write {
            column: format!("col_{:03}", i),
            value: Value::Text(format!("value-{}", i)),
        })
        .collect();

    let mutation = Mutation::new(table_id, pk, None, operations, 1001000, None);

    let mut buf = Vec::new();
    writer
        .write_column_bitmap(&mut buf, &mutation, &schema)
        .unwrap();

    assert_eq!(buf, vec![0]);
}

#[test]
fn test_serialize_list() {
    let list = Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    let bytes = serialize_value(&list).unwrap();
    // 4 bytes count + 3 * (4 bytes len + 4 bytes i32)
    assert_eq!(bytes.len(), 4 + 3 * 8);
    // Count = 3
    assert_eq!(&bytes[0..4], &3i32.to_be_bytes());
    // First element length = 4
    assert_eq!(&bytes[4..8], &4i32.to_be_bytes());
    // First element value = 1
    assert_eq!(&bytes[8..12], &1i32.to_be_bytes());
}

#[test]
fn test_serialize_empty_list() {
    let list = Value::List(vec![]);
    let bytes = serialize_value(&list).unwrap();
    assert_eq!(bytes.len(), 4);
    assert_eq!(&bytes[0..4], &0i32.to_be_bytes());
}

#[test]
fn test_serialize_single_element_list() {
    let list = Value::List(vec![Value::Integer(42)]);
    let bytes = serialize_value(&list).unwrap();
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x01, // count = 1
            0x00, 0x00, 0x00, 0x04, // len = 4
            0x00, 0x00, 0x00, 0x2A, // value = 42
        ]
    );
}

#[test]
fn test_serialize_set() {
    let set = Value::Set(vec![
        Value::Text("alpha".to_string()),
        Value::Text("beta".to_string()),
    ]);
    let bytes = serialize_value(&set).unwrap();
    // Count = 2
    assert_eq!(&bytes[0..4], &2i32.to_be_bytes());
    // First element length = 5 ("alpha")
    assert_eq!(&bytes[4..8], &5i32.to_be_bytes());
    assert_eq!(&bytes[8..13], b"alpha");
}

#[test]
fn test_serialize_single_element_set() {
    let set = Value::Set(vec![Value::Text("alpha".to_string())]);
    let bytes = serialize_value(&set).unwrap();
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x01, // count = 1
            0x00, 0x00, 0x00, 0x05, // len = 5
            b'a', b'l', b'p', b'h', b'a', // value = "alpha"
        ]
    );
}

#[test]
fn test_serialize_empty_set() {
    let set = Value::Set(vec![]);
    let bytes = serialize_value(&set).unwrap();
    assert_eq!(bytes, 0i32.to_be_bytes().to_vec());
}

#[test]
fn test_serialize_map() {
    let map = Value::Map(vec![(Value::Text("key1".to_string()), Value::Integer(100))]);
    let bytes = serialize_value(&map).unwrap();
    // Count = 1
    assert_eq!(&bytes[0..4], &1i32.to_be_bytes());
    // Key length = 4 ("key1")
    assert_eq!(&bytes[4..8], &4i32.to_be_bytes());
    assert_eq!(&bytes[8..12], b"key1");
    // Value length = 4 (i32)
    assert_eq!(&bytes[12..16], &4i32.to_be_bytes());
    // Value = 100
    assert_eq!(&bytes[16..20], &100i32.to_be_bytes());
}

#[test]
fn test_serialize_empty_map() {
    let map = Value::Map(vec![]);
    let bytes = serialize_value(&map).unwrap();
    assert_eq!(bytes.len(), 4);
    assert_eq!(&bytes[0..4], &0i32.to_be_bytes());
}

#[test]
fn test_serialize_tuple() {
    let tuple = Value::Tuple(vec![
        Value::Integer(42),
        Value::Text("hello".to_string()),
        Value::Null,
    ]);
    let bytes = serialize_value(&tuple).unwrap();
    // Field 1: 4 bytes len + 4 bytes i32 = 8
    assert_eq!(&bytes[0..4], &4i32.to_be_bytes());
    assert_eq!(&bytes[4..8], &42i32.to_be_bytes());
    // Field 2: 4 bytes len + 5 bytes text = 9
    assert_eq!(&bytes[8..12], &5i32.to_be_bytes());
    assert_eq!(&bytes[12..17], b"hello");
    // Field 3: NULL = -1 as i32
    assert_eq!(&bytes[17..21], &(-1i32).to_be_bytes());
}

#[test]
fn test_serialize_single_element_tuple() {
    let tuple = Value::Tuple(vec![Value::Text("solo".to_string())]);
    let bytes = serialize_value(&tuple).unwrap();
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x04, // len = 4
            b's', b'o', b'l', b'o', // value = "solo"
        ]
    );
}

#[test]
fn test_serialize_frozen() {
    let frozen = Value::Frozen(Box::new(Value::List(vec![
        Value::Integer(10),
        Value::Integer(20),
    ])));
    let frozen_bytes = serialize_value(&frozen).unwrap();
    let list_bytes =
        serialize_value(&Value::List(vec![Value::Integer(10), Value::Integer(20)])).unwrap();
    // Frozen should produce identical bytes to inner value
    assert_eq!(frozen_bytes, list_bytes);
}

#[test]
fn test_serialize_single_element_frozen() {
    let frozen = Value::Frozen(Box::new(Value::List(vec![Value::Text("solo".to_string())])));
    let frozen_bytes = serialize_value(&frozen).unwrap();
    let list_bytes = serialize_value(&Value::List(vec![Value::Text("solo".to_string())])).unwrap();
    assert_eq!(frozen_bytes, list_bytes);
}

#[test]
fn test_serialize_nested_collection() {
    // MAP<TEXT, FROZEN<LIST<INT>>>
    let nested = Value::Map(vec![(
        Value::Text("nums".to_string()),
        Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
        ]))),
    )]);
    let bytes = serialize_value(&nested).unwrap();
    // Should not error - validates nested serialization works
    assert!(!bytes.is_empty());
    // Count = 1
    assert_eq!(&bytes[0..4], &1i32.to_be_bytes());
}

#[test]
fn test_serialize_udt_with_nested_collections_matches_schema_aware_bytes() {
    let serializer = TypeSerializer::new();
    let company = phase3_company_value();

    let bytes = serialize_value(&Value::Udt(company.clone())).unwrap();
    let expected = serializer
        .serialize_udt(&Value::Udt(company), &phase3_company_schema())
        .unwrap();

    assert_eq!(bytes, expected);
}

#[test]
fn test_serialize_collection_containing_nested_udts() {
    let serializer = TypeSerializer::new();
    let company = phase3_company_value();
    let company_bytes = serializer
        .serialize_udt(&Value::Udt(company.clone()), &phase3_company_schema())
        .unwrap();

    let value = Value::Map(vec![(
        Value::Text("empresa_日本".to_string()),
        Value::Frozen(Box::new(Value::Udt(company))),
    )]);
    let bytes = serialize_value(&value).unwrap();

    let key = "empresa_日本".as_bytes();
    let mut expected = Vec::new();
    expected.extend_from_slice(&1i32.to_be_bytes());
    expected.extend_from_slice(&(key.len() as i32).to_be_bytes());
    expected.extend_from_slice(key);
    expected.extend_from_slice(&(company_bytes.len() as i32).to_be_bytes());
    expected.extend_from_slice(&company_bytes);

    assert_eq!(bytes, expected);
}

#[test]
fn test_serialize_tuple_with_collection_fields_and_udt() {
    let serializer = TypeSerializer::new();
    let address = phase3_address_value();
    let person = phase3_person_value("Tuple User");
    let address_bytes = serializer
        .serialize_udt(&Value::Udt(address.clone()), &phase3_address_schema())
        .unwrap();
    let person_bytes = serializer
        .serialize_udt(&Value::Udt(person.clone()), &phase3_person_schema())
        .unwrap();

    let tuple = Value::Tuple(vec![
        Value::Text("phase3".to_string()),
        Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(3),
            Value::Integer(5),
            Value::Integer(8),
        ]))),
        Value::Frozen(Box::new(Value::Map(vec![(
            Value::Text("home".to_string()),
            Value::Frozen(Box::new(Value::Udt(address))),
        )]))),
        Value::Frozen(Box::new(Value::Udt(person))),
    ]);
    let bytes = serialize_value(&tuple).unwrap();

    let list_bytes = serialize_value(&Value::List(vec![
        Value::Integer(3),
        Value::Integer(5),
        Value::Integer(8),
    ]))
    .unwrap();
    let map_bytes = {
        let key = b"home";
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&1i32.to_be_bytes());
        encoded.extend_from_slice(&(key.len() as i32).to_be_bytes());
        encoded.extend_from_slice(key);
        encoded.extend_from_slice(&(address_bytes.len() as i32).to_be_bytes());
        encoded.extend_from_slice(&address_bytes);
        encoded
    };

    let mut expected = Vec::new();
    expected.extend_from_slice(&6i32.to_be_bytes());
    expected.extend_from_slice(b"phase3");
    expected.extend_from_slice(&(list_bytes.len() as i32).to_be_bytes());
    expected.extend_from_slice(&list_bytes);
    expected.extend_from_slice(&(map_bytes.len() as i32).to_be_bytes());
    expected.extend_from_slice(&map_bytes);
    expected.extend_from_slice(&(person_bytes.len() as i32).to_be_bytes());
    expected.extend_from_slice(&person_bytes);

    assert_eq!(bytes, expected);
}

#[test]
fn test_serialize_high_complexity_nested_collection() {
    let nested = Value::Map(vec![(
        Value::Text("outer".to_string()),
        Value::Frozen(Box::new(Value::List(vec![Value::Frozen(Box::new(
            Value::Map(vec![(
                Value::Text("inner".to_string()),
                Value::Frozen(Box::new(Value::List(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                ]))),
            )]),
        ))]))),
    )]);

    let bytes = serialize_value(&nested).unwrap();

    assert!(!bytes.is_empty());
    assert_eq!(&bytes[0..4], &1i32.to_be_bytes());
}
