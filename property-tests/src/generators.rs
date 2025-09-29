//! Property test generators for CQL types

use proptest::prelude::*;
use crate::types::*;

// ============================================================================
// Core Value Generators
// ============================================================================

/// Generates arbitrary primitive CQL values
pub fn arb_primitive_cql_value() -> impl Strategy<Value = CqlValue> {
    prop_oneof![
        Just(CqlValue::Null),
        any::<bool>().prop_map(CqlValue::Boolean),
        any::<i32>().prop_map(CqlValue::Integer),
        any::<i64>().prop_map(CqlValue::BigInt),
        any::<f64>().prop_map(|f| CqlValue::Float(OrderedFloat(f))),
        any::<f32>().prop_map(|f| CqlValue::Float32(OrderedFloat32(f))),
        any::<i8>().prop_map(CqlValue::TinyInt),
        any::<i16>().prop_map(CqlValue::SmallInt),
        arb_text(),
        arb_blob(),
        any::<i64>().prop_map(CqlValue::Timestamp),
        any::<[u8; 16]>().prop_map(CqlValue::Uuid),
        arb_varint(),
        arb_decimal(),
        arb_duration(),
        arb_json(),
    ]
}

/// Generates arbitrary CQL values including collections
pub fn arb_cql_value() -> impl Strategy<Value = CqlValue> {
    let leaf = arb_primitive_cql_value();

    leaf.prop_recursive(
        8,   // Max depth
        256, // Max nodes
        10,  // Items per collection
        |inner| prop_oneof![
            prop::collection::vec(inner.clone(), 0..10).prop_map(CqlValue::List),
            prop::collection::vec(inner.clone(), 0..10).prop_map(CqlValue::Set),
            prop::collection::vec((inner.clone(), inner.clone()), 0..10).prop_map(CqlValue::Map),
            prop::collection::vec(inner.clone(), 0..5).prop_map(CqlValue::Tuple),
            arb_udt_with_inner(inner.clone()),
            inner.prop_map(|v| CqlValue::Frozen(Box::new(v))),
            arb_tombstone(),
        ]
    )
}

// ============================================================================
// Specific Type Generators
// ============================================================================

/// Generates text values with various Unicode patterns
fn arb_text() -> impl Strategy<Value = CqlValue> {
    prop_oneof![
        // Empty string
        Just("".to_string()),

        // ASCII only
        "[a-zA-Z0-9 .,!?]{0,1000}".prop_map(String::from),

        // Unicode patterns
        "[\u{0000}-\u{007F}]{0,500}",     // ASCII
        "[\u{0080}-\u{07FF}]{0,250}",     // 2-byte UTF-8
        "[\u{0800}-\u{FFFF}]{0,166}",     // 3-byte UTF-8
        "[\u{10000}-\u{10FFFF}]{0,125}",  // 4-byte UTF-8

        // Common strings
        prop_oneof![
            Just("Hello, World!".to_string()),
            Just("".to_string()),
            Just("null".to_string()),
            Just("true".to_string()),
            Just("false".to_string()),
            Just("0".to_string()),
            Just("-1".to_string()),
        ],

        // Long strings
        "[a-zA-Z0-9]{1000,10000}".prop_map(String::from),
    ].prop_map(CqlValue::Text)
}

/// Generates blob values with various patterns
fn arb_blob() -> impl Strategy<Value = CqlValue> {
    prop_oneof![
        // Empty
        Just(vec![]),

        // Single byte
        any::<u8>().prop_map(|b| vec![b]),

        // Random data
        prop::collection::vec(any::<u8>(), 0..10000),

        // Repetitive patterns (compressible)
        (any::<u8>(), 1..1000usize).prop_map(|(byte, len)| vec![byte; len]),

        // Alternating patterns
        (any::<u8>(), any::<u8>(), 100..1000usize).prop_map(|(a, b, len)| {
            (0..len).map(|i| if i % 2 == 0 { a } else { b }).collect()
        }),

        // Common binary prefixes
        prop_oneof![
            Just(vec![0xFF, 0xFE]),               // BOM
            Just(vec![0x89, 0x50, 0x4E, 0x47]),  // PNG header
            Just(vec![0x00, 0x00, 0x00, 0x00]),  // Null bytes
            Just(vec![0xFF, 0xFF, 0xFF, 0xFF]),  // Max bytes
        ],
    ].prop_map(CqlValue::Blob)
}

/// Generates varint values
fn arb_varint() -> impl Strategy<Value = CqlValue> {
    prop_oneof![
        // Small varints
        prop::collection::vec(any::<u8>(), 1..8),

        // Large varints
        prop::collection::vec(any::<u8>(), 8..32),

        // Common patterns
        Just(vec![0]),
        Just(vec![1]),
        Just(vec![0xFF]),
        Just(vec![0x80, 0x01]),
    ].prop_map(CqlValue::Varint)
}

/// Generates decimal values
fn arb_decimal() -> impl Strategy<Value = CqlValue> {
    (
        any::<i32>(),
        prop::collection::vec(any::<u8>(), 1..32)
    ).prop_map(|(scale, unscaled)| CqlValue::Decimal { scale, unscaled })
}

/// Generates duration values
fn arb_duration() -> impl Strategy<Value = CqlValue> {
    (
        any::<i32>(),
        any::<i32>(),
        any::<i64>()
    ).prop_map(|(months, days, nanos)| CqlValue::Duration { months, days, nanos })
}

/// Generates JSON values
fn arb_json() -> impl Strategy<Value = CqlValue> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(|n| serde_json::Value::Number(n.into())),
        "[a-zA-Z0-9 ]{0,100}".prop_map(serde_json::Value::String),
    ];

    leaf.prop_recursive(
        4,   // Max depth for JSON
        64,  // Max nodes
        5,   // Items per collection
        |inner| prop_oneof![
            prop::collection::vec(inner.clone(), 0..5)
                .prop_map(serde_json::Value::Array),
            prop::collection::btree_map("[a-zA-Z]{1,10}", inner, 0..5)
                .prop_map(|m| serde_json::Value::Object(m.into_iter().collect())),
        ]
    ).prop_map(CqlValue::Json)
}

/// Generates UDT values with nested structure
fn arb_udt_with_inner(inner: impl Strategy<Value = CqlValue> + Clone) -> impl Strategy<Value = CqlValue> {
    (
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // type_name
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // keyspace
        prop::collection::vec(
            (
                "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // field name
                prop::option::of(inner)         // field value
            ).prop_map(|(name, value)| UdtField { name, value }),
            0..10
        )
    ).prop_map(|(type_name, keyspace, fields)| {
        CqlValue::Udt(UdtValue {
            type_name,
            keyspace,
            fields,
        })
    })
}

/// Generates simple UDT values
pub fn arb_udt() -> impl Strategy<Value = CqlValue> {
    (
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // type_name
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // keyspace
        prop::collection::vec(
            (
                "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // field name
                prop::option::of(arb_primitive_cql_value())         // field value
            ).prop_map(|(name, value)| UdtField { name, value }),
            0..10
        )
    ).prop_map(|(type_name, keyspace, fields)| {
        CqlValue::Udt(UdtValue {
            type_name,
            keyspace,
            fields,
        })
    })
}

/// Generates tombstone values
fn arb_tombstone() -> impl Strategy<Value = CqlValue> {
    (any::<i64>(), any::<i32>())
        .prop_map(|(deletion_time, local_deletion_time)| {
            CqlValue::Tombstone(TombstoneInfo {
                deletion_time,
                local_deletion_time,
            })
        })
}

// ============================================================================
// Edge Case Generators
// ============================================================================

/// Generates extreme numeric values
pub fn arb_extreme_numerics() -> impl Strategy<Value = CqlValue> {
    prop_oneof![
        // Integer boundaries
        Just(CqlValue::Integer(i32::MIN)),
        Just(CqlValue::Integer(i32::MAX)),
        Just(CqlValue::Integer(0)),
        Just(CqlValue::Integer(-1)),
        Just(CqlValue::Integer(1)),

        // BigInt boundaries
        Just(CqlValue::BigInt(i64::MIN)),
        Just(CqlValue::BigInt(i64::MAX)),
        Just(CqlValue::BigInt(0)),

        // Float special values
        Just(CqlValue::Float(OrderedFloat(f64::INFINITY))),
        Just(CqlValue::Float(OrderedFloat(f64::NEG_INFINITY))),
        Just(CqlValue::Float(OrderedFloat(f64::NAN))),
        Just(CqlValue::Float(OrderedFloat(0.0))),
        Just(CqlValue::Float(OrderedFloat(-0.0))),
        Just(CqlValue::Float(OrderedFloat(f64::MIN))),
        Just(CqlValue::Float(OrderedFloat(f64::MAX))),
        Just(CqlValue::Float(OrderedFloat(f64::EPSILON))),

        // Float32 special values
        Just(CqlValue::Float32(OrderedFloat32(f32::INFINITY))),
        Just(CqlValue::Float32(OrderedFloat32(f32::NEG_INFINITY))),
        Just(CqlValue::Float32(OrderedFloat32(f32::NAN))),
        Just(CqlValue::Float32(OrderedFloat32(0.0))),
        Just(CqlValue::Float32(OrderedFloat32(-0.0))),

        // TinyInt boundaries
        Just(CqlValue::TinyInt(i8::MIN)),
        Just(CqlValue::TinyInt(i8::MAX)),
        Just(CqlValue::TinyInt(0)),

        // SmallInt boundaries
        Just(CqlValue::SmallInt(i16::MIN)),
        Just(CqlValue::SmallInt(i16::MAX)),
        Just(CqlValue::SmallInt(0)),
    ]
}

/// Generates deeply nested structures to test recursion limits
pub fn arb_deeply_nested(max_depth: usize) -> impl Strategy<Value = CqlValue> {
    let leaf = arb_primitive_cql_value();

    leaf.prop_recursive(
        max_depth as u32,
        (max_depth * 50) as u32,
        3,
        |inner| prop_oneof![
            prop::collection::vec(inner.clone(), 1..3).prop_map(CqlValue::List),
            // Simplified UDT for nested structures to avoid clone issues
            (
                "[a-zA-Z][a-zA-Z0-9_]{0,20}",
                "[a-zA-Z][a-zA-Z0-9_]{0,20}",
                prop::collection::vec(
                    (
                        "[a-zA-Z][a-zA-Z0-9_]{0,20}",
                        prop::option::of(inner.clone())
                    ).prop_map(|(name, value)| UdtField { name, value }),
                    0..3
                )
            ).prop_map(|(type_name, keyspace, fields)| {
                CqlValue::Udt(UdtValue { type_name, keyspace, fields })
            }),
            inner.prop_map(|v| CqlValue::Frozen(Box::new(v))),
        ]
    )
}

/// Generates large collections to test memory limits
pub fn arb_large_collections(max_size: usize) -> impl Strategy<Value = CqlValue> {
    prop_oneof![
        prop::collection::vec(arb_primitive_cql_value(), 0..max_size).prop_map(CqlValue::List),
        prop::collection::vec(arb_primitive_cql_value(), 0..max_size).prop_map(CqlValue::Set),
        prop::collection::vec(
            (arb_primitive_cql_value(), arb_primitive_cql_value()),
            0..max_size
        ).prop_map(CqlValue::Map),
    ]
}

// ============================================================================
// Schema Generators
// ============================================================================

/// Generates schema definitions
pub fn arb_schema() -> impl Strategy<Value = Schema> {
    (
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // keyspace
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // table
        prop::collection::vec(arb_key_column(), 1..5),   // partition_keys
        prop::collection::vec(arb_key_column(), 0..5),   // clustering_keys
        prop::collection::vec(arb_column(), 0..20),      // columns
        prop::collection::btree_map(
            "[a-zA-Z][a-zA-Z0-9_]{0,63}",
            "[a-zA-Z0-9 .,!?]{0,200}",
            0..5
        ).prop_map(|m| m.into_iter().collect()) // comments
    ).prop_map(|(keyspace, table, partition_keys, clustering_keys, columns, comments)| {
        Schema {
            keyspace,
            table,
            partition_keys,
            clustering_keys,
            columns,
            comments,
        }
    })
}

/// Generates key column definitions
fn arb_key_column() -> impl Strategy<Value = KeyColumn> {
    (
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // name
        prop_oneof![                    // data_type
            Just("text"), Just("int"), Just("bigint"),
            Just("boolean"), Just("blob"), Just("timestamp"),
            Just("uuid"), Just("double"), Just("float")
        ],
        any::<usize>()                  // position
    ).prop_map(|(name, data_type, position)| KeyColumn {
        name,
        data_type: data_type.to_string(),
        position,
    })
}

/// Generates regular column definitions
fn arb_column() -> impl Strategy<Value = Column> {
    (
        "[a-zA-Z][a-zA-Z0-9_]{0,63}",  // name
        prop_oneof![                    // data_type
            Just("text"), Just("int"), Just("bigint"),
            Just("boolean"), Just("blob"), Just("timestamp"),
            Just("uuid"), Just("double"), Just("float"),
            Just("list<text>"), Just("set<int>"), Just("map<text,int>")
        ],
        any::<bool>()                   // is_static
    ).prop_map(|(name, data_type, is_static)| Column {
        name,
        data_type: data_type.to_string(),
        is_static,
    })
}