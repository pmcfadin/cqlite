//! Test data generators for proof-of-concept demonstrations

use cqlite_core::{Value, types::{UdtValue, UdtField, TableId, RowKey}};

/// Generate simple test data for basic validation
pub fn generate_simple_test_data() -> Vec<(TableId, RowKey, Value)> {
    (1..=100).map(|i| {
        (
            TableId::new("simple_table".to_string()),
            RowKey::from(format!("key_{}", i)),
            Value::Tuple(vec![
                Value::Integer(i),
                Value::Text(format!("name_{}", i)),
                Value::Boolean(i % 2 == 0),
            ])
        )
    }).collect()
}

/// Generate complex test data with all supported complex types
pub fn generate_complex_test_data() -> Vec<(TableId, RowKey, Value)> {
    (1..=50).map(|i| {
        (
            TableId::new("complex_table".to_string()),
            RowKey::from(format!("complex_key_{}", i)),
            create_complex_test_value(i)
        )
    }).collect()
}

/// Create complex test value with nested structures
fn create_complex_test_value(id: i32) -> Value {
    let scores_list = Value::List(vec![
        Value::Integer(id),
        Value::Integer(id * 2),
        Value::Integer(id * 3),
    ]);
    
    let tags_set = Value::Set(vec![
        Value::Text(format!("tag_{}", id % 5)),
        Value::Text("test".to_string()),
        Value::Text("complex".to_string()),
    ]);
    
    let metadata_map = Value::Map(vec![
        (Value::Text("id".to_string()), Value::Integer(id)),
        (Value::Text("score".to_string()), Value::Integer(id % 100)),
        (Value::Text("category".to_string()), Value::Text(format!("cat_{}", id % 3))),
    ]);
    
    let location_tuple = Value::Tuple(vec![
        Value::Float(37.7749 + (id as f64 * 0.001)),   // latitude
        Value::Float(-122.4194 + (id as f64 * 0.001)), // longitude
        Value::Text(format!("City_{}", id % 10)),
    ]);
    
    let address_udt = create_address_udt(
        &format!("{} Main St", id * 100),
        &format!("City_{}", id % 10),
        10000 + id,
        (37.7749 + (id as f64 * 0.001), -122.4194 + (id as f64 * 0.001))
    );
    
    // Main complex record
    Value::Tuple(vec![
        Value::Integer(id),
        Value::Text(format!("record_{}", id)),
        scores_list,
        tags_set,
        metadata_map,
        location_tuple,
        address_udt,
        Value::Boolean(id % 2 == 0),
    ])
}

/// Create address UDT value
pub fn create_address_udt(street: &str, city: &str, zipcode: i32, coordinates: (f64, f64)) -> Value {
    let fields = vec![
        UdtField {
            name: "street".to_string(),
            value: Some(Value::Text(street.to_string())),
        },
        UdtField {
            name: "city".to_string(),
            value: Some(Value::Text(city.to_string())),
        },
        UdtField {
            name: "zipcode".to_string(),
            value: Some(Value::Integer(zipcode)),
        },
        UdtField {
            name: "coordinates".to_string(),
            value: Some(Value::Tuple(vec![
                Value::Float(coordinates.0),
                Value::Float(coordinates.1),
            ])),
        },
    ];
    
    Value::Udt(UdtValue {
        type_name: "address".to_string(),
        keyspace: "test".to_string(),
        fields,
    })
}