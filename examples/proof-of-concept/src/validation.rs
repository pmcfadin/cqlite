//! Validation utilities for proof-of-concept testing

use cqlite_core::{Value, Result};

/// Validate that two values are approximately equal
pub fn values_approximately_equal(v1: &Value, v2: &Value) -> bool {
    match (v1, v2) {
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Text(a), Value::Text(b)) => a == b,
        (Value::Boolean(a), Value::Boolean(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
        (Value::List(a), Value::List(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_approximately_equal(x, y))
        }
        (Value::Set(a), Value::Set(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_approximately_equal(x, y))
        }
        (Value::Map(a), Value::Map(b)) => {
            a.len() == b.len() && 
            a.iter().zip(b.iter()).all(|((k1, v1), (k2, v2))| 
                values_approximately_equal(k1, k2) && values_approximately_equal(v1, v2)
            )
        }
        (Value::Tuple(a), Value::Tuple(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_approximately_equal(x, y))
        }
        _ => std::mem::discriminant(v1) == std::mem::discriminant(v2),
    }
}

/// Simple serialization for testing
pub fn serialize_value_for_test(value: &Value) -> Result<Vec<u8>> {
    use cqlite_core::parser::types::serialize_cql_value;
    serialize_cql_value(value)
}