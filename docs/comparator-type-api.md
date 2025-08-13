# ComparatorType API Documentation

## Overview

The `ComparatorType` system provides comprehensive comparison logic for all CQL data types in CQLite. It ensures proper ordering and equality comparison that matches Cassandra's comparison semantics, supporting primitive types, collections, UDTs, and frozen types.

## Table of Contents

1. [Quick Start](#quick-start)
2. [ComparatorType Enum](#comparatortype-enum)
3. [Creating Comparators](#creating-comparators)
4. [Comparison Operations](#comparison-operations)
5. [Type Compatibility](#type-compatibility)
6. [Integration Examples](#integration-examples)
7. [Performance Considerations](#performance-considerations)
8. [Error Handling](#error-handling)
9. [Advanced Usage](#advanced-usage)

## Quick Start

```rust
use cqlite_core::types::{ComparatorType, Value};
use cqlite_core::schema::CqlType;

// Create a comparator for text types
let text_comparator = ComparatorType::Text;

// Compare two text values
let val1 = Value::Text("apple".to_string());
let val2 = Value::Text("banana".to_string());

let result = text_comparator.compare(&val1, &val2)?;
assert_eq!(result, std::cmp::Ordering::Less);

// Check equality
assert!(!text_comparator.equals(&val1, &val2)?);

// Create from CQL type
let cql_type = CqlType::List(Box::new(CqlType::Int));
let list_comparator = ComparatorType::from_cql_type(&cql_type)?;
```

## ComparatorType Enum

The `ComparatorType` enum supports all CQL data types:

### Primitive Types

| Type | Description | Ordering Support |
|------|-------------|------------------|
| `Boolean` | Boolean values | ✅ |
| `TinyInt` | 8-bit signed integer | ✅ |
| `SmallInt` | 16-bit signed integer | ✅ |
| `Int` | 32-bit signed integer | ✅ |
| `BigInt` | 64-bit signed integer | ✅ |
| `Varint` | Variable-length integer | ✅ |
| `Float` | 32-bit floating point | ✅ |
| `Double` | 64-bit floating point | ✅ |
| `Decimal` | Arbitrary precision decimal | ✅ |
| `Text` | UTF-8 text (lexicographic) | ✅ |
| `Ascii` | ASCII text | ✅ |
| `Varchar` | Variable-length text | ✅ |
| `Blob` | Binary data (byte-wise) | ✅ |
| `Timestamp` | Timestamp (chronological) | ✅ |
| `Date` | Date values | ✅ |
| `Time` | Time values | ✅ |
| `Uuid` | UUID (byte-wise) | ✅ |
| `TimeUuid` | Time-based UUID | ✅ |
| `Counter` | Counter values | ✅ |
| `Duration` | Duration values | ✅ |
| `Inet` | Internet addresses | ✅ |

### Collection Types

| Type | Description | Ordering Support |
|------|-------------|------------------|
| `List(ComparatorType)` | Ordered list with element comparator | ✅ |
| `Set(ComparatorType)` | Unordered set with element comparator | ❌ (equality only) |
| `Map(key, value)` | Map with key and value comparators | ❌ (equality only) |

### Complex Types

| Type | Description | Ordering Support |
|------|-------------|------------------|
| `Tuple(Vec<ComparatorType>)` | Tuple with field comparators | ✅ |
| `Udt { type_name, keyspace, field_comparators }` | User-defined type | ✅ |
| `Frozen(ComparatorType)` | Frozen wrapper | ✅ (depends on inner) |
| `Custom(String)` | Custom/unknown types | ❌ (equality only) |

## Creating Comparators

### From CQL Types

```rust
use cqlite_core::schema::CqlType;
use cqlite_core::types::ComparatorType;

// Primitive type
let int_type = CqlType::Int;
let int_comparator = ComparatorType::from_cql_type(&int_type)?;

// Collection type
let list_type = CqlType::List(Box::new(CqlType::Text));
let list_comparator = ComparatorType::from_cql_type(&list_type)?;

// Map type
let map_type = CqlType::Map(
    Box::new(CqlType::Text), 
    Box::new(CqlType::Int)
);
let map_comparator = ComparatorType::from_cql_type(&map_type)?;

// UDT type
let udt_type = CqlType::Udt("Person".to_string(), vec![
    ("name".to_string(), CqlType::Text),
    ("age".to_string(), CqlType::Int),
]);
let udt_comparator = ComparatorType::from_cql_type(&udt_type)?;
```

### From CQL Type Specifications

```rust
use cqlite_core::types::{ComparatorType, CqlTypeSpec};

// From type spec (includes keyspace information for UDTs)
let udt_spec = CqlTypeSpec::Udt {
    keyspace: Some("myapp".to_string()),
    name: "Address".to_string(),
    fields: vec![
        ("street".to_string(), CqlTypeSpec::Text),
        ("city".to_string(), CqlTypeSpec::Text),
        ("zip".to_string(), CqlTypeSpec::Int),
    ],
};
let udt_comparator = ComparatorType::from_cql_type_spec(&udt_spec)?;
```

### Manual Construction

```rust
use cqlite_core::types::ComparatorType;

// Simple types
let boolean_comparator = ComparatorType::Boolean;
let text_comparator = ComparatorType::Text;

// Collection types
let string_list_comparator = ComparatorType::List(
    Box::new(ComparatorType::Text)
);

let metadata_map_comparator = ComparatorType::Map(
    Box::new(ComparatorType::Text),
    Box::new(ComparatorType::Text),
);

// Tuple type
let coordinate_tuple_comparator = ComparatorType::Tuple(vec![
    ComparatorType::Double, // latitude
    ComparatorType::Double, // longitude
]);

// UDT type
let person_udt_comparator = ComparatorType::Udt {
    type_name: "Person".to_string(),
    keyspace: Some("myapp".to_string()),
    field_comparators: vec![
        ("id".to_string(), ComparatorType::Uuid),
        ("name".to_string(), ComparatorType::Text),
        ("age".to_string(), ComparatorType::Int),
    ],
};
```

## Comparison Operations

### Basic Comparisons

```rust
use cqlite_core::types::{ComparatorType, Value};
use std::cmp::Ordering;

let comparator = ComparatorType::Int;
let val1 = Value::Integer(10);
let val2 = Value::Integer(20);

// Full comparison
let result = comparator.compare(&val1, &val2)?;
assert_eq!(result, Ordering::Less);

// Convenience methods
assert!(comparator.less_than(&val1, &val2)?);
assert!(comparator.greater_than(&val2, &val1)?);
assert!(comparator.less_than_or_equal(&val1, &val2)?);
assert!(comparator.greater_than_or_equal(&val2, &val1)?);
assert!(!comparator.equals(&val1, &val2)?);
```

### Null Handling

```rust
let comparator = ComparatorType::Text;
let null_val = Value::Null;
let text_val = Value::Text("hello".to_string());

// Nulls are always less than non-nulls
assert_eq!(comparator.compare(&null_val, &null_val)?, Ordering::Equal);
assert_eq!(comparator.compare(&null_val, &text_val)?, Ordering::Less);
assert_eq!(comparator.compare(&text_val, &null_val)?, Ordering::Greater);
```

### Collection Comparisons

```rust
use cqlite_core::types::{CollectionValue, CqlTypeSpec};

let list_comparator = ComparatorType::List(Box::new(ComparatorType::Int));

let list1 = Value::List(CollectionValue::new(
    CqlTypeSpec::Int,
    vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
));

let list2 = Value::List(CollectionValue::new(
    CqlTypeSpec::Int,
    vec![Value::Integer(1), Value::Integer(2), Value::Integer(4)],
));

// Lists are compared element by element, then by length
assert_eq!(list_comparator.compare(&list1, &list2)?, Ordering::Less);
```

### UDT Comparisons

```rust
use cqlite_core::types::UdtValue;
use std::collections::HashMap;

let udt_comparator = ComparatorType::Udt {
    type_name: "Person".to_string(),
    keyspace: None,
    field_comparators: vec![
        ("name".to_string(), ComparatorType::Text),
        ("age".to_string(), ComparatorType::Int),
    ],
};

let mut fields1 = HashMap::new();
fields1.insert("name".to_string(), Value::Text("Alice".to_string()));
fields1.insert("age".to_string(), Value::Integer(30));

let udt1 = Value::Udt(UdtValue::new(
    "Person".to_string(),
    None,
    vec![
        ("name".to_string(), CqlTypeSpec::Text),
        ("age".to_string(), CqlTypeSpec::Int),
    ],
    fields1,
));

let mut fields2 = HashMap::new();
fields2.insert("name".to_string(), Value::Text("Bob".to_string()));
fields2.insert("age".to_string(), Value::Integer(25));

let udt2 = Value::Udt(UdtValue::new(
    "Person".to_string(),
    None,
    vec![
        ("name".to_string(), CqlTypeSpec::Text),
        ("age".to_string(), CqlTypeSpec::Int),
    ],
    fields2,
));

// UDTs are compared field by field in definition order
assert_eq!(udt_comparator.compare(&udt1, &udt2)?, Ordering::Less); // Alice < Bob
```

## Type Compatibility

### Checking Compatibility

```rust
// Text and varchar are compatible
let text_comparator = ComparatorType::Text;
let varchar_comparator = ComparatorType::Varchar;

// You can check compatibility via SchemaRegistry or TableSchema
// (compatibility checking is built into those systems)
```

### Compatible Type Pairs

- `Text` ↔ `Varchar`
- `Text` ↔ `Ascii` (in some contexts)
- Collection types with compatible element types
- UDT types with same name and keyspace

## Integration Examples

### With TableSchema

```rust
use cqlite_core::schema::{TableSchema, Column};

let schema = TableSchema {
    keyspace: "myapp".to_string(),
    table: "users".to_string(),
    // ... other fields
    columns: vec![
        Column {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            nullable: false,
            default: None,
        },
        Column {
            name: "name".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
        },
    ],
    // ... other fields
};

// Get comparator for a specific column
let name_comparator = schema.get_column_comparator("name")?;
assert_eq!(name_comparator, ComparatorType::Text);

// Get all column comparators
let all_comparators = schema.get_all_comparators()?;

// Get partition key comparators
let pk_comparators = schema.get_partition_key_comparators()?;

// Check type compatibility
let is_compatible = schema.is_column_type_compatible("name", "varchar")?;
assert!(is_compatible);
```

### With SchemaRegistry

```rust
use cqlite_core::schema::registry::SchemaRegistry;

let registry = SchemaRegistry::new(config, platform, core_config).await?;

// Get comparator for a specific column
let comparator = registry.get_column_comparator("myapp", "users", "name").await?;

// Get all table comparators
let table_comparators = registry.get_table_comparators("myapp", "users").await?;

// Get partition key comparators
let pk_comparators = registry.get_partition_key_comparator("myapp", "users").await?;

// Get clustering key comparators
let ck_comparators = registry.get_clustering_key_comparator("myapp", "users").await?;

// Validate type compatibility
let is_compatible = registry.validate_column_type_compatibility(
    "myapp", "users", "name", "varchar"
).await?;
```

## Performance Considerations

### Ordering Support

Check if a comparator supports ordering before using ordering operations:

```rust
let comparator = ComparatorType::Set(Box::new(ComparatorType::Int));

if comparator.supports_ordering() {
    // Safe to use less_than, greater_than, etc.
    let result = comparator.compare(&val1, &val2)?;
} else {
    // Only equality comparison is supported
    let is_equal = comparator.equals(&val1, &val2)?;
}
```

### Large Collections

For large collections, consider the performance implications:

```rust
// This will compare up to min(len1, len2) elements
let large_list_comparator = ComparatorType::List(Box::new(ComparatorType::Text));

// For very large collections, consider using frozen types for
// byte-wise comparison instead of element-wise comparison
let frozen_list_comparator = ComparatorType::Frozen(
    Box::new(ComparatorType::List(Box::new(ComparatorType::Text)))
);
```

### Deeply Nested Types

ComparatorType handles deeply nested structures efficiently:

```rust
// This is fine - nested structures are handled recursively
let nested_comparator = ComparatorType::List(Box::new(
    ComparatorType::Map(
        Box::new(ComparatorType::Text),
        Box::new(ComparatorType::List(Box::new(ComparatorType::Int)))
    )
));
```

## Error Handling

### Common Error Cases

```rust
use cqlite_core::Error;

let comparator = ComparatorType::Int;

// Type mismatch error
let int_val = Value::Integer(42);
let text_val = Value::Text("hello".to_string());

match comparator.compare(&int_val, &text_val) {
    Ok(ordering) => {
        // This won't happen
    }
    Err(Error::Schema(msg)) => {
        // Type mismatch error
        println!("Type mismatch: {}", msg);
    }
    Err(other) => {
        // Other error types
        println!("Other error: {}", other);
    }
}
```

### Validation Best Practices

```rust
// Always validate types before comparison
fn safe_compare(
    comparator: &ComparatorType, 
    left: &Value, 
    right: &Value
) -> Result<Option<std::cmp::Ordering>, Error> {
    // Check if both values are compatible with the comparator
    // (this is implicit in the compare method, but you could add explicit checks)
    
    match comparator.compare(left, right) {
        Ok(ordering) => Ok(Some(ordering)),
        Err(Error::Schema(_)) => {
            // Type mismatch - values are not comparable
            Ok(None)
        }
        Err(other) => Err(other),
    }
}
```

## Advanced Usage

### Custom Comparison Logic

For custom types, you can extend the comparison logic:

```rust
// Custom comparison wrapper
struct CustomComparator {
    inner: ComparatorType,
    case_sensitive: bool,
}

impl CustomComparator {
    fn compare(&self, left: &Value, right: &Value) -> Result<std::cmp::Ordering, Error> {
        if !self.case_sensitive && matches!(self.inner, ComparatorType::Text) {
            // Custom case-insensitive text comparison
            if let (Value::Text(l), Value::Text(r)) = (left, right) {
                return Ok(l.to_lowercase().cmp(&r.to_lowercase()));
            }
        }
        
        // Fall back to standard comparison
        self.inner.compare(left, right)
    }
}
```

### Bulk Comparison Operations

```rust
fn sort_values(
    values: &mut [Value], 
    comparator: &ComparatorType
) -> Result<(), Error> {
    values.sort_by(|a, b| {
        comparator.compare(a, b)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(())
}

fn find_min_max(
    values: &[Value], 
    comparator: &ComparatorType
) -> Result<Option<(&Value, &Value)>, Error> {
    if values.is_empty() {
        return Ok(None);
    }
    
    let mut min = &values[0];
    let mut max = &values[0];
    
    for value in values.iter().skip(1) {
        if comparator.less_than(value, min)? {
            min = value;
        }
        if comparator.greater_than(value, max)? {
            max = value;
        }
    }
    
    Ok(Some((min, max)))
}
```

### Type Introspection

```rust
fn analyze_comparator(comparator: &ComparatorType) {
    println!("Type: {}", comparator.type_name());
    println!("Supports ordering: {}", comparator.supports_ordering());
    
    match comparator {
        ComparatorType::List(element_type) => {
            println!("List element type: {}", element_type.type_name());
        }
        ComparatorType::Map(key_type, value_type) => {
            println!("Map key type: {}", key_type.type_name());
            println!("Map value type: {}", value_type.type_name());
        }
        ComparatorType::Udt { type_name, keyspace, field_comparators } => {
            println!("UDT name: {}", type_name);
            if let Some(ks) = keyspace {
                println!("UDT keyspace: {}", ks);
            }
            println!("UDT fields: {}", field_comparators.len());
        }
        _ => {}
    }
}
```

## Best Practices

1. **Always check ordering support** before using ordering operations on collections and complex types
2. **Handle null values** properly - they are always considered less than non-null values
3. **Use frozen types** for large collections when you only need equality comparison
4. **Validate type compatibility** before performing comparisons
5. **Consider performance** for deeply nested or large data structures
6. **Use appropriate error handling** for type mismatches
7. **Prefer schema-based comparator creation** over manual construction for consistency

## Related Documentation

- [Schema Registry API](schema-registry-api.md)
- [Table Schema Guide](table-schema-guide.md)
- [CQL Type System](cql-type-system.md)
- [Value Types Reference](value-types-reference.md)