# CQLite Complex Types API Reference

## Overview

CQLite provides comprehensive support for Cassandra's complex data types, enabling you to work with collections, user-defined types, tuples, and frozen structures. This document provides complete API reference and practical examples for all complex types.

## Supported Complex Types

### Collection Types
- **List**: Ordered collection of elements
- **Set**: Unordered collection of unique elements  
- **Map**: Key-value pairs collection

### Structured Types
- **Tuple**: Fixed-size heterogeneous type container
- **UDT**: User Defined Type with named fields
- **Frozen**: Immutable wrapper for any complex type

## Collection Types

### Lists

Lists store ordered sequences of elements of the same type.

#### Creating Lists

```rust
use cqlite_core::Value;

// Empty list
let empty_list = Value::List(vec![]);

// Integer list
let numbers = Value::List(vec![
    Value::Integer(1),
    Value::Integer(2), 
    Value::Integer(3),
]);

// String list
let words = Value::List(vec![
    Value::Text("hello".to_string()),
    Value::Text("world".to_string()),
]);

// Mixed types (valid in CQLite)
let mixed = Value::List(vec![
    Value::Integer(42),
    Value::Text("answer".to_string()),
    Value::Boolean(true),
]);
```

#### CQL Schema Definition
```cql
CREATE TABLE user_events (
    user_id uuid PRIMARY KEY,
    event_tags list<text>,
    scores list<int>,
    timestamps list<timestamp>
);
```

#### Real-World Example: Event Tracking
```rust
use cqlite_core::{Value, Database};
use std::collections::HashMap;

async fn track_user_events() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./data").await?;
    
    // Insert user with event data
    let mut row = HashMap::new();
    row.insert("user_id".to_string(), Value::Uuid([1; 16]));
    
    // Track multiple event types
    row.insert("event_tags".to_string(), Value::List(vec![
        Value::Text("login".to_string()),
        Value::Text("purchase".to_string()),
        Value::Text("logout".to_string()),
    ]));
    
    // Track scores for each event
    row.insert("scores".to_string(), Value::List(vec![
        Value::Integer(100),  // login score
        Value::Integer(250),  // purchase score
        Value::Integer(50),   // logout score
    ]));
    
    // Track timestamps
    row.insert("timestamps".to_string(), Value::List(vec![
        Value::Timestamp(1640995200000000), // 2022-01-01 00:00:00
        Value::Timestamp(1640995800000000), // 2022-01-01 00:10:00
        Value::Timestamp(1640996400000000), // 2022-01-01 00:20:00
    ]));
    
    db.insert("user_events", row).await?;
    Ok(())
}
```

### Sets

Sets store unordered collections of unique elements.

#### Creating Sets

```rust
// String set - automatically deduplicates
let tags = Value::Set(vec![
    Value::Text("technology".to_string()),
    Value::Text("rust".to_string()),
    Value::Text("database".to_string()),
    Value::Text("rust".to_string()), // Duplicate - will be handled by application logic
]);

// Number set
let fibonacci = Value::Set(vec![
    Value::Integer(1),
    Value::Integer(1), // Duplicate
    Value::Integer(2),
    Value::Integer(3),
    Value::Integer(5),
    Value::Integer(8),
]);
```

#### Real-World Example: User Permissions
```rust
async fn manage_user_permissions() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./data").await?;
    
    let mut user_row = HashMap::new();
    user_row.insert("user_id".to_string(), Value::Uuid([2; 16]));
    
    // Set of permissions - no duplicates allowed
    user_row.insert("permissions".to_string(), Value::Set(vec![
        Value::Text("read".to_string()),
        Value::Text("write".to_string()),
        Value::Text("admin".to_string()),
        Value::Text("delete".to_string()),
    ]));
    
    // Set of accessible resources
    user_row.insert("accessible_tables".to_string(), Value::Set(vec![
        Value::Text("users".to_string()),
        Value::Text("orders".to_string()),
        Value::Text("products".to_string()),
    ]));
    
    db.insert("user_permissions", user_row).await?;
    Ok(())
}
```

### Maps

Maps store key-value pairs where both keys and values can be of different types.

#### Creating Maps

```rust
// Simple string-to-string map
let mut config = Vec::new();
config.push((
    Value::Text("timeout".to_string()),
    Value::Text("30s".to_string())
));
config.push((
    Value::Text("retry_count".to_string()), 
    Value::Text("3".to_string())
));
let config_map = Value::Map(config);

// Mixed type map
let mut metadata = Vec::new();
metadata.push((
    Value::Text("version".to_string()),
    Value::Integer(1)
));
metadata.push((
    Value::Text("active".to_string()),
    Value::Boolean(true)
));
metadata.push((
    Value::Text("last_updated".to_string()),
    Value::Timestamp(1640995200000000)
));
let metadata_map = Value::Map(metadata);
```

#### Real-World Example: Product Catalog
```rust
async fn manage_product_catalog() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./data").await?;
    
    let mut product_row = HashMap::new();
    product_row.insert("product_id".to_string(), Value::Uuid([3; 16]));
    product_row.insert("name".to_string(), Value::Text("Laptop".to_string()));
    
    // Product attributes as a map
    let mut attributes = Vec::new();
    attributes.push((
        Value::Text("brand".to_string()),
        Value::Text("TechCorp".to_string())
    ));
    attributes.push((
        Value::Text("screen_size".to_string()),
        Value::Text("15.6 inches".to_string())
    ));
    attributes.push((
        Value::Text("ram".to_string()),
        Value::Text("16GB".to_string())
    ));
    attributes.push((
        Value::Text("storage".to_string()),
        Value::Text("512GB SSD".to_string())
    ));
    
    product_row.insert("attributes".to_string(), Value::Map(attributes));
    
    // Pricing by region
    let mut regional_pricing = Vec::new();
    regional_pricing.push((
        Value::Text("US".to_string()),
        Value::Float(999.99)
    ));
    regional_pricing.push((
        Value::Text("EU".to_string()),
        Value::Float(899.99)
    ));
    regional_pricing.push((
        Value::Text("APAC".to_string()),
        Value::Float(1099.99)
    ));
    
    product_row.insert("regional_pricing".to_string(), Value::Map(regional_pricing));
    
    db.insert("products", product_row).await?;
    Ok(())
}
```

## Structured Types

### Tuples

Tuples store fixed-size sequences of heterogeneous types.

#### Creating Tuples

```rust
// Coordinate tuple (x, y)
let coordinate = Value::Tuple(vec![
    Value::Float(10.5),
    Value::Float(20.3),
]);

// Person info tuple (name, age, active)
let person_info = Value::Tuple(vec![
    Value::Text("John Doe".to_string()),
    Value::Integer(30),
    Value::Boolean(true),
]);

// Complex nested tuple
let complex_tuple = Value::Tuple(vec![
    Value::Integer(1),
    Value::Text("data".to_string()),
    Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]),
    Value::Boolean(false),
]);
```

#### Real-World Example: Geospatial Data
```rust
async fn store_location_data() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./data").await?;
    
    let mut location_row = HashMap::new();
    location_row.insert("location_id".to_string(), Value::Uuid([4; 16]));
    
    // GPS coordinates as tuple (latitude, longitude, altitude)
    location_row.insert("coordinates".to_string(), Value::Tuple(vec![
        Value::Float(37.7749),    // latitude
        Value::Float(-122.4194),  // longitude
        Value::Float(16.0),       // altitude in meters
    ]));
    
    // Address components as tuple (street, city, state, zip)
    location_row.insert("address".to_string(), Value::Tuple(vec![
        Value::Text("123 Main St".to_string()),
        Value::Text("San Francisco".to_string()),
        Value::Text("CA".to_string()),
        Value::Text("94102".to_string()),
    ]));
    
    // Bounding box as tuple (min_lat, min_lng, max_lat, max_lng)
    location_row.insert("bounding_box".to_string(), Value::Tuple(vec![
        Value::Float(37.7649),
        Value::Float(-122.4294),
        Value::Float(37.7849),
        Value::Float(-122.4094),
    ]));
    
    db.insert("locations", location_row).await?;
    Ok(())
}
```

### User Defined Types (UDTs)

UDTs allow you to create custom structured types with named fields.

#### Creating UDTs

```rust
use std::collections::HashMap;

// Create a Person UDT
let mut person_fields = HashMap::new();
person_fields.insert("name".to_string(), Value::Text("Alice Smith".to_string()));
person_fields.insert("age".to_string(), Value::Integer(28));
person_fields.insert("email".to_string(), Value::Text("alice@example.com".to_string()));
person_fields.insert("active".to_string(), Value::Boolean(true));

let person_udt = Value::Udt("Person".to_string(), person_fields);

// Create an Address UDT
let mut address_fields = HashMap::new();
address_fields.insert("street".to_string(), Value::Text("456 Oak Ave".to_string()));
address_fields.insert("city".to_string(), Value::Text("New York".to_string()));
address_fields.insert("state".to_string(), Value::Text("NY".to_string()));
address_fields.insert("zip_code".to_string(), Value::Text("10001".to_string()));
address_fields.insert("country".to_string(), Value::Text("USA".to_string()));

let address_udt = Value::Udt("Address".to_string(), address_fields);
```

#### Real-World Example: E-commerce System
```rust
async fn create_ecommerce_order() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./data").await?;
    
    // Customer UDT
    let mut customer_fields = HashMap::new();
    customer_fields.insert("id".to_string(), Value::Uuid([5; 16]));
    customer_fields.insert("name".to_string(), Value::Text("John Customer".to_string()));
    customer_fields.insert("email".to_string(), Value::Text("john@example.com".to_string()));
    customer_fields.insert("phone".to_string(), Value::Text("+1-555-0123".to_string()));
    customer_fields.insert("loyalty_tier".to_string(), Value::Text("Gold".to_string()));
    
    let customer = Value::Udt("Customer".to_string(), customer_fields);
    
    // Shipping Address UDT
    let mut shipping_fields = HashMap::new();
    shipping_fields.insert("name".to_string(), Value::Text("John Customer".to_string()));
    shipping_fields.insert("street1".to_string(), Value::Text("123 Main St".to_string()));
    shipping_fields.insert("street2".to_string(), Value::Text("Apt 4B".to_string()));
    shipping_fields.insert("city".to_string(), Value::Text("Portland".to_string()));
    shipping_fields.insert("state".to_string(), Value::Text("OR".to_string()));
    shipping_fields.insert("zip".to_string(), Value::Text("97201".to_string()));
    shipping_fields.insert("country".to_string(), Value::Text("USA".to_string()));
    
    let shipping_address = Value::Udt("Address".to_string(), shipping_fields);
    
    // Payment Method UDT
    let mut payment_fields = HashMap::new();
    payment_fields.insert("type".to_string(), Value::Text("credit_card".to_string()));
    payment_fields.insert("last_four".to_string(), Value::Text("1234".to_string()));
    payment_fields.insert("brand".to_string(), Value::Text("Visa".to_string()));
    payment_fields.insert("exp_month".to_string(), Value::Integer(12));
    payment_fields.insert("exp_year".to_string(), Value::Integer(2025));
    
    let payment_method = Value::Udt("PaymentMethod".to_string(), payment_fields);
    
    // Order Items UDT list
    let mut item1_fields = HashMap::new();
    item1_fields.insert("product_id".to_string(), Value::Uuid([6; 16]));
    item1_fields.insert("name".to_string(), Value::Text("Wireless Headphones".to_string()));
    item1_fields.insert("quantity".to_string(), Value::Integer(1));
    item1_fields.insert("unit_price".to_string(), Value::Float(199.99));
    item1_fields.insert("total_price".to_string(), Value::Float(199.99));
    
    let item1 = Value::Udt("OrderItem".to_string(), item1_fields);
    
    let mut item2_fields = HashMap::new();
    item2_fields.insert("product_id".to_string(), Value::Uuid([7; 16]));
    item2_fields.insert("name".to_string(), Value::Text("Phone Case".to_string()));
    item2_fields.insert("quantity".to_string(), Value::Integer(2));
    item2_fields.insert("unit_price".to_string(), Value::Float(29.99));
    item2_fields.insert("total_price".to_string(), Value::Float(59.98));
    
    let item2 = Value::Udt("OrderItem".to_string(), item2_fields);
    
    let order_items = Value::List(vec![item1, item2]);
    
    // Create the complete order
    let mut order_row = HashMap::new();
    order_row.insert("order_id".to_string(), Value::Uuid([8; 16]));
    order_row.insert("customer".to_string(), customer);
    order_row.insert("shipping_address".to_string(), shipping_address);
    order_row.insert("payment_method".to_string(), payment_method);
    order_row.insert("items".to_string(), order_items);
    order_row.insert("subtotal".to_string(), Value::Float(259.97));
    order_row.insert("tax".to_string(), Value::Float(20.80));
    order_row.insert("shipping".to_string(), Value::Float(9.99));
    order_row.insert("total".to_string(), Value::Float(290.76));
    order_row.insert("status".to_string(), Value::Text("confirmed".to_string()));
    order_row.insert("created_at".to_string(), Value::Timestamp(1640995200000000));
    
    db.insert("orders", order_row).await?;
    Ok(())
}
```

### Frozen Types

Frozen types make collections and UDTs immutable, allowing them to be used in primary keys and indexed.

#### Creating Frozen Types

```rust
// Frozen list
let numbers = Value::List(vec![
    Value::Integer(1),
    Value::Integer(2),
    Value::Integer(3),
]);
let frozen_list = Value::Frozen(Box::new(numbers));

// Frozen UDT
let mut person_fields = HashMap::new();
person_fields.insert("name".to_string(), Value::Text("Alice".to_string()));
person_fields.insert("age".to_string(), Value::Integer(30));

let person_udt = Value::Udt("Person".to_string(), person_fields);
let frozen_person = Value::Frozen(Box::new(person_udt));

// Frozen map
let mut config = Vec::new();
config.push((
    Value::Text("environment".to_string()),
    Value::Text("production".to_string())
));
config.push((
    Value::Text("version".to_string()),
    Value::Text("1.0.0".to_string())
));

let config_map = Value::Map(config);
let frozen_config = Value::Frozen(Box::new(config_map));
```

#### Real-World Example: Configuration Management
```rust
async fn store_immutable_config() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./data").await?;
    
    // Database configuration as frozen UDT
    let mut db_config_fields = HashMap::new();
    db_config_fields.insert("host".to_string(), Value::Text("db.example.com".to_string()));
    db_config_fields.insert("port".to_string(), Value::Integer(5432));
    db_config_fields.insert("database".to_string(), Value::Text("production".to_string()));
    db_config_fields.insert("ssl_mode".to_string(), Value::Text("require".to_string()));
    db_config_fields.insert("pool_size".to_string(), Value::Integer(20));
    
    let db_config = Value::Udt("DatabaseConfig".to_string(), db_config_fields);
    let frozen_db_config = Value::Frozen(Box::new(db_config));
    
    // Feature flags as frozen map
    let mut feature_flags = Vec::new();
    feature_flags.push((
        Value::Text("new_checkout".to_string()),
        Value::Boolean(true)
    ));
    feature_flags.push((
        Value::Text("analytics_v2".to_string()),
        Value::Boolean(false)
    ));
    feature_flags.push((
        Value::Text("mobile_app".to_string()),
        Value::Boolean(true)
    ));
    
    let flags_map = Value::Map(feature_flags);
    let frozen_flags = Value::Frozen(Box::new(flags_map));
    
    // Allowed API endpoints as frozen list
    let endpoints = Value::List(vec![
        Value::Text("/api/v1/users".to_string()),
        Value::Text("/api/v1/orders".to_string()),
        Value::Text("/api/v1/products".to_string()),
        Value::Text("/api/v1/health".to_string()),
    ]);
    let frozen_endpoints = Value::Frozen(Box::new(endpoints));
    
    let mut config_row = HashMap::new();
    config_row.insert("config_id".to_string(), Value::Text("production-v1.0".to_string()));
    config_row.insert("database_config".to_string(), frozen_db_config);
    config_row.insert("feature_flags".to_string(), frozen_flags);
    config_row.insert("allowed_endpoints".to_string(), frozen_endpoints);
    config_row.insert("created_at".to_string(), Value::Timestamp(1640995200000000));
    config_row.insert("immutable".to_string(), Value::Boolean(true));
    
    db.insert("system_configs", config_row).await?;
    Ok(())
}
```

## Type Serialization and Parsing

### Binary Format Compatibility

CQLite uses Cassandra-compatible binary format for all complex types:

```rust
use cqlite_core::parser::types::{serialize_cql_value, parse_cql_value, CqlTypeId};

async fn demonstrate_serialization() -> Result<(), Box<dyn std::error::Error>> {
    // Create a complex nested structure
    let mut nested_map = Vec::new();
    nested_map.push((
        Value::Text("items".to_string()),
        Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])
    ));
    nested_map.push((
        Value::Text("metadata".to_string()),
        Value::Tuple(vec![
            Value::Text("version".to_string()),
            Value::Integer(1),
            Value::Boolean(true),
        ])
    ));
    
    let complex_value = Value::Map(nested_map);
    
    // Serialize to Cassandra-compatible format
    let serialized = serialize_cql_value(&complex_value)?;
    println!("Serialized size: {} bytes", serialized.len());
    
    // Parse back from binary format
    if serialized.len() > 1 {
        let (remaining, parsed_value) = parse_cql_value(&serialized[1..], CqlTypeId::Map)?;
        println!("Remaining bytes: {}", remaining.len());
        println!("Parsed successfully: {:?}", parsed_value);
    }
    
    Ok(())
}
```

### Schema Definition Examples

#### CQL Schema for Complex Types

```cql
-- Collections
CREATE TABLE user_profiles (
    user_id uuid PRIMARY KEY,
    tags set<text>,
    preferences map<text, text>,
    recent_searches list<text>
);

-- UDTs
CREATE TYPE address (
    street text,
    city text,
    state text,
    zip_code text,
    country text
);

CREATE TYPE contact_info (
    email text,
    phone text,
    address frozen<address>
);

CREATE TABLE customers (
    customer_id uuid PRIMARY KEY,
    name text,
    contact frozen<contact_info>,
    billing_addresses list<frozen<address>>,
    preferences map<text, boolean>
);

-- Tuples
CREATE TABLE coordinates (
    location_id uuid PRIMARY KEY,
    gps_coordinates tuple<double, double, double>,
    address_components tuple<text, text, text, text>
);

-- Complex nested structures
CREATE TABLE analytics_events (
    event_id uuid PRIMARY KEY,
    user_data frozen<contact_info>,
    event_properties map<text, text>,
    numeric_data list<double>,
    categorical_data set<text>,
    coordinates tuple<double, double>
);
```

#### JSON Schema Representation

```json
{
  "keyspace": "ecommerce",
  "table": "orders",
  "partition_keys": [
    {"name": "order_id", "type": "uuid", "position": 0}
  ],
  "clustering_keys": [],
  "columns": [
    {"name": "order_id", "type": "uuid", "nullable": false},
    {"name": "customer", "type": "frozen<customer_info>", "nullable": false},
    {"name": "items", "type": "list<frozen<order_item>>", "nullable": false},
    {"name": "shipping_address", "type": "frozen<address>", "nullable": false},
    {"name": "metadata", "type": "map<text, text>", "nullable": true},
    {"name": "coordinates", "type": "tuple<double, double>", "nullable": true},
    {"name": "tags", "type": "set<text>", "nullable": true}
  ]
}
```

## Performance Characteristics

### Memory Usage

| Type | Storage Overhead | Access Pattern | Best Use Case |
|------|------------------|----------------|---------------|
| List | Low | Sequential | Ordered data, event logs |
| Set | Low | Hash-based | Unique values, tags |
| Map | Medium | Key-based | Configuration, metadata |
| Tuple | Low | Position-based | Fixed structure data |
| UDT | Medium | Field-based | Complex entities |
| Frozen | +10% | Immutable | Primary keys, indexes |

### Serialization Performance

```rust
use std::time::Instant;

async fn benchmark_complex_types() -> Result<(), Box<dyn std::error::Error>> {
    let iterations = 10_000;
    
    // Benchmark list serialization
    let large_list = Value::List((0..1000).map(|i| Value::Integer(i)).collect());
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = serialize_cql_value(&large_list)?;
    }
    let list_duration = start.elapsed();
    println!("List serialization: {:?} avg per operation", list_duration / iterations);
    
    // Benchmark map serialization
    let mut large_map = Vec::new();
    for i in 0..1000 {
        large_map.push((
            Value::Text(format!("key_{}", i)),
            Value::Integer(i)
        ));
    }
    let map_value = Value::Map(large_map);
    
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = serialize_cql_value(&map_value)?;
    }
    let map_duration = start.elapsed();
    println!("Map serialization: {:?} avg per operation", map_duration / iterations);
    
    // Benchmark UDT serialization
    let mut udt_fields = HashMap::new();
    for i in 0..100 {
        udt_fields.insert(format!("field_{}", i), Value::Integer(i));
    }
    let udt_value = Value::Udt("LargeUDT".to_string(), udt_fields);
    
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = serialize_cql_value(&udt_value)?;
    }
    let udt_duration = start.elapsed();
    println!("UDT serialization: {:?} avg per operation", udt_duration / iterations);
    
    Ok(())
}
```

## Best Practices

### 1. Choose the Right Collection Type

```rust
// ✅ Good: Use Set for unique values
let user_permissions = Value::Set(vec![
    Value::Text("read".to_string()),
    Value::Text("write".to_string()),
    Value::Text("admin".to_string()),
]);

// ❌ Avoid: Using List when uniqueness matters
let user_permissions = Value::List(vec![
    Value::Text("read".to_string()),
    Value::Text("read".to_string()), // Duplicate!
    Value::Text("write".to_string()),
]);

// ✅ Good: Use List for ordered data
let event_log = Value::List(vec![
    Value::Text("user_login".to_string()),
    Value::Text("page_view".to_string()),
    Value::Text("user_logout".to_string()),
]);

// ✅ Good: Use Map for key-value relationships
let mut config = Vec::new();
config.push((Value::Text("timeout".to_string()), Value::Integer(30)));
config.push((Value::Text("retries".to_string()), Value::Integer(3)));
let configuration = Value::Map(config);
```

### 2. Optimize UDT Structure

```rust
// ✅ Good: Logical field grouping
let mut user_fields = HashMap::new();
user_fields.insert("id".to_string(), Value::Uuid([1; 16]));
user_fields.insert("email".to_string(), Value::Text("user@example.com".to_string()));
user_fields.insert("created_at".to_string(), Value::Timestamp(1640995200000000));
user_fields.insert("active".to_string(), Value::Boolean(true));

// ❌ Avoid: Too many fields in single UDT
let mut massive_udt = HashMap::new();
for i in 0..100 {
    massive_udt.insert(format!("field_{}", i), Value::Integer(i));
}
```

### 3. Use Frozen Types Appropriately

```rust
// ✅ Good: Frozen for immutable data
let version_info = Value::Frozen(Box::new(Value::Tuple(vec![
    Value::Integer(1),
    Value::Integer(0),
    Value::Integer(0),
])));

// ✅ Good: Frozen UDT as primary key component
let mut address_fields = HashMap::new();
address_fields.insert("street".to_string(), Value::Text("123 Main St".to_string()));
address_fields.insert("city".to_string(), Value::Text("Portland".to_string()));
address_fields.insert("state".to_string(), Value::Text("OR".to_string()));

let address = Value::Udt("Address".to_string(), address_fields);
let frozen_address = Value::Frozen(Box::new(address));

// ❌ Avoid: Freezing mutable data
let mutable_list = Value::List(vec![]); // This will be modified
let frozen_mutable = Value::Frozen(Box::new(mutable_list)); // Not ideal
```

### 4. Handle Nested Complexity

```rust
// ✅ Good: Reasonable nesting depth
let order_summary = Value::Tuple(vec![
    Value::Text("order_123".to_string()),
    Value::Float(299.99),
    Value::List(vec![
        Value::Text("item1".to_string()),
        Value::Text("item2".to_string()),
    ]),
]);

// ❌ Avoid: Excessive nesting
let overly_nested = Value::List(vec![
    Value::Map(vec![(
        Value::Text("level1".to_string()),
        Value::List(vec![
            Value::Tuple(vec![
                Value::Map(vec![(
                    Value::Text("level4".to_string()),
                    Value::List(vec![/* ... */])
                )])
            ])
        ])
    )])
]);
```

## Error Handling

### Common Issues and Solutions

```rust
use cqlite_core::{Error, Result};

async fn handle_complex_type_errors() -> Result<()> {
    // Handle serialization errors
    let problematic_value = Value::Udt("BadType".to_string(), HashMap::new());
    
    match serialize_cql_value(&problematic_value) {
        Ok(serialized) => {
            println!("Serialized successfully: {} bytes", serialized.len());
        }
        Err(Error::Serialization(msg)) => {
            eprintln!("Serialization failed: {}", msg);
            // Handle gracefully - perhaps use default value
        }
        Err(e) => {
            eprintln!("Unexpected error: {:?}", e);
            return Err(e);
        }
    }
    
    // Handle parsing errors
    let invalid_data = vec![0xFF, 0xFF, 0xFF]; // Invalid format
    
    match parse_cql_value(&invalid_data, CqlTypeId::List) {
        Ok((_, value)) => {
            println!("Parsed value: {:?}", value);
        }
        Err(_) => {
            eprintln!("Failed to parse data - using default empty list");
            let default_list = Value::List(vec![]);
            // Continue with default value
        }
    }
    
    Ok(())
}

// Validation helpers
fn validate_udt_fields(udt: &Value) -> Result<()> {
    if let Value::Udt(type_name, fields) = udt {
        if type_name.is_empty() {
            return Err(Error::validation("UDT type name cannot be empty"));
        }
        
        if fields.is_empty() {
            return Err(Error::validation("UDT must have at least one field"));
        }
        
        // Validate each field
        for (field_name, field_value) in fields {
            if field_name.is_empty() {
                return Err(Error::validation("UDT field name cannot be empty"));
            }
            
            // Recursively validate nested complex types
            validate_complex_type(field_value)?;
        }
    }
    
    Ok(())
}

fn validate_complex_type(value: &Value) -> Result<()> {
    match value {
        Value::List(items) => {
            if items.len() > 65535 {
                return Err(Error::validation("List too large (max 65535 items)"));
            }
            for item in items {
                validate_complex_type(item)?;
            }
        }
        Value::Set(items) => {
            if items.len() > 65535 {
                return Err(Error::validation("Set too large (max 65535 items)"));
            }
            for item in items {
                validate_complex_type(item)?;
            }
        }
        Value::Map(pairs) => {
            if pairs.len() > 65535 {
                return Err(Error::validation("Map too large (max 65535 pairs)"));
            }
            for (key, value) in pairs {
                validate_complex_type(key)?;
                validate_complex_type(value)?;
            }
        }
        Value::Tuple(items) => {
            if items.len() > 32 {
                return Err(Error::validation("Tuple too large (max 32 items)"));
            }
            for item in items {
                validate_complex_type(item)?;
            }
        }
        Value::Udt(_, _) => {
            validate_udt_fields(value)?;
        }
        Value::Frozen(inner) => {
            validate_complex_type(inner)?;
        }
        _ => {} // Primitive types are always valid
    }
    
    Ok(())
}
```

## Migration and Compatibility

### Migrating from Cassandra

When migrating existing Cassandra data:

```rust
async fn migrate_cassandra_collections() -> Result<(), Box<dyn std::error::Error>> {
    // Cassandra SET<text> becomes CQLite Set
    let cassandra_set = "{'tag1', 'tag2', 'tag3'}";
    let cqlite_set = Value::Set(vec![
        Value::Text("tag1".to_string()),
        Value::Text("tag2".to_string()),
        Value::Text("tag3".to_string()),
    ]);
    
    // Cassandra LIST<int> becomes CQLite List
    let cassandra_list = "[1, 2, 3, 4, 5]";
    let cqlite_list = Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
        Value::Integer(4),
        Value::Integer(5),
    ]);
    
    // Cassandra MAP<text, int> becomes CQLite Map
    let cassandra_map = "{'key1': 100, 'key2': 200}";
    let mut cqlite_map = Vec::new();
    cqlite_map.push((Value::Text("key1".to_string()), Value::Integer(100)));
    cqlite_map.push((Value::Text("key2".to_string()), Value::Integer(200)));
    let cqlite_map_value = Value::Map(cqlite_map);
    
    println!("Migration successful for all collection types");
    Ok(())
}
```

### Version Compatibility

CQLite maintains compatibility with:
- Cassandra 3.x format (basic collections)
- Cassandra 4.x format (enhanced UDTs)
- Cassandra 5.x format (full complex types)

## Testing Complex Types

### Unit Testing

```rust
#[cfg(test)]
mod complex_type_tests {
    use super::*;
    
    #[test]
    fn test_list_roundtrip() {
        let original = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        
        let serialized = serialize_cql_value(&original).unwrap();
        let (_, parsed) = parse_cql_value(&serialized[1..], CqlTypeId::List).unwrap();
        
        assert_eq!(original, parsed);
    }
    
    #[test]
    fn test_udt_validation() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::Text("test".to_string()));
        fields.insert("value".to_string(), Value::Integer(42));
        
        let udt = Value::Udt("TestType".to_string(), fields);
        
        assert!(validate_udt_fields(&udt).is_ok());
    }
    
    #[test]
    fn test_frozen_immutability() {
        let list = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
        let frozen = Value::Frozen(Box::new(list));
        
        // Verify frozen type properties
        match &frozen {
            Value::Frozen(inner) => {
                assert!(matches!(**inner, Value::List(_)));
            }
            _ => panic!("Expected frozen type"),
        }
    }
}
```

## Conclusion

CQLite's complex type system provides full compatibility with Cassandra while offering ergonomic Rust APIs. Key takeaways:

1. **Choose the right collection type** for your use case
2. **Use UDTs** for structured data with named fields
3. **Apply frozen types** for immutable data and primary keys
4. **Validate data** before serialization to prevent errors
5. **Test thoroughly** with real-world data patterns

For production use, always benchmark your specific data patterns and monitor memory usage with large collections.