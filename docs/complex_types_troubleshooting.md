# CQLite Complex Types Troubleshooting Guide

## Overview

This guide helps you diagnose and resolve common issues when working with CQLite's complex data types. It covers error messages, performance problems, data corruption, and compatibility issues.

## Common Error Messages and Solutions

### Serialization Errors

#### Error: "Serialization failed: Invalid type mapping"

**Symptom:**
```rust
Error: Serialization failed: Invalid type mapping for value type: Custom("unknown_type")
```

**Cause:** Attempting to serialize a value with an unsupported or incorrectly mapped type.

**Solution:**
```rust
// ❌ Problematic code
let invalid_value = Value::Custom("unsupported_type".to_string());
let result = serialize_cql_value(&invalid_value); // Will fail

// ✅ Correct approach
let valid_value = match user_input {
    "text" => Value::Text("example".to_string()),
    "number" => Value::Integer(42),
    "boolean" => Value::Boolean(true),
    _ => {
        eprintln!("Unsupported type: {}", user_input);
        return Err("Invalid type".into());
    }
};

// Always validate before serialization
if let Err(e) = serialize_cql_value(&valid_value) {
    eprintln!("Serialization error: {}", e);
    // Handle gracefully
}
```

#### Error: "Buffer overflow during serialization"

**Symptom:**
```rust
Error: Buffer overflow during serialization: attempted to write beyond buffer capacity
```

**Cause:** Collection too large for serialization buffer.

**Solution:**
```rust
// ✅ Implement size limits
const MAX_COLLECTION_SIZE: usize = 10_000;

fn create_safe_list(items: Vec<Value>) -> Result<Value, String> {
    if items.len() > MAX_COLLECTION_SIZE {
        return Err(format!("List too large: {} items (max: {})", 
                          items.len(), MAX_COLLECTION_SIZE));
    }
    Ok(Value::List(items))
}

// ✅ Use pagination for large datasets
fn paginate_large_collection(all_items: Vec<Value>, page_size: usize) -> Vec<Value> {
    all_items.chunks(page_size)
        .map(|chunk| Value::List(chunk.to_vec()))
        .collect()
}

// Example usage
let large_dataset: Vec<Value> = (0..100_000).map(|i| Value::Integer(i)).collect();
let pages = paginate_large_collection(large_dataset, 1000);

for (i, page) in pages.iter().enumerate() {
    match serialize_cql_value(page) {
        Ok(serialized) => println!("Page {} serialized: {} bytes", i, serialized.len()),
        Err(e) => eprintln!("Failed to serialize page {}: {}", i, e),
    }
}
```

### Parsing Errors

#### Error: "Unexpected end of input during parsing"

**Symptom:**
```rust
Error: Parse error: Incomplete data - expected 4 bytes, got 2
```

**Cause:** Corrupted or truncated binary data.

**Solution:**
```rust
use cqlite_core::parser::types::{parse_cql_value, CqlTypeId};

fn safe_parse_with_validation(data: &[u8], expected_type: CqlTypeId) -> Result<Value, String> {
    // Validate minimum data length
    if data.is_empty() {
        return Err("Empty data provided".to_string());
    }
    
    // Check for reasonable data size
    if data.len() > 100_000_000 { // 100MB limit
        return Err("Data too large to parse safely".to_string());
    }
    
    // Attempt parsing with error handling
    match parse_cql_value(data, expected_type) {
        Ok((remaining, value)) => {
            if !remaining.is_empty() {
                eprintln!("Warning: {} bytes remaining after parsing", remaining.len());
            }
            Ok(value)
        }
        Err(e) => {
            Err(format!("Parse error: {:?}", e))
        }
    }
}

// Example usage with error recovery
async fn robust_data_loading(binary_data: &[u8]) -> Result<Value, Box<dyn std::error::Error>> {
    // Try parsing as different types if one fails
    let parse_attempts = vec![
        (CqlTypeId::List, "list"),
        (CqlTypeId::Map, "map"),
        (CqlTypeId::Varchar, "text"),
        (CqlTypeId::Blob, "blob"),
    ];
    
    for (type_id, type_name) in parse_attempts {
        match safe_parse_with_validation(binary_data, type_id) {
            Ok(value) => {
                println!("Successfully parsed as {}", type_name);
                return Ok(value);
            }
            Err(e) => {
                eprintln!("Failed to parse as {}: {}", type_name, e);
            }
        }
    }
    
    Err("Could not parse data as any known type".into())
}
```

#### Error: "Invalid VInt encoding"

**Symptom:**
```rust
Error: Invalid VInt encoding: length exceeds maximum allowed
```

**Cause:** Corrupted variable-length integer encoding.

**Solution:**
```rust
use cqlite_core::parser::vint::{parse_vint, encode_vint};

fn validate_and_parse_vint(data: &[u8]) -> Result<(i64, &[u8]), String> {
    // Check minimum length
    if data.is_empty() {
        return Err("Empty VInt data".to_string());
    }
    
    // Check for reasonable maximum length (VInts shouldn't exceed 9 bytes)
    if data.len() > 9 {
        eprintln!("Warning: VInt data longer than expected");
    }
    
    // Validate first byte for proper encoding
    let first_byte = data[0];
    let length_bits = first_byte.leading_zeros() as usize;
    
    if length_bits > 8 {
        return Err("Invalid VInt first byte".to_string());
    }
    
    // Attempt parsing
    match parse_vint(data) {
        Ok((remaining, value)) => {
            // Validate the parsed value is reasonable
            if value.abs() > i64::MAX / 2 {
                eprintln!("Warning: VInt value seems unusually large: {}", value);
            }
            Ok((value, remaining))
        }
        Err(e) => Err(format!("VInt parse error: {:?}", e))
    }
}

// Recovery strategy for corrupted VInts
fn recover_from_vint_corruption(data: &[u8]) -> Vec<Value> {
    let mut recovered_values = Vec::new();
    let mut offset = 0;
    
    while offset < data.len() {
        match validate_and_parse_vint(&data[offset..]) {
            Ok((value, remaining)) => {
                recovered_values.push(Value::BigInt(value));
                offset = data.len() - remaining.len();
            }
            Err(_) => {
                // Skip corrupted byte and try next position
                offset += 1;
            }
        }
    }
    
    recovered_values
}
```

### UDT (User Defined Type) Issues

#### Error: "UDT field not found"

**Symptom:**
```rust
Error: UDT field 'expected_field' not found in type definition
```

**Cause:** Mismatch between expected and actual UDT structure.

**Solution:**
```rust
use std::collections::HashMap;

// ✅ Defensive UDT field access
fn safe_get_udt_field(udt: &Value, field_name: &str) -> Option<&Value> {
    if let Value::Udt(_, fields) = udt {
        fields.get(field_name)
    } else {
        None
    }
}

// ✅ UDT with default values
fn get_udt_field_with_default(udt: &Value, field_name: &str, default: Value) -> Value {
    safe_get_udt_field(udt, field_name)
        .cloned()
        .unwrap_or(default)
}

// ✅ UDT schema validation
fn validate_udt_schema(udt: &Value, expected_fields: &[&str]) -> Result<(), String> {
    if let Value::Udt(type_name, fields) = udt {
        // Check for missing required fields
        for &field in expected_fields {
            if !fields.contains_key(field) {
                return Err(format!("Missing required field '{}' in UDT '{}'", field, type_name));
            }
        }
        
        // Check for unexpected fields (warning only)
        for field_name in fields.keys() {
            if !expected_fields.contains(&field_name.as_str()) {
                eprintln!("Warning: Unexpected field '{}' in UDT '{}'", field_name, type_name);
            }
        }
        
        Ok(())
    } else {
        Err("Value is not a UDT".to_string())
    }
}

// Example usage
async fn process_user_udt_safely(user_udt: Value) -> Result<(), Box<dyn std::error::Error>> {
    // Validate expected schema
    let required_fields = vec!["id", "name", "email"];
    validate_udt_schema(&user_udt, &required_fields)?;
    
    // Safe field access with defaults
    let user_id = get_udt_field_with_default(&user_udt, "id", Value::Text("unknown".to_string()));
    let user_name = get_udt_field_with_default(&user_udt, "name", Value::Text("Anonymous".to_string()));
    let user_email = get_udt_field_with_default(&user_udt, "email", Value::Text("noemail@example.com".to_string()));
    
    // Optional fields with None handling
    let phone = safe_get_udt_field(&user_udt, "phone");
    
    println!("Processing user: ID={:?}, Name={:?}, Email={:?}", user_id, user_name, user_email);
    
    if let Some(phone_value) = phone {
        println!("Phone: {:?}", phone_value);
    } else {
        println!("No phone number provided");
    }
    
    Ok(())
}
```

## Performance Issues

### Slow Serialization

**Symptom:** Serialization taking longer than expected.

**Diagnosis:**
```rust
use std::time::Instant;

async fn diagnose_serialization_performance(value: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    
    // Measure serialization time
    let serialized = serialize_cql_value(value)?;
    let serialization_time = start.elapsed();
    
    // Analyze the data structure
    let (depth, complexity) = analyze_value_complexity(value);
    
    println!("Serialization Performance Analysis:");
    println!("  Time: {:?}", serialization_time);
    println!("  Serialized size: {} bytes", serialized.len());
    println!("  Structure depth: {}", depth);
    println!("  Complexity score: {}", complexity);
    
    // Performance warnings
    if serialization_time.as_millis() > 100 {
        println!("⚠️  WARNING: Slow serialization (>100ms)");
        suggest_performance_improvements(value);
    }
    
    if serialized.len() > 1_000_000 {
        println!("⚠️  WARNING: Large serialized size (>1MB)");
        println!("   Consider using compression or pagination");
    }
    
    Ok(())
}

fn analyze_value_complexity(value: &Value) -> (usize, usize) {
    analyze_value_complexity_recursive(value, 0)
}

fn analyze_value_complexity_recursive(value: &Value, current_depth: usize) -> (usize, usize) {
    match value {
        Value::List(items) => {
            let mut max_depth = current_depth + 1;
            let mut total_complexity = items.len();
            
            for item in items {
                let (item_depth, item_complexity) = analyze_value_complexity_recursive(item, current_depth + 1);
                max_depth = max_depth.max(item_depth);
                total_complexity += item_complexity;
            }
            
            (max_depth, total_complexity)
        }
        Value::Map(pairs) => {
            let mut max_depth = current_depth + 1;
            let mut total_complexity = pairs.len() * 2; // Key + value
            
            for (key, value) in pairs {
                let (key_depth, key_complexity) = analyze_value_complexity_recursive(key, current_depth + 1);
                let (val_depth, val_complexity) = analyze_value_complexity_recursive(value, current_depth + 1);
                
                max_depth = max_depth.max(key_depth).max(val_depth);
                total_complexity += key_complexity + val_complexity;
            }
            
            (max_depth, total_complexity)
        }
        Value::Udt(_, fields) => {
            let mut max_depth = current_depth + 1;
            let mut total_complexity = fields.len();
            
            for (_, field_value) in fields {
                let (field_depth, field_complexity) = analyze_value_complexity_recursive(field_value, current_depth + 1);
                max_depth = max_depth.max(field_depth);
                total_complexity += field_complexity;
            }
            
            (max_depth, total_complexity)
        }
        Value::Tuple(items) => {
            let mut max_depth = current_depth + 1;
            let mut total_complexity = items.len();
            
            for item in items {
                let (item_depth, item_complexity) = analyze_value_complexity_recursive(item, current_depth + 1);
                max_depth = max_depth.max(item_depth);
                total_complexity += item_complexity;
            }
            
            (max_depth, total_complexity)
        }
        Value::Frozen(inner) => {
            analyze_value_complexity_recursive(inner, current_depth)
        }
        _ => (current_depth, 1), // Primitive types
    }
}

fn suggest_performance_improvements(value: &Value) {
    println!("\n💡 Performance Improvement Suggestions:");
    
    match value {
        Value::List(items) if items.len() > 10_000 => {
            println!("  • Consider paginating large lists (current size: {})", items.len());
            println!("  • Use streaming for processing large collections");
        }
        Value::Map(pairs) if pairs.len() > 5_000 => {
            println!("  • Consider splitting large maps into smaller chunks (current size: {})", pairs.len());
            println!("  • Use nested UDTs instead of flat maps where appropriate");
        }
        Value::Udt(_, fields) if fields.len() > 50 => {
            println!("  • Consider breaking down large UDTs into smaller, related types");
            println!("  • Use composition instead of single large UDT (current fields: {})", fields.len());
        }
        _ => {
            println!("  • Use frozen types for immutable data");
            println!("  • Consider caching serialized results for frequently accessed data");
            println!("  • Profile individual collection elements for nested complexity");
        }
    }
}
```

### Memory Usage Issues

**Symptom:** High memory consumption or out-of-memory errors.

**Diagnosis and Solution:**
```rust
use std::mem;

// Memory usage analyzer
fn analyze_memory_usage(value: &Value) -> usize {
    match value {
        Value::Null => mem::size_of::<Value>(),
        Value::Boolean(_) => mem::size_of::<Value>(),
        Value::Integer(_) => mem::size_of::<Value>(),
        Value::BigInt(_) => mem::size_of::<Value>(),
        Value::Float(_) => mem::size_of::<Value>(),
        Value::Text(s) => mem::size_of::<Value>() + s.capacity(),
        Value::Blob(b) => mem::size_of::<Value>() + b.capacity(),
        Value::Timestamp(_) => mem::size_of::<Value>(),
        Value::Uuid(_) => mem::size_of::<Value>(),
        Value::Json(j) => mem::size_of::<Value>() + estimate_json_size(j),
        Value::List(items) => {
            mem::size_of::<Value>() + mem::size_of::<Vec<Value>>() + 
            items.iter().map(analyze_memory_usage).sum::<usize>()
        }
        Value::Set(items) => {
            mem::size_of::<Value>() + mem::size_of::<Vec<Value>>() +
            items.iter().map(analyze_memory_usage).sum::<usize>()
        }
        Value::Map(pairs) => {
            mem::size_of::<Value>() + mem::size_of::<Vec<(Value, Value)>>() +
            pairs.iter().map(|(k, v)| analyze_memory_usage(k) + analyze_memory_usage(v)).sum::<usize>()
        }
        Value::Tuple(items) => {
            mem::size_of::<Value>() + mem::size_of::<Vec<Value>>() +
            items.iter().map(analyze_memory_usage).sum::<usize>()
        }
        Value::Udt(type_name, fields) => {
            mem::size_of::<Value>() + type_name.capacity() +
            mem::size_of::<HashMap<String, Value>>() +
            fields.iter().map(|(k, v)| k.capacity() + analyze_memory_usage(v)).sum::<usize>()
        }
        Value::Frozen(inner) => {
            mem::size_of::<Value>() + mem::size_of::<Box<Value>>() + analyze_memory_usage(inner)
        }
        _ => mem::size_of::<Value>(),
    }
}

fn estimate_json_size(json: &serde_json::Value) -> usize {
    match json {
        serde_json::Value::String(s) => s.capacity(),
        serde_json::Value::Array(arr) => arr.iter().map(estimate_json_size).sum(),
        serde_json::Value::Object(obj) => {
            obj.iter().map(|(k, v)| k.capacity() + estimate_json_size(v)).sum()
        }
        _ => 64, // Estimate for other types
    }
}

// Memory optimization strategies
async fn optimize_memory_usage(value: Value) -> Result<Value, Box<dyn std::error::Error>> {
    let initial_size = analyze_memory_usage(&value);
    println!("Initial memory usage: {} bytes", initial_size);
    
    let optimized = match value {
        Value::List(items) if items.len() > 10_000 => {
            println!("Optimizing large list...");
            // Split into smaller chunks
            optimize_large_list(items)
        }
        Value::Map(pairs) if pairs.len() > 5_000 => {
            println!("Optimizing large map...");
            // Convert to more efficient structure
            optimize_large_map(pairs)
        }
        Value::Udt(name, fields) if fields.len() > 100 => {
            println!("Optimizing large UDT...");
            // Split into smaller UDTs
            optimize_large_udt(name, fields)
        }
        other => other, // No optimization needed
    };
    
    let final_size = analyze_memory_usage(&optimized);
    println!("Optimized memory usage: {} bytes ({:.1}% reduction)", 
             final_size, 
             100.0 * (initial_size - final_size) as f64 / initial_size as f64);
    
    Ok(optimized)
}

fn optimize_large_list(items: Vec<Value>) -> Value {
    // Split into chunks and use nested structure
    const CHUNK_SIZE: usize = 1000;
    let chunks: Vec<Value> = items.chunks(CHUNK_SIZE)
        .map(|chunk| Value::List(chunk.to_vec()))
        .collect();
    
    Value::List(chunks)
}

fn optimize_large_map(pairs: Vec<(Value, Value)>) -> Value {
    // Group by key prefix or type
    let mut grouped = std::collections::HashMap::new();
    
    for (key, value) in pairs {
        let group_key = match &key {
            Value::Text(s) if s.len() > 0 => {
                // Group by first character
                s.chars().next().unwrap_or('_').to_string()
            }
            _ => "other".to_string(),
        };
        
        grouped.entry(group_key).or_insert_with(Vec::new).push((key, value));
    }
    
    // Convert groups to nested maps
    let nested_maps: Vec<(Value, Value)> = grouped.into_iter()
        .map(|(group_key, group_pairs)| {
            (Value::Text(group_key), Value::Map(group_pairs))
        })
        .collect();
    
    Value::Map(nested_maps)
}

fn optimize_large_udt(name: String, fields: HashMap<String, Value>) -> Value {
    // Split fields into logical groups
    let mut basic_fields = HashMap::new();
    let mut advanced_fields = HashMap::new();
    let mut metadata_fields = HashMap::new();
    
    for (field_name, field_value) in fields {
        if field_name.starts_with("meta_") || field_name.contains("_metadata") {
            metadata_fields.insert(field_name, field_value);
        } else if field_name.starts_with("adv_") || field_name.contains("_advanced") {
            advanced_fields.insert(field_name, field_value);
        } else {
            basic_fields.insert(field_name, field_value);
        }
    }
    
    // Create nested UDT structure
    let mut optimized_fields = basic_fields;
    
    if !advanced_fields.is_empty() {
        optimized_fields.insert(
            "advanced".to_string(), 
            Value::Udt(format!("{}Advanced", name), advanced_fields)
        );
    }
    
    if !metadata_fields.is_empty() {
        optimized_fields.insert(
            "metadata".to_string(),
            Value::Udt(format!("{}Metadata", name), metadata_fields)
        );
    }
    
    Value::Udt(name, optimized_fields)
}
```

## Data Corruption Issues

### Detecting Corruption

```rust
// Data integrity checker
async fn check_data_integrity(value: &Value) -> Result<(), String> {
    check_value_integrity(value, 0)
}

fn check_value_integrity(value: &Value, depth: usize) -> Result<(), String> {
    const MAX_DEPTH: usize = 100;
    
    if depth > MAX_DEPTH {
        return Err(format!("Structure too deep: {} levels", depth));
    }
    
    match value {
        Value::Text(s) => {
            if !s.is_empty() && !s.chars().all(|c| c.is_ascii() || c.is_alphanumeric() || c.is_whitespace()) {
                eprintln!("Warning: Text contains unusual characters: {:?}", s);
            }
        }
        Value::List(items) => {
            if items.len() > 100_000 {
                return Err(format!("List too large: {} items", items.len()));
            }
            for (i, item) in items.iter().enumerate() {
                check_value_integrity(item, depth + 1)
                    .map_err(|e| format!("List item {}: {}", i, e))?;
            }
        }
        Value::Map(pairs) => {
            if pairs.len() > 50_000 {
                return Err(format!("Map too large: {} pairs", pairs.len()));
            }
            
            let mut seen_keys = std::collections::HashSet::new();
            for (i, (key, value)) in pairs.iter().enumerate() {
                // Check for duplicate keys
                let key_str = format!("{:?}", key);
                if !seen_keys.insert(key_str.clone()) {
                    eprintln!("Warning: Duplicate key detected: {}", key_str);
                }
                
                check_value_integrity(key, depth + 1)
                    .map_err(|e| format!("Map key {}: {}", i, e))?;
                check_value_integrity(value, depth + 1)
                    .map_err(|e| format!("Map value {}: {}", i, e))?;
            }
        }
        Value::Udt(type_name, fields) => {
            if type_name.is_empty() {
                return Err("UDT has empty type name".to_string());
            }
            if fields.len() > 200 {
                return Err(format!("UDT has too many fields: {}", fields.len()));
            }
            
            for (field_name, field_value) in fields {
                if field_name.is_empty() {
                    return Err("UDT has empty field name".to_string());
                }
                check_value_integrity(field_value, depth + 1)
                    .map_err(|e| format!("UDT field '{}': {}", field_name, e))?;
            }
        }
        Value::Tuple(items) => {
            if items.len() > 32 {
                return Err(format!("Tuple too large: {} items", items.len()));
            }
            for (i, item) in items.iter().enumerate() {
                check_value_integrity(item, depth + 1)
                    .map_err(|e| format!("Tuple item {}: {}", i, e))?;
            }
        }
        Value::Frozen(inner) => {
            check_value_integrity(inner, depth + 1)?;
        }
        _ => {} // Primitive types are generally safe
    }
    
    Ok(())
}

// Recovery from corruption
async fn recover_from_corruption(corrupted_data: &[u8]) -> Result<Value, Box<dyn std::error::Error>> {
    println!("Attempting data recovery...");
    
    // Try different recovery strategies
    
    // Strategy 1: Try to parse as different types
    let recovery_types = vec![
        (CqlTypeId::Blob, "blob"),
        (CqlTypeId::Varchar, "text"),
        (CqlTypeId::List, "list"),
        (CqlTypeId::Map, "map"),
    ];
    
    for (type_id, type_name) in recovery_types {
        if let Ok((_, recovered)) = parse_cql_value(corrupted_data, type_id) {
            println!("Successfully recovered as {}", type_name);
            return Ok(recovered);
        }
    }
    
    // Strategy 2: Try to recover partial data
    if let Some(partial) = try_partial_recovery(corrupted_data) {
        println!("Recovered partial data");
        return Ok(partial);
    }
    
    // Strategy 3: Create a blob with the raw data for manual inspection
    println!("Creating blob with raw data for manual recovery");
    Ok(Value::Blob(corrupted_data.to_vec()))
}

fn try_partial_recovery(data: &[u8]) -> Option<Value> {
    // Try to extract any valid strings
    let mut recovered_strings = Vec::new();
    let mut current_string = Vec::new();
    
    for &byte in data {
        if byte.is_ascii() && !byte.is_ascii_control() {
            current_string.push(byte);
        } else {
            if current_string.len() > 3 { // Only keep strings longer than 3 chars
                if let Ok(s) = String::from_utf8(current_string.clone()) {
                    recovered_strings.push(Value::Text(s));
                }
            }
            current_string.clear();
        }
    }
    
    // Return the last valid string
    if let Ok(s) = String::from_utf8(current_string) {
        if s.len() > 3 {
            recovered_strings.push(Value::Text(s));
        }
    }
    
    if !recovered_strings.is_empty() {
        Some(Value::List(recovered_strings))
    } else {
        None
    }
}
```

## Compatibility Issues

### Version Compatibility

```rust
// Version compatibility checker
#[derive(Debug, PartialEq)]
enum CassandraVersion {
    V3x,
    V4x,
    V5x,
}

async fn check_cassandra_compatibility(
    value: &Value, 
    target_version: CassandraVersion
) -> Result<(), String> {
    match target_version {
        CassandraVersion::V3x => check_v3_compatibility(value),
        CassandraVersion::V4x => check_v4_compatibility(value),
        CassandraVersion::V5x => check_v5_compatibility(value),
    }
}

fn check_v3_compatibility(value: &Value) -> Result<(), String> {
    match value {
        Value::Json(_) => Err("JSON type not supported in Cassandra 3.x".to_string()),
        Value::Duration(_) => Err("Duration type not supported in Cassandra 3.x".to_string()),
        Value::List(items) => {
            for item in items {
                check_v3_compatibility(item)?;
            }
            Ok(())
        }
        Value::Map(pairs) => {
            for (key, value) in pairs {
                check_v3_compatibility(key)?;
                check_v3_compatibility(value)?;
            }
            Ok(())
        }
        Value::Udt(_, fields) => {
            if fields.len() > 50 {
                eprintln!("Warning: Large UDTs may have performance issues in Cassandra 3.x");
            }
            for (_, field_value) in fields {
                check_v3_compatibility(field_value)?;
            }
            Ok(())
        }
        Value::Frozen(inner) => check_v3_compatibility(inner),
        _ => Ok(()), // Other types are compatible
    }
}

fn check_v4_compatibility(value: &Value) -> Result<(), String> {
    // Cassandra 4.x has better support, fewer restrictions
    match value {
        Value::List(items) if items.len() > 65535 => {
            Err("Collections larger than 65535 items may cause issues".to_string())
        }
        Value::Map(pairs) if pairs.len() > 65535 => {
            Err("Maps larger than 65535 pairs may cause issues".to_string())
        }
        _ => Ok(()), // Most types are compatible
    }
}

fn check_v5_compatibility(_value: &Value) -> Result<(), String> {
    // Cassandra 5.x has full support for all CQLite features
    Ok(())
}

// Migration helper
async fn migrate_for_compatibility(
    value: Value,
    target_version: CassandraVersion
) -> Result<Value, String> {
    match target_version {
        CassandraVersion::V3x => migrate_to_v3(value),
        CassandraVersion::V4x => migrate_to_v4(value),
        CassandraVersion::V5x => Ok(value), // No migration needed
    }
}

fn migrate_to_v3(value: Value) -> Result<Value, String> {
    match value {
        Value::Json(json) => {
            // Convert JSON to text for v3 compatibility
            Ok(Value::Text(json.to_string()))
        }
        Value::Duration(duration) => {
            // Convert duration to bigint (microseconds)
            Ok(Value::BigInt(duration))
        }
        Value::List(items) => {
            let migrated_items: Result<Vec<_>, _> = items.into_iter()
                .map(migrate_to_v3)
                .collect();
            Ok(Value::List(migrated_items?))
        }
        Value::Map(pairs) => {
            let migrated_pairs: Result<Vec<_>, _> = pairs.into_iter()
                .map(|(k, v)| Ok((migrate_to_v3(k)?, migrate_to_v3(v)?)))
                .collect();
            Ok(Value::Map(migrated_pairs?))
        }
        Value::Udt(name, fields) => {
            let migrated_fields: Result<HashMap<_, _>, _> = fields.into_iter()
                .map(|(k, v)| Ok((k, migrate_to_v3(v)?)))
                .collect();
            Ok(Value::Udt(name, migrated_fields?))
        }
        Value::Frozen(inner) => {
            Ok(Value::Frozen(Box::new(migrate_to_v3(*inner)?)))
        }
        other => Ok(other), // Already compatible
    }
}

fn migrate_to_v4(value: Value) -> Result<Value, String> {
    // Cassandra 4.x has better compatibility, mainly size limitations
    match value {
        Value::List(items) if items.len() > 32000 => {
            // Split large lists into smaller chunks
            let chunks: Vec<Value> = items.chunks(32000)
                .map(|chunk| Value::List(chunk.to_vec()))
                .collect();
            Ok(Value::List(chunks))
        }
        Value::Map(pairs) if pairs.len() > 32000 => {
            // Split large maps
            let chunks: Vec<(Value, Value)> = pairs.chunks(16000)
                .enumerate()
                .map(|(i, chunk)| {
                    (Value::Text(format!("chunk_{}", i)), Value::Map(chunk.to_vec()))
                })
                .collect();
            Ok(Value::Map(chunks))
        }
        other => Ok(other),
    }
}
```

## Debugging Tools

### Value Inspector

```rust
// Comprehensive value inspection tool
async fn inspect_value(value: &Value, name: &str) {
    println!("\n🔍 Inspecting Value: {}", name);
    println!("═══════════════════════════════════════");
    
    print_value_info(value, 0);
    
    // Check for potential issues
    let issues = find_potential_issues(value);
    if !issues.is_empty() {
        println!("\n⚠️  Potential Issues:");
        for issue in issues {
            println!("  • {}", issue);
        }
    }
    
    // Memory analysis
    let memory_usage = analyze_memory_usage(value);
    println!("\n📊 Memory Usage: {} bytes", memory_usage);
    
    // Serialization test
    match serialize_cql_value(value) {
        Ok(serialized) => {
            println!("✅ Serialization: OK ({} bytes)", serialized.len());
        }
        Err(e) => {
            println!("❌ Serialization: FAILED - {}", e);
        }
    }
}

fn print_value_info(value: &Value, indent: usize) {
    let indent_str = "  ".repeat(indent);
    
    match value {
        Value::Null => println!("{}NULL", indent_str),
        Value::Boolean(b) => println!("{}Boolean: {}", indent_str, b),
        Value::Integer(i) => println!("{}Integer: {}", indent_str, i),
        Value::BigInt(i) => println!("{}BigInt: {}", indent_str, i),
        Value::Float(f) => println!("{}Float: {}", indent_str, f),
        Value::Text(s) => println!("{}Text: '{}' (len: {})", indent_str, s, s.len()),
        Value::Blob(b) => println!("{}Blob: {} bytes", indent_str, b.len()),
        Value::Timestamp(ts) => println!("{}Timestamp: {}", indent_str, ts),
        Value::Uuid(uuid) => println!("{}UUID: {:02x?}", indent_str, uuid),
        Value::Json(json) => println!("{}JSON: {}", indent_str, json),
        Value::List(items) => {
            println!("{}List: {} items", indent_str, items.len());
            for (i, item) in items.iter().enumerate().take(5) {
                println!("{}[{}]:", indent_str, i);
                print_value_info(item, indent + 1);
            }
            if items.len() > 5 {
                println!("{}... and {} more items", indent_str, items.len() - 5);
            }
        }
        Value::Set(items) => {
            println!("{}Set: {} items", indent_str, items.len());
            for (i, item) in items.iter().enumerate().take(5) {
                println!("{}{{{}}}:", indent_str, i);
                print_value_info(item, indent + 1);
            }
            if items.len() > 5 {
                println!("{}... and {} more items", indent_str, items.len() - 5);
            }
        }
        Value::Map(pairs) => {
            println!("{}Map: {} pairs", indent_str, pairs.len());
            for (i, (key, value)) in pairs.iter().enumerate().take(5) {
                println!("{}Key {}:", indent_str, i);
                print_value_info(key, indent + 1);
                println!("{}Value {}:", indent_str, i);
                print_value_info(value, indent + 1);
            }
            if pairs.len() > 5 {
                println!("{}... and {} more pairs", indent_str, pairs.len() - 5);
            }
        }
        Value::Tuple(items) => {
            println!("{}Tuple: {} items", indent_str, items.len());
            for (i, item) in items.iter().enumerate() {
                println!("{}[{}]:", indent_str, i);
                print_value_info(item, indent + 1);
            }
        }
        Value::Udt(type_name, fields) => {
            println!("{}UDT '{}': {} fields", indent_str, type_name, fields.len());
            for (field_name, field_value) in fields.iter().take(10) {
                println!("{}{}:", indent_str, field_name);
                print_value_info(field_value, indent + 1);
            }
            if fields.len() > 10 {
                println!("{}... and {} more fields", indent_str, fields.len() - 10);
            }
        }
        Value::Frozen(inner) => {
            println!("{}Frozen:", indent_str);
            print_value_info(inner, indent + 1);
        }
        _ => println!("{}Other: {:?}", indent_str, value),
    }
}

fn find_potential_issues(value: &Value) -> Vec<String> {
    let mut issues = Vec::new();
    find_issues_recursive(value, &mut issues, 0);
    issues
}

fn find_issues_recursive(value: &Value, issues: &mut Vec<String>, depth: usize) {
    if depth > 20 {
        issues.push(format!("Deep nesting detected (depth: {})", depth));
        return;
    }
    
    match value {
        Value::Text(s) if s.len() > 1_000_000 => {
            issues.push(format!("Very large text field ({} chars)", s.len()));
        }
        Value::Blob(b) if b.len() > 10_000_000 => {
            issues.push(format!("Very large blob ({} bytes)", b.len()));
        }
        Value::List(items) if items.len() > 50_000 => {
            issues.push(format!("Very large list ({} items)", items.len()));
        }
        Value::Map(pairs) if pairs.len() > 25_000 => {
            issues.push(format!("Very large map ({} pairs)", pairs.len()));
        }
        Value::Udt(_, fields) if fields.len() > 100 => {
            issues.push(format!("UDT with many fields ({} fields)", fields.len()));
        }
        Value::List(items) | Value::Set(items) => {
            for item in items {
                find_issues_recursive(item, issues, depth + 1);
            }
        }
        Value::Map(pairs) => {
            for (key, value) in pairs {
                find_issues_recursive(key, issues, depth + 1);
                find_issues_recursive(value, issues, depth + 1);
            }
        }
        Value::Udt(_, fields) => {
            for (_, field_value) in fields {
                find_issues_recursive(field_value, issues, depth + 1);
            }
        }
        Value::Tuple(items) => {
            for item in items {
                find_issues_recursive(item, issues, depth + 1);
            }
        }
        Value::Frozen(inner) => {
            find_issues_recursive(inner, issues, depth);
        }
        _ => {}
    }
}

// Usage example
async fn debug_complex_structure() -> Result<(), Box<dyn std::error::Error>> {
    // Create a potentially problematic structure
    let mut large_map = Vec::new();
    for i in 0..1000 {
        let mut nested_udt = std::collections::HashMap::new();
        for j in 0..50 {
            nested_udt.insert(format!("field_{}", j), Value::Text(format!("Value {}_{}", i, j)));
        }
        large_map.push((
            Value::Text(format!("key_{}", i)),
            Value::Udt("NestedType".to_string(), nested_udt)
        ));
    }
    
    let complex_value = Value::Map(large_map);
    
    // Inspect the structure
    inspect_value(&complex_value, "complex_structure").await;
    
    Ok(())
}
```

## Prevention Strategies

### Input Validation

```rust
// Comprehensive input validation
async fn validate_input_data(value: &Value) -> Result<(), String> {
    // Check overall structure
    check_data_integrity(value).await?;
    
    // Check size limits
    validate_size_limits(value)?;
    
    // Check type constraints
    validate_type_constraints(value)?;
    
    // Check business rules
    validate_business_rules(value)?;
    
    Ok(())
}

fn validate_size_limits(value: &Value) -> Result<(), String> {
    match value {
        Value::Text(s) if s.len() > 1_000_000 => {
            Err(format!("Text too long: {} chars (max: 1M)", s.len()))
        }
        Value::Blob(b) if b.len() > 10_000_000 => {
            Err(format!("Blob too large: {} bytes (max: 10MB)", b.len()))
        }
        Value::List(items) if items.len() > 100_000 => {
            Err(format!("List too large: {} items (max: 100K)", items.len()))
        }
        _ => Ok(())
    }
}

fn validate_type_constraints(value: &Value) -> Result<(), String> {
    match value {
        Value::Integer(i) if *i == i32::MIN => {
            Err("Integer value at minimum limit may cause overflow".to_string())
        }
        Value::Float(f) if f.is_nan() => {
            Err("NaN float values are not supported".to_string())
        }
        Value::Text(s) if s.contains('\0') => {
            Err("Text contains null characters".to_string())
        }
        _ => Ok(())
    }
}

fn validate_business_rules(value: &Value) -> Result<(), String> {
    // Example business rules - customize for your application
    if let Value::Udt(type_name, fields) = value {
        match type_name.as_str() {
            "User" => {
                if !fields.contains_key("id") {
                    return Err("User UDT must have 'id' field".to_string());
                }
                if !fields.contains_key("email") {
                    return Err("User UDT must have 'email' field".to_string());
                }
            }
            "Order" => {
                if !fields.contains_key("order_id") {
                    return Err("Order UDT must have 'order_id' field".to_string());
                }
                if !fields.contains_key("total") {
                    return Err("Order UDT must have 'total' field".to_string());
                }
            }
            _ => {} // Other UDT types
        }
    }
    
    Ok(())
}
```

This troubleshooting guide provides comprehensive solutions for the most common issues when working with CQLite's complex types. Use these diagnostic tools and strategies to identify, resolve, and prevent problems in your applications.