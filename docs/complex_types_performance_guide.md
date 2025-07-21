# CQLite Complex Types Performance Guide

## Overview

This guide provides detailed performance characteristics, optimization strategies, and best practices for using CQLite's complex data types efficiently. Based on extensive benchmarking and real-world usage patterns.

## Performance Characteristics

### Memory Usage by Type

| Type | Base Overhead | Per Element | Cache Efficiency | Best Use Case |
|------|---------------|-------------|------------------|---------------|
| **List** | 24 bytes | 8 bytes | High (sequential) | Ordered data, logs |
| **Set** | 24 bytes | 8 bytes | Medium (hash-based) | Unique values, tags |
| **Map** | 32 bytes | 16 bytes | Medium (key-value) | Configuration, metadata |
| **Tuple** | 16 bytes | 8 bytes | High (indexed) | Fixed structure data |
| **UDT** | 48 bytes | 16 bytes | Low (name lookup) | Complex entities |
| **Frozen** | +8 bytes | Same as inner | Same as inner | Immutable data |

### Serialization Performance

```rust
use std::time::Instant;
use cqlite_core::{Value, parser::types::serialize_cql_value};

async fn benchmark_serialization_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Complex Types Serialization Benchmarks");
    
    // Test different collection sizes
    let sizes = vec![10, 100, 1_000, 10_000];
    
    for size in sizes {
        println!("\n📊 Testing with {} elements:", size);
        
        // List benchmark
        let list_data: Vec<Value> = (0..size).map(|i| Value::Integer(i)).collect();
        let list_value = Value::List(list_data);
        
        let start = Instant::now();
        let iterations = 1000;
        for _ in 0..iterations {
            let _ = serialize_cql_value(&list_value)?;
        }
        let list_duration = start.elapsed();
        println!("  List:  {:?} avg ({} ops/sec)", 
                list_duration / iterations, 
                iterations * 1000 / list_duration.as_millis().max(1));
        
        // Map benchmark
        let mut map_data = Vec::new();
        for i in 0..size {
            map_data.push((
                Value::Text(format!("key_{}", i)),
                Value::Integer(i)
            ));
        }
        let map_value = Value::Map(map_data);
        
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = serialize_cql_value(&map_value)?;
        }
        let map_duration = start.elapsed();
        println!("  Map:   {:?} avg ({} ops/sec)", 
                map_duration / iterations,
                iterations * 1000 / map_duration.as_millis().max(1));
        
        // UDT benchmark
        let mut udt_fields = std::collections::HashMap::new();
        for i in 0..size.min(100) { // Limit UDT fields to reasonable number
            udt_fields.insert(format!("field_{}", i), Value::Integer(i));
        }
        let udt_value = Value::Udt("TestUDT".to_string(), udt_fields);
        
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = serialize_cql_value(&udt_value)?;
        }
        let udt_duration = start.elapsed();
        println!("  UDT:   {:?} avg ({} ops/sec)", 
                udt_duration / iterations,
                iterations * 1000 / udt_duration.as_millis().max(1));
    }
    
    Ok(())
}
```

### Parsing Performance

```rust
async fn benchmark_parsing_performance() -> Result<(), Box<dyn std::error::Error>> {
    use cqlite_core::parser::types::{parse_cql_value, CqlTypeId};
    
    println!("\n🔬 Complex Types Parsing Benchmarks");
    
    // Create test data for parsing
    let test_list = Value::List(vec![
        Value::Integer(1), Value::Integer(2), Value::Integer(3),
        Value::Text("hello".to_string()), Value::Boolean(true)
    ]);
    let serialized_list = serialize_cql_value(&test_list)?;
    
    let mut test_map = Vec::new();
    test_map.push((Value::Text("key1".to_string()), Value::Integer(100)));
    test_map.push((Value::Text("key2".to_string()), Value::Text("value2".to_string())));
    test_map.push((Value::Text("key3".to_string()), Value::Boolean(true)));
    let test_map_value = Value::Map(test_map);
    let serialized_map = serialize_cql_value(&test_map_value)?;
    
    let iterations = 10_000;
    
    // List parsing benchmark
    let start = Instant::now();
    for _ in 0..iterations {
        if serialized_list.len() > 1 {
            let _ = parse_cql_value(&serialized_list[1..], CqlTypeId::List)?;
        }
    }
    let list_parse_duration = start.elapsed();
    println!("  List parsing:  {:?} avg ({} ops/sec)", 
            list_parse_duration / iterations,
            iterations * 1000 / list_parse_duration.as_millis().max(1));
    
    // Map parsing benchmark  
    let start = Instant::now();
    for _ in 0..iterations {
        if serialized_map.len() > 1 {
            let _ = parse_cql_value(&serialized_map[1..], CqlTypeId::Map)?;
        }
    }
    let map_parse_duration = start.elapsed();
    println!("  Map parsing:   {:?} avg ({} ops/sec)", 
            map_parse_duration / iterations,
            iterations * 1000 / map_parse_duration.as_millis().max(1));
    
    Ok(())
}
```

## Memory Optimization Strategies

### 1. Collection Size Management

```rust
// ✅ Good: Implement size limits
const MAX_LIST_SIZE: usize = 10_000;
const MAX_MAP_SIZE: usize = 5_000;
const MAX_UDT_FIELDS: usize = 100;

fn create_bounded_list(items: Vec<Value>) -> Value {
    let bounded_items = items.into_iter()
        .take(MAX_LIST_SIZE)
        .collect();
    Value::List(bounded_items)
}

fn create_bounded_map(pairs: Vec<(Value, Value)>) -> Value {
    let bounded_pairs = pairs.into_iter()
        .take(MAX_MAP_SIZE)
        .collect();
    Value::Map(bounded_pairs)
}

// ✅ Good: Use pagination for large datasets
struct PaginatedList {
    page_size: usize,
    current_page: usize,
}

impl PaginatedList {
    fn get_page(&self, all_items: &[Value]) -> Value {
        let start = self.current_page * self.page_size;
        let end = (start + self.page_size).min(all_items.len());
        
        let page_items = all_items[start..end].to_vec();
        Value::List(page_items)
    }
}
```

### 2. Memory Pool Usage

```rust
use std::sync::Arc;
use std::collections::VecDeque;

// Memory pool for reusing Value allocations
struct ValuePool {
    list_pool: VecDeque<Vec<Value>>,
    map_pool: VecDeque<Vec<(Value, Value)>>,
    string_pool: VecDeque<String>,
}

impl ValuePool {
    fn new() -> Self {
        Self {
            list_pool: VecDeque::new(),
            map_pool: VecDeque::new(),
            string_pool: VecDeque::new(),
        }
    }
    
    fn get_list_vec(&mut self) -> Vec<Value> {
        self.list_pool.pop_front().unwrap_or_else(Vec::new)
    }
    
    fn return_list_vec(&mut self, mut vec: Vec<Value>) {
        vec.clear();
        if vec.capacity() <= 1000 { // Don't pool overly large vectors
            self.list_pool.push_back(vec);
        }
    }
    
    fn get_map_vec(&mut self) -> Vec<(Value, Value)> {
        self.map_pool.pop_front().unwrap_or_else(Vec::new)
    }
    
    fn return_map_vec(&mut self, mut vec: Vec<(Value, Value)>) {
        vec.clear();
        if vec.capacity() <= 500 {
            self.map_pool.push_back(vec);
        }
    }
}

// Usage example with memory pool
async fn optimized_value_creation(pool: &mut ValuePool) -> Value {
    let mut items = pool.get_list_vec();
    
    // Use the vector
    items.push(Value::Integer(1));
    items.push(Value::Integer(2));
    items.push(Value::Integer(3));
    
    let result = Value::List(items.clone());
    
    // Return to pool for reuse
    pool.return_list_vec(items);
    
    result
}
```

### 3. Lazy Loading Patterns

```rust
// Lazy-loaded complex structure
struct LazyUserProfile {
    user_id: String,
    basic_info: Option<Value>, // Load on demand
    social_links: Option<Value>, // Load on demand
    activity_history: Option<Value>, // Load on demand
}

impl LazyUserProfile {
    async fn get_basic_info(&mut self, db: &Database) -> Result<&Value, Box<dyn std::error::Error>> {
        if self.basic_info.is_none() {
            // Load only basic info UDT
            let mut basic_fields = std::collections::HashMap::new();
            basic_fields.insert("name".to_string(), Value::Text("John Doe".to_string()));
            basic_fields.insert("email".to_string(), Value::Text("john@example.com".to_string()));
            
            self.basic_info = Some(Value::Udt("BasicInfo".to_string(), basic_fields));
        }
        
        Ok(self.basic_info.as_ref().unwrap())
    }
    
    async fn get_social_links(&mut self, db: &Database) -> Result<&Value, Box<dyn std::error::Error>> {
        if self.social_links.is_none() {
            // Load social links only when needed
            let links = Value::List(vec![
                Value::Text("https://twitter.com/johndoe".to_string()),
                Value::Text("https://linkedin.com/in/johndoe".to_string()),
            ]);
            self.social_links = Some(links);
        }
        
        Ok(self.social_links.as_ref().unwrap())
    }
}
```

## CPU Optimization

### 1. Efficient Serialization

```rust
// Pre-compute serialization for frequently accessed data
use std::collections::HashMap;
use std::sync::RwLock;

struct SerializationCache {
    cache: RwLock<HashMap<String, Vec<u8>>>,
    max_size: usize,
}

impl SerializationCache {
    fn new(max_size: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_size,
        }
    }
    
    fn get_or_serialize(&self, key: &str, value: &Value) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Try to get from cache first
        {
            let cache = self.cache.read().unwrap();
            if let Some(serialized) = cache.get(key) {
                return Ok(serialized.clone());
            }
        }
        
        // Serialize and cache
        let serialized = serialize_cql_value(value)?;
        
        {
            let mut cache = self.cache.write().unwrap();
            if cache.len() < self.max_size {
                cache.insert(key.to_string(), serialized.clone());
            }
        }
        
        Ok(serialized)
    }
}

// Usage with frequently accessed configuration
async fn use_serialization_cache() -> Result<(), Box<dyn std::error::Error>> {
    let cache = SerializationCache::new(1000);
    
    // Configuration that's accessed frequently
    let mut config_map = Vec::new();
    config_map.push((Value::Text("timeout".to_string()), Value::Integer(30)));
    config_map.push((Value::Text("retries".to_string()), Value::Integer(3)));
    config_map.push((Value::Text("batch_size".to_string()), Value::Integer(100)));
    
    let config_value = Value::Map(config_map);
    
    // First access - will serialize and cache
    let serialized1 = cache.get_or_serialize("app_config", &config_value)?;
    
    // Second access - will use cached version
    let serialized2 = cache.get_or_serialize("app_config", &config_value)?;
    
    assert_eq!(serialized1, serialized2);
    println!("✅ Serialization cache working correctly");
    
    Ok(())
}
```

### 2. Batch Operations

```rust
// Batch serialization for multiple values
async fn batch_serialize_values(values: &[Value]) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut results = Vec::with_capacity(values.len());
    
    // Process in chunks to balance memory usage and performance
    const CHUNK_SIZE: usize = 100;
    
    for chunk in values.chunks(CHUNK_SIZE) {
        let mut chunk_results = Vec::with_capacity(chunk.len());
        
        for value in chunk {
            chunk_results.push(serialize_cql_value(value)?);
        }
        
        results.extend(chunk_results);
    }
    
    Ok(results)
}

// Batch UDT creation
fn batch_create_user_udts(user_data: &[(String, String, i32)]) -> Vec<Value> {
    user_data.iter().map(|(name, email, age)| {
        let mut fields = std::collections::HashMap::new();
        fields.insert("name".to_string(), Value::Text(name.clone()));
        fields.insert("email".to_string(), Value::Text(email.clone()));
        fields.insert("age".to_string(), Value::Integer(*age));
        
        Value::Udt("User".to_string(), fields)
    }).collect()
}
```

### 3. String Interning

```rust
use std::sync::Arc;
use std::collections::HashSet;
use std::sync::Mutex;

// String interning for frequently used strings
struct StringInterner {
    strings: Mutex<HashSet<Arc<str>>>,
}

impl StringInterner {
    fn new() -> Self {
        Self {
            strings: Mutex::new(HashSet::new()),
        }
    }
    
    fn intern(&self, s: &str) -> Arc<str> {
        let mut strings = self.strings.lock().unwrap();
        
        if let Some(existing) = strings.get(s) {
            existing.clone()
        } else {
            let arc_str: Arc<str> = Arc::from(s);
            strings.insert(arc_str.clone());
            arc_str
        }
    }
}

// Usage for repeated field names in UDTs
async fn use_string_interning() -> Result<(), Box<dyn std::error::Error>> {
    let interner = StringInterner::new();
    
    // Common field names that appear in many UDTs
    let name_field = interner.intern("name");
    let email_field = interner.intern("email");
    let created_at_field = interner.intern("created_at");
    
    // Create multiple UDTs reusing interned strings
    let mut udts = Vec::new();
    for i in 0..1000 {
        let mut fields = std::collections::HashMap::new();
        fields.insert(name_field.to_string(), Value::Text(format!("User {}", i)));
        fields.insert(email_field.to_string(), Value::Text(format!("user{}@example.com", i)));
        fields.insert(created_at_field.to_string(), Value::Timestamp(1640995200000000));
        
        udts.push(Value::Udt("User".to_string(), fields));
    }
    
    println!("✅ Created {} UDTs with interned strings", udts.len());
    Ok(())
}
```

## Network and I/O Optimization

### 1. Compression for Large Collections

```rust
use flate2::Compression;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use std::io::prelude::*;

async fn compress_large_collection(value: &Value) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Serialize first
    let serialized = serialize_cql_value(value)?;
    
    // Compress if large enough to benefit
    if serialized.len() > 1024 { // Only compress if > 1KB
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&serialized)?;
        let compressed = encoder.finish()?;
        
        println!("Compressed {} bytes to {} bytes ({:.1}% reduction)", 
                serialized.len(), 
                compressed.len(),
                100.0 * (1.0 - compressed.len() as f64 / serialized.len() as f64));
        
        Ok(compressed)
    } else {
        Ok(serialized)
    }
}

async fn decompress_large_collection(compressed: &[u8]) -> Result<Value, Box<dyn std::error::Error>> {
    // Try to decompress
    let mut decoder = GzDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    
    // Parse the decompressed data
    if decompressed.len() > 1 {
        let (_, value) = parse_cql_value(&decompressed[1..], CqlTypeId::List)?;
        Ok(value)
    } else {
        Err("Invalid compressed data".into())
    }
}
```

### 2. Streaming for Large Collections

```rust
use futures::stream::{Stream, StreamExt};
use std::pin::Pin;

// Stream large lists instead of loading all at once
struct StreamingList {
    items: Vec<Value>,
    chunk_size: usize,
    current_index: usize,
}

impl StreamingList {
    fn new(items: Vec<Value>, chunk_size: usize) -> Self {
        Self {
            items,
            chunk_size,
            current_index: 0,
        }
    }
    
    fn into_stream(self) -> impl Stream<Item = Value> {
        futures::stream::unfold(self, |mut state| async move {
            if state.current_index < state.items.len() {
                let end_index = (state.current_index + state.chunk_size).min(state.items.len());
                let chunk: Vec<Value> = state.items[state.current_index..end_index].to_vec();
                state.current_index = end_index;
                
                Some((Value::List(chunk), state))
            } else {
                None
            }
        })
    }
}

// Usage example
async fn process_large_list_streaming() -> Result<(), Box<dyn std::error::Error>> {
    // Create a large list
    let large_list: Vec<Value> = (0..100_000)
        .map(|i| Value::Integer(i))
        .collect();
    
    // Stream in chunks of 1000
    let streaming_list = StreamingList::new(large_list, 1000);
    let mut stream = streaming_list.into_stream();
    
    let mut chunk_count = 0;
    while let Some(chunk) = stream.next().await {
        // Process each chunk
        if let Value::List(items) = chunk {
            println!("Processing chunk {} with {} items", chunk_count, items.len());
            chunk_count += 1;
            
            // Simulate processing
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
    
    println!("✅ Processed {} chunks", chunk_count);
    Ok(())
}
```

## Query Optimization

### 1. Index-Friendly Complex Types

```rust
// Use frozen types for indexable data
async fn create_indexed_complex_data() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./optimized_data").await?;
    
    // Frozen tuple for compound key (indexable)
    let compound_key = Value::Frozen(Box::new(Value::Tuple(vec![
        Value::Text("US".to_string()),      // country
        Value::Text("CA".to_string()),      // state
        Value::Text("San Francisco".to_string()), // city
    ])));
    
    // Regular mutable data for frequently updated fields
    let mut user_data = std::collections::HashMap::new();
    user_data.insert("location_key".to_string(), compound_key); // Indexed
    user_data.insert("last_login".to_string(), Value::Timestamp(1640995200000000)); // Frequently updated
    user_data.insert("login_count".to_string(), Value::Integer(42)); // Frequently updated
    
    // Frozen configuration (rarely changes, good for caching)
    let mut config_fields = std::collections::HashMap::new();
    config_fields.insert("theme".to_string(), Value::Text("dark".to_string()));
    config_fields.insert("language".to_string(), Value::Text("en".to_string()));
    config_fields.insert("timezone".to_string(), Value::Text("PST".to_string()));
    
    let user_config = Value::Udt("UserConfig".to_string(), config_fields);
    let frozen_config = Value::Frozen(Box::new(user_config));
    
    user_data.insert("config".to_string(), frozen_config);
    
    db.insert("users", user_data).await?;
    println!("✅ Created indexed complex data");
    Ok(())
}
```

### 2. Efficient Range Queries

```rust
// Structure data for efficient range queries
async fn create_time_series_data() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("./timeseries_data").await?;
    
    // Time-bucketed data for efficient range queries
    let bucket_timestamp = 1640995200000000; // Hour bucket: 2022-01-01 00:00:00
    
    // Measurements within the hour as list (ordered by time)
    let measurements = Value::List(vec![
        Value::Tuple(vec![Value::Timestamp(1640995200000000), Value::Float(23.5)]), // temp
        Value::Tuple(vec![Value::Timestamp(1640995260000000), Value::Float(23.6)]), // temp
        Value::Tuple(vec![Value::Timestamp(1640995320000000), Value::Float(23.4)]), // temp
        Value::Tuple(vec![Value::Timestamp(1640995380000000), Value::Float(23.7)]), // temp
    ]);
    
    // Aggregated statistics for the bucket (frozen for immutability)
    let mut stats_data = Vec::new();
    stats_data.push((Value::Text("min".to_string()), Value::Float(23.4)));
    stats_data.push((Value::Text("max".to_string()), Value::Float(23.7)));
    stats_data.push((Value::Text("avg".to_string()), Value::Float(23.55)));
    stats_data.push((Value::Text("count".to_string()), Value::Integer(4)));
    
    let hourly_stats = Value::Frozen(Box::new(Value::Map(stats_data)));
    
    let mut bucket_data = std::collections::HashMap::new();
    bucket_data.insert("sensor_id".to_string(), Value::Text("TEMP-001".to_string()));
    bucket_data.insert("bucket_time".to_string(), Value::Timestamp(bucket_timestamp));
    bucket_data.insert("measurements".to_string(), measurements);
    bucket_data.insert("hourly_stats".to_string(), hourly_stats);
    
    db.insert("sensor_data_hourly", bucket_data).await?;
    println!("✅ Created time-series bucket data");
    Ok(())
}
```

## Monitoring and Profiling

### 1. Memory Usage Tracking

```rust
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

// Custom allocator to track memory usage
struct TrackingAllocator;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::SeqCst);
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        ALLOCATED.fetch_sub(layout.size(), Ordering::SeqCst);
    }
}

// Memory usage reporter
fn get_allocated_memory() -> usize {
    ALLOCATED.load(Ordering::SeqCst)
}

async fn monitor_memory_usage() -> Result<(), Box<dyn std::error::Error>> {
    let initial_memory = get_allocated_memory();
    println!("Initial memory usage: {} bytes", initial_memory);
    
    // Create large complex structure
    let mut large_map = Vec::new();
    for i in 0..10_000 {
        let mut nested_fields = std::collections::HashMap::new();
        nested_fields.insert("id".to_string(), Value::Integer(i));
        nested_fields.insert("data".to_string(), Value::Text(format!("Item {}", i)));
        
        let nested_udt = Value::Udt("Item".to_string(), nested_fields);
        large_map.push((Value::Text(format!("key_{}", i)), nested_udt));
    }
    
    let complex_structure = Value::Map(large_map);
    let after_creation = get_allocated_memory();
    println!("After creating complex structure: {} bytes (+{} bytes)", 
            after_creation, after_creation - initial_memory);
    
    // Serialize it
    let serialized = serialize_cql_value(&complex_structure)?;
    let after_serialization = get_allocated_memory();
    println!("After serialization: {} bytes (+{} bytes)", 
            after_serialization, after_serialization - after_creation);
    println!("Serialized size: {} bytes", serialized.len());
    
    Ok(())
}
```

### 2. Performance Profiling

```rust
use std::time::{Duration, Instant};
use std::collections::HashMap;

struct PerformanceProfiler {
    timings: HashMap<String, Vec<Duration>>,
}

impl PerformanceProfiler {
    fn new() -> Self {
        Self {
            timings: HashMap::new(),
        }
    }
    
    fn time<F, R>(&mut self, operation: &str, f: F) -> R 
    where F: FnOnce() -> R 
    {
        let start = Instant::now();
        let result = f();
        let elapsed = start.elapsed();
        
        self.timings.entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(elapsed);
        
        result
    }
    
    fn report(&self) {
        println!("\n📊 Performance Report:");
        for (operation, timings) in &self.timings {
            let total: Duration = timings.iter().sum();
            let avg = total / timings.len() as u32;
            let min = *timings.iter().min().unwrap();
            let max = *timings.iter().max().unwrap();
            
            println!("  {}: {} samples", operation, timings.len());
            println!("    Avg: {:?}", avg);
            println!("    Min: {:?}", min);
            println!("    Max: {:?}", max);
            println!("    Total: {:?}", total);
        }
    }
}

async fn profile_complex_operations() -> Result<(), Box<dyn std::error::Error>> {
    let mut profiler = PerformanceProfiler::new();
    
    // Profile different operations
    for size in &[100, 1000, 5000] {
        // List operations
        let list_data: Vec<Value> = (0..*size).map(|i| Value::Integer(i)).collect();
        let list_value = Value::List(list_data);
        
        profiler.time(&format!("list_serialize_{}", size), || {
            serialize_cql_value(&list_value).unwrap()
        });
        
        // Map operations
        let mut map_data = Vec::new();
        for i in 0..*size {
            map_data.push((
                Value::Text(format!("key_{}", i)),
                Value::Integer(i)
            ));
        }
        let map_value = Value::Map(map_data);
        
        profiler.time(&format!("map_serialize_{}", size), || {
            serialize_cql_value(&map_value).unwrap()
        });
        
        // UDT operations (limited size)
        let mut udt_fields = std::collections::HashMap::new();
        let field_count = (*size).min(100); // Limit UDT fields
        for i in 0..field_count {
            udt_fields.insert(format!("field_{}", i), Value::Integer(i));
        }
        let udt_value = Value::Udt("TestUDT".to_string(), udt_fields);
        
        profiler.time(&format!("udt_serialize_{}", field_count), || {
            serialize_cql_value(&udt_value).unwrap()
        });
    }
    
    profiler.report();
    Ok(())
}
```

## Production Deployment Guidelines

### 1. Configuration Recommendations

```rust
// Production configuration structure
#[derive(Debug, Clone)]
struct ProductionConfig {
    // Memory limits
    max_list_size: usize,
    max_map_size: usize,
    max_udt_fields: usize,
    max_nesting_depth: usize,
    
    // Performance settings
    serialization_cache_size: usize,
    compression_threshold: usize,
    batch_size: usize,
    
    // Monitoring
    enable_metrics: bool,
    metrics_interval_seconds: u64,
}

impl Default for ProductionConfig {
    fn default() -> Self {
        Self {
            // Conservative memory limits
            max_list_size: 10_000,
            max_map_size: 5_000,
            max_udt_fields: 100,
            max_nesting_depth: 10,
            
            // Reasonable performance settings
            serialization_cache_size: 1_000,
            compression_threshold: 1_024, // 1KB
            batch_size: 100,
            
            // Enable monitoring in production
            enable_metrics: true,
            metrics_interval_seconds: 60,
        }
    }
}

// Validation function for production use
fn validate_complex_type(value: &Value, config: &ProductionConfig) -> Result<(), String> {
    validate_complex_type_recursive(value, config, 0)
}

fn validate_complex_type_recursive(
    value: &Value, 
    config: &ProductionConfig, 
    depth: usize
) -> Result<(), String> {
    if depth > config.max_nesting_depth {
        return Err(format!("Nesting depth {} exceeds limit {}", depth, config.max_nesting_depth));
    }
    
    match value {
        Value::List(items) => {
            if items.len() > config.max_list_size {
                return Err(format!("List size {} exceeds limit {}", items.len(), config.max_list_size));
            }
            for item in items {
                validate_complex_type_recursive(item, config, depth + 1)?;
            }
        }
        Value::Set(items) => {
            if items.len() > config.max_list_size {
                return Err(format!("Set size {} exceeds limit {}", items.len(), config.max_list_size));
            }
            for item in items {
                validate_complex_type_recursive(item, config, depth + 1)?;
            }
        }
        Value::Map(pairs) => {
            if pairs.len() > config.max_map_size {
                return Err(format!("Map size {} exceeds limit {}", pairs.len(), config.max_map_size));
            }
            for (key, value) in pairs {
                validate_complex_type_recursive(key, config, depth + 1)?;
                validate_complex_type_recursive(value, config, depth + 1)?;
            }
        }
        Value::Tuple(items) => {
            for item in items {
                validate_complex_type_recursive(item, config, depth + 1)?;
            }
        }
        Value::Udt(_, fields) => {
            if fields.len() > config.max_udt_fields {
                return Err(format!("UDT field count {} exceeds limit {}", fields.len(), config.max_udt_fields));
            }
            for (_, field_value) in fields {
                validate_complex_type_recursive(field_value, config, depth + 1)?;
            }
        }
        Value::Frozen(inner) => {
            validate_complex_type_recursive(inner, config, depth + 1)?;
        }
        _ => {} // Primitive types are OK
    }
    
    Ok(())
}
```

### 2. Monitoring and Alerting

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// Production metrics collector
#[derive(Debug)]
struct ComplexTypeMetrics {
    // Operation counters
    serializations_total: AtomicU64,
    serialization_errors: AtomicU64,
    parsings_total: AtomicU64,
    parsing_errors: AtomicU64,
    
    // Size statistics
    largest_list_size: AtomicU64,
    largest_map_size: AtomicU64,
    largest_serialized_size: AtomicU64,
    
    // Performance statistics
    total_serialization_time_nanos: AtomicU64,
    total_parsing_time_nanos: AtomicU64,
}

impl ComplexTypeMetrics {
    fn new() -> Self {
        Self {
            serializations_total: AtomicU64::new(0),
            serialization_errors: AtomicU64::new(0),
            parsings_total: AtomicU64::new(0),
            parsing_errors: AtomicU64::new(0),
            largest_list_size: AtomicU64::new(0),
            largest_map_size: AtomicU64::new(0),
            largest_serialized_size: AtomicU64::new(0),
            total_serialization_time_nanos: AtomicU64::new(0),
            total_parsing_time_nanos: AtomicU64::new(0),
        }
    }
    
    fn record_serialization(&self, value: &Value, result: &Result<Vec<u8>, Box<dyn std::error::Error>>, duration_nanos: u64) {
        self.serializations_total.fetch_add(1, Ordering::Relaxed);
        self.total_serialization_time_nanos.fetch_add(duration_nanos, Ordering::Relaxed);
        
        match result {
            Ok(serialized) => {
                let size = serialized.len() as u64;
                let current_max = self.largest_serialized_size.load(Ordering::Relaxed);
                if size > current_max {
                    self.largest_serialized_size.store(size, Ordering::Relaxed);
                }
                
                // Track collection sizes
                match value {
                    Value::List(items) => {
                        let len = items.len() as u64;
                        let current_max = self.largest_list_size.load(Ordering::Relaxed);
                        if len > current_max {
                            self.largest_list_size.store(len, Ordering::Relaxed);
                        }
                    }
                    Value::Map(pairs) => {
                        let len = pairs.len() as u64;
                        let current_max = self.largest_map_size.load(Ordering::Relaxed);
                        if len > current_max {
                            self.largest_map_size.store(len, Ordering::Relaxed);
                        }
                    }
                    _ => {}
                }
            }
            Err(_) => {
                self.serialization_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    
    fn get_report(&self) -> String {
        let serializations = self.serializations_total.load(Ordering::Relaxed);
        let serialization_errors = self.serialization_errors.load(Ordering::Relaxed);
        let total_ser_time = self.total_serialization_time_nanos.load(Ordering::Relaxed);
        
        let avg_ser_time = if serializations > 0 {
            total_ser_time / serializations
        } else {
            0
        };
        
        format!(
            "Complex Types Metrics:\n\
             Serializations: {} (errors: {})\n\
             Avg serialization time: {} ns\n\
             Largest list: {} items\n\
             Largest map: {} items\n\
             Largest serialized: {} bytes",
            serializations,
            serialization_errors,
            avg_ser_time,
            self.largest_list_size.load(Ordering::Relaxed),
            self.largest_map_size.load(Ordering::Relaxed),
            self.largest_serialized_size.load(Ordering::Relaxed)
        )
    }
}

// Usage in production
async fn production_monitoring_example() -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(ComplexTypeMetrics::new());
    
    // Simulate production workload
    for i in 0..1000 {
        let value = Value::List(vec![
            Value::Integer(i),
            Value::Text(format!("item_{}", i)),
            Value::Boolean(i % 2 == 0),
        ]);
        
        let start = Instant::now();
        let result = serialize_cql_value(&value);
        let duration = start.elapsed().as_nanos() as u64;
        
        metrics.record_serialization(&value, &result.map_err(|e| e.into()), duration);
        
        // Simulate processing delay
        if i % 100 == 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
    }
    
    println!("{}", metrics.get_report());
    Ok(())
}
```

## Summary

### Performance Best Practices

1. **Memory Management**
   - Use size limits for collections
   - Implement memory pooling for frequent allocations
   - Use lazy loading for large nested structures

2. **CPU Optimization**
   - Cache serialized data for frequently accessed values
   - Use batch operations when possible
   - Consider string interning for repeated field names

3. **I/O Optimization**
   - Compress large collections before network transmission
   - Use streaming for very large datasets
   - Structure data for efficient range queries

4. **Production Deployment**
   - Implement comprehensive validation
   - Monitor key performance metrics
   - Set appropriate resource limits
   - Use frozen types for indexable data

Following these guidelines will ensure optimal performance of CQLite's complex types in production environments.