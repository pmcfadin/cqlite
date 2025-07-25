# Complex Types Integration Plan

## Overview

This document outlines the step-by-step integration plan for incorporating the new complex types architecture into the existing CQLite codebase.

## Current State Analysis

### Existing Components
1. **Basic Types System** (`src/types.rs`) - Simple Value enum with basic collections
2. **Parser Module** (`src/parser/`) - Basic CQL type parsing
3. **Schema System** (`src/schema/`) - Table schema with basic type support
4. **Storage Engine** - SSTable reading/writing
5. **Query Engine** - Basic query processing

### New Components
1. **Enhanced Types** (`src/types_enhanced.rs`) - Complete complex type support
2. **Complex Parser** (`src/parser/complex_types.rs`) - Advanced type parsing
3. **Type Specifications** - Rich type metadata system

## Integration Strategy

### Phase 1: Foundation (Week 1-2)

#### 1.1 Update Module Structure
```bash
# Add new modules to src/lib.rs
src/
├── types.rs                    # Keep for backward compatibility
├── types_enhanced.rs          # New enhanced types
├── parser/
│   ├── mod.rs
│   ├── types.rs              # Basic type parsing
│   ├── complex_types.rs      # New complex type parsing
│   └── ...
```

#### 1.2 Create Type Compatibility Layer
```rust
// src/types_compat.rs
pub fn convert_value_to_enhanced(old: &types::Value) -> types_enhanced::Value {
    match old {
        types::Value::List(values) => {
            // Convert to enhanced CollectionValue with inferred type
            let element_type = infer_element_type(values);
            types_enhanced::Value::List(
                types_enhanced::CollectionValue::new(element_type, converted_values)
            )
        }
        // ... other conversions
    }
}

pub fn convert_value_from_enhanced(new: &types_enhanced::Value) -> types::Value {
    // Reverse conversion for backward compatibility
}
```

#### 1.3 Update Error Types
```rust
// src/error.rs - Add new error variants
pub enum Error {
    // Existing errors...
    ComplexTypeError(String),
    TypeMismatch { expected: String, found: String },
    UnsupportedTypeFeature(String),
}
```

### Phase 2: Parser Integration (Week 2-3)

#### 2.1 Update Parser Module Structure
```rust
// src/parser/mod.rs
pub mod types;           // Existing basic parsing
pub mod complex_types;   // New complex parsing
pub mod type_registry;   // Type specification registry

// Re-export for compatibility
pub use types::*;
pub use complex_types::*;
```

#### 2.2 Create Unified Parser Interface
```rust
// src/parser/unified.rs
pub struct UnifiedParser {
    basic_parser: BasicTypeParser,
    complex_parser: ComplexTypeParser,
    use_enhanced: bool,
}

impl UnifiedParser {
    pub fn parse_value(&self, input: &[u8], type_info: &TypeInfo) -> Result<ParsedValue> {
        if self.use_enhanced {
            self.complex_parser.parse_complex_cql_value(input, &type_info.spec)
        } else {
            self.basic_parser.parse_cql_value(input, type_info.id)
        }
    }
}
```

#### 2.3 Type Registry System
```rust
// src/parser/type_registry.rs
pub struct TypeRegistry {
    udt_types: HashMap<String, CqlTypeSpec>,
    tuple_types: HashMap<String, Vec<CqlTypeSpec>>,
}

impl TypeRegistry {
    pub fn register_udt(&mut self, name: String, spec: CqlTypeSpec) -> Result<()>;
    pub fn resolve_type(&self, name: &str) -> Option<&CqlTypeSpec>;
    pub fn load_from_schema(&mut self, schema: &TableSchema) -> Result<()>;
}
```

### Phase 3: Schema System Enhancement (Week 3-4)

#### 3.1 Enhance CqlType Enum
```rust
// src/schema/mod.rs - Update existing CqlType
impl CqlType {
    // New methods for complex type support
    pub fn to_type_spec(&self) -> types_enhanced::CqlTypeSpec {
        match self {
            CqlType::List(inner) => CqlTypeSpec::List(Box::new(inner.to_type_spec())),
            CqlType::Map(key, value) => CqlTypeSpec::Map(
                Box::new(key.to_type_spec()),
                Box::new(value.to_type_spec())
            ),
            // ... other conversions
        }
    }
    
    pub fn from_type_spec(spec: &types_enhanced::CqlTypeSpec) -> Self {
        // Reverse conversion
    }
}
```

#### 3.2 Schema Loading Enhancement
```rust
// src/schema/loader.rs
pub struct EnhancedSchemaLoader {
    type_registry: TypeRegistry,
}

impl EnhancedSchemaLoader {
    pub fn load_schema_with_types(&mut self, schema_path: &Path) -> Result<TableSchema> {
        let schema = self.load_basic_schema(schema_path)?;
        
        // Register all UDTs and complex types
        for column in &schema.columns {
            if let Ok(complex_type) = CqlType::parse(&column.data_type) {
                self.register_complex_types(&complex_type)?;
            }
        }
        
        Ok(schema)
    }
}
```

### Phase 4: Storage Engine Integration (Week 4-5)

#### 4.1 SSTable Reader Enhancement
```rust
// src/storage/sstable/enhanced_reader.rs
pub struct EnhancedSSTableReader {
    basic_reader: SSTableReader,
    type_registry: Arc<TypeRegistry>,
    parser: UnifiedParser,
}

impl EnhancedSSTableReader {
    pub fn read_complex_value(&self, data: &[u8], type_spec: &CqlTypeSpec) -> Result<Value> {
        self.parser.parse_complex_cql_value(data, type_spec)
    }
    
    pub fn read_row_with_schema(&self, data: &[u8], schema: &TableSchema) -> Result<Row> {
        // Use schema to parse complex types correctly
    }
}
```

#### 4.2 SSTable Writer Enhancement
```rust
// src/storage/sstable/enhanced_writer.rs
pub struct EnhancedSSTableWriter {
    basic_writer: SSTableWriter,
    type_registry: Arc<TypeRegistry>,
}

impl EnhancedSSTableWriter {
    pub fn write_complex_value(&mut self, value: &Value) -> Result<()> {
        let serialized = serialize_complex_cql_value(value)?;
        self.basic_writer.write_bytes(&serialized)
    }
}
```

### Phase 5: Query Engine Integration (Week 5-6)

#### 5.1 Value Operations Enhancement
```rust
// src/query/value_ops.rs
impl Value {
    // Enhanced operations for complex types
    pub fn get_nested_field(&self, path: &[&str]) -> Option<&Value> {
        match self {
            Value::Udt(udt) => {
                if let Some(field_name) = path.first() {
                    if let Some(field_value) = udt.get_field(field_name) {
                        if path.len() == 1 {
                            Some(field_value)
                        } else {
                            field_value.get_nested_field(&path[1..])
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Value::Map(map) => {
                // Handle map access
            }
            // ... other complex type access
            _ => None,
        }
    }
    
    pub fn compare_complex(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Complex type comparison logic
    }
}
```

#### 5.2 Query Planner Updates
```rust
// src/query/planner.rs
impl QueryPlanner {
    pub fn plan_complex_type_query(&self, query: &Query, schema: &TableSchema) -> Result<ExecutionPlan> {
        // Handle queries involving complex types
        // - UDT field access
        // - Collection filtering
        // - Nested operations
    }
}
```

### Phase 6: Testing and Validation (Week 6-7)

#### 6.1 Comprehensive Test Suite
```rust
// tests/complex_types_integration_test.rs
#[cfg(test)]
mod tests {
    #[test]
    fn test_udt_roundtrip() {
        // Test UDT serialization/deserialization
    }
    
    #[test]
    fn test_nested_collections() {
        // Test list<map<text, int>>
    }
    
    #[test]
    fn test_cassandra_compatibility() {
        // Test with real Cassandra data files
    }
    
    #[test]
    fn test_schema_loading() {
        // Test complex schema loading
    }
    
    #[test]
    fn test_performance() {
        // Performance benchmarks
    }
}
```

#### 6.2 Cassandra Compatibility Testing
```bash
# tests/cassandra_compatibility/
├── setup_cassandra.sh         # Setup test Cassandra instance
├── generate_test_data.cql      # Create complex type test data
├── export_sstables.sh          # Export SSTable files
└── compatibility_test.rs       # Verify we can read Cassandra data
```

### Phase 7: Performance Optimization (Week 7-8)

#### 7.1 Profiling and Optimization
```rust
// src/performance/profiler.rs
pub struct ComplexTypeProfiler {
    parse_times: HashMap<String, Duration>,
    serialize_times: HashMap<String, Duration>,
    memory_usage: HashMap<String, usize>,
}

impl ComplexTypeProfiler {
    pub fn profile_parsing<T>(&mut self, type_name: &str, f: impl FnOnce() -> T) -> T;
    pub fn generate_report(&self) -> PerformanceReport;
}
```

#### 7.2 Memory Optimizations
```rust
// src/types_enhanced.rs - Add memory optimization features
impl Value {
    pub fn estimate_memory_usage(&self) -> usize {
        // Calculate memory footprint
    }
    
    pub fn compact(&mut self) {
        // Compact memory representation
    }
}
```

## Migration Steps

### Step 1: Prepare Codebase
```bash
# 1. Create feature flag for complex types
cargo build --features "complex-types"

# 2. Add new dependencies if needed
# Update Cargo.toml with any new dependencies

# 3. Create compatibility shims
# Ensure existing code continues to work
```

### Step 2: Gradual Rollout
```rust
// src/config.rs
pub struct Config {
    pub enable_complex_types: bool,
    pub complex_type_features: ComplexTypeFeatures,
}

pub struct ComplexTypeFeatures {
    pub enable_udts: bool,
    pub enable_nested_collections: bool,
    pub enable_tuples: bool,
    pub max_nesting_depth: usize,
}
```

### Step 3: API Evolution
```rust
// Maintain backward compatibility
pub mod types {
    // Re-export enhanced types with compatibility layer
    pub use crate::types_enhanced::*;
    
    // Deprecated but still functional
    #[deprecated(note = "Use types_enhanced::Value instead")]
    pub type OldValue = crate::types_legacy::Value;
}
```

### Step 4: Documentation Updates
1. Update API documentation
2. Create migration guide
3. Add complex type examples
4. Update architecture documentation

## Risk Mitigation

### Backward Compatibility Risks
- **Risk**: Breaking existing code
- **Mitigation**: Comprehensive compatibility layer and feature flags

### Performance Risks
- **Risk**: Performance regression
- **Mitigation**: Extensive benchmarking and optimization

### Data Corruption Risks
- **Risk**: Incorrect parsing of binary data
- **Mitigation**: Extensive testing with real Cassandra data

### Memory Usage Risks
- **Risk**: Increased memory consumption
- **Mitigation**: Memory profiling and optimization

## Success Metrics

### Functional Metrics
1. **Compatibility**: 100% compatibility with existing simple types
2. **Complex Types**: Support for all Cassandra 5+ complex types
3. **Roundtrip**: Perfect serialize/deserialize roundtrip fidelity

### Performance Metrics
1. **Parse Speed**: <10% regression on simple types, <2x overhead for complex types
2. **Memory Usage**: <20% increase in memory usage
3. **Throughput**: Maintain >90% of current throughput

### Quality Metrics
1. **Test Coverage**: >95% code coverage
2. **Documentation**: Complete API documentation
3. **Compatibility**: Pass all Cassandra compatibility tests

## Timeline Summary

| Week | Phase | Key Deliverables |
|------|--------|------------------|
| 1-2  | Foundation | Type compatibility layer, error handling |
| 2-3  | Parser Integration | Unified parser, type registry |
| 3-4  | Schema Enhancement | Enhanced schema loading, type resolution |
| 4-5  | Storage Integration | Enhanced SSTable reader/writer |
| 5-6  | Query Integration | Complex type operations, query planning |
| 6-7  | Testing | Comprehensive test suite, Cassandra compatibility |
| 7-8  | Optimization | Performance tuning, memory optimization |

## Post-Integration Tasks

### Documentation
1. Update README with complex type examples
2. Create comprehensive API documentation
3. Write migration guide for existing users
4. Document performance characteristics

### Monitoring and Maintenance
1. Set up performance monitoring
2. Create alerting for compatibility issues
3. Plan regular compatibility testing
4. Establish maintenance procedures

### Future Enhancements
1. Advanced query optimizations
2. Custom type plugins
3. Enhanced tooling and debugging
4. Integration with other Cassandra features

This integration plan ensures a smooth transition to full complex type support while maintaining backward compatibility and system stability.