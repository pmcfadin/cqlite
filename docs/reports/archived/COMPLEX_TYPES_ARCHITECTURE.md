# Complex Types Architecture for CQLite

## Overview

This document outlines the architecture for Cassandra complex types support in CQLite, designed for exact binary compatibility with Cassandra 5+ SSTable format ('oa' format).

## Design Principles

### 1. Binary Compatibility First
- All type representations match Cassandra's exact binary format
- Type metadata preserved for proper serialization/deserialization
- Support for all Cassandra 5+ type identifiers and encodings

### 2. Performance-Oriented Design
- Efficient memory layout for complex types
- Lazy parsing where possible
- Zero-copy operations for fixed-size types
- Optimized traversal for nested structures

### 3. Type Safety and Validation
- Strong type specifications prevent serialization errors
- Runtime type validation during parsing
- Clear error messages for format violations

## Core Architecture Components

### 1. Enhanced Value Enum (`types_enhanced.rs`)

The `Value` enum has been redesigned to support complex types with proper metadata:

```rust
pub enum Value {
    // Primitive types (unchanged)
    Boolean(bool),
    Integer(i32),
    // ... other primitives
    
    // Enhanced complex types with metadata
    List(CollectionValue),
    Set(CollectionValue),
    Map(MapValue),
    Tuple(TupleValue),
    Udt(UdtValue),
    Frozen(FrozenValue),
    
    // Additional Cassandra types
    Varint(Vec<u8>),
    Decimal { scale: i32, unscaled: Vec<u8> },
    Duration { months: i32, days: i32, nanoseconds: i64 },
    // ... others
}
```

### 2. Type Specification System (`CqlTypeSpec`)

The `CqlTypeSpec` enum provides complete type metadata for complex types:

```rust
pub enum CqlTypeSpec {
    // Primitive types
    Boolean, Int, Text, // ... others
    
    // Collection types with element type info
    List(Box<CqlTypeSpec>),
    Set(Box<CqlTypeSpec>),
    Map(Box<CqlTypeSpec>, Box<CqlTypeSpec>),
    
    // Complex types
    Tuple(Vec<CqlTypeSpec>),
    Udt {
        keyspace: Option<String>,
        name: String,
        fields: Vec<(String, CqlTypeSpec)>,
    },
    Frozen(Box<CqlTypeSpec>),
}
```

### 3. Complex Value Containers

#### CollectionValue
```rust
pub struct CollectionValue {
    pub element_type: CqlTypeSpec,
    pub values: Vec<Value>,
}
```
- Preserves element type for proper serialization
- Supports both List and Set semantics
- Efficient iteration and access patterns

#### MapValue
```rust
pub struct MapValue {
    pub key_type: CqlTypeSpec,
    pub value_type: CqlTypeSpec,
    pub entries: Vec<(Value, Value)>,
}
```
- Maintains key and value type specifications
- Preserves insertion order (Vec vs HashMap)
- Compatible with Cassandra's map representation

#### TupleValue
```rust
pub struct TupleValue {
    pub field_types: Vec<CqlTypeSpec>,
    pub values: Vec<Value>,
}
```
- Fixed-size heterogeneous type support
- Type-safe field access by index
- Efficient memory layout

#### UdtValue
```rust
pub struct UdtValue {
    pub type_name: String,
    pub keyspace: Option<String>,
    pub field_specs: Vec<(String, CqlTypeSpec)>,
    pub field_values: HashMap<String, Value>,
}
```
- Preserves field ordering from type definition
- Supports nullable fields (absent from HashMap = null)
- Keyspace-aware type resolution

#### FrozenValue
```rust
pub struct FrozenValue {
    pub inner_type: CqlTypeSpec,
    pub value: Box<Value>,
}
```
- Immutable wrapper for any complex type
- Preserves inner type specification
- Special serialization handling

## Binary Format Compatibility

### Type ID Mapping

The extended type ID system maps directly to Cassandra's binary format:

```rust
pub enum ExtendedCqlTypeId {
    // Primitive types (0x00-0x1F)
    Boolean = 0x04,
    Int = 0x09,
    BigInt = 0x02,
    // ... others
    
    // Collection types (0x20-0x2F)
    List = 0x20,
    Map = 0x21,
    Set = 0x22,
    
    // Complex types (0x30-0x3F)
    Udt = 0x30,
    Tuple = 0x31,
}
```

### Serialization Format

#### Collections (List/Set)
```
[Type ID: u8][Element Type Spec][Count: VInt][Element 1 Length: VInt][Element 1 Data]...
```

#### Maps
```
[Type ID: u8][Key Type Spec][Value Type Spec][Count: VInt][Key 1 Length: VInt][Key 1 Data][Value 1 Length: VInt][Value 1 Data]...
```

#### Tuples
```
[Type ID: u8][Field Count: VInt][Field Type 1 Spec]...[Field 1 Length: VInt][Field 1 Data]...
```

#### UDTs
```
[Type ID: u8][Keyspace Length: VInt][Keyspace][Name Length: VInt][Name][Field Count: VInt][Field Name 1 Length: VInt][Field Name 1][Field Type 1 Spec][Field 1 Length: VInt][Field 1 Data]...
```

## Parser Architecture (`complex_types.rs`)

### Recursive Type Parsing

The parser handles nested type specifications recursively:

```rust
pub fn parse_cql_type_spec(input: &[u8]) -> IResult<&[u8], CqlTypeSpec>
```

This function:
1. Reads the type ID byte
2. Dispatches to appropriate type-specific parser
3. Recursively parses nested type specifications
4. Returns complete type metadata

### Value Parsing with Type Context

```rust
pub fn parse_complex_cql_value(input: &[u8], type_spec: &CqlTypeSpec) -> IResult<&[u8], Value>
```

This function:
1. Uses type specification to guide parsing
2. Handles null values appropriately for each type
3. Recursively parses nested values
4. Maintains type safety throughout parsing

### Length-Prefixed Parsing

All variable-length data uses VInt length prefixes:
- Enables proper boundary detection
- Supports null value encoding (length = 0)
- Compatible with Cassandra's format

## Integration with Existing Systems

### Schema System Integration

The schema system (`schema/mod.rs`) has been enhanced to support complex types:

```rust
pub enum CqlType {
    // Enhanced with proper nested type support
    List(Box<CqlType>),
    Set(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
    Tuple(Vec<CqlType>),
    Udt(String, Vec<(String, CqlType)>),
    Frozen(Box<CqlType>),
}
```

### SSTable Reader Integration

The parser integrates with existing SSTable infrastructure:
1. Uses existing VInt parsing utilities
2. Maintains error handling patterns
3. Supports streaming/chunked parsing
4. Preserves performance characteristics

### Query Engine Integration

The enhanced types integrate seamlessly with the query engine:
1. Type-safe operations on complex values
2. Efficient nested field access
3. Proper comparison and ordering semantics
4. Memory-efficient value transformations

## Performance Considerations

### Memory Layout

1. **Zero-Copy Parsing**: Fixed-size types use direct byte slice references
2. **Lazy Evaluation**: Complex types parse metadata first, values on demand
3. **Efficient Containers**: Use Vec instead of HashMap where ordering matters
4. **Type Reuse**: Share type specifications across multiple values

### Parsing Optimizations

1. **Type Caching**: Cache parsed type specifications for reuse
2. **Streaming Support**: Parse values without loading entire collections
3. **Boundary Checking**: Efficient bounds checking using nom combinators
4. **Error Recovery**: Graceful handling of malformed data

### Serialization Optimizations

1. **Buffer Reuse**: Pre-allocate buffers for known size estimates
2. **Batch Operations**: Efficiently serialize multiple values
3. **Type Elision**: Skip redundant type information where possible
4. **Compression Ready**: Format compatible with SSTable compression

## Validation and Error Handling

### Type Validation

1. **Schema Validation**: Ensure values match declared types
2. **Nested Validation**: Recursively validate complex type contents
3. **Constraint Checking**: Enforce collection size limits and type constraints
4. **Format Validation**: Verify binary format compliance

### Error Recovery

1. **Graceful Degradation**: Convert unknown types to blobs when possible
2. **Partial Parsing**: Continue parsing valid portions of malformed data
3. **Detailed Errors**: Provide context about parsing failures
4. **Corruption Detection**: Identify and report data corruption

## Testing Strategy

### Unit Tests

1. **Type System Tests**: Verify type specification parsing and serialization
2. **Value Tests**: Test all value types with various data combinations
3. **Roundtrip Tests**: Ensure serialize/deserialize consistency
4. **Edge Case Tests**: Handle null values, empty collections, large data

### Integration Tests

1. **SSTable Compatibility**: Test with real Cassandra SSTable files
2. **Schema Compatibility**: Verify with complex schema definitions
3. **Performance Tests**: Measure parsing and serialization performance
4. **Memory Tests**: Validate memory usage patterns

### Compatibility Tests

1. **Cassandra Compatibility**: Test against multiple Cassandra versions
2. **Format Validation**: Verify exact binary format compliance
3. **Interoperability**: Ensure data written by Cassandra can be read
4. **Version Compatibility**: Support format evolution

## Migration Strategy

### Phased Implementation

1. **Phase 1**: Basic complex type support (current implementation)
2. **Phase 2**: Performance optimizations and caching
3. **Phase 3**: Advanced features (custom types, format extensions)
4. **Phase 4**: Query engine integration and optimization

### Backward Compatibility

1. **Existing Code**: All existing code continues to work
2. **Simple Types**: No changes to primitive type handling
3. **API Stability**: Additive changes only to public APIs
4. **Data Compatibility**: Existing data files remain readable

## Future Enhancements

### Advanced Features

1. **Custom Types**: Support for user-defined custom type handlers
2. **Type Evolution**: Handle schema evolution and type changes
3. **Compression**: Type-aware compression for better ratios
4. **Indexing**: Efficient indexing of complex type contents

### Performance Improvements

1. **SIMD Optimizations**: Vectorized parsing for large collections
2. **Memory Mapping**: Direct memory access for large values
3. **Parallel Parsing**: Multi-threaded parsing of independent values
4. **Cache Optimization**: Intelligent caching of parsed type metadata

### Tooling and Debugging

1. **Type Introspection**: Runtime type information and debugging
2. **Format Validation**: Tools to validate SSTable format compliance
3. **Performance Profiling**: Detailed performance analysis tools
4. **Schema Tools**: Advanced schema management and migration tools

## Conclusion

This architecture provides a robust foundation for Cassandra complex types support in CQLite. The design emphasizes:

1. **Exact Compatibility**: Binary-level compatibility with Cassandra
2. **Performance**: Efficient parsing and memory usage
3. **Type Safety**: Strong typing prevents errors
4. **Extensibility**: Easy to add new types and features
5. **Integration**: Seamless integration with existing systems

The implementation is production-ready and provides a solid base for future enhancements while maintaining backward compatibility and performance requirements.