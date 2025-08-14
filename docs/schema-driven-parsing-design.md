# Schema-Driven Parsing Architecture Design

## Overview

This document describes the design and implementation of the schema-driven parsing architecture for CQLite, addressing Issue #28's requirement to eliminate type guessing and ensure all modern parsing paths use explicit schema context with proper comparator support.

## Problem Statement

The previous parsing implementation suffered from fundamental architectural flaws:

1. **Schema-blind parsing** with type guessing and hardcoded assumptions
2. **Frozen type debug string abuse** instead of proper type preservation
3. **Generic column fabrication** with placeholder names and types
4. **Default blob comparator fallbacks** instead of schema-derived comparators
5. **No support for multi-component keys** with proper comparator ordering

These issues prevented correct Cassandra parsing for complex types, collections, UDTs, and proper byte-comparable key ordering.

## Architecture Solution

### Core Components

#### 1. SchemaRegistry Enhancement

```rust
pub struct SchemaRegistry {
    // ... existing fields ...
}

impl SchemaRegistry {
    /// Get the complete schema context for parsing operations
    pub async fn get_parsing_context(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<ParsingContext> {
        let schema = self.get_schema(keyspace, table).await?;
        let partition_comparators = self.get_partition_key_comparator(keyspace, table).await?;
        let clustering_comparators = self.get_clustering_key_comparator(keyspace, table).await?;
        let column_comparators = self.get_table_comparators(keyspace, table).await?;
        
        Ok(ParsingContext {
            schema,
            partition_comparators,
            clustering_comparators,
            column_comparators,
        })
    }
}
```

**Key Features:**
- Centralized schema management with validation
- Automatic comparator derivation from CQL types
- Complete parsing context creation
- Schema versioning and change tracking

#### 2. ParsingContext

```rust
#[derive(Debug, Clone)]
pub struct ParsingContext {
    /// The complete table schema
    pub schema: TableSchema,
    /// Comparators for partition key components
    pub partition_comparators: Vec<ComparatorType>,
    /// Comparators for clustering key components
    pub clustering_comparators: Vec<ComparatorType>,
    /// Comparators for all columns by name
    pub column_comparators: HashMap<String, ComparatorType>,
}
```

**Threading Pattern:**
The `ParsingContext` is threaded through all parsing operations, ensuring that:
- Every value parsing operation has explicit type information
- Comparators are available for all key components
- No fallback to type guessing is possible

#### 3. SchemaParser

```rust
pub struct SchemaParser {
    context: ParsingContext,
}

impl SchemaParser {
    /// Parse partition keys using schema's partition key comparators
    pub fn parse_partition_key(&self, data: &[u8]) -> Result<Vec<Value>> {
        // Uses self.context.partition_comparators for each component
    }
    
    /// Parse clustering keys using schema's clustering key comparators
    pub fn parse_clustering_keys(&self, data: &[u8]) -> Result<Vec<Value>> {
        // Uses self.context.clustering_comparators for each component
    }
    
    /// Parse column value using schema's column type
    pub fn parse_column_value(&self, column_name: &str, data: &[u8]) -> Result<Value> {
        // Uses self.context.column_comparators[column_name]
    }
}
```

**Design Principles:**
- **Strict schema enforcement**: All operations require complete schema context
- **No type guessing**: Every parsing decision is schema-driven
- **Comparator-driven decoding**: Uses exact comparator types for all values
- **Error on missing schema**: Fails fast when schema information is incomplete

#### 4. SchemaAwareReader

```rust
pub struct SchemaAwareReader {
    schema_parser: SchemaParser,
    context: ParsingContext,
    // ... other fields ...
}
```

**Integration Points:**
- Wraps existing SSTableReader for low-level file operations
- Uses SchemaParser for all value parsing
- Provides schema-aware API for get/scan operations
- Validates keys against schema before processing

## Comparator Threading Design

### Comparator Derivation

```rust
impl ComparatorType {
    pub fn from_cql_type(cql_type: &CqlType) -> Result<Self> {
        match cql_type {
            CqlType::Int => Ok(ComparatorType::Int),
            CqlType::Text => Ok(ComparatorType::Text),
            CqlType::List(elem_type) => {
                let elem_comparator = Self::from_cql_type(elem_type)?;
                Ok(ComparatorType::List(Box::new(elem_comparator)))
            }
            CqlType::Frozen(inner_type) => {
                let inner_comparator = Self::from_cql_type(inner_type)?;
                Ok(ComparatorType::Frozen(Box::new(inner_comparator)))
            }
            // ... all other types
        }
    }
}
```

### Threading Pattern

1. **Schema Registration**:
   ```
   SchemaRegistry::register_schema() 
   -> Validates schema completeness
   -> Derives comparators for all columns
   ```

2. **Parsing Context Creation**:
   ```
   SchemaRegistry::get_parsing_context()
   -> Creates complete ParsingContext
   -> Includes all necessary comparators
   ```

3. **Parser Initialization**:
   ```
   SchemaParser::new(context)
   -> Validates context completeness
   -> Stores context for all operations
   ```

4. **Value Parsing**:
   ```
   SchemaParser::parse_*()
   -> Uses context.comparators
   -> No fallback to defaults
   ```

### Multi-Component Key Handling

```rust
impl SchemaParser {
    pub fn parse_partition_key(&self, data: &[u8]) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        let mut offset = 0;

        // Process each component with its specific comparator
        for (idx, comparator) in self.context.partition_comparators.iter().enumerate() {
            let key_column = &self.context.schema.partition_keys[idx];
            let (value, consumed) = self.parse_value_with_comparator(
                &data[offset..],
                comparator,
                &key_column.data_type,
            )?;
            values.push(value);
            offset += consumed;
        }

        Ok(values)
    }
}
```

**Key Features:**
- Each component uses its exact comparator type
- Supports arbitrary number of components
- Maintains byte-comparable ordering
- Handles variable-length components correctly

## Type System Integration

### Supported Types

| CQL Type | ComparatorType | Features |
|----------|----------------|----------|
| `int`, `bigint`, etc. | `Int`, `BigInt` | Native ordering |
| `text`, `ascii` | `Text` | Lexicographic ordering |
| `list<T>` | `List(Box<ComparatorType>)` | Element-wise comparison |
| `set<T>` | `Set(Box<ComparatorType>)` | Set equality only |
| `map<K,V>` | `Map(Box<K>, Box<V>)` | Key-value pair comparison |
| `tuple<T1,T2,..>` | `Tuple(Vec<ComparatorType>)` | Field-wise comparison |
| `frozen<T>` | `Frozen(Box<ComparatorType>)` | Byte-wise comparison |
| UDTs | `Udt{...}` | Field-wise comparison |

### Nested Type Support

```rust
// Example: frozen<map<text,list<int>>>
ComparatorType::Frozen(Box::new(
    ComparatorType::Map(
        Box::new(ComparatorType::Text),
        Box::new(ComparatorType::List(
            Box::new(ComparatorType::Int)
        ))
    )
))
```

## Migration from Legacy Paths

### Before (Type Guessing)

```rust
// OLD: Type guessing with blob fallback
let comparator = detect_type_from_data(data)
    .unwrap_or(ComparatorType::Blob);
```

### After (Schema-Driven)

```rust
// NEW: Strict schema requirement
let comparator = context.get_column_comparator(column_name)
    .ok_or_else(|| Error::Schema("Schema required for parsing"))?;
```

### Legacy Path Handling

- **Marked as DEPRECATED**: All blob fallback paths are commented and marked
- **Error on missing schema**: Modern paths fail fast without schema context
- **Preserved for compatibility**: Legacy 3.x paths remain unchanged
- **Clear migration path**: Use SchemaAwareReader for all new code

## Testing Strategy

### Unit Tests

1. **Multi-component keys**: Test 2+ partition/clustering components with mixed types
2. **Nested collections**: Test deeply nested frozen structures
3. **Ordering equivalence**: Verify byte-comparable vs typed ordering
4. **Error handling**: Test missing schema scenarios

### Integration Tests

1. **Three representative tables**:
   - Simple table (basic types)
   - Collections table (complex nested types)
   - UDT/Frozen table (user-defined and frozen types)

2. **Parity validation**: Zero-diff comparison with sstabledump output

### CI Validation

- **Matrix testing**: Multiple Cassandra versions, formats, compression
- **Zero-tolerance mode**: Fails on first parsing difference
- **Artifact generation**: Parity reports and test outputs

## Performance Characteristics

### Computational Complexity

- **Parsing**: O(n) where n is data size (same as before)
- **Schema lookup**: O(1) with HashMap-based comparator cache
- **Memory overhead**: Minimal - comparators are lightweight enums

### Optimizations

1. **Comparator caching**: Pre-computed and stored in ParsingContext
2. **Schema validation**: One-time validation during registration
3. **Format-specific optimizations**: Available for BIG/BTI formats

## Public API Changes

### New APIs

```rust
// Schema registry with parsing context
pub async fn get_parsing_context(&self, keyspace: &str, table: &str) -> Result<ParsingContext>

// Schema-driven parser
pub struct SchemaParser { ... }

// Schema-aware reader  
pub struct SchemaAwareReader { ... }
```

### Breaking Changes

- **Modern parsing requires schema**: No longer falls back to blob comparator
- **SchemaAwareReader recommended**: For all new parsing operations
- **Legacy paths preserved**: Existing 3.x compatibility maintained

## Future Enhancements

1. **BTI optimizations**: Leverage trie structure for key parsing
2. **Streaming parsing**: Support for large values with schema context
3. **Schema evolution**: Handle schema changes gracefully
4. **Custom comparators**: Support for user-defined comparison logic

## Conclusion

The schema-driven parsing architecture eliminates type guessing while providing:

- **Correctness**: All parsing decisions are schema-driven
- **Performance**: No regression, with optimization opportunities
- **Maintainability**: Clear separation between legacy and modern paths
- **Extensibility**: Support for all Cassandra types and future enhancements

This design satisfies all Issue #28 acceptance criteria and provides a solid foundation for reliable Cassandra 5.0 data reading.