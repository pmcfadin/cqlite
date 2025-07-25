# CQLite Schema Examples

This directory contains various CQL schema examples demonstrating different complexity levels and features supported by CQLite. Each schema serves as a reference for understanding CQLite's capabilities and Cassandra compatibility.

## Schema Organization

### 📁 Simple Schemas (Beginner Level)

#### `example_schema.cql`
**Purpose:** Basic table structure with simple data types  
**Features:** 
- Primary key definition
- Basic column types (uuid, text, timestamp)
- Map data type usage
- Simple clustering

**Use Case:** Perfect for getting started with CQLite and understanding basic table structures.

**Cassandra Version:** Compatible with Cassandra 3.0+

```cql
CREATE TABLE example.users (
    user_id uuid PRIMARY KEY,
    username text,
    email text,
    created_at timestamp,
    profile_data map<text, text>
);
```

### 📁 Complex Schemas (Advanced Level)

#### `complex_schema.cql`
**Purpose:** Advanced e-commerce schema with complex data types  
**Features:**
- Composite partition keys
- Complex data types (list, map, frozen types)
- Clustering order configuration
- Nested collection types

**Use Case:** Real-world e-commerce applications requiring complex data relationships.

**Cassandra Version:** Compatible with Cassandra 4.0+

```cql
CREATE TABLE ecommerce.orders (
    order_id uuid,
    customer_id uuid,
    created_at timestamp,
    status text,
    total_amount decimal,
    items list<frozen<map<text, text>>>,
    shipping_address frozen<map<text, text>>,
    PRIMARY KEY ((customer_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
```

## JSON Schema Definitions

### `example_schema.json`
**Purpose:** JSON representation of the basic user schema  
**Features:**
- Partition key definitions
- Clustering key specifications
- Column type mappings
- Nullable field indicators

**Use Case:** Integration with applications requiring schema metadata in JSON format.

### `schema_example.json`
**Purpose:** Extended JSON schema with metadata  
**Features:**
- Comprehensive column definitions
- Default value specifications
- Table comments and versioning
- Cassandra version compatibility markers

**Use Case:** Production applications requiring detailed schema metadata and version tracking.

## Schema Complexity Levels

### 🟢 Simple (Beginner)
- **Files:** `example_schema.cql`, `example_schema.json`
- **Concepts:** Basic data types, simple keys
- **Prerequisites:** Basic CQL knowledge
- **Time to understand:** 10-15 minutes

### 🟡 Intermediate (Not currently represented)
- **Concepts:** User-defined types, secondary indexes, materialized views
- **Prerequisites:** Solid CQL foundation
- **Time to understand:** 30-45 minutes

### 🔴 Complex (Advanced)
- **Files:** `complex_schema.cql`, `schema_example.json`
- **Concepts:** Complex collections, composite keys, advanced clustering
- **Prerequisites:** Advanced CQL knowledge, production experience
- **Time to understand:** 1-2 hours

## Usage Instructions

### Testing Schemas with CQLite

1. **Validate Schema Syntax:**
   ```bash
   cargo run --bin cqlite-cli -- validate examples/schemas/example_schema.cql
   ```

2. **Parse Complex Types:**
   ```bash
   cargo run --bin cqlite-cli -- parse examples/schemas/complex_schema.cql --verbose
   ```

3. **JSON Schema Validation:**
   ```bash
   cargo run --bin cqlite-cli -- schema validate examples/schemas/example_schema.json
   ```

### Integration Examples

#### Rust Integration
```rust
use cqlite_core::schema::SchemaParser;

let schema = SchemaParser::from_file("examples/schemas/example_schema.cql")?;
let table_def = schema.parse()?;
println!("Table: {} with {} columns", table_def.name, table_def.columns.len());
```

#### JSON Processing
```rust
use serde_json;
use cqlite_core::schema::JsonSchema;

let schema_content = std::fs::read_to_string("examples/schemas/example_schema.json")?;
let schema: JsonSchema = serde_json::from_str(&schema_content)?;
```

## Dependencies and Requirements

### Cassandra Version Compatibility

| Schema File | Cassandra 3.x | Cassandra 4.x | Cassandra 5.x | Notes |
|-------------|---------------|---------------|---------------|-------|
| `example_schema.cql` | ✅ | ✅ | ✅ | Basic types only |
| `complex_schema.cql` | ⚠️ | ✅ | ✅ | Requires frozen collections |
| `example_schema.json` | ✅ | ✅ | ✅ | JSON metadata format |
| `schema_example.json` | ✅ | ✅ | ✅ | Extended metadata |

### CQLite Feature Support

- **Basic Data Types:** All schemas fully supported
- **Collections:** List, Map, Set types supported in complex schemas
- **Frozen Types:** Advanced frozen collection support
- **JSON Schema:** Full JSON schema parsing and validation
- **Complex Keys:** Composite partition and clustering keys

## Validation and Testing

All schemas in this directory have been validated for:

1. **Syntax Correctness:** Valid CQL syntax
2. **Type Safety:** Proper data type usage
3. **Key Structure:** Valid partition and clustering key definitions
4. **Cassandra Compatibility:** Version-specific feature usage
5. **CQLite Support:** Full compatibility with CQLite parser

## Contributing New Schemas

When adding new schema examples:

1. **Naming Convention:** Use descriptive names indicating complexity level
2. **Documentation:** Include comprehensive comments in CQL files
3. **JSON Companion:** Provide JSON schema definition when applicable
4. **Testing:** Validate with CQLite parser before committing
5. **README Update:** Add entry to this README with proper categorization

## Schema Evolution Examples

### Migration Patterns
- Adding columns: `ALTER TABLE` examples
- Index creation: Secondary index patterns
- Type modifications: Safe type evolution paths

### Version Management
- Schema versioning strategies
- Backward compatibility considerations
- Migration script examples

## Performance Considerations

### Schema Design Best Practices
- Partition key selection for even distribution
- Clustering key optimization for query patterns
- Collection size limitations and best practices
- Denormalization strategies for read performance

### Indexing Strategies
- When to use secondary indexes
- Materialized view alternatives
- Custom index implementations

---

## Quick Reference

| Need | Use This Schema | Complexity | Time Required |
|------|----------------|------------|---------------|
| Learning CQL basics | `example_schema.cql` | 🟢 Simple | 15 min |
| Real app example | `complex_schema.cql` | 🔴 Complex | 1-2 hours |
| JSON integration | `example_schema.json` | 🟢 Simple | 10 min |
| Production metadata | `schema_example.json` | 🟡 Intermediate | 30 min |

For more examples and advanced patterns, see the main [CQLite documentation](../../README.md) and [technical guides](../../docs/).