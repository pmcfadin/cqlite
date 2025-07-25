# CQL Parser Implementation

## Overview

This document describes the implementation of the CQL (Cassandra Query Language) parser for CREATE TABLE statements in the cqlite project. The parser extracts table schema information including table names, column definitions, partition keys, clustering keys, and type information.

## Implementation Details

### Location
- **Main parser**: `cqlite-core/src/schema/cql_parser.rs`
- **Integration**: `cqlite-core/src/schema/mod.rs`

### Key Features

#### 1. Comprehensive CQL Support
- **Table Names**: Simple (`users`) and qualified (`myapp.users`) table names
- **Identifiers**: Both unquoted and quoted identifiers (with spaces)
- **Keywords**: Case-insensitive parsing (`CREATE`, `create`, `Create`)
- **Comments**: Handles CQL comments and whitespace

#### 2. Data Type Support
- **Primitive Types**: All Cassandra primitive types (text, bigint, uuid, etc.)
- **Collection Types**: list<type>, set<type>, map<key, value>
- **Complex Types**: tuple<type1, type2, ...>
- **User-Defined Types**: Custom UDT references
- **Frozen Types**: frozen<collection_type> for immutable collections
- **Nested Types**: frozen<list<frozen<set<uuid>>>>

#### 3. Primary Key Handling
- **Inline Definition**: `id uuid PRIMARY KEY`
- **Constraint Definition**: `PRIMARY KEY (partition_key, clustering_key)`
- **Composite Keys**: `PRIMARY KEY ((pk1, pk2), ck1, ck2)`
- **Single Partition**: `PRIMARY KEY (partition_key)`

#### 4. Schema Extraction
- **Table Metadata**: Keyspace, table name, comments
- **Column Information**: Name, data type, nullable, defaults
- **Key Structure**: Partition keys with positions, clustering keys with ordering
- **Type Mapping**: CQL types to internal CqlTypeId enum

## API Reference

### Core Functions

#### `parse_cql_schema(cql: &str) -> Result<TableSchema>`
Parses a complete CQL CREATE TABLE statement and returns a validated TableSchema.

```rust
use cqlite_core::schema::parse_cql_schema;

let cql = r#"
    CREATE TABLE myapp.users (
        id uuid PRIMARY KEY,
        name text,
        email text,
        tags set<text>,
        metadata map<text, text>
    )
"#;

let schema = parse_cql_schema(cql)?;
assert_eq!(schema.keyspace, "myapp");
assert_eq!(schema.table, "users");
assert_eq!(schema.columns.len(), 5);
```

#### `extract_table_name(cql: &str) -> Result<(Option<String>, String)>`
Extracts table name and optional keyspace from CQL without full parsing.

```rust
use cqlite_core::schema::extract_table_name;

let cql = "CREATE TABLE IF NOT EXISTS myapp.orders (id bigint PRIMARY KEY)";
let (keyspace, table) = extract_table_name(cql)?;
assert_eq!(keyspace, Some("myapp".to_string()));
assert_eq!(table, "orders");
```

#### `cql_type_to_type_id(cql_type: &str) -> Result<CqlTypeId>`
Converts CQL type strings to internal type identifiers.

```rust
use cqlite_core::schema::cql_type_to_type_id;
use cqlite_core::parser::types::CqlTypeId;

assert_eq!(cql_type_to_type_id("text")?, CqlTypeId::Varchar);
assert_eq!(cql_type_to_type_id("bigint")?, CqlTypeId::BigInt);
assert_eq!(cql_type_to_type_id("list<text>")?, CqlTypeId::List);
```

#### `table_name_matches() -> bool`
Checks if table names match with optional keyspace wildcarding.

```rust
use cqlite_core::schema::table_name_matches;

// Exact match
assert!(table_name_matches(
    &Some("ks".to_string()), "users",
    &Some("ks".to_string()), "users"
));

// Wildcard keyspace match
assert!(table_name_matches(
    &Some("ks".to_string()), "users",
    &None, "users"
));
```

### SchemaManager Integration

#### `parse_and_register_cql_schema(&mut self, cql: &str) -> Result<&TableSchema>`
Parses CQL and registers the schema with the schema manager.

```rust
let mut schema_manager = SchemaManager::new(storage, &config).await?;
let schema = schema_manager.parse_and_register_cql_schema(cql)?;
```

#### `find_schema_by_table(&self, keyspace: &Option<String>, table: &str) -> Option<&TableSchema>`
Finds a registered schema by table name with optional keyspace matching.

#### `extract_table_info(&self, cql: &str) -> Result<(Option<String>, String)>`
Wrapper for table name extraction.

#### `cql_type_to_internal(&self, cql_type: &str) -> Result<CqlTypeId>`
Wrapper for type conversion.

## Supported CQL Syntax

### Basic Table Definition
```sql
CREATE TABLE users (
    id uuid PRIMARY KEY,
    name text,
    email text
)
```

### Qualified Table Names
```sql
CREATE TABLE myapp.user_profiles (
    user_id uuid PRIMARY KEY,
    profile_data text
)
```

### Complex Primary Keys
```sql
CREATE TABLE time_series (
    partition_key text,
    clustering_key timestamp,
    value double,
    PRIMARY KEY (partition_key, clustering_key)
)
```

### Composite Partition Keys
```sql
CREATE TABLE multi_tenant (
    tenant_id uuid,
    user_id uuid,
    timestamp timestamp,
    data text,
    PRIMARY KEY ((tenant_id, user_id), timestamp)
)
```

### Collection Types
```sql
CREATE TABLE complex_types (
    id uuid PRIMARY KEY,
    tags set<text>,
    metadata map<text, text>,
    coordinates list<double>,
    nested_data frozen<map<text, list<text>>>
)
```

### User-Defined Types
```sql
CREATE TABLE user_profiles (
    user_id uuid PRIMARY KEY,
    address address_type,
    preferences frozen<user_prefs>
)
```

### Tuple Types
```sql
CREATE TABLE coordinates (
    id uuid PRIMARY KEY,
    location tuple<double, double>,
    metadata tuple<text, int, boolean>
)
```

## Error Handling

The parser provides comprehensive error handling:

- **Parse Errors**: Invalid CQL syntax with detailed error messages
- **Type Errors**: Unknown or invalid CQL data types
- **Schema Validation**: Ensures partition keys exist, positions are valid
- **Fallback Parsing**: Simple extraction when full parsing fails

## Testing

The implementation includes comprehensive tests covering:

- **Basic Parsing**: Simple table definitions
- **Complex Types**: Collections, UDTs, tuples, frozen types
- **Primary Keys**: All variations of key definitions
- **Edge Cases**: Quoted identifiers, case sensitivity
- **Type Mapping**: All primitive types to internal format
- **Table Matching**: Various matching scenarios

## Integration

The CQL parser is integrated with:

- **Schema Module**: Part of the schema management system
- **Storage Engine**: Used for schema-aware SSTable reading
- **Query Engine**: Provides schema information for query planning
- **CLI Tools**: Schema extraction for user-facing tools

## Performance

- **Streaming Parser**: Built on nom combinators for efficient parsing
- **Memory Efficient**: Minimal allocations during parsing
- **Fallback Support**: Fast path for simple table name extraction
- **Caching**: Schema results can be cached by the SchemaManager

## Future-Proofing Strategy for Cassandra Evolution

### Overview
This section outlines our comprehensive strategy for maintaining CQL parser accuracy as Apache Cassandra evolves through versions 6.x and 7.x. Based on extensive research into Cassandra's evolution patterns, official grammar sources, and parser technology trade-offs, we recommend a hybrid approach that leverages both our current nom implementation and official ANTLR grammars.

### Key Research Findings

#### Official Grammar Sources
- **Apache Cassandra maintains an official ANTLR grammar** at `cassandra/src/antlr/Parser.g`
- **Community ANTLR4 grammar** available at `grammars-v4/cql3/CqlParser.g4`
- **Active maintenance**: Both grammars are actively maintained and reflect current CQL specifications

#### Cassandra Evolution Patterns
- **Version 5.0 features**: Storage Attached Indexes (SAI), Vector Search, new CQL functions, Dynamic Data Masking
- **Evolution trend**: Steady progression toward SQL-like features while maintaining distributed constraints
- **Grammar stability**: Core statement types (CREATE, SELECT, INSERT, UPDATE, DELETE) are stable with incremental enhancements

#### Parser Technology Analysis
- **nom advantages**: Native Rust, zero-copy parsing, superior performance, streaming support
- **ANTLR advantages**: Official grammar support, better tooling, easier complex grammar handling
- **Performance**: nom typically outperforms ANTLR in Rust environments

### Recommended Hybrid Architecture

#### Phase 1: Enhanced nom Parser (Current + 6 months)
1. **Extend current nom parser for full DML support**:
   - SELECT statement parsing (WHERE, ORDER BY, LIMIT, GROUP BY)
   - INSERT statement parsing (conditional operations, TTL, counter columns)
   - UPDATE statement parsing (SET clauses, IF conditions, TTL)
   - DELETE statement parsing (WHERE conditions, IF EXISTS)

2. **Modular parser architecture**:
   ```rust
   pub mod cql_parser {
       pub mod ddl;      // CREATE TABLE, ALTER TABLE, DROP TABLE
       pub mod dml;      // SELECT, INSERT, UPDATE, DELETE
       pub mod expr;     // WHERE clauses, functions, operators
       pub mod types;    // Data type parsing and validation
       pub mod common;   // Shared parsers (identifiers, keywords, etc.)
   }
   ```

3. **Expression parsing framework**:
   - WHERE clause expressions with all CQL operators
   - Function calls (built-in and user-defined)
   - Type casting and conversions
   - Collection operations (CONTAINS, IN, etc.)

#### Phase 2: ANTLR Integration Layer (6-12 months)
1. **Official grammar consumption**:
   - Automated pipeline to fetch and process official Cassandra ANTLR grammar
   - Grammar version compatibility checking
   - Automated test generation from grammar changes

2. **Hybrid validation approach**:
   ```rust
   pub trait CqlValidator {
       fn validate_against_official_grammar(&self, statement: &str) -> ValidationResult;
       fn parse_with_nom(&self, statement: &str) -> ParseResult;
       fn fallback_to_antlr(&self, statement: &str) -> ParseResult;
   }
   ```

3. **Grammar diff analysis**:
   - Automated detection of grammar changes between Cassandra versions
   - Impact analysis on existing nom parsers
   - Compatibility matrix generation

#### Phase 3: Advanced Features (12-18 months)
1. **Machine learning-assisted parsing**:
   - Pattern recognition for new syntax variations
   - Automatic grammar rule inference from examples
   - Confidence scoring for parsed statements

2. **Plugin architecture for extensions**:
   ```rust
   pub trait CqlExtension {
       fn name(&self) -> &str;
       fn supported_statements(&self) -> Vec<StatementType>;
       fn parse(&self, input: &str) -> ExtensionParseResult;
   }
   ```

### Implementation Strategy

#### Immediate Actions (Next 3 months)
1. **Extend nom parser for DML operations**:
   - SELECT: Support for complex WHERE clauses, JOINs (when added), subqueries
   - INSERT/UPDATE: Conditional operations, TTL, JSON support
   - DELETE: Conditional deletions, batch operations

2. **Create parser test harness**:
   - Comprehensive test suite covering all CQL versions
   - Fuzzing framework for edge case discovery
   - Performance benchmarking suite

3. **Establish grammar monitoring**:
   - CI pipeline to track Apache Cassandra grammar changes
   - Automated compatibility testing against new Cassandra releases
   - Alert system for breaking grammar changes

#### Medium-term Goals (3-12 months)
1. **ANTLR grammar integration**:
   ```bash
   # Automated grammar sync pipeline
   ./scripts/sync_grammar.sh --cassandra-version 6.0
   ./scripts/generate_tests.sh --from-grammar
   ./scripts/validate_compatibility.sh
   ```

2. **Error recovery and reporting**:
   - Advanced error messages with suggestions
   - Partial parsing for incomplete statements
   - Context-aware error recovery

3. **Performance optimization**:
   - Zero-copy parsing for large statements
   - Streaming parser for batch operations
   - Memory pool allocation for parse trees

#### Long-term Vision (12+ months)
1. **Unified CQL ecosystem**:
   - Parser as a service for other Rust Cassandra tools
   - WebAssembly bindings for browser-based tools
   - Language server protocol support for IDEs

2. **Predictive parsing**:
   - Machine learning model trained on Cassandra grammar evolution
   - Automatic suggestion of likely future syntax patterns
   - Proactive compatibility warnings

### Risk Mitigation

#### Grammar Evolution Risks
- **Monitoring**: Continuous tracking of Cassandra development branches
- **Fallback**: ANTLR parser as backup for unsupported syntax
- **Versioning**: Parser version matching to Cassandra versions

#### Performance Risks  
- **Benchmarking**: Regular performance regression testing
- **Profiling**: Memory and CPU usage monitoring
- **Optimization**: JIT compilation for hot parsing paths

#### Maintenance Risks
- **Automation**: Automated testing and grammar synchronization
- **Documentation**: Comprehensive developer guides and API documentation
- **Community**: Open-source contribution guidelines and review processes

### Success Metrics

#### Technical Metrics
- **Coverage**: 100% of official CQL grammar supported
- **Performance**: <1ms parsing time for typical statements
- **Accuracy**: <0.1% false positive/negative rate vs official parser
- **Compatibility**: Support for N-2 Cassandra versions

#### Operational Metrics
- **Update lag**: <30 days from Cassandra grammar changes to parser updates
- **Error recovery**: >95% of syntax errors provide actionable suggestions
- **Test coverage**: >99% statement coverage in test suite

### Resource Requirements

#### Development Resources
- **Senior Rust developer**: 40% time for nom parser development
- **DevOps engineer**: 20% time for automation pipeline
- **QA engineer**: 30% time for testing and validation

#### Infrastructure Requirements
- **CI/CD pipeline**: GitHub Actions with Cassandra test clusters
- **Grammar monitoring**: Automated sync with Apache Cassandra repository
- **Performance testing**: Dedicated benchmarking infrastructure

### Conclusion

This hybrid approach leverages the performance advantages of our current nom implementation while ensuring long-term compatibility with official Cassandra grammar evolution. By combining automated grammar monitoring, comprehensive testing, and fallback mechanisms, we can maintain parsing accuracy as Cassandra evolves through versions 6.x, 7.x, and beyond.

The strategy prioritizes immediate practical needs (DML parsing) while building infrastructure for long-term sustainability. This approach minimizes risk while maximizing our ability to adapt to future CQL changes quickly and accurately.

## Future Enhancements (Legacy)

1. **Full DDL Support**: CREATE INDEX, CREATE TYPE, etc.
2. **Schema Evolution**: ALTER TABLE statement parsing  
3. **Validation**: Enhanced schema validation with UDT registry
4. **Performance**: Parser optimization for large schemas
5. **Extensions**: Support for Cassandra-specific extensions

## Usage Examples

### Basic Usage
```rust
use cqlite_core::schema::{parse_cql_schema, SchemaManager};

// Parse CQL directly
let schema = parse_cql_schema(cql_statement)?;

// Use with SchemaManager
let mut manager = SchemaManager::new(storage, &config).await?;
let registered_schema = manager.parse_and_register_cql_schema(cql_statement)?;
```

### Type Conversion
```rust
use cqlite_core::schema::cql_type_to_type_id;

let type_id = cql_type_to_type_id("frozen<set<uuid>>")?;
// Returns CqlTypeId::Set for the inner type
```

### Table Name Operations
```rust
use cqlite_core::schema::{extract_table_name, table_name_matches};

// Extract names
let (keyspace, table) = extract_table_name("CREATE TABLE ks.users (...)")?;

// Match tables
if table_name_matches(&schema_keyspace, &schema_table, &target_keyspace, &target_table) {
    // Table matches
}
```

This implementation provides a robust foundation for CQL schema parsing in the cqlite project, supporting the full range of Cassandra table definitions while maintaining high performance and comprehensive error handling.