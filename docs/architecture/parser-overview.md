# Parser Architecture

This document provides an overview of the parsing subsystems in cqlite-core.

## Overview

cqlite-core has four distinct parsing subsystems, each serving a different purpose:

1. **CQL Text Parsing** (`src/cql/`) - Query strings to AST
2. **Binary Format Parsing** (`src/parser/`) - SSTable bytes to Rust values
3. **Schema CQL Parsing** (`src/schema/cql_parser.rs`) - CREATE TABLE to TableSchema
4. **Query Statement Parsing** (`src/query/parser.rs`) - DML to ParsedQuery

## Parsing Subsystems

### 1. CQL Text Parsing (`src/cql/`)

Parses CQL (Cassandra Query Language) query strings into Abstract Syntax Trees.

- **Purpose**: Full CQL text parsing with AST construction
- **Technology**: nom parser combinators (ANTLR backend placeholder)
- **Input**: CQL text strings (`"SELECT * FROM users WHERE id = ?"`)
- **Output**: `CqlStatement` AST nodes
- **Integration**: Query engine, schema extraction, semantic validation

**Key Components**:
- `ast.rs` - AST node definitions for all CQL statements
- `nom_backend.rs` - nom-based parser implementation
- `visitor.rs` - Visitor pattern for AST traversal
- `factory.rs` - Parser factory and configuration

**Example**:
```rust
use cqlite_core::cql::{create_default_parser, CqlStatement};

let parser = create_default_parser()?;
let statement = parser.parse("SELECT * FROM users").await?;
```

### 2. Binary Format Parsing (`src/parser/`)

Parses SSTable binary data structures from Cassandra data files.

- **Purpose**: Deserialize binary data from SSTable files
- **Technology**: nom combinators for binary parsing
- **Input**: Raw bytes from `.db` files (Data.db, Statistics.db, Index.db)
- **Output**: Structured Rust values (VInt, headers, CQL values)
- **Integration**: Storage layer, SSTable reader

**Key Components**:
- `vint.rs` - Variable-length integer encoding (Cassandra wire format)
- `header.rs` - SSTable header parsing with version detection
- `types.rs` - CQL primitive type deserialization (int, text, uuid, etc.)
- `complex_types.rs` - Collections, UDTs, tuples, frozen types
- `statistics.rs` - Statistics.db parsing

**Example**:
```rust
use cqlite_core::parser::{parse_vint, SSTableHeader};

let (remaining, value) = parse_vint(&bytes)?;
let header = SSTableHeader::parse(&file_bytes)?;
```

### 3. Schema CQL Parsing (`src/schema/cql_parser.rs`)

Parses CREATE TABLE statements into structured schema metadata.

- **Purpose**: Extract table definitions from DDL
- **Technology**: nom parser combinators
- **Input**: CREATE TABLE statements
- **Output**: `TableSchema` metadata (columns, keys, types)
- **Integration**: UdtRegistry, SchemaRegistry, SSTable reader

**Key Functions**:
- `parse_cql_schema()` - Main entry point for schema parsing
- `split_cql_statements()` - Split multi-statement CQL files
- `cql_type_to_type_id()` - Convert CQL type strings to internal types

**Example**:
```rust
use cqlite_core::schema::parse_cql_schema;

let schema = parse_cql_schema("CREATE TABLE users (id uuid PRIMARY KEY, name text)")?;
```

### 4. Query Statement Parsing (`src/query/parser.rs`)

Lightweight keyword-based parsing for query execution.

- **Purpose**: Parse DML statements for M2 query engine
- **Technology**: Keyword extraction with string manipulation
- **Input**: SELECT, INSERT, UPDATE, DELETE statements
- **Output**: `ParsedQuery` for execution planning
- **Integration**: QueryEngine, M2SelectValidator, QueryPlanner

**Key Components**:
- `QueryParser` - Main parser struct
- `M2SelectValidator` - Validates queries against M2 supported subset
- `ParsedQuery` - Structured query representation

**Example**:
```rust
use cqlite_core::query::QueryParser;

let parser = QueryParser::new(&config);
let query = parser.parse("SELECT * FROM users WHERE id = 1")?;
```

## Key Distinctions

| Module | Parses | Input Type | Output Type | Primary Use |
|--------|--------|------------|-------------|-------------|
| `cql/` | CQL text → AST | `&str` | `CqlStatement` | Advanced parsing, validation |
| `parser/` | SSTable binary | `&[u8]` | Rust values | Data file reading |
| `schema/cql_parser.rs` | CREATE TABLE | `&str` | `TableSchema` | Schema loading |
| `query/parser.rs` | DML statements | `&str` | `ParsedQuery` | Query execution |

## Data Flow

```
CQL Schema Files                    SSTable Files
      │                                   │
      ▼                                   ▼
schema/cql_parser.rs              parser/ (binary)
      │                                   │
      ▼                                   ▼
  TableSchema                     Structured Values
      │                                   │
      └─────────────┬─────────────────────┘
                    ▼
              Query Engine
                    │
                    ▼
            query/parser.rs
                    │
                    ▼
             ParsedQuery
                    │
                    ▼
           Query Execution
```

## When to Use Each

- **Need AST for CQL statements?** → `cql/` module
- **Reading SSTable binary data?** → `parser/` module
- **Loading table schemas from CQL files?** → `schema/cql_parser.rs`
- **Executing queries in the query engine?** → `query/parser.rs`

## Related Documentation

- SSTable format specification: `docs/sstables-definitive-guide/`
- Parser implementation strategy: `docs/technical/parser_strategy.md`
- CQL parser implementation: `docs/technical/CQL_PARSER_IMPLEMENTATION.md`
- Known limitations: `docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`
