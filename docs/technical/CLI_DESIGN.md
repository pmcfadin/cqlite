# CQLite CLI Design Specification

## Overview

This document outlines the comprehensive command-line interface design for CQLite, a high-performance embedded database engine. The CLI provides powerful tools for reading SSTable files, querying data with JSON schemas, and exporting data in multiple formats while maintaining excellent user experience and performance.

## Core Principles

1. **Schema-First**: All operations require or can benefit from schema definitions
2. **Streaming-First**: Large file operations use streaming for memory efficiency
3. **Type-Safe**: Full support for all CQL data types including collections and UDTs
4. **Format-Flexible**: Multiple input/output formats (JSON, CSV, Parquet, SQL)
5. **Performance-Aware**: Built-in monitoring and optimization features

## Command Structure

### Main Command: `cqlite`

```bash
cqlite [GLOBAL_OPTIONS] <COMMAND> [COMMAND_OPTIONS]
```

### Global Options

```bash
-d, --database <FILE>         Database file path
-c, --config <FILE>          Configuration file path
-v, --verbose                Verbose output (-v, -vv, -vvv)
-q, --quiet                  Quiet mode (suppress output)
--format <FORMAT>            Output format [table|json|csv|yaml]
--log-level <LEVEL>          Log level [error|warn|info|debug|trace]
--no-color                   Disable colored output
--progress                   Show progress bars for long operations
```

## Core Commands

### 1. Read Command - `cqlite read`

Read and query SSTable files with schema validation and filtering.

```bash
cqlite read <SSTABLE> --schema <SCHEMA_JSON> [OPTIONS]
```

#### Options:
```bash
-s, --schema <FILE>          Schema JSON file (required)
-l, --limit <N>              Limit number of rows
-o, --offset <N>             Skip number of rows  
-f, --filter <EXPR>          Filter expression (SQL-like WHERE clause)
-p, --project <COLS>         Project specific columns (comma-separated)
--key-range <START>..<END>   Filter by key range
--partition <KEY>            Filter by partition key
--clustering <EXPR>          Filter by clustering columns
--output <FILE>              Output to file instead of stdout
--streaming                  Use streaming mode for large files
--validate                   Validate data types against schema
--index                      Use bloom filter/index when available
```

#### Examples:
```bash
# Basic read with schema
cqlite read data.sst --schema user_schema.json

# Read with filtering and projection
cqlite read data.sst --schema user_schema.json \
  --filter "age > 25 AND status = 'active'" \
  --project "id,name,email" \
  --limit 100

# Streaming large file with key range
cqlite read large_data.sst --schema user_schema.json \
  --key-range "user_001".."user_999" \
  --streaming \
  --output filtered_users.json

# Read specific partition
cqlite read data.sst --schema user_schema.json \
  --partition "tenant_123" \
  --clustering "created_at > '2024-01-01'"
```

### 2. Schema Command - `cqlite schema`

Manage and validate schema definitions.

```bash
cqlite schema <SUBCOMMAND> [OPTIONS]
```

#### Subcommands:

##### `cqlite schema validate`
```bash
cqlite schema validate <SCHEMA_JSON>
```
Validate schema JSON format and type definitions.

##### `cqlite schema infer`
```bash
cqlite schema infer <SSTABLE> [OPTIONS]

-o, --output <FILE>          Output schema file
--sample-size <N>            Number of rows to sample (default: 1000)
--confidence <LEVEL>         Type inference confidence [high|medium|low]
--strict                     Use strict type inference
```

##### `cqlite schema generate`
```bash
cqlite schema generate --table <NAME> [OPTIONS]

-t, --table <NAME>           Table name
-c, --columns <SPEC>         Column specifications
-p, --primary-key <COLS>     Primary key columns
-k, --clustering <COLS>      Clustering columns
-o, --output <FILE>          Output schema file
```

##### `cqlite schema describe`
```bash
cqlite schema describe <SCHEMA_JSON>
```
Display human-readable schema description.

#### Examples:
```bash
# Validate schema
cqlite schema validate user_schema.json

# Infer schema from SSTable
cqlite schema infer data.sst --output inferred_schema.json --sample-size 5000

# Generate schema from specification
cqlite schema generate --table users \
  --columns "id:uuid,name:text,age:int,tags:list<text>" \
  --primary-key "id" \
  --output user_schema.json

# Describe schema in human-readable format
cqlite schema describe user_schema.json
```

### 3. Export Command - `cqlite export`

Export data to various formats with filtering and transformation.

```bash
cqlite export <SOURCE> [OPTIONS]
```

#### Options:
```bash
-s, --schema <FILE>          Schema JSON file (required for SSTable input)
-f, --format <FORMAT>        Output format [json|csv|parquet|sql]
-o, --output <FILE>          Output file path
--filter <EXPR>              Filter expression
--project <COLS>             Project specific columns
--transform <SPEC>           Transform expressions
--compress                   Compress output
--batch-size <N>             Batch size for processing
--streaming                  Use streaming mode
--include-schema             Include schema in output
--date-format <FMT>          Date format for CSV output
--null-value <STR>           Null value representation
```

#### Examples:
```bash
# Export to JSON
cqlite export data.sst --schema user_schema.json \
  --format json \
  --output users.json \
  --filter "age >= 18"

# Export to CSV with custom formatting
cqlite export data.sst --schema user_schema.json \
  --format csv \
  --output users.csv \
  --date-format "%Y-%m-%d" \
  --null-value "NULL"

# Export to Parquet with compression
cqlite export data.sst --schema user_schema.json \
  --format parquet \
  --output users.parquet \
  --compress \
  --streaming

# Export SQL INSERT statements
cqlite export data.sst --schema user_schema.json \
  --format sql \
  --output users.sql \
  --transform "table_name=migrated_users"
```

### 4. Query Command - `cqlite query`

Execute CQL-like queries against SSTable files.

```bash
cqlite query [OPTIONS] <QUERY>
```

#### Options:
```bash
-s, --schema <FILE>          Schema JSON file
--explain                    Show query execution plan
--timing                     Show query timing information
--optimize                   Apply query optimizations
--cache                      Cache query results
--parallel <N>               Number of parallel workers
```

#### Examples:
```bash
# Simple query
cqlite query --schema user_schema.json \
  "SELECT name, email FROM users WHERE age > 25"

# Complex query with explanation
cqlite query --schema user_schema.json \
  --explain --timing \
  "SELECT COUNT(*) as total, AVG(age) as avg_age 
   FROM users 
   WHERE status = 'active' 
   GROUP BY department"

# Optimized parallel query
cqlite query --schema user_schema.json \
  --optimize --parallel 4 \
  "SELECT * FROM users WHERE created_at > '2024-01-01'"
```

### 5. Info Command - `cqlite info`

Display information about SSTable files and database statistics.

```bash
cqlite info <SSTABLE> [OPTIONS]
```

#### Options:
```bash
-s, --schema <FILE>          Schema JSON file for detailed analysis
--stats                      Show detailed statistics
--bloom                      Show bloom filter information
--index                      Show index information
--compression                Show compression statistics
--format <FORMAT>            Output format [table|json|yaml]
```

#### Examples:
```bash
# Basic file info
cqlite info data.sst

# Detailed statistics with schema
cqlite info data.sst --schema user_schema.json --stats

# Bloom filter and index information
cqlite info data.sst --bloom --index --compression
```

### 6. Validate Command - `cqlite validate`

Validate SSTable file integrity and data consistency.

```bash
cqlite validate <SSTABLE> [OPTIONS]
```

#### Options:
```bash
-s, --schema <FILE>          Schema JSON file for type validation
--quick                      Quick validation (headers only)
--deep                       Deep validation (all data)
--checksum                   Verify checksums
--bloom                      Validate bloom filter
--index                      Validate index integrity
--report <FILE>              Output validation report
```

#### Examples:
```bash
# Quick validation
cqlite validate data.sst --quick

# Deep validation with schema
cqlite validate data.sst --schema user_schema.json \
  --deep --checksum --report validation_report.json
```

### 7. Convert Command - `cqlite convert`

Convert between different data formats.

```bash
cqlite convert <INPUT> [OPTIONS]
```

#### Options:
```bash
-i, --input-format <FORMAT>  Input format [sstable|json|csv|parquet]
-o, --output-format <FORMAT> Output format [sstable|json|csv|parquet]
-s, --schema <FILE>          Schema JSON file
--output <FILE>              Output file path
--compress                   Apply compression
--optimize                   Optimize for target format
```

## Schema JSON Format

The schema JSON format defines table structure and data types:

```json
{
  "table_name": "users",
  "version": "1.0",
  "primary_key": ["id"],
  "clustering_keys": ["created_at"],
  "columns": [
    {
      "name": "id",
      "type": "uuid",
      "nullable": false
    },
    {
      "name": "name", 
      "type": "text",
      "nullable": false
    },
    {
      "name": "age",
      "type": "int",
      "nullable": true
    },
    {
      "name": "tags",
      "type": "list<text>",
      "nullable": true
    },
    {
      "name": "metadata",
      "type": "map<text,text>",
      "nullable": true
    },
    {
      "name": "address",
      "type": "user_defined_type",
      "udt_definition": {
        "street": "text",
        "city": "text", 
        "zip": "text"
      },
      "nullable": true
    }
  ],
  "compression": "lz4",
  "bloom_filter": true
}
```

### Supported Data Types

#### Primitive Types:
- `boolean` - Boolean value
- `tinyint` - 8-bit signed integer
- `smallint` - 16-bit signed integer
- `int` - 32-bit signed integer
- `bigint` - 64-bit signed integer
- `float` - 32-bit floating point
- `double` - 64-bit floating point
- `text` - UTF-8 string
- `blob` - Binary data
- `uuid` - UUID (16 bytes)
- `timestamp` - Timestamp with microsecond precision
- `json` - JSON document

#### Collection Types:
- `list<type>` - Ordered list of values
- `set<type>` - Unordered set of unique values
- `map<key_type,value_type>` - Key-value map

#### Advanced Types:
- `tuple<type1,type2,...>` - Tuple with fixed types
- `user_defined_type` - Custom composite type

## Filter Expression Syntax

Filter expressions use SQL-like WHERE clause syntax:

### Basic Operators:
```sql
-- Comparison
age > 25
name = 'John'
status != 'inactive'
created_at >= '2024-01-01'

-- Logical
age > 25 AND status = 'active'
name = 'John' OR name = 'Jane'
NOT (age < 18)

-- Pattern matching
name LIKE 'John%'
email LIKE '%@company.com'

-- Null checks
description IS NULL
metadata IS NOT NULL

-- Range
age BETWEEN 18 AND 65
created_at IN ('2024-01-01', '2024-02-01')
```

### Collection Operations:
```sql
-- List/Set operations
tags CONTAINS 'vip'
tags CONTAINS ANY ['premium', 'gold']
tags CONTAINS ALL ['active', 'verified']

-- Map operations
metadata['department'] = 'engineering'
metadata CONTAINS KEY 'department'
metadata CONTAINS VALUE 'engineering'
```

### Functions:
```sql
-- String functions
LENGTH(name) > 5
UPPER(status) = 'ACTIVE'
SUBSTRING(email, 1, 10) = 'john.smith'

-- Date functions
YEAR(created_at) = 2024
DATE(created_at) = '2024-01-01'

-- Type functions
TYPE_OF(data) = 'json'
```

## Error Handling

### Error Categories:

1. **File Errors**
   - File not found
   - Permission denied
   - Corrupted file format
   - Unsupported file version

2. **Schema Errors**
   - Invalid schema JSON
   - Missing required fields
   - Type mismatch
   - Unknown data types

3. **Query Errors**
   - Invalid filter syntax
   - Unknown column names
   - Type conversion errors
   - Unsupported operations

4. **Resource Errors**
   - Out of memory
   - Disk space exhausted
   - Network timeouts
   - System limits exceeded

### Error Response Format:

```json
{
  "error": {
    "code": "SCHEMA_VALIDATION_ERROR", 
    "message": "Column 'age' type mismatch: expected int, found text",
    "details": {
      "column": "age",
      "expected_type": "int",
      "actual_type": "text",
      "row": 1523,
      "file": "data.sst"
    },
    "suggestions": [
      "Update schema to use 'text' type for column 'age'",
      "Convert column values to integers",
      "Use --transform option to cast types"
    ]
  }
}
```

### Recovery Strategies:

1. **Graceful Degradation**: Continue processing when possible
2. **Partial Results**: Return successfully processed data
3. **Retry Logic**: Automatic retry for transient errors
4. **User Guidance**: Clear error messages with suggested fixes

## Performance Considerations

### Memory Management:
- **Streaming Mode**: Process large files without loading into memory
- **Batch Processing**: Configurable batch sizes for memory efficiency
- **Memory Monitoring**: Track and report memory usage
- **Memory Limits**: Respect system memory constraints

### I/O Optimization:
- **Sequential Access**: Optimize for sequential SSTable reading
- **Buffer Management**: Efficient buffer sizes for different operations
- **Compression**: Support for compressed input/output
- **Parallel I/O**: Multi-threaded file operations when beneficial

### Query Optimization:
- **Index Usage**: Leverage bloom filters and indexes
- **Predicate Pushdown**: Apply filters early in processing
- **Column Pruning**: Read only required columns
- **Statistics**: Use file statistics for query planning

### Progress Reporting:
```bash
# Progress bar for long operations
Reading data.sst ████████████████████████████████ 100% (50MB/50MB) ETA: 0s

# Detailed progress information
Processing: 1,234,567 rows processed, 45,678 filtered, 0 errors
Rate: 125,000 rows/sec, Memory: 234MB, Time: 00:01:23
```

## Configuration

### Configuration File Format (cqlite.toml):

```toml
[default]
# Default database path
database = "~/cqlite.db"

# Default output format
output_format = "table"

# Logging configuration
log_level = "info"
log_file = "cqlite.log"

[performance]
# Memory limits
max_memory = "1GB"
batch_size = 10000
parallel_workers = 4

# I/O settings
buffer_size = "64KB"
use_streaming = true
enable_compression = true

[formats]
# CSV format settings
csv_delimiter = ","
csv_quote = "\""
csv_null_value = ""

# JSON format settings
json_pretty_print = true
json_include_nulls = false

# Date formatting
timestamp_format = "%Y-%m-%d %H:%M:%S"
date_format = "%Y-%m-%d"

[cache]
# Query result caching
enable_cache = true
cache_size = "100MB"
cache_ttl = "1h"
```

## Examples and Use Cases

### 1. Data Migration
```bash
# Export Cassandra SSTable to JSON for migration
cqlite export legacy_data.sst \
  --schema legacy_schema.json \
  --format json \
  --output migration_data.json \
  --streaming \
  --compress

# Convert to new format with schema transformation
cqlite convert migration_data.json \
  --input-format json \
  --output-format parquet \
  --schema new_schema.json \
  --optimize
```

### 2. Data Analysis
```bash
# Analyze user behavior data
cqlite query --schema events_schema.json \
  "SELECT event_type, COUNT(*) as count, AVG(session_duration) as avg_duration
   FROM events 
   WHERE date >= '2024-01-01' 
   GROUP BY event_type 
   ORDER BY count DESC"

# Export analysis results
cqlite export events.sst \
  --schema events_schema.json \
  --filter "user_type = 'premium' AND event_type = 'purchase'" \
  --format csv \
  --output premium_purchases.csv
```

### 3. Data Validation
```bash
# Validate data integrity across multiple files
for file in data_*.sst; do
  cqlite validate "$file" \
    --schema user_schema.json \
    --deep \
    --report "validation_$(basename $file .sst).json"
done

# Combine validation reports
jq -s 'add' validation_*.json > combined_validation_report.json
```

### 4. Performance Testing
```bash
# Benchmark read performance
cqlite info large_dataset.sst --stats | grep "Read Performance"

# Test different batch sizes
for batch in 1000 5000 10000 20000; do
  echo "Testing batch size: $batch"
  time cqlite export large_dataset.sst \
    --schema schema.json \
    --format json \
    --batch-size $batch \
    --output /dev/null
done
```

## Future Enhancements

### Version 2.0 Features:
1. **Interactive Mode**: Full-featured REPL with auto-completion
2. **SQL Support**: Complete SQL query engine
3. **Visualization**: Built-in data visualization capabilities
4. **Monitoring**: Real-time performance monitoring
5. **Plugin System**: Extensible plugin architecture
6. **Distributed Queries**: Query across multiple SSTable files
7. **Data Lineage**: Track data transformations and lineage
8. **Security**: Authentication and authorization features

### Integration Possibilities:
- Apache Spark integration
- Kafka Connect connector
- REST API server mode
- Docker containerization
- Kubernetes operator
- Monitoring system integration (Prometheus, Grafana)

This comprehensive CLI design provides a powerful, user-friendly interface for working with CQLite's SSTable files while maintaining high performance and flexibility for various use cases.