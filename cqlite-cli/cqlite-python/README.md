# 🚀 CQLite Python - The World's First Python Package for Direct SSTable Querying!

[![PyPI version](https://badge.fury.io/py/cqlite.svg)](https://badge.fury.io/py/cqlite)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Downloads](https://pepy.tech/badge/cqlite)](https://pepy.tech/project/cqlite)

**Revolutionary**: Execute SELECT statements directly on Cassandra SSTable files without running Cassandra!

## 🌟 What Makes CQLite Unique?

CQLite is the **FIRST AND ONLY** Python library that allows you to:

- ✨ **Execute SQL SELECT queries directly on SSTable files**
- 🚀 **Query Cassandra data without running Cassandra**
- 🐍 **Native Python integration with pandas, numpy, and async support**
- ⚡ **Zero-copy operations and memory-efficient streaming**
- 📊 **Export to CSV, Parquet, JSON, Excel directly from SSTable files**
- 🔄 **Async query support for large datasets**

## 🎯 Quick Start

### Installation

```bash
pip install cqlite
```

For full features:
```bash
pip install cqlite[all]  # Includes pandas, numpy, pyarrow, async support
```

### Basic Usage

```python
import cqlite

# Open SSTable file
reader = cqlite.SSTableReader("users-big-Data.db")

# Execute SELECT queries directly on the SSTable!
results = reader.query("SELECT name, email, age FROM users WHERE age > 25")

# Convert to pandas DataFrame
df = reader.query_df("SELECT * FROM users WHERE city = 'New York'")

# Async iteration for large datasets
async for row in reader.query_async("SELECT * FROM large_table"):
    process_row(row)
```

### Context Manager Support

```python
with cqlite.SSTableReader("data.db") as reader:
    df = reader.query_df("SELECT * FROM users LIMIT 1000")
    stats = reader.get_stats()
```

## 🔥 Revolutionary Features

### 1. Direct SSTable Querying (WORLD FIRST!)

```python
# Query SSTable files directly - no Cassandra needed!
results = cqlite.query_sstable(
    "users-ka-1-Data.db", 
    "SELECT name, email FROM users WHERE age BETWEEN 25 AND 35"
)
```

### 2. Seamless Pandas Integration

```python
# Get results as pandas DataFrame
df = reader.query_df("SELECT * FROM users")
print(df.describe())

# Use pandas operations
young_users = df[df['age'] < 30]
avg_age_by_city = df.groupby('city')['age'].mean()
```

### 3. Async Support for Large Datasets

```python
import asyncio

async def process_large_table():
    async with cqlite.AsyncSSTableReader("huge-table-Data.db") as reader:
        # Stream millions of rows efficiently
        async for row in reader.query_streaming("SELECT * FROM huge_table"):
            await process_row_async(row)

asyncio.run(process_large_table())
```

### 4. Multiple Export Formats

```python
# Export to different formats
reader.export_csv("SELECT * FROM users", "users.csv")
reader.export_parquet("SELECT * FROM analytics", "analytics.parquet")
reader.export_json("SELECT * FROM logs", "logs.json")

# Export to multiple formats at once
reader.export_all_formats(
    "SELECT * FROM users WHERE active = true", 
    "active_users",
    formats=["csv", "parquet", "json"]
)
```

### 5. Advanced Query Features

```python
# Get query statistics
stats = reader.describe()
print(f"Table: {stats['table_name']}")
print(f"Columns: {stats['schema']['columns']}")

# Validate queries before execution
validation = reader.validate_query("SELECT invalid_column FROM users")
if not validation['valid']:
    print("Query errors:", validation['errors'])

# Get schema information
schema = reader.get_schema()
columns = reader.get_column_names()
partition_keys = reader.get_partition_keys()
```

## 📊 Type System & Python Integration

CQLite provides seamless type conversion between CQL and Python:

| CQL Type | Python Type | Notes |
|----------|-------------|-------|
| `text`, `varchar` | `str` | Unicode strings |
| `int`, `bigint` | `int` | Native Python integers |
| `float`, `double` | `float` | IEEE 754 floating point |
| `boolean` | `bool` | True/False |
| `uuid`, `timeuuid` | `uuid.UUID` | Standard UUID objects |
| `timestamp` | `datetime.datetime` | Timezone-aware |
| `date` | `datetime.date` | Date objects |
| `list<T>` | `List[T]` | Python lists with type conversion |
| `set<T>` | `Set[T]` | Python sets |
| `map<K,V>` | `Dict[K,V]` | Python dictionaries |
| `blob` | `bytes` | Raw binary data |

## 🔧 Advanced Usage

### Batch Processing Multiple SSTable Files

```python
# Process multiple SSTable files
sstable_paths = [
    "users-shard1-Data.db",
    "users-shard2-Data.db", 
    "users-shard3-Data.db"
]

processor = cqlite.AsyncBatchProcessor(sstable_paths)
all_results = await processor.process_all("SELECT COUNT(*) FROM users")
```

### Schema Discovery and Validation

```python
# Discover SSTable files in directory
sstables = cqlite.discover_sstables("/path/to/cassandra/data/keyspace/table/")

for sstable in sstables:
    print(f"Found: {sstable['name']} ({sstable['size_bytes']} bytes)")
    
    # Validate each SSTable
    validation = cqlite.validate_sstable(sstable['path'])
    if validation['valid']:
        print("✅ SSTable is valid")
    else:
        print("❌ Issues:", validation['errors'])
```

### Performance Optimization

```python
# Memory-efficient streaming for large datasets
async def process_large_dataset():
    count = 0
    async for chunk in reader.query_chunks("SELECT * FROM large_table", chunk_size=10000):
        # Process chunk of 10,000 rows
        count += len(chunk)
        print(f"Processed {count} rows...")

# Query optimization suggestions
optimization = cqlite.optimize_query(
    "SELECT * FROM users WHERE email LIKE '%@example.com'",
    available_columns=reader.get_column_names()
)
print("Suggestions:", optimization['suggestions'])
```

## 🛠️ Installation Options

### Basic Installation
```bash
pip install cqlite
```

### With Pandas Support
```bash
pip install cqlite[pandas]
```

### With Parquet Export
```bash
pip install cqlite[parquet]
```

### With Async Support
```bash
pip install cqlite[async]
```

### Full Installation (All Features)
```bash
pip install cqlite[all]
```

## 🔍 Feature Detection

```python
import cqlite

# Check available features
features = cqlite.get_available_features()
print(f"Pandas support: {features['pandas']}")
print(f"Async support: {features['async']}")
print(f"Parquet export: {features['pyarrow']}")

# Or get a summary
cqlite.check_dependencies()
```

## 🎯 Use Cases

### 1. **Data Migration**
```python
# Migrate data from Cassandra SSTable to PostgreSQL
df = reader.query_df("SELECT * FROM users")
df.to_sql('users', postgres_engine, if_exists='append')
```

### 2. **Analytics on Historical Data**
```python
# Analyze historical SSTable files without Cassandra
results = reader.query("""
    SELECT date_trunc('month', created_date) as month, 
           COUNT(*) as user_count
    FROM users 
    WHERE created_date >= '2023-01-01'
    GROUP BY month
""")
```

### 3. **Data Recovery**
```python
# Recover data from SSTable files when Cassandra is down
important_data = reader.query("""
    SELECT id, critical_field, backup_timestamp 
    FROM important_table 
    WHERE backup_timestamp > '2023-12-01'
""")
```

### 4. **Performance Testing**
```python
# Benchmark query performance
perf = cqlite.benchmark_query_performance(
    reader, 
    "SELECT * FROM users WHERE age > 30", 
    iterations=10
)
print(f"Average: {perf['avg_time_seconds']:.3f}s")
print(f"Throughput: {perf['rows_per_second']:.0f} rows/sec")
```

## 🚀 Why CQLite is Revolutionary

### Before CQLite:
- ❌ Need full Cassandra cluster to query data
- ❌ Complex setup and maintenance
- ❌ Resource-intensive operations
- ❌ No direct Python integration
- ❌ Limited analytics capabilities

### With CQLite:
- ✅ Query SSTable files directly
- ✅ Zero infrastructure requirements  
- ✅ Lightweight and fast
- ✅ Native Python integration
- ✅ Full SQL SELECT support
- ✅ Export to any format
- ✅ Async and streaming support

## 📚 Documentation

- 📖 **Full Documentation**: https://docs.cqlite.dev
- 🔧 **API Reference**: https://docs.cqlite.dev/api
- 📝 **Examples**: https://github.com/cqlite/cqlite/tree/main/examples
- 🎯 **Tutorials**: https://docs.cqlite.dev/tutorials

## 🤝 Contributing

We welcome contributions! CQLite is pioneering a new category of database tools.

```bash
git clone https://github.com/cqlite/cqlite
cd cqlite/cqlite-python
pip install -e .[dev]
```

## 📄 License

Apache 2.0 License - see [LICENSE](LICENSE) file.

## 🌟 Star Us!

If CQLite helps you work with Cassandra data, please give us a star! ⭐

---

**CQLite - Making Cassandra data accessible to everyone, everywhere! 🚀**