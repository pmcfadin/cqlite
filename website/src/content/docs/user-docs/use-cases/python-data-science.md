---
title: "Python: Data Science and ETL"
description: How to use the CQLite Python bindings for offline analytics on Cassandra snapshots and backups — pandas workflows, streaming for large tables, and ETL patterns without a live cluster.
sidebar:
  label: Python / Data Science
  order: 2
---

# Python: Data Science and ETL

The CQLite Python bindings (`cqlite` package, built with PyO3) let you read
Cassandra 5.0 SSTables directly in Python — no cluster, no JVM, no Cassandra driver.

This page covers the data-science and ETL use case: offline analytics on snapshots
and backups, pandas integration, and memory-bounded streaming for large tables.

<!-- TODO(W5): link to full Python bindings API reference when merged -->

## Installation

```bash
pip install cqlite
```

Or build from source for the latest development version:

```bash
git clone https://github.com/pmcfadin/cqlite
cd cqlite/bindings/python
pip install maturin
maturin develop        # development build, editable install
```

Requires Python 3.9+ and Rust 1.85+ (for source builds only).

## The core pattern

```python
import cqlite

with cqlite.open("/path/to/sstables", schema="/path/to/schema.cql") as db:
    result = db.execute("SELECT * FROM my_keyspace.my_table LIMIT 100")
    for row in result.rows:
        print(row.to_dict())
```

The `schema` parameter is a CQL file containing `CREATE TABLE` statements.
CQLite requires it to decode binary SSTable data — there is no guessing from bytes.

`cqlite.open()` works as a context manager; `db.close()` is idempotent and safe to
call from any thread.

## Complete example: pandas analytics on a Cassandra backup

This example was run against the real CQLite test datasets and its output is shown
below.

```python
import cqlite
import pandas as pd
from decimal import Decimal

DATASETS = "test-data/datasets/sstables"
SCHEMA   = "test-data/schemas/basic-types.cql"

with cqlite.open(DATASETS, schema=SCHEMA) as db:
    result = db.execute(
        "SELECT name, age, salary, account_balance, active "
        "FROM test_basic.simple_table LIMIT 10"
    )

    # Build a DataFrame from the result rows
    rows = [row.to_dict() for row in result.rows]
    df = pd.DataFrame(rows)

    # CQLite returns Decimal for decimal columns — convert to float for pandas math
    df["account_balance"] = df["account_balance"].apply(float)

    print("DataFrame shape:", df.shape)
    print()
    print("Column dtypes:")
    print(df.dtypes)
    print()
    print("Summary statistics:")
    print(df[["age", "salary", "account_balance"]].describe().round(2))
    print()
    active = df[df["active"] == True]
    print(f"Active users: {len(active)} of {len(df)}")
    if len(active) > 0:
        print(f"  Average salary: {active['salary'].mean():.2f}")
```

**Actual output** (run against `test_basic.simple_table`, 10 rows):

```
DataFrame shape: (10, 5)

Column dtypes:
salary               int64
age                  int64
name                   str
active                bool
account_balance    float64
dtype: object

Summary statistics:
         age     salary  account_balance
count  10.00      10.00            10.00
mean   49.20  123326.50         54020.85
std    20.43   45608.41         24617.66
min    18.00   44179.00          2656.69
25%    31.00   89334.50         44476.72
50%    52.50  132994.50         62270.92
75%    64.00  161093.50         71958.12
max    79.00  175802.00         80199.79

Active users: 4 of 10
  Average salary: 127350.50
```

## CQL → Python type mapping

| CQL type | Python type |
|----------|-------------|
| boolean | `bool` |
| tinyint, smallint, int, bigint, counter | `int` |
| float, double | `float` |
| decimal, varint | `decimal.Decimal` |
| text, varchar, ascii | `str` |
| blob | `bytes` |
| timestamp | `datetime.datetime` (timezone-aware, UTC) |
| date | `datetime.date` |
| time | `int` (nanoseconds since midnight) |
| uuid, timeuuid | `uuid.UUID` |
| duration | `cqlite.Duration(months, days, nanos)` |
| inet | `ipaddress.IPv4Address` or `ipaddress.IPv6Address` |
| list, set | `list` |
| map | `dict` |
| tuple | `tuple` |
| UDT | `dict` (field name → value) |

> **v0.13:** `time` and `duration` now decode to exact, lossless types. See the [v0.13 Migration Guide](https://github.com/pmcfadin/cqlite/blob/main/docs/development/v0.13-migration-guide.md).

## Streaming for large tables

For large tables where loading all rows into memory is impractical, use
`db.execute_streaming()`. It returns a lazy iterator; rows are decoded one at a
time and never buffered in bulk:

```python
import cqlite

DATASETS = "test-data/datasets/sstables"
SCHEMA   = "test-data/schemas/basic-types.cql"

with cqlite.open(DATASETS, schema=SCHEMA) as db:
    count = 0
    total_balance = 0.0

    for row in db.execute_streaming(
        "SELECT name, age, account_balance FROM test_basic.simple_table"
    ):
        d = row.to_dict()
        total_balance += float(d["account_balance"])
        count += 1

    print(f"Processed {count} rows, total balance: {total_balance:.2f}")
```

**Actual output** (run against `test_basic.simple_table`):

```
Processed 100 rows, total balance: 5482530.44
```

The streaming iterator releases the Python GIL during I/O, so it is safe to use
from multiple threads (each with its own iterator).

## ETL pattern: snapshot to CSV / Parquet

```python
import cqlite
import csv
import io

DATASETS = "/mnt/cassandra-backup/data"
SCHEMA   = "/etc/cassandra-schemas/my_keyspace.cql"
OUTPUT   = "/tmp/my_table_export.csv"

with cqlite.open(DATASETS, schema=SCHEMA) as db:
    result = db.execute("SELECT * FROM my_keyspace.my_table")
    columns = [c.name for c in result.columns]

    with open(OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in result.rows:
            writer.writerow(row.to_dict())

print(f"Exported {len(result.rows)} rows to {OUTPUT}")
```

For Parquet, export directly from the bindings — the query
streams, so large tables export within bounded memory:

```python
import cqlite

with cqlite.open(DATASETS, schema=SCHEMA) as db:
    rows = db.export_parquet(
        "SELECT * FROM my_keyspace.my_table",
        "/tmp/my_table_export.parquet",
        row_group_size=10000,    # rows per Parquet row group
        compression="snappy",    # "snappy" (default), "zstd", or "none"
    )

print(f"Exported {rows} rows")
```

The output preserves nested and high-precision types — typed lists, maps,
and structs — see [Output Formats](/cqlite/user-docs/output-formats/).

## Notebook workflow

In a Jupyter or Colab notebook:

```python
# Cell 1: imports and setup
import cqlite
import pandas as pd

DATASETS = "/path/to/cassandra-snapshot"
SCHEMA   = "/path/to/schema.cql"

# Cell 2: load data
with cqlite.open(DATASETS, schema=SCHEMA) as db:
    result = db.execute("SELECT * FROM my_ks.events WHERE date = '2025-01-01'")
    df = pd.DataFrame([row.to_dict() for row in result.rows])

# Cell 3: explore
df.head()
df.describe()
df["event_type"].value_counts().plot(kind="bar")
```

## What you cannot do (yet)

- **Write back to SSTables from Python.** The write API is in the core library but not
  yet exposed in the Python bindings. Tracked in the write support roadmap.

## Further reading

- [GitHub repository](https://github.com/pmcfadin/cqlite)
- [API reference](/cqlite/api/latest/)
<!-- TODO(W5): link to Python bindings reference page when merged -->
