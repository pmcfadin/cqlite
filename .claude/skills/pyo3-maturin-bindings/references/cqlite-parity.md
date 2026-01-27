# CQLite CQL Feature Parity Checklist

Track coverage between Cassandra CQL features and Python bindings.

## Parity Verification Workflow

1. **Identify CQL feature** from Cassandra documentation
2. **Check Rust implementation** - does `cqlite` core support it?
3. **Check Python binding** - is it exposed via PyO3?
4. **Add test** - Python test exercising the feature
5. **Update checklist** - mark status below

## Statement Types

| Statement | Rust Core | Python Binding | Tests | Notes |
|-----------|:---------:|:--------------:|:-----:|-------|
| SELECT | ⬜ | ⬜ | ⬜ | |
| INSERT | ⬜ | ⬜ | ⬜ | |
| UPDATE | ⬜ | ⬜ | ⬜ | |
| DELETE | ⬜ | ⬜ | ⬜ | |
| BATCH | ⬜ | ⬜ | ⬜ | |
| CREATE KEYSPACE | ⬜ | ⬜ | ⬜ | |
| CREATE TABLE | ⬜ | ⬜ | ⬜ | |
| CREATE INDEX | ⬜ | ⬜ | ⬜ | |
| CREATE TYPE | ⬜ | ⬜ | ⬜ | UDT |
| CREATE FUNCTION | ⬜ | ⬜ | ⬜ | UDF |
| CREATE AGGREGATE | ⬜ | ⬜ | ⬜ | UDA |
| ALTER KEYSPACE | ⬜ | ⬜ | ⬜ | |
| ALTER TABLE | ⬜ | ⬜ | ⬜ | |
| DROP * | ⬜ | ⬜ | ⬜ | |
| TRUNCATE | ⬜ | ⬜ | ⬜ | |
| USE | ⬜ | ⬜ | ⬜ | |

## Data Types

| Type | Rust Core | Python Binding | Python Type | Notes |
|------|:---------:|:--------------:|-------------|-------|
| ascii | ⬜ | ⬜ | `str` | |
| bigint | ⬜ | ⬜ | `int` | |
| blob | ⬜ | ⬜ | `bytes` | |
| boolean | ⬜ | ⬜ | `bool` | |
| counter | ⬜ | ⬜ | `int` | |
| date | ⬜ | ⬜ | `datetime.date` | |
| decimal | ⬜ | ⬜ | `decimal.Decimal` | |
| double | ⬜ | ⬜ | `float` | |
| duration | ⬜ | ⬜ | `timedelta`? | Cassandra 3.10+ |
| float | ⬜ | ⬜ | `float` | |
| inet | ⬜ | ⬜ | `str` or `ipaddress` | |
| int | ⬜ | ⬜ | `int` | |
| smallint | ⬜ | ⬜ | `int` | |
| text | ⬜ | ⬜ | `str` | |
| time | ⬜ | ⬜ | `datetime.time` | |
| timestamp | ⬜ | ⬜ | `datetime.datetime` | |
| timeuuid | ⬜ | ⬜ | `uuid.UUID` | |
| tinyint | ⬜ | ⬜ | `int` | |
| uuid | ⬜ | ⬜ | `uuid.UUID` | |
| varchar | ⬜ | ⬜ | `str` | |
| varint | ⬜ | ⬜ | `int` | |

## Collection Types

| Type | Rust Core | Python Binding | Python Type | Notes |
|------|:---------:|:--------------:|-------------|-------|
| list<T> | ⬜ | ⬜ | `list` | |
| set<T> | ⬜ | ⬜ | `set` | |
| map<K,V> | ⬜ | ⬜ | `dict` | |
| frozen<T> | ⬜ | ⬜ | same as T | |
| tuple<...> | ⬜ | ⬜ | `tuple` | |

## Special Features

| Feature | Rust Core | Python Binding | Tests | Notes |
|---------|:---------:|:--------------:|:-----:|-------|
| User-Defined Types (UDT) | ⬜ | ⬜ | ⬜ | |
| Secondary Indexes | ⬜ | ⬜ | ⬜ | |
| Materialized Views | ⬜ | ⬜ | ⬜ | |
| ALLOW FILTERING | ⬜ | ⬜ | ⬜ | |
| LIMIT | ⬜ | ⬜ | ⬜ | |
| ORDER BY | ⬜ | ⬜ | ⬜ | |
| GROUP BY | ⬜ | ⬜ | ⬜ | Cassandra 4.0+ |
| TTL | ⬜ | ⬜ | ⬜ | |
| WRITETIME | ⬜ | ⬜ | ⬜ | |
| IF NOT EXISTS | ⬜ | ⬜ | ⬜ | LWT |
| IF EXISTS | ⬜ | ⬜ | ⬜ | LWT |
| IF conditions | ⬜ | ⬜ | ⬜ | LWT |
| JSON support | ⬜ | ⬜ | ⬜ | INSERT/SELECT JSON |
| DISTINCT | ⬜ | ⬜ | ⬜ | |
| PER PARTITION LIMIT | ⬜ | ⬜ | ⬜ | |
| Token function | ⬜ | ⬜ | ⬜ | |
| Aggregate functions | ⬜ | ⬜ | ⬜ | COUNT, SUM, etc. |

## Cassandra 5.0 Features

| Feature | Rust Core | Python Binding | Tests | Notes |
|---------|:---------:|:--------------:|:-----:|-------|
| Vector type | ⬜ | ⬜ | ⬜ | vector<float, N> |
| SAI indexes | ⬜ | ⬜ | ⬜ | Storage-Attached Indexes |
| VECTOR ANN queries | ⬜ | ⬜ | ⬜ | ANN (Approximate Nearest Neighbor) |

## SSTable Parsing (if applicable)

| Feature | Rust Core | Python Binding | Tests | Notes |
|---------|:---------:|:--------------:|:-----:|-------|
| Read SSTable metadata | ⬜ | ⬜ | ⬜ | |
| Parse Data.db | ⬜ | ⬜ | ⬜ | |
| Parse Index.db | ⬜ | ⬜ | ⬜ | |
| Parse Filter.db | ⬜ | ⬜ | ⬜ | Bloom filter |
| Parse Statistics.db | ⬜ | ⬜ | ⬜ | |
| Compression support | ⬜ | ⬜ | ⬜ | LZ4, Snappy, etc. |
| SSTable format mc | ⬜ | ⬜ | ⬜ | Cassandra 3.x |
| SSTable format nb | ⬜ | ⬜ | ⬜ | Cassandra 4.x |
| SSTable format nc | ⬜ | ⬜ | ⬜ | Cassandra 5.x |

## Legend

- ⬜ Not started
- 🔄 In progress  
- ✅ Complete
- ❌ Not planned / Out of scope
- ⚠️ Partial support

## Adding New Features

When implementing a new CQL feature:

```rust
// 1. Add to Rust core (src/parser.rs or similar)
pub enum StatementType {
    Select,
    Insert,
    NewFeature,  // Add variant
}

// 2. Add Python binding (src/python/types.rs)
#[pymethods]
impl PyStatement {
    #[getter]
    fn is_new_feature(&self) -> bool {
        matches!(self.inner.statement_type(), StatementType::NewFeature)
    }
}

// 3. Add test (tests/test_new_feature.py)
def test_new_feature():
    stmt = cqlite.parse("NEW FEATURE SYNTAX")
    assert stmt.is_new_feature

// 4. Update this checklist
```

## Parity Test Pattern

```python
# tests/test_parity.py
"""
Ensure Python bindings expose all Rust functionality.
"""
import cqlite

# For each statement type that Rust supports,
# verify Python can parse and access properties

PARITY_TESTS = [
    ("SELECT * FROM users", {"query_type": "select", "table": "users"}),
    ("INSERT INTO users (id) VALUES (1)", {"query_type": "insert", "table": "users"}),
    # Add more as features are implemented
]

@pytest.mark.parametrize("cql,expected", PARITY_TESTS)
def test_parity(cql, expected):
    stmt = cqlite.parse(cql)
    for attr, value in expected.items():
        assert getattr(stmt, attr) == value, f"{attr} mismatch for: {cql}"
```
