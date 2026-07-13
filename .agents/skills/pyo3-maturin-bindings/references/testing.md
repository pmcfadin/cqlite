# Testing Strategies

## Testing Layers

```
┌─────────────────────────────────────┐
│  Python Integration Tests (pytest)  │  ← Test the API users actually use
├─────────────────────────────────────┤
│  Rust Unit Tests (cargo test)       │  ← Test core logic
└─────────────────────────────────────┘
```

## Rust Unit Tests

Test core Rust logic independently of PyO3 bindings:

```rust
// src/parser.rs
pub fn parse(cql: &str) -> Result<Statement, ParseError> {
    // Core parsing logic
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_select() {
        let stmt = parse("SELECT * FROM users").unwrap();
        assert_eq!(stmt.query_type(), QueryType::Select);
    }
    
    #[test]
    fn test_parse_invalid() {
        let err = parse("NOT VALID CQL").unwrap_err();
        assert!(err.to_string().contains("unexpected"));
    }
}
```

```bash
cargo test
cargo test --release  # Test optimized code too
```

## Python Integration Tests

Test the actual Python API:

```python
# tests/test_parser.py
import pytest
import cqlite

class TestParse:
    def test_parse_select(self):
        stmt = cqlite.parse("SELECT * FROM users")
        assert stmt.query_type == "select"
    
    def test_parse_with_keyspace(self):
        stmt = cqlite.parse("SELECT * FROM ks.users")
        assert stmt.keyspace == "ks"
        assert stmt.table == "users"
    
    def test_parse_invalid_raises(self):
        with pytest.raises(cqlite.ParseError) as exc_info:
            cqlite.parse("NOT VALID CQL")
        assert "unexpected" in str(exc_info.value)

class TestStatement:
    def test_statement_repr(self):
        stmt = cqlite.parse("SELECT * FROM users")
        assert "SELECT" in repr(stmt)
    
    def test_statement_equality(self):
        stmt1 = cqlite.parse("SELECT * FROM users")
        stmt2 = cqlite.parse("SELECT * FROM users")
        assert stmt1 == stmt2
```

```bash
# Install in dev mode first
maturin develop

# Run tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=cqlite --cov-report=html
```

## Testing Type Conversions

```python
# tests/test_types.py
import pytest
import cqlite

class TestTypeConversions:
    """Verify Rust types convert correctly to/from Python."""
    
    def test_returns_list(self):
        statements = cqlite.parse_all("SELECT * FROM a; SELECT * FROM b")
        assert isinstance(statements, list)
        assert len(statements) == 2
    
    def test_returns_none_for_optional(self):
        stmt = cqlite.parse("SELECT * FROM users")
        assert stmt.keyspace is None  # No keyspace specified
    
    def test_bytes_roundtrip(self):
        data = b"\x00\x01\x02\xff"
        result = cqlite.process_bytes(data)
        assert result == data
        assert isinstance(result, bytes)
    
    def test_dict_extraction(self):
        result = cqlite.execute_query({
            "keyspace": "test",
            "table": "users",
            "limit": 100
        })
        assert result is not None
```

## Testing Error Handling

```python
# tests/test_errors.py
import pytest
import cqlite

class TestErrors:
    def test_exception_hierarchy(self):
        """Verify custom exceptions inherit correctly."""
        assert issubclass(cqlite.ParseError, cqlite.CqliteException)
        assert issubclass(cqlite.CqliteException, Exception)
    
    def test_error_message_preserved(self):
        with pytest.raises(cqlite.ParseError) as exc_info:
            cqlite.parse("SELEC * FROM users")  # Typo
        assert "SELEC" in str(exc_info.value) or "unexpected" in str(exc_info.value)
    
    def test_error_context(self):
        """Errors should include helpful context."""
        with pytest.raises(cqlite.ParseError) as exc_info:
            cqlite.parse("SELECT * FROM")  # Incomplete
        error_msg = str(exc_info.value)
        # Should indicate where parsing failed
        assert "line" in error_msg.lower() or "position" in error_msg.lower()
```

## Benchmarking

```python
# tests/test_benchmark.py
import pytest

@pytest.mark.benchmark
class TestPerformance:
    def test_parse_simple(self, benchmark):
        import cqlite
        result = benchmark(cqlite.parse, "SELECT * FROM users")
        assert result is not None
    
    def test_parse_complex(self, benchmark):
        import cqlite
        complex_query = """
            SELECT col1, col2, col3 
            FROM keyspace.table 
            WHERE pk = ? AND ck > ? 
            LIMIT 1000
        """
        result = benchmark(cqlite.parse, complex_query)
        assert result is not None
```

```bash
pytest tests/test_benchmark.py --benchmark-only
```

## Property-Based Testing

```python
# tests/test_properties.py
from hypothesis import given, strategies as st
import cqlite

class TestProperties:
    @given(st.text(min_size=1, max_size=100))
    def test_parse_never_crashes(self, text):
        """Parser should handle any input without crashing."""
        try:
            cqlite.parse(text)
        except cqlite.ParseError:
            pass  # Expected for invalid input
        # Should never raise other exceptions
    
    @given(st.sampled_from(["users", "orders", "products"]))
    def test_table_name_preserved(self, table):
        stmt = cqlite.parse(f"SELECT * FROM {table}")
        assert stmt.table == table
```

## Testing Async (if applicable)

```python
# tests/test_async.py
import pytest
import asyncio

@pytest.mark.asyncio
async def test_async_operation():
    import cqlite
    # If you have async Rust functions exposed
    result = await cqlite.async_parse("SELECT * FROM users")
    assert result is not None
```

## Test Configuration

```toml
# pyproject.toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = "-v --tb=short"
markers = [
    "benchmark: marks tests as benchmarks",
    "slow: marks tests as slow",
]

[tool.coverage.run]
source = ["cqlite"]
branch = true

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "raise NotImplementedError",
]
```

## CI Test Matrix

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        python: ["3.8", "3.9", "3.10", "3.11", "3.12"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python }}
      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
      - name: Build and test
        run: |
          pip install maturin pytest
          maturin develop
          pytest tests/ -v
      - name: Rust tests
        run: cargo test
```

## Testing Checklist

- [ ] Rust unit tests for core logic
- [ ] Python tests for API surface
- [ ] Type conversion tests (all supported types)
- [ ] Error handling tests (all exception types)
- [ ] Edge cases (empty input, large input, unicode)
- [ ] Benchmark critical paths
- [ ] Test on all supported Python versions
- [ ] Test on all target platforms
