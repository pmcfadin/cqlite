# Error Handling

## Basic Pattern: Implement `From` for PyErr

```rust
use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyRuntimeError, PyIOError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CqliteError {
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Invalid type: expected {expected}, got {got}")]
    TypeError { expected: String, got: String },
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Not found: {0}")]
    NotFound(String),
}

impl From<CqliteError> for PyErr {
    fn from(err: CqliteError) -> PyErr {
        match err {
            CqliteError::Parse(msg) => PyValueError::new_err(msg),
            CqliteError::TypeError { expected, got } => {
                PyTypeError::new_err(format!("expected {}, got {}", expected, got))
            }
            CqliteError::Io(e) => PyIOError::new_err(e.to_string()),
            CqliteError::NotFound(msg) => PyKeyError::new_err(msg),
        }
    }
}
```

## Usage in PyO3 Functions

```rust
#[pyfunction]
fn parse_cql(query: &str) -> PyResult<PyStatement> {
    // CqliteError automatically converts to PyErr via From impl
    let stmt = cqlite::parse(query)?;
    Ok(PyStatement::from(stmt))
}

#[pymethods]
impl PyTable {
    fn get_column(&self, name: &str) -> PyResult<PyColumn> {
        self.inner
            .get_column(name)
            .map(PyColumn::from)
            .ok_or_else(|| CqliteError::NotFound(name.to_string()).into())
    }
}
```

## Custom Python Exception Types

```rust
use pyo3::create_exception;
use pyo3::exceptions::PyException;

// Create custom exception hierarchy
create_exception!(cqlite, CqliteException, PyException);
create_exception!(cqlite, ParseError, CqliteException);
create_exception!(cqlite, ValidationError, CqliteException);
create_exception!(cqlite, SchemaError, CqliteException);

// Register in module
#[pymodule]
fn cqlite(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("CqliteException", m.py().get_type::<CqliteException>())?;
    m.add("ParseError", m.py().get_type::<ParseError>())?;
    m.add("ValidationError", m.py().get_type::<ValidationError>())?;
    m.add("SchemaError", m.py().get_type::<SchemaError>())?;
    Ok(())
}

// Use in From impl
impl From<CqliteError> for PyErr {
    fn from(err: CqliteError) -> PyErr {
        match err {
            CqliteError::Parse(msg) => ParseError::new_err(msg),
            CqliteError::Validation(msg) => ValidationError::new_err(msg),
            CqliteError::Schema(msg) => SchemaError::new_err(msg),
            _ => CqliteException::new_err(err.to_string()),
        }
    }
}
```

## Python Usage of Custom Exceptions

```python
import cqlite

try:
    stmt = cqlite.parse("INVALID CQL")
except cqlite.ParseError as e:
    print(f"Parse failed: {e}")
except cqlite.CqliteException as e:
    print(f"CQLite error: {e}")
```

## Error Context and Chaining

```rust
use pyo3::exceptions::PyValueError;

#[pyfunction]
fn process_file(path: &str) -> PyResult<Vec<PyStatement>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| PyIOError::new_err(format!("Failed to read {}: {}", path, e)))?;
    
    let statements = cqlite::parse_all(&content)
        .map_err(|e| ParseError::new_err(format!("In file {}: {}", path, e)))?;
    
    Ok(statements.into_iter().map(PyStatement::from).collect())
}
```

## Panic Handling

PyO3 catches Rust panics and converts them to Python exceptions. However:

```rust
// BAD: Panics are expensive and lose context
fn get_value(&self, idx: usize) -> &Value {
    &self.values[idx]  // Panics on out of bounds
}

// GOOD: Return PyResult with clear error
fn get_value(&self, idx: usize) -> PyResult<&Value> {
    self.values.get(idx)
        .ok_or_else(|| PyIndexError::new_err(
            format!("index {} out of range (len={})", idx, self.values.len())
        ))
}
```

## Common Exception Types

| Python Exception | Use Case |
|-----------------|----------|
| `PyValueError` | Invalid argument value |
| `PyTypeError` | Wrong argument type |
| `PyKeyError` | Missing dict key / not found |
| `PyIndexError` | Index out of bounds |
| `PyIOError` | File/IO operations |
| `PyRuntimeError` | General runtime errors |
| `PyNotImplementedError` | Unimplemented features |
| `PyOverflowError` | Numeric overflow |

## Result Type Alias

```rust
// In your crate's prelude or lib.rs
pub type CqliteResult<T> = Result<T, CqliteError>;

// Clean function signatures
pub fn parse(query: &str) -> CqliteResult<Statement> { ... }
```
