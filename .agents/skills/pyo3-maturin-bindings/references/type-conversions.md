# Type Conversions

## Primitive Types (Automatic)

| Rust | Python | Notes |
|------|--------|-------|
| `bool` | `bool` | |
| `i8, i16, i32, i64, i128` | `int` | |
| `u8, u16, u32, u64, u128` | `int` | |
| `f32, f64` | `float` | |
| `String`, `&str` | `str` | |
| `Vec<u8>`, `&[u8]` | `bytes` | |
| `Vec<T>` | `list` | |
| `HashMap<K, V>` | `dict` | |
| `HashSet<T>` | `set` | |
| `Option<T>` | `T \| None` | |
| `(A, B, ...)` | `tuple` | Up to 12 elements |

## Complex Type Patterns

### Wrapping Rust Structs

```rust
// Option 1: Wrapper with inner field (preferred for complex types)
#[pyclass]
pub struct PyStatement {
    pub(crate) inner: Statement,
}

impl From<Statement> for PyStatement {
    fn from(stmt: Statement) -> Self {
        Self { inner: stmt }
    }
}

// Option 2: Direct exposure (only if all fields are Python-compatible)
#[pyclass(get_all)]
pub struct SimpleConfig {
    pub name: String,
    pub value: i64,
}
```

### Returning Collections of Custom Types

```rust
#[pymethods]
impl PyParser {
    fn get_statements(&self) -> Vec<PyStatement> {
        self.inner.statements()
            .into_iter()
            .map(PyStatement::from)
            .collect()
    }
}
```

### Accepting Python Objects

```rust
#[pyfunction]
fn process_data(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<String> {
    // Check type and extract
    if let Ok(s) = obj.extract::<String>() {
        return Ok(s);
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        // Process list...
    }
    Err(PyTypeError::new_err("Expected str or list"))
}
```

### Working with Bytes

```rust
use pyo3::types::PyBytes;

#[pyfunction]
fn process_bytes(data: &[u8]) -> Vec<u8> {
    // Input: Python bytes -> Rust &[u8]
    // Output: Rust Vec<u8> -> Python bytes
    data.to_vec()
}

// For zero-copy when possible
#[pyfunction]
fn create_bytes(py: Python<'_>) -> Bound<'_, PyBytes> {
    PyBytes::new(py, &[1, 2, 3])
}
```

### Enums

```rust
// Simple enums (fieldless)
#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq)]
pub enum PyColumnType {
    Text = 0,
    Int = 1,
    Float = 2,
    Blob = 3,
}

// Complex enums require wrapper pattern
pub enum CqlValue {
    Text(String),
    Int(i32),
    List(Vec<CqlValue>),
}

#[pyclass]
pub struct PyCqlValue {
    inner: CqlValue,
}

#[pymethods]
impl PyCqlValue {
    #[getter]
    fn value_type(&self) -> &str {
        match &self.inner {
            CqlValue::Text(_) => "text",
            CqlValue::Int(_) => "int",
            CqlValue::List(_) => "list",
        }
    }
    
    fn as_text(&self) -> PyResult<String> {
        match &self.inner {
            CqlValue::Text(s) => Ok(s.clone()),
            _ => Err(PyTypeError::new_err("Not a text value"))
        }
    }
}
```

### DateTime Handling

```rust
use chrono::{DateTime, Utc, NaiveDateTime};
use pyo3::types::PyDateTime;

#[pyfunction]
fn parse_timestamp(py: Python<'_>, ts: i64) -> PyResult<Bound<'_, PyDateTime>> {
    let dt = DateTime::<Utc>::from_timestamp(ts, 0)
        .ok_or_else(|| PyValueError::new_err("Invalid timestamp"))?;
    PyDateTime::new(
        py,
        dt.year(), dt.month() as u8, dt.day() as u8,
        dt.hour() as u8, dt.minute() as u8, dt.second() as u8,
        dt.timestamp_subsec_micros(),
        None,
    )
}
```

### UUID Handling

```rust
use uuid::Uuid;

// Option 1: String conversion (simple)
#[pyfunction]
fn uuid_to_string(uuid_str: &str) -> PyResult<String> {
    let uuid = Uuid::parse_str(uuid_str)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(uuid.to_string())
}

// Option 2: Accept Python uuid.UUID
#[pyfunction]
fn process_uuid(py: Python<'_>, uuid_obj: &Bound<'_, PyAny>) -> PyResult<String> {
    let uuid_str: String = uuid_obj.getattr("hex")?.extract()?;
    Ok(uuid_str)
}
```

## FromPyObject for Custom Extraction

```rust
use pyo3::FromPyObject;

#[derive(FromPyObject)]
pub struct QueryParams {
    #[pyo3(item)]  // From dict key
    keyspace: String,
    #[pyo3(item)]
    table: String,
    #[pyo3(item("limit"))]  // Renamed key
    max_rows: Option<i64>,
}

#[pyfunction]
fn execute_query(params: QueryParams) -> PyResult<()> {
    // params automatically extracted from dict
    Ok(())
}
```

## IntoPy for Custom Conversion to Python

```rust
impl IntoPy<PyObject> for MyRustType {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let dict = PyDict::new(py);
        dict.set_item("field1", self.field1).unwrap();
        dict.set_item("field2", self.field2).unwrap();
        dict.into()
    }
}
```
