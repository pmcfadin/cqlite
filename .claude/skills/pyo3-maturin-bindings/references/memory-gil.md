# Memory Management and GIL

## The GIL (Global Interpreter Lock)

PyO3 code runs with the GIL held by default. Release it for CPU-intensive Rust work.

### Releasing the GIL

```rust
use pyo3::prelude::*;

#[pyfunction]
fn expensive_computation(py: Python<'_>, data: Vec<u8>) -> PyResult<Vec<u8>> {
    // Release GIL for CPU-bound work
    let result = py.allow_threads(|| {
        // This runs WITHOUT the GIL
        // Other Python threads can run during this time
        heavy_rust_computation(&data)
    });
    
    Ok(result)
}
```

### When to Release the GIL

| Scenario | Release GIL? |
|----------|--------------|
| CPU-intensive computation | ✅ Yes |
| File I/O | ✅ Yes |
| Network I/O | ✅ Yes |
| Accessing Python objects | ❌ No (need GIL) |
| Creating Python objects | ❌ No (need GIL) |
| Short operations (<1ms) | ❌ No (overhead not worth it) |

### GIL and Python Object Access

```rust
#[pyfunction]
fn process_with_callback(
    py: Python<'_>,
    data: Vec<u8>,
    callback: PyObject,
) -> PyResult<()> {
    // Process data without GIL
    let processed = py.allow_threads(|| {
        expensive_processing(&data)
    });
    
    // Re-acquire GIL to call Python callback
    // (automatic when allow_threads returns)
    callback.call1(py, (processed,))?;
    
    Ok(())
}
```

## Memory Ownership

### Owned vs Borrowed References

```rust
// Bound<'py, T> - Borrowed reference, tied to GIL lifetime
fn process_list(list: &Bound<'_, PyList>) -> PyResult<i64> {
    // Can only use while GIL is held
    list.len() as i64
}

// Py<T> - Owned reference, can outlive GIL
#[pyclass]
struct Container {
    // Store owned reference
    data: Py<PyList>,
}

#[pymethods]
impl Container {
    #[new]
    fn new(list: Py<PyList>) -> Self {
        Self { data: list }
    }
    
    fn get_length(&self, py: Python<'_>) -> usize {
        // Bind to current GIL to access
        self.data.bind(py).len()
    }
}
```

### Preventing Memory Leaks

```rust
#[pyclass]
struct ResourceHolder {
    // Py<T> prevents cycles but watch for:
    handle: Option<Py<PyAny>>,
}

#[pymethods]
impl ResourceHolder {
    fn clear(&mut self) {
        // Explicit cleanup
        self.handle = None;
    }
}

// For classes that hold Python objects, consider implementing __del__
// or providing explicit cleanup methods
```

## `#[pyclass]` Memory Considerations

### Cloning

```rust
// Default: pyclass cannot be cloned from Python
#[pyclass]
struct NonCloneable {
    data: ExpensiveData,
}

// Allow Python to copy via __copy__
#[pyclass]
#[derive(Clone)]
struct Cloneable {
    data: CheapData,
}

#[pymethods]
impl Cloneable {
    fn __copy__(&self) -> Self {
        self.clone()
    }
    
    fn __deepcopy__(&self, _memo: &Bound<'_, PyDict>) -> Self {
        self.clone()
    }
}
```

### Frozen Classes (Immutable)

```rust
// Immutable - can be shared across threads safely
#[pyclass(frozen)]
struct ImmutableConfig {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    value: i64,
}

// Benefits:
// - Can implement Sync
// - No runtime borrow checking overhead
// - Thread-safe sharing
```

### Interior Mutability

```rust
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

// Single-threaded mutability
#[pyclass]
struct SingleThreaded {
    data: RefCell<Vec<u8>>,
}

// Thread-safe mutability (when releasing GIL)
#[pyclass]
struct ThreadSafe {
    data: Arc<Mutex<Vec<u8>>>,
}

#[pymethods]
impl ThreadSafe {
    fn process(&self, py: Python<'_>) -> PyResult<()> {
        let data = Arc::clone(&self.data);
        py.allow_threads(|| {
            let mut guard = data.lock().unwrap();
            // Modify data...
        });
        Ok(())
    }
}
```

## Buffer Protocol (Zero-Copy)

```rust
use pyo3::buffer::PyBuffer;

#[pyfunction]
fn process_buffer(buffer: PyBuffer<u8>) -> PyResult<usize> {
    // Zero-copy access to Python buffer (numpy arrays, bytes, etc.)
    let slice = unsafe { buffer.as_slice(buffer.py())? };
    Ok(slice.len())
}

// Expose Rust data as Python buffer
unsafe impl pyo3::class::buffer::PyBufferProtocol for MyClass {
    // Implement buffer protocol for zero-copy access from Python
}
```

## Common Pitfalls

### 1. Holding GIL During I/O

```rust
// BAD: Blocks all Python threads
#[pyfunction]
fn bad_io() -> PyResult<String> {
    Ok(std::fs::read_to_string("large_file.txt")?)
}

// GOOD: Release GIL during I/O
#[pyfunction]
fn good_io(py: Python<'_>) -> PyResult<String> {
    py.allow_threads(|| std::fs::read_to_string("large_file.txt"))
        .map_err(|e| PyIOError::new_err(e.to_string()))
}
```

### 2. Storing Borrowed References

```rust
// BAD: Bound<'py, T> cannot outlive the GIL scope
#[pyclass]
struct Bad<'py> {
    list: Bound<'py, PyList>,  // Won't compile
}

// GOOD: Use Py<T> for owned references
#[pyclass]
struct Good {
    list: Py<PyList>,
}
```

### 3. Calling Python from Non-GIL Thread

```rust
// If you spawn threads that need to call back to Python:
use pyo3::Python;

std::thread::spawn(|| {
    // Must acquire GIL first
    Python::with_gil(|py| {
        // Now safe to use Python objects
    });
});
```
