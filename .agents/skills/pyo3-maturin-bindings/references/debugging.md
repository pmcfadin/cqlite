# Debugging Binding Issues

## Build Issues

### "can't find crate for `pyo3`"

```toml
# Cargo.toml - ensure feature is enabled
[dependencies]
pyo3 = { version = "0.22", features = ["extension-module"] }
```

### Symbol visibility / undefined symbols on macOS

```toml
# .cargo/config.toml
[target.x86_64-apple-darwin]
rustflags = ["-C", "link-arg=-undefined", "-C", "link-arg=dynamic_lookup"]

[target.aarch64-apple-darwin]
rustflags = ["-C", "link-arg=-undefined", "-C", "link-arg=dynamic_lookup"]
```

### "multiple definitions of `PyInit_*`"

Only one `#[pymodule]` per crate. If you have multiple files:

```rust
// src/lib.rs - single entry point
mod types;
mod functions;

#[pymodule]
fn cqlite(m: &Bound<'_, PyModule>) -> PyResult<()> {
    types::register(m)?;
    functions::register(m)?;
    Ok(())
}

// src/types.rs
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MyType>()?;
    Ok(())
}
```

### Maturin can't find Python

```bash
# Explicitly specify Python
maturin develop --interpreter python3.11

# Or ensure venv is activated
source .venv/bin/activate
which python  # Should show venv python
```

## Import Issues

### "ImportError: dynamic module does not define module export function"

Module name mismatch. Check:

```toml
# pyproject.toml
[tool.maturin]
module-name = "cqlite._cqlite"  # Must match #[pymodule] name
```

```rust
// src/lib.rs
#[pymodule]
fn _cqlite(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Name must match module-name (after last dot)
    Ok(())
}
```

### "ModuleNotFoundError" after successful build

```bash
# Ensure installed in current environment
pip list | grep cqlite

# Rebuild
maturin develop --release
```

### Import works in Python shell but not in pytest

```bash
# Run pytest with verbose import
pytest -v --import-mode=importlib

# Check sys.path
python -c "import sys; print(sys.path)"
```

## Runtime Issues

### Segfault / Memory corruption

Common causes:

1. **Returning borrowed reference that outlives scope**
```rust
// BAD
#[pyfunction]
fn bad<'py>(py: Python<'py>) -> &'py str {
    let s = String::from("temp");
    s.as_str()  // Dangling reference!
}

// GOOD
#[pyfunction]
fn good() -> String {
    String::from("temp")  // Owned, safe
}
```

2. **GIL not held when accessing Python objects**
```rust
// BAD - accessing Py<T> without GIL
fn bad_access(obj: &Py<PyAny>) {
    // obj.as_ref()  // Needs GIL!
}

// GOOD
fn good_access(py: Python<'_>, obj: &Py<PyAny>) {
    let bound = obj.bind(py);  // GIL held
}
```

3. **Use-after-free with raw pointers**
```rust
// Avoid raw pointers; use Py<T> or Bound<'_, T>
```

### "RuntimeError: Already borrowed"

PyO3 uses runtime borrow checking for `#[pyclass]`:

```rust
#[pyclass]
struct Container {
    items: Vec<Item>,
}

#[pymethods]
impl Container {
    fn process(&mut self) {  // Mutable borrow
        for item in &self.items {  // Immutable borrow - OK, same scope
            // ...
        }
    }
    
    // Problem: calling method that borrows while already borrowed
    fn bad_nested(&mut self) {
        let item = &self.items[0];  // Borrow
        self.modify();  // Tries to mutably borrow - PANIC!
    }
}
```

**Fix**: Restructure to avoid nested borrows, or use `RefCell`/`Mutex`.

### "TypeError: argument 'x': ..." conversion errors

Check type mapping:

```python
# Python
cqlite.process({"key": "value"})  # Passing dict

# Rust - what does the signature expect?
fn process(data: HashMap<String, String>)  # OK
fn process(data: MyCustomType)  # Needs FromPyObject impl
```

Debug with:
```rust
#[pyfunction]
fn debug_type(obj: &Bound<'_, PyAny>) -> String {
    format!("Type: {}, Value: {:?}", 
        obj.get_type().name().unwrap_or("unknown"),
        obj.repr().map(|r| r.to_string()).unwrap_or_default()
    )
}
```

## Performance Issues

### Slow due to GIL contention

```rust
// Profile to find hotspots
#[pyfunction]
fn slow_function(py: Python<'_>, data: Vec<u8>) -> PyResult<Vec<u8>> {
    // If this is slow, release GIL
    let result = py.allow_threads(|| {
        expensive_rust_computation(&data)
    });
    Ok(result)
}
```

### Excessive allocations crossing FFI

```rust
// BAD: Copies data multiple times
#[pyfunction]
fn bad(data: Vec<u8>) -> Vec<u8> {
    // Vec<u8> copied from Python to Rust, then back
    data
}

// BETTER: Use buffer protocol for zero-copy
#[pyfunction]
fn better(py: Python<'_>, data: &Bound<'_, PyBytes>) -> Bound<'_, PyBytes> {
    // Direct access, no copy
    let slice = data.as_bytes();
    PyBytes::new(py, slice)
}
```

## Debugging Tools

### Enable PyO3 debug output

```bash
RUST_LOG=pyo3=debug maturin develop
```

### Debug with lldb/gdb

```bash
# Build with debug symbols
maturin develop  # Debug by default

# Attach debugger
lldb python
(lldb) run -c "import cqlite; cqlite.crash()"
(lldb) bt  # Backtrace when it crashes
```

### Python-side debugging

```python
import cqlite

# Check what's exported
print(dir(cqlite))

# Check types
stmt = cqlite.parse("SELECT * FROM users")
print(type(stmt))
print(type(stmt).__mro__)  # Inheritance chain

# Check signatures
import inspect
print(inspect.signature(cqlite.parse))
```

### Add debug repr

```rust
#[pymethods]
impl MyType {
    fn __repr__(&self) -> String {
        format!("MyType(inner={:?})", self.inner)
    }
    
    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }
}
```

## Common Error Messages

| Error | Likely Cause |
|-------|--------------|
| `pyo3_runtime::PanicException` | Rust panic (index out of bounds, unwrap on None, etc.) |
| `SystemError: <class 'X'> returned a result with an error set` | Forgot to return `PyResult`, or error during `__new__` |
| `TypeError: 'X' object cannot be converted to 'Y'` | Missing `FromPyObject` impl or wrong type |
| `AttributeError: module 'X' has no attribute 'Y'` | Forgot to add to module with `m.add_*()` |
| `RuntimeError: Already mutably borrowed` | Nested mutable borrows of pyclass |
