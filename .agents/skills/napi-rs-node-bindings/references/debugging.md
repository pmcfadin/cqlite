# Debugging Binding Issues

## Build Issues

### "cannot find -lnode" or missing node headers

```bash
# Ensure napi-build is in build-dependencies
# Cargo.toml
[build-dependencies]
napi-build = "2"
```

```rust
// build.rs must exist
extern crate napi_build;

fn main() {
    napi_build::setup();
}
```

### "undefined symbol" on Linux

Missing linkage. Check Cargo.toml:

```toml
[lib]
crate-type = ["cdylib"]  # Not "dylib" or "staticlib"
```

### Build fails on Windows

Ensure Visual Studio Build Tools are installed:

```bash
# Install via winget
winget install Microsoft.VisualStudio.2022.BuildTools

# Or via npm
npm install --global windows-build-tools
```

### "napi is not defined" or "napi_register_module_v1"

Version mismatch. Ensure consistent napi version:

```toml
[dependencies]
napi = "2"  # Use same major version
napi-derive = "2"
```

## Import Issues

### "Cannot find module" after build

```bash
# Check if .node file was created
ls *.node

# Verify index.js loads the correct file
cat index.js

# Rebuild
npm run build
```

### "Module did not self-register"

Usually a Node.js version mismatch:

```bash
# Check Node.js version
node --version

# Rebuild for current Node
npm run build

# Or specify Node version in package.json
"engines": {
  "node": ">= 18"
}
```

### Binary not found for platform

```bash
# Check what binaries exist
ls *.node

# Expected format: cqlite.{platform}-{arch}.node
# e.g., cqlite.darwin-arm64.node

# Build for current platform
npm run build -- --platform
```

## Runtime Issues

### Segfault / Memory Corruption

Common causes:

1. **Dangling reference from moved value**
```rust
// BAD
#[napi]
pub fn bad() -> &str {
    let s = String::from("temp");
    &s  // Dangling!
}

// GOOD
#[napi]
pub fn good() -> String {
    String::from("temp")
}
```

2. **Using Env after async boundary**
```rust
// BAD - Env is not Send
#[napi]
pub async fn bad(env: Env) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    env.create_string("hello")?;  // env is invalid here!
    Ok(())
}

// GOOD - create values before async
#[napi]
pub async fn good() -> Result<String> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok("hello".to_string())  // Return Rust types
}
```

3. **Thread safety violations**
```rust
// BAD - Rc is not Send
#[napi]
pub struct Bad {
    data: Rc<Vec<u8>>,  // Won't compile with async
}

// GOOD - Use Arc for shared ownership
#[napi]
pub struct Good {
    data: Arc<Vec<u8>>,
}
```

### "TypeError: Cannot read property 'X' of undefined"

Return value not being constructed properly:

```rust
// Check constructor is marked correctly
#[napi]
impl MyClass {
    #[napi(constructor)]  // Must have this
    pub fn new() -> Self {
        Self { /* ... */ }
    }
}
```

### "Error: not implemented"

Missing napi feature:

```toml
# Cargo.toml
[dependencies]
napi = { version = "2", features = [
    "napi9",      # Node-API version
    "async",      # For async functions
    "serde-json", # For JSON types
    "tokio_rt",   # For tokio async runtime
] }
```

### Async function hangs

Blocking the tokio runtime:

```rust
// BAD - blocks the runtime
#[napi]
pub async fn bad() -> String {
    std::thread::sleep(Duration::from_secs(1));  // Blocks!
    "done".to_string()
}

// GOOD - use async sleep
#[napi]
pub async fn good() -> String {
    tokio::time::sleep(Duration::from_secs(1)).await;
    "done".to_string()
}

// GOOD - use spawn_blocking for sync work
#[napi]
pub async fn also_good() -> String {
    tokio::task::spawn_blocking(|| {
        expensive_sync_operation()
    }).await.unwrap()
}
```

### Memory leak

Common with ThreadsafeFunction:

```rust
// BAD - callback never dropped
#[napi]
pub fn bad(callback: ThreadsafeFunction<String>) -> Result<()> {
    std::thread::spawn(move || {
        loop {  // Never ends, callback never dropped
            callback.call(Ok("event".to_string()), ThreadsafeFunctionCallMode::NonBlocking);
        }
    });
    Ok(())
}

// GOOD - provide cleanup mechanism
#[napi]
pub struct Subscription {
    cancel: Arc<AtomicBool>,
}

#[napi]
impl Subscription {
    #[napi]
    pub fn unsubscribe(&self) {
        self.cancel.store(true, Ordering::SeqCst);
    }
}

#[napi]
pub fn subscribe(callback: ThreadsafeFunction<String>) -> Subscription {
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_clone = cancel.clone();
    
    std::thread::spawn(move || {
        while !cancel_clone.load(Ordering::SeqCst) {
            callback.call(Ok("event".to_string()), ThreadsafeFunctionCallMode::NonBlocking);
            std::thread::sleep(Duration::from_secs(1));
        }
    });
    
    Subscription { cancel }
}
```

## Type Errors

### "Expected X, got Y"

Type mismatch between Rust and JavaScript:

```javascript
// Debug types
console.log(typeof value);
console.log(value.constructor.name);
```

```rust
// Add debug output
#[napi]
pub fn debug_type(value: JsUnknown) -> Result<String> {
    let type_of = value.get_type()?;
    Ok(format!("{:?}", type_of))
}
```

### BigInt conversion issues

```rust
// For large numbers, use BigInt explicitly
#[napi]
pub fn process_big(value: BigInt) -> Result<BigInt> {
    // BigInt::get_i64() returns (sign, value, lossless)
    let (_, val, lossless) = value.get_i64();
    if !lossless {
        return Err(Error::from_reason("Value too large"));
    }
    Ok(BigInt::from(val * 2))
}
```

## Debugging Tools

### Enable verbose output

```bash
# Debug logging
RUST_LOG=debug npm run build:debug
```

### Use lldb/gdb

```bash
# macOS
lldb -- node -e "require('./index.js').crash()"
(lldb) run
(lldb) bt  # Backtrace on crash

# Linux
gdb --args node -e "require('./index.js').crash()"
```

### Node.js debugging

```bash
# Inspect native module loading
node --experimental-loader ./debug-loader.mjs

# Enable core dumps
ulimit -c unlimited
node script.js
```

### Add debug repr

```rust
#[napi]
impl MyType {
    #[napi(js_name = "toString")]
    pub fn to_string_js(&self) -> String {
        format!("MyType({:?})", self.inner)
    }
    
    #[napi(js_name = "toJSON")]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.inner).unwrap_or(serde_json::Value::Null)
    }
}
```

## Common Error Messages

| Error | Likely Cause |
|-------|--------------|
| `Module did not self-register` | Node.js version mismatch, rebuild needed |
| `Cannot find module '*.node'` | Binary not built or wrong platform |
| `napi_status::napi_pending_exception` | Unhandled error in Rust code |
| `RuntimeError: unreachable` | Panic in Rust code |
| `TypeError: X is not a constructor` | Missing `#[napi(constructor)]` |
| `TypeError: Cannot convert BigInt to number` | Use BigInt type or convert explicitly |
| `Error: not implemented` | Missing napi feature flag |

## Performance Debugging

```javascript
// Profile with Node.js
node --prof app.js
node --prof-process isolate-*.log > profile.txt

// Memory profiling
node --inspect app.js
// Open chrome://inspect in Chrome
```

```rust
// Add timing
use std::time::Instant;

#[napi]
pub fn timed_operation() -> Result<String> {
    let start = Instant::now();
    let result = do_work()?;
    eprintln!("Operation took {:?}", start.elapsed());
    Ok(result)
}
```
