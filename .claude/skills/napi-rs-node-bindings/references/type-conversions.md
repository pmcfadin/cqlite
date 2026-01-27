# Type Conversions

## Primitive Types (Automatic)

| Rust | JavaScript | TypeScript | Notes |
|------|------------|------------|-------|
| `bool` | `boolean` | `boolean` | |
| `i8, i16, i32, u8, u16, u32` | `number` | `number` | |
| `i64, u64` | `bigint` | `bigint` | Use `#[napi(strict)]` to enforce |
| `f32, f64` | `number` | `number` | |
| `String`, `&str` | `string` | `string` | |
| `Vec<u8>` | `Buffer` | `Buffer` | |
| `Vec<T>` | `Array<T>` | `Array<T>` | |
| `HashMap<K, V>` | `Object` | `Record<K, V>` | K must be string |
| `Option<T>` | `T \| null` | `T \| null` | |
| `()` | `undefined` | `void` | |

## Struct Patterns

### Class (with methods)

```rust
#[napi]
pub struct Statement {
    inner: cqlite::Statement,
}

#[napi]
impl Statement {
    #[napi(constructor)]
    pub fn new(cql: String) -> Result<Self> {
        Ok(Self { inner: cqlite::parse(&cql)? })
    }
    
    #[napi(getter)]
    pub fn query_type(&self) -> String {
        self.inner.query_type().to_string()
    }
    
    #[napi(getter)]
    pub fn keyspace(&self) -> Option<String> {
        self.inner.keyspace().map(|s| s.to_string())
    }
}
```

```typescript
// Generated TypeScript
export class Statement {
    constructor(cql: string)
    get queryType(): string
    get keyspace(): string | null
}
```

### Plain Object (no class wrapper)

```rust
#[napi(object)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub row_count: u32,
    pub has_more: bool,
}
```

```typescript
// Generated TypeScript
export interface QueryResult {
    rows: Array<Row>
    rowCount: number
    hasMore: boolean
}
```

## Enums

### String Enums

```rust
#[napi(string_enum)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
}
```

```typescript
// Generated TypeScript
export const enum QueryType {
    Select = "Select",
    Insert = "Insert",
    Update = "Update",
    Delete = "Delete"
}
```

### Numeric Enums

```rust
#[napi]
pub enum ColumnType {
    Text = 0,
    Int = 1,
    Float = 2,
    Blob = 3,
}
```

## Buffer and TypedArray

```rust
use napi::bindgen_prelude::*;

// Accept Buffer (Node.js)
#[napi]
pub fn process_buffer(data: Buffer) -> Buffer {
    let bytes = data.as_ref();
    // Process bytes...
    Buffer::from(bytes.to_vec())
}

// Accept Uint8Array (universal)
#[napi]
pub fn process_typed_array(data: Uint8Array) -> Uint8Array {
    let bytes = data.as_ref();
    Uint8Array::from(bytes.to_vec())
}

// Zero-copy reference (careful with lifetimes)
#[napi]
pub fn hash_data(data: &[u8]) -> String {
    // data is borrowed, no copy
    format!("{:x}", md5::compute(data))
}
```

## Complex Type Patterns

### Returning Nested Structures

```rust
#[napi(object)]
pub struct Column {
    pub name: String,
    pub column_type: String,
}

#[napi(object)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[napi]
pub fn get_schema() -> Table {
    Table {
        name: "users".to_string(),
        columns: vec![
            Column { name: "id".to_string(), column_type: "uuid".to_string() },
            Column { name: "name".to_string(), column_type: "text".to_string() },
        ],
    }
}
```

### Accepting Complex Input

```rust
#[napi(object)]
pub struct QueryOptions {
    pub keyspace: Option<String>,
    pub limit: Option<u32>,
    pub timeout_ms: Option<u32>,
}

#[napi]
pub fn execute_query(cql: String, options: Option<QueryOptions>) -> Result<QueryResult> {
    let opts = options.unwrap_or_default();
    // Use opts...
}
```

```typescript
// Usage in JavaScript
executeQuery("SELECT * FROM users", {
    keyspace: "myks",
    limit: 100
});
```

### Generic/Union Types with Either

```rust
use napi::Either;

#[napi]
pub fn flexible_input(value: Either<String, f64>) -> String {
    match value {
        Either::A(s) => s,
        Either::B(n) => n.to_string(),
    }
}
```

```typescript
// Generated TypeScript
export function flexibleInput(value: string | number): string
```

### Callbacks and Functions

```rust
use napi::threadsafe_function::{ThreadsafeFunction, ErrorStrategy};

#[napi]
pub fn with_callback(
    callback: ThreadsafeFunction<String, ErrorStrategy::Fatal>
) -> Result<()> {
    callback.call(Ok("Hello from Rust".to_string()), ThreadsafeFunctionCallMode::Blocking)?;
    Ok(())
}

// Simpler: JsFunction for sync callbacks
#[napi]
pub fn transform_with_fn(
    items: Vec<String>,
    transformer: JsFunction,
) -> Result<Vec<String>> {
    items.into_iter()
        .map(|item| transformer.call(None, &[item.into_val(env)?]))
        .collect()
}
```

## Serde Integration

```rust
use napi::bindgen_prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[napi(object)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub options: serde_json::Value,  // Any JSON
}

// Accept arbitrary JSON
#[napi]
pub fn process_json(data: serde_json::Value) -> Result<String> {
    Ok(serde_json::to_string(&data)?)
}
```

## Custom TypeScript Types

```rust
#[napi(ts_args_type = "options: { timeout?: number; retries?: number }")]
pub fn configure(options: serde_json::Value) -> Result<()> {
    // Parse options...
    Ok(())
}

#[napi(ts_return_type = "Promise<{ success: boolean; data?: any }>")]
pub async fn fetch_data() -> Result<serde_json::Value> {
    // Return JSON...
}
```

## Date/Time Handling

```rust
use chrono::{DateTime, Utc};

// As timestamp (milliseconds)
#[napi]
pub fn parse_timestamp(ts: i64) -> Result<String> {
    let dt = DateTime::<Utc>::from_timestamp_millis(ts)
        .ok_or_else(|| Error::from_reason("Invalid timestamp"))?;
    Ok(dt.to_rfc3339())
}

// As Date object
#[napi]
pub fn get_date(env: Env, ts: i64) -> Result<JsDate> {
    env.create_date(ts as f64)
}
```

## BigInt Handling

```rust
#[napi]
pub fn process_bigint(value: BigInt) -> BigInt {
    let (signed, words) = value.get_words();
    // Process...
    BigInt::from_words(signed, words)
}

// Convert to/from i64/u64
#[napi]
pub fn bigint_to_i64(value: BigInt) -> Result<i64> {
    let (_, value, lossless) = value.get_i64();
    if !lossless {
        return Err(Error::from_reason("Value too large for i64"));
    }
    Ok(value)
}
```
