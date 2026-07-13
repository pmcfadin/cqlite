# Error Handling

## Basic Pattern: Implement `From` for napi::Error

```rust
use napi::bindgen_prelude::*;
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

impl From<CqliteError> for napi::Error {
    fn from(err: CqliteError) -> Self {
        napi::Error::from_reason(err.to_string())
    }
}
```

## Usage in napi Functions

```rust
#[napi]
pub fn parse_cql(query: String) -> Result<Statement> {
    // CqliteError automatically converts to napi::Error
    let stmt = cqlite::parse(&query)?;
    Ok(Statement::from(stmt))
}

#[napi]
impl Table {
    #[napi]
    pub fn get_column(&self, name: String) -> Result<Column> {
        self.inner
            .get_column(&name)
            .map(Column::from)
            .ok_or_else(|| CqliteError::NotFound(name).into())
    }
}
```

## Custom Error Classes

```rust
use napi::bindgen_prelude::*;

// Define custom error class
#[napi]
pub struct ParseError {
    pub message: String,
    pub line: u32,
    pub column: u32,
}

#[napi]
impl ParseError {
    #[napi(constructor)]
    pub fn new(message: String, line: u32, column: u32) -> Self {
        Self { message, line, column }
    }
}

// Throw custom error
#[napi]
pub fn parse_strict(query: String) -> Result<Statement> {
    match cqlite::parse(&query) {
        Ok(stmt) => Ok(Statement::from(stmt)),
        Err(e) => {
            // Create and throw custom error
            Err(Error::new(
                Status::GenericFailure,
                format!("Parse error at {}:{}: {}", e.line, e.column, e.message)
            ))
        }
    }
}
```

## Error with Status Codes

```rust
use napi::Status;

#[napi]
pub fn validate_input(value: String) -> Result<()> {
    if value.is_empty() {
        return Err(Error::new(
            Status::InvalidArg,
            "Value cannot be empty"
        ));
    }
    if value.len() > 1000 {
        return Err(Error::new(
            Status::InvalidArg,
            "Value too long (max 1000 chars)"
        ));
    }
    Ok(())
}
```

### Available Status Codes

| Status | Use Case |
|--------|----------|
| `Status::InvalidArg` | Bad input argument |
| `Status::GenericFailure` | General error |
| `Status::ObjectExpected` | Wrong type passed |
| `Status::StringExpected` | Expected string |
| `Status::FunctionExpected` | Expected function |
| `Status::NumberExpected` | Expected number |
| `Status::BooleanExpected` | Expected boolean |
| `Status::ArrayExpected` | Expected array |
| `Status::Pending` | Async operation pending |
| `Status::Cancelled` | Operation cancelled |

## Async Error Handling

```rust
#[napi]
pub async fn read_and_parse(path: String) -> Result<Vec<Statement>> {
    // Errors in async functions become rejected promises
    let content = tokio::fs::read_to_string(&path)
        .await
        .map_err(|e| Error::from_reason(format!("Failed to read {}: {}", path, e)))?;
    
    let statements = cqlite::parse_all(&content)
        .map_err(|e| Error::from_reason(format!("Parse error in {}: {}", path, e)))?;
    
    Ok(statements.into_iter().map(Statement::from).collect())
}
```

```javascript
// JavaScript usage
try {
    const statements = await cqlite.readAndParse("schema.cql");
} catch (err) {
    console.error("Failed:", err.message);
}
```

## Panic Handling

napi-rs catches Rust panics and converts them to JS errors, but panics are expensive:

```rust
// BAD: Panics lose context and are expensive
#[napi]
pub fn get_item(&self, index: u32) -> String {
    self.items[index as usize].clone()  // Panics on out of bounds
}

// GOOD: Return Result with clear error
#[napi]
pub fn get_item(&self, index: u32) -> Result<String> {
    self.items.get(index as usize)
        .map(|s| s.clone())
        .ok_or_else(|| Error::new(
            Status::InvalidArg,
            format!("Index {} out of bounds (length {})", index, self.items.len())
        ))
}
```

## Error Context Pattern

```rust
// Add context to errors from external operations
#[napi]
pub async fn import_schema(path: String) -> Result<Schema> {
    let content = tokio::fs::read_to_string(&path)
        .await
        .map_err(|e| Error::from_reason(format!("Reading {}: {}", path, e)))?;
    
    let parsed = serde_json::from_str(&content)
        .map_err(|e| Error::from_reason(format!("Parsing {} as JSON: {}", path, e)))?;
    
    Schema::validate(parsed)
        .map_err(|e| Error::from_reason(format!("Validating schema from {}: {}", path, e)))
}
```

## JavaScript Error Handling Patterns

```javascript
// Sync errors
try {
    const stmt = cqlite.parse("INVALID CQL");
} catch (err) {
    if (err.message.includes("parse")) {
        console.error("Syntax error:", err.message);
    } else {
        throw err;  // Re-throw unexpected errors
    }
}

// Async errors
async function loadSchema() {
    try {
        return await cqlite.readAndParse("schema.cql");
    } catch (err) {
        console.error("Failed to load schema:", err.message);
        return [];
    }
}

// With error type checking (if using custom errors)
try {
    const stmt = cqlite.parseStrict(query);
} catch (err) {
    if (err instanceof cqlite.ParseError) {
        console.error(`Error at line ${err.line}: ${err.message}`);
    }
}
```

## Result Type Alias

```rust
// In your crate
pub type CqliteResult<T> = std::result::Result<T, CqliteError>;

// napi functions still return napi::Result
#[napi]
pub fn parse(query: String) -> napi::Result<Statement> {
    let stmt: CqliteResult<cqlite::Statement> = cqlite::parse(&query);
    stmt.map(Statement::from).map_err(|e| e.into())
}
```
