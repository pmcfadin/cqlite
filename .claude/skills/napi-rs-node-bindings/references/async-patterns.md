# Async Patterns

## Basic Async Function

```rust
use napi::bindgen_prelude::*;

// Async functions automatically return Promise
#[napi]
pub async fn read_file(path: String) -> Result<String> {
    tokio::fs::read_to_string(&path)
        .await
        .map_err(|e| Error::from_reason(e.to_string()))
}
```

```typescript
// Generated TypeScript
export function readFile(path: string): Promise<string>
```

```javascript
// Usage
const content = await cqlite.readFile("schema.cql");
```

## Tokio Runtime

napi-rs uses tokio by default. Enable in Cargo.toml:

```toml
[dependencies]
napi = { version = "2", features = ["async", "tokio_rt"] }
tokio = { version = "1", features = ["full"] }
```

## CPU-Bound Work in Async Context

```rust
// BAD: Blocks the tokio runtime
#[napi]
pub async fn bad_cpu_work(data: Buffer) -> Result<String> {
    // This blocks the async runtime!
    expensive_computation(&data)
}

// GOOD: Use spawn_blocking for CPU-bound work
#[napi]
pub async fn good_cpu_work(data: Buffer) -> Result<String> {
    let data = data.to_vec();  // Copy for 'static lifetime
    tokio::task::spawn_blocking(move || {
        expensive_computation(&data)
    })
    .await
    .map_err(|e| Error::from_reason(e.to_string()))?
}
```

## Parallel Async Operations

```rust
#[napi]
pub async fn parse_files(paths: Vec<String>) -> Result<Vec<Statement>> {
    let futures: Vec<_> = paths.into_iter()
        .map(|path| async move {
            let content = tokio::fs::read_to_string(&path).await?;
            cqlite::parse(&content)
        })
        .collect();
    
    let results = futures::future::try_join_all(futures)
        .await
        .map_err(|e| Error::from_reason(e.to_string()))?;
    
    Ok(results.into_iter().map(Statement::from).collect())
}
```

## Streaming with AsyncIterator

```rust
use napi::bindgen_prelude::*;
use futures::stream::{self, StreamExt};

#[napi]
pub fn create_row_stream(query: String) -> AsyncIterator<Row> {
    // Create a stream that yields rows
    let stream = stream::iter(fetch_rows(&query))
        .map(|row| Ok(Row::from(row)));
    
    AsyncIterator::new(stream)
}
```

```javascript
// Usage
for await (const row of cqlite.createRowStream("SELECT * FROM users")) {
    console.log(row);
}
```

## Callbacks and ThreadsafeFunction

For calling back into JavaScript from Rust threads:

```rust
use napi::threadsafe_function::{
    ThreadsafeFunction, 
    ThreadsafeFunctionCallMode,
    ErrorStrategy
};

#[napi]
pub fn subscribe_to_events(
    callback: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>
) -> Result<()> {
    // Spawn background task
    std::thread::spawn(move || {
        loop {
            let event = wait_for_event();
            
            // Call JS callback from Rust thread
            callback.call(
                Ok(event),
                ThreadsafeFunctionCallMode::NonBlocking
            );
        }
    });
    
    Ok(())
}
```

```javascript
// Usage
cqlite.subscribeToEvents((err, event) => {
    if (err) {
        console.error("Event error:", err);
        return;
    }
    console.log("Received event:", event);
});
```

### ThreadsafeFunction with Multiple Arguments

```rust
use napi::JsUnknown;

#[napi]
pub fn on_progress(
    callback: ThreadsafeFunction<(u32, u32), ErrorStrategy::Fatal>
) -> Result<()> {
    std::thread::spawn(move || {
        for i in 0..100 {
            callback.call(Ok((i, 100)), ThreadsafeFunctionCallMode::Blocking);
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    Ok(())
}
```

## Async with Cancellation

```rust
use tokio::sync::oneshot;
use std::sync::Arc;
use parking_lot::Mutex;

#[napi]
pub struct CancellableTask {
    cancel_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[napi]
impl CancellableTask {
    #[napi(factory)]
    pub fn start(work: JsFunction) -> Self {
        let (tx, rx) = oneshot::channel();
        
        tokio::spawn(async move {
            tokio::select! {
                _ = rx => {
                    // Cancelled
                }
                result = do_work() => {
                    // Completed
                }
            }
        });
        
        Self { cancel_tx: Arc::new(Mutex::new(Some(tx))) }
    }
    
    #[napi]
    pub fn cancel(&self) -> bool {
        if let Some(tx) = self.cancel_tx.lock().take() {
            tx.send(()).is_ok()
        } else {
            false
        }
    }
}
```

## Promise from Sync Code

```rust
use napi::bindgen_prelude::*;

// Wrap sync code in a promise (runs on thread pool)
#[napi]
pub fn parse_async(env: Env, query: String) -> Result<JsObject> {
    env.execute_tokio_future(
        async move {
            // This runs on tokio runtime
            tokio::task::spawn_blocking(move || {
                cqlite::parse(&query)
            })
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?
            .map(Statement::from)
            .map_err(|e| Error::from_reason(e.to_string()))
        },
        |env, result| {
            result  // Convert to JS value
        }
    )
}
```

## Async Initialization

```rust
#[napi]
pub struct Database {
    inner: Option<cqlite::Database>,
}

#[napi]
impl Database {
    // Async factory method
    #[napi(factory)]
    pub async fn connect(connection_string: String) -> Result<Self> {
        let db = cqlite::Database::connect(&connection_string)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        
        Ok(Self { inner: Some(db) })
    }
    
    #[napi]
    pub async fn query(&self, cql: String) -> Result<Vec<Row>> {
        let db = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Database closed"))?;
        
        db.query(&cql)
            .await
            .map(|rows| rows.into_iter().map(Row::from).collect())
            .map_err(|e| Error::from_reason(e.to_string()))
    }
    
    #[napi]
    pub async fn close(&mut self) -> Result<()> {
        if let Some(db) = self.inner.take() {
            db.close().await.map_err(|e| Error::from_reason(e.to_string()))?;
        }
        Ok(())
    }
}
```

```javascript
// Usage
const db = await Database.connect("localhost:9042");
const rows = await db.query("SELECT * FROM users");
await db.close();
```

## Error Handling in Async

```rust
#[napi]
pub async fn safe_operation() -> Result<String> {
    // Multiple potential failure points
    let data = fetch_data()
        .await
        .map_err(|e| Error::from_reason(format!("Fetch failed: {}", e)))?;
    
    let processed = process_data(data)
        .await
        .map_err(|e| Error::from_reason(format!("Processing failed: {}", e)))?;
    
    Ok(processed)
}
```

```javascript
// JavaScript
try {
    const result = await cqlite.safeOperation();
} catch (err) {
    // Error message includes context
    console.error(err.message);  // "Fetch failed: ..." or "Processing failed: ..."
}
```
