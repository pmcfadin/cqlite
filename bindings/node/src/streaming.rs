//! Streaming iterator for memory-efficient query results.
//!
//! Issue #305: Implements streaming query support for large result sets via
//! JavaScript's `Symbol.asyncIterator` protocol.
//!
//! ## Architecture
//!
//! The streaming implementation uses a hybrid Rust/JavaScript approach:
//! - Rust: `StreamingResult` class with `next()` method returning AsyncTask
//! - JavaScript: Wrapper adds `Symbol.asyncIterator` for `for await...of`
//!
//! This follows the established pattern from `error-wrapper.js` and provides
//! stability over napi-rs's experimental async iterator support.
//!
//! ## Memory Bounded
//!
//! Memory stays bounded by `StreamingConfig` settings (default ~11MB peak):
//! - `bufferSize`: 1024 rows in flight (~1MB)
//! - `chunkSize`: 10,000 rows per chunk (~10MB)
//!
//! ## Backpressure
//!
//! The core library uses bounded mpsc channels to provide natural backpressure.
//! When the consumer is slow, the channel buffer fills up to `bufferSize`, and
//! the producer blocks until the consumer catches up. This ensures memory stays
//! bounded regardless of consumer speed.
//!
//! ## Thread Safety
//!
//! The iterator is wrapped in `Arc<Mutex>` for interior mutability. However,
//! concurrent iteration from multiple JavaScript contexts is NOT supported.
//! The JavaScript layer should ensure sequential `next()` calls via the
//! async iterator protocol's natural serialization.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use napi::bindgen_prelude::*;
use napi::{Env, JsObject};
use napi_derive::napi;

use crate::database::ColumnInfo;
use crate::error::to_napi_error;
use crate::value::row_to_object;

/// Streaming query result iterator.
///
/// Yields rows one at a time via `next()` method which returns an AsyncTask.
/// JavaScript wrapper implements `Symbol.asyncIterator` for `for await...of` support.
///
/// ## Resource Cleanup
///
/// Resources are cleaned up when:
/// 1. All rows consumed (`next()` returns `done: true`)
/// 2. Iterator dropped (early break from loop triggers `return()`)
/// 3. `close()` called explicitly
/// 4. Error occurs
///
/// ## Example (JavaScript)
///
/// ```javascript
/// // No await on executeStreaming - it returns an AsyncIterable directly
/// for await (const row of db.executeStreaming('SELECT * FROM large_table')) {
///   console.log(row);
/// }
/// ```
#[napi]
pub struct StreamingResult {
    /// Inner iterator wrapped in Arc<Mutex> for shared access across async tasks.
    /// Option allows cleanup on close/exhaustion.
    inner: Arc<Mutex<Option<cqlite_core::query::result::QueryResultIterator>>>,
    /// Cached column metadata for the result set.
    columns: Vec<ColumnInfo>,
}

impl StreamingResult {
    /// Create a new StreamingResult from a core QueryResultIterator.
    pub fn new(iter: cqlite_core::query::result::QueryResultIterator) -> napi::Result<Self> {
        // Cache column metadata from iterator
        let columns: Vec<ColumnInfo> = iter
            .metadata
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| ColumnInfo {
                name: col.name.clone(),
                data_type: format!("{:?}", col.data_type),
                nullable: col.nullable,
                position: i as u32,
                table_name: col.table_name.clone(),
            })
            .collect();

        Ok(Self {
            inner: Arc::new(Mutex::new(Some(iter))),
            columns,
        })
    }
}

/// Result from next() - represents iterator protocol result.
pub enum NextResult {
    /// More data available
    Value(HashMap<String, cqlite_core::types::Value>),
    /// Stream exhausted
    Done,
}

/// Async task for fetching the next row.
pub struct NextTask {
    inner: Arc<Mutex<Option<cqlite_core::query::result::QueryResultIterator>>>,
}

impl napi::Task for NextTask {
    type Output = NextResult;
    type JsValue = JsObject;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        // Acquire lock - use unwrap_or_else to handle poisoned mutex by clearing the iterator
        let mut guard = self.inner.lock().unwrap_or_else(|poisoned| {
            // Mutex was poisoned by a panic in another thread
            // Recover by taking ownership and clearing the iterator
            let mut guard = poisoned.into_inner();
            *guard = None;
            guard
        });

        // Check if iterator is already exhausted
        let iter = match guard.as_mut() {
            Some(it) => it,
            None => return Ok(NextResult::Done),
        };

        // Get next row from core iterator using the global runtime
        match crate::runtime::block_on(iter.next_async()) {
            Some(Ok(row)) => {
                // Take ownership of values - no clone needed
                Ok(NextResult::Value(row.values))
            }
            Some(Err(e)) => {
                // Error occurred - cleanup and propagate
                *guard = None;
                Err(to_napi_error(e))
            }
            None => {
                // Stream exhausted - cleanup
                *guard = None;
                Ok(NextResult::Done)
            }
        }
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        let mut result = env.create_object()?;

        match output {
            NextResult::Value(values) => {
                let row_obj = row_to_object(&env, &values)?;
                result.set_named_property("value", row_obj)?;
                result.set_named_property("done", env.get_boolean(false)?)?;
            }
            NextResult::Done => {
                result.set_named_property("value", env.get_undefined()?)?;
                result.set_named_property("done", env.get_boolean(true)?)?;
            }
        }

        Ok(result)
    }
}

#[napi]
impl StreamingResult {
    /// Get the next row from the stream.
    ///
    /// Returns an object matching JavaScript's iterator protocol:
    /// - `{ value: Row, done: false }` - More rows available
    /// - `{ value: undefined, done: true }` - Stream exhausted
    ///
    /// Errors are thrown as exceptions with structured error properties
    /// (code, category, isRecoverable).
    ///
    /// @returns Promise resolving to iterator result object
    #[napi]
    pub fn next(&self) -> AsyncTask<NextTask> {
        // Clone the Arc reference to share with the task
        AsyncTask::new(NextTask {
            inner: self.inner.clone(),
        })
    }

    /// Number of rows received so far.
    ///
    /// This counter increases as rows are yielded from the stream.
    /// Useful for progress tracking.
    ///
    /// @returns Number of rows received
    #[napi(getter, js_name = "rowsReceived")]
    pub fn rows_received(&self) -> napi::Result<u32> {
        // Use unwrap_or_else to handle poisoned mutex gracefully
        let guard = self.inner.lock().unwrap_or_else(|poisoned| {
            // Return the inner value even if poisoned - we just need to read
            poisoned.into_inner()
        });

        match guard.as_ref() {
            Some(iter) => Ok(iter.rows_received() as u32),
            None => Ok(0), // Already exhausted
        }
    }

    /// Column metadata for the result set.
    ///
    /// Contains information about each column's name, type, and nullability.
    /// Available immediately after creating the streaming result.
    ///
    /// @returns Array of ColumnInfo objects
    #[napi(getter)]
    pub fn columns(&self) -> Vec<ColumnInfo> {
        self.columns.clone()
    }

    /// Release resources early (synchronous).
    ///
    /// Called automatically when:
    /// - All rows are consumed
    /// - JavaScript iterator's `return()` is called (e.g., `break` from loop)
    /// - Error occurs during iteration
    ///
    /// Safe to call multiple times - subsequent calls are no-ops.
    /// This method is synchronous and does not need to be awaited.
    #[napi]
    pub fn close(&self) -> napi::Result<()> {
        // Use unwrap_or_else to handle poisoned mutex gracefully
        let mut guard = self.inner.lock().unwrap_or_else(|poisoned| {
            // Recover by taking ownership
            poisoned.into_inner()
        });

        // Drop the iterator to release resources
        *guard = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_streaming_result_structure() {
        // This test verifies the structure is correct at compile time.
        // Actual runtime tests are in the JavaScript test suite.
    }
}
