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
use crate::error::{runtime_init_error, to_napi_error};
use crate::value::{intern_column_keys, row_to_object, ConvCtx};

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
    /// Authoritative SELECT-order column names, shared with each `next()` task so
    /// rows are emitted in column order rather than HashMap hash order (#1446).
    column_names: Arc<Vec<String>>,
    /// Per-stream span (issue #1040). Shared with each `next()` task so rows
    /// yielded across iterations accumulate, and finalised on
    /// exhaustion/close. `Span` is cheap to clone and is a no-op when telemetry
    /// is disabled.
    span: tracing::Span,
    /// Rows fetched per `next()` AsyncTask (issue #1443). Each `next()` pulls a
    /// BATCH of up to this many rows via `collect_chunk`, amortising the libuv
    /// threadpool dispatch + `block_on` over K rows instead of paying it per
    /// row. Sourced from the stream's `bufferSize` (bounded-channel capacity),
    /// so the batch never exceeds the backpressure window.
    batch_size: usize,
}

impl StreamingResult {
    /// Create a new StreamingResult from a core QueryResultIterator, the
    /// owning per-stream span, and the per-`next()` batch size (the stream's
    /// `bufferSize`, issue #1443).
    pub fn new(
        iter: cqlite_core::query::result::QueryResultIterator,
        span: tracing::Span,
        batch_size: usize,
    ) -> napi::Result<Self> {
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

        let column_names = Arc::new(columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>());

        // Per-`next()` batch size (issue #1443): the stream's `bufferSize`,
        // which is the bounded-channel backpressure window, so batching this
        // many rows per `collect_chunk` never exceeds it. `collect_chunk` caps
        // at MAX_CHUNK_SIZE, so any value is safe; a floor of 1 guarantees
        // forward progress (bufferSize is validated non-zero upstream today).
        let batch_size = batch_size.max(1);

        Ok(Self {
            inner: Arc::new(Mutex::new(Some(iter))),
            columns,
            column_names,
            span,
            batch_size,
        })
    }

    /// Record the final rows-YIELDED count onto the stream span (issue #1443).
    ///
    /// The span's `"rows"` metric is documented as "rows yielded to the
    /// consumer". On EARLY TERMINATION the JS wrapper discards the un-yielded
    /// tail of the last fetched batch, so `iter.rows_received()` (advanced by
    /// the whole fetched batch in `collect_chunk`) over-counts. When the wrapper
    /// can supply the exact number of rows it actually yielded, `yielded` is
    /// `Some` and we record THAT. When it is `None` — natural exhaustion, or an
    /// external/direct `close()` where the yielded count is unknown — we fall
    /// back to `iter.rows_received()`, which is accurate on exhaustion (every
    /// fetched row was yielded) and is the best available signal otherwise.
    fn finalize_span(&self, yielded: Option<u64>) {
        let rows = yielded.unwrap_or_else(|| self.rows_received().unwrap_or(0) as u64);
        self.span.record("rows", rows);
    }
}

/// Result from next() - represents iterator protocol result.
///
/// Issue #1443: `next()` fetches a BATCH of rows per AsyncTask, so `Value`
/// carries a `Vec` of row maps rather than a single row. The JS wrapper buffers
/// the batch and yields one row per `for await` iteration, keeping the per-row
/// consumer contract unchanged while amortising dispatch over K rows.
pub enum NextResult {
    /// One or more rows are available (a non-empty batch).
    Value(Vec<HashMap<String, cqlite_core::types::Value>>),
    /// Stream exhausted (no more rows).
    Done,
}

/// Async task for fetching the next batch of rows.
pub struct NextTask {
    inner: Arc<Mutex<Option<cqlite_core::query::result::QueryResultIterator>>>,
    /// SELECT-order column names for this stream, used to emit each yielded
    /// row's properties in column order rather than HashMap hash order (#1446).
    column_names: Arc<Vec<String>>,
    /// Stream span, finalised with the row count when the stream ends or errors
    /// (issue #1040).
    span: tracing::Span,
    /// Rows to fetch in this task (issue #1443); the stream's `bufferSize`.
    batch_size: usize,
}

impl napi::Task for NextTask {
    type Output = NextResult;
    type JsValue = JsObject;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        // Firewall the async-worker thread against a decode panic (issue #1754):
        // a panic on this libuv threadpool thread cannot unwind across the FFI
        // frame and would abort the whole Node process even under `panic=unwind`.
        // Catch it here and reject with a typed error instead.
        crate::error::catch_unwind_to_napi("executeStreaming.next", || self.compute_inner())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        self.resolve_impl(env, output)
    }
}

impl NextTask {
    fn compute_inner(&mut self) -> napi::Result<NextResult> {
        use tracing::Instrument;

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

        // Fetch a BATCH of up to `batch_size` rows from the core iterator using
        // the global runtime (issue #1443). One `block_on` amortises the libuv
        // dispatch over K rows instead of paying it per row, and frees the
        // threadpool thread again quickly so concurrent `fs`/`crypto` work is not
        // starved. `collect_chunk` respects the bounded channel's backpressure
        // (the producer still blocks when the buffer is full) and caps at
        // MAX_CHUNK_SIZE, so the batch never grows unbounded. The fetch is
        // `.instrument`-ed by the stream span (no guard held across the runtime).
        //
        // Snapshot the delivered-row count BEFORE the fetch (issue #1443): on a
        // mid-batch error, `collect_chunk` drops the up-to-(K-1) rows it already
        // read (they are never yielded), yet `rows_received()` counted them. Prior
        // batches were each fully delivered, so `rows_before` is exactly the rows
        // actually emitted to the consumer at the point of failure.
        let rows_before: u64 = iter.rows_received();
        let chunk = crate::runtime::block_on(
            iter.collect_chunk(self.batch_size)
                .instrument(self.span.clone()),
        )
        .map_err(runtime_init_error)?;
        match chunk {
            Ok(rows) if rows.is_empty() => {
                // Empty chunk means the stream is exhausted (`collect_chunk` only
                // returns fewer than requested when the channel closed). Finalise
                // span with the total yielded, cleanup.
                let yielded: u64 = iter.rows_received();
                self.span.record("rows", yielded);
                *guard = None;
                Ok(NextResult::Done)
            }
            Ok(rows) => {
                // Materialise the interned `Arc<str>` name handles into `String`
                // keys at the FFI boundary (issue #1334): the JS-facing row maps
                // are keyed by `String`. An empty chunk signals exhaustion; a
                // non-empty chunk (partial OR full) is always yielded, and the
                // next `next()` observes the empty chunk to terminate and clean up.
                let batch = rows
                    .into_iter()
                    .map(|row| {
                        row.values
                            .into_iter()
                            .map(|(k, v)| (k.to_string(), v))
                            .collect::<HashMap<String, cqlite_core::types::Value>>()
                    })
                    .collect();
                Ok(NextResult::Value(batch))
            }
            Err(e) => {
                // Error occurred - record at the boundary, finalise span, cleanup.
                // Record `rows_before` (rows actually emitted before this failing
                // batch), not `iter.rows_received()`, which over-counts the dropped
                // partial batch (issue #1443).
                crate::observability::record_boundary_error(&e);
                self.span.record("rows", rows_before);
                *guard = None;
                Err(to_napi_error(e))
            }
        }
    }

    fn resolve_impl(&mut self, env: Env, output: NextResult) -> napi::Result<JsObject> {
        let mut result = env.create_object()?;

        match output {
            NextResult::Value(batch) => {
                // Build a JS ARRAY of row objects for the batch (issue #1443).
                // The interned SELECT-order column keys are computed once per
                // batch and reused across every row (as `executeNative` does),
                // so interning cost is now amortised over K rows rather than paid
                // per row. The JS wrapper buffers this array and yields one row
                // per `for await` iteration, so the per-row consumer contract is
                // unchanged; batching is invisible to consumers. Column order
                // (#1446) is preserved via `self.column_names`.
                let col_keys = intern_column_keys(&env, &self.column_names)?;
                // Issue #1448: ONE conversion context per `resolve` call, reused
                // across every row in the batch (napi handles are scoped to this
                // `resolve` `Env`, so the ctor cache cannot outlive it). Each
                // Set/Map ctor is fetched at most once per batch rather than once
                // per set/map cell — the same ctor-cache invariant #1448 uses for
                // `executeNative`, now amortised over the whole batch.
                let ctx = ConvCtx::new(&env);
                let mut rows_arr = env.create_array_with_length(batch.len())?;
                for (i, values) in batch.iter().enumerate() {
                    let row_obj = row_to_object(&ctx, &col_keys, values)?;
                    rows_arr.set_element(i as u32, row_obj)?;
                }
                result.set_named_property("rows", rows_arr)?;
                result.set_named_property("done", env.get_boolean(false)?)?;
            }
            NextResult::Done => {
                let empty = env.create_array_with_length(0)?;
                result.set_named_property("rows", empty)?;
                result.set_named_property("done", env.get_boolean(true)?)?;
            }
        }

        Ok(result)
    }
}

#[napi]
impl StreamingResult {
    /// Get the next BATCH of rows from the stream (issue #1443).
    ///
    /// Each call fetches up to `bufferSize` rows in a single AsyncTask, so the
    /// JS wrapper can buffer them and yield one row per `for await` iteration
    /// without a per-row threadpool dispatch. Returns an object:
    /// - `{ rows: Array<Row>, done: false }` - a non-empty batch of rows
    /// - `{ rows: [], done: true }` - stream exhausted
    ///
    /// Errors are thrown as exceptions with structured error properties
    /// (code, category, isRecoverable).
    ///
    /// @returns Promise resolving to a batch result object
    #[napi]
    pub fn next(&self) -> AsyncTask<NextTask> {
        // Clone the Arc reference + span to share with the task
        AsyncTask::new(NextTask {
            inner: self.inner.clone(),
            column_names: self.column_names.clone(),
            span: self.span.clone(),
            batch_size: self.batch_size,
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
    ///
    /// Note: In the JavaScript wrapper, columns return an empty array before
    /// the first iteration. The native struct has columns immediately, but
    /// the wrapper defers stream creation until first `next()` call.
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
    ///
    /// `yielded` (issue #1443) is the exact number of rows the JS wrapper
    /// actually yielded to the consumer, passed on an early `break`/`return()`
    /// so the span records rows YIELDED rather than the whole fetched batch (the
    /// wrapper discards the un-yielded tail). It is omitted (`None`) for an
    /// external/direct `close()`, where the yielded count is unknown and
    /// `rows_received()` is used instead.
    #[napi]
    pub fn close(&self, yielded: Option<u32>) -> napi::Result<()> {
        // Finalise the stream span with the rows yielded so far before the
        // iterator is dropped (issue #1040). Reads the count first so an early
        // `break` from a `for await` loop still records progress.
        self.finalize_span(yielded.map(u64::from));

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
